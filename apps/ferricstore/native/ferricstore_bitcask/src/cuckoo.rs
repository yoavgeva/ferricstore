//! Cuckoo filter implementation for FerricStore.
//!
//! A space-efficient probabilistic data structure similar to Bloom filters,
//! but supporting deletion and approximate counting. Stores fingerprints of
//! elements in a hash table with two candidate bucket positions per element.
//!
//! ## Storage format
//!
//! The filter serializes to a compact byte array for Bitcask storage:
//!
//! ```text
//! [magic: 2B][version: 1B][capacity: 4B][bucket_size: 1B]
//! [fingerprint_size: 1B][max_kicks: 2B][num_items: 8B][num_deletes: 8B]
//! [buckets: capacity * bucket_size * fingerprint_size bytes]
//! ```
//!
//! Total header size: 27 bytes.

use std::sync::Mutex;

use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, ResourceArc, Term};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying a cuckoo filter blob.
const MAGIC: [u8; 2] = [0xCF, 0x01];
/// Current serialization version.
const VERSION: u8 = 1;
/// Header size in bytes.
const HEADER_SIZE: usize = 27;
/// Default fingerprint size in bytes.
const DEFAULT_FINGERPRINT_SIZE: usize = 2;
/// Default number of fingerprints per bucket.
const DEFAULT_BUCKET_SIZE: usize = 4;
/// Default maximum kick attempts during insertion.
const DEFAULT_MAX_KICKS: u32 = 500;

// ---------------------------------------------------------------------------
// CuckooFilter
// ---------------------------------------------------------------------------

/// A cuckoo filter storing fingerprints in a flat bucket array.
pub struct CuckooFilter {
    /// Flat byte array: `num_buckets * bucket_size * fingerprint_size` bytes.
    buckets: Vec<u8>,
    /// Number of buckets.
    num_buckets: usize,
    /// Number of fingerprint slots per bucket.
    bucket_size: usize,
    /// Fingerprint size in bytes.
    fingerprint_size: usize,
    /// Number of items currently inserted.
    num_items: u64,
    /// Number of items that have been deleted.
    num_deletes: u64,
    /// Maximum kick attempts before declaring the filter full.
    max_kicks: u32,
}

impl CuckooFilter {
    /// Create a new empty cuckoo filter with the given capacity (number of buckets).
    #[must_use]
    pub fn new(capacity: usize, bucket_size: usize, max_kicks: u32) -> Self {
        let fingerprint_size = DEFAULT_FINGERPRINT_SIZE;
        let total_bytes = capacity * bucket_size * fingerprint_size;
        Self {
            buckets: vec![0u8; total_bytes],
            num_buckets: capacity,
            bucket_size,
            fingerprint_size,
            num_items: 0,
            num_deletes: 0,
            max_kicks,
        }
    }

    /// Compute the MD5 hash of `data`.
    fn md5(data: &[u8]) -> [u8; 16] {
        // We implement MD5 ourselves to avoid external dependencies.
        // This uses the standard RFC 1321 algorithm.
        md5_hash(data)
    }

    /// Compute the fingerprint for an element.
    /// Returns the first `fingerprint_size` bytes of MD5, ensuring it is never all zeros.
    fn fingerprint(&self, element: &[u8]) -> Vec<u8> {
        let hash = Self::md5(element);
        let mut fp = hash[..self.fingerprint_size].to_vec();

        // Ensure fingerprint is never all zeros (zero = empty slot sentinel).
        if fp.iter().all(|&b| b == 0) {
            fp[0] = 1;
        }
        fp
    }

    /// Compute the primary bucket index for an element.
    fn primary_bucket(&self, element: &[u8]) -> usize {
        let hash = Self::md5(element);
        // Skip first `fingerprint_size` bytes (used for fingerprint), then read 8 bytes LE.
        let start = self.fingerprint_size;
        let hash_val = u64::from_le_bytes([
            hash[start],
            hash[start + 1],
            hash[start + 2],
            hash[start + 3],
            hash[start + 4],
            hash[start + 5],
            hash[start + 6],
            hash[start + 7],
        ]);
        (hash_val as usize) % self.num_buckets
    }

    /// Compute the alternate bucket: `bucket XOR hash(fingerprint)`.
    fn alternate_bucket(&self, bucket: usize, fp: &[u8]) -> usize {
        let hash = Self::md5(fp);
        let fp_hash = u64::from_le_bytes([
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
        ]);
        ((bucket as u64) ^ fp_hash) as usize % self.num_buckets
    }

    /// Get the offset into the bucket array for a given bucket and slot.
    fn slot_offset(&self, bucket_idx: usize, slot_idx: usize) -> usize {
        (bucket_idx * self.bucket_size + slot_idx) * self.fingerprint_size
    }

    /// Read the fingerprint at a given bucket and slot.
    fn get_slot(&self, bucket_idx: usize, slot_idx: usize) -> &[u8] {
        let offset = self.slot_offset(bucket_idx, slot_idx);
        &self.buckets[offset..offset + self.fingerprint_size]
    }

    /// Write a fingerprint at a given bucket and slot.
    fn set_slot(&mut self, bucket_idx: usize, slot_idx: usize, fp: &[u8]) {
        let offset = self.slot_offset(bucket_idx, slot_idx);
        self.buckets[offset..offset + self.fingerprint_size].copy_from_slice(fp);
    }

    /// Check if a slot is empty (all zeros).
    fn is_slot_empty(&self, bucket_idx: usize, slot_idx: usize) -> bool {
        let offset = self.slot_offset(bucket_idx, slot_idx);
        self.buckets[offset..offset + self.fingerprint_size]
            .iter()
            .all(|&b| b == 0)
    }

    /// Find the first empty slot in a bucket. Returns `Some(slot_idx)` or `None`.
    fn find_empty_slot(&self, bucket_idx: usize) -> Option<usize> {
        for slot in 0..self.bucket_size {
            if self.is_slot_empty(bucket_idx, slot) {
                return Some(slot);
            }
        }
        None
    }

    /// Add an element to the filter. Returns `Ok(())` on success, `Err(())` if full.
    pub fn add(&mut self, element: &[u8]) -> Result<(), ()> {
        let fp = self.fingerprint(element);
        let b1 = self.primary_bucket(element);
        let b2 = self.alternate_bucket(b1, &fp);

        // Try primary bucket.
        if let Some(slot) = self.find_empty_slot(b1) {
            self.set_slot(b1, slot, &fp);
            self.num_items += 1;
            return Ok(());
        }

        // Try alternate bucket.
        if let Some(slot) = self.find_empty_slot(b2) {
            self.set_slot(b2, slot, &fp);
            self.num_items += 1;
            return Ok(());
        }

        // Both full: kick.
        self.kick(fp, b1)
    }

    /// Cuckoo eviction loop.
    fn kick(&mut self, mut fp: Vec<u8>, mut bucket: usize) -> Result<(), ()> {
        for kicks in 0..self.max_kicks {
            let slot_idx = (kicks as usize) % self.bucket_size;

            // Read evicted fingerprint.
            let evicted = self.get_slot(bucket, slot_idx).to_vec();

            // Place our fingerprint in that slot.
            self.set_slot(bucket, slot_idx, &fp);

            // Find alternate bucket for evicted fingerprint.
            let alt = self.alternate_bucket(bucket, &evicted);

            // Try to place evicted fingerprint in its alternate bucket.
            if let Some(empty_slot) = self.find_empty_slot(alt) {
                self.set_slot(alt, empty_slot, &evicted);
                self.num_items += 1;
                return Ok(());
            }

            // Continue kicking from the alternate bucket.
            fp = evicted;
            bucket = alt;
        }

        Err(())
    }

    /// Add an element only if it does not already exist.
    /// Returns 1 if added, 0 if already present, or `Err(())` if full.
    pub fn addnx(&mut self, element: &[u8]) -> Result<u64, ()> {
        if self.exists(element) {
            return Ok(0);
        }
        self.add(element)?;
        Ok(1)
    }

    /// Check if an element may exist in the filter.
    pub fn exists(&self, element: &[u8]) -> bool {
        let fp = self.fingerprint(element);
        let b1 = self.primary_bucket(element);
        let b2 = self.alternate_bucket(b1, &fp);

        self.bucket_contains(b1, &fp) || self.bucket_contains(b2, &fp)
    }

    /// Check if a bucket contains a given fingerprint.
    fn bucket_contains(&self, bucket_idx: usize, fp: &[u8]) -> bool {
        for slot in 0..self.bucket_size {
            if self.get_slot(bucket_idx, slot) == fp {
                return true;
            }
        }
        false
    }

    /// Check multiple elements at once. Returns a vector of 0/1 values.
    pub fn mexists(&self, elements: &[&[u8]]) -> Vec<u64> {
        elements
            .iter()
            .map(|e| if self.exists(e) { 1 } else { 0 })
            .collect()
    }

    /// Delete one occurrence of an element. Returns 1 if deleted, 0 if not found.
    pub fn delete(&mut self, element: &[u8]) -> u64 {
        let fp = self.fingerprint(element);
        let b1 = self.primary_bucket(element);
        let b2 = self.alternate_bucket(b1, &fp);

        let empty = vec![0u8; self.fingerprint_size];

        // Try primary bucket first.
        for slot in 0..self.bucket_size {
            if self.get_slot(b1, slot) == fp.as_slice() {
                self.set_slot(b1, slot, &empty);
                self.num_items -= 1;
                self.num_deletes += 1;
                return 1;
            }
        }

        // Try alternate bucket.
        for slot in 0..self.bucket_size {
            if self.get_slot(b2, slot) == fp.as_slice() {
                self.set_slot(b2, slot, &empty);
                self.num_items -= 1;
                self.num_deletes += 1;
                return 1;
            }
        }

        0
    }

    /// Count occurrences of an element's fingerprint across both candidate buckets.
    pub fn count(&self, element: &[u8]) -> u64 {
        let fp = self.fingerprint(element);
        let b1 = self.primary_bucket(element);
        let b2 = self.alternate_bucket(b1, &fp);

        let mut total = 0u64;
        for slot in 0..self.bucket_size {
            if self.get_slot(b1, slot) == fp.as_slice() {
                total += 1;
            }
        }
        for slot in 0..self.bucket_size {
            if self.get_slot(b2, slot) == fp.as_slice() {
                total += 1;
            }
        }
        total
    }

    /// Serialize the filter to a byte array for Bitcask storage.
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.buckets.len());

        // Magic (2 bytes)
        buf.extend_from_slice(&MAGIC);
        // Version (1 byte)
        buf.push(VERSION);
        // Capacity / num_buckets (4 bytes, LE)
        buf.extend_from_slice(&(self.num_buckets as u32).to_le_bytes());
        // Bucket size (1 byte)
        buf.push(self.bucket_size as u8);
        // Fingerprint size (1 byte)
        buf.push(self.fingerprint_size as u8);
        // Max kicks (2 bytes, LE)
        buf.extend_from_slice(&(self.max_kicks as u16).to_le_bytes());
        // Num items (8 bytes, LE)
        buf.extend_from_slice(&self.num_items.to_le_bytes());
        // Num deletes (8 bytes, LE)
        buf.extend_from_slice(&self.num_deletes.to_le_bytes());
        // Bucket data
        buf.extend_from_slice(&self.buckets);

        buf
    }

    /// Deserialize a filter from a byte array.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the data is too short, has wrong magic, or wrong version.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < HEADER_SIZE {
            return Err("cuckoo: data too short".to_string());
        }
        if data[0..2] != MAGIC {
            return Err("cuckoo: invalid magic".to_string());
        }
        if data[2] != VERSION {
            return Err(format!("cuckoo: unsupported version {}", data[2]));
        }

        let num_buckets =
            u32::from_le_bytes([data[3], data[4], data[5], data[6]]) as usize;
        let bucket_size = data[7] as usize;
        let fingerprint_size = data[8] as usize;
        let max_kicks = u16::from_le_bytes([data[9], data[10]]) as u32;
        let num_items = u64::from_le_bytes([
            data[11], data[12], data[13], data[14], data[15], data[16], data[17], data[18],
        ]);
        let num_deletes = u64::from_le_bytes([
            data[19], data[20], data[21], data[22], data[23], data[24], data[25], data[26],
        ]);

        let expected_bucket_bytes = num_buckets * bucket_size * fingerprint_size;
        if data.len() < HEADER_SIZE + expected_bucket_bytes {
            return Err(format!(
                "cuckoo: expected {} bucket bytes, got {}",
                expected_bucket_bytes,
                data.len() - HEADER_SIZE
            ));
        }

        let buckets = data[HEADER_SIZE..HEADER_SIZE + expected_bucket_bytes].to_vec();

        Ok(Self {
            buckets,
            num_buckets,
            bucket_size,
            fingerprint_size,
            num_items,
            num_deletes,
            max_kicks,
        })
    }
}

// ---------------------------------------------------------------------------
// NIF resource
// ---------------------------------------------------------------------------

/// Resource wrapper for the cuckoo filter, protected by a Mutex.
pub struct CuckooResource {
    pub filter: Mutex<CuckooFilter>,
}

// ---------------------------------------------------------------------------
// NIF atoms
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

// ---------------------------------------------------------------------------
// NIF functions
// ---------------------------------------------------------------------------

/// Create a new cuckoo filter.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_create(
    env: Env,
    capacity: u64,
    bucket_size: u64,
    max_kicks: u64,
    _expansion: u64,
) -> NifResult<Term> {
    if capacity == 0 {
        return Ok((atoms::error(), "capacity must be > 0").encode(env));
    }
    let bs = if bucket_size == 0 {
        DEFAULT_BUCKET_SIZE
    } else {
        bucket_size as usize
    };
    let mk = if max_kicks == 0 {
        DEFAULT_MAX_KICKS
    } else {
        max_kicks as u32
    };

    let filter = CuckooFilter::new(capacity as usize, bs, mk);
    let resource = ResourceArc::new(CuckooResource {
        filter: Mutex::new(filter),
    });

    Ok((atoms::ok(), resource).encode(env))
}

/// Add an element to the filter.
/// Returns `:ok` or `{:error, "filter is full"}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_add<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>, item: Binary<'a>) -> NifResult<Term<'a>> {
    let mut filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    match filter.add(item.as_slice()) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(()) => Ok((atoms::error(), "filter is full").encode(env)),
    }
}

/// Add an element only if it does not already exist.
/// Returns 0 (already present) or 1 (added), or `{:error, ...}` if full.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_addnx<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>, item: Binary<'a>) -> NifResult<Term<'a>> {
    let mut filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    match filter.addnx(item.as_slice()) {
        Ok(val) => Ok(val.encode(env)),
        Err(()) => Ok((atoms::error(), "filter is full").encode(env)),
    }
}

/// Delete one occurrence of an element.
/// Returns 0 (not found) or 1 (deleted).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_del<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>, item: Binary<'a>) -> NifResult<Term<'a>> {
    let mut filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let result = filter.delete(item.as_slice());
    Ok(result.encode(env))
}

/// Check if an element may exist.
/// Returns 0 or 1.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_exists<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>, item: Binary<'a>) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let result: u64 = if filter.exists(item.as_slice()) { 1 } else { 0 };
    Ok(result.encode(env))
}

/// Check multiple elements at once.
/// Returns a list of 0/1 values.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_mexists<'a>(
    env: Env<'a>,
    resource: ResourceArc<CuckooResource>,
    items: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let slices: Vec<&[u8]> = items.iter().map(|b| b.as_slice()).collect();
    let results = filter.mexists(&slices);
    Ok(results.encode(env))
}

/// Count occurrences of an element's fingerprint.
/// Returns a non-negative integer.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_count<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>, item: Binary<'a>) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let result = filter.count(item.as_slice());
    Ok(result.encode(env))
}

/// Return filter metadata as a map.
/// Returns `{:ok, map}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_info<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let total_slots = (filter.num_buckets * filter.bucket_size) as u64;
    let info = (
        atoms::ok(),
        (
            filter.num_buckets as u64,
            filter.bucket_size as u64,
            filter.fingerprint_size as u64,
            filter.num_items,
            filter.num_deletes,
            total_slots,
            filter.max_kicks as u64,
        ),
    );
    Ok(info.encode(env))
}

/// Serialize the filter to a binary for Bitcask storage.
/// Returns `{:ok, binary}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_serialize<'a>(env: Env<'a>, resource: ResourceArc<CuckooResource>) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let bytes = filter.serialize();
    let mut bin = OwnedBinary::new(bytes.len()).ok_or(rustler::Error::BadArg)?;
    bin.as_mut_slice().copy_from_slice(&bytes);
    Ok((atoms::ok(), Binary::from_owned(bin, env)).encode(env))
}

/// Deserialize a filter from a binary blob.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_deserialize<'a>(env: Env<'a>, data: Binary<'a>) -> NifResult<Term<'a>> {
    match CuckooFilter::deserialize(data.as_slice()) {
        Ok(filter) => {
            let resource = ResourceArc::new(CuckooResource {
                filter: Mutex::new(filter),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(msg) => Ok((atoms::error(), msg).encode(env)),
    }
}

// ---------------------------------------------------------------------------
// MD5 implementation (RFC 1321)
// ---------------------------------------------------------------------------

/// Standard MD5 per-round shift amounts.
const S: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

/// Pre-computed table: T[i] = floor(2^32 * |sin(i+1)|).
const K: [u32; 64] = [
    0xd76a_a478, 0xe8c7_b756, 0x2420_70db, 0xc1bd_ceee,
    0xf57c_0faf, 0x4787_c62a, 0xa830_4613, 0xfd46_9501,
    0x6980_98d8, 0x8b44_f7af, 0xffff_5bb1, 0x895c_d7be,
    0x6b90_1122, 0xfd98_7193, 0xa679_438e, 0x49b4_0821,
    0xf61e_2562, 0xc040_b340, 0x265e_5a51, 0xe9b6_c7aa,
    0xd62f_105d, 0x0244_1453, 0xd8a1_e681, 0xe7d3_fbc8,
    0x21e1_cde6, 0xc337_07d6, 0xf4d5_0d87, 0x455a_14ed,
    0xa9e3_e905, 0xfcef_a3f8, 0x676f_02d9, 0x8d2a_4c8a,
    0xfffa_3942, 0x8771_f681, 0x6d9d_6122, 0xfde5_380c,
    0xa4be_ea44, 0x4bde_cfa9, 0xf6bb_4b60, 0xbebf_bc70,
    0x289b_7ec6, 0xeaa1_27fa, 0xd4ef_3085, 0x0488_1d05,
    0xd9d4_d039, 0xe6db_99e5, 0x1fa2_7cf8, 0xc4ac_5665,
    0xf429_2244, 0x432a_ff97, 0xab94_23a7, 0xfc93_a039,
    0x655b_59c3, 0x8f0c_cc92, 0xffef_f47d, 0x8584_5dd1,
    0x6fa8_7e4f, 0xfe2c_e6e0, 0xa301_4314, 0x4e08_11a1,
    0xf753_7e82, 0xbd3a_f235, 0x2ad7_d2bb, 0xeb86_d391,
];

/// Compute MD5 hash of `input`, returning the 16-byte digest.
fn md5_hash(input: &[u8]) -> [u8; 16] {
    // Step 1: Padding.
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut msg = input.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    // Step 2: Process each 64-byte block.
    let mut a0: u32 = 0x6745_2301;
    let mut b0: u32 = 0xefcd_ab89;
    let mut c0: u32 = 0x98ba_dcfe;
    let mut d0: u32 = 0x1032_5476;

    for chunk in msg.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            let off = i * 4;
            *word = u32::from_le_bytes([chunk[off], chunk[off + 1], chunk[off + 2], chunk[off + 3]]);
        }

        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };

            let temp = d;
            d = c;
            c = b;
            b = b.wrapping_add(
                (a.wrapping_add(f).wrapping_add(K[i]).wrapping_add(m[g])).rotate_left(S[i]),
            );
            a = temp;
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut digest = [0u8; 16];
    digest[0..4].copy_from_slice(&a0.to_le_bytes());
    digest[4..8].copy_from_slice(&b0.to_le_bytes());
    digest[8..12].copy_from_slice(&c0.to_le_bytes());
    digest[12..16].copy_from_slice(&d0.to_le_bytes());
    digest
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_empty() {
        let digest = md5_hash(b"");
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn md5_hello() {
        let digest = md5_hash(b"hello");
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn new_filter_empty() {
        let f = CuckooFilter::new(1024, 4, 500);
        assert_eq!(f.num_buckets, 1024);
        assert_eq!(f.bucket_size, 4);
        assert_eq!(f.num_items, 0);
        assert_eq!(f.num_deletes, 0);
        assert_eq!(f.buckets.len(), 1024 * 4 * 2);
    }

    #[test]
    fn add_and_exists() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        assert!(!f.exists(b"hello"));
        assert!(f.add(b"hello").is_ok());
        assert!(f.exists(b"hello"));
    }

    #[test]
    fn add_and_delete() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"hello").unwrap();
        assert_eq!(f.delete(b"hello"), 1);
        assert!(!f.exists(b"hello"));
        assert_eq!(f.num_items, 0);
        assert_eq!(f.num_deletes, 1);
    }

    #[test]
    fn addnx_prevents_duplicate() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        assert_eq!(f.addnx(b"hello").unwrap(), 1);
        assert_eq!(f.addnx(b"hello").unwrap(), 0);
    }

    #[test]
    fn count_tracks_duplicates() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"hello").unwrap();
        f.add(b"hello").unwrap();
        assert_eq!(f.count(b"hello"), 2);
    }

    #[test]
    fn mexists_returns_correct_values() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"a").unwrap();
        f.add(b"b").unwrap();
        let results = f.mexists(&[b"a", b"b", b"c"]);
        assert_eq!(results, vec![1, 1, 0]);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"hello").unwrap();
        f.add(b"world").unwrap();
        f.delete(b"world");

        let bytes = f.serialize();
        let f2 = CuckooFilter::deserialize(&bytes).unwrap();

        assert_eq!(f2.num_buckets, 1024);
        assert_eq!(f2.bucket_size, 4);
        assert_eq!(f2.num_items, 1);
        assert_eq!(f2.num_deletes, 1);
        assert!(f2.exists(b"hello"));
        assert!(!f2.exists(b"world"));
    }

    #[test]
    fn fingerprint_never_zero() {
        let f = CuckooFilter::new(1024, 4, 500);
        // Test many elements to increase chance of hitting the zero case.
        for i in 0..10_000 {
            let fp = f.fingerprint(format!("elem_{i}").as_bytes());
            assert!(!fp.iter().all(|&b| b == 0), "fingerprint was all zeros for elem_{i}");
        }
    }

    #[test]
    fn no_false_negatives() {
        let mut f = CuckooFilter::new(2048, 4, 500);
        for i in 0..200 {
            let elem = format!("element_{i}");
            f.add(elem.as_bytes()).unwrap();
        }
        for i in 0..200 {
            let elem = format!("element_{i}");
            assert!(f.exists(elem.as_bytes()), "false negative for {elem}");
        }
    }

    #[test]
    fn filter_full_returns_error() {
        // Tiny filter: 2 buckets * 2 slots = 4 total slots.
        let mut f = CuckooFilter::new(2, 2, 10);
        let mut inserted = 0;
        for i in 0..100 {
            if f.add(format!("elem_{i}").as_bytes()).is_ok() {
                inserted += 1;
            }
        }
        // Should have filled up before 100 insertions.
        assert!(inserted < 100);
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn delete_nonexistent_returns_zero() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        assert_eq!(f.delete(b"never_added"), 0);
    }

    #[test]
    fn addnx_on_existing_returns_zero() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        assert_eq!(f.addnx(b"x").unwrap(), 1);
        assert_eq!(f.addnx(b"x").unwrap(), 0);
    }

    #[test]
    fn count_for_added_item() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"counted").unwrap();
        assert_eq!(f.count(b"counted"), 1);
    }

    #[test]
    fn count_for_deleted_item() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"temp").unwrap();
        f.delete(b"temp");
        assert_eq!(f.count(b"temp"), 0);
    }

    #[test]
    fn empty_key_works() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        assert!(f.add(b"").is_ok());
        assert!(f.exists(b""));
    }

    #[test]
    fn large_key_1mb() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        let big = vec![0xFFu8; 1_024 * 1_024];
        assert!(f.add(&big).is_ok());
        assert!(f.exists(&big));
    }

    #[test]
    fn fill_to_95_percent_capacity() {
        // 256 buckets * 4 slots = 1024 total slots
        let mut f = CuckooFilter::new(256, 4, 500);
        let target = (1024.0 * 0.95) as usize; // ~972 items
        let mut inserted = 0;
        for i in 0..2000 {
            if f.add(format!("fill_{i}").as_bytes()).is_ok() {
                inserted += 1;
            }
            if inserted >= target {
                break;
            }
        }

        // Measure false positive rate at 95% capacity
        let test_count = 10_000;
        let fp = (0..test_count)
            .filter(|i| f.exists(format!("probe_{i}").as_bytes()))
            .count();
        let fpr = fp as f64 / test_count as f64;
        // Cuckoo filter with 2-byte fingerprints should have FPR ~ 2/(2^16) * bucket_size
        // Expect < 5% even when nearly full
        assert!(
            fpr < 0.05,
            "FPR at 95% capacity: {fpr:.4} (inserted {inserted} items)"
        );
    }

    #[test]
    fn deserialize_truncated_data_returns_error() {
        let f = CuckooFilter::new(1024, 4, 500);
        let bytes = f.serialize();
        // Truncate to just the header
        let truncated = &bytes[..HEADER_SIZE];
        let result = CuckooFilter::deserialize(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_wrong_magic_returns_error() {
        let mut bytes = vec![0xFF, 0xFF]; // wrong magic
        bytes.push(VERSION);
        bytes.extend_from_slice(&[0; HEADER_SIZE - 3]);
        let result = CuckooFilter::deserialize(&bytes);
        assert!(result.is_err());
        match result {
            Err(msg) => assert!(msg.contains("invalid magic"), "wrong error: {msg}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn concurrent_adds_with_mutex() {
        use std::sync::{Arc, Mutex};
        let f = Arc::new(Mutex::new(CuckooFilter::new(4096, 4, 500)));
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let f_clone = Arc::clone(&f);
                std::thread::spawn(move || {
                    for i in 0..250 {
                        let mut guard = f_clone.lock().unwrap();
                        let _ = guard.add(format!("t{t}_k{i}").as_bytes());
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let guard = f.lock().unwrap();
        // All 1000 items should have been attempted
        assert!(guard.num_items > 0);
    }

    // ==================================================================
    // Deep NIF edge cases — targeting cuckoo filter / FFI pitfalls
    // ==================================================================

    #[test]
    fn add_to_completely_full_filter_returns_error() {
        // Tiny filter: 1 bucket * 1 slot = 1 total slot, max_kicks=0
        let mut f = CuckooFilter::new(1, 1, 0);
        // First add should succeed
        let first = f.add(b"first");
        assert!(first.is_ok());
        // Second add must fail (only 1 slot, kicks=0)
        let second = f.add(b"second");
        assert!(second.is_err(), "full filter must return error");
    }

    #[test]
    fn delete_from_empty_filter_returns_zero() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        assert_eq!(f.delete(b"nothing"), 0);
        assert_eq!(f.num_items, 0);
        assert_eq!(f.num_deletes, 0);
    }

    #[test]
    fn addnx_race_condition_mutex_protected() {
        use std::sync::{Arc, Mutex};
        let f = Arc::new(Mutex::new(CuckooFilter::new(4096, 4, 500)));

        // Multiple threads all trying addnx on the same item
        let handles: Vec<_> = (0..20)
            .map(|_| {
                let f_clone = Arc::clone(&f);
                std::thread::spawn(move || {
                    let mut guard = f_clone.lock().unwrap();
                    guard.addnx(b"race_item").unwrap()
                })
            })
            .collect();

        let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // Exactly one thread should have returned 1 (inserted), rest 0
        let inserted_count = results.iter().filter(|&&r| r == 1).count();
        assert_eq!(inserted_count, 1, "exactly one addnx must succeed");
    }

    #[test]
    fn deserialize_all_zeros_returns_error() {
        let zeros = vec![0u8; 100];
        let result = CuckooFilter::deserialize(&zeros);
        assert!(result.is_err(), "all-zero bytes must fail deserialization (bad magic)");
    }

    #[test]
    fn deserialize_truncated_at_every_byte_fuzzlike() {
        let mut f = CuckooFilter::new(64, 4, 500);
        for i in 0..10 {
            f.add(format!("fuzz_{i}").as_bytes()).unwrap();
        }
        let bytes = f.serialize();

        // Try deserializing at every truncation point
        for truncate_at in 0..bytes.len() {
            let truncated = &bytes[..truncate_at];
            let result = CuckooFilter::deserialize(truncated);
            // Must either succeed or return error, never panic
            let _ = result;
        }
    }

    #[test]
    fn serialize_empty_filter_roundtrip() {
        let f = CuckooFilter::new(256, 4, 500);
        let bytes = f.serialize();
        let f2 = CuckooFilter::deserialize(&bytes).unwrap();
        assert_eq!(f2.num_items, 0);
        assert_eq!(f2.num_buckets, 256);
        assert!(!f2.exists(b"anything"));
    }

    #[test]
    fn add_delete_add_same_item() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"cycle").unwrap();
        assert!(f.exists(b"cycle"));
        f.delete(b"cycle");
        assert!(!f.exists(b"cycle"));
        f.add(b"cycle").unwrap();
        assert!(f.exists(b"cycle"));
    }

    #[test]
    fn count_after_multiple_adds_and_deletes() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        f.add(b"x").unwrap();
        f.add(b"x").unwrap();
        f.add(b"x").unwrap();
        assert_eq!(f.count(b"x"), 3);
        f.delete(b"x");
        assert_eq!(f.count(b"x"), 2);
        f.delete(b"x");
        f.delete(b"x");
        assert_eq!(f.count(b"x"), 0);
    }

    #[test]
    fn mexists_empty_list() {
        let f = CuckooFilter::new(1024, 4, 500);
        let results = f.mexists(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn deserialize_wrong_version_returns_error() {
        let mut bytes = vec![MAGIC[0], MAGIC[1]];
        bytes.push(0xFF); // bad version
        bytes.extend_from_slice(&[0; HEADER_SIZE - 3]);
        let result = CuckooFilter::deserialize(&bytes);
        assert!(result.is_err());
    }
}
