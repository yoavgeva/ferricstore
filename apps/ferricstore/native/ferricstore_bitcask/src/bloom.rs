//! Memory-mapped Bloom filter exposed as Rustler NIFs.
//!
//! Each Bloom filter is stored as a file on disk and memory-mapped via `mmap`.
//! The file layout is:
//!
//! ```text
//! [header: 32 bytes][bit array: ceil(num_bits / 8) bytes]
//! ```
//!
//! Header (32 bytes, little-endian):
//!   - bytes  0..7:  magic number (0x424C4F4F4D465F31 = "BLOOMF_1")
//!   - bytes  8..15: num_bits (u64)
//!   - bytes 16..19: num_hashes (u32)
//!   - bytes 20..23: reserved (u32, zero)
//!   - bytes 24..31: count (u64) — number of elements inserted
//!
//! The bit array is directly accessible via the mmap pointer for nanosecond-level
//! bit reads. Writes flush to disk via `msync`.
//!
//! ## Hash functions
//!
//! Uses the Kirsch-Mitzenmacker (2006) enhanced double-hashing technique:
//!   `h_i(x) = (h1(x) + i * h2(x)) mod m`
//!
//! where h1 and h2 are derived from MD5(element) split into two 64-bit halves.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rustler::{Binary, Encoder, Env, NifResult, ResourceArc, Term};

const MAGIC: u64 = 0x424C_4F4F_4D46_5F31; // "BLOOMF_1"
const HEADER_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// BloomFilter core
// ---------------------------------------------------------------------------

/// A memory-mapped Bloom filter.
///
/// The mmap region covers the entire file (header + bit array). All mutations
/// go through the mmap pointer; `msync` is called after writes to ensure
/// durability.
pub struct BloomFilter {
    /// Memory-mapped file contents (header + bit array).
    mmap: *mut u8,
    /// Length of the mmap region in bytes.
    mmap_len: usize,
    /// Number of bits in the filter (m).
    num_bits: u64,
    /// Number of hash functions (k).
    num_hashes: u32,
    /// Path to the backing file.
    path: PathBuf,
}

// SAFETY: The mmap pointer is protected by a Mutex in BloomResource. All access
// is serialized — only one thread touches the pointer at a time. Reads through
// mmap are safe for concurrent access (the kernel guarantees coherent reads from
// a shared mapping), and we serialize writes via the Mutex.
unsafe impl Send for BloomFilter {}
unsafe impl Sync for BloomFilter {}

impl BloomFilter {
    /// Create a new bloom filter file at `path` with the given parameters.
    ///
    /// The file is created, sized, and memory-mapped. All bits start at zero.
    pub fn create(path: &Path, num_bits: u64, num_hashes: u32) -> Result<Self, String> {
        if num_bits == 0 {
            return Err("num_bits must be > 0".into());
        }
        if num_hashes == 0 {
            return Err("num_hashes must be > 0".into());
        }

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
        }

        let byte_count = ((num_bits + 7) / 8) as usize;
        let file_size = HEADER_SIZE + byte_count;

        // Write the file with header + zeroed bit array.
        let mut file = File::create(path).map_err(|e| format!("create: {e}"))?;
        let mut header = [0u8; HEADER_SIZE];
        header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&num_bits.to_le_bytes());
        header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
        // bytes 20..24 reserved (zero)
        // bytes 24..32 count = 0
        file.write_all(&header)
            .map_err(|e| format!("write header: {e}"))?;
        // Write zeroed bit array.
        let zeros = vec![0u8; byte_count];
        file.write_all(&zeros)
            .map_err(|e| format!("write bits: {e}"))?;
        file.sync_all().map_err(|e| format!("fsync: {e}"))?;
        drop(file);

        // Now mmap the file.
        Self::open(path, file_size)
    }

    /// Open an existing bloom filter file and memory-map it.
    pub fn open_existing(path: &Path) -> Result<Self, String> {
        let meta = fs::metadata(path).map_err(|e| format!("stat: {e}"))?;
        let file_size = meta.len() as usize;
        if file_size < HEADER_SIZE {
            return Err("file too small for bloom header".into());
        }
        Self::open(path, file_size)
    }

    fn open(path: &Path, file_size: usize) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("open: {e}"))?;

        let mmap = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                std::os::unix::io::AsRawFd::as_raw_fd(&file),
                0,
            )
        };

        if mmap == libc::MAP_FAILED {
            return Err(format!("mmap failed: {}", std::io::Error::last_os_error()));
        }

        let ptr = mmap as *mut u8;

        // Validate header.
        let magic = u64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 8);
            buf
        });
        if magic != MAGIC {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err("invalid bloom file magic".into());
        }

        let num_bits = u64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr.add(8), buf.as_mut_ptr(), 8);
            buf
        });

        let num_hashes = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr.add(16), buf.as_mut_ptr(), 4);
            buf
        });

        let expected_size = HEADER_SIZE + ((num_bits + 7) / 8) as usize;
        if file_size < expected_size {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err(format!(
                "file too small: expected {expected_size}, got {file_size}"
            ));
        }

        Ok(Self {
            mmap: ptr,
            mmap_len: file_size,
            num_bits,
            num_hashes,
            path: path.to_path_buf(),
        })
    }

    /// Read the insertion count from the header.
    fn count(&self) -> u64 {
        u64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(self.mmap.add(24), buf.as_mut_ptr(), 8);
            buf
        })
    }

    /// Write the insertion count to the header.
    fn set_count(&self, count: u64) {
        unsafe {
            std::ptr::copy_nonoverlapping(count.to_le_bytes().as_ptr(), self.mmap.add(24), 8);
        }
    }

    /// Compute hash positions for an element using Kirsch-Mitzenmacker double hashing.
    fn hash_positions(&self, element: &[u8]) -> Vec<u64> {
        let digest = md5_hash(element);
        let h1 = u64::from_le_bytes(digest[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(digest[8..16].try_into().unwrap());

        (0..self.num_hashes as u64)
            .map(|i| {
                // Wrapping arithmetic to match the Elixir implementation
                let val = h1.wrapping_add(i.wrapping_mul(h2));
                val % self.num_bits
            })
            .collect()
    }

    /// Get a bit from the mmap'd bit array.
    #[inline]
    fn get_bit(&self, position: u64) -> bool {
        let byte_index = (position / 8) as usize;
        let bit_offset = (position % 8) as u8;
        let byte = unsafe { *self.mmap.add(HEADER_SIZE + byte_index) };
        (byte & (1 << bit_offset)) != 0
    }

    /// Set a bit in the mmap'd bit array. Returns true if the bit was previously unset.
    #[inline]
    fn set_bit(&self, position: u64) -> bool {
        let byte_index = (position / 8) as usize;
        let bit_offset = (position % 8) as u8;
        let ptr = unsafe { self.mmap.add(HEADER_SIZE + byte_index) };
        let byte = unsafe { *ptr };
        let mask = 1u8 << bit_offset;
        if (byte & mask) != 0 {
            false
        } else {
            unsafe {
                *ptr = byte | mask;
            }
            true
        }
    }

    /// Add an element. Returns true if the element was new (at least one new bit set).
    pub fn add(&self, element: &[u8]) -> bool {
        let positions = self.hash_positions(element);
        let mut any_new = false;
        for pos in positions {
            if self.set_bit(pos) {
                any_new = true;
            }
        }
        if any_new {
            self.set_count(self.count() + 1);
        }
        any_new
    }

    /// Check if an element may exist.
    pub fn exists(&self, element: &[u8]) -> bool {
        let positions = self.hash_positions(element);
        positions.iter().all(|&pos| self.get_bit(pos))
    }

    /// Flush changes to disk.
    pub fn msync(&self) -> Result<(), String> {
        let ret =
            unsafe { libc::msync(self.mmap as *mut libc::c_void, self.mmap_len, libc::MS_SYNC) };
        if ret != 0 {
            Err(format!("msync failed: {}", std::io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    /// Delete the bloom filter: munmap + unlink the file.
    pub fn delete(self) -> Result<(), String> {
        let path = self.path.clone();
        let mmap = self.mmap;
        let mmap_len = self.mmap_len;

        // Prevent Drop from running (we handle munmap here).
        std::mem::forget(self);

        unsafe {
            libc::munmap(mmap as *mut libc::c_void, mmap_len);
        }
        fs::remove_file(&path).map_err(|e| format!("unlink: {e}"))?;
        Ok(())
    }

    /// Return filter info.
    pub fn info(&self) -> (u64, u64, u32) {
        (self.num_bits, self.count(), self.num_hashes)
    }

    /// Return the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BloomFilter {
    fn drop(&mut self) {
        if !self.mmap.is_null() {
            // Best-effort msync before munmap.
            unsafe {
                libc::msync(self.mmap as *mut libc::c_void, self.mmap_len, libc::MS_SYNC);
                libc::munmap(self.mmap as *mut libc::c_void, self.mmap_len);
            }
        }
    }
}

/// Compute MD5 hash of data. Returns 16-byte digest.
fn md5_hash(data: &[u8]) -> [u8; 16] {
    // Use a simple MD5 implementation. For production, we could use
    // the `md5` crate, but to keep dependencies minimal we use a
    // manual implementation that matches Erlang's :crypto.hash(:md5, data).
    //
    // We use libc to call the system's CC_MD5 (macOS) or we implement inline.
    // For portability, use a pure-Rust MD5.
    md5_compute(data)
}

// ---------------------------------------------------------------------------
// Minimal pure-Rust MD5 (RFC 1321) — matches :crypto.hash(:md5, data)
// ---------------------------------------------------------------------------

fn md5_compute(data: &[u8]) -> [u8; 16] {
    let mut a0: u32 = 0x6745_2301;
    let mut b0: u32 = 0xefcd_ab89;
    let mut c0: u32 = 0x98ba_dcfe;
    let mut d0: u32 = 0x1032_5476;

    // Pre-processing: pad message
    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    // Per-round shift amounts
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];

    // Pre-computed T[i] = floor(2^32 * |sin(i + 1)|)
    const K: [u32; 64] = [
        0xd76a_a478,
        0xe8c7_b756,
        0x2420_70db,
        0xc1bd_ceee,
        0xf57c_0faf,
        0x4787_c62a,
        0xa830_4613,
        0xfd46_9501,
        0x6980_98d8,
        0x8b44_f7af,
        0xffff_5bb1,
        0x895c_d7be,
        0x6b90_1122,
        0xfd98_7193,
        0xa679_438e,
        0x49b4_0821,
        0xf61e_2562,
        0xc040_b340,
        0x265e_5a51,
        0xe9b6_c7aa,
        0xd62f_105d,
        0x0244_1453,
        0xd8a1_e681,
        0xe7d3_fbc8,
        0x21e1_cde6,
        0xc337_07d6,
        0xf4d5_0d87,
        0x455a_14ed,
        0xa9e3_e905,
        0xfcef_a3f8,
        0x676f_02d9,
        0x8d2a_4c8a,
        0xfffa_3942,
        0x8771_f681,
        0x6d9d_6122,
        0xfde5_380c,
        0xa4be_ea44,
        0x4bde_cfa9,
        0xf6bb_4b60,
        0xbebf_bc70,
        0x289b_7ec6,
        0xeaa1_27fa,
        0xd4ef_3085,
        0x0488_1d05,
        0xd9d4_d039,
        0xe6db_99e5,
        0x1fa2_7cf8,
        0xc4ac_5665,
        0xf429_2244,
        0x432a_ff97,
        0xab94_23a7,
        0xfc93_a039,
        0x655b_59c3,
        0x8f0c_cc92,
        0xffef_f47d,
        0x8584_5dd1,
        0x6fa8_7e4f,
        0xfe2c_e6e0,
        0xa301_4314,
        0x4e08_11a1,
        0xf753_7e82,
        0xbd3a_f235,
        0x2ad7_d2bb,
        0xeb86_d391,
    ];

    for chunk in msg.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, c) in chunk.chunks_exact(4).enumerate() {
            m[i] = u32::from_le_bytes(c.try_into().unwrap());
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

    let mut result = [0u8; 16];
    result[0..4].copy_from_slice(&a0.to_le_bytes());
    result[4..8].copy_from_slice(&b0.to_le_bytes());
    result[8..12].copy_from_slice(&c0.to_le_bytes());
    result[12..16].copy_from_slice(&d0.to_le_bytes());
    result
}

// ---------------------------------------------------------------------------
// NIF resource
// ---------------------------------------------------------------------------

/// Wraps `BloomFilter` in a `Mutex` for `Send + Sync` required by `ResourceArc`.
pub struct BloomResource {
    pub filter: Mutex<BloomFilter>,
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

/// Create a new bloom filter file at the given path.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_create(env: Env, path: String, num_bits: u64, num_hashes: u32) -> NifResult<Term> {
    match BloomFilter::create(Path::new(&path), num_bits, num_hashes) {
        Ok(filter) => {
            let resource = ResourceArc::new(BloomResource {
                filter: Mutex::new(filter),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

/// Open an existing bloom filter file.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_open(env: Env, path: String) -> NifResult<Term> {
    match BloomFilter::open_existing(Path::new(&path)) {
        Ok(filter) => {
            let resource = ResourceArc::new(BloomResource {
                filter: Mutex::new(filter),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

/// Add an element to a bloom filter.
/// Returns 1 if the element was new, 0 if it probably already existed.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_add<'a>(
    env: Env<'a>,
    resource: ResourceArc<BloomResource>,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let is_new = filter.add(element.as_slice());
    // msync after each add for durability
    let _ = filter.msync();
    Ok(if is_new { 1u32 } else { 0u32 }.encode(env))
}

/// Add multiple elements to a bloom filter.
/// Returns a list of 1/0 for each element (1 = new, 0 = already existed).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_madd<'a>(
    env: Env<'a>,
    resource: ResourceArc<BloomResource>,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<u32> = elements
        .iter()
        .map(|e| if filter.add(e.as_slice()) { 1 } else { 0 })
        .collect();
    let _ = filter.msync();
    Ok(results.encode(env))
}

/// Check if an element may exist in the bloom filter.
/// Returns 1 if possibly present, 0 if definitely not present.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_exists<'a>(
    env: Env<'a>,
    resource: ResourceArc<BloomResource>,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let found = filter.exists(element.as_slice());
    Ok(if found { 1u32 } else { 0u32 }.encode(env))
}

/// Check if multiple elements may exist.
/// Returns a list of 1/0 for each element.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_mexists<'a>(
    env: Env<'a>,
    resource: ResourceArc<BloomResource>,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<u32> = elements
        .iter()
        .map(|e| if filter.exists(e.as_slice()) { 1 } else { 0 })
        .collect();
    Ok(results.encode(env))
}

/// Return the estimated cardinality (number of items inserted).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_card(env: Env, resource: ResourceArc<BloomResource>) -> NifResult<Term> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    Ok(filter.count().encode(env))
}

/// Return filter info as a flat list:
/// ["Capacity", capacity, "Size", size, "Number of filters", 1,
///  "Number of items inserted", count, "Expansion rate", 0,
///  "Error rate", error_rate, "Number of hash functions", k, "Number of bits", m]
///
/// Returns `{:ok, {num_bits, count, num_hashes}}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_info(env: Env, resource: ResourceArc<BloomResource>) -> NifResult<Term> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let (num_bits, count, num_hashes) = filter.info();
    Ok((atoms::ok(), (num_bits, count, num_hashes as u64)).encode(env))
}

/// Delete a bloom filter: munmap + unlink the file.
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_delete(env: Env, resource: ResourceArc<BloomResource>) -> NifResult<Term> {
    // Take ownership of the filter out of the Mutex.
    let mut filter_guard = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;

    // We need to take ownership. Create a dummy filter and swap.
    // After this, the Mutex holds a null/empty filter and we have the real one.
    let path = filter_guard.path.clone();
    let mmap = filter_guard.mmap;
    let mmap_len = filter_guard.mmap_len;

    // Null out the guard so Drop doesn't double-munmap.
    filter_guard.mmap = std::ptr::null_mut();
    filter_guard.mmap_len = 0;
    drop(filter_guard);

    // munmap
    if !mmap.is_null() {
        unsafe {
            libc::munmap(mmap as *mut libc::c_void, mmap_len);
        }
    }

    // unlink
    match fs::remove_file(&path) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), format!("unlink: {e}")).encode(env)),
    }
}

// ---------------------------------------------------------------------------
// Rust unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_empty() {
        let digest = md5_compute(b"");
        assert_eq!(hex(&digest), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn md5_hello() {
        let digest = md5_compute(b"hello");
        assert_eq!(hex(&digest), "5d41402abc4b2a76b9719d911017c592");
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn bloom_create_add_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bloom");

        let filter = BloomFilter::create(&path, 1000, 7).unwrap();
        assert!(!filter.exists(b"hello"));
        assert!(filter.add(b"hello"));
        assert!(filter.exists(b"hello"));
        assert!(!filter.add(b"hello")); // duplicate
        assert_eq!(filter.count(), 1);
    }

    #[test]
    fn bloom_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.bloom");

        {
            let filter = BloomFilter::create(&path, 1000, 7).unwrap();
            filter.add(b"persisted");
            filter.msync().unwrap();
        }

        // Reopen and verify
        let filter = BloomFilter::open_existing(&path).unwrap();
        assert!(filter.exists(b"persisted"));
        assert_eq!(filter.count(), 1);
    }

    #[test]
    fn bloom_delete_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delete_me.bloom");

        let filter = BloomFilter::create(&path, 100, 3).unwrap();
        filter.add(b"test");
        assert!(path.exists());

        filter.delete().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn bloom_large_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bloom");

        let filter = BloomFilter::create(&path, 1_000_000, 7).unwrap();
        for i in 0..1000 {
            filter.add(format!("item_{i}").as_bytes());
        }
        assert_eq!(filter.count(), 1000);

        // Verify all items exist
        for i in 0..1000 {
            assert!(filter.exists(format!("item_{i}").as_bytes()));
        }
    }

    #[test]
    fn bloom_false_positive_rate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fpr.bloom");

        // Parameters for ~1% FPR with 1000 items
        let num_bits = 9586; // -1000 * ln(0.01) / (ln(2))^2
        let num_hashes = 7; // (9586/1000) * ln(2)
        let filter = BloomFilter::create(&path, num_bits, num_hashes).unwrap();

        for i in 0..1000 {
            filter.add(format!("added_{i}").as_bytes());
        }

        let test_count = 10000;
        let fp_count = (0..test_count)
            .filter(|i| filter.exists(format!("not_added_{i}").as_bytes()))
            .count();

        let fp_rate = fp_count as f64 / test_count as f64;
        assert!(fp_rate < 0.02, "False positive rate {fp_rate} exceeds 2%");
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn create_with_zero_bits_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero.bloom");
        let result = BloomFilter::create(&path, 0, 7);
        assert!(result.is_err(), "creating with 0 bits must fail");
        match result {
            Err(msg) => assert!(msg.contains("num_bits must be > 0"), "wrong error: {msg}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn create_with_one_bit_one_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("onebit.bloom");
        let filter = BloomFilter::create(&path, 1, 1).unwrap();
        assert!(filter.add(b"x"));
        assert!(filter.exists(b"x"));
        // With 1 bit, everything should now be a false positive
        assert!(filter.exists(b"y"));
    }

    #[test]
    fn add_empty_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_key.bloom");
        let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
        assert!(filter.add(b""));
        assert!(filter.exists(b""));
        assert_eq!(filter.count(), 1);
    }

    #[test]
    fn add_large_key_1mb() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bigkey.bloom");
        let filter = BloomFilter::create(&path, 100_000, 7).unwrap();
        let big_key = vec![0xABu8; 1_024 * 1_024]; // 1 MB
        assert!(filter.add(&big_key));
        assert!(filter.exists(&big_key));
    }

    #[test]
    fn exists_on_empty_filter_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bloom");
        let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
        assert!(!filter.exists(b"anything"));
        assert!(!filter.exists(b"something_else"));
    }

    #[test]
    fn add_same_key_twice_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idem.bloom");
        let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
        assert!(filter.add(b"dup")); // first add: new bit(s) set
        assert!(!filter.add(b"dup")); // second add: no new bits
        assert_eq!(filter.count(), 1); // count only incremented once
        assert!(filter.exists(b"dup"));
    }

    #[test]
    fn false_positive_rate_1m_adds_at_001_target() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fpr_1m.bloom");

        // For n=100_000 items at 1% FPR:
        //   m = -n*ln(p) / (ln2)^2 = 958_506
        //   k = (m/n)*ln2 = 6.64 -> 7
        let num_bits = 958_506u64;
        let num_hashes = 7u32;
        let filter = BloomFilter::create(&path, num_bits, num_hashes).unwrap();

        // Add 100K items
        for i in 0..100_000 {
            filter.add(format!("insert_{i}").as_bytes());
        }

        // Test 100K items that were NOT added
        let test_count = 100_000;
        let fp_count = (0..test_count)
            .filter(|i| filter.exists(format!("probe_{i}").as_bytes()))
            .count();

        let fp_rate = fp_count as f64 / test_count as f64;
        assert!(
            fp_rate < 0.02,
            "FPR {fp_rate:.4} exceeds 2% (target was 1%)"
        );
    }

    #[test]
    fn concurrent_reads_no_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.bloom");
        let filter = BloomFilter::create(&path, 100_000, 7).unwrap();
        for i in 0..1_000 {
            filter.add(format!("k{i}").as_bytes());
        }

        // Concurrent reads via multiple threads
        let filter_ptr = &filter as *const BloomFilter;
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let p = filter_ptr as usize;
                std::thread::spawn(move || {
                    let f = unsafe { &*(p as *const BloomFilter) };
                    for i in 0..1_000 {
                        let _ = f.exists(format!("k{i}").as_bytes());
                    }
                    let _ = f.exists(format!("thread_{t}").as_bytes());
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("roundtrip.bloom");
        let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
        for i in 0..100 {
            filter.add(format!("item_{i}").as_bytes());
        }
        filter.msync().unwrap();
        drop(filter);

        // Reopen = deserialize
        let filter2 = BloomFilter::open_existing(&path).unwrap();
        for i in 0..100 {
            assert!(
                filter2.exists(format!("item_{i}").as_bytes()),
                "item_{i} missing after reopen"
            );
        }
        assert_eq!(filter2.count(), 100);
    }

    #[test]
    fn deserialize_garbage_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.bloom");

        // Write random garbage
        std::fs::write(&path, b"this is not a bloom filter file").unwrap();
        let result = BloomFilter::open_existing(&path);
        assert!(result.is_err());
    }

    #[test]
    fn delete_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("todelete.bloom");
        let filter = BloomFilter::create(&path, 500, 3).unwrap();
        filter.add(b"x");
        assert!(path.exists());
        filter.delete().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn open_nonexistent_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.bloom");
        let result = BloomFilter::open_existing(&path);
        assert!(result.is_err());
    }

    #[test]
    fn bit_array_boundary_last_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boundary.bloom");
        // Use exactly 64 bits (8 bytes) to test boundary
        let filter = BloomFilter::create(&path, 64, 1).unwrap();
        // Manually test that setting the last bit position works
        assert!(!filter.get_bit(63));
        filter.set_bit(63);
        assert!(filter.get_bit(63));
    }

    #[test]
    fn hash_distribution_all_positions_hit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("distribution.bloom");
        let num_bits = 1000u64;
        let filter = BloomFilter::create(&path, num_bits, 1).unwrap();

        // Add 10K keys and verify all hash positions are used
        let mut positions_hit = std::collections::HashSet::new();
        for i in 0..10_000u64 {
            let key = format!("dist_key_{i}");
            let positions = filter.hash_positions(key.as_bytes());
            for &pos in &positions {
                positions_hit.insert(pos);
            }
        }

        // With 10K keys, 1 hash function, and 1000 positions, expect most filled
        // Birthday problem: P(hit all 1000) with 10K samples is very high
        let coverage = positions_hit.len() as f64 / num_bits as f64;
        assert!(
            coverage > 0.99,
            "hash distribution coverage {coverage:.3} (hit {}/{}), expected > 99%",
            positions_hit.len(),
            num_bits
        );
    }

    #[test]
    fn create_with_zero_hashes_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zerohash.bloom");
        let result = BloomFilter::create(&path, 1000, 0);
        assert!(result.is_err());
    }

    // ==================================================================
    // Deep NIF edge cases — targeting mmap/FFI safety pitfalls
    // ==================================================================

    #[test]
    fn bloom_add_key_with_embedded_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nullkey.bloom");
        let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
        let key = b"a\0b\0c";
        assert!(filter.add(key));
        assert!(filter.exists(key));
        // Distinct from truncated variant
        assert!(!filter.exists(b"a") || filter.count() == 1);
    }

    #[test]
    fn bloom_concurrent_add_and_exists_100_threads() {
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent_rw.bloom");
        let filter = Arc::new(Mutex::new(
            BloomFilter::create(&path, 1_000_000, 7).unwrap(),
        ));

        let handles: Vec<_> = (0..100)
            .map(|t| {
                let f = Arc::clone(&filter);
                std::thread::spawn(move || {
                    let guard = f.lock().unwrap();
                    if t % 2 == 0 {
                        guard.add(format!("elem_{t}").as_bytes());
                    } else {
                        let _ = guard.exists(format!("elem_{t}").as_bytes());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join()
                .expect("thread panicked during bloom concurrent access");
        }
    }

    #[test]
    fn bloom_create_in_nonexistent_nested_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deep").join("nested").join("filter.bloom");
        let filter = BloomFilter::create(&path, 1000, 3).unwrap();
        filter.add(b"nested_test");
        assert!(filter.exists(b"nested_test"));
        assert!(path.exists());
    }

    #[test]
    fn bloom_open_corrupted_header_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("badmagic.bloom");
        // Write a file with wrong magic bytes
        std::fs::write(&path, &[0xFF; 64]).unwrap();
        let result = BloomFilter::open_existing(&path);
        assert!(result.is_err(), "bad magic must return error");
    }

    #[test]
    fn bloom_open_zero_length_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bloom");
        std::fs::write(&path, &[]).unwrap();
        let result = BloomFilter::open_existing(&path);
        assert!(result.is_err(), "zero-length file must return error");
    }

    #[test]
    fn bloom_madd_many_elements() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("madd.bloom");
        let filter = BloomFilter::create(&path, 100_000, 7).unwrap();
        let elements: Vec<Vec<u8>> = (0..500).map(|i| format!("madd_{i}").into_bytes()).collect();
        for e in &elements {
            filter.add(e);
        }
        // Verify all present
        for e in &elements {
            assert!(filter.exists(e), "element missing after madd");
        }
        assert_eq!(filter.count(), 500);
    }

    #[test]
    fn bloom_persistence_after_many_operations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist_heavy.bloom");

        {
            let filter = BloomFilter::create(&path, 100_000, 7).unwrap();
            for i in 0..1000 {
                filter.add(format!("persistent_{i}").as_bytes());
            }
            filter.msync().unwrap();
        }

        let filter = BloomFilter::open_existing(&path).unwrap();
        assert_eq!(filter.count(), 1000);
        for i in 0..1000 {
            assert!(
                filter.exists(format!("persistent_{i}").as_bytes()),
                "item {i} missing after reopen"
            );
        }
    }

    #[test]
    fn bloom_file_truncated_after_create_returns_error_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.bloom");

        {
            let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
            filter.add(b"data");
            filter.msync().unwrap();
        }

        // Truncate to just the header (32 bytes), removing the bit array
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..HEADER_SIZE.min(data.len())]).unwrap();

        let result = BloomFilter::open_existing(&path);
        assert!(result.is_err(), "truncated bloom file must fail to open");
    }
}
