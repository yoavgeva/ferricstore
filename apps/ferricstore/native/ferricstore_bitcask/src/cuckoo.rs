//! Cuckoo filter implementation for FerricStore.
//!
//! A space-efficient probabilistic data structure similar to Bloom filters,
//! but supporting deletion and approximate counting. Stores fingerprints of
//! elements in a hash table with two candidate bucket positions per element.
//!
//! ## Storage modes
//!
//! - **In-memory** (`cuckoo_create`): backed by a `Vec<u8>`, suitable for
//!   ephemeral filters or Bitcask serialize/deserialize.
//! - **Mmap file** (`cuckoo_create_file` / `cuckoo_open_file`): backed by
//!   `libc::mmap()` on a file, persistent across restarts.
//!
//! ## File layout (mmap mode)
//!
//! ```text
//! [magic: 2B][version: 1B][capacity: 4B][bucket_size: 1B]
//! [fingerprint_size: 1B][max_kicks: 2B][num_items: 8B][num_deletes: 8B]
//! [buckets: capacity * bucket_size * fingerprint_size bytes]
//! ```
//!
//! Total header size: 27 bytes (same as serialization format).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
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
// Storage enum — in-memory Vec or mmap'd file
// ---------------------------------------------------------------------------

/// Backing storage for the cuckoo filter's bucket array.
enum Storage {
    /// In-memory storage backed by a heap-allocated Vec.
    InMemory(Vec<u8>),
    /// Memory-mapped file storage.
    Mmap {
        /// Pointer to the start of the mmap'd region (header + buckets).
        ptr: *mut u8,
        /// Total length of the mmap'd region in bytes.
        len: usize,
        /// File descriptor (kept open for the lifetime of the mapping).
        fd: i32,
        /// Path to the backing file (retained for diagnostics).
        _path: PathBuf,
    },
}

// SAFETY: The mmap pointer is protected by a Mutex in CuckooResource. All
// access is serialized — only one thread touches the pointer at a time.
unsafe impl Send for Storage {}
unsafe impl Sync for Storage {}

impl Storage {
    /// Get a pointer to the start of the bucket data (past the header).
    fn bucket_ptr(&self) -> *const u8 {
        match self {
            Storage::InMemory(v) => v.as_ptr(),
            Storage::Mmap { ptr, .. } => unsafe { ptr.add(HEADER_SIZE) },
        }
    }

    /// Get a mutable pointer to the start of the bucket data.
    fn bucket_ptr_mut(&mut self) -> *mut u8 {
        match self {
            Storage::InMemory(v) => v.as_mut_ptr(),
            Storage::Mmap { ptr, .. } => unsafe { ptr.add(HEADER_SIZE) },
        }
    }

    /// Get a byte slice of the bucket data.
    fn bucket_slice(&self, len: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.bucket_ptr(), len) }
    }

    /// Get a mutable byte slice of the bucket data.
    fn bucket_slice_mut(&mut self, len: usize) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.bucket_ptr_mut(), len) }
    }

    /// Whether this is mmap-backed.
    fn is_mmap(&self) -> bool {
        matches!(self, Storage::Mmap { .. })
    }

    /// Call msync(MS_ASYNC) if mmap-backed.
    fn msync_async(&self) {
        if let Storage::Mmap { ptr, len, .. } = self {
            unsafe {
                libc::msync(*ptr as *mut libc::c_void, *len, libc::MS_ASYNC);
            }
        }
    }

    /// Call msync(MS_SYNC) if mmap-backed.
    #[cfg(test)]
    fn msync_sync(&self) {
        if let Storage::Mmap { ptr, len, .. } = self {
            unsafe {
                libc::msync(*ptr as *mut libc::c_void, *len, libc::MS_SYNC);
            }
        }
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        if let Storage::Mmap { ptr, len, fd, .. } = self {
            if !ptr.is_null() {
                unsafe {
                    libc::msync(*ptr as *mut libc::c_void, *len, libc::MS_SYNC);
                    libc::munmap(*ptr as *mut libc::c_void, *len);
                    libc::close(*fd);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CuckooFilter
// ---------------------------------------------------------------------------

/// A cuckoo filter storing fingerprints in a flat bucket array.
pub struct CuckooFilter {
    /// Backing storage (in-memory Vec or mmap'd file).
    storage: Storage,
    /// Number of buckets.
    num_buckets: usize,
    /// Number of fingerprint slots per bucket.
    bucket_size: usize,
    /// Fingerprint size in bytes.
    fingerprint_size: usize,
    /// Number of items currently inserted (only used for in-memory mode;
    /// for mmap mode, read/written from the header).
    num_items: u64,
    /// Number of items that have been deleted (same caveat as num_items).
    num_deletes: u64,
    /// Maximum kick attempts before declaring the filter full.
    max_kicks: u32,
}

impl CuckooFilter {
    /// Create a new empty cuckoo filter with the given capacity (number of buckets).
    /// Uses in-memory storage.
    #[must_use]
    pub fn new(capacity: usize, bucket_size: usize, max_kicks: u32) -> Self {
        let fingerprint_size = DEFAULT_FINGERPRINT_SIZE;
        let total_bytes = capacity * bucket_size * fingerprint_size;
        Self {
            storage: Storage::InMemory(vec![0u8; total_bytes]),
            num_buckets: capacity,
            bucket_size,
            fingerprint_size,
            num_items: 0,
            num_deletes: 0,
            max_kicks,
        }
    }

    /// Create a new mmap-backed cuckoo filter file at `path`.
    pub fn create_file(path: &Path, capacity: usize, bucket_size: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("capacity must be > 0".into());
        }
        if bucket_size == 0 {
            return Err("bucket_size must be > 0".into());
        }

        let fingerprint_size = DEFAULT_FINGERPRINT_SIZE;
        let max_kicks = DEFAULT_MAX_KICKS;
        let bucket_bytes = capacity * bucket_size * fingerprint_size;
        let file_size = HEADER_SIZE + bucket_bytes;

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
        }

        // Write the file: header + zeroed buckets.
        let mut file = File::create(path).map_err(|e| format!("create: {e}"))?;
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&(capacity as u32).to_le_bytes());
        header[7] = bucket_size as u8;
        header[8] = fingerprint_size as u8;
        header[9..11].copy_from_slice(&(max_kicks as u16).to_le_bytes());
        // num_items (8B) = 0 at bytes 11..19
        // num_deletes (8B) = 0 at bytes 19..27
        file.write_all(&header)
            .map_err(|e| format!("write header: {e}"))?;
        let zeros = vec![0u8; bucket_bytes];
        file.write_all(&zeros)
            .map_err(|e| format!("write buckets: {e}"))?;
        file.sync_all().map_err(|e| format!("fsync: {e}"))?;
        drop(file);

        // Now mmap the file.
        Self::open_mmap(path, file_size)
    }

    /// Open an existing mmap-backed cuckoo filter file.
    pub fn open_file(path: &Path) -> Result<Self, String> {
        let meta = fs::metadata(path).map_err(|e| format!("stat: {e}"))?;
        let file_size = meta.len() as usize;
        if file_size < HEADER_SIZE {
            return Err("file too small for cuckoo header".into());
        }
        Self::open_mmap(path, file_size)
    }

    /// Internal: mmap an existing cuckoo file and validate its header.
    fn open_mmap(path: &Path, file_size: usize) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("open: {e}"))?;

        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&file);

        let mmap = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if mmap == libc::MAP_FAILED {
            return Err(format!("mmap failed: {}", std::io::Error::last_os_error()));
        }

        let ptr = mmap as *mut u8;

        // Validate magic.
        let magic = unsafe {
            let mut buf = [0u8; 2];
            std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 2);
            buf
        };
        if magic != MAGIC {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err("invalid cuckoo file magic".into());
        }

        // Validate version.
        let version = unsafe { *ptr.add(2) };
        if version != VERSION {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err(format!("unsupported cuckoo version {version}"));
        }

        // Read header fields.
        let num_buckets = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr.add(3), buf.as_mut_ptr(), 4);
            buf
        }) as usize;

        let bucket_size = unsafe { *ptr.add(7) } as usize;
        let fingerprint_size = unsafe { *ptr.add(8) } as usize;

        let max_kicks = u16::from_le_bytes(unsafe {
            let mut buf = [0u8; 2];
            std::ptr::copy_nonoverlapping(ptr.add(9), buf.as_mut_ptr(), 2);
            buf
        }) as u32;

        let num_items = u64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr.add(11), buf.as_mut_ptr(), 8);
            buf
        });

        let num_deletes = u64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr.add(19), buf.as_mut_ptr(), 8);
            buf
        });

        let expected_size = HEADER_SIZE + num_buckets * bucket_size * fingerprint_size;
        if file_size < expected_size {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err(format!(
                "file too small: expected {expected_size}, got {file_size}"
            ));
        }

        // Hint the kernel that access is random (hash-determined bucket positions).
        unsafe {
            libc::madvise(mmap, file_size, libc::MADV_RANDOM);
        }

        // Dup the fd so we own it independently of the File object.
        let owned_fd = unsafe { libc::dup(fd) };
        if owned_fd < 0 {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err(format!(
                "dup fd failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        // Drop the File — we keep owned_fd.
        drop(file);

        Ok(Self {
            storage: Storage::Mmap {
                ptr,
                len: file_size,
                fd: owned_fd,
                _path: path.to_path_buf(),
            },
            num_buckets,
            bucket_size,
            fingerprint_size,
            num_items,
            num_deletes,
            max_kicks,
        })
    }

    /// Close an mmap-backed filter (munmap + close fd). For in-memory, this is a no-op.
    pub fn close(&mut self) {
        // Write header counters before extracting fields.
        self.write_header_counters();
        if let Storage::Mmap { ptr, len, fd, .. } = &mut self.storage {
            if !ptr.is_null() {
                unsafe {
                    libc::msync(*ptr as *mut libc::c_void, *len, libc::MS_SYNC);
                    libc::munmap(*ptr as *mut libc::c_void, *len);
                    libc::close(*fd);
                }
                *ptr = std::ptr::null_mut();
            }
        }
    }

    /// Write num_items and num_deletes to the mmap header.
    fn write_header_counters(&self) {
        if let Storage::Mmap { ptr, .. } = &self.storage {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.num_items.to_le_bytes().as_ptr(),
                    ptr.add(11),
                    8,
                );
                std::ptr::copy_nonoverlapping(
                    self.num_deletes.to_le_bytes().as_ptr(),
                    ptr.add(19),
                    8,
                );
            }
        }
    }

    /// Total number of bytes in the bucket array.
    fn bucket_data_len(&self) -> usize {
        self.num_buckets * self.bucket_size * self.fingerprint_size
    }

    /// Compute a 16-byte hash of `data` using xxh3.
    fn hash128(data: &[u8]) -> [u8; 16] {
        let h = xxhash_rust::xxh3::xxh3_128(data);
        h.to_le_bytes()
    }

    /// Compute the fingerprint and primary bucket from a single hash of the element.
    fn fingerprint_and_bucket(&self, element: &[u8]) -> (Vec<u8>, usize) {
        let hash = Self::hash128(element);

        // Fingerprint: first `fingerprint_size` bytes, ensuring non-zero.
        let mut fp = hash[..self.fingerprint_size].to_vec();
        if fp.iter().all(|&b| b == 0) {
            fp[0] = 1;
        }

        // Primary bucket: next 8 bytes after fingerprint.
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
        let bucket = (hash_val as usize) % self.num_buckets;

        (fp, bucket)
    }

    /// Compute the fingerprint for an element (standalone, used in tests).
    #[cfg(test)]
    fn fingerprint(&self, element: &[u8]) -> Vec<u8> {
        let (fp, _) = self.fingerprint_and_bucket(element);
        fp
    }

    /// Compute the alternate bucket: `bucket XOR hash(fingerprint)`.
    fn alternate_bucket(&self, bucket: usize, fp: &[u8]) -> usize {
        let hash = Self::hash128(fp);
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
        let data = self.storage.bucket_slice(self.bucket_data_len());
        &data[offset..offset + self.fingerprint_size]
    }

    /// Write a fingerprint at a given bucket and slot.
    fn set_slot(&mut self, bucket_idx: usize, slot_idx: usize, fp: &[u8]) {
        let offset = self.slot_offset(bucket_idx, slot_idx);
        let fp_size = self.fingerprint_size;
        let data_len = self.bucket_data_len();
        let data = self.storage.bucket_slice_mut(data_len);
        data[offset..offset + fp_size].copy_from_slice(fp);
    }

    /// Check if a slot is empty (all zeros).
    fn is_slot_empty(&self, bucket_idx: usize, slot_idx: usize) -> bool {
        self.get_slot(bucket_idx, slot_idx).iter().all(|&b| b == 0)
    }

    /// Find the first empty slot in a bucket. Returns `Some(slot_idx)` or `None`.
    fn find_empty_slot(&self, bucket_idx: usize) -> Option<usize> {
        (0..self.bucket_size).find(|&slot| self.is_slot_empty(bucket_idx, slot))
    }

    /// Add an element to the filter. Returns `Ok(())` on success, `Err(())` if full.
    #[allow(clippy::result_unit_err)]
    pub fn add(&mut self, element: &[u8]) -> Result<(), ()> {
        let (fp, b1) = self.fingerprint_and_bucket(element);
        let b2 = self.alternate_bucket(b1, &fp);

        // Try primary bucket.
        if let Some(slot) = self.find_empty_slot(b1) {
            self.set_slot(b1, slot, &fp);
            self.num_items += 1;
            self.flush_counters();
            return Ok(());
        }

        // Try alternate bucket.
        if let Some(slot) = self.find_empty_slot(b2) {
            self.set_slot(b2, slot, &fp);
            self.num_items += 1;
            self.flush_counters();
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
                self.flush_counters();
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
    #[allow(clippy::result_unit_err)]
    pub fn addnx(&mut self, element: &[u8]) -> Result<u64, ()> {
        if self.exists(element) {
            return Ok(0);
        }
        self.add(element)?;
        Ok(1)
    }

    /// Check if an element may exist in the filter.
    pub fn exists(&self, element: &[u8]) -> bool {
        let (fp, b1) = self.fingerprint_and_bucket(element);
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
        elements.iter().map(|e| u64::from(self.exists(e))).collect()
    }

    /// Delete one occurrence of an element. Returns 1 if deleted, 0 if not found.
    pub fn delete(&mut self, element: &[u8]) -> u64 {
        let (fp, b1) = self.fingerprint_and_bucket(element);
        let b2 = self.alternate_bucket(b1, &fp);

        let empty_buf = [0u8; 8]; // fingerprint_size is always <= 8
        let empty = &empty_buf[..self.fingerprint_size];

        // Try primary bucket first.
        for slot in 0..self.bucket_size {
            if self.get_slot(b1, slot) == fp.as_slice() {
                self.set_slot(b1, slot, empty);
                self.num_items -= 1;
                self.num_deletes += 1;
                self.flush_counters();
                return 1;
            }
        }

        // Try alternate bucket.
        for slot in 0..self.bucket_size {
            if self.get_slot(b2, slot) == fp.as_slice() {
                self.set_slot(b2, slot, empty);
                self.num_items -= 1;
                self.num_deletes += 1;
                self.flush_counters();
                return 1;
            }
        }

        0
    }

    /// Count occurrences of an element's fingerprint across both candidate buckets.
    pub fn count(&self, element: &[u8]) -> u64 {
        let (fp, b1) = self.fingerprint_and_bucket(element);
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

    /// Flush header counters and msync_async after writes (mmap mode only).
    fn flush_counters(&mut self) {
        if self.storage.is_mmap() {
            self.write_header_counters();
            self.storage.msync_async();
        }
    }

    /// Serialize the filter to a byte array for Bitcask storage.
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let data = self.storage.bucket_slice(self.bucket_data_len());
        let mut buf = Vec::with_capacity(HEADER_SIZE + data.len());

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
        buf.extend_from_slice(data);

        buf
    }

    /// Deserialize a filter from a byte array (in-memory mode).
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

        let num_buckets = u32::from_le_bytes([data[3], data[4], data[5], data[6]]) as usize;
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
            storage: Storage::InMemory(buckets),
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
// NIF functions — in-memory
// ---------------------------------------------------------------------------

/// Create a new cuckoo filter (in-memory).
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_create(
    env: Env,
    capacity: u64,
    bucket_size: u64,
    max_kicks: u64,
    expansion: u64,
) -> NifResult<Term> {
    let _ = expansion; // Reserved for future use; NIF API requires the parameter.
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

// ---------------------------------------------------------------------------
// NIF functions — mmap file-backed
// ---------------------------------------------------------------------------

/// Create a new mmap-backed cuckoo filter file.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_create_file(
    env: Env,
    path: String,
    capacity: u64,
    bucket_size: u64,
) -> NifResult<Term> {
    let bs = if bucket_size == 0 {
        DEFAULT_BUCKET_SIZE
    } else {
        bucket_size as usize
    };

    match CuckooFilter::create_file(Path::new(&path), capacity as usize, bs) {
        Ok(filter) => {
            let resource = ResourceArc::new(CuckooResource {
                filter: Mutex::new(filter),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

/// Open an existing mmap-backed cuckoo filter file.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_open_file(env: Env, path: String) -> NifResult<Term> {
    match CuckooFilter::open_file(Path::new(&path)) {
        Ok(filter) => {
            let resource = ResourceArc::new(CuckooResource {
                filter: Mutex::new(filter),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

/// Close an mmap-backed cuckoo filter (munmap + close fd).
/// Returns `:ok`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_close(env: Env, resource: ResourceArc<CuckooResource>) -> NifResult<Term> {
    let mut filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    filter.close();
    Ok(atoms::ok().encode(env))
}

// ---------------------------------------------------------------------------
// NIF functions — operations (work on both in-memory and mmap)
// ---------------------------------------------------------------------------

/// Add an element to the filter.
/// Returns `:ok` or `{:error, "filter is full"}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_add<'a>(
    env: Env<'a>,
    resource: ResourceArc<CuckooResource>,
    item: Binary<'a>,
) -> NifResult<Term<'a>> {
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
pub fn cuckoo_addnx<'a>(
    env: Env<'a>,
    resource: ResourceArc<CuckooResource>,
    item: Binary<'a>,
) -> NifResult<Term<'a>> {
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
pub fn cuckoo_del<'a>(
    env: Env<'a>,
    resource: ResourceArc<CuckooResource>,
    item: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let result = filter.delete(item.as_slice());
    Ok(result.encode(env))
}

/// Check if an element may exist.
/// Returns 0 or 1.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_exists<'a>(
    env: Env<'a>,
    resource: ResourceArc<CuckooResource>,
    item: Binary<'a>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let result: u64 = u64::from(filter.exists(item.as_slice()));
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
    let slices: Vec<&[u8]> = items.iter().map(Binary::as_slice).collect();
    let results = filter.mexists(&slices);
    Ok(results.encode(env))
}

/// Count occurrences of an element's fingerprint.
/// Returns a non-negative integer.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_count<'a>(
    env: Env<'a>,
    resource: ResourceArc<CuckooResource>,
    item: Binary<'a>,
) -> NifResult<Term<'a>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let result = filter.count(item.as_slice());
    Ok(result.encode(env))
}

/// Return filter metadata as a map.
/// Returns `{:ok, map}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_info(env: Env<'_>, resource: ResourceArc<CuckooResource>) -> NifResult<Term<'_>> {
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
pub fn cuckoo_serialize(
    env: Env<'_>,
    resource: ResourceArc<CuckooResource>,
) -> NifResult<Term<'_>> {
    let filter = resource.filter.lock().map_err(|_| rustler::Error::BadArg)?;
    let bytes = filter.serialize();
    let mut bin = OwnedBinary::new(bytes.len()).ok_or(rustler::Error::BadArg)?;
    bin.as_mut_slice().copy_from_slice(&bytes);
    Ok((atoms::ok(), Binary::from_owned(bin, env)).encode(env))
}

/// Deserialize a filter from a binary blob (in-memory).
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_filter_empty() {
        let f = CuckooFilter::new(1024, 4, 500);
        assert_eq!(f.num_buckets, 1024);
        assert_eq!(f.bucket_size, 4);
        assert_eq!(f.num_items, 0);
        assert_eq!(f.num_deletes, 0);
        assert_eq!(f.bucket_data_len(), 1024 * 4 * 2);
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
            assert!(
                !fp.iter().all(|&b| b == 0),
                "fingerprint was all zeros for elem_{i}"
            );
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
    // Mmap file-backed tests
    // -----------------------------------------------------------------------

    #[test]
    fn mmap_create_add_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.cuckoo");

        let mut f = CuckooFilter::create_file(&path, 1024, 4).unwrap();
        assert!(!f.exists(b"hello"));
        assert!(f.add(b"hello").is_ok());
        assert!(f.exists(b"hello"));
        assert_eq!(f.num_items, 1);
    }

    #[test]
    fn mmap_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.cuckoo");

        {
            let mut f = CuckooFilter::create_file(&path, 1024, 4).unwrap();
            f.add(b"persisted").unwrap();
            f.add(b"also_persisted").unwrap();
            f.storage.msync_sync();
        }

        // Reopen and verify.
        let f = CuckooFilter::open_file(&path).unwrap();
        assert!(f.exists(b"persisted"));
        assert!(f.exists(b"also_persisted"));
        assert_eq!(f.num_items, 2);
    }

    #[test]
    fn mmap_delete_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.cuckoo");

        {
            let mut f = CuckooFilter::create_file(&path, 1024, 4).unwrap();
            f.add(b"keep").unwrap();
            f.add(b"remove").unwrap();
            f.delete(b"remove");
            f.storage.msync_sync();
        }

        let f = CuckooFilter::open_file(&path).unwrap();
        assert!(f.exists(b"keep"));
        assert!(!f.exists(b"remove"));
        assert_eq!(f.num_items, 1);
        assert_eq!(f.num_deletes, 1);
    }

    #[test]
    fn mmap_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("close.cuckoo");

        {
            let mut f = CuckooFilter::create_file(&path, 1024, 4).unwrap();
            f.add(b"test_close").unwrap();
            f.close();
        }

        let f = CuckooFilter::open_file(&path).unwrap();
        assert!(f.exists(b"test_close"));
    }

    #[test]
    fn mmap_open_nonexistent_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.cuckoo");
        let result = CuckooFilter::open_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn mmap_open_garbage_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.cuckoo");
        std::fs::write(&path, b"this is not a cuckoo file").unwrap();
        // File is only 25 bytes, too short for header
        let result = CuckooFilter::open_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn mmap_open_bad_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("badmagic.cuckoo");
        let mut data = vec![0xFF; 64];
        // Set a plausible file size but bad magic
        data[0] = 0xFF;
        data[1] = 0xFF;
        std::fs::write(&path, &data).unwrap();
        let result = CuckooFilter::open_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn mmap_many_items_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many.cuckoo");

        {
            let mut f = CuckooFilter::create_file(&path, 4096, 4).unwrap();
            for i in 0..1000 {
                let _ = f.add(format!("item_{i}").as_bytes());
            }
            f.storage.msync_sync();
        }

        let f = CuckooFilter::open_file(&path).unwrap();
        // Verify all items present (no false negatives).
        for i in 0..1000 {
            assert!(
                f.exists(format!("item_{i}").as_bytes()),
                "false negative for item_{i} after reopen"
            );
        }
    }

    #[test]
    fn mmap_nested_dir_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deep").join("nested").join("filter.cuckoo");
        let mut f = CuckooFilter::create_file(&path, 256, 4).unwrap();
        f.add(b"nested").unwrap();
        assert!(f.exists(b"nested"));
        assert!(path.exists());
    }

    // -----------------------------------------------------------------------
    // Edge-case tests (in-memory)
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
        assert!(
            result.is_err(),
            "all-zero bytes must fail deserialization (bad magic)"
        );
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

    // ------------------------------------------------------------------
    // H-4 + H-7: xxh3 replaces MD5, single hash per exists()
    // ------------------------------------------------------------------

    #[test]
    fn h4_h7_no_false_negatives_10k_items() {
        let mut f = CuckooFilter::new(4096, 4, 500);

        // Insert 10K items
        for i in 0..10_000 {
            let _ = f.add(format!("h4_item_{i}").as_bytes());
        }

        // Verify zero false negatives for all inserted items
        for i in 0..10_000 {
            assert!(
                f.exists(format!("h4_item_{i}").as_bytes()),
                "false negative for h4_item_{i}"
            );
        }
    }

    #[test]
    fn h7_exists_correctness_5k_present_5k_absent() {
        let mut f = CuckooFilter::new(4096, 4, 500);

        // Add 5K items
        for i in 0..5_000 {
            let _ = f.add(format!("present_{i}").as_bytes());
        }

        // Check 5K present items - zero false negatives
        for i in 0..5_000 {
            assert!(
                f.exists(format!("present_{i}").as_bytes()),
                "false negative for present_{i}"
            );
        }

        // Check 5K absent items - track false positives
        let fp_count = (0..5_000)
            .filter(|i| f.exists(format!("absent_{i}").as_bytes()))
            .count();

        // With 2-byte fingerprints, FPR should be reasonable
        let fpr = fp_count as f64 / 5_000.0;
        assert!(
            fpr < 0.05,
            "False positive rate {fpr:.4} too high for 2-byte fingerprints"
        );
    }

    #[test]
    fn h7_serialization_roundtrip_after_hash_change() {
        let mut f = CuckooFilter::new(2048, 4, 500);
        for i in 0..200 {
            f.add(format!("serial_{i}").as_bytes()).unwrap();
        }

        let bytes = f.serialize();
        let f2 = CuckooFilter::deserialize(&bytes).unwrap();

        // All items should still be found after deserialization
        for i in 0..200 {
            assert!(
                f2.exists(format!("serial_{i}").as_bytes()),
                "serial_{i} missing after deserialize"
            );
        }
        assert_eq!(f2.num_items, 200);
    }

    #[test]
    fn h7_delete_correctness_after_hash_change() {
        let mut f = CuckooFilter::new(2048, 4, 500);

        // Add items
        for i in 0..100 {
            f.add(format!("del_{i}").as_bytes()).unwrap();
        }

        // Delete even-numbered items
        for i in (0..100).step_by(2) {
            let result = f.delete(format!("del_{i}").as_bytes());
            assert_eq!(result, 1, "delete must succeed for del_{i}");
        }

        // Odd items should still exist
        for i in (1..100).step_by(2) {
            assert!(
                f.exists(format!("del_{i}").as_bytes()),
                "del_{i} must still exist after deleting even items"
            );
        }
    }

    // ------------------------------------------------------------------
    // L-6: stack-allocated empty sentinel in delete()
    // ------------------------------------------------------------------

    #[test]
    fn l6_delete_many_times_no_leak() {
        let mut f = CuckooFilter::new(2048, 4, 500);
        for i in 0..500 {
            f.add(format!("del_test_{i}").as_bytes()).unwrap();
        }
        assert_eq!(f.num_items, 500);

        // Delete all items
        for i in 0..500 {
            let result = f.delete(format!("del_test_{i}").as_bytes());
            assert_eq!(result, 1, "delete must succeed for del_test_{i}");
        }

        assert_eq!(f.num_items, 0, "all items should be deleted");
        assert_eq!(f.num_deletes, 500, "delete count should match");

        // Verify no item is found
        for i in 0..500 {
            assert!(
                !f.exists(format!("del_test_{i}").as_bytes()),
                "del_test_{i} should not exist after deletion"
            );
        }
    }

    #[test]
    fn l6_delete_nonexistent_returns_zero_stack_sentinel() {
        let mut f = CuckooFilter::new(1024, 4, 500);
        // Delete from empty filter must return 0 and not corrupt the filter
        for i in 0..100 {
            assert_eq!(
                f.delete(format!("ghost_{i}").as_bytes()),
                0,
                "deleting non-existent item must return 0"
            );
        }
        assert_eq!(f.num_items, 0);
        assert_eq!(f.num_deletes, 0);
    }
}
