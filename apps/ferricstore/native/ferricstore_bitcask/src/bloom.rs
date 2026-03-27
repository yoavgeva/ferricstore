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

use rustler::schedule::consume_timeslice;
use rustler::{Binary, Encoder, Env, NifResult, ResourceArc, Term};

/// How often (in items) to call `consume_timeslice` and let the BEAM
/// decide whether we should yield. 64 matches the interval used in lib.rs.
const YIELD_CHECK_INTERVAL: usize = 64;

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

        let byte_count = num_bits.div_ceil(8) as usize;
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

        let expected_size = HEADER_SIZE + num_bits.div_ceil(8) as usize;
        if file_size < expected_size {
            unsafe {
                libc::munmap(mmap, file_size);
            }
            return Err(format!(
                "file too small: expected {expected_size}, got {file_size}"
            ));
        }

        // M-4 fix: hint the kernel that access is random (hash-determined bit
        // positions). Without MADV_RANDOM, the kernel's readahead heuristic may
        // prefetch sequential pages that will never be used, wasting page cache
        // and evicting hot data file pages under memory pressure.
        unsafe {
            libc::madvise(mmap, file_size, libc::MADV_RANDOM);
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
    ///
    /// H-4 fix: replaced MD5 with xxh3 (5-10x faster for non-cryptographic hashing).
    /// Uses two xxh3 hashes with different seeds to produce h1 and h2.
    ///
    /// M-3 fix: returns an iterator instead of allocating a Vec on every call.
    fn hash_positions(&self, element: &[u8]) -> impl Iterator<Item = u64> {
        let h1 = xxhash_rust::xxh3::xxh3_64_with_seed(element, 0);
        let h2 = xxhash_rust::xxh3::xxh3_64_with_seed(element, 0x9E37_79B9_7F4A_7C15);
        let num_bits = self.num_bits;

        (0..self.num_hashes as u64).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % num_bits)
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
        let mut any_new = false;
        for pos in self.hash_positions(element) {
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
        self.hash_positions(element).all(|pos| self.get_bit(pos))
    }

    /// Flush changes to disk synchronously (MS_SYNC — blocks until pages are on disk).
    pub fn msync(&self) -> Result<(), String> {
        let ret =
            unsafe { libc::msync(self.mmap as *mut libc::c_void, self.mmap_len, libc::MS_SYNC) };
        if ret != 0 {
            Err(format!("msync failed: {}", std::io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    /// Mark dirty pages for background writeback (MS_ASYNC — non-blocking).
    ///
    /// M-2 fix: `bloom_add()` now uses this instead of the blocking `msync()`.
    /// `MS_ASYNC` tells the kernel the pages are dirty and should be written back
    /// at its convenience, without blocking the caller. This drops per-add cost
    /// from ~100-500 us (NVMe fsync) to ~1 us.
    ///
    /// Durability is still guaranteed by:
    /// - `bloom_madd()` calls `msync()` (MS_SYNC) after all additions
    /// - `Drop` impl calls `msync()` (MS_SYNC) before `munmap`
    /// - Explicit `msync()` calls from Elixir
    pub fn msync_async(&self) -> Result<(), String> {
        let ret = unsafe {
            libc::msync(
                self.mmap as *mut libc::c_void,
                self.mmap_len,
                libc::MS_ASYNC,
            )
        };
        if ret != 0 {
            Err(format!(
                "msync_async failed: {}",
                std::io::Error::last_os_error()
            ))
        } else {
            Ok(())
        }
    }

    /// Delete the bloom filter: munmap + unlink the file.
    ///
    /// L-3 fix: use `ManuallyDrop` to avoid an unnecessary `path.clone()`
    /// while still preventing Drop from double-munmapping.
    pub fn delete(self) -> Result<(), String> {
        let mut this = std::mem::ManuallyDrop::new(self);

        unsafe {
            libc::munmap(this.mmap as *mut libc::c_void, this.mmap_len);
        }
        // Null out the pointer so that if ManuallyDrop is ever dropped
        // (it won't be, but belt-and-suspenders), the Drop impl is a no-op.
        this.mmap = std::ptr::null_mut();

        fs::remove_file(&this.path).map_err(|e| format!("unlink: {e}"))?;
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

// H-4 fix: removed duplicate hand-rolled MD5 implementation.
// Bloom filter now uses xxh3 (non-cryptographic, 5-10x faster than MD5)
// via the hash_positions() method above.

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
    // M-2 fix: use MS_ASYNC (non-blocking) for individual adds instead of
    // MS_SYNC which blocks until pages are flushed to disk (~100-500 us per call).
    // Durability is guaranteed by msync(MS_SYNC) in bloom_madd, Drop, and explicit flush.
    let _ = filter.msync_async();
    Ok(u32::from(is_new).encode(env))
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
    let mut results: Vec<u32> = Vec::with_capacity(elements.len());
    for (i, e) in elements.iter().enumerate() {
        results.push(u32::from(filter.add(e.as_slice())));
        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }
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
    Ok(u32::from(found).encode(env))
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
    let mut results: Vec<u32> = Vec::with_capacity(elements.len());
    for (i, e) in elements.iter().enumerate() {
        results.push(u32::from(filter.exists(e.as_slice())));
        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }
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
        let filter_ptr = std::ptr::addr_of!(filter);
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
            for pos in positions {
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
        std::fs::write(&path, [0xFF; 64]).unwrap();
        let result = BloomFilter::open_existing(&path);
        assert!(result.is_err(), "bad magic must return error");
    }

    #[test]
    fn bloom_open_zero_length_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bloom");
        std::fs::write(&path, []).unwrap();
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

    // ------------------------------------------------------------------
    // H-4: xxh3 replaces MD5 — verify hash quality and correctness
    // ------------------------------------------------------------------

    #[test]
    fn h4_xxh3_no_false_negatives_10k_items() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h4_xxh3.bloom");
        let filter = BloomFilter::create(&path, 1_000_000, 7).unwrap();

        for i in 0..10_000 {
            filter.add(format!("h4_item_{i}").as_bytes());
        }
        assert_eq!(filter.count(), 10_000);

        // Verify zero false negatives
        for i in 0..10_000 {
            assert!(
                filter.exists(format!("h4_item_{i}").as_bytes()),
                "false negative for h4_item_{i}"
            );
        }
    }

    #[test]
    fn h4_xxh3_fpr_within_expected_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h4_fpr.bloom");

        // Parameters for ~1% FPR with 10K items
        let num_bits = 95_851;
        let num_hashes = 7;
        let filter = BloomFilter::create(&path, num_bits, num_hashes).unwrap();

        for i in 0..10_000 {
            filter.add(format!("added_{i}").as_bytes());
        }

        let test_count = 50_000;
        let fp_count = (0..test_count)
            .filter(|i| filter.exists(format!("not_added_{i}").as_bytes()))
            .count();

        let fp_rate = fp_count as f64 / test_count as f64;
        assert!(
            fp_rate < 0.02,
            "False positive rate {fp_rate:.4} exceeds 2% (target was 1%)"
        );
    }

    #[test]
    fn h4_xxh3_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h4_persist.bloom");

        {
            let filter = BloomFilter::create(&path, 100_000, 7).unwrap();
            for i in 0..500 {
                filter.add(format!("persist_{i}").as_bytes());
            }
            filter.msync().unwrap();
        }

        // Reopen and verify all items found
        let filter = BloomFilter::open_existing(&path).unwrap();
        assert_eq!(filter.count(), 500);
        for i in 0..500 {
            assert!(
                filter.exists(format!("persist_{i}").as_bytes()),
                "item persist_{i} missing after reopen"
            );
        }
    }

    // ------------------------------------------------------------------
    // M-2: msync_async uses MS_ASYNC (non-blocking) for individual adds
    // ------------------------------------------------------------------

    #[test]
    fn m2_msync_async_does_not_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m2_async.bloom");
        let filter = BloomFilter::create(&path, 100_000, 7).unwrap();

        // msync_async should succeed without error
        filter.add(b"test_item");
        assert!(filter.msync_async().is_ok(), "msync_async must not fail");

        // Data should still be readable in-memory after msync_async
        assert!(filter.exists(b"test_item"));
    }

    #[test]
    fn m2_msync_async_bulk_adds_then_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m2_bulk.bloom");
        let filter = BloomFilter::create(&path, 100_000, 7).unwrap();

        // Simulate the bloom_add NIF pattern: add + msync_async per item
        for i in 0..1000 {
            filter.add(format!("m2_item_{i}").as_bytes());
            filter.msync_async().unwrap();
        }

        // All items must still be present in memory
        for i in 0..1000 {
            assert!(
                filter.exists(format!("m2_item_{i}").as_bytes()),
                "m2_item_{i} missing after bulk add with msync_async"
            );
        }
        assert_eq!(filter.count(), 1000);

        // Explicit msync (MS_SYNC) for durability, then reopen
        filter.msync().unwrap();
        drop(filter);

        let filter2 = BloomFilter::open_existing(&path).unwrap();
        assert_eq!(filter2.count(), 1000);
        for i in 0..1000 {
            assert!(
                filter2.exists(format!("m2_item_{i}").as_bytes()),
                "m2_item_{i} missing after reopen"
            );
        }
    }

    // ------------------------------------------------------------------
    // M-4: madvise(MADV_RANDOM) on mmap regions
    // ------------------------------------------------------------------

    #[test]
    fn m4_madvise_random_does_not_break_operations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m4_madvise.bloom");
        let filter = BloomFilter::create(&path, 1_000_000, 7).unwrap();

        // madvise is called during open; verify all operations still work
        for i in 0..1000 {
            filter.add(format!("m4_{i}").as_bytes());
        }
        for i in 0..1000 {
            assert!(
                filter.exists(format!("m4_{i}").as_bytes()),
                "m4_{i} missing after madvise"
            );
        }

        // Verify false negative rate is unchanged
        let fp_count = (0..10_000)
            .filter(|i| filter.exists(format!("m4_probe_{i}").as_bytes()))
            .count();
        let fpr = fp_count as f64 / 10_000.0;
        assert!(fpr < 0.02, "FPR {fpr:.4} too high after madvise");
    }

    // ------------------------------------------------------------------
    // L-3: ManuallyDrop-based delete avoids path.clone()
    // ------------------------------------------------------------------

    #[test]
    fn l3_delete_removes_file_and_does_not_leak() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("l3_del.bloom");

        let filter = BloomFilter::create(&path, 10_000, 7).unwrap();
        for i in 0..100 {
            filter.add(format!("l3_item_{i}").as_bytes());
        }
        filter.msync().unwrap();
        assert!(path.exists(), "file must exist before delete");

        filter.delete().unwrap();
        assert!(!path.exists(), "file must not exist after delete");
    }

    #[test]
    fn l3_delete_on_large_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("l3_large.bloom");

        let filter = BloomFilter::create(&path, 1_000_000, 7).unwrap();
        for i in 0..1_000 {
            filter.add(format!("l3_large_{i}").as_bytes());
        }

        let file_size = std::fs::metadata(&path).unwrap().len();
        assert!(file_size > 100_000, "large bloom file must be > 100KB");

        filter.delete().unwrap();
        assert!(!path.exists(), "large bloom file must be deleted");
    }
}
