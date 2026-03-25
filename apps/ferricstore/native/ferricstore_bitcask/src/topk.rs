//! Top-K data structure backed by a Count-Min Sketch and a min-heap.
//!
//! Maintains the K most frequent elements seen so far. Each element
//! addition updates an internal CMS to estimate frequency, then inserts
//! into a bounded min-heap of size K. When the heap is full and a new
//! element exceeds the minimum frequency, the least-frequent element is
//! evicted and returned to the caller.
//!
//! Exposed to Elixir as NIF functions operating on a `TopKResource`.

use std::collections::{BinaryHeap, HashSet};
use std::sync::Mutex;

use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, ResourceArc, Term};

// ---------------------------------------------------------------------------
// Count-Min Sketch (internal)
// ---------------------------------------------------------------------------

/// A Count-Min Sketch for frequency estimation.
///
/// Uses double hashing: h(i) = h1 + i * h2  (mod width) for row i.
struct CountMinSketch {
    width: usize,
    depth: usize,
    /// Row-major: counters[row * width + col]
    counters: Vec<i64>,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        Self {
            width,
            depth,
            counters: vec![0i64; width * depth],
        }
    }

    /// Hash an element using Erlang-compatible double hashing.
    /// Returns `depth` hash values, one per row.
    fn hash_indices(&self, element: &[u8]) -> Vec<usize> {
        // Use FNV-1a style hashing with two independent seeds for
        // double hashing. We need deterministic hashes that match
        // across calls.
        let h1 = fnv1a(element, 0x811c_9dc5);
        let h2 = fnv1a(element, 0x050c_5d1f);
        (0..self.depth)
            .map(|i| {
                let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
                (h % self.width as u64) as usize
            })
            .collect()
    }

    /// Increment element's count and return the new minimum estimate.
    fn increment(&mut self, element: &[u8], count: i64) -> i64 {
        let indices = self.hash_indices(element);
        let mut min_count = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let idx = row * self.width + col;
            self.counters[idx] += count;
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

}

/// FNV-1a hash with a configurable offset basis for double hashing.
fn fnv1a(data: &[u8], offset_basis: u64) -> u64 {
    let mut hash = offset_basis;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Heap item
// ---------------------------------------------------------------------------

/// An item in the min-heap, ordered by estimated count (ascending).
#[derive(Clone, Debug)]
struct HeapItem {
    count: i64,
    element: String,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.element == other.element
    }
}

impl Eq for HeapItem {}

/// Reverse ordering so `BinaryHeap` acts as a min-heap (smallest count at top).
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse: smallest count has highest priority (min-heap).
        other
            .count
            .cmp(&self.count)
            .then_with(|| self.element.cmp(&other.element))
    }
}

// ---------------------------------------------------------------------------
// TopK structure
// ---------------------------------------------------------------------------

/// Top-K tracker: maintains the K items with the highest estimated frequency.
pub struct TopK {
    k: usize,
    width: usize,
    depth: usize,
    decay: f64,
    cms: CountMinSketch,
    /// Min-heap of top-k items (smallest count at top via reversed Ord).
    heap: BinaryHeap<HeapItem>,
    /// Fast membership check for items currently in the heap.
    fingerprints: HashSet<String>,
}

impl TopK {
    pub fn new(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        Self {
            k,
            width,
            depth,
            decay,
            cms: CountMinSketch::new(width, depth),
            heap: BinaryHeap::with_capacity(k + 1),
            fingerprints: HashSet::with_capacity(k + 1),
        }
    }

    /// Add a single element with the given increment. Returns the displaced
    /// element name if an eviction occurred, or `None`.
    pub fn add(&mut self, element: &str, count: i64) -> Option<String> {
        let estimated = self.cms.increment(element.as_bytes(), count);

        // If element is already tracked, update its count in the heap.
        if self.fingerprints.contains(element) {
            self.update_existing(element, estimated);
            return None;
        }

        // If heap has room, just insert.
        if self.heap.len() < self.k {
            self.heap.push(HeapItem {
                count: estimated,
                element: element.to_string(),
            });
            self.fingerprints.insert(element.to_string());
            return None;
        }

        // Heap is full: check if new element beats the minimum.
        let min_count = self.heap.peek().map_or(0, |item| item.count);
        if estimated > min_count {
            // Evict the minimum.
            let evicted = self.heap.pop().unwrap();
            self.fingerprints.remove(&evicted.element);

            self.heap.push(HeapItem {
                count: estimated,
                element: element.to_string(),
            });
            self.fingerprints.insert(element.to_string());
            Some(evicted.element)
        } else {
            None
        }
    }

    /// Update the count of an element already in the heap.
    ///
    /// L-5 fix: use `into_vec()` to get the underlying Vec without allocation,
    /// mutate the target element in place, then reconstruct the heap via `From`.
    /// This avoids the intermediate `drain().collect()` allocation.
    fn update_existing(&mut self, element: &str, new_count: i64) {
        let mut items = std::mem::take(&mut self.heap).into_vec();
        for item in &mut items {
            if item.element == element {
                item.count = new_count;
                break;
            }
        }
        self.heap = BinaryHeap::from(items);
    }

    /// Check if an element is currently in the top-K.
    pub fn query(&self, element: &str) -> bool {
        self.fingerprints.contains(element)
    }

    /// Return all top-K items sorted by count descending.
    pub fn list(&self) -> Vec<(String, i64)> {
        let mut items: Vec<(String, i64)> = self
            .heap
            .iter()
            .map(|item| (item.element.clone(), item.count))
            .collect();
        items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        items
    }

    /// Return metadata: (k, width, depth, decay).
    pub fn info(&self) -> (usize, usize, usize, f64) {
        (self.k, self.width, self.depth, self.decay)
    }

    // ------------------------------------------------------------------
    // Serialization (for Bitcask byte array storage)
    // ------------------------------------------------------------------

    /// Serialize the TopK to a byte blob for storage.
    ///
    /// Format:
    ///   [k:u32][width:u32][depth:u32][decay:f64]
    ///   [cms_counters: width*depth * i64]
    ///   [heap_len:u32]
    ///   for each heap item:
    ///     [element_len:u32][element_bytes][count:i64]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.k as u32).to_le_bytes());
        buf.extend_from_slice(&(self.width as u32).to_le_bytes());
        buf.extend_from_slice(&(self.depth as u32).to_le_bytes());
        buf.extend_from_slice(&self.decay.to_le_bytes());

        // CMS counters
        for &counter in &self.cms.counters {
            buf.extend_from_slice(&counter.to_le_bytes());
        }

        // Heap items
        let items = self.list(); // sorted desc
        buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
        for (element, count) in &items {
            let elem_bytes = element.as_bytes();
            buf.extend_from_slice(&(elem_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(elem_bytes);
            buf.extend_from_slice(&count.to_le_bytes());
        }

        buf
    }

    /// Deserialize a TopK from a byte blob.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 20 {
            return Err("TopK: data too short for header".to_string());
        }
        let mut pos = 0;

        let k = read_u32(data, &mut pos)? as usize;
        let width = read_u32(data, &mut pos)? as usize;
        let depth = read_u32(data, &mut pos)? as usize;
        let decay = read_f64(data, &mut pos)?;

        // CMS counters
        let cms_size = width * depth;
        let mut counters = Vec::with_capacity(cms_size);
        for _ in 0..cms_size {
            counters.push(read_i64(data, &mut pos)?);
        }

        let cms = CountMinSketch {
            width,
            depth,
            counters,
        };

        // Heap items
        let heap_len = read_u32(data, &mut pos)? as usize;
        let mut heap = BinaryHeap::with_capacity(k + 1);
        let mut fingerprints = HashSet::with_capacity(k + 1);

        for _ in 0..heap_len {
            let elem_len = read_u32(data, &mut pos)? as usize;
            if pos + elem_len > data.len() {
                return Err("TopK: data truncated in element".to_string());
            }
            let element =
                String::from_utf8(data[pos..pos + elem_len].to_vec()).map_err(|e| e.to_string())?;
            pos += elem_len;
            let count = read_i64(data, &mut pos)?;

            fingerprints.insert(element.clone());
            heap.push(HeapItem { count, element });
        }

        Ok(Self {
            k,
            width,
            depth,
            decay,
            cms,
            heap,
            fingerprints,
        })
    }
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32, String> {
    if *pos + 4 > data.len() {
        return Err("TopK: data truncated reading u32".to_string());
    }
    let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(val)
}

fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64, String> {
    if *pos + 8 > data.len() {
        return Err("TopK: data truncated reading i64".to_string());
    }
    let val = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(val)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Result<f64, String> {
    if *pos + 8 > data.len() {
        return Err("TopK: data truncated reading f64".to_string());
    }
    let val = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(val)
}

// ---------------------------------------------------------------------------
// Rustler NIF resource
// ---------------------------------------------------------------------------

/// Resource wrapper for the TopK, protected by a Mutex for thread safety.
pub struct TopKResource {
    pub topk: Mutex<TopK>,
}

// ---------------------------------------------------------------------------
// NIF atoms
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
    }
}

// ---------------------------------------------------------------------------
// NIF functions
// ---------------------------------------------------------------------------

/// Create a new TopK tracker.
/// Returns `{:ok, ref}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_create(env: Env, k: usize, width: usize, depth: usize, decay: f64) -> NifResult<Term> {
    if k == 0 {
        return Ok((atoms::error(), "k must be > 0").encode(env));
    }
    if width == 0 {
        return Ok((atoms::error(), "width must be > 0").encode(env));
    }
    if depth == 0 {
        return Ok((atoms::error(), "depth must be > 0").encode(env));
    }
    if !(0.0..=1.0).contains(&decay) {
        return Ok((atoms::error(), "decay must be between 0 and 1").encode(env));
    }

    let topk = TopK::new(k, width, depth, decay);
    let resource = ResourceArc::new(TopKResource {
        topk: Mutex::new(topk),
    });

    Ok((atoms::ok(), resource).encode(env))
}

/// Add items (each with increment 1). Returns a list of displaced items
/// (nil for no eviction, string for evicted element name).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_add(
    env: Env,
    resource: ResourceArc<TopKResource>,
    items: Vec<String>,
) -> NifResult<Term> {
    let mut topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;

    let results: Vec<Term> = items
        .iter()
        .map(|item| match topk.add(item, 1) {
            Some(evicted) => evicted.encode(env),
            None => atoms::nil().encode(env),
        })
        .collect();

    Ok(results.encode(env))
}

/// Increment items by specified amounts. `items_and_increments` is a flat
/// list of `[element, count, element, count, ...]` pairs encoded as
/// `Vec<(String, i64)>`.
/// Returns a list of displaced items (nil or evicted element name).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_incrby(
    env: Env,
    resource: ResourceArc<TopKResource>,
    pairs: Vec<(String, i64)>,
) -> NifResult<Term> {
    let mut topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;

    let results: Vec<Term> = pairs
        .iter()
        .map(|(item, count)| match topk.add(item, *count) {
            Some(evicted) => evicted.encode(env),
            None => atoms::nil().encode(env),
        })
        .collect();

    Ok(results.encode(env))
}

/// Query whether items are in the top-K.
/// Returns a list of 0 (not in top-K) or 1 (in top-K).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_query(
    env: Env,
    resource: ResourceArc<TopKResource>,
    items: Vec<String>,
) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;

    let results: Vec<i32> = items
        .iter()
        .map(|item| if topk.query(item) { 1 } else { 0 })
        .collect();

    Ok(results.encode(env))
}

/// List all items in the top-K, sorted by count descending.
/// Returns a list of element strings.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_list(env: Env, resource: ResourceArc<TopKResource>) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;

    let items: Vec<String> = topk.list().into_iter().map(|(elem, _)| elem).collect();

    Ok(items.encode(env))
}

/// List all items in the top-K with their estimated counts, sorted by
/// count descending. Returns a list of `{element, count}` tuples.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_list_with_count(env: Env, resource: ResourceArc<TopKResource>) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;

    let items: Vec<(String, i64)> = topk.list();

    Ok(items.encode(env))
}

/// Return metadata: `{k, width, depth, decay}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_info(env: Env, resource: ResourceArc<TopKResource>) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let (k, width, depth, decay) = topk.info();

    Ok((k, width, depth, decay).encode(env))
}

/// Serialize the TopK to a binary blob.
/// Returns `{:ok, binary}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_to_bytes<'a>(env: Env<'a>, resource: ResourceArc<TopKResource>) -> NifResult<Term<'a>> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let bytes = topk.to_bytes();
    let mut bin = OwnedBinary::new(bytes.len()).ok_or(rustler::Error::BadArg)?;
    bin.as_mut_slice().copy_from_slice(&bytes);
    Ok((atoms::ok(), Binary::from_owned(bin, env)).encode(env))
}

/// Deserialize a TopK from a binary blob.
/// Returns `{:ok, ref}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_from_bytes<'a>(env: Env<'a>, data: Binary<'a>) -> NifResult<Term<'a>> {
    match TopK::from_bytes(data.as_slice()) {
        Ok(topk) => {
            let resource = ResourceArc::new(TopKResource {
                topk: Mutex::new(topk),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(msg) => Ok((atoms::error(), msg).encode(env)),
    }
}

// ===========================================================================
// Memory-mapped file-backed TopK
// ===========================================================================
//
// File layout (little-endian):
//
// ```text
// [header: 64 bytes]
// [CMS counters: width * depth * 8 bytes (i64 each)]
// [heap entries: k * HEAP_ENTRY_SIZE bytes]
// ```
//
// Header (64 bytes):
//   bytes  0..7:  magic (0x544F504B_4D4D5031 = "TOPKMMP1")
//   bytes  8..11: k (u32)
//   bytes 12..15: width (u32)
//   bytes 16..19: depth (u32)
//   bytes 20..27: decay (f64)
//   bytes 28..31: heap_len (u32) — number of items currently in heap
//   bytes 32..63: reserved (zero)
//
// Each heap entry (HEAP_ENTRY_SIZE = 264 bytes):
//   bytes 0..7:   count (i64)
//   bytes 8..11:  element_len (u32)
//   bytes 12..263: element bytes (max 252 bytes, zero-padded)

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const TOPK_MAGIC: u64 = 0x544F_504B_4D4D_5031; // "TOPKMMP1"
const TOPK_HEADER_SIZE: usize = 64;
const HEAP_ENTRY_SIZE: usize = 264; // 8 (count) + 4 (len) + 252 (element)
const MAX_ELEMENT_LEN: usize = 252;

/// A memory-mapped TopK data structure.
///
/// The mmap region covers the entire file: header + CMS counters + heap entries.
pub struct MmapTopK {
    mmap: *mut u8,
    mmap_len: usize,
    k: usize,
    width: usize,
    depth: usize,
    decay: f64,
    path: PathBuf,
    /// In-memory fingerprint set for fast membership checks.
    /// Rebuilt from heap entries on open.
    fingerprints: HashSet<String>,
}

// SAFETY: The mmap pointer is protected by a Mutex in MmapTopKResource.
unsafe impl Send for MmapTopK {}
unsafe impl Sync for MmapTopK {}

impl MmapTopK {
    fn file_size(k: usize, width: usize, depth: usize) -> usize {
        TOPK_HEADER_SIZE + (width * depth * 8) + (k * HEAP_ENTRY_SIZE)
    }

    fn cms_offset() -> usize {
        TOPK_HEADER_SIZE
    }

    fn heap_offset(width: usize, depth: usize) -> usize {
        TOPK_HEADER_SIZE + (width * depth * 8)
    }

    pub fn create(
        path: &Path,
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
    ) -> Result<Self, String> {
        if k == 0 {
            return Err("k must be > 0".into());
        }
        if width == 0 {
            return Err("width must be > 0".into());
        }
        if depth == 0 {
            return Err("depth must be > 0".into());
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
        }

        let file_size = Self::file_size(k, width, depth);

        let mut file = File::create(path).map_err(|e| format!("create: {e}"))?;
        let mut header = [0u8; TOPK_HEADER_SIZE];
        header[0..8].copy_from_slice(&TOPK_MAGIC.to_le_bytes());
        header[8..12].copy_from_slice(&(k as u32).to_le_bytes());
        header[12..16].copy_from_slice(&(width as u32).to_le_bytes());
        header[16..20].copy_from_slice(&(depth as u32).to_le_bytes());
        header[20..28].copy_from_slice(&decay.to_le_bytes());
        // heap_len = 0, reserved = 0 (already zeroed)
        file.write_all(&header)
            .map_err(|e| format!("write header: {e}"))?;
        let zeros = vec![0u8; file_size - TOPK_HEADER_SIZE];
        file.write_all(&zeros)
            .map_err(|e| format!("write body: {e}"))?;
        file.sync_all().map_err(|e| format!("fsync: {e}"))?;
        drop(file);

        Self::open(path, file_size, k, width, depth, decay)
    }

    pub fn open_existing(path: &Path) -> Result<Self, String> {
        let meta = fs::metadata(path).map_err(|e| format!("stat: {e}"))?;
        let file_size = meta.len() as usize;
        if file_size < TOPK_HEADER_SIZE {
            return Err("file too small for topk header".into());
        }

        // Read header to get params
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

        let magic = u64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 8);
            buf
        });
        if magic != TOPK_MAGIC {
            unsafe { libc::munmap(mmap, file_size); }
            return Err("invalid topk file magic".into());
        }

        let k = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr.add(8), buf.as_mut_ptr(), 4);
            buf
        }) as usize;
        let width = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr.add(12), buf.as_mut_ptr(), 4);
            buf
        }) as usize;
        let depth = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr.add(16), buf.as_mut_ptr(), 4);
            buf
        }) as usize;
        let decay = f64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr.add(20), buf.as_mut_ptr(), 8);
            buf
        });

        let expected = Self::file_size(k, width, depth);
        if file_size < expected {
            unsafe { libc::munmap(mmap, file_size); }
            return Err(format!(
                "file too small: expected {expected}, got {file_size}"
            ));
        }

        // Build fingerprints from heap entries
        let heap_len = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr.add(28), buf.as_mut_ptr(), 4);
            buf
        }) as usize;

        let mut fingerprints = HashSet::with_capacity(k + 1);
        let heap_base = Self::heap_offset(width, depth);
        for i in 0..heap_len {
            let entry_ptr = unsafe { ptr.add(heap_base + i * HEAP_ENTRY_SIZE) };
            let elem_len = u32::from_le_bytes(unsafe {
                let mut buf = [0u8; 4];
                std::ptr::copy_nonoverlapping(entry_ptr.add(8), buf.as_mut_ptr(), 4);
                buf
            }) as usize;
            if elem_len <= MAX_ELEMENT_LEN {
                let elem_bytes = unsafe {
                    std::slice::from_raw_parts(entry_ptr.add(12), elem_len)
                };
                if let Ok(s) = std::str::from_utf8(elem_bytes) {
                    fingerprints.insert(s.to_string());
                }
            }
        }

        // Don't munmap - we reuse it
        Ok(Self {
            mmap: ptr,
            mmap_len: file_size,
            k,
            width,
            depth,
            decay,
            path: path.to_path_buf(),
            fingerprints,
        })
    }

    fn open(
        path: &Path,
        file_size: usize,
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
    ) -> Result<Self, String> {
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

        Ok(Self {
            mmap: mmap as *mut u8,
            mmap_len: file_size,
            k,
            width,
            depth,
            decay,
            path: path.to_path_buf(),
            fingerprints: HashSet::with_capacity(k + 1),
        })
    }

    // -- Header accessors --

    fn heap_len(&self) -> usize {
        u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(self.mmap.add(28), buf.as_mut_ptr(), 4);
            buf
        }) as usize
    }

    fn set_heap_len(&self, len: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                (len as u32).to_le_bytes().as_ptr(),
                self.mmap.add(28),
                4,
            );
        }
    }

    // -- CMS accessors --

    fn cms_get(&self, row: usize, col: usize) -> i64 {
        let offset = Self::cms_offset() + (row * self.width + col) * 8;
        i64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(self.mmap.add(offset), buf.as_mut_ptr(), 8);
            buf
        })
    }

    fn cms_set(&self, row: usize, col: usize, val: i64) {
        let offset = Self::cms_offset() + (row * self.width + col) * 8;
        unsafe {
            std::ptr::copy_nonoverlapping(
                val.to_le_bytes().as_ptr(),
                self.mmap.add(offset),
                8,
            );
        }
    }

    fn cms_increment(&self, element: &[u8], count: i64) -> i64 {
        let h1 = fnv1a(element, 0x811c_9dc5);
        let h2 = fnv1a(element, 0x050c_5d1f);
        let mut min_count = i64::MAX;
        for i in 0..self.depth {
            let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let col = (h % self.width as u64) as usize;
            let val = self.cms_get(i, col) + count;
            self.cms_set(i, col, val);
            min_count = min_count.min(val);
        }
        min_count
    }

    fn cms_estimate(&self, element: &[u8]) -> i64 {
        let h1 = fnv1a(element, 0x811c_9dc5);
        let h2 = fnv1a(element, 0x050c_5d1f);
        let mut min_count = i64::MAX;
        for i in 0..self.depth {
            let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let col = (h % self.width as u64) as usize;
            min_count = min_count.min(self.cms_get(i, col));
        }
        min_count
    }

    // -- Heap accessors --

    fn heap_entry_count(&self, idx: usize) -> i64 {
        let offset = Self::heap_offset(self.width, self.depth) + idx * HEAP_ENTRY_SIZE;
        i64::from_le_bytes(unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(self.mmap.add(offset), buf.as_mut_ptr(), 8);
            buf
        })
    }

    fn heap_entry_element(&self, idx: usize) -> String {
        let base = Self::heap_offset(self.width, self.depth) + idx * HEAP_ENTRY_SIZE;
        let elem_len = u32::from_le_bytes(unsafe {
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(self.mmap.add(base + 8), buf.as_mut_ptr(), 4);
            buf
        }) as usize;
        let clamped = elem_len.min(MAX_ELEMENT_LEN);
        let bytes = unsafe {
            std::slice::from_raw_parts(self.mmap.add(base + 12), clamped)
        };
        String::from_utf8_lossy(bytes).to_string()
    }

    fn set_heap_entry(&self, idx: usize, element: &str, count: i64) {
        let base = Self::heap_offset(self.width, self.depth) + idx * HEAP_ENTRY_SIZE;
        unsafe {
            // Write count
            std::ptr::copy_nonoverlapping(
                count.to_le_bytes().as_ptr(),
                self.mmap.add(base),
                8,
            );
            // Write element length
            let elem_bytes = element.as_bytes();
            let len = elem_bytes.len().min(MAX_ELEMENT_LEN);
            std::ptr::copy_nonoverlapping(
                (len as u32).to_le_bytes().as_ptr(),
                self.mmap.add(base + 8),
                4,
            );
            // Write element bytes (zero-padded)
            std::ptr::write_bytes(self.mmap.add(base + 12), 0, MAX_ELEMENT_LEN);
            std::ptr::copy_nonoverlapping(
                elem_bytes.as_ptr(),
                self.mmap.add(base + 12),
                len,
            );
        }
    }

    /// Find the index of the min-count entry in the heap, or None if empty.
    fn find_min_heap_idx(&self) -> Option<(usize, i64)> {
        let len = self.heap_len();
        if len == 0 {
            return None;
        }
        let mut min_idx = 0;
        let mut min_count = self.heap_entry_count(0);
        for i in 1..len {
            let c = self.heap_entry_count(i);
            if c < min_count {
                min_count = c;
                min_idx = i;
            }
        }
        Some((min_idx, min_count))
    }

    /// Add a single element with the given increment. Returns the displaced
    /// element name if an eviction occurred, or `None`.
    pub fn add(&mut self, element: &str, count: i64) -> Option<String> {
        let estimated = self.cms_increment(element.as_bytes(), count);

        // Already tracked? Update count in-place.
        if self.fingerprints.contains(element) {
            let len = self.heap_len();
            for i in 0..len {
                if self.heap_entry_element(i) == element {
                    self.set_heap_entry(i, element, estimated);
                    break;
                }
            }
            return None;
        }

        let len = self.heap_len();

        // Heap has room
        if len < self.k {
            self.set_heap_entry(len, element, estimated);
            self.set_heap_len(len + 1);
            self.fingerprints.insert(element.to_string());
            return None;
        }

        // Heap full: check if new element beats the minimum
        if let Some((min_idx, min_count)) = self.find_min_heap_idx() {
            if estimated > min_count {
                let evicted = self.heap_entry_element(min_idx);
                self.fingerprints.remove(&evicted);
                self.set_heap_entry(min_idx, element, estimated);
                self.fingerprints.insert(element.to_string());
                return Some(evicted);
            }
        }

        None
    }

    pub fn query(&self, element: &str) -> bool {
        self.fingerprints.contains(element)
    }

    pub fn count(&self, element: &str) -> i64 {
        self.cms_estimate(element.as_bytes())
    }

    pub fn list(&self) -> Vec<(String, i64)> {
        let len = self.heap_len();
        let mut items: Vec<(String, i64)> = (0..len)
            .map(|i| (self.heap_entry_element(i), self.heap_entry_count(i)))
            .collect();
        items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        items
    }

    pub fn info(&self) -> (usize, usize, usize, f64) {
        (self.k, self.width, self.depth, self.decay)
    }

    pub fn msync(&self) -> Result<(), String> {
        let ret = unsafe {
            libc::msync(self.mmap as *mut libc::c_void, self.mmap_len, libc::MS_ASYNC)
        };
        if ret != 0 {
            Err(format!("msync failed: {}", std::io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for MmapTopK {
    fn drop(&mut self) {
        if !self.mmap.is_null() {
            unsafe {
                libc::msync(self.mmap as *mut libc::c_void, self.mmap_len, libc::MS_ASYNC);
                libc::munmap(self.mmap as *mut libc::c_void, self.mmap_len);
            }
        }
    }
}

/// Resource wrapper for MmapTopK.
pub struct MmapTopKResource {
    pub topk: Mutex<MmapTopK>,
}

// -- Mmap TopK NIF functions --

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_create_file(
    env: Env,
    path: String,
    k: usize,
    width: usize,
    depth: usize,
    decay: f64,
) -> NifResult<Term> {
    if !(0.0..=1.0).contains(&decay) {
        return Ok((atoms::error(), "decay must be between 0 and 1").encode(env));
    }
    match MmapTopK::create(Path::new(&path), k, width, depth, decay) {
        Ok(topk) => {
            let resource = ResourceArc::new(MmapTopKResource {
                topk: Mutex::new(topk),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_open_file(env: Env, path: String) -> NifResult<Term> {
    match MmapTopK::open_existing(Path::new(&path)) {
        Ok(topk) => {
            let resource = ResourceArc::new(MmapTopKResource {
                topk: Mutex::new(topk),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_add(
    env: Env,
    resource: ResourceArc<MmapTopKResource>,
    items: Vec<String>,
) -> NifResult<Term> {
    let mut topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<Term> = items
        .iter()
        .map(|item| match topk.add(item, 1) {
            Some(evicted) => evicted.encode(env),
            None => atoms::nil().encode(env),
        })
        .collect();
    let _ = topk.msync();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_incrby(
    env: Env,
    resource: ResourceArc<MmapTopKResource>,
    pairs: Vec<(String, i64)>,
) -> NifResult<Term> {
    let mut topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<Term> = pairs
        .iter()
        .map(|(item, count)| match topk.add(item, *count) {
            Some(evicted) => evicted.encode(env),
            None => atoms::nil().encode(env),
        })
        .collect();
    let _ = topk.msync();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_query(
    env: Env,
    resource: ResourceArc<MmapTopKResource>,
    items: Vec<String>,
) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<i32> = items
        .iter()
        .map(|item| if topk.query(item) { 1 } else { 0 })
        .collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_list(env: Env, resource: ResourceArc<MmapTopKResource>) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let items: Vec<String> = topk.list().into_iter().map(|(elem, _)| elem).collect();
    Ok(items.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_count(
    env: Env,
    resource: ResourceArc<MmapTopKResource>,
    items: Vec<String>,
) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<i64> = items.iter().map(|item| topk.count(item)).collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_info(env: Env, resource: ResourceArc<MmapTopKResource>) -> NifResult<Term> {
    let topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    let (k, width, depth, decay) = topk.info();
    Ok((k, width, depth, decay).encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_close(env: Env, resource: ResourceArc<MmapTopKResource>) -> NifResult<Term> {
    let mut topk = resource.topk.lock().map_err(|_| rustler::Error::BadArg)?;
    if !topk.mmap.is_null() {
        unsafe {
            libc::msync(topk.mmap as *mut libc::c_void, topk.mmap_len, libc::MS_SYNC);
            libc::munmap(topk.mmap as *mut libc::c_void, topk.mmap_len);
        }
        topk.mmap = std::ptr::null_mut();
        topk.mmap_len = 0;
    }
    Ok(atoms::ok().encode(env))
}

// ---------------------------------------------------------------------------
// Rust-only unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_add_and_query() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        assert_eq!(topk.add("a", 1), None);
        assert_eq!(topk.add("b", 1), None);
        assert_eq!(topk.add("c", 1), None);
        assert!(topk.query("a"));
        assert!(topk.query("b"));
        assert!(topk.query("c"));
        assert!(!topk.query("d"));
    }

    #[test]
    fn eviction_when_full() {
        let mut topk = TopK::new(2, 8, 7, 0.9);
        topk.add("a", 100);
        topk.add("b", 50);

        // "c" with count 200 should evict the min
        let evicted = topk.add("c", 200);
        assert!(evicted.is_some());
        let evicted_name = evicted.unwrap();
        assert!(evicted_name == "a" || evicted_name == "b");
    }

    #[test]
    fn no_eviction_when_count_too_low() {
        let mut topk = TopK::new(2, 8, 7, 0.9);
        topk.add("a", 100);
        topk.add("b", 100);
        assert_eq!(topk.add("c", 1), None);
    }

    #[test]
    fn serialization_roundtrip() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        topk.add("alpha", 10);
        topk.add("beta", 20);
        topk.add("gamma", 30);

        let bytes = topk.to_bytes();
        let restored = TopK::from_bytes(&bytes).unwrap();

        assert_eq!(restored.k, 3);
        assert_eq!(restored.width, 8);
        assert_eq!(restored.depth, 7);
        assert!((restored.decay - 0.9).abs() < f64::EPSILON);

        assert!(restored.query("alpha"));
        assert!(restored.query("beta"));
        assert!(restored.query("gamma"));

        let list = restored.list();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].0, "gamma");
        assert_eq!(list[1].0, "beta");
        assert_eq!(list[2].0, "alpha");
    }

    #[test]
    fn list_sorted_desc() {
        let mut topk = TopK::new(5, 8, 7, 0.9);
        topk.add("a", 10);
        topk.add("b", 50);
        topk.add("c", 30);

        let items = topk.list();
        assert_eq!(items[0].0, "b");
        assert_eq!(items[1].0, "c");
        assert_eq!(items[2].0, "a");
    }

    #[test]
    fn update_existing_element() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        topk.add("a", 1);
        topk.add("a", 1);
        topk.add("a", 1);

        assert!(topk.query("a"));
        let items = topk.list();
        assert_eq!(items.len(), 1);
        assert!(items[0].1 >= 3);
    }

    #[test]
    fn empty_topk_list() {
        let topk = TopK::new(5, 8, 7, 0.9);
        assert_eq!(topk.list().len(), 0);
    }

    #[test]
    fn k_equals_one() {
        let mut topk = TopK::new(1, 8, 7, 0.9);
        topk.add("a", 100);
        assert!(topk.query("a"));

        topk.add("b", 200);
        assert!(topk.query("b"));
        assert!(!topk.query("a"));
    }

    #[test]
    fn cms_fnv1a_deterministic() {
        let h1 = fnv1a(b"hello", 0x811c_9dc5);
        let h2 = fnv1a(b"hello", 0x811c_9dc5);
        assert_eq!(h1, h2);

        let h3 = fnv1a(b"world", 0x811c_9dc5);
        assert_ne!(h1, h3);
    }

    #[test]
    fn from_bytes_too_short() {
        let result = TopK::from_bytes(&[0u8; 10]);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn add_k_plus_1_items_one_displaced() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        topk.add("a", 10);
        topk.add("b", 20);
        topk.add("c", 30);
        // Adding a 4th with higher count than the min should displace something
        let displaced = topk.add("d", 40);
        assert!(displaced.is_some(), "expected displacement of min item");
        assert_eq!(topk.list().len(), 3);
    }

    #[test]
    fn query_non_top_item() {
        let mut topk = TopK::new(2, 8, 7, 0.9);
        topk.add("a", 100);
        topk.add("b", 200);
        topk.add("c", 300); // displaces "a"
        assert!(!topk.query("a"), "displaced item should not be queryable");
        assert!(topk.query("b"));
        assert!(topk.query("c"));
    }

    #[test]
    fn list_returns_exactly_k_items() {
        let mut topk = TopK::new(5, 8, 7, 0.9);
        for i in 0..10 {
            topk.add(&format!("item_{i}"), (i + 1) as i64);
        }
        let list = topk.list();
        assert_eq!(list.len(), 5);
    }

    #[test]
    fn list_with_count_sorted_desc() {
        let mut topk = TopK::new(5, 8, 7, 0.9);
        topk.add("low", 1);
        topk.add("mid", 50);
        topk.add("high", 100);

        let items = topk.list();
        let counts: Vec<i64> = items.iter().map(|(_, c)| *c).collect();
        for i in 1..counts.len() {
            assert!(
                counts[i - 1] >= counts[i],
                "list not sorted desc: {:?}",
                counts
            );
        }
    }

    #[test]
    fn decay_old_items_displaced_by_new_frequent() {
        // With k=2 and low initial counts, new items with high increments displace old
        let mut topk = TopK::new(2, 8, 7, 0.9);
        topk.add("old1", 1);
        topk.add("old2", 2);
        // Now add a much more frequent item
        topk.add("new_hot", 1000);
        assert!(topk.query("new_hot"));
        // At least one old item should have been displaced
        let in_topk = [topk.query("old1"), topk.query("old2")];
        assert!(
            in_topk.contains(&false),
            "expected at least one old item displaced"
        );
    }

    #[test]
    fn empty_topk_list_is_empty() {
        let topk = TopK::new(10, 8, 7, 0.9);
        assert_eq!(topk.list().len(), 0);
    }

    #[test]
    fn add_same_item_1000x_always_in_topk() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        for _ in 0..1000 {
            topk.add("frequent", 1);
        }
        assert!(topk.query("frequent"));
        let items = topk.list();
        assert!(
            items.iter().any(|(e, _)| e == "frequent"),
            "frequent item missing from list"
        );
    }

    #[test]
    fn concurrent_adds_with_mutex() {
        use std::sync::{Arc, Mutex};
        let topk = Arc::new(Mutex::new(TopK::new(10, 100, 7, 0.9)));
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let topk_c = Arc::clone(&topk);
                std::thread::spawn(move || {
                    for i in 0..250 {
                        let mut guard = topk_c.lock().unwrap();
                        guard.add(&format!("t{t}_i{i}"), 1);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let guard = topk.lock().unwrap();
        assert_eq!(guard.list().len(), 10);
    }

    #[test]
    fn from_bytes_truncated_element_returns_error() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        topk.add("test", 1);
        let bytes = topk.to_bytes();
        // Truncate mid-way through the heap items
        let truncated = &bytes[..bytes.len() - 5];
        let result = TopK::from_bytes(truncated);
        assert!(result.is_err());
    }

    // ==================================================================
    // Deep NIF edge cases — targeting TopK / FFI safety pitfalls
    // ==================================================================

    #[test]
    fn add_item_longer_than_64kb() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        let big_item = "x".repeat(65536);
        topk.add(&big_item, 100);
        assert!(topk.query(&big_item));
    }

    #[test]
    fn all_items_same_frequency_all_in_topk() {
        let mut topk = TopK::new(5, 100, 7, 0.9);
        for i in 0..5 {
            topk.add(&format!("same_freq_{i}"), 10);
        }
        let list = topk.list();
        assert_eq!(list.len(), 5);
    }

    #[test]
    fn decay_very_low_does_not_crash() {
        // decay = 0.0 is degenerate
        let mut topk = TopK::new(3, 8, 7, 0.0);
        topk.add("a", 10);
        topk.add("b", 20);
        topk.add("c", 30);
        // Should not crash even with zero decay
        let list = topk.list();
        assert!(!list.is_empty());
    }

    #[test]
    fn decay_one_point_zero() {
        // decay = 1.0 means no decay
        let mut topk = TopK::new(3, 8, 7, 1.0);
        topk.add("a", 10);
        topk.add("b", 20);
        let list = topk.list();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn add_empty_string_item() {
        let mut topk = TopK::new(3, 8, 7, 0.9);
        topk.add("", 5);
        assert!(topk.query(""));
        let list = topk.list();
        assert!(list.iter().any(|(e, _)| e.is_empty()));
    }

    #[test]
    fn k_equals_1_constant_replacement() {
        let mut topk = TopK::new(1, 8, 7, 0.9);
        topk.add("first", 10);
        assert!(topk.query("first"));

        // Higher count replaces
        topk.add("second", 100);
        assert!(topk.query("second"));

        // Lower count does not replace
        topk.add("third", 1);
        assert!(!topk.query("third"));
    }

    #[test]
    fn serialize_empty_topk_roundtrip() {
        let topk = TopK::new(5, 8, 7, 0.9);
        let bytes = topk.to_bytes();
        let restored = TopK::from_bytes(&bytes).unwrap();
        assert_eq!(restored.list().len(), 0);
        assert_eq!(restored.k, 5);
    }

    #[test]
    fn incrby_large_count_does_not_overflow() {
        let mut topk = TopK::new(3, 100, 7, 0.9);
        topk.add("big", i64::MAX / 2);
        topk.add("big", i64::MAX / 2);
        // Must not panic
        let list = topk.list();
        assert!(list.iter().any(|(e, _)| e == "big"));
    }

    #[test]
    fn many_unique_items_only_top_k_remain() {
        let mut topk = TopK::new(10, 200, 7, 0.9);
        for i in 0..1000 {
            topk.add(&format!("item_{i}"), (i + 1) as i64);
        }
        let list = topk.list();
        assert_eq!(list.len(), 10);
        // All items in the list should have high counts
        for (_, count) in &list {
            assert!(*count > 0);
        }
    }

    #[test]
    fn concurrent_add_with_mutex_no_corruption() {
        use std::sync::{Arc, Mutex};
        let topk = Arc::new(Mutex::new(TopK::new(10, 100, 7, 0.9)));

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let topk_c = Arc::clone(&topk);
                std::thread::spawn(move || {
                    for i in 0..100 {
                        let mut guard = topk_c.lock().unwrap();
                        guard.add(&format!("t{t}_i{i}"), 1);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let guard = topk.lock().unwrap();
        let list = guard.list();
        assert_eq!(list.len(), 10);
    }

    // ==================================================================
    // Mmap-backed TopK tests
    // ==================================================================

    #[test]
    fn mmap_topk_create_add_query() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.topk");

        let mut topk = MmapTopK::create(&path, 3, 8, 7, 0.9).unwrap();
        assert!(!topk.query("a"));
        topk.add("a", 10);
        assert!(topk.query("a"));
        assert_eq!(topk.heap_len(), 1);
    }

    #[test]
    fn mmap_topk_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.topk");

        {
            let mut topk = MmapTopK::create(&path, 5, 8, 7, 0.9).unwrap();
            topk.add("alpha", 10);
            topk.add("beta", 20);
            topk.add("gamma", 30);
            topk.msync().unwrap();
        }

        let topk = MmapTopK::open_existing(&path).unwrap();
        assert!(topk.query("alpha"));
        assert!(topk.query("beta"));
        assert!(topk.query("gamma"));
        assert_eq!(topk.heap_len(), 3);

        let list = topk.list();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].0, "gamma");
    }

    #[test]
    fn mmap_topk_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("evict.topk");

        let mut topk = MmapTopK::create(&path, 2, 8, 7, 0.9).unwrap();
        topk.add("a", 100);
        topk.add("b", 50);
        let evicted = topk.add("c", 200);
        assert!(evicted.is_some());
        assert_eq!(topk.heap_len(), 2);
        assert!(topk.query("c"));
    }

    #[test]
    fn mmap_topk_no_eviction_low_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_evict.topk");

        let mut topk = MmapTopK::create(&path, 2, 8, 7, 0.9).unwrap();
        topk.add("a", 100);
        topk.add("b", 100);
        assert_eq!(topk.add("c", 1), None);
        assert_eq!(topk.heap_len(), 2);
    }

    #[test]
    fn mmap_topk_update_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("update.topk");

        let mut topk = MmapTopK::create(&path, 5, 8, 7, 0.9).unwrap();
        topk.add("x", 10);
        topk.add("x", 10);
        topk.add("x", 10);
        assert!(topk.query("x"));
        assert_eq!(topk.heap_len(), 1);
    }

    #[test]
    fn mmap_topk_list_sorted_desc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sorted.topk");

        let mut topk = MmapTopK::create(&path, 5, 8, 7, 0.9).unwrap();
        topk.add("low", 1);
        topk.add("mid", 50);
        topk.add("high", 100);

        let items = topk.list();
        let counts: Vec<i64> = items.iter().map(|(_, c)| *c).collect();
        for i in 1..counts.len() {
            assert!(counts[i - 1] >= counts[i], "list not sorted desc: {counts:?}");
        }
    }

    #[test]
    fn mmap_topk_count_via_cms() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("count.topk");

        let mut topk = MmapTopK::create(&path, 5, 100, 7, 0.9).unwrap();
        topk.add("item", 42);
        let est = topk.count("item");
        assert_eq!(est, 42);
    }

    #[test]
    fn mmap_topk_info() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("info.topk");

        let topk = MmapTopK::create(&path, 10, 200, 5, 0.8).unwrap();
        let (k, w, d, decay) = topk.info();
        assert_eq!(k, 10);
        assert_eq!(w, 200);
        assert_eq!(d, 5);
        assert!((decay - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn mmap_topk_open_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.topk");
        std::fs::write(&path, &[0xFF; 128]).unwrap();
        let result = MmapTopK::open_existing(&path);
        assert!(result.is_err());
    }

    #[test]
    fn mmap_topk_open_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.topk");
        let result = MmapTopK::open_existing(&path);
        assert!(result.is_err());
    }

    #[test]
    fn mmap_topk_nested_dir_create() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a").join("b").join("c").join("test.topk");
        let topk = MmapTopK::create(&path, 3, 8, 7, 0.9).unwrap();
        assert!(path.exists());
        assert_eq!(topk.heap_len(), 0);
    }

    #[test]
    fn mmap_topk_many_items() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many.topk");

        let mut topk = MmapTopK::create(&path, 10, 200, 7, 0.9).unwrap();
        for i in 0..100 {
            topk.add(&format!("item_{i}"), (i + 1) as i64);
        }
        assert_eq!(topk.list().len(), 10);
    }

    #[test]
    fn mmap_topk_reopen_after_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.topk");

        {
            let mut topk = MmapTopK::create(&path, 2, 100, 7, 0.9).unwrap();
            topk.add("stay_high", 1000);
            topk.add("stay_mid", 500);
            topk.add("evict_me", 1); // should not evict (count too low)
            topk.msync().unwrap();
        }

        let topk = MmapTopK::open_existing(&path).unwrap();
        assert!(topk.query("stay_high"));
        assert!(topk.query("stay_mid"));
        assert_eq!(topk.heap_len(), 2);
    }

    // ------------------------------------------------------------------
    // L-5: update_existing uses into_vec + in-place mutation (no extra alloc)
    // ------------------------------------------------------------------

    #[test]
    fn l5_update_existing_preserves_heap_invariant() {
        let mut topk = TopK::new(5, 128, 4, 1.0);

        // Add 5 items with increasing counts
        for i in 0..5 {
            topk.add(&format!("item_{i}"), (i + 1) as i64);
        }

        // All 5 should be in the top-K
        for i in 0..5 {
            assert!(
                topk.query(&format!("item_{i}")),
                "item_{i} should be in top-K"
            );
        }

        // Update item_0 many times to increase its count
        for _ in 0..50 {
            topk.add("item_0", 1);
        }

        // item_0 should still be in the heap with an updated count
        assert!(topk.query("item_0"), "item_0 must still be in top-K");

        // Verify heap ordering: list() returns sorted desc
        let list = topk.list();
        assert_eq!(list.len(), 5);
        for i in 0..list.len() - 1 {
            assert!(
                list[i].1 >= list[i + 1].1,
                "list must be sorted descending by count"
            );
        }

        // item_0 should be near the top since its count was incremented 50+ times
        assert_eq!(
            list[0].0, "item_0",
            "item_0 must be the highest count after 50 increments"
        );
    }
}
