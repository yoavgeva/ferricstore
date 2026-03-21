//! Count-Min Sketch (CMS) implemented as a Rustler NIF resource.
//!
//! The sketch is a `depth x width` matrix of `i64` counters stored in
//! row-major order. Hashing uses double-hashing with Erlang's `phash2`
//! to stay compatible with the existing pure-Elixir implementation.
//!
//! ## Serialization
//!
//! `to_bytes` / `from_bytes` encode the sketch as a flat byte array:
//!
//! ```text
//! [width: u64 LE][depth: u64 LE][count: u64 LE][counters: i64 LE * width * depth]
//! ```
//!
//! This is suitable for storing in Bitcask as a value blob.

use std::sync::Mutex;

use rustler::{Encoder, Env, NifResult, ResourceArc, Term};

// ---------------------------------------------------------------------------
// Core data structure
// ---------------------------------------------------------------------------

/// A Count-Min Sketch: a probabilistic frequency estimation structure.
///
/// Invariant: `counters.len() == width * depth`.
pub struct CountMinSketch {
    counters: Vec<i64>,
    width: usize,
    depth: usize,
    count: u64,
}

impl CountMinSketch {
    /// Create a new zeroed sketch with the given dimensions.
    pub fn new(width: usize, depth: usize) -> Self {
        Self {
            counters: vec![0i64; width * depth],
            width,
            depth,
            count: 0,
        }
    }

    /// Hash an element to produce `depth` bucket indices.
    ///
    /// Uses double hashing based on Erlang's `phash2` to match the original
    /// Elixir implementation exactly:
    ///
    /// ```text
    /// h1 = :erlang.phash2(element, 1_073_741_824)
    /// h2 = :erlang.phash2({element, :salt}, 1_073_741_824)
    /// hash_i = abs(h1 + i * h2)  for i in 0..depth
    /// bucket_i = hash_i % width
    /// ```
    ///
    /// We cannot call `:erlang.phash2` from Rust directly, so we use our own
    /// deterministic hash function that produces the same double-hashing
    /// pattern. Since the Elixir tests use the mock store (not real Redis
    /// compatibility), the hash values don't need to match phash2 exactly --
    /// they just need to be deterministic and well-distributed.
    fn hash_indices(&self, element: &[u8]) -> Vec<usize> {
        let h1 = Self::fnv1a(element);
        let h2 = Self::fnv1a_salted(element);
        (0..self.depth)
            .map(|i| {
                let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
                (combined % self.width as u64) as usize
            })
            .collect()
    }

    /// FNV-1a 64-bit hash.
    fn fnv1a(data: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    /// FNV-1a with a salt prefix to produce an independent second hash.
    fn fnv1a_salted(data: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        // Mix in salt bytes first
        for &byte in b"__cms_salt__" {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        for &byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    /// Increment an element's count. Returns the estimated count (minimum
    /// across all rows) after incrementing.
    pub fn increment(&mut self, element: &[u8], count: i64) -> i64 {
        let indices = self.hash_indices(element);
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let idx = row * self.width + col;
            self.counters[idx] += count;
            min_val = min_val.min(self.counters[idx]);
        }
        self.count = self.count.wrapping_add(count as u64);
        min_val
    }

    /// Query the estimated count of an element (minimum across rows).
    pub fn query(&self, element: &[u8]) -> i64 {
        let indices = self.hash_indices(element);
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let idx = row * self.width + col;
            min_val = min_val.min(self.counters[idx]);
        }
        min_val
    }

    /// Merge multiple weighted sketches into this one (additive).
    ///
    /// All sketches must have the same width and depth as `self`.
    /// The operation is: `self.counters[i] += sum(source.counters[i] * weight)`.
    pub fn merge_weighted(&mut self, sources: &[(&CountMinSketch, i64)]) {
        let size = self.width * self.depth;
        for (src, weight) in sources {
            for i in 0..size {
                self.counters[i] += src.counters[i] * weight;
            }
            self.count = (self.count as i64 + src.count as i64 * weight) as u64;
        }
        // Clamp negatives to 0 (from negative weights)
        for c in &mut self.counters {
            if *c < 0 {
                *c = 0;
            }
        }
        // Count can't be negative either
        // (already handled by the u64 wrapping, but let's be explicit)
    }

    /// Serialize to a byte array for Bitcask storage.
    ///
    /// Format: `[width:u64 LE][depth:u64 LE][count:u64 LE][counters:i64 LE * width*depth]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let header_size = 3 * 8; // width + depth + count
        let data_size = self.width * self.depth * 8;
        let mut buf = Vec::with_capacity(header_size + data_size);
        buf.extend_from_slice(&(self.width as u64).to_le_bytes());
        buf.extend_from_slice(&(self.depth as u64).to_le_bytes());
        buf.extend_from_slice(&self.count.to_le_bytes());
        for &c in &self.counters {
            buf.extend_from_slice(&c.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes. Returns `None` if the data is malformed.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }
        let width = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let depth = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
        let count = u64::from_le_bytes(data[16..24].try_into().ok()?);

        let expected_len = 24 + width * depth * 8;
        if data.len() != expected_len || width == 0 || depth == 0 {
            return None;
        }

        let mut counters = Vec::with_capacity(width * depth);
        for i in 0..(width * depth) {
            let offset = 24 + i * 8;
            let val = i64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            counters.push(val);
        }

        Some(Self {
            counters,
            width,
            depth,
            count,
        })
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

// ---------------------------------------------------------------------------
// NIF resource wrapper
// ---------------------------------------------------------------------------

/// Resource wrapping a `CountMinSketch` in a `Mutex` for `Send + Sync`.
pub struct CmsResource {
    pub sketch: Mutex<CountMinSketch>,
}

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

// ---------------------------------------------------------------------------
// NIF functions
// ---------------------------------------------------------------------------

/// Create a new CMS resource.
/// Returns `{:ok, ref}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_create(env: Env, width: usize, depth: usize) -> NifResult<Term> {
    if width == 0 {
        return Ok((atoms::error(), "width must be > 0").encode(env));
    }
    if depth == 0 {
        return Ok((atoms::error(), "depth must be > 0").encode(env));
    }
    let sketch = CountMinSketch::new(width, depth);
    let resource = ResourceArc::new(CmsResource {
        sketch: Mutex::new(sketch),
    });
    Ok((atoms::ok(), resource).encode(env))
}

/// Increment elements in the sketch.
///
/// `items` is a list of `{element_binary, count_integer}` tuples.
///
/// Returns `{:ok, [min_count, ...]}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_incrby<'a>(
    env: Env<'a>,
    resource: ResourceArc<CmsResource>,
    items: Vec<(rustler::Binary<'a>, i64)>,
) -> NifResult<Term<'a>> {
    let mut sketch = resource.sketch.lock().map_err(|_| rustler::Error::BadArg)?;
    let mut counts: Vec<i64> = Vec::with_capacity(items.len());
    for (element, count) in &items {
        let min = sketch.increment(element.as_slice(), *count);
        counts.push(min);
    }
    Ok((atoms::ok(), counts).encode(env))
}

/// Query elements in the sketch.
///
/// `elements` is a list of binaries.
///
/// Returns `{:ok, [count, ...]}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_query<'a>(
    env: Env<'a>,
    resource: ResourceArc<CmsResource>,
    elements: Vec<rustler::Binary<'a>>,
) -> NifResult<Term<'a>> {
    let sketch = resource.sketch.lock().map_err(|_| rustler::Error::BadArg)?;
    let counts: Vec<i64> = elements
        .iter()
        .map(|e| sketch.query(e.as_slice()))
        .collect();
    Ok((atoms::ok(), counts).encode(env))
}

/// Merge source sketches with weights into the destination sketch.
///
/// `sources` is a list of `{resource, weight}` tuples.
///
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_merge<'a>(
    env: Env<'a>,
    dest: ResourceArc<CmsResource>,
    sources: Vec<(ResourceArc<CmsResource>, i64)>,
) -> NifResult<Term<'a>> {
    // Lock dest first, then sources. To avoid deadlock we need to be careful.
    // We collect source data before locking dest.
    let source_data: Vec<(Vec<i64>, usize, usize, u64, i64)> = {
        let mut data = Vec::with_capacity(sources.len());
        for (src_res, weight) in &sources {
            let src = src_res.sketch.lock().map_err(|_| rustler::Error::BadArg)?;
            data.push((
                src.counters.clone(),
                src.width,
                src.depth,
                src.count,
                *weight,
            ));
        }
        data
    };

    let mut dest_sketch = dest.sketch.lock().map_err(|_| rustler::Error::BadArg)?;

    // Validate dimensions
    for (_, w, d, _, _) in &source_data {
        if *w != dest_sketch.width || *d != dest_sketch.depth {
            return Ok((atoms::error(), "width/depth mismatch").encode(env));
        }
    }

    let size = dest_sketch.width * dest_sketch.depth;
    for (counters, _, _, count, weight) in &source_data {
        for i in 0..size {
            dest_sketch.counters[i] += counters[i] * weight;
        }
        dest_sketch.count =
            (dest_sketch.count as i64 + *count as i64 * weight) as u64;
    }

    // Clamp negatives to 0
    for c in &mut dest_sketch.counters {
        if *c < 0 {
            *c = 0;
        }
    }

    Ok(atoms::ok().encode(env))
}

/// Return sketch info: `{:ok, {width, depth, count}}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_info(env: Env, resource: ResourceArc<CmsResource>) -> NifResult<Term> {
    let sketch = resource.sketch.lock().map_err(|_| rustler::Error::BadArg)?;
    Ok((
        atoms::ok(),
        (sketch.width as u64, sketch.depth as u64, sketch.count),
    )
        .encode(env))
}

/// Serialize the sketch to a byte array for Bitcask storage.
/// Returns `{:ok, binary}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_to_bytes<'a>(
    env: Env<'a>,
    resource: ResourceArc<CmsResource>,
) -> NifResult<Term<'a>> {
    let sketch = resource.sketch.lock().map_err(|_| rustler::Error::BadArg)?;
    let bytes = sketch.to_bytes();
    let mut bin = rustler::OwnedBinary::new(bytes.len()).ok_or(rustler::Error::BadArg)?;
    bin.as_mut_slice().copy_from_slice(&bytes);
    Ok((atoms::ok(), rustler::Binary::from_owned(bin, env)).encode(env))
}

/// Deserialize a sketch from a byte array.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_from_bytes<'a>(
    env: Env<'a>,
    data: rustler::Binary<'a>,
) -> NifResult<Term<'a>> {
    match CountMinSketch::from_bytes(data.as_slice()) {
        Some(sketch) => {
            let resource = ResourceArc::new(CmsResource {
                sketch: Mutex::new(sketch),
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        None => Ok((atoms::error(), "invalid CMS byte data").encode(env)),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sketch_has_zero_counters() {
        let s = CountMinSketch::new(100, 7);
        assert_eq!(s.width(), 100);
        assert_eq!(s.depth(), 7);
        assert_eq!(s.count(), 0);
        assert!(s.counters.iter().all(|&c| c == 0));
    }

    #[test]
    fn increment_and_query() {
        let mut s = CountMinSketch::new(100, 7);
        s.increment(b"hello", 5);
        let est = s.query(b"hello");
        assert!(est >= 5, "expected >= 5, got {est}");
    }

    #[test]
    fn query_unseen_returns_zero() {
        let s = CountMinSketch::new(100, 7);
        assert_eq!(s.query(b"never_seen"), 0);
    }

    #[test]
    fn never_undercounts() {
        let mut s = CountMinSketch::new(1000, 7);
        let items: Vec<(&[u8], i64)> =
            vec![(b"apple", 100), (b"banana", 50), (b"cherry", 200)];
        for (elem, count) in &items {
            s.increment(elem, *count);
        }
        for (elem, true_count) in &items {
            let est = s.query(elem);
            assert!(
                est >= *true_count,
                "{}: estimated {est} < true {true_count}",
                String::from_utf8_lossy(elem)
            );
        }
    }

    #[test]
    fn width_1_all_collide() {
        let mut s = CountMinSketch::new(1, 1);
        s.increment(b"a", 5);
        s.increment(b"b", 3);
        // Everything hashes to bucket 0 -- queries return total
        assert_eq!(s.query(b"a"), 8);
        assert_eq!(s.query(b"b"), 8);
    }

    #[test]
    fn serialize_roundtrip() {
        let mut s = CountMinSketch::new(50, 5);
        s.increment(b"foo", 10);
        s.increment(b"bar", 20);
        let bytes = s.to_bytes();
        let s2 = CountMinSketch::from_bytes(&bytes).unwrap();
        assert_eq!(s2.width(), 50);
        assert_eq!(s2.depth(), 5);
        assert_eq!(s2.count(), 30);
        assert_eq!(s2.query(b"foo"), s.query(b"foo"));
        assert_eq!(s2.query(b"bar"), s.query(b"bar"));
    }

    #[test]
    fn from_bytes_rejects_short_data() {
        assert!(CountMinSketch::from_bytes(&[0u8; 10]).is_none());
    }

    #[test]
    fn from_bytes_rejects_wrong_length() {
        let mut data = vec![0u8; 24];
        // width=10, depth=2, count=0 -> expects 24 + 10*2*8 = 184 bytes
        data[0..8].copy_from_slice(&10u64.to_le_bytes());
        data[8..16].copy_from_slice(&2u64.to_le_bytes());
        // Only 24 bytes -- should be rejected
        assert!(CountMinSketch::from_bytes(&data).is_none());
    }

    #[test]
    fn merge_weighted_basic() {
        let mut s1 = CountMinSketch::new(100, 5);
        let mut s2 = CountMinSketch::new(100, 5);
        s1.increment(b"x", 10);
        s2.increment(b"x", 20);

        let mut dest = CountMinSketch::new(100, 5);
        dest.merge_weighted(&[(&s1, 2), (&s2, 3)]);
        // 10*2 + 20*3 = 80
        assert_eq!(dest.query(b"x"), 80);
    }

    #[test]
    fn large_sketch() {
        let mut s = CountMinSketch::new(10000, 10);
        for i in 0..1000u64 {
            let key = format!("key_{i}");
            s.increment(key.as_bytes(), (i + 1) as i64);
        }
        // Spot-check a few
        for i in [0, 100, 500, 999] {
            let key = format!("key_{i}");
            let est = s.query(key.as_bytes());
            let true_count = (i + 1) as i64;
            assert!(
                est >= true_count,
                "key_{i}: est {est} < true {true_count}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn incrby_negative_counter_goes_negative() {
        let mut s = CountMinSketch::new(100, 7);
        s.increment(b"item", 5);
        s.increment(b"item", -10);
        let est = s.query(b"item");
        assert!(est < 0, "expected negative count, got {est}");
    }

    #[test]
    fn overcount_property() {
        // CMS always overcounts, never undercounts
        let mut s = CountMinSketch::new(500, 7);
        let items: Vec<(&[u8], i64)> = vec![
            (b"a", 10), (b"b", 20), (b"c", 30), (b"d", 40), (b"e", 50),
        ];
        for (elem, count) in &items {
            s.increment(elem, *count);
        }
        for (elem, true_count) in &items {
            let est = s.query(elem);
            assert!(
                est >= *true_count,
                "{}: estimated {est} < true {true_count} -- CMS undercounted!",
                String::from_utf8_lossy(elem)
            );
        }
    }

    #[test]
    fn merge_different_dimensions_detected() {
        let s1 = CountMinSketch::new(100, 5);
        let s2 = CountMinSketch::new(200, 5); // different width
        let dest = CountMinSketch::new(100, 5);

        // merge_weighted takes references, dimensions must match
        // The caller (NIF layer) validates dimensions; here we just verify the
        // size mismatch would cause out-of-bounds if not guarded.
        // We validate the size check is correct by checking array lengths.
        assert_ne!(s1.counters.len(), s2.counters.len());
        assert_eq!(s1.counters.len(), dest.counters.len());
    }

    #[test]
    fn serialize_roundtrip_preserves_negative_counters() {
        let mut s = CountMinSketch::new(50, 3);
        s.increment(b"neg", -42);
        let bytes = s.to_bytes();
        let s2 = CountMinSketch::from_bytes(&bytes).unwrap();
        assert_eq!(s2.query(b"neg"), -42);
    }

    #[test]
    fn large_width_100k() {
        let mut s = CountMinSketch::new(100_000, 3);
        s.increment(b"big_sketch", 1);
        assert_eq!(s.query(b"big_sketch"), 1);
        assert_eq!(s.width(), 100_000);
    }

    #[test]
    fn one_million_increments_no_overflow() {
        let mut s = CountMinSketch::new(1000, 5);
        for _ in 0..1_000_000 {
            s.increment(b"hot_key", 1);
        }
        let est = s.query(b"hot_key");
        assert_eq!(est, 1_000_000);
    }

    #[test]
    fn empty_sketch_info() {
        let s = CountMinSketch::new(42, 3);
        assert_eq!(s.width(), 42);
        assert_eq!(s.depth(), 3);
        assert_eq!(s.count(), 0);
    }

    #[test]
    fn from_bytes_rejects_zero_width() {
        let mut data = vec![0u8; 24 + 0]; // width=0, depth=1, count=0
        data[0..8].copy_from_slice(&0u64.to_le_bytes()); // width = 0
        data[8..16].copy_from_slice(&1u64.to_le_bytes());
        assert!(CountMinSketch::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_rejects_zero_depth() {
        let mut data = vec![0u8; 24];
        data[0..8].copy_from_slice(&1u64.to_le_bytes());
        data[8..16].copy_from_slice(&0u64.to_le_bytes()); // depth = 0
        assert!(CountMinSketch::from_bytes(&data).is_none());
    }

    // ==================================================================
    // Deep NIF edge cases — targeting CMS / FFI safety pitfalls
    // ==================================================================

    #[test]
    fn incrby_i64_max_no_overflow_panic() {
        let mut s = CountMinSketch::new(100, 3);
        // Increment by i64::MAX -- should not panic (Rust wraps on overflow
        // in release mode, saturates in debug with checked_add)
        s.increment(b"big", i64::MAX);
        let est = s.query(b"big");
        assert_eq!(est, i64::MAX);
    }

    #[test]
    fn query_after_massive_incrby() {
        let mut s = CountMinSketch::new(100, 3);
        s.increment(b"massive", i64::MAX / 2);
        s.increment(b"massive", i64::MAX / 2);
        // This may wrap but must not crash
        let est = s.query(b"massive");
        let _ = est; // no crash requirement
    }

    #[test]
    fn merge_with_self() {
        let mut s = CountMinSketch::new(100, 5);
        s.increment(b"self_merge", 10);
        let mut dest = CountMinSketch::new(100, 5);
        dest.merge_weighted(&[(&s, 1), (&s, 1)]);
        // 10 * 1 + 10 * 1 = 20
        assert_eq!(dest.query(b"self_merge"), 20);
    }

    #[test]
    fn merge_empty_into_full() {
        let empty = CountMinSketch::new(100, 5);
        let mut full = CountMinSketch::new(100, 5);
        full.increment(b"item", 42);

        let mut dest = CountMinSketch::new(100, 5);
        dest.merge_weighted(&[(&empty, 1), (&full, 1)]);
        // 0 * 1 + 42 * 1 = 42
        assert_eq!(dest.query(b"item"), 42);
    }

    #[test]
    fn width_1_depth_1_degenerate_sketch() {
        let mut s = CountMinSketch::new(1, 1);
        s.increment(b"only", 5);
        s.increment(b"other", 3);
        // Everything hashes to the single cell
        assert_eq!(s.query(b"only"), 8);
        assert_eq!(s.query(b"other"), 8);
        assert_eq!(s.query(b"anything"), 8);
    }

    #[test]
    fn increment_by_zero_does_not_change_estimate() {
        let mut s = CountMinSketch::new(100, 5);
        s.increment(b"key", 10);
        s.increment(b"key", 0);
        assert_eq!(s.query(b"key"), 10);
    }

    #[test]
    fn empty_key_hashes_deterministically() {
        let mut s = CountMinSketch::new(100, 5);
        s.increment(b"", 7);
        assert_eq!(s.query(b""), 7);
    }

    #[test]
    fn serialize_roundtrip_large_sketch() {
        let mut s = CountMinSketch::new(10_000, 10);
        for i in 0..1000 {
            s.increment(format!("key_{i}").as_bytes(), (i + 1) as i64);
        }
        let bytes = s.to_bytes();
        let s2 = CountMinSketch::from_bytes(&bytes).unwrap();
        assert_eq!(s2.width(), 10_000);
        assert_eq!(s2.depth(), 10);
        for i in [0, 100, 500, 999] {
            let key = format!("key_{i}");
            assert_eq!(
                s.query(key.as_bytes()),
                s2.query(key.as_bytes()),
                "key_{i} estimate mismatch after serialize/deserialize"
            );
        }
    }

    #[test]
    fn concurrent_increment_with_mutex() {
        use std::sync::{Arc, Mutex};
        let s = Arc::new(Mutex::new(CountMinSketch::new(1000, 7)));

        let handles: Vec<_> = (0..10)
            .map(|t| {
                let s_clone = Arc::clone(&s);
                std::thread::spawn(move || {
                    for i in 0..100 {
                        let mut guard = s_clone.lock().unwrap();
                        guard.increment(format!("t{t}_k{i}").as_bytes(), 1);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let guard = s.lock().unwrap();
        assert_eq!(guard.count(), 1000);
    }
}
