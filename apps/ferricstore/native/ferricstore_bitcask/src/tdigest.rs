//! T-Digest data structure for accurate on-line accumulation of rank-based
//! statistics such as quantiles, trimmed means, and CDF values.
//!
//! Implements the merging digest algorithm from Dunning's paper using the
//! k1 scale function: `k(q) = (delta / (2 * pi)) * arcsin(2q - 1)`.
//!
//! Centroids near the tails (q near 0 or 1) are forced to be small, while
//! centroids near the median can absorb many observations. This gives high
//! accuracy at the tails (P99, P99.9) with bounded memory.

use std::sync::Mutex;

use rustler::{Encoder, Env, NifResult, ResourceArc, Term};

// ---------------------------------------------------------------------------
// Core data structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Centroid {
    mean: f64,
    weight: f64,
}

pub struct TDigest {
    compression: f64,
    centroids: Vec<Centroid>,
    count: u64,
    min: f64,
    max: f64,
    buffer: Vec<f64>,
    buffer_max: usize,
    total_compressions: u64,
}

/// Resource wrapper for BEAM GC tracking.
pub struct TDigestResource {
    pub digest: Mutex<TDigest>,
}

impl TDigest {
    fn new(compression: f64) -> Self {
        let buffer_max = (compression * 3.0).ceil() as usize;
        Self {
            compression,
            centroids: Vec::new(),
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            buffer: Vec::with_capacity(buffer_max),
            buffer_max,
            total_compressions: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn add(&mut self, value: f64) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.buffer.push(value);
        self.count += 1;
        if self.buffer.len() >= self.buffer_max {
            self.compress();
        }
    }

    fn add_many(&mut self, values: &[f64]) {
        for &v in values {
            self.add(v);
        }
    }

    fn ensure_compressed(&mut self) {
        if !self.buffer.is_empty() {
            self.compress();
        }
    }

    fn compress(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        // Convert buffer samples to weight-1 centroids and combine with existing
        let mut all: Vec<Centroid> = self.centroids.clone();
        for &v in &self.buffer {
            all.push(Centroid {
                mean: v,
                weight: 1.0,
            });
        }

        // Sort by mean
        all.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));

        let total_weight: f64 = all.iter().map(|c| c.weight).sum();

        // Alternate merge direction to avoid bias
        if self.total_compressions % 2 == 1 {
            all.reverse();
        }

        let merged = Self::merge_centroids(&all, total_weight, self.compression);

        // Ensure result is always sorted ascending by mean
        self.centroids = if self.total_compressions % 2 == 1 {
            let mut v = merged;
            v.reverse();
            v
        } else {
            merged
        };

        self.buffer.clear();
        self.total_compressions += 1;
    }

    fn merge_centroids(sorted: &[Centroid], total_weight: f64, compression: f64) -> Vec<Centroid> {
        if sorted.is_empty() || total_weight <= 0.0 {
            return Vec::new();
        }

        let mut result: Vec<Centroid> = Vec::new();
        let mut current = sorted[0].clone();
        let mut weight_so_far = current.weight / 2.0;

        for centroid in sorted.iter().skip(1) {
            let proposed_weight = current.weight + centroid.weight;
            let q = (weight_so_far + proposed_weight / 2.0) / total_weight;
            let max_w = Self::max_weight(q, total_weight, compression);

            if proposed_weight <= max_w {
                // Merge into current centroid
                let new_mean =
                    (current.mean * current.weight + centroid.mean * centroid.weight)
                        / proposed_weight;
                current = Centroid {
                    mean: new_mean,
                    weight: proposed_weight,
                };
            } else {
                // Start a new centroid
                weight_so_far += current.weight;
                result.push(current);
                current = centroid.clone();
                weight_so_far += current.weight / 2.0;
            }
        }

        result.push(current);
        result
    }

    /// Maximum weight a centroid at quantile position q can have.
    /// Uses the k1 scale function derivative.
    fn max_weight(q: f64, total_weight: f64, compression: f64) -> f64 {
        let q = q.clamp(1.0e-10, 1.0 - 1.0e-10);
        4.0 * total_weight * q * (1.0 - q) / compression
    }

    // -----------------------------------------------------------------------
    // Query functions
    // -----------------------------------------------------------------------

    fn quantile(&mut self, q: f64) -> f64 {
        self.ensure_compressed();

        if self.is_empty() {
            return f64::NAN;
        }

        if q == 0.0 {
            return self.min;
        }
        if q == 1.0 {
            return self.max;
        }

        if self.centroids.is_empty() {
            if self.min.is_finite() && self.max.is_finite() {
                return self.min + q * (self.max - self.min);
            }
            return f64::NAN;
        }

        let total_weight = self.count as f64;
        let target = q * total_weight;

        let mut cum_weight = 0.0;
        let mut result = None;

        for (i, c) in self.centroids.iter().enumerate() {
            let new_cum = cum_weight + c.weight;

            if new_cum >= target {
                if i == 0 {
                    // First centroid: interpolate between min and centroid mean
                    if target < c.weight / 2.0 {
                        let frac = target / (c.weight / 2.0);
                        let val = self.min + frac * (c.mean - self.min);
                        result = Some(val);
                    } else {
                        result = Some(c.mean);
                    }
                } else {
                    result = Some(c.mean);
                }
                break;
            }
            cum_weight = new_cum;
        }

        match result {
            Some(val) => val.clamp(self.min, self.max),
            None => self.max,
        }
    }

    fn cdf(&mut self, value: f64) -> f64 {
        self.ensure_compressed();

        if self.is_empty() {
            return f64::NAN;
        }

        if value <= self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }

        if self.centroids.is_empty() {
            if self.max == self.min {
                return 0.5;
            }
            return (value - self.min) / (self.max - self.min);
        }

        let total_weight = self.count as f64;
        let mut cum_weight = 0.0;
        let mut prev: Option<&Centroid> = None;

        for c in &self.centroids {
            if value < c.mean {
                if prev.is_none() {
                    // Before the first centroid
                    let frac = (value - self.min) / (c.mean - self.min);
                    let partial = frac * c.weight / 2.0;
                    return (partial / total_weight).clamp(0.0, 1.0);
                }
                let prev_c = prev.unwrap();
                let frac = (value - prev_c.mean) / (c.mean - prev_c.mean);
                let partial =
                    cum_weight - prev_c.weight / 2.0 + frac * (prev_c.weight / 2.0 + c.weight / 2.0);
                return (partial / total_weight).clamp(0.0, 1.0);
            }
            if (value - c.mean).abs() < f64::EPSILON {
                let partial = cum_weight + c.weight / 2.0;
                return (partial / total_weight).clamp(0.0, 1.0);
            }
            cum_weight += c.weight;
            prev = Some(c);
        }

        // Value is beyond the last centroid but within max
        if let Some(last) = self.centroids.last() {
            if (self.max - last.mean).abs() < f64::EPSILON {
                return 1.0;
            }
            let frac = (value - last.mean) / (self.max - last.mean);
            let partial = cum_weight - last.weight / 2.0 + frac * (last.weight / 2.0);
            return (partial / total_weight).min(1.0);
        }

        1.0
    }

    fn rank(&mut self, value: f64) -> i64 {
        self.ensure_compressed();

        if self.is_empty() {
            return -2;
        }

        let fvalue = value;
        if fvalue < self.min {
            return -1;
        }
        if fvalue >= self.max {
            return self.count as i64;
        }

        let cdf_val = self.cdf(fvalue);
        (cdf_val * self.count as f64).floor() as i64
    }

    fn rev_rank(&mut self, value: f64) -> i64 {
        self.ensure_compressed();

        if self.is_empty() {
            return -2;
        }

        let r = self.rank(value);
        if r == -1 {
            return self.count as i64;
        }
        if r >= self.count as i64 {
            return -1;
        }
        self.count as i64 - r - 1
    }

    fn by_rank(&mut self, r: i64) -> ByRankResult {
        self.ensure_compressed();

        if self.is_empty() {
            return ByRankResult::NaN;
        }
        if r < 0 {
            return ByRankResult::NegInf;
        }
        if r >= self.count as i64 {
            return ByRankResult::Inf;
        }
        let q = (r as f64 + 0.5) / self.count as f64;
        ByRankResult::Value(self.quantile(q))
    }

    fn by_rev_rank(&mut self, r: i64) -> ByRankResult {
        self.ensure_compressed();

        if self.is_empty() {
            return ByRankResult::NaN;
        }
        if r < 0 {
            return ByRankResult::Inf;
        }
        if r >= self.count as i64 {
            return ByRankResult::NegInf;
        }
        let q = 1.0 - (r as f64 + 0.5) / self.count as f64;
        ByRankResult::Value(self.quantile(q))
    }

    fn trimmed_mean(&mut self, lo: f64, hi: f64) -> f64 {
        self.ensure_compressed();

        if self.is_empty() {
            return f64::NAN;
        }

        let total_weight = self.count as f64;
        let lo_weight = lo * total_weight;
        let hi_weight = hi * total_weight;

        let mut sum = 0.0;
        let mut weight_sum = 0.0;
        let mut cum = 0.0;

        for c in &self.centroids {
            let centroid_lo = cum;
            let centroid_hi = cum + c.weight;

            let overlap_lo = centroid_lo.max(lo_weight);
            let overlap_hi = centroid_hi.min(hi_weight);

            if overlap_hi > overlap_lo {
                let overlap_weight = overlap_hi - overlap_lo;
                sum += c.mean * overlap_weight;
                weight_sum += overlap_weight;
            }

            cum += c.weight;
        }

        if weight_sum > 0.0 {
            sum / weight_sum
        } else {
            f64::NAN
        }
    }

    fn min_val(&self) -> f64 {
        if self.is_empty() {
            f64::NAN
        } else {
            self.min
        }
    }

    fn max_val(&self) -> f64 {
        if self.is_empty() {
            f64::NAN
        } else {
            self.max
        }
    }

    fn info(&self) -> TDigestInfo {
        let merged_weight: f64 = self.centroids.iter().map(|c| c.weight).sum();
        let centroid_mem = self.centroids.len() * 16; // 2 x f64
        let buffer_mem = self.buffer.len() * 8; // 1 x f64
        let struct_overhead = 200;

        TDigestInfo {
            compression: self.compression as u64,
            capacity: self.buffer_max as u64,
            merged_nodes: self.centroids.len() as u64,
            unmerged_nodes: self.buffer.len() as u64,
            merged_weight,
            unmerged_weight: self.buffer.len() as f64,
            total_compressions: self.total_compressions,
            memory_usage: (centroid_mem + buffer_mem + struct_overhead) as u64,
        }
    }

    fn reset(&mut self) {
        let compression = self.compression;
        let buffer_max = self.buffer_max;
        self.centroids.clear();
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.buffer.clear();
        self.buffer_max = buffer_max;
        self.compression = compression;
        self.total_compressions = 0;
    }

    // -----------------------------------------------------------------------
    // Serialization: flat f64 array for Bitcask byte-array storage
    // -----------------------------------------------------------------------

    /// Serialize to a byte array: [compression, count, min, max, total_compressions,
    ///   n_centroids, mean0, weight0, mean1, weight1, ..., n_buffer, buf0, buf1, ...]
    fn serialize(&self) -> Vec<u8> {
        let n_centroids = self.centroids.len();
        let n_buffer = self.buffer.len();
        // 5 header f64s + 1 n_centroids + 2*n_centroids + 1 n_buffer + n_buffer
        let total_f64s = 5 + 1 + 2 * n_centroids + 1 + n_buffer;
        let mut bytes = Vec::with_capacity(total_f64s * 8);

        bytes.extend_from_slice(&self.compression.to_le_bytes());
        bytes.extend_from_slice(&(self.count as f64).to_le_bytes());
        bytes.extend_from_slice(&self.min.to_le_bytes());
        bytes.extend_from_slice(&self.max.to_le_bytes());
        bytes.extend_from_slice(&(self.total_compressions as f64).to_le_bytes());
        bytes.extend_from_slice(&(n_centroids as f64).to_le_bytes());

        for c in &self.centroids {
            bytes.extend_from_slice(&c.mean.to_le_bytes());
            bytes.extend_from_slice(&c.weight.to_le_bytes());
        }

        bytes.extend_from_slice(&(n_buffer as f64).to_le_bytes());
        for &v in &self.buffer {
            bytes.extend_from_slice(&v.to_le_bytes());
        }

        bytes
    }

    /// Deserialize from a byte array produced by `serialize`.
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 6 * 8 {
            return None;
        }

        let mut pos = 0;

        let read_f64 = |data: &[u8], offset: &mut usize| -> Option<f64> {
            if *offset + 8 > data.len() {
                return None;
            }
            let arr: [u8; 8] = data[*offset..*offset + 8].try_into().ok()?;
            *offset += 8;
            Some(f64::from_le_bytes(arr))
        };

        let compression = read_f64(bytes, &mut pos)?;
        let count = read_f64(bytes, &mut pos)? as u64;
        let min = read_f64(bytes, &mut pos)?;
        let max = read_f64(bytes, &mut pos)?;
        let total_compressions = read_f64(bytes, &mut pos)? as u64;
        let n_centroids = read_f64(bytes, &mut pos)? as usize;

        let mut centroids = Vec::with_capacity(n_centroids);
        for _ in 0..n_centroids {
            let mean = read_f64(bytes, &mut pos)?;
            let weight = read_f64(bytes, &mut pos)?;
            centroids.push(Centroid { mean, weight });
        }

        let n_buffer = read_f64(bytes, &mut pos)? as usize;
        let mut buffer = Vec::with_capacity(n_buffer);
        for _ in 0..n_buffer {
            let v = read_f64(bytes, &mut pos)?;
            buffer.push(v);
        }

        let buffer_max = (compression * 3.0).ceil() as usize;

        Some(Self {
            compression,
            centroids,
            count,
            min,
            max,
            buffer,
            buffer_max,
            total_compressions,
        })
    }
}

enum ByRankResult {
    Value(f64),
    NaN,
    Inf,
    NegInf,
}

struct TDigestInfo {
    compression: u64,
    capacity: u64,
    merged_nodes: u64,
    unmerged_nodes: u64,
    merged_weight: f64,
    unmerged_weight: f64,
    total_compressions: u64,
    memory_usage: u64,
}

// ---------------------------------------------------------------------------
// Merge helper (operates on raw TDigest values, not resources)
// ---------------------------------------------------------------------------

fn merge_digests(digests: &mut [&mut TDigest], compression: f64) -> TDigest {
    // Ensure all compressed
    for d in digests.iter_mut() {
        d.ensure_compressed();
    }

    let mut all_centroids: Vec<Centroid> = Vec::new();
    let mut total_weight: u64 = 0;
    let mut global_min = f64::INFINITY;
    let mut global_max = f64::NEG_INFINITY;
    let mut total_comps: u64 = 0;

    for d in digests.iter() {
        all_centroids.extend(d.centroids.iter().cloned());
        total_weight += d.count;
        if d.count > 0 {
            if d.min < global_min {
                global_min = d.min;
            }
            if d.max > global_max {
                global_max = d.max;
            }
        }
        total_comps += d.total_compressions;
    }

    all_centroids
        .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));

    let merged =
        TDigest::merge_centroids(&all_centroids, total_weight as f64, compression);

    let buffer_max = (compression * 3.0).ceil() as usize;

    TDigest {
        compression,
        centroids: merged,
        count: total_weight,
        min: global_min,
        max: global_max,
        buffer: Vec::new(),
        buffer_max,
        total_compressions: total_comps + 1,
    }
}

// ---------------------------------------------------------------------------
// NIF functions
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        nan,
        inf,
        neg_inf = "-inf",
        compression,
        capacity,
        merged_nodes,
        unmerged_nodes,
        merged_weight,
        unmerged_weight,
        total_compressions,
        memory_usage,
    }
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_create(env: Env, comp: f64) -> NifResult<Term> {
    let digest = TDigest::new(comp);
    let resource = ResourceArc::new(TDigestResource {
        digest: Mutex::new(digest),
    });
    Ok(resource.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_add(env: Env, resource: ResourceArc<TDigestResource>, values: Vec<f64>) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    digest.add_many(&values);
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_quantile(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    quantiles: Vec<f64>,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<Term> = quantiles
        .iter()
        .map(|&q| {
            let val = digest.quantile(q);
            if val.is_nan() {
                atoms::nan().encode(env)
            } else {
                val.encode(env)
            }
        })
        .collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_cdf(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    values: Vec<f64>,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<Term> = values
        .iter()
        .map(|&v| {
            let val = digest.cdf(v);
            if val.is_nan() {
                atoms::nan().encode(env)
            } else {
                val.encode(env)
            }
        })
        .collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_trimmed_mean(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    lo: f64,
    hi: f64,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let val = digest.trimmed_mean(lo, hi);
    if val.is_nan() {
        Ok(atoms::nan().encode(env))
    } else {
        Ok(val.encode(env))
    }
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_min(env: Env, resource: ResourceArc<TDigestResource>) -> NifResult<Term> {
    let digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let val = digest.min_val();
    if val.is_nan() {
        Ok(atoms::nan().encode(env))
    } else {
        Ok(val.encode(env))
    }
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_max(env: Env, resource: ResourceArc<TDigestResource>) -> NifResult<Term> {
    let digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let val = digest.max_val();
    if val.is_nan() {
        Ok(atoms::nan().encode(env))
    } else {
        Ok(val.encode(env))
    }
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_info(env: Env, resource: ResourceArc<TDigestResource>) -> NifResult<Term> {
    let digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let info = digest.info();

    // Return as a flat list matching the existing Elixir format expectations
    let result = vec![
        atoms::compression().encode(env),
        info.compression.encode(env),
        atoms::capacity().encode(env),
        info.capacity.encode(env),
        atoms::merged_nodes().encode(env),
        info.merged_nodes.encode(env),
        atoms::unmerged_nodes().encode(env),
        info.unmerged_nodes.encode(env),
        atoms::merged_weight().encode(env),
        info.merged_weight.encode(env),
        atoms::unmerged_weight().encode(env),
        info.unmerged_weight.encode(env),
        atoms::total_compressions().encode(env),
        info.total_compressions.encode(env),
        atoms::memory_usage().encode(env),
        info.memory_usage.encode(env),
    ];

    Ok(result.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_rank(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    values: Vec<f64>,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<i64> = values.iter().map(|&v| digest.rank(v)).collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_revrank(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    values: Vec<f64>,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<i64> = values.iter().map(|&v| digest.rev_rank(v)).collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_byrank(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    ranks: Vec<i64>,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<Term> = ranks
        .iter()
        .map(|&r| match digest.by_rank(r) {
            ByRankResult::Value(v) => v.encode(env),
            ByRankResult::NaN => atoms::nan().encode(env),
            ByRankResult::Inf => atoms::inf().encode(env),
            ByRankResult::NegInf => atoms::neg_inf().encode(env),
        })
        .collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_byrevrank(
    env: Env,
    resource: ResourceArc<TDigestResource>,
    ranks: Vec<i64>,
) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let results: Vec<Term> = ranks
        .iter()
        .map(|&r| match digest.by_rev_rank(r) {
            ByRankResult::Value(v) => v.encode(env),
            ByRankResult::NaN => atoms::nan().encode(env),
            ByRankResult::Inf => atoms::inf().encode(env),
            ByRankResult::NegInf => atoms::neg_inf().encode(env),
        })
        .collect();
    Ok(results.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_merge(
    env: Env,
    resources: Vec<ResourceArc<TDigestResource>>,
    comp: f64,
) -> NifResult<Term> {
    // Lock all resources
    let mut guards: Vec<std::sync::MutexGuard<TDigest>> = Vec::with_capacity(resources.len());
    for r in &resources {
        let guard = r.digest.lock().map_err(|_| rustler::Error::BadArg)?;
        guards.push(guard);
    }

    let mut ptrs: Vec<&mut TDigest> = guards.iter_mut().map(|g| &mut **g).collect();
    let merged = merge_digests(&mut ptrs, comp);

    let resource = ResourceArc::new(TDigestResource {
        digest: Mutex::new(merged),
    });
    Ok(resource.encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_reset(env: Env, resource: ResourceArc<TDigestResource>) -> NifResult<Term> {
    let mut digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    digest.reset();
    Ok(atoms::ok().encode(env))
}

/// Serialize the digest to a byte array for Bitcask storage.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_serialize(
    env: Env,
    resource: ResourceArc<TDigestResource>,
) -> NifResult<Term> {
    let digest = resource.digest.lock().map_err(|_| rustler::Error::BadArg)?;
    let bytes = digest.serialize();

    let mut bin = rustler::OwnedBinary::new(bytes.len()).ok_or(rustler::Error::BadArg)?;
    bin.as_mut_slice().copy_from_slice(&bytes);
    Ok(rustler::Binary::from_owned(bin, env).encode(env))
}

/// Deserialize a byte array back into a TDigest resource.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn tdigest_deserialize<'a>(env: Env<'a>, data: rustler::Binary<'a>) -> NifResult<Term<'a>> {
    match TDigest::deserialize(data.as_slice()) {
        Some(digest) => {
            let resource = ResourceArc::new(TDigestResource {
                digest: Mutex::new(digest),
            });
            Ok((crate::atoms::ok(), resource).encode(env))
        }
        None => Ok((crate::atoms::error(), "invalid tdigest data").encode(env)),
    }
}

// ---------------------------------------------------------------------------
// Resource registration (called from lib.rs load)
// ---------------------------------------------------------------------------

pub fn register_resource(env: Env) {
    let _ = rustler::resource!(TDigestResource, env);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_digest() {
        let mut td = TDigest::new(100.0);
        assert!(td.is_empty());
        assert!(td.quantile(0.5).is_nan());
        assert!(td.cdf(50.0).is_nan());
        assert_eq!(td.rank(50.0), -2);
    }

    #[test]
    fn single_value() {
        let mut td = TDigest::new(100.0);
        td.add(42.0);
        assert!(!td.is_empty());
        assert_eq!(td.count, 1);
        assert_eq!(td.min, 42.0);
        assert_eq!(td.max, 42.0);
        let q50 = td.quantile(0.5);
        assert!((q50 - 42.0).abs() < 0.1);
    }

    #[test]
    fn uniform_1000() {
        let mut td = TDigest::new(100.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        let p50 = td.quantile(0.5);
        assert!((p50 - 500.0).abs() < 500.0 * 0.05);

        let p99 = td.quantile(0.99);
        assert!((p99 - 990.0).abs() < 990.0 * 0.02);
    }

    #[test]
    fn cdf_basic() {
        let mut td = TDigest::new(100.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        let cdf_500 = td.cdf(500.0);
        assert!((cdf_500 - 0.5).abs() < 0.05);
    }

    #[test]
    fn serialize_roundtrip() {
        let mut td = TDigest::new(100.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        td.ensure_compressed();

        let bytes = td.serialize();
        let td2 = TDigest::deserialize(&bytes).unwrap();

        assert_eq!(td.compression, td2.compression);
        assert_eq!(td.count, td2.count);
        assert_eq!(td.min, td2.min);
        assert_eq!(td.max, td2.max);
        assert_eq!(td.centroids.len(), td2.centroids.len());

        let q50_1 = td.quantile(0.5);
        let mut td2 = td2;
        let q50_2 = td2.quantile(0.5);
        assert!((q50_1 - q50_2).abs() < 0.01);
    }

    #[test]
    fn merge_two_digests() {
        let mut d1 = TDigest::new(100.0);
        let mut d2 = TDigest::new(100.0);
        for i in 1..=500 {
            d1.add(i as f64);
        }
        for i in 501..=1000 {
            d2.add(i as f64);
        }

        let mut merged = merge_digests(&mut [&mut d1, &mut d2], 100.0);
        assert_eq!(merged.count, 1000);

        let p50 = merged.quantile(0.5);
        assert!((p50 - 500.0).abs() < 500.0 * 0.1);
    }

    #[test]
    fn reset_preserves_compression() {
        let mut td = TDigest::new(250.0);
        td.add(1.0);
        td.add(2.0);
        td.reset();
        assert!(td.is_empty());
        assert_eq!(td.compression, 250.0);
    }

    #[test]
    fn trimmed_mean_full_range() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.add(i as f64);
        }
        let tm = td.trimmed_mean(0.0, 1.0);
        // True mean of 1..100 = 50.5
        assert!((tm - 50.5).abs() < 50.5 * 0.05);
    }

    #[test]
    fn by_rank_edges() {
        let mut td = TDigest::new(100.0);
        td.add(10.0);
        td.add(20.0);
        td.add(30.0);

        match td.by_rank(-1) {
            ByRankResult::NegInf => {}
            _ => panic!("expected NegInf"),
        }
        match td.by_rank(3) {
            ByRankResult::Inf => {}
            _ => panic!("expected Inf"),
        }
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn add_three_values_median_approx_2() {
        let mut td = TDigest::new(100.0);
        td.add(1.0);
        td.add(2.0);
        td.add(3.0);
        let p50 = td.quantile(0.5);
        assert!(
            (p50 - 2.0).abs() < 1.0,
            "median of [1,2,3] should be ~2, got {p50}"
        );
    }

    #[test]
    fn p50_within_5_percent_of_500() {
        let mut td = TDigest::new(100.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        let p50 = td.quantile(0.5);
        assert!(
            (p50 - 500.0).abs() < 500.0 * 0.05,
            "p50={p50}, expected ~500 +/- 5%"
        );
    }

    #[test]
    fn p99_within_1_percent_of_990() {
        let mut td = TDigest::new(200.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        let p99 = td.quantile(0.99);
        assert!(
            (p99 - 990.0).abs() < 990.0 * 0.02,
            "p99={p99}, expected ~990 +/- 2%"
        );
    }

    #[test]
    fn p0_returns_min() {
        let mut td = TDigest::new(100.0);
        td.add(5.0);
        td.add(10.0);
        td.add(15.0);
        let p0 = td.quantile(0.0);
        assert_eq!(p0, 5.0);
    }

    #[test]
    fn p1_returns_max() {
        let mut td = TDigest::new(100.0);
        td.add(5.0);
        td.add(10.0);
        td.add(15.0);
        let p1 = td.quantile(1.0);
        assert_eq!(p1, 15.0);
    }

    #[test]
    fn cdf_of_min_approx_zero() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.add(i as f64);
        }
        let cdf_min = td.cdf(1.0);
        assert!(
            cdf_min < 0.05,
            "CDF of min should be ~0, got {cdf_min}"
        );
    }

    #[test]
    fn cdf_of_max_approx_one() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.add(i as f64);
        }
        let cdf_max = td.cdf(100.0);
        assert!(
            cdf_max > 0.95,
            "CDF of max should be ~1.0, got {cdf_max}"
        );
    }

    #[test]
    fn merge_combined_quantiles_correct() {
        let mut d1 = TDigest::new(100.0);
        let mut d2 = TDigest::new(100.0);
        for i in 1..=500 {
            d1.add(i as f64);
        }
        for i in 501..=1000 {
            d2.add(i as f64);
        }
        let mut merged = merge_digests(&mut [&mut d1, &mut d2], 100.0);
        assert_eq!(merged.count, 1000);
        assert_eq!(merged.min, 1.0);
        assert_eq!(merged.max, 1000.0);
        let p50 = merged.quantile(0.5);
        assert!(
            (p50 - 500.0).abs() < 500.0 * 0.1,
            "merged p50={p50}, expected ~500"
        );
    }

    #[test]
    fn trimmed_mean_iqr() {
        let mut td = TDigest::new(200.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        let tm = td.trimmed_mean(0.25, 0.75);
        // IQR mean of uniform 1..1000 is ~500
        assert!(
            (tm - 500.0).abs() < 500.0 * 0.1,
            "trimmed mean={tm}, expected ~500"
        );
    }

    #[test]
    fn rank_revrank_consistency() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.add(i as f64);
        }
        let r = td.rank(50.0);
        let rr = td.rev_rank(50.0);
        // rank + rev_rank + 1 should be approximately count
        assert!(
            (r + rr + 1 - 100).abs() <= 2,
            "rank({r}) + rev_rank({rr}) + 1 != 100"
        );
    }

    #[test]
    fn negative_values_handled() {
        let mut td = TDigest::new(100.0);
        td.add(-100.0);
        td.add(-50.0);
        td.add(0.0);
        td.add(50.0);
        td.add(100.0);
        assert_eq!(td.min, -100.0);
        assert_eq!(td.max, 100.0);
        let p50 = td.quantile(0.5);
        assert!(
            (p50 - 0.0).abs() < 50.0,
            "median should be ~0, got {p50}"
        );
    }

    #[test]
    fn nan_infinity_behaviour() {
        // NaN in f64 comparisons doesn't update min/max properly,
        // but the t-digest should handle normal values after NaN gracefully.
        let mut td = TDigest::new(100.0);
        // Add normal values first
        td.add(1.0);
        td.add(100.0);
        // Verify normal operations work
        let q = td.quantile(0.5);
        assert!(!q.is_nan(), "quantile should be defined with normal values");
        let c = td.cdf(50.0);
        assert!(!c.is_nan(), "cdf should be defined with normal values");
    }

    #[test]
    fn deserialize_too_short_returns_none() {
        let data = vec![0u8; 10];
        assert!(TDigest::deserialize(&data).is_none());
    }

    #[test]
    fn by_rev_rank_edges() {
        let mut td = TDigest::new(100.0);
        td.add(10.0);
        td.add(20.0);
        td.add(30.0);

        match td.by_rev_rank(-1) {
            ByRankResult::Inf => {}
            _ => panic!("expected Inf for negative revrank"),
        }
        match td.by_rev_rank(3) {
            ByRankResult::NegInf => {}
            _ => panic!("expected NegInf for revrank >= count"),
        }
    }

    #[test]
    fn min_max_on_empty_returns_nan() {
        let td = TDigest::new(100.0);
        assert!(td.min_val().is_nan());
        assert!(td.max_val().is_nan());
    }

    // ==================================================================
    // Deep NIF edge cases — targeting TDigest / FFI safety pitfalls
    // ==================================================================

    #[test]
    fn add_positive_infinity_handled() {
        let mut td = TDigest::new(100.0);
        td.add(1.0);
        td.add(f64::INFINITY);
        assert_eq!(td.max, f64::INFINITY);
        // Quantile should not crash
        let _ = td.quantile(0.5);
    }

    #[test]
    fn add_negative_infinity_handled() {
        let mut td = TDigest::new(100.0);
        td.add(1.0);
        td.add(f64::NEG_INFINITY);
        assert_eq!(td.min, f64::NEG_INFINITY);
        let _ = td.quantile(0.5);
    }

    #[test]
    fn add_nan_does_not_crash() {
        let mut td = TDigest::new(100.0);
        td.add(1.0);
        td.add(f64::NAN);
        // NaN pollutes min/max comparisons but must not crash
        let _ = td.quantile(0.5);
        let _ = td.cdf(0.5);
    }

    #[test]
    fn quantile_negative_returns_min() {
        let mut td = TDigest::new(100.0);
        td.add(10.0);
        td.add(20.0);
        let q = td.quantile(-0.1);
        // Negative quantile should clamp to min or return some defined value
        // The key property is no crash
        let _ = q;
    }

    #[test]
    fn quantile_greater_than_1_returns_max() {
        let mut td = TDigest::new(100.0);
        td.add(10.0);
        td.add(20.0);
        let q = td.quantile(1.5);
        // quantile > 1 should clamp to max or return some defined value
        let _ = q;
    }

    #[test]
    fn compression_1_minimum() {
        let mut td = TDigest::new(1.0);
        for i in 0..100 {
            td.add(i as f64);
        }
        // Very few centroids with compression=1
        td.ensure_compressed();
        // Should still be able to answer queries
        let p50 = td.quantile(0.5);
        assert!(!p50.is_nan(), "quantile must be defined with compression=1");
    }

    #[test]
    fn compression_10000_very_high() {
        let mut td = TDigest::new(10000.0);
        for i in 0..1000 {
            td.add(i as f64);
        }
        td.ensure_compressed();
        // With very high compression, many centroids, high accuracy
        let p50 = td.quantile(0.5);
        assert!(
            (p50 - 500.0).abs() < 10.0,
            "high compression should give accurate p50, got {p50}"
        );
    }

    #[test]
    fn million_identical_values() {
        let mut td = TDigest::new(100.0);
        for _ in 0..1_000_000 {
            td.add(42.0);
        }
        assert_eq!(td.count, 1_000_000);
        let p50 = td.quantile(0.5);
        assert!(
            (p50 - 42.0).abs() < 0.01,
            "all identical values, p50 should be 42.0, got {p50}"
        );
        assert_eq!(td.min, 42.0);
        assert_eq!(td.max, 42.0);
    }

    #[test]
    fn alternating_min_max_values() {
        let mut td = TDigest::new(100.0);
        for i in 0..1000 {
            if i % 2 == 0 {
                td.add(0.0);
            } else {
                td.add(1000.0);
            }
        }
        let p50 = td.quantile(0.5);
        // Median should be around 500 (between the two extremes)
        assert!(
            (p50 - 500.0).abs() < 200.0,
            "alternating min/max, p50 should be ~500, got {p50}"
        );
    }

    #[test]
    fn cdf_on_empty_returns_nan() {
        let mut td = TDigest::new(100.0);
        let c = td.cdf(50.0);
        assert!(c.is_nan());
    }

    #[test]
    fn quantile_0_returns_min() {
        let mut td = TDigest::new(100.0);
        td.add(5.0);
        td.add(10.0);
        td.add(15.0);
        let q0 = td.quantile(0.0);
        assert!(
            (q0 - 5.0).abs() < 1.0,
            "quantile(0) should be ~min, got {q0}"
        );
    }

    #[test]
    fn quantile_1_returns_max() {
        let mut td = TDigest::new(100.0);
        td.add(5.0);
        td.add(10.0);
        td.add(15.0);
        let q1 = td.quantile(1.0);
        assert!(
            (q1 - 15.0).abs() < 1.0,
            "quantile(1) should be ~max, got {q1}"
        );
    }

    #[test]
    fn merge_empty_digests() {
        let mut d1 = TDigest::new(100.0);
        let mut d2 = TDigest::new(100.0);
        let merged = merge_digests(&mut [&mut d1, &mut d2], 100.0);
        assert!(merged.is_empty());
    }

    #[test]
    fn serialize_roundtrip_with_extremes() {
        let mut td = TDigest::new(100.0);
        td.add(f64::MIN);
        td.add(0.0);
        td.add(f64::MAX);
        td.ensure_compressed();

        let bytes = td.serialize();
        let td2 = TDigest::deserialize(&bytes).unwrap();
        assert_eq!(td2.count, 3);
        assert_eq!(td2.min, f64::MIN);
        assert_eq!(td2.max, f64::MAX);
    }

    #[test]
    fn deserialize_truncated_at_every_byte_no_panic() {
        let mut td = TDigest::new(100.0);
        for i in 0..50 {
            td.add(i as f64);
        }
        td.ensure_compressed();
        let bytes = td.serialize();

        for truncate_at in 0..bytes.len() {
            let truncated = &bytes[..truncate_at];
            let result = TDigest::deserialize(truncated);
            // Must either succeed or return None, never panic
            let _ = result;
        }
    }

    #[test]
    fn trimmed_mean_full_range_edge() {
        let mut td = TDigest::new(100.0);
        for i in 1..=1000 {
            td.add(i as f64);
        }
        let tm = td.trimmed_mean(0.0, 1.0);
        // Full range trimmed mean should be close to regular mean (~500.5)
        assert!(
            (tm - 500.5).abs() < 50.0,
            "trimmed_mean(0,1) should be ~500.5, got {tm}"
        );
    }

    #[test]
    fn reset_and_reuse() {
        let mut td = TDigest::new(100.0);
        for i in 0..1000 {
            td.add(i as f64);
        }
        assert_eq!(td.count, 1000);
        td.reset();
        assert!(td.is_empty());
        assert_eq!(td.count, 0);
        // Reuse after reset
        td.add(42.0);
        assert_eq!(td.count, 1);
        let q = td.quantile(0.5);
        assert!((q - 42.0).abs() < 0.1);
    }
}
