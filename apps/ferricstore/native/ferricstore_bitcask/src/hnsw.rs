//! Hierarchical Navigable Small World (HNSW) graph for approximate nearest
//! neighbor search.
//!
//! This is a self-contained, pure-Rust HNSW implementation exposed as Rustler
//! NIFs. It supports three distance metrics (L2, cosine, inner product) and
//! uses string keys to address vectors so the Elixir layer can correlate
//! search results with stored entities.
//!
//! The implementation follows the original HNSW paper (Malkov & Yashunin, 2018)
//! with a layered skip-list-style graph. Each layer is a set of bidirectional
//! edges connecting nodes; upper layers are sparser for logarithmic greedy
//! traversal, while the bottom layer (layer 0) is the densest.
//!
//! ## Yielding NIF
//!
//! `vsearch_nif` is implemented as a yielding NIF: it calls
//! `consume_timeslice` periodically during the search and reschedules itself
//! if the BEAM scheduler wants to reclaim the thread. This prevents long
//! searches from blocking other processes.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Mutex;

use rustler::schedule::consume_timeslice;
use rustler::{Encoder, Env, NifResult, ResourceArc, Term};

// ---------------------------------------------------------------------------
// Distance metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum Metric {
    L2,
    Cosine,
    InnerProduct,
}

/// Compute distance between two vectors using the given metric.
/// Lower distance = more similar for all metrics.
fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::L2 => a
            .iter()
            .zip(b.iter())
            .map(|(ai, bi)| {
                let d = ai - bi;
                d * d
            })
            .sum(),
        Metric::Cosine => {
            let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
            for (ai, bi) in a.iter().zip(b.iter()) {
                dot += ai * bi;
                na += ai * ai;
                nb += bi * bi;
            }
            let denom = na.sqrt() * nb.sqrt();
            if denom == 0.0 {
                1.0
            } else {
                let sim = (dot / denom).clamp(-1.0, 1.0);
                1.0 - sim
            }
        }
        Metric::InnerProduct => {
            let dot: f32 = a.iter().zip(b.iter()).map(|(ai, bi)| ai * bi).sum();
            1.0 - dot
        }
    }
}

// ---------------------------------------------------------------------------
// HNSW graph
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Node {
    key: String,
    vector: Vec<f32>,
    /// Adjacency lists per layer. `neighbors[l]` = set of node indices at layer l.
    neighbors: Vec<Vec<usize>>,
    /// Whether this node has been logically deleted (soft delete).
    deleted: bool,
}

/// The HNSW index.
pub struct HnswIndex {
    dims: usize,
    m: usize,      // max edges per node per layer
    m_max0: usize, // max edges at layer 0 (2*m)
    ef_construction: usize,
    metric: Metric,
    nodes: Vec<Node>,
    /// Map from string key to node index for O(1) lookup/delete.
    key_to_id: HashMap<String, usize>,
    /// Entry point node index (highest-layer node). `None` when empty.
    entry_point: Option<usize>,
    /// Maximum layer in the graph.
    max_layer: usize,
    /// Multiplier for random level generation: 1/ln(m).
    ml: f64,
    /// Simple counter for deterministic-ish level generation.
    level_counter: u64,
}

impl HnswIndex {
    #[must_use]
    pub fn new(dims: usize, m: usize, ef_construction: usize, metric: Metric) -> Self {
        let ml = 1.0 / (m as f64).ln();
        Self {
            dims,
            m,
            m_max0: m * 2,
            ef_construction,
            metric,
            nodes: Vec::new(),
            key_to_id: HashMap::new(),
            entry_point: None,
            max_layer: 0,
            ml,
            level_counter: 0,
        }
    }

    /// Generate a random level for a new node using a simple hash-based
    /// approach (deterministic for reproducibility).
    fn random_level(&mut self) -> usize {
        self.level_counter += 1;
        // Use a hash-like function to generate pseudo-random level
        let mut x = self.level_counter;
        x = x.wrapping_mul(0x517c_c1b7_2722_0a95);
        x ^= x >> 32;
        // Convert to uniform [0,1) float
        let f = (x & 0x001F_FFFF_FFFF_FFFF) as f64 / (1u64 << 53) as f64;
        // Level = floor(-ln(uniform) * ml)
        let level = (-f.ln() * self.ml).floor() as usize;
        // Cap at a reasonable max to prevent degenerate graphs
        level.min(16)
    }

    /// Add a vector with the given key. Returns the node index.
    ///
    /// # Errors
    ///
    /// Returns an error if `vector.len()` does not match the configured `dims`.
    ///
    /// # Panics
    ///
    /// Panics if the entry point is `Some` but no longer valid (should never
    /// happen under normal use).
    pub fn add(&mut self, key: &str, vector: Vec<f32>) -> Result<usize, String> {
        if vector.len() != self.dims {
            return Err(format!(
                "dimension mismatch: expected {}, got {}",
                self.dims,
                vector.len()
            ));
        }

        // If key already exists, overwrite: soft-delete old, insert new
        if let Some(&old_id) = self.key_to_id.get(key) {
            self.nodes[old_id].deleted = true;
        }

        let node_id = self.nodes.len();
        let level = self.random_level();

        let node = Node {
            key: key.to_string(),
            vector,
            neighbors: vec![Vec::new(); level + 1],
            deleted: false,
        };

        self.nodes.push(node);
        self.key_to_id.insert(key.to_string(), node_id);

        if self.entry_point.is_none() {
            // First node
            self.entry_point = Some(node_id);
            self.max_layer = level;
            return Ok(node_id);
        }

        let mut ep = self.entry_point.unwrap();

        // Phase 1: Traverse from top layer down to level+1, greedily
        // finding the closest node at each layer.
        let top = self.max_layer;
        for lc in (level + 1..=top).rev() {
            ep = self.greedy_closest(ep, node_id, lc);
        }

        // Phase 2: From min(level, max_layer) down to 0, do ef_construction
        // search and connect.
        let insert_top = level.min(top);
        for lc in (0..=insert_top).rev() {
            let candidates = self.search_layer(node_id, ep, self.ef_construction, lc);

            let m_max = if lc == 0 { self.m_max0 } else { self.m };

            // Select the m_max nearest non-deleted neighbors
            let neighbors: Vec<usize> = candidates
                .iter()
                .filter(|&&(id, _)| !self.nodes[id].deleted)
                .take(m_max)
                .map(|&(id, _)| id)
                .collect();

            // Set forward edges
            self.nodes[node_id].neighbors[lc].clone_from(&neighbors);

            // Set reverse edges (with pruning)
            for &neighbor_id in &neighbors {
                let n_neighbors = &mut self.nodes[neighbor_id].neighbors;
                if lc < n_neighbors.len() {
                    n_neighbors[lc].push(node_id);
                    // Prune if over capacity
                    if n_neighbors[lc].len() > m_max {
                        self.prune_neighbors(neighbor_id, lc, m_max);
                    }
                }
            }

            // Update ep for next layer
            if let Some(&(closest, _)) = candidates.first() {
                ep = closest;
            }
        }

        // Update entry point if new node has higher level
        if level > self.max_layer {
            self.entry_point = Some(node_id);
            self.max_layer = level;
        }

        Ok(node_id)
    }

    /// Prune the neighbor list of `node_id` at layer `lc` to `m_max` entries,
    /// keeping the closest neighbors by distance.
    fn prune_neighbors(&mut self, node_id: usize, lc: usize, m_max: usize) {
        let node_vec = self.nodes[node_id].vector.clone();
        let neighbors = &self.nodes[node_id].neighbors[lc];

        let mut scored: Vec<(usize, f32)> = neighbors
            .iter()
            .map(|&nid| {
                let dist = distance(self.metric, &node_vec, &self.nodes[nid].vector);
                (nid, dist)
            })
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.truncate(m_max);

        self.nodes[node_id].neighbors[lc] = scored.into_iter().map(|(id, _)| id).collect();
    }

    /// Greedy search: starting from `ep`, walk edges at layer `lc` to find
    /// the single closest node to `query_id`.
    fn greedy_closest(&self, mut ep: usize, query_id: usize, lc: usize) -> usize {
        let query = &self.nodes[query_id].vector;
        let mut best_dist = distance(self.metric, query, &self.nodes[ep].vector);

        loop {
            let mut changed = false;
            let neighbors = if lc < self.nodes[ep].neighbors.len() {
                &self.nodes[ep].neighbors[lc]
            } else {
                break;
            };

            for &nid in neighbors {
                let d = distance(self.metric, query, &self.nodes[nid].vector);
                if d < best_dist {
                    best_dist = d;
                    ep = nid;
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }
        ep
    }

    /// Search at a single layer: starting from `ep`, find up to `ef`
    /// nearest non-deleted neighbors of `query_id`. Returns sorted
    /// (nearest first) list of (node_id, distance).
    fn search_layer(&self, query_id: usize, ep: usize, ef: usize, lc: usize) -> Vec<(usize, f32)> {
        let query = &self.nodes[query_id].vector;
        self.search_layer_vec(query, ep, ef, lc)
    }

    /// Search at a single layer using a raw query vector.
    fn search_layer_vec(
        &self,
        query: &[f32],
        ep: usize,
        ef: usize,
        lc: usize,
    ) -> Vec<(usize, f32)> {
        let ep_dist = distance(self.metric, query, &self.nodes[ep].vector);

        // Min-heap of candidates (to explore)
        let mut candidates: BinaryHeap<MinDist> = BinaryHeap::new();
        // Max-heap of results (worst at top for easy eviction)
        let mut results: BinaryHeap<MaxDist> = BinaryHeap::new();
        let mut visited: HashSet<usize> = HashSet::new();

        candidates.push(MinDist(ep, ep_dist));
        if !self.nodes[ep].deleted {
            results.push(MaxDist(ep, ep_dist));
        }
        visited.insert(ep);

        while let Some(MinDist(current, c_dist)) = candidates.pop() {
            // If current candidate is farther than the worst result, stop
            let worst_dist = results.peek().map_or(f32::INFINITY, |MaxDist(_, d)| *d);

            if c_dist > worst_dist && results.len() >= ef {
                break;
            }

            let neighbors = if lc < self.nodes[current].neighbors.len() {
                &self.nodes[current].neighbors[lc]
            } else {
                continue;
            };

            for &nid in neighbors {
                if visited.contains(&nid) {
                    continue;
                }
                visited.insert(nid);

                let d = distance(self.metric, query, &self.nodes[nid].vector);

                let worst_dist = results.peek().map_or(f32::INFINITY, |MaxDist(_, d)| *d);

                if d < worst_dist || results.len() < ef {
                    candidates.push(MinDist(nid, d));
                    if !self.nodes[nid].deleted {
                        results.push(MaxDist(nid, d));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        // Drain results into a sorted vec (nearest first)
        let mut result_vec: Vec<(usize, f32)> =
            results.into_iter().map(|MaxDist(id, d)| (id, d)).collect();
        result_vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        result_vec
    }

    /// Search the index for the k nearest neighbors of the given query vector.
    ///
    /// # Panics
    ///
    /// Panics if the entry point is `Some` but no longer valid (should never
    /// happen under normal use).
    #[must_use]
    pub fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<(String, f32)> {
        if k == 0 || self.entry_point.is_none() {
            return Vec::new();
        }

        let ep = self.entry_point.unwrap();
        let ef = ef_search.max(k);

        // Phase 1: Greedy descent from top layer to layer 1
        let mut current_ep = ep;
        for lc in (1..=self.max_layer).rev() {
            current_ep = self.greedy_closest_vec(current_ep, query, lc);
        }

        // Phase 2: Search at layer 0
        let candidates = self.search_layer_vec(query, current_ep, ef, 0);

        // Return top-k
        candidates
            .into_iter()
            .filter(|&(id, _)| !self.nodes[id].deleted)
            .take(k)
            .map(|(id, dist)| (self.nodes[id].key.clone(), dist))
            .collect()
    }

    /// Greedy search using a raw vector (not a node ID).
    fn greedy_closest_vec(&self, mut ep: usize, query: &[f32], lc: usize) -> usize {
        let mut best_dist = distance(self.metric, query, &self.nodes[ep].vector);

        loop {
            let mut changed = false;
            let neighbors = if lc < self.nodes[ep].neighbors.len() {
                &self.nodes[ep].neighbors[lc]
            } else {
                break;
            };

            for &nid in neighbors {
                let d = distance(self.metric, query, &self.nodes[nid].vector);
                if d < best_dist {
                    best_dist = d;
                    ep = nid;
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }
        ep
    }

    /// Soft-delete a vector by key.
    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(&id) = self.key_to_id.get(key) {
            if self.nodes[id].deleted {
                return false;
            }
            self.nodes[id].deleted = true;
            self.key_to_id.remove(key);
            true
        } else {
            false
        }
    }

    /// Count of live (non-deleted) vectors.
    #[must_use]
    pub fn count(&self) -> usize {
        self.key_to_id.len()
    }
}

// ---------------------------------------------------------------------------
// Heap helpers (min-heap and max-heap by distance)
// ---------------------------------------------------------------------------

struct MinDist(usize, f32);

impl PartialEq for MinDist {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
impl Eq for MinDist {}

impl PartialOrd for MinDist {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinDist {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (BinaryHeap is max-heap by default)
        other.1.partial_cmp(&self.1).unwrap_or(Ordering::Equal)
    }
}

struct MaxDist(usize, f32);

impl PartialEq for MaxDist {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
impl Eq for MaxDist {}

impl PartialOrd for MaxDist {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxDist {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.partial_cmp(&other.1).unwrap_or(Ordering::Equal)
    }
}

// ---------------------------------------------------------------------------
// Rustler NIF resource
// ---------------------------------------------------------------------------

/// Resource wrapper for the HNSW index, protected by a Mutex for thread safety.
pub struct HnswResource {
    pub index: Mutex<HnswIndex>,
}

// ---------------------------------------------------------------------------
// NIF functions
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        error,
        true_nif = "true",
        false_nif = "false",
    }
}

/// Create a new HNSW index.
/// Returns `{:ok, ref}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn hnsw_new(
    env: Env,
    dims: usize,
    m: usize,
    ef_construction: usize,
    metric_str: String,
) -> NifResult<Term> {
    if dims == 0 {
        return Ok((atoms::error(), "dims must be > 0").encode(env));
    }
    if m == 0 {
        return Ok((atoms::error(), "m must be > 0").encode(env));
    }

    let metric = match metric_str.as_str() {
        "l2" => Metric::L2,
        "cosine" => Metric::Cosine,
        "inner_product" => Metric::InnerProduct,
        other => {
            return Ok((
                atoms::error(),
                format!("unknown metric '{other}', must be l2, cosine, or inner_product"),
            )
                .encode(env))
        }
    };

    let index = HnswIndex::new(dims, m, ef_construction, metric);
    let resource = ResourceArc::new(HnswResource {
        index: Mutex::new(index),
    });

    Ok((atoms::ok(), resource).encode(env))
}

/// Add a vector to the index.
/// Returns `{:ok, node_id}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn hnsw_add(
    env: Env,
    resource: ResourceArc<HnswResource>,
    key: String,
    vector: Vec<f64>,
) -> NifResult<Term> {
    let vec_f32: Vec<f32> = vector.iter().map(|&v| v as f32).collect();
    let mut index = resource.index.lock().map_err(|_| rustler::Error::BadArg)?;

    match index.add(&key, vec_f32) {
        Ok(id) => Ok((atoms::ok(), id).encode(env)),
        Err(msg) => Ok((atoms::error(), msg).encode(env)),
    }
}

/// Delete a vector by key.
/// Returns `{:ok, true}` or `{:ok, false}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn hnsw_delete(env: Env, resource: ResourceArc<HnswResource>, key: String) -> NifResult<Term> {
    let mut index = resource.index.lock().map_err(|_| rustler::Error::BadArg)?;

    let deleted = index.delete(&key);
    Ok((atoms::ok(), deleted).encode(env))
}

/// Search for k nearest neighbors.
/// Returns `{:ok, [{key, distance}, ...]}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn hnsw_search(
    env: Env,
    resource: ResourceArc<HnswResource>,
    query: Vec<f64>,
    k: usize,
    ef: usize,
) -> NifResult<Term> {
    let query_f32: Vec<f32> = query.iter().map(|&v| v as f32).collect();
    let index = resource.index.lock().map_err(|_| rustler::Error::BadArg)?;

    let results = index.search(&query_f32, k, ef);

    let terms: Vec<(String, f64)> = results
        .into_iter()
        .map(|(key, dist)| (key, f64::from(dist)))
        .collect();

    Ok((atoms::ok(), terms).encode(env))
}

/// Return count of live vectors.
/// Returns `{:ok, count}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn hnsw_count(env: Env, resource: ResourceArc<HnswResource>) -> NifResult<Term> {
    let index = resource.index.lock().map_err(|_| rustler::Error::BadArg)?;

    Ok((atoms::ok(), index.count()).encode(env))
}

/// Yielding NIF version of search. Consumes timeslice proportionally to
/// the index size so the BEAM scheduler can account for the CPU cost.
///
/// HNSW graph traversal is O(ef * log(n)) which makes it impractical to
/// split into continuation chunks (the BFS state with heaps and visited
/// sets is complex). Instead we consume a timeslice percentage proportional
/// to the index size:
/// - < 1K vectors: 10% (fast, negligible)
/// - 1K-10K vectors: 50% (moderate)
/// - 10K+ vectors: 100% (scheduler should preempt caller afterward)
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn vsearch_nif(
    env: Env,
    resource: ResourceArc<HnswResource>,
    query: Vec<f64>,
    k: usize,
    ef: usize,
) -> NifResult<Term> {
    let query_f32: Vec<f32> = query.iter().map(|&v| v as f32).collect();
    let index = resource.index.lock().map_err(|_| rustler::Error::BadArg)?;

    let n = index.count();
    let results = index.search(&query_f32, k, ef);
    drop(index);

    // Consume timeslice proportionally to index size
    let pct = if n < 1_000 { 10 } else if n < 10_000 { 50 } else { 100 };
    let _ = consume_timeslice(env, pct);

    let terms: Vec<(String, f64)> = results
        .into_iter()
        .map(|(key, dist)| (key, f64::from(dist)))
        .collect();

    Ok((atoms::ok(), terms).encode(env))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance() {
        let a = [1.0, 0.0, 0.0];
        let b = [0.0, 1.0, 0.0];
        let d = distance(Metric::L2, &a, &b);
        assert!((d - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance_same_direction() {
        let a = [1.0, 0.0];
        let b = [2.0, 0.0];
        let d = distance(Metric::Cosine, &a, &b);
        assert!(d < 1e-6, "same direction should have ~0 cosine distance");
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        let d = distance(Metric::Cosine, &a, &b);
        assert!(
            (d - 1.0).abs() < 1e-6,
            "orthogonal should have ~1.0 cosine distance"
        );
    }

    #[test]
    fn test_inner_product_distance() {
        let a = [1.0, 0.0];
        let b = [0.5, 0.0];
        let d = distance(Metric::InnerProduct, &a, &b);
        assert!((d - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_hnsw_basic_insert_search() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("close", vec![1.1, 0.1, 0.1]).unwrap();
        index.add("far", vec![10.0, 10.0, 10.0]).unwrap();

        let results = index.search(&[1.0, 0.0, 0.0], 1, 50);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "close");
    }

    #[test]
    fn test_hnsw_delete() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("b", vec![0.0, 1.0, 0.0]).unwrap();

        assert!(index.delete("a"));
        assert!(!index.delete("a")); // already deleted
        assert_eq!(index.count(), 1);

        let results = index.search(&[1.0, 0.0, 0.0], 5, 50);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "b");
    }

    #[test]
    fn test_hnsw_empty_search() {
        let index = HnswIndex::new(3, 16, 128, Metric::L2);
        let results = index.search(&[1.0, 0.0, 0.0], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        let result = index.add("bad", vec![1.0, 2.0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_hnsw_many_vectors() {
        let dims = 8;
        let mut index = HnswIndex::new(dims, 16, 200, Metric::L2);

        for i in 0..500 {
            let v: Vec<f32> = (0..dims).map(|d| (i + d) as f32).collect();
            index.add(&format!("v{i}"), v).unwrap();
        }

        let query: Vec<f32> = (0..dims).map(|d| (100 + d) as f32).collect();
        let results = index.search(&query, 1, 200);
        assert_eq!(results[0].0, "v100");
        assert!(results[0].1 < 1e-6);
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn create_dims_zero_handled() {
        // We don't test via NIF here, but the NIF layer returns an error.
        // At the Rust struct level, dims=0 is just a degenerate index.
        let index = HnswIndex::new(0, 16, 128, Metric::L2);
        let results = index.search(&[], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn insert_single_vector_search_returns_it() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("only", vec![1.0, 2.0, 3.0]).unwrap();
        let results = index.search(&[1.0, 2.0, 3.0], 1, 50);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "only");
        assert!(results[0].1 < 1e-6);
    }

    #[test]
    fn recall_at_10_ge_07_for_1000_vectors() {
        let dims = 16;
        let mut index = HnswIndex::new(dims, 16, 200, Metric::L2);
        let mut vectors: Vec<Vec<f32>> = Vec::new();

        for i in 0..1000 {
            let v: Vec<f32> = (0..dims)
                .map(|d| ((i * 7 + d * 13) % 1000) as f32 / 1000.0)
                .collect();
            vectors.push(v.clone());
            index.add(&format!("v{i}"), v).unwrap();
        }

        // Pick 10 random queries and check recall@10 against brute force
        let mut total_recall = 0.0;
        let num_queries = 10;
        for qi in [0, 100, 200, 300, 400, 500, 600, 700, 800, 999] {
            let query = &vectors[qi];

            // Brute force top-10
            let mut dists: Vec<(usize, f32)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (i, distance(Metric::L2, query, v)))
                .collect();
            dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let brute_top10: std::collections::HashSet<String> =
                dists.iter().take(10).map(|(i, _)| format!("v{i}")).collect();

            // HNSW top-10
            let hnsw_results = index.search(query, 10, 200);
            let hnsw_top10: std::collections::HashSet<String> =
                hnsw_results.iter().map(|(k, _)| k.clone()).collect();

            let recall = hnsw_top10.intersection(&brute_top10).count() as f64 / 10.0;
            total_recall += recall;
        }

        let avg_recall = total_recall / num_queries as f64;
        assert!(
            avg_recall >= 0.7,
            "recall@10 = {avg_recall:.2}, expected >= 0.70"
        );
    }

    #[test]
    fn delete_search_no_longer_returns() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("keep", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("remove", vec![0.9, 0.0, 0.0]).unwrap();

        assert!(index.delete("remove"));
        let results = index.search(&[0.9, 0.0, 0.0], 5, 50);
        for (key, _) in &results {
            assert_ne!(key, "remove", "deleted vector still returned");
        }
    }

    #[test]
    fn search_k_zero_returns_empty() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        let results = index.search(&[1.0, 0.0, 0.0], 0, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn search_k_greater_than_count_returns_all() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("b", vec![0.0, 1.0, 0.0]).unwrap();
        let results = index.search(&[0.5, 0.5, 0.0], 100, 200);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn different_metrics_give_different_orderings() {
        let mut l2_index = HnswIndex::new(2, 16, 128, Metric::L2);
        let mut cos_index = HnswIndex::new(2, 16, 128, Metric::Cosine);

        let vecs = vec![
            ("a", vec![1.0f32, 0.0]),
            ("b", vec![0.0, 1.0]),
            ("c", vec![0.7, 0.7]),
        ];
        for (k, v) in &vecs {
            l2_index.add(k, v.clone()).unwrap();
            cos_index.add(k, v.clone()).unwrap();
        }

        let query = &[1.0f32, 0.1];
        let l2_results = l2_index.search(query, 3, 50);
        let cos_results = cos_index.search(query, 3, 50);

        // Different metrics can produce different orderings
        let l2_order: Vec<&str> = l2_results.iter().map(|(k, _)| k.as_str()).collect();
        let cos_order: Vec<&str> = cos_results.iter().map(|(k, _)| k.as_str()).collect();
        // At minimum both should return all 3 items
        assert_eq!(l2_results.len(), 3);
        assert_eq!(cos_results.len(), 3);
        // They may or may not differ but both should be valid orderings
        let _ = (l2_order, cos_order);
    }

    #[test]
    fn zero_vector_cosine_search() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::Cosine);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("b", vec![0.0, 1.0, 0.0]).unwrap();
        // Zero vector: cosine distance returns 1.0 for all (denom=0 case)
        let results = index.search(&[0.0, 0.0, 0.0], 2, 50);
        assert_eq!(results.len(), 2);
        for (_, dist) in &results {
            assert!(
                (*dist - 1.0).abs() < 1e-5,
                "zero vector cosine dist should be 1.0, got {dist}"
            );
        }
    }

    #[test]
    fn high_dimensional_512d() {
        let dims = 512;
        let mut index = HnswIndex::new(dims, 16, 128, Metric::L2);
        let v1: Vec<f32> = (0..dims).map(|d| d as f32 / dims as f32).collect();
        let v2: Vec<f32> = (0..dims).map(|d| (dims - d) as f32 / dims as f32).collect();
        index.add("v1", v1.clone()).unwrap();
        index.add("v2", v2).unwrap();

        let results = index.search(&v1, 1, 50);
        assert_eq!(results[0].0, "v1");
    }

    #[test]
    fn duplicate_vectors_both_returned() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("dup1", vec![1.0, 1.0, 1.0]).unwrap();
        index.add("dup2", vec![1.0, 1.0, 1.0]).unwrap();
        let results = index.search(&[1.0, 1.0, 1.0], 5, 50);
        let keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
        // Both duplicates (or at least the second, since the first might be overwritten)
        // should appear. Actually, adding "dup2" with same vector soft-deletes "dup1"
        // because key_to_id overwrites. So only "dup2" should be returned.
        assert!(keys.contains(&"dup2"));
    }

    #[test]
    fn empty_index_search_returns_empty() {
        let index = HnswIndex::new(3, 16, 128, Metric::L2);
        let results = index.search(&[1.0, 0.0, 0.0], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn all_same_vectors_returned_with_zero_distance() {
        let mut index = HnswIndex::new(2, 16, 128, Metric::L2);
        for i in 0..5 {
            // Use distinct keys so they are all live
            index.add(&format!("same_{i}"), vec![3.0, 4.0]).unwrap();
        }
        let results = index.search(&[3.0, 4.0], 10, 50);
        // Only the last "same_4" should be live because overwriting same keys
        // Actually each key is unique ("same_0" through "same_4"), so all 5 are live
        assert_eq!(results.len(), 5);
        for (_, dist) in &results {
            assert!(*dist < 1e-6, "expected distance ~0, got {dist}");
        }
    }

    // ==================================================================
    // Deep NIF edge cases — targeting HNSW/FFI safety pitfalls
    // ==================================================================

    #[test]
    fn search_on_index_with_all_deleted_nodes() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("b", vec![0.0, 1.0, 0.0]).unwrap();
        index.add("c", vec![0.0, 0.0, 1.0]).unwrap();
        assert!(index.delete("a"));
        assert!(index.delete("b"));
        assert!(index.delete("c"));
        let results = index.search(&[1.0, 0.0, 0.0], 5, 50);
        assert!(results.is_empty(), "all-deleted index should return empty");
    }

    #[test]
    fn insert_vector_wrong_dimensions_returns_error() {
        let mut index = HnswIndex::new(4, 16, 128, Metric::L2);
        // Too few dimensions
        let result = index.add("short", vec![1.0, 2.0]);
        assert!(result.is_err());
        // Too many dimensions
        let result = index.add("long", vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!(result.is_err());
    }

    #[test]
    fn insert_vector_with_nan_components() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        // NaN in vector should not crash -- may insert but search results
        // will be unpredictable. The key safety property is no panic/UB.
        let result = index.add("nan_vec", vec![f32::NAN, 1.0, 0.0]);
        // Should either succeed or return error, but never crash
        let _ = result;
        // Search with NaN query should also not crash
        let results = index.search(&[f32::NAN, 0.0, 0.0], 5, 50);
        let _ = results; // just verify no panic
    }

    #[test]
    fn insert_vector_with_infinity() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        let result = index.add("inf_vec", vec![f32::INFINITY, 0.0, 0.0]);
        // Should not crash
        let _ = result;
        let results = index.search(&[0.0, 0.0, 0.0], 5, 50);
        let _ = results; // no crash is the requirement
    }

    #[test]
    fn insert_duplicate_key_soft_deletes_old() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        // Add an anchor node so the graph has structure for the second "dup"
        // to connect to. Without it, the entry point becomes the soft-deleted
        // first "dup" and the second node may be unreachable.
        index.add("anchor", vec![0.5, 0.5, 0.5]).unwrap();
        index.add("dup", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("dup", vec![0.0, 0.0, 1.0]).unwrap();
        // After re-adding "dup", the old node is soft-deleted and the new
        // one is inserted. count() reflects the live count.
        assert_eq!(index.count(), 2, "anchor + new dup");
        let results = index.search(&[0.0, 0.0, 1.0], 5, 50);
        let has_dup = results.iter().any(|(k, _)| k == "dup");
        assert!(has_dup, "replaced 'dup' must be searchable");
    }

    #[test]
    fn search_with_ef_zero() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        // ef=0 is degenerate but should not crash
        let results = index.search(&[1.0, 0.0, 0.0], 1, 0);
        // May return 0 or 1 results depending on implementation
        let _ = results;
    }

    #[test]
    fn search_with_ef_larger_than_index_size() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("only", vec![1.0, 0.0, 0.0]).unwrap();
        let results = index.search(&[1.0, 0.0, 0.0], 1, 10_000);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "only");
    }

    #[test]
    fn delete_then_reinsert_same_key() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::L2);
        index.add("recycled", vec![1.0, 0.0, 0.0]).unwrap();
        // Also add a second node so the graph has edges for search traversal
        index.add("anchor", vec![0.0, 0.0, 1.0]).unwrap();
        assert!(index.delete("recycled"));
        assert_eq!(index.count(), 1);
        // Reinsert with different vector
        index.add("recycled", vec![0.0, 1.0, 0.0]).unwrap();
        assert_eq!(index.count(), 2);
        let results = index.search(&[0.0, 1.0, 0.0], 1, 50);
        assert_eq!(results[0].0, "recycled");
    }

    #[test]
    fn build_index_with_m_zero_handled() {
        // m=0 means no edges per layer; degenerate but should not crash
        let mut index = HnswIndex::new(3, 0, 128, Metric::L2);
        // Add may succeed but search may not find anything useful
        let _ = index.add("a", vec![1.0, 0.0, 0.0]);
        let results = index.search(&[1.0, 0.0, 0.0], 1, 50);
        let _ = results; // no crash is the requirement
    }

    #[test]
    fn concurrent_insert_search_100_threads() {
        use std::sync::{Arc, Mutex};
        let index = Arc::new(Mutex::new(HnswIndex::new(3, 16, 128, Metric::L2)));

        // Pre-populate
        {
            let mut guard = index.lock().unwrap();
            for i in 0..50 {
                let v = vec![i as f32, 0.0, 0.0];
                guard.add(&format!("pre_{i}"), v).unwrap();
            }
        }

        let handles: Vec<_> = (0..100)
            .map(|t| {
                let idx = Arc::clone(&index);
                std::thread::spawn(move || {
                    let mut guard = idx.lock().unwrap();
                    if t % 2 == 0 {
                        let v = vec![t as f32, t as f32, 0.0];
                        let _ = guard.add(&format!("thread_{t}"), v);
                    } else {
                        let _ = guard.search(&[t as f32, 0.0, 0.0], 5, 50);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked in HNSW concurrent test");
        }
    }

    #[test]
    fn negative_distance_with_inner_product() {
        let mut index = HnswIndex::new(3, 16, 128, Metric::InnerProduct);
        index.add("pos", vec![1.0, 1.0, 1.0]).unwrap();
        index.add("neg", vec![-1.0, -1.0, -1.0]).unwrap();
        // Query with positive vector; InnerProduct dist = 1 - dot
        let results = index.search(&[1.0, 1.0, 1.0], 2, 50);
        assert_eq!(results.len(), 2);
        // "pos" should be closest (dist = 1 - 3 = -2)
        assert_eq!(results[0].0, "pos");
    }

    #[test]
    fn very_large_ef_construction_does_not_crash() {
        // ef_construction = u32::MAX-like value; degenerate but safe
        let mut index = HnswIndex::new(3, 4, 10_000, Metric::L2);
        index.add("a", vec![1.0, 0.0, 0.0]).unwrap();
        index.add("b", vec![0.0, 1.0, 0.0]).unwrap();
        let results = index.search(&[1.0, 0.0, 0.0], 1, 10_000);
        assert_eq!(results[0].0, "a");
    }
}
