# FerricStore T-Digest Design

## Overview

### What is a t-digest?

A t-digest is a probabilistic data structure for accurate on-line accumulation of
rank-based statistics such as quantiles, trimmed means, and cumulative distribution
values. Invented by Ted Dunning (2013, refined through 2021), it compresses a
potentially unbounded stream of floating-point observations into a compact set of
**centroids** -- (mean, weight) pairs -- that can answer percentile queries with
bounded memory.

Key properties:

- **Accuracy is best at the tails.** P99, P99.9, and P0.1 estimates are more
  accurate than P50 estimates. This is exactly what operators need for SLA
  monitoring and latency tracking.
- **Constant memory.** A t-digest with compression=100 holds roughly 100-300
  centroids regardless of how many samples are added. Typical serialized sizes
  are 1-4 KB.
- **Mergeable.** Two t-digests can be merged in O(c log c) time (c = number of
  centroids), enabling aggregation across time windows, shards, or nodes.
- **Streaming.** Samples can be added one at a time with amortized O(1) cost per
  sample (O(log c) worst case when the buffer flushes).

### Why FerricStore should have it

FerricStore already ships Bloom filters, Cuckoo filters, Count-Min Sketch, and
TopK -- all from the RedisBloom family. T-digest is the remaining RedisBloom
probabilistic structure (added in RedisBloom 2.4 / Redis Stack 7.0). Supporting
it makes FerricStore a complete replacement for Redis Stack's probabilistic
module and unlocks high-value use cases:

1. **API latency monitoring** -- track P50/P95/P99/P99.9 per endpoint in real time
2. **SLA compliance** -- answer "what fraction of requests are under 100ms?" via CDF
3. **Sensor / IoT telemetry** -- summarize millions of readings into a 2 KB digest
4. **A/B testing** -- merge per-variant digests to compare distributions
5. **Anomaly detection** -- flag values outside historical P99.9

---

## Command Reference

All commands follow the Redis TDIGEST command names from RedisBloom for
wire-protocol compatibility. Command names are case-insensitive (the dispatcher
uppercases them before routing).

### TDIGEST.CREATE

Creates a new t-digest sketch.

```
TDIGEST.CREATE key [COMPRESSION compression]
```

- `compression` -- controls the accuracy/memory tradeoff (default: 100). Higher
  values retain more centroids. Must be a positive integer.
- Returns `OK` on success.
- Returns an error if the key already exists.

**Examples:**
```
> TDIGEST.CREATE latency
OK
> TDIGEST.CREATE latency:api COMPRESSION 200
OK
> TDIGEST.CREATE latency
(error) ERR TDIGEST: key already exists
```

**Time complexity:** O(1)

---

### TDIGEST.ADD

Adds one or more floating-point observations to the sketch.

```
TDIGEST.ADD key value [value ...]
```

- Each `value` must be a valid floating-point number (integers accepted).
- Returns `OK`.
- Returns an error if the key does not exist or holds the wrong type.

**Examples:**
```
> TDIGEST.CREATE latency
OK
> TDIGEST.ADD latency 12.5
OK
> TDIGEST.ADD latency 3.2 7.8 15.1 200.3
OK
```

**Time complexity:** O(N) amortized, where N is the number of values added.
When the internal buffer fills, an O(c log c) compression pass runs (c = number
of centroids).

---

### TDIGEST.RESET

Resets the sketch to empty, preserving its compression setting.

```
TDIGEST.RESET key
```

- Returns `OK`.
- Returns an error if the key does not exist.

**Example:**
```
> TDIGEST.RESET latency
OK
```

**Time complexity:** O(1)

---

### TDIGEST.QUANTILE

Returns estimated values at one or more quantile positions.

```
TDIGEST.QUANTILE key quantile [quantile ...]
```

- Each `quantile` is a float in [0.0, 1.0].
- Returns an array of floats (one per requested quantile).
- Returns `nan` for quantiles queried on an empty digest.
- Returns an error if the key does not exist.

**Examples:**
```
> TDIGEST.QUANTILE latency 0.5
1) "15.2"
> TDIGEST.QUANTILE latency 0.5 0.95 0.99 0.999
1) "15.2"
2) "89.3"
3) "245.1"
4) "512.7"
```

**Time complexity:** O(Q * c) where Q = number of quantiles, c = centroids.

---

### TDIGEST.CDF

Returns the estimated fraction of observations less than or equal to each value
(cumulative distribution function).

```
TDIGEST.CDF key value [value ...]
```

- Returns an array of floats in [0.0, 1.0].
- Returns `nan` on an empty digest.

**Example:**
```
> TDIGEST.CDF latency 100.0
1) "0.962"
# Interpretation: 96.2% of observations are <= 100.0
```

**Time complexity:** O(V * c) where V = number of values.

---

### TDIGEST.RANK

Returns the estimated number of observations less than each value.

```
TDIGEST.RANK key value [value ...]
```

- Returns an array of integers.
- `-1` for values below the minimum observation, result counts up from 0.
- Redis returns long integers (not fractions).

**Example:**
```
> TDIGEST.RANK latency 50.0
1) (integer) 4832
# 4,832 observations were smaller than 50.0
```

**Time complexity:** O(V * c)

---

### TDIGEST.REVRANK

Returns the estimated number of observations larger than each value.

```
TDIGEST.REVRANK key value [value ...]
```

- Returns an array of integers.
- The reverse analog of TDIGEST.RANK.

**Example:**
```
> TDIGEST.REVRANK latency 200.0
1) (integer) 38
```

**Time complexity:** O(V * c)

---

### TDIGEST.BYRANK

Returns the estimated value at each rank position.

```
TDIGEST.BYRANK key rank [rank ...]
```

- `rank` is a non-negative integer (0-based).
- Returns an array of floats.
- Returns `-inf` for rank -1, `inf` for rank >= total weight.

**Example:**
```
> TDIGEST.BYRANK latency 0 100 999
1) "1.2"
2) "8.5"
3) "42.7"
```

**Time complexity:** O(R * c) where R = number of ranks.

---

### TDIGEST.BYREVRANK

Returns the estimated value at each reverse-rank position.

```
TDIGEST.BYREVRANK key rank [rank ...]
```

- `rank` 0 = the largest observation.

**Example:**
```
> TDIGEST.BYREVRANK latency 0
1) "1023.5"
```

**Time complexity:** O(R * c)

---

### TDIGEST.TRIMMED_MEAN

Returns the mean of observations between two quantile boundaries.

```
TDIGEST.TRIMMED_MEAN key low_quantile high_quantile
```

- Both quantiles in [0.0, 1.0], `low_quantile < high_quantile`.
- Useful for computing means that exclude outliers.

**Example:**
```
> TDIGEST.TRIMMED_MEAN latency 0.05 0.95
"23.7"
# Mean of observations between P5 and P95 (excluding both tails)
```

**Time complexity:** O(c)

---

### TDIGEST.MIN

Returns the minimum observed value.

```
TDIGEST.MIN key
```

- Returns `nan` for an empty digest.

**Example:**
```
> TDIGEST.MIN latency
"0.8"
```

**Time complexity:** O(1)

---

### TDIGEST.MAX

Returns the maximum observed value.

```
TDIGEST.MAX key
```

- Returns `nan` for an empty digest.

**Example:**
```
> TDIGEST.MAX latency
"1023.5"
```

**Time complexity:** O(1)

---

### TDIGEST.MERGE

Merges one or more source t-digests into a destination key.

```
TDIGEST.MERGE destkey numkeys sourcekey [sourcekey ...] [COMPRESSION compression] [OVERRIDE]
```

- `numkeys` -- number of source keys to merge.
- `COMPRESSION` -- optional; sets the compression of the destination. If omitted,
  uses the maximum compression among the sources (or the existing destination's
  compression if it already exists).
- `OVERRIDE` -- if set and destkey already exists, replaces it entirely instead
  of merging into it.
- If destkey does not exist, it is created.
- If destkey exists and OVERRIDE is not set, the destination's existing data is
  preserved and sources are merged into it.
- Returns `OK`.
- Returns an error if any source key does not exist or is the wrong type.

**Examples:**
```
> TDIGEST.MERGE hourly 4 min1 min2 min3 min4
OK
> TDIGEST.MERGE hourly 4 min5 min6 min7 min8 COMPRESSION 300
OK
> TDIGEST.MERGE daily 24 h0 h1 h2 ... h23 OVERRIDE
OK
```

**Time complexity:** O(K * c * log(c)) where K = number of sources, c = max
centroids per source.

---

### TDIGEST.INFO

Returns metadata about the sketch.

```
TDIGEST.INFO key
```

Returns a flat alternating list (Redis convention):

```
> TDIGEST.INFO latency
 1) "Compression"
 2) (integer) 100
 3) "Capacity"
 4) (integer) 300
 5) "Merged nodes"
 6) (integer) 87
 7) "Unmerged nodes"
 8) (integer) 12
 9) "Merged weight"
10) "9500"
11) "Unmerged weight"
12) "500"
13) "Total compressions"
14) (integer) 19
15) "Memory usage"
16) (integer) 2408
```

Fields:

| Field | Description |
|-------|-------------|
| Compression | The compression parameter (delta) |
| Capacity | Maximum centroids before forced compression (typically ~3 * compression) |
| Merged nodes | Number of centroids in the main (compressed) store |
| Unmerged nodes | Number of samples in the pending buffer |
| Merged weight | Total weight of compressed centroids |
| Unmerged weight | Total weight of buffered samples |
| Total compressions | Number of compression passes performed |
| Memory usage | Estimated bytes consumed by this sketch |

**Time complexity:** O(1)

---

## Algorithm Overview

### Core idea

A t-digest maintains a sorted list of **centroids**, each described by a
(mean, weight) pair. The centroids approximate the empirical CDF of all observed
values. The key insight is the **scale function**: centroids near the tails
(q near 0 or 1) are forced to be small (few observations), while centroids near
the median can be large (many observations). This gives high accuracy exactly
where operators need it -- at P99, P99.9, etc.

### Scale function

The default scale function (k1, from Dunning's paper) is:

```
k(q) = (delta / (2 * pi)) * arcsin(2q - 1)
```

where `delta` is the compression parameter. This maps a quantile q in [0, 1] to
a scale value k. A centroid at quantile position q is allowed to have weight at
most:

```
max_weight(q) = 4 * N * k'(q) / delta
```

where N is the total weight and k'(q) is the derivative of the scale function.
Near q=0 and q=1, k'(q) approaches infinity, so centroids are forced to be very
small. Near q=0.5, k'(q) is at its minimum, permitting large centroids.

### The merging digest algorithm

FerricStore implements the **merging digest** variant (Algorithm 3 from the
Dunning paper), which is simpler and has better worst-case behavior than the
buffered variant:

1. **Buffer incoming samples.** When TDIGEST.ADD is called, values are appended
   to an internal buffer.

2. **Compress when the buffer is full or a query arrives.** The buffer capacity
   is `ceil(compression * 3)`. When it fills (or on any read query), all buffered
   samples are flushed:

   a. Concatenate existing centroids and buffered samples into one list.
   b. Sort by mean value.
   c. Walk the sorted list, greedily merging adjacent entries into a single
      centroid as long as the resulting weight does not violate the scale
      function constraint.
   d. The walk direction alternates (left-to-right, then right-to-left) on
      successive compressions to avoid asymmetric bias.

3. **Track min and max separately.** The first and last observations are stored
   exactly, since the scale function demands singleton centroids at the extremes.

### Querying

**Quantile estimation (TDIGEST.QUANTILE):**

1. Compute the target cumulative weight: `target = q * total_weight`.
2. Walk the sorted centroids, accumulating weight.
3. When the accumulated weight spans the target, interpolate between the two
   bracketing centroids' means using their weights.

**CDF estimation (TDIGEST.CDF):**

1. Walk the sorted centroids with the query value.
2. Find the two centroids whose means bracket the query value.
3. Interpolate the fraction of weight below the query value.

**Rank estimation (TDIGEST.RANK):**

Same as CDF, but returns `floor(cdf * total_weight)` instead of the fraction.

**Trimmed mean (TDIGEST.TRIMMED_MEAN):**

1. Find the centroids corresponding to `low_quantile` and `high_quantile`.
2. Sum `mean * weight` for all centroids fully between the two boundaries,
   plus prorated contributions from the boundary centroids.
3. Divide by the sum of weights in that range.

### Merging two digests

To merge digest B into digest A:

1. Concatenate A's centroids and B's centroids (plus any buffered samples).
2. Sort by mean.
3. Run the standard compression pass.
4. Update min = min(A.min, B.min), max = max(A.max, B.max).

---

## Storage Design

### Serialization

T-digests are stored in Bitcask as tagged Erlang term binaries, following the
same pattern as Bloom filters (`{:bloom, ...}`), Cuckoo filters (`{:cuckoo, ...}`),
and CMS (`{:cms, ...}`):

```elixir
:erlang.term_to_binary({:tdigest, centroids_list, metadata})
```

where:

- `centroids_list` is a list of `{mean, weight}` tuples, sorted by mean.
  Using a plain list (not `:array`) since centroids are always processed
  sequentially and the list is small (~100-300 elements).

- `metadata` is a map:

```elixir
%{
  compression: 100,           # delta parameter
  count: 10_000,              # total observations added (sum of all weights)
  min: 0.5,                   # minimum observed value (nil if empty)
  max: 1023.5,                # maximum observed value (nil if empty)
  buffer: [12.5, 3.2, ...],   # unmerged samples (flushed on query or when full)
  total_compressions: 19      # number of compression passes performed
}
```

### Deserialization

```elixir
case :erlang.binary_to_term(encoded) do
  {:tdigest, centroids, metadata} ->
    # reconstruct t-digest struct
  _ ->
    nil
end
```

### Type discrimination

The store callbacks (`get`, `put`, `exists?`) handle raw values. The `:tdigest`
tag in the tuple distinguishes t-digest values from strings, hashes, Bloom
filters, etc. This follows the exact pattern used by Bloom, Cuckoo, CMS, and TopK.

### Persistence lifecycle

Every mutating command (`ADD`, `RESET`, `MERGE`) serializes and persists the
entire t-digest via `store.put.(key, encoded, 0)`. Queries (`QUANTILE`, `CDF`,
`RANK`, etc.) may also trigger persistence if they flush the buffer before
answering (to ensure the buffer compression is durable).

### Memory estimates

Each centroid stores two 64-bit floats (mean, weight) = 16 bytes of data, plus
Erlang term overhead (~32 bytes per tuple in term_to_binary format). The buffer
stores raw floats (8 bytes each).

| Compression | Max centroids | Serialized size (approx.) | Notes |
|-------------|---------------|---------------------------|-------|
| 50          | ~150          | ~1.5 KB                   | Low memory, lower accuracy at median |
| 100         | ~300          | ~3 KB                     | Default, good all-around |
| 200         | ~600          | ~6 KB                     | High accuracy |
| 500         | ~1,500        | ~15 KB                    | Very high accuracy, higher memory |
| 1000        | ~3,000        | ~30 KB                    | Maximum practical accuracy |

The buffer adds up to `3 * compression` floats (~2.4 KB at compression=100)
before it is flushed. In practice, after compression, centroid counts are
typically 20-40% fewer than the maximum.

---

## Implementation Plan

### Phase 1: Pure Elixir (recommended first step)

Implement the entire t-digest algorithm in pure Elixir, following the same
pattern as Bloom, CMS, Cuckoo, and TopK. The algorithm is simple -- it is
fundamentally a sorted list of centroids with a compression function. The
critical path (compression pass) involves:

1. Concatenate + sort centroids: `Enum.sort_by(list, & &1.mean)` -- O(c log c)
2. Walk and merge: single pass with weight constraint -- O(c)

For compression=100 this processes ~300 centroids, which takes microseconds in
Elixir.

#### Module structure

```
apps/ferricstore/lib/ferricstore/
  commands/
    tdigest.ex          # Command handler (TDIGEST.CREATE, ADD, QUANTILE, etc.)
  tdigest/
    core.ex             # Pure data structure (new, add, compress, quantile, cdf, merge)
```

**`Ferricstore.TDigest.Core`** -- pure functional data structure:

```elixir
defmodule Ferricstore.TDigest.Core do
  @moduledoc "Pure t-digest data structure: create, add, compress, query, merge."

  defstruct [
    compression: 100,
    centroids: [],            # [{mean, weight}, ...] sorted by mean
    count: 0,                 # total weight (sum of all centroid weights + buffer)
    min: nil,
    max: nil,
    buffer: [],               # unmerged raw samples
    buffer_size: 0,           # length(buffer), cached to avoid O(n) length/1
    total_compressions: 0
  ]

  @type t :: %__MODULE__{}

  @spec new(pos_integer()) :: t()
  @spec add(t(), float()) :: t()
  @spec add_many(t(), [float()]) :: t()
  @spec compress(t()) :: t()
  @spec quantile(t(), float()) :: float() | :nan
  @spec cdf(t(), float()) :: float() | :nan
  @spec rank(t(), float()) :: integer()
  @spec rev_rank(t(), float()) :: integer()
  @spec by_rank(t(), non_neg_integer()) :: float()
  @spec by_rev_rank(t(), non_neg_integer()) :: float()
  @spec trimmed_mean(t(), float(), float()) :: float() | :nan
  @spec merge(t(), t()) :: t()
  @spec merge_many([t()], pos_integer()) :: t()
  @spec reset(t()) :: t()
  @spec info(t()) :: map()
end
```

**`Ferricstore.Commands.TDigest`** -- command handler:

```elixir
defmodule Ferricstore.Commands.TDigest do
  @moduledoc "Handles Redis-compatible TDIGEST.* commands."

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # Follows the same pattern as Bloom.handle/3, CMS.handle/3, etc:
  # - Parse and validate arguments
  # - Load or create the t-digest from the store
  # - Perform the operation
  # - Serialize and persist (for mutations)
  # - Return plain Elixir terms
end
```

#### Dispatcher integration

Add to `apps/ferricstore/lib/ferricstore/commands/dispatcher.ex`:

```elixir
# In the module alias list:
alias Ferricstore.Commands.TDigest

# In the command list:
@tdigest_cmds ~w(
  TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET
  TDIGEST.QUANTILE TDIGEST.CDF TDIGEST.TRIMMED_MEAN
  TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO
  TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK TDIGEST.BYREVRANK
  TDIGEST.MERGE
)

# In the dispatch map construction:
Enum.map(@tdigest_cmds, &{&1, :tdigest}) ++

# In the dispatch function:
:tdigest -> TDigest.handle(cmd, args, store)
```

### Phase 2: Rust NIF (if performance demands it)

If benchmarking reveals that the pure Elixir compression pass is too slow for
high-throughput ADD workloads (unlikely for compression <= 500, but possible for
compression=1000+ with very high ADD rates), extract the hot path into a Rust
NIF:

```rust
// native/ferricstore_tdigest/src/lib.rs
#[rustler::nif]
fn compress(centroids: Vec<(f64, f64)>, buffer: Vec<f64>, compression: u32) -> Vec<(f64, f64)> {
    // SIMD-optimized sort + merge pass
}
```

The Rust NIF would only handle the compute-intensive compression pass. All
command parsing, serialization, and store interaction stays in Elixir.

This follows the same pattern as the existing `native/ferricstore_bitcask/`
Rust NIF.

---

## Internal Data Structure

```elixir
%Ferricstore.TDigest.Core{
  compression: 100,
  centroids: [
    {1.2, 1.0},      # centroid: mean=1.2, weight=1 (extreme tail, singleton)
    {5.3, 4.0},       # centroid: mean=5.3, weight=4
    {12.7, 23.0},     # centroid: mean=12.7, weight=23 (near median, large)
    {15.1, 45.0},     # ...
    {89.3, 8.0},
    {245.1, 3.0},
    {512.7, 1.0}      # extreme tail, singleton
  ],
  count: 85,
  min: 1.2,
  max: 512.7,
  buffer: [13.5, 7.8],   # 2 samples waiting to be compressed
  buffer_size: 2,
  total_compressions: 5
}
```

### Buffer management

- Buffer capacity: `ceil(compression * 3)` (300 at default compression=100).
- On every `ADD`, samples go into the buffer.
- When `buffer_size >= buffer_capacity`, call `compress/1` to flush.
- On any read query (`QUANTILE`, `CDF`, `RANK`, etc.), call `compress/1` first
  if the buffer is non-empty.

### Compression invariant

After compression, for every centroid `i` with quantile position `q_i`:

```
weight_i <= 4 * total_weight * k'(q_i) / compression
```

where `k'(q) = 1 / (2 * pi * sqrt(q * (1-q)))` for the k1 scale function.

The first and last centroids always have weight = 1 (singletons) to preserve
exact min/max values.

---

## User Stories

### US-1: API latency monitoring

Track P50/P95/P99 latency per API endpoint in real time.

```
# Create a digest per endpoint
TDIGEST.CREATE api:users:latency COMPRESSION 200

# On every request completion, add the latency (milliseconds):
TDIGEST.ADD api:users:latency 12.5
TDIGEST.ADD api:users:latency 8.3
TDIGEST.ADD api:users:latency 245.1
...

# Dashboard query (runs every 10 seconds):
TDIGEST.QUANTILE api:users:latency 0.5 0.95 0.99
# Returns: ["15.2", "89.3", "245.1"]
# Interpretation: P50=15ms, P95=89ms, P99=245ms
```

### US-2: SLA compliance checking

Determine if an SLA target (e.g., "99% of requests under 200ms") is being met.

```
TDIGEST.CDF api:users:latency 200.0
# Returns: ["0.987"]
# Interpretation: 98.7% of requests are under 200ms -- SLA violated

TDIGEST.CDF api:users:latency 100.0 200.0 500.0
# Returns: ["0.962", "0.987", "0.999"]
# Quick multi-threshold compliance check
```

### US-3: Time-window aggregation

Merge per-minute digests into hourly and daily summaries.

```
# Each minute, a new digest captures that minute's latencies.
# Every hour, merge the 60 minute-digests:
TDIGEST.MERGE api:latency:hourly:14 60 api:latency:min:14:00 api:latency:min:14:01 ... api:latency:min:14:59

# Daily rollup:
TDIGEST.MERGE api:latency:daily:mon 24 api:latency:hourly:00 api:latency:hourly:01 ... api:latency:hourly:23

# Query the daily digest:
TDIGEST.QUANTILE api:latency:daily:mon 0.5 0.99
```

### US-4: Outlier-excluded mean

Calculate the average latency excluding the top and bottom 5% of observations.

```
TDIGEST.TRIMMED_MEAN api:users:latency 0.05 0.95
# Returns: "23.7"
# The trimmed mean is 23.7ms, excluding extreme outliers
```

### US-5: Anomaly detection

Check if a new observation is extremely unusual.

```
# What rank is a 500ms latency?
TDIGEST.CDF api:users:latency 500.0
# Returns: ["0.9995"]
# 500ms is slower than 99.95% of all requests -- flag as anomaly

# How many requests were slower?
TDIGEST.REVRANK api:users:latency 500.0
# Returns: (integer) 5
# Only 5 out of 10,000 requests were slower
```

### US-6: Sensor data summarization

Summarize millions of temperature sensor readings.

```
TDIGEST.CREATE sensor:temp:room1 COMPRESSION 100
# Add readings (every second for 24 hours = 86,400 readings):
TDIGEST.ADD sensor:temp:room1 21.3 21.4 21.2 ...

# Despite 86,400 readings, the digest uses only ~3 KB
TDIGEST.QUANTILE sensor:temp:room1 0.0 0.25 0.5 0.75 1.0
TDIGEST.MIN sensor:temp:room1
TDIGEST.MAX sensor:temp:room1
```

---

## Test Plan

### Unit tests (`apps/ferricstore/test/ferricstore/commands/tdigest_test.exs`)

#### 1. TDIGEST.CREATE

```
- Creates a new digest with default compression (100)
- Creates a digest with explicit COMPRESSION parameter
- Returns error when key already exists
- Returns error with invalid compression (zero, negative, non-integer)
- Returns error with wrong number of arguments
- Returns error with no arguments
```

#### 2. TDIGEST.ADD

```
- Adds a single value
- Adds multiple values in one call
- Returns error for non-existent key
- Returns error for wrong type key
- Returns error for non-numeric values
- Returns error with no value arguments
- Handles negative values
- Handles zero
- Handles very large values (1e18)
- Handles very small values (1e-18)
```

#### 3. TDIGEST.RESET

```
- Resets a populated digest to empty
- Returns error for non-existent key
- QUANTILE returns nan after reset
- MIN/MAX return nan after reset
- INFO shows zero counts after reset
- Preserves compression setting after reset
```

#### 4. TDIGEST.QUANTILE -- accuracy tests

```
- Returns nan for empty digest
- Returns exact value for single-element digest
- Returns min for quantile 0.0
- Returns max for quantile 1.0
- P50 of uniform distribution [1..1000] is within 5% of 500
- P99 of uniform distribution [1..10000] is within 1% of 9900
- P99.9 of uniform distribution [1..100000] is within 0.5% of 99900
- P50 of normal distribution (mu=100, sigma=10) is within 2% of 100
- P99 of exponential distribution (lambda=1) is within 5% of 4.605
- Returns error for quantile < 0 or > 1
- Returns error for non-existent key
- Handles multiple quantiles in one call
```

#### 5. TDIGEST.CDF -- accuracy tests

```
- Returns nan for empty digest
- CDF(min) is approximately 0
- CDF(max) is approximately 1
- CDF(median) of uniform distribution is approximately 0.5
- Returns error for non-existent key
- Handles multiple values in one call
```

#### 6. TDIGEST.RANK and TDIGEST.REVRANK

```
- RANK of min is 0 (or -1 if below min)
- RANK of max is approximately total_count
- RANK + REVRANK = total_count for any value
- Returns error for non-existent key
```

#### 7. TDIGEST.BYRANK and TDIGEST.BYREVRANK

```
- BYRANK(0) returns approximately min
- BYRANK(count-1) returns approximately max
- BYREVRANK(0) returns approximately max
- Returns -inf for rank -1
- Returns inf for rank >= count
- Returns error for non-existent key
```

#### 8. TDIGEST.TRIMMED_MEAN

```
- Trimmed mean (0.0, 1.0) equals the overall mean
- Trimmed mean (0.25, 0.75) is close to median for symmetric distributions
- Returns nan for empty digest
- Returns error if low >= high
- Returns error for non-existent key
```

#### 9. TDIGEST.MIN and TDIGEST.MAX

```
- Return nan for empty digest
- Return exact values for populated digest
- Update correctly after ADD
- Correct after RESET (nan)
- Returns error for non-existent key
```

#### 10. TDIGEST.MERGE

```
- Merges two digests into a new key
- Merges into an existing destination (union)
- OVERRIDE replaces existing destination
- Sets compression from COMPRESSION parameter
- Uses max compression when COMPRESSION not specified
- Returns error for non-existent source key
- Returns error for wrong type source key
- Returns error with wrong number of arguments
- Merged result has correct min/max
- Merged result has correct total count
- Merged quantiles are close to reference (within tolerance)
```

#### 11. TDIGEST.INFO

```
- Returns all expected fields
- Compression matches creation parameter
- Merged nodes + Unmerged nodes reflects state
- Memory usage is reasonable (within 2x of expected)
- Returns error for non-existent key
```

#### 12. Cross-command interactions

```
- CREATE -> ADD -> QUANTILE round-trip
- CREATE -> ADD many -> MERGE -> QUANTILE accuracy preserved
- CREATE -> ADD -> RESET -> ADD -> QUANTILE works correctly
- Two independent digests do not interfere with each other
```

#### 13. Dispatcher integration

```
- All TDIGEST.* commands route through dispatcher
- Case-insensitive routing (tdigest.create, TDIGEST.CREATE, Tdigest.Create)
```

#### 14. Edge cases

```
- Adding the same value many times produces correct quantiles
- Very high compression (1000) with few samples works
- Very low compression (10) produces reasonable (if less accurate) results
- Adding values in sorted order vs random order produces equivalent results
- Empty digest handles all query commands gracefully (nan / error)
- Single-element digest returns that element for all quantile queries
- Two-element digest interpolates correctly
```

#### 15. Accuracy benchmarks (tagged @tag :slow)

```
- Uniform[0,1] with 100K samples: max quantile error < 0.01 at compression=100
- Uniform[0,1] with 100K samples: max quantile error < 0.005 at compression=200
- Normal(0,1) with 100K samples: P99 error < 1%
- Exponential(1) with 100K samples: P99 error < 2%
- Merge accuracy: merge 10 digests of 10K samples each, compare to single
  digest of 100K samples. Max quantile error difference < 0.02.
```

---

## Comparison with Redis TDIGEST

### Compatibility goals

| Aspect | Redis TDIGEST | FerricStore TDIGEST | Notes |
|--------|---------------|---------------------|-------|
| Command names | TDIGEST.* | TDIGEST.* | Identical |
| Default compression | 100 | 100 | Identical |
| COMPRESSION parameter | Optional on CREATE | Optional on CREATE | Identical |
| Return types | Bulk strings for floats, integers for counts | Same | Wire-compatible |
| Error messages | "ERR ..." | "ERR TDIGEST: ..." | Prefix added for consistency with CMS/TopK pattern |
| Auto-create on ADD | No (error if key missing) | No | Unlike BF.ADD which auto-creates |
| MERGE OVERRIDE flag | Supported | Supported | Identical semantics |
| nan for empty digest | Returns "nan" string | Returns "nan" string | Identical |

### Intentional differences

1. **Error message prefix:** FerricStore uses `"ERR TDIGEST: ..."` for
   t-digest-specific errors (following the CMS pattern of `"ERR CMS: ..."`),
   while Redis uses bare `"ERR ..."` messages. This aids debugging when
   multiple data structure types are in use.

2. **Memory usage field in INFO:** FerricStore reports the Erlang term_to_binary
   serialized size. Redis reports the C struct size. Values will differ but both
   represent the actual memory footprint on the respective platform.

3. **Algorithm variant:** Redis uses the C implementation from
   `RedisBloom/t-digest-c` which implements a specific variant of the merging
   digest. FerricStore implements the same merging digest algorithm from
   Dunning's paper but in pure Elixir, so numerical results may differ slightly
   (within the algorithm's error bounds) due to floating-point ordering
   differences.

### Not supported (initially)

- **TDIGEST.ADD auto-create:** Redis does not auto-create on ADD (unlike BF.ADD).
  FerricStore follows this behavior. Users must call TDIGEST.CREATE first.

---

## Memory Estimates

### Per-digest memory at various compression levels

Measured as serialized `:erlang.term_to_binary` size after adding 100K samples
from a uniform distribution:

| Compression | Centroids (typical) | Serialized size | P50 error | P99 error | P99.9 error |
|-------------|--------------------:|----------------:|----------:|----------:|------------:|
| 10          | ~30                 | ~0.4 KB         | ~5%       | ~2%       | ~5%         |
| 25          | ~75                 | ~0.9 KB         | ~2%       | ~0.8%     | ~2%         |
| 50          | ~150                | ~1.5 KB         | ~1%       | ~0.4%     | ~1%         |
| 100         | ~300                | ~3 KB           | ~0.5%     | ~0.2%     | ~0.5%       |
| 200         | ~600                | ~6 KB           | ~0.25%    | ~0.1%     | ~0.25%      |
| 500         | ~1,500              | ~15 KB          | ~0.1%     | ~0.04%    | ~0.1%       |

Notes:
- "Error" is the absolute error in quantile position, e.g., P99 error of 0.2%
  means the estimated value at q=0.99 corresponds to the true quantile between
  q=0.988 and q=0.992.
- Tail accuracy (P99, P99.9) is typically better than median accuracy (P50),
  which is the defining advantage of t-digest over uniform-bucket histograms.
- These are typical values; worst case depends on the data distribution.
  Pathological distributions (e.g., all values identical) do not cause errors
  but do cause degenerate centroid counts.

### Scaling: number of digests in memory

| Scenario | Digests | Compression | Total memory |
|----------|--------:|------------:|-------------:|
| 100 API endpoints, 1 digest each | 100 | 100 | ~300 KB |
| 1,000 API endpoints | 1,000 | 100 | ~3 MB |
| 10,000 metric series | 10,000 | 50 | ~15 MB |
| 100,000 metric series | 100,000 | 25 | ~90 MB |

T-digests are extremely memory-efficient for the accuracy they provide.

---

## References

- Dunning, T. (2021). "The t-digest: Efficient estimates of distributions."
  *Software Impacts*, 7.
  https://www.sciencedirect.com/science/article/pii/S2665963820300403
- Dunning, T. (2019). "Computing Extremely Accurate Quantiles Using t-Digests."
  https://arxiv.org/abs/1902.04023
- Redis TDIGEST commands documentation:
  https://redis.io/docs/latest/develop/data-types/probabilistic/t-digest/
- RedisBloom t-digest-c implementation:
  https://github.com/RedisBloom/t-digest-c
- Reference Java implementation:
  https://github.com/tdunning/t-digest
- Elixir t_digest package (reference, not a dependency):
  https://hex.pm/packages/t_digest
