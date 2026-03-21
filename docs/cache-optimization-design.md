# Cache Optimization Design: LFU Eviction, Keydir-Hot Cache Merge, and L1 Process-Local Cache

This document describes three cache optimizations for FerricStore. All three are
design-only -- no implementation is included. The designs reference the existing
codebase architecture: ETS `keydir_N` and `hot_cache_N` tables per shard,
`Router.ets_get/4` fast-path reads, `MemoryGuard` pressure-based eviction, and
`ClientTracking` invalidation infrastructure.

## 1. LFU with Decay (Replacing LRU)

### Problem

The current eviction in `MemoryGuard.maybe_evict/1` samples up to 10 keys from
`keydir_N` and evicts from `hot_cache_N` using LRU-like heuristics. However, the
`hot_cache_lookup/3` function in `Router` updates the `access_ms` timestamp on
every read. This means a one-time full KEYS scan or SCAN iteration touches every
key, resetting all access timestamps -- effectively randomizing eviction order and
potentially evicting the genuinely hot keys.

Redis solved this with LFU (Least Frequently Used) with logarithmic counters and
time-based decay (Redis 4.0+). FerricStore should adopt the same approach.

### Data structure

Add two fields to each entry in the `keydir_N` ETS table:

```
Current keydir entry:  {key, expire_at_ms}                        -- 2-tuple
Proposed keydir entry: {key, expire_at_ms, lfu_counter, decay_ts} -- 4-tuple
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `lfu_counter` | integer (0-255) | 8 bits logical | Logarithmic access frequency counter |
| `decay_ts` | integer | 16 bits logical | Minutes since epoch mod 65536 (wraps ~45 days) |

The total memory overhead is small: two additional integer elements per ETS tuple.
On a 64-bit BEAM, each additional small integer in an ETS tuple costs one word
(8 bytes), so +16 bytes per key. For 10M keys this is ~160 MB additional keydir
RAM -- comparable to the existing `expire_at_ms` field cost.

### Counter increment behavior

On every read access (in `Router.hot_cache_lookup/3` and `Shard.warm_from_store/2`):

```elixir
defp lfu_increment(counter, lfu_log_factor) do
  if counter < 255 do
    p = 1.0 / (counter * lfu_log_factor + 1)
    if :rand.uniform() < p, do: counter + 1, else: counter
  else
    counter
  end
end
```

With the default `lfu_log_factor = 10`:

| Current counter | Probability of increment |
|----------------|-------------------------|
| 0 | 100% |
| 1 | 9.1% |
| 5 | 1.96% |
| 10 | 0.99% |
| 50 | 0.20% |
| 100 | 0.10% |
| 255 | 0% (capped) |

This means a key accessed 1M times reaches a counter of roughly 20-25. A key
accessed once stays at 1. The compression is extreme by design -- it lets us
distinguish "very hot" from "moderately hot" from "cold" in just 8 bits.

### Decay behavior

A periodic tick (default: every 1 minute, configurable via `lfu_decay_time`)
halves the counter for keys that have not been accessed since the last decay:

```elixir
defp lfu_decay(counter, last_access_minutes, current_minutes, lfu_decay_time) do
  idle_minutes = current_minutes - last_access_minutes
  # How many decay periods have elapsed
  num_periods = div(idle_minutes, lfu_decay_time)
  # Halve the counter once per elapsed period, minimum 0
  Enum.reduce(1..max(num_periods, 0), counter, fn _, c -> div(c, 2) end)
end
```

Decay is applied lazily: not via a background sweep, but at read time and at
eviction sampling time. When `MemoryGuard.maybe_evict/1` samples a key, it
computes the decayed counter on the spot. When `Router.hot_cache_lookup/3` reads
a key, it applies decay before incrementing. This avoids a global sweep that
would touch every key.

### New key initialization

New keys start with `lfu_counter = 5` (not 0). This prevents a newly-inserted
key from being immediately evictable -- it has the same counter as a key
accessed ~5 times. The value 5 matches Redis's `LFU_INIT_VAL`.

### Eviction algorithm

Replace the current `MemoryGuard.maybe_evict/1` sampling logic:

1. Sample N random keys from `keydir_N` (default N=10, same as current).
2. For each sampled key, compute its decayed LFU counter.
3. Evict the key with the lowest decayed counter.
4. On ties, evict the key with the oldest `decay_ts` (least recently accessed).
5. Only evict from `hot_cache_N` (keydir entry and Bitcask data remain -- same
   as current behavior). The next GET re-warms from Bitcask.

For `volatile_lfu` and `volatile_ttl` policies, only sample keys with
`expire_at_ms > 0`. For `allkeys_lfu`, sample all keys.

### Changes to hot_cache_lookup

The current `Router.hot_cache_lookup/3` updates the access timestamp:

```elixir
# Current: updates access_ms for LRU
:ets.insert(hot_cache, {key, value, now})
```

With LFU, replace this with a counter increment and decay-timestamp update in
`keydir_N`. The `hot_cache_N` entry no longer needs `access_ms` at all:

```elixir
# Proposed: update LFU counter + decay_ts in keydir, leave hot_cache as {key, value}
case :ets.lookup(keydir, key) do
  [{^key, exp, counter, _old_ts}] ->
    new_counter = lfu_increment(counter, lfu_log_factor())
    :ets.insert(keydir, {key, exp, new_counter, current_minute()})
  _ -> :ok
end
```

This also eliminates the `hot_cache` write-on-read that currently happens for
every ETS-served GET, reducing write contention on the hot_cache table.

### Config parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxmemory-policy` | `volatile-lfu` | New valid values: `allkeys-lfu`, `volatile-lfu` (alongside existing `allkeys-lru`, `volatile-lru`, `volatile-ttl`, `noeviction`) |
| `lfu-log-factor` | `10` | Higher = slower counter growth, better discrimination for very hot keys |
| `lfu-decay-time` | `1` | Minutes between halving. Higher = longer memory of past access frequency |

These should be added to `Ferricstore.Config` as read-write parameters with
`CONFIG GET`/`CONFIG SET` support.

### Why LFU is better than LRU for FerricStore

1. **Scan resistance.** A `KEYS *` or `SCAN` touching every key once bumps each
   key's LRU timestamp to "now", randomizing eviction. With LFU, each key's
   counter increments by at most 1 (and likely 0 for high counters), so truly
   hot keys remain protected.

2. **Temporal stability.** A key accessed 1000 times per second accumulates a
   high LFU counter. If access drops temporarily (e.g., between request bursts),
   the counter decays slowly -- the key survives cold periods without being
   evicted. LRU would evict it the moment something else is accessed more
   recently.

3. **Better cache hit rate.** Redis benchmarks show LFU achieves 5-15% higher
   hit rates than LRU on real-world workloads with mixed access patterns.

### Migration path

The keydir tuple format change (`{key, exp}` to `{key, exp, counter, decay_ts}`)
affects every `ets.lookup` and `ets.insert` call on keydir tables. Affected
modules:

- `Router` -- `ets_get/4`, `hot_cache_lookup/3`
- `Shard` -- `ets_lookup/2`, `ets_insert/4`, `ets_delete_key/2`, init, sweep
- `MemoryGuard` -- `maybe_evict/1`
- `Promotion` -- `promote_collection!/7`, `recover_promoted/5`
- `Raft.StateMachine` -- keydir insert/delete in apply callbacks

During rolling upgrades, the shard init should detect the old 2-tuple format and
upgrade in place by setting `counter = 5, decay_ts = current_minute()`.

---

## 2. Keydir + Hot Cache Merge Options

### Background

FerricStore currently uses two ETS tables per shard:

- `keydir_N` -- `{key, expire_at_ms}` -- metadata only, ~120 bytes/key
- `hot_cache_N` -- `{key, value, access_ms}` -- cached values, variable size

A hot-path read in `Router.ets_get/4` performs two ETS lookups: first `keydir_N`
to check expiry, then `hot_cache_N` to get the value. This design was chosen
because keeping large values out of the keydir table means keydir stays small
enough to fit in L2/L3 cache, and MemoryGuard can manage the two budgets
independently (`keydir_max_ram` vs `hot_cache_max_ram`).

This section analyzes three alternatives.

### Option A: Single merged table

```
{key, value, expire_at_ms, lfu_counter}
```

**Pros:**
- One ETS lookup per read instead of two.
- Simpler code -- one table to manage, one set of insert/delete calls.
- No cache-miss divergence (a key cannot be "in keydir but not in hot_cache").

**Cons:**
- Large values (e.g., 10 MB JSON blobs) live in the same ETS table as the
  keydir metadata. ETS copies values on read, so every lookup of a 10 MB value
  copies 10 MB. Currently, keydir lookups only copy the small metadata tuple.
- MemoryGuard can no longer independently shrink the value cache under pressure
  (its current strategy is to evict from `hot_cache` while keeping `keydir`
  intact so the key still routes to Bitcask on the next read).
- Evicting a value requires deleting the entire entry, losing the keydir metadata.
  The next access would need a Bitcask `keys()` scan to rediscover the key --
  unacceptable.
- The `keydir_max_ram` budget concept breaks down because metadata and values
  share one table.

**Verdict:** Not recommended. The independent sizing of keydir vs hot_cache is a
core architectural feature that enables graceful degradation under memory
pressure.

### Option B: Keep separate tables, add hot flag to keydir

```
keydir_N:    {key, expire_at_ms, lfu_counter, :hot | :cold}
hot_cache_N: {key, value}
```

The `:hot` / `:cold` flag in keydir tells `Router.ets_get/4` whether to bother
looking in `hot_cache_N`. For cold keys, the router skips the second ETS lookup
and goes straight to the GenServer `{:get, key}` call.

**Pros:**
- Eliminates the hot_cache lookup for cold keys. Currently, every keydir miss
  still checks hot_cache (which is always empty for cold keys), wasting ~200ns.
- Keydir stays small and cache-friendly.
- MemoryGuard retains independent budget control.
- Minimal code change: add one element to the keydir tuple, update
  `ets_get/4` to check the flag.

**Cons:**
- Still two lookups for hot keys. The second lookup (hot_cache) takes ~200ns.
- The flag must be updated on eviction (hot -> cold) and on cache warming
  (cold -> hot), adding two extra `:ets.insert` calls to those paths.

**Estimated latency impact:**
- Cold key read: -200ns (skip hot_cache miss lookup)
- Hot key read: unchanged (still two lookups)
- Eviction: +negligible (one extra keydir update)

### Option C: ResourceBinary value pointer in keydir

```
keydir_N: {key, expire_at_ms, lfu_counter, value_ref | nil}
```

Where `value_ref` is a NIF ResourceBinary -- a reference-counted pointer to
memory managed by the Rust NIF. Reading the value is a zero-copy operation:
the BEAM gets a pointer into NIF-managed memory without copying.

**Pros:**
- One ETS lookup, zero-copy value access.
- Could theoretically achieve ~5ns read latency (same as a process-local map
  lookup) since the ETS lookup returns a reference, not a copy.

**Cons:**
- Requires significant NIF changes: the Rust side must keep values in a
  reference-counted arena that outlives individual NIF calls.
- Resource lifecycle is complex: the value must not be freed while any BEAM
  process holds a reference. This interacts poorly with Bitcask compaction
  (which rewrites data files and invalidates file offsets).
- A bug in lifecycle management leads to use-after-free or memory leaks --
  both are worse than a performance regression.
- ETS does not natively understand ResourceBinary pinning; the reference could
  be garbage-collected while still in ETS if the resource term is not properly
  ref-counted.
- Debugging and profiling become harder since values live outside BEAM heap
  accounting.

**Verdict:** Too complex for the current stage. Defer to v2 if profiling shows
the second ETS lookup is a measurable bottleneck.

### Recommendation

**Option B (hot flag in keydir)** is the recommended approach:

1. Low implementation risk -- additive change to keydir tuple, no NIF changes.
2. Combines naturally with the LFU counter addition (both add fields to keydir).
3. Provides measurable improvement for the cold-key read path.
4. Preserves the independent keydir/hot_cache budget model.

If LFU is implemented first (Section 1), the keydir tuple becomes:

```
{key, expire_at_ms, lfu_counter, decay_ts, :hot | :cold}
```

Five elements per tuple, ~40 bytes per key of overhead on 64-bit BEAM (5 words).
For 10M keys: ~400 MB keydir RAM, within the default 256 MB `keydir_max_ram`
only for smaller key counts. The `keydir_max_ram` default may need to increase
to 512 MB or be auto-calculated from `max_memory_bytes`.

---

## 3. L1 Process-Local Cache

### Problem

Every GET in FerricStore currently traverses:

```
Connection process  --GenServer.call-->  Router.ets_get/4
                                            |
                                    ETS lookup (keydir_N)  ~200ns
                                            |
                                    ETS lookup (hot_cache_N)  ~200ns
                                            |
                                        total: ~400ns best case
```

For workloads where a single connection repeatedly reads the same keys (session
tokens, rate limit counters, config flags), the ~400ns ETS round-trip is
repeated identically on every request. A process-local map lookup costs ~5ns.

### Architecture

```
L1: Connection process dictionary / Map   per-connection, ~64 entries, ~5ns read
    |
    v  (L1 miss)
L2: ETS hot_cache_N                       per-shard, millions of entries, ~200ns read
    |
    v  (L2 miss)
L3: Bitcask on disk                       all entries, ~50-200us read
```

### Data structure

Store the L1 cache in the `%FerricstoreServer.Connection{}` struct:

```elixir
defstruct [
  # ... existing fields ...
  l1_cache: %{},          # key => l1_entry
  l1_size: 0,             # current entry count
]

# L1 cache entry
%{
  value: binary(),
  expire_at_ms: integer(),
  cached_at_ms: integer(),     # when this entry was cached (for TTL)
  lfu_counter: integer()       # local LFU for per-connection eviction
}
```

### Size limit

- Maximum 64 entries per connection (configurable via `l1-cache-size`).
- When full, evict the entry with the lowest `lfu_counter`.
- Total memory per connection: `64 * avg_value_size`. For 1 KB average values,
  this is ~64 KB per connection. At 10K connections: ~640 MB total L1 memory,
  which is significant -- the default should be conservative (64 entries), and
  L1 should be disabled by default in cluster mode where memory is tighter.

### Read path with L1

```elixir
defp get_with_l1(key, state) do
  now = System.os_time(:millisecond)

  case Map.get(state.l1_cache, key) do
    %{value: value, expire_at_ms: exp, cached_at_ms: cached_at} = entry
        when (exp == 0 or exp > now) and cached_at + l1_ttl_ms() > now ->
      # L1 hit: update local LFU counter, return value
      new_entry = %{entry | lfu_counter: entry.lfu_counter + 1}
      new_cache = Map.put(state.l1_cache, key, new_entry)
      {value, %{state | l1_cache: new_cache}}

    _miss_or_stale ->
      # L1 miss: fall through to Router.get/1 (L2/L3)
      value = Router.get(key)
      state = if value != nil, do: l1_insert(state, key, value, now), else: state
      {value, state}
  end
end
```

### Write path with L1 (write-through)

When the connection itself writes a key:

```elixir
defp put_with_l1(key, value, expire_at_ms, state) do
  result = Router.put(key, value, expire_at_ms)
  # Write-through: update our own L1 so subsequent reads see our write
  case result do
    :ok ->
      now = System.os_time(:millisecond)
      new_state = l1_insert(state, key, value, now, expire_at_ms)
      {:ok, new_state}
    error ->
      {error, state}
  end
end
```

### Invalidation strategies

#### Strategy 1: TTL-based (simplest)

L1 entries expire after a configurable TTL (default: 100ms).

```elixir
defp l1_valid?(entry, now) do
  (entry.expire_at_ms == 0 or entry.expire_at_ms > now) and
    entry.cached_at_ms + l1_ttl_ms() > now
end
```

**Properties:**
- Stale reads possible within the TTL window. A key modified by another
  connection at time T is visible to this connection at time T + 100ms.
- No cross-connection coordination needed.
- Best for: session lookups, config reads, rate limit checks where 100ms
  staleness is acceptable.

**Tuning:** Lower TTL = less staleness, more L2 fallback. At TTL=0, L1 is
effectively disabled (every read falls through). At TTL=1000ms, staleness
window is 1 second.

#### Strategy 2: CLIENT TRACKING integration

Leverage the existing `ClientTracking` module. When a key is modified:

1. `ClientTracking.notify_key_modified/3` sends a RESP3 push invalidation to
   all tracking connections.
2. The connection's `handle_info` for the invalidation message deletes the
   key from L1.

```elixir
# In connection handle_info for invalidation push
def handle_info({:invalidate, keys}, state) do
  new_cache = Enum.reduce(keys, state.l1_cache, fn key, cache ->
    Map.delete(cache, key)
  end)
  %{state | l1_cache: new_cache, l1_size: map_size(new_cache)}
end
```

**Properties:**
- Zero staleness (invalidation is immediate, subject to message delivery).
- Requires `CLIENT TRACKING ON` to be active.
- Slightly higher write-path cost: every write sends invalidation messages to
  all tracking connections.
- Best for: consistency-critical reads where staleness is unacceptable.

**Integration note:** `ClientTracking` already sends invalidation messages as
raw RESP3 bytes over the socket. For L1 invalidation we need an in-process
message, not a socket write. This requires a small extension: when a connection
has both L1 and TRACKING enabled, `notify_key_modified` should `send/2` a
`{:l1_invalidate, key}` message to the connection process in addition to (or
instead of) the socket push.

#### Strategy 3: Write-through (own writes only)

When THIS connection writes a key, immediately update its own L1. Other
connections' L1 entries for the same key are invalidated only by TTL (Strategy 1)
or TRACKING (Strategy 2).

This provides a **read-your-own-writes** guarantee: after `SET foo bar`, a
subsequent `GET foo` on the same connection always returns `bar`, even before the
TTL expires.

This strategy is always active and costs nothing extra. It is complementary
to Strategy 1 or Strategy 2.

### Recommended default configuration

| Scenario | L1 strategy | Rationale |
|----------|-------------|-----------|
| Standalone server | Strategy 1 (TTL 100ms) + Strategy 3 (write-through) | Low complexity, acceptable staleness |
| Consistency-critical app | Strategy 2 (TRACKING) + Strategy 3 | Zero staleness, higher write cost |
| Cluster mode (Raft) | L1 disabled by default | Raft replication adds latency that masks L1 benefit; staleness across nodes is harder to reason about |

### When L1 hurts performance

L1 is not free. It should be disabled (or kept very small) in these scenarios:

1. **Write-heavy workloads.** If a key is written more often than read, L1
   entries are constantly invalidated or expired, and the insert/evict overhead
   exceeds the lookup savings.

2. **High key cardinality per connection.** If each connection accesses unique
   keys (e.g., a proxy fanning out to random keys), the 64-entry L1 thrashes
   with zero reuse. The eviction overhead is pure waste.

3. **Large values.** L1 stores values in the connection process heap. A 1 MB
   value cached in L1 adds 1 MB to the process heap, increasing GC pause times.
   Consider skipping L1 for values larger than a threshold (e.g., 4 KB).

4. **Many connections.** At 10K connections with 64 entries of 1 KB each, L1
   consumes ~640 MB of BEAM heap memory outside MemoryGuard's accounting. This
   memory is invisible to the eviction system. A global L1 memory budget
   (tracked via `:atomics`) should cap total L1 usage.

### Config parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `l1-cache-enabled` | `yes` (standalone), `no` (cluster) | Enable/disable L1 per-connection cache |
| `l1-cache-size` | `64` | Maximum entries per connection |
| `l1-cache-ttl-ms` | `100` | TTL for L1 entries in milliseconds |
| `l1-cache-max-value-size` | `4096` | Skip L1 for values larger than this (bytes) |

---

## 4. Combined Performance Impact

| Optimization | Read latency impact | Memory overhead | Implementation complexity |
|---|---|---|---|
| LFU eviction | No direct latency change; higher cache hit rate means fewer Bitcask reads | +16 bytes/key in keydir | Medium -- touches keydir tuple format across 6 modules |
| Keydir hot flag (Option B) | -200ns for cold key reads (skip hot_cache miss) | +8 bytes/key in keydir | Low -- additive field, no structural change |
| L1 process-local cache | -395ns on L1 hit (5ns vs 400ns for two ETS lookups) | +64 * avg_value_size per connection | Medium -- new code in Connection, invalidation plumbing |
| **All three combined** | **Up to 80x faster for repeated reads on same connection** (5ns vs 400ns) with better eviction decisions | +24 bytes/key in keydir + per-connection L1 | Medium overall |

### Latency waterfall comparison

| Path | Current | With all optimizations |
|------|---------|----------------------|
| L1 hit (same connection, same key) | N/A | ~5ns |
| L2 hit (ETS hot key) | ~400ns (two lookups) | ~200ns (one lookup + hot flag skip) |
| L2 miss, cold key in keydir | ~400ns + GenServer call | ~200ns + GenServer call |
| L3 miss (Bitcask read) | ~50-200us | ~50-200us (unchanged) |

---

## 5. User Stories

### US-1: Session service (L1 + LFU)

A web app reads `session:abc` on every HTTP request over the same persistent
connection. Current behavior: each GET costs ~400ns (two ETS lookups). With L1:
the first GET costs ~400ns (L1 miss, warms L1), subsequent GETs within the 100ms
TTL cost ~5ns. At 10K req/sec on one connection, this saves ~4ms/sec of CPU time.

Meanwhile, a background SCAN job iterates all keys for analytics. With LRU, the
scan resets every key's access time, potentially evicting `session:abc` from
hot_cache. With LFU, `session:abc` has a high frequency counter (it is read on
every request) and survives the scan without eviction. No cold-start penalty
after the scan completes.

### US-2: Rate limiter under scan pressure (LFU)

A rate limiter checks `ratelimit:ip:1.2.3.4` on every request. This key is
accessed 50K times/sec. With LRU, a concurrent `KEYS *` (which touches every
key once) can cause the rate limit key to be evicted, leading to a cold Bitcask
read (~100us) on the next request -- a 250x latency spike. With LFU, the rate
limit key's counter is ~25 (very high), and a single touch from KEYS barely
changes it. The key is never evicted.

### US-3: Config cache (L1 + TTL)

An app reads `config:feature_flags` on every request for feature flag evaluation.
The value changes rarely (once per deploy). With L1 + 100ms TTL, a config change
takes at most 100ms to propagate to all connections. This is acceptable for
feature flags. Without L1, each read costs ~400ns. With L1, reads after the
first cost ~5ns -- a 80x improvement for a value that almost never changes.

### US-4: Pub/Sub fan-out with CLIENT TRACKING (L1 + Strategy 2)

A chat application has 5K connections, each caching `user:N:status` in L1.
When a user changes status, the write triggers `ClientTracking.notify_key_modified`,
which sends L1 invalidation messages to all connections tracking that key. Each
connection clears its L1 entry. The next read falls through to L2 (ETS) and
re-warms L1. Total staleness: zero (invalidation is push-based). Without L1,
reads are always ~400ns. With L1, steady-state reads are ~5ns with instant
invalidation on change.

---

## 6. Implementation Priority

| Priority | Optimization | Rationale |
|----------|-------------|-----------|
| **1** | **LFU with decay** | Biggest overall impact. Fixes a correctness problem (scan poisoning eviction) and improves cache hit rate for all workloads. No new infrastructure needed -- modifies existing code paths. |
| **2** | **L1 process-local cache** | Biggest latency win for repeated-read workloads. Requires new code in Connection but is self-contained -- does not affect the shard or Bitcask layers. |
| **3** | **Keydir hot flag** | Small optimization. The -200ns saving for cold keys is marginal compared to the GenServer call that follows. Can be deferred or bundled with the LFU keydir tuple change. |

### Dependencies

- LFU changes the keydir tuple format. If implemented first, the hot flag
  (priority 3) can be added to the same tuple change for free.
- L1 is independent of LFU and the keydir change. It can be implemented in
  parallel.
- L1 + CLIENT TRACKING integration (Strategy 2) depends on the existing
  `ClientTracking` module, which is already implemented. The extension needed
  (in-process invalidation message) is ~20 lines of code.

### Risk assessment

| Optimization | Risk | Mitigation |
|---|---|---|
| LFU | Medium -- keydir tuple format change affects 6+ modules | Feature-flag the new format; support both 2-tuple and 4-tuple during migration |
| L1 cache | Low -- isolated to Connection process, no shared state mutation | Disable by default in cluster mode; add `l1-cache-enabled no` escape hatch |
| Keydir hot flag | Low -- additive change, backward compatible | Ship with LFU tuple change |
