# FerricStore — Time Series Columnar Storage

---

## 1. Motivation

FerricStore's base time series design stores each sample as an independent Bitcask entry: one key (8-byte timestamp), one value (8-byte float + labels). This is correct and fast for writes, but leaves compression on the table for historical data.

TimescaleDB achieves 90%+ compression on cold data via columnar encoding: timestamps stored together (compressed), values stored together (compressed). Without an equivalent, FerricStore TS uses significantly more disk for any series with history beyond a few hours.

The solution is **columnar files with LMDB indexing**: each series gets a single append-only columnar file, with an LMDB B-tree holding partition metadata. Day partitions are sealed and compressed automatically — no operator intervention, no change to the write path.

| Metric | Unencoded Bitcask | Columnar + ALP | Gain |
|---|---|---|---|
| Storage per 86,400 samples (1/sec day) | ~2.76 MB | ~28 KB | ~100x compression |
| Range scan (full day) | 86,400 pread() calls | 1 pread() call | 86,400x fewer syscalls |
| Cold read latency (1 day) | ~4.3s (86,400 × 50μs) | ~200μs | ~21,500x faster |
| Write latency | ~3μs async file append | Same (sealing is async) | No change |

> **Note:** Compression ratios assume typical event count data — smooth monotonic values stored as float64. ALP maps these to integers and bit-packs them. Highly random values compress less but ALP degrades gracefully.

---

## 2. Storage Architecture

### 2.1 The Two-Layer Model

The storage model has two parts:

- **Columnar file per series** — all samples for one series in one append-only file
- **LMDB** — two databases in one mmap'd env: partition index (per-interval metadata) and series registry (per-series config). Durable, instantly queryable at startup with no rebuild.

```
data/
  ts/
    ts_click.col           ← all 90 days in one file
    ts_temperature.col
    ...
  meta/
    ts_index.lmdb          ← partition index + series registry in one file
```

Cross-series queries go from millions of file ops (old chunks) to one per series, preceded by a fast LMDB range scan that resolves byte offsets without touching any columnar file.

### 2.2 LMDB Partition Index

LMDB is a memory-mapped B-tree key-value store — embedded, no server process, ACID, and as fast as RAM once the OS pages are warm. The same file FerricStore uses for Raft metadata. Used inside OpenLDAP, Tor, and Meilisearch for exactly this pattern.

LMDB holds two databases in the same env (same mmap'd file, one `msync`):

- **`partitions`** — per-interval partition metadata (byte offsets, min/max, codec, sealed flag)
- **`series`** — per-series config (name, codec, retention, grace, labels)

No Bitcask involvement in TS at all. Bitcask remains FerricStore's KV storage engine — TS is a separate subsystem with its own storage.

#### Partition Index — Key and Value Layout

```rust
// Key: 12 bytes, fixed-width, sortable
// Sorts by series then chronologically within series
struct PartitionKey {
    series_hash:  u64,  // xxHash64 of series name
    partition_id: u32,  // = timestamp_ms / partition_interval_ms
}

// Value: 64 bytes, fixed-width
struct PartitionEntry {
    ts_offset:  u64,  // byte offset of timestamp section in .col file
    val_offset: u64,  // byte offset of value section in .col file
    row_count:  u32,  // number of samples in this partition
    ts_min:     i64,  // earliest timestamp ms — for time pruning
    ts_max:     i64,  // latest timestamp ms
    val_min:    f64,  // minimum value — for value pruning
    val_max:    f64,  // maximum value
    codec:      u8,   // 0=raw 1=alp 2=neats (v2)
    sealed:     u8,   // 0=open 1=sealed
    _pad:       [u8;6],
}
```

Fixed-width key and value means LMDB uses direct byte comparators — no parsing, no allocation. The sort order (series_hash + partition_id) puts all partitions for one series physically adjacent in the B-tree, so a range scan for one series is a sequential read. Mixed partition intervals within one series are supported — each entry's `ts_min`/`ts_max` defines its exact bounds.

**Rust crate:** `heed` v0.20 — MIT licensed, used by Meilisearch.

```toml
[dependencies]
heed = "0.20"
```

```rust
let env = EnvOpenOptions::new()
    .map_size(10 * 1024 * 1024 * 1024)  // 10GB virtual — not physical RAM
    .max_dbs(2)
    .open("meta/ts_index.lmdb")?;

let partitions: Database<OwnedType<[u8;12]>, OwnedType<[u8;64]>> =
    env.create_database(Some("partitions"))?;

let series: Database<OwnedType<[u8;8]>, OwnedType<[u8;256]>> =
    env.create_database(Some("series"))?;
```

The `map_size` is virtual address space, not RAM. Actual disk usage depends on partition interval — more partitions = more LMDB entries, but still small. At 1-hour intervals: 10K series × 2,160 partitions (90d) × 76 bytes + 10K series entries × 264 bytes = ~1.6GB. At 1-day intervals: ~71MB. LMDB pages in only what queries touch.

#### Series Registry — Key and Value Layout

```rust
// Key: 8 bytes (xxHash64 of series name)
type SeriesKey = [u8; 8];

// Value: up to 256 bytes, fixed-width
struct SeriesMeta {
    name_len:     u16,
    name:         [u8; 128],  // series name string
    codec:        u8,         // 0=raw 1=alp 2=neats (v2)
    retention_ms: u64,
    grace_ms:     u64,
    labels_len:   u16,
    labels:       [u8; 96],   // packed label key-value pairs
    created_at:   u64,
    _pad:         [u8; 5],
}
```

**TS.CREATE writes one entry:**

```rust
fn ts_create_series(name: &str, meta: SeriesMeta) {
    let mut txn = env.begin_rw_txn()?;
    let key = xxhash64(name).to_be_bytes();
    txn.put(series_db, &key, &meta.as_bytes(), WriteFlags::empty())?;
    txn.commit()?;
}
```

**Startup — populate ETS from LMDB:**

```rust
// Cursor scan all series — one sequential B-tree traversal
fn ts_load_all_series() -> Vec<SeriesMeta> {
    let txn = env.begin_ro_txn()?;
    let mut cursor = series_db.open_ro_cursor(&txn)?;
    cursor.iter()
        .map(|(_, v)| SeriesMeta::from_bytes(v))
        .collect()
}
```

```elixir
# On startup: LMDB → ETS, ~5ms for 10K series
series_list = Nif.ts_load_all_series()
Enum.each(series_list, fn meta ->
  ETS.insert(:ts_series_meta, {meta.name, meta})
  Enum.each(meta.labels, fn {k, v} ->
    ETS.insert(:ts_label_index, {meta.name, k, v})
  end)
end)
```

**Write path — one LMDB write per partition seal:**

```rust
fn ts_upsert_partition(series_hash: u64, partition_id: u32, entry: PartitionEntry) {
    let mut txn = env.begin_rw_txn()?;
    let key = encode_key(series_hash, partition_id);
    txn.put(db, &key, &entry.as_bytes(), WriteFlags::empty())?;
    txn.commit()?;  // single msync — fast, durable
}
```

**Read path — range scan for a query:**

```rust
// "Give me all partitions for ts:click in the last 7 days"
let series_hash = xxhash64("ts:click");
let from_ts     = now_ms - 7 * 86_400_000;
let from_id     = (from_ts / partition_interval_ms) as u32;
let to_id       = (now_ms / partition_interval_ms) as u32;

let txn      = env.begin_ro_txn()?;
let mut cursor = partitions_db.open_ro_cursor(&txn)?;

let entries: Vec<PartitionEntry> = cursor
    .iter_from(encode_key(series_hash, from_id))
    .take_while(|(k, _)| k <= encode_key(series_hash, to_id))
    .map(|(_, v)| PartitionEntry::from_bytes(v))
    .collect();
```

Sequential B-tree traversal. Number of entries depends on partition interval — auto-tuned per series.

**Startup behaviour:**

```
FerricStore starts:
  1. Open LMDB env → mmap the file                   (~1ms)
     All partition metadata is immediately addressable
  2. Cursor scan series_registry → populate ETS       (~5ms for 10K series)
     :ts_series_meta and :ts_label_index filled
  3. Done. No Bitcask keydir rebuild. No file header reads.
```

### 2.3 Auto-Tuned Partition Interval

Partition interval is auto-tuned per series based on observed sample rate. The user does not configure it.

**After the first partition seals, the system measures and adjusts:**

```elixir
defp auto_partition_interval(samples_per_second) do
  cond do
    samples_per_second > 100   -> 300_000      # >100/s  → 5 min
    samples_per_second > 10    -> 3_600_000    # >10/s   → 1 hour
    samples_per_second > 0.1   -> 86_400_000   # >0.1/s  → 1 day
    true                       -> 604_800_000  # sparse  → 1 week
  end
end
```

The first partition always uses 1-day default. After it seals, the system counts `row_count / partition_duration_seconds` and picks the right interval for future partitions. The interval is stored in the LMDB `SeriesMeta` and re-evaluated on each seal.

**Why auto-tuning matters for queries:**

```
"Last 5 minutes" query at 30K samples/sec:

Day partitions:    read entire day (~GB compressed), decode 2.6B samples, discard 99.7%
5-min partitions:  read 1 partition (~14KB), decode 9M samples, use all of it

Auto-tuning picks 5-min for 30K/s → precise reads, no waste.
```

Smaller partitions also give finer-grained LMDB pruning — each partition has a tighter `val_min`/`val_max` range, so `WHERE value > X` skips more partitions.

**Mixed intervals within one series are supported.** If the sample rate changes (e.g., traffic spike), the interval adjusts. Historical partitions keep their original interval. The LMDB scan uses `ts_min`/`ts_max` from each `PartitionEntry` to resolve exact bounds — the query path doesn't assume uniform interval.

| Sample rate | Interval | Partitions per 90d | LMDB entries | Seal size |
|---|---|---|---|---|
| 30K/s | 5 min | 25,920 | 25,920 × 76B = 2MB | ~14KB compressed |
| 100/s | 1 hour | 2,160 | 2,160 × 76B = 164KB | ~14KB compressed |
| 1/s | 1 day | 90 | 90 × 76B = 7KB | ~28KB compressed |
| 0.01/s | 1 week | 13 | 13 × 76B = 1KB | ~1KB compressed |

### 2.4 Columnar File Layout

The partition index lives entirely in LMDB. The columnar file is pure data:

```
ts_click.col

┌─────────────────────────────────────────────────────────┐
│  FILE HEADER (32 bytes)                                  │
│  magic=0x46455254, version=1, series_hash, sample_count  │
├─────────────────────────────────────────────────────────┤
│  TIMESTAMP COLUMN                                        │
│  All N timestamps, contiguous, growing at tail           │
│  Open partition: raw 8-byte big-endian int64             │
│  Sealed partition: ALP encoded in-place                  │
├─────────────────────────────────────────────────────────┤
│  VALUE COLUMN                                            │
│  All N values, contiguous, growing at tail               │
│  Open partition: raw 8-byte IEEE 754 float64             │
│  Sealed partition: ALP encoded in-place                  │
├─────────────────────────────────────────────────────────┤
│  LABELS SECTION                                          │
│  Deduplicated string table + per-sample index            │
└─────────────────────────────────────────────────────────┘
```

New samples append to the open partition tail. The file only grows forward.

### 2.5 Full Storage Architecture Diagram

```
ETS (volatile, populated from LMDB on startup):
  :ts_series_meta    — series_name → file path, codec, retention   (hot-path cache)
  :ts_label_index    — series_name → label set                     (MRANGE filter)
  :ts_warm_cache     — {series, day} → decoded samples             (decode cache, opt-in)

LMDB  (meta/ts_index.lmdb):
  series db:     series_hash → SeriesMeta                (source of truth for series config)
  partitions db: (series_hash, day) → PartitionEntry     (source of truth for partition metadata)

Columnar files  (data/ts/*.col):
  All sample data — timestamp + value columns            (grows at tail, append-only)
```

Two layers. ETS is a volatile cache populated from LMDB on startup (~5ms). LMDB is the durable source of truth for all metadata. Columnar files hold raw sample data.

---

## 3. Partition Lifecycle

### 3.1 Partition States

Every partition in a series passes through four states:

| State | Storage | Reads | Writes | Transition |
|---|---|---|---|---|
| **Open** | Raw 8-byte values appended to columnar file tail | pread from file (raw, no decode needed) | Async file append | Day boundary passes |
| **Sealing** | Raw bytes being compressed in-place (read-only window) | Raw bytes (compression in progress) | Rejected — partition is closed | Compression completes |
| **Sealed** | ALP-compressed in columnar file | io_uring pread + SIMD decode | Not applicable — immutable | Retention policy expires it |
| **Expired** | Hole-punched (sparse) | N/A — removed from LMDB index | N/A | — |

### 3.2 Sealing Trigger

The `PartitionSealer` GenServer monitors active series. A partition is sealed when both conditions are true:

- The partition's time interval has elapsed (e.g., the 5-minute or 1-hour window has closed)
- A configurable grace period has elapsed (default: 60 seconds) to allow late-arriving samples

The grace period matters. Event pipelines have tail latency — a sample timestamped at the partition boundary may arrive seconds later due to network or processing delay. Sealing immediately would discard it. With a 60-second grace, it lands in the correct partition before sealing.

> **Note:** Grace period is configurable per series via `TS.CREATE ... CHUNK_GRACE 30000` (milliseconds). High-frequency IoT series can use 5 seconds. Event pipelines with async flush jobs should use 120 seconds.

### 3.3 What Happens During Sealing

When a partition interval elapses (+ grace period), the `PartitionSealer` GenServer:

1. Read the open partition's raw bytes from the columnar file
2. ALP-encode the timestamp column in a Tokio task
3. ALP-encode the value column in a parallel Tokio task
4. Compute `val_min`, `val_max` over the value column (one pass, free — already in memory for encoding)
5. Write encoded bytes back in-place (always smaller — safe)
6. **Write LMDB partition entry** with `ts_min`, `ts_max`, `val_min`, `val_max`, offsets, `sealed=true` — this is the first and only LMDB write for this partition (no writes during the open phase)
7. Update LMDB series entry's `sample_count` field (same transaction)

### 3.4 Idempotency and Crash Safety

The seal operation is **idempotent**. If the process crashes mid-seal, the raw bytes remain intact in the columnar file. On restart, the `PartitionSealer` re-detects the unsealed partition (LMDB entry has `sealed=0` or is missing) and re-seals it.

The LMDB write in step 6 is the commit point. If it succeeds, the partition is sealed. If it fails, the raw bytes are still valid and the partition can be re-sealed.

---

## 4. Read & Write Paths

### 4.1 Write Path

```
Client: TS.ADD ts:click 1700000001000 42

1. Async columnar file append              (~3μs, non-blocking)
2. Done.
```

One syscall. No LMDB write, no ETS write.

LMDB is single-writer — one write transaction at a time across the entire env. Updating partition metadata on every `TS.ADD` would cap total throughput at ~200K samples/sec across all series (LMDB commit = `msync` ≈ 5μs). Instead, LMDB is only written at **seal time** — one transaction per partition seal.

For the open (current) partition, `row_count` and `ts_max` are derived from the file at query time:

```rust
// No LMDB needed for the open partition:
row_count = (file_size - header_size - sealed_bytes) / 16  // 16 bytes per sample
ts_max    = pread(fd, file_size - 16, 8)                   // last timestamp in file
```

This eliminates the LMDB bottleneck entirely. Per-series write throughput is bounded only by file append syscall latency (~333K samples/sec per series). Total throughput is bounded by NVMe bandwidth — at 16 bytes/sample and 5GB/s, that's ~312M samples/sec theoretical.

Writes never touch sealed partitions. New samples always append to the open partition tail.

### 4.2 Two-Level Pruning

Before any columnar file is touched, LMDB eliminates irrelevant partitions:

```
TS.FILTER ts:temperature FROM -90d TO now WHERE value > 37.5

Level 1 — LMDB time pruning (in memory, ~1μs per series):
  Scan LMDB entries for ts:temperature
  ts_max < query_from → skip (partition predates the query window)
  → Prunes irrelevant partitions

Level 2 — LMDB value pruning (same LMDB scan, zero extra cost):
  val_max < 37.5 → skip (no sample in this partition exceeds threshold)
  → Prunes days where temperature was always below 37.5

io_uring submission:
  Only the byte ranges for partitions that passed both checks
  Potentially 5–10% of total file bytes for selective queries
```

Both levels of pruning happen inside the LMDB B-tree scan — one pass, no disk reads, before any columnar file is opened.

### 4.3 Single-Series Range Query

```
TS.RANGE ts:click FROM -7d TO now

1. ETS lookup :ts_series_meta "ts:click" → file path             (~1μs)
2. LMDB range scan: series_hash + day_range → 7 PartitionEntry   (~5μs)
3. io_uring: submit 7 byte ranges in one batch                    (~100μs + I/O)
4. Tokio: ALP decode + filter to exact timestamp range
5. Return results
```

### 4.4 Cross-Series MRANGE

```
TS.MRANGE FROM -30d TO now FILTER event_type=click WHERE value > 1000

1. ETS label lookup → 10K matching series                        (~1ms)
2. LMDB scan per series:
     For each series: scan partition entries in time range
     val_max < 1000 → skip partition (value pruning in LMDB)
     Remaining: maybe 3K series × ~10 days = 30K byte ranges    (~50ms)
3. io_uring: 30K reads submitted in one batch                    (one ring write)
4. NVMe serves in parallel
5. Tokio: 30K parallel ALP decode + AGG tasks
6. Coordinator merges results
```

The `WHERE value > 1000` prunes entire partitions in LMDB before any columnar file read. A series where click counts are always < 1000 never touches disk at all. Smaller auto-tuned intervals give tighter `val_min`/`val_max` ranges per partition — more effective pruning.

> **Note:** io_uring ring size is 4096 SQEs. For queries exceeding this (>4096 byte ranges), batch in chunks of ~3000 SQEs with completion draining between batches.

### 4.5 Query Latency Table

With io_uring batch submission, all reads are submitted in one ring write and served by NVMe in parallel.

**Example: 30 partitions (30 × 28KB = 840KB total):**

```
NVMe sequential read at 5GB/s:   ~168μs
io_uring ring submission:         ~1–2μs (one syscall, not 30)
In-memory decode (30 partitions): ~60–100μs (ALP SIMD decode)

Total:                            ~230–270μs
```

Without io_uring (plain sequential `pread()` loop) each syscall round-trip costs ~500μs, so 30 × 500μs = 15–20ms. io_uring batch submission is what makes the difference.

| Query | Latency (io_uring) | Bottleneck |
|---|---|---|
| Last 5 min (open partition) | `~50–100μs` | 1 pread — raw bytes, no decode needed |
| Last 24 hours | `~200–300μs` | 1 open pread + 1 sealed pread + decode |
| Yesterday's hourly totals (24 partitions) | `~120–180μs` | 24 parallel preads + decode |
| 30 days of daily totals | `~230–270μs` | 30 parallel preads + decode |
| 7 days raw (7 partitions) | `~500μs–1ms` | Decode is now the bottleneck |

> **Note:** For large queries (7+ days raw), in-memory decode dominates over I/O. 7 partitions × 86,400 samples = ~605K ALP decode operations. This is parallelisable across Tokio tasks by splitting the partition list.

---

## 5. Memory Tiering

### 5.1 Three-Tier Model

```
┌─────────────────────────────────────────────────────────┐
│  Tier 0 — ETS warm cache                  ~10–50μs reads│
│  Recently-accessed sealed partitions, decoded in ETS    │
│  Controlled by: WARM_CACHE_CHUNKS per series            │
│  Eviction: LRU, evicts decoded partitions when limit hit│
│  Default: 0 (disabled — opt-in per series)              │
├─────────────────────────────────────────────────────────┤
│  Tier 1 — NVMe columnar files             ~50–500μs     │
│  All data: open partitions (raw) + sealed (ALP-encoded) │
│  Controlled by: RETENTION per series                    │
│  Read path: io_uring batched pread + SIMD decode        │
├─────────────────────────────────────────────────────────┤
│  Tier 2 — Expired (deleted)                             │
│  Beyond RETENTION window                                │
│  Managed by: RetentionSweeper GenServer                 │
└─────────────────────────────────────────────────────────┘
```

Open partitions (current day) are raw 8-byte values — a `pread()` returns usable data with no decode step. Sealed partitions require ALP SIMD decode. The warm cache eliminates repeated decode of the same sealed partition.

### 5.2 TS.CREATE Parameters

```
TS.CREATE key
  [RETENTION ms]              existing — total history to keep
  [WARM_CACHE_CHUNKS n]       new — how many recently-accessed sealed partitions
                                    stay decoded in ETS after first read
                                    Default: 0 (opt-in per series)
  [CHUNK_GRACE ms]            existing — late-sample grace period before sealing
```

**Examples:**

```
# Dashboard series — query last 24h constantly, history up to 90 days
TS.CREATE ts:click RETENTION 7776000000 WARM_CACHE_CHUNKS 24

# IoT sensor — keep 1 year history, rarely queried
TS.CREATE ts:temperature RETENTION 31536000000 WARM_CACHE_CHUNKS 0

# Billing metric — keep 30 days, entire history accessed regularly
TS.CREATE ts:revenue RETENTION 2592000000 WARM_CACHE_CHUNKS 720
```

### 5.3 Global Config

```elixir
# ferricstore.conf

# Hard RAM ceiling for all TS ETS warm caches combined.
# When hit, LRU eviction across all series.
ts.warm_cache_max_mb = 1024

# io_uring queue depth for sealed partition reads.
# Higher = more parallel reads per batch, diminishing returns above 64.
ts.io_uring_queue_depth = 32
```

### 5.4 How Warm Cache Works

The warm cache is an ETS table keyed by `{series_name, day}` holding the fully decoded sample list from a sealed partition. It is populated lazily on first read — no proactive warming.

**Read path for sealed partitions:**

```
TS.RANGE ts:click FROM -30d TO now

1. LMDB scan → 30 PartitionEntry

2. For each partition, check ETS warm cache:
   HIT  → return decoded samples directly           (~10μs)
   MISS → add to io_uring batch

3. io_uring pread all misses → ALP SIMD decode

4. Store decoded results in warm cache:
   Cache at limit? → LRU evict oldest entries first
                     ETS.delete frees memory immediately
                   → Insert new entries

5. Return all results (hits + freshly decoded)
```

**Warm cache entry lifecycle:**
- Inserted on first disk read of a sealed partition
- Evicted LRU when `ts.warm_cache_max_mb` is exceeded
- Open partitions are NEVER cached (they're still being written to)

**Eviction under memory pressure:**

The query always succeeds. When the cache is full and new decoded partitions need to be cached, LRU entries are evicted first. `ETS.delete` frees memory immediately — the BEAM's ETS allocator owns it, no OS-level leak.

```
Cache at 1024MB limit, query needs 6 new partitions:

  ETS.delete({ts:temperature, day_old_1})    ← freed, ~28KB reclaimed
  ETS.delete({ts:temperature, day_old_2})    ← freed, ~28KB reclaimed
  ...enough space...
  ETS.insert({ts:click, day_new_1}, decoded) ← cached for next query
  ...
```

**Transient memory during large queries:**

Decoded partitions exist briefly in the Rust Tokio task and the calling Elixir process before entering ETS. This transient memory is outside the warm cache budget. To bound it, `ts_scan_range` decodes in batches of `ts.io_uring_queue_depth` (default 32) partitions:

```
TS.MRANGE across 10K series × 30 days = 300K partitions

Without batching: 300K × 28KB = 8.4GB transient  ← bad

With batching (queue_depth=32):
  for each batch of 32 partitions:
    io_uring pread 32 → decode → send to Elixir → Rust buffer freed
    Peak transient: 32 × 28KB = 896KB             ← bounded

  Elixir side aggregates incrementally per batch
  Never holds all raw samples at once
```

The warm cache limit controls **long-lived** cached data. Transient decode memory is bounded by `ts.io_uring_queue_depth` — a separate knob.

The warm cache gives operators a knob that says: "keep frequently-queried sealed partitions decoded in RAM so they serve at ETS speed, not disk speed." The RAM cost is ~28KB × 24 = 672KB per series. For 1000 series that's 672MB — a fraction of what RedisTimeSeries would need to keep the same history in RAM.

### 5.5 RAM Cost Comparison

Scenario: 1000 series, 30-day retention, querying last 24 hours frequently.

| System | Data in RAM | History storage | Total RAM for TS | 30-day disk cost |
|---|---|---|---|---|
| RedisTimeSeries | All 30 days (required) | RAM only | ~450GB | $0 |
| FerricStore TS (no warm cache) | Metadata only | NVMe columnar files | ~100MB | ~840MB NVMe |
| FerricStore TS (24h warm cache) | 24 decoded sealed partitions/series | NVMe for older history | ~700MB | ~840MB NVMe |
| FerricStore TS (max warm) | Full 30d warm decoded | NVMe (backup only) | ~450GB | ~840MB NVMe |

The sweet spot is the third row: 24h warm cache gives near-RAM performance for the most commonly queried window at 1/640th the RAM cost. Without warm cache, FerricStore uses only ~100MB of RAM for 1000 series — just metadata. All reads go through io_uring at ~200–500μs.

> **Note:** `WARM_CACHE_CHUNKS 0` is the default — no warm cache, all cold reads go straight to io_uring. Useful for write-heavy series where history is rarely queried (IoT sensors, log aggregation). Opt-in per series.

### 5.6 Prefetch Optimisation

For `TS.RANGE` queries spanning multiple partitions, the NIF overlaps warm cache lookups with disk I/O:

```rust
// ts_scan_range with prefetch:
// 1. Check warm cache for all partitions in the query range
// 2. Submit io_uring reads for all misses immediately (non-blocking)
// 3. While reads are in flight, collect warm cache hits
// 4. When reads complete, decode, store in warm cache, and merge
// Net effect: warm cache serving hides disk latency for misses
```

---

## 6. Compression

### 6.1 ALP — Single Codec for Both Timestamps and Values

**Paper:** Afroozeh, Kuffó, Boncz — CWI Amsterdam (SIGMOD 2024). MIT licensed. Already adopted in DuckDB.

ALP (Adaptive Lossless floating-Point compression) is the single codec for v1. It handles both timestamps and values with one implementation, one SIMD decode path, and one set of invariants.

**The insight:** Most real-world floats are either decimal numbers (42.0, 37.5, 100.0) or high-precision scientific values. ALP handles each case separately:

**Case 1 — Integers and decimal floats (event counts, metrics, timestamps):**

```
42.0 → multiply by 10^e (find e that makes all values integer)
     → 420 (integer)
     → Frame-of-Reference: subtract base value from all integers
     → FastLanes bit-pack: pack residuals into minimum bits
     → SIMD-native: 8 values per AVX2 instruction

Example — 1-second event timestamps (ms):
  [1700000001000, 1700000002000, 1700000003000, ...]
  Already integers → FoR base = 1700000001000
  Residuals: [0, 1000, 2000, 3000, ...]
  After second FoR (delta): [0, 1000, 1000, 1000, ...]
  All residuals = 1000 → bit-pack to ~10 bits per sample

Example — event counts stored as float64:
  [1.0, 5.0, 42.0, 1000.0, ...]
  multiply by 10^0 → integers [1, 5, 42, 1000]
  FoR base = 1, residuals fit in 10 bits
  ~1.25 bytes per sample vs 8 bytes raw
```

**Case 2 — High-precision floats (sensor readings with many decimal places):**

```
  Split 64-bit float into left bits (exponent + high mantissa)
                          + right bits (low mantissa)
  Left:  dictionary compress (few unique patterns in real data)
  Right: bit-pack to minimum necessary bits

Example — temperature sensor:
  [37.51234, 37.51289, 37.51301, ...]
  Left bits share exponent → dictionary of ~3 patterns
  Right bits: 8-bit residuals after dictionary lookup
  ~2 bytes per sample vs 8 bytes raw
```

**Why ALP for timestamps too (not delta-delta):**

Timestamps are monotonic integers — the ideal input for ALP Case 1. Frame-of-Reference on regular timestamps produces residuals that are all identical (e.g., all 1000 for 1-second intervals), which bit-pack to near-zero bits per sample. This is equivalent compression to delta-delta encoding, but with SIMD-native encode and decode (8 values per AVX2 instruction) instead of sequential bit-twiddling.

One codec, one decode path, one SIMD implementation. No delta-delta. No Gorilla XOR.

**Implementation:** The C++ reference implementation is MIT-licensed (`github.com/cwida/ALP`). Reimplement the core in Rust (~500 lines) or wrap via FFI. ALP decode in Rust NIF (hot path, SIMD). ALP encode in Elixir (background seal, 200ms is fine).

### 6.2 Future Codecs (v2)

- **NeaTS** (ICDE 2025): Learned compression with O(log n) random access into compressed data. Fits the time series with nonlinear functions + small bounded residuals. Enables narrow range queries without decompressing entire partitions. Marked v2 — only relevant when narrow-range query optimization is needed.
- **Chimp** and **Sprintz**: Dropped. ALP dominates Chimp on compression ratio and decode speed. ALP handles timestamps natively, eliminating the need for Sprintz.

### 6.3 Codec Auto-Selection (v2)

When NeaTS is added, the `CodecProbe` module in Elixir probes at seal time:

```elixir
defmodule FerricStore.TS.CodecProbe do
  def pick(values) do
    smoothness = measure_smoothness(values)

    cond do
      smoothness < 0.3 -> :alp           # bursty data — NeaTS fit will be poor
      smoothness >= 0.3 ->
        case neats_fit_quality(Enum.take(values, 1000)) do
          ratio when ratio < 0.20 -> :neats_alp  # good fit
          _                       -> :alp          # poor fit
        end
    end
  end
end
```

Codec lock-in after 3 consistent partitions. Per-partition codec stored in LMDB `PartitionEntry.codec` field — mixed codecs within one series file are fully supported. The decode path reads the `codec` byte and uses the right decoder per partition.

Only relevant when NeaTS is implemented as an alternative. For v1, all partitions use ALP.

### 6.4 Labels Section Format

Labels are deduplicated using a string table to avoid repeating common label sets across samples:

- **String table:** length-prefixed list of unique label strings
- **Per-sample label index:** varint index into string table (1–2 bytes typical)
- **If all samples share the same labels** (common case): `flags` bit 0 = `1`, single label entry, no per-sample index

> **Note:** For event data with region/device labels, all samples in a partition typically share the same label set. The flags bit optimization reduces the labels section to a single string entry — effectively zero per-sample overhead.

---

## 7. Elixir / Rust Boundary

### 7.1 The Rule

**Rust if and only if:**
1. The operation requires SIMD (AVX2) — Elixir has no access to SIMD instructions
2. The operation requires io_uring — no Elixir interface exists
3. The operation is on the query hot path AND the user is waiting for it

**Elixir for everything else.**

If it runs in the background and the user never waits for it, it belongs in Elixir. If it runs on the query hot path and the user waits for it, it belongs in Rust.

**Why this matters:** Rust NIFs allocate memory outside the BEAM heap. The GC cannot see these allocations. If a NIF is used for background work, the developer must manually manage when memory is freed. Elixir processes allocate on the BEAM heap — when a process exits, everything is freed. A `Task` that seals one partition leaves zero memory behind.

### 7.2 NIF Consistency

All Rust NIFs follow the project's established patterns:

- **No dirty schedulers.** All NIFs use `schedule = "Normal"` with `consume_timeslice` + `enif_schedule_nif` for BEAM-guided yielding. For IO-bound work, Tokio async with `OwnedEnv::send_and_clear`.
- **Pure stateless functions.** No HashMap, no Mutex, no internal state. All inputs as arguments, all results returned. State lives in Elixir (ETS).
- **Existing Tokio pattern.** IO-bound NIFs spawn onto the shared Tokio runtime, send results back via `pid` + `OwnedEnv`.

### 7.3 Full Boundary Table

| Component | Language | Reason |
|---|---|---|
| `ts_scan_range` NIF | Rust | SIMD AVX2 decode + io_uring — user waits |
| `ts_simd_filter` NIF | Rust | SIMD value column scan — user waits |
| `ts_lmdb_scan` NIF | Rust | LMDB `heed` binding — thin wrapper |
| `ts_lmdb_upsert` NIF | Rust | LMDB `heed` binding — thin wrapper |
| `TS.Encoding.encode_values` | Elixir | Background seal — 200ms fine, clean GC |
| `TS.Encoding.encode_timestamps` | Elixir | Background seal — 200ms fine, clean GC |
| `TS.Encoding.neats_fit` (v2) | Elixir | Math + background — Nx or pure Elixir |
| `TS.CodecProbe.pick` (v2) | Elixir | 1ms math on 1000 samples — trivially Elixir |
| `TS.PartitionSealer` GenServer | Elixir | Orchestration — pure OTP |
| `TS.CompactionWorker` GenServer | Elixir | Orchestration — pure OTP |
| `TS.RetentionSweeper` GenServer | Elixir | Orchestration — pure OTP |
| `TS.Counter` (TS.INCR) | Elixir | ETS INCR — Elixir native |
| `TS.Pipeline` parser (v2) | Elixir | Keyword parsing — pure Elixir |
| All command dispatch | Elixir | RESP parsing already Elixir |
| File appends | Elixir | `File.write!` — fine for background |
| LMDB metadata logic | Elixir | Calls thin Rust NIFs, logic in Elixir |

### 7.4 Rust NIF Functions

| NIF | Why Rust | What it does |
|---|---|---|
| `ts_scan_range(fd, from_off, to_off, agg, bucket_ms)` | SIMD decode + io_uring | Seek, read, AVX2 decode, aggregate — user waits |
| `ts_simd_filter(bytes, op, threshold)` | SIMD value scan | TS.FILTER hot path — user waits |
| `ts_lmdb_scan(hash, from_day, to_day, val_min, val_max)` | LMDB heed binding | Range scan partition index |
| `ts_lmdb_upsert(hash, day, entry)` | LMDB heed binding | Write one partition entry |

```rust
fn ts_scan_range(env: Env, args: &[Term]) -> Result<Term, Error> {
    let pid       = args[0].decode::<Pid>()?;
    let fd        = args[1].decode::<RawFd>()?;
    let from_off  = args[2].decode::<u64>()?;
    let to_off    = args[3].decode::<u64>()?;
    let agg       = args[4].decode::<AggType>()?;
    let bucket_ms = args[5].decode::<u64>()?;

    tokio_rt().spawn(async move {
        let bytes  = pread_range(fd, from_off, to_off - from_off).await?;  // io_uring
        let values = alp_decode_simd(&bytes.value_section);                 // AVX2
        let ts     = alp_decode_simd(&bytes.ts_section);                    // AVX2
        let result = aggregate(ts, values, agg, bucket_ms);
        send_result(pid, result);
    });
    Ok(atoms::ok().encode(env))
}
```

### 7.5 Elixir Modules

The `TS.PartitionSealer` that orchestrates sealing:

```elixir
defmodule FerricStore.TS.PartitionSealer do
  use GenServer

  # Runs as a Task — spawns, works, exits. GC cleans up.
  def seal(series_name, partition_id) do
    Task.start(fn ->
      raw = File.read!(col_file_path(series_name))
      {timestamps, values} = parse_raw_partition(raw, partition_id)

      # Encoding — pure Elixir, background, slow is fine
      encoded_ts  = FerricStore.TS.Encoding.encode_timestamps(timestamps)
      encoded_val = FerricStore.TS.Encoding.encode_values(values)

      # val_min/val_max computed during encode — free, one pass
      {val_min, val_max} = FerricStore.TS.Encoding.minmax(values)

      # Write encoded bytes — Elixir File.write
      write_partition(series_name, day, encoded_ts, encoded_val)

      # LMDB update — thin Rust NIF, ~1μs
      Nif.ts_lmdb_upsert(
        series_hash(series_name), partition_id,
        build_entry(encoded_ts, encoded_val, timestamps, val_min, val_max, :alp)
      )
    end)
  end
end
```

### 7.6 The Nx Option for NeaTS (v2)

NeaTS function fitting (linear regression, polynomial fit) involves matrix operations on small arrays (~1000 samples). This is exactly what the Nx library handles — EXLA backend compiles these to native code and can use BLAS if available.

```elixir
defmodule FerricStore.TS.Encoding do
  import Nx.Defn

  defn linear_fit(xs, ys) do
    # Ordinary least squares — Nx compiles to native, BLAS-accelerated
    n    = Nx.size(xs)
    sx   = Nx.sum(xs)
    sy   = Nx.sum(ys)
    sxx  = Nx.sum(Nx.multiply(xs, xs))
    sxy  = Nx.sum(Nx.multiply(xs, ys))
    a    = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    b    = (sy - a * sx) / n
    {a, b}
  end
end
```

This stays in Elixir, runs fast enough for background work, and avoids any Rust allocation for the fitting step.

---

## 8. Supervision Tree

| Process | Restart | Role |
|---|---|---|
| `FerricStore.Supervisor` | `one_for_one` | Root supervisor |
| `FerricStore.TS.Registry` | `permanent` | ETS owner for series metadata and compaction rules |
| `FerricStore.TS.PartitionSealer` | `permanent` | Seals closed partitions on a tick schedule (every 10s) |
| `FerricStore.TS.RetentionSweeper` | `permanent` | Deletes partitions beyond retention window (every 1h) |
| `FerricStore.TS.CompactionWorker` | `transient` | Runs per-series compaction rules (hourly → daily rollups) |

**Telemetry Events:**

| Event | Measurements | Metadata |
|---|---|---|
| `[:ferricstore, :ts, :partition, :sealed]` | `bytes_before, bytes_after, duration_ms, sample_count` | `series, day, compression_ratio` |
| `[:ferricstore, :ts, :partition, :seal_failed]` | `duration_ms` | `series, day, reason` |
| `[:ferricstore, :ts, :retention, :dropped]` | `partitions_dropped, bytes_freed` | `series, cutoff_day` |
| `[:ferricstore, :ts, :reconstruction, :completed]` | `duration_ms, entries_replayed` | `series, trigger` |

---

## 9. Retention

### 9.1 The Problem

With a columnar file, old data is at the **start** of the file and new data is at the **tail**. You can't delete from the front of a file without either rewriting the entire file or using a different strategy.

```
ts_views_post_123.col

[2024-01-01 partition]  ← oldest, expired, want to delete
[2024-01-02 partition]  ← expired
[2024-01-03 partition]  ← expired
...
[2024-03-31 partition]  ← current, keep
```

**Chosen approach: Sparse file + hole punching.** Use OS-level hole punching to reclaim disk blocks for expired partitions. The file inode remains, the punched regions consume zero actual disk blocks, reads from punched regions return zeroes (safe, detectable). O(1) per partition, reclaims disk immediately, no rewriting of live data.

### 9.2 Hole Punching — Cross-Platform

**Linux: `fallocate(PUNCH_HOLE)`**

```rust
fn ts_punch_partition(fd: RawFd, offset: u64, len: u64) -> Result<(), Error> {
    // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
    // Reclaims disk blocks, preserves file size, returns zeros on read
    fallocate(fd, FallocFlags::PUNCH_HOLE | FallocFlags::KEEP_SIZE,
              offset as i64, len as i64)?;
    Ok(())
}
```

Supported on ext4, XFS, btrfs (kernel ≥ 2.6.38). On NVMe-backed filesystems this is a metadata-only operation — fast regardless of partition size.

**macOS: `fcntl(F_PUNCHHOLE)`**

```rust
#[cfg(target_os = "macos")]
fn ts_punch_partition(fd: RawFd, offset: u64, len: u64) -> Result<(), Error> {
    // macOS requires 4096-byte alignment for both offset and length
    let aligned_offset = offset & !4095;
    let aligned_len = ((offset + len + 4095) & !4095) - aligned_offset;

    let punch = fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: aligned_offset as i64,
        fp_length: aligned_len as i64,
    };
    fcntl(fd, F_PUNCHHOLE, &punch)?;
    Ok(())
}
```

Supported on APFS (macOS 10.13+). Requires 4096-byte alignment for offset and length.

**Fallback (HFS+ or unsupported filesystems):**

If hole punching is not available, fall back to:
1. Mark expired partitions as dead in LMDB (delete the entry)
2. Periodic compaction: when dead bytes exceed 50% of file size, rewrite the file excluding dead partitions

This fallback is slower but functionally correct. The LMDB deletion alone prevents expired data from appearing in queries; compaction reclaims disk space on a deferred schedule.

### 9.3 RetentionSweeper GenServer

```elixir
defmodule FerricStore.TS.RetentionSweeper do
  use GenServer

  # Ticks every 1 hour by default
  # Configurable: ts.retention_sweep_interval_ms

  def handle_info(:sweep, state) do
    now_ms   = System.system_time(:millisecond)

    # Get all series with a RETENTION policy from ETS
    series_list = ETS.select(:ts_series_meta, [{:_, :_, :_, :"$retention_ms", :_}])

    Enum.each(series_list, fn {name, file_path, retention_ms, _} ->
      cutoff_ms = now_ms - retention_ms

      # LMDB range scan: all partitions for this series where ts_max < cutoff
      expired = Nif.ts_lmdb_scan_before(series_hash(name), cutoff_ms)

      Enum.each(expired, fn entry ->
        # Punch holes in columnar file — Rust NIF, ~1μs per partition
        Nif.ts_punch_partition(
          file_fd(file_path),
          entry.ts_offset, entry.ts_len
        )
        Nif.ts_punch_partition(
          file_fd(file_path),
          entry.val_offset, entry.val_len
        )
        # Delete from LMDB partition index — now invisible to queries
        Nif.ts_lmdb_delete(series_hash(name), entry.partition_id)

        # Telemetry
        :telemetry.execute(
          [:ferricstore, :ts, :retention, :dropped],
          %{bytes_freed: entry.ts_len + entry.val_len},
          %{series: name, partition_id: entry.partition_id}
        )
      end)
    end)

    schedule_next_sweep(state)
    {:noreply, state}
  end
end
```

One LMDB scan + one hole punch per expired partition. No file rewriting. No read blackout. Safe to run while queries are in-flight.

### 9.4 Day-Level Granularity

Retention deletes entire partitions, not individual samples. Granularity matches the auto-tuned partition interval:

```
RETENTION 30d, partition interval = 1 hour:
  → delete any partition where ts_max < now - 30d
  → granularity: 1 hour (last expired partition may be up to 1h past the cutoff)

RETENTION 30d, partition interval = 1 day:
  → same logic, but granularity: 1 day
```

Partition-level retention is predictable and fast — one LMDB delete + one hole punch per expired partition. The auto-tuned interval ensures high-frequency series (5-min partitions) get fine-grained retention while low-frequency series (weekly partitions) have minimal overhead.

> **Note on low-frequency series:** A series with 1 sample per week still gets a full weekly partition. Overhead: one LMDB entry (76 bytes) + minimal data. For a 365-day retention with weekly partitions, that's 52 entries = ~4KB — negligible.

### 9.5 Per-Series Retention Policies

Different event types warrant different retention periods:

```
# Views — high volume, 90 days
TS.CREATE ts:views:post:* RETENTION 7776000000

# Likes — low volume, keep longer
TS.CREATE ts:likes:post:* RETENTION 31536000000

# Trending scores (computed) — keep short, recomputable
TS.CREATE ts:trending:* RETENTION 604800000
```

Retention is stored in the LMDB series entry and cached in ETS `:ts_series_meta`. The `RetentionSweeper` reads it from ETS — no disk access needed to decide what to sweep.

### 9.6 Changing Retention (TS.ALTER)

```
TS.ALTER key RETENTION new_retention_ms

# Extend retention — no immediate action, future sweeps use new value
TS.ALTER ts:views:post:* RETENTION 15552000000

# Shorten retention — triggers immediate sweep for the newly expired range
TS.ALTER ts:views:post:* RETENTION 2592000000
```

`TS.ALTER` updates the LMDB series entry and ETS metadata. If retention is shortened, the `RetentionSweeper` is signalled to run immediately for that series.

### 9.7 Rollup Retention

Keep raw data short, rollups longer:

```
# Raw series — 30 days
TS.CREATE ts:views:post:123 RETENTION 2592000000

# Hourly rollup — 1 year
TS.CREATE ts:views:post:123:per_hour RETENTION 31536000000

# Daily rollup — 5 years
TS.CREATE ts:views:post:123:per_day RETENTION 157680000000

# Compaction rules generate rollup series automatically
TS.CREATERULE ts:views:post:123 ts:views:post:123:per_hour AGG sum BUCKET 3600000
TS.CREATERULE ts:views:post:123:per_hour ts:views:post:123:per_day AGG sum BUCKET 86400000
```

Each series has its own RETENTION. Raw data expires at 30 days. Hourly summaries survive for a year. Daily summaries survive for 5 years.

### 9.8 Disk Cost With Retention

| Series type | Retention | Samples/day | Disk per series |
|---|---|---|---|
| `ts:views:post:*` | 90d | 86,400 | ~90 × 28KB = 2.5MB |
| `ts:likes:post:*` | 365d | ~100 (sparse) | ~365 × 0.8KB = 292KB |
| `ts:views:post:*:per_hour` | 365d | 24 | ~365 × 2KB = 730KB |
| `ts:views:post:*:per_day` | 1825d | 1 | ~1825 × 144B = 257KB |

For 1 million posts × all series types:

```
ts:views     (raw):        1M × 2.5MB  = 2.5TB
ts:likes:                  1M × 292KB  = 292GB
ts:views:per_hour:         1M × 730KB  = 730GB
ts:views:per_day:          1M × 257KB  = 257GB

Total:                                 ~3.8TB
```

On a 3-node Raft cluster: 3.8TB × 3 = ~11.4TB total. At ~$0.10/GB NVMe = ~$1,140/month.

Compare to RedisTimeSeries storing the same 90-day views history in RAM: 2.5TB × $5/GB = $12,500/month.

### 9.9 Retention vs Compaction

Retention and file compaction are different operations:

| | Retention | File Compaction |
|---|---|---|
| What it removes | Expired time series partitions | Dead space in `.col` files after many hole punches |
| When it runs | RetentionSweeper, hourly | When dead bytes exceed 50% of file size |
| Mechanism | LMDB delete + hole punch | Rewrite `.col` file excluding dead regions |
| User-visible | Data disappears after RETENTION window | Disk space reclaimed, no visible change |

Hole punching reclaims disk blocks immediately on filesystems that support it. File compaction is only needed as a fallback or when sparse files accumulate excessive fragmentation.

---

## 10. WAL-Backed Internals

### 10.1 Two-Layer Storage Model

Every `TS.ADD` passes through Raft before being applied to ETS and the columnar file:

```
Client: TS.ADD ts:views:post:123 1700000001000 42

Raft WAL entry:
  {index: 18847, term: 3, command: {ts_add, "ts:views:post:123", 1700000001000, 42.0}}

Applied to:
  Columnar file     ← persisted, compressed, partitioned
  Raft WAL          ← durable, retained for log_retention_hours
```

The columnar file is a **materialised view** of the WAL. The WAL is the source of truth. If they diverge (corruption, bug, partial write), the WAL wins and the columnar file is reconstructed from it.

```
Layer 1: Raft WAL (event log)
  Every TS.ADD in order, with original timestamps and values
  Durable and append-only
  Bounded by: log_retention_hours (default: 168h / 7 days)

Layer 2: Columnar file (materialised view)
  Optimised for query — compressed, partitioned, LMDB indexed
  Derived from Layer 1
  Can be fully reconstructed from Layer 1 within retention window
```

### 10.2 Automatic Reconstruction

On every read from a sealed partition, the NIF verifies a checksum stored in the LMDB partition entry. On mismatch:

```elixir
defmodule FerricStore.TS.Reconstruction do

  def rebuild_from_wal(series_name) do
    # 1. Delete the corrupted columnar file
    File.rm!(col_file_path(series_name))

    # 2. Create a fresh empty file
    file_fd = File.open!(col_file_path(series_name), [:write, :binary])

    # 3. Walk the Raft WAL for this series
    :ra.log_fold(
      server_ref(),
      fn
        {_index, _term, {ts_add, ^series_name, ts, val}} ->
          FerricStore.TS.FileWriter.append(file_fd, ts, val)
        _ -> :ok
      end,
      :ok
    )

    # 4. Re-seal and encode all partitions that are old enough
    FerricStore.TS.PartitionSealer.seal_all_closed(series_name)

    # 5. Rebuild LMDB partition index from the new file
    FerricStore.TS.LmdbIndex.rebuild_for_series(series_name)
  end
end
```

The user never sees this. Telemetry fires so operators are alerted.

### 10.3 CREATERULE Retroactive Backfill

When `TS.CREATERULE` is called on a series that already has history, FerricStore attempts to backfill the destination series. The quality depends on how much WAL remains.

**Case 1 — Full WAL coverage (series younger than log_retention_hours):**
```
Series started 3 days ago. WAL retention = 7 days.
All events still in WAL.

TS.CREATERULE ts:views ts:views:per_hour AGG sum BUCKET 3600000

Action: replay all ts_add events through the AGG function
Result: per_hour series populated from day 1, event-accurate
```

**Case 2 — Partial WAL, bucket ≥ partition interval (good quality backfill):**
```
Series started 90 days ago. WAL retention = 7 days.
Partition interval = 1 hour. Days 1–83: sealed hourly partitions.

TS.CREATERULE ts:views ts:views:per_week AGG sum BUCKET 604800000

Action:
  Phase A — columnar backfill (days 1–83):
    Read sealed partitions, re-aggregate to weekly buckets
    Lossless for sum, count, min, max
    Approximate for avg (correct if row counts stored in partition entry)

  Phase B — WAL backfill (days 84–90):
    Replay raw ts_add events through AGG function
    Exact values

Result: per_week series populated from day 1
        exact for days 84–90, re-aggregated for days 1–83
```

**Case 3 — Partial WAL, bucket < partition interval (cannot backfill beyond WAL):**
```
Series started 90 days ago. WAL retention = 7 days.
Partition interval = 1 day. User wants hourly rollup.

TS.CREATERULE ts:views ts:views:per_hour AGG sum BUCKET 3600000

Action:
  Cannot reconstruct hourly data from daily partitions.
  WAL backfill (days 84–90): exact.

Result: per_hour series starts from day 84.
        Days 1–83 marked as missing in TS.INFO.
        Return value includes a warning.
```

### 10.4 TS.INFO Coverage Output

After `TS.CREATERULE`, `TS.INFO` on the destination series shows exactly what data it contains:

```
TS.INFO ts:views:post:123:per_week

series:           ts:views:post:123:per_week
source_series:    ts:views:post:123
agg:              sum BUCKET 1w
coverage:
  type:           mixed
  approx_range:   2024-01-01 → 2024-03-22  (re-aggregated from sealed partitions)
  exact_range:    2024-03-22 → today        (replayed from WAL, event-accurate)
  missing_range:  none
sample_count:     13 weeks
```

### 10.5 MIRROR_STREAM

For users who need to stream every `TS.ADD` to an external system, `MIRROR_STREAM` connects TS writes to a FerricStore Stream key automatically:

```
TS.CREATE ts:views:post:123 RETENTION 7776000000 MIRROR_STREAM ts:changes:views:post:123
```

On every `TS.ADD ts:views:post:123`, FerricStore automatically appends:

```
XADD ts:changes:views:post:123 * ts 1700000001000 val 42
```

The consumer uses existing `XREAD` / `XREADGROUP` commands. When `MIRROR_STREAM` is added to an existing series, FerricStore seeds the stream from the WAL (within `log_retention_hours`).

### 10.6 Practical Recommendation

**Define compaction rules at series creation time.** The retroactive backfill is a recovery mechanism, not a workflow.

```
TS.CREATE ts:views:post:* RETENTION 7776000000
TS.CREATERULE ts:views:post:* ts:views:post:*:per_hour AGG sum BUCKET 3600000
TS.CREATERULE ts:views:post:* ts:views:post:*:per_day  AGG sum BUCKET 86400000

# Now per_hour and per_day series are built in real-time from the start.
# No WAL retention concern. No backfill gaps. Perfect coverage always.
```

Rules defined at creation = event-accurate coverage for the lifetime of the series.

---

## 11. Command Surface — Versioned

### 11.1 v1 Commands

| Command | Description |
|---|---|
| `TS.ADD key timestamp value [LABELS k v ...]` | Add a sample |
| `TS.GET key` | Get latest sample |
| `TS.RANGE key FROM to TO [AGG type BUCKET ms] [WHERE value op threshold] [FILL value]` | Range query with optional aggregation, filtering, and fill |
| `TS.MRANGE FROM to TO FILTER label=value [AGG type BUCKET ms] [WHERE ...]` | Cross-series range query |
| `TS.CREATE key [RETENTION ms] [WARM_CACHE_CHUNKS n] [CHUNK_GRACE ms] [LABELS k v ...]` | Create a series |
| `TS.INFO key` | Series metadata and coverage |
| `TS.CREATERULE src dst AGG type BUCKET ms` | Create compaction rule |
| `TS.ALTER key [RETENTION ms] [LABELS k v ...]` | Alter series parameters |

### 11.2 v1.1 Commands

| Command | Description |
|---|---|
| `TS.FILTER key FROM to TO WHERE expr [LIMIT n]` | Sample-level value filtering with SIMD evaluation |

### 11.3 v2 Commands

| Command | Description |
|---|---|
| `TS.STAT key PERCENTILE n FROM to TO [BUCKET ms]` | Percentile queries (t-digest) |
| `TS.COMPUTE "expr" FROM to TO [AGG type BUCKET ms]` | Cross-series arithmetic |
| `TS.PIPELINE [FILTER ...] \| RANGE \| WHERE \| AGG \| COMPUTE \| TOPN \| LIMIT` | Chained operations in single NIF call |

### 11.4 Command Comparison Table

| Command | RedisTimeSeries | FerricStore TS |
|---|---|---|
| `TS.ADD` | ✓ | ✓ |
| `TS.RANGE` | ✓ basic | ✓ + `WHERE`, `FILL`, `ALIGN`, `LIMIT` |
| `TS.MRANGE` | ✓ basic | ✓ + same extensions |
| `TS.CREATERULE` | ✓ required | ✓ optional — ad-hoc `AGG` works without it |
| `TS.GET` | ✓ | ✓ |
| `TS.INFO` | ✓ | ✓ |
| `TS.FILTER` | ✗ | ✓ (v1.1) |
| `TS.COMPUTE` | ✗ | ✓ (v2) |
| `TS.STAT` (PERCENTILE, TOPN, CORR, ANOMALY) | ✗ | ✓ (v2) |
| `TS.PIPELINE` | ✗ | ✓ (v2) |

---

## 12. Comparison

| Capability | QuestDB | InfluxDB 3.0 | TimescaleDB | RedisTimeSeries | FerricStore TS |
|---|---|---|---|---|---|
| Write latency | ~1–5ms | ~1–5ms | ~2–10ms | ~0.1–0.5ms | **~3μs** |
| Hot read (recent) | ~1ms | ~1ms | ~1–5ms | ~0.1–0.5ms | **~50–100μs** (open partition pread) |
| Warm read (last 24h) | ~1ms | ~1ms | ~1–5ms | ~0.1ms (RAM) | **~10–50μs** (warm cache) |
| Cold read (30d history) | ~24ms / 100M rows | ~100ms range | ~1s / 100M rows | ~0.1ms (RAM) | **~230–270μs** (io_uring) |
| RAM for 30d / 1000 series | low (columnar disk) | low (Parquet disk) | low (PG disk) | **~450GB** | **~2–6GB** (configurable) |
| Storage cost (30d) | NVMe cheap | Parquet cheap | NVMe cheap | RAM expensive | NVMe cheap |
| SQL / JOINs | Full SQL | Full SQL | Full SQL + JOINs | TS.MRANGE only | TS.MRANGE only |
| Compression | columnar | Parquet | Hypercore | Gorilla in RAM | ALP on disk |
| Single-threaded bottleneck | No | No | No | **Yes** (Redis core) | No (per-series GenServer) |
| Infrastructure | Separate service | Separate service | Separate service | Separate service | **Zero — embedded** |
| Consistency | eventual/async | eventual/async | PG ACID | async replication | **Raft quorum** |
| Persistence on crash | WAL | WAL | WAL | RDB/AOF (slow restore) | LMDB mmap (instant restore) |

**Positioning:**

- vs **QuestDB**: QuestDB wins on bulk ingestion and large analytical scans (SIMD + JIT). FerricStore wins on per-event write latency, hot reads, and zero infrastructure overhead.
- vs **InfluxDB 3.0**: InfluxDB wins on SQL, open Parquet format, and unlimited cardinality at scale. FerricStore wins on write latency and embedded deployment.
- vs **TimescaleDB**: TimescaleDB wins on full SQL with relational JOINs. FerricStore wins on write latency, hot reads, and not needing a separate PostgreSQL instance.
- vs **RedisTimeSeries**: FerricStore wins on write latency (3μs vs 0.1–0.5ms), RAM cost (6GB vs 450GB for 30d history), consistency (Raft vs async replication), and no single-threaded bottleneck. RedisTimeSeries wins on cold read latency only when all history fits economically in RAM.

---

## 13. Implementation Order

### Phase 1 — v1: Core TS + ALP + LMDB + Retention

- Columnar file format + LMDB partition index
- `TS.ADD`, `TS.GET`, `TS.RANGE` (with `WHERE`, `AGG`, `FILL`)
- `TS.MRANGE` with label filtering
- `TS.CREATE`, `TS.INFO`, `TS.CREATERULE`, `TS.ALTER`
- ALP encode (Elixir) + ALP decode (Rust NIF, AVX2)
- `PartitionSealer`, `RetentionSweeper`, `CompactionWorker` GenServers
- Hole punching retention (Linux + macOS)
- Warm cache (WARM_CACHE_CHUNKS)
- io_uring batched reads

### Phase 2 — v1.1: TS.FILTER + Labels

- `TS.FILTER` with SIMD value filtering (`ts_simd_filter` NIF)
- Labels section in columnar files (dedup string table)
- Label-based MRANGE with LMDB value pruning

### Phase 3 — v2: Advanced Codecs + Analytics

- NeaTS codec with O(log n) random access
- `CodecProbe` auto-selection module
- `TS.STAT` (PERCENTILE, TOPN, CORR, ANOMALY)
- `TS.COMPUTE` cross-series arithmetic
- `TS.PIPELINE` chained operations
- Nx integration for NeaTS function fitting

---

*End of Time Series Columnar Storage Spec*