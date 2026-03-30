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

LMDB holds three databases in the same env (same mmap'd file, one `msync`):

- **`partitions`** — per-interval partition metadata (byte offsets, min/max, codec, sealed flag)
- **`series`** — per-series config (name, codec, retention, grace, labels)
- **`rules`** — compaction rules (source → destination series, aggregation type, bucket size)

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
    codec:      u8,   // 0=raw 1=alp 2=neats
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
    .max_dbs(3)
    .open("meta/ts_index.lmdb")?;

let partitions: Database<OwnedType<[u8;12]>, OwnedType<[u8;64]>> =
    env.create_database(Some("partitions"))?;

let series: Database<OwnedType<[u8;8]>, OwnedType<[u8;256]>> =
    env.create_database(Some("series"))?;

let rules: Database<OwnedType<[u8;16]>, OwnedType<[u8;64]>> =
    env.create_database(Some("rules"))?;
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
    codec:        u8,         // 0=raw 1=alp 2=neats
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

#### Compaction Rules — Key and Value Layout

```rust
// Key: 16 bytes (source series hash + destination series hash)
struct RuleKey {
    src_hash: u64,   // xxHash64 of source series name
    dst_hash: u64,   // xxHash64 of destination series name
}

// Value: 64 bytes, fixed-width
struct RuleEntry {
    src_name_len: u16,
    src_name:     [u8; 20],  // truncated, full name in series db
    dst_name_len: u16,
    dst_name:     [u8; 20],
    agg_type:     u8,        // 0=sum 1=avg 2=min 3=max 4=count
    bucket_ms:    u64,
    created_at:   u64,
    _pad:         [u8; 5],
}
```

On startup, rules are loaded into ETS alongside series metadata. The `CompactionWorker` reads from ETS to know which rules to apply after each partition seal.

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
  series db:     series_hash → SeriesMeta                      (source of truth for series config)
  partitions db: (series_hash, partition_id) → PartitionEntry  (source of truth for partition metadata)
  rules db:      (src_hash, dst_hash) → RuleEntry              (compaction rules)

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

`TS.ADD` uses async replication — write locally, ack immediately, replicate in background. No Raft quorum wait. This matches FerricStore's existing async mode for KV data.

```
Client → Node A (any node):

  TS.ADD ts:click 1700000001000 42

  1. GenServer.cast to TS.Writer for ts:click  (~100ns, mailbox insert)
  2. Ack to client                              ← immediate, before flush
  3. TS.Writer flushes batch to .col file       ← every 1ms or 1000 samples
  4. Replicate batch to other nodes             ← fire-and-forget
```

**Why async, not quorum:** Time series samples are append-only, arrive continuously, and losing a few on crash is acceptable. At 30K/s with ~10ms replication lag, a crash loses ~300 samples — the next 300 arrive in 10ms. Quorum would add 1-5ms latency to every write for a durability guarantee that time series data doesn't need.

**Ack before flush (Option C):** The client gets `:ok` when the sample enters the GenServer mailbox, not when it hits disk. If the node crashes before flush, buffered samples (up to 1ms worth) are lost. This is the same trade-off every TSDB makes — Prometheus, InfluxDB, VictoriaMetrics, and Redis all ack from buffer and flush periodically. The 1ms buffer window is smaller than what any competitor accepts.

**Structural commands use Raft quorum** (see §11.5 for the full split).

**TS.Writer — one GenServer per series, batched writes:**

Each series gets its own `TS.Writer` GenServer, started on demand via `DynamicSupervisor`. This gives per-series parallelism — no series blocks another, and each GenServer handles its own flush timer and fd.

```elixir
defmodule FerricStore.TS.Writer do
  use GenServer

  # Flush every 1ms or when buffer reaches 1000 samples
  @flush_interval_ms 1
  @flush_threshold 1000

  def start_link(series_name) do
    GenServer.start_link(__MODULE__, series_name,
      name: {:via, Registry, {TS.WriterRegistry, series_name}})
  end

  def init(series_name) do
    schedule_flush()
    {:ok, %{
      series: series_name,
      fd: FerricStore.TS.FdManager.get_fd(series_name),
      buffer: [],
      count: 0
    }}
  end

  def handle_cast({:ts_add, timestamp, value}, state) do
    state = %{state |
      buffer: [{timestamp, value} | state.buffer],
      count: state.count + 1
    }

    if state.count >= @flush_threshold do
      {:noreply, flush(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info(:flush, state) do
    schedule_flush()
    {:noreply, flush(state)}
  end

  defp flush(%{count: 0} = state), do: state
  defp flush(state) do
    batch = Enum.reverse(state.buffer)

    # One write syscall for the entire batch
    :file.write(state.fd, encode_batch(batch))

    # Queue for async replication
    FerricStore.TS.Replicator.queue(state.series, batch)

    %{state | buffer: [], count: 0}
  end
end
```

**Routing TS.ADD to the right writer:**

```elixir
def handle_ts_add(series_name, timestamp, value) do
  # Find or start the writer for this series
  writer = case Registry.lookup(TS.WriterRegistry, series_name) do
    [{pid, _}] -> pid
    [] -> DynamicSupervisor.start_child(TS.WriterSupervisor,
            {TS.Writer, series_name}) |> elem(1)
  end

  GenServer.cast(writer, {:ts_add, timestamp, value})
  :ok  # ack immediately
end
```

**Why per-series GenServer:**

```
Single GenServer (all series):
  150K messages/sec in one mailbox — bottleneck at scale
  One series flush blocks all others

Per-series GenServer:
  ts:click → its own writer (30K msg/sec — comfortable)
  ts:temp  → its own writer (30K msg/sec — comfortable)
  Parallel flushes, no series blocks another
  GenServer overhead: ~1KB per process × 10K series = 10MB
```

**Why batching matters:**

```
Without batching (30K/s per series):
  30,000 write() syscalls/sec per series

With batching (1ms flush interval):
  30 samples/flush × 16 bytes = 480 bytes per write()
  1,000 write() syscalls/sec per series
  30x fewer syscalls
```

**Trade-off:** Up to 1ms of latency before the sample hits disk. For time series dashboards that refresh every 5 seconds, this is invisible.

LMDB is only written at **seal time** — one transaction per partition seal. For the open (current) partition, `row_count` and `ts_max` are derived from the file at query time:

```rust
// No LMDB needed for the open partition:
row_count = (file_size - header_size - sealed_bytes) / 16  // 16 bytes per sample
ts_max    = pread(fd, file_size - 16, 8)                   // last timestamp in file
```

Per-series throughput is bounded by how fast the GenServer processes messages. A single GenServer handles ~1M messages/sec on modern hardware — at 16 bytes per sample, that's ~500K series-writes/sec total across all series.

Writes never touch sealed partitions. New samples always append to the open partition tail.

**File descriptor management:** All `.col` file access goes through `FerricStore.TS.FdManager.get_fd(series_name)`. v1 implementation: open all fds at startup, require `ulimit -n 65536`. This interface allows swapping in an LRU fd pool later for 100K+ series without changing any caller code.

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

No delta-delta. No Gorilla XOR.

**Implementation:** The C++ reference implementation is MIT-licensed (`github.com/cwida/ALP`). Reimplement the core in Rust (~500 lines) or wrap via FFI. ALP decode in Rust NIF (hot path, SIMD). ALP encode in Elixir (background seal).

### 6.2 NeaTS — Learned Compression with Random Access

**Paper:** Guerra, Vinciguerra, Boffa, Ferragina (ICDE 2025). Open source at `github.com/and-gue/NeaTS`.

NeaTS fits the time series with a sequence of nonlinear functions (linear, quadratic, exponential, logarithmic) plus small bounded residuals:

```
Samples 0–450:    y ≈ 42x + 0.1      (linear trend)
Samples 451–890:  y ≈ 42 * e^0.001x  (exponential growth)
Residuals: tiny, stored in few bits each via ALP
```

**The critical property:** O(log n) random access into compressed data. With ALP, reading sample 10,000 requires decoding samples 1–9,999 sequentially. With NeaTS, reading sample 10,000 means: look up which function covers position 10,000 in the function index, evaluate the function, add the residual.

**Impact on narrow range queries:**

```
TS.RANGE ts:click FROM 09:30 TO 09:35
  (query 5 minutes inside a sealed 1-hour partition)

With ALP:
  Decode entire partition from start → filter to range
  108M samples decoded at 30K/s, ~9M needed → 12× wasted work

With NeaTS:
  Binary search function index for 09:30 offset
  Evaluate function + residuals for those ~9M samples only
  12× less decode work for narrow range queries
```

**For value filtering (`WHERE value > x`):** NeaTS lets you evaluate the filter against the function approximation first. If the entire function segment is below the threshold, skip it without decoding residuals. Fast rejection of irrelevant regions.

NeaTS works best on smooth data (views, temperature, revenue). For bursty data (errors, spikes), the function fit is poor and ALP is better. The `CodecProbe` auto-selects.

### 6.3 Codec Auto-Selection

The `CodecProbe` module in Elixir probes at seal time — one extra pass over the value column that's already in memory. Adds ~1ms to a seal.

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

  defp measure_smoothness(values) do
    # Coefficient of variation of consecutive deltas, inverted to 0–1
    deltas = values |> Enum.chunk_every(2, 1, :discard)
                    |> Enum.map(fn [a, b] -> b - a end)
    mean   = Enum.sum(deltas) / length(deltas)
    abs_mean = max(abs(mean), 1.0)
    variance = deltas |> Enum.map(fn d -> (d - mean) * (d - mean) end)
                      |> Enum.sum()
                      |> Kernel./(length(deltas))
    # 0.0 = very bursty, 1.0 = perfectly smooth
    1.0 / (1.0 + :math.sqrt(variance) / abs_mean)
  end
end
```

**How it works in practice:**

```
ts:views:post:123    → smoothness=0.85, neats_fit=0.03 → :neats_alp
ts:likes:post:123    → smoothness=0.08                 → :alp
ts:errors            → smoothness=0.02                 → :alp
ts:revenue           → smoothness=0.78, neats_fit=0.06 → :neats_alp
ts:temperature       → smoothness=0.92, neats_fit=0.01 → :neats_alp
```

**Codec lock-in after 3 consistent partitions:**

After 3 consecutive partitions pick the same codec, the probe stops running and the codec is locked. If the series behaviour changes (post goes viral — smooth views suddenly spike), smoothness drops, `codec_locked` resets, and the probe re-evaluates.

Per-partition codec stored in LMDB `PartitionEntry.codec` field — mixed codecs within one series file are fully supported. The decode path reads the `codec` byte and uses the right decoder per partition.

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
| `TS.Encoding.neats_fit` | Elixir | Math + background — Nx or pure Elixir |
| `TS.CodecProbe.pick` | Elixir | 1ms math on 1000 samples — trivially Elixir |
| `TS.PartitionSealer` GenServer | Elixir | Orchestration — pure OTP |
| `TS.CompactionWorker` GenServer | Elixir | Orchestration — pure OTP |
| `TS.RetentionSweeper` GenServer | Elixir | Orchestration — pure OTP |
| `TS.Counter` (TS.INCR) | Elixir | ETS INCR — Elixir native |
| `TS.Pipeline` parser | Elixir | Keyword parsing — pure Elixir |
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
        let bytes = pread_range(fd, from_off, to_off - from_off).await?;  // io_uring
        let codec = bytes.codec_byte();
        let (ts, values) = match codec {
            CODEC_ALP   => (alp_decode_simd(&bytes.ts_section),
                            alp_decode_simd(&bytes.value_section)),
            CODEC_NEATS => (alp_decode_simd(&bytes.ts_section),
                            neats_decode(&bytes.value_section, from_ts, to_ts)),
            _           => (raw_decode(&bytes.ts_section),
                            raw_decode(&bytes.value_section)),
        };
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

      # Codec probe — pure Elixir math, ~1ms
      codec = FerricStore.TS.CodecProbe.pick(values)

      # Encoding — pure Elixir, background, slow is fine
      encoded_ts  = FerricStore.TS.Encoding.encode_timestamps(timestamps)
      encoded_val = case codec do
        :alp       -> FerricStore.TS.Encoding.alp_encode(values)
        :neats_alp -> FerricStore.TS.Encoding.neats_encode(values)
      end

      # val_min/val_max computed during encode — free, one pass
      {val_min, val_max} = FerricStore.TS.Encoding.minmax(values)

      # Write encoded bytes — Elixir File.write
      write_partition(series_name, partition_id, encoded_ts, encoded_val)

      # LMDB update — thin Rust NIF, ~1μs
      Nif.ts_lmdb_upsert(
        series_hash(series_name), partition_id,
        build_entry(encoded_ts, encoded_val, timestamps, val_min, val_max, codec)
      )
    end)
  end
end
```

### 7.6 The Nx Option for NeaTS

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
| `FerricStore.TS.WriterSupervisor` | `permanent` | DynamicSupervisor for per-series TS.Writer GenServers |
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

Retention must be consensus-driven — all nodes in the Raft cluster must agree on what's expired. The `RetentionSweeper` runs **only on the leader** and proposes retention commands through Raft. Followers apply the same deletions when they process the replicated log entry.

```elixir
defmodule FerricStore.TS.RetentionSweeper do
  use GenServer

  # Ticks every 1 hour by default (leader only)
  # Configurable: ts.retention_sweep_interval_ms

  def handle_info(:sweep, state) do
    # Only the leader proposes retention commands
    unless FerricStore.Raft.is_leader?(), do: {:noreply, state}

    now_ms = System.system_time(:millisecond)

    series_list = ETS.select(:ts_series_meta, [{:_, :_, :_, :"$retention_ms", :_}])

    Enum.each(series_list, fn {name, _file_path, retention_ms, _} ->
      cutoff_ms = now_ms - retention_ms

      expired = Nif.ts_lmdb_scan_before(series_hash(name), cutoff_ms)

      unless Enum.empty?(expired) do
        # Propose through Raft — all nodes will apply this
        partition_ids = Enum.map(expired, & &1.partition_id)
        FerricStore.Raft.propose({:ts_expire, name, partition_ids})
      end
    end)

    schedule_next_sweep(state)
    {:noreply, state}
  end
end
```

**Raft state machine apply — runs on ALL nodes (leader + followers):**

```elixir
# In the Raft state machine apply/2 callback:
def apply(_meta, {:ts_expire, series_name, partition_ids}, state) do
  file_path = ETS.lookup(:ts_series_meta, series_name).file_path

  Enum.each(partition_ids, fn partition_id ->
    entry = Nif.ts_lmdb_get(series_hash(series_name), partition_id)

    # Punch holes in columnar file
    Nif.ts_punch_partition(file_fd(file_path), entry.ts_offset, entry.ts_len)
    Nif.ts_punch_partition(file_fd(file_path), entry.val_offset, entry.val_len)

    # Delete from LMDB partition index
    Nif.ts_lmdb_delete(series_hash(series_name), partition_id)
  end)

  :telemetry.execute(
    [:ferricstore, :ts, :retention, :dropped],
    %{partitions_dropped: length(partition_ids)},
    %{series: series_name}
  )

  {state, :ok}
end
```

Every node applies the same `{:ts_expire, series, partitions}` command from the Raft log. All nodes punch the same holes, delete the same LMDB entries, at the same log index. Retention is consistent across the cluster.

One LMDB scan + one Raft proposal per sweep on the leader. One hole punch per expired partition on every node. No file rewriting. No read blackout. Safe to run while queries are in-flight.

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

### 10.1 Two-Tier Replication Model

TS uses a split replication model: async for sample data, Raft quorum for structural commands.

```
Async replication (sample data):
  TS.ADD → write to local .col file → ack client → replicate async
  No Raft WAL entry. Columnar file IS the durable store.
  Trade-off: crash may lose last ~10ms of unreplicated samples.

Raft quorum (structural commands):
  TS.CREATE, TS.ALTER, TS.CREATERULE → proposed through ra
  {:ts_expire, ...}  → retention sweep
  {:ts_seal, ...}    → partition sealed
  All nodes apply at the same log index. Consistency guaranteed.
```

The Raft WAL contains structural commands only — not individual samples. This keeps the WAL small and avoids the Raft throughput ceiling for high-volume sample ingestion.

The columnar file is the source of truth for sample data. Each node has its own copy, kept in sync by async replication. Sealed partitions are identical across nodes (same input → deterministic encoding).

### 10.2 Automatic Reconstruction

Since samples are not in the Raft WAL, reconstruction uses **peer replication** — copy the `.col` file from another node that has it intact.

On every read from a sealed partition, the NIF verifies a checksum stored in the LMDB partition entry. On mismatch:

```elixir
defmodule FerricStore.TS.Reconstruction do

  def rebuild_from_peer(series_name) do
    # 1. Find a healthy peer that has this series
    peer = find_peer_with_series(series_name)

    # 2. Stream the .col file from peer
    File.rm!(col_file_path(series_name))
    stream_file_from_peer(peer, series_name, col_file_path(series_name))

    # 3. Rebuild LMDB partition index from the new file
    FerricStore.TS.LmdbIndex.rebuild_for_series(series_name)

    :telemetry.execute(
      [:ferricstore, :ts, :reconstruction, :completed],
      %{series: series_name, source: peer}
    )
  end
end
```

Same mechanism as new-node bootstrap (§11.3), but for a single series. The user never sees this — the read that detected corruption blocks briefly while the file is streamed, then serves correct data.

### 10.3 CREATERULE — Raft Flow and Storage

`TS.CREATERULE` flows through Raft like all mutations:

```
Client: TS.CREATERULE ts:views ts:views:per_hour AGG sum BUCKET 3600000

1. Leader proposes Raft command:
   {:ts_createrule, "ts:views", "ts:views:per_hour", :sum, 3600000}

2. All nodes apply (state machine callback):
   a. Write rule to LMDB rules db
   b. Insert into ETS :ts_compaction_rules
   c. Leader only: trigger retroactive backfill (see below)

3. CompactionWorker on all nodes now knows:
   "When ts:views seals a partition, also aggregate into ts:views:per_hour"
```

The rule is stored in LMDB `rules` db — durable, included in snapshots, available immediately on startup. The `CompactionWorker` reads rules from ETS (populated from LMDB on boot) after each partition seal.

**Backfill runs on each node independently** — every node has the same sealed partitions (deterministic encoding), so each node reads its own .col file and produces the same aggregated result.

### 10.4 CREATERULE Retroactive Backfill

When `TS.CREATERULE` is called on a series that already has history, each node backfills the destination series from its local sealed partitions. Since samples are not in the Raft WAL, backfill reads directly from the columnar file.

**Case 1 — Bucket ≥ partition interval (can re-aggregate):**
```
Series has 90 days of history. Partition interval = 1 hour.

TS.CREATERULE ts:views ts:views:per_week AGG sum BUCKET 604800000

Action:
  Read all sealed partitions from .col file
  Re-aggregate: hourly values → weekly buckets (sum of sums)
  Lossless for sum, count, min, max
  Approximate for avg (correct if row counts stored in partition entry)

  Open partition: read raw bytes, aggregate directly

Result: per_week series populated from the start, re-aggregated
```

**Case 2 — Bucket < partition interval (cannot disaggregate):**
```
Series has 90 days of history. Partition interval = 1 day.
User wants hourly rollup.

TS.CREATERULE ts:views ts:views:per_hour AGG sum BUCKET 3600000

Action:
  Sealed daily partitions cannot be broken into hours —
  the individual sample timestamps are compressed, but decoding them
  IS possible (ALP/NeaTS decode gives back exact timestamps).

  Decode each sealed partition → re-aggregate into hourly buckets
  This works because sealed partitions contain all original samples,
  just compressed. Decoding is the full inverse.

Result: per_hour series populated from the start, exact
```

> **Note:** Unlike a WAL-based approach, columnar backfill always has access to all samples within RETENTION — they're in the .col file, just compressed. The only data that's truly gone is beyond the RETENTION window (hole-punched).

### 10.5 TS.INFO Coverage Output

After `TS.CREATERULE`, `TS.INFO` on the destination series shows exactly what data it contains:

```
TS.INFO ts:views:post:123:per_week

series:           ts:views:post:123:per_week
source_series:    ts:views:post:123
agg:              sum BUCKET 1w
coverage:
  range:          2024-01-01 → today    (backfilled from sealed partitions + live)
  missing_range:  none                  (only if data was beyond RETENTION at rule creation)
sample_count:     13 weeks
```

### 10.6 MIRROR_STREAM

For users who need to stream every `TS.ADD` to an external system, `MIRROR_STREAM` connects TS writes to a FerricStore Stream key automatically:

```
TS.CREATE ts:views:post:123 RETENTION 7776000000 MIRROR_STREAM ts:changes:views:post:123
```

On every `TS.ADD ts:views:post:123`, FerricStore automatically appends:

```
XADD ts:changes:views:post:123 * ts 1700000001000 val 42
```

The consumer uses existing `XREAD` / `XREADGROUP` commands. When `MIRROR_STREAM` is added to an existing series, FerricStore seeds the stream by decoding sealed partitions from the columnar file — all data within RETENTION is available for seeding.

### 10.7 Practical Recommendation

**Define compaction rules at series creation time.** The retroactive backfill is a recovery mechanism, not a workflow.

```
TS.CREATE ts:views:post:* RETENTION 7776000000
TS.CREATERULE ts:views:post:* ts:views:post:*:per_hour AGG sum BUCKET 3600000
TS.CREATERULE ts:views:post:* ts:views:post:*:per_day  AGG sum BUCKET 86400000

# Now per_hour and per_day series are built in real-time from the start.
# No backfill needed. Perfect coverage always.
```

Rules defined at creation = event-accurate coverage for the lifetime of the series. Retroactive backfill works too (decodes sealed partitions), but defining rules upfront is simpler.

---

## 11. Replication & New-Node Bootstrap

### 11.1 How TS Data Replicates

TS uses two replication channels:

**Async replication (sample data):**
```
TS.ADD ts:click 1700000001000 42.0

  Node A (receiving node):
    1. Append to local .col file → ack client
    2. Send sample to Node B, C in background (fire-and-forget)
    3. B, C append to their local .col files
```

No Raft. No quorum wait. Each node has its own copy of the .col file, kept in sync by async replication. Nodes may be a few milliseconds behind — acceptable for time series reads.

**Raft quorum (structural commands):**
```
Raft log entries for TS:

  {ts_create, "ts:click", %{retention: ...}}              ← series creation
  {ts_alter, "ts:click", %{retention: ...}}               ← series modification
  {ts_createrule, "ts:click", "ts:click:per_hour", ...}   ← compaction rule
  {ts_expire, "ts:click", [partition_1, partition_2]}      ← retention sweep
  {ts_seal, "ts:click", partition_5}                       ← partition sealed
```

All nodes apply structural commands at the same log index. Sealing and retention are deterministic — same input data, same codec probe result, same encoded bytes.

### 11.2 Raft Snapshot Contents

The WAL only retains `log_retention_hours` (default: 7 days). The columnar files hold the full history (up to RETENTION). When a new node joins or a follower falls too far behind, ra sends a snapshot instead of replaying the WAL.

**The TS snapshot includes:**

```
Snapshot:
  1. LMDB file    — meta/ts_index.lmdb          (~71MB for 10K series)
     Contains: series registry + all partition entries
     This is the complete metadata — no rebuild needed

  2. Columnar files — data/ts/*.col               (size depends on retention)
     Contains: all sample data for all series
     Expired partitions are hole-punched (sparse) — zero bytes transferred

  3. ETS is NOT included
     Rebuilt from LMDB on snapshot install (~5ms)
```

### 11.3 New-Node Bootstrap

```
Node D joins a 3-node cluster (A=leader, B, C):

1. Node D starts, joins Raft group
   ra detects: follower has no state → needs full snapshot

2. Leader A streams snapshot to D:
   a. LMDB file (small, sent first)          → D can see metadata immediately
   b. Columnar files (streamed in chunks)     → D receives series one at a time

3. As each .col file arrives, D can serve reads for that series
   Not blocked on full transfer completing

4. After snapshot install completes:
   a. D opens LMDB → populates ETS            (~5ms)
   b. D is now a full replica

5. Leader streams WAL entries from snapshot index forward
   D applies new TS.ADD entries as they arrive
   D is now caught up and participating in consensus
```

**Transfer size is bounded by retention:**

```
Retention = 30d, 10K series at 1 sample/sec:
  LMDB:    ~71MB
  .col:    10K × 30 × 28KB = ~8.4GB
  Total:   ~8.5GB

Retention = 90d, 1M series at mixed rates:
  LMDB:    ~1.6GB
  .col:    ~3.8TB
  Total:   ~3.8TB (same as disk footprint — no amplification)
```

Expired partitions (hole-punched regions) are sparse — they contain zero bytes and are skipped during transfer. A series with 90 days of data but 30-day retention only transfers 30 days of actual bytes.

### 11.4 Follower Falls Behind (Longer Than WAL Retention)

```
Node B was down for 10 days. WAL retention = 7 days.
Node B's last applied index is older than the oldest WAL entry.

ra handles this automatically:
  1. Leader detects: follower's last index < WAL start
  2. Leader sends full snapshot (same as new-node bootstrap)
  3. B installs snapshot, catches up from WAL forward
  4. B is a full replica again
```

No manual intervention. The TS snapshot mechanism is the same whether the node is new or recovering.

### 11.5 Consistency Guarantees

```
Sample data: Async replication — eventual consistency
             Nodes may be ~10ms behind the writing node
             On crash: last ~10ms of unreplicated samples may be lost
             Acceptable trade-off for time series event data

Structure:   Raft quorum — strong consistency
             TS.CREATE, TS.ALTER, TS.CREATERULE, ts_expire, ts_seal
             All nodes apply at the same log index

Retention:   Consensus-driven via {:ts_expire, ...} Raft commands
             All nodes expire the same partitions at the same log index

Sealing:     Deterministic — same raw bytes → same ALP/NeaTS encoding
             {:ts_seal} proposed through Raft
             All nodes seal independently, produce identical results

Reads:       Served from local LMDB + local .col files
             No network hop for queries (reads are local)
             May be slightly behind the writing node for recent data
```

---

## 12. Command Surface

### 12.1 Commands

| Command | Description |
|---|---|
| `TS.ADD key timestamp value [LABELS k v ...]` | Add a sample |
| `TS.GET key` | Get latest sample |
| `TS.RANGE key FROM to TO [AGG type BUCKET ms] [WHERE value op threshold] [FILL value]` | Range query with optional aggregation, filtering, and fill |
| `TS.MRANGE FROM to TO FILTER label=value [AGG type BUCKET ms] [WHERE ...]` | Cross-series range query |
| `TS.FILTER key FROM to TO WHERE expr [LIMIT n]` | Sample-level value filtering with SIMD evaluation |
| `TS.CREATE key [RETENTION ms] [WARM_CACHE_CHUNKS n] [CHUNK_GRACE ms] [LABELS k v ...]` | Create a series |
| `TS.INFO key` | Series metadata and coverage |
| `TS.CREATERULE src dst AGG type BUCKET ms` | Create compaction rule |
| `TS.ALTER key [RETENTION ms] [LABELS k v ...]` | Alter series parameters |
| `TS.STAT key PERCENTILE n FROM to TO [BUCKET ms]` | Percentile queries (t-digest) |
| `TS.COMPUTE "expr" FROM to TO [AGG type BUCKET ms]` | Cross-series arithmetic |
| `TS.PIPELINE [FILTER ...] \| RANGE \| WHERE \| AGG \| COMPUTE \| TOPN \| LIMIT` | Chained operations in single NIF call |

### 12.2 Command Comparison Table

| Command | RedisTimeSeries | FerricStore TS |
|---|---|---|
| `TS.ADD` | ✓ | ✓ |
| `TS.RANGE` | ✓ basic | ✓ + `WHERE`, `FILL`, `ALIGN`, `LIMIT` |
| `TS.MRANGE` | ✓ basic | ✓ + same extensions |
| `TS.CREATERULE` | ✓ required | ✓ optional — ad-hoc `AGG` works without it |
| `TS.GET` | ✓ | ✓ |
| `TS.INFO` | ✓ | ✓ |
| `TS.FILTER` | ✗ | ✓ |
| `TS.COMPUTE` | ✗ | ✓ |
| `TS.STAT` (PERCENTILE, TOPN, CORR, ANOMALY) | ✗ | ✓ |
| `TS.PIPELINE` | ✗ | ✓ |

---

## 13. Comparison

| Capability | QuestDB | InfluxDB 3.0 | TimescaleDB | RedisTimeSeries | FerricStore TS |
|---|---|---|---|---|---|
| Write latency | ~1–5ms | ~1–5ms | ~2–10ms | ~0.1–0.5ms | **~3μs** |
| Hot read (recent) | ~1ms | ~1ms | ~1–5ms | ~0.1–0.5ms | **~50–100μs** (open partition pread) |
| Warm read (last 24h) | ~1ms | ~1ms | ~1–5ms | ~0.1ms (RAM) | **~10–50μs** (warm cache) |
| Cold read (30d history) | ~24ms / 100M rows | ~100ms range | ~1s / 100M rows | ~0.1ms (RAM) | **~230–270μs** (io_uring) |
| RAM for 30d / 1000 series | low (columnar disk) | low (Parquet disk) | low (PG disk) | **~450GB** | **~2–6GB** (configurable) |
| Storage cost (30d) | NVMe cheap | Parquet cheap | NVMe cheap | RAM expensive | NVMe cheap |
| SQL / JOINs | Full SQL | Full SQL | Full SQL + JOINs | TS.MRANGE only | TS.MRANGE only |
| Compression | columnar | Parquet | Hypercore | Gorilla in RAM | ALP + NeaTS on disk |
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

## 14. Implementation Order

1. Columnar file format + LMDB (partition index + series registry)
2. `TS.CREATE`, `TS.ADD`, `TS.GET` — basic write/read path
3. ALP encode (Elixir) + ALP decode (Rust NIF, AVX2)
4. `PartitionSealer` GenServer + auto-tuned partition intervals
5. `TS.RANGE` with `WHERE`, `AGG`, `FILL` + io_uring batched reads
6. NeaTS codec + `CodecProbe` auto-selection
7. `TS.MRANGE` with label filtering + LMDB value pruning
8. `TS.FILTER` with SIMD value filtering (`ts_simd_filter` NIF)
9. Labels section in columnar files (dedup string table)
10. Warm cache (`WARM_CACHE_CHUNKS`)
11. `RetentionSweeper` + hole punching (Linux + macOS)
12. `CompactionWorker` + `TS.CREATERULE` + WAL backfill
13. `TS.INFO`, `TS.ALTER`
14. `TS.STAT` (PERCENTILE, TOPN, CORR, ANOMALY)
15. `TS.COMPUTE` cross-series arithmetic
16. `TS.PIPELINE` chained operations

---

*End of Time Series Columnar Storage Spec*