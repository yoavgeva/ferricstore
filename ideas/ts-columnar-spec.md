# FerricStore — Time Series: Columnar Chunk Encoding
### Spec Addendum — Closing the Compression Gap with TimescaleDB

---

## 1. Why Columnar Chunk Encoding

FerricStore's base time series design stores each sample as an independent Bitcask entry: one key (8-byte timestamp), one value (8-byte float + labels). This is correct and fast for writes, but leaves compression on the table for historical data.

TimescaleDB achieves 90%+ compression on cold chunks via columnar encoding: timestamps stored together (delta-delta compressed), values stored together (Gorilla/XOR compressed). Without an equivalent, FerricStore TS uses significantly more disk for any series with history beyond a few hours.

The solution is **chunk sealing**: when a time bucket closes, the Rust NIF batch-converts all individual Bitcask entries for that bucket into a single columnar binary blob, replacing N independent entries with one compressed entry. This happens automatically — no operator intervention, no change to the write path.

| Metric | Unencoded Bitcask | Columnar Chunk | Gain |
|---|---|---|---|
| Storage per 3600 samples (1/sec hour) | ~115 KB | ~8–14 KB | 8–14x compression |
| Range scan (full hour) | 3600 pread() calls | 1 pread() call | 3600x fewer syscalls |
| Cold read latency (1 hour) | ~180ms (3600 × 50μs) | ~0.5ms | ~360x faster |
| Hot read (ETS, last 1hr) | ETS ordered_set | ETS ordered_set | No change |
| Write latency | ~3μs ETS + async Bitcask | Same (sealing is async) | No change |

> **Note:** Compression ratios assume typical click/view/login event counts — smooth monotonic values. Highly random values compress less. Delta-delta encoding degrades gracefully: worst case (random) it adds ~4 bytes overhead per sample vs raw, which is less than 1% of the baseline entry size.

---

## 2. Chunk Lifecycle

### 2.1 States

Every time bucket in a series passes through three states:

| State | Storage | Reads | Writes | Transition |
|---|---|---|---|---|
| **Open** | Individual Bitcask entries per sample | ETS hot (recent) or per-entry Bitcask reads | Direct ETS insert + async Bitcask append | Bucket end timestamp passes |
| **Sealing** | Individual entries being converted (read-only window) | Individual entries (conversion in progress) | Rejected — bucket is closed | Conversion completes (~10ms typical) |
| **Sealed** | Single columnar binary blob in Bitcask | One pread() + in-memory decode | Not applicable — immutable | Retention policy drops it |

### 2.2 Sealing Trigger

The `ChunkSealer` GenServer monitors active series. A bucket is sealed when both conditions are true:

- The bucket's end timestamp has passed (e.g., `10:00:00` for the `09:xx` bucket)
- A configurable grace period has elapsed (default: 60 seconds) to allow late-arriving samples

The grace period matters. Event pipelines have tail latency — a sample timestamped `09:59:55` may arrive at FerricStore at `10:00:05` due to network or processing delay. Sealing immediately at bucket boundary would discard it. With a 60-second grace, it lands in the correct bucket before sealing.

> **Note:** Grace period is configurable per series via `TS.CREATE ... CHUNK_GRACE 30000` (milliseconds). High-frequency IoT series can use 5 seconds. Event pipelines with async flush jobs should use 120 seconds.

### 2.3 What Happens During Sealing

The `ChunkSealer` calls a single Rust NIF:

```rust
ts_seal_chunk(series_ref, bucket_start_ms, bucket_end_ms) -> {:ok, bytes_before, bytes_after} | {:error, reason}
```

Inside the NIF, the sealing process is:

1. Scan all Bitcask entries with keys in `[bucket_start_ms, bucket_end_ms)`
2. Extract timestamp column: N × 8-byte big-endian integers
3. Extract value column: N × 8-byte IEEE 754 floats
4. Delta-delta encode the timestamp column
5. Gorilla XOR encode the value column
6. Serialize labels as a deduplicated string table (one entry per unique label set)
7. Write the columnar blob as a single new Bitcask entry at key `chunk:{bucket_start_ms}`
8. Delete all N individual sample entries (Bitcask tombstones — merged away on next compaction)

The seal is atomic from the reader's perspective: a `TS.RANGE` spanning this bucket will see either all individual entries or the sealed chunk, never a partial view, because the chunk key is only written after all individual entries are tombstoned.

> **Important:** The atomicity guarantee is best-effort under crash conditions. If the process crashes mid-seal, the individual entries remain intact (tombstones are not durable until Bitcask merge). On restart, the `ChunkSealer` re-detects the unsealed bucket and re-seals it. The seal operation is **idempotent**.

---

## 3. Binary Format

### 3.1 Chunk Header (32 bytes, fixed)

| Offset | Size | Field | Description |
|---|---|---|---|
| 0 | 4 bytes | `magic` | `0x46455243` (ASCII: `FERC`) — identifies columnar chunk |
| 4 | 1 byte | `version` | Format version — currently `0x01` |
| 5 | 1 byte | `flags` | Bit 0: `has_labels`. Bit 1: `has_metadata`. Bits 2–7: reserved |
| 6 | 2 bytes | `reserved` | Zero — reserved for future use |
| 8 | 4 bytes | `sample_count` | Number of samples in this chunk (uint32 LE) |
| 12 | 8 bytes | `first_timestamp` | Absolute timestamp of first sample (ms, uint64 LE) |
| 20 | 4 bytes | `ts_section_len` | Byte length of timestamp section |
| 24 | 4 bytes | `val_section_len` | Byte length of value section |
| 28 | 4 bytes | `labels_section_len` | Byte length of labels section (0 if no labels) |

### 3.2 Timestamp Section — Delta-Delta Encoding

Timestamps are stored as delta-of-deltas, which exploits the regularity of time series data. For a 1-sample-per-second series, all deltas are 1000ms — the delta-of-deltas are all zero, compressing to near-zero bits per sample.

**Encoding algorithm (Rust):**

```rust
let delta_0 = timestamps[0] - first_timestamp;   // stored in header
let delta_1 = timestamps[1] - timestamps[0];      // first delta

// For i >= 2:
let dod = (timestamps[i] - timestamps[i-1]) - (timestamps[i-1] - timestamps[i-2]);
```

Each delta-of-delta is stored using variable-length encoding:

| Delta-of-delta value | Encoding | Bits used |
|---|---|---|
| `0` | Single bit: `0` | 1 bit |
| `-63` to `+64` | 2-bit prefix `10` + 7-bit signed value | 9 bits |
| `-255` to `+256` | 3-bit prefix `110` + 9-bit signed value | 12 bits |
| `-2047` to `+2048` | 4-bit prefix `1110` + 12-bit signed value | 16 bits |
| Any other value | 4-bit prefix `1111` + 64-bit signed value | 68 bits |

> **Note:** For regular 1-second event data, virtually all delta-of-deltas are 0 (1 bit each). A 3600-sample hour compresses to ~450 bytes for timestamps alone. Irregular bursty data uses ~9 bits average, giving ~4KB for the same 3600 samples.

### 3.3 Value Section — Gorilla XOR Encoding

Float values are XOR-encoded using the Facebook Gorilla algorithm. Consecutive float values in time series data share leading and trailing zero bits in their IEEE 754 representation. XOR of two similar values leaves mostly zeros, which compress efficiently.

**Encoding per sample:**

1. XOR the current float bits with the previous float bits
2. If XOR is zero → write bit `0` (1 bit total)
3. If XOR shares the same leading/trailing zero pattern as previous → write bit `10` + meaningful bits only
4. Otherwise → write bit `11` + 5-bit leading zeros count + 6-bit meaningful length + meaningful bits

For event count data (integers like 1, 2, 5, 12...) stored as float64, the XOR pattern is highly compressible. Typical compression: 3–4 bytes per sample vs 8 bytes raw. A 3600-sample hour compresses to ~11–14 KB for values.

### 3.4 Labels Section

Labels are deduplicated using a string table to avoid repeating common label sets across samples:

- **String table:** length-prefixed list of unique label strings
- **Per-sample label index:** varint index into string table (1–2 bytes typical)
- **If all samples share the same labels** (common case): `flags` bit 0 = `1`, single label entry, no per-sample index

> **Note:** For the user activity event use case (click/view/login with region/device labels), all samples in a bucket typically share the same label set. The flags bit optimization reduces the labels section to a single string entry — effectively zero per-sample overhead.

---

## 4. Rust NIF Functions

### 4.1 New NIF Exports

| NIF function | Scheduler | Purpose |
|---|---|---|
| `ts_seal_chunk(pid, ref, bucket_start, bucket_end)` | Async Tokio — IO | Seal one time bucket into columnar blob |
| `ts_read_chunk(pid, ref, bucket_start)` | Async Tokio — IO | Read and decode a sealed chunk |
| `ts_range_mixed(pid, ref, from_ms, to_ms, agg_type, bucket_ms)` | Async Tokio — IO | Range query spanning sealed + unsealed buckets |
| `ts_chunk_info(ref, bucket_start)` | Sync (keydir only) | Header metadata without decoding |
| `ts_estimate_compression(ref, bucket_start, bucket_end)` | Async Tokio — IO | Dry-run estimate before sealing |

### 4.2 `ts_seal_chunk` Detail

Key implementation constraints:

- Must be fully async — sealing 3600 entries involves disk reads and a disk write
- Must be atomic-enough: write chunk key **before** deleting individual entries, never the reverse
- Must be idempotent: if chunk key already exists, return `ok` without re-sealing
- Must report `bytes_before` and `bytes_after` for monitoring

```rust
fn ts_seal_chunk(env: Env, args: &[Term]) -> Result<Term, Error> {
    let pid: Pid          = args[0].decode()?;
    let ref_arc           = args[1].decode::<ResourceArc<BitcaskHandle>>()?;
    let bucket_start: u64 = args[2].decode()?;
    let bucket_end: u64   = args[3].decode()?;

    tokio_rt().spawn(async move {
        let result = seal_chunk(&ref_arc, bucket_start, bucket_end).await;
        send_result(pid, result);
    });
    Ok(atoms::ok().encode(env))
}
```

### 4.3 `ts_range_mixed` Detail

Range queries frequently span both sealed and unsealed buckets. This NIF handles the fan-out transparently:

1. For each bucket boundary in `[from_ms, to_ms]`:
   - Check if `chunk:{bucket_start}` key exists in Bitcask keydir (O(1) hashmap lookup)
   - If yes → read sealed chunk, decode, filter, apply aggregation
   - If no → scan individual entries in that bucket range
2. Merge results in timestamp order
3. Apply final aggregation across all buckets if requested

The keydir lookup costs ~50ns per bucket. The sealed path is dramatically faster for historical buckets: one `pread` per bucket vs hundreds or thousands of individual reads.

---

## 5. Elixir GenServer Architecture

### 5.1 ChunkSealer GenServer

```elixir
defmodule FerricStore.TS.ChunkSealer do
  use GenServer

  # State: %{series_name => %{bucket_start_ms => :open | :sealing | :sealed}}

  # Ticks every 10 seconds.
  # For each open bucket where now > bucket_end + grace_period:
  #   Call ts_seal_chunk NIF async
  #   Mark bucket as :sealing in state
  #   On NIF result: mark :sealed, emit telemetry
end
```

### 5.2 Supervision Tree

| Process | Restart | Role |
|---|---|---|
| `FerricStore.Supervisor` | `one_for_one` | Root supervisor |
| `FerricStore.TS.Registry` | `permanent` | ETS owner for series metadata and compaction rules |
| `FerricStore.TS.ChunkSealer` | `permanent` | Seals closed buckets on a tick schedule |
| `FerricStore.TS.RetentionSweeper` | `permanent` | Deletes samples/chunks beyond retention window |
| `FerricStore.TS.CompactionWorker` | `transient` | Runs per-series compaction rules (hourly → daily rollups) |

### 5.3 Telemetry Events

| Event | Measurements | Metadata |
|---|---|---|
| `[:ferricstore, :ts, :chunk, :sealed]` | `bytes_before, bytes_after, duration_ms, sample_count` | `series, bucket_start, compression_ratio` |
| `[:ferricstore, :ts, :chunk, :seal_failed]` | `duration_ms` | `series, bucket_start, reason` |
| `[:ferricstore, :ts, :retention, :dropped]` | `chunks_dropped, bytes_freed` | `series, cutoff_ms` |

---

## 6. Query Path: Sealed vs Unsealed

| Data location | Path | Typical latency |
|---|---|---|
| Last 5 minutes (ETS hot cache) | ETS `ordered_set` range | < 100μs |
| Last 2 hours (ETS + 1 sealed chunk) | ETS + `ts_read_chunk` | < 2ms |
| Yesterday's hourly totals (24 sealed chunks) | 24 × `ts_read_chunk` in parallel | < 10ms |
| Last 30 days of daily totals (30 sealed chunks) | 30 × `ts_read_chunk` in parallel | < 20ms |
| Raw samples, 7 days (168 sealed hourly chunks) | `ts_range_mixed`, batched reads | < 100ms |

> **Note:** Parallel chunk reads use Tokio's `join!` macro — multiple sealed buckets are submitted to the io_uring ring in one batch, not read sequentially.

---

## 7. Interaction with Bitcask Merge

The existing Bitcask merge process sees sealed chunks as regular key-value entries — one key (`chunk:{bucket_start}`) mapping to a large value (the columnar blob). No changes to merge logic are needed:

- The chunk key is **live** — kept during merge
- The N individual sample tombstones are **dead** — removed during merge, reclaiming disk space
- The columnar blob is copied to the new merged file as a single entry

Disk space reduction from sealing is not immediate — tombstones are only reclaimed after the next Bitcask merge cycle. The existing merge trigger thresholds handle this naturally: a large sealing event producing many tombstones accelerates the merge trigger.

**Ordering guarantee:** the `chunk:` prefix sorts before raw 8-byte timestamp keys for samples in the same bucket. The Rust merge scan processes keys in sorted order, so chunks are always preserved before their constituent tombstones are processed.

---

## 8. Impact on Compaction Rules

Compaction rules (e.g., `ts:click → ts:click:per_hour, SUM, 3600000ms`) must read sealed chunks as input:

- If the source bucket **is sealed** → call `ts_read_chunk`, decode, aggregate
- If the source bucket **is unsealed** → call `ts_range` as before
- The `CompactionRule` GenServer detects sealing state via `ts_chunk_info` (keydir-only, O(1))

The rollup hierarchy seals independently at each level: raw samples seal first, then hourly rollups, then daily rollups.

### Compression Ratios Across Rollup Levels

| Series | Bucket size | Samples/bucket | Sealed size | vs raw Bitcask |
|---|---|---|---|---|
| `ts:click` (raw) | 1 hour | ~3600 (1/sec) | ~14 KB | vs ~115 KB (8x) |
| `ts:click:per_minute` | 1 day | 1440 | ~6 KB | vs ~46 KB (7.5x) |
| `ts:click:per_hour` | 1 week | 168 | ~1.2 KB | vs ~5.4 KB (4.5x) |
| `ts:click:per_day` | 1 year | 365 | ~2.4 KB | vs ~11.7 KB (5x) |

---

## 9. Implementation Order

| Week | Deliverable | Depends on |
|---|---|---|
| TS Week 1–2 | Base TS: `ts_add`, `ts_range` NIFs, `TS.ADD`, `TS.RANGE`, ETS hot layer | Bitcask NIF foundation |
| TS Week 3 | Compaction rules GenServer, `TS.CREATERULE`, `TS.DELETERULE` | Base TS |
| TS Week 4 | Labels support, `TS.MRANGE`, secondary ETS index | Base TS |
| Columnar Week 1 | Binary format, `ts_seal_chunk` NIF (write path only) | Base TS complete |
| Columnar Week 2 | `ts_read_chunk` NIF, `ts_range_mixed` NIF, `ChunkSealer` GenServer | Columnar Week 1 |
| Columnar Week 3 | Telemetry, monitoring metrics, compression ratio reporting (`TS.INFO`) | Columnar Week 2 |
| Columnar Week 4 | Compaction rules updated to read sealed chunks, full integration tests | All above |

> **Note:** Base TS is fully functional without columnar encoding. Sealing is an optimization layer — you can ship TS without it and add sealing later. `ts_range_mixed` handles both sealed and unsealed buckets transparently.

---

## 10. Corrected Query Latency — io_uring Batch Reads

The original spec stated `<20ms` for 30 sealed chunks. That was wrong. It assumed 30 sequential `pread()` syscalls. With io_uring batch submission, all reads are submitted in one ring write and served by NVMe in parallel.

**Correct math for 30 daily chunks (30 × 14KB = 420KB total):**

```
NVMe sequential read at 5GB/s:   ~84μs
io_uring ring submission:         ~1–2μs (one syscall, not 30)
In-memory decode (30 chunks):     ~60–100μs (delta-delta = integer subtraction)

Total:                            ~150–200μs
```

The old `<20ms` estimate was 100× too high. Without io_uring (plain sequential `pread()` loop) the estimate would have been correct — each syscall round-trip costs ~500μs, so 30 × 500μs = 15–20ms. io_uring batch submission is what makes the difference.

**Corrected latency table:**

| Query | Old estimate | Corrected (io_uring) | Bottleneck |
|---|---|---|---|
| Last 5 min | `<100μs` ✓ | `<100μs` | ETS `ordered_set` — no disk |
| Last 2 hours | `<2ms` | `~200–300μs` | ETS + 1 pread + decode |
| Yesterday's hourly totals | `<10ms` | `~120–180μs` | 24 parallel preads + decode |
| 30 days of daily totals | `<20ms` ❌ | `~150–200μs` | 30 parallel preads + decode |
| 7 days raw (168 chunks) | not in spec | `~500μs–1ms` | 2.4MB read — decode is now the bottleneck |

> **Note:** For large queries (7+ days raw), in-memory decode dominates over I/O. 168 chunks × 3600 samples = ~605K delta-delta operations at ~1ns each = ~600μs. This is parallelisable across Tokio tasks by splitting the chunk list.

---

## 11. Memory Tiering — Using Disk to Reduce RAM Pressure

FerricStore TS already has a natural two-tier model: ETS for hot data, Bitcask/NVMe for sealed history. This section specifies three new tuning knobs that expose this as an explicit, operator-controlled tiering system — directly addressing the core weakness of RedisTimeSeries (all history must live in RAM).

### 11.1 The Four-Tier Model

```
┌─────────────────────────────────────────────────────────────┐
│  Tier 0 — ETS hot window                  ~3μs reads        │
│  Raw samples for the last HOT_WINDOW duration               │
│  Controlled by: HOT_WINDOW per series                       │
│  Default: last open bucket only                             │
├─────────────────────────────────────────────────────────────┤
│  Tier 1 — ETS warm cache                  ~10–50μs reads    │
│  Recently-accessed sealed chunks, decoded and kept in ETS   │
│  Controlled by: WARM_CACHE_CHUNKS per series                │
│  Eviction: LRU, evicts decoded chunks when limit reached    │
├─────────────────────────────────────────────────────────────┤
│  Tier 2 — NVMe sealed chunks              ~150–500μs reads  │
│  All sealed history, compressed on disk                     │
│  Controlled by: RETENTION per series                        │
│  Read path: io_uring batched pread + in-memory decode       │
├─────────────────────────────────────────────────────────────┤
│  Tier 3 — Expired (deleted)                                 │
│  Beyond RETENTION window                                    │
│  Managed by: RetentionSweeper GenServer                     │
└─────────────────────────────────────────────────────────────┘
```

### 11.2 New TS.CREATE Parameters

```
TS.CREATE key
  [RETENTION ms]              existing — total history to keep
  [HOT_WINDOW ms]             new — how much raw sample data stays in ETS
  [WARM_CACHE_CHUNKS n]       new — how many recently-accessed sealed chunks
                                    stay decoded in ETS after first read
  [CHUNK_GRACE ms]            existing — late-sample grace period before sealing
```

**Examples:**

```
# Dashboard series — query last 2h constantly, history up to 90 days
TS.CREATE ts:click RETENTION 7776000000 HOT_WINDOW 7200000 WARM_CACHE_CHUNKS 24

# IoT sensor — only need last 5 min hot, keep 1 year history
TS.CREATE ts:temperature RETENTION 31536000000 HOT_WINDOW 300000 WARM_CACHE_CHUNKS 0

# Billing metric — keep 30 days, entire history accessed regularly
TS.CREATE ts:revenue RETENTION 2592000000 HOT_WINDOW 3600000 WARM_CACHE_CHUNKS 720
```

### 11.3 New Global Config

```elixir
# ferricstore.conf

# Hard RAM ceiling for all TS ETS hot windows combined.
# When hit, oldest open buckets are force-sealed and evicted to Bitcask.
ts.hot_cache_max_mb = 512

# Hard RAM ceiling for all TS ETS warm caches combined.
# When hit, LRU eviction across all series.
ts.warm_cache_max_mb = 1024

# io_uring queue depth for sealed chunk reads.
# Higher = more parallel reads per batch, diminishing returns above 64.
ts.io_uring_queue_depth = 32
```

### 11.4 How the Warm Cache Works

The warm cache is an ETS table keyed by `{series_name, bucket_start_ms}` holding the fully decoded sample list from a sealed chunk. It is populated lazily on first read — no proactive warming.

```elixir
# Read path in ts_range_mixed:
# 1. Check Tier 0 (ETS hot)         → return immediately if hit
# 2. Check Tier 1 (ETS warm cache)  → return immediately if hit
# 3. Issue io_uring batch for misses → decode → insert into warm cache → return

# Warm cache entry lifecycle:
# - Inserted on first disk read of a sealed chunk
# - Evicted LRU when ts.warm_cache_max_mb is exceeded
# - Invalidated if the chunk is re-sealed (shouldn't happen, but handled)
```

The warm cache effectively gives operators a knob that says: "I know my dashboard queries the last 24 hours constantly — keep those 24 sealed chunks decoded in RAM so they serve at ETS speed, not disk speed." The RAM cost is ~14KB × 24 = 336KB per series. For 1000 series that's 336MB — a fraction of what RedisTimeSeries would need to keep the same history in RAM.

### 11.5 RAM Cost Comparison at Scale

Scenario: 1000 series, 30-day retention, querying last 24 hours frequently.

| System | Hot data in RAM | History storage | Total RAM for TS | 30-day disk cost |
|---|---|---|---|---|
| RedisTimeSeries | All 30 days (required) | RAM only | ~450GB | $0 |
| FerricStore TS (no warm cache) | Last open bucket only | NVMe sealed chunks | ~2GB | ~420MB NVMe |
| FerricStore TS (24h warm cache) | Last 2h hot + 22h warm decoded | NVMe for older history | ~6GB | ~360MB NVMe |
| FerricStore TS (max warm) | Last 2h hot + full 30d warm decoded | NVMe (backup only) | ~450GB | ~420MB NVMe |

The bottom row shows that FerricStore can optionally match RedisTimeSeries's all-in-RAM behaviour at the same RAM cost — but with NVMe as a durable backup underneath. The sweet spot is the third row: 24h warm cache gives near-RAM performance for the most commonly queried window at 1/75th the RAM cost.

> **Note:** `WARM_CACHE_CHUNKS 0` is a valid setting — no warm cache, all cold reads go straight to io_uring. Useful for write-heavy series where history is rarely queried (IoT sensors, log aggregation).

### 11.6 Prefetch Optimisation

For `TS.RANGE` queries that span both ETS and disk, the NIF can overlap disk I/O with ETS serving:

```rust
// ts_range_mixed with prefetch:
// 1. Identify which buckets are in ETS (hit) vs Bitcask (miss)
// 2. Submit io_uring reads for all misses immediately (non-blocking)
// 3. While reads are in flight, collect ETS hits
// 4. When reads complete, decode and merge
// Net effect: disk latency is hidden behind ETS serving time
```

For queries where the ETS portion takes longer than the disk read (large hot window, small sealed range), the effective cold read latency approaches zero from the user's perspective.

---

## 12. Revised Full Comparison — All Databases

Incorporating the corrected io_uring latency numbers and the tiering model.

| Capability | QuestDB | InfluxDB 3.0 | TimescaleDB | RedisTimeSeries | FerricStore TS |
|---|---|---|---|---|---|
| Write latency | ~1–5ms | ~1–5ms | ~2–10ms | ~0.1–0.5ms | **~3μs** |
| Hot read (recent) | ~1ms | ~1ms | ~1–5ms | ~0.1–0.5ms | **~3–100μs** |
| Warm read (last 24h) | ~1ms | ~1ms | ~1–5ms | ~0.1ms (RAM) | **~10–50μs** (warm cache) |
| Cold read (30d history) | ~24ms / 100M rows | ~100ms range | ~1s / 100M rows | ~0.1ms (RAM) | **~150–200μs** (io_uring) |
| RAM for 30d / 1000 series | low (columnar disk) | low (Parquet disk) | low (PG disk) | **~450GB** | **~2–6GB** (configurable) |
| Storage cost (30d) | NVMe cheap | Parquet cheap | NVMe cheap | RAM expensive | NVMe cheap |
| SQL / JOINs | Full SQL | Full SQL | Full SQL + JOINs | TS.MRANGE only | TS.MRANGE only |
| Compression | columnar | Parquet | Hypercore | Gorilla in RAM | delta-delta + Gorilla on disk |
| Single-threaded bottleneck | No | No | No | **Yes** (Redis core) | No (per-series GenServer) |
| Infrastructure | Separate service | Separate service | Separate service | Separate service | **Zero — embedded** |
| Consistency | eventual/async | eventual/async | PG ACID | async replication | **Raft quorum** |
| Persistence on crash | WAL | WAL | WAL | RDB/AOF (slow restore) | Bitcask hint-file (fast restore) |

**The positioning in one sentence per competitor:**

- vs **QuestDB**: QuestDB wins on bulk ingestion and large analytical scans (SIMD + JIT). FerricStore wins on per-event write latency, hot reads, and zero infrastructure overhead.
- vs **InfluxDB 3.0**: InfluxDB wins on SQL, open Parquet format, and unlimited cardinality at scale. FerricStore wins on write latency and embedded deployment.
- vs **TimescaleDB**: TimescaleDB wins on full SQL with relational JOINs. FerricStore wins on write latency, hot reads, and not needing a separate PostgreSQL instance.
- vs **RedisTimeSeries**: FerricStore wins on write latency (3μs vs 0.1–0.5ms), RAM cost (6GB vs 450GB for 30d history), consistency (Raft vs async replication), and no single-threaded bottleneck. RedisTimeSeries wins on cold read latency only when all history fits economically in RAM — which stops being true beyond a few hours of retention at any meaningful series count.

---

## 13. Revised Storage Architecture — Columnar Files

> **Supersedes sections 2–4.** The per-sample Bitcask model is replaced by a per-series columnar file model. Bitcask becomes an index, not a storage engine. This section describes the new authoritative design.

### 13.1 The Core Change

The original design stored every sample as an independent Bitcask entry. The new design has three parts working together:

- **Columnar file per series** — all samples for one series in one file
- **LMDB partition index** — a memory-mapped B-tree holding partition metadata for all series, durable and instantly queryable at startup with no rebuild
- **Bitcask** — series registry only, one entry per series pointing to its file

**Before:**
```
ts:click series, 90 days:
  timeseries/ts:click/chunk:1700000000/  ← 2,160 sealed chunk files
  ...each with its own Bitcask entry
```

**After:**
```
data/
  ts/
    ts_click.col           ← all 90 days in one file
    ts_temperature.col
    ...
  meta/
    partition_index.lmdb   ← metadata for every partition of every series
```

Cross-series queries go from 21.6M file ops (old chunks) to 10K file ops — one per series, preceded by a fast LMDB range scan that resolves byte offsets without touching any columnar file.

### 13.2 LMDB Partition Index

LMDB is a memory-mapped B-tree key-value store — embedded, no server process, ACID, and as fast as RAM once the OS pages are warm. The same file FerricStore uses for Raft metadata. Used inside OpenLDAP, Tor, and Meilisearch for exactly this pattern.

**Why LMDB over ETS here:**

| | ETS | LMDB |
|---|---|---|
| Durable on crash | No — volatile | Yes — ACID |
| Startup rebuild | Yes — reads all file headers (~500ms for 10K series) | No — mmap, instant |
| Range scan by (series, day) | Table scan | B-tree cursor, O(log n) |
| Val min/max pruning | Yes, but volatile | Yes, durable |
| Memory | Always loaded | OS pages on demand |

**Key and value layout:**

```rust
// Key: 12 bytes, fixed-width, sortable
// Sorts by series then chronologically within series
struct PartitionKey {
    series_hash: u64,  // xxHash64 of series name
    day:         u32,  // unix day number (days since epoch)
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
    codec:      u8,   // 0=raw 1=alp 2=chimp 3=neats 4=sprintz
    sealed:     u8,   // 0=open 1=sealed
    _pad:       [u8;6],
}
```

Fixed-width key and value means LMDB uses direct byte comparators — no parsing, no allocation. The sort order (series_hash + day) puts all partitions for one series physically adjacent in the B-tree, so a range scan for one series is a sequential read.

**Rust crate:** `heed` v0.20 — MIT licensed, used by Meilisearch.

```toml
[dependencies]
heed = "0.20"
```

```rust
let env = EnvOpenOptions::new()
    .map_size(10 * 1024 * 1024 * 1024)  // 10GB virtual — not physical RAM
    .open("meta/partition_index.lmdb")?;

let db: Database<OwnedType<[u8;12]>, OwnedType<[u8;64]>> =
    env.create_database(None)?;
```

The `map_size` is virtual address space, not RAM. For 10K series × 90 days = 900K entries × 76 bytes = ~68MB on disk. LMDB pages in only what queries touch.

**Write path — one LMDB write per partition seal:**

```rust
fn ts_upsert_partition(series_hash: u64, day: u32, entry: PartitionEntry) {
    let mut txn = env.begin_rw_txn()?;
    let key = encode_key(series_hash, day);
    txn.put(db, &key, &entry.as_bytes(), WriteFlags::empty())?;
    txn.commit()?;  // single msync — fast, durable
}
```

**Read path — range scan for a query:**

```rust
// "Give me all partitions for ts:click in the last 7 days"
let series_hash = xxhash64("ts:click");
let from_key    = encode_key(series_hash, today - 7);
let to_key      = encode_key(series_hash, today);

let txn      = env.begin_ro_txn()?;
let mut cursor = db.open_ro_cursor(&txn)?;

let entries: Vec<PartitionEntry> = cursor
    .iter_from(from_key)
    .take_while(|(k, _)| k <= to_key)
    .map(|(_, v)| PartitionEntry::from_bytes(v))
    .collect();
```

For 10K series × 7-day query = 70K entries, all returned in one sequential B-tree traversal. No random seeks inside LMDB.

**Startup behaviour:**

```
FerricStore starts:
  Open LMDB env → mmap the file                (~1ms)
  All partition metadata is immediately addressable
  Zero rebuild. Zero file header reads.
  OS loads LMDB pages on first access (demand paging).
```

Compare to ETS rebuild: reading 10K file headers = 30MB of I/O, 900K ETS inserts = ~500ms–2s. LMDB eliminates this entirely.

### 13.3 Columnar File Layout

The partition index is no longer embedded in the file header — it lives entirely in LMDB. The columnar file is pure data:

```
ts_click.col

┌─────────────────────────────────────────────────────────┐
│  FILE HEADER (32 bytes)                                  │
│  magic=0x46455254, version=1, series_hash, sample_count  │
├─────────────────────────────────────────────────────────┤
│  TIMESTAMP COLUMN                                        │
│  All N timestamps, contiguous, growing at tail           │
│  Open partition: raw 8-byte big-endian int64             │
│  Sealed partition: ALP/Sprintz encoded in-place          │
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

The file header shrinks from 64 bytes to 32 bytes because `partition_index_offset` is gone — that information now lives in LMDB. New samples append to the open partition tail. The file only grows forward.

### 13.4 Two Levels of Pruning Before Any Columnar File Is Touched

```
TS.FILTER ts:temperature FROM -90d TO now WHERE value > 37.5

Level 1 — LMDB time pruning (in memory, ~1μs per series):
  Scan LMDB entries for ts:temperature
  ts_max < query_from → skip (partition predates the query window)
  → Prunes irrelevant day partitions

Level 2 — LMDB value pruning (same LMDB scan, zero extra cost):
  val_max < 37.5 → skip (no sample in this partition exceeds threshold)
  → Prunes days where temperature was always below 37.5

io_uring submission:
  Only the byte ranges for partitions that passed both checks
  Potentially 5–10% of total file bytes for selective queries
```

Both levels of pruning happen inside the LMDB B-tree scan — one pass, no disk reads, before any columnar file is opened.

### 13.5 Partition Sealing — Updated

When a day boundary passes (+ grace period), the `PartitionSealer` GenServer:

1. Read the open partition's raw bytes from the columnar file
2. ALP-encode the timestamp column in a Tokio task
3. ALP-encode the value column in a parallel Tokio task
4. Compute `val_min`, `val_max` over the value column (one pass, free)
5. Write encoded bytes back in-place (always smaller — safe)
6. **Write one LMDB entry** with `ts_min`, `ts_max`, `val_min`, `val_max`, offsets, `sealed=true`
7. Update Bitcask entry's `sample_count` field

The `val_min` / `val_max` computation is free — it's a single pass over the value column that's already in memory for encoding. These values then live in LMDB forever, enabling value pruning on every future query with zero additional cost.

### 13.6 Read Path — Updated

**Single-series range query:**
```
TS.RANGE ts:click FROM -7d TO now

1. Bitcask lookup "ts:click" → file path                        (~50ns)
2. LMDB range scan: series_hash + day_range → 7 PartitionEntry  (~5μs)
3. io_uring: submit 7 byte ranges in one batch                   (~100μs + I/O)
4. Tokio: ALP decode + filter to exact timestamp range
5. Return results
```

**Cross-series range query (10K series):**
```
TS.MRANGE FROM -30d TO now FILTER event_type=click WHERE value > 1000

1. ETS label lookup → 10K matching series                        (~1ms)
2. LMDB scan per series:
     For each series: scan 30 day entries
     val_max < 1000 → skip partition (value pruning in LMDB)
     Remaining: maybe 3K series × ~10 days = 30K byte ranges    (~50ms)
3. io_uring: 30K reads submitted in one batch                    (one ring write)
4. NVMe serves in parallel
5. Tokio: 30K parallel ALP decode + AGG tasks
6. Coordinator merges results
```

The `WHERE value > 1000` prunes entire day partitions in LMDB before any columnar file read. A series where click counts are always < 1000 never touches disk at all.

### 13.7 Full Storage Architecture

```
ETS:
  :ts_label_index    — series_name → label set      (small, rebuild cheap on restart)
  :ts_series_meta    — series_name → file path, codec, retention
  :ts_hot_cache      — recent raw samples            (volatile by design)
  :ts_warm_cache     — recently decoded partitions   (volatile by design)

LMDB  (meta/partition_index.lmdb):
  (series_hash, day) → PartitionEntry               (durable, mmap-fast, ~68MB for 10K×90d)

Columnar files  (data/ts/*.col):
  All sample data — timestamp + value columns        (grows at tail, append-only)

Bitcask:
  Series registry — "ts:click" → file path + codec  (one entry per series)
```

Each layer owns exactly what it is good at. ETS owns hot volatile state. LMDB owns durable queryable metadata. Columnar files own raw data. Bitcask owns the series registry.

### 13.8 Elixir / Rust Boundary

Rust NIFs exist for exactly two reasons: SIMD instructions (which Elixir cannot access) and io_uring (no Elixir interface exists). Everything else is Elixir — the orchestration, the background encoding, the codec decisions, the file management.

**Rule:** if it runs in the background and the user never waits for it, it belongs in Elixir. If it runs on the query hot path and the user waits for it, it belongs in Rust.

**Rust NIFs — hot path only:**

| NIF | Why Rust | What it does |
|---|---|---|
| `ts_scan_range(fd, from_off, to_off, agg, bucket_ms)` | SIMD decode + io_uring | Seek, read, AVX2 decode, aggregate — user waits |
| `ts_simd_filter(bytes, op, threshold)` | SIMD value scan | TS.FILTER hot path — user waits |
| `ts_lmdb_scan(hash, from_day, to_day, val_min, val_max)` | LMDB heed binding | Range scan partition index |
| `ts_lmdb_upsert(hash, day, entry)` | LMDB heed binding | Write one partition entry |

**Elixir — background work:**

| Module | What it does | Why Elixir is fine |
|---|---|---|
| `TS.PartitionSealer` GenServer | Decides when to seal, orchestrates steps | Orchestration logic, not compute |
| `TS.Encoding` | Delta-delta, ALP encode, NeaTS fit | Background, async — 200ms is acceptable |
| `TS.CodecProbe` | Measures smoothness, picks codec | Math on 1000 samples — ~1ms in Elixir |
| `TS.FileWriter` | Appends encoded bytes to `.col` file | `File.write!` — fine for background |
| `TS.CompactionWorker` | Rollup rules | Orchestration |
| `TS.RetentionSweeper` | Deletes expired partitions | Infrequent, background |
| `TS.Counter` | Accumulates TS.INCR in ETS | Pure ETS — Elixir native |

**Memory model advantage of Elixir for background work:**

Elixir binaries > 64 bytes live on the shared heap with reference counting. A 30MB columnar partition read in a sealing Task is one allocation. When the Task finishes, the GC frees it automatically. No manual deallocation, no memory outside the BEAM's visibility, no hidden growth. Rust NIF buffers live outside the BEAM heap — if you use Rust for background work you must be careful about when they're freed. For work the user never waits for, Elixir's memory model is strictly safer.

`ts_scan_range` — the only place where speed justifies Rust:

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
        let ts     = sprintz_decode(&bytes.ts_section);                     // SIMD
        let result = aggregate(ts, values, agg, bucket_ms);
        send_result(pid, result);
    });
    Ok(atoms::ok().encode(env))
}
```

The `TS.PartitionSealer` in Elixir that calls these:

```elixir
defmodule FerricStore.TS.PartitionSealer do
  use GenServer

  # Runs as a Task — spawns, works, exits. GC cleans up.
  def seal(series_name, day) do
    Task.start(fn ->
      raw = File.read!(col_file_path(series_name))
      {timestamps, values} = parse_raw_partition(raw, day)

      # Codec probe — pure Elixir math, ~1ms
      codec = FerricStore.TS.CodecProbe.pick(values)

      # Encoding — pure Elixir, background, slow is fine
      encoded_ts  = FerricStore.TS.Encoding.encode_timestamps(timestamps, codec)
      encoded_val = FerricStore.TS.Encoding.encode_values(values, codec)

      # val_min/val_max computed during encode — free, one pass
      {val_min, val_max} = FerricStore.TS.Encoding.minmax(values)

      # Write encoded bytes — Elixir File.write
      write_partition(series_name, day, encoded_ts, encoded_val)

      # LMDB update — thin Rust NIF, ~1μs
      Nif.ts_lmdb_upsert(
        series_hash(series_name), day_number(day),
        build_entry(encoded_ts, encoded_val, timestamps, val_min, val_max, codec)
      )
    end)
  end
end
```

---

## 14. New Command Surface

> The columnar file model enables a new analytical command surface that RedisTimeSeries cannot offer. These commands are pushed down to the NIF — computation happens server-side, not in application code.

### 14.1 `TS.RANGE` Extensions

Existing command gains new clauses:

```
TS.RANGE key FROM from TO to
  [AGG type BUCKET ms]        existing
  [WHERE value op threshold]  new — value filter, SIMD-evaluated
  [FILL value]                new — fill empty buckets (0 = dashboard-safe)
  [ALIGN CALENDAR]            new — buckets start at midnight, not relative to FROM
  [LIMIT n OFFSET n]          new — pagination
```

`WHERE` is evaluated against the value column before decoding timestamps. For a 90-day sealed partition, SIMD processes 8 values per instruction — misses cost almost nothing.

```
TS.RANGE ts:errors FROM -90d TO now WHERE value > 0 AGG count BUCKET 1d FILL 0
→ "daily error-day count for the last 90 days, including zero days"
```

### 14.2 `TS.FILTER` — Sample-Level Value Filtering

```
TS.FILTER key FROM from TO to WHERE expr [LIMIT n]

TS.FILTER ts:temperature  FROM -7d  TO now  WHERE value > 37.5
TS.FILTER ts:revenue      FROM -30d TO now  WHERE value > 10000
TS.FILTER ts:errors       FROM -1h  TO now  WHERE value != 0
TS.FILTER ts:latency      FROM -24h TO now  WHERE value > 500 AND value < 5000
```

Returns raw `(timestamp, value)` pairs matching the predicate. Useful for finding anomalous samples or feeding into alerting pipelines. The `WHERE` predicate is evaluated column-by-column using SIMD — only matching timestamps are decoded.

### 14.3 `TS.COMPUTE` — Cross-Series Arithmetic

```
TS.COMPUTE "expr" FROM from TO to [AGG type BUCKET ms]

# Ratio (conversion rate)
TS.COMPUTE "ts:purchase / ts:click" FROM -30d TO now AGG avg BUCKET 1d

# Delta (profit)
TS.COMPUTE "ts:revenue - ts:cost" FROM -90d TO now AGG sum BUCKET 1w

# Normalised CTR
TS.COMPUTE "ts:click / ts:impressions * 100" FROM -7d TO now AGG avg BUCKET 1h

# Multi-region total
TS.COMPUTE "ts:eu_clicks + ts:us_clicks + ts:ap_clicks" FROM -24h TO now AGG sum BUCKET 1h
```

Series must share the same or alignable sample rate. If bucket granularity is specified, series are aggregated to that bucket before arithmetic — avoids requiring exact timestamp alignment across series.

Supported operators: `+`, `-`, `*`, `/`. Expression is a quoted string, parsed once at command execution. No SQL parser required — a simple recursive descent parser over the four operators.

### 14.4 `TS.STAT` — Analytical Queries

```
# Percentile within a series
TS.STAT key PERCENTILE n FROM from TO to [BUCKET ms]
TS.STAT ts:response_time PERCENTILE 95 FROM -24h TO now
TS.STAT ts:response_time PERCENTILE 99 FROM -24h TO now BUCKET 1h

# Top-N / Bottom-N series by aggregate value
TS.TOPN  n FILTER label=value AGG type FROM from TO to
TS.BOTN  n FILTER label=value AGG type FROM from TO to
TS.TOPN  10 FILTER event_type=click AGG sum FROM -24h TO now
TS.BOTN  5  FILTER device_type=sensor AGG avg FROM -7d TO now

# Correlation between two series
TS.CORR ts:clicks ts:purchases FROM -30d TO now [BUCKET ms]
→ returns {coefficient, lag_ms, sample_count}

# Anomaly detection — samples outside N std-devs from rolling mean
TS.ANOMALY key STDDEV n WINDOW window_ms FROM from TO to
TS.ANOMALY ts:errors STDDEV 3 WINDOW 3600000 FROM -7d TO now
→ returns [(timestamp, value, zscore), ...]
```

All of these are computed server-side in the NIF on the decoded value column. `PERCENTILE` uses a t-digest approximation for large ranges. `TOPN`/`BOTN` use a min-heap, never materialising the full sorted list. `CORR` uses Pearson correlation on bucket-aligned values. `ANOMALY` uses a single-pass rolling mean + variance algorithm (Welford's method).

### 14.5 `TS.PIPELINE` — Chained Operations

```
TS.PIPELINE [FILTER label=value ...]
  | RANGE FROM from TO to
  | WHERE value op threshold
  | AGG type BUCKET ms
  | COMPUTE expr
  | TOPN n
  | LIMIT n [OFFSET n]
```

Each stage feeds into the next. The pipeline is parsed once and executed as a single NIF call — no round-trips between stages.

```
# "Which hours last week had conversion rate above 5%?"
TS.PIPELINE
  | COMPUTE "ts:purchase / ts:click * 100" FROM -7d TO now
  | AGG avg BUCKET 1h
  | WHERE value > 5
  | LIMIT 20

# "Daily active users last 30 days, top 10 days"
TS.PIPELINE FILTER event_type=login
  | RANGE FROM -30d TO now
  | AGG count BUCKET 1d FILL 0
  | TOPN 10
```

The pipeline is linear — no branching, no subqueries. This keeps the parser trivial while covering the large majority of real product analytics queries.

### 14.6 Command Summary

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

## 15. Compression Research — Algorithm Selection

> The current spec uses Gorilla XOR for values and delta-delta for timestamps. Three recent papers (2022–2025) provide better alternatives. This section maps the research to concrete implementation decisions.

### 15.1 ALP — Adaptive Lossless Floating-Point Compression (SIGMOD 2024)

**Paper:** Afroozeh, Kuffó, Boncz — CWI Amsterdam. MIT licensed. Already adopted in DuckDB.

**The insight:** Most real-world floats are either decimal numbers (42.0, 37.5, 100.0) or high-precision scientific values. ALP handles each case separately:

```
Case 1 — Decimal floats (event counts, metrics):
  42.0 → multiply by 10^1 → integer 420
  Frame-of-Reference + FastLanes bit-pack
  SIMD-native from the ground up — 8 values per AVX2 instruction

Case 2 — High-precision floats (sensor readings):
  Split 64-bit float into left bits + right bits
  Left: dictionary compress (few unique patterns)
  Right: bit-pack to minimum necessary bits
```

**Gain over Gorilla:** 1–2 orders of magnitude faster compression and decompression. Similar or better compression ratio. Crucially — ALP is designed for SIMD first, not retrofitted. The value column becomes a single SIMD-friendly flat array.

**For FerricStore:** FerricStore's event count data (integers stored as float64 — 1, 5, 42, 1000) is almost entirely Case 1. ALP maps these to integers and bit-packs them. For a series where values are consistently `< 1000`, ALP reduces the value column to ~2–3 bits per sample after Frame-of-Reference.

**Action:** Replace Gorilla with ALP in `ts_seal_partition` NIF for the value column. The C++ reference implementation is MIT-licensed and available at `github.com/cwida/ALP`. Wrap as a Rust NIF via FFI or reimplement the core in Rust (~500 lines).

### 15.2 Chimp — Improved Gorilla (VLDB 2022)

**The fix:** Gorilla tracks leading zeros but ignores trailing zeros. Chimp tracks both. For smooth metric data where consecutive values are close (37.5, 37.6, 37.5), Chimp achieves ~30% better compression than Gorilla.

**For FerricStore:** Chimp is a lower-risk intermediate step before ALP. If implementing ALP first is too much scope, Chimp is a 200-line change to the existing Gorilla NIF that delivers meaningful improvement immediately.

### 15.3 NeaTS — Learned Compression with Random Access (ICDE 2025)

**Paper:** Guerra, Vinciguerra, Boffa, Ferragina. Open source at `github.com/and-gue/NeaTS`.

**The core idea:** Instead of encoding sample-by-sample, NeaTS fits the time series with a sequence of nonlinear functions (linear, quadratic, exponential, logarithmic) plus small bounded residuals:

```
Samples 0–450:    y ≈ 42x + 0.1      (linear trend)
Samples 451–890:  y ≈ 42 * e^0.001x  (exponential growth)
Residuals: tiny, stored in few bits each
```

**The critical property for FerricStore:** random access into compressed data. With Gorilla or ALP, reading sample 10,000 requires decoding samples 1–9,999 sequentially. With NeaTS, reading sample 10,000 means: look up which function covers position 10,000 in the function index, evaluate the function, add the residual. **O(log n) access to any sample.**

**Impact on queries:**

```
TS.RANGE ts:click FROM 09:30 TO 09:35
  (query 5 minutes inside a sealed 1-hour partition)

With ALP/Gorilla:
  Decode entire partition from start → filter to range
  3600 samples decoded, ~300 needed → 12× wasted work

With NeaTS:
  Binary search function index for 09:30 offset
  Evaluate function + residuals for those ~300 samples only
  12× less decode work for narrow range queries
```

**For TS.FILTER WHERE value > x:** NeaTS lets you evaluate the filter against the function approximation first. If the entire function segment is below the threshold, skip it without decoding residuals. Fast rejection of irrelevant regions.

**Action:** Implement NeaTS as an optional codec alongside ALP. Select per-series based on query pattern:
```
TS.CREATE ts:click COMPRESSION ALP       ← full-range scans, bulk AGG
TS.CREATE ts:temperature COMPRESSION NEATS  ← frequent narrow-range queries
TS.CREATE ts:click COMPRESSION AUTO      ← probe on first seal, pick best
```

### 15.4 Sprintz — SIMD-Native Integer Compression (for timestamps)

FerricStore's timestamp column uses delta-delta encoding. Sprintz replaces this with a vectorized forecasting + bit-packing + RLE pipeline that can decompress at up to 3GB/s in a single thread.

For FerricStore's regular 1-second event data (delta-of-deltas are all zero), Sprintz's run-length encoding step collapses the entire regular region to a single run-length entry. A 3600-sample hour of regular timestamps compresses to essentially constant size regardless of sample count.

**Action:** Use Sprintz for the timestamp column when sample regularity is detected at seal time (variance of delta-of-deltas < threshold). Fall back to delta-delta for irregular event streams.

### 15.5 Codec Auto-Selection — Elixir Probe

The codec decision runs in Elixir at seal time — one extra pass over the value column that's already in memory. No Rust needed. The probe adds ~1ms to a seal that takes ~200ms anyway.

**The decision tree:**

```elixir
defmodule FerricStore.TS.CodecProbe do

  def pick(values) do
    # Step 1 — measure smoothness via delta variance
    smoothness = measure_smoothness(values)

    cond do
      smoothness < 0.3 ->
        # High variance — bursty data (likes, hates, errors)
        # NeaTS function fit will be poor, residuals stay large
        :alp

      smoothness >= 0.3 ->
        # Low variance — smooth data (views, temperature, revenue)
        # Check if NeaTS actually wins on a sample
        case neats_fit_quality(Enum.take(values, 1000)) do
          ratio when ratio < 0.05 -> :neats_alp   # excellent fit
          ratio when ratio < 0.20 -> :neats_alp   # good fit
          _                       -> :alp          # poor fit, not worth it
        end
    end
  end

  defp measure_smoothness(values) do
    deltas = values |> Enum.chunk_every(2, 1, :discard)
                    |> Enum.map(fn [a, b] -> b - a end)
    mean   = Enum.sum(deltas) / length(deltas)
    abs_mean = max(abs(mean), 1.0)
    variance = deltas |> Enum.map(fn d -> (d - mean) * (d - mean) end)
                      |> Enum.sum()
                      |> Kernel./(length(deltas))
    # Returns 0.0 (very bursty) to 1.0 (perfectly smooth)
    1.0 / (1.0 + :math.sqrt(variance) / abs_mean)
  end

  defp neats_fit_quality(sample) do
    # Fit a linear approximation to the sample
    n      = length(sample)
    xs     = Enum.to_list(0..(n - 1))
    {a, b} = linear_fit(xs, sample)
    # Measure residual variance vs raw variance
    predicted  = Enum.map(xs, fn x -> a * x + b end)
    residuals  = Enum.zip(sample, predicted) |> Enum.map(fn {v, p} -> v - p end)
    raw_var    = variance(sample)
    resid_var  = variance(residuals)
    # ratio close to 0 = NeaTS fits well, ratio close to 1 = no better than raw
    if raw_var == 0, do: 0.0, else: resid_var / raw_var
  end
end
```

**What this looks like in practice for your events:**

```
ts:views:post:123    → smoothness=0.85, neats_fit=0.03 → :neats_alp
ts:likes:post:123    → smoothness=0.08                 → :alp
ts:hates:post:123    → smoothness=0.04                 → :alp
ts:comments:post:*   → smoothness=0.12                 → :alp
ts:revenue           → smoothness=0.78, neats_fit=0.06 → :neats_alp
ts:errors            → smoothness=0.02                 → :alp
```

The system figures this out from the data. The user never configures it.

**Codec lock-in after 3 consistent partitions:**

```elixir
defp maybe_lock_codec(series_name, new_codec) do
  meta = ETS.get(:ts_series_meta, series_name)

  cond do
    meta.codec_locked ->
      # User forced a codec via COMPRESSION clause — never change
      meta.preferred_codec

    meta.codec_history |> Enum.take(3) |> Enum.all?(&(&1 == new_codec)) ->
      # 3 consistent partitions — lock it in, skip probe from now on
      ETS.update(:ts_series_meta, series_name, %{preferred_codec: new_codec, codec_locked: true})
      new_codec

    true ->
      # Still probing — record this partition's choice
      ETS.update(:ts_series_meta, series_name,
        %{codec_history: [new_codec | meta.codec_history]})
      new_codec
  end
end
```

**Series that change behaviour** (post goes viral — smooth views suddenly spike):

```
Partitions 1–90:  :neats_alp (smooth growth, codec locked)
Partition 91:     probe re-runs (smoothness drops below 0.3)
                  → temporarily :alp for this partition
                  codec_locked = false, re-evaluating
Partition 92:     stabilises at new higher baseline, smooth again
                  → :neats_alp
                  → lock after 3 consistent again
```

Per-partition codec in LMDB handles this cleanly. Historical partitions keep their original codec. The decode path reads the `codec` byte from the LMDB entry and uses the right decoder per partition — mixed codecs within one series file are fully supported.

### 15.6 TS.CREATE Codec Options

```
TS.CREATE key
  [COMPRESSION AUTO | ALP | NEATS+ALP | CHIMP]

AUTO      default — probe at first seal, lock after 3 partitions
ALP       force ALP for all partitions, codec_locked=true
NEATS+ALP force combined codec, codec_locked=true
CHIMP     force Chimp (lighter than ALP, good intermediate option)
```

`AUTO` is the right default for all users. The explicit options are escape hatches for operators who know their data distribution will never change.

### 15.7 Implementation Priority

```
Phase 1 — ALP decode in Rust NIF, ALP encode in Elixir:
  ts_scan_range NIF: AVX2 ALP decode (Rust, ~500 lines)
  TS.Encoding.encode_values: ALP encode (Elixir, background)
  MIT-licensed C++ reference impl to guide the Rust port
  Biggest immediate gain — SIMD decode 10× faster than scalar

Phase 2 — Sprintz timestamp decode in Rust, encode in Elixir:
  ts_scan_range NIF: add Sprintz decode path
  TS.Encoding.encode_timestamps: Sprintz encode (Elixir, background)
  6× faster timestamp decode

Phase 3 — NeaTS function fitting in Elixir, residual decode in Rust:
  TS.Encoding.neats_fit: function partitioning (Elixir)
  ts_scan_range NIF: NeaTS random access path (Rust)
  Enables O(log n) access for TS.FILTER and narrow TS.RANGE

Phase 4 — AUTO codec selection:
  TS.CodecProbe module (pure Elixir)
  COMPRESSION AUTO default on TS.CREATE
  Per-partition codec in LMDB entry
  Viral series re-probe on behaviour change
```

---

## 16. Elixir / Rust Design Principle

> This section is the canonical reference for where code lives. When adding new functionality, apply this principle before writing any code.

### 16.1 The Rule

**Rust if and only if:**
1. The operation requires SIMD (AVX2) — Elixir has no access to SIMD instructions
2. The operation requires io_uring — no Elixir interface exists
3. The operation is on the query hot path AND the user is waiting for it

**Elixir for everything else.**

### 16.2 Why This Matters

Rust NIFs allocate memory outside the BEAM heap. The GC cannot see these allocations. If a NIF is used for background work, the developer must manually manage when memory is freed. Mistakes cause silent memory growth that the BEAM's introspection tools cannot detect.

Elixir processes allocate on the BEAM heap. When a process exits, everything is freed. A `Task` that seals one partition — spawns, reads 30MB of data, encodes it, writes it back, exits — leaves zero memory behind. No manual cleanup. No hidden growth.

```
Background work lifecycle:

Elixir Task:
  spawn → allocate on BEAM heap → work → exit → GC frees everything
  Duration: 200ms  Memory after: 0

Rust NIF (wrong use):
  call → allocate Rust-side buffer → return → ... who frees the buffer?
  Requires careful ResourceArc + Drop implementation
  Easy to get wrong, silent if wrong
```

### 16.3 Full Boundary Table

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

### 16.4 The Nx Option for NeaTS

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

This stays in Elixir, runs fast enough for background work, and avoids any Rust allocation for the fitting step. The residuals it produces are then encoded by the ALP path — also in Elixir at seal time.
---

## 17. Retention — Full Specification

> Previous sections mention `RETENTION ms` and a `RetentionSweeper` GenServer but do not spec how deletion works with the columnar file model. This section is the authoritative design.

### 17.1 The Problem With Columnar Files and Retention

With the old sealed-chunk model, deleting expired data was trivial: delete the chunk file. One `File.rm!`, done.

With a columnar file, old data is at the **start** of the file and new data is at the **tail**. You can't delete from the front of a file without either rewriting the entire file or using a different strategy.

```
ts_views_post_123.col

[2024-01-01 partition]  ← oldest, expired, want to delete
[2024-01-02 partition]  ← expired
[2024-01-03 partition]  ← expired
...
[2024-03-31 partition]  ← current, keep
```

Three strategies, each with different tradeoffs:

**Option A — Rewrite the file** (delete expired partitions, write new file)
- Correct but expensive: rewrites potentially hundreds of MB
- Causes a read blackout window while rewriting
- Not viable for frequent retention sweeps

**Option B — Tombstone partitions in LMDB, leave file unchanged**
- Mark expired partitions as deleted in the LMDB partition index
- The file grows unboundedly — disk space is never reclaimed
- Not viable long-term

**Option C — Sparse file + hole punching (chosen approach)**
- Use `fallocate(FALLOC_FL_PUNCH_HOLE)` to punch holes in the file
- The OS returns those pages to the filesystem — disk space is reclaimed
- The file inode still exists and still has its original size in bytes
- But the punched regions are "sparse" — they consume zero actual disk blocks
- Reads from punched regions return zeroes — safe, detectable

This is the correct approach. It is O(1) per partition (one syscall), reclaims disk immediately, and does not require rewriting any live data.

### 17.2 How Hole Punching Works

```rust
// Rust NIF: punch a hole in the columnar file for an expired partition
fn ts_punch_partition(fd: RawFd, ts_offset: u64, ts_len: u64,
                      val_offset: u64, val_len: u64) -> Result<(), Error> {
    // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
    // Reclaims disk blocks, preserves file size, returns zeros on read
    fallocate(fd, FallocFlags::PUNCH_HOLE | FallocFlags::KEEP_SIZE,
              ts_offset as i64, ts_len as i64)?;
    fallocate(fd, FallocFlags::PUNCH_HOLE | FallocFlags::KEEP_SIZE,
              val_offset as i64, val_len as i64)?;
    Ok(())
}
```

The LMDB partition entry is then deleted — the partition index no longer knows this partition exists, so it is never included in query results. The holes in the file are unreachable dead space.

**Disk space reclamation is immediate.** The filesystem returns the blocks to the free pool as soon as `fallocate` returns. No background compaction needed.

**Linux kernel requirement:** `fallocate` with `PUNCH_HOLE` is supported on ext4, XFS, btrfs, and NTFS (kernel ≥ 2.6.38). On NVMe-backed filesystems this is a metadata-only operation — fast regardless of partition size.

### 17.3 RetentionSweeper GenServer

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
      cutoff_day = day_number(cutoff_ms)

      # LMDB range scan: all partitions for this series older than cutoff
      expired = Nif.ts_lmdb_scan_before(series_hash(name), cutoff_day)

      Enum.each(expired, fn entry ->
        # Punch holes in columnar file — Rust NIF, ~1μs per partition
        Nif.ts_punch_partition(
          file_fd(file_path),
          entry.ts_offset, entry.ts_len,
          entry.val_offset, entry.val_len
        )
        # Delete from LMDB partition index — now invisible to queries
        Nif.ts_lmdb_delete(series_hash(name), entry.day)

        # Telemetry
        :telemetry.execute(
          [:ferricstore, :ts, :retention, :dropped],
          %{bytes_freed: entry.ts_len + entry.val_len},
          %{series: name, day: entry.day}
        )
      end)
    end)

    schedule_next_sweep(state)
    {:noreply, state}
  end
end
```

One LMDB scan + one hole punch per expired partition. No file rewriting. No read blackout. Safe to run while queries are in-flight.

### 17.4 Granularity — Day-Level, Not Sample-Level

Retention deletes entire day partitions, not individual samples. This means:

```
RETENTION 30d  →  delete any partition where day < today - 30

Series with samples at:
  2024-01-01 09:00  ← in the deleted partition (day -90)
  2024-01-01 23:59  ← also deleted (same partition)
  2024-03-02 00:01  ← kept (day -29, within retention)
```

A sample at `2024-01-01 23:59` that is 89 days old is deleted along with everything else from that day, even though `30d` might technically mean it falls on the edge. This is intentional — day-granularity is predictable, fast, and matches how operators think about retention ("keep 30 days of data", not "keep exactly 2,592,000,000ms of data").

Operators who need finer granularity can set `RETENTION` to a smaller value and adjust the sweep interval.

### 17.5 Per-Series Retention Policies

Different event types in your system warrant different retention periods:

```
# Views — high volume, need long history for trend analysis
TS.CREATE ts:views:post:* RETENTION 90d HOT_WINDOW 24h COMPRESSION AUTO

# Likes/hates — low volume, keep longer (meaningful signals)
TS.CREATE ts:likes:post:* RETENTION 365d HOT_WINDOW 24h COMPRESSION ALP

# Comments — medium volume, medium retention
TS.CREATE ts:comments:post:* RETENTION 180d HOT_WINDOW 24h COMPRESSION ALP

# Trending scores (computed) — keep short, recomputable
TS.CREATE ts:trending:* RETENTION 7d HOT_WINDOW 2h COMPRESSION AUTO

# Leaderboard snapshots — keep 90 days
TS.CREATE ts:leaderboard:* RETENTION 90d HOT_WINDOW 1h COMPRESSION ALP
```

Retention is stored in the Bitcask series entry and the ETS `:ts_series_meta` table. The `RetentionSweeper` reads it from ETS — no disk access needed to decide what to sweep.

### 17.6 Changing Retention on an Existing Series

```
TS.ALTER key RETENTION new_retention_ms

# Extend retention — no immediate action, future sweeps use new value
TS.ALTER ts:views:post:* RETENTION 180d

# Shorten retention — triggers immediate sweep for the newly expired range
TS.ALTER ts:views:post:* RETENTION 30d
```

`TS.ALTER` updates the Bitcask entry and ETS metadata. If retention is shortened, the `RetentionSweeper` is signalled to run immediately for that series rather than waiting for the next scheduled sweep.

### 17.7 Rollup Retention — Keep Aggregates Longer Than Raw

A common pattern: keep raw data for 30 days but keep hourly rollups for 1 year.

```
# Raw series — 30 days
TS.CREATE ts:views:post:123 RETENTION 30d

# Hourly rollup — 1 year
TS.CREATE ts:views:post:123:per_hour RETENTION 365d

# Daily rollup — 5 years
TS.CREATE ts:views:post:123:per_day RETENTION 1825d

# Compaction rules generate rollup series automatically
TS.CREATERULE ts:views:post:123 ts:views:post:123:per_hour AGG sum BUCKET 1h
TS.CREATERULE ts:views:post:123:per_hour ts:views:post:123:per_day AGG sum BUCKET 1d
```

Each series has its own RETENTION. The compaction rules feed aggregated data into the rollup series independently. Raw data expires at 30 days. Hourly summaries survive for a year. Daily summaries survive for 5 years. The user can always answer "what was the daily view count for this post 2 years ago?" even though the raw second-by-second data is long gone.

### 17.8 Disk Cost With Retention

With NEATS+ALP compression and day-level retention:

| Series type | Retention | Samples/day | Codec | Disk per series |
|---|---|---|---|---|
| `ts:views:post:*` | 90d | 86,400 | NEATS+ALP | ~90 × 28KB = 2.5MB |
| `ts:likes:post:*` | 365d | ~100 (sparse) | ALP | ~365 × 0.8KB = 292KB |
| `ts:hates:post:*` | 365d | ~20 (very sparse) | ALP | ~365 × 0.16KB = 58KB |
| `ts:views:post:*:per_hour` | 365d | 24 | NEATS+ALP | ~365 × 2KB = 730KB |
| `ts:views:post:*:per_day` | 1825d | 1 | ALP | ~1825 × 80B = 146KB |

For 1 million posts × all series types:

```
ts:views     (raw):        1M × 2.5MB  = 2.5TB
ts:likes:                  1M × 292KB  = 292GB
ts:hates:                  1M × 58KB   = 58GB
ts:comments:               1M × 150KB  = 150GB
ts:views:per_hour:         1M × 730KB  = 730GB
ts:views:per_day:          1M × 146KB  = 146GB

Total:                                 ~3.9TB
```

On a 3-node Raft cluster: 3.9TB × 3 (replication) = ~11.7TB total. At ~$0.10/GB NVMe = ~$1,170/month.

Compare to RedisTimeSeries storing the same 90-day views history in RAM: 2.5TB × $5/GB = $12,500/month just for RAM — and that excludes the rollup series.

### 17.9 Retention vs Compaction — Not The Same Thing

Worth being explicit since these are often confused:

| | Retention | Compaction (Bitcask merge) |
|---|---|---|
| What it removes | Expired time series partitions | Dead Bitcask entries (tombstones, overwritten keys) |
| When it runs | RetentionSweeper, hourly | MergeWorker, triggered by dead byte ratio |
| Mechanism | LMDB delete + hole punch | Bitcask file rewrite |
| Affects | Columnar `.col` files + LMDB | Bitcask `.cask` files |
| User-visible | Data disappears after RETENTION window | Disk space reclaimed, no visible change |

They run independently. Retention cleans up the columnar TS data. Bitcask merge cleans up the series registry (Bitcask entries for deleted series, overwritten metadata). Both are necessary. Neither replaces the other.

---

## 18. WAL-Backed Internals — Replay, Reconstruction, and Compaction Rules

> The Raft WAL gives FerricStore TS capabilities most time series databases cannot offer. This section documents how they work internally — none of these require new user-facing commands except where noted.

### 18.1 Two-Layer Storage Model

Every `TS.ADD` passes through Raft before being applied to ETS and the columnar file:

```
Client: TS.ADD ts:views:post:123 1700000001000 42

Raft WAL entry:
  {index: 18847, term: 3, command: {ts_add, "ts:views:post:123", 1700000001000, 42.0}}

Applied to:
  ETS hot cache     ← volatile, evicts after HOT_WINDOW
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

### 18.2 Automatic Columnar File Reconstruction

On every read from a sealed partition, the NIF verifies a checksum stored in the LMDB partition entry. On mismatch:

```elixir
defmodule FerricStore.TS.Reconstruction do

  def rebuild_from_wal(series_name) do
    # 1. Delete the corrupted columnar file
    File.rm!(col_file_path(series_name))

    # 2. Create a fresh empty file
    file_fd = File.open!(col_file_path(series_name), [:write, :binary])

    # 3. Walk the Raft WAL for this series
    #    ra provides log iteration from a given index
    :ra.log_fold(
      server_ref(),
      fn
        {_index, _term, {ts_add, ^series_name, ts, val}} ->
          # Re-apply each event to the fresh file
          FerricStore.TS.FileWriter.append(file_fd, ts, val)
        _ -> :ok   # skip entries for other series or other command types
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

The user never sees this. The read that detected corruption blocks briefly while reconstruction runs, then serves the correct data. Telemetry fires so operators are alerted:

```elixir
:telemetry.execute(
  [:ferricstore, :ts, :reconstruction, :completed],
  %{duration_ms: elapsed, entries_replayed: count},
  %{series: series_name, trigger: :checksum_mismatch}
)
```

### 18.3 TS.CREATERULE Retroactive Backfill — The Three Cases

When `TS.CREATERULE` is called on a series that already has history, FerricStore attempts to backfill the destination series. The quality of backfill depends on how much WAL remains and the relationship between the rule's bucket size and the sealed partition granularity.

**The WAL cutoff:**
```elixir
wal_cutoff_ms = System.system_time(:millisecond) - log_retention_hours() * 3_600_000
```

**Case 1 — Full WAL coverage (series younger than log_retention_hours):**
```
Series started 3 days ago. WAL retention = 7 days.
All events still in WAL.

TS.CREATERULE ts:views ts:views:per_hour AGG sum BUCKET 3600000

Action: replay all ts_add events for ts:views through the AGG function
Result: per_hour series populated from day 1, event-accurate
```

**Case 2 — Partial WAL, bucket ≥ 1 day (good quality backfill):**
```
Series started 90 days ago. WAL retention = 7 days.
Days 1–83: only sealed columnar partitions (day-level granularity)
Days 84–90: WAL entries exist

TS.CREATERULE ts:views ts:views:per_week AGG sum BUCKET 604800000

Action:
  Phase A — columnar backfill (days 1–83):
    Read sealed day-level partitions
    Re-aggregate: 7 day values → 1 week value (sum of sums)
    Lossless for sum, count, min, max
    Approximate for avg (correct if row counts stored in partition entry)

  Phase B — WAL backfill (days 84–90):
    Replay raw ts_add events through AGG function
    Exact values

Result: per_week series populated from day 1
        exact for days 84–90, re-aggregated for days 1–83
```

**Case 3 — Partial WAL, bucket < 1 day, history beyond WAL (cannot backfill):**
```
Series started 90 days ago. WAL retention = 7 days.
User wants hourly rollup.
Days 1–83: only day-level sealed partitions — cannot disaggregate to hours

TS.CREATERULE ts:views ts:views:per_hour AGG sum BUCKET 3600000

Action:
  Cannot reconstruct hourly data from day-level partitions.
  Phase A — skip. Mark as missing.
  Phase B — WAL backfill (days 84–90): exact.

Result: per_hour series starts from day 84.
        Days 1–83 are marked as missing in TS.INFO.
        Return value includes a warning.
```

**Implementation:**

```elixir
defmodule FerricStore.TS.CompactionRule do

  def createrule(src, dst, agg, bucket_ms) do
    wal_cutoff    = wal_cutoff_ms()
    series_start  = get_series_first_timestamp(src)
    one_day_ms    = 86_400_000

    coverage =
      cond do
        series_start >= wal_cutoff ->
          # Case 1: full WAL coverage
          replay_from_wal(src, dst, agg, bucket_ms, series_start, now_ms())
          %{type: :full_wal, exact_from: series_start}

        bucket_ms >= one_day_ms ->
          # Case 2: partial WAL, bucket >= 1 day — columnar backfill viable
          backfill_from_columnar(src, dst, agg, bucket_ms, series_start, wal_cutoff)
          replay_from_wal(src, dst, agg, bucket_ms, wal_cutoff, now_ms())
          %{type: :mixed,
            approx_from: series_start,
            exact_from:  wal_cutoff}

        true ->
          # Case 3: sub-day bucket, history beyond WAL — partial only
          replay_from_wal(src, dst, agg, bucket_ms, wal_cutoff, now_ms())
          %{type: :partial_wal,
            exact_from:   wal_cutoff,
            missing_from: series_start,
            warning:      "sub-day bucket cannot be backfilled beyond WAL retention"}
      end

    # Store coverage metadata in dst series Bitcask entry
    store_coverage_metadata(dst, coverage)
    {:ok, coverage}
  end

  defp backfill_from_columnar(src, dst, agg, bucket_ms, from_ms, to_ms) do
    # Read sealed day partitions from LMDB for the columnar range
    # Re-aggregate day values into the new bucket size
    # For sum/count/min/max: re-aggregation is lossless
    # For avg: weighted by row_count stored in each PartitionEntry
    partitions = Nif.ts_lmdb_scan(series_hash(src),
                   day_number(from_ms), day_number(to_ms))

    Enum.each(partitions, fn p ->
      bucket_ts = floor(p.ts_min / bucket_ms) * bucket_ms
      value     = re_aggregate(agg, p)
      # Write to dst series directly — bypasses ETS, straight to columnar file
      FerricStore.TS.FileWriter.append_direct(dst, bucket_ts, value)
    end)
  end
end
```

### 18.4 TS.INFO Coverage Output

After `TS.CREATERULE`, `TS.INFO` on the destination series shows exactly what data it contains and where it came from:

```
TS.INFO ts:views:post:123:per_week

series:           ts:views:post:123:per_week
source_series:    ts:views:post:123
agg:              sum BUCKET 1w
coverage:
  type:           mixed
  approx_range:   2024-01-01 → 2024-03-22  (re-aggregated from day partitions)
  exact_range:    2024-03-22 → today        (replayed from WAL, event-accurate)
  missing_range:  none
sample_count:     13 weeks
```

For Case 3:
```
TS.INFO ts:views:post:123:per_hour

coverage:
  type:           partial_wal
  exact_range:    2024-03-22 → today
  missing_range:  2024-01-01 → 2024-03-22
  warning:        sub-day bucket cannot be backfilled beyond WAL retention
```

### 18.5 MIRROR_STREAM — CDC Without a New Command

For users who need to stream every `TS.ADD` to an external system (Kafka, a data warehouse, webhooks), the right surface is not a new command — it's a parameter on `TS.CREATE` that mirrors writes to a FerricStore Stream key.

```
TS.CREATE key ... [MIRROR_STREAM stream_key]

TS.CREATE ts:views:post:123 RETENTION 90d MIRROR_STREAM ts:changes:views:post:123
```

On every `TS.ADD ts:views:post:123`, FerricStore automatically appends to `ts:changes:views:post:123`:

```
XADD ts:changes:views:post:123 * ts 1700000001000 val 42
```

The consumer uses existing `XREAD` / `XREADGROUP` commands — already in FerricStore's Streams implementation. Cursor-based, consumer groups, acknowledgement — all the CDC machinery already exists. `MIRROR_STREAM` just connects TS writes to the Stream automatically.

When `MIRROR_STREAM` is added to an existing series, FerricStore seeds the stream from the WAL (within `log_retention_hours`), then continues live. The same three-case logic from section 18.3 applies to the seeding — what's in the WAL is exact, what's beyond it is best-effort from columnar partitions.

### 18.6 The Practical Recommendation

**Define compaction rules at series creation time.** The retroactive backfill in section 18.3 is a recovery mechanism, not a workflow. If you know you need weekly rollups, create the rule when you create the series:

```
TS.CREATE ts:views:post:* RETENTION 90d COMPRESSION AUTO
TS.CREATERULE ts:views:post:* ts:views:post:*:per_hour AGG sum BUCKET 3600000
TS.CREATERULE ts:views:post:* ts:views:post:*:per_day  AGG sum BUCKET 86400000

# Now per_hour and per_day series are built in real-time from the start.
# No WAL retention concern. No backfill gaps. Perfect coverage always.
```

Rules defined at creation = event-accurate coverage for the lifetime of the series. Retroactive rules = best-effort, bounded by WAL retention and partition granularity.

---

*End of Columnar Chunk Encoding Spec*
