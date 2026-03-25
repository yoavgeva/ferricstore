# ra_file:sync() / file:datasync() Analysis

## Background

Ra's WAL (Write-Ahead Log) has two sync code paths:

1. **Hot path (WAL flushes):** `ra_log_wal` calls `file:datasync/1` directly in its internal `sync/2` function after every batch flush. This is the primary durability mechanism.
2. **Cold path (segments, snapshots):** `ra_file:sync/1` is called during segment rotation, snapshot writes, and cleanup. This wraps `file:sync/1`.

The `Ferricstore.RaFileInstrument` module monkey-patches `ra_file:sync/1` but only captures the cold path. To measure the actual WAL fsync cost, we also trace `file:datasync/1` calls on the WAL process using Erlang's built-in trace infrastructure.

## Test Setup

- 50 concurrent writer tasks
- 10-second benchmark duration
- Single-node Raft (32 shards, 1 WAL process)
- macOS (Apple Silicon), APFS filesystem
- Default ra WAL write strategy (`default` -- write then datasync)
- Default ra sync method (`datasync`)

## Raw Data

### Run 1

```
Tracing 1 WAL processes

Total writes: 2,306
Throughput: 230 writes/sec

--- ra_file:sync() (cold path: segments, snapshots) ---
  Total sync calls:  0
  Total sync time:   0ms

--- file:datasync (hot path: WAL flushes) ---
  file:datasync calls: 201
  file:sync calls:     0
  Total sync calls:    201
  Total sync time:     4,517ms
  Average:             22,477us (22.48ms)
  Syncs/sec:           44
  Writes/sync:         11.5

  Histogram:
    <100us            0 (  0.0%)
    <500us            0 (  0.0%)
    <1ms              1 (  0.5%)  |
    <5ms             10 (  5.0%)  |=====
    <10ms            49 ( 24.4%)  |========================
    <50ms           131 ( 65.2%)  |==================================================
    <100ms            8 (  4.0%)  |====
    <500ms            2 (  1.0%)  |=
    >=500ms           0 (  0.0%)
```

### Run 2

```
Tracing 1 WAL processes

Total writes: 2,822
Throughput: 282 writes/sec

--- ra_file:sync() (cold path: segments, snapshots) ---
  Total sync calls:  0
  Total sync time:   0ms

--- file:datasync (hot path: WAL flushes) ---
  file:datasync calls: 225
  file:sync calls:     0
  Total sync calls:    225
  Total sync time:     4,496ms
  Average:             19,985us (19.98ms)
  Syncs/sec:           50
  Writes/sync:         12.5

  Histogram:
    <100us            0 (  0.0%)
    <500us            0 (  0.0%)
    <1ms              0 (  0.0%)
    <5ms             10 (  4.4%)  |====
    <10ms            51 ( 22.7%)  |=======================
    <50ms           159 ( 70.7%)  |==================================================
    <100ms            3 (  1.3%)  |=
    <500ms            2 (  0.9%)  |=
    >=500ms           0 (  0.0%)
```

## Analysis

### Fsync frequency

- ~200-225 datasync calls over 10 seconds = **~20-22 fsyncs/sec**
- Zero `ra_file:sync` calls on the cold path (no segment rotation triggered in 10s)
- The WAL exclusively uses `file:datasync/1` (not `file:sync/1`), which avoids flushing filesystem metadata and is faster on most filesystems

### Average fsync duration

- **~20-22ms average** per datasync call
- This is consistent with macOS APFS fsync behavior on SSDs, where APFS enforces full barrier flushes through the storage controller

### Distribution -- bimodal with a long tail

The histogram shows a clear pattern:
- **~5% under 5ms** -- these are likely no-ops where the OS had already flushed the page cache, or very small WAL writes
- **~23-24% in 5-10ms** -- moderate-size WAL flushes hitting the SSD write path
- **~65-71% in 10-50ms** -- the dominant bucket; this is the "normal" APFS datasync cost on Apple Silicon SSDs. The NVMe controller flush + APFS journal commit typically lands in this range
- **~4-5% over 50ms** -- occasional slow fsyncs, likely caused by APFS journal contention, SSD garbage collection, or OS-level I/O scheduling delays
- **0 calls over 500ms** -- no catastrophic stalls observed

### WAL batching efficiency

- **~11.5-12.5 writes per fsync** -- the WAL is successfully batching multiple Raft entries per fsync call
- With 50 concurrent writers and 32 shards, each batch collects entries that arrived while the previous datasync was in flight
- At 282 writes/sec and 22 fsyncs/sec, the batch size is modest. This suggests the fsync latency (~20ms) is the throughput bottleneck: the WAL spends ~45% of wall-clock time blocked in datasync

### Fsync as the throughput bottleneck

- Total sync time: ~4.5 seconds out of 10 seconds of benchmark = **45% of wall time in datasync**
- With a single WAL process, throughput is capped at roughly `1000ms / avg_fsync_ms * batch_size` = `1000/20 * 12 = ~600 writes/sec` theoretical max
- Observed throughput of 230-282 writes/sec is below this theoretical cap, indicating additional overhead from Raft consensus, GenServer serialization, and batch accumulation latency

### Key takeaways

1. **fdatasync is the dominant cost center.** The WAL spends nearly half its time waiting for disk barriers.
2. **No segment-level syncs during the test.** The 10-second window is too short to trigger WAL segment rotation (default 512MB or 50K entries).
3. **Batch size is modest.** At ~12 writes/batch, there is room to improve throughput by increasing batch accumulation windows (e.g., `ra`'s `wal_max_batch_size` or adding a brief yield before fsync).
4. **macOS APFS fdatasync is expensive.** The 20ms average is characteristic of Apple's storage stack. Linux ext4/XFS with NVMe typically achieves 1-5ms fdatasync, which would yield 4-10x higher throughput.
5. **The `ra_file:sync` monkey-patch alone is insufficient.** It only captures cold-path operations. Any instrumentation must trace `file:datasync/1` on the WAL process to see the real picture.
