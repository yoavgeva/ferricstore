# RA Fork Optimization Results

## Setup

- **Fork location:** `/Users/yoavgea/repos/ra` (branch: `ferricstore-optimizations`)
- **FerricStore dependency:** `{:ra, path: "../../../ra"}` in `apps/ferricstore/mix.exs`
- **Base ra version:** 3.x (commit `27cc333`)
- **Platform:** macOS (Apple Silicon), APFS filesystem
- **fdatasync latency on macOS:** ~20ms (vs ~0.1-2ms on Linux NVMe)

## Benchmark Configuration

```
memtier_benchmark --protocol=resp3 --threads=2 --clients=10 -n 1000
  --ratio=1:0 --data-size=256 --pipeline=10
```

All benchmarks: quorum writes, 4 shards, single node (standalone mode).

## Changes Applied to RA Fork

### 1. Constants (Phase 2)

**Files changed:**
- `src/ra_server.hrl`: `AER_CHUNK_SIZE` 128 -> 512, `FLUSH_COMMANDS_SIZE` 16 -> 128
- `src/ra.hrl`: `WAL_DEFAULT_MAX_BATCH_SIZE` 8192 -> 32768

### 2. Async fdatasync (Phase 3)

**File changed:** `src/ra_log_wal.erl` (+139 lines)

Ported FerricStore's patched WAL (previously hot-loaded at boot) directly into the ra fork source. Changes:

- **`#state{}` record:** Added `pending_sync` and `sync_in_flight` fields for tracking async fdatasync state.
- **`flush_pending/1`:** Changed to write-only (no sync). Data goes to kernel buffer via `file:write`, sync is deferred.
- **`complete_batch/1`:** Instead of notifying writers inline after sync, queues `{Waiting, WalRanges}` into `pending_sync` and calls `maybe_start_async_sync/1`.
- **`maybe_start_async_sync/1`:** If no sync is in flight, spawns a linked process that opens a separate fd to the WAL file (required because `prim_file` raw fds are tied to the opening process), calls `file:datasync`, and sends `{wal_async_sync_complete}` back.
- **`handle_op({info, {wal_async_sync_complete}}, ...)`:** When sync completes, notifies ALL queued writers (from potentially many batches), then starts another sync cycle if more data arrived.
- **`drain_pending_sync/1`:** Synchronous drain called before WAL rollover to ensure all writers are notified before the old fd is closed.
- **`notify_all_pending/1`:** Iterates through all queued pending batches and calls `complete_batch_writer` for each writer.
- **`complete_batch_and_roll/1`:** Updated to drain pending sync before rollover.

**Invariant preserved:** Writers are ONLY notified AFTER fdatasync completes. Raft durability guarantees are maintained.

### 3. FerricStore Integration

- **`application.ex`:** Added `ra_is_path_dep?/0` check to skip hot-loading the patched WAL beam when the local ra fork is used (the fork already has the changes compiled in).
- **`mix.exs`:** Changed ra dependency from `{:ra, "~> 3.1"}` to `{:ra, path: "../../../ra"}`.

## Benchmark Results

### Baseline (local ra fork, no changes)

| Run | Ops/sec | Avg Latency (ms) |
|-----|---------|-------------------|
| 1   | 300     | 689               |
| 2   | 301     | 669               |
| 3   | 308     | 674               |

**Average: ~303 ops/sec**

### After Phase 2 (Constants only)

| Run | Ops/sec | Avg Latency (ms) |
|-----|---------|-------------------|
| 1   | 295     | 693               |
| 2   | 306     | 668               |
| 3   | 312     | 666               |
| 4   | 310     | 662               |

**Average: ~306 ops/sec** (no meaningful change)

The constants changes (`AER_CHUNK_SIZE`, `FLUSH_COMMANDS_SIZE`, `WAL_DEFAULT_MAX_BATCH_SIZE`) have minimal impact in single-node mode with 4 shards. They are primarily relevant for:
- Cluster mode (larger AppendEntries batches improve replication throughput)
- High shard counts (larger WAL batches improve amortization)
- The `WAL_DEFAULT_MAX_BATCH_SIZE` of 32768 was already set in FerricStore's ra system config

### After Phase 3 (Async fdatasync + Constants)

| Run | Ops/sec | Avg Latency (ms) |
|-----|---------|-------------------|
| 1   | 300     | 685               |
| 2   | 302     | 669               |
| 3   | 308     | 674               |

**Average: ~303 ops/sec** (same as baseline)

This is expected because FerricStore was ALREADY using the async fdatasync patch via hot-loading the patched beam at boot. The fork just inlines the same changes into the ra source code, eliminating the need for hot-loading.

### Shard Count Scaling (with fork)

| Shards | Ops/sec | Avg Latency (ms) |
|--------|---------|-------------------|
| 4      | 305     | 670               |
| 16     | 435     | 480               |

**16 shards: +43% throughput over 4 shards.** More shards = more ra_server processes writing to the WAL in parallel, better amortization of fdatasync across entries.

### Pipeline Depth Scaling (4 shards)

| Pipeline | Ops/sec | Avg Latency (ms) |
|----------|---------|-------------------|
| 10       | 328     | 1252              |
| 50       | 348     | 5794              |
| 100      | 363     | 10826             |

Throughput barely increases despite massive pipeline depth. Latency grows proportionally. Confirms the bottleneck is server-side (fdatasync latency), not client-side.

### Low Priority (`:low`) vs Normal Priority

Tested using `:ra.pipeline_command(..., :low)` instead of `:normal` to leverage ra's built-in command batching via `flush_commands`:

| Priority | Ops/sec | Avg Latency (ms) |
|----------|---------|-------------------|
| `:normal` | 305   | 670               |
| `:low`    | 305   | 665               |

**No improvement.** FerricStore's batcher already batches commands before sending to ra, and on Mac the bottleneck is fdatasync latency, not command processing overhead.

## Analysis

On macOS with APFS, `fdatasync` takes ~20ms. With async fdatasync, the WAL can accept entries while sync is in flight, but the effective throughput ceiling is:

```
entries_per_sync = ops/sec * sync_latency
300 ops/sec * 0.020s = ~6 entries per fdatasync
```

Each sync covers ~6 entries. To increase throughput on Mac, we need either:
1. **More shards** (proven: 16 shards -> 435 ops/sec)
2. **Faster fdatasync** (Linux NVMe: ~0.1-2ms -> theoretical 7.5K+ ops/sec)
3. **io_uring async fsync** (Linux only, <50us -> theoretical 50K+ ops/sec)

The constants changes will show their value on Linux with NVMe where fdatasync is fast and the ra_server processing/batching overhead becomes the bottleneck instead.

## What the Fork Provides

1. **Eliminates hot-loading:** The async fdatasync patch is compiled directly into ra, removing the fragile beam hot-load mechanism.
2. **Increased constants:** `AER_CHUNK_SIZE=512`, `FLUSH_COMMANDS_SIZE=128`, `WAL_DEFAULT_MAX_BATCH_SIZE=32768` — these will matter more on Linux.
3. **Clean upstream base:** The fork is based on latest ra main (3.x), making it easy to rebase on upstream updates.

## Next Steps (from optimization plan)

1. **Benchmark on Linux NVMe** — the real target platform. Expected: 7.5K+ ops/sec baseline, higher with the fork's constants.
2. **io_uring fsync NIF** — replace `file:datasync` with `io_uring IORING_OP_FSYNC` for <50us fsync on Linux.
3. **Parallel leader disk write** — send AppendEntries before local WAL confirms (safe per Raft thesis 10.2.1).
4. **Multiple WAL processes** — partition shards across ra systems with separate WAL processes on separate NVMe drives.
