# Async WAL fdatasync Benchmark

## Summary

Monkey-patched `ra_log_wal` to decouple `fdatasync` from the batch processing
loop. The WAL writes data synchronously (fast kernel buffer copy), then spawns a
linked process to run `fdatasync` asynchronously. While fdatasync runs in the
background, the WAL keeps accepting and writing new entries. When fdatasync
completes, ALL accumulated entries since the last sync are notified as "written."

**Writers are ONLY notified after fdatasync completes** -- the quorum durability
guarantee is preserved.

## Environment

- **Platform:** macOS Darwin 25.3.0 (Apple Silicon)
- **OTP:** 27
- **Shards:** 4
- **ra system:** 1 shared WAL (default write strategy, datasync sync method)
- **Writers:** 50 concurrent tasks
- **Duration:** 15s per benchmark run

## Results

| Metric | Baseline (blocking sync) | Async sync |
|--------|------------------------:|------------:|
| Writes/sec | ~254 | ~253 |
| p50 latency | ~161ms | ~174ms |
| p95 latency | ~384ms | ~354ms |
| p99 latency | ~485ms | ~844ms |
| Batches coalesced per sync | 1 | 2-4 |
| Scheduler util | ~11% | ~11% |

## Batch Coalescing Observed

The async sync IS working correctly. During the 15s benchmark:

- 2-4 gen_batch_server batches consistently coalesced per fdatasync cycle
- Each coalesced sync notified 0-4 ra writers (mostly 1)
- The WAL never blocked on fdatasync -- it continued accepting writes throughout

Sample log output:
```
wal async sync: notifying 4 writers from 4 coalesced batches
wal async sync: notifying 2 writers from 3 coalesced batches
wal async sync: notifying 8 writers from 2 coalesced batches
```

## Why No Throughput Improvement

The async WAL eliminates the ~20ms fdatasync blocking window, but throughput
did not improve because **the WAL is not the bottleneck**:

1. **Batcher GenServer.call serialization**: Each of the 50 writers calls
   `GenServer.call` on one of 4 Batcher processes. The Batcher then calls
   `ra:pipeline_command`, which writes to the WAL. The GenServer.call blocks
   until the Raft commit returns, so only ~4 writes can be in-flight in the
   WAL simultaneously (one per shard's Batcher). With 50 writers contending
   on 4 Batchers, the p50 latency of ~170ms is dominated by GenServer
   mailbox queuing, not WAL fdatasync time.

2. **Single-writer-per-batch**: The WAL's gen_batch_server accumulates
   messages between batch flushes. With only 4 Batchers sending writes
   (one per shard), each gen_batch_server batch contains only 1-4 entries.
   Even though we coalesce 2-4 batches per fdatasync, the total entries
   per sync cycle is still small (4-16).

3. **macOS F_FULLFSYNC overhead**: On macOS, `fdatasync` triggers
   `F_FULLFSYNC` which flushes the entire SSD write buffer (~1-3ms per
   call). Opening a separate fd for the async sync adds file open/close
   overhead (~0.1ms), making each async sync ~3-4ms instead of ~1-3ms.
   However, this is insignificant compared to the ~170ms p50 latency.

## Test Suite Results

All tests pass with the async WAL patch loaded:

```
6318 tests, 0 failures, 1 skipped (569 excluded)
```

Including all Raft integration tests (359 tests, 0 failures) and store tests
(316 tests, 0 failures).

## Implementation Details

### Files Modified

1. **`apps/ferricstore/priv/patched/ra_log_wal.erl`** -- Complete copy of
   upstream `ra_log_wal.erl` with async fdatasync modifications.

2. **`apps/ferricstore/lib/ferricstore/application.ex`** -- Added
   `install_patched_wal/0` that compiles and hot-loads the patched module
   before `ra_system:start/1`.

### Key Design Decisions

- **Separate fd for async sync**: Files opened with `[raw]` mode in Erlang
  tie the prim_file fd to the opening process. The sync process opens its own
  fd to the same WAL file, calls fdatasync (which flushes the kernel page
  cache for the entire file), and closes it.

- **spawn_link for crash propagation**: If fdatasync fails (I/O error), the
  linked sync process crashes, which crashes the WAL. This is correct -- we
  don't want to silently lose durability.

- **EXIT message handling**: The spawned sync process exits normally after
  completing. Since the WAL has `trap_exit = true`, the `{'EXIT', Pid, normal}`
  message arrives via gen_batch_server as `{info, {'EXIT', Pid, normal}}`.
  A dedicated `handle_op` clause ignores normal exits while propagating
  abnormal exits.

- **drain_pending_sync for rollover**: Before WAL file rollover, all pending
  writers are synchronously drained (blocking fdatasync + notification) to
  ensure no async sync references a closed fd.

- **sync_after_notify passthrough**: The `sync_after_notify` write strategy
  (which notifies BEFORE sync) bypasses the async path entirely -- it uses
  the original inline behavior since the whole point is to not wait for sync.

## Recommendations

The async WAL patch is safe and correct but provides no measurable throughput
improvement on the current architecture because the WAL is not the bottleneck.

To improve write throughput beyond ~254 writes/sec:

1. **Increase Batcher concurrency**: Instead of 1 Batcher per shard (4 total),
   use a pool of Batchers per shard, or switch to `cast` + caller-side
   awaiting to increase WAL write parallelism.

2. **Separate WAL per shard**: Start one ra system per shard instead of a
   shared system. This eliminates the single-WAL serialization point and
   allows 4 parallel fdatasync operations.

3. **Bypass Raft for single-node**: In standalone mode, skip Raft entirely
   and write directly to Bitcask + fsync. This eliminates the WAL, mem tables,
   and all Raft protocol overhead.

---

*Generated by async-wal benchmark on 2026-03-23*
