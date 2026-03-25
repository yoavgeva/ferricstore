# No-Raft Benchmark (raft_enabled: false)

## Purpose

Measure FerricStore's raw throughput ceiling without Raft consensus overhead.
This represents the maximum performance for single-node embedded deployments
where crash-safety comes from periodic fsync rather than Raft WAL.

With Raft enabled, every write goes through ra's WAL with fdatasync, which
serializes all writes behind a single fsync syscall. With Raft disabled,
writes go directly to ETS (immediate visibility) + async Bitcask batch
(no fsync), removing the consensus bottleneck entirely.

## Environment

- **Machine**: macOS, 16 cores (16 schedulers online)
- **OTP**: 28
- **Shards**: 4
- **Workers**: 50 concurrent tasks
- **Comparison duration**: 5s per mode
- **Date**: 2026-03-23

## Write Path Comparison

### Raft ON (quorum durability)

```
FerricStore.set -> Router.put -> quorum_bypass? = true
  -> Batcher.write_async -> ra:process_command -> WAL fdatasync
  -> StateMachine.apply -> ETS insert + v2_append_batch
```

### Raft OFF (async durability)

```
FerricStore.set -> Router.put -> quorum_bypass? = false
  -> Shard GenServer {:put, ...} -> ETS insert (immediate)
  -> pending batch list -> flush_pending -> v2_append_batch_nosync
```

## Results

### Write Throughput Comparison (5s, 50 writers)

| Metric | Raft ON | Raft OFF | Speedup |
|--------|---------|----------|---------|
| Throughput | 8,977/sec | 4,387/sec | 0.5x |
| Total ops | 44,884 | 21,936 | |
| Latency p50 | 4.3ms | 3.7ms | |
| Latency p95 | 14.4ms | 50.6ms | |
| Latency p99 | 20.8ms | 102.2ms | |
| Scheduler util | 10.8% | 8.8% | |
| Memory delta | +26.0 MB | +11.7 MB | |

## Analysis

The 0.5x speedup confirms that ra's WAL fdatasync is the
dominant bottleneck in the write path. Without Raft:

- Writes are limited only by ETS insert throughput and Shard GenServer
  message processing speed.
- Bitcask appends happen asynchronously in batches (no fsync), so they
  don't block the write caller.
- The Shard GenServer processes writes in ~3.7ms (p50)
  vs ~4.3ms with Raft.

## Implications

### Single-node embedded deployments

`raft_enabled: false` with periodic `v2_fsync` (e.g. every 1s) is viable
for use cases where:
- The application can tolerate losing up to 1s of writes on crash
- No multi-node replication is needed
- Maximum throughput is the priority

### Multi-node deployments

Raft is required for replication. To optimize the Raft path:
- Increase Batcher `window_ms` to amortize fdatasync across more writes
- Use `o_sync` WAL strategy to avoid separate fdatasync syscall
- Consider async replication modes for non-critical data
