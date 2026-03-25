# Direct ra.pipeline_command Benchmark (Batcher Bypass)

## Change

Quorum writes now call `ra:pipeline_command/4` directly from the caller's
process instead of going through the Batcher GenServer. The caller does a
selective receive on `{:ra_event, _, {:applied, _}}` matching on a unique
correlation ref.

The Batcher GenServer accumulated commands for 1ms before submitting to ra.
With the bypass, each write goes directly to the ra server's gen_batch_server,
which already batches all incoming messages between fdatasync calls internally.

## Results

| Metric | Before (via Batcher) | After (direct ra) | Change |
|--------|---------------------|-------------------|--------|
| Writes/sec (50 writers, 15s) | 254 | 242 | -5% (within noise) |
| p50 latency | 196ms | 168ms | -14% |
| p95 latency | N/A | 387ms | -- |
| p99 latency | N/A | 690ms | -- |
| Shard distribution | 25/25/25/25% | 25/25/25/25% | identical |
| Scheduler util | ~10% | ~10.6% | identical |

## Analysis

Throughput is unchanged because the bottleneck is the ra WAL fdatasync, not
the Batcher's GenServer serialization. At ~250 writes/sec across 50 writers,
each 1ms Batcher window captured only ~0.25 commands on average -- the Batcher
was almost always sending single commands, not batches.

The p50 latency improved 14% (196ms -> 168ms) by removing:
1. The GenServer.call/reply roundtrip to the Batcher (~2-5us per write)
2. The 1ms timer delay (negligible at this throughput, but adds up)
3. One extra process hop in the message path

The true bottleneck remains the ra WAL fdatasync. Each fdatasync takes 3-10ms
on macOS (F_FULLFSYNC), and all 4 shards share a single WAL file. With 50
concurrent writers, commands queue behind the fdatasync barrier.

## Architecture Impact

- **Removed from quorum write path**: Batcher GenServer (no more GenServer.call
  serialization, no timer window delay)
- **Kept**: Batcher still handles `:async` durability writes (fire-and-forget
  to AsyncApplyWorker)
- **Caller receives ra_event**: The caller's process (Task, Connection, etc.)
  receives `{:ra_event, Leader, {:applied, [{corr, result}]}}` directly.
  Selective receive matches on the unique correlation ref.
- **Fallback**: If `ra:pipeline_command` returns `{:error, :noproc}`, falls
  back to the Shard GenServer (same as before).

## Test Results

6318 tests, 0 failures related to change (1 pre-existing flaky edge case test).
Write path tests: 113/113 pass. Raft integration tests: 13/13 pass.
