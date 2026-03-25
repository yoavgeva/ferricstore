# Background Bitcask Write Benchmark

## Summary

Moved the synchronous Bitcask NIF write (~50us) out of `StateMachine.apply/3`
into a background `BitcaskWriter` GenServer. Small values (< 64KB) are inserted
into ETS immediately with `file_id = :pending`, and the Bitcask write is
deferred to a background process that batches and flushes every 1ms or 100
entries. Large values still use the synchronous path since their ETS value is
nil and cold reads need a valid disk offset immediately.

## Design

```
Before:  apply/3  ->  NIF.v2_append_batch_nosync (~50us)  ->  ETS insert (~1us)
After:   apply/3  ->  ETS insert (~1us)  ->  GenServer.cast to BitcaskWriter (~1us)
                      BitcaskWriter  ->  batch NIF write every 1ms
```

One `BitcaskWriter` per shard, started before `ShardSupervisor` in the
supervision tree. The active file path is passed with each write cast, so
file rotations are handled transparently.

### Correctness invariants

- `file_id = :pending` in ETS signals "not yet on disk"
- MemoryGuard skips `:pending` entries (cannot evict data not yet on disk)
- `warm_from_bitcask` guards with `is_integer(fid) and fid > 0`
- `do_delete` flushes the writer before writing a tombstone (prevents
  resurrection on recovery due to Bitcask last-record-wins semantics)
- After Bitcask write, `update_element` sets real `{file_id, offset, vsize}`
- Large values (>= 64KB) bypass the writer entirely (synchronous path)

## Benchmark Results

### Full-Stack Write Throughput (4 shards, 50 writers, 15s)

| Metric | Before (sync NIF) | After (background writer) | Change |
|--------|-------------------|---------------------------|--------|
| Throughput | 5,081 writes/sec | 5,742 writes/sec | +13% |
| p50 latency | ~7ms | 6.3ms | -10% |
| p95 latency | ~27ms | 23.8ms | -12% |
| p99 latency | ~42ms | 36.2ms | -14% |

### Write Path Profile (1000 sequential writes)

| Step | Before | After | Change |
|------|--------|-------|--------|
| StateMachine apply (NIF + ETS) | ~50us | ~2us | -96% |
| wait_ra_event (WAL fsync) | ~820us | ~865us | same |
| Total per-write | ~870us | ~870us | -0% |

### Comparison with Pure ra (no Bitcask)

| Configuration | Throughput | Gap vs Pure ra |
|--------------|-----------|----------------|
| Pure ra (TestKvMachine) | 31,289 writes/sec | 1.0x |
| FerricStore (background writer) | 5,742 writes/sec | 5.4x |
| FerricStore (sync NIF, before) | 5,081 writes/sec | 6.2x |

### Read Throughput (unchanged)

| Metric | Value |
|--------|-------|
| Throughput | 127,393 reads/sec |
| p50 latency | 106us |

## Analysis

The 13% throughput improvement is modest because the dominant bottleneck is the
ra WAL fsync (~800us per batch), not the Bitcask NIF (~50us). Removing the NIF
from the critical path reduced the apply callback time by ~96% (from ~50us to
~2us), but this only constitutes ~6% of the total write latency.

The remaining 5.4x gap between FerricStore and pure ra is due to:
1. **Batcher overhead**: collecting writes into batches, HLC timestamping
2. **Router/Shard GenServer hops**: message passing through the call chain
3. **ETS operations**: insert + prefix tracking in apply callback
4. **WAL contention**: 4 shards sharing one WAL vs 1 ra server in pure ra test

## Files Changed

- `apps/ferricstore/lib/ferricstore/store/bitcask_writer.ex` -- new GenServer
- `apps/ferricstore/lib/ferricstore/raft/state_machine.ex` -- deferred writes for small values
- `apps/ferricstore/lib/ferricstore/memory_guard.ex` -- skip `:pending` entries
- `apps/ferricstore/lib/ferricstore/store/shard.ex` -- handle `:pending` fid in cold paths
- `apps/ferricstore/lib/ferricstore/application.ex` -- start BitcaskWriter processes
- Various test files -- flush BitcaskWriter before verifying disk state
