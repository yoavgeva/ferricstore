# Ideas / Future Work

## Time Series Support

Add native time series data type to FerricStore — TS.CREATE, TS.ADD, TS.RANGE, TS.MRANGE, TS.GET, TS.INFO, TS.QUERYINDEX. Compete with RedisTimeSeries and TimescaleDB for metrics, IoT, and observability workloads.

Key advantage over RedisTimeSeries: persistent by default (Raft + Bitcask), survives restarts. Key advantage over TimescaleDB: Redis wire protocol, embedded mode, microsecond writes.

### Columnar Chunk Encoding

Once time series is implemented, close the compression gap with TimescaleDB by sealing closed time buckets into columnar binary blobs. Delta-delta timestamps + Gorilla/XOR values = 8-14x compression, 3600x fewer syscalls for range scans. Write path unchanged — sealing is async background work.

See [ts-columnar-spec.md](ts-columnar-spec.md) for the columnar encoding design.

## ACID Transactions — BEGIN / COMMIT / ROLLBACK

Single-shard ACID transactions with automatic rollback. All commands succeed or none apply. Uses buffer architecture for zero dirty reads — commands execute into a process-dictionary buffer, only flushed to ETS + Bitcask if all succeed. State machine on Raft leader ensures consistency across nodes.

See [acid-transactions.md](acid-transactions.md) for full design.

### Phase 2: Cross-Node Cross-Shard ACID

Extends to cross-node transactions using Two-Phase Commit (2PC) across Raft groups. PREPARE → vote → COMMIT/ABORT. Hard problems: coordinator crash, network partitions, double latency.

Most users can avoid this with hash tags: `{user}:profile` and `{user}:settings` hash to the same shard.

## FERRICSTORE.RESET — Per-Node Shard Reset

A "reset this node" command that clears local state for one or all shards
and re-syncs from the Raft leader via snapshot. Useful for:

- Fixing data corruption on one node without affecting others
- Re-syncing after disk failure or manual intervention
- No customer data loss — leader still has everything

```
FERRICSTORE.RESET SHARD 0    # reset shard 0 on this node
FERRICSTORE.RESET ALL        # reset all shards on this node
```

ra already handles snapshot install when followers fall behind. This
command would trigger it manually by deleting local Bitcask + ETS +
registries for the target shard(s), then requesting a fresh snapshot
from the Raft leader.

## FLUSHDB Multi-Node Registry Propagation

FLUSHDB currently clears keys via Raft (replicated to all nodes) but
clears probabilistic registries (Bloom, Cuckoo, CMS mmap handles)
locally only. On multi-node, followers' registries are not cleared.

Fix: Add a `{:flush_registries, shard_index}` Raft command that each
node's StateMachine executes to clear local registries. Or include
registry cleanup in the StateMachine's delete handler when it detects
a probabilistic key prefix.
