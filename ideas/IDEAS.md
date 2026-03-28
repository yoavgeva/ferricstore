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
