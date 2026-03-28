# Ideas / Future Work

## ACID Transactions — BEGIN / COMMIT / ROLLBACK

Single-shard ACID transactions with automatic rollback. All commands succeed or none apply. Uses buffer architecture for zero dirty reads — commands execute into a process-dictionary buffer, only flushed to ETS + Bitcask if all succeed. State machine on Raft leader ensures consistency across nodes.

See [acid-transactions.md](acid-transactions.md) for full design.

### Phase 2: Cross-Node Cross-Shard ACID

Extends to cross-node transactions using Two-Phase Commit (2PC) across Raft groups. PREPARE → vote → COMMIT/ABORT. Hard problems: coordinator crash, network partitions, double latency.

Most users can avoid this with hash tags: `{user}:profile` and `{user}:settings` hash to the same shard.

Reference: CockroachDB and Spanner are the only systems that solve this well. Redis Cluster rejects cross-shard transactions entirely.

## Time Series: Columnar Chunk Encoding

Close the compression gap with TimescaleDB by sealing closed time buckets into columnar binary blobs. Delta-delta timestamps + Gorilla/XOR values = 8-14x compression, 3600x fewer syscalls for range scans.

See [ts-columnar-spec.md](ts-columnar-spec.md) for full design.
