# Ideas / Future Work

## Cross-Node Cross-Shard ACID Transactions (Phase 2)

Phase 1 (implemented) supports `BEGIN`/`COMMIT`/`ROLLBACK` for single-shard and same-node cross-shard transactions. Full ACID with automatic rollback.

Phase 2 would extend this to cross-node cross-shard transactions using Two-Phase Commit (2PC) across Raft groups:

1. **PREPARE**: Coordinator sends commands to each shard's Raft leader. Each shard dry-runs in a staging area, votes YES/NO.
2. **COMMIT/ABORT**: If all vote YES, coordinator sends COMMIT to all. If any votes NO, sends ABORT to all.

Hard problems: coordinator crash between phases (needs transaction recovery log), network partitions (temporary inconsistency), double latency (2 Raft round-trips vs 1).

Most users can avoid this by using hash tags: `{user}:profile` and `{user}:settings` hash to the same shard. Phase 2 only needed when keys genuinely must live on different shards.

Reference: CockroachDB and Google Spanner are the only widely-used systems that solve this well. Redis Cluster rejects cross-shard transactions entirely (`CROSSSLOT` error).
