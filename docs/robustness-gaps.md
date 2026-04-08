# Robustness Gaps — Design & Implementation Plan

Known architectural gaps that affect correctness, reliability, or operational
visibility in production cluster deployments.

---

## Completed

| # | Gap | Status |
|---|-----|--------|
| 1 | HLC clock migration — all TTL/expiry uses HLC.now_ms() | DONE |
| 2 | Raft error propagation — quorum writes bypass batcher, 10s timeout, leader retry | DONE |
| 3 | GenServer serialization — reads, writes, async all bypass Shard GenServer | DONE |
| 4 | Compaction during replication — Shard GenServer blocks compaction when writes paused | DONE (already handled) |

## Not Applicable

| # | Gap | Reason |
|---|-----|--------|
| 5 | Async write loss reporting | Async durability contract — user accepts loss risk. Same as Redis default. |
| 6 | Read-your-writes across nodes | Expected behavior for distributed stores. Redis Cluster is the same. |

## Remaining

### Keydir Memory Accounting is Approximate

**Severity: Low (eviction accuracy)**

`MemoryGuard` estimates ETS memory via `word_size * :ets.info(table, :memory)`.
This misses off-heap refc binaries (values > 64 bytes stored in ETS). Eviction
decisions may be slightly inaccurate.

Fix: sample N random ETS entries periodically (every 30s), measure binary
sizes, extrapolate. Low priority — eviction is already conservative.

| File | Change |
|------|--------|
| `lib/ferricstore/memory_guard.ex` | Add binary sampling to memory calculation |
