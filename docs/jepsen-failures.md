# Jepsen Test Failures

28 tests, 11 failures (locally on 16-core Mac). These are multi-node
tests using `:peer` to spawn 3-node clusters.

## Real Data Bugs (4)

### 1. Read-Your-Writes Violated

**Test:** `LinearizabilityTest` — "100 sequential writes readable"
**Error:** `wrote v47 but read v46`

Write to node, immediately read from same node — gets stale value.
Quorum write returned `:ok` (Raft committed) but GET returns the
previous value. This means either:
- The write was committed to Raft WAL but not applied to ETS yet
- The read went to a different shard/path than the write
- The ETS update in the state machine's apply is deferred

### 2. Stream Entries Missing

**Test:** `StreamDurabilityTest` — "XRANGE returns entries in order"
**Error:** `XRANGE should return 50 entries, got 49`

And: `1 stream entries missing on surviving node`

XADD returned success but the entry is not in XRANGE after a node
kill. Stream entries go through Raft — if committed, they should
survive. Either the XADD ACK was sent before Raft commit, or the
stream metadata (ETS-based) is inconsistent with the stored entries.

### 3. Async Writes Lost After ACK

**Test:** `AsyncDurabilityTest` — "100 async writes all readable"
**Error:** `lost_write, expected v2, got nil`

Async write returned `:ok`, immediate read returns nil. The async
path writes to ETS first then fires Raft — the ETS write should be
visible immediately. If it's nil, either:
- ETS was cleared between write and read (shard restart?)
- The key hashed to a different shard than expected
- flush_all_keys from another test cleared it

### 4. Quorum Write Not Durable

**Test:** `AsyncDurabilityTest` — "quorum writes immediately durable"
**Error:** `Quorum write should be immediately durable on node`

Quorum write to one node, read from same node returns nil. Same
class of issue as #1 — write committed but not visible.

## Infrastructure Issues (7)

### 5. Timeouts (4 tests)

**Tests:** LostWritesTest (2), SetDurabilityTest (2)
**Error:** `test timed out after 120000ms`

Node kill + write loop takes too long. The loop writes, kills a node,
writes more, then verifies. On slow machines or with large WALs, the
node restart + WAL replay exceeds 120s.

### 6. Partition Not Working (2 tests)

**Tests:** PartitionDurabilityTest (2)
**Error:** `n3 should be disconnected from n1`

`Node.disconnect` doesn't fully isolate the nodes — Erlang
distribution reconnects automatically. Need `net_kernel` tricks
or `iptables`-style blocking (not available in tests).

## Priority

| # | Bug | Severity | Action |
|---|-----|----------|--------|
| 1 | Read-your-writes | CRITICAL | Investigate — is the read going to the right shard? |
| 4 | Quorum not durable | CRITICAL | Same root cause as #1 |
| 2 | Stream entry lost | HIGH | Check XADD Raft path |
| 3 | Async write lost | MEDIUM | Expected in async — but should survive on writing node |
| 5 | Timeouts | LOW | Increase timeout or reduce write count |
| 6 | Partition | LOW | Test infrastructure limitation |
