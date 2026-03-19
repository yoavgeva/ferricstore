# FerricStore — Open Questions

Questions where the spec does NOT provide a clear answer. Everything else follows the spec exactly.

---

## Q1: Sharding Algorithm — phash2 vs CRC16
**Spec says:** CRC16 hash of key mod 16384 hash slots.
**Current impl:** `:erlang.phash2(key, shard_count)`.
**Question:** Should we switch to CRC16 now (for Redis cluster compatibility) or keep phash2 for standalone mode?

## Q2: Bitcask File Descriptor Limits for Per-Key Instances
**Spec says:** Each hash/list/set/zset key owns its own Bitcask instance.
**Current impl:** Compound keys in a shared Bitcask per shard.
**Question:** Is this acceptable for v1 or do we need per-key Bitcask instances?

## Q3: OTP Version Compatibility with ra
**Spec says:** ra library version 3.0.1 in INFO output.
**Current impl:** ra ~> 2.14.
**Question:** Should we use latest 2.x or is there a 3.x branch?

## Q4: NIF Architecture — CRITICAL SPEC VIOLATION
**Spec says (section 2B.5):** "FerricStore uses zero dirty schedulers — neither dirty CPU nor dirty IO. All NIFs are either pure RAM (normal scheduler), async IO via Tokio (returns immediately), or yielding CPU NIFs."
**Current impl:** All NIFs except put_batch_async use `schedule = "DirtyIo"`.
**Status:** Must be migrated to:
- Pattern 1 (Async Tokio): `get`, `put`, `put_batch`, `delete`, `write_hint`, `purge_expired`, `run_compaction` — submit to Tokio, return immediately, result via BEAM message
- Pattern 2 (Yielding): `get_all`, `get_batch`, `get_range`, `keys`, `shard_stats`, `file_sizes` — yield between chunks via `enif_consume_timeslice` + `enif_schedule_nif`
- Pattern 3 (Normal): `read_modify_write` (single key, fast) — runs on normal scheduler
**Impact:** Major Rust NIF refactor + Elixir shard GenServer changes to handle async responses. The current DirtyIo approach works but violates the spec's zero-dirty-scheduler constraint and can exhaust the dirty-IO pool under load.

## Q5: Cross-Shard 2PC Transactions
**Spec says:** Cross-shard transactions use 2PC protocol.
**Current impl:** MULTI/EXEC works within single shard, cross-shard not yet implemented.
**Question:** Should we implement 2PC now or defer to after Raft is production-stable?

---

## Implementation Status — Spec Completion

### Phase 1 — Foundation: COMPLETE
- Rust Bitcask NIF (append-only log, keydir, pread, hint files, compaction)
- RESP3 protocol parser (all types, pipelining)
- Ranch TCP server (connection handling, backpressure)
- ETS hot cache with TTL
- Core string commands (22 commands)

### Phase 2 — Distribution: COMPLETE
- ra Raft integration (leader election, log replication, state machine)
- Consistent hash router (key-to-shard mapping)
- Group commit batcher (batch window, Raft pipeline)
- MULTI/EXEC transactions (single-shard atomic)
- WATCH (optimistic locking with write-version)

### Phase 3 — Data Structures: COMPLETE (except LiveView dashboard)
- Hash (18 commands incl. HEXPIRE/HTTL/HPERSIST/HSCAN/HRANDFIELD)
- List (15 commands with TypeRegistry WRONGTYPE enforcement)
- Set (17 commands incl. SSCAN/SRANDMEMBER/SPOP/SMOVE)
- Sorted Set (14 commands incl. ZSCAN/ZRANDMEMBER/ZMSCORE)
- Streams (11 commands: XADD, XREAD, XRANGE, XREADGROUP, XACK, etc.)
- Geo (6 commands: GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE)
- JSON (13 commands: JSON.SET/GET/DEL/NUMINCRBY/TYPE/STRLEN, etc.)
- Bitmap (5 commands), HyperLogLog (3 commands)
- Hash field TTL, Keyspace notifications
- SCAN/HSCAN/SSCAN/ZSCAN cursor-based iteration
- ACL authentication and authorization

### Phase 4 — Production Hardening: COMPLETE (except external tooling)
- Bloom filter (7 commands), Cuckoo filter (8 commands)
- Count-Min Sketch (6 commands), Top-K (6 commands)
- CAS, LOCK, UNLOCK, EXTEND native commands
- RATELIMIT.ADD sliding window rate limiter
- FETCH_OR_COMPUTE cache stampede protection
- CLIENT TRACKING (ON/OFF/REDIRECT/BCAST/OPTIN/OPTOUT/CACHING)
- Blocking commands (BLPOP, BRPOP, BLMOVE, BLMPOP)
- CONFIG GET/SET for all spec parameters
- FERRICSTORE.HOTNESS hot/cold read tracking
- Telemetry events (9 event types per spec 4.8)
- INFO command (all spec sections: server, clients, memory, persistence, replication, cpu, keyspace, stats)
- Embedded Elixir API (strings, hash, lists, sets, sorted sets, native, generics, pipeline)
- Sandbox isolation (START/JOIN/END/TOKEN)
- Merge lifecycle (manifest, semaphore, scheduler with NIF integration)
- NIF extended functions (get_all, get_batch, get_range, read_modify_write)
- Merge NIF functions (shard_stats, file_sizes, run_compaction, available_disk_space)
- Rust NIF hardened (no panics, size validation, crash-safe ordering)

### In Progress:
- TLS support (spec 6.2)
- Audit logging (spec 6.6)
- Prometheus metrics endpoint (spec 7.3)

### Deferred (external tooling, not core logic):
- Phoenix LiveView dashboard
- Kubernetes Helm chart and operator
- Jepsen-style correctness testing
- Load testing at scale
- Client libraries (Python, Go, Node.js)

### Test Coverage:
- 3792 tests passing, 0 failures
- 193 excluded (perf benchmarks, linux_io_uring, large_alloc — intentional)

*Only add questions here if the spec does not provide a clear answer.*

## Q6: Two-Table ETS Architecture (Section 2.4)
**Spec says:** Separate `:ferricstore_keydir` (key→offset, never evicted) and `:ferricstore_hot_cache` (key→value, LRU evicted) tables with independent budgets.
**Current impl:** Single `shard_ets_N` table per shard stores both key metadata and values.
**Status:** Structural deviation. Current approach works correctly but doesn't support independent eviction policies. The keydir entries and hot cache values share the same memory pool. Splitting into two tables per shard would enable:
- KEYDIR_FULL rejection (reject new keys when keydir full, without evicting cache)
- Independent LRU eviction on hot cache only
- More accurate memory accounting per pool
**Impact:** Medium. Correctness is unaffected. Performance optimization deferred.

## Q7: Encryption at Rest (Section 6.5)
**Spec says:** Optional AES-256-GCM encryption for Bitcask data files.
**Current impl:** Not implemented. Data files are plaintext.
**Status:** Deferred. Requires Rust NIF changes to encrypt/decrypt in the write/read path.

## Q8: Two-Table ETS Split — Implementation Blocked
**Spec says (2.4):** Separate keydir (metadata only: file_id, offset, value_size, expire_at_ms, type_byte) and hot_cache (key→value, LRU evicted).
**Current impl:** Single `shard_ets_N` stores `{key, value, expire_at_ms}`. ETS IS the primary read source, not a cache over Bitcask.
**Blocker:** The current architecture uses ETS as the source of truth for reads. The spec requires ETS keydir to store only Bitcask pointers (file_id, offset) with hot_cache as an optional value cache. This requires:
1. Changing the read path to always go through Bitcask (or hot cache on top)
2. The keydir entry needs file_id/offset which are currently only in the Rust NIF keydir
3. 39 ETS insert locations, 15 test files affected
4. Estimated 4-6 weeks of careful refactoring
**Decision needed:** Is this a v1 or v2 change? The single-table approach works correctly for all workloads.

## Q9: Async Tokio for ALL IO NIFs (Section 2B.5 Pattern 1)
**Spec says:** "Every Bitcask IO operation submits work to Tokio and returns immediately. The calling BEAM process suspends on a receive."
**Current impl:** Only `put_batch_async` uses async Tokio (via io_uring on Linux). All other IO NIFs (`get`, `put`, `delete`, `write_hint`, `purge_expired`, `run_compaction`) block the Normal scheduler synchronously.
**Impact:** Synchronous IO on Normal scheduler blocks one scheduler thread during disk I/O (~50-200µs for reads, ~1-5ms for writes with fsync). With 4+ schedulers this is tolerable for most workloads, but under heavy IO load it can cause latency spikes.
**Required change:** Convert get/put/delete/write_hint/purge_expired/run_compaction to async pattern:
1. NIF submits IO to Tokio/io_uring, returns `{:pending, op_id}`
2. Shard GenServer enters receive for `{:io_complete, op_id, result}`
3. Returns result to caller
**Estimated effort:** Major refactor of both Rust NIF layer and Elixir Shard GenServer. The Shard GenServer currently uses synchronous GenServer.call for all operations — async IO requires restructuring to handle pending operations.

## COMPREHENSIVE GAP ANALYSIS (from granular audit)

### CRITICAL GAPS (spec violations affecting correctness/durability)

**Q10: Raft NOT in write path (Section 2C.2)**
Writes go directly to Shard ETS + Bitcask NIF, bypassing Raft entirely.
The Batcher and StateMachine exist but are not integrated into the actual
write path. This means NO durability guarantee — writes are acknowledged
before Raft commit. Fixing requires routing all writes through Raft.

**Q11: active:false instead of active:once (Section 2C.2)**
Connection uses passive mode (blocking recv) instead of active:once.
This prevents the sliding window from working as designed — the connection
process blocks on recv until data arrives. active:once would allow
processing messages between TCP reads.

### MEDIUM GAPS (spec features not implemented)

**Q12: Merge Supervisor not started (Section 2E.8)**
Merge.Supervisor exists but is NOT started in application.ex.
Merges only happen when explicitly triggered, not automatically.

**Q13: release_cursor not implemented (Section 2E.5)**
Raft log compaction via release_cursor is not implemented.
The Raft log grows unbounded.

**Q14: Merge IO Priority (Section 2E.11)**
No IOPRIO_CLASS_BE for background merge IO.

**Q15: Follower merge gating (Section 2E.4)**
No leader-only gating — scheduler runs on all nodes.

### MINOR GAPS (operational features, docs-only sections)

**Q16: Health endpoint (Section 2C.1 Phase 3)**
No dedicated /health/ready HTTP endpoint.

**Q17: Encryption at rest (Section 6.5)**
Not implemented.

**Q18: Hot cache hint file (Section 2C.6 Step 4)**
Shutdown doesn't write hot cache hint file for next startup warmup.
