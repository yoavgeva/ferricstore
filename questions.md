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

## Q4: NIF Yielding for Large Operations
**Spec says:** get_all with millions of keys should not block the BEAM scheduler.
**Current impl:** DirtyIo scheduling for all disk I/O NIFs.
**Question:** Should we implement chunked/yielding NIFs for get_all/get_range or is DirtyIo sufficient? User wants Normal scheduling with yielding.

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
