# FerricStore — Open Questions

Questions where the spec does NOT provide a clear answer. Everything else follows the spec exactly.

---

## Q1: Sharding Algorithm — phash2 vs CRC16
**Spec says:** CRC16 hash of key mod 16384 hash slots.
**Current impl:** `:erlang.phash2(key, shard_count)`.
**Question:** Should we switch to CRC16 now (for Redis cluster compatibility) or keep phash2 for standalone mode? The spec mentions CRC16 for cluster slot routing but we're single-node currently.

## Q2: Bitcask File Descriptor Limits for Per-Key Instances
**Spec says:** Each hash/list/set/zset key owns its own Bitcask instance.
**Question:** With 1M hash keys, that's 1M Bitcask instances × 2-4 file descriptors each = 2-4M FDs. The spec mentions 5M LimitNOFILE. Is this acceptable for v1 or should we use compound keys in a shared Bitcask as an interim approach?

## Q3: OTP Version Compatibility with ra
**Spec says:** ra library version 3.0.1 in INFO output.
**Question:** Current ra releases are 2.x. Should we use latest 2.x or is there a 3.x branch? Need to verify compatibility with our OTP version.

---

## Implementation Progress (2026-03-18)

### Fully Implemented per Spec:
- RESP3 protocol (all types)
- String commands (22 commands)
- Hash commands (15 commands) — being refactored to per-key Bitcask
- List commands (15 commands) — being refactored
- Set commands (17 commands) — being refactored
- Sorted Set commands (14 commands) — being refactored
- Bitmap commands (5 commands)
- HyperLogLog (3 commands)
- Transactions (MULTI/EXEC/WATCH)
- Pub/Sub (10 commands)
- TTL/Expiry (9 commands)
- Generic key commands (15 commands)
- Server/Client/Config/Auth/Memory/SLOWLOG commands (60+ commands)
- Native commands (CAS, LOCK, RATELIMIT.ADD, FETCH_OR_COMPUTE)
- Cluster info commands
- Keyspace notifications module
- Active expiry sweep
- Sandbox commands

### In Progress:
- Data structure storage refactor to per-key Bitcask (per spec 2B.4)
- Raft consensus integration via ra (per spec 2.5)

### Not Yet Started:
- Streams (XADD, XREAD, etc.) — spec section on stream storage
- Geo commands — builds on sorted set
- JSON commands — needs JSONPath engine
- Probabilistic structures (Bloom, Cuckoo, CMS, TopK) — spec says Rust NIF + mmap
- Blocking commands (BLPOP family) — spec says BEAM waiter registry
- CLIENT TRACKING — spec section on invalidation
- MemoryGuard — spec section 2.4
- Merge lifecycle automation — spec section 2E

*Only add questions here if the spec does not provide a clear answer.*
