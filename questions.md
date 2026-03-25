# FerricStore — Open Questions

Questions where the spec does NOT provide a clear answer. Everything else follows the spec exactly.

---

## RESOLVED (kept for history)

- **Q1 (Sharding):** Using phash2 for standalone — acceptable for v1.
- **Q2 (Per-key Bitcask):** Using compound keys in shared Bitcask + promotion for large collections — implemented.
- **Q3 (ra version):** Using ra ~> 2.14 — compatible.
- **Q4 (NIF scheduling):** ALL NIFs now on Normal scheduler with yielding via enif_schedule_nif — FIXED.
- **Q6 (Two-table ETS):** SUPERSEDED — migrated to single-table keydir with LFU (v2 architecture).
- **Q8 (ETS split blocked):** SUPERSEDED — single-table keydir replaces two-table split.
- **Q10 (Raft not in write path):** FIXED — all 15+ write operations route through Raft.
- **Q11 (active:once):** FIXED — connections use active:once sockets.
- **Q12 (Merge supervisor):** FIXED — started in application.ex.
- **Q13 (release_cursor):** FIXED — emitted every 1000 applies.
- **Q16 (Health endpoint):** FIXED — /health/ready on port 9090 + HTML dashboard.

## FUTURE FEATURES

### F1: Shard Placement / Leadership Rebalance (Vertical + Horizontal Scaling)
**Problem:** When a new node joins with more CPUs, it should lead more shards proportionally.
**Design:**
- `CLUSTER REBALANCE` command redistributes shard leadership by node weight (CPU count)
- `ra:transfer_leadership/2` moves leadership instantly (no data migration)
- Requires overprovisioning shards at startup (recommend 32+ shards)
- Example: Node 1 (4 CPUs) leads 8/32 shards, Node 2 (16 CPUs) leads 24/32 shards
- Auto-rebalance on node join/leave
**Status:** Deferred — requires cluster management layer.

### F2: Umbrella Package Split — DONE
**Problem:** Embedded users get Ranch/TCP deps they don't need.
**Design:** Split into `ferricstore` (core) + `ferricstore_server` (TCP/HTTP).
**Status:** IMPLEMENTED — umbrella project with `apps/ferricstore/` (core, no TCP deps) + `apps/ferricstore_server/` (standalone TCP/HTTP with Ranch).

### F3: ACL v2 Implementation — IN PROGRESS
**Problem:** Key patterns stored but not enforced, plaintext passwords, missing categories.
**Design:** See `docs/acl-design.md` — comprehensive 3-phase plan with user stories.
**Status:** Passwords upgraded to PBKDF2-SHA256 (100K iterations). Key pattern enforcement at dispatch time in progress. ACL SAVE/LOAD deferred.

## STILL OPEN

### Q5: Cross-Shard 2PC Transactions
**Spec says:** Cross-shard transactions use 2PC protocol.
**Current impl:** MULTI/EXEC works for commands, cross-shard atomicity relies on per-command Raft.
**Question:** Should we implement 2PC now or defer?

### Q7: Encryption at Rest (Section 6.5)
**Spec says:** Optional AES-256-GCM encryption for Bitcask data files.
**Current impl:** Not implemented. Data files are plaintext.
**Decision:** Deferred — requires Rust NIF changes to encrypt/decrypt in the write/read path.

### Q9: Async Tokio vs Yielding NIFs (Section 2B.5) — RESOLVED
**Spec says:** Every Bitcask IO operation submits work to Tokio and returns immediately.
**Decision:** Yielding NIFs (consume_timeslice + schedule_nif) instead of Tokio.
**Rationale:** For CPU-bound work (HNSW, bloom, CMS), yielding is the right approach — moving CPU work to Tokio doesn't save cycles, just moves where they run. For IO-bound work (fsync, pread), NVMe completes in ~50-200μs — too fast for Tokio's submit/callback overhead to matter. Yielding gives the BEAM scheduler the same benefit (run other processes between slices) with 10x less complexity. Tokio would only help for >1ms IO operations (spinning disks, network storage). On NVMe, the difference is negligible.

### Q14: Merge IO Priority (Section 2E.11)
**Spec says:** IOPRIO_CLASS_BE for background merge IO.
**Current impl:** No IO priority setting.
**Decision:** Linux-specific optimization, deferred.

### Q15: Follower merge gating (Section 2E.4)
**Spec says:** Only leader runs merge.
**Current impl:** Single-node, always leader.
**Decision:** Not applicable until multi-node.

### Q18: Hot cache hint file (Section 2C.6 Step 4)
**Spec says:** Shutdown writes hot cache hint file for next startup warmup.
**Current impl:** Hint files written per Bitcask file, but no separate ETS cache hint.
**Decision:** The Bitcask hint files serve the same purpose — keys are warmed from hints on startup.

---

## Implementation Status

### Core (complete):
- All 250+ Redis commands (Strings, Hash, List, Set, ZSet, Stream, Geo, JSON, Bitmap, HyperLogLog, Bloom, Cuckoo, CMS, TopK, Vector)
- RESP3 protocol (parser + encoder, HELLO 3 only)
- Raft consensus (ra library, all writes through Raft)
- Single-table ETS keydir with LFU eviction (v2 architecture)
- Umbrella project: `apps/ferricstore/` (core) + `apps/ferricstore_server/` (TCP/HTTP)
- Collection promotion (Hash/Set/ZSet to dedicated Bitcask)
- Merge/compaction (scheduler, manifest, semaphore)
- NIF yielding (all NIFs on Normal scheduler, consume_timeslice)
- Sliding window pipeline dispatch (active:once)
- CLIENT TRACKING invalidation wiring
- ACL (SETUSER/DELUSER/LIST/GETUSER/WHOAMI, PBKDF2-SHA256 passwords)
- TLS listener (Ranch SSL)
- Audit logging (ETS ring buffer)
- Health checks + HTML dashboard
- Prometheus metrics
- Embedded Elixir API
- Sandbox isolation
- L1 per-connection cache (~5ns reads, CLIENT TRACKING invalidation)
- Hybrid Logical Clock (spec 2G.6) — :atomics-based, lock-free
- Namespace-aware group commit batcher (spec 2F.3)
- Cross-shard 2PC Transaction Coordinator
- ClusterHelper + multi-node :peer tests

### v2 Architecture (in progress):
- mmap-backed probabilistic structures (Bloom, Cuckoo, CMS, TopK, TDigest) — DONE
- mmap-backed HNSW vector index (.v + .hnsw files) — DONE
- v2 pure stateless Rust NIFs (append_record, pread_at, scan_file, hint files, copy_records) — DONE
- 7-tuple ETS keydir {key, value|nil, expire, lfu, file_id, offset, value_size} — IN PROGRESS
- Wire v2 NIFs into Shard GenServer — IN PROGRESS
- Remove Rust keydir HashMap + Mutex (~80MB RAM savings) — IN PROGRESS
- Tokio async IO for cold reads — IN PROGRESS
- ResourceBinary zero-copy for embedded mode — IN PROGRESS
- Sendfile for large GETs (>64KB) in standalone mode — IN PROGRESS
- ACL key pattern enforcement at dispatch — IN PROGRESS

### Deferred:
- Encryption at rest (Q7) — requires Rust NIF encrypt/decrypt
- Phoenix LiveView dashboard — current HTML dashboard works
- Kubernetes Helm chart
- Jepsen-style testing
- Client libraries (Python, Go, Node.js)
- ACL SAVE/LOAD file persistence
- Shard rebalance (F1) — requires cluster management layer

*Only add questions here if the spec does not provide a clear answer.*
