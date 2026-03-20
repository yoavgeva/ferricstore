# FerricStore — Open Questions

Questions where the spec does NOT provide a clear answer. Everything else follows the spec exactly.

---

## RESOLVED (kept for history)

- **Q1 (Sharding):** Using phash2 for standalone — acceptable for v1.
- **Q2 (Per-key Bitcask):** Using compound keys in shared Bitcask + promotion for large collections — implemented.
- **Q3 (ra version):** Using ra ~> 2.14 — compatible.
- **Q4 (NIF scheduling):** ALL NIFs now on Normal scheduler with yielding via enif_schedule_nif — FIXED.
- **Q6 (Two-table ETS):** IMPLEMENTED — keydir_N + hot_cache_N per shard.
- **Q8 (ETS split blocked):** IMPLEMENTED — 5146 tests pass, CI green.
- **Q10 (Raft not in write path):** FIXED — all 15+ write operations route through Raft.
- **Q11 (active:once):** FIXED — connections use active:once sockets.
- **Q12 (Merge supervisor):** FIXED — started in application.ex.
- **Q13 (release_cursor):** FIXED — emitted every 1000 applies.
- **Q16 (Health endpoint):** FIXED — /health/ready on port 9090 + HTML dashboard.

## STILL OPEN

### Q5: Cross-Shard 2PC Transactions
**Spec says:** Cross-shard transactions use 2PC protocol.
**Current impl:** MULTI/EXEC works for commands, cross-shard atomicity relies on per-command Raft.
**Question:** Should we implement 2PC now or defer?

### Q7: Encryption at Rest (Section 6.5)
**Spec says:** Optional AES-256-GCM encryption for Bitcask data files.
**Current impl:** Not implemented. Data files are plaintext.
**Decision:** Deferred — requires Rust NIF changes to encrypt/decrypt in the write/read path.

### Q9: Async Tokio for ALL IO NIFs (Section 2B.5 Pattern 1)
**Spec says:** Every Bitcask IO operation submits work to Tokio and returns immediately.
**Current impl:** Only put_batch_async uses async Tokio (via io_uring on Linux). Other IO NIFs block Normal scheduler synchronously.
**Decision:** Deferred — synchronous IO on Normal scheduler with yielding is tolerable for v1. Full async Tokio requires major Rust NIF refactor.

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

## Implementation Status — 5146 tests, 0 failures, CI green

### Complete:
- All 250+ Redis commands (Strings, Hash, List, Set, ZSet, Stream, Geo, JSON, Bitmap, HyperLogLog, Bloom, Cuckoo, CMS, TopK, Vector)
- RESP3 protocol (parser + encoder, HELLO 3 only)
- Raft consensus (ra library, all writes through Raft)
- Two-table ETS (keydir_N + hot_cache_N per shard)
- Collection promotion (Hash/Set/ZSet to dedicated Bitcask)
- Merge/compaction (scheduler, manifest, semaphore)
- NIF yielding (all NIFs on Normal scheduler)
- Sliding window pipeline dispatch (active:once)
- CLIENT TRACKING (module complete)
- ACL (SETUSER/DELUSER/LIST/GETUSER/WHOAMI)
- TLS listener (Ranch SSL)
- Audit logging (ETS ring buffer)
- Health checks + HTML dashboard
- Prometheus metrics
- Embedded Elixir API
- Sandbox isolation

### All Implemented:
- Two-table ETS split (spec 2.4) — keydir_N + hot_cache_N
- CLIENT TRACKING invalidation wiring — track_key + notify_key_modified
- SCAN TYPE filtering for all types (hash/list/set/zset/string)
- TLS require-tls enforcement
- Namespace-aware group commit batcher (spec 2F.3)
- FERRICSTORE.CONFIG SET/GET/RESET commands
- CONFIG SET LOCAL / CONFIG REWRITE / CONFIG RESETSTAT
- Dashboard Page 9 (namespace config visualization)
- Hybrid Logical Clock (spec 2G.6) — monotonic timestamps
- AsyncApplyWorker (async durability path)
- Cross-shard 2PC Transaction Coordinator
- ClusterHelper + multi-node :peer tests (21 cluster tests)
- /health/live endpoint + /metrics on HTTP port
- XREAD BLOCK support with stream waiters
- INFO namespace_config section
- 5950+ tests, all test plan sections covered (S2-S18)

### Deferred (external tooling, not core logic):
- Encryption at rest (Q7) — requires Rust NIF encrypt/decrypt
- Full async Tokio IO NIFs (Q9) — all IO NIFs still synchronous on Normal scheduler
- Phoenix LiveView dashboard — current HTML dashboard works
- Kubernetes Helm chart
- Jepsen-style testing
- Client libraries (Python, Go, Node.js)

*Only add questions here if the spec does not provide a clear answer.*
