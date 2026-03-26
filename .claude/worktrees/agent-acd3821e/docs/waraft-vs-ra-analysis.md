# waraft vs ra Analysis

**Date:** 2026-03-23
**Context:** Evaluating whether WhatsApp's waraft could replace rabbitmq/ra as our Raft layer, specifically to solve the WAL fsync throughput bottleneck.

---

## 1. Overview

### waraft (WhatsApp)
- **Repo:** https://github.com/WhatsApp/waraft
- **License:** Apache 2.0
- **Language:** 100% Erlang, zero external dependencies
- **Stars:** ~603, 43 forks
- **Description:** Raft consensus library built at Meta/WhatsApp for message storage. Deployed in production across 5+ datacenters for strongly consistent message storage.
- **Claimed throughput:** ~200K transactions/sec in a 5-node cluster

### ra (RabbitMQ)
- **Repo:** https://github.com/rabbitmq/ra
- **License:** MPL 2.0 (with Apache 2.0 option)
- **Language:** Erlang
- **Description:** Multi-Raft implementation designed to run thousands of Raft groups per node. Powers RabbitMQ quorum queues, streams, and Khepri.

---

## 2. Maintenance Status

### waraft
- **Last commit:** March 14, 2026 (actively maintained)
- **Total commits:** 398
- **Recent activity:** Consistent commits every few days throughout Jan-Mar 2026
- **Open issues:** 2
- **Open PRs:** 4
- **Releases:** **None.** No tagged releases on GitHub.
- **CI:** Tests against OTP 24 only (single version in build matrix)
- **Contributors:** Multiple (internal Meta workflow with "Reviewed By" annotations and differential revision numbers, suggesting Phabricator-style internal review)
- **Documentation:** Sparse. README has a single example and the 200K/s claim. No architecture docs, no API reference, no guides.

### ra
- Mature project with regular releases, extensive documentation, detailed internals docs
- Widely deployed in RabbitMQ (millions of production nodes)
- Supports OTP 25+
- Well-documented API, internals guide, and configuration options

**Verdict:** waraft is actively developed but has no releases, minimal docs, and tests on a single OTP version. ra is production-hardened with proper release engineering.

---

## 3. Architecture Comparison

| Feature | ra (RabbitMQ) | waraft (WhatsApp) |
|---------|--------------|-------------------|
| **OTP gen behavior** | `gen_batch_server` (custom) | `gen_statem` (server) + `gen_server` (log, storage, acceptor) |
| **Multi-group support** | First-class. Thousands of groups share one WAL | Per-partition processes. No shared WAL |
| **WAL** | Single shared `ra_log_wal` process per ra system | **No WAL.** Log is a pluggable behavior (`wa_raft_log`). Only shipped provider is `wa_raft_log_ets` (pure in-memory) |
| **Log persistence** | WAL -> segment files (durable) | Delegated to log provider. Open-source version: ETS only (volatile). WhatsApp uses internal providers not in the repo |
| **State machine** | `ra_machine` behavior (callbacks: `init`, `apply`, `state_enter`, `snapshot_*`) | `wa_raft_storage` behavior (callbacks: `storage_open`, `storage_apply`, `storage_read`, `storage_create_snapshot`, `storage_open_snapshot`) |
| **Command submission** | `ra:process_command/2,3` (sync), `ra:pipeline_command/3,4` (async pipelined) | `wa_raft_acceptor:commit/2,3` (sync), `wa_raft_acceptor:commit_async/3,4` (async). No pipeline equivalent |
| **Batching** | Adaptive batching in WAL (batch grows during fsync latency) | Commit batching in leader: configurable interval (2ms default) and max entries (15 default) |
| **I/O sync strategy** | Configurable: `datasync` / `sync` / `none` via `ra_log_wal` config | Durable state only: `prim_file:sync/1` for term/voted_for. Log sync depends on provider (ETS provider = none) |
| **File operations** | `file:write/2` + `file:datasync/1` (hardcoded to Erlang `file` module) | No file I/O in open-source log. `wa_raft_durable_state` uses `prim_file:write_file` + `file:rename` + `prim_file:sync` |
| **Snapshots** | Built-in snapshot support with installable snapshots | Snapshot support via storage callbacks. ETS example uses `ets:tab2file/2` |
| **Transport** | Erlang distribution (default) | Pluggable transport behavior (`wa_raft_transport`). Default: Erlang distribution |
| **Cluster management** | `ra:add_member/3`, `ra:remove_member/3` | Config-based membership changes via `wa_raft_server:adjust_config/3` |
| **Counters/metrics** | `ra_counters` (atomics-based, zero-cost) | `wa_raft_metrics` module |
| **Read semantics** | Consistent queries, local queries, members query | Strong reads via `wa_raft_acceptor:read/2,3` |

---

## 4. Does waraft Solve Our Bottleneck?

### Our bottleneck (recap)
```
Single ra_log_wal process
  -> file:write(Batch)
  -> file:datasync(Fd)
  -> ~20ms on macOS APFS
  -> 50 fsyncs/sec * ~12 writes/batch = ~600 writes/sec ceiling
  -> Cannot be bypassed: ra_log_wal uses hardcoded file:datasync
```

### waraft's I/O path

**The open-source waraft has no WAL and no durable log at all.**

The only shipped log provider (`wa_raft_log_ets`) is explicitly documented as:
> "a completely in-memory RAFT log provider [...] should not be used when any durability guarantees are required against node shutdown."

The `wa_raft_log` behavior defines a `flush/1` callback with the semantics:
> "Flush log to disk on a best-effort basis. The return value is ignored."

This means:
1. WhatsApp uses an **internal, proprietary durable log provider** that is not open-sourced
2. The open-source version is essentially a **consensus-only framework** — you must bring your own durable log
3. We would need to **write our own durable log provider** that implements the `wa_raft_log` callbacks

### Does it help with fsync?

**No, not directly.** waraft sidesteps the problem by not including a durable log. If we wrote our own log provider using the same `file:datasync` approach, we'd have the exact same bottleneck. The advantage is that the interface IS pluggable — we could write a Rust NIF-backed log provider.

However, we could achieve the same thing by forking ra and replacing `ra_log_wal`'s file operations with our NIF, which is less work than switching the entire Raft layer.

### Comparison of I/O path pluggability

| Aspect | ra | waraft |
|--------|-----|--------|
| Log I/O pluggable? | **No.** `ra_log_wal` hardcodes `file:write` + `file:datasync` | **Yes.** `wa_raft_log` is a behavior. Implement your own provider |
| Can inject NIF for I/O? | Only by forking `ra_log_wal.erl` | Yes, implement `wa_raft_log` callbacks that call NIFs |
| Effort to inject NIF | Moderate (fork one module, ~1500 LOC) | High (implement entire durable log provider from scratch) |
| Risk | Breaking ra internals (WAL <-> segment writer coupling) | Building a production-grade WAL from scratch |

---

## 5. Migration Feasibility

### Scope of changes

If we switched from ra to waraft:

1. **State machine rewrite:** ra's `ra_machine` callbacks -> waraft's `wa_raft_storage` callbacks. Different interface (e.g., `apply/2` vs `storage_apply/3`). Moderate effort.

2. **Command submission rewrite:** `ra:process_command` / `ra:pipeline_command` -> `wa_raft_acceptor:commit` / `commit_async`. **No pipeline_command equivalent** — waraft's commit_async returns immediately but doesn't pipeline multiple in-flight commands the way ra does. This is a throughput regression risk.

3. **Write our own durable log provider:** This is the big one. We'd need to implement the `wa_raft_log` behavior with:
   - Durable append (WAL or equivalent)
   - fsync strategy
   - Log compaction / rotation
   - Recovery after crash
   - This is essentially writing our own WAL from scratch

4. **Write our own durable storage provider:** The ETS storage is demo-only. We'd need to implement `wa_raft_storage` with actual persistence.

5. **Lose ra ecosystem:** No `ra_counters`, no `ra_system` multi-system support, no `ra_leaderboard`, no battle-tested segment writer. We'd be building much of this infrastructure ourselves.

6. **OTP compatibility:** waraft only tests on OTP 24. We'd need to verify/fix OTP 26+ compatibility.

### Estimated effort
- State machine port: ~1-2 weeks
- Durable log provider: ~4-8 weeks (this is essentially building a WAL)
- Durable storage provider: ~2-4 weeks
- Integration, testing, edge cases: ~4-6 weeks
- **Total: ~3-5 months** of focused work, with high risk

### What we'd gain
- Pluggable log I/O (can use our Rust NIF for writes + io_uring fsync)
- Pluggable transport (could use a custom protocol instead of Erlang distribution)
- Potentially better batching control (configurable batch interval + max entries)

### What we'd lose
- Production-hardened WAL (ra's is battle-tested in millions of RabbitMQ deployments)
- Pipeline commands (key for throughput)
- Shared WAL amortization across Raft groups
- Comprehensive documentation and community support
- `gen_batch_server` efficiency (waraft uses standard gen_statem)

---

## 6. Other Alternatives

### raft-rs (TiKV) via Rustler NIF
- **Repo:** https://github.com/tikv/raft-rs
- **What it is:** Rust implementation of Raft core consensus only. No WAL, no transport, no storage — you bring everything.
- **Storage trait:** Fully pluggable. You implement the `Storage` trait to provide log persistence.
- **Batching:** The `Ready` struct batches all pending state changes. You control when/how to persist.
- **I/O path:** Completely under our control — we could use io_uring directly from Rust.
- **Feasibility as NIF:** Possible via Rustler, but the entire Raft state machine loop would run in Rust. The BEAM would send commands to the NIF and receive results. This is architecturally very different — we'd lose OTP supervision, gen_statem state introspection, observer visibility, and distributed Erlang integration.
- **Risk:** Very high. We'd be writing a distributed systems framework, not just swapping a library.
- **Effort:** 6-12 months minimum. Not recommended.

### Graft (Elixir)
- Academic Raft implementation with runtime monitoring.
- Not production-ready. Not suitable.

### RaftedValue (Elixir)
- Simple gen_statem-based Raft. No WAL, no batching optimizations.
- Toy implementation. Not suitable.

### etcd's Raft (Go)
- Would require a Go sidecar process communicating via ports.
- Latency overhead of serialization + IPC makes this impractical for our throughput target.

### Custom WAL NIF (keep ra, replace the I/O layer)
- Fork `ra_log_wal.erl` and replace `file:write/2` + `file:datasync/1` with NIF calls to our existing Rust Bitcask backend (which already supports io_uring).
- **Effort:** ~2-4 weeks to fork and wire up, ~2 weeks to test.
- **Risk:** Moderate — ra_log_wal is well-understood, and we only change the I/O syscalls.
- This is the minimum-viable approach.

---

## 7. Recommendation

**Do not switch to waraft.** Stay with ra and fork the WAL I/O path.

### Reasoning

1. **waraft doesn't ship a durable log.** The open-source version is missing the most critical component for our use case. Switching to waraft means we'd spend months building the very thing we're trying to optimize (a WAL), only to end up in the same place — needing to wire up io_uring fsync.

2. **waraft's pluggability is a red herring for us.** Yes, the log interface is pluggable. But ra's WAL is a single ~1500 LOC module where the file operations are concentrated in a handful of functions. Forking that module and replacing `file:write` + `file:datasync` with a NIF call is less work than implementing the entire `wa_raft_log` behavior with durability.

3. **No pipeline_command.** ra's `pipeline_command` is a key throughput mechanism. waraft's `commit_async` is not equivalent — it doesn't maintain in-flight request tracking or correlation IDs the way ra's pipeline does.

4. **No shared WAL amortization.** ra's single-WAL-per-system design means one fsync covers writes from all Raft groups. waraft's per-partition log means we'd need to solve the fsync-per-group problem ourselves (or build a shared log layer, which is... rebuilding ra_log_wal).

5. **Operational risk.** No releases, OTP 24 only, sparse docs. We'd be taking on significant operational risk for unclear benefit.

### Recommended path

```
Phase 1 (2-4 weeks): Fork ra_log_wal, replace file:write + file:datasync
         with NIF calls to our Rust io_uring backend.

Phase 2 (1-2 weeks): Benchmark. If the shared WAL itself is the bottleneck
         (single process serialization), consider ra_log_wal per-system
         sharding (multiple WAL processes, partition Raft groups across them).

Phase 3 (if needed): If ra's architecture is fundamentally limiting,
         THEN consider waraft or raft-rs as a longer-term migration,
         with the durable log provider we've already built for Phase 1
         as a reusable component.
```

This gives us the io_uring fsync improvement (our actual bottleneck) with minimal risk and 2-4 weeks of work, versus 3-5 months for a waraft migration that might not even be faster.
