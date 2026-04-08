# Robustness Gaps — Design & Implementation Plan

Known architectural gaps that affect correctness, reliability, or operational
visibility in production cluster deployments. Ordered by severity.

---

## 1. Clock Skew in TTL Expiration

**Severity: Critical (data correctness)**

**Status: DONE**

All 57 TTL/expiry sites migrated from `System.os_time(:millisecond)` to
`Ferricstore.HLC.now_ms()` across 17 files. HLC piggybacking on Raft commands
is wired up (state machine merges leader timestamps on followers). Drift
monitoring via telemetry is built into the HLC module.

See [hlc-clock-migration.md](hlc-clock-migration.md) for the original design.

---

## 2. Error Propagation in the Raft Write Path

**Severity: Critical (silent data loss / caller confusion)**

**Status: MOSTLY DONE (architecture change made it largely unnecessary)**

The original concern was about the Batcher silently dropping errors. The write
path has since been redesigned:

- **Quorum writes** now go directly through `Router.quorum_write` →
  `ra.pipeline_command` → `wait_for_ra_applied` with a 10s timeout. This
  bypasses the Batcher entirely. Errors are propagated:
  - `{:error, :noproc}` → falls back to Shard GenServer
  - `{:rejected, {:not_leader, ...}}` → retries with new leader
  - Timeout → returns `{:error, "ERR write timeout"}`
- **State machine crash protection** — catch-all `apply/3` clause prevents ra
  from crashing on unknown commands. `value_for_ets/2` and `to_disk_binary/1`
  have catch-all clauses for non-primitive types.

Remaining minor gap: if `StateMachine.apply` crashes on a known command type
(not caught by the catch-all), the caller gets a 10s timeout instead of a
specific error. This is acceptable — the crash is logged by ra and the caller
knows the write failed.

See [raft-error-propagation.md](raft-error-propagation.md) for the original design.

---

## 3. Shard GenServer is a Single Point of Serialization

**Severity: Medium (throughput under mixed workloads)**

**Status: DONE**

Both Phase 1 and beyond are implemented. The Shard GenServer is fully bypassed
on the hot path:

- **Reads**: `Router.get` reads directly from ETS (`ets_get_full`). Cold reads
  go to Bitcask NIF via `v2_pread_at` — no GenServer call. Only edge cases
  (invalid file ref) fall back to GenServer.
- **Quorum writes**: `Router.quorum_write` → `ra.pipeline_command` directly.
  No GenServer serialization.
- **Async writes**: `Router.async_write` writes to ETS + BitcaskWriter cast +
  fire-and-forget Raft. No GenServer serialization.

The Shard GenServer is only used as a fallback when ra is unavailable
(`{:error, :noproc}`). Phase 2 (separate read/write GenServers) is unnecessary.

---

## 4. Compaction During Replication

**Severity: Medium (data sync correctness)**

**Status: NOT IMPLEMENTED**

The design doc says `pause_writes` blocks compaction, but `Merge.Scheduler`
runs independently. If compaction fires during a data sync copy, the file list
can change mid-copy.

### Design

**Option A: Pause compaction during sync**

Add a `sync_in_progress` flag that `Merge.Scheduler` checks:

```elixir
# In Shard, when pause_writes is called:
def handle_call({:pause_writes}, _from, state) do
  Ferricstore.Merge.Semaphore.pause(state.index)
  # ... existing flush logic
end

# In Merge.Scheduler, before starting a merge job:
def maybe_start_merge(shard_index) do
  if Semaphore.paused?(shard_index) do
    :skip  # retry next interval
  else
    do_merge(shard_index)
  end
end
```

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/store/shard.ex` | Call `Semaphore.pause/resume` in pause/resume_writes |
| `lib/ferricstore/merge/semaphore.ex` | Add `pause/1`, `resume/1`, `paused?/1` |
| `lib/ferricstore/merge/scheduler.ex` | Check `paused?` before starting merge |

---

## 5. Recovery After Crash with Pending Async Writes

**Severity: Medium (operational visibility)**

**Status: NOT IMPLEMENTED**

Async writes go to ETS immediately, then fire-and-forget to Raft. On crash,
uncommitted async writes are lost. This is by design, but there's no mechanism
to detect or report how much data was lost.

### Design

Write a periodic checkpoint of the async pending count to a file. On startup,
compare checkpoint with actual Raft `last_applied` to calculate lost writes.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/raft/async_apply_worker.ex` | Track pending count |
| `lib/ferricstore/application.ex` | Recovery check on startup |

---

## 6. Keydir Memory Accounting is Approximate

**Severity: Low (eviction accuracy)**

**Status: NOT IMPLEMENTED**

`MemoryGuard` estimates ETS memory via `word_size * :ets.info(table, :memory)`.
This misses: Bitcask value bytes cached in ETS (off-heap refc binaries),
compound key overhead, promotion markers.

### Design

Sample N random ETS entries periodically (every 30s), measure binary sizes,
extrapolate to estimate total off-heap memory. Expensive, so run infrequently.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/memory_guard.ex` | Add binary sampling to memory calculation |

---

## 7. No Read-Your-Writes Guarantee Across Nodes

**Severity: Low (consistency model)**

**Status: NOT IMPLEMENTED**

Client writes to leader, gets `:ok`. Immediately reads from a follower — the
follower might not have applied the entry yet. Single-node deployments are not
affected (reads and writes use the same ETS).

### Design

**Follower read with index check (preferred):**

The Router checks the local shard's `last_applied` against the cluster leader's
`commit_index`. If they match, local read is used. If not, forward the read to
the leader. This adds a network round-trip only when the follower is behind.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/store/router.ex` | Add `consistent_get` with index check |
| `lib/ferricstore/raft/cluster.ex` | Add `get_commit_index` helper |

---

## Implementation Priority

| # | Gap | Severity | Effort | Status |
|---|-----|----------|--------|--------|
| 1 | HLC clock migration | Critical | Medium | DONE |
| 2 | Raft error propagation | Critical | Medium | DONE (architecture change) |
| 3 | Direct ETS reads / bypass GenServer | Medium | Low | DONE |
| 4 | Compaction during replication | Medium | Low | Not implemented |
| 5 | Async write loss reporting | Medium | Low | Not implemented |
| 6 | Keydir memory accuracy | Low | Low | Not implemented |
| 7 | Read-your-writes | Low | Medium | Not implemented |
