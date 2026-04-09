# Robustness Gaps

Production-relevant issues. Raft is always enabled in production — Shard
GenServer fallback paths are not listed.

---

## 1. State Machine flush_pending_writes Failure

**Severity: Critical (data loss)**

In `state_machine.ex`, `flush_pending_writes` calls `NIF.v2_append_batch_nosync`.
If it fails (ENOSPC, EIO), the error is logged but ETS entries remain with
`file_id = :pending`. The state machine returns success to ra, which commits
the entry. All replicas have the same corrupted state.

On restart, `recover_keydir` finds entries with `:pending` file_id — they
point to nothing on disk. Data is lost.

**Impact:** Quorum replication doesn't help because the state machine is
deterministic — the same failure happens on all replicas.

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex:1585-1601`

---

## 2. AsyncApplyWorker Unbounded Retry Buffer

**Severity: Medium (memory exhaustion, async mode only)**

When Raft submissions fail, commands queue in `retry_buffer` (`:queue`).
`@max_retries = 10` limits retries per command but doesn't bound the queue
size. During a Raft election (seconds of downtime), thousands of async
writes accumulate without limit.

**Impact:** Only affects async durability mode. Memory could grow unbounded
during Raft unavailability.

**File:** `apps/ferricstore/lib/ferricstore/raft/async_apply_worker.ex:362-378`

---

## 3. Cross-Shard Transaction Timeout Stale Messages

**Severity: Medium (incorrect results)**

`Router.wait_for_ra_applied` has a 10s timeout. If it fires, the caller gets
`{:error, "ERR write timeout"}`. But the `{:ra_event, _, {:applied, _}}`
message may arrive later in the process mailbox. The next `wait_for_ra_applied`
call picks up the stale event and returns the wrong result for a different
command.

This affects the connection process (each TCP connection is a single process
that calls `wait_for_ra_applied` serially).

**Impact:** After a timeout, the next write on the same connection could get
the previous write's result. Rare — requires a 10s timeout followed by
another write on the same connection.

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex:101-138`

---

## 4. Compaction File Cleanup Can Crash Shard

**Severity: Medium (crash loop)**

In `shard.ex`, if `NIF.v2_copy_records` fails during compaction, `File.rm(dest)`
is called without try-catch. If `File.rm` also fails (permission, locked file),
the exception propagates and crashes the Shard GenServer. Supervisor restarts
the shard, which may trigger compaction again → crash loop.

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex:556-580`

---

## 5. Promotion Delete Failures Silently Ignored

**Severity: Medium (storage leak)**

When deleting a key that has a promoted instance (dedicated storage for large
collections like hashes), the promoted instance cleanup can fail silently.
The key is deleted from the main keydir but the promoted instance's storage
remains on disk, growing unbounded.

**File:** `apps/ferricstore/lib/ferricstore/store/shard/writes.ex`
