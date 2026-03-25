# Cross-Shard Atomic Transactions Design

## Status

**Draft** -- March 2026

**Related files**:
- `apps/ferricstore/lib/ferricstore/transaction/coordinator.ex` -- current 2PC coordinator
- `apps/ferricstore/lib/ferricstore/store/shard.ex` -- `prepare_tx`/`commit_tx`/`rollback_tx` handlers
- `apps/ferricstore_server/lib/ferricstore_server/connection.ex` -- MULTI/EXEC/WATCH/DISCARD dispatch
- `apps/ferricstore/lib/ferricstore/store/router.ex` -- key-to-shard routing, hash tag support
- `apps/ferricstore/lib/ferricstore/raft/state_machine.ex` -- `{:batch, commands}` Raft command

---

## 1. Problem Statement

Users expect MULTI/EXEC to be atomic across all keys, but keys may hash to
different shards. Each shard is an independent Raft group. The current
implementation has a structural gap: if command 3 of 5 fails, commands 1-2
are already applied to ETS and queued for Bitcask persistence. There is no
undo mechanism. The `rollback_tx` handler in `Shard` acknowledges this directly:

```
# Writes were already applied during prepare (ETS + pending batch).
# A full rollback would require tracking and reversing each mutation.
```

This means a MULTI block like:

```
MULTI
SET {user}:name alice      # shard 0
SET {user}:email a@b.com   # shard 0
SET counter 42             # shard 2 (different shard)
EXEC
```

...is not truly atomic. If shard 2 is temporarily unavailable during prepare,
shard 0's writes are already committed and visible. The client receives `nil`
(EXECABORT), but the first two SETs have taken effect.

---

## 2. Current Implementation

### 2.1 MULTI/EXEC Connection State

`connection.ex` manages transaction state per-connection:

- `multi_state: :none | :queuing` -- toggled by MULTI/EXEC/DISCARD.
- `multi_queue: [{cmd, args}]` -- commands accumulated in reverse order
  (O(1) prepend, reversed at EXEC time).
- `multi_queue_count` -- tracked separately to avoid O(N) `length/1`.
- `watched_keys: %{key => version}` -- populated by WATCH, checked at EXEC.

During `:queuing`, all non-passthrough commands are intercepted and queued
with `+QUEUED` responses. Queue-time validation catches syntax errors early
via `validate_command/3` which dry-runs through `Dispatcher.dispatch/3` with
a no-op store.

### 2.2 EXEC Dispatch

`dispatch("EXEC", ...)` calls `execute_transaction/1`, which delegates to
`Ferricstore.Transaction.Coordinator.execute/3` with the reversed queue,
watched keys map, and sandbox namespace.

### 2.3 Transaction Coordinator (`coordinator.ex`)

The Coordinator is **stateless** (no GenServer). It:

1. **Checks WATCH** -- `watches_clean?/1` compares saved versions against
   `Router.get_version/1`. If any watched key's shard write-version changed,
   returns `nil` immediately. The version is the sum of the Shard's internal
   `write_version` counter and the shared `WriteVersion` atomics counter
   (`state.write_version + WriteVersion.get(state.index)`). This is a
   **shard-level** version, not per-key -- any write to the shard bumps it,
   causing false positives.

2. **Classifies shards** -- `classify_shards/2` groups commands by target
   shard using `Router.shard_for/1` on each command's first key argument.
   Builds a `shard_groups` map (`%{shard_idx => [{orig_idx, cmd, args}]}`)
   and an `index_map` for result reassembly.

3. **Single-shard fast path** -- if all commands target one shard, executes
   them sequentially through the Dispatcher with a Router-backed store map.
   No 2PC overhead. Not atomic at the Raft level (commands are individual
   Router calls, not a single `{:batch, ...}` Raft command).

4. **Cross-shard 2PC path** -- for multi-shard transactions:
   - Phase 1 (Prepare): sends `{:prepare_tx, tx_id, commands}` to each shard
     via `GenServer.call` concurrently using `Task.async`. Each shard executes
     commands within its `handle_call` (serialized, no interleaving) using
     a local store that writes ETS directly and sends `{:tx_pending_write, ...}`
     messages to self for deferred Bitcask persistence.
   - Phase 2 (Commit): sends `{:commit_tx, tx_id}` to all shards if all
     returned `{:prepared, tx_id, results}`. This merely cleans up
     `staged_txs` metadata.
   - Rollback: sends `{:rollback_tx, tx_id}` to prepared shards if any shard
     failed. This also merely cleans up `staged_txs` -- writes are NOT undone.

### 2.4 Specific Problems

| # | Problem | Impact | Severity |
|---|---------|--------|----------|
| 1 | **No rollback on partial failure.** `rollback_tx` is a no-op for already-applied writes. | Cross-shard MULTI is not atomic. Prepare failure on shard B leaves shard A's writes committed and visible. | **Critical** |
| 2 | **Single-shard fast path is not Raft-atomic.** Commands execute individually through Router, not as a `{:batch, ...}` Raft command. | If the process crashes mid-transaction, a partial prefix of commands is committed. With Raft enabled, individual commands go through `pipeline_command` separately. | High |
| 3 | **Shard-level WATCH granularity.** Version is per-shard, not per-key. Any write to the shard increments the version. | High false-positive WATCH abort rate under write load. A transaction watching `user:42` aborts if an unrelated `session:xyz` is written to the same shard. | Medium |
| 4 | **No isolation during prepare.** ETS writes are visible immediately during `prepare_tx`. Other readers see uncommitted state before Phase 2 completes. | Dirty reads within the prepare-to-commit window (~1-5ms for local shards). | Medium |
| 5 | **5-second call timeout.** `@call_timeout 5_000` in the Coordinator means a slow shard causes transaction abort. | Under load, legitimate transactions may be spuriously aborted. | Low |

---

## 3. Requirements

### Must Have

- R1: All commands in a MULTI block succeed or all have no effect (atomicity).
- R2: Users do not need to know about shards. Transparent cross-shard handling.
- R3: WATCH still works -- optimistic locking aborts the transaction if any
  watched key was modified between WATCH and EXEC.
- R4: Single-shard MULTI (all keys on the same shard) must be a fast path with
  no cross-shard coordination overhead.
- R5: Hash tags (`{tag}`) continue to co-locate related keys for users who
  want guaranteed single-shard transactions.

### Should Have

- R6: Performance -- cross-shard MULTI should not be more than 2-3x slower
  than single-shard MULTI for 2-shard cases.
- R7: No visible dirty reads -- other clients should not see partially-applied
  transaction state.
- R8: Per-key WATCH granularity instead of per-shard (reduce false positives).

### Nice to Have

- R9: Deadlock-free protocol (no key-level locking, or deterministic lock ordering).
- R10: Embedded API (`FerricStore.multi/1`) benefits from the same atomicity.
- R11: Graceful degradation -- if one shard is temporarily unreachable, the
  transaction aborts cleanly with no side effects.

---

## 4. Design Options

### Option A: Raft-Level 2PC with Prepare/Commit Log Entries

```
EXEC -->
  Phase 1 (Prepare):
    For each shard:
      Submit {:prepare_tx, tx_id, commands} to ra (Raft log entry)
      StateMachine validates all commands, writes to a staging area
      Returns {:prepared, tx_id, results} or {:abort, tx_id, reason}

    If ALL shards prepared --> Phase 2
    If ANY shard aborted  --> send {:abort_tx, tx_id} to all

  Phase 2 (Commit):
    For each shard:
      Submit {:commit_tx, tx_id} to ra
      StateMachine promotes staged writes to live state
      Returns results

    OR on abort:
      Submit {:abort_tx, tx_id} to ra
      StateMachine discards staged writes
```

**Pros:**
- True atomicity: staged writes are invisible until commit.
- Raft-durable: prepare and commit decisions survive crashes.
- Correct isolation: readers see old values until commit.

**Cons:**
- 2 Raft round-trips per shard (prepare + commit).
- Staging area adds memory pressure and StateMachine complexity. The staging
  area must be included in Raft snapshots, increasing snapshot size.
- Must handle coordinator crash between prepare and commit (the classic 2PC
  blocking problem). Two-phase commit cannot dependably recover from a failure
  of both the coordinator and a cohort member during the commit phase. Since
  each shard's Raft group is durable, a new coordinator can query shard state
  to resolve in-doubt transactions, but this recovery logic is significant.
- Key-level locking during the prepare-to-commit window to prevent
  concurrent writers from modifying staged keys. Lock duration is bounded
  by Raft latency (~1-2ms local) but still introduces contention.

### Option B: Optimistic Execution with Raft-Atomic Batches (Recommended)

```
EXEC -->
  1. Group commands by target shard.
  2. For single-shard case:
     Submit as one {:tx_batch, tx_id, commands, watch_versions} Raft entry.
     Atomic by construction (single Raft log entry = atomic application).
  3. For cross-shard case:
     a. For each shard in parallel:
        Send {:tx_validate, tx_id, commands, watch_keys} via GenServer.call.
        Shard dry-runs all commands (read-only) to check:
          - Type errors (INCR on a list, etc.)
          - WATCH version conflicts for keys on this shard
          - Key existence checks for NX/XX conditions
        Returns {:valid, tx_id} or {:invalid, tx_id, reason}
     b. If ALL shards return :valid:
        For each shard in parallel:
          Submit {:tx_batch, tx_id, commands, watch_versions} as a
          single Raft log entry via ra:pipeline_command.
          StateMachine applies all commands atomically (single apply/3 call).
          Returns {:ok, results}
     c. If ANY shard returns :invalid:
        Return EXECABORT to client. No writes occurred.
     d. Collect results, reassemble in original command order.
```

**Pros:**
- Validation is read-only -- no staged state, no locks, no rollback needed.
- Each shard's write phase is a single Raft log entry -- atomic by construction
  within that shard.
- One Raft round-trip per shard (parallel) for the write phase.
- Single-shard case reduces to one Raft log entry with zero coordination.
- No coordinator state to recover on crash. Stateless coordinator.
- Minimal StateMachine changes (extends existing `{:batch, commands}`).

**Cons:**
- Not strictly serializable across shards. Between validation and write,
  another client could modify a key, making the validation stale. This is
  the TOCTOU (time-of-check-to-time-of-use) gap.
- Mitigation: WATCH covers the TOCTOU gap for keys the client explicitly
  watches. The WATCH check is duplicated inside the StateMachine's `apply/3`
  for the `{:tx_batch}` command, making it atomic with the writes. For keys
  not watched, the validation-to-write window is typically <1ms.
- Cross-shard partial failure during the write phase (one shard's Raft
  accepts but another's fails) still results in partial application, handled
  by retry + idempotency (see section 6.5).

### Option C: CockroachDB-Style Parallel Commits

```
EXEC -->
  1. Group commands by target shard.
  2. Pick one shard as the "transaction record" holder (first key's shard).
  3. In parallel:
     - Write a STAGING transaction record to the holder shard via Raft.
     - For each shard: submit write intents (provisional values) via Raft.
  4. When all intents are confirmed written:
     - The transaction is implicitly committed (STAGING record + all
       intents confirmed = committed).
     - Lazily update the transaction record to COMMITTED.
  5. On conflict or failure:
     - Mark transaction record as ABORTED.
     - Intents are cleaned up lazily by readers or background GC.
```

This is the approach used by CockroachDB. Their "Parallel Commits" protocol
achieves one-round-trip strict serializability by writing a STAGING transaction
record and write intents in parallel, then treating the transaction as
committed when all intents are confirmed. The key insight is that a
coordinator crash does not block the protocol -- any reader encountering an
intent can resolve the transaction's status by checking the transaction record
and the status of all intents.

TiKV uses a similar approach based on Google's Percolator, where one key
is chosen as the "primary" (equivalent to the transaction record holder) and
other keys are "secondaries" with locks pointing to the primary. Conflict
detection uses a timestamp oracle for MVCC versioning.

**Pros:**
- One Raft round-trip total (parallel intents + record).
- No blocking -- readers resolve transaction status independently.
- Proven at scale (CockroachDB, TiKV/TiDB).

**Cons:**
- Massive implementation complexity. Every read path must check whether a
  value is an intent and resolve the transaction status by querying the
  transaction record.
- Requires MVCC. FerricStore has HLC but not multi-version storage. Adding
  MVCC means fundamental changes to the ETS keydir layout, Bitcask storage
  format, merge/compaction, and cold-read path.
- Intent resolution on the read path adds latency to ALL reads, not just
  transactional ones. The fast ETS read path in Router (`ets_get/3`) would
  need to check for intent markers.
- GC for abandoned intents (orphaned by coordinator crash) requires
  background scanning.

### Option D: Saga Pattern (Compensating Transactions)

```
EXEC -->
  Execute commands shard by shard sequentially.
  If shard N fails:
    For shards 0..N-1: send compensating commands (DEL for SET, etc.)
```

**Pros:**
- Simple to implement.
- One round-trip per shard (sequential, not parallel).

**Cons:**
- Compensating commands can fail too (double failure).
- Brief inconsistency window -- writes are visible before compensation.
- Not all commands have clean compensations (INCR by 5 then DECR by 5
  races with concurrent INCRs; GETSET destroys the old value; GETDEL is
  irreversible).
- Fundamentally not atomic -- observers see intermediate states.

---

## 5. Recommendation: Option B (Optimistic Execution with Raft-Atomic Batches)

Option B provides the best trade-off for FerricStore's architecture:

- **Matches Redis semantics.** Redis MULTI/EXEC without WATCH provides no
  isolation guarantee -- other clients can read and write keys between
  individual commands. WATCH provides the optimistic concurrency layer.
  FerricStore's Option B gives strictly stronger guarantees than Redis:
  per-shard atomicity via Raft batches, which Redis Cluster cannot provide
  at all (CROSSSLOT error on cross-slot MULTI).

- **Minimal complexity.** No staged state, no intent resolution on reads,
  no coordinator recovery protocol. The StateMachine already has
  `{:batch, commands}`. The new `{:tx_batch, ...}` is a small extension.

- **Performance.** Single Raft round-trip per shard, submitted in parallel.
  The validation phase is a fast GenServer.call (read-only, no disk I/O).
  Single-shard transactions are one Raft entry, matching the theoretical
  minimum.

- **Correct single-shard path.** A single `{:tx_batch, ...}` Raft entry
  gives true atomicity for the common case. This alone fixes Problem #2.

Option A (full 2PC) would be the right choice if we need strict serializability
across shards, but it doubles Raft round-trips and requires coordinator crash
recovery. Option C (parallel commits) would be right for a distributed SQL
database, but requires MVCC -- a fundamental storage architecture change.
Neither is necessary for Redis-compatible semantics.

---

## 6. Detailed Design

### 6.1 Transaction Flow: TCP MULTI/EXEC

No changes to `connection.ex` MULTI queuing. The existing queue-time
validation, passthrough commands, queue limits, and buffer management remain
as-is. The `execute_transaction/1` call to `Coordinator.execute/3` is the
only integration point.

#### 6.1.1 Single-Shard Fast Path

When `classify_shards/2` returns `{:single_shard, shard_idx}`:

```
Connection                 Coordinator              Raft (shard's ra group)
    |                          |                          |
    |--- EXEC ---------------→|                          |
    |                          |-- watches_clean?() ----→ |
    |                          |   (fast pre-check)       |
    |                          |                          |
    |                          |-- classify: single_shard |
    |                          |                          |
    |                          |-- ra:pipeline_command ---→|
    |                          |   {:tx_batch, tx_id,     |
    |                          |    commands,              |
    |                          |    watch_versions}        |
    |                          |                          |
    |                          |   StateMachine.apply/3:  |
    |                          |   1. check watch_versions|
    |                          |   2. apply all commands   |
    |                          |   3. return results       |
    |                          |                          |
    |                          |←-- {:ok, results} -------|
    |←-- array of results ----|                          |
```

The `{:tx_batch, ...}` command is submitted to Raft via `ra:pipeline_command/4`,
the same mechanism as `quorum_write` in Router. The StateMachine processes it
in a single `apply/3` call, making all commands atomic.

**Key change from current behavior:** Today the single-shard fast path
dispatches commands individually through `Dispatcher.dispatch/3` with a
Router-backed store map. Each command executes as an independent operation.
The new path bundles all commands into one Raft log entry.

#### 6.1.2 Cross-Shard Path

When `classify_shards/2` returns `{:multi_shard, shard_groups, index_map}`:

```
Connection       Coordinator          Shard 0 (GenServer)    Shard 2 (Raft)
    |                |                    |                      |
    |--- EXEC ------→|                    |                      |
    |                |                    |                      |
    |                |== Phase 1: VALIDATE (parallel) ===========|
    |                |--{:tx_validate}-→  |                      |
    |                |--{:tx_validate}----+------------------→   |
    |                |                    |                      |
    |                |   (read-only:      |   (read-only:        |
    |                |    type checks,    |    type checks,      |
    |                |    watch versions, |    watch versions,   |
    |                |    NX/XX conds)    |    NX/XX conds)      |
    |                |                    |                      |
    |                |←-{:valid}---------|                      |
    |                |←-{:valid}---------+----------------------|
    |                |                    |                      |
    |                |== Phase 2: WRITE (parallel, via Raft) ===|
    |                |--pipeline_command→  |                      |
    |                |--pipeline_command---+------------------→   |
    |                |                    |                      |
    |                |   (Raft commit)    |   (Raft commit)      |
    |                |                    |                      |
    |                |←-{:ok, results0}--|                      |
    |                |←-{:ok, results2}--+----------------------|
    |                |                    |                      |
    |                |-- reassemble in original command order ---|
    |←-- results ----|                    |                      |
```

Phase 1 (validate) and Phase 2 (write) each use `Task.async` + `Task.await_many`
for parallelism across shards.

### 6.2 StateMachine Changes

#### New command: `{:tx_batch, tx_id, commands, watch_versions}`

```elixir
# In Ferricstore.Raft.StateMachine

def apply(meta, {:tx_batch, tx_id, commands, watch_versions}, state) do
  # Step 1: Check idempotency -- if this tx_id was already applied, return
  # the cached result without re-applying.
  case check_tx_idempotency(state, tx_id) do
    {:hit, cached_result} ->
      {state, cached_result}

    :miss ->
      # Step 2: Validate WATCH versions for keys on this shard.
      watches_ok =
        Enum.all?(watch_versions, fn {key, saved_version} ->
          current = get_key_version(state, key)
          current == saved_version
        end)

      if watches_ok do
        # Step 3: Apply all commands atomically.
        old_count = state.applied_count

        {results, new_count} =
          Enum.map_reduce(commands, old_count, fn cmd, count ->
            result = apply_single(state, cmd)
            {result, count + 1}
          end)

        result = {:ok, results}
        new_state =
          state
          |> Map.put(:applied_count, new_count)
          |> record_tx_id(tx_id, result)

        maybe_release_cursor(meta, old_count, new_state, result)
      else
        # WATCH conflict -- abort with no writes.
        result = {:watch_conflict, nil}
        new_state = record_tx_id(state, tx_id, result)
        {new_state, result}
      end
  end
end
```

This is structurally similar to the existing `{:batch, commands}` handler,
with two additions:

1. **WATCH version checking inside `apply/3`.** Checking versions inside the
   StateMachine makes it atomic with the writes -- there is zero gap between
   the version check and the mutations. This closes the TOCTOU window for
   watched keys.

2. **Transaction idempotency tracking.** A bounded ring buffer of recently
   applied `{tx_id, result}` pairs. If a retry submits the same `tx_id`,
   the cached result is returned without re-applying commands.

#### `get_key_version/2`

For the initial implementation (shard-level versions), this reads the shard's
write version counter. For the future per-key version enhancement, it reads
from the ETS keydir.

```elixir
defp get_key_version(state, _key) do
  # Shard-level version (initial). Per-key version is a future enhancement.
  state.applied_count
end
```

Using `applied_count` (the Raft log index) as the version is natural: it
monotonically increases with every applied command and is deterministic
across all nodes in the Raft group.

#### Idempotency ring buffer

```elixir
@tx_idempotency_size 4096

defp check_tx_idempotency(%{recent_tx_ids: recent}, tx_id) do
  case Map.get(recent, tx_id) do
    nil -> :miss
    result -> {:hit, result}
  end
end

defp record_tx_id(state, tx_id, result) do
  recent = Map.put(state.recent_tx_ids, tx_id, result)

  # Evict oldest entry if over capacity.
  recent =
    if map_size(recent) > @tx_idempotency_size do
      {oldest_key, _} = Enum.min_by(recent, fn {k, _} -> k end)
      Map.delete(recent, oldest_key)
    else
      recent
    end

  %{state | recent_tx_ids: recent}
end
```

The `recent_tx_ids` map is added to the StateMachine state struct and is
included in Raft snapshots. Since `tx_id` is generated via
`:erlang.unique_integer([:positive, :monotonic])`, it is monotonically
increasing and can be used for oldest-first eviction.

### 6.3 Shard-Side Changes

#### New `handle_call`: `{:tx_validate, tx_id, commands, watch_keys}`

A read-only validation pass. Runs inside the shard's `handle_call` to get
a consistent view (no other `handle_call` can interleave).

```elixir
def handle_call({:tx_validate, tx_id, commands, watch_keys}, _from, state) do
  # 1. Check WATCH versions for keys owned by this shard.
  watches_ok =
    Enum.all?(watch_keys, fn {key, saved_version} ->
      current_version(state, key) == saved_version
    end)

  if not watches_ok do
    {:reply, {:invalid, tx_id, :watch_conflict}, state}
  else
    # 2. Dry-run commands to detect type errors, NX/XX failures, etc.
    case dry_run_commands(commands, state) do
      :ok ->
        {:reply, {:valid, tx_id}, state}

      {:error, reason} ->
        {:reply, {:invalid, tx_id, reason}, state}
    end
  end
end
```

#### `dry_run_commands/2`

Executes each command against a read-only snapshot of the shard's state.
Does not mutate ETS or queue Bitcask writes. Returns `:ok` if all commands
would succeed, or `{:error, reason}` with the first failure.

```elixir
defp dry_run_commands(commands, state) do
  read_only_store = build_read_only_store(state)

  Enum.reduce_while(commands, :ok, fn {cmd, args}, :ok ->
    case Dispatcher.dispatch(cmd, args, read_only_store) do
      {:error, _} = err -> {:halt, err}
      _ -> {:cont, :ok}
    end
  end)
end
```

The `read_only_store` uses the same ETS lookup functions as `build_local_store`
for reads, but replaces all write callbacks with no-ops that return `:ok`.
This lets the Dispatcher run the full command logic (type checking, condition
evaluation) without side effects.

#### Remove `prepare_tx`, `commit_tx`, `rollback_tx`

The current 2PC handlers (`handle_call({:prepare_tx, ...})`,
`handle_call({:commit_tx, ...})`, `handle_call({:rollback_tx, ...})`) and
the `staged_txs` field in the Shard struct are removed. The
`{:tx_pending_write, ...}` and `{:tx_pending_delete, ...}` message handlers
are also removed.

### 6.4 Coordinator Changes

```elixir
defmodule Ferricstore.Transaction.Coordinator do
  @validate_timeout 5_000
  @write_timeout 10_000
  @max_retries 2
  @retry_backoff_ms 500

  def execute([], _watched_keys, _ns), do: []

  def execute(queue, watched_keys, ns) do
    case classify_shards(queue, ns) do
      {:single_shard, shard_idx} ->
        execute_single_shard(queue, shard_idx, watched_keys, ns)

      {:multi_shard, shard_groups, index_map} ->
        execute_cross_shard(
          shard_groups, index_map, length(queue), watched_keys, ns
        )
    end
  end

  # -- Single-shard: one Raft log entry --

  defp execute_single_shard(queue, shard_idx, watched_keys, ns) do
    shard_watches = filter_watches_for_shard(watched_keys, shard_idx, ns)
    sm_commands = Enum.map(queue, &to_state_machine_command(&1, ns))
    tx_id = generate_tx_id()

    case submit_to_raft(shard_idx, {:tx_batch, tx_id, sm_commands, shard_watches}) do
      {:ok, results} -> results
      {:watch_conflict, nil} -> nil
      {:error, _} -> nil
    end
  end

  # -- Cross-shard: validate then write --

  defp execute_cross_shard(shard_groups, index_map, total, watched_keys, ns) do
    tx_id = generate_tx_id()

    # Phase 1: Validate on all shards in parallel.
    validate_tasks =
      Enum.map(shard_groups, fn {shard_idx, cmds_with_indices} ->
        Task.async(fn ->
          shard_watches = filter_watches_for_shard(watched_keys, shard_idx, ns)
          shard_cmds = Enum.map(cmds_with_indices, fn {_i, cmd, args} -> {cmd, args} end)
          result = validate_on_shard(shard_idx, tx_id, shard_cmds, shard_watches)
          {shard_idx, result}
        end)
      end)

    validate_results = Task.await_many(validate_tasks, @validate_timeout)

    all_valid =
      Enum.all?(validate_results, fn
        {_idx, {:valid, _}} -> true
        _ -> false
      end)

    if not all_valid do
      nil
    else
      # Phase 2: Submit tx_batch to each shard's Raft in parallel.
      write_tasks =
        Enum.map(shard_groups, fn {shard_idx, cmds_with_indices} ->
          Task.async(fn ->
            shard_watches = filter_watches_for_shard(watched_keys, shard_idx, ns)
            sm_cmds = Enum.map(cmds_with_indices, fn {_i, cmd, args} ->
              to_state_machine_command({cmd, args}, ns)
            end)
            result = submit_to_raft_with_retry(
              shard_idx, {:tx_batch, tx_id, sm_cmds, shard_watches}
            )
            {shard_idx, result}
          end)
        end)

      write_results = Task.await_many(write_tasks, @write_timeout)

      case partition_write_results(write_results) do
        {:all_ok, results_by_shard} ->
          reassemble_results(results_by_shard, index_map, total)

        {:watch_conflict, _} ->
          nil

        {:partial_failure, _succeeded, _failed} ->
          # Log and return nil. See section 6.5 for detailed analysis.
          Logger.error("Cross-shard tx #{tx_id}: partial failure")
          nil
      end
    end
  end

  defp submit_to_raft_with_retry(shard_idx, command, retries \\ @max_retries) do
    case submit_to_raft(shard_idx, command) do
      {:error, _reason} when retries > 0 ->
        Process.sleep(@retry_backoff_ms)
        submit_to_raft_with_retry(shard_idx, command, retries - 1)
      result ->
        result
    end
  end

  defp validate_on_shard(shard_idx, tx_id, commands, watch_keys) do
    shard_name = Router.shard_name(shard_idx)
    try do
      GenServer.call(shard_name, {:tx_validate, tx_id, commands, watch_keys},
                     @validate_timeout)
    catch
      :exit, _ -> {:invalid, tx_id, :shard_unavailable}
    end
  end
end
```

#### `to_state_machine_command/2`

Translates RESP-level `{command_string, args}` tuples to StateMachine
command tuples. This is necessary because the `{:tx_batch, ...}` is processed
by `StateMachine.apply/3`, which expects tuples like `{:put, key, value, exp}`,
not RESP strings like `{"SET", ["key", "value"]}`.

| RESP Command | StateMachine tuple |
|--------------|--------------------|
| `{"SET", [k, v]}` | `{:put, k, v, 0}` |
| `{"SET", [k, v, "EX", s]}` | `{:put, k, v, now_ms + s*1000}` |
| `{"SET", [k, v, "PX", ms]}` | `{:put, k, v, now_ms + ms}` |
| `{"DEL", [k]}` | `{:delete, k}` |
| `{"INCR", [k]}` | `{:incr, k, 1}` |
| `{"INCRBY", [k, n]}` | `{:incr, k, n}` |
| `{"INCRBYFLOAT", [k, n]}` | `{:incr_float, k, n}` |
| `{"APPEND", [k, s]}` | `{:append, k, s}` |
| `{"GETSET", [k, v]}` | `{:getset, k, v}` |
| `{"GETDEL", [k]}` | `{:getdel, k}` |
| `{"LPUSH", [k \| vs]}` | `{:list_op, k, {:lpush, vs}}` |
| `{"RPUSH", [k \| vs]}` | `{:list_op, k, {:rpush, vs}}` |
| `{"HSET", [k \| fvs]}` | sequence of `{:compound_put, ...}` |

**Read commands** (GET, HGET, LRANGE, etc.) cannot be translated to
StateMachine write commands. These need special handling in `apply_single`:

```elixir
defp apply_single(state, {:read, key}) do
  # Read from ETS keydir within the StateMachine's apply/3 context.
  # This gives a consistent read at the Raft log position.
  ets_lookup_value(state.keydir, key)
end
```

The Coordinator translates `{"GET", [key]}` to `{:read, key}` for inclusion
in the `{:tx_batch, ...}`. This is safe because `apply/3` runs on the
leader node which owns the ETS table.

### 6.5 Failure Scenarios

#### Scenario 1: Validation failure (any shard)

**Trigger:** Shard returns `{:invalid, tx_id, reason}` during Phase 1.

**Handling:** Return `nil` (EXECABORT) to client. No writes were submitted.
Clean exit with zero side effects. Covers: WATCH conflicts detected at
validation time, type errors (INCR on non-integer), NX/XX condition failures,
shard unavailability.

#### Scenario 2: Raft write failure on one shard (cross-shard)

**Trigger:** Phase 2 `{:tx_batch, ...}` succeeds on shard 0 but times out
or fails on shard 2 after retries.

**Handling:** This is the hardest failure mode. Shard 0's writes are
Raft-committed and cannot be rolled back without compensating writes.

**Approach: retry with idempotency, then accept partial failure.**

1. **Retry the failed shard** (up to `@max_retries` times with `@retry_backoff_ms`
   between attempts). Raft write failures are typically transient: leader
   election (~100-500ms), network partition (seconds), or high write load
   (backpressure). The `tx_id` idempotency guard in the StateMachine ensures
   retries are safe -- duplicate `{:tx_batch, tx_id, ...}` returns the cached
   result.

2. **If retries exhausted, accept partial failure.** Log the incident at
   `:error` level with the `tx_id`, succeeded shards, and failed shards.
   Return `nil` to the client. The client's retry (WATCH/MULTI/EXEC loop)
   will see the partially-applied state and either succeed or abort again.

**Why not compensating writes?** Compensating writes (DEL for SET, DECR for
INCR) are unreliable for read-modify-write operations. Consider:
- `INCR counter` increased counter from 5 to 6. Another client then INCRs
  to 7. Compensating DECR yields 6, not the original 5.
- `GETSET key newval` destroyed the old value. Compensation is impossible
  without storing the old value.
- `GETDEL key` deleted and returned the value. No compensation.

Compensating writes introduce more complexity than they solve and cannot
guarantee correctness in the presence of concurrent operations.

#### Scenario 3: WATCH conflict detected in Raft apply (cross-shard)

**Trigger:** Between validation (Phase 1) and Raft apply (Phase 2), another
writer modifies a watched key. The StateMachine's `apply/3` detects the
version mismatch in `{:tx_batch, ...}`.

**Handling:** That shard's `{:tx_batch}` returns `{:watch_conflict, nil}` --
no writes applied on that shard. Other shards may have already committed
their `{:tx_batch}`.

This is a partial application: shard 0 committed, shard 2 aborted due to
WATCH conflict. The Coordinator returns `nil` to the client.

**Why this is acceptable:** WATCH/MULTI/EXEC is inherently an optimistic
concurrency protocol. The client already expects `EXEC` to return `nil`
and must implement a retry loop. The partially-applied writes from shard 0
are semantically equivalent to what would happen if those commands were
executed outside a transaction -- they represent valid, individually-correct
state transitions.

**Stronger guarantee (future work):** To prevent partial writes on WATCH
conflict, we could introduce a lightweight cross-shard "barrier" where all
shards' `{:tx_batch}` commands include a barrier token, and a shard only
applies its batch if it can confirm (via a single extra round-trip to a
designated coordinator shard) that no other shard reported a WATCH conflict.
This adds one more Raft round-trip and is essentially Option A (2PC). We
defer this to future work unless user demand justifies the complexity.

#### Scenario 4: Coordinator (connection process) crashes during EXEC

**Trigger:** The BEAM process handling the connection crashes between Phase 1
and Phase 2, or during Phase 2.

**Handling:**
- Crash during validation: no writes submitted. Clean.
- Crash during Phase 2: `ra:pipeline_command` is a cast -- commands already
  enqueued in Raft leaders' inboxes will be applied regardless of the
  coordinator's fate. Tasks linked to the coordinator also crash, but the
  Raft commands are already submitted. This may result in partial application.
- The client's TCP connection closes. Client reconnects and retries.

The `tx_id` idempotency guard handles the case where a retried transaction
happens to submit the same commands to shards that already applied them
(though in practice, the retry is a new MULTI/EXEC with a new `tx_id`).

#### Scenario 5: Client disconnects during EXEC

**Trigger:** Client closes TCP connection while EXEC is running.

**Handling:** EXEC runs synchronously in the connection process (it's a
stateful command). The `{:tcp_closed, _}` message is queued in the process
mailbox until EXEC returns. EXEC completes, response send to the closed
socket fails silently, connection process cleans up normally.

The transaction commits or fails independently of the client's connection
state. No special handling needed.

#### Scenario 6: Slow shard (not failed)

**Trigger:** One shard's Raft group is slow (leader election in progress).

**Handling:** `Task.await_many(@write_timeout)` allows up to 10 seconds
for all shards. If a shard doesn't respond in time, it's treated as a
failure (Scenario 2). The `ra:pipeline_command` may eventually apply --
the idempotency guard handles duplicate application.

**Tuning:** Both `@validate_timeout` and `@write_timeout` should be
configurable.

### 6.6 WATCH Integration

#### Current flow (unchanged at the protocol level)

```
WATCH key1 key2
  --> connection.ex: for each key, Router.get_version(key) -> saved in state

... other clients may write key1 or key2 ...

MULTI
SET key1 "new_value"
SET key3 "other"
EXEC
  --> Coordinator.execute(queue, watched_keys, ns)
```

#### Change 1: Version check moves into Raft

Currently, `watches_clean?/1` checks versions in the Coordinator before
classifying shards. This leaves a gap between the check and the Raft write.

**New behavior:** The `watch_versions` map is partitioned by shard and
included in each shard's `{:tx_batch, ...}` Raft command. The StateMachine
validates versions atomically with the writes inside `apply/3`. The
Coordinator retains a pre-check in `tx_validate` as a fast-fail optimization
(avoids a Raft round-trip when a conflict is already detectable), but the
StateMachine check is the authoritative one.

#### Change 2: Per-key versions (Phase 4, independent)

Currently, `Router.get_version(key)` returns a shard-level counter. The
planned per-key version enhancement extends the ETS keydir tuple from
7 elements to 8:

```
Current:  {key, value, expire_at_ms, lfu, file_id, offset, value_size}
Proposed: {key, value, expire_at_ms, lfu, file_id, offset, value_size, key_version}
```

`key_version` is incremented on every write to that specific key. WATCH
records `{key, key_version}`. The StateMachine checks per-key versions in
`{:tx_batch}`.

**Cost:** 8 bytes per key in ETS (one word for the integer). For 10M keys,
80MB additional RAM.

**Compatibility:** `key_version` is maintained in ETS only, not persisted
to Bitcask. On shard restart, all versions reset to 0. Any in-flight WATCH
fails (correct behavior -- shard state may have changed during restart).

This is a separate, independent change from the cross-shard transaction
design. We can ship cross-shard atomicity first and upgrade WATCH granularity
later.

### 6.7 Single-Shard Fast Path Detail

Most MULTI/EXEC transactions will be single-shard, either naturally (keys
hash to the same shard by chance) or by design (hash tags like `{user:42}`).

**Current path:** Commands dispatched individually through `Dispatcher.dispatch/3`.
Each goes through Router, which calls `quorum_write` (Raft) or
`GenServer.call` (non-Raft) independently. N commands = N Raft round-trips.

**New path:** All commands bundled into one `{:tx_batch, tx_id, commands, watch_versions}`
Raft entry. Submitted via `ra:pipeline_command`. One Raft round-trip, one
`apply/3` call, one fsync.

**Latency improvement:** For a 10-command single-shard MULTI, the current
path requires 10 sequential Raft round-trips (~10-20ms). The new path
requires 1 (~1-2ms). This is a 5-10x improvement.

**Non-Raft mode:** When Raft is disabled, the single-shard fast path submits
the batch to the Shard GenServer as `{:batch_execute, tx_id, commands, watch_versions}`.
The Shard applies all commands within one `handle_call`, guaranteeing
atomicity via GenServer message processing semantics (no interleaving).

### 6.8 Embedded API

The current `FerricStore.Tx.execute/1` dispatches commands individually via
`FerricStore.set/3`, `FerricStore.get/1`, etc. -- no atomicity.

**New behavior:** `FerricStore.Tx.execute/1` translates Tx commands to
`{command_string, args}` tuples and calls `Coordinator.execute/3`:

```elixir
defmodule FerricStore.Tx do
  def execute(%__MODULE__{commands: commands}) do
    queue =
      commands
      |> Enum.reverse()
      |> Enum.map(&to_coordinator_command/1)

    ns = Process.get(:ferricstore_sandbox)
    Ferricstore.Transaction.Coordinator.execute(queue, %{}, ns)
  end

  defp to_coordinator_command({:set, key, value, opts}) do
    # Build args list matching what connection.ex would produce
    args = [key, value | opts_to_args(opts)]
    {"SET", args}
  end

  defp to_coordinator_command({:get, key}), do: {"GET", [key]}
  defp to_coordinator_command({:del, key}), do: {"DEL", [key]}
  defp to_coordinator_command({:incr, key}), do: {"INCR", [key]}
  defp to_coordinator_command({:incr_by, key, n}), do: {"INCRBY", [key, Integer.to_string(n)]}
  # ... etc for all Tx command types
end
```

This gives the embedded API the same atomicity guarantees as the TCP path.

**WATCH for embedded API (future):** A `FerricStore.watch/2` function that
records key versions in the process dictionary and passes them to
`Coordinator.execute/3`:

```elixir
# Future API sketch:
FerricStore.watch(["key1", "key2"], fn ->
  FerricStore.multi(fn tx ->
    tx
    |> FerricStore.Tx.set("key1", "v1")
    |> FerricStore.Tx.set("key2", "v2")
  end)
end)
# Returns {:ok, results} or {:watch_conflict, nil}
```

This is not included in the initial implementation scope.

---

## 7. Pipeline (Non-Atomic) vs Multi (Atomic)

Both pipeline and MULTI batch multiple commands for throughput. The semantic
difference:

| Aspect | Pipeline | MULTI/EXEC |
|--------|----------|------------|
| Atomicity | None. Each command is independent. | All-or-nothing within each shard's Raft batch. |
| Isolation | None. Other clients see intermediate states. | Per-shard: writes invisible until Raft apply completes. Cross-shard: no isolation guarantee between shards. |
| WATCH | N/A | Optimistic locking. Aborts if watched keys changed. |
| Failure mode | Individual commands fail independently. Others succeed. | Any failure aborts the entire transaction. |
| Raft entries | One per command (though Raft WAL batches fsyncs). | One per shard (all commands in one entry). |
| Use case | Throughput optimization. Fire-and-forget batching. | Consistency for related key mutations. |

**Shared optimization:** Both group commands by shard and dispatch in
parallel. Both benefit from Raft's internal WAL batching (one fsync covers
multiple Raft entries submitted in the same batch window). The key
difference is that MULTI wraps each shard's commands in a single
`{:tx_batch, ...}` Raft entry, while pipeline submits each command as an
independent Raft entry.

**Pipeline with errors:** In a pipeline, if command 3 of 5 returns an error,
commands 1-2 and 4-5 still execute. The client receives all 5 results
including the error. In MULTI/EXEC, if validation detects that command 3
would fail, the entire transaction is aborted before any writes.

---

## 8. Performance Estimates

### Single-shard MULTI (all keys on same shard)

| Metric | Current | New |
|--------|---------|-----|
| Raft round-trips | N (one per command) | 1 |
| fsync calls | ~1 (Raft WAL batches within flush window) | 1 |
| GenServer.call overhead | N * ~3us (Router dispatch) | 0 (direct ra:pipeline_command) |
| StateMachine apply calls | N | 1 (iterates N commands internally) |
| Estimated latency (10 cmds) | ~5-10ms | ~1-2ms |

### Cross-shard MULTI (2 shards, 5 commands each)

| Metric | Current | New |
|--------|---------|-----|
| Raft round-trips | 10 (per-shard, sequential via prepare_tx) | 2 (parallel, 1 per shard) |
| Validation round-trip | 0 (no validation phase) | 2 GenServer.calls parallel (~200us) |
| GenServer calls for 2PC | 2 prepare + 2 commit = 4 | 2 validate (Phase 1) |
| Total Raft writes | 10 individual commands | 2 batch entries |
| Estimated latency | ~10-20ms | ~2-4ms |

### Cross-shard MULTI (4 shards, 20 commands total)

| Metric | Current | New |
|--------|---------|-----|
| Raft round-trips | 20 (sequential) | 4 (parallel, 1 per shard) |
| Validation | 0 | 4 GenServer.calls parallel (~200us) |
| Estimated latency | ~20-40ms | ~2-4ms (bounded by slowest shard) |

The dominant win is:
1. **Batching:** N commands become 1 Raft entry per shard.
2. **Parallelism:** All shards' Raft entries submitted simultaneously.
3. **No 2PC overhead:** validate is a cheap read-only GenServer.call;
   commit phase is eliminated entirely.

---

## 9. Implementation Plan

### Phase 1: Single-Shard Raft-Atomic Batch (Week 1-2)

**Goal:** Make single-shard MULTI truly atomic via a single Raft log entry.
This alone fixes Problem #2 and delivers the biggest performance improvement.

**Tasks:**

1. Add `{:tx_batch, tx_id, commands, watch_versions}` handler to
   `StateMachine.apply/3` with WATCH version checking and idempotency ring
   buffer. Add `recent_tx_ids` field to StateMachine state.
2. Add `to_state_machine_command/2` translation function in Coordinator.
   Handle all command types currently supported in MULTI.
3. Update `Coordinator.execute` for single-shard case: submit `{:tx_batch}`
   via `ra:pipeline_command` instead of sequential Dispatcher calls.
4. Add `Shard.handle_call({:batch_execute, ...})` for non-Raft fallback.
5. Add `{:read, key}` handling in `StateMachine.apply_single/2` for GET
   commands within transactions.
6. Tests:
   - Single-shard MULTI atomicity: submit 5 writes, verify all-or-nothing
     by checking Raft log has exactly 1 entry for the batch.
   - WATCH conflict detection inside StateMachine: WATCH a key, modify it
     via another connection, EXEC should return nil.
   - Idempotency: submit same `tx_id` twice, verify second returns cached
     result without re-applying.
   - All command types: SET, GET, DEL, INCR, APPEND, HSET, LPUSH, etc.
   - Performance benchmark: 10-command MULTI before/after.

**Risk:** Low. The StateMachine already has `{:batch, commands}`.

### Phase 2: Cross-Shard Validation + Write (Week 2-3)

**Goal:** Replace the broken 2PC with validate-then-write protocol.

**Tasks:**

1. Add `Shard.handle_call({:tx_validate, ...})` for read-only validation.
2. Implement `dry_run_commands/2` with read-only store.
3. Update `Coordinator.execute_cross_shard` to use validate-then-write
   with parallel Task dispatch and retry logic.
4. Add `submit_to_raft_with_retry/3` with configurable retries and backoff.
5. Remove `prepare_tx`, `commit_tx`, `rollback_tx` from Shard.
6. Remove `staged_txs` from Shard struct.
7. Remove `{:tx_pending_write, ...}` and `{:tx_pending_delete, ...}` message
   handlers.
8. Tests:
   - Cross-shard atomicity: keys on 2 different shards, verify all or none.
   - Validation failure: INCR on a list key, verify no writes on any shard.
   - WATCH conflict across shards: modify watched key on shard 0, verify
     entire transaction aborts.
   - Partial Raft failure: stop one shard's Raft, verify retry + eventual
     success or clean abort.
   - Concurrent transactions: 10 concurrent MULTI/EXEC on overlapping keys.
   - Regression: single-shard fast path still works.

**Risk:** Medium. The validation-to-write TOCTOU window needs thorough
testing under concurrent load.

### Phase 3: Embedded API Integration (Week 3)

**Goal:** `FerricStore.multi/1` provides atomicity via the Coordinator.

**Tasks:**

1. Update `FerricStore.Tx.execute/1` to translate commands and call
   `Coordinator.execute/3`.
2. Add `to_coordinator_command/1` for all Tx command types.
3. Tests:
   - Embedded MULTI atomicity: single-shard and cross-shard.
   - Sandbox namespace propagation.

**Risk:** Low. Plumbing change with clear interfaces.

### Phase 4: Per-Key WATCH Versions (Week 4, optional)

**Goal:** Reduce WATCH false positive rate from shard-level to per-key.

**Tasks:**

1. Extend ETS keydir tuple to 8 elements (add `key_version` counter).
2. Update all ETS insert sites (`ets_insert/4` in Shard, `async_put` in
   Router) to include `key_version`. Increment on each write.
3. Update all ETS lookup sites (`ets_get/3` in Router, `ets_lookup_warm` in
   Shard, prefix scans, merge recovery) to handle 8-tuple.
4. Update `Router.get_version/1` to return per-key version from ETS.
5. Update `StateMachine.get_key_version/2` to check per-key version.
6. Tests:
   - WATCH on key A, write to unrelated key B on same shard, verify EXEC
     succeeds (currently fails with shard-level version).
   - Performance benchmark: version check overhead per operation.

**Risk:** Medium. The ETS tuple format is used in ~15 code locations. Requires
careful audit and migration testing. Can be done independently of Phases 1-3.

---

## 10. Alternatives Considered

### Why not full 2PC through Raft (Option A)?

Full 2PC with Raft-level prepare/commit gives strict atomicity and isolation
across shards. However:
- **2x Raft round-trips:** prepare + commit, each requiring consensus. For
  the cross-shard case, this doubles latency.
- **Staging area in StateMachine:** The staging area must be included in Raft
  snapshots, increasing snapshot size proportional to in-flight transactions.
- **Key-level locking:** During the prepare-to-commit window, staged keys
  must be locked to prevent concurrent writers from creating inconsistencies.
  Lock duration is bounded by Raft latency (~1-2ms) but introduces contention.
- **Coordinator crash recovery:** The classic 2PC blocking problem. If the
  coordinator crashes after prepare but before commit, all participants are
  in an uncertain state. Recovery requires querying all shards for in-doubt
  transaction IDs and making a global commit/abort decision. While Raft
  durability makes each shard's state recoverable, the cross-shard
  coordination protocol for recovery is significant engineering effort.
- **Complexity estimate:** 3-4x the code of Option B.

FerricStore is a Redis-compatible store. Redis Cluster does not support
cross-slot MULTI at all (CROSSSLOT error). Option B already exceeds Redis's
guarantees. Full 2PC is warranted for SQL databases (CockroachDB) but
over-engineering for key-value semantics.

### Why not CockroachDB-style parallel commits (Option C)?

Parallel commits achieve one-round-trip strict serializability, the gold
standard for distributed transactions. However:
- **MVCC required:** Every value must be stored as a versioned pair
  (intent vs committed) with timestamp ordering. FerricStore uses single-version
  ETS tuples and append-only Bitcask logs. Adding MVCC requires fundamental
  changes to the storage engine, ETS layout, cold-read path, and merge/compaction.
- **Intent resolution on reads:** Every read must check if the value is an
  uncommitted intent, look up the transaction record, and resolve the
  transaction's status. This adds latency to ALL reads -- not just
  transactional ones. The current hot-path ETS read in `Router.ets_get/3`
  (~50ns) would need intent-checking logic.
- **GC for abandoned intents:** If a coordinator crashes after writing intents
  but before marking the transaction as committed/aborted, the intents become
  orphans. A background process must scan for and resolve them.
- **HLC dependency strengthened:** FerricStore has HLC but uses it only for
  stream IDs and Raft piggybacking. Parallel commits would require HLC
  timestamps on every read and write for conflict detection.

This is the right architecture for a distributed SQL database. It is not
justified for a Redis-compatible key-value store.

### Why not reject cross-shard MULTI like Redis (CROSSSLOT)?

Redis Cluster returns `CROSSSLOT Keys in request don't hash to the same slot`
for any MULTI containing keys in different hash slots. This is the simplest
approach. However:
- FerricStore's value proposition includes being easier to use than Redis
  Cluster. Requiring hash tags for all transactional workloads pushes
  complexity to users.
- Single-node FerricStore with multiple shards (the typical deployment) uses
  shards for parallelism, not distribution. Cross-shard communication is a
  local GenServer call (~3us), not a network hop (~500us). The performance
  cost of cross-shard transactions is low enough to support.
- We already have a cross-shard MULTI implementation (the 2PC coordinator).
  Fixing it is better than removing it.

### Why not DragonflyDB-style VLL (deterministic lock ordering)?

DragonflyDB uses VLL (Very Lightweight Locking) from a database research
paper. Multi-key operations acquire locks in ascending shard-ID order to
prevent deadlocks, then execute atomically across shards.

This works well for DragonflyDB's shared-nothing thread-per-shard architecture
where locks are per-shard spin-waits. In FerricStore's architecture:
- Shards are GenServers, not threads. "Locking a shard" means blocking its
  GenServer, which blocks all other clients on that shard.
- FerricStore uses Raft for durability. Even with shard locks held, the Raft
  write must complete before locks can be released, extending the lock
  duration by the Raft round-trip time.
- The benefit of VLL is deterministic deadlock freedom. Our Option B achieves
  the same property differently: the validate phase is read-only (no locks),
  and the write phase uses independent Raft entries (no cross-shard
  coordination during writes).

### Why not FoundationDB-style OCC with range conflict detection?

FoundationDB achieves strict serializability through optimistic concurrency
control: transactions read at a snapshot version, writes are buffered locally,
and at commit time a centralized Resolver checks for read-write conflicts
across all concurrent transactions.

This requires:
- A centralized conflict resolution service (the Resolver).
- Range-based conflict tracking (not just point keys).
- MVCC for snapshot reads.

Interesting for future consideration if FerricStore evolves toward a
transactional database, but far beyond current requirements.
