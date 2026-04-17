# Async Write Path Redesign

## Problem

On `perf/write-throughput-optimization` branch, benchmarks show async and quorum writes
achieving nearly identical throughput (~44K ops/sec). Async should be significantly
faster because it gives up durability guarantees for speed.

### Root causes

1. **AsyncApplyWorker serialization**: async writes go through a single GenServer
   per shard (`AsyncApplyWorker.replicate`), processing casts one-by-one. Each cast
   issues an individual `ra.pipeline_command` — no batching.

2. **Double disk writes**: Router's `async_write_put` writes to Bitcask via
   `BitcaskWriter.write`, AND the state machine writes to Bitcask again during
   Raft apply via `v2_append_batch_nosync`. Same data, two syscalls.

3. **Double ETS writes**: Router inserts to ETS, then state machine inserts again
   on apply. Wasteful and introduces race conditions with concurrent writes to
   the same key.

4. **No batching at Raft layer**: since each async write submits its own
   `ra.pipeline_command`, the Ra WAL sees N individual commands instead of one
   batch of N, losing the group-commit amortization that makes quorum fast.

## Goals

- Async writes return `:ok` in ~5μs (small values) or ~1ms (big values, bounded
  by disk write time).
- One `ra.pipeline_command({:batch, [cmds]})` per batch window, matching quorum's
  throughput characteristics.
- No redundant Bitcask writes.
- No redundant ETS writes.
- Read-your-writes on the origin node (client that issued the write).
- Correct on replicas (eventual consistency after Raft replication).

## Non-goals

- Strict consistency across nodes for async writes (documented async trade-off).
- Kernel-crash durability (async never promised this).
- Changes to quorum write behavior.

## Design Overview

**Principle**: Router owns all local writes on the origin node. State machine only
writes on replicas. Batcher replaces AsyncApplyWorker for Raft submission.

```
Client → Router (origin)
          ├─ ETS insert (always)
          ├─ Bitcask write (big values only, synchronous NIF OR BitcaskWriter cast for small)
          ├─ Batcher.async_submit (cast, fire-and-forget)
          └─ return :ok

Batcher (origin, batched):
          └─ ra.pipeline_command({:batch, [{:async, cmd1}, ...]})

Raft replication → State machine apply (all nodes):
          ├─ {:async, inner} →
          │  ├─ if ETS has key → we're origin, skip (Router did it)
          │  └─ else → we're replica, apply inner normally
          └─ {:put, ...}, etc. → quorum path (unchanged)
```

## Detailed Flow

### Small value (value fits in ETS hot cache)

**Origin node** (latency: ~5μs until `:ok`):

1. Router `:ets.insert(keydir, {key, value, exp, lfu, :pending, 0, size})`
   - Value is inlined in ETS — reads hit immediately.
2. Router `BitcaskWriter.write(idx, path, file_id, keydir, key, disk_val, exp)`
   - Cast to per-shard BitcaskWriter GenServer (~1μs).
   - BitcaskWriter batches casts, writes via `v2_append_batch_nosync`, updates
     ETS `:pending` → real `file_id, offset`.
   - BitcaskWriter serializes per-shard, so concurrent writes to the same key
     land in the correct order ("latest write wins" preserved).
3. Router `Batcher.async_submit(idx, {:async, {:put, key, value, exp}})`
   - Cast to per-shard Batcher (~1μs).
4. Router returns `:ok`.

**Batcher (origin, flush timer)**:

5. Accumulated async commands flushed as one batch.
6. `:ra.pipeline_command(shard_id, {:batch, [{:async, cmd1}, ...]}, corr, :normal)`.

**Replica node** (Raft applies the command):

7. State machine `apply(meta, {:async, {:put, key, value, exp}}, state)`:
   - `:ets.lookup(keydir, key)` → `[]` (replica never had Router run).
   - Delegate to normal `apply(meta, {:put, ...}, state)` — writes Bitcask + ETS.

**Origin node** (Raft also applies the command locally):

8. State machine `apply(meta, {:async, {:put, key, value, exp}}, state)`:
   - `:ets.lookup(keydir, key)` → `[{key, value, exp, lfu, file_id, offset, size}]`.
   - ETS has the entry → Router already owns this write → skip.
   - Return `:ok` without touching Bitcask or ETS.

### Big value (value exceeds `hot_cache_max_value_size`)

**Origin node** (latency: ~1ms, bounded by disk write):

1. `disk_val = to_disk_binary(value)`, `value_for_ets = nil`.
2. Router synchronous NIF write:
   `{:ok, [{offset, size}]} = NIF.v2_append_batch_nosync(path, [{key, disk_val, exp}])`.
   - Data is durable on origin (page cache) before next step.
3. Router `:ets.insert(keydir, {key, nil, exp, lfu, file_id, offset, size})`:
   - Value is `nil` — reads do `pread(file_id, offset, size)` to fetch from disk.
4. Router `Batcher.async_submit(idx, {:async, {:put, key, value, exp}})`:
   - Note: the full value is sent to Batcher because replicas need it.
5. Router returns `:ok`.

**Batcher, replica, origin apply**: same as small value.

- On origin, state machine sees ETS entry (with real file_id) → skip.
- On replica, state machine sees empty ETS → apply normally, writes value to
  replica's local Bitcask.

### Reads (unchanged)

- Small value: ETS hit on origin (and replica, after apply).
- Big value: ETS metadata hit → `v2_pread_at(file_id, offset, size)` → value.

## Command Encoding

Async commands are wrapped to distinguish them from quorum commands at the state
machine:

```elixir
# Origin Router emits:
{:async, {:put, key, value, exp}}
{:async, {:delete, key}}
{:async, {:incr, key, delta}}
# ... etc. for all commands async supports

# State machine pattern match:
def apply(meta, {:async, inner}, state) do
  key = Ferricstore.Raft.StateMachine.extract_key(inner)
  case :ets.lookup(state.ets, key) do
    [] ->
      # Replica: apply inner normally
      apply(meta, inner, state)

    [_entry] ->
      # Origin: Router already handled local persistence
      new_state = %{state | applied_count: state.applied_count + 1}
      {new_state, :ok}
  end
end
```

For batch commands containing async entries, the state machine partitions them:

```elixir
def apply(meta, {:batch, commands}, state) do
  # Split by type — origin skip vs replica apply
  {to_apply, to_skip} =
    Enum.split_with(commands, fn
      {:async, inner} ->
        case :ets.lookup(state.ets, extract_key(inner)) do
          [] -> true           # replica, needs apply
          [_] -> false         # origin, skip
        end
      _ ->
        true                   # quorum command, always apply
    end)

  # Apply the non-skipped commands as a batch (existing code path)
  with_pending_writes(state, fn ->
    Enum.each(to_apply, fn cmd -> apply_single(cmd, state) end)
  end)
end
```

## Failure Modes

| Failure | Behavior | Acceptable? |
|---------|----------|-------------|
| Origin process crashes after `:ok` but before Bitcask flush | Data in ETS is lost; Ra WAL has the command → state machine applies on replay → data recovered via Bitcask write on recovery | Yes |
| Origin kernel panics before fsync | Data lost (ETS in memory, Bitcask in page cache not on disk, Ra WAL fsynced so Raft replay rebuilds) | Yes — async trade-off |
| Replica crashes mid-apply | Ra replays on restart, writes Bitcask + ETS | Yes |
| Batcher crashes before flush | Accumulated async commands lost; Ra never saw them; origin's ETS/Bitcask still have them locally | Yes — diverges from replicas, but local read-your-writes holds |
| Concurrent writes to same key (W1 then W2) | BitcaskWriter GenServer processes casts in order → W1 writes offset1, W2 writes offset2. ETS ends up pointing to offset2. Correct. | Yes |

## What Changes

### `apps/ferricstore/lib/ferricstore/store/router.ex`

`async_write_put` replaces `async_submit_to_raft` call with `Batcher.async_submit`:

```diff
- async_submit_to_raft(idx, {:put, key, value, expire_at_ms})
+ Batcher.async_submit(idx, {:async, {:put, key, value, expire_at_ms}})
```

Same change for async delete/incr/append/etc.

### `apps/ferricstore/lib/ferricstore/raft/batcher.ex`

Add:

```elixir
@spec async_submit(non_neg_integer(), command()) :: :ok
def async_submit(shard_index, command) do
  GenServer.cast(batcher_name(shard_index), {:async_submit, command})
end

def handle_cast({:async_submit, command}, state) do
  enqueue_async(command, state)
end

defp enqueue_async(command, state) do
  # Simpler than enqueue_write — no `from`, no reply tracking
  # Accumulate in async slot, flush via ra.pipeline_command on timer
  ...
end
```

Change `submit_async` (flush path) to skip AsyncApplyWorker:

```diff
- defp submit_async(shard_index, batch, froms) do
-   AsyncApplyWorker.apply_batch(shard_index, batch)
-   Enum.each(froms, &GenServer.reply(&1, :ok))
- end
+ defp submit_async(state, batch) do
+   # Submit directly via Raft — Router already wrote locally on origin
+   corr = make_ref()
+   serialized = {:ttb, :erlang.term_to_binary({:batch, batch})}
+   :ra.pipeline_command(state.shard_id, serialized, corr, :normal)
+   # No `from` tracking — callers already got :ok from Router
+   state
+ end
```

### `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`

Add `{:async, inner}` clause (placed before other apply/3 clauses so it matches first):

```elixir
def apply(meta, {:async, inner}, state) do
  key = extract_key_for_async(inner)
  case :ets.lookup(state.ets, key) do
    [] ->
      # Replica — apply inner normally
      __MODULE__.apply(meta, inner, state)

    [_entry] ->
      # Origin — Router already did local persistence
      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, :ok)
  end
end
```

Add `extract_key_for_async/1` helper for each command type:

```elixir
defp extract_key_for_async({:put, key, _, _}), do: key
defp extract_key_for_async({:delete, key}), do: key
defp extract_key_for_async({:incr, key, _}), do: key
# ...
```

Update `{:batch, commands}` handler to partition async-origin-skip commands
from commands requiring apply (as shown in the "Command Encoding" section).

### `apps/ferricstore/lib/ferricstore/raft/async_apply_worker.ex`

Two options:

1. **Remove entirely** — no longer referenced. Clean.
2. **Keep for future multi-node retry-on-not-leader** — remove `apply_batch`
   and `replicate` calls but keep the module so we can reintroduce retry logic
   later if needed.

Recommendation: **option 1** — remove. If we need retry logic later, we can add
it to Batcher directly.

## Performance Expectations

- **Small async write latency** (caller-observed): ~5μs (ETS insert + 2 casts).
- **Big async write latency**: ~disk write time (~1ms for 1MB on NVMe).
- **Throughput**: should match quorum throughput when saturated (both bottlenecked
  by Ra WAL fdatasync cycle). Main win: latency, not throughput, since async
  never blocks on consensus.
- **Syscall count**: reduced by 1 per async write (no duplicate `v2_append_batch_nosync`
  on origin).

## Test Plan

1. Existing `AsyncDurabilityTest` suite should continue to pass (24 tests).
2. `AsyncRaftDivergenceTest` should continue to pass.
3. `AsyncWalTest` — including "INCR is atomic through async WAL" — should pass.
4. Concurrent writes same key test: 50 tasks × 100 INCRs = final value 5000.
5. Large value test: 1MB value readable immediately after `:ok`.
6. Read-your-writes test: `put(k, v)`, immediate `get(k) == v`.
7. Replica convergence: multi-node test (if we run one), all replicas eventually
   have the value.
8. Throughput benchmark: async should match or exceed quorum (~44K+ ops/sec on
   current hardware).
9. Latency benchmark: single-threaded, async p99 < quorum p50.

## Rollout

Single atomic change — all the pieces fit together. Cannot deploy partially
without breaking async.

## Open Questions

- Should big-value Router write be via `BitcaskWriter.write` (async cast,
  batched) instead of synchronous NIF? Trade-off: lower latency on `:ok` but
  brief window where reads miss. Current proposal: keep synchronous for big
  values (read-your-writes is the point).

- Should we deprecate `AsyncApplyWorker` now or after verifying no hidden
  dependencies? Grep shows only router.ex uses it, but worth a search pass.

- Do we need a deferred Bitcask fsync for async writes? Currently relying on
  Ra WAL fsync + page cache. On kernel panic, async data loss is documented
  behavior. A background fsync every N seconds could reduce data loss window
  without hurting throughput (separate work item).
