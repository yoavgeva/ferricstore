# Async Path for Compound, List, and Probabilistic Commands

## Status

Proposed. Extends `docs/async-write-redesign.md` and
`docs/async-rmw-design.md` to cover the remaining write command families.

## Problem

The current async fast path covers:

- **Plain writes**: `put`, `delete` — via `Router.async_write_put` (ETS +
  BitcaskWriter + `Batcher.async_submit`).
- **Plain RMW**: `incr`, `incr_float`, `append`, `getset`, `getdel`,
  `getex`, `setrange` — via `Router.async_rmw` with per-key ETS latch
  and `RmwCoordinator` fallback.

Every other write command ends up on the quorum path regardless of the
namespace's configured durability:

| Family | Current entry point | Effect |
|---|---|---|
| Compound (HSET, HDEL, SADD, SREM, ZADD, ZREM, XADD field-level) | `Router.compound_put`, `Router.compound_delete` → `GenServer.call(Shard)` | ~2–7ms (quorum) |
| Compound bulk (DEL on a hash, WIPE on a set) | `Router.compound_delete_prefix` → `GenServer.call(Shard)` | ~O(n) quorum |
| List ops (LPUSH, RPUSH, LPOP, RPOP, LSET, LREM, LINSERT, LTRIM, LMOVE) | `Router.list_op` → `GenServer.call(Shard)` → Raft | ~2–7ms (quorum) |
| Probabilistic (BF.ADD, CF.ADD, CMS.INCRBY, TOPK.ADD, …) | `Router.prob_write` → `raft_write` → `async_write` catch-all → `quorum_write` | ~2–7ms (quorum) |
| Distributed primitives (CAS, LOCK, UNLOCK, EXTEND, RATELIMIT) | `raft_write` catch-all → `quorum_write` | ~2–7ms (quorum) |

Users who configure a namespace as `:async` expecting all writes to be
fast are surprised when HSET/LPUSH/BF.ADD take milliseconds.

## Goals

- Commands that can benefit from async semantics follow the namespace
  config, just like plain writes and plain RMW.
- No correctness regression. Atomicity, read-your-writes, replica
  convergence preserved.
- Commands that need strict consensus (CAS, LOCK, EXTEND, RATELIMIT)
  continue to use quorum unconditionally, regardless of namespace.

## Scope

### In scope (make async when namespace = async)

**Group A — write-only field-level compound commands**
- `compound_put` (HSET, SADD, ZADD, TS.ADD single-field)
- `compound_delete` (HDEL, SREM, ZREM single-field)

**Group B — RMW list operations**
- `list_op` with operations: `{:lpush, ...}`, `{:rpush, ...}`,
  `{:lpop, ...}`, `{:rpop, ...}`, `{:lset, ...}`, `{:lrem, ...}`,
  `{:linsert, ...}`, `{:ltrim, ...}`
- `list_op_lmove` (may cross shards; only single-shard case is in scope;
  cross-shard stays quorum)

### Always quorum regardless of namespace config (Group C)

- `cas`, `lock`, `unlock`, `extend`, `ratelimit_add`
- `bloom_*`, `cuckoo_*`, `cms_*`, `topk_*` (all 13 prob commands)
- Rationale: two reasons combined.
  1. **Distributed coordination** (cas/lock/extend/ratelimit): contract is
     linearizability. A user who wants them faster than quorum is asking
     for correctness violations.
  2. **Return computed results** (prob commands): every prob command
     returns a value computed by the state machine (bits newly set,
     evicted items, counter deltas). They cannot be fire-and-forget;
     the caller must wait for the state machine's reply. The NIF itself
     does 14-15 file syscalls per call (~15-50μs) — Raft adds ~2-5ms,
     but the NIF is syscall-bound, not Raft-bound. A latch + inline
     path would save some Raft overhead but require careful per-NIF
     concurrency handling (the NIFs do non-atomic read-modify-write
     on bit positions). Not worth the complexity for the modest win.

  All of these stay on the forced-quorum path (`write_async_quorum`)
  which ignores namespace durability config.

### Always quorum for now (Group D)

- `compound_delete_prefix` (DEL of a whole hash / set / zset)
- Rationale: O(n) scan + delete of all fields. The latency is dominated
  by the scan, not the Raft round-trip. Not worth the complexity of an
  async path that would still do an O(n) scan in the caller's process.

### Originally proposed but deferred (Group E)

- Latch + inline path for prob commands. Would reduce latency from
  ~2–5ms to ~50–100μs, but requires:
  - Porting 13 prob command handlers to run in caller's process
  - Per-key latch to serialize concurrent NIF bit-write races
  - Auto-create edge cases + metadata ETS coordination
  - ~600–800 lines of inline executors + tests

  Deferred pending a benchmark that demonstrates prob latency is a
  hot-path bottleneck.

## Durability Decision

Uses the existing per-namespace `Ferricstore.NamespaceConfig`. The
decision is made on the **user-facing parent key**, which is:

| Command family | Key used for namespace lookup |
|---|---|
| Plain | the key itself |
| Compound | the `redis_key` (not the `compound_key`). `HSET user:1 name "alice"` is in the `user` namespace, not `H`. |
| List | the list key itself (no prefix rewriting). `LPUSH session:xs elem` is in the `session` namespace. |
| Prob | the probabilistic structure's key itself. `BF.ADD visits:2026 "user42"` is in the `visits` namespace. |
| LMOVE (cross-shard) | source key's namespace; if src and dst namespaces differ, force quorum for safety. |

This keeps the "namespace = durability" abstraction intact regardless of
the command's internal structure.

## Design by Group

### Group A — Async compound field-level (HSET / HDEL / SADD / SREM / …)

The command touches exactly one compound key and either writes a value
or deletes it. Structurally identical to plain `put`/`delete` — only the
key shape is different. Implementation mirrors `async_write_put`:

```elixir
# Router.compound_put — NEW dispatch
def compound_put(ctx, redis_key, compound_key, value, expire_at_ms) do
  idx = shard_for(ctx, redis_key)

  case durability_for_key(ctx, redis_key) do
    :quorum ->
      # existing path
      shard = elem(ctx.shard_names, idx)
      GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})

    :async ->
      async_compound_put(ctx, idx, redis_key, compound_key, value, expire_at_ms)
  end
end

defp async_compound_put(ctx, idx, _redis_key, compound_key, value, exp) do
  keydir = elem(ctx.keydir_refs, idx)

  # Small value inline; large value synchronous NIF to get a real
  # (file_id, offset) so cold reads of compound_key work immediately.
  value_for_ets =
    cond do
      is_integer(value) -> Integer.to_string(value)
      is_float(value) -> Float.to_string(value)
      byte_size(value) > ctx.hot_cache_max_value_size -> nil
      true -> value
    end

  disk_value = to_disk_binary(value)
  {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)

  if value_for_ets == nil do
    case Bitcask.NIF.v2_append_batch_nosync(file_path, [{compound_key, disk_value, exp}]) do
      {:ok, [{offset, _}]} ->
        :ets.insert(keydir, {compound_key, nil, exp, LFU.initial(), file_id, offset, byte_size(disk_value)})

      {:error, reason} ->
        {:error, "ERR compound disk write failed: #{inspect(reason)}"}
    end
  else
    :ets.insert(keydir, {compound_key, value_for_ets, exp, LFU.initial(), :pending, 0, 0})
    Ferricstore.Store.BitcaskWriter.write(idx, file_path, file_id, keydir, compound_key, disk_value, exp)
  end

  :counters.add(ctx.write_version, idx + 1, 1)

  # Replicate. The state machine's {:async, inner} clause applies
  # {:put, compound_key, value, exp} on replicas; on origin it sees
  # the ETS entry and skips.
  Ferricstore.Raft.Batcher.async_submit(idx, {:put, compound_key, value, exp})
  :ok
end

# compound_delete follows the same pattern with {:delete, compound_key}.
```

**Promotion handling**: `Router.compound_put` in the quorum path also
invokes a "promotion check" (if the hash exceeds the promotion
threshold, move it to a dedicated Bitcask file). Promotion is a Shard
GenServer operation that tracks per-hash entry count. In the async path
we **skip the promotion check** for speed. Consequences:

- Hashes that grow large while in an async namespace stay in the shared
  Bitcask log, never get promoted.
- If the namespace durability is later changed back to quorum, the next
  `compound_put` on that hash triggers promotion normally.
- Acceptable because users choosing async accepted "best effort"
  semantics; promotion is an optimization, not a correctness concern.

### Group B — Async list operations (LPUSH, LPOP, LSET, …)

List ops are RMW at the structural level: every op reads the current
list (element count, head/tail pointers, element values) and writes a
new list. Internally a list is stored as a sequence of compound keys
`L:<redis_key>:<index>` with metadata tracking head/tail indices.

**This is exactly the pattern the RMW latch + worker solves.** The
entire list operation must be atomic for a given list key, and we
already have the infrastructure.

Two key observations:

1. **The latch is on the user-facing list key**, not on individual
   `L:*` compound keys. A single LPUSH touches multiple compound keys
   (head pointer + new element); those writes must happen atomically.
2. **The caller does the list-op computation inline** using the same
   `ListOps.execute/3` function the state machine uses, pointed at an
   origin-local store (ETS + BitcaskWriter casts).

```elixir
# Router.list_op — NEW dispatch
def list_op(ctx, key, operation) do
  idx = shard_for(ctx, key)

  case durability_for_key(ctx, key) do
    :quorum ->
      # existing path
      shard = elem(ctx.shard_names, idx)
      GenServer.call(shard, {:list_op, key, operation})

    :async ->
      async_list_op(ctx, idx, key, operation)
  end
end

defp async_list_op(ctx, idx, key, operation) do
  latch_tab = elem(ctx.latch_refs, idx)

  case :ets.insert_new(latch_tab, {key, self()}) do
    true ->
      try do
        :telemetry.execute([:ferricstore, :list_op, :latch], %{}, %{shard_index: idx})
        execute_list_op_inline(ctx, idx, key, operation)
      after
        :ets.take(latch_tab, key)
      end

    false ->
      # Same worker as plain RMW — just sends through a different cmd tuple.
      try do
        Ferricstore.Store.RmwCoordinator.execute(idx, {:list_op, key, operation})
      catch
        :exit, {:timeout, _} -> {:error, "ERR list_op timeout"}
        :exit, {:noproc, _} -> {:error, "ERR RMW worker unavailable"}
        :exit, _ -> {:error, "ERR RMW worker crashed"}
      end
  end
end

defp execute_list_op_inline(ctx, idx, key, operation) do
  keydir = elem(ctx.keydir_refs, idx)
  {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)

  # Build an origin-local store that ListOps.execute can drive. Each
  # :compound_put/:compound_delete closure writes ETS + casts
  # BitcaskWriter, just like async_compound_put does.
  store = %{
    compound_get: fn _redis_key, compound_key ->
      case :ets.lookup(keydir, compound_key) do
        [{_, value, _, _, _, _, _}] -> value
        [] -> nil
      end
    end,
    compound_put: fn _redis_key, compound_key, value, exp ->
      :ets.insert(keydir, {compound_key, value, exp, LFU.initial(), :pending, 0, 0})
      Ferricstore.Store.BitcaskWriter.write(idx, file_path, file_id, keydir,
        compound_key, to_disk_binary(value), exp)
      :ok
    end,
    compound_delete: fn _redis_key, compound_key ->
      :ets.delete(keydir, compound_key)
      Ferricstore.Store.BitcaskWriter.delete(idx, file_path, compound_key)
      :ok
    end,
    compound_scan: fn _redis_key, prefix ->
      Ferricstore.Store.Shard.ETS.prefix_scan_entries(keydir, prefix, file_path)
      |> Enum.sort_by(fn {field, _} -> field end)
    end,
    compound_count: fn _redis_key, prefix ->
      Ferricstore.Store.Shard.ETS.prefix_count_entries(keydir, prefix)
    end,
    exists?: fn k ->
      case :ets.lookup(keydir, k) do
        [_] -> true
        [] -> false
      end
    end
  }

  result = Ferricstore.Store.ListOps.execute(key, store, operation)

  :counters.add(ctx.write_version, idx + 1, 1)

  # Replicate. Use the SAME {:list_op, key, operation} command the
  # state machine already handles. Replicas apply it against their
  # local state in Raft log order — deterministic because ListOps
  # is pure given a store.
  Ferricstore.Raft.Batcher.async_submit(idx, {:list_op, key, operation})

  result
end
```

**RmwCoordinator extension**: the coordinator currently handles the 7
string RMW commands via `Router.execute_rmw_inline`. Extend it with a
clause for `{:list_op, key, op}` that dispatches to
`execute_list_op_inline`. No change to the latch/worker/mailbox
machinery — the coordinator is command-agnostic.

**LMOVE across shards**: `list_op_lmove` with src and dst on different
shards requires a cross-shard transaction. Keep this on quorum — same
reasoning as compound_delete_prefix (complex, rare).

### Group E — Probabilistic commands (stays quorum)

Originally this group was going to follow the same latch + inline
pattern as plain RMW and list_op. After implementing the rest and
looking at the NIFs more carefully, the decision changed: **prob
commands stay on forced-quorum regardless of namespace config.**

Reasoning:

1. **Every prob command returns a computed value** the caller needs
   (`{:ok, 0|1}` for add, `{:ok, [counts]}` for query, list of evicted
   items for topk_add). The state machine computes them. An "async
   fire-and-forget" semantic doesn't fit.

2. **The Bloom/Cuckoo/CMS/TopK NIFs do 14-15 file syscalls per call**
   (pread/pwrite on individual bits in a flat file, not mmap). Total
   work is 15-50μs of NIF I/O. Raft adds ~2-5ms on top, but the NIF
   itself isn't trivially cheap — a latch + inline path would save
   the Raft overhead but not much else.

3. **The NIFs are not safe for concurrent same-key access** — the
   bit read-modify-write loop has no lock. Today the state machine
   serializes via its mailbox. Moving to a latch + inline path
   requires adding that discipline in the caller's process — doable
   but 13 command variants, each with auto-create edge cases.

4. **The namespace config still does something useful** for prob
   keys: it affects how the state machine's `{:async, inner}`
   wrapper tries origin-skip. But since prob commands always want
   quorum semantics (wait for computed result), we bypass the
   durability-based slot selection entirely.

**Current behavior**: `async_write` catch-all routes ALL remaining
commands (prob, cas, lock, extend, ratelimit) through
`Batcher.write_async_quorum`, which forces the quorum slot regardless
of namespace durability. The caller blocks on the state machine reply
and gets the computed result.

## State Machine — No Changes

The existing `{:async, inner}` clause in `state_machine.ex:222` already
handles arbitrary inner commands:

```elixir
def apply(meta, {:async, inner_cmd}, state) do
  key = async_key_for(inner_cmd)
  case :ets.lookup(state.ets, key) do
    [_entry] -> {state, :ok}                      # origin-skip
    [] -> __MODULE__.apply(meta, inner_cmd, state) # replica apply
  end
end

defp async_key_for({:put, k, _, _}), do: k
defp async_key_for({:delete, k}), do: k
defp async_key_for({:incr, k, _}), do: k
# ... etc.
```

**Only list_op needs clauses in `async_key_for/1`**:

```elixir
defp async_key_for({:list_op, k, _}), do: k
defp async_key_for({:list_op_lmove, src, _dst, _, _}), do: src
```

Prob commands never reach the `{:async, inner}` clause because they
route through forced-quorum (`write_async_quorum`), not `async_submit`.
Their results come back via the `ra_event :applied` → reply path.

For compound_put/compound_delete the async path wraps the command as
`{:put, compound_key, ...}` / `{:delete, compound_key, ...}` which are
already handled by the existing clauses. No change for Group A on the
state-machine side.

## Failure Modes

| Failure | Behavior | Status |
|---|---|---|
| Origin crashes after ETS write, before BitcaskWriter flushes | Ra WAL has the async submission; replicas apply on replay. Origin's ETS may be slightly ahead of Bitcask until next write. Documented async behavior. | Accepted |
| Origin crashes during a list_op inline execution | Latch leaks; sweeper cleans up after 5s. Caller gets `:noproc` or `:timeout`, returns `{:error, ...}`. | Accepted |
| Same-key concurrent list_ops | Latch + worker serializes. FIFO ordering. | Same as plain RMW |
| Concurrent HSET of different fields in same hash | Different compound_keys, no latch collision. Ordering is per-field, not per-hash. | Acceptable — matches Redis behavior where concurrent field writes are independent |
| Concurrent HSET of same field in same hash | Same compound_key → latch collides → worker fallback | Same as plain RMW |
| Auto-create of prob structure races with explicit CREATE | Latch serializes both. Whichever wins writes metadata; the other sees it already exists. | Fine |
| `compound_delete_prefix` during async field writes | Stays quorum → goes through Shard GenServer → serializes with other Shard-handled ops. But async field writes don't serialize through Shard. | **Potential race** — see below |

### Race: `compound_delete_prefix` (quorum) vs `async_compound_put` (async)

User does `HSET user:1 name "alice"` (async) while another client does
`DEL user:1` (which becomes `compound_delete_prefix`, quorum-path).

Timeline:
- T1: `HSET` inserts `H:user:1:name` → "alice" into ETS, casts BitcaskWriter,
  casts `Batcher.async_submit`.
- T2: `DEL` goes through Shard → Batcher.write → Raft applies
  `compound_delete_prefix("H:user:1:")` → state machine deletes all
  `H:user:1:*` from ETS.
- T3: Origin's `:async_submit` from T1 lands in Raft. State machine
  applies `{:async, {:put, "H:user:1:name", "alice", 0}}` → ETS has no
  entry for this key → replica path runs → writes "alice" back.

Now the key is alive again even though DEL was supposed to wipe the
hash. **This is a real bug.** Two mitigations:

1. **Upgrade `compound_delete_prefix` to use the latch pattern** on the
   parent redis_key. The async field-put path takes the latch on the
   compound_key, not the redis_key, so this doesn't directly help.
   Need a redis-key-level latch in addition.
2. **Accept last-write-wins**: document that mixing DEL with async HSET
   on the same hash is undefined. Users who need consistency use
   quorum.

Recommendation: document option 2 for now. Add a note to `async-rmw-design.md`
that the "SET + RMW last-write-wins" rule extends to "DEL + HSET
last-write-wins" in async namespaces. Revisit if it bites in practice.

## What Changes

### Router

- Add dispatch for compound_put, compound_delete, list_op, list_op_lmove,
  prob_write based on `durability_for_key(ctx, redis_key_or_key)`.
- Add async implementations: `async_compound_put`, `async_compound_delete`,
  `async_list_op`, `async_prob_write`.
- Add `execute_list_op_inline/4` and `execute_prob_inline/4` as public
  functions so `RmwCoordinator` can call them on contention fallback.

### RmwCoordinator

- Extend command dispatch to handle `{:list_op, k, op}` and the 13 prob
  commands. Currently it only handles 7 plain RMW. The coordinator
  pattern-matches on the command tuple and calls the matching
  `execute_*_inline` function.

### StateMachine

- Extend `async_key_for/1` to cover list_op, prob, and list_op_lmove
  (15 new clauses).
- Existing `{:async, inner}` apply clause unchanged.

### NamespaceConfig

- No change. Already drives the durability decision for plain writes.

### Tests

- New test file `async_compound_test.exs` — HSET/HDEL latency,
  concurrency, read-your-writes.
- New test file `async_list_op_test.exs` — LPUSH/LPOP correctness under
  concurrency, LMOVE cross-shard forced-quorum.
- New test file `async_prob_test.exs` — BF.ADD, CMS.INCRBY, CF.ADD
  concurrent correctness.
- Existing test suites (compound, list, prob) continue to pass
  unchanged — they default to quorum namespaces.

## Rollout

One PR. Design is additive — existing quorum paths unchanged, new async
paths only activate when namespace durability is `:async`. No user
impact for anyone who hasn't configured async namespaces.

## Performance Expectations

Uncontended async, per command:

| Command | Current | With async path |
|---|---|---|
| HSET / HDEL (single field) | ~2–7 ms | ~10–20 μs |
| SADD / ZADD (single member) | ~2–7 ms | ~10–20 μs |
| LPUSH / RPUSH (single element) | ~2–7 ms | ~20–40 μs (list bookkeeping) |
| BF.ADD / CMS.INCRBY / CF.ADD | ~2–7 ms | ~20–40 μs (NIF compute) |

Contended (50 concurrent callers on the same key):

| Command | Current | With async path |
|---|---|---|
| Same-field HSET | ~100–350 ms (sequenced through Shard) | ~1–2 ms (worker FIFO) |
| Same-list LPUSH | ~100–350 ms | ~1.5–3 ms |
| Same-key BF.ADD | ~100–350 ms | ~1.5–3 ms |

## Open Questions

1. **Promotion for async compound ops.** Should a hash in an async
   namespace ever get promoted to a dedicated Bitcask file? Simplest:
   no — document it. Alternative: track promotion state via a
   background process that scans periodically. Defer.
2. **Prob structure auto-create**: does the async path need to emit a
   "created" telemetry event so operators can distinguish explicit
   CREATE from auto-create via ADD? Same question exists for quorum;
   no new concern.
3. **TS.ADD (time series)**: not in scope here because TS.ADD might
   land as a compound_put or as a dedicated command tuple — check when
   implementing. If it's compound_put, it's Group A; if it's its own
   state machine clause, extend `async_key_for/1` to cover it.

## Summary

Three groups of commands get async paths:

- **A** (compound_put/delete): mirror `async_write_put`, trivial.
- **B** (list_op): use the latch/worker pattern with the existing
  `ListOps.execute/3` engine.
- **E** (prob_write): use the latch/worker pattern per-command.

Groups C (CAS/LOCK/EXTEND/RATELIMIT) and D (compound_delete_prefix)
stay quorum regardless of namespace config.

State machine unchanged except for extending `async_key_for/1`.
RmwCoordinator extended to dispatch new command tuples to new inline
executors.

Estimated scope: ~600 lines of Router/RmwCoordinator code, ~100 lines of
state_machine helpers, ~300 lines of new tests. One PR.
