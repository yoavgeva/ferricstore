# Async RMW Design: Latch-First, Worker Fallback

## Status

Proposed. Replaces the earlier `async-rmw-design.md` content (single-path
coordinator proposal).

## Problem

Async durability commits to ~100μs end-to-end latency on the client path.
Plain `SET`/`DELETE` achieve this: caller writes ETS, casts `BitcaskWriter`,
casts `Batcher.async_submit`, returns `:ok`. ~10–15μs total.

Read-modify-write (RMW) commands — `INCR`, `INCR_FLOAT`, `APPEND`, `GETSET`,
`GETDEL`, `GETEX`, `SETRANGE` — are harder. They must:

1. Read the current value from ETS.
2. Compute the new value.
3. Write the new value to ETS.
4. Return the **computed result** to the caller (not just `:ok`).

Step 1–3 must be **atomic per-key**: two concurrent `INCR k 1` calls must
produce two distinct results (`{:ok, N}` and `{:ok, N+1}`), and the ETS row
must end with the correct new value. No lost updates.

### Current state (post-commit 88ff185)

Async RMW silently falls through to the quorum path (`quorum_write`) because
the earlier Router-process read-compute-write lost updates under concurrency.
Quorum gives correctness via the state machine's serial `apply/3`, but at the
cost of ~2–7ms latency per call — a 20–70× regression for async RMW.

## Goals

- **Correctness**: 50 concurrent `INCR k 1` calls must sum to `+50` on `k`.
  Every caller gets a distinct `{:ok, N}` reflecting the moment it applied.
- **Latency**: p50 ≈ 15–30μs for uncontended RMW. No GenServer hop in the
  common case.
- **Degrades gracefully**: under hot-key contention, latency rises but
  throughput doesn't collapse and no CPU is wasted on spinning.
- **Non-invasive**: `SET`/`DELETE` fast path stays unchanged. Only the 7 RMW
  command paths and one new coordination GenServer per shard.

## Non-Goals

- Cross-key atomic RMW (that's `MULTI`/`EXEC`, different code path).
- Strict total ordering between `SET` and RMW on the same key. Concurrent
  `SET k v` + `INCR k 1` is last-write-wins — documented async semantics.
- Cluster-wide correctness in multi-node mode. We submit the delta command
  (`{:incr, k, δ}`) via `Batcher.async_submit`, and replicas apply it
  deterministically against their own state. Correctness within one node.

## Architecture Overview

Two paths, auto-selected per-call:

```
Async RMW request
       │
       ▼
  :ets.insert_new(latch_tab, {key, self()})
       │
   ┌───┴───┐
   │       │
  true    false
   │       │
   ▼       ▼
 LATCH    WORKER (RmwCoordinator GenServer)
 PATH     PATH
 (fast)   (FIFO ordered, handles contention)
```

**Latch path** (uncontended, common case):

- Caller's own process executes the RMW under a per-key ETS latch.
- No GenServer hop. No mailbox. No extra process.
- ~10–15μs from call to return.
- Scales linearly with distinct keys and scheduler count.

**Worker path** (contended, hot key):

- Caller did `:ets.insert_new` and lost — someone else holds the latch.
- Caller does `GenServer.call(RmwCoordinator, {:rmw, cmd})` — blocks on its
  own `receive`, zero CPU.
- Worker processes RMWs from its mailbox in FIFO order, also via the latch
  (so the worker won't race with a latch-path caller).
- ~30–80K RMW/sec per shard ceiling (one worker's throughput).

**Self-detecting**: no hotness heuristic. The `insert_new` result decides.

## Data Structures

### `latch_tab` — per shard

A `:public, :set, {:write_concurrency, :auto}` ETS table, one per shard.

```elixir
# key → holder_pid
# - `key` is the user key (same as the keydir key)
# - `holder_pid` identifies the latch holder for crash cleanup
```

Added to `FerricStore.Instance` as `latch_refs: tuple()` alongside `keydir_refs`.

### `RmwCoordinator` — per shard

A `GenServer` registered as `:"Ferricstore.Store.RmwCoordinator.#{idx}"` per
shard. It:

- Accepts `{:rmw, cmd}` via `handle_call` — blocking, returns the computed
  result.
- Internally uses the same latch as latch-path callers — briefly "waits"
  (one short retry loop) if a latch-path caller holds the key at the
  moment the message is picked up. Because only one process (the worker)
  ever retries, this is bounded spin, not a thundering herd.
- Holds no in-flight registry: each `handle_call` drains one RMW to
  completion before processing the next. Mailbox order = serialization
  order for all contended RMWs on this shard.

## Concrete Implementation

### Public Router API (unchanged signatures)

```elixir
# apps/ferricstore/lib/ferricstore/store/router.ex

def incr(ctx, key, delta), do: rmw_dispatch(ctx, key, {:incr, key, delta})
def incr_float(ctx, key, delta), do: rmw_dispatch(ctx, key, {:incr_float, key, delta})
def append(ctx, key, suffix), do: rmw_dispatch(ctx, key, {:append, key, suffix})
def getset(ctx, key, value), do: rmw_dispatch(ctx, key, {:getset, key, value})
def getdel(ctx, key), do: rmw_dispatch(ctx, key, {:getdel, key})
def getex(ctx, key, exp), do: rmw_dispatch(ctx, key, {:getex, key, exp})
def setrange(ctx, key, off, v), do: rmw_dispatch(ctx, key, {:setrange, key, off, v})

# Dispatch based on durability; async uses latch-first, quorum unchanged.
defp rmw_dispatch(ctx, key, cmd) do
  idx = shard_for(ctx, key)
  case durability_for_key(ctx, key) do
    :quorum -> quorum_write(ctx, idx, cmd)
    :async -> async_rmw(ctx, idx, key, cmd)
  end
end
```

### Latch-first async RMW

```elixir
defp async_rmw(ctx, idx, key, cmd) do
  latch_tab = elem(ctx.latch_refs, idx)

  case :ets.insert_new(latch_tab, {key, self()}) do
    true ->
      # Uncontended fast path — run inline in the caller's process.
      try do
        execute_rmw_inline(ctx, idx, cmd)
      after
        # ets.take removes the row atomically. A crashed caller's entry
        # is cleaned up by the latch sweeper (see Edge cases).
        :ets.take(latch_tab, key)
      end

    false ->
      # Contended — fall through to the per-shard worker. The worker
      # serializes via its mailbox (FIFO) and also uses the latch, so
      # there's no race with a latch-path caller that acquired just
      # ahead of us.
      try do
        Ferricstore.Store.RmwCoordinator.execute(idx, cmd)
      catch
        :exit, {:timeout, _} -> {:error, "ERR RMW timeout"}
        :exit, {:noproc, _} -> {:error, "ERR RMW worker unavailable"}
      end
  end
end
```

### The inline executor (one clause per RMW command)

Runs with the latch held — therefore exclusive access to `key`'s ETS row
among RMW paths. Uses the same bounds checks and error messages as the
state machine's `do_incr`/`do_put` so Router.get returns the same thing
whether a write went through this path or through quorum apply.

```elixir
@spec execute_rmw_inline(Instance.t(), non_neg_integer(), rmw_command()) :: term()

defp execute_rmw_inline(ctx, idx, {:incr, key, delta}) do
  keydir = elem(ctx.keydir_refs, idx)

  old_int =
    case :ets.lookup(keydir, key) do
      [] -> 0
      [{_, value, exp, _, _, _, _}] ->
        now = HLC.now_ms()
        cond do
          exp != 0 and exp <= now -> 0
          is_integer(value) -> value
          is_binary(value) ->
            case Integer.parse(value) do
              {n, ""} -> n
              _ -> throw({:rmw_error, "ERR value is not an integer or out of range"})
            end
          true -> throw({:rmw_error, "ERR value is not an integer or out of range"})
        end
    end

  new_int = old_int + delta

  if new_int > 9_223_372_036_854_775_807 or new_int < -9_223_372_036_854_775_808 do
    throw({:rmw_error, "ERR increment or decrement would overflow"})
  end

  new_str = Integer.to_string(new_int)

  # 1. ETS (origin-local visibility for GET)
  ets_insert_rmw(ctx, idx, key, new_str, 0)

  # 2. BitcaskWriter (durable on local disk, batched in writer)
  write_bitcask(ctx, idx, key, new_str, 0)

  # 3. Batcher.async_submit — replication with the DELTA command
  #    so replicas apply against their own local value, deterministic
  #    under Raft log ordering.
  Ferricstore.Raft.Batcher.async_submit(idx, {:incr, key, delta})

  {:ok, new_int}
catch
  {:rmw_error, msg} -> {:error, msg}
end

# Analogous clauses for :incr_float, :append, :getset, :getdel, :getex, :setrange.
# Each one:
#   - reads current value from ETS (treating expired TTL as missing),
#   - computes the result per Redis semantics,
#   - writes ETS + casts BitcaskWriter + casts Batcher.async_submit with the
#     *operation*, not the computed value (replica convergence).
```

### The RmwCoordinator worker

```elixir
defmodule Ferricstore.Store.RmwCoordinator do
  @moduledoc """
  Serializes RMW commands that hit contention on the latch path.

  Under uncontended load, Router.async_rmw executes RMW inline in the
  caller's process with a per-key ETS latch. If `:ets.insert_new` reports
  the latch is already held, the caller falls through to this GenServer
  which processes RMWs one at a time from its mailbox (FIFO).

  The worker ALSO acquires the latch before executing. At most one
  latch-path caller can hold it, so the retry loop is bounded (~15μs
  worst case on NVMe + fast path). Only the worker spins on the latch,
  never the callers — callers block on GenServer.call which costs zero
  CPU until replied.
  """

  use GenServer

  alias Ferricstore.Store.Router, as: _R

  def start_link(shard_index: idx), do: GenServer.start_link(__MODULE__, idx, name: name(idx))
  def name(idx), do: :"Ferricstore.Store.RmwCoordinator.#{idx}"

  @spec execute(non_neg_integer(), tuple()) :: term()
  def execute(idx, cmd), do: GenServer.call(name(idx), {:rmw, cmd}, 10_000)

  @impl true
  def init(idx) do
    ctx = FerricStore.Instance.get(:default)
    Process.send_after(self(), :sweep_latches, 5_000)
    {:ok, %{idx: idx, ctx: ctx, latch_tab: elem(ctx.latch_refs, idx)}}
  end

  @impl true
  def handle_call({:rmw, cmd}, _from, state) do
    key = key_of(cmd)
    wait_for_latch(state.latch_tab, key)

    try do
      result = Ferricstore.Store.Router.__execute_rmw_inline__(state.ctx, state.idx, cmd)
      :telemetry.execute([:ferricstore, :rmw, :worker],
                         %{}, %{shard_index: state.idx})
      {:reply, result, state}
    after
      :ets.take(state.latch_tab, key)
    end
  end

  # Periodic sweep: remove latch entries whose holder pid is dead.
  def handle_info(:sweep_latches, state) do
    :ets.foldl(
      fn {key, pid}, _ ->
        if not Process.alive?(pid), do: :ets.delete(state.latch_tab, key)
      end,
      nil,
      state.latch_tab
    )
    Process.send_after(self(), :sweep_latches, 5_000)
    {:noreply, state}
  end

  defp wait_for_latch(tab, key) do
    case :ets.insert_new(tab, {key, self()}) do
      true -> :ok
      false ->
        :erlang.yield()
        wait_for_latch(tab, key)
    end
  end

  defp key_of({:incr, k, _}), do: k
  defp key_of({:incr_float, k, _}), do: k
  defp key_of({:append, k, _}), do: k
  defp key_of({:getset, k, _}), do: k
  defp key_of({:getdel, k}), do: k
  defp key_of({:getex, k, _}), do: k
  defp key_of({:setrange, k, _, _}), do: k
end
```

### Supervision

Add to `Ferricstore.ShardSupervisor`:

```elixir
for i <- 0..(shard_count - 1) do
  {Ferricstore.Store.RmwCoordinator, shard_index: i}
end
```

Order: after `Batcher` and `BitcaskWriter` (the coordinator depends on
those), before any process that issues RMWs.

### Instance context

`apps/ferricstore/lib/ferricstore/instance.ex` adds a `latch_refs` field:

```elixir
defstruct [
  # ... existing fields ...
  :latch_refs,
]
```

Populated at startup like `keydir_refs`:

```elixir
latch_refs =
  for i <- 0..(shard_count - 1) do
    tab_name = :"ferricstore_latch_#{i}"
    :ets.new(tab_name, [:public, :set, :named_table,
                        {:read_concurrency, true},
                        {:write_concurrency, :auto}])
    tab_name
  end
  |> List.to_tuple()
```

## Correctness Argument

### Atomicity for RMW-vs-RMW on the same key

**Claim**: two concurrent RMW calls on key `k` can never lose an update.

**Proof sketch**:

- A row `{k, _pid}` in `latch_tab` is present if and only if some process
  is doing read-modify-write on `k` right now (via latch path or via worker).
- `:ets.insert_new` is atomic — exactly one process sees `true` at any
  instant; concurrent losers see `false`.
- **Latch-path caller** proceeds only if it got `true`. It executes
  read-compute-write with exclusive access, then `:ets.take/2` removes
  the row atomically.
- **Worker** is called by callers that got `false`. Its mailbox is
  FIFO; it processes one RMW at a time. For each, it also spins on
  `insert_new` until it wins — then executes and releases.
- At most one process — the current latch holder (caller or worker) —
  is mutating `k`'s row at any instant.
- Therefore ETS writes to `k`'s row from the RMW path are totally
  ordered.

Under 50 concurrent INCRs on one key: first caller wins latch → finishes
~15μs → 49 callers fail `insert_new` → all route to worker → worker
FIFO-applies each at ~30–50μs → total ~2ms, all 50 increments land.

### Ordering semantics

**Within the RMW path**: linearizable per-key. The commit order matches
either (a) the order the first latch-path winner arrived (for each
uncontended moment), or (b) worker mailbox FIFO (for each contended moment).

**Between RMW and SET**: no ordering guarantee. Concurrent `SET k v`
(no latch) and `INCR k δ` (latch) is last-write-wins. Documented async
semantics.

**Between origin and replicas**: Router's `Batcher.async_submit` submits
the **delta command** (`{:async, {:incr, k, δ}}`), not a pre-computed
value. Replicas apply the delta to their local state in Raft log order.
For commutative RMW (INCR), convergence is guaranteed. For non-commutative
RMW (GETSET), replicas see the "last" assignment as defined by Raft log
order — same as the existing async SET semantics.

### No CPU spinning in the common case

Under contention:

- Latch-path callers do **one** `insert_new`. If it fails, they issue
  `GenServer.call` and the scheduler puts them to sleep (zero CPU) until
  the worker replies. No retries, no yields.
- Only the worker spins — and it's one process. Each spin iteration is
  an ETS `insert_new` (~200ns). The latch is held at most ~15μs by the
  winning caller → worker spins ~50–75 iterations worst case, ~15μs of
  CPU total before winning.
- In the typical uncontended case, worker is never invoked.

No thundering herd. The BEAM scheduler doesn't schedule sleeping callers
until the worker replies to them.

## Edge Cases

### 1. Latch holder crashes

Caller crashes after `:ets.insert_new` but before `:ets.take`. The latch
leaks; subsequent callers see `false` forever.

**Mitigation**: the RmwCoordinator runs a sweep every 5 seconds that
removes entries whose holder pid is dead. A crashed caller blocks its
key's RMW for at most 5s.

### 2. Worker crashes mid-RMW

Supervisor restarts the worker. Any pending `GenServer.call`s get `:exit`,
Router's `async_rmw` catches the exit and returns
`{:error, "ERR RMW worker unavailable"}`. Latch left behind by the
crashed worker is cleaned up by the new worker's sweep on restart.

### 3. Caller timeout on GenServer.call

Default 10s. If the worker's mailbox is backed up that far, something is
wrong. Caller gets `:exit` → Router returns `{:error, "ERR RMW timeout"}`.

### 4. latch_tab doesn't exist at startup

Mitigated by supervisor ordering: `Instance.new/1` creates latch tables
before shard processes (and therefore before any RMW can be issued). If
somehow it's accessed before creation, `:ets.insert_new` crashes with
`:badarg` — caller is supervised, error propagates, test fails loudly.

### 5. Cross-shard compound keys (H:*, S:*, Z:*, T:*)

Compound keys route to their parent key's shard. The latch uses the
compound key itself as the latch key (the same way the keydir does), so
atomicity is preserved per-field. Matches the existing
`cross_shard_op` rate-limit test coverage.

## Interaction with State Machine `{:async, inner}` Clause

Current origin-skip logic in `state_machine.ex:222`:

```elixir
def apply(_meta, {:async, inner_cmd}, state) do
  case :ets.lookup(state.ets, key_of(inner_cmd)) do
    [_entry] -> {state, :ok}                  # origin-skip
    [] -> apply(_meta, inner_cmd, state)      # replica apply
  end
end
```

This works unchanged for the new RMW path:

- **Origin**: Router's latch-path or worker wrote to ETS before calling
  `Batcher.async_submit`. State machine sees ETS has the key → skips.
  Bitcask is written by BitcaskWriter cast, not by state machine.
- **Replica**: Replica's ETS doesn't have the key → state machine
  applies `{:incr, k, δ}` via `do_incr`, which reads replica-local state
  and adds δ.

Key property: **we replicate the delta, not the computed value**. Two
concurrent INCRs on origin:

- Origin: latch serializes them locally → each gets their own new_val.
- Raft log on origin receives `{:async, {:incr, k, 1}}` for each, in
  submission order.
- Replicas apply each delta in Raft log order. Convergence holds because
  integer addition is associative + commutative. For non-commutative RMW
  (GETSET, SETRANGE), replicas end at "last Raft-ordered command wins"
  — same model as existing async SET.

## Performance Expectations

### Uncontended

- Per RMW: `:ets.insert_new` + `:ets.lookup` + `:ets.insert` + `:ets.take`
  + 2 GenServer casts + return. All in caller's heap.
- **p50: ~15μs, p99: ~50μs** (ETS internal lock contention, scheduler
  hops).
- Throughput: ~60K RMW/sec per scheduler × 16 schedulers = ~1M
  RMW/sec/shard ceiling. Practically limited downstream by BitcaskWriter
  and Raft WAL.

### Heavy same-key contention (worst case)

- All callers bounce to worker after their first `insert_new`.
- Worker processes each at ~50μs (including latch wait + ETS + casts).
- **Throughput per hot key: ~20K–40K RMW/sec** (worker-bound).
- p99 grows linearly with queue depth. At 1000 queued callers:
  p99 ≈ 50ms.
- No CPU waste: callers sleep in `receive`; only worker spins (briefly).

### Realistic mixed workload

- Rate limiters / session counters: ~80% latch path, ~1M RMW/sec total.
- Occasional hot key: 1 key pinned to worker, rest on latch.
- Aggregate: ~500K–1M RMW/sec per shard × 4 shards = 2–4M RMW/sec total.

## Test Plan

All tests in `apps/ferricstore/test/ferricstore/store/async_rmw_test.exs`,
added alongside existing `async_write_redesign_test.exs`.

1. **Uncontended correctness**: single caller does each of the 7 RMW
   commands; result and ETS state match direct-path reference.
2. **50 concurrent distinct keys**: 50 tasks each INCR a different key.
   Expect all 50 keys at value 1. Latch-path metric > worker-path metric.
3. **50 concurrent same key**: 50 tasks each INCR same key by 1.
   Expect key at 50. Worker-path metric > 0.
4. **1000 INCRs from 25 tasks on same key (port from existing TDD test)**:
   expect final value "1000". No lost updates.
5. **Mixed SET + INCR on same key**: 500 tasks, half SET a literal, half
   INCR. Final value is either an integer or a literal (last-write-wins).
   No crash, no error.
6. **Latch holder crash**: a Task holding the latch exits abnormally; a
   new RMW on the same key succeeds within 6s (sweep interval + margin).
7. **Worker crash**: kill the RmwCoordinator mid-RMW; next RMW gets
   `{:error, "ERR RMW worker unavailable"}` then succeeds after
   supervisor restart.
8. **Latency**: single-threaded, measure p50/p99 for uncontended INCR.
   Assert p50 < 100μs.
9. **Telemetry**: new events
   `[:ferricstore, :rmw, :latch]` and `[:ferricstore, :rmw, :worker]`
   for path tracking. Assert ratio matches expected under each load.
10. **Replica convergence (multi-node, if harness available)**: origin
    applies 1000 concurrent INCRs on the same key; replica eventually
    reads 1000.

## Rollout

One PR with all pieces (supervisor wiring, Instance ctx field, Router
dispatch, RmwCoordinator module, latch tab creation, sweep, tests).
Cannot land partially — removing the current `quorum_write` fallback
from `Router.async_write` without the new path in place would regress to
the original lost-updates bug.

After merge:

1. Run full async suite — the existing TDD test for concurrent INCR
   passes via the new path.
2. Remove the temporary `quorum_write` routing for RMW in Router
   (commit 88ff185 workaround).
3. Measure: latency on single-thread bench, throughput on 1/10/100
   distinct keys, throughput on 1 hot key.

## Open Questions

- **Sweep interval**: 5s is a middle-ground default. If observed latch
  leaks are common (they shouldn't be), tighten to 1s. Configurable via
  `:latch_sweep_interval_ms`.
- **Worker spin cap**: the worker's `wait_for_latch` loops forever if a
  caller never releases. In practice the sweeper removes such entries
  after 5s. Consider capping at 1s and returning
  `{:error, "ERR latch stuck"}` — escalation vs infinite wait. Start
  without the cap; add if observed.
- **Bitcask write ordering**: `BitcaskWriter` already serializes casts
  per shard. Under the latch, same-key RMWs are ordered by
  release-then-acquire, so their BitcaskWriter casts land in the same
  order. Cross-key RMWs have no ordering guarantee at the writer (they
  shouldn't need any).
- **Telemetry shapes**: `[:ferricstore, :rmw, :latch]` could carry a
  `{:latency_us, ...}` measurement if useful for hotspot detection.
  Start with a simple counter event, expand later.

## Summary

Two-regime system. Latch path for linear scaling on distinct keys (the
99% case) with ~15μs p50. Worker GenServer for strict FIFO on hot keys
(the 1% pathological case) with ~50μs p50 and no CPU waste. No lost
updates, graceful degradation. Drop-in replacement for the current
`quorum_write` fallback — same Router API, same Raft replication story,
20–70× better latency on the uncontended path.
