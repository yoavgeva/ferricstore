# Raft Write Path Error Propagation

## Problem

The batcher has 4 failure modes where callers get a timeout instead of a meaningful error:

1. **ra unreachable** -- `pipeline_command` is a cast, always returns `:ok`, caller waits forever
2. **StateMachine.apply crashes** -- ra marks entry as applied, caller never gets reply
3. **Batcher overloaded** -- unbounded slot accumulation, eventual OOM or timeout
4. **Stale leader** -- already partially handled via `{:rejected, ...}` but no telemetry

## Current Flow

```
Client -> Batcher.handle_call({:write, cmd}, from)
  -> stores {from, cmd} in slot
  -> timer fires or slot full
  -> Batcher submits batch via ra:pipeline_command (fire-and-forget)
  -> ra applies -> StateMachine.apply returns result
  -> ra sends {:ra_event, ...} back to Batcher
  -> Batcher calls GenServer.reply(from, result)
```

Where errors get lost:

- `ra:pipeline_command` fails silently (it's a cast)
- `StateMachine.apply` crashes -- ra catches it, entry is "applied", but no result sent back
- Batcher accumulates commands without bound when ra is slow
- `handle_info` catch-all silently drops unknown events

## Design

### 1. Pending Command Timeout

Add a deadline to every pending entry. A periodic sweep reaps expired entries and replies with a specific error.

**State changes:**
```elixir
# Before
pending: %{corr => {froms, kind}}

# After
pending: %{corr => {froms, kind, submitted_at_ms}}
```

**New timer:**
```elixir
# In init/1:
:timer.send_interval(1_000, :check_pending)
```

**Sweep logic:**
```elixir
@pending_timeout_ms 8_000

def handle_info(:check_pending, state) do
  now = System.monotonic_time(:millisecond)

  {expired, remaining} =
    Map.split_with(state.pending, fn {_corr, {_froms, _kind, submitted_at}} ->
      now - submitted_at > @pending_timeout_ms
    end)

  Enum.each(expired, fn {_corr, {froms, _kind, _submitted_at}} ->
    Enum.each(froms, fn from ->
      safe_reply(from, {:error, :raft_timeout})
    end)
  end)

  if map_size(expired) > 0 do
    Logger.warning("Batcher shard #{state.shard_index}: #{map_size(expired)} pending commands timed out")
    :telemetry.execute(
      [:ferricstore, :raft, :timeout],
      %{count: map_size(expired)},
      %{shard: state.shard_index}
    )
  end

  {:noreply, %{state | pending: remaining}}
end
```

Why 8s not 10s: gives callers a meaningful `{:error, :raft_timeout}` before the
`GenServer.call` 10s timeout fires as an exit.

**pipeline_submit changes:**
```elixir
defp pipeline_submit(state, [single_cmd], froms) do
  corr = make_ref()
  now = System.monotonic_time(:millisecond)
  :ra.pipeline_command(state.shard_id, single_cmd, corr, :normal)
  %{state | pending: Map.put(state.pending, corr, {froms, :single, now})}
end

defp pipeline_submit(state, batch, froms) do
  corr = make_ref()
  now = System.monotonic_time(:millisecond)
  :ra.pipeline_command(state.shard_id, {:batch, batch}, corr, :normal)
  %{state | pending: Map.put(state.pending, corr, {froms, :batch, now})}
end
```

### 2. Backpressure / Overload Rejection

Reject new writes when pending queue is too deep.

**State changes:**
```elixir
@max_pending_commands 10_000
```

**In enqueue_write:**
```elixir
defp enqueue_write(command, from, state) do
  if pending_command_count(state) >= @max_pending_commands do
    {:reply, {:error, :overloaded}, state}
  else
    # existing enqueue logic
  end
end

defp pending_command_count(state) do
  slot_count =
    state.slots
    |> Map.values()
    |> Enum.reduce(0, fn slot, acc -> acc + length(slot.froms) end)

  pending_count =
    state.pending
    |> Map.values()
    |> Enum.reduce(0, fn {froms, _kind, _ts}, acc -> acc + length(froms) end)

  slot_count + pending_count
end
```

### 3. StateMachine Apply Error Wrapping

Wrap `apply_single` so ra always gets a result tuple back.

**In state_machine.ex:**
```elixir
# In apply({:batch, commands}):
# Before:
result = apply_single(state, cmd)

# After:
result = safe_apply_single(state, cmd)

defp safe_apply_single(state, cmd) do
  apply_single(state, cmd)
rescue
  e -> {:error, {:apply_crashed, Exception.message(e)}}
catch
  kind, reason -> {:error, {:apply_crashed, {kind, reason}}}
end
```

The caller sees `{:error, {:apply_crashed, reason}}` instead of timing out.

### 4. Leadership Change Telemetry

The `{:rejected, ...}` handler already replies `{:error, {:not_leader, leader}}`. Add observability:

```elixir
def handle_info({:ra_event, _from_id, {:rejected, {not_leader, maybe_leader, corr}}}, state) do
  case Map.pop(state.pending, corr) do
    {nil, _pending} ->
      {:noreply, state}

    {{froms, _kind, _ts}, new_pending} ->
      leader = resolve_leader(not_leader, maybe_leader, state)

      :telemetry.execute(
        [:ferricstore, :raft, :rejected],
        %{count: length(froms)},
        %{shard: state.shard_index, reason: :not_leader}
      )

      Logger.warning(
        "Batcher shard #{state.shard_index}: command rejected, leader is #{inspect(leader)}"
      )

      Enum.each(froms, fn from ->
        safe_reply(from, {:error, {:not_leader, leader}})
      end)

      {:noreply, maybe_reply_flush_waiters(%{state | pending: new_pending})}
  end
end
```

## Error Type Summary

| Scenario | Current behavior | New behavior |
|---|---|---|
| Ra unreachable | Caller hangs, GenServer timeout exit | `{:error, :raft_timeout}` after 8s |
| Apply crashes | Caller hangs, GenServer timeout exit | `{:error, {:apply_crashed, reason}}` |
| Batcher overloaded | Unbounded accumulation, OOM/timeout | `{:error, :overloaded}` immediately |
| Leader changed | `{:error, {:not_leader, leader}}` | Same + telemetry + log |
| Batcher terminated | `{:error, :batcher_terminated}` | Same (already handled) |
| Batch result mismatch | `{:error, :batch_result_mismatch}` | Same (already handled) |

## Files to Change

| File | Change |
|---|---|
| `raft/batcher.ex` | Add `submitted_at` to pending, add `:check_pending` timer, add overload check in `enqueue_write`, add `@max_pending_commands` |
| `raft/state_machine.ex` | Add `safe_apply_single` wrapper around `apply_single` |

## What NOT to Change

- **Router** -- no retry logic. The caller gets the error and decides.
- **Connection handler** -- already handles `{:error, _}` tuples from Router.
- **Batcher flush timeout (10s)** -- stays as-is. The pending sweep at 8s ensures callers get errors before the flush `GenServer.call` times out.
- **Async durability path** -- unchanged. Async writes are fire-and-forget by design.

## Telemetry Events Added

| Event | Measurements | Metadata |
|---|---|---|
| `[:ferricstore, :raft, :timeout]` | `%{count: n}` | `%{shard: idx}` |
| `[:ferricstore, :raft, :rejected]` | `%{count: n}` | `%{shard: idx, reason: :not_leader}` |
| `[:ferricstore, :raft, :overloaded]` | `%{pending: n}` | `%{shard: idx}` |
