defmodule Ferricstore.Raft.Batcher do
  @moduledoc """
  Namespace-aware group commit batcher for a single FerricStore shard.

  Per spec sections 2C.5 and 2F.3, each shard has its own Batcher GenServer
  that accumulates write commands into per-namespace buffers, each with its
  own commit window and durability mode. When a namespace's timer fires, only
  that namespace's buffer is flushed.

  ## How it works

  1. A client calls `write/2` which sends a `GenServer.call` to the batcher.
  2. The batcher extracts the key's namespace prefix (e.g. `"session"` from
     `"session:abc123"`, `"_root"` for keys without a colon).
  3. The namespace config is looked up from the `:ferricstore_ns_config` ETS
     table to determine `window_ms` and `durability` for this prefix.
  4. The command and caller are appended to the namespace's buffer slot,
     identified by `{prefix, durability}`.
  5. On the first write to an empty slot, a timer is started using the
     namespace's `window_ms`.
  6. When the timer fires (`:flush_slot`), only that slot's commands are
     submitted to Raft via `ra:pipeline_command/3`.
  7. Each caller receives their individual result from the batch once the
     ra command commits and the batcher receives the `ra_event` notification.

  ## Pipelined ra submission (non-blocking)

  To avoid serializing all writers through one GenServer while the previous
  batch is in-flight through Raft consensus, the batcher uses
  `ra:pipeline_command/3` instead of the blocking `ra:process_command/2`.

  `pipeline_command` is a cast -- it returns immediately with `:ok`, and
  the batcher receives an async `{ra_event, Leader, {applied, [...]}}` message
  when the command is committed and applied by the state machine.

  The batcher maintains a `pending` map keyed by correlation reference, which
  maps each in-flight batch to the list of callers (`froms`) that are waiting
  for a reply. When the `ra_event` arrives, the batcher extracts the result
  and calls `GenServer.reply/2` for each caller.

  This means the GenServer never blocks on Raft. During the time a batch is
  in-flight, the batcher continues to accept new writes and accumulate them
  into fresh slots. This eliminates the throughput bottleneck where 50 writers
  were serialized through one blocked GenServer.

  ## Namespace configuration

  Per-prefix configuration is originally sourced from the `:ferricstore_ns_config`
  ETS table managed by `Ferricstore.NamespaceConfig`. To avoid two ETS lookups
  (~400ns) on every write, the batcher caches namespace config in its process
  state (`ns_cache`). The first write for a given prefix fetches from ETS and
  caches the result; subsequent writes for the same prefix use the cached
  value with zero ETS overhead.

  When namespace config changes (via `FERRICSTORE.CONFIG SET` or `RESET`),
  `NamespaceConfig` broadcasts `:ns_config_changed` to all batcher processes,
  which clears their caches. The next write for any prefix then re-reads
  from ETS.

  If no configuration exists for a prefix, the defaults are used:
  `window_ms = 1`, `durability = :quorum`.

  For `:quorum` durability, commands are submitted to ra via
  `:ra.pipeline_command/3` with a correlation reference. Callers are replied to
  when the `ra_event` notification arrives confirming the command was applied.

  For `:async` durability (spec 2F.3), there are two entry points:

  - `Batcher.async_submit/2` (preferred) is called by `Router.async_write_*`
    after Router has already persisted locally (ETS + Bitcask for big values).
    Commands accumulate in a dedicated `{prefix, :async_origin}` slot and
    flush as one `ra.pipeline_command({:batch, [{:async, cmd}, ...]})` for
    replication. The state machine's `{:async, inner}` clause origin-skips
    on the node that already has the ETS entry. No callers to reply to —
    Router already returned `:ok` to its caller.

  - `Batcher.write/2` on an async namespace (legacy callers) is the blocking
    entry; the caller is replied `:ok` immediately when the slot is flushed,
    commands go to Raft as a regular `{:batch, [cmds]}` (no `{:async, ...}`
    wrapper) and the state machine applies them normally on every node.

  ## Why a separate GenServer?

  The batcher is intentionally separate from the Shard GenServer and the
  ra state machine. This separation keeps the batching logic independent
  of the consensus layer and allows the Shard to remain focused on read
  operations and ETS management.

  ## Configuration

    * `:shard_id` (required) -- the ra server ID for this shard
    * `:shard_index` (required) -- zero-based shard index
    * `:max_batch_size` -- flush immediately when batch reaches this size (default: 1000)
  """

  use GenServer

  require Logger

  alias Ferricstore.NamespaceConfig

  @default_max_batch_size 50_000

  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:delete, binary()}
          | {:incr, binary(), integer()}
          | {:incr_float, binary(), float()}
          | {:append, binary(), binary()}
          | {:getset, binary(), binary()}
          | {:getdel, binary()}
          | {:getex, binary(), non_neg_integer()}
          | {:setrange, binary(), non_neg_integer(), binary()}
          | {:cas, binary(), binary(), binary(), non_neg_integer() | nil}
          | {:lock, binary(), binary(), non_neg_integer()}
          | {:unlock, binary(), binary()}
          | {:extend, binary(), binary(), non_neg_integer()}
          | {:ratelimit_add, binary(), pos_integer(), pos_integer(), pos_integer()}
          | {:ratelimit_add, binary(), pos_integer(), pos_integer(), pos_integer(), non_neg_integer()}
          | {:list_op, binary(), term()}

  @typedoc """
  A slot key identifies a unique batching bucket by namespace prefix and
  durability mode. Commands with the same prefix but different durability
  modes (which can happen if config changes mid-flight) are batched
  separately.
  """
  @type slot_key :: {binary(), :quorum | :async}

  @typedoc """
  A slot holds the accumulated commands and callers for a single namespace
  buffer, along with the timer reference for that slot's commit window.
  """
  @type slot :: %{
          cmds: [command()],
          froms: [GenServer.from()],
          timer_ref: reference() | nil,
          window_ms: pos_integer()
        }

  defstruct [
    :shard_id,
    :shard_index,
    :max_batch_size,
    slots: %{},
    ns_cache: %{},
    # Map from correlation ref -> {froms, :single | :batch} for in-flight ra commands
    pending: %{},
    # List of {from} callers waiting for all in-flight to drain (flush barrier)
    flush_waiters: []
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts a batcher GenServer for the given shard.

  ## Options

    * `:shard_id` (required) -- ra server ID `{name, node()}` for this shard
    * `:shard_index` (required) -- zero-based shard index (used for process name)
    * `:max_batch_size` -- max commands per slot before forced flush (default: #{@default_max_batch_size})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: batcher_name(shard_index))
  end

  @doc """
  Submits a write command to the batcher for the given shard.

  The command is accumulated into the appropriate namespace buffer and
  submitted when the namespace's commit window expires or the buffer
  reaches `max_batch_size`.

  For `:quorum` durability, this call blocks until the ra command is
  committed and applied. For `:async` durability, the call returns as
  soon as the slot is flushed (state machine application continues in
  the background on the origin and on replicas).

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `command` -- a write command tuple, e.g. `{:put, key, value, expire_at_ms}`

  ## Returns

    * `:ok` on success
    * `{:error, reason}` on failure
  """
  @spec write(non_neg_integer(), command()) :: :ok | {:error, term()}
  def write(shard_index, command) do
    GenServer.call(batcher_name(shard_index), {:write, command}, 10_000)
  end

  @doc """
  Submits a write command asynchronously, replying directly to `reply_to`.

  Unlike `write/2`, this function does not block the calling process.
  The batcher accepts the command via `GenServer.cast` (non-blocking) and
  will call `GenServer.reply(reply_to, result)` when the command is committed.

  This is used by the Shard GenServer to avoid blocking on Raft consensus.
  The Shard returns `{:noreply, state}` and the Batcher replies directly
  to the original caller (Router/connection process).

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `command` -- a write command tuple
    * `reply_to` -- the `from` ref from the caller's `GenServer.call`
  """
  @spec write_async(non_neg_integer(), command(), GenServer.from()) :: :ok
  def write_async(shard_index, command, reply_to) do
    GenServer.cast(batcher_name(shard_index), {:write, command, reply_to})
  end

  @doc """
  Like `write_async/3` but forces quorum durability regardless of namespace
  config. Used by RMW operations (INCR, APPEND, GETSET, etc.) that need
  consensus for atomicity even when the namespace is configured async.
  """
  @spec write_async_quorum(non_neg_integer(), command(), GenServer.from()) :: :ok
  def write_async_quorum(shard_index, command, reply_to) do
    GenServer.cast(batcher_name(shard_index), {:write_quorum, command, reply_to})
  end

  @doc """
  Submits an async-durability write. Fire-and-forget.

  Called by Router on the origin node AFTER it has already written the value
  locally to ETS (and Bitcask for large values). The Batcher accumulates async
  commands in a slot and flushes them as a single batched `ra.pipeline_command`
  for replication. The caller already has `:ok` from Router — no reply needed.

  Commands are wrapped as `{:async, inner_cmd}` before submission so the
  state machine can distinguish them: on the origin node (which has the entry
  in ETS) apply/3 will skip; on replicas the inner command is applied normally.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `inner_command` -- the raw write command (e.g. `{:put, k, v, exp}`)
  """
  @spec async_submit(non_neg_integer(), command()) :: :ok
  def async_submit(shard_index, inner_command) do
    GenServer.cast(batcher_name(shard_index), {:async_submit, inner_command})
  end

  @doc """
  Returns the registered process name for the batcher at `shard_index`.

  ## Examples

      iex> Ferricstore.Raft.Batcher.batcher_name(0)
      :"Ferricstore.Raft.Batcher.0"
  """
  @spec batcher_name(non_neg_integer()) :: atom()
  def batcher_name(shard_index), do: :"Ferricstore.Raft.Batcher.#{shard_index}"

  @doc """
  Synchronously flushes all pending writes across all namespace slots.

  Used in tests and before shard shutdown to ensure all writes are committed.
  Waits for all in-flight pipelined ra commands to complete before returning.
  """
  @spec flush(non_neg_integer()) :: :ok
  def flush(shard_index) do
    GenServer.call(batcher_name(shard_index), :flush, 10_000)
  end

  @doc """
  Extracts the namespace prefix from a command's key.

  The prefix is the portion of the key before the first colon (`:`).
  Keys without a colon are assigned to the `"_root"` namespace.

  ## Parameters

    * `command` -- a write command tuple

  ## Examples

      iex> Ferricstore.Raft.Batcher.extract_prefix({:put, "session:abc", "v", 0})
      "session"

      iex> Ferricstore.Raft.Batcher.extract_prefix({:delete, "nocolon"})
      "_root"

      iex> Ferricstore.Raft.Batcher.extract_prefix({:put, "ts:sensor:42", "v", 0})
      "ts"
  """
  @spec extract_prefix(command()) :: binary()
  def extract_prefix(command) when is_tuple(command) do
    key = elem(command, 1)

    if is_binary(key) do
      case :binary.split(key, ":") do
        [^key] -> "_root"
        [prefix | _rest] -> prefix
      end
    else
      "_root"
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 is called on shutdown, allowing us to
    # reply to all pending callers instead of leaving them hung.
    Process.flag(:trap_exit, true)

    shard_id = Keyword.fetch!(opts, :shard_id)
    shard_index = Keyword.fetch!(opts, :shard_index)
    max_batch_size = Keyword.get(opts, :max_batch_size, @default_max_batch_size)

    state = %__MODULE__{
      shard_id: shard_id,
      shard_index: shard_index,
      max_batch_size: max_batch_size
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:write, command}, from, state) do
    enqueue_write(command, from, state)
  end

  def handle_call(:flush, from, state) do
    # Flush all pending slots (submits pipelined ra commands)
    new_state = flush_all_slots(state)

    # If there are in-flight pipelined commands, defer the reply until they
    # all complete. Otherwise reply immediately.
    if map_size(new_state.pending) == 0 do
      {:reply, :ok, new_state}
    else
      {:noreply, %{new_state | flush_waiters: [from | new_state.flush_waiters]}}
    end
  end

  @impl true
  def handle_cast({:write, command, reply_to}, state) do
    enqueue_write(command, reply_to, state)
  end

  def handle_cast({:async_submit, inner_command}, state) do
    enqueue_async_submit(inner_command, state)
  end

  def handle_cast({:write_quorum, command, reply_to}, state) do
    enqueue_write_forced_quorum(command, reply_to, state)
  end

  @impl true
  def handle_info({:flush_slot, slot_key}, state) do
    case Map.get(state.slots, slot_key) do
      nil ->
        {:noreply, state}

      slot ->
        # Clear the timer ref since the timer has already fired
        updated_slot = %{slot | timer_ref: nil}
        state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

        case do_flush_slot(state, slot_key) do
          {:noreply, new_state} -> {:noreply, new_state}
        end
    end
  end

  # Handle ra_event notifications from pipeline_command.
  # Applied commands: {ra_event, Leader, {applied, [{correlation, result}]}}
  def handle_info({:ra_event, _leader, {:applied, applied_list}}, state) do
    new_state =
      Enum.reduce(applied_list, state, fn {corr, result}, acc ->
        case Map.pop(acc.pending, corr) do
          {nil, _pending} ->
            # Unknown correlation -- ignore (could be stale after leader change)
            acc

          {{_froms, :async_no_reply}, new_pending} ->
            # Async batch — no callers to reply to (Router/async_ns already
            # replied :ok). Just remove from pending so flush waiters can
            # observe completion.
            %{acc | pending: new_pending}

          {{froms, :single}, new_pending} ->
            # Single command: result is the direct apply result
            [from] = froms
            GenServer.reply(from, result)
            %{acc | pending: new_pending}

          {{froms, :batch}, new_pending} ->
            reply_batch(froms, result)
            %{acc | pending: new_pending}
        end
      end)

    new_state = maybe_reply_flush_waiters(new_state)
    {:noreply, new_state}
  end

  # Handle rejected commands (not_leader). This can happen if the ra leader
  # changes during a pipeline submission. Re-submit using process_command
  # (blocking, but this is a rare recovery path) with the correct leader.
  def handle_info({:ra_event, _from_id, {:rejected, {not_leader, maybe_leader, corr}}}, state) do
    case Map.pop(state.pending, corr) do
      {nil, _pending} ->
        {:noreply, state}

      {{froms, _kind}, new_pending} ->
        new_state = %{state | pending: new_pending}

        leader =
          case {not_leader, maybe_leader} do
            {:not_leader, leader} when leader != :undefined and leader != nil -> leader
            _ -> state.shard_id
          end

        # Reply with error so callers can retry through the correct leader.
        Enum.each(froms, fn from ->
          GenServer.reply(from, {:error, {:not_leader, leader}})
        end)

        {:noreply, maybe_reply_flush_waiters(new_state)}
    end
  end

  # Handle legacy :flush messages (e.g. from cancel_timer race conditions)
  def handle_info(:flush, state) do
    {:noreply, state}
  end

  # Invalidate the namespace config cache when config changes.
  # Sent by NamespaceConfig after any set/reset operation.
  def handle_info(:ns_config_changed, state) do
    # Flush all open slots immediately so queued commands with the old
    # window_ms are processed. Next commands create fresh slots with
    # the new config values.
    new_state = flush_all_slots(state)
    {:noreply, %{new_state | ns_cache: %{}}}
  end

  # Catch-all for unexpected messages (e.g. stale Task results, DOWN messages
  # from previous implementation). Silently discard.
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Reply to all callers waiting in unflushed slots.
    Enum.each(state.slots, fn {_slot_key, slot} ->
      Enum.each(slot.froms, fn from ->
        safe_reply(from, {:error, :batcher_terminated})
      end)
    end)

    # Reply to all callers waiting for in-flight Raft commands.
    Enum.each(state.pending, fn {_corr, {froms, _kind}} ->
      Enum.each(froms, fn from ->
        safe_reply(from, {:error, :batcher_terminated})
      end)
    end)

    # Reply to flush barrier waiters.
    Enum.each(state.flush_waiters, fn from ->
      safe_reply(from, {:error, :batcher_terminated})
    end)

    :ok
  end

  defp reply_batch(froms, {:ok, results}) when is_list(results) do
    if length(results) == length(froms) do
      Enum.zip(froms, results)
      |> Enum.each(fn {from, r} -> GenServer.reply(from, r) end)
    else
      Logger.error(
        "Batcher: batch result count mismatch — " <>
          "#{length(froms)} callers but #{length(results)} results"
      )

      Enum.each(froms, fn from ->
        GenServer.reply(from, {:error, :batch_result_mismatch})
      end)
    end
  end

  defp reply_batch(froms, other) do
    Enum.each(froms, fn from -> GenServer.reply(from, other) end)
  end

  # Replies to a caller, catching errors if the caller already exited.
  defp safe_reply(from, msg) do
    try do
      GenServer.reply(from, msg)
    catch
      _, _ -> :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Private: write enqueue (shared by handle_call and handle_cast)
  # ---------------------------------------------------------------------------

  # Enqueues a write command into the appropriate namespace slot.
  # Returns `{:noreply, state}` -- the caller is replied to later when the
  # batch is flushed and committed.
  @spec enqueue_write(command(), GenServer.from(), %__MODULE__{}) :: {:noreply, %__MODULE__{}}
  defp enqueue_write(command, from, state) do
    prefix = extract_prefix(command)
    {{window_ms, durability}, state} = lookup_ns_config(prefix, state)
    slot_key = {prefix, durability}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    # For async durability: reply immediately, do not wait for flush timer.
    # The caller gets :ok now; the write is batched and flushed in background.
    if durability == :async do
      GenServer.reply(from, :ok)
    end

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        froms: if(durability == :async, do: slot.froms, else: [from | slot.froms]),
        window_ms: window_ms,
        count: Map.get(slot, :count, 0) + 1
    }

    # Start timer on first write to this slot
    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    # Flush immediately if slot is full (O(1) count check instead of O(n) length)
    if updated_slot.count >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: flush logic
  # ---------------------------------------------------------------------------

  @spec flush_all_slots(%__MODULE__{}) :: %__MODULE__{}
  defp flush_all_slots(state) do
    Enum.reduce(Map.keys(state.slots), state, fn slot_key, acc ->
      case do_flush_slot(acc, slot_key) do
        {:noreply, new_state} -> new_state
      end
    end)
  end

  @spec do_flush_slot(%__MODULE__{}, slot_key()) :: {:noreply, %__MODULE__{}}
  defp do_flush_slot(state, slot_key) do
    case Map.get(state.slots, slot_key) do
      nil ->
        {:noreply, state}

      %{cmds: []} = slot ->
        cancel_timer(slot.timer_ref)
        new_slots = Map.delete(state.slots, slot_key)
        {:noreply, %{state | slots: new_slots}}

      slot ->
        cancel_timer(slot.timer_ref)

        batch = Enum.reverse(slot.cmds)
        froms = Enum.reverse(slot.froms)

        {_prefix, durability} = slot_key

        new_state =
          case durability do
            :async_origin ->
              # Commands came via Batcher.async_submit (Router wrote locally).
              # Wrap each command as {:async, inner} so state machine can
              # origin-skip on the node that already has the ETS entry.
              submit_async_origin(state, batch)

            :async ->
              # Commands came via Batcher.write on an :async namespace
              # (blocking callers, e.g. RMW ops via Shard). Router did NOT
              # write locally — state machine must apply the inner command.
              # Reply :ok to the blocked callers, then submit as a regular
              # batch (no {:async, ...} wrapper).
              submit_async_ns(state, batch, froms)

            :quorum ->
              pipeline_submit(state, batch, froms)
          end

        # Remove the slot entirely once flushed (clean up empty slots)
        new_slots = Map.delete(new_state.slots, slot_key)
        {:noreply, %{new_state | slots: new_slots}}
    end
  end

  # Submit a batch via ra:pipeline_command/3 with a correlation ref.
  # For single commands, submit directly (no batch wrapper).
  # For multiple commands, wrap in {:batch, commands}.
  # Returns updated state with the correlation tracked in `pending`.
  @spec pipeline_submit(%__MODULE__{}, [command()], [GenServer.from()]) :: %__MODULE__{}
  defp pipeline_submit(state, [single_cmd], froms) do
    corr = make_ref()
    serialized = {:ttb, :erlang.term_to_binary(single_cmd)}
    :ra.pipeline_command(state.shard_id, serialized, corr, :normal)
    %{state | pending: Map.put(state.pending, corr, {froms, :single})}
  end

  defp pipeline_submit(state, batch, froms) do
    corr = make_ref()
    serialized = {:ttb, :erlang.term_to_binary({:batch, batch})}
    :ra.pipeline_command(state.shard_id, serialized, corr, :normal)
    %{state | pending: Map.put(state.pending, corr, {froms, :batch})}
  end

  # Flush path for commands accumulated via Batcher.async_submit (called by
  # Router.async_write_*). Router has already persisted locally (ETS + Bitcask
  # for big values) before calling async_submit. Commands are wrapped as
  # `{:async, inner}` so the state machine can origin-skip on the node that
  # already has the ETS entry. No callers to reply to — Router already got :ok.
  #
  # Tracks the correlation in `pending` with `:async_no_reply` so that
  # Batcher.flush waiters can observe when all in-flight async commands have
  # applied to the state machine (important for tests + shutdown drain).
  defp submit_async_origin(state, batch) do
    :telemetry.execute(
      [:ferricstore, :batcher, :async_flush],
      %{batch_size: length(batch)},
      %{shard_index: state.shard_index, origin: true}
    )

    wrapped = Enum.map(batch, fn cmd -> {:async, cmd} end)

    corr = make_ref()
    serialized = {:ttb, :erlang.term_to_binary({:batch, wrapped})}
    :ra.pipeline_command(state.shard_id, serialized, corr, :normal)
    %{state | pending: Map.put(state.pending, corr, {[], :async_no_reply})}
  end

  # Flush path for commands accumulated via Batcher.write (blocking) on an
  # :async-durability namespace. Router did NOT write locally — the state
  # machine must apply the inner command. Reply :ok to blocked callers, then
  # submit commands as a regular batch (unwrapped) via ra.pipeline_command.
  # Tracks the correlation in `pending` so flush waiters observe completion.
  defp submit_async_ns(state, batch, froms) do
    :telemetry.execute(
      [:ferricstore, :batcher, :async_flush],
      %{batch_size: length(batch)},
      %{shard_index: state.shard_index, origin: false}
    )

    Enum.each(froms, fn from -> GenServer.reply(from, :ok) end)

    corr = make_ref()
    serialized = {:ttb, :erlang.term_to_binary({:batch, batch})}
    :ra.pipeline_command(state.shard_id, serialized, corr, :normal)
    %{state | pending: Map.put(state.pending, corr, {[], :async_no_reply})}
  end

  # Enqueue a write that MUST go through quorum (RMW ops where the caller
  # needs atomicity). Bypasses namespace-config durability lookup and uses
  # the quorum slot regardless of how the namespace is configured.
  defp enqueue_write_forced_quorum(command, from, state) do
    prefix = extract_prefix(command)
    # Still need window_ms from config, but force durability to quorum.
    {{window_ms, _durability}, state} = lookup_ns_config(prefix, state)
    slot_key = {prefix, :quorum}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        froms: [from | slot.froms],
        window_ms: window_ms,
        count: Map.get(slot, :count, 0) + 1
    }

    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    if updated_slot.count >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
    end
  end

  # Enqueue an async_submit cast (from Router.async_write_*). No `from` to
  # track — Router already returned :ok to its caller.
  defp enqueue_async_submit(command, state) do
    prefix = extract_prefix(command)
    {{window_ms, _durability}, state} = lookup_ns_config(prefix, state)
    # Dedicated slot: commands here will be wrapped as {:async, inner} during
    # flush so the state machine knows Router has already persisted locally
    # on the origin (origin-skip optimization in state_machine.ex). This
    # differs from {prefix, :async} slots where commands came through
    # Batcher.write on an :async namespace — those go to the state machine
    # unwrapped because Router did NOT write locally (it was a blocking call
    # through Shard, e.g. for RMW ops routed to quorum).
    slot_key = {prefix, :async_origin}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        # No froms for async_submit — Router already replied.
        froms: slot.froms,
        window_ms: window_ms,
        count: Map.get(slot, :count, 0) + 1
    }

    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    if updated_slot.count >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
    end
  end

  # Reply to flush waiters when all in-flight pipelined commands have completed.
  @spec maybe_reply_flush_waiters(%__MODULE__{}) :: %__MODULE__{}
  defp maybe_reply_flush_waiters(%{pending: pending, flush_waiters: waiters} = state) do
    if map_size(pending) == 0 and waiters != [] do
      Enum.each(waiters, fn from -> GenServer.reply(from, :ok) end)
      %{state | flush_waiters: []}
    else
      state
    end
  end

  # ---------------------------------------------------------------------------
  # Private: namespace config lookup
  # ---------------------------------------------------------------------------

  @spec lookup_ns_config(binary(), %__MODULE__{}) ::
          {{pos_integer(), :quorum | :async}, %__MODULE__{}}
  defp lookup_ns_config(prefix, state) do
    case Map.get(state.ns_cache, prefix) do
      nil ->
        window = NamespaceConfig.window_for(prefix)
        durability = NamespaceConfig.durability_for(prefix)
        new_cache = Map.put(state.ns_cache, prefix, {window, durability})
        {{window, durability}, %{state | ns_cache: new_cache}}

      cached ->
        {cached, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: slot helpers
  # ---------------------------------------------------------------------------

  @spec new_slot(pos_integer()) :: slot()
  defp new_slot(window_ms) do
    %{cmds: [], froms: [], timer_ref: nil, window_ms: window_ms, count: 0}
  end

  defp cancel_timer(nil), do: :ok

  # Uses `info: false` to avoid a return-value message, and skips the
  # selective receive that scanned the entire mailbox (~1-5us under load).
  # Stale {:flush_slot, _} messages are harmless: handle_info already
  # handles the case where the slot no longer exists (returns {:noreply, state}).
  defp cancel_timer(ref), do: Process.cancel_timer(ref, info: false)
end
