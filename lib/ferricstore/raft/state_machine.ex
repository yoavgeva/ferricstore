defmodule Ferricstore.Raft.StateMachine do
  @moduledoc """
  Ra state machine for a single FerricStore shard.

  Each shard is an independent Raft group. The state machine receives write
  commands via `apply/3`, which deterministically applies them to both the
  Bitcask persistent store (via synchronous NIF) and the ETS hot cache.

  ## Callbacks

    * `init/1` -- receives the shard config, returns initial machine state.
    * `apply/3` -- deterministic command application (called on every node).
      Supports `:put`, `:delete`, and `:batch` commands.
    * `state_enter/2` -- lifecycle hook for leader/follower transitions.
    * `tick/2` -- periodic callback (unused currently, placeholder for metrics).
    * `init_aux/1` -- initializes non-replicated auxiliary state.
    * `handle_aux/5` -- handles non-replicated auxiliary commands (new API).
    * `overview/1` -- returns a summary map for debugging/monitoring.

  ## Design notes

  Per the spec (section 2C.4):
  - `apply/3` is deterministic and runs on every node in the Raft group.
  - Only synchronous NIF calls are allowed inside `apply/3`.
  - Effects (`send_msg`, `release_cursor`) are returned as the third element
    of the apply return tuple.
  - In single-node mode, the shard's Raft group has one member (self quorum),
    so every write commits immediately after local log append + fsync.

  ## Log compaction (spec 2E.5)

  The Raft log grows unbounded unless compacted. Every
  `:release_cursor_interval` applied commands (default: 1000), `apply/3`
  emits a `{:release_cursor, ra_index, state}` effect. This tells ra that
  all log entries up to `ra_index` are fully reflected in the given state
  snapshot and can be safely truncated.

  The interval is stored in the machine state at init time (from the config
  map or application env) so that `apply/3` remains deterministic -- it never
  reads runtime configuration.
  """

  @behaviour :ra_machine

  alias Ferricstore.Bitcask.NIF

  @default_release_cursor_interval 1_000

  @type shard_state :: %{
          shard_index: non_neg_integer(),
          store: reference(),
          ets: atom(),
          applied_count: non_neg_integer(),
          release_cursor_interval: pos_integer()
        }

  # ---------------------------------------------------------------------------
  # ra_machine callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Initializes the state machine for a shard.

  The `config` map must include:

    * `:shard_index` -- zero-based shard index
    * `:store` -- Bitcask NIF reference (already opened)
    * `:ets` -- ETS table name (already created)

  Optional:

    * `:release_cursor_interval` -- number of applies between release_cursor
      effects (default: #{@default_release_cursor_interval}). Can also be set
      via `Application.get_env(:ferricstore, :release_cursor_interval)`.

  Returns the initial machine state.
  """
  @impl true
  @spec init(map()) :: shard_state()
  def init(config) do
    interval =
      Map.get_lazy(config, :release_cursor_interval, fn ->
        Application.get_env(:ferricstore, :release_cursor_interval, @default_release_cursor_interval)
      end)

    %{
      shard_index: config.shard_index,
      store: config.store,
      ets: config.ets,
      applied_count: 0,
      release_cursor_interval: interval
    }
  end

  @doc """
  Applies a replicated command to the shard state.

  Supported commands:

    * `{:put, key, value, expire_at_ms}` -- Write a key-value pair with optional
      expiry. Writes to Bitcask (sync NIF) and updates ETS.
    * `{:delete, key}` -- Delete a key. Writes a tombstone to Bitcask, removes
      from ETS.
    * `{:batch, commands}` -- Apply a list of commands atomically. Each command
      in the batch is a tuple matching one of the above forms. Returns
      `{:ok, results}` where results is a list of individual command results.

  Returns `{new_state, result}` or `{new_state, result, effects}`.
  """
  @impl true
  def apply(meta, {:put, key, value, expire_at_ms}, state) do
    result = do_put(state, key, value, expire_at_ms)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:delete, key}, state) do
    result = do_delete(state, key)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:batch, commands}, state) do
    old_count = state.applied_count

    {results, new_count} =
      Enum.map_reduce(commands, old_count, fn cmd, count ->
        result = apply_single(state, cmd)
        {result, count + 1}
      end)

    new_state = %{state | applied_count: new_count}
    maybe_release_cursor(meta, old_count, new_state, {:ok, results})
  end

  @doc """
  Lifecycle hook called when the Raft node transitions roles.

  In single-node mode, the node is always the leader. In multi-node clusters,
  this can be used to start/stop leader-only processes (e.g., merge scheduler,
  active expiry sweeper).

  Returns a list of effects (currently empty).
  """
  @impl true
  def state_enter(:leader, _state), do: []
  def state_enter(:follower, _state), do: []
  def state_enter(:candidate, _state), do: []
  def state_enter(:await_condition, _state), do: []
  def state_enter(:delete_and_terminate, _state), do: []
  def state_enter(:receive_snapshot, _state), do: []
  def state_enter(_role, _state), do: []

  @doc """
  Periodic tick callback. Returns a list of effects (currently empty).
  """
  @impl true
  def tick(_time_ms, _state) do
    []
  end

  @doc """
  Initializes non-replicated auxiliary state.

  Aux state is local to each node and not replicated via Raft. Used for
  tracking hot-key statistics and other node-local metadata.
  """
  @impl true
  def init_aux(_name) do
    %{hot_keys: %{}}
  end

  @doc """
  Handles non-replicated auxiliary commands (5-arity new API).

  The `int_state` parameter is ra's internal state and must be passed back
  unchanged in the return tuple.

  Currently supports:
    * `{:cast, {:key_written, key}}` -- Increments a local hot-key counter.
  """
  @impl true
  def handle_aux(_raft_state, :cast, {:key_written, key}, aux, int_state) do
    count = Map.get(aux.hot_keys, key, 0)
    new_aux = %{aux | hot_keys: Map.put(aux.hot_keys, key, count + 1)}
    {:no_reply, new_aux, int_state}
  end

  def handle_aux(_raft_state, _type, _cmd, aux, int_state) do
    {:no_reply, aux, int_state}
  end

  @doc """
  Returns a summary map for debugging and monitoring.

  Includes the shard index, ETS keydir size, total applied command count,
  and the release_cursor interval.
  """
  @impl true
  def overview(state) do
    ets_size =
      try do
        :ets.info(state.ets, :size)
      rescue
        ArgumentError -> 0
      end

    %{
      shard_index: state.shard_index,
      keydir_size: ets_size,
      applied_count: state.applied_count,
      release_cursor_interval: state.release_cursor_interval
    }
  end

  # ---------------------------------------------------------------------------
  # Private: release_cursor compaction
  # ---------------------------------------------------------------------------

  # Checks whether the applied_count crossed an interval boundary AND the
  # ra meta contains a valid index. If both conditions are met, emits a
  # `{:release_cursor, ra_index, state}` effect so ra can compact the log
  # up to this point.
  #
  # For single commands (put/delete), old_count + 1 == new applied_count,
  # so `div(old, interval) != div(new, interval)` is equivalent to
  # `rem(new, interval) == 0`.
  #
  # For batches, the applied_count may jump by N, potentially crossing one
  # or more interval boundaries. We emit a single release_cursor at the
  # batch's ra index when any boundary was crossed.
  #
  # When meta has no :index (e.g. unit tests calling apply/3 directly with
  # an empty map), the 2-tuple `{state, result}` is returned and no effect
  # is emitted.
  @spec maybe_release_cursor(map(), non_neg_integer(), shard_state(), term()) ::
          {shard_state(), term()} | {shard_state(), term(), list()}
  defp maybe_release_cursor(%{index: ra_index}, old_count, state, result) do
    interval = state.release_cursor_interval

    if div(old_count, interval) != div(state.applied_count, interval) do
      {state, result, [{:release_cursor, ra_index, state}]}
    else
      {state, result}
    end
  end

  defp maybe_release_cursor(_meta, _old_count, state, result) do
    {state, result}
  end

  # ---------------------------------------------------------------------------
  # Private: command execution
  # ---------------------------------------------------------------------------

  defp apply_single(state, {:put, key, value, expire_at_ms}) do
    do_put(state, key, value, expire_at_ms)
  end

  defp apply_single(state, {:delete, key}) do
    do_delete(state, key)
  end

  defp do_put(state, key, value, expire_at_ms) do
    # Synchronous Bitcask write -- deterministic, called on every node.
    # put_batch is used for a single entry to match the existing NIF API
    # which handles fsync internally.
    case NIF.put_batch(state.store, [{key, value, expire_at_ms}]) do
      :ok ->
        :ets.insert(state.ets, {key, value, expire_at_ms})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_delete(state, key) do
    NIF.delete(state.store, key)
    :ets.delete(state.ets, key)
    :ok
  end
end
