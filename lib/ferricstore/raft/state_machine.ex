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
  alias Ferricstore.Store.ListOps

  @default_release_cursor_interval 1_000

  @type shard_state :: %{
          shard_index: non_neg_integer(),
          store: reference(),
          ets: atom(),
          hot_cache: atom(),
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
      hot_cache: Map.get(config, :hot_cache, :"hot_cache_#{config.shard_index}"),
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
    * `{:list_op, key, operation}` -- Execute a list operation (LPUSH, RPUSH,
      LPOP, RPOP, etc.) as an atomic read-modify-write. Reads the current value
      from ETS/Bitcask, delegates to `ListOps.execute/4`, and persists the result.
    * `{:compound_put, compound_key, value, expire_at_ms}` -- Write a hash/set/zset
      field. Inserts `{compound_key, value, expire_at_ms}` into ETS and Bitcask.
    * `{:compound_delete, compound_key}` -- Delete a hash/set/zset field. Removes
      the compound key from ETS and Bitcask.
    * `{:compound_delete_prefix, prefix}` -- Delete all compound keys matching the
      given prefix from ETS and Bitcask. Used by DEL on data structures (hashes,
      sets, sorted sets) to clean up all fields.
    * `{:incr_float, key, delta}` -- Atomic read-modify-write float increment.
      Reads the current value, parses as float, adds `delta`, formats the result,
      and writes back. Returns `{:ok, new_float_string}` or
      `{:error, "ERR value is not a valid float"}`.
    * `{:append, key, suffix}` -- Atomic read-modify-write append. Reads the
      current value (or `""`), concatenates `suffix`, writes back. Returns
      `{:ok, byte_size(new_value)}`.
    * `{:getset, key, new_value}` -- Atomic get-and-set. Reads the old value,
      writes the new value with no expiry, returns the old value (or `nil`).
    * `{:getdel, key}` -- Atomic get-and-delete. Reads the value, deletes the
      key, returns the value (or `nil`).
    * `{:getex, key, expire_at_ms}` -- Atomic get-and-update-expiry. Reads the
      value, re-writes with the new `expire_at_ms`, returns the value (or `nil`).
    * `{:setrange, key, offset, value}` -- Atomic set-range. Reads the current
      value, pads with zero bytes if needed, replaces bytes at `offset`, writes
      back. Returns `{:ok, byte_size(new_value)}`.
    * `{:cas, key, expected, new_value, ttl_ms}` -- Compare-and-swap. Reads the
      current value; if it matches `expected`, writes `new_value` with optional
      TTL. Returns `1` (swapped), `0` (mismatch), or `nil` (key missing/expired).
    * `{:lock, key, owner, ttl_ms}` -- Distributed lock acquire. If the key does
      not exist, is expired, or is already held by the same owner, sets
      `{owner, ttl}`. Returns `:ok` or `{:error, reason}`.
    * `{:unlock, key, owner}` -- Distributed lock release. If the key exists and
      the owner matches, deletes the key. Returns `1` on success,
      `{:error, reason}` on owner mismatch.
    * `{:extend, key, owner, ttl_ms}` -- Distributed lock TTL extension. If the
      key exists and the owner matches, updates the TTL. Returns `1` on success,
      `{:error, reason}` on owner mismatch or missing key.
    * `{:ratelimit_add, key, window_ms, max, count}` -- Sliding window rate
      limiter. Reads counters, rotates windows, computes effective count, and
      updates. Returns `[status, count, remaining, ttl_ms]`.

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

  def apply(meta, {:list_op, key, operation}, state) do
    result = do_list_op(state, key, operation)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:compound_put, compound_key, value, expire_at_ms}, state) do
    result = do_put(state, compound_key, value, expire_at_ms)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:compound_delete, compound_key}, state) do
    result = do_delete(state, compound_key)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:compound_delete_prefix, prefix}, state) do
    result = do_delete_prefix(state, prefix)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:incr_float, key, delta}, state) do
    result = do_incr_float(state, key, delta)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:append, key, suffix}, state) do
    result = do_append(state, key, suffix)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:getset, key, new_value}, state) do
    result = do_getset(state, key, new_value)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:getdel, key}, state) do
    result = do_getdel(state, key)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:getex, key, expire_at_ms}, state) do
    result = do_getex(state, key, expire_at_ms)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:setrange, key, offset, value}, state) do
    result = do_setrange(state, key, offset, value)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:cas, key, expected, new_value, ttl_ms}, state) do
    result = do_cas(state, key, expected, new_value, ttl_ms)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:lock, key, owner, ttl_ms}, state) do
    result = do_lock(state, key, owner, ttl_ms)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:unlock, key, owner}, state) do
    result = do_unlock(state, key, owner)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:extend, key, owner, ttl_ms}, state) do
    result = do_extend(state, key, owner, ttl_ms)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:ratelimit_add, key, window_ms, max, count}, state) do
    result = do_ratelimit_add(state, key, window_ms, max, count)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
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

  defp apply_single(state, {:list_op, key, operation}) do
    do_list_op(state, key, operation)
  end

  defp apply_single(state, {:compound_put, compound_key, value, expire_at_ms}) do
    do_put(state, compound_key, value, expire_at_ms)
  end

  defp apply_single(state, {:compound_delete, compound_key}) do
    do_delete(state, compound_key)
  end

  defp apply_single(state, {:compound_delete_prefix, prefix}) do
    do_delete_prefix(state, prefix)
  end

  defp apply_single(state, {:incr_float, key, delta}) do
    do_incr_float(state, key, delta)
  end

  defp apply_single(state, {:append, key, suffix}) do
    do_append(state, key, suffix)
  end

  defp apply_single(state, {:getset, key, new_value}) do
    do_getset(state, key, new_value)
  end

  defp apply_single(state, {:getdel, key}) do
    do_getdel(state, key)
  end

  defp apply_single(state, {:getex, key, expire_at_ms}) do
    do_getex(state, key, expire_at_ms)
  end

  defp apply_single(state, {:setrange, key, offset, value}) do
    do_setrange(state, key, offset, value)
  end

  defp apply_single(state, {:cas, key, expected, new_value, ttl_ms}) do
    do_cas(state, key, expected, new_value, ttl_ms)
  end

  defp apply_single(state, {:lock, key, owner, ttl_ms}) do
    do_lock(state, key, owner, ttl_ms)
  end

  defp apply_single(state, {:unlock, key, owner}) do
    do_unlock(state, key, owner)
  end

  defp apply_single(state, {:extend, key, owner, ttl_ms}) do
    do_extend(state, key, owner, ttl_ms)
  end

  defp apply_single(state, {:ratelimit_add, key, window_ms, max, count}) do
    do_ratelimit_add(state, key, window_ms, max, count)
  end

  defp do_put(state, key, value, expire_at_ms) do
    # Synchronous Bitcask write -- deterministic, called on every node.
    # put_batch is used for a single entry to match the existing NIF API
    # which handles fsync internally.
    case NIF.put_batch(state.store, [{key, value, expire_at_ms}]) do
      :ok ->
        :ets.insert(state.ets, {key, expire_at_ms})
        :ets.insert(state.hot_cache, {key, value})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_delete(state, key) do
    NIF.delete(state.store, key)
    :ets.delete(state.ets, key)
    :ets.delete(state.hot_cache, key)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private: string mutation operations
  # ---------------------------------------------------------------------------

  # Atomic INCRBYFLOAT: reads current value, parses as float, adds delta,
  # formats result, writes back. Preserves existing expire_at_ms.
  defp do_incr_float(state, key, delta) do
    case do_get_meta(state, key) do
      nil ->
        new_str = format_float(delta)
        do_put(state, key, new_str, 0)
        {:ok, new_str}

      {value, expire_at_ms} ->
        case parse_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            new_str = format_float(new_val)
            do_put(state, key, new_str, expire_at_ms)
            {:ok, new_str}

          :error ->
            {:error, "ERR value is not a valid float"}
        end
    end
  end

  # Parses a binary as a float. Accepts integer strings ("10") and float
  # strings ("3.14"). Returns `{:ok, float}` or `:error`. Mirrors shard.ex.
  defp parse_float(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} ->
        {:ok, val * 1.0}

      _ ->
        case Float.parse(str) do
          {val, ""} ->
            if val in [:infinity, :neg_infinity, :nan] do
              :error
            else
              {:ok, val}
            end

          _ ->
            :error
        end
    end
  end

  # Formats a float using Erlang's compact decimal format, stripping trailing
  # zeros and unnecessary decimal point (matches Redis INCRBYFLOAT output).
  defp format_float(val) when is_float(val) do
    formatted = :erlang.float_to_binary(val, [:compact, {:decimals, 17}])

    if String.contains?(formatted, ".") do
      formatted
      |> String.trim_trailing("0")
      |> String.trim_trailing(".")
      |> then(fn s -> s end)
    else
      formatted
    end
  end

  # Atomic APPEND: reads current value (or ""), concatenates suffix, writes
  # back. Preserves the existing expire_at_ms on the key.
  defp do_append(state, key, suffix) do
    {old_val, expire_at_ms} =
      case do_get_meta(state, key) do
        nil -> {"", 0}
        {v, exp} -> {v, exp}
      end

    new_val = old_val <> suffix
    do_put(state, key, new_val, expire_at_ms)
    {:ok, byte_size(new_val)}
  end

  # Atomic GETSET: reads old value, writes new value with no expiry, returns
  # old value directly (not wrapped in {:ok, ...}).
  defp do_getset(state, key, new_value) do
    old = do_get(state, key)
    do_put(state, key, new_value, 0)
    old
  end

  # Atomic GETDEL: reads value, deletes key, returns value directly (not
  # wrapped in {:ok, ...}). Returns nil if key does not exist.
  defp do_getdel(state, key) do
    old = do_get(state, key)

    if old != nil do
      do_delete(state, key)
    end

    old
  end

  # Atomic GETEX: reads value, re-writes with new expire_at_ms, returns value
  # directly (not wrapped). Returns nil if key does not exist or is expired.
  defp do_getex(state, key, expire_at_ms) do
    case do_get_meta(state, key) do
      nil ->
        nil

      {value, _old_exp} ->
        do_put(state, key, value, expire_at_ms)
        value
    end
  end

  # Atomic SETRANGE: reads current value, pads with zero bytes if needed,
  # replaces bytes at offset, writes back. Preserves expire_at_ms.
  defp do_setrange(state, key, offset, value) do
    {old_val, expire_at_ms} =
      case do_get_meta(state, key) do
        nil -> {"", 0}
        {v, exp} -> {v, exp}
      end

    new_val = sm_apply_setrange(old_val, offset, value)
    do_put(state, key, new_val, expire_at_ms)
    {:ok, byte_size(new_val)}
  end

  # Overwrites bytes at `offset` with `value`, zero-padding if the original
  # string is shorter than offset. Mirrors shard.ex apply_setrange/3.
  defp sm_apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        if offset > old_len do
          old <> :binary.copy(<<0>>, offset - old_len)
        else
          old
        end

      offset >= old_len ->
        padding = :binary.copy(<<0>>, offset - old_len)
        old <> padding <> value

      offset + val_len >= old_len ->
        binary_part(old, 0, offset) <> value

      true ->
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: compare-and-swap
  # ---------------------------------------------------------------------------

  # Reads the current value from ETS (with Bitcask fallback), compares it
  # against `expected`. If match, writes `new_value` with optional TTL.
  # Returns 1 (swapped), 0 (mismatch), or nil (missing/expired).
  #
  # Replicates the exact shard.ex handle_cas_direct logic.
  defp do_cas(state, key, expected, new_value, ttl_ms) do
    case ets_lookup(state, key) do
      {:hit, ^expected, old_exp} ->
        expire = if ttl_ms, do: System.os_time(:millisecond) + ttl_ms, else: old_exp
        do_put(state, key, new_value, expire)
        1

      {:hit, _other, _exp} ->
        0

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # Private: distributed lock operations
  # ---------------------------------------------------------------------------

  # Acquires a lock. If the key doesn't exist, is expired, or is already held
  # by the same owner, sets {owner, ttl}. Returns :ok or {:error, reason}.
  #
  # Replicates the exact shard.ex handle_lock_direct logic.
  defp do_lock(state, key, owner, ttl_ms) do
    expire = System.os_time(:millisecond) + ttl_ms

    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        # Same owner -- re-acquire (idempotent)
        do_put(state, key, owner, expire)
        :ok

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK lock is held by another owner"}

      _ ->
        # Missing or expired -- acquire
        do_put(state, key, owner, expire)
        :ok
    end
  end

  # Releases a lock. If the key exists and the owner matches, deletes the key.
  # Returns 1 on success, {:error, reason} on owner mismatch.
  #
  # Replicates the exact shard.ex handle_unlock_direct logic.
  defp do_unlock(state, key, owner) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        do_delete(state, key)
        1

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK caller is not the lock owner"}

      _ ->
        # Missing or expired -- treat as already unlocked
        1
    end
  end

  # Extends a lock's TTL. If the key exists and the owner matches, updates
  # the TTL. Returns 1 on success, {:error, reason} on mismatch or missing.
  #
  # Replicates the exact shard.ex handle_extend_direct logic.
  defp do_extend(state, key, owner, ttl_ms) do
    new_expire = System.os_time(:millisecond) + ttl_ms

    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        do_put(state, key, owner, new_expire)
        1

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK caller is not the lock owner"}

      _ ->
        {:error, "DISTLOCK lock does not exist or has expired"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: sliding window rate limiter
  # ---------------------------------------------------------------------------

  # Implements a sliding window rate limiter. Reads current counters from ETS,
  # rotates windows as needed, computes the effective count using a weighted
  # sliding window approximation, and updates the stored state.
  # Returns [status, count, remaining, ms_until_reset].
  #
  # Replicates the exact shard.ex handle_ratelimit_add_direct logic.
  defp do_ratelimit_add(state, key, window_ms, max, count) do
    now = System.os_time(:millisecond)

    {cur_count, cur_start, prv_count} =
      case ets_lookup(state, key) do
        {:hit, value, _exp} -> decode_ratelimit(value)
        _ -> {0, now, 0}
      end

    # Rotate windows
    {cur_count, cur_start, prv_count} =
      cond do
        now - cur_start >= window_ms * 2 -> {0, now, 0}
        now - cur_start >= window_ms -> {0, now, cur_count}
        true -> {cur_count, cur_start, prv_count}
      end

    # Compute effective count with sliding window approximation
    elapsed = now - cur_start
    weight = max(0.0, 1.0 - elapsed / window_ms)
    effective = cur_count + trunc(Float.round(prv_count * weight))
    expire_at_ms = cur_start + window_ms * 2

    {status, final_count, remaining, value} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        {"denied", effective, max(0, max - effective), value}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        {"allowed", new_eff, max(0, max - new_eff), value}
      end

    do_put(state, key, value, expire_at_ms)
    ms_until_reset = max(0, cur_start + window_ms - now)
    [status, final_count, remaining, ms_until_reset]
  end

  defp encode_ratelimit(cur, start, prev), do: "#{cur}:#{start}:#{prev}"

  defp decode_ratelimit(value) do
    case String.split(value, ":") do
      [cur, start, prev] ->
        {String.to_integer(cur), String.to_integer(start), String.to_integer(prev)}

      _ ->
        {0, System.os_time(:millisecond), 0}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: ETS lookup with expiry checking
  # ---------------------------------------------------------------------------

  # Reads a key from ETS, checking expiry. Falls back to Bitcask for cold
  # keys. Returns {:hit, value, expire_at_ms}, :expired, or :miss.
  # Mirrors the shard's `ets_lookup/2` logic with Bitcask fallback for
  # keys that may not yet be warmed into ETS.
  defp ets_lookup(state, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(state.ets, key) do
      [{^key, 0}] ->
        case :ets.lookup(state.hot_cache, key) do
          [{^key, value}] -> {:hit, value, 0}
          [] -> :miss
        end

      [{^key, exp}] when exp > now ->
        case :ets.lookup(state.hot_cache, key) do
          [{^key, value}] -> {:hit, value, exp}
          [] -> :miss
        end

      [{^key, _exp}] ->
        :ets.delete(state.ets, key)
        :ets.delete(state.hot_cache, key)
        :expired

      [] ->
        # ETS miss -- try Bitcask for cold keys
        case NIF.get_zero_copy(state.store, key) do
          {:ok, nil} ->
            :miss

          {:ok, value} ->
            :ets.insert(state.ets, {key, 0})
            :ets.insert(state.hot_cache, {key, value})
            {:hit, value, 0}

          _error ->
            :miss
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: list operations (read-modify-write via ListOps)
  # ---------------------------------------------------------------------------

  # Performs a complete read-modify-write for a list operation within a single
  # Raft apply. The get/put/delete closures operate directly on ETS and Bitcask
  # (the same stores available to the state machine) so the entire operation is
  # atomic from the Raft log's perspective.
  defp do_list_op(state, key, operation) do
    get_fn = fn -> do_get(state, key) end

    put_fn = fn encoded_binary ->
      do_put(state, key, encoded_binary, 0)
    end

    delete_fn = fn ->
      do_delete(state, key)
    end

    ListOps.execute(get_fn, put_fn, delete_fn, operation)
  end

  # ---------------------------------------------------------------------------
  # Private: compound delete prefix (scan + batch delete)
  # ---------------------------------------------------------------------------

  # Scans ETS for all keys matching the given prefix and deletes each from
  # both ETS and Bitcask. Used by DEL on hashes, sets, and sorted sets to
  # remove all compound fields belonging to a data structure.
  defp do_delete_prefix(state, prefix) do
    keys_to_delete =
      :ets.foldl(
        fn {key, _exp}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) do
            [key | acc]
          else
            acc
          end
        end,
        [],
        state.ets
      )

    Enum.each(keys_to_delete, fn key ->
      do_delete(state, key)
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private: read from ETS with Bitcask fallback
  # ---------------------------------------------------------------------------

  # Reads a value from ETS, falling back to Bitcask for cold keys. Mirrors
  # the shard's `do_get/2` logic so that list operations can read current
  # state within the state machine.
  defp do_get(state, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(state.ets, key) do
      [{^key, 0}] ->
        case :ets.lookup(state.hot_cache, key) do
          [{^key, value}] -> value
          [] -> nil
        end

      [{^key, exp}] when exp > now ->
        case :ets.lookup(state.hot_cache, key) do
          [{^key, value}] -> value
          [] -> nil
        end

      [{^key, _exp}] ->
        :ets.delete(state.ets, key)
        :ets.delete(state.hot_cache, key)
        nil

      [] ->
        case NIF.get_zero_copy(state.store, key) do
          {:ok, nil} ->
            nil

          {:ok, value} ->
            :ets.insert(state.ets, {key, 0})
            :ets.insert(state.hot_cache, {key, value})
            value

          _error ->
            nil
        end
    end
  end

  # Reads a value + expire_at_ms from ETS, falling back to Bitcask for cold
  # keys. Returns `{value, expire_at_ms}` or `nil`. Mirrors shard.ex
  # do_get_meta/2.
  defp do_get_meta(state, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(state.ets, key) do
      [{^key, 0}] ->
        case :ets.lookup(state.hot_cache, key) do
          [{^key, value}] -> {value, 0}
          [] -> nil
        end

      [{^key, exp}] when exp > now ->
        case :ets.lookup(state.hot_cache, key) do
          [{^key, value}] -> {value, exp}
          [] -> nil
        end

      [{^key, _exp}] ->
        :ets.delete(state.ets, key)
        :ets.delete(state.hot_cache, key)
        nil

      [] ->
        case NIF.get_zero_copy(state.store, key) do
          {:ok, nil} ->
            nil

          {:ok, value} ->
            :ets.insert(state.ets, {key, 0})
            :ets.insert(state.hot_cache, {key, value})
            {value, 0}

          _error ->
            nil
        end
    end
  end
end
