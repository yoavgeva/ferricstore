defmodule Ferricstore.Store.Shard.NativeOps do
  @moduledoc "Shard-level CAS, distributed lock, rate-limit, and list operation handlers with Raft and direct-write paths."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{ValueCodec}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Reads, as: ShardReads

  require Logger

  # -------------------------------------------------------------------
  # CAS / LOCK / UNLOCK / EXTEND / RATELIMIT / LIST handlers
  # -------------------------------------------------------------------

  @spec handle_cas(binary(), term(), binary(), non_neg_integer() | nil, map()) :: {:reply, term(), map()}
  @doc false
  def handle_cas(key, expected, new_value, ttl_ms, state) do
    if state.raft? do
      handle_cas_raft(key, expected, new_value, ttl_ms, state)
    else
      handle_cas_direct(key, expected, new_value, ttl_ms, state)
    end
  end

  defp handle_cas_raft(key, expected, new_value, ttl_ms, state) do
    expire_at_ms = if ttl_ms, do: Ferricstore.HLC.now_ms() + ttl_ms, else: nil
    result = Ferricstore.Raft.Batcher.write(state.index, {:cas, key, expected, new_value, expire_at_ms})

    case result do
      r when r in [1, 0, nil] ->
        new_version = if r == 1, do: state.write_version + 1, else: state.write_version
        {:reply, r, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_cas_direct(key, expected, new_value, ttl_ms, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^expected, old_exp}, state} ->
        expire = if ttl_ms, do: Ferricstore.HLC.now_ms() + ttl_ms, else: old_exp
        ShardETS.ets_insert(state, key, new_value, expire)
        new_pending = [{key, new_value, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: ShardFlush.flush_pending(new_state), else: new_state
        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} -> {:reply, 0, state}
      {:expired, state} -> {:reply, nil, state}
      {:missing, state} -> {:reply, nil, state}
    end
  end

  @spec handle_lock(binary(), binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_lock(key, owner, ttl_ms, state) do
    if state.raft? do
      handle_lock_raft(key, owner, ttl_ms, state)
    else
      handle_lock_direct(key, owner, ttl_ms, state)
    end
  end

  defp handle_lock_raft(key, owner, ttl_ms, state) do
    expire_at_ms = Ferricstore.HLC.now_ms() + ttl_ms
    result = Ferricstore.Raft.Batcher.write(state.index, {:lock, key, owner, expire_at_ms})

    case result do
      :ok ->
        {:reply, :ok, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_lock_direct(key, owner, ttl_ms, state) do
    expire = Ferricstore.HLC.now_ms() + ttl_ms

    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        ShardETS.ets_insert(state, key, owner, expire)
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: ShardFlush.flush_pending(new_state), else: new_state
        {:reply, :ok, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK lock is held by another owner"}, state}

      {_, state} ->
        ShardETS.ets_insert(state, key, owner, expire)
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: ShardFlush.flush_pending(new_state), else: new_state
        {:reply, :ok, new_state}
    end
  end

  @spec handle_unlock(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_unlock(key, owner, state) do
    if state.raft? do
      handle_unlock_raft(key, owner, state)
    else
      handle_unlock_direct(key, owner, state)
    end
  end

  defp handle_unlock_raft(key, owner, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:unlock, key, owner})

    case result do
      1 ->
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_unlock_direct(key, owner, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)
        state = ShardFlush.track_delete_dead_bytes(state, key)

        case NIF.v2_append_tombstone(state.active_file_path, key) do
          {:ok, _} ->
            ShardETS.ets_delete_key(state, key)
            {:reply, 1, %{state | write_version: state.write_version + 1}}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: tombstone write failed for UNLOCK: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {_, state} -> {:reply, 1, state}
    end
  end

  @spec handle_extend(binary(), binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_extend(key, owner, ttl_ms, state) do
    if state.raft? do
      handle_extend_raft(key, owner, ttl_ms, state)
    else
      handle_extend_direct(key, owner, ttl_ms, state)
    end
  end

  defp handle_extend_raft(key, owner, ttl_ms, state) do
    expire_at_ms = Ferricstore.HLC.now_ms() + ttl_ms
    result = Ferricstore.Raft.Batcher.write(state.index, {:extend, key, owner, expire_at_ms})

    case result do
      1 ->
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_extend_direct(key, owner, ttl_ms, state) do
    new_expire = Ferricstore.HLC.now_ms() + ttl_ms

    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        ShardETS.ets_insert(state, key, owner, new_expire)
        new_pending = [{key, owner, new_expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: ShardFlush.flush_pending(new_state), else: new_state
        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {_, state} ->
        {:reply, {:error, "DISTLOCK lock does not exist or has expired"}, state}
    end
  end

  @spec handle_ratelimit_add(binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_ratelimit_add(key, window_ms, max, count, state) do
    if state.raft? do
      handle_ratelimit_add_raft(key, window_ms, max, count, state)
    else
      handle_ratelimit_add_direct(key, window_ms, max, count, state)
    end
  end

  defp handle_ratelimit_add_raft(key, window_ms, max, count, state) do
    now_ms = System.os_time(:millisecond)
    result = Ferricstore.Raft.Batcher.write(state.index, {:ratelimit_add, key, window_ms, max, count, now_ms})

    case result do
      [_status, _count, _remaining, _ttl] = reply ->
        new_version = state.write_version + 1
        {:reply, reply, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  @spec handle_ratelimit_add_direct(binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), map()) :: {:reply, [term()], map()}
  @doc false
  def handle_ratelimit_add_direct(key, window_ms, max, count, state) do
    now = Ferricstore.HLC.now_ms()

    {cur_count, cur_start, prv_count} =
      case ShardETS.ets_lookup_warm(state, key) do
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

    {status, final_count, remaining, state} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}
        new_state = if state.flush_in_flight == nil, do: ShardFlush.flush_pending(new_state), else: new_state
        {"denied", effective, max(0, max - effective), new_state}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}
        new_state = if state.flush_in_flight == nil, do: ShardFlush.flush_pending(new_state), else: new_state
        {"allowed", new_eff, max(0, max - new_eff), new_state}
      end

    ms_until_reset = max(0, cur_start + window_ms - now)
    {:reply, [status, final_count, remaining, ms_until_reset], state}
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec handle_list_op(binary(), term(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_list_op(key, operation, state) do
    if state.raft? do
      handle_list_op_raft(key, operation, state)
    else
      handle_list_op_direct(key, operation, state)
    end
  end

  defp handle_list_op_raft(key, operation, state) do
    store = build_list_compound_store_raft(key, state)
    result = Ferricstore.Store.ListOps.execute(key, store, operation)
    new_version = state.write_version + 1
    {:reply, result, %{state | write_version: new_version}}
  end

  defp handle_list_op_direct(key, operation, state) do
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)
    store = build_list_compound_store_direct(key, state)
    result = Ferricstore.Store.ListOps.execute(key, store, operation)
    {:reply, result, state}
  end

  @spec handle_list_op_lmove(binary(), binary(), atom(), atom(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_list_op_lmove(src_key, dst_key, from_dir, to_dir, state) do
    if state.raft? do
      handle_list_op_lmove_raft(src_key, dst_key, from_dir, to_dir, state)
    else
      handle_list_op_lmove_direct(src_key, dst_key, from_dir, to_dir, state)
    end
  end

  defp handle_list_op_lmove_raft(src_key, dst_key, from_dir, to_dir, state) do
    store = build_list_compound_store_raft(src_key, state)
    result = Ferricstore.Store.ListOps.execute_lmove(src_key, dst_key, store, from_dir, to_dir)
    new_version = state.write_version + 1
    {:reply, result, %{state | write_version: new_version}}
  end

  defp handle_list_op_lmove_direct(src_key, dst_key, from_dir, to_dir, state) do
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)
    store = build_list_compound_store_direct(src_key, state)
    result = Ferricstore.Store.ListOps.execute_lmove(src_key, dst_key, store, from_dir, to_dir)
    {:reply, result, state}
  end

  @spec build_list_compound_store_raft(binary(), map()) :: map()
  @doc false
  def build_list_compound_store_raft(_key, state) do
    %{
      compound_get: fn _redis_key, compound_key ->
        do_compound_get(state, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        Ferricstore.Raft.Batcher.write(state.index, {:put, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn _redis_key, compound_key ->
        Ferricstore.Raft.Batcher.write(state.index, {:delete, compound_key})
      end,
      compound_scan: fn _redis_key, prefix ->
        results = ShardETS.prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
        Enum.sort_by(results, fn {field, _} -> field end)
      end
    }
  end

  @spec build_list_compound_store_direct(binary(), map()) :: map()
  @doc false
  def build_list_compound_store_direct(_key, state) do
    %{
      compound_get: fn _redis_key, compound_key ->
        do_compound_get(state, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        case NIF.v2_append_batch(state.active_file_path, [{compound_key, value, expire_at_ms}]) do
          {:ok, [{offset, _value_size}]} ->
            ShardETS.ets_insert_with_location(state, compound_key, value, expire_at_ms, state.active_file_id, offset, byte_size(value))
          _ ->
            ShardETS.ets_insert(state, compound_key, value, expire_at_ms)
        end
        :ok
      end,
      compound_delete: fn _redis_key, compound_key ->
        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            ShardETS.ets_delete_key(state, compound_key)
            :ok
          {:error, reason} ->
            Logger.error("Shard #{state.index}: tombstone write failed for list compound_delete: #{inspect(reason)}")
            {:error, reason}
        end
      end,
      compound_scan: fn _redis_key, prefix ->
        results = ShardETS.prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
        Enum.sort_by(results, fn {field, _} -> field end)
      end
    }
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @spec resolve_for_native(map(), binary()) :: {{:hit, term(), non_neg_integer()}, map()} | {:expired, map()} | {:missing, map()}
  @doc false
  def resolve_for_native(state, key) do
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, exp} -> {{:hit, value, exp}, state}
      :expired -> {:expired, state}
      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)
        case ShardReads.do_get_meta(state, key) do
          nil -> {:missing, state}
          {value, exp} -> {{:hit, value, exp}, state}
        end
    end
  end

  @spec encode_ratelimit(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: binary()
  @doc false
  def encode_ratelimit(cur, start, prev), do: ValueCodec.encode_ratelimit(cur, start, prev)

  @spec decode_ratelimit(binary()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  @doc false
  def decode_ratelimit(value), do: ValueCodec.decode_ratelimit(value)

  # Alias for compound key reads — same logic as do_get since compound keys
  # are stored as regular ETS/Bitcask entries.
  defp do_compound_get(state, compound_key), do: ShardReads.do_get(state, compound_key)
end
