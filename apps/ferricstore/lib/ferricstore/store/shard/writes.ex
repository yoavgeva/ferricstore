defmodule Ferricstore.Store.Shard.Writes do
  @moduledoc "Shard write-path handlers: put, delete, incr, append, getset, getdel, getex, and setrange with async flush and Raft support."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  # Maximum pending entries before triggering a synchronous flush.
  @max_pending_size 10_000

  # -------------------------------------------------------------------
  # WRITE-PATH handlers (return {:reply, result, state} or {:noreply, state})
  # -------------------------------------------------------------------

  @spec handle_put(binary(), term(), non_neg_integer(), GenServer.from(), map()) :: {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_put(key, value, expire_at_ms, from, state) do
    # Reject new-key writes when the keydir is at capacity (spec 2.4).
    # Updates to existing keys are always allowed regardless of memory pressure.
    is_new = case :ets.lookup(state.keydir, key) do
      [] -> true
      _ -> false
    end

    if is_new and Ferricstore.MemoryGuard.reject_writes?() do
      Ferricstore.MemoryGuard.nudge()
      {:reply, {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}, state}
    else
      if state.raft? do
        Ferricstore.Raft.Batcher.write_async(state.index, {:put, key, value, expire_at_ms}, from)
        new_version = state.write_version + 1
        {:noreply, %{state | write_version: new_version}}
      else
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_count = state.pending_count + 1
        new_version = state.write_version + 1

        state =
          if new_count > @max_pending_size do
            s = %{state | pending: new_pending, pending_count: new_count}
            s = ShardFlush.await_in_flight(s)
            ShardFlush.flush_pending_sync(s)
          else
            %{state | pending: new_pending, pending_count: new_count}
          end

        new_state = %{state | write_version: new_version}

        if state.flush_in_flight == nil do
          {:reply, :ok, ShardFlush.flush_pending(new_state)}
        else
          {:reply, :ok, new_state}
        end
      end
    end
  end

  @spec handle_delete(binary(), GenServer.from(), map()) :: {:reply, :ok, map()} | {:noreply, map()}
  @doc false
  def handle_delete(key, from, state) do
    if state.raft? do
      Ferricstore.Raft.Batcher.write_async(state.index, {:delete, key}, from)
      new_version = state.write_version + 1
      {:noreply, %{state | write_version: new_version}}
    else
      state = ShardFlush.await_in_flight(state)
      state = ShardFlush.flush_pending_sync(state)
      state = ShardFlush.track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          ShardETS.ets_delete_key(state, key)
          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end
          new_version = state.write_version + 1
          {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}

        {:error, reason} ->
          # Do NOT delete from ETS if the tombstone write failed —
          # the key would resurrect on restart (no tombstone on disk).
          Logger.error("Shard #{state.index}: tombstone write failed for DELETE: #{inspect(reason)}")
          {:reply, {:error, "ERR disk write failed: #{inspect(reason)}"}, state}
      end
    end
  end

  @spec handle_incr(binary(), integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_incr(key, delta, state) do
    if state.raft? do
      handle_incr_raft(key, delta, state)
    else
      handle_incr_direct(key, delta, state)
    end
  end

  defp handle_incr_raft(key, delta, state) do
    {current_value, expire_at_ms} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, value, exp} -> {value, exp}
        :expired -> {nil, 0}
        :miss -> {Ferricstore.Store.Shard.Reads.do_get(state, key), 0}
      end

    case current_value do
      nil ->
        result = Ferricstore.Raft.Batcher.write(state.index, {:put, key, delta, 0})
        new_version = state.write_version + 1

        case result do
          :ok -> {:reply, {:ok, delta}, %{state | write_version: new_version}}
          {:error, _} = err -> {:reply, err, state}
        end

      value ->
        case ShardETS.coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            result = Ferricstore.Raft.Batcher.write(state.index, {:put, key, new_val, expire_at_ms})
            new_version = state.write_version + 1

            case result do
              :ok -> {:reply, {:ok, new_val}, %{state | write_version: new_version}}
              {:error, _} = err -> {:reply, err, state}
            end

          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}
        end
    end
  end

  defp handle_incr_direct(key, delta, state) do
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        case ShardETS.coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            ShardETS.ets_insert(state, key, new_val, expire_at_ms)
            new_pending = [{key, new_val, expire_at_ms} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}
        end

      :expired ->
        ShardETS.ets_insert(state, key, delta, 0)
        new_pending = [{key, delta, 0} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, delta}, new_state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get(state, key) do
          nil ->
            ShardETS.ets_insert(state, key, delta, 0)
            new_pending = [{key, delta, 0} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, delta}, new_state}

          value ->
            expire_at_ms =
              case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case ShardETS.coerce_integer(value) do
              {:ok, int_val} ->
                new_val = int_val + delta
                ShardETS.ets_insert(state, key, new_val, expire_at_ms)
                new_pending = [{key, new_val, expire_at_ms} | state.pending]
                new_version = state.write_version + 1
                new_state = %{state | pending: new_pending, write_version: new_version}

                new_state =
                  if state.flush_in_flight == nil,
                    do: ShardFlush.flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not an integer or out of range"}, state}
            end
        end
    end
  end

  @spec handle_incr_float(binary(), float(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_incr_float(key, delta, state) do
    if state.raft? do
      handle_incr_float_raft(key, delta, state)
    else
      handle_incr_float_direct(key, delta, state)
    end
  end

  defp handle_incr_float_raft(key, delta, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:incr_float, key, delta})
    new_version = state.write_version + 1

    case result do
      {:ok, _new_str} = ok -> {:reply, ok, %{state | write_version: new_version}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  defp handle_incr_float_direct(key, delta, state) do
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        case ShardETS.coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            ShardETS.ets_insert(state, key, new_val, expire_at_ms)
            new_pending = [{key, new_val, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not a valid float"}, state}
        end

      :expired ->
        new_val = delta * 1.0
        ShardETS.ets_insert(state, key, new_val, 0)
        new_pending = [{key, new_val, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, new_val}, new_state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get(state, key) do
          nil ->
            new_val = delta * 1.0
            ShardETS.ets_insert(state, key, new_val, 0)
            new_pending = [{key, new_val, 0} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          value ->
            expire_at_ms =
              case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case ShardETS.coerce_float(value) do
              {:ok, float_val} ->
                new_val = float_val + delta
                ShardETS.ets_insert(state, key, new_val, expire_at_ms)
                new_pending = [{key, new_val, expire_at_ms} | state.pending]
                new_state = %{state | pending: new_pending}

                new_state =
                  if state.flush_in_flight == nil,
                    do: ShardFlush.flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not a valid float"}, state}
            end
        end
    end
  end

  @spec handle_append(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_append(key, suffix, state) do
    if state.raft? do
      handle_append_raft(key, suffix, state)
    else
      handle_append_direct(key, suffix, state)
    end
  end

  defp handle_append_raft(key, suffix, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:append, key, suffix})
    new_version = state.write_version + 1

    case result do
      {:ok, _len} = ok -> {:reply, ok, %{state | write_version: new_version}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  defp handle_append_direct(key, suffix, state) do
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        new_val = ShardETS.to_disk_binary(value) <> suffix
        ShardETS.ets_insert(state, key, new_val, expire_at_ms)
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}

      :expired ->
        ShardETS.ets_insert(state, key, suffix, 0)
        new_pending = [{key, suffix, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(suffix)}, new_state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        {old_val, expire_at_ms} =
          case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
            {v, exp} -> {ShardETS.to_disk_binary(v), exp}
            nil -> {"", 0}
          end

        new_val = old_val <> suffix
        ShardETS.ets_insert(state, key, new_val, expire_at_ms)
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}
    end
  end

  @spec handle_getset(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_getset(key, new_value, state) do
    if state.raft? do
      handle_getset_raft(key, new_value, state)
    else
      handle_getset_direct(key, new_value, state)
    end
  end

  defp handle_getset_raft(key, new_value, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:getset, key, new_value})
    new_version = state.write_version + 1

    case result do
      {:error, _} = err -> {:reply, err, state}
      old -> {:reply, old, %{state | write_version: new_version}}
    end
  end

  defp handle_getset_direct(key, new_value, state) do
    {old, state} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, value, _expire_at_ms} -> {value, state}
        :expired -> {nil, state}
        :miss ->
          state = ShardFlush.await_in_flight(state)
          state = ShardFlush.flush_pending_sync(state)
          {Ferricstore.Store.Shard.Reads.do_get(state, key), state}
      end

    ShardETS.ets_insert(state, key, new_value, 0)
    new_pending = [{key, new_value, 0} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: ShardFlush.flush_pending(new_state),
        else: new_state

    {:reply, old, new_state}
  end

  @spec handle_getdel(binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_getdel(key, state) do
    if state.raft? do
      handle_getdel_raft(key, state)
    else
      handle_getdel_direct(key, state)
    end
  end

  defp handle_getdel_raft(key, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:getdel, key})
    new_version = state.write_version + 1

    case result do
      {:error, _} = err -> {:reply, err, state}
      old -> {:reply, old, %{state | write_version: new_version}}
    end
  end

  defp handle_getdel_direct(key, state) do
    {old, state} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, value, _expire_at_ms} -> {value, state}
        :expired -> {nil, state}
        :miss ->
          state = ShardFlush.await_in_flight(state)
          state = ShardFlush.flush_pending_sync(state)
          {Ferricstore.Store.Shard.Reads.do_get(state, key), state}
      end

    if old != nil do
      state = ShardFlush.await_in_flight(state)
      state = ShardFlush.flush_pending_sync(state)
      state = ShardFlush.track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          ShardETS.ets_delete_key(state, key)
          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end
          {:reply, old, %{state | pending: new_pending}}

        {:error, reason} ->
          Logger.error("Shard #{state.index}: tombstone write failed for GETDEL: #{inspect(reason)}")
          {:reply, {:error, reason}, state}
      end
    else
      {:reply, nil, state}
    end
  end

  @spec handle_getex(binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_getex(key, expire_at_ms, state) do
    if state.raft? do
      handle_getex_raft(key, expire_at_ms, state)
    else
      handle_getex_direct(key, expire_at_ms, state)
    end
  end

  defp handle_getex_raft(key, expire_at_ms, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:getex, key, expire_at_ms})
    new_version = state.write_version + 1

    case result do
      {:error, _} = err -> {:reply, err, state}
      value -> {:reply, value, %{state | write_version: new_version}}
    end
  end

  defp handle_getex_direct(key, expire_at_ms, state) do
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, _old_exp} ->
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, value, new_state}

      :expired ->
        {:reply, nil, state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get(state, key) do
          nil ->
            {:reply, nil, state}

          value ->
            ShardETS.ets_insert(state, key, value, expire_at_ms)
            new_pending = [{key, value, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, value, new_state}
        end
    end
  end

  @spec handle_setrange(binary(), non_neg_integer(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_setrange(key, offset, value, state) do
    if state.raft? do
      handle_setrange_raft(key, offset, value, state)
    else
      handle_setrange_direct(key, offset, value, state)
    end
  end

  defp handle_setrange_raft(key, offset, value, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:setrange, key, offset, value})
    new_version = state.write_version + 1

    case result do
      {:ok, _len} = ok -> {:reply, ok, %{state | write_version: new_version}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  defp handle_setrange_direct(key, offset, value, state) do
    {old_val, expire_at_ms} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, v, exp} -> {ShardETS.to_disk_binary(v), exp}
        :expired -> {"", 0}
        :miss ->
          state = ShardFlush.await_in_flight(state)
          state = ShardFlush.flush_pending_sync(state)

          case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
            {v, exp} -> {ShardETS.to_disk_binary(v), exp}
            nil -> {"", 0}
          end
      end

    new_val = apply_setrange(old_val, offset, value)
    ShardETS.ets_insert(state, key, new_val, expire_at_ms)
    new_pending = [{key, new_val, expire_at_ms} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: ShardFlush.flush_pending(new_state),
        else: new_state

    {:reply, {:ok, byte_size(new_val)}, new_state}
  end

  @spec handle_delete_prefix(binary(), map()) :: {:reply, :ok, map()}
  @doc false
  def handle_delete_prefix(prefix, state) do
    keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

    if state.raft? do
      Enum.each(keys_to_delete, fn key ->
        Ferricstore.Raft.Batcher.write(state.index, {:delete, key})
      end)
      new_version = state.write_version + 1
      {:reply, :ok, %{state | write_version: new_version}}
    else
      Enum.each(keys_to_delete, fn key -> ShardETS.ets_delete_key(state, key) end)
      {:reply, :ok, state}
    end
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @spec apply_setrange(binary(), non_neg_integer(), binary()) :: binary()
  @doc false
  def apply_setrange(old, offset, value) do
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
end
