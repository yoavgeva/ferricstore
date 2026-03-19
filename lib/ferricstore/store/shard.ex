defmodule Ferricstore.Store.Shard do
  @moduledoc """
  GenServer managing one Bitcask partition backed by an ETS hot-read cache.

  ## Write path: group commit

  To hit high write throughput (50k+ req/s), individual `put` calls do **not**
  block waiting for an fsync except for the very first write in each batch
  window:

  1. The key is written to ETS immediately (reads see it at once).
  2. The entry is appended to an in-memory pending list.
  3. If this is the **first write in a new batch window**, the pending list is
     flushed via `NIF.put_batch_async/2`. On Linux with io_uring this submits
     writes + fsync to the ring and returns immediately. On other platforms it
     falls back to synchronous `put_batch`.
  4. If the batch window already has pending writes, the put returns immediately
     — the new entry will be flushed by the recurring timer or the next sync
     point.
  5. A recurring timer fires every `@flush_interval_ms` (1 ms by default) and
     calls `NIF.put_batch_async/2` with all accumulated entries.

  ## Async I/O lifecycle

  When `NIF.put_batch_async/2` returns `{:pending, op_id}`, the shard stores
  `op_id` in `flush_in_flight`. While a flush is in-flight, subsequent
  `flush_pending` calls are no-ops — new writes accumulate in `pending` and
  will be flushed on the next timer tick after the in-flight completes.

  When the fsync CQE arrives, the NIF sends `{:io_complete, op_id, result}`
  to this process. The `handle_info` callback clears `flush_in_flight`,
  allowing the next timer tick to flush any accumulated pending writes.

  ## Read path: ETS bypass

  `Router.get/1` and `Router.get_meta/1` read ETS directly without going
  through this GenServer for hot (cached) keys. Only cold keys (not yet in
  ETS) fall back to a `{:get, key}` call here, which loads from Bitcask and
  warms the cache.

  ## ETS layout

  Each entry is a tuple `{key, value, expire_at_ms}` where `expire_at_ms = 0`
  means the key never expires. Expired entries are lazily evicted on read.

  ## Process registration

  Shards register under the name returned by
  `Ferricstore.Store.Router.shard_name/1`, e.g.
  `:"Ferricstore.Store.Shard.0"`.
  """

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Router

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1

  # Timeout for synchronous flush (blocking receive for async completion).
  @sync_flush_timeout_ms 5_000
  @default_sweep_interval_ms 1_000
  @default_max_keys_per_sweep 100

  defstruct [
    :store,
    :ets,
    :index,
    :data_dir,
    pending: [],
    flush_in_flight: nil,
    write_version: 0,
    sweep_at_ceiling_count: 0,
    sweep_struggling: false,
    promoted_instances: %{}
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a shard GenServer.

  ## Options

    * `:index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:flush_interval_ms` -- batch-commit interval in ms (default: #{@flush_interval_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    name = Router.shard_name(index)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
    path = Ferricstore.DataDir.shard_data_path(data_dir, index)
    File.mkdir_p!(path)
    {:ok, store} = NIF.new(path)

    ets =
      case :ets.whereis(:"shard_ets_#{index}") do
        :undefined ->
          :ets.new(:"shard_ets_#{index}", [:set, :public, :named_table])

        _ref ->
          # Clear stale data from previous incarnation (e.g. after supervisor
          # restart or test-induced crash). The Bitcask log is the source of
          # truth — the keydir is rebuilt from hint files, not from ETS.
          :ets.delete_all_objects(:"shard_ets_#{index}")
          :"shard_ets_#{index}"
      end

    # Start the Raft server for this shard if raft is enabled.
    # The ra system must already be started (done in Application.start).
    if Application.get_env(:ferricstore, :raft_enabled, true) do
      Ferricstore.Raft.Cluster.start_shard_server(index, store, ets)
    end

    # Recover any promoted collection instances from the shared Bitcask.
    promoted =
      Ferricstore.Store.Promotion.recover_promoted(store, ets, data_dir, index)

    schedule_flush(flush_ms)
    schedule_expiry_sweep()
    {:ok, %__MODULE__{store: store, ets: ets, index: index, data_dir: data_dir,
                       pending: [], flush_in_flight: nil,
                       promoted_instances: promoted},
     {:continue, {:flush_interval, flush_ms}}}
  end

  @impl true
  def handle_continue({:flush_interval, ms}, state) do
    # Store flush interval in process dictionary so handle_info can reschedule.
    Process.put(:flush_interval_ms, ms)
    {:noreply, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    # Fast path: ETS hit — no need to wait for in-flight writes.
    case ets_lookup(state.ets, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      :miss ->
        # Cold path: key not in ETS. Flush any pending/in-flight async writes
        # so Bitcask has the latest data before we query it.
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        {:reply, warm_from_store(state, key), state}
    end
  end

  def handle_call({:get_meta, key}, _from, state) do
    case ets_lookup(state.ets, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, {value, expire_at_ms}, state}

      :expired ->
        {:reply, nil, state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        {:reply, do_get_meta(state, key), state}
    end
  end

  # Compound key scan: returns all live entries matching a prefix.
  # Used by HSCAN, SSCAN, ZSCAN via the compound_scan store callback.
  def handle_call({:scan_prefix, prefix}, _from, state) do
    now = System.os_time(:millisecond)

    results =
      :ets.foldl(
        fn {key, value, exp}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
            # Extract sub-key after the null byte separator
            field =
              case :binary.split(key, <<0>>) do
                [_prefix_part, sub] -> sub
                _ -> key
              end

            [{field, value} | acc]
          else
            acc
          end
        end,
        [],
        state.ets
      )

    {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
  end

  # Count entries matching a compound key prefix.
  def handle_call({:count_prefix, prefix}, _from, state) do
    now = System.os_time(:millisecond)

    count =
      :ets.foldl(
        fn {key, _value, exp}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
            acc + 1
          else
            acc
          end
        end,
        0,
        state.ets
      )

    {:reply, count, state}
  end

  # Delete all entries matching a compound key prefix.
  def handle_call({:delete_prefix, prefix}, _from, state) do
    keys_to_delete =
      :ets.foldl(
        fn {key, _value, _exp}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) do
            [key | acc]
          else
            acc
          end
        end,
        [],
        state.ets
      )

    Enum.each(keys_to_delete, fn key -> :ets.delete(state.ets, key) end)
    {:reply, :ok, state}
  end

  # -------------------------------------------------------------------
  # Promotion-aware compound operations
  #
  # These handlers route to either the shared Bitcask or a dedicated
  # promoted Bitcask based on whether the redis_key has been promoted.
  # They also trigger promotion checks after writes.
  # -------------------------------------------------------------------

  def handle_call({:compound_get, redis_key, compound_key}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- use ETS/shared Bitcask (same as {:get, compound_key})
        case ets_lookup(state.ets, compound_key) do
          {:hit, value, _exp} -> {:reply, value, state}
          :expired -> {:reply, nil, state}
          :miss ->
            state = await_in_flight(state)
            state = flush_pending_sync(state)
            {:reply, warm_from_store(state, compound_key), state}
        end

      dedicated ->
        # Promoted -- read from ETS first, then dedicated Bitcask
        case ets_lookup(state.ets, compound_key) do
          {:hit, value, _exp} -> {:reply, value, state}
          :expired -> {:reply, nil, state}
          :miss ->
            case NIF.get(dedicated, compound_key) do
              {:ok, nil} -> {:reply, nil, state}
              {:ok, value} ->
                :ets.insert(state.ets, {compound_key, value, 0})
                {:reply, value, state}
              _error -> {:reply, nil, state}
            end
        end
    end
  end

  def handle_call({:compound_get_meta, redis_key, compound_key}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        case ets_lookup(state.ets, compound_key) do
          {:hit, value, expire_at_ms} -> {:reply, {value, expire_at_ms}, state}
          :expired -> {:reply, nil, state}
          :miss ->
            state = await_in_flight(state)
            state = flush_pending_sync(state)
            {:reply, warm_meta_from_store(state, compound_key), state}
        end

      dedicated ->
        case ets_lookup(state.ets, compound_key) do
          {:hit, value, expire_at_ms} -> {:reply, {value, expire_at_ms}, state}
          :expired -> {:reply, nil, state}
          :miss ->
            case NIF.get(dedicated, compound_key) do
              {:ok, nil} -> {:reply, nil, state}
              {:ok, value} ->
                :ets.insert(state.ets, {compound_key, value, 0})
                {:reply, {value, 0}, state}
              _error -> {:reply, nil, state}
            end
        end
    end
  end

  def handle_call({:compound_put, redis_key, compound_key, value, expire_at_ms}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- write to ETS + shared pending batch
        :ets.insert(state.ets, {compound_key, value, expire_at_ms})
        new_pending = [{compound_key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        # Check if this key should be promoted
        new_state = maybe_promote(new_state, redis_key, compound_key)

        {:reply, :ok, new_state}

      dedicated ->
        # Promoted -- write to ETS + dedicated Bitcask directly
        :ets.insert(state.ets, {compound_key, value, expire_at_ms})
        NIF.put(dedicated, compound_key, value, expire_at_ms)
        {:reply, :ok, state}
    end
  end

  def handle_call({:compound_delete, redis_key, compound_key}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- synchronous delete from shared Bitcask
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        NIF.delete(state.store, compound_key)
        :ets.delete(state.ets, compound_key)
        new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == compound_key end)
        new_version = state.write_version + 1
        {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}

      dedicated ->
        # Promoted -- delete from dedicated Bitcask
        NIF.delete(dedicated, compound_key)
        :ets.delete(state.ets, compound_key)
        {:reply, :ok, state}
    end
  end

  def handle_call({:compound_scan, redis_key, prefix}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- scan ETS (same as {:scan_prefix, prefix})
        now = System.os_time(:millisecond)

        results =
          :ets.foldl(
            fn {key, value, exp}, acc ->
              if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
                field =
                  case :binary.split(key, <<0>>) do
                    [_prefix_part, sub] -> sub
                    _ -> key
                  end

                [{field, value} | acc]
              else
                acc
              end
            end,
            [],
            state.ets
          )

        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}

      dedicated ->
        # Promoted -- scan dedicated Bitcask via get_all, filter by prefix
        now = System.os_time(:millisecond)

        # First check ETS for warm entries
        ets_results =
          :ets.foldl(
            fn {key, value, exp}, acc ->
              if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
                field =
                  case :binary.split(key, <<0>>) do
                    [_prefix_part, sub] -> sub
                    _ -> key
                  end

                [{field, value} | acc]
              else
                acc
              end
            end,
            [],
            state.ets
          )

        # Also get from dedicated Bitcask for entries not yet in ETS
        bitcask_results =
          case NIF.get_all(dedicated) do
            {:ok, pairs} ->
              ets_keys = MapSet.new(ets_results, fn {field, _} -> field end)

              pairs
              |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
              |> Enum.map(fn {k, v} ->
                field =
                  case :binary.split(k, <<0>>) do
                    [_prefix_part, sub] -> sub
                    _ -> k
                  end

                {field, v}
              end)
              |> Enum.reject(fn {field, _v} -> MapSet.member?(ets_keys, field) end)

            _ ->
              []
          end

        all_results = ets_results ++ bitcask_results
        {:reply, Enum.sort_by(all_results, fn {field, _} -> field end), state}
    end
  end

  def handle_call({:compound_count, redis_key, prefix}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- count in ETS (same as {:count_prefix, prefix})
        now = System.os_time(:millisecond)

        count =
          :ets.foldl(
            fn {key, _value, exp}, acc ->
              if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
                acc + 1
              else
                acc
              end
            end,
            0,
            state.ets
          )

        {:reply, count, state}

      dedicated ->
        # Promoted -- count from dedicated Bitcask
        count =
          case NIF.keys(dedicated) do
            keys when is_list(keys) ->
              Enum.count(keys, &String.starts_with?(&1, prefix))

            _ ->
              0
          end

        {:reply, count, state}
    end
  end

  def handle_call({:compound_delete_prefix, redis_key, prefix}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- delete from ETS (same as {:delete_prefix, prefix})
        keys_to_delete =
          :ets.foldl(
            fn {key, _value, _exp}, acc ->
              if is_binary(key) and String.starts_with?(key, prefix) do
                [key | acc]
              else
                acc
              end
            end,
            [],
            state.ets
          )

        Enum.each(keys_to_delete, fn key -> :ets.delete(state.ets, key) end)
        {:reply, :ok, state}

      _dedicated ->
        # Promoted -- clean up the dedicated Bitcask entirely
        alias Ferricstore.Store.Promotion

        # Delete compound keys from ETS
        keys_to_delete =
          :ets.foldl(
            fn {key, _value, _exp}, acc ->
              if is_binary(key) and String.starts_with?(key, prefix) do
                [key | acc]
              else
                acc
              end
            end,
            [],
            state.ets
          )

        Enum.each(keys_to_delete, fn key -> :ets.delete(state.ets, key) end)

        # Clean up the dedicated instance and remove from state
        Promotion.cleanup_promoted!(
          redis_key,
          state.store,
          state.ets,
          state.data_dir,
          state.index
        )

        new_promoted = Map.delete(state.promoted_instances, redis_key)
        {:reply, :ok, %{state | promoted_instances: new_promoted}}
    end
  end

  # Check if a redis_key is promoted.
  def handle_call({:promoted?, redis_key}, _from, state) do
    {:reply, Map.has_key?(state.promoted_instances, redis_key), state}
  end

  def handle_call({:put, key, value, expire_at_ms}, _from, state) do
    # Reject new-key writes when the keydir is at capacity (spec 2.4).
    # Updates to existing keys are always allowed regardless of memory pressure.
    is_new = :ets.lookup(state.ets, key) == []

    if is_new and Ferricstore.MemoryGuard.reject_writes?() do
      {:reply, {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}, state}
    else
      if raft_enabled?() do
        # Raft path: route through Batcher -> ra -> StateMachine.apply
        # StateMachine.apply writes to ETS + Bitcask synchronously.
        # Batcher.write blocks until the Raft commit completes.
        alias Ferricstore.Raft.Batcher
        result = Batcher.write(state.index, {:put, key, value, expire_at_ms})
        new_version = state.write_version + 1
        {:reply, result, %{state | write_version: new_version}}
      else
        # Direct path (no Raft): write to ETS immediately so reads see it
        # right away, then queue for async Bitcask flush.
        :ets.insert(state.ets, {key, value, expire_at_ms})
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        # Flush immediately when no async flush is in-flight. This ensures every
        # put is submitted to io_uring (and thus kernel-managed) before the call
        # returns, providing crash durability even if the process is killed before
        # the timer fires. Multiple puts arriving while a flush is in-flight are
        # batched together and flushed on the next timer tick after the CQE.
        if state.flush_in_flight == nil do
          {:reply, :ok, flush_pending(new_state)}
        else
          {:reply, :ok, new_state}
        end
      end
    end
  end

  # Atomic increment: reads current value, parses as integer, adds delta, writes back.
  # Returns {:ok, new_integer} or {:error, reason}.
  def handle_call({:incr, key, delta}, _from, state) do
    if raft_enabled?() do
      handle_incr_raft(key, delta, state)
    else
      handle_incr_direct(key, delta, state)
    end
  end

  # Raft path for INCR: reads the current value from ETS/Bitcask (local read),
  # computes the new value, then routes the resulting put through the Raft
  # Batcher so the write is replicated and committed before replying.
  defp handle_incr_raft(key, delta, state) do
    alias Ferricstore.Raft.Batcher

    {current_value, expire_at_ms} =
      case ets_lookup(state.ets, key) do
        {:hit, value, exp} -> {value, exp}
        :expired -> {nil, 0}
        :miss -> {do_get(state, key), 0}
      end

    case current_value do
      nil ->
        new_str = Integer.to_string(delta)
        result = Batcher.write(state.index, {:put, key, new_str, 0})
        new_version = state.write_version + 1

        case result do
          :ok -> {:reply, {:ok, delta}, %{state | write_version: new_version}}
          {:error, _} = err -> {:reply, err, state}
        end

      value ->
        case parse_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            new_str = Integer.to_string(new_val)
            result = Batcher.write(state.index, {:put, key, new_str, expire_at_ms})
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

  # Direct path for INCR (no Raft): reads current value, computes new value,
  # writes to ETS + pending batch for async Bitcask flush.
  defp handle_incr_direct(key, delta, state) do
    case ets_lookup(state.ets, key) do
      {:hit, value, expire_at_ms} ->
        case parse_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            new_str = Integer.to_string(new_val)
            :ets.insert(state.ets, {key, new_str, expire_at_ms})
            new_pending = [{key, new_str, expire_at_ms} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}
        end

      :expired ->
        # Treat as non-existent: set to delta
        new_str = Integer.to_string(delta)
        :ets.insert(state.ets, {key, new_str, 0})
        new_pending = [{key, new_str, 0} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, delta}, new_state}

      :miss ->
        # Check Bitcask
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        case do_get(state, key) do
          nil ->
            new_str = Integer.to_string(delta)
            :ets.insert(state.ets, {key, new_str, 0})
            new_pending = [{key, new_str, 0} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, delta}, new_state}

          value ->
            # get the metadata for the expire
            expire_at_ms =
              case do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case parse_integer(value) do
              {:ok, int_val} ->
                new_val = int_val + delta
                new_str = Integer.to_string(new_val)
                :ets.insert(state.ets, {key, new_str, expire_at_ms})
                new_pending = [{key, new_str, expire_at_ms} | state.pending]
                new_version = state.write_version + 1
                new_state = %{state | pending: new_pending, write_version: new_version}

                new_state =
                  if state.flush_in_flight == nil,
                    do: flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not an integer or out of range"}, state}
            end
        end
    end
  end

  # Atomic float increment: reads current value, parses as float, adds delta, writes back.
  # Returns {:ok, new_float_string} or {:error, reason}.
  def handle_call({:incr_float, key, delta}, _from, state) do
    case ets_lookup(state.ets, key) do
      {:hit, value, expire_at_ms} ->
        case parse_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            new_str = format_float(new_val)
            :ets.insert(state.ets, {key, new_str, expire_at_ms})
            new_pending = [{key, new_str, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_str}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not a valid float"}, state}
        end

      :expired ->
        new_str = format_float(delta)
        :ets.insert(state.ets, {key, new_str, 0})
        new_pending = [{key, new_str, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, new_str}, new_state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        case do_get(state, key) do
          nil ->
            new_str = format_float(delta)
            :ets.insert(state.ets, {key, new_str, 0})
            new_pending = [{key, new_str, 0} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_str}, new_state}

          value ->
            expire_at_ms =
              case do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case parse_float(value) do
              {:ok, float_val} ->
                new_val = float_val + delta
                new_str = format_float(new_val)
                :ets.insert(state.ets, {key, new_str, expire_at_ms})
                new_pending = [{key, new_str, expire_at_ms} | state.pending]
                new_state = %{state | pending: new_pending}

                new_state =
                  if state.flush_in_flight == nil,
                    do: flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_str}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not a valid float"}, state}
            end
        end
    end
  end

  # Atomic append: reads current value (or ""), appends suffix, writes back.
  # Returns {:ok, new_byte_length}.
  def handle_call({:append, key, suffix}, _from, state) do
    case ets_lookup(state.ets, key) do
      {:hit, value, expire_at_ms} ->
        new_val = value <> suffix
        :ets.insert(state.ets, {key, new_val, expire_at_ms})
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}

      :expired ->
        :ets.insert(state.ets, {key, suffix, 0})
        new_pending = [{key, suffix, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(suffix)}, new_state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        {old_val, expire_at_ms} =
          case do_get_meta(state, key) do
            {v, exp} -> {v, exp}
            nil -> {"", 0}
          end

        new_val = old_val <> suffix
        :ets.insert(state.ets, {key, new_val, expire_at_ms})
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}
    end
  end

  # Atomic get-and-set: returns old value (or nil), sets new value.
  def handle_call({:getset, key, new_value}, _from, state) do
    old =
      case ets_lookup(state.ets, key) do
        {:hit, value, _expire_at_ms} -> value
        :expired -> nil
        :miss ->
          state = await_in_flight(state)
          state = flush_pending_sync(state)
          do_get(state, key)
      end

    :ets.insert(state.ets, {key, new_value, 0})
    new_pending = [{key, new_value, 0} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: flush_pending(new_state),
        else: new_state

    {:reply, old, new_state}
  end

  # Atomic get-and-delete: returns value (or nil), deletes key.
  def handle_call({:getdel, key}, _from, state) do
    old =
      case ets_lookup(state.ets, key) do
        {:hit, value, _expire_at_ms} -> value
        :expired -> nil
        :miss ->
          state = await_in_flight(state)
          state = flush_pending_sync(state)
          do_get(state, key)
      end

    if old != nil do
      # Synchronous delete for durability
      state = await_in_flight(state)
      state = flush_pending_sync(state)
      NIF.delete(state.store, key)
      :ets.delete(state.ets, key)
      new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
      {:reply, old, %{state | pending: new_pending}}
    else
      {:reply, nil, state}
    end
  end

  # Atomic get-and-update-expiry: returns value, updates TTL.
  # expire_at_ms = 0 means PERSIST (remove expiry).
  def handle_call({:getex, key, expire_at_ms}, _from, state) do
    case ets_lookup(state.ets, key) do
      {:hit, value, _old_exp} ->
        :ets.insert(state.ets, {key, value, expire_at_ms})
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, value, new_state}

      :expired ->
        {:reply, nil, state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        case do_get(state, key) do
          nil ->
            {:reply, nil, state}

          value ->
            :ets.insert(state.ets, {key, value, expire_at_ms})
            new_pending = [{key, value, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, value, new_state}
        end
    end
  end

  # Atomic set-range: overwrites portion of string at offset with value.
  # Zero-pads if key doesn't exist or string is shorter than offset.
  # Returns {:ok, new_byte_length}.
  def handle_call({:setrange, key, offset, value}, _from, state) do
    {old_val, expire_at_ms} =
      case ets_lookup(state.ets, key) do
        {:hit, v, exp} -> {v, exp}
        :expired -> {"", 0}
        :miss ->
          state = await_in_flight(state)
          state = flush_pending_sync(state)

          case do_get_meta(state, key) do
            {v, exp} -> {v, exp}
            nil -> {"", 0}
          end
      end

    new_val = apply_setrange(old_val, offset, value)
    :ets.insert(state.ets, {key, new_val, expire_at_ms})
    new_pending = [{key, new_val, expire_at_ms} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: flush_pending(new_state),
        else: new_state

    {:reply, {:ok, byte_size(new_val)}, new_state}
  end

  def handle_call({:delete, key}, _from, state) do
    if raft_enabled?() do
      # Raft path: route through Batcher -> ra -> StateMachine.apply
      # StateMachine.apply writes tombstone to Bitcask and removes from ETS.
      alias Ferricstore.Raft.Batcher
      result = Batcher.write(state.index, {:delete, key})
      new_version = state.write_version + 1
      {:reply, result, %{state | write_version: new_version}}
    else
      # Direct path: delete is always synchronous — tombstones must be durable
      # immediately so a crash doesn't resurrect the key.
      #
      # 1. Wait for any in-flight async flush to complete.
      # 2. Flush remaining pending writes synchronously.
      # 3. Write the tombstone.
      # 4. Remove the deleted key from pending to prevent resurrection.
      state = await_in_flight(state)
      state = flush_pending_sync(state)
      NIF.delete(state.store, key)
      :ets.delete(state.ets, key)
      # Remove any pending entry for this key (belt-and-suspenders: flush above
      # already cleared pending, but be explicit).
      new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
      new_version = state.write_version + 1
      {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}
    end
  end

  # Returns the current write_version for WATCH support.
  def handle_call({:get_version, _key}, _from, state) do
    {:reply, state.write_version, state}
  end

  # --- Native commands: CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD ---

  def handle_call({:cas, key, expected, new_value, ttl_ms}, _from, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^expected, old_exp}, state} ->
        expire = if ttl_ms, do: System.os_time(:millisecond) + ttl_ms, else: old_exp
        :ets.insert(state.ets, {key, new_value, expire})
        new_pending = [{key, new_value, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} -> {:reply, 0, state}
      {:expired, state} -> {:reply, nil, state}
      {:missing, state} -> {:reply, nil, state}
    end
  end

  def handle_call({:lock, key, owner, ttl_ms}, _from, state) do
    expire = System.os_time(:millisecond) + ttl_ms
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        :ets.insert(state.ets, {key, owner, expire})
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, :ok, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK lock is held by another owner"}, state}

      {_, state} ->
        :ets.insert(state.ets, {key, owner, expire})
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:unlock, key, owner}, _from, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        NIF.delete(state.store, key)
        :ets.delete(state.ets, key)
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {_, state} -> {:reply, 1, state}
    end
  end

  def handle_call({:extend, key, owner, ttl_ms}, _from, state) do
    new_expire = System.os_time(:millisecond) + ttl_ms
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        :ets.insert(state.ets, {key, owner, new_expire})
        new_pending = [{key, owner, new_expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {_, state} ->
        {:reply, {:error, "DISTLOCK lock does not exist or has expired"}, state}
    end
  end

  def handle_call({:ratelimit_add, key, window_ms, max, count}, _from, state) do
    now = System.os_time(:millisecond)

    {cur_count, cur_start, prv_count} =
      case ets_lookup(state.ets, key) do
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
        :ets.insert(state.ets, {key, value, expire_at_ms})
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {"denied", effective, max(0, max - effective), new_state}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        :ets.insert(state.ets, {key, value, expire_at_ms})
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {"allowed", new_eff, max(0, max - new_eff), new_state}
      end

    ms_until_reset = max(0, cur_start + window_ms - now)
    {:reply, [status, final_count, remaining, ms_until_reset], state}
  end

  # ---------------------------------------------------------------------------
  # List operations
  # ---------------------------------------------------------------------------

  def handle_call({:list_op, key, operation}, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)

    alias Ferricstore.Store.ListOps

    get_fn = fn -> do_get(state, key) end
    put_fn = fn encoded_binary ->
      :ets.insert(state.ets, {key, encoded_binary, 0})
      NIF.put(state.store, key, encoded_binary, 0)
      :ok
    end
    delete_fn = fn ->
      NIF.delete(state.store, key)
      :ets.delete(state.ets, key)
      :ok
    end

    result = ListOps.execute(get_fn, put_fn, delete_fn, operation)
    {:reply, result, state}
  end

  def handle_call({:list_op_lmove, src_key, dst_key, from_dir, to_dir}, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)

    alias Ferricstore.Store.ListOps

    src_get_fn = fn -> do_get(state, src_key) end
    dst_get_fn = fn -> do_get(state, dst_key) end
    src_put_fn = fn encoded ->
      :ets.insert(state.ets, {src_key, encoded, 0})
      NIF.put(state.store, src_key, encoded, 0)
      :ok
    end
    dst_put_fn = fn encoded ->
      :ets.insert(state.ets, {dst_key, encoded, 0})
      NIF.put(state.store, dst_key, encoded, 0)
      :ok
    end
    src_delete_fn = fn ->
      NIF.delete(state.store, src_key)
      :ets.delete(state.ets, src_key)
      :ok
    end

    result = ListOps.execute_lmove(src_get_fn, src_put_fn, src_delete_fn, dst_get_fn, dst_put_fn, from_dir, to_dir)
    {:reply, result, state}
  end

  def handle_call({:exists, key}, _from, state) do
    # For ETS misses we need Bitcask to be up to date — flush first.
    case ets_lookup(state.ets, key) do
      {:hit, _value, _expire_at_ms} ->
        {:reply, true, state}

      :expired ->
        {:reply, false, state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        {:reply, do_get(state, key) != nil, state}
    end
  end

  def handle_call(:keys, _from, state) do
    # Flush first so NIF.keys() sees all pending writes.
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, live_keys(state), state}
  end

  # Merge-related calls: delegate to NIF and return results directly.

  def handle_call(:shard_stats, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, NIF.shard_stats(state.store), state}
  end

  def handle_call(:file_sizes, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, NIF.file_sizes(state.store), state}
  end

  def handle_call({:run_compaction, file_ids}, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, NIF.run_compaction(state.store, file_ids), state}
  end

  def handle_call(:available_disk_space, _from, state) do
    {:reply, NIF.available_disk_space(state.store), state}
  end

  # Synchronous flush — used by tests and by delete to ensure durability.
  def handle_call(:flush, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_pending(state)
    schedule_flush(Process.get(:flush_interval_ms, @flush_interval_ms))
    {:noreply, state}
  end

  # Synchronous expiry sweep — used by tests to trigger a sweep and wait for
  # completion before making assertions.
  def handle_call(:expiry_sweep, _from, state) do
    state = do_expiry_sweep(state)
    {:reply, :ok, state}
  end

  # Active expiry sweep: scan ETS for expired keys and delete them.
  def handle_info(:expiry_sweep, state) do
    state = do_expiry_sweep(state)
    schedule_expiry_sweep()
    {:noreply, state}
  end

  # Handle async io_uring completion message from the NIF background thread.
  def handle_info({:io_complete, op_id, result}, state) do
    if state.flush_in_flight == op_id do
      case result do
        :ok ->
          {:noreply, %{state | flush_in_flight: nil}}

        {:error, reason} ->
          # The async flush failed. Log the error but clear in-flight so
          # the next timer tick can attempt another flush. The keydir was
          # updated optimistically by prepare_batch_for_async — on the next
          # store open, log replay will reconcile.
          Logger.error(
            "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
          )

          {:noreply, %{state | flush_in_flight: nil}}
      end
    else
      # Stale or unknown op_id — ignore.
      {:noreply, state}
    end
  end

  # -------------------------------------------------------------------
  # Graceful shutdown (spec 2C.6, step 8)
  #
  # OTP calls terminate/2 when the supervisor stops this child during
  # application shutdown (children are stopped in reverse start order).
  # We flush pending writes, write the Bitcask hint file, and emit
  # telemetry so operators can observe shutdown timing.
  # -------------------------------------------------------------------

  @impl true
  def terminate(_reason, state) do
    t0 = System.monotonic_time(:microsecond)

    # Step 1: drain any in-flight async flush and flush remaining pending
    # writes synchronously to guarantee all data hits disk before exit.
    state = await_in_flight(state)
    state = flush_pending_sync(state)

    t_flush = System.monotonic_time(:microsecond)

    # Step 2: write the Bitcask hint file so the next startup can rebuild
    # the keydir from hints (seconds) instead of replaying the full log
    # (minutes for large datasets).
    hint_result = NIF.write_hint(state.store)

    case hint_result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{state.index}: failed to write hint file during shutdown: #{inspect(reason)}"
        )
    end

    t_hint = System.monotonic_time(:microsecond)

    # Step 3: emit shutdown telemetry for operator visibility.
    :telemetry.execute(
      [:ferricstore, :shard, :shutdown],
      %{
        flush_duration_us: t_flush - t0,
        hint_duration_us: t_hint - t_flush,
        total_duration_us: t_hint - t0
      },
      %{shard_index: state.index}
    )

    Logger.info(
      "Shard #{state.index}: shutdown complete " <>
        "(flush=#{t_flush - t0}us, hint=#{t_hint - t_flush}us)"
    )

    :ok
  end

  # -------------------------------------------------------------------
  # Private: flush
  # -------------------------------------------------------------------

  # Async flush — used by the timer and by put (first-write-in-window).
  # If a flush is already in-flight or pending is empty, this is a no-op.
  defp flush_pending(%{pending: []} = state), do: state
  defp flush_pending(%{flush_in_flight: op_id} = state) when op_id != nil, do: state

  defp flush_pending(%{pending: pending, store: store} = state) do
    # Reverse to preserve insertion order (list was prepended).
    batch =
      pending
      |> Enum.reverse()
      |> Enum.map(fn {k, v, exp} -> {k, v, exp} end)

    case NIF.put_batch_async(store, batch) do
      {:pending, op_id} ->
        # Async submission succeeded — clear pending, track in-flight.
        %{state | pending: [], flush_in_flight: op_id}

      :ok ->
        # Sync fallback (macOS / no io_uring) — completed immediately.
        %{state | pending: []}

      {:error, _reason} ->
        # On error keep the pending list so writes are not lost — the timer
        # will retry on the next tick.
        state
    end
  end

  # Synchronous flush — used by delete, :flush, and :keys calls that need
  # durability guarantees. Uses the sync put_batch path. The caller must
  # first call `await_in_flight/1` to ensure no async op is in-flight.
  defp flush_pending_sync(%{pending: []} = state), do: state

  defp flush_pending_sync(%{pending: pending, store: store} = state) do
    batch =
      pending
      |> Enum.reverse()
      |> Enum.map(fn {k, v, exp} -> {k, v, exp} end)

    case NIF.put_batch(store, batch) do
      :ok ->
        %{state | pending: []}

      {:error, _reason} ->
        state
    end
  end

  # Block until any in-flight async flush completes. This is only called
  # from synchronous GenServer callbacks (delete, keys, flush) that need
  # durability before proceeding.
  defp await_in_flight(%{flush_in_flight: nil} = state), do: state

  defp await_in_flight(%{flush_in_flight: op_id} = state) do
    receive do
      {:io_complete, ^op_id, :ok} ->
        %{state | flush_in_flight: nil}

      {:io_complete, ^op_id, {:error, reason}} ->
        Logger.error(
          "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
        )

        %{state | flush_in_flight: nil}
    after
      @sync_flush_timeout_ms ->
        Logger.error(
          "Shard #{state.index}: timed out waiting for async flush op #{op_id}"
        )

        # Clear in-flight to unblock the caller. The async op may still
        # complete later — its message will be ignored (unknown op_id).
        %{state | flush_in_flight: nil}
    end
  end

  defp schedule_flush(ms) do
    Process.send_after(self(), :flush, ms)
  end

  # -------------------------------------------------------------------
  # Private: read helpers
  # -------------------------------------------------------------------

  defp do_get(state, key) do
    case ets_lookup(state.ets, key) do
      {:hit, value, _expire_at_ms} -> value
      :expired -> nil
      :miss -> warm_from_store(state, key)
    end
  end

  defp do_get_meta(state, key) do
    case ets_lookup(state.ets, key) do
      {:hit, value, expire_at_ms} -> {value, expire_at_ms}
      :expired -> nil
      :miss -> warm_meta_from_store(state, key)
    end
  end

  # Classifies an ETS lookup as a cache hit, expired entry, or miss.
  # Expired entries are evicted immediately.
  defp ets_lookup(ets, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(ets, key) do
      [{^key, value, 0}] ->
        {:hit, value, 0}

      [{^key, value, exp}] when exp > now ->
        {:hit, value, exp}

      [{^key, _value, _exp}] ->
        :ets.delete(ets, key)
        :expired

      [] ->
        :miss
    end
  end

  defp warm_from_store(state, key) do
    case NIF.get(state.store, key) do
      {:ok, nil} ->
        nil

      {:ok, value} ->
        :ets.insert(state.ets, {key, value, 0})
        value

      _error ->
        nil
    end
  end

  defp warm_meta_from_store(state, key) do
    case NIF.get(state.store, key) do
      {:ok, nil} ->
        nil

      {:ok, value} ->
        :ets.insert(state.ets, {key, value, 0})
        {value, 0}

      _error ->
        nil
    end
  end

  defp live_keys(state) do
    now = System.os_time(:millisecond)

    state.store
    |> NIF.keys()
    |> Enum.filter(fn key -> key_alive?(state.ets, key, now) end)
  end

  defp key_alive?(ets, key, now) do
    case :ets.lookup(ets, key) do
      [{_, _, 0}] -> true
      [{_, _, exp}] -> exp > now
      [] -> true
    end
  end

  # -------------------------------------------------------------------
  # Private: integer / float parsing
  # -------------------------------------------------------------------

  defp parse_integer(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} -> {:ok, val}
      _ -> :error
    end
  end

  defp parse_float(str) when is_binary(str) do
    # Try integer first (Redis considers "10" valid for INCRBYFLOAT)
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

  defp format_float(val) when is_float(val) do
    # Redis formats floats with up to 17 significant digits, removing trailing zeros.
    # We use Erlang's ~.17g format and strip trailing zeros after decimal point.
    formatted = :erlang.float_to_binary(val, [:compact, {:decimals, 17}])

    # If result has a decimal point, strip trailing zeros but keep at least one digit after dot
    if String.contains?(formatted, ".") do
      formatted
      |> String.trim_trailing("0")
      |> String.trim_trailing(".")
      |> then(fn
        # If we trimmed everything after dot, re-check: Redis doesn't strip the decimal point
        # for values that had fractional parts. But for integer-like floats, Redis drops it.
        s -> s
      end)
    else
      formatted
    end
  end

  # Applies SETRANGE logic: overwrites bytes at `offset` with `value`,
  # zero-padding if the original string is shorter than offset.
  defp apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        # Empty value -- just pad up to offset if needed
        if offset > old_len do
          old <> :binary.copy(<<0>>, offset - old_len)
        else
          old
        end

      offset >= old_len ->
        # Need to pad between end of old and start of overwrite
        padding = :binary.copy(<<0>>, offset - old_len)
        old <> padding <> value

      offset + val_len >= old_len ->
        # Overwrite extends past end of old string
        binary_part(old, 0, offset) <> value

      true ->
        # Overwrite in the middle of the string
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  # -------------------------------------------------------------------
  # Private: active expiry sweep
  # -------------------------------------------------------------------

  # Number of consecutive ceiling-hit sweeps before emitting the
  # :expiry_struggling telemetry event.
  @struggling_threshold 3

  # Performs a single expiry sweep pass: scans ETS for up to `max_keys`
  # expired entries, deletes them from ETS, and purges expired entries
  # from the Bitcask store. Tracks consecutive ceiling-hit sweeps and
  # emits telemetry when the sweep is struggling or recovers.
  defp do_expiry_sweep(state) do
    now = System.os_time(:millisecond)
    max_keys = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep, @default_max_keys_per_sweep)
    expired_keys = scan_expired(state.ets, now, max_keys)

    count = length(expired_keys)

    if count > 0 do
      Enum.each(expired_keys, fn key -> :ets.delete(state.ets, key) end)
      NIF.purge_expired(state.store)

      require Logger
      Logger.debug("Shard #{state.index}: expiry sweep removed #{count} key(s)")
    end

    # Track whether the sweep hit the ceiling (removed exactly max_keys).
    hit_ceiling = count >= max_keys and count > 0

    {new_ceiling_count, new_struggling} =
      if hit_ceiling do
        new_count = state.sweep_at_ceiling_count + 1

        if new_count >= @struggling_threshold and not state.sweep_struggling do
          :telemetry.execute(
            [:ferricstore, :expiry, :struggling],
            %{shard_index: state.index, consecutive_ceiling_sweeps: new_count, max_keys_per_sweep: max_keys},
            %{}
          )

          {new_count, true}
        else
          {new_count, state.sweep_struggling}
        end
      else
        if state.sweep_struggling do
          :telemetry.execute(
            [:ferricstore, :expiry, :recovered],
            %{shard_index: state.index, previous_ceiling_sweeps: state.sweep_at_ceiling_count},
            %{}
          )
        end

        {0, false}
      end

    %{state | sweep_at_ceiling_count: new_ceiling_count, sweep_struggling: new_struggling}
  end

  defp scan_expired(ets, now, limit) do
    match_spec = [
      {{:"$1", :_, :"$3"},
       [{:andalso, {:>, :"$3", 0}, {:"=<", :"$3", now}}],
       [:"$1"]}
    ]

    case :ets.select(ets, match_spec, limit) do
      {keys, _continuation} -> keys
      :"$end_of_table" -> []
    end
  end

  defp schedule_expiry_sweep do
    interval = Application.get_env(:ferricstore, :expiry_sweep_interval_ms, @default_sweep_interval_ms)
    Process.send_after(self(), :expiry_sweep, interval)
  end

  # --- Native command helpers ---

  defp resolve_for_native(state, key) do
    case ets_lookup(state.ets, key) do
      {:hit, value, exp} -> {{:hit, value, exp}, state}
      :expired -> {:expired, state}
      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        case do_get_meta(state, key) do
          nil -> {:missing, state}
          {value, exp} -> {{:hit, value, exp}, state}
        end
    end
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

  # -------------------------------------------------------------------
  # Private: collection promotion helpers
  # -------------------------------------------------------------------

  # Returns the dedicated NIF store ref for a promoted key, or nil.
  defp promoted_store(state, redis_key) do
    Map.get(state.promoted_instances, redis_key)
  end

  # After a compound_put to the shared Bitcask, checks whether the
  # collection should be promoted. Triggers for hash (H:), set (S:),
  # and sorted set (Z:) compound keys when the entry count exceeds the
  # threshold.
  #
  # Lists are NOT promoted because they store all elements in a single
  # Bitcask entry (serialized Erlang term) rather than compound keys.
  # A list with 1000 elements is still one Bitcask entry, so promotion
  # would provide no benefit.
  defp maybe_promote(state, redis_key, compound_key) do
    alias Ferricstore.Store.{CompoundKey, Promotion}

    threshold = Promotion.threshold()

    # Disabled if threshold is 0, or already promoted
    if threshold == 0 or Map.has_key?(state.promoted_instances, redis_key) do
      state
    else
      # Detect the collection type from the compound key prefix
      case detect_compound_type(redis_key, compound_key) do
        nil ->
          # Not a promotable compound key (e.g. list, type metadata)
          state

        {type, prefix} ->
          # Count entries for this collection
          now = System.os_time(:millisecond)

          count =
            :ets.foldl(
              fn {key, _value, exp}, acc ->
                if is_binary(key) and String.starts_with?(key, prefix) and
                     (exp == 0 or exp > now) do
                  acc + 1
                else
                  acc
                end
              end,
              0,
              state.ets
            )

          if count > threshold do
            # Flush pending writes so the shared Bitcask has all data
            state = await_in_flight(state)
            state = flush_pending_sync(state)

            case Promotion.promote_collection!(
                   type,
                   redis_key,
                   state.store,
                   state.ets,
                   state.data_dir,
                   state.index
                 ) do
              {:ok, dedicated_store} ->
                new_promoted = Map.put(state.promoted_instances, redis_key, dedicated_store)
                %{state | promoted_instances: new_promoted}

              {:error, _reason} ->
                # Promotion failed -- continue using shared Bitcask
                state
            end
          else
            state
          end
      end
    end
  end

  # Detects the compound key type from its prefix and returns
  # `{type_atom, scan_prefix}` or `nil` if not a promotable type.
  defp detect_compound_type(redis_key, compound_key) do
    alias Ferricstore.Store.CompoundKey

    cond do
      String.starts_with?(compound_key, CompoundKey.hash_prefix(redis_key)) ->
        {:hash, CompoundKey.hash_prefix(redis_key)}

      String.starts_with?(compound_key, CompoundKey.set_prefix(redis_key)) ->
        {:set, CompoundKey.set_prefix(redis_key)}

      String.starts_with?(compound_key, CompoundKey.zset_prefix(redis_key)) ->
        {:zset, CompoundKey.zset_prefix(redis_key)}

      true ->
        nil
    end
  end

  # Returns whether Raft consensus is enabled for write durability.
  # Reads the application env at runtime so tests can disable Raft without
  # recompiling.
  defp raft_enabled? do
    Application.get_env(:ferricstore, :raft_enabled, true)
  end
end
