defmodule Ferricstore.Store.Shard.Transaction do
  @moduledoc false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites

  # -------------------------------------------------------------------
  # Transaction execution handler
  # -------------------------------------------------------------------

  @doc false
  def handle_tx_execute(queue, sandbox_namespace, state) do
    Process.put(:tx_deleted_keys, MapSet.new())
    store = build_local_store(state)

    results =
      try do
        Enum.map(queue, fn {cmd, args} ->
          namespaced_args = namespace_args(args, sandbox_namespace)

          try do
            Ferricstore.Commands.Dispatcher.dispatch(cmd, namespaced_args, store)
          catch
            :exit, {:noproc, _} ->
              {:error, "ERR server not ready, shard process unavailable"}

            :exit, {reason, _} ->
              {:error, "ERR internal error: #{inspect(reason)}"}
          end
        end)
      after
        Process.delete(:tx_deleted_keys)
      end

    {:reply, results, state}
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @doc false
  def namespace_args(args, nil), do: args
  def namespace_args([], _ns), do: []
  def namespace_args([key | rest], ns) when is_binary(key), do: [ns <> key | rest]
  def namespace_args(args, _ns), do: args

  @doc false
  def build_local_store(state) do
    my_idx = state.index
    instance_ctx = state.instance_ctx
    # Build a minimal context with only the fields closures need, to avoid
    # capturing the entire state struct (which includes pending list,
    # promoted_instances, etc. that hold stale references).
    ctx = %{
      keydir: state.keydir,
      index: state.index,
      shard_data_path: state.shard_data_path,
      data_dir: state.data_dir
    }

    # Direct put: write to ETS immediately, queue for async Bitcask flush.
    # This mirrors the non-raft {:put, ...} handler logic.
    local_put = fn key, value, expire_at_ms ->
      is_local = Router.shard_for(instance_ctx, key) == my_idx

      if is_local do
        ShardETS.ets_insert(ctx, key, value, expire_at_ms)
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
        end

        send(self(), {:tx_pending_write, key, value, expire_at_ms})
        :ok
      else
        Router.put(instance_ctx, key, value, expire_at_ms)
      end
    end

    local_delete = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        ShardETS.ets_delete_key(state, key)
        # Track deletion so subsequent reads within this tx see the key as gone
        deleted = Process.get(:tx_deleted_keys, MapSet.new())
        Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
        send(self(), {:tx_pending_delete, key})
        :ok
      else
        Router.delete(instance_ctx, key)
      end
    end

    local_get = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        # Check if key was deleted within this transaction
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          nil
        else
          case ShardETS.ets_lookup_warm(state, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              # Read directly from Bitcask to avoid GenServer.call deadlock
              case ShardReads.v2_local_read(state, key) do
                {:ok, nil} -> nil
                {:ok, value} ->
                  ShardETS.ets_insert(state, key, value, 0)
                  value
                _error -> nil
              end
          end
        end
      else
        Router.get(instance_ctx, key)
      end
    end

    local_get_meta = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          nil
        else
          case ShardETS.ets_lookup_warm(state, key) do
            {:hit, value, exp} -> {value, exp}
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(state, key) do
                {:ok, nil} -> nil
                {:ok, value} ->
                  ShardETS.ets_insert(state, key, value, 0)
                  {value, 0}
                _error -> nil
              end
          end
        end
      else
        Router.get_meta(instance_ctx, key)
      end
    end

    local_exists = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          false
        else
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, _, _} -> true
            :expired -> false
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> false
                {:ok, _value} -> true
                _error -> false
              end
          end
        end
      else
        Router.exists?(instance_ctx, key)
      end
    end

    local_incr = fn key, delta ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        current =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        case current do
          nil ->
            ShardETS.ets_insert(ctx, key, delta, 0)
            send(self(), {:tx_pending_write, key, delta, 0})
            {:ok, delta}

          value ->
            case ShardETS.coerce_integer(value) do
              {:ok, int_val} ->
                new_val = int_val + delta
                ShardETS.ets_insert(ctx, key, new_val, 0)
                send(self(), {:tx_pending_write, key, new_val, 0})
                {:ok, new_val}

              :error ->
                {:error, "ERR value is not an integer or out of range"}
            end
        end
      else
        Router.incr(instance_ctx, key, delta)
      end
    end

    local_incr_float = fn key, delta ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        current =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        case current do
          nil ->
            new_val = delta * 1.0
            ShardETS.ets_insert(ctx, key, new_val, 0)
            send(self(), {:tx_pending_write, key, new_val, 0})
            {:ok, new_val}

          value ->
            case ShardETS.coerce_float(value) do
              {:ok, float_val} ->
                new_val = float_val + delta
                ShardETS.ets_insert(ctx, key, new_val, 0)
                send(self(), {:tx_pending_write, key, new_val, 0})
                {:ok, new_val}

              :error ->
                {:error, "ERR value is not a valid float"}
            end
        end
      else
        Router.incr_float(instance_ctx, key, delta)
      end
    end

    local_append = fn key, suffix ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        current =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> ShardETS.to_disk_binary(value)
            :expired -> ""
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> ""
                {:ok, v} -> v
                _ -> ""
              end
          end

        new_val = current <> suffix
        ShardETS.ets_insert(ctx, key, new_val, 0)
        send(self(), {:tx_pending_write, key, new_val, 0})
        {:ok, byte_size(new_val)}
      else
        Router.append(instance_ctx, key, suffix)
      end
    end

    local_getset = fn key, new_value ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        old =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        ShardETS.ets_insert(ctx, key, new_value, 0)
        send(self(), {:tx_pending_write, key, new_value, 0})
        old
      else
        Router.getset(instance_ctx, key, new_value)
      end
    end

    local_getdel = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        old =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        if old do
          ShardETS.ets_delete_key(ctx, key)
          send(self(), {:tx_pending_delete, key})
        end

        old
      else
        Router.getdel(instance_ctx, key)
      end
    end

    local_getex = fn key, expire_at_ms ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        value =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, v, _exp} -> v
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        if value do
          ShardETS.ets_insert(ctx, key, value, expire_at_ms)
          send(self(), {:tx_pending_write, key, value, expire_at_ms})
        end

        value
      else
        Router.getex(instance_ctx, key, expire_at_ms)
      end
    end

    local_setrange = fn key, offset, value ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        old =
          case ShardETS.ets_lookup_warm(ctx, key) do
            {:hit, v, _exp} -> ShardETS.to_disk_binary(v)
            :expired -> ""
            :miss ->
              case ShardReads.v2_local_read(ctx, key) do
                {:ok, nil} -> ""
                {:ok, v} -> v
                _ -> ""
              end
          end

        new_val = ShardWrites.apply_setrange(old, offset, value)
        ShardETS.ets_insert(ctx, key, new_val, 0)
        send(self(), {:tx_pending_write, key, new_val, 0})
        {:ok, byte_size(new_val)}
      else
        Router.setrange(instance_ctx, key, offset, value)
      end
    end

    %{
      get: local_get,
      get_meta: local_get_meta,
      put: local_put,
      delete: local_delete,
      exists?: local_exists,
      keys: fn -> Router.keys(instance_ctx) end,
      flush: fn ->
        Enum.each(Router.keys(instance_ctx), fn k -> Router.delete(instance_ctx, k) end)
        :ok
      end,
      dbsize: fn -> Router.dbsize(instance_ctx) end,
      incr: local_incr,
      incr_float: local_incr_float,
      append: local_append,
      getset: local_getset,
      getdel: local_getdel,
      getex: local_getex,
      setrange: local_setrange,
      cas: fn key, expected, new_value, ttl_ms -> Router.cas(instance_ctx, key, expected, new_value, ttl_ms) end,
      lock: fn key, owner, ttl_ms -> Router.lock(instance_ctx, key, owner, ttl_ms) end,
      unlock: fn key, owner -> Router.unlock(instance_ctx, key, owner) end,
      extend: fn key, owner, ttl_ms -> Router.extend(instance_ctx, key, owner, ttl_ms) end,
      ratelimit_add: fn key, window_ms, max, count -> Router.ratelimit_add(instance_ctx, key, window_ms, max, count) end,
      list_op: fn key, operation -> Router.list_op(instance_ctx, key, operation) end,
      compound_get: fn redis_key, compound_key ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Local: read compound key directly from ETS
          case ShardETS.ets_lookup_warm(ctx, compound_key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, compound_key) do
                {:ok, nil} -> nil
                {:ok, v} ->
                  ShardETS.ets_insert(ctx, compound_key, v, 0)
                  v
                _ -> nil
              end
          end
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_get, redis_key, compound_key})
        end
      end,
      compound_get_meta: fn redis_key, compound_key ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          case ShardETS.ets_lookup_warm(ctx, compound_key) do
            {:hit, value, exp} -> {value, exp}
            :expired -> nil
            :miss ->
              case ShardReads.v2_local_read(ctx, compound_key) do
                {:ok, nil} -> nil
                {:ok, v} ->
                  ShardETS.ets_insert(ctx, compound_key, v, 0)
                  {v, 0}
                _ -> nil
              end
          end
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
        end
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          ShardETS.ets_insert(ctx, compound_key, value, expire_at_ms)
          send(self(), {:tx_pending_write, compound_key, value, expire_at_ms})
          :ok
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
        end
      end,
      compound_delete: fn redis_key, compound_key ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          ShardETS.ets_delete_key(ctx, compound_key)
          send(self(), {:tx_pending_delete, compound_key})
          :ok
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_delete, redis_key, compound_key})
        end
      end,
      compound_scan: fn redis_key, prefix ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          # Pass shard_data_path to enable cold-read for recovered keys
          results = ShardETS.prefix_scan_entries(ctx.keydir, prefix, ctx.shard_data_path)
          Enum.sort_by(results, fn {field, _} -> field end)
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_scan, redis_key, prefix})
        end
      end,
      compound_count: fn redis_key, prefix ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          ShardETS.prefix_count_entries(ctx.keydir, prefix)
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_count, redis_key, prefix})
        end
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          keys_to_delete = ShardETS.prefix_collect_keys(ctx.keydir, prefix)

          Enum.each(keys_to_delete, fn key ->
            ShardETS.ets_delete_key(ctx, key)
            send(self(), {:tx_pending_delete, key})
          end)

          :ok
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
        end
      end,
      prob_dir: fn ->
        Path.join(ctx.shard_data_path, "prob")
      end,
      prob_write: fn command ->
        Router.prob_write(instance_ctx, command)
      end,
      shard_index: ctx.index,
      data_dir: ctx.data_dir,
      on_push: &Ferricstore.Waiters.notify_push/1
    }
  end
end
