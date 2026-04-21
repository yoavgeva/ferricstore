defmodule FerricstoreServer.Connection.Pipeline do
  @moduledoc "Sliding-window pipeline dispatcher that groups pure commands by shard and executes them concurrently."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.TcpOpts
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  # Commands that must be executed synchronously because they read or mutate
  # connection-level state (transaction mode, pub/sub subscriptions, auth,
  # sandbox, blocking ops, etc.).
  @stateful_cmds ~w(
    HELLO CLIENT QUIT AUTH ACL RESET SANDBOX
    MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    BLPOP BRPOP BLMOVE BLMPOP
  )

  # Commands that ALWAYS span multiple shards regardless of arg count.
  @always_multi_cmds ~w(MGET MSET MSETNX BITOP PFCOUNT PFMERGE
    SDIFF SINTER SUNION SDIFFSTORE SINTERSTORE SUNIONSTORE SINTERCARD)

  # Commands that take a variable number of keys.
  @variadic_key_cmds ~w(DEL UNLINK EXISTS)

  # Server-level commands that span all shards and must act as barriers.
  @barrier_server_cmds ~w(DBSIZE FLUSHDB FLUSHALL KEYS SCAN RANDOMKEY)

  # Server-level commands that don't target a specific key.
  @server_cmds_no_key ~w(PING ECHO DBSIZE FLUSHDB FLUSHALL KEYS INFO COMMAND
    SELECT LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE CONFIG MODULE WAITAOF
    MEMORY RANDOMKEY SCAN OBJECT WAIT
    CLUSTER.HEALTH CLUSTER.STATS FERRICSTORE.HOTNESS FERRICSTORE.METRICS)

  # Commands that read keys (O(1) MapSet lookup for hot-path read classification).
  @read_cmds ~w(GET MGET GETRANGE STRLEN GETEX GETDEL GETSET
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN
    LRANGE LLEN LINDEX LPOS
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER
    ZSCORE ZRANK ZREVRANK ZRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE
    TYPE EXISTS TTL PTTL EXPIRETIME PEXPIRETIME
    GETBIT BITCOUNT BITPOS PFCOUNT
    OBJECT SUBSTR
    GEOHASH GEOPOS GEODIST GEOSEARCH
    XLEN XRANGE XREVRANGE XREAD XINFO
    JSON.GET JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRLEN JSON.MGET)

  @read_cmds_set MapSet.new(@read_cmds)

  # Maximum commands in a single pipeline batch (100K).
  @max_pipeline_size 100_000

  @doc """
  Returns the maximum pipeline batch size.
  """
  @spec max_pipeline_size() :: pos_integer()
  def max_pipeline_size, do: @max_pipeline_size

  # ---------------------------------------------------------------------------
  # Pipeline dispatch entry point
  # ---------------------------------------------------------------------------

  @doc """
  Dispatches a pipeline of commands using a sliding window.

  For a single command, falls through to sequential dispatch (no benefit from
  async). For multiple commands, groups consecutive "pure" commands and
  dispatches them concurrently as Tasks.

  `handle_command_fn` is `fn cmd, state -> {:quit | :continue, response, state}`.
  `send_response_fn` is `fn socket, transport, iodata -> :ok`.
  """
  @spec pipeline_dispatch(
          commands :: [term()],
          state :: struct(),
          handle_command_fn :: (term(), struct() -> {atom(), iodata(), struct()}),
          send_response_fn :: (term(), term(), iodata() -> :ok)
        ) :: {:quit, struct()} | {:continue, struct()}
  def pipeline_dispatch([single_cmd], state, handle_command_fn, send_response_fn) do
    case handle_command_fn.(single_cmd, state) do
      {:quit, response, quit_state} ->
        send_response_fn.(state.socket, state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response_fn.(state.socket, state.transport, response)
        {:continue, new_state}
    end
  end

  def pipeline_dispatch(commands, state, handle_command_fn, send_response_fn) do
    case try_batch_set_fast_path(commands, state, send_response_fn) do
      {:ok, result} -> result
      :fallback -> sliding_window_dispatch(commands, state, handle_command_fn, send_response_fn)
    end
  end

  # ---------------------------------------------------------------------------
  # Batch SET fast path
  # ---------------------------------------------------------------------------
  # When ALL commands in a pipeline batch are plain `SET key value` (no options),
  # bypass the normal sliding_window_dispatch which serializes each SET.
  #
  # Async: batch ETS inserts, reply OK immediately.
  # Quorum: submit all to Batcher(s) concurrently, wait for all Ra commits,
  #         then send all responses. This lets all SETs share the same Batcher
  #         batch + WAL fdatasync instead of serializing through it.

  defp try_batch_set_fast_path(commands, state, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing do
      :fallback
    else
      case extract_plain_sets(commands) do
        {:ok, kv_pairs} ->
          ctx = state.instance_ctx
          live_ctx = FerricStore.Instance.get(:default)
          acl_ok = state.acl_cache == :full_access or
                   (is_map(state.acl_cache) and state.acl_cache.commands == :all and state.acl_cache.keys == :all)

          if not acl_ok do
            :fallback
          else
            pressure_ok = :atomics.get(ctx.pressure_flags, 1) == 0 and
                          :atomics.get(ctx.pressure_flags, 2) == 0
            if not pressure_ok do
              :fallback
            else
              case live_ctx.durability_mode do
                :all_async ->
                  {:ok, do_batch_set_async(kv_pairs, state, send_response_fn)}

                :all_quorum ->
                  {:ok, do_batch_set_quorum(kv_pairs, state, send_response_fn)}

                :mixed ->
                  {async_kvs, quorum_kvs} = Enum.split_with(kv_pairs, fn {key, _} ->
                    Router.durability_for_key_public(ctx, key) == :async
                  end)

                  if quorum_kvs == [] do
                    {:ok, do_batch_set_async(async_kvs, state, send_response_fn)}
                  else
                    :fallback
                  end
              end
            end
          end

        :fallback ->
          :fallback
      end
    end
  end

  defp extract_plain_sets(commands) do
    extract_plain_sets(commands, [])
  end

  defp extract_plain_sets([], acc), do: {:ok, Enum.reverse(acc)}

  defp extract_plain_sets([[name, key, value] | rest], acc) when is_binary(name) and is_binary(key) do
    if fast_upcase(name) == "SET" do
      extract_plain_sets(rest, [{key, value} | acc])
    else
      :fallback
    end
  end

  defp extract_plain_sets(_, _acc), do: :fallback

  defp do_batch_set_async(kv_pairs, state, send_response_fn) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))
    Router.batch_async_put(state.instance_ctx, kv_pairs)

    ok = Encoder.ok_response()
    response = List.duplicate(ok, length(kv_pairs))
    send_response_fn.(state.socket, state.transport, response)
    {:continue, state}
  end

  defp do_batch_set_quorum(kv_pairs, state, send_response_fn) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))
    results = Router.batch_quorum_put(state.instance_ctx, kv_pairs)

    response = Enum.map(results, fn
      :ok -> Encoder.ok_response()
      {:error, _} = err -> Encoder.encode(err)
    end)

    send_response_fn.(state.socket, state.transport, response)
    {:continue, state}
  end

  # ---------------------------------------------------------------------------
  # Sliding window dispatch
  # ---------------------------------------------------------------------------

  defp sliding_window_dispatch(commands, state, handle_command_fn, send_response_fn) do
    if state.transport == :ranch_tcp do
      TcpOpts.set_cork(state.socket, true)
      result = do_sliding_window(commands, [], true, state, handle_command_fn, send_response_fn)
      TcpOpts.set_cork(state.socket, false)
      result
    else
      do_sliding_window(commands, [], true, state, handle_command_fn, send_response_fn)
    end
  end

  # Base case: no more commands, flush any remaining pure group.
  defp do_sliding_window([], pure_acc, all_reads?, state, handle_command_fn, send_response_fn) do
    case flush_pure_group_pre(Enum.reverse(pure_acc), all_reads?, state, handle_command_fn, send_response_fn) do
      {:quit, _quit_state} = quit -> quit
      {:continue, new_state} -> {:continue, new_state}
    end
  end

  defp do_sliding_window([cmd | rest], pure_acc, all_reads?, state, handle_command_fn, send_response_fn) do
    normalised = normalise_cmd(cmd)

    cond do
      stateful_command_normalised?(normalised, state) ->
        case flush_pure_group_pre(Enum.reverse(pure_acc), all_reads?, state, handle_command_fn, send_response_fn) do
          {:quit, _quit_state} = quit ->
            quit

          {:continue, flushed_state} ->
            case handle_command_fn.(cmd, flushed_state) do
              {:quit, response, quit_state} ->
                send_response_fn.(quit_state.socket, quit_state.transport, response)
                {:quit, quit_state}

              {:continue, response, new_state} ->
                send_response_fn.(new_state.socket, new_state.transport, response)
                do_sliding_window(rest, [], true, new_state, handle_command_fn, send_response_fn)
            end
        end

      barrier_command_normalised?(normalised) ->
        case flush_pure_group_pre(Enum.reverse(pure_acc), all_reads?, state, handle_command_fn, send_response_fn) do
          {:quit, _quit_state} = quit ->
            quit

          {:continue, flushed_state} ->
            is_read = read_cmd?(normalised)
            do_sliding_window(rest, [{cmd, normalised}], is_read, flushed_state, handle_command_fn, send_response_fn)
        end

      true ->
        is_read = read_cmd?(normalised)
        do_sliding_window(rest, [{cmd, normalised} | pure_acc], all_reads? and is_read, state, handle_command_fn, send_response_fn)
    end
  end

  # ---------------------------------------------------------------------------
  # Flush pure group
  # ---------------------------------------------------------------------------

  defp flush_pure_group_pre([], _all_reads?, state, _handle_command_fn, _send_response_fn), do: {:continue, state}

  defp flush_pure_group_pre([{single_cmd, _norm}], _all_reads?, state, handle_command_fn, send_response_fn) do
    case handle_command_fn.(single_cmd, state) do
      {:quit, response, quit_state} ->
        send_response_fn.(quit_state.socket, quit_state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response_fn.(new_state.socket, new_state.transport, response)
        {:continue, new_state}
    end
  end

  defp flush_pure_group_pre(normalised, all_reads?, state, _handle_command_fn, send_response_fn) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

    if all_reads? do
      flush_pure_reads_fast_normalised(normalised, store, state, send_response_fn)
    else
      flush_pure_group_sliding_window_normalised(normalised, store, state, send_response_fn)
    end
  end

  defp flush_pure_reads_fast_normalised(normalised, store, state, send_response_fn) do
    {final_action, responses} =
      Enum.reduce_while(normalised, {:continue, []}, fn {_cmd, norm}, {:continue, acc} ->
        case dispatch_pure_command_normalised(norm, store, state) do
          {:quit, response} ->
            {:halt, {:quit, [response | acc]}}

          {:continue, response} ->
            {:cont, {:continue, [response | acc]}}
        end
      end)

    batched = Enum.reverse(responses)
    send_response_fn.(state.socket, state.transport, batched)

    case final_action do
      :quit -> {:quit, state}
      :continue -> {:continue, state}
    end
  end

  defp flush_pure_group_sliding_window_normalised(normalised, store, state, send_response_fn) do
    indexed_cmds =
      normalised
      |> Enum.with_index()
      |> Enum.map(fn {{_cmd, norm}, idx} -> {norm, idx, command_shard_key_normalised(state.instance_ctx, norm)} end)

    lanes = Enum.group_by(indexed_cmds, fn {_norm, _idx, shard_key} -> shard_key end)

    total = length(normalised)
    conn_pid = self()

    lane_tasks =
      Enum.map(lanes, fn {_shard_key, lane_cmds} ->
        Task.async(fn ->
          Enum.each(lane_cmds, fn {norm, idx, _shard_key} ->
            result = dispatch_pure_command_normalised(norm, store, state)
            send(conn_pid, {:lane_result, idx, result})
          end)
        end)
      end)

    result = sliding_window_collect(state, 0, total, %{}, send_response_fn)

    Enum.each(lane_tasks, fn task ->
      Task.await(task, :infinity)
    end)

    result
  end

  # ---------------------------------------------------------------------------
  # Sliding window collect
  # ---------------------------------------------------------------------------

  defp sliding_window_collect(state, cursor, total, _buffer, _send_response_fn) when cursor >= total do
    {:continue, state}
  end

  defp sliding_window_collect(state, cursor, total, buffer, send_response_fn) do
    case Map.pop(buffer, cursor) do
      {{action, response}, new_buffer} ->
        case action do
          :quit ->
            send_response_fn.(state.socket, state.transport, response)
            {:quit, state}

          :continue ->
            send_response_fn.(state.socket, state.transport, response)
            sliding_window_collect(state, cursor + 1, total, new_buffer, send_response_fn)
        end

      {nil, _buffer} ->
        receive do
          {:lane_result, ^cursor, {action, response}} ->
            case action do
              :quit ->
                send_response_fn.(state.socket, state.transport, response)
                {:quit, state}

              :continue ->
                send_response_fn.(state.socket, state.transport, response)
                sliding_window_collect(state, cursor + 1, total, buffer, send_response_fn)
            end

          {:lane_result, idx, result} when idx > cursor ->
            new_buffer = Map.put(buffer, idx, result)
            sliding_window_collect(state, cursor, total, new_buffer, send_response_fn)
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Command classification helpers
  # ---------------------------------------------------------------------------

  @doc false
  def normalise_cmd({:inline, [name | args]}) when is_binary(name),
    do: {fast_upcase(name), args}

  def normalise_cmd({:inline, []}), do: :unknown
  def normalise_cmd([name | args]) when is_binary(name), do: {fast_upcase(name), args}
  def normalise_cmd(_other), do: :unknown

  defp fast_upcase(<<first, _::binary>> = cmd) when first >= ?A and first <= ?Z, do: cmd
  defp fast_upcase(cmd), do: String.upcase(cmd)

  @doc false
  def stateful_command_normalised?(:unknown, _state), do: true

  def stateful_command_normalised?({name, _args}, state) do
    state.multi_state == :queuing or
      in_pubsub_mode?(state) or
      requires_auth?(state) or
      name in @stateful_cmds or
      String.starts_with?(name, "CLIENT")
  end

  @doc false
  def read_cmd?({name, _args}), do: MapSet.member?(@read_cmds_set, name)
  def read_cmd?(:unknown), do: false

  @doc false
  def barrier_command_normalised?(:unknown), do: false

  def barrier_command_normalised?({name, args}) do
    name in @always_multi_cmds or
      name in @barrier_server_cmds or
      (name in @variadic_key_cmds and match?([_, _ | _], args))
  end

  @doc false
  def command_shard_key_normalised(_ctx, :unknown), do: :server

  def command_shard_key_normalised(ctx, {name, args}) do
    cond do
      name in @always_multi_cmds ->
        :barrier

      name in @variadic_key_cmds ->
        case args do
          [single_key] -> {:shard, Router.shard_for(ctx, single_key)}
          _ -> :barrier
        end

      name in @server_cmds_no_key ->
        :server

      args != [] ->
        {:shard, Router.shard_for(ctx, hd(args))}

      true ->
        :server
    end
  end

  @doc false
  def dispatch_pure_command_normalised(:unknown, _store, _state) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"})}
  end

  def dispatch_pure_command_normalised({name, args}, store, state) do
    with :ok <- ConnAuth.check_command_cached(state.acl_cache, name),
         :ok <- ConnAuth.check_keys_cached(state.acl_cache, name, args) do
      Stats.incr_commands(state.stats_counter)

      result =
        try do
          Dispatcher.dispatch(name, args, store)
        catch
          :exit, {:noproc, _} ->
            {:error, "ERR server not ready, shard process unavailable"}

          :exit, {reason, _} ->
            {:error, "ERR internal error: #{inspect(reason)}"}
        end

      ConnTracking.maybe_notify_keyspace(name, args, result)
      ConnTracking.maybe_notify_tracking(name, args, result, state)
      {:continue, Encoder.encode(result)}
    else
      {:error, _reason} = err ->
        FerricstoreServer.Acl.log_command_denied(
          state.username,
          name,
          format_peer(state.peer),
          state.client_id
        )

        {:continue, Encoder.encode(err)}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false
  defp in_pubsub_mode?(state), do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  defp requires_auth?(state) do
    not state.authenticated and state.require_auth
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
