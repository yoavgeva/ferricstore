defmodule Ferricstore.Transaction.Coordinator do
  @moduledoc """
  Two-Phase Commit (2PC) coordinator for cross-shard MULTI/EXEC transactions.

  When a MULTI/EXEC transaction contains commands that target multiple shards,
  this module orchestrates a two-phase protocol to ensure atomicity:

  ## Phase 1 -- Prepare

  1. Group queued commands by their target shard.
  2. Send `{:prepare_tx, tx_id, commands}` to each involved shard concurrently.
  3. Each shard executes all its commands within a single `handle_call`
     (ensuring no other client can interleave on that shard) and returns
     `{:prepared, tx_id, results}`.
  4. If ALL shards return `:prepared`, proceed to Phase 2.
  5. If ANY shard fails, send `{:rollback_tx, tx_id}` to all prepared shards.

  ## Phase 2 -- Commit

  1. Send `{:commit_tx, tx_id}` to each prepared shard.
  2. Each shard cleans up staged transaction metadata.
  3. Collect all results and return to the client in original command order.

  ## Fast Path

  When all commands in the transaction target a single shard, the coordinator
  skips the 2PC protocol entirely and dispatches commands directly through the
  Dispatcher, avoiding any overhead.

  ## Design

  This module is a stateless set of functions (no GenServer). The key insight
  for avoiding deadlocks is that each shard receives a batch of commands that
  target only that shard, and executes them using a shard-local store that
  reads/writes ETS and Bitcask directly (bypassing the GenServer.call path
  that would deadlock). Commands targeting other shards go through the normal
  Router path since we are not inside those shards' GenServers.
  """

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router

  require Logger

  @call_timeout 5_000

  @doc """
  Executes a queued MULTI/EXEC transaction, choosing the fast path for
  single-shard transactions or the 2PC protocol for cross-shard transactions.

  ## Parameters

    - `queue` -- list of `{command, args}` tuples queued during MULTI
    - `watched_keys` -- map of `%{key => version}` from WATCH commands
    - `sandbox_namespace` -- optional namespace prefix (binary) or `nil`

  ## Returns

    - A list of results in original command order on success
    - `nil` if WATCH detected a conflict (transaction aborted)
  """
  @spec execute([{binary(), [binary()]}], %{binary() => non_neg_integer()}, binary() | nil) ::
          [term()] | nil
  def execute([], _watched_keys, _sandbox_namespace), do: []

  def execute(queue, watched_keys, sandbox_namespace) do
    if watches_clean?(watched_keys) do
      store = build_store(sandbox_namespace)

      case classify_shards(queue, sandbox_namespace) do
        {:single_shard, _shard_idx} ->
          execute_fast_path(queue, store)

        {:multi_shard, shard_groups, index_map} ->
          execute_2pc(shard_groups, index_map, length(queue))
      end
    else
      nil
    end
  end

  # ---------------------------------------------------------------------------
  # Fast path: all commands target the same shard
  # ---------------------------------------------------------------------------

  defp execute_fast_path(queue, store) do
    Enum.map(queue, fn {cmd, args} ->
      dispatch_safe(cmd, args, store)
    end)
  end

  # ---------------------------------------------------------------------------
  # 2PC path: commands span multiple shards
  # ---------------------------------------------------------------------------

  defp execute_2pc(shard_groups, index_map, total) do
    tx_id = generate_tx_id()

    # Phase 1: Send prepare to all shards concurrently.
    # Each shard receives only the commands that target it.
    # Commands execute inside the shard's handle_call, ensuring no other
    # client can interleave on that shard.
    prepare_tasks =
      Enum.map(shard_groups, fn {shard_idx, commands_with_indices} ->
        Task.async(fn ->
          shard_name = Router.shard_name(shard_idx)

          shard_commands =
            Enum.map(commands_with_indices, fn {_orig_idx, cmd, args} -> {cmd, args} end)

          result =
            try do
              GenServer.call(shard_name, {:prepare_tx, tx_id, shard_commands}, @call_timeout)
            catch
              :exit, {:timeout, _} ->
                {:abort, tx_id, :timeout}

              :exit, {:noproc, _} ->
                {:abort, tx_id, :shard_unavailable}

              :exit, {reason, _} ->
                {:abort, tx_id, reason}
            end

          {shard_idx, result}
        end)
      end)

    prepare_results = Task.await_many(prepare_tasks, @call_timeout + 1_000)

    # Check if all shards prepared successfully
    all_prepared =
      Enum.all?(prepare_results, fn
        {_shard_idx, {:prepared, ^tx_id, _results}} -> true
        _ -> false
      end)

    if all_prepared do
      # Phase 2: Commit -- tell all shards to finalize
      Enum.each(prepare_results, fn {shard_idx, _} ->
        shard_name = Router.shard_name(shard_idx)

        try do
          GenServer.call(shard_name, {:commit_tx, tx_id}, @call_timeout)
        catch
          :exit, reason ->
            Logger.error(
              "2PC commit failed on shard #{shard_idx} for tx #{tx_id}: #{inspect(reason)}"
            )
        end
      end)

      # Reassemble results in original command order
      results_by_shard =
        Map.new(prepare_results, fn {shard_idx, {:prepared, ^tx_id, results}} ->
          {shard_idx, results}
        end)

      reassemble_results(results_by_shard, index_map, total)
    else
      # Rollback all prepared shards
      Enum.each(prepare_results, fn
        {shard_idx, {:prepared, ^tx_id, _results}} ->
          shard_name = Router.shard_name(shard_idx)

          try do
            GenServer.call(shard_name, {:rollback_tx, tx_id}, @call_timeout)
          catch
            :exit, _ -> :ok
          end

        _ ->
          :ok
      end)

      Logger.error("2PC prepare failed for tx #{tx_id}: #{inspect(prepare_results)}")
      nil
    end
  end

  # ---------------------------------------------------------------------------
  # Command dispatch helper
  # ---------------------------------------------------------------------------

  defp dispatch_safe(cmd, args, store) do
    try do
      Dispatcher.dispatch(cmd, args, store)
    catch
      :exit, {:noproc, _} ->
        {:error, "ERR server not ready, shard process unavailable"}

      :exit, {reason, _} ->
        {:error, "ERR internal error: #{inspect(reason)}"}
    end
  end

  # ---------------------------------------------------------------------------
  # Shard classification
  # ---------------------------------------------------------------------------

  # Determines which shards the transaction's commands target.
  # Returns either:
  #   {:single_shard, shard_idx} -- all commands target one shard
  #   {:multi_shard, shard_groups, index_map} -- commands span multiple shards
  #
  # shard_groups: %{shard_idx => [{original_index, cmd, args}, ...]}
  # index_map: %{original_index => {shard_idx, position_within_shard}}
  @spec classify_shards([{binary(), [binary()]}], binary() | nil) ::
          {:single_shard, non_neg_integer()}
          | {:multi_shard, %{non_neg_integer() => list()}, %{non_neg_integer() => tuple()}}
  defp classify_shards(queue, sandbox_namespace) do
    indexed =
      queue
      |> Enum.with_index()
      |> Enum.map(fn {{cmd, args}, idx} ->
        shard_idx = command_shard(args, sandbox_namespace)
        {idx, cmd, args, shard_idx}
      end)

    shard_indices = indexed |> Enum.map(fn {_, _, _, s} -> s end) |> Enum.uniq()

    case shard_indices do
      [single] ->
        {:single_shard, single}

      _multiple ->
        shard_groups =
          Enum.group_by(
            indexed,
            fn {_idx, _cmd, _args, shard_idx} -> shard_idx end,
            fn {idx, cmd, args, _shard_idx} -> {idx, cmd, args} end
          )

        index_map =
          Enum.reduce(shard_groups, %{}, fn {shard_idx, cmds}, acc ->
            cmds
            |> Enum.with_index()
            |> Enum.reduce(acc, fn {{orig_idx, _cmd, _args}, pos}, inner ->
              Map.put(inner, orig_idx, {shard_idx, pos})
            end)
          end)

        {:multi_shard, shard_groups, index_map}
    end
  end

  # Determines which shard a command targets based on its first key argument.
  defp command_shard(args, sandbox_namespace) do
    key = extract_key(args)

    full_key =
      case sandbox_namespace do
        nil -> key
        ns -> ns <> key
      end

    Router.shard_for(full_key)
  end

  @spec extract_key([binary()]) :: binary()
  defp extract_key([key | _]) when is_binary(key), do: key
  defp extract_key(_args), do: ""

  # ---------------------------------------------------------------------------
  # Result reassembly
  # ---------------------------------------------------------------------------

  defp reassemble_results(results_by_shard, index_map, total) do
    Enum.map(0..(total - 1), fn orig_idx ->
      {shard_idx, pos} = Map.fetch!(index_map, orig_idx)
      results_by_shard |> Map.fetch!(shard_idx) |> Enum.at(pos)
    end)
  end

  # ---------------------------------------------------------------------------
  # WATCH support
  # ---------------------------------------------------------------------------

  defp watches_clean?(watched) when map_size(watched) == 0, do: true

  defp watches_clean?(watched) do
    Enum.all?(watched, fn {key, saved_version} ->
      try do
        Router.get_version(key) == saved_version
      catch
        :exit, _ -> false
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Store builder (mirrors connection.ex build_store/build_raw_store)
  # ---------------------------------------------------------------------------

  @doc false
  def build_store(nil), do: build_raw_store()

  def build_store(ns) when is_binary(ns) do
    raw = build_raw_store()

    %{
      raw
      | get: fn key -> raw.get.(ns <> key) end,
        get_meta: fn key -> raw.get_meta.(ns <> key) end,
        put: fn key, val, exp -> raw.put.(ns <> key, val, exp) end,
        delete: fn key -> raw.delete.(ns <> key) end,
        exists?: fn key -> raw.exists?.(ns <> key) end
    }
  end

  defp build_raw_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn ->
        Enum.each(Router.keys(), &Router.delete/1)
        :ok
      end,
      dbsize: &Router.dbsize/0,
      incr: &Router.incr/2,
      incr_float: &Router.incr_float/2,
      append: &Router.append/2,
      getset: &Router.getset/2,
      getdel: &Router.getdel/1,
      getex: &Router.getex/2,
      setrange: &Router.setrange/3,
      cas: &Router.cas/4,
      lock: &Router.lock/3,
      unlock: &Router.unlock/2,
      extend: &Router.extend/3,
      ratelimit_add: &Router.ratelimit_add/4,
      list_op: &Router.list_op/2,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  # ---------------------------------------------------------------------------
  # Transaction ID generation
  # ---------------------------------------------------------------------------

  defp generate_tx_id do
    :erlang.unique_integer([:positive, :monotonic])
  end
end
