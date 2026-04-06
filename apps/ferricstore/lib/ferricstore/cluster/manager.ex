defmodule Ferricstore.Cluster.Manager do
  @moduledoc """
  Manages cluster membership: monitors node connections, orchestrates join/leave
  flows, and coordinates data sync for new followers.

  Subscribes to :net_kernel nodeup/nodedown events. When a new node connects
  (via libcluster or manual Node.connect), checks if it needs to be added to
  the Raft groups. When a node disconnects, starts a removal timer.

  ## Modes

    * `:standalone` — no cluster configured, no-op
    * `:cluster` — cluster_nodes configured, actively managing membership
  """

  use GenServer
  require Logger

  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @default_remove_delay_ms 60_000

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the current cluster mode (:standalone or :cluster)."
  @spec mode() :: :standalone | :cluster
  def mode do
    GenServer.call(__MODULE__, :mode)
  end

  @doc "Returns sync status for this node (:synced, :syncing, or :not_started)."
  @spec sync_status() :: :synced | :syncing | :not_started
  def sync_status do
    GenServer.call(__MODULE__, :sync_status)
  end

  @doc "Returns a map of all known nodes and their status."
  @spec node_status() :: map()
  def node_status do
    GenServer.call(__MODULE__, :node_status)
  end

  @doc """
  Adds a node to the cluster. Triggers data sync if needed.

  The node must be reachable via Erlang distribution (Node.connect or libcluster).
  """
  @spec add_node(node(), atom()) :: :ok | {:error, term()}
  def add_node(node, role \\ :voter) do
    GenServer.call(__MODULE__, {:add_node, node, role}, 120_000)
  end

  @doc """
  Removes a node from the cluster gracefully.

  If the node is a leader for any shard, leadership is transferred first.
  """
  @spec remove_node(node()) :: :ok | {:error, term()}
  def remove_node(node) do
    GenServer.call(__MODULE__, {:remove_node, node}, 30_000)
  end

  @doc "Gracefully leaves the cluster (called on the departing node)."
  @spec leave() :: :ok | {:error, term()}
  def leave do
    GenServer.call(__MODULE__, :leave, 30_000)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    cluster_nodes = Application.get_env(:ferricstore, :cluster_nodes, [])
    role = Application.get_env(:ferricstore, :cluster_role, :voter)
    remove_delay = Keyword.get(opts, :remove_delay_ms,
      Application.get_env(:ferricstore, :cluster_remove_delay_ms, @default_remove_delay_ms))

    mode = if cluster_nodes == [], do: :standalone, else: :cluster

    # Always subscribe to nodeup/nodedown — even standalone nodes need to
    # detect when a new node wants to join them (auto-discovery).
    if Node.alive?() do
      :net_kernel.monitor_nodes(true, node_type: :visible)
    end

    if mode == :cluster do
      Logger.info("ClusterManager started in cluster mode, role=#{role}, nodes=#{inspect(cluster_nodes)}")
    else
      Logger.info("ClusterManager started in standalone mode")
    end

    state = %{
      mode: mode,
      role: role,
      cluster_nodes: cluster_nodes,
      remove_delay_ms: remove_delay,
      known_nodes: MapSet.new(),
      remove_timers: %{},
      sync_status: if(mode == :cluster, do: :synced, else: :not_started),
      shard_sync_status: %{},
      shard_count: Application.get_env(:ferricstore, :shard_count, 4)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:mode, _from, state) do
    {:reply, state.mode, state}
  end

  def handle_call(:sync_status, _from, state) do
    {:reply, state.sync_status, state}
  end

  def handle_call(:node_status, _from, state) do
    status =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        case RaftCluster.members(shard_idx) do
          {:ok, members, leader} ->
            {shard_idx, %{members: members, leader: leader}}

          {:error, reason} ->
            {shard_idx, %{error: reason}}
        end
      end

    {:reply, %{
      mode: state.mode,
      role: state.role,
      node: node(),
      connected_nodes: Node.list(),
      known_nodes: MapSet.to_list(state.known_nodes),
      sync_status: state.sync_status,
      shard_sync_status: state.shard_sync_status,
      shards: status
    }, state}
  end

  def handle_call({:add_node, target_node, role}, _from, state) do
    membership = role_to_membership(role)
    result = do_join_node(target_node, membership, state)
    new_known = MapSet.put(state.known_nodes, target_node)
    {:reply, result, %{state | known_nodes: new_known}}
  end

  def handle_call({:remove_node, target_node}, _from, state) do
    result = do_remove_node(target_node, state)
    new_known = MapSet.delete(state.known_nodes, target_node)
    {:reply, result, %{state | known_nodes: new_known}}
  end

  def handle_call(:leave, _from, state) do
    result = do_leave(state)
    {:reply, result, %{state | mode: :standalone}}
  end

  # ---------------------------------------------------------------------------
  # Node monitoring
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("ClusterManager: node connected: #{node}")

    state = cancel_remove_timer(state, node)

    cond do
      # Case 1: We know this node — it's in our cluster_nodes config
      node in state.cluster_nodes ->
        new_known = MapSet.put(state.known_nodes, node)
        spawn(fn ->
          # Read the remote node's role, not ours
          remote_role = try do
            :erpc.call(node, Application, :get_env, [:ferricstore, :cluster_role, :voter], 5_000)
          catch
            _, _ -> :voter
          end
          do_auto_join(node, remote_role)
        end)
        {:noreply, %{state | known_nodes: new_known, mode: :cluster}}

      # Case 2: We don't know this node, but it might want to join us.
      # Check if the remote node's cluster_nodes includes us.
      true ->
        spawn(fn ->
          try do
            remote_nodes = :erpc.call(node, Application, :get_env, [:ferricstore, :cluster_nodes, []], 5_000)
            if node() in remote_nodes do
              # Read the remote node's configured role (voter/replica/readonly)
              remote_role = :erpc.call(node, Application, :get_env, [:ferricstore, :cluster_role, :voter], 5_000)
              Logger.info("ClusterManager: #{node} wants to join us as #{remote_role}, initiating auto-join")
              GenServer.call(__MODULE__, {:add_node, node, remote_role}, 120_000)
            end
          catch
            _, _ -> :ok
          end
        end)
        {:noreply, state}
    end
  end

  def handle_info({:nodedown, _node, _info}, %{mode: :standalone} = state) do
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("ClusterManager: node disconnected: #{node}")

    # Start a delayed removal timer — don't remove immediately (could be transient)
    timer_ref = Process.send_after(self(), {:remove_timeout, node}, state.remove_delay_ms)
    new_timers = Map.put(state.remove_timers, node, timer_ref)

    {:noreply, %{state | remove_timers: new_timers}}
  end

  def handle_info({:remove_timeout, node}, state) do
    if node not in Node.list() do
      Logger.warning("ClusterManager: node #{node} still down after #{state.remove_delay_ms}ms, removing from Raft groups")
      do_remove_node(node, state)
    end

    new_timers = Map.delete(state.remove_timers, node)
    new_known = MapSet.delete(state.known_nodes, node)
    {:noreply, %{state | remove_timers: new_timers, known_nodes: new_known}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private: join flow (add to Raft + data sync — used by both auto and manual)
  # ---------------------------------------------------------------------------

  # Full join: sync data FIRST, then add to Raft groups.
  # Order matters: if we add to Raft first, the new node's existing ra servers
  # (started as single-node leaders) conflict with the cluster's leaders.
  # By syncing data first, the new node receives the cluster's Bitcask files.
  # Then when added to Raft, ra can start replicating from the sync point.
  #
  # This is the single code path for both :nodeup auto-join and CLUSTER.JOIN.
  defp do_join_node(target_node, membership, state) do
    Logger.info("ClusterManager: joining #{target_node} (#{membership})")
    ctx = FerricStore.Instance.get(:default)

    # Step 1: Stop ra servers on the target node.
    stop_raft_on_target(target_node, state.shard_count)

    # Step 2: Sync data — direct shard-by-shard copy from leader.
    sync_result = direct_sync(target_node, ctx)

    case sync_result do
      {:ok, sync_indices} ->
        # Step 3: Add target to our Raft groups.
        {raft_result, _shard_results} = do_add_node(target_node, membership, state)

        case raft_result do
          :ok ->
            # Step 4: Start ra servers on target as followers.
            # Pass sync_indices so the state machine skips entries already
            # present in the copied Bitcask data.
            start_raft_on_target(target_node, state.shard_count, sync_indices)
            Logger.info("ClusterManager: #{target_node} fully joined and synced")
            :ok

          {:error, _} = err ->
            Logger.error("ClusterManager: Raft add failed for #{target_node}: #{inspect(err)}")
            err
        end

      {:error, reason} ->
        Logger.error("ClusterManager: data sync failed for #{target_node}: #{inspect(reason)}")
        {:error, {:sync_failed, reason}}
    end
  end

  defp direct_sync(target_node, ctx) do
    case Ferricstore.Cluster.DataSync.sync_all_shards(target_node, ctx) do
      {:ok, sync_results} ->
        Logger.info("ClusterManager: data synced to #{target_node}: #{inspect(sync_results)}")
        # Extract raft indices from sync results
        sync_indices =
          for {shard_idx, {:synced, detail}} <- sync_results, into: %{} do
            case detail do
              :wal_bridgeable -> {shard_idx, 0}
              raft_idx when is_integer(raft_idx) -> {shard_idx, raft_idx}
              _ -> {shard_idx, 0}
            end
          end
        {:ok, sync_indices}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Auto-join: triggered by :nodeup, runs in a spawned process so
  # handle_info returns immediately. Same code path as manual join.
  defp do_auto_join(target_node, role) do
    membership = role_to_membership(role)
    Logger.info("ClusterManager: auto-joining #{target_node} as #{role}")

    # Small delay to let the new node's application finish starting
    Process.sleep(2_000)

    case do_join_node_standalone(target_node, membership) do
      :ok ->
        Logger.info("ClusterManager: auto-join complete for #{target_node}")

      {:error, reason} ->
        Logger.error("ClusterManager: auto-join failed for #{target_node}: #{inspect(reason)}")
    end
  end

  # Standalone version of do_join_node that doesn't need GenServer state
  defp do_join_node_standalone(target_node, membership) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    # Step 1: Add to all Raft groups
    for shard_idx <- 0..(shard_count - 1) do
      RaftCluster.add_member(shard_idx, target_node, membership)
    end

    # Step 2: Sync data
    ctx = FerricStore.Instance.get(:default)

    case Ferricstore.Cluster.DataSync.sync_all_shards(target_node, ctx) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: add/remove/leave operations
  # ---------------------------------------------------------------------------

  defp do_add_node(target_node, membership, state) do
    Logger.info("ClusterManager: adding #{target_node} as #{membership} to all #{state.shard_count} shards")

    shard_results =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        case RaftCluster.add_member(shard_idx, target_node, membership) do
          :ok ->
            Logger.debug("ClusterManager: added #{target_node} to shard #{shard_idx}")
            {shard_idx, :ok}

          {:error, reason} ->
            Logger.error(
              "ClusterManager: failed to add #{target_node} to shard #{shard_idx}: #{inspect(reason)}"
            )

            {shard_idx, {:error, reason}}
        end
      end

    failed = Enum.filter(shard_results, fn {_, v} -> v != :ok end)

    if failed != [] do
      {{:error, {:partial_add, shard_results}}, shard_results}
    else
      {:ok, shard_results}
    end
  end

  defp do_remove_node(target_node, state) do
    Logger.info("ClusterManager: removing #{target_node} from all #{state.shard_count} shards")

    for shard_idx <- 0..(state.shard_count - 1) do
      # If the target is leader for this shard, transfer leadership first
      case RaftCluster.members(shard_idx) do
        {:ok, _members, {_name, ^target_node}} ->
          # Target is leader — transfer before removing
          other_voters = Node.list() -- [target_node]

          if other_voters != [] do
            RaftCluster.transfer_leadership(shard_idx, hd(other_voters))
            Process.sleep(100)
          end

        _ ->
          :ok
      end

      RaftCluster.remove_member(shard_idx, target_node)
    end

    :ok
  end

  defp do_leave(state) do
    Logger.info("ClusterManager: leaving cluster")

    for shard_idx <- 0..(state.shard_count - 1) do
      # Transfer leadership away from us if we're the leader
      case RaftCluster.members(shard_idx) do
        {:ok, _members, {_name, node}} when node == node() ->
          other_voters = Node.list()

          if other_voters != [] do
            Logger.info("ClusterManager: transferring shard #{shard_idx} leadership to #{hd(other_voters)}")
            RaftCluster.transfer_leadership(shard_idx, hd(other_voters))
            Process.sleep(200)
          end

        _ ->
          :ok
      end

      # Remove ourselves from the Raft group
      RaftCluster.remove_member(shard_idx, node())
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private: helpers
  # ---------------------------------------------------------------------------

  defp cancel_remove_timer(state, node) do
    case Map.pop(state.remove_timers, node) do
      {nil, _timers} ->
        state

      {timer_ref, new_timers} ->
        Process.cancel_timer(timer_ref)
        Logger.info("ClusterManager: cancelled removal timer for #{node} (reconnected)")
        %{state | remove_timers: new_timers}
    end
  end

  # Start Raft servers on the target node. Called after data sync + add_member.
  # The target's shards are running with raft_enabled: false — they need ra
  # servers started so they can receive Raft replication from the leaders.
  # Stop and clean up ra servers on the target so they don't conflict
  # with the cluster's Raft groups when we add the target as a member.
  defp stop_raft_on_target(target_node, shard_count) do
    Logger.info("ClusterManager: stopping Raft on #{target_node} before join")

    ra_sys = Ferricstore.Raft.Cluster.system_name()

    for shard_idx <- 0..(shard_count - 1) do
      server_id = Ferricstore.Raft.Cluster.shard_server_id_on(shard_idx, target_node)

      try do
        :erpc.call(target_node, :ra, :stop_server, [ra_sys, server_id])
        :erpc.call(target_node, :ra, :force_delete_server, [ra_sys, server_id])
      catch
        _, _ -> :ok
      end
    end
  end

  defp start_raft_on_target(target_node, shard_count, sync_indices) do
    Logger.info("ClusterManager: starting Raft on #{target_node}")

    cluster_members = [node() | Node.list()] |> Enum.uniq()
    cluster_members = if target_node in cluster_members, do: cluster_members, else: [target_node | cluster_members]

    for shard_idx <- 0..(shard_count - 1) do
      try do
        target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default])
        shard_data_path = Ferricstore.DataDir.shard_data_path(target_ctx.data_dir, shard_idx)
        keydir = elem(target_ctx.keydir_refs, shard_idx)

        # skip_below_index tells the state machine to ignore entries already
        # present in the Bitcask data copied during sync.
        skip_idx = Map.get(sync_indices, shard_idx, 0)

        result = :erpc.call(target_node, Ferricstore.Raft.Cluster, :join_shard_server, [
          shard_idx, shard_data_path, 0, Path.join(shard_data_path, "00000.log"), keydir,
          cluster_members, [skip_below_index: skip_idx]
        ])

        Logger.info("ClusterManager: shard #{shard_idx} Raft joined on #{target_node} (skip_below=#{skip_idx}): #{inspect(result)}")
      catch
        kind, reason ->
          Logger.warning("ClusterManager: shard #{shard_idx} Raft join failed on #{target_node}: #{inspect({kind, reason})}")
      end
    end
  end

  defp role_to_membership(:voter), do: :voter
  defp role_to_membership(:replica), do: :promotable
  defp role_to_membership(:readonly), do: :non_voter
end
