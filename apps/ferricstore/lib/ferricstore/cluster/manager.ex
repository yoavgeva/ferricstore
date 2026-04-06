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

    if mode == :cluster do
      # Subscribe to nodeup/nodedown events
      :net_kernel.monitor_nodes(true, node_type: :visible)
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
    {result, shard_sync} = do_add_node(target_node, membership, state)
    new_known = MapSet.put(state.known_nodes, target_node)
    merged_sync = Map.merge(state.shard_sync_status, %{target_node => shard_sync})
    {:reply, result, %{state | known_nodes: new_known, shard_sync_status: merged_sync}}
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
  def handle_info({:nodeup, node, _info}, %{mode: :standalone} = state) do
    Logger.debug("ClusterManager: ignoring nodeup for #{node} (standalone mode)")
    {:noreply, state}
  end

  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("ClusterManager: node connected: #{node}")

    # Cancel any pending removal timer for this node
    state = cancel_remove_timer(state, node)

    # Check if this node is in our cluster config
    if node in state.cluster_nodes do
      new_known = MapSet.put(state.known_nodes, node)
      {:noreply, %{state | known_nodes: new_known}}
    else
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

  defp role_to_membership(:voter), do: :voter
  defp role_to_membership(:replica), do: :promotable
  defp role_to_membership(:readonly), do: :non_voter
end
