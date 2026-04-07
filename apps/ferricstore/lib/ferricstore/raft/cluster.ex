defmodule Ferricstore.Raft.Cluster do
  @moduledoc """
  Manages the ra Raft cluster for FerricStore shards.

  Each shard is an independent Raft group with its own leader. In single-node
  mode (development, testing), each group has exactly one member -- self quorum.

  This module provides functions to:
    * Start the ra system
    * Start individual shard ra servers
    * Build ra server IDs and configurations

  ## Deployment topology (per spec section 2.6)

  Single node: each shard's Raft group has one member. Writes are durable
  after local log append + fsync. No network round trip needed.

  Three-node cluster: each shard's Raft group has three members. Writes
  require quorum (2 of 3) acknowledgement before commit.
  """

  require Logger

  @ra_system :ferricstore_raft

  @doc """
  Returns the ra system name used by FerricStore.
  """
  @spec system_name() :: atom()
  def system_name, do: @ra_system

  @doc """
  Starts the ra system for FerricStore.

  Must be called before any ra servers are started. The data directory
  for ra's WAL and segment files is placed under `data_dir/ra`.

  ## Parameters

    * `data_dir` -- base data directory for FerricStore
  """
  @spec start_system(binary()) :: :ok | {:error, term()}
  def start_system(data_dir) do
    ra_data_dir = Path.join(data_dir, "ra") |> to_charlist()
    File.mkdir_p!(Path.join(data_dir, "ra"))

    names = :ra_system.derive_names(@ra_system)

    config = %{
      name: @ra_system,
      names: names,
      data_dir: ra_data_dir,
      wal_data_dir: ra_data_dir,
      segment_max_entries: 32_768
    }

    case :ra_system.start(config) do
      {:ok, _pid} ->
        Logger.info("ra system started: #{inspect(@ra_system)}")
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} = err ->
        Logger.error("Failed to start ra system: #{inspect(reason)}")
        err
    end
  end

  @doc """
  Starts a ra server that joins an existing Raft group as a follower.

  Unlike `start_shard_server/6` which may create a new single-node group,
  this function configures `initial_members` with the provided cluster members
  so ra knows to join the existing group. Used when a new node joins an
  already-running cluster after data sync.
  """
  @spec join_shard_server(non_neg_integer(), binary(), non_neg_integer(), binary(), atom(), [node()], keyword()) ::
          :ok | {:error, term()}
  def join_shard_server(shard_index, shard_data_path, active_file_id, active_file_path, ets, cluster_members, opts \\ []) do
    ra_sys = Keyword.get(opts, :ra_system, @ra_system)
    membership = Keyword.get(opts, :membership, :voter)
    skip_below_index = Keyword.get(opts, :skip_below_index, 0)
    server_id = shard_server_id(shard_index)

    machine_config = %{
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      active_file_id: active_file_id,
      active_file_path: active_file_path,
      ets: ets,
      data_dir: Path.dirname(shard_data_path),
      skip_below_index: skip_below_index
    }

    initial_members =
      Enum.map(cluster_members, fn member_node ->
        shard_server_id_on(shard_index, member_node)
      end)

    server_config = %{
      id: server_id,
      uid: shard_uid(shard_index),
      cluster_name: shard_cluster_name(shard_index),
      initial_members: initial_members,
      membership: membership,
      machine: {:module, Ferricstore.Raft.StateMachine, machine_config},
      log_init_args: %{uid: shard_uid(shard_index)},
      system: ra_sys,
      min_recovery_checkpoint_interval: 1
    }

    case :ra.start_server(ra_sys, server_config) do
      :ok ->
        Logger.info("Shard #{shard_index}: joined cluster with #{length(initial_members)} members")
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, :not_new} ->
        # ra found existing state on disk — restart instead of start
        Logger.info("Shard #{shard_index}: existing ra state found, restarting to join cluster")
        case :ra.restart_server(ra_sys, server_id) do
          :ok -> :ok
          {:error, reason} ->
            Logger.error("Shard #{shard_index}: restart failed: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} = err ->
        Logger.error("Shard #{shard_index}: failed to join cluster: #{inspect(reason)}")
        err
    end
  end

  @doc """
  Returns the ra server ID for a shard on a specific node.
  """
  @spec shard_server_id_on(non_neg_integer(), node()) :: :ra.server_id()
  def shard_server_id_on(shard_index, node) do
    {:"ferricstore_shard_#{shard_index}", node}
  end

  @doc """
  Adds a node to an existing shard's Raft group.

  The membership determines the node's role:
    * `:voter` — full quorum member (default)
    * `:promotable` — receives replication, can be promoted to voter
    * `:non_voter` — permanent read-only, never promoted

  ## Parameters

    * `shard_index` — zero-based shard index
    * `node` — the Erlang node to add
    * `membership` — `:voter`, `:promotable`, or `:non_voter` (default: `:voter`)
  """
  @spec add_member(non_neg_integer(), node(), atom()) :: :ok | {:error, term()}
  def add_member(shard_index, node, membership \\ :voter) do
    leader = shard_server_id(shard_index)
    new_member = %{id: shard_server_id_on(shard_index, node), membership: membership}

    case :ra.add_member(leader, new_member) do
      {_, _, _leader} -> :ok
      {:error, :already_member} -> :ok
      {:error, reason} -> {:error, reason}
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Removes a node from a shard's Raft group.
  """
  @spec remove_member(non_neg_integer(), node()) :: :ok | {:error, term()}
  def remove_member(shard_index, node) do
    leader = shard_server_id(shard_index)
    member = shard_server_id_on(shard_index, node)

    case :ra.remove_member(leader, member) do
      {_, _, _leader} -> :ok
      {:error, :not_member} -> :ok
      {:error, reason} -> {:error, reason}
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Returns the current members and leader for a shard's Raft group.
  """
  @spec members(non_neg_integer()) :: {:ok, list(), term()} | {:error, term()}
  def members(shard_index) do
    :ra.members(shard_server_id(shard_index))
  end

  @doc """
  Transfers leadership of a shard to a specific node.
  """
  @spec transfer_leadership(non_neg_integer(), node()) :: :ok | {:error, term()}
  def transfer_leadership(shard_index, target_node) do
    server_id = shard_server_id(shard_index)
    target_id = shard_server_id_on(shard_index, target_node)
    :ra.transfer_leadership(server_id, target_id)
  end

  @doc """
  Starts a ra server for a single shard.

  In single-node mode, creates a self-quorum Raft group. In cluster mode,
  uses the configured cluster_nodes as initial_members so all nodes form
  a single Raft group per shard.

  The `membership` option controls this node's role in the group:
    * `:voter` — full quorum member (default)
    * `:promotable` — receives replication, can be promoted
    * `:non_voter` — permanent read-only

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `shard_data_path` -- path to shard's Bitcask data directory
    * `active_file_id` -- current active log file ID
    * `active_file_path` -- path to current active log file
    * `ets` -- ETS table name (already created)

  ## Returns

    * `:ok` on success
    * `{:error, reason}` on failure
  """
  @spec start_shard_server(non_neg_integer(), binary(), non_neg_integer(), binary(), atom(), keyword()) ::
          :ok | {:error, term()}
  def start_shard_server(shard_index, shard_data_path, active_file_id, active_file_path, ets, opts \\ []) do
    ra_sys = Keyword.get(opts, :ra_system, @ra_system)
    membership = Keyword.get(opts, :membership, :voter)
    server_id = shard_server_id(shard_index)

    machine_config = %{
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      active_file_id: active_file_id,
      active_file_path: active_file_path,
      ets: ets,
      data_dir: Path.dirname(shard_data_path)
    }

    # In cluster mode, initial_members includes all configured nodes.
    # In single-node mode (no cluster_nodes), just self.
    cluster_nodes = Application.get_env(:ferricstore, :cluster_nodes, [])

    initial_members =
      if cluster_nodes == [] do
        [server_id]
      else
        Enum.map(cluster_nodes, fn node ->
          shard_server_id_on(shard_index, node)
        end)
      end

    server_config = %{
      id: server_id,
      uid: shard_uid(shard_index),
      cluster_name: shard_cluster_name(shard_index),
      initial_members: initial_members,
      membership: membership,
      machine: {:module, Ferricstore.Raft.StateMachine, machine_config},
      log_init_args: %{uid: shard_uid(shard_index)},
      system: ra_sys,
      min_recovery_checkpoint_interval: 1
    }

    case :ra.start_server(ra_sys, server_config) do
      :ok ->
        trigger_and_wait(server_id)

      {:error, {:already_started, _pid}} ->
        handle_already_started(ra_sys, server_id, server_config, shard_index)

      {:error, reason} ->
        handle_start_error(ra_sys, server_id, server_config, shard_index, reason)
    end
  end

  defp trigger_and_wait(server_id) do
    :ra.trigger_election(server_id)
    wait_for_leader(server_id)
  end

  defp handle_already_started(ra_sys, server_id, server_config, shard_index) do
    Logger.info("ra server for shard #{shard_index} already running, stopping and restarting with same UID")
    _ = :ra.stop_server(ra_sys, server_id)
    Process.sleep(100)

    case :ra.start_server(ra_sys, server_config) do
      :ok ->
        trigger_and_wait(server_id)

      {:error, :not_new} ->
        restart_existing_server(ra_sys, server_id, shard_index)

      {:error, retry_reason} = err ->
        Logger.error("Failed to start ra server (after stop) for shard #{shard_index}: #{inspect(retry_reason)}")
        err
    end
  end

  defp restart_existing_server(ra_sys, server_id, shard_index) do
    case :ra.restart_server(ra_sys, server_id) do
      :ok ->
        trigger_and_wait(server_id)

      {:error, restart_reason} = err ->
        Logger.error("Failed to restart ra server for shard #{shard_index}: #{inspect(restart_reason)}")
        err
    end
  end

  defp handle_start_error(ra_sys, server_id, server_config, shard_index, reason) do
    Logger.warning(
      "ra server for shard #{shard_index} failed with #{inspect(reason)}, " <>
        "attempting fresh start with unique UID"
    )
    _ = :ra.stop_server(ra_sys, server_id)
    _ = :ra.force_delete_server(ra_sys, server_id)

    restart_uid = shard_uid(shard_index) <> "_#{System.unique_integer([:positive])}"
    restart_config = %{server_config | uid: restart_uid, log_init_args: %{uid: restart_uid}}

    case :ra.start_server(ra_sys, restart_config) do
      :ok ->
        trigger_and_wait(server_id)

      {:error, retry_reason} = err ->
        Logger.error("Failed to start ra server (recovery) for shard #{shard_index}: #{inspect(retry_reason)}")
        err
    end
  end

  @doc """
  Stops and deletes the ra server for a shard.

  Used during shard restarts and in test cleanup.

  ## Parameters

    * `shard_index` -- zero-based shard index
  """
  @spec stop_shard_server(non_neg_integer()) :: :ok | {:error, term()}
  def stop_shard_server(shard_index) do
    server_id = shard_server_id(shard_index)

    case :ra.stop_server(@ra_system, server_id) do
      :ok -> :ok
      {:error, :noproc} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns the ra server ID for a shard.

  The server ID is a `{name, node()}` tuple as required by ra.

  ## Examples

      iex> Ferricstore.Raft.Cluster.shard_server_id(0)
      {:"ferricstore_shard_0", node()}
  """
  @spec shard_server_id(non_neg_integer()) :: :ra.server_id()
  def shard_server_id(shard_index) do
    {:"ferricstore_shard_#{shard_index}", node()}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp shard_uid(shard_index) do
    "ferricstore_shard_#{shard_index}"
  end

  defp shard_cluster_name(shard_index) do
    :"ferricstore_shard_cluster_#{shard_index}"
  end

  # Waits for the ra server to elect a leader. In single-node mode this
  # should happen almost immediately after triggering the election.
  defp wait_for_leader(server_id, attempts \\ 50)
  defp wait_for_leader(_server_id, 0), do: {:error, :leader_election_timeout}

  defp wait_for_leader(server_id, attempts) do
    case :ra.members(server_id) do
      {:ok, _members, _leader} ->
        :ok

      {:error, _} ->
        Process.sleep(10)
        wait_for_leader(server_id, attempts - 1)

      {:timeout, _} ->
        Process.sleep(10)
        wait_for_leader(server_id, attempts - 1)
    end
  end
end
