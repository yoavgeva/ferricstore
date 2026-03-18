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
  Starts a ra server for a single shard.

  Creates a single-node Raft group (self quorum) for the shard. The state
  machine is `Ferricstore.Raft.StateMachine`.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `store` -- Bitcask NIF reference (already opened)
    * `ets` -- ETS table name (already created)

  ## Returns

    * `:ok` on success
    * `{:error, reason}` on failure
  """
  @spec start_shard_server(non_neg_integer(), reference(), atom()) :: :ok | {:error, term()}
  def start_shard_server(shard_index, store, ets) do
    server_id = shard_server_id(shard_index)

    machine_config = %{
      shard_index: shard_index,
      store: store,
      ets: ets
    }

    server_config = %{
      id: server_id,
      uid: shard_uid(shard_index),
      cluster_name: shard_cluster_name(shard_index),
      initial_members: [server_id],
      machine: {:module, Ferricstore.Raft.StateMachine, machine_config},
      log_init_args: %{uid: shard_uid(shard_index)},
      system: @ra_system
    }

    case :ra.start_server(@ra_system, server_config) do
      :ok ->
        # Trigger election so single-node becomes leader immediately
        :ra.trigger_election(server_id)
        wait_for_leader(server_id)

      {:error, {:already_started, _pid}} ->
        # ra server already running -- this happens when a shard was killed
        # (Process.exit :kill bypasses terminate/2) and the supervisor
        # restarts it. The ra server process is still alive but holds
        # references to the old (now-dead) Bitcask store and ETS table.
        #
        # Strategy: stop the old ra server, force-delete its state (since the
        # machine state references are stale), and start fresh. The Bitcask
        # data on disk is intact and will be reopened by the new Shard init.
        _ = :ra.stop_server(@ra_system, server_id)
        _ = :ra.force_delete_server(@ra_system, server_id)

        case :ra.start_server(@ra_system, server_config) do
          :ok ->
            :ra.trigger_election(server_id)
            wait_for_leader(server_id)

          {:error, reason} = err ->
            Logger.error(
              "Failed to restart ra server for shard #{shard_index}: #{inspect(reason)}"
            )

            err
        end

      {:error, reason} = err ->
        Logger.error("Failed to start ra server for shard #{shard_index}: #{inspect(reason)}")
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
