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
  @spec start_shard_server(non_neg_integer(), binary(), non_neg_integer(), binary(), atom()) ::
          :ok | {:error, term()}
  def start_shard_server(shard_index, shard_data_path, active_file_id, active_file_path, ets) do
    server_id = shard_server_id(shard_index)

    machine_config = %{
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      active_file_id: active_file_id,
      active_file_path: active_file_path,
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
        # ra server already running -- this happens when a shard GenServer was
        # killed (Process.exit :kill bypasses terminate/2) and the supervisor
        # restarts it. The ra server process is still alive but holds stale
        # machine_config (old ETS ref, old Bitcask path).
        #
        # Strategy: stop the old ra server gracefully, then restart it with
        # the SAME UID so it replays the WAL and recovers all committed state.
        # We must NOT force_delete -- that would destroy the WAL/snapshot data.
        Logger.info("ra server for shard #{shard_index} already running, stopping and restarting with same UID")
        _ = :ra.stop_server(@ra_system, server_id)

        # Wait for the ra process to fully terminate before restarting.
        Process.sleep(100)

        case :ra.start_server(@ra_system, server_config) do
          :ok ->
            :ra.trigger_election(server_id)
            wait_for_leader(server_id)

          {:error, :not_new} ->
            # The server data directory already exists (expected after stop).
            # Use restart_server which re-opens existing state.
            case :ra.restart_server(@ra_system, server_id) do
              :ok ->
                :ra.trigger_election(server_id)
                wait_for_leader(server_id)

              {:error, restart_reason} = err ->
                Logger.error(
                  "Failed to restart ra server for shard #{shard_index}: #{inspect(restart_reason)}"
                )

                err
            end

          {:error, retry_reason} = err ->
            Logger.error(
              "Failed to start ra server (after stop) for shard #{shard_index}: #{inspect(retry_reason)}"
            )

            err
        end

      {:error, reason} ->
        # Any other error (e.g. :not_new, :shutdown from corrupt log,
        # {:corrupt_log, ...}). Force-delete and start fresh with a unique
        # UID to avoid WAL entry conflicts from previous incarnations.
        Logger.warning(
          "ra server for shard #{shard_index} failed with #{inspect(reason)}, " <>
            "attempting fresh start with unique UID"
        )

        _ = :ra.stop_server(@ra_system, server_id)
        _ = :ra.force_delete_server(@ra_system, server_id)

        restart_uid = shard_uid(shard_index) <> "_#{System.unique_integer([:positive])}"

        restart_config = %{
          server_config
          | uid: restart_uid,
            log_init_args: %{uid: restart_uid}
        }

        case :ra.start_server(@ra_system, restart_config) do
          :ok ->
            :ra.trigger_election(server_id)
            wait_for_leader(server_id)

          {:error, retry_reason} = err ->
            Logger.error(
              "Failed to start ra server (recovery) for shard #{shard_index}: #{inspect(retry_reason)}"
            )

            err
        end
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

  # Starts a ra server with retries. On corrupt_log errors (stale WAL entries
  # after force_delete), force-delete again and retry with a fresh UID.
  defp start_with_retry(system, server_id, config, shard_index, attempts \\ 3)

  defp start_with_retry(_system, _server_id, _config, shard_index, 0) do
    Logger.error("Failed to start ra server for shard #{shard_index} after retries")
    {:error, :start_failed_after_retries}
  end

  defp start_with_retry(system, server_id, config, shard_index, attempts) do
    case :ra.start_server(system, config) do
      :ok ->
        :ra.trigger_election(server_id)
        wait_for_leader(server_id)

      {:error, {:already_started, _pid}} ->
        # Another process started it concurrently
        :ok

      {:error, reason} ->
        Logger.warning(
          "ra server start for shard #{shard_index} failed (#{inspect(reason)}), " <>
            "retrying (#{attempts - 1} left)"
        )

        _ = :ra.force_delete_server(system, server_id)
        Process.sleep(100)

        retry_uid = shard_uid(shard_index) <> "_#{System.unique_integer([:positive])}"

        retry_config = %{
          config
          | uid: retry_uid,
            log_init_args: %{uid: retry_uid}
        }

        start_with_retry(system, server_id, retry_config, shard_index, attempts - 1)
    end
  end

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
