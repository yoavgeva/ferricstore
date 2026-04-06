defmodule Ferricstore.Cluster.DataSync do
  @moduledoc "Shard-by-shard data directory copy for new node sync."

  require Logger

  @doc """
  Copies a single shard's data from the leader to a target node.

  1. Pause writes on the shard
  2. Record Raft log index
  3. Copy all files in shard data dir + ra dir to target node
  4. Resume writes
  """
  @spec sync_shard(non_neg_integer(), node(), FerricStore.Instance.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def sync_shard(shard_index, target_node, ctx) do
    shard = elem(ctx.shard_names, shard_index)
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)
    ra_path = Path.join(ctx.data_dir, "ra")

    # 1. Pause writes
    :ok = GenServer.call(shard, {:pause_writes}, 30_000)

    try do
      # 2. Get current Raft index
      server_id = Ferricstore.Raft.Cluster.shard_server_id(shard_index)
      raft_index = get_raft_index(server_id)

      # 3. Copy files
      copy_directory(shard_data_path, target_node, shard_data_path)

      # Also copy the ra WAL dir for this shard
      ra_shard_dir = Path.join(ra_path, "ferricstore_shard_#{shard_index}")

      if File.exists?(ra_shard_dir) do
        target_ra_dir =
          Path.join(Path.dirname(shard_data_path), "ra/ferricstore_shard_#{shard_index}")

        copy_directory(ra_shard_dir, target_node, target_ra_dir)
      end

      {:ok, raft_index}
    rescue
      e -> {:error, Exception.message(e)}
    after
      # 4. Always resume writes
      GenServer.call(shard, {:resume_writes}, 5_000)
    end
  end

  @doc """
  Copies all shards sequentially.
  """
  @spec sync_all_shards(node(), FerricStore.Instance.t()) :: :ok | {:error, term()}
  def sync_all_shards(target_node, ctx) do
    for shard_idx <- 0..(ctx.shard_count - 1) do
      case sync_shard(shard_idx, target_node, ctx) do
        {:ok, _index} -> :ok
        {:error, reason} -> throw({:sync_failed, shard_idx, reason})
      end
    end

    :ok
  catch
    {:sync_failed, shard_idx, reason} ->
      {:error, {:shard_sync_failed, shard_idx, reason}}
  end

  # Copy a local directory to a remote node via :erpc
  @spec copy_directory(binary(), node(), binary()) :: :ok
  defp copy_directory(local_path, target_node, remote_path) do
    # Ensure remote dir exists
    :erpc.call(target_node, File, :mkdir_p!, [remote_path])

    # Copy each file
    {:ok, files} = File.ls(local_path)

    Enum.each(files, fn file ->
      source = Path.join(local_path, file)
      dest = Path.join(remote_path, file)

      if File.dir?(source) do
        copy_directory(source, target_node, dest)
      else
        content = File.read!(source)
        :erpc.call(target_node, File, :write!, [dest, content])
      end
    end)
  end

  @spec get_raft_index(:ra.server_id()) :: non_neg_integer()
  defp get_raft_index(server_id) do
    case :ra.member_overview(server_id) do
      {:ok, overview, _} -> Map.get(overview, :commit_index, 0)
      _ -> 0
    end
  end
end
