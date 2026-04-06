defmodule Ferricstore.Cluster.SnapshotDownloader do
  @moduledoc """
  Downloads cluster snapshots from object storage for new node bootstrap.

  When a new node joins the cluster and cannot catch up via Raft WAL replay
  alone (the WAL gap is too large), it uses this module to download the latest
  full snapshot from object storage, extract each shard's data, and then apply
  any remaining WAL entries to reach the current state.

  ## Usage

      opts = [
        adapter: Ferricstore.Cluster.SnapshotStore.Local,
        base_dir: "/tmp/snapshots",
        prefix: "snapshots",
        dest_dir: "/var/lib/ferricstore/data"
      ]

      case Ferricstore.Cluster.SnapshotDownloader.download_latest(opts) do
        {:ok, manifest} ->
          # manifest contains per-shard raft_index values for WAL replay
          IO.inspect(manifest)

        {:error, reason} ->
          IO.puts("Snapshot download failed: \#{inspect(reason)}")
      end
  """

  require Logger

  alias Ferricstore.Cluster.SnapshotStore.Manifest

  @default_prefix "snapshots"

  @doc """
  Downloads the latest snapshot and extracts all shard data.

  Steps:
  1. Download `latest.json` to find the current manifest key.
  2. Download and parse the manifest.
  3. For each shard (in parallel): download the tar archive and extract it
     to the destination shard data directory.
  4. Return the parsed manifest with per-shard Raft indices.

  ## Options

    * `:adapter` (required) -- the `SnapshotStore` adapter module
    * `:dest_dir` (required) -- local directory to extract shard data into
    * `:prefix` -- key prefix (default: `"snapshots"`)
    * All other options are passed through to the adapter callbacks.

  ## Returns

    * `{:ok, manifest}` -- the parsed manifest map (see `Manifest` module)
    * `{:error, reason}` -- download or extraction failed
  """
  @spec download_latest(keyword()) :: {:ok, map()} | {:error, term()}
  def download_latest(opts) do
    adapter = Keyword.fetch!(opts, :adapter)
    dest_dir = Keyword.fetch!(opts, :dest_dir)
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    adapter_opts = Keyword.drop(opts, [:adapter, :dest_dir, :prefix])

    tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_dl_#{System.unique_integer([:positive])}")

    try do
      File.mkdir_p!(tmp_dir)

      with {:ok, manifest_key} <- fetch_latest_pointer(adapter, prefix, tmp_dir, adapter_opts),
           {:ok, manifest} <- fetch_manifest(adapter, manifest_key, tmp_dir, adapter_opts),
           :ok <- download_all_shards(adapter, manifest, dest_dir, tmp_dir, adapter_opts) do
        Logger.info(
          "SnapshotDownloader: restored #{manifest["shard_count"]} shards " <>
            "from snapshot #{manifest["timestamp"]}"
        )

        {:ok, manifest}
      end
    after
      File.rm_rf(tmp_dir)
    end
  end

  @doc """
  Downloads a specific snapshot by its manifest key.

  Same as `download_latest/1` but skips the `latest.json` lookup and uses
  the provided manifest key directly.

  ## Options

  Same as `download_latest/1`, plus:

    * `:manifest_key` (required) -- the key of the manifest to download
  """
  @spec download_snapshot(keyword()) :: {:ok, map()} | {:error, term()}
  def download_snapshot(opts) do
    adapter = Keyword.fetch!(opts, :adapter)
    dest_dir = Keyword.fetch!(opts, :dest_dir)
    manifest_key = Keyword.fetch!(opts, :manifest_key)
    adapter_opts = Keyword.drop(opts, [:adapter, :dest_dir, :manifest_key, :prefix])

    tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_dl_#{System.unique_integer([:positive])}")

    try do
      File.mkdir_p!(tmp_dir)

      with {:ok, manifest} <- fetch_manifest(adapter, manifest_key, tmp_dir, adapter_opts),
           :ok <- download_all_shards(adapter, manifest, dest_dir, tmp_dir, adapter_opts) do
        {:ok, manifest}
      end
    after
      File.rm_rf(tmp_dir)
    end
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  @spec fetch_latest_pointer(module(), binary(), binary(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  defp fetch_latest_pointer(adapter, prefix, tmp_dir, adapter_opts) do
    latest_key = Path.join(prefix, "latest.json")
    local_path = Path.join(tmp_dir, "latest.json")

    case adapter.download(latest_key, local_path, adapter_opts) do
      :ok ->
        case Jason.decode(File.read!(local_path)) do
          {:ok, %{"manifest_key" => key}} -> {:ok, key}
          {:ok, _} -> {:error, :invalid_latest_json}
          {:error, reason} -> {:error, {:json_decode_error, reason}}
        end

      {:error, reason} ->
        {:error, {:latest_download_failed, reason}}
    end
  end

  @spec fetch_manifest(module(), binary(), binary(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defp fetch_manifest(adapter, manifest_key, tmp_dir, adapter_opts) do
    local_path = Path.join(tmp_dir, "manifest.json")

    case adapter.download(manifest_key, local_path, adapter_opts) do
      :ok ->
        Manifest.parse(File.read!(local_path))

      {:error, reason} ->
        {:error, {:manifest_download_failed, reason}}
    end
  end

  @spec download_all_shards(module(), map(), binary(), binary(), keyword()) ::
          :ok | {:error, term()}
  defp download_all_shards(adapter, manifest, dest_dir, tmp_dir, adapter_opts) do
    shards = Map.get(manifest, "shards", [])

    tasks =
      Enum.map(shards, fn shard ->
        Task.async(fn ->
          download_shard(adapter, shard, dest_dir, tmp_dir, adapter_opts)
        end)
      end)

    results = Task.await_many(tasks, :infinity)
    errors = Enum.filter(results, &match?({:error, _}, &1))

    if errors == [] do
      :ok
    else
      {:error, {:shard_download_failed, errors}}
    end
  end

  @spec download_shard(module(), map(), binary(), binary(), keyword()) ::
          :ok | {:error, term()}
  defp download_shard(adapter, shard, dest_dir, tmp_dir, adapter_opts) do
    shard_index = shard["index"]
    tar_key = shard["tar_key"]
    tar_local = Path.join(tmp_dir, "shard_#{shard_index}.tar.gz")
    shard_dest = Path.join([dest_dir, "data", "shard_#{shard_index}"])

    with :ok <- adapter.download(tar_key, tar_local, adapter_opts) do
      File.mkdir_p!(shard_dest)
      extract_tar(tar_local, shard_dest)
    end
  rescue
    e ->
      Logger.error(
        "SnapshotDownloader: shard #{shard["index"]} failed: #{Exception.message(e)}"
      )

      {:error, {shard["index"], Exception.message(e)}}
  end

  @spec extract_tar(binary(), binary()) :: :ok | {:error, term()}
  defp extract_tar(tar_path, dest_dir) do
    case :erl_tar.extract(String.to_charlist(tar_path), [
           :compressed,
           {:cwd, String.to_charlist(dest_dir)}
         ]) do
      :ok -> :ok
      {:error, reason} -> {:error, {:extract_failed, reason}}
    end
  end
end
