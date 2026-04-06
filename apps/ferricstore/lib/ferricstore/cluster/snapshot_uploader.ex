defmodule Ferricstore.Cluster.SnapshotUploader do
  @moduledoc """
  Periodic snapshot uploader for cluster data sync via object storage.

  Creates point-in-time snapshots of all shards using filesystem hardlinks
  (instant, zero-copy), tars them, and uploads the archives through the
  configured `Ferricstore.Cluster.SnapshotStore` adapter. A JSON manifest
  records per-shard Raft indices and file checksums so that downloaders can
  verify integrity and determine which snapshot to apply.

  ## How it works

  Each snapshot cycle:

  1. For each shard (sequentially):
     a. Pause writes on the shard (brief, sub-millisecond).
     b. Hardlink all shard data files into a temp directory.
     c. Record the current Raft commit index.
     d. Resume writes.
     e. Tar the hardlink directory (background, no write pause).
     f. Upload the tar via the SnapshotStore adapter.
     g. Clean up the temp directory.

  2. After all shards: write and upload a manifest JSON, then update the
     `latest.json` pointer.

  ## Configuration

  The uploader only starts its timer when `:snapshot_store` is configured:

      config :ferricstore, :snapshot_store,
        adapter: Ferricstore.Cluster.SnapshotStore.Local,
        base_dir: "/tmp/snapshots",
        interval_ms: 300_000,   # 5 minutes (default)
        prefix: "snapshots"     # key prefix (default)

  ## Supervision

  Add to the supervision tree (only starts timer if snapshot_store is configured):

      {Ferricstore.Cluster.SnapshotUploader, []}
  """

  use GenServer

  require Logger

  alias Ferricstore.Cluster.SnapshotStore.Manifest

  @default_interval_ms 300_000
  @default_prefix "snapshots"

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc "Starts the snapshot uploader."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Triggers an immediate snapshot cycle. Returns when complete."
  @spec trigger() :: :ok | {:error, term()}
  def trigger do
    GenServer.call(__MODULE__, :trigger, :infinity)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    config = Application.get_env(:ferricstore, :snapshot_store)

    case config do
      nil ->
        Logger.debug("SnapshotUploader: no snapshot_store configured, idle")
        {:ok, %{config: nil, timer: nil}}

      config when is_list(config) ->
        interval = Keyword.get(config, :interval_ms, @default_interval_ms)
        timer = Process.send_after(self(), :snapshot_cycle, interval)

        Logger.info(
          "SnapshotUploader: started with interval=#{interval}ms, " <>
            "adapter=#{inspect(Keyword.get(config, :adapter))}"
        )

        {:ok, %{config: config, timer: timer}}
    end
  end

  @impl true
  def handle_info(:snapshot_cycle, %{config: nil} = state) do
    {:noreply, state}
  end

  def handle_info(:snapshot_cycle, state) do
    run_snapshot_cycle(state.config)
    interval = Keyword.get(state.config, :interval_ms, @default_interval_ms)
    timer = Process.send_after(self(), :snapshot_cycle, interval)
    {:noreply, %{state | timer: timer}}
  end

  @impl true
  def handle_call(:trigger, _from, state) do
    # Always read fresh config — it may have been set after init
    config = state.config || Application.get_env(:ferricstore, :snapshot_store)

    if config do
      result = run_snapshot_cycle(config)
      {:reply, result, %{state | config: config}}
    else
      {:reply, {:error, :not_configured}, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Snapshot cycle
  # ---------------------------------------------------------------------------

  @spec run_snapshot_cycle(keyword()) :: :ok | {:error, term()}
  defp run_snapshot_cycle(config) do
    adapter = Keyword.fetch!(config, :adapter)
    prefix = Keyword.get(config, :prefix, @default_prefix)
    adapter_opts = Keyword.drop(config, [:adapter, :interval_ms, :prefix])

    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    timestamp = DateTime.utc_now() |> DateTime.to_iso8601()
    snapshot_prefix = Path.join(prefix, timestamp)

    tmp_base = Path.join(System.tmp_dir!(), "ferricstore_snapshot_#{System.unique_integer([:positive])}")

    try do
      File.mkdir_p!(tmp_base)

      shard_results =
        Enum.map(0..(shard_count - 1), fn shard_index ->
          snapshot_shard(shard_index, data_dir, tmp_base, snapshot_prefix, adapter, adapter_opts)
        end)

      errors = Enum.filter(shard_results, &match?({:error, _}, &1))

      if errors != [] do
        Logger.error("SnapshotUploader: #{length(errors)} shard(s) failed: #{inspect(errors)}")
        {:error, {:partial_failure, errors}}
      else
        shard_entries = Enum.map(shard_results, fn {:ok, entry} -> entry end)

        manifest = Manifest.build(timestamp, shard_count, shard_entries)
        manifest_json = Jason.encode!(manifest, pretty: true)

        # Write and upload manifest
        manifest_local = Path.join(tmp_base, "manifest.json")
        File.write!(manifest_local, manifest_json)
        manifest_key = Path.join(snapshot_prefix, "manifest.json")
        :ok = adapter.upload(manifest_local, manifest_key, adapter_opts)

        # Update latest pointer
        latest = Jason.encode!(%{"timestamp" => timestamp, "manifest_key" => manifest_key})
        latest_local = Path.join(tmp_base, "latest.json")
        File.write!(latest_local, latest)
        latest_key = Path.join(prefix, "latest.json")
        :ok = adapter.upload(latest_local, latest_key, adapter_opts)

        Logger.info("SnapshotUploader: cycle complete, #{shard_count} shards, ts=#{timestamp}")
        :ok
      end
    rescue
      e ->
        Logger.error("SnapshotUploader: cycle failed: #{Exception.message(e)}")
        {:error, {:cycle_failed, Exception.message(e)}}
    after
      File.rm_rf(tmp_base)
    end
  end

  @spec snapshot_shard(
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          module(),
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  defp snapshot_shard(shard_index, data_dir, tmp_base, snapshot_prefix, adapter, adapter_opts) do
    shard_name = :"Ferricstore.Store.Shard.#{shard_index}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

    snapshot_dir = Path.join(tmp_base, "shard_#{shard_index}")
    tar_path = Path.join(tmp_base, "shard_#{shard_index}.tar.gz")

    try do
      # 1. Pause writes (brief)
      :ok = GenServer.call(shard_name, {:pause_writes}, 30_000)

      try do
        # 2. Hardlink files to temp dir
        create_hardlink_snapshot(shard_data_path, snapshot_dir)

        # 3. Record raft index
        server_id = Ferricstore.Raft.Cluster.shard_server_id(shard_index)
        raft_index = get_raft_index(server_id)

        # 4. Resume writes (before tar + upload)
        GenServer.call(shard_name, {:resume_writes}, 5_000)

        # 5. Tar the hardlink dir
        create_tar(snapshot_dir, tar_path)

        # 6. Compute file checksums
        files = list_snapshot_files(snapshot_dir)

        # 7. Upload via adapter
        tar_key = Path.join(snapshot_prefix, "shard_#{shard_index}.tar.gz")
        :ok = adapter.upload(tar_path, tar_key, adapter_opts)

        # 8. Cleanup
        File.rm_rf(snapshot_dir)
        File.rm(tar_path)

        {:ok,
         %{
           index: shard_index,
           raft_index: raft_index,
           tar_key: tar_key,
           files: files
         }}
      rescue
        e ->
          # Make sure writes are resumed even on error
          try do
            GenServer.call(shard_name, {:resume_writes}, 5_000)
          catch
            _, _ -> :ok
          end

          reraise e, __STACKTRACE__
      end
    rescue
      e ->
        Logger.error(
          "SnapshotUploader: shard #{shard_index} failed: #{Exception.message(e)}"
        )

        {:error, {shard_index, Exception.message(e)}}
    end
  end

  # ---------------------------------------------------------------------------
  # Hardlink snapshot
  # ---------------------------------------------------------------------------

  @doc false
  @spec create_hardlink_snapshot(binary(), binary()) :: :ok
  def create_hardlink_snapshot(shard_data_path, snapshot_dir) do
    File.mkdir_p!(snapshot_dir)

    case File.ls(shard_data_path) do
      {:ok, files} ->
        Enum.each(files, fn file ->
          source = Path.join(shard_data_path, file)
          dest = Path.join(snapshot_dir, file)

          if File.dir?(source) do
            create_hardlink_snapshot(source, dest)
          else
            File.ln!(source, dest)
          end
        end)

      {:error, :enoent} ->
        :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Tar creation
  # ---------------------------------------------------------------------------

  @spec create_tar(binary(), binary()) :: :ok
  defp create_tar(source_dir, tar_path) do
    # Use Erlang's :erl_tar for portability
    files =
      source_dir
      |> list_all_files()
      |> Enum.map(fn abs_path ->
        rel = Path.relative_to(abs_path, source_dir)
        {String.to_charlist(rel), String.to_charlist(abs_path)}
      end)

    :ok = :erl_tar.create(String.to_charlist(tar_path), files, [:compressed])
  end

  @spec list_all_files(binary()) :: [binary()]
  defp list_all_files(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn entry ->
          full = Path.join(dir, entry)

          if File.dir?(full) do
            list_all_files(full)
          else
            [full]
          end
        end)

      {:error, _} ->
        []
    end
  end

  @spec list_snapshot_files(binary()) :: [map()]
  defp list_snapshot_files(dir) do
    list_all_files(dir)
    |> Enum.map(fn path ->
      %{
        name: Path.relative_to(path, dir),
        size: File.stat!(path).size,
        sha256: sha256_file(path)
      }
    end)
  end

  @spec sha256_file(binary()) :: binary()
  defp sha256_file(path) do
    path
    |> File.stream!([], 65_536)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  @spec get_raft_index(:ra.server_id()) :: non_neg_integer()
  defp get_raft_index(server_id) do
    case :ra.member_overview(server_id) do
      {:ok, overview, _} -> Map.get(overview, :commit_index, 0)
      _ -> 0
    end
  end
end
