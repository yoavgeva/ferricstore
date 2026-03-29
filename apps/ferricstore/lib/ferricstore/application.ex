defmodule Ferricstore.Application do
  @moduledoc """
  OTP Application for the FerricStore core engine.

  Starts the core supervision tree: shards, Raft, ETS tables, merge
  schedulers, PubSub, and MemoryGuard. Network-facing children (Ranch
  TCP/TLS listener, HTTP health endpoint) are started by the separate
  `:ferricstore_server` application.

  ## Supervision tree (`:one_for_one`)

  ```
  Ferricstore.Supervisor
  ├── Ferricstore.Stats                   (global counters & run metadata)
  ├── Ferricstore.SlowLog                 (slow command log)
  ├── Ferricstore.AuditLog                (audit trail)
  ├── Ferricstore.Config                  (runtime config)
  ├── Ferricstore.NamespaceConfig         (per-namespace overrides)
  ├── Ferricstore.Acl                     (access control lists)
  ├── Ferricstore.HLC                     (Hybrid Logical Clock)
  ├── Ferricstore.Raft.Batcher (x N)     (group-commit batchers)
  ├── Ferricstore.Store.ShardSupervisor   (one_for_one over N Shard GenServers)
  ├── Ferricstore.Raft.AsyncApplyWorker (x N)
  ├── Ferricstore.Merge.Supervisor        (Semaphore + N Scheduler GenServers)
  ├── Ferricstore.PubSub
  ├── Ferricstore.FetchOrCompute
  └── Ferricstore.MemoryGuard
  ```

  `Stats` starts first so counters are available before any connection arrives.
  The `ShardSupervisor` must start **before** the Ranch listener (in the server
  app) so that the key-value store is ready before any client connection arrives.

  ## Configuration (application env)

    * `:mode`             - `:standalone` (default) or `:embedded`
    * `:port`             - TCP port to bind (default: `6379`; test env uses `0` for ephemeral)
    * `:data_dir`         - Bitcask data directory (default: `"data"`)
    * `:health_port`      - HTTP health check port (default: `4000`; test env uses `0`)
    * `:tls_port`         - TLS port to bind (default: `nil`; not started unless configured)
    * `:tls_cert_file`    - path to PEM certificate file
    * `:tls_key_file`     - path to PEM private key file
    * `:tls_ca_cert_file` - path to CA certificate bundle (optional)
    * `:require_tls`      - when `true`, reject plaintext connections (default: `false`)
  """

  use Application

  require Logger

  @default_large_value_warning_bytes 512 * 1024

  @impl true
  def start(_type, _args) do
    mode = Ferricstore.Mode.current()
    port = Application.get_env(:ferricstore, :port, 6379)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count =
      case Application.get_env(:ferricstore, :shard_count, 0) do
        0 -> System.schedulers_online()
        n when is_integer(n) and n > 0 -> n
      end
    Logger.info("FerricStore starting in #{mode} mode")

    # Create the on-disk directory layout (spec 2B.4) before any process
    # tries to open shard directories or Raft WALs.
    Ferricstore.DataDir.ensure_layout!(data_dir, shard_count)

    # Cache LFU config in persistent_term for hot-path reads (~5ns vs ~250ns).
    # Must run before any shard starts touching keys.
    Ferricstore.Store.LFU.init_config_cache()

    # Cache hot_cache_max_value_size in persistent_term for zero-overhead
    # hot-path reads. Values larger than this threshold are stored as nil
    # in ETS (cold) to avoid copying large binaries on every :ets.lookup.
    :persistent_term.put(
      :ferricstore_hot_cache_max_value_size,
      Application.get_env(:ferricstore, :hot_cache_max_value_size, 65_536)
    )

    # Initialize per-shard atomic write version counters (used by WATCH/EXEC
    # and the Shard-bypass quorum write path in Router).
    Ferricstore.Store.WriteVersion.init(shard_count)

    # Initialize MemoryGuard persistent_term flags (default: not full).
    # MemoryGuard.perform_check will update these every 100ms.
    :persistent_term.put(:ferricstore_keydir_full, false)
    :persistent_term.put(:ferricstore_reject_writes, false)

    # Initialize keyspace notification events config in persistent_term
    # (default: empty string = disabled). Updated by Config.apply_side_effect
    # when CONFIG SET notify-keyspace-events is called.
    :persistent_term.put(:ferricstore_keyspace_events, "")

    # Initialize the slot map BEFORE any shard starts. This builds
    # a uniform 1024-slot -> shard mapping and stores it in persistent_term.
    # Also sets :ferricstore_shard_count.
    Ferricstore.Store.SlotMap.init(shard_count)
    Ferricstore.Store.Router.init_shard_names(shard_count)
    :persistent_term.put(:ferricstore_promotion_threshold,
      Application.get_env(:ferricstore, :promotion_threshold, 100))
    :persistent_term.put(:ferricstore_read_sample_rate,
      Application.get_env(:ferricstore, :read_sample_rate, 100))

    # Initialize sandbox registry (only in test -- sandbox_enabled is set in config/test.exs)
    if Application.get_env(:ferricstore, :sandbox_enabled, false) do
      FerricStore.Sandbox.init_registry()
    end

    # Initialize waiter registry ETS for blocking commands
    Ferricstore.Waiters.init()
    # Initialize client tracking ETS tables
    Ferricstore.ClientTracking.init_tables()
    # Initialize stream metadata ETS tables (owned by this long-lived process)
    Ferricstore.Commands.Stream.init_tables()
    # Initialize HNSW vector index registry (used by VCREATE/VADD/VSEARCH)
    Ferricstore.Store.HnswRegistry.create_table()

    # Load the patched ra_log_wal with async fdatasync BEFORE starting
    # the ra system, so the patched module is in place when the WAL starts.
    install_patched_wal()

    # Start the ra system before shards so that Shard.init can start ra servers.
    Ferricstore.Raft.Cluster.start_system(data_dir)

    batcher_children =
      Enum.map(0..(shard_count - 1), fn i ->
        shard_id = Ferricstore.Raft.Cluster.shard_server_id(i)

        Supervisor.child_spec(
          {Ferricstore.Raft.Batcher,
           shard_index: i, shard_id: shard_id},
          id: :"batcher_#{i}"
        )
      end)

    async_worker_children =
      Enum.map(0..(shard_count - 1), fn i ->
        Supervisor.child_spec(
          {Ferricstore.Raft.AsyncApplyWorker, shard_index: i},
          id: :"async_apply_worker_#{i}"
        )
      end)

    # Background Bitcask writers — one per shard. Must start BEFORE the
    # ShardSupervisor because StateMachine.apply sends casts to these
    # processes during shard init/recovery when replaying the Raft log.
    bitcask_writer_children =
      Enum.map(0..(shard_count - 1), fn i ->
        Supervisor.child_spec(
          {Ferricstore.Store.BitcaskWriter, shard_index: i},
          id: :"bitcask_writer_#{i}"
        )
      end)

    # Optional libcluster node discovery (DNS, Kubernetes labels, or gossip).
    # When topologies are configured, Cluster.Supervisor is the first child so
    # that node discovery begins before the store is ready to serve traffic.
    # When no topologies are configured (nil or []), the supervisor is omitted.
    cluster_children = cluster_supervisor_children()

    # Core children: always started regardless of mode.
    children =
      cluster_children ++
      [
        Ferricstore.Stats,
        Ferricstore.SlowLog,
        Ferricstore.AuditLog,
        Ferricstore.Config,
        Ferricstore.NamespaceConfig,
        Ferricstore.Acl,
        Ferricstore.HLC
      ] ++
        batcher_children ++
        bitcask_writer_children ++
        [
        {Ferricstore.Store.ShardSupervisor, data_dir: data_dir, shard_count: shard_count}
      ] ++
        async_worker_children ++
        [
          {Ferricstore.Merge.Supervisor, data_dir: data_dir, shard_count: shard_count},
          Ferricstore.PubSub,
          Ferricstore.FetchOrCompute,
          {Ferricstore.MemoryGuard, memory_guard_opts()}
        ]

    {max_r, max_s} = Application.get_env(:ferricstore, :supervisor_max_restarts, {20, 10})
    opts = [strategy: :one_for_one, name: Ferricstore.Supervisor, max_restarts: max_r, max_seconds: max_s]
    result = Supervisor.start_link(children, opts)

    case result do
      {:ok, _pid} ->
        # Mark the node as ready for Kubernetes readiness probes (spec 2C.1).
        # In embedded mode, set_ready(true) is still called so that
        # Health.ready?() returns true for any code that checks it.
        Ferricstore.Health.set_ready(true)

        :telemetry.execute(
          [:ferricstore, :node, :startup_complete],
          %{duration_ms: System.monotonic_time(:millisecond)},
          %{shard_count: shard_count, port: port, mode: mode}
        )

        # Step 6 - Large value check:
        # Scan keydir for values exceeding the configured threshold.
        # Pure RAM scan -- keydir already holds value_size per entry, no disk reads.
        # Non-blocking: fires before any traffic is served so operator sees the
        # warning immediately.
        check_large_values(shard_count)

      _ ->
        :ok
    end

    result
  end

  @impl true
  def prep_stop(state) do
    t0 = System.monotonic_time(:millisecond)

    # Step 1: Mark not ready — Kubernetes stops routing traffic
    Ferricstore.Health.set_ready(false)
    Logger.info("Shutdown: marked not ready")

    :telemetry.execute(
      [:ferricstore, :node, :shutdown_started],
      %{uptime_ms: t0},
      %{}
    )

    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    # Step 2: Flush all Raft batchers — drain pending commands to Raft
    for i <- 0..(shard_count - 1) do
      try do
        Ferricstore.Raft.Batcher.flush(i)
      catch
        :exit, _ -> :ok
      end
    end
    Logger.info("Shutdown: batchers flushed")

    # Step 3: Flush all BitcaskWriters — drain deferred disk writes
    try do
      Ferricstore.Store.BitcaskWriter.flush_all(shard_count)
    catch
      :exit, _ -> :ok
    end
    Logger.info("Shutdown: BitcaskWriters flushed")

    # Step 3b: Fsync all active Bitcask log files.
    # BitcaskWriter uses v2_append_batch_nosync (data in OS page cache only).
    # Without fsync, a subsequent Process.exit(:kill) can lose unsynced data
    # on Linux (Docker overlayfs). macOS APFS retains page cache across kills.
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    for i <- 0..(shard_count - 1) do
      try do
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
        active_file_path = :persistent_term.get({:ferricstore_active_file_path, i}, nil)

        if active_file_path && File.exists?(active_file_path) do
          Ferricstore.Bitcask.NIF.v2_fsync(active_file_path)
        else
          # Fallback: fsync all log files in the shard directory
          case File.ls(shard_path) do
            {:ok, files} ->
              files
              |> Enum.filter(&String.ends_with?(&1, ".log"))
              |> Enum.each(fn f ->
                Ferricstore.Bitcask.NIF.v2_fsync(Path.join(shard_path, f))
              end)
            _ -> :ok
          end
        end
      catch
        _, _ -> :ok
      end
    end
    Logger.info("Shutdown: Bitcask files fsynced")

    # Step 4: Flush all shards — hint files + fsync
    # (terminate/1 on each shard will also do this, but doing it here
    # while the system is still healthy is more reliable)
    for i <- 0..(shard_count - 1) do
      name = Ferricstore.Store.Router.shard_name(i)
      try do
        GenServer.call(name, :flush, 5_000)
      catch
        :exit, _ -> :ok
      end
    end
    Logger.info("Shutdown: shards flushed")

    # Step 5: Force WAL rollover.
    # Triggers the WAL to close its current file and hand mem tables to the
    # segment writer. The segment writer processes asynchronously, but this
    # is safe: Bitcask data is already on disk (sync NIF in apply/3), and
    # the ra WAL is fsynced by ra itself. The rollover just helps ra compact
    # its log — not required for correctness.
    try do
      wal_name = :ra_system.derive_names(Ferricstore.Raft.Cluster.system_name()).wal
      :ra_log_wal.force_roll_over(wal_name)
    catch
      _, _ -> :ok
    end
    Logger.info("Shutdown: WAL rolled over")

    # Step 6: Check snapshot state for each shard and log warning if
    # there are many entries since last snapshot (will need replay on restart)
    for i <- 0..(shard_count - 1) do
      try do
        server_id = Ferricstore.Raft.Cluster.shard_server_id(i)

        case :ra.member_overview(server_id) do
          {:ok, overview} ->
            check_snapshot_gap(i, overview)

          {:ok, overview, _leader} ->
            check_snapshot_gap(i, overview)

          _ ->
            :ok
        end
      catch
        _, _ -> :ok
      end
    end

    elapsed = System.monotonic_time(:millisecond) - t0
    Logger.info("Shutdown: graceful flush complete in #{elapsed}ms")

    state
  end

  defp check_snapshot_gap(shard_index, overview) do
    last_applied = Map.get(overview, :last_applied, 0)
    snapshot_index = Map.get(overview, :snapshot_index, 0)
    # -1 means no snapshot ever taken
    snapshot_index = if snapshot_index == -1, do: 0, else: snapshot_index
    gap = last_applied - snapshot_index

    if gap > 5_000 do
      Logger.warning(
        "Shutdown: shard #{shard_index} has #{gap} entries since last snapshot " <>
          "(last_applied=#{last_applied}, snapshot_index=#{snapshot_index}). " <>
          "Next restart will replay these entries. Consider reducing release_cursor_interval."
      )
    else
      Logger.info("Shutdown: shard #{shard_index} snapshot gap=#{gap} (ok)")
    end
  end

  # ---------------------------------------------------------------------------
  # Large value check (Step 6)
  # ---------------------------------------------------------------------------

  @doc """
  Scans all shard ETS tables for values exceeding the configured threshold.

  Returns `{count, largest_key, largest_size}` where `count` is the number of
  entries whose value exceeds `threshold_bytes`, `largest_key` is the key with
  the largest value, and `largest_size` is its size in bytes.

  Returns `{0, nil, 0}` when no large values are found.

  This is a pure RAM scan -- ETS already holds the full value per entry, so no
  disk reads are needed.

  ## Parameters

    * `shard_count` -- number of shards to scan
    * `threshold_bytes` -- values larger than this are flagged (default:
      `Application.get_env(:ferricstore, :embedded_large_value_warning_bytes, 512 * 1024)`)

  """
  @spec scan_large_values(non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), binary() | nil, non_neg_integer()}
  def scan_large_values(shard_count, threshold_bytes \\ nil) do
    threshold =
      threshold_bytes ||
        Application.get_env(
          :ferricstore,
          :embedded_large_value_warning_bytes,
          @default_large_value_warning_bytes
        )

    Enum.reduce(0..(shard_count - 1), {0, nil, 0}, fn i, {count, largest_key, largest_size} ->
      keydir = :"keydir_#{i}"

      try do
        :ets.foldl(
          fn {key, value, _expire_at_ms, _lfu, _fid, _off, vsize}, {c, lk, ls} when is_binary(value) ->
            size = byte_size(value)

            if size > threshold do
              if size > ls do
                {c + 1, key, size}
              else
                {c + 1, lk, ls}
              end
            else
              {c, lk, ls}
            end

            {key, nil, _exp, _lfu, _fid, _off, vsize}, {c, lk, ls} when is_integer(vsize) and vsize > 0 ->
              # Cold key (value evicted from RAM) -- use vsize from disk location
              if vsize > threshold do
                if vsize > ls do
                  {c + 1, key, vsize}
                else
                  {c + 1, lk, ls}
                end
              else
                {c, lk, ls}
              end

            _entry, acc ->
              acc
          end,
          {count, largest_key, largest_size},
          keydir
        )
      rescue
        ArgumentError ->
          # ETS table does not exist (shard may be restarting).
          {count, largest_key, largest_size}
      end
    end)
  end

  # Runs the large value check and emits a warning + telemetry if any are found.
  defp check_large_values(shard_count) do
    case scan_large_values(shard_count) do
      {0, _key, _size} ->
        :ok

      {count, largest_key, largest_size} ->
        Logger.warning(
          "Embedded large value check: #{count} value(s) exceed threshold; " <>
            "largest key=#{inspect(largest_key)} (#{largest_size} bytes)"
        )

        :telemetry.execute(
          [:ferricstore, :embedded, :large_values_detected],
          %{count: count, largest_size: largest_size},
          %{largest_key: largest_key}
        )
    end
  end

  # ---------------------------------------------------------------------------
  # Cluster supervisor (libcluster)
  # ---------------------------------------------------------------------------

  # Returns a list containing the Cluster.Supervisor child spec when libcluster
  # topologies are configured, or an empty list when they are not. This makes
  # libcluster entirely optional -- the application starts cleanly without it.
  @spec cluster_supervisor_children() :: [Supervisor.child_spec()]
  defp cluster_supervisor_children do
    case Application.get_env(:libcluster, :topologies) do
      nil ->
        []

      [] ->
        []

      :disabled ->
        []

      topologies when is_list(topologies) ->
        [{Cluster.Supervisor, [topologies, [name: Ferricstore.ClusterSupervisor]]}]
    end
  end

  # ---------------------------------------------------------------------------
  # MemoryGuard options
  # ---------------------------------------------------------------------------

  defp memory_guard_opts do
    opts = []

    opts =
      case Application.get_env(:ferricstore, :max_memory_bytes) do
        nil -> opts
        val -> Keyword.put(opts, :max_memory_bytes, val)
      end

    opts =
      case Application.get_env(:ferricstore, :eviction_policy) do
        nil -> opts
        val -> Keyword.put(opts, :eviction_policy, val)
      end

    case Application.get_env(:ferricstore, :memory_guard_interval_ms) do
      nil -> opts
      val -> Keyword.put(opts, :interval_ms, val)
    end
  end

  # ---------------------------------------------------------------------------
  # Patched ra_log_wal (async fdatasync)
  # ---------------------------------------------------------------------------

  # Compiles and hot-loads a patched version of ra_log_wal that decouples
  # fdatasync from the batch processing loop. The patched module:
  #
  # 1. Writes data to the kernel buffer synchronously (fast)
  # 2. Spawns a linked process to run fdatasync asynchronously
  # 3. While fdatasync runs, keeps accepting new entries
  # 4. When fdatasync completes, notifies ALL accumulated writers
  #
  # Writers are ONLY notified AFTER fdatasync, preserving Raft durability.
  #
  # This must be called BEFORE ra_system:start/1 so the patched module is
  # loaded before the WAL process starts.
  @spec install_patched_wal() :: :ok | :error
  defp install_patched_wal do
    priv_dir = :code.priv_dir(:ferricstore)
    beam_path = Path.join(priv_dir, "patched/ra_log_wal.beam")

    if File.exists?(beam_path) do
      binary = File.read!(beam_path)
      :code.purge(:ra_log_wal)
      {:module, :ra_log_wal} = :code.load_binary(:ra_log_wal, ~c"ra_log_wal.erl", binary)
      Logger.info("Loaded patched ra_log_wal with async fdatasync")
      :ok
    else
      Logger.error(
        "Patched ra_log_wal.beam not found at #{beam_path}. " <>
          "Run `mix compile` to generate it from priv/patched/ra_log_wal.erl"
      )

      :error
    end
  end
end
