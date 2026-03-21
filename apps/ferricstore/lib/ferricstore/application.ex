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
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    raft_enabled? = Application.get_env(:ferricstore, :raft_enabled, true)

    Logger.info("FerricStore starting in #{mode} mode")

    # Create the on-disk directory layout (spec 2B.4) before any process
    # tries to open shard directories or Raft WALs.
    Ferricstore.DataDir.ensure_layout!(data_dir, shard_count)

    # Initialize waiter registry ETS for blocking commands
    Ferricstore.Waiters.init()
    # Initialize client tracking ETS tables
    Ferricstore.ClientTracking.init_tables()
    # Initialize stream metadata ETS tables (owned by this long-lived process)
    Ferricstore.Commands.Stream.init_tables()
    # Initialize HNSW vector index registry (used by VCREATE/VADD/VSEARCH)
    Ferricstore.Store.HnswRegistry.create_table()

    # Start the ra system before shards so that Shard.init can start ra servers.
    if raft_enabled? do
      Ferricstore.Raft.Cluster.start_system(data_dir)
    end

    batcher_children =
      if raft_enabled? do
        Enum.map(0..(shard_count - 1), fn i ->
          shard_id = Ferricstore.Raft.Cluster.shard_server_id(i)

          Supervisor.child_spec(
            {Ferricstore.Raft.Batcher,
             shard_index: i, shard_id: shard_id},
            id: :"batcher_#{i}"
          )
        end)
      else
        []
      end

    async_worker_children =
      Enum.map(0..(shard_count - 1), fn i ->
        Supervisor.child_spec(
          {Ferricstore.Raft.AsyncApplyWorker, shard_index: i},
          id: :"async_apply_worker_#{i}"
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
        [
        {Ferricstore.Store.ShardSupervisor, data_dir: data_dir}
      ] ++
        async_worker_children ++
        [
          {Ferricstore.Merge.Supervisor, data_dir: data_dir, shard_count: shard_count},
          Ferricstore.PubSub,
          Ferricstore.FetchOrCompute,
          {Ferricstore.MemoryGuard, memory_guard_opts()}
        ]

    opts = [strategy: :one_for_one, name: Ferricstore.Supervisor]
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
          %{shard_count: shard_count, port: port, raft_enabled: raft_enabled?, mode: mode}
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
    # Mark the node as not ready so Kubernetes stops routing traffic
    # before the supervision tree begins shutting down.
    Ferricstore.Health.set_ready(false)

    :telemetry.execute(
      [:ferricstore, :node, :shutdown_started],
      %{uptime_ms: System.monotonic_time(:millisecond)},
      %{}
    )

    state
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
      hot_cache = :"hot_cache_#{i}"

      try do
        :ets.foldl(
          fn {key, value}, {c, lk, ls} ->
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
          end,
          {count, largest_key, largest_size},
          hot_cache
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
end
