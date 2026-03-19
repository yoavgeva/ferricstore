defmodule Ferricstore.Application do
  @moduledoc """
  OTP Application for FerricStore.

  Supervision tree (`:one_for_one`):

  ```
  Ferricstore.Supervisor
  ├── Ferricstore.Stats                   (global counters & run metadata)
  ├── Ferricstore.Store.ShardSupervisor   (one_for_one over N Shard GenServers)
  ├── Ferricstore.Merge.Supervisor        (Semaphore + N Scheduler GenServers)
  ├── Ranch listener (Ferricstore.Server.Listener)
  ├── Ranch TLS listener (Ferricstore.Server.TlsListener) [optional]
  └── Health HTTP endpoint (Ferricstore.Health.Endpoint)
  ```

  `Stats` starts first so counters are available before any connection arrives.
  The `ShardSupervisor` must start **before** the Ranch listener so that the
  key-value store is ready before any client connection can arrive.
  The `Merge.Supervisor` starts after `ShardSupervisor` so that shards are
  available when schedulers attempt their initial fragmentation check.

  The health endpoint starts last. It returns 503 until `Health.set_ready(true)`
  is called after the supervision tree is fully started (spec 2C.1 Phase 3).

  ## Configuration (application env)

    * `:port`             - TCP port to bind (default: `6379`; test env uses `0` for ephemeral)
    * `:data_dir`         - Bitcask data directory (default: `"data"`)
    * `:health_port`      - HTTP health check port (default: `9090`; test env uses `0`)
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
    port = Application.get_env(:ferricstore, :port, 6379)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    raft_enabled? = Application.get_env(:ferricstore, :raft_enabled, true)

    # Create the on-disk directory layout (spec 2B.4) before any process
    # tries to open shard directories or Raft WALs.
    Ferricstore.DataDir.ensure_layout!(data_dir, shard_count)

    # Initialize waiter registry ETS for blocking commands
    Ferricstore.Waiters.init()
    # Initialize client tracking ETS tables
    Ferricstore.ClientTracking.init_tables()

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

    health_port = Application.get_env(:ferricstore, :health_port, 9090)

    children =
      [
        Ferricstore.Stats,
        Ferricstore.SlowLog,
        Ferricstore.AuditLog,
        Ferricstore.Config,
        Ferricstore.Acl
      ] ++
        batcher_children ++
        [
        {Ferricstore.Store.ShardSupervisor, data_dir: data_dir}
      ] ++
        [
          {Ferricstore.Merge.Supervisor, data_dir: data_dir, shard_count: shard_count},
          Ferricstore.PubSub,
          Ferricstore.FetchOrCompute,
          {Ferricstore.MemoryGuard, memory_guard_opts()},
          ranch_listener_spec(port)
        ] ++
        tls_listener_children() ++
        [
          Ferricstore.Health.Endpoint.child_spec(health_port)
        ]

    opts = [strategy: :one_for_one, name: Ferricstore.Supervisor]
    result = Supervisor.start_link(children, opts)

    case result do
      {:ok, _pid} ->
        # Mark the node as ready for Kubernetes readiness probes (spec 2C.1).
        # This must happen after the supervision tree is fully started so
        # that the /health/ready endpoint returns 200 only when all shards
        # and the TCP listener are operational.
        Ferricstore.Health.set_ready(true)

        :telemetry.execute(
          [:ferricstore, :node, :startup_complete],
          %{duration_ms: System.monotonic_time(:millisecond)},
          %{shard_count: shard_count, port: port, raft_enabled: raft_enabled?}
        )

        # Step 6 - Embedded large value check (embedded mode only):
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
      ets = :"shard_ets_#{i}"

      try do
        :ets.foldl(
          fn {key, value, _expire_at_ms}, {c, lk, ls} ->
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
          ets
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
  # TLS listener children
  # ---------------------------------------------------------------------------

  # Returns a list with the TLS Ranch listener child spec if TLS is configured,
  # or an empty list otherwise.
  defp tls_listener_children do
    tls_opts = [
      port: Application.get_env(:ferricstore, :tls_port),
      certfile: Application.get_env(:ferricstore, :tls_cert_file),
      keyfile: Application.get_env(:ferricstore, :tls_key_file),
      cacertfile: Application.get_env(:ferricstore, :tls_ca_cert_file)
    ]

    case Ferricstore.Server.TlsListener.child_spec_if_configured(tls_opts) do
      nil -> []
      spec -> [spec]
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
  # Ranch child spec
  # ---------------------------------------------------------------------------

  # Builds a Ranch TCP listener child spec.
  #
  # Ranch 2.x exposes `:ranch.child_spec/5` which returns a plain Supervisor
  # child spec suitable for embedding in any supervisor.  The listener will
  # bind to `port` (0 = ephemeral, useful in tests).
  defp ranch_listener_spec(port) do
    transport_opts = %{socket_opts: [port: port]}
    protocol_opts = %{}

    :ranch.child_spec(
      Ferricstore.Server.Listener,
      :ranch_tcp,
      transport_opts,
      Ferricstore.Server.Connection,
      protocol_opts
    )
  end
end
