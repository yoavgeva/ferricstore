defmodule Ferricstore.Application do
  @moduledoc """
  OTP Application for FerricStore.

  Supervision tree (`:one_for_one`):

  ```
  Ferricstore.Supervisor
  ├── Ferricstore.Stats                   (global counters & run metadata)
  ├── Ferricstore.Store.ShardSupervisor   (one_for_one over N Shard GenServers)
  └── Ranch listener (Ferricstore.Server.Listener)
  ```

  `Stats` starts first so counters are available before any connection arrives.
  The `ShardSupervisor` must start **before** the Ranch listener so that the
  key-value store is ready before any client connection can arrive.

  ## Configuration (application env)

    * `:port`     - TCP port to bind (default: `6379`; test env uses `0` for ephemeral)
    * `:data_dir` - Bitcask data directory (default: `"data"`)
  """

  use Application

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:ferricstore, :port, 6379)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    raft_enabled? = Application.get_env(:ferricstore, :raft_enabled, true)

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

    children =
      [
        Ferricstore.Stats,
        Ferricstore.SlowLog,
        Ferricstore.Config,
        {Ferricstore.Store.ShardSupervisor, data_dir: data_dir}
      ] ++
        batcher_children ++
        [
          Ferricstore.PubSub,
          Ferricstore.FetchOrCompute,
          {Ferricstore.MemoryGuard, memory_guard_opts()},
          ranch_listener_spec(port)
        ]

    opts = [strategy: :one_for_one, name: Ferricstore.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # ---------------------------------------------------------------------------
  # Ranch child spec
  # ---------------------------------------------------------------------------

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
