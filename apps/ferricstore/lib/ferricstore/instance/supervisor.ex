defmodule FerricStore.Instance.Supervisor do
  @moduledoc """
  Per-instance supervision tree for a FerricStore instance.

  Starts all processes needed for the instance: shards, batchers,
  writers, merge schedulers, Raft system, MemoryGuard, Stats, etc.

  Each instance is fully isolated — its own ETS tables, Raft WAL,
  data directory, and process tree.
  """

  use Supervisor

  @doc """
  Starts the instance supervisor and all child processes.

  The instance context (`ctx`) must already be built via
  `FerricStore.Instance.build/2` before calling this.
  """
  @spec start_link(atom(), keyword()) :: Supervisor.on_start()
  def start_link(name, opts) do
    Supervisor.start_link(__MODULE__, {name, opts}, name: :"#{name}.Supervisor")
  end

  @impl true
  def init({name, opts}) do
    ctx = FerricStore.Instance.build(name, opts)

    # Ensure data directory layout exists
    Ferricstore.DataDir.ensure_layout!(ctx.data_dir, ctx.shard_count)

    children =
      [
        # Stats (per-instance counters)
        {Ferricstore.Stats, [name: :"#{name}.Stats", instance: ctx]},

        # MemoryGuard (per-instance)
        {Ferricstore.MemoryGuard, [
          name: :"#{name}.MemoryGuard",
          instance: ctx,
          max_memory_bytes: ctx.max_memory_bytes,
          keydir_max_ram: ctx.keydir_max_ram,
          eviction_policy: ctx.eviction_policy
        ]},

        # Shard supervisor (starts N shards)
        {Ferricstore.Store.ShardSupervisor, [name: :"#{name}.ShardSupervisor", instance: ctx]}
      ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 20, max_seconds: 10)
  end
end
