import Config

# Use port 0 in test so the OS assigns an ephemeral port.
# Tests that need the port call Application.get_env(:ferricstore, :port).
config :ferricstore, :port, 0
config :ferricstore, :health_port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_test_#{:os.getpid()}"

# Use a shorter sync-flush timeout in tests so that await_in_flight fails fast
# (1 second) rather than blocking for 5 seconds. The Router call timeout is
# set to 10 seconds, so there is plenty of headroom even on slow CI runners.
config :ferricstore, :sync_flush_timeout_ms, 1_000

# MemoryGuard: use large budget and slow interval to avoid noise in tests
config :ferricstore, :max_memory_bytes, 1_073_741_824
config :ferricstore, :eviction_policy, :volatile_lru
config :ferricstore, :memory_guard_interval_ms, 5_000

# Merge: use a very long check interval to prevent periodic merge timers
# from firing during tests. Tests that need to trigger merges use
# Scheduler.trigger_check/1 explicitly.
config :ferricstore, :merge,
  check_interval_ms: 600_000,
  fragmentation_threshold: 0.99

# Expiry sweep: use a very long interval to prevent periodic timers from
# firing during tests. Tests that need expiry sweeps call
# GenServer.call(shard, :expiry_sweep) explicitly.
config :ferricstore, :expiry_sweep_interval_ms, 600_000

# Disable read sampling in tests so stats/LFU counts are deterministic.
# Benchmarks should override this with @tag read_sample_rate: 100.
config :ferricstore, :read_sample_rate, 1

# Disable libcluster in test -- no multicast or node discovery during tests.
# NOTE: We must use :disabled (not []) because Config.Reader.merge deep-merges
# keyword lists, so [] gets merged with the parent config's keyword list and
# the parent values survive. Using a non-keyword-list atom forces a full replace.
config :libcluster, topologies: :disabled
config :ferricstore, :sandbox_enabled, true

# FerricstoreEcto test repo (SQLite3 in-memory with shared cache)
# pool_size: 5 to allow concurrent test access; journal_mode: wal for
# concurrent reads during writes.
config :ferricstore_ecto, FerricstoreEcto.TestRepo,
  database: "file:ferricstore_ecto_test?mode=memory&cache=shared",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 5,
  log: false
