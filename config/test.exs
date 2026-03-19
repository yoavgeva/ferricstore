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

# Raft: enabled in test — single node is its own quorum (per spec).
config :ferricstore, :raft_enabled, true
