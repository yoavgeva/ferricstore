import Config

# Use port 0 in test so the OS assigns an ephemeral port.
# Tests that need the port call Application.get_env(:ferricstore, :port).
config :ferricstore, :port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_test_#{:os.getpid()}"

# Use a shorter sync-flush timeout in tests so that await_in_flight fails fast
# (1 second) rather than blocking for 5 seconds. The Router call timeout is
# set to 10 seconds, so there is plenty of headroom even on slow CI runners.
config :ferricstore, :sync_flush_timeout_ms, 1_000

# MemoryGuard: use large budget and slow interval to avoid noise in tests
config :ferricstore, :max_memory_bytes, 1_073_741_824
config :ferricstore, :eviction_policy, :volatile_lru
config :ferricstore, :memory_guard_interval_ms, 5_000

# Raft: disable by default in test (Raft tests manage their own ra lifecycle)
config :ferricstore, :raft_enabled, false
