# :perf — slow throughput tests; run with `mix test --include perf`
# :linux_io_uring — tests that require Linux + io_uring kernel support (≥ 5.1).
#   Always excluded from the default run (`mix test`) on all platforms.
#   In CI, the dedicated workflow step runs them with `mix test --only linux_io_uring`
#   only when io_uring is confirmed available on the runner.
#   Locally on Linux, run them with `mix test --include linux_io_uring`.
# :large_alloc — tests that allocate ≥ 512 MiB in-process to verify the size
#   guard fires. Excluded by default to avoid OOM on CI runners with < 2 GB RAM.
#   Run locally with `mix test --include large_alloc`.
ExUnit.start(exclude: [:perf, :linux_io_uring, :large_alloc, :pending_nif_rebuild, :pending_memory_guard_timing, :pending_cross_type_wrongtype, :pending_raft_startup])

# Clean up the test data directory after the suite finishes.
# Each run uses a unique dir (ferricstore_test_<pid>) so parallel runs don't
# collide, but without cleanup they accumulate on disk indefinitely.
# ExUnit.after_suite runs before the BEAM exits, while the application is still
# running and the shard NIF file handles are still open — File.rm_rf works
# on macOS/Linux even with open handles (unlink-while-open semantics).
data_dir = Application.fetch_env!(:ferricstore, :data_dir)
ExUnit.after_suite(fn _result -> File.rm_rf(data_dir) end)
