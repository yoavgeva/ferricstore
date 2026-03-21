# :perf — slow throughput tests; run with `mix test --include perf`
# :linux_io_uring — tests that require Linux + io_uring kernel support (≥ 5.1).
#   Always excluded from the default run (`mix test`) on all platforms.
#   In CI, the dedicated workflow step runs them with `mix test --only linux_io_uring`
#   only when io_uring is confirmed available on the runner.
#   Locally on Linux, run them with `mix test --include linux_io_uring`.
# :large_alloc — tests that allocate ≥ 512 MiB in-process to verify the size
#   guard fires. Excluded by default to avoid OOM on CI runners with < 2 GB RAM.
#   Run locally with `mix test --include large_alloc`.
# :cluster — multi-node `:peer` cluster tests (OTP 25+). These spin up
#   multiple BEAM nodes and are slow (~10-30s per test). Excluded by default.
#   Run with `mix test --include cluster` or
#   `mix test test/ferricstore/cluster/ --include cluster`.
# :jepsen — Jepsen-style durability and consistency tests that spin up multi-node
#   `:peer` clusters and inject faults. Excluded by default; run with
#   `mix test test/ferricstore/jepsen/ --include jepsen`.
# :bloom_nif_mmap — tests for the NIF-backed mmap Bloom filter persistence
#   feature which is not yet integrated into the pure-Elixir Bloom module.
#   Run with `mix test --include bloom_nif_mmap`.
# :legacy_hot_cache — tests that verify the old two-table (keydir + hot_cache)
#   eviction model. The codebase migrated to a single-table LFU format.
#   The replacement suite is spec/single_table_lfu_test.exs.
#   Run with `mix test --include legacy_hot_cache`.
ExUnit.start(exclude: [:perf, :linux_io_uring, :large_alloc, :cluster, :jepsen, :bloom_nif_mmap, :legacy_hot_cache])

# Clean up the test data directory after the suite finishes.
# Each run uses a unique dir (ferricstore_test_<pid>) so parallel runs don't
# collide, but without cleanup they accumulate on disk indefinitely.
# ExUnit.after_suite runs before the BEAM exits, while the application is still
# running and the shard NIF file handles are still open — File.rm_rf works
# on macOS/Linux even with open handles (unlink-while-open semantics).
data_dir = Application.fetch_env!(:ferricstore, :data_dir)
ExUnit.after_suite(fn _result -> File.rm_rf(data_dir) end)
