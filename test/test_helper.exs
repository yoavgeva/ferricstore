# :perf — slow throughput tests; run with `mix test --include perf`
# :linux_io_uring — tests that require Linux + io_uring kernel support (≥ 5.1).
#   Always excluded from the default run (`mix test`) on all platforms.
#   In CI, the dedicated workflow step runs them with `mix test --only linux_io_uring`
#   only when io_uring is confirmed available on the runner.
#   Locally on Linux, run them with `mix test --include linux_io_uring`.
ExUnit.start(exclude: [:perf, :linux_io_uring])
