# :perf — slow throughput tests, run with --include perf
# :linux_io_uring — tests that require Linux + io_uring kernel support (≥ 5.1).
#   On macOS these are excluded automatically. On Linux they run unless
#   io_uring is unavailable (old kernel / seccomp policy), in which case
#   the NIF falls back to the sync path and the tests still pass because
#   they handle both {:pending, op_id} and :ok return values.
linux? = :os.type() == {:unix, :linux}

if linux? do
  ExUnit.start(exclude: [:perf])
else
  ExUnit.start(exclude: [:perf, :linux_io_uring])
end
