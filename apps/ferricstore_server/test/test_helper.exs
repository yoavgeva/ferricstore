# Server app test helper.
# Tags are inherited from the umbrella root run and from the core app.
ExUnit.start(exclude: [:perf, :linux_io_uring, :large_alloc, :cluster, :jepsen])

data_dir = Application.fetch_env!(:ferricstore, :data_dir)
ExUnit.after_suite(fn _result -> File.rm_rf(data_dir) end)
