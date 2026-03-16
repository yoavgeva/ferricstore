# bench/write_backend_bench.exs
#
# Compares async io_uring writes vs synchronous fsync writes.
#
# Two paths under test:
#   sync  — NIF.put_batch/2   — blocks caller until fsync completes
#   async — NIF.put_batch_async/2 — submits SQEs and returns immediately;
#           a background thread sends {:io_complete, op_id, :ok} when fsync
#           finishes. On macOS / no io_uring it falls back to sync.
#
# Benchmark dimensions:
#   1. Single-writer throughput: one caller, varying batch sizes (1/10/100 entries).
#      Measures raw fsync overhead per batch at different write granularities.
#
#   2. Concurrent-writer throughput: N callers each writing one entry simultaneously.
#      Measures group-commit benefit — async batches N in-flight fsyncs into one.
#
# Run locally:
#   MIX_ENV=bench mix run bench/write_backend_bench.exs
#
# The benchmark prints ops/second (Benchee ips) and a comparison table.
# On Linux with io_uring, async should show significant gains especially under
# concurrent load where multiple fsyncs are batched together.

alias Ferricstore.Bitcask.NIF

bench_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()

defmodule WriteBackendBench do
  @moduledoc false

  def new_store(label) do
    dir = System.tmp_dir!() <> "/ferricstore_wbb_#{label}_#{:rand.uniform(9_999_999)}"
    File.mkdir_p!(dir)
    {:ok, store} = NIF.new(dir)
    {store, dir}
  end

  def cleanup(dir), do: File.rm_rf!(dir)

  # Await the {:io_complete, op_id, result} message for an async submission.
  # Raises on timeout or error so benchmark failures are visible.
  def await_async({:pending, op_id}, timeout \\ 15_000) do
    receive do
      {:io_complete, ^op_id, :ok} -> :ok
      {:io_complete, ^op_id, {:error, reason}} -> raise "async write failed: #{reason}"
    after
      timeout -> raise "async write timed out after #{timeout}ms (op_id=#{op_id})"
    end
  end

  def await_async(:ok, _timeout), do: :ok

  # Build a batch of `n` entries with unique keys using a counter.
  def make_batch(counter, prefix, n) do
    base = :counters.get(counter, 1)
    :counters.add(counter, 1, n)
    Enum.map(0..(n - 1), fn i -> {"#{prefix}_#{base + i}", "val_#{base + i}", 0} end)
  end
end

# ---------------------------------------------------------------------------
# Detect whether async path is available (Linux + io_uring).
# On macOS NIF.put_batch_async falls back to sync, so we note it in output.
# ---------------------------------------------------------------------------

{probe_store, probe_dir} = WriteBackendBench.new_store("probe")
async_result = NIF.put_batch_async(probe_store, [{"probe_key", "probe_val", 0}])

async_available =
  case async_result do
    {:pending, op_id} ->
      receive do
        {:io_complete, ^op_id, :ok} -> true
        _ -> false
      after
        5_000 -> false
      end

    :ok ->
      # Returned :ok directly — async fell back to sync
      false

    _ ->
      false
  end

WriteBackendBench.cleanup(probe_dir)

mode_label = if async_available, do: "io_uring async", else: "sync fallback"
IO.puts("""
=== Write Backend Benchmark: sync fsync vs async io_uring ===
Async path: #{mode_label}
Warmup: #{bench_warmup}s | Run: #{bench_time}s per scenario
""")

# ---------------------------------------------------------------------------
# Section 1: Single-writer throughput — sync vs async, varying batch size
# ---------------------------------------------------------------------------

IO.puts("--- Section 1: Single-writer throughput (1 caller) ---\n")

single_writer_scenarios =
  Enum.flat_map([1, 10, 100], fn batch_size ->
    {sync_store, sync_dir} = WriteBackendBench.new_store("single_sync_#{batch_size}")
    {async_store, async_dir} = WriteBackendBench.new_store("single_async_#{batch_size}")
    counter = :counters.new(1, [:atomics])

    sync_scenario = {
      "sync  put_batch #{String.pad_leading(to_string(batch_size), 3)} entries/call",
      {
        fn _ ->
          batch = WriteBackendBench.make_batch(counter, "s#{batch_size}", batch_size)
          :ok = NIF.put_batch(sync_store, batch)
        end,
        before_scenario: fn _ -> :ok end,
        after_scenario: fn _ ->
          WriteBackendBench.cleanup(sync_dir)
        end
      }
    }

    async_scenario = {
      "async put_batch #{String.pad_leading(to_string(batch_size), 3)} entries/call",
      {
        fn _ ->
          batch = WriteBackendBench.make_batch(counter, "a#{batch_size}", batch_size)
          NIF.put_batch_async(async_store, batch) |> WriteBackendBench.await_async()
        end,
        before_scenario: fn _ -> :ok end,
        after_scenario: fn _ ->
          WriteBackendBench.cleanup(async_dir)
        end
      }
    }

    [sync_scenario, async_scenario]
  end)
  |> Map.new()

Benchee.run(
  single_writer_scenarios,
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: "bench/output/write_backend_single_writer.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Section 2: Concurrent-writer throughput — N callers, 1 entry each
#
# Each Benchee iteration spawns N tasks that each write one entry
# asynchronously / synchronously and waits for all to complete.
# This exercises the group-commit path where multiple in-flight fsyncs
# can be coalesced by the kernel.
# ---------------------------------------------------------------------------

IO.puts("\n--- Section 2: Concurrent-writer throughput (N callers × 1 entry) ---\n")

concurrent_scenarios =
  Enum.flat_map([4, 16, 64], fn n_writers ->
    {sync_store, sync_dir} = WriteBackendBench.new_store("conc_sync_#{n_writers}")
    {async_store, async_dir} = WriteBackendBench.new_store("conc_async_#{n_writers}")
    counter = :counters.new(1, [:atomics])

    sync_scenario = {
      "sync  #{String.pad_leading(to_string(n_writers), 2)} concurrent writers",
      {
        fn _ ->
          base = :counters.get(counter, 1)
          :counters.add(counter, 1, n_writers)

          tasks =
            Enum.map(0..(n_writers - 1), fn i ->
              Task.async(fn ->
                :ok = NIF.put_batch(sync_store, [{"cs#{n_writers}_#{base + i}", "v", 0}])
              end)
            end)

          Task.await_many(tasks, 30_000)
          :ok
        end,
        before_scenario: fn _ -> :ok end,
        after_scenario: fn _ ->
          WriteBackendBench.cleanup(sync_dir)
        end
      }
    }

    async_scenario = {
      "async #{String.pad_leading(to_string(n_writers), 2)} concurrent writers",
      {
        fn _ ->
          base = :counters.get(counter, 1)
          :counters.add(counter, 1, n_writers)

          tasks =
            Enum.map(0..(n_writers - 1), fn i ->
              Task.async(fn ->
                NIF.put_batch_async(async_store, [{"ca#{n_writers}_#{base + i}", "v", 0}])
                |> WriteBackendBench.await_async()
              end)
            end)

          Task.await_many(tasks, 30_000)
          :ok
        end,
        before_scenario: fn _ -> :ok end,
        after_scenario: fn _ ->
          WriteBackendBench.cleanup(async_dir)
        end
      }
    }

    [sync_scenario, async_scenario]
  end)
  |> Map.new()

Benchee.run(
  concurrent_scenarios,
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML,
     file: "bench/output/write_backend_concurrent.html", auto_open: false}
  ]
)

IO.puts("""

=== Summary ===
On Linux with io_uring available (#{mode_label}):
- Single-writer: async eliminates fsync blocking from the caller's critical path.
  Each call returns as soon as SQEs are submitted (~μs) vs blocking for fsync (~ms).
- Concurrent: async allows N in-flight fsyncs to be coalesced by the kernel
  (group-commit), dramatically increasing write throughput under concurrent load.
""")
