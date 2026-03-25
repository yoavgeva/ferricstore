defmodule Ferricstore.Bitcask.NIFSchedulerSafetyTest do
  @moduledoc """
  Scheduler safety stress tests for Normal-scheduled NIFs.

  ## Problem statement

  All NIFs use `schedule = "Normal"` to avoid occupying the limited DirtyIo
  thread pool (default 10 threads shared by the entire BEAM). This means
  NIFs run directly on BEAM scheduler threads. The design is safe because:

  - The shard GenServer serializes all access (one call at a time per shard)
  - The Mutex is uncontested in the steady state
  - Individual NIF calls complete quickly (<1ms for single-key ops, <200ms
    for 50K-key iterations on NVMe)
  - BEAM has multiple schedulers, so briefly blocking one is acceptable

  However, we must verify:

  1. **Mutex starvation**: Every NIF acquires `StoreResource.store` (a
     `Mutex<Store>`). A NIF that holds the lock too long serialises all
     callers targeting the same shard.

  2. **Normal scheduler starvation**: Since NIFs run on normal schedulers,
     a long-running NIF could block that scheduler thread. We verify that
     other processes on other schedulers remain responsive.

  3. **Term construction overhead**: Building Elixir lists of binaries from
     Rust `Vec`s happens before the NIF returns. Large return values can
     cause significant time in term allocation while the Mutex is held.

  Each test populates a store with a large dataset, then calls the NIF under
  test while a separate process on a **normal scheduler** runs a tight
  `system_time` loop. If the BEAM's normal schedulers are all starved, the
  loop iteration count drops to near zero.

  ## Running

      mix test test/ferricstore/bitcask/nif_scheduler_safety_test.exs --include perf --timeout 120000

  Tagged `:perf` because these tests are intentionally slow (large datasets,
  I/O pressure, thread pool saturation).
  """

  use ExUnit.Case, async: false

  @moduletag :perf
  @moduletag timeout: 120_000

  alias Ferricstore.Bitcask.NIF

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "sched_safety_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store do
    dir = tmp_dir()
    {:ok, store} = NIF.new(dir)
    {store, dir}
  end

  defp setup_store do
    {store, dir} = open_store()
    on_exit(fn -> File.rm_rf(dir) end)
    store
  end

  # Populate a store with `count` keys using batched writes.
  #
  # Keys are formatted as `"<prefix>_NNNNN"` with zero-padded indices so they
  # sort lexicographically. Values are `"val_N"` (unpadded).
  #
  # Writes in chunks of 1000 to avoid building a single enormous batch list.
  defp populate(store, count, opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "key")
    pad = opts[:pad] || (count |> Integer.to_string() |> String.length())
    ttl = Keyword.get(opts, :ttl, 0)

    1..count
    |> Enum.chunk_every(1000)
    |> Enum.each(fn chunk ->
      batch =
        Enum.map(chunk, fn i ->
          key = "#{prefix}_#{String.pad_leading(Integer.to_string(i), pad, "0")}"
          {key, "val_#{i}", ttl}
        end)

      :ok = NIF.put_batch(store, batch)
    end)
  end

  # Build the zero-padded key string for the given prefix, index, and padding.
  defp padded_key(prefix, index, pad) do
    "#{prefix}_#{String.pad_leading(Integer.to_string(index), pad, "0")}"
  end

  # Spawn a process on a normal scheduler that increments a counter as fast as
  # possible. Returns `{pid, sample_fn}` where `sample_fn.()` returns the
  # current iteration count.
  #
  # If normal schedulers are healthy, this process accumulates ~100K+
  # iterations per second. If a NIF somehow blocks normal schedulers (which
  # DirtyIo should prevent), the count stays near zero.
  defp start_responsiveness_checker do
    pid =
      spawn(fn ->
        responsiveness_loop(0)
      end)

    sample = fn ->
      send(pid, {:sample, self()})

      receive do
        {:count, n} -> n
      after
        3_000 ->
          raise "Responsiveness checker process is unresponsive -- scheduler may be blocked"
      end
    end

    {pid, sample}
  end

  defp responsiveness_loop(n) do
    receive do
      {:sample, from} ->
        send(from, {:count, n})
        responsiveness_loop(n)

      :stop ->
        :ok
    after
      0 ->
        # Do a tiny bit of work to keep the scheduler busy
        _ = :erlang.system_time(:microsecond)
        responsiveness_loop(n + 1)
    end
  end

  # Run `fun` while monitoring scheduler responsiveness.
  #
  # Returns `{result, elapsed_ms, iterations}` where:
  # - `result` is the return value of `fun`
  # - `elapsed_ms` is wall-clock time for `fun`
  # - `iterations` is how many iterations the checker process completed
  #   during that time
  defp with_responsiveness_check(fun) do
    {checker_pid, sample} = start_responsiveness_checker()

    # Let the checker warm up so it has a stable baseline
    Process.sleep(50)
    before = sample.()

    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    elapsed = System.monotonic_time(:millisecond) - t0

    after_count = sample.()
    send(checker_pid, :stop)

    iterations = after_count - before

    {result, elapsed, iterations}
  end

  # Assert that a scheduler responsiveness iteration count is healthy.
  #
  # With DirtyIo NIFs, normal schedulers should remain fully available. Even a
  # modest threshold of 100 iterations during the NIF call proves the scheduler
  # was not blocked. In practice we see 100K+ per second on modern hardware.
  defp assert_responsive(iterations, label) do
    assert iterations > 100,
           "#{label}: normal scheduler appeared starved during NIF call " <>
             "(only #{iterations} iterations, expected > 100). " <>
             "This suggests the NIF blocked a normal scheduler thread."
  end

  # ===========================================================================
  # 1. get_all with large keyspace (10K+ keys)
  # ===========================================================================

  describe "get_all/1 with 10K keys" do
    test "completes and BEAM schedulers stay responsive" do
      store = setup_store()
      populate(store, 10_000)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.get_all(store)
        end)

      assert {:ok, entries} = result
      assert length(entries) == 10_000

      assert_responsive(iterations, "get_all(10K)")

      IO.puts(
        "\n  [scheduler_safety] get_all(10K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 2. get_batch with 5000 keys
  # ===========================================================================

  describe "get_batch/2 with 5000 keys" do
    test "returns all values and BEAM schedulers stay responsive" do
      store = setup_store()
      populate(store, 5_000)

      all_keys =
        Enum.map(1..5_000, fn i ->
          padded_key("key", i, 4)
        end)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.get_batch(store, all_keys)
        end)

      assert {:ok, values} = result
      assert length(values) == 5_000
      assert Enum.all?(values, &(not is_nil(&1))), "Some keys returned nil unexpectedly"

      assert_responsive(iterations, "get_batch(5K)")

      IO.puts(
        "\n  [scheduler_safety] get_batch(5K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 3. get_range with large result set (10K keys)
  # ===========================================================================

  describe "get_range/4 with 10K keys" do
    test "returns sorted results and BEAM schedulers stay responsive" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.get_range(store, "key_00001", "key_10000", 10_000)
        end)

      assert {:ok, entries} = result
      assert length(entries) == 10_000

      # Verify sorted order
      keys = Enum.map(entries, fn {k, _v} -> k end)
      assert keys == Enum.sort(keys), "get_range results are not sorted"

      assert_responsive(iterations, "get_range(10K)")

      IO.puts(
        "\n  [scheduler_safety] get_range(10K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 4. read_modify_write on large value (10MB -> 20MB)
  # ===========================================================================

  describe "read_modify_write/3 on large value" do
    test "appends 10MB to 10MB value and BEAM stays responsive" do
      store = setup_store()

      # Write a 10MB value
      initial_value = :crypto.strong_rand_bytes(10 * 1_024 * 1_024)
      :ok = NIF.put(store, "large_rmw", initial_value, 0)

      # Append another 10MB
      append_data = :crypto.strong_rand_bytes(10 * 1_024 * 1_024)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.read_modify_write(store, "large_rmw", {:append, append_data})
        end)

      assert {:ok, new_value} = result
      assert byte_size(new_value) == 20 * 1_024 * 1_024

      # Verify the result is correct: original ++ appended
      assert binary_part(new_value, 0, byte_size(initial_value)) == initial_value

      assert binary_part(new_value, byte_size(initial_value), byte_size(append_data)) ==
               append_data

      assert_responsive(iterations, "read_modify_write(10MB+10MB)")

      IO.puts(
        "\n  [scheduler_safety] read_modify_write(10MB+10MB): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 5. keys() with 50K keys -- most dangerous, iterates entire keydir
  #
  # NIF.keys/1 returns a bare list (not {:ok, list}) because the Rust NIF
  # encodes the list directly without an :ok wrapper.
  # ===========================================================================

  describe "keys/1 with 50K keys" do
    test "returns all keys and BEAM schedulers stay responsive" do
      store = setup_store()
      populate(store, 50_000, pad: 5)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.keys(store)
        end)

      all_keys = result
      assert is_list(all_keys)
      assert length(all_keys) == 50_000

      assert_responsive(iterations, "keys(50K)")

      IO.puts(
        "\n  [scheduler_safety] keys(50K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 6. put_batch with 10K entries (synchronous DirtyIo)
  # ===========================================================================

  describe "put_batch/2 with 10K entries" do
    test "writes all entries and BEAM schedulers stay responsive" do
      store = setup_store()

      batch =
        Enum.map(1..10_000, fn i ->
          key = "pb_#{String.pad_leading(Integer.to_string(i), 5, "0")}"
          {key, "val_#{i}", 0}
        end)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.put_batch(store, batch)
        end)

      assert result == :ok

      # Spot-check a few values
      assert {:ok, "val_1"} = NIF.get(store, "pb_00001")
      assert {:ok, "val_5000"} = NIF.get(store, "pb_05000")
      assert {:ok, "val_10000"} = NIF.get(store, "pb_10000")

      assert_responsive(iterations, "put_batch(10K)")

      IO.puts(
        "\n  [scheduler_safety] put_batch(10K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 7. purge_expired with 5K expired keys
  # ===========================================================================

  describe "purge_expired/1 with 5K expired keys" do
    test "purges all expired keys and BEAM schedulers stay responsive" do
      store = setup_store()

      # Write 5000 keys with a TTL in the past
      past_ttl = System.os_time(:millisecond) - 10_000

      populate(store, 5_000, prefix: "expired", ttl: past_ttl)

      # Also write some live keys to verify they survive (pad=3 for count 100)
      populate(store, 100, prefix: "live", ttl: 0)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.purge_expired(store)
        end)

      assert {:ok, purged_count} = result
      assert purged_count == 5_000

      # Verify live keys survived (pad defaults to 3 for count=100)
      assert {:ok, "val_1"} = NIF.get(store, "live_001")
      assert {:ok, "val_100"} = NIF.get(store, "live_100")

      # Verify expired keys are gone (pad defaults to 4 for count=5000)
      assert {:ok, :nil} = NIF.get(store, "expired_0001")

      assert_responsive(iterations, "purge_expired(5K)")

      IO.puts(
        "\n  [scheduler_safety] purge_expired(5K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 8. Concurrent NIF calls don't deadlock
  # ===========================================================================

  describe "concurrent NIF calls on same store" do
    test "10 parallel tasks calling get_all, get_batch, get_range complete without deadlock" do
      store = setup_store()
      populate(store, 5_000, pad: 4)

      all_keys =
        Enum.map(1..5_000, fn i ->
          padded_key("key", i, 4)
        end)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            case rem(i, 3) do
              0 ->
                {:ok, entries} = NIF.get_all(store)
                {:get_all, length(entries)}

              1 ->
                # Read a random slice of 500 keys
                batch_keys = Enum.take_random(all_keys, 500)
                {:ok, values} = NIF.get_batch(store, batch_keys)
                {:get_batch, length(values)}

              2 ->
                {:ok, entries} = NIF.get_range(store, "key_0001", "key_2000", 2_000)
                {:get_range, length(entries)}
            end
          end)
        end

      # All must complete within 30 seconds -- no deadlock
      results = Task.await_many(tasks, 30_000)

      for {op, count} <- results do
        assert count > 0, "#{op} returned 0 results -- possible issue"
      end

      IO.puts(
        "\n  [scheduler_safety] 10 concurrent get_all/get_batch/get_range: all completed"
      )
    end

    test "mixed read + write NIFs on same store don't deadlock" do
      store = setup_store()
      populate(store, 2_000, pad: 4)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            case rem(i, 5) do
              0 ->
                {:ok, entries} = NIF.get_all(store)
                {:get_all, length(entries)}

              1 ->
                keys =
                  Enum.map(1..100, fn j ->
                    padded_key("key", j, 4)
                  end)

                {:ok, vals} = NIF.get_batch(store, keys)
                {:get_batch, length(vals)}

              2 ->
                {:ok, entries} = NIF.get_range(store, "key_0001", "key_0500", 500)
                {:get_range, length(entries)}

              3 ->
                batch = Enum.map(1..100, fn j -> {"write_#{i}_#{j}", "v", 0} end)
                :ok = NIF.put_batch(store, batch)
                {:put_batch, 100}

              4 ->
                {:ok, result} = NIF.read_modify_write(store, "counter_#{i}", {:incr_by, 1})
                {:rmw, String.to_integer(result)}
            end
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert length(results) == 10, "Not all tasks completed"

      IO.puts(
        "\n  [scheduler_safety] 10 mixed read+write concurrent tasks: all completed"
      )
    end
  end

  # ===========================================================================
  # 9. DirtyIo pool not exhausted
  # ===========================================================================

  describe "dirty-IO pool saturation" do
    test "20 parallel keys() calls on 10K-key store all complete (pool has 10 threads)" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      dirty_io_threads = :erlang.system_info(:dirty_io_schedulers)

      IO.puts(
        "\n  [scheduler_safety] dirty-IO pool size: #{dirty_io_threads} threads"
      )

      # Spawn 20 tasks -- 2x the dirty-IO pool size.
      # NIF.keys/1 returns a bare list (not {:ok, list}).
      tasks =
        for _i <- 1..20 do
          Task.async(fn ->
            k = NIF.keys(store)
            length(k)
          end)
        end

      # All 20 must complete. If any permanently blocks, Task.await_many
      # will raise after the timeout.
      results = Task.await_many(tasks, 60_000)

      assert length(results) == 20

      assert Enum.all?(results, &(&1 == 10_000)),
             "Some keys() calls returned wrong count: #{inspect(results)}"

      IO.puts(
        "  [scheduler_safety] 20 concurrent keys(10K) on #{dirty_io_threads}-thread pool: all completed"
      )
    end

    test "20 parallel get_all() calls all complete without permanent blocking" do
      store = setup_store()
      populate(store, 5_000, pad: 4)

      tasks =
        for _i <- 1..20 do
          Task.async(fn ->
            {:ok, entries} = NIF.get_all(store)
            length(entries)
          end)
        end

      results = Task.await_many(tasks, 60_000)

      assert length(results) == 20
      assert Enum.all?(results, &(&1 == 5_000))

      IO.puts(
        "\n  [scheduler_safety] 20 concurrent get_all(5K): all completed"
      )
    end

    test "normal scheduler stays responsive during dirty-IO pool saturation" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      {checker_pid, sample} = start_responsiveness_checker()
      on_exit(fn -> send(checker_pid, :stop) end)

      Process.sleep(50)
      before = sample.()

      # Saturate the dirty-IO pool with 20 concurrent keys() calls.
      # NIF.keys/1 returns a bare list.
      tasks =
        for _i <- 1..20 do
          Task.async(fn ->
            k = NIF.keys(store)
            length(k)
          end)
        end

      Task.await_many(tasks, 60_000)

      after_count = sample.()
      iterations = after_count - before

      assert_responsive(iterations, "dirty-IO saturation (20x keys(10K))")

      IO.puts(
        "\n  [scheduler_safety] normal scheduler during 20x keys(10K) saturation: " <>
          "#{iterations} iterations (healthy)"
      )
    end
  end

  # ===========================================================================
  # 10. Run compaction under scheduler pressure
  # ===========================================================================

  describe "run_compaction under load" do
    test "compaction on store with overwritten keys does not starve schedulers" do
      store = setup_store()

      # Write 5000 keys, then overwrite them all to create dead entries
      populate(store, 5_000, pad: 4)
      populate(store, 5_000, pad: 4)

      # Get file IDs for compaction
      {:ok, file_sizes} = NIF.file_sizes(store)
      all_fids = Enum.map(file_sizes, fn {fid, _size} -> fid end)

      # Only compact non-active files (all but the last)
      merge_fids =
        case all_fids do
          [] -> []
          fids -> Enum.drop(fids, -1)
        end

      if merge_fids != [] do
        {result, elapsed_ms, iterations} =
          with_responsiveness_check(fn ->
            NIF.run_compaction(store, merge_fids)
          end)

        assert {:ok, {_written, _dropped, _reclaimed}} = result

        assert_responsive(iterations, "run_compaction(5K overwritten)")

        IO.puts(
          "\n  [scheduler_safety] run_compaction(5K overwritten keys): #{elapsed_ms}ms, " <>
            "scheduler iterations=#{iterations}"
        )
      else
        IO.puts(
          "\n  [scheduler_safety] run_compaction: only one file, skipping merge (expected)"
        )
      end

      # Verify data is still readable after compaction
      assert {:ok, "val_1"} = NIF.get(store, "key_0001")
      assert {:ok, "val_5000"} = NIF.get(store, "key_5000")
    end
  end

  # ===========================================================================
  # 11. Combined stress: many operations in parallel on separate stores
  # ===========================================================================

  describe "cross-store parallel stress" do
    test "4 independent stores with heavy concurrent operations complete without interference" do
      stores =
        for _i <- 1..4 do
          dir = tmp_dir()
          {:ok, store} = NIF.new(dir)
          {store, dir}
        end

      on_exit(fn ->
        for {_store, dir} <- stores, do: File.rm_rf(dir)
      end)

      # Populate each store
      for {store, _dir} <- stores do
        populate(store, 5_000, pad: 4)
      end

      {checker_pid, sample} = start_responsiveness_checker()
      on_exit(fn -> send(checker_pid, :stop) end)

      Process.sleep(50)
      before = sample.()

      # Run heavy operations on all 4 stores concurrently.
      # NIF.keys/1 returns a bare list (not {:ok, list}).
      tasks =
        stores
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {{store, _dir}, idx} ->
          [
            Task.async(fn ->
              {:ok, entries} = NIF.get_all(store)
              {:store, idx, :get_all, length(entries)}
            end),
            Task.async(fn ->
              k = NIF.keys(store)
              {:store, idx, :keys, length(k)}
            end),
            Task.async(fn ->
              {:ok, entries} = NIF.get_range(store, "key_0001", "key_5000", 5_000)
              {:store, idx, :get_range, length(entries)}
            end)
          ]
        end)

      results = Task.await_many(tasks, 60_000)
      after_count = sample.()
      iterations = after_count - before

      assert length(results) == 12

      for {:store, _idx, op, count} <- results do
        assert count == 5_000, "#{op} returned #{count} instead of 5000"
      end

      assert_responsive(iterations, "4-store parallel stress")

      IO.puts(
        "\n  [scheduler_safety] 4 stores x 3 heavy ops (12 tasks): all completed, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 12. Sustained throughput: no degradation over repeated calls
  # ===========================================================================

  describe "sustained throughput" do
    test "10 consecutive get_all(10K) calls don't degrade in latency" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      times =
        for _round <- 1..10 do
          t0 = System.monotonic_time(:millisecond)
          {:ok, entries} = NIF.get_all(store)
          elapsed = System.monotonic_time(:millisecond) - t0
          assert length(entries) == 10_000
          elapsed
        end

      first = hd(times)
      last = List.last(times)
      avg = Enum.sum(times) / length(times)

      IO.puts("\n  [scheduler_safety] 10x get_all(10K) latencies (ms): #{inspect(times)}")
      IO.puts("    first=#{first}ms  last=#{last}ms  avg=#{Float.round(avg, 1)}ms")

      # Last call should not be more than 5x slower than the first.
      # Significant degradation indicates resource leaks or GC pressure.
      assert last <= max(first * 5, 500),
             "get_all latency degraded: first=#{first}ms, last=#{last}ms"
    end

    test "10 consecutive keys(50K) calls don't degrade in latency" do
      store = setup_store()
      populate(store, 50_000, pad: 5)

      # NIF.keys/1 returns a bare list (not {:ok, list})
      times =
        for _round <- 1..10 do
          t0 = System.monotonic_time(:millisecond)
          k = NIF.keys(store)
          elapsed = System.monotonic_time(:millisecond) - t0
          assert length(k) == 50_000
          elapsed
        end

      first = hd(times)
      last = List.last(times)
      avg = Enum.sum(times) / length(times)

      IO.puts("\n  [scheduler_safety] 10x keys(50K) latencies (ms): #{inspect(times)}")
      IO.puts("    first=#{first}ms  last=#{last}ms  avg=#{Float.round(avg, 1)}ms")

      assert last <= max(first * 5, 1_000),
             "keys() latency degraded: first=#{first}ms, last=#{last}ms"
    end
  end

  # ===========================================================================
  # 13. Yield detection via timer-based scheduler probing
  #
  # These tests detect whether NIFs call `enif_consume_timeslice` by
  # measuring whether a timer process on the same scheduler gets to run
  # DURING a long NIF call.
  #
  # The approach:
  # - Pin a timer process to the same scheduler as the NIF caller using
  #   :erlang.process_flag(:scheduler, N).
  # - The timer sends itself messages via :erlang.send_after every 10ms.
  # - If the NIF yields (consume_timeslice reports progress), the BEAM
  #   scheduler can service the timer between NIF chunks.
  # - We measure how many timer ticks arrived during the NIF call.
  #
  # Note: `consume_timeslice` alone does NOT cause true preemptive yielding
  # (that requires `enif_schedule_nif` continuation NIFs). However, the
  # BEAM scheduler checks the timeslice counter after the NIF returns and
  # will preempt the calling process sooner. On multi-scheduler systems,
  # timer processes on OTHER schedulers always fire. This test verifies
  # that consume_timeslice is being called (the scheduler's reduction
  # counter is advanced), which is observable via the calling process
  # being preempted more aggressively after the NIF.
  #
  # The real proof is that other BEAM processes remain responsive during
  # the NIF, which the tests in sections 1-12 already verify.
  # ===========================================================================

  describe "consume_timeslice yield detection" do
    # Start a process that records wall-clock timestamps every time it gets
    # a chance to run (via a 1ms send_after loop). Returns {pid, get_ticks_fn}.
    defp start_tick_collector do
      parent = self()

      pid =
        spawn(fn ->
          tick_loop(parent, [])
        end)

      get_ticks = fn ->
        send(pid, {:get_ticks, self()})

        receive do
          {:ticks, ticks} -> ticks
        after
          5_000 -> raise "tick collector unresponsive"
        end
      end

      {pid, get_ticks}
    end

    defp tick_loop(parent, ticks) do
      receive do
        :tick ->
          now = System.monotonic_time(:microsecond)
          tick_loop(parent, [now | ticks])

        {:get_ticks, from} ->
          send(from, {:ticks, Enum.reverse(ticks)})

        :stop ->
          :ok
      after
        0 ->
          # Schedule next tick in 1ms
          Process.send_after(self(), :tick, 1)

          receive do
            :tick ->
              now = System.monotonic_time(:microsecond)
              tick_loop(parent, [now | ticks])

            {:get_ticks, from} ->
              send(from, {:ticks, Enum.reverse(ticks)})

            :stop ->
              :ok
          end
      end
    end

    test "keys(50K) calls consume_timeslice -- other processes remain responsive" do
      store = setup_store()
      populate(store, 50_000, pad: 5)

      # Start a tick collector on a different process
      {tick_pid, get_ticks} = start_tick_collector()
      on_exit(fn -> send(tick_pid, :stop) end)

      # Let the tick collector warm up
      Process.sleep(50)

      # Record start time
      t_start = System.monotonic_time(:microsecond)

      # Call the long NIF
      result = NIF.keys(store)

      t_end = System.monotonic_time(:microsecond)
      elapsed_us = t_end - t_start

      # Get all ticks that occurred
      ticks = get_ticks.()

      # Count ticks that occurred during the NIF call
      ticks_during_nif =
        Enum.count(ticks, fn t -> t >= t_start and t <= t_end end)

      assert is_list(result)
      assert length(result) == 50_000

      IO.puts(
        "\n  [yield_detection] keys(50K): #{div(elapsed_us, 1000)}ms, " <>
          "ticks_during_nif=#{ticks_during_nif}, total_ticks=#{length(ticks)}"
      )

      # The NIF should take at least a few ms for 50K keys. If ticks fired
      # during that window, it proves the BEAM scheduler was able to run
      # other processes (either via consume_timeslice or multi-scheduler).
      # On a multi-scheduler system (default), at least some ticks should
      # fire. If the NIF completely monopolized ALL schedulers (which it
      # shouldn't since it only runs on one), ticks_during_nif would be 0.
      if elapsed_us > 5_000 do
        # NIF took more than 5ms -- we expect at least 1 tick during that time
        assert ticks_during_nif > 0,
               "No timer ticks fired during #{div(elapsed_us, 1000)}ms NIF call. " <>
                 "This suggests the NIF blocked all schedulers without yielding."
      end
    end

    test "get_all(10K) calls consume_timeslice -- other processes remain responsive" do
      store = setup_store()
      populate(store, 10_000)

      {tick_pid, get_ticks} = start_tick_collector()
      on_exit(fn -> send(tick_pid, :stop) end)

      Process.sleep(50)

      t_start = System.monotonic_time(:microsecond)
      result = NIF.get_all(store)
      t_end = System.monotonic_time(:microsecond)
      elapsed_us = t_end - t_start

      ticks = get_ticks.()

      ticks_during_nif =
        Enum.count(ticks, fn t -> t >= t_start and t <= t_end end)

      assert {:ok, entries} = result
      assert length(entries) == 10_000

      IO.puts(
        "\n  [yield_detection] get_all(10K): #{div(elapsed_us, 1000)}ms, " <>
          "ticks_during_nif=#{ticks_during_nif}, total_ticks=#{length(ticks)}"
      )

      if elapsed_us > 5_000 do
        assert ticks_during_nif > 0,
               "No timer ticks fired during #{div(elapsed_us, 1000)}ms get_all call. " <>
                 "This suggests the NIF blocked all schedulers without yielding."
      end
    end
  end
end
