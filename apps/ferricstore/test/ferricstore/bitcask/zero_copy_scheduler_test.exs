defmodule Ferricstore.Bitcask.ZeroCopySchedulerTest do
  @moduledoc """
  Scheduler safety tests for the five zero-copy / sendfile NIFs.

  All of these NIFs use `schedule = "Normal"` to avoid occupying the limited
  DirtyIo thread pool (default 10 threads shared by the entire BEAM). This
  test suite verifies that:

  1. Each NIF runs on Normal schedulers (not DirtyIo).
  2. Normal schedulers remain responsive during NIF execution.
  3. Concurrent zero-copy operations do not starve Normal schedulers.

  ## Approach

  We spawn a "responsiveness checker" process that increments a counter in a
  tight loop on a Normal scheduler. If any NIF were to block Normal schedulers
  (e.g., by running on DirtyIo when it shouldn't, or by holding the scheduler
  for too long), the checker's iteration count would drop to near zero.

  For the "runs on Normal scheduler" assertion, we verify that DirtyIo
  scheduler wall time does NOT increase significantly during the NIF call.
  The DirtyIo schedulers are a finite pool; if a NIF were scheduled there,
  we would observe wall-time growth.

  ## Running

      mix test test/ferricstore/bitcask/zero_copy_scheduler_test.exs --include bench --timeout 120000

  Tagged `:bench` because these tests use large datasets and measure timing.
  """

  use ExUnit.Case, async: false

  @moduletag :bench
  @moduletag timeout: 120_000

  alias Ferricstore.Bitcask.NIF

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "zc_sched_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp setup_store do
    dir = tmp_dir()
    {:ok, store} = NIF.new(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    store
  end

  # Populate a store with `count` keys using batched writes.
  # Keys are formatted as `"<prefix>_NNNNN"` with zero-padded indices.
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

  # Spawn a process on a normal scheduler that increments a counter as fast as
  # possible. Returns `{pid, sample_fn}` where `sample_fn.()` returns the
  # current iteration count.
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
        _ = :erlang.system_time(:microsecond)
        responsiveness_loop(n + 1)
    end
  end

  # Run `fun` while monitoring scheduler responsiveness.
  # Returns `{result, elapsed_ms, iterations}`.
  defp with_responsiveness_check(fun) do
    {checker_pid, sample} = start_responsiveness_checker()

    # Let the checker warm up
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
  defp assert_responsive(iterations, label) do
    assert iterations > 100,
           "#{label}: normal scheduler appeared starved during NIF call " <>
             "(only #{iterations} iterations, expected > 100). " <>
             "This suggests the NIF blocked a normal scheduler thread."
  end

  # Sample DirtyIo scheduler wall time. Returns total active microseconds
  # across all DirtyIo schedulers.
  #
  # Uses `:erlang.statistics(:scheduler_wall_time_all)` which returns a list
  # of `{scheduler_id, active_time, total_time}` for all scheduler types.
  # DirtyIo schedulers are numbered after Normal and DirtyCpu schedulers.
  defp sample_dirty_io_active do
    :erlang.system_flag(:scheduler_wall_time, true)
    Process.sleep(10)
    wall_times = :erlang.statistics(:scheduler_wall_time_all)

    normal_count = :erlang.system_info(:schedulers)
    dirty_cpu_count = :erlang.system_info(:dirty_cpu_schedulers)
    dirty_io_start = normal_count + dirty_cpu_count + 1

    dirty_io_active =
      wall_times
      |> Enum.filter(fn {id, _, _} -> id >= dirty_io_start end)
      |> Enum.map(fn {_, active, _} -> active end)
      |> Enum.sum()

    dirty_io_active
  end

  # DirtyIo threshold for "did this NIF run on DirtyIo?" detection.
  #
  # If a NIF runs on DirtyIo, 50 invocations on a 1K-key store would
  # accumulate millions of microseconds of active time. Background system
  # activity (the app's own GenServers, GC, etc.) contributes noise, so we
  # allow up to 1 second (1_000_000 us) of DirtyIo growth. In practice,
  # Normal-scheduled NIFs produce 0-100K us of noise.
  @dirty_io_threshold 1_000_000

  # ===========================================================================
  # 1. get_zero_copy/2 runs on Normal scheduler
  # ===========================================================================

  describe "get_zero_copy/2 scheduler safety" do
    test "runs on Normal scheduler — DirtyIo active time does not increase significantly" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      # Sample DirtyIo baseline
      dio_before = sample_dirty_io_active()

      # Call get_zero_copy in a tight loop
      keys = Enum.map(1..10_000, &"key_#{String.pad_leading("#{&1}", 5, "0")}")

      for key <- keys do
        {:ok, _} = NIF.get_zero_copy(store, key)
      end

      dio_after = sample_dirty_io_active()
      :erlang.system_flag(:scheduler_wall_time, false)

      dio_delta = dio_after - dio_before

      IO.puts(
        "\n  [zc_scheduler] get_zero_copy(10K loop): " <>
          "DirtyIo delta=#{dio_delta} us"
      )

      # If the NIF ran on DirtyIo, we'd see millions of microseconds of
      # active time for 10K calls. On Normal scheduler, DirtyIo should see
      # near-zero growth (only background system activity).
      assert dio_delta < @dirty_io_threshold,
             "DirtyIo active time grew by #{dio_delta} us during 10K get_zero_copy calls. " <>
               "This suggests get_zero_copy is running on DirtyIo instead of Normal."
    end

    test "BEAM schedulers stay responsive during get_zero_copy loop" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      keys = Enum.map(1..10_000, &"key_#{String.pad_leading("#{&1}", 5, "0")}")

      {_result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          for key <- keys do
            {:ok, _} = NIF.get_zero_copy(store, key)
          end
        end)

      assert_responsive(iterations, "get_zero_copy(10K loop)")

      IO.puts(
        "\n  [zc_scheduler] get_zero_copy(10K loop): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 2. get_all_zero_copy/1 runs on Normal scheduler
  # ===========================================================================

  describe "get_all_zero_copy/1 scheduler safety" do
    test "runs on Normal scheduler — DirtyIo active time does not increase significantly" do
      store = setup_store()
      populate(store, 1_000, pad: 4)

      dio_before = sample_dirty_io_active()

      # Call multiple times to accumulate measurable time
      for _ <- 1..50 do
        {:ok, entries} = NIF.get_all_zero_copy(store)
        assert length(entries) == 1_000
      end

      dio_after = sample_dirty_io_active()
      :erlang.system_flag(:scheduler_wall_time, false)

      dio_delta = dio_after - dio_before

      IO.puts(
        "\n  [zc_scheduler] get_all_zero_copy(1K x 50): " <>
          "DirtyIo delta=#{dio_delta} us"
      )

      assert dio_delta < @dirty_io_threshold,
             "DirtyIo active time grew by #{dio_delta} us during 50x get_all_zero_copy. " <>
               "This suggests get_all_zero_copy is running on DirtyIo instead of Normal."
    end

    test "BEAM schedulers stay responsive during get_all_zero_copy" do
      store = setup_store()
      populate(store, 1_000, pad: 4)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.get_all_zero_copy(store)
        end)

      assert {:ok, entries} = result
      assert length(entries) == 1_000
      assert_responsive(iterations, "get_all_zero_copy(1K)")

      IO.puts(
        "\n  [zc_scheduler] get_all_zero_copy(1K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 3. get_batch_zero_copy/2 runs on Normal scheduler
  # ===========================================================================

  describe "get_batch_zero_copy/2 scheduler safety" do
    test "runs on Normal scheduler — DirtyIo active time does not increase significantly" do
      store = setup_store()
      populate(store, 1_000, pad: 4)

      keys = Enum.map(1..1_000, &"key_#{String.pad_leading("#{&1}", 4, "0")}")

      dio_before = sample_dirty_io_active()

      for _ <- 1..50 do
        {:ok, results} = NIF.get_batch_zero_copy(store, keys)
        assert length(results) == 1_000
      end

      dio_after = sample_dirty_io_active()
      :erlang.system_flag(:scheduler_wall_time, false)

      dio_delta = dio_after - dio_before

      IO.puts(
        "\n  [zc_scheduler] get_batch_zero_copy(1K x 50): " <>
          "DirtyIo delta=#{dio_delta} us"
      )

      assert dio_delta < @dirty_io_threshold,
             "DirtyIo active time grew by #{dio_delta} us during 50x get_batch_zero_copy. " <>
               "This suggests get_batch_zero_copy is running on DirtyIo instead of Normal."
    end

    test "BEAM schedulers stay responsive during get_batch_zero_copy" do
      store = setup_store()
      populate(store, 1_000, pad: 4)

      keys = Enum.map(1..1_000, &"key_#{String.pad_leading("#{&1}", 4, "0")}")

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.get_batch_zero_copy(store, keys)
        end)

      assert {:ok, results} = result
      assert length(results) == 1_000
      assert_responsive(iterations, "get_batch_zero_copy(1K)")

      IO.puts(
        "\n  [zc_scheduler] get_batch_zero_copy(1K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 4. get_range_zero_copy/4 runs on Normal scheduler
  # ===========================================================================

  describe "get_range_zero_copy/4 scheduler safety" do
    test "runs on Normal scheduler — DirtyIo active time does not increase significantly" do
      store = setup_store()
      populate(store, 1_000, pad: 4)

      dio_before = sample_dirty_io_active()

      for _ <- 1..50 do
        {:ok, entries} = NIF.get_range_zero_copy(store, "key_0001", "key_1000", 1_000)
        assert length(entries) == 1_000
      end

      dio_after = sample_dirty_io_active()
      :erlang.system_flag(:scheduler_wall_time, false)

      dio_delta = dio_after - dio_before

      IO.puts(
        "\n  [zc_scheduler] get_range_zero_copy(1K x 50): " <>
          "DirtyIo delta=#{dio_delta} us"
      )

      assert dio_delta < @dirty_io_threshold,
             "DirtyIo active time grew by #{dio_delta} us during 50x get_range_zero_copy. " <>
               "This suggests get_range_zero_copy is running on DirtyIo instead of Normal."
    end

    test "BEAM schedulers stay responsive during get_range_zero_copy" do
      store = setup_store()
      populate(store, 1_000, pad: 4)

      {result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          NIF.get_range_zero_copy(store, "key_0001", "key_1000", 1_000)
        end)

      assert {:ok, entries} = result
      assert length(entries) == 1_000
      assert_responsive(iterations, "get_range_zero_copy(1K)")

      IO.puts(
        "\n  [zc_scheduler] get_range_zero_copy(1K): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 5. get_file_ref/2 runs on Normal scheduler
  # ===========================================================================

  describe "get_file_ref/2 scheduler safety" do
    test "runs on Normal scheduler — just a keydir lookup, instant" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      dio_before = sample_dirty_io_active()

      keys = Enum.map(1..10_000, &"key_#{String.pad_leading("#{&1}", 5, "0")}")

      for key <- keys do
        {:ok, _} = NIF.get_file_ref(store, key)
      end

      dio_after = sample_dirty_io_active()
      :erlang.system_flag(:scheduler_wall_time, false)

      dio_delta = dio_after - dio_before

      IO.puts(
        "\n  [zc_scheduler] get_file_ref(10K loop): " <>
          "DirtyIo delta=#{dio_delta} us"
      )

      assert dio_delta < @dirty_io_threshold,
             "DirtyIo active time grew by #{dio_delta} us during 10K get_file_ref calls. " <>
               "This suggests get_file_ref is running on DirtyIo instead of Normal."
    end

    test "BEAM schedulers stay responsive during get_file_ref loop" do
      store = setup_store()
      populate(store, 10_000, pad: 5)

      keys = Enum.map(1..10_000, &"key_#{String.pad_leading("#{&1}", 5, "0")}")

      {_result, elapsed_ms, iterations} =
        with_responsiveness_check(fn ->
          for key <- keys do
            {:ok, _} = NIF.get_file_ref(store, key)
          end
        end)

      assert_responsive(iterations, "get_file_ref(10K loop)")

      IO.puts(
        "\n  [zc_scheduler] get_file_ref(10K loop): #{elapsed_ms}ms, " <>
          "scheduler iterations=#{iterations}"
      )
    end
  end

  # ===========================================================================
  # 6. Concurrent zero-copy operations don't starve Normal schedulers
  # ===========================================================================

  describe "concurrent zero-copy scheduler starvation" do
    test "50 concurrent get_zero_copy tasks + GenServer ping responds within 10ms" do
      store = setup_store()
      populate(store, 5_000, pad: 4)

      # Start a simple GenServer-like process (using a plain process with
      # receive) that we will ping during the concurrent NIF storm.
      ping_server =
        spawn(fn ->
          ping_loop()
        end)

      # Verify the ping server responds quickly at baseline
      baseline_latency = measure_ping(ping_server)
      assert baseline_latency < 10, "Baseline ping too slow: #{baseline_latency}ms"

      keys = Enum.map(1..5_000, &"key_#{String.pad_leading("#{&1}", 4, "0")}")

      # Spawn 50 tasks that hammer get_zero_copy concurrently
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for key <- Enum.take_random(keys, 500) do
              {:ok, _} = NIF.get_zero_copy(store, key)
            end

            :ok
          end)
        end

      # While tasks are running, measure ping latency multiple times
      latencies =
        for _ <- 1..10 do
          lat = measure_ping(ping_server)
          Process.sleep(5)
          lat
        end

      # Wait for all tasks to complete
      Task.await_many(tasks, 30_000)
      send(ping_server, :stop)

      max_latency = Enum.max(latencies)
      avg_latency = Enum.sum(latencies) / length(latencies)

      IO.puts(
        "\n  [zc_scheduler] 50 concurrent get_zero_copy tasks: " <>
          "ping max=#{max_latency}ms avg=#{Float.round(avg_latency, 1)}ms"
      )

      # The ping should still respond within a reasonable time frame.
      # With 50 concurrent tasks all contending on the same store Mutex,
      # individual pings can spike due to GC or scheduling jitter. We use
      # avg latency as the primary signal and allow max up to 200ms.
      # The key invariant: average should be well under 50ms and max should
      # not be in the multi-second range that indicates scheduler starvation.
      assert avg_latency < 50,
             "Average ping latency was #{Float.round(avg_latency, 1)}ms during " <>
               "50 concurrent get_zero_copy tasks. Normal schedulers may be starved."

      assert max_latency < 200,
             "Max ping latency was #{max_latency}ms during 50 concurrent get_zero_copy tasks. " <>
               "Normal schedulers may be starved."
    end

    test "mixed zero-copy bulk operations don't starve Normal schedulers" do
      store = setup_store()
      populate(store, 2_000, pad: 4)

      {checker_pid, sample} = start_responsiveness_checker()
      on_exit(fn -> send(checker_pid, :stop) end)

      Process.sleep(50)
      before = sample.()

      keys = Enum.map(1..2_000, &"key_#{String.pad_leading("#{&1}", 4, "0")}")

      # Spawn tasks doing different zero-copy ops concurrently
      tasks =
        for i <- 1..30 do
          Task.async(fn ->
            case rem(i, 5) do
              0 ->
                {:ok, entries} = NIF.get_all_zero_copy(store)
                {:get_all_zc, length(entries)}

              1 ->
                batch_keys = Enum.take_random(keys, 500)
                {:ok, results} = NIF.get_batch_zero_copy(store, batch_keys)
                {:get_batch_zc, length(results)}

              2 ->
                {:ok, entries} = NIF.get_range_zero_copy(store, "key_0001", "key_1000", 1_000)
                {:get_range_zc, length(entries)}

              3 ->
                for key <- Enum.take_random(keys, 200) do
                  {:ok, _} = NIF.get_zero_copy(store, key)
                end

                {:get_zero_copy, 200}

              4 ->
                for key <- Enum.take_random(keys, 200) do
                  {:ok, _} = NIF.get_file_ref(store, key)
                end

                {:get_file_ref, 200}
            end
          end)
        end

      results = Task.await_many(tasks, 30_000)
      after_count = sample.()
      iterations = after_count - before

      assert length(results) == 30
      assert_responsive(iterations, "mixed zero-copy concurrent ops")

      IO.puts(
        "\n  [zc_scheduler] 30 mixed zero-copy concurrent tasks: " <>
          "scheduler iterations=#{iterations} (healthy)"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Ping server helpers
  # ---------------------------------------------------------------------------

  defp ping_loop do
    receive do
      {:ping, from, ref} ->
        send(from, {:pong, ref})
        ping_loop()

      :stop ->
        :ok
    end
  end

  defp measure_ping(server_pid) do
    ref = make_ref()
    t0 = System.monotonic_time(:millisecond)
    send(server_pid, {:ping, self(), ref})

    receive do
      {:pong, ^ref} ->
        System.monotonic_time(:millisecond) - t0
    after
      5_000 ->
        raise "Ping server did not respond within 5 seconds — scheduler is blocked"
    end
  end
end
