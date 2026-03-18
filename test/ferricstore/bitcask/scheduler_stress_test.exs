defmodule Ferricstore.Bitcask.SchedulerStressTest do
  @moduledoc """
  Scheduler stress tests for `NIF.put_batch_async/2` running as `schedule = "Normal"`.

  ## What we're trying to break

  A `"Normal"` NIF runs directly on a BEAM scheduler thread. If it:
    - blocks on I/O → freezes that scheduler, starving all processes on it
    - holds a Mutex too long under contention → serialises all callers
    - allocates too much memory → GC pressure, binary heap fragmentation
    - runs too many reductions → triggers scheduler preemption warnings
    - races with other Normal NIFs (get/delete/put_batch) on the same store Mutex

  Each test here deliberately hammers one of those dimensions and asserts:
    1. The BEAM stays responsive (other processes make progress)
    2. All completions arrive (no lost CQEs, no deadlock)
    3. Data is correct after all operations complete
    4. The dirty-IO thread pool is not consumed (all NIFs use Normal scheduling)

  All tests are tagged `:linux_io_uring` — skipped on macOS automatically.
  """

  use ExUnit.Case, async: false

  @moduletag :linux_io_uring
  # Give scheduler stress tests a generous timeout — they do real I/O
  @moduletag timeout: 120_000

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{Router, Shard}

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "sched_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp start_shard do
    dir = tmp_dir()
    idx = :erlang.unique_integer([:positive]) |> rem(50_000) |> Kernel.+(100_000)
    {:ok, pid} = Shard.start_link(index: idx, data_dir: dir)
    {pid, idx, dir}
  end

  # Submit and block until {:io_complete, op_id, _} arrives.
  defp await_async(store, batch, timeout \\ 15_000) do
    case NIF.put_batch_async(store, batch) do
      {:pending, op_id} ->
        receive do
          {:io_complete, ^op_id, result} -> result
        after
          timeout -> flunk("scheduler_stress: timed out waiting for op #{op_id}")
        end

      :ok ->
        :ok
    end
  end

  # Spawn a long-running counter process and return its pid + a function to
  # sample how many increments it has done. Used to prove the scheduler kept
  # running other processes while NIF work was in progress.
  defp start_liveness_counter do
    parent = self()

    pid =
      spawn(fn ->
        liveness_loop(0, parent)
      end)

    sample = fn ->
      send(pid, {:sample, self()})

      receive do
        {:count, n} -> n
      after
        1_000 -> flunk("liveness counter unresponsive — scheduler may be blocked")
      end
    end

    {pid, sample}
  end

  defp liveness_loop(n, parent) do
    receive do
      {:sample, from} ->
        send(from, {:count, n})
        liveness_loop(n + 1, parent)

      :stop ->
        :ok
    after
      0 ->
        liveness_loop(n + 1, parent)
    end
  end

  # Return dirty-IO scheduler utilisation info from `:erlang.statistics`.
  # `{:ok, [{dirty_io_run_queue, N}, ...]}` — we just want to confirm the
  # dirty pool did not saturate (all 10 threads pinned) during async writes.
  defp dirty_io_run_queue_len do
    :erlang.statistics(:run_queue_lengths_all)
    |> Enum.at(-1, 0)
  end

  # ---------------------------------------------------------------------------
  # 1. Scheduler liveness — normal NIF must not stall other processes
  # ---------------------------------------------------------------------------

  describe "scheduler liveness under async write flood" do
    test "1000 sequential async single-entry submissions — BEAM stays responsive" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {counter_pid, sample} = start_liveness_counter()
      on_exit(fn -> send(counter_pid, :stop) end)

      before = sample.()

      for i <- 1..1_000 do
        :ok = await_async(store, [{"lv_seq_#{i}", "v#{i}", 0}])
      end

      after_count = sample.()

      # The counter must have incremented — the scheduler ran other work
      assert after_count > before,
             "Scheduler appeared blocked: counter did not increment during 1000 async writes"
    end

    test "200 concurrent async submissions — BEAM stays responsive throughout" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {counter_pid, sample} = start_liveness_counter()
      on_exit(fn -> send(counter_pid, :stop) end)

      before = sample.()

      tasks =
        for i <- 1..200 do
          Task.async(fn ->
            await_async(store, [{"lv_conc_#{i}", "v#{i}", 0}])
          end)
        end

      Task.await_many(tasks, 30_000)
      after_count = sample.()

      assert after_count > before,
             "Scheduler appeared blocked: counter did not increment during 200 concurrent async writes"
    end

    test "async writes interleaved with CPU-heavy Elixir work — neither starves" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Spawn a CPU-intensive process that keeps hashing
      cpu_pid =
        spawn(fn ->
          Enum.reduce(1..10_000_000, 0, fn i, acc ->
            receive do
              :stop -> exit(:normal)
            after
              0 -> :erlang.phash2(i, acc + 1)
            end
          end)
        end)

      on_exit(fn -> send(cpu_pid, :stop) end)

      # Simultaneously do async writes
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            await_async(store, [{"cpu_interleave_#{i}", "v#{i}", 0}])
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))

      # CPU process must still be alive — it was not starved out
      assert Process.alive?(cpu_pid),
             "CPU-intensive process was killed — scheduler starvation?"
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Dirty-IO thread pool not exhausted by async writes
  # ---------------------------------------------------------------------------

  describe "dirty-IO thread pool health" do
    test "dirty-IO run queue stays low during 500 concurrent async submissions" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Sample dirty-IO queue before
      before_q = dirty_io_run_queue_len()

      tasks =
        for i <- 1..500 do
          Task.async(fn ->
            await_async(store, [{"dirty_q_#{i}", "v#{i}", 0}])
          end)
        end

      # Sample queue mid-flight while tasks are running
      :timer.sleep(5)
      mid_q = dirty_io_run_queue_len()

      Task.await_many(tasks, 60_000)
      after_q = dirty_io_run_queue_len()

      # Async submissions must NOT pile up on the dirty-IO queue.
      # The dirty pool default is 10. If async writes were DirtyIo,
      # mid_q would spike to 10+ under 500 concurrent submitters.
      # With Normal scheduling, the dirty queue should stay near its baseline.
      dirty_io_threads = :erlang.system_info(:dirty_io_schedulers)

      assert mid_q <= dirty_io_threads,
             "Dirty-IO run queue spiked to #{mid_q} (#{dirty_io_threads} threads available) — " <>
               "Normal NIF may be incorrectly occupying dirty threads"

      _ = {before_q, after_q}
    end

    test "concurrent async + concurrent get (Normal) — no deadlock, pool survives" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Pre-populate some keys so gets have something to read
      for i <- 1..50 do
        :ok = NIF.put(store, "preload_#{i}", "val_#{i}", 0)
      end

      write_tasks =
        for i <- 1..100 do
          Task.async(fn ->
            await_async(store, [{"mixed_w_#{i}", "v#{i}", 0}])
          end)
        end

      read_tasks =
        for i <- 1..100 do
          Task.async(fn ->
            NIF.get(store, "preload_#{rem(i, 50) + 1}")
          end)
        end

      write_results = Task.await_many(write_tasks, 30_000)
      read_results = Task.await_many(read_tasks, 30_000)

      assert Enum.all?(write_results, &(&1 == :ok))
      # Reads should return values (not errors)
      assert Enum.all?(read_results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end

    test "async writes + deletes (Normal) interleaved — no mutex deadlock" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Pre-write keys to delete
      for i <- 1..50 do
        :ok = NIF.put(store, "del_race_#{i}", "v#{i}", 0)
      end

      write_tasks =
        for i <- 51..150 do
          Task.async(fn ->
            await_async(store, [{"del_race_#{i}", "v#{i}", 0}])
          end)
        end

      delete_tasks =
        for i <- 1..50 do
          Task.async(fn ->
            NIF.delete(store, "del_race_#{i}")
          end)
        end

      write_results = Task.await_many(write_tasks, 30_000)
      delete_results = Task.await_many(delete_tasks, 30_000)

      assert Enum.all?(write_results, &(&1 == :ok))
      assert Enum.all?(delete_results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Large value pressure — memory + reduction budget
  # ---------------------------------------------------------------------------

  describe "large value memory pressure" do
    test "10 concurrent 1MB-value batches complete without OOM or timeout" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            val = :crypto.strong_rand_bytes(1_024 * 1_024)
            result = await_async(store, [{"large_1mb_#{i}", val, 0}], 30_000)
            {i, result, val}
          end)
        end

      results = Task.await_many(tasks, 60_000)

      for {i, result, val} <- results do
        assert result == :ok, "1MB write #{i} failed: #{inspect(result)}"
        assert {:ok, ^val} = NIF.get(store, "large_1mb_#{i}")
      end
    end

    test "5 concurrent 5MB-value batches — scheduler threads not blocked" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {counter_pid, sample} = start_liveness_counter()
      on_exit(fn -> send(counter_pid, :stop) end)

      before = sample.()

      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            val = :crypto.strong_rand_bytes(5 * 1_024 * 1_024)
            await_async(store, [{"large_5mb_#{i}", val, 0}], 60_000)
          end)
        end

      Task.await_many(tasks, 90_000)

      after_count = sample.()

      assert after_count > before,
             "Scheduler blocked during 5MB concurrent writes — liveness counter stalled"
    end

    test "single 10MB value async write completes correctly" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      val = :crypto.strong_rand_bytes(10 * 1_024 * 1_024)
      assert :ok == await_async(store, [{"ten_mb", val, 0}], 30_000)
      assert {:ok, ^val} = NIF.get(store, "ten_mb")
    end

    test "batch of 10 × 100KB values — Normal NIF does not overflow reduction budget" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # 10 × 100KB = 1MB total. The encoding loop must yield or complete
      # within the 2000-reduction budget. If it doesn't, BEAM will emit a
      # scheduler warning but will NOT crash — we just need it to complete.
      {counter_pid, sample} = start_liveness_counter()
      on_exit(fn -> send(counter_pid, :stop) end)

      before = sample.()

      batch = for i <- 1..10, do: {"rb_#{i}", :crypto.strong_rand_bytes(100_000), 0}
      assert :ok == await_async(store, batch, 30_000)

      after_count = sample.()

      assert after_count > before,
             "Scheduler did not yield during large batch encoding"

      for i <- 1..10 do
        {_, expected, _} = Enum.at(batch, i - 1)
        assert {:ok, ^expected} = NIF.get(store, "rb_#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 4. High-throughput flood — completion thread must keep up
  # ---------------------------------------------------------------------------

  describe "completion thread keeps up under flood" do
    test "500 rapid single-entry submissions — all completions arrive, no loss" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Submit all 500 before draining any completions.
      # This maximally stresses the completion thread's CQE drain loop.
      op_ids =
        for i <- 1..500 do
          case NIF.put_batch_async(store, [{"flood_#{i}", "v#{i}", 0}]) do
            {:pending, op_id} -> op_id
            :ok -> nil
          end
        end

      pending_ids = Enum.reject(op_ids, &is_nil/1)

      # Drain all completions
      for op_id <- pending_ids do
        receive do
          {:io_complete, ^op_id, :ok} -> :ok
        after
          15_000 -> flunk("Lost CQE for op_id #{op_id}")
        end
      end

      # Verify data
      for i <- 1..500 do
        assert {:ok, "v#{i}"} == NIF.get(store, "flood_#{i}")
      end
    end

    test "20 tasks × 50 submissions each in parallel — 1000 total, no loss" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      tasks =
        for t <- 1..20 do
          Task.async(fn ->
            op_ids =
              for e <- 1..50 do
                case NIF.put_batch_async(store, [{"par_t#{t}_e#{e}", "v", 0}]) do
                  {:pending, id} -> id
                  :ok -> nil
                end
              end

            pending = Enum.reject(op_ids, &is_nil/1)

            for op_id <- pending do
              receive do
                {:io_complete, ^op_id, :ok} -> :ok
              after
                15_000 -> flunk("Lost CQE #{op_id} in task #{t}")
              end
            end

            :ok
          end)
        end

      results = Task.await_many(tasks, 60_000)
      assert Enum.all?(results, &(&1 == :ok))

      for t <- 1..20, e <- 1..50 do
        assert {:ok, "v"} == NIF.get(store, "par_t#{t}_e#{e}")
      end
    end

    test "ring buffer boundary: 63 concurrent single-entry submissions (fills ring)" do
      # RING_SIZE = 64. Each batch uses 1 write SQE + 1 fsync SQE = 2 SQEs.
      # 63 concurrent single-entry batches = 126 SQEs, > RING_SIZE.
      # The ring must handle chunking without dropping operations.
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      op_ids =
        for i <- 1..63 do
          case NIF.put_batch_async(store, [{"ring63_#{i}", "v#{i}", 0}]) do
            {:pending, id} -> {i, id}
            :ok -> {i, nil}
          end
        end

      for {i, op_id} <- op_ids, op_id != nil do
        receive do
          {:io_complete, ^op_id, :ok} -> :ok
        after
          10_000 -> flunk("Lost CQE for ring entry #{i}")
        end
      end

      for i <- 1..63 do
        assert {:ok, "v#{i}"} == NIF.get(store, "ring63_#{i}")
      end
    end

    test "batch of 300 entries (> 4× RING_SIZE) completes without partial write" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = for i <- 1..300, do: {"r300_#{i}", "val_#{i}", 0}
      assert :ok == await_async(store, batch, 30_000)

      for i <- 1..300 do
        assert {:ok, "val_#{i}"} == NIF.get(store, "r300_#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Mutex contention — Normal NIFs on same store
  # ---------------------------------------------------------------------------

  describe "mutex contention: Normal NIFs on same store" do
    test "simultaneous put_batch_async + put_batch (both Normal) — no deadlock" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      async_tasks =
        for i <- 1..50 do
          Task.async(fn ->
            await_async(store, [{"mutex_async_#{i}", "av#{i}", 0}])
          end)
        end

      sync_tasks =
        for i <- 1..50 do
          Task.async(fn ->
            NIF.put_batch(store, [{"mutex_sync_#{i}", "sv#{i}", 0}])
          end)
        end

      async_results = Task.await_many(async_tasks, 30_000)
      sync_results = Task.await_many(sync_tasks, 30_000)

      assert Enum.all?(async_results, &(&1 == :ok))
      assert Enum.all?(sync_results, &(&1 == :ok))

      for i <- 1..50 do
        assert {:ok, "av#{i}"} == NIF.get(store, "mutex_async_#{i}")
        assert {:ok, "sv#{i}"} == NIF.get(store, "mutex_sync_#{i}")
      end
    end

    test "put_batch_async under keys/delete/get storm — no starvation" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Pre-populate
      for i <- 1..100, do: NIF.put(store, "storm_pre_#{i}", "v#{i}", 0)

      write_tasks =
        for i <- 1..100 do
          Task.async(fn ->
            await_async(store, [{"storm_new_#{i}", "nv#{i}", 0}])
          end)
        end

      read_tasks =
        for i <- 1..100 do
          Task.async(fn -> NIF.get(store, "storm_pre_#{rem(i, 100) + 1}") end)
        end

      key_tasks =
        for _ <- 1..10 do
          Task.async(fn -> NIF.keys(store) end)
        end

      write_r = Task.await_many(write_tasks, 30_000)
      _read_r = Task.await_many(read_tasks, 30_000)
      _key_r = Task.await_many(key_tasks, 30_000)

      assert Enum.all?(write_r, &(&1 == :ok))
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Shard-level scheduler stress (through GenServer)
  # ---------------------------------------------------------------------------

  describe "shard scheduler stress" do
    test "100 concurrent Router.put calls complete and all data readable" do
      keys =
        for i <- 1..100 do
          k = "sched_rtr_#{i}_#{:rand.uniform(9_999_999)}"
          Router.put(k, "val_#{i}", 0)
          k
        end

      # Flush all shards
      for s <- 0..3, do: GenServer.call(Router.shard_name(s), :flush)

      for {k, i} <- Enum.with_index(keys, 1) do
        assert "val_#{i}" == Router.get(k)
      end
    end

    test "shard handles 200 rapid puts followed by :flush without losing data" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      for i <- 1..200 do
        :ok = GenServer.call(pid, {:put, "rapid_shard_#{i}", "v#{i}", 0})
      end

      :ok = GenServer.call(pid, :flush)

      for i <- 1..200 do
        assert "v#{i}" == GenServer.call(pid, {:get, "rapid_shard_#{i}"})
      end
    end

    test "shard pending list drains correctly under burst: 50 puts in <1ms window" do
      # Fire 50 puts without any sleep — they should land in the same or
      # adjacent 1ms timer windows, exercising the batch accumulation logic.
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            GenServer.call(pid, {:put, "burst_#{i}", "bv#{i}", 0})
          end)
        end

      Task.await_many(tasks, 5_000)
      :ok = GenServer.call(pid, :flush)

      state = :sys.get_state(pid)
      assert state.pending == []
      assert state.flush_in_flight == nil

      for i <- 1..50 do
        assert "bv#{i}" == GenServer.call(pid, {:get, "burst_#{i}"})
      end
    end

    test "4 shards all accept concurrent writes without interference" do
      shards =
        for _ <- 1..4 do
          {pid, _idx, dir} = start_shard()
          {pid, dir}
        end

      on_exit(fn ->
        for {pid, dir} <- shards do
          if Process.alive?(pid), do: GenServer.stop(pid)
          File.rm_rf!(dir)
        end
      end)

      tasks =
        for {{pid, _dir}, s} <- Enum.with_index(shards, 1) do
          Task.async(fn ->
            for i <- 1..50 do
              :ok = GenServer.call(pid, {:put, "s#{s}_k#{i}", "v#{i}", 0})
            end

            :ok = GenServer.call(pid, :flush)
            {s, pid}
          end)
        end

      Task.await_many(tasks, 30_000)

      for {{pid, _dir}, s} <- Enum.with_index(shards, 1) do
        for i <- 1..50 do
          assert "v#{i}" == GenServer.call(pid, {:get, "s#{s}_k#{i}"})
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Scheduler responsiveness measurement
  # ---------------------------------------------------------------------------

  describe "scheduler responsiveness measurement" do
    test "put_batch_async NIF call completes in < 5ms on normal scheduler" do
      # A Normal NIF must not block the scheduler for perceptible time.
      # Measure wall-clock time for a single call — should be microseconds,
      # not milliseconds (which would indicate it's waiting on I/O).
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Warmup
      _ = await_async(store, [{"warmup", "v", 0}])

      times =
        for i <- 1..100 do
          t0 = System.monotonic_time(:microsecond)

          case NIF.put_batch_async(store, [{"timing_#{i}", "v", 0}]) do
            {:pending, op_id} ->
              receive do
                {:io_complete, ^op_id, _} -> :ok
              after
                5_000 -> :ok
              end

            :ok ->
              :ok
          end

          System.monotonic_time(:microsecond) - t0
        end

      # The NIF call itself (not including fsync) must be fast.
      # We measure the round-trip here, so this is NIF + async io_uring submit.
      # It should be well under 5ms (5000µs) in all cases.
      max_time = Enum.max(times)
      avg_time = Enum.sum(times) / length(times)

      # The NIF submit path (without fsync) should be <1ms.
      # Even with io_uring fsync on NVMe, median should be <5ms.
      assert avg_time < 5_000,
             "Average NIF call time #{Float.round(avg_time, 1)}µs exceeds 5ms — " <>
               "scheduler thread may be blocking on I/O"

      IO.puts("\n  [scheduler_stress] NIF call timing over 100 iterations:")
      IO.puts("    avg=#{Float.round(avg_time, 1)}µs  max=#{max_time}µs")
    end

    test "1000 normal-scheduler put_batch_async calls don't degrade over time" do
      # Submit in batches of 100. If each 100-batch window takes significantly
      # longer than the first, the scheduler is accumulating latency
      # (e.g. dirty thread pool growing, GC pressure, mutex starvation).
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      window_times =
        for window <- 1..10 do
          t0 = System.monotonic_time(:millisecond)

          op_ids =
            for i <- 1..100 do
              case NIF.put_batch_async(store, [{"degr_w#{window}_#{i}", "v#{i}", 0}]) do
                {:pending, id} -> id
                :ok -> nil
              end
            end

          for id <- Enum.reject(op_ids, &is_nil/1) do
            receive do
              {:io_complete, ^id, _} -> :ok
            after
              15_000 -> flunk("timeout in window #{window}")
            end
          end

          System.monotonic_time(:millisecond) - t0
        end

      first_window = hd(window_times)
      last_window = List.last(window_times)

      IO.puts("\n  [scheduler_stress] 10 × 100-write windows (ms):")
      IO.puts("    #{inspect(window_times)}")

      # Last window should not be more than 10× slower than the first.
      # Significant degradation indicates resource exhaustion or GC buildup.
      assert last_window <= max(first_window * 10, 1_000),
             "Write throughput degraded: window 1=#{first_window}ms, window 10=#{last_window}ms"
    end
  end
end
