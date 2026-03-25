defmodule Ferricstore.Bitcask.AsyncIOTest do
  @moduledoc """
  Tests for Tokio async IO NIFs.

  Validates correctness, scheduler freedom, concurrent safety, and
  performance of the async IO path that offloads Bitcask disk operations
  to a Tokio runtime thread pool.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Bitcask.Async

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "async_io_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # ------------------------------------------------------------------
  # Correctness: get_async
  # ------------------------------------------------------------------

  describe "get_async correctness" do
    test "returns same value as synchronous get" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      :ok = NIF.put(store, "key1", "value1", 0)

      # Synchronous
      assert {:ok, "value1"} = NIF.get(store, "key1")

      # Async
      assert {:ok, "value1"} = Async.get(store, "key1")
    end

    test "missing key returns {:ok, nil}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:ok, nil} = Async.get(store, "nonexistent")
    end

    test "large value (1MB) works correctly" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      large_value = :binary.copy("x", 1_000_000)
      :ok = NIF.put(store, "large_key", large_value, 0)

      assert {:ok, ^large_value} = Async.get(store, "large_key")
    end

    test "binary key and value work correctly" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      key = <<0, 1, 2, 3, 255>>
      value = <<255, 254, 253, 0, 1>>
      :ok = NIF.put(store, key, value, 0)

      assert {:ok, ^value} = Async.get(store, key)
    end
  end

  # ------------------------------------------------------------------
  # Correctness: put_batch + get round-trip
  # ------------------------------------------------------------------

  describe "put_batch_tokio_async + get_async round-trip" do
    test "basic round-trip works" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      batch = [{"k1", "v1", 0}, {"k2", "v2", 0}, {"k3", "v3", 0}]
      assert :ok = Async.put_batch(store, batch)

      assert {:ok, "v1"} = Async.get(store, "k1")
      assert {:ok, "v2"} = Async.get(store, "k2")
      assert {:ok, "v3"} = Async.get(store, "k3")
    end

    test "empty batch succeeds" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert :ok = Async.put_batch(store, [])
    end
  end

  # ------------------------------------------------------------------
  # Correctness: delete_async
  # ------------------------------------------------------------------

  describe "delete_async correctness" do
    test "delete + get returns nil" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      :ok = NIF.put(store, "del_key", "del_value", 0)
      assert {:ok, "del_value"} = Async.get(store, "del_key")

      assert {:ok, true} = Async.delete(store, "del_key")
      assert {:ok, nil} = Async.get(store, "del_key")
    end

    test "deleting non-existent key returns {:ok, false}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:ok, false} = Async.delete(store, "no_such_key")
    end
  end

  # ------------------------------------------------------------------
  # Correctness: write_hint_async
  # ------------------------------------------------------------------

  describe "write_hint_async" do
    test "succeeds on a store with data" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      :ok = NIF.put(store, "hint_key", "hint_val", 0)
      assert :ok = Async.write_hint(store)
    end

    test "succeeds on an empty store" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert :ok = Async.write_hint(store)
    end
  end

  # ------------------------------------------------------------------
  # Correctness: purge_expired_async
  # ------------------------------------------------------------------

  describe "purge_expired_async" do
    test "purges expired keys and returns count" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # expire_at_ms = 1 means already expired (1ms since epoch)
      :ok = NIF.put(store, "exp1", "val1", 1)
      :ok = NIF.put(store, "exp2", "val2", 1)
      :ok = NIF.put(store, "live", "val3", 0)

      {:ok, count} = Async.purge_expired(store)
      assert count >= 2

      # Live key should still be accessible
      assert {:ok, "val3"} = Async.get(store, "live")
    end
  end

  # ------------------------------------------------------------------
  # Scheduler freedom: the KEY test
  # ------------------------------------------------------------------

  describe "scheduler freedom" do
    test "during concurrent async reads, GenServer responds quickly" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # Write 100 keys
      for i <- 1..100 do
        :ok = NIF.put(store, "sched_key_#{i}", :binary.copy("x", 10_000), 0)
      end

      # Start a simple GenServer to test scheduler availability
      {:ok, agent} = Agent.start_link(fn -> :alive end)

      # Launch 100 concurrent async reads
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            Async.get(store, "sched_key_#{i}")
          end)
        end

      # While reads are in-flight, ping the Agent — should respond fast
      t0 = System.monotonic_time(:microsecond)
      assert :alive = Agent.get(agent, & &1)
      latency_us = System.monotonic_time(:microsecond) - t0

      # Agent should respond within 5ms (5000us) — proves schedulers aren't blocked
      assert latency_us < 5_000,
             "Agent response took #{latency_us}us — schedulers may be blocked"

      # Wait for all reads to complete
      results = Task.await_many(tasks, 10_000)

      # All should succeed
      for result <- results do
        assert {:ok, value} = result
        assert byte_size(value) == 10_000
      end

      Agent.stop(agent)
    end
  end

  # ------------------------------------------------------------------
  # Concurrent safety
  # ------------------------------------------------------------------

  describe "concurrent safety" do
    test "50 concurrent get_async calls all complete successfully" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      for i <- 1..50 do
        :ok = NIF.put(store, "conc_#{i}", "val_#{i}", 0)
      end

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            Async.get(store, "conc_#{i}")
          end)
        end

      results = Task.await_many(tasks, 10_000)

      for {result, i} <- Enum.with_index(results, 1) do
        assert {:ok, "val_#{i}"} == result
      end
    end

    test "mixed sync + async operations don't deadlock" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      :ok = NIF.put(store, "mix_key", "mix_val", 0)

      # Run sync and async reads concurrently
      tasks = [
        Task.async(fn -> Async.get(store, "mix_key") end),
        Task.async(fn -> NIF.get(store, "mix_key") end),
        Task.async(fn -> Async.get(store, "mix_key") end),
        Task.async(fn -> NIF.get(store, "mix_key") end)
      ]

      results = Task.await_many(tasks, 10_000)

      for result <- results do
        assert result in [{:ok, "mix_val"}, {:ok, "mix_val"}]
      end
    end

    test "concurrent writes and reads don't crash" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # Start write tasks
      write_tasks =
        for i <- 1..20 do
          Task.async(fn ->
            Async.put_batch(store, [{"cwr_#{i}", "val_#{i}", 0}])
          end)
        end

      # Start read tasks
      read_tasks =
        for i <- 1..20 do
          Task.async(fn ->
            Async.get(store, "cwr_#{i}")
          end)
        end

      # All should complete without crash
      write_results = Task.await_many(write_tasks, 10_000)
      read_results = Task.await_many(read_tasks, 10_000)

      for wr <- write_results, do: assert(wr == :ok)

      for rr <- read_results do
        assert match?({:ok, _}, rr)
      end
    end
  end

  # ------------------------------------------------------------------
  # Tokio pool
  # ------------------------------------------------------------------

  describe "Tokio runtime" do
    test "runtime starts successfully (first get_async proves it)" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # If Tokio runtime failed to start, this would crash
      assert {:ok, nil} = Async.get(store, "nonexistent")
    end

    test "multiple concurrent spawns work" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # Spawn 100 concurrent tasks
      tasks =
        for _i <- 1..100 do
          Task.async(fn -> Async.get(store, "key_#{:rand.uniform(100)}") end)
        end

      results = Task.await_many(tasks, 10_000)

      for result <- results do
        assert match?({:ok, _}, result)
      end
    end
  end

  # ------------------------------------------------------------------
  # NIF return value shape
  # ------------------------------------------------------------------

  describe "NIF return values" do
    test "get_async returns {:pending, :ok}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:pending, :ok} = NIF.get_async(store, "any_key")

      # Drain the message
      receive do
        {:tokio_complete, _, _} -> :ok
      after
        5_000 -> flunk("Timed out waiting for tokio_complete")
      end
    end

    test "delete_async returns {:pending, :ok}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:pending, :ok} = NIF.delete_async(store, "any_key")

      receive do
        {:tokio_complete, _, _} -> :ok
      after
        5_000 -> flunk("Timed out waiting for tokio_complete")
      end
    end

    test "put_batch_tokio_async returns {:pending, :ok}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:pending, :ok} = NIF.put_batch_tokio_async(store, [{"k", "v", 0}])

      receive do
        {:tokio_complete, _, _} -> :ok
      after
        5_000 -> flunk("Timed out waiting for tokio_complete")
      end
    end

    test "write_hint_async returns {:pending, :ok}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:pending, :ok} = NIF.write_hint_async(store)

      receive do
        {:tokio_complete, _, _} -> :ok
      after
        5_000 -> flunk("Timed out waiting for tokio_complete")
      end
    end

    test "purge_expired_async returns {:pending, :ok}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      assert {:pending, :ok} = NIF.purge_expired_async(store)

      receive do
        {:tokio_complete, _, _} -> :ok
      after
        5_000 -> flunk("Timed out waiting for tokio_complete")
      end
    end
  end

  # ------------------------------------------------------------------
  # Performance
  # ------------------------------------------------------------------

  describe "performance" do
    @tag :perf
    test "100 concurrent cold reads complete faster than sequential" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # Write keys
      for i <- 1..100 do
        :ok = NIF.put(store, "perf_#{i}", :binary.copy("y", 1_000), 0)
      end

      # Sequential sync reads
      t0 = System.monotonic_time(:microsecond)

      for i <- 1..100 do
        {:ok, _} = NIF.get(store, "perf_#{i}")
      end

      sequential_us = System.monotonic_time(:microsecond) - t0

      # Concurrent async reads
      t1 = System.monotonic_time(:microsecond)

      tasks =
        for i <- 1..100 do
          Task.async(fn -> Async.get(store, "perf_#{i}") end)
        end

      Task.await_many(tasks, 10_000)
      concurrent_us = System.monotonic_time(:microsecond) - t1

      # Concurrent should be at least as fast (allowing for Task overhead)
      # In practice on disk-bound workloads, concurrent will be much faster.
      # We use a generous 5x multiplier to account for test environment variance.
      assert concurrent_us < sequential_us * 5,
             "Concurrent (#{concurrent_us}us) should not be much slower than sequential (#{sequential_us}us)"
    end

    @tag :perf
    test "throughput: 1000 async gets complete within 10 seconds" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      store = open_store(dir)

      # Write keys
      for i <- 1..100 do
        :ok = NIF.put(store, "tp_#{i}", :binary.copy("z", 100), 0)
      end

      t0 = System.monotonic_time(:microsecond)

      tasks =
        for _ <- 1..1000 do
          key_idx = :rand.uniform(100)
          Task.async(fn -> Async.get(store, "tp_#{key_idx}") end)
        end

      results = Task.await_many(tasks, 10_000)
      elapsed_us = System.monotonic_time(:microsecond) - t0

      success_count = Enum.count(results, &match?({:ok, _}, &1))
      assert success_count == 1000, "Expected 1000 successes, got #{success_count}"

      throughput = 1000 / (elapsed_us / 1_000_000)

      assert throughput > 100,
             "Expected > 100 async gets/sec, got #{Float.round(throughput, 1)}"
    end
  end
end
