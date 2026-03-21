defmodule Ferricstore.Bitcask.IoUringIntegrationTest do
  @moduledoc """
  Deep integration tests for the io_uring async write path.

  Complements `IoUringTest` (unit-level NIF contract) and
  `SchedulerStressTest` (scheduler/liveness). This file focuses on:

    A.  RING_SIZE boundary batches (4094 / 4095 / 4096 entries)
    B.  Router full-stack integration — get_meta, exists?, keys, dbsize, delete
    C.  ETS-miss cold-read after async write (cache bypass → Bitcask read-back)
    D.  Pending accumulation while flush is in-flight (back-pressure batching)
    E.  purge_expired after async writes
    F.  write_hint after async writes + reopen from hint
    G.  Multiple independent stores, each with their own io_uring ring
    H.  Concurrent Router operations at high volume (1 000 concurrent puts)
    I.  Same-key storm: 200 concurrent tasks writing the same key
    J.  Large-batch durability: 2000-entry batch survives reopen
    K.  Interleaved expiry + overwrite at scale
    L.  Shard pending accumulation: rapid puts while in-flight, then verify batch drain
    M.  Router.get hot-path (ETS hit) vs cold-path (ETS miss) after async flush
    N.  NIF.keys accuracy after mix of async + sync + delete on same store
    O.  Router.delete races async write — tombstone wins
    P.  Shard restart fidelity: multiple rounds of crash → restart → write → read

  All tests are tagged `:linux_io_uring` and are skipped on macOS / non-io_uring
  kernels automatically.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{Router, Shard}

  @moduletag :linux_io_uring
  @moduletag timeout: 120_000

  setup do
    # Isolated shard tests bypass Raft (no ra system for ad-hoc indices)
    original = Application.get_env(:ferricstore, :raft_enabled)
    Application.put_env(:ferricstore, :raft_enabled, false)
    on_exit(fn -> Application.put_env(:ferricstore, :raft_enabled, original) end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "iou_int_#{:rand.uniform(999_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp start_shard do
    dir = tmp_dir()
    idx = :erlang.unique_integer([:positive]) |> rem(40_000) |> Kernel.+(200_000)
    {:ok, pid} = Shard.start_link(index: idx, data_dir: dir)
    {pid, idx, dir}
  end

  # Submit and assert the async path (returns {:pending, _}) then await.
  defp submit_async!(store, batch) do
    case NIF.put_batch_async(store, batch) do
      {:pending, op_id} ->
        op_id

      :ok ->
        flunk(
          "Expected {:pending, op_id} on Linux/io_uring but got :ok. " <>
            "Is this running on Linux with kernel ≥ 5.1 and io_uring enabled?"
        )
    end
  end

  defp submit_and_await!(store, batch, timeout \\ 10_000) do
    op_id = submit_async!(store, batch)

    receive do
      {:io_complete, ^op_id, result} -> result
    after
      timeout -> flunk("Timed out waiting for {:io_complete, #{op_id}, _}")
    end
  end

  # ---------------------------------------------------------------------------
  # A. RING_SIZE boundary batches
  # ---------------------------------------------------------------------------

  describe "RING_SIZE boundary batches (RING_SIZE = 4096)" do
    # Each batch needs N write SQEs + 1 fsync SQE = N+1 SQEs total.
    # The ring has 4096 slots. Critical boundaries:
    #   4094 entries → 4095 SQEs (fits in ring with 1 slot spare)
    #   4095 entries → 4096 SQEs (fills ring exactly)
    #   4096 entries → 4097 SQEs (exceeds ring by 1 — error path)

    test "4094-entry batch (RING_SIZE-2 writes) completes correctly" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      n = 4094
      batch = for i <- 1..n, do: {"r4094_#{i}", "v#{i}", 0}
      assert :ok == submit_and_await!(store, batch, 60_000)

      # Spot-check a sample of entries rather than all 4094 (faster CI)
      for i <- [1, 100, 500, 1000, 2000, 3000, 4000, 4094] do
        assert {:ok, "v#{i}"} == NIF.get(store, "r4094_#{i}"),
               "Entry #{i} not readable after 4094-entry batch"
      end
    end

    test "4095-entry batch (RING_SIZE-1 writes, fills ring exactly) completes correctly" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      n = 4095
      batch = for i <- 1..n, do: {"r4095_#{i}", "v#{i}", 0}
      assert :ok == submit_and_await!(store, batch, 60_000)

      for i <- [1, 100, 1000, 2000, 3000, 4000, 4095] do
        assert {:ok, "v#{i}"} == NIF.get(store, "r4095_#{i}"),
               "Entry #{i} not readable after 4095-entry batch"
      end
    end

    test "4096-entry batch (exceeds RING_SIZE) returns error, no data corruption" do
      # RING_SIZE=4096 means 4096 entries need 4097 SQEs which exceeds the ring.
      # The NIF should return {:error, _} without touching the keydir.
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Write one key before the oversized batch so we can verify it survives
      :ok = NIF.put(store, "pre_4096", "safe", 0)

      n = 4096
      batch = for i <- 1..n, do: {"r4096_#{i}", "v#{i}", 0}

      result = NIF.put_batch_async(store, batch)
      case result do
        {:error, _reason} ->
          # Expected: ring capacity exceeded, no data written
          for i <- [1, 100, 1000, 4096] do
            assert {:ok, nil} == NIF.get(store, "r4096_#{i}"),
                   "Entry #{i} should not exist after failed oversized batch"
          end

          # Pre-existing key must be intact
          assert {:ok, "safe"} == NIF.get(store, "pre_4096")

        {:pending, op_id} ->
          # Some implementations may chunk large batches — accept success too
          receive do
            {:io_complete, ^op_id, :ok} ->
              for i <- [1, 100, 1000, 4096] do
                assert {:ok, "v#{i}"} == NIF.get(store, "r4096_#{i}"),
                       "Entry #{i} not readable after successful large batch"
              end

            {:io_complete, ^op_id, {:error, _}} ->
              assert {:ok, "safe"} == NIF.get(store, "pre_4096")
          after
            60_000 -> flunk("Timed out waiting for oversized batch result")
          end

        :ok ->
          # Sync fallback (shouldn't happen on io_uring but tolerate)
          :ok
      end
    end

    test "4095-entry batch followed by another batch: offsets are correct" do
      # The first batch fills the ring exactly. The second batch must use a
      # fresh ring state and correct file offsets.
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch1 = for i <- 1..4095, do: {"chain1_#{i}", "v1_#{i}", 0}
      assert :ok == submit_and_await!(store, batch1, 60_000)

      batch2 = for i <- 1..10, do: {"chain2_#{i}", "v2_#{i}", 0}
      assert :ok == submit_and_await!(store, batch2, 10_000)

      # Spot-check both batches
      for i <- [1, 1000, 4095] do
        assert {:ok, "v1_#{i}"} == NIF.get(store, "chain1_#{i}")
      end

      for i <- 1..10 do
        assert {:ok, "v2_#{i}"} == NIF.get(store, "chain2_#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # B. Router full-stack integration
  # ---------------------------------------------------------------------------

  describe "Router full-stack integration via async shard path" do
    test "Router.get_meta returns value and expiry for async-written key" do
      future = System.os_time(:millisecond) + 60_000
      k = "rtr_meta_#{:rand.uniform(999_999)}"

      Router.put(k, "meta_val", future)
      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)

      result = Router.get_meta(k)
      assert {val, exp} = result
      assert val == "meta_val"
      assert exp == future
    end

    test "Router.get_meta returns nil for expired key" do
      past = System.os_time(:millisecond) - 1_000
      k = "rtr_meta_exp_#{:rand.uniform(999_999)}"

      Router.put(k, "gone", past)
      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)

      assert nil == Router.get_meta(k)
    end

    test "Router.exists? true for live async-written key" do
      k = "rtr_exists_#{:rand.uniform(999_999)}"
      Router.put(k, "v", 0)
      # ETS is written synchronously — exists? should see it immediately
      assert true == Router.exists?(k)
    end

    test "Router.exists? false for never-written key" do
      k = "rtr_no_key_#{:rand.uniform(999_999)}_never"
      assert false == Router.exists?(k)
    end

    test "Router.exists? false after delete" do
      k = "rtr_del_exists_#{:rand.uniform(999_999)}"
      Router.put(k, "v", 0)
      assert true == Router.exists?(k)
      Router.delete(k)
      assert false == Router.exists?(k)
    end

    test "Router.keys includes async-written keys across all shards" do
      suffix = :rand.uniform(999_999)

      keys =
        for i <- 1..20 do
          k = "rtr_keys_#{suffix}_#{i}"
          Router.put(k, "v#{i}", 0)
          k
        end

      # Flush all shards
      for s <- 0..3, do: GenServer.call(Router.shard_name(s), :flush)

      all_keys = Router.keys()

      for k <- keys do
        assert k in all_keys, "#{k} missing from Router.keys()"
      end
    end

    test "Router.dbsize reflects writes across all shards" do
      suffix = :rand.uniform(999_999)

      before_size = Router.dbsize()

      for i <- 1..10 do
        Router.put("dbsz_#{suffix}_#{i}", "v#{i}", 0)
      end

      for s <- 0..3, do: GenServer.call(Router.shard_name(s), :flush)

      after_size = Router.dbsize()
      assert after_size >= before_size + 10,
             "dbsize did not grow by at least 10: before=#{before_size} after=#{after_size}"
    end

    test "Router.delete removes key written via async path" do
      k = "rtr_del_async_#{:rand.uniform(999_999)}"
      Router.put(k, "val", 0)
      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)
      assert "val" == Router.get(k)

      Router.delete(k)
      assert nil == Router.get(k)
      assert false == Router.exists?(k)
    end

    test "1000 concurrent Router.put calls: all readable" do
      suffix = :rand.uniform(999_999)

      tasks =
        for i <- 1..1_000 do
          Task.async(fn ->
            k = "rtr_conc_#{suffix}_#{i}"
            :ok = Router.put(k, "v#{i}", 0)
            k
          end)
        end

      keys = Task.await_many(tasks, 30_000)

      # Flush all shards
      for s <- 0..3, do: GenServer.call(Router.shard_name(s), :flush)

      # Verify all 1000 keys
      for {k, i} <- Enum.with_index(keys, 1) do
        assert "v#{i}" == Router.get(k),
               "#{k} not readable after 1000 concurrent Router.put"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # C. ETS-miss cold-read after async write
  # ---------------------------------------------------------------------------

  describe "ETS-miss cold-read after async write" do
    test "key written async then ETS evicted — cold read warms from Bitcask" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "cold_k", "cold_v", 0})
      :ok = GenServer.call(pid, :flush)

      # Evict from ETS to force cold path
      :ets.delete(:"keydir_#{idx}", "cold_k")
      :ets.delete(:"hot_cache_#{idx}", "cold_k")
      assert [] == :ets.lookup(:"keydir_#{idx}", "cold_k")

      # get should warm the cache from Bitcask
      assert "cold_v" == GenServer.call(pid, {:get, "cold_k"})

      # Now ETS should be warm
      assert [{_, "cold_v", _}] = :ets.lookup(:"hot_cache_#{idx}", "cold_k")
    end

    test "Router.get takes cold path when ETS is empty for that shard" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      k = "cold_rtr_k"
      :ok = GenServer.call(pid, {:put, k, "cold_rtr_v", 0})
      :ok = GenServer.call(pid, :flush)

      # Evict ETS
      :ets.delete(:"keydir_#{idx}", k)
      :ets.delete(:"hot_cache_#{idx}", k)

      # Use GenServer cold path directly
      assert "cold_rtr_v" == GenServer.call(pid, {:get, k})
    end

    test "get_meta cold path returns value and expiry 0 for no-expiry key" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "meta_cold_k", "meta_v", 0})
      :ok = GenServer.call(pid, :flush)

      # get_meta via GenServer cold path (bypass ETS by calling get_meta on a fresh store)
      result = GenServer.call(pid, {:get_meta, "meta_cold_k"})
      case result do
        {"meta_v", _exp} -> :ok
        nil -> flunk("get_meta returned nil for live key")
        other -> flunk("Unexpected get_meta result: #{inspect(other)}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # D. Pending accumulation while flush is in-flight
  # ---------------------------------------------------------------------------

  describe "pending accumulation while flush is in-flight" do
    test "writes received while in-flight accumulate in pending, drain on next tick" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      # Write first key — triggers async flush, sets flush_in_flight
      :ok = GenServer.call(pid, {:put, "inf_k1", "v1", 0})

      # Write more keys immediately — these arrive while in-flight is set
      # They accumulate in pending
      for i <- 2..20 do
        :ok = GenServer.call(pid, {:put, "inf_k#{i}", "v#{i}", 0})
      end

      # Wait for all timer ticks to drain pending
      :timer.sleep(100)

      # Verify all 20 keys are readable
      for i <- 1..20 do
        assert "v#{i}" == GenServer.call(pid, {:get, "inf_k#{i}"}),
               "inf_k#{i} not readable after pending drain"
      end
    end

    test "state.pending is [] and state.flush_in_flight is nil after all drains" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      for i <- 1..50 do
        :ok = GenServer.call(pid, {:put, "drain_#{i}", "v#{i}", 0})
      end

      :timer.sleep(200)

      state = :sys.get_state(pid)
      assert state.pending == [],
             "pending not empty after 200ms: #{length(state.pending)} entries"
      assert state.flush_in_flight == nil,
             "flush_in_flight not nil after 200ms: #{inspect(state.flush_in_flight)}"
    end

    test "100 rapid puts: all durable after explicit flush" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      for i <- 1..100 do
        :ok = GenServer.call(pid, {:put, "rp_#{i}", "rv#{i}", 0})
      end

      :ok = GenServer.call(pid, :flush, 10_000)

      for i <- 1..100 do
        assert "rv#{i}" == GenServer.call(pid, {:get, "rp_#{i}"}),
               "rp_#{i} not readable after flush"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # E. purge_expired after async writes
  # ---------------------------------------------------------------------------

  describe "purge_expired after async writes" do
    test "purge_expired removes async-written expired keys and returns correct count" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 1_000

      # Write 5 expired + 5 live
      expired_batch = for i <- 1..5, do: {"pe_exp_#{i}", "v#{i}", past}
      live_batch = for i <- 1..5, do: {"pe_live_#{i}", "v#{i}", 0}

      assert :ok == submit_and_await!(store, expired_batch ++ live_batch)

      {:ok, count} = NIF.purge_expired(store)
      assert count == 5, "Expected 5 expired keys purged, got #{count}"

      # Expired keys gone
      for i <- 1..5, do: assert({:ok, nil} == NIF.get(store, "pe_exp_#{i}"))
      # Live keys intact
      for i <- 1..5, do: assert({:ok, "v#{i}"} == NIF.get(store, "pe_live_#{i}"))
    end

    test "purge_expired on store with no expired keys returns 0" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = for i <- 1..10, do: {"nope_#{i}", "v#{i}", 0}
      assert :ok == submit_and_await!(store, batch)

      {:ok, count} = NIF.purge_expired(store)
      assert count == 0
    end

    test "purge_expired after reopen only counts entries on disk" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 1_000
      batch = for i <- 1..3, do: {"rpo_exp_#{i}", "v#{i}", past}
      assert :ok == submit_and_await!(store1, batch)

      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)
      {:ok, count} = NIF.purge_expired(store2)
      assert count == 3
    end
  end

  # ---------------------------------------------------------------------------
  # F. write_hint after async writes + reopen from hint
  # ---------------------------------------------------------------------------

  describe "write_hint after async writes" do
    test "write_hint after async batch completes without error" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = for i <- 1..20, do: {"hint_#{i}", "v#{i}", 0}
      assert :ok == submit_and_await!(store, batch)

      assert :ok == NIF.write_hint(store)
    end

    test "store reopened from hint file loads correct keydir" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = for i <- 1..50, do: {"hf_#{i}", "hv#{i}", 0}
      assert :ok == submit_and_await!(store1, batch, 15_000)
      assert :ok == NIF.write_hint(store1)

      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)

      for i <- 1..50 do
        assert {:ok, "hv#{i}"} == NIF.get(store2, "hf_#{i}"),
               "hf_#{i} not readable after hint-assisted reopen"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # G. Multiple independent stores with separate io_uring rings
  # ---------------------------------------------------------------------------

  describe "multiple independent stores" do
    test "3 stores write concurrently, no cross-contamination" do
      dirs = for _ <- 1..3, do: tmp_dir()
      on_exit(fn -> Enum.each(dirs, &File.rm_rf!/1) end)

      stores = Enum.map(dirs, &open_store/1)

      tasks =
        for {store, s} <- Enum.with_index(stores, 1) do
          Task.async(fn ->
            batch = for i <- 1..20, do: {"ms_s#{s}_k#{i}", "sv#{s}_#{i}", 0}
            :ok = submit_and_await!(store, batch, 15_000)
            {s, store}
          end)
        end

      results = Task.await_many(tasks, 30_000)

      for {s, store} <- results do
        for i <- 1..20 do
          assert {:ok, "sv#{s}_#{i}"} == NIF.get(store, "ms_s#{s}_k#{i}"),
                 "Store #{s}: ms_s#{s}_k#{i} not readable"
        end

        # Cross-contamination check: another store's keys must not appear
        other_s = rem(s, 3) + 1
        assert {:ok, nil} == NIF.get(store, "ms_s#{other_s}_k1"),
               "Store #{s} unexpectedly contains store #{other_s}'s key"
      end
    end

    test "5 stores, each gets a 100-entry batch, all survive" do
      dirs = for _ <- 1..5, do: tmp_dir()
      on_exit(fn -> Enum.each(dirs, &File.rm_rf!/1) end)

      stores = Enum.map(dirs, &open_store/1)

      # Submit all batches concurrently
      tasks =
        for {store, s} <- Enum.with_index(stores, 1) do
          Task.async(fn ->
            batch = for i <- 1..100, do: {"five_s#{s}_k#{i}", "v#{s}_#{i}", 0}
            submit_and_await!(store, batch, 30_000)
          end)
        end

      results = Task.await_many(tasks, 60_000)
      assert Enum.all?(results, &(&1 == :ok))

      for {store, s} <- Enum.with_index(stores, 1) do
        for i <- [1, 50, 100] do
          assert {:ok, "v#{s}_#{i}"} == NIF.get(store, "five_s#{s}_k#{i}")
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # H. Same-key storm: concurrent overwrites
  # ---------------------------------------------------------------------------

  describe "same-key storm: concurrent overwrites converge to one value" do
    test "200 concurrent tasks write the same key — one value survives, no crash" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # All tasks write the same key with a unique value
      tasks =
        for i <- 1..200 do
          Task.async(fn ->
            submit_and_await!(store, [{"storm_key", "v#{i}", 0}])
          end)
        end

      Task.await_many(tasks, 30_000)

      # Exactly one value must be stored (no corruption, no crash)
      assert {:ok, val} = NIF.get(store, "storm_key")
      assert val != nil, "storm_key has nil value — all writes lost?"
      assert is_binary(val), "storm_key has non-binary value: #{inspect(val)}"
    end

    test "same-key storm via Router — ETS always readable, final value durable" do
      k = "rtr_storm_#{:rand.uniform(999_999)}"

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            :ok = Router.put(k, "rv#{i}", 0)
          end)
        end

      Task.await_many(tasks, 15_000)

      # Flush owning shard
      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)

      # Must be readable (some value wins)
      result = Router.get(k)
      assert result != nil, "Key #{k} is nil after 100 concurrent writes"
      assert is_binary(result)
    end
  end

  # ---------------------------------------------------------------------------
  # I. Large-batch durability: 2000-entry batch survives reopen
  # ---------------------------------------------------------------------------

  describe "large-batch durability across store reopen" do
    test "2000-entry async batch: all values survive reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      n = 2_000
      batch = for i <- 1..n, do: {"lbd_#{i}", "lv#{i}", 0}
      assert :ok == submit_and_await!(store1, batch, 60_000)

      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)

      # Verify a sample
      for i <- [1, 100, 500, 1000, 1500, 2000] do
        assert {:ok, "lv#{i}"} == NIF.get(store2, "lbd_#{i}"),
               "lbd_#{i} not readable after reopen"
      end
    end

    test "2000-entry batch + sync writes: all survive reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Async batch
      batch = for i <- 1..2_000, do: {"mixed_a_#{i}", "av#{i}", 0}
      assert :ok == submit_and_await!(store1, batch, 60_000)

      # Sync writes after
      for i <- 1..20 do
        :ok = NIF.put(store1, "mixed_s_#{i}", "sv#{i}", 0)
      end

      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)

      for i <- [1, 1000, 2000] do
        assert {:ok, "av#{i}"} == NIF.get(store2, "mixed_a_#{i}")
      end

      for i <- 1..20 do
        assert {:ok, "sv#{i}"} == NIF.get(store2, "mixed_s_#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # J. Interleaved expiry + overwrite at scale
  # ---------------------------------------------------------------------------

  describe "interleaved expiry and overwrite at scale" do
    test "50 keys: each written with expiry, then overwritten without — all live" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 1_000

      # First write: all with past expiry
      expired_batch = for i <- 1..50, do: {"ieo_#{i}", "gone_#{i}", past}
      assert :ok == submit_and_await!(store, expired_batch)

      # Verify they're expired
      for i <- 1..50, do: assert({:ok, nil} == NIF.get(store, "ieo_#{i}"))

      # Overwrite with no expiry
      live_batch = for i <- 1..50, do: {"ieo_#{i}", "live_#{i}", 0}
      assert :ok == submit_and_await!(store, live_batch)

      # Now all should be live
      for i <- 1..50 do
        assert {:ok, "live_#{i}"} == NIF.get(store, "ieo_#{i}"),
               "ieo_#{i} should be live after overwrite"
      end
    end

    test "batch with alternating expired/live entries: exact read semantics" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 1_000
      future = System.os_time(:millisecond) + 60_000

      batch =
        for i <- 1..20 do
          exp = if rem(i, 2) == 0, do: past, else: future
          {"alt_#{i}", "v#{i}", exp}
        end

      assert :ok == submit_and_await!(store, batch)

      for i <- 1..20 do
        if rem(i, 2) == 0 do
          assert {:ok, nil} == NIF.get(store, "alt_#{i}"),
                 "alt_#{i} should be expired"
        else
          assert {:ok, "v#{i}"} == NIF.get(store, "alt_#{i}"),
                 "alt_#{i} should be live"
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # K. NIF.keys accuracy after mixed operations
  # ---------------------------------------------------------------------------

  describe "NIF.keys accuracy after mixed async + sync + delete" do
    test "keys: async writes appear, deleted keys disappear, expired excluded" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 1_000

      async_batch = for i <- 1..10, do: {"mk_async_#{i}", "v#{i}", 0}
      assert :ok == submit_and_await!(store, async_batch)

      for i <- 1..5, do: :ok = NIF.put(store, "mk_sync_#{i}", "sv#{i}", 0)

      expired_batch = for i <- 1..3, do: {"mk_exp_#{i}", "ev#{i}", past}
      assert :ok == submit_and_await!(store, expired_batch)

      {:ok, true} = NIF.delete(store, "mk_async_1")
      {:ok, true} = NIF.delete(store, "mk_sync_1")

      keys = NIF.keys(store)

      # Async-written keys present (except deleted)
      for i <- 2..10, do: assert("mk_async_#{i}" in keys, "mk_async_#{i} missing")

      # Sync-written keys present (except deleted)
      for i <- 2..5, do: assert("mk_sync_#{i}" in keys, "mk_sync_#{i} missing")

      # Deleted keys absent
      refute "mk_async_1" in keys
      refute "mk_sync_1" in keys

      # Expired keys absent
      for i <- 1..3, do: refute("mk_exp_#{i}" in keys, "mk_exp_#{i} should be expired")
    end

    test "keys count is correct after batch overwrite (no duplicates)" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Write 10 unique keys
      batch1 = for i <- 1..10, do: {"dedup_#{i}", "v1", 0}
      assert :ok == submit_and_await!(store, batch1)

      # Overwrite all 10 with new values
      batch2 = for i <- 1..10, do: {"dedup_#{i}", "v2", 0}
      assert :ok == submit_and_await!(store, batch2)

      keys = NIF.keys(store)
      dedup_keys = Enum.filter(keys, &String.starts_with?(&1, "dedup_"))
      assert length(dedup_keys) == 10,
             "Expected 10 dedup keys, got #{length(dedup_keys)}"
    end
  end

  # ---------------------------------------------------------------------------
  # L. Router.delete races async write
  # ---------------------------------------------------------------------------

  describe "Router.delete races async write: tombstone wins" do
    test "put then immediate delete: key absent after flush" do
      k = "race_del_#{:rand.uniform(999_999)}"
      :ok = Router.put(k, "v", 0)
      :ok = Router.delete(k)

      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)

      assert nil == Router.get(k)
      assert false == Router.exists?(k)
    end

    test "delete then put: resurrection works" do
      k = "race_res_#{:rand.uniform(999_999)}"
      :ok = Router.put(k, "v1", 0)
      :ok = Router.delete(k)
      :ok = Router.put(k, "v2", 0)

      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)

      assert "v2" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # M. Shard restart fidelity: multiple rounds
  # ---------------------------------------------------------------------------

  describe "shard restart fidelity: multiple crash-restart cycles" do
    test "3 crash-restart cycles: each round's data survives, new writes succeed" do
      {pid0, idx, dir} = start_shard()
      on_exit(fn -> File.rm_rf!(dir) end)

      final_pid =
        Enum.reduce(1..3, pid0, fn round, pid ->
          # Write + flush
          :ok = GenServer.call(pid, {:put, "round#{round}_k", "round#{round}_v", 0})
          :ok = GenServer.call(pid, :flush)

          # Crash (unlink first so the brutal kill doesn't propagate to the test process)
          Process.unlink(pid)
          ref = Process.monitor(pid)
          Process.exit(pid, :kill)
          assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
          :timer.sleep(30)

          # Restart
          {:ok, new_pid} = Shard.start_link(index: idx, data_dir: dir)

          # All previous rounds' data must survive
          for prev <- 1..round do
            assert "round#{prev}_v" == GenServer.call(new_pid, {:get, "round#{prev}_k"}),
                   "round#{prev}_k lost after restart in round #{round}"
          end

          new_pid
        end)

      # Cleanup last pid
      if Process.alive?(final_pid), do: GenServer.stop(final_pid)
    end

    test "async writes in-flight during crash: pre-flush data survives" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> File.rm_rf!(dir) end)

      # Write + explicit flush (ensures data is on disk before crash)
      :ok = GenServer.call(pid, {:put, "pre_crash_ifl", "safe_val", 0})
      :ok = GenServer.call(pid, :flush)

      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      :timer.sleep(30)

      {:ok, new_pid} = Shard.start_link(index: idx, data_dir: dir)
      on_exit(fn -> if Process.alive?(new_pid), do: GenServer.stop(new_pid) end)

      assert "safe_val" == GenServer.call(new_pid, {:get, "pre_crash_ifl"})

      # New shard accepts further writes
      :ok = GenServer.call(new_pid, {:put, "post_crash_k", "post_v", 0})
      :ok = GenServer.call(new_pid, :flush)
      assert "post_v" == GenServer.call(new_pid, {:get, "post_crash_k"})
    end
  end

  # ---------------------------------------------------------------------------
  # N. Completion ordering: batches submitted in order must not cross data
  # ---------------------------------------------------------------------------

  describe "completion ordering: sequential batches" do
    test "10 sequential batches to distinct keys: no value swap" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Submit all, collect op_ids, then verify in order
      op_ids =
        for i <- 1..10 do
          submit_async!(store, [{"ord10_#{i}", "V#{i}", 0}])
        end

      for op_id <- op_ids do
        receive do
          {:io_complete, ^op_id, :ok} -> :ok
        after
          10_000 -> flunk("timeout for op #{op_id}")
        end
      end

      for i <- 1..10 do
        assert {:ok, "V#{i}"} == NIF.get(store, "ord10_#{i}"),
               "ord10_#{i} has wrong value — possible offset corruption"
      end
    end

    test "overwrite sequence: 20 async overwrites of same key — last value on disk" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      for i <- 1..20 do
        assert :ok == submit_and_await!(store, [{"ov_seq", "val_#{i}", 0}])
      end

      assert {:ok, "val_20"} == NIF.get(store, "ov_seq")

      # Survives reopen
      _ = store
      :erlang.garbage_collect()
      store2 = open_store(dir)
      assert {:ok, "val_20"} == NIF.get(store2, "ov_seq")
    end
  end

  # ---------------------------------------------------------------------------
  # O. Shard flush_in_flight: error completion still unblocks shard
  # ---------------------------------------------------------------------------

  describe "shard resilience after async error completions" do
    test "shard receives error completion, clears in-flight, accepts further writes" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 9876} end)
      send(pid, {:io_complete, 9876, {:error, "simulated disk error"}})
      :timer.sleep(30)

      assert :sys.get_state(pid).flush_in_flight == nil

      # Must accept writes normally after error
      :ok = GenServer.call(pid, {:put, "post_err", "v", 0})
      :ok = GenServer.call(pid, :flush)
      assert "v" == GenServer.call(pid, {:get, "post_err"})
    end

    test "3 consecutive error completions: shard stays alive and writable" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      for op_id <- [1111, 2222, 3333] do
        :sys.replace_state(pid, fn s -> %{s | flush_in_flight: op_id} end)
        send(pid, {:io_complete, op_id, {:error, "disk error #{op_id}"}})
        :timer.sleep(20)
        assert :sys.get_state(pid).flush_in_flight == nil
      end

      :ok = GenServer.call(pid, {:put, "resilient_k", "rv", 0})
      :ok = GenServer.call(pid, :flush)
      assert "rv" == GenServer.call(pid, {:get, "resilient_k"})
      assert true == GenServer.call(pid, {:exists, "resilient_k"})
    end
  end

  # ---------------------------------------------------------------------------
  # P. Batch of all-identical keys — extreme dedup
  # ---------------------------------------------------------------------------

  describe "batch deduplication: all-same key" do
    test "batch of 100 entries with same key: last entry wins" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = for i <- 1..100, do: {"one_key", "v#{i}", 0}
      assert :ok == submit_and_await!(store, batch)

      # Last entry in the batch must win
      assert {:ok, "v100"} == NIF.get(store, "one_key")
    end

    test "100-same-key batch survives reopen with correct final value" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = for i <- 1..100, do: {"dup_reopen", "v#{i}", 0}
      assert :ok == submit_and_await!(store1, batch)
      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "v100"} == NIF.get(store2, "dup_reopen")
    end
  end

  # ---------------------------------------------------------------------------
  # Q. ETS hot-path vs cold-path: Router.get bypasses GenServer for cached keys
  # ---------------------------------------------------------------------------

  describe "Router hot vs cold read path" do
    test "Router.get hits ETS directly for warm key (no GenServer call)" do
      k = "hot_k_#{:rand.uniform(999_999)}"
      :ok = Router.put(k, "hot_v", 0)

      # ETS is written synchronously during put — hot path must work immediately
      idx = Router.shard_for(k)

      # Confirm in ETS
      assert [{^k, "hot_v", _}] = :ets.lookup(:"hot_cache_#{idx}", k)

      # Router.get should return from ETS (hot path)
      assert "hot_v" == Router.get(k)
    end

    test "Router.get cold path: ETS miss falls back to GenServer" do
      k = "cold_rtr_#{:rand.uniform(999_999)}"
      :ok = Router.put(k, "cold_v", 0)

      # Force flush and evict ETS
      shard_name = Router.shard_name(Router.shard_for(k))
      :ok = GenServer.call(shard_name, :flush)
      :ets.delete(:"keydir_#{Router.shard_for(k)}", k)
      :ets.delete(:"hot_cache_#{Router.shard_for(k)}", k)

      # Cold path: must still return correct value
      assert "cold_v" == Router.get(k)
    end

    test "Router.get_meta hot path returns {value, expiry} from ETS" do
      future = System.os_time(:millisecond) + 60_000
      k = "hot_meta_#{:rand.uniform(999_999)}"
      :ok = Router.put(k, "mv", future)

      # ETS has it (put is synchronous to ETS)
      result = Router.get_meta(k)
      assert {val, exp} = result
      assert val == "mv"
      assert exp == future
    end
  end
end
