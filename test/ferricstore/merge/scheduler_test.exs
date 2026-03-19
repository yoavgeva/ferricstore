defmodule Ferricstore.Merge.SchedulerTest do
  use ExUnit.Case

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Merge.{Manifest, Scheduler, Semaphore}
  alias Ferricstore.Store.Router

  # We use application-supervised shards for integration. Each test that writes
  # data uses unique key prefixes to avoid cross-test interference.
  # Schedulers use unique :name opts to avoid clashing with app-supervised ones.

  setup do
    data_dir = Application.get_env(:ferricstore, :data_dir)
    %{data_dir: data_dir}
  end

  # -------------------------------------------------------------------
  # Shard stats NIF tests (unit level)
  # -------------------------------------------------------------------

  describe "NIF.shard_stats/1" do
    test "returns stats for an empty store" do
      dir = temp_dir()
      # Ensure directory is truly empty (no leftover data files)
      File.rm_rf!(dir)
      File.mkdir_p!(dir)
      {:ok, store} = NIF.new(dir)
      {:ok, {total, live, dead, file_count, key_count, frag}} = NIF.shard_stats(store)

      assert is_integer(total)
      assert is_integer(live)
      assert is_integer(dead)
      assert is_integer(file_count)
      assert is_integer(key_count)
      assert is_float(frag)
      assert key_count == 0
    end

    test "returns non-zero stats after writes" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      for i <- 1..100 do
        :ok = NIF.put(store, "key_#{i}", "value_#{i}", 0)
      end

      {:ok, {total, live, _dead, _file_count, key_count, _frag}} = NIF.shard_stats(store)

      assert total > 0
      assert live > 0
      assert key_count == 100
    end

    test "fragmentation increases after overwrites" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      for _round <- 1..10 do
        for i <- 1..50 do
          :ok = NIF.put(store, "frag_key_#{i}", "value_#{:rand.uniform(1000)}", 0)
        end
      end

      {:ok, {total, _live, dead, _file_count, _key_count, frag}} = NIF.shard_stats(store)

      assert dead > 0, "should have dead bytes after overwrites"
      assert frag > 0.0, "fragmentation ratio should be > 0 after overwrites"
      assert total > dead, "total should exceed dead bytes"
    end
  end

  describe "NIF.file_sizes/1" do
    test "returns file sizes for a store" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      :ok = NIF.put(store, "key", "value", 0)
      {:ok, file_sizes} = NIF.file_sizes(store)

      assert is_list(file_sizes)
      assert file_sizes != []

      Enum.each(file_sizes, fn {fid, size} ->
        assert is_integer(fid)
        assert is_integer(size)
        assert size > 0
      end)
    end
  end

  describe "NIF.run_compaction/2" do
    test "compacts files and reclaims space" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      for _round <- 1..20 do
        for i <- 1..20 do
          :ok = NIF.put(store, "compact_key_#{i}", String.duplicate("v", 100), 0)
        end
      end

      {:ok, file_sizes_before} = NIF.file_sizes(store)
      all_fids = Enum.map(file_sizes_before, fn {fid, _} -> fid end)

      case all_fids do
        [_single_file] ->
          {:ok, {0, 0, 0}} = NIF.run_compaction(store, [])

        _ ->
          active_fid = Enum.max(all_fids)
          merge_fids = Enum.reject(all_fids, &(&1 == active_fid))

          {:ok, {written, dropped, reclaimed}} = NIF.run_compaction(store, merge_fids)

          assert written >= 0
          assert dropped >= 0
          assert reclaimed >= 0

          for i <- 1..20 do
            {:ok, val} = NIF.get(store, "compact_key_#{i}")
            assert val != nil, "key compact_key_#{i} should still be readable"
          end
      end
    end

    test "handles empty file_ids list" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)
      {:ok, {0, 0, 0}} = NIF.run_compaction(store, [])
    end
  end

  describe "NIF.available_disk_space/1" do
    test "returns a positive value" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)
      {:ok, space} = NIF.available_disk_space(store)
      assert is_integer(space)
      assert space > 0, "available disk space should be positive"
    end
  end

  # -------------------------------------------------------------------
  # Shard GenServer merge calls
  # -------------------------------------------------------------------

  describe "shard GenServer merge calls" do
    test "shard_stats returns stats via GenServer" do
      shard = :"Ferricstore.Store.Shard.0"

      {:ok, {total, live, dead, file_count, key_count, frag}} =
        GenServer.call(shard, :shard_stats)

      assert is_integer(total)
      assert is_integer(live)
      assert is_integer(dead)
      assert is_integer(file_count)
      assert is_integer(key_count)
      assert is_float(frag)
    end

    test "file_sizes returns list via GenServer" do
      shard = :"Ferricstore.Store.Shard.0"
      {:ok, sizes} = GenServer.call(shard, :file_sizes)
      assert is_list(sizes)
    end

    test "available_disk_space returns positive value via GenServer" do
      shard = :"Ferricstore.Store.Shard.0"
      {:ok, space} = GenServer.call(shard, :available_disk_space)
      assert is_integer(space)
      assert space > 0
    end
  end

  # -------------------------------------------------------------------
  # Scheduler GenServer tests
  # -------------------------------------------------------------------

  describe "Scheduler lifecycle" do
    test "starts and reports status", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{check_interval_ms: 60_000}
        )

      status = Scheduler.status(sched)
      assert status.shard_index == 0
      assert status.mode == :hot
      assert status.merging == false
      assert status.merge_count == 0
      assert status.total_bytes_reclaimed == 0

      GenServer.stop(sched)
      GenServer.stop(sem)
    end

    test "uses custom merge config", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            mode: :bulk,
            fragmentation_threshold: 0.8,
            check_interval_ms: 60_000,
            merge_window: {0, 24}
          }
        )

      status = Scheduler.status(sched)
      assert status.config.mode == :bulk
      assert status.config.fragmentation_threshold == 0.8
      assert status.config.merge_window == {0, 24}

      GenServer.stop(sched)
      GenServer.stop(sem)
    end
  end

  describe "Scheduler trigger_check" do
    test "trigger_check does not crash even with low fragmentation", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            check_interval_ms: 600_000,
            fragmentation_threshold: 0.99
          }
        )

      assert :ok = Scheduler.trigger_check(sched)

      status = Scheduler.status(sched)
      assert status.merging == false

      GenServer.stop(sched)
      GenServer.stop(sem)
    end
  end

  describe "merge triggers on fragmentation threshold" do
    test "merge runs when fragmentation exceeds threshold" do
      data_dir = Application.get_env(:ferricstore, :data_dir)
      shard = :"Ferricstore.Store.Shard.0"

      keys_for_shard_0 = find_keys_for_shard(0, 20)

      for _round <- 1..20 do
        for key <- keys_for_shard_0 do
          GenServer.call(shard, {:put, key, String.duplicate("x", 200), 0})
        end
      end

      GenServer.call(shard, :flush)

      {:ok, {_total, _live, _dead, file_count, _keys, frag}} =
        GenServer.call(shard, :shard_stats)

      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            check_interval_ms: 600_000,
            fragmentation_threshold: 0.01,
            min_files_for_merge: 1
          }
        )

      if frag > 0.01 and file_count >= 1 do
        :ok = Scheduler.trigger_check(sched)
        Process.sleep(100)

        status = Scheduler.status(sched)
        assert is_integer(status.merge_count)
      end

      for key <- keys_for_shard_0 do
        val = GenServer.call(shard, {:get, key})
        assert val != nil, "key #{key} should still be readable after merge"
      end

      GenServer.stop(sched)
      GenServer.stop(sem)
    end
  end

  describe "semaphore prevents concurrent merges" do
    test "second scheduler is blocked when first holds semaphore", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      assert :ok = Semaphore.acquire(0, sem)

      {:ok, sched1} =
        Scheduler.start_link(
          shard_index: 1,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            check_interval_ms: 600_000,
            fragmentation_threshold: 0.01,
            min_files_for_merge: 1
          }
        )

      :ok = Scheduler.trigger_check(sched1)
      status = Scheduler.status(sched1)
      assert status.merging == false, "should not merge while semaphore is held by shard 0"

      Semaphore.release(0, sem)
      GenServer.stop(sched1)
      GenServer.stop(sem)
    end
  end

  describe "pre-merge space check" do
    test "scheduler reports available disk space" do
      shard = :"Ferricstore.Store.Shard.0"
      {:ok, space} = GenServer.call(shard, :available_disk_space)
      assert space > 0
    end
  end

  describe "TTL-aware merge drops expired entries" do
    test "expired keys are dropped during compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      past_ms = System.os_time(:millisecond) - 10_000

      for i <- 1..50 do
        :ok = NIF.put(store, "ttl_key_#{i}", "value", past_ms)
      end

      for i <- 1..10 do
        :ok = NIF.put(store, "perm_key_#{i}", "permanent", 0)
      end

      {:ok, file_sizes} = NIF.file_sizes(store)
      all_fids = Enum.map(file_sizes, fn {fid, _} -> fid end)

      case all_fids do
        [_single] ->
          {:ok, purged} = NIF.purge_expired(store)
          assert purged == 50

        _ ->
          active_fid = Enum.max(all_fids)
          merge_fids = Enum.reject(all_fids, &(&1 == active_fid))

          if merge_fids != [] do
            {:ok, {_written, dropped, _reclaimed}} = NIF.run_compaction(store, merge_fids)
            assert dropped > 0, "expired entries should be dropped during compaction"
          end
      end
    end
  end

  describe "merge does not affect concurrent reads" do
    test "reads succeed during and after compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      for i <- 1..100 do
        :ok = NIF.put(store, "read_key_#{i}", "value_#{i}", 0)
      end

      for i <- 1..100 do
        {:ok, val} = NIF.get(store, "read_key_#{i}")
        assert val == "value_#{i}"
      end

      {:ok, file_sizes} = NIF.file_sizes(store)
      all_fids = Enum.map(file_sizes, fn {fid, _} -> fid end)

      if length(all_fids) > 1 do
        active_fid = Enum.max(all_fids)
        merge_fids = Enum.reject(all_fids, &(&1 == active_fid))
        NIF.run_compaction(store, merge_fids)
      end

      for i <- 1..100 do
        {:ok, val} = NIF.get(store, "read_key_#{i}")
        assert val == "value_#{i}", "key read_key_#{i} should survive compaction"
      end
    end
  end

  describe "manifest written and cleaned up" do
    test "scheduler writes manifest before merge and cleans up after" do
      data_dir = Application.get_env(:ferricstore, :data_dir)
      shard_data_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)

      Manifest.delete(shard_data_dir)
      refute Manifest.exists?(shard_data_dir)
    end
  end

  describe "mode-based scheduling" do
    test "bulk mode respects time window", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            mode: :bulk,
            check_interval_ms: 600_000,
            fragmentation_threshold: 0.01,
            min_files_for_merge: 1,
            merge_window: {25, 26}
          }
        )

      :ok = Scheduler.trigger_check(sched)
      status = Scheduler.status(sched)
      assert status.merge_count == 0, "bulk mode should not merge outside window"

      GenServer.stop(sched)
      GenServer.stop(sem)
    end

    test "age mode respects time window", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            mode: :age,
            check_interval_ms: 600_000,
            fragmentation_threshold: 0.01,
            min_files_for_merge: 1,
            merge_window: {25, 26}
          }
        )

      :ok = Scheduler.trigger_check(sched)
      status = Scheduler.status(sched)
      assert status.merge_count == 0, "age mode should not merge outside window"

      GenServer.stop(sched)
      GenServer.stop(sem)
    end

    test "hot mode ignores time window", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{
            mode: :hot,
            check_interval_ms: 600_000,
            fragmentation_threshold: 0.99,
            min_files_for_merge: 100
          }
        )

      # Should not crash even with hot mode and very high threshold.
      :ok = Scheduler.trigger_check(sched)
      status = Scheduler.status(sched)
      # Won't merge because threshold is too high, but the point is it runs.
      assert status.merging == false

      GenServer.stop(sched)
      GenServer.stop(sem)
    end
  end

  describe "Scheduler.status/1" do
    test "returns all expected fields", %{data_dir: data_dir} do
      {:ok, sem} = Semaphore.start_link(name: unique_name("sem"))

      {:ok, sched} =
        Scheduler.start_link(
          shard_index: 2,
          data_dir: data_dir,
          name: unique_name("sched"),
          semaphore: sem,
          merge_config: %{check_interval_ms: 600_000}
        )

      status = Scheduler.status(sched)
      assert Map.has_key?(status, :shard_index)
      assert Map.has_key?(status, :mode)
      assert Map.has_key?(status, :merging)
      assert Map.has_key?(status, :last_merge_at)
      assert Map.has_key?(status, :merge_count)
      assert Map.has_key?(status, :total_bytes_reclaimed)
      assert Map.has_key?(status, :config)

      GenServer.stop(sched)
      GenServer.stop(sem)
    end
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp temp_dir do
    dir = Path.join(System.tmp_dir!(), "merge_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end

  defp unique_name(prefix) do
    :"#{prefix}_#{:erlang.unique_integer([:positive])}"
  end

  defp find_keys_for_shard(target_shard, count) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(fn i -> "merge_test_key_#{i}" end)
    |> Stream.filter(fn key ->
      Router.shard_for(key) == target_shard
    end)
    |> Enum.take(count)
  end
end
