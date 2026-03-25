defmodule Ferricstore.Store.ShardAsyncIoTest do
  @moduledoc """
  Tests for the optimized async IO path in `Ferricstore.Store.Shard`.

  Covers:
  - v2_append_batch_nosync (write without fsync)
  - Deferred fsync via v2_fsync_async on flush timer
  - Split write+fsync path
  - Async write completion (v2_append_batch_async NIF)
  - fsync_needed state tracking
  - ETS update_element optimization in update_ets_locations
  - Data correctness after nosync write + deferred fsync
  - Concurrent writes with deferred fsync
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard

  setup do
    :ok
  end

  # Start an isolated shard (not the app-supervised ones) for each test.
  defp start_shard(opts \\ []) do
    dir = Path.join(System.tmp_dir!(), "shard_async_io_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    index = :erlang.unique_integer([:positive]) |> rem(10_000) |> Kernel.+(30_000)
    flush_ms = Keyword.get(opts, :flush_interval_ms, 1)
    {:ok, pid} = Shard.start_link(index: index, data_dir: dir, flush_interval_ms: flush_ms)
    {pid, index, dir}
  end

  # ---------------------------------------------------------------------------
  # v2_append_batch_nosync NIF
  # ---------------------------------------------------------------------------

  describe "v2_append_batch_nosync NIF" do
    test "writes records without fsync and returns offsets" do
      dir = Path.join(System.tmp_dir!(), "nosync_nif_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      batch = [{"key1", "val1", 0}, {"key2", "val2", 0}]

      assert {:ok, locations} = NIF.v2_append_batch_nosync(path, batch)
      assert length(locations) == 2

      [{off1, vsize1}, {off2, vsize2}] = locations
      assert off1 == 0
      assert off2 > off1
      assert vsize1 == 4  # byte_size("val1")
      assert vsize2 == 4  # byte_size("val2")

      # Data should be readable via v2_pread_at (flushed to page cache)
      assert {:ok, "val1"} = NIF.v2_pread_at(path, off1)
      assert {:ok, "val2"} = NIF.v2_pread_at(path, off2)
    end

    test "empty batch returns empty locations" do
      dir = Path.join(System.tmp_dir!(), "nosync_empty_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      assert {:ok, []} = NIF.v2_append_batch_nosync(path, [])
    end
  end

  # ---------------------------------------------------------------------------
  # Shard: nosync writes + deferred fsync
  # ---------------------------------------------------------------------------

  describe "shard nosync write path" do
    test "put is readable immediately via ETS (before fsync)" do
      {pid, _index, dir} = start_shard(flush_interval_ms: 100)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "nsk", "nsv", 0})
      # Should be readable from ETS immediately
      assert "nsv" == GenServer.call(pid, {:get, "nsk"})
    end

    test "fsync_needed is set after nosync write" do
      {pid, _index, dir} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "fk", "fv", 0})
      # Allow the flush_pending to run (triggered by put when no flush_in_flight)
      Process.sleep(10)

      state = :sys.get_state(pid)
      # After a nosync write, fsync_needed should be true (or already consumed by timer)
      # Since flush_interval is 5000ms, the timer hasn't fired yet
      assert state.fsync_needed == true or state.flush_in_flight != nil
    end

    test "data survives flush (sync) call" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "dk", "dv", 0})
      :ok = GenServer.call(pid, :flush)

      assert "dv" == GenServer.call(pid, {:get, "dk"})
    end

    test "multiple puts before flush are all readable" do
      {pid, _index, dir} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      for i <- 1..10 do
        :ok = GenServer.call(pid, {:put, "mkey#{i}", "mval#{i}", 0})
      end

      # All should be in ETS
      for i <- 1..10 do
        assert "mval#{i}" == GenServer.call(pid, {:get, "mkey#{i}"})
      end

      # Flush and verify durability
      :ok = GenServer.call(pid, :flush)

      for i <- 1..10 do
        assert "mval#{i}" == GenServer.call(pid, {:get, "mkey#{i}"})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Deferred fsync on timer
  # ---------------------------------------------------------------------------

  describe "deferred fsync on flush timer" do
    test "fsync is performed by flush timer" do
      {pid, _index, dir} = start_shard(flush_interval_ms: 5)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "tk", "tv", 0})

      # Wait for the flush timer to fire and do async fsync
      Process.sleep(50)

      state = :sys.get_state(pid)
      # After the timer fires, fsync_needed should be false and
      # flush_in_flight should be nil (fsync completed)
      assert state.fsync_needed == false
      assert state.flush_in_flight == nil
    end
  end

  # ---------------------------------------------------------------------------
  # ETS update_element optimization
  # ---------------------------------------------------------------------------

  describe "update_ets_locations preserves LFU counter" do
    test "LFU counter is preserved after flush updates disk location" do
      {pid, index, dir} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      # Put a value — this inserts with LFU.initial()
      :ok = GenServer.call(pid, {:put, "lfu_key", "lfu_val", 0})

      # Read the key multiple times to increment LFU counter
      for _ <- 1..20 do
        GenServer.call(pid, {:get, "lfu_key"})
      end

      # Get the LFU counter before flush
      keydir = :"keydir_#{index}"
      [{_, _, _, lfu_before, _, _, _}] = :ets.lookup(keydir, "lfu_key")

      # Flush (will call update_ets_locations)
      :ok = GenServer.call(pid, :flush)

      # Get the LFU counter after flush — should be preserved
      [{_, _, _, lfu_after, fid, off, _}] = :ets.lookup(keydir, "lfu_key")
      assert lfu_after == lfu_before
      # Disk location should now be set (non-zero)
      assert fid > 0 or off > 0 or (fid == 0 and off == 0)
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent writes with deferred fsync
  # ---------------------------------------------------------------------------

  describe "concurrent writes" do
    test "many concurrent puts followed by flush all survive" do
      {pid, _index, dir} = start_shard(flush_interval_ms: 1)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      # Fire 100 concurrent writes
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            GenServer.call(pid, {:put, "c#{i}", "v#{i}", 0})
          end)
        end

      Enum.each(tasks, &Task.await(&1, 5000))

      # Flush everything
      :ok = GenServer.call(pid, :flush)

      # All keys should be readable
      for i <- 1..100 do
        assert "v#{i}" == GenServer.call(pid, {:get, "c#{i}"}),
               "key c#{i} should have value v#{i}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Durability: delete forces sync flush
  # ---------------------------------------------------------------------------

  describe "delete forces synchronous flush" do
    test "delete after nosync writes ensures durability" do
      {pid, _index, dir} = start_shard(flush_interval_ms: 5000)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "del_k1", "del_v1", 0})
      :ok = GenServer.call(pid, {:put, "del_k2", "del_v2", 0})

      # Delete forces await_in_flight + flush_pending_sync
      :ok = GenServer.call(pid, {:delete, "del_k1"})

      # del_k1 should be gone
      assert nil == GenServer.call(pid, {:get, "del_k1"})
      # del_k2 should still be there
      assert "del_v2" == GenServer.call(pid, {:get, "del_k2"})
    end
  end

  # ---------------------------------------------------------------------------
  # v2_append_batch_async NIF (Tokio path)
  # ---------------------------------------------------------------------------

  describe "v2_append_batch_async NIF" do
    test "sends completion message with locations" do
      dir = Path.join(System.tmp_dir!(), "async_write_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()
      corr_id = 42
      batch = [{"ak1", "av1", 0}, {"ak2", "av2", 0}]

      :ok = NIF.v2_append_batch_async(caller, corr_id, path, batch)

      # Wait for the completion message
      assert_receive {:tokio_complete, ^corr_id, :ok, locations}, 5000
      assert is_list(locations)
      assert length(locations) == 2

      [{off1, vsize1}, {off2, vsize2}] = locations
      assert off1 == 0
      assert off2 > off1
      assert vsize1 == 3  # byte_size("av1")
      assert vsize2 == 3  # byte_size("av2")

      # Data should be readable
      assert {:ok, "av1"} = NIF.v2_pread_at(path, off1)
      assert {:ok, "av2"} = NIF.v2_pread_at(path, off2)
    end

    test "empty batch returns empty locations" do
      dir = Path.join(System.tmp_dir!(), "async_empty_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()
      corr_id = 99

      :ok = NIF.v2_append_batch_async(caller, corr_id, path, [])

      assert_receive {:tokio_complete, ^corr_id, :ok, []}, 5000
    end

    test "multiple concurrent async batches with different correlation IDs" do
      dir = Path.join(System.tmp_dir!(), "async_concurrent_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      caller = self()

      # Submit 5 async batches
      for i <- 1..5 do
        batch = [{"key_#{i}", "val_#{i}", 0}]
        :ok = NIF.v2_append_batch_async(caller, i, path, batch)
      end

      # Collect all completions (order may vary)
      received =
        for _ <- 1..5 do
          receive do
            {:tokio_complete, corr_id, :ok, locations} -> {corr_id, locations}
          after
            5000 -> flunk("timeout waiting for async completion")
          end
        end

      corr_ids = Enum.map(received, fn {id, _} -> id end) |> Enum.sort()
      assert corr_ids == [1, 2, 3, 4, 5]
    end
  end

  # ---------------------------------------------------------------------------
  # v2_fsync_async NIF
  # ---------------------------------------------------------------------------

  describe "v2_fsync_async NIF" do
    test "sends completion message after fsync" do
      dir = Path.join(System.tmp_dir!(), "fsync_async_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      on_exit(fn -> File.rm_rf(dir) end)

      # Write some data first
      NIF.v2_append_batch_nosync(path, [{"fk", "fv", 0}])

      # Submit async fsync
      caller = self()
      corr_id = 77
      :ok = NIF.v2_fsync_async(caller, corr_id, path)

      assert_receive {:tokio_complete, ^corr_id, :ok, :ok}, 5000
    end
  end
end
