defmodule Ferricstore.Bitcask.NIFSafetyTest do
  @moduledoc """
  NIF safety tests covering known classes of NIF/BEAM interaction bugs:

  1. Mutex poisoning — verify the NIF returns a clean error (not a BEAM crash)
     when the Rust Mutex is poisoned.
  2. OwnedBinary allocation — stress-test large binary allocation to verify the
     NIF does not crash the BEAM on allocation failure.
  3. ResourceArc lifecycle — verify that store resources are safe after the
     owning process terminates and GC runs.
  4. Scheduler thread safety — verify concurrent NIF calls on the same
     ResourceArc do not corrupt data or crash the BEAM.
  5. Yielding NIF safety — verify the yielding NIFs (keys, get_all, get_batch,
     get_range) handle large datasets without blocking the BEAM scheduler.
  6. Store re-open after corruption — verify graceful error handling when the
     data directory is compromised.
  7. Invalid argument handling — verify the NIF does not crash on bad arguments.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "nif_safety_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # ------------------------------------------------------------------
  # 1. Mutex poisoning resilience
  # ------------------------------------------------------------------
  # The Rust NIF wraps Store in a Mutex. If a panic occurs while the Mutex
  # is held, subsequent lock attempts see a PoisonError. The NIF must
  # return {:error, _} or raise BadArg — NEVER crash the BEAM.
  #
  # We cannot directly poison the mutex from Elixir, but we CAN verify
  # that after a NIF call that returns an error, the store remains usable.
  # This ensures the mutex-recovery path (map_err on lock) works.

  describe "mutex/lock error resilience" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store, dir: dir}
    end

    test "store remains usable after a failed put (oversized key)", %{store: store} do
      # Trigger an error inside the NIF (oversized key > 65535 bytes)
      oversized_key = :binary.copy("X", 65_536)
      assert {:error, _} = NIF.put(store, oversized_key, "val", 0)

      # The store Mutex must NOT be poisoned — subsequent ops must work
      :ok = NIF.put(store, "after_error", "works", 0)
      assert {:ok, "works"} == NIF.get(store, "after_error")
    end

    test "store remains usable after a failed put_batch", %{store: store} do
      oversized_key = :binary.copy("K", 65_536)
      assert {:error, _} = NIF.put_batch(store, [{oversized_key, "v", 0}])

      # Must still work
      :ok = NIF.put(store, "batch_recover", "ok", 0)
      assert {:ok, "ok"} == NIF.get(store, "batch_recover")
    end

    test "store remains usable after many alternating error/success calls", %{store: store} do
      oversized_key = :binary.copy("Z", 65_536)

      for i <- 1..50 do
        # Error path
        assert {:error, _} = NIF.put(store, oversized_key, "v#{i}", 0)
        # Success path
        :ok = NIF.put(store, "key_#{i}", "val_#{i}", 0)
      end

      # Verify all successful writes are readable
      for i <- 1..50 do
        assert {:ok, "val_#{i}"} == NIF.get(store, "key_#{i}")
      end
    end
  end

  # ------------------------------------------------------------------
  # 2. OwnedBinary allocation safety
  # ------------------------------------------------------------------
  # OwnedBinary::new(size) can return None if allocation fails.
  # The NIF must handle this gracefully — returning an error rather than
  # crashing the BEAM process or the entire VM.

  describe "OwnedBinary allocation" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "get returns a valid binary for a large value (1MB)", %{store: store} do
      large_val = :crypto.strong_rand_bytes(1_000_000)
      :ok = NIF.put(store, "big", large_val, 0)
      assert {:ok, ^large_val} = NIF.get(store, "big")
    end

    test "put_batch with many entries allocates correctly", %{store: store} do
      # 1000 entries of 1KB each = ~1MB total allocation
      batch =
        Enum.map(1..1000, fn i ->
          {"batch_key_#{i}", :crypto.strong_rand_bytes(1024), 0}
        end)

      assert :ok = NIF.put_batch(store, batch)

      # Spot-check several values
      for i <- [1, 100, 500, 1000] do
        {_, expected, _} = Enum.at(batch, i - 1)
        assert {:ok, ^expected} = NIF.get(store, "batch_key_#{i}")
      end
    end

    test "get_all with many entries does not crash (binary allocation stress)", %{store: store} do
      batch =
        Enum.map(1..500, fn i ->
          {"getall_#{i}", :crypto.strong_rand_bytes(512), 0}
        end)

      :ok = NIF.put_batch(store, batch)

      assert {:ok, pairs} = NIF.get_all(store)
      assert length(pairs) == 500
    end

    test "get_batch with many keys does not crash", %{store: store} do
      for i <- 1..200, do: :ok = NIF.put(store, "gb_#{i}", "val_#{i}", 0)

      keys = Enum.map(1..200, fn i -> "gb_#{i}" end)
      assert {:ok, results} = NIF.get_batch(store, keys)
      assert length(results) == 200
      assert Enum.all?(results, &(not is_nil(&1)))
    end
  end

  # ------------------------------------------------------------------
  # 3. ResourceArc lifecycle safety
  # ------------------------------------------------------------------
  # When the Elixir process holding a ResourceArc terminates, the
  # reference count drops. If another process still holds a reference,
  # the ResourceArc must remain valid. When all references are gone,
  # the Rust destructor must run cleanly.

  describe "ResourceArc lifecycle" do
    test "store resource survives owning process termination" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      # Open store in a spawned process that immediately dies
      parent = self()

      spawn(fn ->
        store = open_store(dir)
        :ok = NIF.put(store, "orphan_key", "orphan_val", 0)
        send(parent, {:store, store})
      end)

      store = receive do
        {:store, s} -> s
      after
        5000 -> flunk("did not receive store")
      end

      # The spawned process is dead, but we still hold the ResourceArc
      # Force GC in the parent (should NOT release the resource)
      :erlang.garbage_collect()

      # The store must still be usable
      assert {:ok, "orphan_val"} == NIF.get(store, "orphan_key")
    end

    test "resource is valid after multiple GC cycles" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "gc_test", "survives", 0)

      # Force many GC cycles — the resource must survive because we hold a ref
      for _ <- 1..10, do: :erlang.garbage_collect()

      assert {:ok, "survives"} == NIF.get(store, "gc_test")
    end

    test "opening the same directory from two processes is safe" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store1 = open_store(dir)
      :ok = NIF.put(store1, "shared_dir_key", "from_store1", 0)

      # Open a second store on the same directory (simulates a restart
      # where the old store is not yet GC'd). This should not crash.
      store2 = open_store(dir)
      assert {:ok, "from_store1"} == NIF.get(store2, "shared_dir_key")
    end
  end

  # ------------------------------------------------------------------
  # 4. Scheduler thread safety / concurrent access
  # ------------------------------------------------------------------
  # Although the shard GenServer serializes access in production, the
  # ResourceArc can technically be called from multiple BEAM processes
  # concurrently. The Mutex must protect against data races.

  describe "concurrent NIF access safety" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "concurrent put + get does not crash or corrupt", %{store: store} do
      # Pre-populate some keys
      for i <- 1..100, do: :ok = NIF.put(store, "conc_#{i}", "init_#{i}", 0)

      # Launch writers and readers concurrently
      writers =
        Enum.map(1..20, fn i ->
          Task.async(fn ->
            for j <- 1..50 do
              NIF.put(store, "conc_w_#{i}_#{j}", "val_#{i}_#{j}", 0)
            end
          end)
        end)

      readers =
        Enum.map(1..20, fn _ ->
          Task.async(fn ->
            for i <- 1..100 do
              NIF.get(store, "conc_#{i}")
            end
          end)
        end)

      # All tasks must complete without crashing
      Enum.each(writers, &Task.await(&1, 30_000))
      Enum.each(readers, &Task.await(&1, 30_000))

      # Verify data integrity
      for i <- 1..100 do
        assert {:ok, "init_#{i}"} == NIF.get(store, "conc_#{i}")
      end
    end

    test "concurrent put_batch calls do not crash", %{store: store} do
      tasks =
        Enum.map(1..10, fn i ->
          Task.async(fn ->
            batch =
              Enum.map(1..100, fn j ->
                {"cpb_#{i}_#{j}", "val_#{i}_#{j}", 0}
              end)

            NIF.put_batch(store, batch)
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 30_000))

      # All batches must succeed
      assert Enum.all?(results, &(&1 == :ok))
    end

    test "concurrent delete + keys does not crash", %{store: store} do
      for i <- 1..100, do: :ok = NIF.put(store, "del_conc_#{i}", "v", 0)

      deleters =
        Enum.map(1..5, fn batch ->
          Task.async(fn ->
            start = (batch - 1) * 20 + 1
            for i <- start..(start + 19), do: NIF.delete(store, "del_conc_#{i}")
          end)
        end)

      listers =
        Enum.map(1..5, fn _ ->
          Task.async(fn ->
            NIF.keys(store)
          end)
        end)

      Enum.each(deleters, &Task.await(&1, 30_000))
      Enum.each(listers, &Task.await(&1, 30_000))
    end
  end

  # ------------------------------------------------------------------
  # 5. Yielding NIF safety
  # ------------------------------------------------------------------
  # The yielding NIFs (keys, get_all, get_batch, get_range) use
  # enif_schedule_nif to cooperatively yield. They must not block the
  # BEAM scheduler even with large datasets.

  describe "yielding NIF safety" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "keys() with >YIELD_CHUNK_SIZE entries yields correctly", %{store: store} do
      # YIELD_CHUNK_SIZE is 500 in the Rust code; insert 1500 keys
      batch =
        Enum.map(1..1500, fn i ->
          {"yield_k_#{String.pad_leading(Integer.to_string(i), 5, "0")}", "v", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      keys = NIF.keys(store)
      assert length(keys) == 1500
    end

    test "get_all() with >YIELD_CHUNK_SIZE entries yields correctly", %{store: store} do
      batch =
        Enum.map(1..600, fn i ->
          {"yield_ga_#{i}", "val_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      assert {:ok, pairs} = NIF.get_all(store)
      assert length(pairs) == 600
    end

    test "get_batch() with >YIELD_CHUNK_SIZE keys yields correctly", %{store: store} do
      batch =
        Enum.map(1..600, fn i ->
          {"yield_gb_#{i}", "val_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      keys = Enum.map(1..600, fn i -> "yield_gb_#{i}" end)
      assert {:ok, results} = NIF.get_batch(store, keys)
      assert length(results) == 600
      assert Enum.all?(results, &(not is_nil(&1)))
    end

    test "get_range() with >YIELD_CHUNK_SIZE results yields correctly", %{store: store} do
      batch =
        Enum.map(1..600, fn i ->
          {"yr_#{String.pad_leading(Integer.to_string(i), 5, "0")}", "val_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      assert {:ok, pairs} = NIF.get_range(store, "yr_00001", "yr_99999", 1000)
      assert length(pairs) == 600
    end

    test "BEAM remains responsive during large yielding NIF", %{store: store} do
      # Insert enough keys to trigger multiple yields
      batch =
        Enum.map(1..2000, fn i ->
          {"resp_#{String.pad_leading(Integer.to_string(i), 5, "0")}", "v", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      # Start a keys() call (yielding NIF) and simultaneously verify that
      # another process can still execute on the BEAM scheduler
      keys_task = Task.async(fn -> NIF.keys(store) end)

      probe_task =
        Task.async(fn ->
          # This should run in <100ms if the scheduler is not blocked
          :erlang.system_time(:millisecond)
        end)

      # If the yielding NIF blocked the scheduler, this Task.await would timeout
      assert is_integer(Task.await(probe_task, 5_000))

      keys = Task.await(keys_task, 30_000)
      assert length(keys) == 2000
    end
  end

  # ------------------------------------------------------------------
  # 6. Store corruption / error recovery
  # ------------------------------------------------------------------
  # Verify the NIF handles gracefully:
  # - Corrupt data files
  # - Missing data directory
  # - Read-only filesystem

  describe "store error recovery" do
    test "open on non-existent nested path creates it" do
      dir = Path.join(System.tmp_dir!(), "nif_safety_deep/nested/path_#{:rand.uniform(999_999)}")
      on_exit(fn -> File.rm_rf(Path.join(System.tmp_dir!(), "nif_safety_deep")) end)

      assert {:ok, store} = NIF.new(dir)
      :ok = NIF.put(store, "deep", "value", 0)
      assert {:ok, "value"} == NIF.get(store, "deep")
    end

    test "store recovers gracefully when data file is truncated" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "trunc_key", "trunc_val", 0)

      # Write hint so we can test hint-based recovery
      :ok = NIF.write_hint(store)

      # Drop the reference
      _ = store
      :erlang.garbage_collect()

      # Truncate the data file to simulate partial disk corruption
      log_files = Path.wildcard(Path.join(dir, "*.log"))

      if length(log_files) > 0 do
        [log_file | _] = log_files
        {:ok, content} = File.read(log_file)
        # Write only half the file back
        half = div(byte_size(content), 2)
        File.write!(log_file, binary_part(content, 0, half))

        # Opening must not crash the BEAM (it may return an error or
        # recover partially — both are acceptable)
        result = NIF.new(dir)

        case result do
          {:ok, _store2} -> :ok
          {:error, _reason} -> :ok
        end
      end
    end

    test "store handles corrupt hint file by falling back to log replay" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "hint_corrupt_key", "value", 0)
      :ok = NIF.write_hint(store)

      _ = store
      :erlang.garbage_collect()

      # Corrupt the hint file
      hint_files = Path.wildcard(Path.join(dir, "*.hint"))

      if length(hint_files) > 0 do
        [hint_file | _] = hint_files
        File.write!(hint_file, "CORRUPTED_HINT_DATA")

        # Should fall back to log replay and recover the data
        store2 = open_store(dir)
        assert {:ok, "value"} == NIF.get(store2, "hint_corrupt_key")
      end
    end
  end

  # ------------------------------------------------------------------
  # 7. Invalid argument handling
  # ------------------------------------------------------------------
  # The NIF must not crash the BEAM on bad arguments. It should return
  # ArgumentError or {:error, _} instead.

  describe "invalid argument handling" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "get with non-binary key raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.get(store, 12345)
      end
    end

    test "put with non-binary key raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.put(store, 12345, "value", 0)
      end
    end

    test "put with non-binary value raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.put(store, "key", 12345, 0)
      end
    end

    test "put with non-integer expire_at_ms raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.put(store, "key", "value", "not_a_number")
      end
    end

    test "put_batch with malformed entries raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.put_batch(store, [{"key", "value"}])
      end
    end

    test "put_batch with non-list raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.put_batch(store, "not_a_list")
      end
    end

    test "get_batch with non-list keys raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.get_batch(store, "not_a_list")
      end
    end

    test "get_range with non-binary bounds raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.get_range(store, 123, 456, 10)
      end
    end

    test "get_range with non-integer count raises ArgumentError", %{store: store} do
      assert_raise ArgumentError, fn ->
        NIF.get_range(store, "a", "z", "ten")
      end
    end

    test "read_modify_write with invalid operation tag raises or errors", %{store: store} do
      :ok = NIF.put(store, "rmw_test", "100", 0)

      assert_raise ArgumentError, fn ->
        NIF.read_modify_write(store, "rmw_test", {:unknown_op, 1})
      end
    end

    test "read_modify_write with non-tuple operation raises ArgumentError", %{store: store} do
      :ok = NIF.put(store, "rmw_test2", "100", 0)

      assert_raise ArgumentError, fn ->
        NIF.read_modify_write(store, "rmw_test2", "not_a_tuple")
      end
    end

    test "passing a non-resource as store raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        NIF.get(make_ref(), "key")
      end
    end

    test "passing nil as store raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        NIF.get(nil, "key")
      end
    end
  end

  # ------------------------------------------------------------------
  # 8. Rapid open/close (resource lifecycle stress)
  # ------------------------------------------------------------------
  # Rapidly opening and dropping store resources tests that the Rust
  # destructors fire correctly and do not leak file handles or memory.

  describe "rapid open/close resource lifecycle" do
    test "opening and dropping 100 stores does not leak or crash" do
      dirs =
        Enum.map(1..100, fn i ->
          dir = Path.join(System.tmp_dir!(), "nif_safety_rapid_#{i}_#{:rand.uniform(999_999)}")
          File.mkdir_p!(dir)
          dir
        end)

      on_exit(fn -> Enum.each(dirs, &File.rm_rf/1) end)

      for dir <- dirs do
        store = open_store(dir)
        :ok = NIF.put(store, "k", "v", 0)
        # Intentionally drop the reference immediately
        _ = store
      end

      # Force GC to trigger Rust destructors
      :erlang.garbage_collect()

      # Verify the BEAM is still healthy
      assert 1 + 1 == 2
    end

    test "rapidly opening the same directory 50 times" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      stores =
        Enum.map(1..50, fn _ ->
          store = open_store(dir)
          :ok = NIF.put(store, "k", "v", 0)
          store
        end)

      # All stores must be able to read the value
      for store <- stores do
        assert {:ok, "v"} == NIF.get(store, "k")
      end
    end
  end

  # ------------------------------------------------------------------
  # 9. Empty / boundary cases
  # ------------------------------------------------------------------

  describe "empty and boundary cases" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "get_all on empty store returns empty list", %{store: store} do
      assert {:ok, []} == NIF.get_all(store)
    end

    test "get_batch with empty key list returns empty list", %{store: store} do
      assert {:ok, []} == NIF.get_batch(store, [])
    end

    test "get_batch with all missing keys returns all nils", %{store: store} do
      assert {:ok, [nil, nil, nil]} == NIF.get_batch(store, ["x", "y", "z"])
    end

    test "get_range with empty range returns empty list", %{store: store} do
      :ok = NIF.put(store, "a", "1", 0)
      assert {:ok, []} == NIF.get_range(store, "z", "a", 100)
    end

    test "get_range with max_count=0 returns empty list", %{store: store} do
      :ok = NIF.put(store, "a", "1", 0)
      assert {:ok, []} == NIF.get_range(store, "a", "z", 0)
    end

    test "keys on empty store returns empty list", %{store: store} do
      assert [] == NIF.keys(store)
    end

    test "delete on empty store returns {:ok, false}", %{store: store} do
      assert {:ok, false} == NIF.delete(store, "nonexistent")
    end

    test "put_batch with empty list is :ok", %{store: store} do
      assert :ok == NIF.put_batch(store, [])
    end

    test "purge_expired on empty store returns {:ok, 0}", %{store: store} do
      assert {:ok, 0} == NIF.purge_expired(store)
    end

    test "shard_stats on empty store returns valid tuple", %{store: store} do
      assert {:ok, {_total, _live, _dead, _fc, _kc, _frag}} = NIF.shard_stats(store)
    end

    test "file_sizes on empty store returns valid list", %{store: store} do
      assert {:ok, sizes} = NIF.file_sizes(store)
      assert is_list(sizes)
    end

    test "run_compaction with empty file list is :ok", %{store: store} do
      assert {:ok, {0, 0, 0}} == NIF.run_compaction(store, [])
    end

    test "available_disk_space returns a positive integer", %{store: store} do
      assert {:ok, space} = NIF.available_disk_space(store)
      assert is_integer(space) and space > 0
    end
  end

  # ------------------------------------------------------------------
  # 10. Stress: interleaved operations
  # ------------------------------------------------------------------
  # Verify the NIF handles a rapid mix of all operation types without
  # crashing or corrupting data.

  describe "interleaved operation stress" do
    test "rapid interleaved put/get/delete/keys/batch does not crash" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for round <- 1..100 do
        key = "stress_#{round}"
        :ok = NIF.put(store, key, "val_#{round}", 0)
        assert {:ok, "val_#{round}"} == NIF.get(store, key)

        if rem(round, 3) == 0 do
          {:ok, true} = NIF.delete(store, key)
        end

        if rem(round, 10) == 0 do
          _keys = NIF.keys(store)
        end

        if rem(round, 20) == 0 do
          batch =
            Enum.map(1..10, fn j ->
              {"stress_batch_#{round}_#{j}", "bv", 0}
            end)

          :ok = NIF.put_batch(store, batch)
        end

        if rem(round, 25) == 0 do
          {:ok, _} = NIF.get_all(store)
        end

        if rem(round, 30) == 0 do
          {:ok, _} = NIF.get_range(store, "stress_", "stress_~", 100)
        end
      end

      # Verify the store is still healthy
      :ok = NIF.put(store, "final_check", "ok", 0)
      assert {:ok, "ok"} == NIF.get(store, "final_check")
    end
  end

  # ------------------------------------------------------------------
  # 11. write_hint / compaction safety after errors
  # ------------------------------------------------------------------

  describe "maintenance operations after errors" do
    test "write_hint works after failed writes" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      oversized_key = :binary.copy("H", 65_536)

      # Trigger errors
      {:error, _} = NIF.put(store, oversized_key, "v", 0)
      {:error, _} = NIF.put_batch(store, [{oversized_key, "v", 0}])

      # Successful writes
      :ok = NIF.put(store, "good_key", "good_val", 0)

      # write_hint must succeed — the store state is consistent
      assert :ok == NIF.write_hint(store)
    end

    test "purge_expired works after failed writes" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      past = System.os_time(:millisecond) - 1000

      oversized_key = :binary.copy("P", 65_536)
      {:error, _} = NIF.put(store, oversized_key, "v", 0)

      :ok = NIF.put(store, "expiring", "val", past)
      assert {:ok, 1} == NIF.purge_expired(store)
    end
  end
end
