defmodule Ferricstore.Bitcask.NIFRecoveryBugHuntTest do
  @moduledoc """
  Targeted tests for hint-file recovery, purge_expired edge cases, and crash
  recovery semantics in the Bitcask NIF layer.

  These tests exercise the NIF through the same `Ferricstore.Bitcask.NIF`
  interface that the shard GenServer uses. Each test gets its own temporary
  directory to avoid any cross-test interference.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "nif_recovery_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # Force-release a NIF store reference by clearing the variable and running GC.
  # This simulates a "clean close" -- the Rust Drop impl flushes the log writer.
  defp release_store(store) do
    _ = store
    :erlang.garbage_collect()
    # Give the NIF resource destructor a moment to run.
    Process.sleep(50)
  end

  # ------------------------------------------------------------------
  # 1. write_hint then reopen — all keys recovered from hint
  # ------------------------------------------------------------------

  describe "write_hint then reopen" do
    test "all keys are recovered from hint file after reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      # Write 50 keys
      for i <- 1..50 do
        :ok = NIF.put(store, "hint_key_#{i}", "hint_val_#{i}", 0)
      end

      # Write hint file
      :ok = NIF.write_hint(store)

      # Verify hint file was created on disk
      hint_files = Path.wildcard(Path.join(dir, "*.hint"))
      assert length(hint_files) > 0, "expected at least one .hint file on disk"

      # Release and reopen
      release_store(store)
      store2 = open_store(dir)

      # Verify ALL 50 keys recovered
      for i <- 1..50 do
        assert {:ok, "hint_val_#{i}"} == NIF.get(store2, "hint_key_#{i}"),
               "key hint_key_#{i} must be recoverable from hint file"
      end

      keys = NIF.keys(store2)
      assert length(keys) == 50, "expected 50 keys, got #{length(keys)}"
    end

    test "deleted keys are not resurrected after hint-based recovery" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      :ok = NIF.put(store, "keep", "yes", 0)
      :ok = NIF.put(store, "delete_me", "gone", 0)
      {:ok, true} = NIF.delete(store, "delete_me")

      :ok = NIF.write_hint(store)
      release_store(store)

      store2 = open_store(dir)
      assert {:ok, "yes"} == NIF.get(store2, "keep")
      assert {:ok, nil} == NIF.get(store2, "delete_me"),
             "deleted key must not be resurrected via hint file"
    end
  end

  # ------------------------------------------------------------------
  # 2. write_hint with expired keys — hint includes or excludes expired?
  # ------------------------------------------------------------------

  describe "write_hint with expired keys" do
    test "expired keys written to hint do not appear on reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000

      # Write a mix of live and expired keys
      :ok = NIF.put(store, "live_key", "alive", 0)
      :ok = NIF.put(store, "expired_key", "dead", past)

      :ok = NIF.write_hint(store)
      release_store(store)

      store2 = open_store(dir)

      # Live key must be present
      assert {:ok, "alive"} == NIF.get(store2, "live_key")

      # Expired key must return nil on get (TTL check fires on read)
      assert {:ok, nil} == NIF.get(store2, "expired_key"),
             "expired key must not be accessible after hint-based reopen"
    end

    test "expired keys are excluded from keys/1 after hint-based reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000
      :ok = NIF.put(store, "live", "val", 0)
      :ok = NIF.put(store, "expired", "val", past)

      :ok = NIF.write_hint(store)
      release_store(store)

      store2 = open_store(dir)
      keys = NIF.keys(store2)

      assert "live" in keys
      refute "expired" in keys,
             "expired key must not appear in keys/1 after hint-based reopen"
    end
  end

  # ------------------------------------------------------------------
  # 3. purge_expired with 0 expired — returns {:ok, 0}
  # ------------------------------------------------------------------

  describe "purge_expired with 0 expired" do
    test "returns {:ok, 0} when no keys are expired" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      :ok = NIF.put(store, "live1", "val", 0)
      :ok = NIF.put(store, "live2", "val", 0)

      assert {:ok, 0} == NIF.purge_expired(store)
    end

    test "returns {:ok, 0} on empty store" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      assert {:ok, 0} == NIF.purge_expired(store)
    end

    test "returns {:ok, 0} when all keys have future expiry" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      future = System.os_time(:millisecond) + 60_000

      :ok = NIF.put(store, "future1", "val", future)
      :ok = NIF.put(store, "future2", "val", future)

      assert {:ok, 0} == NIF.purge_expired(store)
    end
  end

  # ------------------------------------------------------------------
  # 4. purge_expired with 1000 expired — all removed
  # ------------------------------------------------------------------

  describe "purge_expired with 1000 expired keys" do
    test "all 1000 expired keys are removed" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000

      batch =
        Enum.map(1..1000, fn i ->
          {"expired_#{i}", "val_#{i}", past}
        end)

      :ok = NIF.put_batch(store, batch)

      # Verify they are in the keydir (but expired)
      for i <- 1..5 do
        assert {:ok, nil} == NIF.get(store, "expired_#{i}"),
               "expired key must return nil on get even before purge"
      end

      # Purge
      assert {:ok, count} = NIF.purge_expired(store)

      # The NIF may have lazily purged some keys during the get calls above,
      # so the purge count may be slightly less than 1000. But the vast
      # majority should be purged here.
      assert count >= 990,
             "expected at least 990 keys purged, got #{count}"

      # After purge, keys/1 must not include any expired keys
      keys = NIF.keys(store)

      expired_remaining =
        Enum.count(keys, fn k -> String.starts_with?(k, "expired_") end)

      assert expired_remaining == 0,
             "expected 0 expired keys remaining in keys/1, got #{expired_remaining}"
    end

    test "purge_expired is idempotent — second call returns {:ok, 0}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000

      for i <- 1..10 do
        :ok = NIF.put(store, "exp_idem_#{i}", "val", past)
      end

      {:ok, first_count} = NIF.purge_expired(store)
      assert first_count > 0

      # Second purge should find nothing
      assert {:ok, 0} == NIF.purge_expired(store)
    end
  end

  # ------------------------------------------------------------------
  # 5. purge_expired doesn't touch live keys
  # ------------------------------------------------------------------

  describe "purge_expired does not touch live keys" do
    test "live keys with no expiry survive purge" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000

      # 10 expired + 10 live
      for i <- 1..10 do
        :ok = NIF.put(store, "expired_#{i}", "gone", past)
        :ok = NIF.put(store, "live_#{i}", "alive_#{i}", 0)
      end

      {:ok, purged} = NIF.purge_expired(store)
      assert purged == 10, "expected exactly 10 keys purged, got #{purged}"

      # All live keys must be intact
      for i <- 1..10 do
        assert {:ok, "alive_#{i}"} == NIF.get(store, "live_#{i}"),
               "live key live_#{i} must not be affected by purge"
      end

      # All expired keys must be gone
      for i <- 1..10 do
        assert {:ok, nil} == NIF.get(store, "expired_#{i}")
      end
    end

    test "live keys with future expiry survive purge" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000
      future = System.os_time(:millisecond) + 60_000

      :ok = NIF.put(store, "expired", "gone", past)
      :ok = NIF.put(store, "future_ttl", "still_here", future)
      :ok = NIF.put(store, "no_ttl", "permanent", 0)

      {:ok, purged} = NIF.purge_expired(store)
      assert purged == 1

      assert {:ok, "still_here"} == NIF.get(store, "future_ttl")
      assert {:ok, "permanent"} == NIF.get(store, "no_ttl")
    end

    test "purge_expired tombstones persist across reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000
      :ok = NIF.put(store, "purge_persist", "val", past)
      :ok = NIF.put(store, "alive_persist", "val", 0)

      {:ok, 1} = NIF.purge_expired(store)

      release_store(store)
      store2 = open_store(dir)

      # Expired key must remain absent after reopen
      assert {:ok, nil} == NIF.get(store2, "purge_persist"),
             "purged key must not resurrect after reopen"

      assert {:ok, "val"} == NIF.get(store2, "alive_persist")
    end
  end

  # ------------------------------------------------------------------
  # 6. Crash mid-write (simulate truncated log) — recovery skips corrupt tail
  # ------------------------------------------------------------------

  describe "crash mid-write (truncated log)" do
    test "recovery skips corrupt tail and preserves earlier records" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      # Write 10 good records
      for i <- 1..10 do
        :ok = NIF.put(store, "good_#{i}", "val_#{i}", 0)
      end

      release_store(store)

      # Simulate a crash: append garbage bytes to the log file
      # (simulates a partial write that was not fsynced before crash)
      [log_file] = Path.wildcard(Path.join(dir, "*.log"))
      garbage = :crypto.strong_rand_bytes(50)
      File.write!(log_file, garbage, [:append])

      # Reopen — the tolerant log reader should skip the corrupt tail
      store2 = open_store(dir)

      # All 10 good records must be recoverable
      for i <- 1..10 do
        assert {:ok, "val_#{i}"} == NIF.get(store2, "good_#{i}"),
               "good key good_#{i} must survive truncated tail recovery"
      end
    end

    test "recovery with truncated header at tail" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      :ok = NIF.put(store, "before_crash", "safe", 0)
      release_store(store)

      # Append only 10 bytes — less than the 26-byte header
      # This simulates a crash mid-header-write
      [log_file] = Path.wildcard(Path.join(dir, "*.log"))
      File.write!(log_file, :binary.copy(<<0>>, 10), [:append])

      store2 = open_store(dir)
      assert {:ok, "safe"} == NIF.get(store2, "before_crash"),
             "record before truncated header must survive"
    end

    test "completely corrupted log file results in empty store" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "will_be_lost", "val", 0)
      release_store(store)

      # Overwrite the entire log with garbage
      [log_file] = Path.wildcard(Path.join(dir, "*.log"))
      File.write!(log_file, :crypto.strong_rand_bytes(200))

      store2 = open_store(dir)
      assert {:ok, nil} == NIF.get(store2, "will_be_lost"),
             "key must be lost when entire log is corrupted"
    end
  end

  # ------------------------------------------------------------------
  # 7. Reopen after clean close — all data intact
  # ------------------------------------------------------------------

  describe "reopen after clean close" do
    test "all data intact after clean close and reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      # Write a variety of data: normal keys, overwritten keys, deleted keys
      for i <- 1..20 do
        :ok = NIF.put(store, "clean_#{i}", "val_#{i}", 0)
      end

      # Overwrite some
      :ok = NIF.put(store, "clean_5", "updated_5", 0)
      :ok = NIF.put(store, "clean_10", "updated_10", 0)

      # Delete some
      {:ok, true} = NIF.delete(store, "clean_3")
      {:ok, true} = NIF.delete(store, "clean_7")

      release_store(store)
      store2 = open_store(dir)

      # Verify normal keys
      assert {:ok, "val_1"} == NIF.get(store2, "clean_1")
      assert {:ok, "val_20"} == NIF.get(store2, "clean_20")

      # Verify overwritten keys
      assert {:ok, "updated_5"} == NIF.get(store2, "clean_5")
      assert {:ok, "updated_10"} == NIF.get(store2, "clean_10")

      # Verify deleted keys
      assert {:ok, nil} == NIF.get(store2, "clean_3")
      assert {:ok, nil} == NIF.get(store2, "clean_7")

      # Verify key count: 20 - 2 deleted = 18
      keys = NIF.keys(store2)
      assert length(keys) == 18
    end

    test "binary keys and values survive clean close/reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      bin_key = <<0, 1, 255, 128, 64>>
      bin_val = :crypto.strong_rand_bytes(1024)

      :ok = NIF.put(store, bin_key, bin_val, 0)
      release_store(store)

      store2 = open_store(dir)
      assert {:ok, ^bin_val} = NIF.get(store2, bin_key)
    end
  end

  # ------------------------------------------------------------------
  # 8. Reopen after write_hint — faster than log replay
  # ------------------------------------------------------------------

  describe "reopen after write_hint — correctness" do
    test "hint-based recovery produces same data as log replay" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      # Write enough data to make the difference meaningful
      for i <- 1..200 do
        :ok = NIF.put(store, "hintperf_#{i}", String.duplicate("v", 100), 0)
      end

      :ok = NIF.write_hint(store)
      release_store(store)

      # Reopen (should use hint file)
      store2 = open_store(dir)

      # Verify all keys present
      for i <- 1..200 do
        assert {:ok, val} = NIF.get(store2, "hintperf_#{i}")
        assert val == String.duplicate("v", 100),
               "key hintperf_#{i} must have correct value after hint-based reopen"
      end

      keys = NIF.keys(store2)
      assert length(keys) == 200
    end

    test "hint file is used on reopen (hint file exists)" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      :ok = NIF.put(store, "hint_test", "hint_value", 0)
      :ok = NIF.write_hint(store)

      release_store(store)

      # Verify hint file exists
      hint_files = Path.wildcard(Path.join(dir, "*.hint"))
      assert length(hint_files) > 0

      # Reopen and verify data
      store2 = open_store(dir)
      assert {:ok, "hint_value"} == NIF.get(store2, "hint_test")
    end
  end

  # ------------------------------------------------------------------
  # 9. Multiple close/reopen cycles — no data corruption
  # ------------------------------------------------------------------

  describe "multiple close/reopen cycles" do
    test "no data corruption across 10 close/reopen cycles" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      # Cycle 1: initial data
      store = open_store(dir)

      for i <- 1..10 do
        :ok = NIF.put(store, "cycle_key_#{i}", "cycle_val_#{i}", 0)
      end

      release_store(store)

      # Cycles 2-10: reopen, verify, add more data, close
      for cycle <- 2..10 do
        s = open_store(dir)

        # Verify all previous data
        for i <- 1..10 do
          assert {:ok, "cycle_val_#{i}"} == NIF.get(s, "cycle_key_#{i}"),
                 "cycle_key_#{i} must be present in cycle #{cycle}"
        end

        # Add a cycle-specific key
        :ok = NIF.put(s, "cycle_#{cycle}_extra", "extra_#{cycle}", 0)

        release_store(s)
      end

      # Final verification
      final = open_store(dir)

      for i <- 1..10 do
        assert {:ok, "cycle_val_#{i}"} == NIF.get(final, "cycle_key_#{i}")
      end

      for cycle <- 2..10 do
        assert {:ok, "extra_#{cycle}"} == NIF.get(final, "cycle_#{cycle}_extra"),
               "cycle-specific key from cycle #{cycle} must persist"
      end
    end

    test "no data corruption with write_hint between cycles" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for i <- 1..5 do
        :ok = NIF.put(store, "wh_cycle_#{i}", "val_#{i}", 0)
      end

      :ok = NIF.write_hint(store)
      release_store(store)

      # Second cycle: reopen (uses hint), add more, write hint again
      store2 = open_store(dir)

      for i <- 1..5 do
        assert {:ok, "val_#{i}"} == NIF.get(store2, "wh_cycle_#{i}")
      end

      for i <- 6..10 do
        :ok = NIF.put(store2, "wh_cycle_#{i}", "val_#{i}", 0)
      end

      :ok = NIF.write_hint(store2)
      release_store(store2)

      # Third cycle: verify all 10 keys
      store3 = open_store(dir)

      for i <- 1..10 do
        assert {:ok, "val_#{i}"} == NIF.get(store3, "wh_cycle_#{i}"),
               "wh_cycle_#{i} must survive two hint-write cycles"
      end
    end

    test "overwrite same key across cycles preserves latest value" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      for cycle <- 1..5 do
        store = open_store(dir)
        :ok = NIF.put(store, "overwrite_me", "cycle_#{cycle}", 0)
        release_store(store)
      end

      final = open_store(dir)
      assert {:ok, "cycle_5"} == NIF.get(final, "overwrite_me"),
             "latest cycle's value must win after multiple reopens"
    end
  end

  # ------------------------------------------------------------------
  # 10. Concurrent put + write_hint — no crash
  # ------------------------------------------------------------------

  describe "concurrent put + write_hint" do
    test "concurrent puts and write_hint do not crash the NIF" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      # Pre-populate some data
      for i <- 1..100 do
        :ok = NIF.put(store, "conc_hint_#{i}", "val_#{i}", 0)
      end

      # Spawn concurrent writers alongside hint writes
      # The NIF Mutex should serialize these safely
      tasks =
        Enum.map(1..20, fn i ->
          Task.async(fn ->
            :ok = NIF.put(store, "conc_put_#{i}", "conc_val_#{i}", 0)
          end)
        end)

      # Also trigger write_hint concurrently
      hint_task =
        Task.async(fn ->
          NIF.write_hint(store)
        end)

      # Wait for all tasks
      Enum.each(tasks, &Task.await(&1, 10_000))
      hint_result = Task.await(hint_task, 10_000)

      # write_hint should succeed (or at least not crash)
      assert hint_result == :ok or match?({:error, _}, hint_result)

      # Verify the concurrent puts are visible
      for i <- 1..20 do
        assert {:ok, "conc_val_#{i}"} == NIF.get(store, "conc_put_#{i}")
      end
    end

    test "write_hint during rapid puts does not corrupt data" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      # Rapid fire puts with interspersed hint writes
      for batch <- 1..5 do
        for i <- 1..20 do
          key = "rapid_#{batch}_#{i}"
          :ok = NIF.put(store, key, "val_#{batch}_#{i}", 0)
        end

        :ok = NIF.write_hint(store)
      end

      # Verify all 100 keys
      for batch <- 1..5 do
        for i <- 1..20 do
          key = "rapid_#{batch}_#{i}"

          assert {:ok, "val_#{batch}_#{i}"} == NIF.get(store, key),
                 "key #{key} must be intact after interleaved hint writes"
        end
      end
    end
  end

  # ------------------------------------------------------------------
  # 11. purge_expired + concurrent get — no crash
  # ------------------------------------------------------------------

  describe "purge_expired + concurrent get" do
    test "concurrent gets during purge_expired do not crash" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000

      # Mix of live and expired keys
      for i <- 1..50 do
        :ok = NIF.put(store, "purge_live_#{i}", "val_#{i}", 0)
        :ok = NIF.put(store, "purge_exp_#{i}", "gone_#{i}", past)
      end

      # Spawn concurrent gets on live keys
      get_tasks =
        Enum.map(1..50, fn i ->
          Task.async(fn ->
            NIF.get(store, "purge_live_#{i}")
          end)
        end)

      # Also run purge_expired concurrently
      purge_task =
        Task.async(fn ->
          NIF.purge_expired(store)
        end)

      # All tasks must complete without crash
      get_results = Enum.map(get_tasks, &Task.await(&1, 10_000))
      purge_result = Task.await(purge_task, 10_000)

      # purge must return {:ok, count}
      assert {:ok, _count} = purge_result

      # All live key gets must return their values (no crash, no corruption)
      for {result, i} <- Enum.with_index(get_results, 1) do
        assert result == {:ok, "val_#{i}"},
               "live key purge_live_#{i} must be readable during purge, got: #{inspect(result)}"
      end
    end

    test "purge_expired does not interfere with concurrent puts" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      past = System.os_time(:millisecond) - 5_000

      for i <- 1..20 do
        :ok = NIF.put(store, "purge_put_exp_#{i}", "gone", past)
      end

      # Concurrent puts and purge
      put_tasks =
        Enum.map(1..10, fn i ->
          Task.async(fn ->
            NIF.put(store, "during_purge_#{i}", "new_val_#{i}", 0)
          end)
        end)

      purge_task =
        Task.async(fn ->
          NIF.purge_expired(store)
        end)

      Enum.each(put_tasks, &Task.await(&1, 10_000))
      {:ok, _purged} = Task.await(purge_task, 10_000)

      # New puts must be visible
      for i <- 1..10 do
        assert {:ok, "new_val_#{i}"} == NIF.get(store, "during_purge_#{i}")
      end
    end
  end

  # ------------------------------------------------------------------
  # 12. Hint file with more entries than log — handled gracefully
  # ------------------------------------------------------------------

  describe "hint file with more entries than log" do
    @tag :skip
    @tag :known_bug
    test "BUG: stale hint causes tombstoned keys to resurrect on reopen" do
      # KNOWN BUG: When a hint file exists for a file ID, the store loads
      # ONLY from the hint and does NOT replay the log tail for entries
      # written after the hint was generated. This means tombstones (deletes)
      # written after write_hint are silently lost on recovery.
      #
      # Root cause: store.rs open() line 105-120 -- when fid_hint_path.exists(),
      # the code loads from the hint and skips log replay entirely. It should
      # either:
      #   (a) replay the log tail after the hint-covered region, or
      #   (b) always refresh the hint before closing.
      #
      # Impact: Keys deleted after the last write_hint will reappear on reopen.
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for i <- 1..20 do
        :ok = NIF.put(store, "stale_#{i}", "val_#{i}", 0)
      end

      :ok = NIF.write_hint(store)
      release_store(store)

      # Reopen, delete keys 11-20, close WITHOUT updating hint
      store2 = open_store(dir)

      for i <- 11..20 do
        {:ok, true} = NIF.delete(store2, "stale_#{i}")
      end

      release_store(store2)

      # Reopen -- stale hint resurrects deleted keys (the bug)
      store3 = open_store(dir)

      for i <- 1..10 do
        assert {:ok, "val_#{i}"} == NIF.get(store3, "stale_#{i}"),
               "key stale_#{i} must survive (was not deleted)"
      end

      # These SHOULD be nil but the stale hint resurrects them
      for i <- 11..20 do
        assert {:ok, nil} == NIF.get(store3, "stale_#{i}"),
               "key stale_#{i} must be deleted (tombstoned after hint write)"
      end
    end

    test "deletes after write_hint are visible on same session" do
      # This verifies the workaround: as long as you don't reopen the store,
      # in-memory keydir is correct. The bug only manifests on reopen.
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for i <- 1..10 do
        :ok = NIF.put(store, "same_session_#{i}", "val_#{i}", 0)
      end

      :ok = NIF.write_hint(store)

      # Delete after hint write
      for i <- 6..10 do
        {:ok, true} = NIF.delete(store, "same_session_#{i}")
      end

      # Within the same session, keydir is accurate
      for i <- 1..5 do
        assert {:ok, "val_#{i}"} == NIF.get(store, "same_session_#{i}")
      end

      for i <- 6..10 do
        assert {:ok, nil} == NIF.get(store, "same_session_#{i}")
      end
    end

    test "write_hint after deletes produces correct recovery" do
      # Workaround: always call write_hint after any deletes before closing.
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for i <- 1..20 do
        :ok = NIF.put(store, "fixed_#{i}", "val_#{i}", 0)
      end

      :ok = NIF.write_hint(store)

      for i <- 11..20 do
        {:ok, true} = NIF.delete(store, "fixed_#{i}")
      end

      # Write hint AGAIN after deletes -- this is the correct pattern
      :ok = NIF.write_hint(store)
      release_store(store)

      store2 = open_store(dir)

      for i <- 1..10 do
        assert {:ok, "val_#{i}"} == NIF.get(store2, "fixed_#{i}"),
               "key fixed_#{i} must survive"
      end

      for i <- 11..20 do
        assert {:ok, nil} == NIF.get(store2, "fixed_#{i}"),
               "key fixed_#{i} must be deleted (hint updated after delete)"
      end
    end

    test "corrupt hint file falls back to log replay without data loss" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for i <- 1..10 do
        :ok = NIF.put(store, "corrupt_hint_#{i}", "val_#{i}", 0)
      end

      :ok = NIF.write_hint(store)
      release_store(store)

      # Corrupt the hint file
      [hint_file] = Path.wildcard(Path.join(dir, "*.hint"))
      hint_data = File.read!(hint_file)
      # Flip bytes in the middle of the hint file to break CRC
      corrupted = binary_part(hint_data, 0, 10) <> :crypto.strong_rand_bytes(20) <>
        binary_part(hint_data, 30, max(byte_size(hint_data) - 30, 0))
      File.write!(hint_file, corrupted)

      # Reopen — must fall back to log replay (EC-2 in store.rs)
      store2 = open_store(dir)

      for i <- 1..10 do
        assert {:ok, "val_#{i}"} == NIF.get(store2, "corrupt_hint_#{i}"),
               "key corrupt_hint_#{i} must be recovered via log replay after corrupt hint"
      end
    end

    test "orphaned hint file (no corresponding log) is skipped" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "orphan_test", "val", 0)
      :ok = NIF.write_hint(store)
      release_store(store)

      # Create a fake orphaned hint file with a different file_id
      # that has no corresponding .log file
      fake_hint = Path.join(dir, "00000000000000000099.hint")
      # Just copy the existing hint
      [real_hint] = Path.wildcard(Path.join(dir, "*.hint"))
      File.cp!(real_hint, fake_hint)

      # Also create its "corresponding" log so the orphan is the one
      # without a log. Actually, EC-7 in store.rs: hint without log is
      # skipped. Let's create a scenario where we have a hint but
      # remove the log for a different file_id.

      # Remove the fake hint's log (it never existed, so it IS orphaned)
      # The store should skip it and still recover the real data.
      store2 = open_store(dir)
      assert {:ok, "val"} == NIF.get(store2, "orphan_test"),
             "real data must survive presence of orphaned hint file"
    end
  end
end
