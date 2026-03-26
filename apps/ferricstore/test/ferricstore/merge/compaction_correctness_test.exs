defmodule Ferricstore.Merge.CompactionCorrectnessTest do
  @moduledoc """
  End-to-end compaction correctness tests that verify the Bitcask merge/compaction
  process does not lose data under various scenarios.

  These tests operate at the NIF level (`NIF.new/1` + `NIF.run_compaction/2`)
  for deterministic control over when compaction runs and what files are merged.
  """

  use ExUnit.Case, async: false

  @moduletag :compaction

  alias Ferricstore.Bitcask.NIF

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp temp_dir do
    dir = Path.join(System.tmp_dir!(), "compaction_correctness_#{:erlang.unique_integer([:positive])}")
    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    dir
  end

  # Returns {active_fid, merge_fids} or :single_file if only one file exists.
  defp mergeable_files(store) do
    {:ok, file_sizes} = NIF.file_sizes(store)
    all_fids = Enum.map(file_sizes, fn {fid, _} -> fid end)

    case all_fids do
      [_single] -> :single_file
      _ ->
        active_fid = Enum.max(all_fids)
        merge_fids = Enum.reject(all_fids, &(&1 == active_fid))
        {active_fid, merge_fids}
    end
  end

  # -------------------------------------------------------------------
  # 1. Basic compaction: live keys survive
  # -------------------------------------------------------------------

  describe "basic compaction: live keys survive" do
    test "all 100 current values are correct after compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Write 100 keys
      for i <- 1..100 do
        :ok = NIF.put(store, "key_#{i}", "original_#{i}", 0)
      end

      # Overwrite 50 of them to create dead entries
      for i <- 1..50 do
        :ok = NIF.put(store, "key_#{i}", "updated_#{i}", 0)
      end

      # Force more writes to create multiple log files for merge
      for _round <- 1..20 do
        for i <- 1..50 do
          :ok = NIF.put(store, "key_#{i}", "updated_#{i}", 0)
        end
      end

      case mergeable_files(store) do
        :single_file ->
          # All data in one file -- compact with empty list is a no-op.
          {:ok, {0, 0, 0}} = NIF.run_compaction(store, [])

        {_active_fid, merge_fids} ->
          {:ok, {written, dropped, _reclaimed}} = NIF.run_compaction(store, merge_fids)
          assert written >= 0
          assert dropped >= 0
      end

      # Verify all 100 keys have their correct current values
      for i <- 1..50 do
        {:ok, val} = NIF.get(store, "key_#{i}")
        assert val == "updated_#{i}", "key_#{i} should have the updated value after compaction"
      end

      for i <- 51..100 do
        {:ok, val} = NIF.get(store, "key_#{i}")
        assert val == "original_#{i}", "key_#{i} should retain its original value after compaction"
      end
    end
  end

  # -------------------------------------------------------------------
  # 2. Deleted keys don't resurrect
  # -------------------------------------------------------------------

  describe "deleted keys don't resurrect" do
    test "deleted keys stay deleted after compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Write 100 keys with overwrites to create multiple files
      for _round <- 1..20 do
        for i <- 1..100 do
          :ok = NIF.put(store, "del_key_#{i}", "value_#{i}", 0)
        end
      end

      # Delete 50 keys
      for i <- 1..50 do
        {:ok, _} = NIF.delete(store, "del_key_#{i}")
      end

      # Verify deletes took effect before compaction
      for i <- 1..50 do
        {:ok, val} = NIF.get(store, "del_key_#{i}")
        assert val == nil, "del_key_#{i} should be nil before compaction"
      end

      case mergeable_files(store) do
        :single_file ->
          {:ok, {0, 0, 0}} = NIF.run_compaction(store, [])

        {_active_fid, merge_fids} ->
          {:ok, {_written, _dropped, _reclaimed}} = NIF.run_compaction(store, merge_fids)
      end

      # Verify deleted keys are still deleted after compaction (no resurrection)
      for i <- 1..50 do
        {:ok, val} = NIF.get(store, "del_key_#{i}")
        assert val == nil, "del_key_#{i} must NOT resurrect after compaction"
      end

      # Verify surviving keys are intact
      for i <- 51..100 do
        {:ok, val} = NIF.get(store, "del_key_#{i}")
        assert val == "value_#{i}", "del_key_#{i} should survive compaction"
      end
    end
  end

  # -------------------------------------------------------------------
  # 3. Expired keys cleaned up
  # -------------------------------------------------------------------

  describe "expired keys cleaned up" do
    test "expired keys are gone after compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Write 100 keys with 1ms TTL (expire_at_ms in the past)
      past_ms = System.os_time(:millisecond) - 10_000

      for _round <- 1..20 do
        for i <- 1..100 do
          :ok = NIF.put(store, "ttl_key_#{i}", "value_#{i}", past_ms)
        end
      end

      # Also write 20 permanent keys to verify they survive
      for i <- 1..20 do
        :ok = NIF.put(store, "perm_key_#{i}", "permanent_#{i}", 0)
      end

      # Wait a tiny bit to be sure expiry has passed
      Process.sleep(5)

      case mergeable_files(store) do
        :single_file ->
          # Even without multi-file merge, purge_expired should clean up
          {:ok, purged} = NIF.purge_expired(store)
          assert purged > 0, "purge_expired should remove expired keys"

        {_active_fid, merge_fids} ->
          {:ok, {_written, dropped, _reclaimed}} = NIF.run_compaction(store, merge_fids)
          assert dropped > 0, "expired entries should be dropped during compaction"
      end

      # Verify permanent keys survive
      for i <- 1..20 do
        {:ok, val} = NIF.get(store, "perm_key_#{i}")
        assert val == "permanent_#{i}", "perm_key_#{i} should survive compaction"
      end
    end
  end

  # -------------------------------------------------------------------
  # 4. Compaction during writes
  # -------------------------------------------------------------------

  describe "compaction during writes" do
    test "no data loss when writes happen around compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Phase 1: Write initial batch to create multiple files
      for _round <- 1..20 do
        for i <- 1..50 do
          :ok = NIF.put(store, "cw_key_#{i}", "pre_compact_#{i}", 0)
        end
      end

      # Run compaction on non-active files
      case mergeable_files(store) do
        :single_file -> :ok
        {_active_fid, merge_fids} ->
          {:ok, _} = NIF.run_compaction(store, merge_fids)
      end

      # Phase 2: Write more keys AFTER compaction
      for i <- 51..100 do
        :ok = NIF.put(store, "cw_key_#{i}", "post_compact_#{i}", 0)
      end

      # Verify all data from both phases is intact
      for i <- 1..50 do
        {:ok, val} = NIF.get(store, "cw_key_#{i}")
        assert val == "pre_compact_#{i}", "cw_key_#{i} (pre-compact) should survive"
      end

      for i <- 51..100 do
        {:ok, val} = NIF.get(store, "cw_key_#{i}")
        assert val == "post_compact_#{i}", "cw_key_#{i} (post-compact) should be readable"
      end
    end
  end

  # -------------------------------------------------------------------
  # 5. Multiple compaction cycles
  # -------------------------------------------------------------------

  describe "multiple compaction cycles" do
    test "all data from all rounds survives three compaction cycles" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # --- Round 1 ---
      for _r <- 1..15 do
        for i <- 1..30 do
          :ok = NIF.put(store, "r1_key_#{i}", "round1_#{i}", 0)
        end
      end

      case mergeable_files(store) do
        :single_file -> :ok
        {_, fids} -> {:ok, _} = NIF.run_compaction(store, fids)
      end

      # Verify round 1 keys
      for i <- 1..30 do
        {:ok, val} = NIF.get(store, "r1_key_#{i}")
        assert val == "round1_#{i}", "r1_key_#{i} should survive first compaction"
      end

      # --- Round 2 ---
      for _r <- 1..15 do
        for i <- 1..30 do
          :ok = NIF.put(store, "r2_key_#{i}", "round2_#{i}", 0)
        end
      end

      case mergeable_files(store) do
        :single_file -> :ok
        {_, fids} -> {:ok, _} = NIF.run_compaction(store, fids)
      end

      # Verify both rounds
      for i <- 1..30 do
        {:ok, val} = NIF.get(store, "r1_key_#{i}")
        assert val == "round1_#{i}", "r1_key_#{i} should survive second compaction"
      end

      for i <- 1..30 do
        {:ok, val} = NIF.get(store, "r2_key_#{i}")
        assert val == "round2_#{i}", "r2_key_#{i} should survive second compaction"
      end

      # --- Round 3 ---
      for _r <- 1..15 do
        for i <- 1..30 do
          :ok = NIF.put(store, "r3_key_#{i}", "round3_#{i}", 0)
        end
      end

      case mergeable_files(store) do
        :single_file -> :ok
        {_, fids} -> {:ok, _} = NIF.run_compaction(store, fids)
      end

      # Final verification: all three rounds present
      for i <- 1..30 do
        {:ok, val} = NIF.get(store, "r1_key_#{i}")
        assert val == "round1_#{i}", "r1_key_#{i} should survive third compaction"
      end

      for i <- 1..30 do
        {:ok, val} = NIF.get(store, "r2_key_#{i}")
        assert val == "round2_#{i}", "r2_key_#{i} should survive third compaction"
      end

      for i <- 1..30 do
        {:ok, val} = NIF.get(store, "r3_key_#{i}")
        assert val == "round3_#{i}", "r3_key_#{i} should survive third compaction"
      end
    end
  end

  # -------------------------------------------------------------------
  # 6. Large values survive compaction
  # -------------------------------------------------------------------

  describe "large values survive compaction" do
    test "10 keys with 100KB values have byte-for-byte integrity after compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Generate distinct 100KB values per key
      large_values =
        for i <- 1..10 do
          # Each key gets a unique repeating byte pattern
          value = String.duplicate("#{i}" <> String.duplicate("x", 99), 1000)
          :ok = NIF.put(store, "large_key_#{i}", value, 0)
          {i, value}
        end

      # Create fragmentation via overwrites of smaller padding keys
      for _round <- 1..20 do
        for j <- 1..20 do
          :ok = NIF.put(store, "pad_#{j}", String.duplicate("p", 200), 0)
        end
      end

      case mergeable_files(store) do
        :single_file -> :ok
        {_, fids} -> {:ok, _} = NIF.run_compaction(store, fids)
      end

      # Verify byte-for-byte integrity
      for {i, expected_value} <- large_values do
        {:ok, val} = NIF.get(store, "large_key_#{i}")
        assert val == expected_value,
               "large_key_#{i} should have identical value after compaction " <>
                 "(expected #{byte_size(expected_value)} bytes, got #{if val, do: byte_size(val), else: "nil"})"
      end
    end
  end

  # -------------------------------------------------------------------
  # 7. Hint files generated after compaction
  # -------------------------------------------------------------------

  describe "hint files generated after compaction" do
    test "hint files exist after compaction" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Write enough data with overwrites to create multiple files
      for _round <- 1..20 do
        for i <- 1..20 do
          :ok = NIF.put(store, "hint_key_#{i}", String.duplicate("h", 100), 0)
        end
      end

      case mergeable_files(store) do
        :single_file ->
          # Cannot test hint file generation without multi-file compaction;
          # just verify the store is healthy.
          for i <- 1..20 do
            {:ok, val} = NIF.get(store, "hint_key_#{i}")
            assert val != nil
          end

        {_active_fid, merge_fids} ->
          # Count hint files before compaction
          {:ok, files_before} = File.ls(dir)
          hints_before = Enum.filter(files_before, &String.ends_with?(&1, ".hint"))

          {:ok, {written, _dropped, _reclaimed}} = NIF.run_compaction(store, merge_fids)
          assert written > 0, "compaction should write live records"

          # After compaction, a new hint file should exist
          {:ok, files_after} = File.ls(dir)
          hints_after = Enum.filter(files_after, &String.ends_with?(&1, ".hint"))

          assert length(hints_after) > length(hints_before),
                 "compaction should create new hint file(s); " <>
                   "before: #{inspect(hints_before)}, after: #{inspect(hints_after)}"

          # Verify all data still readable (hint file didn't corrupt state)
          for i <- 1..20 do
            {:ok, val} = NIF.get(store, "hint_key_#{i}")
            assert val != nil, "hint_key_#{i} should be readable after compaction with hint"
          end
      end
    end
  end

  # -------------------------------------------------------------------
  # 8. Compaction reduces disk usage
  # -------------------------------------------------------------------

  describe "compaction reduces disk usage" do
    test "disk usage is significantly smaller after compacting overwritten keys" do
      dir = temp_dir()
      {:ok, store} = NIF.new(dir)

      # Write 1000 keys
      for i <- 1..1000 do
        :ok = NIF.put(store, "disk_key_#{i}", "initial_value_#{i}", 0)
      end

      # Overwrite all 1000 keys multiple times to create heavy fragmentation
      for _round <- 1..10 do
        for i <- 1..1000 do
          :ok = NIF.put(store, "disk_key_#{i}", "overwritten_#{i}", 0)
        end
      end

      # Measure disk usage before compaction
      {:ok, files_before} = File.ls(dir)

      total_before =
        files_before
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reduce(0, fn name, acc ->
          case File.stat(Path.join(dir, name)) do
            {:ok, %{size: s}} -> acc + s
            _ -> acc
          end
        end)

      case mergeable_files(store) do
        :single_file ->
          # With only one file, we can still verify data integrity
          for i <- 1..1000 do
            {:ok, val} = NIF.get(store, "disk_key_#{i}")
            assert val == "overwritten_#{i}"
          end

        {_active_fid, merge_fids} ->
          {:ok, {_written, _dropped, reclaimed}} = NIF.run_compaction(store, merge_fids)

          # Measure disk usage after compaction
          {:ok, files_after} = File.ls(dir)

          total_after =
            files_after
            |> Enum.filter(&String.ends_with?(&1, ".log"))
            |> Enum.reduce(0, fn name, acc ->
              case File.stat(Path.join(dir, name)) do
                {:ok, %{size: s}} -> acc + s
                _ -> acc
              end
            end)

          assert total_after < total_before,
                 "disk usage should decrease after compaction; " <>
                   "before: #{total_before} bytes, after: #{total_after} bytes"

          assert reclaimed > 0, "run_compaction should report bytes reclaimed > 0"

          # Verify all data still readable
          for i <- 1..1000 do
            {:ok, val} = NIF.get(store, "disk_key_#{i}")
            assert val == "overwritten_#{i}", "disk_key_#{i} should survive compaction"
          end
      end
    end
  end
end
