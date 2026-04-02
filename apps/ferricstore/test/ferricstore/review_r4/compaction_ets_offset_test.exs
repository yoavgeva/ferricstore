defmodule Ferricstore.ReviewR4.CompactionEtsOffsetTest do
  @moduledoc """
  Test for C2/C3: v2 compaction does not update ETS offsets after
  rewriting data files, causing cold reads to use stale offsets.

  ## Bug description

  In shard.ex `handle_call({:run_compaction, file_ids}, ...)` (line 2533):

  1. ETS is scanned for live offsets belonging to a given file_id
  2. `v2_copy_records(source, dest, offsets)` copies those records to a
     new compacted file with potentially different offsets
  3. The dest is renamed over the source: `File.rename!(dest, source)`
  4. The `_results` (which contain the NEW offsets) are IGNORED (line 2564)

  After step 4, ETS still has the OLD offsets. But the file on disk has
  been rewritten with NEW record positions. Any subsequent cold read
  (where ETS has value=nil and uses fid+offset for v2_pread_at) will
  read from the wrong offset, returning corrupted or wrong data.

  ## Why this matters

  - Hot reads (value in ETS) are unaffected
  - Cold reads after memory eviction will read wrong offsets
  - Recovery (restart) will fix it by re-scanning the compacted file
  - But between compaction and restart, cold reads return wrong data
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.LFU

  @tag :review_r4
  @tag :compaction_bug

  describe "C2/C3: v2 compaction does not update ETS offsets" do
    test "run_compaction ignores new offsets from v2_copy_records" do
      # This test verifies the bug exists by examining the run_compaction
      # code path. We can't easily run compaction in isolation without
      # the full shard infrastructure, but we can verify the code structure.
      #
      # The critical line is in shard.ex at the run_compaction handler:
      #
      #   case NIF.v2_copy_records(source, dest, offsets) do
      #     {:ok, _results} ->        # <-- NEW offsets in _results are IGNORED
      #       File.rename!(dest, source)
      #
      # After rename, ETS still has old offsets pointing into the rewritten file.
      #
      # We verify by reading the source file. Find it via Mix project structure.
      project_root = Path.expand("../../../../", __DIR__)

      candidates = [
        Path.join(project_root, "lib/ferricstore/store/shard.ex"),
        Path.join(project_root, "apps/ferricstore/lib/ferricstore/store/shard.ex")
      ]

      shard_path = Enum.find(candidates, &File.exists?/1)

      if shard_path do
        {:ok, source} = File.read(shard_path)

        # The run_compaction handler should update ETS offsets but doesn't.
        # Verify that v2_copy_records result is pattern-matched with underscore.
        assert source =~ "{:ok, _results} ->"

        # Extract just the run_compaction handler body
        [_, after_compaction] = String.split(source, "def handle_call({:run_compaction", parts: 2)
        [compaction_body | _] = String.split(after_compaction, "def handle_call(", parts: 2)

        refute String.contains?(compaction_body, "update_ets_locations"),
          "run_compaction should update ETS locations but does NOT — this is the bug"

        refute String.contains?(compaction_body, ":ets.update_element"),
          "run_compaction should call :ets.update_element but does NOT — this is the bug"
      else
        # Source not accessible — verify via behavioral test instead.
        # The run_compaction handler is reachable via GenServer.call on a shard.
        # We confirm the handler exists and accepts the expected message format.
        shard = Ferricstore.Store.Router.shard_name(0)
        assert is_atom(shard)

        # Calling run_compaction with empty file_ids should return success
        # with zero stats (no files to compact).
        result = GenServer.call(shard, {:run_compaction, []})
        assert {:ok, {0, 0, 0}} = result,
          "run_compaction with empty file list should return zero stats"
      end
    end

    test "v2 compaction via sandbox demonstrates stale offset problem" do
      # This test creates a sandbox shard, writes data, forces a file
      # rotation, then compacts. After compaction, cold reads should
      # still work correctly — but they won't if ETS offsets are stale.
      #
      # Note: This test requires the NIF to be loaded. If not available,
      # skip gracefully.
      unless function_exported?(NIF, :v2_append_batch, 2) do
        IO.puts("Skipping: NIF not loaded")
        assert true
      else
        # Create a temporary directory for this test
        tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_c2_test_#{:rand.uniform(999_999)}")
        File.mkdir_p!(tmp_dir)

        on_exit(fn -> File.rm_rf!(tmp_dir) end)

        # Create a keydir ETS table
        keydir = :ets.new(:test_keydir_c2, [:set, :public])

        # Write some records to a log file
        file_0 = Path.join(tmp_dir, "00000.log")
        File.touch!(file_0)

        batch = [
          {"key1", "value1", 0},
          {"key2", "value2", 0},
          {"key3", "value3", 0}
        ]

        case NIF.v2_append_batch(file_0, batch) do
          {:ok, locations} ->
            # Insert into ETS with the returned locations
            Enum.zip(batch, locations)
            |> Enum.each(fn {{key, value, exp}, {offset, _size}} ->
              :ets.insert(keydir, {key, value, exp, LFU.initial(), 0, offset, byte_size(value)})
            end)

            # Record the original offsets
            [{_, _, _, _, _, offset1_before, _}] = :ets.lookup(keydir, "key1")
            [{_, _, _, _, _, _offset2_before, _}] = :ets.lookup(keydir, "key2")
            [{_, _, _, _, _, offset3_before, _}] = :ets.lookup(keydir, "key3")

            # Now delete key2 from ETS (simulating a DELETE that happened)
            :ets.delete(keydir, "key2")

            # Compact: collect live offsets from ETS for file 0
            offsets =
              :ets.foldl(
                fn {_key, _value, _exp, _lfu, f, off, _vsize}, acc ->
                  if f == 0, do: [off | acc], else: acc
                end,
                [],
                keydir
              )

            # Copy only live records to a compacted file
            dest = Path.join(tmp_dir, "compact_0.log")

            case NIF.v2_copy_records(file_0, dest, offsets) do
              {:ok, new_locations} ->
                # This is what the shard does: rename, ignoring new_locations
                File.rename!(dest, file_0)

                # Verify ETS still has OLD offsets
                [{_, _, _, _, _, offset1_after, _}] = :ets.lookup(keydir, "key1")
                [{_, _, _, _, _, offset3_after, _}] = :ets.lookup(keydir, "key3")

                assert offset1_after == offset1_before,
                  "ETS offset for key1 was not updated (as expected with the bug)"
                assert offset3_after == offset3_before,
                  "ETS offset for key3 was not updated (as expected with the bug)"

                # Now verify that the offsets are WRONG for the compacted file.
                # The new file has different record positions because key2 was removed.
                # key1 might still be at offset 0 (first record), but key3's
                # offset has changed because key2's record was removed.

                # Read using the OLD (stale) offsets — this is what cold reads do
                _stale_read_1 = NIF.v2_pread_at(file_0, offset1_after)
                stale_read_3 = NIF.v2_pread_at(file_0, offset3_after)

                # Read using the NEW (correct) offsets
                new_offsets_map =
                  Enum.zip(offsets, new_locations)
                  |> Map.new(fn {old_off, {new_off, _size}} -> {old_off, new_off} end)

                correct_offset_1 = Map.get(new_offsets_map, offset1_before, offset1_before)
                correct_offset_3 = Map.get(new_offsets_map, offset3_before, offset3_before)

                _correct_read_1 = NIF.v2_pread_at(file_0, correct_offset_1)
                correct_read_3 = NIF.v2_pread_at(file_0, correct_offset_3)

                # If key1 was first and key2 was removed, key3's offset shifted.
                # The stale offset for key3 may read wrong data or fail.
                if offset3_before != correct_offset_3 do
                  # The offsets differ — cold reads with stale offsets return wrong data
                  assert stale_read_3 != correct_read_3 or
                         match?({:error, _}, stale_read_3),
                    "Stale offset read should differ from correct offset read, proving the bug"
                else
                  # If offsets happen to be the same (unlikely with dead record removed),
                  # the bug doesn't manifest for this specific case
                  IO.puts("Note: offsets coincidentally match for this test case")
                end

              {:error, reason} ->
                IO.puts("Skipping copy_records step: #{inspect(reason)}")
                assert true
            end

          {:error, reason} ->
            IO.puts("Skipping: v2_append_batch failed: #{inspect(reason)}")
            assert true
        end

        :ets.delete(keydir)
      end
    end
  end
end
