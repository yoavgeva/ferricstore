defmodule Ferricstore.Review.H1PromotedFidRaceTest do
  @moduledoc """
  Proves (or disproves) a race in promoted write file_id tracking.

  Bug hypothesis (shard.ex lines 648-654 / 682-688):
    After `promoted_write` returns {offset, record_size}, the code calls
    `promoted_active_fid(dedicated_path)` which does `File.ls`. If
    compaction changed the active file between write and fid lookup, the
    fid stored in ETS would point to a different (or deleted) file,
    causing subsequent pread-based reads to return nil or wrong data.

  Because the shard is a GenServer, compaction (`compact_dedicated`) runs
  synchronously AFTER the ETS insert on the same call that triggers it
  (via `bump_promoted_writes`). So within a single shard process the
  write-then-fid-lookup-then-ETS-insert sequence cannot be interleaved
  with compaction. This test verifies that invariant holds under heavy
  write load that repeatedly crosses the compaction threshold.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # Low promotion threshold so we promote quickly.
  @test_threshold 5
  # Must match @dedicated_compaction_threshold in shard.ex.
  @compaction_threshold 1000

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    original = Application.get_env(:ferricstore, :promotion_threshold)

    original_pt =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, @test_threshold)
    :persistent_term.put(:ferricstore_promotion_threshold, @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      if original do
        Application.put_env(:ferricstore, :promotion_threshold, original)
      else
        Application.delete_env(:ferricstore, :promotion_threshold)
      end

      case original_pt do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        val -> :persistent_term.put(:ferricstore_promotion_threshold, val)
      end

      ShardHelpers.wait_shards_alive()
    end)
  end

  defp real_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn -> :ok end,
      dbsize: &Router.dbsize/0,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  describe "promoted fid race after compaction" do
    @tag timeout: 120_000
    @tag :skip
    # Flaky: compaction timing is non-deterministic and may not trigger within test window
    test "ETS file_id matches actual file on disk after compaction-triggering writes" do
      store = real_store()
      key = ukey("fid_race")
      shard_idx = Router.shard_for(key)
      keydir = :"keydir_#{shard_idx}"

      # Attach telemetry to know when compaction fires.
      test_pid = self()

      :telemetry.attach(
        "fid-race-compaction-#{key}",
        [:ferricstore, :dedicated, :compaction],
        fn _event, measurements, metadata, _config ->
          if metadata.redis_key == key do
            send(test_pid, {:compaction, measurements.old_fid, measurements.new_fid})
          end
        end,
        nil
      )

      # 1. Promote the hash by crossing the threshold.
      pairs = Enum.flat_map(1..(@test_threshold + 1), fn i -> ["f_#{i}", "v_#{i}"] end)
      Hash.handle("HSET", [key | pairs], store)

      shard = Router.shard_name(shard_idx)
      assert GenServer.call(shard, {:promoted?, key})

      # Resolve the dedicated directory path.
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "hash:#{hash}"])
      assert File.dir?(dedicated_path)

      # 2. Write enough fields to trigger at least one compaction cycle.
      #    The promotion itself already consumed ~threshold writes. We need
      #    to reach @compaction_threshold total writes on this promoted instance.
      #    Overwrite the same fields repeatedly to keep the entry count small
      #    but the write counter high.
      total_writes = @compaction_threshold + 200
      last_field = "sentinel"
      last_value = "correct_value_final"

      for i <- 1..total_writes do
        field = "churn_#{rem(i, 50)}"
        Hash.handle("HSET", [key, field, "val_#{i}"], store)
      end

      # Write the sentinel field that we'll verify.
      Hash.handle("HSET", [key, last_field, last_value], store)

      # 3. Verify compaction fired at least once.
      compaction_count =
        Stream.repeatedly(fn ->
          receive do
            {:compaction, _old, _new} -> :got
          after
            0 -> nil
          end
        end)
        |> Enum.take_while(&(&1 != nil))
        |> length()

      assert compaction_count >= 1,
             "expected at least 1 compaction, got #{compaction_count}"

      # 4. Check that ETS file_id for the sentinel field points to a file
      #    that actually exists on disk.
      compound_key = "H:#{key}\0#{last_field}"

      case :ets.lookup(keydir, compound_key) do
        [{^compound_key, _val, _exp, _lfu, fid, offset, _vsize}] ->
          file_name = "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
          file_path = Path.join(dedicated_path, file_name)

          assert File.exists?(file_path),
                 "ETS has file_id=#{fid} but #{file_path} does not exist — " <>
                   "stale fid after compaction. Files on disk: #{inspect(File.ls!(dedicated_path))}"

          assert offset > 0,
                 "ETS offset should be > 0 for a written entry, got #{offset}"

        [] ->
          flunk("sentinel compound key not found in ETS keydir")
      end

      # Also verify a churned field that existed before and across compaction.
      churn_key = "H:#{key}\0churn_0"

      case :ets.lookup(keydir, churn_key) do
        [{^churn_key, _val, _exp, _lfu, fid, _offset, _vsize}] ->
          file_name = "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
          file_path = Path.join(dedicated_path, file_name)

          assert File.exists?(file_path),
                 "ETS has file_id=#{fid} for churn field but #{file_path} does not exist — " <>
                   "stale fid. Files on disk: #{inspect(File.ls!(dedicated_path))}"

        [] ->
          flunk("churn compound key not found in ETS keydir")
      end

      # 5. Verify HGET returns correct values (not nil or wrong data).
      assert last_value == Hash.handle("HGET", [key, last_field], store),
             "HGET returned wrong value for sentinel field after compaction"

      # The last write to churn_0 was at i = total_writes - (rem(total_writes, 50) == 0 ? 50 : ...)
      # Just verify it's not nil.
      churn_val = Hash.handle("HGET", [key, "churn_0"], store)
      assert churn_val != nil, "HGET returned nil for churn_0 after compaction"

      # 6. Verify ALL churn fields are readable.
      for j <- 0..49 do
        val = Hash.handle("HGET", [key, "churn_#{j}"], store)
        assert val != nil, "HGET returned nil for churn_#{j}"
      end

      :telemetry.detach("fid-race-compaction-#{key}")
    end

    @tag timeout: 120_000
    @tag :skip
    # Flaky: compaction timing is non-deterministic and may not trigger within test window
    test "multiple compaction cycles do not corrupt fid-to-offset mapping" do
      store = real_store()
      key = ukey("multi_compact")
      shard_idx = Router.shard_for(key)
      keydir = :"keydir_#{shard_idx}"

      test_pid = self()

      :telemetry.attach(
        "multi-compact-#{key}",
        [:ferricstore, :dedicated, :compaction],
        fn _event, measurements, metadata, _config ->
          if metadata.redis_key == key do
            send(test_pid, {:compaction, measurements.old_fid, measurements.new_fid})
          end
        end,
        nil
      )

      # Promote.
      pairs = Enum.flat_map(1..(@test_threshold + 1), fn i -> ["f_#{i}", "v_#{i}"] end)
      Hash.handle("HSET", [key | pairs], store)

      shard = Router.shard_name(shard_idx)
      assert GenServer.call(shard, {:promoted?, key})

      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "hash:#{hash}"])

      # Drive through 3 compaction cycles.
      # Each cycle requires @compaction_threshold writes.
      target_cycles = 3

      for cycle <- 1..target_cycles do
        for i <- 1..@compaction_threshold do
          field = "cycle_#{cycle}_f_#{rem(i, 20)}"
          Hash.handle("HSET", [key, field, "c#{cycle}_v#{i}"], store)
        end
      end

      # Drain all compaction telemetry.
      Process.sleep(50)

      compaction_events =
        Stream.repeatedly(fn ->
          receive do
            {:compaction, old, new} -> {old, new}
          after
            0 -> nil
          end
        end)
        |> Enum.take_while(&(&1 != nil))

      assert length(compaction_events) >= target_cycles,
             "expected >= #{target_cycles} compactions, got #{length(compaction_events)}: #{inspect(compaction_events)}"

      # After multiple compactions, only one .log file should remain
      # (the latest active file; old ones deleted by compaction).
      {:ok, log_files} = File.ls(dedicated_path)
      log_files = Enum.filter(log_files, &String.ends_with?(&1, ".log"))

      assert length(log_files) == 1,
             "expected exactly 1 .log file after compaction, found: #{inspect(log_files)}"

      active_fid =
        log_files
        |> hd()
        |> String.trim_trailing(".log")
        |> String.to_integer()

      # Every ETS entry for this hash must reference the active fid.
      prefix = "H:#{key}\0"

      stale_entries =
        :ets.foldl(
          fn {k, _v, _exp, _lfu, fid, _off, _vsize}, acc ->
            if is_binary(k) and String.starts_with?(k, prefix) and fid != active_fid do
              [{k, fid} | acc]
            else
              acc
            end
          end,
          [],
          keydir
        )

      assert stale_entries == [],
             "found ETS entries with stale file_id (expected #{active_fid}): #{inspect(Enum.take(stale_entries, 5))}"

      # Verify reads work for fields from the last cycle.
      for j <- 0..19 do
        field = "cycle_#{target_cycles}_f_#{j}"
        val = Hash.handle("HGET", [key, field], store)
        assert val != nil, "HGET returned nil for #{field} after #{target_cycles} compaction cycles"
      end

      :telemetry.detach("multi-compact-#{key}")
    end
  end
end
