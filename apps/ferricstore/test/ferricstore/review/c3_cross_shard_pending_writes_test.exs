defmodule Ferricstore.Review.C3CrossShardPendingWritesTest do
  @moduledoc """
  Proves (or disproves) the cross-shard transaction pending writes isolation bug.

  The suspected bug: In `raft/state_machine.ex` lines 212-246, `cross_shard_tx`
  initializes a single process-level `:sm_pending_writes` buffer (line 214) and
  flushes it at the end (line 244) using `state.active_file_path` — the anchor
  shard's file path. If any command dispatched during the cross-shard transaction
  appends to `:sm_pending_writes` instead of going through the per-shard
  `BitcaskWriter`, those writes would be flushed to the anchor shard's Bitcask
  file, not the correct shard's file.

  The test writes keys to two different shards via a cross-shard transaction,
  flushes all pending I/O, then scans each shard's Bitcask log files to verify
  every key physically resides in the correct shard's files.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)

    [k0, k1] = ShardHelpers.keys_on_different_shards(2)
    shard0 = Router.shard_for(FerricStore.Instance.get(:default), k0)
    shard1 = Router.shard_for(FerricStore.Instance.get(:default), k1)

    %{k0: k0, k1: k1, shard0: shard0, shard1: shard1}
  end

  # Scans all .log files in a shard's data directory and returns the set of
  # non-tombstone keys found on disk.
  defp keys_on_disk(shard_index) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

    case File.ls(shard_path) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reject(&String.starts_with?(&1, "compact_"))
        |> Enum.flat_map(fn log_name ->
          log_path = Path.join(shard_path, log_name)

          case NIF.v2_scan_file(log_path) do
            {:ok, records} ->
              # Records: [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]
              # Build a map of key -> last_seen_tombstone? to handle overwrites
              records
              |> Enum.reduce(%{}, fn {key, _off, _vs, _exp, is_tombstone}, acc ->
                Map.put(acc, key, is_tombstone)
              end)
              |> Enum.reject(fn {_key, is_tombstone} -> is_tombstone end)
              |> Enum.map(fn {key, _} -> key end)

            _ ->
              []
          end
        end)
        |> MapSet.new()

      _ ->
        MapSet.new()
    end
  end

  describe "cross-shard tx pending writes isolation" do
    test "writes land in the correct shard's Bitcask files, not the anchor shard's",
         %{k0: k0, k1: k1, shard0: shard0, shard1: shard1} do
      # Sanity: the two keys are on different shards
      assert shard0 != shard1

      # Execute a cross-shard transaction that writes to both shards
      queue = [{"SET", [k0, "val_shard0"]}, {"SET", [k1, "val_shard1"]}]
      result = Coordinator.execute(queue, %{}, nil)
      assert result == [:ok, :ok]

      # Values must be readable through the normal path
      assert Router.get(FerricStore.Instance.get(:default), k0) == "val_shard0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "val_shard1"

      # Flush all pending async writes to disk so we can scan Bitcask files
      ShardHelpers.flush_all_shards()

      # Scan each shard's Bitcask log files
      disk_keys_shard0 = keys_on_disk(shard0)
      disk_keys_shard1 = keys_on_disk(shard1)

      # k0 must be in shard0's files, NOT shard1's
      assert MapSet.member?(disk_keys_shard0, k0),
             "key #{inspect(k0)} (shard #{shard0}) missing from shard #{shard0}'s Bitcask files"

      refute MapSet.member?(disk_keys_shard1, k0),
             "key #{inspect(k0)} (shard #{shard0}) was written to shard #{shard1}'s Bitcask files — " <>
               "pending writes isolation bug!"

      # k1 must be in shard1's files, NOT shard0's
      assert MapSet.member?(disk_keys_shard1, k1),
             "key #{inspect(k1)} (shard #{shard1}) missing from shard #{shard1}'s Bitcask files"

      refute MapSet.member?(disk_keys_shard0, k1),
             "key #{inspect(k1)} (shard #{shard1}) was written to shard #{shard0}'s Bitcask files — " <>
               "pending writes isolation bug!"
    end

    test "INCR across shards lands in correct Bitcask files",
         %{k0: k0, k1: k1, shard0: shard0, shard1: shard1} do
      # Pre-set values so INCR has something to increment
      Router.put(FerricStore.Instance.get(:default), k0, "10", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "20", 0)
      ShardHelpers.flush_all_shards()

      # Cross-shard INCR
      queue = [{"INCR", [k0]}, {"INCR", [k1]}]
      result = Coordinator.execute(queue, %{}, nil)
      assert result == [{:ok, 11}, {:ok, 21}]

      ShardHelpers.flush_all_shards()

      disk_keys_shard0 = keys_on_disk(shard0)
      disk_keys_shard1 = keys_on_disk(shard1)

      # k0's updated value must be in shard0, not shard1
      assert MapSet.member?(disk_keys_shard0, k0),
             "key #{inspect(k0)} missing from shard #{shard0} after cross-shard INCR"

      refute MapSet.member?(disk_keys_shard1, k0),
             "key #{inspect(k0)} leaked to shard #{shard1} after cross-shard INCR"

      # k1's updated value must be in shard1, not shard0
      assert MapSet.member?(disk_keys_shard1, k1),
             "key #{inspect(k1)} missing from shard #{shard1} after cross-shard INCR"

      refute MapSet.member?(disk_keys_shard0, k1),
             "key #{inspect(k1)} leaked to shard #{shard0} after cross-shard INCR"
    end

    test "sm_pending_writes buffer is empty during cross-shard dispatch (no stale writes)",
         %{k0: k0, k1: k1} do
      # This test verifies the mechanism: after a cross-shard tx, the
      # :sm_pending_writes buffer should have been empty (nothing to flush
      # to the wrong file). We verify indirectly: if the buffer were non-empty
      # and flushed to the anchor shard, the anchor would contain keys from
      # the remote shard. The disk scan in the first test covers this.
      #
      # Here we do a larger batch to increase the chance of triggering any
      # accumulation in :sm_pending_writes.
      queue =
        Enum.flat_map(0..9, fn i ->
          [
            {"SET", ["{#{k0}}:batch_#{i}", "v#{i}"]},
            {"SET", ["{#{k1}}:batch_#{i}", "v#{i}"]}
          ]
        end)

      result = Coordinator.execute(queue, %{}, nil)
      assert length(result) == 20
      assert Enum.all?(result, &(&1 == :ok))

      ShardHelpers.flush_all_shards()

      shard0 = Router.shard_for(FerricStore.Instance.get(:default), k0)
      shard1 = Router.shard_for(FerricStore.Instance.get(:default), k1)
      disk_keys_shard0 = keys_on_disk(shard0)
      disk_keys_shard1 = keys_on_disk(shard1)

      # All k0-rooted keys must be in shard0 only
      for i <- 0..9 do
        key = "{#{k0}}:batch_#{i}"
        assert MapSet.member?(disk_keys_shard0, key),
               "#{key} missing from shard #{shard0}"

        refute MapSet.member?(disk_keys_shard1, key),
               "#{key} leaked to shard #{shard1}"
      end

      # All k1-rooted keys must be in shard1 only
      for i <- 0..9 do
        key = "{#{k1}}:batch_#{i}"
        assert MapSet.member?(disk_keys_shard1, key),
               "#{key} missing from shard #{shard1}"

        refute MapSet.member?(disk_keys_shard0, key),
               "#{key} leaked to shard #{shard0}"
      end
    end
  end
end
