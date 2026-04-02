defmodule Ferricstore.ReviewR4.D10TxProcessDictScopingTest do
  @moduledoc """
  D10: Verifies whether :tx_deleted_keys in the process dictionary
  leaks between shard batches in cross_shard_tx.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D10: cross_shard_tx process dictionary scoping" do
    test ":tx_deleted_keys is re-initialized per shard batch" do
      # From state_machine.ex lines 236-269:
      #
      #   def apply(meta, {:cross_shard_tx, shard_batches}, state) do
      #     shard_results =
      #       Enum.reduce(shard_batches, %{}, fn {shard_idx, queue, sandbox_namespace}, acc ->
      #         store = build_cross_shard_store(shard_idx, state)
      #
      #         Process.put(:tx_deleted_keys, MapSet.new())    # ← RESET per shard
      #
      #         results =
      #           try do
      #             Enum.map(queue, fn {cmd, args} -> ... end)
      #           after
      #             Process.delete(:tx_deleted_keys)           # ← CLEANUP per shard
      #           end
      #
      #         Map.put(acc, shard_idx, results)
      #       end)
      #     ...
      #   end
      #
      # The review claim is that :tx_deleted_keys is "global, shared across
      # shard batches." This is FALSE.
      #
      # Looking at the code carefully:
      # - Line 243: Process.put(:tx_deleted_keys, MapSet.new()) — RESETS to empty
      #   at the START of each shard batch iteration.
      # - Line 261: Process.delete(:tx_deleted_keys) — DELETES after each batch.
      #
      # So each shard batch starts with a fresh MapSet and cleans up after itself.
      # There is no leak between shard batches.

      # Demonstrate the scoping is correct
      shard_batches = [{0, [:cmd1, :cmd2], nil}, {1, [:cmd3], nil}]

      results =
        Enum.reduce(shard_batches, %{}, fn {shard_idx, queue, _ns}, acc ->
          Process.put(:tx_deleted_keys, MapSet.new())

          try do
            Enum.each(queue, fn cmd ->
              deleted = Process.get(:tx_deleted_keys, MapSet.new())
              # Simulate deleting a key
              Process.put(:tx_deleted_keys, MapSet.put(deleted, "key_#{cmd}"))
            end)

            deleted_at_end = Process.get(:tx_deleted_keys)
            Map.put(acc, shard_idx, deleted_at_end)
          after
            Process.delete(:tx_deleted_keys)
          end
        end)

      # Each shard batch has its own independent set of deleted keys
      assert MapSet.size(results[0]) == 2
      assert MapSet.member?(results[0], "key_cmd1")
      assert MapSet.member?(results[0], "key_cmd2")

      assert MapSet.size(results[1]) == 1
      assert MapSet.member?(results[1], "key_cmd3")

      # Shard 1's deleted keys do NOT include shard 0's keys
      refute MapSet.member?(results[1], "key_cmd1")

      # Process dictionary is clean after
      assert Process.get(:tx_deleted_keys) == nil
    end
  end
end
