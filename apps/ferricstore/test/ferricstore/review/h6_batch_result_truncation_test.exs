defmodule Ferricstore.Review.H6BatchResultTruncationTest do
  @moduledoc """
  Regression test for batch result count mismatch in Batcher.

  Bug: In `handle_info({:ra_event, _, {:applied, _}}, state)` (batcher.ex
  lines 339-351), the `:batch` branch uses `Enum.zip(froms, results)` to
  pair callers with their results. `Enum.zip/2` silently truncates to the
  length of the shorter list. If the StateMachine ever returns fewer results
  than commands in the batch, the trailing callers never receive a reply and
  hang until their GenServer.call timeout fires.

  Today the StateMachine returns one result per command, so the happy path
  works. This test documents the invariant: every concurrent caller in a
  batch must receive a reply within a reasonable timeout. A timeout failure
  here would indicate the zip-truncation bug is manifesting.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  defp ukey(i), do: "_root:h6_trunc_#{i}_#{:rand.uniform(9_999_999)}"

  describe "batch result count matches caller count" do
    test "all concurrent callers receive a reply (no zip truncation)" do
      # Generate enough keys to guarantee batching on at least one shard.
      # All keys share the "_root" prefix so they land in the same namespace
      # slot within a given shard.
      n = 20
      keys = for i <- 1..n, do: ukey(i)

      # Group by shard and pick the shard with the most keys.
      by_shard = Enum.group_by(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)
      {shard_idx, shard_keys} = Enum.max_by(by_shard, fn {_, ks} -> length(ks) end)

      # Fire all writes concurrently so they accumulate in the same batch.
      tasks =
        Enum.map(shard_keys, fn k ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, k, "v", 0})
          end)
        end)

      # If zip truncation occurs, some tasks will hang until this timeout.
      results = Task.await_many(tasks, 10_000)

      # Every caller must get :ok — none should timeout or receive an error.
      assert length(results) == length(shard_keys)
      assert Enum.all?(results, &(&1 == :ok))

      # Verify data actually landed.
      for k <- shard_keys do
        assert Router.get(FerricStore.Instance.get(:default), k) == "v"
      end
    end

    test "mixed command types in a batch all receive replies" do
      base = :rand.uniform(9_999_999)
      shard_keys_and_cmds =
        for i <- 1..10 do
          k = "_root:h6_mix_#{base}_#{i}"
          cmd =
            case rem(i, 3) do
              0 -> {:put, k, "val_#{i}", 0}
              1 -> {:put, k, "val_#{i}", 0}
              2 -> {:incr, k, 1}
            end

          {k, cmd}
        end

      # Group by shard, pick the largest group.
      by_shard =
        Enum.group_by(shard_keys_and_cmds, fn {k, _cmd} -> Router.shard_for(FerricStore.Instance.get(:default), k) end)

      {shard_idx, group} = Enum.max_by(by_shard, fn {_, g} -> length(g) end)

      tasks =
        Enum.map(group, fn {_k, cmd} ->
          Task.async(fn ->
            Batcher.write(shard_idx, cmd)
          end)
        end)

      # All callers must reply within timeout — a hang here means truncation.
      results = Task.await_many(tasks, 10_000)
      assert length(results) == length(group)
    end
  end
end
