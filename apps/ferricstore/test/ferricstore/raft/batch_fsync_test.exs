defmodule Ferricstore.Raft.BatchFsyncTest do
  @moduledoc """
  Verifies that a Raft batch of N writes shares a single fsync by
  measuring timing. A batch of 20 writes should complete in roughly
  the same time as a single write (one fsync), while 20 sequential
  individual writes take ~20x longer (20 fsyncs).
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "batch of 20 writes is faster than 20 individual writes" do
    ctx = FerricStore.Instance.get(:default)
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(0)

    # Warm up — first Raft command is always slow (leader election, WAL init)
    Router.put(ctx, "warmup_key", "x", 0)

    # Measure: 20 writes as a single Raft batch (1 fsync)
    # Use Router.put for individual writes so keys route to correct shards.
    # For the batch, submit directly to one shard's Raft.
    batch_commands = for i <- 1..20, do: {:put, "bfsync_batch_#{i}", "v#{i}", 0}

    {batch_us, {:ok, results, _}} =
      :timer.tc(fn ->
        :ra.process_command(shard_id, {:batch, batch_commands}, %{reply_from: :local, timeout: 10_000})
      end)

    {:applied_at, _idx, {:ok, individual_results}} = results
    assert length(individual_results) == 20

    # Measure: 20 individual writes to the same shard (each its own Raft command + fsync)
    individual_commands = for i <- 1..20, do: {:put, "bfsync_indiv_#{i}", "v#{i}", 0}

    {individual_us, _} =
      :timer.tc(fn ->
        for cmd <- individual_commands do
          :ra.process_command(shard_id, cmd, %{reply_from: :local, timeout: 10_000})
        end
      end)

    # The batch should be significantly faster than individual writes.
    # Individual writes do 20 Raft rounds + 20 fsyncs.
    # Batch does 1 Raft round + 1 fsync.
    # We expect at least 2x speedup (conservative — real speedup is 10-20x).
    ratio = individual_us / max(batch_us, 1)

    assert ratio > 2.0,
      "Expected batch (#{batch_us}μs) to be much faster than individual (#{individual_us}μs), " <>
      "ratio=#{Float.round(ratio, 1)}x — fsync may not be batched"
  end
end
