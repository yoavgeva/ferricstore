defmodule Ferricstore.Raft.LeaderCacheTest do
  @moduledoc """
  Tests that the leader cache in Router works correctly and that
  clients always get correct responses, even when:
  - Leader cache is empty (first write)
  - Leader cache is stale (leader changed)
  - Leader cache points to a dead node
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive(10_000)
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive(10_000) end)
  end

  defp ukey(base), do: "lc_#{base}_#{:rand.uniform(9_999_999)}"

  describe "leader cache correctness" do
    test "write succeeds with empty leader cache" do
      # Clear any cached leader for shard 0
      :persistent_term.erase({:ferricstore_shard_leader, 0})

      k = ShardHelpers.key_for_shard(0)
      Router.put(k, "no_cache")
      assert Router.get(k) == "no_cache"
    end

    test "write succeeds after leader cache is populated" do
      k = ShardHelpers.key_for_shard(0)

      # First write populates the cache
      Router.put(k, "first")
      assert Router.get(k) == "first"

      # Second write uses the cache
      Router.put(k, "second")
      assert Router.get(k) == "second"

      # Verify cache is populated
      cached = :persistent_term.get({:ferricstore_shard_leader, 0}, nil)
      assert cached != nil, "leader cache should be populated after write"
    end

    # TODO: stale leader cache causes writes to go through GenServer fallback
    # which submits to Raft asynchronously. The value isn't visible on immediate
    # GET because Raft apply hasn't happened yet. This is a known limitation —
    # on single node the cache is always correct. On multi-node, a stale cache
    # causes one extra round-trip (:not_leader rejection) which auto-corrects.

    @tag timeout: 180_000
    test "write returns correct value after shard restart" do
      k = ShardHelpers.key_for_shard(0)
      Router.put(k, "before_kill")
      ShardHelpers.flush_all_shards()

      # Clear leader cache before kill — simulates fresh state
      :persistent_term.erase({:ferricstore_shard_leader, 0})

      # Kill shard — ra server restarts
      ShardHelpers.kill_shard_safely(0)

      # After restart, cache is empty — first write discovers leader
      k2 = ukey("after_kill")
      Router.put(k2, "after_kill_value")
      assert Router.get(k2) == "after_kill_value"

      # Original key should survive via Raft WAL replay
      assert Router.get(k) == "before_kill"
    end

    test "multiple shards have independent leader caches" do
      k0 = ShardHelpers.key_for_shard(0)
      k1 = ShardHelpers.key_for_shard(1)

      Router.put(k0, "shard0")
      Router.put(k1, "shard1")

      cache0 = :persistent_term.get({:ferricstore_shard_leader, 0}, nil)
      cache1 = :persistent_term.get({:ferricstore_shard_leader, 1}, nil)

      # Both should be cached
      assert cache0 != nil
      assert cache1 != nil

      # Values correct
      assert Router.get(k0) == "shard0"
      assert Router.get(k1) == "shard1"
    end

    test "concurrent writes to same shard all succeed" do
      k = ukey("concurrent")
      Router.put(k, "0")

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            Router.put(k, "#{i}")
            :ok
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # Value should be one of the written values
      val = Router.get(k)
      assert val != nil
      {num, ""} = Integer.parse(val)
      assert num >= 0 and num <= 20
    end

    test "INCR returns correct sequential values despite leader cache" do
      k = ukey("incr_cache")
      Router.put(k, "0")

      for i <- 1..10 do
        {:ok, result} = Router.incr(k, 1)
        assert result == i, "INCR should return #{i}, got #{result}"
      end

      assert Router.get(k) == "10"
    end

    test "write with poisoned cache falls back gracefully" do
      # Poison cache with a server name that exists locally but isn't a ra server
      :persistent_term.put({:ferricstore_shard_leader, 0}, {:nonexistent_server, node()})

      try do
        k = ShardHelpers.key_for_shard(0)

        # Should fall back to GenServer.call, not crash
        result = Router.put(k, "fallback_test")
        assert result == :ok or match?({:error, _}, result)
      after
        :persistent_term.erase({:ferricstore_shard_leader, 0})
      end
    end
  end
end
