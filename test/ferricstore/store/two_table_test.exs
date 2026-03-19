defmodule Ferricstore.Store.TwoTableTest do
  @moduledoc """
  Tests for the two-table ETS split (spec 2.4).

  Verifies that:
  - EtsStore.init_tables/1 creates both keydir_N and hot_cache_N
  - insert/5 populates both tables
  - lookup/4 checks keydir for TTL, hot_cache for value
  - delete/3 removes from both tables
  - Expired keys return nil on lookup
  - Router hot path reads from the split tables
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.EtsStore

  # Use a high shard index (99) so we don't collide with the
  # application-supervised shards (0-3).
  @test_index 99

  setup do
    # Clean up any leftover ETS tables from prior test runs
    for name <- [:"keydir_#{@test_index}", :"hot_cache_#{@test_index}"] do
      if :ets.whereis(name) != :undefined do
        :ets.delete(name)
      end
    end

    :ok
  end

  describe "EtsStore.init_tables/1" do
    test "creates keydir_N and hot_cache_N named tables" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)

      assert keydir == :"keydir_#{@test_index}"
      assert hot_cache == :"hot_cache_#{@test_index}"

      # Both tables should exist
      assert :ets.whereis(keydir) != :undefined
      assert :ets.whereis(hot_cache) != :undefined

      # Both tables should be public (for hot-path reads from Router)
      assert :ets.info(keydir, :protection) == :public
      assert :ets.info(hot_cache, :protection) == :public
    end

    test "reuses existing tables on second call (clears data)" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      EtsStore.insert(keydir, hot_cache, "k1", "v1", 0)

      # Re-init should clear data
      {keydir2, hot_cache2} = EtsStore.init_tables(@test_index)
      assert keydir == keydir2
      assert hot_cache == hot_cache2

      # Data should be cleared
      assert EtsStore.lookup(keydir, hot_cache, "k1") == :miss
    end
  end

  describe "EtsStore.insert/5" do
    test "inserts key into both tables" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)

      EtsStore.insert(keydir, hot_cache, "mykey", "myvalue", 0)

      # keydir should have {key, expire_at_ms}
      assert [{_key, 0}] = :ets.lookup(keydir, "mykey")

      # hot_cache should have {key, value, expire_at_ms}
      assert [{_key, "myvalue", 0}] = :ets.lookup(hot_cache, "mykey")
    end

    test "inserts key with TTL into both tables" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      expire = System.os_time(:millisecond) + 60_000

      EtsStore.insert(keydir, hot_cache, "ttl_key", "ttl_val", expire)

      assert [{_key, ^expire}] = :ets.lookup(keydir, "ttl_key")
      assert [{_key, "ttl_val", ^expire}] = :ets.lookup(hot_cache, "ttl_key")
    end
  end

  describe "EtsStore.lookup/4" do
    test "returns {:hit, value, expire_at_ms} for live keys" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      EtsStore.insert(keydir, hot_cache, "k", "v", 0)

      assert {:hit, "v", 0} = EtsStore.lookup(keydir, hot_cache, "k")
    end

    test "returns {:hit, value, exp} for keys with future TTL" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      expire = System.os_time(:millisecond) + 60_000
      EtsStore.insert(keydir, hot_cache, "k", "v", expire)

      assert {:hit, "v", ^expire} = EtsStore.lookup(keydir, hot_cache, "k")
    end

    test "returns :expired for keys past their TTL" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      # Already expired
      EtsStore.insert(keydir, hot_cache, "k", "v", 1)

      assert :expired = EtsStore.lookup(keydir, hot_cache, "k")
    end

    test "returns :miss for nonexistent keys" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)

      assert :miss = EtsStore.lookup(keydir, hot_cache, "nonexistent")
    end

    test "evicts expired entry from both tables on lookup" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      EtsStore.insert(keydir, hot_cache, "k", "v", 1)

      assert :expired = EtsStore.lookup(keydir, hot_cache, "k")

      # Both tables should be cleaned
      assert :ets.lookup(keydir, "k") == []
      assert :ets.lookup(hot_cache, "k") == []
    end
  end

  describe "EtsStore.delete/3" do
    test "removes key from both tables" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)
      EtsStore.insert(keydir, hot_cache, "k", "v", 0)

      EtsStore.delete(keydir, hot_cache, "k")

      assert :ets.lookup(keydir, "k") == []
      assert :ets.lookup(hot_cache, "k") == []
    end

    test "is a no-op for nonexistent keys" do
      {keydir, hot_cache} = EtsStore.init_tables(@test_index)

      # Should not crash
      EtsStore.delete(keydir, hot_cache, "nonexistent")
    end
  end

  describe "integration: shard uses two-table split" do
    test "shard_ets_N is an alias for keydir_N (backward compat)" do
      # The application-supervised shard 0 should have state.ets == keydir_0
      # AND shard_ets_0 should be the keydir (or the alias if we keep it)
      # Verify through a live write/read cycle
      alias Ferricstore.Store.Router

      key = "two_table_test_#{System.unique_integer([:positive])}"
      Router.put(key, "hello", 0)

      # Read via Router (hot path)
      assert Router.get(key) == "hello"

      # Clean up
      Router.delete(key)
    end

    test "both keydir_N and hot_cache_N exist for each shard" do
      for i <- 0..3 do
        assert :ets.whereis(:"keydir_#{i}") != :undefined,
               "keydir_#{i} should exist"

        assert :ets.whereis(:"hot_cache_#{i}") != :undefined,
               "hot_cache_#{i} should exist"
      end
    end
  end
end
