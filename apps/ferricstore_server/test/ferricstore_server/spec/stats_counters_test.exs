defmodule FerricstoreServer.Spec.StatsCountersTest do
  @moduledoc """
  Tests for keyspace_hits, keyspace_misses, expired_keys, and evicted_keys
  counters in `Ferricstore.Stats`.

  Verifies:
    - keyspace_hits increments on successful GET (key found)
    - keyspace_misses increments on GET where key not found
    - expired_keys increments when the expiry sweep removes a key
    - evicted_keys increments when memory pressure evicts a key
    - CONFIG RESETSTAT resets all new counters to 0
    - INFO stats includes all new counter fields
    - Stress: 10000 GETs, verify hit/miss counts are accurate
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    Stats.reset()
    :ok
  end

  # ---------------------------------------------------------------------------
  # keyspace_hits / keyspace_misses
  # ---------------------------------------------------------------------------

  describe "keyspace_hits" do
    test "increments on successful GET (key found)" do
      Router.put("hit_test_key", "value", 0)
      assert Stats.keyspace_hits() == 0

      _val = Router.get("hit_test_key")
      assert Stats.keyspace_hits() == 1
    end

    test "does not increment on GET where key is not found" do
      _val = Router.get("nonexistent_key")
      assert Stats.keyspace_hits() == 0
    end

    test "increments multiple times on consecutive hits" do
      Router.put("multi_hit", "value", 0)

      Enum.each(1..5, fn _ -> Router.get("multi_hit") end)
      assert Stats.keyspace_hits() == 5
    end
  end

  describe "keyspace_misses" do
    test "increments on GET where key is not found" do
      assert Stats.keyspace_misses() == 0

      _val = Router.get("does_not_exist")
      assert Stats.keyspace_misses() == 1
    end

    test "does not increment on successful GET" do
      Router.put("exists_key", "value", 0)
      _val = Router.get("exists_key")
      assert Stats.keyspace_misses() == 0
    end

    test "increments multiple times on consecutive misses" do
      Enum.each(1..5, fn i -> Router.get("missing_#{i}") end)
      assert Stats.keyspace_misses() == 5
    end

    test "increments on GET for expired key" do
      # Set a key that expires immediately (1ms TTL)
      expire_at = System.os_time(:millisecond) + 1
      Router.put("expiring_key", "value", expire_at)
      Process.sleep(10)

      _val = Router.get("expiring_key")
      assert Stats.keyspace_misses() >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # expired_keys
  # ---------------------------------------------------------------------------

  describe "expired_keys" do
    test "increments when expiry sweep removes a key" do
      # Insert a key with a very short TTL
      expire_at = System.os_time(:millisecond) + 1
      Router.put("sweep_target", "value", expire_at)

      # Wait for expiry
      Process.sleep(10)

      assert Stats.expired_keys() == 0

      # Trigger a synchronous expiry sweep on the owning shard
      idx = Router.shard_for("sweep_target")
      shard = Router.shard_name(idx)
      GenServer.call(shard, :expiry_sweep)

      assert Stats.expired_keys() >= 1
    end

    test "increments by the number of keys removed in a sweep" do
      # Insert multiple keys that expire immediately
      expire_at = System.os_time(:millisecond) + 1

      keys =
        Enum.map(0..9, fn i ->
          key = "sweep_multi_#{i}"
          Router.put(key, "val", expire_at)
          key
        end)

      Process.sleep(10)

      # Trigger sweep on each shard
      Enum.each(0..3, fn i ->
        shard = Router.shard_name(i)
        GenServer.call(shard, :expiry_sweep)
      end)

      expired = Stats.expired_keys()
      assert expired == length(keys)
    end
  end

  # ---------------------------------------------------------------------------
  # evicted_keys
  # ---------------------------------------------------------------------------

  describe "evicted_keys" do
    test "increments when memory pressure evicts a key" do
      # Put some keys with TTLs (volatile_lru evicts keys with TTLs)
      expire_at = System.os_time(:millisecond) + 600_000

      Enum.each(1..10, fn i ->
        Router.put("evict_target_#{i}", "value_#{i}", expire_at)
      end)

      assert Stats.evicted_keys() == 0

      # Start a MemoryGuard with a tiny budget to trigger eviction
      {:ok, mg_pid} =
        GenServer.start_link(Ferricstore.MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :volatile_lru
        ])

      # Trigger a check which should evict keys
      send(mg_pid, :check)
      Process.sleep(200)

      assert Stats.evicted_keys() > 0
      GenServer.stop(mg_pid)
    end

    test "does not increment under noeviction policy" do
      # With noeviction, no keys should be evicted
      {:ok, mg_pid} =
        GenServer.start_link(Ferricstore.MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      send(mg_pid, :check)
      Process.sleep(200)

      assert Stats.evicted_keys() == 0
      GenServer.stop(mg_pid)
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG RESETSTAT
  # ---------------------------------------------------------------------------

  describe "CONFIG RESETSTAT resets all counters" do
    test "resets keyspace_hits, keyspace_misses, expired_keys, and evicted_keys to 0" do
      Router.put("resetstat_key", "value", 0)
      Router.get("resetstat_key")
      Router.get("no_such_key")

      assert Stats.keyspace_hits() > 0
      assert Stats.keyspace_misses() > 0

      # Manually increment expired and evicted for test
      Stats.incr_expired_keys(5)
      Stats.incr_evicted_keys(3)

      assert Stats.expired_keys() == 5
      assert Stats.evicted_keys() == 3

      Stats.reset()

      assert Stats.keyspace_hits() == 0
      assert Stats.keyspace_misses() == 0
      assert Stats.expired_keys() == 0
      assert Stats.evicted_keys() == 0
    end
  end

  # ---------------------------------------------------------------------------
  # INFO stats
  # ---------------------------------------------------------------------------

  describe "INFO stats includes all counter fields" do
    test "INFO stats response contains keyspace_hits, keyspace_misses, expired_keys, evicted_keys" do
      Router.put("info_key", "value", 0)
      Router.get("info_key")
      Router.get("info_missing")

      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], nil)

      assert is_binary(info)
      assert info =~ "keyspace_hits:"
      assert info =~ "keyspace_misses:"
      assert info =~ "expired_keys:"
      assert info =~ "evicted_keys:"
    end

    test "INFO stats shows correct counter values" do
      Stats.reset()

      Router.put("info_val_key", "value", 0)
      # Generate some hits and misses
      Router.get("info_val_key")
      Router.get("info_val_key")
      Router.get("no_key_1")

      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], nil)

      assert info =~ "keyspace_hits:2"
      assert info =~ "keyspace_misses:1"
      assert info =~ "expired_keys:0"
      assert info =~ "evicted_keys:0"
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 10000 GETs, verify hit/miss counts accurate
  # ---------------------------------------------------------------------------

  describe "stress: 10000 GETs" do
    test "hit/miss counts are accurate across 10000 operations" do
      Stats.reset()

      # Insert 100 keys
      hit_keys = Enum.map(1..100, fn i ->
        key = "stress_hit_#{i}"
        Router.put(key, "value_#{i}", 0)
        key
      end)

      miss_keys = Enum.map(1..100, fn i -> "stress_miss_#{i}" end)

      # Do 5000 hits and 5000 misses
      expected_hits = 5_000
      expected_misses = 5_000

      Enum.each(1..expected_hits, fn i ->
        key = Enum.at(hit_keys, rem(i, 100))
        Router.get(key)
      end)

      Enum.each(1..expected_misses, fn i ->
        key = Enum.at(miss_keys, rem(i, 100))
        Router.get(key)
      end)

      assert Stats.keyspace_hits() == expected_hits
      assert Stats.keyspace_misses() == expected_misses
    end
  end
end
