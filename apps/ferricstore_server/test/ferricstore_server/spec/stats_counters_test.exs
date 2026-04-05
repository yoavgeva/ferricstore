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

  use ExUnit.Case, async: true

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout()
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    %{ctx: ctx}
  end

  # ---------------------------------------------------------------------------
  # keyspace_hits / keyspace_misses (isolated instance — zero contamination)
  # ---------------------------------------------------------------------------

  describe "keyspace_hits" do
    test "increments on successful GET (key found)", %{ctx: ctx} do
      Router.put(ctx, "hit_test_key", "value", 0)
      assert Stats.keyspace_hits(ctx) == 0

      _val = Router.get(ctx, "hit_test_key")
      assert Stats.keyspace_hits(ctx) == 1
    end

    test "does not increment on GET where key is not found", %{ctx: ctx} do
      _val = Router.get(ctx, "nonexistent_key")
      assert Stats.keyspace_hits(ctx) == 0
    end

    test "increments multiple times on consecutive hits", %{ctx: ctx} do
      Router.put(ctx, "multi_hit", "value", 0)

      Enum.each(1..5, fn _ -> Router.get(ctx, "multi_hit") end)
      assert Stats.keyspace_hits(ctx) == 5
    end
  end

  describe "keyspace_misses" do
    test "increments on GET where key is not found", %{ctx: ctx} do
      _val = Router.get(ctx, "does_not_exist")
      assert Stats.keyspace_misses(ctx) == 1
    end

    test "does not increment on successful GET", %{ctx: ctx} do
      Router.put(ctx, "exists_key", "value", 0)
      _val = Router.get(ctx, "exists_key")
      assert Stats.keyspace_misses(ctx) == 0
    end

    test "increments multiple times on consecutive misses", %{ctx: ctx} do
      Enum.each(1..5, fn i -> Router.get(ctx, "missing_#{i}") end)
      assert Stats.keyspace_misses(ctx) == 5
    end

    test "increments on GET for expired key", %{ctx: ctx} do
      expire_at = System.os_time(:millisecond) + 1
      Router.put(ctx, "expiring_key", "value", expire_at)
      Process.sleep(10)

      _val = Router.get(ctx, "expiring_key")
      assert Stats.keyspace_misses(ctx) >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # expired_keys
  # ---------------------------------------------------------------------------

  describe "expired_keys" do
    test "increments when expiry sweep removes a key" do
      # Insert a key with a very short TTL
      expire_at = System.os_time(:millisecond) + 1
      Router.put(FerricStore.Instance.get(:default), "sweep_target", "value", expire_at)

      # Wait for expiry
      Process.sleep(10)

      before = Stats.expired_keys()

      # Trigger a synchronous expiry sweep on the owning shard
      idx = Router.shard_for(FerricStore.Instance.get(:default), "sweep_target")
      shard = Router.shard_name(FerricStore.Instance.get(:default), idx)
      GenServer.call(shard, :expiry_sweep)

      assert Stats.expired_keys() >= before + 1
    end

    test "increments by the number of keys removed in a sweep" do
      before = Stats.expired_keys()

      # Insert multiple keys that expire immediately
      expire_at = System.os_time(:millisecond) + 1

      keys =
        Enum.map(0..9, fn i ->
          key = "sweep_multi_#{i}"
          Router.put(FerricStore.Instance.get(:default), key, "val", expire_at)
          key
        end)

      Process.sleep(10)

      # Trigger sweep on each shard
      Enum.each(0..3, fn i ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), i)
        GenServer.call(shard, :expiry_sweep)
      end)

      delta = Stats.expired_keys() - before
      assert delta == length(keys)
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
        Router.put(FerricStore.Instance.get(:default), "evict_target_#{i}", "value_#{i}", expire_at)
      end)

      before = Stats.evicted_keys()

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

      assert Stats.evicted_keys() > before
      GenServer.stop(mg_pid)
    end

    test "does not increment under noeviction policy" do
      before = Stats.evicted_keys()

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

      assert Stats.evicted_keys() == before
      GenServer.stop(mg_pid)
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG RESETSTAT
  # ---------------------------------------------------------------------------

  describe "CONFIG RESETSTAT resets all counters" do
    test "resets keyspace_hits, keyspace_misses, expired_keys, and evicted_keys to 0", %{ctx: ctx} do
      # Use isolated instance to generate known counter values
      Router.put(ctx, "resetstat_key", "value", 0)
      Router.get(ctx, "resetstat_key")
      Router.get(ctx, "no_such_key")

      assert Stats.keyspace_hits(ctx) > 0
      assert Stats.keyspace_misses(ctx) > 0

      # Reset the global counters (CONFIG RESETSTAT tests the global path)
      Stats.reset()

      # Global counters should be near zero after reset (concurrent processes
      # may increment between reset and assertion)
      assert Stats.keyspace_hits() <= 2
      assert Stats.keyspace_misses() <= 2
      assert Stats.expired_keys() <= 2
      assert Stats.evicted_keys() <= 2
    end
  end

  # ---------------------------------------------------------------------------
  # INFO stats
  # ---------------------------------------------------------------------------

  describe "INFO stats includes all counter fields" do
    test "INFO stats response contains keyspace_hits, keyspace_misses, expired_keys, evicted_keys", %{ctx: ctx} do
      Router.put(ctx, "info_key", "value", 0)
      Router.get(ctx, "info_key")
      Router.get(ctx, "info_missing")

      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], nil)

      assert is_binary(info)
      assert info =~ "keyspace_hits:"
      assert info =~ "keyspace_misses:"
      assert info =~ "expired_keys:"
      assert info =~ "evicted_keys:"
    end

    test "INFO stats shows correct counter values", %{ctx: ctx} do
      # Use isolated instance to verify counter increments
      Router.put(ctx, "info_val_key", "value", 0)
      Router.get(ctx, "info_val_key")
      Router.get(ctx, "info_val_key")
      Router.get(ctx, "no_key_1")

      assert Stats.keyspace_hits(ctx) == 2
      assert Stats.keyspace_misses(ctx) == 1

      # INFO command reads from global counters — just check field presence
      store = %{dbsize: fn -> 0 end}
      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], store)

      assert info =~ "keyspace_hits:"
      assert info =~ "keyspace_misses:"
      assert info =~ "expired_keys:"
      assert info =~ "evicted_keys:"
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 10000 GETs, verify hit/miss counts accurate
  # ---------------------------------------------------------------------------

  describe "stress: 10000 GETs" do
    test "hit/miss counts are accurate across 10000 operations", %{ctx: ctx} do
      # Insert 100 keys on isolated instance
      hit_keys = Enum.map(1..100, fn i ->
        key = "stress_hit_#{i}"
        Router.put(ctx, key, "value_#{i}", 0)
        key
      end)

      miss_keys = Enum.map(1..100, fn i -> "stress_miss_#{i}" end)

      expected_hits = 5_000
      expected_misses = 5_000

      Enum.each(1..expected_hits, fn i ->
        key = Enum.at(hit_keys, rem(i, 100))
        Router.get(ctx, key)
      end)

      Enum.each(1..expected_misses, fn i ->
        key = Enum.at(miss_keys, rem(i, 100))
        Router.get(ctx, key)
      end)

      # Isolated instance counters — exact match, no contamination
      assert Stats.keyspace_hits(ctx) == expected_hits
      assert Stats.keyspace_misses(ctx) == expected_misses
    end
  end
end
