defmodule Ferricstore.Spec.EvictionPolicyTest do
  @moduledoc """
  Tests for approximate LRU eviction policies per spec section 2.4.

  Covers all four eviction policies:
    * `volatile_lru` -- evict least recently used keys with a TTL set
    * `allkeys_lru` -- evict least recently used key regardless of TTL
    * `volatile_ttl` -- evict the key with the shortest remaining TTL
    * `noeviction` -- reject writes when memory is full (no keys evicted)

  FerricStore eviction removes entries from the hot_cache ETS table only.
  The keydir and Bitcask on-disk data are preserved so evicted keys remain
  accessible -- the next read will be a cold read (Bitcask pread) that
  automatically re-warms the value into hot_cache. Eviction degrades
  performance (hot -> cold reads), not data availability.

  The LRU implementation uses Redis-style approximate sampling: N random
  keys are sampled from the hot_cache and the least recently used among
  the sample is evicted. The default sample size is 10.
  """

  use ExUnit.Case, async: false

  @moduletag :legacy_hot_cache

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  import Ferricstore.Test.ShardHelpers

  setup do
    flush_all_keys()

    # Reset evicted_keys counter
    Stats.reset()

    on_exit(fn ->
      # Restore default eviction policy and generous budget after each test
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1_073_741_824,
        keydir_max_ram: 256 * 1024 * 1024
      })

      flush_all_keys()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # hot_cache stores last_access_ms alongside value
  # ---------------------------------------------------------------------------

  describe "hot_cache stores last_access_ms" do
    test "writing a key stores value in hot_cache" do
      Router.put("access_test_key", "hello", 0)
      Process.sleep(20)

      hot_cache = :"hot_cache_#{Router.shard_for("access_test_key")}"
      expected_key = "access_test_key"

      # hot_cache entries are always 3-tuples {key, value, access_ms}.
      assert [{^expected_key, "hello", access_ms}] = :ets.lookup(hot_cache, expected_key)
      assert is_integer(access_ms)
      assert access_ms > 0
    end

    test "reading a key upgrades hot_cache entry to 3-tuple with access time" do
      Router.put("access_read_key", "world", 0)
      Process.sleep(20)

      hot_cache = :"hot_cache_#{Router.shard_for("access_read_key")}"

      # Read the key via Router.get -- should upgrade to 3-tuple with access_ms
      assert Router.get("access_read_key") == "world"

      # After a GET, the entry is a 3-tuple with updated access_ms.
      assert [{_, "world", access_after}] = :ets.lookup(hot_cache, "access_read_key")
      assert is_integer(access_after)
      assert access_after > 0
    end
  end

  # ---------------------------------------------------------------------------
  # volatile_lru: evict LRU keys with TTL from hot_cache
  # ---------------------------------------------------------------------------

  describe "volatile_lru eviction" do
    test "evicts hot_cache entries for keys with TTL, not keys without TTL" do
      # Insert keys without TTL (permanent)
      for i <- 1..5 do
        Router.put("permanent_#{i}", "value_#{i}", 0)
      end

      # Insert keys with TTL (volatile)
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..10 do
        Router.put("volatile_#{i}", "value_#{i}", future)
      end

      Process.sleep(50)

      # Count hot_cache entries before eviction
      permanent_hot_before = count_hot_cache_entries("permanent_")
      volatile_hot_before = count_hot_cache_entries("volatile_")

      assert permanent_hot_before == 5
      assert volatile_hot_before == 10

      # Configure volatile_lru with a tiny budget to trigger eviction
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      # Permanent keys should all still be in hot_cache
      permanent_hot_after = count_hot_cache_entries("permanent_")
      assert permanent_hot_after == 5,
             "permanent keys should not be evicted under volatile_lru"

      # Some volatile keys should have been evicted from hot_cache
      volatile_hot_after = count_hot_cache_entries("volatile_")
      assert volatile_hot_after < 10,
             "Expected some volatile keys evicted from hot_cache, but all #{volatile_hot_after} remain"

      # Evicted keys are still readable (cold path through Bitcask)
      for i <- 1..10 do
        assert Router.get("volatile_#{i}") == "value_#{i}",
               "volatile_#{i} should still be readable via cold path after eviction"
      end

      assert Stats.evicted_keys() > 0
    end

    test "does not evict anything when no keys have TTL" do
      for i <- 1..20 do
        Router.put("no_ttl_key_#{i}", String.duplicate("x", 100), 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("no_ttl_key_")

      # No volatile keys at all -- volatile_lru has nothing to evict
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      hot_after = count_hot_cache_entries("no_ttl_key_")
      assert hot_after == hot_before,
             "No keys should be evicted when none have TTL under volatile_lru"
    end
  end

  # ---------------------------------------------------------------------------
  # allkeys_lru: evict LRU key regardless of TTL from hot_cache
  # ---------------------------------------------------------------------------

  describe "allkeys_lru eviction" do
    test "evicts hot_cache entries regardless of TTL" do
      # Write 20 keys, some with TTL, some without
      for i <- 1..10 do
        Router.put("allkeys_notl_#{i}", "val_#{i}", 0)
      end

      future = System.os_time(:millisecond) + 60_000

      for i <- 1..10 do
        Router.put("allkeys_ttl_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      hot_before = count_all_hot_cache_entries()

      MemoryGuard.reconfigure(%{
        eviction_policy: :allkeys_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      hot_after = count_all_hot_cache_entries()
      assert hot_after < hot_before,
             "Expected some hot_cache evictions under allkeys_lru, " <>
               "before=#{hot_before}, after=#{hot_after}"

      assert Stats.evicted_keys() > 0
    end

    test "recently accessed key is more likely to survive eviction" do
      # Write some keys
      for i <- 1..20 do
        Router.put("lru_test_#{i}", String.duplicate("v", 50), 0)
      end

      Process.sleep(50)

      # Access only a subset of keys (make them "hot")
      hot_keys = for i <- 1..5, do: "lru_test_#{i}"

      for key <- hot_keys do
        Router.get(key)
      end

      Process.sleep(10)

      # Trigger eviction with allkeys_lru
      MemoryGuard.reconfigure(%{
        eviction_policy: :allkeys_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      # Hot keys are more likely to survive in hot_cache (probabilistic)
      hot_surviving = Enum.count(hot_keys, fn key ->
        idx = Router.shard_for(key)
        :ets.lookup(:"hot_cache_#{idx}", key) != []
      end)

      # With LRU sampling, recently-accessed keys should mostly survive
      assert hot_surviving >= 1,
             "Expected at least 1 recently-accessed key to survive LRU eviction in hot_cache"
    end
  end

  # ---------------------------------------------------------------------------
  # volatile_ttl: evict the key with shortest remaining TTL from hot_cache
  # ---------------------------------------------------------------------------

  describe "volatile_ttl eviction" do
    test "evicts key with shortest remaining TTL first" do
      now = System.os_time(:millisecond)

      # Keys with varying TTLs -- short TTL: 5 seconds
      for i <- 1..5 do
        Router.put("short_ttl_#{i}", "val_#{i}", now + 5_000)
      end

      # Long TTL: 60 seconds
      for i <- 1..5 do
        Router.put("long_ttl_#{i}", "val_#{i}", now + 60_000)
      end

      # No TTL
      for i <- 1..5 do
        Router.put("no_ttl_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_ttl,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      # Short TTL keys should be evicted from hot_cache first
      short_remaining = count_hot_cache_entries("short_ttl_")
      long_remaining = count_hot_cache_entries("long_ttl_")
      no_ttl_remaining = count_hot_cache_entries("no_ttl_")

      # No-TTL keys are not eligible for volatile_ttl eviction
      assert no_ttl_remaining == 5,
             "Keys without TTL should not be evicted under volatile_ttl"

      # Short TTL keys should have been evicted more than long TTL keys
      assert short_remaining <= long_remaining,
             "Expected short TTL keys (#{short_remaining} remaining) to be evicted " <>
               "before long TTL keys (#{long_remaining} remaining)"

      assert Stats.evicted_keys() > 0
    end

    test "does not evict keys without TTL" do
      for i <- 1..20 do
        Router.put("persist_#{i}", String.duplicate("x", 100), 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("persist_")

      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_ttl,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      hot_after = count_hot_cache_entries("persist_")
      assert hot_after == hot_before,
             "No keys should be evicted when none have TTL under volatile_ttl"
    end
  end

  # ---------------------------------------------------------------------------
  # noeviction: reject writes when full
  # ---------------------------------------------------------------------------

  describe "noeviction policy" do
    test "does not evict any hot_cache entries" do
      for i <- 1..10 do
        Router.put("noevict_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("noevict_")

      MemoryGuard.reconfigure(%{
        eviction_policy: :noeviction,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      hot_after = count_hot_cache_entries("noevict_")
      assert hot_after == hot_before,
             "No keys should be evicted under noeviction policy"

      assert Stats.evicted_keys() == 0
    end

    test "reject_writes? returns true under noeviction at reject threshold" do
      MemoryGuard.reconfigure(%{
        eviction_policy: :noeviction,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      assert MemoryGuard.reject_writes?() == true
    end
  end

  # ---------------------------------------------------------------------------
  # LRU sampling mechanics
  # ---------------------------------------------------------------------------

  describe "LRU sampling" do
    test "eviction removes entries from hot_cache but keys remain in keydir" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("evict_check_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      evicted_count = Stats.evicted_keys()
      assert evicted_count > 0

      # Evicted keys should be gone from hot_cache but present in keydir
      # (keydir is not touched by hot_cache eviction)
      for i <- 1..20 do
        key = "evict_check_#{i}"
        idx = Router.shard_for(key)
        keydir = :"keydir_#{idx}"

        # Key should still be in keydir (eviction only touches hot_cache)
        assert :ets.lookup(keydir, key) != [],
               "Key #{key} should still be in keydir after hot_cache eviction"
      end
    end

    test "eviction increments evicted_keys stat" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..30 do
        Router.put("stat_evict_#{i}", String.duplicate("x", 100), future)
      end

      Process.sleep(50)

      Stats.reset()
      assert Stats.evicted_keys() == 0

      MemoryGuard.reconfigure(%{
        eviction_policy: :allkeys_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      assert Stats.evicted_keys() > 0
    end
  end

  # ---------------------------------------------------------------------------
  # Evicted keys remain readable -- spec para 142
  # ---------------------------------------------------------------------------

  describe "evicted keys remain readable (spec para 142)" do
    test "evicted key is still readable via GET (falls through to Bitcask)" do
      future = System.os_time(:millisecond) + 60_000

      # Write keys with known values
      for i <- 1..20 do
        Router.put("readable_#{i}", "data_#{i}", future)
      end

      Process.sleep(50)

      # Trigger eviction
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      assert Stats.evicted_keys() > 0

      # Every single key must still be readable with correct value
      for i <- 1..20 do
        value = Router.get("readable_#{i}")

        assert value == "data_#{i}",
               "readable_#{i} must return correct value after eviction, got: #{inspect(value)}"
      end
    end

    test "evicted key's keydir entry still exists" do
      future = System.os_time(:millisecond) + 60_000

      keys =
        for i <- 1..20 do
          key = "keydir_check_#{i}"
          Router.put(key, "val_#{i}", future)
          key
        end

      Process.sleep(50)

      # Trigger eviction
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      assert Stats.evicted_keys() > 0

      # Verify every key's keydir entry is intact
      for key <- keys do
        shard_idx = Router.shard_for(key)
        keydir = :"keydir_#{shard_idx}"

        assert :ets.lookup(keydir, key) != [],
               "keydir entry for #{inspect(key)} must survive eviction"
      end
    end

    test "re-reading evicted key warms it back into hot_cache" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("warm_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      # Trigger eviction
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      assert Stats.evicted_keys() > 0

      # Find at least one key that was actually evicted from hot_cache
      evicted_key =
        Enum.find(1..20, fn i ->
          key = "warm_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) == []
        end)

      assert evicted_key != nil, "Expected at least one key evicted from hot_cache"

      key = "warm_#{evicted_key}"
      hc = :"hot_cache_#{Router.shard_for(key)}"

      # Confirm evicted from hot_cache
      assert :ets.lookup(hc, key) == []

      # Read the key -- this should be a cold read that warms back into hot_cache
      assert Router.get(key) == "val_#{evicted_key}"

      # After reading, the key should be back in hot_cache
      assert :ets.lookup(hc, key) != [],
             "Key #{inspect(key)} should be warmed back into hot_cache after cold read"
    end

    test "eviction under memory pressure does not lose any data" do
      # Write a mix of keys: some with TTL, some without
      future = System.os_time(:millisecond) + 60_000

      all_keys =
        for i <- 1..50 do
          key = "pressure_data_#{i}"
          ttl = if rem(i, 2) == 0, do: future, else: 0
          Router.put(key, "value_#{i}", ttl)
          {key, "value_#{i}"}
        end

      Process.sleep(50)

      # Trigger aggressive eviction with allkeys_lru (can evict any key)
      MemoryGuard.reconfigure(%{
        eviction_policy: :allkeys_lru,
        max_memory_bytes: 1
      })

      # Multiple eviction rounds
      for _ <- 1..10 do
        MemoryGuard.force_check()
        Process.sleep(10)
      end

      assert Stats.evicted_keys() > 0

      # CRITICAL: every single key must still be readable with correct value
      for {key, expected_value} <- all_keys do
        actual = Router.get(key)

        assert actual == expected_value,
               "DATA LOSS: #{inspect(key)} expected #{inspect(expected_value)}, got #{inspect(actual)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: many keys, fill hot_cache, verify hot keys retained
  # ---------------------------------------------------------------------------

  describe "stress: LRU eviction with many keys" do
    test "recently accessed keys are more likely to survive in hot_cache" do
      # Write 200 keys
      for i <- 1..200 do
        Router.put("stress_#{i}", String.duplicate("v", 50), 0)
      end

      Process.sleep(100)

      # Mark keys 1-20 as "hot" by reading them recently
      hot_keys = for i <- 1..20, do: "stress_#{i}"

      for key <- hot_keys do
        Router.get(key)
      end

      Process.sleep(10)

      # Trigger multiple rounds of eviction
      MemoryGuard.reconfigure(%{
        eviction_policy: :allkeys_lru,
        max_memory_bytes: 1
      })

      for _ <- 1..5 do
        MemoryGuard.force_check()
        Process.sleep(20)
      end

      # Count surviving hot_cache entries
      total_hot = count_all_hot_cache_entries()

      # Some entries should have been evicted from hot_cache
      assert total_hot < 200,
             "Expected some hot_cache evictions, but all #{total_hot} entries remain"

      # Eviction stats should reflect the work done
      assert Stats.evicted_keys() > 0

      # All keys should still be readable (cold path)
      for i <- 1..200 do
        assert Router.get("stress_#{i}") == String.duplicate("v", 50),
               "stress_#{i} should still be readable after hot_cache eviction"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Eviction at pressure level
  # ---------------------------------------------------------------------------

  describe "eviction triggers at pressure and reject levels" do
    test "eviction runs at reject pressure level for non-noeviction policies" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..30 do
        Router.put("pressure_test_#{i}", String.duplicate("x", 100), future)
      end

      Process.sleep(50)

      # Set tiny budget to force reject-level pressure
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      # Verify eviction happened
      assert Stats.evicted_keys() > 0
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Counts hot_cache entries across all shards whose key starts with the
  # given prefix. Handles both 3-element tuples `{key, value, access_ms}`
  # (direct shard path) and 2-element tuples `{key, value}` (Raft state
  # machine path).
  defp count_hot_cache_entries(prefix) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
      hot_cache = :"hot_cache_#{i}"

      try do
        :ets.foldl(
          fn entry, count ->
            key =
              case entry do
                {k, _value, _access_ms} -> k
                {k, _value} -> k
                _ -> nil
              end

            if is_binary(key) and String.starts_with?(key, prefix) do
              count + 1
            else
              count
            end
          end,
          acc,
          hot_cache
        )
      rescue
        ArgumentError -> acc
      end
    end)
  end

  # Counts all hot_cache entries across all shards.
  defp count_all_hot_cache_entries do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
      try do
        acc + :ets.info(:"hot_cache_#{i}", :size)
      rescue
        ArgumentError -> acc
      end
    end)
  end
end
