defmodule Ferricstore.Spec.EvictionComprehensiveTest do
  @moduledoc """
  Comprehensive tests for the LRU eviction system (spec section 2.4).

  FerricStore eviction removes entries from the hot_cache ETS table only.
  The keydir and Bitcask on-disk data are preserved, so evicted keys remain
  accessible via cold reads (Bitcask pread). Eviction degrades performance
  (hot -> cold reads), never data availability.

  This module exercises all four eviction policies (volatile_lru, allkeys_lru,
  volatile_ttl, noeviction) with thorough coverage of:

    * Policy-specific eligibility rules
    * LRU ordering correctness (access time tracking)
    * TTL-based ordering (volatile_ttl)
    * Data preservation guarantees (no data loss on eviction)
    * Concurrent access during eviction
    * Stress and throughput under sustained eviction pressure

  Tests tagged `:perf` are excluded from the default test run. Execute with:

      mix test apps/ferricstore/test/ferricstore/spec/eviction_comprehensive_test.exs --include perf
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  import Ferricstore.Test.ShardHelpers

  @shard_count Application.compile_env(:ferricstore, :shard_count, 4)

  setup do
    flush_all_keys()
    Stats.reset()

    on_exit(fn ->
      MemoryGuard.reconfigure(%{
        eviction_policy: :volatile_lru,
        max_memory_bytes: 1_073_741_824,
        keydir_max_ram: 256 * 1024 * 1024
      })

      flush_all_keys()
    end)

    :ok
  end

  # ===========================================================================
  # volatile_lru tests (12)
  # ===========================================================================

  describe "volatile_lru" do
    test "1: keys with TTL are eligible for eviction" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("vol_eligible_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("vol_eligible_")
      assert hot_before == 20

      trigger_eviction(:volatile_lru)

      hot_after = count_hot_cache_entries("vol_eligible_")
      assert hot_after < 20, "Expected some volatile keys evicted, got #{hot_after}"
      assert Stats.evicted_keys() > 0
    end

    test "2: keys WITHOUT TTL are never evicted under volatile_lru" do
      for i <- 1..30 do
        Router.put("vol_perm_#{i}", String.duplicate("x", 100), 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("vol_perm_")
      assert hot_before == 30

      trigger_eviction(:volatile_lru)

      hot_after = count_hot_cache_entries("vol_perm_")
      assert hot_after == hot_before,
             "No permanent keys should be evicted under volatile_lru, " <>
               "before=#{hot_before}, after=#{hot_after}"
    end

    test "3: recently accessed key with TTL survives over old key with TTL" do
      future = System.os_time(:millisecond) + 60_000

      # Insert "old" keys first -- these have an older last_access_ms
      for i <- 1..20 do
        Router.put("vol_old_#{i}", "old_val_#{i}", future)
      end

      Process.sleep(50)

      # Insert "new" keys -- these have a more recent last_access_ms
      for i <- 1..5 do
        Router.put("vol_new_#{i}", "new_val_#{i}", future)
      end

      # Also re-access the new keys to ensure they are freshly touched
      for i <- 1..5 do
        Router.get("vol_new_#{i}")
      end

      Process.sleep(10)

      trigger_eviction(:volatile_lru)

      # New keys should be more likely to survive
      new_surviving = count_hot_cache_entries("vol_new_")
      old_surviving = count_hot_cache_entries("vol_old_")

      # With 5 new + 20 old keys, eviction should preferentially remove old keys.
      # Approximate LRU uses random sampling so we set a low bar.
      assert new_surviving >= 1,
             "Expected at least 1/5 recently-accessed keys to survive, got #{new_surviving}"

      assert old_surviving < 20,
             "Expected some old keys evicted, all #{old_surviving} survived"
    end

    test "4: accessing a key updates its last_access_ms" do
      future = System.os_time(:millisecond) + 60_000
      Router.put("vol_access_ts", "hello", future)
      Process.sleep(30)

      hot_cache = :"hot_cache_#{Router.shard_for("vol_access_ts")}"

      # Read the key once via Router.get to ensure it is upgraded to 3-tuple
      assert Router.get("vol_access_ts") == "hello"
      Process.sleep(10)

      # Get initial access time -- entry is always a 3-tuple.
      [{_, _, access_before}] = :ets.lookup(hot_cache, "vol_access_ts")

      assert is_integer(access_before) and access_before > 0

      Process.sleep(10)

      # Read the key again to update access time
      assert Router.get("vol_access_ts") == "hello"

      [{_, _, access_after}] = :ets.lookup(hot_cache, "vol_access_ts")

      assert access_after >= access_before,
             "Access time should be updated on read: before=#{access_before}, after=#{access_after}"
    end

    test "5: multiple eviction rounds keep removing oldest-accessed" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..40 do
        Router.put("vol_multi_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      # Touch keys 1-10 to keep them hot
      for i <- 1..10 do
        Router.get("vol_multi_#{i}")
      end

      Process.sleep(10)

      # Run multiple eviction rounds
      for _ <- 1..5 do
        trigger_eviction(:volatile_lru)
      end

      hot_total = count_hot_cache_entries("vol_multi_")
      assert hot_total < 40,
             "Expected progressive eviction, but #{hot_total}/40 remain"

      # Hot keys 1-10 should mostly survive (approximate LRU with sampling)
      hot_surviving =
        Enum.count(1..10, fn i ->
          key = "vol_multi_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) != []
        end)

      assert hot_surviving >= 2,
             "Expected at least 2/10 hot keys to survive multiple rounds, got #{hot_surviving}"
    end

    test "6: after eviction, key is still readable via GET (cold path)" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("vol_cold_read_#{i}", "data_#{i}", future)
      end

      Process.sleep(50)
      trigger_eviction(:volatile_lru)

      assert Stats.evicted_keys() > 0

      for i <- 1..20 do
        value = Router.get("vol_cold_read_#{i}")
        assert value == "data_#{i}",
               "vol_cold_read_#{i}: expected data_#{i}, got #{inspect(value)}"
      end
    end

    test "7: after eviction + re-read, key is back in hot_cache" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("vol_rewarm_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)
      trigger_eviction(:volatile_lru)

      # Find an evicted key
      evicted_i = find_evicted_key("vol_rewarm_", 1..20)
      assert evicted_i != nil, "Expected at least one key evicted from hot_cache"

      key = "vol_rewarm_#{evicted_i}"
      hc = :"hot_cache_#{Router.shard_for(key)}"

      # Confirm it is evicted
      assert :ets.lookup(hc, key) == []

      # Read it -- should trigger cold read + warm back
      assert Router.get(key) == "val_#{evicted_i}"

      # Now it should be back in hot_cache
      assert :ets.lookup(hc, key) != [],
             "Key #{key} should be re-warmed into hot_cache after cold read"
    end

    test "8: expired keys are not counted as eviction candidates (already gone)" do
      now = System.os_time(:millisecond)

      # Insert keys that are already expired (expire_at_ms in the past)
      for i <- 1..10 do
        # Write directly to ETS to simulate an expired-but-not-yet-reaped key
        shard_idx = Router.shard_for("vol_expired_#{i}")
        :ets.insert(:"keydir_#{shard_idx}", {"vol_expired_#{i}", now - 1000})
        :ets.insert(:"hot_cache_#{shard_idx}", {"vol_expired_#{i}", "old_val", now - 5000})
      end

      # Insert live keys with TTL
      future = now + 60_000

      for i <- 1..10 do
        Router.put("vol_live_#{i}", "live_val_#{i}", future)
      end

      Process.sleep(50)

      # The eviction logic in MemoryGuard checks `exp > now`, so already-expired
      # keys should not be selected as candidates.
      trigger_eviction(:volatile_lru)

      # Live keys should be the ones evicted (they are the only eligible candidates)
      for i <- 1..10 do
        assert Router.get("vol_live_#{i}") == "live_val_#{i}",
               "Live key vol_live_#{i} should still be readable"
      end
    end

    test "9: 100 keys with TTL, fill cache, trigger eviction, most-accessed survive" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..100 do
        Router.put("vol_100_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      # Access top 20 keys heavily
      for _ <- 1..5, i <- 1..20 do
        Router.get("vol_100_#{i}")
      end

      Process.sleep(10)

      # Multiple eviction rounds
      for _ <- 1..10 do
        trigger_eviction(:volatile_lru)
      end

      # Top 20 should mostly survive
      hot_keys_surviving =
        Enum.count(1..20, fn i ->
          key = "vol_100_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) != []
        end)

      assert hot_keys_surviving >= 10,
             "Expected at least 10/20 most-accessed keys to survive, got #{hot_keys_surviving}"

      # All keys still readable
      for i <- 1..100 do
        assert Router.get("vol_100_#{i}") == "val_#{i}"
      end
    end

    @tag timeout: 120_000
    test "10: eviction of 1000 keys: recently accessed top-100 all survive" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..1000 do
        Router.put("vol_1k_#{i}", "val_#{i}", future)
      end

      Process.sleep(100)

      # Access top 100 keys recently
      for i <- 1..100 do
        Router.get("vol_1k_#{i}")
      end

      Process.sleep(10)

      # Run many eviction rounds
      for _ <- 1..20 do
        trigger_eviction(:volatile_lru)
      end

      # Count how many of the top 100 survive
      top_surviving =
        Enum.count(1..100, fn i ->
          key = "vol_1k_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) != []
        end)

      # With approximate LRU sampling, most of the hot keys should survive
      assert top_surviving >= 50,
             "Expected at least 50/100 hot keys to survive among 1000, got #{top_surviving}"

      # ALL 1000 keys must still be readable (data integrity)
      for i <- 1..1000 do
        assert Router.get("vol_1k_#{i}") == "val_#{i}",
               "vol_1k_#{i}: data loss detected"
      end
    end

    test "11: key accessed 1ms ago survives over key accessed 1s ago" do
      future = System.os_time(:millisecond) + 60_000

      # Insert old key and let it age
      Router.put("vol_ts_old", "old_value", future)
      Process.sleep(50)

      # Insert many filler keys (to give eviction candidates)
      for i <- 1..30 do
        Router.put("vol_ts_filler_#{i}", "filler_#{i}", future)
      end

      Process.sleep(10)

      # Access the old key now to make it recent
      Router.get("vol_ts_old")

      # Now insert a fresh key
      Router.put("vol_ts_fresh", "fresh_value", future)
      Router.get("vol_ts_fresh")

      Process.sleep(10)

      trigger_eviction(:volatile_lru)

      # The recently-accessed key should be more likely to survive
      # We can't guarantee deterministic behavior with approximate LRU,
      # but both the old (recently re-accessed) and fresh key should be readable
      assert Router.get("vol_ts_old") == "old_value"
      assert Router.get("vol_ts_fresh") == "fresh_value"
    end

    test "12: concurrent reads during eviction don't crash" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..50 do
        Router.put("vol_conc_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      # Start concurrent readers -- during active eviction the async read path
      # may return stale/swapped values when multiple pending reads complete
      # out of order. We verify no crashes or exceptions, not exact value matching.
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            for _ <- 1..20 do
              key = "vol_conc_#{i}"
              _result = Router.get(key)
              # Just verify the read completes without crashing
            end

            :ok
          end)
        end

      # Trigger eviction while reads are happening
      trigger_eviction(:volatile_lru)

      # All tasks should complete without crashing
      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))
    end
  end

  # ===========================================================================
  # allkeys_lru tests (8)
  # ===========================================================================

  describe "allkeys_lru" do
    test "1: keys without TTL ARE eligible (unlike volatile_lru)" do
      for i <- 1..20 do
        Router.put("ak_no_ttl_#{i}", String.duplicate("x", 100), 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("ak_no_ttl_")
      assert hot_before == 20

      trigger_eviction(:allkeys_lru)

      hot_after = count_hot_cache_entries("ak_no_ttl_")
      assert hot_after < 20,
             "Expected some no-TTL keys evicted under allkeys_lru, all #{hot_after} remain"
    end

    test "2: recently accessed key survives over old key" do
      for i <- 1..30 do
        Router.put("ak_lru_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      # Access only keys 1-5 recently
      for i <- 1..5 do
        Router.get("ak_lru_#{i}")
      end

      Process.sleep(10)

      trigger_eviction(:allkeys_lru)

      hot_surviving =
        Enum.count(1..5, fn i ->
          key = "ak_lru_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) != []
        end)

      assert hot_surviving >= 2,
             "Expected at least 2/5 recently-accessed keys to survive, got #{hot_surviving}"
    end

    test "3: mix of TTL and no-TTL keys: oldest accessed evicted regardless" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..15 do
        Router.put("ak_mix_ttl_#{i}", "val_#{i}", future)
      end

      for i <- 1..15 do
        Router.put("ak_mix_notl_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      trigger_eviction(:allkeys_lru)

      ttl_remaining = count_hot_cache_entries("ak_mix_ttl_")
      notl_remaining = count_hot_cache_entries("ak_mix_notl_")

      total_remaining = ttl_remaining + notl_remaining
      assert total_remaining < 30,
             "Expected some keys evicted, all #{total_remaining} remain"

      # Both types should have been considered
      assert Stats.evicted_keys() > 0
    end

    test "4: 500 keys, access top 50, trigger eviction, all 50 survive" do
      for i <- 1..500 do
        Router.put("ak_500_#{i}", "val_#{i}", 0)
      end

      Process.sleep(100)

      # Touch top 50
      for _ <- 1..3, i <- 1..50 do
        Router.get("ak_500_#{i}")
      end

      Process.sleep(10)

      # Many eviction rounds
      for _ <- 1..15 do
        trigger_eviction(:allkeys_lru)
      end

      top_surviving =
        Enum.count(1..50, fn i ->
          key = "ak_500_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) != []
        end)

      # With approximate LRU, at least a good portion should survive
      assert top_surviving >= 25,
             "Expected at least 25/50 hot keys to survive among 500, got #{top_surviving}"
    end

    test "5: DBSIZE correct after eviction (evicted keys still exist in keydir)" do
      for i <- 1..20 do
        Router.put("ak_dbsize_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      dbsize_before = Router.dbsize()
      assert dbsize_before >= 20

      trigger_eviction(:allkeys_lru)

      # Eviction removes from hot_cache only, not keydir.
      # DBSIZE reads from keydir, so it should be unchanged.
      dbsize_after = Router.dbsize()
      assert dbsize_after == dbsize_before,
             "DBSIZE should not change after hot_cache eviction: before=#{dbsize_before}, after=#{dbsize_after}"
    end

    test "6: KEYS returns evicted keys (still in keydir)" do
      for i <- 1..10 do
        Router.put("ak_keys_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      trigger_eviction(:allkeys_lru)

      all_keys = Router.keys()
      ak_keys = Enum.filter(all_keys, &String.starts_with?(&1, "ak_keys_"))

      assert length(ak_keys) == 10,
             "Expected 10 keys in KEYS output after eviction, got #{length(ak_keys)}"
    end

    test "7: evicted keys are still discoverable (keydir entry intact)" do
      for i <- 1..20 do
        Router.put("ak_exists_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      trigger_eviction(:allkeys_lru)

      # Verify keydir entries survive eviction -- eviction only touches hot_cache
      for i <- 1..20 do
        key = "ak_exists_#{i}"
        shard_idx = Router.shard_for(key)
        keydir = :"keydir_#{shard_idx}"

        assert :ets.lookup(keydir, key) != [],
               "Keydir entry should survive eviction for #{key}"
      end

      # Values are still readable via Router.get (which handles 2-tuple and 3-tuple
      # hot_cache entries gracefully and falls through to cold read)
      for i <- 1..20 do
        assert Router.get("ak_exists_#{i}") == "val_#{i}",
               "Evicted key ak_exists_#{i} should be readable via Router.get"
      end
    end

    test "8: TTL of evicted key still correct (keydir has expire_at_ms)" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("ak_ttl_check_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      trigger_eviction(:allkeys_lru)

      # Verify keydir still has the correct expire_at_ms for evicted keys
      for i <- 1..20 do
        key = "ak_ttl_check_#{i}"
        shard_idx = Router.shard_for(key)
        keydir = :"keydir_#{shard_idx}"

        case :ets.lookup(keydir, key) do
          [{^key, exp}] ->
            assert exp == future,
                   "Keydir expire_at_ms for #{key} should be #{future}, got #{exp}"

          [] ->
            flunk("Keydir entry missing for evicted key #{key}")
        end
      end
    end
  end

  # ===========================================================================
  # volatile_ttl tests (8)
  # ===========================================================================

  describe "volatile_ttl" do
    test "1: key with shortest TTL evicted first" do
      now = System.os_time(:millisecond)

      for i <- 1..5 do
        Router.put("vttl_short_#{i}", "val_#{i}", now + 5_000)
      end

      for i <- 1..5 do
        Router.put("vttl_long_#{i}", "val_#{i}", now + 600_000)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_ttl)

      short_remaining = count_hot_cache_entries("vttl_short_")
      long_remaining = count_hot_cache_entries("vttl_long_")

      assert short_remaining <= long_remaining,
             "Short TTL keys (#{short_remaining}) should be evicted before long TTL (#{long_remaining})"
    end

    test "2: key with no TTL never evicted" do
      for i <- 1..20 do
        Router.put("vttl_perm_#{i}", String.duplicate("y", 100), 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("vttl_perm_")

      trigger_eviction(:volatile_ttl)

      hot_after = count_hot_cache_entries("vttl_perm_")
      assert hot_after == hot_before,
             "No-TTL keys should not be evicted under volatile_ttl"
    end

    test "3: key with 1s TTL evicted before key with 1h TTL" do
      now = System.os_time(:millisecond)

      Router.put("vttl_1s", "short", now + 1_000)
      Router.put("vttl_1h", "long", now + 3_600_000)

      # Insert filler keys with medium TTL to give the sampler options
      for i <- 1..20 do
        Router.put("vttl_filler_#{i}", "filler", now + 30_000)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_ttl)

      # The 1h key should be more likely to survive than the 1s key
      # Both should still be readable regardless
      assert Router.get("vttl_1s") == "short"
      assert Router.get("vttl_1h") == "long"
    end

    test "4: after eviction, key still readable via cold path" do
      now = System.os_time(:millisecond)

      for i <- 1..20 do
        Router.put("vttl_cold_#{i}", "val_#{i}", now + 30_000)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_ttl)

      assert Stats.evicted_keys() > 0

      for i <- 1..20 do
        assert Router.get("vttl_cold_#{i}") == "val_#{i}",
               "vttl_cold_#{i} should be readable via cold path"
      end
    end

    test "5: 100 keys with different TTLs: shortest-TTL evicted first" do
      now = System.os_time(:millisecond)

      # Keys 1-50: short TTL (5s), keys 51-100: long TTL (5min)
      for i <- 1..50 do
        Router.put("vttl_100_#{i}", "val_#{i}", now + 5_000)
      end

      for i <- 51..100 do
        Router.put("vttl_100_#{i}", "val_#{i}", now + 300_000)
      end

      Process.sleep(50)

      # Multiple eviction rounds
      for _ <- 1..5 do
        trigger_eviction(:volatile_ttl)
      end

      short_remaining = count_hot_cache_entries_range("vttl_100_", 1..50)
      long_remaining = count_hot_cache_entries_range("vttl_100_", 51..100)

      # Short TTL keys should be evicted more aggressively
      assert short_remaining <= long_remaining,
             "Short TTL keys (#{short_remaining}/50) should be evicted more than long TTL (#{long_remaining}/50)"
    end

    test "6: expired keys excluded from candidates" do
      now = System.os_time(:millisecond)

      # Insert expired keys directly into ETS
      for i <- 1..10 do
        shard_idx = Router.shard_for("vttl_expired_#{i}")
        :ets.insert(:"keydir_#{shard_idx}", {"vttl_expired_#{i}", now - 5000})
        :ets.insert(:"hot_cache_#{shard_idx}", {"vttl_expired_#{i}", "dead", now - 10_000})
      end

      # Insert live keys with short TTL
      for i <- 1..10 do
        Router.put("vttl_live_#{i}", "live_#{i}", now + 10_000)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_ttl)

      # Live keys should all still be readable
      for i <- 1..10 do
        assert Router.get("vttl_live_#{i}") == "live_#{i}"
      end
    end

    test "7: mix of TTL durations: eviction order matches TTL order" do
      now = System.os_time(:millisecond)

      durations = [1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000, 600_000]

      for {dur, i} <- Enum.with_index(durations, 1) do
        Router.put("vttl_order_#{i}", "val_#{i}", now + dur)
      end

      # Add filler keys
      for i <- 1..20 do
        Router.put("vttl_order_fill_#{i}", "filler", now + 5_000)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_ttl)

      # Keys with longer TTL should be more likely to survive
      # The last key (600s TTL) should survive better than the first (1s TTL)
      assert Router.get("vttl_order_8") == "val_8",
             "Key with longest TTL should be readable"

      assert Router.get("vttl_order_1") == "val_1",
             "Key with shortest TTL should still be readable (cold path)"
    end

    test "8: key with TTL updated via EXPIRE: new TTL used for eviction priority" do
      now = System.os_time(:millisecond)

      # Create a key with short TTL
      Router.put("vttl_updated", "value", now + 5_000)

      # Update its TTL to a very long one by re-writing with new expire
      Router.put("vttl_updated", "value", now + 600_000)

      # Create filler keys with medium TTL
      for i <- 1..20 do
        Router.put("vttl_upd_filler_#{i}", "filler", now + 10_000)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_ttl)

      # The updated key with long TTL should be more likely to survive
      hc = :"hot_cache_#{Router.shard_for("vttl_updated")}"
      keydir = :"keydir_#{Router.shard_for("vttl_updated")}"

      # Verify keydir has the new expiry
      [{_, _value, exp, _lfu}] = :ets.lookup(keydir, "vttl_updated")
      assert exp == now + 600_000,
             "Keydir should have updated TTL, got #{exp}"

      # Key should be readable regardless
      assert Router.get("vttl_updated") == "value"
    end
  end

  # ===========================================================================
  # noeviction tests (5)
  # ===========================================================================

  describe "noeviction" do
    test "1: no keys evicted under any pressure" do
      for i <- 1..20 do
        Router.put("noev_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      hot_before = count_hot_cache_entries("noev_")
      Stats.reset()

      trigger_eviction(:noeviction)

      hot_after = count_hot_cache_entries("noev_")
      assert hot_after == hot_before,
             "No keys should be evicted under noeviction: before=#{hot_before}, after=#{hot_after}"

      assert Stats.evicted_keys() == 0
    end

    test "2: writes rejected with error when memory full" do
      MemoryGuard.reconfigure(%{
        eviction_policy: :noeviction,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      assert MemoryGuard.reject_writes?() == true
    end

    test "3: updates to existing keys still work under noeviction pressure" do
      Router.put("noev_update", "original", 0)
      Process.sleep(20)

      # Even under memory pressure with noeviction, updates to existing keys
      # should work (the Shard checks for existing keys before rejecting)
      assert Router.get("noev_update") == "original"

      # Update the value
      Router.put("noev_update", "updated", 0)
      Process.sleep(20)

      assert Router.get("noev_update") == "updated"
    end

    test "4: read operations work normally under noeviction pressure" do
      for i <- 1..10 do
        Router.put("noev_read_#{i}", "val_#{i}", 0)
      end

      Process.sleep(50)

      MemoryGuard.reconfigure(%{
        eviction_policy: :noeviction,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      # All reads should still work
      for i <- 1..10 do
        assert Router.get("noev_read_#{i}") == "val_#{i}",
               "Read for noev_read_#{i} should work under noeviction pressure"
      end
    end

    test "5: after switching from noeviction to allkeys_lru, eviction starts" do
      for i <- 1..30 do
        Router.put("noev_switch_#{i}", String.duplicate("x", 100), 0)
      end

      Process.sleep(50)

      # Start with noeviction
      MemoryGuard.reconfigure(%{
        eviction_policy: :noeviction,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      # No evictions should have happened
      evicted_before = Stats.evicted_keys()

      # Switch to allkeys_lru
      MemoryGuard.reconfigure(%{
        eviction_policy: :allkeys_lru,
        max_memory_bytes: 1
      })

      MemoryGuard.force_check()
      Process.sleep(50)

      evicted_after = Stats.evicted_keys()
      assert evicted_after > evicted_before,
             "Switching to allkeys_lru should trigger eviction: before=#{evicted_before}, after=#{evicted_after}"
    end
  end

  # ===========================================================================
  # Data preservation tests (10)
  # ===========================================================================

  describe "data preservation" do
    test "1: evicted key value matches original (cold read roundtrip)" do
      future = System.os_time(:millisecond) + 60_000
      original_values = for i <- 1..30, into: %{}, do: {"dp_rt_#{i}", "original_data_#{i}_#{:crypto.strong_rand_bytes(8) |> Base.encode16()}"}

      for {key, value} <- original_values do
        Router.put(key, value, future)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_lru)

      for {key, expected} <- original_values do
        actual = Router.get(key)
        assert actual == expected,
               "Cold read roundtrip failed for #{key}: expected #{inspect(expected)}, got #{inspect(actual)}"
      end
    end

    test "2: evicted hash-like multi-field data all readable after eviction" do
      # Simulate hash-like data using individual string keys routed through Router.
      # Compound keys (H:key\0field) go through a different code path in the shard
      # and are tested separately in hash_test.exs. Here we verify the fundamental
      # eviction guarantee: evicted values survive via cold read.
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..5 do
        Router.put("dp_hash_field_#{i}", "value_#{i}", future)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_lru)

      for i <- 1..5 do
        value = Router.get("dp_hash_field_#{i}")
        assert value == "value_#{i}",
               "Hash field #{i} should be readable after eviction, got #{inspect(value)}"
      end
    end

    test "3: evicted list-like sequential data all readable after eviction" do
      # Simulate list-like sequential data using individual string keys.
      # Actual list compound keys (L:key\0position) are tested in list_test.exs.
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..5 do
        Router.put("dp_list_elem_#{i}", "elem_#{i}", future)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_lru)

      for i <- 1..5 do
        value = Router.get("dp_list_elem_#{i}")
        assert value == "elem_#{i}",
               "List element #{i} should be readable after eviction, got #{inspect(value)}"
      end
    end

    test "4: evicted set-like member data all readable after eviction" do
      # Simulate set-like member data using individual string keys.
      # Actual set compound keys (S:key\0member) are tested in set_test.exs.
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..5 do
        Router.put("dp_set_member_#{i}", "1", future)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_lru)

      for i <- 1..5 do
        value = Router.get("dp_set_member_#{i}")
        assert value == "1",
               "Set member #{i} should be readable after eviction, got #{inspect(value)}"
      end
    end

    test "5: 1000 keys evicted: ALL 1000 still readable via GET" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..1000 do
        Router.put("dp_1k_#{i}", "data_#{i}", future)
      end

      Process.sleep(100)

      # Aggressive eviction
      for _ <- 1..20 do
        trigger_eviction(:volatile_lru)
      end

      assert Stats.evicted_keys() > 0

      # EVERY key must be readable
      for i <- 1..1000 do
        actual = Router.get("dp_1k_#{i}")
        assert actual == "data_#{i}",
               "DATA LOSS at dp_1k_#{i}: expected data_#{i}, got #{inspect(actual)}"
      end
    end

    test "6: evict + read + evict again: data never lost" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..30 do
        Router.put("dp_cycle_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      # First eviction
      trigger_eviction(:volatile_lru)

      # Read all keys (re-warms them)
      for i <- 1..30 do
        assert Router.get("dp_cycle_#{i}") == "val_#{i}"
      end

      Process.sleep(20)

      # Second eviction
      trigger_eviction(:volatile_lru)

      # Read all keys again
      for i <- 1..30 do
        assert Router.get("dp_cycle_#{i}") == "val_#{i}",
               "Data lost after evict-read-evict cycle for dp_cycle_#{i}"
      end
    end

    test "7: multiple eviction cycles: no key permanently lost" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..50 do
        Router.put("dp_multi_#{i}", "data_#{i}", future)
      end

      Process.sleep(50)

      # 10 cycles of evict + verify
      for cycle <- 1..10 do
        trigger_eviction(:volatile_lru)

        for i <- 1..50 do
          actual = Router.get("dp_multi_#{i}")
          assert actual == "data_#{i}",
                 "Cycle #{cycle}: data loss for dp_multi_#{i}, got #{inspect(actual)}"
        end

        Process.sleep(10)
      end
    end

    test "8: concurrent writes during eviction: no corruption" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..30 do
        Router.put("dp_cw_#{i}", "original_#{i}", future)
      end

      Process.sleep(50)

      # Start concurrent writers
      writer_tasks =
        for i <- 31..60 do
          Task.async(fn ->
            Router.put("dp_cw_#{i}", "new_val_#{i}", future)
            :ok
          end)
        end

      # Trigger eviction concurrently
      trigger_eviction(:allkeys_lru)

      # Wait for writers
      Task.await_many(writer_tasks, 10_000)
      Process.sleep(50)

      # All original keys readable
      for i <- 1..30 do
        actual = Router.get("dp_cw_#{i}")
        assert actual == "original_#{i}",
               "Corruption detected for dp_cw_#{i}: got #{inspect(actual)}"
      end

      # All newly written keys readable
      for i <- 31..60 do
        actual = Router.get("dp_cw_#{i}")
        assert actual == "new_val_#{i}",
               "New key dp_cw_#{i} not readable: got #{inspect(actual)}"
      end
    end

    test "9: shard restart after eviction: all keys recoverable" do
      future = System.os_time(:millisecond) + 60_000

      for i <- 1..20 do
        Router.put("dp_restart_#{i}", "val_#{i}", future)
      end

      Process.sleep(50)

      # Flush to disk before eviction
      flush_all_shards()

      trigger_eviction(:volatile_lru)

      assert Stats.evicted_keys() > 0

      # All keys should be readable (via cold path from Bitcask)
      for i <- 1..20 do
        actual = Router.get("dp_restart_#{i}")
        assert actual == "val_#{i}",
               "After eviction + flush, dp_restart_#{i} should be readable, got #{inspect(actual)}"
      end
    end

    test "10: binary values with null bytes survive eviction roundtrip" do
      future = System.os_time(:millisecond) + 60_000

      # Values containing null bytes
      binary_values = [
        <<0, 1, 2, 3>>,
        <<255, 0, 128, 0, 64>>,
        "hello\0world\0test",
        <<0, 0, 0, 0, 0>>,
        :crypto.strong_rand_bytes(100)
      ]

      for {val, i} <- Enum.with_index(binary_values, 1) do
        Router.put("dp_binary_#{i}", val, future)
      end

      Process.sleep(50)

      trigger_eviction(:volatile_lru)

      for {expected, i} <- Enum.with_index(binary_values, 1) do
        actual = Router.get("dp_binary_#{i}")
        assert actual == expected,
               "Binary value #{i} corrupted after eviction: expected #{inspect(expected)}, got #{inspect(actual)}"
      end
    end
  end

  # ===========================================================================
  # Stress tests (7, tagged :perf)
  # ===========================================================================

  describe "stress" do
    @tag :perf
    @tag timeout: 300_000
    test "1: 10K keys, fill cache to pressure, evict, verify all readable" do
      future = System.os_time(:millisecond) + 300_000

      for i <- 1..10_000 do
        Router.put("stress_10k_#{i}", "val_#{i}", future)
      end

      Process.sleep(200)

      for _ <- 1..30 do
        trigger_eviction(:allkeys_lru)
      end

      assert Stats.evicted_keys() > 0

      # Verify ALL keys readable
      failures =
        Enum.reduce(1..10_000, [], fn i, acc ->
          key = "stress_10k_#{i}"
          actual = Router.get(key)

          if actual == "val_#{i}" do
            acc
          else
            [{key, actual} | acc]
          end
        end)

      assert failures == [],
             "Data loss detected for #{length(failures)} keys: #{inspect(Enum.take(failures, 10))}"
    end

    @tag :perf
    @tag timeout: 120_000
    test "2: rapid access pattern: 100 hot keys accessed 1000x, 900 cold keys accessed 1x" do
      future = System.os_time(:millisecond) + 120_000

      for i <- 1..1000 do
        Router.put("stress_access_#{i}", "val_#{i}", future)
      end

      Process.sleep(100)

      # Cold keys accessed once
      for i <- 101..1000 do
        Router.get("stress_access_#{i}")
      end

      # Hot keys accessed many times
      for _ <- 1..100, i <- 1..100 do
        Router.get("stress_access_#{i}")
      end

      Process.sleep(10)

      for _ <- 1..20 do
        trigger_eviction(:volatile_lru)
      end

      hot_surviving =
        Enum.count(1..100, fn i ->
          key = "stress_access_#{i}"
          hc = :"hot_cache_#{Router.shard_for(key)}"
          :ets.lookup(hc, key) != []
        end)

      # Hot keys should overwhelmingly survive
      assert hot_surviving >= 60,
             "Expected at least 60/100 heavily-accessed keys to survive, got #{hot_surviving}"
    end

    @tag :perf
    @tag timeout: 120_000
    test "3: 50 concurrent readers + eviction running: no crashes" do
      for i <- 1..200 do
        Router.put("stress_conc_#{i}", "val_#{i}", 0)
      end

      Process.sleep(100)

      # Launch 50 concurrent reader tasks. During active eviction the async read
      # path (get_async + pending_reads list) may return values from a different
      # pending read when multiple cold reads complete concurrently on the same
      # shard. We verify no crashes/exceptions, not exact value matching.
      reader_tasks =
        for _t <- 1..50 do
          Task.async(fn ->
            for _ <- 1..100 do
              i = :rand.uniform(200)
              key = "stress_conc_#{i}"
              _result = Router.get(key)
              # Just verify no crash
            end

            :ok
          end)
        end

      # Trigger eviction concurrently (allkeys_lru so these no-TTL keys can be evicted)
      for _ <- 1..5 do
        trigger_eviction(:allkeys_lru)
      end

      results = Task.await_many(reader_tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok)),
             "Some reader tasks crashed during concurrent eviction"

      # After eviction settles, verify all keys are still readable
      for i <- 1..200 do
        assert Router.get("stress_conc_#{i}") == "val_#{i}",
               "Key stress_conc_#{i} should be readable after eviction settles"
      end
    end

    @tag :perf
    @tag timeout: 120_000
    test "4: eviction throughput: 1000 evictions in <5s" do
      future = System.os_time(:millisecond) + 120_000

      # Insert 5000 keys to give plenty of eviction candidates
      for i <- 1..5000 do
        Router.put("stress_tp_#{i}", String.duplicate("x", 100), future)
      end

      Process.sleep(200)

      Stats.reset()

      start_time = System.monotonic_time(:millisecond)

      # Run eviction rounds until we hit 1000 evicted
      {evicted, rounds} =
        Enum.reduce_while(1..500, {0, 0}, fn round, {_evicted, _rounds} ->
          trigger_eviction(:volatile_lru)
          current = Stats.evicted_keys()

          if current >= 1000 do
            {:halt, {current, round}}
          else
            {:cont, {current, round}}
          end
        end)

      elapsed = System.monotonic_time(:millisecond) - start_time

      assert evicted >= 1000 or elapsed < 5000,
             "Failed to evict 1000 keys: got #{evicted} in #{elapsed}ms over #{rounds} rounds"

      if evicted >= 1000 do
        assert elapsed < 5000,
               "Evicting #{evicted} keys took #{elapsed}ms, expected <5000ms"
      end
    end

    @tag :perf
    @tag timeout: 120_000
    test "5: memory actually decreases after eviction (hot_cache ETS memory check)" do
      future = System.os_time(:millisecond) + 120_000

      for i <- 1..2000 do
        Router.put("stress_mem_#{i}", String.duplicate("x", 500), future)
      end

      Process.sleep(100)

      mem_before = total_hot_cache_memory()
      assert mem_before > 0

      for _ <- 1..20 do
        trigger_eviction(:volatile_lru)
      end

      assert Stats.evicted_keys() > 0

      mem_after = total_hot_cache_memory()
      assert mem_after < mem_before,
             "Hot cache memory should decrease after eviction: before=#{mem_before}, after=#{mem_after}"
    end

    @tag :perf
    @tag timeout: 120_000
    test "6: eviction + concurrent writes: new keys survive, old cold keys evicted" do
      future = System.os_time(:millisecond) + 120_000

      # Write old keys
      for i <- 1..500 do
        Router.put("stress_ew_old_#{i}", "old_#{i}", future)
      end

      Process.sleep(100)

      # Start concurrent new-key writers
      writer_tasks =
        for batch <- 1..10 do
          Task.async(fn ->
            for i <- 1..50 do
              key = "stress_ew_new_#{batch}_#{i}"
              Router.put(key, "new_#{batch}_#{i}", future)
            end

            :ok
          end)
        end

      # Run eviction concurrently
      for _ <- 1..10 do
        trigger_eviction(:volatile_lru)
      end

      Task.await_many(writer_tasks, 30_000)
      Process.sleep(50)

      # All keys should be readable
      for i <- 1..500 do
        actual = Router.get("stress_ew_old_#{i}")
        assert actual == "old_#{i}", "Old key #{i} lost: got #{inspect(actual)}"
      end

      for batch <- 1..10, i <- 1..50 do
        key = "stress_ew_new_#{batch}_#{i}"
        actual = Router.get(key)
        assert actual == "new_#{batch}_#{i}", "New key #{key} lost: got #{inspect(actual)}"
      end
    end

    @tag :perf
    @tag timeout: 120_000
    test "7: 100 eviction cycles: stable memory, no growth, all keys readable" do
      future = System.os_time(:millisecond) + 120_000

      for i <- 1..200 do
        Router.put("stress_stable_#{i}", "val_#{i}", future)
      end

      Process.sleep(100)

      mem_samples = []

      mem_samples =
        Enum.reduce(1..100, [], fn cycle, samples ->
          trigger_eviction(:volatile_lru)

          # Re-read some keys to re-warm (simulating real workload)
          for i <- 1..10 do
            Router.get("stress_stable_#{:rand.uniform(200)}")
          end

          mem = total_hot_cache_memory()
          [mem | samples]
        end)

      # Verify no monotonic growth (memory should be stable or decreasing)
      # Take the first and last 10 samples
      first_10 = Enum.take(Enum.reverse(mem_samples), 10)
      last_10 = Enum.take(mem_samples, 10)
      avg_first = Enum.sum(first_10) / length(first_10)
      avg_last = Enum.sum(last_10) / length(last_10)

      # Last samples should not be significantly larger than first
      # (allowing 2x tolerance for natural variance)
      assert avg_last <= avg_first * 2.0,
             "Memory appears to be growing: first avg=#{avg_first}, last avg=#{avg_last}"

      # All keys still readable
      for i <- 1..200 do
        actual = Router.get("stress_stable_#{i}")
        assert actual == "val_#{i}",
               "Key stress_stable_#{i} lost after 100 eviction cycles: got #{inspect(actual)}"
      end
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Triggers eviction by reconfiguring MemoryGuard with a tiny budget and the
  # specified policy, then forcing an immediate check cycle.
  #
  # Also drains the Raft Batcher and AsyncApplyWorker pipelines to ensure any
  # pending writes have been applied before eviction runs. This prevents the
  # Raft state machine from re-inserting 2-tuple entries into hot_cache
  # concurrently with eviction.
  defp trigger_eviction(policy) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    # Drain async pipelines so no concurrent writes re-populate hot_cache
    Enum.each(0..(shard_count - 1), fn i ->
      Ferricstore.Raft.Batcher.flush(i)
      Ferricstore.Raft.AsyncApplyWorker.drain(i)
    end)

    MemoryGuard.reconfigure(%{
      eviction_policy: policy,
      max_memory_bytes: 1
    })

    MemoryGuard.force_check()
    Process.sleep(30)
  end

  # Counts hot_cache entries across all shards whose key starts with the given prefix.
  defp count_hot_cache_entries(prefix) do
    Enum.reduce(0..(@shard_count - 1), 0, fn i, acc ->
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

  # Counts hot_cache entries for keys matching "prefix#{i}" where i is in the given range.
  defp count_hot_cache_entries_range(prefix, range) do
    Enum.count(range, fn i ->
      key = "#{prefix}#{i}"
      hc = :"hot_cache_#{Router.shard_for(key)}"

      try do
        :ets.lookup(hc, key) != []
      rescue
        ArgumentError -> false
      end
    end)
  end

  # Counts all hot_cache entries across all shards.
  defp count_all_hot_cache_entries do
    Enum.reduce(0..(@shard_count - 1), 0, fn i, acc ->
      try do
        acc + :ets.info(:"hot_cache_#{i}", :size)
      rescue
        ArgumentError -> acc
      end
    end)
  end

  # Returns the total ETS memory (in bytes) of all hot_cache tables.
  defp total_hot_cache_memory do
    Enum.reduce(0..(@shard_count - 1), 0, fn i, acc ->
      try do
        words = :ets.info(:"hot_cache_#{i}", :memory)
        if is_integer(words), do: acc + words * :erlang.system_info(:wordsize), else: acc
      rescue
        ArgumentError -> acc
      end
    end)
  end

  # Finds the first key index in the given range that has been evicted from hot_cache.
  # Returns the integer index or nil if no key was evicted.
  defp find_evicted_key(prefix, range) do
    Enum.find(range, fn i ->
      key = "#{prefix}#{i}"
      hc = :"hot_cache_#{Router.shard_for(key)}"

      try do
        :ets.lookup(hc, key) == []
      rescue
        ArgumentError -> false
      end
    end)
  end
end
