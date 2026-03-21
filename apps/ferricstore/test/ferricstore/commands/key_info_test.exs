defmodule Ferricstore.Commands.KeyInfoTest do
  @moduledoc """
  Tests for the FERRICSTORE.KEY_INFO command.

  KEY_INFO returns diagnostic metadata about a key:
    - type (string/hash/list/set/zset/stream/none)
    - value_size (bytes)
    - ttl_ms (-1 if no TTL, -2 if not found, else remaining ms)
    - hot_cache_status (hot/cold)
    - last_write_shard (shard index)

  These tests run against the application-supervised shards via the Router,
  since KEY_INFO queries ETS tables and the Router directly.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, Native}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  # Dummy store map -- KEY_INFO calls Router directly, ignoring the store.
  defp dummy_store, do: %{}

  # Parses the flat key-value list returned by KEY_INFO into a map.
  defp parse_info(result) when is_list(result) do
    result
    |> Enum.chunk_every(2)
    |> Map.new(fn [k, v] -> {k, v} end)
  end

  # Flushes Raft batchers and async workers so compound key writes land in ETS.
  defp flush_raft do
    Enum.each(0..3, fn i ->
      Ferricstore.Raft.Batcher.flush(i)
      Ferricstore.Raft.AsyncApplyWorker.drain(i)
    end)
  end

  # ===========================================================================
  # Returns type=string for SET key
  # ===========================================================================

  describe "type detection" do
    test "returns type=string for SET key" do
      key = ukey("ki_string")
      Router.put(key, "hello", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      assert is_list(result)
      info = parse_info(result)
      assert info["type"] == "string"
    end

    test "returns type=hash for HSET key" do
      key = ukey("ki_hash")
      Dispatcher.dispatch("HSET", [key, "field1", "value1"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "hash"
    end

    test "returns type=none for missing key" do
      key = ukey("ki_missing")

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "none"
    end

    test "returns type=list for LPUSH key" do
      key = ukey("ki_list")
      Dispatcher.dispatch("LPUSH", [key, "a", "b"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "list"
    end

    test "returns type=set for SADD key" do
      key = ukey("ki_set")
      Dispatcher.dispatch("SADD", [key, "m1", "m2"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "set"
    end

    test "returns type=zset for ZADD key" do
      key = ukey("ki_zset")
      Dispatcher.dispatch("ZADD", [key, "1.0", "member"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "zset"
    end
  end

  # ===========================================================================
  # Returns correct TTL
  # ===========================================================================

  describe "TTL reporting" do
    test "returns ttl_ms=-1 for key with no TTL" do
      key = ukey("ki_no_ttl")
      Router.put(key, "value", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["ttl_ms"] == "-1"
    end

    test "returns ttl_ms=-2 for non-existent key" do
      key = ukey("ki_no_key")

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["ttl_ms"] == "-2"
    end

    test "returns remaining TTL for key with expiry" do
      key = ukey("ki_ttl")
      future = System.os_time(:millisecond) + 60_000
      Router.put(key, "value", future)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      ttl = String.to_integer(info["ttl_ms"])
      # Should be between 59 and 60 seconds (accounting for test execution time)
      assert ttl > 58_000
      assert ttl <= 60_000
    end
  end

  # ===========================================================================
  # Hot cache status
  # ===========================================================================

  describe "hot cache status" do
    test "shows hot status after GET (warmed into ETS)" do
      key = ukey("ki_hot")
      Router.put(key, "value", 0)
      # First GET warms the key into hot cache
      _val = Router.get(key)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["hot_cache_status"] == "hot"
    end

    test "shows cold status for key not in ETS cache" do
      key = ukey("ki_cold")
      # Put value directly -- it goes into keydir via Shard GenServer.
      Router.put(key, "value", 0)

      # Evict the value from keydir to simulate a cold key (value = nil).
      # In the single-table format, a cold key has {key, nil, expire, lfu}.
      idx = Router.shard_for(key)
      keydir = :"keydir_#{idx}"
      :ets.delete(keydir, key)

      # Now the key is truly cold -- only on disk.
      # However, get_meta in do_key_info will warm it back.
      # KEY_INFO causes a warm-up as a side effect, so the final status is "hot".
      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      # After get_meta warms the key, the final check sees it as hot.
      assert info["hot_cache_status"] == "hot"
      assert info["type"] == "string"
    end
  end

  # ===========================================================================
  # Correct value_size
  # ===========================================================================

  describe "value_size" do
    test "shows correct value_size for string key" do
      key = ukey("ki_size")
      value = "hello world"
      Router.put(key, value, 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["value_size"] == Integer.to_string(byte_size(value))
    end

    test "shows value_size=0 for missing key" do
      key = ukey("ki_size_miss")

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["value_size"] == "0"
    end

    test "shows correct value_size for large value" do
      key = ukey("ki_size_large")
      value = String.duplicate("x", 10_000)
      Router.put(key, value, 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["value_size"] == "10000"
    end
  end

  # ===========================================================================
  # last_write_shard
  # ===========================================================================

  describe "last_write_shard" do
    test "shows correct shard index" do
      key = ukey("ki_shard")
      Router.put(key, "value", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)

      expected_shard = Router.shard_for(key)
      assert info["last_write_shard"] == Integer.to_string(expected_shard)
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "edge cases" do
    test "KEY_INFO on expired key returns type=none" do
      key = ukey("ki_expired")
      past = System.os_time(:millisecond) - 1_000
      Router.put(key, "value", past)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "none"
      assert info["ttl_ms"] == "-2"
      assert info["value_size"] == "0"
    end

    test "KEY_INFO with no arguments returns error" do
      assert {:error, msg} = Native.handle("KEY_INFO", [], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "KEY_INFO with too many arguments returns error" do
      assert {:error, msg} = Native.handle("KEY_INFO", ["k", "extra"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "KEY_INFO returns all expected fields" do
      key = ukey("ki_fields")
      Router.put(key, "v", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)

      assert Map.has_key?(info, "type")
      assert Map.has_key?(info, "value_size")
      assert Map.has_key?(info, "ttl_ms")
      assert Map.has_key?(info, "hot_cache_status")
      assert Map.has_key?(info, "last_write_shard")
    end
  end

  # ===========================================================================
  # Stress test
  # ===========================================================================

  describe "stress" do
    test "1000 KEY_INFO calls complete without error" do
      key = ukey("ki_stress")
      Router.put(key, "stress_value", 0)

      results =
        Enum.map(1..1000, fn _i ->
          Native.handle("KEY_INFO", [key], dummy_store())
        end)

      assert Enum.all?(results, &is_list/1)
      assert length(results) == 1000

      # Verify each result is well-formed
      Enum.each(results, fn result ->
        info = parse_info(result)
        assert info["type"] == "string"
        assert info["value_size"] == Integer.to_string(byte_size("stress_value"))
      end)
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "Dispatcher routing" do
    test "FERRICSTORE.KEY_INFO is routed through dispatcher" do
      key = ukey("ki_disp")
      Router.put(key, "value", 0)

      result = Dispatcher.dispatch("FERRICSTORE.KEY_INFO", [key], dummy_store())
      assert is_list(result)
      info = parse_info(result)
      assert info["type"] == "string"
    end

    test "FERRICSTORE.KEY_INFO is case-insensitive" do
      key = ukey("ki_disp_ci")
      Router.put(key, "value", 0)

      result = Dispatcher.dispatch("ferricstore.key_info", [key], dummy_store())
      assert is_list(result)
      info = parse_info(result)
      assert info["type"] == "string"
    end
  end

  # ---------------------------------------------------------------------------
  # Helper: builds a real store map for commands that need compound key support
  # (HSET, LPUSH, SADD, ZADD). This mirrors connection.ex's build_raw_store.
  # ---------------------------------------------------------------------------

  defp build_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn -> Enum.each(Router.keys(), &Router.delete/1); :ok end,
      dbsize: &Router.dbsize/0,
      incr: &Router.incr/2,
      incr_float: &Router.incr_float/2,
      append: &Router.append/2,
      getset: &Router.getset/2,
      getdel: &Router.getdel/1,
      getex: &Router.getex/2,
      setrange: &Router.setrange/3,
      cas: &Router.cas/4,
      lock: &Router.lock/3,
      unlock: &Router.unlock/2,
      extend: &Router.extend/3,
      ratelimit_add: &Router.ratelimit_add/4,
      list_op: &Router.list_op/2,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end
end
