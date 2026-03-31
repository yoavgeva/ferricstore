defmodule Ferricstore.Review.H7DedicatedCompactCleanupTest do
  @moduledoc """
  Verifies that leftover compact_*.log files in dedicated promoted
  directories are cleaned up on recovery and don't crash file parsing.
  """

  use ExUnit.Case, async: false

  @moduletag :shard_kill

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.{Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  @test_threshold 5

  setup do
    original = Application.get_env(:ferricstore, :promotion_threshold)
    original_pt =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, @test_threshold)
    :persistent_term.put(:ferricstore_promotion_threshold, @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      if original do
        Application.put_env(:ferricstore, :promotion_threshold, original)
      else
        Application.delete_env(:ferricstore, :promotion_threshold)
      end

      case original_pt do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        val -> :persistent_term.put(:ferricstore_promotion_threshold, val)
      end

      ShardHelpers.wait_shards_alive()
    end)
  end

  defp real_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn -> :ok end,
      dbsize: &Router.dbsize/0,
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

  defp ukey(base), do: "h7_#{base}_#{:rand.uniform(9_999_999)}"

  defp dedicated_dir(key) do
    shard_idx = Router.shard_for(key)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    Promotion.dedicated_path(data_dir, shard_idx, :hash, key)
  end

  test "shard starts after compact_*.log left in dedicated dir" do
    store = real_store()
    key = ukey("compact_cleanup")

    # Promote a hash
    pairs = Enum.flat_map(1..(@test_threshold + 1), fn i -> ["f_#{i}", "v_#{i}"] end)
    Hash.handle("HSET", [key | pairs], store)

    dir = dedicated_dir(key)
    assert File.exists?(dir)

    # Create a fake leftover compact file
    compact_file = Path.join(dir, "compact_0.log")
    File.write!(compact_file, "partial compaction garbage")
    assert File.exists?(compact_file)

    # Flush and kill shard
    ShardHelpers.flush_all_shards()
    shard_idx = Router.shard_for(key)
    ShardHelpers.kill_shard_safely(shard_idx)

    # Shard should restart without crashing
    # compact file should be cleaned up
    refute File.exists?(compact_file)

    # Data should survive
    ShardHelpers.eventually(fn ->
      "v_1" == Hash.handle("HGET", [key, "f_1"], store)
    end, "promoted hash data should survive after compact cleanup")
  end
end
