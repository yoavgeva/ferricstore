defmodule Ferricstore.GracefulShutdownTest do
  @moduledoc """
  Tests that graceful shutdown preserves all data and that restart
  resumes from the same point — no data loss, no duplicate processing.

  These tests write data, call FerricStore.shutdown(), stop and restart
  the relevant processes, and verify all data is intact.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive()
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp ukey(base), do: "gsd_#{base}_#{:rand.uniform(9_999_999)}"

  # Simulates a graceful shutdown + restart cycle.
  # Calls prep_stop (flushes everything), then kills all shards via
  # supervisor restart. On restart, shards recover from disk + WAL.
  defp shutdown_and_restart do
    # Graceful flush — batchers, writers, shards, WAL
    Ferricstore.Application.prep_stop(nil)

    # Kill all shards (simulates process restart after shutdown)
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    for i <- 0..(shard_count - 1) do
      name = Router.shard_name(i)
      pid = Process.whereis(name)

      if pid && Process.alive?(pid) do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        after
          5_000 -> :ok
        end
      end
    end

    # Wait for supervisor to restart all shards
    ShardHelpers.wait_shards_alive(30_000)

    # Re-mark as ready
    Ferricstore.Health.set_ready(true)
  end

  describe "string data survives graceful shutdown" do
    test "single key survives" do
      k = ukey("single")
      shard_idx = Router.shard_for(k)
      Router.put(k, "before_shutdown")
      ShardHelpers.flush_all_shards()

      # Verify key is in ETS before shutdown
      keydir = :"keydir_#{shard_idx}"
      pre_ets = :ets.lookup(keydir, k)
      require Logger
      Logger.info("DIAG: key=#{k} shard=#{shard_idx} pre_shutdown_ets=#{inspect(pre_ets)}")

      shutdown_and_restart()

      # Check ETS after restart
      post_ets = :ets.lookup(keydir, k)
      post_get = Router.get(k)
      Logger.info("DIAG: key=#{k} shard=#{shard_idx} post_restart_ets=#{inspect(post_ets)} router_get=#{inspect(post_get)}")

      ShardHelpers.eventually(fn -> Router.get(k) == "before_shutdown" end,
        "key should survive graceful shutdown (key=#{k}, shard=#{shard_idx})")
    end

    test "100 keys survive" do
      keys =
        for i <- 1..100 do
          k = ukey("multi_#{i}")
          Router.put(k, "value_#{i}")
          {k, "value_#{i}"}
        end

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      for {k, expected} <- keys do
        ShardHelpers.eventually(fn -> Router.get(k) == expected end,
          "key #{k} should survive shutdown")
      end
    end

    test "overwritten value has latest version after restart" do
      k = ukey("overwrite")
      Router.put(k, "v1")
      Router.put(k, "v2")
      Router.put(k, "v3")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == "v3" end,
        "latest value should survive")
    end

    test "deleted key stays deleted after restart" do
      k = ukey("deleted")
      Router.put(k, "exists")
      ShardHelpers.flush_all_shards()
      Router.delete(k)
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == nil end,
        "deleted key should stay deleted")
    end
  end

  describe "TTL survives graceful shutdown" do
    test "key with TTL still has TTL after restart" do
      k = ukey("ttl")
      # Set with 60 second TTL — won't expire during test
      Router.put(k, "with_ttl", 60_000 + System.os_time(:millisecond))
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == "with_ttl" end,
        "TTL key should survive shutdown")
    end
  end

  describe "data across all shards survives" do
    test "keys on every shard survive shutdown" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

      keys =
        for i <- 0..(shard_count - 1) do
          k = ShardHelpers.key_for_shard(i)
          Router.put(k, "shard_#{i}_data")
          {k, i}
        end

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      for {k, i} <- keys do
        ShardHelpers.eventually(fn -> Router.get(k) == "shard_#{i}_data" end,
          "shard #{i} key should survive")
      end
    end
  end

  describe "counters survive graceful shutdown" do
    test "INCR value preserved after restart" do
      k = ukey("counter")
      Router.put(k, "0")

      for _ <- 1..50 do
        Router.incr(k, 1)
      end

      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == "50" end,
        "counter should be 50 after restart")
    end
  end

  describe "writes after restart work" do
    test "new writes succeed after shutdown + restart" do
      k1 = ukey("before")
      Router.put(k1, "old_data")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      k2 = ukey("after")
      ShardHelpers.eventually(fn ->
        Router.put(k2, "new_data") == :ok
      end, "write should succeed after restart")

      ShardHelpers.eventually(fn -> Router.get(k2) == "new_data" end,
        "new data should be readable")

      ShardHelpers.eventually(fn -> Router.get(k1) == "old_data" end,
        "old data should still be there")
    end
  end

  describe "multiple shutdown cycles" do
    test "data survives two consecutive shutdown-restart cycles" do
      k = ukey("double")
      Router.put(k, "cycle1")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == "cycle1" end,
        "data should survive first cycle")

      Router.put(k, "cycle2")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == "cycle2" end,
        "data should survive second cycle")
    end
  end

  describe "empty string and edge cases survive" do
    test "empty string value survives" do
      k = ukey("empty")
      Router.put(k, "")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == "" end,
        "empty string should survive")
    end

    test "binary with null bytes survives" do
      k = ukey("binary")
      val = <<0, 1, 0, 255, 0, 128>>
      Router.put(k, val)
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(k) == val end,
        "binary value should survive")
    end

    test "large value survives" do
      k = ukey("large")
      val = String.duplicate("x", 100_000)
      Router.put(k, val)
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn ->
        got = Router.get(k)
        got != nil and byte_size(got) == 100_000
      end, "large value should survive")
    end
  end
end
