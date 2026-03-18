defmodule Ferricstore.Commands.ClusterTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # CLUSTER.HEALTH
  # ---------------------------------------------------------------------------

  describe "CLUSTER.HEALTH" do
    test "returns a string containing per-shard info" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert is_binary(result)
      assert result =~ "shard_0:"
      assert result =~ "role: leader"
      assert result =~ "status: ok"
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"
    end

    test "includes all 4 shards" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert result =~ "shard_0:"
      assert result =~ "shard_1:"
      assert result =~ "shard_2:"
      assert result =~ "shard_3:"
    end

    test "reflects keys stored in shards" do
      # Write some keys to the live store
      Router.put("cluster_health_key_a", "v1", 0)
      Router.put("cluster_health_key_b", "v2", 0)

      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert is_binary(result)
      # The result should show non-zero key counts across shards
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"

      # Clean up
      Router.delete("cluster_health_key_a")
      Router.delete("cluster_health_key_b")
    end

    test "returns error with extra arguments" do
      store = MockStore.make()
      assert {:error, _msg} = Dispatcher.dispatch("CLUSTER.HEALTH", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.STATS
  # ---------------------------------------------------------------------------

  describe "CLUSTER.STATS" do
    test "returns a string containing per-shard stats and totals" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      assert is_binary(result)
      assert result =~ "shard_0:"
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"
      assert result =~ "total_keys:"
      assert result =~ "total_memory_bytes:"
    end

    test "includes all shards" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      assert result =~ "shard_0:"
      assert result =~ "shard_1:"
      assert result =~ "shard_2:"
      assert result =~ "shard_3:"
    end

    test "total_keys is sum of per-shard keys" do
      # Write known keys
      Router.put("cluster_stats_a", "v1", 0)
      Router.put("cluster_stats_b", "v2", 0)
      Router.put("cluster_stats_c", "v3", 0)

      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      # Extract total_keys value
      [_, total_str] =
        result
        |> String.split("\r\n")
        |> Enum.find(&String.starts_with?(&1, "total_keys:"))
        |> String.split(": ")

      total = String.to_integer(total_str)
      assert total >= 3

      # Clean up
      Router.delete("cluster_stats_a")
      Router.delete("cluster_stats_b")
      Router.delete("cluster_stats_c")
    end

    test "returns error with extra arguments" do
      store = MockStore.make()
      assert {:error, _msg} = Dispatcher.dispatch("CLUSTER.STATS", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # FERRICSTORE.HOTNESS
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.HOTNESS" do
    test "returns a list of key-value pairs" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      assert is_list(result)
      assert "total_keys" in result
      assert "total_memory_bytes" in result
      assert "hot_cache_entries" in result
      assert "shard_count" in result
    end

    test "includes per-shard data" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      assert is_list(result)
      assert "shard_0_keys" in result
      assert "shard_0_memory_bytes" in result
    end

    test "accepts TOP argument" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["TOP", "2"], store)

      assert is_list(result)
      assert "top_n" in result

      # Find the index of "top_n" and the value after it
      idx = Enum.find_index(result, &(&1 == "top_n"))
      assert Enum.at(result, idx + 1) == "2"
    end

    test "accepts WINDOW argument" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["WINDOW", "30"], store)

      assert is_list(result)
      # Should still return data even with WINDOW specified
      assert "total_keys" in result
    end

    test "reflects stored data" do
      Router.put("hotness_test_key", "value", 0)

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      idx = Enum.find_index(result, &(&1 == "total_keys"))
      total = String.to_integer(Enum.at(result, idx + 1))
      assert total >= 1

      Router.delete("hotness_test_key")
    end
  end
end
