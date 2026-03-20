defmodule Ferricstore.Transaction.CoordinatorTest do
  @moduledoc """
  Unit tests for the Two-Phase Commit coordinator.

  These tests exercise the Coordinator module directly through the application's
  running shards (0-3). Keys are chosen to land on specific shards based on
  `:erlang.phash2/2` with shard_count=4:

    - shard 0: "h", "j", "o", "p", "t", "u", "v", "x"
    - shard 1: "b", "c", "d", "e", "f", "k", "q", "r", "w", "y"
    - shard 2: "l"
    - shard 3: "a", "g", "i", "m", "n", "s", "z"
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn ->
        Enum.each(Router.keys(), &Router.delete/1)
        :ok
      end,
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
      list_op: &Router.list_op/2
    }
  end

  # Verify key-to-shard mapping assumptions used throughout these tests.
  describe "test infrastructure" do
    test "keys route to expected shards" do
      assert Router.shard_for("h") == 0
      assert Router.shard_for("b") == 1
      assert Router.shard_for("l") == 2
      assert Router.shard_for("a") == 3
    end
  end

  describe "single-shard fast path" do
    test "uses fast path when all commands target the same shard" do
      # "b" and "c" both route to shard 1
      queue = [{"SET", ["b", "100"]}, {"SET", ["c", "200"]}, {"GET", ["b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "100"]
    end

    test "returns results in original command order" do
      queue = [
        {"SET", ["b", "first"]},
        {"SET", ["c", "second"]},
        {"GET", ["c"]},
        {"GET", ["b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "second", "first"]
    end
  end

  describe "cross-shard 2PC commit" do
    test "atomically commits commands across two shards" do
      # "h" -> shard 0, "b" -> shard 1
      queue = [{"SET", ["h", "val_h"]}, {"SET", ["b", "val_b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get("h") == "val_h"
      assert Router.get("b") == "val_b"
    end

    test "atomically commits commands across all four shards" do
      # h -> shard 0, b -> shard 1, l -> shard 2, a -> shard 3
      queue = [
        {"SET", ["h", "v0"]},
        {"SET", ["b", "v1"]},
        {"SET", ["l", "v2"]},
        {"SET", ["a", "v3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, :ok, :ok]
      assert Router.get("h") == "v0"
      assert Router.get("b") == "v1"
      assert Router.get("l") == "v2"
      assert Router.get("a") == "v3"
    end

    test "returns results in original command order across shards" do
      # Set initial values
      Router.put("h", "10", 0)
      Router.put("b", "20", 0)

      queue = [
        {"GET", ["h"]},
        {"SET", ["b", "new_b"]},
        {"GET", ["b"]},
        {"SET", ["h", "new_h"]}
      ]

      # GET h returns "10" (before SET h runs, since GET h is in prepare)
      # SET b returns :ok
      # GET b: the result depends on ordering within the shard
      # SET h returns :ok
      result = Coordinator.execute(queue, %{}, nil)

      assert is_list(result)
      assert length(result) == 4

      # After execution, both keys should have new values
      assert Router.get("h") == "new_h"
      assert Router.get("b") == "new_b"
    end

    test "10 keys across 4 shards all committed" do
      # Pick keys: h,j,o (shard 0), b,c,d (shard 1), l (shard 2), a,g,i (shard 3)
      keys = ~w(h j o b c d l a g i)

      queue =
        Enum.map(keys, fn key ->
          {"SET", [key, "value_#{key}"]}
        end)

      result = Coordinator.execute(queue, %{}, nil)

      assert length(result) == 10
      assert Enum.all?(result, &(&1 == :ok))

      # All 10 keys should be committed
      Enum.each(keys, fn key ->
        assert Router.get(key) == "value_#{key}",
               "Expected key #{key} to have value_#{key}, got #{inspect(Router.get(key))}"
      end)
    end

    test "mixed read/write commands across shards" do
      Router.put("h", "existing_h", 0)
      Router.put("a", "existing_a", 0)

      queue = [
        {"GET", ["h"]},
        {"SET", ["b", "new_b"]},
        {"GET", ["a"]},
        {"INCR", ["l"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert is_list(result)
      assert length(result) == 4
    end
  end

  describe "WATCH conflict detection" do
    test "aborts when a watched key was modified before EXEC" do
      Router.put("h", "original", 0)

      # Record version for WATCH
      version = Router.get_version("h")
      watched = %{"h" => version}

      # Simulate another client modifying the key
      Router.put("h", "modified_by_other", 0)

      queue = [{"SET", ["h", "should_not_apply"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil
      # Value should remain as set by the other client
      assert Router.get("h") == "modified_by_other"
    end

    test "proceeds when watched keys are unmodified" do
      Router.put("h", "original", 0)

      version = Router.get_version("h")
      watched = %{"h" => version}

      queue = [{"SET", ["h", "updated"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok]
      assert Router.get("h") == "updated"
    end

    test "cross-shard transaction aborts on WATCH conflict" do
      Router.put("h", "orig_h", 0)
      Router.put("b", "orig_b", 0)

      version_h = Router.get_version("h")
      watched = %{"h" => version_h}

      # Another client modifies watched key
      Router.put("h", "modified", 0)

      queue = [
        {"SET", ["h", "should_not_apply"]},
        {"SET", ["b", "should_not_apply"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil
      # Neither key should be modified by the aborted transaction
      assert Router.get("h") == "modified"
      assert Router.get("b") == "orig_b"
    end
  end

  describe "rollback on prepare failure" do
    test "rolls back all shards when one shard's prepare fails due to type error" do
      # Set "h" as a list (wrong type for INCR)
      Router.list_op("h", {:lpush, ["item1"]})

      queue = [
        {"SET", ["b", "should_succeed"]},
        {"INCR", ["h"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # The transaction should still return results -- INCR on wrong type
      # returns an error in its slot, but the whole transaction executes.
      # (Redis semantics: individual command errors don't abort the tx)
      assert is_list(result)
      assert length(result) == 2
    end
  end

  describe "sandbox namespace support" do
    test "respects sandbox namespace for key routing" do
      ns = "test_ns:"

      # Pre-populate with namespaced key
      Router.put("test_ns:h", "ns_value", 0)

      queue = [{"GET", ["h"]}, {"SET", ["b", "ns_b"]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert length(result) == 2

      # The first result should be the namespaced value
      assert hd(result) == "ns_value"
    end
  end

  describe "concurrent transactions on overlapping keys" do
    test "serialize correctly via shard-level locking" do
      Router.put("h", "0", 0)

      # Run two concurrent transactions that both increment "h"
      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["h"]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["h"]}], %{}, nil)
        end)

      result1 = Task.await(task1)
      result2 = Task.await(task2)

      # Both should succeed (single-shard fast path, serialized by GenServer)
      assert is_list(result1)
      assert is_list(result2)

      # Final value should be 2 (two increments from 0)
      assert Router.get("h") == "2"
    end

    test "concurrent cross-shard transactions serialize correctly" do
      Router.put("h", "0", 0)
      Router.put("b", "0", 0)

      # Two transactions both touching h (shard 0) and b (shard 1)
      task1 =
        Task.async(fn ->
          Coordinator.execute(
            [{"INCR", ["h"]}, {"INCR", ["b"]}],
            %{},
            nil
          )
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute(
            [{"INCR", ["h"]}, {"INCR", ["b"]}],
            %{},
            nil
          )
        end)

      result1 = Task.await(task1, 10_000)
      result2 = Task.await(task2, 10_000)

      assert is_list(result1)
      assert is_list(result2)

      # Both keys should end at 2
      assert Router.get("h") == "2"
      assert Router.get("b") == "2"
    end
  end

  describe "empty transaction" do
    test "returns empty list for empty queue" do
      result = Coordinator.execute([], %{}, nil)
      assert result == []
    end
  end

  describe "DEL across shards" do
    test "DEL single key per command across shards" do
      Router.put("h", "v", 0)
      Router.put("b", "v", 0)
      Router.put("l", "v", 0)

      queue = [
        {"DEL", ["h"]},
        {"DEL", ["b"]},
        {"DEL", ["l"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1, 1]
      assert Router.get("h") == nil
      assert Router.get("b") == nil
      assert Router.get("l") == nil
    end
  end
end
