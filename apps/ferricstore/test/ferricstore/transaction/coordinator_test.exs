defmodule Ferricstore.Transaction.CoordinatorTest do
  @moduledoc """
  Unit tests for the Transaction Coordinator.

  Single-shard transactions dispatch atomically via a single GenServer call.
  Cross-shard transactions execute atomically via anchor-shard Raft entry.

  Key-to-shard mapping (phash2, 4 shards):
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

    test "INCR works atomically within single shard" do
      Router.put("b", "10", 0)

      queue = [{"INCR", ["b"]}, {"INCR", ["b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 12}]
      assert Router.get("b") == "12"
    end

    test "DEL within single shard" do
      Router.put("b", "v", 0)
      Router.put("c", "v", 0)

      queue = [{"DEL", ["b"]}, {"DEL", ["c"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1]
      assert Router.get("b") == nil
      assert Router.get("c") == nil
    end

    test "mixed GET and SET on same shard" do
      Router.put("b", "existing", 0)

      queue = [
        {"GET", ["b"]},
        {"SET", ["b", "updated"]},
        {"GET", ["b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing", :ok, "updated"]
    end
  end

  describe "cross-shard succeeds atomically" do
    test "two shards succeeds" do
      # "h" -> shard 0, "b" -> shard 1
      queue = [{"SET", ["h", "val_h"]}, {"SET", ["b", "val_b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get("h") == "val_h"
      assert Router.get("b") == "val_b"
    end

    test "four shards succeeds" do
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

    test "mixed read/write across shards succeeds" do
      Router.put("h", "existing_h", 0)

      queue = [
        {"GET", ["h"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing_h", :ok]
      assert Router.get("b") == "new_b"
    end

    test "INCR across shards succeeds" do
      Router.put("h", "10", 0)
      Router.put("b", "20", 0)

      queue = [{"INCR", ["h"]}, {"INCR", ["b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 21}]
      assert Router.get("h") == "11"
      assert Router.get("b") == "21"
    end

    test "DEL across shards succeeds" do
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

  describe "hash tags co-locate keys" do
    test "keys with same hash tag route to same shard" do
      queue = [
        {"SET", ["{user:42}:name", "Alice"]},
        {"SET", ["{user:42}:email", "alice@example.com"]},
        {"GET", ["{user:42}:name"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "Alice"]
    end
  end

  describe "WATCH conflict detection" do
    test "aborts when a watched key was modified before EXEC" do
      Router.put("b", "original", 0)

      version = Router.get_version("b")
      watched = %{"b" => version}

      # Simulate another client modifying the key
      Router.put("b", "modified_by_other", 0)

      queue = [{"SET", ["b", "should_not_apply"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil
      assert Router.get("b") == "modified_by_other"
    end

    test "proceeds when watched keys are unmodified" do
      Router.put("b", "original", 0)

      version = Router.get_version("b")
      watched = %{"b" => version}

      queue = [{"SET", ["b", "updated"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok]
      assert Router.get("b") == "updated"
    end

    test "cross-shard WATCH succeeds when watches pass" do
      Router.put("h", "orig_h", 0)

      version_h = Router.get_version("h")
      watched = %{"h" => version_h}

      queue = [
        {"SET", ["h", "new_h"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok, :ok]
      assert Router.get("h") == "new_h"
      assert Router.get("b") == "new_b"
    end

    test "cross-shard WATCH conflict returns nil" do
      Router.put("h", "orig_h", 0)

      version_h = Router.get_version("h")
      watched = %{"h" => version_h}

      # Modify watched key
      Router.put("h", "changed", 0)

      queue = [
        {"SET", ["h", "new_h"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      # WATCH fails first -> nil (before classify even runs)
      assert result == nil
    end
  end

  describe "concurrent single-shard transactions" do
    test "serialize correctly via GenServer" do
      Router.put("b", "0", 0)

      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["b"]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["b"]}], %{}, nil)
        end)

      result1 = Task.await(task1)
      result2 = Task.await(task2)

      assert is_list(result1)
      assert is_list(result2)

      assert Router.get("b") == "2"
    end
  end

  describe "sandbox namespace support" do
    test "respects sandbox namespace for key routing" do
      ns = "test_ns:"

      Router.put("test_ns:b", "ns_value", 0)

      # Single key avoids cross-shard issues with namespace prefix
      queue = [{"GET", ["b"]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert length(result) == 1
      assert hd(result) == "ns_value"
    end

    test "sandbox namespace SET and GET on same key" do
      ns = "test_ns:"

      queue = [{"SET", ["b", "ns_b"]}, {"GET", ["b"]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert result == [:ok, "ns_b"]
    end
  end

  describe "empty transaction" do
    test "returns empty list for empty queue" do
      result = Coordinator.execute([], %{}, nil)
      assert result == []
    end
  end
end
