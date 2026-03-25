defmodule Ferricstore.Transaction.CoordinatorTest do
  @moduledoc """
  Unit tests for the Transaction Coordinator.

  Single-shard transactions dispatch atomically via a single GenServer call.
  Cross-shard transactions execute atomically via anchor-shard Raft entry.

  Key-to-shard mapping (slot-based, 4 shards, contiguous 256-slot ranges):
    - shard 0: "j", "t", "u", "y", "z"
    - shard 1: "g", "k", "l", "r"
    - shard 2: "a", "c", "e", "n", "p", "v", "w"
    - shard 3: "b", "d", "f", "h", "i", "m", "o", "q", "s", "x"
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
      assert Router.shard_for("j") == 0
      assert Router.shard_for("g") == 1
      assert Router.shard_for("a") == 2
      assert Router.shard_for("b") == 3
    end
  end

  describe "single-shard fast path" do
    test "uses fast path when all commands target the same shard" do
      # "g" and "k" both route to shard 1
      queue = [{"SET", ["g", "100"]}, {"SET", ["k", "200"]}, {"GET", ["g"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "100"]
    end

    test "returns results in original command order" do
      queue = [
        {"SET", ["g", "first"]},
        {"SET", ["k", "second"]},
        {"GET", ["k"]},
        {"GET", ["g"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "second", "first"]
    end

    test "INCR works atomically within single shard" do
      Router.put("g", "10", 0)

      queue = [{"INCR", ["g"]}, {"INCR", ["g"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 12}]
      assert Router.get("g") == "12"
    end

    test "DEL within single shard" do
      Router.put("g", "v", 0)
      Router.put("k", "v", 0)

      queue = [{"DEL", ["g"]}, {"DEL", ["k"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1]
      assert Router.get("g") == nil
      assert Router.get("k") == nil
    end

    test "mixed GET and SET on same shard" do
      Router.put("g", "existing", 0)

      queue = [
        {"GET", ["g"]},
        {"SET", ["g", "updated"]},
        {"GET", ["g"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing", :ok, "updated"]
    end
  end

  describe "cross-shard succeeds atomically" do
    test "two shards succeeds" do
      # "j" -> shard 0, "b" -> shard 3
      queue = [{"SET", ["j", "val_j"]}, {"SET", ["b", "val_b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get("j") == "val_j"
      assert Router.get("b") == "val_b"
    end

    test "four shards succeeds" do
      queue = [
        {"SET", ["j", "v0"]},
        {"SET", ["g", "v1"]},
        {"SET", ["a", "v2"]},
        {"SET", ["b", "v3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, :ok, :ok]
      assert Router.get("j") == "v0"
      assert Router.get("g") == "v1"
      assert Router.get("a") == "v2"
      assert Router.get("b") == "v3"
    end

    test "mixed read/write across shards succeeds" do
      Router.put("j", "existing_j", 0)

      queue = [
        {"GET", ["j"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing_j", :ok]
      assert Router.get("b") == "new_b"
    end

    test "INCR across shards succeeds" do
      Router.put("j", "10", 0)
      Router.put("b", "20", 0)

      queue = [{"INCR", ["j"]}, {"INCR", ["b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 21}]
      assert Router.get("j") == "11"
      assert Router.get("b") == "21"
    end

    test "DEL across shards succeeds" do
      Router.put("j", "v", 0)
      Router.put("g", "v", 0)
      Router.put("a", "v", 0)

      queue = [
        {"DEL", ["j"]},
        {"DEL", ["g"]},
        {"DEL", ["a"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1, 1]
      assert Router.get("j") == nil
      assert Router.get("g") == nil
      assert Router.get("a") == nil
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
      Router.put("g", "original", 0)

      version = Router.get_version("g")
      watched = %{"g" => version}

      # Simulate another client modifying the key
      Router.put("g", "modified_by_other", 0)

      queue = [{"SET", ["g", "should_not_apply"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil
      assert Router.get("g") == "modified_by_other"
    end

    test "proceeds when watched keys are unmodified" do
      Router.put("g", "original", 0)

      version = Router.get_version("g")
      watched = %{"g" => version}

      queue = [{"SET", ["g", "updated"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok]
      assert Router.get("g") == "updated"
    end

    test "cross-shard WATCH succeeds when watches pass" do
      Router.put("j", "orig_j", 0)

      version_j = Router.get_version("j")
      watched = %{"j" => version_j}

      queue = [
        {"SET", ["j", "new_j"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok, :ok]
      assert Router.get("j") == "new_j"
      assert Router.get("b") == "new_b"
    end

    test "cross-shard WATCH conflict returns nil" do
      Router.put("j", "orig_j", 0)

      version_j = Router.get_version("j")
      watched = %{"j" => version_j}

      # Modify watched key
      Router.put("j", "changed", 0)

      queue = [
        {"SET", ["j", "new_j"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      # WATCH fails first -> nil (before classify even runs)
      assert result == nil
    end
  end

  describe "concurrent single-shard transactions" do
    test "serialize correctly via GenServer" do
      Router.put("g", "0", 0)

      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["g"]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["g"]}], %{}, nil)
        end)

      result1 = Task.await(task1)
      result2 = Task.await(task2)

      assert is_list(result1)
      assert is_list(result2)

      assert Router.get("g") == "2"
    end
  end

  describe "sandbox namespace support" do
    test "respects sandbox namespace for key routing" do
      ns = "test_ns:"

      Router.put("test_ns:g", "ns_value", 0)

      # Single key avoids cross-shard issues with namespace prefix
      queue = [{"GET", ["g"]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert length(result) == 1
      assert hd(result) == "ns_value"
    end

    test "sandbox namespace SET and GET on same key" do
      ns = "test_ns:"

      queue = [{"SET", ["g", "ns_g"]}, {"GET", ["g"]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert result == [:ok, "ns_g"]
    end
  end

  describe "empty transaction" do
    test "returns empty list for empty queue" do
      result = Coordinator.execute([], %{}, nil)
      assert result == []
    end
  end
end
