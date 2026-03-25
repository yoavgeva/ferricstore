defmodule Ferricstore.Transaction.CrossShardAtomicTest do
  @moduledoc """
  TDD tests for cross-shard atomic MULTI/EXEC transactions.

  These tests verify the NEW behavior: cross-shard transactions should succeed
  atomically (not return CROSSSLOT). The approach: submit a single Raft log
  entry to an "anchor shard" containing commands for ALL involved shards.
  One Raft entry = one fsync = atomic commit or no commit.

  These tests are expected to FAIL until the cross-shard atomic transaction
  implementation is complete.

  Key-to-shard mapping (slot-based, 4 shards, contiguous 256-slot ranges):
    - shard 0: "j", "t", "u", "y", "z"
    - shard 1: "g", "k", "l", "r"
    - shard 2: "a", "c", "e", "n", "p", "v", "w"
    - shard 3: "b", "d", "f", "h", "i", "m", "o", "q", "s", "x"

  Sandbox namespace "test_ns:" key-to-shard mapping:
    - shard 0: "d", "e", "g", "h", "j", "l", "n", "q", "y", "z"
    - shard 1: "b", "i", "k", "o", "s", "u", "v", "w", "x"
    - shard 2: "f", "m", "r"
    - shard 3: "a", "c", "p", "t"
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Test infrastructure: verify key-to-shard assumptions
  # ---------------------------------------------------------------------------

  describe "test infrastructure" do
    test "keys route to expected shards" do
      assert Router.shard_for("j") == 0
      assert Router.shard_for("g") == 1
      assert Router.shard_for("a") == 2
      assert Router.shard_for("b") == 3
    end

    test "cross-shard key pairs are on different shards" do
      # j -> shard 0, g -> shard 1
      assert Router.shard_for("j") != Router.shard_for("g")
      # j -> shard 0, a -> shard 2
      assert Router.shard_for("j") != Router.shard_for("a")
      # j -> shard 0, b -> shard 3
      assert Router.shard_for("j") != Router.shard_for("b")
    end
  end

  # ---------------------------------------------------------------------------
  # Basic cross-shard atomicity
  # ---------------------------------------------------------------------------

  describe "basic cross-shard atomicity" do
    test "SET keys on 2 different shards — both written" do
      # j -> shard 0, b -> shard 3
      queue = [{"SET", ["j", "val_j"]}, {"SET", ["b", "val_b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get("j") == "val_j"
      assert Router.get("b") == "val_b"
    end

    test "SET keys across all 4 shards — all written" do
      # j -> shard 0, g -> shard 1, a -> shard 2, b -> shard 3
      queue = [
        {"SET", ["j", "s0"]},
        {"SET", ["g", "s1"]},
        {"SET", ["a", "s2"]},
        {"SET", ["b", "s3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, :ok, :ok]
      assert Router.get("j") == "s0"
      assert Router.get("g") == "s1"
      assert Router.get("a") == "s2"
      assert Router.get("b") == "s3"
    end

    test "mixed GET and SET across shards — results in correct order" do
      Router.put("j", "existing_j", 0)
      Router.put("a", "existing_a", 0)

      queue = [
        {"GET", ["j"]},
        {"SET", ["g", "new_g"]},
        {"GET", ["a"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing_j", :ok, "existing_a", :ok]
      assert Router.get("g") == "new_g"
      assert Router.get("b") == "new_b"
    end

    test "INCR on keys across different shards — both incremented" do
      Router.put("j", "10", 0)
      Router.put("b", "20", 0)

      queue = [{"INCR", ["j"]}, {"INCR", ["b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 21}]
      assert Router.get("j") == "11"
      assert Router.get("b") == "21"
    end

    test "DEL across shards — all deleted" do
      Router.put("j", "v0", 0)
      Router.put("g", "v1", 0)
      Router.put("a", "v2", 0)

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

    test "GET before SET within same transaction across shards (read-before-write ordering)" do
      # GET a key that doesn't exist yet, then SET it in the same tx
      # The GET should see nil (pre-transaction state), then SET creates it
      queue = [
        {"GET", ["j"]},
        {"SET", ["j", "created"]},
        {"GET", ["b"]},
        {"SET", ["b", "also_created"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # Within a Redis transaction, commands see state as it evolves during execution.
      # GET before SET returns nil, GET after SET would return the new value.
      # But since this is a flat queue (not interleaved reads), the first GET sees
      # pre-tx state if commands are grouped per-shard, or evolving state if sequential.
      # Redis MULTI/EXEC semantics: commands execute sequentially, each sees prior effects.
      assert result == [nil, :ok, nil, :ok]
      assert Router.get("j") == "created"
      assert Router.get("b") == "also_created"
    end
  end

  # ---------------------------------------------------------------------------
  # Atomicity verification (the key tests)
  # ---------------------------------------------------------------------------

  describe "atomicity verification" do
    test "all writes from a cross-shard tx are visible AFTER execute returns" do
      queue = [
        {"SET", ["j", "atomic_j"]},
        {"SET", ["g", "atomic_g"]},
        {"SET", ["a", "atomic_a"]},
        {"SET", ["b", "atomic_b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # All must succeed
      assert result == [:ok, :ok, :ok, :ok]

      # All values must be readable immediately after execute returns
      assert Router.get("j") == "atomic_j"
      assert Router.get("g") == "atomic_g"
      assert Router.get("a") == "atomic_a"
      assert Router.get("b") == "atomic_b"
    end

    test "concurrent readers eventually see all-new values after execute returns" do
      # Pre-set values on two different shards
      Router.put("j", "old_j", 0)
      Router.put("b", "old_b", 0)

      # Execute cross-shard transaction
      queue = [{"SET", ["j", "new_j"]}, {"SET", ["b", "new_b"]}]
      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]

      # After execute returns, ALL writes must be visible (durability guarantee)
      assert Router.get("j") == "new_j"
      assert Router.get("b") == "new_b"
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH integration
  # ---------------------------------------------------------------------------

  describe "WATCH integration with cross-shard tx" do
    test "WATCH key on shard 0, modify from outside, cross-shard EXEC aborts — NO writes on ANY shard" do
      Router.put("j", "original_j", 0)
      Router.put("b", "original_b", 0)

      # Capture version of watched key on shard 0
      version_j = Router.get_version("j")
      watched = %{"j" => version_j}

      # Simulate another client modifying the watched key
      Router.put("j", "modified_by_other", 0)

      # Cross-shard transaction: j (shard 0) + b (shard 3)
      queue = [
        {"SET", ["j", "from_tx_j"]},
        {"SET", ["b", "from_tx_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      # WATCH conflict -> nil (transaction aborted)
      assert result == nil

      # Neither shard should have been written to
      assert Router.get("j") == "modified_by_other"
      assert Router.get("b") == "original_b"
    end

    test "WATCH key, no modification, cross-shard EXEC succeeds" do
      Router.put("j", "original_j", 0)

      version_j = Router.get_version("j")
      watched = %{"j" => version_j}

      # Cross-shard transaction with unmodified watched key
      queue = [
        {"SET", ["j", "new_j"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok, :ok]
      assert Router.get("j") == "new_j"
      assert Router.get("b") == "new_b"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "empty transaction returns empty list" do
      result = Coordinator.execute([], %{}, nil)
      assert result == []
    end

    test "cross-shard tx with command error (INCR on non-integer) — error in results, other commands still execute" do
      # Set a non-integer value on shard 0
      Router.put("j", "not_a_number", 0)

      # Cross-shard tx: INCR non-integer on shard 0, SET on shard 3
      queue = [
        {"INCR", ["j"]},
        {"SET", ["b", "val_b"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # Redis semantics: individual command errors don't abort the tx.
      # INCR returns an error, SET succeeds.
      assert is_list(result)
      assert length(result) == 2

      # First result: INCR error
      assert {:error, _msg} = Enum.at(result, 0)

      # Second result: SET success
      assert Enum.at(result, 1) == :ok

      # SET on shard 3 should have been applied despite INCR error on shard 0
      assert Router.get("b") == "val_b"
    end

    test "large cross-shard transaction (50 commands across 4 shards)" do
      # Build 50 SET commands distributed across shards
      # shard 0 keys: j, t, u, y, z
      # shard 1 keys: g, k, l, r
      # shard 2 keys: a, c, e, n, p, v, w
      # shard 3 keys: b, d, f, h, i, m, o, q, s, x
      keys_by_shard = %{
        0 => ~w(j t u y z),
        1 => ~w(g k l r),
        2 => ~w(a c e n p v w),
        3 => ~w(b d f h i m o q s x)
      }

      # Take enough keys from each shard to total 50 commands.
      # We'll use all unique keys (26 letters) plus duplicate operations.
      all_keys = Enum.flat_map(keys_by_shard, fn {_shard, keys} -> keys end)

      # 26 unique letters + 24 more from repeating (with unique values)
      extended_keys = all_keys ++ Enum.take(all_keys, 24)
      assert length(extended_keys) == 50

      queue =
        extended_keys
        |> Enum.with_index()
        |> Enum.map(fn {key, idx} -> {"SET", [key, "val_#{idx}"]} end)

      result = Coordinator.execute(queue, %{}, nil)

      assert is_list(result)
      assert length(result) == 50
      assert Enum.all?(result, &(&1 == :ok))

      # Verify a sample from each shard is written
      assert Router.get("j") != nil
      assert Router.get("g") != nil
      assert Router.get("a") != nil
      assert Router.get("b") != nil
    end

    test "cross-shard tx with PING (keyless command) mixed with keyed commands" do
      queue = [
        {"PING", []},
        {"SET", ["j", "val_j"]},
        {"SET", ["b", "val_b"]},
        {"PING", []}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert is_list(result)
      assert length(result) == 4

      # PING returns {:simple, "PONG"}, SET returns :ok
      assert Enum.at(result, 0) == {:simple, "PONG"}
      assert Enum.at(result, 1) == :ok
      assert Enum.at(result, 2) == :ok
      assert Enum.at(result, 3) == {:simple, "PONG"}

      assert Router.get("j") == "val_j"
      assert Router.get("b") == "val_b"
    end

    test "sandbox namespace with cross-shard keys" do
      ns = "test_ns:"

      # With namespace "test_ns:", keys route differently:
      # "d" -> test_ns:d -> shard 0
      # "a" -> test_ns:a -> shard 3
      # These are on different shards under the namespace
      assert Router.shard_for(ns <> "d") == 0
      assert Router.shard_for(ns <> "a") == 3

      queue = [
        {"SET", ["d", "ns_val_d"]},
        {"SET", ["a", "ns_val_a"]}
      ]

      result = Coordinator.execute(queue, %{}, ns)

      assert result == [:ok, :ok]

      # The actual stored keys include the namespace prefix
      assert Router.get(ns <> "d") == "ns_val_d"
      assert Router.get(ns <> "a") == "ns_val_a"
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent transactions
  # ---------------------------------------------------------------------------

  describe "concurrent cross-shard transactions" do
    test "two concurrent cross-shard transactions on overlapping keys — both complete, consistent state" do
      # Pre-set values
      Router.put("j", "0", 0)
      Router.put("b", "0", 0)

      # Two concurrent transactions both INCR the same keys across shards
      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["j"]}, {"INCR", ["b"]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["j"]}, {"INCR", ["b"]}], %{}, nil)
        end)

      result1 = Task.await(task1, 10_000)
      result2 = Task.await(task2, 10_000)

      # Both transactions should complete (not error)
      assert is_list(result1)
      assert is_list(result2)

      # Final values should reflect both increments
      assert Router.get("j") == "2"
      assert Router.get("b") == "2"
    end
  end
end
