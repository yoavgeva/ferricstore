defmodule Ferricstore.Transaction.CrossShardAtomicTest do
  @moduledoc """
  TDD tests for cross-shard atomic MULTI/EXEC transactions.

  These tests verify the NEW behavior: cross-shard transactions should succeed
  atomically (not return CROSSSLOT). The approach: submit a single Raft log
  entry to an "anchor shard" containing commands for ALL involved shards.
  One Raft entry = one fsync = atomic commit or no commit.

  These tests are expected to FAIL until the cross-shard atomic transaction
  implementation is complete.

  Key-to-shard mapping (phash2, 4 shards):
    - shard 0: "h", "j", "o", "p", "t", "u", "v", "x"
    - shard 1: "b", "c", "d", "e", "f", "k", "q", "r", "w", "y"
    - shard 2: "l"
    - shard 3: "a", "g", "i", "m", "n", "s", "z"

  Sandbox namespace "test_ns:" key-to-shard mapping:
    - shard 0: "d", "l", "r", "u", "x"
    - shard 1: "c", "f", "j", "k", "q", "y", "z"
    - shard 2: "a", "e", "m", "n", "o", "s"
    - shard 3: "b", "g", "h", "i", "p", "t", "v", "w"
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
      assert Router.shard_for("h") == 0
      assert Router.shard_for("b") == 1
      assert Router.shard_for("l") == 2
      assert Router.shard_for("a") == 3
    end

    test "cross-shard key pairs are on different shards" do
      # h -> shard 0, b -> shard 1
      assert Router.shard_for("h") != Router.shard_for("b")
      # h -> shard 0, l -> shard 2
      assert Router.shard_for("h") != Router.shard_for("l")
      # h -> shard 0, a -> shard 3
      assert Router.shard_for("h") != Router.shard_for("a")
    end
  end

  # ---------------------------------------------------------------------------
  # Basic cross-shard atomicity
  # ---------------------------------------------------------------------------

  describe "basic cross-shard atomicity" do
    test "SET keys on 2 different shards — both written" do
      # h -> shard 0, b -> shard 1
      queue = [{"SET", ["h", "val_h"]}, {"SET", ["b", "val_b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get("h") == "val_h"
      assert Router.get("b") == "val_b"
    end

    test "SET keys across all 4 shards — all written" do
      # h -> shard 0, b -> shard 1, l -> shard 2, a -> shard 3
      queue = [
        {"SET", ["h", "s0"]},
        {"SET", ["b", "s1"]},
        {"SET", ["l", "s2"]},
        {"SET", ["a", "s3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, :ok, :ok]
      assert Router.get("h") == "s0"
      assert Router.get("b") == "s1"
      assert Router.get("l") == "s2"
      assert Router.get("a") == "s3"
    end

    test "mixed GET and SET across shards — results in correct order" do
      Router.put("h", "existing_h", 0)
      Router.put("a", "existing_a", 0)

      queue = [
        {"GET", ["h"]},
        {"SET", ["b", "new_b"]},
        {"GET", ["a"]},
        {"SET", ["l", "new_l"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing_h", :ok, "existing_a", :ok]
      assert Router.get("b") == "new_b"
      assert Router.get("l") == "new_l"
    end

    test "INCR on keys across different shards — both incremented" do
      Router.put("h", "10", 0)
      Router.put("b", "20", 0)

      queue = [{"INCR", ["h"]}, {"INCR", ["b"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 21}]
      assert Router.get("h") == "11"
      assert Router.get("b") == "21"
    end

    test "DEL across shards — all deleted" do
      Router.put("h", "v0", 0)
      Router.put("b", "v1", 0)
      Router.put("l", "v2", 0)

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

    test "GET before SET within same transaction across shards (read-before-write ordering)" do
      # GET a key that doesn't exist yet, then SET it in the same tx
      # The GET should see nil (pre-transaction state), then SET creates it
      queue = [
        {"GET", ["h"]},
        {"SET", ["h", "created"]},
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
      assert Router.get("h") == "created"
      assert Router.get("b") == "also_created"
    end
  end

  # ---------------------------------------------------------------------------
  # Atomicity verification (the key tests)
  # ---------------------------------------------------------------------------

  describe "atomicity verification" do
    test "all writes from a cross-shard tx are visible AFTER execute returns" do
      queue = [
        {"SET", ["h", "atomic_h"]},
        {"SET", ["b", "atomic_b"]},
        {"SET", ["l", "atomic_l"]},
        {"SET", ["a", "atomic_a"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # All must succeed
      assert result == [:ok, :ok, :ok, :ok]

      # All values must be readable immediately after execute returns
      assert Router.get("h") == "atomic_h"
      assert Router.get("b") == "atomic_b"
      assert Router.get("l") == "atomic_l"
      assert Router.get("a") == "atomic_a"
    end

    test "concurrent readers eventually see all-new values after execute returns" do
      # Pre-set values on two different shards
      Router.put("h", "old_h", 0)
      Router.put("b", "old_b", 0)

      # Execute cross-shard transaction
      queue = [{"SET", ["h", "new_h"]}, {"SET", ["b", "new_b"]}]
      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]

      # After execute returns, ALL writes must be visible (durability guarantee)
      assert Router.get("h") == "new_h"
      assert Router.get("b") == "new_b"
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH integration
  # ---------------------------------------------------------------------------

  describe "WATCH integration with cross-shard tx" do
    test "WATCH key on shard 0, modify from outside, cross-shard EXEC aborts — NO writes on ANY shard" do
      Router.put("h", "original_h", 0)
      Router.put("b", "original_b", 0)

      # Capture version of watched key on shard 0
      version_h = Router.get_version("h")
      watched = %{"h" => version_h}

      # Simulate another client modifying the watched key
      Router.put("h", "modified_by_other", 0)

      # Cross-shard transaction: h (shard 0) + b (shard 1)
      queue = [
        {"SET", ["h", "from_tx_h"]},
        {"SET", ["b", "from_tx_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      # WATCH conflict -> nil (transaction aborted)
      assert result == nil

      # Neither shard should have been written to
      assert Router.get("h") == "modified_by_other"
      assert Router.get("b") == "original_b"
    end

    test "WATCH key, no modification, cross-shard EXEC succeeds" do
      Router.put("h", "original_h", 0)

      version_h = Router.get_version("h")
      watched = %{"h" => version_h}

      # Cross-shard transaction with unmodified watched key
      queue = [
        {"SET", ["h", "new_h"]},
        {"SET", ["b", "new_b"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok, :ok]
      assert Router.get("h") == "new_h"
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
      Router.put("h", "not_a_number", 0)

      # Cross-shard tx: INCR non-integer on shard 0, SET on shard 1
      queue = [
        {"INCR", ["h"]},
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

      # SET on shard 1 should have been applied despite INCR error on shard 0
      assert Router.get("b") == "val_b"
    end

    test "large cross-shard transaction (50 commands across 4 shards)" do
      # Build 50 SET commands distributed across shards
      # shard 0 keys: h, j, o, p, t, u, v, x
      # shard 1 keys: b, c, d, e, f, k, q, r, w, y
      # shard 2 keys: l
      # shard 3 keys: a, g, i, m, n, s, z
      keys_by_shard = %{
        0 => ~w(h j o p t u v x),
        1 => ~w(b c d e f k q r w y),
        2 => ~w(l),
        3 => ~w(a g i m n s z)
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
      assert Router.get("h") != nil
      assert Router.get("b") != nil
      assert Router.get("l") != nil
      assert Router.get("a") != nil
    end

    test "cross-shard tx with PING (keyless command) mixed with keyed commands" do
      queue = [
        {"PING", []},
        {"SET", ["h", "val_h"]},
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

      assert Router.get("h") == "val_h"
      assert Router.get("b") == "val_b"
    end

    test "sandbox namespace with cross-shard keys" do
      ns = "test_ns:"

      # With namespace "test_ns:", keys route differently:
      # "d" -> test_ns:d -> shard 0
      # "c" -> test_ns:c -> shard 1
      # These are on different shards under the namespace
      assert Router.shard_for(ns <> "d") == 0
      assert Router.shard_for(ns <> "c") == 1

      queue = [
        {"SET", ["d", "ns_val_d"]},
        {"SET", ["c", "ns_val_c"]}
      ]

      result = Coordinator.execute(queue, %{}, ns)

      assert result == [:ok, :ok]

      # The actual stored keys include the namespace prefix
      assert Router.get(ns <> "d") == "ns_val_d"
      assert Router.get(ns <> "c") == "ns_val_c"
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent transactions
  # ---------------------------------------------------------------------------

  describe "concurrent cross-shard transactions" do
    test "two concurrent cross-shard transactions on overlapping keys — both complete, consistent state" do
      # Pre-set values
      Router.put("h", "0", 0)
      Router.put("b", "0", 0)

      # Two concurrent transactions both INCR the same keys across shards
      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["h"]}, {"INCR", ["b"]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", ["h"]}, {"INCR", ["b"]}], %{}, nil)
        end)

      result1 = Task.await(task1, 10_000)
      result2 = Task.await(task2, 10_000)

      # Both transactions should complete (not error)
      assert is_list(result1)
      assert is_list(result2)

      # Final values should reflect both increments
      assert Router.get("h") == "2"
      assert Router.get("b") == "2"
    end
  end
end
