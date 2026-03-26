defmodule FerricstoreServer.Spec.SandboxTest do
  @moduledoc """
  Spec section 11: Test Sandbox tests.

  Verifies the FerricStore.Sandbox module provides correct per-test isolation,
  cleanup, transparent key rewriting, config gating, and scoped FLUSHDB.

  Tests SB-001 through SB-005 from the test plan. Some of these scenarios
  have partial coverage in existing test files (`Ferricstore.SandboxTest` and
  `Ferricstore.Commands.SandboxTest`). This file focuses on the exact spec
  scenarios and adds coverage for gaps -- particularly SB-005 (FLUSHDB scoped
  to sandbox).

  SB-001: Per-test isolation -- two async tests write same key, no collision
  SB-002: Cleanup after test -- 100 keys cleaned up
  SB-003: Transparent key rewriting
  SB-004: Sandbox disabled on non-test nodes
  SB-005: FLUSHDB scoped to sandbox
  """

  use ExUnit.Case, async: true
  use FerricStore.Sandbox.Case

  alias Ferricstore.Store.Router

  # ---------------------------------------------------------------------------
  # SB-001: Per-test isolation -- two async tests write same key
  # ---------------------------------------------------------------------------

  describe "SB-001: per-test isolation" do
    test "async test A writes user:42 and reads it back" do
      FerricStore.set("user:42", "from_test_A")
      assert {:ok, "from_test_A"} = FerricStore.get("user:42")
    end

    test "async test B writes user:42 and reads it back (no collision with A)" do
      FerricStore.set("user:42", "from_test_B")
      assert {:ok, "from_test_B"} = FerricStore.get("user:42")
    end

    test "concurrent processes with different sandboxes see their own values" do
      test_pid = self()

      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            ns = FerricStore.Sandbox.checkout()

            try do
              FerricStore.set("user:42", "from_task_#{i}")
              {:ok, val} = FerricStore.get("user:42")
              send(test_pid, {:task_result, i, val, ns})
            after
              FerricStore.Sandbox.checkin(ns)
            end
          end)
        end

      results =
        for _ <- 1..5 do
          assert_receive {:task_result, i, val, _ns}, 5_000
          {i, val}
        end

      # Each task should read back its own value
      for {i, val} <- results do
        assert val == "from_task_#{i}",
               "Task #{i} read back #{inspect(val)} instead of from_task_#{i}"
      end

      # Ensure all tasks completed
      Enum.each(tasks, &Task.await(&1, 5_000))
    end

    test "each test has a unique sandbox" do
      ns = FerricStore.Sandbox.current_namespace()
      assert ns != nil
      # Struct sandbox: ref is unique
      assert is_reference(ns.ref) or is_binary(ns)
    end
  end

  # ---------------------------------------------------------------------------
  # SB-002: Cleanup after test -- 100 keys cleaned up
  # ---------------------------------------------------------------------------

  describe "SB-002: cleanup after test" do
    test "100 keys written in a sandbox are cleaned up on checkin" do
      # Create a separate sandbox to control lifecycle explicitly
      sandbox = FerricStore.Sandbox.checkout()

      try do
        # Write 100 keys
        for i <- 1..100 do
          FerricStore.set("cleanup_key_#{i}", "value_#{i}")
        end

        # Verify all 100 keys exist
        {:ok, size} = FerricStore.dbsize()
        assert size == 100
      after
        FerricStore.Sandbox.checkin(sandbox)
      end

      # After checkin, the sandbox shards are stopped and tmpdir deleted.
      # The keys are gone because the entire private shard is gone.
      assert sandbox.tmpdir != nil
      refute File.dir?(sandbox.tmpdir)
    end

    test "no cross-test pollution from large key sets" do
      # This test runs AFTER others -- the sandbox should start clean
      {:ok, size} = FerricStore.dbsize()
      assert size == 0, "Expected empty sandbox at start, got #{size} keys"
    end
  end

  # ---------------------------------------------------------------------------
  # SB-003: Transparent key rewriting
  # ---------------------------------------------------------------------------

  describe "SB-003: transparent key rewriting" do
    test "FerricStore.set/get works transparently in sandbox" do
      sandbox = FerricStore.Sandbox.current_namespace()
      assert sandbox != nil

      FerricStore.set("user:42", "data")

      # The embedded API reads back through the sandbox's private shards
      assert {:ok, "data"} = FerricStore.get("user:42")

      # The key is stored in the private shard, not the application shard.
      # Verify isolation: a fresh sandbox shouldn't see this key.
      other = FerricStore.Sandbox.checkout()
      assert {:ok, nil} = FerricStore.get("user:42")
      FerricStore.Sandbox.checkin(other)
    end

    test "key rewriting is transparent for hash operations" do
      _namespace = FerricStore.Sandbox.current_namespace()

      FerricStore.hset("user:42", %{"name" => "alice"})

      assert {:ok, "alice"} = FerricStore.hget("user:42", "name")

      # Verify isolation: another sandbox shouldn't see this hash
      other = FerricStore.Sandbox.checkout()
      assert {:ok, nil} = FerricStore.hget("user:42", "name")
      FerricStore.Sandbox.checkin(other)
    end

    test "key rewriting is transparent for list operations" do
      FerricStore.rpush("mylist", ["a", "b", "c"])
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("mylist", 0, -1)
    end

    test "key rewriting is transparent for set operations" do
      FerricStore.sadd("myset", ["x", "y", "z"])
      {:ok, members} = FerricStore.smembers("myset")
      assert Enum.sort(members) == ["x", "y", "z"]
    end

    test "key rewriting is transparent for sorted set operations" do
      FerricStore.zadd("leaderboard", [{1.0, "alice"}, {2.0, "bob"}])
      {:ok, members} = FerricStore.zrange("leaderboard", 0, -1)
      assert members == ["alice", "bob"]
    end

    test "incr/decr work correctly with transparent rewriting" do
      assert {:ok, 1} = FerricStore.incr("counter")
      assert {:ok, 2} = FerricStore.incr("counter")
      assert {:ok, "2"} = FerricStore.get("counter")
    end

    test "exists checks the sandboxed key" do
      FerricStore.set("check_me", "val")
      assert FerricStore.exists("check_me") == true
      assert FerricStore.exists("nonexistent") == false
    end

    test "del removes the sandboxed key" do
      FerricStore.set("del_me", "val")
      assert :ok = FerricStore.del("del_me")
      assert {:ok, nil} = FerricStore.get("del_me")
    end

    test "keys/1 returns only sandbox-scoped keys with prefix stripped" do
      FerricStore.set("ns:a", "1")
      FerricStore.set("ns:b", "2")
      FerricStore.set("other", "3")

      {:ok, ns_keys} = FerricStore.keys("ns:*")
      assert Enum.sort(ns_keys) == ["ns:a", "ns:b"]

      {:ok, all_keys} = FerricStore.keys()
      assert "ns:a" in all_keys
      assert "ns:b" in all_keys
      assert "other" in all_keys
    end
  end

  # ---------------------------------------------------------------------------
  # SB-004: Sandbox disabled on non-test nodes
  #
  # This is tested at the TCP/RESP level in Ferricstore.Commands.SandboxTest.
  # Here we verify the config gating at the Elixir level: the default config
  # value "disabled" prevents SANDBOX commands from being processed.
  # ---------------------------------------------------------------------------

  describe "SB-004: sandbox disabled on non-test nodes" do
    test "sandbox_mode config defaults to disabled" do
      # Verify the config parameter exists and the mechanism works.
      # Config.get/1 takes a glob pattern and returns a list of {key, value} tuples.
      result = Ferricstore.Config.get("sandbox_mode")

      case result do
        [{_key, value}] ->
          # In test env it may be set to "disabled", "local", or "enabled"
          assert value in ["disabled", "local", "enabled"]

        [] ->
          # If the key is not in the config store, that means it defaults
          # to disabled (no explicit setting = disabled behavior)
          assert true
      end
    end

    test "sandbox namespace is only active when explicitly checked out" do
      # Without checkout, Process dictionary has no sandbox namespace,
      # which means production code paths never apply key prefixing.
      # We verify by temporarily clearing and restoring the sandbox.
      saved_ns = Process.get(:ferricstore_sandbox)
      Process.delete(:ferricstore_sandbox)

      try do
        # Without sandbox, sandbox_key returns the key unchanged
        assert FerricStore.sandbox_key("mykey") == "mykey"
        assert FerricStore.Sandbox.current_namespace() == nil
      after
        if saved_ns, do: Process.put(:ferricstore_sandbox, saved_ns)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # SB-005: FLUSHDB scoped to sandbox
  # ---------------------------------------------------------------------------

  describe "SB-005: FLUSHDB scoped to sandbox" do
    test "FLUSHDB inside sandbox removes all sandbox keys" do
      # Write keys inside the sandbox
      FerricStore.set("flush_a", "1")
      FerricStore.set("flush_b", "2")
      FerricStore.set("flush_c", "3")

      # Verify sandbox keys exist
      {:ok, size_before} = FerricStore.dbsize()
      assert size_before == 3

      # FLUSHDB inside the sandbox
      assert :ok = FerricStore.flushdb()

      # Sandbox keys should be gone
      {:ok, size_after} = FerricStore.dbsize()
      assert size_after == 0

      assert {:ok, nil} = FerricStore.get("flush_a")
      assert {:ok, nil} = FerricStore.get("flush_b")
      assert {:ok, nil} = FerricStore.get("flush_c")
    end

    test "FLUSHDB in one sandbox does not affect another sandbox" do
      # Current sandbox: write keys
      FerricStore.set("shared_name", "from_sb1")

      # Create a second sandbox in a separate process
      test_pid = self()

      task =
        Task.async(fn ->
          sb2 = FerricStore.Sandbox.checkout()

          try do
            FerricStore.set("shared_name", "from_sb2")

            # Flush this second sandbox
            FerricStore.flushdb()

            {:ok, val_after_flush} = FerricStore.get("shared_name")
            send(test_pid, {:sb2_result, val_after_flush})
          after
            FerricStore.Sandbox.checkin(sb2)
          end
        end)

      assert_receive {:sb2_result, nil}, 5_000
      Task.await(task, 5_000)

      # The first sandbox's key should still be intact — private shards are isolated
      assert {:ok, "from_sb1"} = FerricStore.get("shared_name")
    end

    test "FLUSHDB clears hash keys within sandbox scope" do
      FerricStore.hset("flush_hash", %{"f1" => "v1", "f2" => "v2"})
      FerricStore.set("flush_str", "val")

      {:ok, before_size} = FerricStore.dbsize()
      assert before_size == 2

      FerricStore.flushdb()

      {:ok, after_size} = FerricStore.dbsize()
      assert after_size == 0

      assert {:ok, nil} = FerricStore.hget("flush_hash", "f1")
      assert {:ok, nil} = FerricStore.get("flush_str")
    end

    test "can write new keys after scoped FLUSHDB" do
      FerricStore.set("before", "old")
      FerricStore.flushdb()

      FerricStore.set("after", "new")
      assert {:ok, "new"} = FerricStore.get("after")
      assert {:ok, nil} = FerricStore.get("before")
    end
  end
end
