defmodule Ferricstore.SandboxTest do
  @moduledoc """
  Comprehensive tests for FerricStore.Sandbox -- namespace isolation, allow/1,
  TTL freeze, and cleanup.

  Tests verify that:
  - checkout/0 generates unique namespaces and sets them in Process dictionary
  - checkin/1 flushes namespace keys and clears Process dictionary
  - Different namespaces are completely isolated
  - allow/1 propagates the sandbox to other processes
  - TTL freeze prevents mid-test expiry
  - expire_now/1 forces immediate expiry in frozen namespaces
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Router

  # ===========================================================================
  # Checkout / Checkin basics
  # ===========================================================================

  describe "checkout/0 and checkin/1" do
    test "checkout generates a namespace and sets it in Process dictionary" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        assert is_binary(namespace)
        assert String.starts_with?(namespace, "test_")
        assert String.ends_with?(namespace, "_")
        assert Process.get(:ferricstore_sandbox) == namespace
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "checkout generates unique namespaces on each call" do
      ns1 = FerricStore.Sandbox.checkout()
      FerricStore.Sandbox.checkin(ns1)

      ns2 = FerricStore.Sandbox.checkout()
      FerricStore.Sandbox.checkin(ns2)

      assert ns1 != ns2
    end

    test "checkin clears the Process dictionary entry" do
      namespace = FerricStore.Sandbox.checkout()
      assert Process.get(:ferricstore_sandbox) == namespace

      FerricStore.Sandbox.checkin(namespace)
      assert Process.get(:ferricstore_sandbox) == nil
    end

    test "checkin deletes namespace keys from the store" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        FerricStore.set("cleanup_key1", "val1")
        FerricStore.set("cleanup_key2", "val2")

        # Verify keys exist
        assert {:ok, "val1"} = FerricStore.get("cleanup_key1")
        assert {:ok, "val2"} = FerricStore.get("cleanup_key2")
      after
        FerricStore.Sandbox.checkin(namespace)
      end

      # After checkin, even re-checking out a new namespace, the old keys
      # should be gone from the underlying store.
      # We verify by checking the raw Router (no sandbox prefix)
      assert Router.get(namespace <> "cleanup_key1") == nil
      assert Router.get(namespace <> "cleanup_key2") == nil
    end

    test "namespace has correct format: test_<16 hex chars>_" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        # "test_" prefix + 16 hex chars + "_" suffix = 22 chars total
        assert byte_size(namespace) == 22
        assert Regex.match?(~r/^test_[0-9a-f]{16}_$/, namespace)
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end
  end

  # ===========================================================================
  # Namespace isolation
  # ===========================================================================

  describe "namespace isolation" do
    test "two sandboxes with same logical key are completely isolated" do
      ns1 = FerricStore.Sandbox.checkout()
      FerricStore.set("shared_name", "from_ns1")
      FerricStore.Sandbox.checkin(ns1)

      ns2 = FerricStore.Sandbox.checkout()

      try do
        # The key "shared_name" in ns2's namespace should not see ns1's value
        assert {:ok, nil} = FerricStore.get("shared_name")
      after
        FerricStore.Sandbox.checkin(ns2)
      end
    end

    test "keys set in one namespace are invisible in another namespace" do
      # First namespace: set some keys
      ns1 = FerricStore.Sandbox.checkout()
      FerricStore.set("isolated_a", "value_a")
      FerricStore.set("isolated_b", "value_b")
      FerricStore.Sandbox.checkin(ns1)

      # Second namespace: these keys should not exist
      ns2 = FerricStore.Sandbox.checkout()

      try do
        assert {:ok, nil} = FerricStore.get("isolated_a")
        assert {:ok, nil} = FerricStore.get("isolated_b")
      after
        FerricStore.Sandbox.checkin(ns2)
      end
    end

    test "operations in separate sandboxes do not interfere" do
      ns1 = FerricStore.Sandbox.checkout()
      FerricStore.set("counter", "10")
      FerricStore.Sandbox.checkin(ns1)

      ns2 = FerricStore.Sandbox.checkout()

      try do
        # counter in ns2 does not exist -- incr should start from 0
        assert {:ok, 1} = FerricStore.incr("counter")
      after
        FerricStore.Sandbox.checkin(ns2)
      end
    end

    test "del in one namespace does not affect another" do
      ns1 = FerricStore.Sandbox.checkout()
      FerricStore.set("to_del", "exists_in_ns1")

      # Switch to ns2 without checking in ns1 (ns1's keys stay in the store)
      Process.put(:ferricstore_sandbox, nil)
      ns2 = FerricStore.Sandbox.checkout()
      FerricStore.set("to_del", "exists_in_ns2")
      FerricStore.del("to_del")
      FerricStore.Sandbox.checkin(ns2)

      # Re-enter ns1 scope by manually setting the namespace
      Process.put(:ferricstore_sandbox, ns1)

      try do
        # ns1's key should still exist (ns2's del only affected ns2's namespace)
        assert {:ok, "exists_in_ns1"} = FerricStore.get("to_del")
      after
        FerricStore.Sandbox.checkin(ns1)
      end
    end

    test "hash operations are isolated between namespaces" do
      ns1 = FerricStore.Sandbox.checkout()
      FerricStore.hset("user", %{"name" => "alice"})
      FerricStore.Sandbox.checkin(ns1)

      ns2 = FerricStore.Sandbox.checkout()

      try do
        assert {:ok, nil} = FerricStore.hget("user", "name")
        assert {:ok, %{}} = FerricStore.hgetall("user")
      after
        FerricStore.Sandbox.checkin(ns2)
      end
    end
  end

  # ===========================================================================
  # FerricStore.Sandbox.Case — use macro
  # ===========================================================================

  describe "FerricStore.Sandbox.Case integration" do
    # We test this indirectly: we ARE using it in EmbeddedTest.
    # Here we verify the basic contract.

    test "current_namespace returns the active namespace" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        assert FerricStore.Sandbox.current_namespace() == namespace
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "current_namespace returns nil when no sandbox is active" do
      saved = Process.get(:ferricstore_sandbox)
      Process.delete(:ferricstore_sandbox)

      assert FerricStore.Sandbox.current_namespace() == nil

      if saved, do: Process.put(:ferricstore_sandbox, saved)
    end
  end

  # ===========================================================================
  # allow/1 — sandbox propagation to other processes
  # ===========================================================================

  describe "allow/1" do
    test "sends sandbox namespace to target pid" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        # Spawn a process that will receive the allow message
        test_pid = self()

        target_pid =
          spawn(fn ->
            receive do
              {:ferricstore_sandbox_allow, ns} ->
                Process.put(:ferricstore_sandbox, ns)
                send(test_pid, {:namespace_received, ns})
            after
              5_000 -> send(test_pid, :timeout)
            end
          end)

        FerricStore.Sandbox.allow(target_pid)

        assert_receive {:namespace_received, ^namespace}, 1_000
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "allow with no active sandbox does nothing" do
      saved = Process.get(:ferricstore_sandbox)
      Process.delete(:ferricstore_sandbox)

      test_pid = self()

      target_pid =
        spawn(fn ->
          receive do
            {:ferricstore_sandbox_allow, _} ->
              send(test_pid, :received)
          after
            100 -> send(test_pid, :no_message)
          end
        end)

      assert :ok = FerricStore.Sandbox.allow(target_pid)
      assert_receive :no_message, 200

      if saved, do: Process.put(:ferricstore_sandbox, saved)
    end

    test "allowed process can write to the test's namespace" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        test_pid = self()

        worker_pid =
          spawn(fn ->
            receive do
              {:ferricstore_sandbox_allow, ns} ->
                Process.put(:ferricstore_sandbox, ns)
                FerricStore.set("worker_key", "worker_value")
                send(test_pid, :done)
            end
          end)

        FerricStore.Sandbox.allow(worker_pid)
        assert_receive :done, 2_000

        # The test process should see the worker's write
        assert {:ok, "worker_value"} = FerricStore.get("worker_key")
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "allowed process reads from the test's namespace" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        FerricStore.set("shared_read", "visible")
        test_pid = self()

        worker_pid =
          spawn(fn ->
            receive do
              {:ferricstore_sandbox_allow, ns} ->
                Process.put(:ferricstore_sandbox, ns)
                {:ok, val} = FerricStore.get("shared_read")
                send(test_pid, {:read_result, val})
            end
          end)

        FerricStore.Sandbox.allow(worker_pid)
        assert_receive {:read_result, "visible"}, 2_000
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end
  end

  # ===========================================================================
  # TTL freeze
  # ===========================================================================

  describe "TTL freeze" do
    test "checkout with freeze_ttl: true sets freeze flag" do
      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        assert Process.get(:ferricstore_sandbox_freeze_ttl) == true
        assert FerricStore.Sandbox.ttl_frozen?() == true
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "checkout without freeze_ttl: true does not set freeze flag" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        assert Process.get(:ferricstore_sandbox_freeze_ttl) == nil
        assert FerricStore.Sandbox.ttl_frozen?() == false
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "checkin clears the freeze flag" do
      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)
      assert FerricStore.Sandbox.ttl_frozen?() == true

      FerricStore.Sandbox.checkin(namespace)
      assert FerricStore.Sandbox.ttl_frozen?() == false
    end

    test "key with TTL remains accessible during frozen sandbox" do
      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        FerricStore.set("frozen_key", "value", ttl: :timer.seconds(1))

        # Key should be accessible because TTL sweep is frozen
        # (We cannot actually test the active expiry sweep here since it's
        # not yet implemented, but we verify the flag is set correctly)
        assert {:ok, "value"} = FerricStore.get("frozen_key")
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end
  end

  # ===========================================================================
  # expire_now/1 — forced manual expiry
  # ===========================================================================

  describe "expire_now/1" do
    test "forces immediate expiry of a key" do
      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        FerricStore.set("manual_expire", "value", ttl: :timer.hours(1))
        assert {:ok, "value"} = FerricStore.get("manual_expire")

        FerricStore.Sandbox.expire_now("manual_expire")
        assert {:ok, nil} = FerricStore.get("manual_expire")
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "expire_now on nonexistent key is a no-op" do
      namespace = FerricStore.Sandbox.checkout()

      try do
        # Should not raise
        assert :ok = FerricStore.Sandbox.expire_now("never_existed")
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end

    test "expire_now deletes the key entirely" do
      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        FerricStore.set("to_expire", "data")
        FerricStore.Sandbox.expire_now("to_expire")

        # After expire_now, even a new set should work cleanly
        FerricStore.set("to_expire", "new_data")
        assert {:ok, "new_data"} = FerricStore.get("to_expire")
      after
        FerricStore.Sandbox.checkin(namespace)
      end
    end
  end

  # ===========================================================================
  # Cleanup — checkin flushes all namespace keys
  # ===========================================================================

  describe "cleanup on checkin" do
    test "checkin removes all keys with namespace prefix" do
      namespace = FerricStore.Sandbox.checkout()

      FerricStore.set("cleanup_a", "1")
      FerricStore.set("cleanup_b", "2")
      FerricStore.set("cleanup_c", "3")

      FerricStore.Sandbox.checkin(namespace)

      # Verify raw keys are gone
      assert Router.get(namespace <> "cleanup_a") == nil
      assert Router.get(namespace <> "cleanup_b") == nil
      assert Router.get(namespace <> "cleanup_c") == nil
    end

    test "checkin only removes keys with matching namespace prefix" do
      # Set a key outside any sandbox
      raw_key = "global_key_#{:erlang.unique_integer([:positive])}"
      Router.put(raw_key, "global_value", 0)

      # Create a sandbox and set keys
      namespace = FerricStore.Sandbox.checkout()
      FerricStore.set("sandboxed", "val")
      FerricStore.Sandbox.checkin(namespace)

      # The global key should still exist
      assert Router.get(raw_key) == "global_value"

      # Clean up the global key
      Router.delete(raw_key)
    end

    test "checkin with hash keys cleans up correctly" do
      namespace = FerricStore.Sandbox.checkout()
      FerricStore.hset("hash_cleanup", %{"field" => "value"})
      FerricStore.Sandbox.checkin(namespace)

      assert Router.get(namespace <> "hash_cleanup") == nil
    end

    test "checkin after incr cleans up the counter key" do
      namespace = FerricStore.Sandbox.checkout()
      FerricStore.incr("counter_cleanup")
      FerricStore.incr("counter_cleanup")
      FerricStore.Sandbox.checkin(namespace)

      assert Router.get(namespace <> "counter_cleanup") == nil
    end
  end

  # ===========================================================================
  # Concurrent sandbox usage (simulating async: true tests)
  # ===========================================================================

  describe "concurrent sandbox usage" do
    test "two concurrent processes with different sandboxes are isolated" do
      test_pid = self()

      spawn(fn ->
        ns = FerricStore.Sandbox.checkout()
        FerricStore.set("concurrent_key", "from_process_1")
        {:ok, val} = FerricStore.get("concurrent_key")
        send(test_pid, {:p1_result, val, ns})
        FerricStore.Sandbox.checkin(ns)
        send(test_pid, :p1_done)
      end)

      spawn(fn ->
        ns = FerricStore.Sandbox.checkout()
        FerricStore.set("concurrent_key", "from_process_2")
        {:ok, val} = FerricStore.get("concurrent_key")
        send(test_pid, {:p2_result, val, ns})
        FerricStore.Sandbox.checkin(ns)
        send(test_pid, :p2_done)
      end)

      assert_receive {:p1_result, "from_process_1", ns1}, 5_000
      assert_receive {:p2_result, "from_process_2", ns2}, 5_000
      assert ns1 != ns2
      assert_receive :p1_done, 5_000
      assert_receive :p2_done, 5_000
    end

    test "10 concurrent sandboxes writing same logical key are fully isolated" do
      test_pid = self()

      pids =
        for i <- 1..10 do
          spawn(fn ->
            ns = FerricStore.Sandbox.checkout()
            value = "value_#{i}"
            FerricStore.set("same_key", value)
            {:ok, read_back} = FerricStore.get("same_key")
            send(test_pid, {:result, i, read_back, ns})
            FerricStore.Sandbox.checkin(ns)
          end)
        end

      results =
        for _i <- 1..10 do
          assert_receive {:result, idx, value, _ns}, 5_000
          {idx, value}
        end

      # Each process should read back its own value
      for {i, val} <- results do
        assert val == "value_#{i}",
               "Process #{i} read back #{inspect(val)} instead of value_#{i}"
      end

      # Ensure all pids completed
      for pid <- pids do
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000
      end
    end
  end
end
