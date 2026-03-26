defmodule Ferricstore.SandboxTest do
  @moduledoc """
  Comprehensive tests for FerricStore.Sandbox -- true shard isolation, allow/2,
  TTL freeze, and cleanup.

  Tests verify that:
  - checkout/0 returns a sandbox struct with private shards
  - checkin/1 stops private shards and cleans up ETS + tmpdir
  - Different sandboxes are completely isolated (separate ETS tables)
  - allow/2 propagates the sandbox to other processes via ETS registry
  - TTL freeze prevents mid-test expiry
  - expire_now/1 forces immediate expiry
  """
  use ExUnit.Case, async: true

  # ===========================================================================
  # Checkout / Checkin basics
  # ===========================================================================

  describe "checkout/0 and checkin/1" do
    test "checkout returns a sandbox struct and sets it in Process dictionary" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        assert %FerricStore.Sandbox{} = sandbox
        assert sandbox.shard_count == 2
        assert is_binary(sandbox.tmpdir)
        assert tuple_size(sandbox.shards) == 2
        assert tuple_size(sandbox.keydirs) == 2
        assert Process.get(:ferricstore_sandbox) == sandbox
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "checkout generates unique sandboxes on each call" do
      s1 = FerricStore.Sandbox.checkout()
      FerricStore.Sandbox.checkin(s1)

      s2 = FerricStore.Sandbox.checkout()
      FerricStore.Sandbox.checkin(s2)

      assert s1.ref != s2.ref
      assert s1.tmpdir != s2.tmpdir
    end

    test "checkin clears the Process dictionary entry" do
      sandbox = FerricStore.Sandbox.checkout()
      assert Process.get(:ferricstore_sandbox) == sandbox

      FerricStore.Sandbox.checkin(sandbox)
      assert Process.get(:ferricstore_sandbox) == nil
    end

    test "checkin stops shard processes" do
      sandbox = FerricStore.Sandbox.checkout()
      pids = for i <- 0..(sandbox.shard_count - 1), do: elem(sandbox.shards, i)
      assert Enum.all?(pids, &Process.alive?/1)

      FerricStore.Sandbox.checkin(sandbox)
      # Give processes a moment to terminate
      Process.sleep(50)
      assert Enum.all?(pids, fn pid -> not Process.alive?(pid) end)
    end

    test "checkin removes tmpdir" do
      sandbox = FerricStore.Sandbox.checkout()
      assert File.exists?(sandbox.tmpdir)

      FerricStore.Sandbox.checkin(sandbox)
      refute File.exists?(sandbox.tmpdir)
    end

    test "checkin deletes ETS tables" do
      sandbox = FerricStore.Sandbox.checkout()
      keydirs = for i <- 0..(sandbox.shard_count - 1), do: elem(sandbox.keydirs, i)
      assert Enum.all?(keydirs, fn kd -> :ets.info(kd) != :undefined end)

      FerricStore.Sandbox.checkin(sandbox)
      assert Enum.all?(keydirs, fn kd -> :ets.info(kd) == :undefined end)
    end

    test "custom shard_count is respected" do
      sandbox = FerricStore.Sandbox.checkout(shard_count: 4)

      try do
        assert sandbox.shard_count == 4
        assert tuple_size(sandbox.shards) == 4
        assert tuple_size(sandbox.keydirs) == 4
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end
  end

  # ===========================================================================
  # Namespace isolation (true shard isolation)
  # ===========================================================================

  describe "namespace isolation" do
    test "two sandboxes with same logical key are completely isolated" do
      s1 = FerricStore.Sandbox.checkout()
      FerricStore.set("shared_name", "from_test_1")
      FerricStore.Sandbox.checkin(s1)

      s2 = FerricStore.Sandbox.checkout()

      try do
        assert {:ok, nil} = FerricStore.get("shared_name")
      after
        FerricStore.Sandbox.checkin(s2)
      end
    end

    test "keys set in one sandbox are invisible in another" do
      s1 = FerricStore.Sandbox.checkout()
      FerricStore.set("isolated_a", "value_a")
      FerricStore.set("isolated_b", "value_b")
      FerricStore.Sandbox.checkin(s1)

      s2 = FerricStore.Sandbox.checkout()

      try do
        assert {:ok, nil} = FerricStore.get("isolated_a")
        assert {:ok, nil} = FerricStore.get("isolated_b")
      after
        FerricStore.Sandbox.checkin(s2)
      end
    end

    test "operations in separate sandboxes do not interfere" do
      s1 = FerricStore.Sandbox.checkout()
      FerricStore.set("counter", "10")
      FerricStore.Sandbox.checkin(s1)

      s2 = FerricStore.Sandbox.checkout()

      try do
        assert {:ok, 1} = FerricStore.incr("counter")
      after
        FerricStore.Sandbox.checkin(s2)
      end
    end

    test "del in one sandbox does not affect another" do
      s1 = FerricStore.Sandbox.checkout()
      FerricStore.set("to_del", "exists_in_s1")
      ref1 = s1

      # Switch to s2 without checking in s1
      Process.put(:ferricstore_sandbox, nil)
      s2 = FerricStore.Sandbox.checkout()
      FerricStore.set("to_del", "exists_in_s2")
      FerricStore.del("to_del")
      FerricStore.Sandbox.checkin(s2)

      # Re-enter s1 scope
      Process.put(:ferricstore_sandbox, ref1)

      try do
        assert {:ok, "exists_in_s1"} = FerricStore.get("to_del")
      after
        FerricStore.Sandbox.checkin(ref1)
      end
    end

    test "hash operations are isolated between sandboxes" do
      s1 = FerricStore.Sandbox.checkout()
      FerricStore.hset("user", %{"name" => "alice"})
      FerricStore.Sandbox.checkin(s1)

      s2 = FerricStore.Sandbox.checkout()

      try do
        assert {:ok, nil} = FerricStore.hget("user", "name")
        assert {:ok, %{}} = FerricStore.hgetall("user")
      after
        FerricStore.Sandbox.checkin(s2)
      end
    end

    test "dbsize returns count for current sandbox only" do
      s1 = FerricStore.Sandbox.checkout()

      try do
        FerricStore.set("key1", "val1")
        FerricStore.set("key2", "val2")
        assert {:ok, 2} = FerricStore.dbsize()
      after
        FerricStore.Sandbox.checkin(s1)
      end
    end

    test "keys() returns keys for current sandbox only" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        FerricStore.set("foo", "bar")
        FerricStore.set("baz", "qux")
        {:ok, keys} = FerricStore.keys()
        assert Enum.sort(keys) == ["baz", "foo"]
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end
  end

  # ===========================================================================
  # FerricStore.Sandbox.Case — use macro
  # ===========================================================================

  describe "FerricStore.Sandbox.Case integration" do
    test "current_namespace returns the active sandbox" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        assert FerricStore.Sandbox.current_namespace() == sandbox
      after
        FerricStore.Sandbox.checkin(sandbox)
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
  # allow/2 — sandbox propagation to other processes via ETS registry
  # ===========================================================================

  describe "allow/2" do
    test "allowed process can write to the test's sandbox" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        test_pid = self()

        worker_pid =
          spawn(fn ->
            # Worker has no process dict sandbox, but ETS registry has it
            FerricStore.set("worker_key", "worker_value")
            send(test_pid, :done)
          end)

        FerricStore.Sandbox.allow(self(), worker_pid)
        assert_receive :done, 2_000

        # The test process should see the worker's write
        assert {:ok, "worker_value"} = FerricStore.get("worker_key")
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "allowed process reads from the test's sandbox" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        FerricStore.set("shared_read", "visible")
        test_pid = self()

        worker_pid =
          spawn(fn ->
            {:ok, val} = FerricStore.get("shared_read")
            send(test_pid, {:read_result, val})
          end)

        FerricStore.Sandbox.allow(self(), worker_pid)
        assert_receive {:read_result, "visible"}, 2_000
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "allow with no active sandbox does nothing" do
      saved = Process.get(:ferricstore_sandbox)
      Process.delete(:ferricstore_sandbox)

      # Should not raise
      assert :ok = FerricStore.Sandbox.allow(self(), spawn(fn -> :ok end))

      if saved, do: Process.put(:ferricstore_sandbox, saved)
    end

    test "allow via allow/1 works for struct sandbox" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        test_pid = self()

        worker_pid =
          spawn(fn ->
            FerricStore.set("via_allow1", "works")
            send(test_pid, :done)
          end)

        FerricStore.Sandbox.allow(worker_pid)
        assert_receive :done, 2_000

        assert {:ok, "works"} = FerricStore.get("via_allow1")
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end
  end

  # ===========================================================================
  # TTL freeze
  # ===========================================================================

  describe "TTL freeze" do
    test "checkout with freeze_ttl: true sets freeze flag" do
      sandbox = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        assert Process.get(:ferricstore_sandbox_freeze_ttl) == true
        assert FerricStore.Sandbox.ttl_frozen?() == true
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "checkout without freeze_ttl does not set freeze flag" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        assert Process.get(:ferricstore_sandbox_freeze_ttl) == nil
        assert FerricStore.Sandbox.ttl_frozen?() == false
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "checkin clears the freeze flag" do
      sandbox = FerricStore.Sandbox.checkout(freeze_ttl: true)
      assert FerricStore.Sandbox.ttl_frozen?() == true

      FerricStore.Sandbox.checkin(sandbox)
      assert FerricStore.Sandbox.ttl_frozen?() == false
    end

    test "key with TTL remains accessible during frozen sandbox" do
      sandbox = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        FerricStore.set("frozen_key", "value", ttl: :timer.seconds(1))
        assert {:ok, "value"} = FerricStore.get("frozen_key")
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end
  end

  # ===========================================================================
  # expire_now/1 — forced manual expiry
  # ===========================================================================

  describe "expire_now/1" do
    test "forces immediate expiry of a key" do
      sandbox = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        FerricStore.set("manual_expire", "value", ttl: :timer.hours(1))
        assert {:ok, "value"} = FerricStore.get("manual_expire")

        FerricStore.Sandbox.expire_now("manual_expire")
        assert {:ok, nil} = FerricStore.get("manual_expire")
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "expire_now on nonexistent key is a no-op" do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        assert :ok = FerricStore.Sandbox.expire_now("never_existed")
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "expire_now deletes the key entirely" do
      sandbox = FerricStore.Sandbox.checkout(freeze_ttl: true)

      try do
        FerricStore.set("to_expire", "data")
        FerricStore.Sandbox.expire_now("to_expire")

        FerricStore.set("to_expire", "new_data")
        assert {:ok, "new_data"} = FerricStore.get("to_expire")
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end
  end

  # ===========================================================================
  # Concurrent sandbox usage (simulating async: true tests)
  # ===========================================================================

  describe "concurrent sandbox usage" do
    test "two concurrent processes with different sandboxes are isolated" do
      test_pid = self()

      spawn(fn ->
        s = FerricStore.Sandbox.checkout()
        FerricStore.set("concurrent_key", "from_process_1")
        {:ok, val} = FerricStore.get("concurrent_key")
        send(test_pid, {:p1_result, val})
        FerricStore.Sandbox.checkin(s)
        send(test_pid, :p1_done)
      end)

      spawn(fn ->
        s = FerricStore.Sandbox.checkout()
        FerricStore.set("concurrent_key", "from_process_2")
        {:ok, val} = FerricStore.get("concurrent_key")
        send(test_pid, {:p2_result, val})
        FerricStore.Sandbox.checkin(s)
        send(test_pid, :p2_done)
      end)

      assert_receive {:p1_result, "from_process_1"}, 5_000
      assert_receive {:p2_result, "from_process_2"}, 5_000
      assert_receive :p1_done, 5_000
      assert_receive :p2_done, 5_000
    end

    test "10 concurrent sandboxes writing same logical key are fully isolated" do
      test_pid = self()

      pids =
        for i <- 1..10 do
          spawn(fn ->
            s = FerricStore.Sandbox.checkout()
            value = "value_#{i}"
            FerricStore.set("same_key", value)
            {:ok, read_back} = FerricStore.get("same_key")
            send(test_pid, {:result, i, read_back})
            FerricStore.Sandbox.checkin(s)
          end)
        end

      results =
        for _i <- 1..10 do
          assert_receive {:result, idx, value}, 10_000
          {idx, value}
        end

      for {i, val} <- results do
        assert val == "value_#{i}",
               "Process #{i} read back #{inspect(val)} instead of value_#{i}"
      end

      for pid <- pids do
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000
      end
    end
  end

  # ===========================================================================
  # Backward compatibility -- old string namespace sandbox
  # ===========================================================================

  describe "backward compatibility with string namespace" do
    test "old-style checkin with string namespace still works" do
      # Manually set a string namespace (old behavior)
      namespace = "test_backcompat_#{:erlang.unique_integer([:positive])}_"
      Process.put(:ferricstore_sandbox, namespace)

      # The old checkin should work
      assert :ok = FerricStore.Sandbox.checkin(namespace)
      assert Process.get(:ferricstore_sandbox) == nil
    end
  end
end
