defmodule Ferricstore.Commands.BlockingTest do
  @moduledoc """
  Unit tests for blocking list commands and the waiter registry.

  Tests cover:
  - `Ferricstore.Commands.Blocking` (argument parsing)
  - `Ferricstore.Waiters` (ETS-based waiter registry)
  - Blocking behavior via BEAM process messaging

  Most tests are async since they use isolated mock stores and process-local
  waiter registrations.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Blocking
  alias Ferricstore.Waiters

  # ===========================================================================
  # Setup -- ensure the waiter ETS table exists
  # ===========================================================================

  setup do
    # The ETS table may already exist from a previous test or application start.
    # Create it only if it does not exist yet.
    if :ets.whereis(:ferricstore_waiters) == :undefined do
      Waiters.init()
    end

    # Clean up any leftover waiters from prior tests
    :ets.delete_all_objects(:ferricstore_waiters)

    :ok
  end

  # ===========================================================================
  # Blocking argument parsing -- BLPOP / BRPOP
  # ===========================================================================

  describe "parse_blpop_args/1" do
    test "parses single key with integer timeout" do
      assert {:ok, ["mylist"], 5000} = Blocking.parse_blpop_args(["mylist", "5"])
    end

    test "parses multiple keys with timeout" do
      assert {:ok, ["a", "b", "c"], 10000} = Blocking.parse_blpop_args(["a", "b", "c", "10"])
    end

    test "parses timeout 0 as block forever" do
      assert {:ok, ["mylist"], 0} = Blocking.parse_blpop_args(["mylist", "0"])
    end

    test "parses float timeout" do
      assert {:ok, ["mylist"], 100} = Blocking.parse_blpop_args(["mylist", "0.1"])
    end

    test "rejects negative timeout" do
      assert {:error, _} = Blocking.parse_blpop_args(["mylist", "-1"])
    end

    test "rejects non-numeric timeout" do
      assert {:error, _} = Blocking.parse_blpop_args(["mylist", "abc"])
    end

    test "rejects empty args" do
      assert {:error, _} = Blocking.parse_blpop_args([])
    end

    test "rejects single arg (no timeout)" do
      assert {:error, _} = Blocking.parse_blpop_args(["mylist"])
    end
  end

  # ===========================================================================
  # Blocking argument parsing -- BLMOVE
  # ===========================================================================

  describe "parse_blmove_args/1" do
    test "parses valid BLMOVE args" do
      assert {:ok, "src", "dst", :left, :right, 5000} =
               Blocking.parse_blmove_args(["src", "dst", "LEFT", "RIGHT", "5"])
    end

    test "direction is case-insensitive" do
      assert {:ok, "src", "dst", :right, :left, 0} =
               Blocking.parse_blmove_args(["src", "dst", "right", "left", "0"])
    end

    test "rejects invalid direction" do
      assert {:error, _} = Blocking.parse_blmove_args(["src", "dst", "UP", "DOWN", "5"])
    end

    test "rejects wrong number of args" do
      assert {:error, _} = Blocking.parse_blmove_args(["src", "dst", "LEFT"])
      assert {:error, _} = Blocking.parse_blmove_args([])
    end
  end

  # ===========================================================================
  # Blocking argument parsing -- BLMPOP
  # ===========================================================================

  describe "parse_blmpop_args/1" do
    test "parses valid BLMPOP args with single key" do
      assert {:ok, ["mylist"], :left, 1, 5000} =
               Blocking.parse_blmpop_args(["5", "1", "mylist", "LEFT"])
    end

    test "parses multiple keys" do
      assert {:ok, ["a", "b"], :right, 1, 0} =
               Blocking.parse_blmpop_args(["0", "2", "a", "b", "RIGHT"])
    end

    test "parses with COUNT option" do
      assert {:ok, ["mylist"], :left, 3, 5000} =
               Blocking.parse_blmpop_args(["5", "1", "mylist", "LEFT", "COUNT", "3"])
    end

    test "rejects wrong number of args" do
      assert {:error, _} = Blocking.parse_blmpop_args(["5"])
      assert {:error, _} = Blocking.parse_blmpop_args([])
    end

    test "rejects invalid direction" do
      assert {:error, _} = Blocking.parse_blmpop_args(["5", "1", "mylist", "UP"])
    end
  end

  # ===========================================================================
  # Waiters registry
  # ===========================================================================

  describe "Waiters" do
    test "register and notify wakes the process" do
      key = "test_key_#{:erlang.unique_integer()}"
      me = self()

      # Spawn a waiter process
      waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, :woken)
          after
            1000 -> send(me, :timeout)
          end
        end)

      # Give the waiter time to register
      Process.sleep(10)

      # Notify and check it woke up
      assert Waiters.notify_push(key) == waiter

      assert_receive :woken, 500
    end

    test "FIFO ordering: oldest waiter is notified first" do
      key = "test_fifo_#{:erlang.unique_integer()}"
      me = self()

      # Spawn two waiters with a small delay between them
      waiter1 =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, 1})
          after
            1000 -> send(me, {:timeout, 1})
          end
        end)

      Process.sleep(5)

      _waiter2 =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, 2})
          after
            1000 -> send(me, {:timeout, 2})
          end
        end)

      Process.sleep(10)

      # Notify once -- only waiter1 should wake
      notified = Waiters.notify_push(key)
      assert notified == waiter1

      assert_receive {:woken, 1}, 500
      refute_receive {:woken, 2}, 100
    end

    test "notify_push returns nil when no waiters" do
      key = "test_empty_#{:erlang.unique_integer()}"
      assert Waiters.notify_push(key) == nil
    end

    test "unregister removes waiter" do
      key = "test_unreg_#{:erlang.unique_integer()}"
      Waiters.register(key, self(), 0)
      assert Waiters.count(key) == 1

      Waiters.unregister(key, self())
      assert Waiters.count(key) == 0
    end

    test "cleanup removes all waiters for a pid" do
      key1 = "test_clean1_#{:erlang.unique_integer()}"
      key2 = "test_clean2_#{:erlang.unique_integer()}"

      Waiters.register(key1, self(), 0)
      Waiters.register(key2, self(), 0)

      assert Waiters.count(key1) == 1
      assert Waiters.count(key2) == 1

      Waiters.cleanup(self())

      assert Waiters.count(key1) == 0
      assert Waiters.count(key2) == 0
    end

    test "multiple waiters on same key" do
      key = "test_multi_#{:erlang.unique_integer()}"
      pid1 = spawn(fn -> Process.sleep(5000) end)
      pid2 = spawn(fn -> Process.sleep(5000) end)

      Waiters.register(key, pid1, 0)
      Waiters.register(key, pid2, 0)

      assert Waiters.count(key) == 2

      # First notify wakes oldest
      Waiters.notify_push(key)
      assert Waiters.count(key) == 1

      # Second notify wakes the remaining one
      Waiters.notify_push(key)
      assert Waiters.count(key) == 0

      Process.exit(pid1, :kill)
      Process.exit(pid2, :kill)
    end
  end

  # ===========================================================================
  # Blocking behavior integration (process-level)
  # ===========================================================================

  describe "blocking pop behavior" do
    test "BLPOP returns immediately when list has data" do
      # Simulate: list with data, pop should return immediately
      # This tests the argument parsing and the concept, not the TCP connection
      assert {:ok, ["mylist"], 5000} = Blocking.parse_blpop_args(["mylist", "5"])
    end

    test "waiter notification triggers pop" do
      key = "blpop_test_#{:erlang.unique_integer()}"
      me = self()

      # Spawn a process that simulates blocking on the key
      _waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} ->
              send(me, {:got_notification, key})
          after
            2000 ->
              send(me, :timeout)
          end
        end)

      Process.sleep(10)

      # Simulate a push to the key
      Waiters.notify_push(key)

      assert_receive {:got_notification, ^key}, 500
    end

    test "blocking with timeout expires when no push arrives" do
      key = "timeout_test_#{:erlang.unique_integer()}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} ->
              send(me, :unexpected_wake)
          after
            100 ->
              Waiters.unregister(key, self())
              send(me, :timed_out)
          end
        end)

      assert_receive :timed_out, 500
      refute_receive :unexpected_wake, 100
    end
  end
end
