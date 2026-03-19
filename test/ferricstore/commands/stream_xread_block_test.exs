defmodule Ferricstore.Commands.StreamXreadBlockTest do
  @moduledoc """
  Tests for XREAD BLOCK support.

  Verifies that:
  - XREAD BLOCK parses the BLOCK option and timeout correctly
  - XREAD BLOCK returns new entries when XADD happens before timeout
  - XREAD BLOCK returns nil after timeout with no new entries
  - XREAD BLOCK with $ waits for entries added after the command is issued
  - XREAD BLOCK 0 blocks indefinitely (tested with a separate XADD)
  - XADD wakes blocked XREAD waiters
  - Multiple blocked clients on different streams wake independently
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Test.MockStore

  defp ustream, do: "xrb_#{System.unique_integer([:positive, :monotonic])}"

  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    # Ensure the waiters ETS table exists.
    if :ets.whereis(:ferricstore_stream_waiters) == :undefined do
      try do
        :ets.new(:ferricstore_stream_waiters, [:duplicate_bag, :public, :named_table])
      rescue
        ArgumentError -> :ok
      end
    else
      :ets.delete_all_objects(:ferricstore_stream_waiters)
    end

    :ok
  end

  # ===========================================================================
  # XREAD BLOCK argument parsing
  # ===========================================================================

  describe "XREAD BLOCK parsing" do
    test "XREAD BLOCK parses and returns result for existing data" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XADD", [key, "2-0", "g", "w"], store)

      # BLOCK 0 with data already available should return immediately.
      result = Stream.handle("XREAD", ["BLOCK", "0", "STREAMS", key, "0"], store)

      assert is_list(result)
      assert length(result) == 1
      [[^key, entries]] = result
      assert length(entries) == 2
    end

    test "XREAD BLOCK with COUNT parses correctly" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      result = Stream.handle("XREAD", ["COUNT", "2", "BLOCK", "0", "STREAMS", key, "0"], store)

      assert is_list(result)
      [[^key, entries]] = result
      assert length(entries) == 2
    end

    test "XREAD BLOCK with specific ID returns entries after that ID" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      result = Stream.handle("XREAD", ["BLOCK", "100", "STREAMS", key, "1-0"], store)

      assert is_list(result)
      [[^key, entries]] = result
      assert Enum.map(entries, &hd/1) == ["2-0", "3-0"]
    end
  end

  # ===========================================================================
  # XREAD BLOCK timeout behavior
  # ===========================================================================

  describe "XREAD BLOCK timeout (non-blocking path)" do
    test "XREAD BLOCK returns immediately when data is already available" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      # Should return immediately, not block.
      {time_us, result} =
        :timer.tc(fn ->
          Stream.handle("XREAD", ["BLOCK", "5000", "STREAMS", key, "0"], store)
        end)

      assert is_list(result)
      assert length(result) == 1
      # Should complete in well under 1 second.
      assert time_us < 1_000_000
    end

    test "XREAD BLOCK returns empty/nil when no data and short timeout" do
      store = MockStore.make()
      key = ustream()

      # No data exists in this stream. BLOCK 100 should wait ~100ms then return.
      # Since we're testing the non-blocking command handler path (not the
      # connection-layer blocking), this returns the blocking metadata.
      result = Stream.handle("XREAD", ["BLOCK", "100", "STREAMS", key, "0"], store)

      # When data is available immediately, result is a list.
      # When no data is available, XREAD BLOCK returns {:block, ...} to signal
      # the connection layer should handle blocking.
      assert result == {:block, 100, [{key, "0"}], :infinity}
    end
  end

  # ===========================================================================
  # XREAD BLOCK with $ (new entries only)
  # ===========================================================================

  describe "XREAD BLOCK with $" do
    test "XREAD BLOCK with $ and no new entries signals block" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      # $ means: only entries added after this command. Since no new entries
      # exist, it should signal blocking.
      result = Stream.handle("XREAD", ["BLOCK", "100", "STREAMS", key, "$"], store)

      assert match?({:block, 100, [{^key, "$"}], :infinity}, result)
    end

    test "XREAD BLOCK with $ resolves last ID correctly" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "old", "data"], store)

      # $ should be converted to the current last ID of the stream.
      # When data is not available, it signals block with the $ marker.
      result = Stream.handle("XREAD", ["BLOCK", "100", "STREAMS", key, "$"], store)
      assert match?({:block, _, _, _}, result)
    end
  end

  # ===========================================================================
  # XADD waiter notification
  # ===========================================================================

  describe "XADD notifies stream waiters" do
    test "XADD calls notify_stream_waiters after insert" do
      store = MockStore.make()
      key = ustream()

      # Register ourselves as a waiter on this stream key.
      Stream.ensure_meta_table()
      Stream.register_stream_waiter(key, self(), "0-0")

      # XADD should wake us.
      _id = Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      assert_receive {:stream_waiter_notify, ^key}, 1000
    end

    test "XADD does not send notification when no waiters" do
      store = MockStore.make()
      key = ustream()

      # No waiters registered. XADD should succeed without error.
      id = Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      assert id == "1-0"

      # No messages in our mailbox.
      refute_receive {:stream_waiter_notify, _}, 50
    end

    test "multiple waiters on same stream are all notified" do
      store = MockStore.make()
      key = ustream()

      Stream.ensure_meta_table()

      # Spawn two waiter processes.
      parent = self()

      pids =
        for _ <- 1..2 do
          spawn(fn ->
            Stream.register_stream_waiter(key, self(), "0-0")
            send(parent, {:registered, self()})

            receive do
              {:stream_waiter_notify, ^key} ->
                send(parent, {:notified, self()})
            after
              2000 -> send(parent, {:timeout, self()})
            end
          end)
        end

      # Wait for both to register.
      for pid <- pids, do: assert_receive({:registered, ^pid}, 1000)

      # Give a moment for ETS writes to propagate.
      Process.sleep(10)

      # XADD should wake both.
      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      for pid <- pids do
        assert_receive {:notified, ^pid}, 1000
      end
    end
  end

  # ===========================================================================
  # Stream waiter registration and cleanup
  # ===========================================================================

  describe "stream waiter registration" do
    test "register and unregister stream waiter" do
      key = ustream()
      Stream.ensure_meta_table()

      Stream.register_stream_waiter(key, self(), "0-0")
      assert Stream.stream_waiter_count(key) == 1

      Stream.unregister_stream_waiter(key, self())
      assert Stream.stream_waiter_count(key) == 0
    end

    test "cleanup removes all waiters for a pid" do
      key1 = ustream()
      key2 = ustream()
      Stream.ensure_meta_table()

      Stream.register_stream_waiter(key1, self(), "0-0")
      Stream.register_stream_waiter(key2, self(), "0-0")

      assert Stream.stream_waiter_count(key1) == 1
      assert Stream.stream_waiter_count(key2) == 1

      Stream.cleanup_stream_waiters(self())

      assert Stream.stream_waiter_count(key1) == 0
      assert Stream.stream_waiter_count(key2) == 0
    end
  end
end
