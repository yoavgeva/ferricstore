defmodule Ferricstore.Commands.SlowLogTest do
  @moduledoc """
  Tests for the SLOWLOG command family.

  Tests both the SlowLog module directly and the Server command handlers
  that expose SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET, and SLOWLOG HELP.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.SlowLog
  alias Ferricstore.Test.MockStore

  setup do
    # Reset the slow log before each test to ensure isolation.
    SlowLog.reset()
    :ok
  end

  # ---------------------------------------------------------------------------
  # SlowLog module — direct API tests
  # ---------------------------------------------------------------------------

  describe "SlowLog module" do
    test "get returns empty list initially" do
      assert SlowLog.get() == []
    end

    test "len returns 0 initially" do
      assert SlowLog.len() == 0
    end

    test "reset returns :ok" do
      assert SlowLog.reset() == :ok
    end

    test "maybe_log records command when duration exceeds threshold" do
      # Set threshold to 0 so every command is logged.
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["SET", "key", "value"], 100)
      # Cast is async — give GenServer time to process.
      Process.sleep(50)

      assert SlowLog.len() == 1
      entries = SlowLog.get()
      assert length(entries) == 1
      [{id, timestamp_us, duration_us, command}] = entries
      assert id == 0
      assert is_integer(timestamp_us)
      assert duration_us == 100
      assert command == ["SET", "key", "value"]
    end

    test "maybe_log does not record when duration is below threshold" do
      # Default threshold is 10_000 us.
      SlowLog.maybe_log(["GET", "key"], 5)
      Process.sleep(50)

      assert SlowLog.len() == 0
    end

    test "maybe_log does not record when threshold is -1 (disabled)" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, -1)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["SET", "key", "value"], 999_999)
      Process.sleep(50)

      assert SlowLog.len() == 0
    end

    test "get with count limits results" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      for i <- 1..5 do
        SlowLog.maybe_log(["CMD_#{i}"], i * 100)
        Process.sleep(10)
      end

      Process.sleep(50)

      assert SlowLog.len() == 5
      assert length(SlowLog.get(2)) == 2
      assert length(SlowLog.get(0)) == 0
    end

    test "entries are returned newest first" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["FIRST"], 100)
      Process.sleep(10)
      SlowLog.maybe_log(["SECOND"], 200)
      Process.sleep(50)

      [{id1, _, _, cmd1}, {id2, _, _, cmd2}] = SlowLog.get()
      assert id1 > id2
      assert cmd1 == ["SECOND"]
      assert cmd2 == ["FIRST"]
    end

    test "ring buffer evicts oldest entries when max_len exceeded" do
      original_threshold = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      original_max = Application.get_env(:ferricstore, :slowlog_max_len)

      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)
      Application.put_env(:ferricstore, :slowlog_max_len, 3)

      on_exit(fn ->
        if original_threshold do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original_threshold)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end

        if original_max do
          Application.put_env(:ferricstore, :slowlog_max_len, original_max)
        else
          Application.delete_env(:ferricstore, :slowlog_max_len)
        end
      end)

      for i <- 1..5 do
        SlowLog.maybe_log(["CMD_#{i}"], i * 100)
        Process.sleep(10)
      end

      Process.sleep(50)

      assert SlowLog.len() == 3

      # The newest 3 entries should be retained.
      entries = SlowLog.get()
      commands = Enum.map(entries, fn {_, _, _, cmd} -> cmd end)
      assert ["CMD_5"] in commands
      assert ["CMD_4"] in commands
      assert ["CMD_3"] in commands
    end

    test "reset clears all entries and resets ID counter" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["OLD_CMD"], 100)
      Process.sleep(50)
      assert SlowLog.len() == 1

      SlowLog.reset()
      assert SlowLog.len() == 0
      assert SlowLog.get() == []

      # After reset, IDs restart from 0.
      SlowLog.maybe_log(["NEW_CMD"], 200)
      Process.sleep(50)

      [{id, _, _, _}] = SlowLog.get()
      assert id == 0
    end
  end

  # ---------------------------------------------------------------------------
  # SLOWLOG command handlers — via Server.handle/3
  # ---------------------------------------------------------------------------

  describe "SLOWLOG GET" do
    test "returns empty list initially" do
      result = Server.handle("SLOWLOG", ["GET"], MockStore.make())
      assert result == []
    end

    test "returns entries after slow operations are logged" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["SET", "mykey", "myval"], 15_000)
      Process.sleep(50)

      result = Server.handle("SLOWLOG", ["GET"], MockStore.make())
      assert length(result) == 1
      [[id, timestamp, duration, command]] = result
      assert is_integer(id)
      assert is_integer(timestamp)
      assert duration == 15_000
      assert command == ["SET", "mykey", "myval"]
    end

    test "with count limits results" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      for i <- 1..5 do
        SlowLog.maybe_log(["CMD_#{i}"], i * 100)
        Process.sleep(10)
      end

      Process.sleep(50)

      result = Server.handle("SLOWLOG", ["GET", "2"], MockStore.make())
      assert length(result) == 2
    end

    test "with invalid count returns error" do
      result = Server.handle("SLOWLOG", ["GET", "abc"], MockStore.make())
      assert {:error, _} = result
    end

    test "entry format is [id, timestamp, duration, command_array]" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["GET", "foo"], 42)
      Process.sleep(50)

      [[id, timestamp, duration, command]] =
        Server.handle("SLOWLOG", ["GET"], MockStore.make())

      assert is_integer(id) and id >= 0
      assert is_integer(timestamp) and timestamp > 0
      assert is_integer(duration) and duration == 42
      assert is_list(command) and command == ["GET", "foo"]
    end
  end

  describe "SLOWLOG LEN" do
    test "returns 0 initially" do
      assert 0 == Server.handle("SLOWLOG", ["LEN"], MockStore.make())
    end

    test "returns correct count after logging" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["A"], 10)
      SlowLog.maybe_log(["B"], 20)
      Process.sleep(50)

      assert 2 == Server.handle("SLOWLOG", ["LEN"], MockStore.make())
    end
  end

  describe "SLOWLOG RESET" do
    test "returns :ok" do
      assert :ok == Server.handle("SLOWLOG", ["RESET"], MockStore.make())
    end

    test "clears all entries" do
      original = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end
      end)

      SlowLog.maybe_log(["CMD"], 100)
      Process.sleep(50)
      assert SlowLog.len() > 0

      Server.handle("SLOWLOG", ["RESET"], MockStore.make())
      assert 0 == Server.handle("SLOWLOG", ["LEN"], MockStore.make())
    end
  end

  describe "SLOWLOG HELP" do
    test "returns list of help strings" do
      result = Server.handle("SLOWLOG", ["HELP"], MockStore.make())
      assert is_list(result)
      assert length(result) > 0
      assert Enum.all?(result, &is_binary/1)
    end
  end

  describe "SLOWLOG unknown subcommand" do
    test "returns error for unknown subcommand" do
      result = Server.handle("SLOWLOG", ["UNKNOWN"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end

    test "returns error for no subcommand" do
      result = Server.handle("SLOWLOG", [], MockStore.make())
      assert {:error, _} = result
    end
  end
end
