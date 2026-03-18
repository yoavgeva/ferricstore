defmodule Ferricstore.Commands.ConfigTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.Config
  alias Ferricstore.Test.MockStore

  # Reset config to defaults after each test to avoid cross-test contamination.
  setup do
    defaults = Config.defaults()
    on_exit(fn -> Enum.each(defaults, fn {k, v} -> Config.set(k, v) end) end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # CONFIG GET
  # ---------------------------------------------------------------------------

  describe "CONFIG GET" do
    test "CONFIG GET * returns all default config params" do
      result = Server.handle("CONFIG", ["GET", "*"], MockStore.make())
      assert is_list(result)
      # Result is a flat list [key, value, key, value, ...]
      assert rem(length(result), 2) == 0
      # Should contain known defaults
      assert "maxmemory" in result
      assert "hz" in result
      assert "requirepass" in result
    end

    test "CONFIG GET with exact key returns that key-value pair" do
      result = Server.handle("CONFIG", ["GET", "hz"], MockStore.make())
      assert result == ["hz", "10"]
    end

    test "CONFIG GET with pattern filters results" do
      result = Server.handle("CONFIG", ["GET", "max*"], MockStore.make())
      assert is_list(result)
      # Should include maxmemory, maxmemory-policy, maxclients
      keys = every_other(result, 0)
      assert "maxmemory" in keys
      assert "maxmemory-policy" in keys
      assert "maxclients" in keys
      # Should not include non-max keys
      refute "hz" in keys
      refute "port" in keys
    end

    test "CONFIG GET with non-matching pattern returns empty list" do
      result = Server.handle("CONFIG", ["GET", "nonexistent"], MockStore.make())
      assert result == []
    end

    test "CONFIG GET with ? wildcard matches single character" do
      result = Server.handle("CONFIG", ["GET", "h?"], MockStore.make())
      keys = every_other(result, 0)
      assert "hz" in keys
    end

    test "CONFIG GET with no args returns error" do
      result = Server.handle("CONFIG", ["GET"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG SET
  # ---------------------------------------------------------------------------

  describe "CONFIG SET" do
    test "CONFIG SET changes value and GET reflects it" do
      assert :ok = Server.handle("CONFIG", ["SET", "hz", "20"], MockStore.make())

      result = Server.handle("CONFIG", ["GET", "hz"], MockStore.make())
      assert result == ["hz", "20"]
    end

    test "CONFIG SET with new key adds it" do
      assert :ok = Server.handle("CONFIG", ["SET", "custom-param", "hello"], MockStore.make())

      result = Server.handle("CONFIG", ["GET", "custom-param"], MockStore.make())
      assert result == ["custom-param", "hello"]
    end

    test "CONFIG SET with no args returns error" do
      result = Server.handle("CONFIG", ["SET"], MockStore.make())
      assert {:error, _} = result
    end

    test "CONFIG SET with only key and no value returns error" do
      result = Server.handle("CONFIG", ["SET", "hz"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG RESETSTAT
  # ---------------------------------------------------------------------------

  describe "CONFIG RESETSTAT" do
    test "CONFIG RESETSTAT returns OK" do
      assert :ok = Server.handle("CONFIG", ["RESETSTAT"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG REWRITE
  # ---------------------------------------------------------------------------

  describe "CONFIG REWRITE" do
    test "CONFIG REWRITE returns OK" do
      assert :ok = Server.handle("CONFIG", ["REWRITE"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG unknown subcommand
  # ---------------------------------------------------------------------------

  describe "CONFIG unknown" do
    test "unknown CONFIG subcommand returns error" do
      result = Server.handle("CONFIG", ["BADSUBCMD"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end

    test "CONFIG with no args returns error" do
      result = Server.handle("CONFIG", [], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Config GenServer direct API
  # ---------------------------------------------------------------------------

  describe "Config GenServer API" do
    test "get/1 returns matching pairs" do
      pairs = Config.get("hz")
      assert pairs == [{"hz", "10"}]
    end

    test "set/2 updates a value" do
      assert :ok = Config.set("hz", "50")
      assert [{"hz", "50"}] = Config.get("hz")
    end

    test "get_value/1 returns single value" do
      assert "10" = Config.get_value("hz")
    end

    test "get_value/1 returns nil for unknown key" do
      assert nil == Config.get_value("totally_unknown_key")
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Extract every other element from a flat list starting at the given offset.
  defp every_other(list, offset) do
    list
    |> Enum.drop(offset)
    |> Enum.take_every(2)
  end
end
