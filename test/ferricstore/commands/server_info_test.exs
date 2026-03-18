defmodule Ferricstore.Commands.ServerInfoTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # INFO — default (all sections)
  # ---------------------------------------------------------------------------

  describe "INFO (all sections)" do
    test "INFO with no args returns all sections" do
      store = MockStore.make(%{"k1" => {"v1", 0}, "k2" => {"v2", 0}})
      result = Server.handle("INFO", [], store)

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "# Clients"
      assert result =~ "# Memory"
      assert result =~ "# Keyspace"
      assert result =~ "# Stats"
    end

    test "INFO all returns all sections explicitly" do
      store = MockStore.make()
      result = Server.handle("INFO", ["all"], store)

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "# Clients"
      assert result =~ "# Memory"
      assert result =~ "# Keyspace"
      assert result =~ "# Stats"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO server
  # ---------------------------------------------------------------------------

  describe "INFO server" do
    test "returns server section with expected fields" do
      result = Server.handle("INFO", ["server"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "redis_version:7.4.0"
      assert result =~ "ferricstore_version:0.1.0"
      assert result =~ "tcp_port:"
      assert result =~ "uptime_in_seconds:"
      assert result =~ "process_id:"
      assert result =~ "run_id:"
    end

    test "run_id is a 40-char hex string" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      run_id_line = Enum.find(fields, &String.starts_with?(&1, "run_id:"))
      assert run_id_line != nil

      run_id = String.trim_leading(run_id_line, "run_id:")
      assert byte_size(run_id) == 40
      assert Regex.match?(~r/^[0-9a-f]+$/, run_id)
    end

    test "uptime_in_seconds is a non-negative integer" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      uptime_line = Enum.find(fields, &String.starts_with?(&1, "uptime_in_seconds:"))
      uptime_str = String.trim_leading(uptime_line, "uptime_in_seconds:")
      {uptime, ""} = Integer.parse(uptime_str)
      assert uptime >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # INFO memory
  # ---------------------------------------------------------------------------

  describe "INFO memory" do
    test "returns memory section with used_memory and used_memory_human" do
      result = Server.handle("INFO", ["memory"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Memory"
      assert result =~ "used_memory:"
      assert result =~ "used_memory_human:"
    end

    test "used_memory is a positive integer" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      mem_line = Enum.find(fields, &String.starts_with?(&1, "used_memory:"))
      mem_str = String.trim_leading(mem_line, "used_memory:")
      {mem, ""} = Integer.parse(mem_str)
      assert mem > 0
    end

    test "used_memory_human contains a size suffix" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      human_line = Enum.find(fields, &String.starts_with?(&1, "used_memory_human:"))
      human_str = String.trim_leading(human_line, "used_memory_human:")
      # Should end with K, M, G, or B
      assert Regex.match?(~r/[KMGB]$/, human_str)
    end
  end

  # ---------------------------------------------------------------------------
  # INFO keyspace
  # ---------------------------------------------------------------------------

  describe "INFO keyspace" do
    test "reflects actual key count" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}, "c" => {"3", 0}})
      result = Server.handle("INFO", ["keyspace"], store)

      assert is_binary(result)
      assert result =~ "# Keyspace"
      assert result =~ "keys=3"
    end

    test "empty store shows keys=0" do
      store = MockStore.make()
      result = Server.handle("INFO", ["keyspace"], store)

      assert result =~ "keys=0"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO clients
  # ---------------------------------------------------------------------------

  describe "INFO clients" do
    test "returns clients section with connected_clients field" do
      result = Server.handle("INFO", ["clients"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Clients"
      assert result =~ "connected_clients:"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO stats
  # ---------------------------------------------------------------------------

  describe "INFO stats" do
    test "returns stats section with counter fields" do
      result = Server.handle("INFO", ["stats"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Stats"
      assert result =~ "total_connections_received:"
      assert result =~ "total_commands_processed:"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO with unknown section
  # ---------------------------------------------------------------------------

  describe "INFO unknown section" do
    test "returns empty string for unknown section" do
      result = Server.handle("INFO", ["nonexistent"], MockStore.make())
      assert result == ""
    end
  end

  # ---------------------------------------------------------------------------
  # INFO case insensitivity
  # ---------------------------------------------------------------------------

  describe "INFO section case insensitivity" do
    test "INFO SERVER (uppercase) returns server section" do
      result = Server.handle("INFO", ["SERVER"], MockStore.make())
      assert result =~ "# Server"
    end

    test "INFO Memory (mixed case) returns memory section" do
      result = Server.handle("INFO", ["Memory"], MockStore.make())
      assert result =~ "# Memory"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO with too many args
  # ---------------------------------------------------------------------------

  describe "INFO error cases" do
    test "INFO with two section args returns error" do
      assert {:error, _} = Server.handle("INFO", ["server", "memory"], MockStore.make())
    end
  end
end
