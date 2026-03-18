defmodule Ferricstore.Commands.ClientTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Client
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp make_conn_state(overrides \\ %{}) do
    Map.merge(
      %{
        client_id: 42,
        client_name: nil,
        created_at: System.monotonic_time(:millisecond) - 5_000,
        peer: {{127, 0, 0, 1}, 12345}
      },
      overrides
    )
  end

  defp store, do: MockStore.make()

  # ---------------------------------------------------------------------------
  # CLIENT ID
  # ---------------------------------------------------------------------------

  describe "CLIENT ID" do
    test "returns the client_id as an integer" do
      conn = make_conn_state(%{client_id: 99})
      {result, _conn} = Client.handle("ID", [], conn, store())
      assert result == 99
      assert is_integer(result)
    end

    test "does not modify connection state" do
      conn = make_conn_state()
      {_result, updated} = Client.handle("ID", [], conn, store())
      assert updated == conn
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("ID", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT SETNAME / GETNAME round trip
  # ---------------------------------------------------------------------------

  describe "CLIENT SETNAME" do
    test "sets the client name and returns :ok" do
      conn = make_conn_state()
      {result, updated} = Client.handle("SETNAME", ["myconn"], conn, store())
      assert result == :ok
      assert updated.client_name == "myconn"
    end

    test "rejects names with spaces" do
      conn = make_conn_state()
      {{:error, msg}, _updated} = Client.handle("SETNAME", ["my conn"], conn, store())
      assert msg =~ "cannot contain spaces"
    end

    test "with no args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("SETNAME", [], conn, store())
      assert msg =~ "wrong number of arguments"
    end

    test "with too many args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("SETNAME", ["a", "b"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "CLIENT GETNAME" do
    test "returns nil when no name is set" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("GETNAME", [], conn, store())
      assert result == nil
    end

    test "returns the name after SETNAME" do
      conn = make_conn_state()
      {:ok, conn2} = Client.handle("SETNAME", ["myconn"], conn, store())
      {result, _conn3} = Client.handle("GETNAME", [], conn2, store())
      assert result == "myconn"
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("GETNAME", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "CLIENT SETNAME / GETNAME round trip" do
    test "set then get returns the set name" do
      conn = make_conn_state()
      {:ok, conn2} = Client.handle("SETNAME", ["test-connection"], conn, store())
      {name, _conn3} = Client.handle("GETNAME", [], conn2, store())
      assert name == "test-connection"
    end

    test "can overwrite a previously set name" do
      conn = make_conn_state()
      {:ok, conn2} = Client.handle("SETNAME", ["first"], conn, store())
      {:ok, conn3} = Client.handle("SETNAME", ["second"], conn2, store())
      {name, _conn4} = Client.handle("GETNAME", [], conn3, store())
      assert name == "second"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT INFO
  # ---------------------------------------------------------------------------

  describe "CLIENT INFO" do
    test "returns a formatted string with connection details" do
      conn = make_conn_state(%{client_id: 7, client_name: nil})
      {result, _conn} = Client.handle("INFO", [], conn, store())

      assert is_binary(result)
      assert result =~ "id=7"
      assert result =~ "addr=127.0.0.1:12345"
      assert result =~ "name="
      assert result =~ "age="
    end

    test "includes client name when set" do
      conn = make_conn_state(%{client_name: "myconn"})
      {result, _conn} = Client.handle("INFO", [], conn, store())
      assert result =~ "name=myconn"
    end

    test "age reflects elapsed time" do
      # Connection created 10 seconds ago
      conn = make_conn_state(%{created_at: System.monotonic_time(:millisecond) - 10_000})
      {result, _conn} = Client.handle("INFO", [], conn, store())

      # Extract age value
      case Regex.run(~r/age=(\d+)/, result) do
        [_, age_str] ->
          {age, ""} = Integer.parse(age_str)
          assert age >= 9 and age <= 12

        nil ->
          flunk("Expected age= in CLIENT INFO output")
      end
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("INFO", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT LIST
  # ---------------------------------------------------------------------------

  describe "CLIENT LIST" do
    test "returns info for current connection" do
      conn = make_conn_state(%{client_id: 3})
      {result, _conn} = Client.handle("LIST", [], conn, store())

      assert is_binary(result)
      assert result =~ "id=3"
    end

    test "TYPE normal returns info" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "normal"], conn, store())
      assert is_binary(result)
      assert result =~ "id="
    end

    test "TYPE master returns empty string" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "master"], conn, store())
      assert result == ""
    end

    test "TYPE replica returns empty string" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "replica"], conn, store())
      assert result == ""
    end

    test "TYPE pubsub returns empty string" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "pubsub"], conn, store())
      assert result == ""
    end
  end

  # ---------------------------------------------------------------------------
  # Unknown subcommand
  # ---------------------------------------------------------------------------

  describe "CLIENT unknown subcommand" do
    test "returns error with subcommand name" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("BOGUS", [], conn, store())
      assert msg =~ "unknown subcommand"
      assert msg =~ "BOGUS"
    end
  end
end
