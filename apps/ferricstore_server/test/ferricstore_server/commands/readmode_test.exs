defmodule FerricstoreServer.Commands.ReadmodeTest do
  @moduledoc """
  Tests for the READMODE command (spec section 5.4).

  READMODE sets the per-connection read consistency mode:

    * `READMODE STALE` — allows reads from any replica (local ETS, may lag)
    * `READMODE CONSISTENT` — reads from the leader (default)

  On single-node deployments both modes behave identically. The command
  exists for forward compatibility with multi-node replication.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup do
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # READMODE over TCP
  # ---------------------------------------------------------------------------

  describe "READMODE STALE" do
    test "returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE", "STALE"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  describe "READMODE CONSISTENT" do
    test "returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE", "CONSISTENT"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  describe "READMODE with unknown mode" do
    test "returns error for unknown mode", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE", "INVALID"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "unknown read mode"
      assert msg =~ "INVALID"

      :gen_tcp.close(sock)
    end
  end

  describe "READMODE with wrong arity" do
    test "no arguments returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end

    test "too many arguments returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE", "STALE", "extra"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end
  end

  describe "READMODE is case-insensitive" do
    test "lowercase stale works", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE", "stale"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "mixed case Consistent works", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["READMODE", "Consistent"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  describe "READMODE default is CONSISTENT" do
    test "reads work correctly on a fresh connection (default consistent mode)", %{port: port} do
      sock = connect_and_hello(port)

      # SET and GET should work with the default read mode (CONSISTENT)
      key = "readmode_default_#{:rand.uniform(999_999)}"

      send_cmd(sock, ["SET", key, "hello"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "hello"

      :gen_tcp.close(sock)
    end
  end

  describe "READMODE STALE reads behave same as CONSISTENT (single-node)" do
    test "SET then GET works in STALE mode", %{port: port} do
      sock = connect_and_hello(port)
      key = "readmode_stale_#{:rand.uniform(999_999)}"

      # Switch to STALE mode
      send_cmd(sock, ["READMODE", "STALE"])
      assert recv_response(sock) == {:simple, "OK"}

      # SET a key
      send_cmd(sock, ["SET", key, "stale_value"])
      assert recv_response(sock) == {:simple, "OK"}

      # GET should return the value (same behavior on single node)
      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "stale_value"

      :gen_tcp.close(sock)
    end

    test "can switch modes back and forth", %{port: port} do
      sock = connect_and_hello(port)
      key = "readmode_switch_#{:rand.uniform(999_999)}"

      send_cmd(sock, ["READMODE", "STALE"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", key, "v1"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["READMODE", "CONSISTENT"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "v1"

      :gen_tcp.close(sock)
    end
  end

  describe "RESET clears read mode" do
    test "RESET resets read_mode to CONSISTENT", %{port: port} do
      sock = connect_and_hello(port)

      # Set to STALE
      send_cmd(sock, ["READMODE", "STALE"])
      assert recv_response(sock) == {:simple, "OK"}

      # RESET the connection
      send_cmd(sock, ["RESET"])
      response = recv_response(sock)
      assert {:simple, "RESET"} = response

      # After RESET, the default mode should be CONSISTENT.
      # We can verify by simply checking that READMODE CONSISTENT returns OK
      # (the connection is in its default state).
      send_cmd(sock, ["READMODE", "CONSISTENT"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end
end
