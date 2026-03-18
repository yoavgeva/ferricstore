defmodule Ferricstore.Commands.AuthTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Config
  alias Ferricstore.Resp.{Encoder, Parser}
  alias Ferricstore.Server.Listener

  # Reset requirepass after each test to avoid contaminating other tests.
  setup do
    on_exit(fn -> Config.set("requirepass", "") end)
    %{port: Listener.port()}
  end

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
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_raw(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    sock
  end

  defp connect_and_hello(port) do
    sock = connect_raw(port)
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # AUTH without requirepass
  # ---------------------------------------------------------------------------

  describe "AUTH without requirepass set" do
    test "AUTH returns error when no password is configured", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "somepassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "no password is set"

      :gen_tcp.close(sock)
    end

    test "AUTH with username returns error when no password is configured", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "somepassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "no password is set"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # AUTH with requirepass
  # ---------------------------------------------------------------------------

  describe "AUTH with requirepass set" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "AUTH with correct password returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "AUTH with wrong password returns WRONGPASS error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "wrongpassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "AUTH with username and correct password returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "AUTH with username and wrong password returns WRONGPASS", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "bad"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "commands are rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "key", "value"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "GET is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", "key"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "PING is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "HELLO is allowed before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["HELLO", "3"])
      response = recv_response(sock)

      # HELLO returns the greeting map, not an error
      assert is_map(response)
      assert response["server"] == "ferricstore"

      :gen_tcp.close(sock)
    end

    test "QUIT is allowed before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["QUIT"])
      response = recv_response(sock)

      assert response == {:simple, "OK"}
      # Socket should be closed by the server
    end

    test "commands work after successful AUTH", %{port: port} do
      sock = connect_and_hello(port)

      # Authenticate
      send_cmd(sock, ["AUTH", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      # Now commands should work
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      send_cmd(sock, ["SET", "auth_test_key", "value"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "auth_test_key"])
      assert recv_response(sock) == "value"

      :gen_tcp.close(sock)
    end

    test "AUTH with no args returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL WHOAMI
  # ---------------------------------------------------------------------------

  describe "ACL WHOAMI" do
    test "ACL WHOAMI returns 'default'", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "WHOAMI"])
      assert recv_response(sock) == "default"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LIST
  # ---------------------------------------------------------------------------

  describe "ACL LIST" do
    test "ACL LIST returns default user rule", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "LIST"])
      response = recv_response(sock)

      assert [rule] = response
      assert rule =~ "user default"
      assert rule =~ "+@all"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL GETUSER
  # ---------------------------------------------------------------------------

  describe "ACL GETUSER" do
    test "ACL GETUSER default returns user info", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "GETUSER", "default"])
      response = recv_response(sock)

      assert is_list(response)
      assert "flags" in response
      assert "commands" in response

      :gen_tcp.close(sock)
    end

    test "ACL GETUSER unknown user returns nil", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "GETUSER", "unknownuser"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL CAT
  # ---------------------------------------------------------------------------

  describe "ACL CAT" do
    test "ACL CAT returns category list", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "CAT"])
      response = recv_response(sock)

      assert is_list(response)
      assert length(response) > 0
      assert "read" in response
      assert "write" in response
      assert "admin" in response

      :gen_tcp.close(sock)
    end

    test "ACL CAT with specific category returns list", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "CAT", "read"])
      response = recv_response(sock)

      assert is_list(response)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LOG
  # ---------------------------------------------------------------------------

  describe "ACL LOG" do
    test "ACL LOG returns empty list after reset", %{port: port} do
      sock = connect_and_hello(port)

      # Reset first to clear any entries from prior tests
      send_cmd(sock, ["ACL", "LOG", "RESET"])
      recv_response(sock)

      send_cmd(sock, ["ACL", "LOG"])
      assert recv_response(sock) == []

      :gen_tcp.close(sock)
    end

    test "ACL LOG RESET returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "LOG", "RESET"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "ACL LOG with count returns empty list after reset", %{port: port} do
      sock = connect_and_hello(port)

      # Reset first to clear any entries from prior tests
      send_cmd(sock, ["ACL", "LOG", "RESET"])
      recv_response(sock)

      send_cmd(sock, ["ACL", "LOG", "10"])
      assert recv_response(sock) == []

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL unknown subcommand
  # ---------------------------------------------------------------------------

  describe "ACL unknown" do
    test "ACL with unknown subcommand returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "BADSUBCMD"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "unknown subcommand"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL gated by auth
  # ---------------------------------------------------------------------------

  describe "ACL gated by authentication" do
    test "ACL is rejected before authentication when requirepass is set", %{port: port} do
      Config.set("requirepass", "secret123")
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "WHOAMI"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end
  end
end
