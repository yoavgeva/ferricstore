defmodule Ferricstore.Commands.SandboxTest do
  @moduledoc """
  Tests for SANDBOX commands: START, JOIN, END, TOKEN.

  Verifies namespace isolation, key prefixing, cleanup on END, and
  config gating (disabled by default).
  """
  use ExUnit.Case, async: false
  alias Ferricstore.Config
  alias Ferricstore.Resp.{Encoder, Parser}
  alias Ferricstore.Server.Listener

  # Reset sandbox_mode after each test.
  setup do
    on_exit(fn -> Config.set("sandbox_mode", "disabled") end)
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

  defp connect(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    # Do HELLO handshake.
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # SANDBOX disabled by default returns error
  # ---------------------------------------------------------------------------

  describe "SANDBOX disabled by default returns error" do
    test "SANDBOX START returns error when disabled", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "SANDBOX commands are not enabled"

      :gen_tcp.close(sock)
    end

    test "SANDBOX JOIN returns error when disabled", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "JOIN", "some_token"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "SANDBOX commands are not enabled"

      :gen_tcp.close(sock)
    end

    test "SANDBOX END returns error when disabled", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "END"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "SANDBOX commands are not enabled"

      :gen_tcp.close(sock)
    end

    test "SANDBOX TOKEN returns error when disabled", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "TOKEN"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "SANDBOX commands are not enabled"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SANDBOX START returns namespace
  # ---------------------------------------------------------------------------

  describe "SANDBOX START returns namespace" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "returns a test_ prefixed namespace", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      namespace = recv_response(sock)

      assert is_binary(namespace)
      assert String.starts_with?(namespace, "test_")
      # 8 random bytes = 16 hex chars.
      assert String.length(namespace) == 5 + 16

      # Clean up.
      send_cmd(sock, ["SANDBOX", "END"])
      _ok = recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "each START generates a unique namespace", %{port: port} do
      sock1 = connect(port)
      sock2 = connect(port)

      send_cmd(sock1, ["SANDBOX", "START"])
      ns1 = recv_response(sock1)

      send_cmd(sock2, ["SANDBOX", "START"])
      ns2 = recv_response(sock2)

      refute ns1 == ns2

      # Clean up.
      send_cmd(sock1, ["SANDBOX", "END"])
      recv_response(sock1)
      send_cmd(sock2, ["SANDBOX", "END"])
      recv_response(sock2)

      :gen_tcp.close(sock1)
      :gen_tcp.close(sock2)
    end
  end

  # ---------------------------------------------------------------------------
  # Keys written in sandbox are prefixed
  # ---------------------------------------------------------------------------

  describe "keys written in sandbox are prefixed" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "SET in sandbox prefixes the key", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      namespace = recv_response(sock)

      # Write a key inside the sandbox.
      send_cmd(sock, ["SET", "mykey", "myvalue"])
      assert {:simple, "OK"} = recv_response(sock)

      # Read it back inside sandbox.
      send_cmd(sock, ["GET", "mykey"])
      assert "myvalue" = recv_response(sock)

      # The actual key in the store should be prefixed.
      actual = Ferricstore.Store.Router.get(namespace <> "mykey")
      assert actual == "myvalue"

      # A non-sandbox connection should not see "mykey" without prefix.
      sock2 = connect(port)
      send_cmd(sock2, ["GET", "mykey"])
      assert nil == recv_response(sock2)

      # Clean up.
      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end

    test "EXISTS in sandbox checks prefixed key", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      _ns = recv_response(sock)

      send_cmd(sock, ["SET", "ex_key", "v"])
      recv_response(sock)

      send_cmd(sock, ["EXISTS", "ex_key"])
      assert 1 = recv_response(sock)

      send_cmd(sock, ["EXISTS", "nonexistent"])
      assert 0 = recv_response(sock)

      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "DEL in sandbox deletes prefixed key", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      _ns = recv_response(sock)

      send_cmd(sock, ["SET", "del_key", "v"])
      recv_response(sock)

      send_cmd(sock, ["DEL", "del_key"])
      assert 1 = recv_response(sock)

      send_cmd(sock, ["GET", "del_key"])
      assert nil == recv_response(sock)

      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SANDBOX END clears sandbox keys
  # ---------------------------------------------------------------------------

  describe "SANDBOX END clears sandbox keys" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "END removes all keys with namespace prefix", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      namespace = recv_response(sock)

      # Write several keys.
      send_cmd(sock, ["SET", "key1", "v1"])
      recv_response(sock)
      send_cmd(sock, ["SET", "key2", "v2"])
      recv_response(sock)
      send_cmd(sock, ["SET", "key3", "v3"])
      recv_response(sock)

      # Verify they exist in the underlying store.
      assert Ferricstore.Store.Router.get(namespace <> "key1") == "v1"
      assert Ferricstore.Store.Router.get(namespace <> "key2") == "v2"
      assert Ferricstore.Store.Router.get(namespace <> "key3") == "v3"

      # End the sandbox.
      send_cmd(sock, ["SANDBOX", "END"])
      assert {:simple, "OK"} = recv_response(sock)

      # All sandbox keys should be gone.
      assert Ferricstore.Store.Router.get(namespace <> "key1") == nil
      assert Ferricstore.Store.Router.get(namespace <> "key2") == nil
      assert Ferricstore.Store.Router.get(namespace <> "key3") == nil

      :gen_tcp.close(sock)
    end

    test "END without active sandbox returns error", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "END"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "no active sandbox"

      :gen_tcp.close(sock)
    end

    test "after END, keys are no longer prefixed", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      _ns = recv_response(sock)

      send_cmd(sock, ["SET", "temp", "sandboxed"])
      recv_response(sock)

      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      # Now SET should write to the real namespace.
      key = "after_end_#{:rand.uniform(9_999_999)}"
      send_cmd(sock, ["SET", key, "real"])
      recv_response(sock)

      assert Ferricstore.Store.Router.get(key) == "real"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SANDBOX JOIN shares namespace
  # ---------------------------------------------------------------------------

  describe "SANDBOX JOIN shares namespace" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "JOIN lets another connection share the same namespace", %{port: port} do
      sock1 = connect(port)
      sock2 = connect(port)

      # sock1 starts a sandbox.
      send_cmd(sock1, ["SANDBOX", "START"])
      namespace = recv_response(sock1)

      # sock1 writes a key.
      send_cmd(sock1, ["SET", "shared_key", "shared_value"])
      recv_response(sock1)

      # sock2 joins the sandbox.
      send_cmd(sock2, ["SANDBOX", "JOIN", namespace])
      assert {:simple, "OK"} = recv_response(sock2)

      # sock2 should see the key.
      send_cmd(sock2, ["GET", "shared_key"])
      assert "shared_value" = recv_response(sock2)

      # sock2 writes a key.
      send_cmd(sock2, ["SET", "from_sock2", "val2"])
      recv_response(sock2)

      # sock1 should see it.
      send_cmd(sock1, ["GET", "from_sock2"])
      assert "val2" = recv_response(sock1)

      # Clean up.
      send_cmd(sock1, ["SANDBOX", "END"])
      recv_response(sock1)

      :gen_tcp.close(sock1)
      :gen_tcp.close(sock2)
    end
  end

  # ---------------------------------------------------------------------------
  # SANDBOX TOKEN returns current namespace
  # ---------------------------------------------------------------------------

  describe "SANDBOX TOKEN returns current namespace" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "TOKEN returns nil when no sandbox active", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "TOKEN"])
      assert nil == recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "TOKEN returns namespace when sandbox is active", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      namespace = recv_response(sock)

      send_cmd(sock, ["SANDBOX", "TOKEN"])
      token = recv_response(sock)

      assert token == namespace

      # Clean up.
      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "TOKEN returns nil after SANDBOX END", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      _ns = recv_response(sock)

      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      send_cmd(sock, ["SANDBOX", "TOKEN"])
      assert nil == recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # sandbox_mode "enabled" also works
  # ---------------------------------------------------------------------------

  describe "sandbox_mode enabled" do
    setup %{port: port} do
      Config.set("sandbox_mode", "enabled")
      %{port: port}
    end

    test "SANDBOX START works when mode is 'enabled'", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      namespace = recv_response(sock)

      assert is_binary(namespace)
      assert String.starts_with?(namespace, "test_")

      send_cmd(sock, ["SANDBOX", "END"])
      recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SANDBOX with no subcommand
  # ---------------------------------------------------------------------------

  describe "SANDBOX with no subcommand" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "returns unknown SANDBOX subcommand error", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "unknown SANDBOX subcommand"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # RESET clears sandbox namespace
  # ---------------------------------------------------------------------------

  describe "RESET clears sandbox namespace" do
    setup %{port: port} do
      Config.set("sandbox_mode", "local")
      %{port: port}
    end

    test "RESET clears the sandbox namespace", %{port: port} do
      sock = connect(port)

      send_cmd(sock, ["SANDBOX", "START"])
      _ns = recv_response(sock)

      send_cmd(sock, ["SANDBOX", "TOKEN"])
      token = recv_response(sock)
      assert token != nil

      send_cmd(sock, ["RESET"])
      recv_response(sock)

      send_cmd(sock, ["SANDBOX", "TOKEN"])
      assert nil == recv_response(sock)

      :gen_tcp.close(sock)
    end
  end
end
