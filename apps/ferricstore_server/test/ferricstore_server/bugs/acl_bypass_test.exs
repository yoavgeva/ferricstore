defmodule FerricstoreServer.Bugs.AclBypassTest do
  @moduledoc """
  Regression test for Bug #3: READMODE and SANDBOX bypass ACL checks.

  The bug doc claims READMODE/SANDBOX fall through to the `true` catch-all
  in the handle_command cond, bypassing ACL. Investigation shows this is
  incorrect — READMODE/SANDBOX correctly go through the ACL check branch
  because `cmd not in @acl_bypass_cmds` evaluates to true for them.

  However, a RELATED bypass exists: READMODE and SANDBOX have specific
  dispatch clauses (connection.ex:440-441) that match BEFORE the pubsub
  mode check (connection.ex:469-477). This means they can be executed
  while the connection is in pubsub mode, bypassing the pubsub restriction
  that should limit commands to SUBSCRIBE/UNSUBSCRIBE/PING/QUIT/RESET.

  File: apps/ferricstore_server/lib/ferricstore_server/connection.ex:440,469
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 30_000

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # TCP helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
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

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    :ets.delete_all_objects(:ferricstore_pubsub)
    :ets.delete_all_objects(:ferricstore_pubsub_patterns)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "READMODE should be blocked in pubsub mode" do
    test "READMODE STALE is rejected when connection is in pubsub mode", %{port: port} do
      sock = connect_and_hello(port)

      # Enter pubsub mode
      send_cmd(sock, ["SUBSCRIBE", "test_channel"])
      resp = recv_response(sock)
      assert {:push, ["subscribe", "test_channel", 1]} = resp

      # Now try READMODE — should be rejected because we're in pubsub mode.
      # Only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/QUIT/RESET
      # should be allowed in pubsub mode.
      send_cmd(sock, ["READMODE", "STALE"])
      resp = recv_response(sock)

      # Bug: READMODE has a specific dispatch clause that matches BEFORE the
      # pubsub mode check, so it succeeds instead of being rejected.
      assert match?({:error, "ERR Can't execute" <> _}, resp),
        "READMODE should be rejected in pubsub mode, but got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end

    test "SANDBOX is rejected when connection is in pubsub mode", %{port: port} do
      sock = connect_and_hello(port)

      # Enter pubsub mode
      send_cmd(sock, ["SUBSCRIBE", "test_channel2"])
      resp = recv_response(sock)
      assert {:push, ["subscribe", "test_channel2", 1]} = resp

      # SANDBOX should be rejected in pubsub mode
      send_cmd(sock, ["SANDBOX", "START"])
      resp = recv_response(sock)

      # Bug: SANDBOX has a specific dispatch clause that matches BEFORE the
      # pubsub mode check, so it attempts to execute instead of being rejected.
      # It may return a SANDBOX-specific error, but should return pubsub error first.
      refute match?({:simple, "OK"}, resp) or match?({:error, "ERR SANDBOX" <> _}, resp),
        "SANDBOX should be rejected with pubsub mode error, but got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end
  end
end
