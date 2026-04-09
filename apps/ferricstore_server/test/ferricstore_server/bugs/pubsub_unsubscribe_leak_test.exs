defmodule FerricstoreServer.Bugs.PubsubUnsubscribeLeakTest do
  @moduledoc """
  Regression test for Bug #2: UNSUBSCRIBE with no args only clears
  `pubsub_channels`, not `pubsub_patterns`. The connection stays in pubsub
  mode permanently because `in_pubsub_mode?` still sees non-empty patterns.

  Same bug in reverse: PUNSUBSCRIBE with no args doesn't clear channels.

  File: apps/ferricstore_server/lib/ferricstore_server/connection/pubsub.ex:50-56
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

  describe "UNSUBSCRIBE with no args should clear pattern subscriptions too" do
    test "UNSUBSCRIBE (no args) after SUBSCRIBE + PSUBSCRIBE allows PING", %{port: port} do
      sock = connect_and_hello(port)

      # 1. SUBSCRIBE foo
      send_cmd(sock, ["SUBSCRIBE", "foo"])
      # subscribe confirmation: {:push, ["subscribe", "foo", 1]}
      resp = recv_response(sock)
      assert {:push, ["subscribe", "foo", 1]} = resp

      # 2. PSUBSCRIBE bar*
      send_cmd(sock, ["PSUBSCRIBE", "bar*"])
      # psubscribe confirmation: {:push, ["psubscribe", "bar*", 2]}
      resp = recv_response(sock)
      assert {:push, ["psubscribe", "bar*", 2]} = resp

      # 3. UNSUBSCRIBE (no args) — should unsubscribe from ALL (channels AND patterns)
      send_cmd(sock, ["UNSUBSCRIBE"])
      # We get unsubscribe confirmation for "foo"
      resp = recv_response(sock)
      assert {:push, ["unsubscribe", "foo", _count]} = resp

      # 4. PUNSUBSCRIBE (no args) — should also clean up patterns
      #    But if the bug exists, UNSUBSCRIBE should have already cleared patterns.
      #    We skip this to test the actual bug: UNSUBSCRIBE alone doesn't clear patterns.

      # 5. PING — should succeed if we're out of pubsub mode.
      #    If the bug exists, patterns still remain and PING returns an error
      #    because the connection thinks it's still in pubsub mode.
      send_cmd(sock, ["PING"])
      _ping_resp = recv_response(sock)

      # PING is allowed in pubsub mode (special-cased), so it always works.
      # The real test is whether we can run a non-pubsub command.
      # GET is NOT allowed in pubsub mode, so it will fail if we're still in it.
      send_cmd(sock, ["GET", "somekey"])
      resp2 = recv_response(sock)

      # If still in pubsub mode, GET returns an error about pubsub context.
      # If correctly exited pubsub mode, GET returns nil (bulk nil).
      refute match?({:error, "ERR Can't execute" <> _}, resp2),
        "GET should work after UNSUBSCRIBE clears all subscriptions, " <>
        "but pattern subscriptions leaked and connection is still in pubsub mode"

      :gen_tcp.close(sock)
    end

    test "PUNSUBSCRIBE (no args) after SUBSCRIBE + PSUBSCRIBE leaves channels leaked", %{port: port} do
      sock = connect_and_hello(port)

      # 1. SUBSCRIBE foo
      send_cmd(sock, ["SUBSCRIBE", "foo"])
      resp = recv_response(sock)
      assert {:push, ["subscribe", "foo", 1]} = resp

      # 2. PSUBSCRIBE bar*
      send_cmd(sock, ["PSUBSCRIBE", "bar*"])
      resp = recv_response(sock)
      assert {:push, ["psubscribe", "bar*", 2]} = resp

      # 3. PUNSUBSCRIBE (no args) — only clears patterns, not channels
      send_cmd(sock, ["PUNSUBSCRIBE"])
      resp = recv_response(sock)
      assert {:push, ["punsubscribe", "bar*", _count]} = resp

      # 4. UNSUBSCRIBE (no args) — now clear channels too
      send_cmd(sock, ["UNSUBSCRIBE"])
      resp = recv_response(sock)
      assert {:push, ["unsubscribe", "foo", 0]} = resp

      # 5. Now we should be fully out of pubsub mode
      send_cmd(sock, ["GET", "somekey"])
      resp2 = recv_response(sock)

      refute match?({:error, "ERR Can't execute" <> _}, resp2),
        "GET should work after clearing both channels and patterns"

      :gen_tcp.close(sock)
    end
  end
end
