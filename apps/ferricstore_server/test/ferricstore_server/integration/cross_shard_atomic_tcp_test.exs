defmodule FerricstoreServer.Integration.CrossShardAtomicTcpTest do
  @moduledoc """
  TCP-level integration tests for cross-shard atomic MULTI/EXEC transactions.

  These tests verify the NEW behavior over real TCP sockets with RESP3:
  cross-shard MULTI/EXEC should succeed atomically (not return CROSSSLOT).
  The anchor shard approach submits a single Raft log entry containing
  commands for ALL involved shards. One Raft entry = one fsync = atomic.

  These tests are expected to FAIL until the cross-shard atomic transaction
  implementation is complete.

  Key-to-shard mapping (slot-based, 4 shards, contiguous 256-slot ranges):
    - shard 0: "j", "t", "u", "y", "z"
    - shard 1: "g", "k", "l", "r"
    - shard 2: "a", "c", "e", "n", "p", "v", "w"
    - shard 3: "b", "d", "f", "h", "i", "m", "o", "q", "s", "x"

  RESP3 encoding note: simple strings like "OK" and "QUEUED" are returned
  as `{:simple, "OK"}` and `{:simple, "QUEUED"}` by the parser. Inside
  EXEC result arrays, SET responses are also `{:simple, "OK"}`.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Resp.Encoder
  alias Ferricstore.Resp.Parser
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

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
    {:ok, data} = :gen_tcp.recv(sock, 0, 15_000)
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

  # RESP3 simple string helpers
  defp ok, do: {:simple, "OK"}
  defp queued, do: {:simple, "QUEUED"}

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)

    on_exit(fn ->
      :gen_tcp.close(sock)
      ShardHelpers.flush_all_keys()
    end)

    %{sock: sock, port: port}
  end

  # ---------------------------------------------------------------------------
  # Basic TCP cross-shard
  # ---------------------------------------------------------------------------

  describe "basic cross-shard MULTI/EXEC succeeds atomically" do
    test "SET on shard 0 + SET on shard 3 — returns [OK, OK], both readable", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      # j -> shard 0, b -> shard 3
      send_cmd(sock, ["SET", "j", "cross_val_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", "b", "cross_val_3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      # Both keys should be readable
      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == "cross_val_0"

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "cross_val_3"
    end

    test "MULTI with 4 keys across all shards, EXEC — all written", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      # j -> shard 0, g -> shard 1, a -> shard 2, b -> shard 3
      send_cmd(sock, ["SET", "j", "s0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "g", "s1"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "a", "s2"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "s3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok(), ok(), ok()]

      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == "s0"
      send_cmd(sock, ["GET", "g"])
      assert recv_response(sock) == "s1"
      send_cmd(sock, ["GET", "a"])
      assert recv_response(sock) == "s2"
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "s3"
    end

    test "MULTI with GET + SET across shards, EXEC — results in order", %{sock: sock} do
      # Pre-set values on different shards
      send_cmd(sock, ["SET", "j", "existing_j"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", "a", "existing_a"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "g", "new_g"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", "a"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "new_b"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == ["existing_j", ok(), "existing_a", ok()]
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH over TCP with cross-shard tx
  # ---------------------------------------------------------------------------

  describe "WATCH with cross-shard tx over TCP" do
    test "WATCH conflict aborts cross-shard EXEC — NO writes on either shard", %{
      sock: sock,
      port: port
    } do
      # Pre-set values on two shards
      send_cmd(sock, ["SET", "j", "original_j"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", "b", "original_b"])
      assert recv_response(sock) == ok()

      # WATCH key on shard 0
      send_cmd(sock, ["WATCH", "j"])
      assert recv_response(sock) == ok()

      # Queue cross-shard commands
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "j", "from_tx_j"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "from_tx_b"])
      assert recv_response(sock) == queued()

      # Another connection modifies the watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", "j", "modified_by_other"])
      assert recv_response(sock2) == ok()
      :gen_tcp.close(sock2)

      # EXEC should return nil (aborted)
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil

      # Neither shard should have transaction writes
      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == "modified_by_other"

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "original_b"
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery after cross-shard tx
  # ---------------------------------------------------------------------------

  describe "recovery after cross-shard tx" do
    test "after successful cross-shard EXEC, connection is back to normal mode", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      # Cross-shard: j -> shard 0, b -> shard 3
      send_cmd(sock, ["SET", "j", "tx_j"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "tx_b"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      # Connection should be back to normal — can issue regular commands
      send_cmd(sock, ["SET", "j", "after_tx"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == "after_tx"

      # Different shard also works
      send_cmd(sock, ["SET", "a", "normal_mode"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "a"])
      assert recv_response(sock) == "normal_mode"
    end
  end

  # ---------------------------------------------------------------------------
  # Hash tags still use single-shard fast path
  # ---------------------------------------------------------------------------

  describe "hash tags still use single-shard fast path" do
    test "keys with same hash tag execute as single-shard (fast path)", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "{user:42}:name", "Alice"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "{user:42}:email", "alice@example.com"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      send_cmd(sock, ["GET", "{user:42}:name"])
      assert recv_response(sock) == "Alice"
      send_cmd(sock, ["GET", "{user:42}:email"])
      assert recv_response(sock) == "alice@example.com"
    end
  end
end
