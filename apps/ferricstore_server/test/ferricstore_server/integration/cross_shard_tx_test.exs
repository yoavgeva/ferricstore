defmodule FerricstoreServer.Integration.CrossShardTxTest do
  @moduledoc """
  TCP-level integration tests for cross-shard MULTI/EXEC transactions.

  Cross-shard transactions now execute atomically via the anchor-shard Raft
  approach. A single Raft log entry is submitted to the anchor shard containing
  commands for ALL involved shards.

  Key-to-shard mapping (phash2, 4 shards):
    - shard 0: "h", "j", "o", "p", "t", "u", "v", "x"
    - shard 1: "b", "c", "d", "e", "f", "k", "q", "r", "w", "y"
    - shard 2: "l"
    - shard 3: "a", "g", "i", "m", "n", "s", "z"

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

  # RESP3 simple string helper
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
  # Cross-shard MULTI/EXEC succeeds atomically
  # ---------------------------------------------------------------------------

  describe "cross-shard MULTI/EXEC succeeds atomically" do
    test "SET keys on different shards succeeds", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      # h -> shard 0, b -> shard 1
      send_cmd(sock, ["SET", "h", "cross_val_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", "b", "cross_val_1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == "cross_val_0"

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "cross_val_1"
    end

    test "SET keys across all four shards succeeds", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      # h -> shard 0, b -> shard 1, l -> shard 2, a -> shard 3
      send_cmd(sock, ["SET", "h", "s0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "s1"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "l", "s2"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "a", "s3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok(), ok(), ok()]

      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == "s0"
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "s1"
      send_cmd(sock, ["GET", "l"])
      assert recv_response(sock) == "s2"
      send_cmd(sock, ["GET", "a"])
      assert recv_response(sock) == "s3"
    end

    test "mixed reads and writes across shards succeeds", %{sock: sock} do
      send_cmd(sock, ["SET", "h", "existing_h"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", "a", "existing_a"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "new_b"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", "a"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == ["existing_h", ok(), "existing_a"]
    end

    test "INCR on keys across different shards succeeds", %{sock: sock} do
      send_cmd(sock, ["SET", "h", "10"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", "b", "20"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["INCR", "h"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["INCR", "b"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [11, 21]

      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == 11
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == 21
    end

    test "connection returns to normal state after cross-shard tx", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "h", "v0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "v1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == [ok(), ok()]

      # Connection should be back to normal — can issue commands
      send_cmd(sock, ["SET", "h", "after_tx"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == "after_tx"
    end
  end

  # ---------------------------------------------------------------------------
  # Hash tags co-locate keys on same shard (fast path)
  # ---------------------------------------------------------------------------

  describe "hash tags use single-shard fast path" do
    test "keys with same hash tag execute atomically", %{sock: sock} do
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

  # ---------------------------------------------------------------------------
  # Single-shard MULTI/EXEC still works atomically
  # ---------------------------------------------------------------------------

  describe "single-shard transaction still works" do
    test "commands all on same shard use fast path", %{sock: sock} do
      # "b" and "c" both on shard 1
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "b", "fast_b"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "c", "fast_c"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok(), "fast_b"]
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH still works with single-shard transactions
  # ---------------------------------------------------------------------------

  describe "WATCH with single-shard transaction" do
    test "WATCH conflict aborts single-shard transaction", %{sock: sock, port: port} do
      # "b" and "c" both on shard 1
      send_cmd(sock, ["SET", "b", "original"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["WATCH", "b"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "b", "from_tx"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "c", "from_tx_c"])
      assert recv_response(sock) == queued()

      # Another connection modifies watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", "b", "modified_by_other"])
      assert recv_response(sock2) == ok()
      :gen_tcp.close(sock2)

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "modified_by_other"

      # "c" should not be modified (transaction aborted)
      send_cmd(sock, ["GET", "c"])
      assert recv_response(sock) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # DISCARD works normally
  # ---------------------------------------------------------------------------

  describe "DISCARD works normally" do
    test "DISCARD clears queue", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "b", "will_discard"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "c", "will_discard"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == nil
      send_cmd(sock, ["GET", "c"])
      assert recv_response(sock) == nil
    end
  end
end
