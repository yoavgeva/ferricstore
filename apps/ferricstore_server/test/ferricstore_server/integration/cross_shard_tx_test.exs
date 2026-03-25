defmodule FerricstoreServer.Integration.CrossShardTxTest do
  @moduledoc """
  TCP-level integration tests for cross-shard MULTI/EXEC transactions.

  Cross-shard transactions now execute atomically via the anchor-shard Raft
  approach. A single Raft log entry is submitted to the anchor shard containing
  commands for ALL involved shards.

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

      # j -> shard 0, b -> shard 3
      send_cmd(sock, ["SET", "j", "cross_val_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", "b", "cross_val_3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == "cross_val_0"

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "cross_val_3"
    end

    test "SET keys across all four shards succeeds", %{sock: sock} do
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

    test "mixed reads and writes across shards succeeds", %{sock: sock} do
      send_cmd(sock, ["SET", "j", "existing_j"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", "a", "existing_a"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "new_b"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", "a"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == ["existing_j", ok(), "existing_a"]
    end

    test "INCR on keys across different shards succeeds", %{sock: sock} do
      send_cmd(sock, ["SET", "j", "10"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", "b", "20"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["INCR", "j"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["INCR", "b"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [11, 21]

      send_cmd(sock, ["GET", "j"])
      assert recv_response(sock) == "11"
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "21"
    end

    test "connection returns to normal state after cross-shard tx", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "j", "v0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "v3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == [ok(), ok()]

      # Connection should be back to normal — can issue commands
      send_cmd(sock, ["SET", "j", "after_tx"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "j"])
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
      # "g" and "k" both on shard 1
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "g", "fast_g"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "k", "fast_k"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", "g"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok(), "fast_g"]
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH still works with single-shard transactions
  # ---------------------------------------------------------------------------

  describe "WATCH with single-shard transaction" do
    test "WATCH conflict aborts single-shard transaction", %{sock: sock, port: port} do
      # "g" and "k" both on shard 1
      send_cmd(sock, ["SET", "g", "original"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["WATCH", "g"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "g", "from_tx"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "k", "from_tx_k"])
      assert recv_response(sock) == queued()

      # Another connection modifies watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", "g", "modified_by_other"])
      assert recv_response(sock2) == ok()
      :gen_tcp.close(sock2)

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil

      send_cmd(sock, ["GET", "g"])
      assert recv_response(sock) == "modified_by_other"

      # "k" should not be modified (transaction aborted)
      send_cmd(sock, ["GET", "k"])
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

      send_cmd(sock, ["SET", "g", "will_discard"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "k", "will_discard"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", "g"])
      assert recv_response(sock) == nil
      send_cmd(sock, ["GET", "k"])
      assert recv_response(sock) == nil
    end
  end
end
