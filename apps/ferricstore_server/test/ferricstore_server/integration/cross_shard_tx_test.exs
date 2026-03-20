defmodule FerricstoreServer.Integration.CrossShardTxTest do
  @moduledoc """
  TCP-level integration tests for cross-shard MULTI/EXEC transactions.

  These tests connect over real TCP sockets, send RESP3-encoded commands, and
  verify that the Two-Phase Commit coordinator provides atomicity for
  transactions spanning multiple shards.

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
  # Cross-shard MULTI/EXEC
  # ---------------------------------------------------------------------------

  describe "MULTI with keys on different shards, EXEC succeeds" do
    test "SET keys on shard 0 and shard 1", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "h", "cross_val_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", "b", "cross_val_1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      # Verify both keys were written
      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == "cross_val_0"

      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "cross_val_1"
    end

    test "SET keys across all four shards", %{sock: sock} do
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

      # Verify all four keys
      for {key, expected} <- [{"h", "s0"}, {"b", "s1"}, {"l", "s2"}, {"a", "s3"}] do
        send_cmd(sock, ["GET", key])
        assert recv_response(sock) == expected
      end
    end

    test "mixed reads and writes across shards", %{sock: sock} do
      # Pre-populate
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

      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 0) == "existing_h"
      assert Enum.at(result, 1) == ok()
      assert Enum.at(result, 2) == "existing_a"
    end
  end

  describe "WATCH key on shard 0, modify from another connection, EXEC returns nil" do
    test "WATCH conflict aborts cross-shard transaction", %{sock: sock, port: port} do
      # Set initial value
      send_cmd(sock, ["SET", "h", "original"])
      assert recv_response(sock) == ok()

      # WATCH the key on connection 1
      send_cmd(sock, ["WATCH", "h"])
      assert recv_response(sock) == ok()

      # Start MULTI on connection 1
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "h", "from_tx"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "from_tx_b"])
      assert recv_response(sock) == queued()

      # Connection 2 modifies the watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", "h", "modified_by_other"])
      assert recv_response(sock2) == ok()
      :gen_tcp.close(sock2)

      # EXEC should return nil (abort) because WATCH detected conflict
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil

      # "h" should have the value set by connection 2
      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == "modified_by_other"

      # "b" should not have been modified
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == nil
    end
  end

  describe "large cross-shard transaction" do
    test "50 commands across all 4 shards", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      # Generate 50 unique keys spread across shards
      keys =
        for i <- 1..50 do
          "txkey_#{i}"
        end

      Enum.each(keys, fn key ->
        send_cmd(sock, ["SET", key, "val_#{key}"])
        assert recv_response(sock) == queued()
      end)

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 50
      assert Enum.all?(result, &(&1 == ok()))

      # Verify a sample of keys
      for key <- Enum.take(keys, 10) do
        send_cmd(sock, ["GET", key])
        assert recv_response(sock) == "val_#{key}"
      end
    end
  end

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

  describe "INCR across shards" do
    test "INCR on keys across different shards", %{sock: sock} do
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
      assert recv_response(sock) == "11"
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == "21"
    end
  end

  describe "DISCARD works normally" do
    test "DISCARD clears cross-shard queue", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "h", "will_discard"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "b", "will_discard"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == ok()

      # Keys should not exist
      send_cmd(sock, ["GET", "h"])
      assert recv_response(sock) == nil
      send_cmd(sock, ["GET", "b"])
      assert recv_response(sock) == nil
    end
  end
end
