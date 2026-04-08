defmodule FerricstoreServer.Integration.CommandsTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for FerricStore Redis commands.

  These tests connect over a real TCP socket, send RESP3-encoded commands, and
  verify responses through the full stack:

      TCP → RESP3 parser → dispatcher → router → shard → Bitcask NIF

  A single Ranch TCP listener is started on an ephemeral port in `setup_all`
  and shared across all tests. Each test uses unique key names (via `ukey/1`)
  to avoid cross-test interference, except for FLUSHDB tests which explicitly
  clear the store and are grouped at the end.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Listener
  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser

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
    # 30s timeout to accommodate FLUSHDB on CI where many keys accumulate
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  # Receives exactly `n` RESP3 responses from the socket, accumulating
  # partial TCP reads as needed. Handles responses arriving in any number
  # of TCP segments (including one-per-segment from the sliding window).
  defp recv_n(sock, n) do
    do_recv_n(sock, n, "", [])
  end

  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc) when remaining > 0 do
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [_ | _] = vals, rest} ->
        taken = Enum.take(vals, remaining)
        new_acc = acc ++ taken
        new_remaining = remaining - length(taken)
        do_recv_n(sock, new_remaining, rest, new_acc)

      {:ok, [], _} ->
        do_recv_n(sock, remaining, buf2, acc)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # Generates a unique key name to avoid cross-test interference.
  defp ukey(name), do: "#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup — single listener for all tests
  # ---------------------------------------------------------------------------

  setup_all do
    # The application supervisor already starts the Ranch listener.
    # Discover the actual bound port (ephemeral in test env).
    %{port: Listener.port()}
  end

  # Flush all keys before each test to keep the keydir small.
  # A growing keydir makes KEYS/DBSIZE calls progressively slower
  # and can cause GenServer timeouts in later tests.
  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ---------------------------------------------------------------------------
  # SET and GET over TCP
  # ---------------------------------------------------------------------------

  describe "SET and GET over TCP" do
    test "SET then GET returns stored value", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_get")

      send_cmd(sock, ["SET", k, "hello"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "hello"

      :gen_tcp.close(sock)
    end

    test "SET GET with binary key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("my key with spaces")

      send_cmd(sock, ["SET", k, "spacevalue"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "spacevalue"

      :gen_tcp.close(sock)
    end

    test "SET GET with large value (10KB)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("large_val")
      large = :binary.copy("A", 10_000)

      send_cmd(sock, ["SET", k, large])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == large

      :gen_tcp.close(sock)
    end

    test "SET with EX then GET within TTL returns value", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_ex_get")

      send_cmd(sock, ["SET", k, "ttlval", "EX", "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "ttlval"

      :gen_tcp.close(sock)
    end

    test "SET with EX then TTL shows positive seconds", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_ex_ttl")

      send_cmd(sock, ["SET", k, "v", "EX", "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl >= 1 and ttl <= 10

      :gen_tcp.close(sock)
    end

    test "SET with PX then PTTL shows positive ms", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_px_pttl")

      send_cmd(sock, ["SET", k, "v", "PX", "5000"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PTTL", k])
      pttl = recv_response(sock)
      assert is_integer(pttl)
      assert pttl >= 1 and pttl <= 5000

      :gen_tcp.close(sock)
    end

    test "SET with NX does not overwrite existing key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_nx")

      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "overwrite", "NX"])
      assert recv_response(sock) == nil

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "original"

      :gen_tcp.close(sock)
    end

    test "SET with XX overwrites existing key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_xx_exists")

      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "updated", "XX"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "updated"

      :gen_tcp.close(sock)
    end

    test "SET with XX on missing key returns nil", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("set_xx_missing")

      send_cmd(sock, ["SET", k, "nope", "XX"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "GET missing key returns nil (RESP3 null)", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", "absolutely_nonexistent_key_#{:rand.uniform(999_999)}"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL and EXISTS over TCP
  # ---------------------------------------------------------------------------

  describe "DEL and EXISTS over TCP" do
    test "DEL returns 1 for existing key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("del_exists")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DEL", k])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end

    test "DEL returns 0 for missing key", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["DEL", "nonexistent_del_#{:rand.uniform(999_999)}"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end

    test "DEL multiple keys returns count", %{port: port} do
      sock = connect_and_hello(port)
      a = ukey("del_multi_a")
      b = ukey("del_multi_b")
      c = ukey("del_multi_c")
      d = ukey("del_multi_d_missing")

      send_cmd(sock, ["SET", a, "1"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", b, "2"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", c, "3"])
      assert recv_response(sock) == {:simple, "OK"}

      # DEL a, b, d (d was never set) — should return 2
      send_cmd(sock, ["DEL", a, b, d])
      assert recv_response(sock) == 2

      :gen_tcp.close(sock)
    end

    test "EXISTS returns 1 for present key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exists_present")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXISTS", k])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end

    test "EXISTS returns 0 for absent key", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["EXISTS", "nonexistent_exists_#{:rand.uniform(999_999)}"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end

    test "EXISTS counts same key twice", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exists_twice")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXISTS", k, k])
      assert recv_response(sock) == 2

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # MGET and MSET over TCP
  # ---------------------------------------------------------------------------

  describe "MGET and MSET over TCP" do
    test "MSET then MGET round-trip", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("mset_k1")
      k2 = ukey("mset_k2")

      send_cmd(sock, ["MSET", k1, "val1", k2, "val2"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MGET", k1, k2])
      assert recv_response(sock) == ["val1", "val2"]

      :gen_tcp.close(sock)
    end

    test "MGET with missing key returns nil in array", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("mget_present")
      missing = "mget_missing_#{:rand.uniform(999_999)}"

      send_cmd(sock, ["SET", k1, "exists"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MGET", k1, missing])
      assert recv_response(sock) == ["exists", nil]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # EXPIRE and TTL over TCP
  # ---------------------------------------------------------------------------

  describe "EXPIRE and TTL over TCP" do
    test "EXPIRE then TTL returns seconds remaining", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("expire_ttl")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "100"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl >= 1 and ttl <= 100

      :gen_tcp.close(sock)
    end

    test "EXPIRE then PTTL returns ms remaining", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("expire_pttl")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "100"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PTTL", k])
      pttl = recv_response(sock)
      assert is_integer(pttl)
      assert pttl >= 1 and pttl <= 100_000

      :gen_tcp.close(sock)
    end

    test "PERSIST removes TTL", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("persist")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "100"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PERSIST", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end

    test "TTL on key without expiry returns -1", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("ttl_no_expiry")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["TTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end

    test "TTL on missing key returns -2", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["TTL", "missing_ttl_#{:rand.uniform(999_999)}"])
      assert recv_response(sock) == -2

      :gen_tcp.close(sock)
    end

    test "PTTL on key without expiry returns -1", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pttl_no_expiry")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PTTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end

    test "PTTL on missing key returns -2", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PTTL", "missing_pttl_#{:rand.uniform(999_999)}"])
      assert recv_response(sock) == -2

      :gen_tcp.close(sock)
    end

    test "EXPIRE returns 0 for missing key", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["EXPIRE", "missing_expire_#{:rand.uniform(999_999)}", "10"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end

    test "EXPIREAT with future timestamp", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("expireat_future")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      future_ts = div(System.os_time(:second), 1) + 3600

      send_cmd(sock, ["EXPIREAT", k, Integer.to_string(future_ts)])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # KEYS and DBSIZE over TCP
  # ---------------------------------------------------------------------------

  describe "KEYS and DBSIZE over TCP" do
    test "KEYS * returns all keys in store", %{port: port} do
      sock = connect_and_hello(port)
      prefix = "keys_all_#{:rand.uniform(999_999)}"
      k1 = "#{prefix}:a"
      k2 = "#{prefix}:b"
      k3 = "#{prefix}:c"

      send_cmd(sock, ["SET", k1, "1"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k2, "2"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k3, "3"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["KEYS", "#{prefix}:*"])
      keys = recv_response(sock)
      assert is_list(keys)
      assert Enum.sort(keys) == Enum.sort([k1, k2, k3])

      :gen_tcp.close(sock)
    end

    test "KEYS with prefix pattern", %{port: port} do
      sock = connect_and_hello(port)
      suffix = "_#{:rand.uniform(999_999)}"
      u1 = "user#{suffix}:1"
      u2 = "user#{suffix}:2"
      other = "other#{suffix}:x"

      send_cmd(sock, ["SET", u1, "a"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", u2, "b"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", other, "c"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["KEYS", "user#{suffix}:*"])
      keys = recv_response(sock)
      assert is_list(keys)
      assert Enum.sort(keys) == Enum.sort([u1, u2])

      :gen_tcp.close(sock)
    end

    test "DBSIZE returns total key count", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("dbsize_a")
      k2 = ukey("dbsize_b")
      k3 = ukey("dbsize_c")

      # Record the baseline
      send_cmd(sock, ["DBSIZE"])
      before = recv_response(sock)
      assert is_integer(before)

      send_cmd(sock, ["SET", k1, "1"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k2, "2"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k3, "3"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DBSIZE"])
      after_count = recv_response(sock)
      assert is_integer(after_count)
      assert after_count >= before + 3

      :gen_tcp.close(sock)
    end

    test "KEYS returns empty array when no match", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["KEYS", "zzz_nomatch_#{:rand.uniform(999_999)}*"])
      assert recv_response(sock) == []

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # FLUSHDB over TCP
  # ---------------------------------------------------------------------------

  describe "FLUSHDB over TCP" do
    test "FLUSHDB returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", ukey("flush_ok"), "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["FLUSHDB"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "FLUSHDB clears keys", %{port: port} do
      sock = connect_and_hello(port)

      ka = ukey("flush_a")
      kb = ukey("flush_b")
      kc = ukey("flush_c")

      send_cmd(sock, ["SET", ka, "1"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", kb, "2"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", kc, "3"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["FLUSHDB"])
      assert recv_response(sock) == {:simple, "OK"}

      # Verify our specific keys are gone (not DBSIZE which can be
      # contaminated by other test modules running concurrently)
      send_cmd(sock, ["GET", ka])
      assert recv_response(sock) == nil
      send_cmd(sock, ["GET", kb])
      assert recv_response(sock) == nil
      send_cmd(sock, ["GET", kc])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ECHO over TCP
  # ---------------------------------------------------------------------------

  describe "ECHO over TCP" do
    test "ECHO returns the message", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ECHO", "hello"])
      assert recv_response(sock) == "hello"

      :gen_tcp.close(sock)
    end

    test "ECHO with binary data", %{port: port} do
      sock = connect_and_hello(port)
      msg = "hello world with spaces and special chars!"

      send_cmd(sock, ["ECHO", msg])
      assert recv_response(sock) == msg

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Pipelining real commands
  # ---------------------------------------------------------------------------

  describe "pipelining real commands" do
    test "pipeline SET then GET", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pipe_set_get")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "pipelined"]),
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 2)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == "pipelined"

      :gen_tcp.close(sock)
    end

    test "pipeline 10 SET commands then 10 GET commands", %{port: port} do
      sock = connect_and_hello(port)
      keys = for i <- 1..10, do: ukey("pipe10_#{i}")

      set_cmds =
        Enum.map(keys, fn k -> Encoder.encode(["SET", k, "val_#{k}"]) end)

      get_cmds =
        Enum.map(keys, fn k -> Encoder.encode(["GET", k]) end)

      pipeline = IO.iodata_to_binary(set_cmds ++ get_cmds)
      :ok = :gen_tcp.send(sock, pipeline)

      responses = recv_n(sock, 20)

      # First 10 should be OK
      set_responses = Enum.take(responses, 10)
      assert Enum.all?(set_responses, &(&1 == {:simple, "OK"}))

      # Last 10 should be the values
      get_responses = Enum.drop(responses, 10)

      Enum.zip(keys, get_responses)
      |> Enum.each(fn {k, resp} -> assert resp == "val_#{k}" end)

      :gen_tcp.close(sock)
    end

    test "pipeline mixed commands (SET, GET, DEL, EXISTS)", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("pipe_mix_1")
      k2 = ukey("pipe_mix_2")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, "one"]),
          Encoder.encode(["SET", k2, "two"]),
          Encoder.encode(["GET", k1]),
          Encoder.encode(["GET", k2]),
          Encoder.encode(["EXISTS", k1]),
          Encoder.encode(["DEL", k1]),
          Encoder.encode(["EXISTS", k1]),
          Encoder.encode(["GET", k1])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 8)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == {:simple, "OK"}
      assert Enum.at(responses, 2) == "one"
      assert Enum.at(responses, 3) == "two"
      assert Enum.at(responses, 4) == 1
      assert Enum.at(responses, 5) == 1
      assert Enum.at(responses, 6) == 0
      assert Enum.at(responses, 7) == nil

      :gen_tcp.close(sock)
    end

    test "pipeline MSET then MGET", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("pipe_mset_1")
      k2 = ukey("pipe_mset_2")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["MSET", k1, "mv1", k2, "mv2"]),
          Encoder.encode(["MGET", k1, k2])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 2)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == ["mv1", "mv2"]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Data survives reconnect
  # ---------------------------------------------------------------------------

  describe "data survives reconnect" do
    test "key written in one connection is readable in another", %{port: port} do
      k = ukey("reconnect_rw")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "persisted"])
      assert recv_response(sock1) == {:simple, "OK"}
      :gen_tcp.close(sock1)

      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["GET", k])
      assert recv_response(sock2) == "persisted"
      :gen_tcp.close(sock2)
    end

    test "deleted key is absent in new connection", %{port: port} do
      k = ukey("reconnect_del")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "doomed"])
      assert recv_response(sock1) == {:simple, "OK"}
      send_cmd(sock1, ["DEL", k])
      assert recv_response(sock1) == 1
      :gen_tcp.close(sock1)

      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["GET", k])
      assert recv_response(sock2) == nil
      :gen_tcp.close(sock2)
    end

    test "TTL persists across reconnect", %{port: port} do
      k = ukey("reconnect_ttl")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "v", "EX", "60"])
      assert recv_response(sock1) == {:simple, "OK"}
      :gen_tcp.close(sock1)

      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["TTL", k])
      ttl = recv_response(sock2)
      assert is_integer(ttl)
      assert ttl > 0
      :gen_tcp.close(sock2)
    end
  end

  # ---------------------------------------------------------------------------
  # Error responses over TCP
  # ---------------------------------------------------------------------------

  describe "error responses over TCP" do
    test "unknown command returns ERR", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BADCMD"])
      response = recv_response(sock)
      assert match?({:error, "ERR" <> _}, response)

      :gen_tcp.close(sock)
    end

    test "wrong arity returns ERR", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET"])
      response = recv_response(sock)
      assert match?({:error, "ERR" <> _}, response)

      :gen_tcp.close(sock)
    end

    test "server continues working after error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BADCMD"])
      _err = recv_response(sock)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # APPEND over TCP
  # ---------------------------------------------------------------------------

  describe "APPEND over TCP" do
    test "appends to existing string", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("append")

      send_cmd(sock, ["SET", key, "hello"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["APPEND", key, " world"])
      assert recv_response(sock) == 11

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "hello world"

      :gen_tcp.close(sock)
    end

    test "append to non-existing key creates it", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("append_new")

      send_cmd(sock, ["APPEND", key, "created"])
      assert recv_response(sock) == 7

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "created"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # STRLEN over TCP
  # ---------------------------------------------------------------------------

  describe "STRLEN over TCP" do
    test "returns length of string value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("strlen")

      send_cmd(sock, ["SET", key, "hello"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["STRLEN", key])
      assert recv_response(sock) == 5

      :gen_tcp.close(sock)
    end

    test "returns 0 for missing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("strlen_missing")

      send_cmd(sock, ["STRLEN", key])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # GETRANGE over TCP
  # ---------------------------------------------------------------------------

  describe "GETRANGE over TCP" do
    test "returns substring of stored value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getrange")

      send_cmd(sock, ["SET", key, "Hello, World!"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETRANGE", key, "0", "4"])
      assert recv_response(sock) == "Hello"

      :gen_tcp.close(sock)
    end

    test "negative indices count from end", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getrange_neg")

      send_cmd(sock, ["SET", key, "Hello, World!"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETRANGE", key, "-6", "-1"])
      assert recv_response(sock) == "World!"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SETRANGE over TCP
  # ---------------------------------------------------------------------------

  describe "SETRANGE over TCP" do
    test "overwrites part of string at offset", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("setrange")

      send_cmd(sock, ["SET", key, "Hello World"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SETRANGE", key, "6", "Redis"])
      assert recv_response(sock) == 11

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "Hello Redis"

      :gen_tcp.close(sock)
    end

    test "pads with zero bytes when offset exceeds length", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("setrange_pad")

      send_cmd(sock, ["SET", key, "Hi"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SETRANGE", key, "5", "!"])
      result_len = recv_response(sock)
      assert result_len == 6

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # INCR over TCP
  # ---------------------------------------------------------------------------

  describe "INCR over TCP" do
    test "increments integer value by 1", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incr")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCR", key])
      assert recv_response(sock) == 11

      :gen_tcp.close(sock)
    end

    test "initializes missing key to 0 then increments", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incr_new")

      send_cmd(sock, ["INCR", key])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # INCRBY over TCP
  # ---------------------------------------------------------------------------

  describe "INCRBY over TCP" do
    test "increments by specified amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrby")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBY", key, "5"])
      assert recv_response(sock) == 15

      :gen_tcp.close(sock)
    end

    test "increments by negative amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrby_neg")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBY", key, "-3"])
      assert recv_response(sock) == 7

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # DECR over TCP
  # ---------------------------------------------------------------------------

  describe "DECR over TCP" do
    test "decrements integer value by 1", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("decr")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DECR", key])
      assert recv_response(sock) == 9

      :gen_tcp.close(sock)
    end

    test "initializes missing key to 0 then decrements", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("decr_new")

      send_cmd(sock, ["DECR", key])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # DECRBY over TCP
  # ---------------------------------------------------------------------------

  describe "DECRBY over TCP" do
    test "decrements by specified amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("decrby")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DECRBY", key, "3"])
      assert recv_response(sock) == 7

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # INCRBYFLOAT over TCP
  # ---------------------------------------------------------------------------

  describe "INCRBYFLOAT over TCP" do
    test "increments by float amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrbyfloat")

      send_cmd(sock, ["SET", key, "10.5"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBYFLOAT", key, "0.1"])
      result = recv_response(sock)
      # Result is a bulk string representation of the float
      assert result == "10.6"

      :gen_tcp.close(sock)
    end

    test "increments integer string by float", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrbyfloat_int")

      send_cmd(sock, ["SET", key, "5"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBYFLOAT", key, "2.5"])
      result = recv_response(sock)
      assert result == "7.5"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # MSETNX over TCP
  # ---------------------------------------------------------------------------

  describe "MSETNX over TCP" do
    test "sets all keys when none exist", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("msetnx_a")
      k2 = ukey("msetnx_b")

      send_cmd(sock, ["MSETNX", k1, "val1", k2, "val2"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["MGET", k1, k2])
      assert recv_response(sock) == ["val1", "val2"]

      :gen_tcp.close(sock)
    end

    test "sets no keys when any already exist", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("msetnx_exists")
      k2 = ukey("msetnx_new")

      send_cmd(sock, ["SET", k1, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MSETNX", k1, "overwrite", k2, "val2"])
      assert recv_response(sock) == 0

      # k1 should still be original, k2 should not exist
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "original"

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # GETSET over TCP
  # ---------------------------------------------------------------------------

  describe "GETSET over TCP" do
    test "sets new value and returns old value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getset")

      send_cmd(sock, ["SET", key, "old"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETSET", key, "new"])
      assert recv_response(sock) == "old"

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "new"

      :gen_tcp.close(sock)
    end

    test "returns nil for non-existing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getset_new")

      send_cmd(sock, ["GETSET", key, "value"])
      assert recv_response(sock) == nil

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "value"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # GETDEL over TCP
  # ---------------------------------------------------------------------------

  describe "GETDEL over TCP" do
    test "returns value and deletes the key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getdel")

      send_cmd(sock, ["SET", key, "ephemeral"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETDEL", key])
      assert recv_response(sock) == "ephemeral"

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "returns nil for missing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getdel_missing")

      send_cmd(sock, ["GETDEL", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # GETEX over TCP
  # ---------------------------------------------------------------------------

  describe "GETEX over TCP" do
    test "returns value and sets expiry with EX", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getex")

      send_cmd(sock, ["SET", key, "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETEX", key, "EX", "100"])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["TTL", key])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0 and ttl <= 100

      :gen_tcp.close(sock)
    end

    test "returns value and removes expiry with PERSIST", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getex_persist")

      send_cmd(sock, ["SET", key, "myval", "EX", "100"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETEX", key, "PERSIST"])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["TTL", key])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end

    test "returns nil for missing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getex_missing")

      send_cmd(sock, ["GETEX", key, "EX", "100"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SETEX over TCP
  # ---------------------------------------------------------------------------

  describe "SETEX over TCP" do
    test "sets value with expiry in seconds", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("setex")

      send_cmd(sock, ["SETEX", key, "100", "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["TTL", key])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0 and ttl <= 100

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # PSETEX over TCP
  # ---------------------------------------------------------------------------

  describe "PSETEX over TCP" do
    test "sets value with expiry in milliseconds", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("psetex")

      send_cmd(sock, ["PSETEX", key, "100000", "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["PTTL", key])
      pttl = recv_response(sock)
      assert is_integer(pttl)
      assert pttl > 0 and pttl <= 100_000

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SETNX over TCP
  # ---------------------------------------------------------------------------

  describe "SETNX over TCP" do
    test "sets value only when key does not exist", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("setnx")

      send_cmd(sock, ["SETNX", key, "first"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "first"

      send_cmd(sock, ["SETNX", key, "second"])
      assert recv_response(sock) == 0

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "first"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # SET with KEEPTTL over TCP
  # ---------------------------------------------------------------------------

  describe "SET with KEEPTTL over TCP" do
    test "SET with KEEPTTL preserves existing TTL", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("keepttl")

      send_cmd(sock, ["SET", key, "original", "EX", "100"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", key, "updated", "KEEPTTL"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "updated"

      send_cmd(sock, ["TTL", key])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0 and ttl <= 100

      :gen_tcp.close(sock)
    end
  end
end
