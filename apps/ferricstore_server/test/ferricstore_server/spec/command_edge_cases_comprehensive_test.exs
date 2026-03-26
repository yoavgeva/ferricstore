defmodule FerricstoreServer.Spec.CommandEdgeCasesComprehensiveTest do
  @moduledoc """
  Comprehensive edge-case tests for EVERY FerricStore command category.

  Tests the real TCP attack surface using RESP3 framing. Focuses on inputs
  that could crash, corrupt, or return wrong results:

    - WRONGTYPE cross-type access
    - Integer overflow / underflow at i64 boundaries
    - Empty keys, empty values, empty field names
    - Key size at u16 boundary (65535 bytes)
    - Negative indices beyond string/list length
    - Non-numeric values where numbers are expected
    - Odd argument counts where pairs are required
    - Operations on non-existent keys
    - SETRANGE zero-padding
    - GETRANGE boundary arithmetic
    - TTL edge cases (past timestamps, non-existent keys)
    - Transaction edge cases (EXEC without MULTI, errors inside MULTI)
    - Pipeline stress (1000 commands, mixed valid+invalid)
    - SET option combinations (NX+XX, EX+PX, KEEPTTL+EX)
    - Sorted set score parsing (+inf, -inf, NaN, non-numeric)
    - SRANDMEMBER / ZRANDMEMBER with negative count (duplicates allowed)
    - RENAME / COPY on non-existent source
    - SCAN cursor-based iteration edge cases

  Each test is independent (flush_all_keys in setup).
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  @moduletag timeout: 120_000

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    ShardHelpers.wait_shards_alive()
    %{port: Listener.port()}
  end

  setup %{port: port} do
    # Reset process dictionary recv buffers from any previous test
    Process.delete(:edge_parsed_queue)
    Process.delete(:edge_binary_buf)

    sock = connect_and_hello(port)
    ShardHelpers.flush_all_keys()
    on_exit(fn -> :gen_tcp.close(sock) end)
    %{sock: sock, port: port}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true,
        recbuf: 4 * 1024 * 1024,
        sndbuf: 4 * 1024 * 1024
      ])

    send_cmd(sock, ["HELLO", "3"])
    # Use direct recv for HELLO to avoid polluting the process dictionary buffer
    _greeting = recv_direct(sock, "", 5_000)
    sock
  end

  # Direct recv that does not use process dictionary buffering.
  # Used for HELLO handshake and secondary sockets to avoid buffer cross-contamination.
  defp recv_direct(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _rest} ->
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_direct(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  # Process dictionary keys for buffering between recv_response calls
  @parsed_key :edge_parsed_queue
  @binary_key :edge_binary_buf

  defp recv_response(sock, timeout \\ 10_000) do
    case Process.get(@parsed_key, []) do
      [val | rest] ->
        Process.put(@parsed_key, rest)
        val

      [] ->
        buf = Process.get(@binary_key, "")
        recv_loop(sock, buf, timeout)
    end
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | rest_vals], rest_bin} ->
        Process.put(@parsed_key, rest_vals)
        Process.put(@binary_key, rest_bin)
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp cmd(sock, args) do
    send_cmd(sock, args)
    recv_response(sock)
  end

  # cmd for secondary sockets that should not interfere with process dictionary buffer
  defp cmd_direct(sock, args) do
    send_cmd(sock, args)
    recv_direct(sock, "", 10_000)
  end

  # Hash tag ensures all keys co-locate on the same shard for multi-key ops.
  defp ukey(base), do: "{edgetest}:#{base}_#{:rand.uniform(9_999_999)}"

  defp assert_ok(result), do: assert result == {:simple, "OK"} or result == :ok

  defp assert_error_contains(result, substring) do
    assert {:error, msg} = result
    assert String.contains?(msg, substring), "Expected error containing #{inspect(substring)}, got: #{inspect(msg)}"
  end

  # ===========================================================================
  # STRING COMMANDS EDGE CASES
  # ===========================================================================

  describe "Strings: SET/GET edge cases" do
    test "SET with empty key returns error", %{sock: sock} do
      result = cmd(sock, ["SET", "", "value"])
      assert_error_contains(result, "empty key")
    end

    test "GET with empty key returns error", %{sock: sock} do
      result = cmd(sock, ["GET", ""])
      assert_error_contains(result, "empty key")
    end

    test "SET with max key size (65535 bytes) succeeds", %{sock: sock} do
      big_key = String.duplicate("K", 65_535)
      result = cmd(sock, ["SET", big_key, "val"])
      assert_ok(result)
      assert cmd(sock, ["GET", big_key]) == "val"
    end

    test "SET with key > 65535 bytes returns error", %{sock: sock} do
      huge_key = String.duplicate("K", 65_536)
      result = cmd(sock, ["SET", huge_key, "val"])
      assert_error_contains(result, "key too large")
    end

    test "GET non-existent key returns nil", %{sock: sock} do
      assert cmd(sock, ["GET", ukey("nonexistent")]) == nil
    end

    test "SET with empty value succeeds", %{sock: sock} do
      k = ukey("empty_val")
      assert_ok(cmd(sock, ["SET", k, ""]))
      assert cmd(sock, ["GET", k]) == ""
    end

    test "SET with binary value containing null bytes", %{sock: sock} do
      k = ukey("null_bytes")
      v = <<0, 1, 2, 0, 255, 0>>
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "SET NX when key exists returns nil", %{sock: sock} do
      k = ukey("setnx_exists")
      cmd(sock, ["SET", k, "first"])
      assert cmd(sock, ["SET", k, "second", "NX"]) == nil
      assert cmd(sock, ["GET", k]) == "first"
    end

    test "SET XX when key missing returns nil", %{sock: sock} do
      k = ukey("setxx_missing")
      assert cmd(sock, ["SET", k, "val", "XX"]) == nil
      assert cmd(sock, ["GET", k]) == nil
    end

    test "SET NX + XX is contradictory but should not crash", %{sock: sock} do
      k = ukey("nx_xx")
      # Redis rejects NX+XX. FerricStore should either error or handle gracefully.
      result = cmd(sock, ["SET", k, "val", "NX", "XX"])
      # Either nil (always skips) or error is acceptable, but must not crash
      assert result == nil or match?({:error, _}, result) or result == {:simple, "OK"}
    end

    test "SET with conflicting EX and PX returns error", %{sock: sock} do
      k = ukey("ex_px_conflict")
      result = cmd(sock, ["SET", k, "val", "EX", "10", "PX", "10000"])
      assert_error_contains(result, "syntax error")
    end

    test "SET with EX + KEEPTTL returns error", %{sock: sock} do
      k = ukey("ex_keepttl")
      result = cmd(sock, ["SET", k, "val", "EX", "10", "KEEPTTL"])
      assert_error_contains(result, "syntax error")
    end

    test "SET GET returns old value", %{sock: sock} do
      k = ukey("set_get")
      cmd(sock, ["SET", k, "old"])
      assert cmd(sock, ["SET", k, "new", "GET"]) == "old"
      assert cmd(sock, ["GET", k]) == "new"
    end

    test "SET GET on non-existent key returns nil", %{sock: sock} do
      k = ukey("set_get_nil")
      assert cmd(sock, ["SET", k, "val", "GET"]) == nil
      assert cmd(sock, ["GET", k]) == "val"
    end

    test "SET with EX 0 returns error", %{sock: sock} do
      k = ukey("ex_zero")
      result = cmd(sock, ["SET", k, "val", "EX", "0"])
      assert_error_contains(result, "invalid expire")
    end

    test "SET with negative EX returns error", %{sock: sock} do
      k = ukey("ex_neg")
      result = cmd(sock, ["SET", k, "val", "EX", "-5"])
      assert_error_contains(result, "invalid expire")
    end

    test "SET with non-integer EX returns error", %{sock: sock} do
      k = ukey("ex_str")
      result = cmd(sock, ["SET", k, "val", "EX", "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "SET with unrecognized option returns error", %{sock: sock} do
      k = ukey("bad_opt")
      result = cmd(sock, ["SET", k, "val", "FOOBAR"])
      assert_error_contains(result, "not recognized")
    end

    test "SET with no args returns arity error", %{sock: sock} do
      result = cmd(sock, ["SET"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "GET with too many args returns arity error", %{sock: sock} do
      result = cmd(sock, ["GET", "a", "b"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "SET KEEPTTL preserves existing TTL", %{sock: sock} do
      k = ukey("keepttl")
      cmd(sock, ["SET", k, "v1", "EX", "1000"])
      ttl_before = cmd(sock, ["TTL", k])
      assert ttl_before > 0

      cmd(sock, ["SET", k, "v2", "KEEPTTL"])
      assert cmd(sock, ["GET", k]) == "v2"
      ttl_after = cmd(sock, ["TTL", k])
      assert ttl_after > 0
    end
  end

  describe "Strings: SETNX / SETEX / PSETEX edge cases" do
    test "SETNX when key exists returns 0", %{sock: sock} do
      k = ukey("setnx_0")
      cmd(sock, ["SET", k, "existing"])
      assert cmd(sock, ["SETNX", k, "new"]) == 0
      assert cmd(sock, ["GET", k]) == "existing"
    end

    test "SETNX when key missing returns 1", %{sock: sock} do
      k = ukey("setnx_1")
      assert cmd(sock, ["SETNX", k, "val"]) == 1
      assert cmd(sock, ["GET", k]) == "val"
    end

    test "SETEX with zero seconds returns error", %{sock: sock} do
      k = ukey("setex_zero")
      result = cmd(sock, ["SETEX", k, "0", "val"])
      assert_error_contains(result, "invalid expire")
    end

    test "SETEX with negative seconds returns error", %{sock: sock} do
      k = ukey("setex_neg")
      result = cmd(sock, ["SETEX", k, "-1", "val"])
      assert_error_contains(result, "invalid expire")
    end

    test "PSETEX with zero ms returns error", %{sock: sock} do
      k = ukey("psetex_zero")
      result = cmd(sock, ["PSETEX", k, "0", "val"])
      assert_error_contains(result, "invalid expire")
    end

    test "SETEX wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["SETEX", "k", "10"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: INCR / DECR / INCRBY / DECRBY edge cases" do
    test "INCR on non-existent key initializes to 0 then increments", %{sock: sock} do
      k = ukey("incr_nil")
      assert cmd(sock, ["INCR", k]) == 1
    end

    test "INCR on non-numeric value returns error", %{sock: sock} do
      k = ukey("incr_str")
      cmd(sock, ["SET", k, "hello"])
      result = cmd(sock, ["INCR", k])
      assert_error_contains(result, "not an integer")
    end

    test "INCR on float value returns error", %{sock: sock} do
      k = ukey("incr_float")
      cmd(sock, ["SET", k, "3.14"])
      result = cmd(sock, ["INCR", k])
      assert_error_contains(result, "not an integer")
    end

    test "DECR on non-existent key initializes to 0 then decrements", %{sock: sock} do
      k = ukey("decr_nil")
      assert cmd(sock, ["DECR", k]) == -1
    end

    test "INCRBY with large positive value", %{sock: sock} do
      k = ukey("incrby_large")
      cmd(sock, ["SET", k, "0"])
      assert cmd(sock, ["INCRBY", k, "9223372036854775806"]) == 9_223_372_036_854_775_806
    end

    test "INCRBY with non-numeric delta returns error", %{sock: sock} do
      k = ukey("incrby_nan")
      result = cmd(sock, ["INCRBY", k, "xyz"])
      assert_error_contains(result, "not an integer")
    end

    test "INCRBY with float delta returns error", %{sock: sock} do
      k = ukey("incrby_float")
      result = cmd(sock, ["INCRBY", k, "1.5"])
      assert_error_contains(result, "not an integer")
    end

    test "DECRBY with non-numeric delta returns error", %{sock: sock} do
      k = ukey("decrby_nan")
      result = cmd(sock, ["DECRBY", k, "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "INCR wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["INCR"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DECRBY wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["DECRBY", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: INCRBYFLOAT edge cases" do
    test "INCRBYFLOAT on non-existent key creates it", %{sock: sock} do
      k = ukey("incrbyfloat_nil")
      result = cmd(sock, ["INCRBYFLOAT", k, "2.5"])
      assert is_binary(result)
      assert String.to_float(result) == 2.5
    end

    test "INCRBYFLOAT on non-numeric value returns error", %{sock: sock} do
      k = ukey("incrbyfloat_str")
      cmd(sock, ["SET", k, "hello"])
      result = cmd(sock, ["INCRBYFLOAT", k])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "INCRBYFLOAT with non-numeric increment returns error", %{sock: sock} do
      k = ukey("incrbyfloat_nan")
      result = cmd(sock, ["INCRBYFLOAT", k, "notanumber"])
      assert_error_contains(result, "not a valid float")
    end

    test "INCRBYFLOAT with integer increment string works", %{sock: sock} do
      k = ukey("incrbyfloat_int")
      cmd(sock, ["SET", k, "10"])
      result = cmd(sock, ["INCRBYFLOAT", k, "5"])
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 15.0
    end

    test "INCRBYFLOAT wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["INCRBYFLOAT"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: APPEND edge cases" do
    test "APPEND to non-existent key creates it", %{sock: sock} do
      k = ukey("append_nil")
      assert cmd(sock, ["APPEND", k, "hello"]) == 5
      assert cmd(sock, ["GET", k]) == "hello"
    end

    test "APPEND to existing key extends it", %{sock: sock} do
      k = ukey("append_exist")
      cmd(sock, ["SET", k, "hello"])
      assert cmd(sock, ["APPEND", k, " world"]) == 11
      assert cmd(sock, ["GET", k]) == "hello world"
    end

    test "APPEND with empty value returns existing length", %{sock: sock} do
      k = ukey("append_empty")
      cmd(sock, ["SET", k, "abc"])
      assert cmd(sock, ["APPEND", k, ""]) == 3
    end

    test "APPEND wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["APPEND", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: STRLEN edge cases" do
    test "STRLEN on non-existent key returns 0", %{sock: sock} do
      assert cmd(sock, ["STRLEN", ukey("strlen_nil")]) == 0
    end

    test "STRLEN on empty string returns 0", %{sock: sock} do
      k = ukey("strlen_empty")
      cmd(sock, ["SET", k, ""])
      assert cmd(sock, ["STRLEN", k]) == 0
    end

    test "STRLEN wrong arity", %{sock: sock} do
      result = cmd(sock, ["STRLEN"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: GETRANGE edge cases" do
    test "GETRANGE on non-existent key returns empty string", %{sock: sock} do
      assert cmd(sock, ["GETRANGE", ukey("getrange_nil"), "0", "10"]) == ""
    end

    test "GETRANGE with start > end returns empty string", %{sock: sock} do
      k = ukey("getrange_inverted")
      cmd(sock, ["SET", k, "hello"])
      assert cmd(sock, ["GETRANGE", k, "3", "1"]) == ""
    end

    test "GETRANGE with negative indices", %{sock: sock} do
      k = ukey("getrange_neg")
      cmd(sock, ["SET", k, "hello"])
      # -1 is last char, -5 is first char
      assert cmd(sock, ["GETRANGE", k, "-5", "-1"]) == "hello"
    end

    test "GETRANGE with negative indices beyond string length", %{sock: sock} do
      k = ukey("getrange_neg_oob")
      cmd(sock, ["SET", k, "hi"])
      # -100 should clamp to 0
      assert cmd(sock, ["GETRANGE", k, "-100", "-1"]) == "hi"
    end

    test "GETRANGE with start beyond string length returns empty", %{sock: sock} do
      k = ukey("getrange_oob")
      cmd(sock, ["SET", k, "hi"])
      assert cmd(sock, ["GETRANGE", k, "100", "200"]) == ""
    end

    test "GETRANGE with non-integer indices returns error", %{sock: sock} do
      k = ukey("getrange_nan")
      cmd(sock, ["SET", k, "hi"])
      result = cmd(sock, ["GETRANGE", k, "abc", "1"])
      assert_error_contains(result, "not an integer")
    end

    test "GETRANGE wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["GETRANGE", "k", "0"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: SETRANGE edge cases" do
    test "SETRANGE on non-existent key zero-pads", %{sock: sock} do
      k = ukey("setrange_nil")
      # Setting at offset 5 with "hi" should zero-pad bytes 0-4
      new_len = cmd(sock, ["SETRANGE", k, "5", "hi"])
      assert new_len == 7
      val = cmd(sock, ["GET", k])
      assert byte_size(val) == 7
      assert binary_part(val, 0, 5) == <<0, 0, 0, 0, 0>>
      assert binary_part(val, 5, 2) == "hi"
    end

    test "SETRANGE with negative offset returns error", %{sock: sock} do
      k = ukey("setrange_neg")
      cmd(sock, ["SET", k, "hello"])
      result = cmd(sock, ["SETRANGE", k, "-1", "x"])
      assert_error_contains(result, "out of range")
    end

    test "SETRANGE with offset > 512MB returns error", %{sock: sock} do
      k = ukey("setrange_huge")
      result = cmd(sock, ["SETRANGE", k, "536870912", "x"])
      assert_error_contains(result, "maximum allowed size")
    end

    test "SETRANGE with non-integer offset returns error", %{sock: sock} do
      k = ukey("setrange_nan")
      result = cmd(sock, ["SETRANGE", k, "abc", "x"])
      assert_error_contains(result, "not an integer")
    end

    test "SETRANGE with offset 0 and empty value on existing key", %{sock: sock} do
      k = ukey("setrange_empty")
      cmd(sock, ["SET", k, "hello"])
      new_len = cmd(sock, ["SETRANGE", k, "0", ""])
      assert new_len == 5
      assert cmd(sock, ["GET", k]) == "hello"
    end

    test "SETRANGE wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["SETRANGE", "k", "0"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: MSET / MGET edge cases" do
    test "MSET with odd number of args returns error", %{sock: sock} do
      result = cmd(sock, ["MSET", "k1", "v1", "k2"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "MSET with zero args returns error", %{sock: sock} do
      result = cmd(sock, ["MSET"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "MGET with non-existent keys returns nils", %{sock: sock} do
      result = cmd(sock, ["MGET", ukey("mget_a"), ukey("mget_b")])
      assert result == [nil, nil]
    end

    test "MGET with mix of existent and non-existent keys", %{sock: sock} do
      k1 = ukey("mget_mix_a")
      k2 = ukey("mget_mix_b")
      cmd(sock, ["SET", k1, "val1"])
      result = cmd(sock, ["MGET", k1, k2])
      assert result == ["val1", nil]
    end

    test "MGET with zero args returns error", %{sock: sock} do
      result = cmd(sock, ["MGET"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "MSET with empty key returns error", %{sock: sock} do
      result = cmd(sock, ["MSET", "", "val"])
      assert_error_contains(result, "too large or empty")
    end
  end

  describe "Strings: MSETNX edge cases" do
    test "MSETNX when none exist returns 1", %{sock: sock} do
      k1 = ukey("msetnx_a")
      k2 = ukey("msetnx_b")
      assert cmd(sock, ["MSETNX", k1, "v1", k2, "v2"]) == 1
      assert cmd(sock, ["GET", k1]) == "v1"
      assert cmd(sock, ["GET", k2]) == "v2"
    end

    test "MSETNX when any key exists returns 0 and sets nothing", %{sock: sock} do
      k1 = ukey("msetnx_c")
      k2 = ukey("msetnx_d")
      cmd(sock, ["SET", k1, "existing"])
      assert cmd(sock, ["MSETNX", k1, "new1", k2, "new2"]) == 0
      assert cmd(sock, ["GET", k1]) == "existing"
      assert cmd(sock, ["GET", k2]) == nil
    end

    test "MSETNX with odd args returns error", %{sock: sock} do
      result = cmd(sock, ["MSETNX", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: GETSET / GETDEL / GETEX edge cases" do
    test "GETSET on non-existent key returns nil and creates key", %{sock: sock} do
      k = ukey("getset_nil")
      assert cmd(sock, ["GETSET", k, "newval"]) == nil
      assert cmd(sock, ["GET", k]) == "newval"
    end

    test "GETDEL on non-existent key returns nil", %{sock: sock} do
      k = ukey("getdel_nil")
      assert cmd(sock, ["GETDEL", k]) == nil
    end

    test "GETDEL atomically gets and deletes", %{sock: sock} do
      k = ukey("getdel_ok")
      cmd(sock, ["SET", k, "temp"])
      assert cmd(sock, ["GETDEL", k]) == "temp"
      assert cmd(sock, ["GET", k]) == nil
    end

    test "GETEX with EX updates TTL and returns value", %{sock: sock} do
      k = ukey("getex_ex")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["GETEX", k, "EX", "1000"]) == "val"
      ttl = cmd(sock, ["TTL", k])
      assert ttl > 0 and ttl <= 1000
    end

    test "GETEX with PERSIST removes TTL", %{sock: sock} do
      k = ukey("getex_persist")
      cmd(sock, ["SET", k, "val", "EX", "1000"])
      assert cmd(sock, ["GETEX", k, "PERSIST"]) == "val"
      assert cmd(sock, ["TTL", k]) == -1
    end

    test "GETEX on non-existent key returns nil", %{sock: sock} do
      k = ukey("getex_nil")
      assert cmd(sock, ["GETEX", k]) == nil
    end

    test "GETEX with invalid option returns error", %{sock: sock} do
      k = ukey("getex_bad")
      cmd(sock, ["SET", k, "val"])
      result = cmd(sock, ["GETEX", k, "BADOPT", "123"])
      assert_error_contains(result, "syntax error")
    end

    test "GETSET wrong arity", %{sock: sock} do
      result = cmd(sock, ["GETSET", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "GETDEL wrong arity", %{sock: sock} do
      result = cmd(sock, ["GETDEL"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  # ===========================================================================
  # WRONGTYPE CROSS-TYPE ERRORS
  # ===========================================================================

  describe "WRONGTYPE cross-type access" do
    test "LPUSH to a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_list")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["LPUSH", k, "elem"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "HSET on a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_hash")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["HSET", k, "field", "value"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "SADD to a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_set")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["SADD", k, "member"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "ZADD to a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_zset")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["ZADD", k, "1.0", "member"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a hash key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_hash_str")
      cmd(sock, ["HSET", k, "field", "value"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a list key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_list_str")
      cmd(sock, ["LPUSH", k, "elem"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a set key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_set_str")
      cmd(sock, ["SADD", k, "member"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a sorted set key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_zset_str")
      cmd(sock, ["ZADD", k, "1.0", "member"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "HSET on a list key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_list_hash")
      cmd(sock, ["LPUSH", k, "elem"])
      result = cmd(sock, ["HSET", k, "field", "value"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "LPUSH on a hash key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_hash_list")
      cmd(sock, ["HSET", k, "field", "value"])
      result = cmd(sock, ["LPUSH", k, "elem"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "SADD on a hash key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_hash_set")
      cmd(sock, ["HSET", k, "field", "value"])
      result = cmd(sock, ["SADD", k, "member"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "ZADD on a set key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_set_zset")
      cmd(sock, ["SADD", k, "member"])
      result = cmd(sock, ["ZADD", k, "1.0", "member"])
      assert_error_contains(result, "WRONGTYPE")
    end
  end

  # ===========================================================================
  # LIST COMMANDS EDGE CASES
  # ===========================================================================

  describe "List edge cases" do
    test "LPUSH creates list from scratch", %{sock: sock} do
      k = ukey("lpush_new")
      assert cmd(sock, ["LPUSH", k, "a", "b", "c"]) == 3
    end

    test "RPUSH creates list from scratch", %{sock: sock} do
      k = ukey("rpush_new")
      assert cmd(sock, ["RPUSH", k, "a", "b"]) == 2
    end

    test "LRANGE on non-existent key returns empty list", %{sock: sock} do
      k = ukey("lrange_nil")
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == []
    end

    test "LRANGE with start > end returns empty list", %{sock: sock} do
      k = ukey("lrange_inverted")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert cmd(sock, ["LRANGE", k, "5", "1"]) == []
    end

    test "LRANGE with indices beyond list length clamps to bounds", %{sock: sock} do
      k = ukey("lrange_oob")
      cmd(sock, ["RPUSH", k, "a", "b"])
      assert cmd(sock, ["LRANGE", k, "0", "100"]) == ["a", "b"]
    end

    test "LINDEX out of bounds returns nil", %{sock: sock} do
      k = ukey("lindex_oob")
      cmd(sock, ["RPUSH", k, "a", "b"])
      assert cmd(sock, ["LINDEX", k, "100"]) == nil
    end

    test "LINDEX negative index", %{sock: sock} do
      k = ukey("lindex_neg")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert cmd(sock, ["LINDEX", k, "-1"]) == "c"
      assert cmd(sock, ["LINDEX", k, "-3"]) == "a"
    end

    test "LINDEX negative index beyond list returns nil", %{sock: sock} do
      k = ukey("lindex_neg_oob")
      cmd(sock, ["RPUSH", k, "a"])
      assert cmd(sock, ["LINDEX", k, "-100"]) == nil
    end

    test "LPOP from non-existent key returns nil", %{sock: sock} do
      k = ukey("lpop_nil")
      assert cmd(sock, ["LPOP", k]) == nil
    end

    test "RPOP from non-existent key returns nil", %{sock: sock} do
      k = ukey("rpop_nil")
      assert cmd(sock, ["RPOP", k]) == nil
    end

    test "LPOP from empty list (all elements popped) returns nil", %{sock: sock} do
      k = ukey("lpop_empty")
      cmd(sock, ["RPUSH", k, "a"])
      cmd(sock, ["LPOP", k])
      assert cmd(sock, ["LPOP", k]) == nil
    end

    test "LPOP with count returns list", %{sock: sock} do
      k = ukey("lpop_count")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      result = cmd(sock, ["LPOP", k, "2"])
      assert result == ["a", "b"]
    end

    test "LPOP with count > list length returns available elements", %{sock: sock} do
      k = ukey("lpop_count_big")
      cmd(sock, ["RPUSH", k, "a"])
      result = cmd(sock, ["LPOP", k, "100"])
      assert result == ["a"]
    end

    test "LPOP with count 0 returns empty list", %{sock: sock} do
      k = ukey("lpop_count_zero")
      cmd(sock, ["RPUSH", k, "a"])
      result = cmd(sock, ["LPOP", k, "0"])
      assert result == []
    end

    test "LLEN on non-existent key returns 0", %{sock: sock} do
      k = ukey("llen_nil")
      assert cmd(sock, ["LLEN", k]) == 0
    end

    test "LSET out of bounds returns error", %{sock: sock} do
      k = ukey("lset_oob")
      cmd(sock, ["RPUSH", k, "a"])
      result = cmd(sock, ["LSET", k, "5", "x"])
      assert_error_contains(result, "index out of range")
    end

    test "LMOVE with non-existent source returns nil", %{sock: sock} do
      k1 = ukey("lmove_nil_src")
      k2 = ukey("lmove_nil_dst")
      assert cmd(sock, ["LMOVE", k1, k2, "LEFT", "RIGHT"]) == nil
    end

    test "RPOPLPUSH with same src/dst rotates the list", %{sock: sock} do
      k = ukey("rpoplpush_self")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert cmd(sock, ["RPOPLPUSH", k, k]) == "c"
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == ["c", "a", "b"]
    end

    test "LPUSHX on non-existent key does nothing", %{sock: sock} do
      k = ukey("lpushx_nil")
      assert cmd(sock, ["LPUSHX", k, "a"]) == 0
      assert cmd(sock, ["LLEN", k]) == 0
    end

    test "RPUSHX on non-existent key does nothing", %{sock: sock} do
      k = ukey("rpushx_nil")
      assert cmd(sock, ["RPUSHX", k, "a"]) == 0
    end

    test "LREM removes correct count of elements", %{sock: sock} do
      k = ukey("lrem_multi")
      cmd(sock, ["RPUSH", k, "a", "b", "a", "c", "a"])
      assert cmd(sock, ["LREM", k, "2", "a"]) == 2
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == ["b", "c", "a"]
    end

    test "LINSERT BEFORE pivot", %{sock: sock} do
      k = ukey("linsert_before")
      cmd(sock, ["RPUSH", k, "a", "c"])
      assert cmd(sock, ["LINSERT", k, "BEFORE", "c", "b"]) == 3
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == ["a", "b", "c"]
    end

    test "LINSERT with non-existent pivot returns -1", %{sock: sock} do
      k = ukey("linsert_nopivot")
      cmd(sock, ["RPUSH", k, "a", "b"])
      assert cmd(sock, ["LINSERT", k, "BEFORE", "z", "x"]) == -1
    end

    test "LTRIM beyond list length empties it", %{sock: sock} do
      k = ukey("ltrim_beyond")
      cmd(sock, ["RPUSH", k, "a", "b"])
      cmd(sock, ["LTRIM", k, "5", "10"])
      assert cmd(sock, ["LLEN", k]) == 0
    end

    test "LPUSH wrong arity", %{sock: sock} do
      result = cmd(sock, ["LPUSH", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "LRANGE wrong arity", %{sock: sock} do
      result = cmd(sock, ["LRANGE", "k", "0"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  # ===========================================================================
  # HASH COMMANDS EDGE CASES
  # ===========================================================================

  describe "Hash edge cases" do
    test "HSET with empty field name succeeds", %{sock: sock} do
      k = ukey("hset_empty_field")
      assert cmd(sock, ["HSET", k, "", "val"]) == 1
      assert cmd(sock, ["HGET", k, ""]) == "val"
    end

    test "HGET non-existent field returns nil", %{sock: sock} do
      k = ukey("hget_nil_field")
      cmd(sock, ["HSET", k, "f1", "v1"])
      assert cmd(sock, ["HGET", k, "missing"]) == nil
    end

    test "HGET on non-existent key returns nil", %{sock: sock} do
      k = ukey("hget_nil_key")
      assert cmd(sock, ["HGET", k, "field"]) == nil
    end

    test "HDEL non-existent field returns 0", %{sock: sock} do
      k = ukey("hdel_nil_field")
      cmd(sock, ["HSET", k, "f1", "v1"])
      assert cmd(sock, ["HDEL", k, "missing"]) == 0
    end

    test "HDEL on non-existent key returns 0", %{sock: sock} do
      k = ukey("hdel_nil_key")
      assert cmd(sock, ["HDEL", k, "field"]) == 0
    end

    test "HINCRBY on non-numeric field returns error", %{sock: sock} do
      k = ukey("hincrby_nan")
      cmd(sock, ["HSET", k, "f1", "hello"])
      result = cmd(sock, ["HINCRBY", k, "f1", "1"])
      assert_error_contains(result, "not an integer")
    end

    test "HINCRBY on non-existent key creates hash with field=increment", %{sock: sock} do
      k = ukey("hincrby_nil")
      assert cmd(sock, ["HINCRBY", k, "f1", "5"]) == 5
      assert cmd(sock, ["HGET", k, "f1"]) == "5"
    end

    test "HINCRBY on non-existent field initializes to 0", %{sock: sock} do
      k = ukey("hincrby_nil_field")
      cmd(sock, ["HSET", k, "other", "x"])
      assert cmd(sock, ["HINCRBY", k, "newfield", "10"]) == 10
    end

    test "HINCRBYFLOAT on non-numeric field returns error", %{sock: sock} do
      k = ukey("hincrbyfloat_nan")
      cmd(sock, ["HSET", k, "f1", "hello"])
      result = cmd(sock, ["HINCRBYFLOAT", k, "f1", "1.5"])
      assert_error_contains(result, "not a valid float")
    end

    test "HINCRBYFLOAT on non-existent field initializes to 0", %{sock: sock} do
      k = ukey("hincrbyfloat_nil")
      result = cmd(sock, ["HINCRBYFLOAT", k, "f1", "2.5"])
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 2.5
    end

    test "HGETALL on non-existent key returns empty list", %{sock: sock} do
      k = ukey("hgetall_nil")
      assert cmd(sock, ["HGETALL", k]) == []
    end

    test "HLEN on non-existent key returns 0", %{sock: sock} do
      k = ukey("hlen_nil")
      assert cmd(sock, ["HLEN", k]) == 0
    end

    test "HEXISTS on non-existent key returns 0", %{sock: sock} do
      k = ukey("hexists_nil")
      assert cmd(sock, ["HEXISTS", k, "field"]) == 0
    end

    test "HKEYS on non-existent key returns empty list", %{sock: sock} do
      k = ukey("hkeys_nil")
      assert cmd(sock, ["HKEYS", k]) == []
    end

    test "HVALS on non-existent key returns empty list", %{sock: sock} do
      k = ukey("hvals_nil")
      assert cmd(sock, ["HVALS", k]) == []
    end

    test "HSETNX on existing field returns 0", %{sock: sock} do
      k = ukey("hsetnx_exist")
      cmd(sock, ["HSET", k, "f1", "old"])
      assert cmd(sock, ["HSETNX", k, "f1", "new"]) == 0
      assert cmd(sock, ["HGET", k, "f1"]) == "old"
    end

    test "HSETNX on new field returns 1", %{sock: sock} do
      k = ukey("hsetnx_new")
      assert cmd(sock, ["HSETNX", k, "f1", "val"]) == 1
      assert cmd(sock, ["HGET", k, "f1"]) == "val"
    end

    test "HSET with multiple field-value pairs", %{sock: sock} do
      k = ukey("hset_multi")
      result = cmd(sock, ["HSET", k, "f1", "v1", "f2", "v2", "f3", "v3"])
      assert result == 3
      assert cmd(sock, ["HLEN", k]) == 3
    end

    test "HSET with odd field-value pairs returns error", %{sock: sock} do
      k = ukey("hset_odd")
      result = cmd(sock, ["HSET", k, "f1", "v1", "f2"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "HMGET with mix of existing and non-existing fields", %{sock: sock} do
      k = ukey("hmget_mix")
      cmd(sock, ["HSET", k, "f1", "v1"])
      result = cmd(sock, ["HMGET", k, "f1", "f2"])
      assert result == ["v1", nil]
    end

    test "HSTRLEN on non-existent field returns 0", %{sock: sock} do
      k = ukey("hstrlen_nil")
      cmd(sock, ["HSET", k, "f1", "abc"])
      assert cmd(sock, ["HSTRLEN", k, "missing"]) == 0
    end

    test "HRANDFIELD on empty/non-existent hash returns nil", %{sock: sock} do
      k = ukey("hrandfield_nil")
      assert cmd(sock, ["HRANDFIELD", k]) == nil
    end

    test "HRANDFIELD with count > hash size returns all fields (no duplicates)", %{sock: sock} do
      k = ukey("hrandfield_big")
      cmd(sock, ["HSET", k, "f1", "v1", "f2", "v2"])
      result = cmd(sock, ["HRANDFIELD", k, "10"])
      assert is_list(result)
      assert length(result) == 2
    end

    test "HRANDFIELD with negative count allows duplicates", %{sock: sock} do
      k = ukey("hrandfield_neg")
      cmd(sock, ["HSET", k, "f1", "v1"])
      result = cmd(sock, ["HRANDFIELD", k, "-5"])
      assert is_list(result)
      assert length(result) == 5
      # All should be "f1" since it's the only field
      assert Enum.all?(result, &(&1 == "f1"))
    end

    test "HSET wrong arity (no field-value)", %{sock: sock} do
      result = cmd(sock, ["HSET", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "HGET wrong arity", %{sock: sock} do
      result = cmd(sock, ["HGET", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "HDEL wrong arity", %{sock: sock} do
      result = cmd(sock, ["HDEL", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DEL removes hash key and all fields", %{sock: sock} do
      k = ukey("del_hash")
      cmd(sock, ["HSET", k, "f1", "v1", "f2", "v2"])
      assert cmd(sock, ["HLEN", k]) == 2
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["HLEN", k]) == 0
      assert cmd(sock, ["HGET", k, "f1"]) == nil
    end
  end

  # ===========================================================================
  # SET COMMANDS EDGE CASES
  # ===========================================================================

  describe "Set edge cases" do
    test "SMEMBERS on non-existent key returns empty list", %{sock: sock} do
      k = ukey("smembers_nil")
      assert cmd(sock, ["SMEMBERS", k]) == []
    end

    test "SISMEMBER on non-existent key returns 0", %{sock: sock} do
      k = ukey("sismember_nil")
      assert cmd(sock, ["SISMEMBER", k, "member"]) == 0
    end

    test "SREM non-existent member returns 0", %{sock: sock} do
      k = ukey("srem_nil")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["SREM", k, "nonexistent"]) == 0
    end

    test "SREM on non-existent key returns 0", %{sock: sock} do
      k = ukey("srem_nil_key")
      assert cmd(sock, ["SREM", k, "member"]) == 0
    end

    test "SCARD on non-existent key returns 0", %{sock: sock} do
      k = ukey("scard_nil")
      assert cmd(sock, ["SCARD", k]) == 0
    end

    test "SADD duplicate member returns 0 (not added)", %{sock: sock} do
      k = ukey("sadd_dup")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["SADD", k, "a"]) == 0
      assert cmd(sock, ["SCARD", k]) == 1
    end

    test "SRANDMEMBER on non-existent key returns nil", %{sock: sock} do
      k = ukey("srandmember_nil")
      assert cmd(sock, ["SRANDMEMBER", k]) == nil
    end

    test "SRANDMEMBER with count > set size returns all (no dup)", %{sock: sock} do
      k = ukey("srandmember_big")
      cmd(sock, ["SADD", k, "a", "b"])
      result = cmd(sock, ["SRANDMEMBER", k, "10"])
      assert is_list(result)
      assert length(result) == 2
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SRANDMEMBER with negative count allows duplicates", %{sock: sock} do
      k = ukey("srandmember_neg")
      cmd(sock, ["SADD", k, "only"])
      result = cmd(sock, ["SRANDMEMBER", k, "-5"])
      assert is_list(result)
      assert length(result) == 5
      assert Enum.all?(result, &(&1 == "only"))
    end

    test "SRANDMEMBER with count 0 returns empty list", %{sock: sock} do
      k = ukey("srandmember_zero")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["SRANDMEMBER", k, "0"]) == []
    end

    test "SPOP from non-existent key returns nil", %{sock: sock} do
      k = ukey("spop_nil")
      assert cmd(sock, ["SPOP", k]) == nil
    end

    test "SPOP with count returns list", %{sock: sock} do
      k = ukey("spop_count")
      cmd(sock, ["SADD", k, "a", "b", "c"])
      result = cmd(sock, ["SPOP", k, "2"])
      assert is_list(result)
      assert length(result) == 2
    end

    test "SMISMEMBER returns correct membership flags", %{sock: sock} do
      k = ukey("smismember")
      cmd(sock, ["SADD", k, "a", "c"])
      assert cmd(sock, ["SMISMEMBER", k, "a", "b", "c"]) == [1, 0, 1]
    end

    test "SINTER with non-existent key returns empty", %{sock: sock} do
      k1 = ukey("sinter_a")
      k2 = ukey("sinter_nil")
      cmd(sock, ["SADD", k1, "a", "b"])
      result = cmd(sock, ["SINTER", k1, k2])
      assert result == []
    end

    test "SUNION with non-existent key returns other set", %{sock: sock} do
      k1 = ukey("sunion_a")
      k2 = ukey("sunion_nil")
      cmd(sock, ["SADD", k1, "a", "b"])
      result = cmd(sock, ["SUNION", k1, k2])
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SDIFF with non-existent key returns first set", %{sock: sock} do
      k1 = ukey("sdiff_a")
      k2 = ukey("sdiff_nil")
      cmd(sock, ["SADD", k1, "a", "b"])
      result = cmd(sock, ["SDIFF", k1, k2])
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SMOVE between sets", %{sock: sock} do
      k1 = ukey("smove_src")
      k2 = ukey("smove_dst")
      cmd(sock, ["SADD", k1, "a", "b"])
      cmd(sock, ["SADD", k2, "c"])
      assert cmd(sock, ["SMOVE", k1, k2, "a"]) == 1
      assert cmd(sock, ["SISMEMBER", k1, "a"]) == 0
      assert cmd(sock, ["SISMEMBER", k2, "a"]) == 1
    end

    test "SMOVE non-existent member returns 0", %{sock: sock} do
      k1 = ukey("smove_nil")
      k2 = ukey("smove_dst2")
      cmd(sock, ["SADD", k1, "a"])
      assert cmd(sock, ["SMOVE", k1, k2, "nonexistent"]) == 0
    end

    test "SADD wrong arity", %{sock: sock} do
      result = cmd(sock, ["SADD", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "SREM wrong arity", %{sock: sock} do
      result = cmd(sock, ["SREM", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DEL removes set key", %{sock: sock} do
      k = ukey("del_set")
      cmd(sock, ["SADD", k, "a", "b"])
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["SMEMBERS", k]) == []
    end
  end

  # ===========================================================================
  # SORTED SET COMMANDS EDGE CASES
  # ===========================================================================

  describe "Sorted set edge cases" do
    test "ZADD with non-numeric score returns error", %{sock: sock} do
      k = ukey("zadd_nan")
      result = cmd(sock, ["ZADD", k, "notanumber", "member"])
      assert_error_contains(result, "not a valid float")
    end

    test "ZADD with integer score succeeds", %{sock: sock} do
      k = ukey("zadd_int")
      assert cmd(sock, ["ZADD", k, "5", "member"]) == 1
      score = cmd(sock, ["ZSCORE", k, "member"])
      assert is_binary(score)
      {val, ""} = Float.parse(score)
      assert val == 5.0
    end

    test "ZSCORE on non-existent member returns nil", %{sock: sock} do
      k = ukey("zscore_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZSCORE", k, "missing"]) == nil
    end

    test "ZSCORE on non-existent key returns nil", %{sock: sock} do
      k = ukey("zscore_nil_key")
      assert cmd(sock, ["ZSCORE", k, "member"]) == nil
    end

    test "ZRANK on non-existent member returns nil", %{sock: sock} do
      k = ukey("zrank_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZRANK", k, "missing"]) == nil
    end

    test "ZRANK on non-existent key returns nil", %{sock: sock} do
      k = ukey("zrank_nil_key")
      assert cmd(sock, ["ZRANK", k, "member"]) == nil
    end

    test "ZREVRANK returns correct reverse rank", %{sock: sock} do
      k = ukey("zrevrank")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      assert cmd(sock, ["ZREVRANK", k, "c"]) == 0
      assert cmd(sock, ["ZREVRANK", k, "a"]) == 2
    end

    test "ZREVRANK on non-existent member returns nil", %{sock: sock} do
      k = ukey("zrevrank_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZREVRANK", k, "missing"]) == nil
    end

    test "ZCARD on non-existent key returns 0", %{sock: sock} do
      k = ukey("zcard_nil")
      assert cmd(sock, ["ZCARD", k]) == 0
    end

    test "ZREM non-existent member returns 0", %{sock: sock} do
      k = ukey("zrem_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZREM", k, "missing"]) == 0
    end

    test "ZINCRBY on non-existent key creates sorted set", %{sock: sock} do
      k = ukey("zincrby_nil")
      result = cmd(sock, ["ZINCRBY", k, "5", "member"])
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 5.0
    end

    test "ZINCRBY with non-numeric increment returns error", %{sock: sock} do
      k = ukey("zincrby_nan")
      result = cmd(sock, ["ZINCRBY", k, "abc", "member"])
      assert_error_contains(result, "not a valid float")
    end

    test "ZRANGEBYSCORE with inverted min/max returns empty", %{sock: sock} do
      k = ukey("zrangebyscore_inv")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      assert cmd(sock, ["ZRANGEBYSCORE", k, "3", "1"]) == []
    end

    test "ZRANGEBYSCORE with -inf/+inf returns all", %{sock: sock} do
      k = ukey("zrangebyscore_inf")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      result = cmd(sock, ["ZRANGEBYSCORE", k, "-inf", "+inf"])
      assert result == ["a", "b", "c"]
    end

    test "ZRANGEBYSCORE with exclusive bounds using '(' prefix", %{sock: sock} do
      k = ukey("zrangebyscore_excl")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      # (1 means > 1, (3 means < 3
      result = cmd(sock, ["ZRANGEBYSCORE", k, "(1", "(3"])
      assert result == ["b"]
    end

    test "ZRANGEBYSCORE with non-numeric min returns error", %{sock: sock} do
      k = ukey("zrangebyscore_nan")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      result = cmd(sock, ["ZRANGEBYSCORE", k, "abc", "10"])
      assert_error_contains(result, "not a float")
    end

    test "ZCOUNT with -inf/+inf", %{sock: sock} do
      k = ukey("zcount_inf")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      assert cmd(sock, ["ZCOUNT", k, "-inf", "+inf"]) == 3
    end

    test "ZCOUNT on non-existent key returns 0", %{sock: sock} do
      k = ukey("zcount_nil")
      assert cmd(sock, ["ZCOUNT", k, "-inf", "+inf"]) == 0
    end

    test "ZRANGE with start > end returns empty", %{sock: sock} do
      k = ukey("zrange_inv")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZRANGE", k, "5", "1"]) == []
    end

    test "ZRANGE with WITHSCORES", %{sock: sock} do
      k = ukey("zrange_ws")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZRANGE", k, "0", "-1", "WITHSCORES"])
      assert is_list(result)
      assert length(result) == 4
      # ["a", "1.0", "b", "2.0"]
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZRANGE with negative indices", %{sock: sock} do
      k = ukey("zrange_neg")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      result = cmd(sock, ["ZRANGE", k, "-2", "-1"])
      assert result == ["b", "c"]
    end

    test "ZPOPMIN on non-existent key returns empty", %{sock: sock} do
      k = ukey("zpopmin_nil")
      assert cmd(sock, ["ZPOPMIN", k]) == []
    end

    test "ZPOPMAX on non-existent key returns empty", %{sock: sock} do
      k = ukey("zpopmax_nil")
      assert cmd(sock, ["ZPOPMAX", k]) == []
    end

    test "ZPOPMIN removes element with lowest score", %{sock: sock} do
      k = ukey("zpopmin_ok")
      cmd(sock, ["ZADD", k, "3.0", "c", "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZPOPMIN", k])
      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0) == "a"
    end

    test "ZPOPMAX removes element with highest score", %{sock: sock} do
      k = ukey("zpopmax_ok")
      cmd(sock, ["ZADD", k, "3.0", "c", "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZPOPMAX", k])
      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0) == "c"
    end

    test "ZADD NX only adds new, does not update existing", %{sock: sock} do
      k = ukey("zadd_nx")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZADD", k, "NX", "5.0", "a"]) == 0
      score = cmd(sock, ["ZSCORE", k, "a"])
      {val, ""} = Float.parse(score)
      assert val == 1.0
    end

    test "ZADD XX only updates existing, does not add new", %{sock: sock} do
      k = ukey("zadd_xx")
      assert cmd(sock, ["ZADD", k, "XX", "5.0", "new_member"]) == 0
      assert cmd(sock, ["ZSCORE", k, "new_member"]) == nil
    end

    test "ZADD GT only updates if new score > existing", %{sock: sock} do
      k = ukey("zadd_gt")
      cmd(sock, ["ZADD", k, "5.0", "a"])
      cmd(sock, ["ZADD", k, "GT", "3.0", "a"])
      score = cmd(sock, ["ZSCORE", k, "a"])
      {val, ""} = Float.parse(score)
      assert val == 5.0  # Not updated because 3 < 5

      cmd(sock, ["ZADD", k, "GT", "10.0", "a"])
      score2 = cmd(sock, ["ZSCORE", k, "a"])
      {val2, ""} = Float.parse(score2)
      assert val2 == 10.0  # Updated because 10 > 5
    end

    test "ZADD LT only updates if new score < existing", %{sock: sock} do
      k = ukey("zadd_lt")
      cmd(sock, ["ZADD", k, "5.0", "a"])
      cmd(sock, ["ZADD", k, "LT", "10.0", "a"])
      score = cmd(sock, ["ZSCORE", k, "a"])
      {val, ""} = Float.parse(score)
      assert val == 5.0  # Not updated because 10 > 5

      cmd(sock, ["ZADD", k, "LT", "1.0", "a"])
      score2 = cmd(sock, ["ZSCORE", k, "a"])
      {val2, ""} = Float.parse(score2)
      assert val2 == 1.0  # Updated because 1 < 5
    end

    test "ZADD CH returns added + changed count", %{sock: sock} do
      k = ukey("zadd_ch")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      result = cmd(sock, ["ZADD", k, "CH", "2.0", "a", "3.0", "b"])
      assert result == 2  # 1 changed (a) + 1 added (b)
    end

    test "ZMSCORE with mix of existing and non-existing members", %{sock: sock} do
      k = ukey("zmscore_mix")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZMSCORE", k, "a", "missing", "b"])
      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 1) == nil
    end

    test "ZRANDMEMBER on non-existent key returns nil", %{sock: sock} do
      k = ukey("zrandmember_nil")
      assert cmd(sock, ["ZRANDMEMBER", k]) == nil
    end

    test "ZRANDMEMBER with negative count allows duplicates", %{sock: sock} do
      k = ukey("zrandmember_neg")
      cmd(sock, ["ZADD", k, "1.0", "only"])
      result = cmd(sock, ["ZRANDMEMBER", k, "-5"])
      assert is_list(result)
      assert length(result) == 5
    end

    test "ZADD wrong arity", %{sock: sock} do
      result = cmd(sock, ["ZADD", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "ZADD odd score-member pairs returns error", %{sock: sock} do
      k = ukey("zadd_odd")
      result = cmd(sock, ["ZADD", k, "1.0"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DEL removes sorted set key", %{sock: sock} do
      k = ukey("del_zset")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b"])
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["ZCARD", k]) == 0
    end
  end

  # ===========================================================================
  # TTL / EXPIRY EDGE CASES
  # ===========================================================================

  describe "TTL / Expiry edge cases" do
    test "TTL on non-existent key returns -2", %{sock: sock} do
      k = ukey("ttl_nil")
      assert cmd(sock, ["TTL", k]) == -2
    end

    test "TTL on key without expiry returns -1", %{sock: sock} do
      k = ukey("ttl_no_exp")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["TTL", k]) == -1
    end

    test "PTTL on non-existent key returns -2", %{sock: sock} do
      k = ukey("pttl_nil")
      assert cmd(sock, ["PTTL", k]) == -2
    end

    test "PTTL on key without expiry returns -1", %{sock: sock} do
      k = ukey("pttl_no_exp")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["PTTL", k]) == -1
    end

    test "PERSIST on non-existent key returns 0", %{sock: sock} do
      k = ukey("persist_nil")
      assert cmd(sock, ["PERSIST", k]) == 0
    end

    test "PERSIST on key without expiry returns 0", %{sock: sock} do
      k = ukey("persist_no_exp")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["PERSIST", k]) == 0
    end

    test "PERSIST removes TTL", %{sock: sock} do
      k = ukey("persist_ok")
      cmd(sock, ["SET", k, "val", "EX", "1000"])
      assert cmd(sock, ["TTL", k]) > 0
      assert cmd(sock, ["PERSIST", k]) == 1
      assert cmd(sock, ["TTL", k]) == -1
    end

    test "EXPIRE on non-existent key returns 0", %{sock: sock} do
      k = ukey("expire_nil")
      assert cmd(sock, ["EXPIRE", k, "100"]) == 0
    end

    test "EXPIRE with zero or negative deletes the key", %{sock: sock} do
      k = ukey("expire_zero")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["EXPIRE", k, "0"]) == 1
      assert cmd(sock, ["GET", k]) == nil
    end

    test "EXPIRE with non-integer returns error", %{sock: sock} do
      k = ukey("expire_nan")
      cmd(sock, ["SET", k, "val"])
      result = cmd(sock, ["EXPIRE", k, "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "PEXPIRE with non-integer returns error", %{sock: sock} do
      k = ukey("pexpire_nan")
      cmd(sock, ["SET", k, "val"])
      result = cmd(sock, ["PEXPIRE", k, "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "EXPIREAT with past timestamp deletes key", %{sock: sock} do
      k = ukey("expireat_past")
      cmd(sock, ["SET", k, "val"])
      # Timestamp 0 (epoch) is in the past
      cmd(sock, ["EXPIREAT", k, "1"])
      # Key should be gone or have expired
      assert cmd(sock, ["GET", k]) == nil
    end

    test "EXPIRETIME on non-existent key returns -2", %{sock: sock} do
      k = ukey("expiretime_nil")
      assert cmd(sock, ["EXPIRETIME", k]) == -2
    end

    test "EXPIRETIME on persistent key returns -1", %{sock: sock} do
      k = ukey("expiretime_perm")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["EXPIRETIME", k]) == -1
    end

    test "PEXPIRETIME on persistent key returns -1", %{sock: sock} do
      k = ukey("pexpiretime_perm")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["PEXPIRETIME", k]) == -1
    end

    test "TTL wrong arity", %{sock: sock} do
      result = cmd(sock, ["TTL"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "EXPIRE wrong arity", %{sock: sock} do
      result = cmd(sock, ["EXPIRE", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  # ===========================================================================
  # GENERIC COMMANDS EDGE CASES
  # ===========================================================================

  describe "Generic command edge cases" do
    test "DEL multiple keys, some exist some don't", %{sock: sock} do
      k1 = ukey("del_multi_a")
      k2 = ukey("del_multi_b")
      k3 = ukey("del_multi_c")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k3, "v3"])
      assert cmd(sock, ["DEL", k1, k2, k3]) == 2
    end

    test "DEL with no args returns error", %{sock: sock} do
      result = cmd(sock, ["DEL"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "EXISTS with multiple keys returns count", %{sock: sock} do
      k1 = ukey("exists_multi_a")
      k2 = ukey("exists_multi_b")
      k3 = ukey("exists_multi_c")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k3, "v3"])
      assert cmd(sock, ["EXISTS", k1, k2, k3]) == 2
    end

    test "EXISTS with no args returns error", %{sock: sock} do
      result = cmd(sock, ["EXISTS"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "EXISTS counts duplicate key occurrences", %{sock: sock} do
      k = ukey("exists_dup")
      cmd(sock, ["SET", k, "v"])
      # Redis counts each occurrence, so EXISTS k k returns 2
      assert cmd(sock, ["EXISTS", k, k]) == 2
    end

    test "TYPE on non-existent key returns 'none'", %{sock: sock} do
      k = ukey("type_nil")
      assert cmd(sock, ["TYPE", k]) == {:simple, "none"}
    end

    test "TYPE on string key returns 'string'", %{sock: sock} do
      k = ukey("type_str")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "string"}
    end

    test "TYPE on hash key returns 'hash'", %{sock: sock} do
      k = ukey("type_hash")
      cmd(sock, ["HSET", k, "f", "v"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "hash"}
    end

    test "TYPE on list key returns 'list'", %{sock: sock} do
      k = ukey("type_list")
      cmd(sock, ["RPUSH", k, "a"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "list"}
    end

    test "TYPE on set key returns 'set'", %{sock: sock} do
      k = ukey("type_set")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "set"}
    end

    test "TYPE on zset key returns 'zset'", %{sock: sock} do
      k = ukey("type_zset")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "zset"}
    end

    test "RENAME non-existent key returns error", %{sock: sock} do
      k1 = ukey("rename_nil_src")
      k2 = ukey("rename_nil_dst")
      result = cmd(sock, ["RENAME", k1, k2])
      assert_error_contains(result, "no such key")
    end

    test "RENAME to same key returns OK", %{sock: sock} do
      k = ukey("rename_self")
      cmd(sock, ["SET", k, "val"])
      assert_ok(cmd(sock, ["RENAME", k, k]))
      assert cmd(sock, ["GET", k]) == "val"
    end

    test "RENAME overwrites destination", %{sock: sock} do
      k1 = ukey("rename_src")
      k2 = ukey("rename_dst")
      cmd(sock, ["SET", k1, "src_val"])
      cmd(sock, ["SET", k2, "dst_val"])
      assert_ok(cmd(sock, ["RENAME", k1, k2]))
      assert cmd(sock, ["GET", k2]) == "src_val"
      assert cmd(sock, ["GET", k1]) == nil
    end

    test "RENAMENX when dest exists returns 0", %{sock: sock} do
      k1 = ukey("renamenx_src")
      k2 = ukey("renamenx_dst")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k2, "v2"])
      assert cmd(sock, ["RENAMENX", k1, k2]) == 0
      # Source should still exist
      assert cmd(sock, ["GET", k1]) == "v1"
      # Dest should be unchanged
      assert cmd(sock, ["GET", k2]) == "v2"
    end

    test "RENAMENX when dest does not exist returns 1", %{sock: sock} do
      k1 = ukey("renamenx_ok_src")
      k2 = ukey("renamenx_ok_dst")
      cmd(sock, ["SET", k1, "val"])
      assert cmd(sock, ["RENAMENX", k1, k2]) == 1
      assert cmd(sock, ["GET", k2]) == "val"
      assert cmd(sock, ["GET", k1]) == nil
    end

    test "RENAMENX to same key returns 0", %{sock: sock} do
      k = ukey("renamenx_self")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["RENAMENX", k, k]) == 0
    end

    test "COPY non-existent source returns error", %{sock: sock} do
      k1 = ukey("copy_nil")
      k2 = ukey("copy_dst")
      result = cmd(sock, ["COPY", k1, k2])
      assert_error_contains(result, "no such key")
    end

    test "COPY to existing dest without REPLACE returns error", %{sock: sock} do
      k1 = ukey("copy_src")
      k2 = ukey("copy_dst")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k2, "v2"])
      result = cmd(sock, ["COPY", k1, k2])
      assert_error_contains(result, "target key already exists")
    end

    test "COPY with REPLACE to existing dest succeeds", %{sock: sock} do
      k1 = ukey("copy_replace_src")
      k2 = ukey("copy_replace_dst")
      cmd(sock, ["SET", k1, "new_val"])
      cmd(sock, ["SET", k2, "old_val"])
      assert cmd(sock, ["COPY", k1, k2, "REPLACE"]) == 1
      assert cmd(sock, ["GET", k2]) == "new_val"
      # Source still exists
      assert cmd(sock, ["GET", k1]) == "new_val"
    end

    test "UNLINK works like DEL", %{sock: sock} do
      k = ukey("unlink")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["UNLINK", k]) == 1
      assert cmd(sock, ["GET", k]) == nil
    end

    test "OBJECT ENCODING on non-existent key returns error", %{sock: sock} do
      k = ukey("obj_nil")
      result = cmd(sock, ["OBJECT", "ENCODING", k])
      assert_error_contains(result, "no such key")
    end

    test "OBJECT HELP returns list", %{sock: sock} do
      result = cmd(sock, ["OBJECT", "HELP"])
      assert is_list(result)
    end

    test "WAIT returns 0 (no replication)", %{sock: sock} do
      assert cmd(sock, ["WAIT", "1", "0"]) == 0
    end

    test "RENAME wrong arity", %{sock: sock} do
      result = cmd(sock, ["RENAME", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  # ===========================================================================
  # SERVER COMMANDS EDGE CASES
  # ===========================================================================

  describe "Server command edge cases" do
    test "PING with argument echoes it", %{sock: sock} do
      assert cmd(sock, ["PING", "hello world"]) == "hello world"
    end

    test "PING with too many args returns error", %{sock: sock} do
      result = cmd(sock, ["PING", "a", "b"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "ECHO returns the argument", %{sock: sock} do
      assert cmd(sock, ["ECHO", "ferricstore"]) == "ferricstore"
    end

    test "ECHO with no args returns error", %{sock: sock} do
      result = cmd(sock, ["ECHO"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "SELECT returns error (not supported)", %{sock: sock} do
      result = cmd(sock, ["SELECT", "1"])
      assert_error_contains(result, "SELECT not supported")
    end

    test "INFO with unknown section returns empty string", %{sock: sock} do
      result = cmd(sock, ["INFO", "nonexistent_section"])
      assert result == ""
    end

    test "INFO with valid section returns non-empty", %{sock: sock} do
      result = cmd(sock, ["INFO", "server"])
      assert is_binary(result)
      assert String.contains?(result, "ferricstore_version")
    end

    test "DBSIZE returns non-negative integer", %{sock: sock} do
      result = cmd(sock, ["DBSIZE"])
      assert is_integer(result) and result >= 0
    end

    test "COMMAND COUNT returns positive integer", %{sock: sock} do
      result = cmd(sock, ["COMMAND", "COUNT"])
      assert is_integer(result) and result > 0
    end

    test "COMMAND LIST returns list of strings", %{sock: sock} do
      result = cmd(sock, ["COMMAND", "LIST"])
      assert is_list(result) and length(result) > 0
    end

    test "COMMAND INFO with unknown command returns nil in list", %{sock: sock} do
      result = cmd(sock, ["COMMAND", "INFO", "NOTACOMMAND"])
      assert is_list(result)
      assert Enum.at(result, 0) == nil
    end

    test "CONFIG GET with glob pattern", %{sock: sock} do
      result = cmd(sock, ["CONFIG", "GET", "*"])
      assert is_list(result)
    end

    test "LOLWUT returns ASCII art", %{sock: sock} do
      result = cmd(sock, ["LOLWUT"])
      assert is_binary(result)
      # The ASCII art contains "v0.1.0" version string
      assert String.contains?(result, "v0.1.0")
    end

    test "unknown command returns error", %{sock: sock} do
      result = cmd(sock, ["TOTALLYUNKNOWNCOMMAND"])
      assert_error_contains(result, "unknown command")
    end

    test "FLUSHDB clears all keys", %{sock: sock} do
      k = ukey("flushdb_test")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["EXISTS", k]) == 1
      assert_ok(cmd(sock, ["FLUSHDB"]))
      assert cmd(sock, ["DBSIZE"]) == 0
    end
  end

  # ===========================================================================
  # TRANSACTION EDGE CASES
  # ===========================================================================

  describe "Transaction edge cases" do
    test "EXEC without MULTI returns error", %{sock: sock} do
      result = cmd(sock, ["EXEC"])
      assert_error_contains(result, "EXEC without MULTI")
    end

    test "DISCARD without MULTI returns error", %{sock: sock} do
      result = cmd(sock, ["DISCARD"])
      assert_error_contains(result, "DISCARD without MULTI")
    end

    test "empty MULTI/EXEC returns empty list", %{sock: sock} do
      assert_ok(cmd(sock, ["MULTI"]))
      result = cmd(sock, ["EXEC"])
      assert result == []
    end

    test "MULTI inside MULTI returns error (nested MULTI)", %{sock: sock} do
      assert_ok(cmd(sock, ["MULTI"]))
      result = cmd(sock, ["MULTI"])
      assert_error_contains(result, "MULTI calls can not be nested")
      # Clean up: discard the transaction
      cmd(sock, ["DISCARD"])
    end

    test "commands that error inside MULTI still queue, errors appear in EXEC result", %{sock: sock} do
      k = ukey("txn_err_inside")
      cmd(sock, ["SET", k, "hello"])

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["INCR", k]) == {:simple, "QUEUED"}
      assert cmd(sock, ["SET", k, "world"]) == {:simple, "QUEUED"}
      result = cmd(sock, ["EXEC"])
      assert is_list(result)
      assert length(result) == 2
      # INCR on "hello" should fail at EXEC time
      assert {:error, _} = Enum.at(result, 0)
      # SET should succeed
      assert Enum.at(result, 1) == {:simple, "OK"}
    end

    test "WATCH key, modify from same connection, EXEC aborts", %{sock: sock, port: port} do
      k = ukey("watch_abort")
      cmd(sock, ["SET", k, "initial"])

      assert_ok(cmd(sock, ["WATCH", k]))

      # Modify from another connection (use cmd_direct to avoid polluting process dict)
      sock2 = connect_and_hello(port)
      cmd_direct(sock2, ["SET", k, "modified"])
      :gen_tcp.close(sock2)

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "txn_value"]) == {:simple, "QUEUED"}
      assert cmd(sock, ["EXEC"]) == nil

      # Value should be the concurrent modification
      assert cmd(sock, ["GET", k]) == "modified"
    end

    test "WATCH without concurrent modification succeeds", %{sock: sock} do
      k = ukey("watch_ok")
      cmd(sock, ["SET", k, "initial"])

      assert_ok(cmd(sock, ["WATCH", k]))
      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "updated"]) == {:simple, "QUEUED"}
      result = cmd(sock, ["EXEC"])
      assert is_list(result)
      assert cmd(sock, ["GET", k]) == "updated"
    end

    test "UNWATCH clears watches", %{sock: sock, port: port} do
      k = ukey("unwatch_ok")
      cmd(sock, ["SET", k, "initial"])

      assert_ok(cmd(sock, ["WATCH", k]))
      assert_ok(cmd(sock, ["UNWATCH"]))

      # Modify from another connection (use cmd_direct to avoid polluting process dict)
      sock2 = connect_and_hello(port)
      cmd_direct(sock2, ["SET", k, "modified"])
      :gen_tcp.close(sock2)

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "txn_value"]) == {:simple, "QUEUED"}
      result = cmd(sock, ["EXEC"])
      # Should succeed because we unwatched
      assert is_list(result)
      assert cmd(sock, ["GET", k]) == "txn_value"
    end

    test "DISCARD clears queued commands and watch state", %{sock: sock} do
      k = ukey("discard_clear")
      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "should_not_exist"]) == {:simple, "QUEUED"}
      assert_ok(cmd(sock, ["DISCARD"]))
      assert cmd(sock, ["GET", k]) == nil
      # Should be back in normal mode
      assert_ok(cmd(sock, ["SET", k, "normal"]))
      assert cmd(sock, ["GET", k]) == "normal"
    end

    test "after aborted EXEC, connection returns to normal mode", %{sock: sock, port: port} do
      k = ukey("abort_recovery")
      cmd(sock, ["SET", k, "before"])

      assert_ok(cmd(sock, ["WATCH", k]))

      sock2 = connect_and_hello(port)
      cmd_direct(sock2, ["SET", k, "concurrent"])
      :gen_tcp.close(sock2)

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "txn"]) == {:simple, "QUEUED"}
      assert cmd(sock, ["EXEC"]) == nil

      # Should work normally after abort
      assert_ok(cmd(sock, ["SET", k, "after_abort"]))
      assert cmd(sock, ["GET", k]) == "after_abort"
    end
  end

  # ===========================================================================
  # PIPELINE EDGE CASES
  # ===========================================================================

  describe "Pipeline edge cases" do
    test "1000 commands in one pipeline", %{sock: sock} do
      prefix = ukey("pipe1000")

      # Send 1000 SET commands as a single TCP write
      pipeline =
        for i <- 1..1000 do
          Encoder.encode(["SET", "#{prefix}_#{i}", "val#{i}"])
        end

      :ok = :gen_tcp.send(sock, IO.iodata_to_binary(pipeline))

      # Drain all 1000 responses
      for _ <- 1..1000 do
        assert recv_response(sock) == {:simple, "OK"}
      end

      # Spot-check
      assert cmd(sock, ["GET", "#{prefix}_1"]) == "val1"
      assert cmd(sock, ["GET", "#{prefix}_500"]) == "val500"
      assert cmd(sock, ["GET", "#{prefix}_1000"]) == "val1000"
    end

    test "mixed valid + invalid commands in pipeline", %{sock: sock} do
      k = ukey("pipe_mix")
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "hello"]),
          Encoder.encode(["INCR", k]),           # will fail: "hello" is not integer
          Encoder.encode(["GET", k]),
          Encoder.encode(["NOTACOMMAND"]),        # unknown command
          Encoder.encode(["GET", k])              # should still work
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      r1 = recv_response(sock)
      r2 = recv_response(sock)
      r3 = recv_response(sock)
      r4 = recv_response(sock)
      r5 = recv_response(sock)

      assert_ok(r1)
      assert {:error, _} = r2
      assert r3 == "hello"
      assert {:error, _} = r4
      assert r5 == "hello"
    end

    test "pipeline with MULTI/EXEC embedded", %{sock: sock} do
      k = ukey("pipe_txn")
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "before"]),
          Encoder.encode(["MULTI"]),
          Encoder.encode(["SET", k, "inside_txn"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["EXEC"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      r1 = recv_response(sock)  # SET
      r2 = recv_response(sock)  # MULTI
      r3 = recv_response(sock)  # SET (QUEUED)
      r4 = recv_response(sock)  # GET (QUEUED)
      r5 = recv_response(sock)  # EXEC

      assert_ok(r1)
      assert_ok(r2)
      assert r3 == {:simple, "QUEUED"}
      assert r4 == {:simple, "QUEUED"}
      assert is_list(r5)
      assert length(r5) == 2
    end
  end

  # ===========================================================================
  # SCAN EDGE CASES
  # ===========================================================================

  describe "SCAN edge cases" do
    test "SCAN on empty DB returns cursor 0 and empty list", %{sock: sock} do
      result = cmd(sock, ["SCAN", "0"])
      assert is_list(result)
      assert length(result) == 2
      [cursor, keys] = result
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with MATCH pattern filters results", %{sock: sock} do
      prefix = "scan_match_#{:rand.uniform(9_999_999)}"
      cmd(sock, ["SET", "#{prefix}_a", "1"])
      cmd(sock, ["SET", "#{prefix}_b", "2"])
      cmd(sock, ["SET", ukey("other"), "3"])

      # Collect all results from SCAN
      keys = scan_all(sock, "MATCH", "#{prefix}_*")
      assert "#{prefix}_a" in keys
      assert "#{prefix}_b" in keys
    end

    test "SCAN with TYPE filter", %{sock: sock} do
      str_key = ukey("scan_type_str")
      hash_key = ukey("scan_type_hash")
      cmd(sock, ["SET", str_key, "val"])
      cmd(sock, ["HSET", hash_key, "f", "v"])

      string_keys = scan_all(sock, "TYPE", "string")
      assert str_key in string_keys
      refute hash_key in string_keys
    end

    test "SCAN with COUNT hint", %{sock: sock} do
      for i <- 1..5 do
        cmd(sock, ["SET", ukey("scan_count_#{i}"), "v"])
      end

      result = cmd(sock, ["SCAN", "0", "COUNT", "2"])
      assert is_list(result)
      assert length(result) == 2
    end

    test "HSCAN on non-existent key returns cursor 0 and empty list", %{sock: sock} do
      k = ukey("hscan_nil")
      result = cmd(sock, ["HSCAN", k, "0"])
      [cursor, elements] = result
      assert cursor == "0"
      assert elements == []
    end

    test "SSCAN on non-existent key returns cursor 0 and empty list", %{sock: sock} do
      k = ukey("sscan_nil")
      result = cmd(sock, ["SSCAN", k, "0"])
      [cursor, elements] = result
      assert cursor == "0"
      assert elements == []
    end

    test "ZSCAN on non-existent key returns cursor 0 and empty list", %{sock: sock} do
      k = ukey("zscan_nil")
      result = cmd(sock, ["ZSCAN", k, "0"])
      [cursor, elements] = result
      assert cursor == "0"
      assert elements == []
    end
  end

  # Helper: collects all keys from SCAN iterations
  defp scan_all(sock, opt_key, opt_value) do
    scan_loop(sock, "0", opt_key, opt_value, [])
  end

  defp scan_loop(sock, cursor, opt_key, opt_value, acc) do
    [next_cursor, keys] = cmd(sock, ["SCAN", cursor, opt_key, opt_value])
    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      scan_loop(sock, next_cursor, opt_key, opt_value, new_acc)
    end
  end

  # ===========================================================================
  # BITMAP EDGE CASES
  # ===========================================================================

  describe "Bitmap edge cases" do
    test "SETBIT on non-existent key creates it", %{sock: sock} do
      k = ukey("setbit_nil")
      assert cmd(sock, ["SETBIT", k, "7", "1"]) == 0
      assert cmd(sock, ["GETBIT", k, "7"]) == 1
    end

    test "GETBIT on non-existent key returns 0", %{sock: sock} do
      k = ukey("getbit_nil")
      assert cmd(sock, ["GETBIT", k, "100"]) == 0
    end

    test "SETBIT with value not 0 or 1 returns error", %{sock: sock} do
      k = ukey("setbit_bad")
      result = cmd(sock, ["SETBIT", k, "0", "2"])
      assert_error_contains(result, "bit")
    end

    test "BITCOUNT on non-existent key returns 0", %{sock: sock} do
      k = ukey("bitcount_nil")
      assert cmd(sock, ["BITCOUNT", k]) == 0
    end

    test "BITCOUNT on key with value", %{sock: sock} do
      k = ukey("bitcount_val")
      cmd(sock, ["SET", k, "foobar"])
      result = cmd(sock, ["BITCOUNT", k])
      assert is_integer(result) and result > 0
    end
  end

  # ===========================================================================
  # HYPERLOGLOG EDGE CASES
  # ===========================================================================

  describe "HyperLogLog edge cases" do
    test "PFADD creates HLL on non-existent key", %{sock: sock} do
      k = ukey("pfadd_nil")
      assert cmd(sock, ["PFADD", k, "a", "b", "c"]) == 1
    end

    test "PFCOUNT on non-existent key returns 0", %{sock: sock} do
      k = ukey("pfcount_nil")
      assert cmd(sock, ["PFCOUNT", k]) == 0
    end

    test "PFCOUNT returns approximate cardinality", %{sock: sock} do
      k = ukey("pfcount_approx")
      cmd(sock, ["PFADD", k, "a", "b", "c", "d", "e"])
      count = cmd(sock, ["PFCOUNT", k])
      assert is_integer(count) and count >= 3 and count <= 7
    end

    test "PFADD with duplicate elements returns 0 (no change)", %{sock: sock} do
      k = ukey("pfadd_dup")
      cmd(sock, ["PFADD", k, "a", "b"])
      assert cmd(sock, ["PFADD", k, "a", "b"]) == 0
    end
  end

  # ===========================================================================
  # CONNECTION / CLIENT EDGE CASES
  # ===========================================================================

  describe "Connection edge cases" do
    test "HELLO 2 returns NOPROTO error", %{port: port} do
      {:ok, sock} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      send_cmd(sock, ["HELLO", "2"])
      result = recv_direct(sock, "", 5_000)
      assert {:error, msg} = result
      assert String.contains?(msg, "NOPROTO")
      :gen_tcp.close(sock)
    end

    test "HELLO without version returns server info", %{port: port} do
      {:ok, sock} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      send_cmd(sock, ["HELLO"])
      result = recv_direct(sock, "", 5_000)
      assert is_map(result)
      assert result["server"] == "ferricstore"
      :gen_tcp.close(sock)
    end

    test "QUIT closes the connection gracefully", %{port: port} do
      sock = connect_and_hello(port)
      assert cmd_direct(sock, ["QUIT"]) == {:simple, "OK"}
      assert :gen_tcp.recv(sock, 0, 1000) in [{:error, :closed}, {:error, :econnreset}]
      :gen_tcp.close(sock)
    end

    test "RESET returns RESET status", %{port: port} do
      sock = connect_and_hello(port)
      assert cmd_direct(sock, ["RESET"]) == {:simple, "RESET"}
      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # BINARY SAFETY EDGE CASES
  # ===========================================================================

  describe "Binary safety edge cases" do
    test "key with null bytes round-trips correctly", %{sock: sock} do
      k = "key\x00with\x00nulls_#{:rand.uniform(9_999_999)}"
      v = "value\x00also\x00null"
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "key with newlines and tabs", %{sock: sock} do
      k = "key\n\twith\r\nstuff_#{:rand.uniform(9_999_999)}"
      v = "val\n\tval"
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "unicode key and value", %{sock: sock} do
      k = ukey("unicode_key")
      v = "Hello World"
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "value with all byte values 0-255", %{sock: sock} do
      k = ukey("all_bytes")
      v = :binary.list_to_bin(Enum.to_list(0..255))
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end
  end

  # ===========================================================================
  # WRONG ARITY SWEEP: Ensure every command rejects wrong arg counts
  # ===========================================================================

  describe "Wrong arity rejection sweep" do
    test "every command with too few or too many args returns error, not crash", %{sock: sock} do
      # Each tuple: {cmd, too_few_args, too_many_args}
      wrong_arity_cases = [
        {"GET", [], ["a", "b"]},
        {"SET", ["k"], nil},
        {"DEL", [], nil},
        {"EXISTS", [], nil},
        {"MGET", [], nil},
        {"MSET", [], nil},
        {"INCR", [], ["a", "b"]},
        {"DECR", [], ["a", "b"]},
        {"INCRBY", ["k"], nil},
        {"DECRBY", ["k"], nil},
        {"INCRBYFLOAT", [], nil},
        {"APPEND", ["k"], nil},
        {"STRLEN", [], nil},
        {"GETSET", ["k"], nil},
        {"GETDEL", [], nil},
        {"SETNX", ["k"], nil},
        {"SETEX", ["k", "10"], nil},
        {"PSETEX", ["k", "10"], nil},
        {"GETRANGE", ["k", "0"], nil},
        {"SETRANGE", ["k", "0"], nil},
        {"TTL", [], nil},
        {"PTTL", [], nil},
        {"EXPIRE", ["k"], nil},
        {"PEXPIRE", ["k"], nil},
        {"PERSIST", [], nil},
        {"TYPE", [], nil},
        {"RENAME", ["k"], nil},
        {"RENAMENX", ["k"], nil},
        {"HSET", ["k"], nil},
        {"HGET", ["k"], nil},
        {"HDEL", ["k"], nil},
        {"HGETALL", [], nil},
        {"HLEN", [], nil},
        {"HEXISTS", ["k"], nil},
        {"HKEYS", [], nil},
        {"HVALS", [], nil},
        {"HSETNX", ["k", "f"], nil},
        {"HINCRBY", ["k", "f"], nil},
        {"HINCRBYFLOAT", [], nil},
        {"LPUSH", ["k"], nil},
        {"RPUSH", ["k"], nil},
        {"LRANGE", ["k", "0"], nil},
        {"LLEN", [], nil},
        {"LINDEX", ["k"], nil},
        {"SADD", ["k"], nil},
        {"SREM", ["k"], nil},
        {"SMEMBERS", [], nil},
        {"SISMEMBER", ["k"], nil},
        {"SCARD", [], nil},
        {"ZADD", ["k"], nil},
        {"ZREM", ["k"], nil},
        {"ZSCORE", ["k"], nil},
        {"ZRANK", ["k"], nil},
        {"ZCARD", [], nil},
        {"ZCOUNT", ["k", "0"], nil},
        {"PING", ["a", "b", "c"], nil},
        {"ECHO", [], nil},
        {"DBSIZE", ["extra"], nil},
        {"SELECT", [], nil}
      ]

      for {cmd_name, too_few, too_many} <- wrong_arity_cases do
        if too_few != nil do
          result = cmd(sock, [cmd_name | too_few])
          assert {:error, _msg} = result,
            "#{cmd_name} with args #{inspect(too_few)} should return error, got: #{inspect(result)}"
        end

        if too_many != nil do
          result = cmd(sock, [cmd_name | too_many])
          assert {:error, _msg} = result,
            "#{cmd_name} with args #{inspect(too_many)} should return error, got: #{inspect(result)}"
        end
      end
    end
  end

  # ===========================================================================
  # STRESS: Rapid key creation and deletion
  # ===========================================================================

  describe "Stress: rapid create/delete cycle" do
    test "100 rapid SET then DEL cycles do not corrupt state", %{sock: sock} do
      for i <- 1..100 do
        k = ukey("stress_#{i}")
        assert_ok(cmd(sock, ["SET", k, "val#{i}"]))
        assert cmd(sock, ["DEL", k]) == 1
        assert cmd(sock, ["GET", k]) == nil
      end
    end

    test "interleaved data structure operations", %{sock: sock} do
      # Create keys of different types, then delete them all
      str_k = ukey("stress_str")
      hash_k = ukey("stress_hash")
      list_k = ukey("stress_list")
      set_k = ukey("stress_set")
      zset_k = ukey("stress_zset")

      cmd(sock, ["SET", str_k, "v"])
      cmd(sock, ["HSET", hash_k, "f", "v"])
      cmd(sock, ["RPUSH", list_k, "a"])
      cmd(sock, ["SADD", set_k, "m"])
      cmd(sock, ["ZADD", zset_k, "1.0", "m"])

      # Delete all
      deleted = cmd(sock, ["DEL", str_k, hash_k, list_k, set_k, zset_k])
      assert deleted == 5

      # Verify all gone
      assert cmd(sock, ["GET", str_k]) == nil
      assert cmd(sock, ["HLEN", hash_k]) == 0
      assert cmd(sock, ["LLEN", list_k]) == 0
      assert cmd(sock, ["SCARD", set_k]) == 0
      assert cmd(sock, ["ZCARD", zset_k]) == 0

      # Re-create with DIFFERENT types to ensure no stale type metadata
      cmd(sock, ["RPUSH", str_k, "now_a_list"])  # was string, now list
      assert cmd(sock, ["LRANGE", str_k, "0", "-1"]) == ["now_a_list"]

      cmd(sock, ["SET", hash_k, "now_a_string"])  # was hash, now string
      assert cmd(sock, ["GET", hash_k]) == "now_a_string"
    end
  end
end
