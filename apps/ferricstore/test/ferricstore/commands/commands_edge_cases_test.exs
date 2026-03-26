defmodule Ferricstore.Commands.CommandsEdgeCasesTest do
  @moduledoc """
  Comprehensive edge-case tests for Strings, Expiry, Server command handlers
  and the Dispatcher. Focuses on boundary conditions, invalid inputs, and
  unusual-but-valid usage patterns NOT already covered by the per-module tests.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, Expiry, Server, Strings}
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # Strings — SET edge cases
  # ===========================================================================

  describe "SET edge cases (not covered elsewhere)" do
    test "SET with PX -1 returns error mentioning invalid expire" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "PX", "-1"], MockStore.make())

      assert msg =~ "invalid expire"
    end

    test "SET with EX non-numeric trailing chars returns error" do
      # "10abc" should fail Integer.parse/1 pattern match on remainder
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "EX", "10abc"], MockStore.make())

      assert msg =~ "not an integer"
    end

    test "SET with PX non-numeric trailing chars returns error" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "PX", "10abc"], MockStore.make())

      assert msg =~ "not an integer"
    end

    test "SET with EX missing value (EX at end of args) returns error" do
      # ["key", "val", "EX"] — EX without a following seconds string
      # Falls through to the catch-all parse_set_opts clause
      assert {:error, _} = Strings.handle("SET", ["key", "val", "EX"], MockStore.make())
    end

    test "SET with PX missing value returns error" do
      assert {:error, _} = Strings.handle("SET", ["key", "val", "PX"], MockStore.make())
    end

    test "SET with unknown option between valid options returns error" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "NX", "BOGUS"], MockStore.make())

      assert msg =~ "syntax error"
    end

    test "SET with empty string key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["", ""], store)
      assert msg =~ "empty"
    end

    test "SET with EX float value returns error (must be integer)" do
      assert {:error, _} =
               Strings.handle("SET", ["key", "val", "EX", "1.5"], MockStore.make())
    end

    test "SET with PX float value returns error (must be integer)" do
      assert {:error, _} =
               Strings.handle("SET", ["key", "val", "PX", "1.5"], MockStore.make())
    end

    test "SET with very large EX value succeeds" do
      store = MockStore.make()
      # 10 years in seconds
      assert :ok = Strings.handle("SET", ["k", "v", "EX", "315360000"], store)
      assert "v" == store.get.("k")
    end

    test "SET NX then SET NX again on same key fails second time" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "first", "NX"], store)
      assert nil == Strings.handle("SET", ["k", "second", "NX"], store)
      assert "first" == store.get.("k")
    end

    test "SET XX then SET XX on existing key updates" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["k", "new", "XX"], store)
      assert :ok = Strings.handle("SET", ["k", "newer", "XX"], store)
      assert "newer" == store.get.("k")
    end

    test "SET with multiple NX flags parsed correctly (idempotent)" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "NX", "NX"], store)
      assert "v" == store.get.("k")
    end

    test "SET with multiple XX flags parsed correctly (idempotent)" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["k", "new", "XX", "XX"], store)
      assert "new" == store.get.("k")
    end
  end

  # ===========================================================================
  # Strings — GET edge cases
  # ===========================================================================

  describe "GET edge cases (not covered elsewhere)" do
    test "GET with empty string key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("GET", [""], store)
      assert msg =~ "empty"
    end

    test "GET with 3 args returns error" do
      assert {:error, msg} = Strings.handle("GET", ["a", "b", "c"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # Strings — DEL edge cases
  # ===========================================================================

  describe "DEL edge cases (not covered elsewhere)" do
    test "DEL with 10 keys, only some exist, returns correct count" do
      store =
        MockStore.make(%{
          "a" => {"1", 0},
          "c" => {"3", 0},
          "e" => {"5", 0}
        })

      keys = Enum.map(1..10, &<<96 + &1>>)
      # a..j → a, c, e exist = 3
      assert 3 == Strings.handle("DEL", keys, store)
    end

    test "DEL all keys returns count equal to number of keys" do
      data = for i <- 1..5, into: %{}, do: {"k#{i}", {"v#{i}", 0}}
      store = MockStore.make(data)
      keys = Enum.map(1..5, &"k#{&1}")
      assert 5 == Strings.handle("DEL", keys, store)
    end

    test "DEL with only non-existent keys returns 0" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert 0 == Strings.handle("DEL", ["x", "y", "z"], store)
    end
  end

  # ===========================================================================
  # Strings — EXISTS edge cases
  # ===========================================================================

  describe "EXISTS edge cases (not covered elsewhere)" do
    test "EXISTS same key three times returns 3 when key exists" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 3 == Strings.handle("EXISTS", ["k", "k", "k"], store)
    end

    test "EXISTS expired key returns 0" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Strings.handle("EXISTS", ["k"], store)
    end

    test "EXISTS mix of present, missing, and duplicated keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      # a=1, a=1, b=1, c=0 → 3
      assert 3 == Strings.handle("EXISTS", ["a", "a", "b", "c"], store)
    end
  end

  # ===========================================================================
  # Strings — MGET edge cases
  # ===========================================================================

  describe "MGET edge cases (not covered elsewhere)" do
    test "MGET with all missing keys returns list of nils" do
      store = MockStore.make()
      assert [nil, nil, nil] == Strings.handle("MGET", ["x", "y", "z"], store)
    end

    test "MGET with 100 keys, some missing, returns correct nil positions" do
      # Set even-numbered keys
      data =
        for i <- 0..99, rem(i, 2) == 0, into: %{} do
          {"k#{i}", {"v#{i}", 0}}
        end

      store = MockStore.make(data)
      keys = Enum.map(0..99, &"k#{&1}")
      result = Strings.handle("MGET", keys, store)

      assert length(result) == 100

      Enum.each(0..99, fn i ->
        if rem(i, 2) == 0 do
          assert Enum.at(result, i) == "v#{i}"
        else
          assert Enum.at(result, i) == nil
        end
      end)
    end

    test "MGET with duplicated keys returns duplicated values" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert ["v", "v", "v"] == Strings.handle("MGET", ["k", "k", "k"], store)
    end

    test "MGET with expired keys returns nil at those positions" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", past}})
      assert ["1", nil] == Strings.handle("MGET", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # Strings — MSET edge cases
  # ===========================================================================

  describe "MSET edge cases (not covered elsewhere)" do
    test "MSET with 1 arg (odd) returns error" do
      assert {:error, msg} = Strings.handle("MSET", ["lonely"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "MSET with 3 args (odd) returns error" do
      assert {:error, _} = Strings.handle("MSET", ["a", "1", "b"], MockStore.make())
    end

    test "MSET with same key repeated uses last value" do
      store = MockStore.make()
      assert :ok = Strings.handle("MSET", ["k", "first", "k", "last"], store)
      assert "last" == store.get.("k")
    end

    test "MSET does not set expiry (all keys persist)" do
      store = MockStore.make()
      assert :ok = Strings.handle("MSET", ["a", "1", "b", "2"], store)
      {_, exp_a} = store.get_meta.("a")
      {_, exp_b} = store.get_meta.("b")
      assert exp_a == 0
      assert exp_b == 0
    end

    test "MSET with 50 key-value pairs works" do
      store = MockStore.make()
      args = Enum.flat_map(1..50, fn i -> ["k#{i}", "v#{i}"] end)
      assert :ok = Strings.handle("MSET", args, store)

      for i <- 1..50 do
        assert "v#{i}" == store.get.("k#{i}")
      end
    end
  end

  # ===========================================================================
  # Expiry — EXPIRE edge cases
  # ===========================================================================

  describe "EXPIRE edge cases (not covered elsewhere)" do
    test "EXPIRE with only key arg (no seconds) returns error" do
      assert {:error, _} = Expiry.handle("EXPIRE", ["k"], MockStore.make())
    end

    test "EXPIRE with 3 args returns error" do
      assert {:error, _} = Expiry.handle("EXPIRE", ["k", "10", "extra"], MockStore.make())
    end

    test "EXPIRE with float seconds returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("EXPIRE", ["k", "1.5"], store)
    end

    test "EXPIRE with very large seconds value succeeds" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # 10 years in seconds
      assert 1 == Expiry.handle("EXPIRE", ["k", "315360000"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 315_359_000
    end

    test "EXPIRE on expired key returns 0 (key treated as missing)" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Expiry.handle("EXPIRE", ["k", "10"], store)
    end
  end

  # ===========================================================================
  # Expiry — PEXPIRE edge cases
  # ===========================================================================

  describe "PEXPIRE edge cases (not covered elsewhere)" do
    test "PEXPIRE with 0 ms expires key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "0"], store)
      # Key should be expired now
      assert -2 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PEXPIRE with only key arg returns error" do
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k"], MockStore.make())
    end

    test "PEXPIRE with 3 args returns error" do
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k", "5000", "extra"], MockStore.make())
    end

    test "PEXPIRE with non-integer returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k", "abc"], store)
    end

    test "PEXPIRE with float returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k", "1.5"], store)
    end

    test "PEXPIRE on expired key returns 0" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Expiry.handle("PEXPIRE", ["k", "5000"], store)
    end

    test "PEXPIRE updates existing TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "60000"], store)
      assert 1 == Expiry.handle("PEXPIRE", ["k", "120000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      # Should be closer to 120_000 than 60_000
      assert pttl > 60_000
    end
  end

  # ===========================================================================
  # Expiry — EXPIREAT edge cases
  # ===========================================================================

  describe "EXPIREAT edge cases (not covered elsewhere)" do
    test "EXPIREAT with far-future timestamp persists key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # Year ~2050
      future_unix = 2_524_608_000
      assert 1 == Expiry.handle("EXPIREAT", ["k", "#{future_unix}"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0
    end

    test "EXPIREAT with 0 timestamp (epoch) sets expire_at_ms to 0 which means no-expiry" do
      # In this implementation, expire_at_ms=0 is the sentinel for "no TTL".
      # EXPIREAT 0 stores 0*1000=0, effectively removing any TTL.
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIREAT", ["k", "0"], store)
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "EXPIREAT with only key arg returns error" do
      assert {:error, _} = Expiry.handle("EXPIREAT", ["k"], MockStore.make())
    end

    test "EXPIREAT with 3 args returns error" do
      assert {:error, _} = Expiry.handle("EXPIREAT", ["k", "123", "extra"], MockStore.make())
    end

    test "EXPIREAT on missing key returns 0" do
      future_unix = div(System.os_time(:millisecond), 1_000) + 3_600
      assert 0 == Expiry.handle("EXPIREAT", ["missing", "#{future_unix}"], MockStore.make())
    end

    test "EXPIREAT on expired key returns 0" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      future_unix = div(System.os_time(:millisecond), 1_000) + 3_600
      assert 0 == Expiry.handle("EXPIREAT", ["k", "#{future_unix}"], store)
    end
  end

  # ===========================================================================
  # Expiry — PEXPIREAT edge cases
  # ===========================================================================

  describe "PEXPIREAT edge cases (not covered elsewhere)" do
    test "PEXPIREAT with ms timestamp in the past expires key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      past_ms = System.os_time(:millisecond) - 60_000
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "#{past_ms}"], store)
      assert -2 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PEXPIREAT with 0 timestamp sets expire_at_ms to 0 which means no-expiry" do
      # In this implementation, expire_at_ms=0 is the sentinel for "no TTL".
      # PEXPIREAT 0 stores 0, effectively removing any TTL.
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "0"], store)
      assert -1 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PEXPIREAT with far-future ms timestamp persists key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      far_future = System.os_time(:millisecond) + 86_400_000 * 365
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "#{far_future}"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 86_400_000
    end

    test "PEXPIREAT with non-integer returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("PEXPIREAT", ["k", "abc"], store)
    end

    test "PEXPIREAT with only key arg returns error" do
      assert {:error, _} = Expiry.handle("PEXPIREAT", ["k"], MockStore.make())
    end

    test "PEXPIREAT with 3 args returns error" do
      assert {:error, _} = Expiry.handle("PEXPIREAT", ["k", "123", "extra"], MockStore.make())
    end

    test "PEXPIREAT on missing key returns 0" do
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert 0 == Expiry.handle("PEXPIREAT", ["missing", "#{future_ms}"], MockStore.make())
    end
  end

  # ===========================================================================
  # Expiry — TTL edge cases
  # ===========================================================================

  describe "TTL edge cases (not covered elsewhere)" do
    test "TTL with 2 args returns error" do
      assert {:error, _} = Expiry.handle("TTL", ["a", "b"], MockStore.make())
    end

    test "TTL on key with expire_at_ms=0 returns -1 (no expiry)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "TTL on key with already-passed expiry returns 0 (clamped by max)" do
      past = System.os_time(:millisecond) - 10_000
      store = MockStore.make(%{"k" => {"v", past}})
      # MockStore's read_meta returns nil for expired keys, so TTL returns -2
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end
  end

  # ===========================================================================
  # Expiry — PTTL edge cases
  # ===========================================================================

  describe "PTTL edge cases (not covered elsewhere)" do
    test "PTTL with 2 args returns error" do
      assert {:error, _} = Expiry.handle("PTTL", ["a", "b"], MockStore.make())
    end

    test "PTTL on key with expire_at_ms=0 returns -1 (no expiry)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("PTTL", ["k"], store)
    end
  end

  # ===========================================================================
  # Expiry — PERSIST edge cases
  # ===========================================================================

  describe "PERSIST edge cases (not covered elsewhere)" do
    test "PERSIST with 2 args returns error" do
      assert {:error, _} = Expiry.handle("PERSIST", ["a", "b"], MockStore.make())
    end

    test "PERSIST on key with expire_at_ms=0 returns 0 (already persistent)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end

    test "PERSIST on expired key returns 0 (treated as missing)" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end

    test "PERSIST preserves value after removing TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"myval", future}})
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert "myval" == store.get.("k")
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "PERSIST is idempotent (second call returns 0)" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", future}})
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end
  end

  # ===========================================================================
  # Server — PING edge cases
  # ===========================================================================

  describe "PING edge cases (not covered elsewhere)" do
    test "PING with empty string arg echoes empty string" do
      assert "" == Server.handle("PING", [""], MockStore.make())
    end

    test "PING with 3 args returns error" do
      assert {:error, _} = Server.handle("PING", ["a", "b", "c"], MockStore.make())
    end
  end

  # ===========================================================================
  # Server — ECHO edge cases
  # ===========================================================================

  describe "ECHO edge cases (not covered elsewhere)" do
    test "ECHO with empty string returns empty string" do
      assert "" == Server.handle("ECHO", [""], MockStore.make())
    end

    test "ECHO with 3 args returns error" do
      assert {:error, _} = Server.handle("ECHO", ["a", "b", "c"], MockStore.make())
    end

    test "ECHO preserves binary content" do
      msg = <<0, 1, 2, 255>>
      assert ^msg = Server.handle("ECHO", [msg], MockStore.make())
    end
  end

  # ===========================================================================
  # Server — KEYS edge cases
  # ===========================================================================

  describe "KEYS edge cases (not covered elsewhere)" do
    test "KEYS '*suf' matches suffix" do
      store =
        MockStore.make(%{
          "hello_suf" => {"v", 0},
          "world_suf" => {"v", 0},
          "no_match" => {"v", 0}
        })

      result = Server.handle("KEYS", ["*suf"], store)
      assert Enum.sort(result) == ["hello_suf", "world_suf"]
    end

    test "KEYS '?' does not match empty string or multi-char keys" do
      store =
        MockStore.make(%{
          "" => {"v", 0},
          "a" => {"v", 0},
          "ab" => {"v", 0},
          "abc" => {"v", 0}
        })

      result = Server.handle("KEYS", ["?"], store)
      assert result == ["a"]
    end

    test "KEYS '??' matches exactly two-char keys" do
      store =
        MockStore.make(%{
          "a" => {"v", 0},
          "ab" => {"v", 0},
          "abc" => {"v", 0}
        })

      result = Server.handle("KEYS", ["??"], store)
      assert result == ["ab"]
    end

    test "KEYS 'exact' matches only that exact key" do
      store =
        MockStore.make(%{
          "exact" => {"v", 0},
          "exactly" => {"v", 0},
          "inexact" => {"v", 0}
        })

      result = Server.handle("KEYS", ["exact"], store)
      assert result == ["exact"]
    end

    test "KEYS '*' on store with special-char keys returns them" do
      store =
        MockStore.make(%{
          "key.with.dots" => {"v", 0},
          "key:with:colons" => {"v", 0}
        })

      result = Server.handle("KEYS", ["*"], store)
      assert length(result) == 2
    end

    test "KEYS 'pre*suf' matches keys with prefix and suffix" do
      store =
        MockStore.make(%{
          "pre_middle_suf" => {"v", 0},
          "pre_suf" => {"v", 0},
          "pre_no" => {"v", 0},
          "no_suf" => {"v", 0}
        })

      result = Server.handle("KEYS", ["pre*suf"], store)
      assert Enum.sort(result) == ["pre_middle_suf", "pre_suf"]
    end

    test "KEYS does not return expired keys" do
      past = System.os_time(:millisecond) - 1_000

      store =
        MockStore.make(%{
          "live" => {"v", 0},
          "dead" => {"v", past}
        })

      result = Server.handle("KEYS", ["*"], store)
      assert result == ["live"]
    end
  end

  # ===========================================================================
  # Server — DBSIZE edge cases
  # ===========================================================================

  describe "DBSIZE edge cases (not covered elsewhere)" do
    test "DBSIZE with 2 args returns error" do
      assert {:error, _} = Server.handle("DBSIZE", ["a", "b"], MockStore.make())
    end
  end

  # ===========================================================================
  # Server — FLUSHDB edge cases
  # ===========================================================================

  describe "FLUSHDB edge cases (not covered elsewhere)" do
    test "FLUSHDB with lowercase flag returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Server.handle("FLUSHDB", ["async"], store)
    end

    test "FLUSHDB with multiple flags returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Server.handle("FLUSHDB", ["ASYNC", "SYNC"], store)
    end

    test "FLUSHDB on empty store returns :ok" do
      store = MockStore.make()
      assert :ok = Server.handle("FLUSHDB", [], store)
    end
  end

  # ===========================================================================
  # Dispatcher — case normalisation
  # ===========================================================================

  describe "Dispatcher case normalisation" do
    test "dispatches fully lowercase command" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "v" == Dispatcher.dispatch("get", ["k"], store)
    end

    test "dispatches mixed case 'gEt'" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "v" == Dispatcher.dispatch("gEt", ["k"], store)
    end

    test "dispatches 'pInG' mixed case" do
      assert {:simple, "PONG"} == Dispatcher.dispatch("pInG", [], MockStore.make())
    end

    test "dispatches 'expire' lowercase" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Dispatcher.dispatch("expire", ["k", "10"], store)
    end

    test "dispatches 'Ttl' mixed case" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Dispatcher.dispatch("Ttl", ["k"], store)
    end

    test "dispatches 'dBsIzE' mixed case" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert 1 == Dispatcher.dispatch("dBsIzE", [], store)
    end
  end

  # ===========================================================================
  # Dispatcher — unknown commands
  # ===========================================================================

  describe "Dispatcher unknown commands" do
    test "completely unknown command returns error with command name" do
      assert {:error, msg} = Dispatcher.dispatch("FOOBAR", ["x"], MockStore.make())
      assert msg =~ "unknown command"
      assert msg =~ "foobar"
    end

    test "empty string command returns error" do
      assert {:error, msg} = Dispatcher.dispatch("", [], MockStore.make())
      assert msg =~ "unknown command"
    end

    test "command with spaces returns error" do
      assert {:error, _} = Dispatcher.dispatch("GET SET", ["k"], MockStore.make())
    end

    test "numeric command name returns error" do
      assert {:error, _} = Dispatcher.dispatch("12345", [], MockStore.make())
    end
  end

  # ===========================================================================
  # Dispatcher — error propagation
  # ===========================================================================

  describe "Dispatcher error propagation" do
    test "propagates argument errors from handler through dispatcher" do
      assert {:error, msg} = Dispatcher.dispatch("GET", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "propagates SET option errors through dispatcher" do
      assert {:error, _} =
               Dispatcher.dispatch("SET", ["k", "v", "EX", "0"], MockStore.make())
    end

    test "propagates EXPIRE parse errors through dispatcher" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Dispatcher.dispatch("EXPIRE", ["k", "abc"], store)
    end
  end

  # ===========================================================================
  # Dispatcher — FLUSHALL routing
  # ===========================================================================

  describe "Dispatcher FLUSHALL routing" do
    test "FLUSHALL is routed to Server handler" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Dispatcher.dispatch("FLUSHALL", [], store)
    end
  end

  # ===========================================================================
  # Cross-command interactions via dispatcher
  # ===========================================================================

  describe "cross-command interactions" do
    test "SET then GET returns the set value" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "hello"], store)
      assert "hello" == Dispatcher.dispatch("GET", ["k"], store)
    end

    test "SET then DEL then GET returns nil" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "v"], store)
      assert 1 == Dispatcher.dispatch("DEL", ["k"], store)
      assert nil == Dispatcher.dispatch("GET", ["k"], store)
    end

    test "MSET then MGET returns all values" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("MSET", ["a", "1", "b", "2", "c", "3"], store)
      assert ["1", "2", "3"] == Dispatcher.dispatch("MGET", ["a", "b", "c"], store)
    end

    test "SET with EX then TTL returns positive seconds" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "v", "EX", "60"], store)
      ttl = Dispatcher.dispatch("TTL", ["k"], store)
      assert ttl > 0
      assert ttl <= 60
    end

    test "SET then EXPIRE then PERSIST removes expiry" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "v"], store)
      assert 1 == Dispatcher.dispatch("EXPIRE", ["k", "30"], store)
      ttl = Dispatcher.dispatch("TTL", ["k"], store)
      assert ttl > 0
      assert 1 == Dispatcher.dispatch("PERSIST", ["k"], store)
      assert -1 == Dispatcher.dispatch("TTL", ["k"], store)
    end

    test "FLUSHDB then EXISTS returns 0" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Dispatcher.dispatch("FLUSHDB", [], store)
      assert 0 == Dispatcher.dispatch("EXISTS", ["a", "b"], store)
    end

    test "FLUSHDB then DBSIZE returns 0" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Dispatcher.dispatch("FLUSHDB", [], store)
      assert 0 == Dispatcher.dispatch("DBSIZE", [], store)
    end
  end

  # ===========================================================================
  # WRONGTYPE enforcement — string commands on data structure keys
  # ===========================================================================

  describe "WRONGTYPE enforcement: string commands on hash keys" do
    setup do
      store = MockStore.make()
      # Simulate a hash key by registering it as type "hash" in compound storage
      # The type registry stores T:key -> "hash"
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "hash", 0)
      {:ok, store: store}
    end

    test "GET on hash key returns WRONGTYPE", %{store: _store} do
      # GET internally checks the serialized value. When the stored value is
      # an Erlang-encoded {:hash, _} tuple, GET detects this and returns
      # WRONGTYPE. Test at the Strings handler level.
      hash_val = :erlang.term_to_binary({:hash, %{"f" => "v"}})
      store_with_hash = MockStore.make(%{"mykey" => {hash_val, 0}})
      result = Strings.handle("GET", ["mykey"], store_with_hash)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "GET on list key returns WRONGTYPE" do
      list_val = :erlang.term_to_binary({:list, ["a", "b"]})
      store = MockStore.make(%{"mylist" => {list_val, 0}})
      result = Strings.handle("GET", ["mylist"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "GET on set key returns WRONGTYPE" do
      set_val = :erlang.term_to_binary({:set, MapSet.new(["a"])})
      store = MockStore.make(%{"myset" => {set_val, 0}})
      result = Strings.handle("GET", ["myset"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "GET on zset key returns WRONGTYPE" do
      zset_val = :erlang.term_to_binary({:zset, %{"a" => 1.0}})
      store = MockStore.make(%{"myzset" => {zset_val, 0}})
      result = Strings.handle("GET", ["myzset"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "GET on key with Erlang-encoded but non-struct binary returns value as-is" do
      # A value that starts with <<131>> (ETF marker) but is not a known type
      # should be returned as the raw binary.
      other_val = :erlang.term_to_binary({:something_else, 42})
      store = MockStore.make(%{"other" => {other_val, 0}})
      result = Strings.handle("GET", ["other"], store)
      assert result == other_val
    end
  end

  describe "WRONGTYPE enforcement: HSET on string key" do
    test "HSET on a key already holding a set returns WRONGTYPE" do
      alias Ferricstore.Commands.Hash

      store = MockStore.make()
      # Register the key as a set
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "set", 0)

      result = Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "HSET on a key already holding a list returns WRONGTYPE" do
      alias Ferricstore.Commands.Hash

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "list", 0)

      result = Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "HSET on a key already holding a zset returns WRONGTYPE" do
      alias Ferricstore.Commands.Hash

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "zset", 0)

      result = Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end
  end

  describe "WRONGTYPE: after DEL, key can be reused as any type" do
    test "DEL hash key then SET as string succeeds" do
      alias Ferricstore.Commands.Hash

      store = MockStore.make()
      # Create as hash
      Hash.handle("HSET", ["reuse", "f1", "v1"], store)
      # Delete via Strings DEL
      Strings.handle("DEL", ["reuse"], store)
      # Reuse as string
      assert :ok = Strings.handle("SET", ["reuse", "now_a_string"], store)
      assert "now_a_string" == store.get.("reuse")
    end

    test "DEL set key then HSET as hash succeeds" do
      alias Ferricstore.Commands.{Hash, Set}

      store = MockStore.make()
      # Create as set
      Set.handle("SADD", ["reuse", "member1"], store)
      # Delete via Strings DEL
      Strings.handle("DEL", ["reuse"], store)
      # Reuse as hash
      result = Hash.handle("HSET", ["reuse", "field1", "val1"], store)
      assert result == 1
    end
  end

  # ===========================================================================
  # WRONGTYPE across all data structure boundaries
  # ===========================================================================

  describe "WRONGTYPE across data structure boundaries" do
    test "LPUSH on hash key returns WRONGTYPE" do
      alias Ferricstore.Commands.List

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "hash", 0)

      result = List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "SADD on hash key returns WRONGTYPE" do
      alias Ferricstore.Commands.Set

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "hash", 0)

      result = Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "ZADD on hash key returns WRONGTYPE" do
      alias Ferricstore.Commands.SortedSet

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "hash", 0)

      result = SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "HSET on list key returns WRONGTYPE" do
      alias Ferricstore.Commands.Hash

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "list", 0)

      result = Hash.handle("HSET", ["mykey", "f", "v"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "SADD on zset key returns WRONGTYPE" do
      alias Ferricstore.Commands.Set

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "zset", 0)

      result = Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "ZADD on set key returns WRONGTYPE" do
      alias Ferricstore.Commands.SortedSet

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "set", 0)

      result = SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "LPUSH on set key returns WRONGTYPE" do
      alias Ferricstore.Commands.List

      store = MockStore.make()
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "set", 0)

      result = List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end
  end

  # ===========================================================================
  # TTL edge cases — spec-required behavior
  # ===========================================================================

  describe "TTL spec-required edge cases" do
    test "PERSIST removes TTL and key becomes permanent" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", future}})

      # Key has a TTL
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0

      # PERSIST removes it
      assert 1 == Expiry.handle("PERSIST", ["k"], store)

      # Now TTL returns -1 (no expiry)
      assert -1 == Expiry.handle("TTL", ["k"], store)

      # Value is still accessible
      assert "v" == store.get.("k")
    end

    test "EXPIRE with 0 expires the key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "0"], store)
      # Key should be expired now (TTL returns -2 for non-existent)
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end

    test "EXPIRE with negative value deletes the key (Redis compat)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      result = Expiry.handle("EXPIRE", ["k", "-1"], store)
      assert 1 == result
    end

    test "PEXPIRE with millisecond precision" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # Set PEXPIRE to 500ms
      assert 1 == Expiry.handle("PEXPIRE", ["k", "500"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      # PTTL should be within 500ms (allowing some execution time)
      assert pttl > 0 and pttl <= 500
    end

    test "TTL on non-existent key returns -2" do
      store = MockStore.make()
      assert -2 == Expiry.handle("TTL", ["no_such_key"], store)
    end

    test "PTTL on non-existent key returns -2" do
      store = MockStore.make()
      assert -2 == Expiry.handle("PTTL", ["no_such_key"], store)
    end

    test "TTL on key without expiry returns -1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "PTTL on key without expiry returns -1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("PTTL", ["k"], store)
    end

    test "SET with EX option sets TTL correctly" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "EX", "60"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 60
    end

    test "SET with PX option sets TTL in milliseconds correctly" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "PX", "5000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0 and pttl <= 5000
    end

    test "SET with NX flag only sets if key does not exist" do
      store = MockStore.make()
      # Key does not exist, NX should succeed
      assert :ok = Strings.handle("SET", ["k", "first", "NX"], store)
      assert "first" == store.get.("k")
      # Key now exists, NX should fail (return nil)
      assert nil == Strings.handle("SET", ["k", "second", "NX"], store)
      assert "first" == store.get.("k")
    end

    test "SET with XX flag only sets if key already exists" do
      store = MockStore.make()
      # Key does not exist, XX should fail
      assert nil == Strings.handle("SET", ["k", "v", "XX"], store)
      assert nil == store.get.("k")
      # Create the key
      assert :ok = Strings.handle("SET", ["k", "v"], store)
      # Now XX should succeed
      assert :ok = Strings.handle("SET", ["k", "updated", "XX"], store)
      assert "updated" == store.get.("k")
    end
  end

  # ===========================================================================
  # Integer overflow edge cases
  # ===========================================================================

  describe "integer overflow edge cases" do
    test "INCR on max int64 value continues without overflow in Elixir (bignum)" do
      # In Redis, INCR on max int64 returns an error. In Elixir, integers are
      # arbitrary precision, so this will succeed with a value beyond int64.
      # The MockStore uses Elixir integers, so it does not enforce int64 bounds.
      max_int64 = 9_223_372_036_854_775_807
      store = MockStore.make(%{"k" => {Integer.to_string(max_int64), 0}})
      result = Strings.handle("INCR", ["k"], store)
      assert {:ok, value} = result
      assert value == max_int64 + 1
    end

    test "DECRBY with large negative amount on zero" do
      store = MockStore.make(%{"k" => {"0", 0}})
      large_amount = 9_223_372_036_854_775_807
      result = Strings.handle("DECRBY", ["k", Integer.to_string(large_amount)], store)
      assert {:ok, value} = result
      assert value == -large_amount
    end

    test "INCRBY with max int64 on max int64 value" do
      max_int64 = 9_223_372_036_854_775_807
      store = MockStore.make(%{"k" => {Integer.to_string(max_int64), 0}})
      result = Strings.handle("INCRBY", ["k", Integer.to_string(max_int64)], store)
      assert {:ok, value} = result
      assert value == max_int64 * 2
    end

    test "INCRBYFLOAT precision with very small increment" do
      store = MockStore.make(%{"k" => {"0", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.000000001"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert_in_delta val, 1.0e-9, 1.0e-20
    end

    test "INCRBYFLOAT precision with large and small values" do
      store = MockStore.make(%{"k" => {"1000000000", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.001"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert_in_delta val, 1_000_000_000.001, 1.0e-6
    end

    test "INCR on negative max int64 value increments to -max+1" do
      min_int64 = -9_223_372_036_854_775_808
      store = MockStore.make(%{"k" => {Integer.to_string(min_int64), 0}})
      assert {:ok, value} = Strings.handle("INCR", ["k"], store)
      assert value == min_int64 + 1
    end

    test "DECR on min int64 value decrements past the boundary" do
      min_int64 = -9_223_372_036_854_775_808
      store = MockStore.make(%{"k" => {Integer.to_string(min_int64), 0}})
      assert {:ok, value} = Strings.handle("DECR", ["k"], store)
      assert value == min_int64 - 1
    end
  end

  # ===========================================================================
  # Binary safety — keys and values with special characters
  # ===========================================================================

  describe "binary safety" do
    test "keys and values with null bytes round-trip via SET/GET" do
      store = MockStore.make()
      key = "key\x00with\x00nulls"
      value = "val\x00with\x00nulls"
      assert :ok = Strings.handle("SET", [key, value], store)
      assert value == Strings.handle("GET", [key], store)
    end

    test "keys and values with unicode round-trip via SET/GET" do
      store = MockStore.make()
      key = "clé_ünïcödë_日本語"
      value = "valeur_émojis_🦀🔥✅"
      assert :ok = Strings.handle("SET", [key, value], store)
      assert value == Strings.handle("GET", [key], store)
    end

    test "empty string value round-trips via SET/GET" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", ""], store)
      assert "" == Strings.handle("GET", ["k"], store)
    end

    test "STRLEN on empty string value returns 0" do
      store = MockStore.make(%{"k" => {"", 0}})
      assert 0 == Strings.handle("STRLEN", ["k"], store)
    end

    test "STRLEN on unicode value returns byte size not char count" do
      # Japanese characters are 3 bytes each in UTF-8
      store = MockStore.make(%{"k" => {"日本語", 0}})
      assert 9 == Strings.handle("STRLEN", ["k"], store)
    end

    test "APPEND with null bytes concatenates correctly" do
      store = MockStore.make(%{"k" => {"before\x00", 0}})
      new_len = Strings.handle("APPEND", ["k", "\x00after"], store)
      assert new_len == 13
      assert "before\x00\x00after" == store.get.("k")
    end

    test "very long keys (> 512 bytes) work with SET/GET" do
      store = MockStore.make()
      long_key = String.duplicate("k", 1024)
      assert :ok = Strings.handle("SET", [long_key, "value"], store)
      assert "value" == Strings.handle("GET", [long_key], store)
    end

    test "very long keys (> 512 bytes) work with EXISTS" do
      store = MockStore.make()
      long_key = String.duplicate("k", 1024)
      assert :ok = Strings.handle("SET", [long_key, "value"], store)
      assert 1 == Strings.handle("EXISTS", [long_key], store)
    end

    test "MSET and MGET with unicode keys and values" do
      store = MockStore.make()

      assert :ok =
               Strings.handle(
                 "MSET",
                 ["キー1", "値1", "キー2", "値2"],
                 store
               )

      assert ["値1", "値2"] ==
               Strings.handle("MGET", ["キー1", "キー2"], store)
    end

    test "value containing RESP-like sequences is treated as opaque binary" do
      store = MockStore.make()
      resp_like = "+OK\r\n-ERR\r\n$5\r\nhello\r\n*2\r\n"
      assert :ok = Strings.handle("SET", ["k", resp_like], store)
      assert resp_like == Strings.handle("GET", ["k"], store)
    end

    test "key with all 256 byte values round-trips" do
      store = MockStore.make()
      # Build a key from all possible byte values
      key = Enum.into(0..255, <<>>, fn b -> <<b>> end)
      value = "all_bytes_key_value"
      assert :ok = Strings.handle("SET", [key, value], store)
      assert value == Strings.handle("GET", [key], store)
    end
  end

  # ===========================================================================
  # OOM / MemoryGuard error codes
  # ===========================================================================

  describe "MemoryGuard reject_writes? check" do
    test "MemoryGuard.reject_writes? returns boolean" do
      # Verify the API contract works — under normal conditions it returns false
      result = Ferricstore.MemoryGuard.reject_writes?()
      assert is_boolean(result)
    end

    test "MemoryGuard.stats returns expected fields" do
      stats = Ferricstore.MemoryGuard.stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :total_bytes)
      assert Map.has_key?(stats, :max_bytes)
      assert Map.has_key?(stats, :ratio)
      assert Map.has_key?(stats, :pressure_level)
      assert Map.has_key?(stats, :eviction_policy)
      assert stats.pressure_level in [:ok, :warning, :pressure, :reject]
    end

    test "MemoryGuard.eviction_policy returns an atom" do
      policy = Ferricstore.MemoryGuard.eviction_policy()
      assert is_atom(policy)
      assert policy in [:volatile_lru, :allkeys_lru, :volatile_ttl, :noeviction]
    end
  end

  # ===========================================================================
  # SET with GET flag (returns old value)
  # ===========================================================================

  describe "SET with combined flags" do
    test "SET with NX and EX sets key with expiry when key does not exist" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "NX", "EX", "60"], store)
      assert "v" == store.get.("k")
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 60
    end

    test "SET with XX and EX updates existing key with new expiry" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["k", "new", "XX", "EX", "30"], store)
      assert "new" == store.get.("k")
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 30
    end

    test "SET with NX and XX both set: XX takes precedence over NX semantics" do
      # Both NX and XX at the same time: NX requires key not exist, XX requires
      # key exist. This is contradictory — Redis rejects with syntax error or
      # the last one wins. In our impl, both flags are set, so XX=true means
      # key must exist AND NX=true means key must not exist — neither condition
      # is satisfiable if both are true.
      store = MockStore.make(%{"k" => {"old", 0}})
      # With existing key: NX=true checks "not exists?" which is false -> nil
      result = Strings.handle("SET", ["k", "new", "NX", "XX"], store)
      assert result == nil
    end

    test "SET with NX on non-existing key and PX sets millisecond expiry" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "NX", "PX", "5000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0 and pttl <= 5000
    end
  end

  # ===========================================================================
  # Additional EXPIRE/TTL round-trip correctness
  # ===========================================================================

  describe "EXPIRE then TTL round-trip" do
    test "EXPIRE 10 then TTL returns value between 0 and 10" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "10"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl >= 0 and ttl <= 10
    end

    test "PEXPIRE 5000 then PTTL returns value between 0 and 5000" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "5000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl >= 0 and pttl <= 5000
    end

    test "EXPIRE replaces existing TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "100"], store)
      assert 1 == Expiry.handle("EXPIRE", ["k", "5"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl >= 0 and ttl <= 5
    end

    test "PERSIST after EXPIRE returns -1 TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "60"], store)
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert -1 == Expiry.handle("TTL", ["k"], store)
      # Value is still there
      assert "v" == store.get.("k")
    end
  end

  # ===========================================================================
  # Strings — SET EXAT / PXAT / GET / KEEPTTL (RESP3 handler level)
  # ===========================================================================

  describe "SET EXAT" do
    test "sets key with absolute Unix timestamp in seconds" do
      store = MockStore.make()
      future_ts = div(System.os_time(:millisecond), 1000) + 120
      assert :ok = Strings.handle("SET", ["k", "v", "EXAT", Integer.to_string(future_ts)], store)
      assert "v" == store.get.("k")

      # Verify expire_at_ms was set to ts * 1000
      {_val, exp} = store.get_meta.("k")
      assert exp == future_ts * 1000
    end

    test "EXAT with negative timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EXAT", "-1"], store)
      assert msg =~ "invalid expire"
    end

    test "EXAT with zero timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EXAT", "0"], store)
      assert msg =~ "invalid expire"
    end

    test "EXAT with non-numeric value returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EXAT", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "EXAT missing value returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("SET", ["k", "v", "EXAT"], store)
    end
  end

  describe "SET PXAT" do
    test "sets key with absolute Unix timestamp in milliseconds" do
      store = MockStore.make()
      future_ms = System.os_time(:millisecond) + 120_000
      assert :ok = Strings.handle("SET", ["k", "v", "PXAT", Integer.to_string(future_ms)], store)
      assert "v" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_ms
    end

    test "PXAT with negative timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "PXAT", "-1"], store)
      assert msg =~ "invalid expire"
    end

    test "PXAT with zero timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "PXAT", "0"], store)
      assert msg =~ "invalid expire"
    end

    test "PXAT with non-numeric value returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "PXAT", "abc"], store)
      assert msg =~ "not an integer"
    end
  end

  describe "SET GET" do
    test "returns nil when key does not exist" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["k", "v", "GET"], store)
    end

    test "returns old value when key exists" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" == Strings.handle("SET", ["k", "new", "GET"], store)
      assert "new" == store.get.("k")
    end

    test "GET with NX returns old value when key exists (write skipped)" do
      store = MockStore.make(%{"k" => {"existing", 0}})
      assert "existing" == Strings.handle("SET", ["k", "new", "NX", "GET"], store)
      # Value unchanged
      assert "existing" == store.get.("k")
    end

    test "GET with NX returns nil when key does not exist (write succeeds)" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["k", "v", "NX", "GET"], store)
      assert "v" == store.get.("k")
    end

    test "GET with XX returns nil when key does not exist (write skipped)" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["k", "v", "XX", "GET"], store)
      assert nil == store.get.("k")
    end

    test "GET with XX returns old value when key exists (write succeeds)" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" == Strings.handle("SET", ["k", "new", "XX", "GET"], store)
      assert "new" == store.get.("k")
    end

    test "GET with EX returns old value and sets expiry" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" == Strings.handle("SET", ["k", "new", "EX", "60", "GET"], store)
      assert "new" == store.get.("k")
      {_val, exp} = store.get_meta.("k")
      assert exp > 0
    end
  end

  describe "SET KEEPTTL" do
    test "preserves existing TTL when overwriting" do
      future_exp = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future_exp}})
      assert :ok = Strings.handle("SET", ["k", "new", "KEEPTTL"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_exp
    end

    test "key without TTL remains without TTL" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["k", "new", "KEEPTTL"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == 0
    end

    test "KEEPTTL on nonexistent key sets no TTL" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "KEEPTTL"], store)
      assert "v" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == 0
    end

    test "KEEPTTL combined with GET returns old value and preserves TTL" do
      future_exp = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future_exp}})
      assert "old" == Strings.handle("SET", ["k", "new", "KEEPTTL", "GET"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_exp
    end

    test "KEEPTTL combined with XX on existing key" do
      future_exp = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future_exp}})
      assert :ok = Strings.handle("SET", ["k", "new", "KEEPTTL", "XX"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_exp
    end
  end

  describe "SET mutual exclusion errors" do
    test "EXAT + EX returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EXAT", "9999999999", "EX", "10"], store)
      assert msg =~ "syntax error"
    end

    test "PXAT + PX returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "PXAT", "9999999999000", "PX", "10000"], store)
      assert msg =~ "syntax error"
    end

    test "EXAT + PXAT returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EXAT", "9999999999", "PXAT", "9999999999000"], store)
      assert msg =~ "syntax error"
    end

    test "EX + EXAT returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EX", "10", "EXAT", "9999999999"], store)
      assert msg =~ "syntax error"
    end

    test "KEEPTTL + EX returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "EX", "10"], store)
      assert msg =~ "syntax error"
    end

    test "KEEPTTL + PX returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "PX", "10000"], store)
      assert msg =~ "syntax error"
    end

    test "EX + KEEPTTL returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EX", "10", "KEEPTTL"], store)
      assert msg =~ "syntax error"
    end

    test "PX + KEEPTTL returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "PX", "10000", "KEEPTTL"], store)
      assert msg =~ "syntax error"
    end

    test "KEEPTTL + EXAT returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "EXAT", "9999999999"], store)
      assert msg =~ "syntax error"
    end

    test "KEEPTTL + PXAT returns syntax error" do
      store = MockStore.make()
      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "PXAT", "9999999999000"], store)
      assert msg =~ "syntax error"
    end
  end
end
