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
end
