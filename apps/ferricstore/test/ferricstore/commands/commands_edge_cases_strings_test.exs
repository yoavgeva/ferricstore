defmodule Ferricstore.Commands.CommandsEdgeCasesStringsTest do
  @moduledoc """
  Edge cases for string commands (SET, GET, DEL, EXISTS, MGET, MSET).
  Split from CommandsEdgeCasesTest.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Strings
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
end
