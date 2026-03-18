defmodule Ferricstore.Commands.SetTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Set
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # SADD
  # ---------------------------------------------------------------------------

  describe "SADD" do
    test "SADD adds new members and returns count" do
      store = MockStore.make()
      assert 3 == Set.handle("SADD", ["myset", "a", "b", "c"], store)
    end

    test "SADD ignores duplicate members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)
      assert 1 == Set.handle("SADD", ["myset", "b", "c"], store)
    end

    test "SADD with all duplicates returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert 0 == Set.handle("SADD", ["myset", "a"], store)
    end

    test "SADD with no members returns error" do
      assert {:error, _} = Set.handle("SADD", ["myset"], MockStore.make())
    end

    test "SADD with no args returns error" do
      assert {:error, _} = Set.handle("SADD", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SREM
  # ---------------------------------------------------------------------------

  describe "SREM" do
    test "SREM removes existing member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 1 == Set.handle("SREM", ["myset", "b"], store)
    end

    test "SREM on missing member returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert 0 == Set.handle("SREM", ["myset", "missing"], store)
    end

    test "SREM multiple members returns count removed" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 2 == Set.handle("SREM", ["myset", "a", "c", "d"], store)
    end

    test "SREM cleans up type metadata when set becomes empty" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "only"], store)
      Set.handle("SREM", ["myset", "only"], store)
      assert nil == store.compound_get.("myset", "T:myset")
    end

    test "SREM with no members returns error" do
      assert {:error, _} = Set.handle("SREM", ["myset"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS
  # ---------------------------------------------------------------------------

  describe "SMEMBERS" do
    test "SMEMBERS returns all members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      members = Set.handle("SMEMBERS", ["myset"], store)
      assert Enum.sort(members) == ["a", "b", "c"]
    end

    test "SMEMBERS on nonexistent set returns empty list" do
      assert [] == Set.handle("SMEMBERS", ["nonexistent"], MockStore.make())
    end

    test "SMEMBERS with wrong arity returns error" do
      assert {:error, _} = Set.handle("SMEMBERS", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER
  # ---------------------------------------------------------------------------

  describe "SISMEMBER" do
    test "SISMEMBER returns 1 for existing member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)
      assert 1 == Set.handle("SISMEMBER", ["myset", "a"], store)
    end

    test "SISMEMBER returns 0 for missing member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert 0 == Set.handle("SISMEMBER", ["myset", "missing"], store)
    end

    test "SISMEMBER returns 0 for nonexistent key" do
      assert 0 == Set.handle("SISMEMBER", ["nonexistent", "a"], MockStore.make())
    end

    test "SISMEMBER with wrong arity returns error" do
      assert {:error, _} = Set.handle("SISMEMBER", ["key"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SCARD
  # ---------------------------------------------------------------------------

  describe "SCARD" do
    test "SCARD returns set cardinality" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 3 == Set.handle("SCARD", ["myset"], store)
    end

    test "SCARD returns 0 for nonexistent key" do
      assert 0 == Set.handle("SCARD", ["nonexistent"], MockStore.make())
    end

    test "SCARD reflects removals" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      Set.handle("SREM", ["myset", "b"], store)
      assert 2 == Set.handle("SCARD", ["myset"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SINTER
  # ---------------------------------------------------------------------------

  describe "SINTER" do
    test "SINTER returns intersection of two sets" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "b", "c", "d"], store)
      result = Set.handle("SINTER", ["s1", "s2"], store)
      assert Enum.sort(result) == ["b", "c"]
    end

    test "SINTER with disjoint sets returns empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "c", "d"], store)
      assert [] == Set.handle("SINTER", ["s1", "s2"], store)
    end

    test "SINTER with nonexistent key returns empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      assert [] == Set.handle("SINTER", ["s1", "nonexistent"], store)
    end

    test "SINTER single set returns its members" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      result = Set.handle("SINTER", ["s1"], store)
      assert Enum.sort(result) == ["a", "b"]
    end
  end

  # ---------------------------------------------------------------------------
  # SUNION
  # ---------------------------------------------------------------------------

  describe "SUNION" do
    test "SUNION returns union of two sets" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "b", "c"], store)
      result = Set.handle("SUNION", ["s1", "s2"], store)
      assert Enum.sort(result) == ["a", "b", "c"]
    end

    test "SUNION with nonexistent key returns other set" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      result = Set.handle("SUNION", ["s1", "nonexistent"], store)
      assert Enum.sort(result) == ["a", "b"]
    end
  end

  # ---------------------------------------------------------------------------
  # SDIFF
  # ---------------------------------------------------------------------------

  describe "SDIFF" do
    test "SDIFF returns difference" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "b", "c", "d"], store)
      result = Set.handle("SDIFF", ["s1", "s2"], store)
      assert result == ["a"]
    end

    test "SDIFF with nonexistent second set returns first set" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      result = Set.handle("SDIFF", ["s1", "nonexistent"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SDIFF of same set returns empty" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      assert [] == Set.handle("SDIFF", ["s1", "s1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Type enforcement
  # ---------------------------------------------------------------------------

  describe "type enforcement" do
    test "SADD on a key used as hash returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SADD", ["mykey", "a"], store)
    end

    test "SMEMBERS on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "list", 0)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SMEMBERS", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Member IS the key — presence marker
  # ---------------------------------------------------------------------------

  describe "member storage" do
    test "member is stored as compound key with presence marker" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "elixir"], store)
      # The compound key should have the member name and value "1"
      assert "1" == store.compound_get.("myset", <<"S:myset", 0, "elixir">>)
    end
  end
end
