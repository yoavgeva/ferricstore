defmodule Ferricstore.Commands.HashTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # HSET
  # ---------------------------------------------------------------------------

  describe "HSET" do
    test "HSET creates new fields and returns count of added" do
      store = MockStore.make()
      assert 2 == Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
    end

    test "HSET on existing field returns 0 (update, not new)" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 0 == Hash.handle("HSET", ["hash", "f1", "v2"], store)
    end

    test "HSET updates value of existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "old"], store)
      Hash.handle("HSET", ["hash", "f1", "new"], store)
      assert "new" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSET with odd field/value count returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSET", ["hash", "f1"], store)
    end

    test "HSET with no fields returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSET", ["hash"], store)
    end

    test "HSET with no args returns error" do
      assert {:error, _} = Hash.handle("HSET", [], MockStore.make())
    end

    test "HSET mixed new and existing fields returns only new count" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "existing", "old"], store)
      assert 1 == Hash.handle("HSET", ["hash", "existing", "updated", "new", "val"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HGET
  # ---------------------------------------------------------------------------

  describe "HGET" do
    test "HGET returns value for existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice"], store)
      assert "alice" == Hash.handle("HGET", ["hash", "name"], store)
    end

    test "HGET returns nil for missing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert nil == Hash.handle("HGET", ["hash", "missing"], store)
    end

    test "HGET returns nil for missing key" do
      store = MockStore.make()
      assert nil == Hash.handle("HGET", ["nonexistent", "field"], store)
    end

    test "HGET with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HGET", ["key"], MockStore.make())
      assert {:error, _} = Hash.handle("HGET", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HDEL
  # ---------------------------------------------------------------------------

  describe "HDEL" do
    test "HDEL removes existing field and returns 1" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 1 == Hash.handle("HDEL", ["hash", "f1"], store)
      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HDEL on missing field returns 0" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 0 == Hash.handle("HDEL", ["hash", "missing"], store)
    end

    test "HDEL multiple fields returns count of removed" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)
      assert 2 == Hash.handle("HDEL", ["hash", "f1", "f3", "f4"], store)
    end

    test "HDEL with no fields returns error" do
      assert {:error, _} = Hash.handle("HDEL", ["hash"], MockStore.make())
    end

    test "HDEL cleans up type metadata when hash becomes empty" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HDEL", ["hash", "f1"], store)
      # After deleting all fields, the type metadata should be gone
      assert nil == store.compound_get.("hash", "T:hash")
    end
  end

  # ---------------------------------------------------------------------------
  # HMGET
  # ---------------------------------------------------------------------------

  describe "HMGET" do
    test "HMGET returns values for multiple fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      assert ["v1", "v2"] == Hash.handle("HMGET", ["hash", "f1", "f2"], store)
    end

    test "HMGET returns nil for missing fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert ["v1", nil] == Hash.handle("HMGET", ["hash", "f1", "missing"], store)
    end

    test "HMGET on nonexistent key returns all nils" do
      store = MockStore.make()
      assert [nil, nil] == Hash.handle("HMGET", ["nonexistent", "f1", "f2"], store)
    end

    test "HMGET with no fields returns error" do
      assert {:error, _} = Hash.handle("HMGET", ["hash"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HGETALL
  # ---------------------------------------------------------------------------

  describe "HGETALL" do
    test "HGETALL returns flat field-value list" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice", "age", "30"], store)
      result = Hash.handle("HGETALL", ["hash"], store)
      assert is_list(result)
      assert length(result) == 4
      # Convert flat list to map for easier assertion
      map = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
      assert map["name"] == "alice"
      assert map["age"] == "30"
    end

    test "HGETALL on empty/nonexistent hash returns empty list" do
      store = MockStore.make()
      assert [] == Hash.handle("HGETALL", ["nonexistent"], store)
    end

    test "HGETALL with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HGETALL", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HLEN
  # ---------------------------------------------------------------------------

  describe "HLEN" do
    test "HLEN returns number of fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)
      assert 3 == Hash.handle("HLEN", ["hash"], store)
    end

    test "HLEN returns 0 for nonexistent key" do
      store = MockStore.make()
      assert 0 == Hash.handle("HLEN", ["nonexistent"], store)
    end

    test "HLEN with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HLEN", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HEXISTS
  # ---------------------------------------------------------------------------

  describe "HEXISTS" do
    test "HEXISTS returns 1 for existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 1 == Hash.handle("HEXISTS", ["hash", "f1"], store)
    end

    test "HEXISTS returns 0 for missing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 0 == Hash.handle("HEXISTS", ["hash", "missing"], store)
    end

    test "HEXISTS returns 0 for nonexistent key" do
      store = MockStore.make()
      assert 0 == Hash.handle("HEXISTS", ["nonexistent", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HKEYS and HVALS
  # ---------------------------------------------------------------------------

  describe "HKEYS" do
    test "HKEYS returns all field names" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2"], store)
      keys = Hash.handle("HKEYS", ["hash"], store)
      assert Enum.sort(keys) == ["a", "b"]
    end

    test "HKEYS on nonexistent key returns empty list" do
      assert [] == Hash.handle("HKEYS", ["nonexistent"], MockStore.make())
    end
  end

  describe "HVALS" do
    test "HVALS returns all field values" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2"], store)
      vals = Hash.handle("HVALS", ["hash"], store)
      assert Enum.sort(vals) == ["1", "2"]
    end

    test "HVALS on nonexistent key returns empty list" do
      assert [] == Hash.handle("HVALS", ["nonexistent"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HSETNX
  # ---------------------------------------------------------------------------

  describe "HSETNX" do
    test "HSETNX sets field when not present" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSETNX", ["hash", "f1", "v1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSETNX does not overwrite existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "original"], store)
      assert 0 == Hash.handle("HSETNX", ["hash", "f1", "new"], store)
      assert "original" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSETNX with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HSETNX", ["hash", "f1"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HINCRBY
  # ---------------------------------------------------------------------------

  describe "HINCRBY" do
    test "HINCRBY increments integer field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "counter", "10"], store)
      assert 15 == Hash.handle("HINCRBY", ["hash", "counter", "5"], store)
    end

    test "HINCRBY creates field with increment when missing" do
      store = MockStore.make()
      assert 5 == Hash.handle("HINCRBY", ["hash", "counter", "5"], store)
      assert "5" == Hash.handle("HGET", ["hash", "counter"], store)
    end

    test "HINCRBY with negative increment decrements" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "counter", "10"], store)
      assert 7 == Hash.handle("HINCRBY", ["hash", "counter", "-3"], store)
    end

    test "HINCRBY on non-integer field returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice"], store)
      assert {:error, _} = Hash.handle("HINCRBY", ["hash", "name", "1"], store)
    end

    test "HINCRBY with non-integer increment returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HINCRBY", ["hash", "f1", "abc"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HINCRBYFLOAT
  # ---------------------------------------------------------------------------

  describe "HINCRBYFLOAT" do
    test "HINCRBYFLOAT increments float field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "price", "10.5"], store)
      result = Hash.handle("HINCRBYFLOAT", ["hash", "price", "1.5"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 12.0, val, 0.001
    end

    test "HINCRBYFLOAT creates field when missing" do
      store = MockStore.make()
      result = Hash.handle("HINCRBYFLOAT", ["hash", "price", "3.14"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 3.14, val, 0.001
    end

    test "HINCRBYFLOAT on non-numeric field returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice"], store)
      assert {:error, _} = Hash.handle("HINCRBYFLOAT", ["hash", "name", "1.0"], store)
    end

    test "HINCRBYFLOAT with non-float increment returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HINCRBYFLOAT", ["hash", "f1", "abc"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Type enforcement
  # ---------------------------------------------------------------------------

  describe "type enforcement" do
    test "HSET on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      # Manually set the type to "set"
      store.compound_put.("mykey", "T:mykey", "set", 0)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSET", ["mykey", "f", "v"], store)
    end

    test "HGET on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "list", 0)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HGET", ["mykey", "f"], store)
    end

    test "HGETALL on a key used as zset returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "zset", 0)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HGETALL", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Individual field storage verification
  # ---------------------------------------------------------------------------

  describe "individual field storage" do
    test "fields are stored as separate compound keys" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)

      # Each field should be individually accessible via compound_get
      assert "v1" == store.compound_get.("hash", <<"H:hash", 0, "f1">>)
      assert "v2" == store.compound_get.("hash", <<"H:hash", 0, "f2">>)
    end

    test "HGET reads individual field without loading entire hash" do
      store = MockStore.make()
      # Pre-populate compound keys directly (simulating stored data)
      store.compound_put.("hash", <<"H:hash", 0, "f1">>, "v1", 0)
      store.compound_put.("hash", <<"H:hash", 0, "f2">>, "v2", 0)
      store.compound_put.("hash", "T:hash", "hash", 0)

      # HGET should read just the one field
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end
  end
end
