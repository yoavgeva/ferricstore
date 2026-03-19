defmodule Ferricstore.Commands.ScanTypeTest do
  @moduledoc """
  Tests for SCAN TYPE filtering across all supported Redis data types.

  Verifies that `SCAN 0 TYPE <type>` correctly returns only keys whose
  type matches the filter. Type detection relies on `TypeRegistry.get_type/2`,
  which checks compound key type metadata (`T:key`) for data structures and
  falls back to `"string"` for plain string keys.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # Helper: creates a MockStore with string keys and typed (compound) keys.
  #
  # For each typed key, we insert:
  #   1. The "T:<key>" type metadata entry (value = type string, expire = 0)
  #   2. A dummy top-level entry for the key so that `exists?` returns true
  #      and `store.keys.()` includes it
  #
  # String keys need no type metadata -- they are the default type.
  defp make_typed_store(entries) do
    initial =
      Enum.reduce(entries, %{}, fn {key, type}, acc ->
        case type do
          "string" ->
            Map.put(acc, key, {"value", 0})

          data_type when data_type in ~w(hash list set zset) ->
            type_key = CompoundKey.type_key(key)

            acc
            |> Map.put(key, {"", 0})
            |> Map.put(type_key, {data_type, 0})

          _other ->
            Map.put(acc, key, {"value", 0})
        end
      end)

    MockStore.make(initial)
  end

  # Collects ALL keys from a full SCAN iteration (handles pagination).
  defp scan_all_keys(store, type) do
    scan_all_keys(store, type, "0", [])
  end

  defp scan_all_keys(store, type, cursor, acc) do
    [next_cursor, keys] =
      Generic.handle("SCAN", [cursor, "TYPE", type, "COUNT", "100"], store)

    all_keys = acc ++ keys

    if next_cursor == "0" do
      Enum.sort(all_keys)
    else
      scan_all_keys(store, type, next_cursor, all_keys)
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE string
  # ---------------------------------------------------------------------------

  describe "SCAN TYPE string" do
    test "returns only string keys when mixed types exist" do
      store =
        make_typed_store([
          {"str1", "string"},
          {"str2", "string"},
          {"myhash", "hash"},
          {"myset", "set"}
        ])

      keys = scan_all_keys(store, "string")
      assert keys == ["str1", "str2"]
    end

    test "returns all keys when all are strings" do
      store =
        make_typed_store([
          {"a", "string"},
          {"b", "string"},
          {"c", "string"}
        ])

      keys = scan_all_keys(store, "string")
      assert keys == ["a", "b", "c"]
    end

    test "returns empty list when no string keys exist" do
      store =
        make_typed_store([
          {"myhash", "hash"},
          {"myset", "set"}
        ])

      keys = scan_all_keys(store, "string")
      assert keys == []
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE hash
  # ---------------------------------------------------------------------------

  describe "SCAN TYPE hash" do
    test "returns only hash keys" do
      store =
        make_typed_store([
          {"h1", "hash"},
          {"h2", "hash"},
          {"str1", "string"},
          {"myset", "set"}
        ])

      keys = scan_all_keys(store, "hash")
      assert keys == ["h1", "h2"]
    end

    test "returns empty list when no hash keys exist" do
      store =
        make_typed_store([
          {"str1", "string"},
          {"myset", "set"}
        ])

      keys = scan_all_keys(store, "hash")
      assert keys == []
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE list
  # ---------------------------------------------------------------------------

  describe "SCAN TYPE list" do
    test "returns only list keys" do
      store =
        make_typed_store([
          {"l1", "list"},
          {"l2", "list"},
          {"str1", "string"},
          {"myhash", "hash"}
        ])

      keys = scan_all_keys(store, "list")
      assert keys == ["l1", "l2"]
    end

    test "returns empty list when no list keys exist" do
      store =
        make_typed_store([
          {"str1", "string"},
          {"myhash", "hash"}
        ])

      keys = scan_all_keys(store, "list")
      assert keys == []
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE set
  # ---------------------------------------------------------------------------

  describe "SCAN TYPE set" do
    test "returns only set keys" do
      store =
        make_typed_store([
          {"s1", "set"},
          {"s2", "set"},
          {"str1", "string"},
          {"myhash", "hash"}
        ])

      keys = scan_all_keys(store, "set")
      assert keys == ["s1", "s2"]
    end

    test "returns empty list when no set keys exist" do
      store =
        make_typed_store([
          {"str1", "string"},
          {"l1", "list"}
        ])

      keys = scan_all_keys(store, "set")
      assert keys == []
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE zset
  # ---------------------------------------------------------------------------

  describe "SCAN TYPE zset" do
    test "returns only sorted set keys" do
      store =
        make_typed_store([
          {"z1", "zset"},
          {"z2", "zset"},
          {"str1", "string"},
          {"myset", "set"}
        ])

      keys = scan_all_keys(store, "zset")
      assert keys == ["z1", "z2"]
    end

    test "returns empty list when no sorted set keys exist" do
      store =
        make_typed_store([
          {"str1", "string"},
          {"myhash", "hash"}
        ])

      keys = scan_all_keys(store, "zset")
      assert keys == []
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE with nil (no filter)
  # ---------------------------------------------------------------------------

  describe "SCAN without TYPE filter" do
    test "returns all non-internal keys" do
      store =
        make_typed_store([
          {"str1", "string"},
          {"myhash", "hash"},
          {"mylist", "list"},
          {"myset", "set"},
          {"myzset", "zset"}
        ])

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert Enum.sort(keys) == ["myhash", "mylist", "myset", "myzset", "str1"]
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN TYPE combined with MATCH
  # ---------------------------------------------------------------------------

  describe "SCAN TYPE combined with MATCH" do
    test "TYPE and MATCH filters are both applied" do
      store =
        make_typed_store([
          {"user:1", "hash"},
          {"user:2", "hash"},
          {"user:3", "string"},
          {"order:1", "hash"}
        ])

      [_cursor, keys] =
        Generic.handle(
          "SCAN",
          ["0", "TYPE", "hash", "MATCH", "user:*", "COUNT", "100"],
          store
        )

      assert Enum.sort(keys) == ["user:1", "user:2"]
    end
  end

  # ---------------------------------------------------------------------------
  # Internal compound keys are excluded from SCAN results
  # ---------------------------------------------------------------------------

  describe "SCAN excludes internal compound keys" do
    test "T: type metadata keys are not returned in SCAN results" do
      store =
        make_typed_store([
          {"myhash", "hash"},
          {"str1", "string"}
        ])

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      # T:myhash should NOT appear
      refute Enum.any?(keys, &String.starts_with?(&1, "T:"))
      assert Enum.sort(keys) == ["myhash", "str1"]
    end

    test "H: hash compound subkeys with null byte are not returned" do
      # Simulate a Bitcask keydir containing a hash key with its subkeys.
      # The store.keys.() returns ALL keys from the keydir, including
      # internal compound keys like H:myhash\0field1.
      initial = %{
        "myhash" => {"", 0},
        "T:myhash" => {"hash", 0},
        CompoundKey.hash_field("myhash", "field1") => {"value1", 0},
        CompoundKey.hash_field("myhash", "field2") => {"value2", 0},
        "str1" => {"hello", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      # Only user-visible keys should appear
      assert Enum.sort(keys) == ["myhash", "str1"]
    end

    test "S: set compound subkeys with null byte are not returned" do
      initial = %{
        "myset" => {"", 0},
        "T:myset" => {"set", 0},
        CompoundKey.set_member("myset", "member1") => {"1", 0},
        CompoundKey.set_member("myset", "member2") => {"1", 0},
        "str1" => {"hello", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert Enum.sort(keys) == ["myset", "str1"]
    end

    test "L: list compound subkeys with null byte are not returned" do
      initial = %{
        "mylist" => {"", 0},
        "T:mylist" => {"list", 0},
        CompoundKey.list_element("mylist", 1000.0) => {"item1", 0},
        CompoundKey.list_element("mylist", 2000.0) => {"item2", 0},
        "str1" => {"hello", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert Enum.sort(keys) == ["mylist", "str1"]
    end

    test "Z: sorted set compound subkeys with null byte are not returned" do
      initial = %{
        "myzset" => {"", 0},
        "T:myzset" => {"zset", 0},
        CompoundKey.zset_member("myzset", "alice") => {"1.0", 0},
        CompoundKey.zset_member("myzset", "bob") => {"2.0", 0},
        "str1" => {"hello", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert Enum.sort(keys) == ["myzset", "str1"]
    end

    test "V: and VM: vector keys are not returned" do
      initial = %{
        "str1" => {"hello", 0},
        "V:myvec" => {"vector_data", 0},
        "VM:myvec" => {"vector_meta", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert keys == ["str1"]
    end

    test "PM: promotion metadata keys are not returned" do
      initial = %{
        "str1" => {"hello", 0},
        "PM:some_promotion" => {"metadata", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert keys == ["str1"]
    end

    test "mixed compound keys from multiple data types are all excluded" do
      # A realistic scenario: multiple data structure keys with their subkeys
      initial = %{
        # User-visible keys
        "user:name" => {"alice", 0},
        "counter" => {"42", 0},
        "myhash" => {"", 0},
        "myset" => {"", 0},
        "mylist" => {"", 0},
        "myzset" => {"", 0},
        # Type metadata (internal)
        "T:myhash" => {"hash", 0},
        "T:myset" => {"set", 0},
        "T:mylist" => {"list", 0},
        "T:myzset" => {"zset", 0},
        # Hash subkeys (internal)
        CompoundKey.hash_field("myhash", "f1") => {"v1", 0},
        CompoundKey.hash_field("myhash", "f2") => {"v2", 0},
        # Set subkeys (internal)
        CompoundKey.set_member("myset", "m1") => {"1", 0},
        # List subkeys (internal)
        CompoundKey.list_element("mylist", 1000.0) => {"item", 0},
        # Sorted set subkeys (internal)
        CompoundKey.zset_member("myzset", "alice") => {"1.0", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert Enum.sort(keys) == ["counter", "myhash", "mylist", "myset", "myzset", "user:name"]
    end
  end
end
