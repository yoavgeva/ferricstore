defmodule Ferricstore.Commands.StringsTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Strings
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # SET
  # ---------------------------------------------------------------------------

  describe "SET" do
    test "SET key value returns :ok" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "value"], store)
      assert "value" == store.get.("key")
    end

    test "SET with EX sets expiry in seconds" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "value", "EX", "10"], store)
      # key should be accessible right after set (not expired yet)
      assert "value" == store.get.("key")
    end

    test "SET with PX sets expiry in milliseconds" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "value", "PX", "5000"], store)
      assert "value" == store.get.("key")
    end

    test "SET with NX succeeds when key is absent" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["newkey", "val", "NX"], store)
      assert "val" == store.get.("newkey")
    end

    test "SET with NX returns nil when key already present" do
      store = MockStore.make(%{"key" => {"existing", 0}})
      assert nil == Strings.handle("SET", ["key", "new_val", "NX"], store)
      assert "existing" == store.get.("key")
    end

    test "SET with XX returns :ok when key exists" do
      store = MockStore.make(%{"key" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["key", "new", "XX"], store)
      assert "new" == store.get.("key")
    end

    test "SET with XX returns nil when key absent" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["key", "val", "XX"], store)
      assert nil == store.get.("key")
    end

    test "SET with no args returns error" do
      assert {:error, _} = Strings.handle("SET", [], MockStore.make())
    end

    test "SET with only key returns error" do
      assert {:error, _} = Strings.handle("SET", ["key"], MockStore.make())
    end

    test "SET with EX 0 returns error" do
      assert {:error, msg} = Strings.handle("SET", ["key", "val", "EX", "0"], MockStore.make())
      assert msg =~ "invalid expire"
    end

    test "SET with EX -1 returns error" do
      assert {:error, msg} = Strings.handle("SET", ["key", "val", "EX", "-1"], MockStore.make())
      assert msg =~ "invalid expire"
    end

    test "SET with EX non-integer returns error" do
      assert {:error, _} = Strings.handle("SET", ["key", "val", "EX", "abc"], MockStore.make())
    end

    test "SET with PX 0 returns error" do
      assert {:error, msg} = Strings.handle("SET", ["key", "val", "PX", "0"], MockStore.make())
      assert msg =~ "invalid expire"
    end

    test "SET overwrites existing key" do
      store = MockStore.make(%{"key" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["key", "new"], store)
      assert "new" == store.get.("key")
    end

    test "SET with EX and NX combined works when key absent" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "val", "EX", "10", "NX"], store)
      assert "val" == store.get.("key")
    end

    test "SET with EX and NX combined returns nil when key present" do
      store = MockStore.make(%{"key" => {"old", 0}})
      assert nil == Strings.handle("SET", ["key", "val", "EX", "10", "NX"], store)
      assert "old" == store.get.("key")
    end
  end

  # ---------------------------------------------------------------------------
  # GET
  # ---------------------------------------------------------------------------

  describe "GET" do
    test "GET existing key returns value" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "v" == Strings.handle("GET", ["k"], store)
    end

    test "GET missing key returns nil" do
      assert nil == Strings.handle("GET", ["missing"], MockStore.make())
    end

    test "GET expired key returns nil" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert nil == Strings.handle("GET", ["k"], store)
    end

    test "GET with no args returns error" do
      assert {:error, _} = Strings.handle("GET", [], MockStore.make())
    end

    test "GET with too many args returns error" do
      assert {:error, _} = Strings.handle("GET", ["a", "b"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # DEL
  # ---------------------------------------------------------------------------

  describe "DEL" do
    test "DEL existing key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Strings.handle("DEL", ["k"], store)
    end

    test "DEL missing key returns 0" do
      assert 0 == Strings.handle("DEL", ["missing"], MockStore.make())
    end

    test "DEL multiple keys returns count of deleted" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert 2 == Strings.handle("DEL", ["a", "b", "c"], store)
    end

    test "DEL no args returns error" do
      assert {:error, _} = Strings.handle("DEL", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # EXISTS
  # ---------------------------------------------------------------------------

  describe "EXISTS" do
    test "EXISTS present key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Strings.handle("EXISTS", ["k"], store)
    end

    test "EXISTS absent key returns 0" do
      assert 0 == Strings.handle("EXISTS", ["missing"], MockStore.make())
    end

    test "EXISTS multiple keys returns sum" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert 2 == Strings.handle("EXISTS", ["a", "b", "c"], store)
    end

    test "EXISTS same key twice counts twice" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 2 == Strings.handle("EXISTS", ["k", "k"], store)
    end

    test "EXISTS no args returns error" do
      assert {:error, _} = Strings.handle("EXISTS", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # MGET
  # ---------------------------------------------------------------------------

  describe "MGET" do
    test "MGET multiple keys returns array of values with nils for missing" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert ["1", "2", nil] == Strings.handle("MGET", ["a", "b", "c"], store)
    end

    test "MGET single key returns single-element list" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert ["1"] == Strings.handle("MGET", ["a"], store)
    end

    test "MGET no args returns error" do
      assert {:error, _} = Strings.handle("MGET", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # MSET
  # ---------------------------------------------------------------------------

  describe "MSET" do
    test "MSET key val pairs returns :ok" do
      store = MockStore.make()
      assert :ok = Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert "v1" == store.get.("k1")
      assert "v2" == store.get.("k2")
    end

    test "MSET odd number of args returns error" do
      assert {:error, _} = Strings.handle("MSET", ["k1", "v1", "k2"], MockStore.make())
    end

    test "MSET no args returns error" do
      assert {:error, _} = Strings.handle("MSET", [], MockStore.make())
    end

    test "MSET with single pair stores the pair" do
      store = MockStore.make()
      assert :ok = Strings.handle("MSET", ["k", "v"], store)
      assert "v" == store.get.("k")
    end

    test "MSET overwrites existing keys" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("MSET", ["k", "new"], store)
      assert "new" == store.get.("k")
    end
  end

  # ---------------------------------------------------------------------------
  # SET — additional edge cases
  # ---------------------------------------------------------------------------

  describe "SET edge cases" do
    test "SET with PX -1 returns error" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "PX", "-1"], MockStore.make())

      assert msg =~ "invalid expire"
    end

    test "SET with both NX and XX always returns nil (contradictory flags)" do
      # Key absent: NX passes, but XX fails → nil
      store_empty = MockStore.make()
      assert nil == Strings.handle("SET", ["key", "val", "NX", "XX"], store_empty)

      # Key present: NX fails → nil
      store_present = MockStore.make(%{"key" => {"old", 0}})
      assert nil == Strings.handle("SET", ["key", "val", "NX", "XX"], store_present)
    end

    test "SET with EX and PX both specified uses last one" do
      store = MockStore.make()
      before_ms = System.os_time(:millisecond)

      # PX 60000 is specified last — should override EX 1
      assert :ok = Strings.handle("SET", ["k", "v", "EX", "1", "PX", "60000"], store)

      {_value, expire_at_ms} = store.get_meta.("k")
      # The stored expiry should be ~60s from now, not ~1s
      assert expire_at_ms >= before_ms + 50_000
    end

    test "SET stores binary value with null bytes" do
      store = MockStore.make()
      value = <<0, 1, 2, 3>>
      assert :ok = Strings.handle("SET", ["key", value], store)
      assert value == store.get.("key")
    end

    test "SET stores key with null bytes" do
      store = MockStore.make()
      key = <<0, 1, 2>>
      assert :ok = Strings.handle("SET", [key, "val"], store)
      assert "val" == store.get.(key)
    end

    test "SET with very long key (10KB)" do
      store = MockStore.make()
      long_key = String.duplicate("k", 10_000)
      assert :ok = Strings.handle("SET", [long_key, "v"], store)
      assert "v" == store.get.(long_key)
    end

    test "SET with very long value (100KB)" do
      store = MockStore.make()
      long_value = String.duplicate("v", 100_000)
      assert :ok = Strings.handle("SET", ["key", long_value], store)
      assert long_value == store.get.("key")
    end

    test "SET with unrecognized option returns syntax error" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "BOGUS"], MockStore.make())

      assert msg =~ "syntax error"
    end
  end

  # ---------------------------------------------------------------------------
  # GET — additional edge cases
  # ---------------------------------------------------------------------------

  describe "GET edge cases" do
    test "GET returns exact binary value with null bytes (not decoded)" do
      value = <<0, 1, 2>>
      store = MockStore.make(%{"k" => {value, 0}})
      assert ^value = Strings.handle("GET", ["k"], store)
    end

    test "GET after SET with PX returns value before expiry" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "PX", "5000"], store)
      assert "v" == Strings.handle("GET", ["k"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL — additional edge cases
  # ---------------------------------------------------------------------------

  describe "DEL edge cases" do
    test "DEL returns 0 for expired key" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Strings.handle("DEL", ["k"], store)
    end

    test "DEL same key twice returns 1 (key gone after first delete)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Strings.handle("DEL", ["k", "k"], store)
    end
  end
end
