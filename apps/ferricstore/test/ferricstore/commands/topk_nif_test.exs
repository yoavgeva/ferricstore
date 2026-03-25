defmodule Ferricstore.Commands.TopKNifTest do
  @moduledoc """
  NIF-specific tests for the Rust TopK implementation.

  These tests exercise the NIF resource lifecycle, serialization/deserialization,
  and direct NIF API calls to verify the Rust backend works correctly.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.TopK
  alias Ferricstore.Test.MockStore

  describe "NIF resource lifecycle" do
    test "TOPK.RESERVE creates a NIF-backed TopK resource" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      raw = store.get.("hot_keys")
      # TopK now uses mmap-backed files; the store holds a path reference
      assert {:topk_path, path} = raw
      assert is_binary(path)
      assert String.ends_with?(path, ".topk")
    end

    test "NIF resource persists through add, incrby, query operations" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", "a", "b", "c"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "d", "100"], store)
      assert [1, 1, 1, 1] = TopK.handle("TOPK.QUERY", ["hot_keys", "a", "b", "c", "d"], store)
    end
  end

  describe "NIF direct API" do
    test "topk_create validates parameters" do
      assert {:error, _} = NIF.topk_create(0, 8, 7, 0.9)
      assert {:error, _} = NIF.topk_create(3, 0, 7, 0.9)
      assert {:error, _} = NIF.topk_create(3, 8, 0, 0.9)
      assert {:error, _} = NIF.topk_create(3, 8, 7, -0.1)
      assert {:error, _} = NIF.topk_create(3, 8, 7, 1.5)
    end

    test "topk_add returns nil for each element when no eviction" do
      {:ok, ref} = NIF.topk_create(3, 8, 7, 0.9)
      result = NIF.topk_add(ref, ["x", "y", "z"])
      assert length(result) == 3
      assert Enum.all?(result, &is_nil/1)
    end

    test "topk_query returns 1 for present and 0 for absent" do
      {:ok, ref} = NIF.topk_create(3, 8, 7, 0.9)
      NIF.topk_add(ref, ["x", "y", "z"])
      assert [1, 1, 1, 0] = NIF.topk_query(ref, ["x", "y", "z", "w"])
    end

    test "topk_incrby accepts element-count tuple pairs" do
      {:ok, ref} = NIF.topk_create(3, 8, 7, 0.9)
      result = NIF.topk_incrby(ref, [{"alpha", 10}, {"beta", 20}, {"gamma", 30}])
      assert length(result) == 3
    end

    test "topk_list returns elements sorted by count descending" do
      {:ok, ref} = NIF.topk_create(5, 8, 7, 0.9)
      NIF.topk_incrby(ref, [{"a", 10}, {"b", 50}, {"c", 30}])
      assert ["b", "c", "a"] = NIF.topk_list(ref)
    end

    test "topk_list_with_count returns {element, count} tuples" do
      {:ok, ref} = NIF.topk_create(5, 8, 7, 0.9)
      NIF.topk_incrby(ref, [{"a", 10}, {"b", 50}])
      assert [{"b", 50}, {"a", 10}] = NIF.topk_list_with_count(ref)
    end

    test "topk_info returns {k, width, depth, decay}" do
      {:ok, ref} = NIF.topk_create(10, 20, 5, 0.8)
      assert {10, 20, 5, 0.8} = NIF.topk_info(ref)
    end

    test "eviction returns displaced element name as binary" do
      {:ok, ref} = NIF.topk_create(2, 8, 7, 0.9)
      NIF.topk_incrby(ref, [{"a", 100}, {"b", 50}])
      [evicted] = NIF.topk_incrby(ref, [{"c", 200}])
      assert is_binary(evicted)
      assert evicted in ["a", "b"]
    end

    test "handles large number of elements" do
      {:ok, ref} = NIF.topk_create(10, 100, 7, 0.9)
      elements = Enum.map(1..1000, &"elem_#{&1}")
      result = NIF.topk_add(ref, elements)
      assert length(result) == 1000
      assert length(NIF.topk_list(ref)) == 10
    end
  end

  describe "NIF serialization" do
    test "roundtrip preserves state" do
      {:ok, ref} = NIF.topk_create(3, 8, 7, 0.9)
      NIF.topk_incrby(ref, [{"alpha", 10}, {"beta", 20}, {"gamma", 30}])

      {:ok, bytes} = NIF.topk_to_bytes(ref)
      assert is_binary(bytes)

      {:ok, ref2} = NIF.topk_from_bytes(bytes)
      assert {3, 8, 7, 0.9} = NIF.topk_info(ref2)
      assert [1, 1, 1] = NIF.topk_query(ref2, ["alpha", "beta", "gamma"])
      assert ["gamma", "beta", "alpha"] = NIF.topk_list(ref2)
    end

    test "topk_from_bytes rejects invalid data" do
      assert {:error, _} = NIF.topk_from_bytes(<<0, 1, 2>>)
    end

    test "serialized bytes produce equivalent behavior after restore" do
      {:ok, ref} = NIF.topk_create(2, 8, 7, 0.9)
      NIF.topk_incrby(ref, [{"x", 50}, {"y", 100}])
      {:ok, bytes} = NIF.topk_to_bytes(ref)
      {:ok, ref2} = NIF.topk_from_bytes(bytes)

      # Adding to restored ref should work the same
      NIF.topk_incrby(ref2, [{"z", 200}])
      assert [1] = NIF.topk_query(ref2, ["z"])
      assert length(NIF.topk_list(ref2)) == 2
    end
  end
end
