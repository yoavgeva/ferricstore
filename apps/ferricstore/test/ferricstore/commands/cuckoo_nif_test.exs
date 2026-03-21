defmodule Ferricstore.Commands.CuckooNifTest do
  @moduledoc """
  NIF-specific tests for the Cuckoo filter Rust NIF implementation.
  Tests the NIF direct API, serialization roundtrip, and Bitcask storage pattern.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Cuckoo
  alias Ferricstore.Test.MockStore

  describe "NIF serialization roundtrip" do
    test "filter survives serialize/deserialize cycle" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      :ok = NIF.cuckoo_add(ref, "hello")
      :ok = NIF.cuckoo_add(ref, "world")

      {:ok, blob} = NIF.cuckoo_serialize(ref)
      assert is_binary(blob)

      {:ok, ref2} = NIF.cuckoo_deserialize(blob)
      assert NIF.cuckoo_exists(ref2, "hello") == 1
      assert NIF.cuckoo_exists(ref2, "world") == 1
      assert NIF.cuckoo_exists(ref2, "missing") == 0
    end

    test "serialized filter preserves item and delete counts" do
      {:ok, ref} = NIF.cuckoo_create(512, 4, 500, 0)
      :ok = NIF.cuckoo_add(ref, "a")
      :ok = NIF.cuckoo_add(ref, "b")
      :ok = NIF.cuckoo_add(ref, "c")
      1 = NIF.cuckoo_del(ref, "b")

      {:ok, blob} = NIF.cuckoo_serialize(ref)
      {:ok, ref2} = NIF.cuckoo_deserialize(blob)
      {:ok, {_buckets, _bs, _fps, num_items, num_deletes, _total, _mk}} = NIF.cuckoo_info(ref2)
      assert num_items == 2
      assert num_deletes == 1
    end

    test "deserialization of invalid data returns error" do
      assert {:error, _} = NIF.cuckoo_deserialize(<<0, 0, 0>>)
      assert {:error, _} = NIF.cuckoo_deserialize(<<>>)
    end
  end

  describe "NIF direct API" do
    test "cuckoo_create returns a resource" do
      assert {:ok, _ref} = NIF.cuckoo_create(256, 4, 500, 0)
    end

    test "cuckoo_create with zero capacity returns error" do
      assert {:error, _} = NIF.cuckoo_create(0, 4, 500, 0)
    end

    test "cuckoo_add and cuckoo_exists work together" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      assert 0 = NIF.cuckoo_exists(ref, "foo")
      assert :ok = NIF.cuckoo_add(ref, "foo")
      assert 1 = NIF.cuckoo_exists(ref, "foo")
    end

    test "cuckoo_addnx returns 0 for existing element" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      assert 1 = NIF.cuckoo_addnx(ref, "bar")
      assert 0 = NIF.cuckoo_addnx(ref, "bar")
    end

    test "cuckoo_del removes element" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      :ok = NIF.cuckoo_add(ref, "baz")
      assert 1 = NIF.cuckoo_del(ref, "baz")
      assert 0 = NIF.cuckoo_exists(ref, "baz")
      assert 0 = NIF.cuckoo_del(ref, "baz")
    end

    test "cuckoo_mexists checks multiple elements" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      :ok = NIF.cuckoo_add(ref, "x")
      :ok = NIF.cuckoo_add(ref, "y")
      assert [1, 1, 0] = NIF.cuckoo_mexists(ref, ["x", "y", "z"])
    end

    test "cuckoo_count returns fingerprint count" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      :ok = NIF.cuckoo_add(ref, "dup")
      :ok = NIF.cuckoo_add(ref, "dup")
      assert 2 = NIF.cuckoo_count(ref, "dup")
      assert 0 = NIF.cuckoo_count(ref, "none")
    end

    test "cuckoo_info returns filter metadata" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      :ok = NIF.cuckoo_add(ref, "info_test")
      {:ok, {num_buckets, bucket_size, fp_size, num_items, num_deletes, total_slots, max_kicks}} =
        NIF.cuckoo_info(ref)

      assert num_buckets == 1024
      assert bucket_size == 4
      assert fp_size == 2
      assert num_items == 1
      assert num_deletes == 0
      assert total_slots == 1024 * 4
      assert max_kicks == 500
    end

    test "filter full returns error on add" do
      # Tiny filter: 2 buckets * 2 slots = 4 total slots
      {:ok, ref} = NIF.cuckoo_create(2, 2, 10, 0)

      results =
        Enum.map(0..99, fn i ->
          NIF.cuckoo_add(ref, "elem_#{i}")
        end)

      assert Enum.any?(results, &match?({:error, _}, &1))
    end
  end

  describe "NIF Bitcask storage pattern" do
    test "filter persisted to store and reloaded" do
      store = MockStore.make()

      # Add via command handler (persists to store)
      Cuckoo.handle("CF.RESERVE", ["mykey", "512"], store)
      Cuckoo.handle("CF.ADD", ["mykey", "stored_item"], store)

      # Verify the store has a binary blob (not an Elixir term)
      blob = store.get.("mykey")
      assert is_binary(blob)
      # Cuckoo NIF format starts with magic bytes 0xCF 0x01
      assert <<0xCF, 0x01, _rest::binary>> = blob

      # Verify we can reload and query
      assert 1 = Cuckoo.handle("CF.EXISTS", ["mykey", "stored_item"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["mykey", "not_stored"], store)
    end

    test "multiple operations persist correctly through store" do
      store = MockStore.make()

      Cuckoo.handle("CF.ADD", ["k", "a"], store)
      Cuckoo.handle("CF.ADD", ["k", "b"], store)
      Cuckoo.handle("CF.ADD", ["k", "c"], store)
      Cuckoo.handle("CF.DEL", ["k", "b"], store)

      # Fresh load from store should see the state
      assert 1 = Cuckoo.handle("CF.EXISTS", ["k", "a"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["k", "b"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["k", "c"], store)

      info =
        Cuckoo.handle("CF.INFO", ["k"], store)
        |> Enum.chunk_every(2)
        |> Enum.into(%{}, fn [k, v] -> {k, v} end)

      assert info["Number of items inserted"] == 2
      assert info["Number of items deleted"] == 1
    end
  end
end
