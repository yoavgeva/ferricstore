defmodule Ferricstore.Commands.Vector.VinfoRamResidentBytesTest do
  @moduledoc """
  Tests for the `ram_resident_bytes` field in VINFO responses.

  `ram_resident_bytes` reports the estimated in-memory footprint of a
  collection's vectors: `vector_count * dims * 4` (f32 = 4 bytes per
  component). When HNSW with mmap is introduced later, this will reflect
  OS page-cache residency via `mincore(2)`; for now it is purely computed.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Vector
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Converts the flat alternating [key, value, key, value, ...] list
  # from VINFO into a map for easier assertions.
  defp info_map(collection, store) do
    result = Vector.handle("VINFO", [collection], store)
    result
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end

  # Generates a vector of `dims` float-string components, all "1.0".
  defp make_vector(dims) do
    List.duplicate("1.0", dims)
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "VINFO ram_resident_bytes" do
    test "VINFO returns ram_resident_bytes field" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["col", "4", "cosine"], store)

      info = info_map("col", store)
      assert Map.has_key?(info, "ram_resident_bytes")
    end

    test "ram_resident_bytes equals vector_count * dims * 4 for in-memory storage" do
      store = MockStore.make()
      dims = 128
      :ok = Vector.handle("VCREATE", ["col", Integer.to_string(dims), "l2"], store)

      :ok = Vector.handle("VADD", ["col", "k1" | make_vector(dims)], store)
      :ok = Vector.handle("VADD", ["col", "k2" | make_vector(dims)], store)
      :ok = Vector.handle("VADD", ["col", "k3" | make_vector(dims)], store)

      info = info_map("col", store)
      expected = 3 * dims * 4
      assert info["ram_resident_bytes"] == expected
    end

    test "VINFO on empty collection returns ram_resident_bytes 0" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["empty", "64", "cosine"], store)

      info = info_map("empty", store)
      assert info["ram_resident_bytes"] == 0
    end

    test "VINFO after VADD shows increased ram_resident_bytes" do
      store = MockStore.make()
      dims = 16
      :ok = Vector.handle("VCREATE", ["col", Integer.to_string(dims), "cosine"], store)

      info_before = info_map("col", store)
      assert info_before["ram_resident_bytes"] == 0

      :ok = Vector.handle("VADD", ["col", "k1" | make_vector(dims)], store)

      info_after = info_map("col", store)
      assert info_after["ram_resident_bytes"] == 1 * dims * 4
      assert info_after["ram_resident_bytes"] > info_before["ram_resident_bytes"]
    end

    test "VINFO after VDEL shows decreased ram_resident_bytes" do
      store = MockStore.make()
      dims = 8
      :ok = Vector.handle("VCREATE", ["col", Integer.to_string(dims), "l2"], store)

      :ok = Vector.handle("VADD", ["col", "k1" | make_vector(dims)], store)
      :ok = Vector.handle("VADD", ["col", "k2" | make_vector(dims)], store)

      info_before_del = info_map("col", store)
      assert info_before_del["ram_resident_bytes"] == 2 * dims * 4

      1 = Vector.handle("VDEL", ["col", "k1"], store)

      info_after_del = info_map("col", store)
      assert info_after_del["ram_resident_bytes"] == 1 * dims * 4
      assert info_after_del["ram_resident_bytes"] < info_before_del["ram_resident_bytes"]
    end

    @tag timeout: 30_000
    test "stress: VINFO on collection with 10000 vectors" do
      store = MockStore.make()
      dims = 32
      n = 10_000
      :ok = Vector.handle("VCREATE", ["big", Integer.to_string(dims), "cosine"], store)

      for i <- 1..n do
        :ok = Vector.handle("VADD", ["big", "v#{i}" | make_vector(dims)], store)
      end

      info = info_map("big", store)
      assert info["vector_count"] == n
      assert info["ram_resident_bytes"] == n * dims * 4
    end
  end
end
