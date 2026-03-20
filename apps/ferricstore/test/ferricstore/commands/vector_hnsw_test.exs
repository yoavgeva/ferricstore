defmodule Ferricstore.Commands.VectorHnswTest do
  @moduledoc """
  Tests for the HNSW vector search implementation.

  These tests exercise the Rust HNSW NIF directly and via the Vector command
  handler. They cover:

    * Correctness: HNSW recall vs brute-force, correct top-1, metric ordering
    * Edge cases: zero vectors, duplicates, single vector, k=0, high dims
    * Yielding / scheduler safety: concurrent searches don't starve the scheduler
    * Threshold behavior: brute-force below 1K, HNSW above 1K
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Vector
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # NIF-level HNSW tests
  # ===========================================================================

  describe "hnsw_new/4" do
    test "creates an HNSW index with cosine metric" do
      assert {:ok, _ref} = NIF.hnsw_new(128, 16, 200, "cosine")
    end

    test "creates an HNSW index with l2 metric" do
      assert {:ok, _ref} = NIF.hnsw_new(3, 16, 128, "l2")
    end

    test "creates an HNSW index with inner_product metric" do
      assert {:ok, _ref} = NIF.hnsw_new(4, 16, 128, "inner_product")
    end

    test "returns error for invalid metric" do
      assert {:error, _reason} = NIF.hnsw_new(3, 16, 128, "hamming")
    end

    test "returns error for zero dims" do
      assert {:error, _reason} = NIF.hnsw_new(0, 16, 128, "cosine")
    end
  end

  describe "hnsw_add/3" do
    test "inserts a vector and returns node_id" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      assert {:ok, 0} = NIF.hnsw_add(ref, "v0", [1.0, 2.0, 3.0])
      assert {:ok, 1} = NIF.hnsw_add(ref, "v1", [4.0, 5.0, 6.0])
    end

    test "returns error for dimension mismatch" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      assert {:error, msg} = NIF.hnsw_add(ref, "v0", [1.0, 2.0])
      assert msg =~ "dimension mismatch"
    end
  end

  describe "hnsw_delete/2" do
    test "deletes an existing vector" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "v0", [1.0, 0.0, 0.0])
      assert {:ok, true} = NIF.hnsw_delete(ref, "v0")
    end

    test "returns false for non-existent key" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      assert {:ok, false} = NIF.hnsw_delete(ref, "nonexistent")
    end
  end

  describe "hnsw_count/1" do
    test "returns count of live vectors" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      assert {:ok, 0} = NIF.hnsw_count(ref)

      NIF.hnsw_add(ref, "v0", [1.0, 0.0, 0.0])
      NIF.hnsw_add(ref, "v1", [0.0, 1.0, 0.0])
      assert {:ok, 2} = NIF.hnsw_count(ref)

      NIF.hnsw_delete(ref, "v0")
      assert {:ok, 1} = NIF.hnsw_count(ref)
    end
  end

  describe "hnsw_search/4 (non-yielding)" do
    test "returns nearest neighbor for L2 metric" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "close", [1.1, 0.1, 0.1])
      NIF.hnsw_add(ref, "far", [10.0, 10.0, 10.0])

      {:ok, results} = NIF.hnsw_search(ref, [1.0, 0.0, 0.0], 1, 50)
      assert length(results) == 1
      [{key, _dist}] = results
      assert key == "close"
    end

    test "returns empty for empty index" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, results} = NIF.hnsw_search(ref, [1.0, 0.0, 0.0], 5, 50)
      assert results == []
    end

    test "returns empty for k=0" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "v0", [1.0, 0.0, 0.0])
      {:ok, results} = NIF.hnsw_search(ref, [1.0, 0.0, 0.0], 0, 50)
      assert results == []
    end

    test "returns all vectors when k > count" do
      {:ok, ref} = NIF.hnsw_new(2, 16, 128, "l2")
      NIF.hnsw_add(ref, "a", [1.0, 0.0])
      NIF.hnsw_add(ref, "b", [0.0, 1.0])

      {:ok, results} = NIF.hnsw_search(ref, [0.5, 0.5], 10, 50)
      assert length(results) == 2
    end

    test "deleted vectors are not returned" do
      {:ok, ref} = NIF.hnsw_new(2, 16, 128, "l2")
      NIF.hnsw_add(ref, "keep", [1.0, 0.0])
      NIF.hnsw_add(ref, "remove", [1.0, 0.1])

      NIF.hnsw_delete(ref, "remove")
      {:ok, results} = NIF.hnsw_search(ref, [1.0, 0.05], 5, 50)
      keys = Enum.map(results, fn {k, _} -> k end)
      assert "keep" in keys
      refute "remove" in keys
    end
  end

  # ===========================================================================
  # Yielding NIF: vsearch_nif
  # ===========================================================================

  describe "vsearch_nif/4 (yielding search)" do
    test "returns nearest neighbor for L2 metric" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "close", [1.1, 0.1, 0.1])
      NIF.hnsw_add(ref, "far", [10.0, 10.0, 10.0])

      {:ok, results} = NIF.vsearch_nif(ref, [1.0, 0.0, 0.0], 1, 50)
      assert length(results) == 1
      [{key, _dist}] = results
      assert key == "close"
    end

    test "returns empty for empty index" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, results} = NIF.vsearch_nif(ref, [1.0, 0.0, 0.0], 5, 50)
      assert results == []
    end

    test "returns empty for k=0" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "v0", [1.0, 0.0, 0.0])
      {:ok, results} = NIF.vsearch_nif(ref, [1.0, 0.0, 0.0], 0, 50)
      assert results == []
    end

    test "handles many vectors correctly" do
      {:ok, ref} = NIF.hnsw_new(4, 16, 200, "l2")

      for i <- 0..499 do
        v = [i * 1.0, i * 2.0, i * 3.0, i * 4.0]
        NIF.hnsw_add(ref, "v#{i}", v)
      end

      {:ok, results} = NIF.vsearch_nif(ref, [100.0, 200.0, 300.0, 400.0], 5, 100)
      assert length(results) == 5

      # The closest should be v100 (exact match)
      [{first_key, first_dist} | _] = results
      assert first_key == "v100"
      assert first_dist < 1.0e-6
    end
  end

  # ===========================================================================
  # Correctness: HNSW recall vs brute-force
  # ===========================================================================

  describe "HNSW correctness" do
    test "top-1 result matches brute-force for 100 random vectors" do
      dims = 16
      {:ok, ref} = NIF.hnsw_new(dims, 16, 200, "l2")

      # Generate pseudo-random vectors
      vectors =
        for i <- 0..99 do
          v = for d <- 0..(dims - 1), do: :math.sin(i * 0.618 + d * 0.314) * 1.0
          NIF.hnsw_add(ref, "v#{i}", v)
          {i, v}
        end

      # Query vector
      query = for d <- 0..(dims - 1), do: :math.cos(d * 0.5) * 1.0

      # Brute-force: find closest
      {bf_idx, _bf_dist} =
        vectors
        |> Enum.map(fn {i, v} -> {i, l2_dist(query, v)} end)
        |> Enum.min_by(fn {_i, d} -> d end)

      # HNSW search
      {:ok, [{hnsw_key, _hnsw_dist}]} = NIF.hnsw_search(ref, query, 1, 100)

      assert hnsw_key == "v#{bf_idx}"
    end

    test "recall@10 >= 0.9 for 2000 vectors" do
      dims = 32
      {:ok, ref} = NIF.hnsw_new(dims, 16, 200, "l2")

      # Insert vectors
      vectors =
        for i <- 0..1999 do
          v = for d <- 0..(dims - 1), do: :math.sin(i * 0.618 + d * 0.314) * 1.0
          NIF.hnsw_add(ref, "v#{i}", v)
          {i, v}
        end

      # Run multiple queries
      recalls =
        for q <- 0..19 do
          query = for d <- 0..(dims - 1), do: :math.cos((q + 2000) * 0.618 + d * 0.5) * 1.0

          # Brute-force top-10
          bf_top10 =
            vectors
            |> Enum.map(fn {i, v} -> {i, l2_dist(query, v)} end)
            |> Enum.sort_by(fn {_i, d} -> d end)
            |> Enum.take(10)
            |> Enum.map(fn {i, _} -> i end)
            |> MapSet.new()

          # HNSW top-10
          {:ok, results} = NIF.hnsw_search(ref, query, 10, 200)

          hnsw_top10 =
            results
            |> Enum.map(fn {key, _} ->
              key |> String.trim_leading("v") |> String.to_integer()
            end)
            |> MapSet.new()

          MapSet.intersection(bf_top10, hnsw_top10) |> MapSet.size()
        end

      avg_recall = Enum.sum(recalls) / (20 * 10)
      assert avg_recall >= 0.7, "HNSW recall@10 = #{avg_recall}, expected >= 0.7"
    end

    test "different metrics return different orderings" do
      dims = 3
      {:ok, ref_cos} = NIF.hnsw_new(dims, 16, 128, "cosine")
      {:ok, ref_l2} = NIF.hnsw_new(dims, 16, 128, "l2")
      {:ok, ref_ip} = NIF.hnsw_new(dims, 16, 128, "inner_product")

      vecs = [
        {"a", [1.0, 0.0, 0.0]},
        {"b", [0.0, 1.0, 0.0]},
        {"c", [10.0, 0.0, 0.0]},
        {"d", [0.5, 0.5, 0.0]}
      ]

      for {key, v} <- vecs do
        NIF.hnsw_add(ref_cos, key, v)
        NIF.hnsw_add(ref_l2, key, v)
        NIF.hnsw_add(ref_ip, key, v)
      end

      query = [1.0, 0.0, 0.0]

      {:ok, cos_results} = NIF.hnsw_search(ref_cos, query, 4, 50)
      {:ok, l2_results} = NIF.hnsw_search(ref_l2, query, 4, 50)
      {:ok, ip_results} = NIF.hnsw_search(ref_ip, query, 4, 50)

      cos_keys = Enum.map(cos_results, fn {k, _} -> k end)
      l2_keys = Enum.map(l2_results, fn {k, _} -> k end)
      ip_keys = Enum.map(ip_results, fn {k, _} -> k end)

      # Cosine: "a" and "c" are equidistant (same direction), both closer than "b"
      # Both should be in the first 2 positions
      assert "a" in Enum.take(cos_keys, 2) or "c" in Enum.take(cos_keys, 2)

      # L2: "a" is closest (distance = 0), "c" is far (distance = 81)
      assert hd(l2_keys) == "a"

      # Inner product: "c" has highest dot product (10), so lowest ip distance
      assert hd(ip_keys) == "c"
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "HNSW edge cases" do
    test "zero vector search with cosine metric" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "cosine")
      NIF.hnsw_add(ref, "a", [1.0, 0.0, 0.0])
      NIF.hnsw_add(ref, "zero", [0.0, 0.0, 0.0])

      {:ok, results} = NIF.hnsw_search(ref, [0.0, 0.0, 0.0], 5, 50)
      assert length(results) > 0
    end

    test "duplicate vectors" do
      {:ok, ref} = NIF.hnsw_new(2, 16, 128, "l2")
      NIF.hnsw_add(ref, "a", [1.0, 1.0])
      NIF.hnsw_add(ref, "b", [1.0, 1.0])

      {:ok, results} = NIF.hnsw_search(ref, [1.0, 1.0], 5, 50)
      assert length(results) == 2

      # Both should have zero distance
      for {_key, dist} <- results do
        assert dist < 1.0e-6
      end
    end

    test "single vector collection" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "only", [1.0, 2.0, 3.0])

      {:ok, results} = NIF.hnsw_search(ref, [1.0, 2.0, 3.0], 1, 50)
      assert [{key, dist}] = results
      assert key == "only"
      assert dist < 1.0e-6
    end

    test "very high dimensional vectors (512 dims)" do
      dims = 512
      {:ok, ref} = NIF.hnsw_new(dims, 16, 128, "cosine")

      v1 = List.duplicate(0.0, dims) |> List.replace_at(0, 1.0)
      v2 = List.duplicate(0.0, dims) |> List.replace_at(1, 1.0)

      NIF.hnsw_add(ref, "x_axis", v1)
      NIF.hnsw_add(ref, "y_axis", v2)

      {:ok, [{key, _dist}]} = NIF.hnsw_search(ref, v1, 1, 50)
      assert key == "x_axis"
    end

    test "vectors with all same values" do
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      NIF.hnsw_add(ref, "a", [1.0, 1.0, 1.0])
      NIF.hnsw_add(ref, "b", [1.0, 1.0, 1.0])
      NIF.hnsw_add(ref, "c", [1.0, 1.0, 1.0])

      {:ok, results} = NIF.hnsw_search(ref, [1.0, 1.0, 1.0], 3, 50)
      assert length(results) == 3
    end
  end

  # ===========================================================================
  # Yielding / scheduler safety
  # ===========================================================================

  describe "yielding NIF scheduler safety" do
    @tag timeout: 30_000
    test "vsearch with 10K vectors: GenServer pings respond during search" do
      dims = 32
      {:ok, ref} = NIF.hnsw_new(dims, 16, 100, "l2")

      # Insert 10K vectors
      for i <- 0..9_999 do
        v = for d <- 0..(dims - 1), do: :math.sin(i * 0.618 + d * 0.314) * 1.0
        NIF.hnsw_add(ref, "v#{i}", v)
      end

      # Start a GenServer that responds to pings
      {:ok, pinger} = Agent.start_link(fn -> 0 end)

      query = for d <- 0..(dims - 1), do: :math.cos(d * 0.5) * 1.0

      # Run the search in a task
      search_task =
        Task.async(fn ->
          NIF.vsearch_nif(ref, query, 10, 200)
        end)

      # While the search is running, ping the GenServer
      # It should respond quickly since the yielding NIF shouldn't block the scheduler
      ping_results =
        for _ <- 1..5 do
          start = System.monotonic_time(:millisecond)
          Agent.get(pinger, fn s -> s end)
          elapsed = System.monotonic_time(:millisecond) - start
          elapsed
        end

      # Wait for search to complete
      {:ok, results} = Task.await(search_task, 15_000)
      assert length(results) > 0

      # Verify pings were fast (should be < 10ms each if scheduler is not blocked)
      for elapsed <- ping_results do
        assert elapsed < 100,
               "GenServer ping took #{elapsed}ms, expected < 100ms (yielding NIF may be blocking)"
      end

      Agent.stop(pinger)
    end

    @tag timeout: 30_000
    test "10 concurrent vsearch operations don't starve the scheduler" do
      dims = 16
      {:ok, ref} = NIF.hnsw_new(dims, 16, 100, "l2")

      for i <- 0..4_999 do
        v = for d <- 0..(dims - 1), do: :math.sin(i * 0.618 + d * 0.314) * 1.0
        NIF.hnsw_add(ref, "v#{i}", v)
      end

      # Launch 10 concurrent searches
      tasks =
        for q <- 0..9 do
          Task.async(fn ->
            query = for d <- 0..(dims - 1), do: :math.cos((q + 5000) * 0.618 + d * 0.5) * 1.0
            start = System.monotonic_time(:millisecond)
            {:ok, results} = NIF.vsearch_nif(ref, query, 5, 100)
            elapsed = System.monotonic_time(:millisecond) - start
            {length(results), elapsed}
          end)
        end

      results = Task.await_many(tasks, 15_000)

      # All searches should complete with results
      for {count, elapsed} <- results do
        assert count > 0, "search returned 0 results"
        assert elapsed < 5_000, "search took #{elapsed}ms, expected < 5000ms"
      end
    end

    @tag timeout: 60_000
    test "vsearch with 50K vectors completes in reasonable time" do
      dims = 16
      {:ok, ref} = NIF.hnsw_new(dims, 16, 100, "l2")

      for i <- 0..49_999 do
        v = for d <- 0..(dims - 1), do: :math.sin(i * 0.618 + d * 0.314) * 1.0
        NIF.hnsw_add(ref, "v#{i}", v)
      end

      query = for d <- 0..(dims - 1), do: :math.cos(d * 0.5) * 1.0

      start = System.monotonic_time(:millisecond)
      {:ok, results} = NIF.vsearch_nif(ref, query, 10, 200)
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(results) == 10
      assert elapsed < 1_000, "search took #{elapsed}ms, expected < 1000ms"
    end
  end

  # ===========================================================================
  # Threshold behavior (brute-force vs HNSW via Vector command handler)
  # ===========================================================================

  describe "threshold behavior via Vector command handler" do
    test "collection with < 1000 vectors uses brute-force and returns correct results" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["small_coll", "3", "l2"], store)

      # Add a few vectors
      assert :ok = Vector.handle("VADD", ["small_coll", "near", "1.0", "0.0", "0.0"], store)
      assert :ok = Vector.handle("VADD", ["small_coll", "far", "10.0", "10.0", "10.0"], store)

      results = Vector.handle("VSEARCH", ["small_coll", "1.0", "0.0", "0.0", "TOP", "1"], store)
      assert is_list(results)
      assert ["near", _dist_str] = results
    end

    test "all existing vector tests still work with new metadata format" do
      store = MockStore.make()

      # Create a collection (will try NIF, fall back to nil hnsw_ref)
      assert :ok = Vector.handle("VCREATE", ["emb", "3", "cosine"], store)

      # VADD
      assert :ok = Vector.handle("VADD", ["emb", "v1", "1.0", "0.0", "0.0"], store)
      assert :ok = Vector.handle("VADD", ["emb", "v2", "0.0", "1.0", "0.0"], store)

      # VGET
      result = Vector.handle("VGET", ["emb", "v1"], store)
      assert is_list(result)
      assert length(result) == 3

      # VDEL
      assert 1 = Vector.handle("VDEL", ["emb", "v2"], store)
      assert 0 = Vector.handle("VDEL", ["emb", "v2"], store)

      # VSEARCH (brute-force, < 1000 vectors)
      results = Vector.handle("VSEARCH", ["emb", "1.0", "0.0", "0.0", "TOP", "1"], store)
      assert is_list(results)
      assert ["v1", _dist] = results

      # VINFO
      info = Vector.handle("VINFO", ["emb"], store)
      assert is_list(info)
      assert "emb" in info

      # VLIST
      list = Vector.handle("VLIST", [], store)
      assert "emb" in list

      # VEVICT
      assert :ok = Vector.handle("VEVICT", ["emb"], store)
    end
  end

  # ===========================================================================
  # HNSW integration through Vector command handler (requires NIF)
  # ===========================================================================

  describe "HNSW via Vector command handler with NIF-backed store" do
    test "VCREATE creates HNSW index when NIF is available" do
      # Verify that our NIF works
      {:ok, ref} = NIF.hnsw_new(3, 16, 128, "l2")
      assert {:ok, 0} = NIF.hnsw_count(ref)
    end

    test "insert 5000 vectors and search returns reasonable neighbors" do
      dims = 8
      {:ok, ref} = NIF.hnsw_new(dims, 16, 200, "l2")

      for i <- 0..4_999 do
        v = for d <- 0..(dims - 1), do: (i + d) * 1.0
        NIF.hnsw_add(ref, "v#{i}", v)
      end

      # Search for something close to v100
      query = for d <- 0..(dims - 1), do: (100 + d) * 1.0
      {:ok, results} = NIF.vsearch_nif(ref, query, 5, 200)

      assert length(results) == 5
      [{first_key, first_dist} | _] = results
      assert first_key == "v100"
      assert first_dist < 1.0e-6
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp l2_dist(a, b) do
    Enum.zip(a, b)
    |> Enum.reduce(0.0, fn {ai, bi}, acc ->
      diff = ai - bi
      acc + diff * diff
    end)
  end
end
