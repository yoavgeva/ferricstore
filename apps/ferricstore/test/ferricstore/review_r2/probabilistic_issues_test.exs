defmodule Ferricstore.ReviewR2.ProbabilisticIssuesTest do
  @moduledoc """
  Regression tests proving issues found in code review R2 for vector and
  probabilistic data structures:

    * R2-H9:  Vector VSEARCH/VGET crash on corrupted data
    * R2-H10: TopK cache coherency race (regression guard)
    * R2-M5:  Bloom/Cuckoo registries lose metadata on recovery (regression guard)
    * R2-M6:  BF.ADD NIF errors not checked (regression guard)
    * R2-M7:  TOPK.LIST WITHCOUNT NIF errors unchecked (regression guard)
    * R2-M8:  Vector sentinel key pollution in VINFO count
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Vector
  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Commands.TopK
  alias Ferricstore.Test.MockStore

  # Unique key helper to avoid collisions
  defp ukey(prefix), do: "#{prefix}_#{:erlang.unique_integer([:positive])}"

  # ---------------------------------------------------------------------------
  # R2-H9: Vector VSEARCH/VGET crash on corrupted data
  #
  # The bug: VGET calls `:erlang.binary_to_term(encoded)` on whatever bytes
  # are stored at the vector key. VSEARCH does the same inside its Enum.map
  # over compound_scan results. If a key contains corrupted or non-ETF binary
  # data, `binary_to_term` raises `ArgumentError`, crashing the process.
  #
  # Expected: return an error tuple, not crash the caller.
  # ---------------------------------------------------------------------------

  describe "R2-H9: Vector VGET/VSEARCH crash on corrupted data" do
    @tag :review_r2
    test "VGET on corrupted vector data should not crash the process" do
      store = MockStore.make()
      collection = ukey("coll")

      # Create a valid collection
      :ok = Vector.handle("VCREATE", [collection, "3", "cosine"], store)

      # Manually inject corrupted binary via compound_put.
      # This simulates disk corruption or a bug that wrote garbage bytes.
      corrupted_key = "V:" <> collection <> <<0>> <> "corrupted_vec"
      store.compound_put.(collection, corrupted_key, "this_is_not_valid_etf", 0)

      # VGET should not crash. It may return an error or the raw data,
      # but it must not raise / throw / exit.
      result =
        try do
          Vector.handle("VGET", [collection, "corrupted_vec"], store)
        rescue
          e -> {:crashed, e}
        catch
          kind, reason -> {:crashed, {kind, reason}}
        end

      # If the code is correct, result should be an error tuple, not a crash.
      # Currently this WILL crash with ArgumentError from binary_to_term.
      # This test documents the bug: if it fails with {:crashed, _}, the bug
      # is confirmed.
      refute match?({:crashed, _}, result),
        "VGET crashed on corrupted data instead of returning an error: #{inspect(result)}"
    end

    @tag :review_r2
    test "VSEARCH should not crash when one vector entry is corrupted" do
      store = MockStore.make()
      collection = ukey("coll")

      # Create collection and add a valid vector
      :ok = Vector.handle("VCREATE", [collection, "3", "cosine"], store)
      :ok = Vector.handle("VADD", [collection, "valid_vec", "1.0", "0.0", "0.0"], store)

      # Inject a corrupted entry directly
      corrupted_key = "V:" <> collection <> <<0>> <> "corrupted_vec"
      store.compound_put.(collection, corrupted_key, "garbage_bytes", 0)

      # VSEARCH should not crash. It should either skip the corrupted entry
      # or return an error, but never raise.
      result =
        try do
          Vector.handle("VSEARCH", [collection, "1.0", "0.0", "0.0", "TOP", "10"], store)
        rescue
          e -> {:crashed, e}
        catch
          kind, reason -> {:crashed, {kind, reason}}
        end

      refute match?({:crashed, _}, result),
        "VSEARCH crashed on corrupted data instead of handling gracefully: #{inspect(result)}"
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H10: TopK cache coherency race (regression guard)
  #
  # The bug: TopK uses a per-process ETS cache for mmap handles. If two
  # processes operate on the same key concurrently, one may see stale cache
  # state. This test is a regression guard: it creates a TopK, adds items,
  # lists them, and verifies consistent results — if the cache is coherent,
  # the results should be deterministic.
  # ---------------------------------------------------------------------------

  describe "R2-H10: TopK cache coherency regression guard" do
    @tag :review_r2
    test "TopK add + list returns consistent results in single process" do
      store = MockStore.make()
      key = ukey("topk")

      :ok = TopK.handle("TOPK.RESERVE", [key, "5"], store)

      # Add elements with known counts
      TopK.handle("TOPK.INCRBY", [key, "alpha", "100"], store)
      TopK.handle("TOPK.INCRBY", [key, "beta", "50"], store)
      TopK.handle("TOPK.INCRBY", [key, "gamma", "30"], store)

      # LIST should return all three, sorted by count descending
      items = TopK.handle("TOPK.LIST", [key], store)
      assert is_list(items)
      assert "alpha" in items
      assert "beta" in items
      assert "gamma" in items

      # Verify ordering: alpha (100) > beta (50) > gamma (30)
      assert items == ["alpha", "beta", "gamma"]

      # QUERY should confirm all are in the top-K
      assert [1, 1, 1] = TopK.handle("TOPK.QUERY", [key, "alpha", "beta", "gamma"], store)

      # Read again to ensure cache consistency after multiple operations
      items2 = TopK.handle("TOPK.LIST", [key], store)
      assert items == items2, "TopK LIST results changed between consecutive reads"
    end

    @tag :review_r2
    test "TopK add + list returns consistent results across many operations" do
      store = MockStore.make()
      key = ukey("topk_many")

      :ok = TopK.handle("TOPK.RESERVE", [key, "3"], store)

      # Add many elements, the top 3 by count should be deterministic
      for i <- 1..20 do
        TopK.handle("TOPK.INCRBY", [key, "elem_#{i}", "#{i * 10}"], store)
      end

      items = TopK.handle("TOPK.LIST", [key], store)
      assert length(items) == 3

      # The top 3 elements should be elem_20 (200), elem_19 (190), elem_18 (180)
      assert "elem_20" in items
      assert "elem_19" in items
      assert "elem_18" in items
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M5: Bloom/Cuckoo registries lose metadata on recovery
  #
  # The bug: BloomRegistry.recover/2 re-opens bloom files from disk but does
  # not restore the {capacity, error_rate} metadata map. After a shard restart,
  # BF.INFO returns default values instead of the ones specified at BF.RESERVE.
  #
  # This is a regression guard: we create a bloom filter with specific params,
  # verify BF.INFO returns them. The metadata loss only manifests after shard
  # restart (not testable with MockStore), so this guards the pre-restart state.
  # ---------------------------------------------------------------------------

  describe "R2-M5: Bloom metadata preservation regression guard" do
    @tag :review_r2
    test "BF.INFO returns correct capacity and error_rate after BF.RESERVE" do
      store = MockStore.make()
      key = ukey("bloom_meta")

      # Reserve with specific non-default values
      capacity = 5000
      error_rate = 0.001

      assert :ok = Bloom.handle("BF.RESERVE", [key, "#{error_rate}", "#{capacity}"], store)

      # BF.INFO should reflect the specified values
      result = Bloom.handle("BF.INFO", [key], store)
      assert is_list(result)

      info = list_to_info_map(result)
      assert info["Capacity"] == capacity,
        "Expected capacity #{capacity}, got #{inspect(info["Capacity"])}"
      assert info["Error rate"] == error_rate,
        "Expected error_rate #{error_rate}, got #{inspect(info["Error rate"])}"
    end

    @tag :review_r2
    test "BF.INFO returns correct metadata after adding elements" do
      store = MockStore.make()
      key = ukey("bloom_meta2")

      :ok = Bloom.handle("BF.RESERVE", [key, "0.05", "2000"], store)

      # Add some elements
      for i <- 1..100 do
        Bloom.handle("BF.ADD", [key, "item_#{i}"], store)
      end

      result = Bloom.handle("BF.INFO", [key], store)
      info = list_to_info_map(result)

      assert info["Capacity"] == 2000
      assert info["Error rate"] == 0.05
      assert info["Number of items inserted"] == 100
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M6: BF.ADD NIF errors not checked
  #
  # The bug: Bloom.handle("BF.ADD", ...) calls NIF.bloom_add(resource, element)
  # and returns the result directly without checking for {:error, _}. If the
  # NIF fails (e.g., corrupted mmap), the error tuple propagates as-is to the
  # client instead of being handled.
  #
  # Regression guard: BF.ADD on a valid filter should return 0 or 1.
  # ---------------------------------------------------------------------------

  describe "R2-M6: BF.ADD NIF error checking regression guard" do
    @tag :review_r2
    test "BF.ADD returns 0 or 1 on a valid filter, not an error tuple" do
      store = MockStore.make()
      key = ukey("bloom_add")

      :ok = Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)

      # First add should return 1 (new element)
      result1 = Bloom.handle("BF.ADD", [key, "hello"], store)
      assert result1 in [0, 1],
        "BF.ADD should return 0 or 1, got: #{inspect(result1)}"
      assert result1 == 1

      # Second add of same element should return 0 (already present)
      result2 = Bloom.handle("BF.ADD", [key, "hello"], store)
      assert result2 in [0, 1],
        "BF.ADD should return 0 or 1, got: #{inspect(result2)}"
      assert result2 == 0
    end

    @tag :review_r2
    test "BF.ADD auto-create returns integer, not error" do
      store = MockStore.make()
      key = ukey("bloom_auto")

      # Auto-create path (no BF.RESERVE)
      result = Bloom.handle("BF.ADD", [key, "world"], store)
      assert is_integer(result),
        "BF.ADD auto-create should return integer, got: #{inspect(result)}"
      assert result == 1
    end

    @tag :review_r2
    test "BF.MADD returns list of integers, not error tuples" do
      store = MockStore.make()
      key = ukey("bloom_madd")

      :ok = Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)

      result = Bloom.handle("BF.MADD", [key, "a", "b", "c"], store)
      assert is_list(result), "BF.MADD should return a list, got: #{inspect(result)}"
      assert length(result) == 3

      for {r, elem} <- Enum.zip(result, ["a", "b", "c"]) do
        assert r in [0, 1],
          "BF.MADD result for '#{elem}' should be 0 or 1, got: #{inspect(r)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M7: TOPK.LIST WITHCOUNT NIF errors unchecked
  #
  # The bug: TOPK.LIST WITHCOUNT calls NIF.topk_file_list(ref) then passes
  # the result to NIF.topk_file_count(ref, items) and then Enum.zip/2
  # without checking if either NIF call returned an error tuple. If the NIF
  # fails, Enum.zip crashes with a FunctionClauseError.
  #
  # Regression guard: verify the happy path returns valid interleaved results.
  # ---------------------------------------------------------------------------

  describe "R2-M7: TOPK.LIST WITHCOUNT regression guard" do
    @tag :review_r2
    test "TOPK.LIST WITHCOUNT returns valid interleaved [elem, count, ...] results" do
      store = MockStore.make()
      key = ukey("topk_wc")

      :ok = TopK.handle("TOPK.RESERVE", [key, "5"], store)

      TopK.handle("TOPK.INCRBY", [key, "x", "10"], store)
      TopK.handle("TOPK.INCRBY", [key, "y", "20"], store)
      TopK.handle("TOPK.INCRBY", [key, "z", "30"], store)

      result = TopK.handle("TOPK.LIST", [key, "WITHCOUNT"], store)
      assert is_list(result),
        "TOPK.LIST WITHCOUNT should return a list, got: #{inspect(result)}"

      # Should be 6 elements: [elem, count, elem, count, elem, count]
      assert length(result) == 6,
        "Expected 6 elements, got #{length(result)}: #{inspect(result)}"

      # Parse into pairs and validate types
      pairs = Enum.chunk_every(result, 2)

      for [elem, count] <- pairs do
        assert is_binary(elem),
          "Element name should be binary, got: #{inspect(elem)}"
        assert is_integer(count) and count > 0,
          "Count should be a positive integer, got: #{inspect(count)}"
      end

      # Verify descending order by count
      counts = Enum.map(pairs, fn [_, c] -> c end)
      assert counts == Enum.sort(counts, :desc),
        "Counts should be in descending order, got: #{inspect(counts)}"
    end

    @tag :review_r2
    test "TOPK.LIST WITHCOUNT on empty TopK returns empty list" do
      store = MockStore.make()
      key = ukey("topk_wc_empty")

      :ok = TopK.handle("TOPK.RESERVE", [key, "3"], store)

      result = TopK.handle("TOPK.LIST", [key, "WITHCOUNT"], store)
      assert result == [],
        "TOPK.LIST WITHCOUNT on empty TopK should return [], got: #{inspect(result)}"
    end

    @tag :review_r2
    test "TOPK.LIST (without WITHCOUNT) returns only element names" do
      store = MockStore.make()
      key = ukey("topk_plain")

      :ok = TopK.handle("TOPK.RESERVE", [key, "3"], store)
      TopK.handle("TOPK.INCRBY", [key, "a", "5"], store)
      TopK.handle("TOPK.INCRBY", [key, "b", "10"], store)

      result = TopK.handle("TOPK.LIST", [key], store)
      assert is_list(result)
      assert Enum.all?(result, &is_binary/1),
        "TOPK.LIST should return only strings, got: #{inspect(result)}"
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M8: Vector sentinel key pollution in VINFO count
  #
  # The bug: VCREATE stores a sentinel key `V:collection\0__hnsw_meta__` in
  # the compound key space. VINFO subtracts 1 from the raw compound_count if
  # the sentinel exists. If the subtraction logic is wrong, VINFO reports
  # N+1 or N-1 vectors instead of N.
  #
  # This test creates a collection, adds exactly N vectors, and verifies
  # VINFO reports exactly N (not N+1 from the sentinel, not N-1 from
  # over-correction).
  # ---------------------------------------------------------------------------

  describe "R2-M8: Vector sentinel key pollution in VINFO count" do
    @tag :review_r2
    test "VINFO vector_count matches exact number of added vectors" do
      store = MockStore.make()
      collection = ukey("coll_sentinel")
      n = 5

      :ok = Vector.handle("VCREATE", [collection, "3", "cosine"], store)

      # Add exactly N vectors
      for i <- 1..n do
        :ok = Vector.handle("VADD", [collection, "vec_#{i}", "1.0", "2.0", "3.0"], store)
      end

      result = Vector.handle("VINFO", [collection], store)
      info = list_to_info_map(result)

      assert info["vector_count"] == n,
        "Expected vector_count == #{n}, got #{inspect(info["vector_count"])}. " <>
        "Sentinel key may be polluting the count."
    end

    @tag :review_r2
    test "VINFO vector_count is 0 for empty collection (not -1 from sentinel overcorrection)" do
      store = MockStore.make()
      collection = ukey("coll_empty")

      :ok = Vector.handle("VCREATE", [collection, "4", "l2"], store)

      result = Vector.handle("VINFO", [collection], store)
      info = list_to_info_map(result)

      assert info["vector_count"] == 0,
        "Expected vector_count == 0 for empty collection, got #{inspect(info["vector_count"])}"
    end

    @tag :review_r2
    test "VINFO vector_count stays correct after add/delete cycle" do
      store = MockStore.make()
      collection = ukey("coll_cycle")
      n = 10

      :ok = Vector.handle("VCREATE", [collection, "2", "l2"], store)

      # Add N vectors
      for i <- 1..n do
        :ok = Vector.handle("VADD", [collection, "v_#{i}", "1.0", "0.0"], store)
      end

      info = list_to_info_map(Vector.handle("VINFO", [collection], store))
      assert info["vector_count"] == n

      # Delete half
      for i <- 1..div(n, 2) do
        Vector.handle("VDEL", [collection, "v_#{i}"], store)
      end

      info2 = list_to_info_map(Vector.handle("VINFO", [collection], store))
      assert info2["vector_count"] == n - div(n, 2),
        "Expected #{n - div(n, 2)} vectors after deleting #{div(n, 2)}, " <>
        "got #{inspect(info2["vector_count"])}"
    end

    @tag :review_r2
    test "VSEARCH result count matches VINFO vector_count (no sentinel in results)" do
      store = MockStore.make()
      collection = ukey("coll_search_count")
      n = 4

      :ok = Vector.handle("VCREATE", [collection, "2", "l2"], store)

      for i <- 1..n do
        :ok = Vector.handle("VADD", [collection, "v_#{i}", "#{i}.0", "0.0"], store)
      end

      # VSEARCH with k > n should return exactly n results
      search_result = Vector.handle(
        "VSEARCH",
        [collection, "1.0", "0.0", "TOP", "100"],
        store
      )

      keys = search_result |> Enum.chunk_every(2) |> Enum.map(fn [k, _d] -> k end)
      assert length(keys) == n,
        "VSEARCH returned #{length(keys)} results but VINFO shows #{n} vectors. " <>
        "Sentinel key __hnsw_meta__ may be leaking into search results."

      # No sentinel key should appear in results
      refute "__hnsw_meta__" in keys,
        "Sentinel key __hnsw_meta__ appeared in VSEARCH results"
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp list_to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
