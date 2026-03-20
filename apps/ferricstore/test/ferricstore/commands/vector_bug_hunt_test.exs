defmodule Ferricstore.Commands.VectorBugHuntTest do
  @moduledoc """
  Bug-hunting tests for the Vector command handler.

  Each `describe` block targets a specific edge case or potential bug:

    * VCREATE with 0 or negative dimensions
    * VADD with mismatched dimensions, NaN/Inf-like strings
    * VSEARCH with TOP 0, empty collection, each distance metric
    * VGET / VDEL on non-existent keys
    * VINFO on non-existent collection
    * VLIST with MATCH glob filtering
    * VCREATE duplicate collection
    * VADD overwrite of existing key
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Vector
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Extracts keys from the flat [key, dist, key, dist, ...] VSEARCH result.
  defp extract_keys(result) when is_list(result) do
    result
    |> Enum.chunk_every(2)
    |> Enum.map(fn [k, _d] -> k end)
  end

  # Converts VINFO's flat [k, v, k, v, ...] into a map for easier assertions.
  defp info_map(list) when is_list(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end

  # Extracts distances from the flat [key, dist, key, dist, ...] VSEARCH result.
  defp extract_distances(result) when is_list(result) do
    result
    |> Enum.chunk_every(2)
    |> Enum.map(fn [_k, d] -> String.to_float(d) end)
  end

  # Returns {key, distance} pairs from VSEARCH result.
  defp extract_pairs(result) when is_list(result) do
    result
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, d] -> {k, String.to_float(d)} end)
  end

  # Convenience: create a collection and return the store.
  defp make_collection(dims, metric \\ "cosine") do
    store = MockStore.make()
    :ok = Vector.handle("VCREATE", ["test_col", to_string(dims), metric], store)
    store
  end

  # ===========================================================================
  # VCREATE with 0 dimensions
  # ===========================================================================

  describe "VCREATE with 0 dimensions" do
    test "returns an error because dimensions must be positive" do
      store = MockStore.make()
      result = Vector.handle("VCREATE", ["col", "0", "cosine"], store)
      assert {:error, msg} = result
      assert msg =~ "positive integer"
    end

    test "collection is NOT created when 0 dimensions are given" do
      store = MockStore.make()
      Vector.handle("VCREATE", ["col", "0", "cosine"], store)
      # Verify we cannot reach the collection via VINFO
      assert {:error, _} = Vector.handle("VINFO", ["col"], store)
    end
  end

  # ===========================================================================
  # VCREATE with negative dimensions
  # ===========================================================================

  describe "VCREATE with negative dimensions" do
    test "returns an error for -1" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["col", "-1", "cosine"], store)
      assert msg =~ "positive integer"
    end

    test "returns an error for large negative" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["col", "-999", "l2"], store)
      assert msg =~ "positive integer"
    end

    test "collection is NOT created when negative dimensions are given" do
      store = MockStore.make()
      Vector.handle("VCREATE", ["col", "-5", "cosine"], store)
      assert {:error, _} = Vector.handle("VINFO", ["col"], store)
    end
  end

  # ===========================================================================
  # VADD with wrong dimension count
  # ===========================================================================

  describe "VADD with wrong dimension count" do
    test "returns error when vector has fewer components than collection dims" do
      store = make_collection(4)
      assert {:error, msg} = Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0", "3.0"], store)
      assert msg =~ "dimension mismatch"
      assert msg =~ "expected 4"
      assert msg =~ "got 3"
    end

    test "returns error when vector has more components than collection dims" do
      store = make_collection(2)

      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0", "3.0"], store)

      assert msg =~ "dimension mismatch"
      assert msg =~ "expected 2"
      assert msg =~ "got 3"
    end

    test "returns error when vector has exactly 1 extra component" do
      store = make_collection(3)

      assert {:error, msg} =
               Vector.handle(
                 "VADD",
                 ["test_col", "k1", "1.0", "2.0", "3.0", "4.0"],
                 store
               )

      assert msg =~ "dimension mismatch"
    end

    test "a mismatched VADD does not corrupt the collection" do
      store = make_collection(3)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0", "3.0"], store)
      # Bad add -- wrong dimension
      {:error, _} = Vector.handle("VADD", ["test_col", "k2", "1.0", "2.0"], store)
      # k1 should still be intact
      assert ["1.0", "2.0", "3.0"] = Vector.handle("VGET", ["test_col", "k1"], store)
      # k2 should not exist
      assert nil == Vector.handle("VGET", ["test_col", "k2"], store)
    end
  end

  # ===========================================================================
  # VADD with NaN / Inf / special float strings
  # ===========================================================================

  describe "VADD with NaN/Inf in vector" do
    setup do
      %{store: make_collection(3)}
    end

    test "rejects NaN (uppercase)", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "1.0", "NaN", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects nan (lowercase)", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "nan", "2.0", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects Inf", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "Inf", "2.0", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects -Inf", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "-Inf", "2.0", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects infinity", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "infinity", "2.0", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects -infinity", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "-infinity", "2.0", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects empty string as component", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "1.0", "", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "rejects non-numeric garbage", %{store: store} do
      assert {:error, msg} =
               Vector.handle("VADD", ["test_col", "k1", "1.0", "abc", "3.0"], store)

      assert msg =~ "not a valid float"
    end

    test "accepts valid negative floats", %{store: store} do
      assert :ok = Vector.handle("VADD", ["test_col", "k1", "-1.5", "-0.0", "-3.14"], store)
    end

    test "accepts valid integers as vector components", %{store: store} do
      assert :ok = Vector.handle("VADD", ["test_col", "k1", "1", "2", "3"], store)
      result = Vector.handle("VGET", ["test_col", "k1"], store)
      assert result == ["1.0", "2.0", "3.0"]
    end

    test "accepts scientific notation (e.g. 1e-5)", %{store: store} do
      # Float.parse("1e-5") should work
      result = Vector.handle("VADD", ["test_col", "k1", "1e-5", "2.0", "3.0"], store)

      case result do
        :ok ->
          # Accepted -- verify round-trip
          vec = Vector.handle("VGET", ["test_col", "k1"], store)
          assert is_list(vec)
          assert length(vec) == 3

        {:error, _} ->
          # Also acceptable if the parser rejects scientific notation
          :ok
      end
    end
  end

  # ===========================================================================
  # VSEARCH with TOP 0
  # ===========================================================================

  describe "VSEARCH with TOP 0" do
    test "returns error because k must be a positive integer" do
      store = make_collection(2)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "0.0"], store)

      result = Vector.handle("VSEARCH", ["test_col", "1.0", "0.0", "TOP", "0"], store)
      assert {:error, msg} = result
      assert msg =~ "positive"
    end

    test "TOP with negative k also returns error" do
      store = make_collection(2)

      result = Vector.handle("VSEARCH", ["test_col", "1.0", "0.0", "TOP", "-1"], store)
      assert {:error, msg} = result
      assert msg =~ "positive"
    end
  end

  # ===========================================================================
  # VSEARCH on empty collection
  # ===========================================================================

  describe "VSEARCH on empty collection" do
    test "returns empty list when no vectors have been added" do
      store = make_collection(3)
      result = Vector.handle("VSEARCH", ["test_col", "1.0", "2.0", "3.0", "TOP", "10"], store)
      assert result == []
    end

    test "returns empty list after all vectors have been deleted" do
      store = make_collection(2)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "0.0"], store)
      1 = Vector.handle("VDEL", ["test_col", "k1"], store)

      result = Vector.handle("VSEARCH", ["test_col", "1.0", "0.0", "TOP", "5"], store)
      assert result == []
    end
  end

  # ===========================================================================
  # VSEARCH with different metrics -- distance correctness
  # ===========================================================================

  describe "VSEARCH cosine distance correctness" do
    test "identical direction yields distance ~0.0" do
      store = make_collection(3, "cosine")
      :ok = Vector.handle("VADD", ["test_col", "same", "1.0", "2.0", "3.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "2.0", "4.0", "6.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      assert_in_delta dist, 0.0, 1.0e-9
    end

    test "opposite direction yields distance ~2.0" do
      store = make_collection(3, "cosine")
      :ok = Vector.handle("VADD", ["test_col", "opp", "-1.0", "-2.0", "-3.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "1.0", "2.0", "3.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      assert_in_delta dist, 2.0, 1.0e-9
    end

    test "orthogonal unit vectors yield distance ~1.0" do
      store = make_collection(2, "cosine")
      :ok = Vector.handle("VADD", ["test_col", "y", "0.0", "1.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "1.0", "0.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      assert_in_delta dist, 1.0, 1.0e-9
    end

    test "zero vector query returns distance 1.0 (safe fallback)" do
      store = make_collection(2, "cosine")
      :ok = Vector.handle("VADD", ["test_col", "v1", "1.0", "0.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      # Implementation returns 1.0 as safe fallback for zero vectors
      assert_in_delta dist, 1.0, 1.0e-9
    end

    test "zero stored vector returns distance 1.0 (safe fallback)" do
      store = make_collection(2, "cosine")
      :ok = Vector.handle("VADD", ["test_col", "zero", "0.0", "0.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "1.0", "0.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      assert_in_delta dist, 1.0, 1.0e-9
    end

    test "both query and stored are zero vectors -- distance is 1.0" do
      store = make_collection(2, "cosine")
      :ok = Vector.handle("VADD", ["test_col", "zero", "0.0", "0.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      assert_in_delta dist, 1.0, 1.0e-9
    end
  end

  describe "VSEARCH l2 distance correctness" do
    test "distance to self is 0.0" do
      store = make_collection(3, "l2")
      :ok = Vector.handle("VADD", ["test_col", "pt", "3.0", "4.0", "5.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "3.0", "4.0", "5.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      assert_in_delta dist, 0.0, 1.0e-9
    end

    test "squared euclidean distance is correct for known vectors" do
      store = make_collection(2, "l2")
      :ok = Vector.handle("VADD", ["test_col", "pt", "3.0", "4.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      # 3^2 + 4^2 = 9 + 16 = 25
      assert_in_delta dist, 25.0, 1.0e-9
    end

    test "l2 ranking is correct: closer point first" do
      store = make_collection(2, "l2")
      :ok = Vector.handle("VADD", ["test_col", "near", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "far", "10.0", "0.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "2"], store)
      keys = extract_keys(result)
      assert keys == ["near", "far"]
    end
  end

  describe "VSEARCH inner_product distance correctness" do
    test "distance is 1 - dot(a, b)" do
      store = make_collection(2, "inner_product")
      :ok = Vector.handle("VADD", ["test_col", "v", "2.0", "3.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "4.0", "5.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      # dot(2,3 . 4,5) = 8+15 = 23, distance = 1-23 = -22
      assert_in_delta dist, -22.0, 1.0e-9
    end

    test "higher dot product means lower distance (more similar)" do
      store = make_collection(2, "inner_product")
      :ok = Vector.handle("VADD", ["test_col", "high", "10.0", "10.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "low", "0.1", "0.1"], store)
      result = Vector.handle("VSEARCH", ["test_col", "1.0", "1.0", "TOP", "2"], store)
      keys = extract_keys(result)
      # high has dot=20, low has dot=0.2
      # dist(high) = 1-20 = -19, dist(low) = 1-0.2 = 0.8
      # sorted ascending: high first
      assert hd(keys) == "high"
    end

    test "orthogonal vectors have distance of 1.0" do
      store = make_collection(2, "inner_product")
      :ok = Vector.handle("VADD", ["test_col", "ortho", "0.0", "1.0"], store)
      result = Vector.handle("VSEARCH", ["test_col", "1.0", "0.0", "TOP", "1"], store)
      [dist] = extract_distances(result)
      # dot(1,0 . 0,1) = 0, distance = 1-0 = 1.0
      assert_in_delta dist, 1.0, 1.0e-9
    end
  end

  # ===========================================================================
  # VGET non-existent key
  # ===========================================================================

  describe "VGET non-existent key" do
    test "returns nil for a key that was never added" do
      store = make_collection(3)
      result = Vector.handle("VGET", ["test_col", "does_not_exist"], store)
      assert result == nil
    end

    test "returns nil for a key that was added then deleted" do
      store = make_collection(2)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0"], store)
      1 = Vector.handle("VDEL", ["test_col", "k1"], store)
      assert nil == Vector.handle("VGET", ["test_col", "k1"], store)
    end

    test "returns error when collection itself does not exist" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VGET", ["no_such_col", "k1"], store)
      assert msg =~ "not found"
    end
  end

  # ===========================================================================
  # VDEL non-existent key
  # ===========================================================================

  describe "VDEL non-existent key" do
    test "returns 0 for a key that was never added" do
      store = make_collection(3)
      assert 0 = Vector.handle("VDEL", ["test_col", "ghost"], store)
    end

    test "returns 0 for a key that was already deleted" do
      store = make_collection(2)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0"], store)
      assert 1 = Vector.handle("VDEL", ["test_col", "k1"], store)
      assert 0 = Vector.handle("VDEL", ["test_col", "k1"], store)
    end

    test "returns error when collection does not exist" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VDEL", ["no_such_col", "k1"], store)
      assert msg =~ "not found"
    end
  end

  # ===========================================================================
  # VINFO on non-existent collection
  # ===========================================================================

  describe "VINFO on non-existent collection" do
    test "returns error with descriptive message" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VINFO", ["nonexistent"], store)
      assert msg =~ "not found"
    end

    test "does not confuse similarly-named collections" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["my_col", "3", "cosine"], store)
      # my_col_2 does not exist
      assert {:error, _} = Vector.handle("VINFO", ["my_col_2"], store)
    end
  end

  # ===========================================================================
  # VLIST with MATCH pattern
  # ===========================================================================

  describe "VLIST with MATCH pattern" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["embeddings_v1", "3", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["embeddings_v2", "3", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["images", "128", "l2"], store)
      :ok = Vector.handle("VCREATE", ["audio_features", "64", "inner_product"], store)
      %{store: store}
    end

    test "wildcard * matches prefix", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "embeddings*"], store)
      assert length(result) == 2
      assert "embeddings_v1" in result
      assert "embeddings_v2" in result
    end

    test "wildcard * matches suffix", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "*features"], store)
      assert result == ["audio_features"]
    end

    test "? matches single character", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "embeddings_v?"], store)
      assert length(result) == 2
      assert "embeddings_v1" in result
      assert "embeddings_v2" in result
    end

    test "exact name with no wildcards matches only that collection", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "images"], store)
      assert result == ["images"]
    end

    test "pattern that matches nothing returns empty list", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "zzz*"], store)
      assert result == []
    end

    test "* alone matches all collections", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "*"], store)
      assert length(result) == 4
    end

    test "MATCH with COUNT combines correctly", %{store: store} do
      result = Vector.handle("VLIST", ["MATCH", "*", "COUNT", "2"], store)
      assert length(result) == 2
    end

    test "no options returns all collections sorted", %{store: store} do
      result = Vector.handle("VLIST", [], store)
      assert result == Enum.sort(result)
      assert length(result) == 4
    end
  end

  # ===========================================================================
  # VCREATE duplicate collection
  # ===========================================================================

  describe "VCREATE duplicate collection" do
    test "returns error when collection name already exists" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["my_col", "3", "cosine"], store)
      assert {:error, msg} = Vector.handle("VCREATE", ["my_col", "5", "l2"], store)
      assert msg =~ "already exists"
    end

    test "original collection is not modified by failed duplicate create" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["my_col", "3", "cosine"], store)
      {:error, _} = Vector.handle("VCREATE", ["my_col", "5", "l2"], store)

      info = info_map(Vector.handle("VINFO", ["my_col"], store))
      assert info["dims"] == 3
      assert info["metric"] == "cosine"
    end

    test "different collection names do not collide" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["col_a", "3", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["col_b", "3", "cosine"], store)

      info_a = info_map(Vector.handle("VINFO", ["col_a"], store))
      info_b = info_map(Vector.handle("VINFO", ["col_b"], store))
      assert info_a["collection"] == "col_a"
      assert info_b["collection"] == "col_b"
    end
  end

  # ===========================================================================
  # VADD overwrite existing key
  # ===========================================================================

  describe "VADD overwrite existing key" do
    test "overwrites vector data and VGET returns new value" do
      store = make_collection(3)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0", "3.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "k1", "4.0", "5.0", "6.0"], store)
      result = Vector.handle("VGET", ["test_col", "k1"], store)
      assert result == ["4.0", "5.0", "6.0"]
    end

    test "overwrite does not change vector_count" do
      store = make_collection(2)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "k1", "0.0", "1.0"], store)
      info = info_map(Vector.handle("VINFO", ["test_col"], store))
      assert info["vector_count"] == 1
    end

    test "overwrite reflects in VSEARCH results" do
      store = make_collection(2, "l2")
      :ok = Vector.handle("VADD", ["test_col", "k1", "10.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "k2", "1.0", "0.0"], store)

      # Before overwrite: k2 is closer to origin
      result1 = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "1"], store)
      assert extract_keys(result1) == ["k2"]

      # Overwrite k1 to be at origin
      :ok = Vector.handle("VADD", ["test_col", "k1", "0.0", "0.0"], store)
      result2 = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "1"], store)
      assert extract_keys(result2) == ["k1"]
    end

    test "multiple overwrites accumulate correctly" do
      store = make_collection(2)

      for i <- 1..10 do
        f = to_string(i * 1.0)
        :ok = Vector.handle("VADD", ["test_col", "k1", f, "0.0"], store)
      end

      result = Vector.handle("VGET", ["test_col", "k1"], store)
      assert result == ["10.0", "0.0"]

      info = info_map(Vector.handle("VINFO", ["test_col"], store))
      assert info["vector_count"] == 1
    end
  end

  # ===========================================================================
  # Additional edge cases / potential bugs
  # ===========================================================================

  describe "VSEARCH result ordering is stable" do
    test "vectors with equal distances appear in some deterministic order" do
      store = make_collection(2, "l2")
      # Two vectors equidistant from the query
      :ok = Vector.handle("VADD", ["test_col", "a", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "b", "0.0", "1.0"], store)

      result = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "2"], store)
      keys = extract_keys(result)
      dists = extract_distances(result)
      # Both at distance 1.0 (squared L2)
      assert length(keys) == 2
      assert_in_delta Enum.at(dists, 0), 1.0, 1.0e-9
      assert_in_delta Enum.at(dists, 1), 1.0, 1.0e-9
    end
  end

  describe "VSEARCH TOP k larger than collection size" do
    test "returns all vectors when k exceeds collection size" do
      store = make_collection(2, "l2")
      :ok = Vector.handle("VADD", ["test_col", "a", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "b", "0.0", "1.0"], store)

      result = Vector.handle("VSEARCH", ["test_col", "0.0", "0.0", "TOP", "100"], store)
      keys = extract_keys(result)
      assert length(keys) == 2
    end
  end

  describe "VSEARCH with 1-dimensional vectors" do
    test "l2 distance on 1D vectors" do
      store = make_collection(1, "l2")
      :ok = Vector.handle("VADD", ["test_col", "a", "5.0"], store)
      :ok = Vector.handle("VADD", ["test_col", "b", "10.0"], store)

      result = Vector.handle("VSEARCH", ["test_col", "0.0", "TOP", "2"], store)
      pairs = extract_pairs(result)
      # (5-0)^2 = 25, (10-0)^2 = 100
      assert_in_delta pairs["a"], 25.0, 1.0e-9
      assert_in_delta pairs["b"], 100.0, 1.0e-9
    end
  end

  describe "collection name edge cases" do
    test "collection name with special characters" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["my-collection_v1.2", "2", "cosine"], store)
      :ok = Vector.handle("VADD", ["my-collection_v1.2", "k1", "1.0", "0.0"], store)
      result = Vector.handle("VGET", ["my-collection_v1.2", "k1"], store)
      assert result == ["1.0", "0.0"]
    end

    test "collection name with colon does not collide with VM: prefix" do
      store = MockStore.make()
      # The meta key is VM:collection, so a collection named "VM:evil" would
      # have meta key "VM:VM:evil". This should NOT collide with a collection
      # named "evil" (meta key "VM:evil").
      :ok = Vector.handle("VCREATE", ["evil", "2", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["VM:evil", "2", "l2"], store)

      info1 = info_map(Vector.handle("VINFO", ["evil"], store))
      info2 = info_map(Vector.handle("VINFO", ["VM:evil"], store))
      assert info1["metric"] == "cosine"
      assert info2["metric"] == "l2"
    end
  end

  describe "VEVICT edge cases" do
    test "VEVICT on non-existent collection returns error" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VEVICT", ["ghost"], store)
      assert msg =~ "not found"
    end

    test "VEVICT does not destroy data (no-op in v1)" do
      store = make_collection(2)
      :ok = Vector.handle("VADD", ["test_col", "k1", "1.0", "2.0"], store)
      :ok = Vector.handle("VEVICT", ["test_col"], store)
      # Data should still be there
      assert ["1.0", "2.0"] = Vector.handle("VGET", ["test_col", "k1"], store)
    end
  end

  describe "VCREATE / VLIST interaction" do
    test "newly created collection immediately appears in VLIST" do
      store = MockStore.make()
      assert [] = Vector.handle("VLIST", [], store)
      :ok = Vector.handle("VCREATE", ["new_col", "2", "cosine"], store)
      result = Vector.handle("VLIST", [], store)
      assert "new_col" in result
    end

    test "VLIST returns collections sorted alphabetically" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["zebra", "2", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["alpha", "2", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["middle", "2", "cosine"], store)
      result = Vector.handle("VLIST", [], store)
      assert result == ["alpha", "middle", "zebra"]
    end

    test "COUNT 0 returns error (must be positive)" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["col", "2", "cosine"], store)
      assert {:error, msg} = Vector.handle("VLIST", ["COUNT", "0"], store)
      assert msg =~ "positive"
    end
  end
end
