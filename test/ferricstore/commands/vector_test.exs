defmodule Ferricstore.Commands.VectorTest do
  @moduledoc """
  Comprehensive tests for the vector search command handler.

  Covers VCREATE, VADD, VGET, VDEL, VSEARCH, VINFO, VLIST, and VEVICT
  with happy paths, error cases, edge cases, and distance metric verification.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Vector
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # VCREATE
  # ===========================================================================

  describe "VCREATE" do
    test "creates a collection with cosine metric and default HNSW params" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["embeddings", "128", "cosine"], store)
    end

    test "creates a collection with l2 metric" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "3", "l2"], store)
    end

    test "creates a collection with inner_product metric" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "4", "inner_product"], store)
    end

    test "creates a collection with custom M parameter" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine", "M", "32"], store)
    end

    test "creates a collection with custom EF parameter" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine", "EF", "200"], store)
    end

    test "creates a collection with both M and EF parameters" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "3", "l2", "M", "32", "EF", "200"], store)
    end

    test "returns error when collection already exists" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      assert msg =~ "already exists"
    end

    test "returns error with invalid dimension (zero)" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "0", "cosine"], store)
      assert msg =~ "dimension"
    end

    test "returns error with invalid dimension (negative)" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "-3", "cosine"], store)
      assert msg =~ "dimension"
    end

    test "returns error with non-numeric dimension" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "abc", "cosine"], store)
      assert msg =~ "dimension"
    end

    test "returns error with invalid metric" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "3", "hamming"], store)
      assert msg =~ "metric"
    end

    test "returns error with invalid M value (zero)" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "3", "cosine", "M", "0"], store)
      assert msg =~ "M"
    end

    test "returns error with invalid EF value (zero)" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "3", "cosine", "EF", "0"], store)
      assert msg =~ "EF"
    end

    test "returns error with too few arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "3"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with unknown option keyword" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VCREATE", ["vecs", "3", "cosine", "FOO", "10"], store)
      assert msg =~ "syntax"
    end

    test "metric name is case insensitive" do
      store = MockStore.make()
      assert :ok = Vector.handle("VCREATE", ["vecs", "3", "COSINE"], store)
    end
  end

  # ===========================================================================
  # VADD
  # ===========================================================================

  describe "VADD" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      %{store: store}
    end

    test "adds a vector to an existing collection", %{store: store} do
      assert :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "2.0", "3.0"], store)
    end

    test "overwrites an existing vector", %{store: store} do
      assert :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "2.0", "3.0"], store)
      assert :ok = Vector.handle("VADD", ["vecs", "k1", "4.0", "5.0", "6.0"], store)
      result = Vector.handle("VGET", ["vecs", "k1"], store)
      assert result == ["4.0", "5.0", "6.0"]
    end

    test "returns error when collection does not exist", %{store: store} do
      assert {:error, msg} = Vector.handle("VADD", ["nonexistent", "k1", "1.0", "2.0", "3.0"], store)
      assert msg =~ "not found" or msg =~ "does not exist"
    end

    test "returns error when vector dimension does not match collection", %{store: store} do
      assert {:error, msg} = Vector.handle("VADD", ["vecs", "k1", "1.0", "2.0"], store)
      assert msg =~ "dimension"
    end

    test "returns error when vector has too many components", %{store: store} do
      assert {:error, msg} = Vector.handle("VADD", ["vecs", "k1", "1.0", "2.0", "3.0", "4.0"], store)
      assert msg =~ "dimension"
    end

    test "returns error when vector component is not a number", %{store: store} do
      assert {:error, msg} = Vector.handle("VADD", ["vecs", "k1", "1.0", "abc", "3.0"], store)
      assert msg =~ "not a valid float"
    end

    test "accepts integer-format components", %{store: store} do
      assert :ok = Vector.handle("VADD", ["vecs", "k1", "1", "2", "3"], store)
      result = Vector.handle("VGET", ["vecs", "k1"], store)
      assert result == ["1.0", "2.0", "3.0"]
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VADD", ["vecs"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VADD", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "handles negative float components", %{store: store} do
      assert :ok = Vector.handle("VADD", ["vecs", "k1", "-1.5", "0.0", "3.14"], store)
      result = Vector.handle("VGET", ["vecs", "k1"], store)
      assert result == ["-1.5", "0.0", "3.14"]
    end
  end

  # ===========================================================================
  # VGET
  # ===========================================================================

  describe "VGET" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "2.0", "3.0"], store)
      %{store: store}
    end

    test "returns vector components as string list", %{store: store} do
      result = Vector.handle("VGET", ["vecs", "k1"], store)
      assert result == ["1.0", "2.0", "3.0"]
    end

    test "returns nil for non-existent key", %{store: store} do
      result = Vector.handle("VGET", ["vecs", "missing"], store)
      assert result == nil
    end

    test "returns error when collection does not exist", %{store: store} do
      assert {:error, msg} = Vector.handle("VGET", ["nonexistent", "k1"], store)
      assert msg =~ "not found" or msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VGET", ["vecs"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VGET", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VGET", ["vecs", "k1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # VDEL
  # ===========================================================================

  describe "VDEL" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "2.0", "3.0"], store)
      %{store: store}
    end

    test "deletes an existing vector and returns 1", %{store: store} do
      assert 1 = Vector.handle("VDEL", ["vecs", "k1"], store)
      assert nil == Vector.handle("VGET", ["vecs", "k1"], store)
    end

    test "returns 0 when key does not exist in collection", %{store: store} do
      assert 0 = Vector.handle("VDEL", ["vecs", "missing"], store)
    end

    test "returns error when collection does not exist", %{store: store} do
      assert {:error, msg} = Vector.handle("VDEL", ["nonexistent", "k1"], store)
      assert msg =~ "not found" or msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VDEL", ["vecs"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VDEL", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "deleted vector does not appear in VSEARCH results", %{store: store} do
      :ok = Vector.handle("VADD", ["vecs", "k2", "1.0", "0.0", "0.0"], store)
      Vector.handle("VDEL", ["vecs", "k1"], store)
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "10"], store)
      keys = extract_keys(result)
      refute "k1" in keys
      assert "k2" in keys
    end
  end

  # ===========================================================================
  # VSEARCH
  # ===========================================================================

  describe "VSEARCH with cosine metric" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      # Orthogonal-ish vectors for clear ranking
      :ok = Vector.handle("VADD", ["vecs", "same_dir", "1.0", "2.0", "3.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "opposite", "-1.0", "-2.0", "-3.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "ortho", "0.0", "0.0", "1.0"], store)
      %{store: store}
    end

    test "returns nearest neighbors sorted by distance (cosine)", %{store: store} do
      # Query is exactly [1.0, 2.0, 3.0], so same_dir should be closest
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "3"], store)
      keys = extract_keys(result)
      assert hd(keys) == "same_dir"
    end

    test "returns at most k results", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "2"], store)
      keys = extract_keys(result)
      assert length(keys) == 2
    end

    test "returns fewer than k when collection has fewer vectors", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "100"], store)
      keys = extract_keys(result)
      assert length(keys) == 3
    end

    test "cosine distance of identical vectors is 0", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "1"], store)
      [_key, dist_str | _] = result
      dist = String.to_float(dist_str)
      assert_in_delta dist, 0.0, 1.0e-6
    end

    test "cosine distance of opposite vectors is close to 2.0", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "3"], store)
      # Find the opposite vector's distance
      pairs = Enum.chunk_every(result, 2)
      opposite_pair = Enum.find(pairs, fn [k, _d] -> k == "opposite" end)
      assert opposite_pair != nil
      [_, dist_str] = opposite_pair
      dist = String.to_float(dist_str)
      assert_in_delta dist, 2.0, 1.0e-6
    end
  end

  describe "VSEARCH with l2 metric" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "l2"], store)
      :ok = Vector.handle("VADD", ["vecs", "origin", "0.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "near", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "far", "10.0", "0.0"], store)
      %{store: store}
    end

    test "returns nearest neighbors sorted by l2 distance", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "0.0", "0.0", "TOP", "3"], store)
      keys = extract_keys(result)
      assert keys == ["origin", "near", "far"]
    end

    test "l2 distance to self is 0", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "0.0", "0.0", "TOP", "1"], store)
      [_key, dist_str] = result
      dist = String.to_float(dist_str)
      assert_in_delta dist, 0.0, 1.0e-6
    end

    test "l2 distance is squared euclidean", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "0.0", "0.0", "TOP", "3"], store)
      pairs = Enum.chunk_every(result, 2)
      far_pair = Enum.find(pairs, fn [k, _d] -> k == "far" end)
      [_, dist_str] = far_pair
      dist = String.to_float(dist_str)
      # L2 (squared Euclidean) distance from origin to [10,0] = 100.0
      assert_in_delta dist, 100.0, 1.0e-6
    end
  end

  describe "VSEARCH with inner_product metric" do
    setup do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "inner_product"], store)
      # Inner product: higher is more similar, so distance = 1 - dot(a,b)
      :ok = Vector.handle("VADD", ["vecs", "high_dot", "10.0", "10.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "low_dot", "0.1", "0.1"], store)
      :ok = Vector.handle("VADD", ["vecs", "neg_dot", "-5.0", "-5.0"], store)
      %{store: store}
    end

    test "returns nearest neighbors sorted by inner product distance (1 - dot)", %{store: store} do
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "1.0", "TOP", "3"], store)
      keys = extract_keys(result)
      # high_dot has highest dot product (20), so lowest distance
      assert hd(keys) == "high_dot"
      # neg_dot has lowest dot product (-10), so highest distance
      assert List.last(keys) == "neg_dot"
    end
  end

  describe "VSEARCH edge cases" do
    test "returns empty list when collection has no vectors" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "3.0", "TOP", "5"], store)
      assert result == []
    end

    test "returns error when collection does not exist" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VSEARCH", ["nonexistent", "1.0", "TOP", "5"], store)
      assert msg =~ "not found" or msg =~ "does not exist"
    end

    test "returns error with wrong vector dimension" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      assert {:error, msg} = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "TOP", "5"], store)
      assert msg =~ "dimension"
    end

    test "returns error with invalid k value (zero)" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)
      assert {:error, msg} = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "TOP", "0"], store)
      assert msg =~ "positive"
    end

    test "returns error with non-numeric k value" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)
      assert {:error, msg} = Vector.handle("VSEARCH", ["vecs", "1.0", "2.0", "TOP", "abc"], store)
      assert msg =~ "integer"
    end

    test "accepts optional EF parameter" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)
      :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "0.0"], store)
      result = Vector.handle("VSEARCH", ["vecs", "1.0", "0.0", "TOP", "5", "EF", "200"], store)
      keys = extract_keys(result)
      assert "k1" in keys
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VSEARCH", ["vecs"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VSEARCH", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "handles zero vector in cosine (returns distance as 1.0 or NaN-safe)" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)
      :ok = Vector.handle("VADD", ["vecs", "zero", "0.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "nonzero", "1.0", "0.0"], store)
      # Searching with a zero vector -- should not crash
      result = Vector.handle("VSEARCH", ["vecs", "0.0", "0.0", "TOP", "5"], store)
      assert is_list(result)
    end
  end

  # ===========================================================================
  # VINFO
  # ===========================================================================

  describe "VINFO" do
    test "returns collection metadata" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "128", "cosine", "M", "32", "EF", "200"], store)
      vec_components = Enum.map(1..128, fn _ -> "1.0" end)
      :ok = Vector.handle("VADD", ["vecs", "k1" | vec_components], store)

      result = Vector.handle("VINFO", ["vecs"], store)
      info = list_to_info_map(result)
      assert info["collection"] == "vecs"
      assert info["dims"] == 128
      assert info["metric"] == "cosine"
      assert info["m"] == 32
      assert info["ef"] == 200
      assert info["vector_count"] >= 0
    end

    test "returns correct vector count after adds and deletes" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)
      :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "k2", "0.0", "1.0"], store)

      result = Vector.handle("VINFO", ["vecs"], store)
      info = list_to_info_map(result)
      assert info["vector_count"] == 2

      Vector.handle("VDEL", ["vecs", "k1"], store)
      result2 = Vector.handle("VINFO", ["vecs"], store)
      info2 = list_to_info_map(result2)
      assert info2["vector_count"] == 1
    end

    test "returns error for non-existent collection" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VINFO", ["nonexistent"], store)
      assert msg =~ "not found" or msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VINFO", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VINFO", ["a", "b"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # VLIST
  # ===========================================================================

  describe "VLIST" do
    test "returns empty list when no collections exist" do
      store = MockStore.make()
      result = Vector.handle("VLIST", [], store)
      assert result == []
    end

    test "lists all collections" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["alpha", "3", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["beta", "4", "l2"], store)
      result = Vector.handle("VLIST", [], store)
      assert is_list(result)
      assert "alpha" in result
      assert "beta" in result
    end

    test "MATCH pattern filters collections by glob" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["embeddings_v1", "3", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["embeddings_v2", "3", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["images", "3", "l2"], store)

      result = Vector.handle("VLIST", ["MATCH", "embeddings*"], store)
      assert length(result) == 2
      assert "embeddings_v1" in result
      assert "embeddings_v2" in result
      refute "images" in result
    end

    test "COUNT limits number of returned collections" do
      store = MockStore.make()
      for i <- 1..5 do
        :ok = Vector.handle("VCREATE", ["col_#{i}", "3", "cosine"], store)
      end

      result = Vector.handle("VLIST", ["COUNT", "2"], store)
      assert length(result) == 2
    end

    test "MATCH and COUNT can be combined" do
      store = MockStore.make()
      for i <- 1..5 do
        :ok = Vector.handle("VCREATE", ["test_#{i}", "3", "cosine"], store)
      end
      :ok = Vector.handle("VCREATE", ["other", "3", "l2"], store)

      result = Vector.handle("VLIST", ["MATCH", "test_*", "COUNT", "3"], store)
      assert length(result) == 3
      Enum.each(result, fn name -> assert String.starts_with?(name, "test_") end)
    end

    test "returns error with invalid COUNT value" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VLIST", ["COUNT", "abc"], store)
      assert msg =~ "integer"
    end

    test "returns error with unknown option" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VLIST", ["FOO", "bar"], store)
      assert msg =~ "syntax"
    end
  end

  # ===========================================================================
  # VEVICT
  # ===========================================================================

  describe "VEVICT" do
    test "returns OK for existing collection (no-op in v1)" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "cosine"], store)
      assert :ok = Vector.handle("VEVICT", ["vecs"], store)
    end

    test "returns error for non-existent collection" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VEVICT", ["nonexistent"], store)
      assert msg =~ "not found" or msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VEVICT", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, msg} = Vector.handle("VEVICT", ["a", "b"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # Distance metric correctness
  # ===========================================================================

  describe "distance metric correctness" do
    test "cosine distance between unit vectors at known angles" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)
      # Unit vector along x-axis
      :ok = Vector.handle("VADD", ["vecs", "x_axis", "1.0", "0.0"], store)
      # Unit vector along y-axis (orthogonal = cosine dist 1.0)
      :ok = Vector.handle("VADD", ["vecs", "y_axis", "0.0", "1.0"], store)

      result = Vector.handle("VSEARCH", ["vecs", "1.0", "0.0", "TOP", "2"], store)
      pairs = Enum.chunk_every(result, 2)

      x_pair = Enum.find(pairs, fn [k, _] -> k == "x_axis" end)
      y_pair = Enum.find(pairs, fn [k, _] -> k == "y_axis" end)

      [_, x_dist_str] = x_pair
      [_, y_dist_str] = y_pair

      assert_in_delta String.to_float(x_dist_str), 0.0, 1.0e-6
      assert_in_delta String.to_float(y_dist_str), 1.0, 1.0e-6
    end

    test "l2 squared distance is correct for known vectors" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "3", "l2"], store)
      :ok = Vector.handle("VADD", ["vecs", "point", "3.0", "4.0", "0.0"], store)

      result = Vector.handle("VSEARCH", ["vecs", "0.0", "0.0", "0.0", "TOP", "1"], store)
      [_, dist_str] = result
      # Squared L2: 3^2 + 4^2 + 0^2 = 25.0
      assert_in_delta String.to_float(dist_str), 25.0, 1.0e-6
    end

    test "inner_product distance is 1 - dot(a, b)" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "inner_product"], store)
      :ok = Vector.handle("VADD", ["vecs", "v1", "2.0", "3.0"], store)

      result = Vector.handle("VSEARCH", ["vecs", "4.0", "5.0", "TOP", "1"], store)
      [_, dist_str] = result
      # dot(2,3 . 4,5) = 8 + 15 = 23; distance = 1 - 23 = -22
      assert_in_delta String.to_float(dist_str), -22.0, 1.0e-6
    end
  end

  # ===========================================================================
  # Cross-command interactions
  # ===========================================================================

  describe "cross-command interactions" do
    test "VADD then VGET roundtrip preserves vector" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "4", "l2"], store)
      :ok = Vector.handle("VADD", ["vecs", "mykey", "1.5", "-2.5", "3.0", "0.001"], store)
      result = Vector.handle("VGET", ["vecs", "mykey"], store)
      assert result == ["1.5", "-2.5", "3.0", "0.001"]
    end

    test "VDEL removes vector from VSEARCH results" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "l2"], store)
      :ok = Vector.handle("VADD", ["vecs", "a", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["vecs", "b", "0.0", "1.0"], store)
      1 = Vector.handle("VDEL", ["vecs", "a"], store)

      result = Vector.handle("VSEARCH", ["vecs", "1.0", "0.0", "TOP", "10"], store)
      keys = extract_keys(result)
      assert keys == ["b"]
    end

    test "multiple collections are independent" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["col_a", "2", "cosine"], store)
      :ok = Vector.handle("VCREATE", ["col_b", "3", "l2"], store)

      :ok = Vector.handle("VADD", ["col_a", "k1", "1.0", "0.0"], store)
      :ok = Vector.handle("VADD", ["col_b", "k1", "1.0", "2.0", "3.0"], store)

      result_a = Vector.handle("VGET", ["col_a", "k1"], store)
      result_b = Vector.handle("VGET", ["col_b", "k1"], store)

      assert result_a == ["1.0", "0.0"]
      assert result_b == ["1.0", "2.0", "3.0"]
    end

    test "VINFO vector count updates correctly through add/overwrite/delete cycle" do
      store = MockStore.make()
      :ok = Vector.handle("VCREATE", ["vecs", "2", "cosine"], store)

      info = list_to_info_map(Vector.handle("VINFO", ["vecs"], store))
      assert info["vector_count"] == 0

      :ok = Vector.handle("VADD", ["vecs", "k1", "1.0", "0.0"], store)
      info = list_to_info_map(Vector.handle("VINFO", ["vecs"], store))
      assert info["vector_count"] == 1

      # Overwrite k1 -- count stays 1
      :ok = Vector.handle("VADD", ["vecs", "k1", "0.0", "1.0"], store)
      info = list_to_info_map(Vector.handle("VINFO", ["vecs"], store))
      assert info["vector_count"] == 1

      :ok = Vector.handle("VADD", ["vecs", "k2", "1.0", "1.0"], store)
      info = list_to_info_map(Vector.handle("VINFO", ["vecs"], store))
      assert info["vector_count"] == 2

      Vector.handle("VDEL", ["vecs", "k1"], store)
      info = list_to_info_map(Vector.handle("VINFO", ["vecs"], store))
      assert info["vector_count"] == 1
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "dispatcher integration" do
    test "VCREATE is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("VCREATE", ["vecs", "3", "cosine"], store)
    end

    test "VADD is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("VCREATE", ["vecs", "3", "cosine"], store)
      assert :ok = Dispatcher.dispatch("VADD", ["vecs", "k1", "1.0", "2.0", "3.0"], store)
    end

    test "VSEARCH is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("VCREATE", ["vecs", "2", "cosine"], store)
      Dispatcher.dispatch("VADD", ["vecs", "k1", "1.0", "0.0"], store)
      result = Dispatcher.dispatch("VSEARCH", ["vecs", "1.0", "0.0", "TOP", "5"], store)
      assert is_list(result)
    end

    test "vcreate lowercase is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("vcreate", ["vecs", "3", "cosine"], store)
    end

    test "VGET is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("VCREATE", ["vecs", "2", "l2"], store)
      Dispatcher.dispatch("VADD", ["vecs", "k1", "1.0", "2.0"], store)
      result = Dispatcher.dispatch("VGET", ["vecs", "k1"], store)
      assert result == ["1.0", "2.0"]
    end

    test "VDEL is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("VCREATE", ["vecs", "2", "l2"], store)
      Dispatcher.dispatch("VADD", ["vecs", "k1", "1.0", "2.0"], store)
      assert 1 = Dispatcher.dispatch("VDEL", ["vecs", "k1"], store)
    end

    test "VINFO is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("VCREATE", ["vecs", "2", "cosine"], store)
      result = Dispatcher.dispatch("VINFO", ["vecs"], store)
      assert is_list(result)
    end

    test "VLIST is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      result = Dispatcher.dispatch("VLIST", [], store)
      assert is_list(result)
    end

    test "VEVICT is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("VCREATE", ["vecs", "2", "cosine"], store)
      assert :ok = Dispatcher.dispatch("VEVICT", ["vecs"], store)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Extracts keys from the flat [key, dist, key, dist, ...] VSEARCH result.
  defp extract_keys(result) do
    result
    |> Enum.chunk_every(2)
    |> Enum.map(fn [k, _d] -> k end)
  end

  # Converts the flat alternating [key, value, key, value, ...] list
  # from VINFO into a map for easier assertions.
  defp list_to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
