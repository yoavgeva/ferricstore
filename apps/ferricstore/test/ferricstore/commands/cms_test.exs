defmodule Ferricstore.Commands.CMSTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.CMS
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # CMS.INITBYDIM
  # ===========================================================================

  describe "CMS.INITBYDIM" do
    test "creates a new sketch with given dimensions" do
      store = MockStore.make()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert store.exists?.("mysketch")
    end

    test "returns error when key already exists" do
      store = MockStore.make(%{"mysketch" => {{"existing", 0}, 0}})
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert msg =~ "already exists"
    end

    test "returns error with zero width" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "0", "7"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with negative depth" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "100", "-1"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with non-integer width" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "abc", "7"], store)
      assert msg =~ "not an integer"
    end

    test "returns error with non-integer depth" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "100", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "returns error with wrong number of arguments (too few)" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with wrong number of arguments (too many)" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "100", "7", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", [], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # CMS.INITBYPROB
  # ===========================================================================

  describe "CMS.INITBYPROB" do
    test "creates a sketch sized for error rate and confidence" do
      store = MockStore.make()
      assert :ok = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.001", "0.999"], store)
      assert store.exists?.("mysketch")
    end

    test "sketch dimensions match theoretical formulas" do
      store = MockStore.make()
      assert :ok = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.01", "0.99"], store)

      # Use CMS.INFO to inspect dimensions (sketch is now a NIF resource)
      ["width", width, "depth", depth, "count", 0] =
        CMS.handle("CMS.INFO", ["mysketch"], store)

      # width = ceil(e / error) = ceil(2.718 / 0.01) = 272
      expected_width = ceil(:math.exp(1) / 0.01)
      assert width == expected_width

      # depth = ceil(ln(1 / prob)) = ceil(ln(1/0.99)) = ceil(0.01005) = 1
      expected_depth = ceil(:math.log(1.0 / 0.99))
      assert depth == expected_depth
    end

    test "returns error when key already exists" do
      store = MockStore.make(%{"mysketch" => {{"existing", 0}, 0}})
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.01", "0.99"], store)
      assert msg =~ "already exists"
    end

    test "returns error with zero error rate" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0", "0.99"], store)
      assert msg =~ "positive number"
    end

    test "returns error with negative error rate" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "-0.01", "0.99"], store)
      assert msg =~ "positive number"
    end

    test "returns error with probability >= 1" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0.01", "1.0"], store)
      assert msg =~ "between 0 and 1"
    end

    test "returns error with probability <= 0" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0.01", "0.0"], store)
      assert msg =~ "between 0 and 1"
    end

    test "returns error with non-numeric error" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "abc", "0.99"], store)
      assert msg =~ "not a valid number"
    end

    test "returns error with non-numeric probability" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0.01", "abc"], store)
      assert msg =~ "not a valid number"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INITBYPROB", ["key"], store)
      assert {:error, _} = CMS.handle("CMS.INITBYPROB", [], store)
    end
  end

  # ===========================================================================
  # CMS.INCRBY
  # ===========================================================================

  describe "CMS.INCRBY" do
    test "increments a single element by 1" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [1] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "1"], store)
    end

    test "increments a single element by large count" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [42] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "42"], store)
    end

    test "increments multiple elements at once" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)

      result =
        CMS.handle("CMS.INCRBY", ["mysketch", "a", "5", "b", "10", "c", "3"], store)

      assert [5, 10, 3] = result
    end

    test "increments accumulate for the same element" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [1] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "1"], store)
      assert [2] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "1"], store)
      assert [12] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "10"], store)
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["missing", "elem1", "1"], store)
      assert msg =~ "does not exist"
    end

    test "returns error when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["str_key", "elem1", "1"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with odd number of element/count args" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, _} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1"], store)
    end

    test "returns error with zero count" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "0"], store)
      assert msg =~ "invalid count"
    end

    test "returns error with negative count" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "-1"], store)
      assert msg =~ "invalid count"
    end

    test "returns error with non-integer count" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "abc"], store)
      assert msg =~ "invalid count"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INCRBY", [], store)
    end

    test "returns error with only key argument" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, _} = CMS.handle("CMS.INCRBY", ["mysketch"], store)
    end
  end

  # ===========================================================================
  # CMS.QUERY
  # ===========================================================================

  describe "CMS.QUERY" do
    test "returns 0 for elements never inserted" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [0] = CMS.handle("CMS.QUERY", ["mysketch", "never_seen"], store)
    end

    test "returns correct count for inserted element" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "5"], store)
      assert [5] = CMS.handle("CMS.QUERY", ["mysketch", "elem1"], store)
    end

    test "queries multiple elements at once" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "3", "b", "7"], store)

      result = CMS.handle("CMS.QUERY", ["mysketch", "a", "b", "c"], store)
      assert [3, 7, 0] = result
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.QUERY", ["missing", "elem1"], store)
      assert msg =~ "does not exist"
    end

    test "returns error when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = CMS.handle("CMS.QUERY", ["str_key", "elem1"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with no element arguments" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.QUERY", ["key"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.QUERY", [], store)
    end
  end

  # ===========================================================================
  # CMS.INFO
  # ===========================================================================

  describe "CMS.INFO" do
    test "returns width, depth, and count for sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "200", "5"], store)

      assert ["width", 200, "depth", 5, "count", 0] =
               CMS.handle("CMS.INFO", ["mysketch"], store)
    end

    test "count reflects total insertions" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "3", "b", "7"], store)

      result = CMS.handle("CMS.INFO", ["mysketch"], store)
      assert ["width", 100, "depth", 7, "count", 10] = result
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INFO", ["missing"], store)
      assert msg =~ "does not exist"
    end

    test "returns error when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = CMS.handle("CMS.INFO", ["str_key"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INFO", [], store)
      assert {:error, _} = CMS.handle("CMS.INFO", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # CMS.MERGE
  # ===========================================================================

  describe "CMS.MERGE" do
    test "merges two source sketches into a new destination" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "3"], store)
      CMS.handle("CMS.INCRBY", ["src2", "a", "5"], store)
      CMS.handle("CMS.INCRBY", ["src2", "b", "2"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2"], store)

      # Merged sketch should have combined counts
      assert [8] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
      assert [2] = CMS.handle("CMS.QUERY", ["dst", "b"], store)
    end

    test "merges single source sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "50", "5"], store)
      CMS.handle("CMS.INCRBY", ["src1", "x", "10"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      assert [10] = CMS.handle("CMS.QUERY", ["dst", "x"], store)
    end

    test "merges with weights" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "4"], store)
      CMS.handle("CMS.INCRBY", ["src2", "a", "6"], store)

      # Weight src1 by 2 and src2 by 3
      assert :ok =
               CMS.handle(
                 "CMS.MERGE",
                 ["dst", "2", "src1", "src2", "WEIGHTS", "2", "3"],
                 store
               )

      # Expected: 4*2 + 6*3 = 8 + 18 = 26
      assert [26] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
    end

    test "merges into existing destination (additive)" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["dst", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["dst", "a", "10"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "5"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      # 10 + 5 = 15
      assert [15] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
    end

    test "returns error when source sketches have different dimensions" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "200", "7"], store)

      assert {:error, msg} = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2"], store)
      assert msg =~ "width/depth"
    end

    test "returns error when dst and src have different dimensions" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["dst", "200", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)

      assert {:error, msg} = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      assert msg =~ "width/depth"
    end

    test "returns error when source key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.MERGE", ["dst", "1", "missing"], store)
      assert msg =~ "does not exist"
    end

    test "returns error when numkeys is zero" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst", "0"], store)
    end

    test "returns error when numkeys is non-integer" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst", "abc", "src1"], store)
    end

    test "returns error with fewer source keys than numkeys" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst", "3", "src1"], store)
    end

    test "returns error with wrong number of weights" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)

      assert {:error, msg} =
               CMS.handle(
                 "CMS.MERGE",
                 ["dst", "2", "src1", "src2", "WEIGHTS", "1"],
                 store
               )

      assert msg =~ "wrong number of weights"
    end

    test "returns error with non-integer weight" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)

      assert {:error, msg} =
               CMS.handle("CMS.MERGE", ["dst", "1", "src1", "WEIGHTS", "abc"], store)

      assert msg =~ "invalid weight"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.MERGE", [], store)
    end

    test "returns error with only dst argument" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst"], store)
    end
  end

  # ===========================================================================
  # Accuracy tests
  # ===========================================================================

  describe "accuracy" do
    test "never undercounts (CMS invariant)" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "7"], store)

      # Insert elements with known counts
      true_counts = %{
        "apple" => 100,
        "banana" => 50,
        "cherry" => 200,
        "date" => 10,
        "elderberry" => 75
      }

      for {element, count} <- true_counts do
        CMS.handle("CMS.INCRBY", ["mysketch", element, Integer.to_string(count)], store)
      end

      # Verify: estimated count >= true count (never undercounts)
      for {element, true_count} <- true_counts do
        [estimated] = CMS.handle("CMS.QUERY", ["mysketch", element], store)
        assert estimated >= true_count, "#{element}: estimated #{estimated} < true #{true_count}"
      end
    end

    test "estimates are reasonably close for large sketch" do
      store = MockStore.make()
      # Large dimensions should have low error
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "2000", "7"], store)

      # Insert many different elements
      for i <- 1..100 do
        CMS.handle("CMS.INCRBY", ["mysketch", "elem_#{i}", "#{i}"], store)
      end

      # Check that estimates for a few elements are close to true counts
      for i <- [1, 25, 50, 75, 100] do
        [estimated] = CMS.handle("CMS.QUERY", ["mysketch", "elem_#{i}"], store)
        # Should not overcount by more than 20% of total insertions / width
        # Total insertions = 1+2+...+100 = 5050
        # Max overcount per row = 5050 / 2000 = 2.525
        assert estimated >= i
        # Allow generous margin for test stability
        assert estimated <= i + 50,
               "elem_#{i}: estimated #{estimated} too far from true count #{i}"
      end
    end

    test "small sketch has higher error than large sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["small", "10", "3"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["large", "1000", "7"], store)

      # Insert same data into both
      for i <- 1..50 do
        count = Integer.to_string(i)
        CMS.handle("CMS.INCRBY", ["small", "elem_#{i}", count], store)
        CMS.handle("CMS.INCRBY", ["large", "elem_#{i}", count], store)
      end

      # Query same element from both
      [small_est] = CMS.handle("CMS.QUERY", ["small", "elem_1"], store)
      [large_est] = CMS.handle("CMS.QUERY", ["large", "elem_1"], store)

      # Large sketch should give a tighter estimate (closer to true count of 1)
      assert large_est <= small_est
    end

    test "total_count tracks cumulative insertions" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)

      CMS.handle("CMS.INCRBY", ["mysketch", "a", "5"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "b", "3"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "2"], store)

      ["width", _, "depth", _, "count", total] = CMS.handle("CMS.INFO", ["mysketch"], store)
      assert total == 10
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "edge cases" do
    test "width=1 sketch (degenerate -- all elements hash to same bucket)" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1", "1"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "5"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "b", "3"], store)

      # With width=1, all elements collide. Any query returns total count.
      [est_a] = CMS.handle("CMS.QUERY", ["mysketch", "a"], store)
      [est_b] = CMS.handle("CMS.QUERY", ["mysketch", "b"], store)
      assert est_a == 8
      assert est_b == 8
    end

    test "depth=1 sketch works" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "1"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "10"], store)
      [est] = CMS.handle("CMS.QUERY", ["mysketch", "a"], store)
      assert est >= 10
    end

    test "element with empty string" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "", "5"], store)
      assert [5] = CMS.handle("CMS.QUERY", ["mysketch", ""], store)
    end

    test "element with special characters" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "hello world!", "3"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "key:with:colons", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", <<0, 1, 2, 3>>, "2"], store)

      [est1] = CMS.handle("CMS.QUERY", ["mysketch", "hello world!"], store)
      [est2] = CMS.handle("CMS.QUERY", ["mysketch", "key:with:colons"], store)
      [est3] = CMS.handle("CMS.QUERY", ["mysketch", <<0, 1, 2, 3>>], store)

      assert est1 >= 3
      assert est2 >= 7
      assert est3 >= 2
    end

    test "large increment values" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "big", "1000000"], store)
      [est] = CMS.handle("CMS.QUERY", ["mysketch", "big"], store)
      assert est >= 1_000_000
    end

    test "many elements in single INCRBY call" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "7"], store)

      args =
        Enum.flat_map(1..100, fn i ->
          ["elem_#{i}", Integer.to_string(i)]
        end)

      result = CMS.handle("CMS.INCRBY", ["mysketch" | args], store)
      assert length(result) == 100

      # First result should be for elem_1 with count 1
      assert hd(result) >= 1
    end

    test "query for many elements at once" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "7"], store)

      # Insert 50 elements
      for i <- 1..50 do
        CMS.handle("CMS.INCRBY", ["mysketch", "e_#{i}", "#{i}"], store)
      end

      # Query all 50 at once
      elements = Enum.map(1..50, &"e_#{&1}")
      result = CMS.handle("CMS.QUERY", ["mysketch" | elements], store)
      assert length(result) == 50

      # Verify each estimate
      Enum.zip(1..50, result)
      |> Enum.each(fn {true_count, estimated} ->
        assert estimated >= true_count
      end)
    end

    test "INITBYPROB with very small error rate creates large sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.0001", "0.99"], store)
      ["width", width, "depth", _, "count", _] = CMS.handle("CMS.INFO", ["mysketch"], store)
      # width = ceil(e / 0.0001) = ceil(27183) = 27183
      assert width > 10_000
    end

    test "INITBYPROB with very small probability creates deep sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.01", "0.0001"], store)
      ["width", _, "depth", depth, "count", _] = CMS.handle("CMS.INFO", ["mysketch"], store)
      # depth = ceil(ln(1/0.0001)) = ceil(ln(10000)) = ceil(9.21) = 10
      assert depth >= 9
    end

    test "merge with all zero weights creates empty sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "100"], store)

      assert :ok =
               CMS.handle("CMS.MERGE", ["dst", "1", "src1", "WEIGHTS", "0"], store)

      # Everything weighted by 0 -- counts should be 0
      assert [0] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "dispatcher integration" do
    test "CMS commands route through the dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [1] = Dispatcher.dispatch("CMS.INCRBY", ["mysketch", "a", "1"], store)
      assert [1] = Dispatcher.dispatch("CMS.QUERY", ["mysketch", "a"], store)
      assert ["width", 100, "depth", 7, "count", 1] = Dispatcher.dispatch("CMS.INFO", ["mysketch"], store)
    end

    test "CMS commands are case-insensitive via dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("cms.initbydim", ["mysketch", "100", "7"], store)
      assert [1] = Dispatcher.dispatch("cms.incrby", ["mysketch", "a", "1"], store)
      assert [1] = Dispatcher.dispatch("cms.query", ["mysketch", "a"], store)
    end
  end

  # ===========================================================================
  # NIF resource lifecycle
  # ===========================================================================

  describe "NIF resource lifecycle" do
    test "NIF resource survives multiple operations" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      for i <- 1..50 do
        CMS.handle("CMS.INCRBY", ["mysketch", "elem_#{i}", "#{i}"], store)
      end

      for i <- 1..50 do
        [est] = CMS.handle("CMS.QUERY", ["mysketch", "elem_#{i}"], store)
        assert est >= i, "elem_#{i}: expected >= #{i}, got #{est}"
      end

      ["width", 100, "depth", 7, "count", total] = CMS.handle("CMS.INFO", ["mysketch"], store)
      assert total == Enum.sum(1..50)
    end

    test "multiple independent NIF sketches do not interfere" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sketch_a", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["sketch_b", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["sketch_a", "x", "10"], store)
      CMS.handle("CMS.INCRBY", ["sketch_b", "x", "20"], store)
      [a_est] = CMS.handle("CMS.QUERY", ["sketch_a", "x"], store)
      [b_est] = CMS.handle("CMS.QUERY", ["sketch_b", "x"], store)
      assert a_est >= 10
      assert b_est >= 20
      assert a_est < b_est
    end
  end

  # ===========================================================================
  # Large sketch (width=10000, depth=10)
  # ===========================================================================

  describe "large sketch" do
    test "width=10000 depth=10 sketch handles many elements" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["big", "10000", "10"], store)
      for i <- 1..5000, do: CMS.handle("CMS.INCRBY", ["big", "key_#{i}", "1"], store)
      ["width", 10_000, "depth", 10, "count", 5000] = CMS.handle("CMS.INFO", ["big"], store)
      for i <- [1, 1000, 2500, 5000] do
        [est] = CMS.handle("CMS.QUERY", ["big", "key_#{i}"], store)
        assert est >= 1
      end
    end
  end

  # ===========================================================================
  # Merge correctness
  # ===========================================================================

  describe "merge correctness (NIF)" do
    test "merge of three sources with different weights" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["s1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["s2", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["s3", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["s1", "a", "10"], store)
      CMS.handle("CMS.INCRBY", ["s2", "a", "20"], store)
      CMS.handle("CMS.INCRBY", ["s3", "a", "30"], store)
      assert :ok =
        CMS.handle("CMS.MERGE", ["dst", "3", "s1", "s2", "s3", "WEIGHTS", "1", "2", "3"], store)
      [est] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
      assert est == 140
    end

    test "merge preserves counts for distinct elements" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["s1", "200", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["s2", "200", "7"], store)
      CMS.handle("CMS.INCRBY", ["s1", "only_in_s1", "5"], store)
      CMS.handle("CMS.INCRBY", ["s2", "only_in_s2", "8"], store)
      assert :ok = CMS.handle("CMS.MERGE", ["dst", "2", "s1", "s2"], store)
      [est1] = CMS.handle("CMS.QUERY", ["dst", "only_in_s1"], store)
      [est2] = CMS.handle("CMS.QUERY", ["dst", "only_in_s2"], store)
      assert est1 >= 5
      assert est2 >= 8
    end
  end

  # ===========================================================================
  # Concurrent queries
  # ===========================================================================

  describe "concurrent queries" do
    test "parallel queries return consistent results" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "elem", "42"], store)
      tasks = for _ <- 1..100, do: Task.async(fn ->
        CMS.handle("CMS.QUERY", ["mysketch", "elem"], store)
      end)
      results = Task.await_many(tasks, 5000)
      for [est] <- results, do: assert(est >= 42)
    end

    test "parallel increments and queries are safe" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "7"], store)
      incr_tasks = for _ <- 1..50, do: Task.async(fn ->
        CMS.handle("CMS.INCRBY", ["mysketch", "shared_elem", "1"], store)
      end)
      query_tasks = for _ <- 1..50, do: Task.async(fn ->
        CMS.handle("CMS.QUERY", ["mysketch", "shared_elem"], store)
      end)
      Task.await_many(incr_tasks, 5000)
      query_results = Task.await_many(query_tasks, 5000)
      for [est] <- query_results, do: assert(est >= 0)
      [final] = CMS.handle("CMS.QUERY", ["mysketch", "shared_elem"], store)
      assert final >= 50
    end
  end
end
