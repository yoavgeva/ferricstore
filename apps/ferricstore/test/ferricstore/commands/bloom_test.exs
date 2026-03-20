defmodule Ferricstore.Commands.BloomTest do
  @moduledoc """
  Comprehensive tests for the Bloom filter command handler.

  Covers BF.RESERVE, BF.ADD, BF.MADD, BF.EXISTS, BF.MEXISTS, BF.CARD,
  and BF.INFO with happy paths, error cases, edge cases, and accuracy
  verification.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # BF.RESERVE
  # ===========================================================================

  describe "BF.RESERVE" do
    test "creates a new bloom filter with specified error rate and capacity" do
      store = MockStore.make()
      assert :ok = Bloom.handle("BF.RESERVE", ["mybloom", "0.01", "1000"], store)
      assert store.exists?.("mybloom")
    end

    test "returns error when key already exists" do
      store = MockStore.make()
      assert :ok = Bloom.handle("BF.RESERVE", ["mybloom", "0.01", "100"], store)
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["mybloom", "0.01", "100"], store)
      assert msg =~ "item exists"
    end

    test "returns error with invalid error rate (zero)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0", "100"], store)
      assert msg =~ "error rate"
    end

    test "returns error with invalid error rate (one)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "1", "100"], store)
      assert msg =~ "error rate"
    end

    test "returns error with invalid error rate (negative)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "-0.5", "100"], store)
      assert msg =~ "error rate"
    end

    test "returns error with invalid error rate (greater than 1)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "1.5", "100"], store)
      assert msg =~ "error rate"
    end

    test "returns error with invalid capacity (zero)" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "0"], store)
    end

    test "returns error with invalid capacity (negative)" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "-10"], store)
    end

    test "returns error with non-numeric error rate" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "abc", "100"], store)
    end

    test "returns error with non-numeric capacity" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "abc"], store)
    end

    test "returns error with wrong number of arguments (too few)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with wrong number of arguments (too many)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "100", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "accepts integer error rate string like '0'" do
      store = MockStore.make()
      # "0" should be parsed as 0.0 which is invalid
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0", "100"], store)
    end

    test "creates filter with very small error rate" do
      store = MockStore.make()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf", "0.0001", "100"], store)
    end

    test "creates filter with very large capacity" do
      store = MockStore.make()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf", "0.01", "100000"], store)
    end
  end

  # ===========================================================================
  # BF.ADD
  # ===========================================================================

  describe "BF.ADD" do
    test "adds an element to a new filter (auto-creates)" do
      store = MockStore.make()
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
    end

    test "adds an element to an existing filter" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
    end

    test "returns 0 for duplicate element (already present)" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
      assert 0 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
    end

    test "multiple distinct elements all return 1" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)

      for i <- 1..10 do
        assert 1 = Bloom.handle("BF.ADD", ["bf", "elem_#{i}"], store),
               "Expected 1 for element elem_#{i}"
      end
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.ADD", ["bf"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.ADD", ["bf", "a", "b"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.ADD", [], store)
    end

    test "handles empty string element" do
      store = MockStore.make()
      assert 1 = Bloom.handle("BF.ADD", ["bf", ""], store)
    end

    test "handles binary element with special characters" do
      store = MockStore.make()
      assert 1 = Bloom.handle("BF.ADD", ["bf", <<0, 1, 2, 255>>], store)
    end
  end

  # ===========================================================================
  # BF.MADD
  # ===========================================================================

  describe "BF.MADD" do
    test "adds multiple elements at once" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      result = Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      assert result == [1, 1, 1]
    end

    test "returns 0 for elements already present" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      result = Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      assert result == [0, 1, 1]
    end

    test "auto-creates filter when key does not exist" do
      store = MockStore.make()
      result = Bloom.handle("BF.MADD", ["bf", "x", "y"], store)
      assert result == [1, 1]
      assert store.exists?.("bf")
    end

    test "handles single element" do
      store = MockStore.make()
      result = Bloom.handle("BF.MADD", ["bf", "single"], store)
      assert result == [1]
    end

    test "handles duplicates within the same MADD call" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      result = Bloom.handle("BF.MADD", ["bf", "dup", "dup"], store)
      # First add returns 1 (new), second returns 0 (already set)
      assert result == [1, 0]
    end

    test "returns error with only key argument (no elements)" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.MADD", ["bf"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.MADD", [], store)
    end
  end

  # ===========================================================================
  # BF.EXISTS
  # ===========================================================================

  describe "BF.EXISTS" do
    test "returns 1 for an element that was added" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf", "hello"], store)
    end

    test "returns 0 for an element that was not added (probably)" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.001", "1000"], store)
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      # Not a guarantee (false positives exist), but with low error rate
      # and few elements, this should be 0.
      assert 0 = Bloom.handle("BF.EXISTS", ["bf", "goodbye"], store)
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 = Bloom.handle("BF.EXISTS", ["nonexistent", "hello"], store)
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.EXISTS", ["bf"], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.EXISTS", ["bf", "a", "b"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.EXISTS", [], store)
    end
  end

  # ===========================================================================
  # BF.MEXISTS
  # ===========================================================================

  describe "BF.MEXISTS" do
    test "checks multiple elements at once" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.001", "1000"], store)
      Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      result = Bloom.handle("BF.MEXISTS", ["bf", "a", "b", "c", "d"], store)
      assert Enum.slice(result, 0, 3) == [1, 1, 1]
      # "d" was not added, should be 0 (with high probability)
      assert Enum.at(result, 3) == 0
    end

    test "returns all zeros for non-existent key" do
      store = MockStore.make()
      result = Bloom.handle("BF.MEXISTS", ["nonexistent", "a", "b"], store)
      assert result == [0, 0]
    end

    test "handles single element" do
      store = MockStore.make()
      Bloom.handle("BF.ADD", ["bf", "x"], store)
      result = Bloom.handle("BF.MEXISTS", ["bf", "x"], store)
      assert result == [1]
    end

    test "returns error with only key argument" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.MEXISTS", ["bf"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.MEXISTS", [], store)
    end
  end

  # ===========================================================================
  # BF.CARD
  # ===========================================================================

  describe "BF.CARD" do
    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 = Bloom.handle("BF.CARD", ["nonexistent"], store)
    end

    test "returns 0 for newly reserved filter" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 0 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "returns count after adding elements" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      Bloom.handle("BF.ADD", ["bf", "b"], store)
      Bloom.handle("BF.ADD", ["bf", "c"], store)
      assert 3 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "does not count duplicates" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      assert 1 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "counts MADD elements" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.MADD", ["bf", "x", "y", "z"], store)
      assert 3 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.CARD", [], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.CARD", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # BF.INFO
  # ===========================================================================

  describe "BF.INFO" do
    test "returns filter information" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "1000"], store)
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      result = Bloom.handle("BF.INFO", ["bf"], store)
      assert is_list(result)

      info = list_to_info_map(result)
      assert info["Capacity"] == 1000
      assert info["Size"] == 1
      assert info["Number of items inserted"] == 1
      assert info["Error rate"] == 0.01
      assert info["Number of hash functions"] > 0
      assert info["Number of bits"] > 0
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.INFO", ["nonexistent"], store)
      assert msg =~ "not found"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.INFO", [], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.INFO", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # Accuracy verification
  # ===========================================================================

  describe "false positive rate verification" do
    test "Bloom filter respects target false positive rate within 2x margin" do
      store = MockStore.make()
      error_rate = 0.05
      capacity = 1000

      Bloom.handle("BF.RESERVE", ["bf", "#{error_rate}", "#{capacity}"], store)

      # Add capacity elements.
      for i <- 1..capacity do
        Bloom.handle("BF.ADD", ["bf", "added_#{i}"], store)
      end

      # Check elements that were NOT added — count false positives.
      test_count = 10_000
      false_positives =
        Enum.count(1..test_count, fn i ->
          Bloom.handle("BF.EXISTS", ["bf", "not_added_#{i}"], store) == 1
        end)

      observed_rate = false_positives / test_count

      # Allow up to 2x the target error rate as tolerance for randomness.
      assert observed_rate < error_rate * 2,
             "False positive rate #{observed_rate} exceeds 2x target #{error_rate}"
    end

    test "no false negatives: all added elements are found" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "500"], store)

      elements = for i <- 1..200, do: "element_#{i}"
      Enum.each(elements, fn e -> Bloom.handle("BF.ADD", ["bf", e], store) end)

      # Every added element MUST be found (no false negatives).
      Enum.each(elements, fn e ->
        assert 1 == Bloom.handle("BF.EXISTS", ["bf", e], store),
               "False negative for #{e}"
      end)
    end
  end

  # ===========================================================================
  # Cross-command interactions
  # ===========================================================================

  describe "cross-command interactions" do
    test "BF.ADD then BF.EXISTS returns 1" do
      store = MockStore.make()
      Bloom.handle("BF.ADD", ["bf", "test"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf", "test"], store)
    end

    test "BF.MADD then BF.MEXISTS returns correct results" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.001", "1000"], store)
      Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      result = Bloom.handle("BF.MEXISTS", ["bf", "a", "c"], store)
      assert result == [1, 1]
    end

    test "BF.RESERVE then BF.ADD then BF.CARD tracks count" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 0 = Bloom.handle("BF.CARD", ["bf"], store)
      Bloom.handle("BF.ADD", ["bf", "x"], store)
      assert 1 = Bloom.handle("BF.CARD", ["bf"], store)
      Bloom.handle("BF.ADD", ["bf", "y"], store)
      assert 2 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "auto-created filter has default capacity and error rate" do
      store = MockStore.make()
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      result = Bloom.handle("BF.INFO", ["bf"], store)
      info = list_to_info_map(result)
      assert info["Capacity"] == 100
      assert info["Error rate"] == 0.01
    end

    test "multiple independent bloom filters do not interfere" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf1", "0.001", "1000"], store)
      Bloom.handle("BF.RESERVE", ["bf2", "0.001", "1000"], store)

      Bloom.handle("BF.ADD", ["bf1", "only_in_bf1"], store)
      Bloom.handle("BF.ADD", ["bf2", "only_in_bf2"], store)

      assert 1 = Bloom.handle("BF.EXISTS", ["bf1", "only_in_bf1"], store)
      assert 0 = Bloom.handle("BF.EXISTS", ["bf1", "only_in_bf2"], store)
      assert 0 = Bloom.handle("BF.EXISTS", ["bf2", "only_in_bf1"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf2", "only_in_bf2"], store)
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "dispatcher integration" do
    test "BF.ADD is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert 1 = Dispatcher.dispatch("BF.ADD", ["bf", "hello"], store)
    end

    test "BF.EXISTS is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("BF.ADD", ["bf", "hello"], store)
      assert 1 = Dispatcher.dispatch("BF.EXISTS", ["bf", "hello"], store)
    end

    test "bf.add lowercase is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert 1 = Dispatcher.dispatch("bf.add", ["bf", "hello"], store)
    end

    test "Bf.Reserve mixed case is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("Bf.Reserve", ["bf", "0.01", "100"], store)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Converts the flat alternating [key, value, key, value, ...] list
  # from BF.INFO into a map for easier assertions.
  defp list_to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
