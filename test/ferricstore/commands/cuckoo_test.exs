defmodule Ferricstore.Commands.CuckooTest do
  @moduledoc """
  Comprehensive tests for the Cuckoo filter command handler.

  Covers CF.RESERVE, CF.ADD, CF.ADDNX, CF.DEL, CF.EXISTS, CF.MEXISTS,
  CF.COUNT, and CF.INFO with happy paths, error cases, edge cases, and
  accuracy verification.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Cuckoo
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # CF.RESERVE
  # ===========================================================================

  describe "CF.RESERVE" do
    test "creates a new cuckoo filter with specified capacity" do
      store = MockStore.make()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["mycf", "1024"], store)
      assert store.exists?.("mycf")
    end

    test "returns error when key already exists" do
      store = MockStore.make()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["mycf", "1024"], store)
      assert {:error, msg} = Cuckoo.handle("CF.RESERVE", ["mycf", "1024"], store)
      assert msg =~ "item exists"
    end

    test "returns error with invalid capacity (zero)" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.RESERVE", ["cf", "0"], store)
    end

    test "returns error with invalid capacity (negative)" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.RESERVE", ["cf", "-10"], store)
    end

    test "returns error with non-numeric capacity" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.RESERVE", ["cf", "abc"], store)
    end

    test "returns error with wrong number of arguments (too few)" do
      store = MockStore.make()
      assert {:error, msg} = Cuckoo.handle("CF.RESERVE", ["cf"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with wrong number of arguments (too many)" do
      store = MockStore.make()
      assert {:error, msg} = Cuckoo.handle("CF.RESERVE", ["cf", "1024", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.RESERVE", [], store)
    end

    test "creates filter with small capacity" do
      store = MockStore.make()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf", "1"], store)
    end

    test "creates filter with large capacity" do
      store = MockStore.make()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf", "10000"], store)
    end
  end

  # ===========================================================================
  # CF.ADD
  # ===========================================================================

  describe "CF.ADD" do
    test "adds an element to a new filter (auto-creates)" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
    end

    test "adds an element to an existing filter" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
    end

    test "allows adding the same element twice" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      # Cuckoo filters allow duplicate fingerprints
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
    end

    test "multiple distinct elements" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)

      for i <- 1..10 do
        assert 1 = Cuckoo.handle("CF.ADD", ["cf", "elem_#{i}"], store),
               "Expected 1 for element elem_#{i}"
      end
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = Cuckoo.handle("CF.ADD", ["cf"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, msg} = Cuckoo.handle("CF.ADD", ["cf", "a", "b"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.ADD", [], store)
    end

    test "handles empty string element" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", ""], store)
    end

    test "handles binary element with special characters" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", <<0, 1, 2, 255>>], store)
    end
  end

  # ===========================================================================
  # CF.ADDNX
  # ===========================================================================

  describe "CF.ADDNX" do
    test "adds element when not present" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
    end

    test "returns 0 when element already exists" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
    end

    test "auto-creates filter when key does not exist" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
      assert store.exists?.("cf")
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.ADDNX", ["cf"], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.ADDNX", ["cf", "a", "b"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.ADDNX", [], store)
    end
  end

  # ===========================================================================
  # CF.DEL
  # ===========================================================================

  describe "CF.DEL" do
    test "deletes an element that exists" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
    end

    test "returns 0 when element does not exist" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      assert 0 = Cuckoo.handle("CF.DEL", ["cf", "goodbye"], store)
    end

    test "returns 0 when key does not exist" do
      store = MockStore.make()
      assert 0 = Cuckoo.handle("CF.DEL", ["nonexistent", "hello"], store)
    end

    test "element is not found after deletion" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end

    test "deletes only one occurrence of a duplicate" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 2 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.DEL", ["cf"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.DEL", [], store)
    end
  end

  # ===========================================================================
  # CF.EXISTS
  # ===========================================================================

  describe "CF.EXISTS" do
    test "returns 1 for an element that was added" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end

    test "returns 0 for an element that was not added (high probability)" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf", "goodbye"], store)
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 = Cuckoo.handle("CF.EXISTS", ["nonexistent", "hello"], store)
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.EXISTS", ["cf"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.EXISTS", [], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.EXISTS", ["cf", "a", "b"], store)
    end
  end

  # ===========================================================================
  # CF.MEXISTS
  # ===========================================================================

  describe "CF.MEXISTS" do
    test "checks multiple elements at once" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "a"], store)
      Cuckoo.handle("CF.ADD", ["cf", "b"], store)
      result = Cuckoo.handle("CF.MEXISTS", ["cf", "a", "b", "c"], store)
      assert Enum.slice(result, 0, 2) == [1, 1]
      # "c" was not added
      assert Enum.at(result, 2) == 0
    end

    test "returns all zeros for non-existent key" do
      store = MockStore.make()
      result = Cuckoo.handle("CF.MEXISTS", ["nonexistent", "a", "b"], store)
      assert result == [0, 0]
    end

    test "handles single element" do
      store = MockStore.make()
      Cuckoo.handle("CF.ADD", ["cf", "x"], store)
      result = Cuckoo.handle("CF.MEXISTS", ["cf", "x"], store)
      assert result == [1]
    end

    test "returns error with only key argument" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.MEXISTS", ["cf"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.MEXISTS", [], store)
    end
  end

  # ===========================================================================
  # CF.COUNT
  # ===========================================================================

  describe "CF.COUNT" do
    test "returns 0 for element not in filter" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      assert 0 = Cuckoo.handle("CF.COUNT", ["cf", "missing"], store)
    end

    test "returns 1 for element added once" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
    end

    test "returns 2 for element added twice" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 2 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 = Cuckoo.handle("CF.COUNT", ["nonexistent", "hello"], store)
    end

    test "count decreases after deletion" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 2 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.COUNT", ["cf"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.COUNT", [], store)
    end
  end

  # ===========================================================================
  # CF.INFO
  # ===========================================================================

  describe "CF.INFO" do
    test "returns filter information" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      result = Cuckoo.handle("CF.INFO", ["cf"], store)
      assert is_list(result)

      info = list_to_info_map(result)
      assert info["Number of buckets"] == 1024
      assert info["Number of items inserted"] == 1
      assert info["Number of items deleted"] == 0
      assert info["Bucket size"] == 4
      assert info["Fingerprint size"] == 2
      assert info["Size"] == 1024 * 4
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = Cuckoo.handle("CF.INFO", ["nonexistent"], store)
      assert msg =~ "not found"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.INFO", [], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.INFO", ["a", "b"], store)
    end

    test "tracks deletions" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)

      info = list_to_info_map(Cuckoo.handle("CF.INFO", ["cf"], store))
      assert info["Number of items inserted"] == 0
      assert info["Number of items deleted"] == 1
    end
  end

  # ===========================================================================
  # Accuracy verification
  # ===========================================================================

  describe "accuracy verification" do
    test "no false negatives: all added elements are found" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "2048"], store)

      elements = for i <- 1..200, do: "element_#{i}"
      Enum.each(elements, fn e -> Cuckoo.handle("CF.ADD", ["cf", e], store) end)

      Enum.each(elements, fn e ->
        assert 1 == Cuckoo.handle("CF.EXISTS", ["cf", e], store),
               "False negative for #{e}"
      end)
    end

    test "false positive rate is reasonable (under 10%)" do
      store = MockStore.make()
      capacity = 4096
      Cuckoo.handle("CF.RESERVE", ["cf", "#{capacity}"], store)

      # Add some elements.
      n_added = 1000
      for i <- 1..n_added do
        Cuckoo.handle("CF.ADD", ["cf", "added_#{i}"], store)
      end

      # Check elements that were NOT added.
      test_count = 5000
      false_positives =
        Enum.count(1..test_count, fn i ->
          Cuckoo.handle("CF.EXISTS", ["cf", "not_added_#{i}"], store) == 1
        end)

      observed_rate = false_positives / test_count
      # Cuckoo filters with 2-byte fingerprints have theoretical FP rate ~1/256^2
      # but bucket occupancy affects this. Allow generous 10% margin.
      assert observed_rate < 0.10,
             "False positive rate #{Float.round(observed_rate * 100, 2)}% exceeds 10%"
    end

    test "deletion works correctly - deleted elements are not found" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "2048"], store)

      # Add 50 elements.
      elements = for i <- 1..50, do: "elem_#{i}"
      Enum.each(elements, fn e -> Cuckoo.handle("CF.ADD", ["cf", e], store) end)

      # Delete the first 25.
      {to_delete, to_keep} = Enum.split(elements, 25)
      Enum.each(to_delete, fn e -> Cuckoo.handle("CF.DEL", ["cf", e], store) end)

      # Kept elements must still be found.
      Enum.each(to_keep, fn e ->
        assert 1 == Cuckoo.handle("CF.EXISTS", ["cf", e], store),
               "Kept element #{e} not found after sibling deletions"
      end)

      # Deleted elements should not be found.
      Enum.each(to_delete, fn e ->
        assert 0 == Cuckoo.handle("CF.EXISTS", ["cf", e], store),
               "Deleted element #{e} still found"
      end)
    end
  end

  # ===========================================================================
  # Cross-command interactions
  # ===========================================================================

  describe "cross-command interactions" do
    test "CF.ADD then CF.EXISTS returns 1" do
      store = MockStore.make()
      Cuckoo.handle("CF.ADD", ["cf", "test"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "test"], store)
    end

    test "CF.ADDNX prevents duplicates, CF.ADD allows them" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)

      # ADDNX first time succeeds
      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
      # ADDNX second time fails (already exists)
      assert 0 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
      # ADD always succeeds
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      # Count is now 2 (original + the ADD)
      assert 2 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
    end

    test "add-delete-add cycle works" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end

    test "multiple independent cuckoo filters do not interfere" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf1", "1024"], store)
      Cuckoo.handle("CF.RESERVE", ["cf2", "1024"], store)

      Cuckoo.handle("CF.ADD", ["cf1", "only_in_cf1"], store)
      Cuckoo.handle("CF.ADD", ["cf2", "only_in_cf2"], store)

      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf1", "only_in_cf1"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf1", "only_in_cf2"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf2", "only_in_cf1"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf2", "only_in_cf2"], store)
    end

    test "auto-created filter has default capacity" do
      store = MockStore.make()
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      info = list_to_info_map(Cuckoo.handle("CF.INFO", ["cf"], store))
      assert info["Number of buckets"] == 1024
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "dispatcher integration" do
    test "CF.ADD is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert 1 = Dispatcher.dispatch("CF.ADD", ["cf", "hello"], store)
    end

    test "CF.EXISTS is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("CF.ADD", ["cf", "hello"], store)
      assert 1 = Dispatcher.dispatch("CF.EXISTS", ["cf", "hello"], store)
    end

    test "cf.add lowercase is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert 1 = Dispatcher.dispatch("cf.add", ["cf", "hello"], store)
    end

    test "Cf.Reserve mixed case is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("Cf.Reserve", ["cf", "1024"], store)
    end

    test "CF.DEL is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("CF.ADD", ["cf", "hello"], store)
      assert 1 = Dispatcher.dispatch("CF.DEL", ["cf", "hello"], store)
    end

    test "CF.ADDNX is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      assert 1 = Dispatcher.dispatch("CF.ADDNX", ["cf", "hello"], store)
      assert 0 = Dispatcher.dispatch("CF.ADDNX", ["cf", "hello"], store)
    end

    test "CF.COUNT is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("CF.ADD", ["cf", "hello"], store)
      assert 1 = Dispatcher.dispatch("CF.COUNT", ["cf", "hello"], store)
    end

    test "CF.INFO is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("CF.RESERVE", ["cf", "1024"], store)
      result = Dispatcher.dispatch("CF.INFO", ["cf"], store)
      assert is_list(result)
    end

    test "CF.MEXISTS is routed through dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()
      Dispatcher.dispatch("CF.ADD", ["cf", "a"], store)
      result = Dispatcher.dispatch("CF.MEXISTS", ["cf", "a", "b"], store)
      assert result == [1, 0]
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
