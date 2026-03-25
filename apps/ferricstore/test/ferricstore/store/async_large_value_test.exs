defmodule Ferricstore.Store.AsyncLargeValueTest do
  use ExUnit.Case, async: false
  alias Ferricstore.Store.Router

  setup do
    Ferricstore.NamespaceConfig.set("alv_test", "durability", "async")
    Ferricstore.Test.ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set("alv_test", "durability", "quorum")
      Ferricstore.Test.ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  describe "async write with large values (>64KB)" do
    test "large value is readable immediately after write" do
      big_value = :binary.copy("x", 100_000)
      :ok = Router.put("alv_test:big1", big_value, 0)
      assert Router.get("alv_test:big1") == big_value
    end

    test "small value still works (inline ETS)" do
      :ok = Router.put("alv_test:small1", "hello", 0)
      assert Router.get("alv_test:small1") == "hello"
    end

    test "value at exactly 64KB boundary" do
      exact = :binary.copy("y", 65_536)
      :ok = Router.put("alv_test:exact", exact, 0)
      assert Router.get("alv_test:exact") == exact
    end

    test "multiple large values" do
      for i <- 1..10 do
        val = :binary.copy("z", 100_000 + i)
        :ok = Router.put("alv_test:multi_#{i}", val, 0)
      end

      for i <- 1..10 do
        val = Router.get("alv_test:multi_#{i}")
        assert val != nil, "Key alv_test:multi_#{i} should not be nil"
        assert byte_size(val) == 100_000 + i
      end
    end
  end
end
