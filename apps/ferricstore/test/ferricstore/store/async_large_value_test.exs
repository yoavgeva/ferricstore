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

  defp eventually(fun, msg, attempts \\ 400) do
    result =
      try do
        fun.()
      rescue
        _ -> false
      catch
        :exit, _ -> false
      end

    if result do
      :ok
    else
      if attempts > 0 do
        Process.sleep(50)
        eventually(fun, msg, attempts - 1)
      else
        flunk("Timed out: #{msg}")
      end
    end
  end

  describe "async write with large values (>64KB)" do
    test "large value is readable immediately after write" do
      big_value = :binary.copy("x", 100_000)
      :ok = Router.put("alv_test:big1", big_value, 0)

      eventually(
        fn -> Router.get("alv_test:big1") == big_value end,
        "large value should be readable after async flush"
      )
    end

    test "small value still works (inline ETS)" do
      :ok = Router.put("alv_test:small1", "hello", 0)
      assert Router.get("alv_test:small1") == "hello"
    end

    test "value at exactly 64KB boundary" do
      exact = :binary.copy("y", 65_536)
      :ok = Router.put("alv_test:exact", exact, 0)

      eventually(
        fn -> Router.get("alv_test:exact") == exact end,
        "64KB boundary value should be readable after async flush"
      )
    end

    test "multiple large values" do
      for i <- 1..10 do
        val = :binary.copy("z", 100_000 + i)
        :ok = Router.put("alv_test:multi_#{i}", val, 0)
      end

      # Large values (>64KB) are stored as nil in ETS and written to Bitcask
      # asynchronously. Wait for each value to be readable from disk.
      for i <- 1..10 do
        expected_size = 100_000 + i
        key = "alv_test:multi_#{i}"

        eventually(fn ->
          val = Router.get(key)
          val != nil and byte_size(val) == expected_size
        end, "Key #{key} should have size #{expected_size}")
      end
    end
  end
end
