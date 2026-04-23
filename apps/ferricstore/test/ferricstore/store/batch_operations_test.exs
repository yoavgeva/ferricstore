defmodule Ferricstore.Store.BatchOperationsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @ns_async "batch_test_async"
  @ns_quorum "batch_test_quorum"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns_async, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns_async, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ctx, do: FerricStore.Instance.get(:default)

  # ---------------------------------------------------------------------------
  # batch_async_put
  # ---------------------------------------------------------------------------

  describe "batch_async_put" do
    test "all-small batch: values readable immediately" do
      kvs = for i <- 1..20, do: {"#{@ns_async}:bap_small_#{i}", "val_#{i}"}
      :ok = Router.batch_async_put(ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(ctx(), key) == value
      end
    end

    test "all-large batch: values > hot_cache_max written to disk" do
      big = :binary.copy("L", 100 * 1024)
      kvs = for i <- 1..5, do: {"#{@ns_async}:bap_large_#{i}", big}
      :ok = Router.batch_async_put(ctx(), kvs)

      for {key, _} <- kvs do
        assert Router.get(ctx(), key) == big
      end
    end

    test "mixed large/small batch: all values readable" do
      small = "small_value"
      big = :binary.copy("M", 100 * 1024)

      kvs = [
        {"#{@ns_async}:bap_mix_s1", small},
        {"#{@ns_async}:bap_mix_l1", big},
        {"#{@ns_async}:bap_mix_s2", small},
        {"#{@ns_async}:bap_mix_l2", big},
        {"#{@ns_async}:bap_mix_s3", small}
      ]

      :ok = Router.batch_async_put(ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(ctx(), key) == value
      end
    end

    test "empty batch is a no-op" do
      assert :ok = Router.batch_async_put(ctx(), [])
    end

    test "single-element batch works" do
      kvs = [{"#{@ns_async}:bap_single", "one"}]
      :ok = Router.batch_async_put(ctx(), kvs)
      assert Router.get(ctx(), "#{@ns_async}:bap_single") == "one"
    end

    test "overwrites existing keys" do
      key = "#{@ns_async}:bap_overwrite"
      :ok = Router.put(ctx(), key, "original", 0)
      assert Router.get(ctx(), key) == "original"

      :ok = Router.batch_async_put(ctx(), [{key, "updated"}])
      assert Router.get(ctx(), key) == "updated"
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.batch_set (public API)
  # ---------------------------------------------------------------------------

  describe "FerricStore.batch_set" do
    test "all-async namespace returns list of :ok" do
      kvs = for i <- 1..10, do: {"#{@ns_async}:bs_#{i}", "v#{i}"}
      results = FerricStore.batch_set(kvs)
      assert results == List.duplicate(:ok, 10)

      for {key, value} <- kvs do
        assert {:ok, value} == FerricStore.get(key)
      end
    end

    test "all-quorum namespace returns list of :ok" do
      kvs = for i <- 1..10, do: {"#{@ns_quorum}:bs_#{i}", "v#{i}"}
      results = FerricStore.batch_set(kvs)
      assert Enum.all?(results, &(&1 == :ok))

      for {key, value} <- kvs do
        assert {:ok, value} == FerricStore.get(key)
      end
    end

    test "mixed async/quorum namespaces preserves result order" do
      kvs = [
        {"#{@ns_async}:mix_1", "async_val"},
        {"#{@ns_quorum}:mix_2", "quorum_val"},
        {"#{@ns_async}:mix_3", "async_val2"},
        {"#{@ns_quorum}:mix_4", "quorum_val2"}
      ]

      results = FerricStore.batch_set(kvs)
      assert length(results) == 4
      assert Enum.all?(results, &(&1 == :ok))

      assert {:ok, "async_val"} == FerricStore.get("#{@ns_async}:mix_1")
      assert {:ok, "quorum_val"} == FerricStore.get("#{@ns_quorum}:mix_2")
      assert {:ok, "async_val2"} == FerricStore.get("#{@ns_async}:mix_3")
      assert {:ok, "quorum_val2"} == FerricStore.get("#{@ns_quorum}:mix_4")
    end

    test "empty list returns empty list" do
      assert FerricStore.batch_set([]) == []
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.batch_get (public API)
  # ---------------------------------------------------------------------------

  describe "FerricStore.batch_get" do
    test "returns values in same order as keys" do
      for i <- 1..5 do
        :ok = FerricStore.set("bg_order_#{i}", "val_#{i}")
      end

      keys = for i <- 1..5, do: "bg_order_#{i}"
      results = FerricStore.batch_get(keys)
      assert results == ["val_1", "val_2", "val_3", "val_4", "val_5"]
    end

    test "returns nil for missing keys" do
      :ok = FerricStore.set("bg_exists", "here")
      results = FerricStore.batch_get(["bg_exists", "bg_missing", "bg_also_missing"])
      assert results == ["here", nil, nil]
    end

    test "empty list returns empty list" do
      assert FerricStore.batch_get([]) == []
    end

    test "single key works" do
      :ok = FerricStore.set("bg_single", "solo")
      assert FerricStore.batch_get(["bg_single"]) == ["solo"]
    end

    @tag timeout: 120_000
    test "large batch (1000 keys) returns correct results" do
      kvs = for i <- 1..1000, do: {"bg_large_#{i}", "v#{i}"}
      FerricStore.batch_set(kvs)

      keys = for i <- 1..1000, do: "bg_large_#{i}"
      results = FerricStore.batch_get(keys)
      assert length(results) == 1000
      assert Enum.at(results, 0) == "v1"
      assert Enum.at(results, 999) == "v1000"
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.packed_batch_get (binary protocol)
  # ---------------------------------------------------------------------------

  describe "FerricStore.packed_batch_get" do
    test "round-trips correctly" do
      for i <- 1..3, do: FerricStore.set("pbg_#{i}", "val_#{i}")

      keys = ["pbg_1", "pbg_2", "pbg_3"]
      packed = pack_keys(keys)
      result = FerricStore.packed_batch_get(packed)

      values = unpack_values(result, 3)
      assert values == ["val_1", "val_2", "val_3"]
    end

    test "nil values encoded as 0xFFFFFFFF" do
      FerricStore.set("pbg_exists", "here")
      packed = pack_keys(["pbg_exists", "pbg_nope"])
      result = FerricStore.packed_batch_get(packed)

      values = unpack_values(result, 2)
      assert values == ["here", nil]
    end

    test "single key" do
      FerricStore.set("pbg_one", "solo")
      packed = pack_keys(["pbg_one"])
      result = FerricStore.packed_batch_get(packed)
      assert unpack_values(result, 1) == ["solo"]
    end
  end

  # ---------------------------------------------------------------------------
  # Async delete origin-skip correctness
  # ---------------------------------------------------------------------------

  describe "async delete" do
    test "delete is applied on all nodes (not skipped on origin)" do
      key = "#{@ns_async}:del_origin_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "present", 0)
      assert Router.get(ctx(), key) == "present"

      Router.delete(ctx(), key)
      assert Router.get(ctx(), key) == nil

      Process.sleep(200)
      assert Router.get(ctx(), key) == nil
    end

    test "batch delete followed by set works correctly" do
      key = "#{@ns_async}:del_then_set_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "first", 0)
      Router.delete(ctx(), key)
      :ok = Router.put(ctx(), key, "second", 0)
      assert Router.get(ctx(), key) == "second"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp pack_keys(keys) do
    count = length(keys)
    body = for k <- keys, into: <<>>, do: <<byte_size(k)::16, k::binary>>
    <<count::32, body::binary>>
  end

  defp unpack_values(<<>>, 0), do: []
  defp unpack_values(<<0xFFFFFFFF::32, rest::binary>>, n), do: [nil | unpack_values(rest, n - 1)]
  defp unpack_values(<<len::32, val::binary-size(len), rest::binary>>, n), do: [val | unpack_values(rest, n - 1)]
end
