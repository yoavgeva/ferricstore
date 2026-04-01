defmodule Ferricstore.ReviewR2.M5BloomMetaPersistTest do
  @moduledoc """
  Verifies that BF.INFO returns original capacity/error_rate after
  recovery, not derived approximations.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Store.BloomRegistry
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.flush_all_keys() end)
    :ok
  end

  defp real_store do
    %{
      get: &Ferricstore.Store.Router.get/1,
      get_meta: &Ferricstore.Store.Router.get_meta/1,
      put: &Ferricstore.Store.Router.put/3,
      delete: &Ferricstore.Store.Router.delete/1,
      exists?: &Ferricstore.Store.Router.exists?/1,
      keys: &Ferricstore.Store.Router.keys/0,
      flush: fn -> :ok end,
      dbsize: &Ferricstore.Store.Router.dbsize/0
    }
  end

  describe "bloom metadata persists via .meta file" do
    test "save_meta and load_meta round-trip" do
      path = Path.join(System.tmp_dir!(), "test_bloom_#{:rand.uniform(999_999)}.bloom")
      meta = %{capacity: 5000, error_rate: 0.001}

      BloomRegistry.save_meta(path, meta)
      loaded = BloomRegistry.load_meta(path)

      assert loaded == meta

      # Cleanup
      File.rm(path <> ".meta")
    end

    test "load_meta returns nil for missing file" do
      assert nil == BloomRegistry.load_meta("/nonexistent/path.bloom")
    end

    test "BF.RESERVE creates .meta file" do
      store = real_store()
      key = "bloom_meta_test_#{:rand.uniform(999_999)}"

      result = Bloom.handle("BF.RESERVE", [key, "0.01", "1000"], store)
      assert result == :ok

      # Find the bloom path
      shard_idx = Ferricstore.Store.Router.shard_for(key)
      data_dir = Application.get_env(:ferricstore, :data_dir, "data")
      path = BloomRegistry.bloom_path(data_dir, shard_idx, key)

      # .meta file should exist
      assert File.exists?(path <> ".meta")

      # Load and verify
      meta = BloomRegistry.load_meta(path)
      assert meta.capacity == 1000
      assert meta.error_rate == 0.01
    end

    test "BF.INFO returns original values (not derived)" do
      store = real_store()
      key = "bloom_info_test_#{:rand.uniform(999_999)}"

      Bloom.handle("BF.RESERVE", [key, "0.005", "2000"], store)

      info = Bloom.handle("BF.INFO", [key], store)

      # info is a keyword list or map with capacity and error_rate
      assert is_list(info) or is_map(info)

      # Find capacity and error_rate in the info response
      capacity = find_info_value(info, "Capacity")
      error_rate = find_info_value(info, "Error rate")

      assert capacity == 2000, "Expected capacity 2000, got #{inspect(capacity)}"
      assert_in_delta error_rate, 0.005, 0.0001
    end
  end

  defp find_info_value(info, label) when is_list(info) do
    info
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      [^label, value] -> value
      _ -> nil
    end)
  end

  defp find_info_value(info, label) when is_map(info) do
    Map.get(info, label)
  end
end
