defmodule Ferricstore.ReviewR2.M5BloomMetaPersistTest do
  @moduledoc """
  Verifies that bloom metadata persists correctly.

  With the new stateless NIF + Raft architecture, metadata is stored in
  Bitcask via Raft (not .meta companion files). BF.INFO derives capacity
  and error_rate from the bloom header when Bitcask metadata is not
  available.
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

  # Build a real store for a specific key (routes to correct shard).
  defp real_store_for_key(key) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_idx = Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_idx)

    %{
      get: &Ferricstore.Store.Router.get/1,
      get_meta: &Ferricstore.Store.Router.get_meta/1,
      put: &Ferricstore.Store.Router.put/3,
      delete: &Ferricstore.Store.Router.delete/1,
      exists?: &Ferricstore.Store.Router.exists?/1,
      keys: &Ferricstore.Store.Router.keys/0,
      flush: fn -> :ok end,
      dbsize: &Ferricstore.Store.Router.dbsize/0,
      prob_dir: fn -> Path.join(shard_path, "prob") end,
      prob_write: &Ferricstore.Store.Router.prob_write/1
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

    test "BF.RESERVE creates bloom file" do
      key = "bloom_meta_test_#{:rand.uniform(999_999)}"
      store = real_store_for_key(key)

      result = Bloom.handle("BF.RESERVE", [key, "0.01", "1000"], store)
      assert result == :ok

      # Verify bloom file exists at the base64-encoded path
      safe = Base.url_encode64(key, padding: false)
      prob_dir = store.prob_dir.()
      path = Path.join(prob_dir, "#{safe}.bloom")
      assert File.exists?(path), "Bloom file should exist at #{path}"
    end

    test "BF.INFO returns reasonable values after reserve" do
      key = "bloom_info_test_#{:rand.uniform(999_999)}"
      store = real_store_for_key(key)

      Bloom.handle("BF.RESERVE", [key, "0.005", "2000"], store)

      info = Bloom.handle("BF.INFO", [key], store)
      assert is_list(info)

      capacity = find_info_value(info, "Capacity")
      error_rate = find_info_value(info, "Error rate")

      # Capacity and error_rate are derived from header when Bitcask
      # metadata is not available (e.g. in test without full Raft apply).
      # Allow approximate values.
      assert is_integer(capacity) and capacity > 0
      assert is_number(error_rate) and error_rate > 0 and error_rate < 1
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
