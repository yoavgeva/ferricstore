defmodule Ferricstore.Store.BloomRegistryTest do
  @moduledoc """
  Tests for the mmap-backed Bloom filter registry.

  Covers ETS table lifecycle, resource registration, lookup, open-or-lookup
  (lazy re-open from disk), deletion, recovery from disk, and path helpers.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BloomRegistry

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Each test gets a unique shard index to avoid ETS table collisions.
  @base_test_index 5000

  defp unique_index do
    @base_test_index + :rand.uniform(100_000)
  end

  defp make_temp_dir do
    dir = Path.join(System.tmp_dir!(), "bloom_reg_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)
    dir
  end

  defp make_prob_dir(data_dir, index) do
    dir = BloomRegistry.prob_dir(data_dir, index)
    File.mkdir_p!(dir)
    dir
  end

  # ===========================================================================
  # Table management
  # ===========================================================================

  describe "create_table/1" do
    test "creates a new ETS table" do
      index = unique_index()
      name = BloomRegistry.create_table(index)
      assert name == :"bloom_reg_#{index}"
      assert :ets.info(name, :size) == 0
    end

    test "clears existing table on re-creation" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)
      path = BloomRegistry.bloom_path(data_dir, index, "test")
      {:ok, resource} = NIF.bloom_create(path, 100, 3)
      BloomRegistry.register(index, "test", resource, %{capacity: 100, error_rate: 0.01})

      assert :ets.info(:"bloom_reg_#{index}", :size) == 1

      # Re-create should clear
      BloomRegistry.create_table(index)
      assert :ets.info(:"bloom_reg_#{index}", :size) == 0
    end
  end

  # ===========================================================================
  # register / lookup / delete
  # ===========================================================================

  describe "register and lookup" do
    test "stores and retrieves a bloom resource" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)
      path = BloomRegistry.bloom_path(data_dir, index, "mykey")
      {:ok, resource} = NIF.bloom_create(path, 1000, 7)
      meta = %{capacity: 100, error_rate: 0.01}

      BloomRegistry.register(index, "mykey", resource, meta)

      {found_resource, found_meta} = BloomRegistry.lookup(index, "mykey")
      assert found_resource == resource
      assert found_meta == meta
    end

    test "lookup returns nil for missing key" do
      index = unique_index()
      BloomRegistry.create_table(index)

      assert BloomRegistry.lookup(index, "missing") == nil
    end
  end

  describe "delete/2" do
    test "removes resource from ETS and deletes the file" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)
      path = BloomRegistry.bloom_path(data_dir, index, "deleteme")
      {:ok, resource} = NIF.bloom_create(path, 100, 3)
      BloomRegistry.register(index, "deleteme", resource, %{capacity: 100, error_rate: 0.01})

      assert File.exists?(path)
      BloomRegistry.delete(index, "deleteme")
      refute File.exists?(path)
      assert BloomRegistry.lookup(index, "deleteme") == nil
    end

    test "delete on non-existent key is a no-op" do
      index = unique_index()
      BloomRegistry.create_table(index)
      assert :ok = BloomRegistry.delete(index, "nope")
    end
  end

  # ===========================================================================
  # open_or_lookup (lazy re-open)
  # ===========================================================================

  describe "open_or_lookup/4" do
    test "returns cached resource on hit" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)
      path = BloomRegistry.bloom_path(data_dir, index, "cached")
      {:ok, resource} = NIF.bloom_create(path, 1000, 7)
      meta = %{capacity: 100, error_rate: 0.01}
      BloomRegistry.register(index, "cached", resource, meta)

      {:ok, found_resource, found_meta} = BloomRegistry.open_or_lookup(index, "cached", data_dir)
      assert found_resource == resource
      assert found_meta == meta
    end

    test "re-opens from disk on cache miss" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)
      path = BloomRegistry.bloom_path(data_dir, index, "reopen")
      {:ok, resource} = NIF.bloom_create(path, 1000, 7)
      NIF.bloom_add(resource, "hello")

      # Don't register -- simulate cache miss
      {:ok, reopened, _meta} = BloomRegistry.open_or_lookup(index, "reopen", data_dir)
      assert 1 = NIF.bloom_exists(reopened, "hello")
      assert 0 = NIF.bloom_exists(reopened, "world")
    end

    test "returns :not_found when no file on disk" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)

      assert :not_found = BloomRegistry.open_or_lookup(index, "ghost", data_dir)
    end
  end

  # ===========================================================================
  # recover (shard restart)
  # ===========================================================================

  describe "recover/2" do
    test "re-opens all .bloom files in the prob directory" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)

      # Create 3 bloom files directly via NIF
      for key <- ["alpha", "beta", "gamma"] do
        path = BloomRegistry.bloom_path(data_dir, index, key)
        {:ok, resource} = NIF.bloom_create(path, 500, 5)
        NIF.bloom_add(resource, "test_#{key}")
      end

      # Clear the ETS table (simulate shard restart)
      BloomRegistry.create_table(index)

      count = BloomRegistry.recover(data_dir, index)
      assert count == 3

      # All three should be accessible
      for key <- ["alpha", "beta", "gamma"] do
        {resource, _meta} = BloomRegistry.lookup(index, key)
        assert 1 = NIF.bloom_exists(resource, "test_#{key}")
      end
    end

    test "returns 0 for empty prob directory" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      make_prob_dir(data_dir, index)

      assert 0 = BloomRegistry.recover(data_dir, index)
    end

    test "returns 0 for non-existent prob directory" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      # Don't create the prob dir
      assert 0 = BloomRegistry.recover(data_dir, index)
    end

    test "skips corrupted .bloom files" do
      index = unique_index()
      BloomRegistry.create_table(index)

      data_dir = make_temp_dir()
      prob = make_prob_dir(data_dir, index)

      # Create one valid and one corrupted file
      valid_path = BloomRegistry.bloom_path(data_dir, index, "valid")
      {:ok, _resource} = NIF.bloom_create(valid_path, 500, 5)

      corrupted_path = Path.join(prob, "corrupted.bloom")
      File.write!(corrupted_path, "this is garbage")

      count = BloomRegistry.recover(data_dir, index)
      assert count == 1
      assert BloomRegistry.lookup(index, "valid") != nil
      assert BloomRegistry.lookup(index, "corrupted") == nil
    end
  end

  # ===========================================================================
  # Path helpers
  # ===========================================================================

  describe "bloom_path/3" do
    test "constructs the expected path" do
      path = BloomRegistry.bloom_path("/data", 2, "myfilter")
      assert path == "/data/prob/shard_2/myfilter.bloom"
    end

    test "sanitizes special characters in key" do
      path = BloomRegistry.bloom_path("/data", 0, "user:profile:42")
      assert path == "/data/prob/shard_0/user_profile_42.bloom"
    end
  end

  describe "sanitize_key/1" do
    test "replaces non-alphanumeric characters" do
      assert BloomRegistry.sanitize_key("hello world!@#") == "hello_world___"
    end

    test "preserves alphanumeric, underscore, hyphen, dot" do
      assert BloomRegistry.sanitize_key("my-key_v2.test") == "my-key_v2.test"
    end
  end
end
