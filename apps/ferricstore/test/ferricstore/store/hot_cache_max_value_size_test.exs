defmodule Ferricstore.Store.HotCacheMaxValueSizeTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Shard

  @small_threshold 128

  setup do
    # Set a small threshold for testing (128 bytes)
    original_threshold = :persistent_term.get(:ferricstore_hot_cache_max_value_size, 65_536)
    :persistent_term.put(:ferricstore_hot_cache_max_value_size, @small_threshold)

    dir = Path.join(System.tmp_dir!(), "hot_cache_size_test_#{:rand.uniform(999_999)}")
    File.mkdir_p!(dir)

    index = :erlang.unique_integer([:positive]) |> rem(10_000) |> Kernel.+(10_000)
    {:ok, pid} = Shard.start_link(index: index, data_dir: dir)

    on_exit(fn ->
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original_threshold)
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm_rf!(dir)
    end)

    %{shard: pid, index: index}
  end

  describe "value below threshold" do
    test "small value is stored hot in ETS", %{shard: shard, index: index} do
      small_value = String.duplicate("a", @small_threshold - 1)
      :ok = GenServer.call(shard, {:put, "small", small_value, 0})

      # Flush to disk so ETS has disk location
      :ok = GenServer.call(shard, :flush)

      # Check ETS directly -- value should be present (hot)
      keydir = :"keydir_#{index}"
      [{_key, ets_value, _exp, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, "small")
      assert ets_value != nil
      assert ets_value == small_value
    end

    test "small value is returned via get", %{shard: shard} do
      small_value = String.duplicate("b", 50)
      :ok = GenServer.call(shard, {:put, "small_get", small_value, 0})
      assert small_value == GenServer.call(shard, {:get, "small_get"})
    end
  end

  describe "value above threshold" do
    test "large value is stored cold (nil) in ETS", %{shard: shard, index: index} do
      large_value = String.duplicate("x", @small_threshold + 1)
      :ok = GenServer.call(shard, {:put, "large", large_value, 0})

      # Flush to disk so ETS has disk location
      :ok = GenServer.call(shard, :flush)

      # Check ETS directly -- value should be nil (cold)
      keydir = :"keydir_#{index}"
      [{_key, ets_value, _exp, _lfu, _fid, _off, vsize}] = :ets.lookup(keydir, "large")
      assert ets_value == nil
      # value_size should be the actual size, not 0
      assert vsize == byte_size(large_value)
    end

    test "GET on large cold value returns correct data from disk", %{shard: shard} do
      large_value = String.duplicate("y", @small_threshold + 100)
      :ok = GenServer.call(shard, {:put, "large_get", large_value, 0})

      # Flush to ensure it's on disk
      :ok = GenServer.call(shard, :flush)

      # GET should return the correct value (read from disk)
      assert large_value == GenServer.call(shard, {:get, "large_get"})
    end
  end

  describe "value exactly at threshold" do
    test "value at exactly threshold bytes is stored hot (> not >=)", %{shard: shard, index: index} do
      exact_value = String.duplicate("e", @small_threshold)
      :ok = GenServer.call(shard, {:put, "exact", exact_value, 0})

      :ok = GenServer.call(shard, :flush)

      keydir = :"keydir_#{index}"
      [{_key, ets_value, _exp, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, "exact")
      assert ets_value != nil
      assert ets_value == exact_value
    end
  end

  describe "overwrite transitions" do
    test "small value -> large value transitions from hot to cold", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      small = String.duplicate("s", 10)
      large = String.duplicate("L", @small_threshold + 50)

      :ok = GenServer.call(shard, {:put, "transition", small, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be hot
      [{_, v1, _, _, _, _, _}] = :ets.lookup(keydir, "transition")
      assert v1 != nil

      :ok = GenServer.call(shard, {:put, "transition", large, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be cold now
      [{_, v2, _, _, _, _, vsize}] = :ets.lookup(keydir, "transition")
      assert v2 == nil
      assert vsize == byte_size(large)

      # GET still returns the correct large value
      assert large == GenServer.call(shard, {:get, "transition"})
    end

    test "large value -> small value transitions from cold to hot", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      large = String.duplicate("L", @small_threshold + 50)
      small = String.duplicate("s", 10)

      :ok = GenServer.call(shard, {:put, "transition2", large, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be cold
      [{_, v1, _, _, _, _, _}] = :ets.lookup(keydir, "transition2")
      assert v1 == nil

      :ok = GenServer.call(shard, {:put, "transition2", small, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be hot now
      [{_, v2, _, _, _, _, _}] = :ets.lookup(keydir, "transition2")
      assert v2 != nil
      assert v2 == small
    end
  end

  describe "edge cases" do
    test "empty value (0 bytes) is always hot", %{shard: shard, index: index} do
      :ok = GenServer.call(shard, {:put, "empty", "", 0})
      :ok = GenServer.call(shard, :flush)

      keydir = :"keydir_#{index}"
      [{_, ets_value, _, _, _, _, _}] = :ets.lookup(keydir, "empty")
      assert ets_value == ""
    end

    test "binary with embedded nulls works correctly", %{shard: shard} do
      # Large value with embedded nulls
      value = String.duplicate(<<0, 1, 2, 0, 3>>, 30)
      :ok = GenServer.call(shard, {:put, "null_bytes", value, 0})
      :ok = GenServer.call(shard, :flush)

      assert value == GenServer.call(shard, {:get, "null_bytes"})
    end

    test "STRLEN on large cold value uses value_size from 7-tuple (no disk read needed)", %{shard: shard, index: index} do
      large_value = String.duplicate("z", @small_threshold + 200)
      :ok = GenServer.call(shard, {:put, "strlen_key", large_value, 0})
      :ok = GenServer.call(shard, :flush)

      # The keydir entry should have the correct vsize even though value is nil
      keydir = :"keydir_#{index}"
      [{_, nil, _, _, _fid, _off, vsize}] = :ets.lookup(keydir, "strlen_key")
      assert vsize == byte_size(large_value)
    end
  end

  describe "config change at runtime" do
    test "changing threshold via persistent_term updates behavior", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"

      # With current threshold (128), 200-byte value is cold
      value_200 = String.duplicate("a", 200)
      :ok = GenServer.call(shard, {:put, "runtime_key", value_200, 0})
      :ok = GenServer.call(shard, :flush)

      [{_, v1, _, _, _, _, _}] = :ets.lookup(keydir, "runtime_key")
      assert v1 == nil

      # Increase threshold to 256 -- now 200-byte values should be hot
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, 256)

      # Overwrite triggers re-insert with new threshold
      :ok = GenServer.call(shard, {:put, "runtime_key", value_200, 0})
      :ok = GenServer.call(shard, :flush)

      [{_, v2, _, _, _, _, _}] = :ets.lookup(keydir, "runtime_key")
      assert v2 == value_200

      # Restore threshold
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, @small_threshold)
    end
  end

  describe "config set to 0 (everything cold)" do
    test "all values stored cold when threshold is 0", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"

      :persistent_term.put(:ferricstore_hot_cache_max_value_size, 0)

      :ok = GenServer.call(shard, {:put, "zero_threshold", "tiny", 0})
      :ok = GenServer.call(shard, :flush)

      [{_, ets_value, _, _, _, _, _}] = :ets.lookup(keydir, "zero_threshold")
      assert ets_value == nil

      # GET still works (reads from disk)
      assert "tiny" == GenServer.call(shard, {:get, "zero_threshold"})

      # Restore threshold
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, @small_threshold)
    end
  end

  describe "config set to very large number (everything hot)" do
    test "all values stored hot when threshold is very large", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"

      :persistent_term.put(:ferricstore_hot_cache_max_value_size, 1_073_741_824)

      large_value = String.duplicate("H", @small_threshold + 500)
      :ok = GenServer.call(shard, {:put, "huge_threshold", large_value, 0})
      :ok = GenServer.call(shard, :flush)

      [{_, ets_value, _, _, _, _, _}] = :ets.lookup(keydir, "huge_threshold")
      assert ets_value == large_value

      # Restore threshold
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, @small_threshold)
    end
  end

  describe "CONFIG SET/GET integration" do
    test "CONFIG SET hot-cache-max-value-size updates persistent_term" do
      original = :persistent_term.get(:ferricstore_hot_cache_max_value_size)

      :ok = Ferricstore.Config.set("hot-cache-max-value-size", "131072")
      assert :persistent_term.get(:ferricstore_hot_cache_max_value_size) == 131_072

      # CONFIG GET returns the new value
      pairs = Ferricstore.Config.get("hot-cache-max-value-size")
      assert [{"hot-cache-max-value-size", "131072"}] = pairs

      # Restore
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original)
      Application.put_env(:ferricstore, :hot_cache_max_value_size, original)
    end

    test "CONFIG SET hot-cache-max-value-size rejects negative values" do
      assert {:error, _} = Ferricstore.Config.set("hot-cache-max-value-size", "-1")
    end

    test "CONFIG SET hot-cache-max-value-size accepts 0" do
      original = :persistent_term.get(:ferricstore_hot_cache_max_value_size)

      :ok = Ferricstore.Config.set("hot-cache-max-value-size", "0")
      assert :persistent_term.get(:ferricstore_hot_cache_max_value_size) == 0

      # Restore
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original)
      Application.put_env(:ferricstore, :hot_cache_max_value_size, original)
    end
  end
end
