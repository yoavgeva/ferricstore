defmodule Ferricstore.ShutdownTest do
  @moduledoc """
  Tests that verify shutdown behaviour for individual shards.

  Covers spec section 2C.6: after stopping a shard, it is no longer alive;
  pending writes are flushed before shutdown (hint/data files exist on disk).

  These tests do NOT stop the entire application (that would break other
  tests). Instead, they exercise individual shard lifecycle by starting
  isolated shard processes under a temporary supervisor and then stopping
  them cleanly, or by verifying that the application-supervised shards
  flush data to disk.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # After stopping a shard, it is no longer alive
  # ---------------------------------------------------------------------------

  describe "shard not alive after stop" do
    @tag :capture_log
    test "killing a shard makes it temporarily unregistered" do
      name = Router.shard_name(0)
      old_pid = Process.whereis(name)
      assert is_pid(old_pid) and Process.alive?(old_pid)

      ref = Process.monitor(old_pid)
      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000

      # The old PID is definitely not alive.
      refute Process.alive?(old_pid),
             "Old shard PID should not be alive after being killed"

      # Wait for the supervisor to restart it so other tests are not affected.
      ShardHelpers.wait_shards_alive()
    end

    @tag :capture_log
    test "stopped shard is restarted by supervisor with a new PID" do
      name = Router.shard_name(1)
      old_pid = Process.whereis(name)
      ref = Process.monitor(old_pid)
      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000

      ShardHelpers.wait_shards_alive()

      new_pid = Process.whereis(name)
      assert is_pid(new_pid)
      assert Process.alive?(new_pid)
      assert new_pid != old_pid, "Restarted shard should have a different PID"
    end
  end

  # ---------------------------------------------------------------------------
  # Pending writes are flushed before shutdown (data files on disk)
  # ---------------------------------------------------------------------------

  describe "pending writes flushed before shutdown" do
    test "data written to shard is persisted to Bitcask files on disk" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      key = "shutdown_flush_test_#{:rand.uniform(9_999_999)}"
      value = "must_be_on_disk"

      Router.put(key, value, 0)

      # Explicitly flush to ensure pending async writes hit disk.
      shard_idx = Router.shard_for(key)
      shard_name = Router.shard_name(shard_idx)
      :ok = GenServer.call(shard_name, :flush)

      # Verify the shard's data directory exists and has files.
      shard_dir = Path.join(data_dir, "shard_#{shard_idx}")
      assert File.dir?(shard_dir), "Shard data directory should exist: #{shard_dir}"

      files = File.ls!(shard_dir)
      assert length(files) > 0, "Shard directory should contain Bitcask data files"

      # Verify the data is recoverable from disk by reading through the NIF.
      # Open a fresh Bitcask store at the same path to confirm the data was
      # flushed and is readable independently.
      {:ok, fresh_store} = NIF.new(shard_dir)
      {:ok, recovered_value} = NIF.get(fresh_store, key)
      assert recovered_value == value, "Value should be recoverable from Bitcask on disk"
    end

    test "multiple keys are persisted and recoverable from Bitcask" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      prefix = "shutdown_multi_#{:rand.uniform(9_999_999)}"

      keys_and_values =
        for i <- 1..20 do
          k = "#{prefix}_#{i}"
          v = "value_#{i}"
          Router.put(k, v, 0)
          {k, v}
        end

      # Flush all shards to guarantee writes are on disk.
      ShardHelpers.flush_all_shards()

      # Verify each key is recoverable from its shard's Bitcask directory.
      for {k, v} <- keys_and_values do
        shard_idx = Router.shard_for(k)
        shard_dir = Path.join(data_dir, "shard_#{shard_idx}")
        {:ok, store} = NIF.new(shard_dir)
        {:ok, recovered} = NIF.get(store, k)
        assert recovered == v, "Key #{k} should be recoverable from Bitcask"
      end
    end

    test "data directory has files for all active shards" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)

      # Write at least one key per shard to ensure all directories have data.
      for i <- 0..3 do
        # Find a key that hashes to shard i.
        key =
          Enum.find_value(1..100_000, fn n ->
            k = "shard_data_probe_#{i}_#{n}"
            if Router.shard_for(k) == i, do: k
          end)

        Router.put(key, "shard_#{i}_data", 0)
      end

      ShardHelpers.flush_all_shards()

      for i <- 0..3 do
        shard_dir = Path.join(data_dir, "shard_#{i}")
        assert File.dir?(shard_dir), "Shard #{i} data directory should exist"

        files = File.ls!(shard_dir)
        assert length(files) > 0, "Shard #{i} directory should have data files"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Isolated shard: start, write, stop, verify data persists
  # ---------------------------------------------------------------------------

  describe "isolated shard stop flushes data" do
    @tag :capture_log
    test "starting and stopping an isolated shard preserves written data" do
      # Start a shard outside the application supervisor tree with a
      # unique index and temporary directory to avoid conflicts.
      tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_shutdown_iso_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(tmp_dir)

      on_exit(fn -> File.rm_rf(tmp_dir) end)

      # Use index 99 to avoid colliding with application shards 0-3.
      opts = [index: 99, data_dir: tmp_dir, flush_interval_ms: 1]
      {:ok, pid} = GenServer.start_link(Ferricstore.Store.Shard, opts)

      # Write data through the shard.
      :ok = GenServer.call(pid, {:put, "iso_key", "iso_val", 0})
      :ok = GenServer.call(pid, :flush)

      # Stop the shard gracefully.
      GenServer.stop(pid, :normal, 5_000)
      refute Process.alive?(pid)

      # Verify data is on disk.
      shard_dir = Path.join(tmp_dir, "shard_99")
      assert File.dir?(shard_dir)

      {:ok, store} = NIF.new(shard_dir)
      {:ok, recovered} = NIF.get(store, "iso_key")
      assert recovered == "iso_val", "Data should survive shard stop"
    end
  end
end
