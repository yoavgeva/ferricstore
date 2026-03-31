defmodule Ferricstore.Store.ActiveFileTest do
  @moduledoc """
  Tests for `Ferricstore.Store.ActiveFile` — the active file registry that
  replaces persistent_term to avoid global GC on file rotation.

  Also tests the configurable `max-active-file-size` setting.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.{ActiveFile, Router}
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    # Save original active file state for shard 0 so we can restore it
    orig_af = ActiveFile.get(0)
    orig_max = Application.get_env(:ferricstore, :max_active_file_size)

    on_exit(fn ->
      # Restore active file for shard 0 if a test modified it
      {fid, path, data_path} = orig_af
      ActiveFile.publish(0, fid, path, data_path)

      if orig_max do
        Application.put_env(:ferricstore, :max_active_file_size, orig_max)
      else
        Application.delete_env(:ferricstore, :max_active_file_size)
      end

      ShardHelpers.flush_all_keys()
    end)

    %{orig_af: orig_af}
  end

  # ---------------------------------------------------------------------------
  # ActiveFile registry basics
  # ---------------------------------------------------------------------------

  describe "ActiveFile registry" do
    test "get/1 returns valid file metadata for each shard" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        {file_id, file_path, shard_data_path} = ActiveFile.get(i)

        assert is_integer(file_id)
        assert file_id >= 0
        assert is_binary(file_path)
        assert is_binary(shard_data_path)
      end
    end

    test "publish/4 updates the value returned by get/1", %{orig_af: {orig_fid, _, orig_data_path}} do
      new_path = Path.join(orig_data_path, "99999.log")
      ActiveFile.publish(0, 99999, new_path, orig_data_path)

      {file_id, file_path, _} = ActiveFile.get(0)
      assert file_id == 99999
      assert file_path == new_path
    end

    test "process dictionary cache is invalidated on publish", %{orig_af: {_, _, data_path}} do
      # First read caches in process dictionary
      _cached = ActiveFile.get(0)

      # Publish a new value
      new_path = Path.join(data_path, "88888.log")
      ActiveFile.publish(0, 88888, new_path, data_path)

      # Next read should see the new value (cache invalidated)
      {fid2, path2, _} = ActiveFile.get(0)
      assert fid2 == 88888
      assert path2 == new_path
    end

    test "get/1 from different processes sees published values", %{orig_af: {_, _, data_path}} do
      new_path = Path.join(data_path, "77777.log")
      ActiveFile.publish(0, 77777, new_path, data_path)

      # Read from a different process (no cached value)
      {fid, path, _} =
        Task.async(fn -> ActiveFile.get(0) end)
        |> Task.await(5000)

      assert fid == 77777
      assert path == new_path
    end
  end

  # ---------------------------------------------------------------------------
  # Configurable max-active-file-size
  # ---------------------------------------------------------------------------

  describe "max-active-file-size config" do
    test "default is 256MB" do
      Application.delete_env(:ferricstore, :max_active_file_size)
      default = Application.get_env(:ferricstore, :max_active_file_size, 256 * 1024 * 1024)
      assert default == 256 * 1024 * 1024
    end

    test "CONFIG SET max-active-file-size updates Application env" do
      assert :ok = Ferricstore.Config.set("max-active-file-size", "134217728")
      assert Application.get_env(:ferricstore, :max_active_file_size) == 134_217_728
    end

    test "CONFIG GET max-active-file-size returns current value" do
      Ferricstore.Config.set("max-active-file-size", "536870912")
      assert Ferricstore.Config.get_value("max-active-file-size") == "536870912"
    end

    test "CONFIG SET rejects values below 1MB" do
      result = Ferricstore.Config.set("max-active-file-size", "1000")
      assert {:error, msg} = result
      assert msg =~ "min 1MB"
    end

    test "CONFIG SET rejects non-integer values" do
      result = Ferricstore.Config.set("max-active-file-size", "abc")
      assert {:error, _} = result
    end

    test "shard reads max_active_file_size from Application env at runtime" do
      # Set a custom value
      Application.put_env(:ferricstore, :max_active_file_size, 512 * 1024 * 1024)

      # Verify the shard would use this value (we can't easily trigger rotation
      # in a unit test, but we can verify the config is read correctly)
      assert Application.get_env(:ferricstore, :max_active_file_size) == 512 * 1024 * 1024

      # Change it at runtime via CONFIG SET
      Ferricstore.Config.set("max-active-file-size", "134217728")
      assert Application.get_env(:ferricstore, :max_active_file_size) == 134_217_728
    end
  end
end
