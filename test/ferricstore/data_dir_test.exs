defmodule Ferricstore.DataDirTest do
  @moduledoc """
  Tests for `Ferricstore.DataDir` -- on-disk directory layout creation and
  backward-compatible shard path resolution.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.DataDir

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "datadir_test_#{:rand.uniform(9_999_999)}")
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  # ------------------------------------------------------------------
  # ensure_layout!/2
  # ------------------------------------------------------------------

  describe "ensure_layout!/2" do
    test "creates all top-level directories" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      for sub <- ~w(data dedicated prob vectors raft registry hints) do
        assert File.dir?(Path.join(dir, sub)),
               "Expected #{sub}/ to exist under #{dir}"
      end
    end

    test "creates per-shard subdirectories under sharded dirs" do
      dir = tmp_dir()
      shard_count = 8
      :ok = DataDir.ensure_layout!(dir, shard_count)

      for parent <- ~w(data dedicated prob vectors raft), i <- 0..(shard_count - 1) do
        shard_dir = Path.join([dir, parent, "shard_#{i}"])

        assert File.dir?(shard_dir),
               "Expected #{parent}/shard_#{i}/ to exist"
      end
    end

    test "registry and hints do not have per-shard subdirectories" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      for parent <- ~w(registry hints) do
        parent_path = Path.join(dir, parent)
        assert File.dir?(parent_path)

        {:ok, entries} = File.ls(parent_path)
        assert entries == [], "#{parent}/ should be empty, got: #{inspect(entries)}"
      end
    end

    test "is idempotent -- calling twice does not fail" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)
      :ok = DataDir.ensure_layout!(dir, 4)

      assert File.dir?(Path.join([dir, "data", "shard_0"]))
    end

    test "defaults to 4 shards when shard_count is omitted" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir)

      for i <- 0..3 do
        assert File.dir?(Path.join([dir, "data", "shard_#{i}"]))
      end

      refute File.dir?(Path.join([dir, "data", "shard_4"]))
    end

    test "creates the root data_dir if it does not exist" do
      dir = tmp_dir()
      nested = Path.join(dir, "nested/deep")
      :ok = DataDir.ensure_layout!(nested, 2)

      assert File.dir?(nested)
      assert File.dir?(Path.join([nested, "data", "shard_0"]))
    end
  end

  # ------------------------------------------------------------------
  # shard_data_path/2
  # ------------------------------------------------------------------

  describe "shard_data_path/2" do
    test "returns canonical data/shard_N path for fresh install" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      path = DataDir.shard_data_path(dir, 0)
      assert path == Path.join([dir, "data", "shard_0"])
    end

    test "returns legacy shard_N path when legacy directory exists" do
      dir = tmp_dir()
      legacy = Path.join(dir, "shard_2")
      File.mkdir_p!(legacy)

      # Also create the canonical layout so both exist
      :ok = DataDir.ensure_layout!(dir, 4)

      path = DataDir.shard_data_path(dir, 2)
      assert path == legacy
    end

    test "prefers legacy path even when canonical path also exists" do
      dir = tmp_dir()
      legacy = Path.join(dir, "shard_0")
      File.mkdir_p!(legacy)

      :ok = DataDir.ensure_layout!(dir, 4)

      # Both data_dir/shard_0 and data_dir/data/shard_0 exist;
      # legacy takes precedence for backward compatibility.
      path = DataDir.shard_data_path(dir, 0)
      assert path == legacy
    end

    test "uses canonical path when legacy does not exist" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      # No legacy shard_1/ directory
      refute File.dir?(Path.join(dir, "shard_1"))

      path = DataDir.shard_data_path(dir, 1)
      assert path == Path.join([dir, "data", "shard_1"])
    end

    test "works correctly for all shard indices" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 8)

      for i <- 0..7 do
        path = DataDir.shard_data_path(dir, i)
        assert path == Path.join([dir, "data", "shard_#{i}"])
      end
    end
  end

  # ------------------------------------------------------------------
  # Integration: application startup creates layout
  # ------------------------------------------------------------------

  describe "application startup integration" do
    test "data_dir has the full layout after application start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)

      # The application is already started by the test harness.
      # Verify that ensure_layout! was called during startup.
      for sub <- ~w(data dedicated prob vectors raft registry hints) do
        assert File.dir?(Path.join(data_dir, sub)),
               "Expected #{sub}/ to exist under #{data_dir} after app startup"
      end
    end

    test "per-shard directories exist under data/ after application start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert File.dir?(Path.join([data_dir, "data", "shard_#{i}"])),
               "Expected data/shard_#{i}/ under #{data_dir}"
      end
    end

    test "shards are alive and functional after layout creation" do
      # Verify that the layout creation did not break shard initialization.
      for i <- 0..3 do
        name = :"Ferricstore.Store.Shard.#{i}"
        pid = Process.whereis(name)
        assert is_pid(pid), "Shard #{i} should be registered"
        assert Process.alive?(pid), "Shard #{i} should be alive"
      end
    end
  end

  # ------------------------------------------------------------------
  # top_level_dirs/0
  # ------------------------------------------------------------------

  describe "top_level_dirs/0" do
    test "returns the expected list" do
      dirs = DataDir.top_level_dirs()
      assert "data" in dirs
      assert "dedicated" in dirs
      assert "prob" in dirs
      assert "vectors" in dirs
      assert "raft" in dirs
      assert "registry" in dirs
      assert "hints" in dirs
      assert length(dirs) == 7
    end
  end
end
