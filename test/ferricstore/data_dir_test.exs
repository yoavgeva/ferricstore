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
  # Dedicated / prob / vectors directory preparation
  # ------------------------------------------------------------------

  describe "dedicated directory preparation" do
    test "dedicated/shard_N/ directories exist and are writable" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      for i <- 0..3 do
        path = Path.join([dir, "dedicated", "shard_#{i}"])
        assert File.dir?(path), "dedicated/shard_#{i}/ should exist"

        # Verify writable by creating a temp file.
        probe = Path.join(path, "probe.tmp")
        assert :ok = File.write(probe, "test")
        assert {:ok, "test"} = File.read(probe)
        File.rm!(probe)
      end
    end

    test "prob/shard_N/ directories exist and are writable" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      for i <- 0..3 do
        path = Path.join([dir, "prob", "shard_#{i}"])
        assert File.dir?(path), "prob/shard_#{i}/ should exist"

        probe = Path.join(path, "probe.tmp")
        assert :ok = File.write(probe, "test")
        assert {:ok, "test"} = File.read(probe)
        File.rm!(probe)
      end
    end

    test "vectors/shard_N/ directories exist and are writable" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      for i <- 0..3 do
        path = Path.join([dir, "vectors", "shard_#{i}"])
        assert File.dir?(path), "vectors/shard_#{i}/ should exist"

        probe = Path.join(path, "probe.tmp")
        assert :ok = File.write(probe, "test")
        assert {:ok, "test"} = File.read(probe)
        File.rm!(probe)
      end
    end

    test "raft/shard_N/ directories exist and are writable" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      for i <- 0..3 do
        path = Path.join([dir, "raft", "shard_#{i}"])
        assert File.dir?(path), "raft/shard_#{i}/ should exist"

        probe = Path.join(path, "probe.tmp")
        assert :ok = File.write(probe, "test")
        assert {:ok, "test"} = File.read(probe)
        File.rm!(probe)
      end
    end

    test "registry/ is writable (non-sharded)" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      path = Path.join(dir, "registry")
      assert File.dir?(path)

      probe = Path.join(path, "probe.tmp")
      assert :ok = File.write(probe, "test")
      assert {:ok, "test"} = File.read(probe)
      File.rm!(probe)
    end

    test "hints/ is writable (non-sharded)" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 4)

      path = Path.join(dir, "hints")
      assert File.dir?(path)

      probe = Path.join(path, "probe.tmp")
      assert :ok = File.write(probe, "test")
      assert {:ok, "test"} = File.read(probe)
      File.rm!(probe)
    end
  end

  # ------------------------------------------------------------------
  # Backward compatibility: mixed legacy and canonical paths
  # ------------------------------------------------------------------

  describe "backward compatibility: mixed legacy and canonical" do
    test "some shards can use legacy paths while others use canonical" do
      dir = tmp_dir()

      # Create legacy directories for shards 0 and 2 only.
      File.mkdir_p!(Path.join(dir, "shard_0"))
      File.mkdir_p!(Path.join(dir, "shard_2"))

      :ok = DataDir.ensure_layout!(dir, 4)

      # Legacy shards use the old path.
      assert DataDir.shard_data_path(dir, 0) == Path.join(dir, "shard_0")
      assert DataDir.shard_data_path(dir, 2) == Path.join(dir, "shard_2")

      # Non-legacy shards use the new canonical path.
      assert DataDir.shard_data_path(dir, 1) == Path.join([dir, "data", "shard_1"])
      assert DataDir.shard_data_path(dir, 3) == Path.join([dir, "data", "shard_3"])
    end

    test "legacy path detection does not confuse a file with a directory" do
      dir = tmp_dir()
      File.mkdir_p!(dir)

      # Create a regular FILE named shard_0 (not a directory).
      File.write!(Path.join(dir, "shard_0"), "not a directory")

      :ok = DataDir.ensure_layout!(dir, 4)

      # File.dir? returns false for regular files, so canonical path is used.
      assert DataDir.shard_data_path(dir, 0) == Path.join([dir, "data", "shard_0"])
    end

    test "existing data in legacy shard_N/ dirs is found by NIF.new" do
      dir = tmp_dir()
      legacy = Path.join(dir, "shard_0")
      File.mkdir_p!(legacy)

      # Write a value via NIF using the legacy path.
      {:ok, store} = Ferricstore.Bitcask.NIF.new(legacy)
      :ok = Ferricstore.Bitcask.NIF.put(store, "legacy_key", "legacy_val", 0)

      # Now create the full layout. The legacy dir still exists.
      :ok = DataDir.ensure_layout!(dir, 4)

      # shard_data_path should return the legacy path.
      resolved = DataDir.shard_data_path(dir, 0)
      assert resolved == legacy

      # Re-open the store via resolved path: data should still be there.
      {:ok, store2} = Ferricstore.Bitcask.NIF.new(resolved)
      {:ok, val} = Ferricstore.Bitcask.NIF.get(store2, "legacy_key")
      assert val == "legacy_val"
    end
  end

  # ------------------------------------------------------------------
  # Bitcask files in new location
  # ------------------------------------------------------------------

  describe "Bitcask files in new canonical location" do
    test "after writes, .log files appear in data/shard_N/" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 2)

      shard_path = DataDir.shard_data_path(dir, 0)
      {:ok, store} = Ferricstore.Bitcask.NIF.new(shard_path)

      for i <- 1..10 do
        :ok = Ferricstore.Bitcask.NIF.put(store, "key_#{i}", "value_#{i}", 0)
      end

      {:ok, files} = File.ls(shard_path)
      log_files = Enum.filter(files, &String.ends_with?(&1, ".log"))

      assert length(log_files) >= 1,
             "Expected at least one .log file in #{shard_path}, got: #{inspect(files)}"
    end

    test ".log files follow the {file_id:020}.log naming pattern" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 2)

      shard_path = DataDir.shard_data_path(dir, 0)
      {:ok, store} = Ferricstore.Bitcask.NIF.new(shard_path)

      :ok = Ferricstore.Bitcask.NIF.put(store, "naming_test", "value", 0)

      {:ok, files} = File.ls(shard_path)
      log_files = Enum.filter(files, &String.ends_with?(&1, ".log"))

      for log_file <- log_files do
        stem = String.replace_suffix(log_file, ".log", "")
        # Stem should be a 20-digit zero-padded integer.
        assert String.length(stem) == 20, "Expected 20-char stem, got #{stem}"
        assert {_id, ""} = Integer.parse(stem), "Stem should be a valid integer: #{stem}"
      end
    end

    test "hint files are created after write_hint NIF call" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 2)

      shard_path = DataDir.shard_data_path(dir, 0)
      {:ok, store} = Ferricstore.Bitcask.NIF.new(shard_path)

      for i <- 1..5 do
        :ok = Ferricstore.Bitcask.NIF.put(store, "hint_key_#{i}", "value_#{i}", 0)
      end

      :ok = Ferricstore.Bitcask.NIF.write_hint(store)

      {:ok, files} = File.ls(shard_path)
      hint_files = Enum.filter(files, &String.ends_with?(&1, ".hint"))

      assert length(hint_files) >= 1,
             "Expected at least one .hint file after write_hint, got: #{inspect(files)}"
    end

    test "no .log files leak into data/ parent directory" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 2)

      shard_path = DataDir.shard_data_path(dir, 0)
      {:ok, store} = Ferricstore.Bitcask.NIF.new(shard_path)

      :ok = Ferricstore.Bitcask.NIF.put(store, "leak_test", "value", 0)

      # The parent data/ directory should only contain shard subdirectories.
      data_parent = Path.join(dir, "data")
      {:ok, entries} = File.ls(data_parent)

      log_or_hint_in_parent =
        Enum.filter(entries, fn name ->
          String.ends_with?(name, ".log") or String.ends_with?(name, ".hint")
        end)

      assert log_or_hint_in_parent == [],
             "No .log/.hint files should appear directly in data/, " <>
               "got: #{inspect(log_or_hint_in_parent)}"
    end
  end

  # ------------------------------------------------------------------
  # Cross-shard isolation
  # ------------------------------------------------------------------

  describe "cross-shard directory isolation" do
    test "data written to shard 0 does not appear in shard 1 directory" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 2)

      path_0 = DataDir.shard_data_path(dir, 0)
      path_1 = DataDir.shard_data_path(dir, 1)

      {:ok, store0} = Ferricstore.Bitcask.NIF.new(path_0)
      {:ok, store1} = Ferricstore.Bitcask.NIF.new(path_1)

      # Write to shard 0 only.
      for i <- 1..20 do
        :ok = Ferricstore.Bitcask.NIF.put(store0, "shard0_key_#{i}", "value_#{i}", 0)
      end

      # Shard 1's directory should have at most one .log file (the active file
      # created on NIF.new), and no keys from shard 0.
      {:ok, val} = Ferricstore.Bitcask.NIF.get(store1, "shard0_key_1")
      assert val == nil, "shard 0 key should not be visible in shard 1"

      # Verify the file sizes differ: shard 0 has data, shard 1 does not.
      {:ok, {total_0, _live_0, _dead_0, _fc_0, keys_0, _frag_0}} =
        Ferricstore.Bitcask.NIF.shard_stats(store0)

      {:ok, {_total_1, _live_1, _dead_1, _fc_1, keys_1, _frag_1}} =
        Ferricstore.Bitcask.NIF.shard_stats(store1)

      assert keys_0 == 20
      assert keys_1 == 0
      assert total_0 > 0
    end

    test "compaction on shard 0 does not affect shard 1 files" do
      dir = tmp_dir()
      :ok = DataDir.ensure_layout!(dir, 2)

      path_0 = DataDir.shard_data_path(dir, 0)
      path_1 = DataDir.shard_data_path(dir, 1)

      {:ok, store0} = Ferricstore.Bitcask.NIF.new(path_0)
      {:ok, store1} = Ferricstore.Bitcask.NIF.new(path_1)

      # Write to both shards.
      for i <- 1..10 do
        :ok = Ferricstore.Bitcask.NIF.put(store0, "s0_key_#{i}", "val", 0)
        :ok = Ferricstore.Bitcask.NIF.put(store1, "s1_key_#{i}", "val", 0)
      end

      # Overwrite shard 0 keys to create fragmentation.
      for _round <- 1..15 do
        for i <- 1..10 do
          :ok = Ferricstore.Bitcask.NIF.put(store0, "s0_key_#{i}", String.duplicate("x", 50), 0)
        end
      end

      # Snapshot shard 1 files before compaction.
      {:ok, files_before_1} = File.ls(path_1)

      # Compact shard 0.
      {:ok, file_sizes_0} = Ferricstore.Bitcask.NIF.file_sizes(store0)
      all_fids_0 = Enum.map(file_sizes_0, fn {fid, _} -> fid end)

      if length(all_fids_0) > 1 do
        active_fid = Enum.max(all_fids_0)
        merge_fids = Enum.reject(all_fids_0, &(&1 == active_fid))
        {:ok, {_w, _d, _r}} = Ferricstore.Bitcask.NIF.run_compaction(store0, merge_fids)
      end

      # Shard 1 files should be unchanged.
      {:ok, files_after_1} = File.ls(path_1)
      assert Enum.sort(files_before_1) == Enum.sort(files_after_1),
             "Shard 1 files should not change after shard 0 compaction"

      # Shard 1 data should still be readable.
      for i <- 1..10 do
        {:ok, val} = Ferricstore.Bitcask.NIF.get(store1, "s1_key_#{i}")
        assert val == "val", "s1_key_#{i} should be intact after shard 0 compaction"
      end
    end
  end

  # ------------------------------------------------------------------
  # Application startup: per-shard directories under all sharded dirs
  # ------------------------------------------------------------------

  describe "application startup: full sharded directory tree" do
    test "dedicated/shard_N/ dirs exist for all shards after app start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert File.dir?(Path.join([data_dir, "dedicated", "shard_#{i}"])),
               "dedicated/shard_#{i}/ should exist after app start"
      end
    end

    test "prob/shard_N/ dirs exist for all shards after app start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert File.dir?(Path.join([data_dir, "prob", "shard_#{i}"])),
               "prob/shard_#{i}/ should exist after app start"
      end
    end

    test "vectors/shard_N/ dirs exist for all shards after app start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert File.dir?(Path.join([data_dir, "vectors", "shard_#{i}"])),
               "vectors/shard_#{i}/ should exist after app start"
      end
    end

    test "raft/shard_N/ dirs exist for all shards after app start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert File.dir?(Path.join([data_dir, "raft", "shard_#{i}"])),
               "raft/shard_#{i}/ should exist after app start"
      end
    end

    test "registry/ exists after app start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      assert File.dir?(Path.join(data_dir, "registry"))
    end

    test "hints/ exists after app start" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      assert File.dir?(Path.join(data_dir, "hints"))
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
