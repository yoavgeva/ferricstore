defmodule Ferricstore.Merge.ManifestTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Merge.Manifest

  setup do
    dir = Path.join(System.tmp_dir!(), "manifest_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf!(dir)
    end)

    %{dir: dir}
  end

  describe "write/2" do
    test "writes a manifest file that can be read back", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1, 2, 3]}
      assert :ok = Manifest.write(dir, plan)
      assert {:ok, read_plan} = Manifest.read(dir)
      assert read_plan.shard_index == 0
      assert read_plan.input_file_ids == [1, 2, 3]
      assert is_integer(read_plan.started_at)
      assert read_plan.version == 1
    end

    test "overwrites existing manifest", %{dir: dir} do
      plan1 = %{shard_index: 0, input_file_ids: [1, 2]}
      plan2 = %{shard_index: 0, input_file_ids: [3, 4, 5]}

      assert :ok = Manifest.write(dir, plan1)
      assert :ok = Manifest.write(dir, plan2)

      assert {:ok, read_plan} = Manifest.read(dir)
      assert read_plan.input_file_ids == [3, 4, 5]
    end

    test "stores started_at timestamp", %{dir: dir} do
      before = System.system_time(:millisecond)
      plan = %{shard_index: 1, input_file_ids: [10]}
      assert :ok = Manifest.write(dir, plan)
      after_write = System.system_time(:millisecond)

      assert {:ok, read_plan} = Manifest.read(dir)
      assert read_plan.started_at >= before
      assert read_plan.started_at <= after_write
    end
  end

  describe "read/1" do
    test "returns :none when no manifest exists", %{dir: dir} do
      assert :none = Manifest.read(dir)
    end

    test "returns {:error, :corrupt_manifest} for invalid binary", %{dir: dir} do
      path = Path.join(dir, "merge_manifest.bin")
      File.write!(path, "not a valid erlang term")
      assert {:error, :corrupt_manifest} = Manifest.read(dir)
    end
  end

  describe "delete/1" do
    test "removes the manifest file", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1]}
      assert :ok = Manifest.write(dir, plan)
      assert Manifest.exists?(dir)

      assert :ok = Manifest.delete(dir)
      refute Manifest.exists?(dir)
      assert :none = Manifest.read(dir)
    end

    test "returns :ok when no manifest exists", %{dir: dir} do
      assert :ok = Manifest.delete(dir)
    end
  end

  describe "exists?/1" do
    test "returns false when no manifest", %{dir: dir} do
      refute Manifest.exists?(dir)
    end

    test "returns true after write", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1]}
      assert :ok = Manifest.write(dir, plan)
      assert Manifest.exists?(dir)
    end
  end

  describe "recover_if_needed/2" do
    test "returns :ok when no manifest exists", %{dir: dir} do
      assert :ok = Manifest.recover_if_needed(dir, 0)
    end

    test "cleans up manifest and partial output files", %{dir: dir} do
      # Simulate input files.
      File.write!(Path.join(dir, "00000000000000000001.log"), "input1")
      File.write!(Path.join(dir, "00000000000000000002.log"), "input2")

      # Simulate partial output from a crashed merge (file_id > max input).
      File.write!(Path.join(dir, "00000000000000000003.log"), "partial_output")
      File.write!(Path.join(dir, "00000000000000000003.hint"), "partial_hint")

      # Write manifest indicating files 1 and 2 were being merged.
      plan = %{shard_index: 0, input_file_ids: [1, 2]}
      assert :ok = Manifest.write(dir, plan)

      # Run recovery.
      assert :ok = Manifest.recover_if_needed(dir, 0)

      # Manifest should be deleted.
      refute Manifest.exists?(dir)

      # Input files should still exist.
      assert File.exists?(Path.join(dir, "00000000000000000001.log"))
      assert File.exists?(Path.join(dir, "00000000000000000002.log"))

      # Partial output files should be cleaned up.
      refute File.exists?(Path.join(dir, "00000000000000000003.log"))
      refute File.exists?(Path.join(dir, "00000000000000000003.hint"))
    end

    test "handles corrupt manifest gracefully", %{dir: dir} do
      path = Path.join(dir, "merge_manifest.bin")
      File.write!(path, "garbage data that is not a valid erlang term")

      assert :ok = Manifest.recover_if_needed(dir, 0)
      refute Manifest.exists?(dir)
    end

    test "leaves unrelated files untouched during recovery", %{dir: dir} do
      # Input file.
      File.write!(Path.join(dir, "00000000000000000005.log"), "input")

      # File that is NOT greater than max input id (should be untouched).
      File.write!(Path.join(dir, "00000000000000000003.log"), "older_file")

      plan = %{shard_index: 0, input_file_ids: [5]}
      assert :ok = Manifest.write(dir, plan)

      assert :ok = Manifest.recover_if_needed(dir, 0)

      # Both files should still exist (only files > max_input_id=5 are removed).
      assert File.exists?(Path.join(dir, "00000000000000000005.log"))
      assert File.exists?(Path.join(dir, "00000000000000000003.log"))
    end
  end
end
