defmodule Ferricstore.Cluster.SnapshotStoreTest do
  @moduledoc """
  Tests for the SnapshotStore behaviour and the Local filesystem adapter.

  Covers upload, download, list, and delete operations, including edge cases
  like nested keys, missing files, listing with partial prefixes, and
  idempotent deletes.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Cluster.SnapshotStore.Local
  alias Ferricstore.Cluster.SnapshotStore.Manifest

  setup do
    base_dir = Path.join(System.tmp_dir!(), "snapshot_store_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(base_dir)
    on_exit(fn -> File.rm_rf!(base_dir) end)
    %{base_dir: base_dir, opts: [base_dir: base_dir]}
  end

  # ---------------------------------------------------------------------------
  # Local adapter: upload
  # ---------------------------------------------------------------------------

  describe "Local.upload/3" do
    test "uploads a file and stores it under the key", %{opts: opts, base_dir: base_dir} do
      {src, content} = create_temp_file("hello snapshot")

      assert :ok = Local.upload(src, "my/key.tar", opts)
      assert File.read!(Path.join(base_dir, "my/key.tar")) == content
    end

    test "creates intermediate directories for nested keys", %{opts: opts, base_dir: base_dir} do
      {src, _} = create_temp_file("nested")

      assert :ok = Local.upload(src, "a/b/c/d.tar", opts)
      assert File.exists?(Path.join(base_dir, "a/b/c/d.tar"))
    end

    test "overwrites existing key", %{opts: opts, base_dir: base_dir} do
      {src1, _} = create_temp_file("v1")
      {src2, _} = create_temp_file("v2")

      :ok = Local.upload(src1, "key.tar", opts)
      :ok = Local.upload(src2, "key.tar", opts)

      assert File.read!(Path.join(base_dir, "key.tar")) == "v2"
    end

    test "returns error when source file does not exist", %{opts: opts} do
      assert {:error, {:upload_failed, :enoent}} =
               Local.upload("/nonexistent/file.tar", "key.tar", opts)
    end
  end

  # ---------------------------------------------------------------------------
  # Local adapter: download
  # ---------------------------------------------------------------------------

  describe "Local.download/3" do
    test "downloads a previously uploaded file", %{opts: opts} do
      {src, content} = create_temp_file("download me")
      :ok = Local.upload(src, "dl/test.tar", opts)

      dest = Path.join(System.tmp_dir!(), "dl_dest_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm(dest) end)

      assert :ok = Local.download("dl/test.tar", dest, opts)
      assert File.read!(dest) == content
    end

    test "creates destination directory if missing", %{opts: opts} do
      {src, content} = create_temp_file("data")
      :ok = Local.upload(src, "x.tar", opts)

      dest_dir = Path.join(System.tmp_dir!(), "dl_nested_#{System.unique_integer([:positive])}")
      dest = Path.join(dest_dir, "sub/x.tar")
      on_exit(fn -> File.rm_rf(dest_dir) end)

      assert :ok = Local.download("x.tar", dest, opts)
      assert File.read!(dest) == content
    end

    test "returns error when key does not exist", %{opts: opts} do
      dest = Path.join(System.tmp_dir!(), "nope_#{System.unique_integer([:positive])}")

      assert {:error, {:download_failed, :enoent}} =
               Local.download("nonexistent.tar", dest, opts)
    end

    test "downloaded content matches uploaded content byte-for-byte", %{opts: opts} do
      # Binary content with non-UTF8 bytes
      content = :crypto.strong_rand_bytes(4096)
      src = write_temp_file(content)
      :ok = Local.upload(src, "binary.dat", opts)

      dest = Path.join(System.tmp_dir!(), "bin_dl_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm(dest) end)

      :ok = Local.download("binary.dat", dest, opts)
      assert File.read!(dest) == content
    end
  end

  # ---------------------------------------------------------------------------
  # Local adapter: list
  # ---------------------------------------------------------------------------

  describe "Local.list/2" do
    test "lists all keys under a prefix directory", %{opts: opts} do
      upload_content("snapshots/2026/a.tar", "a", opts)
      upload_content("snapshots/2026/b.tar", "b", opts)
      upload_content("snapshots/2025/c.tar", "c", opts)

      {:ok, keys} = Local.list("snapshots/2026", opts)
      assert Enum.sort(keys) == ["snapshots/2026/a.tar", "snapshots/2026/b.tar"]
    end

    test "returns empty list when prefix does not exist", %{opts: opts} do
      assert {:ok, []} = Local.list("nonexistent/prefix", opts)
    end

    test "lists keys with partial prefix match", %{opts: opts} do
      upload_content("snap/abc.tar", "1", opts)
      upload_content("snap/abd.tar", "2", opts)
      upload_content("snap/xyz.tar", "3", opts)

      {:ok, keys} = Local.list("snap/ab", opts)
      assert Enum.sort(keys) == ["snap/abc.tar", "snap/abd.tar"]
    end

    test "lists recursively through nested directories", %{opts: opts} do
      upload_content("root/a/1.tar", "1", opts)
      upload_content("root/a/b/2.tar", "2", opts)
      upload_content("root/a/b/c/3.tar", "3", opts)

      {:ok, keys} = Local.list("root/a", opts)

      assert Enum.sort(keys) == [
               "root/a/1.tar",
               "root/a/b/2.tar",
               "root/a/b/c/3.tar"
             ]
    end
  end

  # ---------------------------------------------------------------------------
  # Local adapter: delete
  # ---------------------------------------------------------------------------

  describe "Local.delete/2" do
    test "deletes an existing key", %{opts: opts} do
      upload_content("del/test.tar", "data", opts)
      assert :ok = Local.delete("del/test.tar", opts)

      # Verify gone
      dest = Path.join(System.tmp_dir!(), "del_check_#{System.unique_integer([:positive])}")
      assert {:error, _} = Local.download("del/test.tar", dest, opts)
    end

    test "is idempotent -- deleting nonexistent key returns :ok", %{opts: opts} do
      assert :ok = Local.delete("does/not/exist.tar", opts)
    end

    test "does not affect other keys", %{opts: opts} do
      upload_content("keep/a.tar", "keep_me", opts)
      upload_content("keep/b.tar", "delete_me", opts)

      :ok = Local.delete("keep/b.tar", opts)

      dest = Path.join(System.tmp_dir!(), "keep_check_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm(dest) end)

      assert :ok = Local.download("keep/a.tar", dest, opts)
      assert File.read!(dest) == "keep_me"
    end
  end

  # ---------------------------------------------------------------------------
  # Round-trip: upload -> list -> download -> delete -> verify
  # ---------------------------------------------------------------------------

  describe "full round-trip" do
    test "upload, list, download, delete cycle", %{opts: opts} do
      content = "round trip content #{:rand.uniform(1_000_000)}"
      {src, _} = create_temp_file(content)
      key = "roundtrip/test.tar"

      # Upload
      assert :ok = Local.upload(src, key, opts)

      # List
      {:ok, keys} = Local.list("roundtrip", opts)
      assert key in keys

      # Download and verify
      dest = Path.join(System.tmp_dir!(), "rt_dl_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm(dest) end)

      assert :ok = Local.download(key, dest, opts)
      assert File.read!(dest) == content

      # Delete
      assert :ok = Local.delete(key, opts)

      # Verify deleted
      {:ok, keys_after} = Local.list("roundtrip", opts)
      refute key in keys_after
    end
  end

  # ---------------------------------------------------------------------------
  # Manifest
  # ---------------------------------------------------------------------------

  describe "Manifest.build/3" do
    test "builds a valid manifest map" do
      entries = [
        %{
          index: 0,
          raft_index: 100,
          tar_key: "snapshots/ts/shard_0.tar.gz",
          files: [%{name: "00000.log", size: 1024, sha256: "abc123"}]
        },
        %{
          index: 1,
          raft_index: 200,
          tar_key: "snapshots/ts/shard_1.tar.gz",
          files: []
        }
      ]

      manifest = Manifest.build("2026-04-06T12:00:00Z", 2, entries)

      assert manifest["timestamp"] == "2026-04-06T12:00:00Z"
      assert manifest["shard_count"] == 2
      assert length(manifest["shards"]) == 2

      [s0, s1] = manifest["shards"]
      assert s0["index"] == 0
      assert s0["raft_index"] == 100
      assert s0["tar_key"] == "snapshots/ts/shard_0.tar.gz"
      assert [%{"name" => "00000.log", "size" => 1024, "sha256" => "abc123"}] = s0["files"]

      assert s1["index"] == 1
      assert s1["raft_index"] == 200
      assert s1["files"] == []
    end
  end

  describe "Manifest.parse/1" do
    test "parses a valid manifest JSON" do
      json =
        Jason.encode!(%{
          "timestamp" => "2026-04-06T12:00:00Z",
          "shard_count" => 1,
          "shards" => [
            %{"index" => 0, "raft_index" => 42, "tar_key" => "k", "files" => []}
          ]
        })

      assert {:ok, manifest} = Manifest.parse(json)
      assert manifest["timestamp"] == "2026-04-06T12:00:00Z"
      assert manifest["shard_count"] == 1
    end

    test "returns error for invalid JSON" do
      assert {:error, {:json_decode_error, _}} = Manifest.parse("not json")
    end

    test "returns error for valid JSON missing required keys" do
      assert {:error, :invalid_manifest_format} =
               Manifest.parse(Jason.encode!(%{"foo" => "bar"}))
    end
  end

  describe "Manifest round-trip" do
    test "build -> encode -> parse produces equivalent manifest" do
      entries = [
        %{index: 0, raft_index: 999, tar_key: "k.tar.gz", files: [%{name: "f", size: 10, sha256: "aa"}]}
      ]

      original = Manifest.build("2026-01-01T00:00:00Z", 1, entries)
      json = Jason.encode!(original)
      assert {:ok, parsed} = Manifest.parse(json)

      assert parsed == original
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp create_temp_file(content) do
    path = Path.join(System.tmp_dir!(), "snap_test_#{System.unique_integer([:positive])}")
    File.write!(path, content)
    {path, content}
  end

  defp write_temp_file(content) do
    path = Path.join(System.tmp_dir!(), "snap_test_#{System.unique_integer([:positive])}")
    File.write!(path, content)
    path
  end

  defp upload_content(key, content, opts) do
    {src, _} = create_temp_file(content)
    :ok = Local.upload(src, key, opts)
  end
end
