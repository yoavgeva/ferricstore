defmodule Ferricstore.FSTest do
  @moduledoc """
  Tests for the `Ferricstore.FS` wrapper that preserves File.* bang
  semantics while routing through our Normal-scheduler NIFs.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.FS

  setup do
    tmp = Path.join(System.tmp_dir!(), "fs_wrap_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)
    %{tmp: tmp}
  end

  describe "bang variants raise File.Error on failure" do
    test "touch! raises on missing parent", %{tmp: tmp} do
      p = Path.join([tmp, "nowhere", "file"])
      assert_raise File.Error, fn -> FS.touch!(p) end
    end

    test "mkdir_p! is idempotent + creates nested", %{tmp: tmp} do
      p = Path.join([tmp, "a", "b", "c"])
      assert :ok = FS.mkdir_p!(p)
      assert File.dir?(p)
      assert :ok = FS.mkdir_p!(p)
    end

    test "rm! raises on missing file", %{tmp: tmp} do
      assert_raise File.Error, fn -> FS.rm!(Path.join(tmp, "ghost")) end
    end

    test "ls! returns list", %{tmp: tmp} do
      File.write!(Path.join(tmp, "one"), "")
      File.write!(Path.join(tmp, "two"), "")
      assert Enum.sort(FS.ls!(tmp)) == ["one", "two"]
    end

    test "ls! raises on missing dir", %{tmp: tmp} do
      assert_raise File.Error, fn -> FS.ls!(Path.join(tmp, "ghost")) end
    end

    test "rename! raises on missing source", %{tmp: tmp} do
      a = Path.join(tmp, "ghost")
      b = Path.join(tmp, "dest")
      assert_raise File.Error, fn -> FS.rename!(a, b) end
    end
  end

  describe "rm_rf is idempotent" do
    test "missing path returns :ok", %{tmp: tmp} do
      assert :ok = FS.rm_rf(Path.join(tmp, "never-existed"))
    end

    test "removes a nested tree", %{tmp: tmp} do
      root = Path.join(tmp, "tree")
      File.mkdir_p!(Path.join([root, "a", "b"]))
      File.write!(Path.join([root, "a", "file"]), "x")

      assert :ok = FS.rm_rf!(root)
      refute File.exists?(root)
    end
  end

  describe "non-bang error-returning variants" do
    test "touch returns {:error, {:not_found, _}} for missing parent", %{tmp: tmp} do
      assert {:error, {:not_found, _}} = FS.touch(Path.join([tmp, "nope", "x"]))
    end

    test "rm returns {:error, {:not_found, _}} for missing file", %{tmp: tmp} do
      assert {:error, {:not_found, _}} = FS.rm(Path.join(tmp, "missing"))
    end

    test "exists? false for missing", %{tmp: tmp} do
      assert FS.exists?(Path.join(tmp, "nothing")) == false
    end

    test "dir? true/false", %{tmp: tmp} do
      f = Path.join(tmp, "f")
      File.write!(f, "")
      assert FS.dir?(tmp) == true
      assert FS.dir?(f) == false
    end
  end
end
