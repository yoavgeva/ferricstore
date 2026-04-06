defmodule Ferricstore.Cluster.SnapshotStore.Local do
  @moduledoc """
  Local filesystem adapter for `Ferricstore.Cluster.SnapshotStore`.

  Stores snapshot objects as plain files under a configurable base directory.
  Useful for development, testing, and single-node deployments where an
  external object store is unnecessary.

  ## Required options

    * `:base_dir` -- root directory for stored objects. Keys are mapped to
      paths relative to this directory (e.g. key `"a/b.tar"` maps to
      `base_dir/a/b.tar`).

  ## Example

      opts = [base_dir: "/tmp/ferricstore_snapshots"]
      :ok = Local.upload("/data/shard_0.tar", "snapshots/latest/shard_0.tar", opts)
  """

  @behaviour Ferricstore.Cluster.SnapshotStore

  @impl true
  @spec upload(binary(), binary(), keyword()) :: :ok | {:error, term()}
  def upload(local_path, key, opts) do
    base = Keyword.fetch!(opts, :base_dir)
    dest = Path.join(base, key)
    File.mkdir_p!(Path.dirname(dest))

    case File.cp(local_path, dest) do
      :ok -> :ok
      {:error, reason} -> {:error, {:upload_failed, reason}}
    end
  end

  @impl true
  @spec download(binary(), binary(), keyword()) :: :ok | {:error, term()}
  def download(key, dest_path, opts) do
    base = Keyword.fetch!(opts, :base_dir)
    source = Path.join(base, key)

    File.mkdir_p!(Path.dirname(dest_path))

    case File.cp(source, dest_path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:download_failed, reason}}
    end
  end

  @impl true
  @spec list(binary(), keyword()) :: {:ok, [binary()]} | {:error, term()}
  def list(prefix, opts) do
    base = Keyword.fetch!(opts, :base_dir)
    search_dir = Path.join(base, prefix)

    if File.dir?(search_dir) do
      {:ok, list_files_recursive(search_dir, base)}
    else
      # The prefix might be a partial path. Walk the parent and filter.
      parent = Path.dirname(search_dir)
      _basename_prefix = Path.basename(prefix)

      if File.dir?(parent) do
        keys =
          list_files_recursive(parent, base)
          |> Enum.filter(&String.starts_with?(&1, prefix))

        {:ok, keys}
      else
        {:ok, []}
      end
    end
  end

  @impl true
  @spec delete(binary(), keyword()) :: :ok | {:error, term()}
  def delete(key, opts) do
    base = Keyword.fetch!(opts, :base_dir)
    path = Path.join(base, key)

    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:delete_failed, reason}}
    end
  end

  # Recursively lists all files under `dir`, returning keys relative to `base`.
  @spec list_files_recursive(binary(), binary()) :: [binary()]
  defp list_files_recursive(dir, base) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn entry ->
          full = Path.join(dir, entry)

          if File.dir?(full) do
            list_files_recursive(full, base)
          else
            [Path.relative_to(full, base)]
          end
        end)

      {:error, _} ->
        []
    end
  end
end
