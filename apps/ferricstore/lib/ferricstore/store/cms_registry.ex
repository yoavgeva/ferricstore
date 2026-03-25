defmodule Ferricstore.Store.CmsRegistry do
  @moduledoc """
  Per-shard ETS cache for mmap-backed Count-Min Sketch NIF resources.

  Each shard owns one ETS table (`:cms_reg_N`) that maps Redis keys to
  `{resource, metadata}` tuples. The `resource` is a NIF `ResourceArc` handle
  to a memory-mapped CMS file; `metadata` is a map containing `:width` and
  `:depth` (the original parameters).

  ## Lifecycle

    1. **Create** -- `CMS.INITBYDIM`/`CMS.INITBYPROB` calls `register/4`.
    2. **Lookup** -- `CMS.INCRBY`, `CMS.QUERY`, etc. call `lookup/2`. On a
       cache miss the file is re-opened via `NIF.cms_open_file/1` and cached.
    3. **Delete** -- `DEL` on a CMS key calls `delete/2` which invokes
       `NIF.cms_close/1` and removes the ETS entry.
    4. **Recovery** -- On shard restart, `recover/2` scans the `prob/shard_N/`
       directory for `.cms` files and re-opens them.

  ## Path convention

  CMS files live at `data_dir/prob/shard_N/KEY.cms` where `KEY` is the
  sanitised Redis key.
  """

  alias Ferricstore.Bitcask.NIF

  require Logger

  # -------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------

  @type metadata :: %{width: pos_integer(), depth: pos_integer()}
  @type entry :: {reference(), metadata()}

  # -------------------------------------------------------------------
  # Table management
  # -------------------------------------------------------------------

  @spec create_table(non_neg_integer()) :: atom()
  def create_table(index) do
    name = table_name(index)

    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, [:set, :public, :named_table, {:read_concurrency, true}])

      _ref ->
        # Explicitly close all cached mmap resources before clearing the table.
        close_all(name)
        :ets.delete_all_objects(name)
        name
    end
  end

  @doc """
  Closes all cached CMS resources in the given table.
  """
  @spec close_all(atom()) :: :ok
  def close_all(table) do
    :ets.foldl(
      fn {_key, resource, _meta}, :ok ->
        try do
          NIF.cms_close(resource)
        rescue
          _ -> :ok
        end
        :ok
      end,
      :ok,
      table
    )
  end

  @spec table_name(non_neg_integer()) :: atom()
  def table_name(index), do: :"cms_reg_#{index}"

  # -------------------------------------------------------------------
  # Registration / lookup / delete
  # -------------------------------------------------------------------

  @spec register(non_neg_integer(), binary(), reference(), metadata()) :: true
  def register(index, key, resource, metadata) do
    :ets.insert(table_name(index), {key, resource, metadata})
  end

  @spec lookup(non_neg_integer(), binary()) :: {reference(), metadata()} | nil
  def lookup(index, key) do
    case :ets.lookup(table_name(index), key) do
      [{^key, resource, metadata}] -> {resource, metadata}
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @spec open_or_lookup(non_neg_integer(), binary(), binary(), metadata() | nil) ::
          {:ok, reference(), metadata()} | :not_found
  def open_or_lookup(index, key, data_dir, metadata_fallback \\ nil) do
    case lookup(index, key) do
      {resource, metadata} ->
        {:ok, resource, metadata}

      nil ->
        path = cms_path(data_dir, index, key)

        if File.exists?(path) do
          case NIF.cms_open_file(path) do
            {:ok, resource} ->
              meta = metadata_fallback || derive_metadata(resource)
              register(index, key, resource, meta)
              {:ok, resource, meta}

            {:error, reason} ->
              Logger.warning("CmsRegistry: failed to reopen #{path}: #{reason}")
              :not_found
          end
        else
          :not_found
        end
    end
  end

  @spec delete(non_neg_integer(), binary()) :: :ok
  def delete(index, key) do
    case lookup(index, key) do
      {resource, _meta} ->
        NIF.cms_close(resource)
        :ets.delete(table_name(index), key)
        :ok

      nil ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  # -------------------------------------------------------------------
  # Recovery (shard restart)
  # -------------------------------------------------------------------

  @spec recover(binary(), non_neg_integer()) :: non_neg_integer()
  def recover(data_dir, index) do
    dir = prob_dir(data_dir, index)

    case File.ls(dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".cms"))
        |> Enum.reduce(0, fn filename, count ->
          key = filename |> String.trim_trailing(".cms")
          path = Path.join(dir, filename)

          case NIF.cms_open_file(path) do
            {:ok, resource} ->
              meta = derive_metadata(resource)
              register(index, key, resource, meta)
              count + 1

            {:error, reason} ->
              Logger.warning("CmsRegistry: failed to recover #{path}: #{reason}")
              count
          end
        end)

      {:error, :enoent} ->
        0
    end
  end

  # -------------------------------------------------------------------
  # Path helpers
  # -------------------------------------------------------------------

  @spec cms_path(binary(), non_neg_integer(), binary()) :: binary()
  def cms_path(data_dir, index, key) do
    safe_key = sanitize_key(key)
    Path.join([data_dir, "prob", "shard_#{index}", "#{safe_key}.cms"])
  end

  @doc false
  @spec prob_dir(binary(), non_neg_integer()) :: binary()
  def prob_dir(data_dir, index) do
    Path.join([data_dir, "prob", "shard_#{index}"])
  end

  @spec sanitize_key(binary()) :: binary()
  def sanitize_key(key) do
    String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  @spec derive_metadata(reference()) :: metadata()
  defp derive_metadata(resource) do
    {:ok, {width, depth, _count}} = NIF.cms_info(resource)
    %{width: width, depth: depth}
  end
end
