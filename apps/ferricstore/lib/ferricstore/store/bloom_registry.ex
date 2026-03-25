defmodule Ferricstore.Store.BloomRegistry do
  @moduledoc """
  Per-shard ETS cache for mmap-backed Bloom filter NIF resources.

  Each shard owns one ETS table (`:bloom_reg_N`) that maps Redis keys to
  `{resource, metadata}` tuples. The `resource` is a NIF `ResourceArc` handle
  to a memory-mapped Bloom filter file; `metadata` is a map containing
  `:capacity` and `:error_rate` (the original parameters from `BF.RESERVE`).

  ## Lifecycle

    1. **Create** -- `BF.RESERVE` calls `register/4` which writes the
       NIF resource + metadata into ETS.
    2. **Lookup** -- `BF.ADD`, `BF.EXISTS`, etc. call `lookup/2`. On a
       cache miss the file is re-opened via `NIF.bloom_open/1` and cached.
    3. **Delete** -- `DEL` on a bloom key calls `delete/2` which invokes
       `NIF.bloom_delete/1` (munmap + unlink) and removes the ETS entry.
    4. **Recovery** -- On shard restart, `recover/2` scans the `prob/shard_N/`
       directory for `.bloom` files and re-opens them.

  ## Path convention

  Bloom files live at `data_dir/prob/shard_N/KEY.bloom` where `KEY` is the
  sanitised Redis key (non-alphanumeric characters replaced with `_`).
  """

  alias Ferricstore.Bitcask.NIF

  require Logger

  # -------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------

  @type metadata :: %{capacity: pos_integer(), error_rate: float()}
  @type entry :: {reference(), metadata()}

  # -------------------------------------------------------------------
  # Table management
  # -------------------------------------------------------------------

  @doc """
  Creates (or clears) the bloom registry ETS table for the given shard index.

  Returns the table name atom.
  """
  @spec create_table(non_neg_integer()) :: atom()
  def create_table(index) do
    name = table_name(index)

    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, [:set, :public, :named_table, {:read_concurrency, true}])

      _ref ->
        # Explicitly close all cached mmap resources before clearing the table.
        # This prevents a GC race where the old mmap destructors could unlink
        # files while recovery is re-opening them.
        close_all(name)
        :ets.delete_all_objects(name)
        name
    end
  end

  @doc """
  Closes all cached bloom filter resources in the given table.
  """
  @spec close_all(atom()) :: :ok
  def close_all(table) do
    :ets.foldl(
      fn {_key, resource, _meta}, :ok ->
        try do
          NIF.bloom_delete(resource)
        rescue
          _ -> :ok
        end
        :ok
      end,
      :ok,
      table
    )
  end

  @doc """
  Returns the ETS table name for the given shard index.
  """
  @spec table_name(non_neg_integer()) :: atom()
  def table_name(index), do: :"bloom_reg_#{index}"

  # -------------------------------------------------------------------
  # Registration / lookup / delete
  # -------------------------------------------------------------------

  @doc """
  Registers a newly created bloom resource in the shard's ETS cache.

  ## Parameters

    * `index` -- shard index
    * `key` -- the Redis key
    * `resource` -- NIF `ResourceArc` from `bloom_create`
    * `metadata` -- `%{capacity: pos_integer(), error_rate: float()}`
  """
  @spec register(non_neg_integer(), binary(), reference(), metadata()) :: true
  def register(index, key, resource, metadata) do
    :ets.insert(table_name(index), {key, resource, metadata})
  end

  @doc """
  Looks up a bloom resource by key. Returns `{resource, metadata}` or `nil`.

  Does NOT auto-open from disk; callers should use `open_or_lookup/4` for
  lazy re-opening.
  """
  @spec lookup(non_neg_integer(), binary()) :: {reference(), metadata()} | nil
  def lookup(index, key) do
    case :ets.lookup(table_name(index), key) do
      [{^key, resource, metadata}] -> {resource, metadata}
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc """
  Looks up a bloom resource, re-opening from disk if not cached.

  When the ETS cache misses but a `.bloom` file exists on disk, the file
  is opened via `NIF.bloom_open/1` and cached. Returns
  `{:ok, resource, metadata}` or `:not_found`.

  ## Parameters

    * `index` -- shard index
    * `key` -- Redis key
    * `data_dir` -- root data directory
    * `metadata_fallback` -- metadata to assign when re-opening a file
      that has no cached metadata (e.g. after shard restart). Pass `nil`
      to derive from the NIF info.
  """
  @spec open_or_lookup(non_neg_integer(), binary(), binary(), metadata() | nil) ::
          {:ok, reference(), metadata()} | :not_found
  def open_or_lookup(index, key, data_dir, metadata_fallback \\ nil) do
    case lookup(index, key) do
      {resource, metadata} ->
        {:ok, resource, metadata}

      nil ->
        path = bloom_path(data_dir, index, key)

        if File.exists?(path) do
          case NIF.bloom_open(path) do
            {:ok, resource} ->
              meta = metadata_fallback || derive_metadata(resource)
              register(index, key, resource, meta)
              {:ok, resource, meta}

            {:error, reason} ->
              Logger.warning("BloomRegistry: failed to reopen #{path}: #{reason}")
              :not_found
          end

        else
          :not_found
        end
    end
  end

  @doc """
  Deletes a bloom resource: munmap + unlink the file + remove from ETS.

  Returns `:ok` even if the key was not registered (idempotent).
  """
  @spec delete(non_neg_integer(), binary()) :: :ok
  def delete(index, key) do
    case lookup(index, key) do
      {resource, _meta} ->
        NIF.bloom_delete(resource)
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

  @doc """
  Re-opens all `.bloom` files in the shard's `prob/` directory.

  Called during shard init to restore mmap handles after a restart.
  Returns the count of recovered filters.
  """
  @spec recover(binary(), non_neg_integer()) :: non_neg_integer()
  def recover(data_dir, index) do
    dir = prob_dir(data_dir, index)

    case File.ls(dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".bloom"))
        |> Enum.reduce(0, fn filename, count ->
          key = filename |> String.trim_trailing(".bloom")
          path = Path.join(dir, filename)

          case NIF.bloom_open(path) do
            {:ok, resource} ->
              meta = derive_metadata(resource)
              register(index, key, resource, meta)
              count + 1

            {:error, reason} ->
              Logger.warning("BloomRegistry: failed to recover #{path}: #{reason}")
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

  @doc """
  Returns the file path for a bloom filter.

  `data_dir/prob/shard_N/KEY.bloom`
  """
  @spec bloom_path(binary(), non_neg_integer(), binary()) :: binary()
  def bloom_path(data_dir, index, key) do
    safe_key = sanitize_key(key)
    Path.join([data_dir, "prob", "shard_#{index}", "#{safe_key}.bloom"])
  end

  @doc false
  @spec prob_dir(binary(), non_neg_integer()) :: binary()
  def prob_dir(data_dir, index) do
    Path.join([data_dir, "prob", "shard_#{index}"])
  end

  @doc """
  Sanitizes a Redis key for use as a filename.

  Replaces any character that is not alphanumeric, `_`, `-`, or `.` with `_`.
  """
  @spec sanitize_key(binary()) :: binary()
  def sanitize_key(key) do
    String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  # Derives metadata from the NIF info when we re-open a file without
  # cached metadata. Capacity is not stored in the bloom file header,
  # so we estimate it from num_bits and num_hashes using the inverse
  # of the optimal sizing formula.
  @spec derive_metadata(reference()) :: metadata()
  defp derive_metadata(resource) do
    {:ok, {num_bits, _count, num_hashes}} = NIF.bloom_info(resource)

    # Inverse of: m = -n * ln(p) / (ln(2))^2 and k = (m/n) * ln(2)
    # From k = (m/n)*ln(2): n = m * ln(2) / k
    # From m = -n*ln(p) / (ln(2))^2: p = exp(-m * (ln(2))^2 / n)
    capacity =
      if num_hashes > 0 do
        max(1, round(num_bits * :math.log(2) / num_hashes))
      else
        100
      end

    error_rate =
      if capacity > 0 do
        :math.exp(-num_bits * :math.pow(:math.log(2), 2) / capacity)
      else
        0.01
      end

    %{capacity: capacity, error_rate: error_rate}
  end
end
