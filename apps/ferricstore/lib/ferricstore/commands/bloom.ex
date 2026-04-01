defmodule Ferricstore.Commands.Bloom do
  @moduledoc """
  Handles Redis-compatible Bloom filter commands backed by mmap NIF resources.

  Each Bloom filter is a memory-mapped file on disk managed by a Rust NIF.
  Files live at `data_dir/prob/shard_N/KEY.bloom`. The mmap handle (NIF
  resource) is cached in a per-shard ETS table via `BloomRegistry` so that
  repeated operations avoid re-opening the file.

  ## Supported Commands

    * `BF.RESERVE key error_rate capacity` -- creates a new Bloom filter
    * `BF.ADD key element` -- adds an element (auto-creates with defaults if missing)
    * `BF.MADD key element [element ...]` -- adds multiple elements
    * `BF.EXISTS key element` -- checks if an element may exist
    * `BF.MEXISTS key element [element ...]` -- checks multiple elements
    * `BF.CARD key` -- returns the number of elements added
    * `BF.INFO key` -- returns filter metadata

  ## Resource caching

  On first access, the NIF resource is opened from the `.bloom` file and
  cached in `BloomRegistry`. Subsequent operations on the same key reuse
  the cached handle. On shard restart, `BloomRegistry.recover/2` re-opens
  all persisted bloom files.

  ## Auto-creation

  `BF.ADD` and `BF.MADD` auto-create a filter with default parameters
  (error_rate = 0.01, capacity = 100) if the key does not exist.

  ## Deletion

  Calling `nif_delete/2` invokes `NIF.bloom_delete/1` which munmaps the
  region and unlinks the file from disk.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{BloomRegistry, Router}

  @default_error_rate 0.01
  @default_capacity 100

  # -------------------------------------------------------------------
  # Public command handler
  # -------------------------------------------------------------------

  @doc """
  Handles a Bloom filter command.

  ## Parameters

    * `cmd` -- uppercased command name (e.g. `"BF.RESERVE"`, `"BF.ADD"`)
    * `args` -- list of string arguments
    * `store` -- injected store map. When `store.bloom_registry` is present
      (a map with `get`, `put`, `delete`, `path` callbacks), it is used for
      resource caching. Otherwise, the module falls back to the global
      `BloomRegistry` ETS tables using `data_dir` from application config.

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # BF.RESERVE key error_rate capacity
  # ---------------------------------------------------------------------------

  def handle("BF.RESERVE", [key, error_rate_str, capacity_str], store) do
    with {:ok, error_rate} <- parse_float(error_rate_str, "error_rate"),
         {:ok, capacity} <- parse_pos_integer(capacity_str, "capacity"),
         :ok <- validate_error_rate(error_rate) do
      if bloom_exists?(key, store) do
        {:error, "ERR item exists"}
      else
        create_bloom(key, capacity, error_rate, store)
      end
    end
  end

  def handle("BF.RESERVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.reserve' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.ADD key element
  # ---------------------------------------------------------------------------

  def handle("BF.ADD", [key, element], store) do
    {resource, _meta} = ensure_bloom(key, store)
    NIF.bloom_add(resource, element)
  end

  def handle("BF.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.add' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.MADD key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("BF.MADD", [key | elements], store) when elements != [] do
    {resource, _meta} = ensure_bloom(key, store)
    NIF.bloom_madd(resource, elements)
  end

  def handle("BF.MADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.madd' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.EXISTS key element
  # ---------------------------------------------------------------------------

  def handle("BF.EXISTS", [key, element], store) do
    case get_bloom(key, store) do
      nil -> 0
      {resource, _meta} -> NIF.bloom_exists(resource, element)
    end
  end

  def handle("BF.EXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.exists' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.MEXISTS key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("BF.MEXISTS", [key | elements], store) when elements != [] do
    case get_bloom(key, store) do
      nil -> List.duplicate(0, length(elements))
      {resource, _meta} -> NIF.bloom_mexists(resource, elements)
    end
  end

  def handle("BF.MEXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.mexists' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.CARD key
  # ---------------------------------------------------------------------------

  def handle("BF.CARD", [key], store) do
    case get_bloom(key, store) do
      nil -> 0
      {resource, _meta} -> NIF.bloom_card(resource)
    end
  end

  def handle("BF.CARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.card' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.INFO key
  # ---------------------------------------------------------------------------

  def handle("BF.INFO", [key], store) do
    case get_bloom(key, store) do
      nil ->
        {:error, "ERR not found"}

      {resource, meta} ->
        case NIF.bloom_info(resource) do
          {:ok, {num_bits, count, num_hashes}} ->
            capacity = Map.get(meta, :capacity, @default_capacity)
            error_rate = Map.get(meta, :error_rate, @default_error_rate)

            [
              "Capacity", capacity,
              "Size", count,
              "Number of filters", 1,
              "Number of items inserted", count,
              "Expansion rate", 0,
              "Error rate", error_rate,
              "Number of hash functions", num_hashes,
              "Number of bits", num_bits
            ]

          {:error, reason} ->
            {:error, "ERR bloom info failed: #{reason}"}
        end
    end
  end

  def handle("BF.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Deletion (called from DEL / UNLINK handlers)
  # ---------------------------------------------------------------------------

  @doc """
  Deletes a bloom filter: munmap + unlink the file + remove from cache.

  Returns `:ok` whether or not the key existed (idempotent).

  ## Parameters

    * `key` -- the Redis key
    * `store` -- the store map (used to resolve registry backend)
  """
  @spec nif_delete(binary(), map()) :: :ok
  def nif_delete(key, store) do
    case resolve_registry(store) do
      {:ets, index, _data_dir} ->
        BloomRegistry.delete(index, key)

      {:callback, registry} ->
        case registry.get.(key) do
          nil ->
            :ok

          {resource, _meta} ->
            NIF.bloom_delete(resource)
            registry.delete.(key)
            :ok
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Optimal sizing (public for tests)
  # ---------------------------------------------------------------------------

  @doc false
  @spec optimal_num_bits(pos_integer(), float()) :: pos_integer()
  def optimal_num_bits(capacity, error_rate) do
    m = -1.0 * capacity * :math.log(error_rate) / :math.pow(:math.log(2), 2)
    max(1, ceil(m))
  end

  @doc false
  @spec optimal_num_hashes(pos_integer(), pos_integer()) :: pos_integer()
  def optimal_num_hashes(num_bits, capacity) do
    k = num_bits / capacity * :math.log(2)
    max(1, round(k))
  end

  # ---------------------------------------------------------------------------
  # Private: registry resolution
  # ---------------------------------------------------------------------------

  # Two modes of operation:
  #
  # 1. **Callback mode** -- `store.bloom_registry` is a map with
  #    `get/put/delete/path` callbacks (used in tests).
  #
  # 2. **ETS mode** -- the global `BloomRegistry` ETS tables are used.
  #    The shard index is derived from the key via `Router.shard_for/1`
  #    and the data_dir from application config.
  @spec resolve_registry(map()) ::
          {:ets, non_neg_integer(), binary()}
          | {:callback, map()}
  defp resolve_registry(%{bloom_registry: registry}) when is_map(registry) do
    {:callback, registry}
  end

  defp resolve_registry(_store) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:ets, nil, data_dir}
  end

  # Resolves the registry for a specific key (fills in shard index for ETS mode).
  @spec resolve_registry_for_key(binary(), map()) ::
          {:ets, non_neg_integer(), binary()}
          | {:callback, map()}
  defp resolve_registry_for_key(_key, %{bloom_registry: registry}) when is_map(registry) do
    {:callback, registry}
  end

  defp resolve_registry_for_key(key, _store) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    index = Router.shard_for(key)
    {:ets, index, data_dir}
  end

  # ---------------------------------------------------------------------------
  # Private: bloom resource access
  # ---------------------------------------------------------------------------

  # Returns {resource, metadata} or nil.
  @spec get_bloom(binary(), map()) :: {reference(), map()} | nil
  defp get_bloom(key, store) do
    case resolve_registry_for_key(key, store) do
      {:ets, index, data_dir} ->
        case BloomRegistry.open_or_lookup(index, key, data_dir) do
          {:ok, resource, meta} -> {resource, meta}
          :not_found -> nil
        end

      {:callback, registry} ->
        case registry.get.(key) do
          nil -> nil
          {resource, meta} -> {resource, meta}
        end
    end
  end

  # Returns true if a bloom key exists (either in cache or on disk).
  @spec bloom_exists?(binary(), map()) :: boolean()
  defp bloom_exists?(key, store) do
    get_bloom(key, store) != nil
  end

  # Returns {resource, metadata}, creating a new filter with defaults if missing.
  @spec ensure_bloom(binary(), map()) :: {reference(), map()}
  defp ensure_bloom(key, store) do
    case get_bloom(key, store) do
      nil ->
        {:ok, resource, meta} =
          create_bloom_internal(key, @default_capacity, @default_error_rate, store)

        {resource, meta}

      existing ->
        existing
    end
  end

  # Creates a new bloom filter and registers it.
  @spec create_bloom(binary(), pos_integer(), float(), map()) :: :ok | {:error, binary()}
  defp create_bloom(key, capacity, error_rate, store) do
    case create_bloom_internal(key, capacity, error_rate, store) do
      {:ok, _resource, _meta} -> :ok
      {:error, _} = err -> err
    end
  end

  @spec create_bloom_internal(binary(), pos_integer(), float(), map()) ::
          {:ok, reference(), map()} | {:error, binary()}
  defp create_bloom_internal(key, capacity, error_rate, store) do
    num_bits = optimal_num_bits(capacity, error_rate)
    num_hashes = optimal_num_hashes(num_bits, capacity)
    meta = %{capacity: capacity, error_rate: error_rate}

    case resolve_registry_for_key(key, store) do
      {:ets, index, data_dir} ->
        path = BloomRegistry.bloom_path(data_dir, index, key)

        case NIF.bloom_create(path, num_bits, num_hashes) do
          {:ok, resource} ->
            BloomRegistry.register(index, key, resource, meta)
            BloomRegistry.save_meta(path, meta)
            {:ok, resource, meta}

          {:error, reason} ->
            {:error, "ERR bloom create failed: #{reason}"}
        end

      {:callback, registry} ->
        path = registry.path.(key)

        case NIF.bloom_create(path, num_bits, num_hashes) do
          {:ok, resource} ->
            registry.put.(key, resource, meta)
            {:ok, resource, meta}

          {:error, reason} ->
            {:error, "ERR bloom create failed: #{reason}"}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: input validation
  # ---------------------------------------------------------------------------

  @spec parse_float(binary(), binary()) :: {:ok, float()} | {:error, binary()}
  defp parse_float(str, name) do
    case Float.parse(str) do
      {val, ""} -> {:ok, val}
      {val, ".0"} -> {:ok, val}
      _ ->
        case Integer.parse(str) do
          {int_val, ""} -> {:ok, int_val / 1}
          _ -> {:error, "ERR bad #{name} value"}
        end
    end
  end

  @spec parse_pos_integer(binary(), binary()) :: {:ok, pos_integer()} | {:error, binary()}
  defp parse_pos_integer(str, name) do
    case Integer.parse(str) do
      {val, ""} when val > 0 -> {:ok, val}
      _ -> {:error, "ERR bad #{name} value"}
    end
  end

  @spec validate_error_rate(float()) :: :ok | {:error, binary()}
  defp validate_error_rate(rate) when rate > 0.0 and rate < 1.0, do: :ok
  defp validate_error_rate(_), do: {:error, "ERR (0 < error rate range < 1)"}
end
