defmodule Ferricstore.Commands.Cuckoo do
  @moduledoc """
  Handles Redis-compatible Cuckoo filter commands backed by mmap NIF resources.

  Each Cuckoo filter is a memory-mapped file on disk managed by a Rust NIF.
  Files live at `data_dir/prob/shard_N/KEY.cuckoo`. The mmap handle (NIF
  resource) is cached in a per-shard ETS table via `CuckooRegistry` so that
  repeated operations avoid re-opening the file.

  ## Supported Commands

    * `CF.RESERVE key capacity` -- creates a new Cuckoo filter
    * `CF.ADD key element` -- adds an element (auto-creates with defaults if missing)
    * `CF.ADDNX key element` -- adds only if the element does not already exist
    * `CF.DEL key element` -- deletes one occurrence of an element
    * `CF.EXISTS key element` -- checks if an element may exist
    * `CF.MEXISTS key element [element ...]` -- checks multiple elements
    * `CF.COUNT key element` -- counts fingerprint occurrences (approximate)
    * `CF.INFO key` -- returns filter metadata

  ## Resource caching

  On first access, the NIF resource is opened from the `.cuckoo` file and
  cached in `CuckooRegistry`. Subsequent operations on the same key reuse
  the cached handle. On shard restart, `CuckooRegistry.recover/2` re-opens
  all persisted cuckoo files.

  ## Auto-creation

  `CF.ADD` and `CF.ADDNX` auto-create a filter with default capacity (1024)
  if the key does not exist.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CuckooRegistry, Router}

  @default_capacity 1024
  @bucket_size 4

  # -------------------------------------------------------------------
  # Public command handler
  # -------------------------------------------------------------------

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # CF.RESERVE key capacity
  # ---------------------------------------------------------------------------

  def handle("CF.RESERVE", [key, capacity_str], store) do
    with {:ok, capacity} <- parse_pos_integer(capacity_str, "capacity") do
      if cuckoo_exists?(key, store) do
        {:error, "ERR item exists"}
      else
        create_cuckoo(key, capacity, store)
      end
    end
  end

  def handle("CF.RESERVE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.reserve' command"}

  # ---------------------------------------------------------------------------
  # CF.ADD key element
  # ---------------------------------------------------------------------------

  def handle("CF.ADD", [key, element], store) do
    {resource, _meta} = ensure_cuckoo(key, store)

    case NIF.cuckoo_add(resource, element) do
      :ok -> 1
      {:error, _} -> {:error, "ERR filter is full"}
    end
  end

  def handle("CF.ADD", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.add' command"}

  # ---------------------------------------------------------------------------
  # CF.ADDNX key element
  # ---------------------------------------------------------------------------

  def handle("CF.ADDNX", [key, element], store) do
    {resource, _meta} = ensure_cuckoo(key, store)

    case NIF.cuckoo_addnx(resource, element) do
      0 -> 0
      1 -> 1
      {:error, _} -> {:error, "ERR filter is full"}
    end
  end

  def handle("CF.ADDNX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.addnx' command"}

  # ---------------------------------------------------------------------------
  # CF.DEL key element
  # ---------------------------------------------------------------------------

  def handle("CF.DEL", [key, element], store) do
    case get_cuckoo(key, store) do
      nil -> 0
      {resource, _meta} ->
        case NIF.cuckoo_del(resource, element) do
          {:error, reason} -> {:error, "ERR cuckoo del failed: #{inspect(reason)}"}
          result -> result
        end
    end
  end

  def handle("CF.DEL", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.del' command"}

  # ---------------------------------------------------------------------------
  # CF.EXISTS key element
  # ---------------------------------------------------------------------------

  def handle("CF.EXISTS", [key, element], store) do
    case get_cuckoo(key, store) do
      nil -> 0
      {resource, _meta} -> NIF.cuckoo_exists(resource, element)
    end
  end

  def handle("CF.EXISTS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.exists' command"}

  # ---------------------------------------------------------------------------
  # CF.MEXISTS key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("CF.MEXISTS", [key | elements], store) when elements != [] do
    case get_cuckoo(key, store) do
      nil -> List.duplicate(0, length(elements))
      {resource, _meta} -> NIF.cuckoo_mexists(resource, elements)
    end
  end

  def handle("CF.MEXISTS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.mexists' command"}

  # ---------------------------------------------------------------------------
  # CF.COUNT key element
  # ---------------------------------------------------------------------------

  def handle("CF.COUNT", [key, element], store) do
    case get_cuckoo(key, store) do
      nil -> 0
      {resource, _meta} -> NIF.cuckoo_count(resource, element)
    end
  end

  def handle("CF.COUNT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.count' command"}

  # ---------------------------------------------------------------------------
  # CF.INFO key
  # ---------------------------------------------------------------------------

  def handle("CF.INFO", [key], store) do
    case get_cuckoo(key, store) do
      nil ->
        {:error, "ERR not found"}

      {resource, _meta} ->
        case NIF.cuckoo_info(resource) do
          {:ok, {num_buckets, bucket_size, fingerprint_size,
                 num_items, num_deletes, total_slots, max_kicks}} ->
            ["Size", total_slots, "Number of buckets", num_buckets,
             "Number of filters", 1, "Number of items inserted", num_items,
             "Number of items deleted", num_deletes, "Bucket size", bucket_size,
             "Fingerprint size", fingerprint_size, "Max iterations", max_kicks,
             "Expansion rate", 0]

          {:error, reason} ->
            {:error, "ERR cuckoo info failed: #{reason}"}
        end
    end
  end

  def handle("CF.INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.info' command"}

  # ---------------------------------------------------------------------------
  # Deletion (called from DEL / UNLINK handlers)
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok
  def nif_delete(key, store) do
    case resolve_registry(store) do
      {:ets, index, _data_dir} ->
        CuckooRegistry.delete(index, key)

      {:callback, registry} ->
        case registry.get.(key) do
          nil ->
            :ok

          {resource, _meta} ->
            NIF.cuckoo_close(resource)
            registry.delete.(key)
            :ok
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: registry resolution
  # ---------------------------------------------------------------------------

  @spec resolve_registry(map()) ::
          {:ets, non_neg_integer(), binary()}
          | {:callback, map()}
  defp resolve_registry(%{cuckoo_registry: registry}) when is_map(registry) do
    {:callback, registry}
  end

  defp resolve_registry(_store) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:ets, nil, data_dir}
  end

  @spec resolve_registry_for_key(binary(), map()) ::
          {:ets, non_neg_integer(), binary()}
          | {:callback, map()}
  defp resolve_registry_for_key(_key, %{cuckoo_registry: registry}) when is_map(registry) do
    {:callback, registry}
  end

  defp resolve_registry_for_key(key, _store) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    index = Router.shard_for(key)
    {:ets, index, data_dir}
  end

  # ---------------------------------------------------------------------------
  # Private: cuckoo resource access
  # ---------------------------------------------------------------------------

  @spec get_cuckoo(binary(), map()) :: {reference(), map()} | nil
  defp get_cuckoo(key, store) do
    case resolve_registry_for_key(key, store) do
      {:ets, index, data_dir} ->
        case CuckooRegistry.open_or_lookup(index, key, data_dir) do
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

  @spec cuckoo_exists?(binary(), map()) :: boolean()
  defp cuckoo_exists?(key, store) do
    get_cuckoo(key, store) != nil
  end

  @spec ensure_cuckoo(binary(), map()) :: {reference(), map()}
  defp ensure_cuckoo(key, store) do
    case get_cuckoo(key, store) do
      nil ->
        {:ok, resource, meta} = create_cuckoo_internal(key, @default_capacity, store)
        {resource, meta}

      existing ->
        existing
    end
  end

  @spec create_cuckoo(binary(), pos_integer(), map()) :: :ok | {:error, binary()}
  defp create_cuckoo(key, capacity, store) do
    case create_cuckoo_internal(key, capacity, store) do
      {:ok, _resource, _meta} -> :ok
      {:error, _} = err -> err
    end
  end

  @spec create_cuckoo_internal(binary(), pos_integer(), map()) ::
          {:ok, reference(), map()} | {:error, binary()}
  defp create_cuckoo_internal(key, capacity, store) do
    meta = %{capacity: capacity}

    case resolve_registry_for_key(key, store) do
      {:ets, index, data_dir} ->
        path = CuckooRegistry.cuckoo_path(data_dir, index, key)

        case NIF.cuckoo_create_file(path, capacity, @bucket_size) do
          {:ok, resource} ->
            CuckooRegistry.register(index, key, resource, meta)
            {:ok, resource, meta}

          {:error, reason} ->
            {:error, "ERR cuckoo create failed: #{reason}"}
        end

      {:callback, registry} ->
        path = registry.path.(key)

        case NIF.cuckoo_create_file(path, capacity, @bucket_size) do
          {:ok, resource} ->
            registry.put.(key, resource, meta)
            {:ok, resource, meta}

          {:error, reason} ->
            {:error, "ERR cuckoo create failed: #{reason}"}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: input validation
  # ---------------------------------------------------------------------------

  @spec parse_pos_integer(binary(), binary()) :: {:ok, pos_integer()} | {:error, binary()}
  defp parse_pos_integer(str, name) do
    case Integer.parse(str) do
      {val, ""} when val > 0 -> {:ok, val}
      _ -> {:error, "ERR bad #{name} value"}
    end
  end
end
