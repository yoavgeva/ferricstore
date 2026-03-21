defmodule Ferricstore.Commands.Cuckoo do
  @moduledoc """
  Handles Redis-compatible Cuckoo filter commands via Rust NIF.

  Cuckoo filters are space-efficient probabilistic data structures similar to
  Bloom filters, but with two key advantages: they support deletion and can
  count the approximate number of times an element was inserted.

  ## Storage

  Cuckoo filters are stored as compact byte arrays in Bitcask. The Rust NIF
  handles serialization/deserialization of the filter state. On each command:

    1. Load the raw binary from the store
    2. Deserialize into a NIF resource (CuckooResource)
    3. Perform the operation
    4. Serialize back to binary and persist

  ## Supported Commands

    * `CF.RESERVE key capacity` - creates a new Cuckoo filter
    * `CF.ADD key element` - adds an element (auto-creates if missing)
    * `CF.ADDNX key element` - adds only if the element does not already exist
    * `CF.DEL key element` - deletes one occurrence of an element
    * `CF.EXISTS key element` - checks if an element may exist
    * `CF.MEXISTS key element [element ...]` - checks multiple elements
    * `CF.COUNT key element` - counts fingerprint occurrences (approximate)
    * `CF.INFO key` - returns filter metadata
  """

  alias Ferricstore.Bitcask.NIF

  @default_capacity 1024
  @bucket_size 4
  @max_kicks 500

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CF.RESERVE", [key, capacity_str], store) do
    with {:ok, capacity} <- parse_pos_integer(capacity_str, "capacity") do
      if store.exists?.(key) do
        {:error, "ERR item exists"}
      else
        ref = nif_create!(capacity)
        persist_filter!(key, ref, store)
        :ok
      end
    end
  end

  def handle("CF.RESERVE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.reserve' command"}

  def handle("CF.ADD", [key, element], store) do
    ref = load_or_create_ref(key, store)
    case NIF.cuckoo_add(ref, element) do
      :ok -> persist_filter!(key, ref, store); 1
      {:error, _} -> {:error, "ERR filter is full"}
    end
  end

  def handle("CF.ADD", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.add' command"}

  def handle("CF.ADDNX", [key, element], store) do
    ref = load_or_create_ref(key, store)
    case NIF.cuckoo_addnx(ref, element) do
      0 -> 0
      1 -> persist_filter!(key, ref, store); 1
      {:error, _} -> {:error, "ERR filter is full"}
    end
  end

  def handle("CF.ADDNX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.addnx' command"}

  def handle("CF.DEL", [key, element], store) do
    case load_ref(key, store) do
      nil -> 0
      ref ->
        case NIF.cuckoo_del(ref, element) do
          1 -> persist_filter!(key, ref, store); 1
          0 -> 0
        end
    end
  end

  def handle("CF.DEL", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.del' command"}

  def handle("CF.EXISTS", [key, element], store) do
    case load_ref(key, store) do
      nil -> 0
      ref -> NIF.cuckoo_exists(ref, element)
    end
  end

  def handle("CF.EXISTS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.exists' command"}

  def handle("CF.MEXISTS", [key | elements], store) when elements != [] do
    case load_ref(key, store) do
      nil -> List.duplicate(0, length(elements))
      ref -> NIF.cuckoo_mexists(ref, elements)
    end
  end

  def handle("CF.MEXISTS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.mexists' command"}

  def handle("CF.COUNT", [key, element], store) do
    case load_ref(key, store) do
      nil -> 0
      ref -> NIF.cuckoo_count(ref, element)
    end
  end

  def handle("CF.COUNT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.count' command"}

  def handle("CF.INFO", [key], store) do
    case load_ref(key, store) do
      nil ->
        {:error, "ERR not found"}
      ref ->
        {:ok, {num_buckets, bucket_size, fingerprint_size,
               num_items, num_deletes, total_slots, max_kicks}} = NIF.cuckoo_info(ref)
        ["Size", total_slots, "Number of buckets", num_buckets,
         "Number of filters", 1, "Number of items inserted", num_items,
         "Number of items deleted", num_deletes, "Bucket size", bucket_size,
         "Fingerprint size", fingerprint_size, "Max iterations", max_kicks,
         "Expansion rate", 0]
    end
  end

  def handle("CF.INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.info' command"}

  # --- Private ---

  defp nif_create!(capacity) do
    {:ok, ref} = NIF.cuckoo_create(capacity, @bucket_size, @max_kicks, 0)
    ref
  end

  defp load_ref(key, store) do
    case store.get.(key) do
      nil -> nil
      encoded when is_binary(encoded) ->
        case NIF.cuckoo_deserialize(encoded) do
          {:ok, ref} -> ref
          {:error, _} -> nil
        end
      _ -> nil
    end
  end

  defp load_or_create_ref(key, store) do
    case load_ref(key, store) do
      nil -> nif_create!(@default_capacity)
      ref -> ref
    end
  end

  defp persist_filter!(key, ref, store) do
    {:ok, blob} = NIF.cuckoo_serialize(ref)
    store.put.(key, blob, 0)
  end

  defp parse_pos_integer(str, name) do
    case Integer.parse(str) do
      {val, ""} when val > 0 -> {:ok, val}
      _ -> {:error, "ERR bad #{name} value"}
    end
  end
end
