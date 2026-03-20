defmodule Ferricstore.Commands.Cuckoo do
  @moduledoc """
  Handles Redis-compatible Cuckoo filter commands.

  Cuckoo filters are space-efficient probabilistic data structures similar to
  Bloom filters, but with two key advantages: they support deletion and can
  count the approximate number of times an element was inserted.

  ## Algorithm

  A Cuckoo filter stores fingerprints of elements in a hash table with two
  candidate bucket positions. When both candidate buckets are full, an existing
  fingerprint is relocated ("kicked") to its alternate bucket, making room
  for the new entry. This process may cascade. If after `@max_kicks` relocations
  no slot is found, the insertion fails (filter is full).

  ## Storage

  Cuckoo filters are stored in the shard key-value store as tagged tuples:

      {:cuckoo, bucket_array_binary, metadata}

  where `metadata` is a map containing:

    * `:capacity` - number of buckets
    * `:bucket_size` - number of slots per bucket (default 4)
    * `:fingerprint_size` - fingerprint size in bytes (default 2)
    * `:num_items` - current number of elements inserted
    * `:num_deletes` - current number of elements deleted

  ## Hash Functions

  Uses `:crypto.hash(:md5, element)` for fingerprint generation and bucket
  indexing. The alternate bucket is computed via partial-key cuckoo hashing:

      alt_bucket = bucket XOR hash(fingerprint)

  This ensures the alternate bucket can be computed from the fingerprint alone,
  which is essential for the relocation ("kicking") step.

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

  @default_capacity 1024
  @bucket_size 4
  @fingerprint_size 2
  @max_kicks 500

  @doc """
  Handles a Cuckoo filter command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"CF.RESERVE"`, `"CF.ADD"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, list, map, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # CF.RESERVE key capacity
  # ---------------------------------------------------------------------------

  def handle("CF.RESERVE", [key, capacity_str], store) do
    with {:ok, capacity} <- parse_pos_integer(capacity_str, "capacity") do
      if store.exists?.(key) do
        {:error, "ERR item exists"}
      else
        filter = new_filter(capacity)
        persist_filter!(key, filter, store)
        :ok
      end
    end
  end

  def handle("CF.RESERVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.reserve' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.ADD key element
  # ---------------------------------------------------------------------------

  def handle("CF.ADD", [key, element], store) do
    filter = load_or_create_filter(key, store)

    case insert(filter, element) do
      {:ok, new_filter} ->
        persist_filter!(key, new_filter, store)
        1

      {:error, :full} ->
        {:error, "ERR filter is full"}
    end
  end

  def handle("CF.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.add' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.ADDNX key element
  # ---------------------------------------------------------------------------

  def handle("CF.ADDNX", [key, element], store) do
    filter = load_or_create_filter(key, store)

    if lookup(filter, element) do
      0
    else
      case insert(filter, element) do
        {:ok, new_filter} ->
          persist_filter!(key, new_filter, store)
          1

        {:error, :full} ->
          {:error, "ERR filter is full"}
      end
    end
  end

  def handle("CF.ADDNX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.addnx' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.DEL key element
  # ---------------------------------------------------------------------------

  def handle("CF.DEL", [key, element], store) do
    case load_filter(key, store) do
      nil ->
        0

      filter ->
        case delete_element(filter, element) do
          {:ok, new_filter} ->
            persist_filter!(key, new_filter, store)
            1

          :not_found ->
            0
        end
    end
  end

  def handle("CF.DEL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.del' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.EXISTS key element
  # ---------------------------------------------------------------------------

  def handle("CF.EXISTS", [key, element], store) do
    case load_filter(key, store) do
      nil -> 0
      filter -> if lookup(filter, element), do: 1, else: 0
    end
  end

  def handle("CF.EXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.exists' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.MEXISTS key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("CF.MEXISTS", [key | elements], store) when elements != [] do
    case load_filter(key, store) do
      nil -> List.duplicate(0, length(elements))
      filter -> Enum.map(elements, &lookup_to_int(filter, &1))
    end
  end

  def handle("CF.MEXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.mexists' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.COUNT key element
  # ---------------------------------------------------------------------------

  def handle("CF.COUNT", [key, element], store) do
    case load_filter(key, store) do
      nil -> 0
      filter -> count_element(filter, element)
    end
  end

  def handle("CF.COUNT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.count' command"}
  end

  # ---------------------------------------------------------------------------
  # CF.INFO key
  # ---------------------------------------------------------------------------

  def handle("CF.INFO", [key], store) do
    case load_filter(key, store) do
      nil ->
        {:error, "ERR not found"}

      filter ->
        total_slots = filter.capacity * filter.bucket_size

        [
          "Size", total_slots,
          "Number of buckets", filter.capacity,
          "Number of filters", 1,
          "Number of items inserted", filter.num_items,
          "Number of items deleted", filter.num_deletes,
          "Bucket size", filter.bucket_size,
          "Fingerprint size", filter.fingerprint_size,
          "Max iterations", @max_kicks,
          "Expansion rate", 0
        ]
    end
  end

  def handle("CF.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cf.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Private: filter construction
  # ---------------------------------------------------------------------------

  @doc false
  @spec new_filter(pos_integer()) :: map()
  def new_filter(capacity) do
    # Each bucket has @bucket_size slots, each slot is @fingerprint_size bytes.
    # A zero fingerprint means the slot is empty.
    slot_bytes = @fingerprint_size
    bucket_bytes = @bucket_size * slot_bytes
    total_bytes = capacity * bucket_bytes
    buckets = <<0::size(total_bytes * 8)>>

    %{
      buckets: buckets,
      capacity: capacity,
      bucket_size: @bucket_size,
      fingerprint_size: @fingerprint_size,
      num_items: 0,
      num_deletes: 0
    }
  end

  # ---------------------------------------------------------------------------
  # Private: hash functions
  # ---------------------------------------------------------------------------

  # Computes the fingerprint for an element. The fingerprint is the first
  # @fingerprint_size bytes of the MD5 hash, with the constraint that it
  # is never all zeros (since zero indicates an empty slot).
  @spec fingerprint(binary()) :: binary()
  defp fingerprint(element) do
    <<fp::binary-size(@fingerprint_size), _::binary>> = :crypto.hash(:md5, element)

    # Ensure fingerprint is never all zeros (empty slot sentinel).
    if fp == <<0::size(@fingerprint_size * 8)>> do
      <<1, 0::size((@fingerprint_size - 1) * 8)>>
    else
      fp
    end
  end

  # Computes the primary bucket index for an element.
  @spec primary_bucket(binary(), pos_integer()) :: non_neg_integer()
  defp primary_bucket(element, capacity) do
    <<_::binary-size(@fingerprint_size), rest::binary>> = :crypto.hash(:md5, element)
    <<hash_val::unsigned-little-64, _::binary>> = rest
    rem(hash_val, capacity)
  end

  # Computes the alternate bucket index given a bucket index and fingerprint.
  # Uses partial-key cuckoo hashing: alt = bucket XOR hash(fingerprint)
  @spec alternate_bucket(non_neg_integer(), binary(), pos_integer()) :: non_neg_integer()
  defp alternate_bucket(bucket, fp, capacity) do
    <<fp_hash::unsigned-little-64, _::binary>> = :crypto.hash(:md5, fp)
    rem(abs(Bitwise.bxor(bucket, fp_hash)), capacity)
  end

  # ---------------------------------------------------------------------------
  # Private: bucket operations
  # ---------------------------------------------------------------------------

  # Returns the list of fingerprints in a bucket (including empty slots as zero binaries).
  @spec get_bucket(binary(), non_neg_integer(), pos_integer(), pos_integer()) :: [binary()]
  defp get_bucket(buckets, bucket_idx, bucket_size, fp_size) do
    bucket_offset = bucket_idx * bucket_size * fp_size

    Enum.map(0..(bucket_size - 1), fn slot ->
      offset = bucket_offset + slot * fp_size
      binary_part(buckets, offset, fp_size)
    end)
  end

  # Sets a specific slot in a bucket to the given fingerprint.
  @spec set_slot(binary(), non_neg_integer(), non_neg_integer(), binary(), pos_integer(), pos_integer()) :: binary()
  defp set_slot(buckets, bucket_idx, slot_idx, fp, bucket_size, fp_size) do
    offset = (bucket_idx * bucket_size + slot_idx) * fp_size
    <<prefix::binary-size(offset), _::binary-size(fp_size), suffix::binary>> = buckets
    <<prefix::binary, fp::binary, suffix::binary>>
  end

  # Finds an empty slot in a bucket. Returns the slot index or nil.
  @spec find_empty_slot([binary()], pos_integer()) :: non_neg_integer() | nil
  defp find_empty_slot(slots, fp_size) do
    empty = <<0::size(fp_size * 8)>>
    Enum.find_index(slots, &(&1 == empty))
  end

  # ---------------------------------------------------------------------------
  # Private: insert
  # ---------------------------------------------------------------------------

  @spec insert(map(), binary()) :: {:ok, map()} | {:error, :full}
  defp insert(filter, element) do
    fp = fingerprint(element)
    b1 = primary_bucket(element, filter.capacity)
    b2 = alternate_bucket(b1, fp, filter.capacity)

    # Try to insert into primary bucket.
    slots1 = get_bucket(filter.buckets, b1, filter.bucket_size, filter.fingerprint_size)

    case find_empty_slot(slots1, filter.fingerprint_size) do
      nil ->
        # Try alternate bucket.
        slots2 = get_bucket(filter.buckets, b2, filter.bucket_size, filter.fingerprint_size)

        case find_empty_slot(slots2, filter.fingerprint_size) do
          nil ->
            # Both buckets full: kick existing entries.
            kick(filter, fp, b1)

          slot_idx ->
            new_buckets = set_slot(filter.buckets, b2, slot_idx, fp, filter.bucket_size, filter.fingerprint_size)
            {:ok, %{filter | buckets: new_buckets, num_items: filter.num_items + 1}}
        end

      slot_idx ->
        new_buckets = set_slot(filter.buckets, b1, slot_idx, fp, filter.bucket_size, filter.fingerprint_size)
        {:ok, %{filter | buckets: new_buckets, num_items: filter.num_items + 1}}
    end
  end

  # Cuckoo kicking: displace entries until an empty slot is found or max kicks reached.
  @spec kick(map(), binary(), non_neg_integer()) :: {:ok, map()} | {:error, :full}
  defp kick(filter, fp, bucket) do
    do_kick(filter, fp, bucket, 0)
  end

  defp do_kick(_filter, _fp, _bucket, kicks) when kicks >= @max_kicks do
    {:error, :full}
  end

  defp do_kick(filter, fp, bucket, kicks) do
    # Pick a random slot to evict from the current bucket.
    slot_idx = rem(kicks, filter.bucket_size)

    # Read the fingerprint currently in that slot.
    evicted_fp = get_slot(filter.buckets, bucket, slot_idx, filter.bucket_size, filter.fingerprint_size)

    # Place our fingerprint in the evicted slot.
    new_buckets = set_slot(filter.buckets, bucket, slot_idx, fp, filter.bucket_size, filter.fingerprint_size)
    updated_filter = %{filter | buckets: new_buckets}

    # Find the alternate bucket for the evicted fingerprint.
    alt = alternate_bucket(bucket, evicted_fp, filter.capacity)

    # Try to place the evicted fingerprint in its alternate bucket.
    alt_slots = get_bucket(updated_filter.buckets, alt, filter.bucket_size, filter.fingerprint_size)

    case find_empty_slot(alt_slots, filter.fingerprint_size) do
      nil ->
        # Alt bucket is also full, continue kicking.
        do_kick(updated_filter, evicted_fp, alt, kicks + 1)

      empty_slot ->
        final_buckets =
          set_slot(updated_filter.buckets, alt, empty_slot,
                   evicted_fp, filter.bucket_size, filter.fingerprint_size)

        {:ok, %{updated_filter | buckets: final_buckets, num_items: filter.num_items + 1}}
    end
  end

  # Gets a specific slot's fingerprint.
  @spec get_slot(binary(), non_neg_integer(), non_neg_integer(), pos_integer(), pos_integer()) :: binary()
  defp get_slot(buckets, bucket_idx, slot_idx, bucket_size, fp_size) do
    offset = (bucket_idx * bucket_size + slot_idx) * fp_size
    binary_part(buckets, offset, fp_size)
  end

  # ---------------------------------------------------------------------------
  # Private: lookup
  # ---------------------------------------------------------------------------

  @spec lookup(map(), binary()) :: boolean()
  defp lookup(filter, element) do
    fp = fingerprint(element)
    b1 = primary_bucket(element, filter.capacity)
    b2 = alternate_bucket(b1, fp, filter.capacity)

    slots1 = get_bucket(filter.buckets, b1, filter.bucket_size, filter.fingerprint_size)
    slots2 = get_bucket(filter.buckets, b2, filter.bucket_size, filter.fingerprint_size)

    fp in slots1 or fp in slots2
  end

  @spec lookup_to_int(map(), binary()) :: 0 | 1
  defp lookup_to_int(filter, element) do
    if lookup(filter, element), do: 1, else: 0
  end

  # ---------------------------------------------------------------------------
  # Private: count
  # ---------------------------------------------------------------------------

  @spec count_element(map(), binary()) :: non_neg_integer()
  defp count_element(filter, element) do
    fp = fingerprint(element)
    b1 = primary_bucket(element, filter.capacity)
    b2 = alternate_bucket(b1, fp, filter.capacity)

    slots1 = get_bucket(filter.buckets, b1, filter.bucket_size, filter.fingerprint_size)
    slots2 = get_bucket(filter.buckets, b2, filter.bucket_size, filter.fingerprint_size)

    count1 = Enum.count(slots1, &(&1 == fp))
    count2 = Enum.count(slots2, &(&1 == fp))

    count1 + count2
  end

  # ---------------------------------------------------------------------------
  # Private: delete
  # ---------------------------------------------------------------------------

  @spec delete_element(map(), binary()) :: {:ok, map()} | :not_found
  defp delete_element(filter, element) do
    fp = fingerprint(element)
    b1 = primary_bucket(element, filter.capacity)
    b2 = alternate_bucket(b1, fp, filter.capacity)

    empty = <<0::size(filter.fingerprint_size * 8)>>

    # Try primary bucket first.
    slots1 = get_bucket(filter.buckets, b1, filter.bucket_size, filter.fingerprint_size)

    case Enum.find_index(slots1, &(&1 == fp)) do
      nil ->
        # Try alternate bucket.
        slots2 = get_bucket(filter.buckets, b2, filter.bucket_size, filter.fingerprint_size)

        case Enum.find_index(slots2, &(&1 == fp)) do
          nil ->
            :not_found

          slot_idx ->
            new_buckets =
              set_slot(filter.buckets, b2, slot_idx, empty,
                       filter.bucket_size, filter.fingerprint_size)

            {:ok, %{filter | buckets: new_buckets,
                    num_items: filter.num_items - 1,
                    num_deletes: filter.num_deletes + 1}}
        end

      slot_idx ->
        new_buckets =
          set_slot(filter.buckets, b1, slot_idx, empty,
                   filter.bucket_size, filter.fingerprint_size)

        {:ok, %{filter | buckets: new_buckets,
                num_items: filter.num_items - 1,
                num_deletes: filter.num_deletes + 1}}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: persistence
  # ---------------------------------------------------------------------------

  @spec load_filter(binary(), map()) :: map() | nil
  defp load_filter(key, store) do
    case store.get.(key) do
      nil -> nil
      encoded when is_binary(encoded) -> deserialize_filter(encoded)
      _ -> nil
    end
  end

  @spec load_or_create_filter(binary(), map()) :: map()
  defp load_or_create_filter(key, store) do
    case load_filter(key, store) do
      nil -> new_filter(@default_capacity)
      filter -> filter
    end
  end

  @spec persist_filter!(binary(), map(), map()) :: :ok
  defp persist_filter!(key, filter, store) do
    encoded = serialize_filter(filter)
    store.put.(key, encoded, 0)
  end

  @spec serialize_filter(map()) :: binary()
  defp serialize_filter(filter) do
    metadata = %{
      capacity: filter.capacity,
      bucket_size: filter.bucket_size,
      fingerprint_size: filter.fingerprint_size,
      num_items: filter.num_items,
      num_deletes: filter.num_deletes
    }

    :erlang.term_to_binary({:cuckoo, filter.buckets, metadata})
  end

  @spec deserialize_filter(binary()) :: map() | nil
  defp deserialize_filter(encoded) do
    case safe_binary_to_term(encoded) do
      {:cuckoo, buckets, metadata} ->
        Map.merge(metadata, %{buckets: buckets})

      _ ->
        nil
    end
  end

  defp safe_binary_to_term(binary) do
    :erlang.binary_to_term(binary)
  rescue
    ArgumentError -> nil
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
