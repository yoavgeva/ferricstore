defmodule Ferricstore.Commands.Bloom do
  @moduledoc """
  Handles Redis-compatible Bloom filter commands.

  Bloom filters are space-efficient probabilistic data structures that test
  whether an element is a member of a set. False positive matches are possible,
  but false negatives are not: an element that has been added will always be
  reported as present, but an element that has not been added may occasionally
  be reported as present.

  ## Storage

  Bloom filters are stored in the shard key-value store as tagged tuples:

      {:bloom, bit_array_binary, metadata}

  where `metadata` is a map containing:

    * `:capacity` - maximum number of elements the filter was sized for
    * `:error_rate` - target false positive rate (0.0 to 1.0)
    * `:num_hashes` - number of hash functions (k)
    * `:num_bits` - total number of bits in the bit array (m)
    * `:size` - current number of elements added

  ## Hash Functions

  Uses `:crypto.hash(:md5, data)` to generate a 128-bit digest, then derives
  k independent hash positions using the enhanced double-hashing technique
  (Kirsch & Mitzenmacher, 2006): `h_i(x) = h1(x) + i * h2(x) mod m`.

  ## Supported Commands

    * `BF.RESERVE key error_rate capacity` - creates a new Bloom filter
    * `BF.ADD key element` - adds an element (auto-creates with defaults if missing)
    * `BF.MADD key element [element ...]` - adds multiple elements
    * `BF.EXISTS key element` - checks if an element may exist
    * `BF.MEXISTS key element [element ...]` - checks multiple elements
    * `BF.CARD key` - returns the number of elements added
    * `BF.INFO key` - returns filter metadata
  """

  @default_error_rate 0.01
  @default_capacity 100

  @doc """
  Handles a Bloom filter command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"BF.RESERVE"`, `"BF.ADD"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, list, map, or `{:error, message}`.
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
      if store.exists?.(key) do
        {:error, "ERR item exists"}
      else
        filter = new_filter(error_rate, capacity)
        persist_filter!(key, filter, store)
        :ok
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
    filter = load_or_create_filter(key, store)
    {changed, new_filter} = add_element(filter, element)
    persist_filter!(key, new_filter, store)
    if changed, do: 1, else: 0
  end

  def handle("BF.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.add' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.MADD key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("BF.MADD", [key | elements], store) when elements != [] do
    filter = load_or_create_filter(key, store)

    {results, final_filter} =
      Enum.map_reduce(elements, filter, fn element, acc ->
        {changed, new_acc} = add_element(acc, element)
        {if(changed, do: 1, else: 0), new_acc}
      end)

    persist_filter!(key, final_filter, store)
    results
  end

  def handle("BF.MADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.madd' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.EXISTS key element
  # ---------------------------------------------------------------------------

  def handle("BF.EXISTS", [key, element], store) do
    case load_filter(key, store) do
      nil -> 0
      filter -> if element_exists?(filter, element), do: 1, else: 0
    end
  end

  def handle("BF.EXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.exists' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.MEXISTS key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("BF.MEXISTS", [key | elements], store) when elements != [] do
    case load_filter(key, store) do
      nil -> List.duplicate(0, length(elements))
      filter -> Enum.map(elements, &exists_to_int(filter, &1))
    end
  end

  def handle("BF.MEXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.mexists' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.CARD key
  # ---------------------------------------------------------------------------

  def handle("BF.CARD", [key], store) do
    case load_filter(key, store) do
      nil -> 0
      filter -> filter.size
    end
  end

  def handle("BF.CARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.card' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.INFO key
  # ---------------------------------------------------------------------------

  def handle("BF.INFO", [key], store) do
    case load_filter(key, store) do
      nil ->
        {:error, "ERR not found"}

      filter ->
        [
          "Capacity", filter.capacity,
          "Size", filter.size,
          "Number of filters", 1,
          "Number of items inserted", filter.size,
          "Expansion rate", 0,
          "Error rate", filter.error_rate,
          "Number of hash functions", filter.num_hashes,
          "Number of bits", filter.num_bits
        ]
    end
  end

  def handle("BF.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Private: filter construction
  # ---------------------------------------------------------------------------

  @doc false
  @spec new_filter(float(), pos_integer()) :: map()
  def new_filter(error_rate, capacity) do
    num_bits = optimal_num_bits(capacity, error_rate)
    num_hashes = optimal_num_hashes(num_bits, capacity)
    # Bit array stored as a binary of ceil(num_bits / 8) bytes, all zeros.
    byte_count = div(num_bits + 7, 8)
    bit_array = <<0::size(byte_count * 8)>>

    %{
      bit_array: bit_array,
      capacity: capacity,
      error_rate: error_rate,
      num_hashes: num_hashes,
      num_bits: num_bits,
      size: 0
    }
  end

  # Optimal number of bits (m) for given capacity (n) and error rate (p):
  #   m = -n * ln(p) / (ln(2))^2
  @spec optimal_num_bits(pos_integer(), float()) :: pos_integer()
  defp optimal_num_bits(capacity, error_rate) do
    m = -1.0 * capacity * :math.log(error_rate) / :math.pow(:math.log(2), 2)
    max(1, ceil(m))
  end

  # Optimal number of hash functions (k):
  #   k = (m / n) * ln(2)
  @spec optimal_num_hashes(pos_integer(), pos_integer()) :: pos_integer()
  defp optimal_num_hashes(num_bits, capacity) do
    k = num_bits / capacity * :math.log(2)
    max(1, round(k))
  end

  # ---------------------------------------------------------------------------
  # Private: hash position computation
  # ---------------------------------------------------------------------------

  # Uses enhanced double hashing (Kirsch & Mitzenmacher, 2006):
  #   h_i(x) = (h1(x) + i * h2(x)) mod m
  #
  # h1 and h2 are derived from the first and second 64-bit halves of
  # :crypto.hash(:md5, element).
  @spec hash_positions(binary(), pos_integer(), pos_integer()) :: [non_neg_integer()]
  defp hash_positions(element, num_hashes, num_bits) do
    <<h1::unsigned-little-64, h2::unsigned-little-64>> = :crypto.hash(:md5, element)

    Enum.map(0..(num_hashes - 1), fn i ->
      rem(h1 + i * h2, num_bits)
      |> abs()
    end)
  end

  # ---------------------------------------------------------------------------
  # Private: bit array operations
  # ---------------------------------------------------------------------------

  @spec get_bit(binary(), non_neg_integer()) :: 0 | 1
  defp get_bit(bit_array, position) do
    byte_index = div(position, 8)
    bit_offset = rem(position, 8)
    <<_::binary-size(byte_index), byte::8, _::binary>> = bit_array
    if Bitwise.band(byte, Bitwise.bsl(1, bit_offset)) != 0, do: 1, else: 0
  end

  @spec set_bit(binary(), non_neg_integer()) :: binary()
  defp set_bit(bit_array, position) do
    byte_index = div(position, 8)
    bit_offset = rem(position, 8)
    <<prefix::binary-size(byte_index), byte::8, suffix::binary>> = bit_array
    new_byte = Bitwise.bor(byte, Bitwise.bsl(1, bit_offset))
    <<prefix::binary, new_byte::8, suffix::binary>>
  end

  # ---------------------------------------------------------------------------
  # Private: add / exists
  # ---------------------------------------------------------------------------

  # Adds an element to the filter. Returns {changed, new_filter} where
  # changed is true if at least one new bit was set (element was not
  # previously present).
  @spec add_element(map(), binary()) :: {boolean(), map()}
  defp add_element(filter, element) do
    positions = hash_positions(element, filter.num_hashes, filter.num_bits)

    {new_bit_array, any_new_bit} =
      Enum.reduce(positions, {filter.bit_array, false}, fn pos, {bits, changed} ->
        if get_bit(bits, pos) == 0 do
          {set_bit(bits, pos), true}
        else
          {bits, changed}
        end
      end)

    if any_new_bit do
      {true, %{filter | bit_array: new_bit_array, size: filter.size + 1}}
    else
      {false, filter}
    end
  end

  @spec element_exists?(map(), binary()) :: boolean()
  defp element_exists?(filter, element) do
    positions = hash_positions(element, filter.num_hashes, filter.num_bits)
    Enum.all?(positions, fn pos -> get_bit(filter.bit_array, pos) == 1 end)
  end

  @spec exists_to_int(map(), binary()) :: 0 | 1
  defp exists_to_int(filter, element) do
    if element_exists?(filter, element), do: 1, else: 0
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
      nil -> new_filter(@default_error_rate, @default_capacity)
      filter -> filter
    end
  end

  @spec persist_filter!(binary(), map(), map()) :: :ok
  defp persist_filter!(key, filter, store) do
    encoded = serialize_filter(filter)
    store.put.(key, encoded, 0)
  end

  # Serializes a Bloom filter to a binary using Erlang term format.
  # The tagged tuple {:bloom, bit_array, metadata} is used so the store
  # can distinguish Bloom filter values from plain string values.
  @spec serialize_filter(map()) :: binary()
  defp serialize_filter(filter) do
    metadata = %{
      capacity: filter.capacity,
      error_rate: filter.error_rate,
      num_hashes: filter.num_hashes,
      num_bits: filter.num_bits,
      size: filter.size
    }

    :erlang.term_to_binary({:bloom, filter.bit_array, metadata})
  end

  @spec deserialize_filter(binary()) :: map() | nil
  defp deserialize_filter(encoded) do
    case safe_binary_to_term(encoded) do
      {:bloom, bit_array, metadata} ->
        Map.merge(metadata, %{bit_array: bit_array})

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

  @spec parse_float(binary(), binary()) :: {:ok, float()} | {:error, binary()}
  defp parse_float(str, name) do
    case Float.parse(str) do
      {val, ""} -> {:ok, val}
      {val, ".0"} -> {:ok, val}
      _ ->
        # Try integer parse as fallback (e.g. "1" should parse as 1.0)
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
