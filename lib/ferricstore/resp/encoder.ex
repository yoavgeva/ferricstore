defmodule Ferricstore.Resp.Encoder do
  @moduledoc """
  Encodes Elixir terms into RESP3 wire format.

  Returns `iodata()` rather than flat binaries to avoid unnecessary copying.
  Callers should use `:gen_tcp.send/2` or `IO.iodata_to_binary/1` as needed.

  ## Type mappings

  | Elixir term                    | RESP3 encoding                            |
  |-------------------------------|-------------------------------------------|
  | `:ok`                         | `+OK\\r\\n`                                |
  | `{:simple, str}`              | `+str\\r\\n`                               |
  | `{:error, msg}`               | `-msg\\r\\n`                               |
  | `integer()`                   | `:N\\r\\n`                                 |
  | `binary()`                    | `$len\\r\\ndata\\r\\n`                      |
  | `nil`                         | `_\\r\\n`                                  |
  | `list()`                      | `*len\\r\\n` + each element                |
  | `map()`                       | `%len\\r\\n` + key/value pairs             |
  | `true`                        | `#t\\r\\n`                                 |
  | `false`                       | `#f\\r\\n`                                 |
  | `float()`                     | `,val\\r\\n`                               |
  | `{:push, list}`               | `>len\\r\\n` + each element                |
  | `{:verbatim, enc, data}`      | `=len\\r\\nenc:data\\r\\n`                  |
  | `{:blob_error, binary}`       | `!len\\r\\ndata\\r\\n`                      |
  | big integer (outside int64)   | `(N\\r\\n`                                 |

  ## Examples

      iex> Ferricstore.Resp.Encoder.encode(:ok) |> IO.iodata_to_binary()
      "+OK\\r\\n"

      iex> Ferricstore.Resp.Encoder.encode(42) |> IO.iodata_to_binary()
      ":42\\r\\n"

      iex> Ferricstore.Resp.Encoder.encode("hello") |> IO.iodata_to_binary()
      "$5\\r\\nhello\\r\\n"
  """

  @crlf "\r\n"

  @type encodable ::
          :ok
          | {:simple, binary()}
          | {:error, binary()}
          | {:blob_error, binary()}
          | integer()
          | binary()
          | nil
          | boolean()
          | float()
          | :infinity
          | :neg_infinity
          | :nan
          | list()
          | MapSet.t()
          | map()
          | {:push, list()}
          | {:verbatim, binary(), binary()}

  # Signed 64-bit integer boundaries
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808

  @doc """
  Encodes an Elixir term into RESP3 wire format as `iodata()`.

  ## Parameters

    - `term` - The Elixir term to encode. See the module documentation for
      the complete mapping of Elixir types to RESP3 types.

  ## Returns

  `iodata()` representing the RESP3-encoded value. Use `IO.iodata_to_binary/1`
  to flatten if needed, or pass directly to `:gen_tcp.send/2`.

  ## Examples

      iex> Ferricstore.Resp.Encoder.encode(nil) |> IO.iodata_to_binary()
      "_\\r\\n"

      iex> Ferricstore.Resp.Encoder.encode([1, 2, 3]) |> IO.iodata_to_binary()
      "*3\\r\\n:1\\r\\n:2\\r\\n:3\\r\\n"
  """
  @spec encode(encodable()) :: iodata()
  def encode(:ok), do: ["+OK", @crlf]

  def encode({:simple, str}) when is_binary(str) do
    validate_no_crlf!(str, :simple_string)
    ["+", str, @crlf]
  end

  def encode({:error, msg}) when is_binary(msg) do
    validate_no_crlf!(msg, :simple_error)
    ["-", msg, @crlf]
  end

  # Handle non-binary error reasons (e.g. atoms like :noproc from GenServer exits)
  def encode({:error, reason}) when is_atom(reason) do
    msg = "ERR #{Atom.to_string(reason)}"
    ["-", msg, @crlf]
  end

  def encode({:blob_error, msg}) when is_binary(msg) do
    ["!", Integer.to_string(byte_size(msg)), @crlf, msg, @crlf]
  end

  def encode(value) when is_integer(value) and value >= @min_int64 and value <= @max_int64 do
    [":", Integer.to_string(value), @crlf]
  end

  def encode(value) when is_integer(value) do
    ["(", Integer.to_string(value), @crlf]
  end

  def encode(nil), do: ["_", @crlf]

  def encode(true), do: ["#t", @crlf]

  def encode(false), do: ["#f", @crlf]

  def encode(:infinity), do: [",inf", @crlf]

  def encode(:neg_infinity), do: [",-inf", @crlf]

  def encode(:nan), do: [",nan", @crlf]

  def encode(value) when is_float(value), do: [",", Float.to_string(value), @crlf]

  def encode(value) when is_binary(value) do
    ["$", Integer.to_string(byte_size(value)), @crlf, value, @crlf]
  end

  def encode({:push, elements}) when is_list(elements) do
    [">", Integer.to_string(length(elements)), @crlf | encode_list_elements(elements)]
  end

  def encode({:verbatim, encoding, data}) when is_binary(encoding) and is_binary(data) do
    payload_len = byte_size(encoding) + 1 + byte_size(data)
    ["=", Integer.to_string(payload_len), @crlf, encoding, ":", data, @crlf]
  end

  def encode(values) when is_list(values) do
    ["*", Integer.to_string(length(values)), @crlf | encode_list_elements(values)]
  end

  def encode(%MapSet{} = set) do
    elements = MapSet.to_list(set)
    ["~", Integer.to_string(length(elements)), @crlf | encode_list_elements(elements)]
  end

  def encode(map) when is_map(map) and not is_struct(map) do
    ["%", Integer.to_string(map_size(map)), @crlf | encode_map_pairs(map)]
  end

  # -- Private helpers --------------------------------------------------------

  defp encode_list_elements(elements) do
    Enum.flat_map(elements, &List.wrap(encode(&1)))
  end

  defp encode_map_pairs(map) do
    Enum.flat_map(map, fn {key, value} ->
      List.wrap(encode(key)) ++ List.wrap(encode(value))
    end)
  end

  defp validate_no_crlf!(str, type) do
    if String.contains?(str, "\r\n") do
      label =
        case type do
          :simple_string -> "simple string"
          :simple_error -> "simple error"
        end

      raise ArgumentError,
            "RESP3 #{label} must not contain \\r\\n (CRLF), got: #{inspect(str)}"
    end
  end
end
