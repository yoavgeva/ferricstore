defmodule Ferricstore.Resp.Parser do
  @moduledoc """
  Pure binary state-machine parser for the RESP3 protocol.

  Parses a binary buffer containing one or more RESP3-encoded values and returns
  all complete values along with any unparsed remainder. This module is entirely
  side-effect-free: no process state, no I/O, no message passing.

  ## Supported RESP3 types

  | Prefix | Type            | Elixir representation                  |
  |--------|-----------------|----------------------------------------|
  | `+`    | Simple string   | `{:simple, binary()}`                  |
  | `-`    | Simple error    | `{:error, binary()}`                   |
  | `:`    | Integer         | `integer()`                            |
  | `$`    | Bulk string     | `binary()` or `nil` for `$-1`          |
  | `*`    | Array           | `list()` or `nil` for `*-1`            |
  | `_`    | Null            | `nil`                                  |
  | `#`    | Boolean         | `true` or `false`                      |
  | `,`    | Double          | `float()`                              |
  | `(`    | Big number      | `integer()`                            |
  | `!`    | Blob error      | `{:error, binary()}`                   |
  | `=`    | Verbatim string | `{:verbatim, encoding, data}`          |
  | `%`    | Map             | `map()`                                |
  | `~`    | Set             | `MapSet.t()`                           |
  | `>`    | Push            | `{:push, list()}`                      |

  Inline commands (plain text terminated by `\\r\\n`) are returned as
  `{:inline, [String.t()]}`.

  ## Pipelining

  Multiple commands may be concatenated in a single buffer. `parse/1` extracts
  all complete values and returns the unparsed tail so callers can prepend the
  next TCP chunk.

  ## Examples

      iex> Ferricstore.Resp.Parser.parse("+OK\\r\\n")
      {:ok, [{:simple, "OK"}], ""}

      iex> Ferricstore.Resp.Parser.parse(":42\\r\\n:99\\r\\n")
      {:ok, [42, 99], ""}

      iex> Ferricstore.Resp.Parser.parse(":42\\r\\n:99\\r")
      {:ok, [42], ":99\\r"}
  """

  @type parsed_value ::
          {:simple, binary()}
          | {:error, binary()}
          | integer()
          | binary()
          | nil
          | boolean()
          | float()
          | {:verbatim, binary(), binary()}
          | map()
          | MapSet.t()
          | {:push, list()}
          | {:inline, [binary()]}
          | list()

  @type parse_result :: {:ok, [parsed_value()], binary()} | {:error, term()}

  @doc """
  Parses a binary buffer containing RESP3-encoded data.

  Returns `{:ok, values, rest}` where `values` is a list of all completely
  parsed RESP3 values and `rest` is the unparsed remainder of the buffer.

  When the buffer contains no complete value (partial data), returns
  `{:ok, [], buffer}` with the original buffer as the remainder.

  Returns `{:error, reason}` only for truly malformed input that cannot be
  recovered from.

  ## Parameters

    - `data` - Binary buffer containing RESP3-encoded data.

  ## Examples

      iex> Ferricstore.Resp.Parser.parse("+OK\\r\\n")
      {:ok, [{:simple, "OK"}], ""}

      iex> Ferricstore.Resp.Parser.parse("PING\\r\\n")
      {:ok, [{:inline, ["PING"]}], ""}

      iex> Ferricstore.Resp.Parser.parse("+OK\\r")
      {:ok, [], "+OK\\r"}
  """
  @spec parse(binary()) :: parse_result()
  def parse(data) when is_binary(data) do
    parse_all(data, [])
  end

  # -- Private: top-level loop ------------------------------------------------

  defp parse_all(<<>>, acc) do
    {:ok, Enum.reverse(acc), <<>>}
  end

  defp parse_all(data, acc) do
    case parse_one(data) do
      {:ok, value, rest} ->
        parse_all(rest, [value | acc])

      :incomplete ->
        {:ok, Enum.reverse(acc), data}

      {:error, _reason} = err ->
        err
    end
  end

  # -- Private: single-value dispatch ----------------------------------------

  defp parse_one(<<"+", rest::binary>>), do: parse_simple_string(rest)
  defp parse_one(<<"-", rest::binary>>), do: parse_simple_error(rest)
  defp parse_one(<<":", rest::binary>>), do: parse_integer(rest)
  defp parse_one(<<"$", rest::binary>>), do: parse_bulk_string(rest)
  defp parse_one(<<"*", rest::binary>>), do: parse_array(rest)
  defp parse_one(<<"_", rest::binary>>), do: parse_null(rest)
  defp parse_one(<<"#", rest::binary>>), do: parse_boolean(rest)
  defp parse_one(<<",", rest::binary>>), do: parse_double(rest)
  defp parse_one(<<"(", rest::binary>>), do: parse_big_number(rest)
  defp parse_one(<<"!", rest::binary>>), do: parse_blob_error(rest)
  defp parse_one(<<"=", rest::binary>>), do: parse_verbatim_string(rest)
  defp parse_one(<<"%", rest::binary>>), do: parse_map(rest)
  defp parse_one(<<"~", rest::binary>>), do: parse_set(rest)
  defp parse_one(<<">", rest::binary>>), do: parse_push(rest)
  defp parse_one(<<"|", rest::binary>>), do: parse_attribute(rest)
  defp parse_one(data), do: parse_inline(data)

  # -- Simple string: +<string>\r\n ------------------------------------------

  defp parse_simple_string(data) do
    case read_line(data) do
      {:ok, line, rest} -> {:ok, {:simple, line}, rest}
      :incomplete -> :incomplete
    end
  end

  # -- Simple error: -<string>\r\n --------------------------------------------

  defp parse_simple_error(data) do
    case read_line(data) do
      {:ok, line, rest} -> {:ok, {:error, line}, rest}
      :incomplete -> :incomplete
    end
  end

  # -- Integer: :<integer>\r\n ------------------------------------------------

  defp parse_integer(data) do
    case read_line(data) do
      {:ok, line, rest} ->
        case Integer.parse(line) do
          {n, ""} -> {:ok, n, rest}
          _ -> {:error, {:invalid_integer, line}}
        end

      :incomplete ->
        :incomplete
    end
  end

  # -- Bulk string: $<length>\r\n<data>\r\n or $-1\r\n -----------------------

  defp parse_bulk_string(data) do
    case read_line(data) do
      {:ok, "-1", rest} ->
        {:ok, nil, rest}

      {:ok, len_str, rest} ->
        case Integer.parse(len_str) do
          {len, ""} when len >= 0 -> read_bulk_data(rest, len)
          _ -> {:error, {:invalid_bulk_length, len_str}}
        end

      :incomplete ->
        :incomplete
    end
  end

  # -- Array: *<count>\r\n<elements...> or *-1\r\n ----------------------------

  defp parse_array(data) do
    case read_line(data) do
      {:ok, "-1", rest} ->
        {:ok, nil, rest}

      {:ok, count_str, rest} ->
        case Integer.parse(count_str) do
          {count, ""} when count >= 0 -> parse_elements(rest, count, [])
          _ -> {:error, {:invalid_array_count, count_str}}
        end

      :incomplete ->
        :incomplete
    end
  end

  # -- Null: _\r\n ------------------------------------------------------------

  defp parse_null(data) do
    case read_line(data) do
      {:ok, "", rest} -> {:ok, nil, rest}
      {:ok, other, _rest} -> {:error, {:invalid_null, other}}
      :incomplete -> :incomplete
    end
  end

  # -- Boolean: #t\r\n or #f\r\n ---------------------------------------------

  defp parse_boolean(data) do
    case read_line(data) do
      {:ok, "t", rest} -> {:ok, true, rest}
      {:ok, "f", rest} -> {:ok, false, rest}
      {:ok, other, _rest} -> {:error, {:invalid_boolean, other}}
      :incomplete -> :incomplete
    end
  end

  # -- Double: ,<float>\r\n ---------------------------------------------------
  # Handles: regular floats, integer-form floats, scientific notation,
  # inf, -inf, and nan (RESP3 spec §Double).

  defp parse_double(data) do
    case read_line(data) do
      {:ok, "inf", rest} -> {:ok, :infinity, rest}
      {:ok, "-inf", rest} -> {:ok, :neg_infinity, rest}
      {:ok, nan, rest} when nan in ["nan", "NaN", "NAN"] -> {:ok, :nan, rest}

      {:ok, line, rest} ->
        case parse_float_value(line) do
          {:ok, f} -> {:ok, f, rest}
          :error -> {:error, {:invalid_double, line}}
        end

      :incomplete ->
        :incomplete
    end
  end

  # -- Big number: (<big_integer>\r\n -----------------------------------------

  defp parse_big_number(data) do
    case read_line(data) do
      {:ok, line, rest} ->
        case Integer.parse(line) do
          {n, ""} -> {:ok, n, rest}
          _ -> {:error, {:invalid_big_number, line}}
        end

      :incomplete ->
        :incomplete
    end
  end

  # -- Blob error: !<length>\r\n<data>\r\n ------------------------------------

  defp parse_blob_error(data) do
    with {:ok, len_str, rest} <- read_line(data),
         {len, ""} when len >= 0 <- Integer.parse(len_str),
         {:ok, blob, rest2} <- read_bulk_data(rest, len) do
      {:ok, {:error, blob}, rest2}
    else
      :incomplete -> :incomplete
      {len, _} when is_integer(len) -> {:error, {:invalid_blob_error_length, len}}
      :error -> {:error, :invalid_blob_error_length}
      {:error, _} = err -> err
    end
  end

  # -- Verbatim string: =<length>\r\n<enc>:<data>\r\n ------------------------
  # Payload format: 3-byte encoding + ":" + data. Minimum valid length is 4.

  defp parse_verbatim_string(data) do
    with {:ok, len_str, rest} <- read_line(data),
         {len, ""} when len >= 4 <- Integer.parse(len_str) do
      parse_verbatim_payload(rest, len)
    else
      :incomplete -> :incomplete
      {len, ""} when is_integer(len) -> {:error, {:invalid_verbatim_length, len}}
      :error -> {:error, {:invalid_verbatim_length, data}}
      {_, _} -> {:error, :invalid_verbatim_length}
    end
  end

  defp parse_verbatim_payload(rest, len) do
    case read_bulk_data(rest, len) do
      {:ok, <<encoding::binary-size(3), ":", verbatim_data::binary>>, rest2} ->
        {:ok, {:verbatim, encoding, verbatim_data}, rest2}

      {:ok, _bad, _} ->
        {:error, :invalid_verbatim_payload}

      other ->
        other
    end
  end

  # -- Map: %<count>\r\n<key1><val1>... ---------------------------------------

  defp parse_map(data) do
    case read_line(data) do
      {:ok, count_str, rest} ->
        case Integer.parse(count_str) do
          {count, ""} when count >= 0 -> parse_map_pairs(rest, count, %{})
          _ -> {:error, {:invalid_map_count, count_str}}
        end

      :incomplete ->
        :incomplete
    end
  end

  # -- Set: ~<count>\r\n<elements...> -----------------------------------------

  defp parse_set(data) do
    with {:ok, count_str, rest} <- read_line(data),
         {count, ""} when count >= 0 <- Integer.parse(count_str),
         {:ok, elements, rest2} <- parse_elements(rest, count, []) do
      {:ok, MapSet.new(elements), rest2}
    else
      :incomplete -> :incomplete
      {count, _} when is_integer(count) -> {:error, {:invalid_set_count, count}}
      :error -> {:error, {:invalid_set_count, data}}
      {:error, _} = err -> err
    end
  end

  # -- Push: ><count>\r\n<elements...> ----------------------------------------

  defp parse_push(data) do
    with {:ok, count_str, rest} <- read_line(data),
         {count, ""} when count >= 0 <- Integer.parse(count_str),
         {:ok, elements, rest2} <- parse_elements(rest, count, []) do
      {:ok, {:push, elements}, rest2}
    else
      :incomplete -> :incomplete
      {count, _} when is_integer(count) -> {:error, {:invalid_push_count, count}}
      :error -> {:error, {:invalid_push_count, data}}
      {:error, _} = err -> err
    end
  end

  # -- Attribute: |<count>\r\n<key1><val1>... ---------------------------------
  # Attribute type is like a map but clients should read past it and return
  # it as metadata. We return it as {:attribute, map} and continue parsing.

  defp parse_attribute(data) do
    with {:ok, count_str, rest} <- read_line(data),
         {count, ""} when count >= 0 <- Integer.parse(count_str),
         {:ok, attrs, rest2} <- parse_map_pairs(rest, count, %{}) do
      {:ok, {:attribute, attrs}, rest2}
    else
      :incomplete -> :incomplete
      {count, _} when is_integer(count) -> {:error, {:invalid_attribute_count, count}}
      :error -> {:error, {:invalid_attribute_count, data}}
      {:error, _} = err -> err
    end
  end

  # -- Inline commands: <text>\r\n --------------------------------------------

  defp parse_inline(data) do
    case read_line(data) do
      {:ok, line, rest} ->
        tokens = String.split(line)
        {:ok, {:inline, tokens}, rest}

      :incomplete ->
        :incomplete
    end
  end

  # -- Helpers ----------------------------------------------------------------

  defp read_line(data) do
    case :binary.match(data, "\r\n") do
      {pos, 2} ->
        <<line::binary-size(pos), "\r\n", rest::binary>> = data
        {:ok, line, rest}

      :nomatch ->
        :incomplete
    end
  end

  defp read_bulk_data(data, len) do
    needed = len + 2

    if byte_size(data) >= needed do
      case data do
        <<payload::binary-size(len), "\r\n", rest::binary>> ->
          {:ok, payload, rest}

        _ ->
          {:error, :bulk_crlf_missing}
      end
    else
      :incomplete
    end
  end

  defp parse_elements(rest, 0, acc) do
    {:ok, Enum.reverse(acc), rest}
  end

  defp parse_elements(data, remaining, acc) do
    case parse_one(data) do
      {:ok, value, rest} ->
        parse_elements(rest, remaining - 1, [value | acc])

      :incomplete ->
        :incomplete

      {:error, _reason} = err ->
        err
    end
  end

  defp parse_map_pairs(rest, 0, acc) do
    {:ok, acc, rest}
  end

  defp parse_map_pairs(data, remaining, acc) do
    with {:ok, key, rest1} <- parse_one(data),
         {:ok, value, rest2} <- parse_one(rest1) do
      parse_map_pairs(rest2, remaining - 1, Map.put(acc, key, value))
    else
      :incomplete -> :incomplete
      {:error, _reason} = err -> err
    end
  end

  # Parses a RESP3 double string. Handles:
  #   - regular floats: "1.5", "-3.14"
  #   - integer-form doubles: "10", "-42"
  #   - scientific notation: "1.5e10", "1e5", "1.0E-3"
  # Returns {:ok, float} or :error.
  defp parse_float_value(str) do
    # Normalize: if no "." but has "e"/"E", insert ".0" before the exponent
    # so String.to_float can parse it (e.g. "1e5" → "1.0e5")
    normalized =
      if String.contains?(str, ".") do
        str
      else
        case String.split(str, ~r/[eE]/, parts: 2) do
          [mantissa, exp] -> "#{mantissa}.0e#{exp}"
          [_] -> str
        end
      end

    case Float.parse(normalized) do
      {f, ""} -> {:ok, f}
      _ -> :error
    end
  end
end
