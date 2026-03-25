defmodule FerricstoreEcto.FieldCodec do
  @moduledoc """
  Type-aware serialization of Ecto schema fields to and from FerricStore
  hash string values.

  Every value stored in a FerricStore hash field is a binary string.
  This module provides deterministic encode/decode round-trips for all
  standard Ecto types, ensuring that values survive a cache round-trip
  without data loss or type coercion.

  ## Supported Types

  | Ecto Type | Encoded Format | Example |
  |-----------|---------------|---------|
  | `:string` | Raw string | `"Alice"` |
  | `:binary` | Base64 | `"aGVsbG8="` |
  | `:integer` | Integer string | `"42"` |
  | `:id` | Integer string | `"42"` |
  | `:binary_id` | Raw string (UUID) | `"a1b2c3..."` |
  | `:float` | Float string | `"3.14"` |
  | `:boolean` | `"true"` / `"false"` | `"true"` |
  | `:utc_datetime` | ISO 8601 | `"2024-01-15T10:30:00Z"` |
  | `:utc_datetime_usec` | ISO 8601 (usec) | `"2024-01-15T10:30:00.123456Z"` |
  | `:naive_datetime` | ISO 8601 | `"2024-01-15T10:30:00"` |
  | `:naive_datetime_usec` | ISO 8601 (usec) | `"2024-01-15T10:30:00.123456"` |
  | `:date` | ISO 8601 | `"2024-01-15"` |
  | `:decimal` | Decimal string | `"99.99"` |
  | `:map` / `{:map, _}` | JSON | `"{\\\"key\\\":\\\"val\\\"}"` |
  | `{:array, _}` | JSON | `"[1,2,3]"` |
  | Unknown types | JSON (fallback) | Via `Jason.encode!/1` |

  ## Nil Handling

  Nil values are encoded as a special sentinel byte (`"\\0"`) to distinguish
  `nil` from the empty string `""`, which is a valid `:string` value. On
  decode, both `nil` input and the sentinel return `nil`.

  ## Schema-Level Operations

  Two convenience functions operate on entire Ecto structs:

  * `serialize_fields/1` -- Converts all non-PK fields of an Ecto struct
    to a `%{String.t() => String.t()}` map suitable for `FerricStore.hset/2`.
    Primary key fields are excluded because they are encoded in the cache
    key, not the cached value.

  * `deserialize_fields/2` -- Converts a `%{String.t() => String.t()}` hash
    map back to a keyword-compatible map for `struct/2`. Missing fields
    (not present in the hash) get `nil`, handling schema evolution gracefully:
    new fields added after a cache entry was written simply get defaults.
  """

  @nil_sentinel "\0"

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Encodes an Ecto field value to a binary string suitable for storage
  in a FerricStore hash field.

  Returns `nil` when the input value is `nil`.

  ## Examples

      iex> FerricstoreEcto.FieldCodec.encode(:string, "hello")
      "hello"

      iex> FerricstoreEcto.FieldCodec.encode(:integer, 42)
      "42"

      iex> FerricstoreEcto.FieldCodec.encode(:boolean, true)
      "true"

      iex> FerricstoreEcto.FieldCodec.encode(:string, nil)
      "\\0"

  """
  @spec encode(atom() | tuple(), term()) :: binary()
  def encode(_type, nil), do: @nil_sentinel
  def encode(:string, val) when is_binary(val), do: val
  def encode(:binary, val) when is_binary(val), do: Base.encode64(val)
  def encode(:integer, val) when is_integer(val), do: Integer.to_string(val)
  def encode(:id, val) when is_integer(val), do: Integer.to_string(val)
  def encode(:binary_id, val) when is_binary(val), do: val
  def encode(:float, val) when is_float(val), do: Float.to_string(val)
  def encode(:float, val) when is_integer(val), do: Float.to_string(val / 1)
  def encode(:boolean, true), do: "true"
  def encode(:boolean, false), do: "false"
  def encode(:utc_datetime, %DateTime{} = dt), do: DateTime.to_iso8601(dt)
  def encode(:utc_datetime_usec, %DateTime{} = dt), do: DateTime.to_iso8601(dt)
  def encode(:naive_datetime, %NaiveDateTime{} = ndt), do: NaiveDateTime.to_iso8601(ndt)
  def encode(:naive_datetime_usec, %NaiveDateTime{} = ndt), do: NaiveDateTime.to_iso8601(ndt)
  def encode(:date, %Date{} = d), do: Date.to_iso8601(d)
  def encode(:decimal, %Decimal{} = d), do: Decimal.to_string(d)
  def encode(:map, val) when is_map(val), do: Jason.encode!(val)
  def encode({:map, _}, val) when is_map(val), do: Jason.encode!(val)
  def encode({:array, _}, val) when is_list(val), do: Jason.encode!(val)
  def encode(_type, val), do: Jason.encode!(val)

  @doc """
  Decodes a binary string from a FerricStore hash field back to the
  corresponding Elixir/Ecto type.

  Returns `nil` when the input is the nil sentinel or `nil`.

  ## Examples

      iex> FerricstoreEcto.FieldCodec.decode(:string, "hello")
      "hello"

      iex> FerricstoreEcto.FieldCodec.decode(:integer, "42")
      42

      iex> FerricstoreEcto.FieldCodec.decode(:boolean, "true")
      true

      iex> FerricstoreEcto.FieldCodec.decode(:string, "\\0")
      nil

  """
  @spec decode(atom() | tuple(), binary() | nil) :: term()
  def decode(_type, nil), do: nil
  def decode(_type, @nil_sentinel), do: nil
  def decode(:string, val), do: val
  def decode(:binary, val), do: Base.decode64!(val)
  def decode(:integer, val), do: String.to_integer(val)
  def decode(:id, val), do: String.to_integer(val)
  def decode(:binary_id, val), do: val
  def decode(:float, val), do: String.to_float(val)
  def decode(:boolean, "true"), do: true
  def decode(:boolean, "false"), do: false
  def decode(:utc_datetime, val), do: parse_datetime(val)
  def decode(:utc_datetime_usec, val), do: parse_datetime_usec(val)
  def decode(:naive_datetime, val), do: parse_naive_datetime(val)
  def decode(:naive_datetime_usec, val), do: parse_naive_datetime_usec(val)
  def decode(:date, val), do: Date.from_iso8601!(val)
  def decode(:decimal, val), do: Decimal.new(val)
  def decode(:map, val), do: Jason.decode!(val)
  def decode({:map, _}, val), do: Jason.decode!(val)
  def decode({:array, _}, val), do: Jason.decode!(val)
  def decode(_type, val), do: Jason.decode!(val)

  # ---------------------------------------------------------------------------
  # Schema-level helpers
  # ---------------------------------------------------------------------------

  @doc """
  Serializes all non-association, non-primary-key fields of an Ecto struct
  to a `%{String.t() => String.t()}` map suitable for `FerricStore.hset/2`.

  Primary key fields are excluded because they are part of the cache key,
  not the cached value. Associations (belongs_to foreign keys are included
  because they are regular fields; the association struct itself is never
  stored).
  """
  @spec serialize_fields(Ecto.Schema.t()) :: %{String.t() => String.t()}
  def serialize_fields(struct) do
    schema = struct.__struct__
    fields = schema.__schema__(:fields)
    pk_fields = schema.__schema__(:primary_key)

    Map.new(fields -- pk_fields, fn field ->
      type = schema.__schema__(:type, field)
      value = Map.get(struct, field)
      {Atom.to_string(field), encode(type, value)}
    end)
  end

  @doc """
  Deserializes a `%{String.t() => String.t()}` hash map from FerricStore
  back to keyword-compatible field values for `struct/2`.

  Missing fields (not present in the hash map) use the schema's default
  value (nil for most types). This handles schema evolution gracefully:
  new fields added after the cache entry was written simply get defaults.
  """
  @spec deserialize_fields(module(), %{String.t() => String.t()}) :: map()
  def deserialize_fields(schema, hash_map) do
    fields = schema.__schema__(:fields)
    pk_fields = schema.__schema__(:primary_key)

    Map.new(fields -- pk_fields, fn field ->
      type = schema.__schema__(:type, field)
      str_key = Atom.to_string(field)

      case Map.get(hash_map, str_key) do
        nil -> {field, nil}
        val -> {field, decode(type, val)}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp parse_datetime(val) do
    case DateTime.from_iso8601(val) do
      {:ok, dt, _offset} -> DateTime.truncate(dt, :second)
      {:error, _} -> nil
    end
  end

  defp parse_datetime_usec(val) do
    case DateTime.from_iso8601(val) do
      {:ok, dt, _offset} -> DateTime.truncate(dt, :microsecond)
      {:error, _} -> nil
    end
  end

  defp parse_naive_datetime(val) do
    case NaiveDateTime.from_iso8601(val) do
      {:ok, ndt} -> NaiveDateTime.truncate(ndt, :second)
      {:error, _} -> nil
    end
  end

  defp parse_naive_datetime_usec(val) do
    case NaiveDateTime.from_iso8601(val) do
      {:ok, ndt} -> NaiveDateTime.truncate(ndt, :microsecond)
      {:error, _} -> nil
    end
  end
end
