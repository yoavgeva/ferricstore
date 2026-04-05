defmodule Ferricstore.Store.ValueCodec do
  @moduledoc """
  Shared helpers for parsing, formatting, and encoding values.

  Extracted from `Shard` and `StateMachine` to eliminate code duplication
  (performance audit L2 / memory audit L7). Both modules delegate to this
  module instead of maintaining identical private copies.
  """

  # ---------------------------------------------------------------------------
  # Integer parsing
  # ---------------------------------------------------------------------------

  @doc "Parses a binary as an integer. Returns `{:ok, integer}` or `:error`."
  @spec parse_integer(binary()) :: {:ok, integer()} | :error
  def parse_integer(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} -> {:ok, val}
      _ -> :error
    end
  end

  # ---------------------------------------------------------------------------
  # Float parsing and formatting
  # ---------------------------------------------------------------------------

  @doc """
  Parses a binary as a float. Accepts integer strings ("10") and float
  strings ("3.14"). Returns `{:ok, float}` or `:error`.
  """
  @spec parse_float(binary()) :: {:ok, float()} | :error
  def parse_float(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} ->
        {:ok, val * 1.0}

      _ ->
        parse_float_value(str)
    end
  end

  defp parse_float_value(str) do
    case Float.parse(str) do
      {val, ""} when val not in [:infinity, :neg_infinity, :nan] -> {:ok, val}
      _ -> :error
    end
  end

  @doc """
  Formats a float for Redis INCRBYFLOAT output: compact decimals, strips
  trailing zeros and unnecessary decimal point.

  Uses `:binary.match/2` instead of `String.contains?/2` and removes the
  dead-code no-op `then` that was present in the original duplicated versions.
  """
  @spec format_float(float()) :: binary()
  def format_float(val) when is_float(val) do
    formatted = :erlang.float_to_binary(val, [:short])

    case :binary.match(formatted, ".") do
      :nomatch ->
        formatted

      _ ->
        formatted
        |> String.trim_trailing("0")
        |> String.trim_trailing(".")
    end
  end

  # ---------------------------------------------------------------------------
  # Rate limiter encoding
  # ---------------------------------------------------------------------------

  @doc """
  Encodes rate limiter state as a fixed 24-byte binary.

  ~10x faster than the old string format (`"\#{cur}:\#{start}:\#{prev}"`).
  No allocation, no parsing — just bit packing.
  """
  @spec encode_ratelimit(integer(), integer(), integer()) :: binary()
  def encode_ratelimit(cur, start, prev), do: <<cur::64, start::64, prev::64>>

  @doc """
  Decodes rate limiter state from a 24-byte binary or legacy string format.
  """
  @spec decode_ratelimit(binary()) :: {integer(), integer(), integer()}
  def decode_ratelimit(<<cur::64, start::64, prev::64>>), do: {cur, start, prev}

  def decode_ratelimit(value) when is_binary(value) do
    # Legacy fallback: decode old string-encoded values ("cur:start:prev").
    case String.split(value, ":") do
      [cur, start, prev] ->
        {String.to_integer(cur), String.to_integer(start), String.to_integer(prev)}

      _ ->
        {0, System.os_time(:millisecond), 0}
    end
  end
end
