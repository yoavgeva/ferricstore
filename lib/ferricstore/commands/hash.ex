defmodule Ferricstore.Commands.Hash do
  @moduledoc """
  Handles Redis hash commands: HSET, HGET, HDEL, HMGET, HGETALL, HLEN,
  HEXISTS, HKEYS, HVALS, HSETNX, HINCRBY, HINCRBYFLOAT.

  Each hash field is stored as an individual compound key entry in the
  shared shard Bitcask:

      H:redis_key\\0field_name -> value

  This allows individual field access without reading or deserializing
  the entire hash. HGETALL scans all entries matching the hash prefix.

  ## Type Enforcement

  All hash commands check the type metadata for the key. If the key
  already exists as a different data type (list, set, zset), a
  WRONGTYPE error is returned.
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.TypeRegistry

  @doc """
  Handles a hash command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"HSET"`, `"HGET"`)
    - `args` - List of string arguments
    - `store` - Injected store map with compound key callbacks

  ## Returns

  Plain Elixir term: integer, string, list, nil, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # HSET key field value [field value ...]
  # ---------------------------------------------------------------------------

  def handle("HSET", [key | field_value_pairs], store) when length(field_value_pairs) >= 2 do
    if rem(length(field_value_pairs), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'hset' command"}
    else
      with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
        pairs = Enum.chunk_every(field_value_pairs, 2)

        added =
          Enum.reduce(pairs, 0, fn [field, value], acc ->
            compound_key = CompoundKey.hash_field(key, field)
            existing = store.compound_get.(key, compound_key)
            store.compound_put.(key, compound_key, value, 0)
            if existing == nil, do: acc + 1, else: acc
          end)

        added
      end
    end
  end

  def handle("HSET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hset' command"}
  end

  # ---------------------------------------------------------------------------
  # HGET key field
  # ---------------------------------------------------------------------------

  def handle("HGET", [key, field], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)
      store.compound_get.(key, compound_key)
    end
  end

  def handle("HGET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hget' command"}
  end

  # ---------------------------------------------------------------------------
  # HDEL key field [field ...]
  # ---------------------------------------------------------------------------

  def handle("HDEL", [key | fields], store) when fields != [] do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      deleted =
        Enum.reduce(fields, 0, fn field, acc ->
          compound_key = CompoundKey.hash_field(key, field)

          if store.compound_get.(key, compound_key) != nil do
            store.compound_delete.(key, compound_key)
            acc + 1
          else
            acc
          end
        end)

      maybe_cleanup_empty_hash(key, deleted, store)
      deleted
    end
  end

  def handle("HDEL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hdel' command"}
  end

  # ---------------------------------------------------------------------------
  # HMGET key field [field ...]
  # ---------------------------------------------------------------------------

  def handle("HMGET", [key | fields], store) when fields != [] do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      Enum.map(fields, fn field ->
        compound_key = CompoundKey.hash_field(key, field)
        store.compound_get.(key, compound_key)
      end)
    end
  end

  def handle("HMGET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hmget' command"}
  end

  # ---------------------------------------------------------------------------
  # HGETALL key
  # ---------------------------------------------------------------------------

  def handle("HGETALL", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = store.compound_scan.(key, prefix)

      # Return flat list [field1, value1, field2, value2, ...]
      Enum.flat_map(pairs, fn {field, value} -> [field, value] end)
    end
  end

  def handle("HGETALL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hgetall' command"}
  end

  # ---------------------------------------------------------------------------
  # HLEN key
  # ---------------------------------------------------------------------------

  def handle("HLEN", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      store.compound_count.(key, prefix)
    end
  end

  def handle("HLEN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hlen' command"}
  end

  # ---------------------------------------------------------------------------
  # HEXISTS key field
  # ---------------------------------------------------------------------------

  def handle("HEXISTS", [key, field], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      if store.compound_get.(key, compound_key) != nil do
        1
      else
        0
      end
    end
  end

  def handle("HEXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hexists' command"}
  end

  # ---------------------------------------------------------------------------
  # HKEYS key
  # ---------------------------------------------------------------------------

  def handle("HKEYS", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = store.compound_scan.(key, prefix)
      Enum.map(pairs, fn {field, _value} -> field end)
    end
  end

  def handle("HKEYS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hkeys' command"}
  end

  # ---------------------------------------------------------------------------
  # HVALS key
  # ---------------------------------------------------------------------------

  def handle("HVALS", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = store.compound_scan.(key, prefix)
      Enum.map(pairs, fn {_field, value} -> value end)
    end
  end

  def handle("HVALS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hvals' command"}
  end

  # ---------------------------------------------------------------------------
  # HSETNX key field value
  # ---------------------------------------------------------------------------

  def handle("HSETNX", [key, field, value], store) do
    with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      if store.compound_get.(key, compound_key) != nil do
        0
      else
        store.compound_put.(key, compound_key, value, 0)
        1
      end
    end
  end

  def handle("HSETNX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hsetnx' command"}
  end

  # ---------------------------------------------------------------------------
  # HINCRBY key field increment
  # ---------------------------------------------------------------------------

  def handle("HINCRBY", [key, field, increment_str], store) do
    with :ok <- TypeRegistry.check_or_set(key, :hash, store),
         {increment, ""} <- Integer.parse(increment_str) do
      do_hincrby(key, field, increment, store)
    else
      {:error, _} = err -> err
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("HINCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hincrby' command"}
  end

  # ---------------------------------------------------------------------------
  # HINCRBYFLOAT key field increment
  # ---------------------------------------------------------------------------

  def handle("HINCRBYFLOAT", [key, field, increment_str], store) do
    with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
      case Float.parse(increment_str) do
        {increment, ""} ->
          compound_key = CompoundKey.hash_field(key, field)
          current = store.compound_get.(key, compound_key)

          case parse_float_value(current) do
            {:ok, current_float} ->
              new_val = current_float + increment
              result_str = format_float(new_val)
              store.compound_put.(key, compound_key, result_str, 0)
              result_str

            :error ->
              {:error, "ERR hash value is not a valid float"}
          end

        :error ->
          {:error, "ERR value is not a valid float"}
      end
    end
  end

  def handle("HINCRBYFLOAT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hincrbyfloat' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp do_hincrby(key, field, increment, store) do
    compound_key = CompoundKey.hash_field(key, field)
    current = store.compound_get.(key, compound_key)

    case parse_integer_value(current) do
      {:ok, current_int} ->
        new_val = current_int + increment
        store.compound_put.(key, compound_key, Integer.to_string(new_val), 0)
        new_val

      :error ->
        {:error, "ERR hash value is not an integer"}
    end
  end

  defp maybe_cleanup_empty_hash(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_hash(key, _deleted, store) do
    prefix = CompoundKey.hash_prefix(key)

    if store.compound_count.(key, prefix) == 0 do
      TypeRegistry.delete_type(key, store)
    end
  end

  defp parse_integer_value(nil), do: {:ok, 0}

  defp parse_integer_value(str) when is_binary(str) do
    case Integer.parse(str) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  defp parse_float_value(nil), do: {:ok, 0.0}

  defp parse_float_value(str) when is_binary(str) do
    case Float.parse(str) do
      {float, ""} -> {:ok, float}
      _ ->
        # Try integer parse (e.g. "10" should work as float too)
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  end

  defp format_float(val) when is_float(val) do
    # Redis-compatible float formatting: remove trailing zeros but keep
    # at least one decimal place
    :erlang.float_to_binary(val, [:compact, decimals: 17])
  end
end
