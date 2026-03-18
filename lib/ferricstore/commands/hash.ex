defmodule Ferricstore.Commands.Hash do
  @moduledoc """
  Handles Redis hash commands: HSET, HGET, HDEL, HMGET, HGETALL, HLEN,
  HEXISTS, HKEYS, HVALS, HSETNX, HINCRBY, HINCRBYFLOAT, HEXPIRE, HTTL,
  HPERSIST, HSCAN, HRANDFIELD.

  Each hash field is stored as an individual compound key entry in the
  shared shard Bitcask:

      H:redis_key\\0field_name -> value

  This allows individual field access without reading or deserializing
  the entire hash. HGETALL scans all entries matching the hash prefix.

  ## Hash Field TTL (Redis 7.4+)

  Individual hash fields can have per-field expiry via `HEXPIRE`, `HTTL`,
  and `HPERSIST`. The expiry is stored as the `expire_at_ms` timestamp on
  each compound key entry.

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
  # HEXPIRE key seconds FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Sets a TTL (in seconds) on individual hash fields.
  # Returns: 1 = expiry set, -2 = field/key does not exist.
  def handle("HEXPIRE", [key, seconds_str, "FIELDS", count_str | fields], store) do
    with {:ok, seconds} <- parse_positive_integer(seconds_str, "seconds"),
         {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        expire_at_ms = System.os_time(:millisecond) + seconds * 1000

        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case store.compound_get_meta.(key, compound_key) do
            nil ->
              -2

            {value, _old_expire} ->
              store.compound_put.(key, compound_key, value, expire_at_ms)
              1
          end
        end)
      end
    end
  end

  def handle("HEXPIRE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hexpire' command"}
  end

  # ---------------------------------------------------------------------------
  # HTTL key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Returns remaining TTL (seconds) for individual hash fields.
  # Returns: TTL >= 0, -1 = no expiry, -2 = field/key does not exist.
  def handle("HTTL", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        now = System.os_time(:millisecond)

        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case store.compound_get_meta.(key, compound_key) do
            nil ->
              -2

            {_value, 0} ->
              -1

            {_value, expire_at_ms} ->
              remaining_ms = expire_at_ms - now
              if remaining_ms > 0, do: div(remaining_ms, 1000), else: -2
          end
        end)
      end
    end
  end

  def handle("HTTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'httl' command"}
  end

  # ---------------------------------------------------------------------------
  # HPERSIST key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Removes expiry from individual hash fields, making them persistent.
  # Returns: 1 = expiry removed, -1 = no expiry set, -2 = field/key does not exist.
  def handle("HPERSIST", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case store.compound_get_meta.(key, compound_key) do
            nil ->
              -2

            {_value, 0} ->
              -1

            {value, _expire_at_ms} ->
              store.compound_put.(key, compound_key, value, 0)
              1
          end
        end)
      end
    end
  end

  def handle("HPERSIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hpersist' command"}
  end

  # ---------------------------------------------------------------------------
  # HSCAN key cursor [MATCH pattern] [COUNT count]
  # ---------------------------------------------------------------------------

  def handle("HSCAN", [key, cursor_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store),
         {:ok, cursor} <- parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- parse_hscan_opts(opts) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = store.compound_scan.(key, prefix)

      filtered =
        case match_pattern do
          nil ->
            pairs

          pattern ->
            regex = glob_to_regex(pattern)
            Enum.filter(pairs, fn {field, _value} -> Regex.match?(regex, field) end)
        end

      {next_cursor, batch} = paginate(filtered, cursor, count)
      elements = Enum.flat_map(batch, fn {field, value} -> [field, value] end)
      [next_cursor, elements]
    end
  end

  def handle("HSCAN", [_key], _store) do
    {:error, "ERR wrong number of arguments for 'hscan' command"}
  end

  def handle("HSCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'hscan' command"}
  end

  # ---------------------------------------------------------------------------
  # HRANDFIELD key [count [WITHVALUES]]
  # ---------------------------------------------------------------------------

  def handle("HRANDFIELD", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = store.compound_scan.(key, prefix)

      case pairs do
        [] -> nil
        _ ->
          {field, _value} = Enum.random(pairs)
          field
      end
    end
  end

  def handle("HRANDFIELD", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      case Integer.parse(count_str) do
        {count, ""} ->
          prefix = CompoundKey.hash_prefix(key)
          pairs = store.compound_scan.(key, prefix)
          select_random_fields(pairs, count, false)

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("HRANDFIELD", [key, count_str, withvalues_str], store) do
    if String.upcase(withvalues_str) != "WITHVALUES" do
      {:error, "ERR syntax error"}
    else
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        case Integer.parse(count_str) do
          {count, ""} ->
            prefix = CompoundKey.hash_prefix(key)
            pairs = store.compound_scan.(key, prefix)
            select_random_fields(pairs, count, true)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end
      end
    end
  end

  def handle("HRANDFIELD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hrandfield' command"}
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
      {float, ""} ->
        {:ok, float}

      _ ->
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  end

  defp format_float(val) when is_float(val) do
    :erlang.float_to_binary(val, [:compact, decimals: 17])
  end

  defp parse_positive_integer(str, label) do
    case Integer.parse(str) do
      {int, ""} when int > 0 -> {:ok, int}
      _ -> {:error, "ERR #{label} is not a positive integer"}
    end
  end

  defp validate_field_count(count, fields) do
    if count == length(fields) do
      :ok
    else
      {:error, "ERR number of fields does not match the count argument"}
    end
  end

  # ---------------------------------------------------------------------------
  # HSCAN helpers
  # ---------------------------------------------------------------------------

  defp parse_cursor(cursor_str) do
    case Integer.parse(cursor_str) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:error, "ERR invalid cursor"}
    end
  end

  defp parse_hscan_opts(opts), do: do_parse_hscan_opts(opts, nil, 10)

  defp do_parse_hscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_hscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_hscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_hscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_hscan_opts([_ | _], _match, _count) do
    {:error, "ERR syntax error"}
  end

  defp paginate(items, cursor, count) do
    total = length(items)

    if cursor >= total do
      {"0", []}
    else
      batch = Enum.slice(items, cursor, count)
      next_pos = cursor + length(batch)

      if next_pos >= total do
        {"0", batch}
      else
        {Integer.to_string(next_pos), batch}
      end
    end
  end

  defp glob_to_regex(pattern) do
    regex_str =
      pattern
      |> String.graphemes()
      |> Enum.map_join(&escape_glob_char/1)

    Regex.compile!("^#{regex_str}$")
  end

  defp escape_glob_char("*"), do: ".*"
  defp escape_glob_char("?"), do: "."
  defp escape_glob_char(char), do: Regex.escape(char)

  defp select_random_fields(pairs, count, with_values) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        selected = Enum.take_random(pairs, count)

        if with_values do
          Enum.flat_map(selected, fn {field, value} -> [field, value] end)
        else
          Enum.map(selected, fn {field, _value} -> field end)
        end

      count < 0 ->
        abs_count = abs(count)

        if pairs == [] do
          []
        else
          selected = for _ <- 1..abs_count, do: Enum.random(pairs)

          if with_values do
            Enum.flat_map(selected, fn {field, value} -> [field, value] end)
          else
            Enum.map(selected, fn {field, _value} -> field end)
          end
        end
    end
  end
end
