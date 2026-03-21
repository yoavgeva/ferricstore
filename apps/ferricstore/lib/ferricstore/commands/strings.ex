# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Strings do
  @moduledoc """
  Handles Redis string commands.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  ## Supported commands

    * `GET key` — returns the value or `nil`
    * `SET key value [EX secs | PX ms] [NX | XX]` — sets a key with optional expiry/conditions
    * `DEL key [key ...]` — deletes keys, returns count deleted
    * `EXISTS key [key ...]` — returns count of existing keys
    * `MGET key [key ...]` — returns list of values (nil for missing)
    * `MSET key value [key value ...]` — sets multiple keys atomically
    * `INCR key` — increment integer value by 1
    * `DECR key` — decrement integer value by 1
    * `INCRBY key increment` — increment integer value by given amount
    * `DECRBY key decrement` — decrement integer value by given amount
    * `INCRBYFLOAT key increment` — increment float value by given amount
    * `APPEND key value` — append to value, return new length
    * `STRLEN key` — return byte length of value
    * `GETSET key value` — set key, return old value
    * `GETDEL key` — get value and delete atomically
    * `GETEX key [EX s | PX ms | EXAT ts | PXAT ms-ts | PERSIST]` — get and update TTL
    * `SETNX key value` — set if not exists
    * `SETEX key seconds value` — set with expiry in seconds
    * `PSETEX key milliseconds value` — set with expiry in milliseconds
    * `GETRANGE key start end` — return substring by byte range
    * `SETRANGE key offset value` — overwrite part of string at offset
    * `MSETNX key value [key value ...]` — set multiple only if none exist
  """

  @doc """
  Handles a string command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"GET"`, `"SET"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks
      and atomic operations like `incr`, `append`, etc.

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, string, list, or `{:error, message}`.
  """
  @max_key_bytes 65_535

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  # ---------------------------------------------------------------------------
  # TYPE -- delegated here from tests; canonical handler is Generic
  # ---------------------------------------------------------------------------

  def handle("TYPE", [key], store) do
    {:simple, Ferricstore.Store.TypeRegistry.get_type(key, store)}
  end

  def handle("TYPE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'type' command"}
  end

  def handle("GET", [""], _store), do: {:error, "ERR empty key"}
  def handle("GET", [key], _store) when byte_size(key) > 65_535, do: {:error, "ERR key too large"}

  def handle("GET", [key], store) do
    case store.get.(key) do
      nil ->
        # Plain key is nil. Check if this is a data structure key (compound keys).
        if is_map_key(store, :compound_get) do
          type_key = Ferricstore.Store.CompoundKey.type_key(key)
          case store.compound_get.(key, type_key) do
            nil -> nil
            _type_str -> @wrongtype_error
          end
        else
          nil
        end
      value when is_binary(value) -> maybe_check_type(value)
      other -> other
    end
  end

  def handle("GET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'get' command"}
  end

  def handle("SET", ["", _value | _opts], _store), do: {:error, "ERR empty key"}
  def handle("SET", [key, _value | _opts], _store) when byte_size(key) > 65_535, do: {:error, "ERR key too large"}
  def handle("SET", [key, value | opts], store), do: do_set(key, value, opts, store)

  def handle("SET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'set' command"}
  end

  def handle("DEL", [], _store) do
    {:error, "ERR wrong number of arguments for 'del' command"}
  end

  def handle("DEL", keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      if do_del_key(key, store), do: acc + 1, else: acc
    end)
  end

  # Deletes a single key, handling both plain string keys and data structure
  # keys that use compound sub-keys. Returns `true` if the key existed and
  # was deleted, `false` otherwise.
  defp do_del_key(key, store) do
    alias Ferricstore.Store.{CompoundKey, TypeRegistry}

    # Check for data structure type metadata when compound operations are
    # available (the store has compound_get). When they are not available
    # (e.g. raw Router-based store without data structure support), fall
    # through to plain key deletion.
    has_compound? = is_map_key(store, :compound_get)

    if has_compound? do
      type_key = CompoundKey.type_key(key)

      case store.compound_get.(key, type_key) do
        nil ->
          # No type metadata -- plain string key
          if store.exists?.(key) do
            store.delete.(key)
            true
          else
            false
          end

        type_str ->
          # Data structure key -- delete compound sub-keys, then type metadata.
          # Lists store data as serialized Erlang terms in the plain key store,
          # so we must also delete the plain key for list types.
          prefix =
            case type_str do
              "hash" -> CompoundKey.hash_prefix(key)
              "list" -> CompoundKey.list_prefix(key)
              "set" -> CompoundKey.set_prefix(key)
              "zset" -> CompoundKey.zset_prefix(key)
            end

          store.compound_delete_prefix.(key, prefix)
          if type_str == "list" do
            meta_key = CompoundKey.list_meta_key(key)
            store.compound_delete.(key, meta_key)
          end
          TypeRegistry.delete_type(key, store)
          true
      end
    else
      if store.exists?.(key) do
        store.delete.(key)
        true
      else
        false
      end
    end
  end

  def handle("EXISTS", [], _store) do
    {:error, "ERR wrong number of arguments for 'exists' command"}
  end

  def handle("EXISTS", keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      exists = store.exists?.(key)
      # Also check TypeRegistry for compound-key-based data structures
      # (lists, hashes, sets, zsets) that don't use the plain key store.
      exists = exists or (is_map_key(store, :compound_get) and
        store.compound_get.(key, Ferricstore.Store.CompoundKey.type_key(key)) != nil)
      if exists, do: acc + 1, else: acc
    end)
  end

  def handle("MGET", [], _store) do
    {:error, "ERR wrong number of arguments for 'mget' command"}
  end

  def handle("MGET", keys, store), do: Enum.map(keys, &store.get.(&1))

  def handle("MSET", [], _store) do
    {:error, "ERR wrong number of arguments for 'mset' command"}
  end

  def handle("MSET", args, store) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'mset' command"}
    else
      pairs = Enum.chunk_every(args, 2)
      bad_key = Enum.find(pairs, fn [k, _v] -> k == "" or byte_size(k) > @max_key_bytes end)

      if bad_key do
        {:error, "ERR key too large or empty"}
      else
        Enum.each(pairs, fn [k, v] -> store.put.(k, v, 0) end)
        :ok
      end
    end
  end

  # ---------------------------------------------------------------------------
  # INCR / DECR / INCRBY / DECRBY
  # ---------------------------------------------------------------------------

  def handle("INCR", [key], store), do: store.incr.(key, 1)
  def handle("INCR", _args, _store), do: {:error, "ERR wrong number of arguments for 'incr' command"}

  def handle("DECR", [key], store), do: store.incr.(key, -1)
  def handle("DECR", _args, _store), do: {:error, "ERR wrong number of arguments for 'decr' command"}

  def handle("INCRBY", [key, delta_str], store) do
    case Integer.parse(delta_str) do
      {delta, ""} -> store.incr.(key, delta)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("INCRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'incrby' command"}

  def handle("DECRBY", [key, delta_str], store) do
    case Integer.parse(delta_str) do
      {delta, ""} -> store.incr.(key, -delta)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("DECRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'decrby' command"}

  # ---------------------------------------------------------------------------
  # INCRBYFLOAT
  # ---------------------------------------------------------------------------

  def handle("INCRBYFLOAT", [key, delta_str], store) do
    case parse_float_arg(delta_str) do
      {:ok, delta} ->
        case store.incr_float.(key, delta) do
          {:ok, new_str} -> new_str
          {:error, _} = err -> err
        end

      :error ->
        {:error, "ERR value is not a valid float"}
    end
  end

  def handle("INCRBYFLOAT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'incrbyfloat' command"}

  # ---------------------------------------------------------------------------
  # APPEND
  # ---------------------------------------------------------------------------

  def handle("APPEND", [key, value], store) do
    {:ok, new_len} = store.append.(key, value)
    new_len
  end

  def handle("APPEND", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'append' command"}

  # ---------------------------------------------------------------------------
  # STRLEN
  # ---------------------------------------------------------------------------

  def handle("STRLEN", [key], store) do
    case store.get.(key) do
      nil -> 0
      value -> byte_size(value)
    end
  end

  def handle("STRLEN", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'strlen' command"}

  # ---------------------------------------------------------------------------
  # GETSET (deprecated but supported)
  # ---------------------------------------------------------------------------

  def handle("GETSET", [key, value], store), do: store.getset.(key, value)

  def handle("GETSET", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getset' command"}

  # ---------------------------------------------------------------------------
  # GETDEL
  # ---------------------------------------------------------------------------

  def handle("GETDEL", [key], store), do: store.getdel.(key)

  def handle("GETDEL", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getdel' command"}

  # ---------------------------------------------------------------------------
  # GETEX
  # ---------------------------------------------------------------------------

  def handle("GETEX", [key], store), do: store.get.(key)

  def handle("GETEX", [key | opts], store), do: do_getex(key, opts, store)

  def handle("GETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getex' command"}

  # ---------------------------------------------------------------------------
  # SETNX
  # ---------------------------------------------------------------------------

  def handle("SETNX", [key, value], store) do
    if store.exists?.(key), do: 0, else: (store.put.(key, value, 0); 1)
  end

  def handle("SETNX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setnx' command"}

  # ---------------------------------------------------------------------------
  # SETEX
  # ---------------------------------------------------------------------------

  def handle("SETEX", [key, secs_str, value], store) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        expire_at_ms = Ferricstore.HLC.now_ms() + secs * 1_000
        store.put.(key, value, expire_at_ms)

      {_secs, ""} ->
        {:error, "ERR invalid expire time in 'setex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("SETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setex' command"}

  # ---------------------------------------------------------------------------
  # PSETEX
  # ---------------------------------------------------------------------------

  def handle("PSETEX", [key, ms_str, value], store) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms > 0 ->
        expire_at_ms = Ferricstore.HLC.now_ms() + ms
        store.put.(key, value, expire_at_ms)

      {_ms, ""} ->
        {:error, "ERR invalid expire time in 'psetex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("PSETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'psetex' command"}

  # ---------------------------------------------------------------------------
  # GETRANGE
  # ---------------------------------------------------------------------------

  def handle("GETRANGE", [key, start_str, end_str], store) do
    with {start_idx, ""} <- Integer.parse(start_str),
         {end_idx, ""} <- Integer.parse(end_str) do
      case store.get.(key) do
        nil -> ""
        value -> do_getrange(value, start_idx, end_idx)
      end
    else
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("GETRANGE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getrange' command"}

  # ---------------------------------------------------------------------------
  # SETRANGE
  # ---------------------------------------------------------------------------

  def handle("SETRANGE", [key, offset_str, value], store) do
    case Integer.parse(offset_str) do
      {offset, ""} when offset >= 0 ->
        {:ok, new_len} = store.setrange.(key, offset, value)
        new_len

      {_offset, ""} ->
        {:error, "ERR offset is out of range"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("SETRANGE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setrange' command"}

  # ---------------------------------------------------------------------------
  # MSETNX
  # ---------------------------------------------------------------------------

  def handle("MSETNX", [], _store),
    do: {:error, "ERR wrong number of arguments for 'msetnx' command"}

  def handle("MSETNX", args, store) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'msetnx' command"}
    else
      pairs = Enum.chunk_every(args, 2)
      keys = Enum.map(pairs, fn [k, _v] -> k end)

      # Check ALL keys first — if any exist, set none
      if Enum.any?(keys, fn k -> store.exists?.(k) end) do
        0
      else
        Enum.each(pairs, fn [k, v] -> store.put.(k, v, 0) end)
        1
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Private — GETEX option parsing and execution
  # ---------------------------------------------------------------------------

  defp do_getex(key, opts, store) do
    case parse_getex_opts(opts) do
      {:ok, expire_at_ms} ->
        store.getex.(key, expire_at_ms)

      {:error, _} = err ->
        err
    end
  end

  defp parse_getex_opts(["PERSIST"]), do: {:ok, 0}

  defp parse_getex_opts(["EX", secs_str]) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        {:ok, Ferricstore.HLC.now_ms() + secs * 1_000}

      {_secs, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PX", ms_str]) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms > 0 ->
        {:ok, Ferricstore.HLC.now_ms() + ms}

      {_ms, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["EXAT", ts_str]) do
    case Integer.parse(ts_str) do
      {ts, ""} when ts > 0 ->
        {:ok, ts * 1_000}

      {_ts, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PXAT", ts_str]) do
    case Integer.parse(ts_str) do
      {ts, ""} when ts > 0 ->
        {:ok, ts}

      {_ts, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(_) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # Private — GETRANGE substring extraction
  # ---------------------------------------------------------------------------

  defp do_getrange(value, start_idx, end_idx) do
    len = byte_size(value)

    # Normalise negative indices
    start_norm = if start_idx < 0, do: max(len + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: len + end_idx, else: end_idx

    # Clamp to bounds
    start_clamped = min(start_norm, len)
    end_clamped = min(end_norm, len - 1)

    if start_clamped > end_clamped do
      ""
    else
      count = end_clamped - start_clamped + 1
      binary_part(value, start_clamped, count)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — float argument parsing
  # ---------------------------------------------------------------------------

  defp parse_float_arg(str) do
    # Try integer first (Redis considers "10" valid for INCRBYFLOAT)
    case Integer.parse(str) do
      {val, ""} ->
        {:ok, val * 1.0}

      _ ->
        case Float.parse(str) do
          {val, ""} ->
            # Reject inf/nan
            cond do
              val == :infinity -> :error
              val == :neg_infinity -> :error
              val != val -> :error  # NaN check
              true -> {:ok, val}
            end

          _ ->
            :error
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private — SET option parsing and execution
  # ---------------------------------------------------------------------------

  defp do_set(key, value, opts, store) do
    with {:ok, expire_at_ms, nx?, xx?} <- parse_set_opts(opts) do
      cond do
        nx? and store.exists?.(key) -> nil
        xx? and not store.exists?.(key) -> nil
        true -> store.put.(key, value, expire_at_ms)
      end
    end
  end

  defp parse_set_opts(opts), do: parse_set_opts(opts, 0, false, false)

  defp parse_set_opts([], exp, nx?, xx?), do: {:ok, exp, nx?, xx?}

  defp parse_set_opts(["NX" | rest], exp, _nx?, xx?) do
    parse_set_opts(rest, exp, true, xx?)
  end

  defp parse_set_opts(["XX" | rest], exp, nx?, _xx?) do
    parse_set_opts(rest, exp, nx?, true)
  end

  defp parse_set_opts(["EX", secs_str | rest], _exp, nx?, xx?) do
    with {secs, ""} <- Integer.parse(secs_str),
         true <- secs > 0 do
      parse_set_opts(rest, Ferricstore.HLC.now_ms() + secs * 1000, nx?, xx?)
    else
      false -> {:error, "ERR invalid expire time in 'set' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_set_opts(["PX", ms_str | rest], _exp, nx?, xx?) do
    with {ms, ""} <- Integer.parse(ms_str),
         true <- ms > 0 do
      parse_set_opts(rest, Ferricstore.HLC.now_ms() + ms, nx?, xx?)
    else
      false -> {:error, "ERR invalid expire time in 'set' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_set_opts([unknown | _rest], _exp, _nx?, _xx?) do
    {:error, "ERR syntax error, option '#{unknown}' not recognized"}
  end

  # ---------------------------------------------------------------------------
  # Private — type checking for GET
  # ---------------------------------------------------------------------------

  # Detects if a stored binary is actually a serialized non-string type
  # (list, hash, set, zset). If so, returns WRONGTYPE error instead of the
  # raw binary. This matches Redis behaviour where GET on a non-string key
  # returns a WRONGTYPE error.
  defp maybe_check_type(<<131, _rest::binary>> = value) do
    try do
      case :erlang.binary_to_term(value) do
        {:list, _} -> @wrongtype_error
        {:hash, _} -> @wrongtype_error
        {:set, _} -> @wrongtype_error
        {:zset, _} -> @wrongtype_error
        _ -> value
      end
    rescue
      ArgumentError -> value
    end
  end

  defp maybe_check_type(value), do: value
end
