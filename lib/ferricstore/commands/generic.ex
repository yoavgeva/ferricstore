defmodule Ferricstore.Commands.Generic do
  @moduledoc """
  Handles Redis generic key commands: TYPE, UNLINK, RENAME, RENAMENX, COPY,
  RANDOMKEY, SCAN, EXPIRETIME, PEXPIRETIME, OBJECT, WAIT.

  These commands operate on keys regardless of value type. Each handler takes
  the uppercased command name, a list of string arguments, and an injected
  store map. Returns plain Elixir terms -- the connection layer handles RESP
  encoding.

  ## Supported commands

    * `TYPE key` -- returns the type of key ("string" for existing, "none" for missing)
    * `UNLINK key [key ...]` -- async DEL; returns count of deleted keys
    * `RENAME key newkey` -- rename key, error if source missing
    * `RENAMENX key newkey` -- rename only if newkey doesn't exist (1 = renamed, 0 = not)
    * `COPY source destination [REPLACE]` -- copy value+TTL (1 = success, 0 = failure)
    * `RANDOMKEY` -- return a random key, or nil if DB is empty
    * `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` -- cursor-based key iteration
    * `EXPIRETIME key` -- absolute Unix timestamp (seconds) when key expires (-1 / -2)
    * `PEXPIRETIME key` -- absolute Unix timestamp (milliseconds) when key expires (-1 / -2)
    * `OBJECT ENCODING key` -- returns "raw" (FerricStore internal encoding)
    * `OBJECT HELP` -- returns list of OBJECT subcommands
    * `OBJECT FREQ key` -- returns 0 (stub, no LFU tracking)
    * `OBJECT IDLETIME key` -- returns 0 (stub)
    * `OBJECT REFCOUNT key` -- always returns 1
    * `WAIT numreplicas timeout` -- returns 0 immediately (no replication)
  """

  @doc """
  Handles a generic key command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"TYPE"`, `"RENAME"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `get_meta`, `put`, `delete`,
      `exists?`, `keys` callbacks

  ## Returns

  Plain Elixir term: string, integer, list, nil, `{:simple, string}`, or
  `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TYPE
  # ---------------------------------------------------------------------------

  def handle("TYPE", [key], store) do
    {:simple, Ferricstore.Store.TypeRegistry.get_type(key, store)}
  end

  def handle("TYPE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'type' command"}
  end

  # ---------------------------------------------------------------------------
  # UNLINK (same semantics as DEL -- async reclaim deferred to merge)
  # ---------------------------------------------------------------------------

  def handle("UNLINK", [], _store) do
    {:error, "ERR wrong number of arguments for 'unlink' command"}
  end

  def handle("UNLINK", keys, store) do
    # UNLINK has the same semantics as DEL for data-structure cleanup;
    # async reclaim is deferred to merge.
    Ferricstore.Commands.Strings.handle("DEL", keys, store)
  end

  # ---------------------------------------------------------------------------
  # RENAME
  # ---------------------------------------------------------------------------

  def handle("RENAME", [key, newkey], store) do
    case store.get_meta.(key) do
      nil ->
        {:error, "ERR no such key"}

      {value, expire_at_ms} ->
        store.put.(newkey, value, expire_at_ms)

        if key != newkey do
          store.delete.(key)
        end

        :ok
    end
  end

  def handle("RENAME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'rename' command"}
  end

  # ---------------------------------------------------------------------------
  # RENAMENX
  # ---------------------------------------------------------------------------

  def handle("RENAMENX", [key, newkey], store) do
    case store.get_meta.(key) do
      nil ->
        {:error, "ERR no such key"}

      {_value, _expire_at_ms} when key == newkey ->
        # Same key -- always 0 since destination "exists"
        0

      {value, expire_at_ms} ->
        if store.exists?.(newkey) do
          0
        else
          store.put.(newkey, value, expire_at_ms)
          store.delete.(key)
          1
        end
    end
  end

  def handle("RENAMENX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'renamenx' command"}
  end

  # ---------------------------------------------------------------------------
  # COPY
  # ---------------------------------------------------------------------------

  def handle("COPY", args, store) when length(args) >= 2 do
    [source, destination | opts] = args

    case parse_copy_opts(opts) do
      {:ok, replace?} ->
        do_copy(source, destination, replace?, store)

      {:error, _} = err ->
        err
    end
  end

  def handle("COPY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'copy' command"}
  end

  # ---------------------------------------------------------------------------
  # RANDOMKEY
  # ---------------------------------------------------------------------------

  def handle("RANDOMKEY", [], store) do
    case store.keys.() do
      [] -> nil
      keys -> Enum.random(keys)
    end
  end

  def handle("RANDOMKEY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'randomkey' command"}
  end

  # ---------------------------------------------------------------------------
  # SCAN
  # ---------------------------------------------------------------------------

  def handle("SCAN", [cursor_str | opts], store) do
    with {:ok, match_pattern, count, type_filter} <- parse_scan_opts(opts) do
      do_scan(cursor_str, match_pattern, count, type_filter, store)
    end
  end

  def handle("SCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'scan' command"}
  end

  # ---------------------------------------------------------------------------
  # EXPIRETIME
  # ---------------------------------------------------------------------------

  def handle("EXPIRETIME", [key], store) do
    case store.get_meta.(key) do
      nil -> -2
      {_value, 0} -> -1
      {_value, expire_at_ms} -> div(expire_at_ms, 1_000)
    end
  end

  def handle("EXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'expiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # PEXPIRETIME
  # ---------------------------------------------------------------------------

  def handle("PEXPIRETIME", [key], store) do
    case store.get_meta.(key) do
      nil -> -2
      {_value, 0} -> -1
      {_value, expire_at_ms} -> expire_at_ms
    end
  end

  def handle("PEXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pexpiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # OBJECT -- subcommand is case-insensitive (uppercased before dispatch)
  # ---------------------------------------------------------------------------

  def handle("OBJECT", [], _store) do
    {:error, "ERR wrong number of arguments for 'object' command"}
  end

  def handle("OBJECT", [subcmd | rest], store) do
    do_object(String.upcase(subcmd), rest, store)
  end

  # ---------------------------------------------------------------------------
  # WAIT
  # ---------------------------------------------------------------------------

  def handle("WAIT", [_numreplicas, _timeout], _store) do
    # No replication support yet -- return 0 immediately.
    0
  end

  def handle("WAIT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'wait' command"}
  end

  # ---------------------------------------------------------------------------
  # Private -- OBJECT subcommands
  # ---------------------------------------------------------------------------

  defp do_object("ENCODING", [key], store) do
    if store.exists?.(key) do
      "raw"
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object("HELP", [], _store) do
    [
      "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "ENCODING <key>",
      "  Return the kind of internal representation the Redis object stored at <key> is using.",
      "FREQ <key>",
      "  Return the logarithmic access frequency counter of a Redis object stored at <key>.",
      "HELP",
      "  Return subcommand help summary.",
      "IDLETIME <key>",
      "  Return the idle time of a Redis object stored at <key>.",
      "REFCOUNT <key>",
      "  Return the reference count of the object stored at <key>."
    ]
  end

  defp do_object("FREQ", [key], store) do
    if store.exists?.(key) do
      0
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object("IDLETIME", [key], store) do
    if store.exists?.(key) do
      0
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object("REFCOUNT", [key], store) do
    if store.exists?.(key) do
      1
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object(subcmd, _rest, _store) do
    {:error, "ERR unknown subcommand or wrong number of arguments for '#{String.downcase(subcmd)}' command"}
  end

  # ---------------------------------------------------------------------------
  # Private -- COPY helpers
  # ---------------------------------------------------------------------------

  defp parse_copy_opts([]), do: {:ok, false}

  defp parse_copy_opts([opt]) do
    if String.upcase(opt) == "REPLACE" do
      {:ok, true}
    else
      {:error, "ERR syntax error"}
    end
  end

  defp parse_copy_opts(_) do
    {:error, "ERR syntax error"}
  end

  defp do_copy(source, destination, replace?, store) do
    case store.get_meta.(source) do
      nil ->
        {:error, "ERR no such key"}

      {value, expire_at_ms} ->
        if not replace? and store.exists?.(destination) do
          {:error, "ERR target key already exists"}
        else
          store.put.(destination, value, expire_at_ms)
          1
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- SCAN option parsing and execution
  # ---------------------------------------------------------------------------

  defp parse_scan_opts(opts), do: parse_scan_opts(opts, nil, 10, nil)

  defp parse_scan_opts([], match, count, type), do: {:ok, match, count, type}

  defp parse_scan_opts([opt, value | rest], match, count, type) do
    case String.upcase(opt) do
      "MATCH" ->
        parse_scan_opts(rest, value, count, type)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 ->
            parse_scan_opts(rest, match, n, type)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end

      "TYPE" ->
        parse_scan_opts(rest, match, count, String.downcase(value))

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_scan_opts([_ | _], _match, _count, _type) do
    {:error, "ERR syntax error"}
  end

  defp do_scan(cursor_str, match_pattern, count, type_filter, store) do
    all_keys =
      store.keys.()
      |> filter_by_type(type_filter)
      |> filter_by_match(match_pattern)
      |> Enum.sort()

    # Cursor "0" means start from the beginning. Otherwise, cursor is the last
    # key seen -- find the first key strictly after it alphabetically.
    {start_index, _} =
      if cursor_str == "0" do
        {0, nil}
      else
        idx = Enum.find_index(all_keys, fn k -> k > cursor_str end)
        {idx || length(all_keys), nil}
      end

    batch = Enum.slice(all_keys, start_index, count)

    next_cursor =
      case batch do
        [] ->
          "0"

        _ ->
          last_key = List.last(batch)
          # If the last key in the batch is also the last key overall, iteration
          # is complete -- return cursor "0".
          if last_key == List.last(all_keys) do
            "0"
          else
            last_key
          end
      end

    [next_cursor, batch]
  end

  defp filter_by_type(keys, nil), do: keys

  defp filter_by_type(keys, "string"), do: keys

  defp filter_by_type(_keys, _other_type), do: []

  defp filter_by_match(keys, nil), do: keys

  defp filter_by_match(keys, pattern) do
    regex = glob_to_regex(pattern)
    Enum.filter(keys, &Regex.match?(regex, &1))
  end

  # Reuses the same glob-to-regex approach as Server.handle("KEYS", ...).
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
end
