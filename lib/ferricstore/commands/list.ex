defmodule Ferricstore.Commands.List do
  @moduledoc """
  Handles Redis list commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN,
  LINDEX, LSET, LREM, LTRIM, LPOS, LINSERT, LMOVE, LPUSHX, RPUSHX.

  Lists are stored as serialized Erlang terms via `Ferricstore.Store.ListOps`.
  Each list key holds a single value: `:erlang.term_to_binary({:list, elements})`.
  This module parses Redis command arguments and delegates to `ListOps` for
  the actual read-modify-write logic.

  ## Type Enforcement

  All list commands that read existing data detect type mismatches via
  `ListOps.decode_stored/1`. Using list commands on a key that holds a
  plain string or hash returns WRONGTYPE.
  """

  alias Ferricstore.Store.{ListOps, TypeRegistry}
  alias Ferricstore.Waiters

  @doc """
  Handles a list command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"LPUSH"`, `"RPUSH"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete` callbacks

  ## Returns

  Plain Elixir term: integer, string, list, nil, `:ok`, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # LPUSH key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("LPUSH", [key | elements], store) when elements != [] do
    with :ok <- TypeRegistry.check_or_set(key, :list, store) do
      {get_fn, put_fn, delete_fn} = store_fns(key, store)
      result = ListOps.execute(get_fn, put_fn, delete_fn, {:lpush, elements})
      if is_integer(result) and result > 0, do: Waiters.notify_push(key)
      result
    end
  end

  def handle("LPUSH", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lpush' command"}
  end

  # ---------------------------------------------------------------------------
  # RPUSH key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("RPUSH", [key | elements], store) when elements != [] do
    with :ok <- TypeRegistry.check_or_set(key, :list, store) do
      {get_fn, put_fn, delete_fn} = store_fns(key, store)
      result = ListOps.execute(get_fn, put_fn, delete_fn, {:rpush, elements})
      if is_integer(result) and result > 0, do: Waiters.notify_push(key)
      result
    end
  end

  def handle("RPUSH", _args, _store) do
    {:error, "ERR wrong number of arguments for 'rpush' command"}
  end

  # ---------------------------------------------------------------------------
  # LPOP key [count]
  # ---------------------------------------------------------------------------

  def handle("LPOP", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      {get_fn, put_fn, delete_fn} = store_fns(key, store)
      ListOps.execute(get_fn, put_fn, delete_fn, {:lpop, 1})
    end
  end

  def handle("LPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          {get_fn, put_fn, delete_fn} = store_fns(key, store)

          if count == 0 do
            # Redis returns empty list for LPOP key 0 when key exists,
            # nil when key doesn't exist.
            case ListOps.decode_stored(get_fn.()) do
              {:ok, _elements} -> []
              :not_found -> nil
              {:error, :wrongtype} ->
                {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
            end
          else
            ListOps.execute(get_fn, put_fn, delete_fn, {:lpop, count})
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("LPOP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lpop' command"}
  end

  # ---------------------------------------------------------------------------
  # RPOP key [count]
  # ---------------------------------------------------------------------------

  def handle("RPOP", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      {get_fn, put_fn, delete_fn} = store_fns(key, store)
      ListOps.execute(get_fn, put_fn, delete_fn, {:rpop, 1})
    end
  end

  def handle("RPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          {get_fn, put_fn, delete_fn} = store_fns(key, store)

          if count == 0 do
            case ListOps.decode_stored(get_fn.()) do
              {:ok, _elements} -> []
              :not_found -> nil
              {:error, :wrongtype} ->
                {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
            end
          else
            ListOps.execute(get_fn, put_fn, delete_fn, {:rpop, count})
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("RPOP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'rpop' command"}
  end

  # ---------------------------------------------------------------------------
  # LRANGE key start stop
  # ---------------------------------------------------------------------------

  def handle("LRANGE", [key, start_str, stop_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} ->
          {get_fn, put_fn, delete_fn} = store_fns(key, store)
          ListOps.execute(get_fn, put_fn, delete_fn, {:lrange, start, stop})

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("LRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lrange' command"}
  end

  # ---------------------------------------------------------------------------
  # LLEN key
  # ---------------------------------------------------------------------------

  def handle("LLEN", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      {get_fn, put_fn, delete_fn} = store_fns(key, store)
      ListOps.execute(get_fn, put_fn, delete_fn, :llen)
    end
  end

  def handle("LLEN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'llen' command"}
  end

  # ---------------------------------------------------------------------------
  # LINDEX key index
  # ---------------------------------------------------------------------------

  def handle("LINDEX", [key, index_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(index_str) do
        {index, ""} ->
          {get_fn, put_fn, delete_fn} = store_fns(key, store)
          ListOps.execute(get_fn, put_fn, delete_fn, {:lindex, index})

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("LINDEX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lindex' command"}
  end

  # ---------------------------------------------------------------------------
  # LSET key index element
  # ---------------------------------------------------------------------------

  def handle("LSET", [key, index_str, element], store) do
    case Integer.parse(index_str) do
      {index, ""} ->
        {get_fn, put_fn, delete_fn} = store_fns(key, store)
        ListOps.execute(get_fn, put_fn, delete_fn, {:lset, index, element})

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("LSET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lset' command"}
  end

  # ---------------------------------------------------------------------------
  # LREM key count element
  # ---------------------------------------------------------------------------

  def handle("LREM", [key, count_str, element], store) do
    case Integer.parse(count_str) do
      {count, ""} ->
        {get_fn, put_fn, delete_fn} = store_fns(key, store)
        ListOps.execute(get_fn, put_fn, delete_fn, {:lrem, count, element})

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("LREM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lrem' command"}
  end

  # ---------------------------------------------------------------------------
  # LTRIM key start stop
  # ---------------------------------------------------------------------------

  def handle("LTRIM", [key, start_str, stop_str], store) do
    case {Integer.parse(start_str), Integer.parse(stop_str)} do
      {{start, ""}, {stop, ""}} ->
        {get_fn, put_fn, delete_fn} = store_fns(key, store)
        ListOps.execute(get_fn, put_fn, delete_fn, {:ltrim, start, stop})

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("LTRIM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ltrim' command"}
  end

  # ---------------------------------------------------------------------------
  # LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
  # ---------------------------------------------------------------------------

  def handle("LPOS", [key, element | opts], store) do
    case parse_lpos_opts(opts) do
      {:ok, rank, count, maxlen} ->
        {get_fn, put_fn, delete_fn} = store_fns(key, store)
        ListOps.execute(get_fn, put_fn, delete_fn, {:lpos, element, rank, count, maxlen})

      {:error, _} = error ->
        error
    end
  end

  def handle("LPOS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lpos' command"}
  end

  # ---------------------------------------------------------------------------
  # LINSERT key BEFORE|AFTER pivot element
  # ---------------------------------------------------------------------------

  def handle("LINSERT", [key, direction_str, pivot, element], store) do
    case parse_direction(direction_str) do
      {:ok, direction} ->
        {get_fn, put_fn, delete_fn} = store_fns(key, store)
        ListOps.execute(get_fn, put_fn, delete_fn, {:linsert, direction, pivot, element})

      :error ->
        {:error, "ERR syntax error"}
    end
  end

  def handle("LINSERT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'linsert' command"}
  end

  # ---------------------------------------------------------------------------
  # LMOVE source destination LEFT|RIGHT LEFT|RIGHT
  # ---------------------------------------------------------------------------

  def handle("LMOVE", [source, destination, from_str, to_str], store) do
    with {:ok, from_dir} <- parse_lr_direction(from_str),
         {:ok, to_dir} <- parse_lr_direction(to_str) do
      {src_get, src_put, src_delete} = store_fns(source, store)
      {dst_get, dst_put, _dst_delete} = store_fns(destination, store)
      ListOps.execute_lmove(src_get, src_put, src_delete, dst_get, dst_put, from_dir, to_dir)
    else
      :error -> {:error, "ERR syntax error"}
    end
  end

  def handle("LMOVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lmove' command"}
  end

  # ---------------------------------------------------------------------------
  # LPUSHX key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("LPUSHX", [key | elements], store) when elements != [] do
    {get_fn, put_fn, delete_fn} = store_fns(key, store)
    ListOps.execute(get_fn, put_fn, delete_fn, {:lpushx, elements})
  end

  def handle("LPUSHX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lpushx' command"}
  end

  # ---------------------------------------------------------------------------
  # RPUSHX key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("RPUSHX", [key | elements], store) when elements != [] do
    {get_fn, put_fn, delete_fn} = store_fns(key, store)
    ListOps.execute(get_fn, put_fn, delete_fn, {:rpushx, elements})
  end

  def handle("RPUSHX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'rpushx' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Builds the {get_fn, put_fn, delete_fn} triple for ListOps from the store map.
  defp store_fns(key, store) do
    get_fn = fn -> store.get.(key) end
    put_fn = fn value -> store.put.(key, value, 0) end
    delete_fn = fn -> store.delete.(key) end
    {get_fn, put_fn, delete_fn}
  end


  # Parses LPOS optional arguments: RANK, COUNT, MAXLEN.
  defp parse_lpos_opts(opts), do: parse_lpos_opts(opts, 1, nil, 0)

  defp parse_lpos_opts([], rank, count, maxlen), do: {:ok, rank, count, maxlen}

  defp parse_lpos_opts(["RANK", val | rest], _rank, count, maxlen) do
    case Integer.parse(val) do
      {0, ""} -> {:error, "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use NEGATIVE to start from the end of the list"}
      {r, ""} -> parse_lpos_opts(rest, r, count, maxlen)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_lpos_opts(["COUNT", val | rest], rank, _count, maxlen) do
    case Integer.parse(val) do
      {c, ""} when c >= 0 -> parse_lpos_opts(rest, rank, c, maxlen)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_lpos_opts(["MAXLEN", val | rest], rank, count, _maxlen) do
    case Integer.parse(val) do
      {m, ""} when m >= 0 -> parse_lpos_opts(rest, rank, count, m)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_lpos_opts([unknown | _rest], _rank, _count, _maxlen) do
    {:error, "ERR syntax error, option '#{unknown}' not recognized"}
  end

  # Parses BEFORE/AFTER direction (case-insensitive).
  defp parse_direction(str) do
    case String.upcase(str) do
      "BEFORE" -> {:ok, :before}
      "AFTER" -> {:ok, :after}
      _ -> :error
    end
  end

  # Parses LEFT/RIGHT direction for LMOVE (case-insensitive).
  defp parse_lr_direction(str) do
    case String.upcase(str) do
      "LEFT" -> {:ok, :left}
      "RIGHT" -> {:ok, :right}
      _ -> :error
    end
  end
end
