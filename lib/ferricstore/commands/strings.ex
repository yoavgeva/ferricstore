defmodule Ferricstore.Commands.Strings do
  @moduledoc """
  Handles Redis string commands: GET, SET, DEL, EXISTS, MGET, MSET.

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
  """

  @doc """
  Handles a string command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"GET"`, `"SET"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, string, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("GET", [key], store), do: store.get.(key)

  def handle("GET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'get' command"}
  end

  def handle("SET", [key, value | opts], store), do: do_set(key, value, opts, store)

  def handle("SET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'set' command"}
  end

  def handle("DEL", [], _store) do
    {:error, "ERR wrong number of arguments for 'del' command"}
  end

  def handle("DEL", keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      if store.exists?.(key) do
        store.delete.(key)
        acc + 1
      else
        acc
      end
    end)
  end

  def handle("EXISTS", [], _store) do
    {:error, "ERR wrong number of arguments for 'exists' command"}
  end

  def handle("EXISTS", keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      if store.exists?.(key), do: acc + 1, else: acc
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
      args
      |> Enum.chunk_every(2)
      |> Enum.each(fn [k, v] -> store.put.(k, v, 0) end)

      :ok
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
      parse_set_opts(rest, System.os_time(:millisecond) + secs * 1000, nx?, xx?)
    else
      false -> {:error, "ERR invalid expire time in 'set' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_set_opts(["PX", ms_str | rest], _exp, nx?, xx?) do
    with {ms, ""} <- Integer.parse(ms_str),
         true <- ms > 0 do
      parse_set_opts(rest, System.os_time(:millisecond) + ms, nx?, xx?)
    else
      false -> {:error, "ERR invalid expire time in 'set' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_set_opts([unknown | _rest], _exp, _nx?, _xx?) do
    {:error, "ERR syntax error, option '#{unknown}' not recognized"}
  end
end
