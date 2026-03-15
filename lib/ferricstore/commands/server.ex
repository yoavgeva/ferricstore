defmodule Ferricstore.Commands.Server do
  @moduledoc """
  Handles Redis server commands: PING, ECHO, DBSIZE, KEYS, FLUSHDB.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  ## Supported commands

    * `PING [message]` — returns `{:simple, "PONG"}` or echoes the message
    * `ECHO message` — returns the message as a bulk string
    * `DBSIZE` — returns the number of keys in the store
    * `KEYS pattern` — returns keys matching a glob pattern (`*`, `?`)
    * `FLUSHDB [ASYNC|SYNC]` — deletes all keys
  """

  @doc """
  Handles a server command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"PING"`, `"KEYS"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `keys`, `dbsize`, `flush` callbacks

  ## Returns

  Plain Elixir term: `{:simple, "PONG"}`, string, integer, list, `:ok`, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("PING", [], _store), do: {:simple, "PONG"}
  def handle("PING", [msg], _store), do: msg

  def handle("PING", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ping' command"}
  end

  def handle("ECHO", [msg], _store), do: msg

  def handle("ECHO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'echo' command"}
  end

  def handle("DBSIZE", [], store), do: store.dbsize.()

  def handle("DBSIZE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'dbsize' command"}
  end

  def handle("KEYS", [pattern], store) do
    regex = glob_to_regex(pattern)
    store.keys.() |> Enum.filter(&Regex.match?(regex, &1))
  end

  def handle("KEYS", [], _store) do
    {:error, "ERR wrong number of arguments for 'keys' command"}
  end

  def handle("KEYS", _args, _store) do
    {:error, "ERR syntax error"}
  end

  def handle("FLUSHDB", args, store) when args in [[], ["ASYNC"], ["SYNC"]] do
    store.flush.()
    :ok
  end

  def handle("FLUSHDB", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # Private — glob-to-regex conversion
  # ---------------------------------------------------------------------------

  defp glob_to_regex(pattern) do
    regex_str =
      pattern
      |> String.graphemes()
      |> Enum.map_join(&escape_glob_char/1)

    Regex.compile!("^#{regex_str}$")
  end

  defp escape_glob_char("*"), do: ".*"
  defp escape_glob_char("?"), do: "."
  defp escape_glob_char(char) do
    Regex.escape(char)
  end
end
