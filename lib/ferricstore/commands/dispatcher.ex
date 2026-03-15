defmodule Ferricstore.Commands.Dispatcher do
  @moduledoc """
  Routes Redis command names to the appropriate handler module.

  The dispatcher normalises the command name to uppercase and delegates to one of:

    * `Ferricstore.Commands.Strings` — GET, SET, DEL, EXISTS, MGET, MSET
    * `Ferricstore.Commands.Expiry` — EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST
    * `Ferricstore.Commands.Server` — PING, ECHO, DBSIZE, KEYS, FLUSHDB

  Unknown commands return `{:error, "ERR unknown command ..."}`.
  """

  alias Ferricstore.Commands.{Expiry, Server, Strings}

  @string_cmds ~w(GET SET DEL EXISTS MGET MSET)
  @expiry_cmds ~w(EXPIRE EXPIREAT PEXPIRE PEXPIREAT TTL PTTL PERSIST)
  @server_cmds ~w(PING ECHO DBSIZE KEYS FLUSHDB FLUSHALL)

  @doc """
  Dispatches a Redis command to the appropriate handler module.

  The command name is normalised to uppercase before routing. Each handler
  receives `(cmd, args, store)` and returns plain Elixir terms.

  ## Parameters

    - `name` - Command name (any case)
    - `args` - List of string arguments
    - `store` - Injected store map

  ## Returns

  The return value from the matched handler, or `{:error, message}` for unknown commands.

  ## Examples

      iex> store = %{get: fn "k" -> "v" end}
      iex> Ferricstore.Commands.Dispatcher.dispatch("GET", ["k"], store)
      "v"
  """
  @spec dispatch(binary(), [binary()], map()) :: term()
  def dispatch(name, args, store) do
    cmd = String.upcase(name)

    cond do
      cmd in @string_cmds -> Strings.handle(cmd, args, store)
      cmd in @expiry_cmds -> Expiry.handle(cmd, args, store)
      cmd in @server_cmds -> Server.handle(cmd, args, store)
      true -> {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}
    end
  end
end
