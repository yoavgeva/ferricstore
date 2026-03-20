defmodule Ferricstore.Commands.Catalog do
  @moduledoc """
  Central registry of all commands supported by FerricStore.

  Each entry contains the command name, arity (negative = variadic), flags,
  first-key / last-key / step indices, and a short summary string.

  This module is the single source of truth consumed by:

    * `COMMAND COUNT`
    * `COMMAND LIST`
    * `COMMAND INFO`
    * `COMMAND DOCS`
    * `COMMAND GETKEYS`

  ## Arity convention (Redis)

    * Positive N — command takes exactly N arguments (including the command name).
    * Negative N — command takes at least |N| arguments.

  ## Key position convention

    * `first_key: 0` means the command has no key arguments.
    * For single-key commands: `first_key: 1, last_key: 1, step: 1`.
    * For multi-key variadic commands: `last_key: -1, step: 1`.
  """

  @type command_entry :: %{
          name: binary(),
          arity: integer(),
          flags: [binary()],
          first_key: integer(),
          last_key: integer(),
          step: integer(),
          summary: binary()
        }

  @commands [
    # -- Strings -----------------------------------------------------------
    %{name: "get", arity: 2, flags: ["readonly", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Returns the string value of a key."},
    %{name: "set", arity: -3, flags: ["write", "denyoom"], first_key: 1, last_key: 1, step: 1,
      summary: "Sets the string value of a key."},
    %{name: "del", arity: -2, flags: ["write"], first_key: 1, last_key: -1, step: 1,
      summary: "Deletes one or more keys."},
    %{name: "exists", arity: -2, flags: ["readonly", "fast"], first_key: 1, last_key: -1, step: 1,
      summary: "Determines whether one or more keys exist."},
    %{name: "mget", arity: -2, flags: ["readonly", "fast"], first_key: 1, last_key: -1, step: 1,
      summary: "Returns the values of one or more keys."},
    %{name: "mset", arity: -3, flags: ["write", "denyoom"], first_key: 1, last_key: -1, step: 2,
      summary: "Atomically sets one or more key-value pairs."},

    # -- Expiry ------------------------------------------------------------
    %{name: "expire", arity: 3, flags: ["write", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Sets the expiration time of a key in seconds."},
    %{name: "pexpire", arity: 3, flags: ["write", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Sets the expiration time of a key in milliseconds."},
    %{name: "expireat", arity: 3, flags: ["write", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Sets the expiration time of a key to a Unix timestamp."},
    %{name: "pexpireat", arity: 3, flags: ["write", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Sets the expiration time of a key to a Unix timestamp in ms."},
    %{name: "ttl", arity: 2, flags: ["readonly", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Returns the remaining time-to-live of a key in seconds."},
    %{name: "pttl", arity: 2, flags: ["readonly", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Returns the remaining time-to-live of a key in milliseconds."},
    %{name: "persist", arity: 2, flags: ["write", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Removes the expiration from a key."},

    # -- Server ------------------------------------------------------------
    %{name: "ping", arity: -1, flags: ["fast", "stale"], first_key: 0, last_key: 0, step: 0,
      summary: "Returns PONG or echoes the given message."},
    %{name: "echo", arity: 2, flags: ["fast"], first_key: 0, last_key: 0, step: 0,
      summary: "Returns the given string."},
    %{name: "dbsize", arity: 1, flags: ["readonly", "fast"], first_key: 0, last_key: 0, step: 0,
      summary: "Returns the number of keys in the database."},
    %{name: "keys", arity: 2, flags: ["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0,
      summary: "Returns all key names that match a pattern."},
    %{name: "flushdb", arity: -1, flags: ["write"], first_key: 0, last_key: 0, step: 0,
      summary: "Removes all keys from the current database."},
    %{name: "flushall", arity: -1, flags: ["write"], first_key: 0, last_key: 0, step: 0,
      summary: "Removes all keys from all databases."},
    %{name: "info", arity: -1, flags: ["stale", "fast"], first_key: 0, last_key: 0, step: 0,
      summary: "Returns information and statistics about the server."},
    %{name: "select", arity: 2, flags: ["fast"], first_key: 0, last_key: 0, step: 0,
      summary: "Not supported. Use named caches."},
    %{name: "lolwut", arity: -1, flags: ["fast"], first_key: 0, last_key: 0, step: 0,
      summary: "Displays the FerricStore ASCII logo."},
    %{name: "debug", arity: -2, flags: ["admin"], first_key: 0, last_key: 0, step: 0,
      summary: "Debugging command."},

    # -- COMMAND subcommands -----------------------------------------------
    %{name: "command", arity: -1, flags: ["random", "stale"], first_key: 0, last_key: 0, step: 0,
      summary: "Returns information about all or specific commands."},

    # -- CLIENT subcommands ------------------------------------------------
    %{name: "client", arity: -2, flags: ["admin", "stale"], first_key: 0, last_key: 0, step: 0,
      summary: "Manages client connections."},

    # -- Connection --------------------------------------------------------
    %{name: "hello", arity: -1, flags: ["fast", "stale"], first_key: 0, last_key: 0, step: 0,
      summary: "Handshakes with the server."},
    %{name: "quit", arity: 1, flags: ["fast"], first_key: 0, last_key: 0, step: 0,
      summary: "Closes the connection."},
    %{name: "reset", arity: 1, flags: ["fast", "stale"], first_key: 0, last_key: 0, step: 0,
      summary: "Resets the connection."},

    # -- FerricStore-native -------------------------------------------------
    %{name: "ferricstore.config", arity: -2, flags: ["admin"], first_key: 0, last_key: 0, step: 0,
      summary: "Manages per-namespace configuration (SET/GET/RESET)."},
    %{name: "ferricstore.key_info", arity: 2, flags: ["readonly", "fast"], first_key: 1, last_key: 1, step: 1,
      summary: "Returns diagnostic metadata about a key (type, size, TTL, cache status, shard)."}
  ]

  @commands_by_name Map.new(@commands, fn cmd -> {cmd.name, cmd} end)

  @doc "Returns the full list of command entries."
  @spec all() :: [command_entry()]
  def all, do: @commands

  @doc "Returns the number of supported commands."
  @spec count() :: non_neg_integer()
  def count, do: length(@commands)

  @doc "Returns all command names as lowercase strings."
  @spec names() :: [binary()]
  def names, do: Enum.map(@commands, & &1.name)

  @doc """
  Looks up a command entry by name (case-insensitive).

  Returns `{:ok, entry}` or `:error`.
  """
  @spec lookup(binary()) :: {:ok, command_entry()} | :error
  def lookup(name) when is_binary(name) do
    case Map.fetch(@commands_by_name, String.downcase(name)) do
      {:ok, _} = ok -> ok
      :error -> :error
    end
  end

  @doc """
  Returns the Redis-style info tuple for a command entry.

  Format: `[name, arity, [flags...], first_key, last_key, step]`
  """
  @spec info_tuple(command_entry()) :: list()
  def info_tuple(%{} = cmd) do
    [cmd.name, cmd.arity, cmd.flags, cmd.first_key, cmd.last_key, cmd.step]
  end

  @doc """
  Extracts the key arguments from a command invocation.

  Given a command name and its arguments list, returns the arguments that are keys
  based on the catalog's first_key/last_key/step metadata.

  Returns `{:ok, keys}` or `{:error, reason}`.
  """
  @spec get_keys(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys(name, args) when is_binary(name) and is_list(args) do
    case lookup(name) do
      {:ok, %{first_key: 0}} ->
        {:ok, []}

      {:ok, %{first_key: first, last_key: last, step: step}} ->
        # Arguments are 0-indexed in our list, but first_key is 1-indexed
        # (position 1 = first arg after the command name).
        first_idx = first - 1
        last_idx = if last == -1, do: length(args) - 1, else: last - 1
        step_val = max(step, 1)

        keys =
          first_idx..last_idx//step_val
          |> Enum.map(fn i -> Enum.at(args, i) end)
          |> Enum.reject(&is_nil/1)

        {:ok, keys}

      :error ->
        {:error, "ERR Invalid command specified"}
    end
  end
end
