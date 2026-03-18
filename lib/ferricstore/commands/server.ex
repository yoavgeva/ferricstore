# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Server do
  @moduledoc """
  Handles Redis server commands: PING, ECHO, DBSIZE, KEYS, FLUSHDB, FLUSHALL,
  INFO, COMMAND, SELECT, LOLWUT, and DEBUG.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  ## Supported commands

    * `PING [message]` — returns `{:simple, "PONG"}` or echoes the message
    * `ECHO message` — returns the message as a bulk string
    * `DBSIZE` — returns the number of keys in the store
    * `KEYS pattern` — returns keys matching a glob pattern (`*`, `?`)
    * `FLUSHDB [ASYNC|SYNC]` — deletes all keys
    * `FLUSHALL [ASYNC|SYNC]` — alias for FLUSHDB (single-db server)
    * `INFO [section]` — returns server information as a bulk string
    * `COMMAND` — returns array of command info tuples
    * `COMMAND COUNT` — returns number of supported commands
    * `COMMAND DOCS name` — returns simplified docs for a command
    * `COMMAND INFO name [name ...]` — returns info for specific commands
    * `COMMAND LIST` — returns all command names
    * `COMMAND GETKEYS command [args...]` — returns which args are keys
    * `SELECT db` — always returns error (not supported)
    * `LOLWUT [VERSION version]` — returns ASCII art with FerricStore branding
    * `DEBUG SLEEP seconds` — sleeps for N seconds (testing only)
  """

  alias Ferricstore.AuditLog
  alias Ferricstore.Commands.Catalog
  alias Ferricstore.Stats

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

  # ---------------------------------------------------------------------------
  # PING
  # ---------------------------------------------------------------------------

  def handle("PING", [], _store), do: {:simple, "PONG"}
  def handle("PING", [msg], _store), do: msg

  def handle("PING", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ping' command"}
  end

  # ---------------------------------------------------------------------------
  # ECHO
  # ---------------------------------------------------------------------------

  def handle("ECHO", [msg], _store), do: msg

  def handle("ECHO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'echo' command"}
  end

  # ---------------------------------------------------------------------------
  # DBSIZE
  # ---------------------------------------------------------------------------

  def handle("DBSIZE", [], store) do
    alias Ferricstore.Store.CompoundKey

    store.keys.()
    |> Enum.reject(&CompoundKey.internal_key?/1)
    |> length()
  end

  def handle("DBSIZE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'dbsize' command"}
  end

  # ---------------------------------------------------------------------------
  # KEYS
  # ---------------------------------------------------------------------------

  def handle("KEYS", [pattern], store) do
    alias Ferricstore.Store.CompoundKey

    regex = glob_to_regex(pattern)

    store.keys.()
    |> Enum.reject(&CompoundKey.internal_key?/1)
    |> Enum.filter(&Regex.match?(regex, &1))
  end

  def handle("KEYS", [], _store) do
    {:error, "ERR wrong number of arguments for 'keys' command"}
  end

  def handle("KEYS", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # FLUSHDB
  # ---------------------------------------------------------------------------

  def handle("FLUSHDB", args, store) when args in [[], ["ASYNC"], ["SYNC"]] do
    AuditLog.log(:dangerous_command, %{command: "FLUSHDB", args: args})
    store.flush.()
    :ok
  end

  def handle("FLUSHDB", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # FLUSHALL — alias for FLUSHDB in our single-database server
  # ---------------------------------------------------------------------------

  def handle("FLUSHALL", args, store) when args in [[], ["ASYNC"], ["SYNC"]] do
    AuditLog.log(:dangerous_command, %{command: "FLUSHALL", args: args})
    store.flush.()
    :ok
  end

  def handle("FLUSHALL", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # SELECT — not supported
  # ---------------------------------------------------------------------------

  def handle("SELECT", [_db], _store) do
    {:error, "ERR SELECT not supported. Use named caches."}
  end

  def handle("SELECT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'select' command"}
  end

  # ---------------------------------------------------------------------------
  # INFO [section]
  # ---------------------------------------------------------------------------

  def handle("INFO", [], store), do: handle("INFO", ["all"], store)

  def handle("INFO", [section], store) do
    section_lower = String.downcase(section)
    info_string(section_lower, store)
  end

  def handle("INFO", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # COMMAND (no subcommand) — return all command info tuples
  # ---------------------------------------------------------------------------

  def handle("COMMAND", [], _store) do
    Catalog.all() |> Enum.map(&Catalog.info_tuple/1)
  end

  # ---------------------------------------------------------------------------
  # COMMAND subcommands
  # ---------------------------------------------------------------------------

  def handle("COMMAND", ["COUNT"], _store), do: Catalog.count()

  def handle("COMMAND", ["LIST"], _store), do: Catalog.names()

  def handle("COMMAND", ["INFO" | names], _store) when names != [] do
    Enum.map(names, fn name ->
      case Catalog.lookup(name) do
        {:ok, cmd} -> Catalog.info_tuple(cmd)
        :error -> nil
      end
    end)
  end

  def handle("COMMAND", ["INFO"], _store) do
    {:error, "ERR wrong number of arguments for 'command|info' command"}
  end

  def handle("COMMAND", ["DOCS" | names], _store) when names != [] do
    result =
      Enum.flat_map(names, fn name ->
        case Catalog.lookup(name) do
          {:ok, cmd} ->
            [cmd.name, [cmd.summary]]

          :error ->
            []
        end
      end)

    result
  end

  def handle("COMMAND", ["DOCS"], _store) do
    {:error, "ERR wrong number of arguments for 'command|docs' command"}
  end

  def handle("COMMAND", ["GETKEYS", cmd_name | cmd_args], _store) do
    case Catalog.get_keys(cmd_name, cmd_args) do
      {:ok, keys} -> keys
      {:error, msg} -> {:error, msg}
    end
  end

  def handle("COMMAND", ["GETKEYS"], _store) do
    {:error, "ERR wrong number of arguments for 'command|getkeys' command"}
  end

  def handle("COMMAND", [subcmd | _rest], _store) do
    {:error, "ERR unknown subcommand '#{subcmd}'. Try COMMAND HELP."}
  end

  # ---------------------------------------------------------------------------
  # LOLWUT [VERSION version]
  # ---------------------------------------------------------------------------

  def handle("LOLWUT", args, _store) when args in [[], ["VERSION", "1"]] do
    art = """
     _____              _      ____  _
    |  ___|__ _ __ _ __(_) ___/ ___|| |_ ___  _ __ ___
    | |_ / _ \\ '__| '__| |/ __\\___ \\| __/ _ \\| '__/ _ \\
    |  _|  __/ |  | |  | | (__ ___) | || (_) | | |  __/
    |_|  \\___|_|  |_|  |_|\\___|____/ \\__\\___/|_|  \\___|
                                          v0.1.0
    """

    String.trim_trailing(art)
  end

  def handle("LOLWUT", ["VERSION", _version], _store) do
    handle("LOLWUT", [], nil)
  end

  def handle("LOLWUT", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # DEBUG SLEEP seconds
  # ---------------------------------------------------------------------------

  def handle("DEBUG", ["SLEEP", seconds_str], _store) do
    AuditLog.log(:dangerous_command, %{command: "DEBUG", args: ["SLEEP", seconds_str]})

    case Integer.parse(seconds_str) do
      {secs, ""} when secs >= 0 ->
        Process.sleep(secs * 1000)
        :ok

      _ ->
        {:error, "ERR invalid argument for DEBUG SLEEP"}
    end
  end

  def handle("DEBUG", ["SLEEP"], _store) do
    {:error, "ERR wrong number of arguments for 'debug' command"}
  end

  def handle("DEBUG", ["RELOAD"], _store), do: :ok

  def handle("DEBUG", ["FLUSHALL" | _], store) do
    AuditLog.log(:dangerous_command, %{command: "DEBUG", args: ["FLUSHALL"]})
    handle("FLUSHALL", [], store)
  end
  def handle("DEBUG", ["SET-ACTIVE-EXPIRE", _flag], _store), do: :ok
  def handle("DEBUG", ["CHANGE-REPL-ID"], _store), do: :ok
  def handle("DEBUG", ["QUICKLIST-PACKED-THRESHOLD" | _], _store), do: :ok
  def handle("DEBUG", ["AOFSTAT"], _store), do: %{}
  def handle("DEBUG", ["SFLAGS"], _store), do: %{}

  def handle("DEBUG", [subcmd | _rest], _store) do
    {:error, "ERR unknown subcommand '#{subcmd}'. Try DEBUG HELP."}
  end

  def handle("DEBUG", [], _store) do
    {:error, "ERR wrong number of arguments for 'debug' command"}
  end

  # ---------------------------------------------------------------------------
  # Private — INFO section builders
  # ---------------------------------------------------------------------------

  @all_sections ["server", "clients", "memory", "keyspace", "stats", "persistence", "replication", "cpu"]

  defp info_string(section, store) when section in ["all", "everything"] do
    @all_sections
    |> Enum.map(fn s -> build_section(s, store) end)
    |> Enum.join("\r\n")
  end

  defp info_string(section, store) when section in @all_sections do
    build_section(section, store)
  end

  defp info_string(_unknown, _store) do
    # Redis returns an empty string for unknown sections
    ""
  end

  defp build_section("server", _store) do
    port = Application.get_env(:ferricstore, :port, 6379)
    uptime_seconds = Stats.uptime_seconds()
    uptime_days = div(uptime_seconds, 86_400)

    {os_family, os_name} = :os.type()
    {major, minor, patch} = :os.version()
    os_string = "#{os_family}:#{os_name} #{major}.#{minor}.#{patch}"

    fields = [
      {"redis_version", "7.4.0"},
      {"ferricstore_version", "0.1.0"},
      {"redis_mode", "standalone"},
      {"os", os_string},
      {"arch_bits", "64"},
      {"tcp_port", Integer.to_string(port)},
      {"uptime_in_seconds", Integer.to_string(uptime_seconds)},
      {"uptime_in_days", Integer.to_string(uptime_days)},
      {"hz", "10"},
      {"configured_hz", "10"},
      {"process_id", Integer.to_string(System.pid() |> String.to_integer())},
      {"run_id", Stats.run_id()},
      {"ferricstore_git_sha", "dev"}
    ]

    format_section("Server", fields)
  end

  defp build_section("clients", _store) do
    connected =
      try do
        :ranch.procs(Ferricstore.Server.Listener, :connections) |> length()
      rescue
        _ -> 0
      end

    blocked = safe_ets_size(:ferricstore_waiters)

    tracking =
      safe_ets_size(:ferricstore_tracking_connections)

    fields = [
      {"connected_clients", Integer.to_string(connected)},
      {"blocked_clients", Integer.to_string(blocked)},
      {"tracking_clients", Integer.to_string(tracking)},
      {"maxclients", "10000"}
    ]

    format_section("Clients", fields)
  end

  defp build_section("memory", _store) do
    total = :erlang.memory(:total)
    process_mem = :erlang.memory(:processes)
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    # Sum ETS memory across all shard tables (one per shard).
    # Each shard's ETS table is named :shard_ets_N.
    keydir_bytes =
      Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
        table = :"shard_ets_#{i}"

        try do
          # :ets.info returns memory in words; multiply by word size to get bytes.
          words = :ets.info(table, :memory)
          acc + words * :erlang.system_info(:wordsize)
        rescue
          ArgumentError -> acc
        end
      end)

    # RSS approximation: best we can do on BEAM is :erlang.memory(:total)
    used_memory_rss = total

    # Peak: we do not track a high-water mark yet, so report current.
    used_memory_peak = total

    # Fragmentation ratio (rss / used). With BEAM they are the same, so ~1.0.
    frag_ratio =
      if total > 0,
        do: Float.round(used_memory_rss / total, 2),
        else: 1.0

    fields = [
      {"used_memory", Integer.to_string(total)},
      {"used_memory_human", format_bytes(total)},
      {"used_memory_rss", Integer.to_string(used_memory_rss)},
      {"used_memory_peak", Integer.to_string(used_memory_peak)},
      {"mem_fragmentation_ratio", format_float_field(frag_ratio)},
      {"keydir_used_bytes", Integer.to_string(keydir_bytes)},
      {"hot_cache_used_bytes", Integer.to_string(keydir_bytes)},
      {"beam_process_memory", Integer.to_string(process_mem)}
    ]

    format_section("Memory", fields)
  end

  defp build_section("keyspace", store) do
    key_count = store.dbsize.()
    # We do not track per-key expiry stats in aggregate, so expires=0 and avg_ttl=0.
    line = "db0:keys=#{key_count},expires=0,avg_ttl=0"

    fields = [
      {"db0", line}
    ]

    format_section("Keyspace", fields)
  end

  defp build_section("stats", _store) do
    hot = Stats.total_hot_reads()
    cold = Stats.total_cold_reads()
    hot_pct = Stats.hot_read_pct()
    cold_per_sec = Stats.cold_reads_per_second()

    fields = [
      {"total_connections_received", Integer.to_string(Stats.total_connections())},
      {"total_commands_processed", Integer.to_string(Stats.total_commands())},
      {"hot_reads", Integer.to_string(hot)},
      {"cold_reads", Integer.to_string(cold)},
      {"hot_read_pct", format_float_field(hot_pct)},
      {"cold_reads_per_second", format_float_field(cold_per_sec)}
    ]

    format_section("Stats", fields)
  end

  defp build_section("persistence", _store) do
    fields = [
      {"loading", "0"},
      {"rdb_changes_since_last_save", "0"},
      {"rdb_last_save_time", "0"}
    ]

    format_section("Persistence", fields)
  end

  defp build_section("replication", _store) do
    fields = [
      {"role", "master"},
      {"connected_slaves", "0"}
    ]

    format_section("Replication", fields)
  end

  defp build_section("cpu", _store) do
    # Stub: BEAM does not expose per-process CPU counters cheaply.
    fields = [
      {"used_cpu_sys", "0.000000"},
      {"used_cpu_user", "0.000000"}
    ]

    format_section("CPU", fields)
  end

  defp format_section(header, fields) do
    lines =
      fields
      |> Enum.map(fn {k, v} -> "#{k}:#{v}" end)
      |> Enum.join("\r\n")

    "# #{header}\r\n#{lines}\r\n"
  end

  defp safe_ets_size(table) do
    try do
      :ets.info(table, :size)
    rescue
      ArgumentError -> 0
    end
  end

  defp format_float_field(val) do
    :erlang.float_to_binary(val, [{:decimals, 2}])
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes}B"

  defp format_bytes(bytes) when bytes < 1024 * 1024 do
    kb = Float.round(bytes / 1024, 2)
    "#{kb}K"
  end

  defp format_bytes(bytes) when bytes < 1024 * 1024 * 1024 do
    mb = Float.round(bytes / (1024 * 1024), 2)
    "#{mb}M"
  end

  defp format_bytes(bytes) do
    gb = Float.round(bytes / (1024 * 1024 * 1024), 2)
    "#{gb}G"
  end

  # ---------------------------------------------------------------------------
  # Private — glob-to-regex conversion
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # CONFIG
  # ---------------------------------------------------------------------------

  def handle("CONFIG", [subcmd | rest], _store) do
    handle_config(String.upcase(subcmd), rest)
  end

  def handle("CONFIG", [], _store) do
    {:error, "ERR wrong number of arguments for 'config' command"}
  end

  defp handle_config("GET", [pattern]) do
    Ferricstore.Config.get(pattern)
    |> Enum.flat_map(fn {k, v} -> [k, v] end)
  end

  defp handle_config("GET", _args), do: {:error, "ERR wrong number of arguments for 'config|get' command"}

  defp handle_config("SET", [key, value]) do
    old_value = Ferricstore.Config.get_value(key)

    case Ferricstore.Config.set(key, value) do
      :ok ->
        AuditLog.log(:config_change, %{
          parameter: key,
          old_value: old_value || "",
          new_value: value
        })

        :ok

      {:error, _reason} = err ->
        err
    end
  end

  defp handle_config("SET", _args), do: {:error, "ERR wrong number of arguments for 'config|set' command"}

  defp handle_config("RESETSTAT", []) do
    Stats.reset()
    Ferricstore.SlowLog.reset()
    :ok
  end

  defp handle_config("RESETSTAT", _), do: {:error, "ERR wrong number of arguments for 'config|resetstat' command"}
  defp handle_config("REWRITE", []), do: :ok
  defp handle_config("REWRITE", _), do: {:error, "ERR wrong number of arguments for 'config|rewrite' command"}

  defp handle_config(subcmd, _) do
    {:error, "ERR unknown subcommand '#{String.downcase(subcmd)}' for 'config' command"}
  end

  # ---------------------------------------------------------------------------
  # MODULE stubs
  # ---------------------------------------------------------------------------

  def handle("MODULE", ["LIST" | _], _store), do: []
  def handle("MODULE", ["LOAD" | _], _store), do: {:error, "ERR FerricStore does not support modules"}
  def handle("MODULE", ["UNLOAD" | _], _store), do: {:error, "ERR FerricStore does not support modules"}
  def handle("MODULE", _, _store), do: {:error, "ERR unknown subcommand for 'module' command"}

  # ---------------------------------------------------------------------------
  # WAITAOF stub
  # ---------------------------------------------------------------------------

  def handle("WAITAOF", [_, _, _], _store), do: [0, 0]
  def handle("WAITAOF", _args, _store), do: {:error, "ERR wrong number of arguments for 'waitaof' command"}

  # ---------------------------------------------------------------------------
  # SLOWLOG
  # ---------------------------------------------------------------------------

  def handle("SLOWLOG", ["GET"], _store), do: format_slowlog_entries(Ferricstore.SlowLog.get())

  def handle("SLOWLOG", ["GET", count_str], _store) do
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 ->
        format_slowlog_entries(Ferricstore.SlowLog.get(count))

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("SLOWLOG", ["LEN"], _store), do: Ferricstore.SlowLog.len()

  def handle("SLOWLOG", ["RESET"], _store) do
    Ferricstore.SlowLog.reset()
    :ok
  end

  def handle("SLOWLOG", ["HELP"], _store) do
    [
      "SLOWLOG GET [<count>] -- Return top entries from the slowlog.",
      "SLOWLOG LEN -- Return the number of entries in the slowlog.",
      "SLOWLOG RESET -- Reset the slowlog."
    ]
  end

  def handle("SLOWLOG", _args, _store) do
    {:error, "ERR unknown subcommand or wrong number of arguments for 'slowlog' command"}
  end

  # ---------------------------------------------------------------------------
  # SAVE / BGSAVE / LASTSAVE
  # ---------------------------------------------------------------------------

  def handle("SAVE", _args, _store), do: :ok
  def handle("BGSAVE", _args, _store), do: {:simple, "Background saving started"}
  def handle("LASTSAVE", _args, _store), do: System.os_time(:second)

  # ---------------------------------------------------------------------------
  # Private — SLOWLOG formatting
  # ---------------------------------------------------------------------------

  defp format_slowlog_entries(entries) do
    Enum.map(entries, fn {id, timestamp_us, duration_us, command} ->
      [id, timestamp_us, duration_us, command]
    end)
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
