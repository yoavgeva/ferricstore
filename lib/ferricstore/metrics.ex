defmodule Ferricstore.Metrics do
  @moduledoc """
  Prometheus-compatible metrics exposition for FerricStore.

  Collects server statistics from `Ferricstore.Stats`, `Ferricstore.MemoryGuard`,
  `Ferricstore.SlowLog`, and BEAM runtime sources, then formats them in the
  Prometheus text exposition format (version 0.0.4).

  No external dependencies are required -- the module produces a plain UTF-8
  string that any Prometheus-compatible scraper can consume.

  ## Exposed metrics

  | Metric name                              | Type    | Source                        |
  |------------------------------------------|---------|-------------------------------|
  | `ferricstore_connected_clients`          | gauge   | Ranch listener connection count |
  | `ferricstore_total_connections_received`  | counter | `Stats.total_connections/0`   |
  | `ferricstore_total_commands_processed`    | counter | `Stats.total_commands/0`      |
  | `ferricstore_hot_reads_total`            | counter | `Stats.total_hot_reads/0`     |
  | `ferricstore_cold_reads_total`           | counter | `Stats.total_cold_reads/0`    |
  | `ferricstore_used_memory_bytes`          | gauge   | `:erlang.memory(:total)`      |
  | `ferricstore_keydir_used_bytes`          | gauge   | shard ETS table memory        |
  | `ferricstore_uptime_seconds`             | gauge   | `Stats.uptime_seconds/0`      |
  | `ferricstore_blocked_clients`            | gauge   | waiters ETS table size        |
  | `ferricstore_tracking_clients`           | gauge   | tracking connections ETS size |
  | `ferricstore_slowlog_entries`            | gauge   | `SlowLog.len/0`              |

  ## Usage

      iex> text = Ferricstore.Metrics.scrape()
      iex> String.contains?(text, "ferricstore_uptime_seconds")
      true

  The `FERRICSTORE.METRICS` Redis command returns this text as a bulk string.
  """

  alias Ferricstore.Stats

  @type metric_type :: :counter | :gauge

  @doc """
  Handles the `FERRICSTORE.METRICS` Redis command.

  Returns the Prometheus text exposition as a bulk string. Accepts no arguments.

  ## Parameters

    * `cmd` -- the uppercased command name (`"FERRICSTORE.METRICS"`)
    * `args` -- argument list (must be empty)

  ## Returns

  The scrape text on success, or `{:error, message}` for wrong arguments.
  """
  @spec handle(binary(), [binary()]) :: binary() | {:error, binary()}
  def handle("FERRICSTORE.METRICS", []), do: scrape()

  def handle("FERRICSTORE.METRICS", _args) do
    {:error, "ERR wrong number of arguments for 'ferricstore.metrics' command"}
  end

  @doc """
  Produces a Prometheus text exposition format string containing all FerricStore
  metrics.

  Each metric includes a `# HELP` line describing its purpose, a `# TYPE` line
  declaring its Prometheus type, and a sample line with the current value.

  ## Returns

  A UTF-8 binary string in Prometheus text exposition format.

  ## Examples

      iex> text = Ferricstore.Metrics.scrape()
      iex> text |> String.split("\\n") |> Enum.count(&String.starts_with?(&1, "# HELP")) >= 11
      true
  """
  @spec scrape() :: binary()
  def scrape do
    metrics()
    |> Enum.map_join("\n", &format_metric/1)
    |> Kernel.<>("\n")
  end

  # ---------------------------------------------------------------------------
  # Private: metric collection
  # ---------------------------------------------------------------------------

  @spec metrics() :: [{binary(), metric_type(), binary(), non_neg_integer()}]
  defp metrics do
    [
      {"ferricstore_connected_clients", :gauge,
       "Number of active client connections", connected_clients()},
      {"ferricstore_total_connections_received", :counter,
       "Total number of TCP connections accepted since startup",
       Stats.total_connections()},
      {"ferricstore_total_commands_processed", :counter,
       "Total number of commands dispatched since startup",
       Stats.total_commands()},
      {"ferricstore_hot_reads_total", :counter,
       "Total number of reads served from the ETS hot cache",
       Stats.total_hot_reads()},
      {"ferricstore_cold_reads_total", :counter,
       "Total number of reads that fell through to Bitcask on disk",
       Stats.total_cold_reads()},
      {"ferricstore_used_memory_bytes", :gauge,
       "Total BEAM VM memory usage in bytes", :erlang.memory(:total)},
      {"ferricstore_keydir_used_bytes", :gauge,
       "Total ETS memory used by shard keydir tables in bytes",
       keydir_used_bytes()},
      {"ferricstore_uptime_seconds", :gauge,
       "Server uptime in seconds", Stats.uptime_seconds()},
      {"ferricstore_blocked_clients", :gauge,
       "Number of clients blocked on BLPOP/BRPOP/BLMOVE/BLMPOP",
       safe_ets_size(:ferricstore_waiters)},
      {"ferricstore_tracking_clients", :gauge,
       "Number of clients with client-side caching tracking enabled",
       safe_ets_size(:ferricstore_tracking_connections)},
      {"ferricstore_slowlog_entries", :gauge,
       "Current number of entries in the slow log",
       slowlog_len()}
    ]
  end

  # ---------------------------------------------------------------------------
  # Private: formatting
  # ---------------------------------------------------------------------------

  @spec format_metric({binary(), metric_type(), binary(), integer()}) :: binary()
  defp format_metric({name, type, help, value}) do
    type_str = Atom.to_string(type)

    "# HELP #{name} #{help}\n# TYPE #{name} #{type_str}\n#{name} #{value}"
  end

  # ---------------------------------------------------------------------------
  # Private: data sources
  # ---------------------------------------------------------------------------

  @spec connected_clients() :: non_neg_integer()
  defp connected_clients do
    try do
      :ranch.procs(Ferricstore.Server.Listener, :connections) |> length()
    rescue
      _ -> 0
    end
  end

  @spec keydir_used_bytes() :: non_neg_integer()
  defp keydir_used_bytes do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
      keydir = :"keydir_#{i}"
      hot_cache = :"hot_cache_#{i}"

      try do
        keydir_words = :ets.info(keydir, :memory)
        hot_cache_words = :ets.info(hot_cache, :memory)

        words =
          case {keydir_words, hot_cache_words} do
            {n1, n2} when is_integer(n1) and is_integer(n2) -> n1 + n2
            {n1, _} when is_integer(n1) -> n1
            {_, n2} when is_integer(n2) -> n2
            _ -> 0
          end

        acc + words * :erlang.system_info(:wordsize)
      rescue
        ArgumentError -> acc
      end
    end)
  end

  @spec safe_ets_size(atom()) :: non_neg_integer()
  defp safe_ets_size(table) do
    try do
      case :ets.info(table, :size) do
        :undefined -> 0
        n when is_integer(n) -> n
        _ -> 0
      end
    rescue
      ArgumentError -> 0
    end
  end

  @spec slowlog_len() :: non_neg_integer()
  defp slowlog_len do
    try do
      Ferricstore.SlowLog.len()
    rescue
      _ -> 0
    catch
      :exit, _ -> 0
    end
  end
end
