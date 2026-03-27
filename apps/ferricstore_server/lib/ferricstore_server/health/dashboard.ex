defmodule FerricstoreServer.Health.Dashboard do
  @moduledoc """
  Self-contained HTML dashboard for FerricStore observability (spec 7.3).

  Renders a single-page HTML dashboard with no external dependencies -- no
  Phoenix, no JavaScript frameworks, no CSS libraries. The page auto-refreshes
  every 2 seconds via an HTML `<meta http-equiv="refresh">` tag.

  ## Sections

  The dashboard renders eight sections matching the spec:

  1. **Overview** -- uptime, total keys, memory usage, active connections
  2. **Per-shard status** -- process status, key count, ETS memory
  3. **Hot/Cold metrics** -- hot read %, cold reads/sec, top prefixes by cold reads
  4. **Memory pressure** -- MemoryGuard levels per shard, eviction policy
  5. **Connections** -- active count, blocked clients, tracking clients
  6. **Slowlog** -- recent slow commands with duration
  7. **Merge status** -- per-shard scheduler status, last merge time, bytes reclaimed
  8. **Namespace Config** -- per-namespace overrides for window_ms and durability

  ## Architecture

  This module is a pure function module with no process state. `collect/0`
  gathers data from existing modules (`Stats`, `Health`, `MemoryGuard`,
  `SlowLog`, `Merge.Scheduler`, `Metrics`) and `render/1` turns that data
  into an HTML binary string.

  The dashboard is served by `FerricstoreServer.Health.Endpoint` at `GET /dashboard`.
  Since it reuses the existing Ranch health listener (default port 9090), no
  additional ports or dependencies are required.

  ## Usage

      # Programmatic access to dashboard data
      data = FerricstoreServer.Health.Dashboard.collect()
      html = FerricstoreServer.Health.Dashboard.render(data)
  """

  alias Ferricstore.{Health, MemoryGuard, NamespaceConfig, SlowLog, Stats}
  alias Ferricstore.Merge.Scheduler, as: MergeScheduler

  @doc false
  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "Dashboard data map containing all sections."
  @type dashboard_data :: %{
          overview: overview_data(),
          shards: [shard_data()],
          hotcold: hotcold_data(),
          memory: memory_data(),
          connections: connections_data(),
          slowlog: [slowlog_entry()],
          merge: [merge_status()],
          namespace_config: [NamespaceConfig.ns_entry()]
        }

  @typedoc "Overview section data."
  @type overview_data :: %{
          status: :ok | :starting,
          uptime_seconds: non_neg_integer(),
          total_keys: non_neg_integer(),
          total_commands: non_neg_integer(),
          total_connections: non_neg_integer(),
          memory_bytes: non_neg_integer(),
          run_id: binary()
        }

  @typedoc "Per-shard status data."
  @type shard_data :: %{
          index: non_neg_integer(),
          status: String.t(),
          keys: non_neg_integer(),
          ets_memory_bytes: non_neg_integer()
        }

  @typedoc "Hot/cold read metrics."
  @type hotcold_data :: %{
          hot_read_pct: float(),
          cold_reads_per_sec: float(),
          total_hot: non_neg_integer(),
          total_cold: non_neg_integer(),
          top_prefixes: [Stats.hotness_entry()]
        }

  @typedoc "Memory pressure data."
  @type memory_data :: %{
          total_bytes: non_neg_integer(),
          max_bytes: non_neg_integer(),
          ratio: float(),
          pressure_level: MemoryGuard.pressure_level(),
          eviction_policy: atom(),
          shards: %{non_neg_integer() => %{bytes: non_neg_integer(), ratio: float()}}
        }

  @typedoc "Connection metrics."
  @type connections_data :: %{
          active: non_neg_integer(),
          blocked: non_neg_integer(),
          tracking: non_neg_integer()
        }

  @typedoc "A single slowlog entry for display."
  @type slowlog_entry :: %{
          id: non_neg_integer(),
          timestamp_us: integer(),
          duration_us: non_neg_integer(),
          command: [binary()]
        }

  @typedoc "Merge scheduler status for one shard."
  @type merge_status :: %{
          shard_index: non_neg_integer(),
          mode: atom(),
          merging: boolean(),
          last_merge_at: integer() | nil,
          merge_count: non_neg_integer(),
          total_bytes_reclaimed: non_neg_integer()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Collects all dashboard data from running subsystems.

  Gathers information from `Health`, `Stats`, `MemoryGuard`, `SlowLog`, and
  `Merge.Scheduler`. Each data source is queried with error handling so that
  the dashboard degrades gracefully if a subsystem is temporarily unavailable.

  ## Returns

  A `dashboard_data()` map with all sections populated.

  ## Examples

      iex> data = Ferricstore.Health.Dashboard.collect()
      iex> is_integer(data.overview.uptime_seconds)
      true
  """
  @spec collect() :: dashboard_data()
  def collect do
    %{
      overview: collect_overview(),
      shards: collect_shards(),
      hotcold: collect_hotcold(),
      memory: collect_memory(),
      connections: collect_connections(),
      slowlog: collect_slowlog(),
      merge: collect_merge(),
      namespace_config: NamespaceConfig.get_all()
    }
  end

  @doc """
  Renders dashboard data as a complete HTML page.

  The returned binary is a self-contained HTML document with inline CSS. No
  external resources are loaded. The page includes a `<meta http-equiv="refresh"
  content="2">` tag for automatic 2-second polling.

  ## Parameters

    * `data` -- a `dashboard_data()` map as returned by `collect/0`

  ## Returns

  A UTF-8 binary containing the full HTML document.

  ## Examples

      iex> data = Ferricstore.Health.Dashboard.collect()
      iex> html = Ferricstore.Health.Dashboard.render(data)
      iex> String.contains?(html, "<title>")
      true
  """
  @spec render(dashboard_data()) :: binary()
  def render(data) do
    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta http-equiv="refresh" content="2">
      <title>FerricStore Dashboard</title>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }
        h1 { color: #58a6ff; margin-bottom: 20px; font-size: 1.5rem; }
        h2 { color: #79c0ff; margin: 20px 0 10px; font-size: 1.15rem; border-bottom: 1px solid #21262d; padding-bottom: 6px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 16px; }
        .card { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 14px; }
        .card .label { font-size: 0.8rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
        .card .value { font-size: 1.4rem; font-weight: 600; color: #f0f6fc; margin-top: 4px; }
        table { width: 100%; border-collapse: collapse; margin-bottom: 16px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; overflow: hidden; }
        th { background: #21262d; color: #8b949e; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 8px 12px; text-align: left; }
        td { padding: 8px 12px; border-top: 1px solid #21262d; font-size: 0.85rem; }
        tr:hover td { background: #1c2128; }
        .status-ok { color: #3fb950; }
        .status-down { color: #f85149; }
        .status-warning { color: #d29922; }
        .status-pressure { color: #f85149; }
        .status-reject { color: #f85149; font-weight: bold; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }
        .badge-ok { background: #238636; color: #fff; }
        .badge-warning { background: #9e6a03; color: #fff; }
        .badge-pressure { background: #da3633; color: #fff; }
        .badge-reject { background: #8b1a1a; color: #fff; }
        .badge-merging { background: #1f6feb; color: #fff; }
        .badge-idle { background: #30363d; color: #8b949e; }
        .mono { font-family: 'SFMono-Regular', Consolas, monospace; font-size: 0.82rem; }
        .footer { margin-top: 24px; color: #484f58; font-size: 0.75rem; text-align: center; }
        .info-icon { display: inline-block; width: 14px; height: 14px; border-radius: 50%; background: #30363d; color: #8b949e; font-size: 10px; text-align: center; line-height: 14px; cursor: help; margin-left: 4px; vertical-align: middle; }
      </style>
    </head>
    <body>
      <h1>FerricStore Dashboard</h1>
    """ <>
      render_overview(data.overview) <>
      render_shards(data.shards) <>
      render_hotcold(data.hotcold) <>
      render_memory(data.memory) <>
      render_connections(data.connections) <>
      render_slowlog(data.slowlog) <>
      render_merge(data.merge) <>
      render_namespace_config(data.namespace_config) <>
      """
        <div class="footer">
          Auto-refresh every 2s &middot; Run ID: #{escape(data.overview.run_id)}
        </div>
      </body>
      </html>
      """
  end

  # ---------------------------------------------------------------------------
  # Data collection (private)
  # ---------------------------------------------------------------------------

  @spec collect_overview() :: overview_data()
  defp collect_overview do
    health = Health.check()

    total_keys =
      health.shards
      |> Enum.map(& &1.keys)
      |> Enum.sum()

    %{
      status: health.status,
      uptime_seconds: health.uptime_seconds,
      total_keys: total_keys,
      total_commands: Stats.total_commands(),
      total_connections: Stats.total_connections(),
      memory_bytes: :erlang.memory(:total),
      run_id: Stats.run_id()
    }
  end

  @spec collect_shards() :: [shard_data()]
  defp collect_shards do
    Enum.map(0..(shard_count() - 1), fn index ->
      keydir = :"keydir_#{index}"

      {status, keys, ets_mem} =
        try do
          keys = :ets.info(keydir, :size)
          keydir_words = :ets.info(keydir, :memory)

          mem_bytes =
            case keydir_words do
              n when is_integer(n) ->
                n * :erlang.system_info(:wordsize)

              _ ->
                0
            end

          shard_name = Ferricstore.Store.Router.shard_name(index)

          shard_status =
            case Process.whereis(shard_name) do
              pid when is_pid(pid) -> if Process.alive?(pid), do: "ok", else: "down"
              nil -> "down"
            end

          {shard_status, keys, mem_bytes}
        rescue
          ArgumentError -> {"down", 0, 0}
        end

      %{index: index, status: status, keys: keys, ets_memory_bytes: ets_mem}
    end)
  end

  @spec collect_hotcold() :: hotcold_data()
  defp collect_hotcold do
    rate = :persistent_term.get(:ferricstore_read_sample_rate, 100)
    hits_sampled = Stats.keyspace_hits()
    misses = Stats.keyspace_misses()
    hot_sampled = Stats.total_hot_reads()
    cold_sampled = Stats.total_cold_reads()

    # hot_reads and keyspace_hits are sampled; cold_reads and misses are exact
    hot_est = hot_sampled * rate
    cold_exact = cold_sampled  # NOT sampled — called on every cold read
    total_hits = hot_est + cold_exact
    total_lookups = total_hits + misses

    %{
      hot_read_pct: Stats.hot_read_pct(),
      cold_reads_per_sec: Stats.cold_reads_per_second(),
      total_hot: hot_est,
      total_cold: cold_exact,
      total_hits: total_hits,
      total_misses: misses,
      total_lookups: total_lookups,
      hit_ratio: if(total_lookups > 0, do: Float.round(total_hits / total_lookups * 100, 1), else: 0.0),
      ram_ratio: if(total_hits > 0, do: Float.round(hot_est / total_hits * 100, 1), else: 0.0),
      disk_ratio: if(total_hits > 0, do: Float.round(cold_exact / total_hits * 100, 1), else: 0.0),
      sample_rate: rate,
      top_prefixes: Stats.hotness_top(10)
    }
  end

  @spec collect_memory() :: memory_data()
  defp collect_memory do
    try do
      stats = MemoryGuard.stats()

      %{
        total_bytes: stats.total_bytes,
        max_bytes: stats.max_bytes,
        ratio: stats.ratio,
        pressure_level: stats.pressure_level,
        eviction_policy: stats.eviction_policy,
        shards: stats.shards
      }
    catch
      :exit, _ ->
        %{
          total_bytes: 0,
          max_bytes: 0,
          ratio: 0.0,
          pressure_level: :ok,
          eviction_policy: :volatile_lru,
          shards: %{}
        }
    end
  end

  @spec collect_connections() :: connections_data()
  defp collect_connections do
    %{
      active: Stats.active_connections(),
      blocked: safe_ets_size(:ferricstore_waiters),
      tracking: safe_ets_size(:ferricstore_tracking_connections)
    }
  end

  @spec collect_slowlog() :: [slowlog_entry()]
  defp collect_slowlog do
    try do
      SlowLog.get(20)
      |> Enum.map(fn {id, timestamp_us, duration_us, command} ->
        %{
          id: id,
          timestamp_us: timestamp_us,
          duration_us: duration_us,
          command: command
        }
      end)
    rescue
      _ -> []
    catch
      :exit, _ -> []
    end
  end

  @spec collect_merge() :: [merge_status()]
  defp collect_merge do
    Enum.map(0..(shard_count() - 1), fn index ->
      try do
        status = MergeScheduler.status(index)

        %{
          shard_index: status.shard_index,
          mode: status.mode,
          merging: status.merging,
          last_merge_at: status.last_merge_at,
          merge_count: status.merge_count,
          total_bytes_reclaimed: status.total_bytes_reclaimed
        }
      catch
        :exit, _ ->
          %{
            shard_index: index,
            mode: :unknown,
            merging: false,
            last_merge_at: nil,
            merge_count: 0,
            total_bytes_reclaimed: 0
          }
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # HTML rendering (private)
  # ---------------------------------------------------------------------------

  @spec render_overview(overview_data()) :: binary()
  defp render_overview(data) do
    status_class = if data.status == :ok, do: "status-ok", else: "status-down"

    """
    <h2>Overview</h2>
    <div class="grid">
      <div class="card">
        <div class="label">Status</div>
        <div class="value #{status_class}">#{escape(Atom.to_string(data.status))}</div>
      </div>
      <div class="card">
        <div class="label">Uptime <span class="info-icon" title="Time since FerricStore started. Restarts reset this counter.">i</span></div>
        <div class="value">#{format_uptime(data.uptime_seconds)}</div>
      </div>
      <div class="card">
        <div class="label">Total Keys <span class="info-icon" title="Total keys across all shards. Includes both hot (in RAM) and cold (on disk) keys.">i</span></div>
        <div class="value">#{format_number(data.total_keys)}</div>
      </div>
      <div class="card">
        <div class="label">Memory <span class="info-icon" title="Total ETS memory across all shard keydirs. Includes key metadata, hot values, and LFU counters.">i</span></div>
        <div class="value">#{format_bytes(data.memory_bytes)}</div>
      </div>
      <div class="card">
        <div class="label">Commands <span class="info-icon" title="Commands processed per second across all connections. Includes both reads and writes.">i</span></div>
        <div class="value">#{format_number(data.total_commands)}</div>
      </div>
      <div class="card">
        <div class="label">Connections <span class="info-icon" title="Active TCP client connections right now. Each connection is an independent Erlang process.">i</span></div>
        <div class="value">#{format_number(Ferricstore.Stats.active_connections())}</div>
      </div>
    </div>
    """
  end

  @spec render_shards([shard_data()]) :: binary()
  defp render_shards(shards) do
    rows =
      Enum.map_join(shards, "\n", fn shard ->
        status_class =
          case shard.status do
            "ok" -> "status-ok"
            _ -> "status-down"
          end

        """
        <tr>
          <td>#{shard.index}</td>
          <td class="#{status_class}">#{escape(shard.status)}</td>
          <td>#{format_number(shard.keys)}</td>
          <td>#{format_bytes(shard.ets_memory_bytes)}</td>
        </tr>
        """
      end)

    """
    <h2>Per-Shard Status <span class="info-icon" title="Number of parallel key partitions. Each shard has its own ETS table and Raft group. Default: one per CPU core.">i</span></h2>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status <span class="info-icon" title="Whether this shard's Raft group has an elected leader. No leader = writes fail.">i</span></th><th>Keys <span class="info-icon" title="Number of keys owned by this shard.">i</span></th><th>ETS Memory <span class="info-icon" title="ETS memory used by this shard's keydir table.">i</span></th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_hotcold(hotcold_data()) :: binary()
  defp render_hotcold(data) do
    prefix_rows =
      case data.top_prefixes do
        [] ->
          "<tr><td colspan=\"4\" style=\"color:#484f58;\">No prefix data yet</td></tr>"

        prefixes ->
          Enum.map_join(prefixes, "\n", fn {prefix, hot, cold, pct} ->
            """
            <tr>
              <td class="mono">#{escape(prefix)}</td>
              <td>#{format_number(hot)}</td>
              <td>#{format_number(cold)}</td>
              <td>#{Float.round(pct, 2)}%</td>
            </tr>
            """
          end)
      end

    """
    <h2>Cache Performance</h2>
    <div class="grid">
      <div class="card">
        <div class="label">Hit Rate <span class="info-icon" title="Percentage of GET requests that found the key. Low = clients reading keys that don't exist or expired.">i</span></div>
        <div class="value" style="color:#{if data.hit_ratio >= 90, do: "#3fb950", else: if data.hit_ratio >= 70, do: "#d29922", else: "#f85149"}">#{data.hit_ratio}%</div>
      </div>
      <div class="card">
        <div class="label">Found in RAM <span class="info-icon" title="Of the keys found, how many were served from ETS (~1-5μs). Higher = faster responses.">i</span></div>
        <div class="value">#{data.ram_ratio}%</div>
      </div>
      <div class="card">
        <div class="label">Found on Disk <span class="info-icon" title="Of the keys found, how many needed Bitcask disk read (~50-200μs). High = consider increasing max_memory.">i</span></div>
        <div class="value">#{data.disk_ratio}%</div>
      </div>
      <div class="card">
        <div class="label">Misses <span class="info-icon" title="GET requests where the key didn't exist. These are fast (ETS lookup returns empty).">i</span></div>
        <div class="value">#{format_number(data.total_misses)}</div>
      </div>
    </div>
    <div style="font-size:0.75rem; color:#484f58; margin: -8px 0 12px 0;">
      Estimated from 1:#{data.sample_rate} sampling. Totals: #{format_number(data.total_lookups)} lookups, #{format_number(data.total_hits)} hits, #{format_number(data.total_misses)} misses
    </div>
    <table>
      <thead>
        <tr><th>Prefix</th><th>RAM Reads</th><th>Disk Reads</th><th>Disk %</th></tr>
      </thead>
      <tbody>
        #{prefix_rows}
      </tbody>
    </table>
    """
  end

  @spec render_memory(memory_data()) :: binary()
  defp render_memory(data) do
    level_str = Atom.to_string(data.pressure_level)

    badge_class =
      case data.pressure_level do
        :ok -> "badge-ok"
        :warning -> "badge-warning"
        :pressure -> "badge-pressure"
        :reject -> "badge-reject"
        _ -> "badge-idle"
      end

    pct = if data.max_bytes > 0, do: Float.round(data.ratio * 100, 1), else: 0.0

    shard_rows =
      data.shards
      |> Enum.sort_by(fn {index, _} -> index end)
      |> Enum.map_join("\n", fn {index, shard} ->
        shard_pct = Float.round(shard.ratio * 100, 1)

        shard_class =
          cond do
            shard.ratio >= 0.95 -> "status-reject"
            shard.ratio >= 0.85 -> "status-pressure"
            shard.ratio >= 0.70 -> "status-warning"
            true -> ""
          end

        """
        <tr>
          <td>#{index}</td>
          <td>#{format_bytes(shard.bytes)}</td>
          <td class="#{shard_class}">#{shard_pct}%</td>
        </tr>
        """
      end)

    """
    <h2>Memory Pressure</h2>
    <div class="grid">
      <div class="card">
        <div class="label">Pressure Level <span class="info-icon" title="ok = normal, warning = 70-85%, pressure = 85-95% (evicting), reject = 95%+ (writes rejected).">i</span></div>
        <div class="value"><span class="badge #{badge_class}">#{escape(level_str)}</span></div>
      </div>
      <div class="card">
        <div class="label">Usage</div>
        <div class="value">#{pct}%</div>
      </div>
      <div class="card">
        <div class="label">Used / Max <span class="info-icon" title="Configured maximum ETS memory budget. Set via FERRICSTORE_MAX_MEMORY.">i</span></div>
        <div class="value">#{format_bytes(data.total_bytes)} / #{format_bytes(data.max_bytes)}</div>
      </div>
      <div class="card">
        <div class="label">Eviction Policy</div>
        <div class="value">#{escape(Atom.to_string(data.eviction_policy))}</div>
      </div>
    </div>
    <table>
      <thead>
        <tr><th>Shard</th><th>ETS Bytes</th><th>Usage %</th></tr>
      </thead>
      <tbody>
        #{shard_rows}
      </tbody>
    </table>
    """
  end

  @spec render_connections(connections_data()) :: binary()
  defp render_connections(data) do
    """
    <h2>Connections</h2>
    <div class="grid">
      <div class="card">
        <div class="label">Active Connections <span class="info-icon" title="Active TCP client connections. Each connection is an independent Erlang process.">i</span></div>
        <div class="value">#{format_number(data.active)}</div>
      </div>
      <div class="card">
        <div class="label">Blocked Clients</div>
        <div class="value">#{format_number(data.blocked)}</div>
      </div>
      <div class="card">
        <div class="label">Tracking Clients</div>
        <div class="value">#{format_number(data.tracking)}</div>
      </div>
    </div>
    """
  end

  @spec render_slowlog([slowlog_entry()]) :: binary()
  defp render_slowlog(entries) do
    rows =
      case entries do
        [] ->
          "<tr><td colspan=\"4\" style=\"color:#484f58;\">No slow commands recorded</td></tr>"

        _ ->
          Enum.map_join(entries, "\n", fn entry ->
            cmd_str = Enum.join(entry.command, " ")
            duration_ms = Float.round(entry.duration_us / 1000.0, 2)
            time_str = format_timestamp_us(entry.timestamp_us)

            """
            <tr>
              <td>#{entry.id}</td>
              <td class="mono">#{escape(time_str)}</td>
              <td>#{duration_ms} ms</td>
              <td class="mono">#{escape(cmd_str)}</td>
            </tr>
            """
          end)
      end

    """
    <h2>Slowlog</h2>
    <table>
      <thead>
        <tr><th>ID</th><th>Time</th><th>Duration</th><th>Command</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_merge([merge_status()]) :: binary()
  defp render_merge(merges) do
    rows =
      Enum.map_join(merges, "\n", fn m ->
        status_badge =
          if m.merging do
            ~s(<span class="badge badge-merging">merging</span>)
          else
            ~s(<span class="badge badge-idle">idle</span>)
          end

        last_merge_str =
          case m.last_merge_at do
            nil -> "never"
            ts -> format_timestamp_ms(ts)
          end

        """
        <tr>
          <td>#{m.shard_index}</td>
          <td>#{escape(Atom.to_string(m.mode))}</td>
          <td>#{status_badge}</td>
          <td>#{last_merge_str}</td>
          <td>#{m.merge_count}</td>
          <td>#{format_bytes(m.total_bytes_reclaimed)}</td>
        </tr>
        """
      end)

    """
    <h2>Merge Status</h2>
    <table>
      <thead>
        <tr><th>Shard</th><th>Mode</th><th>Status</th><th>Last Merge</th><th>Merge Count</th><th>Bytes Reclaimed</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_namespace_config([NamespaceConfig.ns_entry()]) :: binary()
  defp render_namespace_config(entries) do
    {badge_class, badge_label} =
      case entries do
        [] -> {"badge-warning", "all defaults"}
        _ -> {"badge-ok", "configured"}
      end

    body =
      case entries do
        [] ->
          """
          <p style="color:#8b949e; margin: 8px 0;">All namespaces using built-in defaults (1ms, quorum)</p>
          """

        _ ->
          rows =
            Enum.map_join(entries, "\n", fn entry ->
              durability_str = Atom.to_string(entry.durability)

              durability_class =
                if entry.durability == :async, do: "status-warning", else: ""

              changed_at_str =
                if entry.changed_at == 0 do
                  "default"
                else
                  entry.changed_at
                  |> DateTime.from_unix!()
                  |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
                end

              """
              <tr>
                <td class="mono">#{escape(entry.prefix)}</td>
                <td>#{entry.window_ms}</td>
                <td class="#{durability_class}">#{escape(durability_str)}</td>
                <td>#{changed_at_str}</td>
                <td>#{escape(entry.changed_by)}</td>
              </tr>
              """
            end)

          """
          <table>
            <thead>
              <tr><th>Prefix</th><th>Window (ms)</th><th>Durability</th><th>Changed At</th><th>Changed By</th></tr>
            </thead>
            <tbody>
              #{rows}
            </tbody>
          </table>
          """
      end

    """
    <h2>Namespace Config <span class="badge #{badge_class}">#{badge_label}</span></h2>
    #{body}
    """
  end

  # ---------------------------------------------------------------------------
  # Formatting helpers (private)
  # ---------------------------------------------------------------------------

  @spec format_uptime(non_neg_integer()) :: binary()
  defp format_uptime(seconds) do
    days = div(seconds, 86_400)
    hours = div(rem(seconds, 86_400), 3_600)
    mins = div(rem(seconds, 3_600), 60)
    secs = rem(seconds, 60)

    cond do
      days > 0 -> "#{days}d #{hours}h #{mins}m"
      hours > 0 -> "#{hours}h #{mins}m #{secs}s"
      mins > 0 -> "#{mins}m #{secs}s"
      true -> "#{secs}s"
    end
  end

  @spec format_bytes(non_neg_integer()) :: binary()
  defp format_bytes(bytes) when bytes >= 1_073_741_824 do
    "#{Float.round(bytes / 1_073_741_824, 2)} GB"
  end

  defp format_bytes(bytes) when bytes >= 1_048_576 do
    "#{Float.round(bytes / 1_048_576, 2)} MB"
  end

  defp format_bytes(bytes) when bytes >= 1_024 do
    "#{Float.round(bytes / 1_024, 2)} KB"
  end

  defp format_bytes(bytes), do: "#{bytes} B"

  @spec format_number(non_neg_integer()) :: binary()
  defp format_number(n) when n >= 1_000_000 do
    "#{Float.round(n / 1_000_000, 2)}M"
  end

  defp format_number(n) when n >= 1_000 do
    "#{Float.round(n / 1_000, 1)}K"
  end

  defp format_number(n), do: Integer.to_string(n)

  @spec format_timestamp_us(integer()) :: binary()
  defp format_timestamp_us(timestamp_us) do
    timestamp_us
    |> div(1_000_000)
    |> DateTime.from_unix!()
    |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
  end

  @spec format_timestamp_ms(integer()) :: binary()
  defp format_timestamp_ms(timestamp_ms) do
    timestamp_ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
  end

  @spec escape(binary()) :: binary()
  defp escape(str) do
    str
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
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
end
