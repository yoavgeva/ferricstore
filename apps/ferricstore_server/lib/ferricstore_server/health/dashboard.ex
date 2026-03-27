defmodule FerricstoreServer.Health.Dashboard do
  @moduledoc """
  Self-contained HTML dashboard for FerricStore observability (spec 7.3).

  Renders a single-page HTML dashboard with no external dependencies -- no
  Phoenix, no JavaScript frameworks, no CSS libraries. The page auto-refreshes
  every 2 seconds via an HTML `<meta http-equiv="refresh">` tag.

  ## Design

  The dashboard follows a progressive-disclosure layout optimized for
  glanceability:

  1. **Top bar** -- status badge, ops/sec, hit rate, memory bar, connections
  2. **Cache Performance** -- hit rate gauge + RAM vs disk breakdown
  3. **Shards** -- compact status table
  4. **Memory** -- only shown when pressure level != :ok
  5. **Footer** -- uptime, version, sample rate note, refresh indicator

  Slow log, merge status, and namespace config are available as collapsed
  sections for on-demand inspection.

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
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace; background: #0d1117; color: #c9d1d9; padding: 0; min-height: 100vh; }

        /* Top bar */
        .top-bar { display: flex; align-items: center; gap: 24px; padding: 12px 20px; background: #161b22; border-bottom: 1px solid #30363d; flex-wrap: wrap; }
        .top-bar .logo { font-size: 1.1rem; font-weight: 700; color: #58a6ff; white-space: nowrap; }
        .top-bar .metric { display: flex; flex-direction: column; align-items: center; min-width: 80px; }
        .top-bar .metric .label { font-size: 0.65rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
        .top-bar .metric .val { font-size: 1.1rem; font-weight: 700; color: #f0f6fc; }
        .top-bar .sep { width: 1px; height: 32px; background: #30363d; flex-shrink: 0; }

        /* Status badge */
        .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
        .dot-green { background: #3fb950; box-shadow: 0 0 6px #3fb95066; }
        .dot-yellow { background: #d29922; box-shadow: 0 0 6px #d2992266; }
        .dot-red { background: #f85149; box-shadow: 0 0 6px #f8514966; }

        /* Memory bar in top bar */
        .mem-bar-wrap { width: 80px; height: 6px; background: #21262d; border-radius: 3px; margin-top: 2px; overflow: hidden; }
        .mem-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }

        /* Main content */
        .content { padding: 16px 20px 80px; max-width: 1200px; margin: 0 auto; }

        /* Section headers */
        .section-title { font-size: 0.9rem; font-weight: 600; color: #79c0ff; margin: 24px 0 12px; text-transform: uppercase; letter-spacing: 0.5px; }
        .section-title:first-child { margin-top: 8px; }

        /* Hero hit rate */
        .cache-hero { display: flex; gap: 24px; margin-bottom: 16px; flex-wrap: wrap; }
        .hit-rate-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px 28px; text-align: center; min-width: 180px; flex: 0 0 auto; }
        .hit-rate-num { font-size: 3rem; font-weight: 800; line-height: 1.1; }
        .hit-rate-label { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }
        .hit-rate-sub { font-size: 0.8rem; color: #8b949e; margin-top: 8px; }
        .hit-rate-sub span { color: #c9d1d9; font-weight: 600; }

        /* Source breakdown */
        .source-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px 24px; flex: 1; min-width: 200px; }
        .source-row { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; }
        .source-row + .source-row { border-top: 1px solid #21262d; }
        .source-name { font-size: 0.85rem; color: #c9d1d9; }
        .source-detail { font-size: 0.7rem; color: #484f58; }
        .source-pct { font-size: 1.1rem; font-weight: 700; }
        .source-bar-wrap { width: 100%; height: 4px; background: #21262d; border-radius: 2px; margin-top: 4px; }
        .source-bar-fill { height: 100%; border-radius: 2px; }

        /* Compact table */
        table { width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 6px; overflow: hidden; font-size: 0.82rem; }
        th { background: #21262d; color: #8b949e; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 6px 12px; text-align: left; }
        td { padding: 6px 12px; border-top: 1px solid #21262d; }
        tr:hover td { background: #1c2128; }

        /* Status colors */
        .c-green { color: #3fb950; }
        .c-yellow { color: #d29922; }
        .c-red { color: #f85149; }
        .c-muted { color: #484f58; }

        /* Badges */
        .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
        .badge-ok { background: #238636; color: #fff; }
        .badge-warning { background: #9e6a03; color: #fff; }
        .badge-pressure { background: #da3633; color: #fff; }
        .badge-reject { background: #8b1a1a; color: #fff; }
        .badge-merging { background: #1f6feb; color: #fff; }
        .badge-idle { background: #30363d; color: #8b949e; }

        /* Memory pressure alert */
        .pressure-alert { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
        .pressure-alert.level-warning { border-color: #9e6a03; }
        .pressure-alert.level-pressure { border-color: #da3633; }
        .pressure-alert.level-reject { border-color: #f85149; border-width: 2px; }
        .pressure-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
        .pressure-bar-wrap { width: 100%; height: 8px; background: #21262d; border-radius: 4px; overflow: hidden; margin: 8px 0; }
        .pressure-bar-fill { height: 100%; border-radius: 4px; }
        .pressure-details { font-size: 0.8rem; color: #8b949e; }
        .pressure-details span { color: #c9d1d9; font-weight: 600; }
        .pressure-action { font-size: 0.75rem; color: #d29922; margin-top: 6px; font-style: italic; }

        /* Connections inline */
        .conn-row { display: flex; gap: 24px; align-items: center; background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px 16px; font-size: 0.85rem; flex-wrap: wrap; }
        .conn-item .conn-label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.3px; }
        .conn-item .conn-val { font-weight: 700; color: #f0f6fc; }

        /* Collapsible sections */
        details { margin-bottom: 12px; }
        details summary { cursor: pointer; font-size: 0.85rem; color: #8b949e; padding: 8px 0; user-select: none; list-style: none; }
        details summary::-webkit-details-marker { display: none; }
        details summary::before { content: "\\25B6\\00A0"; font-size: 0.7rem; }
        details[open] summary::before { content: "\\25BC\\00A0"; }
        details summary:hover { color: #c9d1d9; }

        /* Footer */
        .footer { position: fixed; bottom: 0; left: 0; right: 0; background: #0d1117; border-top: 1px solid #21262d; padding: 6px 20px; font-size: 0.7rem; color: #484f58; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; }

        /* Tooltip */
        .info-icon { display: inline-block; width: 14px; height: 14px; border-radius: 50%; background: #30363d; color: #8b949e; font-size: 10px; text-align: center; line-height: 14px; cursor: help; margin-left: 4px; vertical-align: middle; }

        .mono { font-family: 'SFMono-Regular', Consolas, monospace; font-size: 0.82rem; }

        /* Responsive */
        @media (max-width: 600px) {
          .top-bar { gap: 12px; padding: 10px 12px; }
          .top-bar .metric .val { font-size: 0.9rem; }
          .top-bar .sep { display: none; }
          .content { padding: 12px 12px 70px; }
          .hit-rate-num { font-size: 2.2rem; }
          .cache-hero { flex-direction: column; }
          .hit-rate-card { min-width: unset; }
        }
      </style>
    </head>
    <body>
    """ <>
      render_top_bar(data) <>
      ~s(<div class="content">) <>
      render_cache_performance(data.hotcold) <>
      render_shards(data.shards) <>
      render_memory_alert(data.memory) <>
      render_connections(data.connections) <>
      render_collapsed_slowlog(data.slowlog) <>
      render_collapsed_merge(data.merge) <>
      render_collapsed_namespace_config(data.namespace_config) <>
      ~s(</div>) <>
      render_footer(data) <>
      """
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
    _hits_sampled = Stats.keyspace_hits()
    misses = Stats.keyspace_misses()
    hot_sampled = Stats.total_hot_reads()
    cold_sampled = Stats.total_cold_reads()

    # hot_reads and keyspace_hits are sampled; cold_reads and misses are exact
    hot_est = hot_sampled * rate
    cold_exact = cold_sampled  # NOT sampled — called on every cold read
    total_hits = hot_est + cold_exact
    total_lookups = total_hits + misses

    uptime = max(Stats.uptime_seconds(), 1)

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
      hits_per_sec: Float.round(total_hits / uptime, 1),
      misses_per_sec: Float.round(misses / uptime, 1),
      ops_per_sec: Float.round(Stats.total_commands() / uptime, 1),
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

  @spec render_top_bar(dashboard_data()) :: binary()
  defp render_top_bar(data) do
    overview = data.overview
    hotcold = data.hotcold
    memory = data.memory
    conns = data.connections

    # Status dot color
    {dot_class, status_text} =
      cond do
        overview.status != :ok -> {"dot-red", "degraded"}
        memory.pressure_level == :reject -> {"dot-red", "rejecting"}
        memory.pressure_level == :pressure -> {"dot-yellow", "pressure"}
        memory.pressure_level == :warning -> {"dot-yellow", "warning"}
        true -> {"dot-green", "healthy"}
      end

    # Hit rate color
    hit_color = hit_rate_color(hotcold.hit_ratio)

    # Memory bar
    mem_pct = if memory.max_bytes > 0, do: Float.round(memory.ratio * 100, 1), else: 0.0
    mem_bar_color = mem_bar_color(mem_pct)
    mem_bar_width = min(mem_pct, 100)

    """
    <div class="top-bar">
      <div class="logo"><span class="status-dot #{dot_class}"></span>FerricStore</div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Status</span>
        <span class="val" style="font-size:0.85rem;">#{escape(status_text)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Ops/sec</span>
        <span class="val">#{format_rate(hotcold.ops_per_sec)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Hit Rate</span>
        <span class="val" style="color:#{hit_color};">#{hotcold.hit_ratio}%</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Memory</span>
        <span class="val" style="font-size:0.85rem;">#{format_bytes(memory.total_bytes)} / #{format_bytes(memory.max_bytes)}</span>
        <div class="mem-bar-wrap"><div class="mem-bar-fill" style="width:#{mem_bar_width}%;background:#{mem_bar_color};"></div></div>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Connections</span>
        <span class="val">#{format_number(conns.active)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Keys</span>
        <span class="val">#{format_number(overview.total_keys)}</span>
      </div>
    </div>
    """
  end

  @spec render_cache_performance(hotcold_data()) :: binary()
  defp render_cache_performance(data) do
    hit_color = hit_rate_color(data.hit_ratio)

    # RAM bar color -- always green (fast path)
    # Disk bar color -- orange (slow path)
    ram_bar_width = min(data.ram_ratio, 100)
    disk_bar_width = min(data.disk_ratio, 100)

    """
    <div class="section-title">Cache Performance</div>
    <div class="cache-hero">
      <div class="hit-rate-card">
        <div class="hit-rate-num" style="color:#{hit_color};">#{data.hit_ratio}%</div>
        <div class="hit-rate-label">Hit Rate</div>
        <div class="hit-rate-sub">
          <span>#{format_rate(data.hits_per_sec)}</span> hits/sec &middot;
          <span>#{format_rate(data.misses_per_sec)}</span> misses/sec
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Where hits come from</div>
        <div class="source-row">
          <div>
            <div class="source-name">RAM <span class="info-icon" title="Served from ETS in-memory cache. Latency: ~1-5us.">i</span></div>
            <div class="source-detail">fast path (~1-5us)</div>
          </div>
          <div class="source-pct c-green">#{data.ram_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{ram_bar_width}%;background:#3fb950;"></div></div>
        <div class="source-row">
          <div>
            <div class="source-name">Disk <span class="info-icon" title="Required Bitcask disk read. Latency: ~50-200us. High disk ratio means memory pressure is evicting hot keys.">i</span></div>
            <div class="source-detail">slow path (~50-200us)</div>
          </div>
          <div class="source-pct c-yellow">#{data.disk_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{disk_bar_width}%;background:#d29922;"></div></div>
      </div>
    </div>
    """
  end

  @spec render_shards([shard_data()]) :: binary()
  defp render_shards(shards) do
    all_ok = Enum.all?(shards, fn s -> s.status == "ok" end)

    rows =
      Enum.map_join(shards, "\n", fn shard ->
        status_html =
          case shard.status do
            "ok" -> ~s(<span class="c-green">ok</span>)
            _ -> ~s(<span class="c-red">#{escape(shard.status)}</span>)
          end

        """
        <tr>
          <td>#{shard.index}</td>
          <td>#{status_html}</td>
          <td>#{format_number(shard.keys)}</td>
          <td>#{format_bytes(shard.ets_memory_bytes)}</td>
        </tr>
        """
      end)

    summary_badge =
      if all_ok do
        ~s(<span class="badge badge-ok">all ok</span>)
      else
        down_count = Enum.count(shards, fn s -> s.status != "ok" end)
        ~s(<span class="badge badge-pressure">#{down_count} down</span>)
      end

    """
    <div class="section-title">Shards #{summary_badge}</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Keys</th><th>Memory</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_memory_alert(memory_data()) :: binary()
  defp render_memory_alert(data) do
    # Only show the memory section when there is pressure
    if data.pressure_level == :ok do
      ""
    else
      level_str = Atom.to_string(data.pressure_level)
      pct = if data.max_bytes > 0, do: Float.round(data.ratio * 100, 1), else: 0.0
      bar_color = mem_bar_color(pct)
      bar_width = min(pct, 100)

      badge_class =
        case data.pressure_level do
          :warning -> "badge-warning"
          :pressure -> "badge-pressure"
          :reject -> "badge-reject"
          _ -> "badge-idle"
        end

      level_class =
        case data.pressure_level do
          :warning -> "level-warning"
          :pressure -> "level-pressure"
          :reject -> "level-reject"
          _ -> ""
        end

      action_text =
        case data.pressure_level do
          :warning -> "Consider increasing max_memory or reviewing eviction policy."
          :pressure -> "Eviction active. Keys are being removed under #{escape(Atom.to_string(data.eviction_policy))} policy."
          :reject -> "Writes are being rejected. Increase max_memory immediately."
          _ -> ""
        end

      shard_rows =
        data.shards
        |> Enum.sort_by(fn {index, _} -> index end)
        |> Enum.map_join("\n", fn {index, shard} ->
          shard_pct = Float.round(shard.ratio * 100, 1)

          shard_class =
            cond do
              shard.ratio >= 0.95 -> "c-red"
              shard.ratio >= 0.85 -> "c-red"
              shard.ratio >= 0.70 -> "c-yellow"
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
      <div class="section-title">Memory Pressure <span class="badge #{badge_class}">#{escape(level_str)}</span></div>
      <div class="pressure-alert #{level_class}">
        <div class="pressure-details">
          <span>#{format_bytes(data.total_bytes)}</span> / <span>#{format_bytes(data.max_bytes)}</span> (#{pct}%)
          &middot; Policy: <span>#{escape(Atom.to_string(data.eviction_policy))}</span>
        </div>
        <div class="pressure-bar-wrap"><div class="pressure-bar-fill" style="width:#{bar_width}%;background:#{bar_color};"></div></div>
        <div class="pressure-action">#{action_text}</div>
      </div>
      <table>
        <thead>
          <tr><th>Shard</th><th>Bytes</th><th>Usage</th></tr>
        </thead>
        <tbody>
          #{shard_rows}
        </tbody>
      </table>
      """
    end
  end

  @spec render_connections(connections_data()) :: binary()
  defp render_connections(data) do
    blocked_class = if data.blocked > 0, do: "c-yellow", else: ""

    """
    <div class="section-title">Connections</div>
    <div class="conn-row">
      <div class="conn-item">
        <span class="conn-label">Active </span>
        <span class="conn-val">#{format_number(data.active)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Blocked </span>
        <span class="conn-val #{blocked_class}">#{format_number(data.blocked)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Tracking </span>
        <span class="conn-val">#{format_number(data.tracking)}</span>
      </div>
    </div>
    """
  end

  @spec render_collapsed_slowlog([slowlog_entry()]) :: binary()
  defp render_collapsed_slowlog(entries) do
    count = length(entries)
    count_label = if count == 0, do: "none", else: "#{count} entries"

    rows =
      case entries do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No slow commands recorded</td></tr>)

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
    <details>
      <summary>Slow Log (#{count_label})</summary>
      <table>
        <thead>
          <tr><th>ID</th><th>Time</th><th>Duration</th><th>Command</th></tr>
        </thead>
        <tbody>
          #{rows}
        </tbody>
      </table>
    </details>
    """
  end

  @spec render_collapsed_merge([merge_status()]) :: binary()
  defp render_collapsed_merge(merges) do
    active_count = Enum.count(merges, & &1.merging)
    summary_label = if active_count > 0, do: "#{active_count} active", else: "idle"

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
    <details>
      <summary>Merge Status (#{summary_label})</summary>
      <table>
        <thead>
          <tr><th>Shard</th><th>Mode</th><th>Status</th><th>Last Merge</th><th>Merges</th><th>Reclaimed</th></tr>
        </thead>
        <tbody>
          #{rows}
        </tbody>
      </table>
    </details>
    """
  end

  @spec render_collapsed_namespace_config([NamespaceConfig.ns_entry()]) :: binary()
  defp render_collapsed_namespace_config(entries) do
    count_label =
      case entries do
        [] -> "defaults"
        list -> "#{length(list)} overrides"
      end

    body =
      case entries do
        [] ->
          ~s[<p style="color:#8b949e; margin: 8px 0; font-size:0.82rem;">All namespaces using built-in defaults (1ms, quorum)</p>]

        _ ->
          rows =
            Enum.map_join(entries, "\n", fn entry ->
              durability_str = Atom.to_string(entry.durability)

              durability_class =
                if entry.durability == :async, do: "c-yellow", else: ""

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
    <details>
      <summary>Namespace Config (#{count_label})</summary>
      #{body}
    </details>
    """
  end

  @spec render_footer(dashboard_data()) :: binary()
  defp render_footer(data) do
    sample_rate = data.hotcold.sample_rate

    """
    <div class="footer">
      <span>Uptime: #{format_uptime(data.overview.uptime_seconds)} &middot; v0.1.0 &middot; Run #{escape(String.slice(data.overview.run_id, 0, 8))}</span>
      <span>Hit/miss stats estimated from 1:#{sample_rate} sampling &middot; Auto-refresh 2s</span>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Formatting helpers (private)
  # ---------------------------------------------------------------------------

  @spec hit_rate_color(float()) :: binary()
  defp hit_rate_color(ratio) do
    cond do
      ratio >= 90.0 -> "#3fb950"
      ratio >= 70.0 -> "#d29922"
      true -> "#f85149"
    end
  end

  @spec mem_bar_color(float()) :: binary()
  defp mem_bar_color(pct) do
    cond do
      pct >= 95.0 -> "#f85149"
      pct >= 85.0 -> "#da3633"
      pct >= 70.0 -> "#d29922"
      true -> "#3fb950"
    end
  end

  @spec format_rate(float()) :: binary()
  defp format_rate(rate) when rate >= 1_000_000.0 do
    "#{Float.round(rate / 1_000_000, 1)}M"
  end

  defp format_rate(rate) when rate >= 1_000.0 do
    "#{Float.round(rate / 1_000, 1)}K"
  end

  defp format_rate(rate) do
    "#{Float.round(rate, 1)}"
  end

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
