defmodule Ferricstore.MemoryGuard do
  @moduledoc """
  Periodic memory pressure monitor for FerricStore shard ETS tables.

  MemoryGuard runs as a GenServer that checks memory usage every 100ms
  (configurable). It monitors each shard's ETS table memory consumption
  and takes action based on configurable pressure thresholds:

    * **Warning (70%)** -- Logs a warning. No action taken.
    * **Pressure (85%)** -- Logs an error and emits telemetry. The system
      should begin considering eviction.
    * **Reject (95%)** -- Logs a critical error and emits telemetry. New
      writes should be rejected with an OOM error when the eviction policy
      is `:noeviction`.

  ## Eviction policies (per spec section 2.4)

    * `:volatile_lru` (default) -- Evict least recently used keys that have
      a TTL set. Keys without TTL are never evicted.
    * `:allkeys_lru` -- Evict least recently used key regardless of TTL.
    * `:volatile_ttl` -- Evict the key with the shortest remaining TTL first.
    * `:noeviction` -- Return OOM error when memory is full. No keys are
      evicted.

  ## Telemetry events

    * `[:ferricstore, :memory, :check]` -- emitted on every check with
      measurements `%{total_bytes: integer, shard_bytes: map}` and metadata
      `%{pressure_level: :ok | :warning | :pressure | :reject}`.

    * `[:ferricstore, :memory, :pressure]` -- emitted when any shard crosses
      the pressure (85%) or reject (95%) threshold. Measurements include
      `%{shard_index: integer, bytes: integer, max_bytes: integer, ratio: float}`.

    * `[:ferricstore, :memory, :recovered]` -- emitted once when pressure drops
      back to `:ok` from `:pressure` or `:reject`. Measurements include
      `%{total_bytes: integer, max_bytes: integer, ratio: float}`. Metadata
      includes `%{previous_level: :pressure | :reject}`.

  ## Configuration

  MemoryGuard reads its configuration from the application environment:

    * `:memory_guard_interval_ms` -- check interval in milliseconds (default: 100)
    * `:max_memory_bytes` -- maximum total ETS memory budget in bytes
      (default: 75% of system memory)
    * `:eviction_policy` -- one of `:volatile_lru`, `:allkeys_lru`,
      `:volatile_ttl`, `:noeviction` (default: `:volatile_lru`)

  ## Supervision

  MemoryGuard is added to the application supervision tree after the
  ShardSupervisor so that shards are already running when checks begin.
  """

  use GenServer

  require Logger

  @check_interval_ms 100

  @warning_threshold 0.70
  @pressure_threshold 0.85
  @reject_threshold 0.95

  @type pressure_level :: :ok | :warning | :pressure | :reject

  defstruct [
    :interval_ms,
    :max_memory_bytes,
    :eviction_policy,
    :shard_count,
    last_pressure_level: :ok
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the MemoryGuard GenServer.

  ## Options

    * `:interval_ms` -- check interval in milliseconds (default: 100)
    * `:max_memory_bytes` -- total ETS memory budget (default: 75% of system RAM)
    * `:eviction_policy` -- eviction policy atom (default: `:volatile_lru`)
    * `:shard_count` -- number of shards to monitor (default: 4)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the current memory usage stats for all shards.

  ## Returns

  A map with:
    * `:total_bytes` -- total ETS memory across all shards
    * `:max_bytes` -- configured maximum memory budget
    * `:ratio` -- `total_bytes / max_bytes`
    * `:pressure_level` -- `:ok`, `:warning`, `:pressure`, or `:reject`
    * `:shards` -- per-shard breakdown as `%{index => %{bytes: n, ratio: f}}`
    * `:eviction_policy` -- configured eviction policy
  """
  @spec stats() :: map()
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc """
  Returns the current eviction policy.
  """
  @spec eviction_policy() :: atom()
  def eviction_policy do
    GenServer.call(__MODULE__, :eviction_policy)
  end

  @doc """
  Returns `true` if memory pressure is at or above the reject threshold.

  This is a fast check that shard write paths can use to decide whether
  to reject new writes under the `:noeviction` policy.
  """
  @spec reject_writes?() :: boolean()
  def reject_writes? do
    GenServer.call(__MODULE__, :reject_writes?)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    interval_ms = Keyword.get(opts, :interval_ms, default_interval())
    max_memory_bytes = Keyword.get(opts, :max_memory_bytes, default_max_memory())
    eviction_policy = Keyword.get(opts, :eviction_policy, default_eviction_policy())
    shard_count = Keyword.get(opts, :shard_count, default_shard_count())

    state = %__MODULE__{
      interval_ms: interval_ms,
      max_memory_bytes: max_memory_bytes,
      eviction_policy: eviction_policy,
      shard_count: shard_count,
      last_pressure_level: :ok
    }

    schedule_check(interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    {:reply, compute_stats(state), state}
  end

  def handle_call(:eviction_policy, _from, state) do
    {:reply, state.eviction_policy, state}
  end

  def handle_call(:reject_writes?, _from, state) do
    {:reply, state.last_pressure_level == :reject and state.eviction_policy == :noeviction, state}
  end

  @impl true
  def handle_info(:check, state) do
    state = perform_check(state)
    schedule_check(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    # Ignore unexpected messages (e.g. from telemetry test handlers that
    # accidentally target the GenServer process).
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private: check logic
  # ---------------------------------------------------------------------------

  defp perform_check(state) do
    stats = compute_stats(state)

    # Emit telemetry for every check
    :telemetry.execute(
      [:ferricstore, :memory, :check],
      %{total_bytes: stats.total_bytes},
      %{
        pressure_level: stats.pressure_level,
        ratio: stats.ratio,
        max_bytes: stats.max_bytes
      }
    )

    # Log and emit additional telemetry for pressure transitions
    case stats.pressure_level do
      :ok ->
        if state.last_pressure_level in [:pressure, :reject] do
          Logger.info("MemoryGuard: memory pressure resolved (#{format_ratio(stats.ratio)})")

          :telemetry.execute(
            [:ferricstore, :memory, :recovered],
            %{total_bytes: stats.total_bytes, max_bytes: stats.max_bytes, ratio: stats.ratio},
            %{previous_level: state.last_pressure_level}
          )
        end

      :warning ->
        if state.last_pressure_level not in [:warning, :pressure, :reject] do
          Logger.warning(
            "MemoryGuard: memory at #{format_ratio(stats.ratio)} " <>
              "(#{format_bytes(stats.total_bytes)}/#{format_bytes(stats.max_bytes)})"
          )
        end

      :pressure ->
        Logger.error(
          "MemoryGuard: high memory pressure at #{format_ratio(stats.ratio)} " <>
            "(#{format_bytes(stats.total_bytes)}/#{format_bytes(stats.max_bytes)})"
        )

        emit_pressure_events(stats)

      :reject ->
        Logger.critical(
          "MemoryGuard: critical memory at #{format_ratio(stats.ratio)} " <>
            "(#{format_bytes(stats.total_bytes)}/#{format_bytes(stats.max_bytes)}) " <>
            "- writes may be rejected (policy: #{state.eviction_policy})"
        )

        emit_pressure_events(stats)
    end

    %{state | last_pressure_level: stats.pressure_level}
  end

  defp compute_stats(state) do
    shard_stats =
      Enum.reduce(0..(state.shard_count - 1), %{}, fn i, acc ->
        bytes = safe_ets_memory(:"keydir_#{i}") + safe_ets_memory(:"hot_cache_#{i}")
        per_shard_max = div(state.max_memory_bytes, max(state.shard_count, 1))

        ratio =
          cond do
            per_shard_max > 0 -> bytes / per_shard_max
            bytes > 0 -> 1.0
            true -> 0.0
          end

        Map.put(acc, i, %{bytes: bytes, ratio: ratio})
      end)

    total_bytes = shard_stats |> Map.values() |> Enum.map(& &1.bytes) |> Enum.sum()

    ratio =
      if state.max_memory_bytes > 0 do
        total_bytes / state.max_memory_bytes
      else
        0.0
      end

    pressure_level = classify_pressure(ratio)

    %{
      total_bytes: total_bytes,
      max_bytes: state.max_memory_bytes,
      ratio: ratio,
      pressure_level: pressure_level,
      shards: shard_stats,
      eviction_policy: state.eviction_policy
    }
  end

  defp safe_ets_memory(table_name) do
    case :ets.info(table_name, :memory) do
      :undefined -> 0
      memory when is_integer(memory) -> memory * word_size()
      _ -> 0
    end
  rescue
    _ -> 0
  catch
    _, _ -> 0
  end

  defp word_size do
    :erlang.system_info(:wordsize)
  end

  defp classify_pressure(ratio) when ratio >= @reject_threshold, do: :reject
  defp classify_pressure(ratio) when ratio >= @pressure_threshold, do: :pressure
  defp classify_pressure(ratio) when ratio >= @warning_threshold, do: :warning
  defp classify_pressure(_ratio), do: :ok

  defp emit_pressure_events(stats) do
    Enum.each(stats.shards, fn {index, shard_stat} ->
      if shard_stat.ratio >= @pressure_threshold do
        per_shard_max = div(stats.max_bytes, max(map_size(stats.shards), 1))

        :telemetry.execute(
          [:ferricstore, :memory, :pressure],
          %{
            shard_index: index,
            bytes: shard_stat.bytes,
            max_bytes: per_shard_max,
            ratio: shard_stat.ratio
          },
          %{pressure_level: stats.pressure_level, eviction_policy: stats.eviction_policy}
        )
      end
    end)
  end

  defp schedule_check(interval_ms) do
    Process.send_after(self(), :check, interval_ms)
  end

  # ---------------------------------------------------------------------------
  # Private: configuration defaults
  # ---------------------------------------------------------------------------

  defp default_interval do
    Application.get_env(:ferricstore, :memory_guard_interval_ms, @check_interval_ms)
  end

  defp default_max_memory do
    Application.get_env(:ferricstore, :max_memory_bytes, default_system_memory())
  end

  defp default_system_memory do
    # Default: 75% of system memory, or 1GB if detection fails.
    # :memsup requires the :os_mon application to be started, which may not
    # be available in all environments. Fall back to 1GB gracefully.
    try do
      data = apply(:memsup, :get_system_memory_data, [])

      case data do
        list when is_list(list) ->
          total = Keyword.get(list, :total_memory, 1_073_741_824)
          trunc(total * 0.75)

        _ ->
          1_073_741_824
      end
    rescue
      _ -> 1_073_741_824
    catch
      _, _ -> 1_073_741_824
    end
  end

  defp default_eviction_policy do
    Application.get_env(:ferricstore, :eviction_policy, :volatile_lru)
  end

  defp default_shard_count do
    Application.get_env(:ferricstore, :shard_count, 4)
  end

  # ---------------------------------------------------------------------------
  # Private: formatting
  # ---------------------------------------------------------------------------

  defp format_ratio(ratio), do: "#{Float.round(ratio * 100, 1)}%"

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
end
