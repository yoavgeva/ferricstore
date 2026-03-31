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
      measurements `%{total_bytes: integer}` and metadata
      `%{pressure_level: :ok | :warning | :pressure | :reject}`.

    * `[:ferricstore, :memory, :pressure]` -- emitted on every check with
      the spec 2.4 pressure level. Measurements include
      `%{total_bytes: integer, max_bytes: integer, ratio: float}`. Metadata
      includes `%{level: :ok | :warn | :pressure | :full}`.

    * `[:ferricstore, :memory, :recovered]` -- emitted once when pressure drops
      back to `:ok` from `:pressure` or `:reject`.

    * `[:ferricstore, :hot_cache, :limit_reduced]` -- emitted when the hot_cache
      budget shrinks due to increasing memory pressure.

    * `[:ferricstore, :hot_cache, :limit_restored]` -- emitted when the hot_cache
      budget recovers as memory pressure decreases.

  ## Configuration

    * `:memory_guard_interval_ms` -- check interval in milliseconds (default: 100)
    * `:max_memory_bytes` -- maximum total ETS memory budget in bytes
    * `:eviction_policy` -- eviction policy atom (default: `:volatile_lru`)
  """

  use GenServer

  require Logger

  @check_interval_ms 100

  @warning_threshold 0.70
  @pressure_threshold 0.85
  @reject_threshold 0.95

  @type pressure_level :: :ok | :warning | :pressure | :reject

  @typedoc "Spec 2.4 pressure level names used in telemetry metadata."
  @type spec_level :: :ok | :warn | :pressure | :full

  defstruct [
    :interval_ms,
    :max_memory_bytes,
    :eviction_policy,
    :shard_count,
    :keydir_max_ram,
    :hot_cache_max_ram,
    :hot_cache_min_ram,
    last_pressure_level: :ok,
    last_hot_cache_budget: nil,
    keydir_pressure_level: :ok
  ]

  @doc "Starts the MemoryGuard GenServer."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the current memory usage stats for all shards."
  @spec stats() :: map()
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc "Returns the current eviction policy."
  @spec eviction_policy() :: atom()
  def eviction_policy do
    GenServer.call(__MODULE__, :eviction_policy)
  end

  @doc """
  Returns true if memory pressure is at or above the reject threshold.

  Reads from `:persistent_term` (~5ns) instead of GenServer.call (~1-5us).
  The value is updated by `perform_check/1` every 100ms.
  """
  @spec reject_writes?() :: boolean()
  def reject_writes? do
    ref = :persistent_term.get(:ferricstore_pressure_flags)
    :atomics.get(ref, 2) == 1
  end

  @doc """
  Returns true if keydir memory usage is at or above 95% of `keydir_max_ram`.

  Reads from `:persistent_term` (~5ns) instead of GenServer.call (~1-5us).
  The value is updated by `perform_check/1` every 100ms. The staleness
  window (100ms) is acceptable since memory pressure changes slowly.

  When true, new key writes should be rejected, but updates to existing keys
  are still allowed.
  """
  @spec keydir_full?() :: boolean()
  def keydir_full? do
    ref = :persistent_term.get(:ferricstore_pressure_flags)
    :atomics.get(ref, 1) == 1
  end

  @doc """
  Directly sets the keydir_full flag. For use in tests only.
  """
  @spec set_keydir_full(boolean()) :: :ok
  def set_keydir_full(value) do
    ref = :persistent_term.get(:ferricstore_pressure_flags)
    :atomics.put(ref, 1, if(value, do: 1, else: 0))
    :ok
  end

  @doc """
  Directly sets the reject_writes flag. For use in tests only.
  """
  @spec set_reject_writes(boolean()) :: :ok
  def set_reject_writes(value) do
    ref = :persistent_term.get(:ferricstore_pressure_flags)
    :atomics.put(ref, 2, if(value, do: 1, else: 0))
    :ok
  end

  @doc """
  Reconfigures MemoryGuard with new budget parameters.

  Accepts a map with optional keys:
    * `:keydir_max_ram` -- maximum keydir ETS memory in bytes
    * `:hot_cache_max_ram` -- maximum hot_cache ETS memory (or `:auto`)
    * `:hot_cache_min_ram` -- minimum hot_cache budget
    * `:max_memory_bytes` -- total memory budget
    * `:eviction_policy` -- eviction policy atom
  """
  @spec reconfigure(map()) :: :ok
  def reconfigure(params) when is_map(params) do
    GenServer.call(__MODULE__, {:reconfigure, params})
  end

  @doc """
  Forces an immediate memory check cycle.

  Useful in tests to synchronously update pressure levels after changing budgets.
  """
  @spec force_check() :: :ok
  def force_check do
    GenServer.call(__MODULE__, :force_check)
  end

  @doc """
  Triggers an immediate memory check + eviction cycle without blocking the caller.

  Called from the write path when a key is rejected due to memory pressure.
  This kicks off eviction immediately rather than waiting up to 100ms for the
  next periodic check. Uses `cast` so the write path returns the error to the
  client without waiting for the eviction to complete.
  """
  @spec nudge() :: :ok
  def nudge do
    GenServer.cast(__MODULE__, :nudge)
  end

  @impl true
  def init(opts) do
    interval_ms = Keyword.get(opts, :interval_ms, default_interval())
    max_memory_bytes = Keyword.get(opts, :max_memory_bytes, default_max_memory())
    eviction_policy = Keyword.get(opts, :eviction_policy, default_eviction_policy())
    shard_count = Keyword.get(opts, :shard_count, default_shard_count())
    keydir_max_ram = Keyword.get(opts, :keydir_max_ram, default_keydir_max_ram())
    hot_cache_min_ram = Application.get_env(:ferricstore, :hot_cache_min_ram, 0)

    initial_budget = hot_cache_budget(max_memory_bytes, :ok)

    state = %__MODULE__{
      interval_ms: interval_ms,
      max_memory_bytes: max_memory_bytes,
      eviction_policy: eviction_policy,
      shard_count: shard_count,
      keydir_max_ram: keydir_max_ram,
      hot_cache_max_ram: max_memory_bytes - keydir_max_ram,
      hot_cache_min_ram: hot_cache_min_ram,
      last_pressure_level: :ok,
      last_hot_cache_budget: initial_budget,
      keydir_pressure_level: :ok
    }

    schedule_check(interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state), do: {:reply, compute_stats(state), state}
  def handle_call(:eviction_policy, _from, state), do: {:reply, state.eviction_policy, state}
  def handle_call(:reject_writes?, _from, state) do
    {:reply, state.last_pressure_level == :reject and state.eviction_policy == :noeviction, state}
  end

  def handle_call(:keydir_full?, _from, state) do
    {:reply, state.keydir_pressure_level == :reject, state}
  end

  def handle_call({:reconfigure, params}, _from, state) do
    new_state =
      state
      |> maybe_update(:keydir_max_ram, Map.get(params, :keydir_max_ram))
      |> maybe_update(:hot_cache_min_ram, Map.get(params, :hot_cache_min_ram))
      |> maybe_update(:max_memory_bytes, Map.get(params, :max_memory_bytes))
      |> maybe_update(:eviction_policy, Map.get(params, :eviction_policy))

    new_state =
      case Map.get(params, :hot_cache_max_ram) do
        nil -> %{new_state | hot_cache_max_ram: new_state.max_memory_bytes - new_state.keydir_max_ram}
        :auto -> %{new_state | hot_cache_max_ram: new_state.max_memory_bytes - new_state.keydir_max_ram}
        val -> %{new_state | hot_cache_max_ram: val}
      end

    {:reply, :ok, new_state}
  end

  def handle_call(:force_check, _from, state) do
    new_state = perform_check(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast(:nudge, state) do
    {:noreply, perform_check(state)}
  end

  @impl true
  def handle_info(:check, state) do
    state = perform_check(state)
    schedule_check(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp perform_check(state) do
    stats = compute_stats(state)
    level = to_spec_level(stats.pressure_level)

    :telemetry.execute(
      [:ferricstore, :memory, :check],
      %{total_bytes: stats.total_bytes},
      %{pressure_level: stats.pressure_level, ratio: stats.ratio, max_bytes: stats.max_bytes}
    )

    emit_spec_pressure_level(stats, level)

    state = emit_hot_cache_budget_events(state, stats, level)

    # Emit keydir-specific pressure telemetry
    if stats.keydir_pressure_level in [:pressure, :reject] do
      :telemetry.execute(
        [:ferricstore, :memory, :keydir_pressure],
        %{
          keydir_bytes: stats.keydir_bytes,
          keydir_max_ram: stats.keydir_max_ram,
          keydir_ratio: stats.keydir_ratio
        },
        %{keydir_pressure_level: stats.keydir_pressure_level}
      )
    end

    case stats.pressure_level do
      :ok ->
        if state.last_pressure_level in [:pressure, :reject] do
          Logger.info("MemoryGuard: memory pressure resolved")
          :telemetry.execute(
            [:ferricstore, :memory, :recovered],
            %{total_bytes: stats.total_bytes, max_bytes: stats.max_bytes, ratio: stats.ratio},
            %{previous_level: state.last_pressure_level}
          )
        end

      :warning ->
        if state.last_pressure_level not in [:warning, :pressure, :reject] do
          Logger.warning("MemoryGuard: memory warning")
        end

      :pressure ->
        Logger.error("MemoryGuard: high memory pressure")
        emit_shard_pressure_events(stats)

      :reject ->
        Logger.critical("MemoryGuard: critical memory")
        emit_shard_pressure_events(stats)
        maybe_evict(state)
    end

    # Publish pressure levels to atomics for lock-free hot-path reads.
    # Callers (Router.check_keydir_full, Shard.put) read these instead of
    # GenServer.call, eliminating the MemoryGuard process as a contention point.
    # Atomics avoid the global GC that persistent_term.put would trigger every 100ms.
    ref = :persistent_term.get(:ferricstore_pressure_flags)
    :atomics.put(ref, 1, if(stats.keydir_pressure_level == :reject, do: 1, else: 0))
    :atomics.put(ref, 2, if(stats.pressure_level == :reject and state.eviction_policy == :noeviction, do: 1, else: 0))

    %{state | last_pressure_level: stats.pressure_level, keydir_pressure_level: stats.keydir_pressure_level}
  end

  # Evicts keys according to the configured eviction policy when memory
  # pressure is at :pressure or :reject level.
  defp maybe_evict(%{eviction_policy: :noeviction}), do: :ok

  defp maybe_evict(%{eviction_policy: policy, shard_count: shard_count}) when policy in [:volatile_lfu, :allkeys_lfu, :volatile_lru, :allkeys_lru, :volatile_ttl] do
    evicted =
      Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
        keydir = :"keydir_#{i}"
        now = System.os_time(:millisecond)

        try do
          # Sample hot entries (value != nil) eligible for eviction.
          # Use {count, list} accumulator to avoid O(n) length/1 on every iteration.
          {_count, eligible} =
            :ets.foldl(fn {key, value, exp, lfu, fid, _off, _vsize}, {count, found} ->
              cond do
                count >= 10 -> {count, found}
                value == nil -> {count, found}  # Already cold, skip
                fid == :pending -> {count, found}  # Background write pending, skip
                policy in [:volatile_lfu, :volatile_lru] and exp > 0 and exp > now -> {count + 1, [{key, exp, lfu} | found]}
                policy == :volatile_ttl and exp > 0 and exp > now -> {count + 1, [{key, exp, lfu} | found]}
                policy in [:allkeys_lfu, :allkeys_lru] -> {count + 1, [{key, exp, lfu} | found]}
                true -> {count, found}
              end
            end, {0, []}, keydir)

          if eligible != [] do
            to_evict =
              case policy do
                :volatile_ttl ->
                  # Sort by TTL ascending (evict shortest TTL first)
                  eligible |> Enum.sort_by(fn {_k, exp, _lfu} -> exp end) |> Enum.take(5)
                p when p in [:volatile_lfu, :allkeys_lfu] ->
                  # Sort by effective (decayed) LFU counter ascending
                  eligible
                  |> Enum.sort_by(fn {_k, _exp, lfu} -> Ferricstore.Store.LFU.effective_counter(lfu) end)
                  |> Enum.take(5)
                _ ->
                  # LRU fallback: take first 5 from sample
                  Enum.take(eligible, 5)
              end

            # Eviction = set value to nil. Key stays in keydir, data stays on disk.
            # Next GET: keydir hit with nil value -> fall through to Bitcask -> re-warm.
            evict_count =
              Enum.reduce(to_evict, 0, fn {key, _exp, _lfu}, cnt ->
                :ets.update_element(keydir, key, {2, nil})
                cnt + 1
              end)

            acc + evict_count
          else
            acc
          end
        rescue _ -> acc
        catch _, _ -> acc
        end
      end)

    if evicted > 0 do
      Ferricstore.Stats.incr_evicted_keys(evicted)
    end

    :ok
  end

  defp maybe_evict(_state), do: :ok

  defp compute_stats(state) do
    {keydir_bytes, shard_stats} =
      Enum.reduce(0..(state.shard_count - 1), {0, %{}}, fn i, {kd_acc, shards_acc} ->
        kd_bytes = safe_ets_memory(:"keydir_#{i}")
        per_shard_max = div(state.max_memory_bytes, max(state.shard_count, 1))
        ratio =
          cond do
            per_shard_max > 0 -> kd_bytes / per_shard_max
            kd_bytes > 0 -> 1.0
            true -> 0.0
          end
        {kd_acc + kd_bytes, Map.put(shards_acc, i, %{bytes: kd_bytes, ratio: ratio})}
      end)

    total_bytes = keydir_bytes
    ratio = if state.max_memory_bytes > 0, do: total_bytes / state.max_memory_bytes, else: 0.0
    pressure_level = classify_pressure(ratio)

    keydir_ratio = if state.keydir_max_ram > 0, do: keydir_bytes / state.keydir_max_ram, else: 0.0
    keydir_pressure_level = classify_pressure(keydir_ratio)

    %{
      total_bytes: total_bytes, max_bytes: state.max_memory_bytes,
      ratio: ratio, pressure_level: pressure_level,
      shards: shard_stats, eviction_policy: state.eviction_policy,
      keydir_bytes: keydir_bytes,
      hot_cache_bytes: 0,
      keydir_max_ram: state.keydir_max_ram,
      hot_cache_max_ram: state.hot_cache_max_ram,
      hot_cache_min_ram: state.hot_cache_min_ram,
      keydir_pressure_level: keydir_pressure_level,
      keydir_ratio: keydir_ratio
    }
  end

  defp safe_ets_memory(table_name) do
    case :ets.info(table_name, :memory) do
      :undefined -> 0
      memory when is_integer(memory) -> memory * :erlang.system_info(:wordsize)
      _ -> 0
    end
  rescue _ -> 0
  catch _, _ -> 0
  end

  defp classify_pressure(ratio) when ratio >= @reject_threshold, do: :reject
  defp classify_pressure(ratio) when ratio >= @pressure_threshold, do: :pressure
  defp classify_pressure(ratio) when ratio >= @warning_threshold, do: :warning
  defp classify_pressure(_ratio), do: :ok

  defp emit_spec_pressure_level(stats, level) do
    :telemetry.execute(
      [:ferricstore, :memory, :pressure],
      %{total_bytes: stats.total_bytes, max_bytes: stats.max_bytes, ratio: stats.ratio},
      %{level: level}
    )
  end

  defp emit_hot_cache_budget_events(state, stats, level) do
    new_budget = hot_cache_budget(stats.max_bytes, level)
    old_budget = state.last_hot_cache_budget

    cond do
      old_budget != nil and new_budget < old_budget ->
        :telemetry.execute(
          [:ferricstore, :hot_cache, :limit_reduced],
          %{new_budget_bytes: new_budget, old_budget_bytes: old_budget},
          %{level: level, shard_count: state.shard_count}
        )
        %{state | last_hot_cache_budget: new_budget}

      old_budget != nil and new_budget > old_budget ->
        :telemetry.execute(
          [:ferricstore, :hot_cache, :limit_restored],
          %{new_budget_bytes: new_budget, old_budget_bytes: old_budget},
          %{level: level, shard_count: state.shard_count}
        )
        %{state | last_hot_cache_budget: new_budget}

      true ->
        %{state | last_hot_cache_budget: new_budget}
    end
  end

  defp emit_shard_pressure_events(stats) do
    Enum.each(stats.shards, fn {index, shard_stat} ->
      if shard_stat.ratio >= @pressure_threshold do
        per_shard_max = div(stats.max_bytes, max(map_size(stats.shards), 1))
        :telemetry.execute(
          [:ferricstore, :memory, :pressure],
          %{shard_index: index, bytes: shard_stat.bytes, max_bytes: per_shard_max, ratio: shard_stat.ratio},
          %{pressure_level: stats.pressure_level, eviction_policy: stats.eviction_policy}
        )
      end
    end)
  end

  defp to_spec_level(:reject), do: :full
  defp to_spec_level(:pressure), do: :pressure
  defp to_spec_level(:warning), do: :warn
  defp to_spec_level(:ok), do: :ok

  defp hot_cache_budget(max_memory_bytes, :ok), do: div(max_memory_bytes * 50, 100)
  defp hot_cache_budget(max_memory_bytes, :warn), do: div(max_memory_bytes * 30, 100)
  defp hot_cache_budget(max_memory_bytes, :pressure), do: div(max_memory_bytes * 15, 100)
  defp hot_cache_budget(max_memory_bytes, :full), do: div(max_memory_bytes * 5, 100)

  defp schedule_check(interval_ms), do: Process.send_after(self(), :check, interval_ms)
  defp default_interval, do: Application.get_env(:ferricstore, :memory_guard_interval_ms, @check_interval_ms)
  defp default_max_memory, do: Application.get_env(:ferricstore, :max_memory_bytes, default_system_memory())

  defp default_system_memory do
    try do
      data = apply(:memsup, :get_system_memory_data, [])
      case data do
        list when is_list(list) -> trunc(Keyword.get(list, :total_memory, 1_073_741_824) * 0.75)
        _ -> 1_073_741_824
      end
    rescue _ -> 1_073_741_824
    catch _, _ -> 1_073_741_824
    end
  end

  defp default_eviction_policy, do: Application.get_env(:ferricstore, :eviction_policy, :volatile_lru)
  defp default_shard_count, do: Application.get_env(:ferricstore, :shard_count, 4)
  defp default_keydir_max_ram, do: Application.get_env(:ferricstore, :keydir_max_ram, 256 * 1024 * 1024)

  defp maybe_update(state, _key, nil), do: state
  defp maybe_update(state, key, value), do: Map.put(state, key, value)

  defp format_ratio(ratio), do: "#{Float.round(ratio * 100, 1)}%"
  defp format_bytes(bytes) when bytes >= 1_073_741_824, do: "#{Float.round(bytes / 1_073_741_824, 2)} GB"
  defp format_bytes(bytes) when bytes >= 1_048_576, do: "#{Float.round(bytes / 1_048_576, 2)} MB"
  defp format_bytes(bytes) when bytes >= 1_024, do: "#{Float.round(bytes / 1_024, 2)} KB"
  defp format_bytes(bytes), do: "#{bytes} B"
end
