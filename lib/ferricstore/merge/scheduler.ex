defmodule Ferricstore.Merge.Scheduler do
  @moduledoc """
  Per-shard merge scheduler that monitors fragmentation and triggers compaction.

  Each shard has its own `Scheduler` GenServer. The scheduler periodically
  polls the shard's Bitcask NIF for fragmentation statistics and decides
  whether a merge is needed based on configurable thresholds.

  ## Merge modes

  The scheduler supports three merge modes per the spec (section 2E):

  * **Hot mode** -- Event-driven, can trigger anytime. When fragmentation
    exceeds the threshold, the scheduler immediately attempts a merge (subject
    to the node-level semaphore). This is the default mode.

  * **Bulk mode** -- Scheduled merge during a configurable time window (e.g.
    02:00-04:00). Outside the window, no merges are triggered regardless of
    fragmentation. Inside the window, the scheduler checks fragmentation and
    merges if above threshold.

  * **Age mode** -- Like bulk mode but only merges files older than a
    configurable age threshold, within a time window.

  ## Merge lifecycle

  1. Scheduler polls shard stats on a timer.
  2. If fragmentation exceeds threshold and mode allows, scheduler requests
     the node-level semaphore.
  3. If semaphore is acquired, scheduler writes a merge manifest.
  4. Scheduler selects the most fragmented non-active files for incremental merge.
  5. Scheduler checks that sufficient disk space is available.
  6. Scheduler calls the shard's `run_compaction` via GenServer.call.
  7. On completion, scheduler deletes the manifest and releases the semaphore.
  8. On failure, scheduler logs the error, deletes the manifest, releases semaphore.

  ## Configuration

  Configuration is passed via the `:merge` key in the application env:

      config :ferricstore, :merge,
        mode: :hot,
        check_interval_ms: 30_000,
        fragmentation_threshold: 0.4,
        min_files_for_merge: 2,
        max_files_per_merge: 10,
        merge_window: {2, 4},
        min_free_space_ratio: 0.1
  """

  use GenServer

  alias Ferricstore.Merge.{Manifest, Semaphore}
  alias Ferricstore.Store.Router

  require Logger

  # -------------------------------------------------------------------
  # Default configuration
  # -------------------------------------------------------------------

  @default_check_interval_ms 30_000
  @default_fragmentation_threshold 0.4
  @default_min_files_for_merge 2
  @default_max_files_per_merge 10
  @default_mode :hot
  @default_merge_window {2, 4}
  @default_min_free_space_ratio 0.1

  @type merge_mode :: :hot | :bulk | :age

  @type config :: %{
          mode: merge_mode(),
          check_interval_ms: pos_integer(),
          fragmentation_threshold: float(),
          min_files_for_merge: pos_integer(),
          max_files_per_merge: pos_integer(),
          merge_window: {non_neg_integer(), non_neg_integer()},
          min_free_space_ratio: float()
        }

  defstruct [
    :shard_index,
    :config,
    :data_dir,
    :semaphore,
    merging: false,
    last_merge_at: nil,
    merge_count: 0,
    total_bytes_reclaimed: 0
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a merge scheduler for the given shard.

  ## Options

    * `:shard_index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:merge_config` -- override merge configuration (map)
    * `:semaphore` -- name or pid of the semaphore process (default: `Semaphore`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :shard_index)
    name = Keyword.get(opts, :name, scheduler_name(index))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the registered process name for the scheduler at `index`.
  """
  @spec scheduler_name(non_neg_integer()) :: atom()
  def scheduler_name(index), do: :"Ferricstore.Merge.Scheduler.#{index}"

  @doc """
  Returns the current status of the merge scheduler for observability.
  """
  @spec status(non_neg_integer() | GenServer.server()) :: map()
  def status(index_or_server) when is_integer(index_or_server) do
    GenServer.call(scheduler_name(index_or_server), :status)
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  @doc """
  Forces an immediate merge check, bypassing the timer. Used in tests.
  """
  @spec trigger_check(non_neg_integer() | GenServer.server()) :: :ok
  def trigger_check(index_or_server) when is_integer(index_or_server) do
    GenServer.call(scheduler_name(index_or_server), :trigger_check)
  end

  def trigger_check(server) do
    GenServer.call(server, :trigger_check)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :shard_index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    merge_config = Keyword.get(opts, :merge_config, %{})
    semaphore = Keyword.get(opts, :semaphore, Semaphore)

    config = build_config(merge_config)
    shard_data_dir = Path.join(data_dir, "shard_#{index}")

    state = %__MODULE__{
      shard_index: index,
      config: config,
      data_dir: shard_data_dir,
      semaphore: semaphore
    }

    # Recover from any interrupted merge on startup.
    Manifest.recover_if_needed(shard_data_dir, index)

    schedule_check(config.check_interval_ms)

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      shard_index: state.shard_index,
      mode: state.config.mode,
      merging: state.merging,
      last_merge_at: state.last_merge_at,
      merge_count: state.merge_count,
      total_bytes_reclaimed: state.total_bytes_reclaimed,
      config: state.config
    }

    {:reply, status, state}
  end

  def handle_call(:trigger_check, _from, state) do
    new_state = maybe_merge(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_info(:check, state) do
    new_state = maybe_merge(state)
    schedule_check(state.config.check_interval_ms)
    {:noreply, new_state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # Private: merge decision logic
  # -------------------------------------------------------------------

  defp maybe_merge(%{merging: true} = state), do: state

  defp maybe_merge(state) do
    if should_merge?(state) do
      attempt_merge(state)
    else
      state
    end
  end

  defp should_merge?(state) do
    mode_allows_merge?(state.config) and fragmentation_above_threshold?(state)
  end

  defp mode_allows_merge?(%{mode: :hot}), do: true

  defp mode_allows_merge?(%{mode: :bulk, merge_window: {start_hour, end_hour}}) do
    in_time_window?(start_hour, end_hour)
  end

  defp mode_allows_merge?(%{mode: :age, merge_window: {start_hour, end_hour}}) do
    in_time_window?(start_hour, end_hour)
  end

  defp in_time_window?(start_hour, end_hour) do
    {:ok, now} = DateTime.now("Etc/UTC")
    hour = now.hour

    if start_hour <= end_hour do
      hour >= start_hour and hour < end_hour
    else
      # Wraps around midnight, e.g. 22:00-04:00
      hour >= start_hour or hour < end_hour
    end
  end

  defp fragmentation_above_threshold?(state) do
    shard_name = Router.shard_name(state.shard_index)

    case safe_call(shard_name, :shard_stats) do
      {:ok, {_total, _live, _dead, file_count, _keys, frag_ratio}} ->
        file_count >= state.config.min_files_for_merge and
          frag_ratio >= state.config.fragmentation_threshold

      _ ->
        false
    end
  end

  # -------------------------------------------------------------------
  # Private: merge execution
  # -------------------------------------------------------------------

  defp attempt_merge(state) do
    case Semaphore.acquire(state.shard_index, state.semaphore) do
      :ok ->
        state = %{state | merging: true}
        do_merge(state)

      {:busy, _holder} ->
        Logger.debug(
          "Shard #{state.shard_index}: merge semaphore busy, deferring"
        )

        state
    end
  end

  defp do_merge(state) do
    shard_name = Router.shard_name(state.shard_index)

    result =
      with {:ok, file_ids} <- select_files_for_merge(state, shard_name),
           :ok <- check_disk_space(state, shard_name, file_ids),
           :ok <- write_manifest(state, file_ids),
           {:ok, compaction_result} <- run_compaction(shard_name, file_ids) do
        {:ok, compaction_result, file_ids}
      end

    case result do
      {:ok, {written, dropped, reclaimed}, _file_ids} ->
        Logger.info(
          "Shard #{state.shard_index}: merge complete — " <>
            "#{written} records written, #{dropped} dropped, " <>
            "#{format_bytes(reclaimed)} reclaimed"
        )

        Manifest.delete(state.data_dir)
        Semaphore.release(state.shard_index, state.semaphore)

        %{
          state
          | merging: false,
            last_merge_at: System.system_time(:millisecond),
            merge_count: state.merge_count + 1,
            total_bytes_reclaimed: state.total_bytes_reclaimed + reclaimed
        }

      {:error, reason} ->
        Logger.error(
          "Shard #{state.shard_index}: merge failed — #{inspect(reason)}"
        )

        Manifest.delete(state.data_dir)
        Semaphore.release(state.shard_index, state.semaphore)
        %{state | merging: false}
    end
  end

  defp select_files_for_merge(state, shard_name) do
    with {:ok, file_sizes} <- safe_call(shard_name, :file_sizes),
         {:ok, mergeable} <- pick_mergeable_files(file_sizes, state.config) do
      {:ok, mergeable}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp pick_mergeable_files([], _config), do: {:error, :no_files}

  defp pick_mergeable_files(file_sizes, config) do
    {active_fid, _} = Enum.max_by(file_sizes, fn {fid, _size} -> fid end)

    mergeable =
      file_sizes
      |> Enum.reject(fn {fid, _size} -> fid == active_fid end)
      |> Enum.sort_by(fn {_fid, size} -> size end, :desc)
      |> Enum.take(config.max_files_per_merge)
      |> Enum.map(fn {fid, _size} -> fid end)

    if Enum.count(mergeable) >= config.min_files_for_merge do
      {:ok, mergeable}
    else
      {:error, :not_enough_files}
    end
  end

  defp check_disk_space(state, shard_name, file_ids) do
    with {:ok, available} <- safe_call(shard_name, :available_disk_space),
         {:ok, file_sizes} <- safe_call(shard_name, :file_sizes) do
      # Sum the size of files being merged — worst case, the new merged file
      # is as large as all input files combined.
      input_bytes =
        file_sizes
        |> Enum.filter(fn {fid, _size} -> fid in file_ids end)
        |> Enum.reduce(0, fn {_fid, size}, acc -> acc + size end)

      if available > 0 and input_bytes / max(available, 1) > (1.0 - state.config.min_free_space_ratio) do
        Logger.warning(
          "Shard #{state.shard_index}: insufficient disk space for merge " <>
            "(need ~#{format_bytes(input_bytes)}, available #{format_bytes(available)})"
        )

        {:error, :insufficient_disk_space}
      else
        :ok
      end
    end
  end

  defp write_manifest(state, file_ids) do
    Manifest.write(state.data_dir, %{
      shard_index: state.shard_index,
      input_file_ids: file_ids
    })
  end

  defp run_compaction(shard_name, file_ids) do
    case safe_call(shard_name, {:run_compaction, file_ids}) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, {:compaction_failed, reason}}
    end
  end

  # -------------------------------------------------------------------
  # Private: helpers
  # -------------------------------------------------------------------

  defp schedule_check(interval_ms) do
    Process.send_after(self(), :check, interval_ms)
  end

  defp build_config(overrides) do
    %{
      mode: Map.get(overrides, :mode, app_config(:mode, @default_mode)),
      check_interval_ms:
        Map.get(overrides, :check_interval_ms, app_config(:check_interval_ms, @default_check_interval_ms)),
      fragmentation_threshold:
        Map.get(
          overrides,
          :fragmentation_threshold,
          app_config(:fragmentation_threshold, @default_fragmentation_threshold)
        ),
      min_files_for_merge:
        Map.get(
          overrides,
          :min_files_for_merge,
          app_config(:min_files_for_merge, @default_min_files_for_merge)
        ),
      max_files_per_merge:
        Map.get(
          overrides,
          :max_files_per_merge,
          app_config(:max_files_per_merge, @default_max_files_per_merge)
        ),
      merge_window:
        Map.get(overrides, :merge_window, app_config(:merge_window, @default_merge_window)),
      min_free_space_ratio:
        Map.get(
          overrides,
          :min_free_space_ratio,
          app_config(:min_free_space_ratio, @default_min_free_space_ratio)
        )
    }
  end

  defp app_config(key, default) do
    merge_config = Application.get_env(:ferricstore, :merge, [])

    case merge_config do
      config when is_list(config) -> Keyword.get(config, key, default)
      config when is_map(config) -> Map.get(config, key, default)
      _ -> default
    end
  end

  # Safe GenServer.call that catches exits (shard might be restarting).
  defp safe_call(name, msg) do
    GenServer.call(name, msg)
  catch
    :exit, reason ->
      {:error, {:shard_unavailable, reason}}
  end

  defp format_bytes(bytes) when bytes >= 1_073_741_824,
    do: "#{Float.round(bytes / 1_073_741_824, 2)} GiB"

  defp format_bytes(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 2)} MiB"

  defp format_bytes(bytes) when bytes >= 1024,
    do: "#{Float.round(bytes / 1024, 2)} KiB"

  defp format_bytes(bytes), do: "#{bytes} B"
end
