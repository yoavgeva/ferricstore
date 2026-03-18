defmodule Ferricstore.SlowLog do
  @moduledoc """
  ETS-backed ring buffer that records commands whose execution time exceeds a
  configurable threshold.

  Mirrors the Redis SLOWLOG facility: each entry captures a monotonically
  increasing ID, a Unix timestamp (microseconds), the wall-clock duration
  (microseconds), and the command with its arguments.

  ## Configuration (application env)

    * `:slowlog_log_slower_than_us` -- threshold in microseconds; commands
      taking longer than this are logged. Default: `10_000` (10 ms).
      Set to `0` to log every command, or `-1` to disable.
    * `:slowlog_max_len` -- maximum number of entries kept in the ring buffer.
      Default: `128`. When full, the oldest entry is evicted.

  ## Ownership

  This module is a GenServer that owns the ETS table
  `:ferricstore_slowlog`. It must be started in the application supervision
  tree before any command dispatch can call `maybe_log/3`.
  """

  use GenServer

  @table :ferricstore_slowlog
  @default_threshold_us 10_000
  @default_max_len 128

  # -------------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------------

  @typedoc "A single slow log entry."
  @type entry :: {id :: non_neg_integer(), timestamp_us :: integer(), duration_us :: non_neg_integer(), command :: [binary()]}

  # -------------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------------

  @doc """
  Starts the SlowLog GenServer and creates the backing ETS table.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Records a command if its duration exceeds the configured threshold.

  This function is designed to be called from the hot dispatch path.
  When the threshold is `-1` (disabled), this is a no-op.

  ## Parameters

    - `command` -- list of binaries, e.g. `["SET", "key", "value"]`
    - `duration_us` -- execution time in microseconds
    - `_metadata` -- reserved for future use (client address, etc.)
  """
  @spec maybe_log([binary()], non_neg_integer(), term()) :: :ok
  def maybe_log(command, duration_us, _metadata \\ nil) do
    threshold = threshold()

    if threshold >= 0 and duration_us > threshold do
      GenServer.cast(__MODULE__, {:log, command, duration_us})
    end

    :ok
  end

  @doc """
  Returns the last `count` slow log entries, newest first.

  When `count` is `nil` or omitted, returns all entries up to `max_len`.
  """
  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ nil) do
    entries =
      @table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {id, _, _, _} -> id end, :desc)

    case count do
      nil -> entries
      n when is_integer(n) and n >= 0 -> Enum.take(entries, n)
    end
  end

  @doc """
  Returns the number of entries currently in the slow log.
  """
  @spec len() :: non_neg_integer()
  def len do
    :ets.info(@table, :size)
  end

  @doc """
  Clears all entries from the slow log and resets the ID counter.
  """
  @spec reset() :: :ok
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @doc """
  Returns the configured threshold in microseconds.

  A value of `-1` means slow logging is disabled.
  """
  @spec threshold() :: integer()
  def threshold do
    Application.get_env(:ferricstore, :slowlog_log_slower_than_us, @default_threshold_us)
  end

  @doc """
  Returns the configured maximum number of entries.
  """
  @spec max_len() :: pos_integer()
  def max_len do
    Application.get_env(:ferricstore, :slowlog_max_len, @default_max_len)
  end

  # -------------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:set, :public, :named_table])
    {:ok, %{table: table, next_id: 0}}
  end

  @impl true
  def handle_cast({:log, command, duration_us}, state) do
    id = state.next_id
    timestamp_us = System.os_time(:microsecond)
    :ets.insert(@table, {id, timestamp_us, duration_us, command})

    # Evict oldest entries if we exceed max_len.
    state = %{state | next_id: id + 1}
    evict_if_needed(state)
    check_near_full(state)
    {:noreply, state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table)
    {:reply, :ok, %{state | next_id: 0}}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

  # -------------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------------

  defp evict_if_needed(_state) do
    max = max_len()
    size = :ets.info(@table, :size)

    if size > max do
      # Find and delete the oldest (lowest ID) entries.
      to_remove = size - max

      @table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {id, _, _, _} -> id end)
      |> Enum.take(to_remove)
      |> Enum.each(fn {id, _, _, _} -> :ets.delete(@table, id) end)
    end
  end

  @near_full_threshold 0.90

  # Emits a telemetry event when the slowlog ring buffer is at or above 90%
  # of its capacity. Only fires after eviction has run, so `size` reflects
  # the post-eviction count.
  defp check_near_full(_state) do
    max = max_len()
    size = :ets.info(@table, :size)

    if max > 0 and size / max >= @near_full_threshold do
      :telemetry.execute(
        [:ferricstore, :slow_log, :near_full],
        %{size: size, max: max, ratio: size / max},
        %{}
      )
    end
  end
end
