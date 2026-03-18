defmodule Ferricstore.Stats do
  @moduledoc """
  Tracks global server statistics using `:counters` for lock-free atomic increments.

  Maintains the following counters:

    * `total_connections_received` — number of TCP connections accepted since startup
    * `total_commands_processed` — number of commands dispatched since startup

  Also stores the server start time and a random run ID (hex string) generated at
  startup.

  ## Architecture

  Uses a single `:counters` reference with two slots for the hot-path counters.
  The start time and run ID are stored via `:persistent_term` for zero-cost reads
  from any process.

  ## Usage

      Ferricstore.Stats.incr_connections()
      Ferricstore.Stats.incr_commands()
      Ferricstore.Stats.total_connections()
      Ferricstore.Stats.total_commands()
      Ferricstore.Stats.uptime_seconds()
      Ferricstore.Stats.run_id()
  """

  use GenServer

  @counter_connections 1
  @counter_commands 2

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc "Starts the Stats process and initialises counters."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Increments the total connections received counter by 1."
  @spec incr_connections() :: :ok
  def incr_connections do
    :counters.add(counter_ref(), @counter_connections, 1)
    :ok
  end

  @doc "Increments the total commands processed counter by 1."
  @spec incr_commands() :: :ok
  def incr_commands do
    :counters.add(counter_ref(), @counter_commands, 1)
    :ok
  end

  @doc "Returns the total number of connections received since startup."
  @spec total_connections() :: non_neg_integer()
  def total_connections do
    :counters.get(counter_ref(), @counter_connections)
  end

  @doc "Returns the total number of commands processed since startup."
  @spec total_commands() :: non_neg_integer()
  def total_commands do
    :counters.get(counter_ref(), @counter_commands)
  end

  @doc "Returns the server uptime in seconds."
  @spec uptime_seconds() :: non_neg_integer()
  def uptime_seconds do
    start = :persistent_term.get({__MODULE__, :start_time})
    div(System.monotonic_time(:millisecond) - start, 1000)
  end

  @doc "Returns the random hex run ID generated at startup."
  @spec run_id() :: binary()
  def run_id do
    :persistent_term.get({__MODULE__, :run_id})
  end

  @doc "Returns the server start time as a monotonic millisecond timestamp."
  @spec start_time() :: integer()
  def start_time do
    :persistent_term.get({__MODULE__, :start_time})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    ref = :counters.new(2, [:atomics])
    run_id = :crypto.strong_rand_bytes(20) |> Base.encode16(case: :lower)
    start_time = System.monotonic_time(:millisecond)

    :persistent_term.put({__MODULE__, :counter_ref}, ref)
    :persistent_term.put({__MODULE__, :run_id}, run_id)
    :persistent_term.put({__MODULE__, :start_time}, start_time)

    {:ok, %{}}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp counter_ref do
    :persistent_term.get({__MODULE__, :counter_ref})
  end
end
