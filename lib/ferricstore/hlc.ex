defmodule Ferricstore.HLC do
  @moduledoc """
  Hybrid Logical Clock (HLC) for FerricStore.

  An HLC combines a physical wall-clock component (milliseconds since epoch)
  with a logical counter to produce timestamps that are:

    1. **Monotonically increasing** — even when the wall clock is not (NTP
       corrections, VM migration, etc.).
    2. **Causally ordered** — merging a remote timestamp via `update/2`
       ensures happens-before relationships are preserved across nodes.
    3. **Close to real time** — the physical component tracks
       `System.os_time(:millisecond)` and only diverges when the wall clock
       jumps backward or a remote node is ahead.

  ## Spec reference (2G.6)

  * HLC is piggybacked on Raft heartbeats.
  * Inter-node TTL precision is bounded to Raft heartbeat RTT (~10 ms).
  * A telemetry warning is emitted at 500 ms drift between HLC physical and
    wall clock.
  * TTL-sensitive reads are rejected at 1 000 ms drift.
  * The HLC millisecond component is used for Stream ID generation (XADD `*`).
  * HLC timestamps are stamped on commands **before** they enter Raft — they
    are not computed inside `apply()`.

  ## Usage

  The application supervision tree starts a single named HLC process
  (`Ferricstore.HLC`). Other modules obtain timestamps via:

      {physical_ms, logical} = Ferricstore.HLC.now()
      ms = Ferricstore.HLC.now_ms()

  When receiving a Raft heartbeat with a piggybacked timestamp:

      :ok = Ferricstore.HLC.update(remote_timestamp)

  ## Timestamp representation

  A timestamp is a 2-tuple `{physical_ms, logical}` where:

    * `physical_ms` — milliseconds since Unix epoch (same scale as
      `System.os_time(:millisecond)`)
    * `logical` — a non-negative integer counter that disambiguates events
      within the same physical millisecond

  Timestamps are ordered lexicographically: physical first, then logical.
  """

  use GenServer

  require Logger

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "An HLC timestamp: `{physical_ms, logical_counter}`."
  @type timestamp :: {non_neg_integer(), non_neg_integer()}

  # Drift thresholds (milliseconds).
  @drift_warning_ms 500
  @drift_reject_ms 1_000

  # ---------------------------------------------------------------------------
  # Client API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the HLC GenServer.

  ## Options

    * `:name` — process name (default: `Ferricstore.HLC`)

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, [], name: name)
  end

  @doc """
  Returns the current HLC timestamp as `{physical_ms, logical}`.

  Each call is guaranteed to return a timestamp strictly greater than the
  previous one (monotonicity). If the wall clock has not advanced since the
  last call, the logical counter is incremented.

  When drift between the HLC physical component and the wall clock exceeds
  500 ms, a `[:ferricstore, :hlc, :drift_warning]` telemetry event is emitted.

  ## Parameters

    * `server` — the HLC process (default: `Ferricstore.HLC`)

  ## Examples

      iex> {phys, logical} = Ferricstore.HLC.now()
      iex> phys > 0
      true
      iex> logical >= 0
      true

  """
  @spec now(GenServer.server()) :: timestamp()
  def now(server \\ __MODULE__) do
    GenServer.call(server, :now)
  catch
    :exit, {:noproc, _} ->
      # Graceful fallback when the HLC GenServer is not running (e.g. in
      # unit tests that exercise command modules without the full application).
      # In this case the HLC degenerates to a plain wall-clock reading.
      {System.os_time(:millisecond), 0}
  end

  @doc """
  Convenience: returns only the physical (millisecond) component of `now/1`.

  This is the value used as the millisecond part of Redis Stream IDs and as
  the base for TTL computations. Falls back to `System.os_time(:millisecond)`
  when the HLC GenServer is not running.

  ## Parameters

    * `server` — the HLC process (default: `Ferricstore.HLC`)

  """
  @spec now_ms(GenServer.server()) :: non_neg_integer()
  def now_ms(server \\ __MODULE__) do
    {physical, _logical} = now(server)
    physical
  end

  @doc """
  Merges a remote HLC timestamp received from another node (e.g. via a Raft
  heartbeat).

  The merge rule follows the standard HLC algorithm:

    1. `new_physical = max(wall_clock, local_physical, remote_physical)`
    2. If all three physical values tie, `logical = max(local_logical,
       remote_logical) + 1`.
    3. If two tie at the max, the logical from the winner is incremented.
    4. If wall clock alone wins, logical resets to 0.

  ## Parameters

    * `server` — the HLC process (default: `Ferricstore.HLC`)
    * `remote_ts` — the remote HLC timestamp `{physical_ms, logical}`

  """
  @spec update(GenServer.server(), timestamp()) :: :ok
  def update(server \\ __MODULE__, remote_ts) do
    GenServer.call(server, {:update, remote_ts})
  end

  @doc """
  Returns the absolute drift in milliseconds between the HLC physical
  component and the current wall clock.

  Under normal single-node operation this is 0 or near-0. A non-zero drift
  indicates that `update/2` received a future timestamp from a remote node
  or the wall clock jumped backward.

  ## Parameters

    * `server` — the HLC process (default: `Ferricstore.HLC`)

  """
  @spec drift_ms(GenServer.server()) :: non_neg_integer()
  def drift_ms(server \\ __MODULE__) do
    GenServer.call(server, :drift_ms)
  end

  @doc """
  Returns `true` when drift exceeds the reject threshold (1 000 ms), meaning
  TTL-sensitive reads should not be served.

  ## Parameters

    * `server` — the HLC process (default: `Ferricstore.HLC`)

  """
  @spec drift_exceeded?(GenServer.server()) :: boolean()
  def drift_exceeded?(server \\ __MODULE__) do
    drift_ms(server) >= @drift_reject_ms
  end

  @doc """
  Compares two HLC timestamps.

  Returns `:lt`, `:eq`, or `:gt` following the same convention as
  `DateTime.compare/2`.

  ## Examples

      iex> Ferricstore.HLC.compare({100, 0}, {200, 0})
      :lt
      iex> Ferricstore.HLC.compare({100, 1}, {100, 0})
      :gt
      iex> Ferricstore.HLC.compare({100, 0}, {100, 0})
      :eq

  """
  @spec compare(timestamp(), timestamp()) :: :lt | :eq | :gt
  def compare({p1, l1}, {p2, l2}) do
    cond do
      p1 > p2 -> :gt
      p1 < p2 -> :lt
      l1 > l2 -> :gt
      l1 < l2 -> :lt
      true -> :eq
    end
  end

  @doc """
  Extracts the millisecond (physical) component from an HLC timestamp.

  This is used when a caller needs a plain integer millisecond value, for
  example as the ms part of a Redis Stream ID.

  ## Examples

      iex> Ferricstore.HLC.encode_ms({1_234_567_890, 42})
      1_234_567_890

  """
  @spec encode_ms(timestamp()) :: non_neg_integer()
  def encode_ms({physical_ms, _logical}), do: physical_ms

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init([]) do
    {:ok, %{physical: 0, logical: 0}}
  end

  @impl true
  def handle_call(:now, _from, state) do
    wall = System.os_time(:millisecond)

    {new_physical, new_logical} =
      cond do
        wall > state.physical ->
          # Wall clock advanced — reset logical.
          {wall, 0}

        wall == state.physical ->
          # Same millisecond — increment logical.
          {state.physical, state.logical + 1}

        true ->
          # Wall clock went backward — keep our physical, bump logical.
          {state.physical, state.logical + 1}
      end

    ts = {new_physical, new_logical}
    new_state = %{state | physical: new_physical, logical: new_logical}

    # Check drift and emit telemetry if warranted.
    maybe_emit_drift_warning(new_physical, wall)

    {:reply, ts, new_state}
  end

  def handle_call({:update, {remote_phys, remote_log}}, _from, state) do
    wall = System.os_time(:millisecond)

    {new_physical, new_logical} =
      cond do
        # Wall clock is strictly ahead of both local and remote.
        wall > state.physical and wall > remote_phys ->
          {wall, 0}

        # Local physical is ahead of both wall clock and remote.
        state.physical > wall and state.physical > remote_phys ->
          {state.physical, state.logical + 1}

        # Remote physical is ahead of both wall clock and local.
        remote_phys > wall and remote_phys > state.physical ->
          {remote_phys, remote_log + 1}

        # Local and remote tie, both ahead of or equal to wall clock.
        state.physical == remote_phys ->
          {state.physical, max(state.logical, remote_log) + 1}

        # Wall clock ties with local physical, both >= remote.
        wall == state.physical ->
          {state.physical, state.logical + 1}

        # Wall clock ties with remote physical, both >= local.
        wall == remote_phys ->
          {wall, remote_log + 1}

        # Fallback (shouldn't reach here, but for safety).
        true ->
          new_p = Enum.max([wall, state.physical, remote_phys])
          {new_p, 0}
      end

    new_state = %{state | physical: new_physical, logical: new_logical}
    {:reply, :ok, new_state}
  end

  def handle_call(:drift_ms, _from, state) do
    wall = System.os_time(:millisecond)
    drift = abs(wall - state.physical)
    {:reply, drift, state}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp maybe_emit_drift_warning(hlc_physical, wall_clock) do
    drift = abs(wall_clock - hlc_physical)

    if drift >= @drift_warning_ms do
      :telemetry.execute(
        [:ferricstore, :hlc, :drift_warning],
        %{drift_ms: drift},
        %{hlc_physical: hlc_physical, wall_clock: wall_clock}
      )
    end

    if drift >= @drift_reject_ms do
      Logger.error(
        "HLC drift #{drift}ms exceeds reject threshold (#{@drift_reject_ms}ms); " <>
          "TTL-sensitive reads will be rejected"
      )
    end
  end
end
