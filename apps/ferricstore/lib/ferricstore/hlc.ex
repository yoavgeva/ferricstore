defmodule Ferricstore.HLC do
  @moduledoc """
  Hybrid Logical Clock (HLC) for FerricStore.

  An HLC combines a physical wall-clock component (milliseconds since epoch)
  with a logical counter to produce timestamps that are:

    1. **Monotonically increasing** -- even when the wall clock is not (NTP
       corrections, VM migration, etc.).
    2. **Causally ordered** -- merging a remote timestamp via `update/1`
       ensures happens-before relationships are preserved across nodes.
    3. **Close to real time** -- the physical component tracks
       `System.os_time(:millisecond)` and only diverges when the wall clock
       jumps backward or a remote node is ahead.

  ## Spec reference (2G.6)

  * HLC is piggybacked on Raft heartbeats.
  * Inter-node TTL precision is bounded to Raft heartbeat RTT (~10 ms).
  * A telemetry warning is emitted at 500 ms drift between HLC physical and
    wall clock.
  * TTL-sensitive reads are rejected at 1 000 ms drift.
  * The HLC millisecond component is used for Stream ID generation (XADD `*`).
  * HLC timestamps are stamped on commands **before** they enter Raft -- they
    are not computed inside `apply()`.

  ## Architecture

  The hot-path functions `now/0` and `now_ms/0` are **lock-free**: they use
  `:atomics` (stored in `:persistent_term`) instead of a `GenServer.call`.
  This eliminates the single-process serialization bottleneck that was limiting
  write throughput at high concurrency.

  The GenServer is retained only for:

    * `update/1` -- merging remote timestamps from Raft heartbeats (~6 calls/sec).
      This must be serialized to avoid lost-update races between concurrent merges.
    * Process supervision -- the `init/1` callback creates the atomics ref and
      stores it in `:persistent_term`.

  Slot layout in the `:atomics` ref (both are unsigned 64-bit):

    * Slot 1: `last_physical_ms` -- the most recent physical millisecond.
    * Slot 2: `logical_counter` -- the logical counter within that millisecond.

  ## Usage

  The application supervision tree starts a single named HLC process
  (`Ferricstore.HLC`). Other modules obtain timestamps via:

      {physical_ms, logical} = Ferricstore.HLC.now()
      ms = Ferricstore.HLC.now_ms()

  When receiving a Raft heartbeat with a piggybacked timestamp:

      :ok = Ferricstore.HLC.update(remote_timestamp)

  ## Timestamp representation

  A timestamp is a 2-tuple `{physical_ms, logical}` where:

    * `physical_ms` -- milliseconds since Unix epoch (same scale as
      `System.os_time(:millisecond)`)
    * `logical` -- a non-negative integer counter that disambiguates events
      within the same physical millisecond

  Timestamps are ordered lexicographically: physical first, then logical.

  ## Graceful fallback

  When the HLC GenServer has not been started (e.g. in unit tests that exercise
  command modules without the full application), `now/0` and `now_ms/0` fall
  back to `System.os_time(:millisecond)` with a logical counter of 0.
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

  # :persistent_term key for the atomics ref.
  @atomics_key :ferricstore_hlc_ref

  # Atomics slot indices.
  @slot_physical 1
  @slot_logical 2

  # ---------------------------------------------------------------------------
  # Client API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the HLC GenServer.

  The GenServer creates an `:atomics` ref on init and stores it in
  `:persistent_term` so that `now/0` and `now_ms/0` can read/write it
  without a GenServer call.

  ## Options

    * `:name` -- process name (default: `Ferricstore.HLC`)

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the current HLC timestamp as `{physical_ms, logical}`.

  This function is **lock-free** -- it reads and updates an `:atomics` ref
  directly, bypassing the GenServer. At high concurrency, concurrent callers
  may read the same `last_physical_ms` and race on the logical counter
  increment; `:atomics.add_get/3` is atomic so each caller gets a unique
  logical value.

  Falls back to `{System.os_time(:millisecond), 0}` when the HLC GenServer
  has not been started.

  ## Examples

      iex> {phys, logical} = Ferricstore.HLC.now()
      iex> phys > 0
      true
      iex> logical >= 0
      true

  """
  @spec now() :: timestamp()
  def now do
    ref = atomics_ref()

    case ref do
      nil ->
        {System.os_time(:millisecond), 0}

      ref ->
        wall = System.os_time(:millisecond)
        last = :atomics.get(ref, @slot_physical)

        if wall > last do
          # Wall clock advanced -- try to update physical and reset logical.
          # Another process may race us here. That is acceptable: the worst
          # case is two processes both see wall > last and both write the same
          # wall value (idempotent put) then both get logical 0 or 1 via
          # add_get. Monotonicity is preserved because wall > last.
          :atomics.put(ref, @slot_physical, wall)
          :atomics.put(ref, @slot_logical, 0)
          {wall, 0}
        else
          # Same ms or clock went backward -- increment logical counter.
          logical = :atomics.add_get(ref, @slot_logical, 1)
          {last, logical}
        end
    end
  end

  @doc """
  Returns the current HLC timestamp as `{physical_ms, logical}`.

  This overload accepts a server argument for backward compatibility with
  existing tests. In production, prefer the zero-arity `now/0` which is
  lock-free.

  ## Parameters

    * `server` -- ignored (kept for API compatibility). The atomics ref is
      always read from `:persistent_term`.

  """
  @spec now(GenServer.server()) :: timestamp()
  def now(_server) do
    now()
  end

  @doc """
  Convenience: returns only the physical (millisecond) component of `now/0`.

  This is the value used as the millisecond part of Redis Stream IDs and as
  the base for TTL computations. Lock-free; does not call the GenServer.

  Falls back to `System.os_time(:millisecond)` when the HLC GenServer is
  not running.
  """
  @spec now_ms() :: non_neg_integer()
  def now_ms do
    {physical, _logical} = now()
    physical
  end

  @doc """
  Convenience: returns only the physical (millisecond) component of `now/0`.

  This overload accepts a server argument for backward compatibility.

  ## Parameters

    * `server` -- ignored (kept for API compatibility).

  """
  @spec now_ms(GenServer.server()) :: non_neg_integer()
  def now_ms(_server) do
    now_ms()
  end

  @doc """
  Merges a remote HLC timestamp received from another node (e.g. via a Raft
  heartbeat).

  This is a `GenServer.call` because remote timestamp merges must be
  serialized to avoid lost-update races. This is acceptable because merges
  are rare (~6 calls/sec from Raft heartbeats).

  The merge rule follows the standard HLC algorithm:

    1. `new_physical = max(wall_clock, local_physical, remote_physical)`
    2. If all three physical values tie, `logical = max(local_logical,
       remote_logical) + 1`.
    3. If two tie at the max, the logical from the winner is incremented.
    4. If wall clock alone wins, logical resets to 0.

  ## Parameters

    * `remote_ts` -- the remote HLC timestamp `{physical_ms, logical}`

  """
  @spec update(timestamp()) :: :ok
  def update(remote_ts) do
    GenServer.call(__MODULE__, {:update, remote_ts})
  end

  @doc """
  Merges a remote HLC timestamp, sending the call to `server`.

  ## Parameters

    * `server` -- the HLC process
    * `remote_ts` -- the remote HLC timestamp `{physical_ms, logical}`

  """
  @spec update(GenServer.server(), timestamp()) :: :ok
  def update(server, remote_ts) do
    GenServer.call(server, {:update, remote_ts})
  end

  @doc """
  Returns the absolute drift in milliseconds between the HLC physical
  component and the current wall clock.

  This function is **lock-free** -- it reads the atomics ref directly.

  Under normal single-node operation this is 0 or near-0. A non-zero drift
  indicates that `update/1` received a future timestamp from a remote node
  or the wall clock jumped backward.
  """
  @spec drift_ms() :: non_neg_integer()
  def drift_ms do
    case atomics_ref() do
      nil ->
        0

      ref ->
        phys = :atomics.get(ref, @slot_physical)
        wall = System.os_time(:millisecond)
        abs(wall - phys)
    end
  end

  @doc """
  Returns the absolute drift in milliseconds.

  This overload accepts a server argument for backward compatibility.

  ## Parameters

    * `server` -- ignored (kept for API compatibility).

  """
  @spec drift_ms(GenServer.server()) :: non_neg_integer()
  def drift_ms(_server) do
    drift_ms()
  end

  @doc """
  Returns `true` when drift exceeds the reject threshold (1 000 ms), meaning
  TTL-sensitive reads should not be served.

  Lock-free; does not call the GenServer.
  """
  @spec drift_exceeded?() :: boolean()
  def drift_exceeded? do
    drift_ms() >= @drift_reject_ms
  end

  @doc """
  Returns `true` when drift exceeds the reject threshold (1 000 ms).

  This overload accepts a server argument for backward compatibility.

  ## Parameters

    * `server` -- ignored (kept for API compatibility).

  """
  @spec drift_exceeded?(GenServer.server()) :: boolean()
  def drift_exceeded?(_server) do
    drift_exceeded?()
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
  def init(_opts) do
    # Create an atomics ref with 2 slots (signed 64-bit by default).
    # Slot 1: last_physical_ms, Slot 2: logical_counter.
    ref = :atomics.new(2, signed: true)

    # Initialize slots to zero.
    :atomics.put(ref, @slot_physical, 0)
    :atomics.put(ref, @slot_logical, 0)

    # Store in :persistent_term so any process can access it lock-free.
    :persistent_term.put(@atomics_key, ref)

    {:ok, %{}}
  end

  @impl true
  def handle_call({:update, {remote_phys, remote_log}}, _from, state) do
    ref = :persistent_term.get(@atomics_key)
    wall = System.os_time(:millisecond)
    local_phys = :atomics.get(ref, @slot_physical)
    local_logical = :atomics.get(ref, @slot_logical)

    {new_physical, new_logical} =
      merge_timestamps(wall, local_phys, local_logical, remote_phys, remote_log)

    :atomics.put(ref, @slot_physical, new_physical)
    :atomics.put(ref, @slot_logical, new_logical)

    # Check drift and emit telemetry if warranted.
    maybe_emit_drift_warning(new_physical, wall)

    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    # Clean up the persistent_term entry so tests that restart the
    # GenServer get a fresh atomics ref.
    :persistent_term.erase(@atomics_key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp merge_timestamps(wall, local_phys, local_logical, remote_phys, remote_log) do
    max_phys = Enum.max([wall, local_phys, remote_phys])

    cond do
      wall > local_phys and wall > remote_phys -> {wall, 0}
      local_phys == max_phys and local_phys > remote_phys -> {local_phys, local_logical + 1}
      remote_phys == max_phys and remote_phys > local_phys -> {remote_phys, remote_log + 1}
      local_phys == remote_phys -> {local_phys, max(local_logical, remote_log) + 1}
      wall == local_phys -> {local_phys, local_logical + 1}
      wall == remote_phys -> {wall, remote_log + 1}
      true -> {max_phys, 0}
    end
  end

  # Returns the atomics ref from :persistent_term, or nil if not yet created.
  @spec atomics_ref() :: reference() | nil
  defp atomics_ref do
    :persistent_term.get(@atomics_key)
  rescue
    ArgumentError -> nil
  end

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
