defmodule Ferricstore.Store.Shard do
  @moduledoc """
  GenServer managing one Bitcask partition backed by an ETS hot-read cache.

  ## Write path: group commit

  To hit high write throughput (50k+ req/s), individual `put` calls do **not**
  block waiting for an fsync except for the very first write in each batch
  window:

  1. The key is written to ETS immediately (reads see it at once).
  2. The entry is appended to an in-memory pending list.
  3. If this is the **first write in a new batch window**, the pending list is
     flushed via `NIF.put_batch_async/2`. On Linux with io_uring this submits
     writes + fsync to the ring and returns immediately. On other platforms it
     falls back to synchronous `put_batch`.
  4. If the batch window already has pending writes, the put returns immediately
     — the new entry will be flushed by the recurring timer or the next sync
     point.
  5. A recurring timer fires every `@flush_interval_ms` (1 ms by default) and
     calls `NIF.put_batch_async/2` with all accumulated entries.

  ## Async I/O lifecycle

  When `NIF.put_batch_async/2` returns `{:pending, op_id}`, the shard stores
  `op_id` in `flush_in_flight`. While a flush is in-flight, subsequent
  `flush_pending` calls are no-ops — new writes accumulate in `pending` and
  will be flushed on the next timer tick after the in-flight completes.

  When the fsync CQE arrives, the NIF sends `{:io_complete, op_id, result}`
  to this process. The `handle_info` callback clears `flush_in_flight`,
  allowing the next timer tick to flush any accumulated pending writes.

  ## Read path: ETS bypass

  `Router.get/1` and `Router.get_meta/1` read ETS directly without going
  through this GenServer for hot (cached) keys. Only cold keys (not yet in
  ETS) fall back to a `{:get, key}` call here, which loads from Bitcask and
  warms the cache.

  ## ETS layout

  Each entry is a tuple `{key, value, expire_at_ms}` where `expire_at_ms = 0`
  means the key never expires. Expired entries are lazily evicted on read.

  ## Process registration

  Shards register under the name returned by
  `Ferricstore.Store.Router.shard_name/1`, e.g.
  `:"Ferricstore.Store.Shard.0"`.
  """

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Router

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1

  # Timeout for synchronous flush (blocking receive for async completion).
  @sync_flush_timeout_ms 5_000

  defstruct [:store, :ets, :index, pending: [], flush_in_flight: nil]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a shard GenServer.

  ## Options

    * `:index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:flush_interval_ms` -- batch-commit interval in ms (default: #{@flush_interval_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    name = Router.shard_name(index)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
    path = Path.join(data_dir, "shard_#{index}")
    File.mkdir_p!(path)
    {:ok, store} = NIF.new(path)
    ets = :ets.new(:"shard_ets_#{index}", [:set, :public, :named_table])
    schedule_flush(flush_ms)
    {:ok, %__MODULE__{store: store, ets: ets, index: index, pending: [],
                       flush_in_flight: nil},
     {:continue, {:flush_interval, flush_ms}}}
  end

  @impl true
  def handle_continue({:flush_interval, ms}, state) do
    # Store flush interval in process dictionary so handle_info can reschedule.
    Process.put(:flush_interval_ms, ms)
    {:noreply, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, do_get(state, key), state}
  end

  def handle_call({:get_meta, key}, _from, state) do
    {:reply, do_get_meta(state, key), state}
  end

  def handle_call({:put, key, value, expire_at_ms}, _from, state) do
    # Write to ETS immediately so reads see it right away.
    :ets.insert(state.ets, {key, value, expire_at_ms})
    new_pending = [{key, value, expire_at_ms} | state.pending]
    new_state = %{state | pending: new_pending}

    # Flush immediately only when this is the first write in a new batch window
    # AND there is no in-flight async flush. At low concurrency this keeps
    # single-write durability. At high concurrency the batch window is already
    # filling up — subsequent puts stay queued for the next flush.
    if state.pending == [] and state.flush_in_flight == nil do
      {:reply, :ok, flush_pending(new_state)}
    else
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:delete, key}, _from, state) do
    # Delete is always synchronous — tombstones must be durable immediately
    # so a crash doesn't resurrect the key.
    #
    # 1. Wait for any in-flight async flush to complete.
    # 2. Flush remaining pending writes synchronously.
    # 3. Write the tombstone.
    # 4. Remove the deleted key from pending to prevent resurrection.
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    NIF.delete(state.store, key)
    :ets.delete(state.ets, key)
    # Remove any pending entry for this key (belt-and-suspenders: flush above
    # already cleared pending, but be explicit).
    new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
    {:reply, :ok, %{state | pending: new_pending}}
  end

  def handle_call({:exists, key}, _from, state) do
    {:reply, do_get(state, key) != nil, state}
  end

  def handle_call(:keys, _from, state) do
    # Flush first so NIF.keys() sees all pending writes.
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, live_keys(state), state}
  end

  # Synchronous flush — used by tests and by delete to ensure durability.
  def handle_call(:flush, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_pending(state)
    schedule_flush(Process.get(:flush_interval_ms, @flush_interval_ms))
    {:noreply, state}
  end

  # Handle async io_uring completion message from the NIF background thread.
  def handle_info({:io_complete, op_id, result}, state) do
    if state.flush_in_flight == op_id do
      case result do
        :ok ->
          {:noreply, %{state | flush_in_flight: nil}}

        {:error, reason} ->
          # The async flush failed. Log the error but clear in-flight so
          # the next timer tick can attempt another flush. The keydir was
          # updated optimistically by prepare_batch_for_async — on the next
          # store open, log replay will reconcile.
          Logger.error(
            "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
          )

          {:noreply, %{state | flush_in_flight: nil}}
      end
    else
      # Stale or unknown op_id — ignore.
      {:noreply, state}
    end
  end

  # -------------------------------------------------------------------
  # Private: flush
  # -------------------------------------------------------------------

  # Async flush — used by the timer and by put (first-write-in-window).
  # If a flush is already in-flight or pending is empty, this is a no-op.
  defp flush_pending(%{pending: []} = state), do: state
  defp flush_pending(%{flush_in_flight: op_id} = state) when op_id != nil, do: state

  defp flush_pending(%{pending: pending, store: store} = state) do
    # Reverse to preserve insertion order (list was prepended).
    batch =
      pending
      |> Enum.reverse()
      |> Enum.map(fn {k, v, exp} -> {k, v, exp} end)

    case NIF.put_batch_async(store, batch) do
      {:pending, op_id} ->
        # Async submission succeeded — clear pending, track in-flight.
        %{state | pending: [], flush_in_flight: op_id}

      :ok ->
        # Sync fallback (macOS / no io_uring) — completed immediately.
        %{state | pending: []}

      {:error, _reason} ->
        # On error keep the pending list so writes are not lost — the timer
        # will retry on the next tick.
        state
    end
  end

  # Synchronous flush — used by delete, :flush, and :keys calls that need
  # durability guarantees. Uses the sync put_batch path. The caller must
  # first call `await_in_flight/1` to ensure no async op is in-flight.
  defp flush_pending_sync(%{pending: []} = state), do: state

  defp flush_pending_sync(%{pending: pending, store: store} = state) do
    batch =
      pending
      |> Enum.reverse()
      |> Enum.map(fn {k, v, exp} -> {k, v, exp} end)

    case NIF.put_batch(store, batch) do
      :ok ->
        %{state | pending: []}

      {:error, _reason} ->
        state
    end
  end

  # Block until any in-flight async flush completes. This is only called
  # from synchronous GenServer callbacks (delete, keys, flush) that need
  # durability before proceeding.
  defp await_in_flight(%{flush_in_flight: nil} = state), do: state

  defp await_in_flight(%{flush_in_flight: op_id} = state) do
    receive do
      {:io_complete, ^op_id, :ok} ->
        %{state | flush_in_flight: nil}

      {:io_complete, ^op_id, {:error, reason}} ->
        Logger.error(
          "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
        )

        %{state | flush_in_flight: nil}
    after
      @sync_flush_timeout_ms ->
        Logger.error(
          "Shard #{state.index}: timed out waiting for async flush op #{op_id}"
        )

        # Clear in-flight to unblock the caller. The async op may still
        # complete later — its message will be ignored (unknown op_id).
        %{state | flush_in_flight: nil}
    end
  end

  defp schedule_flush(ms) do
    Process.send_after(self(), :flush, ms)
  end

  # -------------------------------------------------------------------
  # Private: read helpers
  # -------------------------------------------------------------------

  defp do_get(state, key) do
    case ets_lookup(state.ets, key) do
      {:hit, value, _expire_at_ms} -> value
      :expired -> nil
      :miss -> warm_from_store(state, key)
    end
  end

  defp do_get_meta(state, key) do
    case ets_lookup(state.ets, key) do
      {:hit, value, expire_at_ms} -> {value, expire_at_ms}
      :expired -> nil
      :miss -> warm_meta_from_store(state, key)
    end
  end

  # Classifies an ETS lookup as a cache hit, expired entry, or miss.
  # Expired entries are evicted immediately.
  defp ets_lookup(ets, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(ets, key) do
      [{^key, value, 0}] ->
        {:hit, value, 0}

      [{^key, value, exp}] when exp > now ->
        {:hit, value, exp}

      [{^key, _value, _exp}] ->
        :ets.delete(ets, key)
        :expired

      [] ->
        :miss
    end
  end

  defp warm_from_store(state, key) do
    case NIF.get(state.store, key) do
      {:ok, nil} ->
        nil

      {:ok, value} ->
        :ets.insert(state.ets, {key, value, 0})
        value

      _error ->
        nil
    end
  end

  defp warm_meta_from_store(state, key) do
    case NIF.get(state.store, key) do
      {:ok, nil} ->
        nil

      {:ok, value} ->
        :ets.insert(state.ets, {key, value, 0})
        {value, 0}

      _error ->
        nil
    end
  end

  defp live_keys(state) do
    now = System.os_time(:millisecond)

    state.store
    |> NIF.keys()
    |> Enum.filter(fn key -> key_alive?(state.ets, key, now) end)
  end

  defp key_alive?(ets, key, now) do
    case :ets.lookup(ets, key) do
      [{_, _, 0}] -> true
      [{_, _, exp}] -> exp > now
      [] -> true
    end
  end
end
