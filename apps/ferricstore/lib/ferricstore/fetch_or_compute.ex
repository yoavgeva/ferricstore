defmodule Ferricstore.FetchOrCompute do
  @moduledoc """
  GenServer managing compute locks for cache-aside with stampede protection.

  When a cache miss occurs, only one client is allowed to compute the value
  (the "computer"). All other clients requesting the same key block as
  "waiters" until the computer delivers the result or an error.

  ## Stampede protection

  Without stampede protection, a cache miss on a hot key causes N concurrent
  clients to all independently compute the same expensive value. This module
  serializes computation: only the first caller gets `{:compute, hint}`,
  subsequent callers block until the result arrives.

  ## Timeout handling

  If the computer does not deliver a result within `compute_timeout_ms`
  (default 30 seconds), the next waiter in line is promoted to computer.
  If there are no waiters, the lock is simply cleared.

  ## ETS table

  Compute locks are stored in `:ferricstore_compute_locks` as:

      {key, computer_pid, waiters_list, started_at_ms, compute_hint}

  This table is owned by the GenServer process.
  """

  use GenServer

  alias Ferricstore.Store.Router

  require Logger

  @table :ferricstore_compute_locks
  @default_compute_timeout_ms 30_000

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts the FetchOrCompute GenServer.

  ## Options

    * `:compute_timeout_ms` -- timeout in milliseconds before a stalled
      computer is replaced (default: #{@default_compute_timeout_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Attempts to fetch a key from the store, or initiates a compute lock.

  ## Parameters

    * `key` -- the cache key to look up
    * `ttl_ms` -- TTL in milliseconds for the value when stored
    * `hint` -- an opaque string hint passed to the computer (e.g. a URL
      or function name to call)

  ## Returns

    * `{:hit, value}` -- the key was found in the store
    * `{:compute, hint}` -- the caller is the computer; it must call
      `fetch_or_compute_result/3` or `fetch_or_compute_error/2`
    * `{:ok, value}` -- the caller was a waiter and received the result
    * `{:error, reason}` -- the compute failed (error propagated to waiters)
  """
  @spec fetch_or_compute(binary(), pos_integer(), binary()) ::
          {:hit, binary()} | {:compute, binary()} | {:ok, binary()} | {:error, binary()}
  def fetch_or_compute(key, ttl_ms, hint) do
    GenServer.call(__MODULE__, {:fetch_or_compute, key, ttl_ms, hint, self()}, :infinity)
  end

  @doc """
  Delivers the computed result for a key.

  Stores the value in the Router with the given TTL, wakes all waiters
  with `{:ok, value}`, and clears the compute lock.

  ## Parameters

    * `key` -- the cache key
    * `value` -- the computed value to store
    * `ttl_ms` -- TTL in milliseconds

  ## Returns

  `:ok`
  """
  @spec fetch_or_compute_result(binary(), binary(), pos_integer()) :: :ok
  def fetch_or_compute_result(key, value, ttl_ms) do
    GenServer.call(__MODULE__, {:fetch_or_compute_result, key, value, ttl_ms})
  end

  @doc """
  Reports a compute error for a key.

  Wakes all waiters with `{:error, error_msg}` and clears the compute lock.

  ## Parameters

    * `key` -- the cache key
    * `error_msg` -- error message to propagate to waiters

  ## Returns

  `:ok`
  """
  @spec fetch_or_compute_error(binary(), binary()) :: :ok
  def fetch_or_compute_error(key, error_msg) do
    GenServer.call(__MODULE__, {:fetch_or_compute_error, key, error_msg})
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    timeout_ms = Keyword.get(opts, :compute_timeout_ms, @default_compute_timeout_ms)
    _table = :ets.new(@table, [:set, :named_table, :protected])
    schedule_sweep(timeout_ms)
    {:ok, %{compute_timeout_ms: timeout_ms}}
  end

  @impl true
  def handle_call({:fetch_or_compute, key, _ttl_ms, hint, caller_pid}, from, state) do
    # Check the store first.
    ctx = FerricStore.Instance.get(:default)
    case Router.get(ctx, key) do
      nil ->
        # Cache miss -- check for existing compute lock.
        case :ets.lookup(@table, key) do
          [{^key, _computer_pid, waiters, started_at, _hint}] ->
            # A compute lock exists -- add this caller as a waiter.
            # The caller will block until the computer delivers or times out.
            new_waiters = waiters ++ [{from, caller_pid}]
            :ets.insert(@table, {key, elem(List.first(:ets.lookup(@table, key)), 1), new_waiters, started_at, elem(List.first(:ets.lookup(@table, key)), 4)})

            # Re-read for clarity.
            [{^key, computer_pid, _old_waiters, _started, lock_hint}] = :ets.lookup(@table, key)
            :ets.insert(@table, {key, computer_pid, new_waiters, started_at, lock_hint})

            # Do NOT reply here -- the caller blocks. Reply comes when result arrives.
            {:noreply, state}

          [] ->
            # No lock -- this caller becomes the computer.
            now = System.monotonic_time(:millisecond)
            :ets.insert(@table, {key, caller_pid, [], now, hint})
            {:reply, {:compute, hint}, state}
        end

      value ->
        # Cache hit.
        {:reply, {:hit, value}, state}
    end
  end

  def handle_call({:fetch_or_compute_result, key, value, ttl_ms}, _from, state) do
    # Store the value in the Router.
    expire_at_ms =
      if ttl_ms > 0 do
        System.os_time(:millisecond) + ttl_ms
      else
        0
      end

    ctx = FerricStore.Instance.get(:default)
    Router.put(ctx, key, value, expire_at_ms)

    # Wake all waiters.
    case :ets.lookup(@table, key) do
      [{^key, _computer_pid, waiters, _started, _hint}] ->
        Enum.each(waiters, fn {waiter_from, _pid} ->
          GenServer.reply(waiter_from, {:ok, value})
        end)

        :ets.delete(@table, key)

      [] ->
        :ok
    end

    {:reply, :ok, state}
  end

  def handle_call({:fetch_or_compute_error, key, error_msg}, _from, state) do
    case :ets.lookup(@table, key) do
      [{^key, _computer_pid, waiters, _started, _hint}] ->
        Enum.each(waiters, fn {waiter_from, _pid} ->
          GenServer.reply(waiter_from, {:error, error_msg})
        end)

        :ets.delete(@table, key)

      [] ->
        :ok
    end

    {:reply, :ok, state}
  end

  # -------------------------------------------------------------------
  # Timeout sweep
  # -------------------------------------------------------------------

  @impl true
  def handle_info(:sweep_timeouts, state) do
    now = System.monotonic_time(:millisecond)
    timeout_ms = state.compute_timeout_ms

    # Scan all compute locks for timed-out computers.
    :ets.tab2list(@table)
    |> Enum.each(fn {key, _computer_pid, waiters, started_at, hint} ->
      if now - started_at > timeout_ms, do: promote_or_clear(key, waiters, hint)
    end)

    schedule_sweep(timeout_ms)
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  defp promote_or_clear(key, [{next_from, next_pid} | rest_waiters], hint) do
    new_started = System.monotonic_time(:millisecond)
    :ets.insert(@table, {key, next_pid, rest_waiters, new_started, hint})
    GenServer.reply(next_from, {:compute, hint})
  end

  defp promote_or_clear(key, [], _hint), do: :ets.delete(@table, key)

  defp schedule_sweep(timeout_ms) do
    # Sweep at half the timeout interval for reasonable responsiveness.
    interval = max(div(timeout_ms, 2), 500)
    Process.send_after(self(), :sweep_timeouts, interval)
  end
end
