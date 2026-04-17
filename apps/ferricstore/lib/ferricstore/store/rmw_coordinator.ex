defmodule Ferricstore.Store.RmwCoordinator do
  @moduledoc """
  Per-shard fallback for async read-modify-write (RMW) commands under
  contention.

  See `docs/async-rmw-design.md` for the full design. In short:

  - `Router.async_rmw/4` tries `:ets.insert_new(latch_tab, {key, self()})`.
    If it wins the latch, it runs the RMW inline in the caller's process
    (~15μs p50). Fast path.
  - If the latch is already held, `async_rmw` falls through here and
    does `GenServer.call(RmwCoordinator.name(shard), {:rmw, cmd})`. The
    worker processes RMW commands serially from its mailbox (FIFO). This
    is the slow path under heavy same-key contention, but it never
    loses updates and callers sleep on `receive` while queued (zero CPU).

  The worker itself also acquires the per-key latch before executing —
  bounded spin, because at most one latch holder exists for any key,
  and only one process (the worker) ever spins. No thundering herd.

  Periodic latch sweep (every 5s) removes entries whose holder pid is
  dead — recovery path for a caller that crashed between `insert_new`
  and `ets.take`.
  """

  use GenServer

  require Logger

  @sweep_interval_ms 5_000

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the coordinator for the given shard index.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    idx = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, idx, name: name(idx))
  end

  @doc """
  Registered process name for the coordinator at the given shard index.
  """
  @spec name(non_neg_integer()) :: atom()
  def name(idx), do: :"Ferricstore.Store.RmwCoordinator.#{idx}"

  @doc """
  Execute an RMW command via the worker (fallback path).

  Callers only reach this when they lost the latch CAS in
  `Router.async_rmw/4`. Returns the command's natural result
  (`{:ok, integer}` for INCR, `old_value_or_nil` for GETSET/GETDEL, etc.).

  Timeouts and worker crashes propagate as `:exit` to the caller;
  Router's `async_rmw` catches them and returns `{:error, msg}`.
  """
  @spec execute(non_neg_integer(), tuple()) :: term()
  def execute(idx, cmd), do: GenServer.call(name(idx), {:rmw, cmd}, 10_000)

  @doc """
  Force a sweep of stale latches for this shard. Intended for tests.
  """
  @spec sweep_latches(non_neg_integer()) :: :ok
  def sweep_latches(idx), do: GenServer.call(name(idx), :sweep_latches_now, 5_000)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(idx) do
    # The instance context may not be populated yet at application start
    # order. Defer lookup until first use, but remember the shard index.
    Process.send_after(self(), :sweep_latches, @sweep_interval_ms)
    {:ok, %{idx: idx}}
  end

  @impl true
  def handle_call({:rmw, cmd}, _from, state) do
    ctx = FerricStore.Instance.get(:default)

    case ctx do
      nil ->
        {:reply, {:error, "ERR instance not initialized"}, state}

      _ ->
        latch_tab = elem(ctx.latch_refs, state.idx)
        key = key_of(cmd)
        wait_for_latch(latch_tab, key)

        try do
          result = Ferricstore.Store.Router.execute_rmw_inline(ctx, state.idx, cmd)
          :telemetry.execute([:ferricstore, :rmw, :worker], %{}, %{shard_index: state.idx})
          {:reply, result, state}
        after
          :ets.take(latch_tab, key)
        end
    end
  end

  def handle_call(:sweep_latches_now, _from, state) do
    case FerricStore.Instance.get(:default) do
      nil -> :ok
      ctx -> do_sweep(elem(ctx.latch_refs, state.idx))
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:sweep_latches, state) do
    case FerricStore.Instance.get(:default) do
      nil -> :ok
      ctx -> do_sweep(elem(ctx.latch_refs, state.idx))
    end

    Process.send_after(self(), :sweep_latches, @sweep_interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Acquire the per-key latch. Only the worker process ever spins here, so
  # there's no thundering herd.
  #
  # If the current holder is dead (crashed mid-RMW), take over the latch
  # immediately instead of waiting for the periodic sweeper. This handles
  # the "caller crashed before `:ets.take`" recovery path without a 5s
  # stall on the first RMW to the orphaned key.
  defp wait_for_latch(tab, key) do
    case :ets.insert_new(tab, {key, self()}) do
      true ->
        :ok

      false ->
        case :ets.lookup(tab, key) do
          [{^key, holder}] when is_pid(holder) ->
            if Process.alive?(holder) do
              :erlang.yield()
              wait_for_latch(tab, key)
            else
              # Orphaned latch — take over.
              :ets.delete(tab, key)
              wait_for_latch(tab, key)
            end

          _ ->
            # Race: holder released between our insert_new and our lookup.
            :erlang.yield()
            wait_for_latch(tab, key)
        end
    end
  end

  # Remove latch entries whose holder pid is dead. Called on a timer and
  # via the `:sweep_latches_now` test hook.
  defp do_sweep(tab) do
    dead =
      :ets.foldl(
        fn {key, pid}, acc ->
          if Process.alive?(pid), do: acc, else: [key | acc]
        end,
        [],
        tab
      )

    Enum.each(dead, fn key -> :ets.delete(tab, key) end)

    if dead != [] do
      Logger.debug("RmwCoordinator: swept #{length(dead)} stale latch entries")
    end

    :ok
  end

  defp key_of({:incr, k, _}), do: k
  defp key_of({:incr_float, k, _}), do: k
  defp key_of({:append, k, _}), do: k
  defp key_of({:getset, k, _}), do: k
  defp key_of({:getdel, k}), do: k
  defp key_of({:getex, k, _}), do: k
  defp key_of({:setrange, k, _, _}), do: k
end
