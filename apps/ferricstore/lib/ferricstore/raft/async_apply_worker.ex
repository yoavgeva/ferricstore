defmodule Ferricstore.Raft.AsyncApplyWorker do
  @moduledoc """
  GenServer that processes async write batches for a single FerricStore shard.

  Per spec section 2F.3, when a namespace has durability mode `:async`, the
  write path writes to local Bitcask + ETS first (for immediate read-your-write),
  then submits the command to Raft in the background for replication to followers.

  ## How it works

  1. The `Batcher` detects that a slot has `:async` durability when the
     namespace's commit window fires, OR `Router.async_write` writes locally
     and forwards the command here for Raft submission.
  2. `apply_batch/2` replies immediately with `:ok` (non-blocking cast).
  3. The worker processes commands sequentially: for each `:put` it calls
     `NIF.put_batch/2` to write to Bitcask and updates the keydir ETS table.
     For `:delete` it calls `NIF.delete/2` and removes from ETS.
  4. After local application, the batch is submitted to Raft via
     `ra:pipeline_command/3` with a correlation ref. The worker handles
     the `ra_event` response:
     - `{:applied, ...}` -- command committed, remove from in-flight map.
     - `{:rejected, {:not_leader, leader, corr}}` -- resubmit to the
       hinted leader.
     - `:exit` / noproc -- buffer for retry on a timer.
  5. After each batch completes, a telemetry event is emitted with timing
     and batch size measurements.

  ## Raft submission guarantee

  Unlike fire-and-forget `pipeline_command/2`, this worker tracks every
  submission with a correlation ref and retries on failure. This ensures
  async writes are **eventually consistent** -- the command will reach the
  Raft log as long as the cluster is alive, even if the leader changes
  during submission.

  The client still gets `:ok` before Raft commits (that's the async contract),
  but the command is guaranteed to enter the Raft log eventually.

  ## Process registration

  Each shard has one AsyncApplyWorker registered under the name returned by
  `worker_name/1`, e.g. `:"Ferricstore.Raft.AsyncApplyWorker.0"`.

  ## Telemetry

  After each batch is processed locally:

      :telemetry.execute(
        [:ferricstore, :async_apply, :batch],
        %{duration_us: integer(), batch_size: integer()},
        %{shard_index: integer()}
      )

  On Raft submission failure + retry:

      :telemetry.execute(
        [:ferricstore, :async_apply, :raft_retry],
        %{pending_count: integer()},
        %{shard_index: integer(), reason: atom()}
      )
  """

  use GenServer

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.LFU

  @typedoc "A write command to be applied asynchronously."
  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:delete, binary()}

  # Max retry attempts before dropping a command (prevents unbounded growth)
  @max_retries 10
  # Retry interval when ra server is down
  @retry_interval_ms 500

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts an AsyncApplyWorker GenServer for the given shard.

  ## Options

    * `:shard_index` (required) -- zero-based shard index
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: worker_name(shard_index))
  end

  @doc """
  Submits a batch of write commands to the async worker for processing.

  This function returns immediately with `:ok` without waiting for the
  writes to complete. The worker processes the batch asynchronously.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `commands` -- list of command tuples to apply

  ## Returns

    * `:ok` -- always succeeds (fire-and-forget)

  ## Examples

      iex> AsyncApplyWorker.apply_batch(0, [{:put, "key", "value", 0}])
      :ok
  """
  @spec apply_batch(non_neg_integer(), [command()]) :: :ok
  def apply_batch(shard_index, commands) do
    GenServer.cast(worker_name(shard_index), {:apply_batch, commands})
  end

  @doc """
  Submits commands to Raft only (no local Bitcask/ETS write).

  Used by `Router.async_write` which has already written locally and just
  needs the command replicated through Raft. The worker handles correlation
  tracking and retries on rejection.
  """
  @spec replicate(non_neg_integer(), [command()]) :: :ok
  def replicate(shard_index, commands) do
    GenServer.cast(worker_name(shard_index), {:replicate, commands})
  end

  @doc """
  Blocks until all previously enqueued async batches for `shard_index` have
  been applied AND all in-flight Raft submissions have been resolved.

  Since the worker is a GenServer that processes messages sequentially, a
  synchronous `call` placed after all prior `cast`s will not be handled
  until those casts complete. This makes `drain/1` an efficient barrier
  without any polling or sleep.

  Returns `:ok`, or `:ok` silently if the worker is not running.

  ## Examples

      iex> AsyncApplyWorker.drain(0)
      :ok
  """
  @spec drain(non_neg_integer()) :: :ok
  def drain(shard_index) do
    name = worker_name(shard_index)

    case Process.whereis(name) do
      nil -> :ok
      _pid -> GenServer.call(name, :drain, 10_000)
    end
  end

  @doc """
  Returns the registered process name for the async worker at `shard_index`.

  ## Examples

      iex> Ferricstore.Raft.AsyncApplyWorker.worker_name(0)
      :"Ferricstore.Raft.AsyncApplyWorker.0"
  """
  @spec worker_name(non_neg_integer()) :: atom()
  def worker_name(shard_index), do: :"Ferricstore.Raft.AsyncApplyWorker.#{shard_index}"

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)

    {:ok,
     %{
       shard_index: shard_index,
       shard_id: Ferricstore.Raft.Cluster.shard_server_id(shard_index),
       # %{correlation_ref => {command_or_batch, retry_count}}
       pending: %{},
       # Commands buffered when ra server is down, retried on timer.
       # Uses :queue for O(1) FIFO — preserves submission order on retry.
       retry_buffer: :queue.new()
     }}
  end

  @impl true
  def handle_call(:drain, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:apply_batch, []}, state) do
    {:noreply, state}
  end

  def handle_cast({:apply_batch, commands}, state) do
    start_time = System.monotonic_time(:microsecond)

    # Step 1: Apply locally (Bitcask + ETS)
    apply_commands(state.shard_index, commands)

    duration_us = System.monotonic_time(:microsecond) - start_time

    :telemetry.execute(
      [:ferricstore, :async_apply, :batch],
      %{duration_us: duration_us, batch_size: length(commands)},
      %{shard_index: state.shard_index}
    )

    # Step 2: Submit to Raft for replication (non-blocking, tracked)
    new_state = submit_to_raft(state, commands)

    {:noreply, new_state}
  end

  def handle_cast({:replicate, []}, state) do
    {:noreply, state}
  end

  def handle_cast({:replicate, commands}, state) do
    # Raft submission only — local write already done by Router.async_write
    new_state = submit_to_raft(state, commands)
    {:noreply, new_state}
  end

  # ---------------------------------------------------------------------------
  # ra_event handling — applied / rejected
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({:ra_event, _leader, {:applied, applied_list}}, state) do
    new_pending =
      Enum.reduce(applied_list, state.pending, fn {corr, _result}, pending ->
        Map.delete(pending, corr)
      end)

    {:noreply, %{state | pending: new_pending}}
  end

  def handle_info({:ra_event, _from, {:rejected, {:not_leader, maybe_leader, corr}}}, state) do
    case Map.pop(state.pending, corr) do
      {nil, _pending} ->
        {:noreply, state}

      {{commands, retry_count}, new_pending} ->
        new_state = %{state | pending: new_pending}

        if retry_count >= @max_retries do
          Logger.warning(
            "AsyncApplyWorker: shard #{state.shard_index} dropping async batch after #{@max_retries} retries"
          )

          :telemetry.execute(
            [:ferricstore, :async_apply, :raft_dropped],
            %{batch_size: length(commands), retries: retry_count},
            %{shard_index: state.shard_index}
          )

          {:noreply, new_state}
        else
          # Resubmit to the hinted leader
          leader =
            if maybe_leader not in [nil, :undefined],
              do: maybe_leader,
              else: state.shard_id

          :telemetry.execute(
            [:ferricstore, :async_apply, :raft_retry],
            %{pending_count: map_size(new_state.pending) + 1},
            %{shard_index: state.shard_index, reason: :not_leader}
          )

          new_state = do_pipeline_submit(new_state, leader, commands, retry_count + 1)
          {:noreply, new_state}
        end
    end
  end

  def handle_info({:ra_event, _from, {:rejected, {_reason, _hint, corr}}}, state) do
    case Map.pop(state.pending, corr) do
      {nil, _pending} ->
        {:noreply, state}

      {{commands, retry_count}, new_pending} ->
        new_state = %{state | pending: new_pending}

        if retry_count >= @max_retries do
          Logger.warning(
            "AsyncApplyWorker: shard #{state.shard_index} dropping async batch after #{@max_retries} retries (rejected)"
          )

          {:noreply, new_state}
        else
          :telemetry.execute(
            [:ferricstore, :async_apply, :raft_retry],
            %{pending_count: map_size(new_state.pending) + 1},
            %{shard_index: state.shard_index, reason: :rejected}
          )

          new_state = do_pipeline_submit(new_state, state.shard_id, commands, retry_count + 1)
          {:noreply, new_state}
        end
    end
  end

  # Retry timer: flush the retry buffer (FIFO order preserved by :queue)
  def handle_info(:retry_buffer, state) do
    if :queue.is_empty(state.retry_buffer) do
      {:noreply, state}
    else
      items = :queue.to_list(state.retry_buffer)

      {to_retry, to_drop} =
        Enum.split_with(items, fn {_commands, retry_count} ->
          retry_count < @max_retries
        end)

      Enum.each(to_drop, fn {commands, retry_count} ->
        Logger.warning(
          "AsyncApplyWorker: shard #{state.shard_index} dropping #{length(commands)} buffered commands after #{retry_count} retries"
        )
      end)

      # Resubmit in FIFO order (oldest first)
      new_state =
        Enum.reduce(to_retry, %{state | retry_buffer: :queue.new()}, fn {commands, retry_count}, acc ->
          do_pipeline_submit(acc, acc.shard_id, commands, retry_count + 1)
        end)

      {:noreply, new_state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Private: Raft submission with correlation tracking
  # ---------------------------------------------------------------------------

  # Submits a batch of commands to Raft via pipeline_command/3 with a
  # correlation ref. Tracks the ref in state.pending for retry on rejection.
  defp submit_to_raft(state, commands) do
    do_pipeline_submit(state, state.shard_id, commands, 0)
  end

  defp do_pipeline_submit(state, target, commands, retry_count) do
    corr = make_ref()

    raft_command =
      case commands do
        [single] -> single
        batch -> {:batch, batch}
      end

    try do
      case :ra.pipeline_command(target, raft_command, corr, :normal) do
        :ok ->
          %{state | pending: Map.put(state.pending, corr, {commands, retry_count})}

        {:error, _reason} ->
          # ra server returned error — buffer for retry
          buffer_for_retry(state, commands, retry_count)
      end
    catch
      :exit, _ ->
        # ra server is down (noproc) — buffer for retry
        buffer_for_retry(state, commands, retry_count)
    end
  end

  defp buffer_for_retry(state, commands, retry_count) do
    was_empty = :queue.is_empty(state.retry_buffer)
    new_buffer = :queue.in({commands, retry_count}, state.retry_buffer)

    # Schedule retry if this is the first item in the buffer
    if was_empty do
      Process.send_after(self(), :retry_buffer, @retry_interval_ms)
    end

    :telemetry.execute(
      [:ferricstore, :async_apply, :raft_retry],
      %{pending_count: map_size(state.pending), buffer_size: :queue.len(new_buffer)},
      %{shard_index: state.shard_index, reason: :noproc}
    )

    %{state | retry_buffer: new_buffer}
  end

  # ---------------------------------------------------------------------------
  # Private: local command application (Bitcask + ETS)
  # ---------------------------------------------------------------------------

  # Applies a list of commands directly to the shard's Bitcask and ETS.
  #
  # For puts, we batch all consecutive puts together and flush them in a
  # single `NIF.put_batch/2` call for efficiency. Deletes are applied
  # individually since they don't benefit from batching.
  @spec apply_commands(non_neg_integer(), [command()]) :: :ok
  defp apply_commands(shard_index, commands) do
    {active_file_id, active_file_path} = get_active_file(shard_index)
    keydir = :"keydir_#{shard_index}"

    # Separate puts (which can be batched) from other commands
    {puts, others} = split_puts_and_others(commands)

    # Apply puts in a single batch via v2 append
    if puts != [] do
      batch_entries =
        Enum.map(puts, fn {:put, key, value, expire_at_ms} ->
          {key, value, expire_at_ms}
        end)

      case NIF.v2_append_batch(active_file_path, batch_entries) do
        {:ok, results} ->
          # Update ETS (7-tuple) for each put.
          # Values exceeding the hot_cache_max_value_size threshold are stored
          # as nil (cold) to avoid expensive binary copies on :ets.lookup.
          puts
          |> Enum.zip(results)
          |> Enum.each(fn {{:put, key, value, expire_at_ms}, {offset, value_size}} ->
            value_for_ets = value_for_ets(value)

            :ets.insert(
              keydir,
              {key, value_for_ets, expire_at_ms, LFU.initial(), active_file_id, offset, value_size}
            )
          end)

        {:error, reason} ->
          Logger.error(
            "AsyncApplyWorker: v2_append_batch failed for shard #{shard_index}: #{inspect(reason)}"
          )
      end
    end

    # Apply deletes individually via v2 tombstone append
    Enum.each(others, fn
      {:delete, key} ->
        case NIF.v2_append_tombstone(active_file_path, key) do
          {:ok, _} ->
            :ets.delete(keydir, key)

          {:error, reason} ->
            Logger.error(
              "AsyncApplyWorker: tombstone write failed for shard #{shard_index}, key #{inspect(key)}: #{inspect(reason)}"
            )
        end
    end)

    :ok
  end

  # Splits commands into puts and non-puts (deletes).
  @spec split_puts_and_others([command()]) :: {[command()], [command()]}
  defp split_puts_and_others(commands) do
    Enum.split_with(commands, fn
      {:put, _, _, _} -> true
      _ -> false
    end)
  end

  # Retrieves the active log file info from the Shard via a lightweight
  # GenServer.call instead of :sys.get_state (which copies the entire state).
  @spec get_active_file(non_neg_integer()) :: {non_neg_integer(), binary()}
  defp get_active_file(shard_index) do
    ctx = FerricStore.Instance.get(:default)
    shard_name = elem(ctx.shard_names, shard_index)
    GenServer.call(shard_name, :get_active_file)
  end

  # Returns nil for values exceeding the hot cache max value size threshold,
  # or the value itself if it fits. Prevents large values from being stored
  # in ETS, avoiding expensive binary copies on every :ets.lookup.
  @compile {:inline, value_for_ets: 1}
  defp value_for_ets(nil), do: nil
  defp value_for_ets(value) when is_binary(value) do
    if byte_size(value) > :persistent_term.get(:ferricstore_hot_cache_max_value_size, 65_536) do
      nil
    else
      value
    end
  end
end
