defmodule Ferricstore.Raft.AsyncApplyWorker do
  @moduledoc """
  GenServer that processes async write batches for a single FerricStore shard.

  Per spec section 2F.3, when a namespace has durability mode `:async`, the
  write path bypasses Raft consensus (`:ra.process_command/2`) and instead
  writes directly to the shard's Bitcask store and ETS hot cache. This trades
  strong consistency (quorum acknowledgement) for lower write latency.

  ## How it works

  1. The `Batcher` detects that a slot has `:async` durability when the
     namespace's commit window fires.
  2. Instead of calling `:ra.process_command/2` (which blocks until quorum),
     the Batcher sends the batch to the `AsyncApplyWorker` via `apply_batch/2`.
  3. `apply_batch/2` replies immediately with `:ok` (non-blocking cast).
  4. The worker processes commands sequentially: for each `:put` it calls
     `NIF.put_batch/2` to write to Bitcask and updates both the keydir and
     hot_cache ETS tables. For `:delete` it calls `NIF.delete/2` and removes
     from ETS.
  5. After each batch completes, a telemetry event is emitted with timing
     and batch size measurements.

  ## Design rationale

  In single-node mode, "async durability" simplifies to "write to disk but
  don't wait for Raft quorum". Since the only member is the local node, Raft
  consensus is effectively a no-op beyond the WAL write. By bypassing Raft
  entirely for async namespaces, we eliminate the serialization through the
  ra process and the WAL append + fsync overhead, achieving significantly
  lower write latency.

  In a future multi-node deployment, this worker could be extended to
  submit commands to Raft in the background (fire-and-forget) for eventual
  replication to followers.

  ## Process registration

  Each shard has one AsyncApplyWorker registered under the name returned by
  `worker_name/1`, e.g. `:"Ferricstore.Raft.AsyncApplyWorker.0"`.

  ## Telemetry

  After each batch is processed:

      :telemetry.execute(
        [:ferricstore, :async_apply, :batch],
        %{duration_us: integer(), batch_size: integer()},
        %{shard_index: integer()}
      )
  """

  use GenServer

  require Logger

  alias Ferricstore.Bitcask.NIF

  @typedoc "A write command to be applied asynchronously."
  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:delete, binary()}

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
  Blocks until all previously enqueued async batches for `shard_index` have
  been applied.

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
       shard_index: shard_index
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

    apply_commands(state.shard_index, commands)

    duration_us = System.monotonic_time(:microsecond) - start_time

    :telemetry.execute(
      [:ferricstore, :async_apply, :batch],
      %{duration_us: duration_us, batch_size: length(commands)},
      %{shard_index: state.shard_index}
    )

    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private: command application
  # ---------------------------------------------------------------------------

  # Applies a list of commands directly to the shard's Bitcask and ETS.
  #
  # For puts, we batch all consecutive puts together and flush them in a
  # single `NIF.put_batch/2` call for efficiency. Deletes are applied
  # individually since they don't benefit from batching.
  @lfu_initial_counter 5

  @spec apply_commands(non_neg_integer(), [command()]) :: :ok
  defp apply_commands(shard_index, commands) do
    alias Ferricstore.Store.PrefixIndex

    store = get_store(shard_index)
    keydir = :"keydir_#{shard_index}"
    prefix_table = PrefixIndex.table_name(shard_index)

    # Separate puts (which can be batched) from other commands
    {puts, others} = split_puts_and_others(commands)

    # Apply puts in a single batch
    if puts != [] do
      batch_entries = Enum.map(puts, fn {:put, key, value, expire_at_ms} ->
        {key, value, expire_at_ms}
      end)

      case NIF.put_batch(store, batch_entries) do
        :ok ->
          # Update ETS and prefix index for each put
          Enum.each(puts, fn {:put, key, value, expire_at_ms} ->
            :ets.insert(keydir, {key, value, expire_at_ms, @lfu_initial_counter})

            try do
              PrefixIndex.track(prefix_table, key, shard_index)
            rescue
              ArgumentError -> :ok
            end
          end)

        {:error, reason} ->
          Logger.error(
            "AsyncApplyWorker: put_batch failed for shard #{shard_index}: #{inspect(reason)}"
          )
      end
    end

    # Apply deletes individually
    Enum.each(others, fn
      {:delete, key} ->
        NIF.delete(store, key)
        :ets.delete(keydir, key)

        try do
          PrefixIndex.untrack(prefix_table, key, shard_index)
        rescue
          ArgumentError -> :ok
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

  # Retrieves the Bitcask store reference from the Shard's GenServer state.
  # The store reference is obtained via :sys.get_state for the shard process.
  @spec get_store(non_neg_integer()) :: reference()
  defp get_store(shard_index) do
    shard_name = Ferricstore.Store.Router.shard_name(shard_index)
    state = :sys.get_state(shard_name)
    state.store
  end
end
