defmodule Ferricstore.Raft.Batcher do
  @moduledoc """
  Group commit batcher for a single FerricStore shard.

  Per spec section 2C.5, each shard has its own Batcher GenServer that
  accumulates write commands for up to `batch_window_ms` (default: 1ms),
  then submits them as a single `{:batch, commands}` entry to the Raft log
  via `:ra.process_command/2`.

  ## How it works

  1. A client calls `write/2` which sends a `GenServer.call` to the batcher.
  2. The batcher appends the command and caller to an internal accumulator.
  3. On the **first** write in a new batch window, a timer is started.
  4. When the timer fires (`:flush`), all accumulated commands are submitted
     to ra as a single batch command.
  5. Each caller receives their individual result from the batch.

  ## Why a separate GenServer?

  The batcher is intentionally separate from the Shard GenServer and the
  ra state machine. This separation keeps the batching logic independent
  of the consensus layer and allows the Shard to remain focused on read
  operations and ETS management.

  ## Configuration

    * `:shard_id` (required) -- the ra server ID for this shard
    * `:batch_window_ms` -- max time to accumulate writes (default: 1)
    * `:max_batch_size` -- flush immediately when batch reaches this size (default: 1000)
  """

  use GenServer

  require Logger

  @default_batch_window_ms 1
  @default_max_batch_size 1_000

  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:delete, binary()}
          | {:incr_float, binary(), float()}
          | {:append, binary(), binary()}
          | {:getset, binary(), binary()}
          | {:getdel, binary()}
          | {:getex, binary(), non_neg_integer()}
          | {:setrange, binary(), non_neg_integer(), binary()}

  defstruct [
    :shard_id,
    :batch_window_ms,
    :max_batch_size,
    batch: [],
    froms: [],
    timer_ref: nil
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts a batcher GenServer for the given shard.

  ## Options

    * `:shard_id` (required) -- ra server ID `{name, node()}` for this shard
    * `:shard_index` (required) -- zero-based shard index (used for process name)
    * `:batch_window_ms` -- batch accumulation window in ms (default: #{@default_batch_window_ms})
    * `:max_batch_size` -- max commands per batch before forced flush (default: #{@default_max_batch_size})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: batcher_name(shard_index))
  end

  @doc """
  Submits a write command to the batcher for the given shard.

  The command is accumulated and submitted to ra in the next batch flush.
  This call blocks until the ra command is committed and applied.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `command` -- a write command tuple, e.g. `{:put, key, value, expire_at_ms}`

  ## Returns

    * `:ok` on success
    * `{:error, reason}` on failure
  """
  @spec write(non_neg_integer(), command()) :: :ok | {:error, term()}
  def write(shard_index, command) do
    GenServer.call(batcher_name(shard_index), {:write, command}, 10_000)
  end

  @doc """
  Returns the registered process name for the batcher at `shard_index`.

  ## Examples

      iex> Ferricstore.Raft.Batcher.batcher_name(0)
      :"Ferricstore.Raft.Batcher.0"
  """
  @spec batcher_name(non_neg_integer()) :: atom()
  def batcher_name(shard_index), do: :"Ferricstore.Raft.Batcher.#{shard_index}"

  @doc """
  Synchronously flushes any pending writes in the batcher.

  Used in tests and before shard shutdown to ensure all writes are committed.
  """
  @spec flush(non_neg_integer()) :: :ok
  def flush(shard_index) do
    GenServer.call(batcher_name(shard_index), :flush, 10_000)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    batch_window_ms = Keyword.get(opts, :batch_window_ms, @default_batch_window_ms)
    max_batch_size = Keyword.get(opts, :max_batch_size, @default_max_batch_size)

    state = %__MODULE__{
      shard_id: shard_id,
      batch_window_ms: batch_window_ms,
      max_batch_size: max_batch_size
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:write, command}, from, state) do
    new_state = %{
      state
      | batch: [command | state.batch],
        froms: [from | state.froms]
    }

    # Start timer on first write in window
    new_state =
      if new_state.timer_ref == nil do
        ref = Process.send_after(self(), :flush, state.batch_window_ms)
        %{new_state | timer_ref: ref}
      else
        new_state
      end

    # Flush immediately if batch is full
    if length(new_state.batch) >= state.max_batch_size do
      do_flush(new_state)
    else
      {:noreply, new_state}
    end
  end

  def handle_call(:flush, _from, state) do
    case do_flush(state) do
      {:noreply, new_state} -> {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_info(:flush, state) do
    do_flush(%{state | timer_ref: nil})
  end

  # ---------------------------------------------------------------------------
  # Private: flush logic
  # ---------------------------------------------------------------------------

  defp do_flush(%{batch: []} = state) do
    cancel_timer(state.timer_ref)
    {:noreply, %{state | timer_ref: nil}}
  end

  defp do_flush(state) do
    cancel_timer(state.timer_ref)

    batch = Enum.reverse(state.batch)
    froms = Enum.reverse(state.froms)

    # For single commands, submit directly without batch wrapper
    # For multiple commands, wrap in a batch
    case batch do
      [single_cmd] ->
        submit_single(state.shard_id, single_cmd, froms)

      _multiple ->
        submit_batch(state.shard_id, batch, froms)
    end

    {:noreply, %{state | batch: [], froms: [], timer_ref: nil}}
  end

  defp submit_single(shard_id, command, [from]) do
    case :ra.process_command(shard_id, command) do
      {:ok, result, _leader} ->
        GenServer.reply(from, result)

      {:error, reason} ->
        GenServer.reply(from, {:error, reason})

      {:timeout, _leader} ->
        GenServer.reply(from, {:error, :ra_timeout})
    end
  end

  defp submit_batch(shard_id, batch, froms) do
    case :ra.process_command(shard_id, {:batch, batch}) do
      {:ok, {:ok, results}, _leader} ->
        Enum.zip(froms, results)
        |> Enum.each(fn {from, result} ->
          GenServer.reply(from, result)
        end)

      {:ok, result, _leader} ->
        # Unexpected result shape -- reply to all with the raw result
        Enum.each(froms, fn from ->
          GenServer.reply(from, result)
        end)

      {:error, reason} ->
        Enum.each(froms, fn from ->
          GenServer.reply(from, {:error, reason})
        end)

      {:timeout, _leader} ->
        Enum.each(froms, fn from ->
          GenServer.reply(from, {:error, :ra_timeout})
        end)
    end
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) do
    Process.cancel_timer(ref)
    # Flush any pending :flush message that might have arrived
    receive do
      :flush -> :ok
    after
      0 -> :ok
    end
  end
end
