defmodule Ferricstore.Raft.Batcher do
  @moduledoc """
  Namespace-aware group commit batcher for a single FerricStore shard.

  Per spec sections 2C.5 and 2F.3, each shard has its own Batcher GenServer
  that accumulates write commands into per-namespace buffers, each with its
  own commit window and durability mode. When a namespace's timer fires, only
  that namespace's buffer is flushed.

  ## How it works

  1. A client calls `write/2` which sends a `GenServer.call` to the batcher.
  2. The batcher extracts the key's namespace prefix (e.g. `"session"` from
     `"session:abc123"`, `"_root"` for keys without a colon).
  3. The namespace config is looked up from the `:ferricstore_ns_config` ETS
     table to determine `window_ms` and `durability` for this prefix.
  4. The command and caller are appended to the namespace's buffer slot,
     identified by `{prefix, durability}`.
  5. On the first write to an empty slot, a timer is started using the
     namespace's `window_ms`.
  6. When the timer fires (`:flush_slot`), only that slot's commands are
     submitted.
  7. Each caller receives their individual result from the batch.

  ## Namespace configuration

  Per-prefix configuration is read from the `:ferricstore_ns_config` ETS
  table managed by `Ferricstore.NamespaceConfig`. If no configuration exists
  for a prefix, the defaults are used: `window_ms = 1`, `durability = :quorum`.

  For `:quorum` durability, commands are submitted to ra via
  `:ra.process_command/2`, which blocks until quorum acknowledgement.

  For `:async` durability (spec 2F.3), commands bypass Raft consensus
  entirely and are sent to `Ferricstore.Raft.AsyncApplyWorker`, which
  writes directly to the shard's Bitcask + ETS. Callers are replied to
  immediately with `:ok` without waiting for disk I/O. This trades
  consistency for lower write latency.

  ## Why a separate GenServer?

  The batcher is intentionally separate from the Shard GenServer and the
  ra state machine. This separation keeps the batching logic independent
  of the consensus layer and allows the Shard to remain focused on read
  operations and ETS management.

  ## Configuration

    * `:shard_id` (required) -- the ra server ID for this shard
    * `:shard_index` (required) -- zero-based shard index
    * `:max_batch_size` -- flush immediately when batch reaches this size (default: 1000)
  """

  use GenServer

  require Logger

  alias Ferricstore.NamespaceConfig

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

  @typedoc """
  A slot key identifies a unique batching bucket by namespace prefix and
  durability mode. Commands with the same prefix but different durability
  modes (which can happen if config changes mid-flight) are batched
  separately.
  """
  @type slot_key :: {binary(), :quorum | :async}

  @typedoc """
  A slot holds the accumulated commands and callers for a single namespace
  buffer, along with the timer reference for that slot's commit window.
  """
  @type slot :: %{
          cmds: [command()],
          froms: [GenServer.from()],
          timer_ref: reference() | nil,
          window_ms: pos_integer()
        }

  defstruct [
    :shard_id,
    :shard_index,
    :max_batch_size,
    slots: %{}
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts a batcher GenServer for the given shard.

  ## Options

    * `:shard_id` (required) -- ra server ID `{name, node()}` for this shard
    * `:shard_index` (required) -- zero-based shard index (used for process name)
    * `:max_batch_size` -- max commands per slot before forced flush (default: #{@default_max_batch_size})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: batcher_name(shard_index))
  end

  @doc """
  Submits a write command to the batcher for the given shard.

  The command is accumulated into the appropriate namespace buffer and
  submitted when the namespace's commit window expires or the buffer
  reaches `max_batch_size`.

  For `:quorum` durability, this call blocks until the ra command is
  committed and applied. For `:async` durability, the call returns as
  soon as the command is handed off to the `AsyncApplyWorker`.

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
  Synchronously flushes all pending writes across all namespace slots.

  Used in tests and before shard shutdown to ensure all writes are committed.
  """
  @spec flush(non_neg_integer()) :: :ok
  def flush(shard_index) do
    GenServer.call(batcher_name(shard_index), :flush, 10_000)
  end

  @doc """
  Extracts the namespace prefix from a command's key.

  The prefix is the portion of the key before the first colon (`:`).
  Keys without a colon are assigned to the `"_root"` namespace.

  ## Parameters

    * `command` -- a write command tuple

  ## Examples

      iex> Ferricstore.Raft.Batcher.extract_prefix({:put, "session:abc", "v", 0})
      "session"

      iex> Ferricstore.Raft.Batcher.extract_prefix({:delete, "nocolon"})
      "_root"

      iex> Ferricstore.Raft.Batcher.extract_prefix({:put, "ts:sensor:42", "v", 0})
      "ts"
  """
  @spec extract_prefix(command()) :: binary()
  def extract_prefix(command) when is_tuple(command) do
    key = elem(command, 1)

    case :binary.split(key, ":") do
      [^key] -> "_root"
      [prefix | _rest] -> prefix
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    shard_index = Keyword.fetch!(opts, :shard_index)
    max_batch_size = Keyword.get(opts, :max_batch_size, @default_max_batch_size)

    state = %__MODULE__{
      shard_id: shard_id,
      shard_index: shard_index,
      max_batch_size: max_batch_size
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:write, command}, from, state) do
    prefix = extract_prefix(command)
    {window_ms, durability} = lookup_ns_config(prefix)
    slot_key = {prefix, durability}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        froms: [from | slot.froms],
        window_ms: window_ms
    }

    # Start timer on first write to this slot
    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    # Flush immediately if slot is full
    if length(updated_slot.cmds) >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
    end
  end

  def handle_call(:flush, _from, state) do
    new_state = flush_all_slots(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_info({:flush_slot, slot_key}, state) do
    case Map.get(state.slots, slot_key) do
      nil ->
        {:noreply, state}

      slot ->
        # Clear the timer ref since the timer has already fired
        updated_slot = %{slot | timer_ref: nil}
        state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

        case do_flush_slot(state, slot_key) do
          {:noreply, new_state} -> {:noreply, new_state}
        end
    end
  end

  # Handle legacy :flush messages (e.g. from cancel_timer race conditions)
  def handle_info(:flush, state) do
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private: flush logic
  # ---------------------------------------------------------------------------

  @spec flush_all_slots(%__MODULE__{}) :: %__MODULE__{}
  defp flush_all_slots(state) do
    Enum.reduce(Map.keys(state.slots), state, fn slot_key, acc ->
      case do_flush_slot(acc, slot_key) do
        {:noreply, new_state} -> new_state
      end
    end)
  end

  @spec do_flush_slot(%__MODULE__{}, slot_key()) :: {:noreply, %__MODULE__{}}
  defp do_flush_slot(state, slot_key) do
    case Map.get(state.slots, slot_key) do
      nil ->
        {:noreply, state}

      %{cmds: []} = slot ->
        cancel_timer(slot.timer_ref)
        new_slots = Map.delete(state.slots, slot_key)
        {:noreply, %{state | slots: new_slots}}

      slot ->
        cancel_timer(slot.timer_ref)

        batch = Enum.reverse(slot.cmds)
        froms = Enum.reverse(slot.froms)

        {_prefix, durability} = slot_key

        case durability do
          :async ->
            # Async durability path (spec 2F.3): write directly to the shard
            # via AsyncApplyWorker and reply to callers immediately without
            # waiting for Raft consensus.
            submit_async(state.shard_index, batch, froms)

          :quorum ->
            # Quorum durability path: submit through Raft for consensus.
            # For single commands, submit directly without batch wrapper.
            # For multiple commands, wrap in a batch.
            case batch do
              [single_cmd] ->
                submit_single(state.shard_id, single_cmd, froms)

              _multiple ->
                submit_batch(state.shard_id, batch, froms)
            end
        end

        # Remove the slot entirely once flushed (clean up empty slots)
        new_slots = Map.delete(state.slots, slot_key)
        {:noreply, %{state | slots: new_slots}}
    end
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

  # Async durability path: sends commands to the AsyncApplyWorker (fire-and-forget)
  # and replies to all callers immediately with :ok. The AsyncApplyWorker
  # processes the batch in the background.
  defp submit_async(shard_index, batch, froms) do
    alias Ferricstore.Raft.AsyncApplyWorker

    AsyncApplyWorker.apply_batch(shard_index, batch)

    # Reply immediately to all callers with :ok
    Enum.each(froms, fn from ->
      GenServer.reply(from, :ok)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private: namespace config lookup
  # ---------------------------------------------------------------------------

  @spec lookup_ns_config(binary()) :: {pos_integer(), :quorum | :async}
  defp lookup_ns_config(prefix) do
    {NamespaceConfig.window_for(prefix), NamespaceConfig.durability_for(prefix)}
  end

  # ---------------------------------------------------------------------------
  # Private: slot helpers
  # ---------------------------------------------------------------------------

  @spec new_slot(pos_integer()) :: slot()
  defp new_slot(window_ms) do
    %{cmds: [], froms: [], timer_ref: nil, window_ms: window_ms}
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) do
    Process.cancel_timer(ref)
    # Flush any pending :flush_slot message that might have arrived
    receive do
      {:flush_slot, _} -> :ok
    after
      0 -> :ok
    end
  end
end
