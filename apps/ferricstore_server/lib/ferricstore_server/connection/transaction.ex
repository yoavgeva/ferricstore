defmodule FerricstoreServer.Connection.Transaction do
  @moduledoc false

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Store, as: ConnStore

  # Maximum commands queued inside a MULTI transaction (100K).
  @max_multi_queue_size 100_000

  @doc false
  def dispatch_multi(_args, %{multi_state: :queuing} = state) do
    {:continue, Encoder.encode({:error, "ERR MULTI calls can not be nested"}), state}
  end

  def dispatch_multi(_args, state) do
    new_state = %{state | multi_state: :queuing, multi_queue: [], multi_queue_count: 0}
    {:continue, Encoder.encode(:ok), new_state}
  end

  @doc false
  def dispatch_exec(_args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR EXEC without MULTI"}), state}
  end

  def dispatch_exec(_args, state) do
    result = execute_transaction(state)
    new_state = %{state | multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}}
    {:continue, Encoder.encode(result), new_state}
  end

  @doc false
  def dispatch_discard(_args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR DISCARD without MULTI"}), state}
  end

  def dispatch_discard(_args, state) do
    new_state = %{state | multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}}
    {:continue, Encoder.encode(:ok), new_state}
  end

  @doc false
  def dispatch_watch(_args, %{multi_state: :queuing} = state) do
    {:continue,
     Encoder.encode({:error, "ERR WATCH inside MULTI is not allowed"}), state}
  end

  def dispatch_watch([], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'watch' command"}), state}
  end

  def dispatch_watch(keys, state) do
    try do
      new_watched =
        Enum.reduce(keys, state.watched_keys, fn key, acc ->
          hash = :erlang.phash2(Router.get(state.instance_ctx, key))
          Map.put(acc, key, hash)
        end)

      {:continue, Encoder.encode(:ok), %{state | watched_keys: new_watched}}
    catch
      :exit, {reason, _} ->
        {:continue,
         Encoder.encode({:error, "ERR server not ready: #{inspect(reason)}"}), state}
    end
  end

  @doc false
  def dispatch_unwatch(_args, state) do
    {:continue, Encoder.encode(:ok), %{state | watched_keys: %{}}}
  end

  @doc """
  Handles queuing of commands during MULTI mode. Called when `multi_state: :queuing`
  for commands that are not in the passthrough set (EXEC, DISCARD, MULTI, WATCH, UNWATCH).
  """
  def dispatch_queue(cmd, args, state) do
    if state.multi_queue_count >= @max_multi_queue_size do
      new_state = %{state | multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}}

      {:continue,
       Encoder.encode(
         {:error,
          "ERR MULTI queue overflow (max #{@max_multi_queue_size} commands), transaction discarded"}
       ), new_state}
    else
      store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

      case validate_command(cmd, args, store) do
        :ok ->
          new_queue = [{cmd, args} | state.multi_queue]
          new_count = state.multi_queue_count + 1
          {:continue, Encoder.encode({:simple, "QUEUED"}), %{state | multi_queue: new_queue, multi_queue_count: new_count}}

        {:error, _msg} = err ->
          {:continue, Encoder.encode(err), state}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Transaction execution
  # ---------------------------------------------------------------------------

  defp execute_transaction(%{watched_keys: watched, multi_queue: queue, sandbox_namespace: ns}) do
    # Queue is stored in reverse order (prepend during MULTI) for O(1)
    # queuing. Reverse here at EXEC time to restore command ordering.
    Ferricstore.Transaction.Coordinator.execute(Enum.reverse(queue), watched, ns)
  end

  # ---------------------------------------------------------------------------
  # Command validation (for queue-time syntax checking)
  # ---------------------------------------------------------------------------

  defp validate_command(cmd, args, _store) do
    noop_store = build_noop_store()

    case Dispatcher.dispatch(cmd, args, noop_store) do
      {:error, "ERR unknown command" <> _} = err -> err
      {:error, "ERR wrong number of arguments" <> _} = err -> err
      {:error, "ERR syntax error" <> _} = err -> err
      _ -> :ok
    end
  end

  defp build_noop_store do
    %{
      get: fn _key -> nil end,
      get_meta: fn _key -> nil end,
      put: fn _key, _value, _expire_at_ms -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn _key -> false end,
      keys: fn -> [] end,
      flush: fn -> :ok end,
      dbsize: fn -> 0 end,
      incr: fn _key, _delta -> {:ok, 0} end,
      incr_float: fn _key, _delta -> {:ok, "0"} end,
      append: fn _key, _suffix -> {:ok, 0} end,
      getset: fn _key, _value -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _expire -> nil end,
      setrange: fn _key, _offset, _value -> {:ok, 0} end,
      cas: fn _key, _exp, _new, _ttl -> nil end,
      lock: fn _key, _owner, _ttl -> :ok end,
      unlock: fn _key, _owner -> 1 end,
      extend: fn _key, _owner, _ttl -> 1 end,
      ratelimit_add: fn _key, _window, _max, _count -> ["allowed", 0, 0, 0] end,
      list_op: fn _key, _op -> nil end
    }
  end
end
