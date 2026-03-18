defmodule Ferricstore.Test.ShardHelpers do
  @moduledoc """
  Shared helpers for tests that interact with application-supervised shards.

  Use this module in any test that kills or restarts shards to ensure the
  supervisor tree is fully healthy before and after the test.
  """

  @doc """
  Synchronously flushes all pending async writes on all 4 application-supervised
  shards (0–3) to disk.

  Call this before killing a shard in tests that verify crash durability, to
  ensure rapid consecutive puts (which may still be in state.pending due to
  the async io_uring batch window) are committed to the Bitcask log before the
  crash is simulated.
  """
  @spec flush_all_shards() :: :ok
  def flush_all_shards do
    Enum.each(0..3, fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      case Process.whereis(name) do
        pid when is_pid(pid) -> GenServer.call(pid, :flush, 10_000)
        nil -> :ok
      end
    end)
  end

  @doc """
  Deletes all keys across every shard. Equivalent to FLUSHDB.

  Call this in `setup` callbacks to prevent key accumulation across tests —
  a growing keydir makes KEYS/DBSIZE calls progressively slower and can cause
  GenServer timeouts when a test run accumulates thousands of keys.
  """
  @spec flush_all_keys() :: :ok
  def flush_all_keys do
    alias Ferricstore.Store.Router
    Enum.each(Router.keys(), &Router.delete/1)
  end

  @doc """
  Resets shared mutable state that can leak between tests: waiters registry,
  client tracking tables, and slow log. Call in `setup` for any test that
  cares about a clean global environment.
  """
  @spec flush_global_state() :: :ok
  def flush_global_state do
    # Waiters
    if :ets.whereis(:ferricstore_waiters) != :undefined do
      :ets.delete_all_objects(:ferricstore_waiters)
    end

    # Client tracking
    for table <- [:ferricstore_tracking, :ferricstore_tracking_connections] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    # Slow log
    if :ets.whereis(:ferricstore_slowlog) != :undefined do
      :ets.delete_all_objects(:ferricstore_slowlog)
    end

    # Audit log
    if :ets.whereis(:ferricstore_audit_log) != :undefined do
      :ets.delete_all_objects(:ferricstore_audit_log)
    end

    :ok
  end

  @doc """
  Waits until all 4 application-supervised shards (0–3) are alive.

  Polls every 20ms up to `timeout_ms`. Raises if any shard hasn't restarted
  in time. Call this in `on_exit` callbacks after tests that kill shards.
  """
  @spec wait_shards_alive(non_neg_integer()) :: :ok
  def wait_shards_alive(timeout_ms \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.each(0..3, fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      result =
        Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(20) end), :waiting, fn _, _ ->
          pid = Process.whereis(name)

          cond do
            is_pid(pid) and Process.alive?(pid) ->
              {:halt, :ok}

            System.monotonic_time(:millisecond) > deadline ->
              {:halt, {:timeout, name}}

            true ->
              {:cont, :waiting}
          end
        end)

      case result do
        :ok -> :ok
        {:timeout, name} -> raise "Shard #{inspect(name)} did not restart within #{timeout_ms}ms"
      end
    end)
  end
end
