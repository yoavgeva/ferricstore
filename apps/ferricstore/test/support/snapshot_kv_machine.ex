defmodule Ferricstore.Test.SnapshotKvMachine do
  @moduledoc """
  A simple Map-based ra state machine that emits `release_cursor` effects
  at a configurable interval. Used by snapshot/compaction recovery tests.

  State is `%{data: %{key => value}, applied: non_neg_integer()}`.

  Init config accepts `:release_cursor_interval` (default 100).
  """

  @behaviour :ra_machine

  @impl true
  def init(config) do
    interval = Map.get(config, :release_cursor_interval, 100)
    %{data: %{}, applied: 0, release_cursor_interval: interval}
  end

  @impl true
  def apply(meta, {:put, key, value}, state) do
    new_data = Map.put(state.data, key, value)
    new_applied = state.applied + 1
    new_state = %{state | data: new_data, applied: new_applied}
    maybe_release_cursor(meta, new_state)
  end

  def apply(_meta, {:get, key}, state) do
    # Reads don't bump applied count or trigger release_cursor.
    {state, Map.get(state.data, key)}
  end

  def apply(meta, {:delete, key}, state) do
    new_data = Map.delete(state.data, key)
    new_applied = state.applied + 1
    new_state = %{state | data: new_data, applied: new_applied}
    maybe_release_cursor(meta, new_state)
  end

  def apply(_meta, _cmd, state), do: {state, :ok}

  @impl true
  def state_enter(_role, _state), do: []

  # Query helpers (used with ra:local_query)
  def count(%{data: data}), do: map_size(data)
  def get_value(%{data: data}, key), do: Map.get(data, key)

  defp maybe_release_cursor(meta, state) do
    index = Map.get(meta, :index)
    interval = state.release_cursor_interval

    if index != nil and interval > 0 and rem(state.applied, interval) == 0 do
      {state, :ok, [{:release_cursor, index, state}]}
    else
      {state, :ok}
    end
  end
end
