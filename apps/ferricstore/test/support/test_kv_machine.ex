defmodule Ferricstore.Bench.TestKvMachine do
  @moduledoc """
  Minimal ra state machine for benchmarking pure ra throughput.

  This machine applies `:put` and `:delete` commands to an ETS table
  with zero extra overhead -- no NIF calls, no prefix indexing, no
  expiry tracking. It serves as the baseline to measure how much
  overhead FerricStore's StateMachine.apply adds on top of raw ra
  consensus.

  Used exclusively by `Ferricstore.Bench.SingleShardProfileTest`.
  """

  @behaviour :ra_machine

  @impl true
  def init(config) do
    %{ets: config.ets}
  end

  @impl true
  def apply(_meta, {:put, key, value}, state) do
    :ets.insert(state.ets, {key, value})
    {state, :ok}
  end

  def apply(_meta, {:delete, key}, state) do
    :ets.delete(state.ets, key)
    {state, :ok}
  end

  def apply(_meta, {:batch, commands}, state) do
    results =
      Enum.map(commands, fn
        {:put, key, value} ->
          :ets.insert(state.ets, {key, value})
          :ok

        {:delete, key} ->
          :ets.delete(state.ets, key)
          :ok
      end)

    {state, {:ok, results}}
  end

  def apply(_meta, _unknown, state) do
    {state, :ok}
  end

  @impl true
  def state_enter(_role, _state), do: []
end
