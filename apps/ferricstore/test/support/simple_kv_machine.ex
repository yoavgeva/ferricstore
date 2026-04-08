defmodule Ferricstore.Test.SimpleKvMachine do
  @moduledoc false
  @behaviour :ra_machine

  @impl true
  def init(_config), do: %{}

  @impl true
  def apply(_meta, {:put, key, value}, state) do
    {Map.put(state, key, value), :ok}
  end

  def apply(_meta, {:get, key}, state) do
    {state, Map.get(state, key)}
  end

  def apply(_meta, _cmd, state), do: {state, :ok}

  @impl true
  def state_enter(_role, _state), do: []

  def count(state), do: map_size(state)
end
