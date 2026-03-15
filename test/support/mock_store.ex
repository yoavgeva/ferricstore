defmodule Ferricstore.Test.MockStore do
  @moduledoc false

  @doc """
  Creates an in-process mock store backed by an Agent.

  Each call returns a fresh, isolated store map that satisfies the
  command handler contract: `get`, `get_meta`, `put`, `delete`,
  `exists?`, `keys`, `flush`, `dbsize`.

  `initial` is a map of `%{key => {value, expire_at_ms}}`.
  """
  @spec make(map()) :: map()
  def make(initial \\ %{}) do
    {:ok, pid} = Agent.start_link(fn -> initial end)

    %{
      get: fn key -> Agent.get(pid, &read_value(&1, key)) end,
      get_meta: fn key -> Agent.get(pid, &read_meta(&1, key)) end,
      put: fn key, value, expire_at_ms ->
        Agent.update(pid, &Map.put(&1, key, {value, expire_at_ms}))
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, &Map.delete(&1, key))
        :ok
      end,
      exists?: fn key -> Agent.get(pid, &key_alive?(&1, key)) end,
      keys: fn -> Agent.get(pid, &live_keys/1) end,
      flush: fn -> Agent.update(pid, fn _ -> %{} end) end,
      dbsize: fn -> Agent.get(pid, &live_count/1) end
    }
  end

  # --- Pure helpers applied inside Agent.get/2 callbacks ---

  defp read_value(state, key) do
    case Map.get(state, key) do
      nil -> nil
      {value, 0} -> value
      {value, exp} -> if alive?(exp), do: value, else: nil
    end
  end

  defp read_meta(state, key) do
    case Map.get(state, key) do
      nil -> nil
      {value, 0} -> {value, 0}
      {value, exp} -> if alive?(exp), do: {value, exp}, else: nil
    end
  end

  defp key_alive?(state, key) do
    case Map.get(state, key) do
      nil -> false
      {_, 0} -> true
      {_, exp} -> alive?(exp)
    end
  end

  defp live_keys(state) do
    now = System.os_time(:millisecond)

    state
    |> Enum.filter(fn {_, {_, exp}} -> exp == 0 or exp > now end)
    |> Enum.map(fn {k, _} -> k end)
  end

  defp live_count(state) do
    now = System.os_time(:millisecond)
    Enum.count(state, fn {_, {_, exp}} -> exp == 0 or exp > now end)
  end

  defp alive?(exp), do: exp > System.os_time(:millisecond)
end
