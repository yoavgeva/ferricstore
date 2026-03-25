defmodule Ferricstore.Test.Utils do
  @moduledoc """
  Shared test utilities for polling assertions and other common patterns.
  """

  @doc """
  Retries `fun` until it passes (no exception) or `timeout` milliseconds elapse.

  On the final attempt after the deadline, the function runs without rescue so
  the original assertion error propagates to ExUnit with a clear failure message.
  """
  @spec eventually((-> any()), non_neg_integer(), pos_integer()) :: any()
  def eventually(fun, timeout \\ 5000, interval \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    if System.monotonic_time(:millisecond) > deadline do
      fun.()
    else
      try do
        fun.()
      rescue
        _ ->
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
      end
    end
  end
end
