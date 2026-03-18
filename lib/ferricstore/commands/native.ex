# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Native do
  @moduledoc """
  Handles FerricStore-native commands that go beyond the Redis command set.

  These commands provide primitives useful for distributed coordination and
  rate limiting, implemented as atomic shard-level operations.

  ## Supported commands

    * `CAS key expected new [EX seconds]` — compare-and-swap
    * `LOCK key owner ttl_ms` — acquire a distributed lock
    * `UNLOCK key owner` — release a distributed lock
    * `EXTEND key owner ttl_ms` — extend lock TTL
    * `RATELIMIT.ADD key window_ms max [count]` — sliding window rate limiter
  """

  alias Ferricstore.Store.Router

  @doc """
  Handles a native FerricStore command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"CAS"`, `"LOCK"`)
    - `args` - List of string arguments
    - `store` - Injected store map (unused; native commands call Router directly)

  ## Returns

  Command-specific return values. See individual command docs below.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # CAS — Compare-and-Swap
  # ---------------------------------------------------------------------------

  def handle("CAS", [key, expected, new_value], _store) do
    Router.cas(key, expected, new_value, nil)
  end

  def handle("CAS", [key, expected, new_value, "EX", secs_str], _store) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        ttl_ms = secs * 1_000
        Router.cas(key, expected, new_value, ttl_ms)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("CAS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cas' command"}
  end

  # ---------------------------------------------------------------------------
  # LOCK — Acquire distributed lock
  # ---------------------------------------------------------------------------

  def handle("LOCK", [key, owner, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 ->
        Router.lock(key, owner, ttl_ms)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("LOCK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lock' command"}
  end

  # ---------------------------------------------------------------------------
  # UNLOCK — Release distributed lock
  # ---------------------------------------------------------------------------

  def handle("UNLOCK", [key, owner], _store) do
    Router.unlock(key, owner)
  end

  def handle("UNLOCK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'unlock' command"}
  end

  # ---------------------------------------------------------------------------
  # EXTEND — Extend lock TTL
  # ---------------------------------------------------------------------------

  def handle("EXTEND", [key, owner, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 ->
        Router.extend(key, owner, ttl_ms)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("EXTEND", _args, _store) do
    {:error, "ERR wrong number of arguments for 'extend' command"}
  end

  # ---------------------------------------------------------------------------
  # RATELIMIT.ADD — Sliding window rate limiter
  # ---------------------------------------------------------------------------

  def handle("RATELIMIT.ADD", [key, window_ms_str, max_str], _store) do
    do_ratelimit_add(key, window_ms_str, max_str, "1")
  end

  def handle("RATELIMIT.ADD", [key, window_ms_str, max_str, count_str], _store) do
    do_ratelimit_add(key, window_ms_str, max_str, count_str)
  end

  def handle("RATELIMIT.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ratelimit.add' command"}
  end

  defp do_ratelimit_add(key, window_ms_str, max_str, count_str) do
    with {window_ms, ""} <- Integer.parse(window_ms_str),
         true <- window_ms > 0,
         {max, ""} <- Integer.parse(max_str),
         true <- max > 0,
         {count, ""} <- Integer.parse(count_str),
         true <- count > 0 do
      Router.ratelimit_add(key, window_ms, max, count)
    else
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # FETCH_OR_COMPUTE — cache-aside with stampede protection
  # ---------------------------------------------------------------------------

  def handle("FETCH_OR_COMPUTE", [key, ttl_ms_str], _store) do
    do_fetch_or_compute(key, ttl_ms_str, "")
  end

  def handle("FETCH_OR_COMPUTE", [key, ttl_ms_str, hint], _store) do
    do_fetch_or_compute(key, ttl_ms_str, hint)
  end

  def handle("FETCH_OR_COMPUTE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'fetch_or_compute' command"}
  end

  def handle("FETCH_OR_COMPUTE_RESULT", [key, value, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms >= 0 ->
        Ferricstore.FetchOrCompute.fetch_or_compute_result(key, value, ttl_ms)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("FETCH_OR_COMPUTE_RESULT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'fetch_or_compute_result' command"}
  end

  def handle("FETCH_OR_COMPUTE_ERROR", [key, error_msg], _store) do
    Ferricstore.FetchOrCompute.fetch_or_compute_error(key, error_msg)
  end

  def handle("FETCH_OR_COMPUTE_ERROR", _args, _store) do
    {:error, "ERR wrong number of arguments for 'fetch_or_compute_error' command"}
  end

  defp do_fetch_or_compute(key, ttl_ms_str, hint) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 ->
        case Ferricstore.FetchOrCompute.fetch_or_compute(key, ttl_ms, hint) do
          {:hit, value} -> ["hit", value]
          {:compute, compute_hint} -> ["compute", compute_hint]
          {:ok, value} -> ["hit", value]
          {:error, reason} -> {:error, "ERR compute failed: " <> reason}
        end

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end
end
