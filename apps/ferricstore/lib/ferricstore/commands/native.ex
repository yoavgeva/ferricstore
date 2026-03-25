# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Native do
  @moduledoc """
  Handles FerricStore-native commands that go beyond the Redis command set.

  ## Supported commands

    * `CAS key expected new [EX seconds]` -- compare-and-swap
    * `LOCK key owner ttl_ms` -- acquire a distributed lock
    * `UNLOCK key owner` -- release a distributed lock
    * `EXTEND key owner ttl_ms` -- extend lock TTL
    * `RATELIMIT.ADD key window_ms max [count]` -- sliding window rate limiter
    * `KEY_INFO key` -- returns diagnostic metadata about a key
  """

  alias Ferricstore.Store.Router

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CAS", [key, expected, new_value], _store), do: Router.cas(key, expected, new_value, nil)
  def handle("CAS", [key, expected, new_value, "EX", secs_str], _store) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 -> Router.cas(key, expected, new_value, secs * 1_000)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  def handle("CAS", _args, _store), do: {:error, "ERR wrong number of arguments for 'cas' command"}
  def handle("LOCK", [key, owner, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 -> Router.lock(key, owner, ttl_ms)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  def handle("LOCK", _args, _store), do: {:error, "ERR wrong number of arguments for 'lock' command"}
  def handle("UNLOCK", [key, owner], _store), do: Router.unlock(key, owner)
  def handle("UNLOCK", _args, _store), do: {:error, "ERR wrong number of arguments for 'unlock' command"}
  def handle("EXTEND", [key, owner, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 -> Router.extend(key, owner, ttl_ms)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  def handle("EXTEND", _args, _store), do: {:error, "ERR wrong number of arguments for 'extend' command"}
  def handle("RATELIMIT.ADD", [key, wms, max_str], _store), do: do_ratelimit_add(key, wms, max_str, "1")
  def handle("RATELIMIT.ADD", [key, wms, max_str, cnt], _store), do: do_ratelimit_add(key, wms, max_str, cnt)
  def handle("RATELIMIT.ADD", _args, _store), do: {:error, "ERR wrong number of arguments for 'ratelimit.add' command"}
  def handle("KEY_INFO", [key], _store), do: do_key_info(key)
  def handle("KEY_INFO", _args, _store), do: {:error, "ERR wrong number of arguments for 'key_info' command"}
  def handle("FETCH_OR_COMPUTE", [key, ttl], _store), do: do_fetch_or_compute(key, ttl, "")
  def handle("FETCH_OR_COMPUTE", [key, ttl, hint], _store), do: do_fetch_or_compute(key, ttl, hint)
  def handle("FETCH_OR_COMPUTE", _args, _store), do: {:error, "ERR wrong number of arguments for 'fetch_or_compute' command"}
  def handle("FETCH_OR_COMPUTE_RESULT", [key, value, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms >= 0 -> Ferricstore.FetchOrCompute.fetch_or_compute_result(key, value, ttl_ms)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  def handle("FETCH_OR_COMPUTE_RESULT", _args, _store), do: {:error, "ERR wrong number of arguments for 'fetch_or_compute_result' command"}
  def handle("FETCH_OR_COMPUTE_ERROR", [key, msg], _store), do: Ferricstore.FetchOrCompute.fetch_or_compute_error(key, msg)
  def handle("FETCH_OR_COMPUTE_ERROR", _args, _store), do: {:error, "ERR wrong number of arguments for 'fetch_or_compute_error' command"}

  defp do_ratelimit_add(key, wms, max_str, cnt) do
    with {w, ""} <- Integer.parse(wms), true <- w > 0,
         {m, ""} <- Integer.parse(max_str), true <- m > 0,
         {c, ""} <- Integer.parse(cnt), true <- c > 0 do
      Router.ratelimit_add(key, w, m, c)
    else
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp do_key_info(key) do
    idx = Router.shard_for(key)
    keydir = :"keydir_#{idx}"
    now = System.os_time(:millisecond)
    shard = Router.shard_name(idx)

    store = %{
      get: &Router.get/1,
      compound_get: fn redis_key, compound_key ->
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end
    }

    type = Ferricstore.Store.TypeRegistry.get_type(key, store)
    alive? = type != "none"

    {value, expire_at_ms, hot_status} =
      if alive? do
        {val, exp, hot} = ets_key_info(keydir, key, now)
        if val != nil do
          {val, exp, hot}
        else
          case Router.get_meta(key) do
            nil -> {nil, 0, "cold"}
            {v, e} -> {v, e, "cold"}
          end
        end
      else
        {nil, 0, "cold"}
      end

    value_size = if alive? and is_binary(value), do: byte_size(value), else: 0

    ttl_ms =
      cond do
        not alive? -> -2
        expire_at_ms == 0 -> -1
        true -> max(expire_at_ms - now, 0)
      end

    [
      "type", type,
      "value_size", Integer.to_string(value_size),
      "ttl_ms", Integer.to_string(ttl_ms),
      "hot_cache_status", hot_status,
      "last_write_shard", Integer.to_string(idx)
    ]
  end

  # 7-tuple keydir lookup: {key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
  defp ets_key_info(keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          {value, 0, "hot"}
        [{^key, nil, 0, _lfu, _fid, _off, _vsize}] ->
          {nil, 0, "cold"}
        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {value, exp, "hot"}
        [{^key, nil, exp, _lfu, _fid, _off, _vsize}] when exp > now ->
          {nil, exp, "cold"}
        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          {nil, 0, "cold"}
        [] ->
          {nil, 0, "cold"}
      end
    rescue
      ArgumentError -> {nil, 0, "cold"}
    end
  end

  defp do_fetch_or_compute(key, ttl_ms_str, hint) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 ->
        case Ferricstore.FetchOrCompute.fetch_or_compute(key, ttl_ms, hint) do
          {:hit, v} -> ["hit", v]
          {:compute, ch} -> ["compute", ch]
          {:ok, v} -> ["hit", v]
          {:error, reason} -> {:error, "ERR compute failed: " <> reason}
        end
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
end
