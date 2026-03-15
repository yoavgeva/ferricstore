defmodule Ferricstore.Commands.Expiry do
  @moduledoc """
  Handles Redis expiry commands: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  ## Supported commands

    * `EXPIRE key seconds` — set TTL in seconds, returns 1 on success / 0 if key missing
    * `PEXPIRE key milliseconds` — set TTL in milliseconds
    * `EXPIREAT key unix-timestamp` — set absolute expiry (seconds since epoch)
    * `PEXPIREAT key unix-timestamp-ms` — set absolute expiry (milliseconds since epoch)
    * `TTL key` — remaining TTL in seconds (-1 = no expiry, -2 = key missing)
    * `PTTL key` — remaining TTL in milliseconds
    * `PERSIST key` — remove expiry, returns 1 if removed / 0 otherwise
  """

  @doc """
  Handles an expiry command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"EXPIRE"`, `"TTL"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get_meta`, `put` callbacks

  ## Returns

  Plain Elixir term: integer or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("EXPIRE", [key, secs_str], store), do: set_expiry_seconds(key, secs_str, store)

  def handle("EXPIRE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'expire' command"}
  end

  def handle("PEXPIRE", [key, ms_str], store), do: set_expiry_ms(key, ms_str, store)

  def handle("PEXPIRE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pexpire' command"}
  end

  def handle("EXPIREAT", [key, ts_str], store), do: set_expiry_at_seconds(key, ts_str, store)

  def handle("EXPIREAT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'expireat' command"}
  end

  def handle("PEXPIREAT", [key, ts_str], store), do: set_expiry_at_ms(key, ts_str, store)

  def handle("PEXPIREAT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pexpireat' command"}
  end

  def handle("TTL", [key], store), do: get_ttl_seconds(key, store)

  def handle("TTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ttl' command"}
  end

  def handle("PTTL", [key], store), do: get_ttl_ms(key, store)

  def handle("PTTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pttl' command"}
  end

  def handle("PERSIST", [key], store), do: do_persist(key, store)

  def handle("PERSIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'persist' command"}
  end

  # ---------------------------------------------------------------------------
  # Private — EXPIRE / PEXPIRE (relative)
  # ---------------------------------------------------------------------------

  defp set_expiry_seconds(key, secs_str, store) do
    with {secs, ""} <- Integer.parse(secs_str),
         true <- secs >= 0 do
      apply_expiry(key, System.os_time(:millisecond) + secs * 1_000, store)
    else
      false -> {:error, "ERR invalid expire time in 'expire' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp set_expiry_ms(key, ms_str, store) do
    with {ms, ""} <- Integer.parse(ms_str),
         true <- ms >= 0 do
      apply_expiry(key, System.os_time(:millisecond) + ms, store)
    else
      false -> {:error, "ERR invalid expire time in 'pexpire' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — EXPIREAT / PEXPIREAT (absolute)
  # ---------------------------------------------------------------------------

  defp set_expiry_at_seconds(key, ts_str, store) do
    case Integer.parse(ts_str) do
      {ts, ""} -> apply_expiry(key, ts * 1_000, store)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp set_expiry_at_ms(key, ts_str, store) do
    case Integer.parse(ts_str) do
      {ts, ""} -> apply_expiry(key, ts, store)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — apply expiry to existing key
  # ---------------------------------------------------------------------------

  defp apply_expiry(key, expire_at_ms, store) do
    case store.get_meta.(key) do
      nil ->
        0

      {value, _old_exp} ->
        store.put.(key, value, expire_at_ms)
        1
    end
  end

  # ---------------------------------------------------------------------------
  # Private — TTL / PTTL queries
  # ---------------------------------------------------------------------------

  defp get_ttl_seconds(key, store) do
    case store.get_meta.(key) do
      nil -> -2
      {_, 0} -> -1
      {_, exp} -> max(0, div(exp - System.os_time(:millisecond), 1_000))
    end
  end

  defp get_ttl_ms(key, store) do
    case store.get_meta.(key) do
      nil -> -2
      {_, 0} -> -1
      {_, exp} -> max(0, exp - System.os_time(:millisecond))
    end
  end

  # ---------------------------------------------------------------------------
  # Private — PERSIST (remove expiry)
  # ---------------------------------------------------------------------------

  defp do_persist(key, store) do
    case store.get_meta.(key) do
      nil -> 0
      {_, 0} -> 0
      {value, _exp} ->
        store.put.(key, value, 0)
        1
    end
  end
end
