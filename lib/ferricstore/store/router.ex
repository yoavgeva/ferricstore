defmodule Ferricstore.Store.Router do
  @moduledoc """
  Routes keys to shard GenServers using consistent hashing via `:erlang.phash2/2`.

  This is a pure module with no process state. It provides two categories of
  functions:

  1. **Routing helpers** -- `shard_for/2` and `shard_name/1` map a key to its
     owning shard index and registered process name respectively.

  2. **Convenience accessors** -- `get/1`, `put/3`, `delete/1`, `exists?/1`,
     `keys/0`, and `dbsize/0` dispatch to the correct shard GenServer
     transparently.
  """

  @shard_count Application.compile_env(:ferricstore, :shard_count, 4)

  # -------------------------------------------------------------------
  # Routing helpers
  # -------------------------------------------------------------------

  @doc """
  Returns the shard index (0-based) that owns `key`.

  Uses `:erlang.phash2/2` for fast, deterministic distribution.

  ## Parameters

    * `key` -- binary key to route
    * `shard_count` -- total number of shards (defaults to compile-time
      `:ferricstore, :shard_count` or 4)

  ## Examples

      iex> Ferricstore.Store.Router.shard_for("user:42", 4) in 0..3
      true

  """
  @spec shard_for(binary(), pos_integer()) :: non_neg_integer()
  def shard_for(key, shard_count \\ @shard_count) do
    :erlang.phash2(key, shard_count)
  end

  @doc """
  Returns the registered process name for the shard at `index`.

  ## Examples

      iex> Ferricstore.Store.Router.shard_name(0)
      :"Ferricstore.Store.Shard.0"

  """
  @spec shard_name(non_neg_integer()) :: atom()
  def shard_name(index), do: :"Ferricstore.Store.Shard.#{index}"

  # -------------------------------------------------------------------
  # Convenience accessors (dispatch to correct shard)
  # -------------------------------------------------------------------

  @doc """
  Retrieves the value for `key`, or `nil` if the key does not exist or is
  expired.

  Hot path: reads directly from ETS (no GenServer roundtrip for cached keys).
  Falls back to a GenServer call for cache misses or when the ETS table is
  temporarily unavailable (e.g. during a shard restart).
  """
  @spec get(binary()) :: binary() | nil
  def get(key) do
    idx = shard_for(key)
    ets = :"shard_ets_#{idx}"
    now = System.os_time(:millisecond)

    case ets_get(ets, key, now) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss -> GenServer.call(shard_name(idx), {:get, key})
      :no_table -> GenServer.call(shard_name(idx), {:get, key})
    end
  end

  @doc """
  Returns `{value, expire_at_ms}` for a live key, or `nil` if the key does
  not exist or is expired.

  Hot path: reads directly from ETS for cached keys.
  """
  @spec get_meta(binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(key) do
    idx = shard_for(key)
    ets = :"shard_ets_#{idx}"
    now = System.os_time(:millisecond)

    case ets_get(ets, key, now) do
      {:hit, value, exp} -> {value, exp}
      :expired -> nil
      :miss -> GenServer.call(shard_name(idx), {:get_meta, key})
      :no_table -> GenServer.call(shard_name(idx), {:get_meta, key})
    end
  end

  # ETS fast-path lookup. Returns:
  #   {:hit, value, expire_at_ms} -- key is live
  #   :expired                    -- key existed but has passed its TTL (also evicts it)
  #   :miss                       -- key not in ETS (may be in Bitcask)
  #   :no_table                   -- ETS table does not exist (shard restarting)
  defp ets_get(ets, key, now) do
    try do
      case :ets.lookup(ets, key) do
        [{^key, value, 0}] ->
          {:hit, value, 0}

        [{^key, value, exp}] when exp > now ->
          {:hit, value, exp}

        [{^key, _value, _exp}] ->
          :ets.delete(ets, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  @doc """
  Stores `key` with `value`. `expire_at_ms` is an absolute Unix-epoch
  timestamp in milliseconds; pass `0` for no expiry.
  """
  @spec put(binary(), binary(), non_neg_integer()) :: :ok
  def put(key, value, expire_at_ms \\ 0) do
    GenServer.call(shard_name(shard_for(key)), {:put, key, value, expire_at_ms})
  end

  @doc "Deletes `key`. Returns `:ok` whether or not the key existed."
  @spec delete(binary()) :: :ok
  def delete(key) do
    GenServer.call(shard_name(shard_for(key)), {:delete, key})
  end

  @doc "Returns `true` if `key` exists and is not expired."
  @spec exists?(binary()) :: boolean()
  def exists?(key) do
    GenServer.call(shard_name(shard_for(key)), {:exists, key})
  end

  @doc """
  Atomically increments the integer value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_integer}`
  on success or `{:error, reason}` if the value is not a valid integer.
  """
  @spec incr(binary(), integer()) :: {:ok, integer()} | {:error, binary()}
  def incr(key, delta) do
    GenServer.call(shard_name(shard_for(key)), {:incr, key, delta})
  end

  @doc """
  Atomically increments the float value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
  on success or `{:error, reason}` if the value is not a valid float.
  """
  @spec incr_float(binary(), float()) :: {:ok, binary()} | {:error, binary()}
  def incr_float(key, delta) do
    GenServer.call(shard_name(shard_for(key)), {:incr_float, key, delta})
  end

  @doc """
  Atomically appends `suffix` to the value of `key`.

  If the key does not exist, it is created with value `suffix`.
  Returns `{:ok, new_byte_length}`.
  """
  @spec append(binary(), binary()) :: {:ok, non_neg_integer()}
  def append(key, suffix) do
    GenServer.call(shard_name(shard_for(key)), {:append, key, suffix})
  end

  @doc """
  Atomically gets the old value and sets a new value for `key`.

  Returns the old value, or `nil` if the key did not exist.
  """
  @spec getset(binary(), binary()) :: binary() | nil
  def getset(key, value) do
    GenServer.call(shard_name(shard_for(key)), {:getset, key, value})
  end

  @doc """
  Atomically gets and deletes `key`.

  Returns the value, or `nil` if the key did not exist.
  """
  @spec getdel(binary()) :: binary() | nil
  def getdel(key) do
    GenServer.call(shard_name(shard_for(key)), {:getdel, key})
  end

  @doc """
  Atomically gets the value and updates the expiry of `key`.

  `expire_at_ms` is an absolute Unix-epoch timestamp in milliseconds;
  pass `0` to persist (remove expiry). Returns the value, or `nil` if
  the key did not exist.
  """
  @spec getex(binary(), non_neg_integer()) :: binary() | nil
  def getex(key, expire_at_ms) do
    GenServer.call(shard_name(shard_for(key)), {:getex, key, expire_at_ms})
  end

  @doc """
  Atomically overwrites part of the string at `key` starting at `offset`.

  Zero-pads if the key doesn't exist or the string is shorter than offset.
  Returns `{:ok, new_byte_length}`.
  """
  @spec setrange(binary(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
  def setrange(key, offset, value) do
    GenServer.call(shard_name(shard_for(key)), {:setrange, key, offset, value})
  end

  @doc "Returns all live (non-expired, non-deleted) keys across every shard."
  @spec keys() :: [binary()]
  def keys do
    Enum.flat_map(0..(@shard_count - 1), fn i ->
      GenServer.call(shard_name(i), :keys)
    end)
  end

  @doc "Returns the count of all live keys across every shard."
  @spec dbsize() :: non_neg_integer()
  def dbsize, do: length(keys())

  @doc """
  Returns the current write version of the shard that owns `key`.

  Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
  """
  @spec get_version(binary()) :: non_neg_integer()
  def get_version(key) do
    GenServer.call(shard_name(shard_for(key)), {:get_version, key})
  end

  # -------------------------------------------------------------------
  # Native command accessors
  # -------------------------------------------------------------------

  @spec cas(binary(), binary(), binary(), non_neg_integer() | nil) :: 1 | 0 | nil
  def cas(key, expected, new_value, ttl_ms) do
    GenServer.call(shard_name(shard_for(key)), {:cas, key, expected, new_value, ttl_ms})
  end

  @spec lock(binary(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(key, owner, ttl_ms) do
    GenServer.call(shard_name(shard_for(key)), {:lock, key, owner, ttl_ms})
  end

  @spec unlock(binary(), binary()) :: 1 | {:error, binary()}
  def unlock(key, owner) do
    GenServer.call(shard_name(shard_for(key)), {:unlock, key, owner})
  end

  @spec extend(binary(), binary(), pos_integer()) :: 1 | {:error, binary()}
  def extend(key, owner, ttl_ms) do
    GenServer.call(shard_name(shard_for(key)), {:extend, key, owner, ttl_ms})
  end

  @spec ratelimit_add(binary(), pos_integer(), pos_integer(), pos_integer()) :: [term()]
  def ratelimit_add(key, window_ms, max, count) do
    GenServer.call(shard_name(shard_for(key)), {:ratelimit_add, key, window_ms, max, count})
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec list_op(binary(), term()) :: term()
  def list_op(key, {:lmove, destination, from_dir, to_dir}) do
    src_idx = shard_for(key)
    dst_idx = shard_for(destination)

    if src_idx == dst_idx do
      GenServer.call(shard_name(src_idx), {:list_op_lmove, key, destination, from_dir, to_dir})
    else
      case GenServer.call(shard_name(src_idx), {:list_op, key, {:pop_for_move, from_dir}}) do
        nil -> nil
        {:error, _} = err -> err
        element ->
          push_op = if to_dir == :left, do: {:lpush, [element]}, else: {:rpush, [element]}
          case GenServer.call(shard_name(dst_idx), {:list_op, destination, push_op}) do
            {:error, _} = err -> err
            _length -> element
          end
      end
    end
  end

  def list_op(key, operation) do
    GenServer.call(shard_name(shard_for(key)), {:list_op, key, operation})
  end
end
