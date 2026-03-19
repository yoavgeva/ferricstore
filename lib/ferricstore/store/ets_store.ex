defmodule Ferricstore.Store.EtsStore do
  @moduledoc """
  Two-table ETS helpers for the shard keydir/hot-cache split (spec 2.4).

  Each shard uses two public named ETS tables:

    * `keydir_N` -- `{key, expire_at_ms}` tuples. Used for fast existence and
      TTL checks on the Router hot path without loading the full value into the
      caller's heap.

    * `hot_cache_N` -- `{key, value, expire_at_ms}` tuples. Holds the actual
      values for hot reads. Only consulted after the keydir confirms the key is
      live.

  The legacy `shard_ets_N` name is kept as an alias pointing at the keydir
  table for backward compatibility with code that reads `state.ets`.

  ## Public API

    * `init_tables/1` -- creates (or resets) both tables for shard N.
    * `insert/5` -- inserts into both keydir and hot_cache.
    * `delete/3` -- removes from both keydir and hot_cache.
    * `lookup/4` -- checks keydir for existence/TTL, then reads value from
      hot_cache.
  """

  @doc """
  Creates (or resets) the two ETS tables for shard `index`.

  Returns `{keydir_name, hot_cache_name}`.
  """
  @spec init_tables(non_neg_integer()) :: {atom(), atom()}
  def init_tables(index) do
    keydir_name = :"keydir_#{index}"
    hot_cache_name = :"hot_cache_#{index}"

    keydir_name = init_or_reset(keydir_name)
    hot_cache_name = init_or_reset(hot_cache_name)

    {keydir_name, hot_cache_name}
  end

  @doc """
  Inserts a key into both the keydir and hot_cache tables.

  The keydir receives `{key, expire_at_ms}` (no value -- lightweight for
  existence/TTL checks). The hot_cache receives the full
  `{key, value, expire_at_ms}` tuple.
  """
  @spec insert(atom(), atom(), binary(), binary(), non_neg_integer()) :: true
  def insert(keydir, hot_cache, key, value, expire_at_ms) do
    :ets.insert(keydir, {key, expire_at_ms})
    :ets.insert(hot_cache, {key, value, expire_at_ms})
  end

  @doc """
  Deletes a key from both the keydir and hot_cache tables.
  """
  @spec delete(atom(), atom(), binary()) :: true
  def delete(keydir, hot_cache, key) do
    :ets.delete(keydir, key)
    :ets.delete(hot_cache, key)
  end

  @doc """
  Looks up a key using the two-table pattern.

  1. Checks the keydir for existence and TTL.
  2. If the key is live, reads the value from hot_cache.
  3. If expired, evicts from both tables.

  Returns:
    * `{:hit, value, expire_at_ms}` -- key is live
    * `:expired` -- key was found but past TTL (evicted from both tables)
    * `:miss` -- key not in keydir
  """
  @spec lookup(atom(), atom(), binary()) ::
          {:hit, binary(), non_neg_integer()} | :expired | :miss
  def lookup(keydir, hot_cache, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(keydir, key) do
      [{^key, 0}] ->
        # No expiry -- read value from hot_cache
        read_hot_cache(hot_cache, key, 0)

      [{^key, exp}] when exp > now ->
        # Not yet expired
        read_hot_cache(hot_cache, key, exp)

      [{^key, _exp}] ->
        # Expired -- evict from both tables
        :ets.delete(keydir, key)
        :ets.delete(hot_cache, key)
        :expired

      [] ->
        :miss
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp init_or_reset(table_name) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [:set, :public, :named_table])

      _ref ->
        :ets.delete_all_objects(table_name)
        table_name
    end
  end

  defp read_hot_cache(hot_cache, key, expire_at_ms) do
    case :ets.lookup(hot_cache, key) do
      [{^key, value, _exp}] ->
        {:hit, value, expire_at_ms}

      [] ->
        # Key is in keydir but not in hot_cache (should not happen in normal
        # operation, but handle gracefully).
        :miss
    end
  end
end
