defmodule Ferricstore.Store.PrefixIndex do
  @moduledoc """
  Maintains a per-shard ETS bag table (`prefix_keys_N`) that indexes keys by
  the text before the first `:` delimiter.

  This enables `SCAN MATCH 'prefix:*'` and `KEYS 'prefix:*'` to resolve in
  O(matching keys) instead of O(total keys) by looking up only the relevant
  prefix bucket.

  ## ETS layout

  Each shard owns a `:bag` table named `prefix_keys_N` where N is the shard
  index. Entries are `{prefix, key}` tuples. Because the table is a bag,
  duplicate `{prefix, key}` pairs are avoided by deleting the old entry
  before inserting (via `delete_object`).

  ## Integration

  The shard GenServer calls `track/3` on every `ets_insert` and `untrack/3`
  on every `ets_delete_key`. The `keys_for_prefix/2` function returns all
  keys in the bag matching a given prefix.

  Keys without a `:` delimiter are not indexed (no prefix bucket).
  """

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Extracts the prefix portion of a key (text before the first `:`).

  Returns `nil` if the key does not contain a `:`.

  ## Examples

      iex> Ferricstore.Store.PrefixIndex.extract_prefix("session:abc123")
      "session"

      iex> Ferricstore.Store.PrefixIndex.extract_prefix("no_colon")
      nil

      iex> Ferricstore.Store.PrefixIndex.extract_prefix(":leading")
      ""

  """
  @spec extract_prefix(binary()) :: binary() | nil
  def extract_prefix(""), do: nil

  def extract_prefix(key) when is_binary(key) do
    case :binary.split(key, ":") do
      [_whole] -> nil
      [prefix, _rest] -> prefix
    end
  end

  @doc """
  Creates the prefix index ETS table for the given shard index.

  If the table already exists (e.g. after a shard restart), it is cleared
  and reused.

  Returns the table name atom.
  """
  @spec create_table(non_neg_integer()) :: atom()
  def create_table(index) do
    table_name = table_name(index)

    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [:bag, :public, :named_table, {:read_concurrency, true}, {:write_concurrency, true}])

      _ref ->
        :ets.delete_all_objects(table_name)
        table_name
    end
  end

  @doc """
  Returns the ETS table name for the prefix index of shard `index`.
  """
  @spec table_name(non_neg_integer()) :: atom()
  def table_name(index), do: :"prefix_keys_#{index}"

  @doc """
  Tracks a key in the prefix index. Extracts the prefix from the key and
  inserts a `{prefix, key}` entry into the shard's prefix bag table.

  No-op if the key has no `:` delimiter. Handles duplicate inserts safely
  by deleting any existing `{prefix, key}` entry first.
  """
  @spec track(atom(), binary(), non_neg_integer()) :: :ok
  def track(prefix_table, key, _shard_index) do
    case extract_prefix(key) do
      nil ->
        :ok

      prefix ->
        # Delete existing entry first to prevent duplicates in the bag.
        :ets.delete_object(prefix_table, {prefix, key})
        :ets.insert(prefix_table, {prefix, key})
        :ok
    end
  end

  @doc """
  Removes a key from the prefix index. Extracts the prefix from the key
  and deletes the `{prefix, key}` entry from the shard's prefix bag table.

  No-op if the key has no `:` delimiter.
  """
  @spec untrack(atom(), binary(), non_neg_integer()) :: :ok
  def untrack(prefix_table, key, _shard_index) do
    case extract_prefix(key) do
      nil ->
        :ok

      prefix ->
        :ets.delete_object(prefix_table, {prefix, key})
        :ok
    end
  end

  @doc """
  Returns all keys in the prefix index matching `prefix`, filtering out
  expired entries by checking the keydir table.

  ## Parameters

    * `prefix_table` -- the `prefix_keys_N` ETS bag table
    * `keydir` -- the `keydir_N` ETS set table (for expiry checks)
    * `prefix` -- the prefix string to look up

  ## Returns

  A list of binary keys that have the given prefix and are not expired.
  """
  @spec keys_for_prefix(atom(), atom(), binary()) :: [binary()]
  def keys_for_prefix(prefix_table, keydir, prefix) do
    now = System.os_time(:millisecond)

    try do
      :ets.lookup(prefix_table, prefix)
      |> Enum.reduce([], fn {_prefix, key}, acc ->
        case :ets.lookup(keydir, key) do
          # No expiry set -- live
          [{^key, 0}] -> [key | acc]
          # Has TTL, not yet expired -- live
          [{^key, exp}] when exp > now -> [key | acc]
          # Has TTL, expired -- skip
          [{^key, _exp}] -> acc
          # Not in keydir (cold key, loaded from Bitcask at init but not
          # yet warmed into ETS). Considered live (Bitcask keys have no
          # TTL until warmed).
          [] -> [key | acc]
        end
      end)
    rescue
      ArgumentError -> []
    end
  end

  @doc """
  Rebuilds the prefix index from the keydir ETS table.

  Called during shard initialization to reconstruct the prefix index from
  the keys already present in the keydir (e.g. after a crash recovery where
  keys were loaded from Bitcask into the keydir).
  """
  @spec rebuild_from_keydir(atom(), atom()) :: :ok
  def rebuild_from_keydir(prefix_table, keydir) do
    :ets.foldl(
      fn {key, _exp}, :ok ->
        case extract_prefix(key) do
          nil -> :ok
          prefix ->
            :ets.insert(prefix_table, {prefix, key})
            :ok
        end
      end,
      :ok,
      keydir
    )
  end

  @doc """
  Detects whether a glob pattern is a simple prefix match of the form
  `prefix:*` (literal prefix followed by `:*` with no other glob chars).

  Returns `{:prefix_match, prefix}` if so, or `:not_prefix_match` otherwise.

  ## Examples

      iex> Ferricstore.Store.PrefixIndex.detect_prefix_pattern("session:*")
      {:prefix_match, "session"}

      iex> Ferricstore.Store.PrefixIndex.detect_prefix_pattern("user:*:name")
      :not_prefix_match

      iex> Ferricstore.Store.PrefixIndex.detect_prefix_pattern("*")
      :not_prefix_match

  """
  @spec detect_prefix_pattern(binary()) :: {:prefix_match, binary()} | :not_prefix_match
  def detect_prefix_pattern(pattern) when is_binary(pattern) do
    # Pattern must end with ":*" and the prefix part must have no glob chars
    case :binary.split(pattern, ":*") do
      [prefix, ""] ->
        if String.contains?(prefix, ["*", "?", "[", "]"]) do
          :not_prefix_match
        else
          {:prefix_match, prefix}
        end

      _ ->
        :not_prefix_match
    end
  end
end
