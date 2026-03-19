defmodule Ferricstore.Store.Promotion do
  @moduledoc """
  Collection promotion: migrates large compound-key collections from the
  shared shard Bitcask into a dedicated per-key Bitcask instance.

  ## Background (spec section 2B.4b)

  Small collections (hashes, sets, sorted sets) are stored as compound keys
  in the shared shard Bitcask (`H:key\\0field`, `S:key\\0member`,
  `Z:key\\0member`). When any compound-key collection exceeds the
  configurable promotion threshold (default: 100 entries), it is promoted
  to a dedicated Bitcask instance stored under:

      dedicated/shard_N/{type}:{sha256_of_key}/

  where `{type}` is `hash`, `set`, or `zset`.

  Promotion is **one-way** -- once promoted, a collection stays in its
  dedicated instance even if entries are later deleted below the threshold.
  The dedicated instance is only removed when the entire key is deleted
  via `DEL` / `UNLINK`.

  ## Lists are not promoted

  Lists store all elements as a single serialized Erlang term in one
  Bitcask entry (via `ListOps`). Since there is no compound key fan-out,
  a list with 1000 elements is still a single Bitcask entry and does not
  benefit from promotion. List promotion is intentionally skipped.

  ## Promotion marker

  When a key is promoted, a marker entry `PM:redis_key` is written to the
  shared Bitcask with the type as its value (`"hash"`, `"set"`, or
  `"zset"`). This allows the shard to rediscover promoted keys on restart
  by scanning for `PM:` prefixed keys during initialization.

  ## Configuration

      config :ferricstore, :promotion_threshold, 100

  Set to `0` to disable automatic promotion entirely (no collections will
  ever be promoted).
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.CompoundKey

  require Logger

  @promotion_marker_prefix "PM:"

  @doc """
  Returns the configurable promotion threshold.

  Collections exceeding this many fields/members are promoted to a
  dedicated Bitcask. The threshold comparison is **exclusive**: a
  collection must have *more than* `threshold` entries to trigger
  promotion.

  Defaults to 100. Set to 0 to disable promotion.
  """
  @spec threshold() :: non_neg_integer()
  def threshold do
    Application.get_env(:ferricstore, :promotion_threshold, 100)
  end

  @doc """
  Computes the filesystem path for a dedicated Bitcask instance.

  The path is deterministic based on the data directory, shard index,
  data type, and a SHA-256 hash of the Redis key. Using a hash avoids
  filesystem issues with keys containing special characters.

  ## Parameters

    * `data_dir` -- the root data directory
    * `shard_index` -- zero-based shard index
    * `type` -- the data type atom (`:hash`, `:set`, `:zset`)
    * `redis_key` -- the user-facing Redis key

  ## Examples

      iex> path = Ferricstore.Store.Promotion.dedicated_path("/tmp/fs", 0, :hash, "user:123")
      iex> String.starts_with?(path, "/tmp/fs/dedicated/shard_0/hash:")
      true

  """
  @spec dedicated_path(binary(), non_neg_integer(), atom(), binary()) :: binary()
  def dedicated_path(data_dir, shard_index, type, redis_key) do
    hash = :crypto.hash(:sha256, redis_key) |> Base.encode16(case: :lower)
    type_str = Atom.to_string(type)
    Path.join([data_dir, "dedicated", "shard_#{shard_index}", "#{type_str}:#{hash}"])
  end

  @doc """
  Builds the promotion marker key for a Redis key.

  This key is stored in the shared Bitcask to persist the promotion
  status across shard restarts.

  ## Examples

      iex> Ferricstore.Store.Promotion.marker_key("user:123")
      "PM:user:123"

  """
  @spec marker_key(binary()) :: binary()
  def marker_key(redis_key), do: @promotion_marker_prefix <> redis_key

  @doc """
  Opens a dedicated Bitcask instance for a promoted key.

  Creates the directory if it does not exist, then opens the NIF store.

  ## Parameters

    * `data_dir` -- the root data directory
    * `shard_index` -- zero-based shard index
    * `type` -- the data type atom
    * `redis_key` -- the user-facing Redis key

  ## Returns

    `{:ok, nif_store_ref}` or `{:error, reason}`
  """
  @spec open_dedicated(binary(), non_neg_integer(), atom(), binary()) ::
          {:ok, reference()} | {:error, term()}
  def open_dedicated(data_dir, shard_index, type, redis_key) do
    path = dedicated_path(data_dir, shard_index, type, redis_key)
    File.mkdir_p!(path)
    NIF.new(path)
  end

  @doc """
  Promotes a hash collection from the shared Bitcask to a dedicated instance.

  Delegates to `promote_collection!/6` with type `:hash`.

  See `promote_collection!/6` for full documentation.
  """
  @spec promote_hash!(binary(), reference(), atom(), binary(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def promote_hash!(redis_key, shared_store, ets, data_dir, shard_index) do
    promote_collection!(:hash, redis_key, shared_store, ets, data_dir, shard_index)
  end

  @doc """
  Promotes a compound-key collection from the shared Bitcask to a dedicated
  instance.

  This function:

  1. Opens a new dedicated Bitcask in `dedicated/shard_N/{type}:{sha256}/`.
  2. Scans the shared ETS table for all compound keys matching the type prefix.
  3. Batch-writes all entries to the dedicated Bitcask.
  4. Deletes all compound keys (but NOT the type key) from the shared ETS
     and Bitcask.
  5. Writes a promotion marker (`PM:key`) to the shared Bitcask with the
     type string as its value.

  The type metadata key (`T:key`) remains in the shared Bitcask since the
  type registry always reads from the shared store.

  Supported types: `:hash`, `:set`, `:zset`. Lists are not promotable
  because they store all elements in a single Bitcask entry rather than
  using compound keys.

  ## Parameters

    * `type` -- the data type atom (`:hash`, `:set`, or `:zset`)
    * `redis_key` -- the Redis key being promoted
    * `shared_store` -- NIF reference for the shared shard Bitcask
    * `ets` -- the shard's ETS table
    * `data_dir` -- root data directory
    * `shard_index` -- zero-based shard index

  ## Returns

    `{:ok, dedicated_store_ref}` on success, `{:error, reason}` on failure.
  """
  @spec promote_collection!(atom(), binary(), reference(), atom(), binary(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def promote_collection!(type, redis_key, shared_store, ets, data_dir, shard_index) do
    prefix = compound_prefix_for(type, redis_key)
    type_str = CompoundKey.encode_type(type)
    type_label = type_label(type)
    now = System.os_time(:millisecond)

    # Collect all compound entries from ETS
    entries =
      :ets.foldl(
        fn {key, value, exp}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
            [{key, value, exp} | acc]
          else
            acc
          end
        end,
        [],
        ets
      )

    case open_dedicated(data_dir, shard_index, type, redis_key) do
      {:ok, dedicated_store} ->
        # Batch-write all entries to the dedicated Bitcask.
        if entries != [] do
          batch = Enum.map(entries, fn {k, v, exp} -> {k, v, exp} end)
          NIF.put_batch(dedicated_store, batch)
        end

        # Delete compound keys from shared ETS and Bitcask.
        # The type key (T:key) stays in the shared store.
        Enum.each(entries, fn {key, _value, _exp} ->
          :ets.delete(ets, key)
          NIF.delete(shared_store, key)
        end)

        # Write the promotion marker to the shared Bitcask and ETS.
        mk = marker_key(redis_key)
        NIF.put(shared_store, mk, type_str, 0)
        :ets.insert(ets, {mk, type_str, 0})

        Logger.info(
          "Promoted #{type_label} #{inspect(redis_key)} to dedicated Bitcask " <>
            "(#{length(entries)} entries, shard #{shard_index})"
        )

        {:ok, dedicated_store}

      {:error, reason} = err ->
        Logger.error(
          "Failed to promote #{type_label} #{inspect(redis_key)}: #{inspect(reason)}"
        )

        err
    end
  end

  @doc """
  Scans the shared Bitcask for promotion markers and re-opens dedicated
  instances. Called during shard initialization to restore promoted state.

  ## Parameters

    * `shared_store` -- NIF reference for the shared shard Bitcask
    * `ets` -- the shard's ETS table
    * `data_dir` -- root data directory
    * `shard_index` -- zero-based shard index

  ## Returns

    A map of `%{redis_key => dedicated_store_ref}`.
  """
  @spec recover_promoted(reference(), atom(), binary(), non_neg_integer()) :: map()
  def recover_promoted(shared_store, ets, data_dir, shard_index) do
    # Scan ETS for promotion markers (PM:key -> type_str)
    markers =
      :ets.foldl(
        fn {key, value, _exp}, acc ->
          case key do
            <<"PM:", redis_key::binary>> ->
              [{redis_key, value} | acc]

            _ ->
              acc
          end
        end,
        [],
        ets
      )

    # Also scan Bitcask keys for markers not yet warmed into ETS
    bitcask_keys = NIF.keys(shared_store)

    bitcask_markers =
      Enum.filter(bitcask_keys, &String.starts_with?(&1, @promotion_marker_prefix))
      |> Enum.map(fn <<"PM:", redis_key::binary>> ->
        {:ok, type_str} = NIF.get(shared_store, marker_key(redis_key))
        :ets.insert(ets, {marker_key(redis_key), type_str, 0})
        {redis_key, type_str}
      end)

    all_markers =
      (markers ++ bitcask_markers)
      |> Enum.uniq_by(fn {redis_key, _} -> redis_key end)

    Enum.reduce(all_markers, %{}, fn {redis_key, type_str}, acc ->
      type = CompoundKey.decode_type(type_str)

      case open_dedicated(data_dir, shard_index, type, redis_key) do
        {:ok, dedicated_store} ->
          # Warm the dedicated store's entries into ETS
          case NIF.get_all(dedicated_store) do
            {:ok, pairs} ->
              Enum.each(pairs, fn {k, v} ->
                :ets.insert(ets, {k, v, 0})
              end)

            _ ->
              :ok
          end

          Map.put(acc, redis_key, dedicated_store)

        {:error, reason} ->
          Logger.error(
            "Failed to recover promoted key #{inspect(redis_key)}: #{inspect(reason)}"
          )

          acc
      end
    end)
  end

  @doc """
  Cleans up a promoted key's dedicated Bitcask instance.

  Reads the promotion marker to determine the collection type, removes the
  marker from the shared store, and deletes the dedicated directory from
  disk.

  ## Parameters

    * `redis_key` -- the Redis key to clean up
    * `shared_store` -- NIF reference for the shared shard Bitcask
    * `ets` -- the shard's ETS table
    * `data_dir` -- root data directory
    * `shard_index` -- zero-based shard index
  """
  @spec cleanup_promoted!(binary(), reference(), atom(), binary(), non_neg_integer()) :: :ok
  def cleanup_promoted!(redis_key, shared_store, ets, data_dir, shard_index) do
    mk = marker_key(redis_key)

    # Resolve the collection type from the promotion marker so we can
    # compute the correct dedicated path. Fall back to :hash for
    # backward compatibility if the marker is missing.
    type =
      case :ets.lookup(ets, mk) do
        [{^mk, type_str, _exp}] -> CompoundKey.decode_type(type_str)
        [] ->
          case NIF.get(shared_store, mk) do
            {:ok, type_str} when is_binary(type_str) -> CompoundKey.decode_type(type_str)
            _ -> :hash
          end
      end

    type_label = type_label(type)

    # Remove promotion marker from shared store
    NIF.delete(shared_store, mk)
    :ets.delete(ets, mk)

    # Delete the dedicated directory
    path = dedicated_path(data_dir, shard_index, type, redis_key)

    if File.dir?(path) do
      File.rm_rf!(path)
    end

    Logger.debug(
      "Cleaned up promoted #{type_label} #{inspect(redis_key)} (shard #{shard_index})"
    )

    :ok
  end

  # -------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------

  # Returns the compound key prefix for the given data type and Redis key.
  @spec compound_prefix_for(atom(), binary()) :: binary()
  defp compound_prefix_for(:hash, redis_key), do: CompoundKey.hash_prefix(redis_key)
  defp compound_prefix_for(:set, redis_key), do: CompoundKey.set_prefix(redis_key)
  defp compound_prefix_for(:zset, redis_key), do: CompoundKey.zset_prefix(redis_key)

  # Returns a human-readable label for log messages.
  @spec type_label(atom()) :: binary()
  defp type_label(:hash), do: "hash"
  defp type_label(:set), do: "set"
  defp type_label(:zset), do: "zset"
end
