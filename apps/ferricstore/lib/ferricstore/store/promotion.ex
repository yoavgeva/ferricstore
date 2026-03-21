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

  @spec threshold() :: non_neg_integer()
  def threshold do
    Application.get_env(:ferricstore, :promotion_threshold, 100)
  end

  @spec dedicated_path(binary(), non_neg_integer(), atom(), binary()) :: binary()
  def dedicated_path(data_dir, shard_index, type, redis_key) do
    hash = :crypto.hash(:sha256, redis_key) |> Base.encode16(case: :lower)
    type_str = Atom.to_string(type)
    Path.join([data_dir, "dedicated", "shard_#{shard_index}", "#{type_str}:#{hash}"])
  end

  @spec marker_key(binary()) :: binary()
  def marker_key(redis_key), do: @promotion_marker_prefix <> redis_key

  @spec open_dedicated(binary(), non_neg_integer(), atom(), binary()) ::
          {:ok, reference()} | {:error, term()}
  def open_dedicated(data_dir, shard_index, type, redis_key) do
    path = dedicated_path(data_dir, shard_index, type, redis_key)
    File.mkdir_p!(path)
    NIF.new(path)
  end

  @spec promote_hash!(binary(), reference(), atom(), atom(), binary(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def promote_hash!(redis_key, shared_store, keydir, hot_cache, data_dir, shard_index) do
    promote_collection!(:hash, redis_key, shared_store, keydir, hot_cache, data_dir, shard_index)
  end

  @spec promote_collection!(atom(), binary(), reference(), atom(), atom(), binary(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def promote_collection!(type, redis_key, shared_store, keydir, hot_cache, data_dir, shard_index) do
    prefix = compound_prefix_for(type, redis_key)
    type_str = CompoundKey.encode_type(type)
    type_label = type_label(type)
    now = System.os_time(:millisecond)

    entries =
      :ets.foldl(
        fn {key, exp}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
            case :ets.lookup(hot_cache, key) do
              [{^key, value, _access_ms}] -> [{key, value, exp} | acc]
              [] -> acc
            end
          else
            acc
          end
        end,
        [],
        keydir
      )

    case open_dedicated(data_dir, shard_index, type, redis_key) do
      {:ok, dedicated_store} ->
        if entries != [] do
          batch = Enum.map(entries, fn {k, v, exp} -> {k, v, exp} end)
          NIF.put_batch(dedicated_store, batch)
        end

        Enum.each(entries, fn {key, _value, _exp} ->
          :ets.delete(keydir, key)
          :ets.delete(hot_cache, key)
          NIF.delete(shared_store, key)
        end)

        mk = marker_key(redis_key)
        NIF.put(shared_store, mk, type_str, 0)
        :ets.insert(keydir, {mk, 0})
        :ets.insert(hot_cache, {mk, type_str, System.os_time(:millisecond)})

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

  @spec recover_promoted(reference(), atom(), atom(), binary(), non_neg_integer()) :: map()
  def recover_promoted(shared_store, keydir, hot_cache, data_dir, shard_index) do
    markers =
      :ets.foldl(
        fn {key, _exp}, acc ->
          case key do
            <<"PM:", redis_key::binary>> ->
              case :ets.lookup(hot_cache, key) do
                [{^key, type_str, _access_ms}] -> [{redis_key, type_str} | acc]
                [] -> acc
              end

            _ ->
              acc
          end
        end,
        [],
        keydir
      )

    bitcask_keys = NIF.keys(shared_store)

    bitcask_markers =
      Enum.filter(bitcask_keys, &String.starts_with?(&1, @promotion_marker_prefix))
      |> Enum.map(fn <<"PM:", redis_key::binary>> ->
        {:ok, type_str} = NIF.get_zero_copy(shared_store, marker_key(redis_key))
        :ets.insert(keydir, {marker_key(redis_key), 0})
        :ets.insert(hot_cache, {marker_key(redis_key), type_str, System.os_time(:millisecond)})
        {redis_key, type_str}
      end)

    all_markers =
      (markers ++ bitcask_markers)
      |> Enum.uniq_by(fn {redis_key, _} -> redis_key end)

    Enum.reduce(all_markers, %{}, fn {redis_key, type_str}, acc ->
      type = CompoundKey.decode_type(type_str)

      case open_dedicated(data_dir, shard_index, type, redis_key) do
        {:ok, dedicated_store} ->
          case NIF.get_all(dedicated_store) do
            {:ok, pairs} ->
              now_ms = System.os_time(:millisecond)

              Enum.each(pairs, fn {k, v} ->
                :ets.insert(keydir, {k, 0})
                :ets.insert(hot_cache, {k, v, now_ms})
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

  @spec cleanup_promoted!(binary(), reference(), atom(), atom(), binary(), non_neg_integer()) :: :ok
  def cleanup_promoted!(redis_key, shared_store, keydir, hot_cache, data_dir, shard_index) do
    mk = marker_key(redis_key)

    type =
      case :ets.lookup(hot_cache, mk) do
        [{^mk, type_str, _access_ms}] -> CompoundKey.decode_type(type_str)
        [] ->
          case NIF.get_zero_copy(shared_store, mk) do
            {:ok, type_str} when is_binary(type_str) -> CompoundKey.decode_type(type_str)
            _ -> :hash
          end
      end

    type_label = type_label(type)

    NIF.delete(shared_store, mk)
    :ets.delete(keydir, mk)
    :ets.delete(hot_cache, mk)

    path = dedicated_path(data_dir, shard_index, type, redis_key)

    if File.dir?(path) do
      File.rm_rf!(path)
    end

    Logger.debug(
      "Cleaned up promoted #{type_label} #{inspect(redis_key)} (shard #{shard_index})"
    )

    :ok
  end

  @spec compound_prefix_for(atom(), binary()) :: binary()
  defp compound_prefix_for(:hash, redis_key), do: CompoundKey.hash_prefix(redis_key)
  defp compound_prefix_for(:set, redis_key), do: CompoundKey.set_prefix(redis_key)
  defp compound_prefix_for(:zset, redis_key), do: CompoundKey.zset_prefix(redis_key)

  @spec type_label(atom()) :: binary()
  defp type_label(:hash), do: "hash"
  defp type_label(:set), do: "set"
  defp type_label(:zset), do: "zset"
end
