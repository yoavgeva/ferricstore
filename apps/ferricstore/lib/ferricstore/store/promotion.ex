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
  alias Ferricstore.Store.{CompoundKey, LFU}

  require Logger

  @promotion_marker_prefix "PM:"

  @spec threshold() :: non_neg_integer()
  def threshold do
    :persistent_term.get(:ferricstore_promotion_threshold, 100)
  rescue
    ArgumentError -> Application.get_env(:ferricstore, :promotion_threshold, 100)
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
          {:ok, binary()} | {:error, term()}
  def open_dedicated(data_dir, shard_index, type, redis_key) do
    path = dedicated_path(data_dir, shard_index, type, redis_key)
    File.mkdir_p!(path)
    active_file = Path.join(path, "00000.log")

    unless File.exists?(active_file) do
      File.touch!(active_file)
    end

    {:ok, path}
  end

  @spec promote_hash!(binary(), reference(), atom(), binary(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def promote_hash!(redis_key, shared_store, keydir, data_dir, shard_index) do
    promote_collection!(:hash, redis_key, shared_store, keydir, data_dir, shard_index)
  end

  @spec promote_collection!(atom(), binary(), binary(), atom(), binary(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def promote_collection!(type, redis_key, shard_data_path, keydir, data_dir, shard_index) do
    prefix = compound_prefix_for(type, redis_key)
    type_str = CompoundKey.encode_type(type)
    type_label = type_label(type)
    now = System.os_time(:millisecond)

    entries =
      :ets.foldl(
        fn {key, value, exp, _lfu, _fid, _off, _vsize}, acc ->
          if is_binary(key) and value != nil and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
            [{key, value, exp} | acc]
          else
            acc
          end
        end,
        [],
        keydir
      )

    case open_dedicated(data_dir, shard_index, type, redis_key) do
      {:ok, dedicated_path} ->
        if entries != [] do
          batch = Enum.map(entries, fn {k, v, exp} -> {k, v, exp} end)
          dedicated_active = find_active_file(dedicated_path)

          case NIF.v2_append_batch(dedicated_active, batch) do
            {:ok, _locations} -> :ok

            {:error, reason} ->
              Logger.error("Promotion: v2_append_batch failed for #{inspect(redis_key)}: #{inspect(reason)}")
          end
        end

        # v2: write tombstones to the shared Bitcask log for migrated keys.
        # Entries STAY in ETS so compound_scan/compound_count/compound_get
        # continue to work immediately after promotion.
        active_path = find_active_file(shard_data_path)

        Enum.each(entries, fn {key, _value, _exp} ->
          case NIF.v2_append_tombstone(active_path, key) do
            {:ok, _} -> :ok
            {:error, reason} ->
              Logger.warning("Promotion: tombstone write failed for #{inspect(key)}: #{inspect(reason)}")
          end
        end)

        mk = marker_key(redis_key)
        NIF.v2_append_record(active_path, mk, type_str, 0)
        :ets.insert(keydir, {mk, type_str, 0, LFU.initial(), 0, 0, 0})

        Logger.info(
          "Promoted #{type_label} #{inspect(redis_key)} to dedicated Bitcask " <>
            "(#{length(entries)} entries, shard #{shard_index})"
        )

        {:ok, dedicated_path}

      {:error, reason} = err ->
        Logger.error(
          "Failed to promote #{type_label} #{inspect(redis_key)}: #{inspect(reason)}"
        )

        err
    end
  end

  @spec recover_promoted(binary(), atom(), binary(), non_neg_integer()) :: map()
  def recover_promoted(shard_data_path, keydir, data_dir, shard_index) do
    # v2: promotion markers are recovered from ETS (populated by recover_keydir).
    # Use :ets.select with a match spec bound to the "PM:" prefix instead of
    # scanning every key in the keydir via :ets.foldl (memory audit L6).
    pm_prefix = "PM:"
    pm_len = byte_size(pm_prefix)

    match_spec = [
      {{:"$1", :"$2", :_, :_, :_, :_, :_},
       [{:andalso,
         {:is_binary, :"$1"},
         {:andalso,
           {:>=, {:byte_size, :"$1"}, pm_len},
           {:andalso,
             {:==, {:binary_part, :"$1", 0, pm_len}, pm_prefix},
             {:is_binary, :"$2"}}}}],
       [{{:"$1", :"$2"}}]}
    ]

    all_markers =
      :ets.select(keydir, match_spec)
      |> Enum.map(fn {full_key, value} ->
        <<"PM:", redis_key::binary>> = full_key
        {redis_key, value}
      end)
      |> Enum.uniq_by(fn {redis_key, _} -> redis_key end)

    Enum.reduce(all_markers, %{}, fn {redis_key, type_str}, acc ->
      type = CompoundKey.decode_type(type_str)

      case open_dedicated(data_dir, shard_index, type, redis_key) do
        {:ok, dedicated_path} ->
          # v2: scan dedicated log files to recover entries into ETS
          dedicated_active = find_active_file(dedicated_path)

          case NIF.v2_scan_file(dedicated_active) do
            {:ok, records} ->
              # Build a map of the latest state per key (last-writer-wins)
              final_state =
                Enum.reduce(records, %{}, fn {key, offset, value_size, expire_at_ms, is_tombstone}, acc ->
                  if is_tombstone do
                    Map.put(acc, key, :tombstone)
                  else
                    Map.put(acc, key, {:live, offset, value_size, expire_at_ms})
                  end
                end)

              Enum.each(final_state, fn
                {key, :tombstone} ->
                  :ets.delete(keydir, key)

                {key, {:live, offset, _value_size, expire_at_ms}} ->
                  # Read the actual value from disk so compound_scan finds it
                  value =
                    case NIF.v2_pread_at(dedicated_active, offset) do
                      {:ok, v} when v != nil -> v
                      _ -> nil
                    end

                  :ets.insert(keydir, {key, value, expire_at_ms, LFU.initial(), 0, offset, 0})
              end)

            _ ->
              :ok
          end

          Map.put(acc, redis_key, dedicated_path)

        {:error, reason} ->
          Logger.error(
            "Failed to recover promoted key #{inspect(redis_key)}: #{inspect(reason)}"
          )

          acc
      end
    end)
  end

  @spec cleanup_promoted!(binary(), binary(), atom(), binary(), non_neg_integer()) :: :ok
  def cleanup_promoted!(redis_key, shard_data_path, keydir, data_dir, shard_index) do
    mk = marker_key(redis_key)

    type =
      case :ets.lookup(keydir, mk) do
        [{^mk, type_str, _exp, _lfu, _fid, _off, _vsize}] when type_str != nil ->
          CompoundKey.decode_type(type_str)

        _ ->
          :hash
      end

    type_label = type_label(type)

    # v2: write tombstone for the marker key
    active_path = find_active_file(shard_data_path)

    case NIF.v2_append_tombstone(active_path, mk) do
      {:ok, _} -> :ok
      {:error, reason} ->
        Logger.warning("Promotion cleanup: tombstone write failed for marker #{inspect(mk)}: #{inspect(reason)}")
    end

    :ets.delete(keydir, mk)

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

  # Finds the active (highest numbered) .log file in a shard data directory.
  defp find_active_file(shard_data_path) do
    case File.ls(shard_data_path) do
      {:ok, files} ->
        max_id =
          files
          |> Enum.filter(&String.ends_with?(&1, ".log"))
          |> Enum.map(fn name -> name |> String.trim_trailing(".log") |> String.to_integer() end)
          |> Enum.max(fn -> 0 end)

        Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(max_id), 5, "0")}.log")

      _ ->
        Path.join(shard_data_path, "00000.log")
    end
  end
end
