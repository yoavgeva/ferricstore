defmodule Ferricstore.Store.HnswRegistry do
  @moduledoc """
  Global in-memory registry for HNSW vector indices.

  Each entry maps `{shard_index, collection_name}` to the Rust NIF
  `HnswResource` reference. The registry is a named ETS table so that
  command handlers (which run in arbitrary processes) can look up the
  index without messaging a GenServer.

  ## Lifecycle

    * The table is created once during application startup
      (`create_table/0`).
    * `VCREATE` registers a freshly created HNSW index.
    * `VADD` / `VDEL` look up the index and mutate it in-place (the NIF
      resource is protected by an internal Mutex).
    * `VSEARCH` looks up the index and performs the search.
    * On shard restart, `rebuild_for_shard/3` scans the Bitcask store for
      shard-local metadata sentinel keys, recreates each HNSW index, and
      replays all stored vectors into it.
  """

  alias Ferricstore.Bitcask.NIF

  @table :ferricstore_hnsw_indices

  # Must match the sentinel used in Vector module
  @hnsw_meta_sentinel "__hnsw_meta__"

  @doc """
  Creates the global ETS table. Idempotent -- safe to call multiple times.
  """
  @spec create_table() :: :ok
  def create_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:set, :public, :named_table, {:read_concurrency, true}])
        :ok

      _ref ->
        :ok
    end
  end

  @doc """
  Registers an HNSW NIF resource for a collection on a given shard.
  """
  @spec register(non_neg_integer(), binary(), reference()) :: :ok
  def register(shard_index, collection, hnsw_ref) do
    :ets.insert(@table, {{shard_index, collection}, hnsw_ref})
    :ok
  end

  @doc """
  Looks up the HNSW NIF resource for a collection on a given shard.
  Returns `nil` if not registered.
  """
  @spec lookup(non_neg_integer(), binary()) :: reference() | nil
  def lookup(shard_index, collection) do
    case :ets.lookup(@table, {shard_index, collection}) do
      [{{^shard_index, ^collection}, ref}] -> ref
      [] -> nil
    end
  end

  @doc """
  Removes the HNSW index for a collection on a given shard.
  """
  @spec unregister(non_neg_integer(), binary()) :: :ok
  def unregister(shard_index, collection) do
    :ets.delete(@table, {shard_index, collection})
    :ok
  end

  @doc """
  Removes all HNSW indices for a given shard.
  Called before rebuilding on shard restart.
  """
  @spec clear_shard(non_neg_integer()) :: :ok
  def clear_shard(shard_index) do
    # Delete all entries where the first element of the key tuple matches shard_index
    :ets.select_delete(@table, [
      {{{shard_index, :_}, :_}, [], [true]}
    ])

    :ok
  end

  @doc """
  Rebuilds all HNSW indices for a shard from its Bitcask store.

  Scans for shard-local metadata sentinel keys (`V:collection\\0__hnsw_meta__`)
  to discover collections whose vectors live on this shard. Creates fresh
  HNSW indices and replays all stored vectors into each index.

  This approach avoids cross-shard reads during init -- the metadata sentinel
  is stored on the same shard as the vectors by VCREATE (via compound_put).

  ## Parameters

    * `store` -- the Bitcask NIF store resource for this shard
    * `shard_index` -- the shard index
    * `get_fn` -- function `(key) -> value | nil` to read from Bitcask
  """
  @spec rebuild_for_shard(reference(), non_neg_integer(), (binary() -> binary() | nil)) :: :ok
  def rebuild_for_shard(store, shard_index, get_fn) do
    create_table()
    clear_shard(shard_index)

    # Discover all keys in this shard's Bitcask
    all_keys = NIF.keys(store)

    # Find shard-local metadata sentinel keys.
    # These are compound keys of the form: V:collection\0__hnsw_meta__
    sentinel_suffix = <<0>> <> @hnsw_meta_sentinel

    sentinel_keys =
      all_keys
      |> Enum.filter(fn key ->
        String.starts_with?(key, "V:") and String.ends_with?(key, sentinel_suffix)
      end)

    # For each collection, rebuild the HNSW index
    Enum.each(sentinel_keys, fn sentinel_key ->
      # Extract collection name: strip "V:" prefix and "\0__hnsw_meta__" suffix
      collection =
        sentinel_key
        |> String.replace_leading("V:", "")
        |> String.replace_trailing(sentinel_suffix, "")

      case get_fn.(sentinel_key) do
        nil ->
          :ok

        encoded ->
          {dims, metric, m, ef} = :erlang.binary_to_term(encoded)
          metric_str = Atom.to_string(metric)

          case NIF.hnsw_new(dims, m, ef, metric_str) do
            {:ok, hnsw_ref} ->
              # Register the index
              register(shard_index, collection, hnsw_ref)

              # Find all vector keys for this collection (V:collection\0key)
              # excluding the sentinel key itself
              vec_prefix = "V:" <> collection <> <<0>>

              vector_keys =
                all_keys
                |> Enum.filter(fn key ->
                  String.starts_with?(key, vec_prefix) and key != sentinel_key
                end)

              # Replay vectors into the HNSW index
              Enum.each(vector_keys, fn vec_key ->
                case get_fn.(vec_key) do
                  nil ->
                    :ok

                  vec_encoded ->
                    floats = :erlang.binary_to_term(vec_encoded)
                    # Extract the sub-key after the null byte separator
                    sub_key =
                      case :binary.split(vec_key, <<0>>) do
                        [_prefix, k] -> k
                        _ -> vec_key
                      end

                    NIF.hnsw_add(hnsw_ref, sub_key, floats)
                end
              end)

            {:error, _reason} ->
              :ok
          end
      end
    end)

    :ok
  end
end
