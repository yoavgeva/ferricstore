defmodule Ferricstore.Commands.TopK do
  @moduledoc """
  Handles Top-K commands: TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY,
  TOPK.QUERY, TOPK.LIST, TOPK.INFO.

  ## Storage architecture (mmap-backed)

  Each TopK structure is stored as an mmap file at
  `prob/shard_N/key.topk`. The Bitcask value for the key is just the
  file path string (no data duplication). On creation the file is
  created and mmap'd; on subsequent operations the mmap handle is
  retrieved from a per-process cache (ETS) or re-opened from the
  stored path.

  On restart the keydir is rebuilt from Bitcask, the path is recovered,
  and the mmap file is re-opened on first access.

  ## Supported commands

    * `TOPK.RESERVE key k [width depth decay]` -- create a Top-K tracker
    * `TOPK.ADD key element [element ...]` -- add elements (returns evicted items)
    * `TOPK.INCRBY key element count [element count ...]` -- increment by amount
    * `TOPK.QUERY key element [element ...]` -- check if elements are in top-K
    * `TOPK.LIST key [WITHCOUNT]` -- list top-K elements
    * `TOPK.COUNT key element [element ...]` -- return CMS count estimate
    * `TOPK.INFO key` -- return structure metadata
  """

  alias Ferricstore.Bitcask.NIF

  # Default CMS dimensions for Top-K when not specified
  @default_width 8
  @default_depth 7
  @default_decay 0.9

  @doc """
  Handles a TOPK command.

  ## Parameters

    - `cmd` -- uppercased command name (e.g. `"TOPK.RESERVE"`)
    - `args` -- list of string arguments
    - `store` -- injected store map with `get`, `put`, `exists?`, `prob_dir` callbacks

  ## Returns

  Plain Elixir term: `:ok`, list, map, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TOPK.RESERVE key k [width depth decay]
  # ---------------------------------------------------------------------------

  def handle("TOPK.RESERVE", [key, k_str], store) do
    do_reserve(key, k_str, @default_width, @default_depth, @default_decay, store)
  end

  def handle("TOPK.RESERVE", [key, k_str, width_str, depth_str, decay_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth"),
         {:ok, decay} <- parse_decay(decay_str) do
      do_reserve(key, k_str, width, depth, decay, store)
    end
  end

  def handle("TOPK.RESERVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.reserve' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.ADD key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("TOPK.ADD", [key | elements], store) when elements != [] do
    with {:ok, ref} <- open_topk(store, key) do
      NIF.topk_file_add(ref, elements)
    end
  end

  def handle("TOPK.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.add' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.INCRBY key element count [element count ...]
  # ---------------------------------------------------------------------------

  def handle("TOPK.INCRBY", [key | rest], store) when rest != [] do
    with {:ok, pairs} <- parse_element_count_pairs(rest),
         {:ok, ref} <- open_topk(store, key) do
      NIF.topk_file_incrby(ref, pairs)
    end
  end

  def handle("TOPK.INCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.incrby' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.QUERY key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("TOPK.QUERY", [key | elements], store) when elements != [] do
    with {:ok, ref} <- open_topk(store, key) do
      NIF.topk_file_query(ref, elements)
    end
  end

  def handle("TOPK.QUERY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.query' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.LIST key [WITHCOUNT]
  # ---------------------------------------------------------------------------

  def handle("TOPK.LIST", [key], store) do
    with {:ok, ref} <- open_topk(store, key) do
      case NIF.topk_file_list(ref) do
        {:error, reason} -> {:error, "ERR topk list failed: #{inspect(reason)}"}
        result -> result
      end
    end
  end

  def handle("TOPK.LIST", [key, "WITHCOUNT"], store) do
    with {:ok, ref} <- open_topk(store, key),
         items when is_list(items) <- NIF.topk_file_list(ref),
         counts when is_list(counts) <- NIF.topk_file_count(ref, items) do
      Enum.zip(items, counts) |> Enum.flat_map(fn {elem, count} -> [elem, count] end)
    else
      {:error, reason} -> {:error, "ERR topk list failed: #{inspect(reason)}"}
      other -> {:error, "ERR topk list failed: #{inspect(other)}"}
    end
  end

  def handle("TOPK.LIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.list' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.INFO key
  # ---------------------------------------------------------------------------

  def handle("TOPK.INFO", [key], store) do
    with {:ok, ref} <- open_topk(store, key) do
      {k, width, depth, decay} = NIF.topk_file_info(ref)
      ["k", k, "width", width, "depth", depth, "decay", decay]
    end
  end

  def handle("TOPK.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Private: mmap lifecycle
  # ---------------------------------------------------------------------------

  defp do_reserve(key, k_str, width, depth, decay, store) do
    with {:ok, k} <- parse_pos_integer(k_str, "k"),
         :ok <- check_not_exists(store, key) do
      # Build the mmap file path
      prob_dir = store.prob_dir.()
      path = Path.join(prob_dir, key <> ".topk")

      case NIF.topk_create_file(path, k, width, depth, decay * 1.0) do
        {:ok, ref} ->
          # Store path as Bitcask value
          store.put.(key, {:topk_path, path}, 0)
          # Cache the resource handle
          cache_put(key, ref)
          :ok

        {:error, reason} ->
          {:error, "ERR TOPK: #{reason}"}
      end
    end
  end

  defp open_topk(store, key) do
    # Check cache first
    case cache_get(key) do
      nil ->
        # Read path from Bitcask
        case store.get.(key) do
          nil ->
            # Key not in store -- check if another type's registry owns it
            if key_held_by_other_type?(key, store) do
              {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
            else
              {:error, "ERR TOPK: key does not exist"}
            end

          {:topk_path, path} ->
            case NIF.topk_open_file(path) do
              {:ok, ref} ->
                cache_put(key, ref)
                {:ok, ref}

              {:error, reason} ->
                {:error, "ERR TOPK: #{reason}"}
            end

          _ ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end

      ref ->
        {:ok, ref}
    end
  end

  defp check_not_exists(store, key) do
    if store.exists?.(key), do: {:error, "ERR item already exists"}, else: :ok
  end

  # Checks whether a key is held by another probabilistic data structure type
  defp key_held_by_other_type?(key, store) do
    # Check CMS registry
    cms_found =
      case Map.get(store, :cms_registry) do
        %{get: get_fn} ->
          case get_fn.(key) do
            nil -> false
            _ -> true
          end
        _ -> false
      end

    if cms_found do
      true
    else
      # Check TDigest path in main store
      case store.get.(key) do
        {:tdigest_path, _} -> true
        _ -> false
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: per-process ETS cache for mmap handles
  # ---------------------------------------------------------------------------

  @cache_table :ferricstore_topk_cache

  defp cache_get(key) do
    case :ets.whereis(@cache_table) do
      :undefined -> nil
      _ref ->
        case :ets.lookup(@cache_table, key) do
          [{^key, ref}] -> ref
          [] -> nil
        end
    end
  end

  defp cache_put(key, ref) do
    case :ets.whereis(@cache_table) do
      :undefined ->
        :ets.new(@cache_table, [:set, :public, :named_table, {:read_concurrency, true}])
        :ets.insert(@cache_table, {key, ref})

      _ref ->
        :ets.insert(@cache_table, {key, ref})
    end
  end

  # ---------------------------------------------------------------------------
  # Private: argument parsing
  # ---------------------------------------------------------------------------

  defp parse_pos_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} must be a positive integer"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  defp parse_decay(str) do
    case Float.parse(str) do
      {f, ""} when f >= 0.0 and f <= 1.0 -> {:ok, f}
      {_f, ""} -> {:error, "ERR decay must be between 0 and 1"}
      _ -> {:error, "ERR decay is not a valid number"}
    end
  end

  defp parse_element_count_pairs(args) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'topk.incrby' command"}
    else
      do_parse_pairs(args)
    end
  end

  defp do_parse_pairs(args) do
    result =
      args
      |> Enum.chunk_every(2)
      |> Enum.reduce_while([], fn [element, count_str], acc ->
        case parse_count(count_str) do
          {:ok, count} -> {:cont, [{element, count} | acc]}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case result do
      {:error, _} = err -> err
      list -> {:ok, Enum.reverse(list)}
    end
  end

  defp parse_count(str) do
    case Integer.parse(str) do
      {count, ""} when count >= 1 -> {:ok, count}
      _ -> {:error, "ERR TOPK: invalid count value"}
    end
  end
end
