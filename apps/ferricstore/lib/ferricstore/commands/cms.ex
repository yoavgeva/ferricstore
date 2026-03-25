defmodule Ferricstore.Commands.CMS do
  @moduledoc """
  Count-Min Sketch commands backed by mmap NIF resources.

  Each CMS is a memory-mapped file on disk managed by a Rust NIF.
  Files live at `data_dir/prob/shard_N/KEY.cms`. The mmap handle (NIF
  resource) is cached in a per-shard ETS table via `CmsRegistry` so that
  repeated operations avoid re-opening the file.

  ## Supported Commands

    * `CMS.INITBYDIM key width depth` -- creates a new CMS
    * `CMS.INITBYPROB key error probability` -- creates sized by target accuracy
    * `CMS.INCRBY key element count [element count ...]` -- increments
    * `CMS.QUERY key element [element ...]` -- queries counts
    * `CMS.MERGE dst numkeys src [src ...] [WEIGHTS w ...]` -- merges
    * `CMS.INFO key` -- returns sketch metadata

  ## Resource caching

  On first access, the NIF resource is opened from the `.cms` file and
  cached in `CmsRegistry`. Subsequent operations on the same key reuse
  the cached handle.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CmsRegistry, Router}

  # -------------------------------------------------------------------
  # Public command handler
  # -------------------------------------------------------------------

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CMS.INITBYDIM", [key, width_str, depth_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth"),
         :ok <- check_not_exists(store, key) do
      create_cms(key, width, depth, store)
    end
  end

  def handle("CMS.INITBYDIM", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.initbydim' command"}

  def handle("CMS.INITBYPROB", [key, error_str, prob_str], store) do
    with {:ok, error} <- parse_pos_float(error_str, "error"),
         {:ok, prob} <- parse_prob_float(prob_str, "probability"),
         :ok <- check_not_exists(store, key) do
      width = ceil(:math.exp(1) / error)
      depth = ceil(:math.log(1.0 / prob))
      create_cms(key, width, depth, store)
    end
  end

  def handle("CMS.INITBYPROB", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.initbyprob' command"}

  def handle("CMS.INCRBY", [key | rest], store) when rest != [] do
    with {:ok, pairs} <- parse_element_count_pairs(rest),
         {:ok, ref} <- get_sketch(store, key) do
      items = Enum.map(pairs, fn {element, count} -> {element, count} end)

      case NIF.cms_incrby(ref, items) do
        {:ok, counts} -> counts
        {:error, reason} -> {:error, "ERR CMS incrby failed: #{reason}"}
      end
    end
  end

  def handle("CMS.INCRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.incrby' command"}

  def handle("CMS.QUERY", [key | elements], store) when elements != [] do
    with {:ok, ref} <- get_sketch(store, key) do
      case NIF.cms_query(ref, elements) do
        {:ok, counts} -> counts
        {:error, reason} -> {:error, "ERR CMS query failed: #{reason}"}
      end
    end
  end

  def handle("CMS.QUERY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.query' command"}

  def handle("CMS.MERGE", [dst, numkeys_str | rest], store) do
    with {:ok, numkeys} <- parse_pos_integer(numkeys_str, "numkeys"),
         {:ok, src_keys, weights} <- parse_merge_args(rest, numkeys),
         {:ok, src_refs} <- load_source_sketches(store, src_keys),
         {:ok, src_infos} <- get_sketch_infos(src_refs),
         :ok <- validate_merge_dimensions(src_infos) do
      {first_w, first_d, _} = hd(src_infos)
      apply_merge_to_dst(store, dst, first_w, first_d, src_refs, weights)
    end
  end

  def handle("CMS.MERGE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.merge' command"}

  def handle("CMS.INFO", [key], store) do
    with {:ok, ref} <- get_sketch(store, key) do
      case NIF.cms_info(ref) do
        {:ok, {width, depth, count}} ->
          ["width", width, "depth", depth, "count", count]

        {:error, reason} ->
          {:error, "ERR CMS info failed: #{reason}"}
      end
    end
  end

  def handle("CMS.INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.info' command"}

  # ---------------------------------------------------------------------------
  # Deletion (called from DEL / UNLINK handlers)
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok
  def nif_delete(key, store) do
    case resolve_registry(store) do
      {:ets, index, _data_dir} ->
        CmsRegistry.delete(index, key)

      {:callback, registry} ->
        case registry.get.(key) do
          nil ->
            :ok

          {resource, _meta} ->
            NIF.cms_close(resource)
            registry.delete.(key)
            :ok
        end
    end
  end

  # Internal API for Top-K
  @doc false
  def new_sketch(width, depth, path) do
    case NIF.cms_create_file(path, width, depth) do
      {:ok, ref} -> ref
      {:error, reason} -> raise "CMS create failed: #{reason}"
    end
  end

  @doc false
  def increment(ref, element, count) do
    case NIF.cms_incrby(ref, [{element, count}]) do
      {:ok, [min_count]} -> {ref, min_count}
      {:error, reason} -> raise "CMS incrby failed: #{reason}"
    end
  end

  @doc false
  def estimate(ref, element) do
    case NIF.cms_query(ref, [element]) do
      {:ok, [count]} -> count
      {:error, reason} -> raise "CMS query failed: #{reason}"
    end
  end

  @doc false
  def sketch_info(ref) do
    case NIF.cms_info(ref) do
      {:ok, info} -> info
      {:error, reason} -> raise "CMS info failed: #{reason}"
    end
  end

  # ---------------------------------------------------------------------------
  # Private: registry resolution
  # ---------------------------------------------------------------------------

  @spec resolve_registry(map()) ::
          {:ets, non_neg_integer(), binary()}
          | {:callback, map()}
  defp resolve_registry(%{cms_registry: registry}) when is_map(registry) do
    {:callback, registry}
  end

  defp resolve_registry(_store) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    {:ets, nil, data_dir}
  end

  @spec resolve_registry_for_key(binary(), map()) ::
          {:ets, non_neg_integer(), binary()}
          | {:callback, map()}
  defp resolve_registry_for_key(_key, %{cms_registry: registry}) when is_map(registry) do
    {:callback, registry}
  end

  defp resolve_registry_for_key(key, _store) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    index = Router.shard_for(key)
    {:ets, index, data_dir}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp check_not_exists(store, key) do
    case get_cms(key, store) do
      nil -> :ok
      _ -> {:error, "ERR item already exists"}
    end
  end

  @spec get_cms(binary(), map()) :: {reference(), map()} | nil
  defp get_cms(key, store) do
    case resolve_registry_for_key(key, store) do
      {:ets, index, data_dir} ->
        case CmsRegistry.open_or_lookup(index, key, data_dir) do
          {:ok, resource, meta} -> {resource, meta}
          :not_found -> nil
        end

      {:callback, registry} ->
        case registry.get.(key) do
          nil -> nil
          {resource, meta} -> {resource, meta}
        end
    end
  end

  defp get_sketch(store, key) do
    case get_cms(key, store) do
      nil ->
        # Key not in CMS registry -- check if another type owns it (WRONGTYPE)
        if key_held_by_other_type?(key, store) do
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        else
          {:error, "ERR CMS: key does not exist"}
        end
      {resource, _meta} -> {:ok, resource}
    end
  end

  # Checks whether a key is held by another probabilistic data structure type
  defp key_held_by_other_type?(key, store) do
    # Check TopK path in main store (TopK stores {:topk_path, _})
    case Map.get(store, :get) do
      nil ->
        false

      get_fn ->
        case get_fn.(key) do
          nil -> false
          {:topk_path, _} -> true
          {:tdigest_path, _} -> true
          _ -> true
        end
    end
  end

  defp create_cms(key, width, depth, store) do
    meta = %{width: width, depth: depth}

    case resolve_registry_for_key(key, store) do
      {:ets, index, data_dir} ->
        path = CmsRegistry.cms_path(data_dir, index, key)

        case NIF.cms_create_file(path, width, depth) do
          {:ok, resource} ->
            CmsRegistry.register(index, key, resource, meta)
            :ok

          {:error, reason} ->
            {:error, "ERR CMS create failed: #{reason}"}
        end

      {:callback, registry} ->
        path = registry.path.(key)

        case NIF.cms_create_file(path, width, depth) do
          {:ok, resource} ->
            registry.put.(key, resource, meta)
            :ok

          {:error, reason} ->
            {:error, "ERR CMS create failed: #{reason}"}
        end
    end
  end

  defp get_sketch_infos(refs) do
    results =
      Enum.reduce_while(refs, [], fn ref, acc ->
        case NIF.cms_info(ref) do
          {:ok, info} -> {:cont, [info | acc]}
          {:error, reason} -> {:halt, {:error, "ERR CMS info failed: #{reason}"}}
        end
      end)

    case results do
      {:error, _} = err -> err
      infos -> {:ok, Enum.reverse(infos)}
    end
  end

  defp apply_merge_to_dst(store, dst, first_w, first_d, src_refs, weights) do
    case get_sketch(store, dst) do
      {:ok, existing_ref} ->
        case NIF.cms_info(existing_ref) do
          {:ok, {ew, ed, _}} ->
            if ew != first_w or ed != first_d do
              {:error, "ERR CMS: width/depth of src and dst must be equal"}
            else
              sources = Enum.zip(src_refs, weights)

              case NIF.cms_merge(existing_ref, sources) do
                :ok -> :ok
                {:error, reason} -> {:error, "ERR CMS merge failed: #{reason}"}
              end
            end

          {:error, reason} ->
            {:error, "ERR CMS info failed: #{reason}"}
        end

      {:error, "ERR CMS: key does not exist"} ->
        # Create new dest sketch
        meta = %{width: first_w, depth: first_d}

        case resolve_registry_for_key(dst, store) do
          {:ets, index, data_dir} ->
            path = CmsRegistry.cms_path(data_dir, index, dst)

            case NIF.cms_create_file(path, first_w, first_d) do
              {:ok, dest_ref} ->
                CmsRegistry.register(index, dst, dest_ref, meta)
                sources = Enum.zip(src_refs, weights)

                case NIF.cms_merge(dest_ref, sources) do
                  :ok -> :ok
                  {:error, reason} -> {:error, "ERR CMS merge failed: #{reason}"}
                end

              {:error, reason} ->
                {:error, "ERR CMS create failed: #{reason}"}
            end

          {:callback, registry} ->
            path = registry.path.(dst)

            case NIF.cms_create_file(path, first_w, first_d) do
              {:ok, dest_ref} ->
                registry.put.(dst, dest_ref, meta)
                sources = Enum.zip(src_refs, weights)

                case NIF.cms_merge(dest_ref, sources) do
                  :ok -> :ok
                  {:error, reason} -> {:error, "ERR CMS merge failed: #{reason}"}
                end

              {:error, reason} ->
                {:error, "ERR CMS create failed: #{reason}"}
            end
        end

      {:error, _} = err ->
        err
    end
  end

  defp load_source_sketches(store, src_keys) do
    results = Enum.map(src_keys, &get_sketch(store, &1))
    error = Enum.find(results, &match?({:error, _}, &1))
    if error, do: error, else: {:ok, Enum.map(results, fn {:ok, s} -> s end)}
  end

  defp validate_merge_dimensions(infos) do
    {first_w, first_d, _} = hd(infos)

    if Enum.all?(infos, fn {w, d, _} -> w == first_w and d == first_d end),
      do: :ok,
      else: {:error, "ERR CMS: width/depth of src sketches must be equal"}
  end

  defp parse_pos_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} must be a positive integer"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  defp parse_pos_float(str, label) do
    case Float.parse(str) do
      {f, ""} when f > 0 -> {:ok, f}
      {_f, ""} -> {:error, "ERR #{label} must be a positive number"}
      _ -> {:error, "ERR #{label} is not a valid number"}
    end
  end

  defp parse_prob_float(str, label) do
    case Float.parse(str) do
      {f, ""} when f > 0 and f < 1 -> {:ok, f}
      {_f, ""} -> {:error, "ERR #{label} must be between 0 and 1 exclusive"}
      _ -> {:error, "ERR #{label} is not a valid number"}
    end
  end

  defp parse_element_count_pairs(args) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'cms.incrby' command"}
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
      _ -> {:error, "ERR CMS: invalid count value"}
    end
  end

  defp parse_merge_args(args, numkeys) when length(args) < numkeys do
    _ = numkeys
    {:error, "ERR wrong number of arguments for 'cms.merge' command"}
  end

  defp parse_merge_args(args, numkeys) do
    {src_keys, remaining} = Enum.split(args, numkeys)
    parse_merge_weights(src_keys, remaining, numkeys)
  end

  defp parse_merge_weights(src_keys, [], numkeys),
    do: {:ok, src_keys, List.duplicate(1, numkeys)}

  defp parse_merge_weights(src_keys, ["WEIGHTS" | weight_strs], numkeys) do
    if length(weight_strs) != numkeys do
      {:error, "ERR wrong number of weights for 'cms.merge' command"}
    else
      parse_weight_values(src_keys, weight_strs)
    end
  end

  defp parse_merge_weights(_src_keys, _remaining, _numkeys),
    do: {:error, "ERR syntax error in 'cms.merge' command"}

  defp parse_weight_values(src_keys, weight_strs) do
    weights =
      Enum.reduce_while(weight_strs, [], fn w_str, acc ->
        case Integer.parse(w_str) do
          {w, ""} -> {:cont, [w | acc]}
          _ -> {:halt, :error}
        end
      end)

    case weights do
      :error -> {:error, "ERR CMS: invalid weight value"}
      ws -> {:ok, src_keys, Enum.reverse(ws)}
    end
  end
end
