defmodule Ferricstore.Commands.CMS do
  @moduledoc """
  Count-Min Sketch commands backed by a Rust NIF resource.

  The sketch matrix lives in Rust (`ferricstore_bitcask/src/cms.rs`).
  The Elixir layer handles RESP argument parsing and store interaction.
  """

  alias Ferricstore.Bitcask.NIF

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CMS.INITBYDIM", [key, width_str, depth_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth"),
         :ok <- check_not_exists(store, key) do
      ref = new_sketch(width, depth)
      store.put.(key, {:cms, ref}, 0)
      :ok
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
      ref = new_sketch(width, depth)
      store.put.(key, {:cms, ref}, 0)
      :ok
    end
  end

  def handle("CMS.INITBYPROB", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.initbyprob' command"}

  def handle("CMS.INCRBY", [key | rest], store) when rest != [] do
    with {:ok, pairs} <- parse_element_count_pairs(rest),
         {:ok, ref} <- get_sketch(store, key) do
      items = Enum.map(pairs, fn {element, count} -> {element, count} end)
      {:ok, counts} = NIF.cms_incrby(ref, items)
      store.put.(key, {:cms, ref}, 0)
      counts
    end
  end

  def handle("CMS.INCRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.incrby' command"}

  def handle("CMS.QUERY", [key | elements], store) when elements != [] do
    with {:ok, ref} <- get_sketch(store, key) do
      {:ok, counts} = NIF.cms_query(ref, elements)
      counts
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
      {:ok, {width, depth, count}} = NIF.cms_info(ref)
      ["width", width, "depth", depth, "count", count]
    end
  end

  def handle("CMS.INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.info' command"}

  # Internal API for Top-K
  @doc false
  def new_sketch(width, depth) do
    {:ok, ref} = NIF.cms_create(width, depth)
    ref
  end

  @doc false
  def increment(ref, element, count) do
    {:ok, [min_count]} = NIF.cms_incrby(ref, [{element, count}])
    {ref, min_count}
  end

  @doc false
  def estimate(ref, element) do
    {:ok, [count]} = NIF.cms_query(ref, [element])
    count
  end

  @doc false
  def sketch_info(ref) do
    {:ok, info} = NIF.cms_info(ref)
    info
  end

  # Private helpers

  defp check_not_exists(store, key) do
    if store.exists?.(key), do: {:error, "ERR item already exists"}, else: :ok
  end

  defp get_sketch(store, key) do
    case store.get.(key) do
      nil -> {:error, "ERR CMS: key does not exist"}
      {:cms, ref} -> {:ok, ref}
      _ -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    end
  end

  defp get_sketch_infos(refs) do
    {:ok, Enum.map(refs, fn ref -> {:ok, info} = NIF.cms_info(ref); info end)}
  end

  defp apply_merge_to_dst(store, dst, first_w, first_d, src_refs, weights) do
    case get_sketch(store, dst) do
      {:ok, existing_ref} ->
        {:ok, {ew, ed, _}} = NIF.cms_info(existing_ref)
        if ew != first_w or ed != first_d do
          {:error, "ERR CMS: width/depth of src and dst must be equal"}
        else
          sources = Enum.zip(src_refs, weights)
          :ok = NIF.cms_merge(existing_ref, sources)
          store.put.(dst, {:cms, existing_ref}, 0)
          :ok
        end

      {:error, "ERR CMS: key does not exist"} ->
        dest_ref = new_sketch(first_w, first_d)
        sources = Enum.zip(src_refs, weights)
        :ok = NIF.cms_merge(dest_ref, sources)
        store.put.(dst, {:cms, dest_ref}, 0)
        :ok

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
