defmodule Ferricstore.Commands.CMS do
  @moduledoc """
  Handles Count-Min Sketch commands: CMS.INITBYDIM, CMS.INITBYPROB,
  CMS.INCRBY, CMS.QUERY, CMS.MERGE, CMS.INFO.

  A Count-Min Sketch is a probabilistic data structure for frequency estimation.
  It answers "how many times has element X occurred?" and never undercounts --
  the estimate is always >= the true count. It may overcount by a bounded error
  that decreases as sketch dimensions increase.

  Error bound: `true_count + (total_insertions / width)` with probability
  `1 - (1/2)^depth`.

  ## Storage format

  Sketches are stored as tagged tuples via the injected store map:

      {:cms, %{width: w, depth: d, total_count: n, counters: counters}}

  where `counters` is an Erlang `:array` of `width * depth` non-negative
  integers (row-major order: row 0 occupies indices 0..width-1, row 1
  occupies indices width..2*width-1, etc.).

  ## Supported commands

    * `CMS.INITBYDIM key width depth` -- create sketch with exact dimensions
    * `CMS.INITBYPROB key error probability` -- create sketch sized for target accuracy
    * `CMS.INCRBY key element count [element count ...]` -- increment element counts
    * `CMS.QUERY key element [element ...]` -- query estimated counts
    * `CMS.MERGE dst numkeys src1 [src2 ...] [WEIGHTS w1 w2 ...]` -- merge sketches
    * `CMS.INFO key` -- return sketch metadata
  """

  @doc """
  Handles a CMS command.

  ## Parameters

    - `cmd` -- uppercased command name (e.g. `"CMS.INITBYDIM"`)
    - `args` -- list of string arguments
    - `store` -- injected store map with `get`, `put`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, integer, list, map, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # CMS.INITBYDIM key width depth
  # ---------------------------------------------------------------------------

  def handle("CMS.INITBYDIM", [key, width_str, depth_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth"),
         :ok <- check_not_exists(store, key) do
      sketch = new_sketch(width, depth)
      store.put.(key, {:cms, sketch}, 0)
      :ok
    end
  end

  def handle("CMS.INITBYDIM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cms.initbydim' command"}
  end

  # ---------------------------------------------------------------------------
  # CMS.INITBYPROB key error probability
  # ---------------------------------------------------------------------------

  def handle("CMS.INITBYPROB", [key, error_str, prob_str], store) do
    with {:ok, error} <- parse_pos_float(error_str, "error"),
         {:ok, prob} <- parse_prob_float(prob_str, "probability"),
         :ok <- check_not_exists(store, key) do
      width = ceil(:math.exp(1) / error)
      depth = ceil(:math.log(1.0 / prob))
      sketch = new_sketch(width, depth)
      store.put.(key, {:cms, sketch}, 0)
      :ok
    end
  end

  def handle("CMS.INITBYPROB", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cms.initbyprob' command"}
  end

  # ---------------------------------------------------------------------------
  # CMS.INCRBY key element count [element count ...]
  # ---------------------------------------------------------------------------

  def handle("CMS.INCRBY", [key | rest], store) when rest != [] do
    with {:ok, pairs} <- parse_element_count_pairs(rest),
         {:ok, sketch} <- get_sketch(store, key) do
      {updated, counts} = increment_elements(sketch, pairs)
      store.put.(key, {:cms, updated}, 0)
      counts
    end
  end

  def handle("CMS.INCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cms.incrby' command"}
  end

  # ---------------------------------------------------------------------------
  # CMS.QUERY key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("CMS.QUERY", [key | elements], store) when elements != [] do
    with {:ok, sketch} <- get_sketch(store, key) do
      Enum.map(elements, &query_element(sketch, &1))
    end
  end

  def handle("CMS.QUERY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cms.query' command"}
  end

  # ---------------------------------------------------------------------------
  # CMS.MERGE dst numkeys src1 [src2 ...] [WEIGHTS w1 w2 ...]
  # ---------------------------------------------------------------------------

  def handle("CMS.MERGE", [dst, numkeys_str | rest], store) do
    with {:ok, numkeys} <- parse_pos_integer(numkeys_str, "numkeys"),
         {:ok, src_keys, weights} <- parse_merge_args(rest, numkeys),
         {:ok, sketches} <- load_source_sketches(store, src_keys),
         :ok <- validate_merge_dimensions(sketches) do
      first = hd(sketches)
      merged = merge_sketches(first.width, first.depth, sketches, weights)
      apply_merge_to_dst(store, dst, first, merged)
    end
  end

  def handle("CMS.MERGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cms.merge' command"}
  end

  # ---------------------------------------------------------------------------
  # CMS.INFO key
  # ---------------------------------------------------------------------------

  def handle("CMS.INFO", [key], store) do
    with {:ok, sketch} <- get_sketch(store, key) do
      ["width", sketch.width, "depth", sketch.depth, "count", sketch.total_count]
    end
  end

  def handle("CMS.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cms.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Internal API (for Top-K usage)
  # ---------------------------------------------------------------------------

  @doc false
  @spec new_sketch(pos_integer(), pos_integer()) :: map()
  def new_sketch(width, depth) do
    counters = :array.new(width * depth, default: 0)

    %{
      width: width,
      depth: depth,
      total_count: 0,
      counters: counters
    }
  end

  @doc false
  @spec increment(map(), binary(), non_neg_integer()) :: {map(), non_neg_integer()}
  def increment(sketch, element, count) do
    %{width: width, depth: depth, counters: counters, total_count: total} = sketch
    hashes = hash_element(element, depth)

    {updated_counters, min_count} =
      Enum.reduce(Enum.with_index(hashes), {counters, :infinity}, fn {hash, row}, {ctr, min} ->
        idx = row * width + rem(hash, width)
        new_val = :array.get(idx, ctr) + count
        {:array.set(idx, new_val, ctr), min(min, new_val)}
      end)

    updated = %{sketch | counters: updated_counters, total_count: total + count}
    {updated, min_count}
  end

  @doc false
  @spec estimate(map(), binary()) :: non_neg_integer()
  def estimate(sketch, element), do: query_element(sketch, element)

  # ---------------------------------------------------------------------------
  # Private: sketch operations
  # ---------------------------------------------------------------------------

  defp check_not_exists(store, key) do
    if store.exists?.(key), do: {:error, "ERR item already exists"}, else: :ok
  end

  defp get_sketch(store, key) do
    case store.get.(key) do
      nil -> {:error, "ERR CMS: key does not exist"}
      {:cms, sketch} -> {:ok, sketch}
      _ -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    end
  end

  defp query_element(sketch, element) do
    %{width: width, depth: depth, counters: counters} = sketch
    hashes = hash_element(element, depth)

    hashes
    |> Enum.with_index()
    |> Enum.map(fn {hash, row} -> :array.get(row * width + rem(hash, width), counters) end)
    |> Enum.min()
  end

  defp increment_elements(sketch, pairs) do
    {updated, counts} =
      Enum.reduce(pairs, {sketch, []}, fn {element, count}, {sk, acc} ->
        {new_sk, min_count} = increment(sk, element, count)
        {new_sk, [min_count | acc]}
      end)

    {updated, Enum.reverse(counts)}
  end

  # ---------------------------------------------------------------------------
  # Private: hashing
  # ---------------------------------------------------------------------------

  # Produces `depth` independent hash values for `element` using double hashing.
  defp hash_element(element, depth) do
    h1 = :erlang.phash2(element, 1_073_741_824)
    h2 = :erlang.phash2({element, :salt}, 1_073_741_824)
    Enum.map(0..(depth - 1), fn i -> abs(h1 + i * h2) end)
  end

  # ---------------------------------------------------------------------------
  # Private: merge operations
  # ---------------------------------------------------------------------------

  defp apply_merge_to_dst(store, dst, first, merged) do
    case get_sketch(store, dst) do
      {:ok, existing} ->
        validate_and_merge_into_existing(store, dst, existing, first, merged)

      {:error, "ERR CMS: key does not exist"} ->
        store.put.(dst, {:cms, merged}, 0)
        :ok

      {:error, _} = err ->
        err
    end
  end

  defp validate_and_merge_into_existing(store, dst, existing, first, merged) do
    if existing.width != first.width or existing.depth != first.depth do
      {:error, "ERR CMS: width/depth of src and dst must be equal"}
    else
      combined = merge_two(existing, merged)
      store.put.(dst, {:cms, combined}, 0)
      :ok
    end
  end

  defp load_source_sketches(store, src_keys) do
    results = Enum.map(src_keys, &get_sketch(store, &1))
    error = Enum.find(results, &match?({:error, _}, &1))
    if error, do: error, else: {:ok, Enum.map(results, fn {:ok, s} -> s end)}
  end

  defp validate_merge_dimensions(sketches) do
    first = hd(sketches)

    if Enum.all?(sketches, fn s -> s.width == first.width and s.depth == first.depth end) do
      :ok
    else
      {:error, "ERR CMS: width/depth of src sketches must be equal"}
    end
  end

  defp merge_sketches(width, depth, sketches, weights) do
    size = width * depth
    weighted_pairs = Enum.zip(sketches, weights)

    counters =
      Enum.reduce(0..(size - 1), :array.new(size, default: 0), fn idx, acc ->
        sum = Enum.reduce(weighted_pairs, 0, fn {sk, w}, t -> t + :array.get(idx, sk.counters) * w end)
        :array.set(idx, max(0, sum), acc)
      end)

    total = Enum.reduce(weighted_pairs, 0, fn {sk, w}, t -> t + sk.total_count * w end)
    %{width: width, depth: depth, total_count: max(0, total), counters: counters}
  end

  defp merge_two(existing, incoming) do
    %{width: width, depth: depth} = existing
    size = width * depth

    counters =
      Enum.reduce(0..(size - 1), :array.new(size, default: 0), fn idx, acc ->
        :array.set(idx, :array.get(idx, existing.counters) + :array.get(idx, incoming.counters), acc)
      end)

    %{
      width: width,
      depth: depth,
      total_count: existing.total_count + incoming.total_count,
      counters: counters
    }
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
      do_parse_element_count_pairs(args)
    end
  end

  defp do_parse_element_count_pairs(args) do
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

  defp parse_merge_weights(src_keys, [], numkeys) do
    {:ok, src_keys, List.duplicate(1, numkeys)}
  end

  defp parse_merge_weights(src_keys, ["WEIGHTS" | weight_strs], numkeys) do
    if length(weight_strs) != numkeys do
      {:error, "ERR wrong number of weights for 'cms.merge' command"}
    else
      parse_weight_values(src_keys, weight_strs)
    end
  end

  defp parse_merge_weights(_src_keys, _remaining, _numkeys) do
    {:error, "ERR syntax error in 'cms.merge' command"}
  end

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
