defmodule Ferricstore.Commands.TopK do
  @moduledoc """
  Handles Top-K commands: TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY,
  TOPK.QUERY, TOPK.LIST, TOPK.INFO.

  Top-K maintains the K most frequent elements seen, using a Count-Min Sketch
  for frequency estimation and a min-heap of size K. When an element is added,
  the sketch is updated. If the element's estimated frequency exceeds the
  minimum frequency in the heap, it replaces that element. TOPK.ADD returns
  the evicted element (if any) for each input -- useful for tracking what just
  fell out of the top-K.

  ## Storage format

  Top-K structures are stored as tagged tuples via the injected store map:

      {:topk, %{
        k: k,
        width: w,
        depth: d,
        decay: decay,
        sketch: cms_sketch,
        heap: [{element, count}, ...]
      }}

  The heap is a list of `{element, estimated_count}` tuples. The heap size
  never exceeds `k`.

  ## Decay

  The `decay` parameter (0.0 to 1.0) controls how quickly old elements age out.
  A decay of 1.0 means no decay (pure frequency). Lower values cause older
  items to lose weight over time.

  ## Supported commands

    * `TOPK.RESERVE key k [width depth decay]` -- create a Top-K tracker
    * `TOPK.ADD key element [element ...]` -- add elements (returns evicted items)
    * `TOPK.INCRBY key element count [element count ...]` -- increment by amount
    * `TOPK.QUERY key element [element ...]` -- check if elements are in top-K
    * `TOPK.LIST key [WITHCOUNT]` -- list top-K elements
    * `TOPK.INFO key` -- return structure metadata
  """

  alias Ferricstore.Commands.CMS

  # Default CMS dimensions for Top-K when not specified
  @default_width 8
  @default_depth 7
  @default_decay 0.9

  @doc """
  Handles a TOPK command.

  ## Parameters

    - `cmd` -- uppercased command name (e.g. `"TOPK.RESERVE"`)
    - `args` -- list of string arguments
    - `store` -- injected store map with `get`, `put`, `exists?` callbacks

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
    with {:ok, topk} <- get_topk(store, key) do
      pairs = Enum.map(elements, &{&1, 1})
      {updated, evicted_list} = apply_increments(topk, pairs)
      store.put.(key, {:topk, updated}, 0)
      evicted_list
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
         {:ok, topk} <- get_topk(store, key) do
      {updated, evicted_list} = apply_increments(topk, pairs)
      store.put.(key, {:topk, updated}, 0)
      evicted_list
    end
  end

  def handle("TOPK.INCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.incrby' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.QUERY key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("TOPK.QUERY", [key | elements], store) when elements != [] do
    with {:ok, topk} <- get_topk(store, key) do
      heap_set = MapSet.new(topk.heap, fn {elem, _count} -> elem end)
      Enum.map(elements, &membership_flag(heap_set, &1))
    end
  end

  def handle("TOPK.QUERY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.query' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.LIST key [WITHCOUNT]
  # ---------------------------------------------------------------------------

  def handle("TOPK.LIST", [key], store) do
    with {:ok, topk} <- get_topk(store, key) do
      topk.heap
      |> sort_by_count_desc()
      |> Enum.map(fn {elem, _count} -> elem end)
    end
  end

  def handle("TOPK.LIST", [key, "WITHCOUNT"], store) do
    with {:ok, topk} <- get_topk(store, key) do
      topk.heap
      |> sort_by_count_desc()
      |> Enum.flat_map(fn {elem, count} -> [elem, count] end)
    end
  end

  def handle("TOPK.LIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.list' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.INFO key
  # ---------------------------------------------------------------------------

  def handle("TOPK.INFO", [key], store) do
    with {:ok, topk} <- get_topk(store, key) do
      ["k", topk.k, "width", topk.width, "depth", topk.depth, "decay", topk.decay]
    end
  end

  def handle("TOPK.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Private: structure operations
  # ---------------------------------------------------------------------------

  defp get_topk(store, key) do
    case store.get.(key) do
      nil -> {:error, "ERR TOPK: key does not exist"}
      {:topk, topk} -> {:ok, topk}
      _ -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    end
  end

  defp do_reserve(key, k_str, width, depth, decay, store) do
    with {:ok, k} <- parse_pos_integer(k_str, "k"),
         :ok <- check_not_exists(store, key) do
      topk = %{
        k: k,
        width: width,
        depth: depth,
        decay: decay,
        sketch: CMS.new_sketch(width, depth),
        heap: []
      }

      store.put.(key, {:topk, topk}, 0)
      :ok
    end
  end

  defp check_not_exists(store, key) do
    if store.exists?.(key), do: {:error, "ERR item already exists"}, else: :ok
  end

  defp apply_increments(topk, pairs) do
    Enum.reduce(pairs, {topk, []}, fn {element, count}, {tk, evictions} ->
      {new_tk, evicted} = add_element(tk, element, count)
      {new_tk, evictions ++ [evicted]}
    end)
  end

  # Adds an element with the given count. Returns `{updated_topk, evicted}`
  # where `evicted` is `nil` (no eviction) or the evicted element's name.
  defp add_element(topk, element, count) do
    %{sketch: sketch} = topk
    {updated_sketch, estimated_count} = CMS.increment(sketch, element, count)
    updated_topk = %{topk | sketch: updated_sketch}
    update_heap(updated_topk, element, estimated_count)
  end

  defp update_heap(topk, element, estimated_count) do
    %{k: k, heap: heap} = topk
    already_in_heap = Enum.any?(heap, fn {elem, _} -> elem == element end)

    cond do
      already_in_heap ->
        new_heap = Enum.map(heap, fn
          {^element, _} -> {element, estimated_count}
          other -> other
        end)

        {%{topk | heap: new_heap}, nil}

      length(heap) < k ->
        {%{topk | heap: [{element, estimated_count} | heap]}, nil}

      true ->
        maybe_evict(topk, element, estimated_count)
    end
  end

  defp maybe_evict(topk, element, estimated_count) do
    {min_elem, min_count} = Enum.min_by(topk.heap, fn {_, c} -> c end)

    if estimated_count > min_count do
      new_heap =
        topk.heap
        |> Enum.reject(fn {elem, _} -> elem == min_elem end)
        |> then(fn h -> [{element, estimated_count} | h] end)

      {%{topk | heap: new_heap}, min_elem}
    else
      {topk, nil}
    end
  end

  defp membership_flag(set, element) do
    if MapSet.member?(set, element), do: 1, else: 0
  end

  defp sort_by_count_desc(heap) do
    Enum.sort_by(heap, fn {_elem, count} -> count end, :desc)
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
