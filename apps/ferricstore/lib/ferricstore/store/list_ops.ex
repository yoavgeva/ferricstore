defmodule Ferricstore.Store.ListOps do
  @moduledoc """
  Pure-logic module for list data structure operations.

  Encapsulates all read-modify-write logic for Redis-compatible list
  operations. This module lives in the `Store` layer so it can be used by
  both `Ferricstore.Store.Shard` (the GenServer that owns the data) and
  `Ferricstore.Commands.List` (the command handler that parses arguments).

  ## Storage format

  Lists are stored as serialized Erlang terms: `:erlang.term_to_binary({:list, elements})`.
  This tagged format distinguishes lists from plain string values and hash
  values (`{:hash, _}`).

  ## Design

  All functions accept closures for reading, writing, and deleting stored
  data. This makes the module agnostic to the storage backend — it works
  with both the Bitcask-backed Shard and the Agent-backed MockStore.
  """

  @wrongtype_error {:error,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"}

  # ---------------------------------------------------------------------------
  # Public API — execute a list operation
  # ---------------------------------------------------------------------------

  @doc """
  Executes a list operation on raw stored data.

  Performs the full read-modify-write cycle for a single-key list operation.

  ## Parameters

    - `get_fn` - Zero-arity function returning the raw stored binary for the
      key, or `nil` if the key does not exist
    - `put_fn` - Arity-1 function accepting an encoded binary to persist
    - `delete_fn` - Zero-arity function that deletes the key
    - `operation` - The list operation tuple (e.g. `{:lpush, elements}`)

  ## Returns

  The result of the operation: integer, binary, list, nil, `:ok`, or
  `{:error, message}`.
  """
  @spec execute(
          get_fn :: (-> binary() | nil),
          put_fn :: (binary() -> :ok),
          delete_fn :: (-> :ok),
          operation :: term()
        ) :: term()
  def execute(get_fn, put_fn, delete_fn, operation) do
    case decode_stored(get_fn.()) do
      {:ok, elements} ->
        do_execute(elements, put_fn, delete_fn, operation)

      :not_found ->
        do_execute_missing(put_fn, delete_fn, operation)

      {:error, :wrongtype} ->
        @wrongtype_error
    end
  end

  @doc """
  Executes an LMOVE operation spanning two keys.

  Pops an element from one end of the source list and pushes it to one end
  of the destination list. Correctly handles the same-key case (list
  rotation) by reading the destination after modifying the source.

  ## Parameters

    - `src_get_fn` - Returns raw stored binary for source key, or nil
    - `src_put_fn` - Persists encoded binary for source key
    - `src_delete_fn` - Deletes the source key
    - `dst_get_fn` - Returns raw stored binary for destination key, or nil
    - `dst_put_fn` - Persists encoded binary for destination key
    - `from_dir` - `:left` or `:right` — which end to pop from source
    - `to_dir` - `:left` or `:right` — which end to push to destination

  ## Returns

  The moved element as a binary string, or `nil` if the source list is
  empty or does not exist, or `{:error, message}` on type mismatch.
  """
  @spec execute_lmove(
          src_get_fn :: (-> binary() | nil),
          src_put_fn :: (binary() -> :ok),
          src_delete_fn :: (-> :ok),
          dst_get_fn :: (-> binary() | nil),
          dst_put_fn :: (binary() -> :ok),
          from_dir :: :left | :right,
          to_dir :: :left | :right
        ) :: binary() | nil | {:error, binary()}
  def execute_lmove(src_get_fn, src_put_fn, src_delete_fn, dst_get_fn, dst_put_fn, from_dir, to_dir) do
    with {:src, {:ok, src_elements}} <- {:src, decode_stored(src_get_fn.())},
         true <- src_elements != [] do
      {element, remaining} = pop_element(src_elements, from_dir)

      # Update source BEFORE reading destination. This is critical for
      # same-key LMOVE (rotate), where source == destination.
      if remaining == [] do
        src_delete_fn.()
      else
        src_put_fn.(encode_list(remaining))
      end

      # Now read the destination (which may have been updated above if same key).
      case decode_stored(dst_get_fn.()) do
        {:error, :wrongtype} ->
          @wrongtype_error

        :not_found ->
          dst_put_fn.(encode_list(push_element([], element, to_dir)))
          element

        {:ok, dst_elements} ->
          new_dst = push_element(dst_elements, element, to_dir)
          dst_put_fn.(encode_list(new_dst))
          element
      end
    else
      {:src, :not_found} -> nil
      {:src, {:error, :wrongtype}} -> @wrongtype_error
      false -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Encoding / Decoding
  # ---------------------------------------------------------------------------

  @doc """
  Decodes a stored binary into list elements, detecting type mismatches.

  ## Returns

    - `{:ok, [binary()]}` — valid list
    - `:not_found` — nil input (key does not exist)
    - `{:error, :wrongtype}` — stored value is a string or hash
  """
  @spec decode_stored(binary() | nil) :: {:ok, [binary()]} | :not_found | {:error, :wrongtype}
  def decode_stored(nil), do: :not_found

  def decode_stored(binary) when is_binary(binary) do
    try do
      case :erlang.binary_to_term(binary) do
        {:list, elements} when is_list(elements) -> {:ok, elements}
        {:hash, _} -> {:error, :wrongtype}
        _ -> {:error, :wrongtype}
      end
    rescue
      ArgumentError -> {:error, :wrongtype}
    end
  end

  @doc """
  Encodes a list of elements into the tagged binary storage format.
  """
  @spec encode_list([binary()]) :: binary()
  def encode_list(elements), do: :erlang.term_to_binary({:list, elements})

  @doc """
  Checks if a raw stored binary is a tagged type (list or hash).

  Returns `{:error, "WRONGTYPE ..."}` if the value is a list or hash,
  or the original value if it is a plain string.

  Used by string commands (GET) to enforce type safety.
  """
  @spec check_string_type(binary()) :: binary() | {:error, binary()}
  def check_string_type(value) when is_binary(value) do
    try do
      case :erlang.binary_to_term(value) do
        {:list, _} ->
          {:error,
           "WRONGTYPE Operation against a key holding the wrong kind of value"}

        {:hash, _} ->
          {:error,
           "WRONGTYPE Operation against a key holding the wrong kind of value"}

        _ ->
          # Not a recognized tagged type — treat as raw string.
          value
      end
    rescue
      ArgumentError ->
        # Not valid ETF — definitely a plain string.
        value
    end
  end

  # ---------------------------------------------------------------------------
  # Private — execute on existing list
  # ---------------------------------------------------------------------------

  defp do_execute(elements, put_fn, _delete_fn, {:lpush, new_elements}) do
    # Redis LPUSH inserts elements left-to-right, so the last argument
    # ends up as the leftmost element.
    updated = Enum.reverse(new_elements) ++ elements
    put_fn.(encode_list(updated))
    length(updated)
  end

  defp do_execute(elements, put_fn, _delete_fn, {:rpush, new_elements}) do
    updated = elements ++ new_elements
    put_fn.(encode_list(updated))
    length(updated)
  end

  defp do_execute([], _put_fn, _delete_fn, {:lpop, _count}), do: nil

  defp do_execute(elements, put_fn, delete_fn, {:lpop, count}) do
    actual_count = min(count, length(elements))
    {popped, remaining} = Enum.split(elements, actual_count)

    if remaining == [] do
      delete_fn.()
    else
      put_fn.(encode_list(remaining))
    end

    # When called without explicit count arg, return single element (not list)
    case count do
      1 -> List.first(popped)
      _ -> popped
    end
  end

  defp do_execute([], _put_fn, _delete_fn, {:rpop, _count}), do: nil

  defp do_execute(elements, put_fn, delete_fn, {:rpop, count}) do
    len = length(elements)
    actual_count = min(count, len)
    {remaining, popped} = Enum.split(elements, len - actual_count)

    if remaining == [] do
      delete_fn.()
    else
      put_fn.(encode_list(remaining))
    end

    # Redis RPOP with count returns elements from tail to head (rightmost first)
    popped_reversed = Enum.reverse(popped)

    case count do
      1 -> List.first(popped_reversed)
      _ -> popped_reversed
    end
  end

  defp do_execute(elements, _put_fn, _delete_fn, {:lrange, start, stop}) do
    len = length(elements)
    norm_start = normalize_index(start, len)
    norm_stop = normalize_index(stop, len)

    cond do
      norm_start > norm_stop -> []
      norm_start >= len -> []
      true -> Enum.slice(elements, norm_start..norm_stop//1)
    end
  end

  defp do_execute(elements, _put_fn, _delete_fn, :llen), do: length(elements)

  defp do_execute(elements, _put_fn, _delete_fn, {:lindex, index}) do
    len = length(elements)

    # Guard against negative underflow: if the negative index is beyond the list
    # start, return nil before normalize_index clamps it to 0.
    if index < 0 and len + index < 0 do
      nil
    else
      norm = normalize_index(index, len)

      if norm >= 0 and norm < len do
        Enum.at(elements, norm)
      else
        nil
      end
    end
  end

  defp do_execute(elements, put_fn, _delete_fn, {:lset, index, element}) do
    len = length(elements)
    norm = normalize_index(index, len)

    if norm >= 0 and norm < len do
      updated = List.replace_at(elements, norm, element)
      put_fn.(encode_list(updated))
      :ok
    else
      {:error, "ERR index out of range"}
    end
  end

  defp do_execute(elements, put_fn, delete_fn, {:lrem, count, element}) do
    {updated, removed_count} = remove_elements(elements, count, element)

    cond do
      removed_count == 0 ->
        0

      updated == [] ->
        delete_fn.()
        removed_count

      true ->
        put_fn.(encode_list(updated))
        removed_count
    end
  end

  defp do_execute(elements, put_fn, delete_fn, {:ltrim, start, stop}) do
    len = length(elements)
    norm_start = normalize_index(start, len)
    norm_stop = normalize_index(stop, len)

    trimmed =
      cond do
        norm_start > norm_stop -> []
        norm_start >= len -> []
        true -> Enum.slice(elements, norm_start..norm_stop//1)
      end

    if trimmed == [] do
      delete_fn.()
    else
      put_fn.(encode_list(trimmed))
    end

    :ok
  end

  defp do_execute(elements, _put_fn, _delete_fn, {:lpos, element, rank, count, maxlen}) do
    find_positions(elements, element, rank, count, maxlen)
  end

  defp do_execute(elements, put_fn, _delete_fn, {:linsert, direction, pivot, element}) do
    case find_pivot_index(elements, pivot) do
      nil ->
        -1

      idx ->
        insert_idx = if direction == :before, do: idx, else: idx + 1
        updated = List.insert_at(elements, insert_idx, element)
        put_fn.(encode_list(updated))
        length(updated)
    end
  end

  defp do_execute(_elements, _put_fn, _delete_fn, {:lmove, _destination, _from_dir, _to_dir}) do
    # This clause should not be reached — LMOVE is handled via execute_lmove/7.
    {:error, "ERR lmove must be handled at the store layer"}
  end

  defp do_execute([], _put_fn, _delete_fn, {:pop_for_move, _dir}), do: nil

  defp do_execute(elements, put_fn, delete_fn, {:pop_for_move, dir}) do
    {element, remaining} = pop_element(elements, dir)

    if remaining == [] do
      delete_fn.()
    else
      put_fn.(encode_list(remaining))
    end

    element
  end

  defp do_execute(elements, put_fn, _delete_fn, {:lpushx, new_elements}) do
    updated = Enum.reverse(new_elements) ++ elements
    put_fn.(encode_list(updated))
    length(updated)
  end

  defp do_execute(elements, put_fn, _delete_fn, {:rpushx, new_elements}) do
    updated = elements ++ new_elements
    put_fn.(encode_list(updated))
    length(updated)
  end

  # ---------------------------------------------------------------------------
  # Private — execute when key does not exist
  # ---------------------------------------------------------------------------

  defp do_execute_missing(put_fn, _delete_fn, {:lpush, elements}) do
    list = Enum.reverse(elements)
    put_fn.(encode_list(list))
    length(list)
  end

  defp do_execute_missing(put_fn, _delete_fn, {:rpush, elements}) do
    put_fn.(encode_list(elements))
    length(elements)
  end

  defp do_execute_missing(_put_fn, _delete_fn, {:lpop, _count}), do: nil
  defp do_execute_missing(_put_fn, _delete_fn, {:rpop, _count}), do: nil
  defp do_execute_missing(_put_fn, _delete_fn, {:lrange, _start, _stop}), do: []
  defp do_execute_missing(_put_fn, _delete_fn, :llen), do: 0
  defp do_execute_missing(_put_fn, _delete_fn, {:lindex, _index}), do: nil

  defp do_execute_missing(_put_fn, _delete_fn, {:lset, _index, _element}) do
    {:error, "ERR no such key"}
  end

  defp do_execute_missing(_put_fn, _delete_fn, {:lrem, _count, _element}), do: 0
  defp do_execute_missing(_put_fn, _delete_fn, {:ltrim, _start, _stop}), do: :ok
  defp do_execute_missing(_put_fn, _delete_fn, {:lpos, _element, _rank, _count, _maxlen}), do: nil
  defp do_execute_missing(_put_fn, _delete_fn, {:linsert, _dir, _pivot, _element}), do: 0
  defp do_execute_missing(_put_fn, _delete_fn, {:lpushx, _elements}), do: 0
  defp do_execute_missing(_put_fn, _delete_fn, {:rpushx, _elements}), do: 0
  defp do_execute_missing(_put_fn, _delete_fn, {:lmove, _destination, _from_dir, _to_dir}), do: nil
  defp do_execute_missing(_put_fn, _delete_fn, {:pop_for_move, _dir}), do: nil

  # ---------------------------------------------------------------------------
  # Private — index normalization
  # ---------------------------------------------------------------------------

  # Converts negative indices to positive. Clamps to 0 at the low end.
  defp normalize_index(index, len) when index < 0, do: max(0, len + index)
  defp normalize_index(index, _len), do: index

  # ---------------------------------------------------------------------------
  # Private — element removal
  # ---------------------------------------------------------------------------

  defp remove_elements(elements, 0, target) do
    # Remove all occurrences
    updated = Enum.reject(elements, &(&1 == target))
    {updated, length(elements) - length(updated)}
  end

  defp remove_elements(elements, count, target) when count > 0 do
    # Remove first `count` occurrences from head
    remove_from_head(elements, count, target, [], 0)
  end

  defp remove_elements(elements, count, target) when count < 0 do
    # Remove last `abs(count)` occurrences (from tail)
    # Reverse, remove from head, reverse back
    abs_count = abs(count)

    {reversed_updated, removed} =
      remove_from_head(Enum.reverse(elements), abs_count, target, [], 0)

    {Enum.reverse(reversed_updated), removed}
  end

  defp remove_from_head([], _remaining, _target, acc, removed) do
    {Enum.reverse(acc), removed}
  end

  defp remove_from_head([elem | rest], 0, _target, acc, removed) do
    {Enum.reverse(acc) ++ [elem | rest], removed}
  end

  defp remove_from_head([elem | rest], remaining, target, acc, removed) do
    if elem == target do
      remove_from_head(rest, remaining - 1, target, acc, removed + 1)
    else
      remove_from_head(rest, remaining, target, [elem | acc], removed)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — LPOS position finding
  # ---------------------------------------------------------------------------

  defp find_positions(elements, element, rank, count, maxlen) do
    # Determine scan direction and effective list
    {scan_list, reverse?} =
      if rank < 0 do
        {Enum.reverse(elements), true}
      else
        {elements, false}
      end

    abs_rank = abs(rank)
    effective_maxlen = if maxlen == 0, do: length(scan_list), else: min(maxlen, length(scan_list))
    scan_slice = Enum.take(scan_list, effective_maxlen)
    total_len = length(elements)

    # Find all matching positions within the scan window
    matches =
      scan_slice
      |> Enum.with_index()
      |> Enum.filter(fn {elem, _idx} -> elem == element end)
      |> Enum.map(fn {_elem, idx} ->
        if reverse?, do: total_len - 1 - idx, else: idx
      end)

    # Skip to the rank-th match
    matches_from_rank = Enum.drop(matches, abs_rank - 1)

    case count do
      # No COUNT specified — return single position or nil
      nil ->
        case matches_from_rank do
          [pos | _] -> pos
          [] -> nil
        end

      # COUNT 0 — return all matches from rank onward
      0 ->
        matches_from_rank

      # COUNT N — return up to N matches from rank onward
      n ->
        Enum.take(matches_from_rank, n)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — pivot finding
  # ---------------------------------------------------------------------------

  defp find_pivot_index(elements, pivot) do
    Enum.find_index(elements, &(&1 == pivot))
  end

  # ---------------------------------------------------------------------------
  # Private — element pop/push helpers (for LMOVE)
  # ---------------------------------------------------------------------------

  defp pop_element(elements, :left), do: {hd(elements), tl(elements)}

  defp pop_element(elements, :right) do
    last = List.last(elements)
    remaining = Enum.slice(elements, 0..(length(elements) - 2)//1)
    {last, remaining}
  end

  defp push_element(elements, value, :left), do: [value | elements]
  defp push_element(elements, value, :right), do: elements ++ [value]
end
