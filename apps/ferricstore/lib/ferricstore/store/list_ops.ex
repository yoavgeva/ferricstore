defmodule Ferricstore.Store.ListOps do
  @moduledoc """
  Pure-logic module for list data structure operations.

  ## Storage format (compound key / float-position)

  Each list element is stored as an individual compound key entry:
  `L:redis_key\\0{encoded_position} -> element_value`

  A metadata key stores length and position boundaries:
  `LM:redis_key -> :erlang.term_to_binary({length, next_left_pos, next_right_pos})`

  A legacy `execute/4` overload is retained for the Raft state machine.
  """

  alias Ferricstore.Store.CompoundKey

  @initial_position 0.0
  @position_step 1.0

  @spec execute(binary(), map(), term()) :: term()
  def execute(key, store, operation) do
    meta = read_meta(key, store)
    do_execute(key, store, meta, operation)
  end

  def execute(get_fn, put_fn, delete_fn, operation)
      when is_function(get_fn, 0) and is_function(put_fn, 1) and is_function(delete_fn, 0) do
    legacy_execute_blob(get_fn, put_fn, delete_fn, operation)
  end

  @spec execute_lmove(binary(), binary(), map(), :left | :right, :left | :right) ::
          binary() | nil | {:error, binary()}
  def execute_lmove(src_key, dst_key, store, from_dir, to_dir) when is_map(store) do
    src_meta = read_meta(src_key, store)
    case src_meta do
      nil -> nil
      {0, _, _} -> nil
      {_len, _left, _right} ->
        sorted = sorted_elements(src_key, store)
        if sorted == [] do
          nil
        else
          {pos, element} = case from_dir do
            :left -> hd(sorted)
            :right -> List.last(sorted)
          end
          store.compound_delete.(src_key, CompoundKey.list_element(src_key, pos))
          remaining = Enum.reject(sorted, fn {p, _} -> p == pos end)
          if remaining == [] do
            delete_meta(src_key, store)
          else
            update_meta_from_remaining(src_key, store, length(remaining), remaining)
          end
          dst_meta = read_meta(dst_key, store)
          case dst_meta do
            nil ->
              new_pos = @initial_position
              store.compound_put.(dst_key, CompoundKey.list_element(dst_key, new_pos), element, 0)
              write_meta(dst_key, store, {1, new_pos - @position_step, new_pos + @position_step})
            {dst_len, dst_left, dst_right} ->
              new_pos = case to_dir do
                :left -> dst_left
                :right -> dst_right
              end
              store.compound_put.(dst_key, CompoundKey.list_element(dst_key, new_pos), element, 0)
              new_left = if to_dir == :left, do: new_pos - @position_step, else: dst_left
              new_right = if to_dir == :right, do: new_pos + @position_step, else: dst_right
              write_meta(dst_key, store, {dst_len + 1, new_left, new_right})
          end
          element
        end
    end
  end

  def execute_lmove(src_get_fn, src_put_fn, src_delete_fn, dst_get_fn, dst_put_fn, from_dir, to_dir)
      when is_function(src_get_fn, 0) do
    legacy_execute_lmove(src_get_fn, src_put_fn, src_delete_fn, dst_get_fn, dst_put_fn, from_dir, to_dir)
  end

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

  @spec encode_list([binary()]) :: binary()
  def encode_list(elements), do: :erlang.term_to_binary({:list, elements})

  @spec check_string_type(binary()) :: binary() | {:error, binary()}
  def check_string_type(value) when is_binary(value) do
    try do
      case :erlang.binary_to_term(value) do
        {:list, _} -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        {:hash, _} -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        _ -> value
      end
    rescue
      ArgumentError -> value
    end
  end

  @doc false
  def read_meta(key, store) do
    meta_key = CompoundKey.list_meta_key(key)
    case store.compound_get.(key, meta_key) do
      nil -> nil
      binary -> :erlang.binary_to_term(binary)
    end
  end

  defp write_meta(key, store, {_len, _left, _right} = meta) do
    store.compound_put.(key, CompoundKey.list_meta_key(key), :erlang.term_to_binary(meta), 0)
  end

  defp delete_meta(key, store) do
    store.compound_delete.(key, CompoundKey.list_meta_key(key))
  end

  defp sorted_elements(key, store) do
    prefix = CompoundKey.list_prefix(key)
    store.compound_scan.(key, prefix)
    |> Enum.map(fn {encoded_pos, value} -> {CompoundKey.decode_position(encoded_pos), value} end)
  end

  defp ordered_values(key, store) do
    sorted_elements(key, store) |> Enum.map(fn {_pos, value} -> value end)
  end

  defp update_meta_from_remaining(key, store, new_len, remaining) do
    {min_pos, _} = hd(remaining)
    {max_pos, _} = List.last(remaining)
    write_meta(key, store, {new_len, min_pos - @position_step, max_pos + @position_step})
  end

  # LPUSH
  defp do_execute(key, store, nil, {:lpush, new_elements}), do: do_lpush_new(key, store, new_elements)
  defp do_execute(key, store, {len, left_pos, right_pos}, {:lpush, new_elements}) do
    reversed = Enum.reverse(new_elements); count = length(reversed)
    # reversed=[c,b,a]. Assign: c at left_pos-(count-1)*step, b at left_pos-(count-2)*step, a at left_pos
    Enum.with_index(reversed) |> Enum.each(fn {elem, idx} ->
      pos = left_pos - (count - 1 - idx) * @position_step
      store.compound_put.(key, CompoundKey.list_element(key, pos), elem, 0)
    end)
    new_left = left_pos - (count - 1) * @position_step - @position_step
    new_len = len + length(new_elements)
    write_meta(key, store, {new_len, new_left, right_pos})
    new_len
  end

  # RPUSH
  defp do_execute(key, store, nil, {:rpush, new_elements}), do: do_rpush_new(key, store, new_elements)
  defp do_execute(key, store, {len, left_pos, right_pos}, {:rpush, new_elements}) do
    {new_right, _} = Enum.reduce(new_elements, {right_pos, 0}, fn elem, {pos, idx} ->
      store.compound_put.(key, CompoundKey.list_element(key, pos), elem, 0)
      {pos + @position_step, idx + 1}
    end)
    new_len = len + length(new_elements)
    write_meta(key, store, {new_len, left_pos, new_right})
    new_len
  end

  # LPOP
  defp do_execute(_key, _store, nil, {:lpop, _count}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:lpop, _count}), do: nil
  defp do_execute(key, store, {len, _, _}, {:lpop, count}) do
    sorted = sorted_elements(key, store)
    if sorted == [] do nil else
      actual_count = min(count, length(sorted))
      {to_pop, remaining} = Enum.split(sorted, actual_count)
      Enum.each(to_pop, fn {pos, _} -> store.compound_delete.(key, CompoundKey.list_element(key, pos)) end)
      if remaining == [], do: delete_meta(key, store), else: update_meta_from_remaining(key, store, len - actual_count, remaining)
      popped_values = Enum.map(to_pop, fn {_, val} -> val end)
      case count do 1 -> List.first(popped_values); _ -> popped_values end
    end
  end

  # RPOP
  defp do_execute(_key, _store, nil, {:rpop, _count}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:rpop, _count}), do: nil
  defp do_execute(key, store, {len, _, _}, {:rpop, count}) do
    sorted = sorted_elements(key, store)
    if sorted == [] do nil else
      total = length(sorted)
      actual_count = min(count, total)
      {remaining, to_pop} = Enum.split(sorted, total - actual_count)
      Enum.each(to_pop, fn {pos, _} -> store.compound_delete.(key, CompoundKey.list_element(key, pos)) end)
      if remaining == [], do: delete_meta(key, store), else: update_meta_from_remaining(key, store, len - actual_count, remaining)
      popped_values = to_pop |> Enum.map(fn {_, val} -> val end) |> Enum.reverse()
      case count do 1 -> List.first(popped_values); _ -> popped_values end
    end
  end

  # LRANGE
  defp do_execute(_key, _store, nil, {:lrange, _, _}), do: []
  defp do_execute(key, store, {len, _, _}, {:lrange, start, stop}) do
    ns = normalize_index(start, len); ne = normalize_index(stop, len)
    cond do ns > ne -> []; ns >= len -> []; true -> ordered_values(key, store) |> Enum.slice(ns..ne//1) end
  end

  # LLEN
  defp do_execute(_key, _store, nil, :llen), do: 0
  defp do_execute(_key, _store, {len, _, _}, :llen), do: len

  # LINDEX
  defp do_execute(_key, _store, nil, {:lindex, _}), do: nil
  defp do_execute(key, store, {len, _, _}, {:lindex, index}) do
    if index < 0 and len + index < 0, do: nil, else: (norm = normalize_index(index, len); if norm >= 0 and norm < len, do: ordered_values(key, store) |> Enum.at(norm), else: nil)
  end

  # LSET
  defp do_execute(_key, _store, nil, {:lset, _, _}), do: {:error, "ERR no such key"}
  defp do_execute(key, store, {len, _, _}, {:lset, index, element}) do
    norm = normalize_index(index, len)
    if norm >= 0 and norm < len do
      {old_pos, _} = sorted_elements(key, store) |> Enum.at(norm)
      store.compound_put.(key, CompoundKey.list_element(key, old_pos), element, 0)
      :ok
    else
      {:error, "ERR index out of range"}
    end
  end

  # LREM
  defp do_execute(_key, _store, nil, {:lrem, _, _}), do: 0
  defp do_execute(key, store, {len, _, _}, {:lrem, count, element}) do
    sorted = sorted_elements(key, store)
    {to_remove, remaining, removed_count} = select_removals(sorted, count, element)
    cond do
      removed_count == 0 -> 0
      remaining == [] ->
        Enum.each(to_remove, fn {pos, _} -> store.compound_delete.(key, CompoundKey.list_element(key, pos)) end)
        delete_meta(key, store); removed_count
      true ->
        Enum.each(to_remove, fn {pos, _} -> store.compound_delete.(key, CompoundKey.list_element(key, pos)) end)
        update_meta_from_remaining(key, store, len - removed_count, remaining); removed_count
    end
  end

  # LTRIM
  defp do_execute(_key, _store, nil, {:ltrim, _, _}), do: :ok
  defp do_execute(key, store, {len, _, _}, {:ltrim, start, stop}) do
    ns = normalize_index(start, len); ne = normalize_index(stop, len)
    sorted = sorted_elements(key, store)
    {to_keep, to_delete} = cond do
      ns > ne -> {[], sorted}; ns >= len -> {[], sorted}
      true -> (kept = Enum.slice(sorted, ns..ne//1); ks = MapSet.new(kept, fn {p, _} -> p end); {kept, Enum.reject(sorted, fn {p, _} -> MapSet.member?(ks, p) end)})
    end
    Enum.each(to_delete, fn {pos, _} -> store.compound_delete.(key, CompoundKey.list_element(key, pos)) end)
    if to_keep == [], do: delete_meta(key, store), else: (
      {mp, _} = hd(to_keep); {xp, _} = List.last(to_keep)
      write_meta(key, store, {length(to_keep), mp - @position_step, xp + @position_step})
    )
    :ok
  end

  # LPOS
  defp do_execute(_key, _store, nil, {:lpos, _, _, _, _}), do: nil
  defp do_execute(key, store, {_, _, _}, {:lpos, element, rank, count, maxlen}) do
    find_positions(ordered_values(key, store), element, rank, count, maxlen)
  end

  # LINSERT
  defp do_execute(_key, _store, nil, {:linsert, _, _, _}), do: 0
  defp do_execute(key, store, {len, left_pos, right_pos}, {:linsert, direction, pivot, element}) do
    sorted = sorted_elements(key, store)
    values = Enum.map(sorted, fn {_, val} -> val end)
    case Enum.find_index(values, &(&1 == pivot)) do
      nil -> -1
      idx ->
        new_pos = case direction do
          :before -> if idx == 0, do: (elem(hd(sorted), 0) - @position_step), else: ((elem(Enum.at(sorted, idx - 1), 0) + elem(Enum.at(sorted, idx), 0)) / 2.0)
          :after -> if idx == length(sorted) - 1, do: (elem(List.last(sorted), 0) + @position_step), else: ((elem(Enum.at(sorted, idx), 0) + elem(Enum.at(sorted, idx + 1), 0)) / 2.0)
        end
        store.compound_put.(key, CompoundKey.list_element(key, new_pos), element, 0)
        write_meta(key, store, {len + 1, min(left_pos, new_pos - @position_step), max(right_pos, new_pos + @position_step)})
        len + 1
    end
  end

  defp do_execute(_, _, _, {:lmove, _, _, _}), do: {:error, "ERR lmove must be handled at the store layer"}

  # pop_for_move
  defp do_execute(_key, _store, nil, {:pop_for_move, _}), do: nil
  defp do_execute(_key, _store, {0, _, _}, {:pop_for_move, _}), do: nil
  defp do_execute(key, store, {len, _, _}, {:pop_for_move, dir}) do
    sorted = sorted_elements(key, store)
    if sorted == [] do nil else
      {pos, element} = case dir do :left -> hd(sorted); :right -> List.last(sorted) end
      store.compound_delete.(key, CompoundKey.list_element(key, pos))
      remaining = Enum.reject(sorted, fn {p, _} -> p == pos end)
      if remaining == [], do: delete_meta(key, store), else: update_meta_from_remaining(key, store, len - 1, remaining)
      element
    end
  end

  # LPUSHX / RPUSHX
  defp do_execute(_, _, nil, {:lpushx, _}), do: 0
  defp do_execute(key, store, meta, {:lpushx, elems}), do: do_execute(key, store, meta, {:lpush, elems})
  defp do_execute(_, _, nil, {:rpushx, _}), do: 0
  defp do_execute(key, store, meta, {:rpushx, elems}), do: do_execute(key, store, meta, {:rpush, elems})

  defp do_lpush_new(key, store, elements) do
    reversed = Enum.reverse(elements); count = length(reversed)
    # reversed=[c,b,a] for LPUSH key a b c. c should be leftmost (smallest pos).
    # Assign: c at -(count-1)*step, b at -(count-2)*step, ..., a at 0.0
    Enum.with_index(reversed) |> Enum.each(fn {elem, idx} ->
      pos = @initial_position - (count - 1 - idx) * @position_step
      store.compound_put.(key, CompoundKey.list_element(key, pos), elem, 0)
    end)
    min_a = @initial_position - (count - 1) * @position_step
    write_meta(key, store, {count, min_a - @position_step, @initial_position + @position_step})
    count
  end

  defp do_rpush_new(key, store, elements) do
    count = length(elements)
    Enum.with_index(elements) |> Enum.each(fn {elem, idx} ->
      store.compound_put.(key, CompoundKey.list_element(key, @initial_position + idx * @position_step), elem, 0)
    end)
    max_a = @initial_position + (count - 1) * @position_step
    write_meta(key, store, {count, @initial_position - @position_step, max_a + @position_step})
    count
  end

  defp normalize_index(index, len) when index < 0, do: max(0, len + index)
  defp normalize_index(index, _len), do: index

  defp select_removals(sorted, 0, target) do
    {removed, kept} = Enum.split_with(sorted, fn {_, val} -> val == target end)
    {removed, kept, length(removed)}
  end
  defp select_removals(sorted, count, target) when count > 0, do: remove_n_from_head(sorted, count, target)
  defp select_removals(sorted, count, target) when count < 0 do
    {removed, remaining_rev, n} = remove_n_from_head(Enum.reverse(sorted), abs(count), target)
    {removed, Enum.reverse(remaining_rev), n}
  end

  defp remove_n_from_head(sorted, max_remove, target) do
    {removed, remaining, _} = Enum.reduce(sorted, {[], [], max_remove}, fn {_, val} = entry, {rem_acc, keep_acc, budget} ->
      if val == target and budget > 0, do: {[entry | rem_acc], keep_acc, budget - 1}, else: {rem_acc, [entry | keep_acc], budget}
    end)
    {Enum.reverse(removed), Enum.reverse(remaining), length(removed)}
  end

  defp find_positions(elements, element, rank, count, maxlen) do
    {scan_list, reverse?} = if rank < 0, do: {Enum.reverse(elements), true}, else: {elements, false}
    abs_rank = abs(rank)
    eff = if maxlen == 0, do: length(scan_list), else: min(maxlen, length(scan_list))
    total_len = length(elements)
    matches = Enum.take(scan_list, eff) |> Enum.with_index() |> Enum.filter(fn {e, _} -> e == element end) |> Enum.map(fn {_, idx} -> if reverse?, do: total_len - 1 - idx, else: idx end)
    from_rank = Enum.drop(matches, abs_rank - 1)
    case count do
      nil -> case from_rank do [pos | _] -> pos; [] -> nil end
      0 -> from_rank
      n -> Enum.take(from_rank, n)
    end
  end

  # Legacy blob-based execution
  defp legacy_execute_blob(get_fn, put_fn, delete_fn, operation) do
    case decode_stored(get_fn.()) do
      {:ok, elements} -> leg_do(elements, put_fn, delete_fn, operation)
      :not_found -> leg_missing(put_fn, delete_fn, operation)
      {:error, :wrongtype} -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    end
  end

  defp leg_do(el, pf, _, {:lpush, ne}), do: (u = Enum.reverse(ne) ++ el; pf.(encode_list(u)); length(u))
  defp leg_do(el, pf, _, {:rpush, ne}), do: (u = el ++ ne; pf.(encode_list(u)); length(u))
  defp leg_do([], _, _, {:lpop, _}), do: nil
  defp leg_do(el, pf, df, {:lpop, c}) do
    ac = min(c, length(el)); {popped, rem} = Enum.split(el, ac)
    if rem == [], do: df.(), else: pf.(encode_list(rem))
    case c do 1 -> List.first(popped); _ -> popped end
  end
  defp leg_do([], _, _, {:rpop, _}), do: nil
  defp leg_do(el, pf, df, {:rpop, c}) do
    len = length(el); ac = min(c, len); {rem, popped} = Enum.split(el, len - ac)
    if rem == [], do: df.(), else: pf.(encode_list(rem))
    pr = Enum.reverse(popped)
    case c do 1 -> List.first(pr); _ -> pr end
  end
  defp leg_do(el, _, _, {:lrange, s, e}), do: (len = length(el); ns = normalize_index(s, len); ne = normalize_index(e, len); cond do ns > ne -> []; ns >= len -> []; true -> Enum.slice(el, ns..ne//1) end)
  defp leg_do(el, _, _, :llen), do: length(el)
  defp leg_do(el, _, _, {:lindex, i}), do: (len = length(el); if i < 0 and len + i < 0, do: nil, else: (n = normalize_index(i, len); if n >= 0 and n < len, do: Enum.at(el, n), else: nil))
  defp leg_do(el, pf, _, {:lset, i, e}), do: (len = length(el); n = normalize_index(i, len); if n >= 0 and n < len, do: (pf.(encode_list(List.replace_at(el, n, e))); :ok), else: {:error, "ERR index out of range"})
  defp leg_do(el, pf, df, {:lrem, c, e}) do
    {u, rc} = leg_rem(el, c, e)
    cond do rc == 0 -> 0; u == [] -> (df.(); rc); true -> (pf.(encode_list(u)); rc) end
  end
  defp leg_do(el, pf, df, {:ltrim, s, e}) do
    len = length(el); ns = normalize_index(s, len); ne = normalize_index(e, len)
    t = cond do ns > ne -> []; ns >= len -> []; true -> Enum.slice(el, ns..ne//1) end
    if t == [], do: df.(), else: pf.(encode_list(t)); :ok
  end
  defp leg_do(el, _, _, {:lpos, e, r, c, m}), do: find_positions(el, e, r, c, m)
  defp leg_do(el, pf, _, {:linsert, d, pv, e}) do
    case Enum.find_index(el, &(&1 == pv)) do
      nil -> -1; idx -> (ii = if d == :before, do: idx, else: idx + 1; u = List.insert_at(el, ii, e); pf.(encode_list(u)); length(u))
    end
  end
  defp leg_do(_, _, _, {:lmove, _, _, _}), do: {:error, "ERR lmove must be handled at the store layer"}
  defp leg_do([], _, _, {:pop_for_move, _}), do: nil
  defp leg_do(el, pf, df, {:pop_for_move, dir}) do
    {e, r} = leg_pop(el, dir); if r == [], do: df.(), else: pf.(encode_list(r)); e
  end
  defp leg_do(el, pf, _, {:lpushx, ne}), do: (u = Enum.reverse(ne) ++ el; pf.(encode_list(u)); length(u))
  defp leg_do(el, pf, _, {:rpushx, ne}), do: (u = el ++ ne; pf.(encode_list(u)); length(u))

  defp leg_missing(pf, _, {:lpush, el}), do: (l = Enum.reverse(el); pf.(encode_list(l)); length(l))
  defp leg_missing(pf, _, {:rpush, el}), do: (pf.(encode_list(el)); length(el))
  defp leg_missing(_, _, {:lpop, _}), do: nil
  defp leg_missing(_, _, {:rpop, _}), do: nil
  defp leg_missing(_, _, {:lrange, _, _}), do: []
  defp leg_missing(_, _, :llen), do: 0
  defp leg_missing(_, _, {:lindex, _}), do: nil
  defp leg_missing(_, _, {:lset, _, _}), do: {:error, "ERR no such key"}
  defp leg_missing(_, _, {:lrem, _, _}), do: 0
  defp leg_missing(_, _, {:ltrim, _, _}), do: :ok
  defp leg_missing(_, _, {:lpos, _, _, _, _}), do: nil
  defp leg_missing(_, _, {:linsert, _, _, _}), do: 0
  defp leg_missing(_, _, {:lpushx, _}), do: 0
  defp leg_missing(_, _, {:rpushx, _}), do: 0
  defp leg_missing(_, _, {:lmove, _, _, _}), do: nil
  defp leg_missing(_, _, {:pop_for_move, _}), do: nil

  defp leg_rem(el, 0, t), do: (u = Enum.reject(el, &(&1 == t)); {u, length(el) - length(u)})
  defp leg_rem(el, c, t) when c > 0, do: leg_rem_head(el, c, t, [], 0)
  defp leg_rem(el, c, t) when c < 0 do
    {ru, r} = leg_rem_head(Enum.reverse(el), abs(c), t, [], 0); {Enum.reverse(ru), r}
  end
  defp leg_rem_head([], _, _, acc, r), do: {Enum.reverse(acc), r}
  defp leg_rem_head([e | rest], 0, _, acc, r), do: {Enum.reverse(acc) ++ [e | rest], r}
  defp leg_rem_head([e | rest], rem, t, acc, r), do: if(e == t, do: leg_rem_head(rest, rem - 1, t, acc, r + 1), else: leg_rem_head(rest, rem, t, [e | acc], r))

  defp leg_pop(el, :left), do: {hd(el), tl(el)}
  defp leg_pop(el, :right), do: {List.last(el), Enum.slice(el, 0..(length(el) - 2)//1)}

  defp legacy_execute_lmove(sg, sp, sd, dg, dp, fd, td) do
    with {:src, {:ok, se}} <- {:src, decode_stored(sg.())}, true <- se != [] do
      {e, r} = leg_pop(se, fd)
      if r == [], do: sd.(), else: sp.(encode_list(r))
      case decode_stored(dg.()) do
        {:error, :wrongtype} -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        :not_found -> (dp.(encode_list(leg_push([], e, td))); e)
        {:ok, de} -> (dp.(encode_list(leg_push(de, e, td))); e)
      end
    else
      {:src, :not_found} -> nil
      {:src, {:error, :wrongtype}} -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
      false -> nil
    end
  end

  defp leg_push(el, v, :left), do: [v | el]
  defp leg_push(el, v, :right), do: el ++ [v]
end
