defmodule Ferricstore.Commands.SortedSet do
  @moduledoc """
  Handles Redis sorted set commands: ZADD, ZSCORE, ZRANK, ZRANGE, ZCARD,
  ZREM, ZINCRBY, ZCOUNT, ZPOPMIN, ZPOPMAX, ZRANGEBYSCORE, ZREVRANGE.

  Each sorted set member is stored as a compound key:

      Z:redis_key\\0member -> score_string

  The score is stored as a string representation of a float64. This allows
  O(1) score lookups by member. For range queries, all members are loaded
  and sorted in memory -- acceptable for typical sorted set sizes in cache
  workloads.

  ## Type Enforcement

  All sorted set commands check type metadata. Using sorted set commands on
  a key that holds a different type returns WRONGTYPE.
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.TypeRegistry

  @doc """
  Handles a sorted set command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"ZADD"`, `"ZRANGE"`)
    - `args` - List of string arguments
    - `store` - Injected store map with compound key callbacks

  ## Returns

  Plain Elixir term: integer, float, string, list, nil, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
  # ---------------------------------------------------------------------------

  def handle("ZADD", [key | rest], store) when rest != [] do
    with {:ok, opts, score_member_pairs} <- parse_zadd_opts(rest),
         :ok <- TypeRegistry.check_or_set(key, :zset, store) do
      {added, changed} =
        Enum.reduce(score_member_pairs, {0, 0}, fn {score, member}, {add_acc, ch_acc} ->
          compound_key = CompoundKey.zset_member(key, member)
          existing = store.compound_get.(key, compound_key)

          cond do
            # NX: only add new elements, don't update existing
            opts.nx and existing != nil ->
              {add_acc, ch_acc}

            # XX: only update existing elements, don't add new
            opts.xx and existing == nil ->
              {add_acc, ch_acc}

            existing == nil ->
              store.compound_put.(key, compound_key, Float.to_string(score), 0)
              {add_acc + 1, ch_acc}

            true ->
              {existing_score, ""} = Float.parse(existing)

              should_update =
                cond do
                  opts.gt -> score > existing_score
                  opts.lt -> score < existing_score
                  true -> true
                end

              if should_update and score != existing_score do
                store.compound_put.(key, compound_key, Float.to_string(score), 0)
                {add_acc, ch_acc + 1}
              else
                {add_acc, ch_acc}
              end
          end
        end)

      if opts.ch, do: added + changed, else: added
    end
  end

  def handle("ZADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zadd' command"}
  end

  # ---------------------------------------------------------------------------
  # ZSCORE key member
  # ---------------------------------------------------------------------------

  def handle("ZSCORE", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_key = CompoundKey.zset_member(key, member)

      case store.compound_get.(key, compound_key) do
        nil -> nil
        score_str -> score_str
      end
    end
  end

  def handle("ZSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANK key member
  # ---------------------------------------------------------------------------

  def handle("ZRANK", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      sorted = load_sorted_members(key, store)

      case Enum.find_index(sorted, fn {m, _s} -> m == member end) do
        nil -> nil
        idx -> idx
      end
    end
  end

  def handle("ZRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrank' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANGE key start stop [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZRANGE", [key, start_str, stop_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} ->
          sorted = load_sorted_members(key, store)
          len = length(sorted)
          start_idx = normalize_index(start, len)
          stop_idx = normalize_index(stop, len)
          with_scores = "WITHSCORES" in opts

          if start_idx > stop_idx or start_idx >= len do
            []
          else
            sliced = Enum.slice(sorted, start_idx..stop_idx)

            if with_scores do
              Enum.flat_map(sliced, fn {member, score} ->
                [member, format_score(score)]
              end)
            else
              Enum.map(sliced, fn {member, _score} -> member end)
            end
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrange' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGE key start stop [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZREVRANGE", [key, start_str, stop_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} ->
          sorted = load_sorted_members(key, store) |> Enum.reverse()
          len = length(sorted)
          start_idx = normalize_index(start, len)
          stop_idx = normalize_index(stop, len)
          with_scores = "WITHSCORES" in opts

          if start_idx > stop_idx or start_idx >= len do
            []
          else
            sliced = Enum.slice(sorted, start_idx..stop_idx)

            if with_scores do
              Enum.flat_map(sliced, fn {member, score} ->
                [member, format_score(score)]
              end)
            else
              Enum.map(sliced, fn {member, _score} -> member end)
            end
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZREVRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrange' command"}
  end

  # ---------------------------------------------------------------------------
  # ZCARD key
  # ---------------------------------------------------------------------------

  def handle("ZCARD", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      prefix = CompoundKey.zset_prefix(key)
      store.compound_count.(key, prefix)
    end
  end

  def handle("ZCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zcard' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREM key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("ZREM", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      removed =
        Enum.reduce(members, 0, fn member, acc ->
          compound_key = CompoundKey.zset_member(key, member)

          if store.compound_get.(key, compound_key) != nil do
            store.compound_delete.(key, compound_key)
            acc + 1
          else
            acc
          end
        end)

      if removed > 0 do
        prefix = CompoundKey.zset_prefix(key)

        if store.compound_count.(key, prefix) == 0 do
          TypeRegistry.delete_type(key, store)
        end
      end

      removed
    end
  end

  def handle("ZREM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrem' command"}
  end

  # ---------------------------------------------------------------------------
  # ZINCRBY key increment member
  # ---------------------------------------------------------------------------

  def handle("ZINCRBY", [key, increment_str, member], store) do
    with :ok <- TypeRegistry.check_or_set(key, :zset, store) do
      case Float.parse(increment_str) do
        {increment, ""} ->
          compound_key = CompoundKey.zset_member(key, member)
          existing = store.compound_get.(key, compound_key)

          current_score =
            case existing do
              nil -> 0.0
              score_str ->
                {score, ""} = Float.parse(score_str)
                score
            end

          new_score = current_score + increment
          store.compound_put.(key, compound_key, Float.to_string(new_score), 0)
          format_score(new_score)

        :error ->
          # Try integer parse
          case Integer.parse(increment_str) do
            {increment, ""} ->
              compound_key = CompoundKey.zset_member(key, member)
              existing = store.compound_get.(key, compound_key)

              current_score =
                case existing do
                  nil -> 0.0
                  score_str ->
                    {score, ""} = Float.parse(score_str)
                    score
                end

              new_score = current_score + increment * 1.0
              store.compound_put.(key, compound_key, Float.to_string(new_score), 0)
              format_score(new_score)

            _ ->
              {:error, "ERR value is not a valid float"}
          end
      end
    end
  end

  def handle("ZINCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zincrby' command"}
  end

  # ---------------------------------------------------------------------------
  # ZCOUNT key min max
  # ---------------------------------------------------------------------------

  def handle("ZCOUNT", [key, min_str, max_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {parse_score_bound(min_str), parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          sorted = load_sorted_members(key, store)

          Enum.count(sorted, fn {_member, score} ->
            above_min = score_gte?(score, min_val, min_excl)
            below_max = score_lte?(score, max_val, max_excl)
            above_min and below_max
          end)

        _ ->
          {:error, "ERR min or max is not a float"}
      end
    end
  end

  def handle("ZCOUNT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zcount' command"}
  end

  # ---------------------------------------------------------------------------
  # ZPOPMIN key [count]
  # ---------------------------------------------------------------------------

  def handle("ZPOPMIN", [key], store) do
    handle("ZPOPMIN", [key, "1"], store)
  end

  def handle("ZPOPMIN", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          sorted = load_sorted_members(key, store)
          to_pop = Enum.take(sorted, count)

          result =
            Enum.flat_map(to_pop, fn {member, score} ->
              compound_key = CompoundKey.zset_member(key, member)
              store.compound_delete.(key, compound_key)
              [member, format_score(score)]
            end)

          if to_pop != [] do
            prefix = CompoundKey.zset_prefix(key)

            if store.compound_count.(key, prefix) == 0 do
              TypeRegistry.delete_type(key, store)
            end
          end

          result

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZPOPMIN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zpopmin' command"}
  end

  # ---------------------------------------------------------------------------
  # ZPOPMAX key [count]
  # ---------------------------------------------------------------------------

  def handle("ZPOPMAX", [key], store) do
    handle("ZPOPMAX", [key, "1"], store)
  end

  def handle("ZPOPMAX", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          sorted = load_sorted_members(key, store) |> Enum.reverse()
          to_pop = Enum.take(sorted, count)

          result =
            Enum.flat_map(to_pop, fn {member, score} ->
              compound_key = CompoundKey.zset_member(key, member)
              store.compound_delete.(key, compound_key)
              [member, format_score(score)]
            end)

          if to_pop != [] do
            prefix = CompoundKey.zset_prefix(key)

            if store.compound_count.(key, prefix) == 0 do
              TypeRegistry.delete_type(key, store)
            end
          end

          result

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZPOPMAX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zpopmax' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp load_sorted_members(key, store) do
    prefix = CompoundKey.zset_prefix(key)
    pairs = store.compound_scan.(key, prefix)

    pairs
    |> Enum.map(fn {member, score_str} ->
      {score, ""} = Float.parse(score_str)
      {member, score}
    end)
    |> Enum.sort_by(fn {member, score} -> {score, member} end)
  end

  defp normalize_index(index, len) when index < 0, do: max(0, len + index)
  defp normalize_index(index, _len), do: index

  defp format_score(score) when is_float(score) do
    # Redis returns scores as strings
    :erlang.float_to_binary(score, [:compact, decimals: 17])
  end

  # Parse ZADD options and score/member pairs
  defp parse_zadd_opts(args) do
    parse_zadd_opts(args, %{nx: false, xx: false, gt: false, lt: false, ch: false})
  end

  defp parse_zadd_opts(["NX" | rest], opts), do: parse_zadd_opts(rest, %{opts | nx: true})
  defp parse_zadd_opts(["XX" | rest], opts), do: parse_zadd_opts(rest, %{opts | xx: true})
  defp parse_zadd_opts(["GT" | rest], opts), do: parse_zadd_opts(rest, %{opts | gt: true})
  defp parse_zadd_opts(["LT" | rest], opts), do: parse_zadd_opts(rest, %{opts | lt: true})
  defp parse_zadd_opts(["CH" | rest], opts), do: parse_zadd_opts(rest, %{opts | ch: true})

  defp parse_zadd_opts(score_member_args, opts) do
    if rem(length(score_member_args), 2) != 0 or score_member_args == [] do
      {:error, "ERR wrong number of arguments for 'zadd' command"}
    else
      pairs =
        score_member_args
        |> Enum.chunk_every(2)
        |> Enum.reduce_while([], fn [score_str, member], acc ->
          case parse_score(score_str) do
            {:ok, score} -> {:cont, [{score, member} | acc]}
            :error -> {:halt, :error}
          end
        end)

      case pairs do
        :error -> {:error, "ERR value is not a valid float"}
        pairs -> {:ok, opts, Enum.reverse(pairs)}
      end
    end
  end

  defp parse_score(str) do
    case Float.parse(str) do
      {score, ""} -> {:ok, score}
      _ ->
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  end

  defp parse_score_bound("-inf"), do: {:ok, :neg_infinity, false}
  defp parse_score_bound("+inf"), do: {:ok, :infinity, false}
  defp parse_score_bound("inf"), do: {:ok, :infinity, false}

  defp parse_score_bound("(" <> rest) do
    case parse_score(rest) do
      {:ok, score} -> {:ok, score, true}
      :error -> :error
    end
  end

  defp parse_score_bound(str) do
    case parse_score(str) do
      {:ok, score} -> {:ok, score, false}
      :error -> :error
    end
  end

  # Score comparison helpers that handle :infinity and :neg_infinity atoms.
  defp score_gte?(_score, :neg_infinity, _exclusive), do: true
  defp score_gte?(_score, :infinity, _exclusive), do: false
  defp score_gte?(score, bound, true), do: score > bound
  defp score_gte?(score, bound, false), do: score >= bound

  defp score_lte?(_score, :infinity, _exclusive), do: true
  defp score_lte?(_score, :neg_infinity, _exclusive), do: false
  defp score_lte?(score, bound, true), do: score < bound
  defp score_lte?(score, bound, false), do: score <= bound
end
