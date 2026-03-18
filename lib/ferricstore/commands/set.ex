defmodule Ferricstore.Commands.Set do
  @moduledoc """
  Handles Redis set commands: SADD, SREM, SMEMBERS, SISMEMBER, SCARD,
  SINTER, SUNION, SDIFF, SRANDMEMBER, SPOP.

  Each set member is stored as a compound key:

      S:redis_key\\0member_name -> "1"

  The member name IS the Bitcask sub-key. The value is a presence marker
  `"1"`. This allows O(1) membership testing via direct key lookup.

  ## Type Enforcement

  All set commands check type metadata. Using set commands on a key that
  holds a different type returns WRONGTYPE.
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.TypeRegistry

  @presence_marker "1"

  @doc """
  Handles a set command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"SADD"`, `"SMEMBERS"`)
    - `args` - List of string arguments
    - `store` - Injected store map with compound key callbacks

  ## Returns

  Plain Elixir term: integer, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # SADD key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("SADD", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_or_set(key, :set, store) do
      Enum.reduce(members, 0, fn member, acc ->
        compound_key = CompoundKey.set_member(key, member)
        existing = store.compound_get.(key, compound_key)
        store.compound_put.(key, compound_key, @presence_marker, 0)
        if existing == nil, do: acc + 1, else: acc
      end)
    end
  end

  def handle("SADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sadd' command"}
  end

  # ---------------------------------------------------------------------------
  # SREM key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("SREM", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      removed =
        Enum.reduce(members, 0, fn member, acc ->
          compound_key = CompoundKey.set_member(key, member)

          if store.compound_get.(key, compound_key) != nil do
            store.compound_delete.(key, compound_key)
            acc + 1
          else
            acc
          end
        end)

      # Clean up type metadata if the set is now empty
      if removed > 0 do
        prefix = CompoundKey.set_prefix(key)

        if store.compound_count.(key, prefix) == 0 do
          TypeRegistry.delete_type(key, store)
        end
      end

      removed
    end
  end

  def handle("SREM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'srem' command"}
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS key
  # ---------------------------------------------------------------------------

  def handle("SMEMBERS", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      pairs = store.compound_scan.(key, prefix)
      Enum.map(pairs, fn {member, _} -> member end)
    end
  end

  def handle("SMEMBERS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'smembers' command"}
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER key member
  # ---------------------------------------------------------------------------

  def handle("SISMEMBER", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      compound_key = CompoundKey.set_member(key, member)

      if store.compound_get.(key, compound_key) != nil do
        1
      else
        0
      end
    end
  end

  def handle("SISMEMBER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sismember' command"}
  end

  # ---------------------------------------------------------------------------
  # SCARD key
  # ---------------------------------------------------------------------------

  def handle("SCARD", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      store.compound_count.(key, prefix)
    end
  end

  def handle("SCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'scard' command"}
  end

  # ---------------------------------------------------------------------------
  # SINTER key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SINTER", [_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store) do
      sets = Enum.map(keys, fn key -> get_members_set(key, store) end)

      result =
        case sets do
          [first | rest] -> Enum.reduce(rest, first, &MapSet.intersection(&2, &1))
          [] -> MapSet.new()
        end

      MapSet.to_list(result)
    end
  end

  def handle("SINTER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sinter' command"}
  end

  # ---------------------------------------------------------------------------
  # SUNION key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SUNION", [_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store) do
      sets = Enum.map(keys, fn key -> get_members_set(key, store) end)
      result = Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))
      MapSet.to_list(result)
    end
  end

  def handle("SUNION", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sunion' command"}
  end

  # ---------------------------------------------------------------------------
  # SDIFF key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SDIFF", [first_key | rest_keys], store) do
    with :ok <- check_all_types([first_key | rest_keys], store) do
      first_set = get_members_set(first_key, store)
      rest_sets = Enum.map(rest_keys, fn key -> get_members_set(key, store) end)
      result = Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))
      MapSet.to_list(result)
    end
  end

  def handle("SDIFF", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sdiff' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp get_members_set(key, store) do
    prefix = CompoundKey.set_prefix(key)
    pairs = store.compound_scan.(key, prefix)
    MapSet.new(pairs, fn {member, _} -> member end)
  end

  defp check_all_types(keys, store) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case TypeRegistry.check_type(key, :set, store) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
