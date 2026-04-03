defmodule Ferricstore.Commands.Cluster do
  @moduledoc """
  Handles FerricStore cluster inspection commands.

  Provides per-shard health and statistics information by querying the ETS
  tables owned by each shard GenServer.

  ## Supported commands

    * `CLUSTER.HEALTH` -- returns per-shard status including role, health,
      key count, and memory usage
    * `CLUSTER.STATS` -- returns per-shard key/memory stats plus totals
  """

  alias Ferricstore.Store.{Router, SlotMap}

  @doc """
  Handles a cluster command.

  ## Parameters

    * `cmd` - uppercased command name (e.g. `"CLUSTER.HEALTH"`)
    * `args` - list of string arguments
    * `_store` - injected store map (unused by cluster commands)

  ## Returns

  A list of bulk strings formatted as key-value pairs for RESP3 encoding.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CLUSTER.HEALTH", [], _store) do
    shard_infos = collect_shard_info()

    lines =
      Enum.flat_map(shard_infos, fn {index, info} ->
        role = shard_role(index)

        [
          "shard_#{index}:",
          "  role: #{role}",
          "  status: #{info.status}",
          "  keys: #{info.keys}",
          "  memory_bytes: #{info.memory_bytes}"
        ]
      end)

    Enum.join(lines, "\r\n")
  end

  def handle("CLUSTER.HEALTH", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.health' command"}
  end

  def handle("CLUSTER.STATS", [], _store) do
    shard_infos = collect_shard_info()

    total_keys = Enum.reduce(shard_infos, 0, fn {_idx, info}, acc -> acc + info.keys end)

    total_memory =
      Enum.reduce(shard_infos, 0, fn {_idx, info}, acc -> acc + info.memory_bytes end)

    shard_lines =
      Enum.flat_map(shard_infos, fn {index, info} ->
        [
          "shard_#{index}:",
          "  keys: #{info.keys}",
          "  memory_bytes: #{info.memory_bytes}"
        ]
      end)

    total_lines = [
      "total_keys: #{total_keys}",
      "total_memory_bytes: #{total_memory}"
    ]

    Enum.join(shard_lines ++ total_lines, "\r\n")
  end

  def handle("CLUSTER.STATS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.stats' command"}
  end

  def handle("CLUSTER.KEYSLOT", [key], _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.slot_for(ctx, key)
  end

  def handle("CLUSTER.KEYSLOT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.keyslot' command"}
  end

  def handle("CLUSTER.SLOTS", [], _store) do
    slot_map = SlotMap.get()
    ranges = SlotMap.slot_ranges(slot_map)

    Enum.map(ranges, fn {start_slot, end_slot, shard_index} ->
      [start_slot, end_slot, shard_index]
    end)
  end

  def handle("CLUSTER.SLOTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.slots' command"}
  end

  # FERRICSTORE.HOTNESS — returns per-prefix hot/cold read statistics.
  # Accepts optional TOP n and WINDOW seconds arguments.
  # Response is a flat list of key-value string pairs:
  #   ["hot_reads", "1200", "cold_reads", "45", ..., "prefix", "user", ...]
  def handle("FERRICSTORE.HOTNESS", args, _store) do
    alias Ferricstore.Stats

    {top_n, _window} = parse_hotness_args(args)
    entries = Stats.hotness_top(top_n)

    header = [
      "hot_reads", Integer.to_string(Stats.total_hot_reads()),
      "cold_reads", Integer.to_string(Stats.total_cold_reads()),
      "hot_read_pct", format_pct(Stats.hot_read_pct()),
      "cold_reads_per_second", format_pct(Stats.cold_reads_per_second()),
      "top_n", Integer.to_string(top_n)
    ]

    prefix_entries =
      Enum.flat_map(entries, fn {prefix, hot, cold, cold_pct} ->
        [
          "prefix", prefix,
          "hot", Integer.to_string(hot),
          "cold", Integer.to_string(cold),
          "cold_pct", format_pct(cold_pct)
        ]
      end)

    header ++ prefix_entries
  end

  defp parse_hotness_args(args) do
    top_n = parse_top_n(args, 10)
    window = parse_window(args, 0)
    {top_n, window}
  end

  defp parse_top_n([], default), do: default
  defp parse_top_n(["TOP", n_str | _], default) do
    case Integer.parse(n_str) do
      {n, ""} when n > 0 -> n
      _ -> default
    end
  end
  defp parse_top_n([_ | rest], default), do: parse_top_n(rest, default)

  defp parse_window([], default), do: default
  defp parse_window(["WINDOW", s_str | _], default) do
    case Integer.parse(s_str) do
      {s, ""} when s > 0 -> s
      _ -> default
    end
  end
  defp parse_window([_ | rest], default), do: parse_window(rest, default)

  defp format_pct(val) when is_float(val) do
    :erlang.float_to_binary(val, [{:decimals, 2}])
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  defp collect_shard_info do
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
    Enum.map(0..(shard_count - 1), fn index ->
      keydir = :"keydir_#{index}"
      ctx = FerricStore.Instance.get(:default)
      name = Router.shard_name(ctx, index)

      info =
        try do
          keys = :ets.info(keydir, :size)
          keydir_words = :ets.info(keydir, :memory)
          word_size = :erlang.system_info(:wordsize)
          memory_bytes = keydir_words * word_size

          status =
            case Process.whereis(name) do
              pid when is_pid(pid) -> if Process.alive?(pid), do: "ok", else: "down"
              nil -> "down"
            end

          %{keys: keys, memory_bytes: memory_bytes, status: status}
        rescue
          ArgumentError ->
            %{keys: 0, memory_bytes: 0, status: "down"}
        end

      {index, info}
    end)
  end

  # Returns "leader" or "follower" for the given shard on this node.
  defp shard_role(index) do
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(index)

    case :ra.members(shard_id) do
      {:ok, _members, ^shard_id} -> "leader"
      {:ok, _members, _other} -> "follower"
      _ -> "unknown"
    end
  rescue
    _ -> "unknown"
  catch
    _, _ -> "unknown"
  end
end
