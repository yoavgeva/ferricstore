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

  alias Ferricstore.Store.Router

  @shard_count Application.compile_env(:ferricstore, :shard_count, 4)

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
        [
          "shard_#{index}:",
          "  role: leader",
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

  def handle("FERRICSTORE.HOTNESS", args, _store) do
    top_n = parse_top_n(args, @shard_count)
    shard_data = collect_shard_info()

    total_keys = Enum.reduce(shard_data, 0, fn {_, info}, acc -> acc + info.keys end)
    total_memory = Enum.reduce(shard_data, 0, fn {_, info}, acc -> acc + info.memory_bytes end)

    result = [
      "total_keys", Integer.to_string(total_keys),
      "total_memory_bytes", Integer.to_string(total_memory),
      "hot_cache_entries", Integer.to_string(total_keys),
      "shard_count", Integer.to_string(@shard_count),
      "top_n", Integer.to_string(top_n)
    ]

    shard_entries =
      shard_data
      |> Enum.take(top_n)
      |> Enum.flat_map(fn {index, info} ->
        [
          "shard_#{index}_keys", Integer.to_string(info.keys),
          "shard_#{index}_memory_bytes", Integer.to_string(info.memory_bytes)
        ]
      end)

    result ++ shard_entries
  end

  defp parse_top_n([], default), do: default
  defp parse_top_n(["TOP", n_str | _], default) do
    case Integer.parse(n_str) do
      {n, ""} when n > 0 -> n
      _ -> default
    end
  end
  defp parse_top_n([_ | rest], default), do: parse_top_n(rest, default)

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  defp collect_shard_info do
    Enum.map(0..(@shard_count - 1), fn index ->
      ets = :"shard_ets_#{index}"
      name = Router.shard_name(index)

      info =
        try do
          keys = :ets.info(ets, :size)
          memory_words = :ets.info(ets, :memory)
          word_size = :erlang.system_info(:wordsize)
          memory_bytes = memory_words * word_size

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
end
