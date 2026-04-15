defmodule Ferricstore.WalStats do
  @moduledoc """
  Collects write-path instrumentation from both Rust NIF and Erlang sides.
  Call `Ferricstore.WalStats.collect/0` during a benchmark to understand
  where time is spent.
  """

  @doc """
  Collect all write-path stats. Returns a map with:
  - :nif — Rust background thread counters
  - :batchers — per-shard Batcher queue lengths and reductions
  - :ra_servers — per-shard Ra server queue lengths
  - :wal — WAL gen_batch_server queue length
  - :schedulers — BEAM scheduler utilization
  """
  def collect do
    %{
      nif: collect_nif_stats(),
      batchers: collect_process_stats("Ferricstore.Raft.Batcher"),
      shards: collect_process_stats("Ferricstore.Store.Shard"),
      ra_servers: collect_ra_server_stats(),
      wal: collect_wal_process_stats(),
      system: collect_system_stats()
    }
  end

  @doc "Pretty-print stats to stdout."
  def print do
    stats = collect()

    IO.puts("\n=== WAL NIF Stats ===")
    case stats.nif do
      {:error, reason} ->
        IO.puts("  Error: #{reason}")
      nif_stats ->
        Enum.each(nif_stats, fn {key, val} ->
          IO.puts("  #{key}: #{val}")
        end)
    end

    IO.puts("\n=== Batcher Processes ===")
    Enum.each(stats.batchers, fn {name, info} ->
      IO.puts("  #{name}: queue=#{info.queue} reds=#{info.reductions}")
    end)

    IO.puts("\n=== Shard Processes ===")
    Enum.each(stats.shards, fn {name, info} ->
      IO.puts("  #{name}: queue=#{info.queue} reds=#{info.reductions}")
    end)

    IO.puts("\n=== Ra Server Processes ===")
    Enum.each(stats.ra_servers, fn {name, info} ->
      IO.puts("  #{name}: queue=#{info.queue} func=#{inspect(info.current_function)}")
    end)

    IO.puts("\n=== WAL Process ===")
    case stats.wal do
      nil -> IO.puts("  not found")
      info -> IO.puts("  queue=#{info.queue} reds=#{info.reductions}")
    end

    IO.puts("\n=== System ===")
    IO.puts("  processes: #{stats.system.process_count}")
    IO.puts("  schedulers_online: #{stats.system.schedulers_online}")
    IO.puts("  reductions: #{stats.system.total_reductions}")

    :ok
  end

  defp collect_nif_stats do
    try do
      handle = :persistent_term.get(:ferricstore_wal_handle)
      :ferricstore_wal_nif.stats(handle)
    rescue
      _ -> {:error, :no_handle}
    catch
      _, _ -> {:error, :nif_error}
    end
  end

  defp collect_process_stats(prefix) do
    Process.registered()
    |> Enum.filter(fn name ->
      String.starts_with?(Atom.to_string(name), prefix)
    end)
    |> Enum.sort()
    |> Enum.map(fn name ->
      case Process.whereis(name) do
        nil -> {name, %{queue: 0, reductions: 0}}
        pid ->
          info = Process.info(pid, [:message_queue_len, :reductions])
          {name, %{
            queue: Keyword.get(info || [], :message_queue_len, 0),
            reductions: Keyword.get(info || [], :reductions, 0)
          }}
      end
    end)
  end

  defp collect_ra_server_stats do
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    for i <- 0..(shard_count - 1) do
      name = :"ferricstore_shard_#{i}"
      case Process.whereis(name) do
        nil -> {name, %{queue: 0, current_function: :not_found}}
        pid ->
          info = Process.info(pid, [:message_queue_len, :current_function])
          {name, %{
            queue: Keyword.get(info || [], :message_queue_len, 0),
            current_function: Keyword.get(info || [], :current_function, :unknown)
          }}
      end
    end
  end

  defp collect_wal_process_stats do
    case Process.whereis(:ra_ferricstore_raft_log_wal) do
      nil -> nil
      pid ->
        info = Process.info(pid, [:message_queue_len, :reductions])
        %{
          queue: Keyword.get(info || [], :message_queue_len, 0),
          reductions: Keyword.get(info || [], :reductions, 0)
        }
    end
  end

  defp collect_system_stats do
    %{
      process_count: :erlang.system_info(:process_count),
      schedulers_online: :erlang.system_info(:schedulers_online),
      total_reductions: :erlang.statistics(:reductions) |> elem(0)
    }
  end
end
