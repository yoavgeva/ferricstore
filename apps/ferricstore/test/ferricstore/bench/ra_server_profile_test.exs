defmodule Ferricstore.Bench.RaServerProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 60_000

  test "profile ra_server during 50 writers" do
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(0)
    {name, _node} = shard_id
    ra_pid = Process.whereis(name)

    samples = :ets.new(:prof_samples, [:set, :public])
    stop = :atomics.new(1, [])

    # Poller: sample ra_server every 500us
    poller = Task.async(fn ->
      poll = fn poll, n ->
        receive do
          :stop -> n
        after
          0 ->
            info = Process.info(ra_pid, [:current_function, :status, :message_queue_len])
            :ets.insert(samples, {n, info})
            # busy wait 500us
            target = :erlang.monotonic_time(:microsecond) + 500
            bw = fn bw -> if :erlang.monotonic_time(:microsecond) < target, do: bw.(bw) end
            bw.(bw)
            poll.(poll, n + 1)
        end
      end
      poll.(poll, 0)
    end)

    # 50 writers for 5 seconds
    prefix = "prof_#{System.unique_integer([:positive])}"
    writers = for i <- 1..50 do
      Task.async(fn ->
        l = fn l, n ->
          if :atomics.get(stop, 1) == 1, do: throw(:stop)
          FerricStore.set("#{prefix}:#{i}:#{n}", "v")
          l.(l, n + 1)
        end
        try do l.(l, 0) catch :throw, :stop -> :ok end
      end)
    end

    Process.sleep(5_000)
    :atomics.put(stop, 1, 1)
    send(poller.pid, :stop)
    Task.await_many(writers ++ [poller], 30_000)

    # Analyze
    all = :ets.tab2list(samples)
    total = length(all)

    funcs = all
    |> Enum.map(fn {_n, info} ->
      case Keyword.get(info || [], :current_function) do
        {m, f, a} -> "#{inspect(m)}.#{f}/#{a}"
        other -> inspect(other)
      end
    end)
    |> Enum.frequencies()
    |> Enum.sort_by(fn {_, c} -> -c end)

    IO.puts("\n=== ra_server Profile (#{total} samples @ 500us) ===\n")
    Enum.take(funcs, 20) |> Enum.each(fn {func, count} ->
      pct = Float.round(count / total * 100, 1)
      bar = String.duplicate("█", min(round(pct / 2), 40))
      IO.puts("  #{String.pad_trailing(func, 65)} #{String.pad_leading("#{pct}%", 6)} #{bar}")
    end)

    statuses = all
    |> Enum.map(fn {_n, info} -> Keyword.get(info || [], :status) end)
    |> Enum.frequencies()

    IO.puts("\n  Status:")
    Enum.each(statuses, fn {s, c} ->
      IO.puts("    #{s}: #{Float.round(c / total * 100, 1)}%")
    end)

    queues = Enum.map(all, fn {_n, info} -> Keyword.get(info || [], :message_queue_len, 0) end)
    IO.puts("\n  Message queue: avg=#{div(Enum.sum(queues), max(total, 1))}  max=#{Enum.max(queues)}")

    :ets.delete(samples)
  end
end
