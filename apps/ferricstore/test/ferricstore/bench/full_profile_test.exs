defmodule Ferricstore.Bench.FullProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "profile ALL processes during 50 writers" do
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(0)
    {ra_name, _} = shard_id
    ra_pid = Process.whereis(ra_name)

    names = :ra_system.derive_names(:ferricstore_raft)
    wal_pid = Process.whereis(names.wal)

    # Also find the BitcaskWriter if it exists
    bw_pid = Process.whereis(:"Ferricstore.Store.BitcaskWriter.0")

    IO.puts("\n=== Process PIDs ===")
    IO.puts("  ra_server shard 0: #{inspect(ra_pid)}")
    IO.puts("  WAL:               #{inspect(wal_pid)}")
    IO.puts("  BitcaskWriter 0:   #{inspect(bw_pid)}")

    ra_samples = :ets.new(:ra_s, [:set, :public])
    wal_samples = :ets.new(:wal_s, [:set, :public])
    writer_samples = if bw_pid, do: :ets.new(:bw_s, [:set, :public])

    stop = :atomics.new(1, [])
    counter = :counters.new(1, [:atomics])

    # Poller for all processes
    poller = Task.async(fn ->
      poll = fn poll, n ->
        receive do
          :stop -> n
        after
          0 ->
            ra_info = Process.info(ra_pid, [:current_function, :status, :message_queue_len])
            wal_info = if wal_pid, do: Process.info(wal_pid, [:current_function, :status, :message_queue_len])
            bw_info = if bw_pid, do: Process.info(bw_pid, [:current_function, :status, :message_queue_len])

            :ets.insert(ra_samples, {n, ra_info})
            if wal_info, do: :ets.insert(wal_samples, {n, wal_info})
            if bw_info && writer_samples, do: :ets.insert(writer_samples, {n, bw_info})

            target = :erlang.monotonic_time(:microsecond) + 200
            bw = fn bw -> if :erlang.monotonic_time(:microsecond) < target, do: bw.(bw) end
            bw.(bw)
            poll.(poll, n + 1)
        end
      end
      poll.(poll, 0)
    end)

    # Also trace file:datasync calls on WAL for timing
    sync_counter = :counters.new(2, [:atomics])  # 1=count, 2=total_us
    if wal_pid do
      tracer = spawn(fn ->
        trace_loop = fn trace_loop ->
          receive do
            {:trace_ts, _, :call, {:file, :datasync, _}, ts} ->
              Process.put(:sync_start, ts)
              trace_loop.(trace_loop)
            {:trace_ts, _, :return_from, {:file, :datasync, _}, _, ts} ->
              case Process.get(:sync_start) do
                nil -> :ok
                start ->
                  elapsed = ts - start
                  :counters.add(sync_counter, 1, 1)
                  :counters.add(sync_counter, 2, div(elapsed, 1000))  # convert to us
              end
              trace_loop.(trace_loop)
            :stop -> :ok
            _ -> trace_loop.(trace_loop)
          end
        end
        trace_loop.(trace_loop)
      end)

      :erlang.trace(wal_pid, true, [:call, :monotonic_timestamp, {:tracer, tracer}])
      :erlang.trace_pattern({:file, :datasync, 1}, [{:_, [], [{:return_trace}]}], [:local])
      :erlang.trace_pattern({:file, :sync, 1}, [{:_, [], [{:return_trace}]}], [:local])
      Process.put(:tracer_pid, tracer)
    end

    # 50 writers for 10 seconds
    prefix = "fp_#{System.unique_integer([:positive])}"
    writers = for i <- 1..50 do
      Task.async(fn ->
        l = fn l, n ->
          if :atomics.get(stop, 1) == 1, do: throw(:stop)
          FerricStore.set("#{prefix}:#{i}:#{n}", "v")
          :counters.add(counter, 1, 1)
          l.(l, n + 1)
        end
        try do l.(l, 0) catch :throw, :stop -> :ok end
      end)
    end

    Process.sleep(10_000)
    :atomics.put(stop, 1, 1)
    send(poller.pid, :stop)
    Task.await_many(writers ++ [poller], 30_000)

    if wal_pid, do: :erlang.trace(wal_pid, false, [:all])
    if tracer_pid = Process.get(:tracer_pid), do: send(tracer_pid, :stop)

    total_writes = :counters.get(counter, 1)
    sync_count = :counters.get(sync_counter, 1)
    sync_total_us = :counters.get(sync_counter, 2)

    IO.puts("\n=== THROUGHPUT: #{div(total_writes, 10)} writes/sec (#{total_writes} total) ===")
    IO.puts("\n=== fdatasync stats ===")
    IO.puts("  Total syncs: #{sync_count}")
    IO.puts("  Syncs/sec: #{div(sync_count, 10)}")
    if sync_count > 0 do
      IO.puts("  Avg sync time: #{div(sync_total_us, sync_count)}us")
      IO.puts("  Writes per sync: #{Float.round(total_writes / sync_count, 1)}")
    end

    # Analyze each process
    writer_list = if writer_samples, do: [{"BitcaskWriter", writer_samples}], else: []

    for {label, ets_table} <- [{"ra_server", ra_samples}, {"WAL", wal_samples}] ++ writer_list do
      all = :ets.tab2list(ets_table)
      total = length(all)
      if total > 0 do
      IO.puts("\n=== #{label} (#{total} samples @ 200us) ===")

      funcs = all
      |> Enum.map(fn {_, info} ->
        case Keyword.get(info || [], :current_function) do
          {m, f, a} -> "#{inspect(m)}.#{f}/#{a}"
          other -> inspect(other)
        end
      end)
      |> Enum.frequencies()
      |> Enum.sort_by(fn {_, c} -> -c end)

      Enum.take(funcs, 15) |> Enum.each(fn {func, count} ->
        pct = Float.round(count / total * 100, 1)
        bar = String.duplicate("█", min(round(pct / 2), 40))
        IO.puts("  #{String.pad_trailing(func, 60)} #{String.pad_leading("#{pct}%", 6)} #{bar}")
      end)

      statuses = all
      |> Enum.map(fn {_, info} -> Keyword.get(info || [], :status) end)
      |> Enum.frequencies()

      IO.puts("  Status: #{inspect(statuses)}")

      queues = Enum.map(all, fn {_, info} -> Keyword.get(info || [], :message_queue_len, 0) end)
      IO.puts("  Msg queue: avg=#{div(Enum.sum(queues), max(total, 1))} max=#{Enum.max(queues)}")

      :ets.delete(ets_table)
      end  # if total > 0
    end
  end
end
