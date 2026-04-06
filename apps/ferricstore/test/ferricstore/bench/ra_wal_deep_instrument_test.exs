defmodule Ferricstore.Bench.RaWalDeepInstrumentTest do
  @moduledoc """
  Deep instrumentation of the ra WAL process to understand where wall-clock
  time is spent per batch.

  Uses three complementary techniques:

  1. **Process.info polling** at 1 ms resolution on the WAL pid -- captures
     message_queue_len, reductions, heap_size, current_function, and status.
     This tells us *where* the WAL is (datasync? write? BEAM scheduling?)
     and whether the queue is growing (back-pressure signal).

  2. **ra_counters snapshots** before and after the workload -- captures total
     batches, writes, and bytes_written to compute per-batch averages.

  3. **Erlang tracing** on `file:write/2`, `file:datasync/1`, and
     `ra_log_wal:handle_batch/2` with `{:return_trace}` and `:monotonic_timestamp`.
     The collector process receives trace messages, measures call durations,
     and builds a distribution of time per phase.

  The test prints a comprehensive breakdown showing the fraction of wall time
  spent in serialization/batching, file:write, file:datasync, and writer
  notification, plus idle time between batches.

  Run with:

      mix test test/ferricstore/bench/ra_wal_deep_instrument_test.exs --include bench

  """

  use ExUnit.Case, async: false

  @moduletag :bench

  @workload_seconds 10
  @writer_count 50
  @poll_interval_ms 1

  # ---------------------------------------------------------------
  # Test
  # ---------------------------------------------------------------

  test "deep WAL batch-level instrumentation" do
    # ------ 1. Locate the WAL process ------
    names = :ra_system.derive_names(:ferricstore_raft)
    wal_name = names.wal
    wal_pid = Process.whereis(wal_name)
    assert is_pid(wal_pid), "WAL process not found for #{inspect(wal_name)}"

    IO.puts("\n=== Deep ra WAL Instrumentation ===")
    IO.puts("WAL process: #{inspect(wal_name)} (#{inspect(wal_pid)})")
    IO.puts("Writers: #{@writer_count}  |  Duration: #{@workload_seconds}s")
    IO.puts("Poll interval: #{@poll_interval_ms}ms\n")

    # ------ 2. Snapshot counters before ------
    counters_before = read_wal_counters(wal_name)
    IO.puts("Counters before: #{inspect(counters_before)}")

    # ------ 3. Start the trace collector ------
    collector_pid = start_trace_collector()

    # Set up tracing on the WAL process with monotonic timestamps.
    # We trace file:write/2, file:datasync/1, and ra_log_wal:handle_batch/2
    # to measure per-call durations.
    setup_tracing(wal_pid, collector_pid)

    # ------ 4. Start the poller ------
    poll_ets = :ets.new(:wal_poll_samples, [:ordered_set, :public])
    poller_ref = start_poller(wal_pid, poll_ets)

    # ------ 5. Run the workload ------
    t_start = System.monotonic_time(:microsecond)
    stop_flag = :atomics.new(1, [])
    prefix = "wal_deep_#{System.unique_integer([:positive])}"

    writers =
      for i <- 1..@writer_count do
        Task.async(fn ->
          writer_loop(prefix, i, stop_flag, 0)
        end)
      end

    Process.sleep(@workload_seconds * 1_000)
    :atomics.put(stop_flag, 1, 1)

    write_counts = Task.await_many(writers, 30_000)
    t_end = System.monotonic_time(:microsecond)
    wall_us = t_end - t_start
    total_writes = Enum.sum(write_counts)

    # ------ 6. Stop tracing & polling ------
    :erlang.trace(wal_pid, false, [:all])
    stop_tracing_patterns()
    stop_poller(poller_ref)

    # ------ 7. Snapshot counters after ------
    counters_after = read_wal_counters(wal_name)

    # ------ 8. Collect trace data ------
    trace_data = collect_trace_data(collector_pid)

    # ------ 9. Collect poll data ------
    poll_samples = :ets.tab2list(poll_ets)
    :ets.delete(poll_ets)

    # ------ 10. Analyze and print ------
    analyze_and_print(
      counters_before,
      counters_after,
      trace_data,
      poll_samples,
      wall_us,
      total_writes
    )
  end

  # ---------------------------------------------------------------
  # Writer loop
  # ---------------------------------------------------------------

  defp writer_loop(prefix, writer_id, stop_flag, n) do
    if :atomics.get(stop_flag, 1) == 1 do
      n
    else
      key = "#{prefix}:#{writer_id}:#{n}"
      FerricStore.set(key, "v#{n}")
      writer_loop(prefix, writer_id, stop_flag, n + 1)
    end
  end

  # ---------------------------------------------------------------
  # Counter reading
  # ---------------------------------------------------------------

  defp read_wal_counters(wal_name) do
    case :ra_counters.overview(wal_name) do
      nil -> %{batches: 0, writes: 0, bytes_written: 0, wal_files: 0}
      map when is_map(map) -> map
    end
  end

  # ---------------------------------------------------------------
  # Tracing
  # ---------------------------------------------------------------

  defp setup_tracing(wal_pid, collector_pid) do
    # Enable call tracing on the WAL process, directing messages to our collector.
    # :monotonic_timestamp gives high-resolution monotonic timestamps on each
    # trace message, allowing us to measure call durations without wall-clock skew.
    :erlang.trace(wal_pid, true, [
      :call,
      :monotonic_timestamp,
      {:tracer, collector_pid}
    ])

    # Trace handle_batch/2 with return_trace to measure total batch time.
    :erlang.trace_pattern(
      {:ra_log_wal, :handle_batch, 2},
      [{:_, [], [{:return_trace}]}],
      [:local]
    )

    # Trace flush_pending/1 to measure write+sync time.
    :erlang.trace_pattern(
      {:ra_log_wal, :flush_pending, 1},
      [{:_, [], [{:return_trace}]}],
      [:local]
    )

    # Trace complete_batch/1 to measure notification time.
    :erlang.trace_pattern(
      {:ra_log_wal, :complete_batch, 1},
      [{:_, [], [{:return_trace}]}],
      [:local]
    )

    # Trace file:write/2 to isolate the write() syscall.
    :erlang.trace_pattern(
      {:file, :write, 2},
      [{:_, [], [{:return_trace}]}],
      [:local]
    )

    # Trace file:datasync/1 to isolate fdatasync().
    :erlang.trace_pattern(
      {:file, :datasync, 1},
      [{:_, [], [{:return_trace}]}],
      [:local]
    )
  end

  defp stop_tracing_patterns do
    :erlang.trace_pattern({:ra_log_wal, :handle_batch, 2}, false, [:local])
    :erlang.trace_pattern({:ra_log_wal, :flush_pending, 1}, false, [:local])
    :erlang.trace_pattern({:ra_log_wal, :complete_batch, 1}, false, [:local])
    :erlang.trace_pattern({:file, :write, 2}, false, [:local])
    :erlang.trace_pattern({:file, :datasync, 1}, false, [:local])
  end

  # ---------------------------------------------------------------
  # Trace collector process
  # ---------------------------------------------------------------

  defp start_trace_collector do
    parent = self()

    spawn_link(fn ->
      # State: map of {module, function, arity} => list of call start timestamps
      # and a list of completed measurements: {mfa, duration_native}
      collector_loop(%{pending: %{}, completed: []}, parent)
    end)
  end

  defp collector_loop(state, parent) do
    receive do
      # Call entry: {:trace_ts, Pid, :call, {M, F, Args}, Timestamp}
      {:trace_ts, _pid, :call, {m, f, args}, ts} ->
        arity = length(args)
        mfa = {m, f, arity}
        pending = Map.update(state.pending, mfa, [ts], fn stack -> [ts | stack] end)
        collector_loop(%{state | pending: pending}, parent)

      # Return: {:trace_ts, Pid, :return_from, {M, F, Arity}, _ReturnVal, Timestamp}
      {:trace_ts, _pid, :return_from, {m, f, arity}, _ret, ts} ->
        mfa = {m, f, arity}

        case Map.get(state.pending, mfa, []) do
          [call_ts | rest] ->
            # Convert native monotonic to microseconds.
            # :erlang.convert_time_unit works on monotonic timestamps.
            duration_us =
              :erlang.convert_time_unit(ts - call_ts, :native, :microsecond)

            pending = Map.put(state.pending, mfa, rest)
            completed = [{mfa, duration_us} | state.completed]
            collector_loop(%{state | pending: pending, completed: completed}, parent)

          [] ->
            # Spurious return without matching call -- skip.
            collector_loop(state, parent)
        end

      {:get_data, from} ->
        send(from, {:trace_data, state.completed})
        collector_loop(state, parent)

      {:stop, from} ->
        send(from, {:trace_data, state.completed})

      _other ->
        # Ignore unexpected messages (e.g. other trace events).
        collector_loop(state, parent)
    end
  end

  defp collect_trace_data(collector_pid) do
    send(collector_pid, {:stop, self()})

    receive do
      {:trace_data, data} -> data
    after
      5_000 ->
        IO.puts("WARNING: Timeout collecting trace data")
        []
    end
  end

  # ---------------------------------------------------------------
  # Process.info poller
  # ---------------------------------------------------------------

  defp start_poller(wal_pid, ets_tid) do
    parent = self()
    stop_ref = make_ref()

    pid =
      spawn_link(fn ->
        poll_loop(wal_pid, ets_tid, 0, stop_ref, parent)
      end)

    {pid, stop_ref}
  end

  defp poll_loop(wal_pid, ets_tid, n, stop_ref, parent) do
    receive do
      {:stop, ^stop_ref} ->
        send(parent, {:poller_done, stop_ref})
    after
      @poll_interval_ms ->
        ts = System.monotonic_time(:microsecond)

        info =
          Process.info(wal_pid, [
            :message_queue_len,
            :reductions,
            :heap_size,
            :total_heap_size,
            :current_function,
            :status
          ])

        :ets.insert(ets_tid, {n, ts, info})
        poll_loop(wal_pid, ets_tid, n + 1, stop_ref, parent)
    end
  end

  defp stop_poller({pid, stop_ref}) do
    send(pid, {:stop, stop_ref})

    receive do
      {:poller_done, ^stop_ref} -> :ok
    after
      2_000 -> :ok
    end
  end

  # ---------------------------------------------------------------
  # Analysis and reporting
  # ---------------------------------------------------------------

  defp analyze_and_print(
         counters_before,
         counters_after,
         trace_data,
         poll_samples,
         wall_us,
         total_writes
       ) do
    wall_ms = wall_us / 1_000
    wall_s = wall_us / 1_000_000

    # --- Counter deltas ---
    delta_batches = Map.get(counters_after, :batches, 0) - Map.get(counters_before, :batches, 0)
    delta_writes = Map.get(counters_after, :writes, 0) - Map.get(counters_before, :writes, 0)

    delta_bytes =
      Map.get(counters_after, :bytes_written, 0) - Map.get(counters_before, :bytes_written, 0)

    delta_wal_files =
      Map.get(counters_after, :wal_files, 0) - Map.get(counters_before, :wal_files, 0)

    writes_per_batch = if delta_batches > 0, do: delta_writes / delta_batches, else: 0
    bytes_per_batch = if delta_batches > 0, do: delta_bytes / delta_batches, else: 0
    batches_per_sec = if wall_s > 0, do: delta_batches / wall_s, else: 0
    writes_per_sec = if wall_s > 0, do: total_writes / wall_s, else: 0

    # --- Trace analysis ---
    # Group trace data by MFA
    grouped = Enum.group_by(trace_data, fn {mfa, _dur} -> mfa end, fn {_mfa, dur} -> dur end)

    handle_batch_durations = Map.get(grouped, {:ra_log_wal, :handle_batch, 2}, [])
    flush_pending_durations = Map.get(grouped, {:ra_log_wal, :flush_pending, 1}, [])
    complete_batch_durations = Map.get(grouped, {:ra_log_wal, :complete_batch, 1}, [])
    file_write_durations = Map.get(grouped, {:file, :write, 2}, [])
    file_datasync_durations = Map.get(grouped, {:file, :datasync, 1}, [])

    # Calculate totals
    total_handle_batch_us = Enum.sum(handle_batch_durations)
    total_flush_pending_us = Enum.sum(flush_pending_durations)
    total_complete_batch_us = Enum.sum(complete_batch_durations)
    total_file_write_us = Enum.sum(file_write_durations)
    total_file_datasync_us = Enum.sum(file_datasync_durations)

    # Derived: serialization time = handle_batch - complete_batch
    # (handle_batch includes: foldr over ops (serialize+build pending) + complete_batch)
    # complete_batch includes: flush_pending + notify writers
    # flush_pending includes: file:write + file:datasync
    # notify time = complete_batch - flush_pending
    total_serialization_us = max(0, total_handle_batch_us - total_complete_batch_us)
    total_notify_us = max(0, total_complete_batch_us - total_flush_pending_us)
    total_write_syscall_us = total_file_write_us
    total_datasync_us = total_file_datasync_us
    # Overhead inside flush_pending beyond write+datasync
    total_flush_overhead_us = max(0, total_flush_pending_us - total_file_write_us - total_file_datasync_us)
    # Idle time = wall time - total handle_batch time
    total_idle_us = max(0, wall_us - total_handle_batch_us)

    # Per-batch averages (from trace counts, not ra_counters, to stay consistent)
    traced_batch_count = length(handle_batch_durations)

    avg = fn durations ->
      if durations != [], do: Enum.sum(durations) / length(durations), else: 0
    end

    p50 = fn durations -> percentile(durations, 50) end
    p99 = fn durations -> percentile(durations, 99) end
    max_val = fn durations -> if durations == [], do: 0, else: Enum.max(durations) end

    # --- Poll analysis ---
    {queue_stats, function_freq, status_freq, reduction_rate} = analyze_polls(poll_samples)

    # --- Print report ---
    report = """

    ================================================================================
                         DEEP ra WAL ANALYSIS REPORT
    ================================================================================

    Workload: #{@writer_count} writers x #{@workload_seconds}s
    Total client writes completed: #{total_writes}
    Client write throughput: #{Float.round(writes_per_sec, 0)} writes/sec
    Wall clock: #{Float.round(wall_ms, 1)} ms

    ── ra_counters (WAL-side) ──────────────────────────────────────────────────────

      WAL batches:     #{delta_batches}
      WAL writes:      #{delta_writes}
      Bytes written:   #{format_bytes(delta_bytes)}
      WAL files:       #{delta_wal_files} rollovers
      Batches/sec:     #{Float.round(batches_per_sec, 1)}
      Writes/batch:    #{Float.round(writes_per_batch, 1)}
      Bytes/batch:     #{format_bytes(round(bytes_per_batch))}

    ── Traced function call counts ─────────────────────────────────────────────────

      handle_batch/2:  #{length(handle_batch_durations)} calls
      complete_batch/1:#{length(complete_batch_durations)} calls
      flush_pending/1: #{length(flush_pending_durations)} calls
      file:write/2:    #{length(file_write_durations)} calls
      file:datasync/1: #{length(file_datasync_durations)} calls

    ── Per-batch timing (#{traced_batch_count} traced batches) ─────────────────────

                           avg (us)     p50 (us)     p99 (us)     max (us)
      handle_batch/2    #{pad(avg.(handle_batch_durations))} #{pad(p50.(handle_batch_durations))} #{pad(p99.(handle_batch_durations))} #{pad(max_val.(handle_batch_durations))}
      complete_batch/1  #{pad(avg.(complete_batch_durations))} #{pad(p50.(complete_batch_durations))} #{pad(p99.(complete_batch_durations))} #{pad(max_val.(complete_batch_durations))}
      flush_pending/1   #{pad(avg.(flush_pending_durations))} #{pad(p50.(flush_pending_durations))} #{pad(p99.(flush_pending_durations))} #{pad(max_val.(flush_pending_durations))}
      file:write/2      #{pad(avg.(file_write_durations))} #{pad(p50.(file_write_durations))} #{pad(p99.(file_write_durations))} #{pad(max_val.(file_write_durations))}
      file:datasync/1   #{pad(avg.(file_datasync_durations))} #{pad(p50.(file_datasync_durations))} #{pad(p99.(file_datasync_durations))} #{pad(max_val.(file_datasync_durations))}

    ── Wall-time breakdown (total #{Float.round(wall_ms, 1)} ms) ──────────────────

      Serialization (foldr ops):  #{format_us(total_serialization_us)} (#{pct(total_serialization_us, wall_us)})
        term_to_iovec + header + incr_batch per op, accumulated across all batches

      file:write (write syscall):  #{format_us(total_write_syscall_us)} (#{pct(total_write_syscall_us, wall_us)})
        Single write() call per batch for the accumulated pending iolist

      file:datasync (fdatasync):   #{format_us(total_datasync_us)} (#{pct(total_datasync_us, wall_us)})
        One fdatasync per batch — THE KNOWN BOTTLENECK

      flush_pending overhead:      #{format_us(total_flush_overhead_us)} (#{pct(total_flush_overhead_us, wall_us)})
        Time in flush_pending beyond write+datasync (Erlang overhead)

      Writer notification:         #{format_us(total_notify_us)} (#{pct(total_notify_us, wall_us)})
        Pid ! {ra_log_event, {written, ...}} per writer in the batch

      Idle (between batches):      #{format_us(total_idle_us)} (#{pct(total_idle_us, wall_us)})
        Time WAL spends in gen_batch_server receive loop waiting for messages

    ── Process.info polling (#{length(poll_samples)} samples @ #{@poll_interval_ms}ms) ────────

      Message queue length:
        avg=#{Float.round(queue_stats.avg, 1)}, median=#{queue_stats.median}, p99=#{queue_stats.p99}, max=#{queue_stats.max}
    #{format_queue_histogram(queue_stats.histogram)}
      Process status distribution:
    #{format_frequency(status_freq)}
      Current function distribution (top 10):
    #{format_frequency(Enum.take(function_freq, 10))}
      Reduction rate: ~#{format_number(reduction_rate)} reductions/sec

    ================================================================================
    """

    IO.puts(report)

    # Also write to docs/
    write_report(report)
  end

  # ---------------------------------------------------------------
  # Poll analysis helpers
  # ---------------------------------------------------------------

  defp analyze_polls([]) do
    empty_stats = %{avg: 0, median: 0, p99: 0, max: 0, histogram: []}
    {empty_stats, [], [], 0}
  end

  defp analyze_polls(samples) do
    queue_lens =
      samples
      |> Enum.map(fn {_n, _ts, info} -> Keyword.get(info, :message_queue_len, 0) end)

    sorted_queue = Enum.sort(queue_lens)
    count = length(sorted_queue)

    queue_stats = %{
      avg: Enum.sum(sorted_queue) / max(count, 1),
      median: Enum.at(sorted_queue, div(count, 2), 0),
      p99: Enum.at(sorted_queue, round(count * 0.99) - 1, 0),
      max: Enum.max(sorted_queue, fn -> 0 end),
      histogram: build_histogram(sorted_queue)
    }

    # Function frequency
    functions =
      samples
      |> Enum.map(fn {_n, _ts, info} -> Keyword.get(info, :current_function) end)
      |> Enum.reject(&is_nil/1)
      |> Enum.frequencies()
      |> Enum.sort_by(fn {_k, v} -> -v end)

    # Status frequency
    statuses =
      samples
      |> Enum.map(fn {_n, _ts, info} -> Keyword.get(info, :status) end)
      |> Enum.reject(&is_nil/1)
      |> Enum.frequencies()
      |> Enum.sort_by(fn {_k, v} -> -v end)

    # Reduction rate (reductions delta / time delta)
    reduction_rate =
      case samples do
        [{_, ts1, info1} | _] ->
          {_, ts_last, info_last} = List.last(samples)
          r1 = Keyword.get(info1, :reductions, 0)
          r_last = Keyword.get(info_last, :reductions, 0)
          dt_s = (ts_last - ts1) / 1_000_000

          if dt_s > 0, do: (r_last - r1) / dt_s, else: 0

        _ ->
          0
      end

    {queue_stats, functions, statuses, reduction_rate}
  end

  defp build_histogram(sorted_values) do
    # Build buckets: 0, 1-5, 6-10, 11-50, 51-100, 100+
    buckets = [
      {"    0", fn v -> v == 0 end},
      {"  1-5", fn v -> v >= 1 and v <= 5 end},
      {" 6-10", fn v -> v >= 6 and v <= 10 end},
      {"11-50", fn v -> v >= 11 and v <= 50 end},
      {"51-100", fn v -> v >= 51 and v <= 100 end},
      {" >100", fn v -> v > 100 end}
    ]

    total = length(sorted_values)

    Enum.map(buckets, fn {label, predicate} ->
      count = Enum.count(sorted_values, predicate)
      pct = if total > 0, do: count / total * 100, else: 0
      {label, count, pct}
    end)
  end

  # ---------------------------------------------------------------
  # Statistics helpers
  # ---------------------------------------------------------------

  defp percentile([], _p), do: 0

  defp percentile(values, p) do
    sorted = Enum.sort(values)
    count = length(sorted)
    idx = max(0, round(count * p / 100) - 1)
    Enum.at(sorted, idx, 0)
  end

  # ---------------------------------------------------------------
  # Formatting helpers
  # ---------------------------------------------------------------

  defp pad(value) when is_float(value), do: String.pad_leading(Float.round(value, 0) |> round() |> Integer.to_string(), 12)
  defp pad(value) when is_integer(value), do: String.pad_leading(Integer.to_string(value), 12)

  defp format_us(us) when is_number(us) do
    cond do
      us >= 1_000_000 -> "#{Float.round(us / 1_000_000, 2)}s"
      us >= 1_000 -> "#{Float.round(us / 1_000, 2)}ms"
      true -> "#{round(us)}us"
    end
    |> String.pad_leading(10)
  end

  defp pct(part, total) when total > 0 do
    "#{Float.round(part / total * 100, 1)}%"
  end

  defp pct(_, _), do: "-%"

  defp format_bytes(bytes) when is_number(bytes) do
    cond do
      bytes >= 1_073_741_824 -> "#{Float.round(bytes / 1_073_741_824, 2)} GiB"
      bytes >= 1_048_576 -> "#{Float.round(bytes / 1_048_576, 2)} MiB"
      bytes >= 1_024 -> "#{Float.round(bytes / 1_024, 2)} KiB"
      true -> "#{bytes} B"
    end
  end

  defp format_number(n) when is_float(n) do
    cond do
      n >= 1_000_000_000 -> "#{Float.round(n / 1_000_000_000, 1)}B"
      n >= 1_000_000 -> "#{Float.round(n / 1_000_000, 1)}M"
      n >= 1_000 -> "#{Float.round(n / 1_000, 1)}K"
      true -> "#{Float.round(n, 0)}"
    end
  end

  defp format_number(n) when is_integer(n), do: format_number(n * 1.0)

  defp format_queue_histogram(histogram) do
    max_count = histogram |> Enum.map(fn {_, c, _} -> c end) |> Enum.max(fn -> 1 end)

    Enum.map_join(histogram, "\n", fn {label, count, pct} ->
      bar_len = if max_count > 0, do: round(count / max_count * 40), else: 0
      bar = String.duplicate("#", bar_len)
      "        #{label}: #{String.pad_leading(Integer.to_string(count), 6)} (#{Float.round(pct, 1) |> :erlang.float_to_binary(decimals: 1)}%) |#{bar}"
    end)
  end

  defp format_frequency(freq_list) do
    total = freq_list |> Enum.map(fn {_, c} -> c end) |> Enum.sum()

    Enum.map_join(freq_list, "\n", fn {key, count} ->
      pct = if total > 0, do: count / total * 100, else: 0
      "        #{inspect(key)}: #{count} (#{Float.round(pct, 1)}%)"
    end)
  end

  # ---------------------------------------------------------------
  # Report writer
  # ---------------------------------------------------------------

  defp write_report(report) do
    docs_dir = Path.join(File.cwd!(), "docs")
    File.mkdir_p!(docs_dir)
    path = Path.join(docs_dir, "ra-wal-deep-analysis.md")

    timestamp = DateTime.utc_now() |> DateTime.to_iso8601()

    content = """
    # ra WAL Deep Analysis

    Generated: #{timestamp}

    ```
    #{String.trim(report)}
    ```

    ## How to read this report

    ### Wall-time breakdown

    The WAL process spends its time in these phases per batch:

    1. **Serialization** -- `lists:foldr(handle_op/2, ...)` iterates all ops in the batch.
       For each op: `term_to_iovec` (ETF serialization), header construction, and
       `incr_batch` (append to pending iolist + update waiting map). This is CPU-bound
       BEAM work.

    2. **file:write** -- single `write(2)` syscall for the accumulated pending iolist.
       Typically fast (kernel buffer copy), but can stall if the OS write buffer is full.

    3. **file:datasync** -- `fdatasync(2)` forces the OS to flush data to disk.
       This is the known bottleneck (~20ms on typical SSDs).

    4. **Writer notification** -- after flush, the WAL sends `Pid ! {ra_log_event, ...}`
       to each writer pid in the batch. This is O(writers_in_batch) message sends.

    5. **Idle** -- time spent in `gen_batch_server:loop_wait/2` receive loop waiting
       for new messages. High idle % means the WAL has spare capacity.

    ### Message queue length

    If the queue is consistently > 0, the WAL cannot keep up with incoming writes.
    The queue depth indicates how many ops are waiting to be batched.

    ### Current function distribution

    Shows where the WAL process's instruction pointer is most often found.
    `file:datasync/1` dominance confirms fsync is the bottleneck.
    `gen_batch_server:loop_wait/2` indicates idle time.
    """

    File.write!(path, content)
    IO.puts("Report written to: #{path}")
  end
end
