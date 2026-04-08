defmodule Ferricstore.Cluster.ThroughputBenchTest do
  @moduledoc """
  Realistic throughput benchmarks for FerricStore's Raft consensus layer.

  Simulates production-like traffic patterns with multiple concurrent
  connections hitting a multi-member Raft cluster. Each test runs for a
  fixed duration (30 seconds) and reports throughput, latency percentiles,
  resource usage (CPU, memory), and data integrity verification results.

  Both reads and writes are batched at #{50} operations per iteration so
  that ops/sec numbers are directly comparable across workload mixes.

  ## Tests

  1. **5-node write-heavy (80/20)** -- 50 concurrent workers, 80% batched writes / 20% batched reads
  2. **5-node read-heavy (20/80)** -- 50 concurrent workers, 20% writes / 80% batched reads
  3. **5-node write-only** -- 50 concurrent workers, 100% batched writes
  4. **3-node write-heavy (80/20)** -- 30 concurrent workers (10 per node)
  5. **Latency under load** -- 10 heavy writers + 1 latency probe measuring p50/p95/p99

  ## Running

      cd apps/ferricstore && mix test test/ferricstore/cluster/throughput_bench_test.exs --include bench --timeout 600000
  """

  use ExUnit.Case, async: false

  require Logger

  @moduletag :bench

  @ra_timeout 10_000
  @machine {:module, Ferricstore.Test.KvMachine, %{}}
  @batch_size 50

  # Duration for each benchmark run in milliseconds.
  @bench_duration_ms 30_000

  # Interval for periodic memory sampling (milliseconds).
  @memory_sample_interval_ms 5_000

  # ---------------------------------------------------------------------------
  # Setup / Teardown
  # ---------------------------------------------------------------------------

  setup_all do
    tmp_base =
      Path.join(
        System.tmp_dir!(),
        "ferric_bench_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp_base)

    systems = [
      :bench_5n_write_heavy,
      :bench_5n_read_heavy,
      :bench_5n_write_only,
      :bench_3n_write_heavy,
      :bench_latency
    ]

    for sys <- systems do
      data_dir = Path.join(tmp_base, "#{sys}")
      File.mkdir_p!(data_dir)

      names = :ra_system.derive_names(sys)

      config = %{
        name: sys,
        names: names,
        data_dir: to_charlist(data_dir),
        wal_data_dir: to_charlist(data_dir),
        segment_max_entries: 8192
      }

      case :ra_system.start(config) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end

    on_exit(fn ->
      prefixes = [
        {"b5wh_m_", 5},
        {"b5rh_m_", 5},
        {"b5wo_m_", 5},
        {"b3wh_m_", 3},
        {"blat_m_", 5}
      ]

      for {{prefix, max_i}, sys} <- Enum.zip(prefixes, systems),
          i <- 1..max_i do
        server_id = {:"#{prefix}#{i}", node()}

        try do
          :ra.stop_server(sys, server_id)
        catch
          _, _ -> :ok
        end

        try do
          :ra.force_delete_server(sys, server_id)
        catch
          _, _ -> :ok
        end
      end

      Process.sleep(500)
      File.rm_rf!(tmp_base)
    end)

    %{tmp_base: tmp_base}
  end

  # ---------------------------------------------------------------------------
  # Test 1: 5-node, 50 connections, write-heavy (80/20)
  # ---------------------------------------------------------------------------

  describe "5-node write-heavy (80/20)" do
    @tag :bench
    @tag timeout: 300_000
    test "50 concurrent connections, 80% writes 20% reads, 30 seconds" do
      run_benchmark(
        ra_system: :bench_5n_write_heavy,
        cluster_name: :bench_5n_wh_cluster,
        member_prefix: "b5wh_m_",
        cluster_size: 5,
        num_workers: 50,
        write_pct: 80,
        label: "5-Node Write-Heavy (80/20)"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Test 2: 5-node, 50 connections, read-heavy (20/80)
  # ---------------------------------------------------------------------------

  describe "5-node read-heavy (20/80)" do
    @tag :bench
    @tag timeout: 300_000
    test "50 concurrent connections, 20% writes 80% reads, 30 seconds" do
      run_benchmark(
        ra_system: :bench_5n_read_heavy,
        cluster_name: :bench_5n_rh_cluster,
        member_prefix: "b5rh_m_",
        cluster_size: 5,
        num_workers: 50,
        write_pct: 20,
        label: "5-Node Read-Heavy (20/80)"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Test 3: 5-node, 50 connections, write-only
  # ---------------------------------------------------------------------------

  describe "5-node write-only" do
    @tag :bench
    @tag timeout: 300_000
    test "50 concurrent connections, 100% writes, 30 seconds" do
      run_benchmark(
        ra_system: :bench_5n_write_only,
        cluster_name: :bench_5n_wo_cluster,
        member_prefix: "b5wo_m_",
        cluster_size: 5,
        num_workers: 50,
        write_pct: 100,
        label: "5-Node Write-Only (100/0)"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Test 4: 3-node, 30 connections, write-heavy (80/20)
  # ---------------------------------------------------------------------------

  describe "3-node write-heavy (80/20)" do
    @tag :bench
    @tag timeout: 300_000
    test "30 concurrent connections (10 per node), 80% writes 20% reads, 30 seconds" do
      run_benchmark(
        ra_system: :bench_3n_write_heavy,
        cluster_name: :bench_3n_wh_cluster,
        member_prefix: "b3wh_m_",
        cluster_size: 3,
        num_workers: 30,
        write_pct: 80,
        label: "3-Node Write-Heavy (80/20)"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Test 5: Latency under load
  # ---------------------------------------------------------------------------

  describe "latency under load" do
    @tag :bench
    @tag timeout: 300_000
    test "p50/p95/p99 latency while 10 writers generate background load" do
      ra_system = :bench_latency
      cluster_size = 5
      members = for i <- 1..cluster_size, do: {:"blat_m_#{i}", node()}

      Logger.info("=== Latency Under Load: forming #{cluster_size}-node cluster ===")

      {:ok, started, not_started} =
        :ra.start_cluster(ra_system, :bench_lat_cluster, @machine, members)

      assert length(started) == cluster_size,
             "All #{cluster_size} members should start, not_started: #{inspect(not_started)}"

      leader = wait_for_stable_leader(members)

      # Seed some data for reads
      seed_data(leader, "lat_seed", 5_000)

      # ETS tables for latency samples
      write_lat_tab = :ets.new(:lat_write, [:bag, :public])
      read_lat_tab = :ets.new(:lat_read, [:bag, :public])
      batch_lat_tab = :ets.new(:lat_batch, [:bag, :public])

      {:ok, leader_agent} = Agent.start_link(fn -> leader end)
      stop_flag = :atomics.new(1, [])

      # --- Resource metrics: capture baseline ---
      mem_start = :erlang.memory(:total)
      :erlang.system_flag(:scheduler_wall_time, true)
      sched_start = :erlang.statistics(:scheduler_wall_time)
      {reductions_start, _} = :erlang.statistics(:reductions)

      # Start 10 background heavy writers
      bg_tasks =
        for w <- 1..10 do
          Task.async(fn ->
            background_writer_loop(ra_system, members, leader_agent, stop_flag, w)
          end)
        end

      # Let background load stabilize
      Process.sleep(2_000)

      # Run latency probe for the remaining duration
      probe_duration_ms = @bench_duration_ms - 2_000

      probe_task =
        Task.async(fn ->
          latency_probe_loop(
            members,
            leader_agent,
            stop_flag,
            write_lat_tab,
            read_lat_tab,
            batch_lat_tab,
            System.monotonic_time(:millisecond) + probe_duration_ms
          )
        end)

      Task.await(probe_task, probe_duration_ms + 30_000)

      # Stop background writers
      :atomics.put(stop_flag, 1, 1)

      bg_results =
        Enum.map(bg_tasks, fn task ->
          Task.await(task, 30_000)
        end)

      # --- Resource metrics: capture end ---
      mem_end = :erlang.memory(:total)
      sched_end = :erlang.statistics(:scheduler_wall_time)
      {reductions_end, _} = :erlang.statistics(:reductions)
      :erlang.system_flag(:scheduler_wall_time, false)

      Agent.stop(leader_agent)

      total_bg_writes =
        Enum.reduce(bg_results, 0, fn {writes, _failures}, acc -> acc + writes end)

      # Compute percentiles
      write_latencies = ets_to_sorted_list(write_lat_tab)
      read_latencies = ets_to_sorted_list(read_lat_tab)
      batch_latencies = ets_to_sorted_list(batch_lat_tab)

      :ets.delete(write_lat_tab)
      :ets.delete(read_lat_tab)
      :ets.delete(batch_lat_tab)

      # Compute scheduler utilization
      avg_util = compute_scheduler_utilization(sched_start, sched_end)
      num_schedulers = length(Enum.sort(sched_end || []))
      total_reductions = reductions_end - reductions_start
      duration_s = @bench_duration_ms / 1_000

      report = """

      === Latency Under Load — 5 nodes, 10 background writers, #{duration_s}s ===
        Background write throughput: #{format_number(total_bg_writes)} writes (#{format_number(div(total_bg_writes * 1000, max(@bench_duration_ms, 1)))}/sec)

        Single-key WRITE latency (#{length(write_latencies)} samples):
          p50=#{format_latency(percentile(write_latencies, 50))}  p95=#{format_latency(percentile(write_latencies, 95))}  p99=#{format_latency(percentile(write_latencies, 99))}

        Single-key READ latency (#{length(read_latencies)} samples):
          p50=#{format_latency(percentile(read_latencies, 50))}  p95=#{format_latency(percentile(read_latencies, 95))}  p99=#{format_latency(percentile(read_latencies, 99))}

        Batch WRITE latency (#{length(batch_latencies)} samples, #{@batch_size} keys/batch):
          p50=#{format_latency(percentile(batch_latencies, 50))}  p95=#{format_latency(percentile(batch_latencies, 95))}  p99=#{format_latency(percentile(batch_latencies, 99))}

        --- Resource Usage ---
        Memory at start:      #{format_bytes(mem_start)}
        Memory at end:        #{format_bytes(mem_end)}
        Memory delta:         +#{format_bytes(mem_end - mem_start)}

        Scheduler utilization: #{Float.round(avg_util, 1)}% avg (#{num_schedulers} schedulers)
        Reductions:           #{format_number(total_reductions)} total (#{format_number(round(total_reductions / max(duration_s, 0.001)))}/sec)
      """

      Logger.info(report)

      # Sanity: we should have gathered a meaningful number of samples
      assert length(write_latencies) > 100,
             "Expected > 100 write latency samples, got #{length(write_latencies)}"

      assert length(read_latencies) > 100,
             "Expected > 100 read latency samples, got #{length(read_latencies)}"
    end
  end

  # ===========================================================================
  # Shared benchmark runner
  # ===========================================================================

  defp run_benchmark(opts) do
    ra_system = Keyword.fetch!(opts, :ra_system)
    cluster_name = Keyword.fetch!(opts, :cluster_name)
    member_prefix = Keyword.fetch!(opts, :member_prefix)
    cluster_size = Keyword.fetch!(opts, :cluster_size)
    num_workers = Keyword.fetch!(opts, :num_workers)
    write_pct = Keyword.fetch!(opts, :write_pct)
    label = Keyword.fetch!(opts, :label)

    members = for i <- 1..cluster_size, do: {:"#{member_prefix}#{i}", node()}

    Logger.info("=== #{label}: forming #{cluster_size}-node cluster ===")

    {:ok, started, not_started} =
      :ra.start_cluster(ra_system, cluster_name, @machine, members)

    assert length(started) == cluster_size,
           "All #{cluster_size} members should start, not_started: #{inspect(not_started)}"

    leader = wait_for_stable_leader(members)
    Logger.info("[#{label}] Leader elected: #{inspect(leader)}")

    # Seed data so reads can hit existing keys
    seed_count = 5_000
    seed_data(leader, "seed", seed_count)
    Logger.info("[#{label}] Seeded #{seed_count} keys for reads")

    # ETS table for latency samples: {type, microseconds}
    lat_tab = :ets.new(:bench_lat, [:bag, :public])

    {:ok, leader_agent} = Agent.start_link(fn -> leader end)
    stop_flag = :atomics.new(1, [])

    # Counters: [1] = successful writes, [2] = successful reads,
    #           [3] = failed writes, [4] = failed reads, [5] = elections
    counters = :counters.new(5, [:atomics])

    # Track written keys per worker in ETS for integrity verification
    keys_tab = :ets.new(:bench_keys, [:bag, :public])

    # --- Resource metrics: capture baseline ---
    mem_start = capture_memory()
    :erlang.system_flag(:scheduler_wall_time, true)
    sched_start = :erlang.statistics(:scheduler_wall_time)
    {reductions_start, _} = :erlang.statistics(:reductions)

    # Start periodic memory sampler
    mem_samples_tab = :ets.new(:bench_mem_samples, [:bag, :public])
    :ets.insert(mem_samples_tab, {:sample, mem_start})
    mem_sampler_stop = :atomics.new(1, [])

    mem_sampler_task =
      Task.async(fn ->
        memory_sampler_loop(mem_samples_tab, mem_sampler_stop)
      end)

    start_time = System.monotonic_time(:millisecond)

    worker_tasks =
      for w <- 1..num_workers do
        Task.async(fn ->
          bench_worker_loop(
            ra_system,
            members,
            leader_agent,
            stop_flag,
            counters,
            lat_tab,
            keys_tab,
            w,
            write_pct,
            seed_count
          )
        end)
      end

    Logger.info("[#{label}] Started #{num_workers} worker tasks, running for #{@bench_duration_ms / 1000}s")

    # Let it run for the benchmark duration
    Process.sleep(@bench_duration_ms)

    # Signal stop
    :atomics.put(stop_flag, 1, 1)

    # Await all workers
    Enum.each(worker_tasks, fn task -> Task.await(task, 60_000) end)

    duration_ms = System.monotonic_time(:millisecond) - start_time
    duration_s = duration_ms / 1_000

    # --- Resource metrics: capture end ---
    mem_end = capture_memory()
    sched_end = :erlang.statistics(:scheduler_wall_time)
    {reductions_end, _} = :erlang.statistics(:reductions)
    :erlang.system_flag(:scheduler_wall_time, false)

    # Stop memory sampler
    :atomics.put(mem_sampler_stop, 1, 1)
    Task.await(mem_sampler_task, 10_000)
    :ets.insert(mem_samples_tab, {:sample, mem_end})

    # Compute peak memory from samples
    peak_mem = compute_peak_memory(mem_samples_tab)
    :ets.delete(mem_samples_tab)

    Agent.stop(leader_agent)

    # Gather stats
    successful_writes = :counters.get(counters, 1)
    successful_reads = :counters.get(counters, 2)
    failed_writes = :counters.get(counters, 3)
    failed_reads = :counters.get(counters, 4)
    elections = :counters.get(counters, 5)

    total_ops = successful_writes + successful_reads
    failed_ops = failed_writes + failed_reads

    # Gather latency samples
    write_latencies =
      lat_tab
      |> ets_entries_by_type(:write)
      |> Enum.sort()

    read_latencies =
      lat_tab
      |> ets_entries_by_type(:read)
      |> Enum.sort()

    :ets.delete(lat_tab)

    # Count batches for reporting
    write_batches = div(successful_writes, @batch_size)
    read_batches = div(successful_reads, @batch_size)

    # Compute resource metrics
    avg_util = compute_scheduler_utilization(sched_start, sched_end)
    num_schedulers = length(Enum.sort(sched_end || []))
    total_reductions = reductions_end - reductions_start

    report = """

    === #{label} — #{num_workers} connections, #{Float.round(duration_s, 1)} seconds ===
      Total operations:     #{format_number(total_ops)}
      Successful writes:    #{format_number(successful_writes)} (#{@batch_size} per batch x #{format_number(write_batches)} batches)
      Successful reads:     #{format_number(successful_reads)} (#{@batch_size} per batch x #{format_number(read_batches)} batches)
      Failed ops:           #{format_number(failed_ops)} (#{failed_writes} writes, #{failed_reads} reads)
      Leader elections:     #{elections}

      Write throughput:     #{format_number(round(successful_writes / max(duration_s, 0.001)))} writes/sec
      Read throughput:      #{format_number(round(successful_reads / max(duration_s, 0.001)))} reads/sec
      Combined throughput:  #{format_number(round(total_ops / max(duration_s, 0.001)))} ops/sec

      Write latency:  p50=#{format_latency(percentile(write_latencies, 50))}  p95=#{format_latency(percentile(write_latencies, 95))}  p99=#{format_latency(percentile(write_latencies, 99))}  (per batch of #{@batch_size})
      Read latency:   p50=#{format_latency(percentile(read_latencies, 50))}  p95=#{format_latency(percentile(read_latencies, 95))}  p99=#{format_latency(percentile(read_latencies, 99))}  (per batch of #{@batch_size})

      --- Resource Usage ---
      Memory at start:      #{format_bytes(mem_start.total)} (BEAM total)
      Memory at end:        #{format_bytes(mem_end.total)} (BEAM total)
      Memory delta:         +#{format_bytes(mem_end.total - mem_start.total)}
      Peak ETS memory:      #{format_bytes(peak_mem.ets)}
      Peak process memory:  #{format_bytes(peak_mem.processes)}

      Scheduler utilization: #{Float.round(avg_util, 1)}% avg (#{num_schedulers} schedulers)
      Reductions:           #{format_number(total_reductions)} total (#{format_number(round(total_reductions / max(duration_s, 0.001)))}/sec)
    """

    Logger.info(report)

    # Data integrity verification
    Logger.info("[#{label}] Verifying data integrity...")

    written_keys = ets_all_keys(keys_tab)
    :ets.delete(keys_tab)

    current_leader = wait_for_stable_leader(members)
    state = query_leader_via(current_leader)

    {verified, missing} =
      Enum.reduce(written_keys, {0, 0}, fn {key, expected_val}, {v, m} ->
        case Map.get(state, key) do
          ^expected_val -> {v + 1, m}
          _ -> {v, m + 1}
        end
      end)

    integrity_report = """
      Data integrity: #{format_number(verified)}/#{format_number(verified + missing)} keys verified#{if missing == 0, do: " OK", else: " FAILED (#{missing} missing)"}
    """

    Logger.info(integrity_report)

    assert missing == 0,
           "#{missing} keys claimed successful but missing from Raft state"

    # Sanity checks
    assert total_ops > 0, "Expected at least some operations to succeed"

    Logger.info("=== #{label} PASSED ===")
  end

  # ===========================================================================
  # Worker loop for mixed read/write benchmarks
  # ===========================================================================

  defp bench_worker_loop(
         ra_system,
         members,
         leader_agent,
         stop_flag,
         counters,
         lat_tab,
         keys_tab,
         worker_id,
         write_pct,
         seed_count
       ) do
    if :atomics.get(stop_flag, 1) == 1 do
      :done
    else
      current_leader = Agent.get(leader_agent, & &1)

      if :rand.uniform(100) <= write_pct do
        # --- Write path: batch of @batch_size keys ---
        seq = System.unique_integer([:positive])

        batch_cmds =
          for i <- 0..(@batch_size - 1) do
            key = "w#{worker_id}:#{seq}:#{i}"
            {:put, key, "v#{worker_id}:#{seq}:#{i}"}
          end

        start_us = System.monotonic_time(:microsecond)

        case safe_process_command(current_leader, {:batch, batch_cmds}) do
          {:ok, _reply, new_leader} ->
            elapsed = System.monotonic_time(:microsecond) - start_us
            :ets.insert(lat_tab, {:write, elapsed})
            :counters.add(counters, 1, @batch_size)

            # Record keys for integrity check
            for i <- 0..(@batch_size - 1) do
              key = "w#{worker_id}:#{seq}:#{i}"
              :ets.insert(keys_tab, {key, "v#{worker_id}:#{seq}:#{i}"})
            end

            if new_leader != current_leader do
              Agent.update(leader_agent, fn _ -> new_leader end)
            end

          {:error, _reason} ->
            :counters.add(counters, 3, @batch_size)
            new_elections = maybe_find_leader(members, leader_agent, current_leader)
            if new_elections > 0, do: :counters.add(counters, 5, new_elections)
            Process.sleep(5)

          :timeout ->
            :counters.add(counters, 3, @batch_size)
            new_elections = maybe_find_leader(members, leader_agent, current_leader)
            if new_elections > 0, do: :counters.add(counters, 5, new_elections)
            Process.sleep(5)
        end
      else
        # --- Read path: batch of @batch_size keys via single local_query ---
        keys_to_read =
          for _ <- 1..@batch_size do
            "seed:#{:rand.uniform(seed_count)}"
          end

        start_us = System.monotonic_time(:microsecond)

        case safe_local_query_batch(current_leader, keys_to_read) do
          {:ok, _values} ->
            elapsed = System.monotonic_time(:microsecond) - start_us
            :ets.insert(lat_tab, {:read, elapsed})
            :counters.add(counters, 2, @batch_size)

          :error ->
            :counters.add(counters, 4, @batch_size)
            maybe_find_leader(members, leader_agent, current_leader)
        end
      end

      bench_worker_loop(
        ra_system,
        members,
        leader_agent,
        stop_flag,
        counters,
        lat_tab,
        keys_tab,
        worker_id,
        write_pct,
        seed_count
      )
    end
  end

  # ===========================================================================
  # Background writer for latency test
  # ===========================================================================

  defp background_writer_loop(ra_system, members, leader_agent, stop_flag, writer_id) do
    bg_loop(ra_system, members, leader_agent, stop_flag, writer_id, 0, 0, 0)
  end

  defp bg_loop(ra_system, members, leader_agent, stop_flag, writer_id, counter, writes, failures) do
    if :atomics.get(stop_flag, 1) == 1 do
      {writes, failures}
    else
      current_leader = Agent.get(leader_agent, & &1)

      batch_cmds =
        for i <- 0..(@batch_size - 1) do
          {:put, "bg#{writer_id}:#{counter + i}", "bgv#{writer_id}:#{counter + i}"}
        end

      case safe_process_command(current_leader, {:batch, batch_cmds}) do
        {:ok, _reply, new_leader} ->
          if new_leader != current_leader do
            Agent.update(leader_agent, fn _ -> new_leader end)
          end

          bg_loop(ra_system, members, leader_agent, stop_flag, writer_id, counter + @batch_size, writes + @batch_size, failures)

        {:error, _} ->
          maybe_find_leader(members, leader_agent, current_leader)
          Process.sleep(5)
          bg_loop(ra_system, members, leader_agent, stop_flag, writer_id, counter + @batch_size, writes, failures + @batch_size)

        :timeout ->
          maybe_find_leader(members, leader_agent, current_leader)
          Process.sleep(5)
          bg_loop(ra_system, members, leader_agent, stop_flag, writer_id, counter + @batch_size, writes, failures + @batch_size)
      end
    end
  end

  # ===========================================================================
  # Latency probe loop
  # ===========================================================================

  defp latency_probe_loop(
         members,
         leader_agent,
         stop_flag,
         write_lat_tab,
         read_lat_tab,
         batch_lat_tab,
         deadline
       ) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline or :atomics.get(stop_flag, 1) == 1 do
      :done
    else
      current_leader = Agent.get(leader_agent, & &1)
      seq = System.unique_integer([:positive])

      # Single-key write
      write_key = "probe_w:#{seq}"

      start_us = System.monotonic_time(:microsecond)

      case safe_process_command(current_leader, {:put, write_key, "pv#{seq}"}) do
        {:ok, _, new_leader} ->
          elapsed = System.monotonic_time(:microsecond) - start_us
          :ets.insert(write_lat_tab, {:sample, elapsed})

          if new_leader != current_leader do
            Agent.update(leader_agent, fn _ -> new_leader end)
          end

        _ ->
          maybe_find_leader(members, leader_agent, current_leader)
      end

      # Single-key read
      read_key = "lat_seed:#{:rand.uniform(5_000)}"
      current_leader = Agent.get(leader_agent, & &1)

      start_us = System.monotonic_time(:microsecond)

      case safe_local_query(current_leader, read_key) do
        {:ok, _} ->
          elapsed = System.monotonic_time(:microsecond) - start_us
          :ets.insert(read_lat_tab, {:sample, elapsed})

        :error ->
          :ok
      end

      # Batch write
      batch_cmds =
        for i <- 0..(@batch_size - 1) do
          {:put, "probe_b:#{seq}:#{i}", "pbv#{seq}:#{i}"}
        end

      current_leader = Agent.get(leader_agent, & &1)

      start_us = System.monotonic_time(:microsecond)

      case safe_process_command(current_leader, {:batch, batch_cmds}) do
        {:ok, _, new_leader} ->
          elapsed = System.monotonic_time(:microsecond) - start_us
          :ets.insert(batch_lat_tab, {:sample, elapsed})

          if new_leader != current_leader do
            Agent.update(leader_agent, fn _ -> new_leader end)
          end

        _ ->
          maybe_find_leader(members, leader_agent, current_leader)
      end

      # Small pause between probe rounds to avoid dominating the cluster
      Process.sleep(1)

      latency_probe_loop(
        members,
        leader_agent,
        stop_flag,
        write_lat_tab,
        read_lat_tab,
        batch_lat_tab,
        deadline
      )
    end
  end

  # ===========================================================================
  # Ra helpers
  # ===========================================================================

  defp safe_process_command(leader, cmd) do
    try do
      case :ra.process_command(leader, cmd, 5_000) do
        {:ok, reply, new_leader} -> {:ok, reply, new_leader}
        {:error, reason} -> {:error, reason}
        {:timeout, _} -> :timeout
      end
    catch
      :exit, _reason -> {:error, :exit}
    end
  end

  defp safe_local_query(member, key) do
    try do
      case :ra.local_query(member, fn state -> Map.get(state, key) end, 5_000) do
        {:ok, {_, value}, _} -> {:ok, value}
        {:ok, value, _} -> {:ok, value}
        _ -> :error
      end
    catch
      :exit, _ -> :error
    end
  end

  defp safe_local_query_batch(member, keys) do
    try do
      query_fn = fn state ->
        Enum.map(keys, fn k -> Map.get(state, k) end)
      end

      case :ra.local_query(member, query_fn, 5_000) do
        {:ok, {_, values}, _} -> {:ok, values}
        {:ok, values, _} when is_list(values) -> {:ok, values}
        _ -> :error
      end
    catch
      :exit, _ -> :error
    end
  end

  defp maybe_find_leader(members, leader_agent, old_leader) do
    result =
      Enum.find_value(members, fn member ->
        try do
          case :ra.members(member, 2_000) do
            {:ok, _members, leader} when is_tuple(leader) -> leader
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end
      end)

    case result do
      nil -> 0
      ^old_leader -> 0

      new_leader ->
        Agent.update(leader_agent, fn _ -> new_leader end)
        1
    end
  end

  defp wait_for_stable_leader(members, timeout_ms \\ @ra_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_leader(members, deadline)
  end

  defp do_wait_leader(members, deadline) do
    result =
      Enum.find_value(members, fn member ->
        try do
          case :ra.members(member, 2_000) do
            {:ok, _members, leader} when is_tuple(leader) -> leader
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end
      end)

    cond do
      result != nil ->
        result

      System.monotonic_time(:millisecond) > deadline ->
        raise "Timeout waiting for leader among #{inspect(members)}"

      true ->
        Process.sleep(100)
        do_wait_leader(members, deadline)
    end
  end

  defp query_leader_via(member) do
    query_fn = fn state -> state end

    case :ra.consistent_query(member, query_fn, @ra_timeout) do
      {:ok, {_, state}, _} ->
        state

      {:ok, state, _} when is_map(state) ->
        state

      {:timeout, _} ->
        case :ra.consistent_query(member, query_fn, @ra_timeout * 2) do
          {:ok, {_, state}, _} -> state
          {:ok, state, _} when is_map(state) -> state
          other -> raise "Consistent query failed on #{inspect(member)}: #{inspect(other)}"
        end

      {:error, reason} ->
        raise "Consistent query failed on #{inspect(member)}: #{inspect(reason)}"
    end
  end

  defp seed_data(leader, prefix, count) do
    1..count
    |> Enum.chunk_every(@batch_size)
    |> Enum.each(fn chunk ->
      batch_cmds =
        Enum.map(chunk, fn i ->
          {:put, "#{prefix}:#{i}", "v#{i}"}
        end)

      {:ok, _, _} = :ra.process_command(leader, {:batch, batch_cmds}, @ra_timeout)
    end)
  end

  # ===========================================================================
  # Resource metrics helpers
  # ===========================================================================

  defp capture_memory do
    %{
      total: :erlang.memory(:total),
      processes: :erlang.memory(:processes),
      ets: :erlang.memory(:ets),
      system: :erlang.memory(:system)
    }
  end

  defp memory_sampler_loop(tab, stop_flag) do
    if :atomics.get(stop_flag, 1) == 1 do
      :done
    else
      :ets.insert(tab, {:sample, capture_memory()})
      Process.sleep(@memory_sample_interval_ms)
      memory_sampler_loop(tab, stop_flag)
    end
  end

  defp compute_peak_memory(tab) do
    samples =
      :ets.match(tab, {:sample, :"$1"})
      |> List.flatten()

    %{
      total: samples |> Enum.map(& &1.total) |> Enum.max(fn -> 0 end),
      processes: samples |> Enum.map(& &1.processes) |> Enum.max(fn -> 0 end),
      ets: samples |> Enum.map(& &1.ets) |> Enum.max(fn -> 0 end),
      system: samples |> Enum.map(& &1.system) |> Enum.max(fn -> 0 end)
    }
  end

  defp compute_scheduler_utilization(start_times, end_times) do
    case {start_times, end_times} do
      {nil, _} -> 0.0
      {_, nil} -> 0.0
      {s, e} when is_list(s) and is_list(e) ->
        sorted_start = Enum.sort(s)
        sorted_end = Enum.sort(e)

        utils =
          Enum.zip(sorted_start, sorted_end)
          |> Enum.map(fn {{_id, a1, t1}, {_id2, a2, t2}} ->
            total = t2 - t1

            if total > 0 do
              (a2 - a1) / total * 100
            else
              0.0
            end
          end)

        case length(utils) do
          0 -> 0.0
          n -> Enum.sum(utils) / n
        end

      _ ->
        0.0
    end
  end

  # ===========================================================================
  # Stats helpers
  # ===========================================================================

  defp percentile([], _p), do: 0

  defp percentile(sorted_list, p) when p >= 0 and p <= 100 do
    len = length(sorted_list)
    index = max(0, round(len * p / 100) - 1)
    Enum.at(sorted_list, min(index, len - 1))
  end

  defp ets_entries_by_type(tab, type) do
    :ets.match(tab, {type, :"$1"})
    |> List.flatten()
  end

  defp ets_to_sorted_list(tab) do
    :ets.match(tab, {:sample, :"$1"})
    |> List.flatten()
    |> Enum.sort()
  end

  defp ets_all_keys(tab) do
    :ets.tab2list(tab)
  end

  defp format_latency(0), do: "N/A"

  defp format_latency(us) when us < 1_000 do
    "#{us}us"
  end

  defp format_latency(us) do
    ms = Float.round(us / 1_000, 1)
    "#{ms}ms"
  end

  defp format_number(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.graphemes()
    |> Enum.reverse()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", &Enum.join/1)
    |> String.reverse()
  end

  defp format_number(n) when is_float(n) do
    format_number(round(n))
  end

  defp format_bytes(bytes) when bytes < 1_024 do
    "#{bytes} B"
  end

  defp format_bytes(bytes) when bytes < 1_048_576 do
    "#{Float.round(bytes / 1_024, 1)} KB"
  end

  defp format_bytes(bytes) do
    "#{Float.round(bytes / 1_048_576, 1)} MB"
  end
end
