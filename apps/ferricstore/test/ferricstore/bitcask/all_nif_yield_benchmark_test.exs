defmodule Ferricstore.Bitcask.AllNifYieldBenchmarkTest do
  @moduledoc """
  Yield benchmarks for ALL CPU-heavy NIFs.

  Each test:
  1. Runs the NIF with a large input
  2. Pings a GenServer during execution
  3. Measures ping p99 latency
  4. Asserts p99 < 2ms (proves scheduler not blocked)
  5. Prints throughput numbers

  ## Running

      mix test apps/ferricstore/test/ferricstore/bitcask/all_nif_yield_benchmark_test.exs --include perf

  Tagged `:perf` so these are excluded from the default `mix test` run.
  """

  use ExUnit.Case, async: false

  @moduletag :perf
  @moduletag timeout: 300_000

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Bitcask.Async

  # ============================================================================
  # Helpers
  # ============================================================================

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "yield_bench_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store do
    dir = tmp_dir()
    {:ok, store} = NIF.new(dir)
    {store, dir}
  end

  defp bloom_path(dir, name), do: Path.join(dir, "#{name}.bloom")

  # Populate a Bitcask store with `count` keys using batched writes.
  defp populate(store, count, opts) do
    prefix = Keyword.get(opts, :prefix, "key")
    pad = opts[:pad] || (count |> Integer.to_string() |> String.length())
    ttl = Keyword.get(opts, :ttl, 0)

    1..count
    |> Enum.chunk_every(1000)
    |> Enum.each(fn chunk ->
      batch =
        Enum.map(chunk, fn i ->
          key = "#{prefix}_#{String.pad_leading(Integer.to_string(i), pad, "0")}"
          {key, "val_#{i}", ttl}
        end)

      :ok = NIF.put_batch(store, batch)
    end)
  end

  defp padded_key(prefix, index, pad) do
    "#{prefix}_#{String.pad_leading(Integer.to_string(index), pad, "0")}"
  end

  # ---------------------------------------------------------------------------
  # Pinger GenServer — used to measure scheduler responsiveness during NIF calls
  # ---------------------------------------------------------------------------

  defp start_pinger do
    pid = spawn_link(fn -> pinger_loop() end)

    ping_fn = fn ->
      ref = make_ref()
      t0 = System.monotonic_time(:microsecond)
      send(pid, {:ping, ref, self()})

      receive do
        {:pong, ^ref} -> System.monotonic_time(:microsecond) - t0
      after
        50_000 -> :timeout
      end
    end

    {pid, ping_fn}
  end

  defp pinger_loop do
    receive do
      {:ping, ref, from} ->
        send(from, {:pong, ref})
        pinger_loop()

      :stop ->
        :ok
    end
  end

  # Collect ping latencies while a NIF task is running.
  # Returns {nif_result, latencies_us}.
  defp measure_pings_during(nif_fun, opts \\ []) do
    interval_ms = Keyword.get(opts, :interval_ms, 2)
    {_pinger_pid, ping} = start_pinger()

    # Warm up the pinger
    for _ <- 1..5, do: ping.()

    latencies = :ets.new(:latencies, [:set, :public])
    counter = :atomics.new(1, signed: false)

    # Start the NIF in a task
    nif_task =
      Task.async(fn ->
        nif_fun.()
      end)

    # Start the ping collector in a task
    ping_task =
      Task.async(fn ->
        ping_loop(ping, latencies, counter, interval_ms, nif_task.pid)
      end)

    nif_result = Task.await(nif_task, 120_000)
    # Give the ping loop a moment to notice the NIF task is done
    Process.sleep(10)
    send(ping_task.pid, :stop)
    Task.await(ping_task, 5_000)

    # Collect latencies from ETS
    count = :atomics.get(counter, 1)

    collected =
      for i <- 1..count do
        case :ets.lookup(latencies, i) do
          [{^i, val}] -> val
          [] -> nil
        end
      end
      |> Enum.reject(&is_nil/1)
      |> Enum.reject(&(&1 == :timeout))

    :ets.delete(latencies)
    {nif_result, collected}
  end

  defp ping_loop(ping, latencies, counter, interval_ms, nif_pid) do
    if Process.alive?(nif_pid) do
      lat = ping.()
      idx = :atomics.add_get(counter, 1, 1)
      :ets.insert(latencies, {idx, lat})
      Process.sleep(interval_ms)
      ping_loop(ping, latencies, counter, interval_ms, nif_pid)
    else
      :done
    end
  end

  # Compute p99 from a list of latency values (microseconds).
  defp p99(latencies) when length(latencies) == 0, do: 0

  defp p99(latencies) do
    sorted = Enum.sort(latencies)
    idx = trunc(length(sorted) * 0.99)
    Enum.at(sorted, min(idx, length(sorted) - 1))
  end

  # Assert p99 latency is under 2ms (2000 microseconds).
  defp assert_p99_under_2ms(latencies, label) do
    p99_us = p99(latencies)
    p99_ms = p99_us / 1000.0

    IO.puts(
      "  [yield_bench] #{label}: #{length(latencies)} pings, " <>
        "p99=#{Float.round(p99_ms, 2)}ms"
    )

    assert p99_us < 2_000,
           "#{label}: p99 ping latency #{Float.round(p99_ms, 2)}ms >= 2ms — scheduler blocked"
  end

  # Measure operations per second for a given function.
  defp measure_ops_per_sec(fun, iterations) do
    t0 = System.monotonic_time(:microsecond)

    for _ <- 1..iterations do
      fun.()
    end

    elapsed_us = System.monotonic_time(:microsecond) - t0

    if elapsed_us > 0 do
      trunc(iterations * 1_000_000 / elapsed_us)
    else
      iterations * 1_000_000
    end
  end

  # ============================================================================
  # PROBABILISTIC BULK NIFs
  # ============================================================================

  describe "bloom yield benchmarks" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "bloom_madd 100K items stays responsive", %{dir: dir} do
      path = bloom_path(dir, "madd_bench")
      {:ok, filter} = NIF.bloom_create(path, 10_000_000, 7)

      elements = Enum.map(1..100_000, fn i -> "elem_#{i}" end)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.bloom_madd(filter, elements)
        end)

      assert is_list(result)
      assert length(result) == 100_000
      assert_p99_under_2ms(latencies, "bloom_madd(100K)")
    end

    test "bloom_mexists 100K lookups stays responsive", %{dir: dir} do
      path = bloom_path(dir, "mexists_bench")
      {:ok, filter} = NIF.bloom_create(path, 10_000_000, 7)

      # Pre-populate
      elements = Enum.map(1..100_000, fn i -> "elem_#{i}" end)
      NIF.bloom_madd(filter, elements)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.bloom_mexists(filter, elements)
        end)

      assert is_list(result)
      assert length(result) == 100_000
      assert_p99_under_2ms(latencies, "bloom_mexists(100K)")
    end
  end

  describe "cuckoo yield benchmarks" do
    test "cuckoo_add 10K items stays responsive" do
      # Cuckoo kick chains can be expensive
      {:ok, filter} = NIF.cuckoo_create(16_384, 4, 500, 0)

      {_result, latencies} =
        measure_pings_during(fn ->
          for i <- 1..10_000 do
            NIF.cuckoo_add(filter, "cuckoo_item_#{i}")
          end
        end)

      assert_p99_under_2ms(latencies, "cuckoo_add(10K)")
    end
  end

  describe "CMS yield benchmarks" do
    test "cms_incrby 100K increments stays responsive" do
      {:ok, sketch} = NIF.cms_create(10_000, 10)

      items = Enum.map(1..100_000, fn i -> {"item_#{i}", 1} end)

      # Process in chunks to exercise bulk NIF path
      {_result, latencies} =
        measure_pings_during(fn ->
          items
          |> Enum.chunk_every(1000)
          |> Enum.each(fn chunk ->
            NIF.cms_incrby(sketch, chunk)
          end)
        end)

      assert_p99_under_2ms(latencies, "cms_incrby(100K)")
    end

    test "cms_query 100K queries stays responsive" do
      {:ok, sketch} = NIF.cms_create(10_000, 10)

      # Pre-populate
      items = Enum.map(1..100_000, fn i -> {"item_#{i}", 1} end)

      items
      |> Enum.chunk_every(1000)
      |> Enum.each(fn chunk -> NIF.cms_incrby(sketch, chunk) end)

      elements = Enum.map(1..100_000, fn i -> "item_#{i}" end)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.cms_query(sketch, elements)
        end)

      assert {:ok, counts} = result
      assert length(counts) == 100_000
      assert_p99_under_2ms(latencies, "cms_query(100K)")
    end

    test "cms_merge large sketches stays responsive" do
      # Merge 10 sketches of width=10000 depth=10
      width = 10_000
      depth = 10

      sources =
        for i <- 1..10 do
          {:ok, s} = NIF.cms_create(width, depth)

          items =
            Enum.map(1..1_000, fn j -> {"merge_item_#{i}_#{j}", 1} end)

          NIF.cms_incrby(s, items)
          s
        end

      {:ok, dest} = NIF.cms_create(width, depth)

      # cms_merge expects [{resource, weight}] tuples
      source_pairs = Enum.map(sources, fn s -> {s, 1} end)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.cms_merge(dest, source_pairs)
        end)

      assert result == :ok
      assert_p99_under_2ms(latencies, "cms_merge(10 sketches, w=10000, d=10)")
    end
  end

  describe "TopK yield benchmarks" do
    test "topk_add 100K items stays responsive" do
      {:ok, topk} = NIF.topk_create(50, 1000, 7, 0.9)

      elements = Enum.map(1..100_000, fn i -> "topk_item_#{i}" end)

      {result, latencies} =
        measure_pings_during(fn ->
          # Process in chunks of 1000 to keep individual NIF calls reasonable
          elements
          |> Enum.chunk_every(1000)
          |> Enum.flat_map(fn chunk ->
            NIF.topk_add(topk, chunk)
          end)
        end)

      assert is_list(result)
      assert length(result) == 100_000
      assert_p99_under_2ms(latencies, "topk_add(100K)")
    end

    test "topk_incrby 100K items stays responsive" do
      {:ok, topk} = NIF.topk_create(50, 1000, 7, 0.9)

      pairs = Enum.map(1..100_000, fn i -> {"topk_incr_#{i}", 1} end)

      {result, latencies} =
        measure_pings_during(fn ->
          pairs
          |> Enum.chunk_every(1000)
          |> Enum.flat_map(fn chunk ->
            NIF.topk_incrby(topk, chunk)
          end)
        end)

      assert is_list(result)
      assert length(result) == 100_000
      assert_p99_under_2ms(latencies, "topk_incrby(100K)")
    end
  end

  describe "TDigest yield benchmarks" do
    test "tdigest_add 100K values stays responsive" do
      # Buffer flush triggers O(n log n) sort+merge
      resource = NIF.tdigest_create(200.0)

      values = Enum.map(1..100_000, fn i -> i * 1.0 end)

      {_result, latencies} =
        measure_pings_during(fn ->
          # Add in chunks to trigger multiple buffer flushes
          values
          |> Enum.chunk_every(5000)
          |> Enum.each(fn chunk ->
            NIF.tdigest_add(resource, chunk)
          end)
        end)

      assert_p99_under_2ms(latencies, "tdigest_add(100K)")
    end

    test "tdigest_merge 100 digests stays responsive" do
      resources =
        for _i <- 1..100 do
          res = NIF.tdigest_create(100.0)
          values = Enum.map(1..1_000, fn _j -> :rand.uniform() * 1000.0 end)
          NIF.tdigest_add(res, values)
          res
        end

      {_result, latencies} =
        measure_pings_during(fn ->
          NIF.tdigest_merge(resources, 100.0)
        end)

      assert_p99_under_2ms(latencies, "tdigest_merge(100 digests)")
    end

    test "tdigest_quantile on large digest stays responsive" do
      # 1M values, then 1000 quantile queries
      resource = NIF.tdigest_create(300.0)

      # Add 1M values in chunks
      for _batch <- 1..100 do
        values = Enum.map(1..10_000, fn _ -> :rand.uniform() * 1_000_000.0 end)
        NIF.tdigest_add(resource, values)
      end

      quantiles = Enum.map(0..999, fn i -> i / 999.0 end)

      {result, latencies} =
        measure_pings_during(fn ->
          for _ <- 1..1_000 do
            NIF.tdigest_quantile(resource, quantiles)
          end
        end)

      assert is_list(result)
      assert_p99_under_2ms(latencies, "tdigest_quantile(1M values, 1000 queries)")
    end
  end

  # ============================================================================
  # ITERATION NIFs
  # ============================================================================

  describe "iteration NIF yield benchmarks" do
    setup do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "keys 100K items stays responsive", %{store: store} do
      populate(store, 100_000, pad: 6)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.keys(store)
        end)

      # keys/1 returns a bare list
      assert is_list(result)
      assert length(result) == 100_000
      assert_p99_under_2ms(latencies, "keys(100K)")

      IO.puts(
        "  [yield_bench] keys(100K): throughput = #{length(result)} keys returned"
      )
    end

    test "get_all 10K items stays responsive", %{store: store} do
      populate(store, 10_000, pad: 5)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.get_all(store)
        end)

      assert {:ok, entries} = result
      assert length(entries) == 10_000
      assert_p99_under_2ms(latencies, "get_all(10K)")
    end

    test "get_batch 10K items stays responsive", %{store: store} do
      populate(store, 10_000, pad: 5)

      all_keys = Enum.map(1..10_000, fn i -> padded_key("key", i, 5) end)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.get_batch(store, all_keys)
        end)

      assert {:ok, values} = result
      assert length(values) == 10_000
      assert_p99_under_2ms(latencies, "get_batch(10K)")
    end

    test "get_range 10K items stays responsive", %{store: store} do
      populate(store, 10_000, pad: 5)

      {result, latencies} =
        measure_pings_during(fn ->
          NIF.get_range(store, "key_00001", "key_10000", 10_000)
        end)

      assert {:ok, entries} = result
      assert length(entries) == 10_000
      assert_p99_under_2ms(latencies, "get_range(10K)")
    end
  end

  # ============================================================================
  # HNSW (CPU-bound graph traversal)
  # ============================================================================

  describe "HNSW yield benchmarks" do
    test "vsearch_nif 50K vectors stays responsive" do
      dims = 32
      {:ok, ref} = NIF.hnsw_new(dims, 16, 200, "l2")

      # Build index with 50K vectors
      for i <- 1..50_000 do
        v = for d <- 1..dims, do: :math.sin(i * 0.618 + d * 0.314) * 1.0
        NIF.hnsw_add(ref, "v#{i}", v)
      end

      query = for d <- 1..dims, do: :math.cos(d * 0.5) * 1.0

      {result, latencies} =
        measure_pings_during(fn ->
          # Run 100 searches to stress the yield path
          for _ <- 1..100 do
            NIF.vsearch_nif(ref, query, 10, 200)
          end
        end)

      assert is_list(result)
      assert_p99_under_2ms(latencies, "vsearch_nif(50K vectors, 100 searches)")
    end

    test "hnsw_add during large index build stays responsive" do
      # Building a 10K vector index
      dims = 32
      {:ok, ref} = NIF.hnsw_new(dims, 16, 200, "l2")

      {_result, latencies} =
        measure_pings_during(fn ->
          for i <- 1..10_000 do
            v = for d <- 1..dims, do: :math.sin(i * 0.618 + d * 0.314) * 1.0
            NIF.hnsw_add(ref, "build_v#{i}", v)
          end
        end)

      {:ok, count} = NIF.hnsw_count(ref)
      assert count == 10_000
      assert_p99_under_2ms(latencies, "hnsw_add(10K vectors build)")
    end
  end

  # ============================================================================
  # TOKIO ASYNC IO (should NOT block at all)
  # ============================================================================

  describe "Tokio async IO yield benchmarks" do
    test "get_async 100 concurrent cold reads - zero scheduler impact" do
      # This is the gold standard: Tokio threads do IO, schedulers stay free
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf(dir) end)

      # Write 100 keys with 10KB values (cold reads = likely disk IO)
      batch =
        Enum.map(1..100, fn i ->
          {"async_key_#{i}", :binary.copy("x", 10_000), 0}
        end)

      :ok = NIF.put_batch(store, batch)

      {_pinger_pid, ping} = start_pinger()

      # Warm up pinger
      for _ <- 1..5, do: ping.()

      # Fire 100 concurrent async reads
      t0 = System.monotonic_time(:microsecond)

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            Async.get(store, "async_key_#{i}")
          end)
        end

      # While async reads are in flight, collect ping latencies
      latencies =
        for _ <- 1..50 do
          lat = ping.()
          Process.sleep(1)
          lat
        end

      results = Task.await_many(tasks, 10_000)
      elapsed_us = System.monotonic_time(:microsecond) - t0

      # Verify all reads succeeded
      for result <- results do
        assert {:ok, value} = result
        assert byte_size(value) == 10_000
      end

      valid_latencies = Enum.reject(latencies, &(&1 == :timeout))
      p99_us = p99(valid_latencies)
      p99_ms = p99_us / 1000.0

      IO.puts(
        "  [yield_bench] get_async(100 concurrent): #{Float.round(elapsed_us / 1000.0, 1)}ms total, " <>
          "p99 ping=#{Float.round(p99_ms, 2)}ms, #{length(valid_latencies)} pings"
      )

      assert p99_us < 2_000,
             "get_async: p99 ping latency #{Float.round(p99_ms, 2)}ms >= 2ms — " <>
               "Tokio async should not block schedulers at all"
    end
  end

  # ============================================================================
  # THROUGHPUT COMPARISON TABLE
  # ============================================================================

  describe "throughput comparison" do
    test "throughput comparison: all NIFs" do
      # --- Setup resources ---
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf(dir) end)
      populate(store, 1_000, pad: 4)

      bloom_dir = tmp_dir()
      on_exit(fn -> File.rm_rf(bloom_dir) end)
      bloom_path = bloom_path(bloom_dir, "throughput")
      {:ok, bloom} = NIF.bloom_create(bloom_path, 1_000_000, 7)
      NIF.bloom_madd(bloom, Enum.map(1..1_000, fn i -> "bloom_key_#{i}" end))

      {:ok, cuckoo} = NIF.cuckoo_create(16_384, 4, 500, 0)

      for i <- 1..1_000 do
        NIF.cuckoo_add(cuckoo, "cuckoo_key_#{i}")
      end

      {:ok, cms} = NIF.cms_create(10_000, 10)
      NIF.cms_incrby(cms, Enum.map(1..100, fn i -> {"cms_key_#{i}", 1} end))

      {:ok, topk} = NIF.topk_create(10, 100, 7, 0.9)
      NIF.topk_add(topk, Enum.map(1..100, fn i -> "topk_key_#{i}" end))

      tdigest = NIF.tdigest_create(100.0)
      NIF.tdigest_add(tdigest, Enum.map(1..10_000, fn i -> i * 1.0 end))

      {:ok, hnsw} = NIF.hnsw_new(8, 16, 128, "l2")

      for i <- 1..100 do
        v = for d <- 1..8, do: :math.sin(i * 0.618 + d * 0.314) * 1.0
        NIF.hnsw_add(hnsw, "hnsw_v#{i}", v)
      end

      query_vec = for d <- 1..8, do: :math.cos(d * 0.5) * 1.0

      # --- Measure throughput ---
      iterations = 10_000
      small_iter = 1_000

      results = %{
        bloom_add:
          measure_ops_per_sec(
            fn -> NIF.bloom_add(bloom, "throughput_key_#{:rand.uniform(1_000_000)}") end,
            iterations
          ),
        bloom_exists:
          measure_ops_per_sec(fn -> NIF.bloom_exists(bloom, "bloom_key_500") end, iterations),
        cuckoo_add:
          measure_ops_per_sec(
            fn -> NIF.cuckoo_add(cuckoo, "cuckoo_bench_#{:rand.uniform(1_000_000)}") end,
            small_iter
          ),
        cuckoo_exists:
          measure_ops_per_sec(fn -> NIF.cuckoo_exists(cuckoo, "cuckoo_key_500") end, iterations),
        cms_incrby:
          measure_ops_per_sec(fn -> NIF.cms_incrby(cms, [{"key", 1}]) end, iterations),
        cms_query:
          measure_ops_per_sec(fn -> NIF.cms_query(cms, ["key"]) end, iterations),
        topk_add:
          measure_ops_per_sec(fn -> NIF.topk_add(topk, ["bench_item"]) end, iterations),
        topk_query:
          measure_ops_per_sec(fn -> NIF.topk_query(topk, ["bench_item"]) end, iterations),
        tdigest_add:
          measure_ops_per_sec(fn -> NIF.tdigest_add(tdigest, [42.0]) end, iterations),
        tdigest_quantile:
          measure_ops_per_sec(fn -> NIF.tdigest_quantile(tdigest, [0.5]) end, iterations),
        get_sync:
          measure_ops_per_sec(fn -> NIF.get(store, "key_0500") end, iterations),
        get_zero_copy:
          measure_ops_per_sec(fn -> NIF.get_zero_copy(store, "key_0500") end, iterations),
        hnsw_search:
          measure_ops_per_sec(fn -> NIF.hnsw_search(hnsw, query_vec, 5, 50) end, small_iter),
        vsearch_nif:
          measure_ops_per_sec(fn -> NIF.vsearch_nif(hnsw, query_vec, 5, 50) end, small_iter)
      }

      IO.puts("\n  === NIF Throughput Comparison ===")

      for {name, ops} <- Enum.sort_by(results, fn {_, v} -> -v end) do
        name_str = name |> to_string() |> String.pad_trailing(20)
        ops_str = ops |> Integer.to_string() |> String.pad_leading(12)
        IO.puts("    #{name_str} #{ops_str} ops/sec")
      end

      IO.puts("  ================================\n")

      # Sanity: every NIF should achieve at least 100 ops/sec
      for {name, ops} <- results do
        assert ops > 100,
               "#{name} achieved only #{ops} ops/sec — unexpectedly slow"
      end
    end
  end
end
