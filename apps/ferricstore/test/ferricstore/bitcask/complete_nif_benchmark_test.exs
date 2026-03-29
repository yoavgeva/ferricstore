defmodule Ferricstore.Bitcask.CompleteNifBenchmarkTest do
  @moduledoc """
  Comprehensive benchmark of EVERY NIF function in FerricStore.

  Run with: mix test apps/ferricstore/test/ferricstore/bitcask/complete_nif_benchmark_test.exs --include bench
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  @moduletag :bench

  # ============================================================================
  # Helpers
  # ============================================================================

  defp tmp_dir(label \\ "") do
    dir =
      Path.join(
        System.tmp_dir!(),
        "nif_bench_#{label}_#{:rand.uniform(9_999_999)}"
      )

    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp open_store_with_n(n, label \\ "store") do
    dir = tmp_dir(label)
    store = open_store(dir)

    batch =
      Enum.map(1..n, fn i ->
        key = "key:#{String.pad_leading(Integer.to_string(i), 8, "0")}"
        val = :crypto.strong_rand_bytes(64)
        {key, val, 0}
      end)

    # Write in chunks to avoid very large single batch
    batch
    |> Enum.chunk_every(1000)
    |> Enum.each(fn chunk -> :ok = NIF.put_batch(store, chunk) end)

    {store, dir}
  end

  defp benchmark_nif(name, setup_fn, nif_fn, iterations \\ 10_000) do
    ctx = setup_fn.()

    # Warm up
    for _ <- 1..min(100, iterations), do: nif_fn.(ctx)

    # Measure
    {time_us, _} =
      :timer.tc(fn ->
        for _ <- 1..iterations, do: nif_fn.(ctx)
      end)

    ops_per_sec = div(iterations * 1_000_000, max(time_us, 1))
    per_op_ns = div(time_us * 1000, iterations)

    category =
      cond do
        per_op_ns < 1_000 -> "tiny (<1us)"
        per_op_ns < 10_000 -> "fast (1-10us)"
        per_op_ns < 100_000 -> "moderate (10-100us)"
        per_op_ns < 1_000_000 -> "slow (100us-1ms)"
        true -> "very slow (>1ms)"
      end

    result_line =
      "  #{String.pad_trailing(name, 35)} #{String.pad_leading(to_string(per_op_ns), 10)}ns/op  #{String.pad_leading(Integer.to_string(ops_per_sec), 12)} ops/sec  [#{category}]"

    IO.puts(result_line)

    # Store result for summary
    Agent.update(:bench_results, fn results ->
      [{name, per_op_ns, ops_per_sec, category} | results]
    end)

    {per_op_ns, ops_per_sec}
  end

  # ============================================================================
  # Setup / Teardown
  # ============================================================================

  setup_all do
    Agent.start_link(fn -> [] end, name: :bench_results)

    IO.puts("\n=== FerricStore NIF Benchmark Suite ===\n")

    on_exit(fn ->
      results = Agent.get(:bench_results, & &1)

      sorted =
        results
        |> Enum.sort_by(fn {_name, ns, _ops, _cat} -> ns end)

      IO.puts("\n\n=== FerricStore NIF Benchmark Results (sorted by latency) ===")

      IO.puts(
        "  #{String.pad_trailing("NIF", 35)} #{String.pad_leading("ns/op", 10)}  #{String.pad_leading("ops/sec", 12)}  Category"
      )

      IO.puts("  #{String.duplicate("-", 80)}")

      for {name, ns, ops, cat} <- sorted do
        IO.puts(
          "  #{String.pad_trailing(name, 35)} #{String.pad_leading(Integer.to_string(ns), 10)}  #{String.pad_leading(Integer.to_string(ops), 12)}  #{cat}"
        )
      end

      IO.puts("  #{String.duplicate("-", 80)}")
      IO.puts("  Total NIFs benchmarked: #{length(sorted)}\n")

      Agent.stop(:bench_results)
    end)

    :ok
  end

  # ============================================================================
  # BITCASK CORE
  # ============================================================================

  describe "Bitcask Core" do
    @tag timeout: 120_000
    test "new" do
      benchmark_nif(
        "new",
        fn -> nil end,
        fn _ ->
          dir = tmp_dir("new")
          {:ok, _store} = NIF.new(dir)
          File.rm_rf(dir)
        end,
        100
      )
    end

    @tag timeout: 120_000
    test "put" do
      dir = tmp_dir("put")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "put",
        fn -> {store, 0} end,
        fn {s, _} ->
          k = "put:#{:rand.uniform(999_999)}"
          :ok = NIF.put(s, k, "value_data_here", 0)
        end,
        5_000
      )
    end

    @tag timeout: 120_000
    test "get (hot key)" do
      dir = tmp_dir("get_hot")
      store = open_store(dir)
      :ok = NIF.put(store, "hot_key", String.duplicate("x", 100), 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get (hot)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.get(s, "hot_key") end
      )
    end

    @tag timeout: 120_000
    test "get_zero_copy (hot key)" do
      dir = tmp_dir("get_zc")
      store = open_store(dir)
      :ok = NIF.put(store, "hot_key", String.duplicate("x", 100), 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_zero_copy (hot)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.get_zero_copy(s, "hot_key") end
      )
    end

    @tag timeout: 120_000
    test "get (miss)" do
      dir = tmp_dir("get_miss")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get (miss)",
        fn -> store end,
        fn s -> {:ok, nil} = NIF.get(s, "nonexistent_key") end
      )
    end

    @tag timeout: 120_000
    test "get_file_ref" do
      dir = tmp_dir("get_file_ref")
      store = open_store(dir)
      :ok = NIF.put(store, "ref_key", "ref_value", 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_file_ref",
        fn -> store end,
        fn s -> {:ok, {_path, _off, _sz}} = NIF.get_file_ref(s, "ref_key") end
      )
    end

    @tag timeout: 300_000
    test "delete" do
      dir = tmp_dir("delete")
      store = open_store(dir)

      # Pre-populate keys using batch to avoid per-key fsync overhead
      batch =
        Enum.map(1..5_000, fn i ->
          {"del:#{i}", "v", 0}
        end)

      batch
      |> Enum.chunk_every(500)
      |> Enum.each(fn chunk -> :ok = NIF.put_batch(store, chunk) end)

      on_exit(fn -> File.rm_rf(dir) end)
      counter = :atomics.new(1, [])

      benchmark_nif(
        "delete",
        fn -> {store, counter} end,
        fn {s, c} ->
          i = :atomics.add_get(c, 1, 1)
          NIF.delete(s, "del:#{i}")
        end,
        2_000
      )
    end

    @tag timeout: 120_000
    test "put_batch (10 items)" do
      dir = tmp_dir("put_batch_10")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch_10 =
        Enum.map(1..10, fn i ->
          {"batch10:#{i}", :crypto.strong_rand_bytes(64), 0}
        end)

      benchmark_nif(
        "put_batch (10)",
        fn -> store end,
        fn s -> :ok = NIF.put_batch(s, batch_10) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "put_batch (100 items)" do
      dir = tmp_dir("put_batch_100")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch_100 =
        Enum.map(1..100, fn i ->
          {"batch100:#{i}", :crypto.strong_rand_bytes(64), 0}
        end)

      benchmark_nif(
        "put_batch (100)",
        fn -> store end,
        fn s -> :ok = NIF.put_batch(s, batch_100) end,
        100
      )
    end

    @tag timeout: 120_000
    test "put_batch_async (10 items, uring/fallback)" do
      dir = tmp_dir("put_batch_async")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch_10 =
        Enum.map(1..10, fn i ->
          {"pba:#{i}", :crypto.strong_rand_bytes(64), 0}
        end)

      benchmark_nif(
        "put_batch_async (10, uring)",
        fn -> store end,
        fn s ->
          result = NIF.put_batch_async(s, batch_10)

          case result do
            :ok ->
              :ok

            {:pending, _op_id} ->
              receive do
                {:io_complete, _, _} -> :ok
              after
                5_000 -> :timeout
              end
          end
        end,
        500
      )
    end

    @tag timeout: 120_000
    test "read_modify_write (incr)" do
      dir = tmp_dir("rmw")
      store = open_store(dir)
      :ok = NIF.put(store, "counter", "0", 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "read_modify_write (incr)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.read_modify_write(s, "counter", {:incr_by, 1}) end,
        5_000
      )
    end

    @tag timeout: 120_000
    test "read_modify_write (append)" do
      dir = tmp_dir("rmw_append")
      store = open_store(dir)
      :ok = NIF.put(store, "appendkey", "", 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "read_modify_write (append)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.read_modify_write(s, "appendkey", {:append, "x"}) end,
        2_000
      )
    end

    @tag timeout: 120_000
    test "read_modify_write (set_range)" do
      dir = tmp_dir("rmw_setrange")
      store = open_store(dir)
      :ok = NIF.put(store, "rangekey", String.duplicate("\0", 100), 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "read_modify_write (set_range)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.read_modify_write(s, "rangekey", {:set_range, 10, "hello"}) end,
        5_000
      )
    end

    @tag timeout: 120_000
    test "read_modify_write (set_bit)" do
      dir = tmp_dir("rmw_setbit")
      store = open_store(dir)
      :ok = NIF.put(store, "bitkey", "\0", 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "read_modify_write (set_bit)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.read_modify_write(s, "bitkey", {:set_bit, 3, 1}) end,
        5_000
      )
    end

    @tag timeout: 120_000
    test "read_modify_write (incr_by_float)" do
      dir = tmp_dir("rmw_float")
      store = open_store(dir)
      :ok = NIF.put(store, "floatkey", "0.0", 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "read_modify_write (incr_float)",
        fn -> store end,
        fn s -> {:ok, _} = NIF.read_modify_write(s, "floatkey", {:incr_by_float, 1.5}) end,
        5_000
      )
    end
  end

  # ============================================================================
  # YIELDING ITERATION
  # ============================================================================

  describe "Yielding Iteration" do
    @tag timeout: 120_000
    test "keys (1K)" do
      {store, dir} = open_store_with_n(1_000, "keys1k")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "keys (1K)",
        fn -> store end,
        fn s -> NIF.keys(s) end,
        100
      )
    end

    @tag timeout: 120_000
    test "keys (10K)" do
      {store, dir} = open_store_with_n(10_000, "keys10k")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "keys (10K)",
        fn -> store end,
        fn s -> NIF.keys(s) end,
        10
      )
    end

    @tag timeout: 120_000
    test "get_all (1K)" do
      {store, dir} = open_store_with_n(1_000, "getall1k")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_all (1K)",
        fn -> store end,
        fn s -> NIF.get_all(s) end,
        50
      )
    end

    @tag timeout: 120_000
    test "get_all_zero_copy (1K)" do
      {store, dir} = open_store_with_n(1_000, "getallzc1k")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_all_zero_copy (1K)",
        fn -> store end,
        fn s -> NIF.get_all_zero_copy(s) end,
        50
      )
    end

    @tag timeout: 120_000
    test "get_batch (100 keys)" do
      {store, dir} = open_store_with_n(1_000, "getbatch100")
      keys = Enum.map(1..100, fn i -> "key:#{String.pad_leading(Integer.to_string(i), 8, "0")}" end)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_batch (100)",
        fn -> {store, keys} end,
        fn {s, ks} -> NIF.get_batch(s, ks) end,
        100
      )
    end

    @tag timeout: 120_000
    test "get_batch_zero_copy (100 keys)" do
      {store, dir} = open_store_with_n(1_000, "getbatchzc100")
      keys = Enum.map(1..100, fn i -> "key:#{String.pad_leading(Integer.to_string(i), 8, "0")}" end)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_batch_zero_copy (100)",
        fn -> {store, keys} end,
        fn {s, ks} -> NIF.get_batch_zero_copy(s, ks) end,
        100
      )
    end

    @tag timeout: 120_000
    test "get_range (100 results)" do
      {store, dir} = open_store_with_n(1_000, "getrange100")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_range (100)",
        fn -> store end,
        fn s -> NIF.get_range(s, "key:00000001", "key:99999999", 100) end,
        100
      )
    end

    @tag timeout: 120_000
    test "get_range_zero_copy (100 results)" do
      {store, dir} = open_store_with_n(1_000, "getrangezc100")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_range_zero_copy (100)",
        fn -> store end,
        fn s -> NIF.get_range_zero_copy(s, "key:00000001", "key:99999999", 100) end,
        100
      )
    end
  end

  # ============================================================================
  # MAINTENANCE / STATS
  # ============================================================================

  describe "Maintenance / Stats" do
    @tag timeout: 120_000
    test "shard_stats" do
      {store, dir} = open_store_with_n(100, "stats")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "shard_stats",
        fn -> store end,
        fn s -> {:ok, _} = NIF.shard_stats(s) end
      )
    end

    @tag timeout: 120_000
    test "file_sizes" do
      {store, dir} = open_store_with_n(100, "filesizes")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "file_sizes",
        fn -> store end,
        fn s -> {:ok, _} = NIF.file_sizes(s) end
      )
    end

    @tag timeout: 120_000
    test "available_disk_space" do
      {store, dir} = open_store_with_n(10, "diskspace")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "available_disk_space",
        fn -> store end,
        fn s -> {:ok, _} = NIF.available_disk_space(s) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "write_hint" do
      {store, dir} = open_store_with_n(100, "hint")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "write_hint",
        fn -> store end,
        fn s -> :ok = NIF.write_hint(s) end,
        100
      )
    end

    @tag timeout: 120_000
    test "purge_expired" do
      dir = tmp_dir("purge")
      store = open_store(dir)
      # Add some entries with expiry in the past
      now_ms = System.system_time(:millisecond)

      for i <- 1..100 do
        :ok = NIF.put(store, "exp:#{i}", "v", now_ms - 1000)
      end

      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "purge_expired",
        fn -> store end,
        fn s -> {:ok, _} = NIF.purge_expired(s) end,
        100
      )
    end

    @tag timeout: 120_000
    test "run_compaction" do
      dir = tmp_dir("compact")
      store = open_store(dir)

      for i <- 1..100 do
        :ok = NIF.put(store, "comp:#{i}", String.duplicate("x", 100), 0)
      end

      on_exit(fn -> File.rm_rf(dir) end)

      # Get file sizes to know which files exist
      {:ok, file_sizes} = NIF.file_sizes(store)
      file_ids = Enum.map(file_sizes, fn {id, _size} -> id end)

      if file_ids != [] do
        benchmark_nif(
          "run_compaction",
          fn -> {store, file_ids} end,
          fn {s, fids} -> NIF.run_compaction(s, fids) end,
          10
        )
      end
    end
  end

  # ============================================================================
  # TOKIO ASYNC IO
  # ============================================================================

  describe "Tokio Async IO" do
    @tag timeout: 120_000
    test "get_async" do
      dir = tmp_dir("get_async")
      store = open_store(dir)
      :ok = NIF.put(store, "async_key", String.duplicate("x", 100), 0)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "get_async (submit)",
        fn -> store end,
        fn s ->
          {:pending, :ok} = NIF.get_async(s, "async_key")

          receive do
            {:tokio_complete, :ok, _val} -> :ok
          after
            5_000 -> :timeout
          end
        end,
        1_000
      )
    end

    @tag timeout: 300_000
    test "delete_async" do
      dir = tmp_dir("del_async")
      store = open_store(dir)

      # Pre-populate using batch to avoid per-key fsync overhead
      batch =
        Enum.map(1..2_000, fn i ->
          {"adel:#{i}", "v", 0}
        end)

      batch
      |> Enum.chunk_every(500)
      |> Enum.each(fn chunk -> :ok = NIF.put_batch(store, chunk) end)

      on_exit(fn -> File.rm_rf(dir) end)
      counter = :atomics.new(1, [])

      benchmark_nif(
        "delete_async (submit)",
        fn -> {store, counter} end,
        fn {s, c} ->
          i = :atomics.add_get(c, 1, 1)
          {:pending, :ok} = NIF.delete_async(s, "adel:#{i}")

          receive do
            {:tokio_complete, _, _} -> :ok
          after
            5_000 -> :timeout
          end
        end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "put_batch_tokio_async (10 items)" do
      dir = tmp_dir("pbt_async")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch =
        Enum.map(1..10, fn i ->
          {"tbatch:#{i}", :crypto.strong_rand_bytes(64), 0}
        end)

      benchmark_nif(
        "put_batch_tokio_async (10)",
        fn -> store end,
        fn s ->
          {:pending, :ok} = NIF.put_batch_tokio_async(s, batch)

          receive do
            {:tokio_complete, _, _} -> :ok
          after
            5_000 -> :timeout
          end
        end,
        500
      )
    end

    @tag timeout: 120_000
    test "write_hint_async" do
      {store, dir} = open_store_with_n(100, "hint_async")
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "write_hint_async (submit+recv)",
        fn -> store end,
        fn s ->
          {:pending, :ok} = NIF.write_hint_async(s)

          receive do
            {:tokio_complete, _, _} -> :ok
          after
            5_000 -> :timeout
          end
        end,
        100
      )
    end

    @tag timeout: 120_000
    test "purge_expired_async" do
      dir = tmp_dir("purge_async")
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      benchmark_nif(
        "purge_expired_async (submit+recv)",
        fn -> store end,
        fn s ->
          {:pending, :ok} = NIF.purge_expired_async(s)

          receive do
            {:tokio_complete, _, _} -> :ok
          after
            5_000 -> :timeout
          end
        end,
        100
      )
    end

    @tag timeout: 120_000
    test "run_compaction_async" do
      dir = tmp_dir("compact_async")
      store = open_store(dir)

      for i <- 1..50 do
        :ok = NIF.put(store, "casync:#{i}", String.duplicate("x", 100), 0)
      end

      on_exit(fn -> File.rm_rf(dir) end)

      {:ok, file_sizes} = NIF.file_sizes(store)
      file_ids = Enum.map(file_sizes, fn {id, _size} -> id end)

      if file_ids != [] do
        benchmark_nif(
          "run_compaction_async",
          fn -> {store, file_ids} end,
          fn {s, fids} ->
            {:pending, :ok} = NIF.run_compaction_async(s, fids)

            receive do
              {:tokio_complete, _, _} -> :ok
            after
              10_000 -> :timeout
            end
          end,
          10
        )
      end
    end
  end

  # ============================================================================
  # BLOOM FILTER
  # ============================================================================

  describe "Bloom Filter" do
    @tag timeout: 120_000
    test "bloom_create" do
      benchmark_nif(
        "bloom_create",
        fn -> nil end,
        fn _ ->
          path = Path.join(System.tmp_dir!(), "bloom_bench_#{:rand.uniform(9_999_999)}.bloom")
          {:ok, _ref} = NIF.bloom_create(path, 10_000, 7)
          File.rm(path)
        end,
        500
      )
    end

    @tag timeout: 120_000
    test "bloom_add" do
      path = Path.join(System.tmp_dir!(), "bloom_add_bench.bloom")
      {:ok, ref} = NIF.bloom_create(path, 100_000, 7)
      on_exit(fn -> File.rm(path) end)

      counter = :atomics.new(1, [])

      benchmark_nif(
        "bloom_add",
        fn -> {ref, counter} end,
        fn {r, c} ->
          i = :atomics.add_get(c, 1, 1)
          NIF.bloom_add(r, "elem:#{i}")
        end
      )
    end

    @tag timeout: 120_000
    test "bloom_exists" do
      path = Path.join(System.tmp_dir!(), "bloom_exists_bench.bloom")
      {:ok, ref} = NIF.bloom_create(path, 100_000, 7)

      for i <- 1..1_000, do: NIF.bloom_add(ref, "elem:#{i}")
      on_exit(fn -> File.rm(path) end)

      benchmark_nif(
        "bloom_exists",
        fn -> ref end,
        fn r -> NIF.bloom_exists(r, "elem:500") end
      )
    end

    @tag timeout: 120_000
    test "bloom_madd (100)" do
      path = Path.join(System.tmp_dir!(), "bloom_madd_bench.bloom")
      {:ok, ref} = NIF.bloom_create(path, 1_000_000, 7)
      on_exit(fn -> File.rm(path) end)

      elements = Enum.map(1..100, fn i -> "madd:#{i}" end)

      benchmark_nif(
        "bloom_madd (100)",
        fn -> ref end,
        fn r -> NIF.bloom_madd(r, elements) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "bloom_mexists (100)" do
      path = Path.join(System.tmp_dir!(), "bloom_mexists_bench.bloom")
      {:ok, ref} = NIF.bloom_create(path, 1_000_000, 7)

      for i <- 1..1_000, do: NIF.bloom_add(ref, "mex:#{i}")
      on_exit(fn -> File.rm(path) end)

      elements = Enum.map(1..100, fn i -> "mex:#{i}" end)

      benchmark_nif(
        "bloom_mexists (100)",
        fn -> ref end,
        fn r -> NIF.bloom_mexists(r, elements) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "bloom_card" do
      path = Path.join(System.tmp_dir!(), "bloom_card_bench.bloom")
      {:ok, ref} = NIF.bloom_create(path, 100_000, 7)

      for i <- 1..1_000, do: NIF.bloom_add(ref, "card:#{i}")
      on_exit(fn -> File.rm(path) end)

      benchmark_nif(
        "bloom_card",
        fn -> ref end,
        fn r -> NIF.bloom_card(r) end
      )
    end

    @tag timeout: 120_000
    test "bloom_info" do
      path = Path.join(System.tmp_dir!(), "bloom_info_bench.bloom")
      {:ok, ref} = NIF.bloom_create(path, 100_000, 7)
      on_exit(fn -> File.rm(path) end)

      benchmark_nif(
        "bloom_info",
        fn -> ref end,
        fn r -> NIF.bloom_info(r) end
      )
    end

    @tag timeout: 120_000
    test "bloom_open" do
      path = Path.join(System.tmp_dir!(), "bloom_open_bench.bloom")
      {:ok, _ref} = NIF.bloom_create(path, 100_000, 7)
      on_exit(fn -> File.rm(path) end)

      benchmark_nif(
        "bloom_open",
        fn -> path end,
        fn p -> {:ok, _} = NIF.bloom_open(p) end,
        500
      )
    end

    @tag timeout: 120_000
    test "bloom_delete" do
      benchmark_nif(
        "bloom_delete",
        fn -> nil end,
        fn _ ->
          path = Path.join(System.tmp_dir!(), "bloom_del_#{:rand.uniform(9_999_999)}.bloom")
          {:ok, ref} = NIF.bloom_create(path, 1_000, 3)
          NIF.bloom_delete(ref)
        end,
        500
      )
    end
  end

  # ============================================================================
  # CUCKOO FILTER
  # ============================================================================

  describe "Cuckoo Filter" do
    @tag timeout: 120_000
    test "cuckoo_create" do
      benchmark_nif(
        "cuckoo_create",
        fn -> nil end,
        fn _ -> {:ok, _} = NIF.cuckoo_create(1024, 4, 500, 0) end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_add" do
      {:ok, ref} = NIF.cuckoo_create(100_000, 4, 500, 0)
      counter = :atomics.new(1, [])

      benchmark_nif(
        "cuckoo_add",
        fn -> {ref, counter} end,
        fn {r, c} ->
          i = :atomics.add_get(c, 1, 1)
          NIF.cuckoo_add(r, "cuck:#{i}")
        end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_addnx" do
      {:ok, ref} = NIF.cuckoo_create(100_000, 4, 500, 0)
      counter = :atomics.new(1, [])

      benchmark_nif(
        "cuckoo_addnx",
        fn -> {ref, counter} end,
        fn {r, c} ->
          i = :atomics.add_get(c, 1, 1)
          NIF.cuckoo_addnx(r, "cnx:#{i}")
        end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_exists" do
      {:ok, ref} = NIF.cuckoo_create(10_000, 4, 500, 0)
      for i <- 1..1_000, do: NIF.cuckoo_add(ref, "cex:#{i}")

      benchmark_nif(
        "cuckoo_exists",
        fn -> ref end,
        fn r -> NIF.cuckoo_exists(r, "cex:500") end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_del" do
      {:ok, ref} = NIF.cuckoo_create(100_000, 4, 500, 0)
      for i <- 1..10_000, do: NIF.cuckoo_add(ref, "cdel:#{i}")
      counter = :atomics.new(1, [])

      benchmark_nif(
        "cuckoo_del",
        fn -> {ref, counter} end,
        fn {r, c} ->
          i = :atomics.add_get(c, 1, 1)
          NIF.cuckoo_del(r, "cdel:#{i}")
        end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_mexists (100)" do
      {:ok, ref} = NIF.cuckoo_create(10_000, 4, 500, 0)
      for i <- 1..1_000, do: NIF.cuckoo_add(ref, "cmex:#{i}")

      items = Enum.map(1..100, fn i -> "cmex:#{i}" end)

      benchmark_nif(
        "cuckoo_mexists (100)",
        fn -> ref end,
        fn r -> NIF.cuckoo_mexists(r, items) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "cuckoo_count" do
      {:ok, ref} = NIF.cuckoo_create(10_000, 4, 500, 0)
      NIF.cuckoo_add(ref, "counted")

      benchmark_nif(
        "cuckoo_count",
        fn -> ref end,
        fn r -> NIF.cuckoo_count(r, "counted") end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_info" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)

      benchmark_nif(
        "cuckoo_info",
        fn -> ref end,
        fn r -> NIF.cuckoo_info(r) end
      )
    end

    @tag timeout: 120_000
    test "cuckoo_serialize" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      for i <- 1..100, do: NIF.cuckoo_add(ref, "cser:#{i}")

      benchmark_nif(
        "cuckoo_serialize",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cuckoo_serialize(r) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "cuckoo_deserialize" do
      {:ok, ref} = NIF.cuckoo_create(1024, 4, 500, 0)
      for i <- 1..100, do: NIF.cuckoo_add(ref, "cde:#{i}")
      {:ok, data} = NIF.cuckoo_serialize(ref)

      benchmark_nif(
        "cuckoo_deserialize",
        fn -> data end,
        fn d -> {:ok, _} = NIF.cuckoo_deserialize(d) end,
        1_000
      )
    end
  end

  # ============================================================================
  # COUNT-MIN SKETCH
  # ============================================================================

  describe "Count-Min Sketch" do
    @tag timeout: 120_000
    test "cms_create" do
      benchmark_nif(
        "cms_create",
        fn -> nil end,
        fn _ -> {:ok, _} = NIF.cms_create(1000, 7) end
      )
    end

    @tag timeout: 120_000
    test "cms_incrby (single)" do
      {:ok, ref} = NIF.cms_create(1000, 7)

      benchmark_nif(
        "cms_incrby (1 item)",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cms_incrby(r, [{"elem", 1}]) end
      )
    end

    @tag timeout: 120_000
    test "cms_incrby (100 items)" do
      {:ok, ref} = NIF.cms_create(10_000, 7)
      items = Enum.map(1..100, fn i -> {"item:#{i}", 1} end)

      benchmark_nif(
        "cms_incrby (100 items)",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cms_incrby(r, items) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "cms_query (single)" do
      {:ok, ref} = NIF.cms_create(1000, 7)
      NIF.cms_incrby(ref, [{"q_elem", 100}])

      benchmark_nif(
        "cms_query (1 item)",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cms_query(r, ["q_elem"]) end
      )
    end

    @tag timeout: 120_000
    test "cms_query (100 items)" do
      {:ok, ref} = NIF.cms_create(10_000, 7)
      items = Enum.map(1..100, fn i -> {"qitem:#{i}", 1} end)
      NIF.cms_incrby(ref, items)
      query_items = Enum.map(1..100, fn i -> "qitem:#{i}" end)

      benchmark_nif(
        "cms_query (100 items)",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cms_query(r, query_items) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "cms_merge (2 sketches)" do
      {:ok, dest} = NIF.cms_create(1000, 7)
      {:ok, src1} = NIF.cms_create(1000, 7)
      {:ok, src2} = NIF.cms_create(1000, 7)
      NIF.cms_incrby(src1, [{"a", 10}])
      NIF.cms_incrby(src2, [{"b", 20}])

      benchmark_nif(
        "cms_merge (2 sketches)",
        fn -> {dest, src1, src2} end,
        fn {d, s1, s2} -> :ok = NIF.cms_merge(d, [{s1, 1}, {s2, 1}]) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "cms_info" do
      {:ok, ref} = NIF.cms_create(1000, 7)

      benchmark_nif(
        "cms_info",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cms_info(r) end
      )
    end

    @tag timeout: 120_000
    test "cms_to_bytes" do
      {:ok, ref} = NIF.cms_create(1000, 7)
      NIF.cms_incrby(ref, [{"x", 100}])

      benchmark_nif(
        "cms_to_bytes",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.cms_to_bytes(r) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "cms_from_bytes" do
      {:ok, ref} = NIF.cms_create(1000, 7)
      NIF.cms_incrby(ref, [{"x", 100}])
      {:ok, data} = NIF.cms_to_bytes(ref)

      benchmark_nif(
        "cms_from_bytes",
        fn -> data end,
        fn d -> {:ok, _} = NIF.cms_from_bytes(d) end,
        1_000
      )
    end
  end

  # ============================================================================
  # TOP-K
  # ============================================================================

  describe "Top-K" do
    @tag timeout: 120_000
    test "topk_create" do
      benchmark_nif(
        "topk_create",
        fn -> nil end,
        fn _ -> {:ok, _} = NIF.topk_create(10, 8, 7, 0.9) end
      )
    end

    @tag timeout: 120_000
    test "topk_add (single)" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)

      benchmark_nif(
        "topk_add (1 item)",
        fn -> ref end,
        fn r -> NIF.topk_add(r, ["elem"]) end
      )
    end

    @tag timeout: 120_000
    test "topk_add (100)" do
      {:ok, ref} = NIF.topk_create(10, 800, 7, 0.9)
      items = Enum.map(1..100, fn i -> "topk:#{i}" end)

      benchmark_nif(
        "topk_add (100 items)",
        fn -> ref end,
        fn r -> NIF.topk_add(r, items) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "topk_incrby (100)" do
      {:ok, ref} = NIF.topk_create(10, 800, 7, 0.9)
      pairs = Enum.map(1..100, fn i -> {"incr:#{i}", 1} end)

      benchmark_nif(
        "topk_incrby (100 pairs)",
        fn -> ref end,
        fn r -> NIF.topk_incrby(r, pairs) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "topk_query (100)" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)
      for i <- 1..100, do: NIF.topk_add(ref, ["tq:#{i}"])
      items = Enum.map(1..100, fn i -> "tq:#{i}" end)

      benchmark_nif(
        "topk_query (100 items)",
        fn -> ref end,
        fn r -> NIF.topk_query(r, items) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "topk_list" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)
      for i <- 1..50, do: NIF.topk_add(ref, ["tl:#{i}"])

      benchmark_nif(
        "topk_list",
        fn -> ref end,
        fn r -> NIF.topk_list(r) end
      )
    end

    @tag timeout: 120_000
    test "topk_list_with_count" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)
      for i <- 1..50, do: NIF.topk_add(ref, ["tlc:#{i}"])

      benchmark_nif(
        "topk_list_with_count",
        fn -> ref end,
        fn r -> NIF.topk_list_with_count(r) end
      )
    end

    @tag timeout: 120_000
    test "topk_info" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)

      benchmark_nif(
        "topk_info",
        fn -> ref end,
        fn r -> NIF.topk_info(r) end
      )
    end

    @tag timeout: 120_000
    test "topk_to_bytes" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)
      for i <- 1..50, do: NIF.topk_add(ref, ["tb:#{i}"])

      benchmark_nif(
        "topk_to_bytes",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.topk_to_bytes(r) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "topk_from_bytes" do
      {:ok, ref} = NIF.topk_create(10, 80, 7, 0.9)
      for i <- 1..50, do: NIF.topk_add(ref, ["fb:#{i}"])
      {:ok, data} = NIF.topk_to_bytes(ref)

      benchmark_nif(
        "topk_from_bytes",
        fn -> data end,
        fn d -> {:ok, _} = NIF.topk_from_bytes(d) end,
        1_000
      )
    end
  end

  # ============================================================================
  # T-DIGEST
  # ============================================================================

  describe "T-Digest" do
    @tag timeout: 120_000
    test "tdigest_create" do
      benchmark_nif(
        "tdigest_create",
        fn -> nil end,
        fn _ -> NIF.tdigest_create(100.0) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_add (single)" do
      ref = NIF.tdigest_create(100.0)

      benchmark_nif(
        "tdigest_add (1 value)",
        fn -> ref end,
        fn r -> :ok = NIF.tdigest_add(r, [42.0]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_add (1000 values)" do
      ref = NIF.tdigest_create(100.0)
      values = Enum.map(1..1000, fn i -> i * 1.0 end)

      benchmark_nif(
        "tdigest_add (1000 values)",
        fn -> ref end,
        fn r -> :ok = NIF.tdigest_add(r, values) end,
        100
      )
    end

    @tag timeout: 120_000
    test "tdigest_quantile" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_quantile",
        fn -> ref end,
        fn r -> NIF.tdigest_quantile(r, [0.5, 0.95, 0.99]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_cdf" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_cdf",
        fn -> ref end,
        fn r -> NIF.tdigest_cdf(r, [5000.0]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_trimmed_mean" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_trimmed_mean",
        fn -> ref end,
        fn r -> NIF.tdigest_trimmed_mean(r, 0.1, 0.9) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_rank" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_rank",
        fn -> ref end,
        fn r -> NIF.tdigest_rank(r, [5000.0]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_revrank" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_revrank",
        fn -> ref end,
        fn r -> NIF.tdigest_revrank(r, [5000.0]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_byrank" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_byrank",
        fn -> ref end,
        fn r -> NIF.tdigest_byrank(r, [0, 5000, 9999]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_byrevrank" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..10_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_byrevrank",
        fn -> ref end,
        fn r -> NIF.tdigest_byrevrank(r, [0, 5000, 9999]) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_min" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, [1.0, 2.0, 3.0])

      benchmark_nif(
        "tdigest_min",
        fn -> ref end,
        fn r -> NIF.tdigest_min(r) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_max" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, [1.0, 2.0, 3.0])

      benchmark_nif(
        "tdigest_max",
        fn -> ref end,
        fn r -> NIF.tdigest_max(r) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_info" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..1_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_info",
        fn -> ref end,
        fn r -> NIF.tdigest_info(r) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_merge (2 digests)" do
      ref1 = NIF.tdigest_create(100.0)
      ref2 = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref1, Enum.map(1..1_000, fn i -> i * 1.0 end))
      NIF.tdigest_add(ref2, Enum.map(1001..2_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_merge (2 digests)",
        fn -> {ref1, ref2} end,
        fn {r1, r2} -> NIF.tdigest_merge([r1, r2], 100.0) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "tdigest_reset" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..1_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_reset",
        fn -> ref end,
        fn r -> :ok = NIF.tdigest_reset(r) end
      )
    end

    @tag timeout: 120_000
    test "tdigest_serialize" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..1_000, fn i -> i * 1.0 end))

      benchmark_nif(
        "tdigest_serialize",
        fn -> ref end,
        fn r -> NIF.tdigest_serialize(r) end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "tdigest_deserialize" do
      ref = NIF.tdigest_create(100.0)
      NIF.tdigest_add(ref, Enum.map(1..1_000, fn i -> i * 1.0 end))
      data = NIF.tdigest_serialize(ref)

      benchmark_nif(
        "tdigest_deserialize",
        fn -> data end,
        fn d -> {:ok, _} = NIF.tdigest_deserialize(d) end,
        1_000
      )
    end
  end

  # ============================================================================
  # HNSW VECTOR SEARCH
  # ============================================================================

  describe "HNSW Vector Search" do
    @tag timeout: 120_000
    test "hnsw_new" do
      benchmark_nif(
        "hnsw_new",
        fn -> nil end,
        fn _ -> {:ok, _} = NIF.hnsw_new(128, 16, 200, "l2") end
      )
    end

    @tag timeout: 120_000
    test "hnsw_add" do
      {:ok, ref} = NIF.hnsw_new(32, 16, 200, "l2")

      benchmark_nif(
        "hnsw_add (32d)",
        fn -> ref end,
        fn r ->
          vec = Enum.map(1..32, fn _ -> :rand.uniform() end)
          key = "vec:#{:rand.uniform(999_999)}"
          {:ok, _} = NIF.hnsw_add(r, key, vec)
        end,
        1_000
      )
    end

    @tag timeout: 120_000
    test "hnsw_delete" do
      {:ok, ref} = NIF.hnsw_new(8, 16, 200, "l2")

      for i <- 1..1_000 do
        vec = Enum.map(1..8, fn d -> (i + d) * 1.0 end)
        NIF.hnsw_add(ref, "hdel:#{i}", vec)
      end

      counter = :atomics.new(1, [])

      benchmark_nif(
        "hnsw_delete",
        fn -> {ref, counter} end,
        fn {r, c} ->
          i = :atomics.add_get(c, 1, 1)
          NIF.hnsw_delete(r, "hdel:#{i}")
        end,
        500
      )
    end

    @tag timeout: 120_000
    test "hnsw_search (100 vectors, 8d)" do
      {:ok, ref} = NIF.hnsw_new(8, 16, 200, "l2")

      for i <- 1..100 do
        vec = Enum.map(1..8, fn d -> (i + d) * 1.0 end)
        NIF.hnsw_add(ref, "hs100:#{i}", vec)
      end

      query = Enum.map(1..8, fn d -> 50.0 + d * 1.0 end)

      benchmark_nif(
        "hnsw_search (100 vecs, 8d)",
        fn -> {ref, query} end,
        fn {r, q} -> {:ok, _} = NIF.hnsw_search(r, q, 10, 50) end,
        1_000
      )
    end

    @tag timeout: 180_000
    test "hnsw_search (1K vectors, 32d)" do
      {:ok, ref} = NIF.hnsw_new(32, 16, 200, "l2")

      for i <- 1..1_000 do
        vec = Enum.map(1..32, fn d -> (i * 7 + d * 13) / 1000.0 end)
        NIF.hnsw_add(ref, "hs1k:#{i}", vec)
      end

      query = Enum.map(1..32, fn d -> 500.0 / 1000.0 + d * 13.0 / 1000.0 end)

      benchmark_nif(
        "hnsw_search (1K vecs, 32d)",
        fn -> {ref, query} end,
        fn {r, q} -> {:ok, _} = NIF.hnsw_search(r, q, 10, 200) end,
        100
      )
    end

    @tag timeout: 120_000
    test "hnsw_count" do
      {:ok, ref} = NIF.hnsw_new(8, 16, 200, "l2")

      for i <- 1..100 do
        vec = Enum.map(1..8, fn d -> (i + d) * 1.0 end)
        NIF.hnsw_add(ref, "hc:#{i}", vec)
      end

      benchmark_nif(
        "hnsw_count",
        fn -> ref end,
        fn r -> {:ok, _} = NIF.hnsw_count(r) end
      )
    end

    @tag timeout: 180_000
    test "vsearch_nif (1K vectors, 32d)" do
      {:ok, ref} = NIF.hnsw_new(32, 16, 200, "l2")

      for i <- 1..1_000 do
        vec = Enum.map(1..32, fn d -> (i * 7 + d * 13) / 1000.0 end)
        NIF.hnsw_add(ref, "vs1k:#{i}", vec)
      end

      query = Enum.map(1..32, fn d -> 500.0 / 1000.0 + d * 13.0 / 1000.0 end)

      benchmark_nif(
        "vsearch_nif (1K vecs, 32d)",
        fn -> {ref, query} end,
        fn {r, q} -> {:ok, _} = NIF.vsearch_nif(r, q, 10, 200) end,
        100
      )
    end
  end

  # ============================================================================
  # TRACKING ALLOCATOR
  # ============================================================================

  describe "Tracking Allocator" do
    @tag timeout: 120_000
    test "rust_allocated_bytes" do
      benchmark_nif(
        "rust_allocated_bytes",
        fn -> nil end,
        fn _ -> NIF.rust_allocated_bytes() end
      )
    end
  end
end
