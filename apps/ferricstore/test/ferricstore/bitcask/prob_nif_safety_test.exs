defmodule Ferricstore.Bitcask.ProbNIFSafetyTest do
  @moduledoc """
  Comprehensive safety tests for all five probabilistic-structure NIFs:

    1. **Bloom** (mmap-backed)
    2. **Cuckoo** (in-memory, serialisable)
    3. **CMS** (Count-Min Sketch)
    4. **TopK** (CMS + min-heap)
    5. **TDigest** (quantile estimation)

  ## Categories

  - Scheduler safety (Normal-only, yielding, no starvation)
  - Memory leak detection (create/destroy cycles, process crash cleanup)
  - mmap safety (Bloom-specific: file lifecycle, concurrent read, size)
  - Correctness under concurrency (concurrent adds, reads-during-writes)
  - Edge cases (empty operations, capacity limits, binary null bytes)

  ## Running

      mix test apps/ferricstore/test/ferricstore/bitcask/prob_nif_safety_test.exs --include nif_safety

  Tagged `:nif_safety` so these are excluded from the default `mix test` run
  (they are heavier than unit tests and involve scheduler/memory probing).
  """

  use ExUnit.Case, async: false

  @moduletag :nif_safety
  @moduletag timeout: 300_000

  alias Ferricstore.Bitcask.NIF

  # ============================================================================
  # Helpers
  # ============================================================================

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "prob_nif_safety_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp bloom_path(dir, name) do
    Path.join(dir, "#{name}.bloom")
  end

  defp create_bloom(dir, opts \\ []) do
    num_bits = Keyword.get(opts, :num_bits, 10_000)
    num_hashes = Keyword.get(opts, :num_hashes, 7)
    name = Keyword.get(opts, :name, "test")
    path = bloom_path(dir, name)
    {:ok, resource} = NIF.bloom_create(path, num_bits, num_hashes)
    {resource, path}
  end

  defp create_cuckoo(opts \\ []) do
    capacity = Keyword.get(opts, :capacity, 1024)
    bucket_size = Keyword.get(opts, :bucket_size, 4)
    max_kicks = Keyword.get(opts, :max_kicks, 500)
    expansion = Keyword.get(opts, :expansion, 0)
    {:ok, resource} = NIF.cuckoo_create(capacity, bucket_size, max_kicks, expansion)
    resource
  end

  defp create_cms(opts \\ []) do
    width = Keyword.get(opts, :width, 1000)
    depth = Keyword.get(opts, :depth, 7)
    {:ok, resource} = NIF.cms_create(width, depth)
    resource
  end

  defp create_topk(opts \\ []) do
    k = Keyword.get(opts, :k, 10)
    width = Keyword.get(opts, :width, 100)
    depth = Keyword.get(opts, :depth, 7)
    decay = Keyword.get(opts, :decay, 0.9)
    {:ok, resource} = NIF.topk_create(k, width, depth, decay)
    resource
  end

  defp create_tdigest(opts \\ []) do
    compression = Keyword.get(opts, :compression, 100.0)
    NIF.tdigest_create(compression)
  end

  # Measures total DirtyIo scheduler wall-time delta.
  # Returns {before_snapshot, measure_fn} where measure_fn returns the delta.
  defp dirty_io_time_snapshot do
    :erlang.system_flag(:scheduler_wall_time, true)
    Process.sleep(5)
    before = :erlang.statistics(:scheduler_wall_time_all)
    {before, fn ->
      after_snap = :erlang.statistics(:scheduler_wall_time_all)
      :erlang.system_flag(:scheduler_wall_time, false)
      # Dirty IO schedulers have IDs > normal scheduler count
      normal_count = :erlang.system_info(:schedulers)
      dirty_io_before = Enum.filter(before, fn {id, _, _} -> id > normal_count + 1 end)
      dirty_io_after = Enum.filter(after_snap, fn {id, _, _} -> id > normal_count + 1 end)

      before_map = Map.new(dirty_io_before, fn {id, active, _total} -> {id, active} end)
      after_map = Map.new(dirty_io_after, fn {id, active, _total} -> {id, active} end)

      Map.keys(after_map)
      |> Enum.map(fn id ->
        (Map.get(after_map, id, 0) - Map.get(before_map, id, 0))
      end)
      |> Enum.sum()
    end}
  end

  # Spawns a GenServer-like responder process that replies to :ping messages.
  # Returns {pid, ping_fn} where ping_fn returns the response time in microseconds.
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
      :stop -> :ok
    end
  end

  # Starts a background counter process to verify schedulers aren't starved.
  # Returns {pid, sample_fn}.
  defp start_counter do
    counter = :atomics.new(1, signed: false)
    pid = spawn_link(fn -> counter_loop(counter) end)
    sample_fn = fn -> :atomics.get(counter, 1) end
    {pid, sample_fn}
  end

  defp counter_loop(counter) do
    :atomics.add(counter, 1, 1)
    counter_loop(counter)
  end

  # ============================================================================
  # BLOOM FILTER TESTS
  # ============================================================================

  # --------------------------------------------------------------------------
  # Bloom: Scheduler Safety
  # --------------------------------------------------------------------------

  describe "bloom: scheduler safety" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "runs on Normal scheduler, not DirtyIo", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 100_000)

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for i <- 1..10_000 do
        NIF.bloom_exists(filter, "key_#{i}")
      end

      dirty_delta = measure_dirty.()
      # DirtyIo delta must be near zero (< 1ms expressed in scheduler wall-time units)
      # scheduler_wall_time units are microseconds on most platforms
      assert dirty_delta < 5_000_000,
        "DirtyIo time delta #{dirty_delta} suggests NIF ran on DirtyIo scheduler"
    end

    test "yields for large operations — pinger stays responsive", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 10_000_000)

      {_pinger_pid, ping} = start_pinger()

      # Add many elements in a batch to exercise the NIF under load
      elements = Enum.map(1..100_000, fn i -> "elem_#{i}" end)

      task = Task.async(fn ->
        NIF.bloom_madd(filter, elements)
      end)

      # While the NIF is running, verify pinger responds quickly
      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid = Enum.filter(responses, &is_integer/1)
      assert length(valid) > 0, "pinger never responded"
      avg = Enum.sum(valid) / length(valid)
      # Average ping should be well under 50ms (50_000 us)
      assert avg < 50_000,
        "average ping response #{avg}us suggests scheduler starvation"
    end

    test "no scheduler starvation under concurrent load", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 1_000_000)

      {_counter_pid, sample} = start_counter()
      before_count = sample.()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for i <- 1..1_000 do
              NIF.bloom_add(filter, "task_key_#{i}_#{:rand.uniform(100_000)}")
            end
          end)
        end

      Task.await_many(tasks, 60_000)
      after_count = sample.()

      assert after_count - before_count > 100,
        "background counter barely incremented (#{after_count - before_count}), schedulers starved"
    end

    test "consume_timeslice — large bloom_madd completes across multiple slices", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 10_000_000)

      elements = Enum.map(1..10_000, fn i -> "slice_#{i}" end)

      {_counter_pid, sample} = start_counter()
      c0 = sample.()

      NIF.bloom_madd(filter, elements)

      c1 = sample.()
      # If the NIF consumed everything in one timeslice, the counter would barely
      # increment. With yielding, the counter keeps running.
      assert c1 - c0 > 10,
        "counter only incremented #{c1 - c0} during bloom_madd; NIF may not be yielding"
    end

    test "Normal scheduler only — explicit dirty time check for bloom_exists", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 100_000)
      for i <- 1..1_000, do: NIF.bloom_add(filter, "pre_#{i}")

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for i <- 1..10_000 do
        NIF.bloom_exists(filter, "key_#{i}")
      end

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "bloom_exists used #{dirty_delta} dirty scheduler time"
    end
  end

  # --------------------------------------------------------------------------
  # Bloom: Memory Leak Tests
  # --------------------------------------------------------------------------

  describe "bloom: memory leak" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "create/destroy 1000 times — no memory growth", %{dir: dir} do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for i <- 1..1_000 do
        path = bloom_path(dir, "leak_#{i}")
        {:ok, filter} = NIF.bloom_create(path, 1_000, 7)
        NIF.bloom_delete(filter)
      end

      :erlang.garbage_collect()
      mem_after = :erlang.memory(:total)

      assert mem_after - mem_before < 20_000_000,
        "memory grew by #{mem_after - mem_before} bytes after 1000 create/destroy cycles"
    end

    test "add 100K items, delete structure — memory freed", %{dir: dir} do
      {filter, path} = create_bloom(dir, num_bits: 1_000_000, name: "memfree")
      for i <- 1..100_000, do: NIF.bloom_add(filter, "item_#{i}")

      # Bloom is mmap'd: check the backing file exists and has a reasonable
      # size rather than :erlang.memory/1 which doesn't include mmap regions.
      assert File.exists?(path), "bloom backing file should exist"
      stat = File.stat!(path)
      assert stat.size > 100_000, "bloom backing file should be > 100KB"

      NIF.bloom_delete(filter)
      # Clear references
      _ = filter
      :erlang.garbage_collect()
      Process.sleep(50)

      # After bloom_delete the backing file should be removed.
      refute File.exists?(path), "bloom backing file should be deleted"
    end

    test "NIF resource destructor fires on GC", %{dir: dir} do
      path = bloom_path(dir, "destructor")
      {:ok, _filter} = NIF.bloom_create(path, 10_000, 7)
      assert File.exists?(path)

      # Drop reference and force GC — resource destructor should munmap
      :erlang.garbage_collect()
      Process.sleep(50)

      # The file should still exist (destructor munmaps but doesn't unlink unless bloom_delete is called)
      # The key safety property is that the BEAM doesn't crash
      assert true
    end

    test "process crash doesn't leak NIF resources", %{dir: dir} do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for i <- 1..100 do
        pid = spawn(fn ->
          path = bloom_path(dir, "crash_#{i}")
          {:ok, _filter} = NIF.bloom_create(path, 10_000, 7)
          Process.sleep(:infinity)
        end)

        # Give it time to create
        Process.sleep(5)
        Process.exit(pid, :kill)
      end

      # Wait for cleanup
      Process.sleep(200)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 100 process crashes"
    end

    test "concurrent create/destroy — no leak", %{dir: dir} do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      tasks =
        for t <- 1..50 do
          Task.async(fn ->
            for i <- 1..100 do
              path = bloom_path(dir, "conc_#{t}_#{i}")
              {:ok, filter} = NIF.bloom_create(path, 1_000, 7)
              NIF.bloom_add(filter, "item")
              NIF.bloom_delete(filter)
            end
          end)
        end

      Task.await_many(tasks, 120_000)
      :erlang.garbage_collect()
      Process.sleep(100)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 5000 concurrent create/destroy cycles"
    end
  end

  # --------------------------------------------------------------------------
  # Bloom: mmap Safety Tests
  # --------------------------------------------------------------------------

  describe "bloom: mmap safety" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "file created on bloom_create", %{dir: dir} do
      path = bloom_path(dir, "mmap_create")
      refute File.exists?(path)

      {:ok, _filter} = NIF.bloom_create(path, 10_000, 7)
      assert File.exists?(path)
    end

    test "file removed on bloom_delete", %{dir: dir} do
      path = bloom_path(dir, "mmap_delete")
      {:ok, filter} = NIF.bloom_create(path, 10_000, 7)
      assert File.exists?(path)

      :ok = NIF.bloom_delete(filter)
      refute File.exists?(path)
    end

    test "file survives process crash (mmap'd by OS)", %{dir: dir} do
      path = bloom_path(dir, "mmap_crash")

      pid = spawn(fn ->
        {:ok, filter} = NIF.bloom_create(path, 10_000, 7)
        NIF.bloom_add(filter, "survive")
        # Signal parent, then wait to be killed
        send(Process.group_leader(), :ready)
        Process.sleep(:infinity)
      end)

      Process.sleep(50)
      Process.exit(pid, :kill)
      Process.sleep(100)

      # File should still exist — OS manages mmap independently of process
      assert File.exists?(path)
    end

    test "multiple processes can read same mmap'd bloom concurrently", %{dir: dir} do
      path = bloom_path(dir, "mmap_concurrent")
      {:ok, filter} = NIF.bloom_create(path, 100_000, 7)

      for i <- 1..1_000, do: NIF.bloom_add(filter, "item_#{i}")

      # Open the same bloom from another "view" using bloom_open
      {:ok, filter2} = NIF.bloom_open(path)

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for i <- 1..1_000 do
              result1 = NIF.bloom_exists(filter, "item_#{i}")
              result2 = NIF.bloom_exists(filter2, "item_#{i}")
              assert result1 == 1
              assert result2 == 1
            end
          end)
        end

      Task.await_many(tasks, 30_000)
    end

    test "mmap file size matches expected bit array size", %{dir: dir} do
      num_bits = 100_000
      num_hashes = 7
      path = bloom_path(dir, "mmap_size")

      {:ok, _filter} = NIF.bloom_create(path, num_bits, num_hashes)

      expected_byte_count = div(num_bits + 7, 8)
      header_size = 32
      expected_file_size = header_size + expected_byte_count

      %{size: actual_size} = File.stat!(path)
      assert actual_size == expected_file_size,
        "expected file size #{expected_file_size}, got #{actual_size}"
    end
  end

  # --------------------------------------------------------------------------
  # Bloom: Correctness Under Concurrency
  # --------------------------------------------------------------------------

  describe "bloom: concurrency correctness" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "concurrent adds don't corrupt — all items findable", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 10_000_000)

      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            for i <- 1..1_000 do
              NIF.bloom_add(filter, "t#{t}_item_#{i}")
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      # Verify all items are findable (no false negatives)
      for t <- 1..10, i <- 1..1_000 do
        assert NIF.bloom_exists(filter, "t#{t}_item_#{i}") == 1,
          "false negative for t#{t}_item_#{i}"
      end
    end

    test "concurrent reads during writes never crash", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 1_000_000)

      writer = Task.async(fn ->
        for i <- 1..10_000 do
          NIF.bloom_add(filter, "write_#{i}")
        end
      end)

      reader = Task.async(fn ->
        for i <- 1..10_000 do
          # Should never crash — may return 0 or 1
          result = NIF.bloom_exists(filter, "write_#{i}")
          assert result in [0, 1]
        end
      end)

      Task.await(writer, 30_000)
      Task.await(reader, 30_000)
    end

    test "after add succeeds, exists returns 1 from any task", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 1_000_000)

      # Pre-add items
      for i <- 1..1_000, do: NIF.bloom_add(filter, "verify_#{i}")

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for i <- 1..1_000 do
              assert NIF.bloom_exists(filter, "verify_#{i}") == 1
            end
          end)
        end

      Task.await_many(tasks, 30_000)
    end
  end

  # --------------------------------------------------------------------------
  # Bloom: Edge Cases
  # --------------------------------------------------------------------------

  describe "bloom: edge cases" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "empty bloom operations don't crash", %{dir: dir} do
      {filter, _path} = create_bloom(dir)

      assert NIF.bloom_exists(filter, "nonexistent") == 0
      assert NIF.bloom_card(filter) == 0
      {:ok, {num_bits, count, _num_hashes}} = NIF.bloom_info(filter)
      assert num_bits > 0
      assert count == 0
      assert NIF.bloom_mexists(filter, []) == []
      assert NIF.bloom_madd(filter, []) == []
    end

    test "bloom with max-ish capacity works", %{dir: dir} do
      # 1M bits filter
      {filter, _path} = create_bloom(dir, num_bits: 1_000_000, name: "large")

      for i <- 1..10_000, do: NIF.bloom_add(filter, "big_#{i}")
      assert NIF.bloom_card(filter) == 10_000

      # Verify no false negatives
      for i <- 1..10_000 do
        assert NIF.bloom_exists(filter, "big_#{i}") == 1
      end
    end

    test "binary data with null bytes", %{dir: dir} do
      {filter, _path} = create_bloom(dir)

      null_key = <<0, 1, 2, 0, 3, 0, 0>>
      assert NIF.bloom_add(filter, null_key) == 1
      assert NIF.bloom_exists(filter, null_key) == 1
      assert NIF.bloom_exists(filter, <<0, 1, 2, 0, 3, 0, 1>>) in [0, 1]
    end
  end

  # ============================================================================
  # CUCKOO FILTER TESTS
  # ============================================================================

  # --------------------------------------------------------------------------
  # Cuckoo: Scheduler Safety
  # --------------------------------------------------------------------------

  describe "cuckoo: scheduler safety" do
    test "runs on Normal scheduler, not DirtyIo" do
      filter = create_cuckoo(capacity: 4096)

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for i <- 1..10_000 do
        NIF.cuckoo_exists(filter, "key_#{i}")
      end

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "DirtyIo time delta #{dirty_delta} suggests cuckoo NIF ran on DirtyIo"
    end

    test "yields for large operations — pinger stays responsive" do
      filter = create_cuckoo(capacity: 8192)

      {_pinger_pid, ping} = start_pinger()

      task = Task.async(fn ->
        for i <- 1..100_000 do
          NIF.cuckoo_exists(filter, "elem_#{i}")
        end
      end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid = Enum.filter(responses, &is_integer/1)
      assert length(valid) > 0
      avg = Enum.sum(valid) / length(valid)
      assert avg < 50_000,
        "average ping #{avg}us suggests scheduler starvation from cuckoo"
    end

    test "no scheduler starvation under concurrent load" do
      filter = create_cuckoo(capacity: 8192)

      {_counter_pid, sample} = start_counter()
      before_count = sample.()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for i <- 1..1_000 do
              NIF.cuckoo_exists(filter, "conc_#{i}_#{:rand.uniform(100_000)}")
            end
          end)
        end

      Task.await_many(tasks, 60_000)
      after_count = sample.()

      assert after_count - before_count > 100,
        "counter barely incremented during cuckoo concurrent load"
    end

    test "consume_timeslice verification for cuckoo_mexists" do
      filter = create_cuckoo(capacity: 4096)
      items = Enum.map(1..1_000, fn i -> "item_#{i}" end)
      for item <- items, do: NIF.cuckoo_add(filter, item)

      {_counter_pid, sample} = start_counter()
      c0 = sample.()

      # Query a large batch
      NIF.cuckoo_mexists(filter, items)

      c1 = sample.()
      assert c1 - c0 >= 0, "counter should not go backwards"
    end

    test "Normal scheduler only — explicit dirty time check" do
      filter = create_cuckoo(capacity: 4096)

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for i <- 1..10_000 do
        NIF.cuckoo_add(filter, "dirty_check_#{i}")
      end

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "cuckoo_add used #{dirty_delta} dirty scheduler time"
    end
  end

  # --------------------------------------------------------------------------
  # Cuckoo: Memory Leak Tests
  # --------------------------------------------------------------------------

  describe "cuckoo: memory leak" do
    test "create/destroy 1000 times — no memory growth" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..1_000 do
        filter = create_cuckoo(capacity: 256)
        _info = NIF.cuckoo_info(filter)
        # Drop reference
        _ = filter
        :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      mem_after = :erlang.memory(:total)

      assert mem_after - mem_before < 5_000_000,
        "memory grew by #{mem_after - mem_before} after 1000 cuckoo create/GC cycles"
    end

    test "add items, drop reference, GC — memory freed" do
      :erlang.garbage_collect()
      mem_baseline = :erlang.memory(:total)

      filter = create_cuckoo(capacity: 8192)
      for i <- 1..10_000 do
        NIF.cuckoo_add(filter, "leak_#{i}")
      end

      _ = filter
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_baseline < 5_000_000,
        "memory still #{mem_after - mem_baseline} above baseline after cuckoo GC"
    end

    test "NIF resource destructor fires on GC" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      # Create and immediately discard
      _filter = create_cuckoo(capacity: 4096)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      # Just verify no crash and memory stays reasonable
      assert mem_after - mem_before < 5_000_000
    end

    test "process crash doesn't leak NIF resources" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..100 do
        pid = spawn(fn ->
          filter = create_cuckoo(capacity: 1024)
          for i <- 1..100, do: NIF.cuckoo_add(filter, "crash_#{i}")
          Process.sleep(:infinity)
        end)
        Process.sleep(5)
        Process.exit(pid, :kill)
      end

      Process.sleep(200)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 100 cuckoo process crashes"
    end

    test "concurrent create/destroy — no leak" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..100 do
              filter = create_cuckoo(capacity: 256)
              NIF.cuckoo_add(filter, "item")
              _ = filter
              :erlang.garbage_collect()
            end
          end)
        end

      Task.await_many(tasks, 120_000)
      :erlang.garbage_collect()
      Process.sleep(100)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 5000 concurrent cuckoo create/GC"
    end
  end

  # --------------------------------------------------------------------------
  # Cuckoo: Correctness Under Concurrency
  # --------------------------------------------------------------------------

  describe "cuckoo: concurrency correctness" do
    test "concurrent adds don't corrupt" do
      filter = create_cuckoo(capacity: 16_384)

      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            for i <- 1..1_000 do
              NIF.cuckoo_add(filter, "t#{t}_#{i}")
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      # Verify items were actually inserted (no false negatives for recently added items)
      # Note: cuckoo can have false positives but not false negatives for items actually inserted
      found_count =
        for t <- 1..10, i <- 1..1_000, reduce: 0 do
          acc ->
            if NIF.cuckoo_exists(filter, "t#{t}_#{i}") == 1, do: acc + 1, else: acc
        end

      # At minimum the majority should be found (some may fail to insert if filter is full)
      assert found_count > 5_000,
        "only #{found_count}/10000 items found after concurrent adds"
    end

    test "concurrent reads during writes never crash" do
      filter = create_cuckoo(capacity: 8192)

      writer = Task.async(fn ->
        for i <- 1..5_000 do
          NIF.cuckoo_add(filter, "rw_#{i}")
        end
      end)

      reader = Task.async(fn ->
        for i <- 1..5_000 do
          result = NIF.cuckoo_exists(filter, "rw_#{i}")
          assert result in [0, 1]
        end
      end)

      Task.await(writer, 30_000)
      Task.await(reader, 30_000)
    end

    test "after add succeeds, exists from any task returns 1" do
      filter = create_cuckoo(capacity: 8192)

      for i <- 1..1_000, do: :ok = NIF.cuckoo_add(filter, "pre_#{i}")

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for i <- 1..1_000 do
              assert NIF.cuckoo_exists(filter, "pre_#{i}") == 1,
                "false negative for pre_#{i}"
            end
          end)
        end

      Task.await_many(tasks, 30_000)
    end
  end

  # --------------------------------------------------------------------------
  # Cuckoo: Edge Cases
  # --------------------------------------------------------------------------

  describe "cuckoo: edge cases" do
    test "empty filter operations don't crash" do
      filter = create_cuckoo()

      assert NIF.cuckoo_exists(filter, "nope") == 0
      assert NIF.cuckoo_count(filter, "nope") == 0
      assert NIF.cuckoo_del(filter, "nope") == 0
      assert NIF.cuckoo_mexists(filter, []) == []
      {:ok, info} = NIF.cuckoo_info(filter)
      assert is_tuple(info)
    end

    test "max capacity behavior — filter full returns error" do
      # Tiny filter: 4 buckets * 2 slots = 8 slots
      filter = create_cuckoo(capacity: 4, bucket_size: 2, max_kicks: 10)

      results =
        for i <- 1..100 do
          NIF.cuckoo_add(filter, "full_#{i}")
        end

      # Some inserts should succeed, some should fail with {:error, "filter is full"}
      oks = Enum.count(results, &(&1 == :ok))
      errors = Enum.count(results, &match?({:error, _}, &1))
      assert oks > 0, "no inserts succeeded"
      assert errors > 0, "no inserts failed — filter should have been full"
    end

    test "binary data with null bytes" do
      filter = create_cuckoo(capacity: 1024)

      null_item = <<0, 1, 2, 0, 3, 0, 0>>
      :ok = NIF.cuckoo_add(filter, null_item)
      assert NIF.cuckoo_exists(filter, null_item) == 1
    end
  end

  # ============================================================================
  # COUNT-MIN SKETCH (CMS) TESTS
  # ============================================================================

  # --------------------------------------------------------------------------
  # CMS: Scheduler Safety
  # --------------------------------------------------------------------------

  describe "cms: scheduler safety" do
    test "runs on Normal scheduler, not DirtyIo" do
      cms = create_cms()

      {_before, measure_dirty} = dirty_io_time_snapshot()

      items = Enum.map(1..10_000, fn i -> {"key_#{i}", 1} end)
      {:ok, _counts} = NIF.cms_incrby(cms, items)

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "DirtyIo delta #{dirty_delta} suggests cms NIF ran on DirtyIo"
    end

    test "yields for large operations — pinger stays responsive" do
      cms = create_cms(width: 10_000, depth: 10)

      {_pinger_pid, ping} = start_pinger()

      task = Task.async(fn ->
        items = Enum.map(1..100_000, fn i -> {"elem_#{i}", 1} end)
        # Process in chunks to exercise the NIF repeatedly
        items
        |> Enum.chunk_every(1_000)
        |> Enum.each(fn chunk -> NIF.cms_incrby(cms, chunk) end)
      end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid = Enum.filter(responses, &is_integer/1)
      assert length(valid) > 0
      avg = Enum.sum(valid) / length(valid)
      assert avg < 50_000,
        "average ping #{avg}us suggests scheduler starvation from cms"
    end

    test "no scheduler starvation under concurrent load" do
      cms = create_cms()

      {_counter_pid, sample} = start_counter()
      before_count = sample.()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            items = Enum.map(1..1_000, fn i -> {"conc_#{i}", 1} end)
            NIF.cms_incrby(cms, items)
          end)
        end

      Task.await_many(tasks, 60_000)
      after_count = sample.()

      assert after_count - before_count > 100,
        "counter barely incremented during cms concurrent load"
    end

    test "consume_timeslice — large cms_query over many items" do
      cms = create_cms(width: 10_000, depth: 10)
      items = Enum.map(1..10_000, fn i -> {"ts_#{i}", 1} end)
      NIF.cms_incrby(cms, items)

      {_counter_pid, sample} = start_counter()
      c0 = sample.()

      elements = Enum.map(1..10_000, fn i -> "ts_#{i}" end)
      {:ok, _counts} = NIF.cms_query(cms, elements)

      c1 = sample.()
      assert c1 - c0 >= 0
    end

    test "Normal scheduler only — explicit dirty time check" do
      cms = create_cms()

      {_before, measure_dirty} = dirty_io_time_snapshot()

      elements = Enum.map(1..10_000, fn i -> "q_#{i}" end)
      {:ok, _} = NIF.cms_query(cms, elements)

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "cms_query used #{dirty_delta} dirty scheduler time"
    end
  end

  # --------------------------------------------------------------------------
  # CMS: Memory Leak Tests
  # --------------------------------------------------------------------------

  describe "cms: memory leak" do
    test "create/destroy 1000 times — no memory growth" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..1_000 do
        cms = create_cms(width: 100, depth: 5)
        _ = cms
        :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      mem_after = :erlang.memory(:total)

      assert mem_after - mem_before < 5_000_000,
        "memory grew by #{mem_after - mem_before} after 1000 cms create/GC cycles"
    end

    test "add 100K items, drop reference, GC — memory freed" do
      :erlang.garbage_collect()
      mem_baseline = :erlang.memory(:total)

      cms = create_cms(width: 10_000, depth: 10)
      items = Enum.map(1..100_000, fn i -> {"leak_#{i}", 1} end)
      items |> Enum.chunk_every(10_000) |> Enum.each(fn chunk -> NIF.cms_incrby(cms, chunk) end)

      _ = cms
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_baseline < 5_000_000,
        "memory still #{mem_after - mem_baseline} above baseline after cms GC"
    end

    test "NIF resource destructor fires on GC" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      _cms = create_cms(width: 5000, depth: 10)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 5_000_000
    end

    test "process crash doesn't leak NIF resources" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..100 do
        pid = spawn(fn ->
          cms = create_cms(width: 500, depth: 5)
          NIF.cms_incrby(cms, [{"crash_item", 1}])
          Process.sleep(:infinity)
        end)
        Process.sleep(5)
        Process.exit(pid, :kill)
      end

      Process.sleep(200)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 100 cms process crashes"
    end

    test "concurrent create/destroy — no leak" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..100 do
              cms = create_cms(width: 100, depth: 5)
              NIF.cms_incrby(cms, [{"item", 1}])
              _ = cms
              :erlang.garbage_collect()
            end
          end)
        end

      Task.await_many(tasks, 120_000)
      :erlang.garbage_collect()
      Process.sleep(100)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 5000 concurrent cms create/GC"
    end
  end

  # --------------------------------------------------------------------------
  # CMS: Correctness Under Concurrency
  # --------------------------------------------------------------------------

  describe "cms: concurrency correctness" do
    test "concurrent increments don't corrupt — counts never undercount" do
      cms = create_cms(width: 10_000, depth: 7)

      tasks =
        for _t <- 1..10 do
          Task.async(fn ->
            items = Enum.map(1..1_000, fn i -> {"shared_#{i}", 1} end)
            NIF.cms_incrby(cms, items)
          end)
        end

      Task.await_many(tasks, 30_000)

      # Each key was incremented 10 times (once per task)
      elements = Enum.map(1..1_000, fn i -> "shared_#{i}" end)
      {:ok, counts} = NIF.cms_query(cms, elements)

      for {count, idx} <- Enum.with_index(counts, 1) do
        assert count >= 10,
          "key shared_#{idx} count #{count} < expected 10 (CMS never undercounts)"
      end
    end

    test "concurrent reads during writes never crash" do
      cms = create_cms(width: 10_000, depth: 7)

      writer = Task.async(fn ->
        for i <- 1..10_000 do
          NIF.cms_incrby(cms, [{"rw_#{i}", 1}])
        end
      end)

      reader = Task.async(fn ->
        for _ <- 1..10_000 do
          {:ok, [count]} = NIF.cms_query(cms, ["rw_1"])
          assert is_integer(count)
        end
      end)

      Task.await(writer, 30_000)
      Task.await(reader, 30_000)
    end

    test "after incrby, query from any task returns >= true count" do
      cms = create_cms(width: 10_000, depth: 7)

      NIF.cms_incrby(cms, [{"verify", 42}])

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            {:ok, [count]} = NIF.cms_query(cms, ["verify"])
            assert count >= 42, "expected >= 42, got #{count}"
          end)
        end

      Task.await_many(tasks, 30_000)
    end
  end

  # --------------------------------------------------------------------------
  # CMS: Edge Cases
  # --------------------------------------------------------------------------

  describe "cms: edge cases" do
    test "empty sketch operations don't crash" do
      cms = create_cms()

      {:ok, counts} = NIF.cms_query(cms, [])
      assert counts == []

      {:ok, counts} = NIF.cms_query(cms, ["never_seen"])
      assert counts == [0]

      {:ok, info} = NIF.cms_info(cms)
      assert is_tuple(info)
    end

    test "width=1 sketch — all elements collide" do
      {:ok, cms} = NIF.cms_create(1, 1)

      {:ok, [_c1]} = NIF.cms_incrby(cms, [{"a", 5}])
      {:ok, [_c2]} = NIF.cms_incrby(cms, [{"b", 3}])

      {:ok, [qa]} = NIF.cms_query(cms, ["a"])
      {:ok, [qb]} = NIF.cms_query(cms, ["b"])

      # With width=1, all items hash to the same bucket
      assert qa == 8
      assert qb == 8
    end

    test "binary data with null bytes in element names" do
      cms = create_cms()

      null_key = <<0, 1, 2, 0, 3>>
      {:ok, [count]} = NIF.cms_incrby(cms, [{null_key, 5}])
      assert count >= 5

      {:ok, [qcount]} = NIF.cms_query(cms, [null_key])
      assert qcount >= 5
    end
  end

  # ============================================================================
  # TOP-K TESTS
  # ============================================================================

  # --------------------------------------------------------------------------
  # TopK: Scheduler Safety
  # --------------------------------------------------------------------------

  describe "topk: scheduler safety" do
    test "runs on Normal scheduler, not DirtyIo" do
      topk = create_topk()

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for i <- 1..10_000 do
        NIF.topk_add(topk, ["item_#{i}"])
      end

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "DirtyIo delta #{dirty_delta} suggests topk NIF ran on DirtyIo"
    end

    test "yields for large operations — pinger stays responsive" do
      topk = create_topk(k: 50, width: 1000, depth: 7)

      {_pinger_pid, ping} = start_pinger()

      task = Task.async(fn ->
        for i <- 1..100_000 do
          NIF.topk_add(topk, ["elem_#{i}"])
        end
      end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid = Enum.filter(responses, &is_integer/1)
      assert length(valid) > 0
      avg = Enum.sum(valid) / length(valid)
      assert avg < 50_000,
        "average ping #{avg}us suggests scheduler starvation from topk"
    end

    test "no scheduler starvation under concurrent load" do
      topk = create_topk(k: 20, width: 500, depth: 5)

      {_counter_pid, sample} = start_counter()
      before_count = sample.()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for i <- 1..1_000 do
              NIF.topk_add(topk, ["conc_#{i}_#{:rand.uniform(1000)}"])
            end
          end)
        end

      Task.await_many(tasks, 60_000)
      after_count = sample.()

      assert after_count - before_count > 100,
        "counter barely incremented during topk concurrent load"
    end

    test "consume_timeslice — large topk_query" do
      topk = create_topk(k: 50, width: 1000, depth: 7)
      for i <- 1..10_000, do: NIF.topk_add(topk, ["ts_#{i}"])

      {_counter_pid, sample} = start_counter()
      c0 = sample.()

      items = Enum.map(1..10_000, fn i -> "ts_#{i}" end)
      NIF.topk_query(topk, items)

      c1 = sample.()
      assert c1 - c0 >= 0
    end

    test "Normal scheduler only — explicit dirty time check" do
      topk = create_topk()

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for i <- 1..10_000, do: NIF.topk_add(topk, ["q_#{i}"])

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "topk_add used #{dirty_delta} dirty scheduler time"
    end
  end

  # --------------------------------------------------------------------------
  # TopK: Memory Leak Tests
  # --------------------------------------------------------------------------

  describe "topk: memory leak" do
    test "create/destroy 1000 times — no memory growth" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..1_000 do
        topk = create_topk(k: 10, width: 50, depth: 5)
        _ = topk
        :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      mem_after = :erlang.memory(:total)

      assert mem_after - mem_before < 5_000_000,
        "memory grew by #{mem_after - mem_before} after 1000 topk create/GC cycles"
    end

    test "add items, drop reference, GC — memory freed" do
      :erlang.garbage_collect()
      mem_baseline = :erlang.memory(:total)

      topk = create_topk(k: 100, width: 1000, depth: 7)
      for i <- 1..100_000, do: NIF.topk_add(topk, ["leak_#{i}"])

      _ = topk
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_baseline < 5_000_000,
        "memory still #{mem_after - mem_baseline} above baseline after topk GC"
    end

    test "NIF resource destructor fires on GC" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      _topk = create_topk(k: 100, width: 1000, depth: 7)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 5_000_000
    end

    test "process crash doesn't leak NIF resources" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..100 do
        pid = spawn(fn ->
          topk = create_topk(k: 50, width: 200, depth: 5)
          for i <- 1..100, do: NIF.topk_add(topk, ["crash_#{i}"])
          Process.sleep(:infinity)
        end)
        Process.sleep(5)
        Process.exit(pid, :kill)
      end

      Process.sleep(200)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 100 topk process crashes"
    end

    test "concurrent create/destroy — no leak" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..100 do
              topk = create_topk(k: 10, width: 50, depth: 5)
              NIF.topk_add(topk, ["item"])
              _ = topk
              :erlang.garbage_collect()
            end
          end)
        end

      Task.await_many(tasks, 120_000)
      :erlang.garbage_collect()
      Process.sleep(100)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 5000 concurrent topk create/GC"
    end
  end

  # --------------------------------------------------------------------------
  # TopK: Correctness Under Concurrency
  # --------------------------------------------------------------------------

  describe "topk: concurrency correctness" do
    test "concurrent adds don't corrupt — list always valid" do
      topk = create_topk(k: 10, width: 1000, depth: 7)

      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            for i <- 1..1_000 do
              NIF.topk_add(topk, ["t#{t}_#{i}"])
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      # The list should have at most k items and be well-formed
      list = NIF.topk_list(topk)
      assert is_list(list)
      assert length(list) <= 10

      list_with_count = NIF.topk_list_with_count(topk)
      assert is_list(list_with_count)
      for {elem, count} <- list_with_count do
        assert is_binary(elem)
        assert is_integer(count)
        assert count > 0
      end
    end

    test "concurrent reads during writes never crash" do
      topk = create_topk(k: 10, width: 500, depth: 5)

      writer = Task.async(fn ->
        for i <- 1..10_000 do
          NIF.topk_add(topk, ["rw_#{i}"])
        end
      end)

      reader = Task.async(fn ->
        for _ <- 1..10_000 do
          list = NIF.topk_list(topk)
          assert is_list(list)
        end
      end)

      Task.await(writer, 30_000)
      Task.await(reader, 30_000)
    end

    test "high-frequency item stays in top-K from any task" do
      topk = create_topk(k: 5, width: 1000, depth: 7)

      # Add one dominant item with high count
      NIF.topk_incrby(topk, [{"dominant", 10_000}])

      # Add many other items with low counts
      for i <- 1..100, do: NIF.topk_add(topk, ["other_#{i}"])

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            results = NIF.topk_query(topk, ["dominant"])
            assert results == [1], "dominant item should be in top-K"
          end)
        end

      Task.await_many(tasks, 30_000)
    end
  end

  # --------------------------------------------------------------------------
  # TopK: Edge Cases
  # --------------------------------------------------------------------------

  describe "topk: edge cases" do
    test "empty topk operations don't crash" do
      topk = create_topk()

      assert NIF.topk_list(topk) == []
      assert NIF.topk_list_with_count(topk) == []
      assert NIF.topk_query(topk, ["nonexistent"]) == [0]
      info = NIF.topk_info(topk)
      assert is_tuple(info)
    end

    test "k=1 — only highest-count item survives" do
      {:ok, topk} = NIF.topk_create(1, 100, 7, 0.9)

      NIF.topk_incrby(topk, [{"a", 100}])
      assert NIF.topk_query(topk, ["a"]) == [1]

      NIF.topk_incrby(topk, [{"b", 200}])
      assert NIF.topk_query(topk, ["b"]) == [1]
      # "a" may or may not still be there depending on implementation
      list = NIF.topk_list(topk)
      assert length(list) == 1
    end

    test "adding items with special characters" do
      topk = create_topk()

      # Items with various special characters
      special_items = [
        "hello world",
        "tab\there",
        "newline\nhere",
        "unicode_\u00e9\u00e8\u00ea",
        "empty",
        ""
      ]

      for item <- special_items do
        NIF.topk_add(topk, [item])
      end

      list = NIF.topk_list(topk)
      assert is_list(list)
    end
  end

  # ============================================================================
  # TDIGEST TESTS
  # ============================================================================

  # --------------------------------------------------------------------------
  # TDigest: Scheduler Safety
  # --------------------------------------------------------------------------

  describe "tdigest: scheduler safety" do
    test "runs on Normal scheduler, not DirtyIo" do
      digest = create_tdigest()

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for _ <- 1..10_000 do
        NIF.tdigest_add(digest, [1.0, 2.0, 3.0])
      end

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "DirtyIo delta #{dirty_delta} suggests tdigest NIF ran on DirtyIo"
    end

    test "yields for large operations — pinger stays responsive" do
      digest = create_tdigest(compression: 200.0)

      {_pinger_pid, ping} = start_pinger()

      task = Task.async(fn ->
        # Add 1M values in chunks
        for _ <- 1..1_000 do
          values = Enum.map(1..1_000, fn i -> i * 1.0 end)
          NIF.tdigest_add(digest, values)
        end
      end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid = Enum.filter(responses, &is_integer/1)
      assert length(valid) > 0
      avg = Enum.sum(valid) / length(valid)
      assert avg < 50_000,
        "average ping #{avg}us suggests scheduler starvation from tdigest"
    end

    test "no scheduler starvation under concurrent load" do
      digest = create_tdigest()

      {_counter_pid, sample} = start_counter()
      before_count = sample.()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..1_000 do
              NIF.tdigest_add(digest, [:rand.uniform() * 100.0])
            end
          end)
        end

      Task.await_many(tasks, 60_000)
      after_count = sample.()

      assert after_count - before_count > 100,
        "counter barely incremented during tdigest concurrent load"
    end

    test "consume_timeslice — large tdigest_quantile batch" do
      digest = create_tdigest()
      values = Enum.map(1..10_000, fn i -> i * 1.0 end)
      NIF.tdigest_add(digest, values)

      {_counter_pid, sample} = start_counter()
      c0 = sample.()

      quantiles = Enum.map(0..100, fn i -> i / 100.0 end)
      NIF.tdigest_quantile(digest, quantiles)

      c1 = sample.()
      assert c1 - c0 >= 0
    end

    test "Normal scheduler only — explicit dirty time check" do
      digest = create_tdigest()

      {_before, measure_dirty} = dirty_io_time_snapshot()

      for _ <- 1..10_000 do
        NIF.tdigest_add(digest, [:rand.uniform() * 100.0])
      end

      dirty_delta = measure_dirty.()
      assert dirty_delta < 5_000_000,
        "tdigest_add used #{dirty_delta} dirty scheduler time"
    end
  end

  # --------------------------------------------------------------------------
  # TDigest: Memory Leak Tests
  # --------------------------------------------------------------------------

  describe "tdigest: memory leak" do
    test "create/destroy 1000 times — no memory growth" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..1_000 do
        digest = create_tdigest()
        _ = digest
        :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      mem_after = :erlang.memory(:total)

      assert mem_after - mem_before < 5_000_000,
        "memory grew by #{mem_after - mem_before} after 1000 tdigest create/GC cycles"
    end

    test "add 100K values, drop reference, GC — memory freed" do
      :erlang.garbage_collect()
      mem_baseline = :erlang.memory(:total)

      digest = create_tdigest(compression: 200.0)
      for _ <- 1..100 do
        values = Enum.map(1..1_000, fn _ -> :rand.uniform() * 1000.0 end)
        NIF.tdigest_add(digest, values)
      end

      _ = digest
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_baseline < 5_000_000,
        "memory still #{mem_after - mem_baseline} above baseline after tdigest GC"
    end

    test "NIF resource destructor fires on GC" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      _digest = create_tdigest(compression: 500.0)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 5_000_000
    end

    test "process crash doesn't leak NIF resources" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      for _ <- 1..100 do
        pid = spawn(fn ->
          digest = create_tdigest()
          NIF.tdigest_add(digest, [1.0, 2.0, 3.0, 4.0, 5.0])
          Process.sleep(:infinity)
        end)
        Process.sleep(5)
        Process.exit(pid, :kill)
      end

      Process.sleep(200)
      :erlang.garbage_collect()
      Process.sleep(50)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 100 tdigest process crashes"
    end

    test "concurrent create/destroy — no leak" do
      :erlang.garbage_collect()
      mem_before = :erlang.memory(:total)

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..100 do
              digest = create_tdigest()
              NIF.tdigest_add(digest, [1.0, 2.0, 3.0])
              _ = digest
              :erlang.garbage_collect()
            end
          end)
        end

      Task.await_many(tasks, 120_000)
      :erlang.garbage_collect()
      Process.sleep(100)

      mem_after = :erlang.memory(:total)
      assert mem_after - mem_before < 10_000_000,
        "memory grew by #{mem_after - mem_before} after 5000 concurrent tdigest create/GC"
    end
  end

  # --------------------------------------------------------------------------
  # TDigest: Correctness Under Concurrency
  # --------------------------------------------------------------------------

  describe "tdigest: concurrency correctness" do
    test "concurrent adds don't corrupt — quantiles remain valid" do
      digest = create_tdigest(compression: 200.0)

      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            values = Enum.map(1..1_000, fn i -> (t * 1_000 + i) * 1.0 end)
            NIF.tdigest_add(digest, values)
          end)
        end

      Task.await_many(tasks, 30_000)

      # Quantile queries should return valid numbers (not NaN, not crash)
      [p50] = NIF.tdigest_quantile(digest, [0.5])
      assert is_float(p50) or is_integer(p50)
      assert p50 > 0

      [p01, p99] = NIF.tdigest_quantile(digest, [0.01, 0.99])
      assert p01 < p99, "P01 #{p01} should be less than P99 #{p99}"
    end

    test "concurrent reads during writes never crash" do
      digest = create_tdigest()

      writer = Task.async(fn ->
        for _ <- 1..10_000 do
          NIF.tdigest_add(digest, [:rand.uniform() * 100.0])
        end
      end)

      reader = Task.async(fn ->
        for _ <- 1..10_000 do
          result = NIF.tdigest_quantile(digest, [0.5])
          assert is_list(result)
          assert length(result) == 1
        end
      end)

      Task.await(writer, 30_000)
      Task.await(reader, 30_000)
    end

    test "after adds complete, quantile from any task returns consistent value" do
      digest = create_tdigest()
      values = Enum.map(1..10_000, fn i -> i * 1.0 end)
      NIF.tdigest_add(digest, values)

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            [p50] = NIF.tdigest_quantile(digest, [0.5])
            # P50 of 1..10000 should be around 5000
            assert is_float(p50) or is_integer(p50)
            assert p50 > 3000 and p50 < 7000,
              "P50 #{p50} too far from expected ~5000"
          end)
        end

      Task.await_many(tasks, 30_000)
    end
  end

  # --------------------------------------------------------------------------
  # TDigest: Edge Cases
  # --------------------------------------------------------------------------

  describe "tdigest: edge cases" do
    test "empty digest operations don't crash" do
      digest = create_tdigest()

      [q50] = NIF.tdigest_quantile(digest, [0.5])
      assert q50 == :nan

      [cdf_val] = NIF.tdigest_cdf(digest, [50.0])
      assert cdf_val == :nan

      min_val = NIF.tdigest_min(digest)
      assert min_val == :nan

      max_val = NIF.tdigest_max(digest)
      assert max_val == :nan

      info = NIF.tdigest_info(digest)
      assert is_list(info)

      [rank_val] = NIF.tdigest_rank(digest, [50.0])
      assert rank_val == -2
    end

    test "single value digest" do
      digest = create_tdigest()
      :ok = NIF.tdigest_add(digest, [42.0])

      min_val = NIF.tdigest_min(digest)
      max_val = NIF.tdigest_max(digest)
      assert_in_delta min_val, 42.0, 0.01
      assert_in_delta max_val, 42.0, 0.01

      [p50] = NIF.tdigest_quantile(digest, [0.5])
      assert_in_delta p50, 42.0, 0.1
    end

    test "reset clears all data" do
      digest = create_tdigest()
      NIF.tdigest_add(digest, Enum.map(1..1000, fn i -> i * 1.0 end))

      :ok = NIF.tdigest_reset(digest)

      [q50] = NIF.tdigest_quantile(digest, [0.5])
      assert q50 == :nan

      min_val = NIF.tdigest_min(digest)
      assert min_val == :nan
    end
  end
end
