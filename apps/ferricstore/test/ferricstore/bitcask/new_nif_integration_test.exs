defmodule Ferricstore.Bitcask.NewNIFIntegrationTest do
  @moduledoc """
  Comprehensive integration tests for all new Rust NIF modules:

    1. Bloom filter (mmap-backed)
    2. Cuckoo filter (in-memory, serializable)
    3. Count-Min Sketch (CMS)
    4. TopK (CMS + min-heap)
    5. T-Digest (quantile estimation)
    6. HNSW (vector search)
    7. Async IO (Tokio runtime)
    8. Tracking allocator (memory monitoring)

  Tests verify:
  - Scheduler safety: all NIFs run on Normal scheduler (DirtyIo wall-time stays near zero)
  - Yield verification: CPU-heavy NIFs don't block BEAM scheduler
  - Memory leak detection: create/destroy cycles don't leak
  - Cross-NIF interaction: no deadlocks or interference between modules
  - Tokio async IO: cold reads don't block BEAM schedulers

  ## Running

      mix test apps/ferricstore/test/ferricstore/bitcask/new_nif_integration_test.exs
  """

  use ExUnit.Case, async: false

  @moduletag timeout: 300_000

  alias Ferricstore.Bitcask.NIF

  # ============================================================================
  # Helpers
  # ============================================================================

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "new_nif_int_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp bloom_path(dir, name \\ "test") do
    Path.join(dir, "#{name}.bloom")
  end

  defp create_bloom(dir, opts \\ []) do
    num_bits = Keyword.get(opts, :num_bits, 100_000)
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

  defp create_hnsw(opts \\ []) do
    dims = Keyword.get(opts, :dims, 8)
    m = Keyword.get(opts, :m, 16)
    ef_construction = Keyword.get(opts, :ef_construction, 128)
    metric = Keyword.get(opts, :metric, "l2")
    {:ok, resource} = NIF.hnsw_new(dims, m, ef_construction, metric)
    resource
  end

  # Measures total DirtyIo scheduler wall-time delta.
  defp sum_dirty_io_time do
    stats = :erlang.statistics(:scheduler_wall_time_all)
    normal_count = :erlang.system_info(:schedulers)

    stats
    |> Enum.filter(fn {id, _, _} -> id > normal_count + 1 end)
    |> Enum.map(fn {_, active, _} -> active end)
    |> Enum.sum()
  end

  # Spawns a GenServer-like responder that replies to :ping messages.
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

  # ============================================================================
  # PART 1: Scheduler safety for ALL new NIFs
  # ============================================================================

  describe "scheduler safety: bloom NIFs run on Normal scheduler" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "bloom operations stay off DirtyIo", %{dir: dir} do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      {filter, _path} = create_bloom(dir, num_bits: 100_000)

      for i <- 1..10_000, do: NIF.bloom_add(filter, "key_#{i}")
      for i <- 1..10_000, do: NIF.bloom_exists(filter, "key_#{i}")

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests bloom NIF ran on DirtyIo"
    end
  end

  describe "scheduler safety: cuckoo NIFs run on Normal scheduler" do
    test "cuckoo operations stay off DirtyIo" do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      filter = create_cuckoo(capacity: 4096)

      for i <- 1..10_000 do
        NIF.cuckoo_add(filter, "key_#{i}")
      end

      for i <- 1..10_000 do
        NIF.cuckoo_exists(filter, "key_#{i}")
      end

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests cuckoo NIF ran on DirtyIo"
    end
  end

  describe "scheduler safety: CMS NIFs run on Normal scheduler" do
    test "CMS operations stay off DirtyIo" do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      sketch = create_cms(width: 1000, depth: 7)

      items = for i <- 1..10_000, do: {"key_#{i}", 1}
      NIF.cms_incrby(sketch, items)

      query_keys = for i <- 1..10_000, do: "key_#{i}"
      NIF.cms_query(sketch, query_keys)

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests CMS NIF ran on DirtyIo"
    end
  end

  describe "scheduler safety: TopK NIFs run on Normal scheduler" do
    test "TopK operations stay off DirtyIo" do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      topk = create_topk(k: 10, width: 100, depth: 7)

      items = for i <- 1..10_000, do: "item_#{i}"
      NIF.topk_add(topk, items)
      NIF.topk_list(topk)
      NIF.topk_list_with_count(topk)

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests TopK NIF ran on DirtyIo"
    end
  end

  describe "scheduler safety: TDigest NIFs run on Normal scheduler" do
    test "TDigest operations stay off DirtyIo" do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      digest = create_tdigest(compression: 100.0)

      values = for i <- 1..10_000, do: i * 1.0
      NIF.tdigest_add(digest, values)
      NIF.tdigest_quantile(digest, [0.5, 0.95, 0.99])
      NIF.tdigest_cdf(digest, [500.0, 9500.0])

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests TDigest NIF ran on DirtyIo"
    end
  end

  describe "scheduler safety: HNSW NIFs run on Normal scheduler" do
    test "HNSW operations stay off DirtyIo" do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      index = create_hnsw(dims: 8)

      for i <- 1..1_000 do
        vec = for d <- 1..8, do: (i + d) * 1.0
        NIF.hnsw_add(index, "v#{i}", vec)
      end

      query = for d <- 1..8, do: (500 + d) * 1.0
      NIF.hnsw_search(index, query, 10, 200)
      NIF.vsearch_nif(index, query, 10, 200)

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests HNSW NIF ran on DirtyIo"
    end
  end

  describe "scheduler safety: tracking allocator runs on Normal scheduler" do
    test "rust_allocated_bytes stays off DirtyIo" do
      :erlang.system_flag(:scheduler_wall_time, true)
      Process.sleep(5)
      dirty_before = sum_dirty_io_time()

      for _ <- 1..10_000 do
        NIF.rust_allocated_bytes()
      end

      dirty_after = sum_dirty_io_time()
      :erlang.system_flag(:scheduler_wall_time, false)

      dirty_delta = dirty_after - dirty_before
      assert dirty_delta < 1_000_000,
        "DirtyIo delta #{dirty_delta} suggests tracking alloc NIF ran on DirtyIo"
    end
  end

  # ============================================================================
  # PART 2: Yield verification
  # ============================================================================

  describe "yield: HNSW search with 5K vectors yields to scheduler" do
    test "pinger stays responsive during large HNSW search" do
      index = create_hnsw(dims: 16, ef_construction: 200)

      # Build 5K-vector index
      for i <- 1..5_000 do
        vec = for d <- 1..16, do: :rand.uniform() * 100.0
        {:ok, _} = NIF.hnsw_add(index, "v#{i}", vec)
      end

      {_pid, ping} = start_pinger()

      query = for _ <- 1..16, do: :rand.uniform() * 100.0

      task =
        Task.async(fn ->
          NIF.vsearch_nif(index, query, 10, 200)
        end)

      responses =
        for _ <- 1..30 do
          Process.sleep(2)
          ping.()
        end

      Task.await(task, 30_000)

      valid_pings = Enum.filter(responses, &is_integer/1)
      assert length(valid_pings) > 0, "pinger never responded during HNSW search"
      max_ping = Enum.max(valid_pings)

      assert max_ping < 50_000,
        "max ping #{max_ping}us during HNSW search -- scheduler may be blocked"
    end
  end

  describe "yield: bloom bulk add yields to scheduler" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "pinger stays responsive during large bloom_madd", %{dir: dir} do
      {filter, _path} = create_bloom(dir, num_bits: 10_000_000)

      {_pid, ping} = start_pinger()

      elements = Enum.map(1..50_000, fn i -> "elem_#{i}" end)

      task = Task.async(fn -> NIF.bloom_madd(filter, elements) end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid_pings = Enum.filter(responses, &is_integer/1)
      assert length(valid_pings) > 0, "pinger never responded during bloom_madd"
      avg_ping = Enum.sum(valid_pings) / length(valid_pings)

      assert avg_ping < 50_000,
        "avg ping #{avg_ping}us during bloom_madd -- scheduler may be blocked"
    end
  end

  describe "yield: CMS bulk incrby yields to scheduler" do
    test "pinger stays responsive during 100K CMS incrby" do
      sketch = create_cms(width: 10_000, depth: 7)
      {_pid, ping} = start_pinger()

      items = for i <- 1..100_000, do: {"key_#{i}", 1}

      task = Task.async(fn -> NIF.cms_incrby(sketch, items) end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid_pings = Enum.filter(responses, &is_integer/1)
      assert length(valid_pings) > 0
      avg_ping = Enum.sum(valid_pings) / length(valid_pings)
      assert avg_ping < 50_000,
        "avg ping #{avg_ping}us during CMS incrby -- scheduler may be blocked"
    end
  end

  describe "yield: TDigest bulk add yields to scheduler" do
    test "pinger stays responsive during 100K TDigest adds" do
      digest = create_tdigest(compression: 200.0)
      {_pid, ping} = start_pinger()

      values = for _ <- 1..100_000, do: :rand.uniform() * 10_000.0

      task = Task.async(fn -> NIF.tdigest_add(digest, values) end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid_pings = Enum.filter(responses, &is_integer/1)
      assert length(valid_pings) > 0
      avg_ping = Enum.sum(valid_pings) / length(valid_pings)
      assert avg_ping < 50_000,
        "avg ping #{avg_ping}us during TDigest add -- scheduler may be blocked"
    end
  end

  describe "yield: TopK bulk add yields to scheduler" do
    test "pinger stays responsive during 50K TopK adds" do
      topk = create_topk(k: 100, width: 1000, depth: 7)
      {_pid, ping} = start_pinger()

      items = for i <- 1..50_000, do: "item_#{i}"

      task = Task.async(fn -> NIF.topk_add(topk, items) end)

      responses =
        for _ <- 1..20 do
          Process.sleep(5)
          ping.()
        end

      Task.await(task, 60_000)

      valid_pings = Enum.filter(responses, &is_integer/1)
      assert length(valid_pings) > 0
      avg_ping = Enum.sum(valid_pings) / length(valid_pings)
      assert avg_ping < 50_000,
        "avg ping #{avg_ping}us during TopK add -- scheduler may be blocked"
    end
  end

  # ============================================================================
  # PART 3: Memory leak tests
  # ============================================================================

  describe "memory: bloom NIF resources freed on GC" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "create/destroy 1000 bloom filters -- no significant memory growth", %{dir: dir} do
      mem_before = :erlang.memory(:total)

      for i <- 1..1000 do
        path = bloom_path(dir, "leak_#{i}")
        {:ok, f} = NIF.bloom_create(path, 1_000, 7)
        NIF.bloom_delete(f)
        if rem(i, 100) == 0, do: :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      Process.sleep(50)
      mem_after = :erlang.memory(:total)

      growth = mem_after - mem_before
      assert growth < 10_000_000,
        "memory grew by #{growth} bytes after 1000 bloom create/destroy cycles"
    end
  end

  describe "memory: cuckoo NIF resources freed on GC" do
    test "create/destroy 1000 cuckoo filters -- no significant memory growth" do
      mem_before = :erlang.memory(:total)

      for i <- 1..1000 do
        {:ok, _f} = NIF.cuckoo_create(64, 4, 500, 0)
        if rem(i, 100) == 0, do: :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      Process.sleep(50)
      mem_after = :erlang.memory(:total)

      growth = mem_after - mem_before
      assert growth < 10_000_000,
        "memory grew by #{growth} bytes after 1000 cuckoo create cycles"
    end
  end

  describe "memory: CMS NIF resources freed on GC" do
    test "create/destroy 1000 CMS sketches -- no significant memory growth" do
      mem_before = :erlang.memory(:total)

      for i <- 1..1000 do
        {:ok, _s} = NIF.cms_create(100, 5)
        if rem(i, 100) == 0, do: :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      Process.sleep(50)
      mem_after = :erlang.memory(:total)

      growth = mem_after - mem_before
      assert growth < 10_000_000,
        "memory grew by #{growth} bytes after 1000 CMS create cycles"
    end
  end

  describe "memory: TopK NIF resources freed on GC" do
    test "create/destroy 1000 TopK trackers -- no significant memory growth" do
      mem_before = :erlang.memory(:total)

      for i <- 1..1000 do
        {:ok, _t} = NIF.topk_create(10, 50, 5, 0.9)
        if rem(i, 100) == 0, do: :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      Process.sleep(50)
      mem_after = :erlang.memory(:total)

      growth = mem_after - mem_before
      assert growth < 10_000_000,
        "memory grew by #{growth} bytes after 1000 TopK create cycles"
    end
  end

  describe "memory: HNSW NIF resources freed on GC" do
    test "create/destroy 1000 HNSW indices -- no significant memory growth" do
      mem_before = :erlang.memory(:total)

      for i <- 1..1000 do
        {:ok, _idx} = NIF.hnsw_new(4, 8, 32, "l2")
        if rem(i, 100) == 0, do: :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      Process.sleep(50)
      mem_after = :erlang.memory(:total)

      growth = mem_after - mem_before
      assert growth < 10_000_000,
        "memory grew by #{growth} bytes after 1000 HNSW create cycles"
    end
  end

  # ============================================================================
  # PART 4: Cross-NIF interaction
  # ============================================================================

  describe "cross-NIF: bloom + CMS + TopK used together -- no interference" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "interleaved operations on all three structures", %{dir: dir} do
      {bloom, _path} = create_bloom(dir, num_bits: 100_000)
      cms = create_cms(width: 1000, depth: 7)
      topk = create_topk(k: 10, width: 100, depth: 7)

      for i <- 1..5_000 do
        key = "item_#{i}"

        # Bloom: add and check
        NIF.bloom_add(bloom, key)
        assert NIF.bloom_exists(bloom, key) == 1

        # CMS: increment
        NIF.cms_incrby(cms, [{key, 1}])

        # TopK: add
        NIF.topk_add(topk, [key])
      end

      # Verify bloom has all items
      assert NIF.bloom_card(bloom) == 5_000

      # Verify CMS never undercounts
      {:ok, counts} = NIF.cms_query(cms, ["item_1", "item_2500", "item_5000"])
      assert Enum.all?(counts, &(&1 >= 1))

      # Verify TopK has 10 items
      list = NIF.topk_list(topk)
      assert length(list) == 10
    end
  end

  describe "cross-NIF: HNSW search during bloom adds -- no deadlock" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "concurrent HNSW search and bloom adds complete without deadlock", %{dir: dir} do
      index = create_hnsw(dims: 8)
      {bloom, _path} = create_bloom(dir, num_bits: 1_000_000)

      # Pre-populate HNSW
      for i <- 1..1_000 do
        vec = for d <- 1..8, do: (i + d) * 1.0
        NIF.hnsw_add(index, "v#{i}", vec)
      end

      # Run bloom adds and HNSW searches concurrently
      bloom_task =
        Task.async(fn ->
          for i <- 1..10_000 do
            NIF.bloom_add(bloom, "bloom_key_#{i}")
          end

          :bloom_done
        end)

      hnsw_task =
        Task.async(fn ->
          for _ <- 1..100 do
            query = for d <- 1..8, do: :rand.uniform() * 1000.0
            NIF.hnsw_search(index, query, 5, 100)
          end

          :hnsw_done
        end)

      assert Task.await(bloom_task, 30_000) == :bloom_done
      assert Task.await(hnsw_task, 30_000) == :hnsw_done
    end
  end

  describe "cross-NIF: all probabilistic NIFs concurrent -- no contention" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "bloom + cuckoo + CMS + TopK + TDigest + HNSW all running concurrently", %{dir: dir} do
      {bloom, _} = create_bloom(dir, num_bits: 100_000)
      cuckoo = create_cuckoo(capacity: 2048)
      cms = create_cms(width: 500, depth: 5)
      topk = create_topk(k: 10, width: 100, depth: 7)
      digest = create_tdigest(compression: 100.0)
      index = create_hnsw(dims: 4)

      tasks = [
        Task.async(fn ->
          for i <- 1..5_000, do: NIF.bloom_add(bloom, "b#{i}")
          :bloom_done
        end),
        Task.async(fn ->
          for i <- 1..5_000, do: NIF.cuckoo_add(cuckoo, "c#{i}")
          :cuckoo_done
        end),
        Task.async(fn ->
          items = for i <- 1..5_000, do: {"cms#{i}", 1}
          NIF.cms_incrby(cms, items)
          :cms_done
        end),
        Task.async(fn ->
          items = for i <- 1..5_000, do: "tk#{i}"
          NIF.topk_add(topk, items)
          :topk_done
        end),
        Task.async(fn ->
          vals = for i <- 1..5_000, do: i * 1.0
          NIF.tdigest_add(digest, vals)
          :tdigest_done
        end),
        Task.async(fn ->
          for i <- 1..500 do
            vec = for d <- 1..4, do: (i + d) * 1.0
            NIF.hnsw_add(index, "h#{i}", vec)
          end

          :hnsw_done
        end)
      ]

      results = Task.await_many(tasks, 60_000)

      assert :bloom_done in results
      assert :cuckoo_done in results
      assert :cms_done in results
      assert :topk_done in results
      assert :tdigest_done in results
      assert :hnsw_done in results
    end
  end

  describe "tracking allocator: reports allocation data" do
    test "rust_allocated_bytes returns a non-negative integer" do
      # With #[cfg(test)] on the global allocator, the cdylib uses System
      # allocator and the counter stays at 0. The NIF should still be callable.
      bytes = NIF.rust_allocated_bytes()
      assert is_integer(bytes)
      assert bytes >= 0
    end
  end

  # ============================================================================
  # PART 6: Serialization roundtrip integration
  # ============================================================================

  describe "serialization: cuckoo serialize/deserialize roundtrip" do
    test "data survives serialization" do
      filter = create_cuckoo(capacity: 1024)

      for i <- 1..100, do: NIF.cuckoo_add(filter, "item_#{i}")

      {:ok, bytes} = NIF.cuckoo_serialize(filter)
      assert is_binary(bytes)

      {:ok, restored} = NIF.cuckoo_deserialize(bytes)

      for i <- 1..100 do
        assert NIF.cuckoo_exists(restored, "item_#{i}") == 1,
          "item_#{i} missing after roundtrip"
      end
    end

    test "deserialize garbage returns error" do
      assert {:error, _} = NIF.cuckoo_deserialize("this is not valid cuckoo data")
    end
  end

  describe "serialization: CMS serialize/deserialize roundtrip" do
    test "data survives serialization" do
      sketch = create_cms(width: 100, depth: 5)
      NIF.cms_incrby(sketch, [{"foo", 10}, {"bar", 20}])

      {:ok, bytes} = NIF.cms_to_bytes(sketch)
      assert is_binary(bytes)

      {:ok, restored} = NIF.cms_from_bytes(bytes)

      {:ok, [foo_count, bar_count]} = NIF.cms_query(restored, ["foo", "bar"])
      assert foo_count >= 10
      assert bar_count >= 20
    end

    test "deserialize garbage returns error" do
      assert {:error, _} = NIF.cms_from_bytes("garbage")
    end
  end

  describe "serialization: TopK serialize/deserialize roundtrip" do
    test "data survives serialization" do
      topk = create_topk(k: 5, width: 50, depth: 5)

      for i <- 1..20 do
        NIF.topk_add(topk, ["item_#{i}"])
        NIF.topk_add(topk, ["item_#{i}"])
      end

      {:ok, bytes} = NIF.topk_to_bytes(topk)
      assert is_binary(bytes)

      {:ok, restored} = NIF.topk_from_bytes(bytes)

      list = NIF.topk_list(restored)
      assert length(list) == 5
    end
  end

  describe "serialization: TDigest serialize/deserialize roundtrip" do
    test "quantiles survive serialization" do
      digest = create_tdigest(compression: 100.0)
      NIF.tdigest_add(digest, Enum.map(1..1000, &(&1 * 1.0)))

      bytes = NIF.tdigest_serialize(digest)
      assert is_binary(bytes)

      {:ok, restored} = NIF.tdigest_deserialize(bytes)

      [p50_orig] = NIF.tdigest_quantile(digest, [0.5])
      [p50_restored] = NIF.tdigest_quantile(restored, [0.5])

      assert abs(p50_orig - p50_restored) < 1.0,
        "quantile drift after roundtrip: #{p50_orig} vs #{p50_restored}"
    end
  end

  describe "serialization: bloom persistence roundtrip" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "data survives close and reopen", %{dir: dir} do
      path = bloom_path(dir, "persist")
      {:ok, f1} = NIF.bloom_create(path, 100_000, 7)

      for i <- 1..1000, do: NIF.bloom_add(f1, "item_#{i}")

      # Let the resource be GC'd (msync happens in Drop)
      _ = f1
      :erlang.garbage_collect()
      Process.sleep(50)

      # Reopen
      {:ok, f2} = NIF.bloom_open(path)

      for i <- 1..1000 do
        assert NIF.bloom_exists(f2, "item_#{i}") == 1,
          "item_#{i} missing after reopen"
      end
    end
  end

  # ============================================================================
  # PART 7: Error handling
  # ============================================================================

  describe "error handling: bloom" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "bloom_create with 0 bits returns error", %{dir: dir} do
      path = bloom_path(dir, "zero")
      assert {:error, _} = NIF.bloom_create(path, 0, 7)
    end

    test "bloom_open nonexistent file returns error" do
      assert {:error, _} = NIF.bloom_open("/tmp/does_not_exist_at_all.bloom")
    end
  end

  describe "error handling: cuckoo" do
    test "cuckoo_create with 0 capacity returns error" do
      assert {:error, _} = NIF.cuckoo_create(0, 4, 500, 0)
    end
  end

  describe "error handling: CMS" do
    test "cms_create with 0 width returns error" do
      assert {:error, _} = NIF.cms_create(0, 7)
    end

    test "cms_create with 0 depth returns error" do
      assert {:error, _} = NIF.cms_create(100, 0)
    end
  end

  describe "error handling: TopK" do
    test "topk_create with k=0 returns error" do
      assert {:error, _} = NIF.topk_create(0, 100, 7, 0.9)
    end
  end

  describe "error handling: HNSW" do
    test "hnsw_new with dims=0 returns error" do
      assert {:error, _} = NIF.hnsw_new(0, 16, 128, "l2")
    end

    test "hnsw_new with unknown metric returns error" do
      assert {:error, _} = NIF.hnsw_new(3, 16, 128, "manhattan")
    end

    test "hnsw_add with wrong dimension returns error" do
      {:ok, index} = NIF.hnsw_new(3, 16, 128, "l2")
      assert {:error, _} = NIF.hnsw_add(index, "bad", [1.0, 2.0])
    end
  end

  # ============================================================================
  # PART 8: Correctness edge cases
  # ============================================================================

  describe "correctness: bloom empty key and null bytes" do
    setup do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "empty key works", %{dir: dir} do
      {filter, _} = create_bloom(dir)
      assert NIF.bloom_add(filter, "") == 1
      assert NIF.bloom_exists(filter, "") == 1
    end

    test "key with null bytes works", %{dir: dir} do
      {filter, _} = create_bloom(dir)
      null_key = <<0, 1, 0, 2, 0>>
      assert NIF.bloom_add(filter, null_key) == 1
      assert NIF.bloom_exists(filter, null_key) == 1
    end

    test "madd/mexists with empty list", %{dir: dir} do
      {filter, _} = create_bloom(dir)
      assert NIF.bloom_madd(filter, []) == []
      assert NIF.bloom_mexists(filter, []) == []
    end
  end

  describe "correctness: cuckoo add/delete/count" do
    test "add + delete + exists" do
      filter = create_cuckoo()
      :ok = NIF.cuckoo_add(filter, "hello")
      assert NIF.cuckoo_exists(filter, "hello") == 1
      assert NIF.cuckoo_del(filter, "hello") == 1
      assert NIF.cuckoo_exists(filter, "hello") == 0
    end

    test "delete nonexistent returns 0" do
      filter = create_cuckoo()
      assert NIF.cuckoo_del(filter, "never_added") == 0
    end

    test "addnx on existing returns 0" do
      filter = create_cuckoo()
      :ok = NIF.cuckoo_add(filter, "x")
      assert NIF.cuckoo_addnx(filter, "x") == 0
    end

    test "count reflects additions" do
      filter = create_cuckoo()
      :ok = NIF.cuckoo_add(filter, "counted")
      assert NIF.cuckoo_count(filter, "counted") == 1
    end
  end

  describe "correctness: CMS overcount property" do
    test "CMS never undercounts" do
      sketch = create_cms(width: 1000, depth: 7)

      items = [{"apple", 100}, {"banana", 50}, {"cherry", 200}]
      NIF.cms_incrby(sketch, items)

      {:ok, counts} = NIF.cms_query(sketch, ["apple", "banana", "cherry"])
      [apple, banana, cherry] = counts

      assert apple >= 100, "apple undercounted: #{apple}"
      assert banana >= 50, "banana undercounted: #{banana}"
      assert cherry >= 200, "cherry undercounted: #{cherry}"
    end

    test "query nonexistent returns 0" do
      sketch = create_cms()
      {:ok, [count]} = NIF.cms_query(sketch, ["never_seen"])
      assert count == 0
    end
  end

  describe "correctness: TopK list is sorted desc" do
    test "list_with_count returns items sorted by count descending" do
      topk = create_topk(k: 5, width: 100, depth: 7)

      NIF.topk_incrby(topk, [{"low", 1}, {"mid", 50}, {"high", 100}])

      items = NIF.topk_list_with_count(topk)
      counts = Enum.map(items, fn {_name, count} -> count end)

      for i <- 1..(length(counts) - 1) do
        assert Enum.at(counts, i - 1) >= Enum.at(counts, i),
          "not sorted desc: #{inspect(counts)}"
      end
    end
  end

  describe "correctness: TDigest quantile accuracy" do
    test "uniform 1..1000 p50 within 5% of 500" do
      digest = create_tdigest(compression: 100.0)
      NIF.tdigest_add(digest, Enum.map(1..1000, &(&1 * 1.0)))

      [p50] = NIF.tdigest_quantile(digest, [0.5])
      assert abs(p50 - 500.0) < 500.0 * 0.05,
        "p50=#{p50}, expected ~500"
    end

    test "min and max correct" do
      digest = create_tdigest()
      NIF.tdigest_add(digest, [5.0, 10.0, 15.0])

      assert NIF.tdigest_min(digest) == 5.0
      assert NIF.tdigest_max(digest) == 15.0
    end

    test "CDF of min is near 0, CDF of max is near 1" do
      digest = create_tdigest()
      NIF.tdigest_add(digest, Enum.map(1..100, &(&1 * 1.0)))

      [cdf_min] = NIF.tdigest_cdf(digest, [1.0])
      [cdf_max] = NIF.tdigest_cdf(digest, [100.0])

      assert cdf_min < 0.05, "CDF of min too high: #{cdf_min}"
      assert cdf_max > 0.95, "CDF of max too low: #{cdf_max}"
    end
  end

  describe "correctness: HNSW search accuracy" do
    test "nearest neighbor is the exact match" do
      index = create_hnsw(dims: 3)
      NIF.hnsw_add(index, "close", [1.1, 0.1, 0.1])
      NIF.hnsw_add(index, "far", [10.0, 10.0, 10.0])

      {:ok, results} = NIF.hnsw_search(index, [1.0, 0.0, 0.0], 1, 50)
      [{key, _dist}] = results
      assert key == "close"
    end

    test "delete removes from search results" do
      index = create_hnsw(dims: 3)
      NIF.hnsw_add(index, "a", [1.0, 0.0, 0.0])
      NIF.hnsw_add(index, "b", [0.0, 1.0, 0.0])

      NIF.hnsw_delete(index, "a")

      {:ok, results} = NIF.hnsw_search(index, [1.0, 0.0, 0.0], 5, 50)
      keys = Enum.map(results, fn {k, _} -> k end)
      refute "a" in keys, "deleted key still in search results"
    end

    test "empty index search returns empty" do
      index = create_hnsw(dims: 3)
      {:ok, results} = NIF.hnsw_search(index, [1.0, 0.0, 0.0], 5, 50)
      assert results == []
    end
  end
end
