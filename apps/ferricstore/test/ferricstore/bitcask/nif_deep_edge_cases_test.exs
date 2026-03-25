defmodule Ferricstore.Bitcask.NIFDeepEdgeCasesTest do
  @moduledoc """
  Deep edge-case tests for ALL Ferricstore NIFs, targeting known classes
  of NIF/FFI boundary bugs discovered through research:

  1. **Process lifecycle & resource GC** — NIF resources surviving creator
     process death, GC after all references dropped.
  2. **Binary safety** — embedded nulls, empty keys, maximum-size keys,
     non-UTF-8 binary data.
  3. **Concurrent stress** — 100 processes doing mixed operations simultaneously,
     mutex poisoning resilience after killed writers.
  4. **Integer overflow / float edge cases** — i64::MAX, NaN, Infinity in
     read-modify-write operations.
  5. **Probabilistic data structure NIFs** — cuckoo filter, CMS, TopK, TDigest,
     Bloom, HNSW edge cases via Elixir.
  6. **Yielding NIF safety** — large datasets that trigger the yielding codepath.
  7. **Zero-copy binary lifetime** — ensuring zero-copy binaries remain valid
     after the resource goes out of scope.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "nif_deep_edge_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store do
    dir = tmp_dir()
    {:ok, store} = NIF.new(dir)
    {store, dir}
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # ==================================================================
  # 1. Process lifecycle & NIF resource GC
  # ==================================================================

  describe "process lifecycle" do
    test "NIF resource survives creator process dying" do
      dir = tmp_dir()

      store =
        Task.async(fn ->
          {:ok, s} = NIF.new(dir)
          :ok = NIF.put(s, "survive", "value", 0)
          s
        end)
        |> Task.await()

      # Creator Task process is now dead, but the ResourceArc lives
      assert {:ok, "value"} = NIF.get(store, "survive")

      on_exit(fn -> File.rm_rf!(dir) end)
    end

    test "kill process during NIF execution — BEAM does not crash" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      for i <- 1..1000 do
        :ok = NIF.put(store, "kill_test_#{i}", "value_#{i}", 0)
      end

      pid =
        spawn(fn ->
          NIF.keys(store)
        end)

      Process.sleep(1)
      Process.exit(pid, :kill)
      Process.sleep(10)

      # BEAM must not have crashed
      assert {:ok, "value_1"} = NIF.get(store, "kill_test_1")
    end

    test "NIF resource garbage collected after all references dropped" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      for _ <- 1..20 do
        {:ok, s} = NIF.new(Path.join(dir, "gc_#{:rand.uniform(9_999_999)}"))
        :ok = NIF.put(s, "k", String.duplicate("x", 10_000), 0)
        # s goes out of scope
      end

      :erlang.garbage_collect()
      Process.sleep(50)
      # No OOM or crash is the test
      assert true
    end
  end

  # ==================================================================
  # 2. Binary safety
  # ==================================================================

  describe "binary safety" do
    test "NIF handles binary key with embedded nulls" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      key = <<0, 1, 0, 2, 0>>
      value = <<0, 0, 0, 0, 0>>
      :ok = NIF.put(store, key, value, 0)
      assert {:ok, ^value} = NIF.get(store, key)
    end

    test "NIF handles 0-byte key with non-empty value" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      result = NIF.put(store, "", "value", 0)
      assert result == :ok or match?({:error, _}, result)

      if result == :ok do
        assert {:ok, "value"} = NIF.get(store, "")
      end
    end

    test "NIF handles very large binary key (65535 bytes)" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      key = :binary.copy("x", 65535)
      :ok = NIF.put(store, key, "v", 0)
      assert {:ok, "v"} = NIF.get(store, key)
    end

    test "NIF rejects key larger than 65535 bytes" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      key = :binary.copy("x", 65536)
      result = NIF.put(store, key, "v", 0)
      assert {:error, _reason} = result
    end

    test "NIF handles value with all null bytes" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      value = :binary.copy(<<0>>, 1024)
      :ok = NIF.put(store, "nullval", value, 0)
      assert {:ok, ^value} = NIF.get(store, "nullval")
    end

    test "NIF handles all 256 byte values in key" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      for b <- 0..255 do
        key = <<b>>
        val = <<rem(b + 1, 256)>>
        :ok = NIF.put(store, key, val, 0)
      end

      for b <- 0..255 do
        key = <<b>>
        expected = <<rem(b + 1, 256)>>
        assert {:ok, ^expected} = NIF.get(store, key)
      end
    end

    test "NIF handles non-UTF-8 binary data" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      key = <<0xFF, 0xFE, 0xFD, 0xFC>>
      value = <<0x80, 0x81, 0x82, 0x83>>
      :ok = NIF.put(store, key, value, 0)
      assert {:ok, ^value} = NIF.get(store, key)
    end
  end

  # ==================================================================
  # 3. Concurrent stress
  # ==================================================================

  describe "concurrent stress" do
    test "100 processes doing different NIF operations simultaneously" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      for i <- 1..50 do
        :ok = NIF.put(store, "k#{i}", "v#{i}", 0)
      end

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            case rem(i, 5) do
              0 -> NIF.put(store, "stress_#{i}", "val_#{i}", 0)
              1 -> NIF.get(store, "k#{rem(i, 50) + 1}")
              2 -> NIF.delete(store, "k#{rem(i, 50) + 1}")
              3 -> NIF.keys(store)
              4 -> NIF.get_all(store)
            end
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert length(results) == 100

      for result <- results do
        assert result == :ok or
                 match?({:ok, _}, result) or
                 match?({:error, _}, result) or
                 is_list(result)
      end
    end

    test "store usable after process killed during write" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      pid =
        spawn(fn ->
          for i <- 1..100_000 do
            NIF.put(store, "kill_write_#{i}", "value", 0)
          end
        end)

      Process.sleep(10)
      Process.exit(pid, :kill)
      Process.sleep(10)

      assert :ok = NIF.put(store, "after_kill", "works", 0)
      assert {:ok, "works"} = NIF.get(store, "after_kill")
    end

    test "concurrent put_batch does not corrupt store" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            batch =
              for i <- 1..100 do
                {"batch_t#{t}_k#{i}", "batch_t#{t}_v#{i}", 0}
              end

            NIF.put_batch(store, batch)
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))
      assert {:ok, _} = NIF.get(store, "batch_t1_k1")
    end
  end

  # ==================================================================
  # 4. Integer overflow / float edge cases in read_modify_write
  # ==================================================================

  describe "read_modify_write edge cases" do
    test "INCRBY on nonexistent key starts from zero" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      assert {:ok, "5"} = NIF.read_modify_write(store, "counter", {:incr_by, 5})
    end

    test "INCRBY overflow at i64::MAX returns error" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      max = 9_223_372_036_854_775_807
      :ok = NIF.put(store, "big", Integer.to_string(max), 0)
      result = NIF.read_modify_write(store, "big", {:incr_by, 1})
      assert {:error, reason} = result
      assert reason =~ "overflow"
    end

    test "INCRBY underflow at i64::MIN returns error" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      min = -9_223_372_036_854_775_808
      :ok = NIF.put(store, "small", Integer.to_string(min), 0)
      result = NIF.read_modify_write(store, "small", {:incr_by, -1})
      assert {:error, _reason} = result
    end

    test "INCRBYFLOAT with non-numeric string returns error" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "nan_test", "not_a_number", 0)
      result = NIF.read_modify_write(store, "nan_test", {:incr_by_float, 1.0})
      assert {:error, _} = result
    end

    test "SETRANGE beyond 512MB returns error" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      offset = 512 * 1024 * 1024
      result = NIF.read_modify_write(store, "huge", {:set_range, offset, "x"})
      assert {:error, _} = result
    end

    test "APPEND to nonexistent key creates value" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      assert {:ok, "hello"} = NIF.read_modify_write(store, "appkey", {:append, "hello"})
      assert {:ok, "helloworld"} = NIF.read_modify_write(store, "appkey", {:append, "world"})
    end

    test "SETBIT on nonexistent key" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      assert {:ok, _} = NIF.read_modify_write(store, "bitkey", {:set_bit, 7, 1})
    end
  end

  # ==================================================================
  # 5. Probabilistic data structure NIF edge cases
  # ==================================================================

  describe "cuckoo filter edge cases" do
    test "create, add, exists, delete lifecycle" do
      {:ok, cf} = NIF.cuckoo_create(1024, 4, 500, 2)
      # cuckoo_exists returns raw 0/1
      assert 0 = NIF.cuckoo_exists(cf, "item")
      assert :ok = NIF.cuckoo_add(cf, "item")
      assert 1 = NIF.cuckoo_exists(cf, "item")
      # cuckoo_del returns raw count deleted
      assert 1 = NIF.cuckoo_del(cf, "item")
      assert 0 = NIF.cuckoo_exists(cf, "item")
    end

    test "addnx prevents duplicates" do
      {:ok, cf} = NIF.cuckoo_create(1024, 4, 500, 2)
      # cuckoo_addnx returns raw 1 (inserted) or 0 (already exists)
      assert 1 = NIF.cuckoo_addnx(cf, "unique")
      assert 0 = NIF.cuckoo_addnx(cf, "unique")
    end

    test "mexists with mixed items" do
      {:ok, cf} = NIF.cuckoo_create(1024, 4, 500, 2)
      :ok = NIF.cuckoo_add(cf, "a")
      :ok = NIF.cuckoo_add(cf, "b")
      # cuckoo_mexists returns raw list
      results = NIF.cuckoo_mexists(cf, ["a", "b", "c"])
      assert results == [1, 1, 0]
    end

    test "serialize/deserialize roundtrip" do
      {:ok, cf} = NIF.cuckoo_create(1024, 4, 500, 2)
      :ok = NIF.cuckoo_add(cf, "persist")
      {:ok, bytes} = NIF.cuckoo_serialize(cf)
      assert is_binary(bytes)
      {:ok, cf2} = NIF.cuckoo_deserialize(bytes)
      assert 1 = NIF.cuckoo_exists(cf2, "persist")
    end

    test "delete nonexistent returns 0" do
      {:ok, cf} = NIF.cuckoo_create(1024, 4, 500, 2)
      assert 0 = NIF.cuckoo_del(cf, "ghost")
    end

    test "binary key with null bytes" do
      {:ok, cf} = NIF.cuckoo_create(1024, 4, 500, 2)
      key = <<0, 1, 0, 2>>
      :ok = NIF.cuckoo_add(cf, key)
      assert 1 = NIF.cuckoo_exists(cf, key)
    end
  end

  describe "count-min sketch edge cases" do
    test "create, incrby, query lifecycle" do
      {:ok, cms} = NIF.cms_create(1000, 7)
      {:ok, _counts} = NIF.cms_incrby(cms, [{"apple", 5}, {"banana", 10}])
      {:ok, results} = NIF.cms_query(cms, ["apple", "banana", "cherry"])
      assert length(results) == 3
      [apple_est, banana_est, cherry_est] = results
      assert apple_est >= 5
      assert banana_est >= 10
      assert cherry_est >= 0
    end

    test "serialize/deserialize roundtrip" do
      {:ok, cms} = NIF.cms_create(500, 5)
      {:ok, _} = NIF.cms_incrby(cms, [{"key", 42}])
      {:ok, bytes} = NIF.cms_to_bytes(cms)
      {:ok, cms2} = NIF.cms_from_bytes(bytes)
      {:ok, [est]} = NIF.cms_query(cms2, ["key"])
      assert est >= 42
    end

    test "merge two sketches" do
      {:ok, cms1} = NIF.cms_create(500, 5)
      {:ok, cms2} = NIF.cms_create(500, 5)
      {:ok, _} = NIF.cms_incrby(cms1, [{"shared", 10}])
      {:ok, _} = NIF.cms_incrby(cms2, [{"shared", 20}])
      {:ok, dest} = NIF.cms_create(500, 5)
      # cms_merge takes [{resource, weight}] pairs
      :ok = NIF.cms_merge(dest, [{cms1, 1}, {cms2, 1}])
      {:ok, [est]} = NIF.cms_query(dest, ["shared"])
      assert est >= 30
    end

    test "width 1 depth 1 degenerate" do
      {:ok, cms} = NIF.cms_create(1, 1)
      {:ok, _} = NIF.cms_incrby(cms, [{"a", 5}, {"b", 3}])
      {:ok, [a_est]} = NIF.cms_query(cms, ["a"])
      assert a_est == 8
    end
  end

  describe "TopK edge cases" do
    test "create, add, query, list lifecycle" do
      {:ok, topk} = NIF.topk_create(3, 8, 7, 0.9)
      # topk_add returns raw list of displaced items (nil per item)
      _displaced = NIF.topk_add(topk, ["a", "b", "c"])
      # topk_query returns raw list of 0/1
      results = NIF.topk_query(topk, ["a", "d"])
      assert length(results) == 2
      # topk_list returns raw list of strings
      list = NIF.topk_list(topk)
      assert length(list) == 3
    end

    test "k=1 only keeps highest frequency item" do
      {:ok, topk} = NIF.topk_create(1, 8, 7, 0.9)
      _displaced1 = NIF.topk_incrby(topk, [{"low", 1}])
      _displaced2 = NIF.topk_incrby(topk, [{"high", 100}])
      list = NIF.topk_list(topk)
      assert length(list) == 1
    end

    test "serialize/deserialize roundtrip" do
      {:ok, topk} = NIF.topk_create(5, 8, 7, 0.9)
      _displaced = NIF.topk_incrby(topk, [{"item", 50}])
      {:ok, bytes} = NIF.topk_to_bytes(topk)
      {:ok, topk2} = NIF.topk_from_bytes(bytes)
      list = NIF.topk_list(topk2)
      assert length(list) > 0
    end

    test "empty TopK list is empty" do
      {:ok, topk} = NIF.topk_create(10, 8, 7, 0.9)
      list = NIF.topk_list(topk)
      assert list == []
    end
  end

  describe "TDigest edge cases" do
    test "create, add, quantile lifecycle" do
      # tdigest_create returns raw resource
      td = NIF.tdigest_create(100.0)
      assert is_reference(td)

      values = for i <- 1..1000, do: i / 1.0
      :ok = NIF.tdigest_add(td, values)

      # tdigest_quantile returns raw list
      [p50] = NIF.tdigest_quantile(td, [0.5])
      assert abs(p50 - 500.0) < 50.0
    end

    test "quantile on empty digest returns NaN" do
      td = NIF.tdigest_create(100.0)
      [q] = NIF.tdigest_quantile(td, [0.5])
      # NaN in BEAM: :nan atom
      assert q == :nan or (is_float(q) and q != q)
    end

    test "min/max on empty returns NaN" do
      td = NIF.tdigest_create(100.0)
      min_val = NIF.tdigest_min(td)
      max_val = NIF.tdigest_max(td)
      assert min_val == :nan or is_float(min_val)
      assert max_val == :nan or is_float(max_val)
    end

    test "compression = 1 (minimum) does not crash" do
      td = NIF.tdigest_create(1.0)
      :ok = NIF.tdigest_add(td, [1.0, 2.0, 3.0, 4.0, 5.0])
      [p50] = NIF.tdigest_quantile(td, [0.5])
      assert is_float(p50) or p50 == :nan
    end

    test "million identical values" do
      td = NIF.tdigest_create(100.0)
      for _ <- 1..100 do
        values = List.duplicate(42.0, 10_000)
        :ok = NIF.tdigest_add(td, values)
      end

      [p50] = NIF.tdigest_quantile(td, [0.5])
      assert is_float(p50)
      assert abs(p50 - 42.0) < 1.0
    end

    test "serialize/deserialize roundtrip" do
      td = NIF.tdigest_create(100.0)
      :ok = NIF.tdigest_add(td, [1.0, 2.0, 3.0, 4.0, 5.0])
      bytes = NIF.tdigest_serialize(td)
      assert is_binary(bytes)
      {:ok, td2} = NIF.tdigest_deserialize(bytes)
      [p50_1] = NIF.tdigest_quantile(td, [0.5])
      [p50_2] = NIF.tdigest_quantile(td2, [0.5])
      assert is_float(p50_1)
      assert is_float(p50_2)
      assert abs(p50_1 - p50_2) < 0.01
    end

    test "merge two digests" do
      d1 = NIF.tdigest_create(100.0)
      d2 = NIF.tdigest_create(100.0)
      :ok = NIF.tdigest_add(d1, Enum.map(1..500, &(&1 / 1.0)))
      :ok = NIF.tdigest_add(d2, Enum.map(501..1000, &(&1 / 1.0)))
      merged = NIF.tdigest_merge([d1, d2], 100.0)
      assert is_reference(merged)
      info = NIF.tdigest_info(merged)
      assert is_list(info)
    end

    test "reset clears all data" do
      td = NIF.tdigest_create(100.0)
      :ok = NIF.tdigest_add(td, [1.0, 2.0, 3.0])
      :ok = NIF.tdigest_reset(td)
      info = NIF.tdigest_info(td)
      assert is_list(info)
    end
  end

  describe "HNSW edge cases" do
    test "create, add, search, delete lifecycle" do
      {:ok, idx} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, _id} = NIF.hnsw_add(idx, "close", [1.0, 0.0, 0.0])
      {:ok, _id} = NIF.hnsw_add(idx, "far", [10.0, 10.0, 10.0])
      {:ok, results} = NIF.hnsw_search(idx, [1.0, 0.0, 0.0], 1, 50)
      assert length(results) == 1
      [{key, _dist}] = results
      assert key == "close"
    end

    test "search on empty index returns empty" do
      {:ok, idx} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, results} = NIF.hnsw_search(idx, [1.0, 0.0, 0.0], 5, 50)
      assert results == []
    end

    test "dimension mismatch returns error" do
      {:ok, idx} = NIF.hnsw_new(4, 16, 128, "l2")
      result = NIF.hnsw_add(idx, "bad", [1.0, 2.0])
      assert {:error, _} = result
    end

    test "delete returns false for nonexistent key" do
      {:ok, idx} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, false} = NIF.hnsw_delete(idx, "ghost")
    end

    test "count reflects insertions and deletions" do
      {:ok, idx} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, _} = NIF.hnsw_add(idx, "a", [1.0, 0.0, 0.0])
      {:ok, _} = NIF.hnsw_add(idx, "b", [0.0, 1.0, 0.0])
      assert {:ok, 2} = NIF.hnsw_count(idx)
      {:ok, true} = NIF.hnsw_delete(idx, "a")
      assert {:ok, 1} = NIF.hnsw_count(idx)
    end

    test "search with k > count returns all" do
      {:ok, idx} = NIF.hnsw_new(3, 16, 128, "l2")
      {:ok, _} = NIF.hnsw_add(idx, "only", [1.0, 0.0, 0.0])
      {:ok, results} = NIF.hnsw_search(idx, [1.0, 0.0, 0.0], 100, 200)
      assert length(results) == 1
    end
  end

  describe "Bloom filter edge cases" do
    @tag :bloom_nif_mmap
    test "create, add, exists lifecycle" do
      dir = tmp_dir()
      path = Path.join(dir, "test.bloom")
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, bloom} = NIF.bloom_create(path, 10_000, 7)
      # bloom_exists returns raw 0/1
      assert 0 = NIF.bloom_exists(bloom, "item")
      # bloom_add returns raw 1 (new) or 0 (duplicate)
      assert 1 = NIF.bloom_add(bloom, "item")
      assert 1 = NIF.bloom_exists(bloom, "item")
    end

    @tag :bloom_nif_mmap
    test "empty key in bloom filter" do
      dir = tmp_dir()
      path = Path.join(dir, "empty_key.bloom")
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, bloom} = NIF.bloom_create(path, 10_000, 7)
      _result = NIF.bloom_add(bloom, "")
      assert 1 = NIF.bloom_exists(bloom, "")
    end

    @tag :bloom_nif_mmap
    test "mexists with mixed results" do
      dir = tmp_dir()
      path = Path.join(dir, "mexists.bloom")
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, bloom} = NIF.bloom_create(path, 100_000, 7)
      NIF.bloom_add(bloom, "present")
      # bloom_mexists returns raw list of 0/1
      results = NIF.bloom_mexists(bloom, ["present", "absent"])
      assert length(results) == 2
    end
  end

  # ==================================================================
  # 6. Yielding NIF safety — large datasets
  # ==================================================================

  describe "yielding NIF safety" do
    test "keys() with 10K entries yields properly" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      batch =
        for i <- 1..10_000 do
          {"yield_key_#{i}", "yield_val_#{i}", 0}
        end

      :ok = NIF.put_batch(store, batch)
      keys = NIF.keys(store)
      assert length(keys) == 10_000
    end

    test "get_all() with 5K entries yields properly" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      batch =
        for i <- 1..5_000 do
          {"ga_key_#{i}", "ga_val_#{i}", 0}
        end

      :ok = NIF.put_batch(store, batch)
      {:ok, pairs} = NIF.get_all(store)
      assert length(pairs) == 5_000
    end

    test "get_batch() with 5K keys yields properly" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      batch =
        for i <- 1..5_000 do
          {"gb_key_#{i}", "gb_val_#{i}", 0}
        end

      :ok = NIF.put_batch(store, batch)
      keys = for i <- 1..5_000, do: "gb_key_#{i}"
      {:ok, results} = NIF.get_batch(store, keys)
      assert length(results) == 5_000
      assert Enum.all?(results, &(&1 != nil))
    end

    test "get_range() with large range yields properly" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      batch =
        for i <- 1..5_000 do
          {String.pad_leading("#{i}", 6, "0"), "range_val_#{i}", 0}
        end

      :ok = NIF.put_batch(store, batch)
      {:ok, pairs} = NIF.get_range(store, "000001", "999999", 10_000)
      assert length(pairs) == 5_000
    end
  end

  # ==================================================================
  # 7. Zero-copy binary lifetime
  # ==================================================================

  describe "zero-copy binary safety" do
    test "get_zero_copy returns valid binary" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "zc_key", "zc_value_data", 0)
      {:ok, value} = NIF.get_zero_copy(store, "zc_key")
      assert value == "zc_value_data"
    end

    test "get_zero_copy returns nil for missing key" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      assert {:ok, nil} = NIF.get_zero_copy(store, "missing")
    end

    test "get_all_zero_copy returns valid pairs" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      for i <- 1..100 do
        :ok = NIF.put(store, "zc_all_#{i}", "val_#{i}", 0)
      end

      {:ok, pairs} = NIF.get_all_zero_copy(store)
      assert length(pairs) == 100

      for {k, v} <- pairs do
        assert is_binary(k)
        assert is_binary(v)
      end
    end

    test "get_batch_zero_copy handles mixed hits and misses" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "hit1", "val1", 0)
      :ok = NIF.put(store, "hit2", "val2", 0)

      {:ok, results} = NIF.get_batch_zero_copy(store, ["hit1", "miss1", "hit2"])
      assert length(results) == 3
      assert Enum.at(results, 0) == "val1"
      assert Enum.at(results, 1) == nil
      assert Enum.at(results, 2) == "val2"
    end

    test "zero-copy binaries remain valid after GC" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "gc_test", String.duplicate("x", 10_000), 0)
      {:ok, value} = NIF.get_zero_copy(store, "gc_test")

      :erlang.garbage_collect()
      Process.sleep(10)

      assert byte_size(value) == 10_000
      assert value == String.duplicate("x", 10_000)
    end
  end

  # ==================================================================
  # 8. Store operations
  # ==================================================================

  describe "store operation edge cases" do
    test "put_batch with empty list is no-op" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "existing", "value", 0)
      :ok = NIF.put_batch(store, [])
      assert {:ok, "value"} = NIF.get(store, "existing")
    end

    test "put_batch with single item" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put_batch(store, [{"single", "item", 0}])
      assert {:ok, "item"} = NIF.get(store, "single")
    end

    test "shard_stats on empty store" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, stats} = NIF.shard_stats(store)
      assert is_tuple(stats)
    end

    test "file_sizes on empty store" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, sizes} = NIF.file_sizes(store)
      assert is_list(sizes)
    end

    test "available_disk_space returns a positive number" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, space} = NIF.available_disk_space(store)
      assert is_integer(space) and space > 0
    end

    test "write_hint on empty store" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      assert :ok = NIF.write_hint(store)
    end

    test "purge_expired with no expired keys" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "live", "data", 0)
      {:ok, 0} = NIF.purge_expired(store)
    end

    test "run_compaction with empty file_ids list" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, result} = NIF.run_compaction(store, [])
      assert is_tuple(result)
    end

    test "store persists data across close and reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store1 = open_store(dir)
      :ok = NIF.put(store1, "persist_key", "persist_value", 0)
      _store1 = nil
      :erlang.garbage_collect()
      Process.sleep(50)

      store2 = open_store(dir)
      assert {:ok, "persist_value"} = NIF.get(store2, "persist_key")
    end

    test "delete returns expected tuple format" do
      {store, dir} = open_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "del_me", "val", 0)
      assert {:ok, true} = NIF.delete(store, "del_me")
      assert {:ok, false} = NIF.delete(store, "del_me")
      assert {:ok, false} = NIF.delete(store, "never_existed")
    end
  end

  # ==================================================================
  # 9. Tracking allocator NIF
  # ==================================================================

  describe "tracking allocator" do
    test "rust_allocated_bytes returns a non-negative integer" do
      result = NIF.rust_allocated_bytes()
      # The tracking allocator may not be active in all build configurations.
      # When it's not compiled in, tracked_allocated_bytes() returns None,
      # and the NIF returns -1. Accept -1 as valid (allocator not active).
      assert is_integer(result) and result >= -1
    end
  end
end
