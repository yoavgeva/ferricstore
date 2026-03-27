defmodule Ferricstore.NIFYieldTest do
  @moduledoc """
  Tests for consume_timeslice yielding in batch/multi NIF operations.

  Covers bloom (bf_madd/bf_mexists), CMS (cms_incrby/cms_query/cms_merge),
  and cuckoo (cf_mexists) batch operations with edge cases, input validation,
  fault tolerance, and scheduler health.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "nif_yield_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp create_bloom(opts \\ []) do
    dir = tmp_dir()
    path = Path.join(dir, "test_#{:rand.uniform(9_999_999)}.bloom")
    num_bits = Keyword.get(opts, :num_bits, 1_000_000)
    num_hashes = Keyword.get(opts, :num_hashes, 7)
    {:ok, ref} = NIF.bloom_create(path, num_bits, num_hashes)
    {ref, dir}
  end

  defp create_cms(opts \\ []) do
    width = Keyword.get(opts, :width, 1000)
    depth = Keyword.get(opts, :depth, 7)
    {:ok, ref} = NIF.cms_create(width, depth)
    ref
  end

  defp create_cuckoo(opts \\ []) do
    capacity = Keyword.get(opts, :capacity, 10_000)
    bucket_size = Keyword.get(opts, :bucket_size, 4)
    max_kicks = Keyword.get(opts, :max_kicks, 500)
    expansion = Keyword.get(opts, :expansion, 1)
    {:ok, ref} = NIF.cuckoo_create(capacity, bucket_size, max_kicks, expansion)
    ref
  end

  # ==================================================================
  # Edge cases for batch operations
  # ==================================================================

  describe "empty input" do
    test "bf_madd with empty list" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      assert NIF.bloom_madd(ref, []) == []
    end

    test "bf_mexists with empty list" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      assert NIF.bloom_mexists(ref, []) == []
    end

    test "cms_incrby with empty list" do
      ref = create_cms()
      assert {:ok, []} = NIF.cms_incrby(ref, [])
    end

    test "cms_query with empty list" do
      ref = create_cms()
      assert {:ok, []} = NIF.cms_query(ref, [])
    end

    test "cf_mexists with empty list" do
      ref = create_cuckoo()
      assert NIF.cuckoo_mexists(ref, []) == []
    end
  end

  describe "single item" do
    test "bf_madd with exactly 1 element" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      assert NIF.bloom_madd(ref, ["single"]) == [1]
      # Adding same item again returns 0 (already exists)
      assert NIF.bloom_madd(ref, ["single"]) == [0]
    end

    test "bf_mexists with exactly 1 element" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      assert NIF.bloom_mexists(ref, ["absent"]) == [0]
      NIF.bloom_add(ref, "absent")
      assert NIF.bloom_mexists(ref, ["absent"]) == [1]
    end

    test "cms_incrby with exactly 1 pair" do
      ref = create_cms()
      {:ok, [count]} = NIF.cms_incrby(ref, [{"item", 5}])
      assert count >= 5
    end

    test "cms_query with exactly 1 element" do
      ref = create_cms()
      NIF.cms_incrby(ref, [{"item", 10}])
      {:ok, [count]} = NIF.cms_query(ref, ["item"])
      assert count >= 10
    end

    test "cf_mexists with exactly 1 element" do
      ref = create_cuckoo()
      assert NIF.cuckoo_mexists(ref, ["missing"]) == [0]
      NIF.cuckoo_add(ref, "missing")
      assert NIF.cuckoo_mexists(ref, ["missing"]) == [1]
    end
  end

  describe "large batch (triggers yielding)" do
    test "bf_madd with 10,000 items returns correct results" do
      {ref, dir} = create_bloom(num_bits: 10_000_000)
      on_exit(fn -> File.rm_rf(dir) end)

      elements = Enum.map(1..10_000, &"item_#{&1}")
      results = NIF.bloom_madd(ref, elements)

      assert length(results) == 10_000
      # All items are new, so all should be 1
      assert Enum.all?(results, &(&1 == 1))
    end

    test "bf_mexists with 10,000 items returns correct results" do
      {ref, dir} = create_bloom(num_bits: 10_000_000)
      on_exit(fn -> File.rm_rf(dir) end)

      elements = Enum.map(1..10_000, &"item_#{&1}")
      NIF.bloom_madd(ref, elements)

      results = NIF.bloom_mexists(ref, elements)
      assert length(results) == 10_000
      assert Enum.all?(results, &(&1 == 1))
    end

    test "cms_incrby with 10,000 items" do
      ref = create_cms(width: 10_000)
      items = Enum.map(1..10_000, &{"key_#{&1}", 1})
      {:ok, counts} = NIF.cms_incrby(ref, items)

      assert length(counts) == 10_000
      assert Enum.all?(counts, &(&1 >= 1))
    end

    test "cms_query with 10,000 items" do
      ref = create_cms(width: 10_000)
      items = Enum.map(1..10_000, &{"key_#{&1}", 1})
      NIF.cms_incrby(ref, items)

      keys = Enum.map(1..10_000, &"key_#{&1}")
      {:ok, counts} = NIF.cms_query(ref, keys)

      assert length(counts) == 10_000
      assert Enum.all?(counts, &(&1 >= 1))
    end

    test "cf_mexists with 10,000 items" do
      ref = create_cuckoo(capacity: 20_000)
      elements = Enum.map(1..10_000, &"item_#{&1}")
      Enum.each(elements, &NIF.cuckoo_add(ref, &1))

      results = NIF.cuckoo_mexists(ref, elements)
      assert length(results) == 10_000
      assert Enum.all?(results, &(&1 == 1))
    end
  end

  describe "concurrent batch ops" do
    test "multiple processes doing bf_madd simultaneously" do
      {ref, dir} = create_bloom(num_bits: 10_000_000)
      on_exit(fn -> File.rm_rf(dir) end)

      tasks =
        Enum.map(1..10, fn proc_id ->
          Task.async(fn ->
            elements = Enum.map(1..1_000, &"proc_#{proc_id}_item_#{&1}")
            NIF.bloom_madd(ref, elements)
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 10_000))

      # All tasks complete without crash
      assert length(results) == 10
      Enum.each(results, fn result ->
        assert is_list(result)
        assert length(result) == 1_000
      end)
    end

    test "multiple processes doing cms_incrby simultaneously" do
      ref = create_cms(width: 10_000)

      tasks =
        Enum.map(1..10, fn proc_id ->
          Task.async(fn ->
            items = Enum.map(1..500, &{"proc_#{proc_id}_k_#{&1}", 1})
            NIF.cms_incrby(ref, items)
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 10_000))
      assert length(results) == 10
      Enum.each(results, fn {:ok, counts} -> assert length(counts) == 500 end)
    end
  end

  describe "duplicate items in batch" do
    test "bf_madd with same item repeated 100 times" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      elements = List.duplicate("same_item", 100)
      results = NIF.bloom_madd(ref, elements)

      assert length(results) == 100
      # First insert is new (1), rest are duplicates (0)
      assert hd(results) == 1
      assert Enum.sum(results) == 1
    end

    test "cms_incrby with same key repeated" do
      ref = create_cms()
      items = List.duplicate({"hotkey", 1}, 100)
      {:ok, counts} = NIF.cms_incrby(ref, items)

      assert length(counts) == 100
      # Each increment should yield a progressively higher count
      assert List.last(counts) >= 100
    end
  end

  describe "binary data in batch" do
    test "bf_madd with null bytes, unicode, empty strings" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      elements = [
        <<0, 1, 2, 3>>,
        <<0, 0, 0>>,
        "hello world",
        "",
        <<255, 254, 253>>,
        "normal_string"
      ]

      results = NIF.bloom_madd(ref, elements)
      assert length(results) == 6
      assert Enum.all?(results, &(&1 in [0, 1]))

      # Verify all elements can be found
      exists = NIF.bloom_mexists(ref, elements)
      assert Enum.all?(exists, &(&1 == 1))
    end

    test "cf_mexists with binary data" do
      ref = create_cuckoo()

      elements = [<<0, 1, 2>>, "unicode", "", <<255>>]
      Enum.each(elements, &NIF.cuckoo_add(ref, &1))

      results = NIF.cuckoo_mexists(ref, elements)
      assert Enum.all?(results, &(&1 == 1))
    end

    test "cms_incrby with binary keys" do
      ref = create_cms()

      items = [
        {<<0, 0, 0>>, 1},
        {"", 5},
        {"unicode", 3}
      ]

      {:ok, counts} = NIF.cms_incrby(ref, items)
      assert length(counts) == 3
    end
  end

  # ==================================================================
  # Input validation
  # ==================================================================

  describe "input validation" do
    test "cms_incrby with negative increment" do
      ref = create_cms()
      # CMS allows negative increments (decrement)
      {:ok, [count]} = NIF.cms_incrby(ref, [{"item", -5}])
      assert count == -5
    end

    test "cms_incrby with zero increment" do
      ref = create_cms()
      NIF.cms_incrby(ref, [{"item", 10}])
      {:ok, [count]} = NIF.cms_incrby(ref, [{"item", 0}])
      assert count >= 10
    end

    test "zero-length key as key name in bf_madd" do
      {ref, dir} = create_bloom()
      on_exit(fn -> File.rm_rf(dir) end)

      results = NIF.bloom_madd(ref, [""])
      assert results == [1]

      exists = NIF.bloom_mexists(ref, [""])
      assert exists == [1]
    end
  end

  # ==================================================================
  # Fault tolerance
  # ==================================================================

  describe "fault tolerance" do
    test "concurrent read during write on bloom filter" do
      {ref, dir} = create_bloom(num_bits: 10_000_000)
      on_exit(fn -> File.rm_rf(dir) end)

      # Writer task
      writer =
        Task.async(fn ->
          elements = Enum.map(1..5_000, &"write_item_#{&1}")
          NIF.bloom_madd(ref, elements)
        end)

      # Reader task
      reader =
        Task.async(fn ->
          Enum.map(1..100, fn _ ->
            NIF.bloom_exists(ref, "write_item_1")
          end)
        end)

      writer_result = Task.await(writer, 10_000)
      reader_result = Task.await(reader, 10_000)

      assert is_list(writer_result)
      assert length(writer_result) == 5_000
      # Reader should get valid results (0 or 1), never crash
      assert Enum.all?(reader_result, &(&1 in [0, 1]))
    end

    test "very large single item (1MB) in bf_madd" do
      {ref, dir} = create_bloom(num_bits: 10_000_000)
      on_exit(fn -> File.rm_rf(dir) end)

      large_item = :crypto.strong_rand_bytes(1_024 * 1_024)
      results = NIF.bloom_madd(ref, [large_item])
      assert results == [1]

      exists = NIF.bloom_mexists(ref, [large_item])
      assert exists == [1]
    end
  end

  # ==================================================================
  # Scheduler health — proves yielding works
  # ==================================================================

  describe "scheduler health" do
    test "scheduler not blocked during large bf_madd (50,000 items)" do
      {ref, dir} = create_bloom(num_bits: 50_000_000)
      on_exit(fn -> File.rm_rf(dir) end)

      # Start a large bf_madd in a background task
      madd_task =
        Task.async(fn ->
          elements = Enum.map(1..50_000, &"yield_item_#{&1}")
          NIF.bloom_madd(ref, elements)
        end)

      # Simultaneously do a simple ping from another process.
      # If yielding works, the ping should respond quickly.
      ping_task =
        Task.async(fn ->
          start = System.monotonic_time(:millisecond)
          {:ok, "PONG"} = FerricStore.ping()
          elapsed = System.monotonic_time(:millisecond) - start
          elapsed
        end)

      ping_elapsed = Task.await(ping_task, 5_000)
      madd_result = Task.await(madd_task, 30_000)

      # The ping should complete within 100ms (proves scheduler isn't blocked)
      assert ping_elapsed < 100,
             "ping took #{ping_elapsed}ms, expected <100ms (scheduler may be blocked)"

      assert length(madd_result) == 50_000
    end

    test "scheduler not blocked during large cms_incrby (50,000 items)" do
      ref = create_cms(width: 50_000)

      incrby_task =
        Task.async(fn ->
          items = Enum.map(1..50_000, &{"yield_key_#{&1}", 1})
          NIF.cms_incrby(ref, items)
        end)

      ping_task =
        Task.async(fn ->
          start = System.monotonic_time(:millisecond)
          {:ok, "PONG"} = FerricStore.ping()
          elapsed = System.monotonic_time(:millisecond) - start
          elapsed
        end)

      ping_elapsed = Task.await(ping_task, 5_000)
      {:ok, counts} = Task.await(incrby_task, 30_000)

      assert ping_elapsed < 100,
             "ping took #{ping_elapsed}ms, expected <100ms (scheduler may be blocked)"

      assert length(counts) == 50_000
    end

    test "scheduler not blocked during large cf_mexists (50,000 items)" do
      ref = create_cuckoo(capacity: 60_000)

      # Pre-populate some items
      Enum.each(1..1_000, &NIF.cuckoo_add(ref, "pre_#{&1}"))

      mexists_task =
        Task.async(fn ->
          elements = Enum.map(1..50_000, &"check_#{&1}")
          NIF.cuckoo_mexists(ref, elements)
        end)

      ping_task =
        Task.async(fn ->
          start = System.monotonic_time(:millisecond)
          {:ok, "PONG"} = FerricStore.ping()
          elapsed = System.monotonic_time(:millisecond) - start
          elapsed
        end)

      ping_elapsed = Task.await(ping_task, 5_000)
      results = Task.await(mexists_task, 30_000)

      assert ping_elapsed < 100,
             "ping took #{ping_elapsed}ms, expected <100ms (scheduler may be blocked)"

      assert length(results) == 50_000
    end
  end

  # ==================================================================
  # CMS merge yielding
  # ==================================================================

  describe "cms_merge yielding" do
    test "merge with large sketches" do
      width = 10_000
      depth = 7
      {:ok, dest} = NIF.cms_create(width, depth)
      {:ok, src1} = NIF.cms_create(width, depth)
      {:ok, src2} = NIF.cms_create(width, depth)

      # Populate sources
      items1 = Enum.map(1..1_000, &{"key_#{&1}", 1})
      items2 = Enum.map(1..1_000, &{"key_#{&1}", 2})
      NIF.cms_incrby(src1, items1)
      NIF.cms_incrby(src2, items2)

      # Merge should work correctly with yielding
      assert :ok = NIF.cms_merge(dest, [{src1, 1}, {src2, 1}])

      # Verify merged counts: key_1 should be >= 3 (1 + 2)
      {:ok, [count]} = NIF.cms_query(dest, ["key_1"])
      assert count >= 3
    end

    test "scheduler not blocked during large cms_merge" do
      width = 50_000
      depth = 10
      {:ok, dest} = NIF.cms_create(width, depth)
      {:ok, src} = NIF.cms_create(width, depth)

      NIF.cms_incrby(src, [{"key", 1}])

      merge_task =
        Task.async(fn ->
          NIF.cms_merge(dest, [{src, 1}])
        end)

      ping_task =
        Task.async(fn ->
          start = System.monotonic_time(:millisecond)
          {:ok, "PONG"} = FerricStore.ping()
          elapsed = System.monotonic_time(:millisecond) - start
          elapsed
        end)

      ping_elapsed = Task.await(ping_task, 5_000)
      merge_result = Task.await(merge_task, 30_000)

      assert ping_elapsed < 100,
             "ping took #{ping_elapsed}ms, expected <100ms (scheduler may be blocked)"

      assert merge_result == :ok
    end
  end
end
