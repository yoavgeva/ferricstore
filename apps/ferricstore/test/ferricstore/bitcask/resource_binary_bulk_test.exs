defmodule Ferricstore.Bitcask.ResourceBinaryBulkTest do
  @moduledoc """
  Integration tests for zero-copy bulk NIF operations:
  - get_all_zero_copy/1
  - get_batch_zero_copy/2
  - get_range_zero_copy/4

  These NIFs use ResourceBinary (enif_make_resource_binary) to return BEAM
  binary terms that point directly into Rust-owned memory — zero copy.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "zc_bulk_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp setup_store(_context \\ %{}) do
    dir = tmp_dir()
    store = open_store(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{store: store, dir: dir}
  end

  # ====================================================================
  # get_all_zero_copy/1
  # ====================================================================

  describe "get_all_zero_copy/1" do
    test "returns same results as regular get_all" do
      %{store: store} = setup_store()

      for i <- 1..50 do
        :ok = NIF.put(store, "key_#{i}", "val_#{i}", 0)
      end

      {:ok, regular} = NIF.get_all(store)
      {:ok, zero_copy} = NIF.get_all_zero_copy(store)

      # Both should have the same entries (order may differ)
      assert length(regular) == length(zero_copy)
      regular_sorted = Enum.sort(regular)
      zero_copy_sorted = Enum.sort(zero_copy)
      assert regular_sorted == zero_copy_sorted
    end

    test "empty store returns empty list" do
      %{store: store} = setup_store()
      assert {:ok, []} = NIF.get_all_zero_copy(store)
    end

    test "single entry" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k1", "v1", 0)

      assert {:ok, [{key, val}]} = NIF.get_all_zero_copy(store)
      assert key == "k1"
      assert val == "v1"
    end

    test "100 keys all returned correctly" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..100, fn i ->
          {"key_#{String.pad_leading("#{i}", 4, "0")}", "val_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      assert {:ok, entries} = NIF.get_all_zero_copy(store)
      assert length(entries) == 100

      for i <- 1..100 do
        padded = String.pad_leading("#{i}", 4, "0")
        assert {"key_#{padded}", "val_#{i}"} in entries
      end
    end

    test "large values (100KB each) returned correctly" do
      %{store: store} = setup_store()
      large_val = :binary.copy(<<0xAB>>, 100_000)

      for i <- 1..5 do
        :ok = NIF.put(store, "big_#{i}", large_val, 0)
      end

      assert {:ok, entries} = NIF.get_all_zero_copy(store)
      assert length(entries) == 5

      for {_k, v} <- entries do
        assert byte_size(v) == 100_000
        assert v == large_val
      end
    end

    test "returned binaries are valid Elixir binaries" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "hello", "world", 0)

      {:ok, [{key, val}]} = NIF.get_all_zero_copy(store)

      # Should work with all standard binary operations
      assert is_binary(key)
      assert is_binary(val)
      assert String.valid?(key)
      assert String.valid?(val)
      assert String.length(key) == 5
      assert String.upcase(val) == "WORLD"
      assert byte_size(key) == 5
      assert byte_size(val) == 5
    end

    test "excludes deleted (tombstoned) entries" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "keep", "yes", 0)
      :ok = NIF.put(store, "delete_me", "bye", 0)
      {:ok, true} = NIF.delete(store, "delete_me")

      assert {:ok, entries} = NIF.get_all_zero_copy(store)
      assert length(entries) == 1
      assert {"keep", "yes"} in entries
    end

    test "excludes expired entries (TTL)" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 1000
      future = System.os_time(:millisecond) + 60_000

      :ok = NIF.put(store, "expired", "gone", past)
      :ok = NIF.put(store, "live", "here", future)
      :ok = NIF.put(store, "no_ttl", "forever", 0)

      assert {:ok, entries} = NIF.get_all_zero_copy(store)
      keys = Enum.map(entries, fn {k, _} -> k end)
      refute "expired" in keys
      assert "live" in keys
      assert "no_ttl" in keys
    end

    test "returns latest value after overwrite" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "v1", 0)
      :ok = NIF.put(store, "k", "v2", 0)

      assert {:ok, entries} = NIF.get_all_zero_copy(store)
      assert length(entries) == 1
      assert {"k", "v2"} in entries
    end

    test "handles binary keys with null bytes" do
      %{store: store} = setup_store()
      key = <<0, 1, 2, 3>>
      val = <<255, 254, 253>>
      :ok = NIF.put(store, key, val, 0)

      assert {:ok, [{^key, ^val}]} = NIF.get_all_zero_copy(store)
    end
  end

  # ====================================================================
  # get_batch_zero_copy/2
  # ====================================================================

  describe "get_batch_zero_copy/2" do
    test "returns same results as regular get_batch" do
      %{store: store} = setup_store()

      for i <- 1..20 do
        :ok = NIF.put(store, "k#{i}", "v#{i}", 0)
      end

      keys = Enum.map(1..20, fn i -> "k#{i}" end)
      {:ok, regular} = NIF.get_batch(store, keys)
      {:ok, zero_copy} = NIF.get_batch_zero_copy(store, keys)

      assert regular == zero_copy
    end

    test "missing keys return nil" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "exists", "val", 0)

      {:ok, results} = NIF.get_batch_zero_copy(store, ["exists", "missing1", "missing2"])
      assert results == ["val", nil, nil]
    end

    test "mix of existing and missing keys" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "c", "3", 0)
      :ok = NIF.put(store, "e", "5", 0)

      {:ok, results} = NIF.get_batch_zero_copy(store, ["a", "b", "c", "d", "e"])
      assert results == ["1", nil, "3", nil, "5"]
    end

    test "empty keys list returns empty list" do
      %{store: store} = setup_store()
      {:ok, results} = NIF.get_batch_zero_copy(store, [])
      assert results == []
    end

    test "large batch (1000 keys)" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..1000, fn i ->
          {"k#{String.pad_leading("#{i}", 4, "0")}", "v#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      keys = Enum.map(1..1000, fn i -> "k#{String.pad_leading("#{i}", 4, "0")}" end)
      {:ok, results} = NIF.get_batch_zero_copy(store, keys)

      assert length(results) == 1000
      assert Enum.all?(results, &is_binary/1)

      # Verify first and last
      assert Enum.at(results, 0) == "v1"
      assert Enum.at(results, 999) == "v1000"
    end

    test "returned binaries are valid Elixir binaries" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "key", "hello world", 0)

      {:ok, [val]} = NIF.get_batch_zero_copy(store, ["key"])
      assert is_binary(val)
      assert val == "hello world"
      assert String.upcase(val) == "HELLO WORLD"
      assert byte_size(val) == 11
    end

    test "handles expired keys as nil" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "expired", "gone", past)
      :ok = NIF.put(store, "live", "here", 0)

      {:ok, results} = NIF.get_batch_zero_copy(store, ["expired", "live"])
      assert results == [nil, "here"]
    end

    test "all missing keys" do
      %{store: store} = setup_store()
      {:ok, results} = NIF.get_batch_zero_copy(store, ["a", "b", "c"])
      assert results == [nil, nil, nil]
    end

    test "large values in batch" do
      %{store: store} = setup_store()
      large_val = :binary.copy(<<0x42>>, 100_000)
      :ok = NIF.put(store, "big", large_val, 0)

      {:ok, [val]} = NIF.get_batch_zero_copy(store, ["big"])
      assert val == large_val
      assert byte_size(val) == 100_000
    end
  end

  # ====================================================================
  # get_range_zero_copy/4
  # ====================================================================

  describe "get_range_zero_copy/4" do
    test "returns same results as regular get_range" do
      %{store: store} = setup_store()

      for i <- 1..50 do
        key = "key_#{String.pad_leading("#{i}", 4, "0")}"
        :ok = NIF.put(store, key, "val_#{i}", 0)
      end

      {:ok, regular} = NIF.get_range(store, "key_0001", "key_0050", 100)
      {:ok, zero_copy} = NIF.get_range_zero_copy(store, "key_0001", "key_0050", 100)

      assert regular == zero_copy
    end

    test "empty range returns empty list" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "z", "2", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "m", "n", 100)
      assert entries == []
    end

    test "range with COUNT limit works" do
      %{store: store} = setup_store()

      for i <- 1..20 do
        key = "key_#{String.pad_leading("#{i}", 3, "0")}"
        :ok = NIF.put(store, key, "val_#{i}", 0)
      end

      {:ok, entries} = NIF.get_range_zero_copy(store, "key_001", "key_020", 5)
      assert length(entries) == 5
    end

    test "lexicographic ordering preserved" do
      %{store: store} = setup_store()

      # Insert in random order
      keys = ["cherry", "banana", "apple", "elderberry", "date"]

      for k <- keys do
        :ok = NIF.put(store, k, "val_#{k}", 0)
      end

      {:ok, entries} = NIF.get_range_zero_copy(store, "a", "z", 100)
      result_keys = Enum.map(entries, fn {k, _} -> k end)
      assert result_keys == Enum.sort(result_keys)
    end

    test "empty store returns empty list" do
      %{store: store} = setup_store()
      {:ok, entries} = NIF.get_range_zero_copy(store, "a", "z", 100)
      assert entries == []
    end

    test "single key in range" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "b", "2", 0)
      :ok = NIF.put(store, "d", "4", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "b", "b", 100)
      assert entries == [{"b", "2"}]
    end

    test "returned binaries are valid Elixir binaries" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "key", "value", 0)

      {:ok, [{k, v}]} = NIF.get_range_zero_copy(store, "key", "key", 10)
      assert is_binary(k)
      assert is_binary(v)
      assert k == "key"
      assert v == "value"
    end

    test "max_count of 0 returns empty list" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "key", "value", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "a", "z", 0)
      assert entries == []
    end

    test "excludes expired entries" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 1000

      :ok = NIF.put(store, "a", "live", 0)
      :ok = NIF.put(store, "b", "expired", past)
      :ok = NIF.put(store, "c", "live", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "a", "z", 100)
      keys = Enum.map(entries, fn {k, _} -> k end)
      assert "a" in keys
      refute "b" in keys
      assert "c" in keys
    end

    test "large values in range" do
      %{store: store} = setup_store()
      large_val = :binary.copy(<<0xCD>>, 100_000)

      for i <- 1..3 do
        :ok = NIF.put(store, "r#{i}", large_val, 0)
      end

      {:ok, entries} = NIF.get_range_zero_copy(store, "r1", "r3", 10)
      assert length(entries) == 3

      for {_k, v} <- entries do
        assert byte_size(v) == 100_000
        assert v == large_val
      end
    end
  end

  # ====================================================================
  # General: memory, concurrency, safety
  # ====================================================================

  describe "general safety" do
    test "results are usable after the NIF returns (no use-after-free)" do
      %{store: store} = setup_store()

      for i <- 1..100 do
        :ok = NIF.put(store, "k#{i}", "v#{i}", 0)
      end

      {:ok, all_entries} = NIF.get_all_zero_copy(store)
      {:ok, batch_results} = NIF.get_batch_zero_copy(store, Enum.map(1..100, &"k#{&1}"))
      {:ok, range_entries} = NIF.get_range_zero_copy(store, "k1", "k99", 200)

      # Force GC to potentially free any lingering resources
      :erlang.garbage_collect()

      # All results should still be valid and accessible
      assert length(all_entries) == 100
      assert length(batch_results) == 100
      assert length(range_entries) > 0

      for {k, v} <- all_entries do
        assert is_binary(k)
        assert is_binary(v)
        assert byte_size(k) > 0
        assert byte_size(v) > 0
      end

      for val <- batch_results do
        assert is_binary(val)
        assert byte_size(val) > 0
      end
    end

    test "concurrent bulk ops work correctly" do
      %{store: store} = setup_store()

      for i <- 1..200 do
        :ok = NIF.put(store, "k#{String.pad_leading("#{i}", 4, "0")}", "v#{i}", 0)
      end

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            {:ok, all} = NIF.get_all_zero_copy(store)
            assert length(all) == 200

            keys = Enum.map(1..50, &"k#{String.pad_leading("#{&1}", 4, "0")}")
            {:ok, batch} = NIF.get_batch_zero_copy(store, keys)
            assert length(batch) == 50

            {:ok, range} = NIF.get_range_zero_copy(store, "k0001", "k0100", 200)
            assert length(range) == 100

            :ok
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))
    end

    test "memory does not leak over repeated bulk ops" do
      %{store: store} = setup_store()

      for i <- 1..100 do
        :ok = NIF.put(store, "k#{i}", :binary.copy(<<0>>, 1_000), 0)
      end

      # Warm up and get baseline
      for _ <- 1..10 do
        {:ok, _} = NIF.get_all_zero_copy(store)
        {:ok, _} = NIF.get_batch_zero_copy(store, Enum.map(1..100, &"k#{&1}"))
        {:ok, _} = NIF.get_range_zero_copy(store, "k1", "k99", 200)
      end

      :erlang.garbage_collect()
      Process.sleep(50)

      {:memory, mem_before} = Process.info(self(), :memory)

      for _ <- 1..500 do
        {:ok, _} = NIF.get_all_zero_copy(store)
        {:ok, _} = NIF.get_batch_zero_copy(store, Enum.map(1..100, &"k#{&1}"))
        {:ok, _} = NIF.get_range_zero_copy(store, "k1", "k99", 200)
      end

      :erlang.garbage_collect()
      Process.sleep(50)

      {:memory, mem_after} = Process.info(self(), :memory)

      # Memory growth should be modest — certainly less than 50MB.
      # Without proper cleanup, 500 iterations * 100KB per call = ~50MB.
      growth_mb = (mem_after - mem_before) / (1024 * 1024)
      assert growth_mb < 50, "Memory grew by #{growth_mb} MB — possible leak"
    end

    test "binary operations work on zero-copy results" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "greeting", "Hello, World!", 0)

      {:ok, [{key, val}]} = NIF.get_all_zero_copy(store)

      # Pattern matching
      assert <<"gre", rest::binary>> = key
      assert rest == "eting"

      # String operations
      assert String.contains?(val, "World")
      assert String.split(val, ", ") == ["Hello", "World!"]
      assert String.replace(val, "World", "Elixir") == "Hello, Elixir!"

      # Binary operations
      assert :binary.match(val, "World") == {7, 5}
      assert :binary.part(val, 0, 5) == "Hello"

      # Enum/list operations on binary
      assert String.to_charlist(key) == ~c"greeting"
    end

    test "zero-copy results survive across process boundaries" do
      %{store: store} = setup_store()

      for i <- 1..10 do
        :ok = NIF.put(store, "k#{i}", "v#{i}", 0)
      end

      {:ok, entries} = NIF.get_all_zero_copy(store)

      # Send to another process and verify data integrity
      parent = self()

      spawn(fn ->
        for {k, v} <- entries do
          assert is_binary(k)
          assert is_binary(v)
        end

        send(parent, {:done, length(entries)})
      end)

      assert_receive {:done, 10}, 5000
    end
  end
end
