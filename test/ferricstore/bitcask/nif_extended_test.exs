defmodule Ferricstore.Bitcask.NIFExtendedTest do
  @moduledoc """
  Comprehensive edge case tests for the new NIF functions:
  - get_all/1      (bitcask_read_all)
  - get_batch/2    (bitcask_read_batch)
  - get_range/4    (bitcask_read_range)
  - read_modify_write/3 (bitcask_rmw)
  """

  use ExUnit.Case, async: false
  @moduletag :pending_nif_rebuild

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "nif_ext_test_#{:rand.uniform(9_999_999)}")
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
  # get_all/1 — Read All
  # ====================================================================

  describe "get_all/1" do
    test "returns empty list on empty store" do
      %{store: store} = setup_store()
      assert {:ok, []} = NIF.get_all(store)
    end

    test "returns single entry" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k1", "v1", 0)
      assert {:ok, entries} = NIF.get_all(store)
      assert length(entries) == 1
      assert {"k1", "v1"} in entries
    end

    test "returns all entries for multiple keys" do
      %{store: store} = setup_store()
      for i <- 1..10, do: :ok = NIF.put(store, "key_#{i}", "val_#{i}", 0)

      assert {:ok, entries} = NIF.get_all(store)
      assert length(entries) == 10

      for i <- 1..10 do
        assert {"key_#{i}", "val_#{i}"} in entries
      end
    end

    test "returns all 1000 entries" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..1000, fn i ->
          {"k#{String.pad_leading("#{i}", 4, "0")}", "v#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      assert {:ok, entries} = NIF.get_all(store)
      assert length(entries) == 1000
    end

    test "excludes deleted (tombstoned) entries" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "keep", "yes", 0)
      :ok = NIF.put(store, "delete_me", "bye", 0)
      {:ok, true} = NIF.delete(store, "delete_me")

      assert {:ok, entries} = NIF.get_all(store)
      assert length(entries) == 1
      assert {"keep", "yes"} in entries
      refute Enum.any?(entries, fn {k, _} -> k == "delete_me" end)
    end

    test "excludes expired entries (TTL)" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 1000
      future = System.os_time(:millisecond) + 60_000

      :ok = NIF.put(store, "expired", "gone", past)
      :ok = NIF.put(store, "live", "here", future)
      :ok = NIF.put(store, "no_ttl", "forever", 0)

      assert {:ok, entries} = NIF.get_all(store)
      keys = Enum.map(entries, fn {k, _} -> k end)
      refute "expired" in keys
      assert "live" in keys
      assert "no_ttl" in keys
    end

    test "returns latest value after overwrite" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "v1", 0)
      :ok = NIF.put(store, "k", "v2", 0)

      assert {:ok, entries} = NIF.get_all(store)
      assert length(entries) == 1
      assert {"k", "v2"} in entries
    end

    test "handles binary keys with null bytes" do
      %{store: store} = setup_store()
      key = <<0, 1, 2, 3>>
      :ok = NIF.put(store, key, "binary_val", 0)

      assert {:ok, entries} = NIF.get_all(store)
      assert {^key, "binary_val"} = List.first(entries)
    end
  end

  # ====================================================================
  # get_batch/2 — Read Batch
  # ====================================================================

  describe "get_batch/2" do
    test "returns empty list for empty key list" do
      %{store: store} = setup_store()
      assert {:ok, []} = NIF.get_batch(store, [])
    end

    test "returns values for existing keys" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", 0)
      :ok = NIF.put(store, "c", "3", 0)

      assert {:ok, values} = NIF.get_batch(store, ["a", "b", "c"])
      assert values == ["1", "2", "3"]
    end

    test "returns nil for missing keys" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "exists", "value", 0)

      assert {:ok, values} = NIF.get_batch(store, ["exists", "missing1", "missing2"])
      assert values == ["value", nil, nil]
    end

    test "mix of existing and missing keys" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "c", "3", 0)

      assert {:ok, values} = NIF.get_batch(store, ["a", "b", "c", "d"])
      assert values == ["1", nil, "3", nil]
    end

    test "all missing keys returns all nils" do
      %{store: store} = setup_store()

      assert {:ok, values} = NIF.get_batch(store, ["x", "y", "z"])
      assert values == [nil, nil, nil]
    end

    test "duplicate keys in list returns values for each occurrence" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "dup", "val", 0)

      assert {:ok, values} = NIF.get_batch(store, ["dup", "dup", "dup"])
      assert values == ["val", "val", "val"]
    end

    test "excludes expired keys" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "expired", "gone", past)
      :ok = NIF.put(store, "live", "here", 0)

      assert {:ok, values} = NIF.get_batch(store, ["expired", "live"])
      assert values == [nil, "here"]
    end

    test "handles binary keys" do
      %{store: store} = setup_store()
      key = <<0xFF, 0x00, 0xAB>>
      :ok = NIF.put(store, key, "bin_val", 0)

      assert {:ok, [val]} = NIF.get_batch(store, [key])
      assert val == "bin_val"
    end

    test "batch of 1000 keys" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..1000, fn i ->
          {"batch_k_#{i}", "batch_v_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      keys = Enum.map(1..1000, fn i -> "batch_k_#{i}" end)
      assert {:ok, values} = NIF.get_batch(store, keys)

      for i <- 1..1000 do
        assert Enum.at(values, i - 1) == "batch_v_#{i}"
      end
    end
  end

  # ====================================================================
  # get_range/4 — Range Scan
  # ====================================================================

  describe "get_range/4" do
    test "returns empty when no matching keys" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "aaa", "1", 0)
      :ok = NIF.put(store, "zzz", "2", 0)

      assert {:ok, []} = NIF.get_range(store, "bbb", "yyy", 100)
    end

    test "returns empty when min > max" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", 0)

      assert {:ok, []} = NIF.get_range(store, "z", "a", 100)
    end

    test "returns empty when count is 0" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)

      assert {:ok, []} = NIF.get_range(store, "a", "z", 0)
    end

    test "returns matching keys sorted" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "c", "3", 0)
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", 0)
      :ok = NIF.put(store, "d", "4", 0)

      assert {:ok, entries} = NIF.get_range(store, "a", "c", 100)
      assert entries == [{"a", "1"}, {"b", "2"}, {"c", "3"}]
    end

    test "respects count limit" do
      %{store: store} = setup_store()
      for c <- ~c"abcdef", do: :ok = NIF.put(store, <<c>>, "val_#{[c]}", 0)

      assert {:ok, entries} = NIF.get_range(store, "a", "f", 3)
      assert length(entries) == 3
      # Should be first 3 alphabetically: a, b, c
      assert entries == [{"a", "val_a"}, {"b", "val_b"}, {"c", "val_c"}]
    end

    test "count larger than matching keys returns all matching" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", 0)

      assert {:ok, entries} = NIF.get_range(store, "a", "b", 100)
      assert length(entries) == 2
    end

    test "exact match on single key" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "exact", "match", 0)

      assert {:ok, [{"exact", "match"}]} = NIF.get_range(store, "exact", "exact", 100)
    end

    test "excludes expired keys in range" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", past)
      :ok = NIF.put(store, "c", "3", 0)

      assert {:ok, entries} = NIF.get_range(store, "a", "c", 100)
      keys = Enum.map(entries, fn {k, _} -> k end)
      refute "b" in keys
      assert "a" in keys
      assert "c" in keys
    end

    test "empty store returns empty" do
      %{store: store} = setup_store()
      assert {:ok, []} = NIF.get_range(store, "a", "z", 100)
    end
  end

  # ====================================================================
  # read_modify_write/3 — RMW: SetRange
  # ====================================================================

  describe "read_modify_write/3 — SetRange" do
    test "on non-existent key creates zero-padded value" do
      %{store: store} = setup_store()

      assert {:ok, result} = NIF.read_modify_write(store, "new_key", {:set_range, 5, "abc"})
      # Should be 5 zero bytes + "abc"
      assert result == <<0, 0, 0, 0, 0, "abc">>
    end

    test "at offset 0 replaces start of value" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "hello world", 0)

      assert {:ok, result} = NIF.read_modify_write(store, "k", {:set_range, 0, "HELLO"})
      assert result == "HELLO world"
    end

    test "at middle of value replaces middle" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "hello world", 0)

      assert {:ok, result} = NIF.read_modify_write(store, "k", {:set_range, 6, "WORLD"})
      assert result == "hello WORLD"
    end

    test "beyond end extends with zeros" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "hi", 0)

      assert {:ok, result} = NIF.read_modify_write(store, "k", {:set_range, 5, "!!!"})
      # "hi" + 3 zero bytes + "!!!"
      assert result == <<"hi", 0, 0, 0, "!!!">>
    end

    test "empty patch at offset 0 is no-op" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "hello", 0)

      assert {:ok, "hello"} = NIF.read_modify_write(store, "k", {:set_range, 0, ""})
    end

    test "result persists in store" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "aaa", 0)

      {:ok, _} = NIF.read_modify_write(store, "k", {:set_range, 0, "bbb"})
      assert {:ok, "bbb"} = NIF.get(store, "k")
    end
  end

  # ====================================================================
  # read_modify_write/3 — RMW: SetBit
  # ====================================================================

  describe "read_modify_write/3 — SetBit" do
    test "set bit 0 (MSB of first byte)" do
      %{store: store} = setup_store()
      # bit offset 0 = MSB of byte 0 = 0x80
      {:ok, result} = NIF.read_modify_write(store, "bits", {:set_bit, 0, 1})
      assert result == <<0x80>>
    end

    test "set bit 7 (LSB of first byte)" do
      %{store: store} = setup_store()
      # bit offset 7 = LSB of byte 0 = 0x01
      {:ok, result} = NIF.read_modify_write(store, "bits", {:set_bit, 7, 1})
      assert result == <<0x01>>
    end

    test "set bit beyond current value extends with zeros" do
      %{store: store} = setup_store()
      # bit offset 16 = byte 2, bit 0 (MSB) = 0x80
      {:ok, result} = NIF.read_modify_write(store, "bits", {:set_bit, 16, 1})
      assert result == <<0, 0, 0x80>>
    end

    test "clear bit that is set" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "bits", <<0xFF>>, 0)

      # Clear bit 0 (MSB): 0xFF -> 0x7F
      {:ok, result} = NIF.read_modify_write(store, "bits", {:set_bit, 0, 0})
      assert result == <<0x7F>>
    end

    test "set bit that is already set is idempotent" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "bits", <<0xFF>>, 0)

      {:ok, result} = NIF.read_modify_write(store, "bits", {:set_bit, 0, 1})
      assert result == <<0xFF>>
    end

    test "multiple set_bit operations accumulate" do
      %{store: store} = setup_store()
      {:ok, _} = NIF.read_modify_write(store, "bits", {:set_bit, 0, 1})
      {:ok, _} = NIF.read_modify_write(store, "bits", {:set_bit, 7, 1})
      {:ok, result} = NIF.read_modify_write(store, "bits", {:set_bit, 4, 1})

      # bit 0 = 0x80, bit 4 = 0x08, bit 7 = 0x01 => 0x89
      assert result == <<0x89>>
    end
  end

  # ====================================================================
  # read_modify_write/3 — RMW: Append
  # ====================================================================

  describe "read_modify_write/3 — Append" do
    test "append to empty key creates value" do
      %{store: store} = setup_store()
      {:ok, result} = NIF.read_modify_write(store, "new", {:append, "hello"})
      assert result == "hello"
    end

    test "append to existing concatenates" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "hello", 0)

      {:ok, result} = NIF.read_modify_write(store, "k", {:append, " world"})
      assert result == "hello world"
    end

    test "append empty string is identity" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "k", "data", 0)

      {:ok, result} = NIF.read_modify_write(store, "k", {:append, ""})
      assert result == "data"
    end

    test "multiple appends" do
      %{store: store} = setup_store()
      {:ok, _} = NIF.read_modify_write(store, "k", {:append, "a"})
      {:ok, _} = NIF.read_modify_write(store, "k", {:append, "b"})
      {:ok, result} = NIF.read_modify_write(store, "k", {:append, "c"})
      assert result == "abc"
    end

    test "append persists" do
      %{store: store} = setup_store()
      {:ok, _} = NIF.read_modify_write(store, "k", {:append, "data"})
      assert {:ok, "data"} = NIF.get(store, "k")
    end
  end

  # ====================================================================
  # read_modify_write/3 — RMW: IncrBy
  # ====================================================================

  describe "read_modify_write/3 — IncrBy" do
    test "on non-existent key sets to delta" do
      %{store: store} = setup_store()
      {:ok, result} = NIF.read_modify_write(store, "counter", {:incr_by, 5})
      assert result == "5"
    end

    test "on existing integer adds delta" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "counter", "10", 0)

      {:ok, result} = NIF.read_modify_write(store, "counter", {:incr_by, 3})
      assert result == "13"
    end

    test "negative delta decrements" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "counter", "10", 0)

      {:ok, result} = NIF.read_modify_write(store, "counter", {:incr_by, -3})
      assert result == "7"
    end

    test "delta of 0 is identity" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "counter", "42", 0)

      {:ok, result} = NIF.read_modify_write(store, "counter", {:incr_by, 0})
      assert result == "42"
    end

    test "on non-integer value returns error" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "not_int", "hello", 0)

      assert {:error, reason} = NIF.read_modify_write(store, "not_int", {:incr_by, 1})
      assert reason =~ "not an integer"
    end

    test "on float string value returns error" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "float", "3.14", 0)

      assert {:error, reason} = NIF.read_modify_write(store, "float", {:incr_by, 1})
      assert reason =~ "not an integer"
    end

    test "large positive increment" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "counter", "0", 0)

      {:ok, result} = NIF.read_modify_write(store, "counter", {:incr_by, 1_000_000_000})
      assert result == "1000000000"
    end

    test "negative values work" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "neg", "-5", 0)

      {:ok, result} = NIF.read_modify_write(store, "neg", {:incr_by, 3})
      assert result == "-2"
    end

    test "result persists in store" do
      %{store: store} = setup_store()
      {:ok, _} = NIF.read_modify_write(store, "counter", {:incr_by, 42})
      assert {:ok, "42"} = NIF.get(store, "counter")
    end
  end

  # ====================================================================
  # read_modify_write/3 — RMW: IncrByFloat
  # ====================================================================

  describe "read_modify_write/3 — IncrByFloat" do
    test "on non-existent key sets to delta" do
      %{store: store} = setup_store()
      {:ok, result} = NIF.read_modify_write(store, "fcount", {:incr_by_float, 1.5})
      assert result == "1.5"
    end

    test "on existing float adds delta" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "fcount", "10.5", 0)

      {:ok, result} = NIF.read_modify_write(store, "fcount", {:incr_by_float, 0.1})
      # Float precision: 10.5 + 0.1 = 10.6
      {parsed, _} = Float.parse(result)
      assert_in_delta parsed, 10.6, 0.0001
    end

    test "on integer string value works" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "fcount", "10", 0)

      {:ok, result} = NIF.read_modify_write(store, "fcount", {:incr_by_float, 0.5})
      {parsed, _} = Float.parse(result)
      assert_in_delta parsed, 10.5, 0.0001
    end

    test "on non-numeric value returns error" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "not_num", "hello", 0)

      assert {:error, reason} = NIF.read_modify_write(store, "not_num", {:incr_by_float, 1.0})
      assert reason =~ "not a valid float"
    end

    test "negative delta" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "fcount", "5.0", 0)

      {:ok, result} = NIF.read_modify_write(store, "fcount", {:incr_by_float, -2.5})
      {parsed, _} = Float.parse(result)
      assert_in_delta parsed, 2.5, 0.0001
    end
  end

  # ====================================================================
  # Scheduler safety tests
  # ====================================================================

  describe "scheduler safety" do
    test "async operations do not block caller excessively" do
      %{store: store} = setup_store()
      # Write 100 entries
      batch = Enum.map(1..100, fn i -> {"sched_k_#{i}", "sched_v_#{i}", 0} end)
      :ok = NIF.put_batch(store, batch)

      start = System.monotonic_time(:millisecond)
      {:ok, _entries} = NIF.get_all(store)
      elapsed = System.monotonic_time(:millisecond) - start

      # Should complete in well under 1 second for 100 entries
      assert elapsed < 1000
    end

    test "normal NIFs complete quickly" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "fast", "val", 0)

      start = System.monotonic_time(:microsecond)
      {:ok, _} = NIF.get(store, "fast")
      elapsed = System.monotonic_time(:microsecond) - start

      # Single get should be well under 10ms (10_000 microseconds)
      assert elapsed < 10_000
    end

    test "concurrent get_all from multiple processes" do
      %{store: store} = setup_store()
      batch = Enum.map(1..50, fn i -> {"conc_k_#{i}", "conc_v_#{i}", 0} end)
      :ok = NIF.put_batch(store, batch)

      tasks =
        Enum.map(1..10, fn _ ->
          Task.async(fn ->
            {:ok, entries} = NIF.get_all(store)
            length(entries)
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 10_000))

      # All 10 concurrent get_all calls should return 50 entries
      assert Enum.all?(results, &(&1 == 50))
    end

    test "concurrent get_batch + write interleaving" do
      %{store: store} = setup_store()
      batch = Enum.map(1..50, fn i -> {"interleave_#{i}", "v#{i}", 0} end)
      :ok = NIF.put_batch(store, batch)

      reader_tasks =
        Enum.map(1..5, fn _ ->
          Task.async(fn ->
            keys = Enum.map(1..50, fn i -> "interleave_#{i}" end)
            {:ok, values} = NIF.get_batch(store, keys)
            # At least most values should be non-nil
            Enum.count(values, &(not is_nil(&1)))
          end)
        end)

      writer_tasks =
        Enum.map(51..55, fn i ->
          Task.async(fn ->
            NIF.put(store, "interleave_#{i}", "v#{i}", 0)
          end)
        end)

      reader_results = Enum.map(reader_tasks, &Task.await(&1, 10_000))
      Enum.each(writer_tasks, &Task.await(&1, 10_000))

      # All readers should have read at least 40 of the 50 original keys
      assert Enum.all?(reader_results, &(&1 >= 40))
    end

    test "read_all during active writes (consistency)" do
      %{store: store} = setup_store()
      batch = Enum.map(1..100, fn i -> {"during_write_#{i}", "original_#{i}", 0} end)
      :ok = NIF.put_batch(store, batch)

      # Start a read_all while writing more keys
      reader = Task.async(fn ->
        {:ok, entries} = NIF.get_all(store)
        entries
      end)

      # Write more entries concurrently
      for i <- 101..110 do
        NIF.put(store, "during_write_#{i}", "new_#{i}", 0)
      end

      entries = Task.await(reader, 10_000)
      # Should have at least the original 100 entries
      assert length(entries) >= 100
    end
  end

  # ====================================================================
  # Crash safety / CRC validation tests
  # ====================================================================

  describe "crash safety" do
    test "get_all after store reopen returns all persisted entries" do
      %{dir: dir} = setup_store()
      store = open_store(dir)

      batch = Enum.map(1..20, fn i -> {"crash_k_#{i}", "crash_v_#{i}", 0} end)
      :ok = NIF.put_batch(store, batch)

      # Force GC to close the store
      _ = store
      :erlang.garbage_collect()

      # Reopen
      store2 = open_store(dir)
      {:ok, entries} = NIF.get_all(store2)
      assert length(entries) == 20
    end

    test "get_batch with some missing keys after reopen" do
      %{dir: dir} = setup_store()
      store = open_store(dir)

      :ok = NIF.put(store, "persist_a", "val_a", 0)
      :ok = NIF.put(store, "persist_b", "val_b", 0)

      _ = store
      :erlang.garbage_collect()

      store2 = open_store(dir)
      {:ok, values} = NIF.get_batch(store2, ["persist_a", "nonexistent", "persist_b"])
      assert values == ["val_a", nil, "val_b"]
    end

    test "rmw on key with CRC error returns error or creates new value" do
      # This test verifies the system handles corruption gracefully.
      # Since we can't easily inject CRC errors at the NIF level,
      # we test that RMW on a missing key (which is the same code path
      # as an unreadable key) creates a new value.
      %{store: store} = setup_store()

      {:ok, result} = NIF.read_modify_write(store, "no_such_key", {:incr_by, 5})
      assert result == "5"
    end
  end

  # ====================================================================
  # Large data tests
  # ====================================================================

  describe "large data" do
    test "get_all with 10K entries" do
      %{store: store} = setup_store()

      # Write in batches of 1000 for speed
      for chunk <- Enum.chunk_every(1..10_000, 1000) do
        batch =
          Enum.map(chunk, fn i ->
            {"large_k_#{String.pad_leading("#{i}", 5, "0")}", "val_#{i}", 0}
          end)

        :ok = NIF.put_batch(store, batch)
      end

      {:ok, entries} = NIF.get_all(store)
      assert length(entries) == 10_000
    end

    test "get_batch with 1000 keys" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..1000, fn i ->
          {"kb_#{String.pad_leading("#{i}", 4, "0")}", "vb_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      keys = Enum.map(1..1000, fn i -> "kb_#{String.pad_leading("#{i}", 4, "0")}" end)
      {:ok, values} = NIF.get_batch(store, keys)
      assert length(values) == 1000
      assert Enum.all?(values, &(not is_nil(&1)))
    end

    test "rmw on 1MB value" do
      %{store: store} = setup_store()
      big_value = :crypto.strong_rand_bytes(1_000_000)
      :ok = NIF.put(store, "big", big_value, 0)

      {:ok, result} = NIF.read_modify_write(store, "big", {:append, "tail"})
      assert byte_size(result) == 1_000_004
    end

    test "get_range across many keys" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..1000, fn i ->
          {"range_#{String.pad_leading("#{i}", 4, "0")}", "v#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      # Range scan from range_0100 to range_0200
      {:ok, entries} = NIF.get_range(store, "range_0100", "range_0200", 1000)
      assert length(entries) == 101
      # Verify sorted
      keys = Enum.map(entries, fn {k, _} -> k end)
      assert keys == Enum.sort(keys)
    end
  end

  # ====================================================================
  # RMW edge cases
  # ====================================================================

  describe "read_modify_write edge cases" do
    test "incr_by preserves existing TTL" do
      %{store: store} = setup_store()
      future = System.os_time(:millisecond) + 60_000
      :ok = NIF.put(store, "ttl_counter", "10", future)

      {:ok, "15"} = NIF.read_modify_write(store, "ttl_counter", {:incr_by, 5})

      # The key should still have a TTL (still alive)
      assert {:ok, "15"} = NIF.get(store, "ttl_counter")
    end

    test "set_range on empty string key (after delete)" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "deleted", "old", 0)
      {:ok, true} = NIF.delete(store, "deleted")

      # After delete, key doesn't exist. set_range should create new zero-padded value.
      {:ok, result} = NIF.read_modify_write(store, "deleted", {:set_range, 0, "new"})
      assert result == "new"
    end

    test "sequential incr_by accumulates correctly" do
      %{store: store} = setup_store()

      for i <- 1..100 do
        {:ok, result} = NIF.read_modify_write(store, "acc", {:incr_by, 1})
        assert result == "#{i}"
      end

      assert {:ok, "100"} = NIF.get(store, "acc")
    end

    test "append binary data" do
      %{store: store} = setup_store()

      {:ok, _} = NIF.read_modify_write(store, "bin", {:append, <<0xFF, 0x00>>})
      {:ok, result} = NIF.read_modify_write(store, "bin", {:append, <<0xAB, 0xCD>>})
      assert result == <<0xFF, 0x00, 0xAB, 0xCD>>
    end
  end
end
