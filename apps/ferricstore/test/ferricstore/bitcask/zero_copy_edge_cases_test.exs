defmodule Ferricstore.Bitcask.ZeroCopyEdgeCasesTest do
  @moduledoc """
  Comprehensive edge-case tests for the five zero-copy / sendfile NIFs:

  1. `get_zero_copy/2`     — single-key ResourceBinary GET
  2. `get_all_zero_copy/1`  — bulk get-all via ResourceBinary
  3. `get_batch_zero_copy/2` — bulk batch get via ResourceBinary
  4. `get_range_zero_copy/4` — bulk range get via ResourceBinary
  5. `get_file_ref/2`       — returns {path, offset, size} for sendfile

  These tests exercise boundary conditions (max-size keys, 1-byte values,
  null-byte payloads, empty stores), concurrency, GC survival, cross-process
  send, and interoperability with `:binary` and `IO.iodata_to_binary`.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "zc_edge_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp setup_store do
    dir = tmp_dir()
    store = open_store(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{store: store, dir: dir}
  end

  # ====================================================================
  # get_zero_copy/2 edge cases
  # ====================================================================

  describe "get_zero_copy/2 — key size limits" do
    test "key with maximum allowed size (65535 bytes)" do
      %{store: store} = setup_store()
      max_key = :binary.copy(<<0x41>>, 65_535)
      :ok = NIF.put(store, max_key, "max_key_value", 0)

      assert {:ok, "max_key_value"} = NIF.get_zero_copy(store, max_key)
    end
  end

  describe "get_zero_copy/2 — value edge cases" do
    test "value that is exactly 1 byte" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "one_byte", "X", 0)

      {:ok, val} = NIF.get_zero_copy(store, "one_byte")
      assert val == "X"
      assert byte_size(val) == 1
    end

    test "empty value is treated as tombstone (returns nil)" do
      %{store: store} = setup_store()
      # In Bitcask, value_size == 0 is a tombstone sentinel.
      :ok = NIF.put(store, "empty_val", "", 0)

      assert {:ok, nil} = NIF.get_zero_copy(store, "empty_val")
      # Confirm regular get agrees
      assert {:ok, nil} = NIF.get(store, "empty_val")
    end

    test "binary value with all null bytes" do
      %{store: store} = setup_store()
      null_value = :binary.copy(<<0>>, 256)
      :ok = NIF.put(store, "null_bytes", null_value, 0)

      {:ok, val} = NIF.get_zero_copy(store, "null_bytes")
      assert val == null_value
      assert byte_size(val) == 256
      # Verify every byte is zero
      assert :binary.bin_to_list(val) == List.duplicate(0, 256)
    end
  end

  describe "get_zero_copy/2 — missing key" do
    test "key that does not exist returns {:ok, nil}" do
      %{store: store} = setup_store()
      assert {:ok, nil} = NIF.get_zero_copy(store, "does_not_exist")
    end
  end

  describe "get_zero_copy/2 — concurrency" do
    test "concurrent get_zero_copy from 50 tasks on same key" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "shared_key", "shared_value", 0)

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            {:ok, val} = NIF.get_zero_copy(store, "shared_key")
            assert val == "shared_value"
            :ok
          end)
        end

      results = Task.await_many(tasks, 15_000)
      assert Enum.all?(results, &(&1 == :ok))
    end
  end

  describe "get_zero_copy/2 — delete and overwrite" do
    test "get_zero_copy after delete returns nil" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "del_target", "before_delete", 0)
      assert {:ok, "before_delete"} = NIF.get_zero_copy(store, "del_target")

      {:ok, true} = NIF.delete(store, "del_target")
      assert {:ok, nil} = NIF.get_zero_copy(store, "del_target")
    end

    test "get_zero_copy after overwrite returns new value" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "overwrite_me", "version_1", 0)
      assert {:ok, "version_1"} = NIF.get_zero_copy(store, "overwrite_me")

      :ok = NIF.put(store, "overwrite_me", "version_2_longer", 0)
      assert {:ok, "version_2_longer"} = NIF.get_zero_copy(store, "overwrite_me")
    end
  end

  describe "get_zero_copy/2 — GC survival" do
    test "returned binary survives after multiple GC cycles" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "gc_survivor", "alive_after_gc", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "gc_survivor")

      # Force 3 GC cycles
      :erlang.garbage_collect()
      :erlang.garbage_collect()
      :erlang.garbage_collect()

      # Binary must still be valid and readable
      assert bin == "alive_after_gc"
      assert byte_size(bin) == 14
      assert String.valid?(bin)
    end
  end

  describe "get_zero_copy/2 — binary interoperability" do
    test "returned binary can be used in IO.iodata_to_binary" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "iodata_test", "hello", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "iodata_test")
      iodata = [bin, " ", "world"]
      assert IO.iodata_to_binary(iodata) == "hello world"
    end

    test "returned binary can be pattern matched" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "pattern_key", "FerricStore", 0)

      {:ok, <<"Ferric", rest::binary>>} = NIF.get_zero_copy(store, "pattern_key")
      assert rest == "Store"
    end

    test "get_zero_copy and regular get return identical binary content" do
      %{store: store} = setup_store()
      payload = :crypto.strong_rand_bytes(8192)
      :ok = NIF.put(store, "identity_check", payload, 0)

      {:ok, regular} = NIF.get(store, "identity_check")
      {:ok, zero_copy} = NIF.get_zero_copy(store, "identity_check")

      assert regular == zero_copy
      assert byte_size(regular) == byte_size(zero_copy)
      assert :crypto.hash(:sha256, regular) == :crypto.hash(:sha256, zero_copy)
    end

    test "binary from get_zero_copy can be sent to another process via send/2" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "cross_proc", "message_payload", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "cross_proc")
      parent = self()

      spawn(fn ->
        assert is_binary(bin)
        assert bin == "message_payload"
        assert byte_size(bin) == 15
        send(parent, {:received, bin})
      end)

      assert_receive {:received, received_bin}, 5_000
      assert received_bin == "message_payload"
    end

    test "binary from get_zero_copy works with :binary.match and :binary.split" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "binary_ops", "foo:bar:baz", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "binary_ops")

      # :binary.match
      assert :binary.match(bin, ":") == {3, 1}

      # :binary.split
      parts = :binary.split(bin, ":", [:global])
      assert parts == ["foo", "bar", "baz"]

      # :binary.part
      assert :binary.part(bin, 0, 3) == "foo"
      assert :binary.part(bin, 4, 3) == "bar"
    end
  end

  # ====================================================================
  # get_all_zero_copy/1 edge cases
  # ====================================================================

  describe "get_all_zero_copy/1 — empty and small stores" do
    test "store with 0 keys returns empty list" do
      %{store: store} = setup_store()
      assert {:ok, []} = NIF.get_all_zero_copy(store)
    end

    test "store with 1 key returns list with 1 element" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "only_key", "only_val", 0)

      assert {:ok, [{key, val}]} = NIF.get_all_zero_copy(store)
      assert key == "only_key"
      assert val == "only_val"
    end
  end

  describe "get_all_zero_copy/1 — large store" do
    test "store with 10000 keys — all returned correctly" do
      %{store: store} = setup_store()

      batch =
        Enum.map(1..10_000, fn i ->
          key = "k_#{String.pad_leading("#{i}", 5, "0")}"
          {key, "v_#{i}", 0}
        end)

      # Write in chunks to avoid huge batch
      batch
      |> Enum.chunk_every(1000)
      |> Enum.each(fn chunk -> :ok = NIF.put_batch(store, chunk) end)

      assert {:ok, entries} = NIF.get_all_zero_copy(store)
      assert length(entries) == 10_000

      # Spot-check some entries
      entry_map = Map.new(entries)
      assert entry_map["k_00001"] == "v_1"
      assert entry_map["k_05000"] == "v_5000"
      assert entry_map["k_10000"] == "v_10000"
    end
  end

  describe "get_all_zero_copy/1 — binary keys" do
    test "keys with null bytes" do
      %{store: store} = setup_store()
      key = <<0, 1, 2, 0, 3>>
      :ok = NIF.put(store, key, "null_key_val", 0)

      {:ok, entries} = NIF.get_all_zero_copy(store)
      assert length(entries) == 1
      assert {^key, "null_key_val"} = hd(entries)
    end

    test "keys with unicode characters" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "clef_de_test", "valeur", 0)

      {:ok, entries} = NIF.get_all_zero_copy(store)
      keys = Enum.map(entries, fn {k, _} -> k end)
      assert "clef_de_test" in keys
    end
  end

  describe "get_all_zero_copy/1 — expiry filtering" do
    test "mixed expired and live keys — only live keys returned" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 5_000
      future = System.os_time(:millisecond) + 60_000

      :ok = NIF.put(store, "expired_1", "gone_1", past)
      :ok = NIF.put(store, "expired_2", "gone_2", past)
      :ok = NIF.put(store, "live_1", "here_1", future)
      :ok = NIF.put(store, "live_2", "here_2", 0)

      {:ok, entries} = NIF.get_all_zero_copy(store)
      keys = Enum.map(entries, fn {k, _} -> k end)

      assert "live_1" in keys
      assert "live_2" in keys
      refute "expired_1" in keys
      refute "expired_2" in keys
      assert length(entries) == 2
    end
  end

  describe "get_all_zero_copy/1 — ordering" do
    test "result list ordering is deterministic across calls" do
      %{store: store} = setup_store()

      for i <- 1..50 do
        :ok = NIF.put(store, "det_#{String.pad_leading("#{i}", 3, "0")}", "v#{i}", 0)
      end

      {:ok, entries_1} = NIF.get_all_zero_copy(store)
      {:ok, entries_2} = NIF.get_all_zero_copy(store)
      {:ok, entries_3} = NIF.get_all_zero_copy(store)

      # The ordering should be identical across multiple calls
      assert entries_1 == entries_2
      assert entries_2 == entries_3
    end
  end

  describe "get_all_zero_copy/1 — concurrency" do
    test "concurrent get_all_zero_copy from 10 tasks" do
      %{store: store} = setup_store()

      for i <- 1..100 do
        :ok = NIF.put(store, "conc_#{i}", "val_#{i}", 0)
      end

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            {:ok, entries} = NIF.get_all_zero_copy(store)
            assert length(entries) == 100
            :ok
          end)
        end

      results = Task.await_many(tasks, 15_000)
      assert Enum.all?(results, &(&1 == :ok))
    end
  end

  # ====================================================================
  # get_batch_zero_copy/2 edge cases
  # ====================================================================

  describe "get_batch_zero_copy/2 — empty and boundary" do
    test "empty key list returns empty list" do
      %{store: store} = setup_store()
      {:ok, results} = NIF.get_batch_zero_copy(store, [])
      assert results == []
    end

    test "all keys missing — list of nils" do
      %{store: store} = setup_store()
      {:ok, results} = NIF.get_batch_zero_copy(store, ["no_1", "no_2", "no_3", "no_4", "no_5"])
      assert results == [nil, nil, nil, nil, nil]
    end
  end

  describe "get_batch_zero_copy/2 — duplicate keys" do
    test "duplicate keys in batch — each returns independently" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "dup_key", "dup_value", 0)

      {:ok, results} = NIF.get_batch_zero_copy(store, ["dup_key", "dup_key", "dup_key"])
      assert length(results) == 3
      assert Enum.all?(results, &(&1 == "dup_value"))
    end
  end

  describe "get_batch_zero_copy/2 — large batch with missing" do
    test "batch of 1000 keys with 500 missing" do
      %{store: store} = setup_store()

      # Insert only even-numbered keys
      batch =
        Enum.map(1..500, fn i ->
          {"batch_#{String.pad_leading("#{i * 2}", 4, "0")}", "val_#{i * 2}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      # Request all 1000 keys (1-1000)
      all_keys =
        Enum.map(1..1000, fn i ->
          "batch_#{String.pad_leading("#{i}", 4, "0")}"
        end)

      {:ok, results} = NIF.get_batch_zero_copy(store, all_keys)
      assert length(results) == 1000

      # Count present vs nil
      present_count = Enum.count(results, &(not is_nil(&1)))
      nil_count = Enum.count(results, &is_nil/1)

      assert present_count == 500
      assert nil_count == 500

      # Verify even-numbered positions have values
      for i <- 1..1000 do
        val = Enum.at(results, i - 1)

        if rem(i, 2) == 0 do
          assert val == "val_#{i}", "Expected val_#{i} at position #{i - 1}, got #{inspect(val)}"
        else
          assert val == nil, "Expected nil at position #{i - 1}, got #{inspect(val)}"
        end
      end
    end
  end

  describe "get_batch_zero_copy/2 — deleted keys" do
    test "keys that were just deleted return nil" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "del_a", "va", 0)
      :ok = NIF.put(store, "del_b", "vb", 0)
      :ok = NIF.put(store, "keep_c", "vc", 0)

      {:ok, true} = NIF.delete(store, "del_a")
      {:ok, true} = NIF.delete(store, "del_b")

      {:ok, results} = NIF.get_batch_zero_copy(store, ["del_a", "del_b", "keep_c"])
      assert results == [nil, nil, "vc"]
    end
  end

  # ====================================================================
  # get_range_zero_copy/4 edge cases
  # ====================================================================

  describe "get_range_zero_copy/4 — inverted range" do
    test "min_key > max_key returns empty list" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "alpha", "1", 0)
      :ok = NIF.put(store, "bravo", "2", 0)
      :ok = NIF.put(store, "charlie", "3", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "zulu", "alpha", 100)
      assert entries == []
    end
  end

  describe "get_range_zero_copy/4 — point range" do
    test "min_key == max_key returns that key if exists" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "exact", "found_it", 0)
      :ok = NIF.put(store, "other", "not_this", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "exact", "exact", 100)
      assert entries == [{"exact", "found_it"}]
    end

    test "min_key == max_key for nonexistent key returns empty" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "exists", "yes", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "ghost", "ghost", 100)
      assert entries == []
    end
  end

  describe "get_range_zero_copy/4 — max_count boundaries" do
    test "max_count = 0 returns empty list" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "a", "z", 0)
      assert entries == []
    end

    test "max_count = 1 returns only first key in range" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "r_a", "1", 0)
      :ok = NIF.put(store, "r_b", "2", 0)
      :ok = NIF.put(store, "r_c", "3", 0)

      {:ok, entries} = NIF.get_range_zero_copy(store, "r_a", "r_z", 1)
      assert length(entries) == 1
      [{first_key, _}] = entries
      assert first_key == "r_a"
    end
  end

  describe "get_range_zero_copy/4 — full range" do
    test "range spanning all keys returns everything" do
      %{store: store} = setup_store()

      for i <- 1..100 do
        key = "range_#{String.pad_leading("#{i}", 3, "0")}"
        :ok = NIF.put(store, key, "val_#{i}", 0)
      end

      # Use a range that encompasses all keys
      {:ok, entries} =
        NIF.get_range_zero_copy(store, "range_000", "range_999", 1000)

      assert length(entries) == 100

      # Verify sorted order
      keys = Enum.map(entries, fn {k, _} -> k end)
      assert keys == Enum.sort(keys)
    end
  end

  # ====================================================================
  # get_file_ref/2 edge cases
  # ====================================================================

  describe "get_file_ref/2 — basic behavior" do
    test "key on disk returns {path, offset, size} tuple" do
      %{store: store, dir: dir} = setup_store()
      value = "file_ref_value_data"
      :ok = NIF.put(store, "disk_key", value, 0)

      {:ok, {path, offset, size}} = NIF.get_file_ref(store, "disk_key")

      assert is_binary(path)
      assert String.starts_with?(path, dir)
      assert is_integer(offset)
      assert offset > 0
      assert size == byte_size(value)
    end

    test "nonexistent key returns {:ok, nil}" do
      %{store: store} = setup_store()
      assert {:ok, nil} = NIF.get_file_ref(store, "no_such_key")
    end
  end

  describe "get_file_ref/2 — expired key" do
    test "expired key returns {:ok, nil}" do
      %{store: store} = setup_store()
      past = System.os_time(:millisecond) - 5_000
      :ok = NIF.put(store, "expired_ref", "gone", past)

      assert {:ok, nil} = NIF.get_file_ref(store, "expired_ref")
    end
  end

  describe "get_file_ref/2 — deleted key" do
    test "deleted key returns {:ok, nil}" do
      %{store: store} = setup_store()
      :ok = NIF.put(store, "del_ref", "will_be_deleted", 0)
      {:ok, true} = NIF.delete(store, "del_ref")

      assert {:ok, nil} = NIF.get_file_ref(store, "del_ref")
    end
  end

  describe "get_file_ref/2 — data verification" do
    test "returned offset + size correctly points to value bytes" do
      %{store: store} = setup_store()
      value = "verify_this_value_data_is_correct_on_disk"
      :ok = NIF.put(store, "verify_ref", value, 0)

      {:ok, {path, offset, size}} = NIF.get_file_ref(store, "verify_ref")

      # Read the file directly and extract the value using the returned ref
      file_data = File.read!(path)
      extracted = binary_part(file_data, offset, size)
      assert extracted == value
    end

    test "file ref is valid after multiple writes" do
      %{store: store} = setup_store()

      # Write multiple keys to push the offset forward
      for i <- 1..50 do
        :ok = NIF.put(store, "prefix_#{i}", "value_#{i}", 0)
      end

      target_value = "the_target_value_we_want"
      :ok = NIF.put(store, "target_key", target_value, 0)

      {:ok, {path, offset, size}} = NIF.get_file_ref(store, "target_key")

      # Verify by reading the file directly
      file_data = File.read!(path)
      extracted = binary_part(file_data, offset, size)
      assert extracted == target_value
    end

    test "file ref for large value has correct size" do
      %{store: store} = setup_store()
      large_value = :crypto.strong_rand_bytes(100_000)
      :ok = NIF.put(store, "large_ref", large_value, 0)

      {:ok, {path, offset, size}} = NIF.get_file_ref(store, "large_ref")
      assert size == 100_000

      # Verify the data on disk
      file_data = File.read!(path)
      extracted = binary_part(file_data, offset, size)
      assert extracted == large_value
    end
  end

  # ====================================================================
  # Cross-NIF consistency: zero-copy vs regular
  # ====================================================================

  describe "cross-NIF consistency" do
    test "get_all_zero_copy matches get_all for mixed dataset" do
      %{store: store} = setup_store()

      for i <- 1..200 do
        :ok = NIF.put(store, "consist_#{String.pad_leading("#{i}", 3, "0")}", "val_#{i}", 0)
      end

      {:ok, regular} = NIF.get_all(store)
      {:ok, zero_copy} = NIF.get_all_zero_copy(store)

      assert length(regular) == length(zero_copy)
      assert Enum.sort(regular) == Enum.sort(zero_copy)
    end

    test "get_batch_zero_copy matches get_batch for mixed present/absent keys" do
      %{store: store} = setup_store()

      for i <- 1..50 do
        :ok = NIF.put(store, "bm_#{i}", "val_#{i}", 0)
      end

      keys = Enum.map(1..100, &"bm_#{&1}")
      {:ok, regular} = NIF.get_batch(store, keys)
      {:ok, zero_copy} = NIF.get_batch_zero_copy(store, keys)

      assert regular == zero_copy
    end

    test "get_range_zero_copy matches get_range for full scan" do
      %{store: store} = setup_store()

      for i <- 1..100 do
        key = "rm_#{String.pad_leading("#{i}", 3, "0")}"
        :ok = NIF.put(store, key, "val_#{i}", 0)
      end

      {:ok, regular} = NIF.get_range(store, "rm_001", "rm_100", 200)
      {:ok, zero_copy} = NIF.get_range_zero_copy(store, "rm_001", "rm_100", 200)

      assert regular == zero_copy
    end
  end
end
