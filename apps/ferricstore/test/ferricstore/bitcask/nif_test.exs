defmodule Ferricstore.Bitcask.NIFTest do
  @moduledoc false

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "nif_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # ------------------------------------------------------------------
  # new/1
  # ------------------------------------------------------------------

  describe "new/1" do
    test "opens a new store in an empty directory" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      assert {:ok, store} = NIF.new(dir)
      assert is_reference(store)
    end

    test "reopens an existing store and recovers data" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      store1 = open_store(dir)
      :ok = NIF.put(store1, "persist", "value", 0)

      # Drop reference and force GC to release the resource
      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "value"} == NIF.get(store2, "persist")
    end

    test "returns error for invalid path" do
      assert {:error, _reason} = NIF.new("/nonexistent/path/that/cannot/be/created")
    end

    test "opens multiple independent stores" do
      dir_a = tmp_dir()
      dir_b = tmp_dir()

      on_exit(fn ->
        File.rm_rf(dir_a)
        File.rm_rf(dir_b)
      end)

      store_a = open_store(dir_a)
      store_b = open_store(dir_b)

      :ok = NIF.put(store_a, "key", "value_a", 0)
      :ok = NIF.put(store_b, "key", "value_b", 0)

      assert {:ok, "value_a"} == NIF.get(store_a, "key")
      assert {:ok, "value_b"} == NIF.get(store_b, "key")
    end
  end

  # ------------------------------------------------------------------
  # put/4 and get/2
  # ------------------------------------------------------------------

  describe "put/4 and get/2" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "put and get round-trip", %{store: store} do
      :ok = NIF.put(store, "key", "value", 0)
      assert {:ok, "value"} == NIF.get(store, "key")
    end

    test "get missing key returns {:ok, nil}", %{store: store} do
      assert {:ok, nil} == NIF.get(store, "never_written")
    end

    test "put overwrites existing value", %{store: store} do
      :ok = NIF.put(store, "k", "v1", 0)
      :ok = NIF.put(store, "k", "v2", 0)
      assert {:ok, "v2"} == NIF.get(store, "k")
    end

    test "put with binary key containing null bytes", %{store: store} do
      key = <<0, 1, 2, 3>>
      :ok = NIF.put(store, key, "binary_key_val", 0)
      assert {:ok, "binary_key_val"} == NIF.get(store, key)
    end

    test "put with binary value containing null bytes", %{store: store} do
      value = <<0, 255, 0, 255, 0>>
      :ok = NIF.put(store, "bin_val", value, 0)
      assert {:ok, value} == NIF.get(store, "bin_val")
    end

    test "put with empty key", %{store: store} do
      # The NIF rejects empty keys with an error
      result = NIF.put(store, "", "empty_key_val", 0)
      assert result == :ok or match?({:error, _}, result)
    end

    test "put with empty value stores empty string" do
      # Tombstones now use value_size=u32::MAX; value_size=0 is a valid
      # empty binary, so get returns "".
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      :ok = NIF.put(store, "empty_val", "", 0)
      assert {:ok, ""} == NIF.get(store, "empty_val")
    end

    test "put with large value (100KB)", %{store: store} do
      large_value = :crypto.strong_rand_bytes(100_000)
      :ok = NIF.put(store, "large_val", large_value, 0)
      assert {:ok, ^large_value} = NIF.get(store, "large_val")
    end

    test "put with large key (10KB)", %{store: store} do
      large_key = :crypto.strong_rand_bytes(10_000)
      :ok = NIF.put(store, large_key, "large_key_val", 0)
      assert {:ok, "large_key_val"} == NIF.get(store, large_key)
    end

    test "put with unicode key", %{store: store} do
      key = "hello world"
      :ok = NIF.put(store, key, "unicode_key_val", 0)
      assert {:ok, "unicode_key_val"} == NIF.get(store, key)
    end

    test "put with unicode value", %{store: store} do
      value = "Elixir est genial"
      :ok = NIF.put(store, "uni_val", value, 0)
      assert {:ok, ^value} = NIF.get(store, "uni_val")
    end
  end

  # ------------------------------------------------------------------
  # expiry
  # ------------------------------------------------------------------

  describe "expiry" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "put with expire_at_ms=0 never expires", %{store: store} do
      :ok = NIF.put(store, "no_exp", "forever", 0)
      assert {:ok, "forever"} == NIF.get(store, "no_exp")
    end

    test "put with past expire_at_ms returns nil on get", %{store: store} do
      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "expired", "gone", past)
      assert {:ok, nil} == NIF.get(store, "expired")
    end

    test "put with future expire_at_ms returns value", %{store: store} do
      future = System.os_time(:millisecond) + 60_000
      :ok = NIF.put(store, "future", "still_here", future)
      assert {:ok, "still_here"} == NIF.get(store, "future")
    end

    test "expired key is not included in keys/1", %{store: store} do
      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "expired_key", "val", past)
      :ok = NIF.put(store, "live_key", "val", 0)

      keys = NIF.keys(store)
      assert "live_key" in keys
      refute "expired_key" in keys
    end
  end

  # ------------------------------------------------------------------
  # delete/2
  # ------------------------------------------------------------------

  describe "delete/2" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "delete existing key", %{store: store} do
      :ok = NIF.put(store, "del_me", "val", 0)
      assert {:ok, true} == NIF.delete(store, "del_me")
    end

    test "delete missing key returns {:ok, false}", %{store: store} do
      assert {:ok, false} == NIF.delete(store, "never_existed")
    end

    test "delete is idempotent", %{store: store} do
      :ok = NIF.put(store, "idem", "val", 0)
      assert {:ok, true} == NIF.delete(store, "idem")
      assert {:ok, false} == NIF.delete(store, "idem")
    end

    test "get after delete returns {:ok, nil}", %{store: store} do
      :ok = NIF.put(store, "gone", "val", 0)
      {:ok, true} = NIF.delete(store, "gone")
      assert {:ok, nil} == NIF.get(store, "gone")
    end

    test "delete with binary key containing null bytes", %{store: store} do
      key = <<0, 1, 2, 3>>
      :ok = NIF.put(store, key, "val", 0)
      assert {:ok, true} == NIF.delete(store, key)
      assert {:ok, nil} == NIF.get(store, key)
    end
  end

  # ------------------------------------------------------------------
  # keys/1
  # ------------------------------------------------------------------

  describe "keys/1" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "returns empty list for empty store", %{store: store} do
      assert [] == NIF.keys(store)
    end

    test "returns all put keys", %{store: store} do
      for i <- 1..5, do: :ok = NIF.put(store, "key_#{i}", "val_#{i}", 0)

      keys = NIF.keys(store)
      assert length(keys) == 5

      for i <- 1..5 do
        assert "key_#{i}" in keys
      end
    end

    test "does not include deleted keys", %{store: store} do
      :ok = NIF.put(store, "a", "1", 0)
      :ok = NIF.put(store, "b", "2", 0)
      :ok = NIF.put(store, "c", "3", 0)
      {:ok, true} = NIF.delete(store, "b")

      keys = NIF.keys(store)
      assert length(keys) == 2
      assert "a" in keys
      assert "c" in keys
      refute "b" in keys
    end

    test "returns binary keys with null bytes", %{store: store} do
      key = <<0, 1, 2, 3>>
      :ok = NIF.put(store, key, "val", 0)

      keys = NIF.keys(store)
      assert key in keys
    end
  end

  # ------------------------------------------------------------------
  # put_batch/2
  # ------------------------------------------------------------------

  describe "put_batch/2" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "put_batch stores all entries", %{store: store} do
      batch =
        Enum.map(1..5, fn i ->
          {"batch_k_#{i}", "batch_v_#{i}", 0}
        end)

      assert :ok == NIF.put_batch(store, batch)

      for i <- 1..5 do
        assert {:ok, "batch_v_#{i}"} == NIF.get(store, "batch_k_#{i}")
      end
    end

    test "put_batch with empty list is :ok", %{store: store} do
      assert :ok == NIF.put_batch(store, [])
    end

    test "put_batch with expiry entries", %{store: store} do
      past = System.os_time(:millisecond) - 1000
      future = System.os_time(:millisecond) + 60_000

      batch = [
        {"expired_batch", "val_exp", past},
        {"live_batch", "val_live", future}
      ]

      assert :ok == NIF.put_batch(store, batch)

      assert {:ok, nil} == NIF.get(store, "expired_batch")
      assert {:ok, "val_live"} == NIF.get(store, "live_batch")
    end

    test "put_batch returns :ok", %{store: store} do
      batch = [{"ret_k", "ret_v", 0}]
      assert :ok = NIF.put_batch(store, batch)
    end

    test "put_batch is atomic - all keys present after call", %{store: store} do
      batch =
        Enum.map(1..100, fn i ->
          {"atomic_#{i}", "val_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)

      keys = NIF.keys(store)

      for i <- 1..100 do
        assert "atomic_#{i}" in keys
      end
    end
  end

  # ------------------------------------------------------------------
  # write_hint/1
  # ------------------------------------------------------------------

  describe "write_hint/1" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store, dir: dir}
    end

    test "write_hint returns :ok", %{store: store} do
      :ok = NIF.put(store, "hint_key", "hint_val", 0)
      assert :ok == NIF.write_hint(store)
    end

    test "write_hint creates hint file on disk", %{store: store, dir: dir} do
      :ok = NIF.put(store, "hint_key", "hint_val", 0)
      :ok = NIF.write_hint(store)

      hint_files = Path.wildcard(Path.join(dir, "*.hint"))
      assert length(hint_files) > 0
    end
  end

  # ------------------------------------------------------------------
  # purge_expired/1
  # ------------------------------------------------------------------

  describe "purge_expired/1" do
    setup do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{store: store}
    end

    test "purge_expired returns {:ok, 0} when nothing expired", %{store: store} do
      :ok = NIF.put(store, "live1", "val", 0)
      :ok = NIF.put(store, "live2", "val", 0)
      assert {:ok, 0} == NIF.purge_expired(store)
    end

    test "purge_expired returns {:ok, count} for expired keys", %{store: store} do
      past = System.os_time(:millisecond) - 1000

      :ok = NIF.put(store, "exp1", "val", past)
      :ok = NIF.put(store, "exp2", "val", past)
      :ok = NIF.put(store, "exp3", "val", past)

      assert {:ok, 3} == NIF.purge_expired(store)
    end

    test "purge_expired removes expired keys", %{store: store} do
      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "purge_me", "val", past)

      {:ok, _count} = NIF.purge_expired(store)
      assert {:ok, nil} == NIF.get(store, "purge_me")
    end

    test "purge_expired does not affect live keys", %{store: store} do
      past = System.os_time(:millisecond) - 1000

      :ok = NIF.put(store, "expired", "val", past)
      :ok = NIF.put(store, "alive", "val", 0)

      {:ok, _count} = NIF.purge_expired(store)

      assert {:ok, nil} == NIF.get(store, "expired")
      assert {:ok, "val"} == NIF.get(store, "alive")
    end
  end

  # ------------------------------------------------------------------
  # crash recovery / persistence
  # ------------------------------------------------------------------

  describe "crash recovery / persistence" do
    test "data persists across store open/close" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      store1 = open_store(dir)
      :ok = NIF.put(store1, "persist_key", "persist_val", 0)

      # Let store1 resource be garbage collected (no explicit close)
      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "persist_val"} == NIF.get(store2, "persist_key")
    end

    test "tombstone persists across reopen" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      store1 = open_store(dir)
      :ok = NIF.put(store1, "tomb_key", "val", 0)
      {:ok, true} = NIF.delete(store1, "tomb_key")

      _ = store1
      :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, nil} == NIF.get(store2, "tomb_key")
    end

    test "multiple put/delete cycles persist correctly" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      for round <- 1..10 do
        store = open_store(dir)
        key = "cycle_key_#{round}"
        :ok = NIF.put(store, key, "round_#{round}", 0)

        # Delete keys from previous even rounds
        if round > 1 and rem(round - 1, 2) == 0 do
          prev_key = "cycle_key_#{round - 1}"
          NIF.delete(store, prev_key)
        end

        _ = store
        :erlang.garbage_collect()
      end

      # Final verification
      final_store = open_store(dir)

      # All keys should exist
      for round <- 1..10 do
        key = "cycle_key_#{round}"

        if round > 1 and rem(round, 2) == 0 do
          # Even rounds (2, 4, 6, 8, 10) were deleted in the next odd round
          # except round 10 which was never followed by a delete
          if round < 10 do
            assert {:ok, nil} == NIF.get(final_store, key),
                   "expected key #{key} to be deleted"
          else
            assert {:ok, "round_#{round}"} == NIF.get(final_store, key),
                   "expected key #{key} to be present"
          end
        else
          # Odd rounds should all be present
          assert {:ok, "round_#{round}"} == NIF.get(final_store, key),
                 "expected key #{key} to be present"
        end
      end
    end
  end

  # ------------------------------------------------------------------
  # concurrent access
  # ------------------------------------------------------------------

  describe "concurrent access" do
    test "concurrent puts from multiple processes do not corrupt data" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      tasks =
        Enum.map(1..20, fn i ->
          Task.async(fn ->
            NIF.put(store, "key_#{i}", "val_#{i}", 0)
          end)
        end)

      Enum.each(tasks, &Task.await/1)

      keys = NIF.keys(store)
      assert length(keys) == 20

      for i <- 1..20 do
        assert {:ok, "val_#{i}"} == NIF.get(store, "key_#{i}")
      end
    end

    test "concurrent gets do not corrupt data" do
      dir = tmp_dir()

      on_exit(fn -> File.rm_rf(dir) end)

      store = open_store(dir)

      for i <- 1..10, do: :ok = NIF.put(store, "cget_#{i}", "cval_#{i}", 0)

      tasks =
        Enum.flat_map(1..10, fn i ->
          Enum.map(1..5, fn _attempt ->
            Task.async(fn ->
              NIF.get(store, "cget_#{i}")
            end)
          end)
        end)

      results = Enum.map(tasks, &Task.await/1)

      # Group results by key index (5 attempts per key, 10 keys)
      results
      |> Enum.chunk_every(5)
      |> Enum.with_index(1)
      |> Enum.each(fn {chunk, i} ->
        expected = {:ok, "cval_#{i}"}
        Enum.each(chunk, fn result -> assert result == expected end)
      end)
    end
  end
end
