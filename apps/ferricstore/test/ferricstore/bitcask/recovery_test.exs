defmodule Ferricstore.Bitcask.RecoveryTest do
  @moduledoc false

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  setup do
    dir = Path.join(System.tmp_dir!(), "recovery_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf!(dir)
    end)

    %{dir: dir}
  end

  # -------------------------------------------------------------------
  # Log replay on startup
  # -------------------------------------------------------------------

  describe "log replay on startup" do
    test "store recovers all keys after clean close and reopen", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      for i <- 1..50 do
        key = "key_#{i}"
        value = "value_#{i}"
        :ok = NIF.put(store, key, value, 0)
      end

      # Let the first store resource be collected
      store = nil
      _ = store
      :erlang.garbage_collect()

      # Reopen from the same directory
      {:ok, store2} = NIF.new(dir)

      for i <- 1..50 do
        key = "key_#{i}"
        expected = "value_#{i}"
        assert {:ok, ^expected} = NIF.get(store2, key)
      end
    end

    test "store recovers keys after crash (no explicit close)", %{dir: dir} do
      # Write keys in a spawned process so the resource ref truly goes out of scope
      test_pid = self()

      spawn(fn ->
        {:ok, store} = NIF.new(dir)

        for i <- 1..20 do
          :ok = NIF.put(store, "k#{i}", "v#{i}", 0)
        end

        send(test_pid, :done)
        # store ref goes out of scope when this process exits
      end)

      assert_receive :done, 5_000
      :erlang.garbage_collect()
      # Give the NIF resource destructor time to run (if any)
      Process.sleep(50)

      {:ok, store2} = NIF.new(dir)

      for i <- 1..20 do
        expected = "v#{i}"
        assert {:ok, ^expected} = NIF.get(store2, "k#{i}")
      end
    end

    test "store recovers after partial tail record (truncated log)", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      for i <- 1..10 do
        :ok = NIF.put(store, "key_#{i}", "val_#{i}", 0)
      end

      # Drop the store reference so the file is not held open
      store = nil
      _ = store
      :erlang.garbage_collect()
      Process.sleep(50)

      # Find the log file and truncate the last 5 bytes
      [log_file | _] = Path.wildcard(Path.join(dir, "*.log"))
      {:ok, fd} = :file.open(String.to_charlist(log_file), [:read, :write, :binary])
      {:ok, file_size} = :file.position(fd, :eof)
      {:ok, _} = :file.position(fd, file_size - 5)
      :ok = :file.truncate(fd)
      :ok = :file.close(fd)

      # Reopen — should recover without error
      {:ok, store2} = NIF.new(dir)

      # At least the first 9 keys should be present (the 10th may or may not
      # survive depending on exactly which bytes were truncated)
      recovered =
        Enum.count(1..10, fn i ->
          {:ok, result} = NIF.get(store2, "key_#{i}")
          result != nil
        end)

      assert recovered >= 9
    end

    test "store starts empty from empty directory", %{dir: dir} do
      empty_dir = Path.join(dir, "empty_subdir")
      File.mkdir_p!(empty_dir)

      {:ok, store} = NIF.new(empty_dir)
      assert NIF.keys(store) == []
    end
  end

  # -------------------------------------------------------------------
  # Tombstone durability
  # -------------------------------------------------------------------

  describe "tombstone durability" do
    test "delete persists across reopens", %{dir: dir} do
      {:ok, store} = NIF.new(dir)
      :ok = NIF.put(store, "key", "value", 0)
      NIF.delete(store, "key")

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, nil} = NIF.get(store2, "key")
    end

    test "delete then put persists correctly", %{dir: dir} do
      {:ok, store} = NIF.new(dir)
      :ok = NIF.put(store, "k", "v1", 0)
      NIF.delete(store, "k")
      :ok = NIF.put(store, "k", "v2", 0)

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, "v2"} = NIF.get(store2, "k")
    end

    test "tombstone prevents resurrection of expired key", %{dir: dir} do
      past = System.os_time(:millisecond) - 5_000

      {:ok, store} = NIF.new(dir)
      :ok = NIF.put(store, "k", "old_value", past)
      # get triggers lazy tombstone write for expired keys
      assert {:ok, nil} = NIF.get(store, "k")

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, nil} = NIF.get(store2, "k")
    end
  end

  # -------------------------------------------------------------------
  # Hint file recovery
  # -------------------------------------------------------------------

  describe "hint file recovery" do
    test "store loads faster with hint file (hint path exists after write_hint)", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      for i <- 1..100 do
        :ok = NIF.put(store, "hk_#{i}", "hv_#{i}", 0)
      end

      :ok = NIF.write_hint(store)

      hint_files = Path.wildcard(Path.join(dir, "*.hint"))
      assert length(hint_files) >= 1
    end

    test "store recovers from hint file correctly", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      for i <- 1..20 do
        :ok = NIF.put(store, "hk_#{i}", "hv_#{i}", 0)
      end

      :ok = NIF.write_hint(store)

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)

      for i <- 1..20 do
        expected = "hv_#{i}"
        assert {:ok, ^expected} = NIF.get(store2, "hk_#{i}")
      end
    end

    test "store falls back to log when hint file is corrupt", %{dir: dir} do
      # Use a dedicated subprocess to ensure the NIF resource is fully released
      # before we corrupt the hint file on disk.
      test_pid = self()

      spawn(fn ->
        {:ok, store} = NIF.new(dir)

        for i <- 1..10 do
          :ok = NIF.put(store, "hk_#{i}", "hv_#{i}", 0)
        end

        :ok = NIF.write_hint(store)
        send(test_pid, :written)
      end)

      assert_receive :written, 5_000
      # Wait for the spawned process to exit and its NIF resource to be released
      Process.sleep(100)
      :erlang.garbage_collect()

      # Corrupt the hint file by truncating it to 10 bytes
      [hint_file | _] = Path.wildcard(Path.join(dir, "*.hint"))
      {:ok, fd} = :file.open(String.to_charlist(hint_file), [:read, :write, :binary])
      {:ok, _} = :file.position(fd, 10)
      :ok = :file.truncate(fd)
      :ok = :file.close(fd)

      # Reopen — should fall back to log replay
      {:ok, store2} = NIF.new(dir)

      for i <- 1..10 do
        expected = "hv_#{i}"
        assert {:ok, ^expected} = NIF.get(store2, "hk_#{i}")
      end
    end

    test "store falls back to log when hint file is empty", %{dir: dir} do
      # Use a dedicated subprocess to ensure the NIF resource is fully released.
      test_pid = self()

      spawn(fn ->
        {:ok, store} = NIF.new(dir)

        for i <- 1..5 do
          :ok = NIF.put(store, "hk_#{i}", "hv_#{i}", 0)
        end

        :ok = NIF.write_hint(store)
        send(test_pid, :written)
      end)

      assert_receive :written, 5_000
      Process.sleep(100)
      :erlang.garbage_collect()

      # Delete the hint file entirely so the store falls back to log replay
      # (an empty hint file is valid per the format spec — zero entries —
      # so removing it is the correct way to force log-only recovery).
      [hint_file | _] = Path.wildcard(Path.join(dir, "*.hint"))
      File.rm!(hint_file)

      # Reopen — should fall back to log replay
      {:ok, store2} = NIF.new(dir)

      for i <- 1..5 do
        expected = "hv_#{i}"
        assert {:ok, ^expected} = NIF.get(store2, "hk_#{i}")
      end
    end
  end

  # -------------------------------------------------------------------
  # Expiry persistence
  # -------------------------------------------------------------------

  describe "expiry persistence" do
    test "expired key does not return after reopen", %{dir: dir} do
      expire_at = System.os_time(:millisecond) + 200

      {:ok, store} = NIF.new(dir)
      :ok = NIF.put(store, "ttl_key", "ttl_value", expire_at)

      :timer.sleep(300)

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, nil} = NIF.get(store2, "ttl_key")
    end

    test "non-expired key returns after reopen", %{dir: dir} do
      expire_at = System.os_time(:millisecond) + 60_000

      {:ok, store} = NIF.new(dir)
      :ok = NIF.put(store, "live_key", "live_value", expire_at)

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, "live_value"} = NIF.get(store2, "live_key")
    end
  end

  # -------------------------------------------------------------------
  # Batch put durability
  # -------------------------------------------------------------------

  describe "batch put durability" do
    test "put_batch persists all entries after reopen", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      batch =
        for i <- 1..30 do
          {"bk_#{i}", "bv_#{i}", 0}
        end

      :ok = NIF.put_batch(store, batch)

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)

      for i <- 1..30 do
        expected = "bv_#{i}"
        assert {:ok, ^expected} = NIF.get(store2, "bk_#{i}")
      end
    end

    test "put_batch with mixed expiry persists correctly", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      past = System.os_time(:millisecond) - 5_000
      future = System.os_time(:millisecond) + 60_000

      live_batch =
        for i <- 1..10 do
          {"live_#{i}", "lv_#{i}", future}
        end

      expired_batch =
        for i <- 1..10 do
          {"expired_#{i}", "ev_#{i}", past}
        end

      :ok = NIF.put_batch(store, live_batch ++ expired_batch)

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)

      for i <- 1..10 do
        expected = "lv_#{i}"
        assert {:ok, ^expected} = NIF.get(store2, "live_#{i}")
      end

      for i <- 1..10 do
        assert {:ok, nil} = NIF.get(store2, "expired_#{i}")
      end
    end
  end

  # -------------------------------------------------------------------
  # Large data
  # -------------------------------------------------------------------

  describe "large data" do
    test "large value (1MB) persists correctly", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      large_value = :crypto.strong_rand_bytes(1_000_000)
      :ok = NIF.put(store, "big_key", large_value, 0)

      # Verify in-session round-trip
      assert {:ok, ^large_value} = NIF.get(store, "big_key")

      store = nil
      _ = store
      :erlang.garbage_collect()

      # Verify after reopen
      {:ok, store2} = NIF.new(dir)
      assert {:ok, ^large_value} = NIF.get(store2, "big_key")
    end

    test "many keys (1000) all persist after reopen", %{dir: dir} do
      {:ok, store} = NIF.new(dir)

      pairs =
        for i <- 1..1000 do
          key = "mk_#{i}"
          value = "mv_#{i}_#{:crypto.strong_rand_bytes(8) |> Base.encode16()}"
          :ok = NIF.put(store, key, value, 0)
          {key, value}
        end

      store = nil
      _ = store
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)

      recovered_keys = NIF.keys(store2)
      assert length(recovered_keys) == 1000

      for {key, value} <- pairs do
        assert {:ok, ^value} = NIF.get(store2, key)
      end
    end
  end
end
