defmodule Ferricstore.Bitcask.BitcaskEdgeCasesTest do
  @moduledoc """
  Comprehensive edge-case tests for the Bitcask NIF, recovery semantics,
  and the Store.Shard GenServer.

  These tests intentionally target boundary conditions, unusual inputs,
  and concurrency scenarios that are NOT covered by the existing
  nif_test.exs, recovery_test.exs, and shard_test.exs suites.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard

  setup do
    # Isolated shard tests bypass Raft (no ra system for ad-hoc indices)
    original = Application.get_env(:ferricstore, :raft_enabled)
    Application.put_env(:ferricstore, :raft_enabled, false)
    on_exit(fn -> Application.put_env(:ferricstore, :raft_enabled, original) end)
    :ok
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "bc_edge_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp release_store(store) do
    _ = store
    :erlang.garbage_collect()
    Process.sleep(50)
  end

  defp start_shard do
    dir = tmp_dir()
    index = :erlang.unique_integer([:positive]) |> rem(100_000) |> Kernel.+(100_000)
    {:ok, pid} = Shard.start_link(index: index, data_dir: dir)
    {pid, index, dir}
  end

  # ===================================================================
  # SECTION 1: NIF edge cases
  # ===================================================================

  describe "NIF: key with all 256 byte values (0x00-0xFF)" do
    test "round-trips a key containing every byte value" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      # Build a 256-byte key with every byte value from 0 to 255
      key = :binary.list_to_bin(Enum.to_list(0..255))
      assert byte_size(key) == 256

      :ok = NIF.put(store, key, "all_bytes_val", 0)
      assert {:ok, "all_bytes_val"} == NIF.get(store, key)
      assert key in NIF.keys(store)
    end
  end

  describe "NIF: very large value (512KB)" do
    test "put and get round-trip for 512KB value" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      large_value = :crypto.strong_rand_bytes(512 * 1024)
      :ok = NIF.put(store, "big_512k", large_value, 0)
      assert {:ok, ^large_value} = NIF.get(store, "big_512k")
    end
  end

  describe "NIF: unicode multibyte keys" do
    test "CJK characters in key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      key = "键-鍵-キー-열쇠"
      :ok = NIF.put(store, key, "cjk_val", 0)
      assert {:ok, "cjk_val"} == NIF.get(store, key)
      assert key in NIF.keys(store)
    end

    test "emoji characters in key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      key = "\u{1F600}\u{1F4A9}\u{1F680}"
      :ok = NIF.put(store, key, "emoji_val", 0)
      assert {:ok, "emoji_val"} == NIF.get(store, key)
    end

    test "mixed ASCII and 4-byte UTF-8 codepoints" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      key = "user:\u{10FFFF}:profile"
      :ok = NIF.put(store, key, "mixed_utf8", 0)
      assert {:ok, "mixed_utf8"} == NIF.get(store, key)
    end
  end

  describe "NIF: value with embedded null bytes" do
    test "value consisting entirely of null bytes" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      # 100 null bytes is a non-empty value, distinct from the empty-string tombstone
      value = :binary.copy(<<0>>, 100)
      :ok = NIF.put(store, "all_nulls", value, 0)
      assert {:ok, ^value} = NIF.get(store, "all_nulls")
    end

    test "value with null bytes interspersed with text" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      value = "hello\0world\0foo\0"
      :ok = NIF.put(store, "null_text", value, 0)
      assert {:ok, ^value} = NIF.get(store, "null_text")
    end
  end

  describe "NIF: put_batch with 1000 entries" do
    test "all 1000 entries are stored and retrievable" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      batch =
        Enum.map(1..1000, fn i ->
          {"batch1k_#{i}", "val1k_#{i}", 0}
        end)

      assert :ok == NIF.put_batch(store, batch)

      keys = NIF.keys(store)
      assert length(keys) == 1000

      # Spot-check first, middle, last
      assert {:ok, "val1k_1"} == NIF.get(store, "batch1k_1")
      assert {:ok, "val1k_500"} == NIF.get(store, "batch1k_500")
      assert {:ok, "val1k_1000"} == NIF.get(store, "batch1k_1000")
    end
  end

  describe "NIF: put_batch with duplicate keys (last wins)" do
    test "when the same key appears multiple times, the last value wins" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      batch = [
        {"dup_key", "first", 0},
        {"dup_key", "second", 0},
        {"dup_key", "third", 0}
      ]

      assert :ok == NIF.put_batch(store, batch)
      assert {:ok, "third"} == NIF.get(store, "dup_key")
    end

    test "duplicate keys with different expiry values, last wins" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      past = System.os_time(:millisecond) - 1000

      batch = [
        {"dup_exp", "alive", 0},
        {"dup_exp", "dead", past}
      ]

      assert :ok == NIF.put_batch(store, batch)
      # Last entry has past expiry, so the key should be expired
      assert {:ok, nil} == NIF.get(store, "dup_exp")
    end
  end

  describe "NIF: open same path twice" do
    test "second open on an existing store succeeds and sees prior data" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store1 = open_store(dir)
      :ok = NIF.put(store1, "persist_twice", "val", 0)
      release_store(store1)

      # First reopen
      store2 = open_store(dir)
      assert {:ok, "val"} == NIF.get(store2, "persist_twice")
      release_store(store2)

      # Second reopen (third overall open of same path)
      store3 = open_store(dir)
      assert {:ok, "val"} == NIF.get(store3, "persist_twice")
    end
  end

  describe "NIF: open path that does not yet exist" do
    test "creates the directory and a new empty store" do
      # Use a subdirectory that does not yet exist
      base = tmp_dir()
      new_path = Path.join(base, "brand_new_subdir")
      on_exit(fn -> File.rm_rf!(base) end)

      refute File.exists?(new_path)
      assert {:ok, store} = NIF.new(new_path)
      assert is_reference(store)
      assert NIF.keys(store) == []
    end
  end

  describe "NIF: write_hint on empty store" do
    test "write_hint succeeds even with zero keys" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      assert :ok == NIF.write_hint(store)
    end
  end

  describe "NIF: purge_expired selective behavior" do
    test "purges only expired keys, live keys remain intact" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      past = System.os_time(:millisecond) - 1000
      future = System.os_time(:millisecond) + 60_000

      # 5 expired keys
      for i <- 1..5 do
        :ok = NIF.put(store, "expired_#{i}", "ev_#{i}", past)
      end

      # 5 live keys (no expiry)
      for i <- 1..5 do
        :ok = NIF.put(store, "live_#{i}", "lv_#{i}", 0)
      end

      # 3 live keys with future expiry
      for i <- 1..3 do
        :ok = NIF.put(store, "future_#{i}", "fv_#{i}", future)
      end

      assert {:ok, 5} == NIF.purge_expired(store)

      # Expired keys gone
      for i <- 1..5 do
        assert {:ok, nil} == NIF.get(store, "expired_#{i}")
      end

      # Live keys still present
      for i <- 1..5 do
        assert {:ok, "lv_#{i}"} == NIF.get(store, "live_#{i}")
      end

      # Future-expiry keys still present
      for i <- 1..3 do
        assert {:ok, "fv_#{i}"} == NIF.get(store, "future_#{i}")
      end
    end

    test "purge_expired on empty store returns {:ok, 0}" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      assert {:ok, 0} == NIF.purge_expired(store)
    end

    test "purge_expired twice in a row: second returns 0" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      past = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "once", "val", past)

      assert {:ok, 1} == NIF.purge_expired(store)
      assert {:ok, 0} == NIF.purge_expired(store)
    end
  end

  describe "NIF: overwrite key then check keys/1 has no duplicates" do
    test "overwriting a key does not create duplicate entries in keys/1" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      :ok = NIF.put(store, "unique", "v1", 0)
      :ok = NIF.put(store, "unique", "v2", 0)
      :ok = NIF.put(store, "unique", "v3", 0)

      keys = NIF.keys(store)
      count = Enum.count(keys, &(&1 == "unique"))
      assert count == 1
    end
  end

  describe "NIF: put then delete then re-put" do
    test "re-putting a deleted key makes it accessible again" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)
      store = open_store(dir)

      :ok = NIF.put(store, "phoenix", "v1", 0)
      {:ok, true} = NIF.delete(store, "phoenix")
      assert {:ok, nil} == NIF.get(store, "phoenix")

      :ok = NIF.put(store, "phoenix", "v2", 0)
      assert {:ok, "v2"} == NIF.get(store, "phoenix")
      assert "phoenix" in NIF.keys(store)
    end
  end

  # ===================================================================
  # SECTION 2: Recovery edge cases
  # ===================================================================

  describe "Recovery: write N keys, crash (reopen), all keys present" do
    test "200 keys survive simulated crash via process exit" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      test_pid = self()

      spawn(fn ->
        store = open_store(dir)

        for i <- 1..200 do
          :ok = NIF.put(store, "crash_#{i}", "cv_#{i}", 0)
        end

        send(test_pid, :done)
      end)

      assert_receive :done, 10_000
      :erlang.garbage_collect()
      Process.sleep(100)

      store2 = open_store(dir)

      for i <- 1..200 do
        assert {:ok, "cv_#{i}"} == NIF.get(store2, "crash_#{i}"),
               "key crash_#{i} missing after recovery"
      end

      assert length(NIF.keys(store2)) == 200
    end
  end

  describe "Recovery: key with TTL, reopen after TTL passed" do
    test "expired key is gone after reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      # Set expire_at to 100ms from now
      expire_at = System.os_time(:millisecond) + 100
      store = open_store(dir)
      :ok = NIF.put(store, "ttl_recover", "short_lived", expire_at)

      # Verify it's alive right now
      assert {:ok, "short_lived"} == NIF.get(store, "ttl_recover")

      release_store(store)

      # Wait past the TTL
      Process.sleep(200)

      store2 = open_store(dir)
      assert {:ok, nil} == NIF.get(store2, "ttl_recover")
      refute "ttl_recover" in NIF.keys(store2)
    end
  end

  describe "Recovery: tombstone (delete) survives reopen" do
    test "deleted key absent after reopen even with no purge" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "tomb_test", "alive", 0)
      {:ok, true} = NIF.delete(store, "tomb_test")

      release_store(store)

      store2 = open_store(dir)
      assert {:ok, nil} == NIF.get(store2, "tomb_test")
      refute "tomb_test" in NIF.keys(store2)
    end
  end

  describe "Recovery: large put_batch survives reopen" do
    test "1000 batch-written entries survive reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)

      batch =
        Enum.map(1..1000, fn i ->
          {"rbatch_#{i}", "rv_#{i}", 0}
        end)

      :ok = NIF.put_batch(store, batch)
      release_store(store)

      store2 = open_store(dir)
      keys = NIF.keys(store2)
      assert length(keys) == 1000

      # Spot-check
      assert {:ok, "rv_1"} == NIF.get(store2, "rbatch_1")
      assert {:ok, "rv_500"} == NIF.get(store2, "rbatch_500")
      assert {:ok, "rv_1000"} == NIF.get(store2, "rbatch_1000")
    end
  end

  describe "Recovery: multiple reopen cycles" do
    test "data written across 5 open-write-close cycles is all present" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      # Cycle 1: write keys 1-10
      store = open_store(dir)
      for i <- 1..10, do: :ok = NIF.put(store, "cycle_#{i}", "c1_#{i}", 0)
      release_store(store)

      # Cycle 2: write keys 11-20, overwrite key 5
      store = open_store(dir)
      for i <- 11..20, do: :ok = NIF.put(store, "cycle_#{i}", "c2_#{i}", 0)
      :ok = NIF.put(store, "cycle_5", "overwritten_c2", 0)
      release_store(store)

      # Cycle 3: delete key 3, write keys 21-25
      store = open_store(dir)
      {:ok, true} = NIF.delete(store, "cycle_3")
      for i <- 21..25, do: :ok = NIF.put(store, "cycle_#{i}", "c3_#{i}", 0)
      release_store(store)

      # Cycle 4: write key with short TTL
      store = open_store(dir)
      expire_at = System.os_time(:millisecond) + 100
      :ok = NIF.put(store, "cycle_ttl", "temporary", expire_at)
      release_store(store)

      # Wait for TTL to pass
      Process.sleep(200)

      # Cycle 5: final reopen, verify everything
      store = open_store(dir)

      # Keys 1-2, 4-25 should be present (3 was deleted)
      for i <- [1, 2, 4, 6, 7, 8, 9, 10] do
        assert {:ok, "c1_#{i}"} == NIF.get(store, "cycle_#{i}"),
               "cycle_#{i} should have value from cycle 1"
      end

      # Key 5 was overwritten in cycle 2
      assert {:ok, "overwritten_c2"} == NIF.get(store, "cycle_5")

      # Key 3 was deleted in cycle 3
      assert {:ok, nil} == NIF.get(store, "cycle_3")

      # Keys 11-20 from cycle 2
      for i <- 11..20 do
        assert {:ok, "c2_#{i}"} == NIF.get(store, "cycle_#{i}")
      end

      # Keys 21-25 from cycle 3
      for i <- 21..25 do
        assert {:ok, "c3_#{i}"} == NIF.get(store, "cycle_#{i}")
      end

      # TTL key should be expired
      assert {:ok, nil} == NIF.get(store, "cycle_ttl")
    end
  end

  describe "Recovery: 512KB value survives reopen" do
    test "large binary value is intact after reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      large_value = :crypto.strong_rand_bytes(512 * 1024)
      store = open_store(dir)
      :ok = NIF.put(store, "big_recover", large_value, 0)
      release_store(store)

      store2 = open_store(dir)
      assert {:ok, ^large_value} = NIF.get(store2, "big_recover")
    end
  end

  describe "Recovery: binary key with all byte values survives reopen" do
    test "256-byte key with all byte values is intact after reopen" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      key = :binary.list_to_bin(Enum.to_list(0..255))
      store = open_store(dir)
      :ok = NIF.put(store, key, "all_bytes_recovery", 0)
      release_store(store)

      store2 = open_store(dir)
      assert {:ok, "all_bytes_recovery"} == NIF.get(store2, key)
      assert key in NIF.keys(store2)
    end
  end

  # ===================================================================
  # SECTION 3: Shard GenServer edge cases
  # ===================================================================

  describe "Shard: ETS warm on cache miss from NIF" do
    test "get from NIF populates ETS when entry is not cached" do
      {pid, index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      keydir = :"keydir_#{index}"
      hot_cache = :"hot_cache_#{index}"

      # Put via GenServer (writes to both NIF and ETS)
      :ok = GenServer.call(pid, {:put, "warm_test", "warm_value", 0})

      # Manually evict from ETS to simulate a cache miss
      :ets.delete(keydir, "warm_test")
      :ets.delete(hot_cache, "warm_test")
      assert [] == :ets.lookup(hot_cache, "warm_test")

      # Get should warm ETS from NIF
      assert "warm_value" == GenServer.call(pid, {:get, "warm_test"})
      assert [{_, "warm_value"}] = :ets.lookup(hot_cache, "warm_test")
    end
  end

  describe "Shard: ETS lazy expiry eviction on get" do
    test "expired ETS entry is evicted and nil is returned" do
      {pid, index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      keydir = :"keydir_#{index}"
      hot_cache = :"hot_cache_#{index}"

      # Write a key with 100ms TTL
      expire_at = System.os_time(:millisecond) + 100
      :ok = GenServer.call(pid, {:put, "lazy_exp", "temp_val", expire_at})

      # Confirm it's in ETS
      assert [{_, ^expire_at}] = :ets.lookup(keydir, "lazy_exp")
      assert [{_, "temp_val"}] = :ets.lookup(hot_cache, "lazy_exp")

      # Wait for expiry
      Process.sleep(200)

      # Get should return nil and evict from ETS
      assert nil == GenServer.call(pid, {:get, "lazy_exp"})
      assert [] == :ets.lookup(keydir, "lazy_exp")
      assert [] == :ets.lookup(hot_cache, "lazy_exp")
    end
  end

  describe "Shard: get_meta for missing key" do
    test "returns nil for a key that was never written" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      assert nil == GenServer.call(pid, {:get_meta, "nonexistent_meta"})
    end
  end

  describe "Shard: get_meta for key with no TTL" do
    test "returns {value, 0} for a key stored with expire_at_ms=0" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      :ok = GenServer.call(pid, {:put, "meta_no_ttl", "persistent", 0})
      assert {"persistent", 0} == GenServer.call(pid, {:get_meta, "meta_no_ttl"})
    end
  end

  describe "Shard: get_meta for key with TTL" do
    test "returns {value, expire_at_ms} for a key with future expiry" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      future = System.os_time(:millisecond) + 120_000
      :ok = GenServer.call(pid, {:put, "meta_ttl", "ttl_val", future})
      assert {"ttl_val", ^future} = GenServer.call(pid, {:get_meta, "meta_ttl"})
    end
  end

  describe "Shard: get_meta from NIF fallback (cache miss)" do
    test "get_meta warms ETS and returns {value, 0} on cache miss" do
      {pid, index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      keydir = :"keydir_#{index}"
      hot_cache = :"hot_cache_#{index}"

      :ok = GenServer.call(pid, {:put, "meta_warm", "mw_val", 0})

      # Clear ETS to force NIF fallback
      :ets.delete(keydir, "meta_warm")
      :ets.delete(hot_cache, "meta_warm")

      assert {"mw_val", 0} == GenServer.call(pid, {:get_meta, "meta_warm"})
      # Verify ETS was warmed
      assert [{_, "mw_val"}] = :ets.lookup(hot_cache, "meta_warm")
      assert [{_, 0}] = :ets.lookup(keydir, "meta_warm")
    end
  end

  describe "Shard: put then delete then get" do
    test "returns nil after delete" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      :ok = GenServer.call(pid, {:put, "del_flow", "alive", 0})
      assert "alive" == GenServer.call(pid, {:get, "del_flow"})

      :ok = GenServer.call(pid, {:delete, "del_flow"})
      assert nil == GenServer.call(pid, {:get, "del_flow"})
    end
  end

  describe "Shard: concurrent gets from 20 tasks" do
    test "all 20 tasks read the same correct value" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      :ok = GenServer.call(pid, {:put, "concurrent_read", "shared_val", 0})

      tasks =
        for _ <- 1..20 do
          Task.async(fn ->
            GenServer.call(pid, {:get, "concurrent_read"})
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == "shared_val"))
    end
  end

  describe "Shard: concurrent puts from multiple processes with different keys" do
    test "all 20 concurrent puts succeed and data is consistent" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            key = "cput_#{i}"
            val = "cval_#{i}"
            :ok = GenServer.call(pid, {:put, key, val, 0})
            {key, val}
          end)
        end

      pairs = Task.await_many(tasks, 10_000)

      # Verify all 20 keys are present and correct
      for {key, val} <- pairs do
        assert val == GenServer.call(pid, {:get, key}),
               "expected #{key} to have value #{val}"
      end

      keys = GenServer.call(pid, :keys)
      assert length(keys) == 20
    end
  end

  describe "Shard: GenServer handles unknown messages gracefully" do
    test "unknown call causes the GenServer to crash with FunctionClauseError" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> File.rm_rf!(dir) end)

      # Trap exits so the linked GenServer crash does not kill the test process
      Process.flag(:trap_exit, true)

      # An unhandled call causes a FunctionClauseError in handle_call/3,
      # which terminates the GenServer. The caller receives an :exit signal.
      assert catch_exit(GenServer.call(pid, :unknown_msg, 1000))

      # The GenServer should be dead
      refute Process.alive?(pid)

      Process.flag(:trap_exit, false)
    end

    test "unknown cast causes the GenServer to crash with RuntimeError" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> File.rm_rf!(dir) end)

      # Trap exits so the linked GenServer crash does not kill the test process
      Process.flag(:trap_exit, true)

      GenServer.cast(pid, {:totally_unknown, "data"})

      # Wait for the EXIT message from the crashed GenServer
      assert_receive {:EXIT, ^pid, _reason}, 2000

      # The GenServer should be dead
      refute Process.alive?(pid)

      Process.flag(:trap_exit, false)
    end
  end

  describe "Shard: put with 0 TTL then get_meta returns {value, 0}" do
    test "confirms zero TTL semantics through full round-trip" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      :ok = GenServer.call(pid, {:put, "zero_ttl_meta", "ztm_val", 0})
      result = GenServer.call(pid, {:get_meta, "zero_ttl_meta"})
      assert {"ztm_val", 0} == result
    end
  end

  describe "Shard: exists? after delete returns false" do
    test "exists? returns false after deletion" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      :ok = GenServer.call(pid, {:put, "exist_del", "val", 0})
      assert true == GenServer.call(pid, {:exists, "exist_del"})

      :ok = GenServer.call(pid, {:delete, "exist_del"})
      assert false == GenServer.call(pid, {:exists, "exist_del"})
    end
  end

  describe "Shard: keys excludes both deleted and expired entries" do
    test "keys list only contains live, non-expired entries" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      past = System.os_time(:millisecond) - 1000
      future = System.os_time(:millisecond) + 60_000

      :ok = GenServer.call(pid, {:put, "live_no_exp", "v", 0})
      :ok = GenServer.call(pid, {:put, "live_future", "v", future})
      :ok = GenServer.call(pid, {:put, "expired_past", "v", past})
      :ok = GenServer.call(pid, {:put, "to_delete", "v", 0})
      :ok = GenServer.call(pid, {:delete, "to_delete"})

      keys = GenServer.call(pid, :keys)
      assert "live_no_exp" in keys
      assert "live_future" in keys
      refute "expired_past" in keys
      refute "to_delete" in keys
    end
  end

  describe "Shard: re-put after delete makes key accessible again" do
    test "key comes back after delete + put" do
      {pid, _index, dir} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
        File.rm_rf!(dir)
      end)

      :ok = GenServer.call(pid, {:put, "revive", "v1", 0})
      :ok = GenServer.call(pid, {:delete, "revive"})
      assert nil == GenServer.call(pid, {:get, "revive"})

      :ok = GenServer.call(pid, {:put, "revive", "v2", 0})
      assert "v2" == GenServer.call(pid, {:get, "revive"})
      assert true == GenServer.call(pid, {:exists, "revive"})
    end
  end
end
