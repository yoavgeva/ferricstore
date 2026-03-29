defmodule Ferricstore.Store.PrefixInterningTest do
  @moduledoc """
  Tests for prefix interning: a per-shard ETS bag table (`prefix_keys_N`) that
  indexes keys by the text before the first `:` delimiter. This enables
  SCAN MATCH 'prefix:*' to resolve in O(matching keys) instead of O(total keys).
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias Ferricstore.Store.{PrefixIndex, Router}
  alias Ferricstore.Commands.{Generic, Server}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn ->
      ShardHelpers.flush_all_keys()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Builds a store map backed by the real Router (application-supervised shards).
  defp real_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      keys_with_prefix: &Router.keys_with_prefix/1,
      flush: fn -> :ok end,
      dbsize: &Router.dbsize/0
    }
  end

  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  # -------------------------------------------------------------------
  # PrefixIndex unit tests
  # -------------------------------------------------------------------

  describe "PrefixIndex.extract_prefix/1" do
    test "extracts prefix before first colon" do
      assert PrefixIndex.extract_prefix("session:abc123") == "session"
    end

    test "extracts prefix with multiple colons (only splits on first)" do
      assert PrefixIndex.extract_prefix("user:profile:42") == "user"
    end

    test "returns nil for keys without colon" do
      assert PrefixIndex.extract_prefix("simplekey") == nil
    end

    test "returns nil for empty key" do
      assert PrefixIndex.extract_prefix("") == nil
    end

    test "handles colon at the beginning" do
      assert PrefixIndex.extract_prefix(":orphan") == ""
    end

    test "handles colon at the end" do
      assert PrefixIndex.extract_prefix("trailing:") == "trailing"
    end
  end

  # -------------------------------------------------------------------
  # Prefix index maintenance on PUT
  # -------------------------------------------------------------------

  describe "prefix index updated on PUT" do
    test "inserting a prefixed key adds it to the prefix index" do
      store = real_store()
      key = ukey("session:abc")
      store.put.(key, "value1", 0)

      # The key should appear when querying the prefix index for "session"
      # through the Router
      keys = Router.keys_with_prefix("session")
      assert key in keys
    end

    test "inserting a non-prefixed key does not pollute prefix index" do
      store = real_store()
      key = ukey("nocolon")
      store.put.(key, "val", 0)

      # No prefix extracted, so querying any prefix should not return it
      assert ukey("nocolon") not in Router.keys_with_prefix("nocolon")
    end

    test "overwriting a key does not duplicate in prefix index" do
      store = real_store()
      key = "session:dup_test_#{:rand.uniform(9_999_999)}"
      store.put.(key, "v1", 0)
      store.put.(key, "v2", 0)

      # The key should appear exactly once
      matches = Router.keys_with_prefix("session")
      assert Enum.count(matches, &(&1 == key)) == 1
    end
  end

  # -------------------------------------------------------------------
  # Prefix index maintenance on DELETE
  # -------------------------------------------------------------------

  describe "prefix index updated on DELETE" do
    test "deleting a prefixed key removes it from the prefix index" do
      store = real_store()
      key = "session:del_test_#{:rand.uniform(9_999_999)}"
      store.put.(key, "val", 0)
      assert key in Router.keys_with_prefix("session")

      store.delete.(key)
      refute key in Router.keys_with_prefix("session")
    end

    test "deleting a non-existent key is safe" do
      store = real_store()
      store.delete.("session:nonexistent_#{:rand.uniform(9_999_999)}")
      # Should not crash — just a no-op
    end
  end

  # -------------------------------------------------------------------
  # SCAN MATCH 'prefix:*' returns only matching keys
  # -------------------------------------------------------------------

  describe "SCAN MATCH 'prefix:*' returns only matching keys" do
    test "SCAN MATCH 'session:*' only returns session keys" do
      store = real_store()

      session_keys =
        for i <- 1..5 do
          k = "session:s#{i}_#{:rand.uniform(9_999_999)}"
          store.put.(k, "val#{i}", 0)
          k
        end

      user_keys =
        for i <- 1..3 do
          k = "user:u#{i}_#{:rand.uniform(9_999_999)}"
          store.put.(k, "val#{i}", 0)
          k
        end

      # SCAN with MATCH 'session:*' should only return session keys
      all_scan_results = scan_all(store, "session:*")

      for sk <- session_keys do
        assert sk in all_scan_results, "Expected #{sk} in SCAN results"
      end

      for uk <- user_keys do
        refute uk in all_scan_results, "Did not expect #{uk} in SCAN results"
      end
    end

    test "SCAN without MATCH returns all keys" do
      store = real_store()

      keys =
        for prefix <- ["a", "b", "c"], i <- 1..2 do
          k = "#{prefix}:key#{i}_#{:rand.uniform(9_999_999)}"
          store.put.(k, "v", 0)
          k
        end

      all_results = scan_all(store, nil)

      for k <- keys do
        assert k in all_results
      end
    end

    test "SCAN MATCH with non-prefix pattern still works (regex fallback)" do
      store = real_store()
      k1 = "hello_world_#{:rand.uniform(9_999_999)}"
      k2 = "hello_test_#{:rand.uniform(9_999_999)}"
      k3 = "goodbye_#{:rand.uniform(9_999_999)}"
      store.put.(k1, "v1", 0)
      store.put.(k2, "v2", 0)
      store.put.(k3, "v3", 0)

      results = scan_all(store, "hello_*")
      assert k1 in results
      assert k2 in results
      refute k3 in results
    end

    test "SCAN MATCH 'prefix:*' excludes expired keys" do
      store = real_store()
      live_key = "cache:live_#{:rand.uniform(9_999_999)}"
      expired_key = "cache:expired_#{:rand.uniform(9_999_999)}"

      store.put.(live_key, "alive", 0)
      # Set expiry to 1ms in the past
      store.put.(expired_key, "dead", System.os_time(:millisecond) - 1)

      Process.sleep(5)
      results = scan_all(store, "cache:*")
      assert live_key in results
      refute expired_key in results
    end
  end

  # -------------------------------------------------------------------
  # KEYS command with prefix pattern
  # -------------------------------------------------------------------

  describe "KEYS with prefix pattern" do
    test "KEYS 'order:*' only returns order keys" do
      store = real_store()

      order_keys =
        for i <- 1..3 do
          k = "order:o#{i}_#{:rand.uniform(9_999_999)}"
          store.put.(k, "v", 0)
          k
        end

      other_key = "item:x_#{:rand.uniform(9_999_999)}"
      store.put.(other_key, "v", 0)

      result = Server.handle("KEYS", ["order:*"], store)
      for ok <- order_keys, do: assert(ok in result)
      refute other_key in result
    end
  end

  # -------------------------------------------------------------------
  # Performance: 100K keys across 10 prefixes
  # -------------------------------------------------------------------

  describe "performance: prefix scan is faster than full scan" do
    @tag :bench
    @tag timeout: 300_000
    test "SCAN MATCH 'target:*' uses prefix index for O(matching) lookup" do
      store = real_store()

      # Insert 2K keys across 10 prefixes (200 per prefix).
      # With Raft consensus this takes significant time but is manageable.
      prefixes = for i <- 1..10, do: "perf_pfx#{i}"

      for prefix <- prefixes, i <- 1..200 do
        store.put.("#{prefix}:key#{i}", "v", 0)
      end

      # Time a prefix scan (should use prefix index)
      {prefix_us, prefix_results} =
        :timer.tc(fn ->
          scan_all(store, "perf_pfx1:*")
        end)

      # Time a full scan without prefix (has to scan all keys)
      {full_us, full_results} =
        :timer.tc(fn ->
          scan_all(store, nil)
        end)

      prefix_ms = prefix_us / 1_000
      full_ms = full_us / 1_000

      assert length(prefix_results) == 200
      assert length(full_results) == 2_000

      # The prefix scan should complete quickly (under 50ms)
      assert prefix_ms < 50,
             "Prefix SCAN took #{prefix_ms}ms, expected < 50ms"

      # The prefix scan should be faster than the full scan
      # (or at least comparable if the dataset is small)
      assert prefix_ms <= full_ms + 10,
             "Prefix SCAN (#{prefix_ms}ms) should be no slower than full SCAN (#{full_ms}ms)"
    end
  end

  # -------------------------------------------------------------------
  # Stress: concurrent writes to different prefixes while scanning
  # -------------------------------------------------------------------

  describe "concurrent writes and scans" do
    test "concurrent writes to different prefixes while scanning" do
      store = real_store()

      # Pre-populate some keys
      for i <- 1..100 do
        store.put.("alpha:#{i}", "v", 0)
        store.put.("beta:#{i}", "v", 0)
      end

      # Spawn writers that continuously add keys to different prefixes
      writer_tasks =
        for prefix <- ["gamma", "delta", "epsilon"] do
          Task.async(fn ->
            for i <- 1..200 do
              store.put.("#{prefix}:#{i}", "v#{i}", 0)
            end
          end)
        end

      # Simultaneously scan for alpha keys
      scan_task =
        Task.async(fn ->
          for _ <- 1..10 do
            results = scan_all(store, "alpha:*")
            # Alpha should always have exactly 100 keys (writers don't touch alpha)
            assert length(results) == 100
          end

          :ok
        end)

      # Wait for all tasks
      Enum.each(writer_tasks, &Task.await(&1, 10_000))
      assert Task.await(scan_task, 10_000) == :ok

      # Verify the written keys are scannable
      for prefix <- ["gamma", "delta", "epsilon"] do
        results = scan_all(store, "#{prefix}:*")
        assert length(results) == 200, "Expected 200 #{prefix} keys, got #{length(results)}"
      end
    end
  end

  # -------------------------------------------------------------------
  # Edge cases
  # -------------------------------------------------------------------

  describe "edge cases" do
    test "key with empty prefix (starts with colon)" do
      store = real_store()
      key = ":empty_prefix_#{:rand.uniform(9_999_999)}"
      store.put.(key, "v", 0)

      results = Router.keys_with_prefix("")
      assert key in results
    end

    test "key with only colons" do
      store = real_store()
      key = ":::"
      store.put.(key, "v", 0)

      results = Router.keys_with_prefix("")
      assert key in results

      store.delete.(key)
    end

    test "prefix index survives shard restart" do
      store = real_store()
      key = "persist:restart_test_#{:rand.uniform(9_999_999)}"
      store.put.(key, "durable", 0)

      # Flush to disk
      ShardHelpers.flush_all_shards()

      # Kill and wait for shard restart
      idx = Router.shard_for(key)
      shard_pid = Process.whereis(Router.shard_name(idx))
      Process.exit(shard_pid, :kill)
      ShardHelpers.wait_shards_alive()

      # After restart, the key should be discoverable via SCAN
      # (prefix index rebuilt from keydir on init)
      ShardHelpers.eventually(fn ->
        results = scan_all(store, "persist:*")
        key in results
      end, "prefix index should survive shard restart")
    end
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  # Runs a full SCAN iteration (cursor 0 -> 0) collecting all keys.
  defp scan_all(store, match_pattern) do
    scan_all(store, "0", match_pattern, [])
  end

  defp scan_all(store, cursor, match_pattern, acc) do
    opts =
      if match_pattern do
        [cursor | ["MATCH", match_pattern, "COUNT", "1000"]]
      else
        [cursor | ["COUNT", "1000"]]
      end

    case Generic.handle("SCAN", opts, store) do
      [next_cursor, batch] ->
        new_acc = acc ++ batch

        if next_cursor == "0" do
          Enum.uniq(new_acc)
        else
          scan_all(store, next_cursor, match_pattern, new_acc)
        end

      {:error, _} = err ->
        raise "SCAN failed: #{inspect(err)}"
    end
  end
end
