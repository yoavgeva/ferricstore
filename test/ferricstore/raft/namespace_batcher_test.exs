defmodule Ferricstore.Raft.NamespaceBatcherTest do
  @moduledoc """
  Tests for the namespace-aware group commit batcher (spec section 2F.3).

  Validates that the batcher correctly:
    * Extracts key prefixes from commands
    * Maintains independent per-namespace buffers with separate timers
    * Uses per-namespace `window_ms` from the `:ferricstore_ns_config` ETS table
    * Falls back to defaults (1 ms, :quorum) for unconfigured prefixes
    * Exposes namespace metrics via Prometheus scrape

  These tests reuse the application-supervised ra system and shard servers.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    # Reset namespace config between tests to avoid cross-contamination
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper to generate unique keys with a specific prefix
  defp pkey(prefix, base), do: "#{prefix}:nsbatcher_#{base}_#{:rand.uniform(9_999_999)}"

  # Helper to generate a key without a colon (root namespace)
  defp rootkey(base), do: "nsbatcher_root_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # Prefix extraction
  # ---------------------------------------------------------------------------

  describe "extract_prefix/1" do
    test "extracts prefix before first colon" do
      assert "session" == Batcher.extract_prefix({:put, "session:abc123", "v", 0})
    end

    test "extracts first segment for multi-colon keys" do
      assert "ts" == Batcher.extract_prefix({:put, "ts:sensor:42", "v", 0})
    end

    test "returns _root for keys without colon" do
      assert "_root" == Batcher.extract_prefix({:delete, "nocolon"})
    end

    test "works with all command types" do
      assert "a" == Batcher.extract_prefix({:put, "a:b", "v", 0})
      assert "b" == Batcher.extract_prefix({:delete, "b:c"})
      assert "c" == Batcher.extract_prefix({:incr_float, "c:d", 1.0})
      assert "d" == Batcher.extract_prefix({:append, "d:e", "suf"})
      assert "e" == Batcher.extract_prefix({:getset, "e:f", "v"})
      assert "f" == Batcher.extract_prefix({:getdel, "f:g"})
      assert "g" == Batcher.extract_prefix({:getex, "g:h", 1000})
      assert "h" == Batcher.extract_prefix({:setrange, "h:i", 0, "v"})
    end

    test "empty key returns _root" do
      assert "_root" == Batcher.extract_prefix({:delete, ""})
    end

    test "key starting with colon has empty prefix" do
      assert "" == Batcher.extract_prefix({:delete, ":leadingcolon"})
    end
  end

  # ---------------------------------------------------------------------------
  # Independent namespace batching
  # ---------------------------------------------------------------------------

  describe "independent namespace batching" do
    test "commands with different prefixes are written successfully" do
      k1 = pkey("alpha", "ind1")
      k2 = pkey("beta", "ind2")
      k3 = rootkey("ind3")

      shard1 = Router.shard_for(k1)
      shard2 = Router.shard_for(k2)
      shard3 = Router.shard_for(k3)

      assert :ok == Batcher.write(shard1, {:put, k1, "val_alpha", 0})
      assert :ok == Batcher.write(shard2, {:put, k2, "val_beta", 0})
      assert :ok == Batcher.write(shard3, {:put, k3, "val_root", 0})

      assert "val_alpha" == Router.get(k1)
      assert "val_beta" == Router.get(k2)
      assert "val_root" == Router.get(k3)
    end

    test "commands with same prefix are batched together" do
      # Generate multiple keys with the same prefix, all hashing to the same shard
      prefix = "batchtest"

      keys =
        for i <- 1..20 do
          pkey(prefix, "same_#{i}")
        end

      # Group by shard and pick one shard with multiple keys
      by_shard = Enum.group_by(keys, &Router.shard_for/1)
      {shard_idx, shard_keys} = Enum.max_by(by_shard, fn {_, ks} -> length(ks) end)

      # Send all writes concurrently -- they should be batched within the same
      # namespace slot
      tasks =
        Enum.map(shard_keys, fn k ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, k, "batched_ns", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for k <- shard_keys do
        assert "batched_ns" == Router.get(k)
      end
    end

    test "different prefixes on the same shard are batched independently" do
      # Create keys with different prefixes but force them to the same shard
      # by trying multiple random suffixes until we find matches
      shard_idx = 0

      alpha_key =
        Enum.find(1..1000, fn i ->
          k = "alpha:indep_#{i}"
          Router.shard_for(k) == shard_idx
        end)
        |> then(fn i -> "alpha:indep_#{i}" end)

      beta_key =
        Enum.find(1..1000, fn i ->
          k = "beta:indep_#{i}"
          Router.shard_for(k) == shard_idx
        end)
        |> then(fn i -> "beta:indep_#{i}" end)

      # Configure different windows (both still short enough for test)
      NamespaceConfig.set("alpha", "window_ms", "1")
      NamespaceConfig.set("beta", "window_ms", "1")

      # Write both -- they use separate slots internally
      assert :ok == Batcher.write(shard_idx, {:put, alpha_key, "alpha_val", 0})
      assert :ok == Batcher.write(shard_idx, {:put, beta_key, "beta_val", 0})

      assert "alpha_val" == Router.get(alpha_key)
      assert "beta_val" == Router.get(beta_key)
    end
  end

  # ---------------------------------------------------------------------------
  # Per-namespace window_ms configuration
  # ---------------------------------------------------------------------------

  describe "per-namespace window_ms" do
    test "uses configured window_ms for a namespace" do
      NamespaceConfig.set("fast", "window_ms", "1")
      NamespaceConfig.set("slow", "window_ms", "50")

      # Verify that the config is actually read correctly
      assert 1 == NamespaceConfig.window_for("fast")
      assert 50 == NamespaceConfig.window_for("slow")

      # Both should work for writes, just with different batching windows
      k_fast = pkey("fast", "wtest")
      k_slow = pkey("slow", "wtest")

      shard_fast = Router.shard_for(k_fast)
      shard_slow = Router.shard_for(k_slow)

      assert :ok == Batcher.write(shard_fast, {:put, k_fast, "fast_val", 0})
      assert :ok == Batcher.write(shard_slow, {:put, k_slow, "slow_val", 0})

      assert "fast_val" == Router.get(k_fast)
      assert "slow_val" == Router.get(k_slow)
    end

    test "uses default window for unconfigured prefix" do
      assert 1 == NamespaceConfig.default_window_ms()
      assert 1 == NamespaceConfig.window_for("unconfigured_prefix")

      k = pkey("unconfigured_prefix", "default_test")
      shard = Router.shard_for(k)

      assert :ok == Batcher.write(shard, {:put, k, "default_win", 0})
      assert "default_win" == Router.get(k)
    end

    test "root namespace uses default window" do
      k = rootkey("rootwin")
      shard = Router.shard_for(k)

      assert :ok == Batcher.write(shard, {:put, k, "root_val", 0})
      assert "root_val" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Durability mode configuration
  # ---------------------------------------------------------------------------

  describe "durability mode" do
    test "defaults to :quorum for unconfigured prefix" do
      assert :quorum == NamespaceConfig.default_durability()
      assert :quorum == NamespaceConfig.durability_for("noconfig")
    end

    test "async durability commands complete successfully" do
      NamespaceConfig.set("ephemeral", "durability", "async")

      k = pkey("ephemeral", "async_test")
      shard = Router.shard_for(k)

      # Async durability bypasses Raft consensus and writes via
      # AsyncApplyWorker. The write returns immediately; the data
      # becomes visible after the worker processes the batch.
      assert :ok == Batcher.write(shard, {:put, k, "async_val", 0})
      Process.sleep(100)
      assert "async_val" == Router.get(k)
    end

    test "quorum durability commands complete successfully" do
      NamespaceConfig.set("durable", "durability", "quorum")

      k = pkey("durable", "quorum_test")
      shard = Router.shard_for(k)

      assert :ok == Batcher.write(shard, {:put, k, "quorum_val", 0})
      assert "quorum_val" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Flush
  # ---------------------------------------------------------------------------

  describe "flush with namespaces" do
    test "flush drains all namespace slots" do
      k1 = pkey("ns_flush_a", "f1")
      k2 = pkey("ns_flush_b", "f2")

      shard1 = Router.shard_for(k1)
      shard2 = Router.shard_for(k2)

      :ok = Batcher.write(shard1, {:put, k1, "fv1", 0})
      :ok = Batcher.write(shard2, {:put, k2, "fv2", 0})

      # Flush all shards
      for i <- 0..3 do
        :ok = Batcher.flush(i)
      end

      assert "fv1" == Router.get(k1)
      assert "fv2" == Router.get(k2)
    end

    test "flush returns :ok when no slots have pending writes" do
      assert :ok == Batcher.flush(0)
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace metrics
  # ---------------------------------------------------------------------------

  describe "namespace metrics" do
    test "scrape includes namespace window_ms gauge when config exists" do
      NamespaceConfig.set("metrics_ns", "window_ms", "5")

      text = Ferricstore.Metrics.scrape()

      assert String.contains?(text, "# HELP ferricstore_namespace_window_ms")
      assert String.contains?(text, "# TYPE ferricstore_namespace_window_ms gauge")

      assert String.contains?(
               text,
               "ferricstore_namespace_window_ms{prefix=\"metrics_ns\"} 5"
             )
    end

    test "scrape includes namespace durability gauge when config exists" do
      NamespaceConfig.set("metrics_dur", "durability", "async")

      text = Ferricstore.Metrics.scrape()

      assert String.contains?(text, "# HELP ferricstore_namespace_durability")
      assert String.contains?(text, "# TYPE ferricstore_namespace_durability gauge")

      assert String.contains?(
               text,
               "ferricstore_namespace_durability{prefix=\"metrics_dur\",mode=\"async\"} 1"
             )
    end

    test "scrape includes multiple namespace entries" do
      NamespaceConfig.set("ns_a", "window_ms", "2")
      NamespaceConfig.set("ns_a", "durability", "quorum")
      NamespaceConfig.set("ns_b", "window_ms", "10")
      NamespaceConfig.set("ns_b", "durability", "async")

      text = Ferricstore.Metrics.scrape()

      assert String.contains?(text, "ferricstore_namespace_window_ms{prefix=\"ns_a\"} 2")
      assert String.contains?(text, "ferricstore_namespace_window_ms{prefix=\"ns_b\"} 10")

      assert String.contains?(
               text,
               "ferricstore_namespace_durability{prefix=\"ns_a\",mode=\"quorum\"} 1"
             )

      assert String.contains?(
               text,
               "ferricstore_namespace_durability{prefix=\"ns_b\",mode=\"async\"} 1"
             )
    end

    test "scrape omits namespace metrics when no namespaces are configured" do
      NamespaceConfig.reset_all()

      text = Ferricstore.Metrics.scrape()

      refute String.contains?(text, "ferricstore_namespace_window_ms")
      refute String.contains?(text, "ferricstore_namespace_durability")
    end

    test "namespace metrics do not break the base metrics" do
      NamespaceConfig.set("check_base", "window_ms", "3")

      text = Ferricstore.Metrics.scrape()

      # All base metrics should still be present
      base_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_used_memory_bytes",
        "ferricstore_uptime_seconds"
      ]

      for metric <- base_metrics do
        assert String.contains?(text, metric),
               "Expected base metric #{metric} to still be present"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "write with delete command routes to correct namespace" do
      k = pkey("delns", "edge_del")
      shard = Router.shard_for(k)

      :ok = Batcher.write(shard, {:put, k, "to_delete", 0})
      assert "to_delete" == Router.get(k)

      :ok = Batcher.write(shard, {:delete, k})
      assert nil == Router.get(k)
    end

    test "write with append command routes to correct namespace" do
      k = pkey("appns", "edge_app")
      shard = Router.shard_for(k)

      :ok = Batcher.write(shard, {:put, k, "hello", 0})
      {:ok, 10} = Batcher.write(shard, {:append, k, "world"})
      assert "helloworld" == Router.get(k)
    end

    test "concurrent writes across multiple namespaces on same shard" do
      shard_idx = 0

      # Find keys for different prefixes that all hash to shard 0
      keys_by_prefix =
        for prefix <- ["conc_a", "conc_b", "conc_c"] do
          key =
            Enum.find(1..5000, fn i ->
              k = "#{prefix}:conc_#{i}"
              Router.shard_for(k) == shard_idx
            end)
            |> then(fn i -> "#{prefix}:conc_#{i}" end)

          {prefix, key}
        end

      # Write all concurrently
      tasks =
        Enum.map(keys_by_prefix, fn {_prefix, key} ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, key, "concurrent_val", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for {_prefix, key} <- keys_by_prefix do
        assert "concurrent_val" == Router.get(key)
      end
    end

    test "namespace config change is picked up on next write" do
      # Start with default config
      k1 = pkey("dynamic", "cfg1")
      shard = Router.shard_for(k1)
      :ok = Batcher.write(shard, {:put, k1, "before", 0})

      # Change the config
      NamespaceConfig.set("dynamic", "window_ms", "2")

      # Next write should use the new window
      k2 = pkey("dynamic", "cfg2")
      shard2 = Router.shard_for(k2)
      :ok = Batcher.write(shard2, {:put, k2, "after", 0})

      assert "before" == Router.get(k1)
      assert "after" == Router.get(k2)
    end
  end

  # ---------------------------------------------------------------------------
  # Batcher name
  # ---------------------------------------------------------------------------

  describe "batcher_name/1" do
    test "returns expected atom" do
      assert Batcher.batcher_name(0) == :"Ferricstore.Raft.Batcher.0"
      assert Batcher.batcher_name(3) == :"Ferricstore.Raft.Batcher.3"
    end
  end
end
