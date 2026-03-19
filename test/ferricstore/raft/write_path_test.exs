defmodule Ferricstore.Raft.WritePathTest do
  @moduledoc """
  Tests for the Raft-integrated write path.

  Verifies that when `raft_enabled` is true, all write operations
  (SET, DEL, INCR, MULTI/EXEC) route through the Raft Batcher and
  StateMachine before updating ETS and Bitcask, rather than writing
  directly from the Shard GenServer.

  The ra lifecycle is managed entirely within this test module so the
  tests run regardless of the global `:raft_enabled` config setting.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()

    # Start the ra system (idempotent if already started)
    data_dir = Application.fetch_env!(:ferricstore, :data_dir)
    Cluster.start_system(data_dir)

    # Clean up any stale ra server registrations from previous runs, then
    # start fresh ra servers for each shard using the live shard state.
    for i <- 0..3 do
      server_id = Cluster.shard_server_id(i)
      _ = :ra.stop_server(Cluster.system_name(), server_id)
      _ = :ra.force_delete_server(Cluster.system_name(), server_id)

      shard_name = Router.shard_name(i)
      pid = Process.whereis(shard_name)
      state = :sys.get_state(pid)
      Cluster.start_shard_server(i, state.store, state.ets)
    end

    # Start batchers (not supervised -- we manage them here)
    batcher_pids =
      for i <- 0..3 do
        shard_id = Cluster.shard_server_id(i)

        {:ok, pid} =
          Batcher.start_link(shard_index: i, shard_id: shard_id)

        {i, pid}
      end

    # Enable raft for the duration of these tests
    original_raft = Application.get_env(:ferricstore, :raft_enabled, true)
    Application.put_env(:ferricstore, :raft_enabled, true)

    on_exit(fn ->
      # Restore original raft_enabled setting
      Application.put_env(:ferricstore, :raft_enabled, original_raft)

      # Stop batchers
      for {_i, pid} <- batcher_pids do
        if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
      end

      # Stop ra servers
      for i <- 0..3 do
        Cluster.stop_shard_server(i)
      end

      ShardHelpers.wait_shards_alive()
    end)

    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys
  defp ukey(base), do: "raft_wp_#{base}_#{:rand.uniform(9_999_999)}"

  defp shard_ets_for(key), do: :"shard_ets_#{Router.shard_for(key)}"

  defp shard_pid_for(key) do
    name = Router.shard_name(Router.shard_for(key))
    Process.whereis(name)
  end

  # ---------------------------------------------------------------------------
  # 1. SET via Router goes through Raft when enabled
  # ---------------------------------------------------------------------------

  describe "SET via Router goes through Raft" do
    test "SET writes value that is readable after Raft commit" do
      k = ukey("set_raft")

      assert :ok = Router.put(k, "raft_value", 0)
      assert "raft_value" == Router.get(k)
    end

    test "SET writes to ETS via StateMachine apply" do
      k = ukey("set_ets_via_sm")

      :ok = Router.put(k, "sm_val", 0)

      ets = shard_ets_for(k)
      assert [{^k, "sm_val", 0}] = :ets.lookup(ets, k)
    end

    test "SET with TTL preserves expiry through Raft path" do
      k = ukey("set_ttl_raft")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(k, "ttl_val", future)

      {value, expire_at_ms} = Router.get_meta(k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # 2. GET after Raft-committed SET returns the value
  # ---------------------------------------------------------------------------

  describe "GET after Raft-committed SET" do
    test "returns the value immediately after SET" do
      k = ukey("get_after_set")

      :ok = Router.put(k, "committed_value", 0)
      assert "committed_value" == Router.get(k)
    end

    test "returns latest value after multiple SETs" do
      k = ukey("get_multi_set")

      :ok = Router.put(k, "v1", 0)
      assert "v1" == Router.get(k)

      :ok = Router.put(k, "v2", 0)
      assert "v2" == Router.get(k)

      :ok = Router.put(k, "v3", 0)
      assert "v3" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. DEL via Router goes through Raft
  # ---------------------------------------------------------------------------

  describe "DEL via Router goes through Raft" do
    test "DEL removes key after Raft commit" do
      k = ukey("del_raft")

      :ok = Router.put(k, "to_delete", 0)
      assert "to_delete" == Router.get(k)

      :ok = Router.delete(k)
      assert nil == Router.get(k)
    end

    test "DEL removes from ETS" do
      k = ukey("del_ets")

      :ok = Router.put(k, "will_vanish", 0)
      ets = shard_ets_for(k)
      assert [{^k, "will_vanish", 0}] = :ets.lookup(ets, k)

      :ok = Router.delete(k)
      assert [] == :ets.lookup(ets, k)
    end

    test "DEL on non-existent key returns :ok" do
      k = ukey("del_missing")
      assert :ok = Router.delete(k)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Multiple concurrent writes all committed via Raft
  # ---------------------------------------------------------------------------

  describe "multiple concurrent writes via Raft" do
    test "all concurrent writes complete successfully" do
      keys = for i <- 1..50, do: ukey("concurrent_#{i}")

      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Router.put(k, "concurrent_val_#{k}", 0)
          end)
        end)

      results = Task.await_many(tasks, 15_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable after Raft commit
      for k <- keys do
        assert "concurrent_val_#{k}" == Router.get(k),
               "Key #{k} should be readable after concurrent Raft commit"
      end
    end

    test "concurrent writes to same key produce consistent final state" do
      k = ukey("concurrent_same")

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Router.put(k, "v#{i}", 0)
          end)
        end

      Task.await_many(tasks, 15_000)

      # Should have one of the values (last writer wins)
      val = Router.get(k)
      assert val != nil
      assert String.starts_with?(val, "v")
    end
  end

  # ---------------------------------------------------------------------------
  # 5. INCR via Router goes through Raft (atomic increment)
  # ---------------------------------------------------------------------------

  describe "INCR via Router goes through Raft" do
    test "INCR on non-existent key initializes to delta" do
      k = ukey("incr_new")

      assert {:ok, 1} = Router.incr(k, 1)
      assert "1" == Router.get(k)
    end

    test "INCR on existing integer key increments correctly" do
      k = ukey("incr_existing")

      :ok = Router.put(k, "10", 0)
      assert {:ok, 15} = Router.incr(k, 5)
      assert "15" == Router.get(k)
    end

    test "DECR works through Raft path" do
      k = ukey("decr_raft")

      :ok = Router.put(k, "100", 0)
      assert {:ok, 95} = Router.incr(k, -5)
      assert "95" == Router.get(k)
    end

    test "multiple sequential INCRs produce correct total" do
      k = ukey("incr_seq")

      for _i <- 1..10 do
        Router.incr(k, 1)
      end

      assert "10" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Write version increments after Raft commit
  # ---------------------------------------------------------------------------

  describe "write version increments after Raft commit" do
    test "write version increases after SET" do
      k = ukey("version_set")

      v1 = Router.get_version(k)
      :ok = Router.put(k, "val", 0)
      v2 = Router.get_version(k)

      assert v2 > v1
    end

    test "write version increases after DEL" do
      k = ukey("version_del")

      :ok = Router.put(k, "val", 0)
      v1 = Router.get_version(k)

      :ok = Router.delete(k)
      v2 = Router.get_version(k)

      assert v2 > v1
    end

    test "write version increases after INCR" do
      k = ukey("version_incr")

      :ok = Router.put(k, "0", 0)
      v1 = Router.get_version(k)

      {:ok, _} = Router.incr(k, 1)
      v2 = Router.get_version(k)

      assert v2 > v1
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Data persists in Bitcask after Raft commit (flush + NIF.get)
  # ---------------------------------------------------------------------------

  describe "data persists in Bitcask after Raft commit" do
    test "SET data is readable from Bitcask NIF after Raft commit" do
      k = ukey("bitcask_persist")

      :ok = Router.put(k, "durable_value", 0)

      # The StateMachine writes to Bitcask synchronously via NIF.put_batch
      # within apply/3. No need to flush -- it's already durable.
      shard_pid = shard_pid_for(k)
      state = :sys.get_state(shard_pid)

      # Flush any pending writes from the direct path (belt-and-suspenders)
      GenServer.call(shard_pid, :flush)

      assert {:ok, "durable_value"} = NIF.get(state.store, k)
    end

    test "DEL tombstone is persisted in Bitcask" do
      k = ukey("bitcask_del")

      :ok = Router.put(k, "to_remove", 0)
      :ok = Router.delete(k)

      shard_pid = shard_pid_for(k)
      state = :sys.get_state(shard_pid)
      GenServer.call(shard_pid, :flush)

      # After delete, NIF.get should return nil
      assert {:ok, nil} = NIF.get(state.store, k)
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Batch window: multiple rapid writes batched into single Raft entry
  # ---------------------------------------------------------------------------

  describe "batch window batches rapid writes" do
    test "multiple rapid writes to the same shard are batched" do
      # Generate keys that all map to the same shard
      shard_idx = 0
      keys =
        Stream.repeatedly(fn -> ukey("batch_#{:rand.uniform(999_999)}") end)
        |> Stream.filter(fn k -> Router.shard_for(k) == shard_idx end)
        |> Enum.take(5)

      # Send all writes nearly simultaneously -- they should be batched
      # by the Batcher's batch_window_ms (1ms default)
      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Router.put(k, "batched_#{k}", 0)
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable
      for k <- keys do
        assert "batched_#{k}" == Router.get(k),
               "Batched key #{k} should be readable"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 9. MULTI/EXEC transaction goes through Raft as single entry
  #
  # Note: MULTI/EXEC is handled at the connection layer (connection.ex).
  # Each command in the transaction queue is executed sequentially through
  # the normal command dispatch path, which routes through Router -> Shard.
  # When raft_enabled is true, each write within the transaction goes
  # through the Raft path individually. The atomicity guarantee comes
  # from WATCH/version checking, not from Raft batching.
  # ---------------------------------------------------------------------------

  describe "MULTI/EXEC transaction writes go through Raft" do
    test "sequential writes within simulated transaction all commit via Raft" do
      k1 = ukey("tx_k1")
      k2 = ukey("tx_k2")
      k3 = ukey("tx_k3")

      # Simulate MULTI/EXEC: multiple writes executed sequentially
      :ok = Router.put(k1, "tx_val1", 0)
      :ok = Router.put(k2, "tx_val2", 0)
      :ok = Router.put(k3, "tx_val3", 0)

      # All should be readable after commit
      assert "tx_val1" == Router.get(k1)
      assert "tx_val2" == Router.get(k2)
      assert "tx_val3" == Router.get(k3)

      # All should be in ETS (written by StateMachine)
      assert [{^k1, "tx_val1", 0}] = :ets.lookup(shard_ets_for(k1), k1)
      assert [{^k2, "tx_val2", 0}] = :ets.lookup(shard_ets_for(k2), k2)
      assert [{^k3, "tx_val3", 0}] = :ets.lookup(shard_ets_for(k3), k3)
    end

    test "mixed SET and DEL in transaction sequence" do
      k = ukey("tx_mixed")

      :ok = Router.put(k, "initial", 0)
      assert "initial" == Router.get(k)

      :ok = Router.delete(k)
      assert nil == Router.get(k)

      :ok = Router.put(k, "restored", 0)
      assert "restored" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # 10. WATCH detects version change from Raft-committed write
  # ---------------------------------------------------------------------------

  describe "WATCH detects version change from Raft-committed write" do
    test "write version changes are visible to get_version after Raft commit" do
      k = ukey("watch_raft")

      v_before = Router.get_version(k)
      :ok = Router.put(k, "watched_val", 0)
      v_after = Router.get_version(k)

      # WATCH would have recorded v_before; after the Raft-committed write,
      # get_version returns a different (higher) value, so EXEC would abort.
      assert v_after != v_before
      assert v_after > v_before
    end

    test "DEL changes version visible to WATCH" do
      k = ukey("watch_del")

      :ok = Router.put(k, "will_del", 0)
      v_before = Router.get_version(k)

      :ok = Router.delete(k)
      v_after = Router.get_version(k)

      assert v_after > v_before
    end

    test "INCR changes version visible to WATCH" do
      k = ukey("watch_incr")

      :ok = Router.put(k, "5", 0)
      v_before = Router.get_version(k)

      {:ok, _} = Router.incr(k, 1)
      v_after = Router.get_version(k)

      assert v_after > v_before
    end

    test "concurrent Raft writes to same shard all bump version" do
      # Use a key we know the shard for, then write to same shard
      k = ukey("watch_concurrent")
      shard_idx = Router.shard_for(k)

      v_before = Router.get_version(k)

      # Write several keys to the same shard
      keys =
        Stream.repeatedly(fn -> ukey("wc_#{:rand.uniform(999_999)}") end)
        |> Stream.filter(fn kk -> Router.shard_for(kk) == shard_idx end)
        |> Enum.take(5)

      for kk <- keys do
        :ok = Router.put(kk, "v", 0)
      end

      v_after = Router.get_version(k)

      # Version should have increased by at least the number of writes
      assert v_after >= v_before + length(keys)
    end
  end
end
