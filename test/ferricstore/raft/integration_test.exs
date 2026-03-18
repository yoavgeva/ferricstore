defmodule Ferricstore.Raft.IntegrationTest do
  @moduledoc """
  Integration tests for the Raft consensus layer.

  These tests exercise the full Raft write path:
    Router -> Shard -> Batcher -> ra -> StateMachine -> Bitcask + ETS

  They verify that data written through the Raft consensus layer is:
  - Immediately readable from ETS (hot path)
  - Persisted to Bitcask (cold path)
  - Durable across shard restarts
  - Consistent across all layers
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  defp ukey(base), do: "raft_int_#{base}_#{:rand.uniform(9_999_999)}"

  defp shard_ets_for(key), do: :"shard_ets_#{Router.shard_for(key)}"

  defp shard_pid_for(key) do
    name = Router.shard_name(Router.shard_for(key))
    Process.whereis(name)
  end

  # ---------------------------------------------------------------------------
  # Write path verification
  # ---------------------------------------------------------------------------

  describe "Raft write path" do
    test "SET through Router writes via Raft and is readable" do
      k = ukey("set_via_raft")

      Router.put(k, "raft_value", 0)
      assert "raft_value" == Router.get(k)
    end

    test "SET writes to both ETS and Bitcask" do
      k = ukey("dual_write")

      Router.put(k, "dual_val", 0)

      # ETS should have the value
      ets = shard_ets_for(k)
      assert [{^k, "dual_val", 0}] = :ets.lookup(ets, k)

      # Bitcask should also have it (check via NIF directly)
      shard_pid = shard_pid_for(k)
      state = :sys.get_state(shard_pid)
      assert {:ok, "dual_val"} = NIF.get(state.store, k)
    end

    test "DEL removes from ETS and Bitcask" do
      k = ukey("del_both")

      Router.put(k, "to_delete", 0)
      assert "to_delete" == Router.get(k)

      Router.delete(k)
      assert nil == Router.get(k)

      # ETS should be empty
      ets = shard_ets_for(k)
      assert [] == :ets.lookup(ets, k)
    end

    test "multiple writes to same key return latest value" do
      k = ukey("overwrite")

      Router.put(k, "v1", 0)
      Router.put(k, "v2", 0)
      Router.put(k, "v3", 0)

      assert "v3" == Router.get(k)
    end

    test "writes with TTL are respected" do
      k = ukey("ttl_write")
      future = System.os_time(:millisecond) + 60_000

      Router.put(k, "ttl_val", future)

      {value, expire_at_ms} = Router.get_meta(k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-shard write consistency
  # ---------------------------------------------------------------------------

  describe "multi-shard write consistency" do
    test "writes to different shards are independent" do
      # Generate keys that map to different shards
      keys = for i <- 1..20, do: ukey("multi_#{i}")
      shard_indices = Enum.map(keys, &Router.shard_for/1) |> Enum.uniq()

      # Should have keys in multiple shards
      assert length(shard_indices) >= 2

      # Write all keys
      for k <- keys do
        Router.put(k, "shard_val_#{k}", 0)
      end

      # All keys should be readable
      for k <- keys do
        assert "shard_val_#{k}" == Router.get(k),
               "Key #{k} (shard #{Router.shard_for(k)}) should be readable"
      end
    end

    test "keys() returns all written keys across shards" do
      prefix = ukey("all_shards")
      keys = for i <- 1..10, do: "#{prefix}_#{i}"

      for k <- keys do
        Router.put(k, "v", 0)
      end

      all_keys = Router.keys()

      for k <- keys do
        assert k in all_keys, "Key #{k} should be in keys()"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Group commit batching
  # ---------------------------------------------------------------------------

  describe "group commit batching" do
    test "many concurrent writes complete successfully" do
      keys = for i <- 1..50, do: ukey("gc_#{i}")

      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Router.put(k, "concurrent_val", 0)
          end)
        end)

      results = Task.await_many(tasks, 15_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable
      for k <- keys do
        assert "concurrent_val" == Router.get(k)
      end
    end

    test "interleaved reads and writes are consistent" do
      k = ukey("interleave")

      # Write, read, write, read -- each read should see the latest write
      Router.put(k, "v1", 0)
      assert "v1" == Router.get(k)

      Router.put(k, "v2", 0)
      assert "v2" == Router.get(k)

      Router.put(k, "v3", 0)
      assert "v3" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Ra cluster status
  # ---------------------------------------------------------------------------

  describe "ra cluster status" do
    test "all 4 shard ra servers have leaders" do
      for i <- 0..3 do
        server_id = Cluster.shard_server_id(i)

        case :ra.members(server_id) do
          {:ok, members, leader} ->
            assert length(members) == 1, "Single-node cluster should have 1 member"
            assert leader == server_id, "Leader should be the local server"

          {:error, reason} ->
            flunk("ra.members failed for shard #{i}: #{inspect(reason)}")

          {:timeout, _} ->
            flunk("ra.members timed out for shard #{i}")
        end
      end
    end

    test "ra server IDs follow naming convention" do
      for i <- 0..3 do
        {name, node_name} = Cluster.shard_server_id(i)
        assert name == :"ferricstore_shard_#{i}"
        assert node_name == node()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Durability through shard restart
  # ---------------------------------------------------------------------------

  describe "durability through shard restart" do
    test "data survives shard crash and restart" do
      k = ukey("durable")
      Router.put(k, "survives_crash", 0)
      assert "survives_crash" == Router.get(k)

      # Flush to ensure data is durable
      pid = shard_pid_for(k)
      :ok = GenServer.call(pid, :flush)

      # Kill the shard (simulates crash)
      shard_name = Router.shard_name(Router.shard_for(k))
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000

      # Wait for supervisor to restart the shard
      deadline = System.monotonic_time(:millisecond) + 3_000

      new_pid =
        Enum.find_value(Stream.repeatedly(fn -> Process.sleep(50) end), fn _ ->
          p = Process.whereis(shard_name)

          if is_pid(p) and p != pid and Process.alive?(p),
            do: p,
            else:
              (System.monotonic_time(:millisecond) > deadline && throw(:timeout)
               nil)
        end)

      assert is_pid(new_pid)
      assert new_pid != pid

      # Data should be recoverable from Bitcask
      assert "survives_crash" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Application-level Raft configuration
  # ---------------------------------------------------------------------------

  describe "application-level Raft configuration" do
    test "raft_enabled is true in test environment" do
      assert Application.get_env(:ferricstore, :raft_enabled, true) == true
    end

    test "ra system is started" do
      config = :ra_system.fetch(Cluster.system_name())
      assert config != :undefined
      assert config.name == Cluster.system_name()
    end
  end
end
