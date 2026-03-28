defmodule FerricstoreServer.Integration.VectorPersistenceTest do
  @moduledoc """
  Integration tests for HNSW vector persistence across shard restarts.

  Verifies that vectors stored via VADD survive shard crashes and that
  VSEARCH returns correct results after HNSW index rebuild.
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()

    # Clean up HNSW vector files from previous test runs.
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore_test")
    vectors_dir = Path.join(data_dir, "vectors/test_direct")
    File.rm_rf!(vectors_dir)
    File.mkdir_p!(vectors_dir)

    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Builds a store map backed by the real Router (application-supervised shards).
  #
  # Vector commands (VCREATE, VADD, VSEARCH, etc.) use file-backed HNSW NIFs
  # that store vector data in `vectors_dir`. The directory is derived from the
  # application's data_dir to match the shard's real store map.
  defp real_store do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore_test")
    vectors_dir = Path.join(data_dir, "vectors/test_direct")
    File.mkdir_p!(vectors_dir)

    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn -> :ok end,
      dbsize: &Router.dbsize/0,
      vectors_dir: fn -> vectors_dir end,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  defp kill_and_wait_restart(index) do
    # Flush the Raft batcher first to ensure any pending batched writes
    # are submitted and applied before we kill the shard.
    Ferricstore.Raft.Batcher.flush(index)
    Ferricstore.Raft.AsyncApplyWorker.drain(index)

    # Flush the shard's pending writes to disk before killing.
    # Without this, group-committed entries may be lost on hard kill.
    name = Router.shard_name(index)
    GenServer.call(name, :flush, 5_000)

    old_pid = Process.whereis(name)
    ref = Process.monitor(old_pid)
    Process.exit(old_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
    new_pid = wait_for_new_pid(name, old_pid)

    # Wait for the shard's ra server to elect a leader. Without a leader,
    # any subsequent Raft writes would fail. The shard init calls
    # Cluster.start_shard_server which blocks for leader election, but
    # we verify explicitly to guard against edge cases in CI.
    wait_raft_leader(index)

    # Verify the shard is responsive: a simple GenServer.call ensures
    # that the shard has finished its init (including ETS warming from
    # Bitcask and HNSW index rebuild).
    GenServer.call(name, :flush, 10_000)

    new_pid
  end

  defp wait_for_new_pid(name, old_pid, attempts \\ 40)

  defp wait_for_new_pid(_name, _old_pid, 0) do
    raise "Shard did not restart within the expected time"
  end

  defp wait_for_new_pid(name, old_pid, attempts) do
    case Process.whereis(name) do
      nil ->
        :timer.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      ^old_pid ->
        :timer.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      new_pid ->
        new_pid
    end
  end

  defp wait_raft_leader(index, attempts \\ 50)

  defp wait_raft_leader(_index, 0), do: :ok

  defp wait_raft_leader(index, attempts) do
    server_id = Ferricstore.Raft.Cluster.shard_server_id(index)

    case :ra.members(server_id) do
      {:ok, _members, _leader} ->
        :ok

      _ ->
        Process.sleep(20)
        wait_raft_leader(index, attempts - 1)
    end
  end

  defp eventually(fun, msg \\ "condition not met", attempts \\ 100) do
    result =
      try do
        fun.()
      rescue
        _ -> false
      catch
        :exit, _ -> false
      end

    if result do
      :ok
    else
      if attempts > 1 do
        Process.sleep(100)
        eventually(fun, msg, attempts - 1)
      else
        flunk("Timed out: #{msg}")
      end
    end
  end

  defp extract_keys(result) do
    result
    |> Enum.chunk_every(2)
    |> Enum.map(fn [k, _d] -> k end)
  end

  # ===========================================================================
  # Vector persistence tests
  # ===========================================================================

  describe "HNSW vector persistence across shard restart" do
    @tag timeout: 30_000
    test "VADD 100 vectors, restart shard, VSEARCH returns correct results" do
      alias Ferricstore.Commands.Vector

      store = real_store()
      collection = "persist_test_100"
      dims = 8

      # Create collection
      assert :ok = Vector.handle("VCREATE", [collection, "#{dims}", "l2"], store)

      # Determine which shard owns this collection's vectors
      shard_index = Router.shard_for(collection)

      # Add 100 vectors with deterministic values
      for i <- 0..99 do
        components = for d <- 0..(dims - 1), do: "#{(i + d) * 1.0}"
        assert :ok = Vector.handle("VADD", [collection, "v#{i}" | components], store)
      end

      # Verify search works before restart
      query = for d <- 0..(dims - 1), do: "#{(50 + d) * 1.0}"
      result_before = Vector.handle("VSEARCH", [collection | query] ++ ["TOP", "5"], store)
      keys_before = extract_keys(result_before)
      assert "v50" in keys_before, "v50 should be in results before restart"

      # Kill and restart the shard that owns the vector data.
      # kill_and_wait_restart flushes the Batcher/AsyncApplyWorker,
      # waits for the new PID, Raft leader, and shard responsiveness.
      _new_pid = kill_and_wait_restart(shard_index)

      # Search again after restart -- HNSW index should have been rebuilt.
      # On slow CI runners, the HNSW index rebuild may take additional time
      # after the shard GenServer reports ready, so retry.
      eventually(fn ->
        result_after = Vector.handle("VSEARCH", [collection | query] ++ ["TOP", "5"], store)
        is_list(result_after) and length(result_after) >= 10
      end, "VSEARCH should return results after restart")

      result_after = Vector.handle("VSEARCH", [collection | query] ++ ["TOP", "5"], store)
      keys_after = extract_keys(result_after)

      assert length(keys_after) == 5,
             "Expected 5 results after restart, got #{length(keys_after)}"

      assert "v50" in keys_after,
             "v50 should be in results after restart, got: #{inspect(keys_after)}"

      # The closest vector should be v50 (exact match = distance 0)
      [first_key, first_dist_str | _] = result_after
      assert first_key == "v50", "First result should be v50, got #{first_key}"
      first_dist = String.to_float(first_dist_str)
      assert first_dist < 1.0e-6, "Distance to exact match should be ~0, got #{first_dist}"
    end

    @tag timeout: 30_000
    test "VSEARCH finds exact vector after restart (mmap persistence)" do
      alias Ferricstore.Commands.Vector

      store = real_store()
      collection = "persist_vget"
      dims = 3

      assert :ok = Vector.handle("VCREATE", [collection, "#{dims}", "cosine"], store)

      shard_index = Router.shard_for(collection)

      assert :ok = Vector.handle("VADD", [collection, "mykey", "1.5", "2.5", "3.5"], store)
      assert :ok = Vector.handle("VADD", [collection, "other", "0.0", "0.0", "1.0"], store)

      # Kill and restart
      _new_pid = kill_and_wait_restart(shard_index)

      # VSEARCH for the exact vector should return "mykey" as the closest.
      # Retry until HNSW index rebuild completes on slow CI.
      eventually(fn ->
        result = Vector.handle("VSEARCH", [collection, "1.5", "2.5", "3.5", "TOP", "1"], store)
        is_list(result) and length(result) >= 2 and hd(extract_keys(result)) == "mykey"
      end, "VSEARCH should find 'mykey' after restart")
    end

    @tag timeout: 30_000
    test "VINFO shows correct count after restart" do
      alias Ferricstore.Commands.Vector

      store = real_store()
      collection = "persist_vinfo"
      dims = 4

      assert :ok = Vector.handle("VCREATE", [collection, "#{dims}", "l2"], store)

      shard_index = Router.shard_for(collection)

      for i <- 0..24 do
        components = for d <- 0..(dims - 1), do: "#{(i + d) * 1.0}"
        assert :ok = Vector.handle("VADD", [collection, "v#{i}" | components], store)
      end

      # Verify count before restart
      info_before = Vector.handle("VINFO", [collection], store)
      info_map_before = list_to_info_map(info_before)
      assert info_map_before["vector_count"] == 25

      # Kill and restart
      _new_pid = kill_and_wait_restart(shard_index)

      # Count should still be 25. Retry until HNSW index rebuild completes.
      eventually(fn ->
        info_after = Vector.handle("VINFO", [collection], store)
        is_list(info_after) and length(info_after) >= 2 and
          list_to_info_map(info_after)["vector_count"] == 25
      end, "Expected 25 vectors after restart")
    end

    @tag timeout: 30_000
    test "VSEARCH with cosine metric works after restart" do
      alias Ferricstore.Commands.Vector

      store = real_store()
      collection = "persist_cosine"
      dims = 3

      assert :ok = Vector.handle("VCREATE", [collection, "#{dims}", "cosine"], store)

      shard_index = Router.shard_for(collection)

      # Add vectors in different directions
      assert :ok = Vector.handle("VADD", [collection, "x_axis", "1.0", "0.0", "0.0"], store)
      assert :ok = Vector.handle("VADD", [collection, "y_axis", "0.0", "1.0", "0.0"], store)
      assert :ok = Vector.handle("VADD", [collection, "z_axis", "0.0", "0.0", "1.0"], store)
      assert :ok = Vector.handle("VADD", [collection, "xy_diag", "1.0", "1.0", "0.0"], store)

      # Kill and restart
      _new_pid = kill_and_wait_restart(shard_index)

      # Search for x_axis direction. Retry until HNSW index rebuild completes.
      eventually(fn ->
        result = Vector.handle("VSEARCH", [collection, "1.0", "0.0", "0.0", "TOP", "2"], store)
        is_list(result) and length(result) >= 4 and hd(extract_keys(result)) == "x_axis"
      end, "x_axis should be the closest after restart")
    end

    @tag timeout: 30_000
    test "VDEL before restart is reflected in post-restart VSEARCH" do
      alias Ferricstore.Commands.Vector

      store = real_store()
      collection = "persist_vdel"
      dims = 3

      assert :ok = Vector.handle("VCREATE", [collection, "#{dims}", "l2"], store)

      shard_index = Router.shard_for(collection)

      assert :ok = Vector.handle("VADD", [collection, "keep", "1.0", "0.0", "0.0"], store)
      assert :ok = Vector.handle("VADD", [collection, "delete_me", "1.1", "0.0", "0.0"], store)

      # Delete one vector
      assert 1 = Vector.handle("VDEL", [collection, "delete_me"], store)

      # Kill and restart
      _new_pid = kill_and_wait_restart(shard_index)

      # Search should only return "keep". Retry until HNSW index rebuild completes.
      eventually(fn ->
        result =
          Vector.handle("VSEARCH", [collection, "1.0", "0.0", "0.0", "TOP", "10"], store)

        is_list(result) and length(result) >= 2 and
          "keep" in extract_keys(result) and "delete_me" not in extract_keys(result)
      end, "Deleted vector should not appear and 'keep' should be present after restart")
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp list_to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
