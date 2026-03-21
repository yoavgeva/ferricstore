defmodule FerricstoreServer.Integration.VectorPersistenceTest do
  @moduledoc """
  Integration tests for HNSW vector persistence across shard restarts.

  Verifies that vectors stored via VADD survive shard crashes and that
  VSEARCH returns correct results after HNSW index rebuild.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Builds a store map backed by the real Router (application-supervised shards).
  defp real_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn -> :ok end,
      dbsize: &Router.dbsize/0,
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
    name = Router.shard_name(index)
    old_pid = Process.whereis(name)
    ref = Process.monitor(old_pid)
    Process.exit(old_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
    wait_for_new_pid(name, old_pid)
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

  defp flush_shard(index) do
    name = Router.shard_name(index)

    case Process.whereis(name) do
      pid when is_pid(pid) -> GenServer.call(pid, :flush, 10_000)
      nil -> :ok
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

      # Flush pending writes to disk
      ShardHelpers.flush_all_shards()
      flush_shard(shard_index)

      # Kill and restart the shard that owns the vector data
      _new_pid = kill_and_wait_restart(shard_index)

      # Small delay for ETS to be rebuilt
      Process.sleep(100)

      # Also need to flush/wait for the shard that owns the VM: metadata key
      meta_shard = Router.shard_for("VM:" <> collection)

      if meta_shard != shard_index do
        # The metadata shard was not restarted, so it should still be valid
        :ok
      end

      # Search again after restart -- HNSW index should have been rebuilt
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
    test "VGET works after restart (Bitcask persistence)" do
      alias Ferricstore.Commands.Vector

      store = real_store()
      collection = "persist_vget"
      dims = 3

      assert :ok = Vector.handle("VCREATE", [collection, "#{dims}", "cosine"], store)

      shard_index = Router.shard_for(collection)

      assert :ok = Vector.handle("VADD", [collection, "mykey", "1.5", "2.5", "3.5"], store)

      # Flush and restart
      ShardHelpers.flush_all_shards()
      flush_shard(shard_index)
      _new_pid = kill_and_wait_restart(shard_index)
      Process.sleep(100)

      # VGET should still work
      result = Vector.handle("VGET", [collection, "mykey"], store)
      assert result == ["1.5", "2.5", "3.5"]
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

      # Flush and restart
      ShardHelpers.flush_all_shards()
      flush_shard(shard_index)
      _new_pid = kill_and_wait_restart(shard_index)
      Process.sleep(100)

      # Count should still be 25
      info_after = Vector.handle("VINFO", [collection], store)
      info_map_after = list_to_info_map(info_after)

      assert info_map_after["vector_count"] == 25,
             "Expected 25 vectors after restart, got #{info_map_after["vector_count"]}"
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

      # Flush and restart
      ShardHelpers.flush_all_shards()
      flush_shard(shard_index)
      _new_pid = kill_and_wait_restart(shard_index)
      Process.sleep(100)

      # Search for x_axis direction
      result = Vector.handle("VSEARCH", [collection, "1.0", "0.0", "0.0", "TOP", "2"], store)
      keys = extract_keys(result)

      # x_axis should be closest (cosine dist = 0), xy_diag should be second
      assert hd(keys) == "x_axis",
             "x_axis should be the closest, got: #{inspect(keys)}"
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

      # Flush and restart
      ShardHelpers.flush_all_shards()
      flush_shard(shard_index)
      _new_pid = kill_and_wait_restart(shard_index)
      Process.sleep(100)

      # Search should only return "keep"
      result =
        Vector.handle("VSEARCH", [collection, "1.0", "0.0", "0.0", "TOP", "10"], store)

      keys = extract_keys(result)
      assert "keep" in keys
      refute "delete_me" in keys, "Deleted vector should not appear after restart"
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
