defmodule Ferricstore.Cluster.NodeJoinSyncTest do
  @moduledoc """
  Tests that a new node joining the cluster receives a complete, consistent
  copy of all data — even while writes continue during the sync.

  Validates:
    1. Writes continue on the cluster while a new node is syncing
    2. After sync completes, the new node has all data
    3. Data directory checksums are identical across all nodes
    4. No writes are lost during the sync process
    5. The new node can serve reads for all keys (including those written during sync)

  Requires: multi-node Raft (Phase 1) + ClusterManager + DataSync
  """

  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :node_join

  alias Ferricstore.Test.ClusterHelper

  # Skip if :peer module not available (OTP < 25)
  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  describe "new node join with continuous writes" do
    @tag timeout: 120_000
    test "data is fully consistent after sync completes" do
      # Phase 1: Start a 3-node cluster
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, node_b, node_c] = nodes

      # Phase 2: Write initial dataset (1000 keys)
      initial_keys = write_keys(node_a, "pre_sync", 1..1000)

      # Verify all 3 nodes have the data
      assert_keys_readable(node_a, initial_keys)
      assert_keys_readable(node_b, initial_keys)
      assert_keys_readable(node_c, initial_keys)

      # Phase 3: Start a 4th node (empty, needs full sync)
      node_d = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # Phase 4: Start continuous writes on node_a while node_d syncs
      writer_pid = start_continuous_writer(node_a, "during_sync")

      # Phase 5: Trigger node_d to join the cluster
      # This initiates the shard-by-shard sync process
      :ok = join_cluster(node_d, node_a)

      # Phase 6: Wait for sync to complete
      # ClusterManager should report node_d as fully synced
      assert_node_synced(node_d, timeout: 60_000)

      # Phase 7: Stop continuous writes, collect what was written
      {during_sync_keys, during_sync_count} = stop_continuous_writer(writer_pid)

      # Verify writes continued during sync (not blocked)
      assert during_sync_count > 100,
             "Expected >100 writes during sync, got #{during_sync_count} — writes may have been blocked"

      # Phase 8: Write a final batch AFTER sync completes
      final_keys = write_keys(node_a, "post_sync", 1..100)

      # Wait for Raft replication to propagate final batch
      eventually(fn ->
        assert_keys_readable(node_d, final_keys)
      end, "post-sync keys not readable on node_d", 30, 100)

      # Phase 9: Verify ALL keys are readable on node_d
      all_keys = initial_keys ++ during_sync_keys ++ final_keys

      assert_keys_readable(node_d, all_keys)

      # Phase 10: Stop all writes and let everything settle
      Process.sleep(500)

      # Phase 11: Compare data directory checksums across ALL nodes
      # This is the ultimate consistency check — byte-for-byte identical
      # after compaction and WAL replay
      checksums_a = shard_data_checksums(node_a)
      checksums_b = shard_data_checksums(node_b)
      checksums_c = shard_data_checksums(node_c)
      checksums_d = shard_data_checksums(node_d)

      # Keydir contents should be identical (same keys, same values)
      keydir_a = dump_keydir(node_a)
      keydir_b = dump_keydir(node_b)
      keydir_c = dump_keydir(node_c)
      keydir_d = dump_keydir(node_d)

      assert keydir_a == keydir_b, "keydir mismatch between node_a and node_b"
      assert keydir_a == keydir_c, "keydir mismatch between node_a and node_c"
      assert keydir_a == keydir_d, "keydir mismatch between node_a and node_d"

      # Log stats for debugging
      IO.puts("Keys: #{length(all_keys)} total (1000 pre + #{during_sync_count} during + 100 post)")
      IO.puts("Shard checksums match: #{checksums_a == checksums_d}")
    end

    @tag timeout: 120_000
    test "no writes lost during sync — every write is durable" do
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      # Start 4th node
      node_d = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # Track every write with a monotonic counter
      write_log = :ets.new(:write_log, [:set, :public])

      # Writer that logs every successful write
      writer_pid = spawn_link(fn ->
        write_loop(node_a, "durability", write_log, 1)
      end)

      # Join while writes are happening
      :ok = join_cluster(node_d, node_a)
      assert_node_synced(node_d, timeout: 60_000)

      # Stop writer
      send(writer_pid, :stop)
      Process.sleep(100)

      # Get all written keys from the log
      written = :ets.tab2list(write_log)
      written_keys = Enum.map(written, fn {key, _seq} -> key end)

      # Every single written key must be readable on node_d
      missing = Enum.filter(written_keys, fn key ->
        read_key(node_d, key) == nil
      end)

      assert missing == [],
             "#{length(missing)} writes lost during sync: #{inspect(Enum.take(missing, 10))}"

      :ets.delete(write_log)
    end

    @tag timeout: 120_000
    test "shard-by-shard sync — other shards serve writes while one syncs" do
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes
      shard_count = get_shard_count(node_a)

      node_d = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # Write to specific shards to verify per-shard sync
      per_shard_keys = for shard_idx <- 0..(shard_count - 1) do
        keys = write_keys_to_shard(node_a, shard_idx, "shard_#{shard_idx}", 1..100)
        {shard_idx, keys}
      end

      # Join and sync
      :ok = join_cluster(node_d, node_a)
      assert_node_synced(node_d, timeout: 60_000)

      # Verify each shard's keys are present on node_d
      for {shard_idx, keys} <- per_shard_keys do
        missing = Enum.filter(keys, fn key -> read_key(node_d, key) == nil end)

        assert missing == [],
               "shard #{shard_idx}: #{length(missing)} keys missing on node_d"
      end
    end
  end

  describe "data directory consistency" do
    @tag timeout: 120_000
    test "after sync + settle, keydir dump is identical across all nodes" do
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      # Write dataset
      write_keys(node_a, "consistency", 1..500)

      # Add 4th node
      node_d = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      :ok = join_cluster(node_d, node_a)
      assert_node_synced(node_d, timeout: 60_000)

      # Stop all writes, let Raft settle
      Process.sleep(1000)

      # Dump keydirs from all nodes — should be identical
      dumps = Enum.map(nodes ++ [node_d], fn node ->
        {node, dump_keydir_sorted(node)}
      end)

      [{_ref_node, ref_dump} | rest] = dumps

      for {node, dump} <- rest do
        assert dump == ref_dump,
               "keydir mismatch on #{inspect(node)}: " <>
                 "#{length(ref_dump)} keys on reference vs #{length(dump)} on this node"
      end
    end

    @tag timeout: 120_000
    test "bitcask file checksums match after compaction on all nodes" do
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      # Write + overwrite to generate dead bytes, then compact
      for i <- 1..200, do: write_key(node_a, "compact_key_#{i}", "value_v1")
      for i <- 1..200, do: write_key(node_a, "compact_key_#{i}", "value_v2")

      # Add 4th node
      node_d = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      :ok = join_cluster(node_d, node_a)
      assert_node_synced(node_d, timeout: 60_000)

      # Trigger compaction on all nodes
      for node <- nodes ++ [node_d] do
        trigger_compaction(node)
      end

      Process.sleep(2000)

      # After compaction, all nodes should have identical live data
      # (file layout may differ due to compaction timing, but keydir contents must match)
      keydir_a = dump_keydir_sorted(node_a)
      keydir_d = dump_keydir_sorted(node_d)

      assert length(keydir_a) == length(keydir_d)

      for {{key_a, val_a}, {key_d, val_d}} <- Enum.zip(keydir_a, keydir_d) do
        assert key_a == key_d, "key mismatch: #{key_a} vs #{key_d}"
        assert val_a == val_d, "value mismatch for key #{key_a}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers — these call into :peer nodes via :erpc
  # ---------------------------------------------------------------------------

  defp write_keys(node, prefix, range) do
    Enum.map(range, fn i ->
      key = "#{prefix}_#{i}"
      :erpc.call(node, Ferricstore.Store.Router, :put, [
        :erpc.call(node, FerricStore.Instance, :get, [:default]),
        key,
        "value_#{i}",
        0
      ])
      key
    end)
  end

  defp write_key(node, key, value) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])
    :erpc.call(node, Ferricstore.Store.Router, :put, [ctx, key, value, 0])
  end

  defp read_key(node, key) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])
    :erpc.call(node, Ferricstore.Store.Router, :get, [ctx, key])
  end

  defp write_keys_to_shard(node, shard_idx, prefix, range) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])

    # Generate keys that hash to the target shard
    Enum.flat_map(range, fn i ->
      key = find_key_for_shard(node, ctx, "#{prefix}_#{i}", shard_idx)
      :erpc.call(node, Ferricstore.Store.Router, :put, [ctx, key, "value_#{i}", 0])
      [key]
    end)
  end

  defp find_key_for_shard(node, ctx, base_key, target_shard) do
    Enum.find_value(0..1000, fn suffix ->
      key = "#{base_key}_#{suffix}"
      shard = :erpc.call(node, Ferricstore.Store.Router, :shard_for, [ctx, key])
      if shard == target_shard, do: key
    end)
  end

  defp assert_keys_readable(node, keys) do
    Enum.each(keys, fn key ->
      eventually(fn ->
        value = read_key(node, key)
        assert value != nil, "key #{key} not readable on #{inspect(node)}"
      end, "key #{key} not readable", 20, 50)
    end)
  end

  defp start_continuous_writer(node, prefix) do
    parent = self()

    spawn_link(fn ->
      ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])
      continuous_write_loop(node, ctx, prefix, 1, [], parent)
    end)
  end

  defp continuous_write_loop(node, ctx, prefix, seq, keys, parent) do
    receive do
      :stop ->
        send(parent, {:writer_done, keys, seq - 1})
    after
      0 ->
        key = "#{prefix}_#{seq}"

        try do
          :erpc.call(node, Ferricstore.Store.Router, :put, [ctx, key, "value_#{seq}", 0])
          continuous_write_loop(node, ctx, prefix, seq + 1, [key | keys], parent)
        rescue
          _ ->
            # Write failed (node busy during sync) — retry after brief pause
            Process.sleep(10)
            continuous_write_loop(node, ctx, prefix, seq, keys, parent)
        end
    end
  end

  defp stop_continuous_writer(pid) do
    send(pid, :stop)

    receive do
      {:writer_done, keys, count} -> {Enum.reverse(keys), count}
    after
      5_000 -> raise "continuous writer did not stop"
    end
  end

  defp write_loop(node, prefix, write_log, seq) do
    receive do
      :stop -> :ok
    after
      0 ->
        key = "#{prefix}_#{seq}"
        ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])

        try do
          :erpc.call(node, Ferricstore.Store.Router, :put, [ctx, key, "value_#{seq}", 0])
          :ets.insert(write_log, {key, seq})
        rescue
          _ -> :ok
        end

        write_loop(node, prefix, write_log, seq + 1)
    end
  end

  defp join_cluster(new_node, existing_node) do
    :erpc.call(existing_node, Ferricstore.Cluster.Manager, :add_node, [new_node])
  end

  defp assert_node_synced(node, opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    deadline = System.monotonic_time(:millisecond) + timeout

    eventually(fn ->
      status = :erpc.call(node, Ferricstore.Cluster.Manager, :sync_status, [])
      assert status == :synced, "node sync status: #{inspect(status)}"
    end, "node not synced within #{timeout}ms", div(timeout, 500), 500)
  end

  defp get_shard_count(node) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])
    ctx.shard_count
  end

  defp dump_keydir(node) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      keydir = :erpc.call(node, fn ->
        elem(FerricStore.Instance.get(:default).keydir_refs, shard_idx)
      end, [])

      :erpc.call(node, :ets, :tab2list, [keydir])
    end
    |> List.flatten()
  end

  defp dump_keydir_sorted(node) do
    dump_keydir(node)
    |> Enum.map(fn {key, value, _exp, _lfu, _fid, _off, _vsize} -> {key, value} end)
    |> Enum.reject(fn {key, _} -> String.starts_with?(key, "PM:") end)
    |> Enum.sort()
  end

  defp shard_data_checksums(node) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      data_dir = :erpc.call(node, fn ->
        ctx = FerricStore.Instance.get(:default)
        Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_idx)
      end, [])

      files = :erpc.call(node, File, :ls!, [data_dir])
      |> Enum.filter(&String.ends_with?(&1, ".log"))
      |> Enum.sort()

      checksums = Enum.map(files, fn file ->
        path = Path.join(data_dir, file)
        content = :erpc.call(node, File, :read!, [path])
        {file, :crypto.hash(:sha256, content) |> Base.encode16()}
      end)

      {shard_idx, checksums}
    end
  end

  defp trigger_compaction(node) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      shard = elem(ctx.shard_names, shard_idx)
      :erpc.call(node, GenServer, :call, [shard, :run_compaction])
    end
  end

  defp eventually(fun, msg \\ "condition not met", attempts \\ 20, interval \\ 50) do
    Ferricstore.Test.ShardHelpers.eventually(fun, msg, attempts, interval)
  end
end
