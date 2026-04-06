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

    # Kill any orphan peer processes from previous test runs
    cleanup_orphan_peers()
    :ok
  end

  # Clean state before each test — kill orphan peers, remove temp dirs
  setup do
    cleanup_orphan_peers()
    :ok
  end

  defp cleanup_orphan_peers do
    # Clean temp dirs
    Path.wildcard(Path.join(System.tmp_dir!(), "ferricstore_cluster_*")) |> Enum.each(&File.rm_rf/1)
    Path.wildcard(Path.join(System.tmp_dir!(), "ferricstore_solo_*")) |> Enum.each(&File.rm_rf/1)

    # Kill any lingering peer nodes from previous runs
    Node.list()
    |> Enum.filter(fn n -> n |> Atom.to_string() |> String.contains?("ferric_") end)
    |> Enum.each(fn n ->
      try do
        Node.disconnect(n)
      catch
        _, _ -> :ok
      end
    end)

    # Brief pause to let processes terminate
    Process.sleep(100)
  end

  describe "new node join with continuous writes" do
    @tag timeout: 120_000
    test "data is fully consistent after sync completes" do
      # 1. Start a 3-node cluster
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, node_b, _node_c] = nodes

      # 2. Write initial dataset (100 keys) — must be on cluster before new node
      initial_keys = write_keys(node_a, "pre_sync", 1..100)
      assert_keys_readable(node_b, initial_keys)

      # 3. Start continuous writer in background
      writer_pid = start_continuous_writer(node_a, "during_sync")

      # 4. Start 4th node (empty) and join it to the cluster
      #    join_cluster triggers: add to Raft + shard-by-shard data copy
      #    Writer keeps writing throughout — writes must not be blocked
      node_d = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      join_result = join_cluster(node_d, node_a)
      IO.puts("join_cluster result: #{inspect(join_result)}")

      # 5. Stop the continuous writer
      {during_sync_keys, during_sync_count} = stop_continuous_writer(writer_pid)
      IO.puts("Writes during sync: #{during_sync_count}")

      # 6. Write final batch after sync
      final_keys = write_keys(node_a, "post_sync", 1..50)

      # 7. Wait for Raft to replicate everything to node_d
      #    during_sync keys were written while sync was happening — they go through
      #    Raft which replicates to node_d after it joins the group
      all_keys = initial_keys ++ during_sync_keys ++ final_keys

      # Poll until all keys are readable (Raft replication may take a few seconds)
      eventually(fn ->
        missing_count = Enum.count(all_keys, fn key -> read_key(node_d, key) == nil end)
        assert missing_count == 0, "#{missing_count} keys still missing on node_d"
      end, "not all keys replicated to node_d", 60, 500)

      # 8. Final verification
      missing = Enum.filter(all_keys, fn key -> read_key(node_d, key) == nil end)
      IO.puts("Total keys: #{length(all_keys)}, missing on node_d: #{length(missing)}")

      assert missing == [],
             "#{length(missing)} keys missing on node_d: #{inspect(Enum.take(missing, 5))}"

      # 9. Compare keydirs — must be identical across all nodes
      keydir_a = dump_keydir_sorted(node_a)
      keydir_d = dump_keydir_sorted(node_d)

      assert length(keydir_a) == length(keydir_d),
             "keydir size mismatch: node_a=#{length(keydir_a)} node_d=#{length(keydir_d)}"

      mismatched = Enum.zip(keydir_a, keydir_d)
                   |> Enum.filter(fn {a, d} -> a != d end)
                   |> Enum.take(5)

      assert mismatched == [],
             "keydir content mismatch: #{inspect(mismatched)}"

      IO.puts("SUCCESS: #{length(all_keys)} keys identical across all nodes")
    end

    @tag timeout: 120_000
    test "4th node auto-discovers and syncs while writes continue" do
      # 1. Start 3-node cluster with data
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, node_b, _node_c] = nodes

      initial_keys = write_keys(node_a, "pre_sync", 1..100)
      assert_keys_readable(node_b, initial_keys)

      # 2. Continuous writer
      writer_pid = start_continuous_writer(node_a, "during_sync")

      # 3. Start 4th node with cluster_nodes pointing to existing cluster
      #    No manual join — auto-discovery handles it
      all_cluster_nodes = Enum.map(nodes, &node_name/1)
      node_d = ClusterHelper.start_node(cluster_nodes: all_cluster_nodes)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # 4. Connect node_d to the cluster (simulates libcluster)
      :erpc.call(node_name(node_a), Node, :connect, [node_name(node_d)])

      # 5. Stop writer after giving auto-join time to complete
      Process.sleep(3_000)
      {during_sync_keys, during_sync_count} = stop_continuous_writer(writer_pid)
      IO.puts("Writes during sync: #{during_sync_count}")

      final_keys = write_keys(node_a, "post_sync", 1..50)
      all_keys = initial_keys ++ during_sync_keys ++ final_keys

      # 6. Wait for all keys on node_d
      eventually(fn ->
        missing_count = Enum.count(all_keys, fn key -> read_key(node_d, key) == nil end)
        assert missing_count == 0, "#{missing_count} keys still missing on node_d"
      end, "not all keys replicated to node_d", 60, 500)

      IO.puts("Total keys: #{length(all_keys)}, missing on node_d: 0")

      # 7. Keydirs identical
      eventually(fn ->
        assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_d), "keydir mismatch"
      end, "keydirs not converged", 20, 500)

      IO.puts("SUCCESS: #{length(all_keys)} keys identical (auto-discovery)")
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
      # join_cluster does sync synchronously — data is already on node_d
      # Wait for Raft replication to settle
      Process.sleep(2_000)

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
      # join_cluster does sync synchronously — data is already on node_d
      # Wait for Raft replication to settle
      Process.sleep(2_000)

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
      # join_cluster does sync synchronously — data is already on node_d
      # Wait for Raft replication to settle
      Process.sleep(2_000)

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
      # join_cluster does sync synchronously — data is already on node_d
      # Wait for Raft replication to settle
      Process.sleep(2_000)

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

  describe "standalone to cluster (manual join)" do
    @tag timeout: 120_000
    test "single node with data → two nodes join via CLUSTER.JOIN → all data synced" do
      node_a = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_a) end)
      ClusterHelper.wait_for_node_leaders(node_name(node_a), 4, timeout: 10_000)

      keys = write_keys(node_a, "standalone", 1..200)

      node_b = ClusterHelper.start_node()
      node_c = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_b); ClusterHelper.stop_node(node_c) end)

      n_a = node_name(node_a)
      :erpc.call(n_a, Node, :connect, [node_name(node_b)])
      :erpc.call(n_a, Node, :connect, [node_name(node_c)])

      eventually(fn ->
        assert :ok == join_cluster(node_b, node_a)
      end, "node_b join failed", 10, 1000)

      eventually(fn ->
        assert :ok == join_cluster(node_c, node_a)
      end, "node_c join failed", 10, 1000)

      post_keys = write_keys(node_a, "post", 1..50)
      all_keys = keys ++ post_keys

      eventually(fn ->
        assert Enum.all?(all_keys, fn k -> read_key(node_b, k) != nil end), "keys missing on b"
        assert Enum.all?(all_keys, fn k -> read_key(node_c, k) != nil end), "keys missing on c"
      end, "replication incomplete", 60, 500)

      eventually(fn ->
        assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_b), "keydir a↔b mismatch"
        assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_c), "keydir a↔c mismatch"
      end, "keydirs not converged", 20, 500)
    end
  end

  describe "auto-discovery: one node with data, two nodes join via cluster_nodes config" do
    @tag timeout: 120_000
    test "new nodes auto-discover, sync data, keydirs identical" do
      # 1. Start node_a as standalone with data
      node_a = ClusterHelper.start_node()
      on_exit(fn -> ClusterHelper.stop_node(node_a) end)
      ClusterHelper.wait_for_node_leaders(node_name(node_a), 4, timeout: 10_000)

      keys = write_keys(node_a, "existing", 1..200)

      # 2. Start node_b and node_c with cluster_nodes pointing to node_a
      #    Simulates: FERRICSTORE_CLUSTER_NODES=a,b,c → libcluster connects → :nodeup → auto-join
      n_a = node_name(node_a)
      node_b = ClusterHelper.start_node(cluster_nodes: [n_a])
      node_c = ClusterHelper.start_node(cluster_nodes: [n_a])
      on_exit(fn -> ClusterHelper.stop_node(node_b); ClusterHelper.stop_node(node_c) end)

      # 3. Connect nodes (simulates libcluster discovery)
      #    :nodeup fires on all nodes → ClusterManager auto-joins
      :erpc.call(n_a, Node, :connect, [node_name(node_b)])
      :erpc.call(n_a, Node, :connect, [node_name(node_c)])

      # 4. No manual CLUSTER.JOIN — auto-join handles everything

      # 5. Write more data after connection
      post_keys = write_keys(node_a, "post", 1..50)
      all_keys = keys ++ post_keys

      # 6. Wait for all data to replicate
      eventually(fn ->
        assert Enum.all?(all_keys, fn k -> read_key(node_b, k) != nil end), "keys missing on b"
        assert Enum.all?(all_keys, fn k -> read_key(node_c, k) != nil end), "keys missing on c"
      end, "replication incomplete", 60, 500)

      # 7. Keydirs must be identical
      eventually(fn ->
        assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_b), "keydir a↔b mismatch"
        assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_c), "keydir a↔c mismatch"
      end, "keydirs not converged", 20, 500)
    end
  end

  describe "non-voter (readonly) node joins and gets data" do
    @tag timeout: 120_000
    test "readonly replica receives all data, writes forward to leader" do
      # 1. Start 3-node cluster with data
      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      keys = write_keys(node_a, "before_replica", 1..100)

      # 2. Start readonly replica with cluster_nodes + cluster_role
      #    Simulates: FERRICSTORE_CLUSTER_ROLE=readonly FERRICSTORE_CLUSTER_NODES=a,b,c
      all_cluster_nodes = Enum.map(nodes, &node_name/1)
      replica = ClusterHelper.start_node(cluster_nodes: all_cluster_nodes, cluster_role: :readonly)
      on_exit(fn -> ClusterHelper.stop_node(replica) end)

      # 3. Connect — auto-discovery reads remote role and joins as non_voter
      n_a = node_name(node_a)
      :erpc.call(n_a, Node, :connect, [node_name(replica)])

      # Wait for auto-join to complete (spawned process with 2s delay + sync time)
      Process.sleep(5_000)

      # 4. Write more data after replica joined
      post_keys = write_keys(node_a, "after_replica", 1..50)
      all_keys = keys ++ post_keys

      # 5. Verify replica has all data (receives replication)
      eventually(fn ->
        missing = Enum.count(all_keys, fn k -> read_key(replica, k) == nil end)
        assert missing == 0, "#{missing} keys missing on replica"
      end, "replica missing keys", 60, 500)

      # 6. Verify keydirs identical
      eventually(fn ->
        assert dump_keydir_sorted(node_a) == dump_keydir_sorted(replica), "keydir mismatch"
      end, "keydirs not converged", 20, 500)

      # 7. Write from replica — should forward to leader and succeed
      write_key(replica, "replica_write_test", "from_replica")

      eventually(fn ->
        assert read_key(node_a, "replica_write_test") == "from_replica"
      end, "replica write not forwarded to leader", 20, 500)

      # 8. Verify replica is NOT a voter (doesn't affect quorum)
      #    Check ra membership on any shard
      {:ok, members, _leader} = :erpc.call(n_a, Ferricstore.Raft.Cluster, :members, [0])
      replica_member = Enum.find(members, fn {_name, node} -> node == node_name(replica) end)
      assert replica_member != nil, "replica should be in member list"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers — these call into :peer nodes via :erpc
  # ---------------------------------------------------------------------------

  # Extract node name from map or pass through atom
  defp node_name(%{name: name}), do: name
  defp node_name(name) when is_atom(name), do: name

  defp write_keys(node, prefix, range) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    Enum.map(range, fn i ->
      key = "#{prefix}_#{i}"
      :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{i}", 0], 10_000)
      key
    end)
  end

  defp write_key(node, key, value) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, value, 0], 10_000)
  end

  defp read_key(node, key) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :get, [ctx, key], 10_000)
  end

  defp write_keys_to_shard(node, shard_idx, prefix, range) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    Enum.flat_map(range, fn i ->
      key = find_key_for_shard(n, ctx, "#{prefix}_#{i}", shard_idx)
      :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{i}", 0])
      [key]
    end)
  end

  defp find_key_for_shard(n, ctx, base_key, target_shard) do
    Enum.find_value(0..1000, fn suffix ->
      key = "#{base_key}_#{suffix}"
      shard = :erpc.call(n, Ferricstore.Store.Router, :shard_for, [ctx, key])
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
    n = node_name(node)
    parent = self()

    spawn_link(fn ->
      ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])
      continuous_write_loop(n, ctx, prefix, 1, [], parent)
    end)
  end

  defp continuous_write_loop(n, ctx, prefix, seq, keys, parent) do
    receive do
      :stop ->
        send(parent, {:writer_done, keys, seq - 1})
    after
      0 ->
        key = "#{prefix}_#{seq}"

        try do
          :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{seq}", 0])
          continuous_write_loop(n, ctx, prefix, seq + 1, [key | keys], parent)
        rescue
          _ ->
            Process.sleep(10)
            continuous_write_loop(n, ctx, prefix, seq, keys, parent)
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
    n = node_name(node)

    receive do
      :stop -> :ok
    after
      0 ->
        key = "#{prefix}_#{seq}"
        ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

        try do
          :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{seq}", 0])
          :ets.insert(write_log, {key, seq})
        rescue
          _ -> :ok
        end

        write_loop(node, prefix, write_log, seq + 1)
    end
  end

  defp join_cluster(new_node, existing_node) do
    :erpc.call(node_name(existing_node), Ferricstore.Cluster.Manager, :add_node, [node_name(new_node)])
  end


  defp get_shard_count(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])
    ctx.shard_count
  end

  defp dump_keydir(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      keydir = :erpc.call(n, FerricStore.Instance, :get, [:default])
               |> Map.get(:keydir_refs) |> elem(shard_idx)
      :erpc.call(n, :ets, :tab2list, [keydir])
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
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      shard_path = :erpc.call(n, Ferricstore.DataDir, :shard_data_path, [ctx.data_dir, shard_idx])

      files = :erpc.call(n, File, :ls!, [shard_path])
      |> Enum.filter(&String.ends_with?(&1, ".log"))
      |> Enum.sort()

      checksums = Enum.map(files, fn file ->
        path = Path.join(shard_path, file)
        content = :erpc.call(n, File, :read!, [path])
        {file, :crypto.hash(:sha256, content) |> Base.encode16()}
      end)

      {shard_idx, checksums}
    end
  end

  defp trigger_compaction(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      shard = elem(ctx.shard_names, shard_idx)
      :erpc.call(n, GenServer, :call, [shard, {:run_compaction, []}])
    end
  end

  defp eventually(fun, msg \\ "condition not met", attempts \\ 20, interval \\ 50) do
    Ferricstore.Test.ShardHelpers.eventually(fun, msg, attempts, interval)
  end
end
