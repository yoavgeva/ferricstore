defmodule Ferricstore.Cluster.TopologyTest do
  @moduledoc """
  Cluster topology tests covering test plan Sections 5.1-5.2.

  Non-destructive tests that verify basic cluster startup and operation.
  Destructive tests (node kill, partition) are in separate test modules
  to avoid `setup_all` conflicts.

  ## Running

      mix test test/ferricstore/cluster/ --include cluster
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  # ---------------------------------------------------------------------------
  # Section 5.2: Three-Node Cluster -- Normal Operation
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "all nodes started and all shards have elected leaders", %{nodes: nodes} do
    shard_count = 4

    Enum.each(nodes, fn node ->
      for shard <- 0..(shard_count - 1) do
        server_id = {:"ferricstore_shard_#{shard}", node.name}
        result = :rpc.call(node.name, :ra, :members, [server_id])

        assert match?({:ok, _, _}, result),
               "shard #{shard} on #{node.name} should have a leader, got #{inspect(result)}"
      end
    end)
  end

  @tag :cluster
  test "each node can serve read/write operations independently", %{nodes: nodes} do
    Enum.each(nodes, fn node ->
      key = "topo:rw:#{node.index}"
      value = "value_#{node.index}"

      result = :rpc.call(node.name, Ferricstore.Store.Router, :put, [key, value, 0])
      assert result == :ok, "PUT on #{node.name} should succeed"

      read = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])
      assert read == value, "GET on #{node.name} should return #{value}"
    end)
  end

  @tag :cluster
  test "nodes are connected via Erlang distribution", %{nodes: nodes} do
    [n1, n2, n3] = nodes

    n1_nodes = :rpc.call(n1.name, Node, :list, [])
    assert n2.name in n1_nodes, "n1 should see n2"
    assert n3.name in n1_nodes, "n1 should see n3"
  end

  @tag :cluster
  test "multiple writes and reads work correctly on each node", %{nodes: nodes} do
    Enum.each(nodes, fn node ->
      for i <- 1..20 do
        key = "topo:multi:#{node.index}:#{i}"
        val = "v#{i}"
        :ok = :rpc.call(node.name, Ferricstore.Store.Router, :put, [key, val, 0])
      end

      for i <- 1..20 do
        key = "topo:multi:#{node.index}:#{i}"
        val = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])
        assert val == "v#{i}", "key #{key} on #{node.name} should have value v#{i}"
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # CT-007: Follower read returns correct value
  # (Section 5.2 C3-003)
  #
  # In single-node Raft mode, each node is its own independent leader. This
  # test verifies that a value written on one node can be read back from a
  # different node (each operating independently). When multi-node Raft is
  # added, this test will verify true follower reads via Raft replication.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-007: write on one node, read from another node (independent mode)", %{nodes: nodes} do
    [n1, n2, n3] = nodes

    # Write distinct values on each node
    :ok = :rpc.call(n1.name, Ferricstore.Store.Router, :put, ["ct007:n1", "from_n1", 0])
    :ok = :rpc.call(n2.name, Ferricstore.Store.Router, :put, ["ct007:n2", "from_n2", 0])

    # Each node reads back its own value (independent single-node Raft)
    assert :rpc.call(n1.name, Ferricstore.Store.Router, :get, ["ct007:n1"]) == "from_n1"
    assert :rpc.call(n2.name, Ferricstore.Store.Router, :get, ["ct007:n2"]) == "from_n2"

    # Cross-node reads: in single-node mode, n2 does NOT see n1's data
    # (each node is an independent Raft cluster). When multi-node Raft is
    # added, this will change to assert equality instead.
    n2_reads_n1 = :rpc.call(n2.name, Ferricstore.Store.Router, :get, ["ct007:n1"])
    assert n2_reads_n1 == nil,
           "in single-node mode, n2 should not see n1's key (no replication yet)"

    # Verify n3 also operates independently
    :ok = :rpc.call(n3.name, Ferricstore.Store.Router, :put, ["ct007:n3", "from_n3", 0])
    assert :rpc.call(n3.name, Ferricstore.Store.Router, :get, ["ct007:n3"]) == "from_n3"
    assert :rpc.call(n1.name, Ferricstore.Store.Router, :get, ["ct007:n3"]) == nil
  end
end

# ---------------------------------------------------------------------------
# Destructive test: node failure (separate module, own cluster)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.NodeFailureTest do
  @moduledoc """
  Cluster tests for node failure scenarios from test plan Section 5.3.

  Uses its own cluster via `setup_all` so that destructive operations
  (killing nodes) do not affect other test modules.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  @tag :cluster
  test "remaining nodes continue operating after one node is killed", %{nodes: nodes} do
    # Write to all nodes first
    Enum.each(nodes, fn node ->
      :rpc.call(node.name, Ferricstore.Store.Router, :put, [
        "topo:kill:before:#{node.index}",
        "safe",
        0
      ])
    end)

    # Kill last node
    target = List.last(nodes)
    {_killed, remaining} = ClusterHelper.kill_node(nodes, target)

    # Allow time for the BEAM distribution to settle after the kill.
    # Without this, RPC calls to remaining nodes can transiently fail
    # while the distribution layer detects the dead peer and cleans up
    # pending monitor/link notifications.
    Process.sleep(1_000)

    # Wait for leaders to be re-confirmed on remaining nodes
    :ok = ClusterHelper.wait_for_leaders(remaining, 4, timeout: 5_000)

    # Remaining nodes should still work
    Enum.each(remaining, fn node ->
      result =
        :rpc.call(node.name, Ferricstore.Store.Router, :put, [
          "topo:kill:after:#{node.index}",
          "still_working",
          0
        ])

      assert result == :ok,
             "node #{node.name} should still serve writes after sibling killed"

      val =
        :rpc.call(node.name, Ferricstore.Store.Router, :get, [
          "topo:kill:after:#{node.index}"
        ])

      assert val == "still_working"
    end)
  end

  @tag :cluster
  test "killed node does not affect other nodes' shard leaders", %{nodes: nodes} do
    # Only test remaining nodes (first node may have been killed by previous test)
    alive_nodes =
      Enum.filter(nodes, fn node ->
        case :rpc.call(node.name, Node, :self, []) do
          {:badrpc, _} -> false
          _ -> true
        end
      end)

    :ok = ClusterHelper.wait_for_leaders(alive_nodes, 4, timeout: 3_000)

    Enum.each(alive_nodes, fn node ->
      leader = ClusterHelper.find_leader([node], 0)
      assert leader == node.name, "node #{node.name} should be its own leader"
    end)
  end
end

# ---------------------------------------------------------------------------
# CT-005 & CT-006: Failover tests (own cluster to avoid state pollution)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.FailoverTest do
  @moduledoc """
  Leader failover tests from test plan Section 5.3.

  Covers:
  - CT-005: Write after failover succeeds on surviving nodes
  - CT-006: Data survives leader failover

  Uses its own cluster so that destructive node kills do not pollute
  other test modules.

  ## Running

      mix test test/ferricstore/cluster/ --include cluster
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    # Allow distribution to fully settle from any previous module's
    # cluster teardown before starting our own fresh cluster. The
    # NodeFailureTest module kills peer nodes, and the BEAM distribution
    # layer needs time to fully clean up stale connections before new
    # peer nodes with potentially similar names can be started reliably.
    Process.sleep(500)

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  # Calls an RPC function on a remote node with retries. Returns the result
  # on success, or the last {:badrpc, reason} on persistent failure. Retries
  # on {:badrpc, _} to handle transient distribution hiccups after cluster
  # stop/start cycles.
  defp rpc_with_retry(node_name, mod, fun, args, retries \\ 10) do
    case :rpc.call(node_name, mod, fun, args) do
      {:badrpc, _reason} when retries > 0 ->
        Process.sleep(300)
        rpc_with_retry(node_name, mod, fun, args, retries - 1)

      result ->
        result
    end
  end

  # ---------------------------------------------------------------------------
  # CT-005: Write after failover succeeds on surviving nodes
  # (Section 5.3 CF-002)
  #
  # Kill the leader for shard 0, then verify that writes succeed on the
  # surviving nodes. In single-node mode each node is its own leader, so
  # "failover" means the remaining independent nodes continue operating.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-005: write after failover succeeds on surviving nodes", %{nodes: nodes} do
    [n1, n2, n3] = nodes

    # Write a value on n1 before kill (with retry to handle transient
    # distribution issues from previous module's cluster teardown)
    result_pre =
      rpc_with_retry(n1.name, Ferricstore.Store.Router, :put, [
        "ct005:pre_kill",
        "before",
        0
      ])

    assert result_pre == :ok, "CT-005: pre-kill write on n1 should succeed"

    # Kill n3
    {_killed, _remaining} = ClusterHelper.kill_node(nodes, n3)

    # Allow distribution to settle
    Process.sleep(500)

    # n1 and n2 should still serve writes
    result1 =
      rpc_with_retry(n1.name, Ferricstore.Store.Router, :put, [
        "ct005:post_kill:n1",
        "after_failover_n1",
        0
      ])

    assert result1 == :ok,
           "CT-005: write on n1 should succeed after n3 killed"

    result2 =
      rpc_with_retry(n2.name, Ferricstore.Store.Router, :put, [
        "ct005:post_kill:n2",
        "after_failover_n2",
        0
      ])

    assert result2 == :ok,
           "CT-005: write on n2 should succeed after n3 killed"

    # Verify reads work too
    assert rpc_with_retry(n1.name, Ferricstore.Store.Router, :get, ["ct005:post_kill:n1"]) ==
             "after_failover_n1"

    assert rpc_with_retry(n2.name, Ferricstore.Store.Router, :get, ["ct005:post_kill:n2"]) ==
             "after_failover_n2"
  end

  # ---------------------------------------------------------------------------
  # CT-006: Data survives leader failover
  # (Section 5.3 CF-003)
  #
  # Write data, kill a node, verify the surviving nodes still have their
  # data intact. In single-node mode, each node's data is local only.
  # When multi-node Raft is added, the data should be replicated and
  # visible on any surviving node.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-006: data written before kill survives on surviving nodes", %{nodes: nodes} do
    # Filter alive nodes (n3 may have been killed by the CT-005 test)
    alive_nodes =
      Enum.filter(nodes, fn node ->
        case :rpc.call(node.name, Node, :self, []) do
          {:badrpc, _} -> false
          _ -> true
        end
      end)

    assert length(alive_nodes) >= 2, "need at least 2 alive nodes for CT-006"

    # Write data to all alive nodes
    Enum.each(alive_nodes, fn node ->
      :rpc.call(node.name, Ferricstore.Store.Router, :put, [
        "ct006:durable:#{node.index}",
        "must_survive_#{node.index}",
        0
      ])
    end)

    # Kill one alive node
    [target | survivors] = alive_nodes
    {_killed, _remaining} = ClusterHelper.kill_node(nodes, target)

    # Allow distribution to settle
    Process.sleep(500)

    # Verify all surviving nodes still have their own data
    Enum.each(survivors, fn node ->
      val =
        :rpc.call(node.name, Ferricstore.Store.Router, :get, [
          "ct006:durable:#{node.index}"
        ])

      assert val == "must_survive_#{node.index}",
             "CT-006: data on #{node.name} should survive sibling kill"
    end)
  end
end

# ---------------------------------------------------------------------------
# Destructive test: network partition (separate module, own cluster)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.PartitionTest do
  @moduledoc """
  Cluster tests for network partition scenarios from test plan Section 5.5.

  Covers CT-009 (rejoin after partition -- node catches up).

  Uses its own cluster via `setup_all` so that partition simulation does
  not affect other test modules.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  @tag :cluster
  test "partitioned node loses connectivity to peers", %{nodes: nodes} do
    [n1, _n2, n3] = nodes

    # Partition n3 from the others
    ClusterHelper.partition_node(n3, nodes)

    # Give distribution time to register the disconnect
    Process.sleep(500)

    # n1 should not see n3 in its connected nodes
    n1_nodes = :rpc.call(n1.name, Node, :list, [])
    refute n3.name in n1_nodes, "n3 should be disconnected from n1"

    # n3 should not see n1
    n3_nodes = :rpc.call(n3.name, Node, :list, [])
    refute n1.name in n3_nodes, "n1 should be disconnected from n3"

    # Each node can still operate independently (single-node Raft)
    result =
      :rpc.call(n3.name, Ferricstore.Store.Router, :put, [
        "topo:part:isolated",
        "works",
        0
      ])

    assert result == :ok,
           "partitioned node should still operate in single-node mode"

    result2 =
      :rpc.call(n1.name, Ferricstore.Store.Router, :put, [
        "topo:part:majority",
        "also_works",
        0
      ])

    assert result2 == :ok

    # Heal partition
    ClusterHelper.heal_partition(n3, nodes)
    Process.sleep(500)

    # Nodes should see each other again
    n1_nodes_after = :rpc.call(n1.name, Node, :list, [])
    assert n3.name in n1_nodes_after, "n3 should be reconnected to n1 after heal"
  end

  @tag :cluster
  test "heal partition: nodes reconnect and operations continue", %{nodes: nodes} do
    [n1, _n2, n3] = nodes

    # Write before partition
    :rpc.call(n1.name, Ferricstore.Store.Router, :put, ["topo:heal:pre", "before", 0])

    # Partition n3
    ClusterHelper.partition_node(n3, nodes)
    Process.sleep(300)

    # Write on n1 during partition
    :rpc.call(n1.name, Ferricstore.Store.Router, :put, ["topo:heal:during", "during", 0])

    # Heal partition
    ClusterHelper.heal_partition(n3, nodes)
    Process.sleep(300)

    # n1 should still have its data
    n1_val = :rpc.call(n1.name, Ferricstore.Store.Router, :get, ["topo:heal:pre"])
    assert n1_val == "before"

    n1_during = :rpc.call(n1.name, Ferricstore.Store.Router, :get, ["topo:heal:during"])
    assert n1_during == "during"

    # n3 can still operate after reconnect
    n3_result =
      :rpc.call(n3.name, Ferricstore.Store.Router, :put, ["topo:heal:post", "after", 0])

    assert n3_result == :ok
  end

  # ---------------------------------------------------------------------------
  # CT-009: Rejoin after partition -- node catches up
  # (Section 5.5 NP-004)
  #
  # Partition a node, write data to the majority side during partition,
  # heal the partition, and verify both sides resume normal operation.
  #
  # In single-node Raft mode, there is no Raft log replication, so the
  # isolated node will NOT receive writes from the majority side. This
  # test verifies the partition/heal infrastructure and that each side
  # preserves its own writes. When multi-node Raft is added, the
  # isolated node should catch up via Raft log sync after heal.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-009: rejoin after partition -- nodes resume and local data preserved", %{
    nodes: nodes
  } do
    [n1, n2, n3] = nodes

    # Partition n3 from the cluster
    ClusterHelper.partition_node(n3, nodes)
    Process.sleep(500)

    # Write 50 keys on n1 during partition
    for i <- 1..50 do
      :ok =
        :rpc.call(n1.name, Ferricstore.Store.Router, :put, [
          "ct009:majority:#{i}",
          "val_#{i}",
          0
        ])
    end

    # Write 10 keys on isolated n3 during partition
    for i <- 1..10 do
      :ok =
        :rpc.call(n3.name, Ferricstore.Store.Router, :put, [
          "ct009:isolated:#{i}",
          "iso_#{i}",
          0
        ])
    end

    # Heal partition
    ClusterHelper.heal_partition(n3, nodes)
    Process.sleep(500)

    # Verify n1 still has its data
    for i <- 1..50 do
      val = :rpc.call(n1.name, Ferricstore.Store.Router, :get, ["ct009:majority:#{i}"])
      assert val == "val_#{i}", "n1 should retain majority-side writes after heal"
    end

    # Verify n3 still has its isolated writes
    for i <- 1..10 do
      val = :rpc.call(n3.name, Ferricstore.Store.Router, :get, ["ct009:isolated:#{i}"])
      assert val == "iso_#{i}", "n3 should retain isolated writes after heal"
    end

    # Both sides can write new data after heal
    :ok = :rpc.call(n1.name, Ferricstore.Store.Router, :put, ["ct009:post_heal:n1", "ok", 0])
    :ok = :rpc.call(n2.name, Ferricstore.Store.Router, :put, ["ct009:post_heal:n2", "ok", 0])
    :ok = :rpc.call(n3.name, Ferricstore.Store.Router, :put, ["ct009:post_heal:n3", "ok", 0])

    assert :rpc.call(n1.name, Ferricstore.Store.Router, :get, ["ct009:post_heal:n1"]) == "ok"
    assert :rpc.call(n2.name, Ferricstore.Store.Router, :get, ["ct009:post_heal:n2"]) == "ok"
    assert :rpc.call(n3.name, Ferricstore.Store.Router, :get, ["ct009:post_heal:n3"]) == "ok"

    # Verify Erlang distribution fully restored
    n1_peers = :rpc.call(n1.name, Node, :list, [])
    assert n3.name in n1_peers, "n3 should be visible to n1 after heal"
    n3_peers = :rpc.call(n3.name, Node, :list, [])
    assert n1.name in n3_peers, "n1 should be visible to n3 after heal"
  end
end
