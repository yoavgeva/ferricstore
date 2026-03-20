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
# Destructive test: network partition (separate module, own cluster)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.PartitionTest do
  @moduledoc """
  Cluster tests for network partition scenarios from test plan Section 5.5.

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
end
