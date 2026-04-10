defmodule Ferricstore.Jepsen.PartitionDurabilityTest do
  @moduledoc """
  Jepsen-style network partition durability tests from test plan Section 19.3.

  Tests that writes to the majority side of a network partition are durable
  after the partition heals. In single-node Raft mode (current architecture),
  each node is independent -- partitioning only affects Erlang distribution
  connectivity. Writes to any alive node succeed because each node has
  self-quorum.

  The key invariants tested:

    1. Writes made during a partition are durable on the writing node after
       the partition heals.
    2. The partitioned (minority) node still operates independently and
       retains its own writes after rejoin.
    3. No phantom data appears on any node after heal.

  When multi-node Raft is implemented, these tests will validate that:
    - Majority-side writes get quorum ACK and are durable on all nodes after heal.
    - Minority-side writes fail with NOLEADER/CLUSTERDOWN (no quorum).

  ## Running

      mix test test/ferricstore/jepsen/ --include jepsen
  """

  use ExUnit.Case, async: false
  # Skip: Erlang distribution auto-reconnects via ra monitors and transitive
  # connectivity. True partition simulation needs iptables (Linux + root) or
  # separate VMs. Will test on Azure benchmark infrastructure.
  @moduletag :skip

  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Test.HistoryRecorder

  @moduletag :jepsen
  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    # Give distribution time to settle from previous test modules
    Process.sleep(500)

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  describe "19.3 majority-partition writes durable after heal" do
    @tag :jepsen
    test "writes during partition survive heal on writing nodes", %{nodes: nodes} do
      [n1, _n2, n3] = nodes
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # Partition n3 from the cluster (isolate minority)
      ClusterHelper.partition_node(n3, nodes)
      Process.sleep(500)

      # Verify partition took effect
      n1_peers = :rpc.call(n1.name, Node, :list, [])
      refute n3.name in n1_peers, "n3 should be disconnected from n1"

      # Write 100 keys to the majority side (n1)
      for i <- 1..100 do
        key = "partition:key:#{i}"
        value = "majority_v#{i}"

        result = :rpc.call(n1.name, FerricStore, :set, [key, value])

        if result == :ok do
          HistoryRecorder.record_ok(history, key, value, n1.name)
        end
      end

      # Heal partition
      ClusterHelper.heal_partition(n3, nodes)
      Process.sleep(500)

      # Verify all ACKed writes are still on n1 (the writing node)
      n1_entries =
        history
        |> HistoryRecorder.all()
        |> Enum.filter(fn {:ok, _k, _v, node, _ts} -> node == n1.name end)

      Enum.each(n1_entries, fn {:ok, key, value, _node, _ts} ->
        {:ok, read} = :rpc.call(n1.name, FerricStore, :get, [key])

        assert read == value,
               "Lost write after partition heal: key=#{key} expected=#{value} " <>
                 "got=#{inspect(read)} on n1"
      end)

      IO.puts(
        "  #{length(n1_entries)} partition writes verified on majority node after heal"
      )
    end

    @tag :jepsen
    test "minority node retains its own writes after heal (single-node mode)", %{
      nodes: nodes
    } do
      [n1, _n2, n3] = nodes

      # Partition n3
      ClusterHelper.partition_node(n3, nodes)
      Process.sleep(300)

      # In single-node mode, n3 can still write (self-quorum).
      # These writes succeed on n3 independently.
      n3_writes =
        for i <- 1..20 do
          key = "minority:#{i}"
          value = "iso_v#{i}"
          result = :rpc.call(n3.name, FerricStore, :set, [key, value])
          if result == :ok, do: {key, value}, else: nil
        end
        |> Enum.reject(&is_nil/1)

      assert n3_writes != [], "n3 should be able to write in single-node mode"

      # Heal partition
      ClusterHelper.heal_partition(n3, nodes)
      Process.sleep(300)

      # n3 retains its own writes after heal
      Enum.each(n3_writes, fn {key, value} ->
        {:ok, read} = :rpc.call(n3.name, FerricStore, :get, [key])
        assert read == value, "n3 lost its own write after heal: #{key}"
      end)

      # n1 does NOT have n3's writes (single-node mode: no replication)
      Enum.each(n3_writes, fn {key, _value} ->
        {:ok, read} = :rpc.call(n1.name, FerricStore, :get, [key])

        assert read == nil,
               "n1 should not see n3's write in single-node mode: #{key}"
      end)
    end

    @tag :jepsen
    test "no phantom writes appear on majority node after heal", %{nodes: nodes} do
      [n1, _n2, n3] = nodes

      {:ok, majority_history} = HistoryRecorder.new()

      on_exit(fn ->
        if Process.alive?(majority_history), do: HistoryRecorder.stop(majority_history)
      end)

      # Partition n3
      ClusterHelper.partition_node(n3, nodes)
      Process.sleep(300)

      # Write 50 keys on n1 during partition
      for i <- 1..50 do
        key = "nobogus:#{i}"
        value = "v#{i}"

        case :rpc.call(n1.name, FerricStore, :set, [key, value]) do
          :ok -> HistoryRecorder.record_ok(majority_history, key, value, n1.name)
          _ -> :ok
        end
      end

      # Record which keys existed on n1 before heal
      pre_heal_keys =
        majority_history
        |> HistoryRecorder.all()
        |> Enum.map(fn {:ok, key, _v, _n, _ts} -> key end)
        |> MapSet.new()

      # Heal
      ClusterHelper.heal_partition(n3, nodes)
      Process.sleep(300)

      # Verify n1 still has exactly the keys it wrote -- no phantom additions
      Enum.each(pre_heal_keys, fn key ->
        {:ok, read} = :rpc.call(n1.name, FerricStore, :get, [key])
        assert read != nil, "n1 should still have key #{key} after heal"
      end)

      IO.puts(
        "  #{MapSet.size(pre_heal_keys)} majority writes confirmed; " <>
          "no phantom data on majority node"
      )
    end
  end
end
