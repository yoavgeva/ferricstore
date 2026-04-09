defmodule Ferricstore.Jepsen.LostWritesTest do
  @moduledoc """
  Jepsen-style lost write tests from test plan Section 19.2.

  Tests the fundamental durability guarantee: when FerricStore acknowledges a
  write, that write must survive node crashes. In single-node Raft mode (current
  architecture), each node is its own independent Raft cluster with self-quorum.
  The durability guarantee is: if `FerricStore.set/2` returns `:ok`, the data was
  committed to the local Raft log and applied to Bitcask. It must survive:

    1. Sibling node crashes (trivial -- independent nodes)
    2. The writing node staying alive while others crash
    3. Multiple rounds of node kills

  When multi-node Raft is implemented, these tests will validate cross-node
  quorum durability: ACK means committed on 2/3 nodes before response.

  ## Running

      mix test test/ferricstore/jepsen/ --include jepsen
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Test.HistoryRecorder

  @moduletag :jepsen
  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  describe "19.2 quorum writes survive leader kill" do
    @tag :jepsen
    test "100 ACKed writes survive sibling node kills", %{nodes: nodes} do
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      n_writes = 100

      # Write 100 keys distributed across nodes. After every 10 writes,
      # kill a sibling node to simulate leader churn.
      for i <- 1..n_writes do
        # Round-robin across alive nodes
        node = Enum.at(nodes, rem(i, 3))

        # Skip nodes that have been killed
        case :rpc.call(node.name, Node, :self, []) do
          {:badrpc, _} ->
            :skip

          _ ->
            key = "jepsen:write:#{i}"
            value = "v#{i}"

            result =
              :rpc.call(node.name, FerricStore, :set, [key, value])

            if result == :ok do
              HistoryRecorder.record_ok(history, key, value, node.name)
            end
        end

        # Kill a sibling node every 30 writes to introduce disruption.
        # The writing node stays alive so its data must be preserved.
        if rem(i, 30) == 0 do
          alive =
            Enum.filter(nodes, fn n ->
              case :rpc.call(n.name, Node, :self, []) do
                {:badrpc, _} -> false
                _ -> true
              end
            end)

          if length(alive) > 1 do
            target = List.last(alive)
            ClusterHelper.kill_node(nodes, target)
            Process.sleep(200)
          end
        end
      end

      # Determine which nodes are still alive for verification
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "at least one node should survive"
      :ok = ClusterHelper.wait_for_leaders(alive, 4, timeout: 5_000)

      # Verify: each surviving node has all the writes it ACKed.
      # In single-node mode, a node only has its own writes -- so we check
      # each node for the writes that were directed to it.
      all_entries = HistoryRecorder.all(history)

      per_node_entries =
        Enum.group_by(all_entries, fn {:ok, _k, _v, node, _ts} -> node end)

      Enum.each(alive, fn node ->
        node_entries = Map.get(per_node_entries, node.name, [])

        Enum.each(node_entries, fn {:ok, key, value, _node, _ts} ->
          {:ok, read} = :rpc.call(node.name, FerricStore, :get, [key])

          assert read == value,
                 "Lost write: key=#{key} expected=#{value} got=#{inspect(read)} " <>
                   "on node=#{node.name}"
        end)
      end)

      IO.puts(
        "  #{length(all_entries)} acknowledged writes verified durable after node kills"
      )
    end

    @tag :jepsen
    test "writes to surviving node unaffected by sibling crash", %{nodes: nodes} do
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # Find alive nodes (previous tests in this module may have killed some)
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      # Need at least 1 node; ideally 2+ so we can kill one while writing
      assert alive != [], "need at least 1 alive node"

      writer = hd(alive)
      targets = tl(alive)

      # Write 50 keys to the writer node
      for i <- 1..50 do
        key = "jepsen:survive:#{i}"
        value = "survive_v#{i}"

        :ok = :rpc.call(writer.name, FerricStore, :set, [key, value])
        HistoryRecorder.record_ok(history, key, value, writer.name)
      end

      # Kill all other nodes (may be empty if only 1 was alive)
      Enum.each(targets, fn target ->
        ClusterHelper.kill_node(nodes, target)
      end)

      Process.sleep(500)
      :ok = ClusterHelper.wait_for_leaders([writer], 4, timeout: 5_000)

      # All 50 writes must be readable on the surviving writer
      result = HistoryRecorder.verify_durability(history, [writer])

      case result do
        {:ok, checked} ->
          IO.puts("  #{checked} writes verified durable on surviving writer node")

        {:error, violations} ->
          flunk(
            "Durability violated:\n" <>
              HistoryRecorder.format_violations(violations)
          )
      end
    end
  end
end
