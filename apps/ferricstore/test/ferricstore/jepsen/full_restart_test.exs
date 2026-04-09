defmodule Ferricstore.Jepsen.FullRestartTest do
  @moduledoc """
  Jepsen-style full cluster restart durability test from test plan Section 19.8.

  Verifies that all quorum-committed writes survive a complete cluster restart.
  Every node is stopped and restarted from disk. The Raft WAL and Bitcask
  hint files must contain enough information to recover all committed state.

  ## Test flow

    1. Write 100 keys with quorum durability (default) on each node
    2. Stop ALL nodes (`:peer.stop/1`)
    3. Restart ALL nodes from the same data directories
    4. All 100 keys must be readable on each restarted node

  ## Architecture note

  In single-node Raft mode, each node is independent. A full restart means
  each node individually restarts and recovers its own Raft WAL + Bitcask
  data. When multi-node Raft is implemented, this test will verify that
  the cluster reforms and all quorum-committed writes are present on all
  nodes after restart.

  ## Running

      mix test test/ferricstore/jepsen/ --include jepsen
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :jepsen
  @moduletag :cluster

  # Full restart tests are inherently slow due to stop/start cycles.
  @moduletag timeout: 120_000

  # ---------------------------------------------------------------------------
  # 19.8 All quorum writes survive full cluster restart
  #
  # This is the most fundamental durability test: if data was ACKed with
  # quorum durability and ALL nodes are restarted, every ACKed write must
  # still be present. This tests the combined durability of:
  #   - Raft WAL persistence (ra log segments)
  #   - Bitcask data file persistence
  #   - Bitcask hint file recovery
  #   - ETS cache reconstruction from Bitcask on startup
  # ---------------------------------------------------------------------------

  describe "full cluster restart durability" do
    @tag :jepsen
    test "all quorum writes survive full cluster restart" do
      # Start a fresh cluster for this test (we manage the full lifecycle)
      nodes = ClusterHelper.start_cluster(3)

      # Phase 1: Write 100 keys to each node
      acked_writes =
        Enum.flat_map(nodes, fn node ->
          for i <- 1..100 do
            key = "restart:#{node.index}:#{i}"
            value = "v#{i}"

            result =
              :rpc.call(node.name, FerricStore, :set, [key, value])

            if result == :ok do
              {node.name, node.data_dir, key, value}
            else
              nil
            end
          end
          |> Enum.filter(&(&1 != nil))
        end)

      assert length(acked_writes) == 300,
             "Expected 300 ACKed writes (100 per node), got #{length(acked_writes)}"

      # Verify all writes are readable before restart
      pre_restart_missing =
        Enum.filter(acked_writes, fn {node_name, _data_dir, key, value} ->
          {:ok, actual} = :rpc.call(node_name, FerricStore, :get, [key])
          actual != value
        end)

      assert pre_restart_missing == [],
             "#{length(pre_restart_missing)} writes missing BEFORE restart"

      # Phase 2: Stop ALL nodes
      # Save node info for restart -- we need the data_dir and name.
      node_info =
        Enum.map(nodes, fn node ->
          %{name: node.name, peer: node.peer, data_dir: node.data_dir, index: node.index}
        end)

      Enum.each(nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end
      end)

      # Brief pause to ensure OS-level cleanup completes
      Process.sleep(500)

      # Phase 3: Restart ALL nodes from the same data directories
      restarted_nodes =
        Enum.map(node_info, fn info ->
          # Start a new peer with the same name (different unique suffix)
          unique = :erlang.unique_integer([:positive])
          peer_name = :"ferric_restart_#{unique}"

          code_paths =
            Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)

          {:ok, new_peer, new_node_name} =
            :peer.start(%{
              name: peer_name,
              args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
            })

          # Configure the remote node with the SAME data directory
          configure_remote_node(new_node_name, info.data_dir, 4)

          # Start the FerricStore application -- it should recover from WAL
          case :rpc.call(new_node_name, Application, :ensure_all_started, [:ferricstore]) do
            {:ok, _apps} ->
              :ok

            {:error, reason} ->
              raise "Failed to restart FerricStore on #{new_node_name}: #{inspect(reason)}"

            {:badrpc, reason} ->
              raise "RPC to #{new_node_name} failed: #{inspect(reason)}"
          end

          %{
            name: new_node_name,
            peer: new_peer,
            data_dir: info.data_dir,
            index: info.index,
            original_name: info.name
          }
        end)

      # Wait for shard leaders to be elected on restarted nodes
      :ok = ClusterHelper.wait_for_leaders(restarted_nodes, 4, timeout: 15_000)

      # Phase 4: Verify all ACKed writes survived the restart
      # Each write was on its original node; we verify on the restarted node
      # that took over the same data directory.
      post_restart_violations =
        Enum.flat_map(acked_writes, fn {original_name, data_dir, key, value} ->
          # Find the restarted node with the same data_dir
          restarted =
            Enum.find(restarted_nodes, fn n -> n.data_dir == data_dir end)

          if restarted == nil do
            [{:no_restarted_node, key, original: original_name}]
          else
            {:ok, actual} = :rpc.call(restarted.name, FerricStore, :get, [key])

            if actual == value do
              []
            else
              [
                {:lost_write, key,
                 expected: value,
                 got: actual,
                 original_node: original_name,
                 restarted_node: restarted.name}
              ]
            end
          end
        end)

      # Cleanup: stop restarted nodes
      Enum.each(restarted_nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end
      end)

      # Clean up data directories
      Enum.each(node_info, fn info ->
        File.rm_rf(info.data_dir)
      end)

      assert post_restart_violations == [],
             "#{length(post_restart_violations)} writes lost after full cluster restart:\n" <>
               format_violations(post_restart_violations)

      IO.puts(
        "  #{length(acked_writes)} writes verified durable after full cluster restart"
      )
    end

    @tag :jepsen
    test "incremental values survive full restart and maintain consistency" do
      nodes = ClusterHelper.start_cluster(3)

      # Write incrementing counter values to verify ordering survives restart
      Enum.each(nodes, fn node ->
        for i <- 1..50 do
          key = "restart:incr:#{node.index}"

          :rpc.call(node.name, FerricStore, :set, [
            key,
            Integer.to_string(i)
          ])
        end
      end)

      # Record final values before restart
      pre_values =
        Map.new(nodes, fn node ->
          key = "restart:incr:#{node.index}"
          {:ok, val} = :rpc.call(node.name, FerricStore, :get, [key])
          {node.data_dir, {key, val}}
        end)

      # Save node info for restart
      node_info =
        Enum.map(nodes, fn node ->
          %{name: node.name, peer: node.peer, data_dir: node.data_dir, index: node.index}
        end)

      # Stop all nodes
      Enum.each(nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end
      end)

      Process.sleep(500)

      # Restart all nodes
      restarted_nodes =
        Enum.map(node_info, fn info ->
          unique = :erlang.unique_integer([:positive])
          peer_name = :"ferric_restart_incr_#{unique}"
          code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)

          {:ok, new_peer, new_node_name} =
            :peer.start(%{
              name: peer_name,
              args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
            })

          configure_remote_node(new_node_name, info.data_dir, 4)

          {:ok, _} =
            :rpc.call(new_node_name, Application, :ensure_all_started, [:ferricstore])

          %{
            name: new_node_name,
            peer: new_peer,
            data_dir: info.data_dir,
            index: info.index
          }
        end)

      :ok = ClusterHelper.wait_for_leaders(restarted_nodes, 4, timeout: 15_000)

      # Verify final values match pre-restart values
      violations =
        Enum.flat_map(restarted_nodes, fn node ->
          case Map.get(pre_values, node.data_dir) do
            nil ->
              []

            {key, expected_val} ->
              {:ok, actual} = :rpc.call(node.name, FerricStore, :get, [key])

              if actual == expected_val do
                []
              else
                [{:value_changed, key, expected: expected_val, got: actual, node: node.name}]
              end
          end
        end)

      # Cleanup
      Enum.each(restarted_nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end
      end)

      Enum.each(node_info, fn info -> File.rm_rf(info.data_dir) end)

      assert violations == [],
             "Values changed after restart:\n#{format_violations(violations)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp configure_remote_node(node_name, data_dir, shards) do
    env_settings = [
      {:data_dir, data_dir},
      {:port, 0},
      {:health_port, 0},
      {:shard_count, shards},
      {:memory_guard_interval_ms, 60_000},
      {:max_memory_bytes, 1_073_741_824},
      {:merge, [check_interval_ms: 600_000, fragmentation_threshold: 0.99]}
    ]

    Enum.each(env_settings, fn {key, value} ->
      :ok = :rpc.call(node_name, Application, :put_env, [:ferricstore, key, value])
    end)
  end

  defp format_violations(violations) do
    Enum.map_join(violations, "\n", fn v -> "  #{inspect(v)}" end)
  end
end
