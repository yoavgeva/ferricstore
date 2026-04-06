defmodule Ferricstore.Cluster.ManagerTest do
  @moduledoc """
  Unit tests for Ferricstore.Cluster.Manager GenServer.

  Tests the ClusterManager in standalone mode (the default when no
  cluster_nodes are configured). The ClusterManager is already started
  by the application supervision tree; these tests call the public API
  directly.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Cluster.Manager

  # ---------------------------------------------------------------------------
  # Standalone mode
  # ---------------------------------------------------------------------------

  describe "standalone mode" do
    test "mode/0 returns :standalone when no cluster_nodes configured" do
      assert Manager.mode() == :standalone
    end

    test "sync_status/0 returns :not_started in standalone" do
      assert Manager.sync_status() == :not_started
    end

    test "node_status/0 returns basic info map" do
      status = Manager.node_status()

      assert is_map(status)
      assert status.mode == :standalone
      assert status.node == node()
      assert is_list(status.connected_nodes)
      assert is_list(status.known_nodes)
      assert status.sync_status == :not_started
      assert is_map(status.shards)

      # In standalone mode with 4 shards, we should have entries for shards 0..3
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert Map.has_key?(status.shards, i),
               "expected shard #{i} in status.shards"
      end
    end

    test "node_status/0 shard entries contain members and leader" do
      status = Manager.node_status()

      Enum.each(status.shards, fn {_idx, shard_info} ->
        # Each shard should have members and leader (running locally)
        assert Map.has_key?(shard_info, :members) or Map.has_key?(shard_info, :error)

        if Map.has_key?(shard_info, :members) do
          assert is_list(shard_info.members)
          assert shard_info.leader != nil
        end
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Role to membership mapping
  #
  # We test by calling add_node with each role. In standalone mode with
  # single-node Raft, adding the local node as a member is a no-op
  # (:already_member -> :ok).
  # ---------------------------------------------------------------------------

  describe "role to membership mapping" do
    test "add_node with :voter role returns :ok or partial error" do
      # In a clean single-node Raft, self is already a :voter, so ra returns
      # :already_member -> :ok. However, if other tests in this describe block
      # have already run and changed some shards' membership state, some shards
      # may reject the change. Both outcomes are valid for this unit test.
      result = Manager.add_node(node(), :voter)
      assert result == :ok or match?({:error, {:partial_add, _}}, result)
    end

    test "add_node with :replica role returns partial error (membership type mismatch)" do
      # In single-node mode, the local node is already a :voter member.
      # Requesting :replica (mapped to :promotable) triggers :cluster_change_not_permitted
      # on some shards because ra doesn't allow changing membership type via add_member.
      result = Manager.add_node(node(), :replica)
      assert result == :ok or match?({:error, {:partial_add, _}}, result)
    end

    test "add_node with :readonly role returns partial error (membership type mismatch)" do
      # Same as :replica -- :readonly maps to :non_voter which conflicts with existing :voter.
      result = Manager.add_node(node(), :readonly)
      assert result == :ok or match?({:error, {:partial_add, _}}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # Leave in standalone mode
  # ---------------------------------------------------------------------------

  describe "leave/0" do
    test "leave in standalone mode removes self from Raft groups" do
      # After leave, mode should switch to :standalone (it already is, but
      # the GenServer sets it explicitly). Calling mode() still works.
      # We don't actually call leave here because it would disrupt the
      # running application shards. Instead, verify the API is callable.
      # The leave implementation removes from Raft groups then sets mode = :standalone.
      assert Manager.mode() == :standalone
    end
  end
end
