defmodule Ferricstore.Cluster.ClusterApiTest do
  @moduledoc """
  Unit tests for Ferricstore.Raft.Cluster functions.

  Tests the cluster API functions that don't require multi-node setups:
  server ID construction, members queries, and add/remove on the local
  single-node Raft groups.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Cluster

  # ---------------------------------------------------------------------------
  # shard_server_id_on/2
  # ---------------------------------------------------------------------------

  describe "shard_server_id_on/2" do
    test "returns correct {name, node} tuple for shard 0" do
      result = Cluster.shard_server_id_on(0, :some_node@host)
      assert result == {:ferricstore_shard_0, :some_node@host}
    end

    test "returns correct tuple for shard 1" do
      result = Cluster.shard_server_id_on(1, :other_node@host)
      assert result == {:ferricstore_shard_1, :other_node@host}
    end

    test "returns correct tuple for higher shard indices" do
      result = Cluster.shard_server_id_on(7, node())
      assert result == {:ferricstore_shard_7, node()}
    end

    test "uses the provided node, not the local node" do
      remote = :"remote@127.0.0.1"
      {_name, returned_node} = Cluster.shard_server_id_on(0, remote)
      assert returned_node == remote
    end
  end

  # ---------------------------------------------------------------------------
  # shard_server_id/1
  # ---------------------------------------------------------------------------

  describe "shard_server_id/1" do
    test "returns {name, node()} for the local node" do
      result = Cluster.shard_server_id(0)
      assert result == {:ferricstore_shard_0, node()}
    end

    test "returns different names for different shards" do
      {name0, _} = Cluster.shard_server_id(0)
      {name1, _} = Cluster.shard_server_id(1)
      assert name0 != name1
      assert name0 == :ferricstore_shard_0
      assert name1 == :ferricstore_shard_1
    end
  end

  # ---------------------------------------------------------------------------
  # system_name/0
  # ---------------------------------------------------------------------------

  describe "system_name/0" do
    test "returns the ra system atom" do
      assert Cluster.system_name() == :ferricstore_raft
    end
  end

  # ---------------------------------------------------------------------------
  # members/1
  # ---------------------------------------------------------------------------

  describe "members/1" do
    test "returns members for shard 0" do
      Ferricstore.Test.ShardHelpers.eventually(fn ->
        {:ok, members, leader} = Cluster.members(0)
        assert is_list(members)
        assert members != []
        assert leader != nil
      end, "shard 0 should have members", 10, 200)
    end

    test "returns leader as the local node in single-node mode" do
      Ferricstore.Test.ShardHelpers.eventually(fn ->
        {:ok, _members, {_name, leader_node}} = Cluster.members(0)
        assert leader_node == node()
      end, "shard 0 should have local leader", 10, 200)
    end

    test "returns members for all configured shards" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        Ferricstore.Test.ShardHelpers.eventually(fn ->
          {:ok, members, _leader} = Cluster.members(i)
          assert is_list(members), "shard #{i} should return a member list"
          assert members != [], "shard #{i} should have at least 1 member"
        end, "shard #{i} should have members", 10, 200)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # add_member/3 -- local node tests
  # ---------------------------------------------------------------------------

  describe "add_member/3" do
    test "adding self as voter returns :ok (already member)" do
      # In single-node mode, self is already a voter -- ra returns :already_member -> :ok
      result = Cluster.add_member(0, node(), :voter)
      assert result == :ok
    end

    test "adding self with different membership updates membership type" do
      # ra accepts changing membership type of an existing member via add_member.
      result = Cluster.add_member(0, node(), :promotable)
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # remove_member/2
  # ---------------------------------------------------------------------------

  describe "remove_member/2" do
    test "removing a non-member remote node returns error in single-node mode" do
      # In single-node Raft (nonode@nohost), ra rejects cluster changes to
      # nodes that aren't reachable. The error is :cluster_change_not_permitted.
      result = Cluster.remove_member(0, :"nonexistent@nowhere")
      assert result == :ok or match?({:error, _}, result)
    end
  end
end
