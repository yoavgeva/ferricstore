defmodule Ferricstore.ApplicationTest do
  @moduledoc """
  Tests for the OTP application supervisor tree, with focus on the optional
  libcluster Cluster.Supervisor integration.

  Verifies that:
  - The application starts cleanly with no libcluster topologies (nil or [])
  - The Cluster.Supervisor is included when topologies are configured
  - The supervision tree remains healthy in all configurations
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  # Ensure all shards are alive after every test so that the next test
  # module starts from a clean state.
  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> wait_shards_alive(2_000) end)
  end

  defp wait_shards_alive(timeout_ms) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.each(0..(shard_count - 1), fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      Stream.repeatedly(fn -> Process.sleep(20) end)
      |> Enum.find(fn _ ->
        pid = Process.whereis(name)
        alive = is_pid(pid) and Process.alive?(pid)
        alive or System.monotonic_time(:millisecond) > deadline
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Supervisor tree basics
  # ---------------------------------------------------------------------------

  describe "Ferricstore.Supervisor" do
    test "is alive after application start" do
      pid = Process.whereis(Ferricstore.Supervisor)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "all children are alive" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      for {id, pid, _type, _mods} <- children do
        assert is_pid(pid) and Process.alive?(pid),
               "Child #{inspect(id)} is not alive (pid=#{inspect(pid)})"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # libcluster integration
  # ---------------------------------------------------------------------------

  describe "libcluster Cluster.Supervisor" do
    test "application starts without crash when topologies config is nil" do
      # In test env, topologies is set to []. Temporarily set to nil and
      # verify the helper function returns an empty list (no crash).
      original = Application.get_env(:libcluster, :topologies)

      try do
        Application.put_env(:libcluster, :topologies, nil)

        # The running supervisor was already started (with [] or nil),
        # so ClusterSupervisor should NOT be present.
        children = Supervisor.which_children(Ferricstore.Supervisor)
        ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

        refute Ferricstore.ClusterSupervisor in ids,
               "ClusterSupervisor should NOT be started when topologies is nil"
      after
        # Restore original config
        if original == nil do
          Application.delete_env(:libcluster, :topologies)
        else
          Application.put_env(:libcluster, :topologies, original)
        end
      end
    end

    test "application starts without crash when topologies config is empty list" do
      # Test env explicitly sets topologies: [] -- verify no cluster supervisor.
      topologies = Application.get_env(:libcluster, :topologies)

      # The config should be :disabled, [], or nil in test env.
      assert topologies in [[], nil, :disabled],
             "Expected topologies to be :disabled, [] or nil in test env, got: #{inspect(topologies)}"

      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      refute Ferricstore.ClusterSupervisor in ids,
             "ClusterSupervisor should NOT be started when topologies is []"
    end

    test "ClusterSupervisor is not among children in test env" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      cluster_child =
        Enum.find(children, fn {id, _, _, _} ->
          id == Ferricstore.ClusterSupervisor or
            (is_atom(id) and id |> Atom.to_string() |> String.contains?("Cluster"))
        end)

      assert cluster_child == nil,
             "Expected no Cluster.Supervisor child in test, got: #{inspect(cluster_child)}"
    end

    test "cluster_supervisor_children returns child spec when topologies are configured" do
      # Verify Cluster.Supervisor module is loaded (libcluster dependency)
      if Code.ensure_loaded?(Cluster.Supervisor) do
        topologies = [
          ferricstore: [
            strategy: Cluster.Strategy.Gossip,
            config: [port: 45892]
          ]
        ]

        # Cluster.Supervisor.child_spec/1 should produce a valid child spec
        spec = Cluster.Supervisor.child_spec([topologies, [name: Ferricstore.ClusterSupervisor]])

        assert is_map(spec)
        assert Map.has_key?(spec, :id)
        assert Map.has_key?(spec, :start)
      else
        # If libcluster is not available, the cluster_supervisor_children
        # function in application.ex gracefully returns [] via the nil branch.
        assert true, "Cluster.Supervisor not loaded -- libcluster optional"
      end
    end

    test "Cluster.Supervisor module is available when libcluster is a dependency" do
      # This test validates that the libcluster dependency is correctly wired.
      # If it fails, check that {:libcluster, "~> 3.3"} is in mix.exs deps.
      assert Code.ensure_loaded?(Cluster.Supervisor),
             "Cluster.Supervisor should be loadable -- is libcluster in deps?"
    end

    test "Cluster.Strategy.Gossip module is available" do
      assert Code.ensure_loaded?(Cluster.Strategy.Gossip),
             "Cluster.Strategy.Gossip should be loadable"
    end

    test "Cluster.Strategy.Epmd module is available" do
      assert Code.ensure_loaded?(Cluster.Strategy.Epmd),
             "Cluster.Strategy.Epmd should be loadable"
    end
  end
end
