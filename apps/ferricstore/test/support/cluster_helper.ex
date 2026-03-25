defmodule Ferricstore.Test.ClusterHelper do
  @moduledoc """
  Helpers for spinning up multi-node FerricStore clusters in ExUnit.

  Uses `:peer` (OTP 25+) for in-process BEAM nodes. Each node gets its own
  temporary directory for Bitcask data and runs the full FerricStore
  application including Raft state machines, ETS tables, and the Bitcask NIF.

  ## Current Architecture

  FerricStore currently runs each node as an independent single-node Raft
  cluster (self-quorum). Multi-node Raft consensus across nodes is planned
  but not yet implemented. These helpers are designed to support both modes:

  - **Single-node mode (current)**: Each peer is a fully independent
    FerricStore instance. Tests verify that operations work correctly on
    individual nodes and that the `:peer` infrastructure is sound.

  - **Multi-node mode (future)**: When `Ferricstore.Raft.Cluster` gains
    `bootstrap/0` and `join/1` APIs, `start_cluster/2` will form an actual
    Raft cluster. Tests for replication, failover, and partitions will
    then validate cross-node consistency.

  ## Usage

      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

  ## OTP Requirement

  Requires OTP 25+ for the `:peer` module. On older OTP versions,
  `peer_available?/0` returns `false` and `start_cluster/2` raises.
  """

  require Logger

  @doc """
  Returns `true` if the `:peer` module is available (OTP 25+).

  Tests should call this at the top of `setup_all` and skip if it returns
  `false`, so that the test suite still compiles on older OTP versions.
  """
  @spec peer_available?() :: boolean()
  def peer_available? do
    Code.ensure_loaded?(:peer) and function_exported?(:peer, :start, 1)
  end

  @doc """
  Starts N FerricStore peer nodes.

  Each node gets:
  - A unique BEAM node name (`ferric_<unique>_<i>@<host>`)
  - Its own temporary directory for Bitcask data
  - The same code path as the test runner
  - An individually started FerricStore application

  Currently each node runs as an independent single-node Raft instance.
  When multi-node Raft is implemented, this function will also form the
  cluster.

  ## Parameters

    - `n` -- number of nodes to start (typically 3 or 5)
    - `opts` -- keyword options:
      - `:shards` -- number of shards per node (default: 4)
      - `:timeout` -- leader election timeout in ms (default: 10_000)

  ## Returns

  A list of node maps: `%{name: atom, peer: pid, data_dir: binary, index: integer}`
  """
  @spec start_cluster(pos_integer(), keyword()) :: [map()]
  def start_cluster(n, opts \\ []) do
    unless peer_available?() do
      raise "ClusterHelper requires OTP 25+ for :peer module"
    end

    shards = Keyword.get(opts, :shards, 4)
    timeout = Keyword.get(opts, :timeout, 10_000)
    unique = :erlang.unique_integer([:positive])

    # Ensure the host node is alive for Erlang distribution.
    ensure_distribution!()

    # Start each peer node
    nodes =
      Enum.map(1..n, fn i ->
        node_suffix = "#{unique}_#{i}"
        name = :"ferric_#{node_suffix}"
        data_dir = Path.join(System.tmp_dir!(), "ferricstore_cluster_#{node_suffix}")
        File.mkdir_p!(data_dir)

        # Start the peer with the same code paths as the test runner.
        code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)

        {:ok, peer_pid, node_name} =
          :peer.start(%{
            name: name,
            args: code_paths ++ [~c"-connect_all", ~c"false"]
          })

        # Set application env on the remote node before starting the app
        configure_remote_node(node_name, data_dir, shards)

        # Start the FerricStore application on the remote node
        case :rpc.call(node_name, Application, :ensure_all_started, [:ferricstore]) do
          {:ok, _apps} ->
            :ok

          {:error, reason} ->
            raise "Failed to start FerricStore on #{node_name}: #{inspect(reason)}"

          {:badrpc, reason} ->
            raise "RPC to #{node_name} failed: #{inspect(reason)}"
        end

        %{name: node_name, peer: peer_pid, data_dir: data_dir, index: i}
      end)

    # Connect all nodes to each other for Erlang distribution
    node_names = Enum.map(nodes, & &1.name)

    for n1 <- node_names, n2 <- node_names, n1 != n2 do
      :rpc.call(n1, Node, :connect, [n2])
    end

    # Wait for all shards to have elected leaders on each node
    :ok = wait_for_leaders(nodes, shards, timeout: timeout)

    nodes
  end

  @doc """
  Stops all peer nodes and cleans up their data directories.

  Safe to call even if some nodes have already been stopped.
  """
  @spec stop_cluster([map()]) :: :ok
  def stop_cluster(nodes) do
    Enum.each(nodes, fn node ->
      try do
        :peer.stop(node.peer)
      catch
        _, _ -> :ok
      end

      File.rm_rf(node.data_dir)
    end)

    :ok
  end

  @doc """
  Kills a specific node by stopping its peer process.

  Returns the killed node and the remaining nodes list.

  ## Parameters

    - `nodes` -- list of node maps from `start_cluster/2`
    - `target` -- the node map to kill (or index into nodes list)
  """
  @spec kill_node([map()], map()) :: {map(), [map()]}
  def kill_node(nodes, target) when is_map(target) do
    try do
      :peer.stop(target.peer)
    catch
      _, _ -> :ok
    end

    remaining = Enum.reject(nodes, &(&1.name == target.name))
    {target, remaining}
  end

  @doc """
  Kills the leader node for a given shard.

  In single-node Raft mode, the leader is always the local node. This
  function finds which node is the leader for the specified shard and
  stops it.

  ## Parameters

    - `nodes` -- list of node maps from `start_cluster/2`
    - `shard` -- shard index (default: 0)

  ## Returns

  `{killed_node, remaining_nodes}`
  """
  @spec kill_leader([map()], non_neg_integer()) :: {map(), [map()]}
  def kill_leader(nodes, shard \\ 0) do
    leader_name = find_leader(nodes, shard)
    leader_node = Enum.find(nodes, &(&1.name == leader_name))
    kill_node(nodes, leader_node)
  end

  @doc """
  Finds the current Raft leader for a shard.

  Tries each node in order until one returns a successful `ra:members/1`
  result. In single-node mode, each node is its own leader.

  ## Returns

  The node name atom of the current leader.
  """
  @spec find_leader([map()], non_neg_integer()) :: atom()
  def find_leader(nodes, shard \\ 0) do
    result =
      Enum.find_value(nodes, fn node ->
        server_id = {:"ferricstore_shard_#{shard}", node.name}

        case :rpc.call(node.name, :ra, :members, [server_id]) do
          {:ok, _members, {_leader_name, leader_node}} ->
            leader_node

          _ ->
            nil
        end
      end)

    result || raise "Could not find leader for shard #{shard}"
  end

  @doc """
  Simulates a network partition by disconnecting a node from all others.

  Disconnects in both directions to create a symmetric partition. The node
  process stays alive but cannot communicate with the rest of the cluster.
  """
  @spec partition_node(map(), [map()]) :: :ok
  def partition_node(node, all_nodes) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))

    Enum.each(others, fn other ->
      :rpc.call(node.name, :erlang, :disconnect_node, [other.name])
      :rpc.call(other.name, :erlang, :disconnect_node, [node.name])
    end)

    :ok
  end

  @doc """
  Heals a network partition by reconnecting a node to the cluster.

  Reconnects in both directions.
  """
  @spec heal_partition(map(), [map()]) :: :ok
  def heal_partition(node, all_nodes) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))

    Enum.each(others, fn other ->
      :rpc.call(node.name, Node, :connect, [other.name])
      :rpc.call(other.name, Node, :connect, [node.name])
    end)

    :ok
  end

  @doc """
  Runs a function on a specific FerricStore node via RPC.

  ## Returns

  The result of the remote function call, or `{:badrpc, reason}` on failure.
  """
  @spec run(atom(), module(), atom(), [term()]) :: term()
  def run(node_name, module, function, args) do
    :rpc.call(node_name, module, function, args)
  end

  @doc """
  Waits until all shards have an elected leader on at least one of the given nodes.

  Polls every 100ms up to the configured timeout. In single-node mode, each
  node is its own leader, so this verifies that all ra servers have completed
  their leader election.

  ## Parameters

    - `nodes` -- list of node maps
    - `shards` -- number of shards (integer) or range
    - `opts` -- keyword options:
      - `:timeout` -- maximum wait time in ms (default: 5_000)
  """
  @spec wait_for_leaders([map()], pos_integer() | Range.t(), keyword()) ::
          :ok | {:error, :timeout_waiting_for_leaders}
  def wait_for_leaders(nodes, shards, opts \\ [])

  def wait_for_leaders(nodes, shards, opts) when is_integer(shards) do
    wait_for_leaders(nodes, 0..(shards - 1), opts)
  end

  def wait_for_leaders(nodes, shard_range, opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_leaders(nodes, shard_range, deadline)
  end

  @doc """
  Waits for a node to have all its shards with elected leaders.

  Useful after restarting a single node.
  """
  @spec wait_for_node_leaders(atom(), pos_integer(), keyword()) ::
          :ok | {:error, :timeout_waiting_for_leaders}
  def wait_for_node_leaders(node_name, shards, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_node_leaders(node_name, 0..(shards - 1), deadline)
  end

  # ---------------------------------------------------------------------------
  # Private: leader waiting loops
  # ---------------------------------------------------------------------------

  defp do_wait_leaders(nodes, shard_range, deadline) do
    all_have_leaders =
      Enum.all?(shard_range, fn shard ->
        Enum.any?(nodes, fn node ->
          server_id = {:"ferricstore_shard_#{shard}", node.name}

          case :rpc.call(node.name, :ra, :members, [server_id]) do
            {:ok, _members, _leader} -> true
            _ -> false
          end
        end)
      end)

    cond do
      all_have_leaders ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout_waiting_for_leaders}

      true ->
        Process.sleep(100)
        do_wait_leaders(nodes, shard_range, deadline)
    end
  end

  defp do_wait_node_leaders(node_name, shard_range, deadline) do
    all_ready =
      Enum.all?(shard_range, fn shard ->
        server_id = {:"ferricstore_shard_#{shard}", node_name}

        case :rpc.call(node_name, :ra, :members, [server_id]) do
          {:ok, _members, _leader} -> true
          _ -> false
        end
      end)

    cond do
      all_ready ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout_waiting_for_leaders}

      true ->
        Process.sleep(100)
        do_wait_node_leaders(node_name, shard_range, deadline)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: configure remote node
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

  # ---------------------------------------------------------------------------
  # Private: ensure Erlang distribution is started
  # ---------------------------------------------------------------------------

  defp ensure_distribution! do
    case Node.self() do
      :nonode@nohost ->
        # Start distribution with a unique short name.
        # Short names avoid DNS resolution issues and work reliably
        # on macOS/Linux without extra host configuration.
        unique = :erlang.unique_integer([:positive])
        node_name = :"ferric_runner_#{unique}"

        case Node.start(node_name, :shortnames) do
          {:ok, _} ->
            :ok

          {:error, reason} ->
            raise "Failed to start Erlang distribution (#{inspect(reason)}). " <>
                    "Cluster tests require distributed BEAM. " <>
                    "Try running with: elixir --sname test -S mix test"
        end

      _ ->
        :ok
    end
  end
end
