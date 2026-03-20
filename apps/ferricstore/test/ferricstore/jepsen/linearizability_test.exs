defmodule Ferricstore.Jepsen.LinearizabilityTest do
  @moduledoc """
  Jepsen-style register linearizability tests from test plan Section 19.6.

  Verifies that FerricStore's quorum write + local read guarantee holds:
  after a quorum write ACK, no subsequent read on any node should return a
  value older than that write. Also verifies that followers only serve
  committed values (no phantoms).

  ## Architecture note

  FerricStore currently runs each node as an independent single-node Raft
  cluster (self-quorum). In this mode, "all nodes" for linearizability means
  the writing node itself -- each node is authoritative for its own data.
  When multi-node Raft is implemented, the read-your-writes test will verify
  that a quorum write on the leader is immediately visible on all followers.

  ## Running

      mix test test/ferricstore/jepsen/ --include jepsen
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

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

  # ---------------------------------------------------------------------------
  # 19.6.1 Read-your-writes
  #
  # Quorum write ACK guarantees subsequent reads on ALL nodes return the new
  # value (after sync). In single-node mode, this means a write on a node is
  # immediately readable from that same node (self-quorum guarantee).
  #
  # 100 sequential writes, read from all nodes after each, verify monotonicity.
  # ---------------------------------------------------------------------------

  describe "read-your-writes: quorum write ACK guarantees subsequent reads" do
    @tag :jepsen
    test "100 sequential writes readable with monotonic values on all nodes", %{nodes: nodes} do
      key = "jepsen:register:ryw"

      for i <- 1..100 do
        # Write to a node -- in single-node mode, each node is independent.
        # We write to the node that will be reading, proving self-quorum
        # guarantees immediate read-your-writes.
        Enum.each(nodes, fn node ->
          value = "v#{i}"

          result =
            :rpc.call(node.name, Ferricstore.Store.Router, :put, [key, value, 0])

          assert result == :ok,
                 "PUT v#{i} should succeed on #{node.name}, got: #{inspect(result)}"

          # Immediately read from the same node -- must see >= value written.
          read_val = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])

          assert read_val != nil,
                 "Read after write should not be nil on #{node.name} for iteration #{i}"

          read_int = extract_version(read_val)

          assert read_int >= i,
                 "Stale read on #{node.name}: wrote v#{i} but read #{read_val} " <>
                   "(version #{read_int} < #{i})"
        end)
      end
    end

    @tag :jepsen
    test "write on one node, read from same node returns current value", %{nodes: nodes} do
      # This is the core read-your-writes property for single-node Raft.
      for {node, idx} <- Enum.with_index(nodes) do
        key = "jepsen:ryw:node#{idx}"

        for i <- 1..100 do
          value = "v#{i}"
          :ok = :rpc.call(node.name, Ferricstore.Store.Router, :put, [key, value, 0])

          read_val = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])

          assert read_val == value,
                 "Read-your-writes violated on #{node.name}: " <>
                   "wrote #{value} but read #{inspect(read_val)}"
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 19.6.2 No phantom values
  #
  # A value read from a follower is always a value that was committed (never
  # garbage or uncommitted data). In single-node mode, we verify that reads
  # only return values that were actually written -- never partial writes,
  # corrupted data, or values from uncommitted transactions.
  # ---------------------------------------------------------------------------

  describe "no phantom: value on node is always a committed value" do
    @tag :jepsen
    test "reads only return committed values, never phantoms", %{nodes: nodes} do
      Enum.each(nodes, fn node ->
        key = "jepsen:register:phantom:#{node.index}"

        # Write a sequence of known committed values
        committed_values =
          for i <- 1..50 do
            v = "committed:#{i}"
            :ok = :rpc.call(node.name, Ferricstore.Store.Router, :put, [key, v, 0])
            v
          end

        committed_set = MapSet.new(committed_values) |> MapSet.put(nil)

        # Read 200 times -- only committed values are allowed
        phantoms =
          for _ <- 1..200 do
            v = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])

            if MapSet.member?(committed_set, v) do
              nil
            else
              v
            end
          end
          |> Enum.filter(&(&1 != nil))

        assert phantoms == [],
               "Phantom value(s) observed on #{node.name}: #{inspect(phantoms)}"
      end)
    end

    @tag :jepsen
    test "concurrent reads during writes never see uncommitted data", %{nodes: nodes} do
      [n1 | _] = nodes
      key = "jepsen:phantom:concurrent"

      # Pre-populate with known value
      :ok = :rpc.call(n1.name, Ferricstore.Store.Router, :put, [key, "initial", 0])

      # Track all committed values
      committed_values = Agent.start_link(fn -> MapSet.new(["initial", nil]) end) |> elem(1)

      # Writer task: writes known values sequentially
      writer =
        Task.async(fn ->
          for i <- 1..100 do
            v = "write:#{i}"
            :ok = :rpc.call(n1.name, Ferricstore.Store.Router, :put, [key, v, 0])
            Agent.update(committed_values, &MapSet.put(&1, v))
          end
        end)

      # Reader tasks: read concurrently, collect any phantom values
      readers =
        for _ <- 1..5 do
          Task.async(fn ->
            for _ <- 1..200 do
              v = :rpc.call(n1.name, Ferricstore.Store.Router, :get, [key])
              committed = Agent.get(committed_values, & &1)
              # Allow nil since the key may not yet exist or may be between writes.
              # Also allow any committed value.
              if v == nil or MapSet.member?(committed, v) do
                nil
              else
                v
              end
            end
            |> Enum.filter(&(&1 != nil))
          end)
        end

      Task.await(writer, 30_000)
      reader_results = Task.await_many(readers, 30_000)
      phantoms = List.flatten(reader_results)
      Agent.stop(committed_values)

      assert phantoms == [],
             "Phantom values observed during concurrent read/write: #{inspect(Enum.take(phantoms, 10))}"
    end
  end

  # ---------------------------------------------------------------------------
  # 19.6.3 Monotonicity across sequential writes
  #
  # 100 sequential writes, read from all nodes after each, verify the value
  # never regresses to an earlier version.
  # ---------------------------------------------------------------------------

  describe "monotonicity: values never regress to older versions" do
    @tag :jepsen
    test "100 sequential writes maintain monotonic ordering on each node", %{nodes: nodes} do
      Enum.each(nodes, fn node ->
        key = "jepsen:monotonic:#{node.index}"
        max_seen = 0

        final_max =
          Enum.reduce(1..100, max_seen, fn i, acc ->
            value = "v#{i}"
            :ok = :rpc.call(node.name, Ferricstore.Store.Router, :put, [key, value, 0])

            read_val = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])
            read_version = extract_version(read_val)

            assert read_version >= acc,
                   "Monotonicity violated on #{node.name}: " <>
                     "previously saw v#{acc} but now reading #{read_val} (v#{read_version})"

            max(acc, read_version)
          end)

        assert final_max == 100,
               "Final value on #{node.name} should be version 100, got #{final_max}"
      end)
    end

    @tag :jepsen
    test "overwrite sequence maintains last-write-wins on each node", %{nodes: nodes} do
      Enum.each(nodes, fn node ->
        key = "jepsen:lww:#{node.index}"

        for i <- 1..100 do
          :ok =
            :rpc.call(node.name, Ferricstore.Store.Router, :put, [
              key,
              "version:#{i}",
              0
            ])
        end

        # Final read must be the last written value
        final = :rpc.call(node.name, Ferricstore.Store.Router, :get, [key])
        assert final == "version:100", "Last-write-wins violated on #{node.name}: got #{inspect(final)}"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp extract_version(nil), do: 0

  defp extract_version(value) when is_binary(value) do
    case Regex.run(~r/(\d+)$/, value) do
      [_, num_str] -> String.to_integer(num_str)
      _ -> 0
    end
  end
end
