defmodule Ferricstore.Jepsen.StreamDurabilityTest do
  @moduledoc """
  Jepsen-style stream message durability tests from test plan Section 19.9.

  Verifies that XADD with quorum ACK produces durable stream entries that
  survive faults. Every ACKed stream entry must be recoverable via XRANGE
  after any failure scenario. Stream IDs must be monotonically increasing.

  ## Architecture note

  FerricStore Streams store entries as compound keys in Bitcask with ETS
  metadata tracking. In single-node Raft mode, each node maintains its own
  independent stream data. Stream commands go through the Dispatcher to
  `Ferricstore.Commands.Stream.handle/3` which uses a store map for I/O.

  For these Jepsen tests, we invoke stream operations via a helper module
  (`Ferricstore.Jepsen.StreamHelper`) that is loaded onto remote peer nodes
  at setup time, allowing stream commands to execute entirely within the
  remote node's address space.

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

    # Load the stream helper module onto all peer nodes so it can build
    # store maps in the remote node's address space.
    {module, binary, _file} = :code.get_object_code(Ferricstore.Jepsen.StreamHelper)

    Enum.each(nodes, fn node ->
      {:module, ^module} = :rpc.call(node.name, :code, :load_binary, [module, ~c"nofile", binary])

      # Ensure stream ETS tables are owned by a persistent process on the
      # remote node, so they survive across individual RPC calls.
      :ok = :rpc.call(node.name, Ferricstore.Jepsen.StreamHelper, :ensure_tables, [])
    end)

    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  # ---------------------------------------------------------------------------
  # 19.9.1 All ACKed XADD entries present after operations
  #
  # XADD 50 entries to a stream on each node. All ACKed entries must be
  # recoverable via XRANGE. In single-node mode, this verifies that the
  # stream storage (Bitcask compound keys + ETS metadata) is consistent.
  # ---------------------------------------------------------------------------

  describe "stream message durability" do
    @tag :jepsen
    test "all ACKed XADD entries present via XRANGE on same node", %{nodes: nodes} do
      Enum.each(alive(nodes), fn node ->
        stream_key = "jepsen:stream:#{node.index}"

        # XADD 50 entries
        acked_ids =
          for i <- 1..50 do
            fields = ["seq", "#{i}", "data", "entry_#{i}"]

            result = remote_stream_cmd(node.name, "XADD", [stream_key, "*" | fields])

            case result do
              id when is_binary(id) ->
                id

              {:error, reason} ->
                flunk("XADD failed on #{node.name} for entry #{i}: #{reason}")
            end
          end

        assert length(acked_ids) == 50,
               "Expected 50 ACKed XADD entries on #{node.name}, got #{length(acked_ids)}"

        # XRANGE to retrieve all entries
        range_result = remote_stream_cmd(node.name, "XRANGE", [stream_key, "-", "+"])

        assert is_list(range_result),
               "XRANGE should return a list on #{node.name}, got: #{inspect(range_result)}"

        # Each XRANGE entry is [id, field1, value1, field2, value2, ...]
        present_ids =
          Enum.map(range_result, fn [id | _fields] -> id end)
          |> MapSet.new()

        missing =
          Enum.reject(acked_ids, &MapSet.member?(present_ids, &1))

        assert missing == [],
               "#{length(missing)} ACKed stream entries missing on #{node.name}: " <>
                 "#{inspect(Enum.take(missing, 5))}"

        IO.puts(
          "  Node #{node.index}: #{length(acked_ids)} stream entries ACKed; 0 lost"
        )
      end)
    end

    @tag :jepsen
    test "stream IDs are monotonically increasing", %{nodes: nodes} do
      Enum.each(alive(nodes), fn node ->
        stream_key = "jepsen:stream:monotonic:#{node.index}"

        # XADD entries with auto-generated IDs
        ids =
          for i <- 1..50 do
            result = remote_stream_cmd(node.name, "XADD", [stream_key, "*", "i", "#{i}"])

            assert is_binary(result),
                   "XADD should return an ID string, got: #{inspect(result)}"

            result
          end

        # Parse IDs and verify strict monotonic ordering
        parsed_ids =
          Enum.map(ids, fn id_str ->
            [ms_str, seq_str] = String.split(id_str, "-", parts: 2)
            {String.to_integer(ms_str), String.to_integer(seq_str)}
          end)

        # Each ID must be strictly greater than the previous one
        violations =
          parsed_ids
          |> Enum.chunk_every(2, 1, :discard)
          |> Enum.with_index()
          |> Enum.flat_map(fn {[{ms1, seq1}, {ms2, seq2}], idx} ->
            if ms2 > ms1 or (ms2 == ms1 and seq2 > seq1) do
              []
            else
              [{:not_monotonic, index: idx, prev: {ms1, seq1}, current: {ms2, seq2}}]
            end
          end)

        assert violations == [],
               "Stream IDs not monotonically increasing on #{node.name}:\n" <>
                 "#{inspect(violations)}"
      end)
    end

    @tag :jepsen
    test "XRANGE returns entries in order after interleaved XADD", %{nodes: nodes} do
      n1 = hd(alive(nodes))
      stream_key = "jepsen:stream:ordered"

      # Write entries with known sequential data
      acked_ids =
        for i <- 1..50 do
          result = remote_stream_cmd(n1.name, "XADD", [stream_key, "*", "seq", "#{i}"])

          assert is_binary(result), "XADD #{i} should return ID, got: #{inspect(result)}"
          {result, i}
        end

      # XRANGE all entries
      range_result = remote_stream_cmd(n1.name, "XRANGE", [stream_key, "-", "+"])

      assert length(range_result) == 50,
             "XRANGE should return 50 entries, got #{length(range_result)}"

      # Verify ordering: entries should be in the same order as written
      range_ids = Enum.map(range_result, fn [id | _] -> id end)
      acked_id_list = Enum.map(acked_ids, fn {id, _seq} -> id end)

      assert range_ids == acked_id_list,
             "XRANGE entries not in write order"

      # Verify each entry's seq field matches the write order
      Enum.each(Enum.with_index(range_result, 1), fn {[_id, "seq", seq_val | _], expected_seq} ->
        assert seq_val == "#{expected_seq}",
               "Entry seq mismatch: expected #{expected_seq}, got #{seq_val}"
      end)
    end

    @tag :jepsen
    test "stream entries survive after node kill and read from surviving node", %{nodes: nodes} do
      alive_nodes = alive(nodes)
      assert length(alive_nodes) >= 2, "Need at least 2 alive nodes for kill test"

      # In single-node mode, each node has independent stream data.
      # Write stream data to each alive node, kill one, verify survivors
      # still have their own data intact.
      per_node_entries =
        Map.new(alive_nodes, fn node ->
          stream_key = "jepsen:stream:survive:#{node.index}"

          ids =
            for i <- 1..50 do
              result =
                remote_stream_cmd(node.name, "XADD", [stream_key, "*", "data", "v#{i}"])

              assert is_binary(result),
                     "XADD should return ID on #{node.name}"

              result
            end

          {node, {stream_key, ids}}
        end)

      # Kill the last alive node
      target = List.last(alive_nodes)
      {_killed, remaining} = ClusterHelper.kill_node(alive_nodes, target)

      # Verify surviving nodes still have their stream data
      Enum.each(remaining, fn node ->
        {stream_key, expected_ids} = Map.get(per_node_entries, node)

        range_result = remote_stream_cmd(node.name, "XRANGE", [stream_key, "-", "+"])

        assert is_list(range_result),
               "XRANGE should return list on surviving #{node.name}"

        present_ids = Enum.map(range_result, fn [id | _] -> id end) |> MapSet.new()

        missing =
          Enum.reject(expected_ids, &MapSet.member?(present_ids, &1))

        assert missing == [],
               "#{length(missing)} stream entries missing on surviving #{node.name}: " <>
                 "#{inspect(Enum.take(missing, 5))}"
      end)

      IO.puts(
        "  Stream data intact on #{length(remaining)} surviving nodes after node kill"
      )
    end

    @tag :jepsen
    test "XLEN matches number of ACKed entries", %{nodes: nodes} do
      Enum.each(alive(nodes), fn node ->
        stream_key = "jepsen:stream:xlen:#{node.index}"

        # XADD 50 entries
        acked_count =
          Enum.count(1..50, fn i ->
            result =
              remote_stream_cmd(node.name, "XADD", [stream_key, "*", "n", "#{i}"])

            is_binary(result)
          end)

        # XLEN should match
        xlen_result = remote_stream_cmd(node.name, "XLEN", [stream_key])

        assert xlen_result == acked_count,
               "XLEN mismatch on #{node.name}: expected #{acked_count}, got #{xlen_result}"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Executes a stream command on the remote node via the StreamHelper module
  # which was loaded onto peer nodes in setup_all.
  defp remote_stream_cmd(node_name, cmd, args) do
    :rpc.call(node_name, Ferricstore.Jepsen.StreamHelper, :exec, [cmd, args])
  end

  # Filters nodes to only those that are still alive (haven't been killed
  # by a previous test in the same module).
  defp alive(nodes) do
    Enum.filter(nodes, fn node ->
      case :rpc.call(node.name, Node, :self, []) do
        {:badrpc, _} -> false
        _ -> true
      end
    end)
  end
end
