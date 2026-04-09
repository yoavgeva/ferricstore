defmodule FerricstoreServer.Bugs.PipelineQuitLeakTest do
  @moduledoc """
  Regression test for Bug #4: Pipeline tasks not awaited on QUIT.

  The bug doc claims lane tasks from sliding_window are leaked when QUIT
  is encountered. After thorough investigation:

  1. QUIT is a stateful command — the pure group is always flushed (and
     lane tasks awaited) before QUIT is handled.
  2. The :quit branch in sliding_window_collect is unreachable because pure
     commands never return {:quit, response}.
  3. Task.await runs unconditionally after sliding_window_collect returns.

  The described bug does not exist in the current codebase. The pipeline
  correctly handles QUIT: all commands before QUIT are executed and
  their responses sent, then QUIT closes the connection, and commands
  after QUIT are discarded.

  These tests verify correct behavior and serve as regression guards
  against future changes to pipeline QUIT handling.
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 30_000

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # TCP helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_all_until_closed(sock, buf \\ "", acc \\ []) do
    case :gen_tcp.recv(sock, 0, 2_000) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [_ | _] = vals, rest} ->
            recv_all_until_closed(sock, rest, acc ++ vals)

          {:ok, [], _} ->
            recv_all_until_closed(sock, buf2, acc)
        end

      {:error, :closed} ->
        case Parser.parse(buf) do
          {:ok, vals, _} -> acc ++ vals
          _ -> acc
        end

      {:error, :timeout} ->
        acc
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    {:ok, _data} = :gen_tcp.recv(sock, 0, 5_000)
    sock
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "pipeline with QUIT" do
    test "all pure command responses before QUIT are received by client", %{port: port} do
      prefix = "pql_#{System.unique_integer([:positive])}"

      # Pre-seed 50 keys across different shards
      for i <- 1..50 do
        FerricStore.set("#{prefix}:#{i}", "val#{i}")
      end

      # Pipeline: 50 GETs (pure, multi-shard), then QUIT
      pipeline =
        IO.iodata_to_binary(
          (for i <- 1..50 do
            Encoder.encode(["GET", "#{prefix}:#{i}"])
          end) ++
          [Encoder.encode(["QUIT"])]
        )

      sock = connect_and_hello(port)
      :ok = :gen_tcp.send(sock, pipeline)

      # Read all responses until connection closes
      responses = recv_all_until_closed(sock)

      # We should receive exactly 51 responses: 50 GETs + 1 QUIT
      assert length(responses) == 51,
        "Expected 51 responses (50 GETs + QUIT), but got #{length(responses)}"

      :gen_tcp.close(sock)
    end

    test "large mixed write pipeline before QUIT persists all data", %{port: port} do
      prefix = "pqlw_#{System.unique_integer([:positive])}"

      # Pipeline: 50 SET commands (writes, pure, multi-shard), then QUIT
      pipeline =
        IO.iodata_to_binary(
          (for i <- 1..50 do
            Encoder.encode(["SET", "#{prefix}:#{i}", "written#{i}"])
          end) ++
          [Encoder.encode(["QUIT"])]
        )

      sock = connect_and_hello(port)
      :ok = :gen_tcp.send(sock, pipeline)

      responses = recv_all_until_closed(sock)
      assert length(responses) == 51

      :gen_tcp.close(sock)

      # Verify ALL writes persisted (new connection)
      sock2 = connect_and_hello(port)
      for i <- 1..50 do
        send_cmd(sock2, ["GET", "#{prefix}:#{i}"])
        {:ok, data} = :gen_tcp.recv(sock2, 0, 5_000)
        {:ok, [val], _} = Parser.parse(data)
        assert val == "written#{i}",
          "SET #{prefix}:#{i} should have persisted, got #{inspect(val)}"
      end

      :gen_tcp.close(sock2)
    end
  end
end
