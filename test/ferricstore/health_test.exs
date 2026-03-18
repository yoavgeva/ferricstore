defmodule Ferricstore.HealthTest do
  @moduledoc """
  Health-check tests that verify the server responds correctly to
  diagnostic commands over TCP.

  Covers:
    - Application responds to PING over TCP
    - DBSIZE returns a non-negative integer
    - INFO returns a non-empty string
    - Stats module tracks connections
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Resp.{Encoder, Parser}
  alias Ferricstore.Server.Listener
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # TCP helpers (same pattern as CommandsTcpTest)
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # PING over TCP
  # ---------------------------------------------------------------------------

  describe "PING over TCP" do
    test "PING returns PONG" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "PING with message echoes the message" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING", "hello"])
      assert recv_response(sock) == "hello"

      :gen_tcp.close(sock)
    end

    test "PING works on multiple sequential connections" do
      port = Listener.port()

      for _ <- 1..5 do
        sock = connect_and_hello(port)
        send_cmd(sock, ["PING"])
        assert recv_response(sock) == {:simple, "PONG"}
        :gen_tcp.close(sock)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # DBSIZE returns a non-negative integer
  # ---------------------------------------------------------------------------

  describe "DBSIZE returns a non-negative integer" do
    test "DBSIZE returns zero on empty store" do
      port = Listener.port()
      sock = connect_and_hello(port)

      # Flush the store first.
      send_cmd(sock, ["FLUSHDB"])
      recv_response(sock)

      send_cmd(sock, ["DBSIZE"])
      result = recv_response(sock)
      assert is_integer(result)
      assert result == 0
      :gen_tcp.close(sock)
    end

    test "DBSIZE increases after writes" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["FLUSHDB"])
      recv_response(sock)

      send_cmd(sock, ["DBSIZE"])
      before = recv_response(sock)
      assert is_integer(before)

      # Write 3 keys.
      for i <- 1..3 do
        k = "health_dbsize_#{:rand.uniform(9_999_999)}_#{i}"
        send_cmd(sock, ["SET", k, "v"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      send_cmd(sock, ["DBSIZE"])
      after_count = recv_response(sock)
      assert is_integer(after_count)
      assert after_count >= before + 3

      :gen_tcp.close(sock)
    end

    test "DBSIZE is non-negative even with no data" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["DBSIZE"])
      result = recv_response(sock)
      assert is_integer(result)
      assert result >= 0

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # INFO returns a non-empty string
  # ---------------------------------------------------------------------------

  describe "INFO returns a non-empty string" do
    test "INFO with no arguments returns a non-empty string" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO"])
      result = recv_response(sock)
      assert is_binary(result), "INFO should return a bulk string"
      assert byte_size(result) > 0, "INFO output should be non-empty"

      :gen_tcp.close(sock)
    end

    test "INFO contains server section with run_id" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO"])
      result = recv_response(sock)
      assert is_binary(result)
      assert String.contains?(result, "run_id"), "INFO should contain run_id"

      :gen_tcp.close(sock)
    end

    test "INFO contains uptime information" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO"])
      result = recv_response(sock)
      assert is_binary(result)

      assert String.contains?(result, "uptime_in_seconds"),
             "INFO should contain uptime_in_seconds"

      :gen_tcp.close(sock)
    end

    test "INFO server section returns server info" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO", "server"])
      result = recv_response(sock)
      assert is_binary(result), "INFO server should return a string"
      assert byte_size(result) > 0, "INFO server output should be non-empty"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Stats module tracks connections
  # ---------------------------------------------------------------------------

  describe "Stats module tracks connections" do
    test "total_connections is a non-negative integer" do
      total = Ferricstore.Stats.total_connections()
      assert is_integer(total)
      assert total >= 0
    end

    test "total_commands is a non-negative integer" do
      total = Ferricstore.Stats.total_commands()
      assert is_integer(total)
      assert total >= 0
    end

    test "opening a new TCP connection increments total_connections" do
      before = Ferricstore.Stats.total_connections()

      port = Listener.port()
      sock = connect_and_hello(port)

      # Give the connection handler time to increment the counter.
      Process.sleep(50)

      after_count = Ferricstore.Stats.total_connections()
      assert after_count > before,
             "Expected total_connections to increase from #{before}, got #{after_count}"

      :gen_tcp.close(sock)
    end

    test "sending commands increments total_commands" do
      port = Listener.port()
      sock = connect_and_hello(port)

      before = Ferricstore.Stats.total_commands()

      send_cmd(sock, ["PING"])
      recv_response(sock)
      send_cmd(sock, ["PING"])
      recv_response(sock)
      send_cmd(sock, ["PING"])
      recv_response(sock)

      after_count = Ferricstore.Stats.total_commands()
      assert after_count >= before + 3,
             "Expected total_commands to increase by at least 3 from #{before}, got #{after_count}"

      :gen_tcp.close(sock)
    end

    test "run_id is a 40-character hex string" do
      run_id = Ferricstore.Stats.run_id()
      assert is_binary(run_id)
      assert byte_size(run_id) == 40
      assert Regex.match?(~r/^[0-9a-f]{40}$/, run_id), "Run ID should be lowercase hex"
    end

    test "uptime_seconds is non-negative" do
      uptime = Ferricstore.Stats.uptime_seconds()
      assert is_integer(uptime)
      assert uptime >= 0
    end
  end
end
