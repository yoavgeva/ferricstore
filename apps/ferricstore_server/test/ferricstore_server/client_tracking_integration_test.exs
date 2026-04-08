defmodule FerricstoreServer.ClientTrackingIntegrationTest do
  @moduledoc """
  Integration tests for client-side caching via CLIENT TRACKING.

  These tests verify the full end-to-end flow:

      CLIENT TRACKING ON → GET key → SET key (from another conn) → receive invalidation push

  Each test connects two TCP sockets: one "tracker" that enables tracking and
  reads keys, and one "writer" that modifies those keys. The tracker socket
  receives RESP3 push invalidation messages when tracked keys are modified.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Listener
  alias FerricstoreServer.Resp.{Encoder, Parser}

  # ---------------------------------------------------------------------------
  # Helpers
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

  # Receives a response, returning {:ok, value} on success or :timeout if nothing
  # arrives within the given milliseconds.
  defp recv_response_timeout(sock, timeout_ms) do
    recv_response_timeout(sock, timeout_ms, "")
  end

  defp recv_response_timeout(sock, timeout_ms, buf) do
    case :gen_tcp.recv(sock, 0, timeout_ms) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], ""} -> {:ok, val}
          {:ok, [val], _rest} -> {:ok, val}
          {:ok, [], _} -> recv_response_timeout(sock, timeout_ms, buf2)
        end

      {:error, :timeout} ->
        :timeout
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(name), do: "ct_#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    ShardHelpers.flush_global_state()

    tracker = connect_and_hello(port)
    writer = connect_and_hello(port)

    on_exit(fn ->
      :gen_tcp.close(tracker)
      :gen_tcp.close(writer)
    end)

    %{tracker: tracker, writer: writer, port: port}
  end

  # ---------------------------------------------------------------------------
  # Tests: CLIENT TRACKING ON/OFF basics
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKING ON enables tracking" do
    test "returns OK and enables tracking", %{tracker: tracker} do
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}
    end

    test "TRACKINGINFO shows tracking enabled", %{tracker: tracker} do
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKINGINFO"])
      info = recv_response(tracker)
      assert is_map(info)
      assert info["redirect"] == -1
    end
  end

  describe "CLIENT TRACKING OFF stops invalidation" do
    test "no invalidation after tracking is disabled", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("off_test")

      # Enable tracking and read a key
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["SET", key, "v1"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "v1"

      # Disable tracking
      send_cmd(tracker, ["CLIENT", "TRACKING", "OFF"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Writer modifies the key
      send_cmd(writer, ["SET", key, "v2"])
      assert recv_response(writer) == {:simple, "OK"}

      # Tracker should NOT receive invalidation — verify with a PING round-trip
      send_cmd(tracker, ["PING"])
      result = recv_response(tracker)
      assert result == {:simple, "PONG"}
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: Default mode — track reads, invalidate on writes
  # ---------------------------------------------------------------------------

  describe "default mode: GET then SET from another connection sends invalidation" do
    test "tracker receives invalidation push when writer modifies tracked key", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("default_get")

      # Set up initial value
      send_cmd(writer, ["SET", key, "hello"])
      assert recv_response(writer) == {:simple, "OK"}

      # Enable tracking on tracker
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Read the key (registers tracking)
      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "hello"

      # Writer modifies the key — should trigger invalidation
      send_cmd(writer, ["SET", key, "world"])
      assert recv_response(writer) == {:simple, "OK"}

      # Tracker should receive invalidation push
      # The push is: {:push, [{:simple, "invalidate"}, [key]]}
      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end

    test "invalidation is one-shot — no second invalidation without re-read", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("one_shot")

      send_cmd(writer, ["SET", key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Read to register tracking
      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "v1"

      # First write triggers invalidation
      send_cmd(writer, ["SET", key, "v2"])
      assert recv_response(writer) == {:simple, "OK"}

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push

      # Second write should NOT trigger invalidation (one-shot was consumed)
      send_cmd(writer, ["SET", key, "v3"])
      assert recv_response(writer) == {:simple, "OK"}

      # Verify no invalidation arrives — PING round-trip as fence
      send_cmd(tracker, ["PING"])
      result = recv_response(tracker)
      assert result == {:simple, "PONG"}
    end

    test "HGET tracks the hash key", %{tracker: tracker, writer: writer} do
      key = ukey("hash_track")

      send_cmd(writer, ["HSET", key, "field1", "val1"])
      assert recv_response(writer)

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["HGET", key, "field1"])
      assert recv_response(tracker) == "val1"

      # Modify the hash key
      send_cmd(writer, ["HSET", key, "field2", "val2"])
      assert recv_response(writer)

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end

    test "DEL triggers invalidation on tracked key", %{tracker: tracker, writer: writer} do
      key = ukey("del_track")

      send_cmd(writer, ["SET", key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "v1"

      send_cmd(writer, ["DEL", key])
      assert recv_response(writer)

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end

    test "INCR triggers invalidation on tracked key", %{tracker: tracker, writer: writer} do
      key = ukey("incr_track")

      send_cmd(writer, ["SET", key, "10"])
      assert recv_response(writer) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "10"

      send_cmd(writer, ["INCR", key])
      assert recv_response(writer)

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: OPTIN mode
  # ---------------------------------------------------------------------------

  describe "OPTIN mode only tracks after CLIENT CACHING YES" do
    test "GET without CLIENT CACHING YES does not register tracking", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("optin_no_cache")

      send_cmd(writer, ["SET", key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      # Enable OPTIN tracking
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON", "OPTIN"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Read WITHOUT CLIENT CACHING YES first
      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "v1"

      # Writer modifies the key
      send_cmd(writer, ["SET", key, "v2"])
      assert recv_response(writer) == {:simple, "OK"}

      # Should NOT receive invalidation — PING as fence
      send_cmd(tracker, ["PING"])
      result = recv_response(tracker)
      assert result == {:simple, "PONG"}
    end

    test "CLIENT CACHING YES then GET registers tracking", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("optin_cache_yes")

      send_cmd(writer, ["SET", key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON", "OPTIN"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Opt in for the next command
      send_cmd(tracker, ["CLIENT", "CACHING", "YES"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["GET", key])
      assert recv_response(tracker) == "v1"

      # Writer modifies the key
      send_cmd(writer, ["SET", key, "v2"])
      assert recv_response(writer) == {:simple, "OK"}

      # Should receive invalidation
      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end

    test "OPTIN caching flag resets after one command", %{
      tracker: tracker,
      writer: writer
    } do
      key1 = ukey("optin_reset1")
      key2 = ukey("optin_reset2")

      send_cmd(writer, ["SET", key1, "v1"])
      assert recv_response(writer) == {:simple, "OK"}
      send_cmd(writer, ["SET", key2, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON", "OPTIN"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Opt in and read key1 (tracked)
      send_cmd(tracker, ["CLIENT", "CACHING", "YES"])
      assert recv_response(tracker) == {:simple, "OK"}

      send_cmd(tracker, ["GET", key1])
      assert recv_response(tracker) == "v1"

      # Read key2 WITHOUT re-opting in (caching flag should be reset)
      send_cmd(tracker, ["GET", key2])
      assert recv_response(tracker) == "v1"

      # Modify key2 — should NOT trigger invalidation
      send_cmd(writer, ["SET", key2, "v2"])
      assert recv_response(writer) == {:simple, "OK"}

      # Modify key1 — SHOULD trigger invalidation
      send_cmd(writer, ["SET", key1, "v2"])
      assert recv_response(writer) == {:simple, "OK"}

      # Should only get invalidation for key1
      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key1]]} = push

      # No more invalidations
      send_cmd(tracker, ["PING"])
      assert recv_response(tracker) == {:simple, "PONG"}
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: BCAST mode
  # ---------------------------------------------------------------------------

  describe "BCAST mode sends invalidations for all keys matching prefixes" do
    test "BCAST with no prefix matches all key writes", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("bcast_all")

      # Enable BCAST mode (no prefix = all keys)
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON", "BCAST"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Writer creates a key — should trigger invalidation (no read required)
      send_cmd(writer, ["SET", key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end

    test "BCAST with prefix only matches keys with that prefix", %{
      tracker: tracker,
      writer: writer
    } do
      prefix = "bcast_pfx_#{:rand.uniform(999_999)}:"
      matching_key = prefix <> "mykey"
      non_matching_key = "other_prefix:mykey_#{:rand.uniform(999_999)}"

      # Enable BCAST with a specific prefix
      send_cmd(tracker, ["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", prefix])
      assert recv_response(tracker) == {:simple, "OK"}

      # Write to a non-matching key — no invalidation expected
      send_cmd(writer, ["SET", non_matching_key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      # Write to matching key — invalidation expected
      send_cmd(writer, ["SET", matching_key, "v1"])
      assert recv_response(writer) == {:simple, "OK"}

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^matching_key]]} = push

      # Clean up
      send_cmd(writer, ["DEL", non_matching_key, matching_key])
      recv_response(writer)
    end

    test "BCAST does not require reads to trigger invalidation", %{
      tracker: tracker,
      writer: writer
    } do
      key = ukey("bcast_no_read")

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON", "BCAST"])
      assert recv_response(tracker) == {:simple, "OK"}

      # Directly write without any prior read
      send_cmd(writer, ["SET", key, "brand_new"])
      assert recv_response(writer) == {:simple, "OK"}

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key]]} = push
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: NOLOOP
  # ---------------------------------------------------------------------------

  describe "NOLOOP prevents self-invalidation" do
    test "writer with NOLOOP does not receive invalidation for own writes", %{
      port: port
    } do
      key = ukey("noloop")

      # Use a single connection with NOLOOP
      conn = connect_and_hello(port)

      on_exit(fn -> :gen_tcp.close(conn) end)

      send_cmd(conn, ["CLIENT", "TRACKING", "ON", "NOLOOP"])
      assert recv_response(conn) == {:simple, "OK"}

      # Read the key (register tracking)
      send_cmd(conn, ["SET", key, "v1"])
      assert recv_response(conn) == {:simple, "OK"}

      send_cmd(conn, ["GET", key])
      assert recv_response(conn) == "v1"

      # Write the key from the same connection
      send_cmd(conn, ["SET", key, "v2"])
      assert recv_response(conn) == {:simple, "OK"}

      # Should NOT receive invalidation due to NOLOOP.
      # Check that no data arrives on the socket within 100ms (no push).
      assert {:error, :timeout} = :gen_tcp.recv(conn, 0, 100),
             "Received unexpected data — invalidation may have leaked through NOLOOP"

      # Verify connection is still functional
      send_cmd(conn, ["PING"])
      assert recv_response(conn) == {:simple, "PONG"}
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: Multiple keys and MGET
  # ---------------------------------------------------------------------------

  describe "MGET tracks multiple keys" do
    test "MGET registers tracking for all keys", %{
      tracker: tracker,
      writer: writer
    } do
      key1 = ukey("mget1")
      key2 = ukey("mget2")

      send_cmd(writer, ["SET", key1, "a"])
      assert recv_response(writer) == {:simple, "OK"}
      send_cmd(writer, ["SET", key2, "b"])
      assert recv_response(writer) == {:simple, "OK"}

      send_cmd(tracker, ["CLIENT", "TRACKING", "ON"])
      assert recv_response(tracker) == {:simple, "OK"}

      # MGET registers tracking for both keys
      send_cmd(tracker, ["MGET", key1, key2])
      result = recv_response(tracker)
      assert result == ["a", "b"]

      # Modify key2
      send_cmd(writer, ["SET", key2, "c"])
      assert recv_response(writer) == {:simple, "OK"}

      {:ok, push} = recv_response_timeout(tracker, 2_000)
      assert {:push, [{:simple, "invalidate"}, [^key2]]} = push
    end
  end
end
