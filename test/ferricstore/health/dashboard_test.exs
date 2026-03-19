defmodule Ferricstore.Health.DashboardTest do
  @moduledoc """
  Tests for the built-in HTML dashboard (spec 7.3).

  Covers:
    - Dashboard data collection from all subsystems
    - HTML rendering with all expected sections
    - HTTP endpoint serving at /dashboard
    - Auto-refresh meta tag
    - Content-Type header is text/html
    - Graceful degradation when subsystems are unavailable
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Health.Dashboard
  alias Ferricstore.Health.Endpoint, as: HealthEndpoint
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # HTTP helpers
  # ---------------------------------------------------------------------------

  defp http_get(port, path) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    :ok = :gen_tcp.send(conn, "GET #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  defp recv_all(conn, acc) do
    case :gen_tcp.recv(conn, 0, 5_000) do
      {:ok, data} -> recv_all(conn, acc <> data)
      {:error, :closed} -> acc
    end
  end

  defp extract_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _ -> response
    end
  end

  # ---------------------------------------------------------------------------
  # Dashboard.collect/0
  # ---------------------------------------------------------------------------

  describe "collect/0" do
    test "returns a map with all expected keys" do
      data = Dashboard.collect()

      assert is_map(data)
      assert Map.has_key?(data, :overview)
      assert Map.has_key?(data, :shards)
      assert Map.has_key?(data, :hotcold)
      assert Map.has_key?(data, :memory)
      assert Map.has_key?(data, :connections)
      assert Map.has_key?(data, :slowlog)
      assert Map.has_key?(data, :merge)
    end

    test "overview contains status, uptime, keys, and memory" do
      data = Dashboard.collect()
      overview = data.overview

      assert overview.status in [:ok, :starting]
      assert is_integer(overview.uptime_seconds)
      assert overview.uptime_seconds >= 0
      assert is_integer(overview.total_keys)
      assert overview.total_keys >= 0
      assert is_integer(overview.memory_bytes)
      assert overview.memory_bytes > 0
      assert is_binary(overview.run_id)
      assert byte_size(overview.run_id) == 40
    end

    test "shards is a list matching shard_count" do
      data = Dashboard.collect()
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      assert length(data.shards) == shard_count

      for shard <- data.shards do
        assert is_integer(shard.index)
        assert shard.status in ["ok", "down"]
        assert is_integer(shard.keys)
        assert is_integer(shard.ets_memory_bytes)
      end
    end

    test "hotcold contains read metrics" do
      data = Dashboard.collect()
      hc = data.hotcold

      assert is_float(hc.hot_read_pct)
      assert hc.hot_read_pct >= 0.0 and hc.hot_read_pct <= 100.0
      assert is_float(hc.cold_reads_per_sec)
      assert is_integer(hc.total_hot)
      assert is_integer(hc.total_cold)
      assert is_list(hc.top_prefixes)
    end

    test "memory contains pressure level and eviction policy" do
      data = Dashboard.collect()
      mem = data.memory

      assert mem.pressure_level in [:ok, :warning, :pressure, :reject]
      assert is_atom(mem.eviction_policy)
      assert is_integer(mem.total_bytes)
      assert is_integer(mem.max_bytes)
      assert is_float(mem.ratio)
      assert is_map(mem.shards)
    end

    test "connections contains active, blocked, and tracking counts" do
      data = Dashboard.collect()
      conns = data.connections

      assert is_integer(conns.active)
      assert conns.active >= 0
      assert is_integer(conns.blocked)
      assert conns.blocked >= 0
      assert is_integer(conns.tracking)
      assert conns.tracking >= 0
    end

    test "slowlog is a list of entry maps" do
      data = Dashboard.collect()
      assert is_list(data.slowlog)

      for entry <- data.slowlog do
        assert is_integer(entry.id)
        assert is_integer(entry.timestamp_us)
        assert is_integer(entry.duration_us)
        assert is_list(entry.command)
      end
    end

    test "merge is a list of status maps per shard" do
      data = Dashboard.collect()
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      assert length(data.merge) == shard_count

      for m <- data.merge do
        assert is_integer(m.shard_index)
        assert is_atom(m.mode)
        assert is_boolean(m.merging)
        assert is_integer(m.merge_count)
        assert is_integer(m.total_bytes_reclaimed)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Dashboard.render/1
  # ---------------------------------------------------------------------------

  describe "render/1" do
    setup do
      data = Dashboard.collect()
      html = Dashboard.render(data)
      %{data: data, html: html}
    end

    test "returns a valid HTML document", %{html: html} do
      assert is_binary(html)
      assert String.starts_with?(html, "<!DOCTYPE html>")
      assert String.contains?(html, "<html")
      assert String.contains?(html, "</html>")
    end

    test "contains auto-refresh meta tag", %{html: html} do
      assert String.contains?(html, ~s(http-equiv="refresh"))
      assert String.contains?(html, ~s(content="2"))
    end

    test "contains page title", %{html: html} do
      assert String.contains?(html, "<title>FerricStore Dashboard</title>")
    end

    test "contains overview section", %{html: html} do
      assert String.contains?(html, "Overview")
      assert String.contains?(html, "Uptime")
      assert String.contains?(html, "Total Keys")
      assert String.contains?(html, "Memory")
      assert String.contains?(html, "Commands")
    end

    test "contains per-shard status section", %{html: html} do
      assert String.contains?(html, "Per-Shard Status")
      assert String.contains?(html, "ETS Memory")
    end

    test "contains hot/cold metrics section", %{html: html} do
      assert String.contains?(html, "Hot/Cold Metrics")
      assert String.contains?(html, "Hot Read %")
      assert String.contains?(html, "Cold Reads/sec")
    end

    test "contains memory pressure section", %{html: html} do
      assert String.contains?(html, "Memory Pressure")
      assert String.contains?(html, "Pressure Level")
      assert String.contains?(html, "Eviction Policy")
    end

    test "contains connections section", %{html: html} do
      assert String.contains?(html, "Connections")
      assert String.contains?(html, "Active Connections")
      assert String.contains?(html, "Blocked Clients")
      assert String.contains?(html, "Tracking Clients")
    end

    test "contains slowlog section", %{html: html} do
      assert String.contains?(html, "Slowlog")
      assert String.contains?(html, "Duration")
      assert String.contains?(html, "Command")
    end

    test "contains merge status section", %{html: html} do
      assert String.contains?(html, "Merge Status")
      assert String.contains?(html, "Last Merge")
      assert String.contains?(html, "Bytes Reclaimed")
    end

    test "contains run ID in footer", %{data: data, html: html} do
      assert String.contains?(html, data.overview.run_id)
    end

    test "escapes HTML entities in rendered output" do
      # The render function should safely escape any user-controlled data.
      # Verify by checking that the escape function works correctly.
      data = Dashboard.collect()
      html = Dashboard.render(data)

      # The run_id is hex so no entities, but the structure should not contain
      # raw unescaped angle brackets from data.
      refute String.contains?(html, "<script>")
    end
  end

  # ---------------------------------------------------------------------------
  # HTTP endpoint: GET /dashboard
  # ---------------------------------------------------------------------------

  describe "GET /dashboard HTTP endpoint" do
    test "returns 200 with HTML content" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "text/html"
    end

    test "response body contains valid HTML" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, "<!DOCTYPE html>")
      assert String.contains?(body, "<title>FerricStore Dashboard</title>")
      assert String.contains?(body, "</html>")
    end

    test "response contains auto-refresh meta tag" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, ~s(http-equiv="refresh"))
      assert String.contains?(body, ~s(content="2"))
    end

    test "response contains all dashboard sections" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, "Overview")
      assert String.contains?(body, "Per-Shard Status")
      assert String.contains?(body, "Hot/Cold Metrics")
      assert String.contains?(body, "Memory Pressure")
      assert String.contains?(body, "Connections")
      assert String.contains?(body, "Slowlog")
      assert String.contains?(body, "Merge Status")
    end

    test "Content-Type header is text/html" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      [headers, _body] = String.split(response, "\r\n\r\n", parts: 2)
      assert String.contains?(headers, "Content-Type: text/html; charset=utf-8")
    end

    test "Content-Length header is present and correct" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      [headers, body] = String.split(response, "\r\n\r\n", parts: 2)

      # Extract Content-Length from headers
      content_length =
        headers
        |> String.split("\r\n")
        |> Enum.find_value(fn line ->
          case String.split(line, ": ", parts: 2) do
            ["Content-Length", value] -> String.to_integer(String.trim(value))
            _ -> nil
          end
        end)

      assert content_length == byte_size(body)
    end
  end

  # ---------------------------------------------------------------------------
  # Existing endpoints still work after dashboard addition
  # ---------------------------------------------------------------------------

  describe "existing endpoints unaffected" do
    test "/health/ready still returns JSON" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/ready")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "application/json"
      assert response =~ ~s("status":"ok")
    end

    test "unknown paths still return 404" do
      port = HealthEndpoint.port()
      response = http_get(port, "/nonexistent")

      assert response =~ "HTTP/1.1 404 Not Found"
    end
  end

  # ---------------------------------------------------------------------------
  # Dashboard reflects live data changes
  # ---------------------------------------------------------------------------

  describe "dashboard reflects live data" do
    test "shows shard status as ok when shards are running" do
      data = Dashboard.collect()

      # At least one shard should be ok since the app is running
      assert Enum.any?(data.shards, fn s -> s.status == "ok" end)
    end

    test "memory data shows non-zero max_bytes" do
      data = Dashboard.collect()

      assert data.memory.max_bytes > 0
    end

    test "eviction policy matches configuration" do
      data = Dashboard.collect()
      configured = Application.get_env(:ferricstore, :eviction_policy, :volatile_lru)

      assert data.memory.eviction_policy == configured
    end
  end
end
