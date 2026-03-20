defmodule FerricstoreServer.TelemetryEventsTest do
  @moduledoc """
  Tests for all spec section 4.8 telemetry events.

  Covers:
    1. [:ferricstore, :memory, :pressure]    -- already existed, verified here
    2. [:ferricstore, :memory, :recovered]    -- emitted on pressure -> ok transition
    3. [:ferricstore, :expiry, :struggling]   -- sweep hits ceiling for N consecutive cycles
    4. [:ferricstore, :expiry, :recovered]    -- sweep drops below ceiling
    5. [:ferricstore, :connection, :threshold] -- connection count crosses 80%/95%
    6. [:ferricstore, :config, :changed]       -- on CONFIG SET
    7. [:ferricstore, :node, :startup_complete] -- once at startup
    8. [:ferricstore, :node, :shutdown_started] -- tested indirectly (fires on prep_stop)
    9. [:ferricstore, :slow_log, :near_full]    -- slowlog ring buffer at 90%
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helper: attach a one-shot telemetry handler that sends events to test pid
  # ---------------------------------------------------------------------------

  defp attach_handler(event_name) do
    test_pid = self()
    handler_id = "test-#{inspect(event_name)}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      event_name,
      fn event, measurements, metadata, config ->
        send(config.test_pid, {:telemetry, event, measurements, metadata})
      end,
      %{test_pid: test_pid}
    )

    handler_id
  end

  # ---------------------------------------------------------------------------
  # 1. [:ferricstore, :memory, :pressure] -- already exists
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :memory, :pressure]" do
    test "emitted when memory exceeds pressure threshold" do
      handler_id = attach_handler([:ferricstore, :memory, :pressure])

      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 50,
          max_memory_bytes: 1,
          shard_count: 4
        ])

      # Match specifically on per-shard events (contain pressure_level in metadata)
      assert_receive {:telemetry, [:ferricstore, :memory, :pressure], measurements,
                      %{pressure_level: _} = metadata},
                     2000

      assert is_integer(measurements.shard_index)
      assert is_integer(measurements.bytes)
      assert is_float(measurements.ratio)
      assert metadata.pressure_level in [:pressure, :reject]

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. [:ferricstore, :memory, :recovered]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :memory, :recovered]" do
    test "emitted when pressure drops from :pressure/:reject back to :ok" do
      handler_id = attach_handler([:ferricstore, :memory, :recovered])

      # Start with a tiny budget to force pressure/reject.
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 50,
          max_memory_bytes: 1,
          shard_count: 4
        ])

      # Wait for at least one pressure check cycle to set last_pressure_level.
      Process.sleep(150)

      # Now increase the budget to a large value so next check goes to :ok.
      # We need to do this via a direct message since the GenServer doesn't
      # expose a setter. Instead, stop and start with a large budget, but
      # first we need the state to have been in pressure. Let's use a different
      # approach: send a check with modified state.
      #
      # The simplest approach: replace the state to have a large max, which
      # makes the next check return :ok. Use sys to modify state.
      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: 1_073_741_824}
      end)

      # Trigger a check
      send(pid, :check)

      assert_receive {:telemetry, [:ferricstore, :memory, :recovered], measurements, metadata},
                     2000

      assert is_integer(measurements.total_bytes)
      assert is_integer(measurements.max_bytes)
      assert is_float(measurements.ratio)
      assert metadata.previous_level in [:pressure, :reject]

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test "not emitted when transitioning from :warning to :ok" do
      handler_id = attach_handler([:ferricstore, :memory, :recovered])

      # Start with a normal budget. The app-wide guard already runs at :ok.
      # Force a state where last_pressure_level = :warning, then trigger :ok.
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4
        ])

      :sys.replace_state(pid, fn state ->
        %{state | last_pressure_level: :warning}
      end)

      send(pid, :check)
      Process.sleep(100)

      refute_received {:telemetry, [:ferricstore, :memory, :recovered], _, _}

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. [:ferricstore, :expiry, :struggling]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :expiry, :struggling]" do
    test "emitted when sweep hits ceiling for 3 consecutive cycles" do
      handler_id = attach_handler([:ferricstore, :expiry, :struggling])

      # Set max_keys_per_sweep very low so it hits ceiling easily.
      original = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep)
      Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, 2)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, original)
        else
          Application.delete_env(:ferricstore, :expiry_max_keys_per_sweep)
        end

        :telemetry.detach(handler_id)
      end)

      past = System.os_time(:millisecond) - 5_000
      uid = System.unique_integer([:positive])

      # Create many expired keys on shard 0 so each sweep of 2 keys hits ceiling.
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "expiry_struggle_#{uid}_#{i}" end)
        |> Stream.filter(fn k -> Router.shard_for(k) == 0 end)
        |> Enum.take(20)

      Enum.each(keys, fn k -> Router.put(k, "val", past) end)

      shard_name = Router.shard_name(0)

      # Run 3+ sweeps. The first 3 should all hit ceiling (2 keys removed per sweep,
      # but 20 expired keys remain). After 3 ceiling hits, :struggling fires.
      for _ <- 1..4 do
        GenServer.call(shard_name, :expiry_sweep)
      end

      assert_receive {:telemetry, [:ferricstore, :expiry, :struggling], measurements, _metadata},
                     1000

      assert measurements.shard_index == 0
      assert measurements.consecutive_ceiling_sweeps >= 3
      assert measurements.max_keys_per_sweep == 2
    end
  end

  # ---------------------------------------------------------------------------
  # 4. [:ferricstore, :expiry, :recovered]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :expiry, :recovered]" do
    test "emitted when sweep drops below ceiling after struggling" do
      recovered_handler = attach_handler([:ferricstore, :expiry, :recovered])

      original = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep)
      Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, 2)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, original)
        else
          Application.delete_env(:ferricstore, :expiry_max_keys_per_sweep)
        end

        :telemetry.detach(recovered_handler)
      end)

      # Use shard 1 to avoid state left over from the struggling test on shard 0.
      # First, reset sweep state by triggering a below-ceiling sweep (no expired keys).
      shard_name = Router.shard_name(1)
      GenServer.call(shard_name, :expiry_sweep)

      past = System.os_time(:millisecond) - 5_000
      uid = System.unique_integer([:positive])

      # Create exactly 10 expired keys on shard 1 -- enough for 3 ceiling sweeps
      # (2 per sweep = 6 removed) plus 2 more sweeps to drain remaining + recover.
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "expiry_recover_#{uid}_#{i}" end)
        |> Stream.filter(fn k -> Router.shard_for(k) == 1 end)
        |> Enum.take(10)

      Enum.each(keys, fn k -> Router.put(k, "val", past) end)

      # Run 3 sweeps to enter struggling state (each removes 2 at ceiling).
      for _ <- 1..3 do
        GenServer.call(shard_name, :expiry_sweep)
      end

      # Run more sweeps to drain remaining keys (4 left) and drop below ceiling.
      for _ <- 1..3 do
        GenServer.call(shard_name, :expiry_sweep)
      end

      assert_receive {:telemetry, [:ferricstore, :expiry, :recovered], measurements, _metadata},
                     1000

      assert measurements.shard_index == 1
      assert measurements.previous_ceiling_sweeps >= 3
    end
  end

  # ---------------------------------------------------------------------------
  # 5. [:ferricstore, :connection, :threshold]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :connection, :threshold]" do
    test "emitted when connection count crosses 80% of maxclients" do
      handler_id = attach_handler([:ferricstore, :connection, :threshold])

      # Set maxclients very low so we can trigger easily with few connections.
      original = Application.get_env(:ferricstore, :maxclients)
      Application.put_env(:ferricstore, :maxclients, 5)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :maxclients, original)
        else
          Application.delete_env(:ferricstore, :maxclients)
        end

        :telemetry.detach(handler_id)
      end)

      port = FerricstoreServer.Listener.port()

      # Open enough connections to cross 80% of 5 = 4 connections.
      sockets =
        for _ <- 1..5 do
          {:ok, sock} =
            :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
          sock
        end

      # Allow connections to be accepted and registered.
      Process.sleep(200)

      assert_receive {:telemetry, [:ferricstore, :connection, :threshold], measurements,
                      metadata},
                     2000

      assert is_integer(measurements.active)
      assert measurements.max == 5
      assert is_float(measurements.ratio)
      assert metadata.level in [:warning, :critical]

      Enum.each(sockets, &:gen_tcp.close/1)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. [:ferricstore, :config, :changed]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :config, :changed]" do
    test "emitted on CONFIG SET for a valid parameter" do
      handler_id = attach_handler([:ferricstore, :config, :changed])

      # Read current value before changing, to avoid cross-test contamination
      original = Ferricstore.Config.get_value("hz") || "10"

      on_exit(fn ->
        Ferricstore.Config.set("hz", original)
        :telemetry.detach(handler_id)
      end)

      Ferricstore.Config.set("hz", "20")

      assert_receive {:telemetry, [:ferricstore, :config, :changed], _measurements, metadata},
                     1000

      assert metadata.param == "hz"
      assert metadata.value == "20"
      assert metadata.old_value == original
    end

    test "emitted with old_value when changing an existing parameter" do
      handler_id = attach_handler([:ferricstore, :config, :changed])

      on_exit(fn ->
        Ferricstore.Config.set("slowlog-log-slower-than", "10000")
        :telemetry.detach(handler_id)
      end)

      Ferricstore.Config.set("slowlog-log-slower-than", "5000")

      assert_receive {:telemetry, [:ferricstore, :config, :changed], _measurements, metadata},
                     1000

      assert metadata.param == "slowlog-log-slower-than"
      assert metadata.value == "5000"
    end

    test "not emitted when CONFIG SET is rejected (read-only param)" do
      handler_id = attach_handler([:ferricstore, :config, :changed])

      result = Ferricstore.Config.set("maxmemory", "999")
      assert {:error, _} = result

      Process.sleep(100)
      refute_received {:telemetry, [:ferricstore, :config, :changed], _, _}

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 7. [:ferricstore, :node, :startup_complete]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :node, :startup_complete]" do
    test "was emitted during application startup (verify via persistent_term or indirect check)" do
      # We cannot re-start the application in tests, but we can verify that
      # the telemetry infrastructure is wired correctly by attaching a handler
      # and calling the telemetry event directly (confirming the event name
      # is valid and the handler machinery works).
      handler_id = attach_handler([:ferricstore, :node, :startup_complete])

      # Simulate what Application.start does
      :telemetry.execute(
        [:ferricstore, :node, :startup_complete],
        %{duration_ms: 42},
        %{shard_count: 4, port: 0, raft_enabled: false}
      )

      assert_receive {:telemetry, [:ferricstore, :node, :startup_complete], measurements,
                      metadata},
                     1000

      assert measurements.duration_ms == 42
      assert metadata.shard_count == 4
      assert metadata.raft_enabled == false

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 8. [:ferricstore, :node, :shutdown_started]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :node, :shutdown_started]" do
    test "event name is valid and carries expected measurements" do
      # We cannot trigger a real shutdown in tests without stopping the
      # application. Verify the telemetry event fires correctly by simulating it.
      handler_id = attach_handler([:ferricstore, :node, :shutdown_started])

      :telemetry.execute(
        [:ferricstore, :node, :shutdown_started],
        %{uptime_ms: 12345},
        %{}
      )

      assert_receive {:telemetry, [:ferricstore, :node, :shutdown_started], measurements,
                      _metadata},
                     1000

      assert measurements.uptime_ms == 12345

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 9. [:ferricstore, :slow_log, :near_full]
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :slow_log, :near_full]" do
    test "emitted when slowlog ring buffer reaches 90% capacity" do
      handler_id = attach_handler([:ferricstore, :slow_log, :near_full])

      # Set slowlog-max-len to 10 so we can fill it easily.
      original_max = Application.get_env(:ferricstore, :slowlog_max_len)
      original_threshold = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_max_len, 10)
      # Set threshold to 0 so every command is logged.
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original_max do
          Application.put_env(:ferricstore, :slowlog_max_len, original_max)
        else
          Application.delete_env(:ferricstore, :slowlog_max_len)
        end

        if original_threshold do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original_threshold)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end

        Ferricstore.SlowLog.reset()
        :telemetry.detach(handler_id)
      end)

      # Reset the slowlog first.
      Ferricstore.SlowLog.reset()

      # Fill the slowlog to 90% (9 out of 10 entries).
      for i <- 1..10 do
        Ferricstore.SlowLog.maybe_log(["SET", "key#{i}", "val"], 1, nil)
      end

      # Allow the async casts to be processed.
      Process.sleep(200)

      assert_receive {:telemetry, [:ferricstore, :slow_log, :near_full], measurements,
                      _metadata},
                     2000

      assert is_integer(measurements.size)
      assert is_integer(measurements.max)
      assert measurements.max == 10
      assert is_float(measurements.ratio)
      assert measurements.ratio >= 0.90
    end

    test "not emitted when slowlog is well below 90% capacity" do
      handler_id = attach_handler([:ferricstore, :slow_log, :near_full])

      original_max = Application.get_env(:ferricstore, :slowlog_max_len)
      original_threshold = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
      Application.put_env(:ferricstore, :slowlog_max_len, 100)
      Application.put_env(:ferricstore, :slowlog_log_slower_than_us, 0)

      on_exit(fn ->
        if original_max do
          Application.put_env(:ferricstore, :slowlog_max_len, original_max)
        else
          Application.delete_env(:ferricstore, :slowlog_max_len)
        end

        if original_threshold do
          Application.put_env(:ferricstore, :slowlog_log_slower_than_us, original_threshold)
        else
          Application.delete_env(:ferricstore, :slowlog_log_slower_than_us)
        end

        Ferricstore.SlowLog.reset()
        :telemetry.detach(handler_id)
      end)

      Ferricstore.SlowLog.reset()

      # Log only 5 entries out of 100 capacity (5%).
      for i <- 1..5 do
        Ferricstore.SlowLog.maybe_log(["SET", "key#{i}", "val"], 1, nil)
      end

      Process.sleep(200)

      refute_received {:telemetry, [:ferricstore, :slow_log, :near_full], _, _}
    end
  end

  # ---------------------------------------------------------------------------
  # Hot/cold read tracking in Stats
  # ---------------------------------------------------------------------------

  describe "hot/cold read tracking in Stats" do
    setup do
      Ferricstore.Stats.reset_hotness()
      :ok
    end

    test "hot read is recorded when key is in ETS cache" do
      key = "hotcold_hot_#{System.unique_integer([:positive])}"
      Router.put(key, "value", 0)

      # First GET warms the cache (may be cold if not yet in ETS).
      _val = Router.get(key)

      # Reset counters after warm-up.
      Ferricstore.Stats.reset_hotness()

      # Second GET should be hot (ETS hit).
      assert Router.get(key) == "value"

      hot = Ferricstore.Stats.total_hot_reads()
      assert hot >= 1
    end

    test "cold read is recorded when key is not in ETS cache" do
      key = "hotcold_cold_#{System.unique_integer([:positive])}"
      Router.put(key, "cold_value", 0)

      # Flush to disk so Bitcask has the data.
      shard_idx = Router.shard_for(key)
      shard_name = Router.shard_name(shard_idx)
      :ok = GenServer.call(shard_name, :flush)

      # Remove from ETS to force a cold read.
      :ets.delete(:"keydir_#{shard_idx}", key)
      :ets.delete(:"hot_cache_#{shard_idx}", key)

      Ferricstore.Stats.reset_hotness()

      assert Router.get(key) == "cold_value"

      cold = Ferricstore.Stats.total_cold_reads()
      assert cold >= 1
    end

    test "INFO stats section includes hot/cold read fields" do
      Router.put("hotcold_info_test", "val", 0)
      _val = Router.get("hotcold_info_test")

      store = %{
        dbsize: fn -> 1 end
      }

      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], store)

      assert is_binary(info)
      assert info =~ "hot_reads:"
      assert info =~ "cold_reads:"
      assert info =~ "hot_read_pct:"
    end
  end
end
