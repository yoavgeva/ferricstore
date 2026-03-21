defmodule FerricstoreServer.NodeLifecycleTest do
  @moduledoc """
  Tests for full node lifecycle: the combination of startup verification,
  runtime operation, shard crash recovery, and graceful shutdown semantics.

  This module exercises the transitions between states rather than just
  verifying a single point in time. It covers:

    - Supervisor tree is healthy after application start
    - Writing data, crashing a shard, and recovering it
    - Shard restart preserves data written before the crash
    - ETS tables are recreated after shard restart
    - TCP listener remains functional during shard restart
    - Graceful shard shutdown preserves all pending data
  """

  use ExUnit.Case, async: false

  alias Ferricstore.DataDir
  alias Ferricstore.Store.{Router, ShardSupervisor}
  alias Ferricstore.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp ukey(base), do: "nlc_#{base}_#{:rand.uniform(9_999_999)}"

  defp wait_for_new_pid(name, old_pid, attempts \\ 40)

  defp wait_for_new_pid(_name, _old_pid, 0) do
    raise "Shard did not restart within the expected time"
  end

  defp wait_for_new_pid(name, old_pid, attempts) do
    case Process.whereis(name) do
      nil ->
        Process.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      ^old_pid ->
        Process.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      new_pid ->
        new_pid
    end
  end

  defp kill_and_wait_restart(index) do
    name = Router.shard_name(index)
    old_pid = Process.whereis(name)
    ref = Process.monitor(old_pid)
    Process.exit(old_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
    new_pid = wait_for_new_pid(name, old_pid)
    {old_pid, new_pid}
  end

  # TCP helpers

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
  # Supervisor tree health
  # ---------------------------------------------------------------------------

  describe "supervisor tree health" do
    test "top-level supervisor is alive and has expected children" do
      pid = Process.whereis(Ferricstore.Supervisor)
      assert is_pid(pid) and Process.alive?(pid)

      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _, _, _} -> id end)

      assert ShardSupervisor in ids
      assert Ferricstore.Stats in ids
      assert Ferricstore.MemoryGuard in ids
    end

    test "all children are alive" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      for {id, pid, _type, _mods} <- children do
        assert is_pid(pid) and Process.alive?(pid),
               "Child #{inspect(id)} should be alive"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Write -> crash -> recover cycle
  # ---------------------------------------------------------------------------

  describe "write, crash, and recover cycle" do
    @tag :capture_log
    test "data survives shard crash and is available after restart" do
      key = ukey("crash_recover")
      value = "important_data"

      Router.put(key, value, 0)
      assert value == Router.get(key)

      # Flush to disk before crashing.
      shard_idx = Router.shard_for(key)
      shard_name = Router.shard_name(shard_idx)
      :ok = GenServer.call(shard_name, :flush)

      # Kill the shard that owns this key.
      old_pid = Process.whereis(shard_name)
      ref = Process.monitor(old_pid)
      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000

      # Wait for the supervisor to restart it.
      new_pid = wait_for_new_pid(shard_name, old_pid)
      assert new_pid != old_pid

      # Data should be recovered from Bitcask.
      assert value == Router.get(key),
             "Data should survive shard crash and be recovered from Bitcask"
    end

    @tag :capture_log
    test "multiple keys across different shards survive targeted shard crash" do
      keys =
        for i <- 1..20 do
          k = ukey("multi_crash_#{i}")
          Router.put(k, "val_#{i}", 0)
          k
        end

      ShardHelpers.flush_all_shards()

      # Kill shard 0 only.
      {_old, _new} = kill_and_wait_restart(0)

      # All 20 keys should still be readable.
      for {k, i} <- Enum.with_index(keys, 1) do
        assert "val_#{i}" == Router.get(k),
               "Key #{k} should survive shard 0 crash"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # ETS recreation after shard restart
  # ---------------------------------------------------------------------------

  describe "ETS tables recreated after shard restart" do
    @tag :capture_log
    test "ETS table exists and is usable after shard restart" do
      # Write to shard 2 and verify ETS.
      key =
        Enum.find_value(1..100_000, fn n ->
          k = "nlc_ets_probe_#{n}"
          if Router.shard_for(k) == 2, do: k
        end)

      Router.put(key, "ets_test", 0)
      assert "ets_test" == Router.get(key)

      # Verify ETS has the entry.
      ets_name = :hot_cache_2
      assert [{^key, "ets_test", _}] = :ets.lookup(ets_name, key)

      # Flush and crash shard 2.
      :ok = GenServer.call(Router.shard_name(2), :flush)
      {_old, _new} = kill_and_wait_restart(2)

      # Wait for the new shard to finish init (ETS table creation).
      # The shard registers its name before init completes, so we need
      # to call it to ensure init has finished.
      :ok = GenServer.call(Router.shard_name(2), :flush)

      # ETS table should still exist (recreated by the new shard process).
      ref = :ets.whereis(ets_name)
      refute ref == :undefined, "ETS table should be recreated after restart"

      # The old cached entry is gone (ETS was recreated), but the data
      # is recoverable from Bitcask on the first GET.
      assert "ets_test" == Router.get(key)

      # Now it should be back in ETS.
      assert [{^key, "ets_test", _}] = :ets.lookup(ets_name, key)
    end
  end

  # ---------------------------------------------------------------------------
  # TCP listener during shard restart
  # ---------------------------------------------------------------------------

  describe "TCP listener during shard restart" do
    @tag :capture_log
    test "PING works during and after shard restart" do
      port = Listener.port()
      sock = connect_and_hello(port)

      # Verify baseline.
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      # Kill shard 3.
      {_old, _new} = kill_and_wait_restart(3)

      # TCP should still work.
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    @tag :capture_log
    test "SET/GET works after shard restart" do
      port = Listener.port()
      sock = connect_and_hello(port)

      # Write a key before crash.
      key = ukey("tcp_lifecycle")
      send_cmd(sock, ["SET", key, "before_crash"])
      assert recv_response(sock) == {:simple, "OK"}

      # Flush the shard that owns this key.
      shard_idx = Router.shard_for(key)
      :ok = GenServer.call(Router.shard_name(shard_idx), :flush)

      # Kill that shard.
      {_old, _new} = kill_and_wait_restart(shard_idx)

      # Verify data is recovered via TCP.
      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "before_crash"

      # New writes should also work.
      key2 = ukey("tcp_after")
      send_cmd(sock, ["SET", key2, "after_crash"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key2])
      assert recv_response(sock) == "after_crash"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Graceful shard shutdown preserves data
  # ---------------------------------------------------------------------------

  describe "graceful shard shutdown preserves data" do
    @tag :capture_log
    test "isolated shard: write, stop gracefully, verify data on disk" do
      # Isolated shard tests bypass Raft (no ra system for ad-hoc indices)
      original = Application.get_env(:ferricstore, :raft_enabled)
      Application.put_env(:ferricstore, :raft_enabled, false)

      tmp_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_nlc_graceful_#{:rand.uniform(9_999_999)}"
        )

      File.mkdir_p!(tmp_dir)
      on_exit(fn ->
        Application.put_env(:ferricstore, :raft_enabled, original)
        File.rm_rf(tmp_dir)
      end)

      # Start isolated shard (index 98 to avoid conflicts).
      opts = [index: 98, data_dir: tmp_dir, flush_interval_ms: 1]
      {:ok, pid} = GenServer.start_link(Ferricstore.Store.Shard, opts)

      # Write several keys.
      for i <- 1..10 do
        :ok = GenServer.call(pid, {:put, "lifecycle_#{i}", "val_#{i}", 0})
      end

      :ok = GenServer.call(pid, :flush)

      # Stop gracefully.
      GenServer.stop(pid, :normal, 5_000)
      refute Process.alive?(pid)

      # Verify data is on disk by opening a fresh NIF store.
      shard_dir = DataDir.shard_data_path(tmp_dir, 98)
      {:ok, store} = Ferricstore.Bitcask.NIF.new(shard_dir)

      for i <- 1..10 do
        {:ok, val} = Ferricstore.Bitcask.NIF.get(store, "lifecycle_#{i}")
        assert val == "val_#{i}", "Key lifecycle_#{i} should survive graceful stop"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Full lifecycle: startup state + operation + health check
  # ---------------------------------------------------------------------------

  describe "full lifecycle integration" do
    test "startup state is consistent, operations work, health checks pass" do
      # 1. Verify startup state.
      for i <- 0..3 do
        name = Router.shard_name(i)
        pid = Process.whereis(name)
        assert is_pid(pid) and Process.alive?(pid)

        ets = :"keydir_#{i}"
        refute :ets.whereis(ets) == :undefined
      end

      refute :ets.whereis(:ferricstore_waiters) == :undefined
      refute :ets.whereis(:ferricstore_tracking) == :undefined

      assert Process.alive?(Process.whereis(Ferricstore.MemoryGuard))
      assert Process.alive?(Process.whereis(Ferricstore.Stats))

      # 2. Perform some operations.
      port = Listener.port()
      sock = connect_and_hello(port)

      for i <- 1..5 do
        k = ukey("lifecycle_op_#{i}")
        send_cmd(sock, ["SET", k, "val_#{i}"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      # 3. Health checks.
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      send_cmd(sock, ["DBSIZE"])
      dbsize = recv_response(sock)
      assert is_integer(dbsize) and dbsize >= 5

      send_cmd(sock, ["INFO"])
      info = recv_response(sock)
      assert is_binary(info) and byte_size(info) > 0

      :gen_tcp.close(sock)

      # 4. Stats are tracking.
      assert Ferricstore.Stats.total_connections() > 0
      assert Ferricstore.Stats.total_commands() > 0
    end
  end
end
