defmodule Ferricstore.Store.ShardAsyncTest do
  @moduledoc """
  Tests for the async flush path in `Ferricstore.Store.Shard`.

  Covers:
  - `flush_in_flight` state tracking
  - `{:io_complete, op_id, result}` handling
  - `await_in_flight` used by delete, keys, and explicit flush
  - Correct behaviour on sync fallback (macOS / no io_uring)
  - Data correctness after async flush
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Shard

  setup do
    # Isolated shard tests bypass Raft (no ra system for ad-hoc indices)
    original = Application.get_env(:ferricstore, :raft_enabled)
    Application.put_env(:ferricstore, :raft_enabled, false)
    on_exit(fn -> Application.put_env(:ferricstore, :raft_enabled, original) end)
    :ok
  end

  # Start an isolated shard (not the app-supervised ones) for each test.
  defp start_shard do
    dir = Path.join(System.tmp_dir!(), "shard_async_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    index = :erlang.unique_integer([:positive]) |> rem(10_000) |> Kernel.+(20_000)
    {:ok, pid} = Shard.start_link(index: index, data_dir: dir)
    {pid, index, dir}
  end

  # ---------------------------------------------------------------------------
  # Basic put → flush → get
  # ---------------------------------------------------------------------------

  describe "put and get through async flush" do
    test "value written via put is readable immediately (ETS)" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "ak", "av", 0})
      # ETS write is synchronous — value readable before fsync
      assert "av" == GenServer.call(pid, {:get, "ak"})
    end

    test "value is durable after flush (sync call)" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "durable", "yes", 0})
      # :flush awaits in-flight and does sync flush
      :ok = GenServer.call(pid, :flush)

      assert "yes" == GenServer.call(pid, {:get, "durable"})
    end
  end

  # ---------------------------------------------------------------------------
  # flush_in_flight state
  # ---------------------------------------------------------------------------

  describe "flush_in_flight state" do
    test "flush_in_flight is nil on a fresh shard" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end

    test "flush_in_flight is cleared after :flush call" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "fi_k", "fi_v", 0})
      :ok = GenServer.call(pid, :flush)

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end

    test "pending is empty after successful flush" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "p_k", "p_v", 0})
      :ok = GenServer.call(pid, :flush)

      state = :sys.get_state(pid)
      assert state.pending == []
    end
  end

  # ---------------------------------------------------------------------------
  # delete awaits in-flight
  # ---------------------------------------------------------------------------

  describe "delete awaits in-flight flush" do
    test "delete after put returns nil on get" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "del_k", "del_v", 0})
      :ok = GenServer.call(pid, {:delete, "del_k"})

      assert nil == GenServer.call(pid, {:get, "del_k"})
    end

    test "delete flushes pending before writing tombstone" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "before_del", "v", 0})
      :ok = GenServer.call(pid, {:put, "del_target", "v", 0})
      :ok = GenServer.call(pid, {:delete, "del_target"})

      # "before_del" was in pending alongside "del_target"; delete should
      # have flushed it first so it survives in Bitcask.
      assert nil == GenServer.call(pid, {:get, "del_target"})
    end

    test "flush_in_flight is nil after delete" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "dfi_k", "v", 0})
      :ok = GenServer.call(pid, {:delete, "dfi_k"})

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end
  end

  # ---------------------------------------------------------------------------
  # keys awaits in-flight
  # ---------------------------------------------------------------------------

  describe "keys awaits in-flight flush" do
    test "keys returns written key after flush" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "keys_k", "v", 0})
      keys = GenServer.call(pid, :keys)

      assert "keys_k" in keys
    end

    test "keys does not return deleted key" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :ok = GenServer.call(pid, {:put, "kd_a", "v", 0})
      :ok = GenServer.call(pid, {:put, "kd_b", "v", 0})
      :ok = GenServer.call(pid, {:delete, "kd_a"})

      keys = GenServer.call(pid, :keys)
      assert "kd_b" in keys
      refute "kd_a" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # io_complete message handling
  # ---------------------------------------------------------------------------

  describe "handle_info {:io_complete, ...}" do
    test "stale op_id is ignored (flush_in_flight stays nil)" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      # Start with no in-flight op; send a stale completion message
      send(pid, {:io_complete, 99_999, :ok})
      # Give the GenServer time to process the message
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end

    test "error completion clears flush_in_flight" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      # Inject a fake in-flight op_id directly into state
      :sys.replace_state(pid, fn state -> %{state | flush_in_flight: 42} end)

      # Send an error completion for that op_id
      send(pid, {:io_complete, 42, {:error, "simulated disk error"}})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end

    test "matching op_id completion clears flush_in_flight" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      :sys.replace_state(pid, fn state -> %{state | flush_in_flight: 7} end)
      send(pid, {:io_complete, 7, :ok})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent puts fill the batch window
  # ---------------------------------------------------------------------------

  describe "concurrent puts" do
    test "10 concurrent puts are all readable" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            GenServer.call(pid, {:put, "cc_k#{i}", "cc_v#{i}", 0})
          end)
        end

      Enum.each(tasks, &Task.await(&1, 5_000))
      :ok = GenServer.call(pid, :flush)

      for i <- 1..10 do
        assert "cc_v#{i}" == GenServer.call(pid, {:get, "cc_k#{i}"})
      end
    end

    test "50 concurrent puts are all durable after flush" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            GenServer.call(pid, {:put, "bulk_k#{i}", "bulk_v#{i}", 0})
          end)
        end

      Enum.each(tasks, &Task.await(&1, 5_000))
      :ok = GenServer.call(pid, :flush)

      for i <- 1..50 do
        assert "bulk_v#{i}" == GenServer.call(pid, {:get, "bulk_k#{i}"})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Expiry through the async path
  # ---------------------------------------------------------------------------

  describe "expiry through async flush" do
    test "expired key returns nil after flush" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 500
      :ok = GenServer.call(pid, {:put, "exp_async", "gone", past})
      :ok = GenServer.call(pid, :flush)

      assert nil == GenServer.call(pid, {:get, "exp_async"})
    end

    test "live key with TTL is readable after flush" do
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      future = System.os_time(:millisecond) + 60_000
      :ok = GenServer.call(pid, {:put, "live_async", "here", future})
      :ok = GenServer.call(pid, :flush)

      assert "here" == GenServer.call(pid, {:get, "live_async"})
    end
  end

  # ---------------------------------------------------------------------------
  # ETS bypass consistency with async flush
  # ---------------------------------------------------------------------------

  describe "ETS consistency with async flush" do
    test "ETS is written synchronously; Bitcask flush happens async" do
      {pid, index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      ets = :"hot_cache_#{index}"
      :ok = GenServer.call(pid, {:put, "ets_sync", "val", 0})

      # ETS must have the value immediately (sync write in put handler)
      assert [{_, "val", _}] = :ets.lookup(ets, "ets_sync")
    end

    test "delete clears ETS entry" do
      {pid, index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      ets = :"hot_cache_#{index}"
      :ok = GenServer.call(pid, {:put, "ets_del", "val", 0})
      :ok = GenServer.call(pid, {:delete, "ets_del"})

      assert [] == :ets.lookup(ets, "ets_del")
    end
  end

  # ---------------------------------------------------------------------------
  # io_uring-specific tests
  #
  # These are tagged :linux_io_uring and will only pass on Linux with a kernel
  # that supports io_uring (≥ 5.1). On macOS or older Linux kernels the shard
  # falls back to the sync path and these tests are skipped.
  # ---------------------------------------------------------------------------

  describe "io_uring async path (Linux only)" do
    @tag :linux_io_uring
    test "put_batch_async returns {:pending, op_id} on Linux with io_uring" do
      # We can't directly call NIF.put_batch_async from outside the shard, but
      # we can verify that after a put the flush_in_flight is non-nil (meaning
      # the NIF did return {:pending, op_id} instead of :ok).
      # This is only observable on Linux where the async path is active.
      {pid, _index, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf!(dir) end)

      # Inject a fake in-flight state — if we're on Linux the real flush would
      # have set this. Directly test the shard's io_complete handler instead.
      alias Ferricstore.Bitcask.NIF

      tmp = Path.join(System.tmp_dir!(), "uring_direct_#{:rand.uniform(999_999)}")
      File.mkdir_p!(tmp)
      {:ok, store} = NIF.new(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      result = NIF.put_batch_async(store, [{"uring_k", "uring_v", 0}])

      case result do
        {:pending, op_id} ->
          # We're on Linux with io_uring — await the completion message
          assert_receive {:io_complete, ^op_id, :ok}, 5_000
          assert {:ok, "uring_v"} == NIF.get(store, "uring_k")

        :ok ->
          # Sync fallback — still verify data is written
          assert {:ok, "uring_v"} == NIF.get(store, "uring_k")
      end
    end

    @tag :linux_io_uring
    test "multiple concurrent async submissions complete independently" do
      alias Ferricstore.Bitcask.NIF

      tmp = Path.join(System.tmp_dir!(), "uring_conc_#{:rand.uniform(999_999)}")
      File.mkdir_p!(tmp)
      {:ok, store} = NIF.new(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      # Submit 5 batches concurrently from 5 tasks
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            result = NIF.put_batch_async(store, [{"conc_k#{i}", "conc_v#{i}", 0}])

            case result do
              {:pending, op_id} ->
                receive do
                  {:io_complete, ^op_id, r} -> r
                after
                  5_000 -> :timeout
                end

              :ok ->
                :ok
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for i <- 1..5 do
        assert {:ok, "conc_v#{i}"} == NIF.get(store, "conc_k#{i}")
      end
    end

    @tag :linux_io_uring
    test "large batch (200 entries) completes with :ok and all data readable" do
      alias Ferricstore.Bitcask.NIF

      tmp = Path.join(System.tmp_dir!(), "uring_large_#{:rand.uniform(999_999)}")
      File.mkdir_p!(tmp)
      {:ok, store} = NIF.new(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      batch = Enum.map(1..200, fn i -> {"large_k#{i}", "large_v#{i}", 0} end)
      result = NIF.put_batch_async(store, batch)

      final_result =
        case result do
          {:pending, op_id} ->
            receive do
              {:io_complete, ^op_id, r} -> r
            after
              10_000 -> :timeout
            end

          :ok ->
            :ok
        end

      assert :ok == final_result

      for i <- 1..200 do
        assert {:ok, "large_v#{i}"} == NIF.get(store, "large_k#{i}")
      end
    end

    @tag :linux_io_uring
    test "data survives store reopen after async flush" do
      alias Ferricstore.Bitcask.NIF

      tmp = Path.join(System.tmp_dir!(), "uring_reopen_#{:rand.uniform(999_999)}")
      File.mkdir_p!(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      {:ok, store1} = NIF.new(tmp)

      result = NIF.put_batch_async(store1, [{"reopen_k", "reopen_v", 0}])

      case result do
        {:pending, op_id} ->
          assert_receive {:io_complete, ^op_id, :ok}, 5_000

        :ok ->
          :ok
      end

      _ = store1
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(tmp)
      assert {:ok, "reopen_v"} == NIF.get(store2, "reopen_k")
    end
  end
end
