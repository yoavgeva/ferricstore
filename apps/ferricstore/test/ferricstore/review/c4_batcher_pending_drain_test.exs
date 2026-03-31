defmodule Ferricstore.Review.C4BatcherPendingDrainTest do
  @moduledoc """
  Proves the Batcher pending-map drain bug.

  The bug: `Ferricstore.Raft.Batcher` has no `terminate/2` callback. If the
  Batcher process dies while callers are waiting in `state.pending` or
  `state.flush_waiters`, those callers never receive a `GenServer.reply`.

  For direct `Batcher.write/2` callers (GenServer.call), this is partially
  mitigated: GenServer.call monitors the callee, so when the Batcher dies the
  caller gets `{:EXIT, ...}`. However, `Batcher.write_async/3` is a cast —
  the `reply_to` from ref belongs to a *different* GenServer.call (client →
  Shard), whose monitor watches the Shard, not the Batcher. If the Batcher
  dies, nobody replies to `reply_to`, and the original client hangs until its
  GenServer.call timeout (10s default).

  This test exercises the write_async path: a caller does GenServer.call to a
  proxy process, the proxy delegates to the Batcher via write_async (cast),
  then the Batcher is killed. The caller should get an error promptly, but
  without a terminate/2 callback it hangs.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Test.ShardHelpers

  @shard_index 0

  setup do
    ShardHelpers.wait_shards_alive()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  describe "batcher pending drain on kill" do
    @tag timeout: 15_000
    test "write_async callers hang when batcher is killed (no terminate/2)" do
      batcher_name = Batcher.batcher_name(@shard_index)
      batcher_pid = Process.whereis(batcher_name)
      assert is_pid(batcher_pid), "Batcher must be alive"

      # We simulate what the Shard does: start a GenServer.call from a "client"
      # process, have a proxy accept the call and delegate to Batcher.write_async,
      # then kill the Batcher while the write is in-flight.
      #
      # The key insight: the client's GenServer.call monitors the *proxy*, not
      # the Batcher. So when the Batcher dies, the client's monitor doesn't fire.

      test_pid = self()

      # Start a proxy GenServer that mimics the Shard's write_async delegation
      {:ok, proxy} =
        GenServer.start_link(
          __MODULE__.ProxyServer,
          %{shard_index: @shard_index, test_pid: test_pid}
        )

      key = "drain_test_#{:rand.uniform(999_999)}"

      # Spawn a "client" that calls the proxy (like a connection calling a Shard).
      # The proxy will cast to the Batcher via write_async, then return {:noreply, ...}.
      # The client blocks on GenServer.call waiting for a reply that should come
      # from the Batcher's ra_event handler.
      caller =
        Task.async(fn ->
          try do
            GenServer.call(proxy, {:write, key}, 8_000)
          catch
            :exit, reason -> {:exit, reason}
          end
        end)

      # Wait for the write to be enqueued in the Batcher
      assert_receive {:write_enqueued, ^key}, 2_000

      # Monitor before kill so we don't race
      ref = Process.monitor(batcher_pid)

      # Kill the Batcher while the caller is waiting for a reply
      Process.exit(batcher_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^batcher_pid, _reason}, 2_000

      # Now try to get the caller's result. If the bug exists, the caller
      # hangs until the 8s GenServer.call timeout. If fixed (terminate/2
      # drains pending), the caller gets an error promptly.
      #
      # We use a 3s deadline: if the caller responds within 3s, the pending
      # map was drained (or GenServer.call caught the exit). If it takes
      # longer, the caller is hung.
      result =
        case Task.yield(caller, 3_000) do
          {:ok, value} -> {:replied, value}
          nil -> :hung
        end

      case result do
        :hung ->
          # BUG CONFIRMED: caller is hung because no terminate/2 drained pending.
          # Clean up: shut down the task and proxy.
          Task.shutdown(caller, :brutal_kill)
          GenServer.stop(proxy, :normal, 1_000)

          flunk("""
          BUG: write_async caller hung for >3s after Batcher kill.
          The Batcher has no terminate/2 callback, so callers in state.pending
          never receive GenServer.reply when the Batcher dies. The caller's
          GenServer.call monitors the proxy (Shard), not the Batcher, so it
          doesn't get an :EXIT signal.

          Fix: add terminate/2 to Batcher that replies {:error, :shutting_down}
          to all froms in state.pending and state.flush_waiters.
          """)

        {:replied, value} ->
          # If we get here, the caller was unblocked. This could mean:
          # 1. terminate/2 was added (fix applied), or
          # 2. Some other mechanism unblocked the caller.
          # Either way, verify the caller got an error, not :ok.
          assert match?({:exit, _}, value) or match?({:error, _}, value),
                 "Expected error or exit, got: #{inspect(value)}"
      end
    end

    @tag timeout: 15_000
    test "direct Batcher.write callers get :exit when batcher is killed" do
      # Contrast test: direct GenServer.call to Batcher IS protected by
      # GenServer.call's built-in monitor. This should NOT hang.
      batcher_name = Batcher.batcher_name(@shard_index)
      batcher_pid = Process.whereis(batcher_name)
      assert is_pid(batcher_pid)

      key = "drain_direct_#{:rand.uniform(999_999)}"

      caller =
        Task.async(fn ->
          try do
            Batcher.write(@shard_index, {:put, key, "val", 0})
          catch
            :exit, reason -> {:exit, reason}
          end
        end)

      # Give the write a moment to reach the Batcher
      Process.sleep(5)

      # Kill the Batcher
      Process.exit(batcher_pid, :kill)

      # Direct callers should get unblocked promptly via GenServer.call monitor
      result = Task.await(caller, 5_000)

      # The caller either succeeded (write committed before kill) or got :exit
      assert result == :ok or match?({:exit, _}, result),
             "Direct caller should get :ok or {:exit, _}, got: #{inspect(result)}"
    end

    @tag timeout: 15_000
    test "flush_waiters hang when batcher is killed mid-flush" do
      batcher_name = Batcher.batcher_name(@shard_index)
      batcher_pid = Process.whereis(batcher_name)
      assert is_pid(batcher_pid)

      # Write something so there's a pending slot to flush
      key = "drain_flush_#{:rand.uniform(999_999)}"
      :ok = Batcher.write(@shard_index, {:put, key, "val", 0})

      # Now issue writes that will be in-flight, then call flush.
      # flush adds the caller to flush_waiters if pending is non-empty.
      # We need pending to be non-empty when flush is called, so we
      # write via a task (will be batched) and immediately flush.

      # Spawn several writes that should be in the batch window
      write_tasks =
        for i <- 1..5 do
          Task.async(fn ->
            k = "drain_flush_w#{i}_#{:rand.uniform(999_999)}"

            try do
              Batcher.write(@shard_index, {:put, k, "v", 0})
            catch
              :exit, _ -> :exited
            end
          end)
        end

      # Small sleep to let writes accumulate in the slot (before timer fires)
      Process.sleep(1)

      # flush caller -- this is a GenServer.call so it IS monitored.
      # But flush_waiters in state are replied via GenServer.reply in
      # maybe_reply_flush_waiters, which won't run if Batcher dies.
      # However, since flush uses GenServer.call, the built-in monitor
      # should catch the death.
      flush_caller =
        Task.async(fn ->
          try do
            Batcher.flush(@shard_index)
          catch
            :exit, reason -> {:exit, reason}
          end
        end)

      # Give flush a moment to register
      Process.sleep(5)

      # Kill the Batcher
      Process.exit(batcher_pid, :kill)

      # flush_caller uses GenServer.call, so it should get :exit
      flush_result = Task.await(flush_caller, 5_000)

      assert flush_result == :ok or match?({:exit, _}, flush_result),
             "flush caller should be unblocked, got: #{inspect(flush_result)}"

      # Clean up write tasks
      for t <- write_tasks do
        Task.yield(t, 2_000) || Task.shutdown(t, :brutal_kill)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Proxy GenServer — mimics the Shard's write_async pattern
  # ---------------------------------------------------------------------------

  defmodule ProxyServer do
    @moduledoc false
    use GenServer

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:write, key}, from, state) do
      # Mimic what Shard does: delegate to Batcher via write_async (cast),
      # passing our caller's `from` so the Batcher replies directly.
      Batcher.write_async(
        state.shard_index,
        {:put, key, "test_value", 0},
        from
      )

      # Notify the test that the write was enqueued
      send(state.test_pid, {:write_enqueued, key})

      # Return noreply — the Batcher will reply to `from` directly
      {:noreply, state}
    end
  end
end
