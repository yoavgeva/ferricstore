defmodule FerricstoreServer.Integration.ProcessCrashRecoveryTest do
  @moduledoc """
  Crash recovery tests for supervised processes.

  Each test kills exactly ONE process, waits for supervisor restart, and
  verifies data integrity. Tests are spaced with sleep to avoid exhausting
  the top-level supervisor's max_restarts budget (default: 3 in 5 seconds).
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive(10_000)
    :ok
  end

  setup do
    # Space out kills to stay within supervisor max_restarts budget
    Process.sleep(2_000)
    ShardHelpers.wait_shards_alive(10_000)
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      :persistent_term.put(:ferricstore_reject_writes, false)
      :persistent_term.put(:ferricstore_keydir_full, false)
      ShardHelpers.wait_shards_alive(10_000)
    end)
  end

  defp ukey(base), do: "pcr_#{base}_#{:rand.uniform(9_999_999)}"

  defp kill_and_wait(name, timeout \\ 5000) do
    pid = Process.whereis(name)
    assert pid != nil, "Process #{inspect(name)} not found"
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, timeout

    Enum.reduce_while(1..div(timeout, 50), :waiting, fn _, _ ->
      case Process.whereis(name) do
        pid when is_pid(pid) ->
          if Process.alive?(pid), do: {:halt, :ok}, else: {:cont, :waiting}

        nil ->
          Process.sleep(50)
          {:cont, :waiting}
      end
    end)
  end

  # ===========================================================================
  # Shard GenServer — most critical
  # ===========================================================================

  test "shard crash: data survives and new writes work" do
    idx = 0
    k = ShardHelpers.key_for_shard(idx)
    Router.put(k, "before_crash")
    ShardHelpers.flush_all_shards()

    ShardHelpers.kill_shard_safely(idx)

    assert Router.get(k) == "before_crash"

    k2 = ukey("after_shard")
    Router.put(k2, "new_write")
    assert Router.get(k2) == "new_write"
  end

  test "shard crash: other shards unaffected" do
    keys =
      for i <- 0..3 do
        k = ShardHelpers.key_for_shard(i)
        Router.put(k, "shard_#{i}")
        k
      end

    ShardHelpers.flush_all_shards()

    ShardHelpers.kill_shard_safely(0)

    for {k, i} <- Enum.with_index(keys) do
      assert Router.get(k) == "shard_#{i}"
    end
  end

  # ===========================================================================
  # Data-path processes
  # ===========================================================================

  test "Batcher crash: writes before are durable, writes after succeed" do
    k = ukey("batcher")
    Router.put(k, "durable")
    ShardHelpers.flush_all_shards()

    kill_and_wait(Ferricstore.Raft.Batcher.batcher_name(0))

    assert Router.get(k) == "durable"

    k2 = ukey("batcher_post")
    Router.put(k2, "after")
    ShardHelpers.flush_all_shards()
    assert Router.get(k2) == "after"
  end

  test "BitcaskWriter crash: data survives and writes resume" do
    k = ukey("writer")
    Router.put(k, "durable")
    ShardHelpers.flush_all_shards()

    kill_and_wait(Ferricstore.Store.BitcaskWriter.writer_name(0))

    assert Router.get(k) == "durable"

    k2 = ukey("writer_post")
    Router.put(k2, "after")
    ShardHelpers.flush_all_shards()
    assert Router.get(k2) == "after"
  end

  test "AsyncApplyWorker crash: quorum writes unaffected" do
    k = ukey("async")
    Router.put(k, "safe")
    ShardHelpers.flush_all_shards()

    kill_and_wait(Ferricstore.Raft.AsyncApplyWorker.worker_name(0))

    assert Router.get(k) == "safe"
  end

  # ===========================================================================
  # Ancillary singletons
  # ===========================================================================

  test "Stats crash: data unaffected" do
    k = ukey("stats")
    Router.put(k, "safe")
    kill_and_wait(Ferricstore.Stats)
    assert Router.get(k) == "safe"
  end

  test "SlowLog crash: data unaffected" do
    k = ukey("slowlog")
    Router.put(k, "safe")
    kill_and_wait(Ferricstore.SlowLog)
    assert Router.get(k) == "safe"
  end

  test "PubSub crash: data unaffected" do
    k = ukey("pubsub")
    Router.put(k, "safe")
    kill_and_wait(Ferricstore.PubSub)
    assert Router.get(k) == "safe"
  end

  test "Config crash: data unaffected" do
    k = ukey("config")
    Router.put(k, "safe")
    kill_and_wait(Ferricstore.Config)
    assert Router.get(k) == "safe"
  end

  test "MemoryGuard crash: data unaffected" do
    k = ukey("memguard")
    Router.put(k, "safe")
    kill_and_wait(Ferricstore.MemoryGuard)
    assert Router.get(k) == "safe"
  end
end
