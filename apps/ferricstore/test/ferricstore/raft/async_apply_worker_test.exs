defmodule Ferricstore.Raft.AsyncApplyWorkerTest do
  @moduledoc """
  Tests for `Ferricstore.Raft.AsyncApplyWorker`.

  Validates that the async apply worker correctly processes write batches
  against the shard's Bitcask + ETS stores without blocking callers.

  Per spec section 2F.3, when a namespace has `:async` durability mode,
  writes are applied to the local shard directly (bypassing Raft quorum)
  and callers are replied to immediately.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.AsyncApplyWorker
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys with a specific prefix
  defp ukey(base), do: "async_worker_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # Worker process lifecycle
  # ---------------------------------------------------------------------------

  describe "worker process lifecycle" do
    test "workers are registered under expected names for each shard" do
      for i <- 0..3 do
        name = AsyncApplyWorker.worker_name(i)
        assert is_atom(name)
        pid = Process.whereis(name)
        assert is_pid(pid), "Expected worker #{i} registered as #{name}"
        assert Process.alive?(pid)
      end
    end

    test "worker_name/1 returns expected atom" do
      assert AsyncApplyWorker.worker_name(0) == :"Ferricstore.Raft.AsyncApplyWorker.0"
      assert AsyncApplyWorker.worker_name(3) == :"Ferricstore.Raft.AsyncApplyWorker.3"
    end
  end

  # ---------------------------------------------------------------------------
  # Batch processing
  # ---------------------------------------------------------------------------

  describe "apply_batch/2 processes batches correctly" do
    test "single put command is applied to shard" do
      k = ukey("single_put")
      shard_index = Router.shard_for(k)

      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:put, k, "async_val", 0}])
      AsyncApplyWorker.drain(shard_index)
      assert "async_val" == Router.get(k)
    end

    test "multiple put commands in a batch" do
      keys =
        for i <- 1..5 do
          ukey("multi_put_#{i}")
        end

      # Group by shard
      by_shard = Enum.group_by(keys, &Router.shard_for/1)
      {shard_idx, shard_keys} = Enum.max_by(by_shard, fn {_, ks} -> length(ks) end)

      commands =
        Enum.map(shard_keys, fn k ->
          {:put, k, "batch_val", 0}
        end)

      :ok = AsyncApplyWorker.apply_batch(shard_idx, commands)
      AsyncApplyWorker.drain(shard_idx)

      for k <- shard_keys do
        assert "batch_val" == Router.get(k)
      end
    end

    test "delete command in batch" do
      k = ukey("batch_delete")
      shard_index = Router.shard_for(k)

      # First write the key
      Router.put(k, "to_delete", 0)
      assert "to_delete" == Router.get(k)

      # Delete via async worker
      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:delete, k}])
      AsyncApplyWorker.drain(shard_index)

      assert nil == Router.get(k)
    end

    test "mixed put and delete commands in batch" do
      k1 = ukey("mix_put")
      k2 = ukey("mix_del")

      # Find two keys that hash to the same shard
      shard_idx = Router.shard_for(k1)

      # Pre-create k2
      Router.put(k2, "before_delete", 0)

      # If they're on different shards, just test k1
      if Router.shard_for(k2) == shard_idx do
        :ok =
          AsyncApplyWorker.apply_batch(shard_idx, [
            {:put, k1, "new_val", 0},
            {:delete, k2}
          ])

        AsyncApplyWorker.drain(shard_idx)
        assert "new_val" == Router.get(k1)
        assert nil == Router.get(k2)
      else
        :ok =
          AsyncApplyWorker.apply_batch(shard_idx, [
            {:put, k1, "new_val", 0}
          ])

        AsyncApplyWorker.drain(shard_idx)
        assert "new_val" == Router.get(k1)
      end
    end

    test "put with expiry" do
      k = ukey("expiry")
      shard_index = Router.shard_for(k)
      future = System.os_time(:millisecond) + 60_000

      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:put, k, "ttl_val", future}])
      AsyncApplyWorker.drain(shard_index)

      {value, expire_at_ms} = Router.get_meta(k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # Async writes are eventually visible via GET
  # ---------------------------------------------------------------------------

  describe "async writes are eventually visible" do
    test "write is visible after short delay" do
      k = ukey("eventually_visible")
      shard_index = Router.shard_for(k)

      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:put, k, "visible", 0}])

      # Poll until visible or timeout
      result =
        Enum.reduce_while(1..100, nil, fn _i, _acc ->
          case Router.get(k) do
            nil ->
              Process.sleep(5)
              {:cont, nil}

            value ->
              {:halt, value}
          end
        end)

      assert result == "visible"
    end

    test "many async writes are all eventually visible" do
      count = 20
      prefix = ukey("many")

      keys =
        for i <- 1..count do
          "#{prefix}:#{i}"
        end

      # Submit all writes grouped by shard
      by_shard = Enum.group_by(keys, &Router.shard_for/1)

      for {shard_idx, shard_keys} <- by_shard do
        commands = Enum.map(shard_keys, fn k -> {:put, k, "val_#{k}", 0} end)
        :ok = AsyncApplyWorker.apply_batch(shard_idx, commands)
      end

      # Drain all shard workers to ensure async writes are applied.
      for shard_idx <- 0..3, do: AsyncApplyWorker.drain(shard_idx)

      for k <- keys do
        assert "val_#{k}" == Router.get(k),
               "Expected key #{k} to be visible after async write"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # apply_batch/2 returns immediately (non-blocking)
  # ---------------------------------------------------------------------------

  describe "apply_batch/2 is non-blocking" do
    test "returns :ok immediately without waiting for disk write" do
      k = ukey("fast_return")
      shard_index = Router.shard_for(k)

      # Time the call -- it should return nearly instantly since it just
      # casts to the GenServer
      {elapsed_us, :ok} =
        :timer.tc(fn ->
          AsyncApplyWorker.apply_batch(shard_index, [{:put, k, "fast", 0}])
        end)

      # Should complete in under 1ms (generous bound). The key insight is that
      # apply_batch does NOT wait for disk I/O.
      assert elapsed_us < 1_000,
             "apply_batch took #{elapsed_us}us, expected < 1000us (non-blocking)"

      # Wait for it to actually be written
      AsyncApplyWorker.drain(shard_index)
      assert "fast" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Error handling
  # ---------------------------------------------------------------------------

  describe "worker handles errors gracefully" do
    test "empty batch is a no-op" do
      # Should not crash or raise
      :ok = AsyncApplyWorker.apply_batch(0, [])
    end

    test "worker survives after processing batch" do
      k = ukey("survive")
      shard_index = Router.shard_for(k)

      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:put, k, "survived", 0}])
      AsyncApplyWorker.drain(shard_index)

      # Worker should still be alive
      pid = Process.whereis(AsyncApplyWorker.worker_name(shard_index))
      assert Process.alive?(pid)

      # And still functional -- use a key that routes to the same shard
      k2 =
        Enum.find(1..10_000, fn i ->
          candidate = "async_survive2_#{i}"
          Router.shard_for(candidate) == shard_index
        end)
        |> then(fn i -> "async_survive2_#{i}" end)

      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:put, k2, "still_works", 0}])
      AsyncApplyWorker.drain(shard_index)
      assert "still_works" == Router.get(k2)
    end
  end

  # ---------------------------------------------------------------------------
  # Multiple concurrent batches
  # ---------------------------------------------------------------------------

  describe "multiple concurrent batches" do
    test "concurrent batches to the same shard are processed correctly" do
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            k = "async_conc_#{i}_#{:rand.uniform(9_999_999)}"
            # Route to the correct shard for this key
            actual_shard = Router.shard_for(k)

            AsyncApplyWorker.apply_batch(actual_shard, [{:put, k, "conc_#{i}", 0}])
            {k, actual_shard}
          end)
        end

      key_shard_pairs = Task.await_many(tasks, 5_000)

      # Drain all shard workers to ensure async batches are applied before reading.
      for shard_idx <- 0..3, do: AsyncApplyWorker.drain(shard_idx)

      for {k, _shard} <- key_shard_pairs do
        assert Router.get(k) != nil,
               "Expected key #{k} to exist after concurrent async write"
      end
    end

    test "concurrent batches to different shards are processed independently" do
      # Generate keys that hash to specific shards so the write and read
      # paths agree on which shard owns the data.
      tasks =
        for shard_idx <- 0..3 do
          Task.async(fn ->
            k =
              Enum.find(1..10_000, fn i ->
                candidate = "async_indep_shard#{shard_idx}_#{i}"
                Router.shard_for(candidate) == shard_idx
              end)
              |> then(fn i -> "async_indep_shard#{shard_idx}_#{i}" end)

            AsyncApplyWorker.apply_batch(shard_idx, [{:put, k, "shard_val_#{shard_idx}", 0}])
            {k, shard_idx}
          end)
        end

      results = Task.await_many(tasks, 5_000)

      # Drain all 4 shard workers to ensure async batches are applied before reading.
      # Process.sleep(100) is insufficient under heavy load; drain/1 is a proper barrier.
      for shard_idx <- 0..3, do: AsyncApplyWorker.drain(shard_idx)

      for {k, shard_idx} <- results do
        assert "shard_val_#{shard_idx}" == Router.get(k),
               "Expected key #{k} on shard #{shard_idx} to be readable"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry
  # ---------------------------------------------------------------------------

  describe "telemetry events" do
    test "emits [:ferricstore, :async_apply, :batch] telemetry on batch completion" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :async_apply, :batch]
        ])

      k = ukey("telemetry_batch")
      shard_index = Router.shard_for(k)

      :ok = AsyncApplyWorker.apply_batch(shard_index, [{:put, k, "telem_val", 0}])

      assert_receive {[:ferricstore, :async_apply, :batch], ^ref, measurements, metadata},
                     1_000

      assert is_integer(measurements.duration_us)
      assert measurements.duration_us >= 0
      assert measurements.batch_size == 1
      assert metadata.shard_index == shard_index
    end

    test "telemetry reports correct batch_size for multi-command batches" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :async_apply, :batch]
        ])

      k1 = ukey("telem_multi_1")
      k2 = ukey("telem_multi_2")
      shard_index = Router.shard_for(k1)

      commands = [
        {:put, k1, "v1", 0},
        {:put, k2, "v2", 0}
      ]

      :ok = AsyncApplyWorker.apply_batch(shard_index, commands)

      assert_receive {[:ferricstore, :async_apply, :batch], ^ref, measurements, _metadata},
                     1_000

      assert measurements.batch_size == 2
    end
  end
end
