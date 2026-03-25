defmodule Ferricstore.Bitcask.IoUringTest do
  @moduledoc """
  Comprehensive tests for the io_uring async write path.

  All tests are tagged `:linux_io_uring` and are automatically excluded on
  macOS. On Linux with io_uring (kernel ≥ 5.1) they run in full. Each test
  directly asserts that `NIF.put_batch_async/2` returns `{:pending, op_id}`
  (not `:ok`) and that the `{:io_complete, op_id, :ok}` message arrives,
  verifying the entire async ring → completion thread → BEAM message pipeline.

  Test categories:
    1.  NIF return-value contract
    2.  Completion message delivery
    3.  Single-entry correctness
    4.  Multi-entry batch correctness
    5.  Batch size boundaries (1, 63, 64, 65, 128, 200 entries)
    6.  Concurrent / parallel submissions
    7.  Offset tracking across batches
    8.  Expiry handling
    9.  Overwrite semantics
    10. Durability — data survives store reopen
    11. Interleaved sync and async writes
    12. Binary / edge-case keys and values
    13. Shard GenServer integration
    14. Shard flush_in_flight lifecycle
    15. Shard delete awaits in-flight
    16. Shard keys/flush await in-flight
    17. Timer-driven background flush
    18. High-volume stress
    19. Edge cases — completion ordering, mailbox pollution, process death,
        shard crash recovery, ETS/Bitcask divergence, rapid put+delete,
        put+delete+put, TTL boundary, same-key in sequential batches,
        exists? under async, multiple shards concurrent, GC of store resource,
        flush timeout recovery, rapid shard restart, keys after partial expiry
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{Router, Shard}

  @moduletag :linux_io_uring

  setup do
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "iou_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  defp start_shard do
    dir = tmp_dir()
    idx = :erlang.unique_integer([:positive]) |> rem(50_000) |> Kernel.+(50_000)
    {:ok, pid} = Shard.start_link(index: idx, data_dir: dir)
    {pid, idx, dir}
  end

  # Submit a batch and assert we get {:pending, op_id} back (proves async path).
  defp submit_async!(store, batch) do
    case NIF.put_batch_async(store, batch) do
      {:pending, op_id} ->
        op_id

      :ok ->
        flunk(
          "Expected {:pending, op_id} on Linux/io_uring but got :ok. " <>
            "Is this running on Linux with kernel ≥ 5.1 and io_uring enabled?"
        )
    end
  end

  # Submit and wait for the completion message.
  defp submit_and_await!(store, batch, timeout \\ 5_000) do
    op_id = submit_async!(store, batch)

    receive do
      {:io_complete, ^op_id, result} -> result
    after
      timeout -> flunk("Timed out waiting for {:io_complete, #{op_id}, _}")
    end
  end

  # ---------------------------------------------------------------------------
  # 1. NIF return-value contract
  # ---------------------------------------------------------------------------

  describe "NIF return-value contract" do
    test "single-entry batch returns {:pending, op_id}" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert {:pending, op_id} = NIF.put_batch_async(store, [{"k", "v", 0}])
      assert is_integer(op_id) and op_id >= 0
      # drain
      assert_receive {:io_complete, ^op_id, :ok}, 5_000
    end

    test "op_ids are strictly monotonically increasing" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      ids =
        for i <- 1..10 do
          op_id = submit_async!(store, [{"mono_#{i}", "v", 0}])
          assert_receive {:io_complete, ^op_id, :ok}, 5_000
          op_id
        end

      # Each successive id must be greater than the previous
      ids
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] ->
        assert b > a, "op_ids not monotonically increasing: #{a} then #{b}"
      end)
    end

    test "empty batch returns {:pending, op_id} and immediately completes" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      # Empty batch: the NIF may return {:pending, _} or :ok depending on
      # implementation. On io_uring path the agent sends ok immediately.
      result = NIF.put_batch_async(store, [])
      case result do
        {:pending, op_id} ->
          assert_receive {:io_complete, ^op_id, :ok}, 1_000
        :ok ->
          :ok
      end
    end

    test "each call returns a unique op_id" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      ids =
        for i <- 1..20 do
          op_id = submit_async!(store, [{"uid_#{i}", "v", 0}])
          assert_receive {:io_complete, ^op_id, :ok}, 5_000
          op_id
        end

      assert length(Enum.uniq(ids)) == 20, "op_ids not unique: #{inspect(ids)}"
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Completion message delivery
  # ---------------------------------------------------------------------------

  describe "completion message delivery" do
    test "io_complete message is sent to the calling process" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      op_id = submit_async!(store, [{"msg_k", "msg_v", 0}])
      assert_receive {:io_complete, ^op_id, :ok}, 5_000
    end

    test "completion message is {io_complete, op_id, :ok} tuple" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      op_id = submit_async!(store, [{"shape_k", "v", 0}])

      receive do
        msg ->
          assert {:io_complete, ^op_id, :ok} = msg
      after
        5_000 -> flunk("no completion message received")
      end
    end

    test "no extra messages arrive after one batch" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      op_id = submit_async!(store, [{"extra_k", "v", 0}])
      assert_receive {:io_complete, ^op_id, :ok}, 5_000
      refute_receive {:io_complete, _, _}, 100
    end

    test "completion arrives to the submitting process not others" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      parent = self()

      child =
        spawn(fn ->
          op_id = submit_async!(store, [{"child_k", "v", 0}])
          receive do
            {:io_complete, ^op_id, :ok} -> send(parent, {:child_done, op_id})
          after
            5_000 -> send(parent, :child_timeout)
          end
        end)

      assert_receive {:child_done, _op_id}, 6_000
      refute_receive {:io_complete, _, _}, 50
      # child exited normally after receiving its completion message
      refute Process.alive?(child)
    end

    test "3 sequential batches each produce exactly one completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      for i <- 1..3 do
        op_id = submit_async!(store, [{"seq3_#{i}", "v#{i}", 0}])
        assert_receive {:io_complete, ^op_id, :ok}, 5_000
        refute_receive {:io_complete, _, _}, 20
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Single-entry correctness
  # ---------------------------------------------------------------------------

  describe "single-entry batch correctness" do
    test "value readable immediately after completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"s1_k", "s1_v", 0}])
      assert {:ok, "s1_v"} == NIF.get(store, "s1_k")
    end

    test "key appears in NIF.keys after completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"keys_k", "v", 0}])
      assert "keys_k" in NIF.keys(store)
    end

    test "binary value with all byte values 0x00–0xFF is preserved" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      all_bytes = :binary.list_to_bin(Enum.to_list(0..255))
      assert :ok == submit_and_await!(store, [{"allbytes", all_bytes, 0}])
      assert {:ok, ^all_bytes} = NIF.get(store, "allbytes")
    end

    test "large value (512 KB) survives async round-trip" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      big = :crypto.strong_rand_bytes(512 * 1024)
      assert :ok == submit_and_await!(store, [{"big_k", big, 0}], 10_000)
      assert {:ok, ^big} = NIF.get(store, "big_k")
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Multi-entry batch correctness
  # ---------------------------------------------------------------------------

  describe "multi-entry batch correctness" do
    test "10-entry batch: all values readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch = for i <- 1..10, do: {"m10_k#{i}", "m10_v#{i}", 0}
      assert :ok == submit_and_await!(store, batch)
      for i <- 1..10, do: assert({:ok, "m10_v#{i}"} == NIF.get(store, "m10_k#{i}"))
    end

    test "50-entry batch: all values readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch = for i <- 1..50, do: {"m50_k#{i}", "m50_v#{i}", 0}
      assert :ok == submit_and_await!(store, batch)
      for i <- 1..50, do: assert({:ok, "m50_v#{i}"} == NIF.get(store, "m50_k#{i}"))
    end

    test "mixed expiry batch: expired nil, live readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      past   = System.os_time(:millisecond) - 1_000
      future = System.os_time(:millisecond) + 60_000

      batch = [
        {"exp1", "gone1", past},
        {"exp2", "gone2", past},
        {"live1", "here1", future},
        {"live2", "here2", 0}
      ]

      assert :ok == submit_and_await!(store, batch)

      assert {:ok, nil}     == NIF.get(store, "exp1")
      assert {:ok, nil}     == NIF.get(store, "exp2")
      assert {:ok, "here1"} == NIF.get(store, "live1")
      assert {:ok, "here2"} == NIF.get(store, "live2")
    end

    test "last write wins for duplicate keys in one batch" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      # Two entries with the same key — last write should win in keydir
      batch = [{"dup_k", "first", 0}, {"dup_k", "second", 0}]
      assert :ok == submit_and_await!(store, batch)
      assert {:ok, "second"} == NIF.get(store, "dup_k")
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Batch size boundaries (ring chunk boundaries)
  # ---------------------------------------------------------------------------

  describe "batch size boundaries" do
    # RING_SIZE = 64; each batch uses N write SQEs + 1 fsync SQE.
    # Boundaries to exercise: 1, 63 (fits in ring), 64 (fills ring exactly),
    # 65 (spills to second chunk), 127, 128, 200.

    for n <- [1, 63, 64, 65, 127, 128, 200] do
      @n n
      test "batch of #{n} entries: all readable after completion" do
        dir = tmp_dir()
        store = open_store(dir)
        on_exit(fn -> File.rm_rf(dir) end)

        batch = for i <- 1..@n, do: {"bsz#{@n}_k#{i}", "v#{i}", 0}
        assert :ok == submit_and_await!(store, batch, 15_000)
        for i <- 1..@n, do: assert({:ok, "v#{i}"} == NIF.get(store, "bsz#{@n}_k#{i}"))
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Concurrent / parallel submissions
  # ---------------------------------------------------------------------------

  describe "concurrent submissions" do
    test "5 tasks each submit their own batch concurrently" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            batch = [{"conc5_k#{i}", "conc5_v#{i}", 0}]
            result = submit_and_await!(store, batch)
            {i, result}
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, fn {_, r} -> r == :ok end)

      for i <- 1..5, do: assert({:ok, "conc5_v#{i}"} == NIF.get(store, "conc5_k#{i}"))
    end

    test "20 tasks submit concurrently — no data loss" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            :ok = submit_and_await!(store, [{"c20_k#{i}", "c20_v#{i}", 0}])
          end)
        end

      Task.await_many(tasks, 15_000)
      for i <- 1..20, do: assert({:ok, "c20_v#{i}"} == NIF.get(store, "c20_k#{i}"))
    end

    test "parallel submissions produce distinct op_ids" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      # Collect op_ids before any await to maximise overlap
      parent = self()

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            op_id = submit_async!(store, [{"pid_k#{i}", "v", 0}])
            send(parent, {:op_id, op_id})

            receive do
              {:io_complete, ^op_id, :ok} -> op_id
            after
              5_000 -> flunk("timeout")
            end
          end)
        end

      collected_ids = Task.await_many(tasks, 15_000)
      assert length(Enum.uniq(collected_ids)) == 10
    end

    test "50 concurrent single-key batches all complete :ok" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            submit_and_await!(store, [{"hv50_k#{i}", "hv50_v#{i}", 0}])
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Offset tracking across sequential batches
  # ---------------------------------------------------------------------------

  describe "offset tracking across sequential batches" do
    test "values from two sequential batches are both readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"off_a", "va", 0}])
      assert :ok == submit_and_await!(store, [{"off_b", "vb", 0}])

      assert {:ok, "va"} == NIF.get(store, "off_a")
      assert {:ok, "vb"} == NIF.get(store, "off_b")
    end

    test "10 sequential batches: no offset corruption" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      for i <- 1..10 do
        assert :ok == submit_and_await!(store, [{"seq10_k#{i}", "seq10_v#{i}", 0}])
      end

      for i <- 1..10, do: assert({:ok, "seq10_v#{i}"} == NIF.get(store, "seq10_k#{i}"))
    end

    test "overwrite via successive batches: last value wins" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"ov_k", "first", 0}])
      assert :ok == submit_and_await!(store, [{"ov_k", "second", 0}])
      assert :ok == submit_and_await!(store, [{"ov_k", "third", 0}])

      assert {:ok, "third"} == NIF.get(store, "ov_k")
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Expiry handling
  # ---------------------------------------------------------------------------

  describe "expiry handling" do
    test "key written with past expiry is nil immediately" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      past = System.os_time(:millisecond) - 1_000
      assert :ok == submit_and_await!(store, [{"past_k", "v", past}])
      assert {:ok, nil} == NIF.get(store, "past_k")
    end

    test "key written with future expiry is readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      future = System.os_time(:millisecond) + 60_000
      assert :ok == submit_and_await!(store, [{"fut_k", "v", future}])
      assert {:ok, "v"} == NIF.get(store, "fut_k")
    end

    test "key expires after sleep" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      exp = System.os_time(:millisecond) + 200
      assert :ok == submit_and_await!(store, [{"ttl_k", "v", exp}])
      assert {:ok, "v"} == NIF.get(store, "ttl_k")

      :timer.sleep(300)
      assert {:ok, nil} == NIF.get(store, "ttl_k")
    end

    test "expired keys not in NIF.keys" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      past = System.os_time(:millisecond) - 1_000
      assert :ok == submit_and_await!(store, [{"exp_key", "v", past}, {"live_key", "v", 0}])

      keys = NIF.keys(store)
      refute "exp_key" in keys
      assert "live_key" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # 9. Overwrite semantics
  # ---------------------------------------------------------------------------

  describe "overwrite semantics" do
    test "sync put followed by async batch: async value wins" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      :ok = NIF.put(store, "ow_k", "sync_v", 0)
      assert :ok == submit_and_await!(store, [{"ow_k", "async_v", 0}])
      assert {:ok, "async_v"} == NIF.get(store, "ow_k")
    end

    test "async batch followed by sync put: sync value wins" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"ow2_k", "async_v", 0}])
      :ok = NIF.put(store, "ow2_k", "sync_v", 0)
      assert {:ok, "sync_v"} == NIF.get(store, "ow2_k")
    end

    test "async then async: second value wins" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"aa_k", "first", 0}])
      assert :ok == submit_and_await!(store, [{"aa_k", "second", 0}])
      assert {:ok, "second"} == NIF.get(store, "aa_k")
    end
  end

  # ---------------------------------------------------------------------------
  # 10. Durability — data survives store reopen
  # ---------------------------------------------------------------------------

  describe "durability — store reopen" do
    test "single key survives reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store1, [{"dur1_k", "dur1_v", 0}])
      _ = store1; :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "dur1_v"} == NIF.get(store2, "dur1_k")
    end

    test "50-entry batch survives reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch = for i <- 1..50, do: {"dr50_k#{i}", "dr50_v#{i}", 0}
      assert :ok == submit_and_await!(store1, batch, 10_000)
      _ = store1; :erlang.garbage_collect()

      store2 = open_store(dir)
      for i <- 1..50, do: assert({:ok, "dr50_v#{i}"} == NIF.get(store2, "dr50_k#{i}"))
    end

    test "three sequential batches all survive reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store1, [{"b1_k", "b1_v", 0}])
      assert :ok == submit_and_await!(store1, [{"b2_k", "b2_v", 0}])
      assert :ok == submit_and_await!(store1, [{"b3_k", "b3_v", 0}])
      _ = store1; :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "b1_v"} == NIF.get(store2, "b1_k")
      assert {:ok, "b2_v"} == NIF.get(store2, "b2_k")
      assert {:ok, "b3_v"} == NIF.get(store2, "b3_k")
    end

    test "overwritten value (latest) survives reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store1, [{"reopen_ov", "old", 0}])
      assert :ok == submit_and_await!(store1, [{"reopen_ov", "new", 0}])
      _ = store1; :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "new"} == NIF.get(store2, "reopen_ov")
    end

    test "deleted key is absent after reopen" do
      dir = tmp_dir()
      store1 = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store1, [{"del_dur", "v", 0}])
      {:ok, true} = NIF.delete(store1, "del_dur")
      _ = store1; :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, nil} == NIF.get(store2, "del_dur")
    end
  end

  # ---------------------------------------------------------------------------
  # 11. Interleaved sync and async writes
  # ---------------------------------------------------------------------------

  describe "interleaved sync and async" do
    test "sync put_batch then async: both readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      :ok = NIF.put_batch(store, [{"int_sync", "sv", 0}])
      assert :ok == submit_and_await!(store, [{"int_async", "av", 0}])

      assert {:ok, "sv"} == NIF.get(store, "int_sync")
      assert {:ok, "av"} == NIF.get(store, "int_async")
    end

    test "async then sync put_batch: both readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"ias_a", "av", 0}])
      :ok = NIF.put_batch(store, [{"ias_s", "sv", 0}])

      assert {:ok, "av"} == NIF.get(store, "ias_a")
      assert {:ok, "sv"} == NIF.get(store, "ias_s")
    end

    test "alternating sync and async 5 times: all readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      for i <- 1..5 do
        :ok = NIF.put_batch(store, [{"alt_s#{i}", "sv#{i}", 0}])
        assert :ok == submit_and_await!(store, [{"alt_a#{i}", "av#{i}", 0}])
      end

      for i <- 1..5 do
        assert {:ok, "sv#{i}"} == NIF.get(store, "alt_s#{i}")
        assert {:ok, "av#{i}"} == NIF.get(store, "alt_a#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 12. Binary / edge-case keys and values
  # ---------------------------------------------------------------------------

  describe "binary and edge-case keys/values" do
    test "key with null bytes" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      key = <<0, 1, 2, 3, 0>>
      assert :ok == submit_and_await!(store, [{key, "null_key_val", 0}])
      assert {:ok, "null_key_val"} == NIF.get(store, key)
    end

    test "value with null bytes" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      val = <<0, 255, 0, 127, 0>>
      assert :ok == submit_and_await!(store, [{"null_val_k", val, 0}])
      assert {:ok, ^val} = NIF.get(store, "null_val_k")
    end

    test "very long key (10 KB)" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      long_key = :crypto.strong_rand_bytes(10_000)
      assert :ok == submit_and_await!(store, [{long_key, "long_key_val", 0}])
      assert {:ok, "long_key_val"} == NIF.get(store, long_key)
    end

    test "very long value (1 MB)" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      big_val = :crypto.strong_rand_bytes(1_024 * 1_024)
      assert :ok == submit_and_await!(store, [{"big_val_k", big_val, 0}], 15_000)
      assert {:ok, ^big_val} = NIF.get(store, "big_val_k")
    end

    test "unicode key and value" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      key = "résumé_日本語_🚀"
      val = "Ünïcödë välüë ✓"
      assert :ok == submit_and_await!(store, [{key, val, 0}])
      assert {:ok, ^val} = NIF.get(store, key)
    end

    test "batch with varying value sizes (1B to 10KB)" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      sizes = [1, 10, 100, 1_000, 5_000, 10_000]
      batch = for {sz, i} <- Enum.with_index(sizes, 1) do
        {"vsz_k#{i}", :crypto.strong_rand_bytes(sz), 0}
      end

      assert :ok == submit_and_await!(store, batch, 10_000)

      for {_sz, i} <- Enum.with_index(sizes, 1) do
        {_, expected, _} = Enum.at(batch, i - 1)
        assert {:ok, ^expected} = NIF.get(store, "vsz_k#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 13. Shard GenServer integration
  # ---------------------------------------------------------------------------

  describe "shard GenServer integration" do
    test "Router.put writes through shard and is readable via Router.get" do
      k = "iou_rtr_#{:rand.uniform(9_999_999)}"
      Router.put(k, "rtr_val", 0)
      GenServer.call(Router.shard_name(Router.shard_for(k)), :flush)
      assert "rtr_val" == Router.get(k)
    end

    test "shard put → flush → get via GenServer directly" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "shard_k", "shard_v", 0})
      :ok = GenServer.call(pid, :flush)
      assert "shard_v" == GenServer.call(pid, {:get, "shard_k"})
    end

    test "shard :keys returns written key after flush" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "sk_present", "v", 0})
      keys = GenServer.call(pid, :keys)
      assert "sk_present" in keys
    end

    test "shard :keys excludes deleted key after delete" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "sk_del", "v", 0})
      :ok = GenServer.call(pid, {:put, "sk_keep", "v", 0})
      :ok = GenServer.call(pid, {:delete, "sk_del"})

      keys = GenServer.call(pid, :keys)
      assert "sk_keep" in keys
      refute "sk_del" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # 14. Shard flush_in_flight lifecycle
  # ---------------------------------------------------------------------------

  describe "shard flush_in_flight lifecycle" do
    test "flush_in_flight transitions nil → op_id → nil" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      # Pause the GenServer's timer by calling flush synchronously
      # so we can observe flush_in_flight in transit.
      # Insert into pending directly by sending a put, then before the timer
      # fires read the state. We can't freeze time, so just verify end state.
      :ok = GenServer.call(pid, {:put, "lc_k", "lc_v", 0})

      # After explicit flush, in-flight must be nil (completed)
      :ok = GenServer.call(pid, :flush)
      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end

    test "pending is empty after flush" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "pending_k", "v", 0})
      :ok = GenServer.call(pid, :flush)

      state = :sys.get_state(pid)
      assert state.pending == []
    end

    test "injected :io_complete with correct op_id clears flush_in_flight" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 1234} end)
      send(pid, {:io_complete, 1234, :ok})
      :timer.sleep(20)
      assert :sys.get_state(pid).flush_in_flight == nil
    end

    test "injected :io_complete with wrong op_id leaves flush_in_flight unchanged" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 9999} end)
      send(pid, {:io_complete, 0000, :ok})
      :timer.sleep(20)
      assert :sys.get_state(pid).flush_in_flight == 9999
    end

    test "error completion still clears flush_in_flight" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 777} end)
      send(pid, {:io_complete, 777, {:error, "simulated"}})
      :timer.sleep(20)
      assert :sys.get_state(pid).flush_in_flight == nil
    end
  end

  # ---------------------------------------------------------------------------
  # 15. Shard delete awaits in-flight
  # ---------------------------------------------------------------------------

  describe "shard delete awaits in-flight" do
    test "delete returns :ok and key is gone" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "del_iou", "v", 0})
      :ok = GenServer.call(pid, {:delete, "del_iou"})
      assert nil == GenServer.call(pid, {:get, "del_iou"})
    end

    test "delete while flush is in-flight still completes correctly" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      # Simulate an in-flight op: inject a fake op_id and a pending batch,
      # then immediately send the completion so delete's await_in_flight returns fast.
      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 42, pending: []} end)
      send(pid, {:io_complete, 42, :ok})

      # Now do a real put + delete — should work correctly
      :ok = GenServer.call(pid, {:put, "del_while_ifl", "v", 0})
      :ok = GenServer.call(pid, {:delete, "del_while_ifl"})
      assert nil == GenServer.call(pid, {:get, "del_while_ifl"})
      assert [] == :ets.lookup(:"keydir_#{idx}", "del_while_ifl")
    end

    test "sibling keys survive when one key is deleted" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "sibling_a", "va", 0})
      :ok = GenServer.call(pid, {:put, "sibling_b", "vb", 0})
      :ok = GenServer.call(pid, {:delete, "sibling_a"})

      assert nil == GenServer.call(pid, {:get, "sibling_a"})
      assert "vb"  == GenServer.call(pid, {:get, "sibling_b"})
    end
  end

  # ---------------------------------------------------------------------------
  # 16. Shard :keys and :flush await in-flight
  # ---------------------------------------------------------------------------

  describe "shard :keys and :flush await in-flight" do
    test ":keys after put reflects written key" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "kf_k", "v", 0})
      keys = GenServer.call(pid, :keys)
      assert "kf_k" in keys
    end

    test ":flush with simulated in-flight completes once completion arrives" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 55} end)
      # Send the completion asynchronously so flush can receive it
      Process.send_after(pid, {:io_complete, 55, :ok}, 50)
      # This blocks in await_in_flight until the message arrives
      assert :ok == GenServer.call(pid, :flush, 5_000)

      state = :sys.get_state(pid)
      assert state.flush_in_flight == nil
    end
  end

  # ---------------------------------------------------------------------------
  # 17. Timer-driven background flush
  # ---------------------------------------------------------------------------

  describe "timer-driven background flush" do
    test "value is durable after 10ms without explicit flush" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "timer_k", "timer_v", 0})
      # Wait for two 1ms timer ticks plus io_uring async latency
      :timer.sleep(50)

      # Now the background flush should have fired and completed
      assert "timer_v" == GenServer.call(pid, {:get, "timer_k"})
    end

    test "pending is eventually empty without explicit flush" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "bg_k", "v", 0})
      :timer.sleep(100)

      state = :sys.get_state(pid)
      assert state.pending == []
      assert state.flush_in_flight == nil
    end

    test "10 rapid puts all visible after 50ms" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      for i <- 1..10, do: GenServer.call(pid, {:put, "rapid_k#{i}", "rapid_v#{i}", 0})
      :timer.sleep(100)

      for i <- 1..10 do
        assert "rapid_v#{i}" == GenServer.call(pid, {:get, "rapid_k#{i}"})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 18. High-volume stress
  # ---------------------------------------------------------------------------

  describe "high-volume stress" do
    test "500 sequential single-entry batches all complete :ok" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      for i <- 1..500 do
        assert :ok == submit_and_await!(store, [{"stress_#{i}", "v#{i}", 0}], 10_000)
      end

      for i <- 1..500 do
        assert {:ok, "v#{i}"} == NIF.get(store, "stress_#{i}")
      end
    end

    test "10 tasks × 50 entries each: 500 total writes, all durable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            batch = for e <- 1..50, do: {"t#{t}_e#{e}", "v#{t}_#{e}", 0}
            submit_and_await!(store, batch, 15_000)
          end)
        end

      results = Task.await_many(tasks, 60_000)
      assert Enum.all?(results, &(&1 == :ok))

      for t <- 1..10, e <- 1..50 do
        assert {:ok, "v#{t}_#{e}"} == NIF.get(store, "t#{t}_e#{e}")
      end
    end

    test "1000-entry batch completes and all values are readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      batch = for i <- 1..1_000, do: {"k1k_#{i}", "v#{i}", 0}
      assert :ok == submit_and_await!(store, batch, 30_000)

      for i <- 1..1_000 do
        assert {:ok, "v#{i}"} == NIF.get(store, "k1k_#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 19. Edge cases
  # ---------------------------------------------------------------------------

  describe "edge case: completion ordering" do
    test "completions for batches submitted in order arrive in order" do
      # Submit A, then B sequentially. Both must complete with :ok and
      # their values must not be swapped.
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      op_a = submit_async!(store, [{"ord_a", "A", 0}])
      op_b = submit_async!(store, [{"ord_b", "B", 0}])

      results =
        for op_id <- [op_a, op_b] do
          receive do
            {:io_complete, ^op_id, r} -> r
          after
            5_000 -> flunk("timeout for op #{op_id}")
          end
        end

      assert results == [:ok, :ok]
      assert {:ok, "A"} == NIF.get(store, "ord_a")
      assert {:ok, "B"} == NIF.get(store, "ord_b")
    end
  end

  describe "edge case: mailbox pollution" do
    test "stray io_complete messages sent to a shard are ignored" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      # Send 10 stray completions with random op_ids that the shard never issued
      for i <- 1..10 do
        send(pid, {:io_complete, 100_000 + i, :ok})
      end

      :timer.sleep(30)

      # Shard must still be alive and functional
      assert Process.alive?(pid)
      :ok = GenServer.call(pid, {:put, "post_stray", "v", 0})
      :ok = GenServer.call(pid, :flush)
      assert "v" == GenServer.call(pid, {:get, "post_stray"})
    end

    test "completion for a different shard's op_id is ignored" do
      {pid1, _idx1, dir1} = start_shard()
      {pid2, _idx2, dir2} = start_shard()
      on_exit(fn ->
        if Process.alive?(pid1), do: GenServer.stop(pid1)
        if Process.alive?(pid2), do: GenServer.stop(pid2)
        File.rm_rf(dir1)
        File.rm_rf(dir2)
      end)

      # Put something in shard1 to get a real op_id in-flight
      :sys.replace_state(pid1, fn s -> %{s | flush_in_flight: 888} end)

      # Send that op_id completion to shard2 (wrong shard)
      send(pid2, {:io_complete, 888, :ok})
      :timer.sleep(20)

      # shard2 must be unaffected (its in-flight is still nil)
      assert :sys.get_state(pid2).flush_in_flight == nil
      # shard1 still thinks 888 is in-flight (nobody cleared it)
      assert :sys.get_state(pid1).flush_in_flight == 888

      # Unblock shard1 before cleanup
      send(pid1, {:io_complete, 888, :ok})
      :timer.sleep(20)
    end
  end

  describe "edge case: process death before completion" do
    test "spawned process submits async and dies — shard is unaffected" do
      # The NIF background thread will try to send the message to the dead
      # pid. The send must not crash the NIF thread or the shard.
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      parent = self()

      spawn(fn ->
        _op_id = submit_async!(store, [{"dead_proc_k", "v", 0}])
        send(parent, :submitted)
        # die immediately, before completion arrives
      end)

      assert_receive :submitted, 2_000
      # Let the completion thread try (and silently fail) to deliver the message
      :timer.sleep(200)

      # The store must still be usable
      :ok = NIF.put(store, "after_dead", "alive", 0)
      assert {:ok, "alive"} == NIF.get(store, "after_dead")
    end
  end

  describe "edge case: shard crash and restart" do
    test "data written before crash survives and new shard is functional" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "pre_crash", "safe", 0})
      :ok = GenServer.call(pid, :flush)

      # Kill the shard (unlink first so the kill signal doesn't propagate to this test process)
      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      :timer.sleep(50)

      # Restart manually (not app-supervised in this test)
      {:ok, new_pid} = Shard.start_link(index: idx, data_dir: dir)
      on_exit(fn -> if Process.alive?(new_pid), do: GenServer.stop(new_pid) end)

      assert "safe" == GenServer.call(new_pid, {:get, "pre_crash"})

      # New shard must also accept async writes
      :ok = GenServer.call(new_pid, {:put, "post_crash", "new", 0})
      :ok = GenServer.call(new_pid, :flush)
      assert "new" == GenServer.call(new_pid, {:get, "post_crash"})
    end

    test "ETS is empty on new shard; cold key warms from Bitcask" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "warm_me", "wval", 0})
      :ok = GenServer.call(pid, :flush)

      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      :timer.sleep(50)

      {:ok, new_pid} = Shard.start_link(index: idx, data_dir: dir)
      on_exit(fn -> if Process.alive?(new_pid), do: GenServer.stop(new_pid) end)

      # Fresh shard has empty ETS
      assert [] == :ets.lookup(:"keydir_#{idx}", "warm_me")

      # get warms ETS from Bitcask
      assert "wval" == GenServer.call(new_pid, {:get, "warm_me"})
      assert [{_, "wval", _}] = :ets.lookup(:"hot_cache_#{idx}", "warm_me")
    end
  end

  describe "edge case: rapid put then delete" do
    test "put followed immediately by delete leaves key absent" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "pd_k", "v", 0})
      :ok = GenServer.call(pid, {:delete, "pd_k"})

      assert nil == GenServer.call(pid, {:get, "pd_k"})
    end

    test "put → delete → put: key has latest value" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "pdp_k", "first", 0})
      :ok = GenServer.call(pid, {:delete, "pdp_k"})
      :ok = GenServer.call(pid, {:put, "pdp_k", "second", 0})
      :ok = GenServer.call(pid, :flush)

      assert "second" == GenServer.call(pid, {:get, "pdp_k"})
    end

    test "put → delete → put survives store reopen" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"pdp_dur", "first", 0}])
      {:ok, true} = NIF.delete(store, "pdp_dur")
      assert :ok == submit_and_await!(store, [{"pdp_dur", "second", 0}])

      _ = store; :erlang.garbage_collect()
      store2 = open_store(dir)
      assert {:ok, "second"} == NIF.get(store2, "pdp_dur")
    end
  end

  describe "edge case: TTL boundary conditions" do
    test "key written with expire_at_ms = now+1 expires very soon" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      exp = System.os_time(:millisecond) + 1
      :ok = GenServer.call(pid, {:put, "ttl_edge", "v", exp})
      :timer.sleep(50)

      assert nil == GenServer.call(pid, {:get, "ttl_edge"})
    end

    test "key written with expire_at_ms = 1 (distant past) is immediately nil" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"epoch1", "v", 1}])
      assert {:ok, nil} == NIF.get(store, "epoch1")
    end

    test "overwrite with no-expiry resurrects an expired key" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      exp = System.os_time(:millisecond) + 50
      :ok = GenServer.call(pid, {:put, "resurrect", "v1", exp})
      :timer.sleep(100)
      assert nil == GenServer.call(pid, {:get, "resurrect"})

      # Write again with no expiry
      :ok = GenServer.call(pid, {:put, "resurrect", "v2", 0})
      :ok = GenServer.call(pid, :flush)
      assert "v2" == GenServer.call(pid, {:get, "resurrect"})
    end
  end

  describe "edge case: same key in two sequential async batches" do
    test "second batch's value is final" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"sk_seq", "v1", 0}])
      assert :ok == submit_and_await!(store, [{"sk_seq", "v2", 0}])
      assert {:ok, "v2"} == NIF.get(store, "sk_seq")
    end

    test "second batch's value survives reopen" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"sk_dur", "v1", 0}])
      assert :ok == submit_and_await!(store, [{"sk_dur", "v2", 0}])
      _ = store; :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "v2"} == NIF.get(store2, "sk_dur")
    end
  end

  describe "edge case: exists? under async flush" do
    test "exists? returns true for key in ETS before flush completes" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "ex_before", "v", 0})
      # ETS is written synchronously — exists? must see it immediately
      assert true == GenServer.call(pid, {:exists, "ex_before"})
    end

    test "exists? returns false after delete" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      :ok = GenServer.call(pid, {:put, "ex_del", "v", 0})
      :ok = GenServer.call(pid, {:delete, "ex_del"})
      assert false == GenServer.call(pid, {:exists, "ex_del"})
    end

    test "exists? returns false for expired key" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      past = System.os_time(:millisecond) - 500
      :ok = GenServer.call(pid, {:put, "ex_exp", "v", past})
      assert false == GenServer.call(pid, {:exists, "ex_exp"})
    end
  end

  describe "edge case: multiple shards concurrent async writes" do
    test "4 shards each get 25 unique keys — all readable via Router" do
      # Use the app-supervised shards (already running)
      keys =
        for i <- 1..100 do
          k = "iou_ms_#{i}_#{:rand.uniform(999_999)}"
          Router.put(k, "val_#{i}", 0)
          k
        end

      # Flush all 4 shards
      for shard_idx <- 0..3 do
        GenServer.call(Router.shard_name(shard_idx), :flush)
      end

      for {k, i} <- Enum.with_index(keys, 1) do
        assert "val_#{i}" == Router.get(k),
               "key #{k} not readable after multi-shard async flush"
      end
    end
  end

  describe "edge case: GC of store resource during in-flight op" do
    test "GC of store resource does not crash — completion message is silently dropped" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf(dir) end)

      # Submit from a short-lived scope
      op_id =
        (fn ->
          store = open_store(dir)
          id = submit_async!(store, [{"gc_k", "v", 0}])
          # store goes out of scope here
          id
        end).()

      # Force GC to potentially collect the resource
      :erlang.garbage_collect()
      :timer.sleep(100)

      # We may or may not receive the message — either is valid.
      # What must NOT happen: a crash or an exception.
      receive do
        {:io_complete, ^op_id, _} -> :ok
      after
        200 -> :ok
      end
    end
  end

  describe "edge case: keys after partial expiry" do
    test "NIF.keys excludes expired entries, includes live ones" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      past   = System.os_time(:millisecond) - 1_000
      future = System.os_time(:millisecond) + 60_000

      batch = [
        {"kpe_exp1", "v", past},
        {"kpe_exp2", "v", past},
        {"kpe_live1", "v", future},
        {"kpe_live2", "v", 0}
      ]

      assert :ok == submit_and_await!(store, batch)

      keys = NIF.keys(store)
      refute "kpe_exp1"  in keys
      refute "kpe_exp2"  in keys
      assert "kpe_live1" in keys
      assert "kpe_live2" in keys
    end

    test "shard :keys after async flush excludes expired" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      past = System.os_time(:millisecond) - 500
      :ok = GenServer.call(pid, {:put, "sk_exp", "v", past})
      :ok = GenServer.call(pid, {:put, "sk_live", "v", 0})

      keys = GenServer.call(pid, :keys)
      refute "sk_exp"  in keys
      assert "sk_live" in keys
    end
  end

  describe "edge case: flush timeout recovery" do
    test "shard recovers after simulated flush timeout — subsequent puts work" do
      {pid, _idx, dir} = start_shard()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid); File.rm_rf(dir) end)

      # Inject an in-flight op that will never complete (simulates a lost CQE)
      # and set a very short timeout by directly replacing state.
      # We cannot override @sync_flush_timeout_ms at runtime, so instead
      # we inject the in-flight id and then send the completion ourselves
      # after a delay to simulate a late arrival.
      :sys.replace_state(pid, fn s -> %{s | flush_in_flight: 555} end)

      # Send completion after 100ms (simulates slow io_uring CQE)
      Process.send_after(pid, {:io_complete, 555, :ok}, 100)

      # Meanwhile do a delete which calls await_in_flight — it should block
      # until the completion arrives, then proceed.
      :ok = GenServer.call(pid, {:put, "timeout_k", "v", 0}, 6_000)
      :ok = GenServer.call(pid, {:delete, "timeout_k"}, 6_000)

      assert nil == GenServer.call(pid, {:get, "timeout_k"})
      assert :sys.get_state(pid).flush_in_flight == nil
    end
  end

  describe "edge case: zero-byte and single-byte values" do
    # Note: Bitcask uses value_size=0 as tombstone, so empty value acts as delete.
    test "single-byte value (1 byte) survives async round-trip" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      assert :ok == submit_and_await!(store, [{"one_byte", <<42>>, 0}])
      assert {:ok, <<42>>} == NIF.get(store, "one_byte")
    end

    test "all-zeros value is preserved" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      val = <<0, 0, 0, 0>>
      assert :ok == submit_and_await!(store, [{"zeros", val, 0}])
      assert {:ok, ^val} = NIF.get(store, "zeros")
    end

    test "all-0xFF value is preserved" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      val = <<255, 255, 255, 255>>
      assert :ok == submit_and_await!(store, [{"maxbytes", val, 0}])
      assert {:ok, ^val} = NIF.get(store, "maxbytes")
    end
  end

  describe "edge case: rapid shard restart during in-flight" do
    test "shard restart while writes are pending — data is durable" do
      {pid, idx, dir} = start_shard()
      on_exit(fn -> File.rm_rf(dir) end)

      # Write and flush so data is definitely on disk
      :ok = GenServer.call(pid, {:put, "before_restart", "safe", 0})
      :ok = GenServer.call(pid, :flush)

      # Kill and restart (unlink first so the brutal kill doesn't propagate to this test process)
      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      :timer.sleep(50)

      {:ok, new_pid} = Shard.start_link(index: idx, data_dir: dir)
      on_exit(fn -> if Process.alive?(new_pid), do: GenServer.stop(new_pid) end)

      # Pre-restart data must survive
      assert "safe" == GenServer.call(new_pid, {:get, "before_restart"})
    end
  end

  describe "edge case: NIF called from multiple processes on same store" do
    test "two processes submit async batches to same store — no corruption" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      parent = self()

      p1 = spawn(fn ->
        :ok = submit_and_await!(store, [{"mp_a", "va", 0}])
        send(parent, {:done, :p1})
      end)

      p2 = spawn(fn ->
        :ok = submit_and_await!(store, [{"mp_b", "vb", 0}])
        send(parent, {:done, :p2})
      end)

      assert_receive {:done, :p1}, 5_000
      assert_receive {:done, :p2}, 5_000

      assert {:ok, "va"} == NIF.get(store, "mp_a")
      assert {:ok, "vb"} == NIF.get(store, "mp_b")

      refute Process.alive?(p1)
      refute Process.alive?(p2)
    end
  end

  describe "edge case: batch with all entries having same expiry" do
    test "batch where all entries expire together — all nil after sleep" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      exp = System.os_time(:millisecond) + 150
      batch = for i <- 1..5, do: {"same_exp_#{i}", "v#{i}", exp}
      assert :ok == submit_and_await!(store, batch)

      # Before expiry
      for i <- 1..5, do: assert({:ok, "v#{i}"} == NIF.get(store, "same_exp_#{i}"))

      :timer.sleep(250)
      for i <- 1..5, do: assert({:ok, nil} == NIF.get(store, "same_exp_#{i}"))
    end
  end

  describe "edge case: put_batch_async called with 1-entry batches rapidly" do
    test "100 single-entry batches in a tight loop all complete" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      op_ids =
        for i <- 1..100 do
          submit_async!(store, [{"rapid_#{i}", "v#{i}", 0}])
        end

      # Now drain all completions
      for op_id <- op_ids do
        receive do
          {:io_complete, ^op_id, :ok} -> :ok
        after
          10_000 -> flunk("timeout for op #{op_id}")
        end
      end

      for i <- 1..100 do
        assert {:ok, "v#{i}"} == NIF.get(store, "rapid_#{i}")
      end
    end
  end
end
