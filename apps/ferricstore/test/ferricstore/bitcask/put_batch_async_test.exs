defmodule Ferricstore.Bitcask.PutBatchAsyncTest do
  @moduledoc """
  Tests for `NIF.put_batch_async/2`.

  On Linux with io_uring available the NIF returns `{:pending, op_id}` and
  later sends `{:io_complete, op_id, :ok}` to the calling process.

  On macOS (or any platform without io_uring) the NIF falls back to the
  synchronous `put_batch` path and returns `:ok` directly.

  Tests are written to handle both code paths so the suite is green on all
  platforms.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "pba_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # Receive the `:io_complete` message for `op_id` if we got `{:pending, op_id}`,
  # or return `:ok` immediately if `put_batch_async` already returned `:ok`.
  defp await_result(:ok), do: :ok

  defp await_result({:pending, op_id}) do
    receive do
      {:io_complete, ^op_id, result} -> result
    after
      5_000 -> flunk("Timed out waiting for {:io_complete, #{op_id}, _}")
    end
  end

  # ---------------------------------------------------------------------------
  # Return value shape
  # ---------------------------------------------------------------------------

  describe "return value" do
    test "returns :ok or {:pending, op_id}" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      result = NIF.put_batch_async(store, [{"k", "v", 0}])

      assert result == :ok or match?({:pending, _}, result),
             "expected :ok or {:pending, op_id}, got #{inspect(result)}"
    end

    test "empty batch returns :ok or {:pending, op_id}" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      result = NIF.put_batch_async(store, [])
      assert result == :ok or match?({:pending, _}, result)
    end

    test "op_id is a non-negative integer when pending" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      case NIF.put_batch_async(store, [{"k", "v", 0}]) do
        {:pending, op_id} -> assert is_integer(op_id) and op_id >= 0
        :ok -> :ok
      end
    end

    test "successive calls return distinct op_ids when pending" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      results =
        for i <- 1..5 do
          r = NIF.put_batch_async(store, [{"k#{i}", "v#{i}", 0}])
          # drain any pending message so the next call can proceed
          _ = await_result(r)
          r
        end

      pending_ids =
        Enum.flat_map(results, fn
          {:pending, id} -> [id]
          :ok -> []
        end)

      if length(pending_ids) > 1 do
        assert length(Enum.uniq(pending_ids)) == length(pending_ids),
               "op_ids must be unique: #{inspect(pending_ids)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Completion message (async path)
  # ---------------------------------------------------------------------------

  describe "async completion" do
    test "io_complete message arrives with :ok for a normal batch" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = [{"ack_k", "ack_v", 0}]
      result = NIF.put_batch_async(store, batch)
      assert :ok == await_result(result)
    end

    test "io_complete arrives for each independent batch" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      for i <- 1..3 do
        batch = [{"seq_k#{i}", "seq_v#{i}", 0}]
        result = NIF.put_batch_async(store, batch)
        assert :ok == await_result(result), "batch #{i} did not complete with :ok"
      end
    end

    test "io_complete op_id matches the returned op_id" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      case NIF.put_batch_async(store, [{"match_k", "match_v", 0}]) do
        {:pending, op_id} ->
          assert_receive {:io_complete, ^op_id, :ok}, 5_000

        :ok ->
          # Sync path — no message expected
          :ok
      end
    end

    test "no stray io_complete messages after await" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      result = NIF.put_batch_async(store, [{"stray_k", "v", 0}])
      :ok = await_result(result)

      # No second message should arrive
      refute_receive {:io_complete, _, _}, 50
    end
  end

  # ---------------------------------------------------------------------------
  # Data correctness
  # ---------------------------------------------------------------------------

  describe "data correctness" do
    test "value is readable after async completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = [{"read_k", "read_v", 0}]
      :ok = await_result(NIF.put_batch_async(store, batch))

      assert {:ok, "read_v"} == NIF.get(store, "read_k")
    end

    test "all values in a multi-entry batch are readable after completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = Enum.map(1..10, fn i -> {"multi_k#{i}", "multi_v#{i}", 0} end)
      :ok = await_result(NIF.put_batch_async(store, batch))

      for i <- 1..10 do
        assert {:ok, "multi_v#{i}"} == NIF.get(store, "multi_k#{i}")
      end
    end

    test "value survives store reopen after async completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = await_result(NIF.put_batch_async(store, [{"dur_k", "dur_v", 0}]))

      _ = store
      :erlang.garbage_collect()

      store2 = open_store(dir)
      assert {:ok, "dur_v"} == NIF.get(store2, "dur_k")
    end

    test "expired entries are not visible after async completion" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      past = System.os_time(:millisecond) - 1_000
      future = System.os_time(:millisecond) + 60_000

      batch = [
        {"exp_async", "gone", past},
        {"live_async", "here", future}
      ]

      :ok = await_result(NIF.put_batch_async(store, batch))

      assert {:ok, nil} == NIF.get(store, "exp_async")
      assert {:ok, "here"} == NIF.get(store, "live_async")
    end

    test "overwrite via put_batch_async reflects last value" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = await_result(NIF.put_batch_async(store, [{"ov_k", "v1", 0}]))
      :ok = await_result(NIF.put_batch_async(store, [{"ov_k", "v2", 0}]))

      assert {:ok, "v2"} == NIF.get(store, "ov_k")
    end

    test "large batch (100 entries) all readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      batch = Enum.map(1..100, fn i -> {"lb_k#{i}", "lb_v#{i}", 0} end)
      :ok = await_result(NIF.put_batch_async(store, batch))

      for i <- 1..100 do
        assert {:ok, "lb_v#{i}"} == NIF.get(store, "lb_k#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Mixed sync / async
  # ---------------------------------------------------------------------------

  describe "interleaved put and put_batch_async" do
    test "sync put after async completion is readable" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = await_result(NIF.put_batch_async(store, [{"mix_async", "a", 0}]))
      :ok = NIF.put(store, "mix_sync", "s", 0)

      assert {:ok, "a"} == NIF.get(store, "mix_async")
      assert {:ok, "s"} == NIF.get(store, "mix_sync")
    end

    test "put_batch_async after sync put sees both" do
      dir = tmp_dir()
      store = open_store(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      :ok = NIF.put(store, "first_sync", "s", 0)
      :ok = await_result(NIF.put_batch_async(store, [{"then_async", "a", 0}]))

      assert {:ok, "s"} == NIF.get(store, "first_sync")
      assert {:ok, "a"} == NIF.get(store, "then_async")
    end
  end
end
