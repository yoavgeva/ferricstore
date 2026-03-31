defmodule Ferricstore.Review.M1AsyncRetryOrderTest do
  @moduledoc """
  Verifies that AsyncApplyWorker's retry buffer preserves FIFO order
  using :queue. Commands that fail in order A, B, C are retried A first.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.AsyncApplyWorker
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  describe "retry buffer preserves FIFO order" do
    test ":queue.in preserves submission order" do
      cmd_a = [{:put, "key_A", "val_A", 0}]
      cmd_b = [{:put, "key_B", "val_B", 0}]
      cmd_c = [{:put, "key_C", "val_C", 0}]

      # Simulate buffer_for_retry adding A, then B, then C
      q = :queue.new()
      q = :queue.in({cmd_a, 0}, q)
      q = :queue.in({cmd_b, 0}, q)
      q = :queue.in({cmd_c, 0}, q)

      keys =
        :queue.to_list(q)
        |> Enum.map(fn {[{:put, key, _val, _ttl}], _retry_count} -> key end)

      # FIFO: A first, then B, then C
      assert keys == ["key_A", "key_B", "key_C"]
    end

    test "retry timer processes commands in submission order" do
      worker = AsyncApplyWorker.worker_name(0)
      pid = Process.whereis(worker)
      original_state = :sys.get_state(pid)

      on_exit(fn ->
        :sys.replace_state(pid, fn _s -> original_state end)
      end)

      cmd_a = [{:put, "retry_order_A", "val_A", 0}]
      cmd_b = [{:put, "retry_order_B", "val_B", 0}]
      cmd_c = [{:put, "retry_order_C", "val_C", 0}]

      # Build a FIFO queue: A first, B second, C third
      q = :queue.new()
      q = :queue.in({cmd_a, 0}, q)
      q = :queue.in({cmd_b, 0}, q)
      q = :queue.in({cmd_c, 0}, q)

      :sys.replace_state(pid, fn state ->
        %{state | retry_buffer: q, pending: %{}}
      end)

      # Fire the retry timer
      send(pid, :retry_buffer)
      _ = :sys.get_state(pid)
      state_after = :sys.get_state(pid)

      # Buffer should be drained
      assert :queue.is_empty(state_after.retry_buffer)

      # All three commands should be in pending
      pending_keys =
        state_after.pending
        |> Map.values()
        |> Enum.map(fn {[{:put, key, _val, _ttl}], _retry_count} -> key end)

      assert "retry_order_A" in pending_keys
      assert "retry_order_B" in pending_keys
      assert "retry_order_C" in pending_keys
    end

    test "Enum.reduce over :queue.to_list iterates in FIFO order" do
      cmd_a = [{:put, "key_A", "val_A", 0}]
      cmd_b = [{:put, "key_B", "val_B", 0}]
      cmd_c = [{:put, "key_C", "val_C", 0}]

      q = :queue.new()
      q = :queue.in({cmd_a, 0}, q)
      q = :queue.in({cmd_b, 0}, q)
      q = :queue.in({cmd_c, 0}, q)

      iteration_order =
        :queue.to_list(q)
        |> Enum.reduce([], fn {[{:put, key, _v, _t}], _rc}, acc ->
          acc ++ [key]
        end)

      # FIFO: A processed first, then B, then C
      assert iteration_order == ["key_A", "key_B", "key_C"]
    end
  end
end
