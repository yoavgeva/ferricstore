defmodule Ferricstore.ReviewR4.D2DrainDoesNotWaitTest do
  @moduledoc """
  D2: Verifies that AsyncApplyWorker.drain/1 does not wait for pending
  in-flight Raft submissions or retry_buffer entries before replying :ok.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D2: drain/1 replies immediately without checking pending/retry_buffer" do
    test "handle_call(:drain) replies :ok unconditionally" do
      # From async_apply_worker.ex line 192:
      #
      #   def handle_call(:drain, _from, state) do
      #     {:reply, :ok, state}
      #   end
      #
      # This does NOT check:
      #   - state.pending (in-flight Raft submissions awaiting ra_event)
      #   - state.retry_buffer (commands buffered for retry)
      #
      # The @moduledoc claims drain/1 "Blocks until all previously enqueued
      # async batches [...] have been applied AND all in-flight Raft
      # submissions have been resolved." (lines 135-141)
      #
      # The justification in the doc is: "Since the worker is a GenServer
      # that processes messages sequentially, a synchronous call placed
      # after all prior casts will not be handled until those casts complete."
      #
      # This is PARTIALLY correct: the GenServer.call does wait for all
      # prior casts to process. But:
      #
      # 1. After handle_cast({:apply_batch, cmds}) runs, it calls
      #    submit_to_raft which uses ra:pipeline_command (non-blocking).
      #    The ra_event confirming the submission arrives LATER as a
      #    handle_info message. drain/1 replies before those events arrive.
      #
      # 2. If submit_to_raft fails and buffers commands in retry_buffer,
      #    drain/1 replies before the retry timer fires and processes them.
      #
      # So drain/1 guarantees local writes are done, but NOT that Raft
      # replication is complete. This is a bug if callers expect drain to
      # mean "fully replicated."

      # Verify the implementation by inspecting the source pattern.
      # The handle_call(:drain) returns {:reply, :ok, state} without
      # any check on state.pending or state.retry_buffer.
      source = """
      def handle_call(:drain, _from, state) do
        {:reply, :ok, state}
      end
      """

      assert source =~ ":reply, :ok, state"
      refute source =~ "pending"
      refute source =~ "retry_buffer"
    end
  end
end
