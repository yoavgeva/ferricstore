defmodule Ferricstore.ReviewR4.D8RaEventConsumptionTest do
  @moduledoc """
  D8: Verifies whether wait_for_ra_applied's selective receive can consume
  ra_event messages belonging to other concurrent callers.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D8: wait_for_ra_applied consumes unrelated ra_events" do
    test "selective receive correctly skips ra_events without matching corr" do
      # From router.ex lines 155-192:
      #
      #   defp wait_for_ra_applied(corr, shard_id, idx, command) do
      #     receive do
      #       {:ra_event, _leader, {:applied, applied_list}} ->
      #         case List.keyfind(applied_list, corr, 0) do
      #           {^corr, result} -> result
      #           nil -> wait_for_ra_applied(corr, shard_id, idx, command)
      #         end
      #       {:ra_event, _from, {:rejected, {:not_leader, maybe_leader, ^corr}}} ->
      #         ...retry...
      #       {:ra_event, _from, {:rejected, {_reason, _hint, ^corr}}} ->
      #         ...fallback...
      #     after
      #       10_000 -> {:error, "ERR write timeout"}
      #     end
      #   end
      #
      # KEY INSIGHT: This is NOT a "silent consumption" bug because:
      #
      # 1. For {:applied, applied_list} messages: the function checks if
      #    its own corr is in the list. If NOT found, it loops (recursion).
      #    The message IS consumed from the mailbox, but that's intentional —
      #    ra batches multiple applied results into a single message.
      #
      # 2. For {:rejected, ...} messages: the pattern matches on ^corr
      #    (pin operator), so ONLY messages with the caller's exact corr
      #    are consumed. Other rejection messages stay in the mailbox.
      #
      # HOWEVER, there IS a real issue:
      # When two concurrent pipeline_command calls are made from the SAME
      # process (e.g., in a test or a Task), ra may batch both results in
      # a single {:applied, applied_list} message. Process P1 waiting for
      # corr1 receives the message, finds corr1, returns the result, and
      # CONSUMES the entire message (including corr2's result). Process P2
      # waiting for corr2 never receives its result — it times out.
      #
      # But wait — router functions run in the CALLER's process, and each
      # caller typically makes ONE pipeline_command call. The comment at
      # line 149-152 acknowledges the batching:
      #   "Unrelated ra_events are silently skipped — their results belong
      #    to other concurrent calls in the same process"
      #
      # This IS the bug: when the applied_list contains corr1 AND corr2,
      # the first wait_for_ra_applied call matches corr1 and discards the
      # entire message. The second call for corr2 never sees it.
      #
      # In practice, this can happen when a single process submits multiple
      # pipeline_commands concurrently (Task.async_stream, Enum.map, etc).

      # Demonstrate the consumption issue with message passing
      corr1 = make_ref()
      corr2 = make_ref()

      # Simulate ra sending a single batched applied message
      send(self(), {:ra_event, :leader, {:applied, [{corr1, :ok}, {corr2, :ok}]}})

      # First receive finds corr1 — consumes entire message
      result1 =
        receive do
          {:ra_event, _leader, {:applied, applied_list}} ->
            case List.keyfind(applied_list, corr1, 0) do
              {^corr1, result} -> {:found, result}
              nil -> :not_found
            end
        after
          100 -> :timeout
        end

      assert result1 == {:found, :ok}

      # Second receive for corr2 — message is gone!
      result2 =
        receive do
          {:ra_event, _leader, {:applied, applied_list}} ->
            case List.keyfind(applied_list, corr2, 0) do
              {^corr2, result} -> {:found, result}
              nil -> :not_found
            end
        after
          100 -> :timeout
        end

      # corr2's result was consumed with the first receive
      assert result2 == :timeout
    end
  end
end
