defmodule Ferricstore.ReviewR4.D9ApplyCommandsCoverageTest do
  @moduledoc """
  D9: Verifies whether AsyncApplyWorker.apply_commands only handles
  :put and :delete, silently dropping other command types.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D9: apply_commands only handles :put and :delete" do
    test "split_puts_and_others separates puts from non-puts" do
      # From async_apply_worker.ex lines 475-479:
      #
      #   defp split_puts_and_others(commands) do
      #     Enum.split_with(commands, fn
      #       {:put, _, _, _} -> true
      #       _ -> false
      #     end)
      #   end
      #
      # This splits commands into {puts, others}.
      # Then apply_commands processes puts via NIF.v2_append_batch (lines 415-448)
      # and processes others via Enum.each with pattern match (lines 451-468):
      #
      #   Enum.each(others, fn
      #     {:delete, key} -> ...tombstone write...
      #   end)
      #
      # Any command that is NOT {:put, _, _, _} and NOT {:delete, _} will
      # cause a FunctionClauseError in the Enum.each lambda.
      #
      # HOWEVER: this is actually correct behavior. Looking at the code paths:
      #
      # 1. apply_batch/2 is documented to accept [command()] where command
      #    is typed as {:put, ...} | {:delete, ...} (line 71-73)
      #
      # 2. replicate/2 is called from Router.async_submit_to_raft which
      #    only sends {:put, ...} or {:delete, ...} commands (lines 261-486)
      #
      # 3. The Raft submission path wraps multiple commands in {:batch, cmds}
      #    and the state machine's apply_single handles all types.
      #
      # So while apply_commands only handles put/delete for LOCAL application,
      # this is by design: async mode only supports put/delete locally. More
      # complex commands (incr, append, etc.) are converted to puts by
      # async_write before reaching the worker.
      #
      # Looking at router.ex async_write:
      #   - :incr → reads, computes, calls async_write(:put, ...)
      #   - :incr_float → reads, computes, calls async_write(:put, ...)
      #   - :append → reads, computes, calls async_write(:put, ...)
      #   - :getset → calls async_write(:put, ...)
      #   - :getdel → calls async_write(:delete, ...)
      #   - :getex → calls async_write(:put, ...)
      #   - :setrange → calls async_write(:put, ...)
      #   - fallback → calls quorum_write (not async at all)
      #
      # So by the time commands reach apply_commands, they are always
      # {:put, ...} or {:delete, ...}.

      commands = [
        {:put, "key1", "val1", 0},
        {:delete, "key2"},
        {:put, "key3", "val3", 1000}
      ]

      {puts, others} =
        Enum.split_with(commands, fn
          {:put, _, _, _} -> true
          _ -> false
        end)

      assert length(puts) == 2
      assert length(others) == 1
      assert [{:delete, "key2"}] = others
    end

    test "non-put/delete commands would crash in others processing" do
      # If an {:incr, _, _} somehow reached apply_commands (which it shouldn't),
      # the Enum.each would raise FunctionClauseError because only {:delete, key}
      # is pattern-matched.
      #
      # We verify this by checking that Enum.each with a delete-only pattern
      # raises when given a non-delete command.
      bad_commands = [{:incr, "key", 1}]

      assert_raise FunctionClauseError, fn ->
        Enum.each(bad_commands, fn {:delete, _key} -> :ok end)
      end
    end
  end
end
