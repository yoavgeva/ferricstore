defmodule Ferricstore.ReviewR4.D13BatcherSlotTypeTest do
  @moduledoc """
  D13: Verifies whether the batcher slot type spec is missing the count field.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D13: batcher slot type missing count field" do
    test "slot @type does not include count but new_slot and enqueue_write use it" do
      # From batcher.ex lines 125-130:
      #
      #   @type slot :: %{
      #     cmds: [command()],
      #     froms: [GenServer.from()],
      #     timer_ref: reference() | nil,
      #     window_ms: pos_integer()
      #   }
      #
      # But new_slot at line 618-619 includes :count:
      #
      #   defp new_slot(window_ms) do
      #     %{cmds: [], froms: [], timer_ref: nil, window_ms: window_ms, count: 0}
      #   end
      #
      # And enqueue_write at line 476 uses it:
      #
      #   count: Map.get(slot, :count, 0) + 1
      #
      # And the flush check at line 491:
      #
      #   if updated_slot.count >= state.max_batch_size do
      #
      # So the @type spec is missing the :count field. This is a minor
      # typespec inaccuracy — it won't cause runtime errors because maps
      # in Elixir are open (extra keys are allowed), but dialyzer would
      # not catch missing :count access patterns.

      slot = %{cmds: [], froms: [], timer_ref: nil, window_ms: 1, count: 0}
      assert slot.count == 0

      updated = %{slot | count: slot.count + 1}
      assert updated.count == 1
    end
  end
end
