defmodule Ferricstore.ReviewR4.D5HlcDuplicateTimestampTest do
  @moduledoc """
  D5: Verifies whether HLC.now/0 can produce duplicate timestamps
  when two concurrent callers both see wall > last.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D5: HLC.now/0 duplicate timestamps" do
    test "race in wall-advanced branch can produce duplicates" do
      # From hlc.ex lines 142-168 (now/0):
      #
      #   wall = System.os_time(:millisecond)
      #   last = :atomics.get(ref, @slot_physical)
      #
      #   if wall > last do
      #     :atomics.put(ref, @slot_physical, wall)    # step A
      #     :atomics.put(ref, @slot_logical, 0)        # step B
      #     {wall, 0}                                   # step C
      #   else
      #     logical = :atomics.add_get(ref, @slot_logical, 1)
      #     {last, logical}
      #   end
      #
      # Race scenario (two processes P1 and P2, same millisecond):
      #
      # 1. P1: wall=1000, last=999 → wall > last → enters if-branch
      # 2. P2: wall=1000, last=999 → wall > last → enters if-branch
      # 3. P1: put(physical, 1000), put(logical, 0), returns {1000, 0}
      # 4. P2: put(physical, 1000), put(logical, 0), returns {1000, 0}
      #
      # Both return {1000, 0} — duplicate timestamp.
      #
      # The code comment at line 155-158 acknowledges this:
      #   "Another process may race us here. That is acceptable: the worst
      #    case is two processes both see wall > last and both write the same
      #    wall value (idempotent put) then both get logical 0 or 1 via
      #    add_get. Monotonicity is preserved because wall > last."
      #
      # However the comment says "logical 0 or 1" which is incorrect.
      # Both processes execute put(logical, 0) then return {wall, 0}.
      # Neither calls add_get — they both take the if-branch which
      # hardcodes logical=0 in the return value.
      #
      # The fix would be: after putting physical, use add_get on logical
      # instead of put(0) + hardcoded return.

      # We can demonstrate this with atomics directly, simulating the
      # interleaving where both processes read last BEFORE either writes.
      ref = :atomics.new(2, signed: true)
      :atomics.put(ref, 1, 999)  # last physical
      :atomics.put(ref, 2, 5)    # last logical

      wall = 1000
      slot_physical = 1
      slot_logical = 2

      # Simulate the race: both processes read last=999 before either writes.
      # This is the actual interleaving that causes duplicates.
      p1_last = :atomics.get(ref, slot_physical)  # P1 reads: 999
      p2_last = :atomics.get(ref, slot_physical)  # P2 reads: 999

      assert wall > p1_last
      assert wall > p2_last

      # Both enter the if-branch and write the same values
      :atomics.put(ref, slot_physical, wall)   # P1 writes
      :atomics.put(ref, slot_logical, 0)       # P1 resets logical
      p1_result = {wall, 0}                     # P1 returns {1000, 0}

      :atomics.put(ref, slot_physical, wall)   # P2 writes (idempotent)
      :atomics.put(ref, slot_logical, 0)       # P2 resets logical (clobbers P1's counter!)
      p2_result = {wall, 0}                     # P2 returns {1000, 0}

      # Both produce the exact same timestamp — duplicate
      assert p1_result == p2_result
      assert p1_result == {1000, 0}
    end
  end
end
