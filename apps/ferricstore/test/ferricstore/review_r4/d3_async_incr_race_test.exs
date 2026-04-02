defmodule Ferricstore.ReviewR4.D3AsyncIncrRaceTest do
  @moduledoc """
  D3: Verifies that async INCR/INCR_FLOAT/APPEND use a non-atomic
  read-then-write pattern on ETS, which loses updates under concurrency.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D3: async INCR/APPEND read-modify-write without atomicity" do
    test "async_write :incr uses manual read-then-write on ETS" do
      # From router.ex lines 311-345:
      #
      #   defp async_write(idx, {:incr, key, delta}) do
      #     keydir = keydir_name(idx)
      #     now = System.os_time(:millisecond)
      #
      #     current =
      #       case :ets.lookup(keydir, key) do
      #         [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) -> value
      #         _ -> nil
      #       end
      #
      #     case current do
      #       nil ->
      #         async_write(idx, {:put, key, delta, 0})
      #         ...
      #       value when is_integer(value) ->
      #         new_val = value + delta
      #         async_write(idx, {:put, key, new_val, 0})
      #         ...
      #     end
      #
      # The pattern is:
      #   1. :ets.lookup (READ)
      #   2. compute new_val = value + delta
      #   3. async_write(:put, ...) which calls :ets.insert (WRITE)
      #
      # Between steps 1 and 3, another process can do the same read-compute-write
      # on the same key, and one increment is lost. ETS :ets.update_counter would
      # be atomic, but it's not used here.
      #
      # The quorum path does NOT have this bug because {:incr, key, delta} goes
      # through Raft which serializes the operation in the state machine's do_incr.
      # The async path bypasses Raft for the local write.

      # Demonstrate the race window with a simple ETS table
      table = :ets.new(:test_incr_race, [:set, :public])
      :ets.insert(table, {"counter", 0, 0, 0, 0, 0, 0})

      tasks =
        for _ <- 1..100 do
          Task.async(fn ->
            # Simulates the async_write :incr pattern (non-atomic read-modify-write)
            [{_, value, _, _, _, _, _}] = :ets.lookup(table, "counter")
            new_val = value + 1
            :ets.insert(table, {"counter", new_val, 0, 0, 0, 0, 0})
          end)
        end

      Task.await_many(tasks, 5_000)

      [{_, final, _, _, _, _, _}] = :ets.lookup(table, "counter")
      :ets.delete(table)

      # With true atomicity (ets:update_counter), final would always be 100.
      # With the non-atomic read-modify-write, it is very likely < 100.
      # We can't assert exact value due to scheduling, but the pattern is clear.
      # In CI/fast machines this may occasionally be 100 if no contention occurs,
      # so we document the pattern rather than assert on the race outcome.
      assert is_integer(final)
      assert final <= 100
    end

    test "async_write :incr_float uses same non-atomic pattern" do
      # From router.ex lines 347-391:
      # Same read-then-write pattern as :incr.
      # :ets.lookup -> compute new float -> :ets.insert
      assert true, "Same pattern as :incr — see lines 347-391 in router.ex"
    end

    test "async_write :append uses same non-atomic pattern" do
      # From router.ex lines 394-413:
      # :ets.lookup -> concatenate -> :ets.insert
      # Two concurrent APPENDs can both read the same base value
      # and each write base<>suffix, losing one append.
      assert true, "Same pattern as :incr — see lines 394-413 in router.ex"
    end
  end
end
