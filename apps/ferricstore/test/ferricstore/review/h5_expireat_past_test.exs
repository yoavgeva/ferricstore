defmodule Ferricstore.Review.H5ExpireatPastTest do
  @moduledoc """
  Proves that EXPIREAT and PEXPIREAT accept past timestamps without
  immediately deleting the key.

  Redis behaviour: a past absolute timestamp should delete the key
  on the spot (like EXPIRE 0 / negative) and TTL should return -2.

  Bug: `set_expiry_at_seconds/3` and `set_expiry_at_ms/3` in
  `Ferricstore.Commands.Expiry` (lines 112-124) pass the timestamp
  straight to `apply_expiry/3` without checking whether it is in the
  past. The key gets a past expiry written into the store instead of
  being deleted immediately.

  Secondary bug: EXPIREAT 0 converts to expire_at_ms = 0, which is
  the sentinel for "no expiry". The key becomes immortal instead of
  being deleted.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Expiry
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # EXPIREAT with past timestamp (non-zero)
  # ---------------------------------------------------------------------------

  describe "EXPIREAT past timestamp" do
    test "past timestamp is stored instead of actively deleting the key" do
      store = MockStore.make(%{"k" => {"v", 0}})

      # Timestamp 1000 = 1970-01-01T00:16:40Z — firmly in the past.
      assert 1 == Expiry.handle("EXPIREAT", ["k", "1000"], store)

      # MockStore's lazy-expiry hides the bug — get_meta returns nil
      # because alive?() fails at read time. But the command itself
      # never called store.delete; the entry is still physically in the
      # agent with expire_at_ms = 1_000_000.
      #
      # In Redis, EXPIREAT with a past timestamp actively deletes the
      # key (like DEL), same as EXPIRE with a negative value (line 84).
      meta = store.get_meta.("k")
      assert meta == nil, "lazy expiry masks the bug — get_meta returns nil"

      # TTL also returns -2 thanks to lazy expiry in MockStore.
      # A store without lazy filtering would return 0 (clamped by max/2).
      assert Expiry.handle("TTL", ["k"], store) == -2
    end

    @tag :bug
    test "BUG: EXPIREAT 0 makes key immortal instead of deleting it" do
      store = MockStore.make(%{"k" => {"v", 0}})

      # EXPIREAT 0 => set_expiry_at_seconds converts to 0 * 1000 = 0.
      # expire_at_ms = 0 is the sentinel for "no expiry", so the key
      # becomes immortal. Redis would delete it immediately.
      assert 1 == Expiry.handle("EXPIREAT", ["k", "0"], store)

      # BUG: key is still alive and readable — it should be deleted.
      assert store.get.("k") == "v",
             "EXPIREAT 0 made the key immortal (expire_at_ms=0 means no expiry)"

      # BUG: TTL returns -1 (no expiry) instead of -2 (key gone).
      assert Expiry.handle("TTL", ["k"], store) == -1
    end
  end

  # ---------------------------------------------------------------------------
  # PEXPIREAT with past timestamp (non-zero)
  # ---------------------------------------------------------------------------

  describe "PEXPIREAT past timestamp" do
    test "past ms timestamp is stored instead of actively deleting the key" do
      store = MockStore.make(%{"k" => {"v", 0}})

      # 1_000_000 ms = 1000 seconds after epoch — still 1970.
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "1000000"], store)

      # Same bug: key gets a past expiry instead of being deleted.
      # MockStore lazy-expiry masks it — get returns nil at read time.
      assert store.get.("k") == nil
      assert Expiry.handle("PTTL", ["k"], store) == -2
    end

    @tag :bug
    test "BUG: PEXPIREAT 0 makes key immortal instead of deleting it" do
      store = MockStore.make(%{"k" => {"v", 0}})

      # PEXPIREAT 0 => expire_at_ms = 0, the "no expiry" sentinel.
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "0"], store)

      # BUG: key is still alive and readable — it should be deleted.
      assert store.get.("k") == "v",
             "PEXPIREAT 0 made the key immortal (expire_at_ms=0 means no expiry)"

      # BUG: PTTL returns -1 (no expiry) instead of -2 (key gone).
      assert Expiry.handle("PTTL", ["k"], store) == -1
    end
  end

  # ---------------------------------------------------------------------------
  # Contrast: EXPIRE/PEXPIRE correctly handle non-positive values
  # ---------------------------------------------------------------------------

  describe "EXPIRE/PEXPIRE handle non-positive values correctly (for contrast)" do
    test "EXPIRE with negative seconds actively deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "-1"], store)
      # Key is truly gone from the store — not just lazy-expired.
      assert store.get.("k") == nil
      assert Expiry.handle("TTL", ["k"], store) == -2
    end

    test "EXPIRE 0 actively deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "0"], store)
      assert store.get.("k") == nil
      assert Expiry.handle("TTL", ["k"], store) == -2
    end

    test "PEXPIRE with negative ms actively deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "-1"], store)
      assert store.get.("k") == nil
      assert Expiry.handle("PTTL", ["k"], store) == -2
    end
  end
end
