defmodule Ferricstore.Commands.KeydirFullTest do
  @moduledoc """
  Tests for the KEYDIR_FULL error when the keydir (ETS) approaches its
  memory limit under the `:noeviction` policy (spec section 2.4).

  New-key writes are rejected when MemoryGuard reports reject-level pressure
  and the eviction policy is `:noeviction`. Updates to existing keys always
  succeed regardless of pressure level.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # ---------------------------------------------------------------------------
  # Setup: flush keys and save/restore MemoryGuard state
  # ---------------------------------------------------------------------------

  setup do
    ShardHelpers.flush_all_keys()

    # Save the original MemoryGuard state so we can restore it after each test.
    original_state = :sys.get_state(MemoryGuard)

    on_exit(fn ->
      # Restore MemoryGuard to its original state.
      :sys.replace_state(MemoryGuard, fn _current -> original_state end)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Forces MemoryGuard into reject+noeviction state so reject_writes?() is true.
  defp force_reject_mode do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :reject, eviction_policy: :noeviction}
    end)
  end

  # Forces MemoryGuard into ok pressure (no rejection).
  defp force_ok_mode do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :ok, eviction_policy: :noeviction}
    end)
  end

  # Forces MemoryGuard into reject pressure but with a non-noeviction policy.
  defp force_reject_with_eviction do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :reject, eviction_policy: :volatile_lru}
    end)
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "KEYDIR_FULL rejection (spec 2.4)" do
    test "new key is rejected when pressure is at reject level with noeviction" do
      # Write one key before enabling reject mode.
      assert :ok = Router.put("seed_key", "value", 0)

      # Switch MemoryGuard to reject+noeviction mode.
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true

      # Try writing a NEW key -- should be rejected.
      result = Router.put("brand_new_key", "value", 0)
      assert {:error, msg} = result
      assert msg =~ "KEYDIR_FULL"
    end

    test "update to existing key succeeds even under reject pressure" do
      # Write a key before pressure kicks in.
      assert :ok = Router.put("existing_key", "old_value", 0)

      # Switch to reject mode.
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true

      # Update the EXISTING key -- should succeed despite pressure.
      assert :ok = Router.put("existing_key", "new_value", 0)

      # Verify the update took effect.
      assert Router.get("existing_key") == "new_value"
    end

    test "new key succeeds when pressure is below reject threshold" do
      force_ok_mode()
      assert MemoryGuard.reject_writes?() == false

      # New key should succeed.
      assert :ok = Router.put("allowed_key", "value", 0)
      assert Router.get("allowed_key") == "value"
    end

    test "new key succeeds when eviction policy is not :noeviction" do
      # Reject-level pressure but with volatile_lru policy.
      force_reject_with_eviction()

      # reject_writes? should be false because policy is volatile_lru, not noeviction.
      assert MemoryGuard.reject_writes?() == false

      # New key should succeed.
      assert :ok = Router.put("new_volatile_key", "value", 0)
      assert Router.get("new_volatile_key") == "value"
    end

    test "KEYDIR_FULL error message is correct" do
      assert :ok = Router.put("seed_msg", "v", 0)
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true

      result = Router.put("fail_key", "v", 0)

      assert {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"} = result
    end

    test "multiple new key attempts are all rejected under pressure" do
      force_reject_mode()

      for i <- 1..5 do
        result = Router.put("new_key_#{i}", "v", 0)
        assert {:error, msg} = result
        assert msg =~ "KEYDIR_FULL"
      end
    end

    test "existing key with TTL update succeeds under pressure" do
      # Write key with no expiry.
      assert :ok = Router.put("ttl_key", "value", 0)

      force_reject_mode()

      # Update with new TTL -- should succeed (it's an existing key).
      expire_at = System.os_time(:millisecond) + 60_000
      assert :ok = Router.put("ttl_key", "value", expire_at)
    end
  end
end
