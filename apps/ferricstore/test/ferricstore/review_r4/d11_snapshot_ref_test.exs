defmodule Ferricstore.ReviewR4.D11SnapshotRefTest do
  @moduledoc """
  D11: Verifies whether cross_shard_locks stores make_ref() as owner_ref,
  which would be non-serializable across nodes.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D11: cross_shard_locks stores references in Raft state" do
    test "owner_ref is make_ref() — stored in Raft-snapshotted state" do
      # From cross_shard_op.ex line 138:
      #   owner_ref = make_ref()
      #
      # This ref is passed to the Raft state machine via:
      #   {:lock_keys, keys_to_lock, owner_ref, expire_at}  (line 203)
      #   {:cross_shard_intent, owner_ref, full_intent}      (line 321)
      #
      # In state_machine.ex, do_lock_keys stores it at line 1611:
      #   Map.put(acc, key, {owner_ref, expire_at_ms})
      #
      # And do_write_intent stores it as a key in the intents map at line 1650:
      #   Map.put(intents, owner_ref, intent_map)
      #
      # Both cross_shard_locks and cross_shard_intents are part of the Raft
      # state (returned from init/1 at lines 123-124). This state is
      # snapshotted by ra's release_cursor mechanism and must be serializable
      # to survive node restarts.
      #
      # HOWEVER: Erlang references ARE serializable via :erlang.term_to_binary.
      # They can be written to disk and read back. The concern about "node-local"
      # references is about uniqueness guarantees, not serializability.
      #
      # The real issue is more subtle: after a snapshot restore on a different
      # node (or after restart), the reference is still valid as a map key
      # and can still be compared with ==. But make_ref() values from before
      # the restart will never be generated again, so orphaned locks/intents
      # can only be cleaned up by:
      #   1. Lock TTL expiry (checked in check_key_lock)
      #   2. Intent resolver (resolve_stale_intents)
      #   3. {:clear_locks} command
      #
      # This is acceptable because:
      # - Locks have a 5s TTL (@lock_ttl_ms in cross_shard_op.ex)
      # - IntentResolver runs on startup to clean stale intents
      # - The reference doesn't need to match a live process

      ref = make_ref()

      # References survive serialization/deserialization
      serialized = :erlang.term_to_binary(ref)
      deserialized = :erlang.binary_to_term(serialized)
      assert ref == deserialized

      # References work as map keys after deserialization
      state = %{cross_shard_locks: %{"key1" => {ref, 12_345}}}
      serialized_state = :erlang.term_to_binary(state)
      restored = :erlang.binary_to_term(serialized_state)
      assert restored.cross_shard_locks["key1"] == {ref, 12_345}

      {restored_ref, _exp} = restored.cross_shard_locks["key1"]
      assert restored_ref == ref
    end
  end
end
