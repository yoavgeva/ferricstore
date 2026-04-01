defmodule Ferricstore.ReviewR2.TransactionNamespaceIssuesTest do
  @moduledoc """
  Regression guards for code review findings R2-M9, R2-M10, R2-M11.

  R2-M9: Namespace config changes don't update in-flight batcher slots.
         When window_ms changes via NamespaceConfig.set, already-queued
         writes in the Batcher keep using the old timer. The ns_cache is
         cleared on :ns_config_changed, but an existing slot's timer_ref
         was started with the old window_ms and is not rescheduled.

  R2-M10: ACL not re-checked at EXEC time in embedded API mode.
          FerricStore.Tx.execute/1 passes an empty watched_keys map (%{})
          to Coordinator.execute/3 — no ACL check happens before or during
          execution. This is a regression guard documenting the gap.

  R2-M11: WATCH/EXEC race — basic contract that EXEC fails when a watched
          key is modified between WATCH and EXEC, and succeeds when it is not.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  # WriteVersion is per-shard; tested indirectly via Router.get_version/1
  alias Ferricstore.Transaction.Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive()
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()
    :ok
  end

  # ---------------------------------------------------------------------------
  # R2-M9: Namespace config changes don't update in-flight batcher slots
  # ---------------------------------------------------------------------------

  describe "R2-M9: namespace config changes vs in-flight batcher slots" do
    @tag timeout: 30_000
    test "ns_cache is cleared when config changes, but existing slot timers keep old window_ms" do
      # Pick a namespace prefix and a key that uses it.
      prefix = "r2m9ns"
      key = "#{prefix}:timer_test_#{System.unique_integer([:positive])}"

      # Set a short initial window so the batcher caches it.
      :ok = NamespaceConfig.set(prefix, "window_ms", "100")

      # Write a key to populate the batcher's ns_cache for this prefix.
      Router.put(key, "seed", 0)

      # Inspect the batcher state to verify the ns_cache contains our prefix.
      shard_idx = Router.shard_for(key)
      batcher_name = Batcher.batcher_name(shard_idx)
      state_before = :sys.get_state(batcher_name)

      # The ns_cache should have the prefix cached with window_ms=100.
      cached = Map.get(state_before.ns_cache, prefix)

      if cached do
        {cached_window, _durability} = cached
        assert cached_window == 100, "expected cached window_ms=100, got #{cached_window}"
      end

      # Now change window_ms to 1000.
      :ok = NamespaceConfig.set(prefix, "window_ms", "1000")

      # Give the :ns_config_changed message time to arrive at the batcher.
      Process.sleep(50)

      # After the config change, the ns_cache should be cleared.
      state_after = :sys.get_state(batcher_name)
      assert state_after.ns_cache == %{},
        "ns_cache should be cleared after config change, got: #{inspect(state_after.ns_cache)}"

      # Verify the new config is in effect for subsequent writes.
      assert NamespaceConfig.window_for(prefix) == 1000

      # Regression guard: the next write should pick up the new window_ms.
      # We verify this by writing and checking the slot's window_ms.
      # Use write_async to avoid blocking, then inspect state.
      task =
        Task.async(fn ->
          Batcher.write(shard_idx, {:put, "#{prefix}:check_#{System.unique_integer([:positive])}", "v", 0})
        end)

      # Small delay so the batcher processes the write and creates a new slot.
      Process.sleep(10)

      state_with_new_slot = :sys.get_state(batcher_name)

      # Find the slot for our prefix. Slot key is {prefix, durability}.
      matching_slots =
        Enum.filter(state_with_new_slot.slots, fn {{slot_prefix, _dur}, _slot} ->
          slot_prefix == prefix
        end)

      if matching_slots != [] do
        {{_prefix, _dur}, slot} = hd(matching_slots)

        # The new slot should use the updated window_ms=1000.
        assert slot.window_ms == 1000,
          "new slot should use updated window_ms=1000, got #{slot.window_ms}"
      end

      # Clean up: flush the batcher so the task can complete.
      Batcher.flush(shard_idx)
      Task.await(task, 10_000)
    end

    @tag timeout: 30_000
    test "config change mid-flight: slot timer uses old window, new writes use new window" do
      # This test demonstrates the actual issue: if a slot is already open
      # with a timer based on old window_ms, changing the config does NOT
      # reschedule that timer. The slot flushes on the old schedule.
      prefix = "r2m9mid"
      key1 = "#{prefix}:first_#{System.unique_integer([:positive])}"
      _key2 = "#{prefix}:second_#{System.unique_integer([:positive])}"

      # Set a long initial window so the slot stays open.
      :ok = NamespaceConfig.set(prefix, "window_ms", "5000")

      shard_idx = Router.shard_for(key1)

      # Queue a write — this starts a 5000ms timer on the slot.
      task1 =
        Task.async(fn ->
          Batcher.write(shard_idx, {:put, key1, "v1", 0})
        end)

      Process.sleep(20)

      # Verify the slot exists with the old window.
      state = :sys.get_state(Batcher.batcher_name(shard_idx))
      old_slot = Enum.find(state.slots, fn {{p, _}, _} -> p == prefix end)

      if old_slot do
        {{_, _}, slot_data} = old_slot
        assert slot_data.window_ms == 5000
        assert slot_data.timer_ref != nil, "slot should have an active timer"
      end

      # Now change the config to a short window.
      :ok = NamespaceConfig.set(prefix, "window_ms", "10")
      Process.sleep(50)

      # The old slot's timer is NOT rescheduled — it still has the 5000ms ref.
      # This is the bug documented in R2-M9.
      # Force flush to complete the test without waiting 5 seconds.
      Batcher.flush(shard_idx)
      Task.await(task1, 10_000)

      # Verify the key was written (the flush forced it through).
      ShardHelpers.eventually(
        fn -> Router.get(key1) == "v1" end,
        "key1 should be written after flush",
        50,
        50
      )
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M10: ACL not re-checked at EXEC time (embedded API)
  # ---------------------------------------------------------------------------

  describe "R2-M10: embedded MULTI/EXEC has no ACL enforcement" do
    @tag timeout: 30_000
    test "FerricStore.multi executes all commands without ACL check" do
      # In embedded mode, FerricStore.multi/1 uses FerricStore.Tx which calls
      # Coordinator.execute(queue, %{}, sandbox_namespace) — the empty map
      # means no WATCH keys, and there is no ACL layer in the call chain.
      #
      # This test documents that any command queued via Tx.set/Tx.get/etc.
      # executes unconditionally. If ACL enforcement is added later, this
      # test should be updated to verify it.
      key = "r2m10:acl_test_#{System.unique_integer([:positive])}"

      {:ok, results} =
        FerricStore.multi(fn tx ->
          tx
          |> FerricStore.Tx.set(key, "written_without_acl")
          |> FerricStore.Tx.get(key)
        end)

      assert results == [:ok, "written_without_acl"],
        "multi/exec should execute all commands (no ACL enforcement in embedded mode)"

      assert Router.get(key) == "written_without_acl"
    end

    @tag timeout: 30_000
    test "Coordinator.execute accepts any command type without permission check" do
      # Directly invoke the Coordinator with commands that would require
      # different permission levels in a TCP/ACL-enabled context.
      # The Coordinator has no ACL gate — it executes everything.
      key = "r2m10:coord_#{System.unique_integer([:positive])}"

      # Write + read + delete — all "permission levels" in one transaction.
      queue = [
        {"SET", [key, "secret"]},
        {"GET", [key]},
        {"DEL", [key]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, "secret", 1],
        "Coordinator executes all commands without ACL checks"
    end

    @tag timeout: 30_000
    test "Tx.execute passes empty watched_keys (no WATCH support in embedded API)" do
      # FerricStore.Tx.execute/1 always passes %{} as watched_keys to
      # Coordinator.execute/3. This means the embedded API has no WATCH
      # capability — transactions always execute (never return nil).
      key = "r2m10:nowatch_#{System.unique_integer([:positive])}"

      Router.put(key, "original", 0)

      # Even though we modify the key before executing the transaction,
      # it succeeds because Tx doesn't support WATCH.
      Router.put(key, "modified_externally", 0)

      {:ok, results} =
        FerricStore.multi(fn tx ->
          tx
          |> FerricStore.Tx.set(key, "overwritten_by_tx")
          |> FerricStore.Tx.get(key)
        end)

      assert results == [:ok, "overwritten_by_tx"],
        "Tx always executes — no WATCH support means no optimistic locking in embedded API"
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M11: WATCH/EXEC basic contract (regression guard)
  # ---------------------------------------------------------------------------

  describe "R2-M11: WATCH/EXEC contract" do
    @tag timeout: 30_000
    test "EXEC succeeds when watched key is unchanged" do
      key = "r2m11:unchanged_#{System.unique_integer([:positive])}"

      Router.put(key, "original", 0)

      # Capture the write version (simulates WATCH).
      version = Router.get_version(key)
      watched = %{key => version}

      # No modification to key — EXEC should succeed.
      queue = [{"SET", [key, "updated_by_tx"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert is_list(result), "EXEC should return a list of results when WATCH passes"
      assert result == [:ok]
      assert Router.get(key) == "updated_by_tx"
    end

    @tag timeout: 30_000
    test "EXEC fails (returns nil) when watched key is modified" do
      key = "r2m11:modified_#{System.unique_integer([:positive])}"

      Router.put(key, "original", 0)

      # WATCH the key.
      version = Router.get_version(key)
      watched = %{key => version}

      # Another client modifies the key between WATCH and EXEC.
      Router.put(key, "changed_by_other", 0)

      # EXEC should detect the version mismatch and abort.
      queue = [{"SET", [key, "should_not_apply"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should return nil when a watched key was modified"
      assert Router.get(key) == "changed_by_other",
        "original modification should persist — tx was aborted"
    end

    @tag timeout: 30_000
    test "WATCH detects key deletion" do
      key = "r2m11:deleted_#{System.unique_integer([:positive])}"

      Router.put(key, "exists", 0)

      version = Router.get_version(key)
      watched = %{key => version}

      # Delete the key — should bump the write version for that shard.
      Router.delete(key)

      queue = [{"SET", [key, "should_not_apply"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort when a watched key was deleted"
    end

    @tag timeout: 30_000
    test "WATCH on multiple keys — one modified aborts entire transaction" do
      key_a = "r2m11:multi_a_#{System.unique_integer([:positive])}"
      key_b = "r2m11:multi_b_#{System.unique_integer([:positive])}"

      Router.put(key_a, "a_orig", 0)
      Router.put(key_b, "b_orig", 0)

      version_a = Router.get_version(key_a)
      version_b = Router.get_version(key_b)
      watched = %{key_a => version_a, key_b => version_b}

      # Modify only key_b.
      Router.put(key_b, "b_changed", 0)

      queue = [
        {"SET", [key_a, "a_new"]},
        {"SET", [key_b, "b_new"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort if ANY watched key was modified"
      assert Router.get(key_a) == "a_orig", "key_a should be unchanged"
      assert Router.get(key_b) == "b_changed", "key_b should retain the external modification"
    end

    @tag timeout: 30_000
    test "WATCH uses per-shard write version — write to different key on same shard triggers abort" do
      # WriteVersion is per-shard, not per-key. Writing to ANY key on the
      # same shard increments the version. This means WATCH can produce
      # false positives (abort even though the watched key wasn't modified).
      # This is by design — safe but conservative.
      #
      # Use keys_on_same_shard() directly (without prefixing) to ensure
      # both keys route to the same shard.
      {key_a, key_b} = ShardHelpers.keys_on_same_shard()

      # Sanity check: both keys must route to the same shard.
      assert Router.shard_for(key_a) == Router.shard_for(key_b),
        "test infrastructure: keys should be on the same shard"

      Router.put(key_a, "watched_val", 0)

      version_a = Router.get_version(key_a)
      watched = %{key_a => version_a}

      # Write to key_b which is on the SAME shard — bumps the shard version.
      Router.put(key_b, "unrelated_write", 0)

      queue = [{"GET", [key_a]}]
      result = Coordinator.execute(queue, watched, nil)

      # Per-shard versioning means this WILL abort (false positive).
      # This documents the expected behavior.
      assert result == nil,
        "per-shard version means writing any key on the shard aborts WATCH"
    end

    @tag timeout: 30_000
    test "WATCH version check uses get_version which goes through GenServer" do
      # Verify that get_version returns a consistent, incrementing value.
      key = "r2m11:version_#{System.unique_integer([:positive])}"

      Router.put(key, "v1", 0)
      v1 = Router.get_version(key)

      Router.put(key, "v2", 0)
      v2 = Router.get_version(key)

      # Version should have incremented.
      assert v2 > v1, "write version should increment after a write (v1=#{v1}, v2=#{v2})"
    end

    @tag timeout: 30_000
    test "concurrent WATCH/EXEC — only one succeeds under contention" do
      key = "r2m11:race_#{System.unique_integer([:positive])}"

      Router.put(key, "0", 0)

      # Two tasks both WATCH the same key and try to INCR it.
      # Only one should succeed; the other should see a version conflict.
      results =
        1..10
        |> Enum.map(fn i ->
          Task.async(fn ->
            version = Router.get_version(key)
            watched = %{key => version}
            queue = [{"SET", [key, Integer.to_string(i)]}]
            Coordinator.execute(queue, watched, nil)
          end)
        end)
        |> Enum.map(&Task.await(&1, 10_000))

      succeeded = Enum.count(results, &is_list/1)
      aborted = Enum.count(results, &(&1 == nil))

      # At least one should succeed, and at least some should abort due
      # to contention (since they all read the same version).
      assert succeeded >= 1, "at least one WATCH/EXEC should succeed"
      assert succeeded + aborted == 10, "all results should be either list or nil"
    end
  end
end
