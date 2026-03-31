defmodule Ferricstore.Review.H2LruEvictionTest do
  @moduledoc """
  Proves that volatile_lru and allkeys_lru eviction policies do NOT implement
  actual LRU ordering.

  Bug: memory_guard.ex lines 361-363 fall through to a default clause that takes
  the first 5 entries from ETS fold order instead of sorting by last access time
  (ldt). As a result, recently-accessed keys can be evicted while stale keys
  survive — the opposite of correct LRU behavior.
  """

  use ExUnit.Case, async: false

  import Bitwise

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.LFU

  @keydir :keydir_0

  setup do
    on_exit(fn ->
      for i <- 0..9 do
        :ets.delete(@keydir, "lru_test_#{i}")
      end

      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
    end)

    :ok
  end

  describe "volatile_lru eviction does not respect last access time" do
    test "recently-accessed keys may be evicted over stale keys" do
      far_future = System.os_time(:millisecond) + 3_600_000

      # Stale keys: ldt 100 minutes ago.
      old_ldt = (LFU.now_minutes() - 100) &&& 0xFFFF
      old_lfu = LFU.pack(old_ldt, 5)

      for i <- 0..4 do
        :ets.insert(@keydir, {"lru_test_#{i}", "val", far_future, old_lfu, 1, 0, 3})
      end

      # Fresh keys: ldt = now, touched.
      for i <- 5..9 do
        fresh_lfu = LFU.pack(LFU.now_minutes(), 5)
        :ets.insert(@keydir, {"lru_test_#{i}", "val", far_future, fresh_lfu, 1, 0, 3})
        LFU.touch(@keydir, "lru_test_#{i}", fresh_lfu)
      end

      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :volatile_lru
        ])

      send(pid, :check)
      Process.sleep(200)
      GenServer.stop(pid)

      {evicted, survived} = partition_keys()

      assert length(evicted) > 0, "expected eviction to occur"

      # Under correct LRU, evicted keys should have the OLDEST ldt values.
      # Collect ldt from evicted and survived sets.
      evicted_ldts = ldts_for(evicted)
      survived_ldts = ldts_for(survived)

      # If LRU were correct: max(evicted_ldts) <= min(survived_ldts).
      # The buggy code ignores ldt entirely, so this invariant is violated
      # whenever a fresh key is evicted while a stale key survives.
      if survived_ldts != [] and evicted_ldts != [] do
        oldest_survivor = Enum.min(survived_ldts)
        newest_evicted = Enum.max(evicted_ldts)

        assert newest_evicted >= oldest_survivor,
               "BUG NOT REPRODUCED: eviction accidentally matched LRU order. " <>
                 "evicted_ldts=#{inspect(evicted_ldts)}, survived_ldts=#{inspect(survived_ldts)}"
      end
    end
  end

  describe "allkeys_lru eviction does not respect last access time" do
    test "recently-accessed keys may be evicted over stale keys" do
      far_future = System.os_time(:millisecond) + 3_600_000

      old_ldt = (LFU.now_minutes() - 100) &&& 0xFFFF
      old_lfu = LFU.pack(old_ldt, 5)

      for i <- 0..4 do
        :ets.insert(@keydir, {"lru_test_#{i}", "val", far_future, old_lfu, 1, 0, 3})
      end

      for i <- 5..9 do
        fresh_lfu = LFU.pack(LFU.now_minutes(), 5)
        :ets.insert(@keydir, {"lru_test_#{i}", "val", far_future, fresh_lfu, 1, 0, 3})
        LFU.touch(@keydir, "lru_test_#{i}", fresh_lfu)
      end

      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :allkeys_lru
        ])

      send(pid, :check)
      Process.sleep(200)
      GenServer.stop(pid)

      {evicted, survived} = partition_keys()

      assert length(evicted) > 0, "expected eviction to occur"

      evicted_ldts = ldts_for(evicted)
      survived_ldts = ldts_for(survived)

      if survived_ldts != [] and evicted_ldts != [] do
        oldest_survivor = Enum.min(survived_ldts)
        newest_evicted = Enum.max(evicted_ldts)

        assert newest_evicted >= oldest_survivor,
               "BUG NOT REPRODUCED: eviction accidentally matched LRU order. " <>
                 "evicted_ldts=#{inspect(evicted_ldts)}, survived_ldts=#{inspect(survived_ldts)}"
      end
    end
  end

  # -- helpers ---------------------------------------------------------------

  defp partition_keys do
    Enum.reduce(0..9, {[], []}, fn i, {ev, sv} ->
      key = "lru_test_#{i}"

      case :ets.lookup(@keydir, key) do
        [{_, nil, _, _, _, _, _}] -> {[key | ev], sv}
        [{_, _, _, _, _, _, _}] -> {ev, [key | sv]}
        [] -> {ev, sv}
      end
    end)
  end

  defp ldts_for(keys) do
    Enum.map(keys, fn key ->
      [{_, _, _, lfu, _, _, _}] = :ets.lookup(@keydir, key)
      {ldt, _counter} = LFU.unpack(lfu)
      ldt
    end)
  end
end
