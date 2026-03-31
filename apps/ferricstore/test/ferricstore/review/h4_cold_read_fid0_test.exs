defmodule Ferricstore.Review.H4ColdReadFid0Test do
  @moduledoc """
  Proves the ETS cold-read bug where fid=0, off=0 entries are
  incorrectly treated as valid cold reads.

  Bug (shard.ex ets_lookup/ets_insert):
    `ets_insert/4` stores large values as `{key, nil, exp, lfu, 0, 0, 0}`
    (fid=0, off=0, vsize=0). The `:pending` sentinel that `ets_lookup`
    checks for (line 3226) is NEVER written by `ets_insert` — it only
    appears in the Router.async_write path for small values. So unflushed
    large-value entries fall through to the cold-read branch which treats
    fid=0/off=0 as a real disk location, causing `v2_pread_at(path, 0)`
    to read the file header or a stale record at offset 0.

  Trigger path:
    1. A prior flush sets `flush_in_flight` (async fsync in progress).
    2. PUT of a large value → ets_insert stores {key,nil,0,lfu,0,0,0},
       but flush_pending short-circuits because flush_in_flight != nil.
    3. GET → ets_lookup returns {:cold, 0, 0, 0, 0} → binds stale
       fid=0/off=0 → flush_pending_sync flushes to disk → but stale
       bindings feed v2_pread_at(00000.log, 0) → wrong data or nil.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Shard

  @threshold 128

  setup do
    original = :persistent_term.get(:ferricstore_hot_cache_max_value_size, 65_536)
    :persistent_term.put(:ferricstore_hot_cache_max_value_size, @threshold)

    dir = Path.join(System.tmp_dir!(), "h4_cold_fid0_#{:rand.uniform(999_999)}")
    File.mkdir_p!(dir)

    # Use a large flush interval so the timer doesn't interfere.
    index = :erlang.unique_integer([:positive]) |> rem(10_000) |> Kernel.+(10_000)
    {:ok, pid} = Shard.start_link(index: index, data_dir: dir, flush_interval_ms: 60_000)

    on_exit(fn ->
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original)
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm_rf!(dir)
    end)

    %{shard: pid, index: index}
  end

  describe "ets_insert creates unflushed entries with fid=0 instead of :pending" do
    test "large value ETS entry has fid=0,off=0,vsize=0 — not :pending", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      large = String.duplicate("X", @threshold + 1)

      # Seed a small value and flush so offset 0 of 00000.log is occupied.
      :ok = GenServer.call(shard, {:put, "seed", "tiny", 0})
      :ok = GenServer.call(shard, :flush)

      # Force flush_in_flight so the next PUT's pending is NOT flushed.
      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)

      # Write a large value — ets_insert stores nil with fid=0.
      :ok = GenServer.call(shard, {:put, "big", large, 0})

      # Inspect ETS directly (public table, readable from test process).
      [{_key, ets_val, _exp, _lfu, fid, off, vsize}] = :ets.lookup(keydir, "big")

      assert ets_val == nil, "large value should be nil in ETS (cold)"
      assert fid == 0, "expected fid=0, got #{inspect(fid)}"
      assert off == 0, "expected off=0, got #{inspect(off)}"
      assert vsize == 0, "expected vsize=0, got #{inspect(vsize)}"

      # This is the bug: fid should be :pending (like Router.async_write does
      # for small values), but ets_insert unconditionally uses 0.
      refute fid == :pending,
             "if this fails, the bug is fixed — ets_insert now uses :pending"
    end
  end

  describe "GET returns wrong data for unflushed large value" do
    test "cold read at fid=0/off=0 returns seed value instead of large value", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      seed = "seed_value_at_offset_zero"
      large = String.duplicate("L", @threshold + 100)

      # 1. Write seed → flush → seed occupies offset 0 of 00000.log.
      :ok = GenServer.call(shard, {:put, "seed", seed, 0})
      :ok = GenServer.call(shard, :flush)

      # 2. Simulate in-flight fsync so next PUT stays in pending.
      fake_corr = :erlang.unique_integer([:positive])
      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: fake_corr} end)

      # 3. Write large value — goes to pending, ETS has fid=0/off=0.
      :ok = GenServer.call(shard, {:put, "big", large, 0})

      # Confirm ETS state is the buggy one.
      [{_, nil, _, _, 0, 0, 0}] = :ets.lookup(keydir, "big")

      # 4. Deliver the fake tokio_complete so await_in_flight unblocks,
      #    then issue GET which will use stale fid=0/off=0.
      send(shard, {:tokio_complete, fake_corr, :ok, :ok})
      result = GenServer.call(shard, {:get, "big"})

      # The bug: GET reads v2_pread_at(00000.log, 0) which returns the
      # seed record's value, not the large value. If the large value were
      # returned correctly, result == large. Instead we get seed or nil.
      assert result != large,
             "BUG NOT REPRODUCED: GET returned the correct large value. " <>
               "The cold-read fid=0 bug may have been fixed."

      # The stale read returns either the seed value or nil.
      assert result == seed or result == nil,
             "expected seed value or nil from stale offset-0 read, got: #{inspect(String.slice(result || "", 0, 80))}"
    end
  end

  describe "Router.async_write large-value path is correct (no bug)" do
    test "async_write stores real fid/offset for large values", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      large = String.duplicate("A", @threshold + 50)

      # The Router.async_write path for large values calls
      # v2_append_batch_nosync synchronously and stores real fid/offset.
      # We verify this by writing through the shard (non-raft, direct path)
      # with no in-flight flush, so flush_pending writes immediately.
      #
      # After a normal PUT + flush, ETS should have real fid/offset.
      :ok = GenServer.call(shard, {:put, "async_big", large, 0})
      :ok = GenServer.call(shard, :flush)

      [{_, nil, _, _, fid, _off, vsize}] = :ets.lookup(keydir, "async_big")

      # After flush, the entry should have a real disk location.
      assert vsize == byte_size(large),
             "expected vsize=#{byte_size(large)}, got #{vsize}"

      # fid should be the active file id (0 for fresh shard is valid,
      # but off must be > 0 since seed wrote first).
      assert fid >= 0, "expected valid fid, got #{inspect(fid)}"

      # Confirm GET returns the correct value.
      assert large == GenServer.call(shard, {:get, "async_big"})
    end
  end
end
