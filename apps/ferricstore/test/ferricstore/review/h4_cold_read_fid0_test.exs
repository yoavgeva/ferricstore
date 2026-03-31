defmodule Ferricstore.Review.H4ColdReadFid0Test do
  @moduledoc """
  Verifies that ets_insert uses :pending as fid for unflushed entries,
  preventing cold reads from misinterpreting them as disk locations.

  Previously ets_insert stored fid=0, which matched the cold read pattern
  and caused pread_at(path, 0) to return wrong data. Now uses :pending,
  which ets_lookup treats as a miss — falling through to await_in_flight.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Shard

  @threshold 128

  setup do
    original = :persistent_term.get(:ferricstore_hot_cache_max_value_size, 65_536)
    :persistent_term.put(:ferricstore_hot_cache_max_value_size, @threshold)

    dir = Path.join(System.tmp_dir!(), "h4_cold_fid0_#{:rand.uniform(999_999)}")
    File.mkdir_p!(dir)

    index = :erlang.unique_integer([:positive]) |> rem(10_000) |> Kernel.+(10_000)
    {:ok, pid} = Shard.start_link(index: index, data_dir: dir, flush_interval_ms: 60_000)

    on_exit(fn ->
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original)
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm_rf!(dir)
    end)

    %{shard: pid, index: index}
  end

  describe "ets_insert uses :pending for unflushed entries" do
    test "large value ETS entry has fid=:pending", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      large = String.duplicate("X", @threshold + 1)

      # Seed a small value and flush so offset 0 of 00000.log is occupied.
      :ok = GenServer.call(shard, {:put, "seed", "tiny", 0})
      :ok = GenServer.call(shard, :flush)

      # Force flush_in_flight so the next PUT's pending is NOT flushed.
      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)

      # Write a large value — ets_insert stores nil with fid=:pending.
      :ok = GenServer.call(shard, {:put, "big", large, 0})

      [{_key, ets_val, _exp, _lfu, fid, off, vsize}] = :ets.lookup(keydir, "big")

      assert ets_val == nil, "large value should be nil in ETS (cold)"
      assert fid == :pending, "expected fid=:pending, got #{inspect(fid)}"
      assert off == 0
      assert vsize == 0
    end
  end

  describe "GET on unflushed large value works correctly" do
    test "read triggers flush and returns correct value", %{shard: shard, index: index} do
      large = String.duplicate("L", @threshold + 100)

      # Write large value — goes to pending with fid=:pending.
      :ok = GenServer.call(shard, {:put, "big", large, 0})

      # GET should trigger flush_pending_sync and return the correct value.
      result = GenServer.call(shard, {:get, "big"})
      assert result == large
    end
  end

  describe "after flush, ETS has real disk location" do
    test "fid and vsize are updated after flush", %{shard: shard, index: index} do
      keydir = :"keydir_#{index}"
      large = String.duplicate("A", @threshold + 50)

      :ok = GenServer.call(shard, {:put, "flushed_big", large, 0})
      :ok = GenServer.call(shard, :flush)

      [{_, nil, _, _, fid, _off, vsize}] = :ets.lookup(keydir, "flushed_big")

      assert fid != :pending, "fid should be updated from :pending after flush"
      assert is_integer(fid)
      assert vsize == byte_size(large)

      # GET returns correct value via cold read with real offset.
      assert large == GenServer.call(shard, {:get, "flushed_big"})
    end
  end
end
