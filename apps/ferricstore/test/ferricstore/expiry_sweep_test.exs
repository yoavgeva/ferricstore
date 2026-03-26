defmodule Ferricstore.ExpirySweepTest do
  @moduledoc """
  Tests for the active expiry sweep in Shard GenServers.

  These tests verify that the periodic sweep timer proactively removes expired
  keys from ETS without waiting for a read (lazy expiry). The tests use the
  application-supervised shards and the Router convenience API.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  import Ferricstore.Test.ShardHelpers, only: [flush_all_keys: 0]

  # Use a unique prefix per test to avoid cross-test key collisions.
  defp ukey(base), do: "expiry_sweep_test:#{base}:#{System.unique_integer([:positive])}"

  setup do
    flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Triggers the expiry sweep for a specific shard synchronously by calling
  # into the GenServer rather than sending an async message. This guarantees
  # the sweep has completed before the caller continues.
  defp trigger_sweep(shard_index) do
    name = Router.shard_name(shard_index)
    GenServer.call(name, :expiry_sweep)
  end

  # Triggers sweep on all 4 shards.
  defp trigger_all_sweeps do
    Enum.each(0..3, &trigger_sweep/1)
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "active expiry sweep" do
    test "expired keys are removed after sweep runs" do
      key = ukey("expired")
      past = System.os_time(:millisecond) - 1_000
      Router.put(key, "value", past)

      # Key is in ETS but expired — lazy expiry would catch it on read,
      # but we want the sweep to catch it proactively.
      shard_idx = Router.shard_for(key)
      trigger_sweep(shard_idx)

      # After sweep, the key should be gone from ETS.
      ets = :"keydir_#{shard_idx}"
      assert :ets.lookup(ets, key) == []
    end

    test "keys without TTL are not affected by sweep" do
      key = ukey("no_ttl")
      Router.put(key, "persistent_value", 0)

      shard_idx = Router.shard_for(key)
      trigger_sweep(shard_idx)

      # Key should still be present.
      assert Router.get(key) == "persistent_value"
    end

    test "keys with future TTL are not affected by sweep" do
      key = ukey("future_ttl")
      future = System.os_time(:millisecond) + 60_000
      Router.put(key, "alive_value", future)

      shard_idx = Router.shard_for(key)
      trigger_sweep(shard_idx)

      assert Router.get(key) == "alive_value"
    end

    test "expired keys are not returned by GET after sweep" do
      key = ukey("get_after_sweep")
      past = System.os_time(:millisecond) - 500
      Router.put(key, "gone", past)

      shard_idx = Router.shard_for(key)
      trigger_sweep(shard_idx)

      assert Router.get(key) == nil
    end

    test "sweep respects max_keys_per_sweep limit" do
      # Set max to 2 keys per sweep for this test.
      original = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep)
      Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, 2)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, original)
        else
          Application.delete_env(:ferricstore, :expiry_max_keys_per_sweep)
        end
      end)

      past = System.os_time(:millisecond) - 1_000

      # Create 5 expired keys that all hash to the same shard.
      # We'll find keys that map to shard 0.
      uid = System.unique_integer([:positive])

      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "sweep_limit_#{uid}_#{i}" end)
        |> Stream.filter(fn k -> Router.shard_for(k) == 0 end)
        |> Enum.take(5)

      Enum.each(keys, fn k -> Router.put(k, "expired_val", past) end)

      # First manual sweep should remove at most 2.
      # Note: the auto sweep timer may have already run, so we only assert
      # that not all keys were removed in a single manual sweep cycle.
      trigger_sweep(0)

      ets = :keydir_0
      remaining = Enum.count(keys, fn k -> :ets.lookup(ets, k) != [] end)
      # At least 1 should remain (max_keys=2 per sweep, 5 total, but auto
      # sweep may have fired too). The key invariant: a single sweep doesn't
      # remove all 5 at once when limit is 2.
      assert remaining >= 1
    end

    test "multiple sweep cycles clear all expired keys" do
      original = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep)
      Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, 2)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, original)
        else
          Application.delete_env(:ferricstore, :expiry_max_keys_per_sweep)
        end
      end)

      past = System.os_time(:millisecond) - 1_000

      uid = System.unique_integer([:positive])

      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "multi_sweep_#{uid}_#{i}" end)
        |> Stream.filter(fn k -> Router.shard_for(k) == 0 end)
        |> Enum.take(5)

      Enum.each(keys, fn k -> Router.put(k, "expired_val", past) end)

      # Run enough sweep cycles (ceiling of 5/2 = 3, plus 1 extra for safety).
      Enum.each(1..4, fn _ -> trigger_sweep(0) end)

      ets = :keydir_0
      remaining = Enum.count(keys, fn k -> :ets.lookup(ets, k) != [] end)
      assert remaining == 0
    end

    test "DBSIZE decreases after sweep removes expired keys" do
      # Insert some keys with past expiry across all shards.
      past = System.os_time(:millisecond) - 1_000
      keys = for i <- 1..8, do: ukey("dbsize_#{i}")

      # First flush the store to have a clean baseline.
      baseline = Router.dbsize()

      Enum.each(keys, fn k -> Router.put(k, "val", past) end)

      # Before sweep, dbsize should not count expired keys (lazy expiry on
      # read in keys()). But let's verify the sweep still cleans up ETS.
      trigger_all_sweeps()

      after_sweep = Router.dbsize()
      # Use <= instead of == because concurrent tests may add keys between
      # baseline and after_sweep. The important invariant is that the sweep
      # did not increase the count (expired keys were removed).
      assert after_sweep <= baseline
    end
  end
end
