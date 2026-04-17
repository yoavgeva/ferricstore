defmodule Ferricstore.Store.AsyncRmwTest do
  @moduledoc """
  TDD tests for the async RMW latch+worker design
  (see docs/async-rmw-design.md).

  Target behavior:
  - Uncontended RMW runs inline in caller process under per-key ETS latch
    (:ets.insert_new). No GenServer hop, ~15μs p50.
  - Contended RMW (another caller holds the latch) falls through to
    Ferricstore.Store.RmwCoordinator, which serializes via its mailbox.
  - Concurrent RMWs on the same key never lose updates; each caller gets
    a distinct, correctly-ordered result.
  - Router.async_submit replicates the DELTA command (e.g. {:incr, k, δ})
    so replicas apply in Raft log order for deterministic convergence.
  - Latch leaks on caller crash are cleaned up by a periodic sweep.
  - SET vs RMW on the same key is last-write-wins (async semantics).

  These tests will fail against the current quorum_write fallback path
  (commit 88ff185). They pass once the latch+worker design is in place.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @ns "rmw_async"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ctx, do: FerricStore.Instance.get(:default)
  defp ukey(base), do: "#{@ns}:#{base}_#{:erlang.unique_integer([:positive])}"

  # ---------------------------------------------------------------------------
  # Uncontended single-caller correctness — one assertion per RMW command
  # ---------------------------------------------------------------------------

  describe "uncontended RMW correctness" do
    test "INCR on nonexistent key returns delta and stores it" do
      k = ukey("incr_nokey")
      assert {:ok, 5} = Router.incr(ctx(), k, 5)
      assert Router.get(ctx(), k) == "5"
    end

    test "INCR on existing integer returns sum" do
      k = ukey("incr_existing")
      :ok = Router.put(ctx(), k, "10", 0)
      assert {:ok, 13} = Router.incr(ctx(), k, 3)
      assert Router.get(ctx(), k) == "13"
    end

    test "INCR on non-integer string returns error, leaves value unchanged" do
      k = ukey("incr_bad")
      :ok = Router.put(ctx(), k, "hello", 0)
      assert {:error, _msg} = Router.incr(ctx(), k, 1)
      assert Router.get(ctx(), k) == "hello"
    end

    test "APPEND on nonexistent key creates it, returns byte size" do
      k = ukey("append_nokey")
      assert {:ok, 5} = Router.append(ctx(), k, "hello")
      assert Router.get(ctx(), k) == "hello"
    end

    test "APPEND on existing value concatenates" do
      k = ukey("append_existing")
      :ok = Router.put(ctx(), k, "hello", 0)
      assert {:ok, 11} = Router.append(ctx(), k, " world")
      assert Router.get(ctx(), k) == "hello world"
    end

    test "GETSET returns old value and installs new" do
      k = ukey("getset")
      :ok = Router.put(ctx(), k, "old", 0)
      assert "old" = Router.getset(ctx(), k, "new")
      assert Router.get(ctx(), k) == "new"
    end

    test "GETSET on nonexistent returns nil and installs" do
      k = ukey("getset_new")
      assert nil == Router.getset(ctx(), k, "val")
      assert Router.get(ctx(), k) == "val"
    end

    test "GETDEL returns value and deletes key" do
      k = ukey("getdel")
      :ok = Router.put(ctx(), k, "value", 0)
      assert "value" = Router.getdel(ctx(), k)
      assert Router.get(ctx(), k) == nil
    end

    test "GETDEL on nonexistent returns nil" do
      k = ukey("getdel_new")
      assert nil == Router.getdel(ctx(), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Path selection — which path did each RMW take?
  # ---------------------------------------------------------------------------

  describe "path selection telemetry" do
    test "uncontended single RMW takes the latch path" do
      key = ukey("path_uncontended")
      :ok = Router.put(ctx(), key, "0", 0)

      handler_id = {:rmw_test, :uncontended_latch}

      _ =
        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :rmw, :latch],
            [:ferricstore, :rmw, :worker]
          ],
          fn event, _meas, _meta, test_pid ->
            send(test_pid, {:rmw_path, event})
          end,
          self()
        )

      try do
        {:ok, _} = Router.incr(ctx(), key, 1)
        assert_receive {:rmw_path, [:ferricstore, :rmw, :latch]}, 500
        refute_received {:rmw_path, [:ferricstore, :rmw, :worker]}
      after
        :telemetry.detach(handler_id)
      end
    end

    test "contended RMWs on the same key use the worker path" do
      key = ukey("path_contended")
      :ok = Router.put(ctx(), key, "0", 0)

      handler_id = {:rmw_test, :contended_worker}

      _ =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :rmw, :worker],
          fn _event, _meas, _meta, test_pid ->
            send(test_pid, :worker_path_taken)
          end,
          self()
        )

      try do
        # Many concurrent callers on the same key should force at least
        # one caller onto the worker path.
        tasks =
          for _ <- 1..50 do
            Task.async(fn -> Router.incr(ctx(), key, 1) end)
          end

        Task.await_many(tasks, 10_000)

        assert_receive :worker_path_taken, 2_000
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Atomicity under concurrency (the bug this whole design addresses)
  # ---------------------------------------------------------------------------

  describe "concurrent RMW atomicity" do
    test "50 concurrent INCRs on the same key sum to +50" do
      key = ukey("concurrent_50")
      :ok = Router.put(ctx(), key, "0", 0)

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> Router.incr(ctx(), key, 1) end)
        end

      results = Task.await_many(tasks, 10_000)

      # Every task got {:ok, N}; no errors.
      assert Enum.all?(results, fn
               {:ok, n} when is_integer(n) -> true
               _ -> false
             end)

      # The 50 returned values are a permutation of 1..50 (every apply
      # saw a distinct moment in the serialization order).
      ns =
        Enum.map(results, fn {:ok, n} -> n end)
        |> Enum.sort()

      assert ns == Enum.to_list(1..50)

      # Final ETS value reflects the total.
      assert Router.get(ctx(), key) == "50"
    end

    test "1000 INCRs from 25 concurrent tasks sum to 1000" do
      key = ukey("concurrent_1000")
      :ok = Router.put(ctx(), key, "0", 0)

      tasks =
        for _ <- 1..25 do
          Task.async(fn ->
            for _ <- 1..40 do
              Router.incr(ctx(), key, 1)
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      # Let replication settle — the Batcher flush timer needs to fire.
      :timer.sleep(200)

      assert Router.get(ctx(), key) == "1000"
    end

    test "concurrent APPENDs produce a string of the correct total length" do
      key = ukey("concurrent_append")

      tasks =
        for _ <- 1..20 do
          Task.async(fn -> Router.append(ctx(), key, "x") end)
        end

      Task.await_many(tasks, 10_000)

      final = Router.get(ctx(), key)
      assert is_binary(final)
      assert byte_size(final) == 20
      assert final == String.duplicate("x", 20)
    end

    test "concurrent distinct-key INCRs all succeed via latch path" do
      handler_id = {:rmw_test, :distinct_keys}

      counts = :counters.new(2, [:atomics])
      # counts[1] = latch count, counts[2] = worker count

      _ =
        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :rmw, :latch],
            [:ferricstore, :rmw, :worker]
          ],
          fn
            [:ferricstore, :rmw, :latch], _, _, c -> :counters.add(c, 1, 1)
            [:ferricstore, :rmw, :worker], _, _, c -> :counters.add(c, 2, 1)
          end,
          counts
        )

      try do
        tasks =
          for i <- 1..50 do
            Task.async(fn ->
              Router.incr(ctx(), ukey("distinct_#{i}"), 1)
            end)
          end

        Task.await_many(tasks, 10_000)

        latch_n = :counters.get(counts, 1)
        worker_n = :counters.get(counts, 2)

        # Distinct keys → near-zero contention → mostly latch.
        assert latch_n >= 40,
               "expected ≥40 latch path hits for 50 distinct keys, got #{latch_n}"
        assert worker_n <= 10,
               "expected ≤10 worker path hits for 50 distinct keys, got #{worker_n}"
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # SET + RMW interaction (last-write-wins)
  # ---------------------------------------------------------------------------

  describe "mixed SET + INCR" do
    test "concurrent SETs and INCRs on same key never crash; final value is valid" do
      key = ukey("mixed")
      :ok = Router.put(ctx(), key, "0", 0)

      setters =
        for i <- 1..25 do
          Task.async(fn -> Router.put(ctx(), key, "literal_#{i}", 0) end)
        end

      incrementers =
        for _ <- 1..25 do
          Task.async(fn -> Router.incr(ctx(), key, 1) end)
        end

      _set_results = Task.await_many(setters, 10_000)
      incr_results = Task.await_many(incrementers, 10_000)

      # All INCRs return either a valid integer or an error (because a SET
      # landed a non-integer in between). None crash.
      assert Enum.all?(incr_results, fn
               {:ok, n} when is_integer(n) -> true
               {:error, _msg} -> true
               _ -> false
             end)

      final = Router.get(ctx(), key)
      # Last write wins: could be an integer string (from INCR) or
      # "literal_N" (from SET). Don't assert which — just valid shape.
      assert is_binary(final)
    end
  end

  # ---------------------------------------------------------------------------
  # Latency budget
  # ---------------------------------------------------------------------------

  describe "latency" do
    @tag :latency
    test "uncontended async INCR p50 under 500μs" do
      # Generous threshold — the design targets ~15μs p50 but CI + ETS
      # write_concurrency contention with other tests can push this up.
      # 500μs is comfortably below the 2-7ms quorum baseline, confirming
      # the latch path is faster than the fallback.
      key_prefix = "lat_#{:erlang.unique_integer([:positive])}"

      for i <- 1..20 do
        Router.put(ctx(), "#{@ns}:#{key_prefix}_warm_#{i}", "0", 0)
        Router.incr(ctx(), "#{@ns}:#{key_prefix}_warm_#{i}", 1)
      end

      samples =
        for i <- 1..200 do
          key = "#{@ns}:#{key_prefix}_bench_#{i}"
          Router.put(ctx(), key, "0", 0)

          t0 = System.monotonic_time(:microsecond)
          {:ok, _} = Router.incr(ctx(), key, 1)
          System.monotonic_time(:microsecond) - t0
        end

      sorted = Enum.sort(samples)
      p50 = Enum.at(sorted, div(length(sorted), 2))
      p99 = Enum.at(sorted, trunc(length(sorted) * 0.99))

      assert p50 < 500,
             "async INCR p50 #{p50}μs exceeded 500μs budget " <>
               "(p99 #{p99}μs); latch path probably not engaged"
    end
  end

  # ---------------------------------------------------------------------------
  # Latch leak recovery on caller crash
  # ---------------------------------------------------------------------------

  describe "latch leak recovery" do
    @tag timeout: 15_000
    test "RMW on a key whose latch was leaked by a crashed caller recovers" do
      key = ukey("leak")
      :ok = Router.put(ctx(), key, "0", 0)

      # Start a task that acquires the latch and then crashes without
      # releasing it. We simulate this by spawning a linked process that
      # pokes the latch table directly (representing a mid-RMW crash).
      parent = self()

      {:ok, latch_tab} = latch_tab_for_key(ctx(), key)

      # Insert a fake latch owned by a now-dead pid to simulate a leak.
      dead_pid =
        spawn(fn ->
          # Briefly alive then exit. The caller could have died here mid-RMW.
          receive do
            :die -> :ok
          after
            10 -> :ok
          end
        end)

      # Wait for it to die.
      ref = Process.monitor(dead_pid)
      send(dead_pid, :die)
      assert_receive {:DOWN, ^ref, :process, ^dead_pid, _}, 500

      refute Process.alive?(dead_pid)
      :ets.insert(latch_tab, {key, dead_pid})

      # Now a new RMW on the same key should eventually recover (sweeper
      # removes the dead entry; then INCR succeeds). Sweeper runs every
      # 5s — allow up to 10s for recovery.
      Task.start(fn ->
        result = Router.incr(ctx(), key, 1)
        send(parent, {:recovered, result})
      end)

      assert_receive {:recovered, {:ok, _n}}, 10_000
    end
  end

  # ---------------------------------------------------------------------------
  # Worker crash graceful handling
  # ---------------------------------------------------------------------------

  describe "worker crash" do
    @tag timeout: 10_000
    test "killing the RmwCoordinator returns an error then recovers" do
      key = ukey("worker_crash")
      :ok = Router.put(ctx(), key, "0", 0)

      idx = Router.shard_for(ctx(), key)
      pid = Process.whereis(Ferricstore.Store.RmwCoordinator.name(idx))
      assert is_pid(pid)

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

      # Next RMW while worker is still restarting may return an error.
      # The error must be graceful (not a raise). Eventually, a retry
      # succeeds.
      result = retry_rmw(fn -> Router.incr(ctx(), key, 1) end, 100)
      assert match?({:ok, _}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp latch_tab_for_key(ctx, key) do
    idx = Router.shard_for(ctx, key)

    case Map.get(ctx, :latch_refs) do
      nil -> :error
      refs -> {:ok, elem(refs, idx)}
    end
  end

  defp retry_rmw(_fun, 0), do: {:error, :exhausted}
  defp retry_rmw(fun, n) do
    case fun.() do
      {:ok, _} = ok -> ok
      _ ->
        :timer.sleep(50)
        retry_rmw(fun, n - 1)
    end
  end
end
