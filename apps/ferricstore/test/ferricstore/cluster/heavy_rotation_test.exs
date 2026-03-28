defmodule Ferricstore.Cluster.HeavyRotationTest do
  @moduledoc """
  Heavy write-load Raft cluster tests during rolling member rotation.

  Unlike `RollingRotationTest` which writes a handful of keys between each
  stop/start cycle, these tests maintain **concurrent writer tasks** that push
  keys as fast as possible while members are being rotated. This simulates
  real production traffic where clients never pause.

  The writers use batched `ra.process_command` calls to maximize throughput.
  Each writer sends batches of 50 puts per ra command, achieving 20K+ successful
  key writes during the rotation window.

  ## Tests

  1. **5-node cluster, 20K+ writes during rolling rotation** --
     Concurrent writers run while all 5 members are rotated one-at-a-time.
     Expects >= 20,000 successful writes with high success rate.

  2. **3-node cluster, 20K+ writes during rolling rotation** --
     Same as above but with the minimum Raft cluster size. Quorum is
     2/3, meaning any single failure still allows writes, but leader
     elections have more impact since there is less headroom.

  3. **3-node dual failure (quorum loss)** --
     Stops 2 of 3 members, verifies writes timeout/fail, restores members,
     verifies baseline data survives. Acknowledges that timed-out writes
     may land once quorum is restored (Raft does not guarantee that a
     timed-out write was NOT applied).

  ## Running

      mix test test/ferricstore/cluster/heavy_rotation_test.exs --include cluster
  """

  use ExUnit.Case, async: false

  require Logger

  @moduletag :cluster

  @ra_timeout 10_000
  @machine {:module, Ferricstore.Test.KvMachine, %{}}

  # Batch size for the continuous writer. Each ra command applies this many puts.
  @batch_size 50

  # Number of parallel writer tasks.
  @writer_concurrency 4

  # ---------------------------------------------------------------------------
  # Setup / Teardown
  # ---------------------------------------------------------------------------

  setup_all do
    tmp_base =
      Path.join(
        System.tmp_dir!(),
        "ferric_heavy_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp_base)

    systems = [:heavy_5node_test, :heavy_3node_test, :heavy_3node_dual_test]

    for sys <- systems do
      data_dir = Path.join(tmp_base, "#{sys}")
      File.mkdir_p!(data_dir)

      names = :ra_system.derive_names(sys)

      config = %{
        name: sys,
        names: names,
        data_dir: to_charlist(data_dir),
        wal_data_dir: to_charlist(data_dir),
        segment_max_entries: 8192
      }

      case :ra_system.start(config) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end

    on_exit(fn ->
      prefixes = ["h5_member_", "h3_member_", "h3d_member_"]
      max_size = 5

      for {prefix, sys} <- Enum.zip(prefixes, systems), i <- 1..max_size do
        server_id = {:"#{prefix}#{i}", node()}

        try do
          :ra.stop_server(sys, server_id)
        catch
          _, _ -> :ok
        end

        try do
          :ra.force_delete_server(sys, server_id)
        catch
          _, _ -> :ok
        end
      end

      Process.sleep(500)
      File.rm_rf!(tmp_base)
    end)

    %{tmp_base: tmp_base}
  end

  # ---------------------------------------------------------------------------
  # Test 1: 5-node cluster with 20K+ writes during rolling rotation
  # ---------------------------------------------------------------------------

  describe "5-node heavy rotation" do
    @tag :cluster
    @tag timeout: 300_000
    test "continuous writes succeed through rolling restart of all 5 members" do
      ra_system = :heavy_5node_test
      cluster_size = 5
      members = for i <- 1..cluster_size, do: {:"h5_member_#{i}", node()}

      Logger.info("=== 5-node heavy rotation: forming cluster ===")

      {:ok, started, not_started} =
        :ra.start_cluster(ra_system, :heavy_5node_cluster, @machine, members)

      assert length(started) == cluster_size,
             "All #{cluster_size} members should start, not_started: #{inspect(not_started)}"

      leader = wait_for_stable_leader(members)
      Logger.info("[5-node] Leader elected: #{inspect(leader)}")

      # Phase 2: write baseline keys using batches for speed
      Logger.info("[5-node] Writing 5,000 baseline keys")
      write_baseline_batched(leader, "base", 5_000)
      Logger.info("[5-node] Baseline complete")

      # Phase 3: start concurrent writers, then do rolling rotation
      leader_ref = :atomics.new(1, [])
      # Store leader index in atomics for the writers to read
      # We'll use an Agent instead for the mutable leader reference.
      {:ok, leader_agent} = Agent.start_link(fn -> leader end)

      stop_flag = :atomics.new(1, [])
      # 0 = running, 1 = stop

      start_time = System.monotonic_time(:millisecond)

      writer_tasks =
        for w <- 1..@writer_concurrency do
          Task.async(fn ->
            writer_loop(
              ra_system,
              members,
              leader_agent,
              stop_flag,
              _writer_id = w,
              _counter = 0,
              _successes = 0,
              _failures = 0,
              _elections = 0,
              _keys = []
            )
          end)
        end

      Logger.info("[5-node] Started #{@writer_concurrency} writer tasks")

      # Rolling restart while writers are active
      Logger.info("[5-node] Starting rolling rotation")

      for i <- 1..cluster_size do
        target = Enum.at(members, i - 1)
        remaining = List.delete(members, target)

        Logger.info("[5-node] Rotation #{i}/#{cluster_size}: stopping #{inspect(target)}")

        :ok = :ra.stop_server(ra_system, target)
        Process.sleep(300)

        new_leader = wait_for_stable_leader(remaining, 10_000)
        Agent.update(leader_agent, fn _ -> new_leader end)

        # Keep member down to accumulate writes during degraded state
        Process.sleep(2_000)

        restart_result = :ra.restart_server(ra_system, target)

        assert restart_result in [:ok, {:error, :already_started}],
               "Restart failed: #{inspect(restart_result)}"

        wait_for_member_alive(target, 15_000)
        Logger.info("[5-node] Rotation #{i}/#{cluster_size}: #{inspect(target)} back")

        # Update leader after restart in case it changed
        updated_leader = wait_for_stable_leader(members)
        Agent.update(leader_agent, fn _ -> updated_leader end)

        Process.sleep(300)
      end

      # Phase 4: stop writers
      Logger.info("[5-node] Stopping writers")
      :atomics.put(stop_flag, 1, 1)

      results = Enum.map(writer_tasks, fn task -> Task.await(task, 30_000) end)

      duration_ms = System.monotonic_time(:millisecond) - start_time

      Agent.stop(leader_agent)

      # Aggregate results
      {total_successes, total_failures, total_elections, all_keys} =
        Enum.reduce(results, {0, 0, 0, []}, fn {s, f, e, keys}, {as, af, ae, ak} ->
          {as + s, af + f, ae + e, keys ++ ak}
        end)

      total_attempted = total_successes + total_failures

      success_rate =
        if total_attempted > 0, do: total_successes / total_attempted * 100, else: 0

      throughput =
        if duration_ms > 0,
          do: Float.round(total_successes / (duration_ms / 1_000), 1),
          else: 0

      report = """

      === 5-node Heavy Rotation Results ===
        Cluster size: #{cluster_size} members
        Writer tasks: #{@writer_concurrency}
        Writer duration: #{Float.round(duration_ms / 1_000, 1)} seconds
        Total writes attempted: #{total_attempted}
        Successful writes: #{total_successes}
        Failed writes: #{total_failures}
        Success rate: #{Float.round(success_rate, 2)}%
        Throughput: #{throughput} writes/sec
        Leader elections observed: #{total_elections}
      """

      Logger.info(report)

      assert total_successes >= 20_000,
             "Expected >= 20,000 successful writes, got #{total_successes}."

      assert success_rate > 90.0,
             "Expected > 90% success rate, got #{Float.round(success_rate, 2)}%"

      # Phase 5: verify ALL successful writes on the cluster
      Logger.info("[5-node] Verifying #{total_successes} successful writes")

      current_leader = wait_for_stable_leader(members)
      state = query_leader_via(current_leader)

      # Verify baseline
      for i <- 1..5_000 do
        key = "base:#{i}"
        assert Map.get(state, key) == "v#{i}", "Missing baseline key #{key}"
      end

      # Verify every successful heavy-write key
      missing_keys =
        Enum.reduce(all_keys, [], fn key, acc ->
          # Key format: "heavy:W:C" -> value format: "val:W:C"
          expected_value = "val:" <> String.replace_prefix(key, "heavy:", "")

          case Map.get(state, key) do
            ^expected_value -> acc
            _other -> [key | acc]
          end
        end)

      assert missing_keys == [],
             "#{length(missing_keys)} keys claimed successful but missing. " <>
               "First 10: #{inspect(Enum.take(missing_keys, 10))}"

      # Verify on a non-leader member via local query.
      # The follower may still be applying the Raft log, so retry with backoff.
      non_leader = members |> List.delete(current_leader) |> hd()

      wait_until_local_consistent(non_leader, all_keys, 30_000)

      Logger.info(
        "[5-node] Data integrity: #{total_successes}/#{total_successes} keys verified"
      )

      Logger.info("=== 5-node heavy rotation PASSED ===")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 2: 3-node cluster with 20K+ writes during rolling rotation
  # ---------------------------------------------------------------------------

  describe "3-node heavy rotation" do
    @tag :cluster
    @tag timeout: 300_000
    test "continuous writes succeed through rolling restart of all 3 members" do
      ra_system = :heavy_3node_test
      cluster_size = 3
      members = for i <- 1..cluster_size, do: {:"h3_member_#{i}", node()}

      Logger.info("=== 3-node heavy rotation: forming cluster ===")

      {:ok, started, not_started} =
        :ra.start_cluster(ra_system, :heavy_3node_cluster, @machine, members)

      assert length(started) == cluster_size,
             "All #{cluster_size} members should start, not_started: #{inspect(not_started)}"

      leader = wait_for_stable_leader(members)
      Logger.info("[3-node] Leader elected: #{inspect(leader)}")

      # Phase 2: baseline writes
      Logger.info("[3-node] Writing 5,000 baseline keys")
      write_baseline_batched(leader, "base", 5_000)
      Logger.info("[3-node] Baseline complete")

      # Phase 3: concurrent writers + rolling rotation
      {:ok, leader_agent} = Agent.start_link(fn -> leader end)
      stop_flag = :atomics.new(1, [])

      start_time = System.monotonic_time(:millisecond)

      writer_tasks =
        for w <- 1..@writer_concurrency do
          Task.async(fn ->
            writer_loop(
              ra_system,
              members,
              leader_agent,
              stop_flag,
              w,
              0,
              0,
              0,
              0,
              []
            )
          end)
        end

      Logger.info("[3-node] Started #{@writer_concurrency} writer tasks")
      Logger.info("[3-node] Starting rolling rotation")

      for i <- 1..cluster_size do
        target = Enum.at(members, i - 1)
        remaining = List.delete(members, target)

        Logger.info("[3-node] Rotation #{i}/#{cluster_size}: stopping #{inspect(target)}")

        :ok = :ra.stop_server(ra_system, target)
        Process.sleep(500)

        new_leader = wait_for_stable_leader(remaining, 15_000)
        Agent.update(leader_agent, fn _ -> new_leader end)

        # Longer degraded window for 3-node to stress bare-minimum quorum
        Process.sleep(3_000)

        restart_result = :ra.restart_server(ra_system, target)

        assert restart_result in [:ok, {:error, :already_started}],
               "Restart failed: #{inspect(restart_result)}"

        wait_for_member_alive(target, 15_000)
        Logger.info("[3-node] Rotation #{i}/#{cluster_size}: #{inspect(target)} back")

        updated_leader = wait_for_stable_leader(members)
        Agent.update(leader_agent, fn _ -> updated_leader end)

        Process.sleep(500)
      end

      # Phase 4: stop writers
      Logger.info("[3-node] Stopping writers")
      :atomics.put(stop_flag, 1, 1)

      results = Enum.map(writer_tasks, fn task -> Task.await(task, 30_000) end)
      duration_ms = System.monotonic_time(:millisecond) - start_time
      Agent.stop(leader_agent)

      {total_successes, total_failures, total_elections, all_keys} =
        Enum.reduce(results, {0, 0, 0, []}, fn {s, f, e, keys}, {as, af, ae, ak} ->
          {as + s, af + f, ae + e, keys ++ ak}
        end)

      total_attempted = total_successes + total_failures

      success_rate =
        if total_attempted > 0, do: total_successes / total_attempted * 100, else: 0

      throughput =
        if duration_ms > 0,
          do: Float.round(total_successes / (duration_ms / 1_000), 1),
          else: 0

      report = """

      === 3-node Heavy Rotation Results ===
        Cluster size: #{cluster_size} members
        Writer tasks: #{@writer_concurrency}
        Writer duration: #{Float.round(duration_ms / 1_000, 1)} seconds
        Total writes attempted: #{total_attempted}
        Successful writes: #{total_successes}
        Failed writes: #{total_failures}
        Success rate: #{Float.round(success_rate, 2)}%
        Throughput: #{throughput} writes/sec
        Leader elections observed: #{total_elections}
      """

      Logger.info(report)

      assert total_successes >= 20_000,
             "Expected >= 20,000 successful writes, got #{total_successes}."

      assert success_rate > 85.0,
             "Expected > 85% success rate, got #{Float.round(success_rate, 2)}%"

      # Phase 5: verify data integrity
      Logger.info("[3-node] Verifying #{total_successes} successful writes")

      current_leader = wait_for_stable_leader(members)
      state = query_leader_via(current_leader)

      # Verify baseline
      for i <- 1..5_000 do
        key = "base:#{i}"
        assert Map.get(state, key) == "v#{i}", "Missing baseline key #{key}"
      end

      # Verify continuous writes
      missing_keys =
        Enum.reduce(all_keys, [], fn key, acc ->
          expected_value = "val:" <> String.replace_prefix(key, "heavy:", "")

          case Map.get(state, key) do
            ^expected_value -> acc
            _other -> [key | acc]
          end
        end)

      assert missing_keys == [],
             "#{length(missing_keys)} keys claimed successful but missing. " <>
               "First 10: #{inspect(Enum.take(missing_keys, 10))}"

      # Verify on all members. Followers may still be applying the Raft log,
      # so retry with backoff.
      for member <- members do
        wait_until_local_consistent(member, all_keys, 30_000)
      end

      Logger.info(
        "[3-node] Data integrity: #{total_successes}/#{total_successes} keys verified on all members"
      )

      Logger.info("=== 3-node heavy rotation PASSED ===")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 3: 3-node dual failure (quorum loss)
  # ---------------------------------------------------------------------------

  describe "3-node dual failure" do
    @tag :cluster
    @tag timeout: 120_000
    test "writes fail when 2 of 3 members are down, no data loss on recovery" do
      ra_system = :heavy_3node_dual_test
      cluster_size = 3
      members = for i <- 1..cluster_size, do: {:"h3d_member_#{i}", node()}

      Logger.info("=== 3-node dual failure: forming cluster ===")

      {:ok, started, not_started} =
        :ra.start_cluster(ra_system, :heavy_3node_dual_cluster, @machine, members)

      assert length(started) == cluster_size,
             "All #{cluster_size} members should start, not_started: #{inspect(not_started)}"

      leader = wait_for_stable_leader(members)
      Logger.info("[3-node-dual] Leader elected: #{inspect(leader)}")

      # Write baseline data that must survive the dual failure
      Logger.info("[3-node-dual] Writing 1,000 baseline keys")
      write_baseline_batched(leader, "dual", 1_000)
      Logger.info("[3-node-dual] Baseline written")

      # Determine which members to stop. We specifically stop the leader
      # plus one follower so the sole survivor cannot commit anything.
      non_leaders = List.delete(members, leader)
      victim_follower = hd(non_leaders)
      sole_survivor = List.last(non_leaders)

      Logger.info(
        "[3-node-dual] Stopping leader #{inspect(leader)} and follower #{inspect(victim_follower)}"
      )

      :ok = :ra.stop_server(ra_system, leader)
      :ok = :ra.stop_server(ra_system, victim_follower)

      # Wait long enough for the sole survivor to detect the failures
      # and attempt (and fail) leader election
      Process.sleep(3_000)

      # Writes should fail or timeout -- the sole survivor cannot form quorum
      write_results =
        for i <- 1..10 do
          try do
            case :ra.process_command(sole_survivor, {:put, "noquorum:#{i}", "x"}, 2_000) do
              {:ok, _, _} -> :ok
              {:error, reason} -> {:error, reason}
              {:timeout, _} -> :timeout
            end
          catch
            :exit, reason -> {:exit, reason}
          end
        end

      successes = Enum.count(write_results, &(&1 == :ok))
      non_successes = Enum.count(write_results, &(&1 != :ok))

      Logger.info(
        "[3-node-dual] No-quorum writes: #{successes} returned ok, #{non_successes} failed/timed out"
      )

      # The key assertion: the sole survivor should NOT be able to confirm
      # writes because it cannot achieve quorum. With the leader dead,
      # the survivor is a follower and cannot accept writes.
      assert successes == 0,
             "Expected 0 successful writes without quorum, got #{successes}. " <>
               "Results: #{inspect(write_results)}"

      # Restore follower first -- quorum restored (2/3)
      Logger.info("[3-node-dual] Restoring #{inspect(victim_follower)} to restore quorum")

      :ra.restart_server(ra_system, victim_follower)
      wait_for_member_alive(victim_follower, 15_000)
      Process.sleep(2_000)

      recovery_leader =
        wait_for_stable_leader([victim_follower, sole_survivor], 15_000)

      # Writes should work again
      {:ok, _, _} =
        :ra.process_command(recovery_leader, {:put, "recovered:1", "yes"}, @ra_timeout)

      Logger.info("[3-node-dual] Writes restored after quorum recovery")

      # Restore the original leader
      :ra.restart_server(ra_system, leader)
      wait_for_member_alive(leader, 15_000)
      Process.sleep(1_000)

      # Verify no baseline data loss
      full_leader = wait_for_stable_leader(members)
      state = query_leader_via(full_leader)

      missing_baseline =
        Enum.count(1..1_000, fn i ->
          Map.get(state, "dual:#{i}") != "v#{i}"
        end)

      assert missing_baseline == 0,
             "#{missing_baseline} baseline keys lost during dual failure"

      # The recovered write must be present
      assert Map.get(state, "recovered:1") == "yes",
             "Post-recovery write missing"

      # Verify on all members via local query. Followers may still be
      # applying the Raft log after recovery, so retry with backoff.
      dual_keys = Enum.map(1..1_000, &"dual:#{&1}")

      for member <- members do
        wait_until_local_consistent(
          member,
          dual_keys,
          30_000,
          fn key ->
            i = key |> String.split(":") |> List.last() |> String.to_integer()
            "v#{i}"
          end
        )
      end

      report = """

      === 3-node Dual Failure Results ===
        Cluster size: #{cluster_size} members
        Baseline keys: 1,000
        No-quorum write attempts: #{length(write_results)}
        No-quorum successes: #{successes} (expected 0)
        No-quorum failures/timeouts: #{non_successes}
        Baseline data loss: #{missing_baseline} keys
        All #{cluster_size} members verified consistent after recovery
      """

      Logger.info(report)

      Logger.info("=== 3-node dual failure PASSED ===")
    end
  end

  # ---------------------------------------------------------------------------
  # Writer loop (runs inside Task)
  # ---------------------------------------------------------------------------

  # Each writer sends batched puts to maximize throughput. A batch of 50 puts
  # is a single ra command, so we get 50 key writes per round trip.
  defp writer_loop(
         ra_system,
         members,
         leader_agent,
         stop_flag,
         writer_id,
         counter,
         successes,
         failures,
         elections,
         confirmed_keys
       ) do
    # Check stop flag
    if :atomics.get(stop_flag, 1) == 1 do
      {successes, failures, elections, confirmed_keys}
    else
      current_leader = Agent.get(leader_agent, & &1)

      # Build a batch of puts
      batch_cmds =
        for c <- counter..(counter + @batch_size - 1) do
          {:put, "heavy:#{writer_id}:#{c}", "val:#{writer_id}:#{c}"}
        end

      batch_keys =
        for c <- counter..(counter + @batch_size - 1) do
          "heavy:#{writer_id}:#{c}"
        end

      case safe_process_command(current_leader, {:batch, batch_cmds}) do
        {:ok, _reply, new_leader} ->
          # Update leader if it changed
          if new_leader != current_leader do
            Agent.update(leader_agent, fn _ -> new_leader end)
          end

          writer_loop(
            ra_system,
            members,
            leader_agent,
            stop_flag,
            writer_id,
            counter + @batch_size,
            successes + @batch_size,
            failures,
            elections,
            batch_keys ++ confirmed_keys
          )

        {:error, _reason} ->
          # Leader changed or unavailable. Try to find a new one.
          new_elections = maybe_find_leader(members, leader_agent, current_leader)
          Process.sleep(10)

          writer_loop(
            ra_system,
            members,
            leader_agent,
            stop_flag,
            writer_id,
            counter + @batch_size,
            successes,
            failures + @batch_size,
            elections + new_elections,
            confirmed_keys
          )

        :timeout ->
          new_elections = maybe_find_leader(members, leader_agent, current_leader)

          writer_loop(
            ra_system,
            members,
            leader_agent,
            stop_flag,
            writer_id,
            counter + @batch_size,
            successes,
            failures + @batch_size,
            elections + new_elections,
            confirmed_keys
          )
      end
    end
  end

  # Wraps ra.process_command with error handling to avoid crashing the writer task.
  defp safe_process_command(leader, cmd) do
    try do
      case :ra.process_command(leader, cmd, 5_000) do
        {:ok, reply, new_leader} -> {:ok, reply, new_leader}
        {:error, reason} -> {:error, reason}
        {:timeout, _} -> :timeout
      end
    catch
      :exit, _reason -> {:error, :exit}
    end
  end

  # Attempts to find and update the leader. Returns 1 if a new leader was found, 0 otherwise.
  defp maybe_find_leader(members, leader_agent, old_leader) do
    result =
      Enum.find_value(members, fn member ->
        try do
          case :ra.members(member, 2_000) do
            {:ok, _members, leader} when is_tuple(leader) -> leader
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end
      end)

    case result do
      nil ->
        0

      ^old_leader ->
        0

      new_leader ->
        Agent.update(leader_agent, fn _ -> new_leader end)
        1
    end
  end

  # ---------------------------------------------------------------------------
  # Baseline write helper
  # ---------------------------------------------------------------------------

  # Writes `count` keys using batches of @batch_size for speed.
  defp write_baseline_batched(leader, prefix, count) do
    1..count
    |> Enum.chunk_every(@batch_size)
    |> Enum.each(fn chunk ->
      batch_cmds =
        Enum.map(chunk, fn i ->
          {:put, "#{prefix}:#{i}", "v#{i}"}
        end)

      {:ok, _, _} = :ra.process_command(leader, {:batch, batch_cmds}, @ra_timeout)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp wait_for_stable_leader(members, timeout_ms \\ @ra_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_leader(members, deadline)
  end

  defp do_wait_leader(members, deadline) do
    result =
      Enum.find_value(members, fn member ->
        try do
          case :ra.members(member, 2_000) do
            {:ok, _members, leader} when is_tuple(leader) -> leader
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end
      end)

    cond do
      result != nil ->
        result

      System.monotonic_time(:millisecond) > deadline ->
        raise "Timeout waiting for leader among #{inspect(members)}"

      true ->
        Process.sleep(100)
        do_wait_leader(members, deadline)
    end
  end

  defp query_leader_via(member) do
    query_fn = fn state -> state end

    case :ra.consistent_query(member, query_fn, @ra_timeout) do
      {:ok, {_, state}, _} ->
        state

      {:ok, state, _} when is_map(state) ->
        state

      {:timeout, _} ->
        case :ra.consistent_query(member, query_fn, @ra_timeout * 2) do
          {:ok, {_, state}, _} -> state
          {:ok, state, _} when is_map(state) -> state
          other -> raise "Consistent query failed on #{inspect(member)}: #{inspect(other)}"
        end

      {:error, reason} ->
        raise "Consistent query failed on #{inspect(member)}: #{inspect(reason)}"
    end
  end

  defp query_local(member) do
    query_fn = fn state -> state end

    case :ra.local_query(member, query_fn, @ra_timeout) do
      {:ok, {_, state}, _} ->
        state

      {:ok, state, _} when is_map(state) ->
        state

      {:error, reason} ->
        raise "Local query failed on #{inspect(member)}: #{inspect(reason)}"
    end
  end

  defp wait_for_member_alive(member, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_alive(member, deadline)
  end

  defp do_wait_alive(member, deadline) do
    alive =
      try do
        case :ra.members(member, 2_000) do
          {:ok, _members, _leader} -> true
          _ -> false
        end
      catch
        :exit, _ -> false
      end

    cond do
      alive ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        raise "Timeout waiting for #{inspect(member)} to become alive"

      true ->
        Process.sleep(200)
        do_wait_alive(member, deadline)
    end
  end

  # Waits until a local query on `member` contains all expected keys.
  # `value_fn` defaults to deriving the expected value from the "heavy:W:C" key format.
  defp wait_until_local_consistent(member, keys, timeout_ms, value_fn \\ nil) do
    value_fn = value_fn || fn key ->
      "val:" <> String.replace_prefix(key, "heavy:", "")
    end

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_local_consistent(member, keys, value_fn, deadline)
  end

  defp do_wait_local_consistent(member, keys, value_fn, deadline) do
    state = query_local(member)

    missing =
      Enum.count(keys, fn key ->
        Map.get(state, key) != value_fn.(key)
      end)

    cond do
      missing == 0 ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        assert missing == 0,
               "#{missing} keys missing on #{inspect(member)} after timeout"

      true ->
        Process.sleep(500)
        do_wait_local_consistent(member, keys, value_fn, deadline)
    end
  end
end
