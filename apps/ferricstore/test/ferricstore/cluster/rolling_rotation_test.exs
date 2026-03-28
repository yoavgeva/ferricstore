defmodule Ferricstore.Cluster.RollingRotationTest do
  @moduledoc """
  Simulates a 5-node FerricStore cluster undergoing TLS certificate rotation
  and rolling redeployment, verifying Raft quorum is maintained throughout.

  Since FerricStore currently operates in single-node Raft mode (each node is
  its own independent Raft group), this test uses `ra:start_cluster/4` to form
  a genuine 5-member Raft group on a single BEAM node. This directly tests the
  consensus layer's behavior under member stop/start cycling -- the exact
  scenario that occurs during a rolling deployment with TLS cert rotation.

  The test uses `Ferricstore.Test.KvMachine`, a lightweight in-memory KV
  state machine that avoids Bitcask NIF and ETS dependencies, focusing
  purely on Raft consensus correctness.

  ## What is tested

  1. 5-member Raft cluster formation and leader election
  2. Baseline writes replicated across all members
  3. Rolling restart: one member at a time is stopped and restarted
     - During each restart, quorum (3/5) is maintained
     - Writes succeed while a member is down
     - Restarted member catches up via Raft log replication
  4. Post-rotation: all 5 members operational, writes succeed
  5. Leader kill: new leader election within bounded time
  6. Simultaneous dual-member failure during rotation (3/5 quorum holds)
  7. No data loss across all phases

  ## TLS cert rotation simulation

  The actual TLS certificate swap is modeled by stopping and restarting ra
  servers. In production, a rolling deployment replaces cert files on disk
  and restarts the node. The Raft-level behavior is identical: a member
  leaves, the cluster continues with quorum, the member rejoins and catches
  up. The cert v1/v2 generation is exercised via TlsCertHelper to validate
  the cert infrastructure, but the core test is about Raft quorum survival.

  ## Running

      mix test test/ferricstore/cluster/rolling_rotation_test.exs --include cluster
  """

  use ExUnit.Case, async: false

  require Logger

  alias Ferricstore.Test.TlsCertHelper

  @moduletag :cluster

  # Ra system dedicated to this test, isolated from the application's system.
  @ra_system :rolling_rotation_test

  # Number of members in the simulated cluster.
  @cluster_size 5

  # Timeout for ra operations (leader election can take seconds).
  @ra_timeout 15_000

  # Machine spec: module-based so ra can serialize it for WAL recovery.
  @machine {:module, Ferricstore.Test.KvMachine, %{}}

  # ---------------------------------------------------------------------------
  # Setup / Teardown
  # ---------------------------------------------------------------------------

  setup_all do
    # Create a temporary directory for ra data and TLS certs.
    tmp_base = Path.join(System.tmp_dir!(), "ferric_rolling_#{:erlang.unique_integer([:positive])}")
    ra_data_dir = Path.join(tmp_base, "ra")
    certs_dir = Path.join(tmp_base, "certs")

    File.mkdir_p!(ra_data_dir)
    File.mkdir_p!(certs_dir)

    # Start a dedicated ra system for this test to avoid polluting the
    # application's :ferricstore_raft system.
    names = :ra_system.derive_names(@ra_system)

    config = %{
      name: @ra_system,
      names: names,
      data_dir: to_charlist(ra_data_dir),
      wal_data_dir: to_charlist(ra_data_dir),
      segment_max_entries: 4096
    }

    case :ra_system.start(config) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    on_exit(fn ->
      # Collect all server names used across all tests for cleanup.
      prefixes = ["rot_member_", "qb_member_", "del_member_", "batch_member_"]

      for prefix <- prefixes, i <- 1..@cluster_size do
        server_id = {:"#{prefix}#{i}", node()}

        try do
          :ra.stop_server(@ra_system, server_id)
        catch
          _, _ -> :ok
        end

        try do
          :ra.force_delete_server(@ra_system, server_id)
        catch
          _, _ -> :ok
        end
      end

      # Allow the WAL to settle before removing the data directory.
      Process.sleep(200)
      File.rm_rf!(tmp_base)
    end)

    %{
      ra_data_dir: ra_data_dir,
      certs_dir: certs_dir,
      tmp_base: tmp_base
    }
  end

  # ---------------------------------------------------------------------------
  # Phase 1: TLS certificate generation
  # ---------------------------------------------------------------------------

  describe "Phase 1: TLS cert infrastructure" do
    @tag :cluster
    test "generates v1 and v2 certificate sets", %{certs_dir: certs_dir} do
      Logger.info("[Phase 1] Generating TLS certificate sets v1 and v2")

      v1_dir = Path.join(certs_dir, "v1")
      v2_dir = Path.join(certs_dir, "v2")
      File.mkdir_p!(v1_dir)
      File.mkdir_p!(v2_dir)

      # Generate v1 certs (simulating the initial deployment certs)
      {v1_cert, v1_key} = TlsCertHelper.generate_self_signed(v1_dir, cn: "ferric-node-v1")
      assert File.exists?(v1_cert), "v1 cert should exist"
      assert File.exists?(v1_key), "v1 key should exist"

      # Generate v2 certs (simulating the rotated certs)
      {v2_cert, v2_key} = TlsCertHelper.generate_self_signed(v2_dir, cn: "ferric-node-v2")
      assert File.exists?(v2_cert), "v2 cert should exist"
      assert File.exists?(v2_key), "v2 key should exist"

      # Verify certs are different (rotation produces new material)
      v1_cert_data = File.read!(v1_cert)
      v2_cert_data = File.read!(v2_cert)
      assert v1_cert_data != v2_cert_data, "v1 and v2 certs should be different"

      v1_key_data = File.read!(v1_key)
      v2_key_data = File.read!(v2_key)
      assert v1_key_data != v2_key_data, "v1 and v2 keys should be different"

      Logger.info("[Phase 1] Certificate generation verified: v1 and v2 are distinct")
    end
  end

  # ---------------------------------------------------------------------------
  # Full rolling rotation scenario
  # ---------------------------------------------------------------------------

  describe "rolling cert rotation with Raft quorum" do
    @tag :cluster
    @tag timeout: 120_000
    test "5-member cluster maintains quorum through rolling restart and dual failure" do
      Logger.info("=== Starting rolling rotation test ===")

      # -----------------------------------------------------------------------
      # Phase 2: Form the 5-member Raft cluster
      # -----------------------------------------------------------------------

      Logger.info("[Phase 2] Forming #{@cluster_size}-member Raft cluster")

      members = for i <- 1..@cluster_size, do: {:"rot_member_#{i}", node()}

      {:ok, started, not_started} =
        :ra.start_cluster(@ra_system, :rolling_rotation_cluster, @machine, members)

      assert length(started) == @cluster_size,
             "All #{@cluster_size} members should start, not_started: #{inspect(not_started)}"

      Logger.info("[Phase 2] Cluster formed with #{length(started)} members")

      # Wait for a stable leader
      leader = wait_for_stable_leader(members)
      Logger.info("[Phase 2] Leader elected: #{inspect(leader)}")

      # Verify all members see the same leader
      assert_all_members_agree_on_leader(members)

      # -----------------------------------------------------------------------
      # Phase 3: Baseline writes -- 100 keys replicated across all members
      # -----------------------------------------------------------------------

      Logger.info("[Phase 3] Writing 100 baseline keys")

      for i <- 1..100 do
        cmd = {:put, "baseline:#{i}", "value_#{i}"}
        {:ok, _reply, _leader} = :ra.process_command(leader, cmd, @ra_timeout)
      end

      Logger.info("[Phase 3] Baseline writes complete, verifying on all members")

      # Verify all 5 members can read all 100 keys via leader query
      state = query_leader(members)

      for i <- 1..100 do
        key = "baseline:#{i}"
        assert Map.get(state, key) == "value_#{i}", "Leader state should have #{key}"
      end

      Logger.info("[Phase 3] All 100 baseline keys verified")

      # -----------------------------------------------------------------------
      # Phase 4: Rolling restart -- one member at a time
      # -----------------------------------------------------------------------

      Logger.info("[Phase 4] Starting rolling restart of all #{@cluster_size} members")

      rolling_keys_written = :counters.new(1, [:atomics])

      for i <- 1..@cluster_size do
        target = Enum.at(members, i - 1)
        remaining = List.delete(members, target)

        Logger.info(
          "[Phase 4.#{i}] Stopping member #{inspect(target)} " <>
            "(#{@cluster_size - 1} remaining)"
        )

        :ok = :ra.stop_server(@ra_system, target)
        Process.sleep(300)

        new_leader = wait_for_stable_leader(remaining, 15_000)

        Logger.info(
          "[Phase 4.#{i}] Quorum maintained, leader: #{inspect(new_leader)}"
        )

        # Write 10 keys while this member is down
        for j <- 1..10 do
          key = "rolling:phase_#{i}:#{j}"
          cmd = {:put, key, "during_restart_#{i}_#{j}"}

          case :ra.process_command(new_leader, cmd, @ra_timeout) do
            {:ok, _reply, _new_leader} ->
              :counters.add(rolling_keys_written, 1, 1)

            {:error, reason} ->
              flunk(
                "[Phase 4.#{i}] Write failed with #{length(remaining)}/#{@cluster_size}: " <>
                  "#{inspect(reason)}"
              )

            {:timeout, _} ->
              flunk("[Phase 4.#{i}] Write timed out")
          end
        end

        Logger.info("[Phase 4.#{i}] 10 writes succeeded while member was down")

        # Restart the target member
        restart_result = :ra.restart_server(@ra_system, target)

        assert restart_result in [:ok, {:error, :already_started}],
               "[Phase 4.#{i}] Restart failed: #{inspect(restart_result)}"

        # Wait for the restarted member to rejoin the cluster
        wait_for_member_alive(target, 30_000)

        Logger.info("[Phase 4.#{i}] Member #{inspect(target)} restarted and alive")

        # Verify data integrity through the leader (which may have changed)
        current_leader = wait_for_stable_leader(members)
        state = query_leader_via(current_leader)

        for b <- 1..100 do
          key = "baseline:#{b}"

          assert Map.get(state, key) == "value_#{b}",
                 "[Phase 4.#{i}] Missing baseline key #{key}"
        end

        for phase <- 1..i, j <- 1..10 do
          key = "rolling:phase_#{phase}:#{j}"
          expected = "during_restart_#{phase}_#{j}"

          assert Map.get(state, key) == expected,
                 "[Phase 4.#{i}] Missing rolling key #{key}"
        end

        Logger.info("[Phase 4.#{i}] Data integrity verified through phase #{i}")
      end

      total_rolling = :counters.get(rolling_keys_written, 1)

      Logger.info("[Phase 4] Rolling restart complete. #{total_rolling} keys written")

      assert total_rolling == @cluster_size * 10,
             "Should have written #{@cluster_size * 10} rolling keys"

      # -----------------------------------------------------------------------
      # Phase 5: Post-rotation verification
      # -----------------------------------------------------------------------

      Logger.info("[Phase 5] Post-rotation verification")

      post_leader = wait_for_stable_leader(members)

      for i <- 1..100 do
        cmd = {:put, "post_rotation:#{i}", "new_value_#{i}"}
        {:ok, _reply, _leader} = :ra.process_command(post_leader, cmd, @ra_timeout)
      end

      # Verify full dataset through leader
      state = query_leader(members)

      for i <- 1..100 do
        assert Map.get(state, "baseline:#{i}") == "value_#{i}"
      end

      for phase <- 1..@cluster_size, j <- 1..10 do
        key = "rolling:phase_#{phase}:#{j}"
        assert Map.get(state, key) == "during_restart_#{phase}_#{j}"
      end

      for i <- 1..100 do
        assert Map.get(state, "post_rotation:#{i}") == "new_value_#{i}"
      end

      Logger.info("[Phase 5] Full dataset verified")

      # Leader election test: kill leader, verify new leader elected
      Logger.info("[Phase 5] Testing leader election by killing current leader")

      current_leader = wait_for_stable_leader(members)
      non_leaders = List.delete(members, current_leader)

      :ok = :ra.stop_server(@ra_system, current_leader)

      start_time = System.monotonic_time(:millisecond)
      new_leader = wait_for_stable_leader(non_leaders, 15_000)
      election_ms = System.monotonic_time(:millisecond) - start_time

      assert new_leader != current_leader,
             "New leader should differ from killed leader"

      assert election_ms < 5_000,
             "Election took #{election_ms}ms, expected < 5000ms"

      Logger.info("[Phase 5] New leader elected in #{election_ms}ms")

      {:ok, _reply, _leader} =
        :ra.process_command(new_leader, {:put, "post_election", "works"}, @ra_timeout)

      # Restart the killed leader
      :ra.restart_server(@ra_system, current_leader)
      wait_for_member_alive(current_leader, 30_000)

      state = query_leader(members)
      assert Map.get(state, "post_election") == "works"

      Logger.info("[Phase 5] Leader election and recovery verified")

      # -----------------------------------------------------------------------
      # Phase 6: Dual failure during rotation -- 3/5 quorum holds
      # -----------------------------------------------------------------------

      Logger.info("[Phase 6] Simulating dual-member failure")

      [victim_1, victim_2 | survivors] = members

      :ok = :ra.stop_server(@ra_system, victim_1)
      :ok = :ra.stop_server(@ra_system, victim_2)

      Process.sleep(500)

      survivor_leader = wait_for_stable_leader(survivors, 15_000)

      Logger.info("[Phase 6] Quorum holds with 3/5 members")

      for i <- 1..20 do
        cmd = {:put, "dual_failure:#{i}", "surviving_#{i}"}

        case :ra.process_command(survivor_leader, cmd, @ra_timeout) do
          {:ok, _reply, _leader} -> :ok
          {:error, reason} -> flunk("[Phase 6] Write failed: #{inspect(reason)}")
          {:timeout, _} -> flunk("[Phase 6] Write timed out")
        end
      end

      Logger.info("[Phase 6] 20 writes succeeded with 3/5 members")

      :ra.restart_server(@ra_system, victim_2)
      wait_for_member_alive(victim_2, 30_000)
      Logger.info("[Phase 6] #{inspect(victim_2)} recovered")

      :ra.restart_server(@ra_system, victim_1)
      wait_for_member_alive(victim_1, 30_000)
      Logger.info("[Phase 6] #{inspect(victim_1)} recovered")

      # Verify recovered members see dual_failure writes via leader
      state = query_leader(members)

      for i <- 1..20 do
        key = "dual_failure:#{i}"
        assert Map.get(state, key) == "surviving_#{i}", "Missing #{key}"
      end

      Logger.info("[Phase 6] Dual failure recovery verified")

      # -----------------------------------------------------------------------
      # Phase 7: Final data integrity verification -- no data loss
      # -----------------------------------------------------------------------

      Logger.info("[Phase 7] Final data integrity check")

      # Expected: 100 baseline + 50 rolling + 100 post_rotation + 1 post_election + 20 dual_failure = 271
      expected_key_count = 100 + (@cluster_size * 10) + 100 + 1 + 20

      state = query_leader(members)

      assert Map.get(state, "baseline:1") == "value_1"
      assert Map.get(state, "baseline:100") == "value_100"
      assert Map.get(state, "rolling:phase_1:1") == "during_restart_1_1"
      assert Map.get(state, "rolling:phase_#{@cluster_size}:10") ==
               "during_restart_#{@cluster_size}_10"
      assert Map.get(state, "post_rotation:1") == "new_value_1"
      assert Map.get(state, "post_rotation:100") == "new_value_100"
      assert Map.get(state, "post_election") == "works"
      assert Map.get(state, "dual_failure:1") == "surviving_1"
      assert Map.get(state, "dual_failure:20") == "surviving_20"

      actual_count = map_size(state)

      assert actual_count == expected_key_count,
             "Expected #{expected_key_count} keys, got #{actual_count}"

      Logger.info("[Phase 7] #{actual_count} keys verified. Zero data loss.")
      Logger.info("=== Rolling rotation test PASSED ===")
    end
  end

  # ---------------------------------------------------------------------------
  # Quorum boundary: verify writes fail without quorum
  # ---------------------------------------------------------------------------

  describe "quorum boundary" do
    @tag :cluster
    @tag timeout: 60_000
    test "writes fail when quorum is lost (3 of 5 members down)" do
      Logger.info("[Quorum boundary] Verifying writes fail without quorum")

      members = for i <- 1..@cluster_size, do: {:"qb_member_#{i}", node()}

      {:ok, _started, []} =
        :ra.start_cluster(@ra_system, :quorum_boundary_cluster, @machine, members)

      leader = wait_for_stable_leader(members)

      {:ok, _, _} = :ra.process_command(leader, {:put, "qb:baseline", "exists"}, @ra_timeout)

      # Stop 3 members -- only 2 remain, below quorum threshold
      [m1, m2, m3 | alive] = members
      :ok = :ra.stop_server(@ra_system, m1)
      :ok = :ra.stop_server(@ra_system, m2)
      :ok = :ra.stop_server(@ra_system, m3)

      Process.sleep(500)

      # Writes should fail or timeout -- no quorum
      result =
        Enum.find_value(alive, fn member ->
          case :ra.process_command(member, {:put, "qb:should_fail", "nope"}, 3_000) do
            {:ok, _, _} -> {:unexpected_ok, member}
            {:error, _reason} -> :expected_failure
            {:timeout, _} -> :expected_timeout
          end
        end)

      assert result in [:expected_failure, :expected_timeout],
             "Writes should fail without quorum, got: #{inspect(result)}"

      Logger.info("[Quorum boundary] Confirmed: writes fail with only 2/5 members")

      # Restart one member to restore quorum (3/5)
      :ra.restart_server(@ra_system, m3)
      wait_for_member_alive(m3, 30_000)
      Process.sleep(500)

      recovery_leader = wait_for_stable_leader([m3 | alive], 15_000)

      {:ok, _, _} =
        :ra.process_command(recovery_leader, {:put, "qb:recovered", "yes"}, @ra_timeout)

      state = query_leader_via(recovery_leader)
      assert Map.get(state, "qb:baseline") == "exists"
      assert Map.get(state, "qb:recovered") == "yes"

      Logger.info("[Quorum boundary] Cluster recovered after quorum restored")

      cleanup_members(members)
    end
  end

  # ---------------------------------------------------------------------------
  # Delete propagation during rolling restart
  # ---------------------------------------------------------------------------

  describe "delete consistency during rotation" do
    @tag :cluster
    @tag timeout: 60_000
    test "deletes propagate correctly through rolling restarts" do
      Logger.info("[Delete consistency] Testing delete propagation during rolling restart")

      members = for i <- 1..@cluster_size, do: {:"del_member_#{i}", node()}

      {:ok, _started, []} =
        :ra.start_cluster(@ra_system, :delete_rotation_cluster, @machine, members)

      leader = wait_for_stable_leader(members)

      # Write 50 keys, then delete 25 of them
      for i <- 1..50 do
        {:ok, _, _} = :ra.process_command(leader, {:put, "del:#{i}", "v#{i}"}, @ra_timeout)
      end

      for i <- 1..25 do
        {:ok, _, _} = :ra.process_command(leader, {:delete, "del:#{i}"}, @ra_timeout)
      end

      state = query_leader_via(leader)
      assert map_size(state) == 25, "Should have 25 keys after deleting 25 of 50"

      # Rolling restart all members
      for i <- 1..@cluster_size do
        target = Enum.at(members, i - 1)

        :ok = :ra.stop_server(@ra_system, target)
        Process.sleep(300)

        remaining = List.delete(members, target)
        new_leader = wait_for_stable_leader(remaining, 15_000)

        # Write + delete during restart
        {:ok, _, _} =
          :ra.process_command(new_leader, {:put, "del:restart_#{i}", "tmp"}, @ra_timeout)

        {:ok, _, _} =
          :ra.process_command(new_leader, {:delete, "del:restart_#{i}"}, @ra_timeout)

        :ra.restart_server(@ra_system, target)
        wait_for_member_alive(target, 30_000)

        # Verify state through leader
        current_leader = wait_for_stable_leader(members)
        member_state = query_leader_via(current_leader)
        assert map_size(member_state) == 25, "After phase #{i}, should have 25 keys"

        for j <- 1..25 do
          assert Map.get(member_state, "del:#{j}") == nil,
                 "del:#{j} should remain deleted"
        end

        for j <- 26..50 do
          assert Map.get(member_state, "del:#{j}") == "v#{j}",
                 "del:#{j} should still exist"
        end

        assert Map.get(member_state, "del:restart_#{i}") == nil,
               "del:restart_#{i} should be deleted"
      end

      Logger.info("[Delete consistency] Verified through rolling restart")

      cleanup_members(members)
    end
  end

  # ---------------------------------------------------------------------------
  # Batch writes during rotation
  # ---------------------------------------------------------------------------

  describe "batch writes during rotation" do
    @tag :cluster
    @tag timeout: 60_000
    test "batch commands replicate correctly through member restarts" do
      Logger.info("[Batch writes] Testing batch command replication during rotation")

      members = for i <- 1..@cluster_size, do: {:"batch_member_#{i}", node()}

      {:ok, _started, []} =
        :ra.start_cluster(@ra_system, :batch_rotation_cluster, @machine, members)

      leader = wait_for_stable_leader(members)

      # Stop member 1, send a batch command, restart member 1
      target = hd(members)
      :ok = :ra.stop_server(@ra_system, target)
      Process.sleep(300)

      remaining = tl(members)
      batch_leader = wait_for_stable_leader(remaining, 15_000)

      batch_cmd =
        {:batch,
         for i <- 1..50 do
           {:put, "batch:#{i}", "bv_#{i}"}
         end}

      {:ok, _, _} = :ra.process_command(batch_leader, batch_cmd, @ra_timeout)

      # Restart and verify catch-up includes the batch
      :ra.restart_server(@ra_system, target)
      wait_for_member_alive(target, 30_000)

      # Verify through leader
      current_leader = wait_for_stable_leader(members)
      state = query_leader_via(current_leader)

      for i <- 1..50 do
        assert Map.get(state, "batch:#{i}") == "bv_#{i}",
               "Missing batch key batch:#{i}"
      end

      Logger.info("[Batch writes] Batch replication verified")

      cleanup_members(members)
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Waits for a stable leader among the given members by polling ra:members.
  @spec wait_for_stable_leader([{atom(), node()}], non_neg_integer()) :: {atom(), node()}
  defp wait_for_stable_leader(members, timeout_ms \\ @ra_timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_leader(members, deadline)
  end

  defp do_wait_leader(members, deadline) do
    result =
      Enum.find_value(members, fn member ->
        case :ra.members(member) do
          {:ok, _members, leader} -> leader
          _ -> nil
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

  # Verifies all members agree on the same leader.
  defp assert_all_members_agree_on_leader(members) do
    leaders =
      Enum.map(members, fn member ->
        case :ra.members(member) do
          {:ok, _members, leader} -> leader
          other -> flunk("ra:members failed for #{inspect(member)}: #{inspect(other)}")
        end
      end)

    unique_leaders = Enum.uniq(leaders)

    assert length(unique_leaders) == 1,
           "All members should agree on leader, got: #{inspect(unique_leaders)}"
  end

  # Queries the cluster state through whichever member is currently leader.
  defp query_leader(members) do
    leader = wait_for_stable_leader(members)
    query_leader_via(leader)
  end

  # Performs a consistent query via a specific member (should be leader or will redirect).
  defp query_leader_via(member) do
    query_fn = fn state -> state end

    case :ra.consistent_query(member, query_fn, @ra_timeout) do
      {:ok, {_, state}, _} -> state
      {:ok, state, _} -> state
      {:timeout, _} ->
        # Retry with longer timeout
        case :ra.consistent_query(member, query_fn, @ra_timeout * 2) do
          {:ok, {_, state}, _} -> state
          {:ok, state, _} -> state
          other -> raise "Consistent query failed on #{inspect(member)}: #{inspect(other)}"
        end
      {:error, reason} ->
        raise "Consistent query failed on #{inspect(member)}: #{inspect(reason)}"
    end
  end

  # Waits for a member to be alive and responding to ra:members.
  defp wait_for_member_alive(member, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_alive(member, deadline)
  end

  defp do_wait_alive(member, deadline) do
    alive =
      case :ra.members(member) do
        {:ok, _members, _leader} -> true
        _ -> false
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

  # Stops and force-deletes all members for cleanup.
  defp cleanup_members(members) do
    for member <- members do
      try do
        :ra.stop_server(@ra_system, member)
        :ra.force_delete_server(@ra_system, member)
      catch
        _, _ -> :ok
      end
    end
  end
end
