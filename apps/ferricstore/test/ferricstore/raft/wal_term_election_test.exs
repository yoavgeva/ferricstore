defmodule Ferricstore.Raft.WalTermElectionTest do
  @moduledoc """
  Regression test for ra leader crash when WAL written event arrives
  after a term change (election). Reproduces the FunctionClauseError
  in ra_server_proc.handle_leader/2.

  The bug: under heavy write load, rapid ra elections can cause a
  term change between when entries are written to the WAL and when
  the async fdatasync notification arrives. If the entry's term in
  the mem table doesn't match the term in the WAL notification,
  ra crashes.
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 60_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  describe "diagnostics" do
    test "capture ra crash details on prob writes" do
      ctx = FerricStore.Instance.get(:default)

      # Monitor all ra shard processes to catch crashes with stacktraces
      monitors =
        for shard_idx <- 0..(ctx.shard_count - 1) do
          shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)
          {shard_name, _node} = shard_id

          case Process.whereis(shard_name) do
            nil ->
              nil

            pid ->
              ref = Process.monitor(pid)
              {shard_idx, pid, ref}
          end
        end
        |> Enum.reject(&is_nil/1)

      # Heavy writes through Raft — this is what triggers the crash
      errors =
        for i <- 1..100 do
          try do
            FerricStore.topk_reserve("diag_tk_#{i}", 3)
            :ok
          catch
            :exit, reason ->
              {:exit, i, reason}
          end
        end
        |> Enum.reject(&(&1 == :ok))

      # Also try some regular writes
      for i <- 1..50 do
        try do
          Router.put(ctx, "diag_kv_#{i}", "val_#{i}", 0)
        catch
          :exit, _ -> :error
        end
      end

      # Give ra a moment to crash if it's going to
      Process.sleep(500)

      # Collect DOWN messages from monitored ra processes
      down_msgs = collect_downs(monitors)

      if errors != [] do
        IO.puts("\n=== #{length(errors)} write errors ===")

        for {:exit, i, reason} <- Enum.take(errors, 5) do
          IO.puts("  Write #{i}: #{inspect(reason, pretty: true, limit: 500)}")
        end
      end

      if down_msgs != [] do
        IO.puts("\n=== #{length(down_msgs)} RA PROCESS CRASHES ===")

        for {shard_idx, _pid, reason} <- down_msgs do
          IO.puts("\n--- Shard #{shard_idx} crash reason ---")
          IO.puts(inspect(reason, pretty: true, limit: :infinity))
        end

        flunk(
          "#{length(down_msgs)} ra processes crashed. See output above for stacktraces."
        )
      else
        IO.puts("\nNo ra crashes detected in #{length(monitors)} monitored shards.")
        IO.puts("Write errors: #{length(errors)} out of 100")
      end
    end

    test "capture ra crash with concurrent writes and elections" do
      ctx = FerricStore.Instance.get(:default)

      # Monitor all ra shard processes
      monitors =
        for shard_idx <- 0..(ctx.shard_count - 1) do
          shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)
          {shard_name, _node} = shard_id

          case Process.whereis(shard_name) do
            nil -> nil
            pid -> {shard_idx, pid, Process.monitor(pid)}
          end
        end
        |> Enum.reject(&is_nil/1)

      # Concurrent writers
      writers =
        for i <- 1..20 do
          Task.async(fn ->
            for j <- 1..10 do
              key = "diag_conc_#{i}_#{j}"

              try do
                FerricStore.topk_reserve(key, 3)
                FerricStore.topk_add(key, ["a", "b"])
                :ok
              catch
                :exit, reason -> {:exit, reason}
              end
            end
          end)
        end

      # Trigger elections during writes
      for _ <- 1..5 do
        for shard_idx <- 0..(ctx.shard_count - 1) do
          shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)

          try do
            :ra.trigger_election(shard_id)
          catch
            _, _ -> :ok
          end
        end

        Process.sleep(30)
      end

      # Wait for writers
      results = Task.await_many(writers, 30_000)

      exit_reasons =
        results
        |> List.flatten()
        |> Enum.filter(&match?({:exit, _}, &1))

      Process.sleep(500)
      down_msgs = collect_downs(monitors)

      if exit_reasons != [] do
        IO.puts("\n=== #{length(exit_reasons)} exit reasons from writers ===")

        for {:exit, reason} <- Enum.take(exit_reasons, 3) do
          IO.puts("  #{inspect(reason, pretty: true, limit: 500)}")
        end
      end

      if down_msgs != [] do
        IO.puts("\n=== #{length(down_msgs)} RA PROCESS CRASHES ===")

        for {shard_idx, _pid, reason} <- down_msgs do
          IO.puts("\n--- Shard #{shard_idx} crash reason ---")
          IO.puts(inspect(reason, pretty: true, limit: :infinity))
        end

        flunk("#{length(down_msgs)} ra processes crashed")
      else
        IO.puts("\nNo ra crashes. Exit reasons: #{length(exit_reasons)}")
      end
    end
  end

  describe "WAL term mismatch under rapid elections" do
    test "prob writes survive rapid election cycling" do
      ctx = FerricStore.Instance.get(:default)

      # Write a TopK structure through Raft (triggers WAL write)
      assert :ok = FerricStore.topk_reserve("wal_term_tk1", 3)

      # Trigger a rapid election cycle on shard 0 to force term increment
      shard_id = Ferricstore.Raft.Cluster.shard_server_id(0)
      :ra.trigger_election(shard_id)
      Process.sleep(100)
      :ra.trigger_election(shard_id)
      Process.sleep(100)

      # Write more prob commands — these go through Raft after the
      # term changed. If the WAL notification carries the old term,
      # ra will crash with FunctionClauseError.
      assert :ok = FerricStore.topk_reserve("wal_term_tk2", 5)
      assert {:ok, [nil, nil]} = FerricStore.topk_add("wal_term_tk1", ["x", "y"])

      # Verify the shard's ra process is still alive
      {:ok, _members, _leader} = Ferricstore.Raft.Cluster.members(0)
    end

    test "heavy concurrent prob writes with elections don't crash ra" do
      ctx = FerricStore.Instance.get(:default)
      shard_count = ctx.shard_count

      # Start concurrent writers doing prob commands
      writers =
        for i <- 1..10 do
          Task.async(fn ->
            for j <- 1..20 do
              key = "wal_stress_#{i}_#{j}"

              try do
                FerricStore.topk_reserve(key, 3)
                FerricStore.topk_add(key, ["a_#{j}", "b_#{j}"])
              rescue
                _ -> :error
              catch
                :exit, _ -> :error
              end
            end
          end)
        end

      # While writes are happening, trigger elections on all shards
      for _ <- 1..5 do
        for shard_idx <- 0..(shard_count - 1) do
          shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)

          try do
            :ra.trigger_election(shard_id)
          catch
            _, _ -> :ok
          end
        end

        Process.sleep(50)
      end

      # Wait for all writers
      results = Task.await_many(writers, 30_000)
      _errors = results |> List.flatten() |> Enum.count(&(&1 == :error))

      # Some writes may fail during elections (expected), but ra must survive.
      # The key assertion: all shards still have leaders after the stress test.
      for shard_idx <- 0..(shard_count - 1) do
        ShardHelpers.eventually(
          fn ->
            case Ferricstore.Raft.Cluster.members(shard_idx) do
              {:ok, _members, _leader} -> assert true
              other -> flunk("Shard #{shard_idx} has no leader: #{inspect(other)}")
            end
          end,
          "shard #{shard_idx} leader not recovered",
          20,
          200
        )
      end
    end

    test "regular writes survive election during WAL sync" do
      ctx = FerricStore.Instance.get(:default)

      # Write initial data
      for i <- 1..100 do
        Router.put(ctx, "wal_election_#{i}", "value_#{i}", 0)
      end

      # Trigger elections while more writes happen
      writer =
        Task.async(fn ->
          for i <- 101..200 do
            try do
              Router.put(ctx, "wal_election_#{i}", "value_#{i}", 0)
              :ok
            catch
              :exit, _ -> :error
            end
          end
        end)

      for _ <- 1..10 do
        for shard_idx <- 0..(ctx.shard_count - 1) do
          shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)

          try do
            :ra.trigger_election(shard_id)
          catch
            _, _ -> :ok
          end
        end

        Process.sleep(20)
      end

      Task.await(writer, 30_000)

      # All shards must still be operational
      for shard_idx <- 0..(ctx.shard_count - 1) do
        ShardHelpers.eventually(
          fn ->
            {:ok, _, _} = Ferricstore.Raft.Cluster.members(shard_idx)
          end,
          "shard #{shard_idx} dead after elections",
          20,
          200
        )
      end

      # Writes after elections must still work
      Router.put(ctx, "post_election_key", "works", 0)
      assert "works" == Router.get(ctx, "post_election_key")
    end
  end

  # Collect any DOWN messages from monitored processes
  defp collect_downs(monitors) do
    Enum.reduce(monitors, [], fn {shard_idx, pid, _ref}, acc ->
      receive do
        {:DOWN, _ref, :process, ^pid, reason} ->
          [{shard_idx, pid, reason} | acc]
      after
        0 -> acc
      end
    end)
  end
end
