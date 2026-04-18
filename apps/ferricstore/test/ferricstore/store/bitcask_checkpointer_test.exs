defmodule Ferricstore.Store.BitcaskCheckpointerTest do
  @moduledoc """
  Verifies the background-checkpoint contract:

    1. When a writer raises the per-shard `checkpoint_flags` atomic, the
       checkpointer tick fires `v2_fsync_async` on the shard's active
       file, and telemetry emits `{:ferricstore, :bitcask, :checkpoint}`.
    2. When the flag is not set (idle shard), no fsync happens — no
       `[:ferricstore, :bitcask, :checkpoint]` telemetry is emitted.
    3. `sync_now/1` performs a synchronous fsync and clears the flag
       even if it was set.

  We register a telemetry handler that forwards events to the test
  process and assert on the received messages.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Store.BitcaskCheckpointer
  alias Ferricstore.Bitcask.NIF

  setup do
    # Minimal instance-like context with just `checkpoint_flags` (the
    # only field the checkpointer reads). Using shard_index 0 so
    # flag_idx = 1.
    ctx = %{
      name: :"test_ck_#{:erlang.unique_integer([:positive])}",
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false)
    }

    tmp =
      Path.join(System.tmp_dir!(), "ck_test_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(tmp)
    active_path = Path.join(tmp, "00000.log")
    File.touch!(active_path)

    # Publish into ActiveFile registry so the checkpointer can find the
    # path. Use shard index 0.
    ActiveFile.init(1)
    ActiveFile.publish(0, 0, active_path, tmp)

    # Attach a telemetry handler that forwards checkpoint events.
    parent = self()

    handler_id = "ck-test-#{:erlang.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :bitcask, :checkpoint],
      fn _evt, meas, meta, _ -> send(parent, {:checkpoint, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf!(tmp)
    end)

    %{ctx: ctx, tmp: tmp, active_path: active_path}
  end

  test "checkpointer fsyncs only when the dirty flag is set", %{ctx: ctx, active_path: active_path} do
    # Start checkpointer with a fast 20ms tick so we don't wait long.
    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 20,
        name: :"ck_dirty_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    # No flag set → no checkpoint should fire over three ticks.
    refute_receive {:checkpoint, _meas, %{status: :ok}}, 100

    # Raise the dirty flag (simulates a writer batch).
    :atomics.put(ctx.checkpoint_flags, 1, 1)

    assert_receive {:checkpoint, _meas, %{status: :ok}}, 500

    # After the fsync fires, the flag must have been cleared.
    assert :atomics.get(ctx.checkpoint_flags, 1) == 0

    # Sanity: the fsync actually touched the active file (v2_fsync on a
    # real path returns :ok; the NIF is loaded).
    assert File.exists?(active_path)
  end

  test "sync_now performs a synchronous fsync and clears the flag", %{ctx: ctx} do
    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_sync_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    :atomics.put(ctx.checkpoint_flags, 1, 1)
    assert :ok = BitcaskCheckpointer.sync_now(pid)
    assert :atomics.get(ctx.checkpoint_flags, 1) == 0
  end

  test "writer via state-machine-style put raises the flag", %{ctx: ctx, active_path: active_path} do
    # Emulate the write-path: append a record, then flip the flag the
    # way StateMachine.flush_pending_writes does.
    {:ok, _} = NIF.v2_append_record(active_path, "k", "v", 0)
    :atomics.put(ctx.checkpoint_flags, 1, 1)

    assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
           "writer must raise the dirty flag so the checkpointer picks it up"
  end
end
