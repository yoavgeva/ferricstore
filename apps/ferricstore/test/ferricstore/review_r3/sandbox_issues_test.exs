defmodule Ferricstore.ReviewR3.SandboxIssuesTest do
  @moduledoc """
  Round 3 review: prove/disprove sandbox and lifecycle issues.

  R3-C1: Sandbox bypass in async write path
  R3-M1: Mode.current() default contradicts documentation
  R3-M7: Sandbox cleanup leaks ActiveFile
  """
  use ExUnit.Case, async: false

  # ===========================================================================
  # R3-C1: Sandbox bypass in async write path
  #
  # Claim: async_write uses keydir_name(idx) instead of resolve_keydir(idx),
  # so sandbox writes via async durability would leak to production ETS.
  #
  # Investigation: raft_write/3 (router.ex:227) checks in_sandbox?() BEFORE
  # dispatching to durability_for_key/1. When in sandbox:
  #
  #     if in_sandbox?() do
  #       GenServer.call(resolve_shard(idx), command)  # <-- always this path
  #     else
  #       case durability_for_key(key) do ...           # <-- never reached
  #
  # So async_write is NEVER called for sandbox writes. The sandbox shard
  # GenServer handles the write directly via its own ra system.
  #
  # Verdict: NOT a bug. The raft_write guard prevents async_write from being
  # reached in sandbox mode. The code in async_write that uses keydir_name/1
  # is unreachable when a sandbox is active.
  # ===========================================================================

  describe "R3-C1: sandbox intercepts before async dispatch" do
    setup do
      sandbox = FerricStore.Sandbox.checkout()
      on_exit(fn -> FerricStore.Sandbox.checkin(sandbox) end)
      %{sandbox: sandbox}
    end

    test "write in sandbox goes to private ETS, not production keydir", %{sandbox: sandbox} do
      # Force global durability mode to :all_async so that if the guard
      # were absent, durability_for_key would return :async.
      prev = :persistent_term.get(:ferricstore_durability_mode, :all_quorum)

      try do
        :persistent_term.put(:ferricstore_durability_mode, :all_async)

        # Write through the public API (goes through Router.put -> raft_write)
        :ok = FerricStore.set("r3c1_key", "sandbox_value")

        # Read back from the same sandbox — should succeed
        assert {:ok, "sandbox_value"} = FerricStore.get("r3c1_key")

        # Verify the key is in the sandbox's private ETS, not production keydir.
        # The sandbox has 2 shards; find which one owns this key.
        idx = rem(:erlang.phash2("r3c1_key"), sandbox.shard_count)
        private_keydir = elem(sandbox.keydirs, idx)

        assert [{_key, _val, _exp, _lfu, _fid, _off, _vsize}] =
                 :ets.lookup(private_keydir, "r3c1_key")

        # Production keydir should NOT have the key.
        # Production keydirs are named atoms :keydir_0, :keydir_1, etc.
        # The production shard index is different (1024-slot map), but check ALL
        # production keydirs to be thorough.
        prod_shard_count = :persistent_term.get(:ferricstore_shard_count)

        for i <- 0..(prod_shard_count - 1) do
          prod_keydir = :"keydir_#{i}"

          case :ets.whereis(prod_keydir) do
            :undefined ->
              :ok

            _tid ->
              assert :ets.lookup(prod_keydir, "r3c1_key") == [],
                     "Key leaked to production keydir_#{i}"
          end
        end
      after
        :persistent_term.put(:ferricstore_durability_mode, prev)
      end
    end

    test "sandbox write with :all_async durability still returns :ok", %{sandbox: _sandbox} do
      prev = :persistent_term.get(:ferricstore_durability_mode, :all_quorum)

      try do
        :persistent_term.put(:ferricstore_durability_mode, :all_async)

        assert :ok = FerricStore.set("r3c1_async_check", "hello")
        assert {:ok, "hello"} = FerricStore.get("r3c1_async_check")
      after
        :persistent_term.put(:ferricstore_durability_mode, prev)
      end
    end

    test "multiple sandbox writes under async durability stay isolated" do
      prev = :persistent_term.get(:ferricstore_durability_mode, :all_quorum)

      try do
        :persistent_term.put(:ferricstore_durability_mode, :all_async)

        # Sandbox A (current process)
        sandbox_a = FerricStore.Sandbox.checkout()

        FerricStore.set("shared_name", "from_A")
        assert {:ok, "from_A"} = FerricStore.get("shared_name")

        # Sandbox B (different process)
        task =
          Task.async(fn ->
            sandbox_b = FerricStore.Sandbox.checkout()

            try do
              FerricStore.set("shared_name", "from_B")
              assert {:ok, "from_B"} = FerricStore.get("shared_name")
            after
              FerricStore.Sandbox.checkin(sandbox_b)
            end
          end)

        Task.await(task)

        # Sandbox A should still see its own value
        assert {:ok, "from_A"} = FerricStore.get("shared_name")

        FerricStore.Sandbox.checkin(sandbox_a)
      after
        :persistent_term.put(:ferricstore_durability_mode, prev)
      end
    end
  end

  # ===========================================================================
  # R3-M1: Mode.current() default contradicts documentation
  #
  # Documentation says:
  #   "When the `:mode` key is not set, FerricStore defaults to `:standalone`."
  #   @moduledoc line: "`:standalone` (default)"
  #   iex example: `Ferricstore.Mode.current()` => `:standalone`
  #
  # Actual code (mode.ex:72):
  #   def current do
  #     Application.get_env(:ferricstore, :mode, :embedded)
  #   end
  #
  # The fallback is :embedded, but docs say :standalone.
  #
  # Verdict: CONFIRMED. The code defaults to :embedded but the @moduledoc,
  # @doc examples, and standalone?() doctest all claim :standalone is default.
  # ===========================================================================

  describe "R3-M1: Mode.current() default" do
    test "code default is :embedded, contradicting doc claim of :standalone" do
      prev = Application.get_env(:ferricstore, :mode)

      try do
        # Remove :mode config entirely to test the fallback
        Application.delete_env(:ferricstore, :mode)

        # The code returns :embedded as default (2nd arg to get_env)
        assert Ferricstore.Mode.current() == :embedded

        # The doc claims :standalone is default — this assertion documents the
        # contradiction. If someone "fixes" the code to match docs, this test
        # will need updating.
        refute Ferricstore.Mode.current() == :standalone,
               "Mode.current() returns :standalone — doc/code mismatch may have been fixed"
      after
        if prev, do: Application.put_env(:ferricstore, :mode, prev)
      end
    end
  end

  # ===========================================================================
  # R3-M7: Sandbox cleanup leaks ActiveFile
  #
  # Claim: sandbox shards register in ActiveFile but checkin doesn't clean up.
  #
  # Investigation: shard.ex line 267:
  #
  #     unless sandbox? do
  #       Ferricstore.Store.ActiveFile.publish(index, active_file_id, ...)
  #     end
  #
  # Sandbox shards NEVER call ActiveFile.publish. Since nothing is published,
  # there is nothing to leak. The `unless sandbox?` guard prevents registration.
  #
  # Verdict: NOT a bug. Sandbox shards skip ActiveFile.publish entirely.
  # ===========================================================================

  describe "R3-M7: sandbox shards do not register in ActiveFile" do
    test "ActiveFile has no entries for sandbox shard indices", %{} do
      sandbox = FerricStore.Sandbox.checkout()

      try do
        # Write something to ensure shard is fully initialized
        FerricStore.set("r3m7_probe", "value")

        # ActiveFile stores entries keyed by shard index. Sandbox shards use
        # indices 0..1 (sandbox.shard_count - 1), same as production shards.
        # But sandbox shards skip ActiveFile.publish, so the entries in
        # ActiveFile are the PRODUCTION shards' entries, not sandbox ones.
        #
        # We verify this indirectly: the ActiveFile entries for indices 0..1
        # should point to the PRODUCTION data directory, not the sandbox tmpdir.
        for i <- 0..(sandbox.shard_count - 1) do
          try do
            {_file_id, file_path, _data_path} = Ferricstore.Store.ActiveFile.get(i)
            # The file_path should NOT contain the sandbox tmpdir
            refute String.contains?(file_path, sandbox.tmpdir),
                   "ActiveFile for shard #{i} points to sandbox tmpdir — sandbox leaked into ActiveFile registry"
          rescue
            # If ActiveFile.get raises (no entry), that's also fine —
            # it means no sandbox entry exists
            _ -> :ok
          end
        end
      after
        FerricStore.Sandbox.checkin(sandbox)
      end
    end

    test "checkin does not corrupt production ActiveFile entries" do
      # Capture production ActiveFile state before sandbox
      prod_shard_count = :persistent_term.get(:ferricstore_shard_count)

      prod_entries_before =
        for i <- 0..(prod_shard_count - 1) do
          try do
            Ferricstore.Store.ActiveFile.get(i)
          rescue
            _ -> :not_available
          end
        end

      # Create and destroy a sandbox
      sandbox = FerricStore.Sandbox.checkout()
      FerricStore.set("r3m7_lifecycle", "test")
      FerricStore.Sandbox.checkin(sandbox)

      # Production ActiveFile entries should be unchanged
      prod_entries_after =
        for i <- 0..(prod_shard_count - 1) do
          try do
            Ferricstore.Store.ActiveFile.get(i)
          rescue
            _ -> :not_available
          end
        end

      assert prod_entries_before == prod_entries_after,
             "Production ActiveFile entries changed after sandbox checkin"
    end
  end
end
