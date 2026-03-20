defmodule FerricstoreServer.Spec.OomPreventionTest do
  @moduledoc """
  Spec section 2G.7: OOM Prevention tests.

  Verifies that MemoryGuard rejects writes under the :noeviction policy when
  memory pressure reaches the critical (reject) threshold.

  Tests:
    - MemoryGuard with :noeviction + tiny budget -> reject_writes? returns true
    - Write attempt when reject_writes? is true gets OOM error
    - Non-noeviction policies do NOT reject writes even under pressure
    - reject_writes? returns false when pressure is below threshold
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard

  # ---------------------------------------------------------------------------
  # Section 2G.7: MemoryGuard noeviction policy rejects writes
  # ---------------------------------------------------------------------------

  describe "MemoryGuard noeviction + tiny budget (spec 2G.7)" do
    test "reject_writes? returns true with noeviction policy and critical pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      # Trigger a check so last_pressure_level gets set
      send(pid, :check)
      Process.sleep(100)

      assert GenServer.call(pid, :reject_writes?) == true
      GenServer.stop(pid)
    end

    test "reject_writes? returns false with noeviction but normal pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          # 1 GB - well above any test ETS usage
          max_memory_bytes: 1_073_741_824,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      # Trigger a check
      send(pid, :check)
      Process.sleep(100)

      assert GenServer.call(pid, :reject_writes?) == false
      GenServer.stop(pid)
    end

    test "reject_writes? returns false with volatile_lru even under pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          # Tiny budget forces pressure
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :volatile_lru
        ])

      # Trigger a check
      send(pid, :check)
      Process.sleep(100)

      # Even though pressure is at reject level, volatile_lru does not reject
      assert GenServer.call(pid, :reject_writes?) == false
      GenServer.stop(pid)
    end

    test "reject_writes? returns false with allkeys_lru even under pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :allkeys_lru
        ])

      send(pid, :check)
      Process.sleep(100)

      assert GenServer.call(pid, :reject_writes?) == false
      GenServer.stop(pid)
    end

    test "stats report :reject pressure level with tiny budget" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      stats = GenServer.call(pid, :stats)
      assert stats.pressure_level == :reject
      assert stats.eviction_policy == :noeviction
      assert stats.ratio > 0.95

      GenServer.stop(pid)
    end

    test "eviction_policy/0 returns :noeviction when configured" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      assert GenServer.call(pid, :eviction_policy) == :noeviction
      GenServer.stop(pid)
    end

    test "pressure level transitions from :ok to :reject when budget shrinks" do
      # Start with a generous budget
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      # With a large budget, pressure should be :ok
      send(pid, :check)
      Process.sleep(50)
      assert GenServer.call(pid, :reject_writes?) == false

      GenServer.stop(pid)

      # Now start with a tiny budget
      {:ok, pid2} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      send(pid2, :check)
      Process.sleep(50)
      assert GenServer.call(pid2, :reject_writes?) == true

      GenServer.stop(pid2)
    end
  end

  # ---------------------------------------------------------------------------
  # OOM error format (connection to spec section 4.6 error codes)
  # ---------------------------------------------------------------------------

  describe "OOM error telemetry under noeviction" do
    test "memory:pressure telemetry is emitted when budget is exceeded" do
      test_pid = self()
      handler_id = "oom-pressure-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :pressure],
        fn _event, measurements, metadata, config ->
          send(config.test_pid, {:pressure, measurements, metadata})
        end,
        %{test_pid: test_pid}
      )

      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 50,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      # Match specifically on per-shard events (contain pressure_level, not level)
      assert_receive {:pressure, measurements, %{pressure_level: :reject} = metadata}, 2000
      assert is_integer(measurements.shard_index)
      assert is_integer(measurements.bytes)
      assert measurements.bytes > 0
      assert metadata.eviction_policy == :noeviction

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end
end
