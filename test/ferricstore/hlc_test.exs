defmodule Ferricstore.HLCTest do
  use ExUnit.Case, async: true

  alias Ferricstore.HLC

  # ---------------------------------------------------------------------------
  # Setup — start a fresh HLC per test to avoid cross-test pollution.
  # ---------------------------------------------------------------------------

  setup do
    hlc = start_supervised!({HLC, name: :"hlc_#{System.unique_integer([:positive])}"})
    %{hlc: hlc}
  end

  # ---------------------------------------------------------------------------
  # now/1 — basic monotonicity
  # ---------------------------------------------------------------------------

  describe "now/1" do
    test "returns a 2-tuple {physical_ms, logical}", %{hlc: hlc} do
      {physical, logical} = HLC.now(hlc)

      assert is_integer(physical)
      assert is_integer(logical)
      assert physical > 0
      assert logical >= 0
    end

    test "returns monotonically increasing values across sequential calls", %{hlc: hlc} do
      ts1 = HLC.now(hlc)
      ts2 = HLC.now(hlc)
      ts3 = HLC.now(hlc)

      # Monotonicity: each subsequent timestamp must be strictly greater.
      assert HLC.compare(ts2, ts1) == :gt
      assert HLC.compare(ts3, ts2) == :gt
    end

    test "rapid calls in the same millisecond increment the logical counter", %{hlc: hlc} do
      # Call many times rapidly — at least some will land in the same ms.
      timestamps = for _ <- 1..100, do: HLC.now(hlc)

      # All timestamps must be unique.
      assert length(Enum.uniq(timestamps)) == 100

      # Check that when physical ms is repeated, logical counter increments.
      grouped = Enum.group_by(timestamps, fn {phys, _log} -> phys end)

      Enum.each(grouped, fn {_ms, entries} ->
        logicals = Enum.map(entries, fn {_phys, log} -> log end)
        # Logical values within the same ms must be strictly ascending.
        assert logicals == Enum.sort(logicals)
        assert logicals == Enum.uniq(logicals)
      end)
    end

    test "physical component is close to wall clock", %{hlc: hlc} do
      wall_before = System.os_time(:millisecond)
      {physical, _logical} = HLC.now(hlc)
      wall_after = System.os_time(:millisecond)

      assert physical >= wall_before
      assert physical <= wall_after + 1
    end
  end

  # ---------------------------------------------------------------------------
  # update/2 — merging remote timestamps
  # ---------------------------------------------------------------------------

  describe "update/2" do
    test "with a future remote timestamp advances the local clock", %{hlc: hlc} do
      # Get current time.
      {local_phys, _} = HLC.now(hlc)

      # Simulate a remote timestamp 1000ms in the future.
      remote_ts = {local_phys + 1_000, 5}
      :ok = HLC.update(hlc, remote_ts)

      # After update, now() should be at or beyond the remote timestamp.
      {new_phys, new_log} = HLC.now(hlc)
      assert new_phys >= local_phys + 1_000

      # The logical counter should be correct relative to the merge.
      assert new_log >= 0
    end

    test "with a past remote timestamp does not go backward", %{hlc: hlc} do
      ts_before = HLC.now(hlc)

      # Remote timestamp is way in the past.
      remote_ts = {1_000_000, 0}
      :ok = HLC.update(hlc, remote_ts)

      ts_after = HLC.now(hlc)
      assert HLC.compare(ts_after, ts_before) == :gt
    end

    test "with equal physical time picks the higher logical counter", %{hlc: hlc} do
      {local_phys, _} = HLC.now(hlc)

      # Remote has the same physical time but a high logical counter.
      remote_ts = {local_phys, 999}
      :ok = HLC.update(hlc, remote_ts)

      {new_phys, new_log} = HLC.now(hlc)

      # We should be at least at local_phys with logical > 999.
      assert new_phys >= local_phys

      if new_phys == local_phys do
        assert new_log > 999
      end
    end
  end

  # ---------------------------------------------------------------------------
  # drift_ms/1 — wall clock drift detection
  # ---------------------------------------------------------------------------

  describe "drift_ms/1" do
    test "returns near-zero drift under normal operation", %{hlc: hlc} do
      _ts = HLC.now(hlc)
      drift = HLC.drift_ms(hlc)

      # Under normal conditions, drift should be < 10ms.
      assert drift < 10
    end

    test "returns positive drift after updating with a far-future timestamp", %{hlc: hlc} do
      future_ms = System.os_time(:millisecond) + 2_000
      :ok = HLC.update(hlc, {future_ms, 0})

      drift = HLC.drift_ms(hlc)
      # Drift should be roughly 2000ms (minus some wall-clock advancement).
      assert drift >= 1_900
      assert drift <= 2_100
    end
  end

  # ---------------------------------------------------------------------------
  # compare/2 — timestamp ordering
  # ---------------------------------------------------------------------------

  describe "compare/2" do
    test "compares by physical time first" do
      assert HLC.compare({100, 0}, {200, 0}) == :lt
      assert HLC.compare({200, 0}, {100, 0}) == :gt
    end

    test "compares by logical time when physical is equal" do
      assert HLC.compare({100, 1}, {100, 2}) == :lt
      assert HLC.compare({100, 2}, {100, 1}) == :gt
    end

    test "returns :eq for identical timestamps" do
      assert HLC.compare({100, 5}, {100, 5}) == :eq
    end
  end

  # ---------------------------------------------------------------------------
  # encode_ms/1 — millisecond component for stream IDs
  # ---------------------------------------------------------------------------

  describe "encode_ms/1" do
    test "returns the physical component of an HLC timestamp" do
      assert HLC.encode_ms({1_234_567_890, 42}) == 1_234_567_890
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry — drift warning events
  # ---------------------------------------------------------------------------

  describe "telemetry drift warning" do
    test "emits [:ferricstore, :hlc, :drift_warning] when drift exceeds 500ms", %{hlc: hlc} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :hlc, :drift_warning]
        ])

      # Push the HLC 600ms into the future.
      future_ms = System.os_time(:millisecond) + 600
      :ok = HLC.update(hlc, {future_ms, 0})

      # Trigger a now() call — this is where drift is checked.
      _ts = HLC.now(hlc)

      assert_receive {[:ferricstore, :hlc, :drift_warning], ^ref, %{drift_ms: drift}, _meta}
      assert drift >= 500
    end

    test "does not emit warning when drift is within threshold", %{hlc: hlc} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :hlc, :drift_warning]
        ])

      _ts = HLC.now(hlc)

      refute_receive {[:ferricstore, :hlc, :drift_warning], ^ref, _, _}, 50
    end
  end

  # ---------------------------------------------------------------------------
  # Convenience: now_ms/1 for callers that just need a monotonic millisecond
  # ---------------------------------------------------------------------------

  describe "now_ms/1" do
    test "returns a single integer (physical component)", %{hlc: hlc} do
      ms = HLC.now_ms(hlc)
      assert is_integer(ms)
      assert ms > 0
    end

    test "returns monotonically increasing values", %{hlc: hlc} do
      ms1 = HLC.now_ms(hlc)
      ms2 = HLC.now_ms(hlc)

      # In rapid succession the physical ms may be the same, but it should
      # never go backward.
      assert ms2 >= ms1
    end
  end
end
