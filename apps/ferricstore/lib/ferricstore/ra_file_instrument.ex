defmodule Ferricstore.RaFileInstrument do
  @moduledoc """
  Instruments ra_file:sync/1 to collect timing data about WAL fsync calls.

  Call `install/0` before ra starts to monkey-patch the ra_file module.
  Call `report/0` to print collected stats.
  Call `uninstall/0` to restore the original module.

  Data is stored in a :counters (total_calls, total_us) and an ETS table
  for histogram buckets.
  """

  @table :ra_file_instrument
  @counters_key :ra_file_instrument_counters

  @doc "Install the instrumented ra_file module (call before ra starts)"
  def install do
    # Create ETS for histogram buckets
    try do
      :ets.new(@table, [:set, :public, :named_table])
    rescue
      _ -> :ets.delete_all_objects(@table)
    end

    # counters: {total_calls, total_microseconds, max_us, min_us}
    counters = :counters.new(4, [:atomics])
    :counters.put(counters, 4, 999_999_999)  # min starts high
    :persistent_term.put(@counters_key, counters)

    # Initialize histogram buckets (microseconds)
    # <100us, <500us, <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, >=500ms
    for bucket <- [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, :overflow] do
      :ets.insert(@table, {bucket, 0})
    end

    # Compile and load the instrumented ra_file module using Erlang abstract forms
    forms = [
      {:attribute, 1, :module, :ra_file},
      {:attribute, 2, :export, [{:sync, 1}, {:rename, 2}]},
      # sync(Fd) ->
      #   T0 = erlang:monotonic_time(microsecond),
      #   Result = file:sync(Fd),
      #   T1 = erlang:monotonic_time(microsecond),
      #   Elapsed = T1 - T0,
      #   'Elixir.Ferricstore.RaFileInstrument':record_sync(Elapsed),
      #   Result.
      {:function, 3, :sync, 1, [
        {:clause, 3, [{:var, 3, :Fd}], [], [
          {:match, 4, {:var, 4, :T0},
            {:call, 4, {:remote, 4, {:atom, 4, :erlang}, {:atom, 4, :monotonic_time}},
              [{:atom, 4, :microsecond}]}},
          {:match, 5, {:var, 5, :Result},
            {:call, 5, {:remote, 5, {:atom, 5, :file}, {:atom, 5, :sync}},
              [{:var, 5, :Fd}]}},
          {:match, 6, {:var, 6, :T1},
            {:call, 6, {:remote, 6, {:atom, 6, :erlang}, {:atom, 6, :monotonic_time}},
              [{:atom, 6, :microsecond}]}},
          {:match, 7, {:var, 7, :Elapsed},
            {:op, 7, :-, {:var, 7, :T1}, {:var, 7, :T0}}},
          {:call, 8, {:remote, 8,
            {:atom, 8, :"Elixir.Ferricstore.RaFileInstrument"},
            {:atom, 8, :record_sync}},
            [{:var, 8, :Elapsed}]},
          {:var, 9, :Result}
        ]}
      ]},
      # rename(Src, Dst) -> prim_file:rename(Src, Dst).
      {:function, 10, :rename, 2, [
        {:clause, 10, [{:var, 10, :Src}, {:var, 10, :Dst}], [], [
          {:call, 10, {:remote, 10, {:atom, 10, :prim_file}, {:atom, 10, :rename}},
            [{:var, 10, :Src}, {:var, 10, :Dst}]}
        ]}
      ]}
    ]
    {:ok, :ra_file, binary} = :compile.forms(forms, [:binary])
    :code.purge(:ra_file)
    {:module, :ra_file} = :code.load_binary(:ra_file, ~c"ra_file.erl", binary)
    :ok
  end

  @doc "Record a sync call duration (called from the patched ra_file)"
  def record_sync(elapsed_us) do
    counters = :persistent_term.get(@counters_key)
    :counters.add(counters, 1, 1)           # total calls
    :counters.add(counters, 2, elapsed_us)  # total microseconds

    # Update max
    current_max = :counters.get(counters, 3)
    if elapsed_us > current_max, do: :counters.put(counters, 3, elapsed_us)

    # Update min
    current_min = :counters.get(counters, 4)
    if elapsed_us < current_min, do: :counters.put(counters, 4, elapsed_us)

    # Histogram bucket
    bucket = histogram_bucket(elapsed_us)
    :ets.update_counter(@table, bucket, {2, 1})
  end

  @histogram_thresholds [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000]

  defp histogram_bucket(elapsed_us) do
    Enum.find(@histogram_thresholds, :overflow, fn threshold -> elapsed_us < threshold end)
  end

  @doc "Print collected fsync stats"
  def report do
    counters = :persistent_term.get(@counters_key)
    total_calls = :counters.get(counters, 1)
    total_us = :counters.get(counters, 2)
    max_us = :counters.get(counters, 3)
    min_us = :counters.get(counters, 4)

    avg_us = if total_calls > 0, do: div(total_us, total_calls), else: 0

    IO.puts("""
    === ra_file:sync() Instrumentation Report ===
      Total sync calls:  #{total_calls}
      Total sync time:   #{div(total_us, 1_000)}ms
      Average:           #{avg_us}us (#{Float.round(avg_us / 1_000, 2)}ms)
      Min:               #{min_us}us
      Max:               #{max_us}us
      Syncs/sec:         #{if total_us > 0, do: div(total_calls * 1_000_000, total_us), else: 0}

      Histogram:
    """)

    buckets = [
      {100, "<100us"},
      {500, "<500us"},
      {1_000, "<1ms"},
      {5_000, "<5ms"},
      {10_000, "<10ms"},
      {50_000, "<50ms"},
      {100_000, "<100ms"},
      {500_000, "<500ms"},
      {:overflow, ">=500ms"}
    ]

    for {key, label} <- buckets do
      [{^key, count}] = :ets.lookup(@table, key)
      pct = if total_calls > 0, do: Float.round(count / total_calls * 100, 1), else: 0.0
      bar = String.duplicate("█", min(round(pct), 50))
      IO.puts("    #{String.pad_trailing(label, 10)} #{String.pad_leading(to_string(count), 8)} (#{String.pad_leading("#{pct}%", 6)}) #{bar}")
    end

    IO.puts("")
  end

  @doc "Reset counters"
  def reset do
    counters = :persistent_term.get(@counters_key)
    for i <- 1..4, do: :counters.put(counters, i, 0)
    :counters.put(counters, 4, 999_999_999)
    :ets.delete_all_objects(@table)
    for bucket <- [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, :overflow] do
      :ets.insert(@table, {bucket, 0})
    end
    :ok
  end
end
