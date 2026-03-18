defmodule Ferricstore.MetricsTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Metrics

  # ---------------------------------------------------------------------------
  # scrape/0 — Prometheus text format validity
  # ---------------------------------------------------------------------------

  describe "scrape/0" do
    test "returns a non-empty binary string" do
      text = Metrics.scrape()
      assert is_binary(text)
      assert byte_size(text) > 0
    end

    test "ends with a trailing newline" do
      text = Metrics.scrape()
      assert String.ends_with?(text, "\n")
    end

    test "contains all expected metric names" do
      text = Metrics.scrape()

      expected_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_hot_reads_total",
        "ferricstore_cold_reads_total",
        "ferricstore_used_memory_bytes",
        "ferricstore_keydir_used_bytes",
        "ferricstore_uptime_seconds",
        "ferricstore_blocked_clients",
        "ferricstore_tracking_clients",
        "ferricstore_slowlog_entries"
      ]

      for metric <- expected_metrics do
        assert String.contains?(text, metric),
               "Expected metric #{metric} to be present in scrape output"
      end
    end

    test "every metric has a HELP line" do
      text = Metrics.scrape()

      expected_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_hot_reads_total",
        "ferricstore_cold_reads_total",
        "ferricstore_used_memory_bytes",
        "ferricstore_keydir_used_bytes",
        "ferricstore_uptime_seconds",
        "ferricstore_blocked_clients",
        "ferricstore_tracking_clients",
        "ferricstore_slowlog_entries"
      ]

      for metric <- expected_metrics do
        assert String.contains?(text, "# HELP #{metric} "),
               "Expected HELP line for #{metric}"
      end
    end

    test "every metric has a TYPE line with counter or gauge" do
      text = Metrics.scrape()

      counters = [
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_hot_reads_total",
        "ferricstore_cold_reads_total"
      ]

      gauges = [
        "ferricstore_connected_clients",
        "ferricstore_used_memory_bytes",
        "ferricstore_keydir_used_bytes",
        "ferricstore_uptime_seconds",
        "ferricstore_blocked_clients",
        "ferricstore_tracking_clients",
        "ferricstore_slowlog_entries"
      ]

      for metric <- counters do
        assert String.contains?(text, "# TYPE #{metric} counter"),
               "Expected TYPE counter for #{metric}"
      end

      for metric <- gauges do
        assert String.contains?(text, "# TYPE #{metric} gauge"),
               "Expected TYPE gauge for #{metric}"
      end
    end

    test "all metric values are non-negative integers" do
      text = Metrics.scrape()

      # Parse each sample line (lines that don't start with #)
      sample_lines =
        text
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))

      assert length(sample_lines) == 11,
             "Expected 11 sample lines, got #{length(sample_lines)}"

      for line <- sample_lines do
        [_name, value_str] = String.split(line, " ", parts: 2)
        {value, ""} = Integer.parse(value_str)

        assert value >= 0,
               "Expected non-negative value for line: #{line}, got #{value}"
      end
    end

    test "follows correct Prometheus triplet order: HELP, TYPE, sample" do
      text = Metrics.scrape()
      lines = String.split(text, "\n", trim: true)

      # Group into triplets (HELP, TYPE, sample)
      triplets = Enum.chunk_every(lines, 3)

      for triplet <- triplets do
        assert length(triplet) == 3,
               "Expected triplet of 3 lines, got #{length(triplet)}: #{inspect(triplet)}"

        [help, type, sample] = triplet
        assert String.starts_with?(help, "# HELP ")
        assert String.starts_with?(type, "# TYPE ")
        refute String.starts_with?(sample, "#")

        # Ensure the metric name is consistent across all three lines
        help_name = help |> String.replace_prefix("# HELP ", "") |> String.split(" ", parts: 2) |> hd()
        type_name = type |> String.replace_prefix("# TYPE ", "") |> String.split(" ", parts: 2) |> hd()
        sample_name = sample |> String.split(" ", parts: 2) |> hd()

        assert help_name == type_name,
               "HELP name (#{help_name}) does not match TYPE name (#{type_name})"

        assert type_name == sample_name,
               "TYPE name (#{type_name}) does not match sample name (#{sample_name})"
      end
    end

    test "used_memory_bytes is positive (BEAM always uses some memory)" do
      text = Metrics.scrape()
      value = extract_metric_value(text, "ferricstore_used_memory_bytes")
      assert value > 0
    end

    test "uptime_seconds is non-negative" do
      text = Metrics.scrape()
      value = extract_metric_value(text, "ferricstore_uptime_seconds")
      assert value >= 0
    end

    test "produces exactly 11 metrics" do
      text = Metrics.scrape()

      help_count =
        text
        |> String.split("\n", trim: true)
        |> Enum.count(&String.starts_with?(&1, "# HELP"))

      assert help_count == 11
    end
  end

  # ---------------------------------------------------------------------------
  # handle/2 — FERRICSTORE.METRICS command dispatch
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.METRICS command" do
    test "returns scrape text with no arguments" do
      result = Metrics.handle("FERRICSTORE.METRICS", [])
      assert is_binary(result)
      assert String.contains?(result, "ferricstore_connected_clients")
      assert String.contains?(result, "# HELP")
      assert String.contains?(result, "# TYPE")
    end

    test "returns error with extra arguments" do
      assert {:error, msg} = Metrics.handle("FERRICSTORE.METRICS", ["extra"])
      assert msg =~ "wrong number of arguments"
    end

    test "result matches scrape/0 output" do
      # Both should produce equivalent output (collected at roughly the same time)
      command_result = Metrics.handle("FERRICSTORE.METRICS", [])
      scrape_result = Metrics.scrape()

      # Both should have the same metric names and structure
      command_metrics = extract_metric_names(command_result)
      scrape_metrics = extract_metric_names(scrape_result)

      assert command_metrics == scrape_metrics
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: FERRICSTORE.METRICS via the Dispatcher
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.METRICS via Dispatcher" do
    test "dispatch routes to metrics handler" do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.METRICS", [], store)

      assert is_binary(result)
      assert String.contains?(result, "# HELP ferricstore_connected_clients")
      assert String.contains?(result, "# TYPE ferricstore_connected_clients gauge")
    end

    test "dispatch is case-insensitive" do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      store = MockStore.make()
      result = Dispatcher.dispatch("ferricstore.metrics", [], store)

      assert is_binary(result)
      assert String.contains?(result, "ferricstore_connected_clients")
    end

    test "dispatch with extra args returns error" do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.METRICS", ["unexpected"], store)

      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp extract_metric_value(text, metric_name) do
    text
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.find_value(fn line ->
      case String.split(line, " ", parts: 2) do
        [^metric_name, value_str] ->
          {value, ""} = Integer.parse(value_str)
          value

        _ ->
          nil
      end
    end)
  end

  defp extract_metric_names(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.starts_with?(&1, "# HELP"))
    |> Enum.map(fn line ->
      line
      |> String.replace_prefix("# HELP ", "")
      |> String.split(" ", parts: 2)
      |> hd()
    end)
    |> Enum.sort()
  end
end
