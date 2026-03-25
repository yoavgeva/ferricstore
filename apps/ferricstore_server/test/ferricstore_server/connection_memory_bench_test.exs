defmodule FerricstoreServer.ConnectionMemoryBenchTest do
  @moduledoc """
  Benchmark / stress tests that find the breaking point of per-connection
  memory in FerricStore.

  Tests are tagged `:bench` and excluded from normal test runs. Execute with:

      mix test apps/ferricstore_server/test/ferricstore_server/connection_memory_bench_test.exs --include bench

  Each test sends increasingly large payloads over a raw TCP connection and
  measures system memory delta, wall-clock time, and whether the connection
  process survives. Results are printed as a table to stdout.

  ## What is measured

  1. **Pipeline size limit (PING)** -- N PING commands in one TCP write (no Raft)
  2. **Pipeline size limit (SET)** -- N SET commands in one TCP write (with Raft)
  3. **MULTI queue limit** -- N commands queued inside MULTI before EXEC
  4. **Large value pipeline** -- 100 SET commands with values of size S bytes
  5. **Buffer accumulation** -- partial RESP3 frames drip-fed to force buffer growth
  6. **Concurrent pipelines** -- M clients each sending K commands simultaneously
  7. **Single massive value** -- one SET with a very large value
  """

  use ExUnit.Case, async: false

  @moduletag :bench

  alias Ferricstore.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # -------------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------------

  defp connect do
    port = Listener.port()

    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true,
        sndbuf: 16_777_216,
        recbuf: 16_777_216
      ])

    sock
  end

  defp encode_command(args) do
    IO.iodata_to_binary(Encoder.encode(args))
  end

  defp format_bytes(bytes) when bytes < 1_024, do: "#{bytes}B"

  defp format_bytes(bytes) when bytes < 1_048_576,
    do: "#{Float.round(bytes / 1_024, 1)}KB"

  defp format_bytes(bytes) when bytes < 1_073_741_824,
    do: "#{Float.round(bytes / 1_048_576, 1)}MB"

  defp format_bytes(bytes),
    do: "#{Float.round(bytes / 1_073_741_824, 2)}GB"

  defp format_time(us) when us < 1_000, do: "#{us}us"
  defp format_time(us) when us < 1_000_000, do: "#{Float.round(us / 1_000, 1)}ms"
  defp format_time(us), do: "#{Float.round(us / 1_000_000, 2)}s"

  defp drain_responses(sock, n, timeout \\ 60_000) do
    do_drain(sock, n, "", [], timeout)
  end

  defp do_drain(_sock, 0, _buf, acc, _timeout), do: Enum.reverse(acc)

  defp do_drain(sock, remaining, buf, acc, timeout) do
    case Parser.parse(buf) do
      {:ok, [], rest} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} ->
            do_drain(sock, remaining, rest <> data, acc, timeout)

          {:error, reason} ->
            Enum.reverse([{:recv_error, reason} | acc])
        end

      {:ok, values, rest} ->
        taken = min(length(values), remaining)
        {use, _discard} = Enum.split(values, taken)
        do_drain(sock, remaining - taken, rest, Enum.reverse(use) ++ acc, timeout)

      {:error, reason} ->
        Enum.reverse([{:parse_error, reason} | acc])
    end
  end

  defp uprefix, do: "bench_#{:erlang.unique_integer([:positive])}"

  defp alive?(sock) do
    try do
      :ok = :gen_tcp.send(sock, encode_command(["PING"]))
      resp = drain_responses(sock, 1, 5_000)
      match?([{:simple, "PONG"}], resp) or match?([{:simple, "PONG"} | _], resp)
    catch
      _, _ -> false
    end
  end

  defp send_chunked(sock, data, chunk_size) do
    if byte_size(data) <= chunk_size do
      :ok = :gen_tcp.send(sock, data)
    else
      <<chunk::binary-size(chunk_size), rest::binary>> = data
      :ok = :gen_tcp.send(sock, chunk)
      send_chunked(sock, rest, chunk_size)
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 1: Pipeline size limit (PING -- no Raft, pure connection stress)
  # -------------------------------------------------------------------------

  describe "pipeline size limit (PING)" do
    @tag timeout: 600_000
    test "finds breaking point for pipeline batch size using PING" do
      IO.puts("\n=== Pipeline Size Benchmark (PING, no Raft) ===")
      IO.puts("  Sending N PING commands in a single TCP write.\n")

      sizes = [100, 1_000, 10_000, 100_000, 500_000, 1_000_000]

      Enum.reduce_while(sizes, :ok, fn size, _acc ->
        sock = connect()
        Process.sleep(10)

        :ok = :gen_tcp.send(sock, encode_command(["PING"]))
        _pong = drain_responses(sock, 1)

        # Build pipeline of N PING commands.
        ping_cmd = encode_command(["PING"])
        raw = :binary.copy(ping_cmd, size)
        payload_mb = Float.round(byte_size(raw) / 1_048_576, 2)

        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {duration_us, send_result} =
          :timer.tc(fn ->
            try do
              case :gen_tcp.send(sock, raw) do
                :ok ->
                  _responses = drain_responses(sock, size, 300_000)
                  :ok

                {:error, reason} ->
                  {:error, "send: #{inspect(reason)}"}
              end
            catch
              kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
            end
          end)

        mem_after = :erlang.memory(:total)
        mem_delta = abs(mem_after - mem_before)

        is_alive = alive?(sock)

        status =
          case {send_result, is_alive} do
            {:ok, true} -> "OK"
            {:ok, false} -> "CRASHED"
            {{:error, msg}, _} -> "ERROR (#{String.slice(msg, 0, 50)})"
          end

        IO.puts(
          "  #{String.pad_trailing("#{size} cmds (#{payload_mb}MB)", 32)} " <>
            "mem_delta=#{String.pad_trailing(format_bytes(mem_delta), 10)}  " <>
            "time=#{String.pad_trailing(format_time(duration_us), 10)}  " <>
            status
        )

        :gen_tcp.close(sock)

        if is_alive, do: {:cont, :ok}, else: {:halt, :crashed}
      end)

      IO.puts("")
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 2: Pipeline size limit (SET -- with Raft)
  # -------------------------------------------------------------------------

  describe "pipeline size limit (SET)" do
    @tag timeout: 600_000
    test "finds breaking point for SET pipeline batch size" do
      IO.puts("\n=== Pipeline Size Benchmark (SET, with Raft) ===")
      IO.puts("  Sending N SET commands in a single TCP write.\n")

      # Reduced sizes since each SET goes through Raft consensus.
      sizes = [100, 1_000, 5_000, 10_000]

      Enum.reduce_while(sizes, :ok, fn size, _acc ->
        prefix = uprefix()
        sock = connect()
        Process.sleep(10)

        :ok = :gen_tcp.send(sock, encode_command(["PING"]))
        _pong = drain_responses(sock, 1)

        raw =
          IO.iodata_to_binary(
            for i <- 1..size do
              Encoder.encode(["SET", "#{prefix}_#{i}", "v"])
            end
          )

        payload_mb = Float.round(byte_size(raw) / 1_048_576, 2)

        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {duration_us, _send_result} =
          :timer.tc(fn ->
            try do
              case :gen_tcp.send(sock, raw) do
                :ok ->
                  drain_responses(sock, size, 300_000)
                  :ok

                {:error, reason} ->
                  {:error, "send: #{inspect(reason)}"}
              end
            catch
              kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
            end
          end)

        mem_after = :erlang.memory(:total)
        mem_delta = abs(mem_after - mem_before)

        is_alive = alive?(sock)
        status_str = if is_alive, do: "OK", else: "CRASHED"

        IO.puts(
          "  #{String.pad_trailing("#{size} cmds (#{payload_mb}MB)", 32)} " <>
            "mem_delta=#{String.pad_trailing(format_bytes(abs(mem_delta)), 10)}  " <>
            "time=#{String.pad_trailing(format_time(duration_us), 10)}  " <>
            status_str
        )

        :gen_tcp.close(sock)

        if is_alive, do: {:cont, :ok}, else: {:halt, :crashed}
      end)

      IO.puts("")
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 3: MULTI queue limit
  # -------------------------------------------------------------------------

  describe "MULTI queue limit" do
    @tag timeout: 600_000
    test "finds breaking point for commands queued in MULTI" do
      IO.puts("\n=== MULTI Queue Size Benchmark ===")
      IO.puts("  Queueing N SET commands inside MULTI, then EXEC.\n")

      sizes = [100, 1_000, 10_000, 50_000, 100_000, 250_000, 500_000]

      Enum.reduce_while(sizes, :ok, fn size, _acc ->
        prefix = uprefix()
        sock = connect()
        Process.sleep(10)

        :ok = :gen_tcp.send(sock, encode_command(["PING"]))
        _pong = drain_responses(sock, 1)

        :ok = :gen_tcp.send(sock, encode_command(["MULTI"]))
        [multi_resp] = drain_responses(sock, 1)
        assert multi_resp == {:simple, "OK"}

        chunk_size = min(size, 10_000)

        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {queue_us, _} =
          :timer.tc(fn ->
            Enum.each(1..size, fn i ->
              :ok = :gen_tcp.send(sock, encode_command(["SET", "#{prefix}_#{i}", "v"]))

              if rem(i, chunk_size) == 0 do
                _queued = drain_responses(sock, chunk_size, 60_000)
              end
            end)

            remaining = rem(size, chunk_size)

            if remaining > 0 do
              _queued = drain_responses(sock, remaining, 60_000)
            end
          end)

        mem_after_queue = :erlang.memory(:total)

        {exec_us, exec_result} =
          :timer.tc(fn ->
            try do
              :ok = :gen_tcp.send(sock, encode_command(["EXEC"]))
              [resp] = drain_responses(sock, 1, 300_000)
              {:ok, resp}
            catch
              kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
            end
          end)

        mem_after_exec = :erlang.memory(:total)

        is_alive = alive?(sock)

        exec_ok? =
          case exec_result do
            {:ok, results} when is_list(results) -> true
            _ -> false
          end

        status =
          cond do
            not is_alive -> "CRASHED"
            not exec_ok? -> "EXEC FAILED"
            true -> "OK"
          end

        queue_mem = abs(mem_after_queue - mem_before)
        total_mem = abs(mem_after_exec - mem_before)

        IO.puts(
          "  #{String.pad_trailing("#{size} cmds", 20)} " <>
            "queue_mem=#{String.pad_trailing(format_bytes(queue_mem), 10)} " <>
            "total_mem=#{String.pad_trailing(format_bytes(total_mem), 10)} " <>
            "queue_time=#{String.pad_trailing(format_time(queue_us), 10)} " <>
            "exec_time=#{String.pad_trailing(format_time(exec_us), 10)} " <>
            status
        )

        :gen_tcp.close(sock)

        if is_alive, do: {:cont, :ok}, else: {:halt, :crashed}
      end)

      IO.puts("")
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 4: Large value pipeline
  # -------------------------------------------------------------------------

  describe "large value pipeline" do
    @tag timeout: 600_000
    test "finds breaking point for value sizes in pipeline" do
      IO.puts("\n=== Large Value Pipeline Benchmark ===")
      IO.puts("  Sending 100 SET commands with values of size S bytes.\n")

      value_sizes = [1_000, 10_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]
      num_commands = 100

      Enum.reduce_while(value_sizes, :ok, fn val_size, _acc ->
        prefix = uprefix()
        sock = connect()
        Process.sleep(10)

        :ok = :gen_tcp.send(sock, encode_command(["PING"]))
        _pong = drain_responses(sock, 1)

        value = :binary.copy(<<0>>, val_size)

        raw =
          IO.iodata_to_binary(
            for i <- 1..num_commands do
              Encoder.encode(["SET", "#{prefix}_#{i}", value])
            end
          )

        payload_mb = Float.round(byte_size(raw) / 1_048_576, 2)

        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {duration_us, _send_result} =
          :timer.tc(fn ->
            try do
              # For large payloads, send in 1MB chunks to avoid gen_tcp send buffer issues.
              send_chunked(sock, raw, 1_048_576)
              drain_responses(sock, num_commands, 120_000)
              :ok
            catch
              kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
            end
          end)

        mem_after = :erlang.memory(:total)
        mem_delta = abs(mem_after - mem_before)

        is_alive = alive?(sock)
        status = if is_alive, do: "OK", else: "CRASHED"

        IO.puts(
          "  #{String.pad_trailing("#{num_commands}x #{format_bytes(val_size)} (#{payload_mb}MB total)", 44)} " <>
            "mem_delta=#{String.pad_trailing(format_bytes(mem_delta), 10)}  " <>
            "time=#{String.pad_trailing(format_time(duration_us), 10)}  " <>
            status
        )

        :gen_tcp.close(sock)
        :erlang.garbage_collect()

        if is_alive, do: {:cont, :ok}, else: {:halt, :crashed}
      end)

      IO.puts("")
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 5: Buffer accumulation (partial frames)
  # -------------------------------------------------------------------------

  describe "buffer accumulation" do
    @tag timeout: 600_000
    test "finds breaking point for buffer growth from partial RESP3 frames" do
      IO.puts("\n=== Buffer Accumulation Benchmark ===")
      IO.puts("  Sending partial RESP3 frames to force buffer accumulation.\n")

      buffer_sizes = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 50_000_000]

      Enum.reduce_while(buffer_sizes, :ok, fn target_size, _acc ->
        sock = connect()
        Process.sleep(10)

        :ok = :gen_tcp.send(sock, encode_command(["PING"]))
        _pong = drain_responses(sock, 1)

        key = "#{uprefix()}_buf"
        value = :binary.copy("A", target_size)
        full_cmd = encode_command(["SET", key, value])
        total_len = byte_size(full_cmd)

        chunk_size = 65_536

        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {duration_us, _send_result} =
          :timer.tc(fn ->
            try do
              send_chunked(sock, full_cmd, chunk_size)
              drain_responses(sock, 1, 120_000)
              :ok
            catch
              kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
            end
          end)

        mem_after = :erlang.memory(:total)
        mem_delta = abs(mem_after - mem_before)

        is_alive = alive?(sock)
        status = if is_alive, do: "OK", else: "CRASHED"

        IO.puts(
          "  #{String.pad_trailing("buffer #{format_bytes(target_size)} (#{format_bytes(total_len)} wire)", 44)} " <>
            "mem_delta=#{String.pad_trailing(format_bytes(mem_delta), 10)}  " <>
            "time=#{String.pad_trailing(format_time(duration_us), 10)}  " <>
            status
        )

        :gen_tcp.close(sock)
        :erlang.garbage_collect()

        if is_alive, do: {:cont, :ok}, else: {:halt, :crashed}
      end)

      IO.puts("")
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 6: Concurrent pipelines
  # -------------------------------------------------------------------------

  describe "concurrent pipelines" do
    @tag timeout: 600_000
    test "concurrent clients each sending PING pipelines" do
      IO.puts("\n=== Concurrent Pipeline Benchmark ===")
      IO.puts("  M clients each sending 10,000 PING commands in pipeline.\n")

      cmds_per_client = 10_000
      client_counts = [1, 5, 10, 25, 50, 100, 200]

      Enum.reduce_while(client_counts, :ok, fn num_clients, _acc ->
        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {duration_us, results} =
          :timer.tc(fn ->
            tasks =
              for _client_id <- 1..num_clients do
                Task.async(fn ->
                  run_client_ping_pipeline(cmds_per_client)
                end)
              end

            Enum.map(tasks, fn task ->
              Task.await(task, 120_000)
            end)
          end)

        mem_after = :erlang.memory(:total)
        mem_delta = abs(mem_after - mem_before)

        successes = Enum.count(results, fn r -> r == :ok end)
        failures = num_clients - successes

        status =
          cond do
            failures == 0 -> "OK"
            failures < num_clients -> "#{failures}/#{num_clients} FAILED"
            true -> "ALL FAILED"
          end

        throughput =
          if duration_us > 0 do
            total_cmds = num_clients * cmds_per_client
            Float.round(total_cmds / (duration_us / 1_000_000), 0)
          else
            0
          end

        IO.puts(
          "  #{String.pad_trailing("#{num_clients} clients x #{cmds_per_client} cmds", 32)} " <>
            "mem_delta=#{String.pad_trailing(format_bytes(mem_delta), 10)}  " <>
            "time=#{String.pad_trailing(format_time(duration_us), 10)}  " <>
            "#{String.pad_trailing("#{round(throughput)} cmd/s", 16)}  " <>
            status
        )

        if failures == num_clients, do: {:halt, :crashed}, else: {:cont, :ok}
      end)

      IO.puts("")
    end
  end

  defp run_client_ping_pipeline(num_commands) do
    try do
      sock = connect()

      :ok = :gen_tcp.send(sock, encode_command(["PING"]))
      _pong = drain_responses(sock, 1)

      ping_cmd = encode_command(["PING"])
      raw = :binary.copy(ping_cmd, num_commands)

      :ok = :gen_tcp.send(sock, raw)
      _responses = drain_responses(sock, num_commands, 60_000)

      :ok = :gen_tcp.send(sock, encode_command(["PING"]))
      _pong = drain_responses(sock, 1, 5_000)

      :gen_tcp.close(sock)
      :ok
    catch
      _, _ -> :error
    end
  end

  # -------------------------------------------------------------------------
  # Benchmark 7: Single massive value (edge case)
  # -------------------------------------------------------------------------

  describe "single massive value" do
    @tag timeout: 600_000
    test "finds max single value size the connection can handle" do
      IO.puts("\n=== Single Value Size Benchmark ===")
      IO.puts("  Sending one SET with increasingly large values.\n")

      value_sizes = [
        100_000,
        1_000_000,
        10_000_000,
        50_000_000,
        100_000_000,
        256_000_000
      ]

      Enum.reduce_while(value_sizes, :ok, fn val_size, _acc ->
        prefix = uprefix()
        sock = connect()
        Process.sleep(10)

        :ok = :gen_tcp.send(sock, encode_command(["PING"]))
        _pong = drain_responses(sock, 1)

        value = :binary.copy("X", val_size)
        key = "#{prefix}_big"

        :erlang.garbage_collect()
        mem_before = :erlang.memory(:total)

        {duration_us, result} =
          :timer.tc(fn ->
            try do
              raw = encode_command(["SET", key, value])
              send_chunked(sock, raw, 1_048_576)
              drain_responses(sock, 1, 120_000)
              :ok
            catch
              kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
            end
          end)

        mem_after = :erlang.memory(:total)
        mem_delta = abs(mem_after - mem_before)

        is_alive = alive?(sock)

        status_str =
          case {result, is_alive} do
            {:ok, true} -> "OK"
            {:ok, false} -> "CRASHED (died after success)"
            {{:error, msg}, _} -> "ERROR (#{String.slice(inspect(msg), 0, 60)})"
          end

        IO.puts(
          "  #{String.pad_trailing("1x #{format_bytes(val_size)}", 28)} " <>
            "mem_delta=#{String.pad_trailing(format_bytes(mem_delta), 10)}  " <>
            "time=#{String.pad_trailing(format_time(duration_us), 10)}  " <>
            status_str
        )

        # Cleanup: delete the big key to free memory.
        if is_alive do
          try do
            :ok = :gen_tcp.send(sock, encode_command(["DEL", key]))
            drain_responses(sock, 1, 5_000)
          catch
            _, _ -> :ok
          end
        end

        :gen_tcp.close(sock)
        :erlang.garbage_collect()

        if is_alive, do: {:cont, :ok}, else: {:halt, :crashed}
      end)

      IO.puts("")
    end
  end

  # -------------------------------------------------------------------------
  # Summary: recommendations
  # -------------------------------------------------------------------------

  @l1_max_entries Application.compile_env(:ferricstore, :l1_cache_max_entries, 64)
  @l1_max_bytes Application.compile_env(:ferricstore, :l1_cache_max_bytes, 1_048_576)

  describe "summary" do
    @tag timeout: 10_000
    test "prints recommendations based on connection.ex analysis" do
      IO.puts("\n=== Connection Memory Analysis & Recommendations ===\n")

      IO.puts("""
        Current state of connection.ex memory protections:
        --------------------------------------------------
        - buffer:        NO LIMIT -- binary concatenation grows unbounded
        - multi_queue:   NO LIMIT -- list grows unbounded during MULTI
        - pipeline size: NO LIMIT -- parsed command list is unbounded
        - value size:    NO LIMIT -- values pass through as-is
        - L1 cache:      BOUNDED  -- #{inspect(@l1_max_entries)} entries, #{inspect(@l1_max_bytes)} bytes max

        Recommended limits to add to connection.ex:
        --------------------------------------------------
        1. MAX_BUFFER_SIZE (default 256MB):
           In handle_data/2, check byte_size(buffer) before concatenation.
           If exceeded, send -ERR and close the connection.

        2. MAX_MULTI_QUEUE_LENGTH (default 100,000):
           In the MULTI queuing clause, check length(multi_queue).
           If exceeded, DISCARD the transaction and send -ERR.

        3. MAX_PIPELINE_SIZE (default 100,000):
           In handle_parsed/2, check length(commands).
           If exceeded, send -ERR and close or reject the batch.

        4. MAX_VALUE_SIZE (default 64MB):
           Check at SET/APPEND time in the dispatcher, not the connection.
           But the connection's buffer is what holds the raw bytes.

        5. MAX_REQUEST_SIZE (per-command, default 512MB):
           In the RESP parser, reject individual bulk strings > limit.
           This is the most effective single protection.

        6. IDLE_TIMEOUT (default 300s):
           Add an `after` clause to the receive loop.
           Close connections that have been idle too long.
      """)
    end
  end
end
