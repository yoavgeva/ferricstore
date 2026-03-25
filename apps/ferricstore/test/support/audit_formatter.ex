defmodule Ferricstore.Test.AuditFormatter do
  @moduledoc """
  ExUnit formatter that logs every test result and ETS state snapshot to
  `/tmp/ferricstore_test_audit.log`.

  Registered via the `:formatters` option in `ExUnit.start/1`. Runs alongside
  the default CLIFormatter so normal terminal output is unaffected.

  Each line records pass/fail, duration, test name, and a snapshot of ETS
  keydir/prefix counts across all shards — useful for spotting state leaks
  between tests.
  """

  use GenServer

  @log_path "/tmp/ferricstore_test_audit.log"

  # -- GenServer callbacks (ExUnit formatter protocol) --

  @impl true
  def init(_opts) do
    # Only clear log on first app's suite start (core app).
    # Subsequent apps (server, ecto) append to the same file.
    if not File.exists?(@log_path) or first_app?() do
      File.write!(@log_path, "=== Suite started #{timestamp()} ===\n")
    else
      File.write!(@log_path, "\n=== App started #{timestamp()} ===\n", [:append])
    end
    {:ok, %{}}
  end

  defp first_app? do
    case System.get_env("FERRICSTORE_AUDIT_STARTED") do
      nil ->
        System.put_env("FERRICSTORE_AUDIT_STARTED", "1")
        true
      _ ->
        false
    end
  end

  @impl true
  def handle_cast({:test_finished, %ExUnit.Test{} = test}, state) do
    status = if test.state == nil, do: "PASS", else: "FAIL"
    duration_ms = div(test.time, 1000)

    shard_count = try_shard_count()
    {ets_count, prefix_count} = ets_snapshot(shard_count)
    {writer_pending, batcher_pending} = queue_snapshot(shard_count)
    pt_count = try do :persistent_term.info().count rescue _ -> 0 end
    proc_count = length(Process.list())
    {bitcask_files, bitcask_bytes} = bitcask_snapshot(shard_count)

    leak_parts =
      []
      |> maybe_append(writer_pending > 0, "writer_pending=#{writer_pending}")
      |> maybe_append(batcher_pending > 0, "batcher_pending=#{batcher_pending}")

    leak_tag = if leak_parts == [], do: "", else: " QUEUED: [#{Enum.join(leak_parts, ", ")}]"

    module = inspect(test.module)
    line =
      "[#{timestamp()}] #{status} (#{duration_ms}ms) #{module} > #{test.name} | " <>
        "ets=#{ets_count} prefix=#{prefix_count} pt=#{pt_count} procs=#{proc_count} " <>
        "disk_files=#{bitcask_files} disk_bytes=#{bitcask_bytes}#{leak_tag}\n"

    File.write!(@log_path, line, [:append])

    {:noreply, state}
  end

  def handle_cast({:suite_finished, _}, state) do
    {:noreply, state}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  # -- Internals --

  defp try_shard_count do
    :persistent_term.get(:ferricstore_shard_count, 4)
  rescue
    _ -> 4
  end

  defp ets_snapshot(shard_count) do
    Enum.reduce(0..(shard_count - 1), {0, 0}, fn i, {ets_acc, prefix_acc} ->
      ets =
        try do
          :ets.info(:"keydir_#{i}", :size)
        rescue
          _ -> 0
        end

      prefix =
        try do
          :ets.info(:"prefix_keys_#{i}", :size)
        rescue
          _ -> 0
        end

      {ets_acc + (ets || 0), prefix_acc + (prefix || 0)}
    end)
  end

  defp queue_snapshot(shard_count) do
    Enum.reduce(0..(shard_count - 1), {0, 0}, fn i, {w_acc, b_acc} ->
      writer_len = message_queue_len(:"Ferricstore.Store.BitcaskWriter.#{i}")
      batcher_len = message_queue_len(:"Ferricstore.Raft.Batcher.#{i}")
      {w_acc + writer_len, b_acc + batcher_len}
    end)
  end

  defp bitcask_snapshot(shard_count) do
    data_dir =
      try do
        Application.fetch_env!(:ferricstore, :data_dir)
      rescue
        _ -> nil
      end

    if data_dir do
      Enum.reduce(0..(shard_count - 1), {0, 0}, fn i, {files_acc, bytes_acc} ->
        shard_dir = Path.join(data_dir, "shard_#{i}")

        case File.ls(shard_dir) do
          {:ok, entries} ->
            log_files = Enum.filter(entries, &String.ends_with?(&1, ".log"))
            total_bytes = Enum.reduce(log_files, 0, fn f, acc ->
              case File.stat(Path.join(shard_dir, f)) do
                {:ok, %{size: size}} -> acc + size
                _ -> acc
              end
            end)
            {files_acc + length(log_files), bytes_acc + total_bytes}

          _ ->
            {files_acc, bytes_acc}
        end
      end)
    else
      {0, 0}
    end
  end

  defp message_queue_len(name) do
    case Process.whereis(name) do
      pid when is_pid(pid) ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, len} -> len
          _ -> 0
        end

      nil ->
        0
    end
  end

  defp maybe_append(list, true, item), do: list ++ [item]
  defp maybe_append(list, false, _item), do: list

  defp timestamp do
    {{y, m, d}, {h, min, s}} = :calendar.local_time()

    :io_lib.format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B", [y, m, d, h, min, s])
    |> IO.iodata_to_binary()
  end
end
