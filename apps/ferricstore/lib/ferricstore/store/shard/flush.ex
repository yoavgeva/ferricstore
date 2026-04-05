defmodule Ferricstore.Store.Shard.Flush do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  require Logger

  # Timeout for synchronous flush (blocking receive for async completion).
  @sync_flush_timeout_ms 5_000

  # Record header size for dead byte accounting (same as @bitcask_header_size).
  @record_header_size 26

  # -------------------------------------------------------------------
  # Flush pending writes
  # -------------------------------------------------------------------

  # Async flush — used by the timer and by put (first-write-in-window).
  # Writes to page cache only (no fsync) — durability comes from the
  # periodic fsync on the flush timer. This reduces per-write latency
  # from ~50-200us (NVMe fsync) to ~1-10us (memcpy to page cache).
  # If a flush is already in-flight or pending is empty, this is a no-op.
  @doc false
  def flush_pending(%{pending: []} = state), do: state
  def flush_pending(%{flush_in_flight: op_id} = state) when op_id != nil, do: state

  def flush_pending(%{pending: pending} = state) do
    raw_batch = Enum.reverse(pending)
    batch = Enum.map(raw_batch, fn {key, value, exp} ->
      {key, ShardETS.to_disk_binary(value), exp}
    end)

    state = maybe_rotate_file(state)

    case NIF.v2_append_batch_nosync(state.active_file_path, batch) do
      {:ok, locations} ->
        Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.index)
        written = total_written(locations)
        state = update_ets_locations(state, batch, locations)
        state = track_flush_bytes(state, written)
        state = %{state | pending: [], pending_count: 0, fsync_needed: true,
          active_file_size: state.active_file_size + written}
        maybe_notify_fragmentation(state)

      {:error, reason} ->
        Ferricstore.Store.DiskPressure.set(state.instance_ctx, state.index)
        Logger.error("Shard #{state.index}: flush_pending (nosync) failed: #{inspect(reason)} — retaining #{length(raw_batch)} pending entries")
        state
    end
  end

  # Synchronous flush — used by delete, :flush, and :keys calls that need
  # durability guarantees. Uses v2_append_batch (write + fsync in one call).
  # Also ensures any previously-nosync'd data is fsynced.
  @doc false
  def flush_pending_sync(%{pending: []} = state) do
    # Even with empty pending, we may need to fsync previously-nosync'd data.
    if state.fsync_needed do
      NIF.v2_fsync(state.active_file_path)
      %{state | fsync_needed: false}
    else
      state
    end
  end

  def flush_pending_sync(%{pending: pending} = state) do
    raw_batch = Enum.reverse(pending)
    batch = Enum.map(raw_batch, fn {key, value, exp} ->
      {key, ShardETS.to_disk_binary(value), exp}
    end)

    state = maybe_rotate_file(state)

    case NIF.v2_append_batch(state.active_file_path, batch) do
      {:ok, locations} ->
        Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.index)
        written = total_written(locations)
        state = update_ets_locations(state, batch, locations)
        state = track_flush_bytes(state, written)
        state = %{state | pending: [], pending_count: 0, fsync_needed: false,
          active_file_size: state.active_file_size + written}
        maybe_notify_fragmentation(state)

      {:error, reason} ->
        Ferricstore.Store.DiskPressure.set(state.instance_ctx, state.index)
        Logger.error("Shard #{state.index}: flush_pending_sync failed: #{inspect(reason)} — retaining #{length(raw_batch)} pending entries")
        state
    end
  end

  # -------------------------------------------------------------------
  # Await in-flight async flush
  # -------------------------------------------------------------------

  # Wait for any in-flight async fsync to complete before proceeding.
  # This blocks the GenServer until the Tokio fsync result arrives.
  # Used before durability-critical operations (delete, keys, explicit flush).
  @doc false
  def await_in_flight(%{flush_in_flight: nil} = state), do: state

  def await_in_flight(%{flush_in_flight: corr_id} = state) do
    receive do
      {:tokio_complete, ^corr_id, :ok, :ok} ->
        %{state | flush_in_flight: nil}

      {:tokio_complete, ^corr_id, :error, _reason} ->
        # Fsync failed — log at caller site if needed. Clear in-flight.
        %{state | flush_in_flight: nil}
    after
      @sync_flush_timeout_ms ->
        # Timeout — clear in-flight to avoid permanent blocking.
        Logger.error("Shard #{state.index}: await_in_flight timed out for corr_id #{corr_id}")
        %{state | flush_in_flight: nil}
    end
  end

  # -------------------------------------------------------------------
  # ETS location updates after flush
  # -------------------------------------------------------------------

  @doc false
  def update_ets_locations(state, batch, locations) do
    fid = state.active_file_id

    new_file_stats =
      Enum.zip(batch, locations)
      |> Enum.reduce(state.file_stats, fn {{key, value, _exp}, {offset, _record_size}}, fs ->
        # Use update_element for the disk-location fields only.
        # This preserves the LFU counter (position 4) and is a single
        # ETS operation — no lookup+insert round-trip needed.
        # Positions: {1=key, 2=value, 3=exp, 4=lfu, 5=fid, 6=offset, 7=vsize}
        #
        # vsize is computed from the original batch value (not the ETS value)
        # because large values are stored as nil in ETS due to
        # hot_cache_max_value_size, which would incorrectly produce vsize=0.
        case :ets.lookup(state.keydir, key) do
          [{^key, _ets_value, _exp, _lfu, old_fid, _old_off, old_vsize}] ->
            vsize = byte_size(value)
            :ets.update_element(state.keydir, key, [
              {5, fid}, {6, offset}, {7, vsize}
            ])

            # Track dead bytes: if the old entry was on disk (not :pending/0),
            # the overwritten data is now dead in the old file.
            if old_fid != 0 and old_vsize > 0 do
              dead_increment = old_vsize + @record_header_size + byte_size(key)
              {old_total, old_dead} = Map.get(fs, old_fid, {0, 0})
              Map.put(fs, old_fid, {old_total, old_dead + dead_increment})
            else
              fs
            end

          [] ->
            fs
        end
      end)

    %{state | file_stats: new_file_stats}
  end

  # -------------------------------------------------------------------
  # Byte tracking / fragmentation
  # -------------------------------------------------------------------

  @doc false
  def total_written(locations) do
    Enum.reduce(locations, 0, fn {_offset, size}, acc -> acc + size end)
  end

  # Increment total_bytes for the active file after a flush.
  @doc false
  def track_flush_bytes(state, written_bytes) do
    fid = state.active_file_id
    {total, dead} = Map.get(state.file_stats, fid, {0, 0})
    %{state | file_stats: Map.put(state.file_stats, fid, {total + written_bytes, dead})}
  end

  # Track dead bytes when a key is deleted via tombstone (direct path only).
  # Reads the old ETS entry to determine which file contains the now-dead record.
  @doc false
  def track_delete_dead_bytes(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, _v, _exp, _lfu, old_fid, _off, old_vsize}] when old_fid != 0 and old_vsize > 0 ->
        dead_increment = old_vsize + @record_header_size + byte_size(key)
        {old_total, old_dead} = Map.get(state.file_stats, old_fid, {0, 0})
        %{state | file_stats: Map.put(state.file_stats, old_fid, {old_total, old_dead + dead_increment})}

      _ ->
        state
    end
  end

  # Check if any non-active file exceeds fragmentation thresholds and notify
  # the merge scheduler. Cheap: iterates a small map (typically <20 files).
  @doc false
  def maybe_notify_fragmentation(state) do
    frag_threshold = state.merge_config.fragmentation_threshold
    dead_bytes_min = state.merge_config.dead_bytes_threshold

    candidates =
      state.file_stats
      |> Enum.filter(fn {fid, {total, dead}} ->
        fid != state.active_file_id and
          total > 0 and
          dead / total >= frag_threshold and
          dead >= dead_bytes_min
      end)
      |> Enum.map(fn {fid, _} -> fid end)

    if candidates != [] do
      file_count = map_size(state.file_stats)
      Ferricstore.Merge.Scheduler.notify_fragmentation(state.index, candidates, file_count)
    end

    state
  end

  # -------------------------------------------------------------------
  # File stats / rotation / hints
  # -------------------------------------------------------------------

  # Compute per-file dead bytes stats from disk file sizes + ETS live data.
  # Called once during init after recover_keydir. O(file_count + key_count).
  @doc false
  def compute_file_stats(shard_path, keydir) do
    case File.ls(shard_path) do
      {:ok, files} ->
        # 1. Get total bytes per file from disk
        file_totals =
          files
          |> Enum.filter(&String.ends_with?(&1, ".log"))
          |> Enum.reject(&String.starts_with?(&1, "compact_"))
          |> Enum.reduce(%{}, fn name, acc ->
            fid = name |> String.trim_trailing(".log") |> String.to_integer()
            full_path = Path.join(shard_path, name)

            size =
              case File.stat(full_path) do
                {:ok, %{size: s}} -> s
                _ -> 0
              end

            Map.put(acc, fid, size)
          end)

        # 2. Sum live bytes per file from ETS (record_header + key + value per entry)
        live_per_file =
          :ets.foldl(
            fn {key, _value, _exp, _lfu, fid, _off, vsize}, acc ->
              if fid != 0 and is_integer(fid) do
                record_bytes = @record_header_size + byte_size(key) + vsize
                Map.update(acc, fid, record_bytes, &(&1 + record_bytes))
              else
                acc
              end
            end,
            %{},
            keydir
          )

        # 3. dead_bytes = total_bytes - live_bytes per file
        Map.new(file_totals, fn {fid, total} ->
          live = Map.get(live_per_file, fid, 0)
          dead = max(total - live, 0)
          {fid, {total, dead}}
        end)

      _ ->
        %{}
    end
  end

  @doc false
  def maybe_rotate_file(state) do
    if state.active_file_size >= state.max_active_file_size do
      write_hint_for_file(state, state.active_file_id)
      new_id = state.active_file_id + 1
      sp = state.shard_data_path
      new_path = ShardETS.file_path(sp, new_id)
      File.touch!(new_path)
      Ferricstore.Store.ActiveFile.publish(state.index, new_id, new_path, sp)

      # Initialize file_stats for the new file
      new_file_stats = Map.put(state.file_stats, new_id, {0, 0})

      # Notify the merge scheduler that a rotation happened.
      # file_count = new_id + 1 (files are 0-indexed: 0, 1, ..., new_id)
      Ferricstore.Merge.Scheduler.notify_rotation(state.index, new_id + 1)

      %{state | active_file_id: new_id, active_file_path: new_path, active_file_size: 0,
        file_stats: new_file_stats}
    else
      state
    end
  end

  @doc false
  def write_hint_for_file(state, target_fid) do
    sp = state.shard_data_path
    hint_path = Path.join(sp, "#{String.pad_leading(Integer.to_string(target_fid), 5, "0")}.hint")

    entries =
      :ets.foldl(
        fn {key, _value, exp, _lfu, fid, off, vsize}, acc ->
          if fid == target_fid do
            # NIF expects: {key, file_id, offset, value_size, expire_at_ms}
            [{key, target_fid, off, vsize, exp} | acc]
          else
            acc
          end
        end,
        [],
        state.keydir
      )

    if entries != [] do
      NIF.v2_write_hint_file(hint_path, entries)
    end
  end

  # -------------------------------------------------------------------
  # Schedule flush timer
  # -------------------------------------------------------------------

  @doc false
  def schedule_flush(ms) do
    Process.send_after(self(), :flush, ms)
  end
end
