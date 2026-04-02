defmodule Ferricstore.Store.Shard do
  @moduledoc """
  GenServer managing one Bitcask partition backed by an ETS hot-read cache.

  ## v2 Architecture: Pure Stateless NIFs

  All state lives in Elixir (ETS keydir + GenServer state). Rust NIFs are
  pure stateless functions: `v2_append_record`, `v2_pread_at`, `v2_fsync`,
  `v2_append_batch`, `v2_append_tombstone`, `v2_scan_file`, hint file I/O.
  No Rust-side Store resource, HashMap keydir, or Mutex.

  ## Write path: group commit

  1. The key is written to ETS immediately (reads see it at once).
  2. The entry is appended to an in-memory pending list.
  3. A recurring timer fires every `@flush_interval_ms` and calls
     `NIF.v2_append_batch/2` with all accumulated entries, then updates
     ETS entries with their disk locations (file_id, offset, value_size).
  4. File rotation occurs when the active file exceeds 256 MB.

  ## Read path: ETS bypass

  `Router.get/1` and `Router.get_meta/1` read ETS directly without going
  through this GenServer for hot (cached) keys. Cold keys (value=nil in ETS)
  have their disk location (file_id, offset) stored in the ETS 7-tuple,
  enabling direct `v2_pread_at` without scanning.

  ## ETS layout

  Each entry is a 7-tuple `{key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}`
  where `expire_at_ms = 0` means the key never expires. The `file_id`, `offset`,
  and `value_size` fields enable cold reads without scanning, STRLEN on cold keys,
  and sendfile zero-copy. Expired entries are lazily evicted on read.

  ## Process registration

  Shards register under the name returned by
  `Ferricstore.Store.Router.shard_name/1`, e.g.
  `:"Ferricstore.Store.Shard.0"`.
  """

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{BloomRegistry, LFU, PrefixIndex, Router, ValueCodec}

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1

  # Timeout for synchronous flush (blocking receive for async completion).
  @sync_flush_timeout_ms 5_000
  @default_sweep_interval_ms 1_000
  @default_max_keys_per_sweep 100

  # Default maximum active file size before rotation (256 MB).
  # Configurable via :max_active_file_size application env.
  @default_max_active_file_size 256 * 1024 * 1024

  # How often (ms) to re-evaluate fragmentation on idle shards.
  # Catches shards that accumulated dead data then went quiet.
  @default_frag_check_interval_ms 60_000

  # Record header size for dead byte accounting (same as @bitcask_header_size).
  @record_header_size 26

  # Default fragmentation thresholds for per-file dead bytes tracking.
  @default_fragmentation_threshold 0.5
  @default_dead_bytes_threshold 134_217_728

  # Promoted (dedicated) compaction thresholds.
  @promoted_frag_threshold 0.5
  @promoted_dead_bytes_min 1_048_576
  @promoted_compaction_cooldown_ms 30_000

  # Maximum pending entries before triggering a synchronous flush.
  # Bounds worst-case shard process heap growth during write bursts.
  @max_pending_size 10_000

  defstruct [
    :ets,
    :keydir,
    :prefix_keys,
    :index,
    :data_dir,
    # Cached result of DataDir.shard_data_path(data_dir, index).
    # Computed once during init; avoids string concat on every cold read/flush.
    :shard_data_path,
    :active_file_id,
    :active_file_path,
    :active_file_size,
    pending: [],
    pending_count: 0,
    flush_in_flight: nil,
    write_version: 0,
    sweep_at_ceiling_count: 0,
    sweep_struggling: false,
    promoted_instances: %{},
    # Per-file dead bytes tracking: %{file_id => {total_bytes, dead_bytes}}
    file_stats: %{},
    # Merge config overrides for fragmentation thresholds
    merge_config: %{},
    # Map from correlation_id => {from, key} for in-flight Tokio async reads.
    # Correlation IDs fix the LIFO ordering bug from the old list-based approach.
    pending_reads: %{},
    # Monotonically increasing counter for async read/write correlation IDs.
    next_correlation_id: 0,
    # Whether a deferred fsync is needed (set to true after nosync writes).
    fsync_needed: false,
    # Whether this shard has Raft infrastructure (Batcher + ra server).
    # Application-supervised shards (0-3) always have Raft. Isolated test
    # shards with ad-hoc indices use the direct write path instead.
    raft?: true,
    # Whether this shard is a sandbox shard (private ETS, no global name).
    sandbox?: false,
    # For sandbox shards with Raft: the private ra system and server ID.
    # When set, the shard submits directly to ra (no Batcher).
    sandbox_ra_system: nil,
    sandbox_ra_server_id: nil,
    # Maximum active file size before rotation. Cached from Application env
    # at init time. Updated via handle_cast(:update_max_active_file_size, n).
    max_active_file_size: 256 * 1024 * 1024
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a shard GenServer.

  ## Options

    * `:index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:flush_interval_ms` -- batch-commit interval in ms (default: #{@flush_interval_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)

    if opts[:sandbox] do
      GenServer.start_link(__MODULE__, opts)
    else
      name = Router.shard_name(index)
      GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true

  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
    sandbox? = Keyword.get(opts, :sandbox, false)

    path =
      if sandbox? do
        # Sandbox shards: data_dir IS the shard dir (already created by Sandbox.checkout)
        File.mkdir_p!(data_dir)
        data_dir
      else
        Ferricstore.DataDir.shard_data_path(data_dir, index)
      end

    File.mkdir_p!(path)

    # v2: scan data_dir for existing .log files, find highest file_id
    {active_file_id, active_file_size} = discover_active_file(path)
    active_file_path = file_path(path, active_file_id)

    # Ensure the active file exists (touch it)
    unless File.exists?(active_file_path) do
      File.touch!(active_file_path)
    end

    {keydir, prefix_keys} =
      if sandbox? do
        # Sandbox: use pre-created anonymous ETS tables
        kd = Keyword.fetch!(opts, :sandbox_keydir)
        pk = Keyword.fetch!(opts, :sandbox_prefix_keys)
        {kd, pk}
      else
        # Production: create/clear named ETS tables
        kd =
          case :ets.whereis(:"keydir_#{index}") do
            :undefined ->
              :ets.new(:"keydir_#{index}", [:set, :public, :named_table, {:read_concurrency, true}, {:write_concurrency, :auto}, {:decentralized_counters, true}])

            _ref ->
              :ets.delete_all_objects(:"keydir_#{index}")
              :"keydir_#{index}"
          end

        # Remove any leftover hot_cache table from a previous run.
        case :ets.whereis(:"hot_cache_#{index}") do
          :undefined -> :ok
          _ref -> :ets.delete(:"hot_cache_#{index}")
        end

        pk = PrefixIndex.create_table(index)
        {kd, pk}
      end

    ets = keydir

    # Create bloom registry only for non-sandbox shards
    unless sandbox? do
      BloomRegistry.create_table(index)
    end

    # v2: recover ETS keydir from hint files or by scanning log files BEFORE
    # starting Raft. This ensures cold entries ({key, nil, ..., fid, off, vsize})
    # are in ETS when ra replays WAL entries via apply/3. Without this, replayed
    # read-modify-write commands (INCR, APPEND, etc.) see ETS misses during
    # replay and start from nil instead of the correct prior value.
    # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
    # Must run BEFORE recover_promoted so PM: markers are in ETS.
    recover_keydir(path, keydir, prefix_keys, index)

    # Start the Raft server for this shard.
    # Application-supervised shards use the global ra system + Batcher.
    # Sandbox shards with a private ra system start their own ra server
    # and submit directly (no Batcher). Sandbox shards without an ra system
    # use the direct write path (ETS + Bitcask pending batch).
    sandbox_ra_system = Keyword.get(opts, :sandbox_ra_system)

    {raft?, sandbox_ra_server_id} =
      if sandbox? do
        if sandbox_ra_system do
          start_sandbox_raft(sandbox_ra_system, index, path, active_file_id, active_file_path, ets, prefix_keys, opts)
        else
          {false, nil}
        end
      else
        {start_raft_if_available(index, path, active_file_id, active_file_path, ets), nil}
      end

    # Recover promoted collection instances (skip for sandbox -- no promoted data)
    promoted =
      if sandbox? do
        %{}
      else
        Ferricstore.Store.Promotion.recover_promoted(path, keydir, data_dir, index)
      end

    # Rebuild HNSW vector indices (skip for sandbox)
    unless sandbox? do
      hnsw_get_fn = fn key ->
        case :ets.lookup(keydir, key) do
          [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil -> value
          [{^key, nil, _exp, _lfu, fid, off, _vsize}] ->
            p = file_path(path, fid)
            case NIF.v2_pread_at(p, off) do
              {:ok, v} -> v
              _ -> nil
            end
          _ -> nil
        end
      end

      Ferricstore.Store.HnswRegistry.rebuild_for_shard(path, index, hnsw_get_fn)
    end

    # Re-open all mmap-backed bloom filter files from disk (skip for sandbox)
    unless sandbox? do
      bloom_count = BloomRegistry.recover(data_dir, index)

      if bloom_count > 0 do
        Logger.debug("Shard #{index}: recovered #{bloom_count} bloom filter(s)")
      end
    end

    # Publish active file metadata to ActiveFile registry (skip for sandbox --
    # sandbox shards don't use the async write path through Router)
    unless sandbox? do
      Ferricstore.Store.ActiveFile.publish(index, active_file_id, active_file_path, path)
    end

    # Compute per-file dead bytes stats from disk sizes + ETS live data.
    file_stats = compute_file_stats(path, keydir)

    # Read merge config for fragmentation thresholds
    merge_config_overrides = Keyword.get(opts, :merge_config, %{})

    merge_config = %{
      fragmentation_threshold:
        Map.get(merge_config_overrides, :fragmentation_threshold, @default_fragmentation_threshold),
      dead_bytes_threshold:
        Map.get(merge_config_overrides, :dead_bytes_threshold, @default_dead_bytes_threshold)
    }

    schedule_flush(flush_ms)
    schedule_expiry_sweep()
    unless sandbox?, do: schedule_frag_check()
    max_file_size = :persistent_term.get(:ferricstore_max_active_file_size, @default_max_active_file_size)

    {:ok, %__MODULE__{ets: keydir, keydir: keydir,
                       prefix_keys: prefix_keys, index: index, data_dir: data_dir,
                       shard_data_path: path,
                       active_file_id: active_file_id,
                       active_file_path: active_file_path,
                       active_file_size: active_file_size,
                       pending: [], flush_in_flight: nil,
                       promoted_instances: promoted,
                       file_stats: file_stats,
                       merge_config: merge_config,
                       raft?: raft?,
                       sandbox?: sandbox?,
                       sandbox_ra_system: sandbox_ra_system,
                       sandbox_ra_server_id: sandbox_ra_server_id,
                       max_active_file_size: max_file_size},
     {:continue, {:flush_interval, flush_ms}}}
  end

  # Scans the shard data directory for .log files and returns
  # {highest_file_id, file_size_of_highest}. Starts at 0 if no files exist.
  # Uses a single Enum.reduce pass instead of filter + map + max to avoid
  # creating intermediate lists (perf audit L5).
  defp discover_active_file(shard_path) do
    case File.ls(shard_path) do
      {:ok, files} ->
        # Clean up leftover compaction temp files from a previous crash.
        # These are always incomplete — if compaction had finished, the
        # rename would have replaced the original and the temp is gone.
        Enum.each(files, fn name ->
          if String.starts_with?(name, "compact_") and String.ends_with?(name, ".log") do
            File.rm(Path.join(shard_path, name))
            Logger.warning("Shard: removed leftover compaction temp file #{name}")
          end
        end)

        max_id =
          Enum.reduce(files, -1, fn name, best ->
            if String.ends_with?(name, ".log") and not String.starts_with?(name, "compact_") do
              id = name |> String.trim_trailing(".log") |> String.to_integer()
              max(id, best)
            else
              best
            end
          end)

        if max_id < 0 do
          {0, 0}
        else
          size = File.stat!(file_path(shard_path, max_id)).size
          {max_id, size}
        end

      {:error, _} ->
        {0, 0}
    end
  end

  # Recovers the ETS keydir from hint files or by scanning log files.
  # Uses last-writer-wins semantics (higher file_id + higher offset wins).
  defp recover_keydir(shard_path, keydir, prefix_keys, shard_index) do
    case File.ls(shard_path) do
      {:ok, files} ->
        log_files =
          files
          |> Enum.filter(&String.ends_with?(&1, ".log"))
          |> Enum.sort()

        Logger.debug("Shard #{shard_index}: recover_keydir scanning #{length(log_files)} log file(s) at #{shard_path}")

        # Try hint files first for faster recovery
        hint_files =
          files
          |> Enum.filter(&String.ends_with?(&1, ".hint"))
          |> Enum.sort()

        if hint_files != [] do
          # Recover from hint files
          Enum.each(hint_files, fn hint_name ->
            hint_path = Path.join(shard_path, hint_name)
            fid = hint_name |> String.trim_trailing(".hint") |> String.to_integer()

            case NIF.v2_read_hint_file(hint_path) do
              {:ok, entries} ->
                # NIF returns: {key, file_id, offset, value_size, expire_at_ms}
                Enum.each(entries, fn {key, _file_id, offset, value_size, expire_at_ms} ->
                  :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), fid, offset, value_size})
                  PrefixIndex.track(prefix_keys, key, shard_index)
                end)
              _ -> :ok
            end
          end)

          # Scan any log files that don't have corresponding hints (e.g. the active file)
          hinted_ids = MapSet.new(hint_files, fn name ->
            name |> String.trim_trailing(".hint") |> String.to_integer()
          end)

          unhinted_logs = Enum.reject(log_files, fn name ->
            fid = name |> String.trim_trailing(".log") |> String.to_integer()
            MapSet.member?(hinted_ids, fid)
          end)

          Enum.each(unhinted_logs, fn log_name ->
            recover_from_log(shard_path, log_name, keydir, prefix_keys, shard_index)
          end)
        else
          # No hint files -- full scan of all log files
          Enum.each(log_files, fn log_name ->
            recover_from_log(shard_path, log_name, keydir, prefix_keys, shard_index)
          end)
        end

      {:error, reason} ->
        Logger.warning("Shard #{shard_index}: recover_keydir failed to list #{shard_path}: #{inspect(reason)}")
    end

    ets_size = :ets.info(keydir, :size)
    # Log recovered keys for debugging (only first 10 to avoid log spam)
    sample_keys = :ets.tab2list(keydir) |> Enum.take(10) |> Enum.map(fn {k, _v, _e, _l, fid, off, vs} -> "#{k}(fid=#{inspect(fid)},off=#{off},vs=#{vs})" end)
    Logger.debug("Shard #{shard_index}: recover_keydir done, ETS size: #{ets_size}, keys: #{inspect(sample_keys)}")
  end

  defp recover_from_log(shard_path, log_name, keydir, prefix_keys, shard_index) do
    log_path = Path.join(shard_path, log_name)
    fid = log_name |> String.trim_trailing(".log") |> String.to_integer()

    # v2_scan_file returns {:ok, [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]}
    case NIF.v2_scan_file(log_path) do
      {:ok, records} ->
        Enum.each(records, fn {key, offset, value_size, expire_at_ms, is_tombstone} ->
          if is_tombstone do
            :ets.delete(keydir, key)
            PrefixIndex.untrack(prefix_keys, key, shard_index)
          else
            :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), fid, offset, value_size})
            PrefixIndex.track(prefix_keys, key, shard_index)
          end
        end)

      _ ->
        :ok
    end
  end

  # Returns the file path for a given file_id within the shard data directory.
  defp file_path(shard_path, file_id) do
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  # Returns the shard data path for the current state.
  # Deprecated: prefer state.shard_data_path which is cached during init.
  defp shard_path(state) do
    state.shard_data_path || Ferricstore.DataDir.shard_data_path(state.data_dir, state.index)
  end

  @impl true
  def handle_continue({:flush_interval, ms}, state) do
    # Store flush interval in process dictionary so handle_info can reschedule.
    Process.put(:flush_interval_ms, ms)
    {:noreply, state}
  end

  @impl true
  def handle_call({:get, key}, from, state) do
    # Fast path: ETS hit — no need to wait for in-flight writes.
    case ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, _vsize, exp} ->
        # Cold key — value evicted from ETS but disk location known.
        # Use synchronous pread (v2_pread_at_async NIF not yet available).
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            cold_read_warm_ets(state, key, value, exp, fid, off, byte_size(value))
            {:reply, value, state}

          _ ->
            {:reply, nil, state}
        end

      :miss ->
        # Key not in ETS at all — it doesn't exist.
        {:reply, nil, state}
    end
  end

  # Returns {file_path, value_offset, value_size} for sendfile optimization,
  # or nil if the key is not found / expired / only in ETS (hot cache).
  # The offset stored in ETS is the RECORD offset (start of header).
  # For sendfile, we need the VALUE offset = record_offset + 26 (header) + key_len.
  @bitcask_header_size 26
  def handle_call({:get_file_ref, key}, _from, state) do
    case ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} ->
        # Key is hot (in ETS). The value may not yet be flushed to disk,
        # so we cannot safely sendfile. Return nil to fall back to normal path.
        {:reply, nil, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, _exp} ->
        # Cold key — location known from ETS 7-tuple.
        # Adjust offset to skip header and key bytes to get to the value.
        p = file_path(state.shard_data_path, fid)
        value_offset = off + @bitcask_header_size + byte_size(key)
        {:reply, {p, value_offset, vsize}, state}

      :miss ->
        {:reply, nil, state}
    end
  end

  def handle_call({:get_meta, key}, from, state) do
    case ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, {value, expire_at_ms}, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, _vsize, exp} ->
        # Cold key — use synchronous pread (v2_pread_at_async NIF not yet available).
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            cold_read_warm_ets(state, key, value, exp, fid, off, byte_size(value))
            {:reply, {value, exp}, state}

          _ ->
            {:reply, nil, state}
        end

      :miss ->
        {:reply, nil, state}
    end
  end

  # Compound key scan: returns all live entries matching a prefix.
  # Used by HSCAN, SSCAN, ZSCAN via the compound_scan store callback.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:scan_prefix, prefix}, _from, state) do
    results = prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
    {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
  end

  # Count entries matching a compound key prefix.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:count_prefix, prefix}, _from, state) do
    {:reply, prefix_count_entries(state.keydir, prefix), state}
  end

  # Delete all entries matching a compound key prefix.
  # Uses :ets.select match spec instead of :ets.foldl full-table scan.
  def handle_call({:delete_prefix, prefix}, _from, state) do
    keys_to_delete = prefix_collect_keys(state.keydir, prefix)

    if state.raft? do
  
      Enum.each(keys_to_delete, fn key -> raft_write(state, {:delete, key}) end)
      new_version = state.write_version + 1
      {:reply, :ok, %{state | write_version: new_version}}
    else
      Enum.each(keys_to_delete, fn key -> ets_delete_key(state, key) end)
      {:reply, :ok, state}
    end
  end

  # -------------------------------------------------------------------
  # Promotion-aware compound operations
  #
  # These handlers route to either the shared Bitcask or a dedicated
  # promoted Bitcask based on whether the redis_key has been promoted.
  # They also trigger promotion checks after writes.
  # -------------------------------------------------------------------

  def handle_call({:compound_get, redis_key, compound_key}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- use ETS/shared Bitcask (same as {:get, compound_key})
        case ets_lookup_warm(state, compound_key) do
          {:hit, value, _exp} -> {:reply, value, state}
          :expired -> {:reply, nil, state}
          :miss ->
            state = await_in_flight(state)
            state = flush_pending_sync(state)
            {:reply, warm_from_store(state, compound_key), state}
        end

      dedicated_path ->
        # Promoted -- read from ETS first, then dedicated Bitcask via v2
        case ets_lookup_warm(state, compound_key) do
          {:hit, value, _exp} -> {:reply, value, state}
          :expired -> {:reply, nil, state}
          :miss ->
            case promoted_read(dedicated_path, compound_key, state.keydir) do
              {:ok, nil} -> {:reply, nil, state}
              {:ok, value, exp} ->
                ets_insert(state, compound_key, value, exp)
                {:reply, value, state}
              {:ok, value} ->
                ets_insert(state, compound_key, value, 0)
                {:reply, value, state}
              _error -> {:reply, nil, state}
            end
        end
    end
  end

  def handle_call({:compound_get_meta, redis_key, compound_key}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        case ets_lookup_warm(state, compound_key) do
          {:hit, value, expire_at_ms} -> {:reply, {value, expire_at_ms}, state}
          :expired -> {:reply, nil, state}
          :miss ->
            state = await_in_flight(state)
            state = flush_pending_sync(state)
            {:reply, warm_meta_from_store(state, compound_key), state}
        end

      dedicated_path ->
        case ets_lookup_warm(state, compound_key) do
          {:hit, value, expire_at_ms} -> {:reply, {value, expire_at_ms}, state}
          :expired -> {:reply, nil, state}
          :miss ->
            case promoted_read(dedicated_path, compound_key, state.keydir) do
              {:ok, nil} -> {:reply, nil, state}
              {:ok, value, exp} ->
                ets_insert(state, compound_key, value, exp)
                {:reply, {value, exp}, state}
              {:ok, value} ->
                ets_insert(state, compound_key, value, 0)
                {:reply, {value, 0}, state}
              _error -> {:reply, nil, state}
            end
        end
    end
  end

  def handle_call({:compound_put, redis_key, compound_key, value, expire_at_ms}, _from, state) do
    if state.raft? do
      handle_compound_put_raft(redis_key, compound_key, value, expire_at_ms, state)
    else
      handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state)
    end
  end

  # Raft path for compound_put: routes put through Raft for non-promoted,
  # or directly to dedicated Bitcask for promoted keys.
  defp handle_compound_put_raft(redis_key, compound_key, value, expire_at_ms, state) do


    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- route through Raft
        result = raft_write(state, {:put, compound_key, value, expire_at_ms})
        new_version = state.write_version + 1

        case result do
          :ok ->
            new_state = %{state | write_version: new_version}
            # Check if this key should be promoted (local optimization, not replicated)
            new_state = maybe_promote(new_state, redis_key, compound_key)
            {:reply, :ok, new_state}

          {:error, _} = err ->
            {:reply, err, state}
        end

      dedicated_path ->
        # Promoted -- write to dedicated Bitcask first (get fid+offset), then ETS with location
        case promoted_write(dedicated_path, compound_key, value, expire_at_ms) do
          {:ok, {fid, offset, record_size}} ->
            state = track_promoted_dead_bytes(state, redis_key, compound_key, record_size)
            ets_insert_with_location(state, compound_key, value, expire_at_ms, fid, offset, record_size)
            {:reply, :ok, bump_promoted_writes(state, redis_key)}
          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  # Direct path for compound_put (no Raft).
  defp handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- write to ETS + shared pending batch
        ets_insert(state, compound_key, value, expire_at_ms)
        new_pending = [{compound_key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        # Check if this key should be promoted
        new_state = maybe_promote(new_state, redis_key, compound_key)

        {:reply, :ok, new_state}

      dedicated_path ->
        # Promoted -- write to dedicated Bitcask first (get fid+offset), then ETS with location
        case promoted_write(dedicated_path, compound_key, value, expire_at_ms) do
          {:ok, {fid, offset, record_size}} ->
            state = track_promoted_dead_bytes(state, redis_key, compound_key, record_size)
            ets_insert_with_location(state, compound_key, value, expire_at_ms, fid, offset, record_size)
            {:reply, :ok, bump_promoted_writes(state, redis_key)}
          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:compound_delete, redis_key, compound_key}, _from, state) do
    if state.raft? do
      handle_compound_delete_raft(redis_key, compound_key, state)
    else
      handle_compound_delete_direct(redis_key, compound_key, state)
    end
  end

  # Raft path for compound_delete: routes delete through Raft for non-promoted,
  # or directly to dedicated Bitcask for promoted keys.
  defp handle_compound_delete_raft(redis_key, compound_key, state) do


    case promoted_store(state, redis_key) do
      nil ->
        result = raft_write(state, {:delete, compound_key})
        new_version = state.write_version + 1

        case result do
          :ok -> {:reply, :ok, %{state | write_version: new_version}}
          {:error, _} = err -> {:reply, err, state}
        end

      dedicated_path ->
        # Promoted -- delete from dedicated Bitcask via v2
        state = track_promoted_delete_bytes(state, redis_key, compound_key)

        case promoted_tombstone(dedicated_path, compound_key) do
          {:ok, _} ->
            ets_delete_key(state, compound_key)
            {:reply, :ok, bump_promoted_writes(state, redis_key)}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted tombstone failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  # Direct path for compound_delete (no Raft).
  defp handle_compound_delete_direct(redis_key, compound_key, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- synchronous delete from shared Bitcask
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        state = track_delete_dead_bytes(state, compound_key)

        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            ets_delete_key(state, compound_key)
            # flush_pending_sync already sets pending to []. Only scan if non-empty.
            new_pending =
              case state.pending do
                [] -> []
                pending -> Enum.reject(pending, fn {k, _, _} -> k == compound_key end)
              end
            new_version = state.write_version + 1
            {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: tombstone write failed for compound_delete: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end

      dedicated_path ->
        # Promoted -- delete from dedicated Bitcask via v2
        state = track_promoted_delete_bytes(state, redis_key, compound_key)

        case promoted_tombstone(dedicated_path, compound_key) do
          {:ok, _} ->
            ets_delete_key(state, compound_key)
            {:reply, :ok, bump_promoted_writes(state, redis_key)}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted tombstone failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:compound_scan, redis_key, prefix}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- use :ets.select match spec instead of foldl
        # Pass shard_data_path to enable cold-read for recovered keys
        results = prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}

      dedicated_path ->
        # Promoted -- ETS entries point to the dedicated file (fid/offset
        # updated during promote_collection!). Use dedicated_path for cold reads.
        results = prefix_scan_entries(state.keydir, prefix, dedicated_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
    end
  end

  def handle_call({:compound_count, redis_key, prefix}, _from, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- use :ets.select match spec instead of foldl
        {:reply, prefix_count_entries(state.keydir, prefix), state}

      _dedicated_path ->
        # Promoted -- all entries are in ETS (recovered via v2_scan_file on init).
        {:reply, prefix_count_entries(state.keydir, prefix), state}
    end
  end

  def handle_call({:compound_delete_prefix, redis_key, prefix}, _from, state) do
    if state.raft? do
      handle_compound_delete_prefix_raft(redis_key, prefix, state)
    else
      handle_compound_delete_prefix_direct(redis_key, prefix, state)
    end
  end

  # Raft path for compound_delete_prefix: routes deletes through Raft for non-promoted,
  # or directly cleans up dedicated Bitcask for promoted keys.
  defp handle_compound_delete_prefix_raft(redis_key, prefix, state) do


    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- route deletes through Raft
        keys_to_delete = prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key ->
          raft_write(state, {:delete, key})
        end)

        new_version = state.write_version + 1
        {:reply, :ok, %{state | write_version: new_version}}

      _dedicated ->
        # Promoted -- clean up the dedicated Bitcask entirely
        alias Ferricstore.Store.Promotion

        # Delete compound keys from ETS
        keys_to_delete = prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key -> ets_delete_key(state, key) end)

        # Clean up the dedicated instance and remove from state
        Promotion.cleanup_promoted!(
          redis_key,
          state.shard_data_path,
          state.keydir,
          state.data_dir,
          state.index
        )

        new_promoted = Map.delete(state.promoted_instances, redis_key)
        {:reply, :ok, %{state | promoted_instances: new_promoted}}
    end
  end

  # Direct path for compound_delete_prefix (no Raft).
  defp handle_compound_delete_prefix_direct(redis_key, prefix, state) do
    case promoted_store(state, redis_key) do
      nil ->
        # Not promoted -- delete from ETS (same as {:delete_prefix, prefix})
        keys_to_delete = prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key -> ets_delete_key(state, key) end)
        {:reply, :ok, state}

      _dedicated ->
        # Promoted -- clean up the dedicated Bitcask entirely
        alias Ferricstore.Store.Promotion

        # Delete compound keys from ETS
        keys_to_delete = prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key -> ets_delete_key(state, key) end)

        # Clean up the dedicated instance and remove from state
        Promotion.cleanup_promoted!(
          redis_key,
          state.shard_data_path,
          state.keydir,
          state.data_dir,
          state.index
        )

        new_promoted = Map.delete(state.promoted_instances, redis_key)
        {:reply, :ok, %{state | promoted_instances: new_promoted}}
    end
  end

  # Check if a redis_key is promoted.
  def handle_call({:promoted?, redis_key}, _from, state) do
    {:reply, Map.has_key?(state.promoted_instances, redis_key), state}
  end

  def handle_call({:put, key, value, expire_at_ms}, from, state) do
    # Reject new-key writes when the keydir is at capacity (spec 2.4).
    # Updates to existing keys are always allowed regardless of memory pressure.
    is_new = case :ets.lookup(state.keydir, key) do
      [] -> true
      _ -> false
    end

    if is_new and Ferricstore.MemoryGuard.reject_writes?() do
      Ferricstore.MemoryGuard.nudge()
      {:reply, {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}, state}
    else
      if state.raft? do
        # Raft path: forward to Batcher via non-blocking cast. The Batcher
        # will reply directly to the original caller (from) when the ra
        # command commits. This frees the Shard GenServer to process the
        # next request immediately without waiting for Raft consensus.
    
        raft_write_async(state, {:put, key, value, expire_at_ms}, from)
        new_version = state.write_version + 1
        {:noreply, %{state | write_version: new_version}}
      else
        # Direct path (no Raft): write to ETS immediately so reads see it
        # right away, then queue for async Bitcask flush.
        ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_count = state.pending_count + 1
        new_version = state.write_version + 1

        # Cap pending list to @max_pending_size to bound heap memory growth
        # during write bursts. Force a synchronous flush when exceeded.
        state =
          if new_count > @max_pending_size do
            s = %{state | pending: new_pending, pending_count: new_count}
            s = await_in_flight(s)
            flush_pending_sync(s)
          else
            %{state | pending: new_pending, pending_count: new_count}
          end

        new_state = %{state | write_version: new_version}

        # Flush immediately when no async flush is in-flight. This ensures every
        # put is submitted to io_uring (and thus kernel-managed) before the call
        # returns, providing crash durability even if the process is killed before
        # the timer fires. Multiple puts arriving while a flush is in-flight are
        # batched together and flushed on the next timer tick after the CQE.
        if state.flush_in_flight == nil do
          {:reply, :ok, flush_pending(new_state)}
        else
          {:reply, :ok, new_state}
        end
      end
    end
  end

  # Atomic increment: reads current value, parses as integer, adds delta, writes back.
  # Returns {:ok, new_integer} or {:error, reason}.
  def handle_call({:incr, key, delta}, _from, state) do
    if state.raft? do
      handle_incr_raft(key, delta, state)
    else
      handle_incr_direct(key, delta, state)
    end
  end

  # Raft path for INCR: reads the current value from ETS/Bitcask (local read),
  # computes the new value, then routes the resulting put through the Raft
  # Batcher so the write is replicated and committed before replying.
  defp handle_incr_raft(key, delta, state) do


    {current_value, expire_at_ms} =
      case ets_lookup_warm(state, key) do
        {:hit, value, exp} -> {value, exp}
        :expired -> {nil, 0}
        :miss -> {do_get(state, key), 0}
      end

    case current_value do
      nil ->
        result = raft_write(state, {:put, key, delta, 0})
        new_version = state.write_version + 1

        case result do
          :ok -> {:reply, {:ok, delta}, %{state | write_version: new_version}}
          {:error, _} = err -> {:reply, err, state}
        end

      value ->
        case coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            result = raft_write(state, {:put, key, new_val, expire_at_ms})
            new_version = state.write_version + 1

            case result do
              :ok -> {:reply, {:ok, new_val}, %{state | write_version: new_version}}
              {:error, _} = err -> {:reply, err, state}
            end

          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}
        end
    end
  end

  # Direct path for INCR (no Raft): reads current value, computes new value,
  # writes to ETS + pending batch for async Bitcask flush.
  defp handle_incr_direct(key, delta, state) do
    case ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        case coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            ets_insert(state, key, new_val, expire_at_ms)
            new_pending = [{key, new_val, expire_at_ms} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}
        end

      :expired ->
        # Treat as non-existent: set to delta
        ets_insert(state, key, delta, 0)
        new_pending = [{key, delta, 0} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, delta}, new_state}

      :miss ->
        # Check Bitcask
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        case do_get(state, key) do
          nil ->
            ets_insert(state, key, delta, 0)
            new_pending = [{key, delta, 0} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, delta}, new_state}

          value ->
            # get the metadata for the expire
            expire_at_ms =
              case do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case coerce_integer(value) do
              {:ok, int_val} ->
                new_val = int_val + delta
                ets_insert(state, key, new_val, expire_at_ms)
                new_pending = [{key, new_val, expire_at_ms} | state.pending]
                new_version = state.write_version + 1
                new_state = %{state | pending: new_pending, write_version: new_version}

                new_state =
                  if state.flush_in_flight == nil,
                    do: flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not an integer or out of range"}, state}
            end
        end
    end
  end

  # Atomic float increment: reads current value, parses as float, adds delta, writes back.
  # Returns {:ok, new_float_string} or {:error, reason}.
  def handle_call({:incr_float, key, delta}, _from, state) do
    if state.raft? do
      handle_incr_float_raft(key, delta, state)
    else
      handle_incr_float_direct(key, delta, state)
    end
  end

  # Raft path for INCRBYFLOAT: routes compound command through Raft.
  # The state machine performs the full read-modify-write atomically.
  defp handle_incr_float_raft(key, delta, state) do


    result = raft_write(state, {:incr_float, key, delta})
    new_version = state.write_version + 1

    case result do
      {:ok, _new_str} = ok -> {:reply, ok, %{state | write_version: new_version}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  # Direct path for INCRBYFLOAT (no Raft).
  defp handle_incr_float_direct(key, delta, state) do
    case ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        case coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            ets_insert(state, key, new_val, expire_at_ms)
            new_pending = [{key, new_val, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not a valid float"}, state}
        end

      :expired ->
        new_val = delta * 1.0
        ets_insert(state, key, new_val, 0)
        new_pending = [{key, new_val, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, new_val}, new_state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        case do_get(state, key) do
          nil ->
            new_val = delta * 1.0
            ets_insert(state, key, new_val, 0)
            new_pending = [{key, new_val, 0} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          value ->
            expire_at_ms =
              case do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case coerce_float(value) do
              {:ok, float_val} ->
                new_val = float_val + delta
                ets_insert(state, key, new_val, expire_at_ms)
                new_pending = [{key, new_val, expire_at_ms} | state.pending]
                new_state = %{state | pending: new_pending}

                new_state =
                  if state.flush_in_flight == nil,
                    do: flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not a valid float"}, state}
            end
        end
    end
  end

  # Atomic append: reads current value (or ""), appends suffix, writes back.
  # Returns {:ok, new_byte_length}.
  def handle_call({:append, key, suffix}, _from, state) do
    if state.raft? do
      handle_append_raft(key, suffix, state)
    else
      handle_append_direct(key, suffix, state)
    end
  end

  # Raft path for APPEND: routes compound command through Raft.
  # The state machine performs the full read-modify-write atomically.
  defp handle_append_raft(key, suffix, state) do


    result = raft_write(state, {:append, key, suffix})
    new_version = state.write_version + 1

    case result do
      {:ok, _len} = ok -> {:reply, ok, %{state | write_version: new_version}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  # Direct path for APPEND (no Raft).
  defp handle_append_direct(key, suffix, state) do
    case ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        new_val = to_disk_binary(value) <> suffix
        ets_insert(state, key, new_val, expire_at_ms)
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}

      :expired ->
        ets_insert(state, key, suffix, 0)
        new_pending = [{key, suffix, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(suffix)}, new_state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        {old_val, expire_at_ms} =
          case do_get_meta(state, key) do
            {v, exp} -> {to_disk_binary(v), exp}
            nil -> {"", 0}
          end

        new_val = old_val <> suffix
        ets_insert(state, key, new_val, expire_at_ms)
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}
    end
  end

  # Atomic get-and-set: returns old value (or nil), sets new value.
  def handle_call({:getset, key, new_value}, _from, state) do
    if state.raft? do
      handle_getset_raft(key, new_value, state)
    else
      handle_getset_direct(key, new_value, state)
    end
  end

  # Raft path for GETSET: routes compound command through Raft.
  # The state machine performs the atomic get-and-set.
  defp handle_getset_raft(key, new_value, state) do


    result = raft_write(state, {:getset, key, new_value})
    new_version = state.write_version + 1

    case result do
      {:error, _} = err -> {:reply, err, state}
      old -> {:reply, old, %{state | write_version: new_version}}
    end
  end

  # Direct path for GETSET (no Raft).
  defp handle_getset_direct(key, new_value, state) do
    {old, state} =
      case ets_lookup_warm(state, key) do
        {:hit, value, _expire_at_ms} -> {value, state}
        :expired -> {nil, state}
        :miss ->
          state = await_in_flight(state)
          state = flush_pending_sync(state)
          {do_get(state, key), state}
      end

    ets_insert(state, key, new_value, 0)
    new_pending = [{key, new_value, 0} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: flush_pending(new_state),
        else: new_state

    {:reply, old, new_state}
  end

  # Atomic get-and-delete: returns value (or nil), deletes key.
  def handle_call({:getdel, key}, _from, state) do
    if state.raft? do
      handle_getdel_raft(key, state)
    else
      handle_getdel_direct(key, state)
    end
  end

  # Raft path for GETDEL: routes compound command through Raft.
  # The state machine performs the atomic get-and-delete.
  defp handle_getdel_raft(key, state) do


    result = raft_write(state, {:getdel, key})
    new_version = state.write_version + 1

    case result do
      {:error, _} = err -> {:reply, err, state}
      old -> {:reply, old, %{state | write_version: new_version}}
    end
  end

  # Direct path for GETDEL (no Raft).
  defp handle_getdel_direct(key, state) do
    {old, state} =
      case ets_lookup_warm(state, key) do
        {:hit, value, _expire_at_ms} -> {value, state}
        :expired -> {nil, state}
        :miss ->
          state = await_in_flight(state)
          state = flush_pending_sync(state)
          {do_get(state, key), state}
      end

    if old != nil do
      # Synchronous delete for durability
      state = await_in_flight(state)
      state = flush_pending_sync(state)
      state = track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          ets_delete_key(state, key)
          # flush_pending_sync already sets pending to []. Only scan if non-empty.
          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end
          {:reply, old, %{state | pending: new_pending}}

        {:error, reason} ->
          Logger.error("Shard #{state.index}: tombstone write failed for GETDEL: #{inspect(reason)}")
          {:reply, {:error, reason}, state}
      end
    else
      {:reply, nil, state}
    end
  end

  # Atomic get-and-update-expiry: returns value, updates TTL.
  # expire_at_ms = 0 means PERSIST (remove expiry).
  def handle_call({:getex, key, expire_at_ms}, _from, state) do
    if state.raft? do
      handle_getex_raft(key, expire_at_ms, state)
    else
      handle_getex_direct(key, expire_at_ms, state)
    end
  end

  # Raft path for GETEX: routes compound command through Raft.
  # The state machine performs the atomic get-and-update-expiry.
  defp handle_getex_raft(key, expire_at_ms, state) do


    result = raft_write(state, {:getex, key, expire_at_ms})
    new_version = state.write_version + 1

    case result do
      {:error, _} = err -> {:reply, err, state}
      value -> {:reply, value, %{state | write_version: new_version}}
    end
  end

  # Direct path for GETEX (no Raft).
  defp handle_getex_direct(key, expire_at_ms, state) do
    case ets_lookup_warm(state, key) do
      {:hit, value, _old_exp} ->
        ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:reply, value, new_state}

      :expired ->
        {:reply, nil, state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)

        case do_get(state, key) do
          nil ->
            {:reply, nil, state}

          value ->
            ets_insert(state, key, value, expire_at_ms)
            new_pending = [{key, value, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: flush_pending(new_state),
                else: new_state

            {:reply, value, new_state}
        end
    end
  end

  # Atomic set-range: overwrites portion of string at offset with value.
  # Zero-pads if key doesn't exist or string is shorter than offset.
  # Returns {:ok, new_byte_length}.
  def handle_call({:setrange, key, offset, value}, _from, state) do
    if state.raft? do
      handle_setrange_raft(key, offset, value, state)
    else
      handle_setrange_direct(key, offset, value, state)
    end
  end

  # Raft path for SETRANGE: routes compound command through Raft.
  # The state machine performs the full read-modify-write atomically.
  defp handle_setrange_raft(key, offset, value, state) do


    result = raft_write(state, {:setrange, key, offset, value})
    new_version = state.write_version + 1

    case result do
      {:ok, _len} = ok -> {:reply, ok, %{state | write_version: new_version}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  # Direct path for SETRANGE (no Raft).
  defp handle_setrange_direct(key, offset, value, state) do
    {old_val, expire_at_ms} =
      case ets_lookup_warm(state, key) do
        {:hit, v, exp} -> {to_disk_binary(v), exp}
        :expired -> {"", 0}
        :miss ->
          state = await_in_flight(state)
          state = flush_pending_sync(state)

          case do_get_meta(state, key) do
            {v, exp} -> {to_disk_binary(v), exp}
            nil -> {"", 0}
          end
      end

    new_val = apply_setrange(old_val, offset, value)
    ets_insert(state, key, new_val, expire_at_ms)
    new_pending = [{key, new_val, expire_at_ms} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: flush_pending(new_state),
        else: new_state

    {:reply, {:ok, byte_size(new_val)}, new_state}
  end

  def handle_call({:delete, key}, from, state) do
    if state.raft? do
      # Raft path: forward to Batcher via non-blocking cast. The Batcher
      # will reply directly to the original caller when ra commits.
  
      raft_write_async(state, {:delete, key}, from)
      new_version = state.write_version + 1
      {:noreply, %{state | write_version: new_version}}
    else
      # Direct path: delete is always synchronous — tombstones must be durable
      # immediately so a crash doesn't resurrect the key.
      #
      # 1. Wait for any in-flight async flush to complete.
      # 2. Flush remaining pending writes synchronously.
      # 3. Write the tombstone.
      # 4. Remove the deleted key from pending to prevent resurrection.
      state = await_in_flight(state)
      state = flush_pending_sync(state)
      state = track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          ets_delete_key(state, key)
          # flush_pending_sync already sets pending to []. Only scan if non-empty
          # (belt-and-suspenders guard; avoids O(n) Enum.reject on every DELETE).
          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end
          new_version = state.write_version + 1
          {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}

        {:error, reason} ->
          Logger.error("Shard #{state.index}: tombstone write failed for DELETE: #{inspect(reason)}")
          # Still remove from ETS so the key is gone for the current session.
          # The data file may have been removed externally (e.g. test cleanup).
          ets_delete_key(state, key)
          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end
          new_version = state.write_version + 1
          {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}
      end
    end
  end

  # Returns the active file info for the AsyncApplyWorker.
  # Avoids :sys.get_state which copies the entire GenServer state.
  def handle_call(:get_active_file, _from, state) do
    {:reply, {state.active_file_id, state.active_file_path}, state}
  end

  # Returns the current write_version for WATCH support.
  # Combines the Shard's internal counter (incremented for async/non-raft writes)
  # with the shared atomic counter (incremented by Router for quorum bypass writes).
  # This ensures WATCH detects mutations regardless of which write path was used.
  def handle_call({:get_version, _key}, _from, state) do
    shared = Ferricstore.Store.WriteVersion.get(state.index)
    {:reply, state.write_version + shared, state}
  end

  # --- Transaction execution (single-shard atomic batch) ---
  #
  # Executes all commands within a single handle_call, ensuring no other
  # client can interleave on this shard. Used by the Coordinator for
  # single-shard MULTI/EXEC transactions.

  def handle_call({:tx_execute, queue, sandbox_namespace}, _from, state) do
    Process.put(:tx_deleted_keys, MapSet.new())
    store = build_local_store(state)

    results =
      try do
        Enum.map(queue, fn {cmd, args} ->
          # Prepend sandbox namespace to the key argument (first arg)
          # so compound keys, type registry, etc. all use the namespaced key.
          namespaced_args = namespace_args(args, sandbox_namespace)

          try do
            Ferricstore.Commands.Dispatcher.dispatch(cmd, namespaced_args, store)
          catch
            :exit, {:noproc, _} ->
              {:error, "ERR server not ready, shard process unavailable"}

            :exit, {reason, _} ->
              {:error, "ERR internal error: #{inspect(reason)}"}
          end
        end)
      after
        Process.delete(:tx_deleted_keys)
      end

    {:reply, results, state}
  end

  defp namespace_args(args, nil), do: args
  defp namespace_args([], _ns), do: []
  defp namespace_args([key | rest], ns) when is_binary(key), do: [ns <> key | rest]
  defp namespace_args(args, _ns), do: args

  # Builds a store map that uses direct ETS/Router access for reads and
  # Router GenServer.call for writes. Since this shard's commands all target
  # THIS shard, and we're inside the shard's handle_call, we need the store's
  # read callbacks to read from ETS directly (hot path) or fall through to
  # Router.get which reads from ETS first. For writes, Router.put/delete/incr
  # calls GenServer.call on the target shard -- which IS us, causing deadlock.
  #
  # Solution: route write operations through Router as normal. The Router
  # functions for writes call GenServer.call on the shard that owns the key.
  # Since the commands in this prepare batch all target THIS shard, those
  # GenServer.calls would deadlock.
  #
  # Instead, we build a store that detects when the target shard is us and
  # uses a direct write path, falling through to normal Router for other shards.
  defp build_local_store(state) do
    my_idx = state.index
    is_sandbox = state.sandbox?
    # Build a minimal context with only the fields closures need, to avoid
    # capturing the entire state struct (which includes pending list,
    # promoted_instances, etc. that hold stale references).
    ctx = %{
      keydir: state.keydir,
      prefix_keys: state.prefix_keys,
      index: state.index,
      shard_data_path: state.shard_data_path,
      data_dir: state.data_dir
    }

    # Direct put: write to ETS immediately, queue for async Bitcask flush.
    # This mirrors the non-raft {:put, ...} handler logic.
    # In sandbox mode, all keys are treated as local (the Coordinator already
    # routed them to this shard). Router.shard_for inside the shard GenServer
    # cannot resolve sandbox shards because Process.get(:ferricstore_sandbox)
    # is nil in the GenServer's process.
    local_put = fn key, value, expire_at_ms ->
      is_local = is_sandbox or Router.shard_for(key) == my_idx

      if is_local do
        ets_insert(ctx, key, value, expire_at_ms)
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
        end

        send(self(), {:tx_pending_write, key, value, expire_at_ms})
        :ok
      else
        Router.put(key, value, expire_at_ms)
      end
    end

    local_delete = fn key ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        ets_delete_key(state, key)
        # Track deletion so subsequent reads within this tx see the key as gone
        deleted = Process.get(:tx_deleted_keys, MapSet.new())
        Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
        send(self(), {:tx_pending_delete, key})
        :ok
      else
        Router.delete(key)
      end
    end

    local_get = fn key ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        # Check if key was deleted within this transaction
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          nil
        else
          case ets_lookup_warm(state, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              # Read directly from Bitcask to avoid GenServer.call deadlock
              case v2_local_read(state, key) do
                {:ok, nil} -> nil
                {:ok, value} ->
                  ets_insert(state, key, value, 0)
                  value
                _error -> nil
              end
          end
        end
      else
        Router.get(key)
      end
    end

    local_get_meta = fn key ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          nil
        else
          case ets_lookup_warm(state, key) do
            {:hit, value, exp} -> {value, exp}
            :expired -> nil
            :miss ->
              case v2_local_read(state, key) do
                {:ok, nil} -> nil
                {:ok, value} ->
                  ets_insert(state, key, value, 0)
                  {value, 0}
                _error -> nil
              end
          end
        end
      else
        Router.get_meta(key)
      end
    end

    local_exists = fn key ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          false
        else
          case ets_lookup_warm(ctx, key) do
            {:hit, _, _} -> true
            :expired -> false
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> false
                {:ok, _value} -> true
                _error -> false
              end
          end
        end
      else
        Router.exists?(key)
      end
    end

    local_incr = fn key, delta ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        current =
          case ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        case current do
          nil ->
            ets_insert(ctx, key, delta, 0)
            send(self(), {:tx_pending_write, key, delta, 0})
            {:ok, delta}

          value ->
            case coerce_integer(value) do
              {:ok, int_val} ->
                new_val = int_val + delta
                ets_insert(ctx, key, new_val, 0)
                send(self(), {:tx_pending_write, key, new_val, 0})
                {:ok, new_val}

              :error ->
                {:error, "ERR value is not an integer or out of range"}
            end
        end
      else
        Router.incr(key, delta)
      end
    end

    local_incr_float = fn key, delta ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        current =
          case ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        case current do
          nil ->
            new_val = delta * 1.0
            ets_insert(ctx, key, new_val, 0)
            send(self(), {:tx_pending_write, key, new_val, 0})
            {:ok, new_val}

          value ->
            case coerce_float(value) do
              {:ok, float_val} ->
                new_val = float_val + delta
                ets_insert(ctx, key, new_val, 0)
                send(self(), {:tx_pending_write, key, new_val, 0})
                {:ok, new_val}

              :error ->
                {:error, "ERR value is not a valid float"}
            end
        end
      else
        Router.incr_float(key, delta)
      end
    end

    local_append = fn key, suffix ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        current =
          case ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> to_disk_binary(value)
            :expired -> ""
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> ""
                {:ok, v} -> v
                _ -> ""
              end
          end

        new_val = current <> suffix
        ets_insert(ctx, key, new_val, 0)
        send(self(), {:tx_pending_write, key, new_val, 0})
        {:ok, byte_size(new_val)}
      else
        Router.append(key, suffix)
      end
    end

    local_getset = fn key, new_value ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        old =
          case ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        ets_insert(ctx, key, new_value, 0)
        send(self(), {:tx_pending_write, key, new_value, 0})
        old
      else
        Router.getset(key, new_value)
      end
    end

    local_getdel = fn key ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        old =
          case ets_lookup_warm(ctx, key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        if old do
          ets_delete_key(ctx, key)
          send(self(), {:tx_pending_delete, key})
        end

        old
      else
        Router.getdel(key)
      end
    end

    local_getex = fn key, expire_at_ms ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        value =
          case ets_lookup_warm(ctx, key) do
            {:hit, v, _exp} -> v
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> nil
                {:ok, v} -> v
                _ -> nil
              end
          end

        if value do
          ets_insert(ctx, key, value, expire_at_ms)
          send(self(), {:tx_pending_write, key, value, expire_at_ms})
        end

        value
      else
        Router.getex(key, expire_at_ms)
      end
    end

    local_setrange = fn key, offset, value ->
      if is_sandbox or Router.shard_for(key) == my_idx do
        old =
          case ets_lookup_warm(ctx, key) do
            {:hit, v, _exp} -> to_disk_binary(v)
            :expired -> ""
            :miss ->
              case v2_local_read(ctx, key) do
                {:ok, nil} -> ""
                {:ok, v} -> v
                _ -> ""
              end
          end

        new_val = apply_setrange_for_tx(old, offset, value)
        ets_insert(ctx, key, new_val, 0)
        send(self(), {:tx_pending_write, key, new_val, 0})
        {:ok, byte_size(new_val)}
      else
        Router.setrange(key, offset, value)
      end
    end

    %{
      get: local_get,
      get_meta: local_get_meta,
      put: local_put,
      delete: local_delete,
      exists?: local_exists,
      keys: &Router.keys/0,
      flush: fn ->
        Enum.each(Router.keys(), &Router.delete/1)
        :ok
      end,
      dbsize: &Router.dbsize/0,
      incr: local_incr,
      incr_float: local_incr_float,
      append: local_append,
      getset: local_getset,
      getdel: local_getdel,
      getex: local_getex,
      setrange: local_setrange,
      cas: &Router.cas/4,
      lock: &Router.lock/3,
      unlock: &Router.unlock/2,
      extend: &Router.extend/3,
      ratelimit_add: &Router.ratelimit_add/4,
      list_op: &Router.list_op/2,
      compound_get: fn redis_key, compound_key ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          # Local: read compound key directly from ETS
          case ets_lookup_warm(ctx, compound_key) do
            {:hit, value, _exp} -> value
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, compound_key) do
                {:ok, nil} -> nil
                {:ok, v} ->
                  ets_insert(ctx, compound_key, v, 0)
                  v
                _ -> nil
              end
          end
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_get, redis_key, compound_key})
        end
      end,
      compound_get_meta: fn redis_key, compound_key ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          case ets_lookup_warm(ctx, compound_key) do
            {:hit, value, exp} -> {value, exp}
            :expired -> nil
            :miss ->
              case v2_local_read(ctx, compound_key) do
                {:ok, nil} -> nil
                {:ok, v} ->
                  ets_insert(ctx, compound_key, v, 0)
                  {v, 0}
                _ -> nil
              end
          end
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
        end
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          ets_insert(ctx, compound_key, value, expire_at_ms)
          send(self(), {:tx_pending_write, compound_key, value, expire_at_ms})
          :ok
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
        end
      end,
      compound_delete: fn redis_key, compound_key ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          ets_delete_key(ctx, compound_key)
          send(self(), {:tx_pending_delete, compound_key})
          :ok
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_delete, redis_key, compound_key})
        end
      end,
      compound_scan: fn redis_key, prefix ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          # Pass shard_data_path to enable cold-read for recovered keys
          results = prefix_scan_entries(ctx.keydir, prefix, ctx.shard_data_path)
          Enum.sort_by(results, fn {field, _} -> field end)
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_scan, redis_key, prefix})
        end
      end,
      compound_count: fn redis_key, prefix ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          prefix_count_entries(ctx.keydir, prefix)
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_count, redis_key, prefix})
        end
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        if is_sandbox or Router.shard_for(redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          keys_to_delete = prefix_collect_keys(ctx.keydir, prefix)

          Enum.each(keys_to_delete, fn key ->
            ets_delete_key(ctx, key)
            send(self(), {:tx_pending_delete, key})
          end)

          :ok
        else
          shard = Router.resolve_shard(Router.shard_for(redis_key))
          GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
        end
      end,
      prob_dir: fn ->
        Path.join([ctx.data_dir, "prob", "shard_#{ctx.index}"])
      end,
      vectors_dir: fn ->
        Path.join([ctx.data_dir, "vectors", "shard_#{ctx.index}"])
      end
    }
  end

  # SETRANGE helper for transaction local store.
  # Reuses the efficient binary_part-based implementation from apply_setrange/3.
  # The old version converted to byte lists and back, which for a 10KB binary
  # would create ~160KB of transient list allocations (8 bytes/byte on 64-bit).
  defp apply_setrange_for_tx(old, offset, value) do
    apply_setrange(old, offset, value)
  end

  # --- Native commands: CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD ---

  def handle_call({:cas, key, expected, new_value, ttl_ms}, _from, state) do
    if state.raft? do
      handle_cas_raft(key, expected, new_value, ttl_ms, state)
    else
      handle_cas_direct(key, expected, new_value, ttl_ms, state)
    end
  end

  # Raft path for CAS: sends compound command through Raft so the entire
  # read-compare-write is atomic within the replicated state machine.
  defp handle_cas_raft(key, expected, new_value, ttl_ms, state) do


    # Pre-compute absolute expiry before entering Raft so the state machine
    # apply/3 remains deterministic (no clock calls inside apply).
    # Use HLC for consistency across all code paths and multi-node.
    expire_at_ms = if ttl_ms, do: Ferricstore.HLC.now_ms() + ttl_ms, else: nil
    result = raft_write(state, {:cas, key, expected, new_value, expire_at_ms})

    case result do
      r when r in [1, 0, nil] ->
        new_version = if r == 1, do: state.write_version + 1, else: state.write_version
        {:reply, r, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Direct path for CAS (no Raft).
  defp handle_cas_direct(key, expected, new_value, ttl_ms, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^expected, old_exp}, state} ->
        expire = if ttl_ms, do: Ferricstore.HLC.now_ms() + ttl_ms, else: old_exp
        ets_insert(state, key, new_value, expire)
        new_pending = [{key, new_value, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} -> {:reply, 0, state}
      {:expired, state} -> {:reply, nil, state}
      {:missing, state} -> {:reply, nil, state}
    end
  end

  def handle_call({:lock, key, owner, ttl_ms}, _from, state) do
    if state.raft? do
      handle_lock_raft(key, owner, ttl_ms, state)
    else
      handle_lock_direct(key, owner, ttl_ms, state)
    end
  end

  # Raft path for LOCK: sends compound command through Raft so the entire
  # check-and-acquire is atomic within the replicated state machine.
  defp handle_lock_raft(key, owner, ttl_ms, state) do


    # Pre-compute absolute expiry before entering Raft so the state machine
    # apply/3 remains deterministic. Use HLC for multi-node consistency.
    expire_at_ms = Ferricstore.HLC.now_ms() + ttl_ms
    result = raft_write(state, {:lock, key, owner, expire_at_ms})

    case result do
      :ok ->
        {:reply, :ok, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Direct path for LOCK (no Raft).
  defp handle_lock_direct(key, owner, ttl_ms, state) do
    expire = Ferricstore.HLC.now_ms() + ttl_ms

    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        ets_insert(state, key, owner, expire)
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, :ok, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK lock is held by another owner"}, state}

      {_, state} ->
        ets_insert(state, key, owner, expire)
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:unlock, key, owner}, _from, state) do
    if state.raft? do
      handle_unlock_raft(key, owner, state)
    else
      handle_unlock_direct(key, owner, state)
    end
  end

  # Raft path for UNLOCK: sends compound command through Raft so the entire
  # check-and-delete is atomic within the replicated state machine.
  defp handle_unlock_raft(key, owner, state) do


    result = raft_write(state, {:unlock, key, owner})

    case result do
      1 ->
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Direct path for UNLOCK (no Raft).
  defp handle_unlock_direct(key, owner, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        state = track_delete_dead_bytes(state, key)

        case NIF.v2_append_tombstone(state.active_file_path, key) do
          {:ok, _} ->
            ets_delete_key(state, key)
            {:reply, 1, %{state | write_version: state.write_version + 1}}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: tombstone write failed for UNLOCK: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {_, state} -> {:reply, 1, state}
    end
  end

  def handle_call({:extend, key, owner, ttl_ms}, _from, state) do
    if state.raft? do
      handle_extend_raft(key, owner, ttl_ms, state)
    else
      handle_extend_direct(key, owner, ttl_ms, state)
    end
  end

  # Raft path for EXTEND: sends compound command through Raft so the entire
  # check-and-update is atomic within the replicated state machine.
  defp handle_extend_raft(key, owner, ttl_ms, state) do


    # Pre-compute absolute expiry before entering Raft so the state machine
    # apply/3 remains deterministic. Use HLC for multi-node consistency.
    expire_at_ms = Ferricstore.HLC.now_ms() + ttl_ms
    result = raft_write(state, {:extend, key, owner, expire_at_ms})

    case result do
      1 ->
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Direct path for EXTEND (no Raft).
  defp handle_extend_direct(key, owner, ttl_ms, state) do
    new_expire = Ferricstore.HLC.now_ms() + ttl_ms

    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        ets_insert(state, key, owner, new_expire)
        new_pending = [{key, owner, new_expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {_, state} ->
        {:reply, {:error, "DISTLOCK lock does not exist or has expired"}, state}
    end
  end

  def handle_call({:ratelimit_add, key, window_ms, max, count}, _from, state) do
    if state.raft? do
      handle_ratelimit_add_raft(key, window_ms, max, count, state)
    else
      handle_ratelimit_add_direct(key, window_ms, max, count, state)
    end
  end

  # 6-tuple variant: includes pre-computed now_ms from Router.raft_write.
  # Used when sandbox mode sends the raft command directly to the shard.
  def handle_call({:ratelimit_add, key, window_ms, max, count, _now_ms}, _from, state) do
    handle_ratelimit_add_direct(key, window_ms, max, count, state)
  end

  # Raft path for RATELIMIT.ADD: sends compound command through Raft so the
  # entire read-compute-write is atomic within the replicated state machine.
  defp handle_ratelimit_add_raft(key, window_ms, max, count, state) do


    # Pre-compute `now` before entering Raft so the state machine
    # apply/3 remains deterministic.
    now_ms = System.os_time(:millisecond)
    result = raft_write(state, {:ratelimit_add, key, window_ms, max, count, now_ms})

    case result do
      [_status, _count, _remaining, _ttl] = reply ->
        new_version = state.write_version + 1
        {:reply, reply, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Direct path for RATELIMIT.ADD (no Raft).
  defp handle_ratelimit_add_direct(key, window_ms, max, count, state) do
    now = Ferricstore.HLC.now_ms()

    {cur_count, cur_start, prv_count} =
      case ets_lookup_warm(state, key) do
        {:hit, value, _exp} -> decode_ratelimit(value)
        _ -> {0, now, 0}
      end

    # Rotate windows
    {cur_count, cur_start, prv_count} =
      cond do
        now - cur_start >= window_ms * 2 -> {0, now, 0}
        now - cur_start >= window_ms -> {0, now, cur_count}
        true -> {cur_count, cur_start, prv_count}
      end

    # Compute effective count with sliding window approximation
    elapsed = now - cur_start
    weight = max(0.0, 1.0 - elapsed / window_ms)
    effective = cur_count + trunc(Float.round(prv_count * weight))
    expire_at_ms = cur_start + window_ms * 2

    {status, final_count, remaining, state} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {"denied", effective, max(0, max - effective), new_state}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}
        new_state = if state.flush_in_flight == nil, do: flush_pending(new_state), else: new_state
        {"allowed", new_eff, max(0, max - new_eff), new_state}
      end

    ms_until_reset = max(0, cur_start + window_ms - now)
    {:reply, [status, final_count, remaining, ms_until_reset], state}
  end

  # ---------------------------------------------------------------------------
  # List operations
  # ---------------------------------------------------------------------------

  def handle_call({:list_op, key, operation}, _from, state) do
    if state.raft? do
      handle_list_op_raft(key, operation, state)
    else
      handle_list_op_direct(key, operation, state)
    end
  end

  # Compound-key path for list operations (both Raft and direct).
  # Each list element is a separate Bitcask entry: L:key\0<position> -> value
  # Metadata: LM:key -> term_to_binary({length, next_left_pos, next_right_pos})
  # This replaces the legacy blob path where the entire list was one serialized value.
  defp handle_list_op_raft(key, operation, state) do
    store = build_list_compound_store_raft(key, state)
    result = Ferricstore.Store.ListOps.execute(key, store, operation)
    new_version = state.write_version + 1
    {:reply, result, %{state | write_version: new_version}}
  end

  defp handle_list_op_direct(key, operation, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    store = build_list_compound_store_direct(key, state)
    result = Ferricstore.Store.ListOps.execute(key, store, operation)
    {:reply, result, state}
  end

  defp build_list_compound_store_raft(_key, state) do


    %{
      compound_get: fn _redis_key, compound_key ->
        do_compound_get(state, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        raft_write(state, {:put, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn _redis_key, compound_key ->
        raft_write(state, {:delete, compound_key})
      end,
      compound_scan: fn _redis_key, prefix ->
        results = prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
        Enum.sort_by(results, fn {field, _} -> field end)
      end
    }
  end

  defp build_list_compound_store_direct(_key, state) do
    %{
      compound_get: fn _redis_key, compound_key ->
        do_compound_get(state, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        case NIF.v2_append_batch(state.active_file_path, [{compound_key, value, expire_at_ms}]) do
          {:ok, [{offset, _value_size}]} ->
            ets_insert_with_location(state, compound_key, value, expire_at_ms, state.active_file_id, offset, byte_size(value))
          _ ->
            ets_insert(state, compound_key, value, expire_at_ms)
        end
        :ok
      end,
      compound_delete: fn _redis_key, compound_key ->
        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            ets_delete_key(state, compound_key)
            :ok
          {:error, reason} ->
            Logger.error("Shard #{state.index}: tombstone write failed for list compound_delete: #{inspect(reason)}")
            {:error, reason}
        end
      end,
      compound_scan: fn _redis_key, prefix ->
        results = prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
        Enum.sort_by(results, fn {field, _} -> field end)
      end
    }
  end

  def handle_call({:list_op_lmove, src_key, dst_key, from_dir, to_dir}, _from, state) do
    if state.raft? do
      handle_list_op_lmove_raft(src_key, dst_key, from_dir, to_dir, state)
    else
      handle_list_op_lmove_direct(src_key, dst_key, from_dir, to_dir, state)
    end
  end

  # Raft path for LMOVE using compound keys.
  defp handle_list_op_lmove_raft(src_key, dst_key, from_dir, to_dir, state) do
    store = build_list_compound_store_raft(src_key, state)
    result = Ferricstore.Store.ListOps.execute_lmove(src_key, dst_key, store, from_dir, to_dir)
    new_version = state.write_version + 1
    {:reply, result, %{state | write_version: new_version}}
  end

  # Direct path for LMOVE using compound keys.
  defp handle_list_op_lmove_direct(src_key, dst_key, from_dir, to_dir, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    store = build_list_compound_store_direct(src_key, state)
    result = Ferricstore.Store.ListOps.execute_lmove(src_key, dst_key, store, from_dir, to_dir)
    {:reply, result, state}
  end

  def handle_call({:exists, key}, _from, state) do
    # For ETS misses we need Bitcask to be up to date — flush first.
    case ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} ->
        {:reply, true, state}

      {:cold, _fid, _off, _vsize, _exp} ->
        # Cold key — value evicted from RAM but key exists on disk.
        {:reply, true, state}

      :expired ->
        {:reply, false, state}

      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        {:reply, do_get(state, key) != nil, state}
    end
  end

  def handle_call(:keys, _from, state) do
    # Flush first so NIF.keys() sees all pending writes.
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, live_keys(state), state}
  end

  # Returns all live keys matching a given prefix (text before ':'). Uses the
  # prefix_keys bag table for O(matching) lookup instead of scanning all keys.
  def handle_call({:keys_with_prefix, prefix}, _from, state) do
    keys = PrefixIndex.keys_for_prefix(state.prefix_keys, state.keydir, prefix)
    {:reply, keys, state}
  end

  # Merge-related calls: delegate to NIF and return results directly.

  def handle_call(:shard_stats, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    sp = state.shard_data_path
    key_count = :ets.info(state.keydir, :size)

    # Compute file-level stats for merge scheduler
    {total_bytes, live_bytes, dead_bytes, file_count} =
      case File.ls(sp) do
        {:ok, files} ->
          log_files = Enum.filter(files, &String.ends_with?(&1, ".log"))
          fc = length(log_files)
          total = Enum.reduce(log_files, 0, fn name, acc ->
            case File.stat(Path.join(sp, name)) do
              {:ok, %{size: s}} -> acc + s
              _ -> acc
            end
          end)
          # Estimate: live = total / file_count (single active), dead = total - live
          live = if fc > 0, do: div(total, fc), else: 0
          dead = total - live
          {total, live, dead, fc}
        _ ->
          {0, 0, 0, 0}
      end

    frag = if total_bytes > 0, do: dead_bytes / total_bytes, else: 0.0

    {:reply, {:ok, {total_bytes, live_bytes, dead_bytes, file_count, key_count, frag}}, state}
  end

  def handle_call(:file_sizes, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    sp = state.shard_data_path

    sizes =
      case File.ls(sp) do
        {:ok, files} ->
          files
          |> Enum.filter(&String.ends_with?(&1, ".log"))
          |> Enum.map(fn name ->
            fid = name |> String.trim_trailing(".log") |> String.to_integer()
            size = File.stat!(Path.join(sp, name)).size
            {fid, size}
          end)
        _ -> []
      end

    {:reply, {:ok, sizes}, state}
  end

  def handle_call({:run_compaction, file_ids}, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    sp = state.shard_data_path

    # v2 compaction: for each file_id, collect live key offsets from ETS,
    # copy them to a new file, then replace the old file.
    # Track statistics for the merge scheduler.
    {total_written, total_dropped, total_reclaimed} =
      Enum.reduce(file_ids, {0, 0, 0}, fn fid, {written, dropped, reclaimed} ->
        source = file_path(sp, fid)

        offsets =
          :ets.foldl(
            fn {_key, _value, _exp, _lfu, f, off, _vsize}, acc ->
              if f == fid, do: [off | acc], else: acc
            end,
            [],
            state.keydir
          )

        if offsets != [] do
          old_size =
            case File.stat(source) do
              {:ok, %{size: s}} -> s
              _ -> 0
            end

          dest = Path.join(sp, "compact_#{fid}.log")

          case NIF.v2_copy_records(source, dest, offsets) do
            {:ok, _results} ->
              File.rename!(dest, source)

              new_size =
                case File.stat(source) do
                  {:ok, %{size: s}} -> s
                  _ -> 0
                end

              {written + length(offsets), dropped, reclaimed + max(old_size - new_size, 0)}

            {:error, reason} ->
              Logger.error("Shard #{state.index}: compaction copy_records failed for #{source}: #{inspect(reason)}")
              File.rm(dest)
              {written, dropped, reclaimed}
          end
        else
          # All entries in this file are dead — delete the file entirely
          old_size =
            case File.stat(source) do
              {:ok, %{size: s}} -> s
              _ -> 0
            end

          File.rm(source)
          {written, dropped, reclaimed + old_size}
        end
      end)

    # Reset file_stats for compacted files: dead bytes are now gone,
    # total bytes reflect the new compacted file size.
    new_file_stats =
      Enum.reduce(file_ids, state.file_stats, fn fid, fs ->
        case File.stat(file_path(sp, fid)) do
          {:ok, %{size: new_size}} ->
            Map.put(fs, fid, {new_size, 0})

          _ ->
            # File was deleted entirely (all dead)
            Map.delete(fs, fid)
        end
      end)

    {:reply, {:ok, {total_written, total_dropped, total_reclaimed}},
     %{state | file_stats: new_file_stats}}
  end

  def handle_call(:available_disk_space, _from, state) do
    sp = state.shard_data_path
    # Use df to get available disk space for the shard data directory
    case System.cmd("df", ["-k", sp], stderr_to_stdout: true) do
      {output, 0} ->
        lines = String.split(output, "\n", trim: true)
        case lines do
          [_header, data_line | _] ->
            parts = String.split(data_line, ~r/\s+/)
            available_kb = parts |> Enum.at(3, "0") |> String.to_integer()
            {:reply, {:ok, available_kb * 1024}, state}
          _ ->
            {:reply, {:error, "unable to parse df output"}, state}
        end
      _ ->
        {:reply, {:error, "df command failed"}, state}
    end
  end

  # Synchronous flush — used by tests and by delete to ensure durability.
  def handle_call(:flush, _from, state) do
    state = await_in_flight(state)
    state = flush_pending_sync(state)
    {:reply, :ok, state}
  end

  @impl true
  # Handle pending writes from tx_execute. These are queued via send/2
  # during transaction execution to persist ETS-only writes to Bitcask.
  def handle_info({:tx_pending_write, key, value, expire_at_ms}, state) do
    new_pending = [{key, value, expire_at_ms} | state.pending]
    new_version = state.write_version + 1
    new_state = %{state | pending: new_pending, write_version: new_version}

    new_state =
      if state.flush_in_flight == nil,
        do: flush_pending(new_state),
        else: new_state

    {:noreply, new_state}
  end

  def handle_info({:tx_pending_delete, key}, state) do
    if state.raft? do
  
      raft_write(state, {:delete, key})
      new_version = state.write_version + 1
      {:noreply, %{state | write_version: new_version}}
    else
      state = await_in_flight(state)
      state = flush_pending_sync(state)
      state = track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
          new_version = state.write_version + 1
          {:noreply, %{state | pending: new_pending, write_version: new_version}}

        {:error, reason} ->
          Logger.error("Shard #{state.index}: tombstone write failed for tx_pending_delete: #{inspect(reason)}")
          {:noreply, state}
      end
    end
  end

  def handle_info(:flush, state) do
    state = flush_pending(state)

    # Deferred fsync: if any nosync writes happened since the last fsync,
    # submit an async fsync to Tokio. This amortises fsync cost across all
    # writes in the batch window (typically 1ms). Durability window = 1 batch
    # interval — similar to Redis AOF `appendfsync everysec` but much smaller.
    state =
      if state.fsync_needed do
        corr_id = state.next_correlation_id
        NIF.v2_fsync_async(self(), corr_id, state.active_file_path)
        %{state | fsync_needed: false, flush_in_flight: corr_id,
          next_correlation_id: corr_id + 1}
      else
        state
      end

    schedule_flush(Process.get(:flush_interval_ms, @flush_interval_ms))
    {:noreply, state}
  end

  # Synchronous expiry sweep — used by tests to trigger a sweep and wait for
  # completion before making assertions.
  def handle_call(:expiry_sweep, _from, state) do
    state = do_expiry_sweep(state)
    {:reply, :ok, state}
  end

  # Periodic fragmentation re-evaluation for idle shards.
  # Catches shards that accumulated dead data then stopped receiving writes.
  # Also clears disk pressure flag periodically so writes can probe recovery.
  def handle_info(:frag_check, state) do
    if Ferricstore.Store.DiskPressure.under_pressure?(state.index) do
      Ferricstore.Store.DiskPressure.clear(state.index)
    end

    state = maybe_notify_fragmentation(state)
    schedule_frag_check()
    {:noreply, state}
  end

  # Active expiry sweep: scan ETS for expired keys and delete them.
  # When the sweep finds nothing to expire and there are no pending writes
  # or in-flight flushes, hibernate the GenServer to trigger a full GC
  # and shrink the heap. This reclaims memory accumulated during busy periods
  # on idle shards (memory audit L1).
  def handle_info(:expiry_sweep, state) do
    state = do_expiry_sweep(state)
    schedule_expiry_sweep()

    if state.sweep_at_ceiling_count == 0 and
         state.pending == [] and
         state.pending_count == 0 and
         state.flush_in_flight == nil do
      {:noreply, state, :hibernate}
    else
      {:noreply, state}
    end
  end

  # Handle async io_uring completion message from the NIF background thread.
  def handle_info({:io_complete, op_id, result}, state) do
    if state.flush_in_flight == op_id do
      case result do
        :ok ->
          {:noreply, %{state | flush_in_flight: nil}}

        {:error, reason} ->
          # The async flush failed. Log the error but clear in-flight so
          # the next timer tick can attempt another flush. The keydir was
          # updated optimistically by prepare_batch_for_async — on the next
          # store open, log replay will reconcile.
          Logger.error(
            "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
          )

          {:noreply, %{state | flush_in_flight: nil}}
      end
    else
      # Stale or unknown op_id — ignore.
      {:noreply, state}
    end
  end

  # Handle Tokio async completion with correlation ID.
  # Dispatches to fsync completion (flush_in_flight match) or read completion
  # (pending_reads lookup).
  def handle_info({:tokio_complete, corr_id, :ok, value}, state) do
    cond do
      # Async fsync completion — value is :ok for fsync
      corr_id == state.flush_in_flight ->
        {:noreply, %{state | flush_in_flight: nil}}

      # Async read completion — look up in pending_reads
      true ->
        case Map.pop(state.pending_reads, corr_id) do
          {{from, key}, rest_pending} ->
            # Simple GET cold-read completion.
            if value != nil do
              cold_read_warm_ets(state, key, value)
            end

            GenServer.reply(from, value)
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, key, :meta, exp}, rest_pending} ->
            # GET_META cold-read completion — reply with {value, expire_at_ms}.
            if value != nil do
              cold_read_warm_ets(state, key, value)
            end

            GenServer.reply(from, if(value != nil, do: {value, exp}, else: nil))
            {:noreply, %{state | pending_reads: rest_pending}}

          {nil, _} ->
            # Unknown correlation_id — could be a stale fsync or read. Ignore.
            {:noreply, state}
        end
    end
  end

  def handle_info({:tokio_complete, corr_id, :error, reason}, state) do
    if corr_id == state.flush_in_flight do
      # Async fsync error completion.
      Logger.error(
        "Shard #{state.index}: async fsync failed for corr_id #{corr_id}: #{inspect(reason)}"
      )
      {:noreply, %{state | flush_in_flight: nil}}
    else
      case Map.pop(state.pending_reads, corr_id) do
        {{from, _key}, rest_pending} ->
          GenServer.reply(from, nil)
          {:noreply, %{state | pending_reads: rest_pending}}

        {{from, _key, :meta, _exp}, rest_pending} ->
          GenServer.reply(from, nil)
          {:noreply, %{state | pending_reads: rest_pending}}

        {nil, _} ->
          {:noreply, state}
      end
    end
  end

  # Legacy v1 3-tuple format (no correlation ID) — keep for backward compat
  # during rolling upgrades. Once all async NIFs use correlation IDs, remove.
  def handle_info({:tokio_complete, :ok, _value}, state) do
    {:noreply, state}
  end

  def handle_info({:tokio_complete, :error, _reason}, state) do
    {:noreply, state}
  end

  # Catch-all for unexpected messages. Without this, any unmatched message
  # (stale timer, DOWN from a linked process, etc.) would crash the shard
  # GenServer, causing a restart and temporary unavailability.
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # Graceful shutdown (spec 2C.6, step 8)
  #
  # OTP calls terminate/2 when the supervisor stops this child during
  # application shutdown (children are stopped in reverse start order).
  # We flush pending writes, write the Bitcask hint file, and emit
  # telemetry so operators can observe shutdown timing.
  # -------------------------------------------------------------------

  @impl true
  def terminate(_reason, state) do
    t0 = System.monotonic_time(:microsecond)

    # Step 1: drain any in-flight async flush and flush remaining pending
    # writes synchronously to guarantee all data hits disk before exit.
    state = await_in_flight(state)
    state = flush_pending_sync(state)

    t_flush = System.monotonic_time(:microsecond)

    # Step 2: write v2 hint file for the active file so the next startup
    # can rebuild the keydir from hints instead of replaying the full log.
    write_hint_for_file(state, state.active_file_id)
    NIF.v2_fsync(state.active_file_path)

    t_hint = System.monotonic_time(:microsecond)

    # Step 3: emit shutdown telemetry for operator visibility.
    :telemetry.execute(
      [:ferricstore, :shard, :shutdown],
      %{
        flush_duration_us: t_flush - t0,
        hint_duration_us: t_hint - t_flush,
        total_duration_us: t_hint - t0
      },
      %{shard_index: state.index}
    )

    Logger.info(
      "Shard #{state.index}: shutdown complete " <>
        "(flush=#{t_flush - t0}us, hint=#{t_hint - t_flush}us)"
    )

    :ok
  end

  # -------------------------------------------------------------------
  # Private: flush
  # -------------------------------------------------------------------

  # Async flush — used by the timer and by put (first-write-in-window).
  # Writes to page cache only (no fsync) — durability comes from the
  # periodic fsync on the flush timer. This reduces per-write latency
  # from ~50-200us (NVMe fsync) to ~1-10us (memcpy to page cache).
  # If a flush is already in-flight or pending is empty, this is a no-op.
  defp flush_pending(%{pending: []} = state), do: state
  defp flush_pending(%{flush_in_flight: op_id} = state) when op_id != nil, do: state

  defp flush_pending(%{pending: pending} = state) do
    raw_batch = Enum.reverse(pending)
    batch = Enum.map(raw_batch, fn {key, value, exp} ->
      {key, to_disk_binary(value), exp}
    end)

    state = maybe_rotate_file(state)

    case NIF.v2_append_batch_nosync(state.active_file_path, batch) do
      {:ok, locations} ->
        Ferricstore.Store.DiskPressure.clear(state.index)
        written = total_written(locations)
        state = update_ets_locations(state, batch, locations)
        state = track_flush_bytes(state, written)
        state = %{state | pending: [], pending_count: 0, fsync_needed: true,
          active_file_size: state.active_file_size + written}
        maybe_notify_fragmentation(state)

      {:error, reason} ->
        Ferricstore.Store.DiskPressure.set(state.index)
        Logger.error("Shard #{state.index}: flush_pending (nosync) failed: #{inspect(reason)} — retaining #{length(raw_batch)} pending entries")
        state
    end
  end

  # Synchronous flush — used by delete, :flush, and :keys calls that need
  # durability guarantees. Uses v2_append_batch (write + fsync in one call).
  # Also ensures any previously-nosync'd data is fsynced.
  defp flush_pending_sync(%{pending: []} = state) do
    # Even with empty pending, we may need to fsync previously-nosync'd data.
    if state.fsync_needed do
      NIF.v2_fsync(state.active_file_path)
      %{state | fsync_needed: false}
    else
      state
    end
  end

  defp flush_pending_sync(%{pending: pending} = state) do
    raw_batch = Enum.reverse(pending)
    batch = Enum.map(raw_batch, fn {key, value, exp} ->
      {key, to_disk_binary(value), exp}
    end)

    state = maybe_rotate_file(state)

    case NIF.v2_append_batch(state.active_file_path, batch) do
      {:ok, locations} ->
        Ferricstore.Store.DiskPressure.clear(state.index)
        written = total_written(locations)
        state = update_ets_locations(state, batch, locations)
        state = track_flush_bytes(state, written)
        state = %{state | pending: [], pending_count: 0, fsync_needed: false,
          active_file_size: state.active_file_size + written}
        maybe_notify_fragmentation(state)

      {:error, reason} ->
        Ferricstore.Store.DiskPressure.set(state.index)
        Logger.error("Shard #{state.index}: flush_pending_sync failed: #{inspect(reason)} — retaining #{length(raw_batch)} pending entries")
        state
    end
  end

  # Wait for any in-flight async fsync to complete before proceeding.
  # This blocks the GenServer until the Tokio fsync result arrives.
  # Used before durability-critical operations (delete, keys, explicit flush).
  defp await_in_flight(%{flush_in_flight: nil} = state), do: state

  defp await_in_flight(%{flush_in_flight: corr_id} = state) do
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

  defp update_ets_locations(state, batch, locations) do
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

  defp total_written(locations) do
    Enum.reduce(locations, 0, fn {_offset, size}, acc -> acc + size end)
  end

  # Increment total_bytes for the active file after a flush.
  defp track_flush_bytes(state, written_bytes) do
    fid = state.active_file_id
    {total, dead} = Map.get(state.file_stats, fid, {0, 0})
    %{state | file_stats: Map.put(state.file_stats, fid, {total + written_bytes, dead})}
  end

  # Track dead bytes when a key is deleted via tombstone (direct path only).
  # Reads the old ETS entry to determine which file contains the now-dead record.
  defp track_delete_dead_bytes(state, key) do
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
  defp maybe_notify_fragmentation(%{sandbox?: true} = state), do: state

  defp maybe_notify_fragmentation(state) do
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

  # Compute per-file dead bytes stats from disk file sizes + ETS live data.
  # Called once during init after recover_keydir. O(file_count + key_count).
  defp compute_file_stats(shard_path, keydir) do
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

  defp maybe_rotate_file(state) do
    if state.active_file_size >= state.max_active_file_size do
      write_hint_for_file(state, state.active_file_id)
      new_id = state.active_file_id + 1
      sp = state.shard_data_path
      new_path = file_path(sp, new_id)
      File.touch!(new_path)
      Ferricstore.Store.ActiveFile.publish(state.index, new_id, new_path, sp)

      # Initialize file_stats for the new file
      new_file_stats = Map.put(state.file_stats, new_id, {0, 0})

      # Notify the merge scheduler that a rotation happened.
      # file_count = new_id + 1 (files are 0-indexed: 0, 1, ..., new_id)
      unless state.sandbox? do
        Ferricstore.Merge.Scheduler.notify_rotation(state.index, new_id + 1)
      end

      %{state | active_file_id: new_id, active_file_path: new_path, active_file_size: 0,
        file_stats: new_file_stats}
    else
      state
    end
  end

  defp write_hint_for_file(state, target_fid) do
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

  defp schedule_flush(ms) do
    Process.send_after(self(), :flush, ms)
  end

  # -------------------------------------------------------------------
  # Private: read helpers
  # -------------------------------------------------------------------

  # v2 local read for transaction closures. Returns {:ok, value} or {:ok, nil}.
  # Replaces NIF.get_zero_copy(state.store, key) in the 2PC local store.
  defp v2_local_read(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil ->
        {:ok, value}

      [{^key, nil, _exp, _lfu, :pending, _off, _vsize}] ->
        # Not yet flushed to disk — should never reach here. If it does,
        # it means ets_lookup_warm failed to catch the :pending sentinel.
        {:error, "ERR internal: pending entry reached cold read path for #{inspect(key)}"}

      [{^key, nil, _exp, _lfu, fid, off, _vsize}] ->
        # Cold key -- pread from disk
        p = file_path(state.shard_data_path, fid)
        NIF.v2_pread_at(p, off)

      _ ->
        {:ok, nil}
    end
  end

  # Alias for compound key reads — same logic as do_get since compound keys
  # are stored as regular ETS/Bitcask entries.
  defp do_compound_get(state, compound_key), do: do_get(state, compound_key)

  defp do_get(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        value

      {:cold, fid, off, vsize, exp} ->
        # Zero-copy cold read via v2 pread (ResourceBinary).
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            cold_read_warm_ets(state, key, value, exp, fid, off, vsize)
            value

          _ ->
            nil
        end

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  defp do_get_meta(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {value, expire_at_ms}

      {:cold, fid, off, vsize, exp} ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            cold_read_warm_ets(state, key, value, exp, fid, off, vsize)
            {value, exp}

          _ ->
            nil
        end

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  # Inserts a key/value/expiry into the single keydir table with LFU counter
  # and v2 disk location fields, and updates the prefix index.
  # New keys start at LFU counter 5. Disk location fields default to 0
  # (updated after append_record writes to disk).
  #
  # Values larger than :ferricstore_hot_cache_max_value_size are stored as
  # nil (cold) to avoid copying large binaries on every :ets.lookup.
  defp ets_insert(state, key, value, expire_at_ms) do
    value_for_ets = value_for_ets(value)
    :ets.insert(state.keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})

    if state.prefix_keys do
      PrefixIndex.track(state.prefix_keys, key, state.index)
    end
  end

  # Inserts a key/value/expiry into the keydir with known disk location (v2).
  defp ets_insert_with_location(state, key, value, expire_at_ms, file_id, offset, value_size) do
    value_for_ets = value_for_ets(value)
    :ets.insert(state.keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), file_id, offset, value_size})

    if state.prefix_keys do
      PrefixIndex.track(state.prefix_keys, key, state.index)
    end
  end

  # Returns nil for values exceeding the hot cache max value size threshold,
  # or the value itself if it fits. This prevents large values from being
  # stored in ETS, avoiding expensive binary copies on every :ets.lookup.
  @compile {:inline, value_for_ets: 1}
  defp value_for_ets(nil), do: nil
  defp value_for_ets(value) when is_integer(value), do: Integer.to_string(value)
  defp value_for_ets(value) when is_float(value), do: Float.to_string(value)
  defp value_for_ets(value) when is_binary(value) do
    if byte_size(value) > :persistent_term.get(:ferricstore_hot_cache_max_value_size, 65_536) do
      nil
    else
      value
    end
  end

  @compile {:inline, to_disk_binary: 1}
  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v

  # Deletes a key from the keydir table and removes it from the prefix index.
  defp ets_delete_key(state, key) do
    :ets.delete(state.keydir, key)

    if state.prefix_keys do
      PrefixIndex.untrack(state.prefix_keys, key, state.index)
    end
  end

  # Classifies an ETS lookup as a cache hit, cold (evicted), expired, or miss.

  # ---------------------------------------------------------------------------
  # Prefix-based ETS helpers (replaces O(N) :ets.foldl full-table scans)
  # ---------------------------------------------------------------------------

  defp prefix_scan_entries(keydir, prefix, shard_data_path \\ nil) do
    now = System.os_time(:millisecond)
    prefix_len = byte_size(prefix)
    # Select all 7-tuple fields so we can cold-read nil values
    ms = [{{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
           [{:andalso, {:is_binary, :"$1"},
             {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
           [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6"}}]}]
    :ets.select(keydir, ms)
    |> Enum.reduce([], fn {key, value, exp, fid, off, _vsize}, acc ->
      if exp == 0 or exp > now do
        # For cold keys (value=nil), do a disk read to get the actual value
        actual_value =
          if value == nil and shard_data_path != nil do
            p = file_path(shard_data_path, fid)
            case NIF.v2_pread_at(p, off) do
              {:ok, v} -> v
              _ -> nil
            end
          else
            value
          end

        if actual_value != nil do
          field = case :binary.split(key, <<0>>) do
            [_pre, sub] -> sub
            _ -> key
          end
          [{field, actual_value} | acc]
        else
          acc
        end
      else
        acc
      end
    end)
  end

  defp prefix_count_entries(keydir, prefix) do
    now = System.os_time(:millisecond)
    prefix_len = byte_size(prefix)
    ms = [{{:"$1", :_, :"$3", :_, :_, :_, :_},
           [{:andalso, {:is_binary, :"$1"},
             {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
           [:"$3"]}]
    :ets.select(keydir, ms)
    |> Enum.count(fn exp -> exp == 0 or exp > now end)
  end

  defp prefix_collect_keys(keydir, prefix) do
    prefix_len = byte_size(prefix)
    ms = [{{:"$1", :_, :_, :_, :_, :_, :_},
           [{:andalso, {:is_binary, :"$1"},
             {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
           [:"$1"]}]
    :ets.select(keydir, ms)
  end

  # v2 7-tuple format: {key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
  # A hit requires value != nil (hot). value = nil means cold (evicted from RAM).
  # On a hit, probabilistically increments the LFU counter.
  # Returns:
  #   {:hit, value, expire_at_ms}
  #   {:cold, file_id, offset, value_size, expire_at_ms}  -- value evicted, disk location known
  #   :expired
  #   :miss
  defp ets_lookup(%{keydir: keydir}, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(keydir, key) do
      [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
        lfu_touch(keydir, key, lfu)
        {:hit, value, 0}

      [{^key, nil, 0, _lfu, :pending, _off, _vsize}] ->
        # Background write pending, value evicted before disk write.
        # Cannot read from disk yet. Treat as miss (rare edge case).
        :miss

      [{^key, nil, 0, _lfu, fid, off, vsize}] ->
        # Cold key (evicted from RAM) with no expiry -- disk location known.
        {:cold, fid, off, vsize, 0}

      [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        lfu_touch(keydir, key, lfu)
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
        # Background write pending with TTL, value evicted before disk write.
        :miss

      [{^key, nil, exp, _lfu, fid, off, vsize}] when exp > now ->
        # Cold key with valid TTL -- disk location known.
        {:cold, fid, off, vsize, exp}

      [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
        # Expired entry -- delete it
        :ets.delete(keydir, key)
        :expired

      [] ->
        :miss
    end
  end

  # Like ets_lookup/2, but transparently warms cold keys via v2_pread_at.
  # Returns {:hit, value, expire_at_ms}, :expired, or :miss — never {:cold, ...}.
  # Use this for read-modify-write operations that need the value in memory.
  defp ets_lookup_warm(state, key) do
    case ets_lookup(state, key) do
      {:cold, fid, off, _vsize, exp} ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            cold_read_warm_ets(state, key, value)
            {:hit, value, exp}

          _ ->
            :miss
        end

      other ->
        other
    end
  end

  # LFU touch with time-based decay (Redis-compatible).
  # Decays counter based on elapsed minutes, then probabilistically increments.
  defp lfu_touch(keydir, key, packed_lfu) do
    LFU.touch(keydir, key, packed_lfu)
  end

  # v2: cold read via pread_at using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  defp warm_from_store(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, _exp, _lfu, :pending, _off, _vsize}] ->
        # Background write not yet completed -- cannot read from disk.
        nil

      [{^key, nil, exp, _lfu, fid, off, vsize}] ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            v = value_for_ets(value)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            value

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  # Synchronous local read from disk for use inside handle_call (avoids
  # GenServer.call deadlock). Returns {:ok, value}, {:ok, nil}, or :error.
  defp v2_local_read(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, _exp, _lfu, fid, off, _vsize}] ->
        p = file_path(state.shard_data_path, fid)
        NIF.v2_pread_at(p, off)

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil ->
        {:ok, value}

      _ ->
        {:ok, nil}
    end
  end

  # 3-arity convenience: looks up the cold ETS entry to recover disk location
  # metadata, then delegates to the 7-arity version. Used by async read
  # completion handlers that only have {from, key} and the value from disk.
  defp cold_read_warm_ets(state, key, value) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, exp, _lfu, fid, off, vsize}] ->
        cold_read_warm_ets(state, key, value, exp, fid, off, vsize)

      _ ->
        # Entry was already evicted or overwritten — skip warming.
        :ok
    end
  end

  # Re-warms the ETS cache after a successful cold read.
  # Preserves the disk location (file_id, offset, value_size) and expire_at_ms.
  # Values exceeding the hot_cache_max_value_size threshold are NOT warmed --
  # they stay cold (nil) in ETS to avoid expensive binary copies on read.
  defp cold_read_warm_ets(state, key, value, exp, fid, off, vsize) do
    v = value_for_ets(value)
    :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})

    if state.prefix_keys do
      PrefixIndex.track(state.prefix_keys, key, state.index)
    end
  end

  # v2: cold read meta via pread_at using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  defp warm_meta_from_store(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, exp, _lfu, fid, off, vsize}] ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            v = value_for_ets(value)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            {value, exp}

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  defp live_keys(state) do
    now = System.os_time(:millisecond)

    :ets.foldl(
      fn {key, _value, exp, _lfu, _fid, _off, _vsize}, acc ->
        if exp == 0 or exp > now do
          [key | acc]
        else
          acc
        end
      end,
      [],
      state.keydir
    )
  end

  defp key_alive?(keydir, key, now) do
    case :ets.lookup(keydir, key) do
      [{_, _, 0, _, _, _, _}] -> true
      [{_, _, exp, _, _, _, _}] -> exp > now
      [] -> true
    end
  end

  # -------------------------------------------------------------------
  # Private: integer / float parsing — delegates to shared ValueCodec
  # -------------------------------------------------------------------

  defp parse_integer(str), do: ValueCodec.parse_integer(str)
  defp parse_float(str), do: ValueCodec.parse_float(str)
  defp format_float(val), do: ValueCodec.format_float(val)

  defp coerce_integer(v) when is_integer(v), do: {:ok, v}
  defp coerce_integer(v) when is_float(v), do: :error
  defp coerce_integer(v) when is_binary(v), do: parse_integer(v)

  defp coerce_float(v) when is_float(v), do: {:ok, v}
  defp coerce_float(v) when is_integer(v), do: {:ok, v * 1.0}
  defp coerce_float(v) when is_binary(v), do: parse_float(v)

  # Applies SETRANGE logic: overwrites bytes at `offset` with `value`,
  # zero-padding if the original string is shorter than offset.
  defp apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        # Empty value -- just pad up to offset if needed
        if offset > old_len do
          old <> :binary.copy(<<0>>, offset - old_len)
        else
          old
        end

      offset >= old_len ->
        # Need to pad between end of old and start of overwrite
        padding = :binary.copy(<<0>>, offset - old_len)
        old <> padding <> value

      offset + val_len >= old_len ->
        # Overwrite extends past end of old string
        binary_part(old, 0, offset) <> value

      true ->
        # Overwrite in the middle of the string
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  # -------------------------------------------------------------------
  # Private: active expiry sweep
  # -------------------------------------------------------------------

  # Number of consecutive ceiling-hit sweeps before emitting the
  # :expiry_struggling telemetry event.
  @struggling_threshold 3

  # Performs a single expiry sweep pass: scans ETS for up to `max_keys`
  # expired entries, deletes them from ETS, and purges expired entries
  # from the Bitcask store. Tracks consecutive ceiling-hit sweeps and
  # emits telemetry when the sweep is struggling or recovers.
  defp do_expiry_sweep(state) do
    now = System.os_time(:millisecond)
    max_keys = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep, @default_max_keys_per_sweep)
    expired_keys = scan_expired(state.keydir, now, max_keys)

    count = length(expired_keys)

    if count > 0 do
      # v2: write tombstones for expired keys and remove from ETS
      Enum.each(expired_keys, fn key ->
        case NIF.v2_append_tombstone(state.active_file_path, key) do
          {:ok, _} ->
            ets_delete_key(state, key)

          {:error, reason} ->
            Logger.warning("Shard #{state.index}: tombstone write failed during expiry sweep for #{inspect(key)}: #{inspect(reason)}")
        end
      end)
      Ferricstore.Stats.incr_expired_keys(count)

      require Logger
      Logger.debug("Shard #{state.index}: expiry sweep removed #{count} key(s)")
    end

    # Track whether the sweep hit the ceiling (removed exactly max_keys).
    hit_ceiling = count >= max_keys and count > 0

    {new_ceiling_count, new_struggling} =
      if hit_ceiling do
        new_count = state.sweep_at_ceiling_count + 1

        if new_count >= @struggling_threshold and not state.sweep_struggling do
          :telemetry.execute(
            [:ferricstore, :expiry, :struggling],
            %{shard_index: state.index, consecutive_ceiling_sweeps: new_count, max_keys_per_sweep: max_keys},
            %{}
          )

          {new_count, true}
        else
          {new_count, state.sweep_struggling}
        end
      else
        if state.sweep_struggling do
          :telemetry.execute(
            [:ferricstore, :expiry, :recovered],
            %{shard_index: state.index, previous_ceiling_sweeps: state.sweep_at_ceiling_count},
            %{}
          )
        end

        {0, false}
      end

    %{state | sweep_at_ceiling_count: new_ceiling_count, sweep_struggling: new_struggling}
  end

  defp scan_expired(keydir, now, limit) do
    # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
    # Match entries where expire_at_ms > 0 and expire_at_ms <= now
    match_spec = [
      {{:"$1", :_, :"$2", :_, :_, :_, :_},
       [{:andalso, {:>, :"$2", 0}, {:"=<", :"$2", now}}],
       [:"$1"]}
    ]

    case :ets.select(keydir, match_spec, limit) do
      {keys, _continuation} -> keys
      :"$end_of_table" -> []
    end
  end

  defp schedule_expiry_sweep do
    interval = Application.get_env(:ferricstore, :expiry_sweep_interval_ms, @default_sweep_interval_ms)
    Process.send_after(self(), :expiry_sweep, interval)
  end

  defp schedule_frag_check do
    interval = Application.get_env(:ferricstore, :frag_check_interval_ms, @default_frag_check_interval_ms)
    Process.send_after(self(), :frag_check, interval)
  end

  # --- Native command helpers ---

  defp resolve_for_native(state, key) do
    case ets_lookup_warm(state, key) do
      {:hit, value, exp} -> {{:hit, value, exp}, state}
      :expired -> {:expired, state}
      :miss ->
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        case do_get_meta(state, key) do
          nil -> {:missing, state}
          {value, exp} -> {{:hit, value, exp}, state}
        end
    end
  end

  # Delegates to the shared ValueCodec to avoid duplication with state_machine.ex.
  defp encode_ratelimit(cur, start, prev), do: ValueCodec.encode_ratelimit(cur, start, prev)
  defp decode_ratelimit(value), do: ValueCodec.decode_ratelimit(value)

  # -------------------------------------------------------------------
  # Private: collection promotion helpers
  # -------------------------------------------------------------------

  # Returns the dedicated path for a promoted key, or nil.
  # Returns the dedicated path for a promoted key, or nil if not promoted.
  defp promoted_store(state, redis_key) do
    case Map.get(state.promoted_instances, redis_key) do
      %{path: path} -> path
      path when is_binary(path) -> path  # backwards compat during migration
      nil -> nil
    end
  end

  # Reads a key from a promoted dedicated Bitcask directory (v2 path-based).
  # Scans ETS first (compound keys are warmed into ETS on recover).
  # Falls back to scanning the dedicated log file for the key.
  # Returns {:ok, value} or {:ok, value, expire_at_ms} depending on path.
  # The scan fallback returns expiry; the pread fast path does not (callers
  # should read expiry from ETS when the fast path is used).
  defp promoted_read(dedicated_path, compound_key, keydir) do
    # Try O(1) pread using file_id + offset from ETS keydir.
    case :ets.lookup(keydir, compound_key) do
      [{^compound_key, _value, exp, _lfu, fid, offset, _vsize}] when is_integer(fid) and offset > 0 ->
        file_path = dedicated_file_path(dedicated_path, fid)

        case NIF.v2_pread_at(file_path, offset) do
          {:ok, value} -> {:ok, value, exp}
          other -> other
        end

      _ ->
        # No valid offset — fall back to scan of the active file.
        # Scan returns expiry per record, so we can pass it through.
        active = Ferricstore.Store.Promotion.find_active(dedicated_path)

        case NIF.v2_scan_file(active) do
          {:ok, records} ->
            last_entry =
              records
              |> Enum.filter(fn {k, _off, _vsize, _exp, _is_tomb} -> k == compound_key end)
              |> List.last()

            case last_entry do
              nil -> {:ok, nil}
              {_key, _offset, _vsize, _exp, true} -> {:ok, nil}
              {_key, offset, _vsize, exp, false} ->
                case NIF.v2_pread_at(active, offset) do
                  {:ok, value} -> {:ok, value, exp}
                  other -> other
                end
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # Writes a key-value pair to the promoted dedicated Bitcask directory.
  # Returns {:ok, {fid, offset, record_size}} where fid is derived from the
  # file path, avoiding a separate File.ls syscall.
  defp promoted_write(dedicated_path, compound_key, value, expire_at_ms) do
    active = Ferricstore.Store.Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    case NIF.v2_append_record(active, compound_key, value, expire_at_ms) do
      {:ok, {offset, record_size}} -> {:ok, {fid, offset, record_size}}
      {:error, _} = err -> err
    end
  end

  # Writes a tombstone for a key in the promoted dedicated Bitcask directory.
  defp promoted_tombstone(dedicated_path, compound_key) do
    active = Ferricstore.Store.Promotion.find_active(dedicated_path)
    NIF.v2_append_tombstone(active, compound_key)
  end

  # Parses the file_id from a log file path like ".../00005.log" -> 5
  defp parse_fid_from_path(path) do
    path |> Path.basename() |> String.trim_trailing(".log") |> String.to_integer()
  end

  # Builds the file path for a specific file_id in a dedicated directory.
  defp dedicated_file_path(dedicated_path, file_id) do
    Path.join(dedicated_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  # Increments the write counter for a promoted instance and triggers
  # compaction when the threshold is reached.
  defp bump_promoted_writes(state, redis_key) do
    case Map.get(state.promoted_instances, redis_key) do
      %{path: path, total_bytes: total, dead_bytes: dead, last_compacted_at: last} = info ->
        frag = if total > 0, do: dead / total, else: 0.0

        cooldown_ok =
          last == nil or
            System.system_time(:millisecond) - last >= @promoted_compaction_cooldown_ms

        if frag >= @promoted_frag_threshold and dead >= @promoted_dead_bytes_min and cooldown_ok do
          state = compact_dedicated(state, redis_key, path)
          # After compaction, recompute total_bytes from the new file on disk
          new_total = promoted_dir_size(path)
          new_info = %{info | dead_bytes: 0, total_bytes: new_total,
                       last_compacted_at: System.system_time(:millisecond)}
          new_promoted = Map.put(state.promoted_instances, redis_key, new_info)
          %{state | promoted_instances: new_promoted}
        else
          %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, info)}
        end

      # Legacy format without byte tracking — upgrade in place
      %{path: path, writes: _writes} = info ->
        new_info = Map.merge(info, %{total_bytes: promoted_dir_size(path),
                                     dead_bytes: 0, last_compacted_at: nil})
        new_promoted = Map.put(state.promoted_instances, redis_key, new_info)
        %{state | promoted_instances: new_promoted}

      _ ->
        state
    end
  end

  # Returns total size of all .log files in a promoted directory.
  defp promoted_dir_size(dir_path) do
    case File.ls(dir_path) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reduce(0, fn name, acc ->
          case File.stat(Path.join(dir_path, name)) do
            {:ok, %{size: s}} -> acc + s
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  # Tracks dead bytes for a promoted collection on overwrite (put).
  # Reads old ETS entry to determine the previous record size, then
  # increments dead_bytes and total_bytes on the promoted instance.
  defp track_promoted_dead_bytes(state, redis_key, compound_key, new_record_size) do
    case Map.get(state.promoted_instances, redis_key) do
      %{total_bytes: total, dead_bytes: dead} = info ->
        old_record_size =
          case :ets.lookup(state.keydir, compound_key) do
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}] when old_vsize > 0 ->
              @record_header_size + byte_size(compound_key) + old_vsize

            _ ->
              0
          end

        new_info = %{info |
          dead_bytes: dead + old_record_size,
          total_bytes: total + new_record_size
        }

        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, new_info)}

      _ ->
        state
    end
  end

  # Tracks dead bytes for a promoted collection on delete (tombstone).
  # The entire old record becomes dead.
  defp track_promoted_delete_bytes(state, redis_key, compound_key) do
    case Map.get(state.promoted_instances, redis_key) do
      %{dead_bytes: dead} = info ->
        old_record_size =
          case :ets.lookup(state.keydir, compound_key) do
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}] when old_vsize > 0 ->
              @record_header_size + byte_size(compound_key) + old_vsize

            _ ->
              0
          end

        new_info = %{info | dead_bytes: dead + old_record_size}
        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, new_info)}

      _ ->
        state
    end
  end

  # Compacts a dedicated promoted Bitcask by rewriting live entries to a new file.
  #
  # Steps:
  # 1. Scan ETS for live compound keys belonging to this collection
  # 2. Write them to a new file (next file_id)
  # 3. Update ETS entries with new file_id + offsets
  # 4. Delete old file(s)
  defp compact_dedicated(state, redis_key, dedicated_path) do
    alias Ferricstore.Store.{CompoundKey, Promotion}

    # Determine the prefix for this collection's compound keys
    prefix = promoted_prefix_for(state, redis_key)

    if prefix == nil do
      Logger.warning("Shard #{state.index}: cannot determine prefix for promoted key #{inspect(redis_key)}, skipping compaction")
      state
    else
      # Find the current active file_id and compute the next
      active = Ferricstore.Store.Promotion.find_active(dedicated_path)
      old_fid = parse_fid_from_path(active)
      new_fid = old_fid + 1
      new_file = dedicated_file_path(dedicated_path, new_fid)
      File.touch!(new_file)

      # Collect live entries from ETS
      now = System.os_time(:millisecond)

      live_entries =
        :ets.foldl(
          fn {key, value, exp, _lfu, _fid, _off, _vsize}, acc ->
            if is_binary(key) and value != nil and String.starts_with?(key, prefix) and
                 (exp == 0 or exp > now) do
              [{key, value, exp} | acc]
            else
              acc
            end
          end,
          [],
          state.keydir
        )

      if live_entries == [] do
        # Nothing to compact — remove the new empty file
        File.rm(new_file)
        state
      else
        # Write all live entries to the new file
        batch = Enum.map(live_entries, fn {k, v, exp} -> {k, v, exp} end)

        case NIF.v2_append_batch(new_file, batch) do
          {:ok, results} ->
            # Update ETS with new file_id + offsets
            live_entries
            |> Enum.zip(results)
            |> Enum.each(fn {{key, value, expire_at_ms}, {offset, value_size}} ->
              value_for_ets = value_for_ets(value)
              :ets.insert(state.keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), new_fid, offset, value_size})
            end)

            # Delete old files (all files with fid < new_fid)
            case File.ls(dedicated_path) do
              {:ok, files} ->
                Enum.each(files, fn name ->
                  if String.ends_with?(name, ".log") do
                    fid = name |> String.trim_trailing(".log") |> String.to_integer()
                    if fid < new_fid do
                      File.rm(Path.join(dedicated_path, name))
                    end
                  end
                end)

              _ -> :ok
            end

            Logger.debug(
              "Shard #{state.index}: compacted dedicated #{inspect(redis_key)} " <>
                "(#{length(live_entries)} live entries, fid #{old_fid} -> #{new_fid})"
            )

            :telemetry.execute(
              [:ferricstore, :dedicated, :compaction],
              %{live_entries: length(live_entries), old_fid: old_fid, new_fid: new_fid},
              %{shard_index: state.index, redis_key: redis_key}
            )

            state

          {:error, reason} ->
            Logger.error("Shard #{state.index}: dedicated compaction write failed: #{inspect(reason)}")
            File.rm(new_file)
            state
        end
      end
    end
  end

  # Determines the compound key prefix for a promoted redis key by checking
  # the promotion marker type in ETS.
  defp promoted_prefix_for(state, redis_key) do
    mk = Ferricstore.Store.Promotion.marker_key(redis_key)

    case :ets.lookup(state.keydir, mk) do
      [{^mk, "hash", _, _, _, _, _}] -> "H:" <> redis_key <> <<0>>
      [{^mk, "set", _, _, _, _, _}] -> "S:" <> redis_key <> <<0>>
      [{^mk, "zset", _, _, _, _, _}] -> "Z:" <> redis_key <> <<0>>
      _ -> nil
    end
  end

  # After a compound_put to the shared Bitcask, checks whether the
  # collection should be promoted. Triggers for hash (H:), set (S:),
  # and sorted set (Z:) compound keys when the entry count exceeds the
  # threshold.
  #
  # Lists are NOT promoted because they store all elements in a single
  # Bitcask entry (serialized Erlang term) rather than compound keys.
  # A list with 1000 elements is still one Bitcask entry, so promotion
  # would provide no benefit.
  defp maybe_promote(state, redis_key, compound_key) do
    alias Ferricstore.Store.{CompoundKey, Promotion}

    threshold = Promotion.threshold()

    # Disabled if threshold is 0, or already promoted
    if threshold == 0 or Map.has_key?(state.promoted_instances, redis_key) do
      state
    else
      # Detect the collection type from the compound key prefix
      case detect_compound_type(redis_key, compound_key) do
        nil ->
          # Not a promotable compound key (e.g. list, type metadata)
          state

        {type, prefix} ->
          # Count entries for this collection using the shared match-spec
          # helper instead of :ets.foldl which scans all keys in the keydir.
          count = prefix_count_entries(state.keydir, prefix)

          if count > threshold do
            # Flush pending writes so the shared Bitcask has all data
            state = await_in_flight(state)
            state = flush_pending_sync(state)

            case Promotion.promote_collection!(
                   type,
                   redis_key,
                   state.shard_data_path,
                   state.keydir,
                   state.data_dir,
                   state.index
                 ) do
              {:ok, dedicated_store} ->
                new_promoted = Map.put(state.promoted_instances, redis_key, %{
                  path: dedicated_store, writes: 0,
                  total_bytes: 0, dead_bytes: 0, last_compacted_at: nil
                })
                %{state | promoted_instances: new_promoted}

              {:error, _reason} ->
                # Promotion failed -- continue using shared Bitcask
                state
            end
          else
            state
          end
      end
    end
  end

  # Detects the compound key type from its prefix and returns
  # `{type_atom, scan_prefix}` or `nil` if not a promotable type.
  defp detect_compound_type(redis_key, compound_key) do
    alias Ferricstore.Store.CompoundKey

    cond do
      String.starts_with?(compound_key, CompoundKey.hash_prefix(redis_key)) ->
        {:hash, CompoundKey.hash_prefix(redis_key)}

      String.starts_with?(compound_key, CompoundKey.set_prefix(redis_key)) ->
        {:set, CompoundKey.set_prefix(redis_key)}

      String.starts_with?(compound_key, CompoundKey.zset_prefix(redis_key)) ->
        {:zset, CompoundKey.zset_prefix(redis_key)}

      true ->
        nil
    end
  end

  # Submits a write command through Raft. For production shards, routes
  # through the Batcher (group commit). For sandbox shards, submits
  # directly to the private ra system via process_command (blocking).
  defp raft_write(%__MODULE__{sandbox_ra_server_id: sid} = _state, command) when sid != nil do
    case :ra.process_command(sid, command) do
      {:ok, result, _leader} -> result
      {:error, reason} -> {:error, inspect(reason)}
      {:timeout, _} -> {:error, "ERR raft timeout"}
    end
  end

  defp raft_write(%__MODULE__{index: index}, command) do
    Ferricstore.Raft.Batcher.write(index, command)
  end

  # Async variant for handle_call paths that use {:noreply, state}.
  # For production shards, the Batcher replies directly to the caller.
  # For sandbox shards, submits synchronously and replies immediately.
  defp raft_write_async(%__MODULE__{sandbox_ra_server_id: sid} = _state, command, from) when sid != nil do
    result =
      case :ra.process_command(sid, command) do
        {:ok, result, _leader} -> result
        {:error, reason} -> {:error, inspect(reason)}
        {:timeout, _} -> {:error, "ERR raft timeout"}
      end

    GenServer.reply(from, result)
  end

  defp raft_write_async(%__MODULE__{index: index}, command, from) do
    Ferricstore.Raft.Batcher.write_async(index, command, from)
  end

  # Returns true if this shard has a pre-existing Batcher process (started by
  # Application.start for shards 0..N-1). If so, also starts the ra server
  # for this shard. Isolated test shards with ad-hoc indices won't have a
  # Batcher and fall back to the direct write path.
  defp start_raft_if_available(index, shard_data_path, active_file_id, active_file_path, ets) do
    batcher_name = Ferricstore.Raft.Batcher.batcher_name(index)

    if Process.whereis(batcher_name) != nil do
      try do
        Ferricstore.Raft.Cluster.start_shard_server(index, shard_data_path, active_file_id, active_file_path, ets)
        true
      catch
        _, _ -> false
      end
    else
      false
    end
  end

  # Starts a ra server for a sandbox shard using a private ra system.
  # Uses a unique server name derived from the sandbox_uid to avoid
  # collisions with production ra servers and other sandbox instances.
  # Returns {raft?, sandbox_ra_server_id}.
  defp start_sandbox_raft(ra_system, index, shard_data_path, active_file_id, active_file_path, ets, prefix_keys, opts) do
    sandbox_uid = Keyword.fetch!(opts, :sandbox_uid)
    server_name = :"ferricstore_sandbox_shard_#{sandbox_uid}_#{index}"
    server_id = {server_name, node()}
    uid = "sbx_#{sandbox_uid}_#{index}"

    machine_config = %{
      shard_index: index,
      shard_data_path: shard_data_path,
      active_file_id: active_file_id,
      active_file_path: active_file_path,
      ets: ets,
      prefix_keys: prefix_keys
    }

    server_config = %{
      id: server_id,
      uid: uid,
      cluster_name: :"ferricstore_sandbox_cluster_#{sandbox_uid}_#{index}",
      initial_members: [server_id],
      machine: {:module, Ferricstore.Raft.StateMachine, machine_config},
      log_init_args: %{uid: uid},
      system: ra_system
    }

    try do
      case :ra.start_server(ra_system, server_config) do
        :ok ->
          :ra.trigger_election(server_id)
          wait_for_sandbox_leader(server_id)
          {true, server_id}

        {:error, {:already_started, _pid}} ->
          {true, server_id}

        {:error, reason} ->
          Logger.warning("Sandbox ra server start failed: #{inspect(reason)}")
          {false, nil}
      end
    catch
      kind, reason ->
        Logger.warning("Sandbox ra server start crashed: #{inspect(kind)} #{inspect(reason)}")
        {false, nil}
    end
  end

  defp wait_for_sandbox_leader(server_id, attempts \\ 50)
  defp wait_for_sandbox_leader(_server_id, 0), do: :ok

  defp wait_for_sandbox_leader(server_id, attempts) do
    case :ra.members(server_id) do
      {:ok, _members, _leader} -> :ok
      _ ->
        Process.sleep(10)
        wait_for_sandbox_leader(server_id, attempts - 1)
    end
  end

end
