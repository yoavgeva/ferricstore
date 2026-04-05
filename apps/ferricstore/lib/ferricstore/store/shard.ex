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
  alias Ferricstore.Store.{LFU, Router, ValueCodec}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1

  # Default maximum active file size before rotation (256 MB).
  # Configurable via :max_active_file_size application env.
  @default_max_active_file_size 256 * 1024 * 1024

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
    :index,
    :data_dir,
    # Cached result of DataDir.shard_data_path(data_dir, index).
    # Computed once during init; avoids string concat on every cold read/flush.
    :shard_data_path,
    # FerricStore.Instance context — holds all per-instance refs (shard_names,
    # slot_map, keydir_refs, atomics, config) needed to route operations without
    # global state. Passed to Router.* calls instead of persistent_term lookups.
    :instance_ctx,
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
    ctx = Keyword.get(opts, :instance_ctx)
    name = if ctx, do: Router.shard_name(ctx, index), else: :"Ferricstore.Store.Shard.#{index}"
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true

  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
    ctx = Keyword.get(opts, :instance_ctx)

    path = Ferricstore.DataDir.shard_data_path(data_dir, index)
    File.mkdir_p!(path)

    # v2: scan data_dir for existing .log files, find highest file_id
    {active_file_id, active_file_size} = ShardLifecycle.discover_active_file(path)
    active_file_path = file_path(path, active_file_id)

    # Ensure the active file exists (touch it)
    unless File.exists?(active_file_path) do
      File.touch!(active_file_path)
    end

    # Create/clear named ETS tables.
    # Use instance-scoped names from ctx if available, else default naming.
    keydir_name =
      if ctx, do: elem(ctx.keydir_refs, index), else: :"keydir_#{index}"

    keydir =
      case :ets.whereis(keydir_name) do
        :undefined ->
          :ets.new(keydir_name, [:set, :public, :named_table, {:read_concurrency, true}, {:write_concurrency, :auto}, {:decentralized_counters, true}])

        _ref ->
          :ets.delete_all_objects(keydir_name)
          keydir_name
      end

    # Remove any leftover hot_cache table from a previous run.
    case :ets.whereis(:"hot_cache_#{index}") do
      :undefined -> :ok
      _ref -> :ets.delete(:"hot_cache_#{index}")
    end

    ets = keydir

    # v2: recover ETS keydir from hint files or by scanning log files BEFORE
    # starting Raft. This ensures cold entries ({key, nil, ..., fid, off, vsize})
    # are in ETS when ra replays WAL entries via apply/3. Without this, replayed
    # read-modify-write commands (INCR, APPEND, etc.) see ETS misses during
    # replay and start from nil instead of the correct prior value.
    # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
    # Must run BEFORE recover_promoted so PM: markers are in ETS.
    ShardLifecycle.recover_keydir(path, keydir, index)

    # Start the Raft server for this shard (unless explicitly disabled).
    raft? =
      if Keyword.get(opts, :raft_enabled, true) do
        ShardLifecycle.start_raft_if_available(index, path, active_file_id, active_file_path, ets)
      else
        false
      end

    # Recover promoted collection instances
    promoted = Ferricstore.Store.Promotion.recover_promoted(path, keydir, data_dir, index)

    # Migrate existing prob files: scan prob dir for files without
    # corresponding metadata markers in the keydir. Write markers so
    # DEL can clean up prob files and BF.INFO/CMS.INFO can recover metadata.
    ShardLifecycle.migrate_prob_files(path, keydir, index)

    # Publish active file metadata to ActiveFile registry
    Ferricstore.Store.ActiveFile.publish(index, active_file_id, active_file_path, path)

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
    ShardLifecycle.schedule_expiry_sweep()
    ShardLifecycle.schedule_frag_check()
    max_file_size = if ctx, do: ctx.max_active_file_size, else: @default_max_active_file_size

    {:ok, %__MODULE__{ets: keydir, keydir: keydir,
                       index: index, data_dir: data_dir,
                       shard_data_path: path,
                       instance_ctx: ctx,
                       active_file_id: active_file_id,
                       active_file_path: active_file_path,
                       active_file_size: active_file_size,
                       pending: [], flush_in_flight: nil,
                       promoted_instances: promoted,
                       file_stats: file_stats,
                       merge_config: merge_config,
                       raft?: raft?,
                       max_active_file_size: max_file_size},
     {:continue, {:flush_interval, flush_ms}}}
  end

  defp file_path(shard_path, file_id), do: ShardETS.file_path(shard_path, file_id)

  @impl true
  def handle_continue({:flush_interval, ms}, state) do
    # Store flush interval in process dictionary so handle_info can reschedule.
    Process.put(:flush_interval_ms, ms)
    {:noreply, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state), do: ShardReads.handle_get(key, state)

  def handle_call({:get_file_ref, key}, _from, state), do: ShardReads.handle_get_file_ref(key, state)

  def handle_call({:get_meta, key}, _from, state), do: ShardReads.handle_get_meta(key, state)

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
    ShardWrites.handle_delete_prefix(prefix, state)
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
    ShardWrites.handle_put(key, value, expire_at_ms, from, state)
  end

  # Atomic increment: reads current value, parses as integer, adds delta, writes back.
  # Returns {:ok, new_integer} or {:error, reason}.
  def handle_call({:incr, key, delta}, _from, state) do
    ShardWrites.handle_incr(key, delta, state)
  end

  # Atomic float increment: reads current value, parses as float, adds delta, writes back.
  # Returns {:ok, new_float_string} or {:error, reason}.
  def handle_call({:incr_float, key, delta}, _from, state) do
    ShardWrites.handle_incr_float(key, delta, state)
  end

  # Atomic append: reads current value (or ""), appends suffix, writes back.
  # Returns {:ok, new_byte_length}.
  def handle_call({:append, key, suffix}, _from, state) do
    ShardWrites.handle_append(key, suffix, state)
  end

  # Atomic get-and-set: returns old value (or nil), sets new value.
  def handle_call({:getset, key, new_value}, _from, state) do
    ShardWrites.handle_getset(key, new_value, state)
  end

  # Atomic get-and-delete: returns value (or nil), deletes key.
  def handle_call({:getdel, key}, _from, state) do
    ShardWrites.handle_getdel(key, state)
  end

  # Atomic get-and-update-expiry: returns value, updates TTL.
  # expire_at_ms = 0 means PERSIST (remove expiry).
  def handle_call({:getex, key, expire_at_ms}, _from, state) do
    ShardWrites.handle_getex(key, expire_at_ms, state)
  end

  # Atomic set-range: overwrites portion of string at offset with value.
  # Zero-pads if key doesn't exist or string is shorter than offset.
  # Returns {:ok, new_byte_length}.
  def handle_call({:setrange, key, offset, value}, _from, state) do
    ShardWrites.handle_setrange(key, offset, value, state)
  end

  def handle_call({:delete, key}, from, state) do
    ShardWrites.handle_delete(key, from, state)
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
    ctx = state.instance_ctx
    shared =
      if ctx do
        size = :counters.info(ctx.write_version).size
        if state.index < size, do: :counters.get(ctx.write_version, state.index + 1), else: 0
      else
        Ferricstore.Store.WriteVersion.get(state.index)
      end
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
    instance_ctx = state.instance_ctx
    # Build a minimal context with only the fields closures need, to avoid
    # capturing the entire state struct (which includes pending list,
    # promoted_instances, etc. that hold stale references).
    ctx = %{
      keydir: state.keydir,
      index: state.index,
      shard_data_path: state.shard_data_path,
      data_dir: state.data_dir
    }

    # Direct put: write to ETS immediately, queue for async Bitcask flush.
    # This mirrors the non-raft {:put, ...} handler logic.
    local_put = fn key, value, expire_at_ms ->
      is_local = Router.shard_for(instance_ctx, key) == my_idx

      if is_local do
        ets_insert(ctx, key, value, expire_at_ms)
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
        end

        send(self(), {:tx_pending_write, key, value, expire_at_ms})
        :ok
      else
        Router.put(instance_ctx, key, value, expire_at_ms)
      end
    end

    local_delete = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
        ets_delete_key(state, key)
        # Track deletion so subsequent reads within this tx see the key as gone
        deleted = Process.get(:tx_deleted_keys, MapSet.new())
        Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
        send(self(), {:tx_pending_delete, key})
        :ok
      else
        Router.delete(instance_ctx, key)
      end
    end

    local_get = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.get(instance_ctx, key)
      end
    end

    local_get_meta = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.get_meta(instance_ctx, key)
      end
    end

    local_exists = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.exists?(instance_ctx, key)
      end
    end

    local_incr = fn key, delta ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.incr(instance_ctx, key, delta)
      end
    end

    local_incr_float = fn key, delta ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.incr_float(instance_ctx, key, delta)
      end
    end

    local_append = fn key, suffix ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.append(instance_ctx, key, suffix)
      end
    end

    local_getset = fn key, new_value ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.getset(instance_ctx, key, new_value)
      end
    end

    local_getdel = fn key ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.getdel(instance_ctx, key)
      end
    end

    local_getex = fn key, expire_at_ms ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.getex(instance_ctx, key, expire_at_ms)
      end
    end

    local_setrange = fn key, offset, value ->
      if Router.shard_for(instance_ctx, key) == my_idx do
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
        Router.setrange(instance_ctx, key, offset, value)
      end
    end

    %{
      get: local_get,
      get_meta: local_get_meta,
      put: local_put,
      delete: local_delete,
      exists?: local_exists,
      keys: fn -> Router.keys(instance_ctx) end,
      flush: fn ->
        Enum.each(Router.keys(instance_ctx), fn k -> Router.delete(instance_ctx, k) end)
        :ok
      end,
      dbsize: fn -> Router.dbsize(instance_ctx) end,
      incr: local_incr,
      incr_float: local_incr_float,
      append: local_append,
      getset: local_getset,
      getdel: local_getdel,
      getex: local_getex,
      setrange: local_setrange,
      cas: fn key, expected, new_value, ttl_ms -> Router.cas(instance_ctx, key, expected, new_value, ttl_ms) end,
      lock: fn key, owner, ttl_ms -> Router.lock(instance_ctx, key, owner, ttl_ms) end,
      unlock: fn key, owner -> Router.unlock(instance_ctx, key, owner) end,
      extend: fn key, owner, ttl_ms -> Router.extend(instance_ctx, key, owner, ttl_ms) end,
      ratelimit_add: fn key, window_ms, max, count -> Router.ratelimit_add(instance_ctx, key, window_ms, max, count) end,
      list_op: fn key, operation -> Router.list_op(instance_ctx, key, operation) end,
      compound_get: fn redis_key, compound_key ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
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
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_get, redis_key, compound_key})
        end
      end,
      compound_get_meta: fn redis_key, compound_key ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
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
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
        end
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          ets_insert(ctx, compound_key, value, expire_at_ms)
          send(self(), {:tx_pending_write, compound_key, value, expire_at_ms})
          :ok
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
        end
      end,
      compound_delete: fn redis_key, compound_key ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          ets_delete_key(ctx, compound_key)
          send(self(), {:tx_pending_delete, compound_key})
          :ok
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_delete, redis_key, compound_key})
        end
      end,
      compound_scan: fn redis_key, prefix ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          # Pass shard_data_path to enable cold-read for recovered keys
          results = prefix_scan_entries(ctx.keydir, prefix, ctx.shard_data_path)
          Enum.sort_by(results, fn {field, _} -> field end)
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_scan, redis_key, prefix})
        end
      end,
      compound_count: fn redis_key, prefix ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          prefix_count_entries(ctx.keydir, prefix)
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_count, redis_key, prefix})
        end
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        if Router.shard_for(instance_ctx, redis_key) == my_idx do
          # Uses :ets.select match spec instead of :ets.foldl full-table scan
          keys_to_delete = prefix_collect_keys(ctx.keydir, prefix)

          Enum.each(keys_to_delete, fn key ->
            ets_delete_key(ctx, key)
            send(self(), {:tx_pending_delete, key})
          end)

          :ok
        else
          shard = Router.resolve_shard(instance_ctx, Router.shard_for(instance_ctx, redis_key))
          GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
        end
      end,
      prob_dir: fn ->
        Path.join(ctx.shard_data_path, "prob")
      end,
      prob_write: fn command ->
        Router.prob_write(instance_ctx, command)
      end,
      shard_index: ctx.index,
      data_dir: ctx.data_dir,
      on_push: &Ferricstore.Waiters.notify_push/1
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
    ShardNativeOps.handle_cas(key, expected, new_value, ttl_ms, state)
  end

  def handle_call({:lock, key, owner, ttl_ms}, _from, state) do
    ShardNativeOps.handle_lock(key, owner, ttl_ms, state)
  end

  def handle_call({:unlock, key, owner}, _from, state) do
    ShardNativeOps.handle_unlock(key, owner, state)
  end

  def handle_call({:extend, key, owner, ttl_ms}, _from, state) do
    ShardNativeOps.handle_extend(key, owner, ttl_ms, state)
  end

  def handle_call({:ratelimit_add, key, window_ms, max, count}, _from, state) do
    ShardNativeOps.handle_ratelimit_add(key, window_ms, max, count, state)
  end

  # 6-tuple variant: includes pre-computed now_ms from Router.raft_write.
  def handle_call({:ratelimit_add, key, window_ms, max, count, _now_ms}, _from, state) do
    ShardNativeOps.handle_ratelimit_add_direct(key, window_ms, max, count, state)
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

  def handle_call({:exists, key}, _from, state), do: ShardReads.handle_exists(key, state)

  def handle_call(:keys, _from, state), do: ShardReads.handle_keys(state)

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
    state = ShardLifecycle.do_expiry_sweep(state)
    {:reply, :ok, state}
  end

  # Catch-all for commands not handled above (prob commands, etc.).
  # When Raft is disabled (e.g., isolated test instances), these commands
  # arrive via GenServer.call instead of Raft apply/3. Delegate to the
  # state machine's apply logic directly.
  def handle_call(command, _from, state) when is_tuple(command) and not state.raft? do
    sm_state = %{
      shard_index: state.index,
      shard_data_path: state.shard_data_path,
      active_file_id: state.active_file_id,
      active_file_path: state.active_file_path,
      ets: state.ets,
      data_dir: state.data_dir,
      applied_count: 0,
      release_cursor_interval: 20_000,
      cross_shard_locks: %{},
      cross_shard_intents: %{},
      instance_ctx: state.instance_ctx
    }

    case Ferricstore.Raft.StateMachine.apply(%{}, command, sm_state) do
      {_new_state, result} -> {:reply, result, state}
      {_new_state, result, _effects} -> {:reply, result, state}
    end
  end

  # Periodic fragmentation re-evaluation for idle shards.
  # Catches shards that accumulated dead data then stopped receiving writes.
  # Also clears disk pressure flag periodically so writes can probe recovery.
  def handle_info(:frag_check, state) do
    if Ferricstore.Store.DiskPressure.under_pressure?(state.instance_ctx, state.index) do
      Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.index)
    end

    state = maybe_notify_fragmentation(state)
    ShardLifecycle.schedule_frag_check()
    {:noreply, state}
  end

  # Active expiry sweep: scan ETS for expired keys and delete them.
  # When the sweep finds nothing to expire and there are no pending writes
  # or in-flight flushes, hibernate the GenServer to trigger a full GC
  # and shrink the heap. This reclaims memory accumulated during busy periods
  # on idle shards (memory audit L1).
  def handle_info(:expiry_sweep, state) do
    state = ShardLifecycle.do_expiry_sweep(state)
    ShardLifecycle.schedule_expiry_sweep()

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
  def terminate(reason, state), do: ShardLifecycle.do_terminate(reason, state)

  # -------------------------------------------------------------------
  # Private: flush
  # -------------------------------------------------------------------

  defp flush_pending(state), do: ShardFlush.flush_pending(state)
  defp flush_pending_sync(state), do: ShardFlush.flush_pending_sync(state)
  defp await_in_flight(state), do: ShardFlush.await_in_flight(state)
  defp track_delete_dead_bytes(state, key), do: ShardFlush.track_delete_dead_bytes(state, key)
  defp maybe_notify_fragmentation(state), do: ShardFlush.maybe_notify_fragmentation(state)
  defp compute_file_stats(shard_path, keydir), do: ShardFlush.compute_file_stats(shard_path, keydir)
  defp schedule_flush(ms), do: ShardFlush.schedule_flush(ms)

  # -------------------------------------------------------------------
  # Private: read helpers (delegates to Shard.Reads)
  # -------------------------------------------------------------------

  # Alias for compound key reads — same logic as do_get since compound keys
  # are stored as regular ETS/Bitcask entries.
  defp do_compound_get(state, compound_key), do: do_get(state, compound_key)

  defp do_get(state, key), do: ShardReads.do_get(state, key)

  defp do_get_meta(state, key), do: ShardReads.do_get_meta(state, key)

  # Inserts a key/value/expiry into the single keydir table with LFU counter
  # and v2 disk location fields, and updates the prefix index.
  # New keys start at LFU counter 5. Disk location fields default to 0
  # (updated after append_record writes to disk).
  #
  # Values larger than :ferricstore_hot_cache_max_value_size are stored as
  # nil (cold) to avoid copying large binaries on every :ets.lookup.
  defp ets_insert(state, key, value, expire_at_ms),
    do: ShardETS.ets_insert(state, key, value, expire_at_ms)

  defp ets_insert_with_location(state, key, value, expire_at_ms, file_id, offset, value_size),
    do: ShardETS.ets_insert_with_location(state, key, value, expire_at_ms, file_id, offset, value_size)

  defp hot_cache_threshold(state), do: ShardETS.hot_cache_threshold(state)

  defp value_for_ets(value, threshold), do: ShardETS.value_for_ets(value, threshold)

  defp to_disk_binary(v), do: ShardETS.to_disk_binary(v)

  defp ets_delete_key(state, key), do: ShardETS.ets_delete_key(state, key)

  defp prefix_scan_entries(keydir, prefix, shard_data_path),
    do: ShardETS.prefix_scan_entries(keydir, prefix, shard_data_path)

  defp prefix_count_entries(keydir, prefix), do: ShardETS.prefix_count_entries(keydir, prefix)

  defp prefix_collect_keys(keydir, prefix), do: ShardETS.prefix_collect_keys(keydir, prefix)

  defp ets_lookup_warm(state, key), do: ShardETS.ets_lookup_warm(state, key)


  defp warm_from_store(state, key), do: ShardETS.warm_from_store(state, key)
  defp warm_meta_from_store(state, key), do: ShardETS.warm_meta_from_store(state, key)

  defp v2_local_read(state, key), do: ShardReads.v2_local_read(state, key)

  defp cold_read_warm_ets(state, key, value),
    do: ShardETS.cold_read_warm_ets(state, key, value)

  # -------------------------------------------------------------------
  # Private: integer / float parsing — delegates to shared ValueCodec
  # -------------------------------------------------------------------


  defp coerce_integer(v), do: ShardETS.coerce_integer(v)

  defp coerce_float(v), do: ShardETS.coerce_float(v)

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
              value_for_ets = value_for_ets(value, hot_cache_threshold(state))
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

  # Submits a write command through Raft via the Batcher (group commit).
  defp raft_write(%__MODULE__{index: index}, command) do
    Ferricstore.Raft.Batcher.write(index, command)
  end

  # Async variant for handle_call paths that use {:noreply, state}.
  # The Batcher replies directly to the caller.
  defp raft_write_async(%__MODULE__{index: index}, command, from) do
    Ferricstore.Raft.Batcher.write_async(index, command, from)
  end


end
