defmodule Ferricstore.Raft.StateMachine do
  @moduledoc """
  Ra state machine for a single FerricStore shard.

  Each shard is an independent Raft group. The state machine receives write
  commands via `apply/3`, which deterministically applies them to both the
  Bitcask persistent store (via synchronous NIF) and the ETS hot cache.

  ## Callbacks

    * `init/1` -- receives the shard config, returns initial machine state.
    * `apply/3` -- deterministic command application (called on every node).
      Supports `:put`, `:delete`, and `:batch` commands.
    * `state_enter/2` -- lifecycle hook for leader/follower transitions.
    * `tick/2` -- periodic callback (unused currently, placeholder for metrics).
    * `init_aux/1` -- initializes non-replicated auxiliary state.
    * `handle_aux/5` -- handles non-replicated auxiliary commands (new API).
    * `overview/1` -- returns a summary map for debugging/monitoring.

  ## Design notes

  Per the spec (section 2C.4):
  - `apply/3` is deterministic and runs on every node in the Raft group.
  - Only synchronous NIF calls are allowed inside `apply/3`.
  - Effects (`send_msg`, `release_cursor`) are returned as the third element
    of the apply return tuple.
  - In single-node mode, the shard's Raft group has one member (self quorum),
    so every write commits immediately after local log append + fsync.

  ## HLC piggybacking (spec 2G.6)

  HLC timestamps are piggybacked on Raft commands. The `Batcher` stamps each
  command with the leader's current HLC timestamp before submitting it to ra.
  When `apply/3` processes a command carrying an `hlc_ts` metadata map, it
  calls `HLC.update/1` to merge the leader's clock into the local node's HLC.

  In single-node mode this merge is a no-op (the node merges its own
  timestamp). In multi-node clusters, followers use this to stay
  causally synchronized with the leader's clock, bounding inter-node TTL
  precision to Raft heartbeat RTT (~10 ms).

  Commands may arrive in two forms:

    * **Wrapped**: `{inner_command, %{hlc_ts: {physical_ms, logical}}}` --
      the metadata map carries the leader's HLC timestamp for merging.
    * **Unwrapped**: `inner_command` (legacy / test) -- processed as before
      without HLC merging.

  ## Log compaction (spec 2E.5)

  The Raft log grows unbounded unless compacted. Every
  `:release_cursor_interval` applied commands (default: 1000), `apply/3`
  emits a `{:release_cursor, ra_index, state}` effect. This tells ra that
  all log entries up to `ra_index` are fully reflected in the given state
  snapshot and can be safely truncated.

  The interval is stored in the machine state at init time (from the config
  map or application env) so that `apply/3` remains deterministic -- it never
  reads runtime configuration.
  """

  @behaviour :ra_machine

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.HLC
  alias Ferricstore.Store.{BitcaskWriter, LFU, ListOps, Router, ValueCodec}

  @default_release_cursor_interval 20_000

  @type shard_state :: %{
          shard_index: non_neg_integer(),
          shard_data_path: binary(),
          active_file_id: non_neg_integer(),
          active_file_path: binary(),
          ets: atom(),
          applied_count: non_neg_integer(),
          release_cursor_interval: pos_integer()
        }

  # ---------------------------------------------------------------------------
  # ra_machine callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Initializes the state machine for a shard.

  The `config` map must include (v2 -- path-based, no NIF store reference):

    * `:shard_index` -- zero-based shard index
    * `:shard_data_path` -- absolute path to the shard's Bitcask data directory
    * `:active_file_id` -- numeric ID of the active log file
    * `:active_file_path` -- absolute path to the active log file
    * `:ets` -- ETS table name (already created)

  Optional:

    * `:release_cursor_interval` -- number of applies between release_cursor
      effects (default: #{@default_release_cursor_interval}). Can also be set
      via `Application.get_env(:ferricstore, :release_cursor_interval)`.

  Returns the initial machine state.
  """
  @impl true
  @spec init(map()) :: shard_state()
  def init(config) do
    interval =
      Map.get_lazy(config, :release_cursor_interval, fn ->
        Application.get_env(:ferricstore, :release_cursor_interval, @default_release_cursor_interval)
      end)

    %{
      shard_index: config.shard_index,
      shard_data_path: config.shard_data_path,
      active_file_id: config.active_file_id,
      active_file_path: config.active_file_path,
      ets: config.ets,
      data_dir: Map.get(config, :data_dir, Path.dirname(config.shard_data_path)),
      instance_ctx: Map.get(config, :instance_ctx),
      applied_count: 0,
      release_cursor_interval: interval,
      # When a node joins with pre-existing Bitcask data (from direct copy or
      # object storage snapshot), skip_below_index prevents re-applying entries
      # that are already in Bitcask + ETS. Entries at or below this index are
      # no-ops — the data was recovered from disk via recover_keydir.
      skip_below_index: Map.get(config, :skip_below_index, 0),
      # Cross-shard operation locks and intents — persisted in Raft state
      # so they survive shard restarts, snapshots, and leader failovers.
      cross_shard_locks: %{},
      cross_shard_intents: %{}
    }
  end

  @doc """
  Applies a replicated command to the shard state.

  Supported commands:

    * `{:put, key, value, expire_at_ms}` -- Write a key-value pair with optional
      expiry. Writes to Bitcask (sync NIF) and updates ETS.
    * `{:delete, key}` -- Delete a key. Writes a tombstone to Bitcask, removes
      from ETS.
    * `{:batch, commands}` -- Apply a list of commands atomically. Each command
      in the batch is a tuple matching one of the above forms. Returns
      `{:ok, results}` where results is a list of individual command results.
    * `{:list_op, key, operation}` -- Execute a list operation (LPUSH, RPUSH,
      LPOP, RPOP, etc.) as an atomic read-modify-write. Reads the current value
      from ETS/Bitcask, delegates to `ListOps.execute/4`, and persists the result.
    * `{:compound_put, compound_key, value, expire_at_ms}` -- Write a hash/set/zset
      field. Inserts `{compound_key, value, expire_at_ms}` into ETS and Bitcask.
    * `{:compound_delete, compound_key}` -- Delete a hash/set/zset field. Removes
      the compound key from ETS and Bitcask.
    * `{:compound_delete_prefix, prefix}` -- Delete all compound keys matching the
      given prefix from ETS and Bitcask. Used by DEL on data structures (hashes,
      sets, sorted sets) to clean up all fields.
    * `{:incr_float, key, delta}` -- Atomic read-modify-write float increment.
      Reads the current value, parses as float, adds `delta`, formats the result,
      and writes back. Returns `{:ok, new_float_string}` or
      `{:error, "ERR value is not a valid float"}`.
    * `{:append, key, suffix}` -- Atomic read-modify-write append. Reads the
      current value (or `""`), concatenates `suffix`, writes back. Returns
      `{:ok, byte_size(new_value)}`.
    * `{:getset, key, new_value}` -- Atomic get-and-set. Reads the old value,
      writes the new value with no expiry, returns the old value (or `nil`).
    * `{:getdel, key}` -- Atomic get-and-delete. Reads the value, deletes the
      key, returns the value (or `nil`).
    * `{:getex, key, expire_at_ms}` -- Atomic get-and-update-expiry. Reads the
      value, re-writes with the new `expire_at_ms`, returns the value (or `nil`).
    * `{:setrange, key, offset, value}` -- Atomic set-range. Reads the current
      value, pads with zero bytes if needed, replaces bytes at `offset`, writes
      back. Returns `{:ok, byte_size(new_value)}`.
    * `{:cas, key, expected, new_value, ttl_ms}` -- Compare-and-swap. Reads the
      current value; if it matches `expected`, writes `new_value` with optional
      TTL. Returns `1` (swapped), `0` (mismatch), or `nil` (key missing/expired).
    * `{:lock, key, owner, ttl_ms}` -- Distributed lock acquire. If the key does
      not exist, is expired, or is already held by the same owner, sets
      `{owner, ttl}`. Returns `:ok` or `{:error, reason}`.
    * `{:unlock, key, owner}` -- Distributed lock release. If the key exists and
      the owner matches, deletes the key. Returns `1` on success,
      `{:error, reason}` on owner mismatch.
    * `{:extend, key, owner, ttl_ms}` -- Distributed lock TTL extension. If the
      key exists and the owner matches, updates the TTL. Returns `1` on success,
      `{:error, reason}` on owner mismatch or missing key.
    * `{:ratelimit_add, key, window_ms, max, count}` -- Sliding window rate
      limiter. Reads counters, rotates windows, computes effective count, and
      updates. Returns `[status, count, remaining, ttl_ms]`.

  Returns `{new_state, result}` or `{new_state, result, effects}`.
  """
  # Skip entries that are already in Bitcask + ETS from a data sync copy.
  # When a node joins with pre-existing data (copied at raft_index N),
  # entries at or below N are no-ops — avoid redundant ETS overwrites
  # and Bitcask appends.
  @impl true
  def apply(%{index: idx} = meta, _command, %{skip_below_index: skip} = state)
      when skip > 0 and idx <= skip do
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}

    # Clear skip_below_index once we've passed it — no need to check on every apply
    new_state =
      if idx == skip, do: %{new_state | skip_below_index: 0}, else: new_state

    maybe_release_cursor(meta, old_count, new_state, :ok)
  end

  @impl true
  def apply(meta, {:put, key, value, expire_at_ms}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    result =
      case check_key_lock(state, redis_key, nil) do
        :ok ->
          with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

        {:error, :key_locked} ->
          {:error, :key_locked}
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:delete, key}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    result =
      case check_key_lock(state, redis_key, nil) do
        :ok ->
          with_pending_writes(state, fn -> do_delete(state, key) end)

        {:error, :key_locked} ->
          {:error, :key_locked}
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:batch, commands}, state) do
    old_count = state.applied_count

    # All commands in a batch share one pending-writes buffer so they
    # are flushed in a single v2_append_batch_nosync NIF call.
    {results, new_count} =
      with_pending_writes(state, fn ->
        Enum.map_reduce(commands, old_count, fn cmd, count ->
          result = apply_single(state, cmd)
          {result, count + 1}
        end)
      end)

    new_state = %{state | applied_count: new_count}
    maybe_release_cursor(meta, old_count, new_state, {:ok, results})
  end

  def apply(meta, {:cross_shard_tx, shard_batches}, state) do
    old_count = state.applied_count

    shard_results =
      Enum.reduce(shard_batches, %{}, fn {shard_idx, queue, sandbox_namespace}, acc ->
        store = build_cross_shard_store(shard_idx, state)

        Process.put(:tx_deleted_keys, MapSet.new())

        results =
          try do
            Enum.map(queue, fn {cmd, args} ->
              namespaced_args = namespace_args(args, sandbox_namespace)

              try do
                Dispatcher.dispatch(cmd, namespaced_args, store)
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

        Map.put(acc, shard_idx, results)
      end)

    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, shard_results)
  end

  # Legacy: list operations used to be sent as a single {:list_op} Raft entry
  # containing the entire operation. Now lists use compound keys (L:key\0pos)
  # and individual {:put}/{:delete} entries. This handler remains for WAL
  # replay of entries written before the compound-key migration.
  def apply(meta, {:list_op, key, operation}, state) do
    result = with_pending_writes(state, fn -> do_list_op(state, key, operation) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:compound_put, compound_key, value, expire_at_ms}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    result =
      case check_key_lock(state, redis_key, nil) do
        :ok ->
          with_pending_writes(state, fn -> do_put(state, compound_key, value, expire_at_ms) end)

        {:error, :key_locked} ->
          {:error, :key_locked}
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:compound_delete, compound_key}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    result =
      case check_key_lock(state, redis_key, nil) do
        :ok ->
          with_pending_writes(state, fn -> do_delete(state, compound_key) end)

        {:error, :key_locked} ->
          {:error, :key_locked}
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:compound_delete_prefix, prefix}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

    result =
      case check_key_lock(state, redis_key, nil) do
        :ok ->
          with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

        {:error, :key_locked} ->
          {:error, :key_locked}
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:incr, key, delta}, state) do
    result = with_pending_writes(state, fn -> do_incr(state, key, delta) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:incr_float, key, delta}, state) do
    result = with_pending_writes(state, fn -> do_incr_float(state, key, delta) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:append, key, suffix}, state) do
    result = with_pending_writes(state, fn -> do_append(state, key, suffix) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:getset, key, new_value}, state) do
    result = with_pending_writes(state, fn -> do_getset(state, key, new_value) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:getdel, key}, state) do
    result = with_pending_writes(state, fn -> do_getdel(state, key) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:getex, key, expire_at_ms}, state) do
    result = with_pending_writes(state, fn -> do_getex(state, key, expire_at_ms) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:setrange, key, offset, value}, state) do
    result = with_pending_writes(state, fn -> do_setrange(state, key, offset, value) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:cas, key, expected, new_value, ttl_ms}, state) do
    result = with_pending_writes(state, fn -> do_cas(state, key, expected, new_value, ttl_ms) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:lock, key, owner, ttl_ms}, state) do
    result = with_pending_writes(state, fn -> do_lock(state, key, owner, ttl_ms) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:unlock, key, owner}, state) do
    result = with_pending_writes(state, fn -> do_unlock(state, key, owner) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:extend, key, owner, ttl_ms}, state) do
    result = with_pending_writes(state, fn -> do_extend(state, key, owner, ttl_ms) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:ratelimit_add, key, window_ms, max, count}, state) do
    result = with_pending_writes(state, fn -> do_ratelimit_add(state, key, window_ms, max, count, nil) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  # 6-tuple variant: shard pre-computes now_ms for deterministic replay.
  def apply(meta, {:ratelimit_add, key, window_ms, max, count, now_ms}, state) do
    result = with_pending_writes(state, fn -> do_ratelimit_add(state, key, window_ms, max, count, now_ms) end)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  # ---------------------------------------------------------------------------
  # Cross-shard operation commands (mini-percolator)
  #
  # These commands support the CrossShardOp protocol: per-key locking through
  # Raft consensus, intent records for crash recovery, and locked writes.
  # ---------------------------------------------------------------------------

  def apply(meta, {:lock_keys, keys, owner_ref, expire_at_ms}, state) do
    {new_state, result} = do_lock_keys(state, keys, owner_ref, expire_at_ms)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:unlock_keys, keys, owner_ref}, state) do
    {new_state, result} = do_unlock_keys(state, keys, owner_ref)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:cross_shard_intent, owner_ref, intent_map}, state) do
    {new_state, result} = do_write_intent(state, owner_ref, intent_map)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:delete_intent, owner_ref}, state) do
    {new_state, result} = do_delete_intent(state, owner_ref)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:get_intents}, state) do
    result = do_get_intents(state)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:get_lock_count}, state) do
    result = map_size(Map.get(state, :cross_shard_locks, %{}))
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:clear_locks}, state) do
    new_state = %{state | cross_shard_locks: %{}, cross_shard_intents: %{}}
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, :ok)
  end

  def apply(meta, {:locked_put, key, value, expire_at_ms, owner_ref}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    result =
      case check_key_lock(state, redis_key, owner_ref) do
        :ok ->
          with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

        {:error, _} = err ->
          err
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:locked_delete, key, owner_ref}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    result =
      case check_key_lock(state, redis_key, owner_ref) do
        :ok ->
          with_pending_writes(state, fn -> do_delete(state, key) end)

        {:error, _} = err ->
          err
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:locked_delete_prefix, prefix, owner_ref}, state) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

    result =
      case check_key_lock(state, redis_key, owner_ref) do
        :ok ->
          with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

        {:error, _} = err ->
          err
      end

    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  # ---------------------------------------------------------------------------
  # Probabilistic data structure commands (bloom, CMS, cuckoo, TopK)
  #
  # These commands replicate prob mutations through Raft so that followers
  # apply the same NIF writes to their local prob files. Read commands
  # (BF.EXISTS, CMS.QUERY, etc.) bypass Raft and go directly to the local
  # stateless pread NIF.
  # ---------------------------------------------------------------------------

  # -- Bloom --

  def apply(meta, {:bloom_create, key, num_bits, num_hashes, prob_meta}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)
      NIF.bloom_file_create(path, num_bits, num_hashes)
      do_put(state, key, :erlang.term_to_binary(prob_meta), 0)
      :ok
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:bloom_add, key, element, auto_create_params}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)
      auto_create_bloom_if_needed(state, path, key, auto_create_params)
      NIF.bloom_file_add(path, element)
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:bloom_madd, key, elements, auto_create_params}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)
      auto_create_bloom_if_needed(state, path, key, auto_create_params)
      NIF.bloom_file_madd(path, elements)
    end)
    bump_applied(meta, state, result)
  end

  # -- CMS --

  def apply(meta, {:cms_create, key, width, depth}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "cms")
      ensure_prob_dir(state)
      NIF.cms_file_create(path, width, depth)
      meta_val = {:cms_meta, %{width: width, depth: depth}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:cms_incrby, key, items}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "cms")
      NIF.cms_file_incrby(path, items)
    end)
    bump_applied(meta, state, result)
  end

  # src_paths are pre-resolved absolute paths (sources may be on different shards)
  def apply(meta, {:cms_merge, dst_key, src_paths, weights, create_params}, state) do
    result = do_prob_command(state, fn ->
      dst_path = prob_path(state, dst_key, "cms")
      ensure_prob_dir(state)
      unless File.exists?(dst_path) do
        %{width: w, depth: d} = create_params
        NIF.cms_file_create(dst_path, w, d)
        meta_val = {:cms_meta, %{width: w, depth: d}}
        do_put(state, dst_key, :erlang.term_to_binary(meta_val), 0)
      end
      NIF.cms_file_merge(dst_path, src_paths, weights)
    end)
    bump_applied(meta, state, result)
  end

  # -- Cuckoo --

  def apply(meta, {:cuckoo_create, key, capacity, bucket_size}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)
      NIF.cuckoo_file_create(path, capacity, bucket_size)
      meta_val = {:cuckoo_meta, %{capacity: capacity}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:cuckoo_add, key, element, auto_create_params}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)
      auto_create_cuckoo_if_needed(state, path, key, auto_create_params)
      NIF.cuckoo_file_add(path, element)
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:cuckoo_addnx, key, element, auto_create_params}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)
      auto_create_cuckoo_if_needed(state, path, key, auto_create_params)
      NIF.cuckoo_file_addnx(path, element)
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:cuckoo_del, key, element}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      NIF.cuckoo_file_del(path, element)
    end)
    bump_applied(meta, state, result)
  end

  # -- TopK --

  def apply(meta, {:topk_create, key, k, width, depth, decay}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      ensure_prob_dir(state)
      NIF.topk_file_create_v2(path, k, width, depth, decay)
      meta_val = {:topk_meta, %{path: path, k: k, width: width, depth: depth, decay: decay}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:topk_add, key, elements}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_add_v2(path, elements)
    end)
    bump_applied(meta, state, result)
  end

  def apply(meta, {:topk_incrby, key, pairs}, state) do
    result = do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_incrby_v2(path, pairs)
    end)
    bump_applied(meta, state, result)
  end

  # ---------------------------------------------------------------------------
  # HLC-wrapped commands (spec 2G.6)
  #
  # When the Batcher stamps a command with an HLC timestamp, it wraps the
  # original command in a 2-tuple: `{inner_command, %{hlc_ts: {phys, logical}}}`.
  #
  # This catch-all clause:
  #   1. Merges the remote HLC timestamp into the local HLC (side-effect only,
  #      does not affect deterministic state machine output).
  #   2. Delegates to the matching `apply/3` clause for the inner command.
  #
  # In single-node mode, the merge is effectively a no-op because the leader
  # and the applying node are the same process. In multi-node clusters, this
  # ensures follower HLCs stay synchronized with the leader.
  # ---------------------------------------------------------------------------

  # Generic server command hook — allows server apps to replicate their own
  # commands through Raft without the library knowing what they are.
  # The server registers a raft_apply_hook callback on the Instance struct.
  def apply(meta, {:server_command, command}, state) do
    hook =
      case state.instance_ctx do
        %{raft_apply_hook: fun} when is_function(fun) -> fun
        _ ->
          try do
            FerricStore.Instance.get(:default).raft_apply_hook
          rescue
            _ -> nil
          end
      end

    result = if hook, do: hook.(command), else: {:error, :no_hook}
    bump_applied(meta, state, result)
  end

  def apply(meta, {inner_command, %{hlc_ts: remote_ts}}, state) when is_tuple(inner_command) do
    merge_hlc(remote_ts)
    __MODULE__.apply(meta, inner_command, state)
  end

  @doc """
  Lifecycle hook called when the Raft node transitions roles.

  When becoming leader, generates a fresh HLC timestamp via `HLC.now/0` to
  ensure the leader's clock is up to date before it starts stamping commands.
  This is a side-effect only -- it does not affect the deterministic state
  machine output.

  In single-node mode, the node is always the leader. In multi-node clusters,
  this can be used to start/stop leader-only processes (e.g., merge scheduler,
  active expiry sweeper).

  Returns a list of effects (currently empty).
  """
  @impl true
  def state_enter(:leader, _state) do
    # Ensure the leader's HLC is freshly advanced. In multi-node clusters,
    # this guarantees the new leader's clock is at least at wall-clock time
    # before it begins stamping commands for followers to merge.
    HLC.now()
    []
  end

  def state_enter(:follower, _state), do: []
  def state_enter(:candidate, _state), do: []
  def state_enter(:await_condition, _state), do: []
  def state_enter(:delete_and_terminate, _state), do: []
  def state_enter(:receive_snapshot, _state), do: []
  def state_enter(_role, _state), do: []

  @doc """
  Periodic tick callback. Returns a list of effects (currently empty).
  """
  @impl true
  def tick(_time_ms, _state) do
    []
  end

  @doc """
  Initializes non-replicated auxiliary state.

  Aux state is local to each node and not replicated via Raft. Used for
  tracking hot-key statistics and other node-local metadata.
  """
  @impl true
  def init_aux(_name) do
    %{hot_keys: %{}}
  end

  @doc """
  Handles non-replicated auxiliary commands (5-arity new API).

  The `int_state` parameter is ra's internal state and must be passed back
  unchanged in the return tuple.

  Currently supports:
    * `{:cast, {:key_written, key}}` -- Increments a local hot-key counter.
  """
  # Cap hot_keys map to prevent unbounded memory growth. When the map exceeds
  # 10,000 entries, reset it to prevent the ra process heap from growing
  # indefinitely with unique keys. This bounds memory to ~1MB worst case.
  @hot_keys_max_size 10_000

  @impl true
  def handle_aux(_raft_state, :cast, {:key_written, key}, aux, int_state) do
    hot = aux.hot_keys

    if map_size(hot) >= @hot_keys_max_size do
      # Reset to prevent unbounded growth; start fresh with just this key.
      {:no_reply, %{aux | hot_keys: %{key => 1}}, int_state}
    else
      count = Map.get(hot, key, 0)
      {:no_reply, %{aux | hot_keys: Map.put(hot, key, count + 1)}, int_state}
    end
  end

  def handle_aux(_raft_state, _type, _cmd, aux, int_state) do
    {:no_reply, aux, int_state}
  end

  @doc """
  Returns a summary map for debugging and monitoring.

  Includes the shard index, ETS keydir size, total applied command count,
  and the release_cursor interval.
  """
  @impl true
  def overview(state) do
    ets_size =
      try do
        :ets.info(state.ets, :size)
      rescue
        ArgumentError -> 0
      end

    %{
      shard_index: state.shard_index,
      keydir_size: ets_size,
      applied_count: state.applied_count,
      release_cursor_interval: state.release_cursor_interval
    }
  end

  # ---------------------------------------------------------------------------
  # Private: release_cursor compaction
  # ---------------------------------------------------------------------------

  # Checks whether the applied_count crossed an interval boundary AND the
  # ra meta contains a valid index. If both conditions are met, emits a
  # `{:release_cursor, ra_index, state}` effect so ra can compact the log
  # up to this point.
  #
  # For single commands (put/delete), old_count + 1 == new applied_count,
  # so `div(old, interval) != div(new, interval)` is equivalent to
  # `rem(new, interval) == 0`.
  #
  # For batches, the applied_count may jump by N, potentially crossing one
  # or more interval boundaries. We emit a single release_cursor at the
  # batch's ra index when any boundary was crossed.
  #
  # When meta has no :index (e.g. unit tests calling apply/3 directly with
  # an empty map), the 2-tuple `{state, result}` is returned and no effect
  # is emitted.
  @spec maybe_release_cursor(map(), non_neg_integer(), shard_state(), term()) ::
          {shard_state(), term()} | {shard_state(), term(), list()}
  defp maybe_release_cursor(%{index: ra_index}, old_count, state, result) do
    interval = state.release_cursor_interval

    if div(old_count, interval) != div(state.applied_count, interval) do
      {state, result, [{:release_cursor, ra_index, state}]}
    else
      {state, result}
    end
  end

  defp maybe_release_cursor(_meta, _old_count, state, result) do
    {state, result}
  end

  # ---------------------------------------------------------------------------
  # Private: cross-shard transaction store builder
  # ---------------------------------------------------------------------------

  # Builds a store map for a given shard_idx, usable by Dispatcher.dispatch.
  # For the anchor shard (matching state.shard_index), uses state directly.
  # For remote shards, reads active file info from persistent_term.
  defp build_cross_shard_store(shard_idx, anchor_state) do
    instance_ctx = anchor_state.instance_ctx

    ctx =
      if shard_idx == anchor_state.shard_index do
        %{
          keydir: anchor_state.ets,
          index: shard_idx,
          shard_data_path: anchor_state.shard_data_path,
          active_file_path: anchor_state.active_file_path,
          active_file_id: anchor_state.active_file_id
        }
      else
        {file_id, file_path, shard_data_path} =
          Ferricstore.Store.ActiveFile.get(shard_idx)

        keydir =
          if instance_ctx do
            elem(instance_ctx.keydir_refs, shard_idx)
          else
            :"keydir_#{shard_idx}"
          end

        %{
          keydir: keydir,
          index: shard_idx,
          shard_data_path: shard_data_path,
          active_file_path: file_path,
          active_file_id: file_id
        }
      end

    local_put = fn key, value, expire_at_ms ->
      value_for = value_for_ets(value, hot_cache_threshold(anchor_state))
      disk_val = to_disk_binary(value)
      :ets.insert(ctx.keydir, {key, value_for, expire_at_ms, LFU.initial(), 0, 0, 0})
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      if MapSet.member?(deleted, key) do
        Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
      end
      BitcaskWriter.write(
        ctx.index,
        ctx.active_file_path,
        ctx.active_file_id,
        ctx.keydir,
        key,
        disk_val,
        expire_at_ms
      )
      :ok
    end

    local_delete = fn key ->
      :ets.delete(ctx.keydir, key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
      # Write tombstone via BitcaskWriter to ensure ordering
      BitcaskWriter.delete(ctx.index, ctx.active_file_path, key)
      :ok
    end

    local_get = fn key ->
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      if MapSet.member?(deleted, key) do
        nil
      else
        cross_shard_ets_read(ctx, key)
      end
    end

    local_get_meta = fn key ->
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      if MapSet.member?(deleted, key) do
        nil
      else
        cross_shard_ets_read_meta(ctx, key)
      end
    end

    local_exists = fn key ->
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      if MapSet.member?(deleted, key) do
        false
      else
        cross_shard_ets_read(ctx, key) != nil
      end
    end

    local_incr = fn key, delta ->
      current = local_get.(key)
      case current do
        nil ->
          local_put.(key, delta, 0)
          {:ok, delta}
        value ->
          case coerce_integer(value) do
            {:ok, int_val} ->
              new_val = int_val + delta
              local_put.(key, new_val, 0)
              {:ok, new_val}
            :error ->
              {:error, "ERR value is not an integer or out of range"}
          end
      end
    end

    local_incr_float = fn key, delta ->
      current = local_get.(key)
      case current do
        nil ->
          new_val = delta * 1.0
          local_put.(key, new_val, 0)
          {:ok, new_val}
        value ->
          case coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              local_put.(key, new_val, 0)
              {:ok, new_val}
            :error ->
              {:error, "ERR value is not a valid float"}
          end
      end
    end

    local_append = fn key, suffix ->
      current = case local_get.(key) do
        nil -> ""
        v when is_integer(v) -> Integer.to_string(v)
        v when is_float(v) -> Float.to_string(v)
        v -> v
      end
      new_val = current <> suffix
      local_put.(key, new_val, 0)
      {:ok, byte_size(new_val)}
    end

    local_getset = fn key, new_value ->
      old = local_get.(key)
      local_put.(key, new_value, 0)
      old
    end

    local_getdel = fn key ->
      old = local_get.(key)
      if old, do: local_delete.(key)
      old
    end

    local_getex = fn key, expire_at_ms ->
      value = local_get.(key)
      if value, do: local_put.(key, value, expire_at_ms)
      value
    end

    local_setrange = fn key, offset, value ->
      old = case local_get.(key) do
        nil -> ""
        v when is_integer(v) -> Integer.to_string(v)
        v when is_float(v) -> Float.to_string(v)
        v -> v
      end
      new_val = sm_apply_setrange(old, offset, value)
      local_put.(key, new_val, 0)
      {:ok, byte_size(new_val)}
    end

    data_dir =
      if instance_ctx do
        instance_ctx.data_dir
      else
        Application.get_env(:ferricstore, :data_dir, "data")
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
      ratelimit_add: fn key, window_ms, max, count ->
        Router.ratelimit_add(instance_ctx, key, window_ms, max, count)
      end,
      list_op: fn key, op -> Router.list_op(instance_ctx, key, op) end,
      compound_get: fn _redis_key, compound_key ->
        cross_shard_ets_read(ctx, compound_key)
      end,
      compound_get_meta: fn _redis_key, compound_key ->
        cross_shard_ets_read_meta(ctx, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        local_put.(compound_key, value, expire_at_ms)
      end,
      compound_delete: fn _redis_key, compound_key ->
        local_delete.(compound_key)
      end,
      compound_scan: fn _redis_key, prefix ->
        cross_shard_prefix_scan(ctx, prefix)
      end,
      compound_count: fn _redis_key, prefix ->
        cross_shard_prefix_count(ctx.keydir, prefix)
      end,
      compound_delete_prefix: fn _redis_key, prefix ->
        cross_shard_delete_prefix(ctx, prefix, local_delete)
      end,
      prob_dir: fn ->
        Path.join(ctx.shard_data_path, "prob")
      end,
      prob_write: fn command ->
        # Within cross-shard tx, prob writes are applied directly
        # (the state machine is already applying through Raft)
        apply_prob_locally(instance_ctx, command)
      end,
      shard_index: ctx.index,
      data_dir: data_dir
    }
  end

  defp namespace_args(args, nil), do: args
  defp namespace_args([], _ns), do: []
  defp namespace_args([key | rest], ns) when is_binary(key), do: [ns <> key | rest]
  defp namespace_args(args, _ns), do: args

  # Reads a value from a shard's keydir ETS table with cold-read fallback.
  defp cross_shard_ets_read(ctx, key) do
    now = System.os_time(:millisecond)
    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          value
        [{^key, nil, 0, _lfu, fid, off, _vsize}] when is_integer(fid) and fid > 0 ->
          path = sm_file_path_from_ctx(ctx, fid)
          case NIF.v2_pread_at(path, off) do
            {:ok, v} -> v
            _ -> nil
          end
        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          value
        [{^key, nil, exp, _lfu, fid, off, _vsize}] when exp > now and is_integer(fid) and fid > 0 ->
          path = sm_file_path_from_ctx(ctx, fid)
          case NIF.v2_pread_at(path, off) do
            {:ok, v} -> v
            _ -> nil
          end
        _ ->
          nil
      end
    rescue
      ArgumentError -> nil
    end
  end

  # Reads value + expire_at_ms from a shard's keydir ETS table.
  defp cross_shard_ets_read_meta(ctx, key) do
    now = System.os_time(:millisecond)
    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          {value, 0}
        [{^key, nil, 0, _lfu, fid, off, _vsize}] when is_integer(fid) and fid > 0 ->
          path = sm_file_path_from_ctx(ctx, fid)
          case NIF.v2_pread_at(path, off) do
            {:ok, v} -> {v, 0}
            _ -> nil
          end
        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {value, exp}
        [{^key, nil, exp, _lfu, fid, off, _vsize}] when exp > now and is_integer(fid) and fid > 0 ->
          path = sm_file_path_from_ctx(ctx, fid)
          case NIF.v2_pread_at(path, off) do
            {:ok, v} -> {v, exp}
            _ -> nil
          end
        _ ->
          nil
      end
    rescue
      ArgumentError -> nil
    end
  end

  defp cross_shard_prefix_scan(ctx, prefix) do
    now = System.os_time(:millisecond)
    prefix_len = byte_size(prefix)
    ms = [{{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
           [{:andalso, {:is_binary, :"$1"},
             {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
           [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6"}}]}]
    try do
      :ets.select(ctx.keydir, ms)
      |> Enum.reduce([], fn {key, value, exp, fid, off, _vsize}, acc ->
        if exp == 0 or exp > now do
          actual_value =
            if value == nil do
              path = sm_file_path_from_ctx(ctx, fid)
              case NIF.v2_pread_at(path, off) do
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
      |> Enum.sort_by(fn {field, _} -> field end)
    rescue
      ArgumentError -> []
    end
  end

  defp cross_shard_prefix_count(keydir, prefix) do
    prefix_len = byte_size(prefix)
    now = System.os_time(:millisecond)
    ms = [{{:"$1", :_, :"$2", :_, :_, :_, :_},
           [{:andalso, {:is_binary, :"$1"},
             {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
           [:"$2"]}]
    try do
      :ets.select(keydir, ms)
      |> Enum.count(fn exp -> exp == 0 or exp > now end)
    rescue
      ArgumentError -> 0
    end
  end

  defp cross_shard_delete_prefix(ctx, prefix, delete_fn) do
    prefix_len = byte_size(prefix)
    ms = [{{:"$1", :_, :_, :_, :_, :_, :_},
           [{:andalso, {:is_binary, :"$1"},
             {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
           [:"$1"]}]
    try do
      keys = :ets.select(ctx.keydir, ms)
      Enum.each(keys, fn key -> delete_fn.(key) end)
    rescue
      ArgumentError -> :ok
    end
    :ok
  end

  defp sm_file_path_from_ctx(ctx, file_id) do
    Path.join(ctx.shard_data_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  # ---------------------------------------------------------------------------
  # Private: command execution
  # ---------------------------------------------------------------------------

  defp apply_single(state, {:put, key, value, expire_at_ms}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put(state, key, value, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:delete, key}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_delete(state, key)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:list_op, key, operation}) do
    do_list_op(state, key, operation)
  end

  defp apply_single(state, {:compound_put, compound_key, value, expire_at_ms}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put(state, compound_key, value, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:compound_delete, compound_key}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_delete(state, compound_key)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:compound_delete_prefix, prefix}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_delete_prefix(state, prefix)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:incr, key, delta}) do
    do_incr(state, key, delta)
  end

  defp apply_single(state, {:incr_float, key, delta}) do
    do_incr_float(state, key, delta)
  end

  defp apply_single(state, {:append, key, suffix}) do
    do_append(state, key, suffix)
  end

  defp apply_single(state, {:getset, key, new_value}) do
    do_getset(state, key, new_value)
  end

  defp apply_single(state, {:getdel, key}) do
    do_getdel(state, key)
  end

  defp apply_single(state, {:getex, key, expire_at_ms}) do
    do_getex(state, key, expire_at_ms)
  end

  defp apply_single(state, {:setrange, key, offset, value}) do
    do_setrange(state, key, offset, value)
  end

  defp apply_single(state, {:cas, key, expected, new_value, ttl_ms}) do
    do_cas(state, key, expected, new_value, ttl_ms)
  end

  defp apply_single(state, {:lock, key, owner, ttl_ms}) do
    do_lock(state, key, owner, ttl_ms)
  end

  defp apply_single(state, {:unlock, key, owner}) do
    do_unlock(state, key, owner)
  end

  defp apply_single(state, {:extend, key, owner, ttl_ms}) do
    do_extend(state, key, owner, ttl_ms)
  end

  defp apply_single(state, {:ratelimit_add, key, window_ms, max, count}) do
    do_ratelimit_add(state, key, window_ms, max, count, nil)
  end

  defp apply_single(state, {:ratelimit_add, key, window_ms, max, count, now_ms}) do
    do_ratelimit_add(state, key, window_ms, max, count, now_ms)
  end

  # -- Probabilistic data structure commands in batch/cross_shard_tx --

  defp apply_single(state, {:bloom_create, key, num_bits, num_hashes, prob_meta}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)
      NIF.bloom_file_create(path, num_bits, num_hashes)
      do_put(state, key, :erlang.term_to_binary(prob_meta), 0)
      :ok
    end)
  end

  defp apply_single(state, {:bloom_add, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)
      auto_create_bloom_if_needed(state, path, key, auto_create_params)
      NIF.bloom_file_add(path, element)
    end)
  end

  defp apply_single(state, {:bloom_madd, key, elements, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)
      auto_create_bloom_if_needed(state, path, key, auto_create_params)
      NIF.bloom_file_madd(path, elements)
    end)
  end

  defp apply_single(state, {:cms_create, key, width, depth}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cms")
      ensure_prob_dir(state)
      NIF.cms_file_create(path, width, depth)
      meta_val = {:cms_meta, %{width: width, depth: depth}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end)
  end

  defp apply_single(state, {:cms_incrby, key, items}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cms")
      NIF.cms_file_incrby(path, items)
    end)
  end

  defp apply_single(state, {:cms_merge, dst_key, src_paths, weights, create_params}) do
    do_prob_command(state, fn ->
      dst_path = prob_path(state, dst_key, "cms")
      ensure_prob_dir(state)
      unless File.exists?(dst_path) do
        %{width: w, depth: d} = create_params
        NIF.cms_file_create(dst_path, w, d)
        meta_val = {:cms_meta, %{width: w, depth: d}}
        do_put(state, dst_key, :erlang.term_to_binary(meta_val), 0)
      end
      NIF.cms_file_merge(dst_path, src_paths, weights)
    end)
  end

  defp apply_single(state, {:cuckoo_create, key, capacity, bucket_size}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)
      NIF.cuckoo_file_create(path, capacity, bucket_size)
      meta_val = {:cuckoo_meta, %{capacity: capacity}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end)
  end

  defp apply_single(state, {:cuckoo_add, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)
      auto_create_cuckoo_if_needed(state, path, key, auto_create_params)
      NIF.cuckoo_file_add(path, element)
    end)
  end

  defp apply_single(state, {:cuckoo_addnx, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)
      auto_create_cuckoo_if_needed(state, path, key, auto_create_params)
      NIF.cuckoo_file_addnx(path, element)
    end)
  end

  defp apply_single(state, {:cuckoo_del, key, element}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      NIF.cuckoo_file_del(path, element)
    end)
  end

  defp apply_single(state, {:topk_create, key, k, width, depth, decay}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      ensure_prob_dir(state)
      NIF.topk_file_create_v2(path, k, width, depth, decay)
      meta_val = {:topk_meta, %{path: path, k: k, width: width, depth: depth, decay: decay}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end)
  end

  defp apply_single(state, {:topk_add, key, elements}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_add_v2(path, elements)
    end)
  end

  defp apply_single(state, {:topk_incrby, key, pairs}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_incrby_v2(path, pairs)
    end)
  end

  # Wraps a block of state machine operations with batched disk writes.
  # Initializes the pending-writes buffer, runs the block, then flushes
  # all accumulated writes in a single v2_append_batch_nosync NIF call.
  # Guarantees: no :pending entries in ETS after this returns.
  defp with_pending_writes(state, fun) do
    Process.put(:sm_pending_writes, [])
    result = fun.()
    flush_pending_writes(state)
    result
  end

  defp do_put(state, key, value, expire_at_ms) do
    ets_val = value_for_ets(value, hot_cache_threshold(state))
    disk_val = to_disk_binary(value)

    # Insert into ETS immediately so subsequent read-modify-write commands
    # (INCR, APPEND, etc.) in the same batch see the correct value.
    # The file_id is :pending — flush_pending_writes will update it with
    # the real offset after the batch NIF call.
    :ets.insert(
      state.ets,
      {key, ets_val, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
    )

    # Accumulate for batch disk write — flushed by flush_pending_writes
    # at the end of apply/3 before returning to ra.
    pending = Process.get(:sm_pending_writes, [])
    Process.put(:sm_pending_writes, [{key, disk_val, expire_at_ms} | pending])

    :ok
  end

  # Flushes all accumulated disk writes in a single NIF call, then updates
  # ETS entries with real file_id/offset. Called at the end of every apply/3
  # — no :pending entries remain after this returns.
  defp flush_pending_writes(state) do
    case Process.put(:sm_pending_writes, []) do
      [] ->
        :ok

      pending when is_list(pending) ->
        # Reverse to preserve insertion order (we prepend during accumulation)
        batch = Enum.reverse(pending)

        case NIF.v2_append_batch_nosync(state.active_file_path, batch) do
          {:ok, locations} ->
            if state.instance_ctx do
              Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.shard_index)
            else
              Ferricstore.Store.DiskPressure.clear(state.shard_index)
            end

            Enum.zip(batch, locations)
            |> Enum.each(fn {{key, _val, _exp}, {offset, value_size}} ->
              # Update ETS: replace :pending with real file_id and offset.
              # Use update_element to avoid overwriting the value (a concurrent
              # read-modify-write in the same batch may have changed it).
              try do
                :ets.update_element(state.ets, key, [
                  {5, state.active_file_id},
                  {6, offset},
                  {7, value_size}
                ])
              rescue
                ArgumentError -> :ok
              end
            end)

          {:error, reason} ->
            if state.instance_ctx do
              Ferricstore.Store.DiskPressure.set(state.instance_ctx, state.shard_index)
            else
              Ferricstore.Store.DiskPressure.set(state.shard_index)
            end
            require Logger
            Logger.error("StateMachine flush_pending_writes failed: #{inspect(reason)}")
        end

      _ ->
        :ok
    end
  end

  defp do_delete(state, key) do
    # If the key has a pending background write, flush the BitcaskWriter
    # first to ensure the PUT record lands on disk BEFORE the tombstone.
    # Without this, a background PUT arriving after the tombstone would
    # resurrect the key on recovery (Bitcask last-record-wins semantics).
    flush_pending_for_key(state, key)

    # If this key holds prob metadata, delete the associated prob file.
    # Must happen before the ETS entry is removed so we can read the value.
    maybe_delete_prob_file(state, key)

    # v2: append a tombstone record to the active log file + fsync.
    case NIF.v2_append_tombstone(state.active_file_path, key) do
      {:ok, _} ->
        :ets.delete(state.ets, key)
        :ok

      {:error, reason} ->
        # Do NOT delete from ETS if the tombstone write failed —
        # the key would resurrect on restart.
        {:error, reason}
    end
  end

  # Flushes the BitcaskWriter if the key has a pending background write.
  # Called before tombstone writes and delete_prefix operations to ensure
  # correct disk ordering (PUT before TOMBSTONE).
  defp flush_pending_for_key(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, _v, _e, _lfu, :pending, _off, _vs}] ->
        try do
          BitcaskWriter.flush(state.shard_index)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end

      _ ->
        :ok
    end
  end

  # Returns nil for values exceeding the hot cache max value size threshold,
  # or the value itself if it fits. Prevents large values from being stored
  # in ETS, avoiding expensive binary copies on every :ets.lookup.
  @compile {:inline, value_for_ets: 2}
  defp value_for_ets(nil, _threshold), do: nil
  defp value_for_ets(value, _threshold) when is_integer(value), do: Integer.to_string(value)
  defp value_for_ets(value, _threshold) when is_float(value), do: Float.to_string(value)
  defp value_for_ets(value, threshold) when is_binary(value) do
    if byte_size(value) > threshold do
      nil
    else
      value
    end
  end

  @compile {:inline, hot_cache_threshold: 1}
  defp hot_cache_threshold(%{instance_ctx: ctx}) when ctx != nil, do: ctx.hot_cache_max_value_size
  defp hot_cache_threshold(_state), do: 65_536

  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v

  # ---------------------------------------------------------------------------
  # Private: string mutation operations
  # ---------------------------------------------------------------------------

  # Atomic INCR/DECR/INCRBY/DECRBY: reads current value, parses as integer,
  # adds delta, writes back. Preserves existing expire_at_ms.
  # Returns {:ok, new_integer} or {:error, reason}.
  defp do_incr(state, key, delta) do
    case do_get_meta(state, key) do
      nil ->
        do_put(state, key, delta, 0)
        {:ok, delta}

      {value, expire_at_ms} ->
        case coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            do_put(state, key, new_val, expire_at_ms)
            {:ok, new_val}

          :error ->
            {:error, "ERR value is not an integer or out of range"}
        end
    end
  end

  # Parses a binary as an integer. Returns `{:ok, integer}` or `:error`.
  defp parse_integer(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} -> {:ok, val}
      _ -> :error
    end
  end

  # Coerces a value (integer, float, or binary) to integer.
  defp coerce_integer(v) when is_integer(v), do: {:ok, v}
  defp coerce_integer(v) when is_float(v), do: :error
  defp coerce_integer(v) when is_binary(v), do: parse_integer(v)

  # Coerces a value (integer, float, or binary) to float.
  defp coerce_float(v) when is_float(v), do: {:ok, v}
  defp coerce_float(v) when is_integer(v), do: {:ok, v * 1.0}
  defp coerce_float(v) when is_binary(v), do: parse_float(v)

  # Atomic INCRBYFLOAT: reads current value, parses as float, adds delta,
  # formats result, writes back. Preserves existing expire_at_ms.
  defp do_incr_float(state, key, delta) do
    case do_get_meta(state, key) do
      nil ->
        new_val = delta * 1.0
        do_put(state, key, new_val, 0)
        {:ok, new_val}

      {value, expire_at_ms} ->
        case coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            do_put(state, key, new_val, expire_at_ms)
            {:ok, new_val}

          :error ->
            {:error, "ERR value is not a valid float"}
        end
    end
  end

  # Delegates to the shared ValueCodec to avoid duplication with shard.ex.
  defp parse_float(str), do: ValueCodec.parse_float(str)

  # Atomic APPEND: reads current value (or ""), concatenates suffix, writes
  # back. Preserves the existing expire_at_ms on the key.
  defp do_append(state, key, suffix) do
    {old_val, expire_at_ms} =
      case do_get_meta(state, key) do
        nil -> {"", 0}
        {v, exp} -> {to_disk_binary(v), exp}
      end

    new_val = old_val <> suffix
    do_put(state, key, new_val, expire_at_ms)
    {:ok, byte_size(new_val)}
  end

  # Atomic GETSET: reads old value, writes new value with no expiry, returns
  # old value directly (not wrapped in {:ok, ...}).
  defp do_getset(state, key, new_value) do
    old = do_get(state, key)
    do_put(state, key, new_value, 0)
    old
  end

  # Atomic GETDEL: reads value, deletes key, returns value directly (not
  # wrapped in {:ok, ...}). Returns nil if key does not exist.
  defp do_getdel(state, key) do
    old = do_get(state, key)

    if old != nil do
      do_delete(state, key)
    end

    old
  end

  # Atomic GETEX: reads value, re-writes with new expire_at_ms, returns value
  # directly (not wrapped). Returns nil if key does not exist or is expired.
  defp do_getex(state, key, expire_at_ms) do
    case do_get_meta(state, key) do
      nil ->
        nil

      {value, _old_exp} ->
        do_put(state, key, value, expire_at_ms)
        value
    end
  end

  # Atomic SETRANGE: reads current value, pads with zero bytes if needed,
  # replaces bytes at offset, writes back. Preserves expire_at_ms.
  defp do_setrange(state, key, offset, value) do
    {old_val, expire_at_ms} =
      case do_get_meta(state, key) do
        nil -> {"", 0}
        {v, exp} -> {to_disk_binary(v), exp}
      end

    new_val = sm_apply_setrange(old_val, offset, value)
    do_put(state, key, new_val, expire_at_ms)
    {:ok, byte_size(new_val)}
  end

  # Overwrites bytes at `offset` with `value`, zero-padding if the original
  # string is shorter than offset. Mirrors shard.ex apply_setrange/3.
  defp sm_apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        if offset > old_len do
          old <> :binary.copy(<<0>>, offset - old_len)
        else
          old
        end

      offset >= old_len ->
        padding = :binary.copy(<<0>>, offset - old_len)
        old <> padding <> value

      offset + val_len >= old_len ->
        binary_part(old, 0, offset) <> value

      true ->
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: compare-and-swap
  # ---------------------------------------------------------------------------

  # Reads the current value from ETS (with Bitcask fallback), compares it
  # against `expected`. If match, writes `new_value` with optional TTL.
  # Returns 1 (swapped), 0 (mismatch), or nil (missing/expired).
  #
  # Replicates the exact shard.ex handle_cas_direct logic.
  # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
  # timestamp before entering Raft to keep the state machine deterministic
  # (no System.os_time calls). So the 5th arg is already absolute, not relative.
  defp do_cas(state, key, expected, new_value, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^expected, old_exp} ->
        expire = if expire_at_ms, do: expire_at_ms, else: old_exp
        do_put(state, key, new_value, expire)
        1

      {:hit, _other, _exp} ->
        0

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # Private: distributed lock operations
  # ---------------------------------------------------------------------------

  # Acquires a lock. If the key doesn't exist, is expired, or is already held
  # by the same owner, sets {owner, ttl}. Returns :ok or {:error, reason}.
  #
  # Replicates the exact shard.ex handle_lock_direct logic.
  # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
  # timestamp before entering Raft to keep the state machine deterministic.
  defp do_lock(state, key, owner, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        # Same owner -- re-acquire (idempotent)
        do_put(state, key, owner, expire_at_ms)
        :ok

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK lock is held by another owner"}

      _ ->
        # Missing or expired -- acquire
        do_put(state, key, owner, expire_at_ms)
        :ok
    end
  end

  # Releases a lock. If the key exists and the owner matches, deletes the key.
  # Returns 1 on success, {:error, reason} on owner mismatch.
  #
  # Replicates the exact shard.ex handle_unlock_direct logic.
  defp do_unlock(state, key, owner) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        do_delete(state, key)
        1

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK caller is not the lock owner"}

      _ ->
        # Missing or expired -- treat as already unlocked
        1
    end
  end

  # Extends a lock's TTL. If the key exists and the owner matches, updates
  # the TTL. Returns 1 on success, {:error, reason} on mismatch or missing.
  #
  # Replicates the exact shard.ex handle_extend_direct logic.
  # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
  # timestamp before entering Raft to keep the state machine deterministic.
  defp do_extend(state, key, owner, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        do_put(state, key, owner, expire_at_ms)
        1

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK caller is not the lock owner"}

      _ ->
        {:error, "DISTLOCK lock does not exist or has expired"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: cross-shard key locking (mini-percolator)
  # ---------------------------------------------------------------------------

  # Lock map is stored in a process dictionary key per shard. This avoids
  # adding a field to the Raft state struct (which would require migration).
  # The process dictionary persists across apply/3 calls because ra runs the
  # state machine in a dedicated process.

  # Locks all keys atomically. If any key is already locked by a different
  # owner (and not expired), rejects the entire batch.
  # Returns {new_state, result} — locks are persisted in Raft state.
  defp do_lock_keys(state, keys, owner_ref, expire_at_ms) do
    locks = Map.get(state, :cross_shard_locks, %{})
    now = System.os_time(:millisecond)

    conflict =
      Enum.find(keys, fn key ->
        case Map.get(locks, key) do
          nil -> false
          {^owner_ref, _exp} -> false
          {_other, exp} -> exp > now
        end
      end)

    if conflict do
      {state, {:error, :keys_locked}}
    else
      # Prune expired locks to prevent unbounded memory growth
      pruned = Map.reject(locks, fn {_k, {_ref, exp}} -> exp <= now end)

      new_locks =
        Enum.reduce(keys, pruned, fn key, acc ->
          Map.put(acc, key, {owner_ref, expire_at_ms})
        end)

      {%{state | cross_shard_locks: new_locks}, :ok}
    end
  end

  # Unlocks keys owned by the given owner_ref.
  # Returns {new_state, :ok}.
  defp do_unlock_keys(state, keys, owner_ref) do
    locks = Map.get(state, :cross_shard_locks, %{})

    new_locks =
      Enum.reduce(keys, locks, fn key, acc ->
        case Map.get(acc, key) do
          {^owner_ref, _exp} -> Map.delete(acc, key)
          _ -> acc
        end
      end)

    {%{state | cross_shard_locks: new_locks}, :ok}
  end

  # Checks whether a key is locked by someone other than owner_ref.
  defp check_key_lock(state, key, owner_ref) do
    locks = Map.get(state, :cross_shard_locks, %{})
    now = System.os_time(:millisecond)

    case Map.get(locks, key) do
      nil -> :ok
      {^owner_ref, _exp} -> :ok
      {_other, exp} when exp <= now -> :ok
      {_other, _exp} -> {:error, :key_locked}
    end
  end

  # Writes an intent record. Returns {new_state, :ok}.
  defp do_write_intent(state, owner_ref, intent_map) do
    intents = Map.get(state, :cross_shard_intents, %{})
    {%{state | cross_shard_intents: Map.put(intents, owner_ref, intent_map)}, :ok}
  end

  # Deletes an intent record. Returns {new_state, :ok}.
  defp do_delete_intent(state, owner_ref) do
    intents = Map.get(state, :cross_shard_intents, %{})
    {%{state | cross_shard_intents: Map.delete(intents, owner_ref)}, :ok}
  end

  # Returns all intent records.
  defp do_get_intents(state) do
    Map.get(state, :cross_shard_intents, %{})
  end

  # ---------------------------------------------------------------------------
  # Private: sliding window rate limiter
  # ---------------------------------------------------------------------------

  # Implements a sliding window rate limiter. Reads current counters from ETS,
  # rotates windows as needed, computes the effective count using a weighted
  # sliding window approximation, and updates the stored state.
  # Returns [status, count, remaining, ms_until_reset].
  #
  # Replicates the exact shard.ex handle_ratelimit_add_direct logic.
  defp do_ratelimit_add(state, key, window_ms, max, count, precomputed_now_ms) do
    now = precomputed_now_ms || System.os_time(:millisecond)

    {cur_count, cur_start, prv_count} =
      case ets_lookup(state, key) do
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

    {status, final_count, remaining, value} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        {"denied", effective, max(0, max - effective), value}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        {"allowed", new_eff, max(0, max - new_eff), value}
      end

    do_put(state, key, value, expire_at_ms)
    ms_until_reset = max(0, cur_start + window_ms - now)
    [status, final_count, remaining, ms_until_reset]
  end

  # Delegates to the shared ValueCodec to avoid duplication with shard.ex.
  defp encode_ratelimit(cur, start, prev), do: ValueCodec.encode_ratelimit(cur, start, prev)
  defp decode_ratelimit(value), do: ValueCodec.decode_ratelimit(value)

  # ---------------------------------------------------------------------------
  # Private: ETS lookup with expiry checking
  # ---------------------------------------------------------------------------

  # Reads a key from ETS, checking expiry. Falls back to Bitcask for cold
  # keys. Returns {:hit, value, expire_at_ms}, :expired, or :miss.
  # Mirrors the shard's `ets_lookup/2` logic with Bitcask fallback for
  # keys that may not yet be warmed into ETS.
  defp ets_lookup(state, key) do
    now = System.os_time(:millisecond)

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {:hit, value, 0}

      [{^key, nil, 0, _lfu, _fid, _off, _vsize}] ->
        # Cold key -- try Bitcask
        warm_from_bitcask(state, key)

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, _fid, _off, _vsize}] when exp > now ->
        # Cold key with valid TTL -- try Bitcask
        warm_from_bitcask_with_exp(state, key, exp)

      [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
        :ets.delete(state.ets, key)
        :expired

      [] ->
        # ETS miss -- try Bitcask for keys not yet in keydir
        warm_from_bitcask(state, key)
    end
  end

  # v2: warms a cold key from disk using the location stored in the ETS
  # 7-tuple. If the key has a cold entry (value=nil, fid/off known), reads
  # the value via pread_at and updates ETS. For truly missing keys (not in
  # ETS at all after recover_keydir), returns :miss.
  defp warm_from_bitcask(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, nil, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) and fid >= 0 ->
        warm_from_disk(state, key, 0, fid, off)

      _ ->
        # :pending fid or truly missing -- cannot warm from disk.
        :miss
    end
  end

  defp warm_from_bitcask_with_exp(state, key, exp) do
    case :ets.lookup(state.ets, key) do
      [{^key, nil, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) and fid >= 0 ->
        warm_from_disk(state, key, exp, fid, off)

      _ ->
        # :pending fid or truly missing -- cannot warm from disk.
        :miss
    end
  end

  # Reads a value from disk at the given file_id + offset, warms ETS, and
  # returns {:hit, value, expire_at_ms}.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  defp warm_from_disk(state, key, expire_at_ms, fid, off) do
    path = sm_file_path(state, fid)

    case NIF.v2_pread_at(path, off) do
      {:ok, value} when is_binary(value) ->
        v = value_for_ets(value, hot_cache_threshold(state))
        :ets.insert(state.ets, {key, v, expire_at_ms, LFU.initial(), fid, off, byte_size(value)})
        {:hit, value, expire_at_ms}

      _ ->
        :miss
    end
  end

  # Returns the full file path for a log file within this shard's data dir.
  defp sm_file_path(state, file_id) do
    Path.join(state.shard_data_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  # ---------------------------------------------------------------------------
  # Private: list operations (read-modify-write via ListOps)
  # ---------------------------------------------------------------------------

  # Performs a complete read-modify-write for a list operation within a single
  # Raft apply. The get/put/delete closures operate directly on ETS and Bitcask
  # (the same stores available to the state machine) so the entire operation is
  # atomic from the Raft log's perspective.
  defp do_list_op(state, key, operation) do
    get_fn = fn -> do_get(state, key) end

    put_fn = fn encoded_binary ->
      do_put(state, key, encoded_binary, 0)
    end

    delete_fn = fn ->
      do_delete(state, key)
    end

    ListOps.execute(get_fn, put_fn, delete_fn, operation)
  end

  # ---------------------------------------------------------------------------
  # Private: compound delete prefix (scan + batch delete)
  # ---------------------------------------------------------------------------

  # Scans ETS for all keys matching the given prefix and deletes each from
  # both ETS and Bitcask. Used by DEL on hashes, sets, and sorted sets to
  # remove all compound fields belonging to a data structure.
  #
  # Uses :ets.select with a match spec for O(matching) prefix lookup instead
  # of :ets.foldl which would scan every key in the entire keydir.
  defp do_delete_prefix(state, prefix) do
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [{:andalso, {:is_binary, :"$1"},
         {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}],
       [:"$1"]}
    ]

    keys_to_delete = :ets.select(state.ets, match_spec)

    Enum.each(keys_to_delete, fn key ->
      do_delete(state, key)
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private: read from ETS with Bitcask fallback
  # ---------------------------------------------------------------------------

  # Reads a value from ETS, falling back to Bitcask for cold keys. Mirrors
  # the shard's `do_get/2` logic so that list operations can read current
  # state within the state machine.
  defp do_get(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss -> nil
    end
  end

  # Reads a value + expire_at_ms from ETS, falling back to Bitcask for cold
  # keys. Returns `{value, expire_at_ms}` or `nil`.
  defp do_get_meta(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, exp} -> {value, exp}
      :expired -> nil
      :miss -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Private: HLC merging (spec 2G.6)
  # ---------------------------------------------------------------------------

  # Merges a remote HLC timestamp into the local node's HLC. This is a
  # side-effect that does not affect the deterministic state machine output.
  #
  # The merge is wrapped in a try/catch because the HLC GenServer may not be
  # running in unit tests that exercise the state machine in isolation.
  @spec merge_hlc(HLC.timestamp()) :: :ok
  defp merge_hlc(remote_ts) do
    HLC.update(remote_ts)
  rescue
    # HLC GenServer not running (e.g. unit tests without full app)
    _error -> :ok
  catch
    :exit, _reason -> :ok
  end

  # ---------------------------------------------------------------------------
  # Private: probabilistic data structure helpers
  # ---------------------------------------------------------------------------

  # Shorthand for the common prob command pattern: bump applied count +
  # maybe release cursor.
  defp bump_applied(meta, state, result) do
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  # Prob commands don't write to Bitcask log (they write to their own files),
  # so they use with_pending_writes to ensure any metadata puts are batched.
  defp do_prob_command(state, fun) do
    with_pending_writes(state, fun)
  end

  # Returns the file path for a probabilistic data structure file.
  # Uses Base64 URL-safe encoding to handle arbitrary key bytes.
  defp prob_path(state, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    prob_dir = prob_dir(state)
    Path.join(prob_dir, "#{safe}.#{ext}")
  end

  # Returns the prob directory for this shard.
  defp prob_dir(%{shard_data_path: shard_data_path}) do
    Path.join(shard_data_path, "prob")
  end

  # Ensures the prob directory exists.
  defp ensure_prob_dir(state) do
    dir = prob_dir(state)

    unless File.exists?(dir) do
      File.mkdir_p!(dir)
    end
  end

  # Auto-creates a bloom filter file if it doesn't exist.
  defp auto_create_bloom_if_needed(state, path, key, auto_create_params) do
    unless File.exists?(path) do
      if auto_create_params do
        %{num_bits: nb, num_hashes: nh} = auto_create_params
        NIF.bloom_file_create(path, nb, nh)
        meta_val = {:bloom_meta, Map.merge(auto_create_params, %{path: path})}
        do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      end
    end
  end

  # Applies a prob command locally (used in cross-shard tx context where
  # the state machine is already running inside Raft apply).
  defp apply_prob_locally(instance_ctx, command) do
    # In cross-shard tx, prob commands go through Router.prob_write
    # which routes to the correct shard's Raft group.
    Router.prob_write(instance_ctx, command)
  end

  # Auto-creates a cuckoo filter file if it doesn't exist.
  defp auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
    unless File.exists?(path) do
      if auto_create_params do
        %{capacity: cap, bucket_size: bs} = auto_create_params
        NIF.cuckoo_file_create(path, cap, bs)
        meta_val = {:cuckoo_meta, %{capacity: cap}}
        do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      end
    end
  end

  # Enhanced do_delete that cleans up prob files.
  # When a key's value is a prob metadata marker, delete the associated file.
  defp maybe_delete_prob_file(state, key) do
    case do_get(state, key) do
      nil ->
        :ok

      value when is_binary(value) ->
        try do
          case :erlang.binary_to_term(value) do
            {:bloom_meta, %{path: path}} -> File.rm(path)
            {:cms_meta, _} -> File.rm(prob_path(state, key, "cms"))
            {:cuckoo_meta, _} -> File.rm(prob_path(state, key, "cuckoo"))
            {:topk_meta, %{path: path}} -> File.rm(path)
            _ -> :ok
          end
        rescue
          _ -> :ok
        end

      _ ->
        :ok
    end
  end
end
