defmodule Ferricstore.Store.Router do
  @moduledoc """
  Routes keys to shard GenServers using consistent hashing via `:erlang.phash2/2`.

  This is a pure module with no process state. It provides two categories of
  functions:

  1. **Routing helpers** -- `shard_for/2` and `shard_name/2` map a key to its
     owning shard index and registered process name respectively. Supports
     Redis hash tags: keys containing `{tag}` are hashed on the tag content,
     allowing related keys to co-locate on the same shard.

  2. **Convenience accessors** -- `get/2`, `put/4`, `delete/2`, `exists?/2`,
     `keys/1`, and `dbsize/1` dispatch to the correct shard GenServer
     transparently.

  All public functions take a `ctx` (`FerricStore.Instance.t()`) as the first
  argument, replacing all persistent_term lookups with instance-local state.
  """

  alias Ferricstore.Stats
  alias Ferricstore.Store.LFU

  import Bitwise, only: [band: 2]

  @slot_mask 1023

  # ---------------------------------------------------------------------------
  # Shard resolution helpers
  # ---------------------------------------------------------------------------

  @doc false
  def resolve_shard(ctx, idx), do: elem(ctx.shard_names, idx)
  @doc false
  def resolve_keydir(ctx, idx), do: elem(ctx.keydir_refs, idx)
  @doc false
  def effective_shard_count(ctx), do: ctx.shard_count

  # ---------------------------------------------------------------------------
  # Write-path dispatch: quorum writes bypass Shard, async writes use Shard
  # ---------------------------------------------------------------------------

  # Submits a write command directly to ra via `pipeline_command/4`, bypassing
  # the Batcher GenServer entirely. The ra WAL's internal gen_batch_server
  # already batches all commands between fdatasync calls, making the Batcher's
  # 1ms accumulation window redundant and harmful (it serialises 50 writers
  # through one GenServer and adds ~1ms latency per write).
  #
  # `pipeline_command` is non-blocking (cast). It returns `:ok` immediately
  # and sends a `{:ra_event, Leader, {:applied, [{Corr, Result}]}}` message
  # to the calling process when the command commits. Since Router functions
  # execute in the **caller's** process (Task, Connection, etc.), the ra_event
  # lands in the caller's mailbox -- not a GenServer's -- so the selective
  # receive below is safe.
  #
  # After a non-error result, increments the shared write version counter
  # so that WATCH/EXEC can detect the mutation. False positives (incrementing
  # when no state actually changed) are safe -- they only cause a spurious
  # WATCH failure, which the client retries.
  @spec quorum_write(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  defp quorum_write(ctx, idx, command) do
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(idx)
    corr = make_ref()

    result =
      try do
        case :ra.pipeline_command(shard_id, command, corr, :normal) do
          :ok ->
            wait_for_ra_applied(ctx, corr, shard_id, idx, command)

          {:error, :noproc} ->
            # Ra server not alive (shard restarting). Fall back to Shard
            # GenServer which re-initialises ra during its init.
            GenServer.call(elem(ctx.shard_names, idx), command)

          {:error, _reason} ->
            GenServer.call(elem(ctx.shard_names, idx), command)
        end
      catch
        :exit, {:noproc, _} ->
          GenServer.call(elem(ctx.shard_names, idx), command)
      end

    case result do
      {:error, _} -> :ok
      _ ->
        size = :counters.info(ctx.write_version).size
        if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
    end

    result
  end

  # Waits for the `ra_event` containing our correlation ref. The `applied`
  # list may contain results for OTHER concurrent commands (from the same
  # process submitting multiple pipeline_commands). We loop until we find
  # our `corr`. Unrelated ra_events are silently skipped -- their results
  # belong to other concurrent calls in the same process, each of which
  # has its own selective receive waiting for its own correlation ref.
  @spec wait_for_ra_applied(FerricStore.Instance.t(), reference(), :ra.server_id(), non_neg_integer(), tuple()) :: term()
  defp wait_for_ra_applied(ctx, corr, shard_id, idx, command) do
    receive do
      {:ra_event, _leader, {:applied, applied_list}} ->
        case List.keyfind(applied_list, corr, 0) do
          {^corr, result} ->
            result

          nil ->
            # Our command wasn't in this batch -- keep waiting.
            wait_for_ra_applied(ctx, corr, shard_id, idx, command)
        end

      {:ra_event, _from, {:rejected, {:not_leader, maybe_leader, ^corr}}} ->
        # Leader changed. Retry once with the suggested leader, or fall
        # back to the Shard GenServer if no leader hint is available.
        leader =
          if maybe_leader not in [nil, :undefined],
            do: maybe_leader,
            else: shard_id

        retry_corr = make_ref()

        case :ra.pipeline_command(leader, command, retry_corr, :normal) do
          :ok ->
            wait_for_ra_applied(ctx, retry_corr, leader, idx, command)

          _err ->
            GenServer.call(elem(ctx.shard_names, idx), command)
        end

      {:ra_event, _from, {:rejected, {_reason, _hint, ^corr}}} ->
        # Other rejection (e.g. cluster change). Fall back to Shard.
        GenServer.call(elem(ctx.shard_names, idx), command)
    after
      10_000 ->
        {:error, "ERR write timeout"}
    end
  end

  # Determines the durability mode for a key by extracting its namespace
  # prefix and looking up the namespace config. Returns `:quorum` or `:async`.
  #
  # Three-state fast path via ctx.durability_mode (~0ns struct field access):
  #   :all_quorum — no async namespaces configured, return :quorum immediately
  #   :all_async  — all namespaces are async, return :async immediately
  #   :mixed      — some quorum, some async, must split key and lookup
  #
  # The flag is set by NamespaceConfig whenever durability settings change.
  @spec durability_for_key(FerricStore.Instance.t(), binary()) :: :quorum | :async
  defp durability_for_key(ctx, key) do
    case ctx.durability_mode do
      :all_quorum -> :quorum
      :all_async -> :async
      :mixed ->
        prefix =
          case :binary.split(key, ":") do
            [^key] -> "_root"
            [p | _] -> p
          end

        Ferricstore.NamespaceConfig.durability_for(prefix)
    end
  end

  # Dispatches writes based on namespace durability mode.
  #
  # Quorum: submit to Raft, wait for quorum apply. Strongest guarantee.
  # Async:  write ETS immediately, submit to Raft non-blocking (fire-and-forget).
  #         Like Redis Cluster — client sees the write before replication completes.
  #         Leader crash before replication = data loss (documented trade-off).
  @spec raft_write(FerricStore.Instance.t(), non_neg_integer(), binary(), tuple()) :: term()
  defp raft_write(ctx, idx, key, command) do
    if ctx.raft_enabled do
      case durability_for_key(ctx, key) do
        :quorum -> quorum_write(ctx, idx, command)
        :async -> async_write(ctx, idx, command)
      end
    else
      # No Raft — direct GenServer.call to the shard
      GenServer.call(elem(ctx.shard_names, idx), command)
    end
  end

  # Async write path (like Redis Cluster — async replication):
  # 1. Execute locally: direct ETS write + BitcaskWriter (no GenServer)
  # 2. Submit to Raft fire-and-forget (replication to followers)
  #
  # All writes bypass the Shard GenServer entirely — ETS is :public with
  # write_concurrency so any process can write. BitcaskWriter is a cast.
  # This eliminates the GenServer serialization bottleneck.
  #
  # For read-modify-write (INCR etc.), concurrent same-key mutations may
  # race (last writer wins). This matches the async durability contract —
  # users choosing async accept eventual consistency.

  defp async_write(ctx, idx, {:put, key, value, expire_at_ms}) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = if idx < size, do: :atomics.get(ctx.disk_pressure, idx + 1) == 1, else: false

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      async_write_put(ctx, idx, key, value, expire_at_ms)
    end
  end

  defp async_write_put(ctx, idx, key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)
    value_for_ets = case value do
      v when is_integer(v) -> Integer.to_string(v)
      v when is_float(v) -> Float.to_string(v)
      v when is_binary(v) ->
        if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
    end
    disk_value = to_disk_binary(value)
    {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)

    if value_for_ets == nil do
      # Large value: sync NIF write to get offset, then ETS with real location.
      # Cannot use async BitcaskWriter because ETS value is nil (too large for
      # hot cache) and readers would see nil until the async write completes.
      case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(file_path, [{key, disk_value, expire_at_ms}]) do
        {:ok, [{offset, _record_size}]} ->
          :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)})
          size = :counters.info(ctx.write_version).size
          if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
          async_submit_to_raft(idx, {:put, key, value, expire_at_ms})
          :ok

        {:error, reason} ->
          {:error, "ERR disk write failed: #{inspect(reason)}"}
      end
    else
      # Small value: inline in ETS for instant reads, async Bitcask write.
      :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})
      Ferricstore.Store.BitcaskWriter.write(idx, file_path, file_id, keydir, key, disk_value, expire_at_ms)
      size = :counters.info(ctx.write_version).size
      if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
      async_submit_to_raft(idx, {:put, key, value, expire_at_ms})
      :ok
    end
  end

  defp async_write(ctx, idx, {:delete, key}) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = if idx < size, do: :atomics.get(ctx.disk_pressure, idx + 1) == 1, else: false

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      keydir = elem(ctx.keydir_refs, idx)
      :ets.delete(keydir, key)

      {_, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)
      Ferricstore.Store.BitcaskWriter.delete(idx, file_path, key)

      wv_size = :counters.info(ctx.write_version).size
      if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
      async_submit_to_raft(idx, {:delete, key})
      :ok
    end
  end

  defp async_write(ctx, idx, {:incr, key, delta}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    current =
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) -> value
        _ -> nil
      end

    case current do
      nil ->
        async_write(ctx, idx, {:put, key, delta, 0})
        {:ok, delta}

      value when is_integer(value) ->
        new_val = value + delta
        async_write(ctx, idx, {:put, key, new_val, 0})
        {:ok, new_val}

      value when is_binary(value) ->
        case Integer.parse(value) do
          {int_val, ""} ->
            new_val = int_val + delta
            async_write(ctx, idx, {:put, key, new_val, 0})
            {:ok, new_val}

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end

      _other ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp async_write(ctx, idx, {:incr_float, key, delta}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    current =
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) -> value
        _ -> nil
      end

    case current do
      nil ->
        new_val = delta * 1.0
        async_write(ctx, idx, {:put, key, new_val, 0})
        {:ok, new_val}

      value when is_float(value) ->
        new_val = value + delta
        async_write(ctx, idx, {:put, key, new_val, 0})
        {:ok, new_val}

      value when is_integer(value) ->
        new_val = value * 1.0 + delta
        async_write(ctx, idx, {:put, key, new_val, 0})
        {:ok, new_val}

      value when is_binary(value) ->
        case Float.parse(value) do
          {float_val, _} ->
            new_val = float_val + delta
            async_write(ctx, idx, {:put, key, new_val, 0})
            {:ok, new_val}

          :error ->
            case Integer.parse(value) do
              {int_val, ""} ->
                new_val = int_val * 1.0 + delta
                async_write(ctx, idx, {:put, key, new_val, 0})
                {:ok, new_val}

              _ ->
                {:error, "ERR value is not a valid float"}
            end
        end
    end
  end

  defp async_write(ctx, idx, {:append, key, suffix}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    current =
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) -> value
        _ -> nil
      end

    current_str = case current do
      nil -> ""
      v when is_integer(v) -> Integer.to_string(v)
      v when is_float(v) -> Float.to_string(v)
      v when is_binary(v) -> v
    end

    new_value = current_str <> suffix
    async_write(ctx, idx, {:put, key, new_value, 0})
    {:ok, byte_size(new_value)}
  end

  defp async_write(ctx, idx, {:getset, key, new_value}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    old =
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) -> value
        _ -> nil
      end

    async_write(ctx, idx, {:put, key, new_value, 0})
    old
  end

  defp async_write(ctx, idx, {:getdel, key}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    old =
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) -> value
        _ -> nil
      end

    if old, do: async_write(ctx, idx, {:delete, key})
    old
  end

  defp async_write(ctx, idx, {:getex, key, expire_at_ms}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    case :ets.lookup(keydir, key) do
      [{^key, value, exp, _, _, _, _}] when value != nil and (exp == 0 or exp > now) ->
        async_write(ctx, idx, {:put, key, value, expire_at_ms})
        value

      _ ->
        nil
    end
  end

  defp async_write(ctx, idx, {:setrange, key, offset, value}) do
    keydir = elem(ctx.keydir_refs, idx)
    now = System.os_time(:millisecond)

    current =
      case :ets.lookup(keydir, key) do
        [{^key, v, exp, _, _, _, _}] when v != nil and (exp == 0 or exp > now) ->
          to_disk_binary(v)
        _ -> ""
      end

    padded = if byte_size(current) < offset, do: current <> :binary.copy(<<0>>, offset - byte_size(current)), else: current
    new_value = binary_part(padded, 0, offset) <> value <> binary_part(padded, min(offset + byte_size(value), byte_size(padded)), max(0, byte_size(padded) - offset - byte_size(value)))
    async_write(ctx, idx, {:put, key, new_value, 0})
    {:ok, byte_size(new_value)}
  end

  # Commands that are complex or rarely used in async namespaces
  # fall back to quorum (CAS, LOCK, UNLOCK, EXTEND, RATELIMIT, LIST_OP).
  defp async_write(ctx, idx, command) do
    quorum_write(ctx, idx, command)
  end

  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v

  defp async_submit_to_raft(idx, command) do
    Ferricstore.Raft.AsyncApplyWorker.replicate(idx, [command])
  end

  # -------------------------------------------------------------------
  # Routing helpers
  # -------------------------------------------------------------------

  @doc """
  Returns the slot (0-1023) for a key, respecting hash tags.
  """
  @spec slot_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def slot_for(_ctx, key) do
    hash_input = extract_hash_tag(key) || key
    :erlang.phash2(hash_input) |> band(@slot_mask)
  end

  @doc """
  Returns the shard index (0-based) that owns `key`.

  Routes through the 1,024-slot indirection layer:
  `key -> phash2(key) & 0x3FF -> slot -> slot_map[slot] -> shard_index`

  Supports Redis hash tags: if the key contains `{tag}` (non-empty content
  between the first `{` and the next `}`), the tag is used for hashing
  instead of the full key.
  """
  @spec shard_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def shard_for(ctx, key) do
    slot = slot_for(ctx, key)
    elem(ctx.slot_map, slot)
  end

  @doc """
  Extracts the hash tag from a key, following Redis hash tag semantics.

  If the key contains a substring enclosed in `{...}` where the content
  between the first `{` and the next `}` is non-empty, that substring is
  used for hashing instead of the full key. This allows related keys to
  be routed to the same shard.

  ## Examples

      iex> Ferricstore.Store.Router.extract_hash_tag("{user:42}:session")
      "user:42"

      iex> Ferricstore.Store.Router.extract_hash_tag("no_tag")
      nil

      iex> Ferricstore.Store.Router.extract_hash_tag("{}empty")
      nil

  """
  @spec extract_hash_tag(binary()) :: binary() | nil
  def extract_hash_tag(key) do
    case :binary.match(key, "{") do
      {start, 1} ->
        rest_start = start + 1
        rest_len = byte_size(key) - rest_start

        case :binary.match(key, "}", [{:scope, {rest_start, rest_len}}]) do
          {end_pos, 1} when end_pos > rest_start ->
            binary_part(key, rest_start, end_pos - rest_start)

          _ ->
            nil
        end

      :nomatch ->
        nil
    end
  end

  @doc """
  Returns the registered process name for the shard at `index`.

  Uses the pre-computed tuple from the instance context for O(1) lookup.
  """
  @spec shard_name(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  def shard_name(ctx, index), do: elem(ctx.shard_names, index)

  @doc """
  Returns the keydir ETS table ref for the shard at `index`.

  Uses the pre-computed tuple from the instance context for O(1) lookup.
  """
  @spec keydir_name(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
  def keydir_name(ctx, index), do: elem(ctx.keydir_refs, index)

  # -------------------------------------------------------------------
  # Convenience accessors (dispatch to correct shard)
  # -------------------------------------------------------------------

  @doc """
  Returns the on-disk file reference for a key's value, or `nil`.

  Used by the sendfile optimisation in standalone TCP mode. Returns
  `{file_path, value_byte_offset, value_size}` for cold (on-disk) keys.
  Returns `nil` for hot keys (ETS), expired keys, or missing keys --
  the caller should fall back to the normal read path.

  Only cold keys benefit from sendfile: hot keys are already in BEAM memory
  and would need a normal `get` + `transport.send`.
  """
  @spec get_file_ref(FerricStore.Instance.t(), binary()) :: {binary(), non_neg_integer(), non_neg_integer()} | nil
  def get_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = System.os_time(:millisecond)

    case ets_get_full(keydir, key, now) do
      {:hit, _value, _lfu} ->
        # Hot key — value is in ETS, sendfile not applicable.
        nil

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Cold key — return file ref directly, no GenServer needed.
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
        # Adjust offset to skip header and key bytes (sendfile needs value offset).
        value_offset = offset + 26 + byte_size(key)
        {path, value_offset, value_size}

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — fall back to GenServer.
        GenServer.call(resolve_shard(ctx, idx), {:get_file_ref, key})

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        # Key doesn't exist. No GenServer needed.
        nil

      :no_table ->
        nil
    end
  end

  @doc """
  Unified GET that returns everything from a single ETS lookup.

  Returns:
    - `{:hot, value}` — value is in ETS, ready to return
    - `{:cold_ref, path, offset, size}` — value is on disk, file ref for sendfile
    - `{:cold_value, value}` — value was on disk, GenServer fetched it
    - `:miss` — key doesn't exist
  """
  @spec get_with_file_ref(FerricStore.Instance.t(), binary()) :: {:hot, binary()} | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()} | {:cold_value, binary()} | :miss
  def get_with_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = System.os_time(:millisecond)

    case ets_get_full(keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Value is on disk — return file ref for potential sendfile.
        # Use DataDir directly to avoid GenServer roundtrip.
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
        Stats.record_cold_read(ctx, key)
        {:cold_ref, path, offset, value_size}

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but no valid file ref — ask GenServer
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})
        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          Stats.incr_keyspace_misses(ctx)
          :miss
        end

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        :miss

      :miss ->
        # Key not in ETS = doesn't exist. No GenServer needed.
        Stats.incr_keyspace_misses(ctx)
        :miss

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})
        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          Stats.incr_keyspace_misses(ctx)
          :miss
        end
    end
  end

  # Like ets_get but returns file ref info for cold entries and LFU counter for hits.
  # Single lookup provides everything needed — no second ETS read for bookkeeping.
  defp ets_get_full(keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          {:hit, value, lfu}

        [{^key, nil, 0, _lfu, fid, off, vsize}] ->
          {:cold, fid, off, vsize}

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {:hit, value, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}] when exp > now ->
          {:cold, fid, off, vsize}

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  @doc """
  Retrieves the value for `key`, or `nil` if the key does not exist or is
  expired.

  Hot path: reads directly from ETS (no GenServer roundtrip for cached keys).
  Falls back to a GenServer call for cache misses or when the ETS table is
  temporarily unavailable (e.g. during a shard restart).

  Each successful read is recorded as either *hot* (ETS hit) or *cold*
  (Bitcask fallback) in `Ferricstore.Stats` for the `FERRICSTORE.HOTNESS`
  command and the `INFO stats` hot/cold fields.
  """
  @spec get(FerricStore.Instance.t(), binary()) :: binary() | nil
  def get(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = System.os_time(:millisecond)

    case ets_get_full(keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        value

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Cold key — value evicted from ETS but disk location known.
        # Read directly from Bitcask via NIF, bypassing the Shard GenServer.
        # The ETS entry has valid file_id/offset from when the write committed,
        # so pread works without flushing pending async writes.
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

        case Ferricstore.Bitcask.NIF.v2_pread_at(path, offset) do
          {:ok, value} ->
            Stats.record_cold_read(ctx, key)
            # Warm ETS: promote back to hot if value fits in cache
            warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset)
            value

          _ ->
            nil
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but invalid file ref (file_id=0 or value_size=0) — ask GenServer.
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        # Key not in ETS at all — doesn't exist. No GenServer needed.
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result
    end
  end

  @doc """
  Returns `{value, expire_at_ms}` for a live key, or `nil` if the key does
  not exist or is expired.

  Hot path: reads directly from ETS for cached keys. Each read is recorded
  as hot or cold in `Ferricstore.Stats`.
  """
  @spec get_meta(FerricStore.Instance.t(), binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = System.os_time(:millisecond)

    case ets_get_full(keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        # Recover expire_at_ms from ETS (ets_get_full returns lfu, not exp).
        expire_at_ms =
          try do
            case :ets.lookup(keydir, key) do
              [{^key, _val, exp, _lfu, _fid, _off, _vsize}] -> exp
              _ -> 0
            end
          rescue
            ArgumentError -> 0
          end
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Cold key — read value from disk directly, return with expire_at_ms.
        expire_at_ms =
          try do
            case :ets.lookup(keydir, key) do
              [{^key, _val, exp, _lfu, _fid, _off, _vsize}] -> exp
              _ -> 0
            end
          rescue
            ArgumentError -> 0
          end

        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

        case Ferricstore.Bitcask.NIF.v2_pread_at(path, offset) do
          {:ok, value} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            nil
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — ask GenServer.
        Stats.record_cold_read(ctx, key)
        GenServer.call(resolve_shard(ctx, idx), {:get_meta, key})

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        Stats.record_cold_read(ctx, key)
        GenServer.call(resolve_shard(ctx, idx), {:get_meta, key})
    end
  end

  # Sampling rate for read-side bookkeeping (LFU touch + hot/cold stats).
  # 1 in N reads performs the ETS writes. Reduces write contention at high
  # concurrency with negligible impact on LFU accuracy (logarithmic counter)
  # and stats precision (ratio stays the same).
  # Default 100 = sample 1 in 100 reads. Set to 1 to disable sampling.

  # LFU counter already available from the initial ets_get_full lookup.
  # Eliminates the second ETS lookup that sampled_read_bookkeeping does.
  defp sampled_read_bookkeeping_fast(ctx, keydir, key, lfu) do
    rate = ctx.read_sample_rate

    if rate <= 1 or :rand.uniform(rate) == 1 do
      Stats.incr_keyspace_hits(ctx)
      LFU.touch(ctx, keydir, key, lfu)
      Stats.record_hot_read(ctx, key)
    end
  end

  # After a cold read, promote the value back to ETS (hot) if it fits
  # under the hot cache max value size threshold. ETS is :public with
  # write_concurrency so this is safe from any process.
  defp warm_ets_after_cold_read(ctx, keydir, key, value, _file_id, _offset) do
    # Skip promotion when under memory pressure — prevents evict/re-promote
    # thrashing where MemoryGuard evicts values and cold reads immediately
    # re-cache them. skip_promotion? is set at :pressure level (85%+).
    skip_promotion = :atomics.get(ctx.pressure_flags, 3) == 1

    if byte_size(value) <= ctx.hot_cache_max_value_size and not skip_promotion do
      try do
        :ets.update_element(keydir, key, {2, value})
      rescue
        ArgumentError -> :ok
      end
    end
  end

  @doc """
  Stores `key` with `value`. `expire_at_ms` is an absolute Unix-epoch
  timestamp in milliseconds; pass `0` for no expiry.
  """
  @spec put(FerricStore.Instance.t(), binary(), binary(), non_neg_integer()) :: :ok | {:error, binary()}
  @max_key_size 65_535
  @max_value_size 512 * 1024 * 1024

  @doc "Returns the maximum allowed value size in bytes."
  def max_value_size, do: @max_value_size

  def put(ctx, key, value, expire_at_ms \\ 0) do
    cond do
      byte_size(key) > @max_key_size ->
        {:error, "ERR key too large (max #{@max_key_size} bytes)"}

      is_binary(value) and byte_size(value) >= @max_value_size ->
        {:error, "ERR value too large (max #{@max_value_size} bytes)"}

      true ->
        case check_keydir_full(ctx, key) do
          :ok ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:put, key, value, expire_at_ms})

          {:error, _} = err ->
            err
        end
    end
  end

  # Checks if the keydir is full. If so, only allows writes to existing keys.
  # Checks both `keydir_full?` (ETS-level memory guard) and `reject_writes?`
  # (noeviction policy with reject-level pressure). The Shard GenServer has its
  # own `reject_writes?` check in `handle_call({:put, ...})`, but when the
  # quorum bypass path is used, the Shard is skipped, so we must check here.
  # Reads from ctx.pressure_flags atomics instead of persistent_term.
  defp check_keydir_full(ctx, key) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      # Allow updates to existing keys — use ETS direct check
      if exists_fast?(ctx, key) do
        :ok
      else
        # Nudge MemoryGuard to run eviction immediately (async, non-blocking).
        # Without this, the next eviction cycle is up to 100ms away.
        Ferricstore.MemoryGuard.nudge()
        {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  @doc "Deletes `key`. Returns `:ok` whether or not the key existed."
  @spec delete(FerricStore.Instance.t(), binary()) :: :ok
  def delete(ctx, key) do
    idx = shard_for(ctx, key)

    raft_write(ctx, idx, key, {:delete, key})
  end

  @doc """
  Submits a server command through Raft for replication to all nodes.

  Server commands are opaque to the library — the state machine dispatches
  them via the `raft_apply_hook` callback on the Instance struct. Routed
  through shard 0 for consistent ordering.
  """
  @spec server_command(FerricStore.Instance.t(), term()) :: term()
  def server_command(ctx, command) do
    raft_write(ctx, 0, "__server__", {:server_command, command})
  end

  @doc """
  Routes a probabilistic data structure write command through Raft.
  """
  @spec prob_write(FerricStore.Instance.t(), tuple()) :: term()
  def prob_write(ctx, command) do
    key = extract_prob_key(command)
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, command)
  end

  defp extract_prob_key({:bloom_create, key, _, _, _}), do: key
  defp extract_prob_key({:bloom_add, key, _, _}), do: key
  defp extract_prob_key({:bloom_madd, key, _, _}), do: key
  defp extract_prob_key({:cms_create, key, _, _}), do: key
  defp extract_prob_key({:cms_incrby, key, _}), do: key
  defp extract_prob_key({:cms_merge, dst_key, _, _, _}), do: dst_key
  defp extract_prob_key({:cuckoo_create, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_add, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_addnx, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_del, key, _}), do: key
  defp extract_prob_key({:topk_create, key, _, _, _, _}), do: key
  defp extract_prob_key({:topk_add, key, _}), do: key
  defp extract_prob_key({:topk_incrby, key, _}), do: key

  @doc """
  Returns `true` if `key` exists and is not expired.

  Uses direct ETS lookup (no GenServer roundtrip) for hot and cold keys.
  A key is considered existing if it is in the keydir and not expired,
  regardless of whether its value is hot (in ETS) or cold (on disk only).
  """
  @spec exists?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists?(ctx, key) do
    exists_fast?(ctx, key)
  end

  @doc """
  Fast ETS-direct existence check for a key.

  Returns `true` if the key exists in ETS and is not expired, `false` otherwise.
  This bypasses the GenServer entirely, saving ~1-3us per call. Used in the
  hot write path (`check_keydir_full/2`) where we only need a boolean answer
  and can tolerate the fact that cold keys (value=nil but still in keydir)
  are correctly detected as existing.
  """
  @spec exists_fast?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists_fast?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = System.os_time(:millisecond)

    try do
      case :ets.lookup(keydir, key) do
        [{^key, _val, 0, _lfu, _fid, _off, _vsize}] -> true
        [{^key, _val, exp, _lfu, _fid, _off, _vsize}] when exp > now -> true
        _ -> false
      end
    rescue
      ArgumentError -> false
    end
  end

  @doc """
  Atomically increments the integer value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_integer}`
  on success or `{:error, reason}` if the value is not a valid integer.
  """
  @spec incr(FerricStore.Instance.t(), binary(), integer()) :: {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr, key, delta})
  end

  @doc """
  Atomically increments the float value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
  on success or `{:error, reason}` if the value is not a valid float.
  """
  @spec incr_float(FerricStore.Instance.t(), binary(), float()) :: {:ok, binary()} | {:error, binary()}
  def incr_float(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr_float, key, delta})
  end

  @doc """
  Atomically appends `suffix` to the value of `key`.

  If the key does not exist, it is created with value `suffix`.
  Returns `{:ok, new_byte_length}`.
  """
  @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(ctx, key, suffix) do
    raft_write(ctx, shard_for(ctx, key), key, {:append, key, suffix})
  end

  @doc """
  Atomically gets the old value and sets a new value for `key`.

  Returns the old value, or `nil` if the key did not exist.
  """
  @spec getset(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def getset(ctx, key, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:getset, key, value})
  end

  @doc """
  Atomically gets and deletes `key`.

  Returns the value, or `nil` if the key did not exist.
  """
  @spec getdel(FerricStore.Instance.t(), binary()) :: binary() | nil
  def getdel(ctx, key) do
    raft_write(ctx, shard_for(ctx, key), key, {:getdel, key})
  end

  @doc """
  Atomically gets the value and updates the expiry of `key`.

  `expire_at_ms` is an absolute Unix-epoch timestamp in milliseconds;
  pass `0` to persist (remove expiry). Returns the value, or `nil` if
  the key did not exist.
  """
  @spec getex(FerricStore.Instance.t(), binary(), non_neg_integer()) :: binary() | nil
  def getex(ctx, key, expire_at_ms) do
    raft_write(ctx, shard_for(ctx, key), key, {:getex, key, expire_at_ms})
  end

  @doc """
  Atomically overwrites part of the string at `key` starting at `offset`.

  Zero-pads if the key doesn't exist or the string is shorter than offset.
  Returns `{:ok, new_byte_length}`.
  """
  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
  def setrange(ctx, key, offset, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:setrange, key, offset, value})
  end

  @doc "Returns all live (non-expired, non-deleted) keys across every shard."
  @spec keys(FerricStore.Instance.t()) :: [binary()]
  def keys(ctx) do
    sc = ctx.shard_count
    Enum.flat_map(0..(sc - 1), fn i ->
      GenServer.call(resolve_shard(ctx, i), :keys)
    end)
  end

  @doc "Returns the count of all live keys across every shard."
  @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer()
  def dbsize(ctx) do
    sc = ctx.shard_count
    Enum.reduce(0..(sc - 1), 0, fn i, acc ->
      try do
        acc + :ets.info(resolve_keydir(ctx, i), :size)
      rescue
        ArgumentError -> acc
      end
    end)
  end

  @doc """
  Returns the current write version of the shard that owns `key`.

  Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
  """
  @spec get_version(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def get_version(ctx, key) do
    GenServer.call(resolve_shard(ctx, shard_for(ctx, key)), {:get_version, key})
  end

  @doc """
  Returns the keydir disk location for a key, or `:miss`.

  Reads the `{file_id, offset, value_size}` fields directly from the keydir
  ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
  for live keys, or `:miss` if the key is not in the keydir or is expired.

  Used by sendfile zero-copy and STRLEN on cold keys.
  """
  @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) :: {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | :miss
  def get_keydir_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = System.os_time(:millisecond)

    try do
      case :ets.lookup(keydir, key) do
        [{_, _, 0, _, fid, off, vsize}] ->
          {:ok, {fid, off, vsize}}

        [{_, _, exp, _, fid, off, vsize}] when exp > now ->
          {:ok, {fid, off, vsize}}

        [{_, _, _exp, _, _fid, _off, _vsize}] ->
          :miss

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :miss
    end
  end

  # -------------------------------------------------------------------
  # Native command accessors
  # -------------------------------------------------------------------

  @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) :: 1 | 0 | nil
  def cas(ctx, key, expected, new_value, ttl_ms) do
    expire_at_ms = if ttl_ms, do: System.os_time(:millisecond) + ttl_ms, else: nil
    raft_write(ctx, shard_for(ctx, key), key, {:cas, key, expected, new_value, expire_at_ms})
  end

  @spec lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(ctx, key, owner, ttl_ms) do
    expire_at_ms = System.os_time(:millisecond) + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:lock, key, owner, expire_at_ms})
  end

  @spec unlock(FerricStore.Instance.t(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(ctx, key, owner) do
    raft_write(ctx, shard_for(ctx, key), key, {:unlock, key, owner})
  end

  @spec extend(FerricStore.Instance.t(), binary(), binary(), pos_integer()) :: 1 | {:error, binary()}
  def extend(ctx, key, owner, ttl_ms) do
    expire_at_ms = System.os_time(:millisecond) + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:extend, key, owner, expire_at_ms})
  end

  @spec ratelimit_add(FerricStore.Instance.t(), binary(), pos_integer(), pos_integer(), pos_integer()) :: [term()]
  def ratelimit_add(ctx, key, window_ms, max, count) do
    now_ms = System.os_time(:millisecond)
    raft_write(ctx, shard_for(ctx, key), key, {:ratelimit_add, key, window_ms, max, count, now_ms})
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec list_op(FerricStore.Instance.t(), binary(), term()) :: term()
  def list_op(ctx, key, {:lmove, destination, from_dir, to_dir}) do
    src_idx = shard_for(ctx, key)
    dst_idx = shard_for(ctx, destination)

    if src_idx == dst_idx do
      GenServer.call(resolve_shard(ctx, src_idx), {:list_op_lmove, key, destination, from_dir, to_dir})
    else
      case GenServer.call(resolve_shard(ctx, src_idx), {:list_op, key, {:pop_for_move, from_dir}}) do
        nil -> nil
        {:error, _} = err -> err
        element ->
          push_op = if to_dir == :left, do: {:lpush, [element]}, else: {:rpush, [element]}
          case GenServer.call(resolve_shard(ctx, dst_idx), {:list_op, destination, push_op}) do
            {:error, _} = err -> err
            _length -> element
          end
      end
    end
  end

  def list_op(ctx, key, operation) do
    GenServer.call(resolve_shard(ctx, shard_for(ctx, key)), {:list_op, key, operation})
  end
end
