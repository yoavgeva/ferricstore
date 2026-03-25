# FerricStore Performance Audit

Audited: 2026-03-21
Scope: Hot path (Router, Shard, Batcher, StateMachine, LFU, Connection) and warm path (MemoryGuard, PrefixIndex, Strings, Parser, Dispatcher)

---

## CRITICAL -- Fix Immediately

### C1. `Application.get_env` called on EVERY LFU touch (every read hit)

**File:** `apps/ferricstore/lib/ferricstore/store/lfu.ex`, lines 101 and 112
**Function:** `LFU.touch/3`
**Impact:** ~400-500ns per GET on a hot key (two ETS lookups into `:ac_tab`)

`LFU.touch/3` is called on **every ETS cache hit** via `Router.get/1` -> `ets_get` -> `lfu_touch`. It calls `Application.get_env` twice:

```elixir
# Line 101
decay_time = Application.get_env(:ferricstore, :lfu_decay_time, 1)
# Line 112
log_factor = Application.get_env(:ferricstore, :lfu_log_factor, 10)
```

`Application.get_env` does an ETS lookup on `:ac_tab` (~200-250ns each). Two calls per GET adds 400-500ns to every hot read. For sub-microsecond reads, this is a 30-50% tax.

**Also affected:** `LFU.effective_counter/1` (line 80) calls `Application.get_env` once -- used during eviction sampling.

**Fix:** Cache in `:persistent_term` at startup:

```elixir
# In Application.start or a dedicated init:
:persistent_term.put(:ferricstore_lfu_decay_time,
  Application.get_env(:ferricstore, :lfu_decay_time, 1))
:persistent_term.put(:ferricstore_lfu_log_factor,
  Application.get_env(:ferricstore, :lfu_log_factor, 10))

# In LFU.touch/3:
decay_time = :persistent_term.get(:ferricstore_lfu_decay_time)
log_factor = :persistent_term.get(:ferricstore_lfu_log_factor)
```

`:persistent_term.get` is a single pointer deref (~5ns), saving ~400ns per hot read.

---

### C2. `MemoryGuard.keydir_full?/0` and `reject_writes?/0` are GenServer.call on every PUT

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex`, line 251
**Function:** `Router.check_keydir_full/1`
**Also:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, line 951
**Impact:** ~1-5us per PUT (GenServer.call roundtrip to MemoryGuard)

Every PUT goes through `check_keydir_full/1` which calls `MemoryGuard.keydir_full?()` -- a `GenServer.call` that serializes through the MemoryGuard process. Under write-heavy workloads this becomes a **global contention point** since all 4 shards + Router funnel through one GenServer.

The shard handler at line 951 additionally calls `MemoryGuard.reject_writes?()` -- a second GenServer.call.

```elixir
# router.ex line 250-266
defp check_keydir_full(key) do
  try do
    if Ferricstore.MemoryGuard.keydir_full?() do  # <-- GenServer.call
      if exists?(key) do  # <-- another GenServer.call to shard
        :ok
      else
        {:error, "KEYDIR_FULL ..."}
      end
    else
      :ok
    end
  ...
end
```

**Fix:** MemoryGuard should publish its pressure level to a `:persistent_term` or `:atomics` on every check cycle (100ms). Readers do a lock-free read instead of GenServer.call:

```elixir
# In MemoryGuard.perform_check:
:persistent_term.put(:ferricstore_keydir_full, state.keydir_pressure_level == :reject)

# In Router.check_keydir_full:
defp check_keydir_full(key) do
  if :persistent_term.get(:ferricstore_keydir_full, false) do
    if exists?(key), do: :ok, else: {:error, "KEYDIR_FULL ..."}
  else
    :ok
  end
end
```

This eliminates the GenServer bottleneck. The staleness window (100ms) is acceptable since memory pressure changes slowly.

---

### C3. `Router.exists?/1` is a GenServer.call -- used in the PUT hot path

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex`, line 276-278
**Function:** `Router.exists?/1`
**Impact:** ~1-3us per PUT when keydir is full (GenServer.call to shard)

`exists?/1` always goes through `GenServer.call(shard_name(shard_for(key)), {:exists, key})`. This is fine for the command-level EXISTS, but it's called inside `check_keydir_full/1` in the hot write path. When keydir is reporting full, every PUT does a full GenServer roundtrip just to check existence.

**Fix:** Add an ETS-direct fast path for existence checks:

```elixir
@spec exists_fast?(binary()) :: boolean()
def exists_fast?(key) do
  idx = shard_for(key)
  keydir = :"keydir_#{idx}"
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
```

---

### C4. `shard_name/1` creates a new atom via string interpolation on every call

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex`, line 57
**Function:** `Router.shard_name/1`
**Impact:** ~200-400ns per call (string interpolation + atom lookup)

```elixir
def shard_name(index), do: :"Ferricstore.Store.Shard.#{index}"
```

Every GET, PUT, DELETE, EXISTS, INCR, etc. calls `shard_name(shard_for(key))`. The string interpolation `"Ferricstore.Store.Shard.#{index}"` allocates a temporary binary and then converts it to an atom on every invocation. Atom lookup is O(1) but the binary allocation + conversion is ~200-400ns.

With 4 shards, there are only 4 possible return values.

**Fix:** Pre-compute a tuple at compile time:

```elixir
@shard_names (
  for i <- 0..(@shard_count - 1), into: %{} do
    {i, :"Ferricstore.Store.Shard.#{i}"}
  end
)

def shard_name(index), do: Map.fetch!(@shard_names, index)
```

Or even simpler, a tuple for O(1) element access:

```elixir
@shard_names List.to_tuple(
  for i <- 0..(@shard_count - 1), do: :"Ferricstore.Store.Shard.#{i}"
)

def shard_name(index), do: elem(@shard_names, index)
```

`elem/2` is ~5ns vs ~300ns for interpolation. Saves ~300ns on every routed operation.

**Also affected:** `Batcher.batcher_name/1` (line 164) has the same pattern.

---

### C5. `build_store/1` allocates a new 30+ key map on every dispatched command

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 1930-2008
**Function:** `build_raw_store/0`, `build_store/1`
**Impact:** ~500-1000ns per command (map allocation + closure creation)

Every command dispatch calls `build_store(state.sandbox_namespace)` which calls `build_raw_store/0`, constructing a fresh `%{get: ..., put: ..., delete: ..., ...}` map with 30+ entries and anonymous function closures. This is done on every single command -- hot GET included.

The non-sandbox store is identical for every call and could be a module attribute or persistent_term.

**Fix:** Build the raw store once and cache it:

```elixir
@raw_store %{
  get: &Router.get/1,
  get_meta: &Router.get_meta/1,
  put: &Router.put/3,
  # ... all other function references
}

defp build_store(nil), do: @raw_store
defp build_store(ns) when is_binary(ns) do
  # Only sandbox needs dynamic wrapping
  %{@raw_store |
    get: fn key -> Router.get(ns <> key) end,
    ...
  }
end
```

Function captures like `&Router.get/1` are compile-time constants and do not allocate when referenced from a module attribute.

---

## HIGH -- Fix Soon

### H1. `String.upcase/1` called on every command dispatch

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 725
**Also:** `apps/ferricstore/lib/ferricstore/commands/dispatcher.ex`, line 113
**Function:** `handle_command/2`, `Dispatcher.dispatch/3`
**Impact:** ~100-200ns per command (UTF-8 aware upcase + binary allocation)

Commands are uppercased twice: once in `handle_command/2` (line 725) and again in `Dispatcher.dispatch/3` (line 113):

```elixir
# connection.ex:725
cmd = String.upcase(name)

# dispatcher.ex:113
cmd = String.upcase(name)
```

`String.upcase/1` is UTF-8 aware, allocates a new binary, and scans every byte. Redis commands are always ASCII.

**Fix:** Upcase once in the connection layer and pass the already-uppercased command through:

```elixir
# dispatcher.ex: remove the redundant String.upcase
def dispatch(cmd, args, store) do
  # cmd is already uppercased by the connection layer
  start = System.monotonic_time(:microsecond)
  result =
    case Map.get(@cmd_dispatch_map, cmd) do
      ...
```

Also consider an ASCII-only fast upcase for the connection layer:

```elixir
defp ascii_upcase(<<c, rest::binary>>) when c >= ?a and c <= ?z,
  do: <<c - 32, ascii_upcase(rest)::binary>>
defp ascii_upcase(<<c, rest::binary>>), do: <<c, ascii_upcase(rest)::binary>>
defp ascii_upcase(<<>>), do: <<>>
```

---

### H2. `normalise_cmd/1` called multiple times per pipeline command

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 357-365, 370-386, 613-663
**Functions:** `normalise_cmd/1`, `stateful_command?/2`, `barrier_command?/1`, `command_shard_key/1`
**Impact:** ~200-400ns per pipeline command (redundant pattern matching + String.upcase)

For each command in a pipeline, `normalise_cmd/1` is called up to 3 times:
1. `stateful_command?/2` -> `normalise_cmd/1`
2. `barrier_command?/1` -> `normalise_cmd/1`
3. `command_shard_key/1` -> `normalise_cmd/1`

Each call does `String.upcase(name)` and pattern matching.

**Fix:** Normalise once and pass the result through:

```elixir
# Pre-normalise at the top of the sliding window loop
{name, args} = normalise_cmd(cmd)
cond do
  stateful_normalised?(name, args, state) -> ...
  barrier_normalised?(name, args) -> ...
  true -> ...
end
```

---

### H3. `IO.iodata_to_binary/1` on TLS error rejection

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 203
**Impact:** Unnecessary binary allocation on error path

```elixir
error_msg = Encoder.encode({:error, "ERR TLS required: ..."})
transport.send(socket, IO.iodata_to_binary(error_msg))
```

`Encoder.encode` already returns iodata. `IO.iodata_to_binary` forces a copy. `transport.send` accepts iodata directly.

**Fix:** `transport.send(socket, error_msg)` -- remove the `IO.iodata_to_binary` call.

---

### H4. `Enum.reject` for pending list cleanup is O(n) on every DELETE

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, line 1547 (and 686, 1399, 2652)
**Function:** Various direct-path DELETE handlers
**Impact:** O(n) scan where n = pending list length; up to 1000 entries

```elixir
new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
```

The pending list can grow up to `@flush_interval_ms` worth of writes. Each DELETE scans the entire list. Under mixed workloads with high write rates, this is significant.

**Fix:** Since pending is flushed every 1ms, this is bounded. But for the direct (non-Raft) path, the pattern is cleaner with a MapSet of pending keys for O(1) membership + removal, or just accept the 1ms-bounded list size as acceptable.

Lower priority since Raft mode bypasses this entirely.

---

### H5. `ets_get` in Router uses try/rescue for table existence check

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex`, lines 178-208
**Function:** `ets_get/3`
**Impact:** ~50-100ns overhead per call (try/rescue setup)

```elixir
defp ets_get(keydir, key, now) do
  try do
    case :ets.lookup(keydir, key) do
      ...
    end
  rescue
    ArgumentError -> :no_table
  end
end
```

The try/rescue block adds ~50-100ns of overhead even when no exception is raised (BEAM sets up an exception handler frame). The `:no_table` case only happens during shard restarts (extremely rare).

**Fix:** Use `:ets.whereis/1` for the cold check, or just accept the ~50ns cost since it guards against crashes. Alternatively, use `case :ets.info(keydir) do` but that's slower. The current approach is reasonable -- marking as known cost.

---

### H6. `shard_path/1` recomputes the path on every cold read

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 331-333
**Function:** `shard_path/1`
**Impact:** ~200-400ns per cold read (calls `DataDir.shard_data_path` which does string concat)

```elixir
defp shard_path(state) do
  Ferricstore.DataDir.shard_data_path(state.data_dir, state.index)
end
```

Called on every cold read, every flush, every compaction. The result is deterministic for the lifetime of the shard.

**Fix:** The shard already stores `data_dir` and `index` -- just store the computed path in the struct during init:

The struct already has `data_dir` but not the computed shard path. Add `:shard_data_path` to the struct and populate in init:

```elixir
# In init:
shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, index)

# In struct:
:shard_data_path

# Replace all shard_path(state) calls with state.shard_data_path
```

---

### H7. `file_path/2` in shard does String.pad_leading + Path.join on every disk read

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 326-328
**Also:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, lines 981-983
**Function:** `file_path/2`, `sm_file_path/2`
**Impact:** ~300-500ns per cold read (integer formatting + padding + Path.join)

```elixir
defp file_path(shard_path, file_id) do
  Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
end
```

Called on every cold read. The active file path is already cached in state as `active_file_path`, but cold reads for older files construct the path dynamically.

**Fix:** Cache file paths in a map keyed by file_id:

```elixir
# Add to state: file_path_cache: %{file_id => path}
# In file_path/2:
defp file_path(state, file_id) do
  case Map.get(state.file_path_cache, file_id) do
    nil ->
      path = Path.join(state.shard_data_path,
        "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
      # Return path, caller should update cache
      path
    path -> path
  end
end
```

---

### H8. `l1_evict_and_put` scans all L1 entries with `Enum.min_by` for every eviction

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 2293-2313
**Function:** `l1_evict_and_put/4`
**Impact:** O(n) scan of L1 cache on every eviction (n up to 64 entries)

```elixir
{evict_key, evict_entry} = Enum.min_by(state.l1_cache, fn {_, e} -> e.hits end)
```

When L1 is full (64 entries), every new warm-up requires an O(64) scan to find the lowest-hit entry.

**Fix:** Maintain a min-heap or approximate LFU structure. For 64 entries, a simple approach: track the `{min_key, min_hits}` in state and update on insert/touch. Or accept the O(64) cost since L1 is small and this only fires on miss (not on every read).

---

## MEDIUM -- Optimize When Convenient

### M1. `System.os_time(:millisecond)` called redundantly in Router read path

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex`, lines 113, 149, 413
**Functions:** `get/1`, `get_meta/1`, `get_keydir_file_ref/1`
**Impact:** ~30-50ns per call (syscall to get monotonic time)

Each function calls `System.os_time(:millisecond)` independently. In a pipeline of GET + TTL + EXISTS for the same key, this syscall is made 3 times.

**Fix:** Not actionable at the Router level since each function is called independently. The cost is inherent. However, `System.os_time/1` on most platforms is a VDSO call and takes ~20-30ns, so this is acceptable.

---

### M2. `Stats.record_hot_read/1` and `Stats.incr_keyspace_hits/0` called on every GET

**File:** `apps/ferricstore/lib/ferricstore/store/router.ex`, lines 117-118
**Function:** `Router.get/1`
**Impact:** ~100-200ns per GET (two ETS updates or GenServer calls to Stats)

```elixir
Stats.record_hot_read(key)
Stats.incr_keyspace_hits()
```

Two stats operations on every successful read. If Stats uses ETS `:update_counter`, each is ~80ns. If it uses GenServer.call, much worse.

**Fix:** Use `:counters` or `:atomics` for these high-frequency counters instead of ETS updates. Or batch stats updates using process dictionary accumulation flushed on a timer.

---

### M3. `Batcher.extract_prefix/1` uses `:binary.split` on every write

**File:** `apps/ferricstore/lib/ferricstore/raft/batcher.ex`, lines 198-205
**Function:** `extract_prefix/1`
**Impact:** ~50-100ns per write (binary scan for `:`)

```elixir
def extract_prefix(command) when is_tuple(command) do
  key = elem(command, 1)
  case :binary.split(key, ":") do
    [^key] -> "_root"
    [prefix | _rest] -> prefix
  end
end
```

`:binary.split/2` is efficient (Boyer-Moore), but for short keys like `"user:42"` it's ~50ns. This is called on every write through the batcher.

**Fix:** Acceptable as-is. `:binary.split` is already the fastest way to find the first colon. Alternative would be a manual byte scan but with diminishing returns.

---

### M4. `PrefixIndex.track/3` does `delete_object` + `insert` on every write

**File:** `apps/ferricstore/lib/ferricstore/store/prefix_index.ex`, lines 93-104
**Function:** `PrefixIndex.track/3`
**Impact:** ~200-400ns per write to a prefixed key (two ETS operations)

```elixir
def track(prefix_table, key, _shard_index) do
  case extract_prefix(key) do
    nil -> :ok
    prefix ->
      :ets.delete_object(prefix_table, {prefix, key})
      :ets.insert(prefix_table, {prefix, key})
      :ok
  end
end
```

The `delete_object` + `insert` is done to prevent bag duplicates. But if the key was just updated (not newly created), the `{prefix, key}` entry already exists and `delete_object` + `insert` is a no-op pair.

**Fix:** Use `:ets.insert/2` alone -- for `:bag` tables, duplicates can accumulate, but `keys_for_prefix/3` already deduplicates via the keydir lookup. Or switch to a `:duplicate_bag` with periodic cleanup. The current approach is correct but slightly wasteful for updates.

---

### M5. `do_delete_prefix` in StateMachine uses `ets.foldl` over ENTIRE keydir

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, lines 1014-1033
**Function:** `do_delete_prefix/2`
**Impact:** O(total_keys) per DEL on a hash/set/zset

```elixir
defp do_delete_prefix(state, prefix) do
  keys_to_delete =
    :ets.foldl(
      fn {key, _value, _exp, _lfu, _fid, _off, _vsize}, acc ->
        if is_binary(key) and String.starts_with?(key, prefix) do
          [key | acc]
        ...
```

This scans the entire keydir (potentially millions of keys) to find entries matching a prefix. This runs inside the Raft state machine's `apply/3` -- which blocks the entire Raft pipeline for this shard.

**Fix:** Use an ETS match spec with a bound prefix:

```elixir
# Use :ets.select with a match spec that bounds the scan
match_spec = [
  {{:"$1", :_, :_, :_, :_, :_, :_},
   [{:andalso,
     {:is_binary, :"$1"},
     {:==, {:binary_part, :"$1", 0, byte_size(prefix)}, prefix}}],
   [:"$1"]}
]
keys_to_delete = :ets.select(state.ets, match_spec)
```

Or better: leverage the prefix_keys bag table for O(matching) lookup. The StateMachine already has `state.prefix_keys`.

---

### M6. Duplicate `ets.foldl` scans in shard compound operations

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, multiple locations (lines 426-448, 452-469, 472-512, 698-774, 777-811, 813-936)
**Functions:** `handle_call({:scan_prefix, ...})`, `handle_call({:count_prefix, ...})`, `handle_call({:delete_prefix, ...})`, `handle_call({:compound_scan, ...})`, `handle_call({:compound_count, ...})`, `handle_call({:compound_delete_prefix, ...})`
**Impact:** O(total_keys) per compound operation

The same `ets.foldl` pattern scanning the entire keydir with `String.starts_with?` is duplicated across 6+ handlers. Each compound operation (HGETALL, SCARD, DEL on a hash, etc.) triggers a full table scan.

**Fix:** Consolidate into a shared helper and, more importantly, use the existing `prefix_keys` bag table or an ETS match spec to avoid the full scan. The shard already has `state.prefix_keys` available.

---

### M7. `Dispatcher.dispatch/3` calls `SlowLog.maybe_log` on every command

**File:** `apps/ferricstore/lib/ferricstore/commands/dispatcher.ex`, lines 152-153
**Function:** `dispatch/3`
**Impact:** ~50-100ns per command (monotonic time diff + GenServer.call or ETS check)

```elixir
duration = System.monotonic_time(:microsecond) - start
Ferricstore.SlowLog.maybe_log([cmd | args], duration)
```

`System.monotonic_time` is called twice (start and end). The `maybe_log` call checks a threshold. If SlowLog uses GenServer, this is ~1us overhead. If it checks a persistent_term threshold, it's ~50ns.

**Fix:** Acceptable cost if `maybe_log` is cheap (persistent_term threshold check). Verify SlowLog implementation.

---

### M8. `length(updated_slot.cmds)` in Batcher is O(n)

**File:** `apps/ferricstore/lib/ferricstore/raft/batcher.ex`, line 253
**Function:** `handle_call({:write, command}, ...)`
**Impact:** O(n) per write where n = current slot size

```elixir
if length(updated_slot.cmds) >= state.max_batch_size do
```

`length/1` on a list is O(n). With max_batch_size=1000, this means counting up to 1000 elements on every write.

**Fix:** Track a `:count` field in the slot:

```elixir
@spec new_slot(pos_integer()) :: slot()
defp new_slot(window_ms) do
  %{cmds: [], froms: [], timer_ref: nil, window_ms: window_ms, count: 0}
end

# In handle_call:
updated_slot = %{slot | cmds: [command | slot.cmds], froms: [from | slot.froms],
                        window_ms: window_ms, count: slot.count + 1}
...
if updated_slot.count >= state.max_batch_size do
```

---

### M9. `Batcher.cancel_timer/1` does a selective receive

**File:** `apps/ferricstore/lib/ferricstore/raft/batcher.ex`, lines 434-442
**Function:** `cancel_timer/1`
**Impact:** ~1-5us per flush (selective receive scans mailbox)

```elixir
defp cancel_timer(ref) do
  Process.cancel_timer(ref)
  receive do
    {:flush_slot, _} -> :ok
  after
    0 -> :ok
  end
end
```

The `receive ... after 0` scans the entire mailbox looking for `{:flush_slot, _}`. Under high throughput, the batcher's mailbox could contain many pending `{:write, ...}` messages, making this scan expensive.

**Fix:** Use `Process.cancel_timer(ref, info: false)` and accept potential stale flush messages in `handle_info`:

```elixir
defp cancel_timer(nil), do: :ok
defp cancel_timer(ref), do: Process.cancel_timer(ref, info: false)
```

The `handle_info({:flush_slot, slot_key}, state)` already handles the case where the slot is gone (returns `{:noreply, state}`), so stale messages are safe.

---

### M10. `encode_ratelimit` / `decode_ratelimit` use string encoding for counters

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, lines 892-902
**Also:** `apps/ferricstore/lib/ferricstore/store/shard.ex` (similar)
**Impact:** ~200-400ns per RATELIMIT.ADD (string formatting + parsing)

```elixir
defp encode_ratelimit(cur, start, prev), do: "#{cur}:#{start}:#{prev}"

defp decode_ratelimit(value) do
  case String.split(value, ":") do
    [cur, start, prev] ->
      {String.to_integer(cur), String.to_integer(start), String.to_integer(prev)}
    ...
  end
end
```

Three integer->string + string concat for encode, String.split + 3 String.to_integer for decode. For a rate limiter called at high frequency, this is ~400ns of overhead per operation.

**Fix:** Use `:erlang.term_to_binary` / `:erlang.binary_to_term` for compact binary encoding:

```elixir
defp encode_ratelimit(cur, start, prev), do: <<cur::64, start::64, prev::64>>

defp decode_ratelimit(<<cur::64, start::64, prev::64>>), do: {cur, start, prev}
defp decode_ratelimit(_), do: {0, System.os_time(:millisecond), 0}
```

Fixed 24 bytes, no allocation, no parsing. ~10x faster.

---

### M11. `String.contains?` in float formatting (StateMachine)

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, lines 648-656
**Function:** `format_float/1`
**Impact:** ~100-200ns per INCRBYFLOAT

```elixir
defp format_float(val) when is_float(val) do
  formatted = :erlang.float_to_binary(val, [:compact, {:decimals, 17}])
  if String.contains?(formatted, ".") do
    formatted
    |> String.trim_trailing("0")
    |> String.trim_trailing(".")
    |> then(fn s -> s end)
  else
    formatted
  end
end
```

`String.contains?`, `String.trim_trailing` (called twice), and `then/1` (identity function -- no-op) all allocate intermediate binaries. The `then(fn s -> s end)` is dead code.

**Fix:** Remove the no-op `then`, and consider a single-pass binary formatter:

```elixir
defp format_float(val) when is_float(val) do
  formatted = :erlang.float_to_binary(val, [:compact, {:decimals, 17}])
  trim_float(formatted)
end

defp trim_float(bin) do
  case :binary.match(bin, ".") do
    :nomatch -> bin
    _ ->
      bin
      |> String.trim_trailing("0")
      |> String.trim_trailing(".")
  end
end
```

---

## LOW -- Nice to Have

### L1. `in_pubsub_mode?/1` uses `MapSet.size/1` twice per loop iteration

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 2101-2103
**Function:** `in_pubsub_mode?/1`
**Impact:** ~20-50ns per loop iteration (two MapSet.size calls)

```elixir
defp in_pubsub_mode?(state) do
  MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0
end
```

Called at the top of every `loop/1` iteration. `MapSet.size/1` is O(1) but involves two function calls.

**Fix:** Track a boolean `pubsub_active` flag in the connection state, updated on subscribe/unsubscribe.

---

### L2. `format_float` duplicated between shard.ex and state_machine.ex

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex` and `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`
**Impact:** Code duplication, maintenance risk

`parse_float`, `format_float`, `parse_integer`, `encode_ratelimit`, `decode_ratelimit` are all duplicated between shard.ex and state_machine.ex with identical implementations.

**Fix:** Extract into a shared utility module `Ferricstore.Store.ValueCodec` or similar.

---

### L3. `l1_parse_invalidation_keys` calls `IO.iodata_to_binary` + full RESP parse

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 2344-2363
**Function:** `l1_parse_invalidation_keys/1`
**Impact:** ~500-1000ns per invalidation push (iodata flattening + RESP parse)

```elixir
defp l1_parse_invalidation_keys(iodata) do
  binary = IO.iodata_to_binary(iodata)
  case Parser.parse(binary) do ...
```

Every client tracking invalidation push triggers a full RESP parse just to extract key names. Since we generate the push message ourselves, we know its exact structure.

**Fix:** Extract keys directly from the known iodata structure without re-parsing.

---

### L4. `apply_setrange_for_tx` converts binary to charlist and back

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 2107-2119
**Function:** `apply_setrange_for_tx/3`
**Impact:** O(n) memory allocation for charlist conversion

```elixir
defp apply_setrange_for_tx(old, offset, value) do
  old_bytes = :binary.bin_to_list(old)
  val_bytes = :binary.bin_to_list(value)
  padded = ...
  :binary.list_to_bin(head ++ val_bytes ++ rest)
end
```

Converts entire binary to charlist, manipulates, converts back. The main `apply_setrange/3` in shard.ex uses `binary_part` which is much more efficient.

**Fix:** Use the same `binary_part`-based approach as `apply_setrange/3` (and `sm_apply_setrange/3` in state_machine.ex):

```elixir
defp apply_setrange_for_tx(old, offset, value) do
  sm_apply_setrange(old, offset, value)  # reuse existing binary_part implementation
end
```

---

### L5. `discover_active_file` in shard init scans directory listing with multiple passes

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 221-244
**Function:** `discover_active_file/1`
**Impact:** Startup-only, but O(n) for n files in the shard directory (3 passes: filter, map, max)

```elixir
files
|> Enum.filter(&String.ends_with?(&1, ".log"))
|> Enum.map(fn name -> ... end)
```

Three intermediate lists are created. Not hot path, but could be a single `Enum.reduce`:

```elixir
case File.ls(shard_path) do
  {:ok, files} ->
    Enum.reduce(files, {0, 0}, fn name, {max_id, _} = acc ->
      if String.ends_with?(name, ".log") do
        id = name |> String.trim_trailing(".log") |> String.to_integer()
        if id > max_id, do: {id, :needs_stat}, else: acc
      else
        acc
      end
    end)
    ...
```

---

### L6. `require_tls?/0` likely calls Application.get_env on every connection init

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 201
**Impact:** ~200ns per new connection (acceptable since connection init is infrequent)

Should use `persistent_term` if connections are being established at very high rates. Low priority.

---

### L7. `l1_enabled` read from `Application.get_env` on connection init

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 243
**Impact:** ~200ns per new connection

```elixir
l1_enabled: Application.get_env(:ferricstore, :l1_cache_enabled, true)
```

One-time per connection, acceptable.

---

## Concurrency / Architecture Issues

### A1. Shard GenServer is a single-process bottleneck for cold reads + all writes

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`
**Impact:** Head-of-line blocking under mixed workloads

The shard GenServer handles:
- Cold reads (disk I/O via `v2_pread_at`)
- All writes (via Raft batcher or direct)
- Compound operations (hash scans via `ets.foldl`)
- Compaction
- Expiry sweeps

A single slow `ets.foldl` scan (e.g., HGETALL on a 100k-field hash) blocks all other operations on that shard, including simple GETs that could be served from ETS.

Hot reads bypass the GenServer via `Router.get/1` -> ETS, which is correct. But cold reads, all writes, and compound scans serialize through the GenServer.

**Mitigation:** The Raft batcher already offloads write serialization. Cold reads could potentially bypass the GenServer by reading directly from disk using the ETS-stored file location (fid, offset, vsize), but this requires careful coordination with file rotation/compaction.

---

### A2. MemoryGuard `perform_check` iterates all shards and does `ets.info` per shard

**File:** `apps/ferricstore/lib/ferricstore/memory_guard.ex`, lines 331-364
**Function:** `compute_stats/1`
**Impact:** O(shard_count) ETS info calls every 100ms

Each check cycles through all shards calling `:ets.info(:"keydir_#{i}", :memory)`. With 4 shards this is fine. With many more it could become significant. Acceptable for current architecture.

---

### A3. Connection process `build_store` in sandbox mode creates 30+ closures per command

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 1934-1943
**Function:** `build_store/1` with namespace

When sandbox mode is active, every command creates fresh closures that capture the namespace string:

```elixir
defp build_store(ns) when is_binary(ns) do
  raw = build_raw_store()
  %{raw |
    get: fn key -> raw.get.(ns <> key) end,
    ...
  }
end
```

This creates `build_raw_store()` (30+ entries) PLUS overwrites 5 entries with new closures. Two map allocations per command.

**Fix:** For sandbox mode, wrap at the key level rather than the store level, or cache the sandbox store in the connection state (it doesn't change between commands).

---

## Summary Table

| ID  | Severity | File | Est. Savings per Op |
|-----|----------|------|---------------------|
| C1  | CRITICAL | lfu.ex:101,112 | ~400ns/GET |
| C2  | CRITICAL | router.ex:251, shard.ex:951 | ~2-5us/PUT |
| C3  | CRITICAL | router.ex:276 | ~1-3us/PUT (keydir full) |
| C4  | CRITICAL | router.ex:57 | ~300ns/operation |
| C5  | CRITICAL | connection.ex:1930 | ~500-1000ns/command |
| H1  | HIGH | connection.ex:725, dispatcher.ex:113 | ~100-200ns/command |
| H2  | HIGH | connection.ex:357-663 | ~200-400ns/pipeline cmd |
| H3  | HIGH | connection.ex:203 | binary alloc on error |
| H4  | HIGH | shard.ex:1547 | O(n) per DELETE |
| H5  | HIGH | router.ex:178 | ~50-100ns/GET |
| H6  | HIGH | shard.ex:331 | ~200-400ns/cold read |
| H7  | HIGH | shard.ex:326 | ~300-500ns/cold read |
| H8  | HIGH | connection.ex:2293 | O(64) per L1 eviction |
| M1  | MEDIUM | router.ex:113 | ~30ns (acceptable) |
| M2  | MEDIUM | router.ex:117 | ~100-200ns/GET |
| M3  | MEDIUM | batcher.ex:198 | ~50-100ns/write |
| M4  | MEDIUM | prefix_index.ex:93 | ~200-400ns/write |
| M5  | MEDIUM | state_machine.ex:1014 | O(total_keys)/DEL |
| M6  | MEDIUM | shard.ex:426+ | O(total_keys)/compound op |
| M7  | MEDIUM | dispatcher.ex:152 | ~50-100ns/command |
| M8  | MEDIUM | batcher.ex:253 | O(n)/write |
| M9  | MEDIUM | batcher.ex:434 | ~1-5us/flush |
| M10 | MEDIUM | state_machine.ex:892 | ~400ns/RATELIMIT.ADD |
| M11 | MEDIUM | state_machine.ex:648 | ~100-200ns/INCRBYFLOAT |
| L1  | LOW | connection.ex:2101 | ~20-50ns/loop |
| L2  | LOW | shard.ex + state_machine.ex | code duplication |
| L3  | LOW | connection.ex:2344 | ~500-1000ns/invalidation |
| L4  | LOW | shard.ex:2107 | O(n) charlist conversion |
| L5  | LOW | shard.ex:221 | startup only |
| L6  | LOW | connection.ex:201 | ~200ns/connection |
| L7  | LOW | connection.ex:243 | ~200ns/connection |

**Estimated total hot-path savings if C1-C5 + H1 are fixed:**
- GET (hot): ~700-1000ns saved (from ~1.5us to ~0.5-0.8us)
- PUT: ~3-8us saved (from ~10us to ~2-5us, primarily from removing GenServer.call to MemoryGuard)
- Pipeline command dispatch: ~600-1200ns saved per command
