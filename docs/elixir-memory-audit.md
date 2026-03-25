# FerricStore Elixir Memory Audit

**Date:** 2026-03-21
**Scope:** BEAM process memory, ETS memory, binary handling, GC pressure
**Files audited:** router.ex, shard.ex, state_machine.ex, batcher.ex, memory_guard.ex, lfu.ex, connection.ex, async_apply_worker.ex, promotion.ex, strings.ex

---

## CRITICAL

### C1. `Application.get_env` called on every key access in LFU.touch/3

**File:** `apps/ferricstore/lib/ferricstore/store/lfu.ex`, lines 101 and 112, function `touch/3`
**Also:** `effective_counter/1` at line 80

**Current behavior:** Every single GET that hits ETS (the hot path) calls `LFU.touch/3`, which calls `Application.get_env(:ferricstore, :lfu_decay_time, 1)` and `Application.get_env(:ferricstore, :lfu_log_factor, 10)`. `Application.get_env` does an ETS lookup on the application config table per call. At 100k GET/s, this is 200k unnecessary ETS lookups/s, each allocating a small tuple on the caller's heap.

**Recommended fix:** Move both config values to `:persistent_term` at application startup. `persistent_term` is copied into each process's literal pool on first access with zero GC pressure on subsequent reads.

```elixir
# At application startup:
:persistent_term.put(:ferricstore_lfu_decay_time, Application.get_env(:ferricstore, :lfu_decay_time, 1))
:persistent_term.put(:ferricstore_lfu_log_factor, Application.get_env(:ferricstore, :lfu_log_factor, 10))

# In LFU.touch/3:
decay_time = :persistent_term.get(:ferricstore_lfu_decay_time)
log_factor = :persistent_term.get(:ferricstore_lfu_log_factor)
```

**Estimated savings:** ~2 ETS lookups per GET; at 100k GET/s that is ~200k/s fewer ETS reads and fewer short-lived tuples on connection process heaps. Measurable GC pressure reduction on hot paths.

---

### C2. `ets:foldl` full-table scans inside Shard GenServer `handle_call`

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, multiple locations:
- `handle_call({:scan_prefix, ...})` line 430
- `handle_call({:count_prefix, ...})` line 456
- `handle_call({:delete_prefix, ...})` line 477 (twice, Raft and direct paths)
- `handle_call({:compound_scan, ...})` line 705 (and again at 731)
- `handle_call({:compound_count, ...})` line 784
- `handle_call({:compound_delete_prefix, ...})` lines 830, 855, 889, 909
- `maybe_promote/3` line 3485
- `write_hint_for_file/2` line 2958
- `handle_call({:run_compaction, ...})` line 2583
- `build_local_store/1` -- compound_scan/compound_count/compound_delete_prefix closures at lines 2027, 2055, 2074
- `live_keys/1` line 3231

**Also in state_machine.ex:** `do_delete_prefix/2` line 1016

**Current behavior:** `:ets.foldl` iterates every entry in the keydir ETS table for any compound operation (HGETALL, HLEN, HDEL on a hash key, DEL on a data structure, etc.). During the fold, the Shard GenServer is blocked. Each visited ETS entry's 7-tuple is copied onto the calling process heap (the Shard GenServer). For a keydir with 1M entries and a hash with 100 fields, the fold visits all 1M entries to find 100 matches. The intermediate list of keys_to_delete also accumulates on the heap.

**Recommended fix:** Use `:ets.select/2` with a match spec that filters by prefix:

```elixir
# Instead of foldl scanning all keys:
prefix_len = byte_size(prefix)
match_spec = [
  {{:"$1", :_, :_, :_, :_, :_, :_},
   [{:andalso, {:is_binary, :"$1"},
     {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}],
   [:"$1"]}
]
keys = :ets.select(keydir, match_spec)
```

Or better: maintain a secondary ETS `:bag` index `{prefix, key}` for compound keys (similar to `PrefixIndex` but for compound key prefixes like `H:key\0`). This would make all compound operations O(matching) instead of O(total keys).

**Estimated savings:** For a keydir with 1M entries and typical compound operations touching 10-1000 fields, this eliminates copying 999k-1M ETS tuples to the Shard process heap per operation. Reduces GenServer blocking time from O(N_total) to O(N_matching). Prevents multi-MB transient heap spikes.

---

### C3. Connection process `buffer` field uses binary concatenation

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 314, function `handle_data/2`

```elixir
buffer = state.buffer <> data
```

**Current behavior:** Every TCP chunk that arrives is concatenated onto the existing buffer binary using `<>`. This creates a new binary each time if the buffer is non-empty. For pipelined commands arriving in many small TCP segments, this repeatedly copies the accumulated buffer. Since `buffer` is stored in the connection process state (a struct on the heap), each concatenation:
1. Allocates a new binary of size `old_buffer + new_data`
2. Copies old buffer bytes + new data bytes
3. The old buffer binary becomes garbage

For a client sending a 10KB pipeline in 10 x 1KB segments, this copies 1+2+3+...+10 = 55KB total instead of 10KB.

**Recommended fix:** Use an iodata list for the buffer and only convert to binary when parsing:

```elixir
# In handle_data:
buffer_iodata = [state.buffer | data]  # O(1) prepend
binary = IO.iodata_to_binary(buffer_iodata)  # single copy when needed for parsing

# Or simpler: keep binary but use :erlang.iolist_to_binary only once:
# Parser should accept iodata, or buffer should flush more aggressively.
```

Alternatively, since the RESP parser likely needs a contiguous binary, a simpler fix: check if `state.buffer` is empty (the common case for non-pipelined workloads) and avoid the concatenation:

```elixir
buffer = if state.buffer == "", do: data, else: state.buffer <> data
```

The empty-buffer case is already somewhat optimized by BEAM (concatenating with empty binary is cheap), but explicitly checking avoids the function call overhead.

**Estimated savings:** For pipelined workloads with fragmented TCP delivery, this can reduce transient binary allocations by 50-80%. For single-command workloads (buffer is usually empty after parse), savings are minimal.

---

## HIGH

### H1. Shard `pending` list grows unbounded between flush intervals

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, struct definition line 70, and throughout write handlers

**Current behavior:** The `pending` field is a list of `{key, value, expire_at_ms}` tuples. Each write prepends to this list. While the flush interval is 1ms (`@flush_interval_ms`), under burst write workloads, the list can accumulate thousands of entries. Each entry holds a reference to the key binary and value binary. These tuples are on the Shard GenServer's heap.

Key concern: if the value binaries are refc binaries (>64 bytes), the pending list holds references that prevent the parent binary from being GC'd. If the client sends a large value, then immediately sends another command that replaces it, the old value cannot be freed until the pending list is flushed and the tuple is removed.

Additionally, `Enum.reverse(pending)` in `flush_pending/1` (line 2853) creates a second copy of the entire list at flush time, doubling the list memory momentarily.

**Recommended fix:**
1. Add a `max_pending_size` configuration (e.g., 10000 entries) that triggers an immediate synchronous flush when exceeded, capping memory usage.
2. For the reverse operation, since the order within a single flush batch doesn't matter for Bitcask semantics (last-writer-wins), consider using a queue data structure (`:queue`) or simply reversing in the NIF.

**Estimated savings:** Bounds worst-case Shard process heap growth during write bursts. With 10K entries * ~200 bytes average value = ~2MB cap vs. unbounded.

---

### H2. `Enum.reject` for delete in pending list is O(N)

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 686, 1399, 1547, 2652

```elixir
new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
```

**Current behavior:** On every DELETE operation (direct path, non-Raft), the entire pending list is scanned and a new list is built without the deleted key. This is O(N) in the pending list size. Under high write throughput with mixed deletes, this creates significant GC pressure because each `Enum.reject` allocates a new list.

**Recommended fix:** Since Bitcask uses last-writer-wins semantics and tombstones are written synchronously (the delete handler already calls `flush_pending_sync`), the pending list is already empty after the flush. The `Enum.reject` is belt-and-suspenders -- after `flush_pending_sync`, `pending` is `[]`. The reject on an empty list returns `[]`. Verify this invariant holds and remove the reject, or add an assertion.

**Estimated savings:** Eliminates O(N) scan and list allocation on every DELETE. Under 10K pending entries, saves ~10K tuple visits and ~80KB of transient list allocation per delete.

---

### H3. StateMachine `hot_keys` aux state grows unbounded

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, lines 387-403

```elixir
def init_aux(_name), do: %{hot_keys: %{}}

def handle_aux(_raft_state, :cast, {:key_written, key}, aux, int_state) do
  count = Map.get(aux.hot_keys, key, 0)
  new_aux = %{aux | hot_keys: Map.put(aux.hot_keys, key, count + 1)}
  {:no_reply, new_aux, int_state}
end
```

**Current behavior:** The `hot_keys` map in aux state accumulates one entry per unique key that has ever been written. There is no eviction, no TTL, no size cap. For a workload with millions of unique keys, this map grows to millions of entries, each consuming ~100 bytes (key binary + integer + map overhead). At 1M keys, this is ~100MB of ra process heap memory that is never freed.

**Recommended fix:** Either:
1. Cap the map size (e.g., top-1000 keys by count, or reservoir sampling).
2. Periodically reset the map (e.g., every N minutes).
3. Remove the feature entirely if it is unused (I see no consumers of this data).

```elixir
def handle_aux(_raft_state, :cast, {:key_written, key}, aux, int_state) do
  hot = aux.hot_keys
  if map_size(hot) > 10_000 do
    # Reset to prevent unbounded growth
    {:no_reply, %{aux | hot_keys: %{key => 1}}, int_state}
  else
    count = Map.get(hot, key, 0)
    {:no_reply, %{aux | hot_keys: Map.put(hot, key, count + 1)}, int_state}
  end
end
```

**Estimated savings:** Bounds ra process memory growth. At 1M unique keys written, prevents ~100MB of wasted heap memory.

---

### H4. `build_local_store/1` captures entire Shard state in closures

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 1631-2103

**Current behavior:** `build_local_store/1` creates ~15 anonymous functions that each capture `state` (the full Shard GenServer state struct). Each closure holds a reference to the state struct, which includes the `promoted_instances` map (containing NIF resource references), the `pending` list, and all other state fields. Since these closures are passed to `execute_tx_commands` which may pass them to `Dispatcher.dispatch`, the state is kept alive as long as any closure reference exists.

The closures also capture `state` by value at the time `build_local_store` is called. The `ets_insert`, `ets_delete_key`, and `ets_lookup_warm` calls inside the closures use the captured `state`, which means mutations to state (like pending list changes) are NOT visible inside the closures -- but the stale state reference keeps old data alive.

**Recommended fix:** Extract only the needed fields from state:

```elixir
defp build_local_store(state) do
  my_idx = state.index
  keydir = state.keydir
  prefix_keys = state.prefix_keys
  data_dir = state.data_dir
  # ... use these minimal bindings in closures instead of `state`
end
```

**Estimated savings:** Reduces closure capture size from the entire state struct (~500 bytes + referenced data) to a few atoms/integers (~50 bytes). More importantly, prevents holding stale references to the pending list and other mutable state fields.

---

### H5. ETS keydir stores full value binaries for ALL hot keys

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, ETS 7-tuple format:
`{key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}`

**Current behavior:** When a key is "hot" (recently read/written), its full value binary is stored in position 2 of the ETS tuple. Every `:ets.lookup/2` copies the entire tuple -- including the value binary -- to the caller's process heap. For large values (e.g., 100KB JSON strings), a single GET operation copies 100KB from ETS to the connection process heap, then the RESP encoder creates another copy for the socket.

The Router's `ets_get/3` (line 178) also copies the value on every lookup, even for the LFU touch operation. The value is returned as part of `{:hit, value, exp}`, then discarded if the caller only needed to check existence.

**Recommended fix:** Consider a two-tier approach:
1. For values <= 64 bytes (heap binaries), keep in ETS (no extra cost, they're already copied).
2. For values > 64 bytes, store only the disk location in ETS and use a separate ETS table or process dictionary cache with explicit size limits. The cold-read path already handles this (`value = nil` in the 7-tuple).

Alternatively, add a `hot_cache_max_value_size` config that sets values above a threshold to nil in ETS immediately after write (keeping only the disk location). This is essentially forced cold-key treatment for large values.

**Estimated savings:** For a dataset with 10% of values > 10KB, this eliminates ~10KB * N_hot_reads of unnecessary binary copies per second. The exact savings depend on value size distribution.

---

### H6. MemoryGuard `reject_writes?` and `keydir_full?` use GenServer.call on hot path

**File:** `apps/ferricstore/lib/ferricstore/memory_guard.ex`, lines 99-113
**Called from:** `apps/ferricstore/lib/ferricstore/store/router.ex`, line 251 (`check_keydir_full`)
**Called from:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, line 951 (`reject_writes?`)

**Current behavior:** Every PUT operation calls `MemoryGuard.keydir_full?()` (via `Router.check_keydir_full/1`), which does a synchronous `GenServer.call` to the MemoryGuard process. This adds:
1. Message send/receive overhead (~1-5us)
2. Serialization through the MemoryGuard process mailbox (all PUTs contend)
3. An extra process heap allocation for the call tuple

At 100k PUT/s, this is 100k GenServer.call roundtrips/s to a single MemoryGuard process.

**Recommended fix:** Store the pressure level in `:persistent_term` or a dedicated `:atomics` reference:

```elixir
# In MemoryGuard.perform_check:
:persistent_term.put(:ferricstore_keydir_full, state.keydir_pressure_level == :reject)

# In Router.check_keydir_full:
defp check_keydir_full(key) do
  if :persistent_term.get(:ferricstore_keydir_full, false) do
    # only then do the expensive exists? check
  else
    :ok
  end
end
```

**Estimated savings:** Eliminates 1 GenServer.call per PUT operation. At 100k PUT/s, saves ~100k process message pairs/s and removes the MemoryGuard process as a contention bottleneck.

---

### H7. `do_append` in StateMachine creates intermediate binary via `<>` concatenation

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, line 667

```elixir
new_val = old_val <> suffix
```

**Also in shard.ex:** line 1263, 1297, 1834

**Current behavior:** APPEND command reads the old value, concatenates the suffix with `<>`, then writes the new value. The `<>` operator creates a new binary of size `old_size + suffix_size`, copying both. The old value was just read from ETS (already copied to process heap), and the new value will be written to ETS and Bitcask (more copies). For repeated APPENDs on the same key (e.g., building a log string), the cost is quadratic: append 1 copies 1KB, append 2 copies 2KB, etc.

**Recommended fix:** This is inherent to APPEND semantics (the new value must be a contiguous binary for storage). However, for the Raft state machine path, the value is read from ETS, concatenated, then immediately written back to ETS and Bitcask. Using iodata could defer the concatenation to the NIF layer:

```elixir
# If v2_append_batch could accept iodata:
new_val_iodata = [old_val, suffix]
# ... pass iodata to NIF which does the copy in Rust once
```

Since this requires NIF changes, a simpler mitigation: use `:binary.copy/1` on the result to ensure the new binary is a standalone allocation (not holding a reference to the old value's refc binary).

**Estimated savings:** Minor for single APPENDs. For repeated APPENDs building large values, prevents holding references to intermediate refc binaries.

---

## MEDIUM

### M1. Batcher accumulates commands and callers with prepend-then-reverse

**File:** `apps/ferricstore/lib/ferricstore/raft/batcher.ex`, lines 234-238

```elixir
updated_slot = %{
  slot
  | cmds: [command | slot.cmds],
    froms: [from | slot.froms],
    ...
}
```

And at flush time (line 320-321):
```elixir
batch = Enum.reverse(slot.cmds)
froms = Enum.reverse(slot.froms)
```

**Current behavior:** Each write adds a command tuple and a `from` reference to the head of two lists. At flush time, both lists are reversed. With `max_batch_size` of 1000, each flush creates two new lists of 1000 elements. The command tuples contain key and value binaries, which are refc binaries held alive by both the original list and the reversed list until GC.

**Recommended fix:** Use `:queue` module for O(1) append and O(1) dequeue, avoiding the reverse. Or, since the batch is consumed immediately after reverse, this is acceptable -- just ensure the slot is removed from `state.slots` promptly (which it is, at line 346).

**Estimated savings:** Low -- the reverse is O(N) but with small constant factor. The bigger concern is the brief doubling of list memory during reverse.

---

### M2. `l1_evict_and_put` is O(N) scan of entire L1 cache on every eviction

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 2298

```elixir
{evict_key, evict_entry} = Enum.min_by(state.l1_cache, fn {_, e} -> e.hits end)
```

**Current behavior:** When the L1 cache is full (64 entries by default), inserting a new entry requires scanning all 64 entries to find the one with the lowest hit count. At 64 entries this is fast, but the function recurses (line 2308-2309) if byte limits are also exceeded, potentially scanning multiple times.

**Recommended fix:** With only 64 entries, this is acceptable. If `@l1_max_entries` is ever increased, switch to a min-heap or maintain a sorted structure.

**Estimated savings:** Negligible at current L1 size. Worth watching if L1 grows.

---

### M3. `l1_invalidate_from_push` parses RESP binary on every invalidation

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 2332-2363

```elixir
binary = IO.iodata_to_binary(iodata)
case Parser.parse(binary) do ...
```

**Current behavior:** When a tracking invalidation push arrives, the connection process converts iodata to binary, then parses the RESP3 push message to extract key names for L1 removal. `IO.iodata_to_binary` creates a new binary (copy), and `Parser.parse` may allocate intermediate structures.

**Recommended fix:** Avoid re-parsing the RESP message. Instead, send the key list alongside the iodata in the invalidation message:

```elixir
# Instead of:
send(target_pid, {:tracking_invalidation, iodata})

# Send:
send(target_pid, {:tracking_invalidation, iodata, keys})
```

This avoids the binary copy and parse entirely.

**Estimated savings:** One binary allocation + RESP parse per invalidation push. Under high-write workloads with many tracking connections, this can be significant.

---

### M4. `String.split(value, ":")` for rate limiter state creates intermediate list

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, line 896

```elixir
case String.split(value, ":") do
  [cur, start, prev] -> ...
```

**Also in shard.ex:** line 3439

**Current behavior:** Every RATELIMIT.ADD command parses the stored counter state by splitting a string on `:`. `String.split` creates a list of sub-binaries. Since the stored value is a refc binary from ETS, each sub-binary is a sub-binary reference to the parent, preventing the parent from being GC'd until all sub-binaries are released.

**Recommended fix:** Use `:binary.split/2` which is more efficient, and apply `:binary.copy/1` to the extracted integers if they might hold references to a large parent binary. Or better, store rate limit state as a fixed-format binary (e.g., three 8-byte big-endian integers = 24 bytes) parsed with pattern matching:

```elixir
<<cur::64, start::64, prev::64>> = value
```

**Estimated savings:** Eliminates list allocation and sub-binary reference chains per RATELIMIT.ADD call.

---

### M5. Shard process dictionary usage for transaction state

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 1578, 1640-1643, 1658-1659, 1670-1672, 1696-1698, 1721-1723

```elixir
Process.put(:tx_deleted_keys, MapSet.new())
# ... many Process.get/Process.put calls ...
Process.delete(:tx_deleted_keys)
```

**Current behavior:** The 2PC `prepare_tx` handler stores a MapSet of deleted keys in the process dictionary. The process dictionary is not garbage collected incrementally -- entries are only cleaned up by `Process.delete` or when the process terminates. If `Process.delete` is missed (e.g., due to an exception in `execute_tx_commands`), the MapSet stays in the process dictionary forever.

More critically, the process dictionary entries are NOT part of the GenServer state and therefore NOT included in `:sys.get_state/1` -- making them invisible to monitoring tools.

**Recommended fix:** Move `tx_deleted_keys` into the GenServer state struct as a field that is set to `nil` outside of transactions. Wrap `execute_tx_commands` in try/after to ensure cleanup:

```elixir
try do
  results = execute_tx_commands(commands, local_store)
  ...
after
  # Always clean up
end
```

**Estimated savings:** Prevents potential process dictionary memory leaks. Improves debuggability.

---

### M6. `build_store` creates new function closures on every command dispatch

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 1930-2008 (`build_raw_store` and `build_store`)

**Current behavior:** Every command dispatched through the normal path calls `build_store(state.sandbox_namespace)`, which creates a new map of ~25 anonymous function closures. Each closure is a small heap allocation. For the `nil` namespace (common case), the closures are identical on every call but are re-allocated each time.

The sandboxed version (`build_store(ns)`) creates wrapper closures that prepend the namespace prefix.

**Recommended fix:** Cache the raw store map in `persistent_term` or in the connection state. Since `build_raw_store` always returns the same map of function captures (`&Router.get/1`, etc.), it can be computed once at module load time or at connection init:

```elixir
# Module attribute (computed at compile time):
@raw_store %{
  get: &Router.get/1,
  put: &Router.put/3,
  ...
}

defp build_store(nil), do: @raw_store
```

For sandbox mode, the wrapped store could be cached in the connection state and invalidated only when the namespace changes.

**Estimated savings:** Eliminates ~25 closure allocations per command. At 100k cmd/s, saves ~2.5M small heap allocations/s.

---

### M7. `Enum.chunk_every(args, 2)` for MSET/MSETNX creates intermediate lists

**File:** `apps/ferricstore/lib/ferricstore/commands/strings.ex`, lines 196, 417

```elixir
pairs = Enum.chunk_every(args, 2)
```

**Current behavior:** For MSET with N key-value pairs, `chunk_every` creates a list of N/2 two-element lists. For large MSET operations (e.g., MSET with 1000 key-value pairs), this allocates 500 two-element lists + the outer list.

**Recommended fix:** Use direct recursive processing to avoid the intermediate list:

```elixir
defp do_mset([], _store), do: :ok
defp do_mset([k, v | rest], store) do
  store.put.(k, v, 0)
  do_mset(rest, store)
end
```

**Estimated savings:** Eliminates N/2 list cell allocations for large MSET operations. Minor but adds up for bulk loading scenarios.

---

### M8. `maybe_check_type` in Strings.GET calls `:erlang.binary_to_term` on potential ETF binaries

**File:** `apps/ferricstore/lib/ferricstore/commands/strings.ex`, lines 610-624

```elixir
defp maybe_check_type(<<131, _rest::binary>> = value) do
  try do
    case :erlang.binary_to_term(value) do
```

**Current behavior:** Any value whose first byte is 131 (the Erlang External Term Format tag) triggers a full deserialization via `:erlang.binary_to_term`. This allocates the deserialized Elixir term on the heap, inspects it, and discards it. For large serialized lists/hashes/sets stored as ETF, this deserializes the entire structure just to return a WRONGTYPE error.

**Recommended fix:** Instead of deserializing the entire value, peek at the first few bytes to identify the ETF tag:

```elixir
defp maybe_check_type(<<131, 104, 2, _::binary>> = value) do
  # 131 = ETF, 104 = SMALL_TUPLE, 2 = arity 2
  # Peek at the atom tag to identify {:list, _}, {:hash, _}, etc.
  # without deserializing the payload.
  ...
end
```

Or better: use the TypeRegistry to check the key's type before reading the value at all. The `handle("GET", [key], store)` function already checks `compound_get` for data structure types -- if the type check happens first, the raw value is never read for non-string keys.

**Estimated savings:** For large data structures (e.g., a hash with 10K fields stored as ETF), avoids deserializing potentially MB of data just to return an error. Prevents transient multi-MB heap spikes on the connection process.

---

### M9. `apply_setrange_for_tx` uses byte lists for binary manipulation

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 2107-2118

```elixir
defp apply_setrange_for_tx(old, offset, value) do
  old_bytes = :binary.bin_to_list(old)
  val_bytes = :binary.bin_to_list(value)
  ...
  :binary.list_to_bin(head ++ val_bytes ++ rest)
end
```

**Current behavior:** Converts binaries to byte lists for SETRANGE manipulation, then converts back. A byte list uses one word per byte (8 bytes/byte on 64-bit), so a 10KB binary becomes an 80KB list. The `++` operator also copies the left operand.

**Recommended fix:** Use binary pattern matching (same as `apply_setrange/3` at line 3306 which correctly uses `binary_part`):

```elixir
defp apply_setrange_for_tx(old, offset, value) do
  apply_setrange(old, offset, value)  # reuse the efficient implementation
end
```

**Estimated savings:** For a 10KB value, eliminates ~160KB of transient list allocations (old_bytes + val_bytes as lists).

---

## LOW

### L1. No hibernation for idle Shard GenServers

**File:** `apps/ferricstore/lib/ferricstore/store/shard.ex`

**Current behavior:** Shard GenServers never hibernate. After a burst of activity, the GenServer heap remains at its high-water mark until the next GC cycle. The BEAM's generational GC for GenServers is triggered by message arrival, so an idle shard keeps its potentially large heap (accumulated during busy periods) indefinitely.

**Recommended fix:** Add `{:noreply, state, :hibernate}` to the expiry sweep handler when no keys were expired (indicating low activity). Hibernation triggers a full GC and shrinks the heap:

```elixir
# In handle_info(:expiry_sweep, state):
if count == 0 and state.pending == [] and state.flush_in_flight == nil do
  schedule_expiry_sweep()
  {:noreply, state, :hibernate}
else
  schedule_expiry_sweep()
  {:noreply, state}
end
```

**Estimated savings:** Reclaims heap memory during idle periods. Typical savings: 100KB-10MB per idle shard depending on prior activity.

---

### L2. MemoryGuard `compute_stats` iterates shard_count ETS tables every 100ms

**File:** `apps/ferricstore/lib/ferricstore/memory_guard.ex`, lines 331-363

**Current behavior:** Every 100ms, MemoryGuard iterates over all shard ETS tables calling `:ets.info(table, :memory)`. This is cheap (~1us per table) but creates small tuples on the MemoryGuard process heap. The `shard_stats` map accumulates per-shard statistics that are rebuilt every check cycle.

**Recommended fix:** This is acceptable at 4 shards. If shard count grows significantly, consider caching stats and only recomputing on threshold crossings.

**Estimated savings:** Negligible at current scale.

---

### L3. Connection struct holds `pubsub_channels` and `pubsub_patterns` as MapSets

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, lines 92-93

**Current behavior:** Every connection allocates two empty MapSets (`MapSet.new()`), even for connections that never use pub/sub. An empty MapSet is a struct wrapping an empty map -- about 40 bytes per MapSet.

**Recommended fix:** Initialize to `nil` and only create MapSets when SUBSCRIBE/PSUBSCRIBE is first used. Update `in_pubsub_mode?` to handle nil:

```elixir
defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false
defp in_pubsub_mode?(state) do
  MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0
end
```

**Estimated savings:** ~80 bytes per connection. At 10K connections, saves ~800KB.

---

### L4. LFU.initial/0 is called on every ETS insert, performing System.os_time

**File:** `apps/ferricstore/lib/ferricstore/store/lfu.ex`, lines 37-40

```elixir
def initial do
  pack(now_minutes(), @initial_counter)
end
```

**Current behavior:** Every `ets_insert` in the Shard, StateMachine, and AsyncApplyWorker calls `LFU.initial()`, which calls `System.os_time(:second)`. While `System.os_time` is fast (~50ns), the packed value changes only once per minute. Calling it per-insert is wasteful.

**Recommended fix:** Cache the packed initial value in a process-local variable or `:persistent_term`, updating it once per minute:

```elixir
# Use a module attribute + periodic refresh:
@spec cached_initial() :: non_neg_integer()
def cached_initial do
  case :persistent_term.get(:lfu_initial_cached, nil) do
    {packed, minute} ->
      current_minute = now_minutes()
      if current_minute == minute, do: packed, else: refresh_cache(current_minute)
    nil ->
      refresh_cache(now_minutes())
  end
end
```

**Estimated savings:** Saves ~50ns per write operation. At 100k writes/s, saves ~5ms/s of CPU time.

---

### L5. Connection `multi_queue` uses list append (O(N) per command)

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 1268

```elixir
new_queue = state.multi_queue ++ [{cmd, args}]
```

**Current behavior:** Each command queued during MULTI appends to the end of the list, which is O(N). For a transaction with 100 commands, this copies the list 100 times.

**Recommended fix:** Prepend and reverse at EXEC time:

```elixir
# Queue:
new_queue = [{cmd, args} | state.multi_queue]

# At EXEC:
commands = Enum.reverse(state.multi_queue)
```

**Estimated savings:** Changes queuing from O(N^2) to O(N) for N-command transactions. For 100-command transactions, eliminates ~5050 list cell copies.

---

### L6. Promotion `recover_promoted` does `:ets.foldl` to find PM: markers

**File:** `apps/ferricstore/lib/ferricstore/store/promotion.ex`, lines 140-153

**Current behavior:** At shard startup, `recover_promoted` scans the entire ETS keydir to find promotion markers (keys starting with `PM:`). This is a one-time cost per startup, but for large keydirs it delays shard availability.

**Recommended fix:** Use `:ets.select` with a match spec for the `PM:` prefix:

```elixir
match_spec = [
  {{<<"PM:", :"$1"/binary>>, :"$2", :_, :_, :_, :_, :_},
   [{:is_binary, :"$2"}],
   [{{:"$1", :"$2"}}]}
]
```

Or maintain a separate list of promoted keys in the GenServer state.

**Estimated savings:** One-time improvement at startup. Reduces startup time for large keydirs.

---

### L7. `format_float` creates intermediate strings via `String.trim_trailing`

**File:** `apps/ferricstore/lib/ferricstore/raft/state_machine.ex`, lines 645-656
**Also:** `apps/ferricstore/lib/ferricstore/store/shard.ex`, lines 3284-3301

```elixir
formatted
|> String.trim_trailing("0")
|> String.trim_trailing(".")
|> then(fn s -> s end)
```

**Current behavior:** Formats a float, then applies two `String.trim_trailing` operations (each potentially creating a new binary) plus a no-op `then`. The `then(fn s -> s end)` is dead code that allocates a closure and calls it.

**Recommended fix:** Remove the no-op `then`, and combine the trims:

```elixir
defp format_float(val) when is_float(val) do
  formatted = :erlang.float_to_binary(val, [:compact, {:decimals, 17}])
  if String.contains?(formatted, ".") do
    formatted |> String.trim_trailing("0") |> String.trim_trailing(".")
  else
    formatted
  end
end
```

**Estimated savings:** Eliminates one closure allocation and one function call per INCRBYFLOAT.

---

## Summary

| Severity | Count | Key themes |
|----------|-------|------------|
| CRITICAL | 3 | Application.get_env on hot path; full ETS scans in GenServer; buffer binary concatenation |
| HIGH | 7 | Unbounded pending list; unbounded aux state; GenServer.call for pressure check; large value copies in ETS; closure state capture |
| MEDIUM | 9 | Store map re-creation per command; byte-list manipulation; unnecessary deserialization; intermediate list allocations |
| LOW | 7 | No hibernation; MapSet waste; list append in MULTI; no-op code; cached LFU initial |

**Highest-impact quick wins (effort vs. savings):**
1. **C1** -- persistent_term for LFU config (5 min fix, eliminates 200k+ ETS lookups/s)
2. **H6** -- persistent_term for keydir_full flag (10 min fix, eliminates GenServer contention on every PUT)
3. **M6** -- Cache raw store map as module attribute (10 min fix, eliminates 25 closures/command)
4. **L5** -- Prepend instead of append for MULTI queue (2 min fix, O(N^2) to O(N))
5. **C2** -- ETS match spec or secondary index for compound operations (1-2 hour fix, eliminates full-table scans)
