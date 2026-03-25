# NIF Integration Boundary Audit

Audit date: 2026-03-21
Files examined: `nif.ex`, `async.ex`, `shard.ex`, `state_machine.ex`, `async_apply_worker.ex`, `router.ex`, `bloom.ex`, `cuckoo.ex`, `cms.ex`, `lib.rs`

---

## 1. Summary

The NIF boundary is generally well-engineered. The v2 architecture (pure stateless NIFs with Elixir-owned ETS keydir) is a sound design that avoids the Mutex-contention and use-after-free risks inherent in resource-based NIFs. Key strengths:

- All v2 NIFs are stateless: no shared Rust-side keydir or Store resource needed for the primary write/read path.
- Zero-copy reads via `ResourceArc<ValueBuffer>::make_binary` avoid memcpy on the hot read path.
- Correlation IDs on async v2 NIFs fix the LIFO ordering bug from the old list-based approach.
- Yielding NIFs for `keys`, `get_all`, `get_batch`, `get_range` prevent scheduler monopolization.
- `v2_append_batch_nosync` + deferred `v2_fsync_async` amortizes fsync cost across batch windows.

However, this audit identifies **12 issues** ranging from medium-severity error handling gaps to minor performance concerns.

---

## 2. Error Handling Issues

### 2.1 CRITICAL: `v2_append_tombstone` return value discarded in multiple locations

The NIF `v2_append_tombstone` returns `{:ok, {offset, record_size}}` or `{:error, reason}`, but every call site discards the return value. If the tombstone write fails (disk full, permission error), the key is deleted from ETS but NOT from disk, causing the key to resurrect on restart.

**Locations:**

- `state_machine.ex:582` — `do_delete/2`:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, key)
  :ets.delete(state.ets, key)
  ```

- `shard.ex:1417` — `handle_call({:delete, key}, ...)`:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, key)
  ets_delete_key(state, key)
  ```

- `shard.ex:634` — `handle_compound_delete_direct`:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, compound_key)
  ```

- `shard.ex:1266` — `handle_getdel_direct`:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, key)
  ```

- `shard.ex:2137` — `handle_unlock_direct`:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, key)
  ```

- `shard.ex:2318` — `handle_list_op_direct` delete closure:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, key)
  ```

- `shard.ex:2385` — `handle_list_op_lmove_direct` src_delete closure:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, src_key)
  ```

- `shard.ex:2539` — `handle_info({:tx_pending_delete, key}, ...)`:
  ```elixir
  NIF.v2_append_tombstone(state.active_file_path, key)
  ```

- `async_apply_worker.ex:243` — `apply_commands` delete each:
  ```elixir
  NIF.v2_append_tombstone(active_file_path, key)
  ```

**Recommended fix:** Check the return value. If `{:error, _}`, log the failure and do NOT delete from ETS. In the Raft state machine (`state_machine.ex`), returning `{:error, reason}` instead of `:ok` would propagate the failure to the caller.

### 2.2 MEDIUM: `NIF.put` return value discarded for promoted Bitcask writes

In `shard.ex`, promoted compound keys write directly to a dedicated Bitcask store using `NIF.put/4`. The return value is discarded in multiple locations:

- `shard.ex:563` — `handle_compound_put_raft` promoted path:
  ```elixir
  NIF.put(dedicated, compound_key, value, expire_at_ms)
  {:reply, :ok, state}
  ```

- `shard.ex:591` — `handle_compound_put_direct` promoted path:
  ```elixir
  NIF.put(dedicated, compound_key, value, expire_at_ms)
  {:reply, :ok, state}
  ```

**Recommended fix:** Pattern-match on the result and propagate `{:error, reason}` to the caller.

### 2.3 MEDIUM: `NIF.delete` return value discarded for promoted Bitcask deletes

- `shard.ex:621` — `handle_compound_delete_raft` promoted path:
  ```elixir
  NIF.delete(dedicated, compound_key)
  ets_delete_key(state, compound_key)
  ```

- `shard.ex:647` — `handle_compound_delete_direct` promoted path:
  ```elixir
  NIF.delete(dedicated, compound_key)
  ets_delete_key(state, compound_key)
  ```

`NIF.delete/2` returns `{:ok, true}`, `{:ok, false}`, or `{:error, reason}`. An error would leave ETS and Bitcask inconsistent.

### 2.4 MEDIUM: `flush_pending` and `flush_pending_sync` silently swallow errors

- `shard.ex:2751-2753` — `flush_pending`:
  ```elixir
  {:error, _reason} ->
    state
  ```

- `shard.ex:2780-2782` — `flush_pending_sync`:
  ```elixir
  {:error, _reason} ->
    state
  ```

When `v2_append_batch` or `v2_append_batch_nosync` fails, the pending list is NOT cleared. This means writes are still in ETS (visible to reads) but not on disk. On restart, those writes are lost. The pending list will accumulate indefinitely until the next successful flush.

**Recommended fix:** At minimum, log the error with `Logger.error`. Consider: if write fails due to disk full, should the pending list be retained or cleared? Retaining it risks OOM; clearing it risks data loss. The safest approach is to log and retain (for retry on next timer tick), which is what the current code does, but the silent swallow makes debugging impossible.

### 2.5 LOW: `v2_copy_records` return value not checked during compaction

- `shard.ex:2480`:
  ```elixir
  NIF.v2_copy_records(source, dest, offsets)
  File.rename!(dest, source)
  ```

If `v2_copy_records` returns `{:error, reason}`, the `File.rename!` would rename a potentially incomplete or nonexistent file over the source, causing data loss.

**Recommended fix:** Check the NIF result before renaming:
```elixir
case NIF.v2_copy_records(source, dest, offsets) do
  {:ok, _results} -> File.rename!(dest, source)
  {:error, reason} -> Logger.error("Compaction failed for #{source}: #{reason}")
end
```

### 2.6 LOW: Bare pattern match on NIF results in CMS module

- `cms.ex:64`:
  ```elixir
  {:ok, counts} = NIF.cms_incrby(ref, items)
  ```

- `cms.ex:74`:
  ```elixir
  {:ok, counts} = NIF.cms_query(ref, elements)
  ```

- `cms.ex:98`:
  ```elixir
  {:ok, {width, depth, count}} = NIF.cms_info(ref)
  ```

- `cms.ex:132`:
  ```elixir
  {:ok, ref} = NIF.cms_create_file(path, width, depth)
  ```

- `cms.ex:138`:
  ```elixir
  {:ok, [min_count]} = NIF.cms_incrby(ref, [{element, count}])
  ```

- `cms.ex:144`:
  ```elixir
  {:ok, [count]} = NIF.cms_query(ref, [element])
  ```

- `cms.ex:150`:
  ```elixir
  {:ok, info} = NIF.cms_info(ref)
  ```

- `cms.ex:249`:
  ```elixir
  {:ok, {ew, ed, _}} = NIF.cms_info(existing_ref)
  ```

- `cms.ex:261`:
  ```elixir
  :ok = NIF.cms_merge(dest_ref, sources)
  ```

These bare pattern matches will raise `MatchError` (crashing the calling process) if the NIF returns `{:error, reason}`. While the NIF resource has already been validated to exist, disk I/O errors during merge or query could still occur.

**Recommended fix:** Use `case` statements and propagate errors:
```elixir
case NIF.cms_incrby(ref, items) do
  {:ok, counts} -> counts
  {:error, reason} -> {:error, "ERR CMS operation failed: #{reason}"}
end
```

### 2.7 LOW: `bloom.ex:168` bare pattern match on `NIF.bloom_info`

```elixir
{:ok, {num_bits, count, num_hashes}} = NIF.bloom_info(resource)
```

Same issue as 2.6 — will crash on NIF error.

### 2.8 LOW: `cuckoo.ex:163-164` bare pattern match on `NIF.cuckoo_info`

```elixir
{:ok, {num_buckets, bucket_size, fingerprint_size,
       num_items, num_deletes, total_slots, max_kicks}} = NIF.cuckoo_info(resource)
```

Same issue as 2.6.

---

## 3. Type Safety Issues

### 3.1 MEDIUM: Hint file field order mismatch -- offset passes through u32 field

The NIF `v2_write_hint_file` (`lib.rs:2124`) expects entries as `{key, file_id, offset, value_size, expire_at_ms}` where the 4th positional element is declared `u32`.

But `shard.ex:2853` passes entries as `{key, 0, exp, off, vsize}`:
- pos 2 (`file_id` in NIF) = `0` -- OK
- pos 3 (`offset` in NIF) = `exp` (expire_at_ms from ETS)
- pos 4 (`value_size` in NIF, u32) = `off` (actual file offset, **truncated to u32**)
- pos 5 (`expire_at_ms` in NIF) = `vsize` (actual value size)

The Rust NIF writes a `HintEntry` struct with these swapped semantics. On read, `v2_read_hint_file` (`lib.rs:2173-2178`) returns `{key, entry.file_id, entry.offset, entry.value_size, entry.expire_at_ms}`, which yields `{key, 0, exp, off_truncated_to_u32, vsize}`.

Elixir reads at `shard.ex:279` as `{key, _tstamp, expire_at_ms, offset, value_size}`:
- `_tstamp` = 0 (ignored)
- `expire_at_ms` = exp (correct -- roundtripped through `offset` u64 field)
- `offset` = off (correct **only if < 2^32** -- roundtripped through `value_size` u32 field)
- `value_size` = vsize (correct -- roundtripped through `expire_at_ms` u64 field)

**The roundtrip is accidentally self-consistent for files under 4GB.** However, the file offset passes through a u32 field, silently truncating offsets >= 2^32. With `@max_active_file_size` at 256MB, current files are safe, but if max size is ever raised or a non-rotated file grows past 4GB, hint-based recovery would produce wrong offsets.

**Recommended fix:** Change `shard.ex:2853` to pass fields in the NIF's expected order:
```elixir
[{key, target_fid, off, vsize, exp} | acc]
```
And change `shard.ex:279` to destructure in the NIF's return order:
```elixir
{key, _file_id, offset, value_size, expire_at_ms}
```

### 3.2 LOW: `v2_append_record` and `v2_append_tombstone` are on DirtyIo but `v2_append_batch` is also DirtyIo

At `lib.rs:1861` and `lib.rs:1899`, `v2_append_record` and `v2_append_tombstone` run on `schedule = "DirtyIo"`. Meanwhile `v2_append_batch` at `lib.rs:1923` also uses `schedule = "DirtyIo"`. This is the correct choice since they perform fsync, but it means the Raft state machine's synchronous writes consume DirtyIo threads. The primary write path uses `v2_append_batch_nosync` (which correctly uses `schedule = "Normal"` at `lib.rs:2434`).

With 4 shards each potentially writing via Raft state machine, plus compaction and hint writes, up to 6+ DirtyIo threads could be in use simultaneously out of the default 10. This is not a bug but an operational awareness item.

---

## 4. Resource Lifecycle Issues

### 4.1 MEDIUM: Legacy Store resource (v1 NIFs) still in use for promoted Bitcask

Promoted compound data structures still use the old `StoreResource`-based NIFs (`NIF.put`, `NIF.delete`, `NIF.get_zero_copy`, `NIF.get_all`, `NIF.keys`). These hold a `Mutex<Store>` inside a `ResourceArc`. The lifecycle risks are:

1. **Use-after-close:** There is no explicit `close` NIF for `StoreResource`. The resource is freed when all Erlang references are GC'd. If the Elixir process holding the reference crashes, the resource is cleaned up by BEAM GC. This is safe.

2. **Mutex poisoning:** If a NIF panics while holding the store Mutex, subsequent calls will encounter a poisoned Mutex. The H-NEW-1 fix handles this with `unwrap_or_else(|e| e.into_inner())` in async NIFs, but synchronous NIFs (`get`, `put`, `delete`, `get_zero_copy`) use `.map_err(|_| rustler::Error::BadArg)?` which returns `BadArg` -- not an informative error.

3. **Promoted store deletion:** When a promoted key is deleted (`Promotion.cleanup_promoted!`), the Elixir code drops its reference to the `StoreResource`. But if any in-flight NIF call still holds a `ResourceArc` clone (e.g., an async Tokio task), the store stays alive until the task completes. This is safe due to `ResourceArc` reference counting.

### 4.2 LOW: Mmap resources (bloom, cuckoo, CMS, tdigest, topk, HNSW) lack process-scoped cleanup

All mmap-backed NIF resources (`BloomResource`, `CuckooResource`, `CmsResource`, etc.) are cached in per-shard ETS tables via their respective registries. If the shard GenServer crashes:

1. The ETS table is recreated on restart (shard init creates-or-clears tables).
2. But the old NIF resource references become unreachable Erlang terms.
3. The BEAM GC will eventually drop them, which calls the Rust destructor (munmap + close).
4. Meanwhile, recovery re-opens the files, creating new mmap regions for the same files.

There is a brief window where two mmap regions point to the same file. This is safe on most OSes (multiple mmaps of the same file share pages), but could cause issues if the destructor unlocks or deletes the file. The `bloom_delete` NIF explicitly unlinks the file -- if this runs during recovery, the newly-opened bloom filter would lose its backing file.

**Recommended fix:** The bloom/cuckoo/cms registries should explicitly close all cached resources during shard init (before `recover`) to prevent the GC race.

---

## 5. Async Correctness Issues

### 5.1 MEDIUM: v1 async NIFs (Async module) lack correlation IDs — LIFO ordering risk

The `Ferricstore.Bitcask.Async` module (used by promoted stores) sends `{:tokio_complete, :ok, value}` messages WITHOUT correlation IDs. If two concurrent async operations complete out of order, the first `receive` will match the wrong response.

Example scenario:
1. Process calls `Async.get(store, "key1")`, NIF spawns Tokio task
2. Before receiving response, process calls `Async.get(store, "key2")`
3. Tokio task for "key2" completes first
4. The first `receive` in step 1 matches `{:tokio_complete, :ok, "value_of_key2"}`
5. Caller gets wrong value for "key1"

This is mitigated because each `Async.get` call blocks on `receive` (sequential, not concurrent), but any code that calls multiple async NIFs concurrently on the same process would hit this.

The v2 async NIFs correctly use correlation IDs (`{:tokio_complete, corr_id, :ok, value}`). The v1 module should be migrated or deprecated.

### 5.2 LOW: Tokio task panic sends no message to caller

In the Tokio async NIFs (e.g., `get_async` at `lib.rs:1639`), if the spawned task panics:
```rust
async_io::runtime().spawn(async move {
    // ... if this panics ...
    let _ = msg_env.send_and_clear(&pid, |env| ...);
});
```

The `spawn` returns a `JoinHandle` that is immediately dropped. If the task panics, no message is sent to the caller. The caller's `receive` will hang until the timeout (5000ms default).

This is acceptable since: (a) panics inside Tokio tasks are caught by Tokio and logged, (b) the Elixir Async module has a timeout that returns `{:error, :timeout}`. But the 5-second timeout is unnecessarily long for what should be an immediate detection.

### 5.3 LOW: `v2_pread_at_async` NIF returns `:ok` but `Async.v2_pread_at` asserts it

At `async.ex:186`:
```elixir
:ok = NIF.v2_pread_at_async(self(), corr_id, path, offset)
```

This bare pattern match will crash if the NIF returns `{:error, reason}`. But looking at the Rust code (`lib.rs:2316`), `v2_pread_at_async` always returns `:ok` (it spawns a Tokio task and returns immediately). The `{:error, ...}` case would only occur if `nif_not_loaded`. This is acceptable since NIF-not-loaded is a fatal startup condition, but a `case` would be more defensive:
```elixir
case NIF.v2_pread_at_async(self(), corr_id, path, offset) do
  :ok -> receive do ... end
  other -> {:error, {:nif_error, other}}
end
```

Same issue at `async.ex:207` and `async.ex:226`.

---

## 6. Performance Issues

### 6.1 MEDIUM: `v2_append_record` (DirtyIo) used in hot path for list operations

In `shard.ex:2309` (`handle_list_op_direct`):
```elixir
case NIF.v2_append_record(state.active_file_path, key, encoded_binary, 0) do
```

`v2_append_record` runs on `schedule = "DirtyIo"` and performs open + write + fsync for a single record. For list operations (LPUSH, RPUSH, etc.), this is called once per operation. The batch write path (`v2_append_batch` or `v2_append_batch_nosync`) would be more efficient, but list operations need the offset returned to update ETS.

**Recommended fix:** Use `v2_append_batch` with a single-element list to get the same offset back, or accumulate list writes into the pending batch like string operations do.

### 6.2 LOW: `AsyncApplyWorker.get_active_file` uses `:sys.get_state`

At `async_apply_worker.ex:271`:
```elixir
state = :sys.get_state(shard_name)
{state.active_file_id, state.active_file_path}
```

`:sys.get_state/1` sends a synchronous `$gen_call` to the shard GenServer and waits for the full state struct. This is called on every batch. The full state includes the pending list, promoted_instances map, staged_txs map, etc. -- potentially large data.

**Recommended fix:** Add a dedicated `{:get_active_file}` GenServer call that returns only `{file_id, file_path}`, or store active file info in a shared location (e.g., persistent_term or a dedicated ETS table).

### 6.3 LOW: `compound_scan` in promoted path calls `NIF.get_all(dedicated)`

At `shard.ex:666`:
```elixir
case NIF.get_all(dedicated) do
```

This is the yielding NIF that scans all key-value pairs in the promoted Bitcask. For large promoted data structures (e.g., a hash with 100K fields), this reads all values from disk even though only the prefix-matching subset is needed. The result is then filtered by prefix in Elixir.

**Recommended fix:** Use a `get_range` NIF with the prefix as min_key, or add a prefix-scan NIF.

---

## 7. Scheduling Issues

### 7.1 NOTE: `v2_append_batch` on DirtyIo is correct but limits concurrency

`v2_append_batch` (with fsync) at `lib.rs:1923` uses `schedule = "DirtyIo"`. This is correct because fsync blocks. However, the default DirtyIo pool has 10 threads. With 4 shards each potentially calling `v2_append_batch` via the Raft state machine, plus compaction, hint writing, and tombstone writes also on DirtyIo, there could be contention.

Current DirtyIo users: `v2_append_record`, `v2_append_tombstone`, `v2_append_batch`, `v2_fsync`, `v2_write_hint_file`, `v2_copy_records`.

This is not a bug but an operational concern. The `v2_append_batch_nosync` + deferred `v2_fsync_async` path (Normal scheduler + Tokio) avoids this entirely and is used for the hot write path.

---

## 8. Recommendations (Prioritized)

### P0 — Data Integrity

1. **Fix `v2_append_tombstone` error handling** (Issue 2.1): Check return value at all call sites. In `state_machine.ex`, return `{:error, reason}` if the tombstone write fails. In `shard.ex`, do not delete from ETS if the NIF fails.

2. **Fix hint file field order** (Issue 3.1): Change `shard.ex:2849-2860` `write_hint_for_file` to pass `{key, target_fid, off, vsize, exp}` instead of `{key, 0, exp, off, vsize}`. Update `shard.ex:279` to destructure as `{key, _file_id, offset, value_size, expire_at_ms}`.

3. **Fix `v2_copy_records` error handling** (Issue 2.5): Check return before `File.rename!`.

### P1 — Reliability

4. **Add error logging to `flush_pending` and `flush_pending_sync`** (Issue 2.4): Log the reason so disk-full or permission errors are visible.

5. **Fix bare pattern matches in CMS/Bloom/Cuckoo** (Issues 2.6-2.8): Replace `{:ok, ...} = NIF.foo(...)` with `case` expressions.

6. **Check `NIF.put` and `NIF.delete` return values for promoted stores** (Issues 2.2-2.3).

### P2 — Performance

7. **Migrate list operations from `v2_append_record` to batch path** (Issue 6.1).

8. **Replace `:sys.get_state` in AsyncApplyWorker** (Issue 6.2).

9. **Add prefix-scan NIF for promoted compound_scan** (Issue 6.3).

### P3 — Cleanup

10. **Deprecate v1 `Async` module** (Issue 5.1): Migrate promoted stores to v2 path or add correlation IDs.

11. **Explicit mmap resource cleanup on shard restart** (Issue 4.2).

12. **Add `catch_all` to v2 async NIF bare matches in `async.ex`** (Issue 5.3).
