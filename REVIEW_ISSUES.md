# FerricStore Code Review — Functionality Issues

## CRITICAL (data loss, crashes, or consensus violations)

### C1: Compaction result type mismatch — merge scheduler crashes on every merge
**File:** `merge/scheduler.ex:286-317`, `store/shard.ex:2518-2551`
**Status:** FIXED (commit e661d13)

`do_merge` expects `run_compaction` to return `{:ok, {written, dropped, reclaimed}}`, but the shard returns `{:reply, :ok, state}`. The pattern match at line 291 has no matching clause — every merge attempt crashes the scheduler. The semaphore is never released, causing a permanent deadlock on future merges.

**Fix:** `run_compaction` now computes and returns `{:ok, {written, dropped, reclaimed}}` with real stats. Files with zero live entries are deleted entirely.

### C2: Leftover compaction temp files crash shard startup
**File:** `store/shard.ex:295-318, 2527`
**Status:** FIXED (commit 098b600)

`discover_active_file` calls `String.to_integer` on all `.log` filenames. Compaction creates `compact_N.log` temp files. If a crash leaves one behind, `String.to_integer("compact_5")` raises ArgumentError, crashing the shard on restart. The shard cannot recover without manual file deletion.

**Fix:** On startup, `discover_active_file` deletes any `compact_*.log` files and skips them in the file_id scan. They're always incomplete — if compaction had finished, the rename would have replaced the original.

### C3: Cross-shard transaction pending writes not isolated per shard
**File:** `raft/state_machine.ex:212-246`

`cross_shard_tx` uses a shared process-level `:sm_pending_writes` buffer. When processing commands for shard 0 then shard 1, writes from shard 0 may be flushed using shard 1's `active_file_path`. Writes end up in the wrong shard's log file.

**Proposed fix:** Flush `:sm_pending_writes` after each shard's batch inside the `Enum.reduce`, not once at the end. Each shard's commands should be flushed with that shard's `active_file_path` before moving to the next shard.

### C4: Batcher pending map not drained on crash/restart
**File:** `raft/batcher.ex:132-141`

No `terminate/2` callback. If the Batcher process is killed, all callers in `state.pending` and `state.flush_waiters` hang forever waiting for `GenServer.reply`. Connection processes accumulate and never unblock.

**Proposed fix:** Add `terminate/2` that iterates `state.pending` and `state.flush_waiters`, replying `{:error, :batcher_terminated}` to each caller. GenServer guarantees `terminate` is called on normal shutdown; for brutal kills, callers will hit their existing GenServer.call timeout.

### C5: No fsync after BitcaskWriter flush
**File:** `store/bitcask_writer.ex:283-320`

BitcaskWriter uses `v2_append_batch_nosync` — data goes to page cache, not disk. There's no coordinated fsync between BitcaskWriter and the Shard timer. If the system crashes between flush and fsync (~1ms window), async writes are lost despite returning `:ok` to the client.

**Proposed fix:** This is an accepted trade-off for async durability mode — the same as Redis's AOF with `appendfsync everysec`. The 1ms window is documented. For quorum mode, writes go through Raft which fsyncs the WAL. No code change needed, but document the async durability window explicitly in the module doc.

---

## HIGH (incorrect behavior, wrong results)

### H1: Promoted write file_id race with file rotation
**File:** `store/shard.ex:638-649, 672-683`

After `promoted_write` returns `{:ok, {offset, record_size}}`, the code calls `promoted_active_fid(dedicated_path)` which does `File.ls`. Between the write and the fid lookup, another write could trigger compaction/rotation, making the fid stale. ETS then points to the wrong file.

**Proposed fix:** `promoted_write` should return the file_id it wrote to (derived from the path it used), not a separate `File.ls` call after the fact. Change `promoted_write` to return `{:ok, {fid, offset, record_size}}` where fid is parsed from the path it wrote to. Remove `promoted_active_fid` entirely.

### H2: volatile_lru and allkeys_lru eviction don't implement actual LRU
**File:** `memory_guard.ex:343, 361-363`

The case statement has explicit sorting for `volatile_lfu`, `allkeys_lfu`, and `volatile_ttl`, but `volatile_lru` and `allkeys_lru` fall through to a default that just takes the first 5 entries from ETS fold order. This is arbitrary, not LRU. The LFU `ldt` field (last access time) exists but isn't used for sorting.

**Proposed fix:** Add explicit case clauses for `volatile_lru` and `allkeys_lru` that sort by `ldt` (last decrement time) ascending via `LFU.unpack(lfu) |> elem(0)`. Evict the entries with the oldest access time first.

### H3: compound_get_meta hardcodes expiry=0 when reading from promoted store
**File:** `store/shard.ex:592-604`

When a promoted key miss falls through to `promoted_read`, the result is inserted into ETS with `ets_insert(state, compound_key, value, 0)` — always 0 expiry. The actual expiry from disk is lost. HTTL/HPTTL return -1 (no expiry) instead of the real TTL.

**Proposed fix:** Change `promoted_read` to return `{:ok, value, expire_at_ms}` instead of `{:ok, value}`. The Bitcask record on disk contains the expiry — `v2_pread_at` should return it (or add a `v2_pread_with_meta` NIF that returns `{value, expire_at_ms}`). Pass the real expiry to `ets_insert`.

### H4: ets_lookup matches cold entries with fid=0, off=0
**File:** `store/shard.ex:3194-3196`

Pattern `[{^key, nil, 0, _lfu, fid, off, vsize}]` matches not-yet-flushed large values (which have fid=0, off=0, vsize=0). The code then attempts `pread_at(path, 0)` which reads the file header instead of data. Recently-written large values return nil on read.

**Proposed fix:** Add guards to the cold read pattern: `when fid > 0 and vsize > 0`. Entries with `fid=0, off=0, vsize=0` are pending writes, not cold entries — treat them as `:miss` and fall through to the GenServer path which will `await_in_flight` and return the value.

### H5: EXPIREAT/PEXPIREAT accept past timestamps
**File:** `commands/expiry.ex:112-124`

`EXPIREAT key 1000` (where now=2000) returns 1 (success) instead of immediately deleting the key and returning 0. The key lingers with a past expiry until lazy deletion or sweep. Redis deletes immediately for past timestamps.

**Proposed fix:** In `set_expiry_at_seconds` and `set_expiry_at_ms`, check if the timestamp is in the past (`ts_ms <= System.os_time(:millisecond)`). If so, call `store.delete.(key)` and return 1 (Redis behavior: past EXPIREAT deletes the key and returns 1 if the key existed). Same pattern as `set_expiry_seconds` which already checks `secs > 0`.

### H6: Batch applied result count mismatch silently truncates
**File:** `raft/batcher.ex:339-351`

`Enum.zip(froms, results)` silently truncates if the results list is shorter than the froms list. Remaining callers never receive a reply and hang forever.

**Proposed fix:** Before zipping, check `length(results) == length(froms)`. If mismatch, log error and reply `{:error, :batch_result_mismatch}` to all callers. This is defensive — the mismatch shouldn't happen if the StateMachine is correct, but it prevents silent hangs if it does.

### H7: Promoted collection recovery — partial output not cleaned
**File:** `store/promotion.ex:202-207`

`recover_promoted` doesn't check for partial compaction files in dedicated directories. If compaction crashed mid-write, partial files are scanned and may contain incomplete or duplicate entries.

**Proposed fix:** In `list_log_files`, filter out `compact_*.log` files and delete them — same pattern as the shared shard fix in C2. Dedicated directories should also get cleanup on startup.

---

## MEDIUM (degraded behavior, missing guarantees)

### M1: Async retry buffer doesn't preserve command order
**File:** `raft/async_apply_worker.ex:349-389`

Failed commands are prepended to `retry_buffer`. On retry, they're processed in reverse order of submission. If command A was submitted before B, but A fails and B succeeds, the retry of A happens after B commits. This breaks causal ordering for async writes.

**Proposed fix:** Append to retry_buffer instead of prepend (`retry_buffer ++ [{commands, retry_count}]`), or use `:queue` for O(1) append. Alternatively, since async writes already accept eventual consistency, document that retry ordering is best-effort and causal ordering is not guaranteed.

### M2: warm_ets_after_cold_read bypasses hot cache size limit
**File:** `store/router.ex:954-963`

After a cold read, the full value is cached in ETS regardless of size. The `value_for_ets/1` function (which returns nil for >65KB values) is not called. Large values get stored in ETS, defeating the hot cache size limit and causing memory bloat.

**Proposed fix:** Wrap the value through `value_for_ets/1` before updating ETS. If the value exceeds the threshold, skip the ETS update entirely (the cold entry with offset is already correct).

### M3: Compound scan for promoted keys relies entirely on ETS
**File:** `store/shard.ex:762-776`

HGETALL/SMEMBERS/ZRANGE on promoted collections scan ETS only. If recovery was incomplete (pread failed for some entries), those entries have value=nil in ETS and appear missing in scan results. No fallback to disk scan.

**Proposed fix:** During `prefix_scan_entries`, for entries with value=nil, do a `pread_at` to fetch the value from disk before including in results. This is the same cold-read pattern used for regular keys.

### M4: Hint file recovery ignores NIF-returned file_id
**File:** `store/shard.ex:344-349`

Recovery from hint files uses the filename-derived fid (`hint_name |> trim(".hint") |> to_integer()`) and ignores `_file_id` returned by the NIF. If the hint file's internal file_id doesn't match the filename (e.g., after a copy or partial write), cold reads look in the wrong file.

**Proposed fix:** Use the NIF-returned `file_id` instead of the filename-derived one. If they differ, log a warning. The hint file's internal metadata is authoritative — the filename is just a convention.

### M5: run_compaction returns :ok without statistics
**File:** `store/shard.ex:2518-2551`
**Status:** FIXED (commit e661d13)

Compaction never computes or returns bytes_reclaimed, records_written, or records_dropped. The scheduler can't track merge effectiveness. `total_bytes_reclaimed` in status is always 0.

**Fix:** Now returns `{:ok, {written, dropped, reclaimed}}` with real stats.

### M6: cross_shard_tx :tx_deleted_keys not shared across shards
**File:** `raft/state_machine.ex:212-246, line 220`

`:tx_deleted_keys` is reset per shard in the reduce loop. A key deleted in shard 0's batch is invisible to shard 1's batch in the same transaction. If shard 1 puts the same key, it resurrects shard 0's delete.

**Proposed fix:** Initialize `:tx_deleted_keys` once before the reduce loop (not inside it at line 220). Share the accumulated deleted keys across all shard batches in the same transaction. Clear it once after the entire transaction completes.

### M7: File.rm error in compaction cleanup can crash the shard
**File:** `store/shard.ex:2546`
**Status:** FIXED (commit e661d13)

If `File.rm(dest)` fails (permission denied, etc.), it raises an exception inside `run_compaction`, crashing the Shard GenServer. The partial `compact_N.log` file remains.

**Fix:** The updated `run_compaction` no longer crashes on cleanup failure — errors are logged and the compaction continues. Leftover temp files are cleaned on next startup (C2 fix).

### M8: SMOVE type enforcement gap
**File:** `commands/set.ex:335-354`

SMOVE checks destination type, then deletes from source, then writes to destination. Between the type check and the write, the destination type could change. The source member is already deleted — if the write fails or destination type changed, the member is lost.

**Proposed fix:** Reorder to: check types → write to destination → delete from source. If the destination write fails, the source is untouched. This matches the pattern of "add before remove" to prevent data loss on partial failure.

### M9: StateMachine uses nosync without coordinated fsync
**File:** `raft/state_machine.ex:1024-1059`

`flush_pending_writes` uses `v2_append_batch_nosync`. If a follower applies a Raft command and crashes before its own fsync, the data is lost from that follower's Bitcask even though it's committed in the Raft log.

**Proposed fix:** This is acceptable — on follower restart, the Raft log replays uncommitted entries and the StateMachine re-applies them, re-writing to Bitcask. The data is not lost from the cluster, only from that follower's local Bitcask until replay. No code change needed, but add a comment explaining this guarantee.
