# FerricStore Code Review — Functionality Issues

## CRITICAL (data loss, crashes, or consensus violations)

### C1: Compaction result type mismatch — merge scheduler crashes on every merge
**File:** `merge/scheduler.ex:286-317`, `store/shard.ex:2518-2551`

`do_merge` expects `run_compaction` to return `{:ok, {written, dropped, reclaimed}}`, but the shard returns `{:reply, :ok, state}`. The pattern match at line 291 has no matching clause — every merge attempt crashes the scheduler. The semaphore is never released, causing a permanent deadlock on future merges.

### C2: Leftover compaction temp files crash shard startup
**File:** `store/shard.ex:295-318, 2527`

`discover_active_file` calls `String.to_integer` on all `.log` filenames. Compaction creates `compact_N.log` temp files. If a crash leaves one behind, `String.to_integer("compact_5")` raises ArgumentError, crashing the shard on restart. The shard cannot recover without manual file deletion.

### C3: Cross-shard transaction pending writes not isolated per shard
**File:** `raft/state_machine.ex:212-246`

`cross_shard_tx` uses a shared process-level `:sm_pending_writes` buffer. When processing commands for shard 0 then shard 1, writes from shard 0 may be flushed using shard 1's `active_file_path`. Writes end up in the wrong shard's log file.

### C4: Batcher pending map not drained on crash/restart
**File:** `raft/batcher.ex:132-141`

No `terminate/2` callback. If the Batcher process is killed, all callers in `state.pending` and `state.flush_waiters` hang forever waiting for `GenServer.reply`. Connection processes accumulate and never unblock.

### C5: No fsync after BitcaskWriter flush
**File:** `store/bitcask_writer.ex:283-320`

BitcaskWriter uses `v2_append_batch_nosync` — data goes to page cache, not disk. There's no coordinated fsync between BitcaskWriter and the Shard timer. If the system crashes between flush and fsync (~1ms window), async writes are lost despite returning `:ok` to the client.

---

## HIGH (incorrect behavior, wrong results)

### H1: Promoted write file_id race with file rotation
**File:** `store/shard.ex:638-649, 672-683`

After `promoted_write` returns `{:ok, {offset, record_size}}`, the code calls `promoted_active_fid(dedicated_path)` which does `File.ls`. Between the write and the fid lookup, another write could trigger compaction/rotation, making the fid stale. ETS then points to the wrong file.

### H2: volatile_lru and allkeys_lru eviction don't implement actual LRU
**File:** `memory_guard.ex:343, 361-363`

The case statement has explicit sorting for `volatile_lfu`, `allkeys_lfu`, and `volatile_ttl`, but `volatile_lru` and `allkeys_lru` fall through to a default that just takes the first 5 entries from ETS fold order. This is arbitrary, not LRU. The LFU `ldt` field (last access time) exists but isn't used for sorting.

### H3: compound_get_meta hardcodes expiry=0 when reading from promoted store
**File:** `store/shard.ex:592-604`

When a promoted key miss falls through to `promoted_read`, the result is inserted into ETS with `ets_insert(state, compound_key, value, 0)` — always 0 expiry. The actual expiry from disk is lost. HTTL/HPTTL return -1 (no expiry) instead of the real TTL.

### H4: ets_lookup matches cold entries with fid=0, off=0
**File:** `store/shard.ex:3194-3196`

Pattern `[{^key, nil, 0, _lfu, fid, off, vsize}]` matches not-yet-flushed large values (which have fid=0, off=0, vsize=0). The code then attempts `pread_at(path, 0)` which reads the file header instead of data. Recently-written large values return nil on read.

### H5: EXPIREAT/PEXPIREAT accept past timestamps
**File:** `commands/expiry.ex:112-124`

`EXPIREAT key 1000` (where now=2000) returns 1 (success) instead of immediately deleting the key and returning 0. The key lingers with a past expiry until lazy deletion or sweep. Redis deletes immediately for past timestamps.

### H6: Batch applied result count mismatch silently truncates
**File:** `raft/batcher.ex:339-351`

`Enum.zip(froms, results)` silently truncates if the results list is shorter than the froms list. Remaining callers never receive a reply and hang forever.

### H7: Promoted collection recovery — partial output not cleaned
**File:** `store/promotion.ex:202-207`

`recover_promoted` doesn't check for partial compaction files in dedicated directories. If compaction crashed mid-write, partial files are scanned and may contain incomplete or duplicate entries.

---

## MEDIUM (degraded behavior, missing guarantees)

### M1: Async retry buffer doesn't preserve command order
**File:** `raft/async_apply_worker.ex:349-389`

Failed commands are prepended to `retry_buffer`. On retry, they're processed in reverse order of submission. If command A was submitted before B, but A fails and B succeeds, the retry of A happens after B commits. This breaks causal ordering for async writes.

### M2: warm_ets_after_cold_read bypasses hot cache size limit
**File:** `store/router.ex:954-963`

After a cold read, the full value is cached in ETS regardless of size. The `value_for_ets/1` function (which returns nil for >65KB values) is not called. Large values get stored in ETS, defeating the hot cache size limit and causing memory bloat.

### M3: Compound scan for promoted keys relies entirely on ETS
**File:** `store/shard.ex:762-776`

HGETALL/SMEMBERS/ZRANGE on promoted collections scan ETS only. If recovery was incomplete (pread failed for some entries), those entries have value=nil in ETS and appear missing in scan results. No fallback to disk scan.

### M4: Hint file recovery ignores NIF-returned file_id
**File:** `store/shard.ex:344-349`

Recovery from hint files uses the filename-derived fid (`hint_name |> trim(".hint") |> to_integer()`) and ignores `_file_id` returned by the NIF. If the hint file's internal file_id doesn't match the filename (e.g., after a copy or partial write), cold reads look in the wrong file.

### M5: run_compaction returns :ok without statistics
**File:** `store/shard.ex:2518-2551`

Compaction never computes or returns bytes_reclaimed, records_written, or records_dropped. The scheduler can't track merge effectiveness. `total_bytes_reclaimed` in status is always 0.

### M6: cross_shard_tx :tx_deleted_keys not shared across shards
**File:** `raft/state_machine.ex:212-246, line 220`

`:tx_deleted_keys` is reset per shard in the reduce loop. A key deleted in shard 0's batch is invisible to shard 1's batch in the same transaction. If shard 1 puts the same key, it resurrects shard 0's delete.

### M7: File.rm error in compaction cleanup can crash the shard
**File:** `store/shard.ex:2546`

If `File.rm(dest)` fails (permission denied, etc.), it raises an exception inside `run_compaction`, crashing the Shard GenServer. The partial `compact_N.log` file remains.

### M8: SMOVE type enforcement gap
**File:** `commands/set.ex:335-354`

SMOVE checks destination type, then deletes from source, then writes to destination. Between the type check and the write, the destination type could change. The source member is already deleted — if the write fails or destination type changed, the member is lost.

### M9: StateMachine uses nosync without coordinated fsync
**File:** `raft/state_machine.ex:1024-1059`

`flush_pending_writes` uses `v2_append_batch_nosync`. If a follower applies a Raft command and crashes before its own fsync, the data is lost from that follower's Bitcask even though it's committed in the Raft log.
