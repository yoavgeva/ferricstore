# FerricStore Code Review — Functionality Issues

## CRITICAL (data loss, crashes, or consensus violations)

### C1: Compaction result type mismatch — merge scheduler crashes on every merge
**File:** `merge/scheduler.ex:286-317`, `store/shard.ex:2518-2551`
**Status:** FIXED (commit e661d13)
**Test result:** N/A (crash was obvious from code inspection)

**Fix:** `run_compaction` now returns `{:ok, {written, dropped, reclaimed}}` with real stats.

### C2: Leftover compaction temp files crash shard startup
**File:** `store/shard.ex:295-318, 2527`
**Status:** FIXED (commit 098b600)
**Test result:** 3 tests pass — shard starts, temp files cleaned, data survives.

**Fix:** On startup, delete `compact_*.log` files and skip them in file_id scan.

### C3: Cross-shard transaction pending writes not isolated per shard
**File:** `raft/state_machine.ex:212-246`
**Status:** NOT A BUG — dead code removed (commit ccfef8a)
**Test result:** 3 tests pass — writes land in correct shard files. The `build_cross_shard_store` closures write via `BitcaskWriter.write` with per-shard paths, bypassing `:sm_pending_writes` entirely. The `Process.put(:sm_pending_writes, [])` and `flush_pending_writes(state)` were dead code — removed.

### C4: Batcher pending map not drained on crash/restart
**File:** `raft/batcher.ex:132-141`
**Status:** BUG CONFIRMED
**Test result:** Callers via the async path (`write_async` with delegated `from`) hang indefinitely when Batcher dies. Direct `Batcher.write` callers are safe (GenServer.call monitors the process).

**Proposed fix:** Add `terminate/2` that replies `{:error, :batcher_terminated}` to all pending callers.

### C5: No fsync after BitcaskWriter flush
**File:** `store/bitcask_writer.ex:283-320`
**Status:** ACCEPTED TRADE-OFF
**Test result:** N/A — this is the async durability contract (same as Redis AOF `appendfsync everysec`). Quorum writes go through Raft WAL which fsyncs. Document the ~1ms async window.

---

## HIGH (incorrect behavior, wrong results)

### H1: Promoted write file_id race with file rotation
**File:** `store/shard.ex:638-649, 672-683`
**Status:** FIXED (commit 57756be)
**Test result:** 2 tests pass — race does NOT manifest because GenServer serializes all ops. But `promoted_active_fid` was doing a `File.ls` syscall on every promoted write unnecessarily.

**Fix:** `promoted_write` now returns `{fid, offset, record_size}` by parsing fid from the path. Removed `promoted_active_fid` entirely — saves one syscall per promoted write.

### H2: volatile_lru and allkeys_lru eviction don't implement actual LRU
**File:** `memory_guard.ex:343, 361-363`
**Status:** BUG CONFIRMED
**Test result:** Test proves eviction evicts recently-accessed keys (high ldt) while stale keys (low ldt) survive. The `_` catch-all at line 361 takes arbitrary ETS fold order instead of sorting by access time.

**Proposed fix:** Add explicit case clauses for `volatile_lru` and `allkeys_lru` that sort by `ldt` ascending via `LFU.unpack(lfu) |> elem(0)`.

### H3: compound_get_meta hardcodes expiry=0 when reading from promoted store
**File:** `store/shard.ex:592-604`
**Status:** BUG (test written, needs :shard_kill to verify)
**Test result:** Test creates a promoted hash, sets HEXPIRE on a field, kills shard, checks HTTL after restart. Expected to fail because promoted_read returns value without expiry, and ETS is populated with expire_at_ms=0.

**Proposed fix:** Change `promoted_read` to return `{:ok, value, expire_at_ms}`. The Bitcask record on disk contains the expiry.

### H4: ets_lookup matches cold entries with fid=0, off=0
**File:** `store/shard.ex:3194-3196`
**Status:** BUG CONFIRMED
**Test result:** 3 tests prove: (1) `ets_insert` stores fid=0 not `:pending` for large values, (2) cold read at fid=0/off=0 returns seed data instead of the large value, (3) Router.async_write path works correctly in contrast.

**Proposed fix:** Add guards `when fid > 0 and vsize > 0` to the cold read pattern. Entries with fid=0 are pending writes — treat as `:miss`.

### H5: EXPIREAT/PEXPIREAT accept past timestamps
**File:** `commands/expiry.ex:112-124`
**Status:** BUG CONFIRMED
**Test result:** 7 tests prove: (1) EXPIREAT with past timestamp stores expiry instead of deleting, (2) EXPIREAT 0 converts to expire_at_ms=0 (the "no expiry" sentinel), making the key immortal, (3) contrast with EXPIRE which correctly handles non-positive values.

**Proposed fix:** Check if timestamp is in the past. If so, delete the key. Also handle timestamp=0 as a delete (not "no expiry").

### H6: Batch applied result count mismatch silently truncates
**File:** `raft/batcher.ex:339-351`
**Status:** LATENT BUG (regression guard added)
**Test result:** 2 tests pass — all concurrent callers receive replies. The bug can't trigger currently because StateMachine returns correct result counts. Tests serve as regression guard.

**Proposed fix:** Add length check before `Enum.zip`. Reply error to all callers on mismatch.

### H7: Promoted collection recovery — partial output not cleaned
**File:** `store/promotion.ex:202-207`
**Status:** NOT TESTED (same pattern as C2)

**Proposed fix:** Filter out `compact_*.log` in `list_log_files` — same cleanup as shared shard.

---

## MEDIUM (degraded behavior, missing guarantees)

### M1: Async retry buffer doesn't preserve command order
**File:** `raft/async_apply_worker.ex:349-389`
**Status:** BUG CONFIRMED
**Test result:** 3 tests prove: (1) `buffer_for_retry` prepends, reversing order, (2) retry handler iterates buffer head-to-tail processing C before A, (3) pure-logic proof of reversed iteration order.

**Proposed fix:** Append instead of prepend, or use `:queue` for O(1) FIFO.

### M2: warm_ets_after_cold_read bypasses hot cache size limit
**File:** `store/router.ex:954-963`
**Status:** NOT A BUG (size check already exists)
**Test result:** Test confirms the size check at line 955 works correctly — large values stay nil in ETS after cold read. Test serves as regression guard.

### M3: Compound scan for promoted keys relies entirely on ETS
**File:** `store/shard.ex:762-776`
**Status:** NOT TESTED

**Proposed fix:** During `prefix_scan_entries`, pread values that are nil in ETS.

### M4: Hint file recovery ignores NIF-returned file_id
**File:** `store/shard.ex:344-349`
**Status:** NOT TESTED

**Proposed fix:** Use NIF-returned `file_id` instead of filename-derived one.

### M5: run_compaction returns :ok without statistics
**File:** `store/shard.ex:2518-2551`
**Status:** FIXED (commit e661d13)

### M6: cross_shard_tx :tx_deleted_keys not shared across shards
**File:** `raft/state_machine.ex:212-246, line 220`
**Status:** NOT TESTED

**Proposed fix:** Initialize `:tx_deleted_keys` once before the reduce loop.

### M7: File.rm error in compaction cleanup can crash the shard
**File:** `store/shard.ex:2546`
**Status:** FIXED (commit e661d13)

### M8: SMOVE type enforcement gap
**File:** `commands/set.ex:335-354`
**Status:** TOCTOU DOCUMENTED (not triggerable in tests)
**Test result:** 4 tests pass — basic type safety works (WRONGTYPE returned for wrong dest type, source preserved). The race between check and write can't be triggered in single-threaded tests but the code path is documented.

**Proposed fix:** Reorder to: write destination → delete source (add before remove).

### M9: StateMachine uses nosync without coordinated fsync
**File:** `raft/state_machine.ex:1024-1059`
**Status:** ACCEPTED TRADE-OFF
**Test result:** N/A — Raft log replay recovers the data on follower restart. No data loss from cluster perspective.

---

## Summary

| Issue | Severity | Test Result | Status |
|-------|----------|-------------|--------|
| C1 | CRITICAL | N/A | FIXED |
| C2 | CRITICAL | 3 pass | FIXED |
| C3 | CRITICAL | 3 pass | NOT A BUG — dead code removed |
| C4 | CRITICAL | Confirmed | TODO — add terminate/2 |
| C5 | CRITICAL | N/A | ACCEPTED |
| H1 | HIGH | 2 pass | FIXED (perf — removed syscall) |
| H2 | HIGH | Confirmed | TODO — sort by ldt |
| H3 | HIGH | Written | TODO — needs shard_kill verify |
| H4 | HIGH | Confirmed | TODO — add fid>0 guard |
| H5 | HIGH | Confirmed | TODO — handle past timestamps |
| H6 | HIGH | Latent | Regression guard added |
| H7 | HIGH | Not tested | TODO — same as C2 pattern |
| M1 | MEDIUM | Confirmed | TODO — append not prepend |
| M2 | MEDIUM | Not a bug | Regression guard added |
| M3 | MEDIUM | Not tested | TODO |
| M4 | MEDIUM | Not tested | TODO |
| M5 | MEDIUM | N/A | FIXED |
| M6 | MEDIUM | Not tested | TODO |
| M7 | MEDIUM | N/A | FIXED |
| M8 | MEDIUM | Documented | TODO — reorder ops |
| M9 | MEDIUM | N/A | ACCEPTED |
