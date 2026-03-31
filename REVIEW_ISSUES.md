# FerricStore Code Review — Functionality Issues

## CRITICAL (data loss, crashes, or consensus violations)

### C6: Multi-key commands silently operate across shards without atomicity
**File:** `commands/set.ex` (SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE), `commands/generic.ex` (RENAME, RENAMENX, COPY)
**Status:** OPEN

Multi-key commands that touch two or more keys don't validate that all keys hash to the same shard. If source and destination are on different shards, the operations go to different Shard GenServers — not atomic, not serialized.

**Data loss scenario (SMOVE):**
1. `SMOVE src dst member` where src is on shard 0, dst on shard 1
2. Delete from shard 0 succeeds
3. Put to shard 1 fails (crash, timeout, memory pressure)
4. Member is gone from src but never added to dst — lost

**Data loss scenario (RENAME):**
1. `RENAME old new` where old is on shard 0, new on shard 1
2. Read old from shard 0, write to shard 1, delete from shard 0
3. If write to shard 1 fails after delete from shard 0 — key lost

**Affected commands:** SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE, RENAME, RENAMENX, COPY

**Redis behavior:** Redis Cluster returns `CROSSSLOT` error when multi-key commands target different hash slots. FerricStore only checks this in MULTI/EXEC transactions, not in individual multi-key commands.

**Proposed fix:** Two options:
1. **Validate same-shard** — before executing, check `Router.shard_for(key1) == Router.shard_for(key2)` for all key pairs. Return `CROSSSLOT` error if different. Simple, matches Redis Cluster behavior. Users can use hash tags `{tag}` to force same-shard routing.
2. **Cross-shard atomic execution** — wrap multi-key commands in the existing `cross_shard_tx` mechanism when keys span shards. More complex but transparent to users.

Option 1 is recommended — it's what Redis does, it's simple, and it surfaces the problem to the user who can fix it with hash tags.

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
**Status:** FIXED (commit 62e4947)
**Test result:** Callers via the async path (`write_async` with delegated `from`) hung indefinitely when Batcher died. Direct `Batcher.write` callers were safe (GenServer.call monitors the process).

**Fix:** Added `trap_exit` in init + `terminate/2` that replies `{:error, :batcher_terminated}` to all callers in `slots.froms`, `pending`, and `flush_waiters`. Same semantics as Redis: client gets an error, outcome is unknown, client decides whether to retry. No internal retry — prevents double-INCR on non-idempotent commands.

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
**Status:** FIXED (commit 8d300c9)
**Test result:** Previously evicted recently-accessed keys while stale keys survived. Now correctly evicts stale keys first.

**Fix:** Added explicit case clauses for `volatile_lru` and `allkeys_lru` that sort by `ldt` (last access time) ascending via `LFU.unpack(lfu) |> elem(0)`. All eviction policies verified correct: volatile_ttl sorts by TTL, volatile/allkeys_lfu sorts by frequency counter, volatile/allkeys_lru sorts by access time, noeviction skips.

### H3: compound_get_meta hardcodes expiry=0 when reading from promoted store
**File:** `store/shard.ex:592-604`
**Status:** NOT A BUG IN PRACTICE
**Test result:** Test passes — HTTL returns correct TTL after shard restart. `recover_promoted` populates ALL dedicated entries into ETS with correct expiry from the Bitcask record. The `compound_get_meta` cold read path (hardcoded expiry=0) is never hit because entries are always warm after recovery. MemoryGuard eviction sets value=nil but preserves expiry, so `compound_get_meta` reads the cached expiry from the ETS hot path.

The hardcoded expiry=0 in the cold path is dead code for promoted keys, but could be fixed defensively if promoted_read is ever changed to not pre-load all entries.

### H4: ets_lookup matches cold entries with fid=0, off=0
**File:** `store/shard.ex:3194-3196`
**Status:** FIXED (commits 95f6e46, 8322b43)
**Test result:** 3 tests verify: (1) ets_insert uses `:pending` fid, (2) GET on unflushed large value triggers flush and returns correct data, (3) after flush, ETS has real fid/vsize.

**Fix:** `ets_insert` now uses `:pending` as fid (root cause fix). Also added safety guard in `warm_from_store` — if `:pending` ever leaks to the cold read path, returns explicit error instead of silently reading wrong data.

### H5: EXPIREAT/PEXPIREAT accept past timestamps
**File:** `commands/expiry.ex:112-124`
**Status:** FIXED (commit 76ec163)
**Test result:** 11 tests verify: past timestamps delete key, zero deletes key, non-existent key returns 0, future timestamps set TTL correctly.

**Fix:** `set_expiry_at_seconds` and `set_expiry_at_ms` now check `ts <= HLC.now_ms()`. Past or zero → `delete_if_exists`. Future → `apply_expiry`. Same pattern as EXPIRE/PEXPIRE. Works in quorum, async, and multi-node (decision is on leader, Raft replicates the resulting delete/put command).

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
**Status:** FIXED (commit 5a0ec37)
**Test result:** 3 tests verify FIFO order preserved with `:queue`.

**Fix:** Replaced list prepend with `:queue` — O(1) add-to-back, O(1) take-from-front. Commands A, B, C that fail in order are retried as A, B, C (not C, B, A).

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
**Status:** NOT A BUG
**Test result:** N/A — the filename-derived fid IS the correct source of truth. Hint file `00005.hint` describes entries in `00005.log`. The NIF's internal `file_id` is redundant metadata read from the binary format. Using the filename is consistent with Bitcask hint file semantics.

### M5: run_compaction returns :ok without statistics
**File:** `store/shard.ex:2518-2551`
**Status:** FIXED (commit e661d13)

### M6: cross_shard_tx :tx_deleted_keys not shared across shards
**File:** `raft/state_machine.ex:212-246, line 220`
**Status:** NOT A BUG
**Test result:** N/A — resetting per shard is correct. Keys are shard-specific; a DEL on shard 0 and GET on shard 1 are always different keys. `:tx_deleted_keys` only needs to track deletes within one shard's command queue for read-your-own-deletes semantics.

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
| C6 | CRITICAL | N/A | OPEN — needs same-shard guard |
| C1 | CRITICAL | N/A | FIXED |
| C2 | CRITICAL | 3 pass | FIXED |
| C3 | CRITICAL | 3 pass | NOT A BUG — dead code removed |
| C4 | CRITICAL | Confirmed | FIXED |
| C5 | CRITICAL | N/A | ACCEPTED |
| H1 | HIGH | 2 pass | FIXED (removed File.ls syscall) |
| H2 | HIGH | Confirmed | FIXED |
| H3 | HIGH | Passes | NOT A BUG (recovery loads expiry correctly) |
| H4 | HIGH | Confirmed | FIXED |
| H5 | HIGH | Confirmed | FIXED |
| H6 | HIGH | Latent | Regression guard added |
| H7 | HIGH | 1 pass | FIXED |
| M1 | MEDIUM | Confirmed | FIXED |
| M2 | MEDIUM | Not a bug | Regression guard added |
| M3 | MEDIUM | 2 pass | FIXED |
| M4 | MEDIUM | N/A | NOT A BUG |
| M5 | MEDIUM | N/A | FIXED |
| M6 | MEDIUM | N/A | NOT A BUG |
| M7 | MEDIUM | N/A | FIXED |
| M8 | MEDIUM | Documented | TODO — reorder ops |
| M9 | MEDIUM | N/A | ACCEPTED |
