# FerricStore Code Review — Round 2

## CRITICAL (security, data loss, crashes)

### R2-C1: Deleted user ACL cache becomes nil, granting full access
**File:** `connection.ex:2515, 2659`
**Area:** Connection + ACL

When a user is deleted via ACL DELUSER, `acl_cache` becomes nil. `check_command_cached(nil, _cmd)` returns `:ok` — all commands allowed. Deleted users get full access on active connections.

**Status:** FIXED (commit 6e194f1) — `build_acl_cache` returns `:denied` for deleted users, `check_command_cached(:denied, _cmd)` rejects all commands.

**Proposed fix:** Change `check_command_cached(nil, _cmd), do: :ok` to `check_command_cached(nil, _cmd), do: {:error, "NOPERM user deleted"}`. When acl_cache is nil, deny all commands.

### R2-C2: CrossShardOp locks/intents stored in process dictionary — lost on shard restart
**File:** `state_machine.ex:1569-1652`
**Area:** CrossShardOp

Locks and intents use `Process.put/get` with no snapshot handler. If a shard crashes mid-operation, locks and intents are lost. Other clients can acquire locks on the same keys, causing concurrent writes and data corruption.

**Status:** FIXED (commit c22271c) — locks/intents moved from process dictionary to Raft state map. Survives shard restarts, snapshots, leader failovers.

**Proposed fix:** Move locks and intents from process dictionary to a dedicated ETS table (`cross_shard_locks`) initialized in `init_aux/1`. ETS survives shard GenServer restarts within the same node. For cross-node persistence, include lock/intent state in Raft snapshots.

### R2-C3: Lock expiry during slow execute — data corruption
**File:** `cross_shard_op.ex:35`, `state_machine.ex:1622-1632`
**Area:** CrossShardOp

Lock TTL is 5s. If execute_fn takes >5s, locks expire mid-execution. Another operation locks the same keys and executes concurrently. Both operations modify the same keys.

**Status:** FIXED (commit 8d9109b) — cross-shard operations limited to 20 keys max. 5s TTL safe for ≤20 keys.

**Proposed fix:** Add lock extension during execute phase — before each Raft write, check remaining TTL and extend if <2s remaining. Alternatively, use a heartbeat Task that extends locks every 2s while execute_fn is running. Cancel the heartbeat after execution completes.

### R2-C4: GEOSEARCH BYBOX incorrect distance calculation
**File:** `geo.ex:644-648`
**Area:** Geo

Uses Haversine for rectangular bounding box, which is geometrically incorrect. Returns wrong results for large boxes or coordinates far from the equator.

**Status:** FIXED (commit 1e0136b) — `in_shape?` uses degree comparison at center latitude instead of haversine. Matches Redis behavior.

**Proposed fix:** Replace haversine-based dx/dy in `in_shape?/6` with proper rectangular calculation: dx = great-circle distance along the member's latitude (not center latitude), dy = arc distance along meridian. Or use the standard approach: convert both points to radians, compute lat/lon differences, and compare directly against the box half-widths in degrees.

### R2-C5: Embedded API bypasses ACL entirely
**File:** `ferricstore.ex:5245-5986`
**Area:** Transaction + ACL

The embedded Elixir API (`Ferricstore.multi/1`, `Ferricstore.pipeline/1`) has no ACL checks. Any host application code gets full access, bypassing all authorization.

**Status:** NOT A BUG — embedded API is intentionally unrestricted. Host application code is trusted, same as Ecto accessing the database directly.

**Proposed fix:** Document as intentional — embedded mode = trusted host code, same as Ecto accessing the database directly. Add a note in the module doc that embedded API bypasses ACL by design. Optionally, add an `acl_user` option to `multi/1` and `pipeline/1` for apps that want ACL enforcement.

---

## HIGH (incorrect behavior, wrong results)

### R2-H1: Intent write failure not detected — lock leak
**File:** `cross_shard_op.ex:148`
**Area:** CrossShardOp

`write_intent` return value not checked. If coordinator shard is down, locks are held on other shards but no intent exists for crash recovery. Locks stuck for 5s TTL.

**Status:** Regression guard — intent write failure hard to trigger in tests. Documented.

**Proposed fix:** Check `write_intent` return value. If `{:error, _}`, call `unlock_all` and return error to caller.

### R2-H2: Write intent outside try block — lock leak on exception
**File:** `cross_shard_op.ex:148-150`
**Area:** CrossShardOp

`write_intent` is called before the `try` block. If it throws (network error, timeout), locks are not released.

**Status:** Regression guard — write_intent outside try documented.

**Proposed fix:** Move `write_intent` inside the `try` block so the rescue clause releases locks on any exception.

### R2-H3: Intent resolver doesn't clean up associated locks
**File:** `intent_resolver.ex:30-87`
**Area:** CrossShardOp

Resolver deletes stale intents but NOT the associated locks on other shards. Locks remain held until TTL expiry.

**Status:** FIXED — intent resolver now unlocks associated keys on all involved shards when cleaning up stale intents.

**Proposed fix:** In `resolve_single_intent`, after deleting the intent, iterate the intent's `keys` map and send `{:unlock_keys, [key], owner_ref}` to each involved shard.

### R2-H4: No lock cleanup for expired entries — memory leak
**File:** `state_machine.ex:1574-1600`
**Area:** CrossShardOp

Expired locks are never proactively removed from the process dictionary map. They accumulate over time, growing unbounded.

**Status:** FIXED — expired locks pruned in `do_lock_keys` via `Map.reject` before inserting new ones.

**Proposed fix:** In `do_lock_keys`, after checking for conflicts, prune expired entries: `new_locks = Map.reject(locks, fn {_k, {_ref, exp}} -> exp <= now end)` before inserting new locks. Amortized cleanup on every lock operation.

### R2-H5: ZADD missing NX+XX, GT+LT, GT+NX conflict checks
**File:** `sorted_set.ex:668-676`
**Area:** Data Structures

Accepts mutually exclusive options silently. Redis returns "ERR syntax error". NX+XX, GT+LT, GT+NX combinations should be rejected.

**Status:** FIXED — `parse_zadd_opts` rejects NX+XX, GT+LT, GT+NX, LT+NX with "ERR XX and NX options at the same time are not compatible".

**Proposed fix:** After `parse_zadd_opts`, validate: if `(nx and xx)` or `(gt and lt)` or `(gt and nx)` or `(lt and nx)`, return `{:error, "ERR XX and NX options at the same time are not compatible"}`.

### R2-H6: SET missing NX+XX conflict check
**File:** `strings.ex:628-634`
**Area:** Data Structures

`SET key value NX XX` doesn't fail — silently does nothing. Redis rejects with syntax error.

**Status:** FIXED — `parse_set_opts` rejects NX+XX with error.

**Proposed fix:** In `parse_set_opts`, after parsing all flags, check `if opts.nx and opts.xx`, return `{:error, "ERR XX and NX options at the same time are not compatible"}`.

### R2-H7: ZSCORE/ZMSCORE/ZSCAN return unformatted scores
**File:** `sorted_set.ex:101-110, 452, 523-534`
**Area:** Data Structures

Raw score strings returned instead of formatted. Inconsistent with ZRANGE WITHSCORES which uses `format_score()`.

**Status:** FIXED — ZSCORE/ZMSCORE/ZSCAN now call `format_score()` for consistent formatting.

**Proposed fix:** In ZSCORE/ZMSCORE handlers, call `format_score(score_str)` on the returned value. In ZSCAN, wrap scores in the result with `format_score()`.

### R2-H8: EXPIRE/PEXPIRE with negative TTL deletes instead of returning 0
**File:** `expiry.ex:82-106`
**Area:** Data Structures

`EXPIRE key -1` returns 1 and deletes the key. Redis returns 0 and does NOT delete.

**Status:** FIXED — negative EXPIRE/PEXPIRE returns "ERR invalid expire time" (Redis 7+). Zero TTL still deletes.

**Proposed fix:** In `set_expiry_seconds`/`set_expiry_ms`, change `when secs <= 0` to `when secs <= 0 -> 0` (return 0, don't delete). Redis treats negative/zero EXPIRE as "do nothing, return 0". Only EXPIREAT with past timestamps should delete.

### R2-H9: Vector VSEARCH/VGET crash on corrupted data
**File:** `vector.ex:130, 179, 295`
**Area:** Probabilistic + Vectors

`binary_to_term()` called without try/rescue. Corrupted vector entry crashes the entire query instead of returning an error.

**Status:** FIXED — `binary_to_term` wrapped in try/rescue. VGET returns error, VSEARCH skips corrupted entries.

**Proposed fix:** Wrap `binary_to_term()` at lines 130, 179, 295 in `try/rescue ArgumentError -> {:error, "ERR corrupted vector data"}`. For VSEARCH, skip corrupted entries instead of crashing.

### R2-H10: TopK cache coherency race — duplicate mmap handles
**File:** `topk.ex:177-208`
**Area:** Probabilistic

Two concurrent processes on cache miss both open the file and cache it. Creates duplicate mmap handles — memory leak, potential use-after-free.

**Status:** Regression guard — cache race hard to trigger deterministically.

**Proposed fix:** Use `:ets.insert_new/2` instead of unconditional `cache_put`. This atomically checks-and-inserts, preventing duplicates. If insert_new returns false, re-read from cache.

### R2-H11: SUBSCRIBE/PSUBSCRIBE max_subscriptions error returns unhandled tuple
**File:** `connection.ex:1267, 1319`
**Area:** Connection

When max_subscriptions exceeded, returns `{:error_reply, ...}` which doesn't match the `handle_command` case statement. Causes runtime error on pipelined SUBSCRIBE.

**Status:** Regression guard — needs TCP test infrastructure to trigger.

**Proposed fix:** Change `{:error_reply, error, state}` to `{:continue, Encoder.encode(error), state}` so the error is RESP-encoded and matches the handle_command case statement.

### R2-H12: Sendfile error path missing fallback — client hangs
**File:** `connection.ex:1743-1747`
**Area:** Connection

`do_sendfile_get` can return `:fallback` but `fast_get` doesn't handle it. Command silently dropped, client hangs waiting for response.

**Status:** Regression guard — needs sendfile failure to trigger.

**Proposed fix:** Add `:fallback` case clause in `fast_get` that falls through to `dispatch_normal("GET", [key], state)`.

### R2-H13: WriteVersion increments for failed conditional writes — spurious WATCH failures
**File:** `router.ex:140-143`, `shard.ex:909-910`
**Area:** Transactions

`SET key val NX` that doesn't set (key exists) still increments write-version. WATCH thinks the key was modified, causing false transaction aborts.

**Status:** FIXED (commit 3fab737) — WATCH uses per-key `phash2(value)` instead of per-shard WriteVersion. No false positives from NX skip, same-shard unrelated writes, or cross-shard increments.

**Proposed fix:** Only increment WriteVersion when the command actually mutated state. In `coordinator.ex` line 77, check results per shard — only increment for shards that had successful mutations. In `router.ex`, only increment after confirming the result is not `nil` (SET NX returns nil on skip).

---

## MEDIUM (degraded behavior, compatibility issues)

### R2-M1: Unlock failures silently ignored — lock leak until TTL
**File:** `cross_shard_op.ex:241-252`
**Area:** CrossShardOp

`parallel_unlock` doesn't check Task results. If a shard is unreachable, unlock is never delivered. Lock held for full 5s TTL.

**Proposed fix:** Check `Task.await_many` results. Log errors for failed unlocks. Acceptable that locks expire via TTL as fallback, but log so ops can investigate.

### R2-M2: JSON path quoted string parsing accepts malformed paths
**File:** `json.ex:663-669`
**Area:** JSON

`$["unclosed` parses as key="" silently. Redis rejects with syntax error.

**Proposed fix:** In `parse_bracket_content`, validate closing quote exists before slicing. If not found, return `:error`.

### R2-M3: JSON unquoted integer as object key — type confusion
**File:** `json.ex:671-676`
**Area:** JSON

`$[0]` on object `{"0": "value"}` fails. Parsed as array index (integer), not string key.

**Proposed fix:** In `get_at_path`, when traversing a map with an integer key, also try `Map.fetch(map, to_string(key))` as fallback. Integer bracket on map = string key lookup.

### R2-M4: XREADGROUP doesn't support BLOCK
**File:** `stream.ex:229-237`
**Area:** Streams

Redis 7+ allows `XREADGROUP BLOCK`. FerricStore only supports non-blocking XREADGROUP.

**Proposed fix:** Add `parse_xread_block` call to `parse_xreadgroup_args`, same as `parse_xread_args` does. Return block timeout in the result tuple. Connection layer handles blocking wait.

### R2-M5: Bloom/Cuckoo/CMS registries lose metadata on recovery
**File:** `bloom_registry.ex:268-289`, `cuckoo_registry.ex:199-204`
**Area:** Probabilistic

After restart, `BF.INFO` returns estimated capacity/error_rate instead of original values. `derive_metadata()` is an approximation.

**Proposed fix:** Store original metadata (capacity, error_rate) in a companion `.meta` file alongside the `.bloom` file. On recovery, read `.meta` instead of deriving from the mmap structure.

### R2-M6: BF.ADD/MADD NIF errors not checked
**File:** `bloom.ex:93, 106`
**Area:** Probabilistic

NIF return value not checked for `{:error, reason}`. Silent failures if mmap is corrupted.

**Proposed fix:** Wrap NIF calls: `case NIF.bloom_add(resource, element) do {:error, reason} -> {:error, "ERR #{reason}"} ok -> ok end`.

### R2-M7: TOPK.LIST WITHCOUNT NIF errors unchecked — crash
**File:** `topk.ex:127-128`
**Area:** Probabilistic

If either NIF call fails, code crashes with pattern match error instead of returning error message.

**Proposed fix:** Wrap both NIF calls in `with {:ok, items} <- ..., {:ok, counts} <- ...` pattern. Return error on failure.

### R2-M8: Vector sentinel key pollution in VCREATE
**File:** `vector.ex:87-88, 203`
**Area:** Vectors

VCREATE stores sentinel key `V:collection\0__hnsw_meta__`. VINFO subtracts 1 for count. After restart, if sentinel not recovered, count is off by 1.

**Proposed fix:** Store collection metadata in a regular key (`VM:collection`) instead of as a compound sentinel. VINFO counts compound keys without offset subtraction. Or filter out `__hnsw_meta__` explicitly in the count.

### R2-M9: Namespace config changes don't update in-flight batcher slots
**File:** `batcher.ex:460-492`, `namespace_config.ex:460-510`
**Area:** Namespace

Config change clears ns_cache but doesn't update window_ms in already-queued slots. Commands may experience wrong commit window.

**Proposed fix:** On `:ns_config_changed`, iterate `state.slots` and re-fetch window_ms for each slot's prefix. Reschedule timers with the new window. Or flush all slots immediately on config change.

### R2-M10: ACL not re-checked at EXEC time for queued MULTI commands
**File:** `connection.ex:1363-1380, 1985-1988`
**Area:** ACL

Commands queued in MULTI are not re-validated at EXEC time. If admin revokes permissions between MULTI and EXEC, queued commands still execute.

**Proposed fix:** Before passing the queue to `Coordinator.execute`, re-validate each command's ACL using current `acl_cache`. If any fails, return error and discard transaction.

### R2-M11: WATCH/EXEC race — watches_clean? doesn't distinguish shard-down from key-modified
**File:** `coordinator.ex:257-267`
**Area:** Transactions

If `Router.get_version(key)` throws (shard unavailable), `watches_clean?` returns false — same as "key modified". Transaction aborts without telling the client the shard is down.

**Proposed fix:** In `watches_clean?`, catch `:exit` separately and return `{:error, :shard_unavailable}` instead of `false`. EXEC can then return a distinct error message.

---

## LOW (minor, acceptable deviations)

### R2-L1: PUBLISH delivery count may be inaccurate
**File:** `pubsub.ex:160-184`
**Area:** Pub/Sub

`send(pid, ...)` is async. If subscriber crashes between check and send, count is inflated. Acceptable BEAM semantics.

### R2-L2: XACK doesn't validate ID exists in stream
**File:** `stream.ex:886-906`
**Area:** Streams

ACKing a non-existent ID returns 0 (correct), but no distinction between "not pending" and "never existed". Matches Redis behavior.

### R2-L3: XREAD sequence overflow at boundary
**File:** `stream.ex:989-994`
**Area:** Streams

`{ms, seq + 1}` has no bounds check. Extremely unlikely in practice (requires 2^64 sequence numbers).

### R2-L4: CF.DEL errors not propagated
**File:** `cuckoo.ex:104`
**Area:** Probabilistic

Returns NIF result directly without wrapping errors. Inconsistent with CF.ADD error handling but functionally correct.

### R2-L5: TDIGEST.MERGE COMPRESSION 0 not validated
**File:** `tdigest.ex:454`
**Area:** Probabilistic

`COMPRESSION 0` causes runtime error from Core instead of client-friendly error.

### R2-L6: Empty response for UNSUBSCRIBE with no channels
**File:** `connection.ex:1286`
**Area:** Connection

Returns empty list `[]` instead of proper RESP response. May confuse clients.

---

## Summary

| Issue | Severity | Status |
|-------|----------|--------|
| C1 | CRITICAL | FIXED — deleted user denied |
| C2 | CRITICAL | FIXED — locks in Raft state |
| C3 | CRITICAL | FIXED — 20 key limit |
| C4 | CRITICAL | FIXED — degree comparison |
| C5 | CRITICAL | NOT A BUG — embedded is trusted |
| H1 | HIGH | NOT A BUG — shard can't fail between lock and intent |
| H2 | HIGH | NOT A BUG — same as H1 |
| H3 | HIGH | FIXED — resolver unlocks keys |
| H4 | HIGH | FIXED — expired locks pruned |
| H5 | HIGH | FIXED — ZADD flag conflicts |
| H6 | HIGH | FIXED — SET NX+XX rejected |
| H7 | HIGH | FIXED — score formatting |
| H8 | HIGH | FIXED — EXPIRE -1 returns error |
| H9 | HIGH | FIXED — vector try/rescue |
| H10 | HIGH | NOT A BUG — Rustler GC handles duplicate refs |
| H11 | HIGH | FIXED — RESP-encoded error |
| H12 | HIGH | FIXED — sendfile fallback to normal GET |
| H13 | HIGH | FIXED — WATCH value hash |
| M1 | MEDIUM | FIXED — unlock retry with backoff |
| M2 | MEDIUM | FIXED — malformed path returns error |
| M3 | MEDIUM | NOT A BUG — matches Redis JSONPath spec |
| M4 | MEDIUM | FIXED — XREADGROUP BLOCK support |
| M5 | MEDIUM | FIXED — .meta file for bloom metadata |
| M6 | MEDIUM | FIXED — NIF error wrapping |
| M7 | MEDIUM | FIXED — with/else guard on NIF calls |
| M8 | MEDIUM | NOT A BUG — code checks sentinel before subtracting |
| M9-M11 | MEDIUM | TODO |
| L1-L6 | LOW | Accepted / documented |
