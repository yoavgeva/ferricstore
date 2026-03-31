# FerricStore Code Review — Round 2

## CRITICAL (security, data loss, crashes)

### R2-C1: Deleted user ACL cache becomes nil, granting full access
**File:** `connection.ex:2515, 2659`
**Area:** Connection + ACL

When a user is deleted via ACL DELUSER, `acl_cache` becomes nil. `check_command_cached(nil, _cmd)` returns `:ok` — all commands allowed. Deleted users get full access on active connections.

### R2-C2: CrossShardOp locks/intents stored in process dictionary — lost on shard restart
**File:** `state_machine.ex:1569-1652`
**Area:** CrossShardOp

Locks and intents use `Process.put/get` with no snapshot handler. If a shard crashes mid-operation, locks and intents are lost. Other clients can acquire locks on the same keys, causing concurrent writes and data corruption.

### R2-C3: Lock expiry during slow execute — data corruption
**File:** `cross_shard_op.ex:35`, `state_machine.ex:1622-1632`
**Area:** CrossShardOp

Lock TTL is 5s. If execute_fn takes >5s, locks expire mid-execution. Another operation locks the same keys and executes concurrently. Both operations modify the same keys.

### R2-C4: GEOSEARCH BYBOX incorrect distance calculation
**File:** `geo.ex:644-648`
**Area:** Geo

Uses Haversine for rectangular bounding box, which is geometrically incorrect. Returns wrong results for large boxes or coordinates far from the equator.

### R2-C5: Embedded API bypasses ACL entirely
**File:** `ferricstore.ex:5245-5986`
**Area:** Transaction + ACL

The embedded Elixir API (`Ferricstore.multi/1`, `Ferricstore.pipeline/1`) has no ACL checks. Any host application code gets full access, bypassing all authorization.

---

## HIGH (incorrect behavior, wrong results)

### R2-H1: Intent write failure not detected — lock leak
**File:** `cross_shard_op.ex:148`
**Area:** CrossShardOp

`write_intent` return value not checked. If coordinator shard is down, locks are held on other shards but no intent exists for crash recovery. Locks stuck for 5s TTL.

### R2-H2: Write intent outside try block — lock leak on exception
**File:** `cross_shard_op.ex:148-150`
**Area:** CrossShardOp

`write_intent` is called before the `try` block. If it throws (network error, timeout), locks are not released.

### R2-H3: Intent resolver doesn't clean up associated locks
**File:** `intent_resolver.ex:30-87`
**Area:** CrossShardOp

Resolver deletes stale intents but NOT the associated locks on other shards. Locks remain held until TTL expiry.

### R2-H4: No lock cleanup for expired entries — memory leak
**File:** `state_machine.ex:1574-1600`
**Area:** CrossShardOp

Expired locks are never proactively removed from the process dictionary map. They accumulate over time, growing unbounded.

### R2-H5: ZADD missing NX+XX, GT+LT, GT+NX conflict checks
**File:** `sorted_set.ex:668-676`
**Area:** Data Structures

Accepts mutually exclusive options silently. Redis returns "ERR syntax error". NX+XX, GT+LT, GT+NX combinations should be rejected.

### R2-H6: SET missing NX+XX conflict check
**File:** `strings.ex:628-634`
**Area:** Data Structures

`SET key value NX XX` doesn't fail — silently does nothing. Redis rejects with syntax error.

### R2-H7: ZSCORE/ZMSCORE/ZSCAN return unformatted scores
**File:** `sorted_set.ex:101-110, 452, 523-534`
**Area:** Data Structures

Raw score strings returned instead of formatted. Inconsistent with ZRANGE WITHSCORES which uses `format_score()`.

### R2-H8: EXPIRE/PEXPIRE with negative TTL deletes instead of returning 0
**File:** `expiry.ex:82-106`
**Area:** Data Structures

`EXPIRE key -1` returns 1 and deletes the key. Redis returns 0 and does NOT delete.

### R2-H9: Vector VSEARCH/VGET crash on corrupted data
**File:** `vector.ex:130, 179, 295`
**Area:** Probabilistic + Vectors

`binary_to_term()` called without try/rescue. Corrupted vector entry crashes the entire query instead of returning an error.

### R2-H10: TopK cache coherency race — duplicate mmap handles
**File:** `topk.ex:177-208`
**Area:** Probabilistic

Two concurrent processes on cache miss both open the file and cache it. Creates duplicate mmap handles — memory leak, potential use-after-free.

### R2-H11: SUBSCRIBE/PSUBSCRIBE max_subscriptions error returns unhandled tuple
**File:** `connection.ex:1267, 1319`
**Area:** Connection

When max_subscriptions exceeded, returns `{:error_reply, ...}` which doesn't match the `handle_command` case statement. Causes runtime error on pipelined SUBSCRIBE.

### R2-H12: Sendfile error path missing fallback — client hangs
**File:** `connection.ex:1743-1747`
**Area:** Connection

`do_sendfile_get` can return `:fallback` but `fast_get` doesn't handle it. Command silently dropped, client hangs waiting for response.

### R2-H13: WriteVersion increments for failed conditional writes — spurious WATCH failures
**File:** `router.ex:140-143`, `shard.ex:909-910`
**Area:** Transactions

`SET key val NX` that doesn't set (key exists) still increments write-version. WATCH thinks the key was modified, causing false transaction aborts.

---

## MEDIUM (degraded behavior, compatibility issues)

### R2-M1: Unlock failures silently ignored — lock leak until TTL
**File:** `cross_shard_op.ex:241-252`
**Area:** CrossShardOp

`parallel_unlock` doesn't check Task results. If a shard is unreachable, unlock is never delivered. Lock held for full 5s TTL.

### R2-M2: JSON path quoted string parsing accepts malformed paths
**File:** `json.ex:663-669`
**Area:** JSON

`$["unclosed` parses as key="" silently. Redis rejects with syntax error.

### R2-M3: JSON unquoted integer as object key — type confusion
**File:** `json.ex:671-676`
**Area:** JSON

`$[0]` on object `{"0": "value"}` fails. Parsed as array index (integer), not string key.

### R2-M4: XREADGROUP doesn't support BLOCK
**File:** `stream.ex:229-237`
**Area:** Streams

Redis 7+ allows `XREADGROUP BLOCK`. FerricStore only supports non-blocking XREADGROUP.

### R2-M5: Bloom/Cuckoo/CMS registries lose metadata on recovery
**File:** `bloom_registry.ex:268-289`, `cuckoo_registry.ex:199-204`
**Area:** Probabilistic

After restart, `BF.INFO` returns estimated capacity/error_rate instead of original values. `derive_metadata()` is an approximation.

### R2-M6: BF.ADD/MADD NIF errors not checked
**File:** `bloom.ex:93, 106`
**Area:** Probabilistic

NIF return value not checked for `{:error, reason}`. Silent failures if mmap is corrupted.

### R2-M7: TOPK.LIST WITHCOUNT NIF errors unchecked — crash
**File:** `topk.ex:127-128`
**Area:** Probabilistic

If either NIF call fails, code crashes with pattern match error instead of returning error message.

### R2-M8: Vector sentinel key pollution in VCREATE
**File:** `vector.ex:87-88, 203`
**Area:** Vectors

VCREATE stores sentinel key `V:collection\0__hnsw_meta__`. VINFO subtracts 1 for count. After restart, if sentinel not recovered, count is off by 1.

### R2-M9: Namespace config changes don't update in-flight batcher slots
**File:** `batcher.ex:460-492`, `namespace_config.ex:460-510`
**Area:** Namespace

Config change clears ns_cache but doesn't update window_ms in already-queued slots. Commands may experience wrong commit window.

### R2-M10: ACL not re-checked at EXEC time for queued MULTI commands
**File:** `connection.ex:1363-1380, 1985-1988`
**Area:** ACL

Commands queued in MULTI are not re-validated at EXEC time. If admin revokes permissions between MULTI and EXEC, queued commands still execute.

### R2-M11: WATCH/EXEC race — watches_clean? doesn't distinguish shard-down from key-modified
**File:** `coordinator.ex:257-267`
**Area:** Transactions

If `Router.get_version(key)` throws (shard unavailable), `watches_clean?` returns false — same as "key modified". Transaction aborts without telling the client the shard is down.

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

| Severity | Count | Key Areas |
|----------|-------|-----------|
| CRITICAL | 5 | ACL bypass (C1, C5), CrossShardOp locks (C2, C3), Geo math (C4) |
| HIGH | 13 | CrossShardOp leaks (H1-H4), Redis compat (H5-H8), Crashes (H9-H12), WATCH (H13) |
| MEDIUM | 11 | CrossShardOp (M1), JSON/Streams (M2-M4), Probabilistic (M5-M8), Namespace/ACL (M9-M11) |
| LOW | 6 | Pub/Sub, Streams, Cuckoo, TDigest, Connection |
