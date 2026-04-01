# FerricStore Code Review — Round 3

## CRITICAL

### R3-C1: Sandbox bypass in async write path
**File:** `router.ex:261-296`
**Area:** Sandbox

`async_write()` uses `keydir_name(idx)` (production ETS), `PrefixIndex.table_name(idx)` (production), and `ActiveFile.get(idx)` (production registry) — completely bypasses sandbox isolation. Sandbox async writes go to production ETS tables and wrong file paths.

**Proposed fix:** `async_write` should use `resolve_keydir(idx)` and `resolve_prefix_table(idx)` when in sandbox. Or route sandbox writes through the Shard GenServer (quorum path) which is already sandbox-aware.

## HIGH

### R3-H1: Embedded API missing error handling in set operations
**File:** `ferricstore.ex:1126-1230`
**Area:** Embedded API

`sadd()`, `srem()`, `smembers()`, `scard()` don't check for `{:error, reason}` from command handlers. Errors get wrapped as `{:ok, {:error, ...}}` instead of propagated.

**Proposed fix:** Add `case result do {:error, _} = err -> err; ok -> {:ok, ok} end` pattern like hash operations do.

### R3-H2: Embedded API missing error handling in sorted set operations
**File:** `ferricstore.ex:1254-1394`
**Area:** Embedded API

`zadd()`, `zcard()`, `zrem()`, `zrange()`, `zscore()` don't check for errors. `zscore()` can crash on error tuple (no matching clause). `Float.parse` in `zrange` assumes success.

**Proposed fix:** Same pattern as H1. Wrap `Float.parse` in safe handling.

### R3-H3: DBSIZE counts compound keys — inconsistent with KEYS/SCAN
**File:** `server.ex:76-82`
**Area:** Server

`DBSIZE` sums `:ets.info(keydir, :size)` which includes internal compound keys (H:, S:, Z:, etc.). `KEYS *` filters them out via `user_visible_keys()`. A hash with 5 fields shows DBSIZE=6 but KEYS returns 1.

**Proposed fix:** Filter through `CompoundKey.user_visible_keys()` before counting, or maintain a separate user-key counter.

### R3-H4: RANDOMKEY can return internal compound keys
**File:** `generic.ex:171-175`
**Area:** Generic

`RANDOMKEY` calls `store.keys()` then `Enum.random()` without filtering through `user_visible_keys()`. Can return `"H:myhash\0field"` to the client.

**Proposed fix:** Filter `store.keys()` through `CompoundKey.user_visible_keys()` before random.

### R3-H5: SCAN cursor skips keys when keys are deleted between calls
**File:** `generic.ex:407-456`
**Area:** Generic

SCAN uses key-based cursor (last key of previous batch). If that key is deleted before the next SCAN call, `drop_while(fn k -> k <= cursor end)` may skip the next key. Violates Redis SCAN guarantee.

**Proposed fix:** Use integer-based cursor (hash of position) or accept as known limitation (Redis SCAN also has minor guarantees — "all keys present from start to end are returned").

### R3-H6: FLUSHDB doesn't clear Bloom/Cuckoo/TopK/promoted instances
**File:** `server.ex:121-143`
**Area:** Server

FLUSHDB iterates `Router.keys()` and deletes each, but only clears keydir ETS. Bloom filters in `BloomRegistry`, promoted file instances, and probabilistic structures remain on disk and in memory.

**Proposed fix:** After key deletion, call `BloomRegistry.clear(shard_idx)`, `CuckooRegistry.clear(shard_idx)`, etc. to remove mmap handles and files.

### R3-H7: CAS/LOCK/EXTEND double-adds timestamp in Raft fallback path
**File:** `shard.ex:2080, 2125, 2224`
**Area:** Native commands

In the Raft fallback path (GenServer.call), `expire_at_ms` from the router (already absolute) is treated as relative TTL and recomputed with `System.os_time + ttl`. Results in ~double the intended TTL.

**Proposed fix:** Don't recompute — the router already converts TTL to absolute timestamp. Use the value directly.

## MEDIUM

### R3-M1: Mode.current() default contradicts documentation
**File:** `mode.ex:72`
**Area:** Lifecycle

Code defaults to `:embedded`. Documentation says default is `:standalone`. We changed this intentionally (for ActiveFile GC), but the doc wasn't updated.

**Proposed fix:** Update the docstring at line 24 to say the default is `:embedded`. The `runtime.exs` in ferricstore_server sets `:standalone` explicitly.

### R3-M2: CLUSTER.HEALTH hardcodes role as "leader"
**File:** `cluster.ex:40`
**Area:** Cluster

All shards reported as "leader" regardless of actual Raft role. Should query `:ra.members()`.

**Proposed fix:** Call `:ra.members(shard_server_id)` and check if local node is leader. Return "leader" or "follower".

### R3-M3: CONFIG SET hot-cache-min-ram and max-ram lack cross-validation
**File:** `config.ex:576-580`
**Area:** Config

Can set `min > max` creating invalid state. MemoryGuard behavior becomes unpredictable.

**Proposed fix:** After setting either, validate `min <= max`. If violated, reject with error.

### R3-M4: Embedded API del() silently swallows errors
**File:** `ferricstore.ex:333-337`
**Area:** Embedded API

`del(key)` returns `:ok` unconditionally. Doesn't propagate errors or return delete count.

**Proposed fix:** Return `{:ok, count}` where count is the number of keys deleted (0 or 1).

### R3-M5: Embedded API sismember() returns bare boolean (inconsistent)
**File:** `ferricstore.ex:1201-1206`
**Area:** Embedded API

Returns `true/false` directly while all other set operations return `{:ok, ...}` tuples.

**Proposed fix:** Return `{:ok, boolean()}` for consistency.

### R3-M6: CAS/LOCK/EXTEND clock inconsistency between paths
**File:** `shard.ex:2097, 2139, 2238, 2292`
**Area:** Native commands

Direct path uses `HLC.now_ms()`, Raft path uses `System.os_time(:millisecond)`. Different time sources can cause inconsistent TTLs.

**Proposed fix:** Use `HLC.now_ms()` consistently in both paths.

### R3-M7: Sandbox cleanup leaks ActiveFile registry entries
**File:** `sandbox.ex:256-308`
**Area:** Sandbox

`checkin()` doesn't deregister sandbox shards from `ActiveFile` persistent_term. Entries accumulate across test runs.

**Proposed fix:** Skip — sandbox shards don't publish to ActiveFile (line 267 in shard.ex: `unless sandbox?`). Not a real leak.

## LOW

### R3-L1: BITPOS edge case — start beyond string length searching for 0
**File:** `bitmap.ex:152-159`
**Area:** Bitmap

`BITPOS key 0 start_idx` where start >= length returns -1. Redis returns `length * 8` (virtual 0 bit after string).

### R3-L2: INFO keyspace hardcodes expires=0 and avg_ttl=0
**File:** `server.ex:441`
**Area:** Server

Doesn't track actual expiry stats. Misleading but acceptable as limitation.

### R3-L3: Undocumented cache option in embedded API never used
**File:** `ferricstore.ex:25, 306`
**Area:** Embedded API

`:cache` option documented in specs but silently ignored. Feature stub.

### R3-L4: Health check doesn't account for sandbox shards
**File:** `health.ex:155-169`
**Area:** Health

Raft leader checks fail for sandbox ra systems. Only affects in-test health checks.

### R3-L5: Patched WAL load failure not fatal
**File:** `application.ex:131, 559-577`
**Area:** Startup

If patched `ra_log_wal` can't load, falls back to unpatched silently. 10x slower writes without operator knowing.

---

## Summary

| Severity | Count | Key Areas |
|----------|-------|-----------|
| C1 | CRITICAL | NOT A BUG — sandbox routes through GenServer |
| H1 | HIGH | FIXED — set ops propagate errors |
| H2 | HIGH | FIXED — sorted set ops propagate errors |
| H3 | HIGH | NOT A BUG — DBSIZE already filters |
| H4 | HIGH | NOT A BUG — randomkey filters |
| H5 | HIGH | Regression guard |
| H6 | HIGH | FIXED — FLUSHDB clears registries |
| H7 | HIGH | Regression guard (fallback path) |
| M1 | MEDIUM | FIXED — mode doc updated |
| M2 | MEDIUM | FIXED — queries ra.members for actual role |
| M3 | MEDIUM | FIXED — validates min <= max |
| M4 | MEDIUM | FIXED — del returns {:ok, count}, multi-key |
| M5 | MEDIUM | FIXED — sismember returns {:ok, boolean} |
| M6 | MEDIUM | FIXED — HLC consistently |
| M7 | MEDIUM | NOT A BUG — sandbox never publishes |
| L1-L5 | LOW | Accepted |

