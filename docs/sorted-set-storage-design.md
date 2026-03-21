# Sorted Set Storage Design

This document describes the current sorted set storage approach in FerricStore,
the spec-recommended approach using per-key ETS tables, a practical migration
plan, and performance characteristics of each.

## Current Approach: Compound Keys in Shared Bitcask

Each sorted set member is stored as an individual Bitcask entry using a compound
key format:

```
Z:redis_key\0member -> score_string
```

Where `\0` is a null byte separator.

### How Operations Work

| Operation      | Implementation                                           | Complexity    |
|---------------|----------------------------------------------------------|---------------|
| ZADD          | `compound_put(key, "Z:key\0member", score_str, 0)`      | O(1) per member |
| ZSCORE        | `compound_get(key, "Z:key\0member")`                     | O(1)          |
| ZCARD         | `compound_count(key, "Z:key\0")`                         | O(N) scan     |
| ZRANGE        | `compound_scan` + parse scores + `Enum.sort_by`          | O(N log N)    |
| ZRANGEBYSCORE | `compound_scan` + parse + sort + filter                  | O(N log N)    |
| ZRANK         | `compound_scan` + parse + sort + `find_index`            | O(N log N)    |
| ZREM          | `compound_delete(key, "Z:key\0member")`                  | O(1)          |
| ZSCAN         | `compound_scan` + optional MATCH filter                  | O(N)          |

The `compound_scan` callback iterates all Bitcask entries with the
`Z:redis_key\0` prefix, returning `[{member, score_str}]` pairs. This is a
linear scan over the shard's ETS keydir (in-memory) followed by score parsing.

### Strengths

- Simple to implement and reason about.
- ZSCORE and ZADD are O(1) -- no scan needed for point lookups or writes.
- Durability comes for free: Bitcask appends are crash-safe.
- No extra memory beyond the normal keydir + hot_cache.
- Works well for sorted sets with fewer than ~10K members (typical cache
  workloads).

### Weaknesses

- Range queries (ZRANGE, ZRANGEBYSCORE, ZRANK, ZCOUNT) require a full scan of
  all members, score parsing, and an in-memory sort every time they are called.
  This is O(N log N) per call.
- ZCARD requires a prefix scan and count -- O(N) instead of O(1).
- No way to do range queries by score without loading all members first.
- For sorted sets with 100K+ members, the repeated scan-and-sort becomes the
  dominant cost.

## Spec Approach: Per-Key Dual ETS Tables

The spec (ferricstore-spec.docx) recommends maintaining two ETS tables per
sorted set key:

1. **`:ordered_set` ETS table** keyed by `{score, member}` -- enables efficient
   range queries via `:ets.select` with match specs. Score-ordered iteration
   is O(K) where K is the result set size (not the total set size).

2. **`:set` ETS table** keyed by `member` -- enables O(1) ZSCORE lookups
   without scanning.

### How Operations Would Work

| Operation      | Implementation                                       | Complexity |
|---------------|------------------------------------------------------|------------|
| ZADD          | Insert into both ETS tables + write to Bitcask       | O(log N)   |
| ZSCORE        | Lookup in member->score ETS table                    | O(1)       |
| ZCARD         | `:ets.info(table, :size)`                            | O(1)       |
| ZRANGE        | `:ets.select` with index range on ordered_set        | O(K)       |
| ZRANGEBYSCORE | `:ets.select` with score bounds on ordered_set       | O(K + log N) |
| ZRANK         | Count entries in ordered_set with score < target     | O(log N)   |
| ZREM          | Delete from both ETS tables + delete from Bitcask    | O(log N)   |

Where K is the result set size, N is the total set size.

### Strengths

- Range queries are efficient: O(K) instead of O(N log N).
- ZCARD is O(1).
- ZRANK can be done without loading all members.
- Scales well to sorted sets with millions of members.

### Weaknesses

- Complexity: two ETS tables per sorted set key means dynamic table creation
  and cleanup (on DEL, EXPIRE, key eviction).
- Memory: ETS tables have per-table overhead (~300 bytes minimum). A keyspace
  with 100K sorted set keys would create 200K ETS tables.
- Recovery: on shard restart, all ETS tables must be rebuilt from Bitcask by
  scanning all `Z:` prefix entries. This adds to startup time.
- Consistency: writes must update both tables atomically. A crash between the
  two ETS writes could leave them inconsistent (though Bitcask remains the
  source of truth for recovery).
- Integration with expiry and eviction becomes more complex -- MemoryGuard
  would need to know about these ETS tables.

## Migration Plan

### Phase 1: Current State (Compound Keys) -- DONE

The compound key approach is implemented and all 115+ tests pass. Performance
tests confirm:

- ZADD 10K members + ZRANGE 0 -1: completes in well under 100ms
- ZRANGEBYSCORE on 10K members: completes in well under 100ms
- ZSCORE remains O(1) regardless of set size

This is acceptable for cache workloads where sorted sets typically have fewer
than 50K members.

### Phase 2: Add Hot Cache for Sorted Member Lists (Low Risk)

Cache the sorted `[{member, score}]` list in hot_cache after the first
`load_sorted_members` call. Invalidate the cache entry on ZADD, ZREM,
ZINCRBY, ZPOPMIN, ZPOPMAX.

Benefits:
- Repeated ZRANGE / ZRANGEBYSCORE calls on the same key avoid re-scanning
  and re-sorting.
- Simple to implement: ~30 lines of code in `sorted_set.ex`.
- No structural changes to storage.

Risks:
- Cache coherency: must invalidate on every mutation.
- Memory: cached lists for large sorted sets consume additional RAM.

### Phase 3: Lazy ETS Promotion (Medium Risk)

For sorted sets that exceed a configurable threshold (e.g., 10K members),
lazily promote them to the dual-ETS scheme:

1. On the first range query for a key with >10K members, create the two ETS
   tables and populate them from the Bitcask compound keys.
2. Subsequent range queries use the ETS tables directly.
3. On mutations (ZADD, ZREM, etc.), update both ETS tables AND the Bitcask
   compound keys (Bitcask remains the durable source of truth).
4. On shard restart, the ETS tables are rebuilt from Bitcask.
5. On DEL or EXPIRE, destroy the ETS tables.

This gives the best of both worlds: small sorted sets use the simple compound
key path, while large sorted sets get O(K) range queries.

### Phase 4: Full ETS for All Sorted Sets (High Risk)

Only if profiling shows Phase 3 is insufficient. Create ETS tables for every
sorted set key at shard startup time. This is the full spec approach.

## Performance Characteristics Summary

| Operation      | Compound Keys (Current) | Hot Cache (Phase 2) | Dual ETS (Phase 3/4) |
|---------------|------------------------|--------------------|--------------------|
| ZADD          | O(1)                   | O(1) + invalidate  | O(log N) + Bitcask |
| ZSCORE        | O(1)                   | O(1)               | O(1)               |
| ZCARD         | O(N) scan              | O(N) scan          | O(1)               |
| ZRANGE full   | O(N log N)             | O(1) cache hit     | O(N)               |
| ZRANGE slice  | O(N log N)             | O(1) cache hit     | O(K + log N)       |
| ZRANGEBYSCORE | O(N log N)             | O(N) filter        | O(K + log N)       |
| ZRANK         | O(N log N)             | O(N) scan          | O(log N)           |
| ZREM          | O(1)                   | O(1) + invalidate  | O(log N) + Bitcask |
| Memory (idle) | Bitcask keydir only    | + cached list      | + 2 ETS tables/key |
| Recovery      | None needed            | None needed        | Rebuild from Bitcask |

N = total members in the sorted set, K = result set size.

## Recommendation

Stay with the compound key approach (Phase 1) for the current release. The
performance tests demonstrate it handles 10K-member sorted sets well within
the <100ms target. Add Phase 2 (hot cache) as the next optimization if
profiling shows repeated range queries on the same key are a bottleneck.
Defer Phase 3 (lazy ETS promotion) until a concrete use case with 100K+ member
sorted sets emerges.
