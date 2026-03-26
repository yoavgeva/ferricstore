---
name: Async large value read-back bug
description: Large values (>64KB) in async durability mode may return nil on immediate read-back due to stale ETS offset
type: project
---

Large values (> hot_cache_max_value_size, default 64KB) stored via async durability
mode may return nil on immediate read-back. The Shard's non-raft PUT path writes
ETS with file_id=0, offset=0 (unknown disk location), then queues for async Bitcask
flush. The GET cold path flushes pending writes but uses the stale offset captured
before the flush.

**Impact:** Only affects values > 64KB in async durability namespaces. Small values
are stored inline in ETS and readable immediately.

**Fix needed:** After `flush_pending_sync` in the GET cold path, re-read the ETS entry
to get the updated disk offset. Or write large values to Bitcask synchronously in the
async PUT path before returning.

**Why:** This is a pre-existing bug in the Shard's non-raft code path, not introduced
by the async durability work. The non-raft path was rarely used before we exposed it
via `{:async_execute}`.
