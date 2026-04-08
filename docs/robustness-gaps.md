# Robustness Gaps — Design & Implementation Plan

Known architectural gaps that affect correctness, reliability, or operational
visibility in production cluster deployments. Ordered by severity.

---

## 1. Clock Skew in TTL Expiration

**Severity: Critical (data correctness)**

TTL uses `System.os_time(:millisecond)` — wall clock. In a cluster with clock
skew between nodes, keys expire at different times on different nodes. A 30s
clock skew means a 60s TTL key expires 30s early on the ahead node.

The `Ferricstore.HLC` module exists but isn't wired into the TTL path.

**Design:** See [hlc-clock-migration.md](hlc-clock-migration.md)

**Status:** Designed, not implemented.

---

## 2. Error Propagation in the Raft Write Path

**Severity: Critical (silent data loss / caller confusion)**

The batcher silently drops errors. `pipeline_command` is fire-and-forget.
If ra fails to apply a command, the caller gets a timeout instead of a
meaningful error. Compare to PostgreSQL which propagates WAL write failures
all the way to the client with specific error codes.

**Design:** See [raft-error-propagation.md](raft-error-propagation.md)

**Status:** Designed, not implemented.

---

## 3. Shard GenServer is a Single Point of Serialization

**Severity: Medium (throughput under mixed workloads)**

Every read AND write goes through one GenServer per shard. Hot-path reads
(ETS lookups) could bypass the GenServer entirely since ETS is concurrent-safe.
A slow write (compaction, large batch) blocks reads on the same shard.

### Design

**Phase 1: Direct ETS reads (bypass GenServer)**

For simple GET operations where the key is in the hot cache (ETS), read
directly from ETS without going through the shard GenServer:

```elixir
# In Router.get:
def get(ctx, key) do
  shard_idx = shard_for(ctx, key)
  keydir = elem(ctx.keydir_refs, shard_idx)

  case ets_hot_read(keydir, key) do
    {:hit, value} -> value
    :miss -> GenServer.call(shard_name(ctx, shard_idx), {:get, key})
  end
end

defp ets_hot_read(keydir, key) do
  now = Ferricstore.HLC.now_ms()
  case :ets.lookup(keydir, key) do
    [{^key, value, exp, _lfu, _fid, _off, _vsize}]
      when value != nil and (exp == 0 or exp > now) ->
      {:hit, value}
    _ ->
      :miss
  end
end
```

This eliminates the GenServer hop for hot reads. ETS `lookup` is safe for
concurrent access. Cold reads (value == nil, need disk) still go through
the GenServer.

**Phase 2: Separate read/write GenServers** (future, more complex)

Split the shard into a write GenServer (Raft applies, compaction) and a
read path (direct ETS + cold read pool). Not needed if Phase 1 handles
the common case.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/store/router.ex` | Direct ETS read for hot-path GET |
| `lib/ferricstore/store/shard.ex` | No change needed for Phase 1 |

---

## 4. Compaction During Replication

**Severity: Medium (data sync correctness)**

The design doc says `pause_writes` blocks compaction, but `Merge.Scheduler`
runs independently. If compaction fires during a data sync copy, the file list
can change mid-copy.

### Design

**Option A: Pause compaction during sync**

Add a `sync_in_progress` flag that `Merge.Scheduler` checks:

```elixir
# In Shard, when pause_writes is called:
def handle_call({:pause_writes}, _from, state) do
  Ferricstore.Merge.Semaphore.pause(state.index)
  # ... existing flush logic
end

def handle_call({:resume_writes}, _from, state) do
  Ferricstore.Merge.Semaphore.resume(state.index)
  # ... existing resume logic
end

# In Merge.Scheduler, before starting a merge job:
def maybe_start_merge(shard_index) do
  if Semaphore.paused?(shard_index) do
    :skip  # retry next interval
  else
    do_merge(shard_index)
  end
end
```

**Option B: Snapshot file list atomically**

Instead of pausing compaction, take an atomic snapshot of the file list at
the start of the copy. If a file is deleted during copy (by compaction),
skip it — the data it contained is now in the compacted output file, which
the copy will pick up on the next iteration. This requires copy-retry logic.

Option A is simpler and correct. The write pause window is milliseconds.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/store/shard.ex` | Call `Semaphore.pause/resume` in pause/resume_writes |
| `lib/ferricstore/merge/semaphore.ex` | Add `pause/1`, `resume/1`, `paused?/1` |
| `lib/ferricstore/merge/scheduler.ex` | Check `paused?` before starting merge |

---

## 5. Recovery After Crash with Pending Async Writes

**Severity: Medium (operational visibility)**

Async writes go to ETS immediately, then fire-and-forget to Raft. On crash,
uncommitted async writes are lost. This is by design, but there's no mechanism
to detect or report how much data was lost.

### Design

**Track async write count in an atomic counter:**

```elixir
# In AsyncApplyWorker, on each batch:
:atomics.add(@async_pending_counter, shard_index + 1, batch_size)

# On Raft commit confirmation (ra_event):
:atomics.sub(@async_pending_counter, shard_index + 1, committed_count)

# On startup, after recovery:
for shard_idx <- 0..(shard_count - 1) do
  pending = :atomics.get(@async_pending_counter, shard_idx + 1)
  if pending > 0 do
    Logger.warning("Shard #{shard_idx}: #{pending} async writes lost in crash")
    :telemetry.execute(
      [:ferricstore, :recovery, :async_loss],
      %{count: pending},
      %{shard: shard_idx}
    )
  end
end
```

**Problem:** Atomics don't survive crashes. Alternative: write a periodic
checkpoint of the async pending count to a file:

```elixir
# Every 5s, write pending counts to disk:
File.write!(checkpoint_path, :erlang.term_to_binary(pending_counts))

# On startup, read checkpoint and compare with actual Raft last_applied:
# difference = keys written to ETS but not committed to Raft = lost data
```

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/raft/async_apply_worker.ex` | Track pending count |
| `lib/ferricstore/application.ex` | Recovery check on startup |

---

## 6. Keydir Memory Accounting is Approximate

**Severity: Low (eviction accuracy)**

`MemoryGuard` estimates ETS memory via `word_size * :ets.info(table, :memory)`.
This misses: Bitcask value bytes cached in ETS, compound key overhead,
promotion markers.

### Design

**More accurate accounting:**

```elixir
defp keydir_memory_bytes(shard_index) do
  keydir = :"keydir_#{shard_index}"

  # ETS table memory (includes keys, tuple overhead)
  ets_words = :ets.info(keydir, :memory)
  ets_bytes = ets_words * :erlang.system_info(:wordsize)

  # Binary heap: count refc binaries held by ETS
  # :ets.info(table, :memory) does NOT include off-heap refc binaries
  # (values > 64 bytes). We need to sample.
  binary_bytes = sample_binary_overhead(keydir, 100)

  ets_bytes + binary_bytes
end

defp sample_binary_overhead(keydir, sample_size) do
  total = :ets.info(keydir, :size)
  if total == 0, do: 0, else: do_sample(keydir, sample_size, total)
end

defp do_sample(keydir, sample_size, total) do
  # Sample N random entries, measure binary sizes, extrapolate
  sampled_bytes =
    :ets.tab2list(keydir)
    |> Enum.take_random(min(sample_size, total))
    |> Enum.reduce(0, fn {_k, value, _exp, _lfu, _fid, _off, vsize}, acc ->
      if is_binary(value), do: acc + byte_size(value), else: acc + vsize
    end)

  avg = sampled_bytes / min(sample_size, total)
  round(avg * total)
end
```

This is expensive. Run it periodically (every 30s), not on every check.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/memory_guard.ex` | Add binary sampling to memory calculation |

---

## 7. No Read-Your-Writes Guarantee Across Nodes

**Severity: Low (consistency model)**

Client writes to leader, gets `:ok`. Immediately reads from a follower — the
follower might not have applied the entry yet. There's no session consistency
or causal consistency mechanism.

### Design

**Option A: Read-after-write token**

The write response includes the Raft index at which the write was applied.
The client passes this token on subsequent reads. The reader waits until
its local `last_applied` index reaches the token:

```elixir
# Write response (from Batcher):
{:ok, %{raft_index: 12345}}

# Read with consistency token:
Router.get(ctx, key, min_index: 12345)

# In Shard.handle_call({:get, key, opts}):
case Keyword.get(opts, :min_index) do
  nil -> do_read(key)
  min_idx ->
    if state.last_applied >= min_idx do
      do_read(key)
    else
      # Wait for Raft to catch up (with timeout)
      {:noreply, enqueue_deferred_read(state, key, min_idx, from)}
    end
end
```

**Option B: Leader reads**

Route reads to the leader for strong consistency. Simple but defeats the
purpose of replication for reads. Not recommended as default.

**Option C: Follower read with index check (preferred)**

Like Option A but simpler — the Router checks the local shard's
`last_applied` against the cluster leader's `commit_index`. If they match,
the local data is up-to-date. If not, forward the read to the leader:

```elixir
def consistent_get(ctx, key) do
  shard_idx = shard_for(ctx, key)
  local_applied = get_local_last_applied(shard_idx)
  leader_committed = get_leader_commit_index(shard_idx)

  if local_applied >= leader_committed do
    get(ctx, key)  # local read, up-to-date
  else
    forward_read_to_leader(ctx, key, shard_idx)
  end
end
```

This adds a network round-trip only when the follower is behind. For most
reads (follower caught up within 1-10ms), the local path is taken.

### Files to change

| File | Change |
|------|--------|
| `lib/ferricstore/store/router.ex` | Add `consistent_get` with index check |
| `lib/ferricstore/raft/cluster.ex` | Add `get_commit_index` helper |

---

## Implementation Priority

| # | Gap | Severity | Effort | Depends on |
|---|-----|----------|--------|------------|
| 1 | HLC clock migration | Critical | Medium | None |
| 2 | Raft error propagation | Critical | Medium | None |
| 3 | Direct ETS reads | Medium | Low | None |
| 4 | Compaction during replication | Medium | Low | None |
| 5 | Async write loss reporting | Medium | Low | None |
| 6 | Keydir memory accuracy | Low | Low | None |
| 7 | Read-your-writes | Low | Medium | None |
