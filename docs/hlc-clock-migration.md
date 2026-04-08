# HLC Clock Migration: Eliminate Wall Clock Skew in TTL

## Problem

94 calls to `System.os_time` and `System.monotonic_time` across the codebase.
68 of those are TTL/expiry-related. In a cluster with clock skew between nodes:

- Node A sets key with TTL 60s at `System.os_time` = 1000000
- Node B's clock is 30s ahead, so `System.os_time` = 1000030
- Node B checks expiry: `1000060 <= 1000030` = not expired yet, but will expire
  30s early from the user's perspective
- Worse: if Node B is behind by 30s, the key lives 30s longer than intended

The HLC module (`Ferricstore.HLC`) already exists and provides `now_ms/0` which
returns a monotonically increasing timestamp that accounts for clock skew via
the Hybrid Logical Clock algorithm. It's just not wired into the TTL path.

## Principle

**`Ferricstore.HLC.now_ms()` is the ONLY source of truth for time in FerricStore.**

- All TTL computation: `HLC.now_ms() + ttl_ms`
- All expiry checks: `expire_at_ms > 0 and expire_at_ms <= HLC.now_ms()`
- All "current time" reads for data operations: `HLC.now_ms()`

Exceptions (where wall clock / monotonic clock is correct):
- **Telemetry/metrics duration**: `System.monotonic_time` for elapsed time measurement
- **SlowLog timestamps**: wall clock for human-readable logging
- **Process timeouts**: `Process.send_after`, GenServer timeouts (local-only, no replication)
- **Waiters FIFO ordering**: monotonic time for local ordering (not replicated)

## Classification of All 94 Clock Calls

### MUST migrate to HLC (TTL/expiry path -- replicated across nodes)

| File | Line | Current | Purpose |
|------|------|---------|---------|
| `impl.ex` | 39 | `System.os_time(:millisecond) + ttl` | TTL computation |
| `impl.ex` | 221 | `System.os_time(:millisecond) + ttl` | getex TTL |
| `impl.ex` | 240 | `System.os_time(:millisecond) + seconds * 1000` | setex |
| `impl.ex` | 248 | `System.os_time(:millisecond) + milliseconds` | psetex |
| `ferricstore.ex` | ~50 calls | `System.os_time(:millisecond) + ttl` | All TTL in top-level API |
| `commands/hash.ex` | 294,326,395,427,589,805,814 | `System.os_time(:millisecond)` | Hash field TTL |
| `commands/native.ex` | 87 | `System.os_time(:millisecond)` | Ratelimit window |
| `commands/expiry.ex` | (via Expiry.handle) | `System.os_time(:millisecond)` | EXPIRE/PEXPIRE/EXPIREAT |
| `raft/state_machine.ex` | 1133,1162,1190,1232,1911,1956,1994,2045 | `System.os_time(:millisecond)` | Expiry checks in apply |
| `store/shard/ets.ex` | (expiry sweep) | `System.os_time(:millisecond)` | Key expiration sweep |
| `store/shard/lifecycle.ex` | (recover) | `System.os_time(:millisecond)` | Expiry during recovery |
| `cross_shard_op.ex` | 191,316 | `System.os_time(:millisecond)` | Intent TTL |
| `cross_shard_op/intent_resolver.ex` | 62 | `System.os_time(:millisecond)` | Stale intent check |
| `memory_guard.ex` | 465 | `System.os_time(:millisecond)` | Expiry during eviction |
| `fetch_or_compute.ex` | 171 | `System.os_time(:millisecond) + ttl_ms` | Cache TTL |

### KEEP as-is (local-only, not replicated)

| File | Line | Current | Reason to keep |
|------|------|---------|---------------|
| `dispatcher.ex` | 138,142 | `System.monotonic_time(:microsecond)` | Duration measurement for SlowLog |
| `slowlog.ex` | 184 | `System.os_time(:microsecond)` | Human-readable timestamp for logs |
| `audit_log.ex` | 191 | `System.os_time(:microsecond)` | Human-readable audit timestamp |
| `metrics.ex` | 213 | `System.os_time(:millisecond)` | Prometheus scrape expiry check |
| `server.ex` | 367 | `System.os_time(:second)` | LASTSAVE Redis compat response |
| `server.ex` | 854 | `System.os_time(:millisecond)` | INFO keyspace avg TTL display |
| `waiters.ex` | 54 | `System.monotonic_time(:microsecond)` | FIFO ordering (local only) |
| `commands/stream.ex` | 346 | `System.monotonic_time(:microsecond)` | BLPOP waiter ordering |
| `raft/async_apply_worker.ex` | 202,207 | `System.monotonic_time(:microsecond)` | Telemetry duration |
| `fetch_or_compute.ex` | 156,217,235 | `System.monotonic_time(:millisecond)` | Local lock/timeout (not replicated) |

## Implementation

### Step 1: Add `Ferricstore.HLC.now_ms/0` to all TTL computation sites

Replace:
```elixir
expire_at_ms = System.os_time(:millisecond) + ttl
```

With:
```elixir
expire_at_ms = Ferricstore.HLC.now_ms() + ttl
```

### Step 2: Add `Ferricstore.HLC.now_ms/0` to all expiry check sites

Replace:
```elixir
now = System.os_time(:millisecond)
if expire_at_ms > 0 and expire_at_ms <= now, do: :expired
```

With:
```elixir
now = Ferricstore.HLC.now_ms()
if expire_at_ms > 0 and expire_at_ms <= now, do: :expired
```

### Step 3: Wire HLC update into Raft replication

When a follower receives a Raft entry from the leader, the entry's timestamp
should update the follower's HLC. This keeps clocks converging:

```elixir
# In StateMachine.apply/3, at the top of every apply clause:
def apply(%{index: idx} = meta, command, state) do
  # If the command carries a timestamp, update our HLC
  # This happens on followers receiving replicated entries
  maybe_update_hlc(meta)
  # ... rest of apply
end

defp maybe_update_hlc(%{system_time: ts}) when is_integer(ts) do
  Ferricstore.HLC.update(%{wall: ts, logical: 0})
end
defp maybe_update_hlc(_), do: :ok
```

Ra includes `system_time` in the metadata of each log entry. This is the
leader's wall clock at the time the entry was proposed. By feeding this into
`HLC.update/1`, followers' clocks converge toward the leader's clock, bounded
by the HLC's max drift tolerance.

### Step 4: Add drift monitoring

```elixir
# In MemoryGuard or a dedicated health check:
drift = Ferricstore.HLC.drift_ms()
if drift > 5_000 do
  Logger.warning("HLC drift #{drift}ms exceeds 5s threshold")
  :telemetry.execute([:ferricstore, :hlc, :drift], %{ms: drift}, %{})
end
```

## Files to Change

| File | Changes |
|------|---------|
| `impl.ex` | 4 sites: replace `System.os_time` with `HLC.now_ms()` |
| `ferricstore.ex` | ~50 sites: all TTL computation in top-level API |
| `commands/hash.ex` | 7 sites: hash field TTL |
| `commands/native.ex` | 1 site: ratelimit window |
| `commands/expiry.ex` | All EXPIRE/PEXPIRE/EXPIREAT handlers |
| `raft/state_machine.ex` | 8 sites: expiry checks in apply + HLC update |
| `store/shard/ets.ex` | Expiry sweep |
| `store/shard/lifecycle.ex` | Recovery expiry |
| `cross_shard_op.ex` | 2 sites: intent TTL |
| `cross_shard_op/intent_resolver.ex` | 1 site: stale intent check |
| `memory_guard.ex` | 1 site: eviction expiry |
| `fetch_or_compute.ex` | 1 site: cache TTL |

## What NOT to Change

- `System.monotonic_time` for duration measurement (telemetry, slowlog)
- `System.os_time` for human-readable timestamps (audit log, LASTSAVE)
- Process-local timeouts and ordering (waiters, send_after)
- `HLC` module itself (already correct)

## Verification

1. Unit test: set key with TTL on node A, verify expiry time uses HLC
2. Cluster test: 2 nodes with artificial clock skew (mock System.os_time),
   verify key expires at the same logical time on both nodes
3. Existing TTL tests pass unchanged (HLC.now_ms ~= System.os_time when no skew)
