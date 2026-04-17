# Background Bitcask Fsync (Postgres-Style Checkpointing)

## Problem

Every write in FerricStore currently incurs **two fsyncs**:

1. **Ra WAL fsync** (`fdatasync` via patched `ra_log_wal`) — durability of the
   consensus log.
2. **Bitcask fsync** (`v2_fsync` inside `StateMachine.apply/3`) — durability of
   the data file after `v2_append_batch_nosync`.

The Bitcask fsync:
- Runs **synchronously inside the Ra state machine** apply/3 callback.
- Blocks the Ra server process for ~100–200μs per batch on NVMe.
- Is **redundant** — the data is already durable in the Ra WAL (fsynced by the
  WAL writer). If we crash before the Bitcask fsync lands, replay of the Ra log
  will rewrite the lost bytes to Bitcask during recovery.

Profiling on Azure showed Ra servers spending significant time blocked on
`v2_fsync` despite the data already being durable elsewhere.

## Proposal

Remove the per-apply Bitcask fsync from `StateMachine.apply/3`. Data files
become *page-cache-only* between checkpoints. A background process fsyncs each
shard's active Bitcask file every N seconds (default: 60s), matching how
Postgres treats its heap/indexes (durability via WAL, data files checkpointed
periodically).

```
           Today (synchronous per-batch fsync)
Ra apply  →  v2_append_batch_nosync  →  v2_fsync  →  ETS update  →  return
            ~10μs                        ~100–200μs     ~1μs

           Proposed (background checkpoint)
Ra apply  →  v2_append_batch_nosync  →  ETS update  →  return
            ~10μs                        ~1μs

Checkpoint (every 60s, per shard, background Tokio thread):
   v2_fsync_async(active_file_path)  →  log completion
```

## Durability Model

### Current

- Ra WAL is fsynced per WAL batch. Commit ACK returned only after fdatasync.
- Bitcask data file is fsynced per apply batch. Apply returns only after fsync.

On crash: both WAL and Bitcask are durable on disk. No data loss if process
crashes. Kernel panic loses at most a few ms of writes (anything not yet
flushed by either fsync).

### Proposed

- Ra WAL is fsynced per WAL batch (unchanged).
- Bitcask data file is fsynced every 60s by the checkpoint process.

On process crash: Ra WAL replays, state machine re-applies all post-checkpoint
commands, Bitcask files are rebuilt exactly. No data loss.

On kernel panic: Ra WAL is durable (fsynced), Bitcask may be up to 60s stale.
On restart, Ra replays the log from the last applied index — any commands the
state machine applied but whose Bitcask write hadn't yet fsynced will be
re-applied, rewriting them to Bitcask.

**No data loss**, because the Ra WAL is the source of truth for everything the
state machine acknowledged.

### What's lost compared to today?

- **Nothing that's been acknowledged to a client.** All acknowledged data is in
  the fsynced Ra WAL.
- **Replay time grows** by up to 60 seconds of writes on restart after kernel
  panic. At 40K writes/sec × 60s = up to 2.4M entries to replay. Each entry is
  cheap (`v2_append_batch_nosync` + ETS insert in the state machine). Expected
  recovery overhead: ~10–30 seconds of replay before ready-for-traffic.

## Comparison to Postgres

| Aspect | Postgres | FerricStore (proposed) |
|--------|----------|------------------------|
| WAL durability | Per-commit `fsync` of pg_wal | Per-batch fdatasync of Ra WAL |
| Data file durability | Checkpoints (default: 5 min, 1 GB max_wal_size) | Checkpoints (default: 60s per shard) |
| Recovery source | WAL replay from last checkpoint | Ra WAL replay from last applied index |
| Crash safety | Full WAL replay recovers all acked data | Same |
| Checkpoint I/O | Dirty buffers flushed by bgwriter + checkpointer | `v2_fsync_async` per active file |

Postgres uses a 5-minute default; we pick 60s to start because Bitcask files
are append-only and smaller, and we want faster crash-recovery bounds.

## Design

### Components

1. **Remove** `quorum_fsync(state)` call from `StateMachine.flush_pending_writes/1`.
2. **Add** `Ferricstore.Store.BitcaskCheckpointer` — one GenServer per shard or
   a single global GenServer managing all shards (simpler: per-shard).
3. The checkpointer:
   - Runs a timer every `:bitcask_checkpoint_interval_ms` (default: 60_000).
   - On tick: reads the current active file path from the `ActiveFile` registry
     and calls `NIF.v2_fsync_async(self(), corr_id, path)`.
     - `v2_fsync_async` submits to Tokio, completion reported via message.
   - Tracks checkpoint latency/success in telemetry.
   - Skips if no writes have happened since the last checkpoint (per-shard
     `fsync_needed` flag, flipped by `flush_pending_writes`).
4. **Graceful shutdown**: on application stop, the checkpointer performs a
   final synchronous `v2_fsync` before letting the supervisor terminate.

### The `fsync_needed` Flag

To avoid fsyncing idle files, each shard's state machine sets an atomic flag
after each `v2_append_batch_nosync`:

```elixir
defp flush_pending_writes(state) do
  case Process.put(:sm_pending_writes, []) do
    [] -> :ok
    pending ->
      NIF.v2_append_batch_nosync(state.active_file_path, batch)
      # Mark this shard as needing a checkpoint
      :atomics.put(state.checkpoint_flags, state.shard_index + 1, 1)
      update_ets_locations(...)
  end
end
```

The checkpointer:
```elixir
def handle_info(:checkpoint, state) do
  if :atomics.get(state.checkpoint_flags, state.shard_index + 1) == 1 do
    :atomics.put(state.checkpoint_flags, state.shard_index + 1, 0)
    NIF.v2_fsync_async(self(), state.next_corr, state.active_file_path)
    ...
  end
  Process.send_after(self(), :checkpoint, state.interval_ms)
  {:noreply, state}
end
```

This avoids fsync syscalls on idle shards.

### Active File Rotation

Files rotate when they exceed `max_active_file_size` (256 MB default). On
rotation:

- The Shard GenServer performs a **synchronous fsync** of the old file before
  switching. This is a rare event (~every 256 MB of writes) and ensures
  rotated-out files are fully durable regardless of checkpoint timing.
- The checkpointer reads `ActiveFile.get(idx)` each tick to get the current
  path — it doesn't hold a stale reference.

### Configuration

```elixir
config :ferricstore,
  bitcask_checkpoint_interval_ms: 60_000  # default 60s
  # Set to 0 to disable background checkpoints (fallback to per-apply fsync
  # for users who want the pre-change behavior — useful during initial rollout
  # or for environments with non-crash-safe WAL).
```

If `bitcask_checkpoint_interval_ms == 0`: keep `quorum_fsync` in the state
machine (legacy path).

### Telemetry

Emit events from the checkpointer:

```elixir
:telemetry.execute(
  [:ferricstore, :bitcask, :checkpoint],
  %{duration_us: duration_us, shard_index: idx},
  %{status: :ok | :error}
)
```

Operators can plot checkpoint latency, frequency, and errors.

## Failure Modes

### Checkpoint fails (disk full, I/O error)

- Log error at WARN level.
- Set `DiskPressure.set(ctx, shard_index)` — this already throttles new writes.
- Keep `fsync_needed` flag set so the next tick retries.
- Do NOT crash the checkpointer.

### Process dies mid-checkpoint

- Tokio thread finishes the fsync independently (it's off-BEAM). Result
  message arrives at a dead pid — discarded silently.
- Supervisor restarts the checkpointer; next tick retries based on the flag.

### Kernel panic between checkpoints

- Bitcask may be up to 60s stale on that shard.
- Ra WAL is durable; replay rebuilds the stale portion on restart.
- No acknowledged data is lost.

### Very slow disk (fsync takes > 60s)

- Next tick would fire while the previous async fsync is still pending.
- Simple mitigation: the checkpointer tracks `in_flight: boolean` and skips
  the tick if an fsync is still outstanding.

## Testing

1. **Unit**: checkpointer timer fires at the configured interval.
2. **Unit**: `fsync_needed` flag correctly gates fsync calls (no fsync when
   idle; fsync after a write).
3. **Integration**: write 1M entries, kill BEAM mid-checkpoint, restart, verify
   all entries are readable (Ra replay rebuilds Bitcask).
4. **Integration**: artificially slow down `v2_fsync_async` (Tokio sleep),
   verify checkpointer doesn't pile up multiple in-flight fsyncs.
5. **Chaos**: kernel panic simulation (forcibly kill the VM without clean
   shutdown, restart, measure recovery time). Expect <30s recovery for 2.4M
   replayed entries.
6. **Benchmark**: async write throughput before/after. Expected improvement:
   Ra server no longer blocks 100–200μs per apply batch.

## Migration / Rollout

1. Land the checkpointer code behind a config flag
   (`bitcask_checkpoint_interval_ms: 0` preserves existing behavior).
2. Enable in dev/staging with `60_000`. Observe telemetry.
3. Benchmark production-like workloads.
4. Flip the default to `60_000` for production once verified.
5. Post-rollout: consider lowering to 30s or 10s if recovery time targets
   allow it (shorter checkpoint interval → shorter replay window).

## Open Questions

- **One checkpointer per shard vs one global?** Per-shard is simpler (each
  shard has its own Tokio thread via `v2_fsync_async`), and fsyncs naturally
  parallelize across shards. Recommendation: per-shard.
- **Should checkpoint also rotate the log when a shard has been idle for a
  long time?** Not for this initial change; log rotation is size-driven.
- **Do we need `fdatasync` vs `fsync`?** Current code uses `v2_fsync` which
  maps to `fsync`. `fdatasync` is ~30% faster for append-only files (metadata
  doesn't change). Worth switching — separate work item.
- **Interaction with async writes**: async writes already return `:ok` before
  the state machine applies. The Bitcask fsync was a "best effort" disk
  persistence for async. With the checkpointer, async durability is bounded
  by the checkpoint interval (up to 60s). This is documented async behavior.

## Expected Impact

- **Throughput**: per-shard write throughput increases. Ra servers spend less
  time blocked in `v2_fsync`. Measured in similar systems: 10–30% improvement
  at saturation.
- **Latency**: p99 write latency drops (no 100–200μs fsync stall per apply
  batch).
- **Recovery time**: slightly longer (up to 60s of WAL to replay post-crash)
  but still fast in absolute terms.
- **Code simplicity**: state machine becomes simpler — no fsync dance inside
  apply/3; checkpointer is a small, self-contained GenServer.
