# Bitcask Durability: Checkpointed Writes + Full File/Dir Fsync

> **Scope note**: Originally scoped as "replace per-apply fsync with a
> background checkpointer". Audit surfaced that every Bitcask file-and-
> directory mutator has related durability gaps. This design now covers
> the full Bitcask durability story — checkpointer for active-log
> appends, plus file/dir fsync at every namespace-mutating site
> (rotation, compaction, hint files, prob creates, dedicated-file
> promotion and cleanup, shard init, manifest persistence). All these
> paths handle files, so treating them as a single consistent story
> makes it easier to reason about crash recovery.
>
> The state-machine RMW migration
> (SETBIT / HINCRBY / ZINCRBY / PFADD / JSON.* / BITOP / GEOADD / TDigest)
> routes more traffic through `apply/3`; the checkpointer benefits all
> of them uniformly — see [The `fsync_needed` Flag](#the-fsync_needed-flag)
> below.
>
> **Already landed** (foundations):
> - ✅ `NIF.v2_fsync_dir(path)` helper + 10 edge-case Rust tests
> - ✅ `prob_fsync` helper + per-write fsync in bloom/cuckoo/cms/topk
>   write NIFs + 8 Rust tests + 11 Elixir durability-invariant tests
>
> **Ordering** for the remaining work is in
> [Appendix B](#appendix-b-priority-ranked-follow-up-work).

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
after each `v2_append_batch_nosync`.

**Single place to change**: `flush_pending_writes/1` is the only function
that calls `v2_append_batch_nosync`, so the flag is set exactly once per
applied command batch regardless of how many RMW command types land there.
All RMW paths (plain `:put`/`:delete`, the atomic RMW commands like
`:incr`/`:append`/`:setbit`/`:hincrby`/`:zincrby`, and the command-handler
dispatchers `:json_op`/`:hll_op`/`:bitmap_op`/`:geo_op`/`:tdigest_op`) go
through the same pipeline:

```
apply(meta, cmd, state)
    └─ with_pending_writes(state, fn -> do_<command>(state, ...) end)
           ├─ Process.put(:sm_pending_writes, [])
           ├─ fun.() which ultimately calls do_put(state, ...)
           │    └─ appends to Process dictionary via sm_pending_writes
           └─ flush_pending_writes(state)    ← the single write point
                ├─ NIF.v2_append_batch_nosync(state.active_file_path, batch)
                ├─ :atomics.put(state.checkpoint_flags, idx + 1, 1)   ← NEW
                ├─ (quorum_fsync removed — that's the whole point)
                └─ update_ets_locations(...)
```

Because `flush_pending_writes/1` is the single NIF write point, flipping
the flag there captures every RMW from every command family introduced by
the state-machine migration. No per-command changes needed.

```elixir
defp flush_pending_writes(state) do
  case Process.put(:sm_pending_writes, []) do
    [] -> :ok
    pending ->
      NIF.v2_append_batch_nosync(state.active_file_path, batch)
      # Mark this shard as needing a checkpoint. One atomic write per batch,
      # not per command, because flush_pending_writes is called once per
      # apply/3 call.
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

Files rotate when they exceed `max_active_file_size` (256 MB default).

**Invariant**: each shard has exactly **one** active file at a time (see
`Ferricstore.Store.ActiveFile` — maps `shard_index → {file_id, file_path,
shard_data_path}`, single entry). Old files are read-only; they never
receive appends after rotation.

**Why the checkpointer tracks only ONE file per shard**: old files are
already finalized — once we fsync them at the rotation handoff, no new
bytes land in them. So the checkpointer's scope is the active file, and
the rotation handler is the handoff point between "checkpointer syncs
active file N" and "checkpointer syncs active file N+1".

**The subtle timing problem**: between "append the last byte to N" and
"publish N+1 as active", the checkpointer might tick and end up fsyncing
N+1 (new) instead of N (old, with un-fsynced tail bytes).

```
t=0   append bytes to 00005.log  (256 MB reached)
t=1   maybe_rotate_file called
t=2     - write_hint_for_file(5)
t=3     - File.touch!(00006.log)
t=4     - ActiveFile.publish(6)           ← checkpointer now sees 00006
t=5     - [state returned, active_file_id = 6]
```

If a kernel panic happens at t=2..t=5 without the intervening fsync,
the last bytes of `00005.log` (written up to t=0) may be lost.

**Fix**: insert a synchronous `v2_fsync(old_active_path)` between t=1
and t=2, BEFORE any publish of the new file. This atomically transfers
"responsibility for N" from the checkpointer's future work to "already
done, don't worry about it".

Handoff at rotation:

- Immediately before flipping `active_file_id`, `maybe_rotate_file` must
  call **synchronous** `v2_fsync(old_active_path)`. Otherwise the last
  writes to the old file might never be fsynced by the checkpointer (whose
  next tick will fsync the NEW active file).
  *This is not what the current code does* — `maybe_rotate_file` writes the
  hint file and touches the new file but does not fsync the outgoing
  active file. **Must be added as part of this change.**
- The checkpointer reads `ActiveFile.get(idx)` each tick to get the current
  path — it does not hold a stale reference, so a rotation between ticks
  is naturally handled as "old file already fully synced, new file not yet
  written much to, no work to do".

### All File Families in a Shard

A shard doesn't have a single file — it has a tree of them. Understanding
which ones this checkpointer covers is critical:

| File family | Path | Who writes | Fsync today | Checkpointer scope? |
|-------------|------|-----------|-------------|---------------------|
| **Shared Bitcask active log** | `data/shard_N/NNNNN.log` | `v2_append_batch_nosync` in `flush_pending_writes` | **Per-apply** `quorum_fsync` | ✅ **This design replaces it** |
| Shared Bitcask read-only logs | `data/shard_N/NNNNN.log` (rotated) | — (immutable) | fsynced at rotation handoff | No — already finalized |
| **Hint files** | `data/shard_N/NNNNN.hint` | `v2_write_hint_file` at rotation | ❌ **Not fsynced after write** | **⚠ GAP** — see below |
| **Dedicated (promoted) Bitcask** | `data/dedicated/shard_N/{type}:{hash}/NNNNN.log` | `v2_append_record` / `v2_append_tombstone` / `v2_append_batch` — **all call `writer.sync()` per write** | **Per-write fsync** today | ⚠ **Opportunity** — if we move to checkpoint-style for dedicated files too, big win; see below |
| **Prob structure files** (bloom / cuckoo / cms / topk) | `data/shard_N/prob/{type}_{key}` | pread/pwrite per-bit (page cache only) | **Only at `create`**; subsequent writes are page-cache-only | **🚨 SERIOUS GAP** — see below |

### 🚨 Probabilistic structures: fsync per write required

All four prob structures need `sync_data()` at the end of every write
operation. Today they only fsync at CREATE, and every subsequent write
(BF.ADD / CF.ADD / CMS.INCRBY / TOPK.ADD) goes to page cache only. This
is a **pre-existing correctness bug**.

**Why each one needs per-write fsync** (none are fully idempotent under
Raft replay after a partial-write crash):

- **CMS** — `counter = read() + delta; write(counter)` is RMW. Partial
  crash + replay double-counts. Cross-replica divergence possible.
- **TopK** — decay-sketch heap updates. Partial crash leaves heap in
  inconsistent state, replay corrupts further.
- **Bloom** — bit-set is idempotent but the header `count` field is NOT.
  Partial writes can desync `count` from actual bits set, breaking
  `BF.CARD`.
- **Cuckoo** — a single add may write to multiple slots (kick-chain on
  collision). Partial crash can leave a fingerprint at the old slot
  with nothing at the destination, so `CF.EXISTS` returns wrong results.

**Fix**: add `file.sync_data()` at the end of each NIF write. One more
syscall on top of the ~14 pread/pwrite the operation already does.
Relative cost: small (prob writes are already disk-heavy; a single
fdatasync is ~100-200µs out of an already-slow operation).

**Call sites to add sync_data()**:

| NIF | File |
|-----|------|
| `bloom_file_add` | `bloom.rs:166` |
| `bloom_file_madd` | `bloom.rs:213` |
| `cuckoo_file_add` | `cuckoo.rs:305` |
| `cuckoo_file_addnx` | `cuckoo.rs:468` |
| `cuckoo_file_del` | `cuckoo.rs` |
| `cms_file_incrby` | `cms.rs:199` |
| `cms_file_merge` | `cms.rs` |
| `topk_file_add_v2` | `topk.rs` |
| `topk_file_incrby_v2` | `topk.rs` |

**Scope boundary**: this design is about the shared Bitcask log
checkpointer. The prob-file fsync fix is a different patch with different
review criteria, but **should ship before the checkpointer** because
it's a correctness bug that exists today — the checkpointer change does
not make it worse but it's a prerequisite to confidently claim "our
durability story is sound".

### ⚠ Dedicated (promoted) Bitcask files: performance opportunity

Today every HSET / SADD / ZADD on a promoted collection calls
`v2_append_record` which ends with `writer.sync()` — **one fsync per
write**. This is identical to the pre-checkpoint state machine behavior.

After this design lands for shared files, dedicated files will be
comparatively slow:

- Hot shared-file write: ~10µs (page cache)
- Hot dedicated-file write: ~100–200µs (pread/write/fsync on a file with
  no BufWriter amortization)

**Proposal: extend the checkpointer to cover dedicated files too.**

Required changes:

1. Switch dedicated writes from `v2_append_record` / `v2_append_tombstone`
   (which sync) to `_nosync` variants.
   - Need new NIFs `v2_append_record_nosync` and `v2_append_tombstone_nosync`.
2. The checkpointer needs to know about dedicated files. Two options:
   - **Enumerate**: each tick, walk `dedicated/shard_N/*` and fsync each
     active file. Simple; cost is `O(promoted_collections_per_shard)` per
     tick. Fine at 60s interval.
   - **Register**: `Promotion.open_dedicated` publishes into a `DedicatedActive`
     registry analogous to `ActiveFile`. Checkpointer reads registry and
     fsyncs each. O(1) per tick.
3. Rotation of dedicated files (when they exceed threshold) needs the same
   "synchronous fsync of old before publish new" handoff.

This is a big follow-up because the dedicated path has its own rotation,
compaction, and promotion logic — all will need handoff fsyncs added.
**Track as a separate work item.** The initial checkpointer ships with
shared-file scope only.

### ⚠ Hint files: data-loss if rotation crashes

Hint files (`NNNNN.hint`) speed up recovery by giving the shard a pre-built
keydir index without scanning the full `.log`. Today:

- Written via `v2_write_hint_file` inside `maybe_rotate_file`.
- **Never fsynced.** The data lands in page cache and sticks.
- On kernel panic mid-rotation: hint file may be partial or absent.
- Recovery effect: shard falls back to full `v2_scan_file` on startup —
  slower, but correct. **No data loss**, just slower recovery.

**Fix**: add `writer.sync()` inside the NIF's `commit()` method (or add
a separate `v2_fsync(hint_path)` call after `v2_write_hint_file` returns).
Small cost (~1 fsync per 256 MB of writes), correctness/speed win.

**Track in the rotation-fsync work item** — same code site, same fix
shape.

### Directory Durability (file + dir fsync)

`v2_fsync` today calls `fdatasync(file)` — it syncs the file's *data bytes*
only, not the directory entry. POSIX guarantees:

| Operation | File fdatasync sufficient? | Directory fsync also needed? |
|-----------|---------------------------|------------------------------|
| Append to existing file | ✅ yes | no |
| Create a new file | ❌ no | ✅ yes (filename durability) |
| Rename a file | ❌ no | ✅ yes |
| Delete a file | ❌ no | ✅ yes |

A missing dir-fsync can leave the directory in a state where:
- A newly-created file's data is on disk, but the filename entry is lost
  → file effectively doesn't exist on reboot
- A renamed file points at the old inode, not the new one → "un-merged"
  compactions
- A deleted file reappears after reboot → orphan disk usage, collision
  risk on recreate

This design introduces a **`NIF.v2_fsync_dir(path)`** helper (implemented
and tested — see `bitcask/src/lib.rs::fsync_dir`) that must be called at
every Bitcask site that mutates the directory namespace. The sites are
enumerated below per-concern.

### Shared-log rotation

**Location**: `shard/flush.ex:maybe_rotate_file/1`

Current sequence:
```elixir
write_hint_for_file(state, state.active_file_id)   # writes .hint, not fsynced
File.touch!(new_path)                              # creates N+1 file
Ferricstore.Store.ActiveFile.publish(state.index, new_id, new_path, sp)
```

Required changes:

1. **Sync the outgoing active file** before flipping `active_file_id`:
   `NIF.v2_fsync(old_active_path)`. Otherwise the last bytes written
   to it could never be fsynced by the checkpointer (whose next tick
   will target the NEW active file).
2. **Fsync the hint file data** after `write_hint_for_file`. See the
   Hint-file section below.
3. **Fsync the shard directory** after `File.touch!(new_path)`:
   `NIF.v2_fsync_dir(state.shard_data_path)`. Without this, the new
   file may not exist after a kernel panic and the next write from
   the state machine would fail on reopen.

After publish, the checkpointer takes over — it reads `ActiveFile.get(idx)`
per tick so rotations between ticks are naturally handled.

### Compaction

**Location**: `shard.ex:handle_call({:run_compaction, file_ids}, ...)`

Compaction merges fragmented read-only files into a single compacted
file. Sequence per file_id:

1. Read live offsets from `NNNNN.log` → write them to `compact_<fid>.log`
   via `NIF.v2_copy_records`, which internally does `writer.sync()` ✅
2. `File.rename!(dest, source)` (atomic replace).
3. For files with zero live data: `File.rm(source)`.
4. Error cleanup: `File.rm(dest)` if copy failed.

Gaps (all dir-fsync class — file data is already fsynced via
`v2_copy_records`):

- After step 2 (`rename!`): **shard dir entry not fsynced**. On
  kernel panic, rename may not be durable. Recovery sees both old
  `NNNNN.log` (unmerged) AND `compact_<fid>.log` (merged).
  `list_log_files` auto-cleans `compact_*.log` on recovery, so we
  end up with the unmerged file — no data loss, but merge work
  wasted.
- After step 3 (`rm`): **shard dir entry not fsynced**. Deleted
  fully-dead file may reappear on reboot. Recovery would re-scan and
  waste cycles; next compaction cycle cleans up. No data loss.
- After step 4 (cleanup): same class, idempotent.

Required changes: one `NIF.v2_fsync_dir(sp)` call **after the reduce
loop finishes** covers all the renames/rms from a single compaction
pass. Single dir-fsync amortizes across every file_id processed.

### Hint files

**Location**: `shard/flush.ex:write_hint_for_file/2` →
`NIF.v2_write_hint_file(hint_path, entries)`

Hint files speed up recovery by providing a pre-built keydir index so
the shard doesn't need to scan every `.log` file from byte 0 on
startup.

Current behavior: hint file data is written via `HintWriter` but
**the NIF does not call `sync_data()` after `writer.commit()`** — the
data sits in the page cache. On kernel panic, the hint file may be
empty, partial, or absent.

Impact: if the hint is missing/corrupt on recovery, we fall back to
full log scan (slow, but correct). No data loss, just degraded
recovery time.

Required changes:

1. Add `file.sync_data()` inside the hint NIF's commit path.
2. After `v2_write_hint_file` returns in Elixir, call
   `NIF.v2_fsync_dir(state.shard_data_path)` to make the new
   `.hint` file entry durable.

### Probabilistic structures (bloom / cuckoo / cms / topk)

✅ **Already addressed**. See the "Probabilistic structures: fsync per
write required" section above. `prob_fsync()` helper in `lib.rs` is
called at the end of every write NIF. Covered by `prob_durability_test.exs`
and `prob_fsync_tests` Rust module.

**Remaining prob gaps** (follow-up):
- `*_file_create` calls `sync_all` but the dir-entry for the new prob
  file isn't fsynced. Kernel panic right after `BF.RESERVE` can make
  the file vanish; next `BF.ADD` would see ENOENT. **Fix**: add
  `NIF.v2_fsync_dir(prob_dir)` after `*_file_create`.
- `sync_all` at create could be `sync_data` for consistency with the
  per-write path (tiny perf win, optional).

### Dedicated (promoted) Bitcask files

**Locations**: `promotion.ex`, `shard/compound.ex`

Each promoted collection (hash / set / zset with > 100 entries) gets
its own directory `data/dedicated/shard_N/{type}:{sha256}/` with its
own log files. All writes to dedicated logs use `v2_append_record`
/ `v2_append_tombstone` / `v2_append_batch` which **call
`writer.sync()` per write** ✅ — so per-call file-data durability is
covered today.

BUT the dedicated directory and file-namespace operations are riddled
with gaps:

#### D1. `Promotion.open_dedicated/4` — dedicated dir + initial file creation

```elixir
File.mkdir_p!(path)                    # creates dedicated/shard_N/{type}:{hash}/
active_file = Path.join(path, "00000.log")
unless File.exists?(active_file) do
  File.touch!(active_file)             # creates 00000.log
end
```

Gaps:
- `mkdir_p!` — parent `dedicated/shard_N/` dir not fsynced (low
  severity, `mkdir_p` is idempotent).
- `touch!(active_file)` — the dedicated directory itself is not
  fsynced. **On kernel panic the new empty `00000.log` may disappear.**
  The next `v2_append_batch` to it fails on reopen (ENOENT).

Required changes (in order):
```elixir
File.mkdir_p!(path)
NIF.v2_fsync_dir(Path.dirname(path))    # parent entry for new subdir
File.touch!(active_file)
NIF.v2_fsync_dir(path)                  # dedicated dir entry for 00000.log
```

#### D2. `Promotion.promote_collection!/6` — 🚨 non-atomic, runs outside Raft

The core promotion sequence writes in three logical steps that MUST
remain consistent:

```elixir
# Step 1: write dedicated data (entries batched)
NIF.v2_append_batch(dedicated_active, batch)

# Step 2: tombstone every compound key in the SHARED log
Enum.each(entries, fn {key, _, _} -> NIF.v2_append_tombstone(active_path, key) end)

# Step 3: write the marker in the SHARED log
NIF.v2_append_record(active_path, mk, type_str, 0)
```

Each step is individually fsynced (every NIF call above calls
`writer.sync`), but **the three steps are not atomic with respect
to each other**. Failure modes on kernel panic:

| Crash point | Shared log state | Dedicated dir state | Recovery outcome |
|-------------|------------------|----------------------|-------------------|
| After 1, before 2 | Original compound keys still there | Dedicated data present | Data duplicated in both stores. `recover_promoted` doesn't process it (no marker), so dedicated bytes are orphan. No data loss. |
| After some of 2, before the rest | Some keys tombstoned, others live | Dedicated has everything | Partial keys appear deleted; rest still in shared. **Partial view — ugly but no permanent loss.** |
| After all of 2, before 3 | Everything tombstoned, no marker | Dedicated has everything | **Data appears DELETED** even though it lives in dedicated (marker missing). **True data loss from client's perspective.** |
| After 3 | Tombstones + marker | Dedicated has everything | Normal state ✅ |

Additionally, **promotion runs outside the Raft state machine**
(triggered from Shard GenServer in `shard/compound.ex:634`). Raft
replay cannot redo a promotion — recovery reads whatever is
physically on disk.

Required changes — pick ONE:

**Option A (simple reorder, safer)**: marker-first sequence.
```elixir
# Step 1: write marker first (so recovery knows we intended to promote)
NIF.v2_append_record(active_path, mk, type_str, 0)
# Step 2: write dedicated data
NIF.v2_append_batch(dedicated_active, batch)
# Step 3: tombstone shared entries
Enum.each(entries, fn {key, _, _} -> NIF.v2_append_tombstone(active_path, key) end)
```
With this order, recovery rule is: if marker exists AND dedicated
data is present, trust dedicated; if marker exists AND dedicated is
empty, re-run promotion from surviving compound keys in shared log;
if marker doesn't exist, treat compound keys as authoritative.

**Option B (true atomicity)**: route `{:promote, type, redis_key,
entries}` through the Raft state machine as a single command. Replay
becomes deterministic and replicas converge.

Option A is lower-risk and ships with the checkpointer. Option B is
the longer-term architectural fix.

#### D3. Dedicated file rotation

**Location**: `shard/compound.ex` around `File.touch!(new_file)`

Dedicated files also rotate when they hit `max_active_file_size`.
Same two gaps as shared rotation:
- Outgoing active file not synchronously fsynced before switching.
- Dedicated dir not fsynced after new file creation.

If we extend the checkpointer to cover dedicated active files
(separate work item), these gaps must be closed at the same time.

#### D4. `Promotion.cleanup_promoted!/5` — destructor

```elixir
NIF.v2_append_tombstone(active_path, mk)  # marker tombstone (fsynced in NIF)
File.rm_rf!(path)                         # delete dedicated dir + files
```

Gap: `File.rm_rf!` doesn't fsync the parent dir. On kernel panic the
dedicated dir can reappear. Recovery's `recover_promoted` won't
process it (marker is tombstoned), but:
- Disk space leaks
- If subsequent promotion hashes to the same `{type}:{hash}` path,
  the orphan collides with the fresh files.

Required: `NIF.v2_fsync_dir(Path.join([data_dir, "dedicated", "shard_N"]))`
after `rm_rf!`.

#### D5. `list_log_files` cleanup during recovery

**Location**: `promotion.ex:list_log_files/1`

Recovery-time code deletes stale `compact_*.log` temp files left
over from crashed compaction. Same dir-fsync gap — but idempotent
(next recovery would re-rm the same files). Low severity.

Fix: `NIF.v2_fsync_dir(dir)` after the cleanup loop inside
`list_log_files`.

### Shard init

**Location**: `shard.ex` around `File.mkdir_p!(path)` +
`File.touch!(active_file_path)`

First-time shard startup creates the shard data dir + `00000.log`.
Same dir-fsync-on-create gap. Rare (only first boot) but worth
closing:
```elixir
File.mkdir_p!(path)
NIF.v2_fsync_dir(Path.dirname(path))
File.touch!(active_file_path)
NIF.v2_fsync_dir(path)
```

### Manifest / Config

**Locations**: `merge/manifest.ex:74-80`, `config.ex:400-405`

Both use the `tmp_path` + `File.rename` pattern for atomic
persistence. The rename is atomic on POSIX for single-file semantics,
but the dir-entry change isn't fsynced. A crash mid-rename can leave
either the old or the new filename in the dir — atomicity is
preserved (only one of them exists), but durability of the rename
itself isn't.

Mitigation (low priority since self-healing on replay):
```elixir
File.write(tmp_path, content)
NIF.v2_fsync(tmp_path)              # data durable
File.rename(tmp_path, final_path)
NIF.v2_fsync_dir(Path.dirname(final_path))
```

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

## Appendix A: At-a-glance gap summary

All gaps are now described in-body in their respective sections. Use this
table as a cross-reference: one row per file-system operation, linked to
the section that covers it.

| # | Site | File op | File-data fsync today | Dir fsync today | Section |
|---|------|---------|----------------------|-----------------|---------|
| 1 | `state_machine.flush_pending_writes` | append-nosync | per-apply `quorum_fsync` ✅ (being replaced by checkpointer) | N/A (existing file) | [Checkpointer](#the-fsync_needed-flag) |
| 2 | `Shard.Flush.flush_pending_nosync` | append-nosync | deferred `v2_fsync_async` via fsync_needed | N/A | [Checkpointer](#the-fsync_needed-flag) |
| 3 | `Shard.Flush.flush_pending_sync` | append-sync | per-call ✅ | N/A | [Checkpointer](#the-fsync_needed-flag) |
| 4 | `maybe_rotate_file` (shared) — old active | flip | ❌ | — | [Shared-log rotation](#shared-log-rotation) |
| 5 | `maybe_rotate_file` (shared) — `touch!(new_path)` | create | N/A (empty) | ❌ | [Shared-log rotation](#shared-log-rotation) |
| 6 | `write_hint_for_file` | create `.hint` | ❌ | ❌ | [Hint files](#hint-files) |
| 7 | `v2_copy_records` compaction dest | create + write | `writer.sync()` ✅ | — | [Compaction](#compaction) |
| 8 | Compaction `File.rename!(dest, source)` | rename | N/A | ❌ | [Compaction](#compaction) |
| 9 | Compaction `File.rm(source)` — all-dead file | remove | N/A | ❌ | [Compaction](#compaction) |
| 10 | Compaction `File.rm(dest)` — error cleanup | remove | N/A | ❌ | [Compaction](#compaction) |
| 11 | `Promotion.open_dedicated` — `mkdir_p!` | create dir | N/A | ❌ | [D1](#d1-promotionopen_dedicated4--dedicated-dir--initial-file-creation) |
| 12 | `Promotion.open_dedicated` — `touch!(00000.log)` | create | N/A (empty) | ❌ | [D1](#d1-promotionopen_dedicated4--dedicated-dir--initial-file-creation) |
| 13 | `Promotion.promote_collection!` | multi-step | per-NIF-call ✅ each | ❌ | [D2](#d2-promotionpromote_collection6---non-atomic-runs-outside-raft) **🚨 atomicity** |
| 14 | Dedicated file rotation (`shard/compound.ex`) | flip + create | partial (per-write sync on writes) | ❌ | [D3](#d3-dedicated-file-rotation) |
| 15 | `Promotion.cleanup_promoted!` — `rm_rf!` | remove dir | N/A | ❌ | [D4](#d4-promotioncleanup_promoted5--destructor) |
| 16 | `list_log_files` stale-compact cleanup | remove | N/A | ❌ | [D5](#d5-list_log_files-cleanup-during-recovery) |
| 17 | bloom/cuckoo/cms/topk `*_file_add` etc. | pread/pwrite | `prob_fsync` ✅ (just landed) | — | [Prob](#probabilistic-structures-bloom--cuckoo--cms--topk) |
| 18 | bloom/cuckoo/cms/topk `*_file_create` | create | `sync_all` ✅ | ❌ | [Prob](#probabilistic-structures-bloom--cuckoo--cms--topk) |
| 19 | `state_machine.ex` prob `File.rm(path)` delete | remove | N/A | ❌ | [Prob](#probabilistic-structures-bloom--cuckoo--cms--topk) |
| 20 | Shard init — `mkdir_p!` + `touch!(active_file)` | create | N/A | ❌ | [Shard init](#shard-init) |
| 21 | `Merge.Manifest.write_manifest` tmp + rename | rename | ❌ | ❌ | [Manifest/Config](#manifest--config) |
| 22 | `Config.save_config` tmp + rename | rename | ❌ | ❌ | [Manifest/Config](#manifest--config) |
| 23 | Ra WAL fdatasync | — | per-batch ✅ | N/A | Out of scope (ra patch) |

Columns:
- **File-data fsync**: is the file content flushed to disk before the
  function returns?
- **Dir fsync**: is the enclosing directory's namespace entry (filename
  → inode mapping) flushed to disk before the function returns?

Blank (`—`) = not applicable (e.g., rename doesn't create a file, so no
file-data fsync needed).

---

## Appendix B: Priority-ranked follow-up work

Ranked by "damage if it fires" × "probability":

| # | Gap | Impact | Effort |
|---|-----|--------|--------|
| 1 | **Promotion atomicity (A6)** — marker must be written before data move, or whole thing under a Raft command | **Real data loss** under crash mid-promotion | Medium — reorder writes OR move under Raft |
| 2 | **Rotation handoff fsync** (A3.1) | Last writes before rotation lost on kernel panic | Tiny — add one `NIF.v2_fsync` in `maybe_rotate_file` |
| 3 | **Promotion `open_dedicated` dir-fsync** (A6) | Fresh dedicated file can vanish on kernel panic → data loss on next append | Tiny — one `NIF.v2_fsync_dir` after `touch!` |
| 4 | **Checkpointer for shared active log** (A1) | Performance: remove per-apply fsync | Medium — GenServer + atomic flag plumbing |
| 5 | **Hint-file fsync + dir fsync** (A4) | Recovery slowness (fallback to scan) | Small — one `writer.sync()` in NIF + `v2_fsync_dir` |
| 6 | **`v2_fsync_dir(path)` NIF call sites** ✅ DONE NIF; wire up everywhere (A3.2, A6, A7-create, A8) | Files may not exist after kernel panic; recovery patches via replay | Small — a few call sites |
| 7 | **Dedicated files → checkpointer** (A6) | Performance: per-write fsync on promoted collections | Medium — share `fsync_needed` / `ActiveFile` pattern per-dedicated-instance |
| 8 | **Compaction rename/rm dir fsync** (A5) | Merged files may "un-merge" on crash; next compaction cleans up | Small — extend `v2_fsync_dir` coverage |
| 9 | **Manifest/config dir fsync** (A9, A11) | Cross-shard state can be lost on crash; rare, self-healing on replay | Small |
| 10 | **`sync_data` instead of `sync_all` at prob-create** (A7) | Tiny perf win at prob creation | Trivial |

---

## Appendix C: What we are NOT trying to solve

- **Torn writes inside a single syscall**: Linux guarantees atomicity at the
  page level (`write()` of ≤ 4KB is torn-proof). Larger writes can be
  torn, but Bitcask record framing plus CRC detects and discards torn
  records on recovery.
- **Silent disk corruption** (bit rot after fsync): out of scope — needs
  per-record CRC + background scrubbing; Bitcask already has CRC.
- **Device-level cache**: `fdatasync` on NVMe flushes the disk write
  cache on modern hardware. Budget hardware with lying caches is out of
  scope.
- **Replication lag / quorum failures**: Ra handles; we only cover local
  durability on one node.
