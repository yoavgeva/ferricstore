# Option C Lazy Resharding: Feasibility Analysis

**Status**: Research -- March 2026
**Scope**: Codebase analysis against the proposed "Option C" lazy resharding design

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Routing Analysis](#2-core-routing-analysis)
3. [Dynamic Shard Creation](#3-dynamic-shard-creation)
4. [Read Fallback Path](#4-read-fallback-path)
5. [Key Migration](#5-key-migration)
6. [Disk Considerations](#6-disk-considerations)
7. [Raft Coordination](#7-raft-coordination)
8. [Single Node Case](#8-single-node-case)
9. [Blockers Requiring Code Changes](#9-blockers-requiring-code-changes)
10. [Estimated Complexity](#10-estimated-complexity)
11. [Recommended Implementation Order](#11-recommended-implementation-order)
12. [Risks and Edge Cases](#12-risks-and-edge-cases)

---

## 1. Executive Summary

Option C lazy resharding is **feasible but requires significant changes** to the
Router, ShardSupervisor, and Raft layers. The design is sound in principle --
nothing in the codebase makes it impossible -- but there are two hard blockers
and several medium-complexity changes required:

**Hard blockers**:
1. `@shard_count` is a **compile-time constant** in `Router`, `Health`, and
   `Cluster` modules. Must be converted to runtime lookups.
2. `file_id` is **per-shard, not globally unique**. Two shards can have the same
   `file_id` pointing to different physical files. The read fallback cannot
   simply move an ETS 7-tuple between shards without also rewriting the data to
   the new shard's directory or introducing a `shard_data_path` field.

**Medium-complexity changes**: Dynamic shard creation (Supervisor, Raft, Batcher,
MergeScheduler, AsyncApplyWorker, MemoryGuard, PrefixIndex, BloomRegistry,
HnswRegistry), dual-path reads in Router, background sweeper, SCAN generation
awareness, compound key migration.

---

## 2. Core Routing Analysis

### Can `Router.shard_for/1` support `{current_shard_count, previous_shard_count}` without breaking the hot path?

**YES, with changes.**

`shard_for/2` already accepts an explicit `shard_count` parameter:

```elixir
def shard_for(key, shard_count \\ @shard_count) do
  :erlang.phash2(key, shard_count)
end
```

The hot read path in `Router.get/1` calls `shard_for(key)` which uses the
compile-time default. This must change to a runtime lookup. The recommended
approach is `:persistent_term.get(:ferricstore_shard_count)` (sub-nanosecond
read, no process dictionary overhead). The existing resharding design doc
already proposes this pattern.

The fallback logic would be:

```elixir
def get(key) do
  {current, previous} = shard_counts()
  idx = shard_for(key, current)
  case ets_get(:"keydir_#{idx}", key, now) do
    :miss when previous != nil ->
      old_idx = shard_for(key, previous)
      if old_idx != idx, do: try_old_shard(old_idx, key)
    other -> handle_normal(other)
  end
end
```

The overhead of the fallback check on the hot path is one `persistent_term` read
plus one conditional branch (both effectively free when `previous == nil` during
normal operation).

### Is `shard_count` used as a compile-time constant anywhere?

**YES -- this is a blocker.**

Three modules use `Application.compile_env`:

| Module | Line | Usage |
|--------|------|-------|
| `Router` | L19 | `@shard_count Application.compile_env(:ferricstore, :shard_count, 4)` |
| `Health` | L37 | `@shard_count Application.compile_env(:ferricstore, :shard_count, 4)` |
| `Cluster` (commands) | L17 | `@shard_count Application.compile_env(:ferricstore, :shard_count, 4)` |

`Router` uses `@shard_count` in:
- `shard_for/2` default parameter (L43)
- `keys/0` iteration range (L359)
- `keys_with_prefix/1` iteration range (L373)
- `dbsize/0` iteration range (L381)

All other modules read `shard_count` at runtime via
`Application.get_env(:ferricstore, :shard_count, 4)` -- these are fine and would
just need to read from the new runtime source instead.

### Are there places that assume shard count is fixed?

**YES -- many.**

Beyond the compile-time constants above, these locations iterate
`0..(shard_count - 1)` and would need updating:

- `ShardSupervisor.init/1` -- creates exactly N children at init
- `Application.start/2` -- creates N batchers, N async workers
- `Merge.Supervisor.init/1` -- creates N merge schedulers
- `MemoryGuard` -- iterates N shards for memory stats
- `NamespaceConfig` -- broadcasts to N batchers
- `Sandbox.reset/0` -- clears N keydirs
- `Commands.Server` -- iterates N shards for INFO, KEYS, FLUSHDB
- `Metrics` -- iterates N shards
- `connection.ex build_raw_store` flush lambda -- drains N async workers

All of these read `shard_count` at runtime already, so they would naturally
pick up a new count. The issue is that they only iterate `0..N-1` and don't
account for a generation where old shards (0..old_N-1) and new shards
(old_N..new_N-1) coexist.

---

## 3. Dynamic Shard Creation

### Can we `DynamicSupervisor.start_child` or equivalent to add new Shard GenServers at runtime?

**NO -- `ShardSupervisor` is a static `Supervisor`, not a `DynamicSupervisor`.**

`ShardSupervisor` uses `use Supervisor` with `strategy: :one_for_one` and a
fixed child list built at init time. It does not expose any API for adding
children after startup.

**Required change**: Either convert to `DynamicSupervisor` or add a function
that calls `Supervisor.start_child(ShardSupervisor, child_spec)`. The latter is
simpler and maintains the `:one_for_one` restart semantics.

Estimated complexity: **Low** (change Supervisor to allow dynamic children).

### Can we create new ETS tables (`keydir_8`, `keydir_9`, etc.) at runtime?

**YES.** ETS tables are created inside `Shard.init/1`:

```elixir
:ets.new(:"keydir_#{index}", [:set, :public, :named_table, ...])
```

There is no constraint on creating `keydir_8` at runtime. The named table
approach uses atoms, which are never garbage collected, but the number of new
shards is bounded (doubling from 8 to 16 adds 8 atoms -- trivial).

Similarly, `PrefixIndex.create_table/1` creates `prefix_keys_N` and
`BloomRegistry.create_table/1` creates its table -- both work for any index.

### Can we create new Raft groups at runtime via `ra:start_server`?

**YES.** `Raft.Cluster.start_shard_server/5` is already called from
`Shard.init/1` and handles all the setup:

```elixir
Ferricstore.Raft.Cluster.start_shard_server(index, path, active_file_id, active_file_path, ets)
```

The function creates a new ra server with a unique cluster name, triggers
election, and waits for a leader. It handles `already_started` and error
recovery. This works for any shard index at any time, as long as the ra system
is already started (which it is -- done once in `Application.start`).

### Can we create new Batcher GenServers at runtime?

**YES.** `Shard.init/1` already handles this case:

```elixir
if Process.whereis(batcher_name) == nil do
  Ferricstore.Raft.Batcher.start_link(shard_index: index, shard_id: shard_id)
end
```

This is the fallback for test-created shards. For production, we would want to
supervise these under the main supervision tree, but the mechanism works.

### Can we create new data directories (`data/shard_8/`, etc.) at runtime?

**YES.** `DataDir.ensure_layout!/2` is idempotent and creates directories for
a given shard count. Calling it again with a higher count creates the new
directories. Additionally, `Shard.init/1` calls `File.mkdir_p!(path)` for its
own shard directory.

---

## 4. Read Fallback Path

### Can we add a fallback: if miss in `keydir_N` (new), check `keydir_M` (old)?

**YES, with caveats.**

The current read hot path in `Router.get/1`:

```elixir
def get(key) do
  idx = shard_for(key)
  keydir = :"keydir_#{idx}"
  case ets_get(keydir, key, now) do
    {:hit, value, _exp} -> value
    :miss -> GenServer.call(shard_name(idx), {:get, key})
    ...
  end
end
```

A fallback would be added after `:miss`:

```elixir
:miss ->
  case check_old_shard(key) do
    nil -> GenServer.call(shard_name(idx), {:get, key})
    value -> value  # or trigger lazy migration
  end
```

The `:miss` branch already makes a GenServer call (cold read fallback), so
adding an ETS lookup to an old shard's keydir is strictly cheaper. The cost of
the fallback when `previous_shard_count == nil` (no resharding in progress) is
a single `persistent_term` read.

### What about `Router.get_meta`, `Router.exists?`, `Router.get_file_ref`, `Router.get_keydir_file_ref`?

**All need the same fallback pattern.**

- `get_meta/1` (L147) -- same structure as `get/1`, needs fallback
- `exists?/1` (L276) -- delegates to GenServer, needs fallback
- `get_file_ref/1` (L75) -- reads ETS directly, needs fallback
- `get_keydir_file_ref/1` (L410) -- reads ETS directly, needs fallback

All four follow the same `shard_for(key) -> ETS lookup -> GenServer fallback`
pattern. The change is mechanical but must be applied to all of them.

### What about compound keys (Hash/Set/ZSet)?

**Compound keys use the parent key's shard -- same routing, same problem.**

In `connection.ex build_raw_store`:

```elixir
compound_get: fn redis_key, compound_key ->
  shard = Router.shard_name(Router.shard_for(redis_key))
  GenServer.call(shard, {:compound_get, redis_key, compound_key})
end
```

The compound key is stored in the **parent key's shard**. During resharding,
`HGET myhash field` would compute `shard_for("myhash", new_count)`. If `myhash`
was previously in shard 2 (old count) but now routes to shard 5 (new count), all
the `H:myhash\0field` compound entries are in `keydir_2`, not `keydir_5`.

The fallback logic must also apply to compound operations. Since all compound
operations go through GenServer calls (not the ETS hot path), the fallback can
be added at the GenServer dispatch level.

**Complication**: When lazily migrating a hash key, ALL its compound entries
(`H:myhash\0field1`, `H:myhash\0field2`, ...) must be migrated together. This
is non-trivial -- a hash with 1000 fields requires 1000+ ETS moves and disk
writes.

### What about SCAN?

**SCAN iterates all shards and needs generation awareness.**

`Router.keys/0` iterates `0..(@shard_count - 1)`. During resharding, this would
need to iterate both old and new shards, deduplicating keys that exist in both.
`Router.keys_with_prefix/1` has the same issue.

The `do_scan` implementation in `Commands.Generic` calls `store.keys.()` which
calls `Router.keys/0`. The deduplication concern is real: during lazy migration,
a key exists in both old and new shards temporarily.

**Required change**: During resharding, `keys/0` must union old and new shard
ranges and deduplicate. This can be done with a `MapSet` but adds O(N) memory
overhead proportional to total key count.

### What about prefix index?

**Per-shard prefix index needs migration alongside keys.**

`PrefixIndex` tables are per-shard (`prefix_keys_N`). When a key migrates from
shard 2 to shard 5, its prefix index entry must also move. Since `PrefixIndex`
is rebuilt from the keydir in `Shard.init/1` via `rebuild_from_keydir/2`, the
new shard's prefix index will be populated naturally as keys are migrated.

However, during the transition period, `keys_with_prefix("session")` needs to
query prefix tables from both old and new shards.

---

## 5. Key Migration

### Atomic migration: read from old, write to new, delete from old

**NO -- this cannot be made truly atomic across two shards.**

The migration for a single key is:
1. Read value from old shard (ETS 7-tuple or Bitcask disk read)
2. Write to new shard (append to Bitcask + insert into ETS)
3. Delete from old shard (append tombstone + delete from ETS)

Steps 2 and 3 are on different shards with different Raft groups. There is no
cross-shard transaction mechanism that guarantees atomicity of steps 2+3.

### What if we crash between step 2 and 3?

**The key exists in both shards.** On recovery:
- A read of the key via `shard_for(key, new_count)` finds it in the new shard.
  Correct.
- The key also exists in the old shard. Wasted space, but not a correctness bug.
- The background sweeper will eventually re-encounter this key in the old shard,
  check if it exists in the new shard, and delete the old copy.

### Is it OK if a key temporarily exists in both shards?

**YES, with careful ordering.** The protocol must be:

1. Write to new shard (ensures key is available at new location)
2. Delete from old shard (safe because new location already has the data)

If we crash after step 1 but before step 2, we have a duplicate. The read path
always checks the new shard first, so it gets the correct (most recent) value.
The sweeper handles cleanup.

**Risk**: If the value is updated between step 1 and step 2 (a concurrent write
goes to the new shard via `shard_for(key, new_count)`), and then step 2 deletes
from the old shard, we are fine -- the new shard has the latest value.

**Risk**: If a concurrent write goes to the OLD shard (this should not happen
because new writes use `new_count`), the delete in step 2 would destroy that
write. This is prevented by the design: new writes always go to `new_count`.

---

## 6. Disk Considerations

### Is `file_id` globally unique or per-shard?

**PER-SHARD -- this is a hard blocker for the proposed ETS migration approach.**

Each shard maintains its own `active_file_id` starting at 0 and incrementing on
rotation:

```elixir
# shard.ex L2944
new_id = state.active_file_id + 1
```

File paths are constructed relative to the shard's data directory:

```elixir
# shard.ex L326
defp file_path(shard_path, file_id) do
  Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
end
```

So `keydir_0` might have `{key, val, exp, lfu, 3, 1024, 256}` meaning
`shard_0/data/shard_0/00003.log` at offset 1024. And `keydir_1` might have
another key with `file_id=3` meaning `shard_1/data/shard_1/00003.log` -- a
completely different file.

**Implication**: You cannot simply move an ETS 7-tuple from `keydir_2` to
`keydir_5` and expect reads to work. The `file_id=3` that was valid in shard 2's
context (pointing to `shard_2/00003.log`) is meaningless in shard 5's context
(where `file_id=3` might not exist or points to a different file).

The existing resharding design document (section 4) claims file_id is globally
unique: "The file_id must be globally unique across shards." **This is not the
case in the current implementation.**

### Options to resolve the file_id problem

**Option A: Rewrite values during migration** (recommended)

When migrating a key from old shard to new shard:
1. Read the full value from the old shard's Bitcask file
2. Append it to the new shard's active log file
3. Update the ETS entry with the new shard's `file_id` and `offset`
4. Delete from old shard

This is exactly what the lazy migration does anyway -- it's a full write to the
new shard. The ETS 7-tuple in the new shard will have the new shard's `file_id`
and `offset`.

**Cost**: Every migrated key requires a disk read + disk write. For cold keys
(value=nil in ETS), this is a disk read anyway. For hot keys (value in ETS), the
disk write is the only additional cost.

**Option B: Add shard_data_path to ETS tuple**

Change the 7-tuple to an 8-tuple: `{key, value, exp, lfu, shard_data_path, file_id, offset, vsize}`. This makes each entry self-describing about where its
data lives on disk. However, this is a massive change affecting every ETS
read/write in the codebase.

**Option B is not recommended** -- it changes the hot path tuple format
everywhere and adds ~50 bytes of path string per ETS entry.

### How does compaction work with keys spanning old/new shard directories?

**Compaction is per-shard and will naturally consolidate.**

After migration, the new shard's log files contain the migrated key's data. The
old shard's log files still have the original data, but after the key is deleted
from the old shard (tombstone written), compaction of the old shard will reclaim
that space.

The new shard's compaction operates on its own files only, which is correct.
There is no cross-shard file reference after migration (because Option A above
rewrites the data into the new shard's directory).

---

## 7. Raft Coordination

### Can we use a single Raft command `{:reindex, new_count, old_count, generation}`?

**YES, but it needs a cluster-level Raft group.**

Currently, each shard has its own Raft group. The `StateMachine` is per-shard
and knows nothing about other shards. A reindex command must be coordinated
across all shards simultaneously.

**Options**:

1. **Cluster-level Raft group** (recommended): A separate Raft group (not
   per-shard) that handles cluster-wide commands like `:reindex`. All nodes
   participate. When `{:reindex, new_count, old_count, generation}` is committed,
   each node applies it locally: creates new shards, updates routing metadata,
   starts the background sweeper.

2. **Broadcast via `persistent_term`**: For single-node, no Raft needed. Just
   update `persistent_term` values for `shard_count`, `previous_shard_count`,
   and `generation`. This is sufficient for the single-node case.

3. **Per-shard command fan-out**: Send a reconfigure command to each shard's Raft
   group. Problem: non-atomic -- shard 0 might process the reindex before shard 7,
   leading to an inconsistent state window. Not recommended.

### Does the state machine need to coordinate across shard-specific Raft groups?

**YES, for multi-node.** The current per-shard state machine cannot coordinate a
cluster-wide operation. A cluster-level Raft group is the cleanest solution.

For single-node mode, no cross-shard coordination is needed -- all shards are
local and the operation can be performed sequentially.

### Or do we need a cluster-level Raft group?

**YES, for multi-node.** The cluster-level group would handle:
- `{:reindex, new_count, old_count, generation}`
- `{:report_cpus, node, cpu_count}`
- `{:sweeper_complete, shard_index}` (to track migration progress)

For single-node, this group is a single-member group (self-quorum), functionally
equivalent to a local GenServer call.

---

## 8. Single Node Case

### Single node with 8 CPUs to 16 shards -- restart vs live resharding?

**Live resharding is feasible and preferred** for single node.

Since there is no cluster coordination needed:

1. Call `DataDir.ensure_layout!(data_dir, 16)` -- creates new directories
2. Start new Shard GenServers for indices 8-15 (with their Raft groups, Batchers,
   MergeSchedulers, AsyncApplyWorkers)
3. Update `persistent_term` with `{current: 16, previous: 8, generation: 2}`
4. New writes immediately route to the correct new shard
5. Reads check new shard first, fall back to old shard
6. Background sweeper migrates keys from old shards to new shards

No restart needed. No write freeze. Reads continue.

### Can single node do live resharding?

**YES** -- this is the simplest case since all shards are local. The lazy
migration can proceed without any network coordination. A restart with a new
`shard_count` config would also work but requires downtime.

---

## 9. Blockers Requiring Code Changes

### Hard Blockers

| # | Blocker | Location | Description |
|---|---------|----------|-------------|
| 1 | Compile-time `@shard_count` | `Router` L19, `Health` L37, `Cluster` L17 | Must convert to `persistent_term` runtime lookup |
| 2 | Per-shard `file_id` | `Shard.init`, `StateMachine.do_put` | Lazy migration must rewrite values to new shard's Bitcask (cannot just move ETS entries) |

### Required Changes

| # | Change | Files | Complexity |
|---|--------|-------|------------|
| 1 | Runtime shard count via `persistent_term` | `Router`, `Health`, `Cluster` | Low |
| 2 | `persistent_term` for `{current, previous, generation}` routing state | `Router`, `Application` | Low |
| 3 | Dual-path read logic in Router (`get`, `get_meta`, `exists?`, `get_file_ref`, `get_keydir_file_ref`) | `Router` | Medium |
| 4 | Dynamic shard creation function | `ShardSupervisor`, `Application` | Low |
| 5 | Start new Batcher + AsyncApplyWorker + MergeScheduler at runtime | `Application`, `Merge.Supervisor` | Medium |
| 6 | Lazy migration in read fallback (read from old, write to new, delete from old) | New module `Store.Migrator` or in `Router` | High |
| 7 | Background sweeper process | New module `Store.Sweeper` | High |
| 8 | SCAN/KEYS deduplication across generations | `Router.keys/0`, `Router.keys_with_prefix/1`, `Router.dbsize/0` | Medium |
| 9 | Compound key migration (hash/set/zset -- must migrate all fields together) | `Store.Migrator` | High |
| 10 | MemoryGuard awareness of new shards | `MemoryGuard` | Low |
| 11 | Cluster-level Raft group for multi-node reindex | New module `Raft.ClusterGroup` | High (multi-node only) |
| 12 | Update `build_raw_store` compound operations for dual-shard routing | `connection.ex` | Medium |
| 13 | Transaction coordinator shard classification during resharding | `Transaction.Coordinator` | Medium |

---

## 10. Estimated Complexity

### Effort Breakdown

| Phase | Description | Estimated Effort | Dependencies |
|-------|-------------|-----------------|--------------|
| **Phase 0** | Convert `@shard_count` to `persistent_term` | 1 day | None |
| **Phase 1** | Dynamic shard creation (Supervisor, Raft, Batcher, AsyncWorker, Merge) | 2-3 days | Phase 0 |
| **Phase 2** | Dual-path read logic in Router (all read functions) | 2 days | Phase 0 |
| **Phase 3** | Lazy migration on read (single key) | 3-4 days | Phase 1, 2 |
| **Phase 4** | Background sweeper | 2-3 days | Phase 1, 3 |
| **Phase 5** | Compound key migration | 3-4 days | Phase 3 |
| **Phase 6** | SCAN/KEYS/DBSIZE generation awareness | 1-2 days | Phase 2 |
| **Phase 7** | Transaction coordinator updates | 1 day | Phase 2 |
| **Phase 8** | Cluster-level Raft group (multi-node only) | 3-5 days | Phase 1 |
| **Phase 9** | Integration testing + edge cases | 3-5 days | All above |

**Total estimate**: 3-4 weeks for single-node, 5-6 weeks including multi-node.

---

## 11. Recommended Implementation Order

### Milestone 1: Single-Node Lazy Resharding (3 weeks)

1. **Phase 0**: Convert `@shard_count` to `persistent_term`
   - Replace `Application.compile_env` in Router, Health, Cluster commands
   - Initialize `persistent_term` in `Application.start/2`
   - Add `{:current_shard_count, N}` and `{:previous_shard_count, nil}`

2. **Phase 1**: Dynamic shard creation
   - Add `ShardSupervisor.start_shard(index, data_dir)` function
   - Ensure Batcher, AsyncApplyWorker, MergeScheduler can be started dynamically
   - Test: start with 4 shards, dynamically add shard 4-7

3. **Phase 2**: Dual-path reads
   - Add fallback logic to `Router.get/1`, `get_meta/1`, `exists?/1`,
     `get_file_ref/1`, `get_keydir_file_ref/1`
   - When `previous_shard_count != nil` and miss in new shard, check old shard
   - No migration on read yet -- just return the value from the old shard

4. **Phase 3**: Lazy migration on read
   - On fallback hit: read value from old shard, write to new shard's Bitcask,
     delete from old shard
   - Handle the file_id issue by doing a full write to new shard's active file
   - Add migration counter telemetry

5. **Phase 4**: Background sweeper
   - New GenServer that iterates old shards
   - For each key in old shard N, compute `shard_for(key, new_count)`
   - If different from N, migrate via same path as lazy migration
   - Throttled: configurable keys-per-tick, sleep between ticks
   - When all old shards are empty of misrouted keys, clear `previous_shard_count`

6. **Phase 5**: Compound key migration
   - When migrating a hash/set/zset parent key, also migrate all `H:`, `S:`,
     `Z:`, `T:`, `LM:` compound entries
   - Use `compound_scan` to find all compound entries for a parent key
   - Migrate as a batch

7. **Phase 6**: SCAN/KEYS generation awareness
   - `Router.keys/0` unions old and new shard ranges, deduplicates
   - `Router.dbsize/0` counts unique keys across both ranges
   - SCAN cursor must be stable across the dual-shard period

8. **Phase 7**: Transaction coordinator
   - `classify_shards` must use `new_count` for routing
   - WATCH/EXEC version checks must work across shard migration

### Milestone 2: Multi-Node (additional 2 weeks)

9. **Phase 8**: Cluster-level Raft group
   - New Raft group for cluster-wide coordination
   - `{:reindex, new_count, old_count, generation}` command
   - All nodes create new shards and start sweeping in parallel

10. **Phase 9**: Testing
    - Single-node: 4->8 shards with concurrent reads/writes
    - Multi-node: coordinated reindex across 3 nodes
    - Crash recovery during migration
    - Compound key migration correctness
    - SCAN correctness during migration

---

## 12. Risks and Edge Cases

### Risk 1: Compound key partial migration

**Severity: High**

A hash with 1000 fields requires migrating 1001 ETS entries (1 type key + 1000
field keys). If the process crashes mid-migration, some fields are in the new
shard and some in the old. The read fallback must handle this: for compound
operations like HGETALL, check both old and new shards and merge results.

**Mitigation**: Migrate all compound entries as a batch within a single
GenServer call to the old shard (read all) followed by a batch write to the new
shard.

### Risk 2: Memory spike during migration

**Severity: Medium**

During lazy migration on read, we hold both the old and new copies in memory
briefly. For large values, this could cause memory pressure. The background
sweeper also reads values into memory for rewriting.

**Mitigation**: The sweeper should respect MemoryGuard pressure levels and pause
when memory is above the warning threshold.

### Risk 3: Concurrent writes during migration

**Severity: Low**

New writes go to `shard_for(key, new_count)` -- always the new shard. The lazy
migration reads from the old shard and writes to the new shard. If a concurrent
write updates the key in the new shard between the migration read and write, the
migration write would overwrite it with stale data.

**Mitigation**: Use a CAS (compare-and-swap) for the migration write. If the
key already exists in the new shard (put there by a concurrent write), skip the
migration. Alternatively, check if the key exists in the new shard before
migrating.

### Risk 4: SCAN cursor stability

**Severity: Medium**

SCAN uses the last-seen key as a cursor. During migration, keys move between
shards. A SCAN iteration started before migration might miss keys that migrated
during iteration, or see keys twice.

**Mitigation**: SCAN already operates on a snapshot (calls `store.keys.()` which
returns all keys at that moment). The deduplication in `Router.keys/0` handles
the dual-shard case. Cursors based on key ordering remain stable because key
names don't change.

### Risk 5: Blocking commands (BLPOP, BRPOP)

**Severity: Medium**

Blocking list commands register waiters on a specific shard's list key. If the
list key migrates to a new shard, the waiter is orphaned.

**Mitigation**: The waiter registry (`Ferricstore.Waiters`) is global, not
per-shard. When a key migrates and a push happens on the new shard, the waiter
should still be notified. Needs verification.

### Risk 6: MULTI/EXEC transactions spanning migration

**Severity: Medium**

A MULTI/EXEC transaction queues commands with shard assignments computed at
queue time. If a reindex happens between MULTI and EXEC, the shard assignments
are stale.

**Mitigation**: The WATCH mechanism detects this. A WATCH on a key computes
`get_version(key)` which goes to the shard via `shard_for(key)`. If the shard
count changes between WATCH and EXEC, the version check will fail (the key is
now in a different shard), and the transaction is aborted. The client retries,
getting the correct shard assignment.

### Risk 7: Shard shrinking (16 -> 8 shards)

**Severity: High**

Option C as described only handles shard count increase. Shrinking requires
migrating keys FROM higher-indexed shards TO lower-indexed shards, then stopping
and destroying the excess shards. This is significantly more complex:
- Must drain all keys from shards 8-15 before stopping them
- Must stop Raft groups, Batchers, AsyncWorkers, MergeSchedulers
- Must clean up ETS tables
- Cannot serve reads from stopped shards

**Recommendation**: Defer shard shrinking to a future version. Only support
increasing shard count in v1.

### Risk 8: PubSub and keyspace notifications

**Severity: Low**

PubSub is global (`Ferricstore.PubSub`) and not shard-specific. Keyspace
notifications (`Ferricstore.KeyspaceNotifications`) are triggered by key
operations. When a key migrates (write to new + delete from old), it will
generate a SET notification on the new shard and a DEL notification on the old
shard. This is semantically correct but might confuse subscribers who see a
DEL for a key they didn't expect to be deleted.

**Mitigation**: Suppress keyspace notifications for migration operations (add a
`:migration` flag to the write/delete path).

### Risk 9: Hotness tracking during migration

**Severity: Low**

`Stats.record_hot_read`/`record_cold_read` tracks per-key access patterns. A
fallback read (miss in new shard, hit in old shard) should be recorded as a cold
read since it involves a cross-shard lookup.

**Mitigation**: Record fallback reads as cold reads in the stats module.
