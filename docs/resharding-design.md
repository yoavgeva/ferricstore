# FerricStore Resharding Design

## Status

**Draft** -- March 2026

## Overview

FerricStore routes keys using `phash2(key, shard_count)` where `shard_count`
equals the total number of CPUs in the cluster (or a configurable multiplier
thereof). When the cluster topology changes and all nodes converge to equal CPU
counts, the Raft leader triggers a **stop-the-world reindex** that rehashes
every key into the new shard layout.

There is no incremental migration, no MOVED/ASKING protocol, no dual-write
period, and no slot migration state machine. The entire resharding operation
is a single deterministic Raft command that every node executes at the same
logical point in the log.

```
                     Single Node                    Multi-Node (equal CPUs)
                     -----------                    ----------------------

 Shard count:        System.schedulers_online()     sum(all node CPUs)
                     * multiplier                   * multiplier

 Routing:            phash2(key, shard_count)        phash2(key, shard_count)

 Resharding:         Not needed                      Raft-coordinated reindex
                     (shard count = local CPUs)      (write freeze + rehash)

 Data distribution:  All local                       Each node owns its shards

 Downtime:           None                            Write freeze during reindex
                                                     (reads continue, <1s typical)
```

---

## Table of Contents

1. [Shard Count Computation](#1-shard-count-computation)
2. [Reindex Protocol](#2-reindex-protocol)
3. [Write Freeze Behavior](#3-write-freeze-behavior)
4. [Bitcask File Handling](#4-bitcask-file-handling)
5. [Recovery](#5-recovery)
6. [Trigger Mechanism](#6-trigger-mechanism)
7. [Node Discovery](#7-node-discovery)
8. [Error Scenarios](#8-error-scenarios)
9. [Performance Estimates](#9-performance-estimates)
10. [Configuration](#10-configuration)

---

## 1. Shard Count Computation

### Formula

```
shard_count = total_cluster_cpus * multiplier
```

Where:
- `total_cluster_cpus` = sum of `System.schedulers_online()` across all nodes
- `multiplier` = configurable (default 1, optionally 2 for more granularity)

### Examples

| Cluster Shape               | CPUs  | Multiplier | Shard Count |
|-----------------------------|-------|------------|-------------|
| 1 node, 8 cores             | 8     | 1          | 8           |
| 1 node, 8 cores             | 8     | 2          | 16          |
| 2 nodes, 8 cores each       | 16    | 1          | 16          |
| 3 nodes, 8 cores each       | 24    | 1          | 24          |
| 2 nodes, 8 + 16 cores       | --    | --         | NO REINDEX  |

### Shard-to-Node Assignment

Each node owns a contiguous range of shards proportional to its CPU count.
Since reindex only triggers when all nodes have equal CPUs, assignment is
trivially even:

```
shards_per_node = shard_count / num_nodes

Node 0: shards [0 .. shards_per_node - 1]
Node 1: shards [shards_per_node .. 2 * shards_per_node - 1]
...
```

Nodes are sorted by their Raft member ID for deterministic ordering.

### Single-Node Mode

A lone node runs with `shard_count = local_cpus * multiplier`. No rebalancing
is ever needed -- it owns all shards. This is the default startup behavior.

---

## 2. Reindex Protocol

### State Machine Diagram

```
                         ┌───────────────┐
                         │    NORMAL     │
                         │  (read/write) │
                         └──────┬────────┘
                                │
                  leader detects all CPUs equal
                  AND new total != current shard_count
                                │
                         ┌──────▼────────┐
                         │  PROPOSE      │
                         │  {:reindex,   │
                         │   new_count,  │
                         │   epoch + 1}  │
                         └──────┬────────┘
                                │
                    Raft commits command to log
                                │
                         ┌──────▼────────┐
                         │  FROZEN       │
                         │  (reads ok,   │
                         │   writes err) │
                         └──────┬────────┘
                                │
                    each node rehashes all keys
                                │
                         ┌──────▼────────┐
                         │  SWAP         │
                         │  (atomic ETS  │
                         │   table swap) │
                         └──────┬────────┘
                                │
                    update Router.shard_count
                                │
                         ┌──────▼────────┐
                         │    NORMAL     │
                         │  (read/write) │
                         └───────────────┘
```

### Sequence Diagram (3 nodes, 8 CPUs each)

```
  Leader (Node 0)          Node 1                Node 2          Raft Log
       │                     │                     │                │
       │ detect: all CPUs    │                     │                │
       │ equal (8,8,8)       │                     │                │
       │ new total = 24      │                     │                │
       │                     │                     │                │
       ├─ propose {:reindex, 24, epoch=2} ────────────────────────►│
       │                     │                     │                │
       │◄──────────── Raft commit at log index L ──────────────────│
       │                     │◄────────────────────────────────────│
       │                     │                     │◄──────────────│
       │                     │                     │                │
  ┌────┴────┐           ┌────┴────┐           ┌────┴────┐          │
  │ apply() │           │ apply() │           │ apply() │          │
  │         │           │         │           │         │          │
  │ 1. write_frozen=T   │ 1. write_frozen=T   │ 1. write_frozen=T │
  │ 2. create 24 new    │ 2. create 24 new    │ 2. create 24 new  │
  │    ETS tables       │    ETS tables       │    ETS tables      │
  │ 3. iterate all keys │ 3. iterate all keys │ 3. iterate all keys│
  │    phash2(k, 24)    │    phash2(k, 24)    │    phash2(k, 24)  │
  │    insert into new  │    insert into new  │    insert into new │
  │ 4. swap old -> new  │ 4. swap old -> new  │ 4. swap old -> new│
  │ 5. shard_count=24   │ 5. shard_count=24   │ 5. shard_count=24 │
  │ 6. write_frozen=F   │ 6. write_frozen=F   │ 6. write_frozen=F │
  │ 7. epoch=2          │ 7. epoch=2          │ 7. epoch=2        │
  └────┬────┘           └────┬────┘           └────┬────┘          │
       │                     │                     │                │
       │            cluster resumes normally       │                │
       │                     │                     │                │
```

### Step-by-Step (in `apply/3`)

Each node executes identically when the Raft state machine applies
`{:reindex, new_shard_count, new_epoch}`:

```elixir
def apply(_meta, {:reindex, new_shard_count, new_epoch}, state) do
  # Guard: skip if already at this epoch
  if new_epoch <= state.epoch do
    {state, :already_applied}
  else
    # 1. Freeze writes
    :persistent_term.put(:ferricstore_write_frozen, true)

    # 2. Create new ETS tables
    new_tables = for i <- 0..(new_shard_count - 1) do
      :ets.new(:"keydir_new_#{i}", [:set, :public, :named_table,
        read_concurrency: true, write_concurrency: true])
    end

    # 3. Iterate all keys from old tables, rehash into new tables
    for old_idx <- 0..(state.shard_count - 1) do
      :ets.foldl(fn {key, val, exp, lfu, fid, off, vsize}, _acc ->
        new_idx = :erlang.phash2(key, new_shard_count)
        :ets.insert(:"keydir_new_#{new_idx}", {key, val, exp, lfu, fid, off, vsize})
        nil
      end, nil, :"keydir_#{old_idx}")
    end

    # 4. Atomic swap: rename new tables, delete old ones
    for i <- 0..(state.shard_count - 1) do
      :ets.delete(:"keydir_#{i}")
    end
    for i <- 0..(new_shard_count - 1) do
      :ets.rename(:"keydir_new_#{i}", :"keydir_#{i}")
    end

    # 5. Update router shard count
    :persistent_term.put(:ferricstore_shard_count, new_shard_count)

    # 6. Unfreeze writes
    :persistent_term.put(:ferricstore_write_frozen, false)

    # 7. Update state
    new_state = %{state | shard_count: new_shard_count, epoch: new_epoch}
    {new_state, :ok}
  end
end
```

> **Note**: The ETS rename trick (`delete old` + `rename new`) ensures that
> reads against the old table names never see a partially-migrated state. There
> is a brief window where a read to an old table name will get `ArgumentError`
> (table deleted, new not yet renamed). The Router already handles this via
> its `:no_table` fallback path.

---

## 3. Write Freeze Behavior

### During Reindex

Writes check the frozen flag before proceeding:

```elixir
def put(key, value, expire_at_ms \\ 0) do
  if :persistent_term.get(:ferricstore_write_frozen, false) do
    {:error, "READONLY FerricStore is reindexing, writes temporarily disabled"}
  else
    # normal write path
  end
end
```

### Behavior Matrix

| Operation | During Reindex | After Reindex |
|-----------|---------------|---------------|
| GET       | Serves from current tables | Serves from new tables |
| SET       | Returns READONLY error | Normal |
| DEL       | Returns READONLY error | Normal |
| INCR      | Returns READONLY error | Normal |
| KEYS      | Returns current snapshot | Returns new snapshot |
| DBSIZE    | Returns current count | Returns new count |
| SUBSCRIBE | Continues | Continues |
| PING      | PONG | PONG |

### Why Not Queue Writes?

Queuing adds complexity (unbounded buffer, ordering, backpressure) for a
freeze window that should be well under 1 second. Returning an error is
simpler and lets clients retry. Redis uses the same READONLY error for
replicas, so clients already handle it.

---

## 4. Bitcask File Handling

### The Key Insight

Each keydir ETS entry stores the physical location of the value:
`{key, value|nil, expire, lfu, file_id, offset, value_size}`.

The `file_id` + `offset` point to an absolute position in a Bitcask data file.
**These pointers remain valid regardless of which shard's ETS table the entry
lives in.** Moving an entry from `keydir_2` to `keydir_7` does not invalidate
the disk pointer.

### Approach: No File Movement Required

```
Before reindex (4 shards):

  keydir_0: {"user:1" -> file_id=3, offset=0}      shard_0/data/3.bitcask
  keydir_1: {"user:2" -> file_id=1, offset=512}     shard_1/data/1.bitcask
  keydir_2: {"user:3" -> file_id=2, offset=0}       shard_2/data/2.bitcask
  keydir_3: {"user:4" -> file_id=1, offset=1024}    shard_3/data/1.bitcask

After reindex (8 shards):

  keydir_0: (empty)
  keydir_1: {"user:4" -> file_id=1, offset=1024}    STILL shard_3/data/1.bitcask
  keydir_2: {"user:1" -> file_id=3, offset=0}       STILL shard_0/data/3.bitcask
  keydir_3: (empty)
  keydir_4: {"user:2" -> file_id=1, offset=512}     STILL shard_1/data/1.bitcask
  keydir_5: (empty)
  keydir_6: {"user:3" -> file_id=2, offset=0}       STILL shard_2/data/2.bitcask
  keydir_7: (empty)
```

The file_id must be globally unique across shards (not per-shard). This is
already the case if file IDs are assigned from a monotonic counter (e.g.,
`System.unique_integer([:monotonic, :positive])`).

### Eventual Compaction

After reindex, old shard directories contain data files referenced by keys now
in different shards. This is functionally correct but operationally messy.
Background compaction naturally resolves this: when a shard compacts, it
rewrites live entries into its own data directory. Dead entries in old shard
directories are reclaimed.

```
Reindex completes:
  shard_7/keydir has key "user:3" pointing to shard_2/data/2.bitcask

Compaction of shard_7 later:
  shard_7 reads "user:3" value from shard_2/data/2.bitcask
  shard_7 writes "user:3" value to shard_7/data/5.bitcask
  shard_7 updates keydir: file_id=5, offset=0
  shard_2/data/2.bitcask can eventually be deleted (once no keys reference it)
```

---

## 5. Recovery

### Crash Mid-Reindex

If a node crashes during the reindex `apply()`:

1. On restart, the node replays the Raft log from its last snapshot
2. It re-encounters the `{:reindex, new_shard_count, epoch}` command
3. It re-executes the reindex deterministically
4. Same keys + same hash function + same shard count = identical result

### Epoch Guard

The `epoch` field prevents double-reindex:

```elixir
if new_epoch <= state.epoch do
  {state, :already_applied}
end
```

If a node already completed a reindex to epoch 5 (persisted in Raft snapshot),
replaying a stale `{:reindex, _, 5}` command is a no-op.

### Partial ETS State

If the crash happens after some `keydir_new_*` tables were created but before
the swap completed:

- On restart, all ETS tables are gone (ETS is in-memory only)
- The Raft snapshot contains the pre-reindex state
- Raft replay rebuilds keydir from Bitcask hint files, then re-applies the
  reindex command
- Result: clean, consistent state

---

## 6. Trigger Mechanism

### Automatic Trigger

The Raft leader runs a periodic check (every 30 seconds):

```
                    ┌────────────────────────┐
                    │  Leader periodic check  │
                    │  (every 30s)            │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                   ╱ all nodes reporting?     ╲──── No ──── wait
                   ╲                          ╱
                    └───────────┬────────────┘
                               Yes
                    ┌───────────▼────────────┐
                   ╱ all CPUs equal?          ╲──── No ──── wait
                   ╲                          ╱
                    └───────────┬────────────┘
                               Yes
                    ┌───────────▼────────────┐
                   ╱ new total != current     ╲──── No ──── nothing to do
                   ╲ shard_count?             ╱
                    └───────────┬────────────┘
                               Yes
                    ┌───────────▼────────────┐
                   ╱ cooldown elapsed?        ╲──── No ──── wait
                   ╲ (>5 min since last)      ╱
                    └───────────┬────────────┘
                               Yes
                    ┌───────────▼────────────┐
                    │  Propose {:reindex,     │
                    │   new_count, epoch+1}   │
                    └────────────────────────┘
```

### Manual Trigger

```
CLUSTER REBALANCE
```

Forces an immediate reindex regardless of CPU equality. Useful for:
- Operator-initiated rebalancing after hardware changes
- Testing
- Recovering from a bad state

### Cooldown

Minimum 5 minutes between reindex operations (configurable). Prevents
thrashing if nodes are joining/leaving rapidly.

---

## 7. Node Discovery

### Discovery Methods (in priority order)

1. **Static config** -- `config :ferricstore, :cluster_nodes, [:"ferric@10.0.1.1", ...]`
2. **DNS** -- `config :ferricstore, :cluster_dns, "ferric.internal"`
   (SRV or A records, polled every 15s)
3. **libcluster** -- Kubernetes headless service, gossip, etc.

### CPU Count Reporting

Each node publishes its CPU count to Raft state on startup and on any change:

```elixir
# On node startup / scheduler change
ra:process_command(cluster_id, {:report_cpus, node(), System.schedulers_online()})
```

The Raft state machine maintains:

```elixir
%{
  node_cpus: %{
    :"ferric@10.0.1.1" => 8,
    :"ferric@10.0.1.2" => 8,
    :"ferric@10.0.1.3" => 8
  },
  shard_count: 24,
  epoch: 2,
  last_reindex_at: ~U[2026-03-21 10:00:00Z]
}
```

### CPU Count Integrity

A node cannot lie about its CPU count in a way that harms the cluster because:

- The reindex only triggers when all CPUs are equal, so a lying node just
  delays or triggers a reindex with a wrong total
- The `multiplier` is the same on every node (from shared config)
- If a node reports 16 CPUs but only has 8, it gets assigned 16 shards worth
  of work and suffers performance degradation -- self-punishing
- Operators can verify via `CLUSTER INFO` which shows each node's reported CPUs

### Node Departure

When a node leaves (crash, shutdown, network partition):

1. Raft detects the member is down (heartbeat timeout)
2. The leader removes it from `node_cpus` via `{:node_leave, node}`
3. Remaining nodes check CPU equality -- if equal, reindex
4. If not equal, cluster continues with current shard layout
5. Keys owned by the departed node's shards are unavailable until either:
   - The node returns, or
   - A reindex reassigns those shards to surviving nodes

---

## 8. Error Scenarios

### Scenario: Node Joins Mid-Reindex

The reindex is a single Raft command. If a node joins while the command is
being applied, it will receive the command during log replay and apply it
normally. The new node's CPU report arrives after the reindex, potentially
triggering another reindex check on the next 30s cycle.

### Scenario: Leader Fails During Reindex Proposal

Raft elects a new leader. If the command was committed, all nodes apply it.
If it was not committed, it is lost. The new leader's periodic check will
re-propose it on the next cycle.

### Scenario: Network Partition During Reindex

The reindex command is committed via Raft quorum. Nodes in the minority
partition do not receive the command and continue with the old shard layout.
When the partition heals, they replay the log and apply the reindex.

### Scenario: Out-of-Memory During Reindex

During reindex, memory usage approximately doubles (old + new ETS tables
exist simultaneously). If a node OOMs:

- The BEAM crashes
- On restart, Raft replay reconstructs state from Bitcask + log
- Mitigation: pre-check available memory before proposing reindex
- The leader can include a memory check in the periodic trigger

```elixir
# Pre-check: estimate memory needed
total_keys = cluster_dbsize()
estimated_bytes = total_keys * 200  # ~200 bytes per ETS entry average
available = :erlang.memory(:total) * 0.5
if estimated_bytes > available, do: :skip, else: :propose
```

### Scenario: ETS Rename Race

Between `ets.delete(keydir_0)` and `ets.rename(keydir_new_0, keydir_0)`,
a read hitting `keydir_0` gets `ArgumentError`. The Router already handles
this via the `:no_table` fallback (GenServer call to the shard process).
The shard process itself may also be mid-transition, so it should handle
reads against its backup state.

---

## 9. Performance Estimates

### Reindex Time Model

The reindex is CPU-bound (ETS iteration + hash computation + ETS insert).

Per key: ~1us (phash2 ~50ns + ETS lookup ~200ns + ETS insert ~200ns + overhead)

| Key Count | Estimated Time | Notes                    |
|-----------|---------------|--------------------------|
| 10,000    | ~10ms         | Imperceptible            |
| 100,000   | ~100ms        | Barely noticeable        |
| 1,000,000 | ~1s           | Brief write pause        |
| 10,000,000| ~10s          | Significant, plan for it |
| 100,000,000| ~100s        | Probably too long, need chunking |

### Memory Overhead

During reindex, both old and new ETS tables coexist:

| Key Count | Normal ETS RAM | Peak During Reindex |
|-----------|---------------|---------------------|
| 100K      | ~20 MB        | ~40 MB              |
| 1M        | ~200 MB       | ~400 MB             |
| 10M       | ~2 GB         | ~4 GB               |

### Throughput Impact

- **Reads**: No impact during reindex (served from old tables until swap)
- **Writes**: 100% rejected during freeze window
- **After reindex**: Full throughput restored immediately

---

## 10. Configuration

```elixir
# config/config.exs

config :ferricstore,
  # Shard multiplier (shards = total_cpus * multiplier)
  shard_multiplier: 1,

  # Minimum seconds between reindex operations
  reindex_cooldown_seconds: 300,

  # Automatic reindex when CPU counts equalize
  auto_reindex: true,

  # Cluster node discovery
  cluster_strategy: :static,  # :static | :dns | :libcluster
  cluster_nodes: [:"ferric@10.0.1.1", :"ferric@10.0.1.2"],
  # cluster_dns: "ferric.internal",

  # Memory safety: skip reindex if estimated peak exceeds this fraction
  # of total BEAM memory
  reindex_memory_limit: 0.8
```

### CLUSTER Commands

```
CLUSTER INFO          -- show node CPUs, shard count, epoch, frozen status
CLUSTER NODES         -- list all nodes with their CPU counts and shard ranges
CLUSTER REBALANCE     -- force immediate reindex
CLUSTER FREEZE        -- manually freeze writes (debug)
CLUSTER UNFREEZE      -- manually unfreeze writes (debug)
```

---

## Design Rationale

### Why Not Redis-Style Hash Slots?

| Concern                | Hash Slots (Redis)              | Stop-the-World Reindex          |
|------------------------|--------------------------------|--------------------------------|
| Complexity             | MOVED/ASKING, migration state machine, dual-write, slot ownership tracking | Single Raft command |
| Client changes         | Clients must handle redirects   | Clients retry on READONLY      |
| Migration duration     | Minutes to hours (background)   | <1s for typical workloads      |
| Write availability     | Writes continue during migration| Brief write freeze             |
| Correctness risk       | Subtle bugs in partial migration| Deterministic, all-or-nothing  |
| Code to implement      | ~5,000 lines                    | ~200 lines                     |
| Operational burden     | Slot rebalancing, stuck migrations| Automatic, hands-off           |

The trade-off is clear: brief write downtime (sub-second for most workloads)
in exchange for dramatically simpler code and operations. For a system that
targets <10M keys per node, this is the right call.

### Why Require Equal CPUs?

Requiring equal CPU counts before reindex ensures:

1. **Even shard distribution** -- every node gets the same number of shards
2. **No complex weight-based assignment** -- no need to decide how to split
   17 shards across nodes with 8 and 16 CPUs
3. **Predictable capacity planning** -- operators add identical nodes
4. **Simpler reasoning** -- the cluster is either balanced or waiting to balance

This matches how most production deployments work: identical instances from
the same instance type / container spec.
