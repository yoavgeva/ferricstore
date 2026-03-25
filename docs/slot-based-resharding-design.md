# Slot-Based Resharding Design

## Status

**Draft** -- March 2026

**Supersedes**: `resharding-design.md` (stop-the-world reindex approach)

**Related**: `resharding-analysis.md` (codebase feasibility study for Option C lazy resharding)

---

## 1. Motivation

### Current State

FerricStore routes keys via `phash2(key, shard_count)` where `shard_count` is a
compile-time constant (`Application.compile_env(:ferricstore, :shard_count, 4)`)
baked into the `Router` module. This has three concrete problems:

1. **Compile-time lock-in.** Changing shard count requires recompilation and a
   full restart. The `@shard_count` attribute in `Router` (line 19), `Health`
   (line 37), and `Commands.Cluster` (line 17) generates code that cannot be
   changed at runtime.

2. **Stop-the-world resharding.** The current resharding design (`resharding-design.md`)
   freezes all writes, creates N new ETS tables, rehashes every key via
   `phash2(key, new_count)`, swaps tables, and unfreezes. For 10M keys this
   takes ~10 seconds of write unavailability. For 100M keys it is impractical.

3. **Manual shard count.** The operator must configure `:shard_count` manually.
   The natural value is `System.schedulers_online()` (one shard per CPU core),
   but the compile-time dependency prevents auto-detection.

### Proposed State

Adopt the Redis Cluster / DragonflyDB hash-slot indirection model:

```
key  -->  phash2(key) % 1024  -->  slot  -->  slot_map[slot]  -->  shard_index
```

- **1,024 hash slots** provide the indirection layer between hash function
  output and shard assignment.
- **Slot map** is a 1,024-element tuple stored in `:persistent_term`, mapping
  each slot to its owning shard index. Updated atomically.
- **Live resharding** moves slots between shards one at a time, without stopping
  reads or writes. No write freeze. No rehashing of all keys.
- **Auto-configured shard count.** Default: `System.schedulers_online()`. No
  user config needed. Scaling up/down adjusts the slot map, not the hash function.

### Why 1,024 Slots?

- **Large enough** to distribute evenly across up to 128 shards (8 slots per
  shard at 128 shards). FerricStore uses 1 shard per CPU core —
  128 cores is the practical ceiling for single-node deployments.
- **Small enough** to fit entirely in L1 CPU cache (1,024 * 8 bytes = 8 KB).
  16,384 slots (128 KB) would exceed L1 cache on most CPUs.
- **Power of two** enabling `band(phash2(key), 0x3FF)` instead of modulo.
- **Sufficient granularity** for live resharding. Moving 1 slot = ~0.1% of
  data. Finer than needed for local shard rebalancing.

### Why phash2, Not phash2?

Redis Cluster uses phash2 for cross-node slot compatibility (MOVED/ASK redirects).
FerricStore doesn't need this — every node has all data via Raft replication.
There are no MOVED redirects. Slots are purely internal for local shard
assignment. `:erlang.phash2` is the BEAM's built-in hash — fast, well-distributed,
no NIF dependency. Hash tags (`{tag}`) are extracted before hashing, providing
the same co-location guarantee as Redis.

### Resharding Is LOCAL Per-Node, Not Cross-Cluster

FerricStore uses Raft replication. Every node in a cluster holds ALL data --
the full keyspace is replicated to every node. Shards are a **local parallelism
optimization**: they partition the keyspace across multiple ETS tables, Bitcask
instances, and GenServer processes so that a 16-core machine can process 16
independent key ranges concurrently.

Resharding means rearranging data between local shards on the same node. It is
NOT about moving data between nodes (which is unnecessary since every node
already has everything). Each node independently manages its own shard layout
based on its local hardware.

```
Raft replication: every node gets every key (full replication)
Shards: local parallelism optimization per node
Resharding: each node independently rebalances its local shards

Node A (16 cores): slot 7823 -> local shard 15
Node B (8 cores):  slot 7823 -> local shard 7
Same key, same slot, different local shards. Correct -- each node manages its own layout.

Node C (4 cores):  slot 7823 -> local shard 3
All three nodes have the key. Raft ensures they agree on the value.
The shard index is a local routing detail, invisible to replication.
```

**Implications:**

- The slot map is per-node. Each node persists its own `data/slot_map.bin`.
- When a node scales from 8 to 16 cores, only that node reshards. Other nodes
  are unaffected.
- There is no "slot migration across the network" in the Raft replication model.
  Network-level data movement is handled entirely by Raft log replication, which
  is key-granular and independent of shard layout.
- If a future FerricStore variant uses partitioned (non-replicated) sharding
  across nodes, the slot map would become cluster-global. That is out of scope
  for the current Raft-replicated architecture.

---

## 2. Current Architecture

This section documents the exact sharding machinery as it exists today, based on
the codebase at commit `816c6b8`.

### 2.1 Routing (`Router`)

```elixir
# router.ex L19 -- compile-time constant
@shard_count Application.compile_env(:ferricstore, :shard_count, 4)

# router.ex L24-26 -- pre-computed atom names
@shard_names List.to_tuple(
  for i <- 0..(@shard_count - 1), do: :"Ferricstore.Store.Shard.#{i}"
)

# router.ex L50-52
def shard_for(key, shard_count \\ @shard_count) do
  :erlang.phash2(key, shard_count)
end

# router.ex L67-68
def shard_name(index) when index >= 0 and index < @shard_count,
  do: elem(@shard_names, index)
```

The hot read path (`Router.get/1`) bypasses the shard GenServer entirely for ETS
hits. It computes `shard_for(key)` to derive the keydir table name
`:"keydir_#{idx}"` and performs a direct `:ets.lookup`. Only cache misses fall
back to `GenServer.call(shard_name(idx), {:get, key})`.

Write path: `Router.put/3` dispatches to `GenServer.call(shard_name(shard_for(key)), {:put, ...})`.
The shard GenServer delegates to the Raft Batcher for group-commit.

Cross-shard operations: `Router.list_op/2` for `LMOVE` handles the case where
source and destination keys hash to different shards (pop from source, push to
destination).

### 2.2 Per-Shard Resources

Each shard index `i` owns:

| Resource | Name / Path | Created In |
|----------|-------------|------------|
| GenServer process | `:"Ferricstore.Store.Shard.#{i}"` | `Shard.start_link/1` |
| ETS keydir table | `:"keydir_#{i}"` | `Shard.init/1` |
| ETS prefix index | `:"prefix_keys_#{i}"` | `PrefixIndex.create_table/1` |
| ETS bloom registry | (per-shard) | `BloomRegistry.create_table/1` |
| Raft group | `:"ferricstore_shard_#{i}"` | `Raft.Cluster.start_shard_server/5` |
| Batcher GenServer | `:"Ferricstore.Raft.Batcher.#{i}"` | `Application.start/2` |
| AsyncApplyWorker | `:"Ferricstore.Raft.AsyncApplyWorker.#{i}"` | `Application.start/2` |
| MergeScheduler | `:"Ferricstore.Merge.Scheduler.#{i}"` | `Merge.Supervisor.init/1` |
| Data directory | `data/shard_#{i}/` | `DataDir.ensure_layout!/2` |
| Bitcask log files | `data/shard_#{i}/00000.log` ... | `Shard.init/1` |
| Promoted instances | `data/dedicated/shard_#{i}/{type}:{hash}/` | `Promotion.open_dedicated/4` |

### 2.3 Supervision Tree

```
Ferricstore.Supervisor (:one_for_one)
  +-- Stats, SlowLog, AuditLog, Config, NamespaceConfig, Acl, HLC
  +-- Batcher.0, Batcher.1, ..., Batcher.{N-1}     (started in Application.start)
  +-- ShardSupervisor (:one_for_one, static N children)
  |     +-- Shard.0, Shard.1, ..., Shard.{N-1}
  +-- AsyncApplyWorker.0, ..., AsyncApplyWorker.{N-1}
  +-- Merge.Supervisor
  |     +-- Semaphore
  |     +-- Scheduler.0, ..., Scheduler.{N-1}
  +-- PubSub, FetchOrCompute, MemoryGuard
```

`ShardSupervisor` is a **static** `Supervisor` (not `DynamicSupervisor`). It
creates exactly `shard_count` children at init time and does not support adding
or removing children at runtime.

### 2.4 File ID Scope

File IDs are **per-shard, not globally unique**. Each shard maintains its own
monotonically increasing `active_file_id` starting from 0. This means
`file_id=3` in shard 0 (`data/shard_0/00003.log`) and `file_id=3` in shard 1
(`data/shard_1/00003.log`) are completely different physical files. ETS 7-tuples
cannot be moved between shards without rewriting the value to the target shard's
Bitcask files. This is a critical constraint for the migration protocol.

### 2.5 ETS Keydir Format

```
{key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
```

- `value = nil`: cold key (evicted from RAM by LFU). Disk location is in
  `file_id`/`offset`/`value_size`.
- `expire_at_ms = 0`: no expiry.
- The 7-tuple format is hardcoded throughout the codebase (~50 pattern match
  sites in Router, Shard, StateMachine, MemoryGuard, Health, Metrics, etc.).

---

## 3. Proposed Architecture

### 3.1 Hash Function: phash2

Replace `:erlang.phash2` with phash2-CCITT (the Redis Cluster hash function).

**Why phash2 over phash2:**

- **Hash tags.** phash2 enables Redis-compatible hash tags: `{user:42}:session`
  and `{user:42}:profile` hash to the same slot. `phash2` does not support
  substring extraction because it hashes the entire binary.
- **Cross-system compatibility.** Redis clients, Cluster-aware proxies, and
  monitoring tools all understand phash2 % 1024 slot assignment.
- **Deterministic specification.** phash2-CCITT is a published polynomial
  (`0x1021`). `phash2` is an Erlang implementation detail that is not guaranteed
  stable across OTP versions (though it has been stable in practice).

**Implementation: Rust NIF.**

FerricStore uses `:erlang.phash2/2` — the BEAM's built-in hash function. No NIF
needed, no Rust code, no external dependency. It's fast (~30-40ns), well-distributed,
and available on every BEAM platform.

```elixir
def slot_for(key) do
  hash_input = extract_hash_tag(key) || key
  :erlang.phash2(hash_input, 1024)
end

def shard_for(key) do
  slot = slot_for(key)
  slot_map = :persistent_term.get(:ferricstore_slot_map)
  elem(slot_map, slot)
end
```

**Performance:** `:erlang.phash2` is ~30-40ns. The `extract_hash_tag` binary scan
adds ~60-80ns. The slot map lookup via `:persistent_term.get` + `elem/2` adds ~5ns.
Total routing: ~100-130ns per key.

No custom hash implementation needed — `:erlang.phash2/2` is built into the BEAM.

### 3.2 Hash Tags (Redis-Compatible)

Hash tags allow the user to control which slot a key maps to, enabling
multi-key operations on keys that share a slot.

```
{user:42}:session   -->  phash2("user:42") % 1024  -->  slot 7231
{user:42}:profile   -->  phash2("user:42") % 1024  -->  slot 7231
{user:42}:cart      -->  phash2("user:42") % 1024  -->  slot 7231
plain_key           -->  phash2("plain_key") % 1024 -->  slot 10442
```

**Implementation:**

```elixir
defmodule Ferricstore.Store.HashTag do
  @moduledoc """
  Extracts the hash tag substring from a key, following Redis Cluster rules.

  The hash tag is the substring between the first `{` and the next `}` after it.
  If no valid hash tag is found (no braces, empty braces, no closing brace),
  the entire key is used for hashing.
  """

  @spec extract(binary()) :: binary()
  def extract(key) when is_binary(key) do
    case :binary.match(key, <<"{">>) do
      {start, 1} ->
        rest_start = start + 1
        rest_len = byte_size(key) - rest_start

        case :binary.match(key, <<"}">>, scope: {rest_start, rest_len}) do
          {end_pos, 1} when end_pos > rest_start ->
            binary_part(key, rest_start, end_pos - rest_start)

          _ ->
            key
        end

      :nomatch ->
        key
    end
  end
end
```

**Slot assignment with hash tags:**

```elixir
@doc "Returns the slot (0-16383) for a key, respecting hash tags."
@spec slot_for(binary()) :: non_neg_integer()
def slot_for(key) do
  hash_input = HashTag.extract(key)
  Bitwise.band(phash2(hash_input), 0x3FF)
end
```

This is the same function used for routing. The extract + phash2 + mask is the
complete slot computation. Hash tags are a one-function change: `extract/1`
returns either the tag contents or the full key, and the rest of the pipeline
is unchanged.

#### 3.2.1 Why Hash Tags Exist: Multi-Key Command Co-Location

Hash tags guarantee that related keys land on the same slot (and therefore the
same shard), enabling atomic multi-key operations. Without hash tags, users have
no control over which shard a key lands on.

**Commands that require same-slot keys for correctness:**

| Command | Keys involved | What happens without hash tags |
|---------|--------------|-------------------------------|
| MGET / MSET / MSETNX | All keys in the command | Keys on different shards read/written independently (works but not atomic for MSET/MSETNX) |
| SDIFF / SINTER / SUNION | All set keys | Cross-shard set algebra (currently works via gather-from-each-shard in embedded, but slow) |
| RENAME / RENAMENX | source + destination | Source and dest on different shards = not atomic |
| COPY | source + destination | Same issue as RENAME |
| LMOVE / RPOPLPUSH | source + destination | Same issue -- pop and push are not atomic cross-shard |
| MULTI / EXEC | All keys in transaction | Transaction spans multiple shards = not truly atomic |
| WATCH | Watched keys | Watch check per-shard, not globally atomic |
| PFMERGE | All HLL keys | Need to read from multiple shards |

**Example: why this matters**

```
# Without hash tags -- keys land on random shards:
SET user:42:balance 100      -> shard 3
SET user:42:pending_order 50 -> shard 7

# MULTI/EXEC wants to atomically debit balance and clear pending_order.
# But the keys are on different shards. The transaction is NOT atomic.
# A crash between the two writes leaves inconsistent state.

# With hash tags -- keys co-locate:
SET {user:42}:balance 100      -> slot 5823 -> shard 3
SET {user:42}:pending_order 50 -> slot 5823 -> shard 3

# Now MULTI/EXEC operates on a single shard. Truly atomic.
```

#### 3.2.2 CROSSSLOT Error Enforcement

When multi-key commands receive keys that resolve to different slots, FerricStore
should return a CROSSSLOT error to prevent silently non-atomic operations.

```
> MGET {user:42}:session {user:99}:session
slot_for("{user:42}:session")  -> slot 5823
slot_for("{user:99}:session")  -> slot 1204
DIFFERENT slots -> return: -CROSSSLOT Keys in request don't hash to the same slot
```

This matches Redis Cluster behavior. Users must fix their key design (use
consistent hash tags) or split their command into per-slot batches.

**Enforcement modes:**

| Mode | Behavior | When |
|------|----------|------|
| **Standalone (RESP server)** | Always enforce CROSSSLOT for multi-key commands | Default for TCP clients |
| **Embedded (library API)** | Configurable -- enforce or allow cross-shard gather | `config :ferricstore, :crossslot_policy, :enforce \| :allow` |
| **Current (no slots)** | No enforcement, cross-shard ops work but may be non-atomic | Pre-slot-based routing |

**Rationale for configurable enforcement in embedded mode:** When FerricStore
runs as an embedded library (not a TCP server), the application controls all
access patterns. Some applications may prefer the convenience of cross-shard
MGET (non-atomic but functionally fine for read-only operations) over strict
CROSSSLOT errors. The TCP server always enforces for Redis compatibility.

**Implementation in Router:**

```elixir
@doc """
Validates that all keys in a multi-key command resolve to the same slot.
Returns {:ok, slot} if all keys share a slot, or {:error, :crossslot} if not.
"""
@spec check_same_slot([binary()]) :: {:ok, non_neg_integer()} | {:error, :crossslot}
def check_same_slot([]), do: {:ok, 0}

def check_same_slot([first_key | rest]) do
  slot = slot_for(first_key)

  if Enum.all?(rest, fn key -> slot_for(key) == slot end) do
    {:ok, slot}
  else
    {:error, :crossslot}
  end
end
```

**When to add CROSSSLOT enforcement:**

- Hash tag support (the `extract/1` function): add immediately with phash2.
  It is a one-function change in Router with zero performance impact.
- CROSSSLOT error enforcement: add when slot-based resharding is implemented
  (Phase 1). Before slots exist, there is no slot concept to enforce against.

### 3.3 Slot Map

The slot map is the central data structure that maps each of the 1,024 slots
to a shard index. It is stored in `:persistent_term` for near-zero-cost reads
on the hot path.

**Data structure:** A 1,024-element Erlang tuple.

```elixir
# Initial slot map for 8 shards (128 slots per shard):
slot_map = List.to_tuple(
  for slot <- 0..16_383 do
    div(slot, div(1_024, 8))  # slot 0-2047 -> shard 0, etc.
  end
)

:persistent_term.put(:ferricstore_slot_map, slot_map)
```

**Why a tuple in `:persistent_term`?**

- `elem(tuple, index)` is O(1) and takes ~5ns.
- `:persistent_term.get/1` is ~5ns (no copying -- returns a direct reference).
- Total slot lookup: ~10ns.
- The 8 KB tuple is immutable. Updating the slot map means building a new
  tuple and calling `:persistent_term.put/2`, which atomically replaces the
  old one. All processes see the new map on their next access.
- `:persistent_term.put/2` triggers a global GC pause proportional to the
  number of processes (~1us per process). For a system with 10,000 connections,
  this is ~10ms. Slot map updates are rare (only during resharding) so this is
  acceptable.

**Slot map manager module:**

```elixir
defmodule Ferricstore.Store.SlotMap do
  @moduledoc """
  Manages the 1,024-slot to shard-index mapping.

  The slot map is stored in :persistent_term for ~10ns read access.
  Updates are atomic (all processes see the new map on next read).
  """

  @num_slots 1_024

  @spec num_slots() :: pos_integer()
  def num_slots, do: @num_slots

  @spec init(pos_integer()) :: :ok
  def init(shard_count) do
    map = build_uniform(shard_count)
    :persistent_term.put(:ferricstore_slot_map, map)
    :persistent_term.put(:ferricstore_shard_count, shard_count)
    :ok
  end

  @spec shard_for_slot(non_neg_integer()) :: non_neg_integer()
  def shard_for_slot(slot) do
    map = :persistent_term.get(:ferricstore_slot_map)
    elem(map, slot)
  end

  @spec get() :: tuple()
  def get do
    :persistent_term.get(:ferricstore_slot_map)
  end

  @spec update(tuple()) :: :ok
  def update(new_map) when tuple_size(new_map) == @num_slots do
    :persistent_term.put(:ferricstore_slot_map, new_map)
    :ok
  end

  @spec slot_count_for_shard(tuple(), non_neg_integer()) :: non_neg_integer()
  def slot_count_for_shard(map, shard_index) do
    Enum.count(0..(@num_slots - 1), fn slot ->
      elem(map, slot) == shard_index
    end)
  end

  @spec build_uniform(pos_integer()) :: tuple()
  def build_uniform(shard_count) do
    slots_per_shard = div(@num_slots, shard_count)
    remainder = rem(@num_slots, shard_count)

    {map_list, _} =
      Enum.reduce(0..(shard_count - 1), {[], 0}, fn shard, {acc, offset} ->
        count = slots_per_shard + if(shard < remainder, do: 1, else: 0)
        entries = List.duplicate(shard, count)
        {acc ++ entries, offset + count}
      end)

    List.to_tuple(map_list)
  end
end
```

### 3.4 Routing Change

The new `Router` replaces the compile-time `@shard_count` and `phash2` with
runtime slot-based lookup.

```elixir
defmodule Ferricstore.Store.Router do
  alias Ferricstore.Store.{phash2, HashTag, SlotMap}

  # REMOVED: @shard_count Application.compile_env(...)
  # REMOVED: @shard_names pre-computed tuple

  @doc "Returns the slot (0-16383) for a key."
  @spec slot_for(binary()) :: non_neg_integer()
  def slot_for(key) do
    hash_input = HashTag.extract(key)
    Bitwise.band(phash2(hash_input), 0x3FF)
  end

  @doc "Returns the shard index that owns `key`."
  @spec shard_for(binary()) :: non_neg_integer()
  def shard_for(key) do
    slot = slot_for(key)
    SlotMap.shard_for_slot(slot)
  end

  @doc "Returns the registered process name for shard at `index`."
  @spec shard_name(non_neg_integer()) :: atom()
  def shard_name(index), do: :"Ferricstore.Store.Shard.#{index}"

  # phash2 with NIF fast path, pure Elixir fallback
  defp phash2(data) do
    Ferricstore.Bitcask.NIF.phash2(data)
  rescue
    UndefinedFunctionError -> phash2.hash(data)
  end

  # ... rest of Router functions unchanged in structure,
  # but all references to @shard_count become runtime lookups
  # via :persistent_term.get(:ferricstore_shard_count)
end
```

**Iteration patterns** (`keys/0`, `keys_with_prefix/1`, `dbsize/0`) that
currently use `0..(@shard_count - 1)` change to:

```elixir
def keys do
  shard_count = :persistent_term.get(:ferricstore_shard_count)
  Enum.flat_map(0..(shard_count - 1), fn i ->
    GenServer.call(shard_name(i), :keys)
  end)
end
```

### 3.5 Shard Lifecycle

Shards become dynamic. The static `ShardSupervisor` is replaced with a
`DynamicSupervisor` that supports adding and removing shard children at runtime.

```elixir
defmodule Ferricstore.Store.ShardSupervisor do
  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: 100,
      max_seconds: 60
    )
  end

  @doc "Starts a new shard under the supervisor."
  @spec start_shard(non_neg_integer(), binary()) :: DynamicSupervisor.on_start_child()
  def start_shard(index, data_dir) do
    spec = {Ferricstore.Store.Shard, [index: index, data_dir: data_dir]}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  @doc "Stops and removes a shard from the supervisor."
  @spec stop_shard(non_neg_integer()) :: :ok | {:error, :not_found}
  def stop_shard(index) do
    name = Ferricstore.Store.Router.shard_name(index)
    case Process.whereis(name) do
      nil -> {:error, :not_found}
      pid -> DynamicSupervisor.terminate_child(__MODULE__, pid)
    end
  end
end
```

**Default shard count:** `System.schedulers_online()`. No user configuration
needed. The application starts with one shard per CPU core.

Each shard still owns: ETS keydir, Raft group, Batcher, AsyncApplyWorker,
MergeScheduler, PrefixIndex, BloomRegistry.

**Companion process startup.** When a shard is dynamically started, its
companion processes (Batcher, AsyncApplyWorker, MergeScheduler) must also be
started. This is coordinated via a new `Ferricstore.Store.ShardLifecycle`
module:

```elixir
defmodule Ferricstore.Store.ShardLifecycle do
  @moduledoc """
  Coordinates starting/stopping all processes for a shard index.
  """

  @spec start_shard(non_neg_integer(), binary()) :: :ok | {:error, term()}
  def start_shard(index, data_dir) do
    with {:ok, _} <- ShardSupervisor.start_shard(index, data_dir),
         {:ok, _} <- start_batcher(index),
         {:ok, _} <- start_async_worker(index),
         {:ok, _} <- start_merge_scheduler(index, data_dir) do
      :ok
    end
  end

  @spec stop_shard(non_neg_integer()) :: :ok
  def stop_shard(index) do
    # Stop in reverse order: merge, async worker, batcher, then shard
    stop_merge_scheduler(index)
    stop_async_worker(index)
    stop_batcher(index)
    Raft.Cluster.stop_shard_server(index)
    ShardSupervisor.stop_shard(index)
    :ok
  end
end
```

---

## 4. Live Resharding Protocol

### 4.1 Overview

The unit of migration is a **slot**, not a key. A slot contains all keys where
`phash2(key) % 1024 == slot_id`. Moving a slot means moving all its keys from
the source shard to the target shard.

Key properties:

- **No write freeze.** Reads and writes continue throughout migration.
- **Local per-node.** Each node reshards its own local shards independently.
  There is no cross-node data transfer for resharding. Raft replication is
  orthogonal -- it ensures all nodes have the same data regardless of local
  shard layout.
- **Atomic slot ownership transfer.** At any point, each slot has exactly one
  authoritative owner for writes. Reads may check a secondary shard during
  migration.
- **Idempotent.** Re-migrating a key that was already migrated is safe (the
  target shard already has it, the write is a no-op or last-writer-wins).
- **Interruptible and resumable.** The migrator checkpoints its position.
  Crashing mid-migration loses at most one key's worth of progress.

### 4.2 Migration State Machine

Each slot can be in one of three states:

```
          +----------+
          |  STABLE  |   <-- normal operation
          +----+-----+
               |
     coordinator initiates migration
               |
          +----v-----------+
          |   MIGRATING    |   source shard: slot is being drained
          |  src=A, dst=B  |   target shard: slot is being filled
          +----+-----------+
               |
     all keys for slot migrated + verified
               |
          +----v-----+
          |  STABLE  |   <-- slot now owned by target shard
          +----------+
```

There is no separate `IMPORTING` state visible to the router. The slot map
update (changing the slot's owner from A to B) happens at the END of migration,
not the beginning. This simplifies the protocol significantly:

- During migration, the slot map still points to shard A (source).
- New writes for this slot go to shard A (the current owner).
- The migrator reads keys from shard A and writes them to shard B.
- Once all keys are migrated, the slot map is atomically updated: slot -> B.
- After the update, new writes go to shard B.

This is the **"migrate ETS, lazy-migrate disk"** approach. ETS data is migrated
immediately during the slot transfer. Disk data is migrated lazily via
read-through and cleaned up by compaction.

### 4.3 Migration Flow (Detailed)

```
Phase 1: INITIATE
  1. Coordinator decides to move slot S from shard A to shard B.
  2. Coordinator records migration intent:
     :persistent_term.put(:ferricstore_migrating_slots,
       Map.put(current, S, %{src: A, dst: B, status: :migrating}))
  3. Shard B must be running and healthy.

Phase 2: ETS MIGRATION (instant)
  4. Migrator iterates shard A's keydir (:"keydir_A"):
     for each key K where slot_for(K) == S:
       a. Read the full 7-tuple from shard A's ETS
       b. Insert into shard B's ETS keydir (:"keydir_B")
          - file_id/offset/value_size still reference shard A's Bitcask files
          - This is fine: the lazy disk migration handles it (see Phase 3)
       c. Delete from shard A's ETS keydir
       d. Update shard B's prefix index
       e. Delete from shard A's prefix index

  This is fast: ~100ms for 1M keys (ETS insert + delete is ~100ns per key).
  During this phase, the slot map still points to shard A.

  5. Atomically update slot map: slot S -> shard B.
     :persistent_term.put(:ferricstore_slot_map, new_map)
     New writes for slot S now go to shard B's Bitcask (correct shard).

Phase 3: LAZY DISK MIGRATION (background, driven by reads)
  6. After the slot map switch, keys in slot S have their ETS entries in
     shard B, but their Bitcask records may still be in shard A's files.

  Cold read path for a migrated key:
    a. ETS lookup in shard B's keydir -> finds entry with file_id/offset
    b. file_id/offset reference shard A's Bitcask directory
    c. Shard B checks its OWN Bitcask first (new writes land here)
    d. If not found, falls back to shard A's Bitcask (legacy location)
    e. On successful fallback read: lazy-migrate the record
       - Write the value to shard B's Bitcask (gets new file_id/offset)
       - Update shard B's ETS entry with new file_id/offset
       - Write tombstone in shard A's Bitcask for the old record
    f. Return the value to the caller

  This means:
    - First cold read of a migrated key: 2x latency (check two Bitcask dirs)
    - Subsequent reads: normal latency (record is now in shard B's Bitcask)
    - Hot reads (value in ETS): zero impact (never touch disk)

Phase 4: COMPACTION CLEANUP (automatic, no extra work)
  7. Shard A's Bitcask files now contain dead records (migrated keys).
     The EXISTING MergeScheduler for shard A will compact these away on
     its normal cycle. No special cleanup process needed.

  8. Shard A's dead records are reclaimed when:
     - Tombstoned records are removed during merge
     - Files with high dead-record ratios are compacted
     - This happens automatically -- no operator intervention
```

**Why lazy disk migration instead of "copy then switch":**

The original design proposed copying all Bitcask records during migration, then
switching. This has problems:

1. **Disk space doubling.** Every migrated key exists in both source and target
   Bitcask files until compaction runs. For large datasets this is significant.
2. **Migration speed bottleneck.** Bitcask writes are ~200K/sec sequential
   append. Migrating 5M keys takes ~25 seconds of sustained disk I/O.
3. **Unnecessary work.** Many keys are hot (value cached in ETS). They will
   never be read from disk. Copying their Bitcask records is wasted I/O.

The lazy approach:

1. **No extra disk space.** Compaction reclaims dead records automatically.
2. **Instant ETS migration.** The slot is available on the new shard in ~100ms.
3. **Disk migration is demand-driven.** Only cold keys that are actually
   accessed pay the cost. Hot keys never touch the disk migration path.

### 4.4 Impact Summary

| Path | Impact During Migration | Impact After Migration |
|------|------------------------|----------------------|
| **Hot reads (ETS hit)** | Zero -- value is in ETS, served from new shard | Zero |
| **Cold reads (first access)** | 2x latency on first read (check new Bitcask, fallback to old, lazy-migrate) | Normal after lazy-migrate |
| **Cold reads (subsequent)** | Normal -- record was lazy-migrated to new Bitcask | Normal |
| **Writes** | Zero -- go to correct shard immediately after ETS migration | Zero |
| **Disk space** | No increase -- compaction handles dead records | No increase |
| **ETS migration** | ~100ms for 1M keys (one-time cost per slot batch) | N/A |
| **CPU** | Negligible -- ETS insert/delete is ~100ns/key | Zero |
| **Reads to non-migrating slots** | Zero impact | Zero impact |
| **Writes to non-migrating slots** | Zero impact | Zero impact |

### 4.5 Tracking Legacy Bitcask Locations

After ETS migration (Phase 2), shard B's ETS entries contain file_id/offset
values that reference shard A's Bitcask files. The shard must know which
Bitcask directory to read from.

**Approach: migration source map.**

```elixir
# Stored in :persistent_term, maps shard index to a list of
# source shard indices whose Bitcask files may contain legacy records.
:ferricstore_migration_sources => %{
  11 => [3],      # shard 11 may have legacy records in shard 3's Bitcask
  12 => [4, 5],   # shard 12 may have legacy records from shards 4 and 5
}
```

When shard B performs a cold read and the file_id/offset does not resolve in
its own Bitcask directory, it checks each source shard's directory in order.
Once the record is lazy-migrated, the ETS entry is updated with shard B's
own file_id/offset, and subsequent reads go directly to shard B's files.

The migration source map entries are removed once shard A's compaction has
run and all legacy records have either been lazy-migrated or are confirmed
dead (no ETS entries reference shard A's files for keys in the migrated slots).

### 4.6 Read Path During Migration

During ETS migration (~100ms), reads for slot S keys may see partial state:
some keys already in shard B's ETS, some still in shard A's. The slot map
still points to shard A during this phase. The Router reads from shard A,
which may not find keys that have already moved to shard B.

**Handling:** During the brief ETS migration window, the migrator holds slot S
in a `migrating` state. Reads for migrating-slot keys check both shard A and
shard B:

```elixir
def get(key) do
  slot = slot_for(key)

  case check_migrating(slot) do
    nil ->
      # Normal path: read from the owning shard
      shard_idx = SlotMap.shard_for_slot(slot)
      do_get(key, shard_idx)

    %{src: src, dst: dst} ->
      # Migration in progress: check destination first, then source
      case do_get(key, dst) do
        {:ok, _} = result -> result
        :not_found -> do_get(key, src)
      end
  end
end
```

This dual-read path is only active during the ~100ms ETS migration window.
Once the slot map is updated (slot S -> shard B), the normal single-read
path resumes.

### 4.7 Write Path During Migration

During ETS migration, writes for slot S continue going to shard A (slot map
still points to A). The migrator will pick up these writes in its ETS scan.

After the slot map update, writes go to shard B. No special handling needed.

```elixir
def put(key, value, expire_at_ms \\ 0) do
  slot = slot_for(key)
  shard_idx = SlotMap.shard_for_slot(slot)
  GenServer.call(shard_name(shard_idx), {:put, key, value, expire_at_ms})
end
```

There is no per-slot freeze. The ETS migration is fast enough (~100ms) that
the brief dual-read window is acceptable. No BUSY errors. No write queuing.

### 4.8 Consistency Guarantees

**During normal operation (no migration):** Identical to current behavior.
Single-writer per shard via Raft.

**During ETS migration (~100ms):** Reads may check two shards for keys in the
migrating slot. All other slots are unaffected. Writes continue to shard A
(still the owner per slot map). No data loss possible -- the key exists in
at least one of the two shards.

**After ETS migration:** Slot map updated. Reads and writes go to shard B.
Full consistency. Cold reads may hit shard A's Bitcask on first access, but
the lazy-migration path is invisible to the caller.

**Crash during ETS migration:** The slot map still points to shard A. Some
keys may have been partially migrated to shard B's ETS. On restart, shards
rebuild ETS from Bitcask. Since Bitcask files have not been modified (lazy
migration has not started), the rebuild produces the original state. Migration
can be retried from scratch.

**Crash after slot map update but before all lazy migrations complete:** Slot
map points to shard B. Shard B's ETS entries reference shard A's Bitcask
files. On restart, shard B rebuilds ETS from its own Bitcask (which only
has keys written after the migration). Keys not yet lazy-migrated are
effectively lost from shard B's perspective. **Mitigation:** persist the
migration source map alongside the slot map. On restart, shard B knows to
also scan shard A's Bitcask files for its slots during ETS rebuild.

### 4.9 Migrator Process

The migrator is a dedicated GenServer (one per active migration, bounded by a
semaphore to limit concurrent migrations):

```elixir
defmodule Ferricstore.Store.SlotMigrator do
  @moduledoc """
  Migrates ETS entries for a batch of slots from source shard to target shard.

  Disk migration is lazy -- Bitcask records are migrated on first cold read.
  This process only handles the ETS transfer and slot map update.
  """

  use GenServer

  defstruct [
    :slots,              # list of slot IDs to migrate
    :source_shard,
    :target_shard,
    :keys_migrated,
    :keys_total,
    :started_at
  ]

  @keys_per_batch 1000   # keys to migrate per tick (ETS ops are fast)
  @tick_interval_ms 0    # no delay needed -- ETS is in-memory

  # ... implementation
end
```

**Rate limiting:** ETS operations are fast (~100ns each), so the migrator
can process keys at full speed. The ~100ms migration time for 1M keys is
brief enough that no rate limiting is needed. For very large slots (>10M keys),
the migrator processes in batches with periodic `Process.sleep(0)` to yield
to the scheduler.

**Concurrency limit:** At most `max(2, System.schedulers_online() / 4)` slot
batches can be migrating simultaneously.

### 4.10 Why Not Redis Cluster's MOVED/ASK Approach?

Redis Cluster uses client-side redirects:

1. Client sends command to shard A.
2. Shard A responds with `MOVED slot_id shard_B_address` or `ASK slot_id shard_B_address`.
3. Client re-sends the command to shard B.

This works for a distributed system where shards are on different network nodes.
FerricStore's shards are local (same BEAM node). There is no network hop. The
Router can simply look up the correct shard in the slot map and dispatch
directly. Client-side redirects add complexity without benefit in the local case.

For future multi-node FerricStore, MOVED/ASK semantics can be added at the TCP
protocol layer, translating slot map lookups into redirect responses for clients
that connect to the wrong node.

---

## 5. Auto-Scaling

### 5.1 Scale Up (Add Shards)

Scenario: System starts with 8 shards (8 CPU cores). A Kubernetes HPA adds
CPU resources, now 16 cores available. Each node independently detects this
and reshards its own local shards.

```
Step 1: Detect new core count
  System.schedulers_online() returns 16 (was 8).
  FerricStore auto-scaling monitor detects the change.
  This is a LOCAL decision -- no coordination with other nodes needed.

Step 2: Start new shard processes
  ShardLifecycle.start_shard(8, data_dir)
  ShardLifecycle.start_shard(9, data_dir)
  ...
  ShardLifecycle.start_shard(15, data_dir)

  Each creates: GenServer, ETS keydir, Raft group, Batcher, AsyncApplyWorker,
  MergeScheduler, data directory.

Step 3: Compute new slot map
  Old: 1024 slots / 8 shards = 128 slots each
       Shard 0: slots 0-2047
       Shard 1: slots 128-255
       ...

  New: 1024 slots / 16 shards = 64 slots each
       Shard 0: slots 0-1023      (keep)
       Shard 8: slots 1024-2047   (migrate from shard 0)
       Shard 1: slots 128-191     (keep)
       Shard 9: slots 3072-4095   (migrate from shard 1)
       ...

Step 4: Migrate slots (ETS migration, ~100ms per batch)
  For each slot batch that changes owner:
    SlotMigrator.start(slots, source_shard, target_shard)

  Migrations run in parallel (up to concurrency limit).
  Each migration follows the ETS-migrate-then-lazy-disk protocol (section 4.3).

Step 5: Update persistent_term shard_count
  :persistent_term.put(:ferricstore_shard_count, 16)

  (This happens incrementally as each slot batch's map entries are updated.)

Total ETS migration time: ~100ms per shard pair (for 1M total keys).
  All 8 shard pairs can migrate in parallel: ~100ms total.
  Disk migration: lazy, background, driven by cold reads.
  Zero downtime throughout.
```

### 5.2 Scale Down (Remove Shards)

Scenario: Scale from 16 shards to 8 shards.

```
Step 1: Compute new slot map
  Slots currently on shards 8-15 must move to shards 0-7.
  Shard 8: slots 1024-2047 -> migrate to shard 0
  Shard 9: slots 3072-4095 -> migrate to shard 1
  ...

Step 2: Migrate all slots from shards 8-15
  Same ETS migration protocol as scale-up. Each slot batch is moved from a
  higher-indexed shard to a lower-indexed shard.

Step 3: Verify shards 8-15 are empty
  All slots have been migrated. The keydirs for shards 8-15 should
  contain zero keys.

Step 4: Stop excess shard processes
  ShardLifecycle.stop_shard(8)
  ...
  ShardLifecycle.stop_shard(15)

  This stops GenServer, Batcher, AsyncApplyWorker, MergeScheduler,
  Raft group. ETS tables are deleted.

Step 5: Update shard_count
  :persistent_term.put(:ferricstore_shard_count, 8)

Step 6: Clean up data directories (optional, can be deferred)
  Remove data/shard_8/ through data/shard_15/.
  Not urgent -- compaction will have reclaimed most space. The directories
  can be removed during the next maintenance window.
```

### 5.3 Rebalancing Strategies

**Uniform distribution (default):** Each shard gets `floor(1024 / shard_count)`
slots, with the first `1024 % shard_count` shards getting one extra. This is
the simplest and works well when all shards have similar hardware.

**Weighted distribution (future):** Assign more slots to shards on nodes with
more CPU/memory. Useful for heterogeneous clusters. The slot map is just a tuple
-- any distribution can be expressed.

**Minimum-movement rebalancing:** When adding shards, compute the optimal new
distribution and only migrate slots that change owner. For doubling from 8 to 16
shards, exactly half the slots move. For adding one shard (8 to 9), ~1,820 slots
move (1024/9 per shard, redistributed from all existing shards).

### 5.4 Kubernetes Integration

```yaml
# FerricStore detects CPU changes automatically.
# No sidecar, no operator, no external controller needed.

apiVersion: apps/v1
kind: StatefulSet
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: ferricstore
        resources:
          requests:
            cpu: "8"      # starts with 8 shards
          limits:
            cpu: "16"     # can scale to 16 shards
```

**Auto-detection flow:**

1. FerricStore starts. `System.schedulers_online()` returns 8. SlotMap
   initialized with 8 shards.
2. Kubernetes VPA or manual `kubectl edit` increases CPU to 16.
3. BEAM detects new schedulers. FerricStore's `SchedulerMonitor` GenServer
   polls `System.schedulers_online()` every 30 seconds.
4. Detects change: 8 -> 16. Triggers scale-up protocol (section 5.1).
5. No pod restart needed. No configuration change needed.
6. Other nodes in the Raft cluster are unaffected. They reshard independently
   if their CPU resources change.

**Graceful shutdown:**

1. Pod receives SIGTERM.
2. `Application.prep_stop/1` fires. Sets `Health.ready? = false`.
3. Resharding is a local concern. No slots need to "move elsewhere" because
   every other node has all the data via Raft replication.
4. Shards are persisted on disk via Bitcask. On restart, shards recover from
   hint files / log scan as they do today. The slot map is loaded from
   `data/slot_map.bin`.

### 5.5 Multi-Node Considerations

Since FerricStore uses Raft replication (every node has all data), multi-node
slot distribution is NOT needed. Each node independently decides how to
distribute slots across its local shards based on its own CPU count.

```
3-node Raft cluster, all nodes have all keys:

Node A (16 cores): 16 local shards, 1024 slots each
Node B (8 cores):  8 local shards, 2048 slots each
Node C (4 cores):  4 local shards, 4096 slots each

All three nodes serve the same keyspace. A read for key "foo" (slot 12182):
  Node A: slot 12182 -> local shard 11
  Node B: slot 12182 -> local shard 5
  Node C: slot 12182 -> local shard 2

Same key, same value, same slot, different local shard indices. Correct.
```

If a future FerricStore variant uses **partitioned sharding** (non-replicated,
data split across nodes -- like Redis Cluster), the slot map would become
cluster-global and cross-node slot migration would be needed. That is a
fundamentally different architecture and is out of scope for the current
Raft-replicated design.

---

## 6. Data Structures

### 6.1 Slot Map (`:persistent_term`)

```
:ferricstore_slot_map => {shard_0, shard_0, ..., shard_0, shard_1, ...}
                          \______ 2048 ______/    \______ 2048 ______/ ...

Size: 1,024 elements * 8 bytes = 8 KB
Read: ~10ns (persistent_term.get + elem)
Write: ~10ms for 10K processes (persistent_term.put triggers global GC)
Frequency of writes: only during resharding (rare)
```

### 6.2 Migration State (`:persistent_term`)

```
:ferricstore_migrating_slots => %{
  7231 => %{src: 3, dst: 11, status: :migrating, started_at: ...},
  7232 => %{src: 3, dst: 11, status: :migrating, started_at: ...},
  ...
}

:ferricstore_migration_sources => %{
  11 => [3],      # shard 11 has legacy Bitcask records from shard 3
  12 => [4, 5],   # shard 12 has legacy records from shards 4 and 5
}
```

The migrating_slots map is checked on the read path only when non-empty.
The migration_sources map is used for cold-read Bitcask fallback. Cost when
not migrating: one `:persistent_term.get/1` returning an empty map, ~5ns.

### 6.3 Shard Name Cache

Currently, shard names are pre-computed in a compile-time tuple. With dynamic
shard counts, we use a runtime cache:

```elixir
# In Router module state or as a persistent_term
# Updated when shards are added/removed.
:ferricstore_shard_names => {
  :"Ferricstore.Store.Shard.0",
  :"Ferricstore.Store.Shard.1",
  ...
}
```

Alternatively, since `:"Ferricstore.Store.Shard.#{index}"` atom creation is a
one-time cost per shard (atoms are never GC'd), and we only have at most ~128
shards, the fallback path `:"Ferricstore.Store.Shard.#{index}"` is acceptable
at ~300ns per call. The hot ETS read path does not use shard names (it uses
`:"keydir_#{idx}"` directly), so the GenServer dispatch path (~1-5us) absorbs
the 300ns easily. No compile-time tuple needed.

---

## 7. Migration from Current System

### 7.1 Upgrade Path

The transition from `phash2 % shard_count` to `phash2 % 1024 -> slot_map`
requires a one-time data migration because the two hash functions produce
completely different key-to-shard assignments.

**Option A: Offline migration (recommended for v1)**

```
1. Stop FerricStore.
2. Run migration tool:
   a. Read all keys from all shards' Bitcask files (scan hint files).
   b. For each key:
      - Compute new_slot = phash2(key) % 1024
      - Compute new_shard = slot_map[new_slot]  (using the initial uniform map)
      - If new_shard != current_shard:
        * Read value from current shard's Bitcask
        * Write to new shard's Bitcask
        * Write tombstone to current shard's Bitcask
   c. Write updated hint files for all shards.
3. Start FerricStore with the new slot-based routing.
```

**Migration tool performance:**
- Bitcask scan speed: ~500K keys/second (sequential read from hint files).
- Bitcask write speed: ~200K keys/second (sequential append).
- For 10M keys where ~75% change shards: ~7.5M writes, ~37 seconds.
- For 1M keys: ~4 seconds.

**Option B: Live migration (future)**

Support running both hash functions simultaneously with a generation counter.
This is significantly more complex (dual reads, merge results, track which
keys have been migrated) and is deferred.

### 7.2 Backward Compatibility

| Layer | Impact |
|-------|--------|
| RESP protocol | No change. Clients see identical Redis-compatible protocol. |
| Embedded API | No change. `Ferricstore.get/1`, `Ferricstore.put/3` work identically. |
| CONFIG commands | No change. `CONFIG GET shard_count` returns the current count. |
| Key distribution | Changes. Keys will map to different shards after migration. |
| Bitcask file layout | No change. Same file format, same data directories. |
| Raft WAL | Requires reset. Old WAL entries reference old shard assignments. |

### 7.3 Version Detection

The slot map is persisted to disk (in a well-known file) so that on restart,
FerricStore knows whether it is running in v1 (phash2) or v2 (slot-based) mode:

```
data/slot_map.bin   -- if this file exists, use slot-based routing
                    -- if absent, use phash2 routing (legacy)
```

The migration tool creates this file after successful migration.

---

## 8. Performance Impact

### 8.1 Steady-State (No Migration)

| Path | Before | After | Delta |
|------|--------|-------|-------|
| Routing overhead | `phash2` ~40ns | `phash2` ~40ns + `extract_hash_tag` ~70ns + `persistent_term.get` ~5ns + `elem` ~5ns = ~120ns | +80ns (hash tags + slot indirection) |
| Hot read (ETS hit) | ~100ns total | ~100ns total | 0 |
| Cold read (GenServer) | ~3us total | ~3us total | 0 |
| Write (Raft batch) | ~500us total | ~500us total | 0 |

The phash2 NIF is faster than `phash2` because phash2 is a simpler computation
(XOR + shift) and the NIF avoids Erlang term encoding overhead. The slot map
lookup (`persistent_term.get` + `elem`) adds ~10ns but the faster hash compensates.

**Net impact: effectively zero.**

### 8.2 During Migration

| Path | Impact |
|------|--------|
| Reads to non-migrating slots | Zero impact |
| Hot reads (ETS hit) for migrating slot | Zero impact -- value in ETS, served immediately |
| Cold reads for migrating slot (first access) | 2x latency -- check new shard Bitcask, fallback to old, lazy-migrate |
| Cold reads for migrating slot (after lazy-migrate) | Normal -- record now in correct Bitcask |
| Writes to non-migrating slots | Zero impact |
| Writes to migrating slot (after ETS migration) | Zero impact -- go to correct shard's Bitcask immediately |
| Disk space | No increase -- compaction reclaims dead records automatically |
| Disk I/O | Negligible increase from lazy migration (demand-driven, not bulk) |
| CPU | Negligible -- ETS migration is ~100ms total |

### 8.3 Memory

| Component | Size |
|-----------|------|
| Slot map tuple | 8 KB |
| Migration state map | ~100 bytes per migrating slot |
| Migration source map | ~100 bytes per active source |
| Migrator process per slot batch | ~1 KB |
| Total overhead (no migration) | 8 KB |
| Total overhead (migrating 100 slots) | ~140 KB |

Negligible compared to keydir memory (200 bytes per key * millions of keys).

---

## 9. Hash Tags and Multi-Key Commands

### 9.1 Hash Tag Specification

Hash tags are the mechanism by which users ensure related keys land on the same
slot. The implementation follows the Redis Cluster specification exactly.

**Extraction algorithm:**

```elixir
def extract_hash_tag(key) do
  case :binary.match(key, "{") do
    {start, 1} ->
      case :binary.match(key, "}", [{:scope, {start + 1, byte_size(key) - start - 1}}]) do
        {end_pos, 1} when end_pos > start + 1 ->
          binary_part(key, start + 1, end_pos - start - 1)
        _ -> nil
      end
    :nomatch -> nil
  end
end

def slot_for(key) do
  hash_input = extract_hash_tag(key) || key
  phash2(hash_input) &&& 0x3FF
end
```

**Rules:**
1. Find the first `{` in the key.
2. Find the first `}` after that `{`.
3. If the substring between them is non-empty, use it as the hash input.
4. Otherwise (no `{`, no `}` after `{`, or empty `{}`), hash the full key.

**Examples:**

```
{user:42}:session   -> hash "user:42" -> slot 5823
{user:42}:profile   -> hash "user:42" -> slot 5823  (same slot!)
{user:42}:cart      -> hash "user:42" -> slot 5823  (same slot!)
foo{bar}baz         -> hash "bar"     -> slot 5061
foo{}{bar}          -> hash the full key "foo{}{bar}" (first {} is empty)
foo{{bar}}          -> hash "{bar"    (first { to first })
foo                 -> hash "foo"     (no braces)
{foo                -> hash "{foo"    (no closing brace)
```

### 9.2 Multi-Key Command Same-Slot Requirement

The following commands operate on multiple keys and require those keys to hash
to the same slot for the operation to be correct (atomic, consistent):

| Command | Keys involved | Behavior without co-location |
|---------|--------------|------------------------------|
| MGET | All keys | Keys on different shards read independently -- works but not a consistent snapshot |
| MSET / MSETNX | All keys | Keys on different shards written independently -- not atomic |
| SDIFF / SINTER / SUNION | All set keys | Cross-shard set algebra -- works in embedded mode (gather) but slow and not atomic |
| SDIFFSTORE / SINTERSTORE / SUNIONSTORE | All set keys + destination | Same as above + destination may be on yet another shard |
| RENAME / RENAMENX | source + destination | Source and dest on different shards -- delete + create is not atomic |
| COPY | source + destination | Same as RENAME |
| LMOVE / RPOPLPUSH | source + destination | Pop from source, push to dest -- not atomic cross-shard |
| SMOVE | source + destination | Remove from source set, add to dest set -- not atomic |
| MULTI / EXEC | All keys in transaction | Transaction spans shards -- not truly atomic |
| WATCH | Watched keys | Watch check per-shard, not globally atomic |
| PFMERGE | All HLL keys | Need to read HLL registers from multiple shards |
| SORT ... STORE | source + destination | Sort result written to different shard -- not atomic |
| OBJECT HELP/FREQ/etc with multiple keys | All keys | Minor -- mostly informational |

### 9.3 CROSSSLOT Error

When a multi-key command receives keys that resolve to different slots,
FerricStore returns a CROSSSLOT error:

```
> MGET {user:42}:session {user:99}:session
-CROSSSLOT Keys in request don't hash to the same slot
```

This is the Redis Cluster behavior. Users must either:
1. Use hash tags to ensure co-location: `MGET {user:42}:session {user:42}:profile`
2. Split the command into per-slot batches and issue them separately

**Implementation:**

```elixir
@doc """
Validates that all keys resolve to the same slot.
Returns {:ok, slot} on success, {:error, :crossslot} on mismatch.
"""
@spec check_same_slot([binary()]) :: {:ok, non_neg_integer()} | {:error, :crossslot}
def check_same_slot([]), do: {:ok, 0}
def check_same_slot([first_key | rest]) do
  slot = slot_for(first_key)
  if Enum.all?(rest, &(slot_for(&1) == slot)) do
    {:ok, slot}
  else
    {:error, :crossslot}
  end
end
```

**In the command dispatch pipeline:**

```elixir
# In Commands.Strings (MGET example)
def execute(["MGET" | keys], state) do
  case Router.check_same_slot(keys) do
    {:ok, _slot} ->
      values = Enum.map(keys, &Router.get/1)
      {:ok, values, state}

    {:error, :crossslot} ->
      {:error, "CROSSSLOT Keys in request don't hash to the same slot", state}
  end
end
```

### 9.4 Enforcement Modes

| Mode | CROSSSLOT enforcement | Rationale |
|------|----------------------|-----------|
| **Standalone RESP server** | Always enforce | Redis compatibility. Clients expect CROSSSLOT errors. |
| **Embedded library** | Configurable (`:enforce` or `:allow`) | Application controls access patterns. Some apps prefer convenience of cross-shard MGET. |
| **Current (pre-slot)** | No enforcement | No slot concept yet. Cross-shard ops work but may be non-atomic. |

```elixir
# config/config.exs
config :ferricstore, :crossslot_policy, :enforce  # or :allow

# In Router
def check_crossslot(keys) do
  case Application.get_env(:ferricstore, :crossslot_policy, :enforce) do
    :allow -> {:ok, :any}
    :enforce -> check_same_slot(keys)
  end
end
```

### 9.5 Implementation Timeline

1. **Hash tag extraction** (`HashTag.extract/1`): implement with phash2 in
   Phase 1. This is a pure function with zero dependencies -- it just extracts
   a substring. Adding it costs nothing.

2. **`slot_for/1` using hash tags**: implement in Phase 1 alongside phash2.
   The routing pipeline becomes `key -> extract tag -> phash2 -> slot -> shard`.

3. **`check_same_slot/1`**: implement in Phase 1. It is a simple loop calling
   `slot_for/1` on each key.

4. **CROSSSLOT error responses**: implement in Phase 1 for all multi-key
   commands listed in section 9.2. This is the enforcement point.

5. **Configurable enforcement mode**: implement in Phase 1. A single config
   flag controls whether `check_same_slot` is called or bypassed.

---

## 10. Implementation Plan

### Phase 1: Foundation (Estimated: 3-4 days)

**Goal:** Replace compile-time shard routing with runtime slot-based routing.
No live resharding yet -- just the new hash function, hash tags, CROSSSLOT
enforcement, and the indirection layer.

1. Add `extract_hash_tag/1` to Router + slot map via `:persistent_term`.
2. Implement `Ferricstore.Store.HashTag.extract/1`.
3. Implement `Ferricstore.Store.SlotMap` (init, get, update, build_uniform).
4. Convert `Router.shard_for/1` from `phash2(key, @shard_count)` to
   `SlotMap.shard_for_slot(slot_for(key))`.
5. Implement `Router.slot_for/1` with hash tag support.
6. Implement `Router.check_same_slot/1` and CROSSSLOT error handling for all
   multi-key commands (MGET, MSET, SDIFF, SINTER, SUNION, RENAME, COPY,
   LMOVE, MULTI/EXEC, WATCH, PFMERGE).
7. Remove all three `@shard_count Application.compile_env(...)` sites
   (Router, Health, Commands.Cluster). Replace with `:persistent_term.get(:ferricstore_shard_count)`.
8. Change `Application.start/2` to initialize `SlotMap.init(shard_count)`.
9. Replace `@shard_names` pre-computed tuple with runtime `shard_name/1`.
10. Update all `0..(@shard_count - 1)` iteration patterns to use the runtime
    shard count.

**Testing:** All existing tests must pass with the new routing. Keys will map
to different shards, so tests that hardcode shard assignments need updating.
Add property-based tests verifying phash2 matches Redis Cluster's output for
known test vectors. Add tests for hash tag extraction edge cases. Add tests
for CROSSSLOT enforcement.

**Deliverable:** FerricStore boots with slot-based routing. `shard_count`
defaults to `System.schedulers_online()`. Hash tags work. CROSSSLOT errors
are enforced. No runtime resharding yet.

### Phase 2: Dynamic Shards (Estimated: 3-4 days)

**Goal:** Enable adding and removing shards at runtime.

1. Convert `ShardSupervisor` from static `Supervisor` to `DynamicSupervisor`.
2. Implement `Ferricstore.Store.ShardLifecycle` (start_shard, stop_shard).
3. Ensure `Batcher`, `AsyncApplyWorker`, and `MergeScheduler` can be
   started/stopped dynamically for any shard index.
4. Update `Application.start/2` to start initial shards via ShardLifecycle
   instead of static children.
5. Test: start with 4 shards, dynamically add shard 4-7, verify they are
   functional (can receive writes, serve reads).

**Deliverable:** Shards can be added/removed at runtime. Slot map not yet
updated -- this is a building block.

### Phase 3: Slot Migration (Estimated: 5-7 days)

**Goal:** Implement the live migration protocol from section 4.

1. Implement `Ferricstore.Store.SlotMigrator` GenServer (ETS migration).
2. Implement the ETS key iteration + insert-into-target + delete-from-source.
3. Implement the slot map atomic update after ETS migration.
4. Implement the migration source map for lazy Bitcask fallback.
5. Implement the cold-read fallback path (check new Bitcask, fallback to old,
   lazy-migrate).
6. Implement the dual-read path for the ~100ms ETS migration window.
7. Add migration state to `:persistent_term`.
8. Add telemetry events for migration progress (slots migrated, keys/second,
   time remaining).

**Testing:** Migrate a slot with concurrent reads and writes. Verify no data
loss, no stale reads, no duplicate keys. Test crash during each phase. Test
cold-read fallback and lazy migration. Test that compaction reclaims dead
records from the source shard.

**Deliverable:** Individual slots can be migrated between local shards live.

### Phase 4: Auto-Scaling Coordinator (Estimated: 3-4 days)

**Goal:** Automate the decision of when and how to reshard locally.

1. Implement `Ferricstore.Store.SchedulerMonitor` -- polls
   `System.schedulers_online()` every 30 seconds.
2. Implement rebalancing logic: compute optimal slot distribution for new
   shard count, determine which slots move where.
3. Implement `Ferricstore.Store.ReshardCoordinator` -- orchestrates multiple
   slot migrations, tracks overall progress, handles errors.
4. Add `CLUSTER REBALANCE` command for manual trigger.
5. Add `CLUSTER SLOTS` command (Redis Cluster compatible) to inspect slot
   assignments.
6. Add cooldown timer (minimum 5 minutes between auto-reshards).

**Deliverable:** FerricStore auto-scales when CPU count changes, with
monitoring commands. Each node reshards independently.

### Phase 5: Offline Migration Tool (Estimated: 2-3 days)

**Goal:** Migrate existing phash2-based data to phash2 slot-based layout.

1. Implement `mix ferricstore.migrate_slots` Mix task.
2. Scan all hint files to enumerate all keys.
3. Compute new shard assignments under phash2 + slot map.
4. Copy misplaced keys, write tombstones.
5. Write `data/slot_map.bin` marker file.
6. Add version detection in `Application.start/2`.

**Deliverable:** Existing FerricStore deployments can upgrade to slot-based
routing with a single offline migration step.

### Phase 6: Hardening (Estimated: 3-5 days)

1. Compound key migration (hash/set/zset fields must migrate atomically
   with their parent key).
2. Transaction coordinator (`MULTI/EXEC`) awareness of slot migration.
3. Blocking command (`BLPOP/BRPOP`) waiter handling during migration.
4. SCAN cursor stability during migration.
5. Stress testing: concurrent migration + writes + reads + compaction.
6. Performance benchmarking: migration throughput, steady-state overhead.

**Total estimated effort: 19-27 days.**

---

## 11. Alternatives Considered

### 11.1 Keep phash2, Support Live Resharding via Dual-Read (Option C from resharding-analysis.md)

**Summary:** Keep `phash2`, add `{current_count, previous_count}` dual routing
with lazy migration on read and a background sweeper.

**Why rejected:**

- phash2 does not support hash tags. Multi-key operations (`MGET`, `MSET`,
  pipeline of related keys) cannot be colocated.
- Changing shard count changes every key's assignment. Adding one shard (8->9)
  redistributes ~89% of keys. With slot-based indirection, adding one shard
  moves ~11% of slots (1,820 of 1,024).
- The dual-read fallback path (check new shard, then old shard) adds latency
  to every read during migration. The slot-based "migrate ETS, lazy disk"
  approach has zero hot-read overhead during migration.
- The resharding-analysis.md identified 13 required code changes including
  dual-path reads in 5 Router functions, compound key migration, SCAN
  deduplication, and transaction coordinator updates. The slot-based approach
  has similar scope but yields a fundamentally better architecture.

### 11.2 Consistent Hashing Ring (Dynamo/Cassandra-style)

**Summary:** Use a hash ring with virtual nodes (vnodes). Each shard owns
ranges on the ring. Adding a shard splits ranges.

**Why rejected:**

- More complex to implement (ring management, range queries, token assignment).
- Same end result as 1,024 fixed slots but harder to reason about.
- No industry tooling compatibility (Redis ecosystem expects slots).
- Range-based operations (SCAN by hash range) are more natural with fixed slots.

### 11.3 Stop-the-World Reindex (Current resharding-design.md)

**Summary:** Freeze writes, rehash all keys into new ETS tables, swap, unfreeze.

**Why rejected for the long term:**

- Write downtime proportional to key count (10s for 10M keys, 100s for 100M).
- Memory doubles during reindex (old + new ETS tables).
- Cannot be interrupted or resumed.
- Appropriate for small datasets (<1M keys) but not for production scale.
- The stop-the-world approach remains available as a fallback for the offline
  migration tool (section 7.1).

### 11.4 Range-Based Sharding

**Summary:** Assign key ranges (alphabetically or by hash range) to shards.
Split ranges when a shard gets too large.

**Why rejected:**

- Poor distribution for common key patterns (sequential IDs, prefixed keys).
- Requires range index lookup (O(log N)) instead of hash (O(1)).
- Hot spots when a popular key range concentrates on one shard.
- More complex to implement than fixed-slot indirection.

### 11.5 Copy-Then-Switch Disk Migration

**Summary:** During slot migration, copy all Bitcask records from source shard
to target shard, then atomically switch the slot map entry.

**Why rejected:**

- Doubles disk space temporarily (every migrated key exists in both shards).
- Slow: Bitcask sequential writes are ~200K/sec, so migrating 5M keys takes
  ~25 seconds of sustained disk I/O.
- Wastes I/O on hot keys that will never be read from disk.
- Requires a brief per-slot write freeze for the final catch-up (the original
  section 4 design had a FREEZE-AND-SWITCH phase).
- The lazy approach (section 4.3) avoids all of these problems: instant ETS
  migration, demand-driven disk migration, no extra disk space, no freeze.

---

## 12. Open Questions

### 12.1 phash2 NIF vs Pure Elixir Default

Should the default be the Rust NIF (faster, requires compilation) or pure
Elixir (slower, always available)? The NIF infrastructure already exists, and
phash2 is a trivially safe function (no memory allocation, no state, no panics).
**Tentative decision: NIF by default, pure Elixir fallback.**

### 12.2 Slot-Level vs Key-Level Migration Granularity

The design migrates entire slots (all keys in a slot move together). An
alternative is key-level migration (move individual keys, even within the
same slot). Slot-level is simpler and matches Redis Cluster semantics. Key-level
would allow finer-grained progress but adds complexity to the router (must track
per-key migration state). **Tentative decision: slot-level.**

### 12.3 Migration Source Map Lifecycle

After lazy disk migration, how do we know when to remove a shard from the
migration source map? Options:

- **Reference counting:** Track how many ETS entries still reference the source
  shard's Bitcask files. When the count reaches zero, remove the source.
- **Time-based:** Remove the source after N compaction cycles of the source
  shard (all dead records should be reclaimed by then).
- **Scan-based:** Periodically scan the target shard's ETS for entries with
  file_id/offset that reference the source. When none found, remove the source.

**Tentative decision: time-based** (simplest). Remove the source from the
migration map after 3 compaction cycles. If a cold read still hits the source
after that, the record is lost (but this should not happen if compaction is
running normally -- the ETS entry would have been updated by a lazy migration
or the key would have been overwritten/expired).

### 12.4 Slot Map Persistence Format

The slot map must survive restarts. Options:

- **Erlang term file** (`:erlang.term_to_binary` + `File.write`): simple,
  fast, but not human-readable.
- **JSON**: human-readable but larger. 1,024 integers in JSON is ~80 KB.
- **Binary**: packed 16-bit integers, 32 KB. Most compact.

**Tentative decision: Erlang term file** for simplicity. The slot map is
small (8 KB serialized) and only written during resharding.

### 12.5 CLUSTER SLOTS / CLUSTER SHARDS Command Compatibility

Should FerricStore implement the full Redis Cluster `CLUSTER SLOTS` and
`CLUSTER SHARDS` commands for compatibility with Redis Cluster-aware clients?
This would allow tools like `redis-cli --cluster` to inspect slot assignments.
**Proposed: implement a subset** (`CLUSTER SLOTS`, `CLUSTER INFO`,
`CLUSTER KEYSLOT`) for observability. Full `CLUSTER` protocol (MEET, FORGET,
REPLICATE) is deferred to multi-node work.

### 12.6 Handling Promoted Collections During Migration

Promoted collections (hashes, sets, zsets that exceeded the promotion threshold)
have their data in a dedicated Bitcask instance under
`data/dedicated/shard_N/{type}:{hash}/`. When the parent key's slot migrates to
a different shard, the dedicated instance must also migrate:

- Copy all entries from the dedicated instance to a new dedicated instance
  under the target shard's directory.
- Update the promotion marker (`PM:redis_key`) in the target shard's keydir.
- Delete the old dedicated instance.

This is a batch operation that must be atomic with the parent key migration
to avoid partial state. **Proposed: migrate promoted collections as a single
unit, holding a brief lock on the parent key during the copy.**

Note: promoted collection migration is a disk-level operation (copying Bitcask
files between dedicated directories). Unlike regular key migration, this cannot
be lazy because promoted collections have their own Bitcask instances. The
dedicated directory must be copied in full before the slot map switches.

### 12.7 Interaction with Raft State Machine

The current Raft state machine (`StateMachine`) is per-shard. Write commands
are applied deterministically within a shard's Raft group.

Since resharding is a local per-node operation, and Raft replication ensures
all nodes have the same data independent of shard layout, the slot migration
does NOT need to go through Raft. The migrator manipulates ETS and Bitcask
directly on each node.

However, on restart, the shard must rebuild ETS from Bitcask. If the node
crashed after a slot map update but before lazy disk migration completed,
the shard needs to know which source shard directories to scan. This is
handled by persisting the migration source map alongside the slot map in
`data/slot_map.bin`.

### 12.8 ETS Rebuild on Restart with Pending Lazy Migrations

When a node restarts with pending lazy migrations (migration source map is
non-empty), the ETS rebuild must account for keys whose Bitcask records are
in a different shard's directory:

```
Shard B rebuilds ETS:
  1. Scan shard B's own Bitcask files -> insert into ETS
  2. Check migration_sources[B] -> [A]
  3. Scan shard A's Bitcask files for keys in slots owned by shard B
     -> insert into ETS (these are the not-yet-lazy-migrated keys)
```

This adds startup time proportional to the number of pending migrations.
For a node that crashed mid-resharding with many pending lazy migrations,
this could be significant. **Mitigation:** the background lazy-migrator
should proactively migrate cold keys (not just wait for reads) to minimize
the pending lazy migration set.

---

## 13. Appendix: Redis Cluster phash2 Test Vectors

For validation of the phash2 implementation:

```
phash2("") = 0x0000
phash2("123456789") = 0x31C3
phash2("hello") = 0x34E0
phash2("foobar") = 0x86E6

# Slot assignments (phash2 % 1024):
"foo"          -> slot 12182
"bar"          -> slot 5061
"hello"        -> slot 866
"{user:42}"    -> slot 12758
"{user:42}:x"  -> slot 12758  (hash tag: "user:42")
"a{b}c"        -> slot 3300   (hash tag: "b")
"a{b"          -> slot 12191  (no closing brace, full key hashed)
"a{}b"         -> slot 8619   (empty braces, full key hashed)
```

---

## 14. Appendix: Modules Changed

Summary of all modules that reference shard count, shard names, or shard
routing, and the nature of the change required.

| Module | Current Usage | Change Required |
|--------|--------------|-----------------|
| `Store.Router` | `@shard_count` compile-time, `phash2`, `@shard_names` tuple | New slot-based routing, hash tags, CROSSSLOT, runtime shard count |
| `Store.Shard` | `Router.shard_name(index)` for registration | No change (shard_name works for any index) |
| `Store.ShardSupervisor` | Static `Supervisor`, fixed N children | Convert to `DynamicSupervisor` |
| `Store.Promotion` | Uses shard index for dedicated paths | No change (shard index still exists) |
| `Application` | Starts N batchers, N async workers statically | Start via ShardLifecycle |
| `Raft.Cluster` | `start_shard_server/5` per index | No change (already supports any index) |
| `Raft.Batcher` | `batcher_name(index)` registration | No change (already supports any index) |
| `Raft.AsyncApplyWorker` | `worker_name(index)` registration | No change |
| `Raft.StateMachine` | Per-shard, no shard count awareness | No change |
| `Merge.Supervisor` | Creates N schedulers at init | Support dynamic scheduler creation |
| `MemoryGuard` | Iterates N shards for stats | Use runtime shard count |
| `Health` | `@shard_count` compile-time | Use runtime shard count |
| `Commands.Cluster` | `@shard_count` compile-time | Use runtime shard count |
| `Commands.Server` | Iterates N shards for INFO, KEYS, FLUSHDB | Use runtime shard count |
| `Commands.Strings` | MGET, MSET | Add CROSSSLOT check |
| `Commands.Sets` | SDIFF, SINTER, SUNION | Add CROSSSLOT check |
| `Commands.Generic` | RENAME, COPY | Add CROSSSLOT check |
| `Commands.Lists` | LMOVE, RPOPLPUSH | Add CROSSSLOT check |
| `Commands.HyperLogLog` | PFMERGE | Add CROSSSLOT check |
| `Transaction.Coordinator` | MULTI/EXEC key classification | Add CROSSSLOT check |
| `Metrics` | Iterates N shards for keydir stats | Use runtime shard count |
| `Sandbox` | Clears N keydirs | Use runtime shard count |
| `DataDir` | Creates N shard directories | Called with new count on scale-up |
| `connection.ex` (server app) | `Router.shard_for/1` for dispatch | Works as-is |

**New modules:**

| Module | Purpose |
|--------|---------|
| `Store.phash2` | Pure Elixir phash2-CCITT implementation |
| `Store.HashTag` | Hash tag extraction from keys |
| `Store.SlotMap` | Slot map management (persistent_term) |
| `Store.SlotMigrator` | ETS migration + slot map update per slot batch |
| `Store.ReshardCoordinator` | Orchestrates multi-slot rebalancing |
| `Store.ShardLifecycle` | Start/stop all processes for a shard |
| `Store.SchedulerMonitor` | Detects CPU count changes |
| `:erlang.phash2/2` | Built-in BEAM hash (no NIF needed) |
