# Cluster Design: Multi-Node Replication

## Overview

FerricStore cluster replication: every node holds a full copy of all data. All shards are replicated across all nodes via Raft consensus. Any node can serve any key. Writes go to the Raft leader; reads serve from local ETS (eventually consistent on followers, strongly consistent on leader).

This is **replication**, not **partitioning** — every node has every shard. The shard count controls parallelism within a node, not data distribution across nodes.

```
3-node cluster, 4 shards:

Node A                    Node B                    Node C
┌────────────────┐        ┌────────────────┐        ┌────────────────┐
│ Shard 0 (L)    │◄──────►│ Shard 0 (F)    │◄──────►│ Shard 0 (F)    │
│ Shard 1 (L)    │◄──────►│ Shard 1 (F)    │◄──────►│ Shard 1 (F)    │
│ Shard 2 (L)    │◄──────►│ Shard 2 (F)    │◄──────►│ Shard 2 (F)    │
│ Shard 3 (L)    │◄──────►│ Shard 3 (F)    │◄──────►│ Shard 3 (F)    │
└────────────────┘        └────────────────┘        └────────────────┘
         (L) = Raft leader    (F) = Raft follower
```

---

## Node Discovery

**libcluster** (already integrated) handles node discovery and Erlang distribution connection. Supported strategies:

| Strategy | Use case |
|---|---|
| `Cluster.Strategy.Gossip` | Dev/local — multicast UDP |
| `Cluster.Strategy.Kubernetes.DNS` | K8s headless service |
| `Cluster.Strategy.Epmd` | Static node list |
| `Cluster.Strategy.DNSPoll` | DNS A-record polling |

libcluster connects Erlang nodes via `Node.connect/1`. Once connected, `Node.list()` returns all peers.

---

## ClusterManager GenServer

A new `Ferricstore.Cluster.Manager` GenServer on each node. Responsibilities:

1. Monitor `:nodeup`/`:nodedown` events
2. Coordinate Raft membership changes
3. Orchestrate data sync for new followers
4. Expose CLI commands: `CLUSTER.JOIN`, `CLUSTER.LEAVE`, `CLUSTER.STATUS`

### State

```elixir
%{
  mode: :standalone | :cluster,
  known_nodes: MapSet.t(),        # nodes we've seen (survives transient disconnects)
  syncing_nodes: %{node => status}, # nodes currently receiving data sync
  remove_timers: %{node => ref},    # delayed removal timers (crash vs partition)
}
```

### Lifecycle

```
                   Application.start
                         │
                         ▼
              ┌─────────────────────┐
              │  ClusterManager.init │
              │                     │
              │  cluster_nodes = [] │──► mode: :standalone
              │  cluster_nodes ≠ [] │──► mode: :cluster
              └─────────┬───────────┘
                        │
              ┌─────────▼───────────┐
              │  :net_kernel.monitor │
              │  subscribe nodeup/  │
              │  nodedown events    │
              └─────────┬───────────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
    :nodeup          :nodedown     periodic health
    (join flow)     (leave flow)    (30s check)
```

---

## Node Join Flow

### Case 1: Bootstrap — first node starts the cluster

```
Node A starts with FERRICSTORE_CLUSTER_NODES=A,B,C

1. No other nodes reachable yet
2. start_shard_server(i) with initial_members: [{shard_i, A}]
   → Single-node Raft groups (current behavior)
3. ClusterManager enters :waiting_for_peers state
4. libcluster connects B and C as they come up
5. → triggers :nodeup flow (Case 2)
```

**How does A know it's first?** It tries to query `ra:members` on any connected node. If no Raft groups exist anywhere, it's the bootstrap node.

### Case 2: New node joins existing cluster

```
Node B starts, libcluster connects it to A

On Node A (ClusterManager receives :nodeup for B):
  1. Check: does B have FerricStore running?
     → GenServer.call({ClusterManager, B}, :ping)
  2. Check: is B already a member of our Raft groups?
     → ra:members(shard_server_id(i)) — if B is listed, skip
  3. For each shard i:
     a. ra:add_member(shard_server_id(i), {shard_i, B})
        → ra proposes membership change via Raft consensus
        → majority must agree (A is leader + sole voter initially)
     b. ra automatically sends snapshot to B's ra server
        → snapshot contains state machine state (applied_count, etc.)
        → BUT NOT the ETS keydir or Bitcask files
  4. Trigger data sync (see "Data Sync" section below)

On Node B (ClusterManager receives :nodeup for A):
  1. Check: am I already in a cluster?
     → If shards not started yet, wait for A to add us
  2. start_shard_server(i) with initial_members matching A's group
     → ra server starts in :recover state, receives snapshot from leader
  3. After snapshot applied, B's state machine rebuilds ETS keydir
  4. B's shards start serving reads (from local ETS)
  5. B's writes forward to leader (A) via ra:pipeline_command
```

### Race condition: both A and B start simultaneously

Both call `ra:start_server` with `initial_members: [A, B, C]`. ra handles this:
- All three start as followers
- Election timeout triggers — one wins the election
- Winner becomes leader, others become followers
- This works because `initial_members` is identical on all nodes

This is the **preferred bootstrap path** — all nodes start together with the same `initial_members` list from config.

---

## Node Leave Flow

### Case 1: Graceful leave (operator-initiated)

```
Operator runs: CLUSTER.LEAVE on Node C

On Node C:
  1. ClusterManager broadcasts leave intent to all peers
  2. For each shard i where C is a member:
     a. If C is leader: trigger leadership transfer first
        → ra:transfer_leadership(shard_server_id(i), preferred_target)
        → wait for new leader elected
     b. ra:remove_member(shard_server_id(i), {shard_i, C})
  3. Stop shard GenServers
  4. Disconnect from cluster (Node.disconnect)

On remaining nodes:
  1. Receive leave notification
  2. Verify ra membership updated (C removed from each group)
  3. Quorum recalculates: 3→2 nodes, majority = 2
```

### Case 2: Node crash (ungraceful)

```
Node C crashes (process killed, hardware failure, network drop)

On Node A and B (receive :nodedown for C):
  1. ClusterManager starts a removal timer (default: 60s)
     → Don't remove immediately — could be transient network issue
  2. ra detects C is unresponsive
     → If C was leader: A or B triggers election, one becomes leader
     → If C was follower: no impact on writes (quorum still met with A+B)
  3. Timer expires without C reconnecting:
     a. ra:remove_member(shard_server_id(i), {shard_i, C}) for each shard
     b. Cluster is now 2-node (majority = 2, no fault tolerance)
     c. Log warning: "cluster degraded, add a replacement node"

If C reconnects before timer:
  1. Cancel removal timer
  2. Run the universal sync check (see below) per shard
  3. If WAL can bridge → ra replays automatically, C catches up
  4. If WAL gap → shard resync (same as new node join for that shard)
```

### Case 3: Network partition

```
Network splits: [A, B] | [C]

Majority side [A, B]:
  - Leader (if on this side) continues serving writes
  - If leader was on C's side, A or B elects a new leader
  - Writes succeed (2/3 = majority quorum met)

Minority side [C]:
  - C cannot commit writes (1/3 ≠ majority)
  - C serves stale reads from local ETS (if READMODE STALE)
  - C rejects writes with: "ERR CLUSTERDOWN no quorum"

Network heals:
  - Same as reconnect: run universal sync check per shard
  - WAL bridgeable → ra replays, C catches up
  - WAL gap → shard resync for affected shards only
```

---

## Universal Sync Decision

Every scenario — new node, reconnect, partition heal, S3 bootstrap — uses the
same per-shard check. This is the single decision point for "can this node catch
up, or does it need a fresh copy?"

```
For each shard i on the joining/reconnecting node:

  local_index = last Raft index this node applied for shard i
                (0 if empty/new node, or read from ra state on disk)

  Ask leader: {first_index, last_index} = ra:log_stats(shard_server_id(i))

  if local_index >= first_index:
    ┌─────────────────────────────────────────┐
    │  WAL BRIDGEABLE                         │
    │  ra can replay entries local→last        │
    │  → Just join Raft group                 │
    │  → ra handles replay automatically      │
    │  → No data copy needed                  │
    └─────────────────────────────────────────┘

  if local_index < first_index:
    ┌─────────────────────────────────────────┐
    │  WAL GAP — need shard resync            │
    │  Leader's WAL starts at first_index     │
    │  Node's data ends at local_index        │
    │  Gap: local_index..first_index is lost  │
    │                                         │
    │  → Pause writes on leader's shard i     │
    │  → Copy shard i data dir to this node   │
    │  → Resume writes                        │
    │  → Node rebuilds keydir from disk       │
    │  → Joins Raft group, replays delta      │
    └─────────────────────────────────────────┘
```

### All scenarios mapped to this check

| Scenario | local_index | Likely outcome |
|---|---|---|
| New node (empty disk) | 0 | WAL gap → full shard copy |
| Reconnect after brief crash (seconds) | recent | WAL bridgeable → ra replays |
| Reconnect after long downtime (days) | old | WAL gap → shard resync |
| S3 snapshot (1 hour old) | snapshot_index | Depends on WAL retention |
| S3 snapshot (1 week old) | very old | WAL gap → shard resync (S3 was pointless) |
| Partition heals (minutes) | slightly behind | WAL bridgeable → ra replays |
| Partition heals (hours) | behind | Depends on WAL retention |

### Per-shard independence

Each shard is checked independently. A reconnecting node might have:
- Shard 0: local_index 8000, leader first_index 7500 → WAL bridgeable ✓
- Shard 1: local_index 3000, leader first_index 6000 → WAL gap, resync
- Shard 2: local_index 9000, leader first_index 8800 → WAL bridgeable ✓
- Shard 3: local_index 4000, leader first_index 5500 → WAL gap, resync

Only shards 1 and 3 need a data copy. Shards 0 and 2 catch up via WAL replay.
This minimizes the amount of data transferred and the write-pause window.

---

## Data Sync for New Followers

When a new node joins with an empty data dir, it needs a full copy of each shard's
data. The principle: **copy the directory, rebuild the keydir from disk** — the exact
same code path as a normal node restart. No special snapshot format needed.

Two sync sources, chosen based on situation:

| Source | When to use | Speed | Leader impact |
|---|---|---|---|
| **Direct copy from leader** | Live cluster, fast network | Fast (LAN speed) | Brief per-shard write pause |
| **Object storage snapshot** | Cold start, disaster recovery, slow network | Varies (S3 throughput) | Zero — reads from a periodic upload |

### Strategy 1: Direct shard-by-shard copy

Shards sync one at a time. While shard N is syncing, all other shards continue
serving reads and writes normally.

```
For each shard i (sequentially):

Step 1: Pause writes on shard i
  └─ Leader's ClusterManager calls GenServer.call(shard_i, :pause_writes)
  └─ Shard stops accepting new writes, flushes pending to disk
  └─ Raft commands queue in ra (not applied until resumed)
  └─ Duration: milliseconds (just flush + fsync)

Step 2: Record Raft sync point
  └─ Read current ra log index: {:ok, {idx, _term}} = ra:member_state(shard_id)
  └─ This is the "consistent-as-of" point

Step 3: Copy shard data directory to new node
  └─ Files to copy:
     ├── data/shard_i/*.log          (Bitcask data files)
     ├── data/shard_i/*.hint         (hint files for fast recovery)
     ├── data/shard_i/promoted/      (dedicated collection stores)
     ├── data/shard_i/prob/          (bloom, CMS, cuckoo, topk files)
     └── ra/shard_i/                 (Raft WAL + snapshots)
  └─ Transfer via Erlang distribution:
     :erpc.call(new_node, File, :write!, [dest_path, binary])
  └─ Or tar + stream for large dirs

Step 4: Resume writes on shard i
  └─ GenServer.call(shard_i, :resume_writes)
  └─ Queued Raft commands start applying
  └─ Other shards were never paused — zero impact

Step 5: New node starts shard i
  └─ Shard.start_link(index: i, data_dir: copied_dir, ...)
  └─ recover_keydir scans .log + .hint files → rebuilds ETS keydir
  └─ ra server starts, joins Raft group via ra:add_member
  └─ ra replays WAL entries from sync_point to current
  └─ Each replayed entry applies to ETS (catches up writes during copy)
  └─ Shard i is fully caught up and serving

Step 6: Move to shard i+1, repeat
```

**Timeline for 4 shards, 1GB each:**
```
t=0s    Pause shard 0, copy 1GB (~2s on 1Gbps LAN), resume
t=3s    Pause shard 1, copy 1GB, resume
t=6s    Pause shard 2, copy 1GB, resume
t=9s    Pause shard 3, copy 1GB, resume
t=12s   All shards synced, new node fully operational

Write downtime per shard: ~100ms (flush + fsync)
Total time: ~12s for 4GB dataset
Other shards serve traffic throughout
```

### Strategy 2: Object storage snapshots

The leader periodically uploads a consistent snapshot of each shard's data directory
to object storage (S3, GCS, MinIO). New nodes bootstrap from these snapshots instead
of copying from the leader.

```
Leader (periodic, every ~1 hour):

For each shard i:
  1. Pause writes briefly (same as direct copy)
  2. Record Raft log index as snapshot_index
  3. tar + compress shard_i data directory
  4. Upload to: s3://bucket/ferricstore/shard_i/snapshot_{timestamp}.tar.gz
  5. Write manifest: s3://bucket/ferricstore/manifest.json
     {
       "timestamp": "2026-04-06T12:00:00Z",
       "raft_index_per_shard": {0: 45892, 1: 44100, 2: 46000, 3: 43500},
       "shards": [
         {"index": 0, "path": "shard_0/snapshot_20260406T120000.tar.gz", "size_bytes": 1073741824},
         ...
       ]
     }
  6. Resume writes
  7. Clean up old snapshots (keep last N)
```

```
New node joining:

  1. Download manifest from object storage
  2. For each shard i (in parallel — no leader impact):
     a. Download snapshot tar.gz
     b. Extract to local data/shard_i/
     c. Download Raft WAL snapshot
  3. Start all shards:
     a. recover_keydir from disk files (same code path as restart)
     b. Join Raft groups — ra replays WAL entries since snapshot_index
     c. Catches up the ~1 hour delta of writes
  4. Node is fully operational

Advantages:
  - Zero impact on leader during new node bootstrap
  - Parallel download of all shards (vs sequential in direct copy)
  - Disaster recovery: spin up entire cluster from object storage
  - Works across regions / slow networks
```

### Object storage configuration

```elixir
# config/prod.exs
config :ferricstore, :cluster_snapshots,
  enabled: true,
  backend: :s3,                              # :s3 | :gcs | :local
  bucket: "ferricstore-snapshots",
  prefix: "cluster-prod",
  interval_ms: 3_600_000,                    # 1 hour
  retention_count: 24,                        # keep last 24 snapshots
  aws_region: "us-east-1"                     # for S3
```

### Which strategy is chosen?

Uses the universal sync decision (above). Object storage is an optimization —
it provides a head start so the node has *some* data before running the check.

```
New node starts → ClusterManager:

1. Is object storage configured AND has a snapshot?
   → YES: download and extract all shard dirs from S3
   → Each shard now has a local_index = snapshot_raft_index
   → Fall through to step 2

2. Are other cluster nodes reachable?
   → YES: run universal sync check per shard:
     → If local_index >= leader's first_index → WAL bridgeable (S3 saved us a full copy!)
     → If local_index < leader's first_index → WAL gap → direct copy for that shard
   → NO: this is the first node, bootstrap as standalone
```

**Object storage value:** Even if the S3 snapshot is slightly stale, it saves
copying most of the data. If 90% of the data hasn't changed since the snapshot,
only the delta needs WAL replay. If the snapshot IS too stale (WAL gap), the
universal sync check catches it and falls through to direct copy — no harm done
except wasted download time.

**Preventing staleness:** The snapshot uploader should run frequently enough that
the WAL never truncates past the snapshot point. Rule of thumb:
- ra keeps WAL segments based on `wal_max_size_bytes` config
- Upload interval (1 hour) should be shorter than the WAL retention window
- Monitor: if `snapshot_raft_index < leader first_index`, upload more frequently

### Why this design is clean

1. **Same recovery path everywhere** — whether data comes from direct copy, object
   storage, or a normal restart, the node always does `recover_keydir` from disk
   files. One code path, well tested.

2. **No special snapshot format** — just tar the data directory. No ETS
   serialization, no custom binary format. `ls data/shard_0/` on any node shows
   the same files.

3. **No ra snapshot complexity** — we don't need to implement `ra_snapshot` callbacks
   for ETS. ra handles its own WAL/snapshot lifecycle. We handle ours (Bitcask files).

4. **Incremental catch-up** — after the bulk copy (either source), Raft WAL replay
   handles the delta. Writes that happened during the copy are automatically applied.

---

## Edge Cases

### 1. Copy fails mid-transfer

Shard 2 copy interrupted by network drop. Shards 0 and 1 already synced and in
Raft group.

**Handling:** New node tracks sync status per shard (`%{0 => :synced, 1 => :synced,
2 => :failed, 3 => :pending}`). On failure, delete the partial shard dir and retry
that shard only. Already-synced shards continue receiving Raft commands normally.

### 2. Leader changes during sync

While copying shard 2 from Node A, Node A crashes. Node B becomes leader.

**Handling:** Before each shard copy, ClusterManager resolves the current leader
via `ra:members(shard_server_id(i))`. Don't assume one node is leader for all
shards. If the leader changes mid-copy for a shard, abort that shard's copy and
retry with the new leader.

### 3. New node crashes during sync

3 of 4 shards synced, new node crashes before finishing shard 3.

**Handling:** On restart, new node checks each shard's data dir for completeness
(valid .log files, hint files present). Already-synced shards rejoin their Raft
groups via WAL replay — no re-copy needed. Only missing/incomplete shards re-sync.

### 4. Compaction runs during pause

Writes are paused on shard 0, but `Merge.Supervisor` triggers compaction which
rewrites .log files while the copy is in progress.

**Handling:** `:pause_writes` must also block compaction. The shard sets a
`sync_in_progress` flag that `Merge.Supervisor` checks before starting a merge
job on that shard. Resume clears the flag.

### 5. File rotation during copy

Active file reaches max size and rotates, creating a new .log file not in the
original file list.

**Handling:** Not possible — writes are paused during copy. The flush in step 1
forces pending data to disk. No new writes means no file rotation. The file list
at copy start is the complete and final set.

### 6. Object storage upload interrupted

Leader uploads shard 0 and 1 snapshots, then crashes before writing the manifest.

**Handling:** Write per-shard snapshot files first, manifest last. The manifest is
the atomic commit point — it either lists all shards or doesn't exist. Downloader
checks manifest integrity (all shards present, checksums valid) before proceeding.
Incomplete uploads without a manifest are cleaned up on the next upload cycle.

### 7. Two nodes try to join simultaneously

Node B and Node C both start and try to join Node A at the same time.

**Handling:** ClusterManager on the leader serializes sync requests per shard. Only
one shard-copy operation runs at a time for a given shard. The second joiner waits
in a queue. Both nodes can join the Raft group immediately (`ra:add_member`), but
data sync is serialized. Alternatively, once B finishes syncing, C could copy from
B instead of A — spreading the load.

### 8. WAL grows during long sync

100GB dataset takes 10 minutes to copy. Writes accumulate in WAL during that time.

**Handling:** Not a real concern. WAL entries are small command tuples. Even at
100k writes/sec for 10 minutes = 60M entries ≈ a few GB of WAL. ra handles this
efficiently. The new node replays them quickly since each entry is just an ETS
insert (~1μs per entry, 60M entries ≈ 60s of replay).

---

## Read/Write Routing in Cluster Mode

### Writes

All writes go through Raft — `ra:pipeline_command` automatically forwards to the leader. No routing change needed. If the local node IS the leader, the write is applied locally. If not, ra forwards it.

```
Client → Router.put(ctx, key, val, exp)
  → Batcher.write(shard_idx, {:put, key, val, exp})
    → ra:pipeline_command(shard_server_id(idx), command)
      → if local is leader: apply locally
      → if local is follower: forward to leader node
      → leader applies, replicates to followers
      → followers apply to their local ETS
```

### Reads

Always local. Every node reads from its own ETS — this is the entire point of
replication. No forwarding to leader, no network hop. Raft replication keeps
ETS in sync across nodes (typical lag: 1-10ms).

```
Client → Router.get(ctx, key)
  → ets_get_full(keydir, key, now)   ← local ETS, no network hop
  → value or nil
```

No code changes needed for reads. The existing `Router.get` path works
identically on leaders and followers.

---

## Configuration

### Environment variables

```bash
# Required for cluster mode
FERRICSTORE_NODE_NAME=ferric1@10.0.0.1     # Erlang node name
FERRICSTORE_COOKIE=my_secret_cookie          # Erlang cookie

# Optional: static node list (alternative to libcluster discovery)
FERRICSTORE_CLUSTER_NODES=ferric1@10.0.0.1,ferric2@10.0.0.2,ferric3@10.0.0.3

# Optional: tune cluster behavior
FERRICSTORE_CLUSTER_REMOVE_DELAY_MS=60000   # delay before removing crashed node
FERRICSTORE_CLUSTER_SYNC_TIMEOUT_MS=300000  # max time for data sync
```

### libcluster config (config/prod.exs)

```elixir
# Kubernetes
config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Kubernetes.DNS,
      config: [
        service: "ferricstore-headless",
        application_name: "ferricstore"
      ]
    ]
  ]

# Static list
config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Epmd,
      config: [hosts: [:"ferric1@10.0.0.1", :"ferric2@10.0.0.2"]]
    ]
  ]
```

### Redis CLI commands

```
CLUSTER.STATUS       → show all nodes, roles (leader/follower per shard), lag
CLUSTER.JOIN <node>  → manually join a node to the cluster
CLUSTER.LEAVE        → gracefully leave the cluster
CLUSTER.HEALTH       → per-shard leader/follower/member info
CLUSTER.FAILOVER     → trigger manual leadership transfer
```

---

## Files to Create/Modify

| File | Change |
|---|---|
| `lib/ferricstore/cluster/manager.ex` | **NEW** — ClusterManager GenServer: nodeup/nodedown, join/leave orchestration |
| `lib/ferricstore/cluster/data_sync.ex` | **NEW** — shard-by-shard directory copy via Erlang distribution |
| `lib/ferricstore/cluster/snapshot_uploader.ex` | **NEW** — periodic upload of shard dirs to object storage |
| `lib/ferricstore/cluster/snapshot_downloader.ex` | **NEW** — download + extract snapshots from object storage |
| `lib/ferricstore/raft/cluster.ex` | Modify `start_shard_server` to use multi-node initial_members |
| `lib/ferricstore/store/shard.ex` | Add `:pause_writes` / `:resume_writes` handle_call |
| `lib/ferricstore/store/router.ex` | No changes needed — reads already local, writes already go through Raft |
| `lib/ferricstore/application.ex` | Add ClusterManager to supervision tree |
| `lib/ferricstore/commands/cluster.ex` | Add CLUSTER.JOIN, CLUSTER.LEAVE, CLUSTER.FAILOVER, CLUSTER.STATUS |
| `config/runtime.exs` | Cluster env var config (already restored) |
| `config/prod.exs` | Object storage snapshot config |

---

## Failure Modes

| Scenario | Impact | Recovery |
|---|---|---|
| 1 of 3 nodes crashes | Writes continue (2/3 quorum). Reads unaffected. | Node rejoins, ra syncs WAL. |
| 2 of 3 nodes crash | Writes fail (1/3 no quorum). Reads serve stale. | Restart crashed nodes. |
| Network partition (2\|1) | Majority side continues. Minority rejects writes. | Partition heals, ra reconciles. |
| Leader crashes | Followers elect new leader (~100ms). Brief write pause. | Old leader rejoins as follower. |
| New node joins (direct) | Brief per-shard write pause (~100ms each). | Sync completes shard-by-shard. |
| New node joins (S3) | Zero leader impact. | Download + Raft WAL catch-up. |
| Node disk full | Raft WAL writes fail on that node. Others continue. | Free disk space, node re-syncs. |
| Object storage unavailable | Snapshot uploads fail (logged). | Direct copy still works. Retry on next cycle. |

---

## Implementation Phases

### Phase 1: Multi-node Raft groups
- Wire `cluster_nodes` config into `start_shard_server` initial_members
- Start distributed Erlang in application.ex when configured
- Basic ClusterManager: `:nodeup`/`:nodedown` monitoring

### Phase 2: Shard-by-shard direct sync
- Add `:pause_writes`/`:resume_writes` to Shard GenServer
- Implement `DataSync`: copy shard dirs via Erlang distribution
- Integrate with ClusterManager join flow
- Integration test with `:peer` module (3-node cluster)

### Phase 3: CLUSTER commands
- CLUSTER.STATUS — nodes, per-shard leader/follower, lag
- CLUSTER.JOIN — manual join trigger
- CLUSTER.LEAVE — graceful leave with leadership transfer
- CLUSTER.FAILOVER — manual leadership transfer
- CLUSTER.HEALTH — detailed per-shard health

### Phase 4: Object storage snapshots
- Periodic snapshot uploader (S3/GCS/MinIO via ExAws or similar)
- Snapshot downloader for cold bootstrap
- Manifest management (retention, cleanup)
- Prefer object storage when available for new node join

---

## Design Decisions

1. **Reads are always local** — every node reads from its own ETS. This is the entire point of replication. No "consistent read" mode — that would just be a proxy.
2. **Minimum 3 nodes** — Raft requires a majority for writes. With 3 nodes, 1 can fail and writes continue (2/3 quorum). With 2 nodes, any failure blocks writes. Every serious distributed system enforces this: Redis Sentinel, etcd, CockroachDB, RabbitMQ quorum queues. ClusterManager rejects cluster formation with fewer than 3 nodes.

## Design Decisions (continued)

3. **Let ra elect leaders naturally** — no forced leadership distribution. ra's random election timeouts spread leaders across nodes well enough. Forced rebalancing fights Raft's election logic and adds complexity for minimal gain. Can add `CLUSTER.REBALANCE` later if needed.

4. **Pluggable object storage** — define a `Ferricstore.Cluster.SnapshotStore` behaviour with `upload/3`, `download/2`, `list/1`, `delete/1`. Ship S3 adapter as default. Others implement their own (GCS, Azure, MinIO, local filesystem). Same pattern as Ecto adapters.

```elixir
defmodule Ferricstore.Cluster.SnapshotStore do
  @callback upload(path :: binary(), key :: binary(), opts :: keyword()) :: :ok | {:error, term()}
  @callback download(key :: binary(), dest_path :: binary()) :: :ok | {:error, term()}
  @callback list(prefix :: binary()) :: {:ok, [binary()]} | {:error, term()}
  @callback delete(key :: binary()) :: :ok | {:error, term()}
end
```

```elixir
# config/prod.exs
config :ferricstore, :snapshot_store,
  adapter: Ferricstore.Cluster.SnapshotStore.S3,
  bucket: "ferricstore-snapshots",
  prefix: "cluster-prod"
```

## Open Questions

1. **Sync parallelism** — Copy shards sequentially (simpler, predictable) or parallel (faster, more leader load)?
