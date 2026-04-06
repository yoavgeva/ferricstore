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

## Read Replicas (Non-Voting Members)

ra supports `promotable` membership — a server that receives all log replication
but doesn't vote in elections or count toward quorum. Perfect for read replicas.

```
Quorum (3 voters):              Read replicas (N, non-voters):
┌──────────┐                    ┌──────────┐
│ Node A   │◄── Raft ──────────►│ Replica 1│  receives replication
│ (voter)  │                    │ (promotable)  reads from local ETS
├──────────┤                    ├──────────┤
│ Node B   │◄── Raft ──────────►│ Replica 2│  writes forward to leader
│ (voter)  │                    │ (promotable)
├──────────┤                    └──────────┘
│ Node C   │
│ (voter)  │    Replicas can go down without affecting quorum.
└──────────┘    Add replicas in other regions for low-latency reads.
```

### How it works (mostly ra config)

ra handles replication to non-voters automatically. The code changes are minimal:

```elixir
# 1. Server config: start shard with promotable membership on replica nodes
server_config = %{
  id: server_id,
  ...
  membership: :promotable   # instead of :voter (default)
}

# 2. Add replica to existing cluster:
:ra.add_member(leader_id, %{id: {shard_name, replica_node}, membership: :promotable})

# 3. ra replicates all entries to the replica — ETS stays current
# 4. Replica serves reads from local ETS — no code change needed
```

### What needs code

| Change | Why |
|---|---|
| Pass `membership: :promotable` in ra server config when `role == :replica` | ra needs to know this node doesn't vote |
| ClusterManager awareness | Use correct `add_member` call based on role |
| CLUSTER.STATUS | Show voter vs replica per node |

That's it. No write forwarding code needed — `ra:pipeline_command` from a
non-voter automatically forwards to the leader. Both quorum and async writes
work with zero code changes:

- **Quorum write on replica:** `ra:pipeline_command` → ra forwards to leader →
  leader gets majority ack from voters → applied → replicated back to replica
- **Async write on replica:** write locally to ETS + Bitcask (fast) →
  `AsyncApplyWorker.replicate` submits to Raft → leader receives → replicates
  to all nodes

The replica just can't be elected leader and doesn't count toward quorum.

### Configuration

```bash
FERRICSTORE_CLUSTER_ROLE=voter        # default — participates in quorum
FERRICSTORE_CLUSTER_ROLE=replica      # promotable — can become voter via CLUSTER.PROMOTE
FERRICSTORE_CLUSTER_ROLE=readonly     # non_voter — permanent read-only, never promoted
```

| Role | ra membership | Votes? | Replication? | Promotable? | Use case |
|---|---|---|---|---|---|
| `voter` | `:voter` | Yes | Yes | Already voter | Quorum members (min 3) |
| `replica` | `:promotable` | No | Yes | Yes | Same-region read replica, can replace failed voter |
| `readonly` | `:non_voter` | No | Yes | No | Cross-region read replica, latency too high for quorum |

### Promotion and demotion

```
CLUSTER.PROMOTE <node>   → replica (promotable) → voter
                           readonly (non_voter) → rejected (cannot promote)

CLUSTER.DEMOTE <node>    → voter → replica (promotable)
                           useful for planned maintenance
```

ra handles promotion/demotion via Raft membership change commands — the cluster
agrees on the change through consensus, then the member's role updates atomically.

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

Every scenario — new node, reconnect, partition heal, disk snapshot bootstrap — uses the
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
| Cloud disk snapshot (1 hour old) | snapshot_index | Depends on WAL retention |
| Cloud disk snapshot (1 week old) | very old | WAL gap → shard resync (snapshot was pointless) |
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

### Direct shard-by-shard copy

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

### Alternative for large datasets: cloud disk snapshots

For shards larger than a few GB, the direct copy pause can be significant (~2s per GB
on NVMe). For production deployments with large datasets, we recommend using cloud
disk snapshots instead of FerricStore's built-in direct copy:

**AWS EBS snapshots:**
```bash
# On the leader node:
aws ec2 create-snapshot --volume-id vol-xxx --description "ferricstore-snap"
# Returns snap-123 instantly (copy-on-write at block level)

# For the new node:
aws ec2 create-volume --snapshot-id snap-123 --availability-zone us-east-1a
# Mount the new volume → start FerricStore → recover_keydir → join Raft
```

**GCP persistent disk snapshots:**
```bash
gcloud compute disks snapshot ferricstore-disk --snapshot-names=ferricstore-snap
gcloud compute disks create new-disk --source-snapshot=ferricstore-snap
```

The new node boots with a byte-for-byte clone of the leader's disk. `recover_keydir`
rebuilds ETS from the Bitcask files (same code path as a normal restart). Then the
node joins the Raft group and ra replays the delta since the snapshot point.

**Advantages over file-level copy:**
- Zero pause on the leader (cloud provider handles CoW at block level)
- No extra disk space (snapshot is a block-level diff)
- No network transfer between nodes (snapshot lives in cloud storage)
- Works for any dataset size (100GB+ is instant)

This is the recommended approach for production. FerricStore's built-in direct copy
is best for development, testing, and small datasets.

### Why this design is clean

1. **Same recovery path everywhere** — whether data comes from direct copy or
   a cloud disk snapshot, the node always does `recover_keydir` from disk files.
   One code path, well tested.

2. **No special snapshot format** — just copy the data directory. No ETS
   serialization, no custom binary format. `ls data/shard_0/` on any node shows
   the same files.

3. **No ra snapshot complexity** — ra's release_cursor snapshot only contains
   state machine metadata (atom names, counters), NOT the actual key-value data.
   All user data lives in Bitcask files and ETS. The Bitcask file copy is a
   correctness requirement, not an optimization.

4. **Incremental catch-up** — after the bulk copy, Raft WAL replay handles the
   delta. The state machine's `skip_below_index` skips entries already present
   in the copied data, avoiding redundant ETS/Bitcask writes.

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

### 6. Two nodes try to join simultaneously

Node B and Node C both start and try to join Node A at the same time.

**Handling:** ClusterManager on the leader serializes sync requests per shard. Only
one shard-copy operation runs at a time for a given shard. The second joiner waits
in a queue. Both nodes can join the Raft group immediately (`ra:add_member`), but
data sync is serialized. Alternatively, once B finishes syncing, C could copy from
B instead of A — spreading the load.

### 7. WAL grows during long sync

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

All cluster config is via environment variables. No config files to mount.

### Environment variables

```bash
# --- Required for cluster mode ---
FERRICSTORE_NODE_NAME=ferric1@ferric1       # Erlang node name (name@hostname)
FERRICSTORE_COOKIE=my_secret_cookie          # Erlang cookie (must match all nodes)

# --- Cluster role ---
FERRICSTORE_CLUSTER_ROLE=voter               # voter | replica | readonly (default: voter)

# --- Node discovery ---
FERRICSTORE_DISCOVERY=gossip                 # gossip | dns | epmd | consul | etcd | none
FERRICSTORE_DNS_NAME=ferricstore-headless    # for discovery=dns (Kubernetes headless service)
FERRICSTORE_CLUSTER_NODES=n1@h1,n2@h2,n3@h3 # for discovery=epmd (static list)
FERRICSTORE_CONSUL_URL=http://localhost:8500 # for discovery=consul
FERRICSTORE_CONSUL_SERVICE=ferricstore       # Consul service name
FERRICSTORE_ETCD_ENDPOINTS=http://localhost:2379  # for discovery=etcd
FERRICSTORE_ETCD_PREFIX=/ferricstore/nodes   # etcd key prefix

# --- Cluster tuning ---
FERRICSTORE_CLUSTER_REMOVE_DELAY_MS=60000    # delay before removing crashed node (default: 60s)
```

### Docker Compose — 3-node cluster

```yaml
version: "3.8"

services:
  ferric1:
    image: ferricstore:latest
    hostname: ferric1
    environment:
      FERRICSTORE_NODE_NAME: ferric1@ferric1
      FERRICSTORE_COOKIE: secret_cookie
      FERRICSTORE_DISCOVERY: gossip
      FERRICSTORE_DATA_DIR: /data
    volumes:
      - ferric1_data:/data
    ports:
      - "6379:6379"

  ferric2:
    image: ferricstore:latest
    hostname: ferric2
    environment:
      FERRICSTORE_NODE_NAME: ferric2@ferric2
      FERRICSTORE_COOKIE: secret_cookie
      FERRICSTORE_DISCOVERY: gossip
      FERRICSTORE_DATA_DIR: /data
    volumes:
      - ferric2_data:/data
    ports:
      - "6380:6379"

  ferric3:
    image: ferricstore:latest
    hostname: ferric3
    environment:
      FERRICSTORE_NODE_NAME: ferric3@ferric3
      FERRICSTORE_COOKIE: secret_cookie
      FERRICSTORE_DISCOVERY: gossip
      FERRICSTORE_DATA_DIR: /data
    volumes:
      - ferric3_data:/data
    ports:
      - "6381:6379"

volumes:
  ferric1_data:
  ferric2_data:
  ferric3_data:
```

All three nodes auto-discover each other via UDP gossip (same Docker network).
No static node list needed. Data persisted in Docker volumes.

### Kubernetes — StatefulSet + headless service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ferricstore-headless
spec:
  clusterIP: None
  selector:
    app: ferricstore
  ports:
    - port: 6379
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ferricstore
spec:
  serviceName: ferricstore-headless
  replicas: 3
  selector:
    matchLabels:
      app: ferricstore
  template:
    metadata:
      labels:
        app: ferricstore
    spec:
      containers:
        - name: ferricstore
          image: ferricstore:latest
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: FERRICSTORE_NODE_NAME
              value: "$(POD_NAME)@$(POD_NAME).ferricstore-headless"
            - name: FERRICSTORE_COOKIE
              valueFrom:
                secretKeyRef:
                  name: ferricstore-secret
                  key: cookie
            - name: FERRICSTORE_DISCOVERY
              value: dns
            - name: FERRICSTORE_DNS_NAME
              value: ferricstore-headless
            - name: FERRICSTORE_DATA_DIR
              value: /data
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

DNS discovery polls the headless service for A records. Each pod gets a stable
hostname (`ferricstore-0.ferricstore-headless`). Persistent volumes survive
pod restarts.

### Docker Compose — with read replicas

```yaml
services:
  # ... 3 voter nodes as above ...

  replica-us-west:
    image: ferricstore:latest
    hostname: replica-us-west
    environment:
      FERRICSTORE_NODE_NAME: replica-us-west@replica-us-west
      FERRICSTORE_COOKIE: secret_cookie
      FERRICSTORE_CLUSTER_ROLE: replica
      FERRICSTORE_DISCOVERY: gossip
    volumes:
      - replica_us_west_data:/data

  replica-eu:
    image: ferricstore:latest
    hostname: replica-eu
    environment:
      FERRICSTORE_NODE_NAME: replica-eu@replica-eu
      FERRICSTORE_COOKIE: secret_cookie
      FERRICSTORE_CLUSTER_ROLE: readonly
      FERRICSTORE_DISCOVERY: gossip
    volumes:
      - replica_eu_data:/data
```

`replica` (promotable) in same region. `readonly` (non_voter) in different region.

### Standalone mode (default, no cluster)

If `FERRICSTORE_NODE_NAME` is not set, FerricStore starts as a standalone
single-node instance. No distributed Erlang, no libcluster, no Raft replication
to other nodes. Identical to current behavior.

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
| `lib/ferricstore/raft/cluster.ex` | Modify `start_shard_server` to use multi-node initial_members |
| `lib/ferricstore/store/shard.ex` | Add `:pause_writes` / `:resume_writes` handle_call |
| `lib/ferricstore/store/router.ex` | No changes needed — reads already local, writes already go through Raft |
| `lib/ferricstore/application.ex` | Add ClusterManager to supervision tree |
| `lib/ferricstore/commands/cluster.ex` | Add CLUSTER.JOIN, CLUSTER.LEAVE, CLUSTER.FAILOVER, CLUSTER.STATUS |
| `config/runtime.exs` | Cluster env var config (already restored) |

---

## Failure Modes

| Scenario | Impact | Recovery |
|---|---|---|
| 1 of 3 nodes crashes | Writes continue (2/3 quorum). Reads unaffected. | Node rejoins, ra syncs WAL. |
| 2 of 3 nodes crash | Writes fail (1/3 no quorum). Reads serve stale. | Restart crashed nodes. |
| Network partition (2\|1) | Majority side continues. Minority rejects writes. | Partition heals, ra reconciles. |
| Leader crashes | Followers elect new leader (~100ms). Brief write pause. | Old leader rejoins as follower. |
| New node joins (direct copy) | Brief per-shard write pause (~100ms each). | Sync completes shard-by-shard. |
| New node joins (disk snapshot) | Zero leader impact. | Cloud disk snapshot + Raft WAL catch-up. |
| Node disk full | Raft WAL writes fail on that node. Others continue. | Free disk space, node re-syncs. |

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

---

## Design Decisions

1. **Reads are always local** — every node reads from its own ETS. This is the entire point of replication. No "consistent read" mode — that would just be a proxy.
2. **Minimum 3 nodes** — Raft requires a majority for writes. With 3 nodes, 1 can fail and writes continue (2/3 quorum). With 2 nodes, any failure blocks writes. Every serious distributed system enforces this: Redis Sentinel, etcd, CockroachDB, RabbitMQ quorum queues. ClusterManager rejects cluster formation with fewer than 3 nodes.

## Design Decisions (continued)

3. **Let ra elect leaders naturally** — no forced leadership distribution. ra's random election timeouts spread leaders across nodes well enough. Forced rebalancing fights Raft's election logic and adds complexity for minimal gain. Can add `CLUSTER.REBALANCE` later if needed.

4. **Cloud disk snapshots for large datasets** — for production deployments with large datasets (10GB+), use cloud provider disk snapshots (EBS snapshots on AWS, persistent disk snapshots on GCP) to bootstrap new nodes. The snapshot captures the entire data directory at the block level with zero leader impact. The new node boots from the snapshot, runs `recover_keydir`, and catches up via Raft WAL replay. No custom snapshot format, no upload/download machinery — just the cloud provider's native tooling.

5. **Read replicas via ra's `promotable` membership** — ra natively supports non-voting members (`ra_membership() :: voter | promotable | non_voter`). A replica receives all Raft replication (ETS stays current) but doesn't vote in elections or count toward quorum. Reads from local ETS, writes forward to the leader. Adding a replica is just config — no new replication protocol needed.

6. **Ra's snapshot does NOT include ETS/Bitcask data** — ra's snapshot is just the state machine metadata (atom names, counters). All key-value data lives in Bitcask files and ETS. A new follower receiving only ra's snapshot has ZERO user data. The Bitcask file copy (direct copy or cloud disk snapshot) is a **correctness requirement**, not an optimization. The `wal_bridgeable?` check ensures we never skip a required data sync.

7. **Sequential shard sync for direct copy** — copy one shard at a time. Each shard requires a brief write pause on the leader; parallel copy would pause ALL shards simultaneously (full write outage). Sequential keeps 75% of writes flowing. For large datasets, cloud disk snapshots handle the "fast bootstrap" case — new node boots from a block-level snapshot with zero leader impact, then catches up via WAL replay.
