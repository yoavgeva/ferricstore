# FerricStore Throughput Optimization Guide

Each section has: current default, what to benchmark, expected tradeoff.

---

## Tunable Parameters (need benchmarking)

### 1. WAL Batch Window (ra gen_batch_server)

**Current:** ra's `gen_batch_server` accumulates messages until the process
mailbox is empty, then processes them all as a batch. Our async fdatasync
patch adds another layer — writes queue until fdatasync completes, then the
next batch starts.

**What to benchmark:**
- Throughput at different concurrent writer counts (1, 10, 50, 100, 500)
- p50/p95/p99 latency per write at each concurrency level
- How many writes per fdatasync cycle (the "natural batch size")

**Tradeoff:** More writes per fsync = higher throughput but higher tail latency.
The current design auto-batches based on load — no explicit window_ms to tune.

### 2. Batcher max_batch_size

**Current:** `1000` commands per slot before forced flush.

**What to benchmark:**
- Lower values (100, 500) — does it reduce latency without hurting throughput?
- Higher values (2000, 5000) — does it improve throughput for bulk loads?
- Measure both the embedded API path (bypasses Batcher) and TCP path

**Tradeoff:** Larger batches = fewer Raft proposals = higher throughput, but
each proposal is bigger and takes longer to apply.

**Note:** The quorum write path (`Router.quorum_write`) bypasses the Batcher
entirely — it goes directly to `ra.pipeline_command`. The Batcher is only used
by the Shard GenServer fallback path. So this setting mainly affects the
fallback and async paths.

### 3. Shard Count

**Current:** `4` (default), configurable via `FERRICSTORE_SHARD_COUNT` env var.
If set to `0`, defaults to `System.schedulers_online()`.

**What to benchmark:**
- `schedulers_online()` (1:1 with cores)
- `schedulers_online() * 2` (2:1, more parallelism for NVMe)
- `schedulers_online() * 4` (4:1, diminishing returns?)
- Measure write throughput, read throughput, and keydir memory overhead

**Tradeoff:** More shards = more write parallelism (each shard has independent
Raft group + WAL + fsync). But more shards can DECREASE read throughput:

- More ETS tables = more pointer indirection, less CPU cache locality
- MGET across shards hits multiple tables on different cache lines
- Each shard's ETS is smaller, but total memory is the same
- More file descriptors and memory overhead (WAL, Raft state per shard)

```
4 shards on 8 cores  → fewer writes, but reads stay cache-hot
16 shards on 8 cores → more write parallelism, NVMe QD16+
32 shards on 8 cores → diminishing write returns, reads may degrade
```

The sweet spot depends on your read/write ratio. Write-heavy workloads
benefit from more shards. Read-heavy workloads may prefer fewer.

### 4. Socket Active Mode

**Current:** `active: 100` — kernel delivers up to 100 TCP messages before
going passive. Re-arms on `{:tcp_passive, _}`.

**What to benchmark:**
- `active: true` — no back-pressure, highest throughput, risk of mailbox flood
- `active: 50` — less kernel overhead than 100
- `active: 200` — more batching per passive cycle
- `active: :once` — lowest throughput, best back-pressure

**Tradeoff:** Higher N = fewer `setopts` syscalls = higher throughput, but
larger mailbox under slow consumers.

### 5. Hot Cache Max Value Size

**Current:** `65_536` (64KB). Values larger than this are stored as `nil` in
ETS (cold). Reads go to Bitcask NIF pread.

**What to benchmark:**
- Lower (16KB, 32KB) — less ETS memory, more cold reads
- Higher (128KB, 256KB) — more ETS memory, fewer cold reads
- Measure with your actual value size distribution

**Tradeoff:** Larger threshold = more RAM used by ETS hot cache, but fewer
disk reads. The off-heap binary tracking we added makes MemoryGuard accurate
regardless of this setting.

### 6. Max Active File Size (Bitcask)

**Current:** `256MB`. When active file exceeds this, it rolls over to a new
file and the old one becomes eligible for compaction.

**What to benchmark:**
- Smaller (64MB, 128MB) — more frequent rollovers, more compaction opportunities
- Larger (512MB, 1GB) — fewer rollovers, larger sequential writes

**Tradeoff:** Smaller files = more frequent compaction = more I/O overhead
but less wasted space. Larger files = less overhead but recovery takes longer
(more data to replay on restart).

### 7. Release Cursor Interval (Raft Snapshots)

**Current:** `100` — every 100 applied commands, ra may take a snapshot.

**What to benchmark:**
- Lower (50) — more frequent snapshots, faster recovery, more I/O
- Higher (500, 1000) — less snapshot overhead, slower recovery

**Tradeoff:** More frequent snapshots = faster restart (less WAL replay) but
more CPU/IO during normal operation.

---

## Fixed Settings (already optimized, just enable)

### BEAM VM Flags

```bash
# vm.args — add all of these
+sbt db           # scheduler pinning → 10-20% throughput gain
+sbwt very_short  # ~1µs wakeup vs ~100µs
+swt very_low     # wake idle schedulers immediately
+K true           # epoll/kqueue
+P 5000000        # max processes
+Q 65536          # max ports
+A 128            # async threads for disk reads
+MHas aoffcbf     # heap allocator strategy
+MBas aoffcbf     # binary allocator strategy
```

### OS Tuning

```bash
# Huge pages — critical for large ETS
echo 'vm.nr_hugepages = 1024' >> /etc/sysctl.conf

# Never swap
echo 'vm.swappiness = 1' >> /etc/sysctl.conf

# Disable THP (causes latency spikes)
echo never > /sys/kernel/mm/transparent_hugepage/enabled

# NVMe — bypass block layer
echo none > /sys/block/nvme0n1/queue/scheduler
echo 1024 > /sys/block/nvme0n1/queue/nr_requests

# File descriptors
echo '* soft nofile 1048576' >> /etc/security/limits.conf

# CPU governor
cpupower frequency-set -g performance
```

### NUMA (multi-socket)

```bash
# Pin BEAM to one NUMA node
numactl --cpubind=0 --membind=0 ./ferricstore start
```

---

## Application-Level Optimizations

### Embedded mode vs TCP

```
embedded:   ETS lookup → ~1-5µs per read
TCP/RESP3:  syscall + parse + ETS → ~50-300µs per read
```

If your app runs on the same BEAM node, use the embedded API.

### Data modeling — fewer keys

```
3 keys (3 keydir entries):  SET user:42:name, SET user:42:email, SET user:42:country
1 key (1 keydir entry):     HSET user:42 name email country
```

Fewer keys = smaller keydir = better TLB hit rate = faster everything.

### Raw binary storage

```elixir
# Skip JSON — store Erlang terms directly
FerricStore.set("key", :erlang.term_to_binary(term))
term = :erlang.binary_to_term(FerricStore.get("key"))
```

### MULTI/EXEC pipelining

```
MULTI → SET k1 v1 → SET k2 v2 → SET k3 v3 → EXEC
= 1 Raft round-trip instead of 3
```

---

## Benchmarking Methodology

For each tunable, run:

```bash
# Baseline
mix run bench/throughput.exs --writers 50 --duration 30

# Change one variable
FERRICSTORE_SHARD_COUNT=16 mix run bench/throughput.exs --writers 50 --duration 30
```

Measure:
- **Throughput**: ops/sec (writes and reads separately)
- **Latency**: p50, p95, p99 in microseconds
- **Memory**: RSS, ETS memory, binary bytes tracked
- **Disk I/O**: IOPS, bandwidth, fsync count

Compare against baseline. Change one variable at a time.
