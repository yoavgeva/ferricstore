# FerricStore Throughput Optimization Guide

Complete optimization map organized by layer, from highest leverage to lowest.

---

## 1. BEAM VM Flags (vm.args)

Highest leverage — affects every operation.

```bash
# Scheduler pinning — single biggest win
+sbt db           # pin each scheduler to a dedicated CPU core
                  # keeps ETS data, process heaps, TLB entries hot per core
                  # ~10-20% throughput gain

# Wakeup latency — critical for burst handling
+sbwt very_short  # spin briefly before sleeping → ~1µs wakeup vs ~100µs
+swt very_low     # wake idle schedulers the moment work arrives

# I/O polling — mandatory
+K true           # use epoll/kqueue instead of select

# Scale limits — prevent hard crashes
+P 5000000        # max processes (default 262K is too low at scale)
+Q 65536          # max ports/sockets
+A 128            # async threads for Bitcask disk reads

# Memory allocator — reduce fragmentation under sustained load
+MHas aoffcbf     # heap allocator: address order first fit
+MBas aoffcbf     # binary allocator: same strategy
```

## 2. OS-Level: TLB & Huge Pages

When ETS is large (GBs), the CPU TLB (~1500-4000 entries) can't hold all
4KB page mappings. Every ETS lookup becomes a page table walk.

```bash
# 4KB pages: 4GB ETS = 1,000,000 TLB entries needed
# 2MB pages: 4GB ETS = 2,048 TLB entries needed  ← fits in TLB

# Enable explicit huge pages at boot
echo 'vm.nr_hugepages = 1024' >> /etc/sysctl.conf

# Never swap — a swapped cache is a dead cache
echo 'vm.swappiness = 1' >> /etc/sysctl.conf

# Disable transparent huge pages — causes latency spikes during compaction
echo never > /sys/kernel/mm/transparent_hugepage/enabled

# NVMe I/O scheduler — bypass block layer for NVMe
echo none > /sys/block/nvme0n1/queue/scheduler

# Raise block layer queue depth
echo 1024 > /sys/block/nvme0n1/queue/nr_requests

# Open file limits (Bitcask opens many file descriptors)
echo '* soft nofile 1048576' >> /etc/security/limits.conf
echo '* hard nofile 1048576' >> /etc/security/limits.conf

# CPU governor — consistent latency, no frequency throttling
cpupower frequency-set -g performance
```

## 3. Shard Configuration

Each shard = independent Raft group + independent ETS table + independent WAL.
More shards = true parallelism across CPU cores and NVMe queues.

```elixir
config :ferricstore,
  shard_count: System.schedulers_online() * 2
  # 8 cores → 16 shards
  # 16 shards × parallel clients → QD32+ on NVMe (sweet spot)
  # each shard gets its own Raft leader, WAL file, fsync
```

Write throughput model:

```
parallel_writes ≈ shard_count × clients_per_shard
16 shards       → ~400-600K writes/sec (theoretical)
4 shards        → ~150-200K writes/sec
```

## 4. ETS Table Configuration

```elixir
:ets.new(:cache, [
  :set,
  read_concurrency: true,   # per-scheduler read locks → ~4x faster reads
  write_concurrency: true,  # concurrent writes to different buckets
  # or use :auto on OTP 24+ — dynamically switches between modes
])
```

FerricStore already uses `write_concurrency: :auto` on OTP 24+.

## 5. Write Path Optimizations

### Group commit / batching

The Raft WAL batches all writes arriving within the fdatasync window into a
single NIF call with one fsync. This is the key to surviving high write
throughput.

### Pipeline via MULTI/EXEC

Batch multiple commands into one Raft proposal round-trip:

```
MULTI
SET k1 v1
SET k2 v2
SET k3 v3
EXEC
→ one Raft round-trip instead of three
```

### Async durability

Local fsync happens, Raft cross-node consensus is async:

```elixir
# Configure per namespace:
config :ferricstore,
  namespace_durability: %{"cache:" => :async}
# data on local disk immediately, replica sync in background
```

## 6. Wire Protocol & Serialization

### Store raw Erlang binaries instead of JSON

Eliminates client-side serialization cost entirely:

```elixir
# Write
binary = :erlang.term_to_binary(my_term)
FerricStore.set("key", binary)

# Read
binary = FerricStore.get("key")
term = :erlang.binary_to_term(binary)
```

### Embedded mode

When app and store are on the same BEAM node, eliminates TCP, RESP3 framing,
and serialization entirely:

```
embedded read:    ETS lookup from calling process → ~1-5µs
standalone read:  TCP + RESP3 parse + ETS → ~50-300µs
```

## 7. Data Modeling

Fewer keys = smaller keydir = more fits in ETS hot tier = better TLB behavior.

```elixir
# Bad — 3 keydir entries, 3 ETS slots
SET "user:42:name"    "alice"
SET "user:42:email"   "alice@example.com"
SET "user:42:country" "US"

# Good — 1 keydir entry, 1 ETS slot
HSET "user:42" name "alice" email "alice@example.com" country "US"
```

## 8. Memory Hierarchy

Keep data cache-hot. The read speed hierarchy:

| Location | Latency | How to target it |
|----------|---------|------------------|
| Process dict / local var | ~1-4ns | persistent_term for static hot data |
| L1 cache (32-48KB/core) | ~4ns | keep process working set < 16KB |
| L2 cache (256KB-1MB/core) | ~10ns | keep per-scheduler ETS shard small |
| L3 cache (8-512MB shared) | ~40ns | ETS hot tier, stays warm with +sbt db |
| RAM | ~100ns | Bitcask keydir (always in RAM) |
| NVMe cold read | ~50-200us | Bitcask cold path via Rust NIF |

Use persistent_term for config, routing tables, and static lookup data —
zero copy, zero lock, lives in shared VM memory:

```elixir
:persistent_term.put(:my_config, %{...})
:persistent_term.get(:my_config)   # no copy, no ETS overhead
```

## 9. Process & Scheduler Topology

Co-locate communicating processes on the same CCD (cores sharing L3):

```bash
# Check which cores share L3
cat /sys/devices/system/cpu/cpu0/cache/index3/shared_cpu_list

# Pin BEAM to a single NUMA node (for multi-socket servers)
numactl --cpubind=0 --membind=0 ./ferricstore start
```

Keep process heaps small so they stay in L1/L2:
- Don't pre-allocate large min_heap_size — let the heap grow naturally
- Keep hot process working sets under ~16-24KB (L1 budget)
- Store large data in ETS or as refc binaries, not in process heap

## 10. OTP Version

- OTP 24+: `:auto` ETS locking (best of read/write concurrency dynamically)
- OTP 26+: BeamJIT reduces interpreter overhead in hot loops
- Stay current for best performance

---

## Impact Priority Order

| Optimization | Effort | Gain |
|-------------|--------|------|
| `+sbt db` scheduler pinning | Low | High — 10-20% across the board |
| Huge pages / TLB | Low | High — critical for large ETS |
| Shard count = cores x 2 | Low | High — direct write parallelism |
| `+sbwt very_short` | Low | Medium — latency, burst handling |
| MULTI/EXEC pipelining | Medium | High — removes RTT per write |
| Embedded mode | Architecture | Extreme — eliminates TCP entirely |
| Raw Erlang binary storage | Low | Medium — removes JSON overhead |
| `vm.swappiness = 1` | Trivial | Critical — swap = cache death |
| NVMe scheduler `none` | Trivial | Medium — removes block layer overhead |
