# FerricStore

**The Redis-compatible store where every write is durable by default.**

FerricStore is a distributed, ACID-compliant key-value store with Redis wire protocol (RESP3). Every write goes through Raft consensus and is fsync'd to disk before the client gets OK. No "enable persistence" checkbox. No "hope the AOF rewrite finishes before the crash." Your data survives kill -9, node failures, and power loss — automatically.

## Why FerricStore?

### The Problem

Redis, Dragonfly, and Memcached treat persistence as optional. By default, your data lives only in RAM. Enable AOF? You still lose the last second. Enable RDB snapshots? You lose minutes. Even with replication, failover is manual or requires Sentinel — a separate system to babysit your cache.

You shouldn't have to choose between speed and safety.

### Distributed ACID

FerricStore provides full ACID guarantees on quorum-mode writes:

| Property | How |
|---|---|
| **Atomicity** | MULTI/EXEC, CAS, and every individual command execute as single Raft log entries |
| **Consistency** | Raft linearizability — every read sees the latest committed write |
| **Isolation** | Single-threaded state machine per shard — commands never interleave |
| **Durability** | WAL + fdatasync + Raft quorum — majority of nodes must persist before ack |

No other Redis-compatible store offers this. Not Redis (no distributed ACID). Not Dragonfly (no consensus). Not Garnet (single-node transactions only, manual failover). Not KeyDB (no WAL consensus).

### What Happens When a Node Dies

| | Redis | Dragonfly | Garnet | **FerricStore** |
|---|---|---|---|---|
| **Data loss** | Last 1s (AOF) or minutes (RDB) | Since last snapshot | Until replica catches up | **Zero (quorum-committed)** |
| **Failover** | Manual or Sentinel (separate system) | Manual | Manual (external control plane) | **Automatic (Raft leader election)** |
| **Recovery time** | Seconds to minutes | Minutes | Depends on control plane | **Sub-second (new leader elected)** |
| **Client action needed** | Reconnect + retry | Reconnect + retry | Reconnect + retry | **Raft forwards to new leader** |

### Durability Spectrum

Not every key needs the same guarantee. FerricStore lets you choose per namespace:

```elixir
# Session tokens: never lose them (quorum — majority must persist before ack)
FERRICSTORE.CONFIG SET "session:" durability quorum

# Cache entries: speed over safety (async — write to local WAL, replicate in background)
FERRICSTORE.CONFIG SET "cache:" durability async
```

One cluster. Mixed workloads. The right tradeoff for each key prefix.

## Two Deployment Modes

### Embedded (inside your Elixir app)

```elixir
# mix.exs
{:ferricstore, "~> 0.1.0"}

# Your code — zero TCP overhead, zero serialization
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")
```

No sidecar. No Docker. No ops. A persistent, replicated key-value store that ships inside your app. Your Phoenix app starts, FerricStore starts with it. Your tests run, each test gets its own isolated store (Ecto-level sandbox).

### Standalone (Redis-compatible server)

```bash
docker run -p 6379:6379 -v ferricstore_data:/data ferricstore

# Connect with any Redis client
redis-cli -p 6379
127.0.0.1:6379> SET user:42:name alice
OK
127.0.0.1:6379> GET user:42:name
"alice"
```

Drop-in Redis replacement. 250+ commands. Use your existing Redis client libraries.

## Feature Comparison

| | Redis | Dragonfly | Garnet | **FerricStore** |
|---|---|---|---|---|
| RESP3 protocol | Yes | Yes | Yes | **Yes** |
| Durable by default | No | No | Checkpoint | **Yes (WAL + Raft)** |
| Distributed ACID | No | No | Single-node only | **Yes** |
| Automatic failover | Sentinel (separate) | No | No (needs control plane) | **Built-in (Raft)** |
| Embeddable as library | No | No | .NET only | **Elixir/Erlang** |
| Test sandbox | No | No | No | **Ecto-level isolation** |
| Probabilistic structures | Module (RedisBloom) | No | No | **Built-in** |
| Vector search | Module (RediSearch) | No | No | **Built-in** |
| Per-key durability | No | No | No | **Per-namespace config** |
| Built-in dashboard | No | No | No | **Yes (standalone mode)** |

## Architecture

```
                    +--------------------------------------------------+
                    |                  Client Layer                      |
                    |  redis-cli / Redix / any Redis client / Elixir API |
                    +------------------------+-------------------------+
                                             |
                    +------------------------+-------------------------+
                    |              FerricStore Server                    |
                    |  Ranch TCP/TLS  |  RESP3 Parser  |  ACL  |  Router |
                    +------------------------+-------------------------+
                                             |
                    +------------------------+-------------------------+
                    |              FerricStore Core                      |
                    |                                                    |
                    |  +----------+  +----------+  +----------+         |
                    |  | Shard 0  |  | Shard 1  |  | Shard N  |  Router |
                    |  +----+-----+  +----+-----+  +----+-----+         |
                    |       |             |             |                |
                    |  +----+-------------+-------------+----+          |
                    |  |         Raft Consensus (ra)          |          |
                    |  |    per-shard groups, WAL + fdatasync  |          |
                    |  +----+-------------+-------------+----+          |
                    |       |             |             |                |
                    |  +----+-----+  +----+-----+  +---+------+        |
                    |  |ETS keydir|  | Bitcask   |  |  mmap    |        |
                    |  |(hot data)|  |(cold data)|  |(prob/vec)|        |
                    |  +----------+  +----------+  +----------+        |
                    +--------------------------------------------------+
```

### Write Path (Quorum Mode)

1. Client sends SET (TCP/RESP3 or Elixir API)
2. Router hashes key to shard
3. Shard writes to ETS (reads see it immediately)
4. Entry queued in group-commit batcher
5. Batcher submits batch to Raft
6. Raft replicates to quorum, each node fsync's to WAL
7. **Only after quorum confirms durable:** client gets OK

### Read Path

1. Client sends GET
2. Router hashes key to shard
3. Single ETS lookup — no GenServer, no lock, no copy (for hot keys)
4. Hot hit: return in ~1-5us
5. Cold (evicted from ETS): pread from Bitcask via Rust NIF in ~50-200us (NVMe)

### Three-Tier Storage — Disk as Cache, Not Just Persistence

Traditional caches are RAM-only. When memory fills up, keys are evicted and gone — the next request for that key hits your origin database (Postgres, MySQL, API). If your dataset is 100GB and you have 16GB of RAM, you can only cache 16% of it. The other 84% always goes to the origin.

FerricStore keeps evicted data on disk instead of discarding it. When a key is evicted from RAM (ETS), it's still on disk in Bitcask. A cold read from NVMe (~50-200us) is 100x faster than hitting Postgres (~5-50ms). Your effective cache size is your disk, not your RAM.

```
Traditional cache:                    FerricStore:
RAM [16 GB] → Miss → Postgres 10ms   RAM [16 GB] → Miss → Disk 0.1ms → Miss → Postgres 10ms
Cache 16% of dataset                  Cache 100% of dataset on disk, hot 16% in RAM
```

| Tier | Technology | Latency | What lives here |
|------|-----------|---------|-----------------|
| **Hot** | ETS (in-memory) | ~1-5us | Frequently accessed keys (LFU eviction) |
| **Cold** | Bitcask (on disk) | ~50-200us (NVMe) | Everything — evicted keys still readable |
| **Probabilistic** | mmap files | ~1-10us | Bloom, Cuckoo, CMS, TopK, vectors |

**Cache without fear.** With RAM-only caches, you limit what you cache because eviction means a database hit. You avoid caching large datasets, expensive queries, or anything where a miss causes a stampede. With disk-backed storage, eviction is a 100us penalty, not a 10ms database roundtrip. Cache everything — your product catalog, your user profiles, your search results. The worst case is a disk read, not a database storm.

**Never cold-start again.** When Redis restarts, the cache is empty. Every request misses. Your database gets slammed with the full production load until the cache warms up — minutes to hours depending on dataset size. FerricStore restarts with all data on disk. The first request after a deploy reads from Bitcask in microseconds, not from Postgres in milliseconds. There is no cold start.

## Commands (250+)

| Category | Examples | Count |
|----------|---------|-------|
| Strings | GET, SET, MGET, MSET, INCR, APPEND, GETRANGE... | 22 |
| Hash | HSET, HGET, HGETALL, HINCRBY, HSCAN, HEXPIRE... | 24 |
| List | LPUSH, RPUSH, LPOP, RPOP, LRANGE, BLPOP, LMOVE... | 20 |
| Set | SADD, SREM, SMEMBERS, SINTER, SUNION, SPOP... | 17 |
| Sorted Set | ZADD, ZRANGE, ZSCORE, ZRANK, ZPOPMIN, ZSCAN... | 17 |
| Stream | XADD, XLEN, XRANGE, XREAD, XGROUP, XREADGROUP... | 11 |
| Geo | GEOADD, GEOPOS, GEODIST, GEOSEARCH... | 6 |
| JSON | JSON.SET, JSON.GET, JSON.DEL, JSON.NUMINCRBY... | 13 |
| Probabilistic | BF.*, CF.*, CMS.*, TOPK.*, TDIGEST.* | 49 |
| Vector | VCREATE, VADD, VSEARCH, VGET, VDEL... | 8 |
| Native | CAS, LOCK, UNLOCK, RATELIMIT.ADD, FETCH_OR_COMPUTE | 7 |
| Transaction | MULTI, EXEC, DISCARD, WATCH | 5 |
| Pub/Sub | SUBSCRIBE, PUBLISH, PSUBSCRIBE... | 6 |

## Quick Start

### Embedded Mode

```elixir
# mix.exs
def deps do
  [{:ferricstore, "~> 0.1.0"}]
end

# config/config.exs
config :ferricstore, :mode, :embedded
config :ferricstore, :data_dir, "priv/ferricstore_data"
config :ferricstore, :shard_count, 4
```

```elixir
# Durable by default — this write survives kill -9
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")

# Atomic compare-and-swap
{:ok, true} = FerricStore.cas("counter", "1", "2")

# Distributed lock
{:ok, true} = FerricStore.lock("resource:42", "owner-id", ttl: 30_000)

# Cache-aside with stampede protection
case FerricStore.fetch_or_compute("expensive:key", ttl: 60_000) do
  {:ok, {:hit, value}} -> value
  {:ok, {:compute, _}} ->
    value = expensive_computation()
    FerricStore.fetch_or_compute_result("expensive:key", value, ttl: 60_000)
    value
end
```

### Standalone Mode

```bash
# From source
git clone https://github.com/YoavGivati/ferricstore.git
cd ferricstore && mix deps.get && mix run --no-halt

# Docker
docker run -p 6379:6379 -v ferricstore_data:/data ferricstore

# 3-node cluster
docker compose up -d
```

### Test Sandbox

Every test gets an isolated store — private shards, private ETS, private Raft WAL:

```elixir
# test/my_test.exs
use FerricStore.Sandbox.Case

test "my cache test" do
  :ok = FerricStore.set("key", "value")
  {:ok, "value"} = FerricStore.get("key")
  # This key doesn't exist in any other test
end
```

Full `async: true` support. No state leaks between tests.

## Built-in Observability (Standalone Mode)

FerricStore's standalone server includes a dashboard at `/dashboard` with no external dependencies:

- Cache hit rate with RAM vs Disk breakdown
- Key lifecycle (expired, evicted, keydir capacity)
- Per-shard status, keys, memory, disk usage
- Raft consensus health (leader, term, commit index)
- Slow log, merge status, client list
- Per-prefix key distribution
- Prometheus metrics at `/metrics`

## Guides

- [Getting Started](guides/getting-started.md)
- [Configuration Reference](guides/configuration.md)
- [Architecture](guides/architecture.md)
- [Embedded Mode](guides/embedded-mode.md)
- [Standalone Mode](guides/standalone-mode.md)
- [Commands Reference](guides/commands.md)
- [Security](guides/security.md)
- [Deployment](guides/deployment.md)

## Requirements

- Elixir >= 1.19
- Erlang/OTP 27+
- Rust toolchain (for NIF compilation, or use precompiled binaries)

## License

MIT
