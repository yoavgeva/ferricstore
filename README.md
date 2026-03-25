# FerricStore

A Redis-compatible key-value store built in Elixir and Rust. FerricStore runs as a standalone server (drop-in Redis replacement with RESP3 protocol) or embeds directly inside your Elixir application with zero-copy ETS reads at microsecond latency. It combines Raft consensus for durability, Bitcask append-only storage for write throughput, and mmap-backed probabilistic structures for memory-efficient data processing.

## Key Features

- **250+ Redis commands** -- Strings, Hash, List, Set, Sorted Set, Stream, Geo, JSON, Bitmap, HyperLogLog, and more
- **Two deployment modes** -- standalone TCP server or embedded Elixir library
- **RESP3 protocol** -- connect with `redis-cli`, Redix, or any Redis client
- **Raft consensus** -- all writes go through Raft for crash-safe durability
- **Three-tier storage** -- ETS hot cache, Bitcask append-only log, mmap probabilistic structures
- **Probabilistic data structures** -- Bloom, Cuckoo, Count-Min Sketch, TopK, TDigest (mmap-backed)
- **Vector search** -- VCREATE, VADD, VSEARCH with cosine, L2, and inner product distance
- **FerricStore-native commands** -- CAS, distributed LOCK/UNLOCK, RATELIMIT, FETCH_OR_COMPUTE
- **ACL security** -- user accounts, PBKDF2 passwords, command categories, key patterns
- **TLS support** -- optional encrypted connections with certificate auth
- **Sandbox testing** -- async-safe per-test isolation (like Ecto.Sandbox)
- **Prometheus metrics** -- built-in scrape endpoint, no external dependencies
- **LFU eviction** -- Redis-compatible probabilistic eviction with configurable policies

## Quick Start

### Embedded Mode (inside your Elixir app)

**1. Add the dependency**

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.1.0"}
  ]
end
```

**2. Configure**

```elixir
# config/config.exs
config :ferricstore, :mode, :embedded
config :ferricstore, :data_dir, "priv/ferricstore_data"
config :ferricstore, :shard_count, 4
```

**3. Use it**

```elixir
# Strings
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")

# Hash
:ok = FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
{:ok, "alice"} = FerricStore.hget("user:42", "name")

# Sorted Set
{:ok, 2} = FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
{:ok, ["alice", "bob"]} = FerricStore.zrange("leaderboard", 0, -1)

# Compare-and-swap (atomic)
FerricStore.set("counter", "1")
{:ok, true} = FerricStore.cas("counter", "1", "2")

# Cache-aside with stampede protection
case FerricStore.fetch_or_compute("expensive:key", ttl: 60_000) do
  {:ok, {:hit, value}} -> value
  {:ok, {:compute, _hint}} ->
    value = expensive_computation()
    FerricStore.fetch_or_compute_result("expensive:key", value, ttl: 60_000)
    value
end
```

### Standalone Mode (Redis-compatible server)

**1. Clone and start**

```bash
git clone https://github.com/YoavGivati/ferricstore.git
cd ferricstore
mix deps.get
mix run --no-halt
```

**2. Connect with redis-cli**

```bash
redis-cli -p 6379
127.0.0.1:6379> SET user:42:name alice
OK
127.0.0.1:6379> GET user:42:name
"alice"
127.0.0.1:6379> HSET user:42 name alice age 30
(integer) 2
127.0.0.1:6379> ZADD leaderboard 100 alice 200 bob
(integer) 2
```

## Architecture

```
                    ┌──────────────────────────────────────────────────┐
                    │                  Client Layer                     │
                    │  redis-cli / Redix / any Redis client / Elixir API│
                    └────────────────────┬─────────────────────────────┘
                                         │
                    ┌────────────────────┴─────────────────────────────┐
                    │              FerricStore Server                    │
                    │  Ranch TCP/TLS ← RESP3 Parser ← ACL ← Dispatcher │
                    │                     (standalone mode only)         │
                    └────────────────────┬─────────────────────────────┘
                                         │
                    ┌────────────────────┴─────────────────────────────┐
                    │              FerricStore Core                      │
                    │                                                    │
                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
                    │  │ Shard 0 │  │ Shard 1 │  │ Shard N │  Router   │
                    │  │         │  │         │  │         │ (phash2)  │
                    │  └────┬────┘  └────┬────┘  └────┬────┘           │
                    │       │            │            │                  │
                    │  ┌────┴────────────┴────────────┴────┐           │
                    │  │         Raft Group Commit          │           │
                    │  │     (ra library, per-shard)        │           │
                    │  └────┬────────────┬────────────┬────┘           │
                    │       │            │            │                  │
                    │  ┌────┴────┐  ┌────┴────┐  ┌───┴─────┐          │
                    │  │ETS keydir│ │ Bitcask  │ │  mmap    │          │
                    │  │(hot data)│ │(cold data)│ │(prob/vec)│          │
                    │  └─────────┘  └─────────┘  └─────────┘          │
                    └──────────────────────────────────────────────────┘
```

### Three-Tier Storage

| Tier | Technology | Data | Access Pattern | Latency |
|------|-----------|------|---------------|---------|
| **Hot** | ETS | Key-value data (strings, hash, list, set, zset) | Key lookup with LFU eviction | ~1-5 us |
| **Cold** | Bitcask | Same data, evicted from ETS | Append-only log + pread | ~50-200 us |
| **mmap** | Memory-mapped files | Bloom, Cuckoo, CMS, TopK, TDigest, Vectors | OS page cache, zero-copy | ~1-10 us |

### Write Path

1. Client sends command (TCP/RESP3 or Elixir API)
2. Router hashes key to shard via `:erlang.phash2/2`
3. Shard writes to ETS immediately (reads see it at once)
4. Entry queued in group-commit batcher (per-namespace windows)
5. Batcher submits batch to Raft for consensus
6. Raft state machine applies to Bitcask (append-only)
7. Caller receives response after Raft quorum ack

### Read Path

1. Client sends GET (or other read command)
2. Router hashes key to shard
3. ETS direct lookup (no GenServer roundtrip for hot keys)
4. If hot (value in ETS): return immediately (~1-5 us)
5. If cold (value=nil in ETS): pread from Bitcask using file_id/offset (~50-200 us)

## Performance Characteristics

- **Hot reads**: ~1-5 us (ETS direct, no GenServer roundtrip)
- **Cold reads**: ~50-200 us (NVMe pread via Rust NIF)
- **Writes**: group-committed, ~200k ops/s per shard on NVMe
- **Embedded mode**: zero TCP overhead, zero RESP3 parsing
- **LFU eviction**: Redis-compatible probabilistic frequency counter
- **Group commit**: 1ms batching window (configurable per namespace)

## Redis Command Compatibility

| Category | Commands | Count |
|----------|----------|-------|
| Strings | GET, SET, DEL, MGET, MSET, INCR, DECR, APPEND, STRLEN, GETSET, SETNX, SETEX, GETRANGE, SETRANGE... | 22 |
| Hash | HSET, HGET, HDEL, HGETALL, HMGET, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HSCAN, HEXPIRE, HTTL... | 24 |
| List | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM, LTRIM, LMOVE, BLPOP, BRPOP... | 20 |
| Set | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SDIFF, SINTER, SUNION, SPOP, SSCAN, SMOVE... | 17 |
| Sorted Set | ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZCARD, ZINCRBY, ZCOUNT, ZPOPMIN, ZSCAN... | 17 |
| Stream | XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XINFO, XGROUP, XREADGROUP, XACK | 11 |
| Geo | GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE | 6 |
| JSON | JSON.SET, JSON.GET, JSON.DEL, JSON.TYPE, JSON.NUMINCRBY, JSON.ARRAPPEND... | 13 |
| Bitmap | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP | 5 |
| HyperLogLog | PFADD, PFCOUNT, PFMERGE | 3 |
| Bloom Filter | BF.RESERVE, BF.ADD, BF.MADD, BF.EXISTS, BF.MEXISTS, BF.CARD, BF.INFO | 7 |
| Cuckoo Filter | CF.RESERVE, CF.ADD, CF.ADDNX, CF.DEL, CF.EXISTS, CF.MEXISTS, CF.COUNT, CF.INFO | 8 |
| Count-Min Sketch | CMS.INITBYDIM, CMS.INITBYPROB, CMS.INCRBY, CMS.QUERY, CMS.MERGE, CMS.INFO | 6 |
| TopK | TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY, TOPK.QUERY, TOPK.LIST, TOPK.INFO | 6 |
| TDigest | TDIGEST.CREATE, TDIGEST.ADD, TDIGEST.QUANTILE, TDIGEST.CDF, TDIGEST.MIN, TDIGEST.MAX... | 14 |
| Vector | VCREATE, VADD, VGET, VDEL, VSEARCH, VINFO, VLIST, VEVICT | 8 |
| Native | CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD, FETCH_OR_COMPUTE | 7 |
| Transaction | MULTI, EXEC, DISCARD, WATCH, UNWATCH | 5 |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB | 6 |
| Expiry | EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME, PEXPIRETIME | 9 |
| Server | PING, ECHO, INFO, DBSIZE, KEYS, FLUSHDB, CONFIG, ACL, CLIENT, COMMAND, SCAN, MEMORY... | 30+ |

## Why FerricStore?

| | Redis | FerricStore |
|---|---|---|
| **Language** | C | Elixir + Rust NIFs |
| **Embed in Elixir app** | No (separate process) | Yes (in-process, zero-copy) |
| **Consensus** | Redis Sentinel / Cluster | Raft (ra library) |
| **Probabilistic structures** | Redis Stack (separate module) | Built-in (mmap-backed) |
| **Vector search** | Redis Stack | Built-in |
| **Test isolation** | Manual flush | Sandbox (like Ecto.Sandbox) |
| **Observability** | External tools | Observer, remote shell, `:ets.info` |
| **Hot-upgradable** | Restart required | OTP hot code reload |
| **CAS primitive** | Lua scripting | Native CAS command |
| **Rate limiting** | Lua scripting | Native RATELIMIT.ADD command |
| **Cache stampede** | Manual implementation | Native FETCH_OR_COMPUTE |

## Guides

- [Getting Started](guides/getting-started.md) -- installation, configuration, first commands
- [Configuration Reference](guides/configuration.md) -- every config option explained
- [Architecture](guides/architecture.md) -- three-tier storage, Raft, shard routing, NIF design
- [Embedded Mode](guides/embedded-mode.md) -- using FerricStore inside your Elixir app
- [Standalone Mode](guides/standalone-mode.md) -- running as a Redis-compatible server
- [Commands Reference](guides/commands.md) -- all supported commands grouped by category
- [Security](guides/security.md) -- ACL, TLS, protected mode, audit logging

## Project Structure

FerricStore is an Elixir umbrella project with two apps:

```
apps/
  ferricstore/          # Core engine (no network dependencies)
    lib/ferricstore/
      store/            # Shard, Router, ETS keydir, LFU, CompoundKey
      commands/         # 250+ Redis command handlers
      raft/             # Batcher, StateMachine, Cluster, AsyncApplyWorker
      merge/            # Background compaction (Scheduler, Manifest, Semaphore)
      bitcask/          # Rust NIF bindings (append, pread, fsync, bloom, vector)
    native/             # Rust NIF source (ferricstore_bitcask)

  ferricstore_server/   # TCP/HTTP server (standalone mode only)
    lib/ferricstore_server/
      connection.ex     # Ranch protocol handler (RESP3, ACL, pipeline)
      listener.ex       # TCP listener
      tls_listener.ex   # TLS listener
      health/           # HTTP health endpoint + dashboard
```

## Requirements

- Elixir ~> 1.19
- Erlang/OTP 27+
- Rust toolchain (for NIF compilation, or use precompiled binaries)

## License

MIT
