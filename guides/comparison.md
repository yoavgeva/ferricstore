# Comparison with Other Stores

How FerricStore compares to Redis, its forks, and Elixir caching libraries.

## Overview

| | Durable by default | Automatic failover | Embeddable | RESP protocol | Probabilistic structures |
|---|---|---|---|---|---|
| **FerricStore** | **Yes (Raft + fsync)** | **Yes (Raft)** | **Yes (Elixir)** | RESP3 | **Built-in** |
| Redis | No | Sentinel (separate) | No | RESP2/3 | Built-in (Redis 8) |
| Valkey | No | Sentinel (separate) | No | RESP2/3 | Bloom, HLL |
| Dragonfly | No | Commercial only | No | RESP2/3 | Bloom (partial) |
| Garnet | No (AOF optional) | Cluster mode | Partial (.NET) | RESP2/3 | HLL only |
| Kvrocks | **Yes (RocksDB)** | No | No | RESP2/3 | Bloom, HLL, TDigest |
| Cachex | No | No | **Yes** | No | No |
| Nebulex | No | No | **Yes** | No | No |

## Redis

The original. Single-threaded, in-memory, optional persistence via RDB snapshots or AOF.

**What FerricStore does differently:**
- **Durable by default** — Redis requires explicit configuration for any persistence. Even AOF with `fsync every write` doesn't provide consensus. FerricStore writes go through Raft and fsync before the client gets OK.
- **Automatic failover** — Redis needs Sentinel (a separate system) or Redis Cluster. FerricStore has Raft leader election built in.
- **Embeddable** — Redis is always a separate server. FerricStore runs inside your Elixir app with no network overhead.
- **Disk as a cache tier** — when Redis evicts a key, it's gone. The next access hits your database. When FerricStore evicts from RAM, the key is still on disk.

**Where Redis is stronger:**
- Largest ecosystem — every language has mature Redis client libraries.
- Lua scripting (EVAL/EVALSHA).
- 15+ years of production hardening.
- Redis 8 integrates all data structures (JSON, Search, TimeSeries, probabilistic) into one binary.

## Valkey

Linux Foundation fork of Redis 7.2.4, created after Redis's license change. Backed by AWS, Google, Oracle.

**What FerricStore does differently:**
- Same durability and failover advantages as vs Redis — Valkey inherits Redis's architecture.
- Embeddable in Elixir apps.
- Disk-backed eviction.

**Where Valkey is stronger:**
- Drop-in Redis replacement with I/O threading improvements (claims 230% higher throughput than Redis 7.x).
- Rapidly growing community and development velocity.

## Dragonfly

C++, multi-threaded, shared-nothing architecture. Designed for high single-node throughput.

**What FerricStore does differently:**
- **Durable by default** — Dragonfly uses periodic snapshots, not per-write durability.
- **Automatic failover** — Dragonfly's automated failover requires their commercial Dragonfly Cloud offering (BSL 1.1 license). FerricStore has Raft built in.
- **Probabilistic structures** — FerricStore has Bloom, Cuckoo, CMS, TopK, TDigest, HyperLogLog built in. Dragonfly has partial Bloom support only.
- **Embeddable** — Dragonfly is a standalone server.

**Where Dragonfly is stronger:**
- Much higher single-node throughput (multi-threaded, shared-nothing).
- Lower latency snapshots (no fork).
- Supports both Redis and Memcached protocols.
- Scales vertically to large machines.

## Garnet

C# (.NET), from Microsoft Research. Uses Tsavorite storage engine with tiered memory/SSD/cloud storage.

**What FerricStore does differently:**
- **Durable by default** — Garnet requires explicit AOF configuration for durability.
- **Automatic failover** — built-in via Raft, not dependent on external control plane.
- **Full probabilistic structures** — Bloom, Cuckoo, CMS, TopK, TDigest. Garnet has HLL only.
- **No runtime dependency** — FerricStore runs on the BEAM. Garnet requires the .NET runtime.
- **Embeddable in Elixir** — Garnet's embedded mode is .NET only.

**Where Garnet is stronger:**
- Cloud storage tier — both FerricStore and Garnet tier data across memory and local disk, but Garnet's Tsavorite engine also pages to Azure blob storage. Dataset can exceed local disk capacity.
- Custom C# server-side operators via extensibility framework.
- Very high throughput in benchmarks.

## Kvrocks

C++, disk-based (RocksDB), Apache top-level project. The closest competitor in the "durable Redis-compatible" space.

**What FerricStore does differently:**
- **Consensus** — FerricStore uses Raft for automatic failover and linearizable reads. Kvrocks uses async primary-replica replication with no built-in consensus.
- **Embeddable** — Kvrocks is a standalone C++ server. FerricStore runs inside your Elixir app.
- **Hot/cold tiering** — FerricStore keeps hot keys in ETS (in-memory) for microsecond reads, cold keys on disk. Kvrocks reads always go through RocksDB.
- **Full probabilistic structures** — Bloom, Cuckoo, CMS, TopK, TDigest, HyperLogLog.

**Where Kvrocks is stronger:**
- RocksDB is battle-tested for disk-heavy workloads.
- Lua scripting support.
- No per-key RAM overhead — RocksDB manages all indexing on disk. FerricStore keeps a keydir entry in ETS per key (even cold keys), so key count is bounded by RAM.

## Cachex (Elixir)

In-process ETS-based cache for Elixir applications. Rich feature set: TTL, fallbacks, hooks, transactions, LRU eviction, telemetry.

**What FerricStore adds:**
- **Crash survival** — Cachex data lives in ETS. Kill the process, data is gone. FerricStore persists to disk via Raft + Bitcask.
- **Disk-backed eviction** — Cachex eviction means the key is gone and the next access hits your database. FerricStore eviction means the key moves to disk.
- **No cold start** — Cachex starts empty after a restart. FerricStore recovers all data from disk.
- **Redis protocol** — non-Elixir services can access FerricStore via any Redis client.
- **Data structures** — Hash, List, Set, SortedSet, Stream, Geo, JSON, Bitmap, HyperLogLog, Bloom, Cuckoo, CMS, TopK, TDigest.
- **Distributed consensus** — Raft-based replication with automatic failover.

**Where Cachex is better suited:**
- Pure in-memory cache where persistence doesn't matter.
- Simpler dependency (no Rust NIF compilation).
- Distributed partitioning via consistent hash ring (lower overhead than Raft for cache-only workloads).

## Nebulex (Elixir)

Caching framework with pluggable adapters (ETS, Redis, disk). Ecto-inspired API.

**What FerricStore adds:**
- Same advantages as vs Cachex, plus:
- **Single system** — Nebulex is an abstraction that delegates to another store. FerricStore is the store.
- **No adapter configuration** — durability, replication, and failover are built in, not dependent on which adapter you choose.

**Where Nebulex is better suited:**
- When you want to swap cache backends without changing application code.
- Multi-level caching (L1 local + L2 distributed) with a clean API.
- When you already have Redis/Valkey and want an Elixir abstraction over it.
