# Redis Enterprise vs FerricStore -- Feature Comparison

## Executive Summary

**Redis Enterprise** is a mature, commercially-backed data platform with over a decade of production hardening, billions of dollars in investment, and a massive ecosystem. It excels at multi-region geo-replication (Active-Active with CRDTs), tiered storage (Auto Tiering / Redis on Flash), enterprise support contracts, and a proxy-based cluster architecture that delivers 99.999% uptime.

**FerricStore** is an open-source, Elixir-native key-value store built on the BEAM VM with a Rust NIF storage engine (Bitcask). It targets Elixir/Phoenix teams who want an embedded or standalone Redis-compatible database with Raft consensus, deep BEAM integration, and zero licensing costs. FerricStore is early-stage (v0.1) and cannot yet match Redis Enterprise's breadth, maturity, or operational tooling.

### Who should use which?

| Scenario | Recommendation |
|----------|---------------|
| Multi-region Active-Active geo-replication | Redis Enterprise |
| Large enterprise with compliance/audit requirements | Redis Enterprise |
| Datasets exceeding available RAM (need SSD tiering) | Redis Enterprise |
| Full-text search, vector similarity at scale (>100K vectors) | Redis Enterprise |
| Production Elixir/Phoenix app wanting embedded KV with zero network hops | FerricStore |
| Cost-sensitive project that needs Redis compatibility without licensing fees | FerricStore |
| Team that wants to own and audit the entire storage stack | FerricStore |
| Single-region deployment where BEAM concurrency is a natural fit | FerricStore |
| Need for custom commands (CAS, distributed locks, rate limiting) built in | FerricStore |

---

## Architecture Comparison

### Redis Enterprise

- **Proxy-based cluster**: A multi-threaded, lock-free C proxy runs on each node and routes client connections to the correct shard. Clients connect to the proxy, never directly to shards.
- **Shared-nothing**: Each shard is a standard Redis process. The proxy handles slot routing, failover, and connection multiplexing.
- **Control plane / data plane separation**: Cluster management (node membership, rebalancing, upgrades) runs on a separate control path from the data-serving proxies and shards.
- **Multi-tenant**: A single cluster can host many independent databases, each with its own shard count, replication factor, and module configuration.
- **Zero-downtime upgrades**: Rolling upgrades proceed node by node; the proxy transparently reroutes traffic during shard migration.

### FerricStore

- **Direct connection model**: In standalone mode, clients connect directly via Ranch TCP/TLS. There is no proxy layer -- the BEAM connection process routes commands to the correct shard GenServer.
- **Embedded mode**: FerricStore can run inside any Elixir application as a library dependency with no network listener. The host app calls `FerricStore.set/3`, `FerricStore.get/2`, etc. at ETS lookup latency (~1-5 us for hot reads).
- **Per-shard Raft groups**: Each shard is an independent Raft group (using the `ra` library). In single-node mode, each group has one member with self-quorum. In three-node clusters, each group has three members with majority quorum.
- **Two-tier storage**: ETS (BEAM in-memory tables) serves as a hot cache, backed by a Rust Bitcask NIF for durable persistence. Reads check ETS first (no GenServer roundtrip for hot keys), then fall back to Bitcask on disk.
- **Single-tenant**: One FerricStore instance runs one logical database (with configurable shard count). Multi-tenancy is achieved through namespaces (key prefixes with independent commit windows and durability settings).

### Key Architectural Differences

| Aspect | Redis Enterprise | FerricStore |
|--------|-----------------|-------------|
| Client routing | C proxy (multi-threaded, lock-free) | BEAM process per connection + ETS direct lookup |
| Consensus | Proprietary cluster protocol + gossip | Raft (via `ra` library from RabbitMQ) |
| Storage engine | Redis RDB/AOF + Speedb (SSD tier) | Bitcask (Rust NIF) with append-only log + hint files |
| Concurrency model | Single-threaded per shard (multi-vCPU via proxy) | BEAM preemptive scheduling, dirty NIF schedulers |
| Deployment | Cluster of VMs / containers / bare metal | OTP release or embedded in Elixir app |
| Hot-path reads | Through proxy to shard process | Direct ETS lookup (no process hop for cached keys) |

---

## Feature Matrix

| Feature | Redis Enterprise | FerricStore | Notes |
|---------|-----------------|-------------|-------|
| **Data Structures** | | | |
| Strings | Yes | Yes | Full SET/GET with all options |
| Hashes | Yes | Yes | HSET, HGET, HGETALL, HSCAN, etc. |
| Lists | Yes | Yes | LPUSH, RPUSH, LRANGE, LMOVE, etc. |
| Sets | Yes | Yes | SADD, SINTER, SUNION, SDIFF, etc. |
| Sorted Sets | Yes | Yes | ZADD, ZRANGE, ZINCRBY, etc. |
| Streams | Yes | Yes | XADD, XREAD, XGROUP, XREADGROUP, XACK |
| JSON | Yes (native in Redis 8) | Yes | JSON.SET, JSON.GET, JSONPath subset |
| Bloom filters | Yes (native in Redis 8) | Yes | BF.RESERVE, BF.ADD, BF.EXISTS, etc. |
| Cuckoo filters | Yes (native in Redis 8) | Yes | CF.RESERVE, CF.ADD, CF.DEL, etc. |
| Count-Min Sketch | Yes (native in Redis 8) | Yes | CMS.INITBYDIM, CMS.QUERY, etc. |
| Top-K | Yes (native in Redis 8) | Yes | TOPK.RESERVE, TOPK.ADD, etc. |
| HyperLogLog | Yes | Yes | PFADD, PFCOUNT, PFMERGE |
| Bitmaps | Yes | Yes | SETBIT, GETBIT, BITCOUNT, BITOP |
| Geo | Yes | Yes | GEOADD, GEOSEARCH, GEODIST, etc. |
| Time Series | Yes (native in Redis 8) | No | FerricStore has no time series support |
| t-digest | Yes (native in Redis 8) | No | FerricStore has no t-digest support |
| Graph | Deprecated (RedisGraph EOL) | No | RedisGraph reached end-of-life |
| **Search & Query** | | | |
| Full-text search | Yes (RediSearch, built into Redis 8) | No | Major gap -- RediSearch is very mature |
| Secondary indexes | Yes (RediSearch) | No | FerricStore uses prefix index for SCAN only |
| Aggregation pipelines | Yes (RediSearch) | No | |
| Vector similarity search | Yes (RediSearch, horizontal scaling) | Yes (HNSW with yielding NIF, up to 10M vectors) | Redis Enterprise integrates with RediSearch; FerricStore has native HNSW with cosine/L2/inner_product, auto-threshold (brute-force <1K, HNSW >=1K) |
| **Replication & HA** | | | |
| Primary-replica replication | Yes (async + diskless) | Via Raft log replication | Different mechanisms |
| Automatic failover | Yes (single-digit seconds) | Yes (Raft leader election) | Redis Enterprise is more battle-tested |
| Active-Active geo-replication | Yes (CRDT-based, multi-region) | No | Major differentiator for Redis Enterprise |
| Cross-datacenter replication | Yes | No | FerricStore is single-region only |
| Zero-downtime upgrades | Yes (proxy-based rolling) | Yes (OTP hot code loading) | Both support zero-downtime. Redis uses proxy rerouting; FerricStore uses BEAM's native hot code upgrade (code_change/3, release_handler) — modules upgrade while processes keep running, no connections dropped |
| 99.999% uptime SLA | Yes (Active-Active) | No SLA | FerricStore is pre-1.0 |
| **Storage & Memory** | | | |
| Auto Tiering (RAM + SSD) | Yes (Speedb engine, up to 70% cost savings) | Two-tier: ETS hot cache + Bitcask on disk | Different approach -- FerricStore's hot cache is automatic but not SSD-tiered |
| Eviction policies | Yes (standard Redis policies) | Yes (volatile_lru, allkeys_lru, volatile_ttl, noeviction) | Feature parity on eviction |
| Memory pressure monitoring | Yes | Yes (MemoryGuard with telemetry events) | |
| Max memory enforcement | Yes | Yes (keydir RAM limit + hot cache budget) | |
| Data persistence | RDB snapshots + AOF | Bitcask append-only log + hint files + compaction | Different durability models |
| io_uring support | No (standard fsync) | Yes (Linux, auto-detected) | FerricStore can use io_uring for writes on Linux >= 5.1 |
| **Security** | | | |
| TLS/SSL | Yes (in-transit encryption) | Yes (TLS 1.2/1.3 via Ranch SSL) | |
| Encryption at rest | Yes | No | FerricStore does not encrypt Bitcask files on disk |
| RBAC / ACL | Yes (role-based, LDAP integration) | Yes (ACL with categories: read/write/admin/dangerous) | Redis Enterprise has LDAP; FerricStore has local ACL only |
| Password hashing | Yes | Yes (PBKDF2-SHA256) | |
| Audit logging | Yes (administrative actions) | Yes (auth, config changes, dangerous commands, ACL denials) | |
| Protected mode | Yes | Yes (non-localhost rejected until ACL configured) | |
| LDAP / SAML integration | Yes | No | |
| IP allowlisting | Yes | No | |
| **Observability** | | | |
| Prometheus metrics | Yes (v2 scraping endpoint, cluster/node/db/shard/proxy levels) | Yes (text exposition format, custom FERRICSTORE.METRICS command) | Redis Enterprise has much richer metric hierarchy |
| Grafana dashboards | Yes (official dashboards) | No (metrics are compatible but no dashboards shipped) | |
| Slow log | Yes | Yes | |
| INFO command | Yes | Yes | |
| Health endpoint | N/A (managed via control plane) | Yes (HTTP /health/live and /health/ready for K8s) | |
| HTML dashboard | No (separate Grafana) | Yes (built-in /dashboard endpoint) | |
| Per-prefix metrics | No | Yes (key count, keydir bytes, hot/cold reads per prefix) | Unique to FerricStore |
| Hot/cold read tracking | No | Yes (FERRICSTORE.HOTNESS command) | Unique to FerricStore |
| **Protocol** | | | |
| RESP2 | Yes | No (RESP3 only) | FerricStore rejects HELLO 2 |
| RESP3 | Yes | Yes | |
| Pub/Sub | Yes | Yes (PUBLISH, SUBSCRIBE, PSUBSCRIBE) | |
| Client-side caching | Yes | Yes (CLIENT TRACKING with BCAST, OPTIN, OPTOUT, NOLOOP, REDIRECT) | |
| Keyspace notifications | Yes | Yes | |
| Pipelining | Yes | Yes | |
| MULTI/EXEC transactions | Yes | Yes (including cross-shard 2PC) | |
| WATCH/optimistic locking | Yes | Yes | |
| Blocking commands | Yes | Yes (BLPOP, BRPOP, BLMOVE, BLMPOP) | |
| Cluster protocol (MOVED/ASK) | Yes | No (single-endpoint, internal sharding) | |
| **Operations** | | | |
| Managed cloud offering | Yes (Redis Cloud) | No | |
| Kubernetes operator | Yes | No | |
| Multi-database per cluster | Yes | No (single database, namespaces for isolation) | |
| Online resharding | Yes | No | |
| Backup / restore | Yes (RDB snapshots, scheduled) | Via Bitcask file copy (manual) | |
| Data import/export | Yes (Redis Data Integration) | No | |

---

## Where FerricStore Wins

### 1. Embedded Mode -- Zero Network Hops

FerricStore can run as a library inside any Elixir/Phoenix application. There is no TCP connection, no serialization, no proxy -- the host app calls `FerricStore.set/3` and `FerricStore.get/2` directly. Hot reads resolve in ~1-5 microseconds via ETS lookup. This is fundamentally impossible with Redis Enterprise, which always requires a network hop (even on localhost).

### 2. BEAM Concurrency and Fault Tolerance

FerricStore leverages the BEAM VM's preemptive scheduler, per-process garbage collection, and supervisor trees. Each TCP connection is a lightweight BEAM process (~2 KB). The system can handle hundreds of thousands of concurrent connections without thread pool tuning. Supervision trees automatically restart crashed components (shards, listeners, merge schedulers) without bringing down the node.

### 3. Raft Consensus for Strong Consistency

Redis Enterprise's primary-replica replication is asynchronous -- a write acknowledged by the primary may be lost if the primary crashes before the replica receives it. FerricStore uses Raft consensus (via the `ra` library), meaning writes are not acknowledged until a quorum of nodes has durably logged the command. This provides stronger consistency guarantees out of the box, with configurable per-namespace durability (`:quorum` or `:async`).

### 4. Native Custom Commands

FerricStore ships several commands that require external libraries or Lua scripting in Redis:

- **CAS (Compare-and-Swap)**: Atomic compare-and-swap with optional TTL -- no Lua script needed.
- **LOCK / UNLOCK / EXTEND**: Distributed locking with owner identity and TTL extension, built into the server.
- **RATELIMIT.ADD**: Sliding window rate limiter as a first-class command.
- **FETCH_OR_COMPUTE**: Cache stampede prevention -- the first caller gets a "compute" response; concurrent callers block until the result is stored.
- **FERRICSTORE.HOTNESS**: Per-prefix hot/cold read ratio analysis.
- **FERRICSTORE.CONFIG**: Per-namespace commit window and durability tuning at runtime.

### 5. Cost

FerricStore is open source with no licensing fees, no per-shard pricing, and no cloud markup. Redis Enterprise pricing is per-shard, and costs scale with dataset size and required throughput. For cost-sensitive deployments, FerricStore can run on commodity hardware with no commercial dependency.

### 6. Per-Namespace Durability Tuning

FerricStore allows different key prefixes (namespaces) to have different commit windows and durability modes. For example, session keys can use `:async` durability with a 10ms commit window for low latency, while payment keys use `:quorum` durability with a 1ms window. Redis Enterprise does not offer this granularity -- durability settings apply to the entire database.

### 7. io_uring Support

On Linux >= 5.1, FerricStore's Rust NIF automatically detects and uses `io_uring` for write operations, enabling kernel-level batched I/O without extra system calls. Redis uses standard `fsync`.

### 8. Sandbox Testing Support

FerricStore provides built-in test isolation via `FerricStore.Sandbox`. Each test process gets a transparent namespace prefix, allowing concurrent tests to run against the same FerricStore instance without data contamination. This is analogous to Ecto's sandbox mode and deeply useful for ExUnit-based test suites.

---

## Where Redis Enterprise Wins

### 1. Active-Active Geo-Replication (CRDTs)

This is Redis Enterprise's crown jewel. Active-Active uses conflict-free replicated data types (CRDTs) to allow reads and writes at multiple geographic locations simultaneously, with automatic conflict resolution. FerricStore has no equivalent. For applications that require multi-region write capability with sub-millisecond local latency, Redis Enterprise is the only option.

### 2. Auto Tiering (Redis on Flash)

Redis Enterprise can store hot data in DRAM and warm data on SSDs using the Speedb storage engine, reducing infrastructure costs by up to 70% compared to all-DRAM deployments. FerricStore's Bitcask engine stores all data on disk and caches hot data in ETS, but the ETS cache is not SSD-backed and does not support the same tiered storage model.

### 3. Ecosystem Maturity and Production Hardening

Redis Enterprise has been in production at thousands of companies for over a decade. It has dedicated security teams, compliance certifications (SOC 2, HIPAA, PCI-DSS), and 24/7 enterprise support. FerricStore is pre-1.0 software with no production deployments at scale.

### 4. RediSearch -- Full-Text Search and Indexing

RediSearch (now integrated into Redis 8) provides full-text search, secondary indexes, aggregation pipelines, and vector similarity search at scale (millions of vectors with horizontal scaling). FerricStore has native HNSW vector search (implemented in Rust with yielding NIF for BEAM scheduler safety) supporting up to 10M vectors with cosine, L2, and inner product distance metrics. It auto-selects brute-force for <1K vectors and HNSW for larger collections. However, FerricStore has no full-text search or secondary index capability — this remains a gap.

### 5. Time Series Support

Redis TimeSeries (now integrated into Redis 8) provides purpose-built time series ingestion, downsampling, aggregation, and retention policies. FerricStore has no time series support.

### 6. Proxy-Based Transparent Scaling

Redis Enterprise's proxy layer abstracts away the cluster topology from clients. Applications connect to a single endpoint and the proxy handles routing, failover, and rebalancing transparently. Clients never need to know about individual shards. FerricStore handles sharding internally, but does not support online resharding or transparent cluster topology changes.

### 7. Multi-Database / Multi-Tenant

A single Redis Enterprise cluster can host dozens of independent databases, each with its own shard count, replication factor, module configuration, and resource limits. FerricStore runs one logical database per instance.

### 8. Zero-Downtime Upgrades

Redis Enterprise supports rolling upgrades where nodes are upgraded one at a time with automatic traffic rerouting. FerricStore requires a restart to apply code changes.

### 9. Managed Cloud Offering

Redis Cloud provides a fully managed service on AWS, GCP, and Azure with automated provisioning, scaling, backup, and monitoring. FerricStore has no managed offering.

### 10. LDAP / SAML / External Identity Provider Integration

Redis Enterprise integrates with enterprise identity providers for centralized access control. FerricStore only supports local ACL users.

### 11. Encryption at Rest

Redis Enterprise supports encrypting data on disk. FerricStore's Bitcask files are stored in plaintext.

---

## Unique FerricStore Features

These are capabilities that Redis Enterprise does not offer:

| Feature | Description |
|---------|-------------|
| **Embedded mode** | Run as an in-process Elixir library with no network listener |
| **FerricStore.Sandbox** | Test isolation with transparent per-process key namespacing |
| **Per-namespace durability** | Different commit windows and durability modes per key prefix |
| **FETCH_OR_COMPUTE** | Built-in cache stampede prevention (single-flight pattern) |
| **FERRICSTORE.HOTNESS** | Per-prefix hot/cold read ratio analysis |
| **FERRICSTORE.METRICS** | Prometheus text exposition via Redis command (no separate endpoint needed) |
| **Per-prefix Prometheus metrics** | Key count, keydir bytes, hot/cold reads broken down by key prefix |
| **Hybrid Logical Clock (HLC)** | Monotonic timestamps resilient to NTP drift, piggybacked on Raft heartbeats |
| **BEAM supervision** | Automatic restart of crashed shards, listeners, merge schedulers |
| **Namespace-aware group commit** | Writes are batched per namespace with configurable flush windows |
| **Built-in health dashboard** | HTML dashboard at /dashboard with auto-refresh (no Grafana needed) |
| **CAS command** | Native compare-and-swap (Redis requires Lua scripting) |
| **LOCK/UNLOCK/EXTEND** | Native distributed locking (Redis requires Redlock or Lua) |
| **RATELIMIT.ADD** | Native sliding-window rate limiter |
| **io_uring writes** | Automatic io_uring detection and usage on Linux |

---

## Migration Path

### From Redis Enterprise to FerricStore

FerricStore speaks RESP3 and implements the core Redis command set. Most Redis client libraries (including Elixir's `Redix`) can connect to FerricStore with minimal configuration changes.

**Steps:**

1. **Audit command usage**: Run `COMMAND` or check your application code to verify all commands you use are supported by FerricStore. See the feature matrix above for gaps (RediSearch, TimeSeries, t-digest).

2. **Test with existing client library**: Point your Redis client at FerricStore's port (default 6379). FerricStore supports `CLIENT HELLO 3` for RESP3 negotiation.

3. **Data migration**: Use `redis-cli --pipe` or write a migration script that reads keys from Redis Enterprise and writes them to FerricStore. FerricStore does not support RDB import.

4. **Adjust for RESP3-only**: FerricStore does not support RESP2. If your client library defaults to RESP2, configure it to use RESP3 or upgrade to a library version that supports RESP3.

5. **Replace RediSearch queries**: If you use RediSearch for full-text search or complex queries, you will need an alternative (e.g., PostgreSQL full-text search, Elasticsearch, Meilisearch). This is the most significant migration effort for RediSearch-heavy applications.

6. **Replace Active-Active**: If you use Active-Active geo-replication, there is no direct FerricStore equivalent. You would need to redesign for a single-region architecture or use application-level replication.

**What migrates easily:**
- String, Hash, List, Set, Sorted Set, Stream commands
- Bloom filter, Cuckoo filter, Count-Min Sketch, Top-K commands
- JSON commands
- Pub/Sub
- TTL / expiry
- MULTI/EXEC transactions
- ACL users (need to be recreated via `ACL SETUSER`)

**What does not migrate:**
- RediSearch indexes and queries
- TimeSeries data and downsampling rules
- Active-Active CRDT databases
- RDB/AOF snapshot files (different on-disk format)
- Cluster protocol (MOVED/ASK) -- FerricStore uses internal sharding
- Lua scripts (FerricStore does not support EVAL/EVALSHA)

### From FerricStore to Redis Enterprise

Migration in this direction is straightforward for the core command set. FerricStore's native commands (CAS, LOCK, RATELIMIT.ADD, FETCH_OR_COMPUTE) would need to be reimplemented as Lua scripts or replaced with Redis modules.

---

## Cost Comparison

### Redis Enterprise

- **Redis Cloud**: Per-shard pricing based on shard type (memory, throughput tier), cloud provider, and region. Five shard types with different RAM and throughput characteristics. Active-Active databases incur additional per-shard costs at each participating region.
- **Redis Software (self-managed)**: Annual subscription priced per shard. A "shard" includes both primary and replica processes. Pricing scales with shard count, which scales with dataset size and throughput requirements.
- **Typical cost**: For a production deployment with 25 GB of data, high availability, and moderate throughput, expect $500-$2,000+/month on Redis Cloud depending on provider and region. Enterprise support contracts add additional cost.

### FerricStore

- **License**: Open source, no licensing fees.
- **Infrastructure only**: You pay only for the compute and storage you provision (EC2 instances, EBS volumes, etc.).
- **Typical cost**: A single c6i.xlarge (4 vCPU, 8 GB RAM) on AWS at ~$125/month can run a FerricStore instance with 8 GB ETS cache and hundreds of GB of Bitcask data on attached SSD. Three-node Raft clusters cost ~$375/month for infrastructure alone.
- **Operational cost**: Self-managed. You are responsible for monitoring, upgrades, backups, and incident response. There is no vendor support contract available.

### Cost Summary

| Aspect | Redis Enterprise | FerricStore |
|--------|-----------------|-------------|
| License fee | Per-shard annual subscription | Free (open source) |
| Cloud managed | Yes (Redis Cloud) | No |
| Infrastructure | Included in managed; self-provisioned for on-prem | Self-provisioned |
| Support | 24/7 enterprise support available | Community only |
| Total for 25 GB production | ~$500-$2,000+/month | ~$125-$375/month (infra only) |

**Bottom line**: FerricStore can be 4-10x cheaper in direct infrastructure costs, but you trade vendor support, managed operations, and the advanced features (Active-Active, RediSearch, Auto Tiering) that justify Redis Enterprise's pricing.

---

## Honest Assessment

FerricStore is a young project (v0.1) built by a small team. It implements an impressive breadth of Redis commands and data structures, but it lacks the production hardening, operational tooling, and advanced features that come from Redis Enterprise's decade of development and billions in investment.

**Choose Redis Enterprise if**: You need Active-Active geo-replication, full-text search, managed operations, compliance certifications, SSD tiering for cost optimization, or enterprise support. These are capabilities that FerricStore cannot provide today and may not provide for years.

**Choose FerricStore if**: You are building an Elixir application that benefits from embedded mode, you want strong consistency via Raft, you need custom commands (CAS, locks, rate limiting) without Lua scripting, and you are comfortable operating your own infrastructure. The cost savings and BEAM integration can be compelling for the right use case.

---

## Sources

- [Redis Enterprise Cluster Architecture](https://redis.io/technology/redis-enterprise-cluster-architecture/)
- [Active-Active Geo-Distribution](https://redis.io/active-active/)
- [Active-Active Geo-Distributed Redis Documentation](https://redis.io/docs/latest/operate/rs/databases/active-active/)
- [Under the Hood: Redis CRDTs](https://redis.io/resources/under-the-hood/)
- [Redis Auto Tiering](https://redis.io/auto-tiering/)
- [Redis Auto Tiering Documentation](https://redis.io/docs/latest/operate/rs/databases/flash/)
- [Redis Enterprise Pricing](https://redis.io/enterprise/pricing/)
- [Redis Enterprise Pricing Calculator](https://redis.io/pricing/calculator/)
- [Redis Enterprise Security](https://redis.io/technology/enterprise-grade-redis-security/)
- [Redis Enterprise High Availability](https://redis.io/technology/highly-available-redis/)
- [Redis Enterprise Advantages vs Open Source](https://redis.io/redis-enterprise/advantages/)
- [Redis Enterprise vs Open Source Comparison](https://redis.io/compare/open-source/)
- [Redis Enterprise Software Observability](https://redis.io/docs/latest/integrate/prometheus-with-redis-enterprise/observability/)
- [Redis Enterprise Prometheus Metrics v2](https://redis.io/docs/latest/integrate/prometheus-with-redis-enterprise/prometheus-metrics-definitions/)
- [Redis 8 GA Announcement](https://redis.io/blog/redis-8-ga/)
- [Redis Open Source 8.0 Release Notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.0-release-notes/)
- [Redis Enterprise 200M ops/sec Benchmark](https://redis.io/blog/redis-enterprise-extends-linear-scalability-200m-ops-sec/)
- [Redis Enterprise Feature Compatibility](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/enterprise-capabilities/)
