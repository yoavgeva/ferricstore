# FerricStore ACL Design -- v3

## Philosophy

Redis ACL is good but has quirks from 30 years of backward compatibility. FerricStore
can do better because we start fresh. Our ACL must be:

1. **Secure by default** -- protected mode, bcrypt, no plaintext passwords
2. **Redis-compatible** -- same command syntax, same error messages, clients just work
3. **Raft-replicated** -- ACL changes go through consensus, all nodes agree
4. **Namespace-aware** -- key patterns integrate with our prefix system
5. **Auditable** -- every denial logged with context

---

## 1. User Model

```
User {
  username:     binary()           # unique, case-sensitive
  enabled:      boolean()          # on/off
  passwords:    [bcrypt_hash]      # list -- supports rotation
  key_patterns: [glob_pattern]     # ~pattern, empty = no access
  channel_patterns: [glob_pattern] # &pattern, empty = no access
  commands:     MapSet | :all      # allowed commands
  denied_commands: MapSet          # explicit denials (subtracted from :all)
  flags:        [:nopass | ...]    # special flags
}
```

### Improvement over Redis:
- **Passwords are bcrypt-hashed at ingestion** -- never stored plaintext, not even temporarily
- **Separate `denied_commands` set** -- fixes the Redis bug where `-command` after `+@all` is ignored. We track denials separately: allowed = (commands or :all) - denied_commands
- **List of password hashes** -- supports zero-downtime rotation natively

### Default user:
```
user default on nopass ~* &* +@all
```
Same as Redis. Must be locked down before production.

---

## 2. ACL SETUSER Syntax

Fully Redis-compatible:

```
ACL SETUSER username [rule ...]
```

### Rules (all supported):

| Rule | Action | Notes |
|------|--------|-------|
| `on` | Enable user | |
| `off` | Disable user | Existing connections stay alive (Redis compat) |
| `>password` | Add password (bcrypt-hashed internally) | Additive -- multiple passwords supported |
| `<password` | Remove specific password | Compares bcrypt hash |
| `#hash` | Add pre-hashed password (bcrypt) | For config-file provisioning |
| `!hash` | Remove pre-hashed password | |
| `nopass` | Allow auth without password | Clears all passwords |
| `resetpass` | Clear all passwords, require re-auth | |
| `~pattern` | Add key glob pattern | `~*` = all keys, `~cache:*` = prefix |
| `%R~pattern` | Read-only key pattern | FerricStore extension |
| `%W~pattern` | Write-only key pattern | FerricStore extension |
| `resetkeys` | Clear all key patterns | |
| `allkeys` | Alias for `~*` | |
| `&pattern` | Add channel glob pattern | For Pub/Sub restrictions |
| `resetchannels` | Clear all channel patterns | |
| `allchannels` | Alias for `&*` | |
| `+command` | Allow command | Case-insensitive |
| `-command` | Deny command | **Works even after +@all** |
| `+@category` | Allow category | |
| `-@category` | Deny category | **Works even after +@all** |
| `+command\|subcommand` | Allow subcommand | e.g., `+config\|get` |
| `-command\|subcommand` | Deny subcommand | e.g., `-config\|set` |
| `allcommands` | Alias for `+@all` | |
| `nocommands` | Alias for `-@all` | |
| `reset` | Full reset: off, no passwords, no commands, no keys, no channels | |

---

## 3. Command Categories

FerricStore maps **every command** to categories. Redis has 20+ categories. We match
all of them and add our own:

### Standard Redis categories:

| Category | Commands |
|----------|----------|
| `@read` | GET, MGET, HGET, HGETALL, HMGET, LRANGE, LINDEX, LLEN, SMEMBERS, SISMEMBER, SCARD, ZSCORE, ZRANK, ZRANGE, ZCOUNT, XRANGE, XREAD, XLEN, EXISTS, TTL, PTTL, TYPE, DBSIZE, RANDOMKEY, OBJECT, STRLEN, GETRANGE, LPOS, SRANDMEMBER, ZRANDMEMBER, GEODIST, GEOPOS, GEOHASH, GEOSEARCH, PFCOUNT |
| `@write` | SET, MSET, DEL, HSET, HDEL, LPUSH, RPUSH, LPOP, RPOP, SADD, SREM, ZADD, ZREM, XADD, XTRIM, XDEL, APPEND, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, SETRANGE, EXPIRE, PEXPIRE, PERSIST, RENAME, COPY, LSET, LINSERT, LTRIM, LREM, LMOVE, SPOP, SMOVE, ZINCRBY, GEOADD, PFADD, PFMERGE, SETNX, SETEX, PSETEX, MSETNX, GETSET, GETDEL, GETEX, UNLINK, RPOPLPUSH |
| `@string` | GET, SET, MGET, MSET, APPEND, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, STRLEN, GETRANGE, SETRANGE, SETNX, SETEX, PSETEX, MSETNX, GETSET, GETDEL, GETEX |
| `@hash` | HSET, HGET, HDEL, HMGET, HGETALL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD, HSCAN, HEXPIRE, HTTL, HPERSIST, HGETDEL, HGETEX, HSETEX, HPEXPIRE, HPTTL, HEXPIRETIME |
| `@list` | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LINSERT, LTRIM, LREM, LPOS, LMOVE, LPUSHX, RPUSHX, RPOPLPUSH, BLPOP, BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH |
| `@set` | SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SRANDMEMBER, SPOP, SDIFF, SINTER, SUNION, SDIFFSTORE, SINTERSTORE, SUNIONSTORE, SINTERCARD, SMOVE, SSCAN |
| `@sortedset` | ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZCARD, ZINCRBY, ZCOUNT, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZSCAN, ZMSCORE, ZRANGEBYSCORE, ZREVRANGEBYSCORE |
| `@bitmap` | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP |
| `@hyperloglog` | PFADD, PFCOUNT, PFMERGE |
| `@geo` | GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE |
| `@stream` | XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XINFO, XGROUP, XREADGROUP, XACK |
| `@pubsub` | PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB |
| `@transaction` | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| `@connection` | AUTH, HELLO, QUIT, RESET, PING, ECHO, SELECT, CLIENT |
| `@server` | INFO, DBSIZE, KEYS, SCAN, FLUSHDB, FLUSHALL, CONFIG, COMMAND, SAVE, BGSAVE, LASTSAVE, SLOWLOG, DEBUG, LOLWUT, WAIT, WAITAOF, MEMORY |
| `@generic` | DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, COPY, UNLINK, RANDOMKEY, OBJECT, SCAN, KEYS, SORT, EXPIRETIME, PEXPIRETIME |
| `@keyspace` | Same as `@generic` (alias) |
| `@fast` | GET, SET, HGET, HSET, SADD, SISMEMBER, ZADD, ZSCORE, LPUSH, RPUSH, LPOP, RPOP, EXISTS, TTL, INCR, DECR, APPEND, PING, ECHO |
| `@slow` | KEYS, SCAN, SORT, SMEMBERS, HGETALL, LRANGE, ZRANGE, XRANGE, XREAD, FLUSHDB, FLUSHALL, SAVE, BGSAVE, DEBUG |
| `@blocking` | BLPOP, BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH, XREAD (with BLOCK) |
| `@dangerous` | FLUSHALL, FLUSHDB, DEBUG, CONFIG, KEYS, SORT, SHUTDOWN, XAUTOCLAIM, ACL |

### FerricStore-specific categories:

| Category | Commands |
|----------|----------|
| `@admin` | CONFIG, ACL, DEBUG, FLUSHDB, FLUSHALL, SLOWLOG, SAVE, BGSAVE, SHUTDOWN, MEMORY, COMMAND |
| `@sandbox` | FERRICSTORE.SANDBOX.START, FERRICSTORE.SANDBOX.JOIN, FERRICSTORE.SANDBOX.END, FERRICSTORE.SANDBOX.TOKEN |
| `@ferricstore` | FERRICSTORE.CONFIG, FERRICSTORE.KEY_INFO, FERRICSTORE.HOTNESS, FERRICSTORE.METRICS, CLUSTER.HEALTH, CLUSTER.STATS |
| `@vector` | VCREATE, VADD, VGET, VDEL, VSEARCH, VINFO, VLIST, VEVICT |
| `@probabilistic` | BF.RESERVE, BF.ADD, BF.EXISTS, CF.RESERVE, CF.ADD, CF.DEL, CF.EXISTS, CMS.INITBYDIM, CMS.QUERY, TOPK.RESERVE, TOPK.ADD, TOPK.QUERY |
| `@native` | CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD, FETCH_OR_COMPUTE |

---

## 4. Key Pattern Enforcement

**This is the biggest missing piece.** Every command that touches keys must check
if the authenticated user's key patterns allow access.

### How it works:

```
dispatch(cmd, args, state) ->
  1. Extract keys from command (using Catalog key positions)
  2. For each key, check against user's patterns:
     - If any ~pattern matches: allowed
     - If %R~pattern matches and command is read-only: allowed
     - If %W~pattern matches and command is write: allowed
     - If no pattern matches: NOPERM
  3. Proceed to command execution
```

### Key extraction:
Use the command Catalog's `first_key`, `last_key`, `step` metadata to extract
keys from any command. For example:
- `SET key value` -> keys: [key] (position 1)
- `MSET k1 v1 k2 v2` -> keys: [k1, k2] (positions 1, 3, 5...)
- `DEL k1 k2 k3` -> keys: [k1, k2, k3] (all args)

### Namespace integration:
FerricStore's prefix system (`session:*`, `rate:*`) maps naturally to ACL key
patterns. Production recommendation:

```
ACL SETUSER session_service on >pass ~session:* +@read +@write -@dangerous
ACL SETUSER rate_service on >pass ~rate:* +@read +RATELIMIT.ADD -@dangerous
ACL SETUSER cache_service on >pass ~cache:* +@read +@write +@string -@dangerous
```

Each microservice can only touch its own namespace.

---

## 5. Protected Mode

**Improvement over Redis:**

Redis protected mode only checks if the connection is from localhost. Once
`requirepass` is set, any IP can connect.

FerricStore protected mode requires **at least one non-default ACL user with
a password** before accepting non-localhost connections. This prevents the
common mistake of setting a weak requirepass and exposing the server.

```
# Startup check:
if protected_mode == true and no_configured_users?() do
  # Only accept connections from 127.0.0.1
  # Reject all others with:
  # "DENIED FerricStore is in protected mode. Configure ACL users
  #  or set protected-mode no to allow external connections."
end
```

---

## 6. Password Security

**Improvement over Redis:**

Redis uses SHA-256 for password hashing. SHA-256 is fast -- an attacker with
GPU hardware can try billions of hashes per second.

FerricStore uses **bcrypt** (cost factor 12, ~250ms per hash). This makes
brute-force attacks impractical even with leaked password hashes.

### Password lifecycle:
```
>password  ->  bcrypt(password, cost=12)  ->  stored in ETS
<password  ->  bcrypt(password, cost=12)  ->  compare, remove if match
#hash      ->  stored directly (must be valid bcrypt hash)
!hash      ->  remove matching hash
```

### Authentication:
```
AUTH username password ->
  hashes = user.passwords  # list of bcrypt hashes
  Enum.any?(hashes, fn h -> Bcrypt.verify_pass(password, h) end)
```

### Multi-password rotation:
```
# Add new password (both valid during transition)
ACL SETUSER app >new_strong_password

# App deploys with new password
# Remove old password after deploy completes
ACL SETUSER app <old_password
```

---

## 7. ACL Persistence to Disk

### File location

ACL rules are saved to `data_dir/acl.conf`. This file sits at the root of the
data directory alongside the `data/`, `raft/`, and other subdirectories:

```
data_dir/
  acl.conf                          <-- ACL persistence file
  data/shard_0/ ... shard_N/
  dedicated/shard_0/ ... shard_N/
  prob/shard_0/ ... shard_N/
  vectors/shard_0/ ... shard_N/
  raft/shard_0/ ... shard_N/
  registry/
  hints/
```

### File format

One user definition per line, matching the Redis `users.acl` format. Passwords
are always stored as bcrypt hashes (the `#hash` form), never as plaintext:

```
user default on nopass ~* &* +@all
user app on #$2b$12$LJ3m4ys1rGKH8W5pOmT0c.8vRUmOkFbjEqKSzuYVsFGhKpaHistXi ~cache:* ~session:* &* +@read +@write -@dangerous
user admin on #$2b$12$W.Dth6vPkX9QRn7HNKf5N.rXplVnQF8xGzVbMih5e7oj2eXsLkvia ~* &* +@all
user reader on #$2b$12$Ab3dEfGhIjKlMnOpQrStU.vWxYz0123456789AbCdEfGhIjKlMnOp ~* &* +@read
user ci_job_456 off #$2b$12$... ~ci:456:* &* +@all -@dangerous
```

Key constraints:
- Lines starting with `#` (without the `$2b$` bcrypt prefix context) are comments
- Empty lines are ignored
- The parser rejects any line containing `>password` (plaintext) -- only `#hash` is accepted in the file
- Invalid lines cause `ACL LOAD` to abort with an error (no partial load)

### ACL SAVE

Serializes the current in-memory ACL state to `data_dir/acl.conf`:

```elixir
def handle_acl_save(data_dir) do
  lines =
    :ferricstore_acl
    |> :ets.tab2list()
    |> Enum.sort_by(fn {name, _} -> name end)
    |> Enum.map(&format_user_for_file/1)
    |> Enum.join("\n")

  path = Path.join(data_dir, "acl.conf")
  tmp = path <> ".tmp"

  # Atomic write: write to tmp, fsync, rename
  File.write!(tmp, lines <> "\n")
  :ok = File.rename(tmp, path)
end
```

The write uses a tmp-then-rename pattern to prevent corruption if the process
crashes mid-write.

**Raft replication of ACL SAVE:** When a client issues `ACL SAVE`, the command
is replicated through Raft as `{:acl_save}`. Each node's `apply/3` callback
writes its own local `acl.conf`. This ensures all nodes persist the same ACL
state to disk, even though the files are written locally.

### ACL LOAD

Reads `data_dir/acl.conf` and replaces the in-memory ACL state:

```elixir
def handle_acl_load(data_dir) do
  path = Path.join(data_dir, "acl.conf")

  case File.read(path) do
    {:ok, contents} ->
      users = parse_acl_file(contents)
      # Validate all lines before applying (fail-fast)
      :ets.delete_all_objects(:ferricstore_acl)
      Enum.each(users, fn {name, rules} ->
        :ets.insert(:ferricstore_acl, {name, rules})
      end)
      :ok

    {:error, :enoent} ->
      {:error, "ERR There is no ACL file to load"}
  end
end
```

Active connections are NOT terminated on `ACL LOAD`. They keep their current
permissions until they re-authenticate. This matches Redis behavior.

### Startup sequence

On node startup, the ACL subsystem follows this order:

1. **Initialize default user** -- Create the `default` user with `on nopass ~* &* +@all`
2. **Load from file** -- If `data_dir/acl.conf` exists, parse and apply it (replaces the default user if the file defines one)
3. **Apply Raft state** -- If this node is joining an existing cluster, the Raft log replay applies any `acl_setuser` / `acl_deluser` commands that occurred after the last `ACL SAVE`

This means the file is the baseline, and Raft is the source of truth for
uncommitted-to-disk changes. After a clean shutdown where `ACL SAVE` was called,
Raft replay is a no-op (the file already reflects the latest state).

### Why this ordering matters

Consider a node that crashes before `ACL SAVE`:

```
t=0  ACL SETUSER app on >pass ~app:*    # Raft-replicated, in memory
t=1  Node crashes                         # acl.conf still has old state
t=2  Node restarts
t=3  Load acl.conf                        # old state restored
t=4  Raft replay                          # re-applies the SETUSER from t=0
t=5  In-memory state is correct           # matches the rest of the cluster
```

---

## 8. Separate ETS Tables Architecture

FerricStore uses named ETS tables extensively. Rather than mixing concerns in
ad-hoc tables, we define a clear separation of responsibilities. Each table has
a defined access pattern, replication strategy, and persistence model.

### Global tables (cluster-wide)

| Table Name | Type | Access | Replicated | Persistent | Purpose |
|------------|------|--------|------------|------------|---------|
| `:ferricstore_acl` | `:set` | `:public` | Raft | File (`acl.conf`) | ACL user definitions (username -> rules, passwords, patterns) |
| `:ferricstore_config` | `:set` | `:public` | Raft | File (`ferricstore.conf` via CONFIG REWRITE) | Runtime CONFIG SET values (maxmemory, hz, etc.) |
| `:ferricstore_ns_config` | `:set` | `:public` | Raft | File (embedded in config) | Namespace config (per-prefix window size, durability mode) |

### Node-local tables

| Table Name | Type | Access | Replicated | Persistent | Purpose |
|------------|------|--------|------------|------------|---------|
| `:ferricstore_config_local` | `:set` | `:public` | No | No | Node-local CONFIG SET values (log_level, etc.) |
| `:ferricstore_hotness` | `:set` | `:public` | No | No | Per-prefix hotness scores and read counters |
| `:ferricstore_slowlog` | `:ordered_set` | `:public` | No | No | Slow query log ring buffer |
| `:ferricstore_tracking` | `:bag` | `:public` | No | No | Client tracking: key -> [conn_pid] mappings |
| `:ferricstore_tracking_connections` | `:set` | `:public` | No | No | Client tracking: conn_pid -> tracking config |
| `:ferricstore_waiters` | `:set` | `:public` | No | No | Blocking command waiters (BLPOP, BRPOP, etc.) |
| `:ferricstore_compute_locks` | `:set` | `:protected` | No | No | FETCH_OR_COMPUTE in-flight computation locks |
| `:ferricstore_stream_waiters` | `:set` | `:public` | No | No | XREAD BLOCK waiters |
| `:ferricstore_audit_log` | `:ordered_set` | `:public` | No | No | ACL audit log ring buffer (denied commands, auth failures) |
| `:ferricstore_metrics` | `:set` | `:public` | No | No | Metrics counters and gauges (exported to Prometheus) |
| `:ferricstore_stats` | `:set` | `:public` | No | No | Stats counters (keyspace hits/misses, total connections, ops/sec) |

### Per-shard tables

| Table Pattern | Type | Access | Replicated | Persistent | Purpose |
|---------------|------|--------|------------|------------|---------|
| `:keydir_N` | `:set` | `:public` | Raft (via Bitcask) | Bitcask data files | Key directory: key -> {file_id, offset, size, tstamp} |
| `:hot_cache_N` | `:set` | `:public` | No | Hint files | Hot value cache: key -> value (frequently accessed entries) |
| `:prefix_keys_N` | `:set` | `:public` | No | No | Per-prefix key tracking for namespace operations |
| `:stream_meta_N` | `:set` | `:public` | Raft | Bitcask | Stream metadata: stream_key -> {last_id, length, ...} |
| `:stream_groups_N` | `:set` | `:public` | Raft | Bitcask | Stream consumer groups: {stream_key, group} -> state |

Where `N` is the zero-based shard index (0, 1, 2, ...).

### Access level rationale

- **`:public`** -- Most tables are public because multiple processes need
  concurrent read access. The connection process reads `:ferricstore_acl` on
  every command dispatch. The health dashboard reads shard tables for metrics.
  Public with `read_concurrency: true` gives the best throughput for
  read-heavy workloads.

- **`:protected`** -- Used for `:ferricstore_compute_locks` because only the
  owning GenServer should write; other processes only read to check lock status.

- **No `:private` tables** -- Private ETS tables die with the owning process.
  Since FerricStore uses supervision trees with restart strategies, private
  tables would be lost on process restart. Named public/protected tables
  survive process restarts as long as the table owner is re-created.

### Replication strategy rationale

- **Raft-replicated** -- Data that must be consistent across all cluster nodes:
  ACL rules (security boundary), cluster-wide config, namespace config, and
  the actual key-value data in Bitcask.

- **Node-local** -- Data that is inherently per-node: local config overrides,
  metrics/stats counters, client tracking state (connections are per-node),
  blocking command waiters (the waiting client is connected to this node).

### Persistence rationale

- **File-persistent** -- ACL rules (security-critical, must survive restart),
  cluster config (operational settings), and key-value data (Bitcask).

- **Ephemeral** -- Metrics, stats, tracking, waiters, audit log. These are
  rebuilt naturally as clients reconnect and traffic flows. The audit log
  could optionally be persisted in a future version, but the ring buffer
  design means old entries are evicted anyway.

---

## 9. ACL LOG

Enhanced over Redis:

```
ACL LOG entry format:
{
  id:          integer,
  timestamp:   unix_ms,
  event_type:  :auth_failure | :command_denied | :key_denied | :channel_denied,
  username:    binary,
  client_ip:   binary,
  client_id:   integer,
  command:     binary,       # the denied command
  key:         binary | nil, # the denied key (for key_denied)
  reason:      binary,       # human-readable
  count:       integer       # deduplicated count for repeated events
}
```

### Deduplication:
Same (event_type, username, command, key) within 60 seconds increments
`count` instead of creating a new entry. This prevents log spam from
misconfigured clients retrying denied commands.

---

## 10. ACL GENPASS

```
ACL GENPASS [bits]
```
Generates a cryptographically random password using `:crypto.strong_rand_bytes/1`.
Default: 256 bits (64 hex chars). Returns the plaintext password -- the user
must securely store it.

---

## 11. Differences from Redis

| Area | Redis | FerricStore | Why |
|------|-------|-------------|-----|
| Password hash | SHA-256 | bcrypt (cost 12) | GPU-resistant |
| Protected mode | Checks localhost only | Requires configured ACL user | Stronger default |
| rename-command | Supported | Not supported | ACL is strictly better |
| ACL replication | None (per-node) | Raft-replicated | Cluster consistency |
| Key patterns | Glob only | Glob + namespace-aware | Integrates with prefix system |
| ACL LOG | 4 event types | 4 types + dedup + context | Better operational insight |
| `+@all -command` | Broken (silent no-op) | Works correctly | Separate denied_commands set |
| Selectors | Redis 7.0+ | Not in v1 | Complexity vs value |

---

## 12. User Stories

The following user stories are derived from real-world Redis ACL usage patterns
observed in production SaaS platforms, microservice architectures, and
compliance-driven environments. Each story drives specific ACL features and
informs the acceptance test suite.

### US-1: SaaS Multi-tenant Isolation

**As a** SaaS platform operator,
**I want** each tenant's microservice to authenticate with its own ACL user
restricted to a key prefix (`tenant:<id>:*`),
**so that** tenant A cannot read, write, or enumerate tenant B's data, even if
application code has a bug.

**Setup:**
```
ACL SETUSER tenant_100 on >t100_secret ~tenant:100:* +@read +@write -@dangerous
ACL SETUSER tenant_200 on >t200_secret ~tenant:200:* +@read +@write -@dangerous
```

**Behavior:**
- `tenant_100` can `SET tenant:100:session abc` -- succeeds
- `tenant_100` can `GET tenant:200:session` -- NOPERM (key pattern mismatch)
- `tenant_100` can `KEYS tenant:200:*` -- returns empty (cannot enumerate other tenant's keys)
- `tenant_100` can `FLUSHDB` -- NOPERM (dangerous commands denied)

**Why this matters:** In multi-tenant Redis deployments, the most common security
failure is application-level key isolation bugs. A typo in a tenant ID, a
cache key collision, or a missing prefix check can expose one tenant's data to
another. ACL key patterns enforce isolation at the database layer, making it
impossible for application bugs to cross tenant boundaries.

---

### US-2: Read Replica Application

**As a** developer of a read-heavy application (90% reads, 10% writes),
**I want** my application's read path to connect with a `reader` user that
has only `+@read` permissions,
**so that** a bug in the read path cannot accidentally mutate data.

**Setup:**
```
ACL SETUSER reader on >read_pass ~* +@read -@write -@admin -@dangerous
ACL SETUSER writer on >write_pass ~* +@read +@write -@dangerous
ACL SETUSER admin on >admin_pass ~* &* +@all
```

**Behavior:**
- `reader` can `GET key` and `MGET k1 k2` -- succeeds
- `reader` can `SET key value` -- NOPERM
- `reader` can `DEL key` -- NOPERM
- `writer` can `SET key value` -- succeeds
- `writer` can `FLUSHDB` -- NOPERM
- `admin` can `FLUSHDB` -- succeeds

**Why this matters:** Defense in depth. Even if the read path has a code injection
vulnerability, the attacker cannot write data or run admin commands. The blast
radius is limited to data the application already reads.

---

### US-3: Rate Limiter Service

**As a** rate limiter microservice,
**I only need** `RATELIMIT.ADD` and `GET` on keys prefixed with `rate:`,
**so that** a compromise of the rate limiter cannot escalate to FLUSHDB or
reading session tokens.

**Setup:**
```
ACL SETUSER ratelimiter on >rl_secret ~rate:* +RATELIMIT.ADD +GET +TTL -@all
```

**Behavior:**
- `ratelimiter` can `RATELIMIT.ADD rate:api:user:42 10 60` -- succeeds
- `ratelimiter` can `GET rate:api:user:42` -- succeeds
- `ratelimiter` can `FLUSHDB` -- NOPERM
- `ratelimiter` can `GET session:abc` -- NOPERM (key pattern mismatch)
- `ratelimiter` can `SET rate:api:user:42 0` -- NOPERM (SET not in allowed commands)

**Why this matters:** Least-privilege principle. The rate limiter is a high-QPS
service often exposed to untrusted input (user IDs, IP addresses). Minimal
permissions limit the damage if the service is compromised.

---

### US-4: Session Store with Expiry

**As a** session management service,
**I need** to store sessions at `session:*` with TTLs, using only SET, GET, DEL,
EXPIRE, and TTL,
**so that** the session service cannot access other namespaces or run admin commands.

**Setup:**
```
ACL SETUSER sessions on >sess_pass ~session:* +SET +GET +DEL +EXPIRE +TTL +PTTL -@all
```

**Behavior:**
- `sessions` can `SET session:abc123 '{"user":42}' EX 3600` -- succeeds
- `sessions` can `GET session:abc123` -- succeeds
- `sessions` can `DEL session:abc123` -- succeeds
- `sessions` can `EXPIRE session:abc123 7200` -- succeeds
- `sessions` can `GET cache:homepage` -- NOPERM (key pattern mismatch)
- `sessions` can `CONFIG GET maxmemory` -- NOPERM
- `sessions` can `KEYS *` -- NOPERM

**Why this matters:** Session tokens are high-value targets. Restricting the
session service to only its own namespace prevents a compromised session service
from pivoting to other data stores (caches, rate limits, user profiles).

---

### US-5: CI/CD Pipeline Test Isolation

**As a** CI/CD pipeline,
**I want** each test job to get a temporary ACL user scoped to a unique prefix,
**so that** parallel test runs cannot interfere with each other or with production
data.

**Workflow:**
```
# Before test job starts (run by CI orchestrator with admin user):
ACL SETUSER ci_job_7890 on >token_abc123 ~ci:7890:* +@all -@dangerous

# Test job connects as ci_job_7890, runs tests against ci:7890:* keys
SET ci:7890:test_key "value"
GET ci:7890:test_key
DEL ci:7890:test_key

# After test job completes (cleanup):
ACL DELUSER ci_job_7890
DEL ci:7890:*
```

**Behavior:**
- `ci_job_7890` can access `ci:7890:*` keys -- succeeds
- `ci_job_7890` can access `ci:7891:*` keys -- NOPERM
- `ci_job_7890` can access production keys -- NOPERM
- `ci_job_7890` can `FLUSHDB` -- NOPERM (dangerous denied)
- After `ACL DELUSER`, the user cannot authenticate

**Why this matters:** Shared test infrastructure is common. Without ACL isolation,
a test that runs `FLUSHDB` (or a key naming collision) can break other test
runs. Temporary users with prefix-scoped access create true sandboxes.

---

### US-6: Microservice Architecture

**As an** operator of a microservice architecture where multiple services share
one FerricStore cluster,
**I want** each service to have its own ACL user with permissions tailored to its
data access pattern,
**so that** a bug or compromise in one service cannot affect another service's data.

**Setup:**
```
ACL SETUSER cart-service on >cart_pass ~cart:* +@read +@write +@hash -@dangerous
ACL SETUSER inventory-service on >inv_pass ~inventory:* +@read +@write +@string +INCR +DECR -@dangerous
ACL SETUSER analytics-service on >analytics_pass ~* +@read -@write -@admin -@dangerous
ACL SETUSER ops-admin on >ops_pass ~* &* +@all
```

**Behavior:**
- `cart-service` can `HSET cart:user:42 item_1 3` -- succeeds
- `cart-service` can `GET inventory:sku:100` -- NOPERM (wrong prefix)
- `inventory-service` can `INCR inventory:sku:100:stock` -- succeeds
- `inventory-service` can `HSET cart:user:42 item_1 5` -- NOPERM (wrong prefix)
- `analytics-service` can `GET cart:user:42` -- succeeds (read-only, all keys)
- `analytics-service` can `SET cart:user:42 hack` -- NOPERM (no write)
- `ops-admin` can do anything -- full access for operations team

**Why this matters:** Shared Redis clusters are the norm in microservice
architectures. Without per-service ACL users, any service can read or corrupt
any other service's data. This is the single biggest operational risk in shared
Redis deployments. ACL key patterns eliminate it.

---

### US-7: Password Rotation Without Downtime

**As an** operations engineer,
**I want** to rotate passwords without any downtime or failed authentication
windows,
**so that** credential rotation can happen on a regular schedule (e.g., every
90 days) without coordinating a synchronized deploy.

**Workflow:**
```
# Step 1: Add new password (both old and new are valid)
ACL SETUSER app >new_password_2026Q2

# Step 2: Deploy application with new password
# (rolling deploy -- some pods use old password, some use new)
# Both passwords work simultaneously during the rollout

# Step 3: After deploy completes, remove old password
ACL SETUSER app <old_password_2026Q1

# Step 4: Persist to disk
ACL SAVE
```

**Behavior:**
- After step 1: `AUTH app old_password_2026Q1` -- succeeds
- After step 1: `AUTH app new_password_2026Q2` -- succeeds
- After step 3: `AUTH app old_password_2026Q1` -- WRONGPASS
- After step 3: `AUTH app new_password_2026Q2` -- succeeds

**Why this matters:** Credential rotation is a compliance requirement in many
organizations (SOC 2, PCI DSS). Redis's single-password model makes rotation
painful -- there is always a window where some instances have the old password
and some have the new one, causing auth failures. FerricStore's multi-password
support eliminates this window entirely.

---

### US-8: Audit Compliance

**As a** compliance officer for a financial services company,
**I want** every denied access attempt to be logged with full context (who, what,
when, from where),
**so that** we can produce audit trails for regulatory review and detect
brute-force attacks.

**Setup:**
```
# Normal application users configured
ACL SETUSER trading on >trade_pass ~trade:* +@read +@write -@dangerous
ACL SETUSER reporting on >report_pass ~* +@read -@write -@admin
```

**Monitoring workflow:**
```
# Weekly audit review
ACL LOG 100

# Example output:
# 1) count=47, reason="command not allowed", username="reporting",
#    command="SET", client="10.0.3.42:52311", timestamp=1711036800000
# 2) count=3, reason="auth failure", username="unknown_user",
#    client="203.0.113.17:41222", timestamp=1711036799000

# Alert on repeated auth failures (brute force detection):
# If count > 10 for auth_failure from same client_ip in 60s window -> alert
```

**Behavior:**
- `reporting` runs `SET trade:spy "hack"` -- NOPERM, logged to ACL LOG
- Unknown user attempts `AUTH admin password123` 50 times -- each batch logged
  with deduplication (count increments, not 50 separate entries)
- ACL LOG entries include client IP, timestamp, command, and key
- Deduplication prevents log storage from exploding under attack

**Why this matters:** PCI DSS Requirement 10 mandates logging all access to
cardholder data environments. SOC 2 requires evidence of access controls.
ACL LOG provides the raw audit trail. Deduplication prevents attackers from
using log flooding as a denial-of-service vector against the audit system.

---

### US-9: Emergency Lockdown

**As a** security incident responder,
**I want** to immediately disable all non-admin users with a single command
sequence,
**so that** I can stop all application access during a security incident while
preserving admin access for investigation.

**Workflow:**
```
# Incident detected -- lock down immediately
ACL SETUSER app off
ACL SETUSER reader off
ACL SETUSER analytics off
ACL SETUSER reporting off
# Only admin retains access

# Investigation phase (admin only):
ACL LOG 1000              # Review recent denials
KEYS suspicious:*         # Investigate compromised data
INFO clients              # Check connected clients

# After investigation -- restore access:
ACL SETUSER app on
ACL SETUSER reader on
ACL SETUSER analytics on
ACL SETUSER reporting on

# Persist the restored state:
ACL SAVE
```

**Behavior:**
- After `off`: existing connections for disabled users stay alive (Redis compat)
  but any new command returns NOPERM
- After `off`: new connections with disabled user credentials get WRONGPASS
- `admin` user is unaffected throughout
- `ACL SAVE` ensures the restored state survives a restart

**Why this matters:** Incident response speed matters. Being able to disable all
application access in seconds (without restarting the server or modifying
firewall rules) reduces the window of exposure. The "existing connections stay
alive" behavior is intentional -- it lets you observe what the compromised
connections are trying to do.

---

### US-10: Embedded Mode with No ACL

**As an** Elixir developer using FerricStore as an embedded library (no TCP listener),
**I want** the embedded API to bypass ACL entirely,
**so that** I get the performance and simplicity of direct in-process calls without
authentication overhead.

**Setup:**
```elixir
# In your Elixir application:
{:ok, _} = Ferricstore.start_link(mode: :embedded, data_dir: "/tmp/myapp_store")

# Direct API calls -- no ACL check, no AUTH needed
Ferricstore.set("mykey", "myvalue")
Ferricstore.get("mykey")
```

**Behavior:**
- No TCP listener is started -- no network attack surface
- `Ferricstore.Acl` GenServer is not started in embedded mode
- All API calls go directly to the shard, bypassing the connection and ACL layers
- There is no way to issue `AUTH` or `ACL` commands (no RESP parser running)

**Why this matters:** When FerricStore is used as an embedded store inside an
Elixir application, the calling code is trusted by definition -- it is the same
OS process. Adding ACL checks to every in-process call would add latency with
zero security benefit. The network is the trust boundary; without a network
listener, ACL is unnecessary.

---

## 13. Test Scenarios from User Stories

Each user story maps to one or more acceptance tests. These tests validate the
ACL implementation end-to-end, from SETUSER through command dispatch.

### TS-1: Multi-tenant Isolation (from US-1)

```elixir
test "tenant users are isolated by key prefix" do
  # Setup
  Acl.set_user("t100", ["on", ">pass", "~tenant:100:*", "+@read", "+@write", "-@dangerous"])
  Acl.set_user("t200", ["on", ">pass", "~tenant:200:*", "+@read", "+@write", "-@dangerous"])

  # Tenant 100 writes to own prefix -- succeeds
  assert :ok = dispatch_as("t100", ["SET", "tenant:100:name", "Acme"])

  # Tenant 100 reads own key -- succeeds
  assert {:ok, "Acme"} = dispatch_as("t100", ["GET", "tenant:100:name"])

  # Tenant 100 reads tenant 200's key -- NOPERM
  assert {:error, "NOPERM" <> _} = dispatch_as("t100", ["GET", "tenant:200:name"])

  # Tenant 200 writes to tenant 100's prefix -- NOPERM
  assert {:error, "NOPERM" <> _} = dispatch_as("t200", ["SET", "tenant:100:name", "Evil"])

  # FLUSHDB denied for both
  assert {:error, "NOPERM" <> _} = dispatch_as("t100", ["FLUSHDB"])
end
```

### TS-2: Read-only User (from US-2)

```elixir
test "reader user can GET but cannot SET" do
  Acl.set_user("reader", ["on", ">pass", "~*", "+@read"])

  assert :ok = dispatch_as("reader", ["GET", "anykey"])
  assert {:error, "NOPERM" <> _} = dispatch_as("reader", ["SET", "anykey", "val"])
  assert {:error, "NOPERM" <> _} = dispatch_as("reader", ["DEL", "anykey"])
end
```

### TS-3: Minimal Permission Service (from US-3)

```elixir
test "rate limiter user can only RATELIMIT.ADD and GET on rate:* keys" do
  Acl.set_user("rl", ["on", ">pass", "~rate:*", "+RATELIMIT.ADD", "+GET", "+TTL"])

  assert :ok = dispatch_as("rl", ["RATELIMIT.ADD", "rate:api:42", "10", "60"])
  assert :ok = dispatch_as("rl", ["GET", "rate:api:42"])
  assert {:error, "NOPERM" <> _} = dispatch_as("rl", ["FLUSHDB"])
  assert {:error, "NOPERM" <> _} = dispatch_as("rl", ["GET", "session:abc"])
  assert {:error, "NOPERM" <> _} = dispatch_as("rl", ["SET", "rate:api:42", "0"])
end
```

### TS-4: Session Store Isolation (from US-4)

```elixir
test "session user can SET/GET/DEL/EXPIRE only on session:* keys" do
  Acl.set_user("sess", ["on", ">pass", "~session:*", "+SET", "+GET", "+DEL", "+EXPIRE", "+TTL", "+PTTL"])

  assert :ok = dispatch_as("sess", ["SET", "session:abc", "data"])
  assert :ok = dispatch_as("sess", ["GET", "session:abc"])
  assert :ok = dispatch_as("sess", ["DEL", "session:abc"])
  assert :ok = dispatch_as("sess", ["EXPIRE", "session:abc", "3600"])

  # Wrong namespace
  assert {:error, "NOPERM" <> _} = dispatch_as("sess", ["GET", "cache:homepage"])

  # Disallowed command
  assert {:error, "NOPERM" <> _} = dispatch_as("sess", ["KEYS", "*"])
  assert {:error, "NOPERM" <> _} = dispatch_as("sess", ["CONFIG", "GET", "maxmemory"])
end
```

### TS-5: Temporary CI User Lifecycle (from US-5)

```elixir
test "temporary CI user is created, used, and deleted" do
  # Create
  assert :ok = Acl.set_user("ci_job_42", ["on", ">token", "~ci:42:*", "+@all", "-@dangerous"])

  # Use -- own prefix works
  assert :ok = dispatch_as("ci_job_42", ["SET", "ci:42:test", "val"])
  assert {:ok, "val"} = dispatch_as("ci_job_42", ["GET", "ci:42:test"])

  # Other prefix denied
  assert {:error, "NOPERM" <> _} = dispatch_as("ci_job_42", ["GET", "ci:43:test"])

  # Dangerous denied
  assert {:error, "NOPERM" <> _} = dispatch_as("ci_job_42", ["FLUSHDB"])

  # Delete
  assert :ok = Acl.del_user("ci_job_42")
  assert nil == Acl.get_user("ci_job_42")

  # Auth fails after deletion
  assert {:error, "WRONGPASS" <> _} = Acl.authenticate("ci_job_42", "token")
end
```

### TS-6: Microservice Cross-prefix Isolation (from US-6)

```elixir
test "microservices cannot access each other's prefixes" do
  Acl.set_user("cart", ["on", ">p", "~cart:*", "+@read", "+@write", "+@hash", "-@dangerous"])
  Acl.set_user("inventory", ["on", ">p", "~inventory:*", "+@read", "+@write", "+@string", "+INCR", "+DECR", "-@dangerous"])
  Acl.set_user("analytics", ["on", ">p", "~*", "+@read", "-@write", "-@admin", "-@dangerous"])

  # Cart can write to cart:*
  assert :ok = dispatch_as("cart", ["HSET", "cart:user:42", "item_1", "3"])

  # Cart cannot touch inventory:*
  assert {:error, "NOPERM" <> _} = dispatch_as("cart", ["GET", "inventory:sku:100"])

  # Analytics can read anything
  assert :ok = dispatch_as("analytics", ["GET", "cart:user:42"])

  # Analytics cannot write anything
  assert {:error, "NOPERM" <> _} = dispatch_as("analytics", ["SET", "cart:user:42", "hack"])
end
```

### TS-7: Password Rotation (from US-7)

```elixir
test "multi-password rotation with zero downtime" do
  Acl.set_user("app", ["on", ">old_pass", "~*", "+@all"])

  # Old password works
  assert {:ok, "app"} = Acl.authenticate("app", "old_pass")

  # Add new password (both valid)
  Acl.set_user("app", [">new_pass"])
  assert {:ok, "app"} = Acl.authenticate("app", "old_pass")
  assert {:ok, "app"} = Acl.authenticate("app", "new_pass")

  # Remove old password
  Acl.set_user("app", ["<old_pass"])
  assert {:error, "WRONGPASS" <> _} = Acl.authenticate("app", "old_pass")
  assert {:ok, "app"} = Acl.authenticate("app", "new_pass")
end
```

### TS-8: Audit Log Captures Denials (from US-8)

```elixir
test "ACL LOG records denied commands with deduplication" do
  Acl.set_user("reporter", ["on", ">pass", "~*", "+@read"])

  # Attempt denied command multiple times
  for _ <- 1..10 do
    dispatch_as("reporter", ["SET", "key", "val"])
  end

  log = Acl.get_log(10)

  # Should be deduplicated (count > 1, fewer than 10 entries)
  assert length(log) < 10
  entry = hd(log)
  assert entry.event_type == :command_denied
  assert entry.username == "reporter"
  assert entry.command == "SET"
  assert entry.count >= 2
end
```

### TS-9: Emergency Lockdown (from US-9)

```elixir
test "disabling users blocks all their commands" do
  Acl.set_user("app", ["on", ">pass", "~*", "+@all"])
  Acl.set_user("admin", ["on", ">admin_pass", "~*", "+@all"])

  # App works
  assert :ok = dispatch_as("app", ["SET", "key", "val"])

  # Disable app
  Acl.set_user("app", ["off"])

  # App is blocked
  assert {:error, "NOPERM" <> _} = dispatch_as("app", ["SET", "key", "val"])

  # Admin still works
  assert :ok = dispatch_as("admin", ["SET", "key", "val"])

  # Re-enable
  Acl.set_user("app", ["on"])
  assert :ok = dispatch_as("app", ["SET", "key", "val"])
end
```

### TS-10: Embedded Mode Bypasses ACL (from US-10)

```elixir
test "embedded mode does not start ACL GenServer" do
  # Start in embedded mode
  {:ok, store} = Ferricstore.start_link(mode: :embedded, data_dir: tmp_dir)

  # Verify ACL process is not running
  assert Process.whereis(Ferricstore.Acl) == nil

  # Direct calls work without auth
  assert :ok = Ferricstore.set(store, "key", "value")
  assert {:ok, "value"} = Ferricstore.get(store, "key")
end
```

---

## 14. Design Decisions

This section documents the reasoning behind key architectural choices. Each
decision is recorded with context, alternatives considered, and the rationale
for the chosen approach.

### DD-1: Why bcrypt over SHA-256 for password hashing

**Decision:** Use bcrypt with cost factor 12 (~250ms per hash) instead of
SHA-256 (which Redis uses).

**Context:** Redis stores password hashes as SHA-256 hex digests. This was chosen
for speed -- Redis is single-threaded and cannot afford blocking on password
verification. FerricStore is multi-process (Elixir/BEAM), so we can afford
the latency on the connection process without blocking other clients.

**Alternatives considered:**
- **SHA-256** (Redis approach): Fast (~1ns per hash), but a GPU cluster can
  brute-force 10 billion SHA-256 hashes per second. If the ACL file is leaked,
  all passwords are recoverable in minutes.
- **Argon2id**: The state of the art, but requires a NIF dependency and is harder
  to tune. Bcrypt is well-understood and available in pure Elixir.
- **scrypt**: Good GPU resistance, but less widely deployed and less
  battle-tested than bcrypt.

**Rationale:** Bcrypt at cost 12 means an attacker with leaked hashes can try
~4 passwords per second per CPU core, compared to ~10 billion per second with
SHA-256. The 250ms cost is paid once per `AUTH` command, which is acceptable
since clients authenticate once per connection. The BEAM scheduler ensures
this does not block other connections.

---

### DD-2: Why separate ETS tables instead of a single shared table

**Decision:** Use dedicated named ETS tables per concern (`:ferricstore_acl`,
`:ferricstore_config`, `:ferricstore_waiters`, etc.) instead of a single
multipurpose table.

**Context:** The current codebase already uses multiple ETS tables
(`:ferricstore_acl`, `:ferricstore_config_local`, `:ferricstore_tracking`,
`:ferricstore_compute_locks`, etc.), but the rationale has not been documented.

**Alternatives considered:**
- **Single ETS table with composite keys:** e.g., `{:acl, "username"}`,
  `{:config, "maxmemory"}`. Simpler to manage (one table), but requires
  `:public` access for everything, makes `tab2list` expensive (scans all
  concerns), and prevents using different table types (`:set` vs `:bag` vs
  `:ordered_set`) per concern.
- **Process dictionary:** Fast but dies with the process. Not suitable for data
  that must survive process restarts under supervision.

**Rationale:**
1. **Isolation:** A misconfigured ACL write cannot corrupt config data. Each
   table has its own memory accounting, making it easy to identify which
   subsystem is consuming memory.
2. **Access patterns:** ACL is read-heavy (checked on every command) and benefits
   from `read_concurrency: true`. The audit log is write-heavy and uses
   `:ordered_set` for efficient range queries. Waiters use `:set` with frequent
   inserts and deletes. Different ETS configurations per table optimize for
   each pattern.
3. **Operational visibility:** `ets:info(table, :size)` and
   `:ets.info(table, :memory)` per table give operators fine-grained insight
   into system state.
4. **Independent lifecycle:** Tables can be created, cleared, or rebuilt
   independently. `ACL LOAD` can wipe `:ferricstore_acl` without affecting
   config or tracking state.

---

### DD-3: Why Raft-replicate ACL changes

**Decision:** ACL SETUSER, ACL DELUSER, and ACL SAVE are Raft-replicated
commands. All nodes in the cluster apply ACL changes in the same order.

**Context:** Redis does not replicate ACL changes. In a Redis Cluster, each node
has its own independent ACL configuration. Operators must use external
tooling (Ansible, Terraform, etc.) to keep ACL in sync across nodes.

**Alternatives considered:**
- **Per-node ACL (Redis approach):** Simple, but leads to drift. Node A might
  allow a user that node B denies. This is a security gap and an operational
  nightmare.
- **Gossip-based replication:** Eventually consistent, but "eventually" is
  unacceptable for security configuration. A brief window where one node
  allows access that another denies is a vulnerability.
- **External config management (Consul, etcd):** Adds an infrastructure
  dependency. FerricStore already has Raft for data replication; reusing it
  for ACL is zero additional infrastructure.

**Rationale:** ACL is a security boundary. Security boundaries must be
consistent -- if node A denies a user, node B must also deny that user,
immediately and atomically. Raft provides exactly this: linearizable
consistency with no additional infrastructure. The cost is negligible -- ACL
changes are rare (orders of magnitude less frequent than data writes) and
the Raft overhead is < 1ms per change.

---

### DD-4: Why embedded mode skips ACL entirely

**Decision:** When FerricStore runs in embedded mode (no TCP listener), the ACL
subsystem is not started and all API calls bypass authentication and
authorization.

**Context:** FerricStore can be used as an embedded Elixir library, where the
application calls Ferricstore functions directly without going through a TCP
connection and the RESP protocol.

**Alternatives considered:**
- **Always enforce ACL:** Consistent security model, but adds ~1-2us of
  latency per operation for bcrypt hash lookups and key pattern matching that
  provides zero security benefit (the caller is already in the same OS
  process and has full memory access).
- **Optional ACL in embedded mode:** Let the developer choose. Adds
  configuration complexity for a feature nobody would use.

**Rationale:** The network is the trust boundary. Without a TCP listener, there
is no untrusted input. The calling Elixir code can already read and write
arbitrary memory in the BEAM VM. ACL in embedded mode would be security
theater -- it looks like protection but protects against nothing. Skipping
it keeps the embedded API fast and simple.

---

### DD-5: Why `-command` after `+@all` must work (unlike Redis)

**Decision:** Maintain a separate `denied_commands` MapSet that is always
subtracted from the effective permission set, so `-FLUSHDB` after `+@all`
correctly denies FLUSHDB.

**Context:** In Redis, the ACL rule parser processes rules left-to-right. When
`+@all` is encountered, it sets the command set to "all". When `-FLUSHDB` is
subsequently encountered, Redis attempts to remove FLUSHDB from the "all" set,
but since "all" is a special flag (not an enumerated set), the removal is
silently ignored. This is a known bug/limitation that has confused operators
for years.

**Alternatives considered:**
- **Match Redis behavior:** Bug-compatible, but this specific behavior is
  widely considered a Redis bug, not a feature. Being compatible with a known
  bug helps nobody.
- **Expand `+@all` to the full command set:** When `+@all` is seen, replace
  `:all` with the full `MapSet` of every known command. Then `-FLUSHDB` can
  remove from it. Downside: the MapSet contains hundreds of commands, uses
  more memory, and must be updated every time a new command is added.

**Rationale:** The two-set approach (`commands` + `denied_commands`) is clean and
efficient. `:all` remains a flag (constant-time "is this command allowed?"
check). `denied_commands` is a small set (operators typically deny 1-10
commands). The check is: `if commands == :all, do: not in denied_commands,
else: in commands and not in denied_commands`. This matches the mental model
that operators have when they write `+@all -@dangerous` -- they expect
dangerous commands to be denied.

---

### DD-6: Why protected mode is stricter than Redis

**Decision:** FerricStore protected mode requires at least one non-default ACL
user with a password before accepting non-localhost connections. Redis only
checks whether `requirepass` is set or whether the default user has a password.

**Context:** Redis protected mode is a common source of "why can't I connect?"
support tickets, but its check is too loose. Setting `requirepass hunter2`
satisfies Redis's protected mode, but `hunter2` is a terrible password. More
importantly, the default user with `requirepass` has `+@all ~*` -- full access.

**Alternatives considered:**
- **Match Redis behavior:** Compatible, but perpetuates a weak security posture
  that Redis themselves acknowledge as insufficient.
- **Require TLS for non-localhost:** Too strict for development and many
  production deployments behind VPCs.
- **Require password strength checks:** Subjective, hard to get right, and
  annoying for operators who use strong random passwords that happen to be
  short.

**Rationale:** The check "at least one non-default user with a password" ensures
that the operator has explicitly thought about access control. It does not
judge password strength (that is the operator's responsibility), but it does
prevent the most common misconfiguration: exposing a FerricStore instance with
only the default user and no password. This is the #1 attack vector for
exposed Redis instances on the internet.

---

## 15. Implementation Priority

### Phase 1 (Security -- must have before production):
1. Bcrypt password hashing (replace plaintext storage)
2. Key pattern enforcement at dispatch time
3. Fix `-command` after `+@all` (denied_commands set)
4. Protected mode
5. ACL LOG for command/key denials

### Phase 2 (Feature complete):
6. All 20+ command categories properly mapped
7. Channel pattern enforcement (`&pattern`)
8. ACL SAVE / ACL LOAD (file persistence to `data_dir/acl.conf`)
9. ACL GENPASS
10. `%R~` / `%W~` read/write key patterns
11. Multiple passwords per user
12. `#hash` / `!hash` pre-hashed password support
13. Separate ETS table architecture (migrate ad-hoc tables to documented schema)

### Phase 3 (Polish):
14. ACL LOG deduplication
15. `+command|subcommand` syntax
16. `ACL CAT category` lists commands
17. ACL DELUSER terminates active connections
18. `ACL USERS` command
19. Embedded mode ACL bypass
20. Raft-replicated ACL SAVE
