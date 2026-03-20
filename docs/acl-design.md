# FerricStore ACL Design — v2

## Philosophy

Redis ACL is good but has quirks from 30 years of backward compatibility. FerricStore
can do better because we start fresh. Our ACL must be:

1. **Secure by default** — protected mode, bcrypt, no plaintext passwords
2. **Redis-compatible** — same command syntax, same error messages, clients just work
3. **Raft-replicated** — ACL changes go through consensus, all nodes agree
4. **Namespace-aware** — key patterns integrate with our prefix system
5. **Auditable** — every denial logged with context

---

## 1. User Model

```
User {
  username:     binary()           # unique, case-sensitive
  enabled:      boolean()          # on/off
  passwords:    [bcrypt_hash]      # list — supports rotation
  key_patterns: [glob_pattern]     # ~pattern, empty = no access
  channel_patterns: [glob_pattern] # &pattern, empty = no access
  commands:     MapSet | :all      # allowed commands
  denied_commands: MapSet          # explicit denials (subtracted from :all)
  flags:        [:nopass | ...]    # special flags
}
```

### Improvement over Redis:
- **Passwords are bcrypt-hashed at ingestion** — never stored plaintext, not even temporarily
- **Separate `denied_commands` set** — fixes the Redis bug where `-command` after `+@all` is ignored. We track denials separately: allowed = (commands or :all) - denied_commands
- **List of password hashes** — supports zero-downtime rotation natively

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
| `>password` | Add password (bcrypt-hashed internally) | Additive — multiple passwords supported |
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

Redis uses SHA-256 for password hashing. SHA-256 is fast — an attacker with
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

## 7. ACL Persistence

### ACL SAVE
Writes current ACL rules to `data_dir/acl.conf`:
```
user default on nopass ~* &* +@all
user app on #$2b$12$... ~cache:* ~session:* &* +@read +@write -@dangerous
user admin on #$2b$12$... ~* &* +@all
```

### ACL LOAD
Reloads ACL rules from the file. Existing users not in the file are deleted.
Active connections are NOT terminated — they keep their current permissions
until they re-auth.

### Raft replication
ACL changes go through Raft:
```
ACL SETUSER app ...
  -> Batcher -> ra:process_command({:acl_setuser, "app", rules})
  -> apply() on all nodes -> ETS updated atomically on all nodes
```
ACL SAVE is node-local (writes to local disk only). ACL LOAD is also
node-local but triggers Raft commands to propagate loaded rules.

---

## 8. ACL LOG

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

## 9. ACL GENPASS

```
ACL GENPASS [bits]
```
Generates a cryptographically random password using `:crypto.strong_rand_bytes/1`.
Default: 256 bits (64 hex chars). Returns the plaintext password — the user
must securely store it.

---

## 10. Differences from Redis

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

## 11. Implementation Priority

### Phase 1 (Security — must have before production):
1. Bcrypt password hashing (replace plaintext storage)
2. Key pattern enforcement at dispatch time
3. Fix `-command` after `+@all` (denied_commands set)
4. Protected mode
5. ACL LOG for command/key denials

### Phase 2 (Feature complete):
6. All 20+ command categories properly mapped
7. Channel pattern enforcement (`&pattern`)
8. ACL SAVE / ACL LOAD
9. ACL GENPASS
10. `%R~` / `%W~` read/write key patterns
11. Multiple passwords per user
12. `#hash` / `!hash` pre-hashed password support

### Phase 3 (Polish):
13. ACL LOG deduplication
14. `+command|subcommand` syntax
15. `ACL CAT category` lists commands
16. ACL DELUSER terminates active connections
17. `ACL USERS` command
