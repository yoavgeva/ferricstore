# FerricStore — Implementation Tasks

Phase 1 goal: working single-node Redis-compatible server with persistence (RESP3, Bitcask, ETS hot cache, core string commands).

## Tasks

### [x] 1. Scaffold Mix project with Rust NIF setup
- `mix new ferricstore --sup`
- Add deps: rustler, ranch, telemetry
- Create `native/ferricstore_bitcask/` Rust crate with Rustler boilerplate
- Wire Rustler into mix.exs
- Verify `mix compile` passes with a stub NIF

### [x] 2. Implement Bitcask Rust NIF (keydir + append log + compaction)
- `keydir.rs`: in-memory HashMap → `{file_id, offset, value_size, expire_at, ref_bit}`
- `log.rs`: append-only log (crc32 | timestamp | key_size | value_size | key | value)
- `store.rs`: `open`, `get`, `put`, `delete`, `keys`
- `compaction.rs`: merge old log files, write hint files, remove stale/deleted/expired entries
- Hint file startup: rebuild keydir from hint files on open, replay unflushed log tail
- Expose to Elixir: `new/1`, `get/2`, `put/4`, `delete/2`, `keys/1`, `compact/1`
- Use dirty_io scheduler. Phase 1: synchronous fsync.

### [x] 3. Implement RESP3 protocol parser and encoder
- `parser.ex`: pure binary state-machine. Returns `{:ok, [values], rest}` or `{:more, state}`
  - All RESP3 types: `+` `-` `:` `$` `*` `_` `#` `,` `=` `!` `%` `~` `>`
  - Inline commands, pipelining
- `encoder.ex`: Elixir terms → RESP3 binary (`:ok`, `{:error, msg}`, int, binary, list, map, nil, push)
- Unit tests: all types, partial reads, pipeline batches

### [x] 4. TCP server with Ranch — connection handling
- `listener.ex`: Ranch TCP listener on port 6379
- `connection.ex`: Ranch protocol handler (`:ranch_protocol`)
  - State: socket, buffer, transaction state
  - TCP data → buffer → parse RESP3 → dispatch → send responses
  - Pipelined commands: parse all, dispatch sequentially, batch responses
  - `CLIENT HELLO 3` handshake → RESP3 server greeting map
  - Reject `HELLO 2` (RESP2 not supported)
  - Graceful close on QUIT/RESET

### [ ] 5. Shard GenServer + ETS hot cache + Router
- `router.ex`: `shard_for_key/1` via `:erlang.phash2` mod shard_count. Direct `:ets.lookup` for hot reads.
- `shard.ex`: GenServer owning one ETS table + one Bitcask handle
  - ETS schema: `{key, value, expire_at_monotonic}`, type `:set`, access `:public`
  - Write: Bitcask first (durable), then ETS
  - Cold read: Bitcask → promote to ETS → return
  - TTL sweep: background timer every 100ms, `:ets.select` expired keys → delete both layers
- `ets_cache.ex`: ETS table creation, lookup, insert, delete, TTL sweep helpers

### [ ] 6. Implement core Redis commands (strings + TTL + connection)
- `dispatcher.ex`: upcased command name → handler module
- `strings.ex`: GET, SET (+EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET), DEL, EXISTS, MGET, MSET, MSETNX, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETSET, GETDEL, GETEX, SETNX, SETEX, PSETEX
- `ttl.ex`: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME, PEXPIRETIME
- `server_cmd.ex`: PING, ECHO, SELECT (stub), QUIT, RESET, AUTH (stub), INFO, COMMAND (stub), DBSIZE, FLUSHDB, FLUSHALL
- `transactions.ex`: MULTI, EXEC, DISCARD, WATCH — queue in connection state, atomic batch on EXEC
- Correct error strings: wrong type, wrong arity, out of range, missing key → nil/$-1

### [ ] 7. OTP application supervisor tree
- Top-level supervisor (`:one_for_one`): Router + Listener
- `ShardSupervisor`: dynamic supervisor for shard GenServers (`:permanent` restart)
- Config via `Application.get_env`: port, data_dir, shard_count, max_memory
- Graceful shutdown: drain connections, flush Bitcask, close log files

### [ ] 8. End-to-end smoke test with redis-cli
- `redis-cli -3`: verify HELLO handshake
- PING, SET/GET, TTL expiry, INCR, MGET, MSET, DEL, EXISTS
- MULTI/EXEC atomicity, DISCARD
- Persistence: SET → kill → restart → GET still returns value
