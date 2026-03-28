# ACID Transactions (BEGIN / COMMIT / ROLLBACK)

## Overview

FerricStore supports two transaction modes:

| Command | Reads | Writes | Rollback | Use Case |
|---|---|---|---|---|
| MULTI/EXEC | Allowed (queued) | Allowed (queued) | No | Redis-compatible batch |
| BEGIN/COMMIT | Not allowed | Write-only (queued) | Yes — full ACID | Atomic mutations |

BEGIN/COMMIT provides true ACID transactions for write operations on a single shard. All commands succeed or none apply. Zero dirty reads.

## Commands

### BEGIN

Start an ACID transaction. Enters queue mode — subsequent write commands are validated and queued, client receives `+QUEUED`.

### COMMIT

Submit all queued commands to Raft as a single log entry. The Raft state machine executes all commands into a buffer. If ALL succeed, the buffer is flushed to ETS + Bitcask. If ANY fails, the buffer is discarded — nothing was written.

### ROLLBACK

Explicitly discard the queue and return to normal mode. Nothing is submitted to Raft.

## Allowed Commands Inside BEGIN

Write-only data mutation commands only. No reads allowed — read before BEGIN, write inside.

### String commands

SET, DEL, SETNX, SETEX, PSETEX, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, SETRANGE, GETSET, GETDEL

### Hash commands

HSET, HDEL, HINCRBY, HINCRBYFLOAT, HSETNX

### List commands

LPUSH, RPUSH, LPOP, RPOP, LSET, LREM, LTRIM, LINSERT, LMOVE

### Set commands

SADD, SREM, SMOVE, SPOP

### Sorted Set commands

ZADD, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX

### Expiry commands

EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST

### NOT allowed inside BEGIN

- **Read commands** (GET, MGET, EXISTS, TYPE, TTL, HGET, HGETALL, LRANGE, SMEMBERS, ZSCORE, etc.) — `ERR read commands not allowed inside BEGIN`
- **LOCK, UNLOCK, CAS, RATELIMIT.ADD** — `ERR command not supported inside BEGIN`
- **WATCH** — `ERR WATCH not needed with BEGIN`
- **MULTI, BEGIN** — `ERR already in BEGIN`
- **SUBSCRIBE, PUBLISH** — `ERR not allowed inside BEGIN`
- **MSET, MSETNX** — `ERR not allowed inside BEGIN` (multi-key, could span shards)

## Constraints

### Single shard only

All keys in the transaction must hash to the same shard. If keys span multiple shards, COMMIT returns:

```
ERR BEGIN transaction keys must target a single shard. Use hash tags {tag}:key to co-locate keys, or use MULTI for cross-shard batches.
```

Use hash tags to co-locate keys: `{user:42}:balance` and `{user:42}:name` hash to the same shard.

### Max 100 commands

Transactions are capped at 100 commands. The shard GenServer is blocked for other writes during execution (reads are unblocked — they read ETS directly). 100 commands execute in <5ms.

### No WATCH needed

The shard GenServer serializes all writes. While the transaction executes, no other write can interleave. This is implicit locking — no explicit WATCH/optimistic concurrency needed.

## Execution Architecture

### Why the State Machine (not GenServer)

The transaction is NOT executed in the shard GenServer. It's submitted to Raft as a `{:tx_acid, queue, namespace}` command. The Raft leader's state machine executes it in `apply/3`.

Why: If the GenServer on a follower node executed commands locally, then submitted to Raft, the leader's state might have changed in between (another write from another client). The follower's "all OK" decision would be based on stale state. By executing in the state machine, the decision is made on the authoritative leader state.

### Buffer Architecture (Zero Dirty Reads)

Commands do NOT write to ETS during execution. They write to a process-dictionary buffer. Only after ALL commands succeed does the buffer flush to ETS.

```
State Machine apply/3 for {:tx_acid, queue, namespace}:

1. Initialize buffer: Process.put(:tx_acid_buffer, %{})
2. Build buffered store lambdas:
   - local_put writes to buffer (not ETS)
   - local_delete marks key as :deleted in buffer (not ETS)
   - local_get checks buffer first, then ETS (pre-transaction state)
   - local_exists checks buffer first, then ETS
   - local_incr, local_append etc. use local_get + local_put (automatic)
3. Execute each command via Dispatcher.dispatch(cmd, args, buffered_store)
4. Collect results
5. Check: any {:error, _}?
   YES -> discard buffer, return {:rollback, index, reason}
   NO  -> flush buffer to real ETS + queue Bitcask writes, return {:ok, results}
6. Clean up: Process.delete(:tx_acid_buffer)
```

Why buffer works for compound keys (hash, list, set, zset):
- HSET just calls `local_put("HF:key\0field", value)` — point write, no scan
- SADD just calls `local_put("SM:key\0member", "")` — point write
- LPUSH calls `local_put("L:key\0pos", value)` + `local_put("LM:key", metadata)` — point writes
- All write commands are point operations. No prefix scans needed.
- Prefix scans (HGETALL, SMEMBERS, LRANGE) are reads — not allowed inside BEGIN.

### What happens during execution

| Operation from other clients | Blocked? |
|---|---|
| Hot GET (ETS direct lookup) | No — reads pre-transaction ETS state |
| Cold GET (NIF pread from disk) | No — reads from Bitcask directly |
| True miss | No — returns nil immediately |
| Writes to SAME shard | Wait in Raft queue (serialized by state machine) |
| Writes to OTHER shards | No — different Raft group |
| Any operation on other shards | No |

Zero dirty reads: ETS is either pre-transaction state OR fully committed post-transaction state. Never intermediate. Other clients reading during execution see the pre-transaction state.

## Buffer Details

### Buffer format

```elixir
%{
  "user:1" => {:set, "alice", 0},
  "counter:1" => {:set, "42", 0},
  "old:key" => :deleted,
  "HF:user:1\0name" => {:set, "alice", 0},    # hash field
  "SM:tags\0elixir" => {:set, "", 0},           # set member
  "L:tasks\0000001" => {:set, "job-a", 0},      # list element
}
```

### Buffered local_get

```elixir
local_get = fn key ->
  buffer = Process.get(:tx_acid_buffer, %{})
  case Map.get(buffer, key) do
    {:set, value, _exp} -> value       # modified in this transaction
    :deleted -> nil                     # deleted in this transaction
    nil -> ets_read(ctx.keydir, key)   # not in buffer, read pre-tx ETS
  end
end
```

### Buffered local_put

```elixir
local_put = fn key, value, expire_at_ms ->
  buffer = Process.get(:tx_acid_buffer, %{})
  Process.put(:tx_acid_buffer, Map.put(buffer, key, {:set, value, expire_at_ms}))
  :ok
end
```

### Buffered local_delete

```elixir
local_delete = fn key ->
  buffer = Process.get(:tx_acid_buffer, %{})
  Process.put(:tx_acid_buffer, Map.put(buffer, key, :deleted))
  :ok
end
```

### Flush on commit

```elixir
defp flush_acid_buffer(ctx) do
  buffer = Process.get(:tx_acid_buffer, %{})
  for {key, entry} <- buffer do
    case entry do
      {:set, value, expire_at_ms} ->
        # Write to ETS
        value_for = value_for_ets(value)
        :ets.insert(ctx.keydir, {key, value_for, expire_at_ms, LFU.initial(), 0, 0, 0})
        # Queue Bitcask write
        disk_val = to_disk_binary(value)
        BitcaskWriter.write(ctx.index, ctx.active_file_path, ctx.active_file_id, ctx.keydir, key, disk_val, expire_at_ms)
      :deleted ->
        :ets.delete(ctx.keydir, key)
        BitcaskWriter.delete(ctx.index, ctx.active_file_path, key)
    end
  end
end
```

## Response Format

### Success

```
*3
+OK
+OK
:43
```

Array of results, same format as EXEC.

### Rollback (command error)

```
-ERR ROLLBACK command 3 failed: value is not an integer or out of range
```

Single error identifying which command failed. Client knows nothing was applied.

### Cross-shard error

```
-ERR BEGIN transaction keys must target a single shard. Use hash tags {tag}:key to co-locate keys, or use MULTI for cross-shard batches.
```

### Queue full

```
-ERR BEGIN transaction exceeds 100 command limit
```

## Connection State

```elixir
@type multi_state :: :none | :queuing | :queuing_acid
```

BEGIN sets `:queuing_acid`. Commands validated (known command, correct arity, write-only check) and queued. COMMIT triggers Raft submission. ROLLBACK clears queue.

Passthrough commands (always executed immediately, not queued):
BEGIN, COMMIT, ROLLBACK, PING, QUIT, RESET

## ACID Properties Explained

### Atomicity

All commands in the buffer succeed or the buffer is discarded. The final state is either "all applied" or "none applied". Never partial.

### Consistency

The transaction is a single Raft log entry. Raft linearizability guarantees all nodes see the same order. The state machine `apply/3` is deterministic — all replicas reach the same commit/rollback decision.

### Isolation

The state machine is single-threaded per Raft group. No other command can execute between the first and last command of the transaction. The buffer prevents dirty reads — ETS only changes atomically when the buffer flushes.

### Durability

Raft quorum must persist the log entry before the client gets OK. WAL + fdatasync on majority of nodes. If the leader crashes after commit, the entry is already replicated.

## Files to Change

| File | Change |
|---|---|
| `connection.ex` | Add BEGIN/COMMIT/ROLLBACK dispatch, `:queuing_acid` state, write-only command validation |
| `coordinator.ex` | Add `execute_acid/2` — classify shards, reject multi-shard, submit `{:tx_acid, ...}` to Raft |
| `state_machine.ex` | Add `{:tx_acid, queue, namespace}` handler with buffer/execute/commit-or-rollback |
| `ferricstore.ex` | Add embedded API: `FerricStore.begin_transaction/0`, `FerricStore.commit/0`, `FerricStore.rollback/0` |

## Usage Examples

### Redis CLI

```
127.0.0.1:6379> BEGIN
OK
127.0.0.1:6379> SET account:alice:balance 900
QUEUED
127.0.0.1:6379> SET account:bob:balance 1100
QUEUED
127.0.0.1:6379> COMMIT
1) OK
2) OK
```

### Rollback on error

```
127.0.0.1:6379> BEGIN
OK
127.0.0.1:6379> SET counter 100
QUEUED
127.0.0.1:6379> INCR not_a_number
QUEUED
127.0.0.1:6379> COMMIT
(error) ERR ROLLBACK command 2 failed: value is not an integer or out of range
# counter is NOT set to 100
```

### Explicit rollback

```
127.0.0.1:6379> BEGIN
OK
127.0.0.1:6379> SET key value
QUEUED
127.0.0.1:6379> ROLLBACK
OK
# nothing applied
```

### Embedded Elixir API

```elixir
FerricStore.begin_transaction()
FerricStore.set("account:alice:balance", "900")
FerricStore.set("account:bob:balance", "1100")
case FerricStore.commit() do
  {:ok, results} -> IO.inspect(results)
  {:error, reason} -> IO.puts("Rolled back: #{reason}")
end
```

### Hash tag co-location

```
BEGIN
SET {user:42}:name alice
SET {user:42}:email alice@example.com
SET {user:42}:age 30
COMMIT
# All keys hash to same shard because of {user:42} tag
```

## Future Work (Phase 2)

Cross-node cross-shard ACID transactions using Two-Phase Commit. See IDEAS.md for design details. Phase 1 (single-shard) covers 95% of real use cases.
