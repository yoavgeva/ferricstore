# Module Split Design: Shard + Connection

## Goal

Split two oversized modules into focused sub-modules:
- `Ferricstore.Store.Shard` (4245 lines) → 9 modules, all under 500 lines
- `FerricstoreServer.Connection` (2723 lines) → 9 modules, all under 450 lines

Zero clause-ordering warnings. No external API changes.

---

## Part 1: Shard Split

### Architecture

The GenServer stays in `Shard` as a thin dispatch hub. Each `handle_call` clause
delegates to a public function in a focused logic module. Logic modules are
stateless — they receive the state struct, return GenServer reply tuples.

### Modules

| Module | ~Lines | Responsibility |
|--------|--------|----------------|
| `Shard` (hub) | 400 | GenServer dispatch, struct, init |
| `Shard.ETS` | 250 | Keydir operations (lookup, insert, delete, value_for_ets) |
| `Shard.Reads` | 200 | GET, GET_META, GET_FILE_REF, EXISTS, KEYS |
| `Shard.Writes` | 500 | PUT, DELETE, INCR, INCR_FLOAT, APPEND, GETSET, GETDEL, GETEX, SETRANGE |
| `Shard.Compound` | 400 | Compound key ops + promotion logic |
| `Shard.NativeOps` | 350 | CAS, LOCK, UNLOCK, EXTEND, RATELIMIT, LIST ops |
| `Shard.Transaction` | 400 | tx_execute + build_local_store (closure factory) |
| `Shard.Flush` | 300 | Write batching, fsync, file rotation, dead byte tracking |
| `Shard.Lifecycle` | 300 | Recovery, expiry sweep, migration, shutdown |

Note: `raft_write` and `raft_write_async` stay as private functions in the hub
(too small for their own module — just 2 one-liners delegating to Batcher).

### State Struct

Stays in `Ferricstore.Store.Shard` (`%__MODULE__{}`). Sub-modules alias it:
```elixir
alias Ferricstore.Store.Shard, as: S
```

### Return Value Convention

Logic modules return full GenServer reply tuples:
```elixir
# In Shard.Writes:
def handle_put(key, value, expire_at_ms, from, state) do
  if state.raft? do
    raft_write_async(state, {:put, key, value, expire_at_ms}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  else
    # direct path...
    {:reply, :ok, new_state}
  end
end

# In hub:
def handle_call({:put, key, value, expire_at_ms}, from, state) do
  Writes.handle_put(key, value, expire_at_ms, from, state)
end
```

### Dependency Graph (no cycles)

```
Shard (hub)
  ├── Shard.Reads       → ETS, Flush
  ├── Shard.Writes      → ETS, Flush
  ├── Shard.Compound    → ETS, Flush, Reads
  ├── Shard.NativeOps   → ETS, Flush, Reads
  ├── Shard.Transaction → ETS, Reads
  ├── Shard.Flush       → ETS
  ├── Shard.Lifecycle   → ETS, Flush
  └── Shard.ETS         → (leaf — no shard deps)
```

### Implementation Order

1. `Shard.ETS` — leaf node, all ETS helpers become public
2. `Shard.Flush` — depends only on ETS
3. `Shard.Lifecycle` — depends on ETS, Flush
4. `Shard.Reads` — depends on ETS, Flush
5. `Shard.Writes` — depends on ETS, Flush
6. `Shard.Compound` — depends on ETS, Flush, Reads
7. `Shard.NativeOps` — depends on ETS, Flush, Reads
8. `Shard.Transaction` — depends on ETS, Reads
9. Hub cleanup — reorder all handle_call/handle_info contiguously

---

## Part 2: Connection Split

### Architecture

The main Connection module keeps the Ranch protocol, receive loops, struct, and
a thin `dispatch/3` with all clauses contiguous. Each clause is a one-liner
delegating to a sub-module.

### Modules

| Module | ~Lines | Responsibility |
|--------|--------|----------------|
| `Connection` (main) | 450 | Struct, loops, thin dispatch, HELLO |
| `Connection.Pipeline` | 300 | Sliding window, command classification |
| `Connection.Auth` | 250 | AUTH, ACL subcommands, ACL cache |
| `Connection.PubSub` | 120 | SUBSCRIBE/UNSUBSCRIBE dispatch |
| `Connection.Transaction` | 150 | MULTI/EXEC/DISCARD/WATCH |
| `Connection.Blocking` | 350 | BLPOP/BRPOP/BLMOVE/BLMPOP/XREAD BLOCK |
| `Connection.Sendfile` | 200 | GET sendfile optimization |
| `Connection.Store` | 200 | Store map construction (closures) |
| `Connection.Tracking` | 200 | Keyspace notifications, client tracking hooks |

### Thin Dispatch Pattern

```elixir
# ALL dispatch/3 clauses contiguous — ZERO grouping warnings
defp dispatch("AUTH", args, state), do: Auth.dispatch_auth(args, state)
defp dispatch("ACL", args, state), do: Auth.dispatch_acl(args, state)
defp dispatch("MULTI", args, state), do: Transaction.dispatch_multi(args, state)
defp dispatch("EXEC", args, state), do: Transaction.dispatch_exec(args, state)
defp dispatch("SUBSCRIBE", args, state), do: PubSub.dispatch_subscribe(args, state)
defp dispatch("BLPOP", args, state), do: Blocking.dispatch_blpop(args, state)
defp dispatch("GET", [key], %{transport: :ranch_tcp} = state)
     when byte_size(key) > 0, do: Sendfile.dispatch_get(key, state)
defp dispatch("XREAD", args, state), do: Blocking.dispatch_xread(args, state)
defp dispatch(cmd, args, state), do: dispatch_normal(cmd, args, state)
```

### Dependency Graph (no cycles)

```
Connection (main)
  ├── Pipeline    → Store, Tracking, Auth
  ├── Auth        → (external: Acl, AuditLog)
  ├── PubSub      → (external: PubSub, Encoder)
  ├── Transaction → Store
  ├── Blocking    → Store, Tracking
  ├── Sendfile    → Store, Tracking
  ├── Store       → (external: Router, DataDir)
  └── Tracking    → (external: ClientTracking, KeyspaceNotifications)
```

### Implementation Order

1. `Connection.Store` — leaf, no sub-module deps
2. `Connection.Tracking` — leaf, no sub-module deps
3. `Connection.Auth` — leaf
4. `Connection.PubSub` — leaf
5. `Connection.Transaction` — depends on Store
6. `Connection.Blocking` — depends on Store, Tracking
7. `Connection.Sendfile` — depends on Store, Tracking
8. `Connection.Pipeline` — depends on Store, Tracking, Auth
9. Main cleanup — thin dispatch, group all clauses

---

## Part 3: Clause Ordering Fixes (no split needed)

These files have clause-ordering warnings that are fixed by just reordering
functions, not splitting modules:

- `commands/server.ex` — group all `handle/3` clauses
- `commands/strings.ex` — group all `handle/3` clauses
- `commands/namespace.ex` — group all `handle/3` clauses
- `commands/dispatcher.ex` — group `dispatch/3` clauses
- `raft/batcher.ex` — group `handle_call/3` clauses
- `store/compound_key.ex` — redefining @doc

---

## Verification

After all changes:
```bash
mix compile 2>&1 | grep "clauses with the same name" | wc -l
# Should be 0
```

No external API changes — all GenServer.call patterns, Router calls, and test
patterns remain identical.
