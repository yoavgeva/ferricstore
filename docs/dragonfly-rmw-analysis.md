# Dragonfly RMW Architecture — Analysis and Design Transfer

**Status:** Design note. Research + proposal for FerricStore's async RMW path.
**Sources:** Dragonfly source at commit-of-today on `main`
(`src/server/engine_shard.{h,cc}`, `src/server/transaction.{h,cc}`,
`src/server/string_family.cc`, `src/server/journal/journal.cc`,
`docs/df-share-nothing.md`), plus public Dragonfly blog posts.

The goal is to extract the mechanism by which Dragonfly delivers atomic
single-key RMW (`INCR`, `APPEND`, `GETSET`, `GETDEL`, `GETEX`, `SETRANGE`)
without locks, consensus, or GenServer-style mailbox hops, and to translate
that into a concrete BEAM/OTP design for FerricStore's async namespace that
hits ~100 μs latency while preserving the correctness invariants we already
depend on.

---

## 1. Dragonfly's Architecture In One Picture

```
+---------------------- Dragonfly process --------------------+
|  Thread 0 = Shard 0 = Proactor 0     (fiber scheduler)      |
|    - owns DbSlice 0   (PrimeTable[hash(k) % N == 0])        |
|    - owns EngineShard::tlocal()                             |
|    - owns TxQueue  (per-shard, ordered by txid)             |
|    - owns JournalSlice (thread_local journal ring buffer)   |
|  Thread 1 = Shard 1 = Proactor 1                            |
|  ...                                                        |
|  Thread N-1 = Shard N-1 = Proactor N-1                      |
+-------------------------------------------------------------+
```

Every thread is a [helio](https://github.com/romange/helio) proactor. Every
shard has exactly one owning thread. A shard's `DbSlice` (the hash table that
stores values) and `JournalSlice` (the replication log) are
`thread_local`. **Nothing about a shard is shared across threads.** This is
the core invariant. Since only one thread ever touches a shard, no mutex or
atomic is needed to mutate any single key.

The same thread also runs client connection fibers. A single thread can
simultaneously service a TCP connection and execute shard callbacks; fibers
cooperatively yield at I/O points.

A client connection fiber is bound to one thread for its lifetime
(`conn_context.cc`). That thread is the "coordinator" for every command on
that connection. The coordinator:

1. Parses the command.
2. Hashes each key: `shard_id = hash(k) % N`.
3. Builds a `Transaction` object (stack-allocated, refcounted).
4. Dispatches one callback to every shard the transaction touches. Dispatch
   is a helio proactor message — a lock-free SPSC queue per target
   thread. No kernel context switch; the receiving thread wakes on its next
   proactor iteration.
5. Waits on a `run_barrier_` for all shards to finish their hop.
6. Writes the reply on the connection socket.

For a single-key command (`INCR foo`) the coordinator hashes `foo`, finds
shard K, and sends one callback to thread K. Thread K runs the callback on
its own fiber, inside its own proactor. When the callback returns, the
coordinator's fiber wakes and replies to the client.

If the coordinator itself happens to be thread K (because the client
connected to thread K and the key hashed to K), Dragonfly detects this
(`Transaction::CanRunInlined`, `transaction.cc:1625`) and runs the callback
*inline* on the current stack — zero proactor wake, zero handoff. This is the
fast path.

---

## 2. INCR Flow — From Wire to Reply

The actual code, stitched across files:

### 2.1. The arithmetic itself (`string_family.cc:292`)

```cpp
OpResult<int64_t> OpIncrBy(const OpArgs& op_args, string_view key,
                           int64_t incr, bool skip_on_missing) {
  auto& db_slice = op_args.GetDbSlice();
  auto res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_STRING);

  if (!res) {  // key missing -> insert new
    PrimeValue pv;
    pv.SetInt(incr);
    db_slice.AddNew(op_args.db_cntx, key, std::move(pv), 0);
    return incr;
  }

  auto opt_prev = res->it->second.TryGetInt();
  if (!opt_prev) return OpStatus::INVALID_VALUE;

  long long prev = *opt_prev;
  // overflow check omitted
  int64_t new_val = prev + incr;
  res->it->second.SetInt(new_val);
  return new_val;
}
```

This is a **pure function of the shard-local `DbSlice`**. It reads, computes,
and writes in the same stack frame. No lock is taken here, because the
caller has already guaranteed that this function runs on the shard's owning
thread.

### 2.2. The command handler (`string_family.cc:723`)

```cpp
cmd::CmdR IncrByGeneric(CommandContext* cmd_cntx, string_view key,
                        int64_t val) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, val, /*skip_on_missing=*/false);
  };
  auto result = co_await cmd::SingleHopT(cb);
  // ... render result to client reply builder
}
```

The handler packs the mutation into a lambda and calls `SingleHopT` (wrapper
over `Transaction::ScheduleSingleHop`). The lambda is a C++ closure; the
coordinator ships it to the shard thread via the proactor's lock-free
queue. The callback executes *on* the shard thread.

### 2.3. The scheduler (`transaction.cc:890`)

```cpp
OpStatus Transaction::ScheduleSingleHop(RunnableType cb) {
  Execute(cb, /*conclude=*/true);
  return local_result_;
}

void Transaction::Execute(RunnableType cb, bool conclude) {
  cb_ptr_ = cb;
  coordinator_state_ |= COORD_CONCLUDING;
  if ((coordinator_state_ & COORD_SCHED) == 0) ScheduleInternal();
  DispatchHop();
  run_barrier_.Wait();
}
```

`ScheduleInternal` assigns a txid (monotonic `op_seq.fetch_add`) and for each
target shard calls `ScheduleInShard` on that shard's thread. `ScheduleInShard`:

1. Acquires the shard's "intent locks" for the keys being touched
   (these are not mutexes — they are counters on a per-key structure
   inside the `DbSlice`, touched only by the owning thread).
2. Inserts the transaction into the shard's `TxQueue` ordered by txid.
3. If the lock was granted (no prior tx owns it), sets `OUT_OF_ORDER` and
   may run the callback *optimistically during scheduling itself* — this is
   the hottest path for single-key commands on uncontended keys
   (`transaction.cc:1225`: `if (lock_granted && execute_optimistic)`).

For a single-key `INCR` on an uncontended key:

- txid allocated lazily (often skipped: `transaction.cc:1236`,
  "Single shard operations might have delayed acquiring txid unless
  necessary").
- Intent lock granted immediately.
- `RunCallback` invoked synchronously inside `ScheduleInShard` itself.
- `OpIncrBy` reads the old int, adds, writes the new int.
- Journal entry appended to the shard's `JournalSlice`
  (`transaction.cc:1582: LogJournalOnShard`).
- `run_barrier_` decremented; coordinator's `Wait()` returns.

**Total cost for the hot path:** one proactor message (or zero if inline),
one hash lookup on `DbSlice`, one write, one journal append. No mutex, no
syscall, no atomic CAS on the value.

### 2.4. The atomicity argument

Two INCRs on the same key from two different client connections:

- Each connection fiber lives on its own coordinator thread.
- Both hash the key to the same shard K.
- Both coordinators post a callback to thread K's proactor queue.
- Thread K's proactor processes queued callbacks **one at a time on a single
  fiber**. Even though INCR's callback doesn't preempt, the proactor never
  runs two callbacks concurrently on the same thread.
- So the two INCRs serialize at thread K's proactor. First runs to
  completion (`prev + 1` → value = 1). Second then reads value = 1, writes
  value = 2.

No update is lost because **there is exactly one place in the address space
where the key is mutated, and that place is single-threaded by
construction.** This is the fundamental property. VLL, intent locks, and the
TxQueue exist to handle *multi-key* and *multi-shard* transactions. For
single-key RMW they are essentially bypassed — that's the `execute_optimistic`
path.

---

## 3. Multi-Key / Multi-Shard Atomicity (For Completeness)

A command like `MSET a=1 b=2 c=3` where `a`, `b`, `c` hash to 3 different
shards works like this (`transaction.cc:736 ScheduleInternal` and `string_family.cc:1520`):

1. Coordinator allocates a **global** txid (`op_seq.fetch_add(1)` — the one
   atomic counter in the system, used for multi-shard only).
2. Coordinator calls `ScheduleInShard` on all 3 shard threads in parallel.
3. Each shard's `ScheduleInShard` acquires its local intent locks for the
   keys it owns and inserts the tx into its `TxQueue` ordered by txid.
4. If *any* shard fails to schedule (because a conflicting tx with a lower
   txid is already in its queue — the "out of order" case), the whole thing
   retries. This is the VLL "out-of-order-on-conflict" rule.
5. When every shard reaches this tx at the head of its own TxQueue, each
   runs the local portion of the callback (`OpMSet(shard_args)`).
6. Coordinator waits on `run_barrier_` for all shards.

Serializability: across the entire cluster, transactions commit in txid
order. A transaction never sees partial state from a tx with a higher txid.

For FerricStore's purposes — we don't care about multi-key atomicity today,
so this section is informational.

---

## 4. Replication / Persistence Of RMW

Dragonfly's replication is **asynchronous, post-apply, per-shard
journal-tailing**. Critical details from `transaction.cc:1544` and
`journal/journal.cc`:

```cpp
void Transaction::LogAutoJournalOnShard(EngineShard* shard,
                                        RunnableResult result) {
  if (!shard->journal()) return;
  if (result.status != OpStatus::OK) return;  // don't log failures

  journal::Entry::Payload entry_payload{cid_->name(), full_args_};
  LogJournalOnShard(std::move(entry_payload));
}

void Transaction::LogJournalOnShard(journal::Entry::Payload&& payload) {
  journal::RecordEntry(txid_, journal::Op::COMMAND, db_index_, ...);
}
```

Key observations:

1. **The journal entry is the command, not the result.** For INCR, the
   journal records `"INCRBY foo 1"` — not "foo = 42". Replicas re-execute
   the command against their own local DbSlice. This means a replica's
   `OpIncrBy` re-reads its local value, adds 1, and writes. Because replicas
   apply journal entries in txid order *and* the primary assigned txids in
   the same order, replicas converge.

2. **Journaling happens after the callback succeeds** (inside
   `RunCallback` → `LogAutoJournalOnShard`, same fiber, same shard
   thread). The journal append is local; it lands in the shard's
   `thread_local JournalSlice`. No cross-thread synchronization.

3. **The client reply is sent after the coordinator's `run_barrier_.Wait()`
   returns**, which is after the callback (and thus the journal append)
   completed. So a client that got `OK` knows the mutation is in the local
   journal. It does *not* know it has replicated yet — that's asynchronous
   tailing by the replica.

4. **No consensus.** Dragonfly's replication is leader/follower with
   asynchronous log shipping. The primary's word is final. If the primary
   crashes before the tail reaches the replica, that write is lost. Dragonfly
   does not promise consensus durability; it promises in-memory speed.

5. **Replica divergence** on a network partition or primary crash is handled
   by full-sync re-seeding (RDB snapshot + journal catch-up from a known
   LSN). There is no branching or reconciliation — one primary, one truth.

This is *structurally different* from FerricStore because we use Ra
(Raft) for durability/replication and we want quorum-acked writes. But the
timing discipline — *append journal locally after applying, reply after
journal append* — is the model FerricStore's async path already
approximates.

---

## 5. Throughput — Per Shard vs. Aggregate

From Dragonfly's published benchmark (AWS c7gn.16xlarge, 64 vCPU):

| Threads (= shards) | Total ops/s |
|--------------------:|-------------:|
| 1  | 302,603   |
| 2  | 329,755   |
| 4  | 744,708   |
| 8  | 1,370,980 |
| 16 | 2,749,539 |
| 32 | 4,263,950 |
| 64 | 6,432,982 |

Per-shard single-threaded ceiling: ~300 K ops/s (mixed GET/SET). Scaling is
close to linear up to 64 shards. p50 ~ 0.3 ms, p99 ~ 1.1 ms at max load.

A few implications for FerricStore:

- **A single shard can push 300 K ops/s *only* because the shard thread
  never leaves its L1/L2 cache** — one hash table, one journal ring
  buffer, no mutex, no syscall per op. The work per op is on the order of
  a few hundred nanoseconds.
- Dragonfly uses **kernel-bypass networking** (`io_uring` via helio) to
  avoid the syscall cost per recv/send. That's a big piece of why their
  per-core number is so high. A pure in-process BEAM benchmark bypassing
  sockets should be able to approach similar per-core numbers for the
  compute part; the socket syscalls on BEAM remain the ceiling.
- For INCR specifically: the op is all-CPU, no I/O on the hot path. The
  ceiling on a BEAM process doing `:ets.lookup + arithmetic +
  :ets.insert + fast journal append` should be on the order of a few
  hundred thousand per second per shard process, *if* we don't do a
  GenServer-call roundtrip per op.

---

## 6. Comparison To FerricStore's Current Design

### What is the same

- Shard-per-key-range: FerricStore hashes keys to a fixed number of shards;
  Dragonfly does the same. Both map one shard to one owning "executor"
  (GenServer in our case, thread in theirs).
- Single-key atomicity by ownership: both rely on "exactly one place in the
  address space mutates this key". Dragonfly achieves it via thread
  ownership; we achieve it via GenServer mailbox serialization.
- Journal + replay for replication: Dragonfly's journal is in-memory
  thread-local ring + async tailing; our journal is Bitcask WAL + Ra log.
  Both record the command (not the result) and rely on deterministic
  re-execution for convergence.

### What is different

| Aspect | Dragonfly | FerricStore (async RMW today) |
|---|---|---|
| RMW dispatch | One function call or one proactor message | `GenServer.call` → Batcher → Ra cast → state machine apply |
| Hot-path cost | ~few hundred ns | ~2–7 ms (Raft round-trip) |
| Replication | Async log tail, post-apply | Quorum Raft, pre-apply |
| Durability promise | Local journal before reply | Local Bitcask + Raft quorum before reply |
| Per-op allocations | Stack lambda | Message envelope(s), Ra proto record |
| Coordinator path | Inline if same thread | Always mailbox hop |

The *structural* similarity: both serialize via single-owner execution. The
*performance gap*: FerricStore's serialization is a GenServer call with
Raft underneath, which turns every INCR into an RPC. Dragonfly's
serialization is a function call.

### What FerricStore could borrow

1. **Inline fast path when the caller already lives on the shard owner.**
   Dragonfly's `CanRunInlined` skips the proactor handoff when the
   coordinator thread and the shard thread are the same. On BEAM, the
   equivalent is: if the caller is already executing a callback on the
   shard's lane (e.g. a RMW inside a Lua script we have yet to add, or a
   batch operation), run the op inline. Narrow applicability but free.

2. **Skip the mailbox entirely for uncontended single-key RMW.** This is
   the big one and the subject of the proposal below. Dragonfly's
   observation is that the only thing serializing single-key RMW is the
   shard thread — not the TxQueue, not the intent locks, not the global
   txid. The shard thread is the serializer, and the mailbox is just a
   transport. If we can replace the transport with something cheaper while
   keeping the serializer, we capture most of the win.

3. **Record the command, not the result, in the journal.** We already do
   this. Keep doing it. It lets replicas recompute; it also lets the
   primary retry safely if an async journal append fails.

4. **Write-before-reply, replicate-after.** For async writes in
   FerricStore we accept the Dragonfly-like trade: local durability +
   local journal before reply; replication is async. We already plan
   this; Dragonfly confirms it's the right regime for sub-ms latency.

5. **Lazy txid allocation.** Only pay the cross-shard global sequence
   cost when the transaction actually spans shards. Single-shard INCR
   never touches a global counter. FerricStore today doesn't need a
   global counter for single-key RMW either; make sure we don't sneak
   one in.

---

## 7. Proposed Design: FerricStore Single-Key Async RMW (Dragonfly-Style)

**Goal:** single-key `INCR`/`APPEND`/`GETSET`/`GETDEL`/`GETEX`/`SETRANGE` on
an async namespace at ~100 μs p50, preserving all five invariants in
`docs/async-rmw-design.md §2`.

### 7.1. Key idea

Move the serializer off the GenServer `handle_call` mailbox and onto a
**per-shard lightweight "lane" process** that does nothing but execute RMW
closures back-to-back. The lane has a bounded message queue sized for
batching. ETS remains the data store. The Shard GenServer is bypassed for
async RMW entirely; it still owns lifecycle (open/close, flush scheduling,
crash recovery) but is not in the RMW hot path.

Crucially, the caller process **does the ETS mutation itself** under a
per-key latch, and the lane process only serializes when the latch is
contended. This is the BEAM analog of Dragonfly's "run callback optimistically
during schedule" path. When there's no contention, RMW is one atomic CAS on
an ETS counter + one cast to a batcher; when there is contention, we fall
back to the lane for serial execution.

### 7.2. Components

```
Ferricstore.Store.Shard.Lane       one per shard; single GenServer/Task
  state: %{shard_idx, keydir_ets, batcher_ref, pending_journal = []}
  API:  run_rmw(lane, rmw_fn)     -> via {:call, from, rmw_fn} cast+reply pattern

Ferricstore.Store.Shard.RmwLatch   per-shard ETS table acting as a TryLock
  {key} present  ⇒ some other process is inside an RMW for this key
  :ets.insert_new / :ets.delete  are the CAS primitives

Ferricstore.Store.Shard.Keydir     the existing ETS table with the 7-tuple.
```

### 7.3. Hot path — uncontended INCR

```elixir
def incr(ctx, key, delta) do
  shard = Router.shard_for(ctx, key)
  latch = Shard.Lane.latch_table(shard)
  keydir = Shard.Lane.keydir(shard)

  # Try-lock on the key. ets.insert_new is atomic O(1).
  case :ets.insert_new(latch, {key, self()}) do
    true ->
      try do
        new_val = do_incr_inline(keydir, key, delta)
        # cast to batcher; batcher will append to Bitcask + Ra
        # off the hot path. The cast does NOT wait.
        Shard.Lane.journal_async(shard, {:incr, key, delta, new_val})
        {:ok, new_val}
      after
        :ets.delete(latch, key)
      end

    false ->
      # Contended. Hand off to the lane, which serializes with other
      # contenders and the owner of the latch.
      Shard.Lane.run_rmw(shard, fn ->
        do_incr_inline(keydir, key, delta)
      end)
  end
end

defp do_incr_inline(keydir, key, delta) do
  case :ets.lookup(keydir, key) do
    [] ->
      new = delta
      :ets.insert(keydir, {key, new, 0, 0, :pending, 0, byte_size_of(new)})
      new
    [{^key, old, exp, lfu, fid, off, sz}] ->
      {:ok, old_int} = coerce_integer(old)
      new = old_int + delta
      :ets.insert(keydir, {key, new, exp, lfu, :pending, 0, byte_size_of(new)})
      new
  end
end
```

**Why this is atomic despite running in the caller process:**

- `:ets.insert_new/2` is the serialization point. It is atomic across
  all BEAM schedulers. At most one caller holds the latch for a given
  key at a time.
- While the latch is held, `:ets.lookup + :ets.insert` executes
  exclusively for that key. No other caller can read the intermediate
  state because no other caller can acquire the latch.
- Concurrent INCRs on *different* keys don't serialize — they each grab
  their own latch entry.
- If the caller crashes between acquire and release, the latch entry
  leaks. Fix: on shard GenServer heartbeat (every 100 ms), scan latch
  for entries whose owner pid is dead (`Process.alive?/1`) and clear
  them. Race window = heartbeat interval; during it, the affected key
  is un-incrementable. Acceptable for an async namespace; add a
  monitor-based cleaner later if we care.

### 7.4. Contended path — the Lane

When `insert_new` returns `false`, fall back to the Lane process. The Lane is
a GenServer that owns the shard and does exactly what Dragonfly's
shard-thread does: pull RMW closures off its mailbox and execute them on
its own heap.

```elixir
defmodule Ferricstore.Store.Shard.Lane do
  use GenServer

  def run_rmw(shard_idx, fun) do
    GenServer.call(via(shard_idx), {:rmw, fun}, 5_000)
  end

  def handle_call({:rmw, fun}, _from, state) do
    # Execute the closure synchronously. The closure reads/writes ETS
    # directly. We are the only process currently executing a Lane RMW
    # for this shard, so any :ets.insert by us is serialized vs. other
    # Lane RMWs. Hot-path callers using :ets.insert_new latch will
    # *also* serialize against us if they target the same key, because
    # the latch key is the same regardless of which path took it.
    result = fun.()
    # Enqueue journal entry for async durability
    journal = [{:rmw, result} | state.journal]
    {:reply, result, %{state | journal: journal}}
  end
end
```

Wait — there's a bug. If the latch is held by Caller A (fast path), and
Caller B falls back to the Lane, the Lane will happily execute its RMW
without acquiring the latch, racing Caller A. Fix: **the Lane acquires the
latch too.**

Corrected Lane:

```elixir
def handle_call({:rmw, key, fun}, from, state) do
  # Busy-wait with backoff isn't great on BEAM; instead, re-enqueue
  # ourselves via a timer if the latch is held. Better: have the fast
  # path route all RMW to the lane when we see ANY contention, and make
  # the lane the sole serializer. That's simpler.
  ...
end
```

The cleanest design is: **the latch is the only serializer**. Both the fast
path and the lane take the latch. The lane's only job is to retry after a
brief delay when the latch is held. Or — simpler still — the lane is only
used as a queue for callers who couldn't get the latch on their first try,
and the lane's worker loop uses `:ets.insert_new` with a small sleep/retry.

Simplest correct design:

```elixir
def incr(ctx, key, delta) do
  case try_rmw_on_caller(ctx, key, fn keydir ->
         do_incr_inline(keydir, key, delta)
       end) do
    {:ok, result} -> {:ok, result}
    :contended -> Shard.Lane.run_rmw(ctx.shard, key, fn keydir ->
                    do_incr_inline(keydir, key, delta)
                  end)
  end
end

defp try_rmw_on_caller(ctx, key, fun) do
  latch = Shard.Lane.latch_table(ctx.shard)
  keydir = Shard.Lane.keydir(ctx.shard)
  if :ets.insert_new(latch, {key, self()}) do
    try do
      res = fun.(keydir)
      journal_async(ctx.shard, key, res)
      {:ok, res}
    after
      :ets.delete(latch, key)
    end
  else
    :contended
  end
end

# Inside Lane:
def handle_call({:rmw, key, fun}, from, state) do
  # Spawn a worker so the Lane itself isn't blocked if the latch is
  # held by a slow caller. This is optional; if the latch is always
  # released in microseconds, we can do it inline and keep ordering
  # fair.
  Task.start(fn ->
    reply = with_latch(state, key, fn keydir -> fun.(keydir) end)
    GenServer.reply(from, reply)
  end)
  {:noreply, state}
end

defp with_latch(state, key, fun, backoff_us \\ 10) do
  if :ets.insert_new(state.latch, {key, self()}) do
    try do
      res = fun.(state.keydir)
      journal_async(state.shard, key, res)
      {:ok, res}
    after
      :ets.delete(state.latch, key)
    end
  else
    Process.sleep(div(backoff_us, 1000) |> max(1))
    with_latch(state, key, fun, min(backoff_us * 2, 1000))
  end
end
```

### 7.5. Journaling / Bitcask / Ra

`journal_async/3` in this design does **exactly what plain async PUT does
today**: cast to the Batcher, which writes Bitcask via NIF and pipelines
the command to Ra for replication. The difference vs. today's async RMW:

- Today: Shard GenServer does the RMW inside a `handle_call`, then casts
  to Batcher. Every RMW pays the `GenServer.call` mailbox hop.
- Proposed: Caller does the RMW under an ETS latch, then casts to
  Batcher. No GenServer hop in the uncontended case.

What we record in the journal is the **resolved command**:
`{:rmw_applied, :incr, key, delta, new_val, exp}`. Replicas applying this
entry do not need to recompute — they set the key to `new_val` directly.
This matters for async because replicas should *not* re-execute an RMW that
raced with a concurrent RMW on the primary; they should converge to what the
primary decided.

(Dragonfly replays the command, not the result, because replicas execute in
txid order and a single-key RMW has no concurrency on the replica. We could
do the same if we propagate the primary's txid — but recording the result
is simpler and a tiny bit more defensive.)

### 7.6. Read path (unchanged)

`GET` reads ETS directly without going through Lane or Shard. The latch is
not consulted on GET because:

- If a RMW is in progress, the ETS row is either the pre-RMW value or the
  post-RMW value; never a torn state because `:ets.insert` is atomic.
- A GET during an RMW might see the pre- or post-RMW value — this is
  expected under linearizable-at-the-primary semantics. Subsequent GETs
  see the post-RMW value deterministically.

### 7.7. Latency budget (uncontended INCR)

| Step | Estimate |
|---|---|
| Router hash + lookup | ~1 μs |
| `:ets.insert_new` on latch | ~1–2 μs |
| `:ets.lookup` on keydir | ~1 μs |
| Integer parse + add | <1 μs |
| `:ets.insert` on keydir | ~1–2 μs |
| `:ets.delete` on latch | ~1 μs |
| `GenServer.cast` to Batcher | ~3–5 μs |
| Return to caller | ~1 μs |
| **Total hot path** | **~10–15 μs** on BEAM |

Plenty of headroom under the 100 μs target. Realistic budget with
scheduling noise: ~30–50 μs p50, ~100 μs p99.

Under contention (say 25 concurrent INCRs on one key), the 26th caller
waits at the Lane. With a 10 μs RMW, the 25th contender sees ~250 μs
queue delay. Still within async expectations for a pathological workload.

---

## 8. Trade-offs / Downsides

1. **Latch leaks on caller crash.** If a caller dies holding the latch,
   the key is locked until the Shard heartbeat reclaims it (~100 ms). We
   can tighten this with a `Process.monitor` from a latch-janitor
   process, at the cost of one monitor per active RMW. Probably not
   worth it; async RMW is best-effort.

2. **No total ordering across keys.** Dragonfly's txid gives a global
   serial order for multi-key transactions. We don't have that in this
   design — each key has its own serial order, and cross-key ordering is
   the order in which journal_async casts arrive at the Batcher. For
   single-key RMW this doesn't matter. For future multi-key RMW (e.g. a
   Lua script) we'd need something like Dragonfly's TxQueue.

3. **Replica divergence risk.** If we journal the resolved command
   (`new_val`) and a replica's ETS already has a newer value from a
   different source, we overwrite it. Mitigation: the state machine's
   existing `:async_origin` skip rule handles this. Replicas applying a
   resolved RMW should treat it as LWW by txid, same as async PUT
   today.

4. **Latch ETS is hot.** With 25 concurrent INCRs on the same key, we
   spin on `:ets.insert_new` with retries. The Lane fallback bounds this
   but adds a cast + Task.start per contended op. Under extreme
   contention (thousands of writers to one key), throughput collapses to
   Lane throughput — which is still a GenServer mailbox, so ~100 K
   ops/s. Same ceiling as a pure-GenServer design; the difference is we
   only pay it when contended.

5. **Subtle correctness: the existing Shard GenServer also writes
   ETS.** Today, `Shard.handle_call({:put, ...})` inserts into the same
   keydir. If a sync PUT races with our latch-based RMW on the same key,
   the PUT can silently overwrite the RMW's pre-read. Mitigation options:
   (a) route sync PUTs through the latch too, (b) keep sync PUTs on the
   Shard GenServer but make the RMW latch also block sync PUTs via a
   shared "busy" entry in latch, or (c) declare that sync PUT + async RMW
   on the same key is undefined. Option (b) is cheap and correct: sync
   PUT's handle_call does `:ets.insert_new(latch, {key, self()})` at the
   top and retries/falls-back if contended.

6. **No help for multi-key RMW.** `SETRANGE` across shards, future
   `MSETNX` style ops, Lua scripts — this design punts on all of them.
   Multi-key async RMW needs either a Dragonfly-style TxQueue or a
   continued reliance on Raft for the multi-key path.

7. **Observability cost.** Every RMW now takes one of two paths
   (uncontended / contended). Instrumentation must report both, because
   a regression in contended-path latency will be invisible if we only
   measure the fast path.

8. **Testing discipline.** The correctness proof rests on `:ets.insert_new`
   being the only serialization point and all writers honoring the
   latch. One code path that skips the latch — say, a future recovery or
   migration tool — breaks atomicity. We need a runtime assertion (dev
   mode): every write to keydir must assert the caller holds the latch
   *or* is the Lane.

---

## 9. What We're Not Proposing (And Why)

- **We're not proposing to remove Ra / go to async-only replication.**
  Dragonfly made that choice; we explicitly haven't. Sync namespaces
  still go through Ra and quorum-commit. Async namespaces already accept
  lag; this design just makes the async RMW path as fast as the async
  PUT path, not faster.

- **We're not proposing to go lock-free / NIF-based.** A NIF that does
  read/add/write on a shared atomic integer would be faster still, but
  the user's standing rule is that NIFs must be pure and stateless. ETS
  latches respect that rule.

- **We're not proposing a Dragonfly-style TxQueue with global
  txids.** We don't need it for single-key RMW, and the cost of a
  global atomic counter per op would eat most of the win.

---

## 10. Summary

Dragonfly's single-key RMW is fast because:

1. One thread owns the shard, so mutation is a function call, not an RPC.
2. The "transactional framework" is bypassed for the hot path
   (`OPTIMISTIC_EXECUTION` in `ScheduleInShard`).
3. Replication is async post-apply, not pre-apply consensus.
4. Journaling is thread-local, so it costs a pointer bump.

FerricStore's hot-path equivalent is:

1. One ETS latch per key, claimed by the caller process — this is the
   serializer, replacing the owning-thread invariant with a BEAM-atomic
   CAS.
2. RMW compute + ETS write in the caller's process — no mailbox hop.
3. Async cast to the existing Batcher for Bitcask + Ra — same as async
   PUT today, not on the hot path.
4. Fallback to a per-shard Lane GenServer only when the latch is
   contended.

Expected outcome: async RMW p50 in the 30–50 μs range, matching async PUT,
with worst-case 100 μs under moderate contention. Compared to today's
2–7 ms (GenServer → Batcher → Ra → apply → reply round-trip), that's
~50–100× improvement.
