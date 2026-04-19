# Final Clean Benchmark — Quorum + Async

Server: Azure L4as_v4 (4 vCPU, local NVMe). Fresh restart between every run,
explicit `:all_async` / `:all_quorum` mode set, 10s sustained.

## Results (memtier, t=4, pipeline=1, 256B values)

| Mode | Connections | Ops/sec | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| **Async** | 1 | 4,912 | **0.74ms** | 2.56ms | 11.39ms |
| **Async** | 10 | 32,389 | **0.98ms** | 6.11ms | 13.63ms |
| **Async** | 50 | 41,298 | 4.02ms | 16.77ms | 32.51ms |
| **Async** | 100 | 41,847 | 8.77ms | 27.78ms | 46.59ms |
| **Quorum** | 1 | 300 | 12.48ms | 30.21ms | 53.25ms |
| **Quorum** | 10 | 1,377 | 29.31ms | 70.14ms | 91.65ms |
| **Quorum** | 50 | 3,846 | 48.38ms | 121.34ms | 145.41ms |
| **Quorum** | 100 | 6,946 | 54.27ms | 138.24ms | 208.89ms |

## Headline numbers

- **Async p50 = 0.74ms at c=1, 0.98ms at c=10** — within Redis range
- **Async saturates at ~42K ops/sec** on 4 vCPU
- **Quorum = ~7K ops/sec at c=100** — slower than the prior "44K baseline"

## Why is quorum slower than before?

The "44K ops/sec baseline" (commit `3ffaf67`) was measured when our Rust NIF
WAL was silently NOT in use. The prior ra fork had a bug in
`ra_log_sup:make_wal_conf` that dropped the `wal_io_module` config — so ra
fell back to standard `file:write/2` on the `:prim_file` dirty-thread pool,
which uses 128 async threads to parallelize writes.

This session fixed that misrouting (so our NIF is actually used now) and
also fixed two latent NIF bugs:

1. **charlist → binary** at the `IoMod:open` boundary (ra passes charlists,
   rustler's `String` param requires binary) — otherwise `:badarg`.
2. **`rustler::resource!` registration** for `WalHandle` — otherwise
   `ResourceArc::new` panics with `Option::unwrap() on a None value`.

Our NIF handles writes via a single background thread with a shared aligned
buffer and `O_DIRECT`. The theoretical win is zero-copy + zero context-switch,
but the serialized single-thread background writer is clearly slower than
128 parallel `:prim_file` threads at this throughput.

## Fixes committed this session

| Commit | What |
|---|---|
| `2db81e5` (ra) | `ra_log_wal`: convert charlist filename to binary before NIF open |
| `01abfd0` | Register WalHandle resource in wal_nif on_load |
| `cdc786a` | Drop manual `impl Resource` (conflicts with macro) |
| `30f236f` | Pin updated ra fork |

## Quorum is functional now

Before these fixes: Ra failed to boot entirely (WAL init `:nif_panicked`),
shard ra servers never started, every quorum write timed out → 300 ops/sec.

After: Ra boots cleanly, shards elect leaders, quorum writes go through the
full Raft path + fsync. 7K ops/sec at c=100 is slower than the old silent-
fallback number but it's now **correctly durable** (actual fsync happens).

## Next steps to restore quorum throughput

1. **Parallelize the WAL NIF's write path** — match `:prim_file`'s ~128-thread
   parallelism. Either drop O_DIRECT + single-thread design, or use multiple
   writer threads with per-batch fdatasync.
2. Or: **drop the NIF WAL entirely** — `:prim_file` on dirty-IO was faster
   in practice. The reduction burn we previously saw on idle was a separate
   bug (make_wal_conf not threading `wal_io_module`) — now that our NIF is
   properly registered, if we also skip it and use `file:write/2` we'd get
   the prior 44K but with clean scheduler accounting.

## Async ceiling

Async's 42K ceiling at c=50+ is the BEAM scheduler saturation from earlier
analysis (400 connection handlers on 4 schedulers). That's separate from
the WAL story.
