# FerricStore v2 Architecture — Pure Rust NIFs

## Vision

Rust NIFs are **pure, stateless utility functions**. No HashMap, no Mutex, no internal state. All state management lives in Elixir (ETS, GenServer) where it's observable, debuggable, and hot-upgradable.

## Current (v1) vs Proposed (v2)

### v1: Rust owns the keydir
```
ETS keydir (Elixir) → Rust Store {keydir: HashMap, log_writer, ...} → disk
     ↑ duplicate         ↑ stateful, Mutex-locked
```

### v2: Elixir owns everything, Rust is pure IO
```
ETS keydir (Elixir, single source of truth) → Rust pure functions → disk
     ↑ one keydir                                ↑ no state, no Mutex for reads
```

## Single ETS keydir format

```
{key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
  ↑      ↑            ↑             ↑            ↑        ↑         ↑
  key  hot cache    TTL           eviction    disk location for cold reads
       (nil=cold)
```

## Rust NIF API (pure functions only)

### File IO
```rust
fn append_record(path, key, value, expire, timestamp) -> {offset, record_size, crc}
fn pread_at(path, offset, size) -> bytes
fn fsync(path) -> :ok
fn append_batch(path, records) -> [{offset, size}, ...]
fn append_tombstone(path, key) -> {offset, size}
```

### Hint files
```rust
fn write_hint_file(path, entries) -> :ok
fn read_hint_file(path) -> [{key, file_id, offset, value_size, expire}, ...]
```

### Compaction
```rust
fn copy_records(source_path, dest_path, offsets_and_sizes) -> [{old_offset, new_offset}, ...]
```

### Bloom filter (mmap bit operations)
```rust
fn bloom_create_file(path, num_bits) -> :ok
fn bloom_open(path) -> mmap_ref
fn bloom_close(mmap_ref) -> :ok
fn bloom_set_bits(mmap_ref, positions) -> :ok
fn bloom_check_bits(mmap_ref, positions) -> bool
```

### Math (pure CPU)
```rust
fn crc32(data) -> u32
fn cosine_distance(vec_a, vec_b) -> f64
fn l2_distance(vec_a, vec_b) -> f64
fn inner_product(vec_a, vec_b) -> f64
fn pbkdf2(password, salt, iterations, key_length) -> hash
```

### What's removed from Rust
- `keydir.rs` (518 lines) — entire HashMap-based keydir
- `Store` struct with internal state
- `Mutex<Store>` wrapping
- All `keydir.get/put/delete/iter` calls
- `read_modify_write` (INCR/APPEND logic moves to Elixir)
- `purge_expired` (Elixir scans ETS directly)
- `get/get_all/get_batch/get_range` as stateful operations

## Benefits

1. **~80MB less RAM** per 1M keys (no duplicate keydir in Rust)
2. **No Mutex lock on reads** — ETS read_concurrency is lock-free
3. **Observable** — `:ets.info`, Observer, remote shell can inspect everything
4. **Hot-upgradable** — change keydir format without Rust recompile
5. **Testable** — ExUnit tests keydir logic directly, no NIF loading needed
6. **Debuggable** — Elixir stack traces, not Rust panics behind FFI

## Migration path

Phase 1 (current session): Single-table ETS keydir with LFU
Phase 2: Add file_id/offset/value_size to ETS tuple
Phase 3: New pure NIF API (pread_at, append_record)
Phase 4: Remove Rust keydir, remove Store struct
Phase 5: Remove Mutex (pure functions don't need it)

## Three storage tiers — each with the right tool

```
┌─────────────────────┐
│  ETS keydir          │  Key-value data (strings, hash, list, set, zset)
│  {key, value|nil,    │  Elixir manages hot/cold via LFU
│   expire, lfu,       │  value=nil → cold, pread from Bitcask
│   fid, offset, size} │  value=binary → hot, return from ETS
└──────────┬──────────┘
           │ cold reads
           ▼
┌─────────────────────┐
│  Bitcask data files  │  Append-only log files
│  Pure Rust IO:       │  NIF.pread_at(path, offset, size)
│  append, pread,      │  NIF.append_record(path, key, value)
│  fsync, compact      │  No state, no HashMap, no Mutex
└─────────────────────┘

┌─────────────────────┐
│  mmap files          │  Binary structures (fixed-layout, random access)
│  prob/shard_N/*.bloom│  Bloom: bit array, random bit set/check
│  prob/shard_N/*.cuck │  Cuckoo: bucket array, fingerprint ops
│  prob/shard_N/*.cms  │  CMS: counter matrix, hash-indexed increment
│  prob/shard_N/*.topk │  TopK: CMS + min-heap
│  prob/shard_N/*.tdig │  TDigest: sorted centroid array
│  vectors/shard_N/*.v │  Vectors: flat f32 array, distance math
│                      │
│  OS page cache       │  OS decides hot/cold — not us
│  manages RAM         │  No ETS, no serialization, zero-copy
│  NIF = pointer math  │  Pure Rust: read/write at offset
└─────────────────────┘
```

### Why three tiers, not one:

| Data type | Access pattern | Best storage |
|-----------|---------------|-------------|
| Strings/Hash/List/Set/ZSet | Key lookup, TTL, LFU eviction | **ETS** (we need control over eviction) |
| Bloom/Cuckoo/CMS/TopK/TDigest | Random byte/bit access on fixed-size arrays | **mmap** (OS page cache, zero-copy) |
| Vectors | Sequential/random f32 reads, distance math | **mmap** (OS page cache, SIMD-friendly layout) |

### What NEVER goes in ETS:
- Probabilistic structures (too large, wrong abstraction)
- Vectors (too large, need SIMD-aligned layout)
- Anything >64KB (ETS copies on read)

### What NEVER goes in mmap:
- Key-value data (needs TTL, LFU, per-key eviction)
- Metadata (needs atomic update_element)

## The "should this be in Rust?" test

1. Is it CPU-intensive? (hash, distance, crypto) → **Rust**
2. Is it a syscall wrapper? (pread, fsync, mmap) → **Rust**
3. Is it pointer math on mmap? (bloom bits, CMS counters) → **Rust**
4. Does it have state? → **Elixir**
5. Does it make decisions? → **Elixir**
6. Does it need debugging in production? → **Elixir**
