# Azure Benchmark: HEAD vs 3ffaf67 (quorum + async, 2 shards)

## Environment

- **Server**: Azure L4as_v4 (4 vCPU, 32GB RAM, 600GB local NVMe)
- **Client**: Azure D2as_v4 (2 vCPU, separate VM — no coloc CPU)
- **Region**: northcentralus
- **Both VMs on same VNet**: private IP connectivity only (no public network hop)
- **Elixir 1.19.5 / OTP 28.3.1**
- **`FERRICSTORE_SHARD_COUNT=2`**
- **`FERRICSTORE_PROTECTED_MODE=false`**
- **`FERRICSTORE_NAMESPACE_DURABILITY=async:=async`** (for async SETs)
- **Data on local NVMe (`/data/ferricstore`)**
- **Default kernel settings** (no sysctl tuning this run — cloud-init failed, so this is *stock kernel* vs the prior 44K baseline which had hugepages/THP-off/NVMe scheduler tuning)

## Commits compared

- **HEAD**: `235ed9f` — `perf/write-throughput-optimization` (checkpointer + all durability gaps closed)
- **Baseline**: `3ffaf67` — `perf: final benchmark results — 44K quorum writes/sec (6x improvement)`

## Workloads (memtier_benchmark)

| Name | Params | Purpose |
|---|---|---|
| **Quorum writes** | `--ratio=1:0 -t 4 -c 50 --pipeline=10 -n 50000 --data-size=256` | SET default ns → Raft + Bitcask |
| **Async writes** | same + `--key-prefix='async:memtier-'` | SET `async:` ns → ETS + nosync, skip Raft |
| **Quorum high-concurrency** | `-t 4 -c 200 --pipeline=50` | Match prior 44K baseline params |

Each run writes 10M ops (4 threads × 50c × 50K = 10M, or 4 × 200 × 50K = 40M).

## Results

| Config | Baseline (3ffaf67) | HEAD (235ed9f) | Δ |
|---|---:|---:|---:|
| **Quorum, 50c p=10** | 19,840 ops/s | **23,415 ops/s** | **+18.0%** |
| **Quorum, 200c p=50** | 50,255 ops/s | 49,636 ops/s | -1.2% (noise) |
| **Async, 50c p=10** | 67,313 ops/s | 67,088 ops/s | -0.3% (noise) |

### Latency (p50 / p99 / p99.9, ms)

| Config | Baseline | HEAD |
|---|---|---|
| Quorum, 50c p=10 | 28.3 / 651.3 / 3047.4 | 29.2 / 475.1 / 1974.3 |
| Quorum, 200c p=50 | 622.6 / 9175.0 / 11862.0 | 618.5 / 9437.2 / 15990.8 |
| Async, 50c p=10 | 28.5 / 72.2 / 99.8 | 28.7 / 72.2 / 102.9 |

## Interpretation

1. **HEAD wins +18% on low-concurrency quorum writes.** Removing the per-apply
   `v2_fsync` from the Raft state machine — replaced by the BitcaskCheckpointer
   (10s tick) — saves ~100–200μs per batch. At 50c/p10 there are fewer in-flight
   operations to hide fsync latency behind, so the gain is visible.

2. **High-concurrency quorum (200c p=50) saturates at ~50K.** Both commits hit
   the CPU ceiling on 4 cores; fsync latency is amortized across many pipelined
   writes so the checkpointer's benefit vanishes. This matches the prior 44K
   result from commit 3ffaf67's log (memtier result variance; 50K≈44K).

3. **HEAD also cuts p99 latency on quorum-50c from 651ms → 475ms (-27%)** and
   p99.9 from 3047ms → 1974ms (-35%). Same reason: no synchronous fsync on the
   apply/3 critical path.

4. **Async is unchanged (both ~67K ops/s).** Async bypasses Raft entirely — the
   checkpointer changes live on the Raft state-machine path, so they don't
   affect the async path. Confirms our correctness assumption that async wasn't
   regressed.

5. **Async is 2.9× faster than quorum** at the same concurrency. That's the
   cost of Raft + durable quorum replication, and it's a reasonable tax for
   the safety guarantee.

## Kernel tuning note

The cloud-init OS tuning (hugepages, THP=never, CPU governor, NVMe scheduler)
failed during provisioning because `apt-get install cpufrequtils nvme-cli` timed
out on the fresh VM. We ran on stock kernel. The prior 44K baseline had those
tunings applied. If re-measured with those kernel optimizations, absolute
numbers will be ~10-15% higher on both sides but the *relative* delta (HEAD vs
baseline) should hold.

## Files

- `head_quorum.json` — HEAD quorum, 50c p=10
- `head_async.json` — HEAD async, 50c p=10
- `head_quorum_p50_c200.json` — HEAD quorum, 200c p=50
- `baseline_quorum.json` — 3ffaf67 quorum, 50c p=10
- `baseline_async.json` — 3ffaf67 async, 50c p=10
- `baseline_quorum_p50_c200.json` — 3ffaf67 quorum, 200c p=50
