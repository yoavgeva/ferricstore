# Shard Count Benchmark: 4 vs 32 Shards

## Environment

- **Machine**: macOS, 16 cores (16 schedulers online)
- **OTP**: 28
- **Shard count**: 32 (compile-time)
- **ra system**: 1 shared WAL (32 ra servers)
- **Workers**: 50 concurrent tasks
- **Duration**: 15s per test
- **Date**: 2026-03-23

## Results

| Test | 4 shards | 32 shards | Improvement |
|------|----------|-----------|-------------|
| Write (raft ON) | 254/sec | 435/sec | 1.71x |
| Write (raft OFF) | 6,037/sec | 6,594/sec | 1.09x |
| Read | 105,000/sec | 102,069/sec | 0.97x |
| Mixed 80/20 | N/A | 535/sec | -- |

## Latency (32 shards)

| Test | p50 | p95 | p99 |
|------|-----|-----|-----|
| Write (raft ON) | 99.0ms | 194.8ms | 310.4ms |
| Write (raft OFF) | 548us | 35.1ms | 63.9ms |
| Read | 152us | 395us | 8.5ms |
| Mixed 80/20 | 93.1ms | 209.6ms | 399.0ms |

## Resource Usage (32 shards)

| Test | Scheduler util | Memory delta |
|------|---------------|-------------|
| Write (raft ON) | 29.8% | +26.2 MB |
| Write (raft OFF) | 38.4% | +49.3 MB |
| Read | 49.9% | +13.4 MB |
| Mixed 80/20 | 27.9% | +3.3 MB |

## Analysis

### Raft ON Writes

With 4 shards and 50 writers, each shard handles ~12.5 concurrent writers.
All writes funnel through ra's single WAL, so the fdatasync bottleneck limits
throughput to ~254/sec regardless of shard count.

With 32 shards, each shard handles ~1.5 writers. The hypothesis is that more
ra servers means more entries accumulated per WAL flush cycle, amortizing the
fdatasync cost across more writes. Result: 435/sec
(1.71x improvement).

### Raft OFF Writes

The no-raft path goes through Shard GenServers. With 4 shards, 50 writers
create ~12.5 messages queued per GenServer mailbox, causing head-of-line
blocking. With 32 shards, only ~1.5 writers per GenServer, dramatically
reducing mailbox contention. Result: 6,594/sec
(1.09x vs 4-shard baseline of 6,037/sec).

### Reads

Reads bypass the GenServer entirely (ETS direct lookup). More shards means
more ETS tables, which could reduce `write_concurrency` contention on the
LFU touch path. Result: 102,069/sec
(0.97x vs 4-shard baseline of 105,000/sec).

### Mixed 80/20

Combined workload with 535/sec at p50=93.1ms.
