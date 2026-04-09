# FerricStore Benchmark Plan

Using `memtier_benchmark` (same tool Redis Labs uses for their benchmarks).

## Prerequisites

```bash
# Install on macOS
brew install memtier_benchmark

# Install on Linux
sudo apt install memtier-benchmark
# or build from source: https://github.com/RedisLabs/memtier_benchmark
```

## Phase 1: Baseline (no tuning)

Establish baseline numbers before changing anything.

```bash
# Start FerricStore
mix run --no-halt

# Basic SET throughput (50 clients, 100K requests)
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --command="SET __key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=1000000 \
  --data-size=100

# Basic GET throughput
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --command="GET __key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=1000000

# Mixed read/write (1:1 ratio)
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --ratio=1:1 \
  --data-size=100

# Mixed read/write (1:10 — read heavy, typical workload)
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --ratio=1:10 \
  --data-size=100
```

Record: ops/sec, p50, p95, p99, p999 latency.

## Phase 2: Vary Concurrency

Same test, different client counts. Find saturation point.

```bash
for CLIENTS in 1 10 25 50 100 200 500; do
  echo "=== $CLIENTS clients ==="
  memtier_benchmark -s 127.0.0.1 -p 6379 \
    --protocol=resp3 \
    --clients=$CLIENTS --threads=4 \
    --requests=100000 \
    --ratio=1:1 \
    --data-size=100 \
    --hide-histogram
done
```

Expected: throughput increases with clients until CPU or fsync saturates,
then plateaus. Latency increases gradually then spikes at saturation.

## Phase 3: Vary Payload Size

```bash
for SIZE in 10 100 512 1024 4096 16384 65536; do
  echo "=== $SIZE bytes ==="
  memtier_benchmark -s 127.0.0.1 -p 6379 \
    --protocol=resp3 \
    --clients=50 --threads=4 \
    --requests=50000 \
    --command="SET __key__ __data__" \
    --key-pattern=R:R --key-minimum=1 --key-maximum=100000 \
    --data-size=$SIZE \
    --hide-histogram
done
```

Expected: throughput drops as payload grows (more bytes per fsync).

## Phase 4: Pipeline Depth

```bash
for PIPELINE in 1 4 8 16 32 64; do
  echo "=== pipeline $PIPELINE ==="
  memtier_benchmark -s 127.0.0.1 -p 6379 \
    --protocol=resp3 \
    --clients=50 --threads=4 \
    --requests=100000 \
    --pipeline=$PIPELINE \
    --ratio=1:1 \
    --data-size=100 \
    --hide-histogram
done
```

Expected: big throughput jump from pipeline=1 to pipeline=8-16.
Diminishing returns beyond 32.

## Phase 5: Shard Count (restart FerricStore between each)

```bash
for SHARDS in 2 4 8 16 32; do
  echo "=== $SHARDS shards ==="
  # Restart FerricStore with FERRICSTORE_SHARD_COUNT=$SHARDS
  # Wait for startup, then:
  memtier_benchmark -s 127.0.0.1 -p 6379 \
    --protocol=resp3 \
    --clients=50 --threads=4 \
    --requests=100000 \
    --ratio=1:1 \
    --data-size=100 \
    --hide-histogram
done
```

Expected: write throughput scales linearly with shards (more parallel
fsyncs) until CPU saturates. Read throughput may decrease slightly
(more ETS tables, less cache locality).

## Phase 6: BEAM VM Flags (Linux only)

```bash
# Baseline (no flags)
ERL_FLAGS="" mix run --no-halt

# With scheduler pinning
ERL_FLAGS="+sbt db" mix run --no-halt

# With all flags
ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128" mix run --no-halt
```

Run the Phase 1 baseline test after each restart. Compare.

## Phase 7: Compound Operations

```bash
# HSET throughput
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --command="HSET __key__ field __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=100000 \
  --data-size=100

# LPUSH throughput
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --command="LPUSH __key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=100000 \
  --data-size=100

# SADD throughput
memtier_benchmark -s 127.0.0.1 -p 6379 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --command="SADD __key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=100000 \
  --data-size=100
```

Compare against SET throughput — should be similar now that compound
operations bypass GenServer.

## Phase 8: Compare Against Redis

Run the exact same benchmarks against Redis on the same machine.

```bash
# Start Redis
redis-server --port 6380

# Same benchmark against Redis
memtier_benchmark -s 127.0.0.1 -p 6380 \
  --protocol=resp3 \
  --clients=50 --threads=4 \
  --requests=100000 \
  --ratio=1:1 \
  --data-size=100
```

## Recording Results

For each test, record in a table:

| Test | Config | ops/sec | p50 (ms) | p95 (ms) | p99 (ms) | p999 (ms) |
|------|--------|---------|----------|----------|----------|-----------|
| SET baseline | 4 shards, no flags | | | | | |
| SET + sbt db | 4 shards, +sbt db | | | | | |
| SET 16 shards | 16 shards, +sbt db | | | | | |
| GET baseline | 4 shards, no flags | | | | | |
| HSET baseline | 4 shards, no flags | | | | | |
| Redis SET | default config | | | | | |

## Key Questions to Answer

1. What is our SET ops/sec vs Redis? (target: within 2x)
2. Does +sbt db help on Linux? (expect 10-20%)
3. What shard count maximizes write throughput?
4. At what shard count do reads start degrading?
5. How much does pipeline depth help?
6. Are compound ops (HSET, LPUSH) as fast as SET now?
7. What's the throughput ceiling? (fdatasync limited)
