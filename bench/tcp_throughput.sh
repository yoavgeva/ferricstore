#!/bin/bash
# FerricStore TCP Benchmark (via memtier_benchmark)
#
# Run the SAME workload as bench/erpc_throughput.exs but over Redis protocol.
# Tests both QUORUM and ASYNC durability modes.
#
# To enable async namespace, configure before starting FerricStore:
#   config :ferricstore, namespace_durability: %{"async:" => :async}
#
# Usage:
#   mix run --no-halt   # start FerricStore
#   bash bench/tcp_throughput.sh

HOST=${1:-127.0.0.1}
PORT=${2:-6379}
CLIENTS=50
THREADS=4
REQUESTS=100000
DATA_SIZE=100
KEY_MAX=1000000

echo "============================================"
echo "  FerricStore TCP Benchmark"
echo "  Host: $HOST:$PORT"
echo "  Clients: $CLIENTS, Threads: $THREADS"
echo "  Requests: $REQUESTS, Payload: ${DATA_SIZE}B"
echo "============================================"

# ===================================================================
# QUORUM MODE (default — keys without async: prefix)
# ===================================================================

echo ""
echo "=== QUORUM MODE (durable) ==="

echo ""
echo "--- SET (quorum) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SET bench:q:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- GET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="GET bench:q:__key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

echo ""
echo "--- HSET (quorum) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="HSET bench:q:hash:__key__ field __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- LPUSH (quorum) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="LPUSH bench:q:list:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- SADD (quorum) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SADD bench:q:set:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- INCR (quorum) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="INCR bench:q:counter:__key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

# ===================================================================
# ASYNC MODE (keys with async: prefix)
# Requires: namespace_durability: %{"async:" => :async}
# ===================================================================

echo ""
echo "=== ASYNC MODE (fast, local durability only) ==="
echo "(Keys prefixed with async: — requires namespace config)"

echo ""
echo "--- SET (async) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SET async:bench:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- HSET (async) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="HSET async:bench:hash:__key__ field __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- LPUSH (async) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="LPUSH async:bench:list:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- SADD (async) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SADD async:bench:set:__key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- INCR (async) ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="INCR async:bench:counter:__key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

echo ""
echo "=== Done ==="
