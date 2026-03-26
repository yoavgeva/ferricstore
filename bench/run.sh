#!/usr/bin/env bash
set -euo pipefail

echo "=== FerricStore Benchmark Suite ==="
echo "Using memtier_benchmark (same tool as Dragonfly/Redis)"
echo ""

SERVER=${SERVER:-127.0.0.1}
PORT=${PORT:-6379}
THREADS=${THREADS:-4}
CLIENTS=${CLIENTS:-20}
REQUESTS=${REQUESTS:-200000}
DATA_SIZE=${DATA_SIZE:-256}

echo "--- Pre-populating 100K keys ---"
memtier_benchmark -s $SERVER -p $PORT --ratio 1:0 -t 2 -c 5 -n 10000 \
  --data-size $DATA_SIZE --key-maximum 100000 --hide-histogram --distinct-client-seed

echo ""
echo "--- Test 1: Write-only (SET) ---"
memtier_benchmark -s $SERVER -p $PORT --ratio 1:0 -t $THREADS -c $CLIENTS \
  -n $REQUESTS --data-size $DATA_SIZE --distinct-client-seed --print-percentiles 50,99,99.9

echo ""
echo "--- Test 2: Read-only (GET) ---"
memtier_benchmark -s $SERVER -p $PORT --ratio 0:1 -t $THREADS -c $CLIENTS \
  -n $REQUESTS --data-size $DATA_SIZE --distinct-client-seed --print-percentiles 50,99,99.9

echo ""
echo "--- Test 3: Mixed 1:10 (SET:GET) ---"
memtier_benchmark -s $SERVER -p $PORT --ratio 1:10 -t $THREADS -c $CLIENTS \
  -n $REQUESTS --data-size $DATA_SIZE --distinct-client-seed --print-percentiles 50,99,99.9

echo ""
echo "--- Test 4: Pipelined GET (pipeline=10) ---"
memtier_benchmark -s $SERVER -p $PORT --ratio 0:1 -t $THREADS -c 5 \
  -n 2000000 --pipeline=10 --data-size $DATA_SIZE --distinct-client-seed --print-percentiles 50,99,99.9

echo ""
echo "--- Test 5: Large values (4KB) ---"
memtier_benchmark -s $SERVER -p $PORT --ratio 1:0 -t $THREADS -c $CLIENTS \
  -n 50000 --data-size 4096 --distinct-client-seed --print-percentiles 50,99,99.9

echo ""
echo "=== Benchmark Complete ==="
