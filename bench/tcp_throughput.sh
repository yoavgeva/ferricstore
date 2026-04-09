#!/bin/bash
# FerricStore TCP Benchmark (via memtier_benchmark)
#
# Run the SAME workload as bench/erpc_throughput.exs but over Redis protocol.
# Compare results to measure TCP/RESP3 overhead vs Erlang distribution.
#
# Prerequisites:
#   brew install memtier_benchmark   # macOS
#   apt install memtier-benchmark    # Linux
#
# Usage:
#   # Start FerricStore first:
#   mix run --no-halt
#
#   # Then run this:
#   bash bench/tcp_throughput.sh

HOST=${1:-127.0.0.1}
PORT=${2:-6379}
CLIENTS=50
THREADS=4
REQUESTS=100000
DATA_SIZE=100
KEY_MAX=1000000

echo "=== FerricStore TCP Benchmark ==="
echo "Host: $HOST:$PORT"
echo "Clients: $CLIENTS, Threads: $THREADS"
echo "Requests: $REQUESTS, Payload: ${DATA_SIZE}B"
echo ""

echo "--- SET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SET __key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- GET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="GET __key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

echo ""
echo "--- HSET ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="HSET __key__ field __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- LPUSH ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="LPUSH __key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- SADD ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="SADD __key__ __data__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "--- INCR ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --command="INCR __key__" \
  --key-pattern=R:R --key-minimum=1 --key-maximum=$KEY_MAX \
  --hide-histogram

echo ""
echo "--- Mixed SET/GET 1:1 ---"
memtier_benchmark -s $HOST -p $PORT \
  --protocol=resp3 \
  --clients=$CLIENTS --threads=$THREADS \
  --requests=$REQUESTS \
  --ratio=1:1 \
  --data-size=$DATA_SIZE \
  --hide-histogram

echo ""
echo "=== Done ==="
