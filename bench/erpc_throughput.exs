# FerricStore erpc Benchmark
#
# Measures throughput and latency for FerricStore operations over
# Erlang distribution (erpc). Compare against memtier_benchmark
# results for Redis protocol to measure protocol overhead.
#
# Usage:
#   # Single node (embedded — no network):
#   mix run bench/erpc_throughput.exs
#
#   # Remote node (erpc — Erlang distribution):
#   # Terminal 1: start FerricStore
#   elixir --sname ferricstore --cookie bench -S mix run --no-halt
#
#   # Terminal 2: run benchmark
#   elixir --sname bench_client --cookie bench -S mix run bench/erpc_throughput.exs --remote ferricstore@hostname
#
# Equivalent memtier command for comparison:
#   memtier_benchmark -s 127.0.0.1 -p 6379 --protocol=resp3 \
#     --clients=50 --threads=4 --requests=100000 \
#     --ratio=1:1 --data-size=100

remote_node =
  case System.argv() do
    ["--remote", node_str] -> String.to_atom(node_str)
    _ -> nil
  end

if remote_node do
  IO.puts("Connecting to #{remote_node}...")
  Node.connect(remote_node) || raise "Cannot connect to #{remote_node}"
  IO.puts("Connected.")
end

# Configuration — match memtier defaults
key_max = 1_000_000
payload = String.duplicate("x", 100)

# Pre-populate some keys for GET tests
IO.puts("Pre-populating 10K keys...")
for i <- 1..10_000 do
  key = "bench:#{i}"
  if remote_node do
    :erpc.call(remote_node, FerricStore, :set, [key, payload])
  else
    FerricStore.set(key, payload)
  end
end
IO.puts("Done.")

set_fn = if remote_node do
  fn -> :erpc.call(remote_node, FerricStore, :set, ["bench:#{:rand.uniform(key_max)}", payload]) end
else
  fn -> FerricStore.set("bench:#{:rand.uniform(key_max)}", payload) end
end

get_fn = if remote_node do
  fn -> :erpc.call(remote_node, FerricStore, :get, ["bench:#{:rand.uniform(10_000)}"]) end
else
  fn -> FerricStore.get("bench:#{:rand.uniform(10_000)}") end
end

hset_fn = if remote_node do
  fn -> :erpc.call(remote_node, FerricStore, :hset, ["bench:hash:#{:rand.uniform(key_max)}", %{"field" => payload}]) end
else
  fn -> FerricStore.hset("bench:hash:#{:rand.uniform(key_max)}", %{"field" => payload}) end
end

lpush_fn = if remote_node do
  fn -> :erpc.call(remote_node, FerricStore, :rpush, ["bench:list:#{:rand.uniform(key_max)}", [payload]]) end
else
  fn -> FerricStore.rpush("bench:list:#{:rand.uniform(key_max)}", [payload]) end
end

sadd_fn = if remote_node do
  fn -> :erpc.call(remote_node, FerricStore, :sadd, ["bench:set:#{:rand.uniform(key_max)}", [payload]]) end
else
  fn -> FerricStore.sadd("bench:set:#{:rand.uniform(key_max)}", [payload]) end
end

incr_fn = if remote_node do
  fn -> :erpc.call(remote_node, FerricStore, :incr, ["bench:counter:#{:rand.uniform(key_max)}"]) end
else
  fn -> FerricStore.incr("bench:counter:#{:rand.uniform(key_max)}") end
end

mode = if remote_node, do: "erpc (#{remote_node})", else: "embedded (local)"
IO.puts("\nBenchmark mode: #{mode}")
IO.puts("Payload: #{byte_size(payload)} bytes")
IO.puts("Key space: #{key_max}")
IO.puts("Concurrency: 50 parallel processes\n")

Benchee.run(
  %{
    "SET" => set_fn,
    "GET" => get_fn,
    "HSET" => hset_fn,
    "LPUSH" => lpush_fn,
    "SADD" => sadd_fn,
    "INCR" => incr_fn
  },
  time: 10,
  warmup: 2,
  parallel: 50,
  formatters: [
    Benchee.Formatters.Console
  ]
)
