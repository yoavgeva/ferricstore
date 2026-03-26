# Getting Started

This guide walks you through installing FerricStore, configuring it for your use case, and running your first commands. By the end, you will have a working key-value store -- either embedded inside your Elixir application or running as a standalone Redis-compatible server.

## Choosing a Deployment Mode

FerricStore supports two operational modes:

- **Embedded** -- the store runs inside your Elixir application as an OTP supervised process tree. No TCP listener, no RESP3 parsing, no network overhead. You interact with it through the `FerricStore` Elixir module. This is the recommended mode when FerricStore and your application live in the same BEAM node.

- **Standalone** -- FerricStore runs as a full Redis-compatible server with a TCP listener, RESP3 protocol handling, ACL authentication, TLS, health endpoints, and Prometheus metrics. Connect with `redis-cli`, Redix, or any Redis client library in any language.

You can always start with embedded mode and add standalone later -- the core engine is the same.

## Installation

### Embedded Mode

Add `ferricstore` to your dependencies:

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.1.0"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

### Standalone Mode

For standalone mode, you also need `ferricstore_server` which includes the TCP/TLS listener:

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.1.0"},
    {:ferricstore_server, "~> 0.1.0"}
  ]
end
```

Or clone the repository and run directly:

```bash
git clone https://github.com/YoavGivati/ferricstore.git
cd ferricstore
mix deps.get
mix run --no-halt
```

### Docker

```bash
docker build -t ferricstore .
docker run -p 6379:6379 -v ferricstore_data:/data ferricstore
```

Environment variables (set via `docker run -e` or in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_PORT` | `6379` | TCP port for RESP3 listener |
| `FERRICSTORE_HEALTH_PORT` | `6380` | HTTP health/metrics port |
| `FERRICSTORE_DATA_DIR` | `/data` | Data directory inside the container |
| `FERRICSTORE_SHARD_COUNT` | `0` (auto = scheduler count) | Number of shards |
| `FERRICSTORE_DURABILITY` | `quorum` | Default durability (`quorum` or `async`) |
| `FERRICSTORE_NODE_NAME` | unset | Erlang node name (enables clustering) |
| `FERRICSTORE_COOKIE` | `ferricstore` | Erlang distribution cookie |
| `FERRICSTORE_CLUSTER_NODES` | unset | Comma-separated list of peer node names |

For a 3-node cluster with HAProxy: `docker compose up -d`

### Release Build

```bash
MIX_ENV=prod mix release ferricstore
```

Output at `_build/prod/rel/ferricstore/`. Start with:

```bash
_build/prod/rel/ferricstore/bin/ferricstore start
```

### Benchmarking

Requires [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark):

```bash
# macOS
brew install memtier_benchmark

# Run the full suite
./bench/run.sh
```

Tests: write-only (SET), read-only (GET), mixed 1:10, pipelined GET (pipeline=10), large values (4KB).

## Configuration

### Minimal Configuration (Embedded)

```elixir
# config/config.exs
config :ferricstore, :mode, :embedded
config :ferricstore, :data_dir, "priv/ferricstore_data"
```

### Minimal Configuration (Standalone)

```elixir
# config/config.exs
config :ferricstore, :port, 6379
config :ferricstore, :data_dir, "data"
```

### Test Configuration

FerricStore provides Ecto-level test sandbox isolation. Configure test mode with ephemeral ports, temporary data, and sandbox enabled:

```elixir
# config/test.exs
config :ferricstore, :port, 0
config :ferricstore, :health_port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_test_#{:os.getpid()}"
config :ferricstore, :shard_count, 4
config :ferricstore, :max_memory_bytes, 1_073_741_824
config :ferricstore, :eviction_policy, :volatile_lru
config :ferricstore, :sandbox_enabled, true

# Generous supervisor budget for tests that kill shards
config :ferricstore, :supervisor_max_restarts, {1000, 60}

# Disable node discovery during tests
config :libcluster, topologies: :disabled
```

For a complete configuration reference, see the [Configuration Guide](configuration.md).

## Basic Usage (Embedded)

Once FerricStore starts with your application, use the `FerricStore` module directly:

### Strings

```elixir
# SET with optional TTL
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))

# GET
{:ok, "alice"} = FerricStore.get("user:42:name")
{:ok, nil} = FerricStore.get("nonexistent")

# DEL
:ok = FerricStore.del("user:42:name")

# EXISTS
true = FerricStore.exists("mykey")
false = FerricStore.exists("nonexistent")

# INCR / INCRBY
{:ok, 1} = FerricStore.incr("counter")
{:ok, 11} = FerricStore.incr_by("counter", 10)
```

### Hash

```elixir
# HSET
:ok = FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})

# HGET
{:ok, "alice"} = FerricStore.hget("user:42", "name")
{:ok, nil} = FerricStore.hget("user:42", "nonexistent_field")

# HGETALL
{:ok, %{"name" => "alice", "age" => "30"}} = FerricStore.hgetall("user:42")
```

### Lists

```elixir
# LPUSH / RPUSH
{:ok, 3} = FerricStore.lpush("queue", ["a", "b", "c"])
{:ok, 4} = FerricStore.rpush("queue", ["d"])

# LPOP / RPOP
{:ok, "c"} = FerricStore.lpop("queue")
{:ok, "d"} = FerricStore.rpop("queue")

# LRANGE (all elements)
{:ok, elements} = FerricStore.lrange("queue", 0, -1)

# LLEN
{:ok, 2} = FerricStore.llen("queue")
```

### Sets

```elixir
# SADD
{:ok, 3} = FerricStore.sadd("tags", ["elixir", "rust", "redis"])

# SMEMBERS
{:ok, members} = FerricStore.smembers("tags")

# SISMEMBER
true = FerricStore.sismember("tags", "elixir")
false = FerricStore.sismember("tags", "python")

# SCARD
{:ok, 3} = FerricStore.scard("tags")
```

### Sorted Sets

```elixir
# ZADD
{:ok, 2} = FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])

# ZRANGE
{:ok, ["alice", "bob"]} = FerricStore.zrange("leaderboard", 0, -1)

# ZRANGE with scores
{:ok, [{"alice", 100.0}, {"bob", 200.0}]} =
  FerricStore.zrange("leaderboard", 0, -1, withscores: true)

# ZSCORE
{:ok, 100.0} = FerricStore.zscore("leaderboard", "alice")

# ZCARD
{:ok, 2} = FerricStore.zcard("leaderboard")
```

### TTL and Expiration

```elixir
# Set with TTL
:ok = FerricStore.set("session:abc", "data", ttl: :timer.minutes(30))

# Set TTL on existing key
{:ok, true} = FerricStore.expire("user:42", :timer.hours(1))

# Check remaining TTL
{:ok, ms_remaining} = FerricStore.ttl("session:abc")

# Keys without TTL
{:ok, nil} = FerricStore.ttl("permanent_key")
```

### Pipelines

Batch multiple commands into a single group-commit entry:

```elixir
{:ok, results} = FerricStore.pipeline(fn pipe ->
  pipe
  |> FerricStore.Pipe.set("key1", "val1")
  |> FerricStore.Pipe.set("key2", "val2")
  |> FerricStore.Pipe.get("key1")
  |> FerricStore.Pipe.incr("counter")
end)
```

## Basic Usage (Standalone)

Start the server and connect with any Redis client:

```bash
# Start FerricStore
mix run --no-halt

# In another terminal
redis-cli -p 6379
```

All standard Redis commands work:

```
127.0.0.1:6379> SET user:42:name alice EX 3600
OK
127.0.0.1:6379> GET user:42:name
"alice"
127.0.0.1:6379> HSET user:42 name alice age 30
(integer) 2
127.0.0.1:6379> HGETALL user:42
1) "age"
2) "30"
3) "name"
4) "alice"
127.0.0.1:6379> ZADD leaderboard 100 alice 200 bob
(integer) 2
127.0.0.1:6379> ZRANGE leaderboard 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "bob"
4) "200"
127.0.0.1:6379> PING
PONG
```

### Connecting with Redix (from another Elixir app)

```elixir
{:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
{:ok, "OK"} = Redix.command(conn, ["SET", "key", "value"])
{:ok, "value"} = Redix.command(conn, ["GET", "key"])
```

## Testing with Sandbox

FerricStore provides true Ecto-level test isolation. Each test gets private shards, private ETS tables, a private Raft WAL, and a private tmpdir -- the full production stack runs in complete isolation. Tests support `async: true` out of the box.

Requires `config :ferricstore, :sandbox_enabled, true` in test config (set by default).

### Setup

The simplest approach is the built-in case template:

```elixir
use FerricStore.Sandbox.Case
```

Or set up manually for more control:

```elixir
# test/support/case.ex or in each test file
defmodule MyApp.FerricStoreCase do
  use ExUnit.CaseTemplate

  setup do
    sandbox = FerricStore.Sandbox.checkout()
    on_exit(fn -> FerricStore.Sandbox.checkin(sandbox) end)
    %{sandbox: sandbox}
  end
end
```

### Writing Tests

```elixir
defmodule MyApp.CacheTest do
  use MyApp.FerricStoreCase, async: true

  test "stores and retrieves a value" do
    :ok = FerricStore.set("key", "value")
    assert {:ok, "value"} = FerricStore.get("key")
  end

  test "keys are isolated between tests" do
    # This test runs in parallel -- "key" here lives in a completely
    # separate shard/ETS/WAL from the test above.
    assert {:ok, nil} = FerricStore.get("key")
  end
end
```

### Sharing Sandbox with Async Tasks

When your test spawns processes that need to access the same sandbox:

```elixir
test "async task can write to sandbox" do
  task = Task.async(fn ->
    FerricStore.set("from_task", "hello")
  end)

  # Allow the task process to use this test's sandbox
  FerricStore.Sandbox.allow(self(), task.pid)
  Task.await(task)

  assert {:ok, "hello"} = FerricStore.get("from_task")
end
```

### TTL Freeze for Reliable Tests

```elixir
setup do
  sandbox = FerricStore.Sandbox.checkout(freeze_ttl: true)
  on_exit(fn -> FerricStore.Sandbox.checkin(sandbox) end)
  %{sandbox: sandbox}
end

test "expiry works" do
  :ok = FerricStore.set("key", "value", ttl: 100)
  assert {:ok, "value"} = FerricStore.get("key")

  # Force expiry without waiting for real time
  FerricStore.Sandbox.expire_now("key")
  assert {:ok, nil} = FerricStore.get("key")
end
```

### Test Module Tags

FerricStore's test suite uses module tags to categorize slow or disruptive tests. These are excluded from the default `mix test` run:

| Tag | Purpose | Run with |
|-----|---------|----------|
| `@moduletag :shard_kill` | Tests that kill shard processes | `mix test --include shard_kill` |
| `@moduletag :compaction` | Bitcask merge/compaction tests | `mix test --include compaction` |
| `@moduletag :concurrency` | Race condition and stress tests | `mix test --include concurrency` |
| `@moduletag :conn_lifecycle` | Connection lifecycle tests | `mix test --include conn_lifecycle` |
| `@moduletag :bench` | Long-running throughput benchmarks (30s+) | `mix test --include bench` |
| `@moduletag :cluster` | Multi-node `:peer` cluster tests (slow, ~10-30s each) | `mix test --include cluster` |
| `@moduletag :jepsen` | Jepsen-style durability/consistency fault injection | `mix test --include jepsen` |
| `@moduletag :perf` | Performance regression tests | `mix test --include perf` |
| `@moduletag :linux_io_uring` | Linux io_uring backend tests (requires kernel >= 5.1) | `mix test --include linux_io_uring` |
| `@moduletag :large_alloc` | Large allocation tests (>= 512 MiB, may OOM on small runners) | `mix test --include large_alloc` |

Use `ShardHelpers.kill_shard_safely/2` (with built-in rate limiting) instead of raw `Process.exit(pid, :kill)` in shard-kill tests.

## Next Steps

- [Configuration Reference](configuration.md) -- tune memory limits, eviction, Raft, merge, and more
- [Architecture](architecture.md) -- understand the three-tier storage, write/read paths, and NIF design
- [Embedded Mode](embedded-mode.md) -- detailed guide for in-process usage
- [Standalone Mode](standalone-mode.md) -- TLS, health endpoints, Prometheus metrics
- [Commands Reference](commands.md) -- complete list of supported commands
- [Security](security.md) -- ACL, TLS, protected mode
