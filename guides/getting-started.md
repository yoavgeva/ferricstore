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

FerricStore provides test sandbox isolation. Configure test mode with ephemeral ports and temporary data:

```elixir
# config/test.exs
config :ferricstore, :port, 0
config :ferricstore, :health_port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_test_#{:os.getpid()}"
config :ferricstore, :max_memory_bytes, 1_073_741_824
config :ferricstore, :eviction_policy, :volatile_lru

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

FerricStore provides async-safe test isolation, inspired by `Ecto.Adapters.SQL.Sandbox`. Each test gets a unique namespace prefix -- tests can run concurrently without key collisions.

### Setup

```elixir
# test/support/case.ex or in each test file
defmodule MyApp.FerricStoreCase do
  use ExUnit.CaseTemplate

  setup do
    namespace = FerricStore.Sandbox.checkout()
    on_exit(fn -> FerricStore.Sandbox.checkin(namespace) end)
    %{namespace: namespace}
  end
end
```

Or use the built-in case template:

```elixir
use FerricStore.Sandbox.Case
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
    # This test runs in parallel -- "key" here is a different key
    # than "key" in the test above, thanks to sandbox namespacing.
    assert {:ok, nil} = FerricStore.get("key")
  end
end
```

### TTL Freeze for Reliable Tests

```elixir
setup do
  namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)
  on_exit(fn -> FerricStore.Sandbox.checkin(namespace) end)
  %{namespace: namespace}
end

test "expiry works" do
  :ok = FerricStore.set("key", "value", ttl: 100)
  assert {:ok, "value"} = FerricStore.get("key")

  # Force expiry without waiting for real time
  FerricStore.Sandbox.expire_now("key")
  assert {:ok, nil} = FerricStore.get("key")
end
```

## Next Steps

- [Configuration Reference](configuration.md) -- tune memory limits, eviction, Raft, merge, and more
- [Architecture](architecture.md) -- understand the three-tier storage, write/read paths, and NIF design
- [Embedded Mode](embedded-mode.md) -- detailed guide for in-process usage
- [Standalone Mode](standalone-mode.md) -- TLS, health endpoints, Prometheus metrics
- [Commands Reference](commands.md) -- complete list of supported commands
- [Security](security.md) -- ACL, TLS, protected mode
