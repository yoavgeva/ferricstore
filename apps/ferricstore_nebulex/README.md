# FerricstoreNebulex

Nebulex 3.x adapter for [FerricStore](https://github.com/YoavGivati/ferricstore).

Drop-in replacement for `Nebulex.Adapters.Local` backed by FerricStore's persistent, crash-recoverable Bitcask storage engine. Your cache survives BEAM restarts.

## Installation

Add `ferricstore_nebulex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ferricstore, "~> 0.1.0"},
    {:ferricstore_nebulex, "~> 0.1.0"}
  ]
end
```

## Quick Start

```elixir
# 1. Define your cache
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter
end

# 2. Configure it (config/config.exs)
config :my_app, MyApp.Cache,
  prefix: "cache",
  serializer: :erlang_term

# 3. Add to your supervision tree (application.ex)
children = [
  MyApp.Cache
]

# 4. Use it
MyApp.Cache.put("session:42", %{user_id: 42, role: :admin}, ttl: :timer.hours(1))
MyApp.Cache.get("session:42")
#=> %{user_id: 42, role: :admin}
```

## Why FerricStore?

| Feature | Nebulex.Adapters.Local | FerricstoreNebulex.Adapter |
|---------|----------------------|---------------------------|
| Storage | ETS (in-memory) | Bitcask (persistent) |
| Survives restart | No | Yes |
| Eviction | Generation-based | LFU with time decay |
| TTL precision | Millisecond | Millisecond |
| Multi-node | No (use Partitioned) | Raft consensus |
| Data structures | Key-value only | Hash, List, Set, ZSet |
| Value types | Any Elixir term | Any Elixir term |

## Configuration Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `prefix` | `String.t()` | `"nbx"` | Key prefix in FerricStore. All keys are stored as `{prefix}:{key}`. Use different prefixes to namespace multiple caches in the same FerricStore instance. |
| `serializer` | `:erlang_term \| :jason` | `:erlang_term` | Value serialization format. `:erlang_term` supports any Elixir term (structs, tuples, atoms). `:jason` stores JSON -- useful if you also read from non-Elixir clients, but limited to JSON-compatible types. |

### Example: Multiple Caches

```elixir
defmodule MyApp.SessionCache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter
end

defmodule MyApp.QueryCache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter
end

# config/config.exs
config :my_app, MyApp.SessionCache, prefix: "sess"
config :my_app, MyApp.QueryCache, prefix: "qcache"
```

## Supported Operations

All standard Nebulex 3.x operations are supported:

### Key-Value

```elixir
# Put with TTL
MyApp.Cache.put("key", "value", ttl: :timer.minutes(5))

# Put only if key doesn't exist
MyApp.Cache.put_new("key", "value")

# Replace only if key exists
MyApp.Cache.replace("key", "new_value")

# Get
MyApp.Cache.get("key")

# Get and delete atomically
MyApp.Cache.take("key")

# Delete
MyApp.Cache.delete("key")

# Check existence
MyApp.Cache.has_key?("key")
```

### Bulk Operations

```elixir
# Put multiple entries
MyApp.Cache.put_all([{"k1", "v1"}, {"k2", "v2"}], ttl: :timer.hours(1))

# Get multiple entries
MyApp.Cache.get_all(["k1", "k2"])
#=> %{"k1" => "v1", "k2" => "v2"}

# Count all entries
MyApp.Cache.count_all()

# Delete all entries
MyApp.Cache.delete_all()
```

### TTL Management

```elixir
# Set TTL
MyApp.Cache.put("key", "value", ttl: :timer.hours(1))

# Check remaining TTL (milliseconds)
MyApp.Cache.ttl("key")
#=> 3_599_997

# Update TTL on existing key
MyApp.Cache.expire("key", :timer.hours(2))

# Put without changing existing TTL
MyApp.Cache.put("key", "new_value", keep_ttl: true)
```

### Counters

```elixir
# Increment (creates key with value 1 if missing)
MyApp.Cache.incr("page:views")
#=> 1

MyApp.Cache.incr("page:views")
#=> 2

# Increment by arbitrary amount
MyApp.Cache.incr("page:views", 10)
#=> 12

# Decrement
MyApp.Cache.decr("page:views")
#=> 11
```

### Transactions

```elixir
MyApp.Cache.transaction(fn ->
  count = MyApp.Cache.get("counter") || 0
  MyApp.Cache.put("counter", count + 1)
  count + 1
end)
```

### Streaming

```elixir
# Stream all entries
MyApp.Cache.stream()
|> Stream.filter(fn {_key, value} -> value > 100 end)
|> Enum.to_list()
```

## Key Encoding

Binary keys (strings) are stored as-is. Non-binary keys (atoms, integers, tuples) are automatically serialized with `:erlang.term_to_binary/1` and Base64-encoded. This adds roughly 1 microsecond overhead per operation for non-binary keys.

```elixir
# All of these work
MyApp.Cache.put("string_key", "value")
MyApp.Cache.put(:atom_key, "value")
MyApp.Cache.put(42, "value")
MyApp.Cache.put({:user, 42}, "value")
```

## Value Serialization

Values are serialized with `:erlang.term_to_binary/2` (with `:compressed` flag) before storage and deserialized on read. Any Elixir term can be cached transparently:

```elixir
# Structs, maps, tuples, keyword lists -- all work
MyApp.Cache.put("user:42", %User{id: 42, name: "Alice"})
MyApp.Cache.put("config", %{retries: 3, timeout: 5000})
MyApp.Cache.put("tags", [:elixir, :otp, :cache])
```

## Migrating from Cachex

### Before (Cachex)

```elixir
# application.ex
children = [
  {Cachex, name: :my_cache, expiration: expiration(default: :timer.minutes(5))}
]

# usage
Cachex.put(:my_cache, "key", "value")
Cachex.get(:my_cache, "key")
Cachex.del(:my_cache, "key")
Cachex.exists?(:my_cache, "key")
Cachex.incr(:my_cache, "counter")
```

### After (FerricstoreNebulex via Nebulex)

```elixir
# Define cache module
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter
end

# application.ex
children = [MyApp.Cache]

# usage
MyApp.Cache.put("key", "value", ttl: :timer.minutes(5))
MyApp.Cache.get("key")
MyApp.Cache.delete("key")
MyApp.Cache.has_key?("key")
MyApp.Cache.incr("counter")
```

Key differences:
- Nebulex uses a module-based API instead of a named process
- TTL is set per-key at write time, not as a global default
- Data persists across restarts (FerricStore uses Bitcask on disk)
- No generation-based expiration -- entries expire individually via TTL

## Migrating from Nebulex.Adapters.Local

Change one line:

```elixir
# Before
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: Nebulex.Adapters.Local  # <-- ETS-backed, volatile
end

# After
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter  # <-- Bitcask-backed, persistent
end
```

All existing `MyApp.Cache.get/put/delete` calls work unchanged. The only behavioral difference is that data now survives restarts.

## Testing

FerricStore provides a sandbox mode for test isolation. Each test process gets an isolated keyspace:

```elixir
# test/test_helper.exs
FerricStore.Sandbox.mode(:auto)

# test/my_cache_test.exs
defmodule MyApp.CacheTest do
  use ExUnit.Case, async: true

  setup do
    FerricStore.Sandbox.checkout()
    :ok
  end

  test "put and get" do
    MyApp.Cache.put("key", "value")
    assert MyApp.Cache.get("key") == "value"
  end
end
```

## License

MIT
