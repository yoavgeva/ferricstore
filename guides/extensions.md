# Extensions

FerricStore ships with two extension packages that integrate with the broader Elixir ecosystem. Both use the same underlying FerricStore engine -- you can use them independently or together.

## Overview

| Package | What It Does | When to Use |
|---------|-------------|-------------|
| `ferricstore_nebulex` | Nebulex 3.x adapter | You want a standard Nebulex cache API backed by persistent storage |
| `ferricstore_ecto` | Transparent Ecto L2 cache | You want automatic caching for `Repo.get` / `Repo.all` with zero boilerplate |

## Nebulex Adapter

**Package:** `ferricstore_nebulex`
**Drop-in replacement for:** `Nebulex.Adapters.Local`
**Key benefit:** Your Nebulex cache survives BEAM restarts

### Quick Start

```elixir
# 1. Define your cache
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter
end

# 2. Configure (config/config.exs)
config :my_app, MyApp.Cache,
  prefix: "cache",
  serializer: :erlang_term

# 3. Add to supervision tree
children = [MyApp.Cache]

# 4. Use it
MyApp.Cache.put("key", "value", ttl: :timer.hours(1))
MyApp.Cache.get("key")
```

### When to Use

- You already use Nebulex and want persistent storage
- You want the standard Nebulex API (portable across adapters)
- You need counters, TTL management, bulk operations, streaming
- You're migrating from Cachex or ConCache to a persistent cache

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `prefix` | `"nbx"` | Key prefix. Use different prefixes to namespace multiple caches. |
| `serializer` | `:erlang_term` | `:erlang_term` (any Elixir term) or `:jason` (JSON-compatible types only). |

See the [full README](apps/ferricstore_nebulex/README.md) for migration guides, all supported operations, and testing setup.

## Ecto L2 Cache

**Package:** `ferricstore_ecto`
**Drop-in replacement for:** `use Ecto.Repo`
**Key benefit:** Automatic caching for Ecto with zero code changes

### Quick Start

```elixir
# 1. Switch your Repo (one-line change)
defmodule MyApp.Repo do
  use FerricStore.Ecto.CachedRepo,  # was: use Ecto.Repo
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end

# 2. Mark schemas as cacheable
defmodule MyApp.User do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable, ttl: :timer.minutes(10)

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end
end

# 3. Existing code works unchanged
Repo.get(User, 42)     # DB hit, cached
Repo.get(User, 42)     # cache hit, no DB
Repo.update(changeset)  # cache invalidated automatically
```

### When to Use

- You have read-heavy schemas where the same rows are fetched repeatedly
- You want caching without manual cache key management
- You want automatic invalidation on every mutation (no stale data bugs)
- You have reference data tables (countries, currencies) that rarely change

### When NOT to Use

- Write-heavy tables (audit logs, event streams) -- don't add `Cacheable`
- Tables where every read is unique (complex analytics queries)
- When you need sub-millisecond read latency for already-fast DB queries

### Schema Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `ttl` | `300_000` (5 min) | Cache TTL in milliseconds |
| `strategy` | `:read_write` | `:read_write`, `:read_only`, or `:nonstrict_read_write` |
| `region` | `"default"` | Cache region name (for grouping) |
| `enabled` | `true` | Set to `false` to disable caching for a schema |

### Per-Query Control

```elixir
Repo.get(User, 42)                     # default: read cache, populate on miss
Repo.get(User, 42, cache: false)       # bypass cache entirely
Repo.get(User, 42, cache: :refresh)    # skip read, repopulate from DB
Repo.get(User, 42, cache: :cache_only) # cache only, no DB fallback
Repo.get(User, 42, cache: [ttl: ms])   # custom TTL for this read
```

See the [full README](apps/ferricstore_ecto/README.md) for the complete invalidation flow, field serialization details, before/after comparisons, and testing setup.

## Using Both Together

Both packages share the same FerricStore backend. You can use the Nebulex adapter for general-purpose caching and the Ecto cache for automatic Repo caching, in the same application:

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.1.0"},
    {:ferricstore_nebulex, "~> 0.1.0"},
    {:ferricstore_ecto, "~> 0.1.0"}
  ]
end
```

```elixir
# General-purpose cache (sessions, rate limits, feature flags)
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricstoreNebulex.Adapter
end

# Ecto L2 cache (automatic Repo caching)
defmodule MyApp.Repo do
  use FerricStore.Ecto.CachedRepo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

The two packages use different key prefixes by default (`"nbx:"` for Nebulex, `"ecto:"` for Ecto), so they coexist without conflicts.

### Architecture Diagram

```
Your Application
├── MyApp.Cache (Nebulex API)
│   └── FerricstoreNebulex.Adapter
│       └── FerricStore  ─────────────┐
│           Keys: nbx:session:abc     │
│           Keys: nbx:rate:ip:1.2.3   │
│                                      │  Same FerricStore
├── MyApp.Repo (Ecto API)             │  engine & storage
│   └── FerricStore.Ecto.CachedRepo   │
│       └── FerricstoreEcto.Cache     │
│           └── FerricStore ──────────┘
│               Keys: ecto:users:42
│               Keys: ecto:gen:users
│               Keys: ecto:q:123:g0:pks
│
└── FerricStore Supervision Tree
    ├── Store.Router (shard routing)
    ├── Store.Shard (per-shard GenServer)
    └── Bitcask (persistent storage engine)
```

## Dependencies

Both extension packages depend on `ferricstore` (the core engine). You must have FerricStore configured and running in your application's supervision tree before using either extension. See the [Getting Started](getting-started.md) guide for FerricStore setup.

### Version Compatibility

| Package | FerricStore | Nebulex | Ecto | Elixir |
|---------|-------------|---------|------|--------|
| `ferricstore_nebulex 0.1.x` | `~> 0.1.0` | `~> 3.0` | -- | `~> 1.19` |
| `ferricstore_ecto 0.1.x` | `~> 0.1.0` | -- | `~> 3.11` | `~> 1.19` |
