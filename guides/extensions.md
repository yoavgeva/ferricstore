# Extensions

FerricStore ships with extension packages that integrate with the Elixir ecosystem.

## Overview

| Package | What It Does |
|---------|-------------|
| `ferricstore_session` | Plug.Session.Store adapter — durable sessions that survive restarts |
| `ferricstore_ecto` | Transparent Ecto L2 cache |

## Session Store

**Package:** `ferricstore_session`
**Drop-in replacement for:** Cookie store or ETS session store
**Key benefit:** Sessions survive deploys, restarts, and node failures

```elixir
# mix.exs
{:ferricstore_session, "~> 0.3.2"}

# In your Phoenix endpoint:
plug Plug.Session,
  store: FerricStore.Session.Store,
  key: "_my_app_key",
  ttl: 3600,
  prefix: "sess"
```

Unlike cookie sessions (4KB limit, client-side), FerricStore sessions are server-side with no size limit. Unlike ETS sessions, they persist on disk — deploying your app doesn't log out your users.

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

### Architecture

```
Your Application
├── MyApp.Repo (Ecto API)
│   └── FerricStore.Ecto.CachedRepo
│       └── FerricstoreEcto.Cache
│           └── FerricStore
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

The extension packages depend on `ferricstore` (the core engine). You must have FerricStore configured and running in your application's supervision tree before using any extension. See the [Getting Started](getting-started.md) guide for FerricStore setup.

### Version Compatibility

| Package | FerricStore | Ecto | Elixir |
|---------|-------------|------|--------|
| `ferricstore_ecto 0.3.x` | `~> 0.3.2` | `~> 3.11` | `~> 1.19` |
