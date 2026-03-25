# FerricstoreEcto

Transparent L2 cache for Ecto, powered by [FerricStore](https://github.com/YoavGivati/ferricstore).

Change one line in your Repo, mark schemas as cacheable, and every `Repo.get` / `Repo.all` is automatically cached. Mutations invalidate automatically. No manual cache keys, no stale data bugs.

## The Problem

Ecto has no built-in caching. Every `Repo.get(User, 42)` hits the database, even for data that rarely changes. The typical workaround is manual Cachex/ConCache code scattered across your contexts:

```elixir
# Before: manual caching everywhere
def get_user(id) do
  case Cachex.get(:my_cache, "user:#{id}") do
    {:ok, nil} ->
      user = Repo.get(User, id)
      if user, do: Cachex.put(:my_cache, "user:#{id}", user, ttl: :timer.minutes(5))
      user
    {:ok, user} ->
      user
  end
end

# And you have to remember to invalidate:
def update_user(user, attrs) do
  case Repo.update(User.changeset(user, attrs)) do
    {:ok, updated} = result ->
      Cachex.del(:my_cache, "user:#{updated.id}")
      result
    error -> error
  end
end
```

This is error-prone, repetitive, and easy to forget (stale cache bugs).

## The Solution

```elixir
# After: automatic caching, zero boilerplate
defmodule MyApp.Repo do
  use FerricStore.Ecto.CachedRepo,  # was: use Ecto.Repo
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end

defmodule MyApp.User do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable, ttl: :timer.minutes(10)

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end
end

# Existing code works unchanged -- caching happens automatically
user = Repo.get(User, 42)     # DB hit, cached
user = Repo.get(User, 42)     # cache hit, no DB
Repo.update(changeset)         # DB write, cache invalidated
user = Repo.get(User, 42)     # DB hit, re-cached
```

## Installation

Add `ferricstore_ecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ferricstore, "~> 0.1.0"},
    {:ferricstore_ecto, "~> 0.1.0"}
  ]
end
```

## Quick Start

### Step 1: Switch Your Repo

Replace `use Ecto.Repo` with `use FerricStore.Ecto.CachedRepo`:

```elixir
defmodule MyApp.Repo do
  use FerricStore.Ecto.CachedRepo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

All options you pass (`otp_app`, `adapter`, `pool_size`, etc.) are forwarded to `Ecto.Repo` unchanged.

### Step 2: Mark Schemas as Cacheable

Add `use FerricStore.Ecto.Cacheable` to schemas you want cached:

```elixir
defmodule MyApp.User do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable, ttl: :timer.minutes(10)

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end
end
```

Schemas without `Cacheable` (audit logs, event streams, metrics) always hit the database. This is intentional -- you choose what gets cached.

### Step 3: Use Repo as Normal

No code changes needed. All `Repo.get`, `Repo.all`, `Repo.one` calls are automatically cache-aware. All `Repo.insert`, `Repo.update`, `Repo.delete` calls automatically invalidate.

## How It Works

### Two Cache Tiers

**Entity cache** -- Each row is stored as a FerricStore hash keyed by `ecto:{table}:{primary_key}`. Schema fields are stored as individual hash fields, enabling efficient per-field reads without deserializing the entire row.

```
Key:    ecto:users:42
Fields: name => "Alice", email => "alice@example.com", active => "true"
```

**Query cache** -- Query results store only primary keys (as a LIST for ordered queries, SET for unordered). A per-table generation counter (`ecto:gen:{table}`) is embedded in the cache key. When the table is mutated, the counter increments and all old-generation query cache entries become unreachable -- no expensive bulk DELs needed.

```
Key:    ecto:q:123456:g0:pks     (query hash + generation)
Value:  LIST ["42", "17", "3"]   (primary keys only)
```

### Invalidation Flow

On every mutation (`insert`, `update`, `delete`):

1. The Ecto operation runs first (DB write)
2. Only on `{:ok, struct}` result:
   - `DEL ecto:{table}:{pk}` -- remove the entity cache entry
   - `INCR ecto:gen:{table}` -- bump generation counter, orphaning all query caches

On `update_all` / `delete_all`:

1. The Ecto operation runs first
2. All entity cache entries for the table are deleted (prefix scan)
3. Generation counter is bumped

The cache is **never surgically updated** -- the next read repopulates from the database. This eliminates an entire class of stale-data bugs.

### Read Path (Repo.get)

```
Repo.get(User, 42)
  |
  v
Is User cacheable?  --no--> DB query (super)
  |
  yes
  |
  v
HGETALL ecto:users:42
  |
  v
Hit? --yes--> deserialize fields, return struct
  |
  no (miss)
  |
  v
DB query (super)
  |
  v
HSET ecto:users:42 {fields...}
EXPIRE ecto:users:42 {ttl}
  |
  v
Return struct
```

### Read Path (Repo.all)

```
Repo.all(query)
  |
  v
Is schema cacheable + query cacheable?  --no--> DB query (super)
  |
  yes
  |
  v
Read generation counter (gen1)
  |
  v
Read PK list from ecto:q:{hash}:g{gen1}:pks
  |
  v
Hit? --yes--> Re-read generation (gen2)
  |             |
  |             gen1 == gen2? --no--> DB query (miss)
  |             |
  |             yes --> resolve each PK through entity cache
  |                     |
  |                     all hit? --yes--> return structs
  |                     |
  |                     partial miss --> DB query (fallback)
  |
  no (miss)
  |
  v
DB query (super)
  |
  v
Store PK list + populate entity cache for each row
  |
  v
Return structs
```

## Cache Modes (Per-Query Control)

Every Repo operation accepts a `:cache` option:

| Mode | Syntax | Reads cache? | Populates cache? |
|------|--------|:---:|:---:|
| Normal (default) | `cache: true` | Yes | Yes |
| Bypass | `cache: false` | No | No |
| Refresh | `cache: :refresh` | No | Yes |
| Cache only | `cache: :cache_only` | Yes | No DB fallback |
| Put only | `cache: :put_only` | No | Yes |
| Custom TTL | `cache: [ttl: ms]` | Yes | Yes (custom TTL) |

### Examples

```elixir
# Normal (default): read from cache, populate on miss
user = Repo.get(User, 42)

# Bypass: skip cache entirely, useful for admin panels or debugging
user = Repo.get(User, 42, cache: false)

# Refresh: force DB fetch, update cache -- useful after out-of-band DB updates
user = Repo.get(User, 42, cache: :refresh)

# Cache only: return nil on miss instead of hitting DB
user = Repo.get(User, 42, cache: :cache_only)

# Custom TTL: override the schema-level TTL for this specific read
user = Repo.get(User, 42, cache: [ttl: :timer.hours(1)])
```

## Schema Configuration (FerricStore.Ecto.Cacheable)

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:ttl` | `non_neg_integer()` | `300_000` (5 min) | Time-to-live in milliseconds. Use `timer` helpers: `:timer.minutes(10)`, `:timer.hours(1)`. |
| `:region` | `String.t()` | `"default"` | Cache region name. Currently used for grouping; future support for per-region capacity limits. |
| `:strategy` | atom | `:read_write` | Cache strategy. See below. |
| `:enabled` | `boolean()` | `true` | Set to `false` to explicitly disable caching for a schema that previously had it enabled (useful during migrations). |

### Cache Strategies

* `:read_write` (default) -- Cache reads, invalidate on writes. Best for most schemas.

* `:read_only` -- Cache reads. Writes log a warning but still invalidate. Use for reference data that should rarely change (countries, currencies, config flags).

* `:nonstrict_read_write` -- Same as `:read_write` but allows slightly stale reads in exchange for reduced invalidation overhead. (Currently behaves identically to `:read_write`; reserved for future optimization.)

### Examples

```elixir
# Frequently read, rarely written
defmodule MyApp.User do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable,
    ttl: :timer.minutes(10),
    strategy: :read_write
  # ...
end

# Reference data, almost never changes
defmodule MyApp.Country do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable,
    ttl: :timer.hours(24),
    strategy: :read_only
  # ...
end

# Write-heavy, should NOT be cached
defmodule MyApp.AuditLog do
  use Ecto.Schema
  # No `use FerricStore.Ecto.Cacheable` -- always hits DB
  # ...
end

# Temporarily disable caching during a migration
defmodule MyApp.Product do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable,
    ttl: :timer.minutes(5),
    enabled: false  # disable until migration completes
  # ...
end
```

## What Gets Cached (and What Doesn't)

### Cached Operations

| Operation | Cached? | Notes |
|-----------|:---:|-------|
| `Repo.get(Schema, id)` | Yes | Entity cache (by PK) |
| `Repo.get!(Schema, id)` | Yes | Same, raises on nil |
| `Repo.one(query)` | Yes | Query cache + entity cache |
| `Repo.one!(query)` | Yes | Same, raises on nil |
| `Repo.all(query)` | Yes | Query cache (PK list) + entity cache |

### Always Hit DB (But Populate Cache)

| Operation | Why |
|-----------|-----|
| `Repo.get_by(Schema, clauses)` | Keyed by arbitrary clauses, not PK |
| `Repo.get_by!(Schema, clauses)` | Same |

### Mutations (Invalidate Cache)

| Operation | Invalidation |
|-----------|-------------|
| `Repo.insert` | DEL entity + INCR generation |
| `Repo.update` | DEL entity + INCR generation |
| `Repo.delete` | DEL entity + INCR generation |
| `Repo.insert_all` | INCR generation |
| `Repo.update_all` | DEL all entities for table + INCR generation |
| `Repo.delete_all` | DEL all entities for table + INCR generation |

### Not Cacheable

Queries with these features are never cached (always hit DB):

- Queries with locks (`lock: "FOR UPDATE"`)
- Queries with CTEs / combinations (UNION, INTERSECT, EXCEPT)
- Queries with subqueries in FROM position
- Bare table queries without a schema module

## Field Serialization (FerricstoreEcto.FieldCodec)

Every Ecto field type has a deterministic encode/decode round-trip:

| Ecto Type | Stored As | Example |
|-----------|-----------|---------|
| `:string` | Raw string | `"Alice"` |
| `:integer` / `:id` | Integer string | `"42"` |
| `:float` | Float string | `"3.14"` |
| `:boolean` | `"true"` / `"false"` | `"true"` |
| `:utc_datetime` | ISO 8601 | `"2024-01-15T10:30:00Z"` |
| `:utc_datetime_usec` | ISO 8601 (microseconds) | `"2024-01-15T10:30:00.123456Z"` |
| `:naive_datetime` | ISO 8601 | `"2024-01-15T10:30:00"` |
| `:date` | ISO 8601 | `"2024-01-15"` |
| `:decimal` | Decimal string | `"99.99"` |
| `:binary` | Base64 | `"aGVsbG8="` |
| `:map` / `{:map, _}` | JSON | `"{\"key\":\"value\"}"` |
| `{:array, _}` | JSON | `"[1,2,3]"` |
| `nil` (any type) | `"\0"` sentinel | Distinguishes nil from empty string |

Schema evolution is handled gracefully: new fields added after a cache entry was written get their default value (nil).

## Comparison: Manual Caching vs FerricstoreEcto

### Before (Manual Cachex)

```elixir
defmodule MyApp.Accounts do
  def get_user(id) do
    case Cachex.get(:cache, "user:#{id}") do
      {:ok, nil} ->
        user = Repo.get(User, id)
        if user, do: Cachex.put(:cache, "user:#{id}", user, ttl: :timer.minutes(5))
        user
      {:ok, user} -> user
    end
  end

  def update_user(user, attrs) do
    case Repo.update(User.changeset(user, attrs)) do
      {:ok, updated} = result ->
        Cachex.del(:cache, "user:#{updated.id}")
        result
      error -> error
    end
  end

  def list_active_users do
    # Do you cache this? What's the cache key? How do you invalidate
    # when any user's active status changes?
    Repo.all(from u in User, where: u.active == true)
  end
end
```

Problems: manual cache keys, easy to forget invalidation, query caching is hard to get right, code duplication across contexts.

### After (FerricstoreEcto)

```elixir
defmodule MyApp.Accounts do
  def get_user(id), do: Repo.get(User, id)
  def update_user(user, attrs), do: Repo.update(User.changeset(user, attrs))
  def list_active_users, do: Repo.all(from u in User, where: u.active == true)
end
```

Same API, automatic caching, automatic invalidation, query caching included.

## Testing

FerricStore provides sandbox mode for async-safe test isolation:

```elixir
# test/test_helper.exs
FerricStore.Sandbox.mode(:auto)

# test/my_app/accounts_test.exs
defmodule MyApp.AccountsTest do
  use MyApp.DataCase, async: true

  test "get_user returns cached result on second call" do
    user = insert(:user)

    # First call: DB hit, populates cache
    assert Repo.get(User, user.id) == user

    # Second call: cache hit (you can verify with telemetry or logs)
    assert Repo.get(User, user.id) == user
  end

  test "update invalidates cache" do
    user = insert(:user, name: "Alice")

    # Cache the user
    Repo.get(User, user.id)

    # Update in DB
    {:ok, updated} = Repo.update(User.changeset(user, %{name: "Bob"}))

    # Next read should return updated data
    assert Repo.get(User, updated.id).name == "Bob"
  end

  test "bypass cache for admin operations" do
    user = insert(:user)
    assert Repo.get(User, user.id, cache: false) == user
  end
end
```

## Limitations

- **Associations are not cached.** Only the schema's own fields are stored. Preloaded associations are stripped during serialization. Use separate `Repo.get` calls for associated schemas.
- **Composite primary keys** are supported but stored as colon-separated strings (`"pk1:pk2"`). This works correctly as long as PK values don't contain literal colons.
- **No cache warming.** The cache starts empty and populates on first read. For critical hot paths, you can use `cache: :put_only` in a startup task.
- **update_all / delete_all** invalidate all entity cache entries for the table (prefix scan + delete). On tables with millions of cached rows, this can be expensive. For bulk operations on large tables, consider using `cache: false`.
- **Query cache keys** use `:erlang.phash2/1` which has a 32-bit hash space. Collisions are theoretically possible but extremely unlikely for practical query volumes.

## License

MIT
