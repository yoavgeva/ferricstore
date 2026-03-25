# Ecto L2 Cache & Nebulex Adapter Design

## Part 1: Ecto L2 Cache (FerricStore.Ecto.Cache)

### 1.1 Motivation

Elixir/Ecto has no equivalent to Hibernate's second-level cache. Every `Repo.get`,
`Repo.all`, and `Repo.one` hits the database on every call. Phoenix developers
manually add caching with Cachex or ConCache, but the result is ad-hoc:

- **Scattered cache keys** -- each context invents its own key format
- **No automatic invalidation** -- developers must remember to bust the cache on
  every insert, update, and delete
- **No consistency guarantees** -- stale reads after writes are common and hard to debug
- **No per-schema configuration** -- a single TTL for all entities, or different TTL
  logic copy-pasted across contexts
- **Lost on restart** -- ETS/process-based caches lose all data on deploy

FerricStore.Ecto.Cache provides automatic, transparent query caching:

- **Zero code changes** to existing `Repo.get`, `Repo.all`, `Repo.one` calls
- **Automatic invalidation** on `Repo.insert`, `Repo.update`, `Repo.delete`,
  `Repo.update_all`, `Repo.delete_all`
- **Per-schema configuration** -- opt-in/opt-out, custom TTL, cache strategy
- **Per-query bypass** -- `cache: false` for queries that must always hit the database
- **Persistence across restarts** -- Bitcask storage survives BEAM crashes and deploys
- **Distributed invalidation** -- Raft consensus propagates invalidation across nodes
- **Stampede protection** -- built on `FerricStore.FetchOrCompute` to prevent
  thundering herd on cold keys

### 1.2 Architecture Overview

Two cache tiers, modeled after Hibernate's cache architecture:

```
L2 Entity cache:     FerricStore, keyed by {schema, primary_key}
L2 Query cache:      FerricStore, keyed by query hash
Database:            PostgreSQL/MySQL via Ecto adapter
```

Read flow:

```
Repo.get(User, 42)
  │
  ├─ L2 entity check: FerricStore.hgetall("ecto:users:42")
  │   └─ hit → reconstruct struct from hash fields, return
  │
  └─ Database: SELECT * FROM users WHERE id = 42
      └─ populate L2 hash (HSET), return

Repo.all(from u in User, select: {u.name, u.email}, where: u.active)
  │
  ├─ Query cache check: FerricStore.lrange("ecto:q:{hash}:pks", 0, -1)
  │   └─ hit → for each PK, FerricStore.hmget("ecto:users:{pk}", ["name", "email"])
  │            (reads only 2 fields per entity, not the full row)
  │
  └─ Database: SELECT name, email FROM users WHERE active = true
      └─ populate query cache (RPUSH PKs), populate entity hashes (HSET)
```

Write flow:

```
Repo.update(changeset)  # changeset changes only :email
  │
  ├─ Execute UPDATE SQL (via underlying Ecto adapter)
  │
  ├─ On {:ok, struct} →
  │   ├─ Entity invalidation: FerricStore.del("ecto:users:42")
  │   └─ Invalidate query caches touching "users" table
  │
  └─ On {:error, changeset} → do nothing (no data changed)
```

### 1.3 How Hibernate Does It (Reference Model)

Hibernate's second-level cache with Infinispan is the gold standard. Our design
mirrors its architecture while adapting to Ecto's idioms.

**Entity cache:**
- `@Cacheable` annotation on entity class opts the entity in
- `@Cache(usage = READ_WRITE, region = "users")` sets strategy and region
- Cache key is `{entity_class, primary_key}`
- Cache value is the "dehydrated" entity state (field values, not the full object)

**Query cache:**
- `session.createQuery("...").setCacheable(true)` opts a query in
- Cache key is `{query_string, parameter_values}`
- Cache value is a list of entity primary keys (not full entities)
- On hit, each PK is resolved through the entity cache
- This means query cache hits still benefit from entity cache hits

**Cache modes (per session/query):**

| Mode | Read from cache | Write to cache | Use case |
|------|----------------|----------------|----------|
| NORMAL | Yes | Yes | Default |
| GET | Yes | No | Read-heavy, avoid cache pollution |
| PUT | No | Yes | Pre-warm cache |
| REFRESH | No | Yes | Force reload stale data |
| IGNORE | No | No | Bypass cache entirely |

**Cache regions:**
- Separate cache areas with independent TTL and eviction settings
- `@Cache(region = "users")` routes entity to the "users" region
- Default region for entities without explicit region annotation

**Invalidation:**
- On entity update: invalidate entity cache entry + all query caches touching
  that entity's table
- On entity insert: invalidate all query caches touching that table (new row
  changes result sets)
- On entity delete: invalidate entity cache entry + all query caches
- Bulk operations (`UPDATE ... WHERE`, `DELETE ... WHERE`): invalidate entire
  table's entity and query caches

**Concurrency strategies:**

| Strategy | Description | Lock behavior |
|----------|-------------|---------------|
| READ_ONLY | Never modified after initial load | No locks |
| NONSTRICT_READ_WRITE | Occasional updates, stale reads acceptable | No locks, eventual consistency |
| READ_WRITE | Frequent reads and writes | Soft locks during write |
| TRANSACTIONAL | Full ACID cache transactions | JTA/XA transactions |

### 1.4 Ecto Integration Points

We evaluated four approaches to hooking into Ecto without forking it.

#### Option A: `prepare_query/3` callback (built into Ecto)

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres

  @impl true
  def prepare_query(:all, query, opts) do
    # Can modify query and opts, but CANNOT short-circuit (skip DB)
    # Must return {query, opts}
    {query, opts}
  end
end
```

**Problem:** `prepare_query/3` is called inside `Ecto.Repo.Queryable.execute/4`,
which proceeds to call the database adapter regardless of what we return. The
callback can only modify the query and opts -- it cannot return a cached result.
Its operation scope is also limited: it fires for `:all`, `:update_all`,
`:delete_all`, `:stream`, and `:insert_all` but notably not for the schema-based
`insert/2`, `update/2`, `delete/2`.

**Verdict:** Useful for metadata tagging (we use it to inject cache hints into
opts), but insufficient alone.

#### Option B: Wrapper module around Repo

```elixir
defmodule MyApp.CachedRepo do
  def get(schema, id, opts \\ []) do
    cache_key = "ecto:#{schema.__schema__(:source)}:#{id}"
    case FerricStore.hgetall(cache_key) do
      {:ok, %{} = fields} when map_size(fields) > 0 ->
        struct(schema, deserialize_fields(schema, fields))
      _ ->
        result = MyApp.Repo.get(schema, id, opts)
        if result, do: cache_entity(cache_key, result, schema)
        result
    end
  end
end
```

**Problem:** Requires changing every `Repo.get` call to `CachedRepo.get`
throughout the codebase. Existing code, libraries, and third-party packages
that call `Repo` directly bypass the cache entirely.

**Verdict:** Rejected. Violates the zero-code-change requirement.

#### Option C: Custom `__using__` macro that wraps Ecto.Repo (RECOMMENDED)

```elixir
defmodule MyApp.Repo do
  use FerricStore.Ecto.CachedRepo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

`FerricStore.Ecto.CachedRepo` internally calls `use Ecto.Repo` and then
overrides `get/3`, `get!/3`, `get_by/3`, `get_by!/3`, `one/2`, `one!/2`,
`all/2`, `insert/2`, `update/2`, `delete/2`, `insert_all/3`, `update_all/3`,
and `delete_all/2` with caching wrappers. The user's existing code
(`Repo.get`, `Repo.all`, etc.) works unchanged because `MyApp.Repo` is still
the module being called.

**How it works internally:**

```elixir
defmodule FerricStore.Ecto.CachedRepo do
  defmacro __using__(opts) do
    quote do
      use Ecto.Repo, unquote(opts)

      # Override read operations with cache-aware versions
      defoverridable [
        get: 3, get!: 3, get_by: 3, get_by!: 3,
        one: 2, one!: 2, all: 2,
        insert: 2, update: 2, delete: 2,
        insert_all: 3, update_all: 3, delete_all: 2
      ]

      def get(queryable, id, opts \\ []) do
        FerricStore.Ecto.Cache.cached_get(__MODULE__, queryable, id, opts)
      end

      # ... similar for all overridden functions
    end
  end
end
```

**Why this works:** Ecto.Repo defines `get/3`, `all/2`, etc. as regular
functions via `defdelegate` or direct `def` inside the `__using__` macro, then
marks them with `defoverridable`. Our macro calls `use Ecto.Repo` first
(generating Ecto's implementations), then `defoverridable` again, then defines
our cache-aware versions that delegate to the original Ecto implementations on
cache miss.

To call the original (uncached) Ecto implementation from within our override,
we use `super/2` or store a reference to the original module:

```elixir
def get(queryable, id, opts \\ []) do
  case FerricStore.Ecto.Cache.cached_get(__MODULE__, queryable, id, opts) do
    {:cache_hit, result} -> result
    :cache_miss ->
      result = super(queryable, id, Keyword.delete(opts, :cache))
      FerricStore.Ecto.Cache.populate_entity(queryable, result)
      result
  end
end
```

**Verdict:** Recommended. One-line change (`use FerricStore.Ecto.CachedRepo`
instead of `use Ecto.Repo`), transparent to all callers.

#### Option D: Telemetry-based read path + Repo callback write path

Attach to `[:my_app, :repo, :query]` telemetry events. Use `prepare_query/3`
to inject cache metadata into opts. Override write operations for invalidation.

**Problem:** Telemetry events fire *after* the query executes. They are useful
for metrics and populating the cache on miss, but cannot prevent the database
query from executing. The read path still always hits the database.

**Verdict:** Useful as a complement (we attach telemetry handlers for metrics
and cache-miss population), but cannot serve as the primary caching mechanism.

#### Final Decision: Option C + D

- **Option C** for the primary mechanism: `use FerricStore.Ecto.CachedRepo`
  overrides read and write functions
- **Option D** as a supplement: telemetry handlers for observability and
  cache-miss population from queries that bypass the Repo module (raw SQL, etc.)

### 1.5 Detailed Design

#### 1.5.1 Schema-Level Configuration

Modeled after Hibernate's `@Cacheable` and `@Cache` annotations.

**Opt-in model (default, like Hibernate's ENABLE_SELECTIVE):**

```elixir
defmodule MyApp.User do
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable,
    ttl: :timer.minutes(10),
    region: "users",
    strategy: :read_write

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end
end
```

Schemas that do not `use FerricStore.Ecto.Cacheable` are never cached. This
prevents accidental caching of audit logs, event streams, or other
write-heavy/append-only tables.

**What `use FerricStore.Ecto.Cacheable` does:**

```elixir
defmodule FerricStore.Ecto.Cacheable do
  defmacro __using__(opts) do
    quote do
      Module.put_attribute(__MODULE__, :ferricstore_cache_opts, unquote(opts))

      def __ferricstore_cache_config__ do
        %{
          ttl: unquote(Keyword.get(opts, :ttl, :timer.minutes(5))),
          region: unquote(Keyword.get(opts, :region, "default")),
          strategy: unquote(Keyword.get(opts, :strategy, :read_write)),
          enabled: unquote(Keyword.get(opts, :enabled, true))
        }
      end
    end
  end
end
```

The cache layer checks `function_exported?(schema, :__ferricstore_cache_config__, 0)`
to determine if a schema is cacheable. If the function is not exported, the
schema is not cached and all operations pass through to the database directly.

**Opt-out model (cache everything by default):**

```elixir
# config.exs
config :ferricstore_ecto,
  cache_mode: :all,
  default_ttl: :timer.minutes(5),
  exclude: [MyApp.AuditLog, MyApp.EventLog, MyApp.MetricsSnapshot]
```

When `cache_mode: :all`, every schema is cached unless it appears in the
`:exclude` list or has `use FerricStore.Ecto.Cacheable, enabled: false`.

**Runtime configuration override:**

```elixir
# config/runtime.exs
config :ferricstore_ecto, :regions, %{
  "users" => [ttl: :timer.minutes(10), max_entries: 10_000],
  "products" => [ttl: :timer.hours(1), max_entries: 50_000],
  "default" => [ttl: :timer.minutes(5), max_entries: 100_000]
}
```

Runtime config overrides compile-time schema attributes. This allows operators
to tune TTLs without recompilation.

#### 1.5.2 Per-Query Control (Like Hibernate's Cache Modes)

```elixir
# NORMAL mode (default) -- read from cache, write on miss
Repo.get(User, 42)

# IGNORE mode -- bypass cache entirely for this query
Repo.get(User, 42, cache: false)

# REFRESH mode -- skip read, execute query, write new value to cache
Repo.get(User, 42, cache: :refresh)

# GET mode -- read from cache only, return nil on miss (never hit DB)
Repo.get(User, 42, cache: :cache_only)

# PUT mode -- skip read, execute query, write to cache
Repo.get(User, 42, cache: :put_only)

# Custom TTL for this specific query
Repo.all(User, cache: [ttl: :timer.seconds(30)])

# Disable cache for complex analytical queries
Repo.all(
  from u in User,
    join: o in Order, on: o.user_id == u.id,
    where: o.total > 100,
    group_by: u.id,
  cache: false
)
```

Implementation: The overridden `get/3`, `all/2`, etc. extract the `:cache`
option from `opts`, apply the corresponding mode logic, and pass the remaining
opts to the underlying Ecto function.

```elixir
defp cache_mode(opts) do
  case Keyword.get(opts, :cache, true) do
    true        -> :normal
    false       -> :ignore
    :refresh    -> :refresh
    :cache_only -> :cache_only
    :put_only   -> :put_only
    [_ | _] = cache_opts ->
      {:normal, cache_opts}  # normal mode with overrides (e.g. custom TTL)
  end
end
```

#### 1.5.3 Entity Cache Design

Entities are stored as **FerricStore hashes**, not serialized blobs. Each
schema field maps to a hash field, storing the string-encoded value. This
unlocks partial reads that are impossible with `term_to_binary` serialization.

**Why hashes instead of blobs:**

| | `term_to_binary` blob | FerricStore hash |
|---|---|---|
| Read single field | Deserialize entire struct, extract field | `HMGET key field` -- O(1), no deserialization |
| Partial SELECT | Must deserialize all fields, discard unused | `HMGET key f1 f2` -- only reads requested fields |
| Inspect in debug | Opaque binary | `HGETALL key` -- human-readable field/value pairs |
| Memory overhead | ETF header bytes, type tags per field | Flat string keys/values, no ETF framing |
| Schema migration | Binary incompatible after field add/remove | New fields simply absent (handled by defaults) |
| Invalidation | DEL key | DEL key (same cost) |
| Cache populate | Serialize entire struct, SET blob | HSET all fields at once |

**Cache key format:**

```
"ecto:{source_table}:{primary_key_value}"
```

Examples:
- `"ecto:users:42"` -- single integer PK
- `"ecto:users:550e8400-e29b-41d4-a716-446655440000"` -- UUID PK
- `"ecto:order_items:17:42"` -- composite PK (order_id=17, item_id=42)

For composite primary keys, key components are joined with `:` in the order
returned by `schema.__schema__(:primary_key)`.

**Cache value format -- hash fields:**

Each Ecto schema field is stored as a hash field with its string-encoded value.
Associations are never stored in the entity hash (they remain
`%Ecto.Association.NotLoaded{}` on reconstruction) to avoid stale reference
chains.

```elixir
# Storing a %User{id: 42, name: "alice", email: "a@b.com", age: 30, active: true}
FerricStore.hset("ecto:users:42", %{
  "name" => "alice",
  "email" => "a@b.com",
  "age" => "30",
  "active" => "true",
  "inserted_at" => "2026-03-20T10:30:00Z",
  "updated_at" => "2026-03-22T14:15:00Z"
})
```

**Field serialization:**

```elixir
defp serialize_fields(struct) do
  schema = struct.__struct__
  fields = schema.__schema__(:fields)

  Map.new(fields, fn field ->
    value = Map.get(struct, field)
    {Atom.to_string(field), encode_field_value(schema, field, value)}
  end)
end

defp deserialize_fields(schema, hash_map) do
  fields = schema.__schema__(:fields)

  Map.new(fields, fn field ->
    str_key = Atom.to_string(field)
    case Map.get(hash_map, str_key) do
      nil -> {field, schema.__schema__(:field_source_default, field)}
      str_val -> {field, decode_field_value(schema, field, str_val)}
    end
  end)
end

# Encoding is type-aware: integers, floats, booleans, dates, and strings
# all have deterministic string representations. For complex types
# (maps, arrays, embedded schemas), fall back to Jason.encode!/1.
defp encode_field_value(_schema, _field, nil), do: ""
defp encode_field_value(_schema, _field, val) when is_binary(val), do: val
defp encode_field_value(_schema, _field, val) when is_integer(val), do: Integer.to_string(val)
defp encode_field_value(_schema, _field, val) when is_float(val), do: Float.to_string(val)
defp encode_field_value(_schema, _field, true), do: "true"
defp encode_field_value(_schema, _field, false), do: "false"
defp encode_field_value(_schema, _field, %DateTime{} = dt), do: DateTime.to_iso8601(dt)
defp encode_field_value(_schema, _field, %NaiveDateTime{} = ndt), do: NaiveDateTime.to_iso8601(ndt)
defp encode_field_value(_schema, _field, %Date{} = d), do: Date.to_iso8601(d)
defp encode_field_value(_schema, _field, val), do: Jason.encode!(val)
```

**Read path (Repo.get):**

```elixir
def cached_get(repo, queryable, id, opts) do
  schema = resolve_schema(queryable)

  unless cacheable?(schema) and cache_mode(opts) != :ignore do
    return repo.super_get(queryable, id, strip_cache_opts(opts))
  end

  source = schema.__schema__(:source)
  cache_key = entity_key(source, id)
  mode = cache_mode(opts)

  # L2 check (FerricStore hash, ~1-5us for hot keys)
  case l2_get(cache_key, schema, mode) do
    {:hit, struct} ->
      emit_telemetry(:l2_hit, schema, cache_key)
      struct

    :miss when mode == :cache_only ->
      emit_telemetry(:cache_only_miss, schema, cache_key)
      nil

    :miss ->
      # Database query
      result = repo.super_get(queryable, id, strip_cache_opts(opts))
      if result do
        ttl = resolve_ttl(schema, opts)
        l2_put(cache_key, result, ttl)
        emit_telemetry(:miss, schema, cache_key)
      end
      result
  end
end
```

**Partial-field reads (SELECT optimization):**

When a query selects only specific fields, the cache reads only those fields
from the hash instead of deserializing the entire entity.

```elixir
# Query: from u in User, select: {u.name, u.email}, where: u.id == 42
# Instead of HGETALL + deserialize + discard unused fields:
{:ok, [name, email]} = FerricStore.hmget("ecto:users:42", ["name", "email"])
# Returns only the 2 requested fields -- no wasted deserialization
```

This is a fundamental advantage over `term_to_binary`: a `SELECT u.name`
query on a schema with 50 fields reads 1 hash field instead of deserializing
all 50.

**L2 storage and retrieval (hash-based):**

```elixir
defp l2_get(cache_key, _schema, mode) when mode in [:refresh, :put_only] do
  :miss
end

defp l2_get(cache_key, schema, _mode) do
  case FerricStore.hgetall(cache_key) do
    {:ok, %{} = fields} when map_size(fields) > 0 ->
      struct = struct(schema, deserialize_fields(schema, fields))
      {:hit, struct}
    _ ->
      :miss
  end
end

defp l2_put(cache_key, struct, ttl) do
  fields = serialize_fields(struct)
  FerricStore.hset(cache_key, fields)
  if ttl > 0, do: FerricStore.expire(cache_key, ttl)
end

# Partial field read -- used when query has a select clause
defp l2_get_fields(cache_key, schema, field_names) do
  str_fields = Enum.map(field_names, &Atom.to_string/1)

  case FerricStore.hmget(cache_key, str_fields) do
    {:ok, values} when is_list(values) ->
      if Enum.all?(values, &is_nil/1) do
        :miss
      else
        decoded =
          Enum.zip(field_names, values)
          |> Enum.map(fn {field, val} ->
            if val, do: decode_field_value(schema, field, val), else: nil
          end)
        {:hit, List.to_tuple(decoded)}
      end
    _ ->
      :miss
  end
end
```

#### 1.5.4 Query Cache Design

The query cache stores the results of `Repo.all/2` and `Repo.one/2` calls.
Following Hibernate's design, the query cache stores **primary key lists**,
not full result sets. This has two benefits:

1. Entity updates automatically reflect in query cache results (the query cache
   stores PKs, entities are looked up individually through the hash-based entity
   cache)
2. Memory efficiency -- the same entity isn't duplicated across N query results

PK lists are stored using **FerricStore's native data structures** instead of
serialized blobs:

| Query characteristic | FerricStore data structure | Why |
|---|---|---|
| `ORDER BY` present | **LIST** (RPUSH) | Preserves insertion order -- the DB-sorted order is maintained |
| No `ORDER BY` | **SET** (SADD) | Unordered; set membership checks are O(1) |

**Cache key format — generation counter:**

Query cache keys include a **table generation counter** that changes on every
write to that table. This provides O(1) invalidation without tracking sets:

```
"ecto:q:{query_hash}:g{generation}"
```

When a write happens to the `users` table, `ecto:gen:users` is incremented.
All existing query caches (keyed with the old generation) become unreachable.
No DELs needed — old entries expire via TTL/LFU eviction naturally.

```elixir
# Write path (O(1) — just increment a counter):
defp invalidate_query_caches(table_source) do
  FerricStore.incr("ecto:gen:#{table_source}")
  emit_telemetry(:query_invalidation, %{table: table_source})
end

# Read path — include generation in cache key:
defp query_cache_key(query, table_source) do
  hash = query_hash(query)
  {:ok, gen} = FerricStore.get("ecto:gen:#{table_source}")
  gen = gen || "0"
  "ecto:q:#{hash}:g#{gen}"
end

# Read path — recheck generation after cache hit to prevent stale reads:
defp cached_query(repo, query, table_source, opts) do
  {:ok, gen1} = FerricStore.get("ecto:gen:#{table_source}")
  gen1 = gen1 || "0"
  cache_key = "ecto:q:#{query_hash(query)}:g#{gen1}"

  case FerricStore.get(cache_key) do
    {:ok, nil} ->
      # Cache miss — execute query, cache result
      :miss

    {:ok, cached_data} ->
      # Cache hit — verify generation hasn't changed during our read
      {:ok, gen2} = FerricStore.get("ecto:gen:#{table_source}")
      gen2 = gen2 || "0"

      if gen1 == gen2 do
        # Safe — no writes happened between our reads
        {:hit, cached_data}
      else
        # Generation changed — a write happened during our cache read.
        # Discard the stale result and fetch from DB.
        :miss
      end
  end
end
```

**Why generation counter instead of tracking sets:**

| Approach | Cost per write | Cost per read hit | Consistency |
|----------|---------------|-------------------|-------------|
| Tracking sets (SMEMBERS + N DELs) | O(N) where N = cached queries | O(1) | Immediate |
| **Generation counter + recheck** | **O(1)** (1 INCR) | **O(3)** (3 GETs) | **Immediate** |

With 100 cached queries touching the `users` table:
- Tracking sets: 100 DELs per write = 100,000 DELs/sec at 1,000 writes/sec
- Generation counter: 1 INCR per write = 1,000 INCRs/sec at 1,000 writes/sec

The 3 GETs per read hit (gen check + cache lookup + gen recheck) cost ~780ns
at FerricStore ETS speed. This is negligible compared to the O(N) write cost saved.

**The recheck prevents stale reads:** If a write increments the generation
between the first gen read and the cache lookup, the recheck catches it and
falls back to the database. The race window is microseconds — the recheck makes
it zero.

**Old cache entries:** Entries keyed with old generations are never looked up
again. They expire naturally via LFU eviction (least frequently used — and
they'll never be used again) or TTL.

The query hash is computed from the Ecto.Query AST and bound parameters:

```elixir
defp query_hash(query) do
  normalized = %{
    source: extract_source(query),
    wheres: Enum.map(query.wheres, &normalize_where/1),
    order_bys: query.order_bys,
    limit: query.limit,
    offset: query.offset,
    distinct: query.distinct,
    group_bys: query.group_bys,
    havings: query.havings,
    params: Enum.map(query.params || [], fn {val, _type} -> val end)
  }

  :erlang.phash2(normalized)
end
```

**PK list storage with native data structures:**

For queries on cacheable schemas with simple primary keys, the query cache
stores PKs using the appropriate data structure:

```elixir
# Ordered query (has ORDER BY):
# from u in User, where: u.active == true, order_by: [asc: u.name], limit: 20
# Store PK list as a FerricStore LIST -- preserves the DB-sorted order
FerricStore.rpush("ecto:q:#{hash}:pks", ["42", "17", "99"])

# Unordered query (no ORDER BY):
# from u in User, where: u.active == true
# Store PK set as a FerricStore SET -- unordered, O(1) membership check
FerricStore.sadd("ecto:q:#{hash}:pks", ["42", "17", "99"])
```

On cache hit, each PK is resolved through the hash-based entity cache:

```elixir
# LIST-based (ordered)
{:ok, pks} = FerricStore.lrange("ecto:q:#{hash}:pks", 0, -1)
Enum.map(pks, fn pk -> cached_get(repo, schema, pk, opts) end)

# SET-based (unordered)
{:ok, pks} = FerricStore.smembers("ecto:q:#{hash}:pks")
Enum.map(pks, fn pk -> cached_get(repo, schema, pk, opts) end)
```

For queries that select specific fields (not full entities), the query cache
stores PKs and the entity cache serves partial field reads:

```elixir
# Query: from u in User, select: {u.name, u.email}, where: u.active == true
# PK list is stored as usual (LIST or SET)
# On cache hit, resolve each PK through entity hash with partial read:
Enum.map(pks, fn pk ->
  {:ok, [name, email]} = FerricStore.hmget("ecto:users:#{pk}", ["name", "email"])
  {name, email}
end)
# Only reads 2 fields per entity hash, not the full row
```

**Table-to-query tracking (using SETs):**

To invalidate query caches when a table is modified, we maintain a reverse
index from table names to query cache keys using FerricStore sets:

```elixir
# When caching query result — NO registration needed!
# The generation counter is embedded in the cache key.
# When the table is mutated, the generation increments,
# and all old-generation cache keys become unreachable.
defp cache_query_result(query, table_source, pks) do
  cache_key = query_cache_key(query, table_source)
  FerricStore.rpush(cache_key, pks)
  # Set TTL for natural expiration of orphaned entries
  ttl = resolve_query_ttl(query)
  if ttl > 0, do: FerricStore.expire(cache_key, ttl)
end

# On table mutation — O(1), just increment the counter:
# invalidate_query_caches(table_source)
#   → FerricStore.incr("ecto:gen:#{table_source}")
# That's it. No tracking set, no SMEMBERS, no bulk DEL.
```

With the generation counter approach, **no query registration is needed**.
The cache key includes the generation number, so old entries become
unreachable when the generation increments. No tracking sets, no cleanup.

**Read path (Repo.all):**

```elixir
def cached_all(repo, queryable, opts) do
  query = Ecto.Queryable.to_query(queryable)
  schema = primary_schema(query)

  unless cacheable_query?(schema, query, opts) do
    return repo.super_all(queryable, strip_cache_opts(opts))
  end

  mode = cache_mode(opts)
  table_source = schema.__schema__(:source)

  # Generation counter: read current gen, include in cache key
  {:ok, gen1} = FerricStore.get("ecto:gen:#{table_source}")
  gen1 = gen1 || "0"
  q_hash = query_hash(query)
  pk_key = "ecto:q:#{q_hash}:g#{gen1}:pks"

  case query_cache_get(pk_key, query, mode) do
    {:hit, _type, pks} ->
      # Cache hit — recheck generation to prevent stale reads.
      # If a write happened between reading gen1 and the cache lookup,
      # the generation will have changed. Discard and fetch from DB.
      {:ok, gen2} = FerricStore.get("ecto:gen:#{table_source}")
      gen2 = gen2 || "0"

      if gen1 == gen2 do
        # Safe — no writes happened during our cache read
        emit_telemetry(:query_hit, schema, pk_key)
        resolve_pks(repo, schema, pks, query, opts)
      else
        # Generation changed — fall through to DB fetch
        emit_telemetry(:query_miss, schema, pk_key)
        results = repo.super_all(queryable, strip_cache_opts(opts))
        # Re-read gen for the new cache key
        {:ok, fresh_gen} = FerricStore.get("ecto:gen:#{table_source}")
        fresh_key = "ecto:q:#{q_hash}:g#{fresh_gen || "0"}:pks"
        ttl = resolve_ttl(schema, opts)
        cache_query_result(fresh_key, query, schema, results, ttl)
        results
      end

    :miss when mode == :cache_only ->
      []

    :miss ->
      results = repo.super_all(queryable, strip_cache_opts(opts))
      ttl = resolve_ttl(schema, opts)
      cache_query_result(pk_key, query, schema, results, ttl)
      results
  end
end

# Resolve PKs through the hash-based entity cache. If the query has a
# select clause, use partial field reads (HMGET) instead of full HGETALL.
defp resolve_pks(repo, schema, pks, query, opts) do
  case extract_select_fields(query) do
    nil ->
      # Full entity select -- HGETALL each
      Enum.map(pks, fn pk -> cached_get(repo, schema, pk, opts) end)
      |> Enum.reject(&is_nil/1)

    fields ->
      # Partial select -- HMGET only the requested fields
      source = schema.__schema__(:source)
      Enum.map(pks, fn pk ->
        cache_key = entity_key(source, pk)
        case l2_get_fields(cache_key, schema, fields) do
          {:hit, tuple} -> tuple
          :miss ->
            # Entity not in cache -- fetch from DB and populate hash
            entity = repo.super_get(schema, pk, strip_cache_opts(opts))
            if entity, do: l2_put(entity_key(source, pk), entity, resolve_ttl(schema, opts))
            if entity, do: extract_fields(entity, fields), else: nil
        end
      end)
      |> Enum.reject(&is_nil/1)
  end
end
```

**Choosing the right data structure for a query:**

```elixir
defp cache_query_result(q_key, query, schema, results, ttl) do
  pks = Enum.map(results, fn r -> to_string(primary_key_value(r)) end)
  pk_key = "#{q_key}:pks"

  cond do
    has_order_by?(query) ->
      # List -- preserve DB-sorted order
      FerricStore.rpush(pk_key, pks)

    true ->
      # Set -- unordered
      FerricStore.sadd(pk_key, pks)
  end

  if ttl > 0, do: FerricStore.expire(pk_key, ttl)

  # No query registration needed — the generation counter in the cache key
  # handles invalidation. When the table is mutated, the generation increments,
  # and this cache key becomes unreachable.

  # Populate entity hash cache for each result
  source = schema.__schema__(:source)
  Enum.each(results, fn struct ->
    pk = primary_key_value(struct)
    l2_put(entity_key(source, pk), struct, ttl)
  end)
end
```

**Queries that are never cached:**

- Queries with `lock: "FOR UPDATE"` or any `Ecto.Query.lock/2`
- `Repo.stream/2` (streaming queries)
- Raw SQL via `Ecto.Adapters.SQL.query/4`
- Queries with `cache: false` in opts
- Queries on non-cacheable schemas
- Queries with subqueries in FROM position (too complex to track tables)
- Queries with CTEs (common table expressions)

#### 1.5.5 Invalidation

Invalidation is the hardest part of any cache system. Our design follows
Hibernate's invalidation model closely: **on any write operation (insert,
update, delete), always delete the cache entry.** The next read repopulates
from the database, guaranteeing consistency regardless of whether the DB
transaction succeeded or failed.

**Invalidation rule -- always DELETE:**

| Operation | Cache action |
|---|---|
| `Repo.insert/2` | `FerricStore.del(cache_key)` + invalidate query caches |
| `Repo.update/2` | `FerricStore.del(cache_key)` + invalidate query caches |
| `Repo.delete/2` | `FerricStore.del(cache_key)` + invalidate query caches |
| `Repo.update_all/3` | Invalidate all entity + query caches for the table |
| `Repo.delete_all/2` | Invalidate all entity + query caches for the table |

The hash structure (HGETALL/HMGET) is valuable for **reads** -- partial field
access without deserializing the entire entity. It is not used for write-side
cache management. HSET is only used when **populating** the cache after a DB
miss on the read path.

**On `Repo.insert/2` (new entity):**

```elixir
def cached_insert(repo, changeset_or_struct, opts) do
  case repo.super_insert(changeset_or_struct, strip_cache_opts(opts)) do
    {:ok, struct} = result ->
      schema = struct.__struct__
      if cacheable?(schema) do
        source = schema.__schema__(:source)
        pk = primary_key_value(struct)
        # 1. Delete entity cache (may not exist -- that's fine)
        FerricStore.del(entity_key(source, pk))
        # 2. Invalidate ALL query caches touching this table
        #    (a new row changes query result sets)
        invalidate_query_caches(source)
      end
      result

    error ->
      error
  end
end
```

**On `Repo.update/2` — always DELETE, never update cache:**

```elixir
def cached_update(repo, changeset, opts) do
  case repo.super_update(changeset, strip_cache_opts(opts)) do
    {:ok, struct} = result ->
      schema = struct.__struct__
      if cacheable?(schema) do
        source = schema.__schema__(:source)
        pk = primary_key_value(struct)
        cache_key = entity_key(source, pk)

        case schema.__ferricstore_cache_config__().strategy do
          :read_only ->
            Logger.warning("FerricStore.Ecto.Cache: update on :read_only entity #{inspect(schema)}")

          _strategy ->
            # ALWAYS delete on mutation. Never update the cache directly.
            # The next read will repopulate from the DB with correct data.
            FerricStore.del(cache_key)
        end

        # Invalidate all query caches touching this table.
        invalidate_query_caches(source)
      end
      result

    error ->
      # DB transaction failed — cache is untouched. Correct.
      error
  end
end
```

**Why DELETE, not surgical update:**

It may seem wasteful to delete the entire cached entity when only one field
changed. But surgical cache updates are **dangerous**:

```
DANGEROUS (surgical HSET):
  1. DB: BEGIN
  2. DB: UPDATE users SET email = 'new@b.com' WHERE id = 42
  3. Cache: HSET ecto:users:42 email "new@b.com"  ← cache updated
  4. DB: COMMIT fails (constraint, timeout, deadlock)

  Result: DB has OLD email, cache has NEW email. INCONSISTENT.
  Every reader sees wrong data until TTL expires or manual invalidation.

SAFE (DELETE on mutation):
  1. DB: BEGIN
  2. DB: UPDATE users SET email = 'new@b.com' WHERE id = 42
  3. DB: COMMIT succeeds (or fails)
  4. Cache: DEL ecto:users:42

  If commit succeeded: cache miss → next read fetches from DB → correct data
  If commit failed: cache miss → next read fetches from DB → old (correct) data
  Either way: CONSISTENT. Worst case is one extra DB roundtrip.
```

The cost of one extra DB read after a write is negligible compared to the risk
of serving stale/incorrect data. Writes are infrequent compared to reads (typical
read:write ratio is 10:1 to 100:1), so most requests still hit the cache.

**The hash structure is valuable for READS (partial field access via HMGET), not
for write-side cache management.** HSET is only used when populating the cache
after a database miss on the read path.

**On `Repo.delete/2`:**

```elixir
def cached_delete(repo, struct_or_changeset, opts) do
  case repo.super_delete(struct_or_changeset, strip_cache_opts(opts)) do
    {:ok, struct} = result ->
      schema = struct.__struct__
      if cacheable?(schema) do
        source = schema.__schema__(:source)
        pk = primary_key_value(struct)
        # 1. Delete entire entity hash
        FerricStore.del(entity_key(source, pk))
        # 2. Invalidate query caches
        invalidate_query_caches(source)
      end
      result

    error ->
      error
  end
end
```

**On `Repo.update_all/3` (bulk update):**

```elixir
def cached_update_all(repo, queryable, updates, opts) do
  result = repo.super_update_all(queryable, updates, strip_cache_opts(opts))

  # Bulk updates can affect any number of rows -- we must invalidate
  # the entire table's entity hashes AND all query caches.
  query = Ecto.Queryable.to_query(queryable)
  tables = extract_tables(query)
  Enum.each(tables, fn table ->
    invalidate_all_entity_caches(table)
    invalidate_query_caches(table)
  end)

  result
end
```

**On `Repo.delete_all/2` (bulk delete):**

Same as `update_all` -- invalidate all entity hashes for the table and all
query caches.

**Query cache invalidation internals (generation counter):**

```elixir
defp invalidate_query_caches(table_source) do
  # O(1) — just increment the generation counter.
  # All existing query caches for this table become unreachable
  # because they're keyed with the old generation.
  FerricStore.incr("ecto:gen:#{table_source}")
  emit_telemetry(:query_invalidation, %{table: table_source})
end
```

No tracking sets. No SMEMBERS. No bulk DELs. One atomic INCR per write.

**Entity cache bulk invalidation (for update_all/delete_all):**

```elixir
defp invalidate_all_entity_caches(table_source) do
  # Use FerricStore's prefix-based key listing to find all entity hashes
  # for this table. This is O(matching keys) via the prefix index.
  prefix = "ecto:#{table_source}:"
  {:ok, keys} = FerricStore.keys("#{prefix}*")
  Enum.each(keys, &FerricStore.del/1)
  emit_telemetry(:entity_bulk_invalidation, %{table: table_source, count: length(keys)})
end
```

#### 1.5.6 Cache Regions (Like Hibernate)

Cache regions provide independent TTL and capacity settings per logical group
of entities. Each schema can declare its region:

```elixir
defmodule MyApp.User do
  use FerricStore.Ecto.Cacheable, region: "users"
  # ...
end

defmodule MyApp.Product do
  use FerricStore.Ecto.Cacheable, region: "products"
  # ...
end
```

Region configuration in `runtime.exs`:

```elixir
config :ferricstore_ecto, :regions, %{
  "users" => [
    ttl: :timer.minutes(10),
    max_entries: 10_000
  ],
  "products" => [
    ttl: :timer.hours(1),
    max_entries: 50_000
  ],
  "sessions" => [
    ttl: :timer.minutes(30),
    max_entries: 100_000
  ],
  "default" => [
    ttl: :timer.minutes(5),
    max_entries: 100_000
  ]
}
```

Regions map to FerricStore key prefixes. The entity key format becomes:

```
"ecto:{region}:{source_table}:{primary_key}"
```

When a region is not explicitly configured, it falls back to `"default"`.
When `max_entries` is exceeded, LFU eviction (already built into FerricStore)
handles cleanup.

#### 1.5.7 Cache Strategies (Like Hibernate)

| Strategy | Read behavior | Write behavior | Consistency | Use case |
|----------|--------------|----------------|-------------|----------|
| `:read_only` | Cached | Never invalidated by writes | Strong (data never changes) | Reference data: countries, currencies, categories, permissions |
| `:read_write` | Cached | Cache entry deleted on mutation; next read repopulates | Strong | User profiles, settings, config |
| `:nonstrict_read_write` | Cached | Cache entry deleted on mutation; next read repopulates | Eventual | Product listings, search results, blog posts |

**`:read_only` details:**
- Entity cache is populated on first read and refreshed only when TTL expires
- `Repo.update/2` and `Repo.delete/2` on a `:read_only` entity emit a warning
  log but still execute the database operation (we do not block writes)
- Best for data that is loaded once at startup or changes only via migrations

**`:read_write` and `:nonstrict_read_write` details:**
- On `Repo.update/2`: the cache entry is deleted. The next read experiences a
  cache miss and fetches from the database, repopulating the cache.
- Both strategies use the same DELETE invalidation. The distinction is primarily
  for logging and operational visibility -- `:read_write` is intended for
  frequently-read entities where a post-write miss is negligible, while
  `:nonstrict_read_write` signals that brief staleness is acceptable.

#### 1.5.8 Transaction Boundaries

Cache writes must respect database transaction boundaries. If a database
write is rolled back, the cache must not contain stale data.

**Design:**

- Cache invalidation and population happen AFTER the Ecto function returns
  `{:ok, struct}`, which means the database operation has committed
- For `Repo.transaction/2` with multiple operations, we buffer cache
  operations and apply them only when the transaction commits

```elixir
def cached_transaction(repo, fun_or_multi, opts) do
  # Enter transaction mode: buffer cache operations
  Process.put(:__ferricstore_ecto_tx_buffer__, [])

  result = repo.super_transaction(fun_or_multi, opts)

  case result do
    {:ok, _} ->
      # Transaction committed -- apply all buffered cache operations
      buffer = Process.get(:__ferricstore_ecto_tx_buffer__, [])
      Enum.each(Enum.reverse(buffer), &apply_cache_op/1)

    {:error, _} ->
      # Transaction rolled back -- discard all buffered cache operations
      :ok
  end

  Process.delete(:__ferricstore_ecto_tx_buffer__)
  result
end

defp buffer_or_apply(cache_op) do
  case Process.get(:__ferricstore_ecto_tx_buffer__) do
    nil ->
      # Not in a transaction -- apply immediately
      apply_cache_op(cache_op)

    buffer ->
      # In a transaction -- buffer for later
      Process.put(:__ferricstore_ecto_tx_buffer__, [cache_op | buffer])
  end
end
```

**Edge case: nested transactions.** Ecto uses savepoints for nested
transactions. Our buffer model naturally handles this: all operations
accumulate in the same buffer and are applied (or discarded) when the
outermost transaction commits (or rolls back).

#### 1.5.9 Distributed Invalidation

When FerricStore runs in Raft cluster mode, cache invalidation naturally
propagates across nodes because all FerricStore writes (including `del`)
go through Raft consensus.

When a Repo.update on node A deletes `"ecto:users:42"` via `DEL`, that
deletion is replicated to nodes B and C via Raft. Any subsequent
`FerricStore.hgetall("ecto:users:42")` on those nodes returns an empty
map, causing a cache miss and a fresh database read.

#### 1.5.10 Test Sandbox Integration

FerricStore.Ecto.Cache integrates with both `Ecto.Adapters.SQL.Sandbox`
and `FerricStore.Sandbox` for test isolation.

```elixir
defmodule MyApp.DataCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      alias MyApp.Repo
      import Ecto.Query
    end
  end

  setup tags do
    # Standard Ecto sandbox
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(MyApp.Repo)
    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(MyApp.Repo, {:shared, self()})
    end

    # FerricStore sandbox -- isolates cache keys per test
    namespace = FerricStore.Sandbox.checkout()
    on_exit(fn -> FerricStore.Sandbox.checkin(namespace) end)

    %{namespace: namespace}
  end
end
```

Or use the provided case template:

```elixir
defmodule MyApp.CachedDataCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      alias MyApp.Repo
      import Ecto.Query
      use FerricStore.Sandbox.Case
    end
  end
end
```

All cache keys are automatically prefixed with the test namespace, preventing
cross-test pollution even with `async: true`.

#### 1.5.11 Statistics and Observability

**Programmatic stats:**

```elixir
FerricStore.Ecto.Cache.stats()
# %{
#   entity_hits: 45_230,
#   entity_misses: 1_204,
#   query_hits: 12_400,
#   query_misses: 890,
#   invalidations: 340,
#   bulk_invalidations: 12,
#   hit_ratio: 0.97,
#   avg_l2_latency_us: 3.2
# }

FerricStore.Ecto.Cache.stats(:users)  # per-region stats
# %{
#   entity_hits: 12_300,
#   entity_misses: 200,
#   query_hits: 3_400,
#   ...
# }
```

**Telemetry events:**

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ferricstore, :ecto, :cache, :l2_hit]` | `%{duration: native}` | `%{schema: module, key: string, region: string}` |
| `[:ferricstore, :ecto, :cache, :miss]` | `%{duration: native}` | `%{schema: module, key: string, region: string}` |
| `[:ferricstore, :ecto, :cache, :query_hit]` | `%{duration: native}` | `%{key: string, tables: [string]}` |
| `[:ferricstore, :ecto, :cache, :query_miss]` | `%{duration: native}` | `%{key: string, tables: [string]}` |
| `[:ferricstore, :ecto, :cache, :invalidate]` | `%{}` | `%{key: string, reason: atom}` |
| `[:ferricstore, :ecto, :cache, :query_invalidate]` | `%{count: integer}` | `%{table: string}` |
| `[:ferricstore, :ecto, :cache, :bulk_invalidate]` | `%{count: integer}` | `%{table: string}` |

These integrate with standard Phoenix telemetry dashboards and the existing
FerricStore Prometheus metrics endpoint.

### 1.6 Edge Cases and Limitations

**Queries that bypass the cache:**
- `Ecto.Adapters.SQL.query/4` (raw SQL) -- no AST to hash, no tables to track
- `Repo.stream/2` -- streaming returns a lazy enumerable; caching the full
  result set would defeat the purpose of streaming
- Queries with `lock: "FOR UPDATE"` -- the lock implies the caller needs the
  latest data and intends to modify it
- Subqueries in the FROM clause -- table extraction becomes unreliable

**Bulk operations:**
- `Repo.update_all/3` and `Repo.delete_all/2` invalidate the ENTIRE table's
  entity cache and all query caches. This is conservative but correct.
  Hibernate does the same thing.
- A `Repo.update_all(from(u in User, where: u.id == 42), set: [name: "bob"])`
  invalidates all 10,000 User entity cache entries, not just user 42. This is
  because Ecto does not return the affected rows from `update_all`, so we
  cannot know which specific entities were modified. A future optimization
  could parse the WHERE clause to narrow the invalidation scope.

**Schema changes:**
- With hash-based storage, schema changes are handled gracefully without
  version hashing. When a migration adds a new column, the hash simply lacks
  that field -- `deserialize_fields/2` falls back to the schema's default value
  for missing fields. When a migration removes a column, the extra hash field
  is ignored during reconstruction (only fields in `schema.__schema__(:fields)`
  are read).

```elixir
# After migration adds :phone field to users:
# Existing cached hash: %{"name" => "alice", "email" => "a@b.com"}
# Reconstruction: %User{name: "alice", email: "a@b.com", phone: nil}
# The missing :phone field gets the schema default (nil) -- no crash, no stale data

# After migration removes :legacy_field from users:
# Existing cached hash: %{"name" => "alice", "legacy_field" => "old"}
# Reconstruction: only reads fields in schema.__schema__(:fields)
# The extra "legacy_field" in the hash is silently ignored
```

This is a significant advantage over `term_to_binary`: no schema version
hashing, no orphaned keys, no TTL-based cleanup. Hash-based caches survive
schema migrations without any cache flush or key format change.

**Multi-tenant systems:**
- For apps using `Repo.put_dynamic_prefix/1` (PostgreSQL schemas/prefixes),
  the cache key must include the prefix:
  `"ecto:{prefix}:{source}:{pk}"`

**Ecto.Multi:**
- `Ecto.Multi` operations run inside a transaction. Our transaction buffering
  (section 1.5.8) handles this correctly -- all cache operations are buffered
  until the entire Multi commits.

**Soft deletes:**
- Schemas using soft deletes (e.g., `paranoia` or custom `deleted_at` field)
  should use `Repo.update/2` to set the deletion flag. The cache correctly
  invalidates on update: the entity cache entry is deleted, and all query
  caches touching the table are invalidated. This ensures queries filtering
  by `where: is_nil(deleted_at)` do not return stale results.

**ETS table ownership:**
- Cache stats counters are stored in ETS tables owned by a dedicated GenServer
  (`FerricStore.Ecto.Cache.StatsServer`). This follows the pattern used by
  `Ferricstore.Stats` in the core engine.

### 1.7 Migration from Cachex/ConCache

**Before (manual caching with Cachex):**

```elixir
defmodule MyApp.Accounts do
  def get_user(id) do
    case Cachex.get(:my_cache, "user:#{id}") do
      {:ok, nil} ->
        case Repo.get(User, id) do
          nil ->
            nil
          user ->
            Cachex.put(:my_cache, "user:#{id}", user, ttl: :timer.minutes(5))
            user
        end
      {:ok, user} ->
        user
    end
  end

  def update_user(user, attrs) do
    case Repo.update(User.changeset(user, attrs)) do
      {:ok, updated} = result ->
        Cachex.del(:my_cache, "user:#{updated.id}")
        # Hope we didn't forget any query caches...
        Cachex.del(:my_cache, "active_users")
        Cachex.del(:my_cache, "admin_users")
        result
      error ->
        error
    end
  end

  def list_active_users do
    case Cachex.get(:my_cache, "active_users") do
      {:ok, nil} ->
        users = Repo.all(from u in User, where: u.active == true)
        Cachex.put(:my_cache, "active_users", users, ttl: :timer.minutes(2))
        users
      {:ok, users} ->
        users
    end
  end
end
```

**After (automatic with FerricStore.Ecto.Cache):**

```elixir
defmodule MyApp.Accounts do
  def get_user(id) do
    Repo.get(User, id)
  end

  def update_user(user, attrs) do
    Repo.update(User.changeset(user, attrs))
  end

  def list_active_users do
    Repo.all(from u in User, where: u.active == true)
  end
end
```

All caching, invalidation, and consistency are handled automatically.

**Migration steps:**

1. Add `ferricstore` and `ferricstore_ecto` to `mix.exs` deps
2. Change `use Ecto.Repo` to `use FerricStore.Ecto.CachedRepo` in your Repo module
3. Add `use FerricStore.Ecto.Cacheable` to schemas you want cached
4. Add `FerricStore.Sandbox.checkout/checkin` to test setup (alongside Ecto sandbox)
5. Remove all manual Cachex/ConCache cache code from contexts
6. Remove Cachex/ConCache from your supervision tree (if no longer needed)

---

## Part 2: Nebulex Adapter (FerricStore.Nebulex.Adapter)

### 2.1 Motivation

Nebulex is the most popular cache abstraction in Elixir, with a well-defined
adapter behaviour that mirrors Ecto's adapter pattern. Many production
applications already have `MyApp.Cache` modules using Nebulex with the local
adapter or the partitioned adapter. A FerricStore Nebulex adapter lets these
applications switch backends with a one-line config change, gaining
persistence, LFU eviction, and Raft replication without code changes.

### 2.2 Nebulex Adapter Behaviours

Nebulex v3 organizes adapter callbacks into four behaviours:

**`Nebulex.Adapter`** (required):
- `init/1` -- initialize adapter state, return supervisor child specs and metadata
- `__before_compile__/1` (optional) -- compile-time code injection

**`Nebulex.Adapter.KV`** (required for key-value operations):
- `fetch/3` -- retrieve a key
- `put/7` -- store a key with TTL and write condition
- `put_all/5` -- store multiple keys
- `delete/3` -- remove a key
- `take/3` -- get and delete
- `has_key?/3` -- check existence
- `ttl/3` -- get remaining TTL
- `expire/4` -- set TTL
- `touch/3` -- reset TTL to current value (refresh expiry)
- `update_counter/6` -- atomic increment/decrement

**`Nebulex.Adapter.Queryable`** (required for bulk operations):
- `execute/3` -- execute a query (`:get_all`, `:count_all`, `:delete_all`)
- `stream/3` -- lazy streaming of results

**`Nebulex.Adapter.Transaction`** (optional):
- `transaction/3` -- execute function within a transaction
- `in_transaction?/2` -- check if currently in a transaction

### 2.3 Implementation

```elixir
defmodule FerricStore.Nebulex.Adapter do
  @moduledoc """
  Nebulex adapter backed by FerricStore.

  Provides persistent, crash-recoverable caching with LFU eviction and
  optional Raft-based replication. Drop-in replacement for
  `Nebulex.Adapters.Local`.

  ## Configuration

      defmodule MyApp.Cache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: FerricStore.Nebulex.Adapter
      end

      # config/config.exs
      config :my_app, MyApp.Cache,
        prefix: "nbx",            # key prefix in FerricStore (default: "nbx")
        serializer: :erlang_term   # :erlang_term (default) or :jason
  """

  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable

  # -------------------------------------------------------------------------
  # Nebulex.Adapter callbacks
  # -------------------------------------------------------------------------

  @impl Nebulex.Adapter
  def init(config) do
    prefix = Keyword.get(config, :prefix, "nbx")
    serializer = Keyword.get(config, :serializer, :erlang_term)

    adapter_meta = %{
      prefix: prefix,
      serializer: serializer
    }

    # No child specs needed -- FerricStore runs its own supervision tree
    {:ok, [], adapter_meta}
  end

  # -------------------------------------------------------------------------
  # Nebulex.Adapter.KV callbacks
  # -------------------------------------------------------------------------

  @impl Nebulex.Adapter.KV
  def fetch(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    case FerricStore.get(fkey) do
      {:ok, nil} -> {:error, %Nebulex.KeyError{key: key}}
      {:ok, bin} -> {:ok, deserialize(adapter_meta, bin)}
    end
  end

  @impl Nebulex.Adapter.KV
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl, _opts) do
    fkey = full_key(adapter_meta, key)
    bin = serialize(adapter_meta, value)
    ttl_ms = normalize_ttl(ttl)

    case on_write do
      :put ->
        if keep_ttl do
          FerricStore.set(fkey, bin, keepttl: true)
        else
          FerricStore.set(fkey, bin, ttl: ttl_ms)
        end
        {:ok, true}

      :put_new ->
        case FerricStore.setnx(fkey, bin) do
          {:ok, true} ->
            if ttl_ms > 0, do: FerricStore.expire(fkey, ttl_ms)
            {:ok, true}
          {:ok, false} ->
            {:ok, false}
        end

      :replace ->
        if FerricStore.exists(fkey) do
          FerricStore.set(fkey, bin, ttl: ttl_ms)
          {:ok, true}
        else
          {:ok, false}
        end
    end
  end

  @impl Nebulex.Adapter.KV
  def put_all(adapter_meta, entries, on_write, ttl, _opts) do
    ttl_ms = normalize_ttl(ttl)

    case on_write do
      :put ->
        Enum.each(entries, fn {key, value} ->
          fkey = full_key(adapter_meta, key)
          bin = serialize(adapter_meta, value)
          FerricStore.set(fkey, bin, ttl: ttl_ms)
        end)
        {:ok, true}

      :put_new ->
        fentries = Enum.map(entries, fn {k, v} -> {full_key(adapter_meta, k), v} end)
        any_exists = Enum.any?(fentries, fn {fkey, _} -> FerricStore.exists(fkey) end)

        if any_exists do
          {:ok, false}
        else
          Enum.each(fentries, fn {fkey, value} ->
            bin = serialize(adapter_meta, value)
            FerricStore.set(fkey, bin, ttl: ttl_ms)
          end)
          {:ok, true}
        end
    end
  end

  @impl Nebulex.Adapter.KV
  def delete(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)
    FerricStore.del(fkey)
    :ok
  end

  @impl Nebulex.Adapter.KV
  def take(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    case FerricStore.getdel(fkey) do
      {:ok, nil} -> {:error, %Nebulex.KeyError{key: key}}
      {:ok, bin} -> {:ok, deserialize(adapter_meta, bin)}
    end
  end

  @impl Nebulex.Adapter.KV
  def has_key?(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)
    {:ok, FerricStore.exists(fkey)}
  end

  @impl Nebulex.Adapter.KV
  def ttl(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    case FerricStore.ttl(fkey) do
      {:ok, nil} ->
        if FerricStore.exists(fkey) do
          {:ok, :infinity}
        else
          {:error, %Nebulex.KeyError{key: key}}
        end
      {:ok, ms} ->
        {:ok, ms}
    end
  end

  @impl Nebulex.Adapter.KV
  def expire(adapter_meta, key, ttl, _opts) do
    fkey = full_key(adapter_meta, key)
    ttl_ms = normalize_ttl(ttl)

    if FerricStore.exists(fkey) do
      FerricStore.expire(fkey, ttl_ms)
      {:ok, true}
    else
      {:ok, false}
    end
  end

  @impl Nebulex.Adapter.KV
  def touch(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    if FerricStore.exists(fkey) do
      # "touch" refreshes the last access time.
      # FerricStore's LFU counter is automatically bumped on read,
      # so we just need to verify the key exists.
      {:ok, true}
    else
      {:ok, false}
    end
  end

  @impl Nebulex.Adapter.KV
  def update_counter(adapter_meta, key, amount, default, ttl, _opts) do
    fkey = full_key(adapter_meta, key)
    ttl_ms = normalize_ttl(ttl)

    # Ensure key exists with default value if missing
    unless FerricStore.exists(fkey) do
      FerricStore.set(fkey, Integer.to_string(default), ttl: ttl_ms)
    end

    case FerricStore.incr_by(fkey, amount) do
      {:ok, new_val} -> {:ok, new_val}
      {:error, reason} -> {:error, %Nebulex.Error{reason: reason}}
    end
  end

  # -------------------------------------------------------------------------
  # Nebulex.Adapter.Queryable callbacks
  # -------------------------------------------------------------------------

  @impl Nebulex.Adapter.Queryable
  def execute(adapter_meta, %{op: :get_all, query: {:in, keys}}, _opts) do
    results =
      Enum.reduce(keys, [], fn key, acc ->
        fkey = full_key(adapter_meta, key)
        case FerricStore.get(fkey) do
          {:ok, nil} -> acc
          {:ok, bin} -> [{key, deserialize(adapter_meta, bin)} | acc]
        end
      end)

    {:ok, Enum.reverse(results)}
  end

  def execute(adapter_meta, %{op: :get_all, query: {:q, nil}}, _opts) do
    # Return all entries (expensive -- scans all keys with prefix)
    prefix = adapter_meta.prefix <> ":"
    {:ok, all_keys} = FerricStore.keys("#{prefix}*")

    results =
      Enum.map(all_keys, fn fkey ->
        user_key = strip_prefix(adapter_meta, fkey)
        case FerricStore.get(fkey) do
          {:ok, nil} -> nil
          {:ok, bin} -> {user_key, deserialize(adapter_meta, bin)}
        end
      end)
      |> Enum.reject(&is_nil/1)

    {:ok, results}
  end

  def execute(adapter_meta, %{op: :count_all, query: {:q, nil}}, _opts) do
    prefix = adapter_meta.prefix <> ":"
    {:ok, all_keys} = FerricStore.keys("#{prefix}*")
    {:ok, length(all_keys)}
  end

  def execute(adapter_meta, %{op: :count_all, query: {:in, keys}}, _opts) do
    count =
      Enum.count(keys, fn key ->
        fkey = full_key(adapter_meta, key)
        FerricStore.exists(fkey)
      end)

    {:ok, count}
  end

  def execute(adapter_meta, %{op: :delete_all, query: {:q, nil}}, _opts) do
    prefix = adapter_meta.prefix <> ":"
    {:ok, all_keys} = FerricStore.keys("#{prefix}*")
    Enum.each(all_keys, &FerricStore.del/1)
    {:ok, length(all_keys)}
  end

  def execute(adapter_meta, %{op: :delete_all, query: {:in, keys}}, _opts) do
    count =
      Enum.count(keys, fn key ->
        fkey = full_key(adapter_meta, key)
        if FerricStore.exists(fkey) do
          FerricStore.del(fkey)
          true
        else
          false
        end
      end)

    {:ok, count}
  end

  @impl Nebulex.Adapter.Queryable
  def stream(adapter_meta, %{query: {:q, nil}}, _opts) do
    prefix = adapter_meta.prefix <> ":"

    stream =
      Stream.resource(
        fn -> FerricStore.keys("#{prefix}*") end,
        fn
          {:ok, []} -> {:halt, []}
          {:ok, [key | rest]} ->
            user_key = strip_prefix(adapter_meta, key)
            case FerricStore.get(key) do
              {:ok, nil} -> {[], {:ok, rest}}
              {:ok, bin} -> {[{user_key, deserialize(adapter_meta, bin)}], {:ok, rest}}
            end
        end,
        fn _ -> :ok end
      )

    {:ok, stream}
  end

  # -------------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------------

  defp full_key(%{prefix: prefix}, key) do
    "#{prefix}:#{encode_key(key)}"
  end

  defp strip_prefix(%{prefix: prefix}, fkey) do
    prefix_len = byte_size(prefix) + 1  # +1 for ":"
    binary = binary_part(fkey, prefix_len, byte_size(fkey) - prefix_len)
    decode_key(binary)
  end

  defp encode_key(key) when is_binary(key), do: key
  defp encode_key(key), do: :erlang.term_to_binary(key) |> Base.encode64()

  defp decode_key(bin) do
    case Base.decode64(bin) do
      {:ok, term_bin} -> :erlang.binary_to_term(term_bin)
      :error -> bin
    end
  end

  defp serialize(%{serializer: :erlang_term}, value) do
    :erlang.term_to_binary(value, [:compressed])
  end

  defp serialize(%{serializer: :jason}, value) do
    Jason.encode!(value)
  end

  defp deserialize(%{serializer: :erlang_term}, bin) do
    :erlang.binary_to_term(bin)
  end

  defp deserialize(%{serializer: :jason}, bin) do
    Jason.decode!(bin)
  end

  defp normalize_ttl(:infinity), do: 0
  defp normalize_ttl(nil), do: 0
  defp normalize_ttl(ms) when is_integer(ms) and ms > 0, do: ms
  defp normalize_ttl(_), do: 0
end
```

### 2.4 Configuration

**Switching from Nebulex.Adapters.Local to FerricStore -- one-line change:**

```elixir
defmodule MyApp.Cache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: FerricStore.Nebulex.Adapter  # was Nebulex.Adapters.Local
end

# config/config.exs
config :my_app, MyApp.Cache,
  prefix: "cache",
  serializer: :erlang_term
```

All existing `MyApp.Cache.get/2`, `MyApp.Cache.put/3`, etc. calls work
unchanged.

### 2.5 What Users Gain by Switching

| Feature | Nebulex.Adapters.Local | FerricStore.Nebulex.Adapter |
|---------|----------------------|---------------------|
| Persistence | No -- data lost on restart | Yes -- Bitcask survives crashes and deploys |
| Eviction | Generation-based (two generations) | LFU with time decay (Redis-compatible) |
| Crash recovery | Data lost | Data preserved (fsync + WAL) |
| Multi-node | Partitioned adapter (data split) | Raft consensus (full replication) |
| TTL precision | Millisecond | Millisecond |
| Stampede protection | No | Yes -- built-in `FerricStore.FetchOrCompute` |
| Hot/cold tiering | No -- everything in ETS | Yes -- hot keys in ETS, cold on SSD |
| Max value size | Limited by ETS/BEAM memory | 512 MB per value (Bitcask) |
| Data structures | Key-value only | Key-value + hash, list, set, sorted set |
| Observability | Basic telemetry | Prometheus metrics, hot/cold stats, slowlog |

**Data structure advantages over simple key-value caches (Cachex, ConCache, Nebulex.Adapters.Local):**

These caches treat values as opaque blobs. FerricStore's rich data structures
enable capabilities that are fundamentally impossible with blob-based caching:

| Capability | Blob cache (Cachex/ConCache) | FerricStore |
|---|---|---|
| **Partial field read** | Deserialize entire blob, extract field | `HMGET key field` -- reads only the requested field |
| **Cache populate (read miss)** | Serialize entire struct as blob | `HSET key fields` -- field-level storage enables partial reads later |
| **Invalidation on write** | `DEL key` | `DEL key` (same -- always delete, never update) |
| **Schema migration** | Binary incompatible, cache flush needed | Hash fields are independent -- new/removed fields handled gracefully |

### 2.6 Limitations

- **No distributed partitioning.** FerricStore replicates fully across
  nodes (Raft). Nebulex's `Partitioned` adapter splits data. If a user
  needs data partitioning (sharding across nodes), they should use
  Nebulex's partitioned adapter with FerricStore as the local backend.
- **Key encoding overhead.** Non-binary keys (tuples, atoms, integers)
  are serialized to binary via `:erlang.term_to_binary` + Base64. This
  adds ~1us per operation for non-binary keys.
- **Queryable limitations.** `{:q, custom_query}` is not supported beyond
  `nil` (match all). FerricStore does not have a query language for
  arbitrary predicates over values.

---

## Part 3: Other Adapters

### 3.1 Plug.Session.Store (FerricStore.Plug.Session)

**Behaviour:** `Plug.Session.Store`

**Callbacks to implement:**

| Callback | FerricStore mapping |
|----------|-------------------|
| `init/1` | Parse options, set key prefix (`"sess:"`) |
| `get/3` | `FerricStore.get("sess:#{sid}")` -> deserialize |
| `put/4` | `FerricStore.set("sess:#{sid}", serialize(data), ttl: max_age * 1000)` |
| `delete/3` | `FerricStore.del("sess:#{sid}")` |

**Session ID generation:** Use `:crypto.strong_rand_bytes(96)` encoded as
Base64 URL-safe. Same entropy as Plug.Session's default.

**Why FerricStore for sessions:**
- Sessions survive BEAM restarts (critical for rolling deploys)
- TTL-based expiry matches session `max_age` semantics
- Raft replication means sessions are available on any node
- No external dependency (Redis, Memcached, database)

**Configuration:**

```elixir
plug Plug.Session,
  store: FerricStore.Plug.Session,
  key: "_my_app_session",
  max_age: 86_400,                  # 24 hours
  ferricstore_prefix: "sess"        # key prefix (default: "sess")
```

### 3.2 Hammer.Backend (FerricStore.Hammer.Backend)

**Behaviour:** `Hammer.Backend`

**Callbacks to implement:**

| Callback | FerricStore mapping |
|----------|-------------------|
| `count_hit/4` | `FerricStore.incr("rl:#{bucket_key}")` + set TTL on first hit |
| `get_bucket/2` | `FerricStore.get("rl:#{bucket_key}")` -> parse count + created_at |
| `delete_buckets/2` | `FerricStore.del("rl:#{bucket_key}")` for each expired bucket |

**Alternative:** Use FerricStore's native rate limiter command directly:

```elixir
FerricStore.ratelimit_add("api:user:42", window_ms: 60_000, max: 100, count: 1)
```

This is a single atomic operation with sliding window semantics, which is
more efficient and accurate than Hammer's fixed-window bucket approach.

**Configuration:**

```elixir
config :hammer,
  backend: {FerricStore.Hammer.Backend, [prefix: "rl"]}
```

### 3.3 Phoenix.PubSub.Adapter (FerricStore.PubSub.Adapter)

**Behaviour:** `Phoenix.PubSub.Adapter`

**Callbacks to implement:**

| Callback | FerricStore mapping |
|----------|-------------------|
| `init/1` | Start subscriber GenServer, return child specs |
| `node_name/1` | Return `node()` |
| `broadcast/5` | `FerricStore.PubSub.publish(topic, message)` -- broadcast via Raft to all nodes |
| `direct_broadcast/5` | Publish to a specific node only |

**Why FerricStore for PubSub:**
- Already has Raft replication -- messages reach all cluster members
- Already has a PubSub module (`Ferricstore.PubSub`) internally
- No dependency on `:pg2` or distributed Erlang for inter-node messaging

**Limitation:** PubSub messages are transient. FerricStore's PubSub channels
are fire-and-forget (same as Redis PUBLISH). Messages are not persisted.
This matches Phoenix.PubSub semantics (at-most-once delivery).

---

## Part 4: Package Structure

```
ferricstore/               <- core engine (hex: ferricstore)
  apps/
    ferricstore/           <- core KV store, shards, Bitcask, Raft
    ferricstore_server/    <- TCP server, RESP protocol

ferricstore_ecto/          <- separate hex package
  lib/
    ferricstore_ecto/
      cached_repo.ex       <- use FerricStore.Ecto.CachedRepo macro
      cacheable.ex         <- use FerricStore.Ecto.Cacheable macro
      cache.ex             <- cache logic (hash-based get, put, invalidate, stats)
      field_codec.ex       <- type-aware field serialization/deserialization
      query_hash.ex        <- Ecto.Query AST hashing
      query_classifier.ex  <- classify query for LIST vs SET storage
      telemetry.ex         <- telemetry event emission
      stats_server.ex      <- GenServer for counters
  mix.exs                  <- depends on ferricstore + ecto

ferricstore_nebulex/       <- separate hex package
  lib/
    ferricstore_nebulex/
      adapter.ex           <- Nebulex adapter implementation
  mix.exs                  <- depends on ferricstore + nebulex

ferricstore_plug/          <- separate hex package
  lib/
    ferricstore_plug/
      session.ex           <- Plug.Session.Store implementation
  mix.exs                  <- depends on ferricstore + plug

ferricstore_hammer/        <- separate hex package
  lib/
    ferricstore_hammer/
      backend.ex           <- Hammer.Backend implementation
  mix.exs                  <- depends on ferricstore + hammer
```

Each package is published independently to Hex. Users install only what
they need. The core `ferricstore` package has zero dependencies on Ecto,
Nebulex, Plug, or Hammer.

**Dependency versions:**

```elixir
# ferricstore_ecto/mix.exs
defp deps do
  [
    {:ferricstore, "~> 0.1"},
    {:ecto, "~> 3.11"}          # prepare_query/3 available since Ecto 3.5
  ]
end

# ferricstore_nebulex/mix.exs
defp deps do
  [
    {:ferricstore, "~> 0.1"},
    {:nebulex, "~> 3.0"}       # Adapter.KV behaviour (v3 API)
  ]
end
```

---

## Part 5: Implementation Priority

### Phase 1: Nebulex Adapter

**Effort:** ~2 days
**Justification:** Simplest to implement -- all callbacks are direct wrappers
around existing FerricStore API functions. Highest adoption potential because
existing Nebulex users can switch with one line. Exercises the FerricStore
embedded API in a real integration context.

**Deliverables:**
- `FerricStore.Nebulex.Adapter` module with all KV + Queryable callbacks
- Complete test suite using Nebulex's shared adapter tests
- Hex package `ferricstore_nebulex`

### Phase 2: Ecto L2 Entity Cache

**Effort:** ~5 days
**Justification:** The core value proposition. Entity caching (`Repo.get`,
`Repo.get!`, `Repo.get_by`) covers the highest-traffic Ecto operations
and is simpler than query caching because the cache key is deterministic
(table + PK).

**Deliverables:**
- `FerricStore.Ecto.CachedRepo` macro
- `FerricStore.Ecto.Cacheable` macro
- Hash-based entity cache logic (HGETALL/HMGET for reads, HSET for cache population)
- Field serialization/deserialization (type-aware string encoding)
- Partial field reads (HMGET for SELECT queries)
- Write-path invalidation (DEL on insert, update, delete)
- Transaction buffering
- Sandbox integration
- Telemetry events
- Test suite

### Phase 3: Ecto L2 Query Cache

**Effort:** ~5 days
**Justification:** Extends the entity cache to cover `Repo.all` and
`Repo.one`. More complex due to query hashing, table tracking, and
broader invalidation scope. Depends on Phase 2 being stable.

**Deliverables:**
- Query hash computation from Ecto.Query AST
- PK list storage with native data structures (LIST for ordered, SET for
  unordered)
- Table-to-query tracking sets (SADD/SMEMBERS)
- Query cache invalidation on writes
- `update_all`/`delete_all` bulk invalidation

### Phase 4: Plug.Session + Hammer Adapters

**Effort:** ~2 days
**Justification:** Small, self-contained adapters. Low effort, useful for
demonstrating FerricStore's versatility and for users who want a single
backing store for their entire application.

**Deliverables:**
- `FerricStore.Plug.Session` module
- `FerricStore.Hammer.Backend` module
- Test suites for each

---

## Appendix A: Ecto Version Requirements

| Feature | Minimum Ecto Version | Notes |
|---------|---------------------|-------|
| `prepare_query/3` callback | 3.5.0 | Used for metadata injection |
| `defoverridable` on Repo functions | 3.0.0 | Core mechanism for Option C |
| `default_options/1` callback | 3.8.0 | Used for cache-specific defaults |
| Telemetry `[:repo, :query]` events | 3.1.0 | Used for observability |
| `Ecto.Query` struct stability | 3.11.0 | Query hash depends on struct fields |

**Minimum supported Ecto version: 3.11.0** (current stable as of 2026).

## Appendix B: FerricStore API Surface Used by Adapters

| FerricStore Function | Ecto L2 Cache | Nebulex Adapter | Plug.Session | Hammer |
|---------------------|:---:|:---:|:---:|:---:|
| `set/3` | Y | Y | Y | Y |
| `get/1` | | Y | Y | Y |
| `del/1` | Y | Y | Y | Y |
| `exists/1` | | Y | | |
| `expire/2` | Y | Y | | |
| `ttl/1` | | Y | | |
| `setnx/2` | | Y | | |
| `getdel/1` | | Y | | |
| `incr/1` | Y | | | |
| `incr_by/2` | | Y | | Y |
| `hset/2` | Y | | | |
| `hgetall/1` | Y | | | |
| `hmget/2` | Y | | | |
| `sadd/2` | Y | | | |
| `smembers/1` | Y | | | |
| `srem/2` | | | | |
| `rpush/2` | Y | | | |
| `lrange/3` | Y | | | |
| `lrem/2` | | | | |
| `keys/1` | Y | Y | | |
| `sandbox_key/1` | Y | Y | | |
| `Sandbox.checkout/1` | Y | Y | | |
| `fetch_or_compute/2` | | | | |

## Appendix C: Hibernate L2 Cache Comparison

| Hibernate/Infinispan | FerricStore.Ecto.Cache | Difference |
|---------------------|----------------------|------------|
| `@Cacheable` | `use FerricStore.Ecto.Cacheable` | Macro vs annotation |
| `@Cache(region = "...")` | `region: "users"` option | Keyword opt vs annotation attr |
| `@Cache(usage = READ_WRITE)` | `strategy: :read_write` | Atom vs enum |
| `CacheMode.NORMAL` | `cache: true` (default) | |
| `CacheMode.IGNORE` | `cache: false` | |
| `CacheMode.REFRESH` | `cache: :refresh` | |
| `CacheMode.GET` | `cache: :cache_only` | |
| `CacheMode.PUT` | `cache: :put_only` | |
| Query cache `setCacheable(true)` | Automatic for cacheable schemas | More convenient |
| Infinispan regions | FerricStore key prefixes | Same concept, different mechanism |
| JTA transactions | Ecto.Multi / Repo.transaction | Buffer-and-commit model |
| Eviction: LRU/LIRS | Eviction: LFU with time decay | FerricStore's LFU is Redis-compatible |
| Cluster: Infinispan replicated | Cluster: Raft consensus | Stronger consistency guarantees |
| Dehydrated entity state (field map) | Hash fields (HGETALL/HMGET for reads, HSET for populate) | Both store field-level, not serialized blobs |
| No partial field read | HMGET for SELECT queries | FerricStore advantage: partial reads on cache hit |
| Full entity invalidation on update | DEL on mutation (same as Hibernate) | Both use delete-based invalidation for consistency |

Note: Hibernate's "dehydrated state" is conceptually similar to our hash-based
approach -- both store individual field values rather than serialized objects.
The key difference is that FerricStore's hash operations are exposed at the
storage level, enabling partial reads (HMGET) that Infinispan's cache API
does not directly support. Both systems use DELETE-based invalidation on
mutations -- HSET is only used on the read path when populating the cache
after a database miss.
