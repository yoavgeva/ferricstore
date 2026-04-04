# Instance-Based Architecture Design

## Goal

Replace the global singleton FerricStore with module-based instances (like Ecto Repo).
Each instance gets its own shards, ETS tables, Raft system, and config — fully isolated.

## User API

```elixir
# Define an instance (compile-time):
defmodule MyApp.Cache do
  use FerricStore,
    data_dir: "/data/cache",
    max_memory: "1GB",
    shard_count: 4,
    eviction_policy: :allkeys_lfu
end

# Usage — identical to current API but scoped:
MyApp.Cache.set("key", "value")
{:ok, "value"} = MyApp.Cache.get("key")
MyApp.Cache.bf_add("filter", "element")

# Multiple instances in the same app:
defmodule MyApp.Sessions do
  use FerricStore,
    data_dir: "/data/sessions",
    max_memory: "2GB",
    shard_count: 2
end

MyApp.Sessions.set("sess:abc", session_data)
```

## What `use FerricStore` generates

```elixir
defmodule MyApp.Cache do
  use FerricStore, opts

  # The macro generates:
  # 1. A child_spec for the supervision tree
  # 2. All 200+ API functions delegating to FerricStore.Impl
  # 3. An __instance__() function returning the instance ref

  def child_spec(overrides \\ []) do
    %{id: __MODULE__, start: {FerricStore.Instance, :start_link, [__MODULE__, opts ++ overrides]}}
  end

  def set(key, value, opts \\ []) do
    FerricStore.Impl.set(__instance__(), key, value, opts)
  end

  def get(key) do
    FerricStore.Impl.get(__instance__(), key)
  end

  # ... 200+ more functions

  def __instance__ do
    # Returns the instance context — cached in persistent_term per module
    :persistent_term.get({FerricStore.Instance, __MODULE__})
  end
end
```

## The Instance Context

The context is a struct containing all references needed to route operations.
It replaces ALL global persistent_term lookups.

```elixir
defmodule FerricStore.Instance do
  defstruct [
    :name,              # Module name (MyApp.Cache)
    :data_dir,          # Base data directory
    :shard_count,       # Number of shards
    :slot_map,          # Tuple of shard indices (1024 slots)
    :shard_names,       # Tuple of shard GenServer names/pids
    :keydir_names,      # Tuple of ETS table names/refs
    :prefix_table_names, # Tuple of prefix index table names/refs
    :ra_system,         # Ra system name (unique per instance)
    :pressure_flags,    # Atomics ref (3 slots: keydir_full, reject_writes, skip_promotion)
    :disk_pressure,     # Atomics ref (shard_count slots)
    :write_version,     # Counters ref (shard_count slots)
    :stats_counter,     # Counters ref (9 slots)
    :lfu_config,        # {decay_time, log_factor, initial_ref}
    :hot_cache_max_value_size, # Integer
    :max_active_file_size,     # Integer
    :read_sample_rate,  # Integer
    :eviction_policy,   # Atom
    :max_memory_bytes,  # Integer
    :keydir_max_ram,    # Integer
    :memory_limit,      # Integer (cgroup or host RAM)
    :durability_mode,   # :all_quorum | :all_async | :mixed
    :connected_clients_fn, # fn -> integer (injected by server via inject_callbacks/2)
    :process_rss_fn,       # fn -> integer (injected by server via inject_callbacks/2)
    :server_info_fn,       # fn -> map (injected by server via inject_callbacks/2)
  ]
end
```

## Global State Elimination

Every persistent_term, named ETS table, and named process becomes instance-scoped:

### persistent_term (20 keys → 0 global)

| Current global key | Instance field | Notes |
|---|---|---|
| `:ferricstore_shard_count` | `ctx.shard_count` | Direct struct access ~0ns |
| `:ferricstore_slot_map` | `ctx.slot_map` | Direct struct access ~0ns |
| `:ferricstore_shard_names` | `ctx.shard_names` | Direct struct access ~0ns |
| `:ferricstore_keydir_names` | `ctx.keydir_names` | Direct struct access ~0ns |
| `:ferricstore_hot_cache_max_value_size` | `ctx.hot_cache_max_value_size` | Direct ~0ns |
| `:ferricstore_lfu_decay_time` | `ctx.lfu_config` | Direct ~0ns |
| `:ferricstore_lfu_log_factor` | `ctx.lfu_config` | Direct ~0ns |
| `:ferricstore_lfu_initial_ref` | `ctx.lfu_config` | Direct ~0ns |
| `:ferricstore_read_sample_rate` | `ctx.read_sample_rate` | Direct ~0ns |
| `:ferricstore_max_active_file_size` | `ctx.max_active_file_size` | Direct ~0ns |
| `:ferricstore_pressure_flags` | `ctx.pressure_flags` | Direct ~0ns |
| `:ferricstore_durability_mode` | `ctx.durability_mode` | Direct ~0ns |
| `:ferricstore_promotion_threshold` | `ctx.promotion_threshold` | Direct ~0ns |
| `:ferricstore_stats_counter_ref` | `ctx.stats_counter` | Direct ~0ns |
| `:ferricstore_slowlog_threshold` | `ctx.slowlog_threshold` | Direct ~0ns |
| `:ferricstore_slowlog_max_len` | `ctx.slowlog_max_len` | Direct ~0ns |
| `:ferricstore_keyspace_events` | `ctx.keyspace_events` | Direct ~0ns |
| `:ferricstore_has_async_ns` | `ctx.durability_mode` | Derived |
| `:ferricstore_active_file` | Per-shard in instance | Handled by shard |
| `:ferricstore_config` | ETS per instance | Config table |

The `Ferricstore.Mode` module has been deleted entirely — there is no `:mode` config or
standalone/embedded distinction in the library. The `protected_mode` flag is a simple
`Application.get_env(:ferricstore, :protected_mode, false)` that the server sets to `true`
in its `runtime.exs`.

**Performance gain: ~5ns × 3-5 lookups per operation = 15-25ns saved per request.**

### Named ETS tables (per-shard → instance-scoped)

| Current name | Instance approach |
|---|---|
| `:"keydir_0"` .. `:"keydir_3"` | `elem(ctx.keydir_names, idx)` — anonymous ETS, ref in context |
| `:"prefix_keys_0"` .. `:"prefix_keys_3"` | `elem(ctx.prefix_table_names, idx)` |
| `:"bloom_reg_0"` .. `:"bloom_reg_3"` | Removed (prob uses stateless NIFs) |
| `:"cms_reg_0"` .. `:"cms_reg_3"` | Removed |
| `:"cuckoo_reg_0"` .. `:"cuckoo_reg_3"` | Removed |
| `:ferricstore_hotness` | `ctx.hotness_table` — anonymous ETS |
| `:ferricstore_config` | `ctx.config_table` — anonymous ETS |

Anonymous ETS tables are faster than named tables (no atom lookup).

### Named processes (per-shard → instance-scoped)

| Current name | Instance approach |
|---|---|
| `:"Ferricstore.Store.Shard.0"` | `elem(ctx.shard_names, idx)` — can be pid or via tuple |
| `:"Ferricstore.Raft.Batcher.0"` | Derived from instance + shard index |
| `:"Ferricstore.Store.BitcaskWriter.0"` | Derived from instance + shard index |
| `:"Ferricstore.Raft.AsyncApplyWorker.0"` | Derived from instance + shard index |
| `:"Ferricstore.Merge.Scheduler.0"` | Derived from instance + shard index |
| `Ferricstore.Stats` | One per instance |
| `Ferricstore.MemoryGuard` | One per instance |
| `Ferricstore.HLC` | Shared (wall clock is global) |
| `Ferricstore.SlowLog` | One per instance |

Process naming: `{:via, Registry, {FerricStore.Registry, {instance_name, :shard, idx}}}`
or simply store pids in the context struct after startup.

### Atomics/counters (global → instance-scoped)

| Current | Instance approach |
|---|---|
| Pressure flags (3 slots) | `ctx.pressure_flags` — one atomics per instance |
| DiskPressure (shard_count slots) | `ctx.disk_pressure` — one atomics per instance |
| WriteVersion (shard_count counters) | `ctx.write_version` — one counters per instance |
| Stats counters (9 slots) | `ctx.stats_counter` — one counters per instance |
| LFU initial ref (2 slots) | `ctx.lfu_config` |
| HLC (2 slots) | Shared global (time is universal) |
| ActiveFile version (1 slot) | Per instance |

### Ra system (global → instance-scoped)

| Current | Instance approach |
|---|---|
| `:ferricstore_raft` | `:"ferricstore_raft_#{instance_name}"` — unique per instance |
| WAL dir: `data_dir/ra` | `instance_data_dir/ra` |
| Shard server: `:"ferricstore_shard_0"` | `:"#{instance_name}_shard_0"` |

## Module Structure

```
lib/
  ferric_store.ex                    # use macro + __using__ callback
  ferric_store/
    impl.ex                          # All 200+ functions taking ctx as first arg
    instance.ex                      # Instance struct + start_link + init
    instance/
      supervisor.ex                  # Per-instance supervision tree
    store/
      router.ex                      # All functions take ctx as first arg
      shard.ex                       # Shard GenServer (receives ctx at init)
      ...
    raft/
      cluster.ex                     # Takes instance name for ra system naming
      state_machine.ex               # Receives ctx refs via machine_config
      ...
    commands/
      dispatcher.ex                  # Takes ctx
      bloom.ex                       # Takes store map (which includes ctx refs)
      ...
    memory_guard.ex                  # Per-instance GenServer
    stats.ex                         # Per-instance counters
```

## Router Changes

```elixir
# Current:
def get(key) do
  idx = shard_for(key)
  keydir = :"keydir_#{idx}"
  # ... ETS lookup

# New:
def get(ctx, key) do
  idx = shard_for(ctx, key)
  keydir = elem(ctx.keydir_names, idx)
  # ... ETS lookup (same logic, just resolved from ctx)
```

Every Router function gains `ctx` as first arg. Internally, ALL persistent_term.get
calls are replaced with `ctx.field` access.

## Shard Changes

```elixir
# Current:
def init(opts) do
  index = Keyword.fetch!(opts, :index)
  data_dir = Keyword.fetch!(opts, :data_dir)
  # ... creates named ETS tables, starts named Raft

# New:
def init(opts) do
  ctx = Keyword.fetch!(opts, :instance_ctx)
  index = Keyword.fetch!(opts, :index)
  # ... uses anonymous ETS refs from ctx, starts instance-scoped Raft
```

## Command Handler Changes

Command handlers already receive a `store` map. The store map is built by the shard
(or connection) and includes all the callbacks. The ctx is embedded in the store map:

```elixir
# Store map includes ctx:
store = %{
  ctx: ctx,
  get: fn key -> Router.get(ctx, key) end,
  put: fn key, value, exp -> Router.put(ctx, key, value, exp) end,
  prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
  prob_dir: fn -> Path.join(ctx.shard_data_path, "prob") end,
  ...
}
```

Command handlers don't change — they still call `store.get.(key)`.
The ctx flows through the closures.

## Connection Changes (ferricstore_server)

```elixir
# Current:
defp build_raw_store do
  %{get: &Router.get/1, put: &Router.put/3, ...}

# New:
defp build_raw_store(ctx) do
  %{get: &Router.get(ctx, &1), put: &Router.put(ctx, &1, &2, &3), ...}
```

The connection receives the instance ctx at init (from the listener config).
Multiple listeners can point to different instances.

After startup, the server application injects server-specific callbacks into the
instance via `FerricStore.Instance.inject_callbacks/2`. This provides the library
with access to server-only information (connected clients count, process RSS,
server info) without creating any compile-time or runtime dependency on the server:

```elixir
# In ferricstore_server startup (e.g., application.ex):
FerricStore.Instance.inject_callbacks(:default, %{
  connected_clients_fn: fn -> ConnectionRegistry.count() end,
  process_rss_fn: fn -> SystemInfo.rss_bytes() end,
  server_info_fn: fn -> ServerInfo.collect() end
})
```

## Supervision Tree

```
Application
├── FerricStore.Registry (process registry for all instances)
├── FerricStore.HLC (shared — time is global)
└── (instances started dynamically or via config)

Per instance (MyApp.Cache):
├── MyApp.Cache.Supervisor (one_for_one)
│   ├── Stats GenServer
│   ├── MemoryGuard GenServer
│   ├── SlowLog GenServer
│   ├── Config GenServer
│   ├── ShardSupervisor (one_for_one)
│   │   ├── Shard.0
│   │   ├── Shard.1
│   │   ├── Shard.2
│   │   └── Shard.3
│   ├── BatcherSupervisor
│   │   ├── Batcher.0 .. Batcher.3
│   ├── BitcaskWriterSupervisor
│   │   ├── BitcaskWriter.0 .. BitcaskWriter.3
│   ├── AsyncApplyWorkerSupervisor
│   │   ├── AsyncApplyWorker.0 .. AsyncApplyWorker.3
│   ├── MergeSupervisor
│   │   ├── Merge.Scheduler.0 .. Merge.Scheduler.3
│   └── Ra system (ferricstore_raft_myapp_cache)
```

## Test Isolation (free!)

```elixir
# Each test gets its own instance:
setup do
  {:ok, _} = TestCache.start_link(data_dir: make_temp_dir(), shard_count: 2)
  on_exit(fn -> TestCache.stop() end)
  :ok
end

test "my test" do
  TestCache.set("key", "value")
  assert {:ok, "value"} = TestCache.get("key")
end
```

No flush_all_keys, no sandbox, no shared state. Tests can run `async: true`.

## Implementation

This is a new library — there is no existing user base to migrate. We implement
the instance-based architecture directly. No backward compatibility, no
deprecation phases, no global singleton fallback.

The `FerricStore` module exists ONLY as the `use` macro provider. There are no
global `FerricStore.set/2` or `FerricStore.get/1` functions. Users must define
a module with `use FerricStore` to get an API.

The application module does NOT start any instances automatically. Each instance
is started by the user's supervision tree:

```elixir
# In the user's application.ex:
children = [
  MyApp.Cache,     # starts the FerricStore instance
  MyApp.Sessions,  # starts another independent instance
]
```

## Performance Comparison

| Operation | Current (persistent_term) | Instance (struct field) |
|---|---|---|
| shard_for(key) | pt.get(:slot_map) + pt.get(:shard_count) = ~10ns | ctx.slot_map + ctx.shard_count = ~0ns |
| resolve_keydir(idx) | pt.get(:keydir_names) = ~5ns | elem(ctx.keydir_names, idx) = ~0ns |
| LFU config | 2 × pt.get = ~10ns | ctx.lfu_config = ~0ns |
| Hot cache threshold | pt.get = ~5ns | ctx.hot_cache_max_value_size = ~0ns |
| Total per GET (hot) | ~30-40ns in lookups | ~0ns in lookups |

**Estimated 30-40ns faster per operation** from eliminating persistent_term lookups.
At 100K ops/sec that's 3-4ms saved per second. At 1M ops/sec that's 30-40ms.

## Files

All files are rewritten to use the instance context. No global persistent_term,
no globally named processes, no singleton assumptions.

| File | Role |
|---|---|
| `ferricstore.ex` | `use` macro only — generates 208 API functions per module |
| `ferricstore/instance.ex` | Instance struct + start_link + init |
| `ferricstore/instance/supervisor.ex` | Per-instance supervision tree |
| `ferricstore/impl.ex` | All 208 functions taking `ctx` as first arg |
| `store/router.ex` | All routing functions take `ctx` — zero persistent_term |
| `store/shard.ex` | Receives `ctx` at init, all operations use ctx refs |
| `store/disk_pressure.ex` | Takes atomics ref from ctx |
| `store/write_version.ex` | Takes counters ref from ctx |
| `store/active_file.ex` | Instance-scoped |
| `store/lfu.ex` | Reads config from ctx |
| `raft/cluster.ex` | Instance-scoped ra system name |
| `raft/state_machine.ex` | Receives ctx refs via machine_config |
| `raft/batcher.ex` | Instance-scoped naming |
| `raft/async_apply_worker.ex` | Instance-scoped naming |
| `commands/dispatcher.ex` | Passes ctx through store map |
| `commands/*.ex` | No changes (use store map callbacks) |
| `memory_guard.ex` | Per-instance GenServer |
| `stats.ex` | Per-instance counters |
| `application.ex` | Minimal — just HLC + shared Tokio runtime |
| `config.ex` | Per-instance config ETS |
| Connection (server) | Receives instance ctx from listener config |
