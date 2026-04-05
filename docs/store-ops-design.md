# Store Ops Design: Replace closures with direct ctx

## Problem

Command handlers receive a `store` map with ~25 closures:
```elixir
store = %{
  get: fn key -> Router.get(ctx, key) end,
  put: fn key, val, exp -> Router.put(ctx, key, val, exp) end,
  ...25 more closures
}
```

This causes:
- 445-line `build_local_store` closure factory
- ~10ns overhead per anonymous function call
- Untraceable stack traces (`#Function<...>`)
- Stale closure capture in transactions
- Complex MockStore test setup

## Solution: `Ferricstore.Store.Ops` dispatch module

### Phase 1: Create Ops module (no behavior change)

```elixir
defmodule Ferricstore.Store.Ops do
  def get(store, key) when is_map(store), do: store.get.(key)
  def get(%FerricStore.Instance{} = ctx, key), do: Router.get(ctx, key)

  def put(store, key, val, exp) when is_map(store), do: store.put.(key, val, exp)
  def put(%FerricStore.Instance{} = ctx, key, val, exp), do: Router.put(ctx, key, val, exp)
  # ... all 25 operations
end
```

### Phase 2: Migrate handlers (mechanical)

```elixir
# Before: store.get.(key)
# After:  Ops.get(store, key)
```

Each command handler file changes independently. Tests keep working.

### Phase 3: Add Router.compound_* functions

```elixir
def compound_get(ctx, redis_key, compound_key) do
  shard = elem(ctx.shard_names, shard_for(ctx, redis_key))
  GenServer.call(shard, {:compound_get, redis_key, compound_key})
end
```

### Phase 4: Switch hot path to ctx

Connection.dispatch_normal passes `ctx` directly instead of building store map.
`build_raw_store` becomes unnecessary for the normal path.

### Phase 5: Transaction struct

Replace `build_local_store` with `%LocalTxStore{}` struct that Ops handles.

## Files to change (~95 files)

- 23 command handler modules
- 12 core infrastructure files
- 2 test support files
- ~58 test files using MockStore

## Incremental migration

Ops accepts both maps and structs — files migrate one at a time.
