defmodule Ferricstore.Store.Ops do
  @moduledoc """
  Unified interface for store operations.

  Dispatches based on the type of `store` argument:
  - `%FerricStore.Instance{}` struct -> calls Router directly
  - map (closure-based store) -> calls the closure

  This allows incremental migration from closure maps to instance structs
  without changing command handler logic.
  """

  alias Ferricstore.Store.Router

  # --- Basic key operations ---

  def get(%FerricStore.Instance{} = ctx, key), do: Router.get(ctx, key)
  def get(store, key) when is_map(store), do: store.get.(key)

  def get_meta(%FerricStore.Instance{} = ctx, key), do: Router.get_meta(ctx, key)
  def get_meta(store, key) when is_map(store), do: store.get_meta.(key)

  def put(%FerricStore.Instance{} = ctx, key, value, exp), do: Router.put(ctx, key, value, exp)
  def put(store, key, value, exp) when is_map(store), do: store.put.(key, value, exp)

  def delete(%FerricStore.Instance{} = ctx, key), do: Router.delete(ctx, key)
  def delete(store, key) when is_map(store), do: store.delete.(key)

  def exists?(%FerricStore.Instance{} = ctx, key), do: Router.exists?(ctx, key)
  def exists?(store, key) when is_map(store), do: store.exists?.(key)

  def keys(%FerricStore.Instance{} = ctx), do: Router.keys(ctx)
  def keys(store) when is_map(store), do: store.keys.()

  def dbsize(%FerricStore.Instance{} = ctx), do: Router.dbsize(ctx)
  def dbsize(store) when is_map(store), do: store.dbsize.()

  # --- Numeric operations ---

  def incr(%FerricStore.Instance{} = ctx, key, delta), do: Router.incr(ctx, key, delta)
  def incr(store, key, delta) when is_map(store), do: store.incr.(key, delta)

  def incr_float(%FerricStore.Instance{} = ctx, key, delta), do: Router.incr_float(ctx, key, delta)
  def incr_float(store, key, delta) when is_map(store), do: store.incr_float.(key, delta)

  # --- String mutation operations ---

  def append(%FerricStore.Instance{} = ctx, key, suffix), do: Router.append(ctx, key, suffix)
  def append(store, key, suffix) when is_map(store), do: store.append.(key, suffix)

  def getset(%FerricStore.Instance{} = ctx, key, value), do: Router.getset(ctx, key, value)
  def getset(store, key, value) when is_map(store), do: store.getset.(key, value)

  def getdel(%FerricStore.Instance{} = ctx, key), do: Router.getdel(ctx, key)
  def getdel(store, key) when is_map(store), do: store.getdel.(key)

  def getex(%FerricStore.Instance{} = ctx, key, exp), do: Router.getex(ctx, key, exp)
  def getex(store, key, exp) when is_map(store), do: store.getex.(key, exp)

  def setrange(%FerricStore.Instance{} = ctx, key, offset, value), do: Router.setrange(ctx, key, offset, value)
  def setrange(store, key, offset, value) when is_map(store), do: store.setrange.(key, offset, value)

  # --- Native operations ---

  def cas(%FerricStore.Instance{} = ctx, key, expected, new_val, ttl), do: Router.cas(ctx, key, expected, new_val, ttl)
  def cas(store, key, expected, new_val, ttl) when is_map(store), do: store.cas.(key, expected, new_val, ttl)

  def lock(%FerricStore.Instance{} = ctx, key, owner, ttl), do: Router.lock(ctx, key, owner, ttl)
  def lock(store, key, owner, ttl) when is_map(store), do: store.lock.(key, owner, ttl)

  def unlock(%FerricStore.Instance{} = ctx, key, owner), do: Router.unlock(ctx, key, owner)
  def unlock(store, key, owner) when is_map(store), do: store.unlock.(key, owner)

  def extend(%FerricStore.Instance{} = ctx, key, owner, ttl), do: Router.extend(ctx, key, owner, ttl)
  def extend(store, key, owner, ttl) when is_map(store), do: store.extend.(key, owner, ttl)

  def ratelimit_add(%FerricStore.Instance{} = ctx, key, window, max, count), do: Router.ratelimit_add(ctx, key, window, max, count)
  def ratelimit_add(store, key, window, max, count) when is_map(store), do: store.ratelimit_add.(key, window, max, count)

  # --- List operations ---

  def list_op(%FerricStore.Instance{} = ctx, key, op), do: Router.list_op(ctx, key, op)
  def list_op(store, key, op) when is_map(store), do: store.list_op.(key, op)

  # --- Compound key operations ---

  def compound_get(%FerricStore.Instance{} = ctx, redis_key, compound_key), do: Router.compound_get(ctx, redis_key, compound_key)
  def compound_get(store, redis_key, compound_key) when is_map(store), do: store.compound_get.(redis_key, compound_key)

  def compound_get_meta(%FerricStore.Instance{} = ctx, redis_key, compound_key), do: Router.compound_get_meta(ctx, redis_key, compound_key)
  def compound_get_meta(store, redis_key, compound_key) when is_map(store), do: store.compound_get_meta.(redis_key, compound_key)

  def compound_put(%FerricStore.Instance{} = ctx, redis_key, compound_key, value, exp), do: Router.compound_put(ctx, redis_key, compound_key, value, exp)
  def compound_put(store, redis_key, compound_key, value, exp) when is_map(store), do: store.compound_put.(redis_key, compound_key, value, exp)

  def compound_delete(%FerricStore.Instance{} = ctx, redis_key, compound_key), do: Router.compound_delete(ctx, redis_key, compound_key)
  def compound_delete(store, redis_key, compound_key) when is_map(store), do: store.compound_delete.(redis_key, compound_key)

  def compound_scan(%FerricStore.Instance{} = ctx, redis_key, prefix), do: Router.compound_scan(ctx, redis_key, prefix)
  def compound_scan(store, redis_key, prefix) when is_map(store), do: store.compound_scan.(redis_key, prefix)

  def compound_count(%FerricStore.Instance{} = ctx, redis_key, prefix), do: Router.compound_count(ctx, redis_key, prefix)
  def compound_count(store, redis_key, prefix) when is_map(store), do: store.compound_count.(redis_key, prefix)

  def compound_delete_prefix(%FerricStore.Instance{} = ctx, redis_key, prefix), do: Router.compound_delete_prefix(ctx, redis_key, prefix)
  def compound_delete_prefix(store, redis_key, prefix) when is_map(store), do: store.compound_delete_prefix.(redis_key, prefix)

  # --- Prob operations ---

  def prob_write(%FerricStore.Instance{} = ctx, command), do: Router.prob_write(ctx, command)
  def prob_write(store, command) when is_map(store), do: store.prob_write.(command)

  def prob_dir(%FerricStore.Instance{} = ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "prob")
  end
  def prob_dir(store, _key) when is_map(store) and is_map_key(store, :prob_dir), do: store.prob_dir.()
  def prob_dir(store, key) when is_map(store) and is_map_key(store, :prob_dir_for_key), do: store.prob_dir_for_key.(key)

  # --- Flush ---

  def flush(%FerricStore.Instance{} = ctx) do
    Enum.each(Router.keys(ctx), fn k -> Router.delete(ctx, k) end)
    :ok
  end
  def flush(store) when is_map(store), do: store.flush.()

  # --- On push callback (for Waiters notification) ---

  def on_push(store, key) do
    case store do
      %FerricStore.Instance{} -> Ferricstore.Waiters.notify_push(key)
      store when is_map(store) -> if fun = store[:on_push], do: fun.(key)
    end
  end
end
