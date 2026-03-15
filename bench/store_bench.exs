# bench/store_bench.exs
#
# Benchmarks for the Bitcask NIF, ETS cache layer, and Router.
#
# Run:
#   MIX_ENV=bench mix run bench/store_bench.exs
#
# This script starts the full application so that Shard GenServers and
# the Router are available.

alias Ferricstore.Bitcask.NIF
alias Ferricstore.Store.Router

# ---------------------------------------------------------------------------
# Application startup with a temporary data directory
# ---------------------------------------------------------------------------

bench_data_dir = System.tmp_dir!() <> "/ferricstore_bench_#{:rand.uniform(9_999_999)}"
File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
# Use port 0 to avoid binding to the default 6379
Application.put_env(:ferricstore, :port, 0)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== Store / NIF / Router Benchmarks ===")
IO.puts("Data dir: #{bench_data_dir}\n")

# ---------------------------------------------------------------------------
# Helper to create a standalone NIF store in a temp directory
# ---------------------------------------------------------------------------

defmodule BenchHelper do
  @moduledoc false

  def new_nif_store(label) do
    dir = System.tmp_dir!() <> "/ferricstore_nif_bench_#{label}_#{:rand.uniform(9_999_999)}"
    File.mkdir_p!(dir)
    {:ok, store} = NIF.new(dir)
    {store, dir}
  end

  def cleanup(dir) do
    File.rm_rf!(dir)
  end
end

# ---------------------------------------------------------------------------
# Section 1: Raw NIF operations (bypass GenServer / ETS)
# ---------------------------------------------------------------------------

# -- NIF put / get / delete -----------------------------------------------

{nif_store, nif_dir} = BenchHelper.new_nif_store("basic")

# Pre-populate a key for get/delete benchmarks
:ok = NIF.put(nif_store, "existing_key", "some_value", 0)

nif_counter = :counters.new(1, [:atomics])

Benchee.run(
  %{
    "NIF put: single key (no expiry)" => fn ->
      idx = :counters.get(nif_counter, 1)
      :counters.add(nif_counter, 1, 1)
      :ok = NIF.put(nif_store, "bench_key_#{idx}", "bench_value", 0)
    end,

    "NIF put: single key (with expiry)" => fn ->
      idx = :counters.get(nif_counter, 1)
      :counters.add(nif_counter, 1, 1)
      expire_at = System.os_time(:millisecond) + 60_000
      :ok = NIF.put(nif_store, "bench_exp_#{idx}", "bench_value", expire_at)
    end,

    "NIF get: existing key" => fn ->
      {:ok, "some_value"} = NIF.get(nif_store, "existing_key")
    end,

    "NIF get: missing key" => fn ->
      {:ok, nil} = NIF.get(nif_store, "nonexistent_key_#{:rand.uniform(999_999)}")
    end,

    "NIF delete: existing key" => {
      fn {store, key} ->
        NIF.delete(store, key)
      end,
      before_each: fn _ ->
        key = "del_key_#{:rand.uniform(999_999)}"
        :ok = NIF.put(nif_store, key, "to_delete", 0)
        {nif_store, key}
      end
    }
  },
  time: 5,
  warmup: 2,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/store_nif_basic.html", auto_open: false}
  ]
)

BenchHelper.cleanup(nif_dir)

# -- NIF keys() at different store sizes -----------------------------------

IO.puts("\n--- NIF keys() at varying store sizes ---\n")

keys_scenarios = %{
  "NIF keys(): 100 keys" => 100,
  "NIF keys(): 1_000 keys" => 1_000,
  "NIF keys(): 10_000 keys" => 10_000
}

keys_inputs =
  Enum.into(keys_scenarios, %{}, fn {label, count} ->
    {store, dir} = BenchHelper.new_nif_store("keys_#{count}")

    Enum.each(1..count, fn i ->
      :ok = NIF.put(store, "k#{i}", "v#{i}", 0)
    end)

    {label, {store, dir}}
  end)

Benchee.run(
  Enum.into(keys_inputs, %{}, fn {label, {store, _dir}} ->
    {label, fn -> NIF.keys(store) end}
  end),
  time: 5,
  warmup: 2,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/store_nif_keys.html", auto_open: false}
  ]
)

Enum.each(keys_inputs, fn {_label, {_store, dir}} -> BenchHelper.cleanup(dir) end)

# -- NIF put_batch at different sizes --------------------------------------

IO.puts("\n--- NIF put_batch at varying batch sizes ---\n")

batch_10 = Enum.map(1..10, fn i -> {"batch10_k#{i}", "v#{i}", 0} end)
batch_100 = Enum.map(1..100, fn i -> {"batch100_k#{i}", "v#{i}", 0} end)
batch_1000 = Enum.map(1..1000, fn i -> {"batch1000_k#{i}", "v#{i}", 0} end)

{batch_store, batch_dir} = BenchHelper.new_nif_store("batch")

Benchee.run(
  %{
    "NIF put_batch: 10 entries" => fn -> NIF.put_batch(batch_store, batch_10) end,
    "NIF put_batch: 100 entries" => fn -> NIF.put_batch(batch_store, batch_100) end,
    "NIF put_batch: 1000 entries" => fn -> NIF.put_batch(batch_store, batch_1000) end
  },
  time: 5,
  warmup: 2,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/store_nif_batch.html", auto_open: false}
  ]
)

BenchHelper.cleanup(batch_dir)

# ---------------------------------------------------------------------------
# Section 2: Router operations (go through GenServer + ETS)
# ---------------------------------------------------------------------------

IO.puts("\n--- Router operations (GenServer + ETS cache) ---\n")

# Seed 1000 keys through the Router for read benchmarks
Enum.each(1..1000, fn i ->
  Router.put("router_key_#{i}", "router_value_#{i}", 0)
end)

# Warm ETS cache by reading once
Enum.each(1..1000, fn i -> Router.get("router_key_#{i}") end)

router_counter = :counters.new(1, [:atomics])

Benchee.run(
  %{
    "Router.get: ETS cache warm" => fn ->
      idx = rem(:counters.get(router_counter, 1), 1000) + 1
      :counters.add(router_counter, 1, 1)
      _val = Router.get("router_key_#{idx}")
    end,

    "Router.put: single write" => {
      fn key ->
        Router.put(key, "new_value", 0)
      end,
      before_each: fn _ ->
        "router_bench_#{:rand.uniform(999_999)}"
      end
    },

    "Router.keys(): 1000 keys across shards" => fn ->
      _keys = Router.keys()
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/store_router.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
File.rm_rf!(bench_data_dir)
