# Elixir Performance Guide

Practical tips for writing fast, efficient Elixir — from data structures and process design to BEAM-specific patterns and OTP best practices.

---

## Table of Contents

1. [Data Structures](#1-data-structures)
2. [Enumerables & Streams](#2-enumerables--streams)
3. [Processes & Concurrency](#3-processes--concurrency)
4. [Pattern Matching & Guards](#4-pattern-matching--guards)
5. [ETS & Caching](#5-ets--caching)
6. [Memory & Binaries](#6-memory--binaries)
7. [Compiler & Tooling](#7-compiler--tooling)
8. [Helping the Compiler](#8-helping-the-compiler)
9. [BEAM VM Tuning](#9-beam-vm-tuning)

---

## 1. Data Structures

### Prefer structs over plain maps whenever possible
> 🔴 **Critical**

Structs are better than plain maps in almost every way when the keys are known at compile time. They are slightly more memory-efficient — because all instances share the same fixed key layout, the BEAM can optimise key storage across instances, whereas plain maps store keys independently per value. On top of that you get compile-time field validation, `@enforce_keys` to prevent partially-constructed data, protocol dispatch, and pattern matches that self-document the shape. Only fall back to a plain map when keys are dynamic or unknown at compile time.

```elixir
# ❌ Avoid — keys stored per-instance, no safety, no schema
user = %{name: "Ada", email: "ada@example.com"}
user.emial  # silently returns nil — typo goes unnoticed

# ✅ Prefer — slightly leaner memory + compile-time guarantees
defmodule User do
  @enforce_keys [:name, :email]
  defstruct [:name, :email, :role]
end

%User{name: "Ada", email: "ada@example.com"}
# Typo at compile time → KeyError immediately
# Missing :email → raises ArgumentError

# ✅ Structs shine in function head pattern matching
def greet(%User{name: name}), do: "Hello, #{name}"
def greet(%Admin{name: name}), do: "Welcome, #{name}"
```

---

### Prefer maps over keyword lists for lookups
> 🔴 **Critical**

Keyword lists are ordered linked lists — O(n) lookup. Maps and structs give O(log n) access with no scan. Only use keyword lists when order matters or you need duplicate keys (like DSL options passed to a macro).

```elixir
# ❌ Avoid — O(n) scans whole list
opts = [timeout: 5000, retries: 3]
Keyword.get(opts, :timeout)

# ✅ Prefer struct when shape is known
defmodule Opts do
  defstruct timeout: 5000, retries: 3
end
%Opts{}.timeout

# ✅ Plain map when keys vary at runtime
config = %{timeout: 5000, retries: 3}
config.timeout
```

---

### Use structs when the shape is fixed, maps when it isn't
> 🟢 **Best Practice**

The decision rule: if you know all the keys at compile time, use a struct — you get better memory layout and safety for free. If the keys are determined at runtime (grouping, accumulation, arbitrary config), use a plain map.

| Situation | Use |
|-----------|-----|
| Domain entity (User, Order, Invoice) | **Struct** |
| Fixed config with known keys | **Struct** |
| Dynamic grouping / frequency counts | Plain map |
| Accumulator with runtime-determined keys | Plain map |
| Passing options to a macro/DSL | Keyword list |

---

### Use MapSet for membership checks
> 🟡 **Important**

Checking if a value is in a list is O(n). MapSet uses a hash set internally, giving O(1) average membership tests. Critical for hot paths with repeated `contains?` checks.

```elixir
# ❌ Avoid — O(n) scan
ids = [1, 2, 3, 4, 5]
5 in ids

# ✅ Prefer — O(1)
ids = MapSet.new([1, 2, 3, 4, 5])
MapSet.member?(ids, 5)
```

---

### Choose the right collection for your access pattern
> 🟢 **Best Practice**

Lists are great for sequential processing. Tuples are cheap for fixed-size data. Maps excel at keyed lookups. ETS handles large shared datasets. Pick based on how you'll access — not just how you'll build.

```elixir
# Fixed arity record: use tuple
{:ok, user, token}

# Growing keyed data: use map
%{user_id => profile}

# Large shared table: use ETS
:ets.lookup(:cache, key)
```

---

### Avoid repeated list operations in tight loops
> 🟡 **Important**

`length/1`, `Enum.count/1`, `Enum.reverse/1` all traverse the whole list. Cache the result outside the loop, or restructure to use a single pass with `Enum.reduce`.

```elixir
# ❌ Avoid — length/1 called on every iteration!
for i <- 0..length(items)-1 do
  Enum.at(items, i)
end

# ✅ Prefer — single pass, no repeated traversals
Enum.each(items, fn item ->
  process(item)
end)
```

---

## 2. Enumerables & Streams

### Use Stream for large or infinite data
> 🔴 **Critical**

`Enum` builds intermediate lists in memory at every step. `Stream` is lazy — it chains operations and processes elements one at a time. Essential when working with large files, database results, or infinite sequences.

```elixir
# ❌ Avoid — loads all into memory at each step
File.stream!("big.log")
|> Enum.map(&parse/1)
|> Enum.filter(&relevant?/1)
|> Enum.take(10)

# ✅ Prefer — materialise only 10 elements
File.stream!("big.log")
|> Stream.map(&parse/1)
|> Stream.filter(&relevant?/1)
|> Enum.take(10)
```

---

### Prefer Enum.reduce over multiple passes
> 🟡 **Important**

Each `Enum` call is a full traversal. Combining operations into a single reduce avoids n separate passes. If you find yourself piping 3+ Enum operations, consider a reduce.

```elixir
# ❌ Avoid — 3 separate traversals
list
|> Enum.filter(&active?/1)
|> Enum.map(&transform/1)
|> Enum.sum()

# ✅ Prefer — single traversal
Enum.reduce(list, 0, fn item, acc ->
  if active?(item),
    do: acc + transform(item),
    else: acc
end)
```

---

### Build lists with cons, then reverse once
> 🟢 **Best Practice**

Prepending to a list (cons) is O(1). Appending is O(n) because Elixir lists are singly-linked. Build reversed, then reverse at the end — this is the standard pattern that `Enum.reduce` uses internally.

```elixir
# ❌ Avoid — O(n²) appending on each step
Enum.reduce(data, [], fn x, acc ->
  acc ++ [transform(x)]
end)

# ✅ Prefer — O(n) total: prepend + one reverse
data
|> Enum.reduce([], fn x, acc ->
  [transform(x) | acc]
end)
|> Enum.reverse()
```

---

### Use Task.async_stream for parallel work
> 🟣 **Advanced**

When each element requires independent I/O or CPU work, `Task.async_stream` maps across a collection concurrently with backpressure. Specify `max_concurrency` to avoid overwhelming downstream resources.

```elixir
urls
|> Task.async_stream(
  &fetch/1,
  max_concurrency: 10,
  timeout: 5_000
)
|> Enum.map(fn {:ok, result} -> result end)
```

---

## 3. Processes & Concurrency

### Don't use processes as caches
> 🔴 **Critical**

Creating a GenServer just to hold state introduces mailbox overhead and a serialisation bottleneck. Use ETS for shared read-heavy data, or module attributes for compile-time constants. Reserve processes for stateful behaviour with side effects.

```elixir
# ❌ Avoid — GenServer as a lookup table
def get_config(key) do
  GenServer.call(:config, {:get, key})
end

# ✅ Prefer — ETS: concurrent reads, no bottleneck
def get_config(key) do
  :ets.lookup_element(:config, key, 2)
end
```

---

### Keep GenServer message handlers fast
> 🔴 **Critical**

`handle_call/3` and `handle_cast/2` block the process's mailbox for their duration. Offload long work to a Task and reply asynchronously. A slow handler means all callers queue up.

```elixir
# ❌ Avoid — blocks the mailbox
def handle_call(:heavy_work, _from, state) do
  result = do_slow_computation()
  {:reply, result, state}
end

# ✅ Prefer — offload to a Task, reply async
def handle_call(:heavy_work, from, state) do
  Task.start(fn ->
    result = do_slow_computation()
    GenServer.reply(from, result)
  end)
  {:noreply, state}
end
```

---

### Avoid synchronous calls across many processes
> 🟡 **Important**

`GenServer.call/3` blocks the caller until the server responds. Chaining multiple calls adds latency and risks timeouts. Use `GenServer.cast/2` for fire-and-forget, or batch operations into a single call.

```elixir
# Batch: one call, one round trip
GenServer.call(server, {:batch, operations})

# Or async when you don't need a reply
GenServer.cast(server, {:update, value})
```

---

### Limit process spawning in hot paths
> 🟡 **Important**

Processes are cheap, but not free. Spawning thousands per request strains the scheduler and the GC. Use a pool (poolboy, nimble_pool) for bounded concurrency, or let a dedicated GenServer fan out the work.

```elixir
# ❌ Avoid — unbounded process per item
Enum.each(items, fn item ->
  spawn(fn -> process(item) end)
end)

# ✅ Prefer — bounded via Task.async_stream
Task.async_stream(items, &process/1,
  max_concurrency: System.schedulers_online()
)
```

---

## 4. Pattern Matching & Guards

### Pattern match in function heads, not bodies
> 🟢 **Best Practice**

The BEAM compiles function head clauses into an efficient decision tree. Matching in the head avoids entering the function body only to branch with a `case` or `cond`. This is both idiomatic and faster.

```elixir
# ❌ Avoid — enters function then branches
def handle(event) do
  case event do
    {:click, x, y} -> ...
    {:key, code}   -> ...
    _              -> :ignore
  end
end

# ✅ Prefer — compiler builds a decision tree
def handle({:click, x, y}), do: ...
def handle({:key, code}),   do: ...
def handle(_),              do: :ignore
```

---

### Prefer guards over if/case for function clause conditions
> 🔴 **Critical**

Guards are evaluated before the BEAM allocates a stack frame or enters the function body. A failed guard costs almost nothing — no local bindings, no body execution. An `if` inside a body always enters the function first. Beyond performance, guards give you compile-time warnings for unreachable clauses and keep dispatch logic at the boundary where it belongs.

```elixir
# ❌ Avoid — enters the function, allocates frame, then branches
def process(n) do
  if is_integer(n) and n > 0 do
    do_work(n)
  else
    {:error, :invalid}
  end
end

# ✅ Prefer — guard fails before the body is ever entered
def process(n) when is_integer(n) and n > 0, do: do_work(n)
def process(_), do: {:error, :invalid}
```

---

### Know when guards cannot replace if/case
> 🟡 **Important**

Guards are a **restricted language** — only pure, side-effect-free built-in functions are allowed (`is_integer/1`, `is_nil/1`, `>/2`, `length/1`, `map_size/1`, etc.). You cannot call your own functions in a guard. When the condition requires arbitrary logic, `case` or `if` inside the body is the correct tool.

```elixir
# ❌ Won't compile — custom function not allowed in guard
def process(user) when User.active?(user), do: ...

# ✅ Use case inside the body instead
def process(user) do
  case User.active?(user) do
    true  -> do_work(user)
    false -> {:error, :inactive}
  end
end

# ✅ Guards are fine for built-in checks
def process(user) when is_struct(user, User), do: do_work(user)
def process(_), do: {:error, :invalid}
```

---

### Follow the branching hierarchy
> 🟢 **Best Practice**

Pick the right construct for the situation — each level is more expressive but slightly less optimised than the one above it.

| Preference | Construct | When to use |
|---|---|---|
| 1st | Function head + guard | Condition expressible with built-in BIFs |
| 2nd | `case` | Multi-branch logic inside a body |
| 3rd | `cond` | Multiple unrelated boolean conditions |
| 4th | `if` | Simple true/false, no pattern needed |

```elixir
# 1st choice — guard at function head
def handle(n) when is_integer(n) and n > 0, do: do_work(n)

# 2nd choice — case for multi-branch inside body
def handle(input) do
  case parse(input) do
    {:ok, n}    -> do_work(n)
    {:error, e} -> log_error(e)
  end
end

# 3rd choice — cond for unrelated conditions
def classify(n) do
  cond do
    n < 0    -> :negative
    n == 0   -> :zero
    n > 1000 -> :large
    true     -> :normal
  end
end

# 4th choice — if for simple boolean
def maybe_run(enabled, fun) do
  if enabled, do: fun.(), else: :skipped
end
```

---

### Avoid recomputing values in multiple branches
> 🟡 **Important**

When several branches in a `case` need the same derived value, compute it once before the case. The compiler won't automatically hoist shared sub-expressions out of branches.

```elixir
# ❌ Avoid — fetch_data called twice
case status do
  :active   -> fetch_data(id) |> transform()
  :pending  -> fetch_data(id) |> transform()
  :inactive -> nil
end

# ✅ Prefer — compute once
data = if status in [:active, :pending],
  do: fetch_data(id) |> transform()

case status do
  :active   -> data
  :pending  -> data
  :inactive -> nil
end
```

---

## 5. ETS & Caching

### Use ETS for shared, read-heavy data
> 🔴 **Critical**

ETS tables live in shared memory — reads from any process don't require message passing. Concurrent reads are essentially free. Use `:set` for keyed access, `:ordered_set` if you need range queries, `:bag` if multiple values per key are needed.

```elixir
# Create once at startup
:ets.new(:cache, [
  :set, :public, :named_table,
  read_concurrency: true
])

# Any process can read with no bottleneck
:ets.lookup(:cache, key)
```

---

### Set read_concurrency and write_concurrency appropriately
> 🟡 **Important**

`read_concurrency: true` uses separate lock segments, dramatically improving parallel read throughput. `write_concurrency: true` reduces write contention. Set both when you have many concurrent readers and writers.

```elixir
:ets.new(:hot_table, [
  :set, :public, :named_table,
  read_concurrency: true,
  write_concurrency: true
])
```

---

### Use :persistent_term for truly static data
> 🟣 **Advanced**

`:persistent_term` stores terms in shared, read-only memory with zero-copy reads — the fastest possible lookup in the BEAM. Ideal for configs, compiled regexes, and lookup tables that rarely or never change. Writes are expensive and cause GC pauses across all processes.

```elixir
# Write once at startup
:persistent_term.put(:regex, ~r/pattern/)

# Any process reads with zero copy
:persistent_term.get(:regex)
|> Regex.match?(input)
```

---

### Implement TTL with scheduled messages
> 🟢 **Best Practice**

ETS has no built-in expiry. Use `Process.send_after/3` in a GenServer to schedule periodic cleanups. Store the insertion timestamp alongside the value and filter on read, or delete on the cleanup sweep.

```elixir
# In GenServer init:
Process.send_after(self(), :evict_expired, 60_000)

def handle_info(:evict_expired, state) do
  now = System.monotonic_time(:second)
  # delete entries older than TTL...
  Process.send_after(self(), :evict_expired, 60_000)
  {:noreply, state}
end
```

---

## 6. Memory & Binaries

### Use binary pattern matching for parsing
> 🔴 **Critical**

The BEAM's binary matching is implemented in native code and works on sub-binaries without copying. Parsing protocols, file formats, or network packets with pattern matching is far faster than splitting strings or using regex.

```elixir
# ❌ Avoid — allocates, scans repeatedly
[a, b] = String.split(data, ":")
{a, b} = {String.to_integer(a), b}

# ✅ Prefer — native binary match: zero copy, O(1)
<<len::32, rest::binary>> = data
<<value::binary-size(len), _::binary>> = rest
```

---

### Prefer iodata for string building
> 🟡 **Important**

String concatenation (`<>/2`) copies both operands each time — O(n²) in a loop. `IO.iodata` is a nested list of strings that the VM flattens lazily at the final output point. Use it anywhere you're building strings incrementally.

```elixir
# ❌ Avoid — each <> copies everything so far
Enum.reduce(parts, "", fn p, acc ->
  acc <> "," <> p
end)

# ✅ Prefer — no copying until IO.iodata_to_binary
parts
|> Enum.intersperse(",")
|> IO.iodata_to_binary()
```

---

### Avoid large message passing between processes
> 🟡 **Important**

Sending a message between processes copies the data (except for large binaries >= 64 bytes, which are reference-counted). Passing large maps or nested structures frequently is expensive. Consider passing a reference (PID, ETS key) instead.

```elixir
# ❌ Avoid — copies the entire large_map on every call
GenServer.call(pid, {:update, large_map})

# ✅ Prefer — store in ETS; pass the key
:ets.insert(:data, {key, large_map})
GenServer.cast(pid, {:process, key})
```

---

### Release large binaries with Process.hibernate/3
> 🟣 **Advanced**

Long-lived GenServers accumulate heap fragments. `Process.hibernate/3` triggers a full GC, compacts the heap to a minimum, and suspends until the next message. Use it in servers that handle bursts of large-binary work and then go idle.

```elixir
def handle_cast(:idle, state) do
  :proc_lib.hibernate(
    GenServer, :enter_loop,
    [__MODULE__, [], state]
  )
end
```

---

## 7. Compiler & Tooling


### Prefer binary pattern matching over regex
> 🔴 **Critical**

The BEAM's regex engine is PCRE — a full backtracking engine that is powerful but slow compared to native binary matching. For any data with a known structure (protocols, file formats, delimited strings, network packets), binary pattern matching runs in native BEAM bytecode with zero overhead. Only reach for regex when the pattern is genuinely irregular or the complexity of writing the algorithm isn't justified.

```elixir
# ❌ Avoid — PCRE overhead for structured data
def parse_header(data) do
  Regex.run(~r/^(\w+):(\d+)$/, data)
end

# ✅ Prefer — native binary match, no engine overhead
def parse_header(data) do
  [key, value] = String.split(data, ":", parts: 2)
  {key, String.to_integer(value)}
end

# ✅ Even better for binary protocols — zero allocation
def parse_packet(<<type::8, len::16, payload::binary-size(len), _::binary>>) do
  {type, payload}
end
```

---

### Prefer hand-written functions over regex for hot paths
> 🟡 **Important**

A hand-written recursive function or a `String` module call compiles to efficient BEAM bytecode. Regex spins up the PCRE engine on every call. For anything called frequently — validation, tokenisation, parsing — the algorithm will almost always win.

```elixir
# ❌ Avoid in hot paths — PCRE engine on every call
def digits_only?(s), do: Regex.match?(~r/^\d+$/, s)

# ✅ Prefer — pure Elixir, no engine
def digits_only?(s) do
  s != "" and String.to_charlist(s) |> Enum.all?(&(&1 in ?0..?9))
end

# ✅ Or even simpler with binary match
def digits_only?(<<>>), do: false
def digits_only?(s), do: Enum.all?(to_charlist(s), &(&1 in ?0..?9))
```

---

### If you must use regex, compile it at compile time
> 🟡 **Important**

`Regex.compile!/1` inside a function body recompiles the PCRE pattern on every single call — the most expensive possible option. If regex is genuinely the right tool, at minimum use the `~r//` sigil so it compiles once at build time and is stored as a module constant.

```elixir
# ❌ Worst — recompiles on every call
def valid_slug?(s) do
  Regex.match?(Regex.compile!("^[a-z0-9-]+$"), s)
end

# ✅ Better — compiled once at build time
@slug_regex ~r/^[a-z0-9-]+$/

def valid_slug?(s), do: Regex.match?(@slug_regex, s)

# ✅ Best — no regex at all
def valid_slug?(s) do
  s != "" and String.to_charlist(s)
  |> Enum.all?(&(&1 in ?a..?z or &1 in ?0..?9 or &1 == ?-))
end
```

The priority order for string/binary matching is: **binary pattern match → hand-written function → compiled `~r//` → never `Regex.compile!/1` at runtime**.

---

### Use module attributes for compile-time constants
> 🟢 **Best Practice**

Module attributes (`@name`) are inlined at their use sites during compilation. They're free at runtime — no lookup, no call. Use them for lookup tables, regexes, configs, and any value known at compile time.

```elixir
@status_codes %{
  200 => :ok,
  404 => :not_found,
  500 => :server_error
}

def decode(code), do: @status_codes[code]
```

---

### Profile before optimising
> 🟣 **Advanced**

Use `:fprof` or `:eprof` for CPU profiling, `:recon` for live production inspection, and `:observer` for a GUI overview. Use `Benchee` for micro-benchmarks. Always measure — Elixir's hotspots are rarely where intuition suggests.

```elixir
# Benchmark two approaches
Benchee.run(%{
  "approach_a" => fn -> approach_a() end,
  "approach_b" => fn -> approach_b() end
}, time: 5, memory_time: 2)

# Production profiling
:recon.proc_count(:reductions, 5)
```

---

### Enable native compilation for hot modules
> 🟣 **Advanced**

The OTP 24+ JIT runs automatically on amd64. For earlier versions, annotate hot modules explicitly. Gains are largest in pure computation; I/O-heavy code sees little improvement.

```elixir
# OTP < 24: annotate for HiPE
@compile :native
@compile {:hipe_opt, [:o3]}

# OTP 24+: JIT is automatic on amd64
# Verify it's enabled:
:erlang.system_info(:emu_flavor)
# Should return :jit
```

---

## 8. Helping the Compiler

The OTP compiler (OTP 26+) has a type inference pass that analyses your code and uses inferred types to eliminate redundant checks, simplify pattern matches, and remove dead branches — all automatically. It does **not** read your `@spec` annotations; it infers types from the code itself. The more explicit and consistent your code is, the tighter the inference and the more the compiler can optimise.

### Return consistent shapes from functions
> 🔴 **Critical**

When a function returns different shapes in different branches, the inferred type becomes a wide union and downstream optimisations weaken. Tagged tuples with consistent shapes give the compiler a precise type to work with.

```elixir
# ❌ Hard to infer — returns integer OR atom
def fetch(x) do
  if x > 0, do: x * 2, else: :error
end

# ✅ Consistent tagged tuples — compiler knows exact shape
def fetch(x) when x > 0, do: {:ok, x * 2}
def fetch(_),             do: {:error, :invalid}
```

---

### Narrow types at the function boundary with guards and pattern matching
> 🔴 **Critical**

The compiler narrows types as it descends into a function. The earlier a type is narrowed — ideally at the function head — the more of the body it can optimise. A value that enters as `any()` stays `any()` until something constrains it.

```elixir
# ❌ Type stays wide throughout the body
def process(input) do
  value = Map.get(input, :value)
  value * 2  # compiler doesn't know value is integer
end

# ✅ Narrow at the head — type is known immediately
def process(%{value: value}) when is_integer(value) do
  value * 2  # compiler knows: integer arithmetic
end
```

---

### Validate and narrow external data at the boundary
> 🟡 **Important**

Data from external sources — JSON decode, ETS lookup, `receive`, ports — enters as `any()`. If `any()` flows deep into business logic, the compiler cannot optimise any of the downstream operations. Match and guard at the entry point to give the value a concrete type before it propagates.

```elixir
# ❌ any() flows deep into business logic
def handle(%{"count" => count}) do
  compute(count)  # count is any() — no optimisation possible
end

# ✅ Narrow at the boundary
def handle(%{"count" => count}) when is_integer(count) do
  compute(count)  # integer — compiler can optimise arithmetic
end
```

---

### Prefer struct field access over Map.get for known shapes
> 🟡 **Important**

`Map.get/2` and `map[key]` are opaque to the type inferrer — the return type is always `any()`. Struct field access (`.field`) and pattern matching give the compiler a concrete type derived from the struct definition.

```elixir
# ❌ Opaque — result is any()
val = Map.get(data, :count)
val + 1

# ✅ Transparent — compiler knows val's type from the struct
%Counter{count: val} = data
val + 1
```

---

### Keep functions small and single-purpose
> 🟢 **Best Practice**

Type inference works per-function. A large function with many branches produces wide union types that are hard to narrow. Small, focused functions let inference converge on tight types quickly and enable more aggressive inlining and dead-branch elimination.

```elixir
# ❌ One big function — types widen with each branch
def handle(event, state) do
  # many branches, many types flowing around
end

# ✅ Small focused clauses — tight types per function
def handle({:click, x, y}, state), do: handle_click(x, y, state)
def handle({:key, code}, state),   do: handle_key(code, state)
```

---

### Use :maps.filter/2 and :maps.map/2 over Enum for maps
> 🟡 **Important**

The Erlang `:maps` module functions are implemented as BIFs (built-in functions) — they run as native ERTS operations, not Elixir-level iteration. For filtering and mapping over maps specifically, they are measurably faster than `Enum.filter/2` or `Enum.map/2` because they avoid converting the map to a list of tuples first.

```elixir
# ❌ Converts map to list of tuples, then filters
Enum.filter(my_map, fn {_k, v} -> v > 0 end)
|> Map.new()

# ✅ Native BIF — operates directly on the map
:maps.filter(fn _k, v -> v > 0 end, my_map)
```

---

## 9. BEAM VM Tuning

Beyond code-level optimisations, the BEAM VM itself has runtime flags and configuration options that can significantly affect throughput and latency. These go in your `vm.args` file (for releases) or as `elixir` flags.

### Match scheduler count to CPU topology
> 🔴 **Critical**

By default the BEAM spawns one scheduler thread per logical CPU core, which is correct for most workloads. In containerised environments (Docker, Kubernetes) the BEAM may see the host's core count rather than the container's CPU limit, spawning too many schedulers and causing contention. Always verify and pin explicitly in constrained environments.

```bash
# vm.args — set schedulers explicitly in containers
+S 4:4   # 4 scheduler threads, 4 online

# Or via environment at boot
ELIXIR_ERL_OPTIONS="+S 4:4" mix phx.server

# Verify at runtime
:erlang.system_info(:schedulers_online)
```

---

### Tune async thread pool for I/O-bound workloads
> 🟡 **Important**

The async thread pool handles blocking I/O operations (file, ports, native drivers). The default is very small. For I/O-heavy workloads that use ports or NIFs with blocking calls, increasing this prevents schedulers from blocking.

```bash
# vm.args
+A 64   # 64 async threads (default is 1)
```

---

### Disable scheduler compaction for latency-sensitive workloads
> 🟣 **Advanced**

By default the BEAM consolidates load onto fewer schedulers (compaction of load) to maximise throughput. For latency-sensitive workloads this can cause spikes when a loaded scheduler delays your process. Disabling compaction spreads load more evenly at the cost of slightly lower total throughput.

```bash
# vm.args — trade throughput for latency consistency
+scl false   # disable scheduler compaction of load
+sfwi 500    # wake sleeping schedulers every 500ms
```

---

### Set process priority for critical processes
> 🟣 **Advanced**

The BEAM scheduler has four priority levels: `low`, `normal`, `high`, and `max` (reserved for OTP internals). Critical processes — health checks, circuit breakers, watchdogs — can be elevated to `:high` so they are never starved by a flood of normal-priority work. Use sparingly: overusing `:high` defeats the scheduler's fairness.

```elixir
# In a GenServer or process init
Process.flag(:priority, :high)

# Or when spawning
spawn_opt(fn -> run_watchdog() end, [:link, priority: :high])
```

---

### Increase maximum process count for high-concurrency systems
> 🟡 **Important**

The default maximum process count is 262,144. Systems with many short-lived processes (web requests, per-connection handlers) can hit this limit under load. Raise it in `vm.args`. Each process costs ~2KB minimum so account for memory.

```bash
# vm.args
+P 1000000   # allow up to 1 million processes
```

---

### Use NimblePool for non-process resources
> 🟡 **Important**

Traditional process-based pools (poolboy) add an extra process hop — data must be copied to the pool worker and back. NimblePool keeps resources (sockets, ports, HTTP connections) directly in a process-free pool, eliminating one copy per checkout. Use it for TCP connections, ports, and any resource that doesn't need its own process.

```elixir
# Start a pool of HTTP/1 connections
children = [
  {NimblePool,
    worker: {MyHTTPWorker, {:https, "api.example.com", 443}},
    name: MyPool,
    pool_size: System.schedulers_online() * 2}
]

# Checkout avoids process-to-process data copy
NimblePool.checkout!(MyPool, :checkout, fn _from, conn ->
  {result, conn}
end)
```

---

### Purge logger calls at compile time in production
> 🟡 **Important**

`Logger` calls that are below the configured level are still compiled into the module and evaluated at runtime — the string interpolation and argument building happen even when the message is discarded. Setting `compile_time_purge_matching` removes them entirely from the bytecode at compile time.

```elixir
# config/prod.exs
config :logger,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

# Debug and info calls are now zero-cost in production —
# they don't exist in the compiled .beam file at all
```

---

## Quick Reference

| # | Tip | Priority |
|---|-----|----------|
| 1 | Prefer structs over plain maps whenever possible | 🔴 Critical |
| 2 | Maps over keyword lists for lookups | 🔴 Critical |
| 3 | Structs when shape is fixed, maps when it isn't | 🟢 Best Practice |
| 4 | MapSet for membership checks | 🟡 Important |
| 5 | Match access pattern to collection type | 🟢 Best Practice |
| 6 | Avoid repeated list ops in loops | 🟡 Important |
| 7 | Stream for large/infinite data | 🔴 Critical |
| 8 | Reduce over multiple Enum passes | 🟡 Important |
| 9 | Cons + reverse for list building | 🟢 Best Practice |
| 10 | Task.async_stream for parallel work | 🟣 Advanced |
| 11 | Don't use processes as caches | 🔴 Critical |
| 12 | Keep GenServer handlers fast | 🔴 Critical |
| 13 | Avoid chained synchronous calls | 🟡 Important |
| 14 | Bound process spawning | 🟡 Important |
| 15 | Prefer guards over if/case for function clause conditions | 🔴 Critical |
| 16 | Know when guards cannot replace if/case | 🟡 Important |
| 17 | Follow the branching hierarchy | 🟢 Best Practice |
| 18 | Avoid recomputing in branches | 🟡 Important |
| 19 | ETS for shared read-heavy data | 🔴 Critical |
| 20 | Set read/write_concurrency on ETS | 🟡 Important |
| 21 | :persistent_term for static data | 🟣 Advanced |
| 22 | TTL via scheduled messages | 🟢 Best Practice |
| 23 | Binary pattern matching for parsing | 🔴 Critical |
| 24 | iodata for string building | 🟡 Important |
| 25 | Avoid large inter-process messages | 🟡 Important |
| 26 | Process.hibernate for idle servers | 🟣 Advanced |
| 27 | Prefer binary pattern matching over regex | 🔴 Critical |
| 28 | Prefer hand-written functions over regex for hot paths | 🟡 Important |
| 29 | If you must use regex, compile it at compile time | 🟡 Important |
| 30 | Module attributes for constants | 🟢 Best Practice |
| 31 | Profile before optimising | 🟣 Advanced |
| 32 | Enable JIT / native compilation | 🟣 Advanced |
| 33 | Return consistent shapes from functions | 🔴 Critical |
| 34 | Narrow types at the function boundary | 🔴 Critical |
| 35 | Validate and narrow external data at the boundary | 🟡 Important |
| 36 | Prefer struct field access over Map.get | 🟡 Important |
| 37 | Keep functions small and single-purpose | 🟢 Best Practice |
| 38 | Use :maps.filter/2 and :maps.map/2 over Enum for maps | 🟡 Important |
| 39 | Match scheduler count to CPU topology | 🔴 Critical |
| 40 | Tune async thread pool for I/O-bound workloads | 🟡 Important |
| 41 | Disable scheduler compaction for latency-sensitive workloads | 🟣 Advanced |
| 42 | Set process priority for critical processes | 🟣 Advanced |
| 43 | Increase maximum process count for high-concurrency systems | 🟡 Important |
| 44 | Use NimblePool for non-process resources | 🟡 Important |
| 45 | Purge logger calls at compile time in production | 🟡 Important |