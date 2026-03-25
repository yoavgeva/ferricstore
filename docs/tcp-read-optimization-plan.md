# TCP Read Path Optimization Plan

---

## 1. dispatch_pure_command Optimization

**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex`, line 758
**Call sites:** `flush_pure_reads_fast` (line 582), `flush_pure_group_sliding_window` (line 624)

### 1.1 Current Execution Sequence (per command)

For a single `GET key` command flowing through `dispatch_pure_command`, the
following work is performed. Costs are estimated from BEAM operation profiles
(persistent_term read ~5ns, ETS lookup ~100-300ns, :counters.add ~20-40ns,
Map.get on small map ~15-30ns, String.upcase on short binary ~30-80ns).

```
Step                              Estimated cost   Notes
────────────────────────────────  ───────────────  ──────────────────────────────
1. normalise_cmd                  30-80 ns         String.upcase + tuple build
2. check_command_cached           30-80 ns*        REDUNDANT String.upcase + MapSet
3. check_keys_cached              150-400 ns*      Catalog.lookup (Map + downcase) +
                                                   command_access_type (ANOTHER upcase)
4. Stats.incr_commands            40-60 ns         persistent_term read + :counters.add
5. Dispatcher.dispatch            150-250 ns       ascii_uppercase? scan + Map.get +
                                                   System.monotonic_time x2 +
                                                   SlowLog.maybe_log (App.get_env)
6. maybe_notify_keyspace          15-30 ns         Map.get on @keyspace_events (fast)
                                                   + Config.get_value ETS lookup if hit
7. maybe_notify_tracking          ~20 ns (GET)     guard `cmd in @write_cmds` fails
                                                   fast for reads (first != match)
8. Encoder.encode                 15-60 ns         pattern match + iodata build
────────────────────────────────  ───────────────  ──────────────────────────────
TOTAL OVERHEAD (excl. actual      ~450-1040 ns     Per GET, before shard/store work
store lookup)
```

*Steps marked with `*` have redundant String.upcase calls (see below).

### 1.2 Redundant String.upcase Calls

This is the single largest low-hanging fruit. `String.upcase/1` on a UTF-8
binary is not free: it must scan every byte for lowercase ASCII (or multi-byte
codepoints), allocate a new binary, and copy. For a 3-byte command like "GET"
this is ~30-80ns per call. The current code path calls it **four times** for
the same command name:

| Call site | Function | Input already uppercase? |
|-----------|----------|--------------------------|
| `normalise_cmd/1` line 420 | `String.upcase(name)` | No (raw from parser) |
| `check_command_cached/2` line 2733 | `String.upcase(cmd)` | **YES** -- `normalise_cmd` already uppercased |
| `command_access_type/1` line 2817 | `String.upcase(cmd)` | **YES** -- same |
| `Dispatcher.dispatch/3` line 115 | `ascii_uppercase?` guard + conditional upcase | **YES** -- same |

Additionally, `Catalog.lookup/1` (called from `check_keys_cached`) does
`String.downcase(name)` on the already-uppercase command, adding a fifth
case-conversion.

**Current code in `check_command_cached`:**
```elixir
defp check_command_cached(cache, cmd) do
  cmd_up = String.upcase(cmd)            # <-- REDUNDANT: cmd is already uppercase
  cond do
    not cache.enabled -> ...
    cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd_up) -> :ok
    ...
  end
end
```

**Current code in `command_access_type`:**
```elixir
defp command_access_type(cmd) do
  cmd_up = String.upcase(cmd)            # <-- REDUNDANT
  cond do
    cmd_up in @read_write_cmds -> :rw
    cmd_up in @read_cmds -> :read        # <-- 40+ element list scan
    cmd_up in @write_cmds -> :write      # <-- 55+ element list scan
    true -> :rw
  end
end
```

**Current code in `Dispatcher.dispatch`:**
```elixir
def dispatch(name, args, store) do
  cmd = if ascii_uppercase?(name), do: name, else: String.upcase(name)
  # ^ scans every byte even though normalise_cmd already uppercased
  start = System.monotonic_time(:microsecond)
  ...
end
```

#### Proposed fix: Remove all redundant upcase calls

Since `normalise_cmd/1` is the single entry point that converts the raw parser
output to `{UPPERCASED_NAME, args}`, every downstream consumer already receives
an uppercase binary. The fix is mechanical:

1. **`check_command_cached/2`:** Remove `cmd_up = String.upcase(cmd)`, use
   `cmd` directly. The MapSet already stores uppercase keys.

2. **`command_access_type/1`:** Remove `cmd_up = String.upcase(cmd)`, use
   `cmd` directly. The `@read_write_cmds`, `@read_cmds`, `@write_cmds` lists
   are already uppercase.

3. **`Dispatcher.dispatch/3`:** Remove the `ascii_uppercase?` guard and
   conditional upcase. Trust the caller contract: name is already uppercase.
   Add a `@doc` note documenting this precondition.

4. **`Catalog.lookup/1`:** The catalog's `@commands_by_name` map is keyed by
   lowercase names. Two options:
   - (a) Change the map to be keyed by uppercase names (one-time compile cost).
   - (b) Add `Catalog.lookup_uppercase/1` that skips the downcase call.
   Option (a) is simpler.

**Estimated savings:** 3 x String.upcase (~90-240ns) + 1 x String.downcase
(~30-80ns) + 1 x ascii_uppercase? scan (~10-20ns) = **~130-340ns per command**.

### 1.3 ACL Fast Path for Default User

In a typical deployment without ACL configured, every connection authenticates
as the `"default"` user with `commands: :all` and `keys: :all`. The current
code still enters `check_command_cached` and `check_keys_cached` on every
command, performing map field access and MapSet lookups.

**Current fast paths:**
```elixir
defp check_command_cached(nil, _cmd), do: :ok              # only when ACL disabled
defp check_keys_cached(nil, _cmd, _args), do: :ok          # only when ACL disabled
defp check_keys_cached(%{keys: :all}, _cmd, _args), do: :ok  # fast for keys
```

There is no equivalent fast path for the common `commands: :all` +
`denied_commands: empty MapSet` case. The code falls into:

```elixir
cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd_up) -> :ok
```

This does an atom comparison + `MapSet.member?` (which is `Map.get` internally)
on every command.

#### Proposed fix: Precompute a `:full_access` flag

When building the ACL cache in `build_acl_cache/1`, if `commands == :all` and
`denied_commands` is empty and `keys == :all` and `enabled == true`, set
`acl_cache` to the atom `:full_access` instead of a map:

```elixir
defp build_acl_cache(username) do
  case Ferricstore.Acl.get_user(username) do
    nil -> nil
    user ->
      if user.enabled and user.commands == :all and
         Map.get(user, :denied_commands, MapSet.new()) |> MapSet.size() == 0 and
         user.keys == :all do
        :full_access
      else
        %{commands: user.commands, ...}
      end
  end
end
```

Then add clause heads:
```elixir
defp check_command_cached(:full_access, _cmd), do: :ok
defp check_keys_cached(:full_access, _cmd, _args), do: :ok
```

**Estimated savings:** ~60-150ns per command (skip MapSet.member? + map field
access + Catalog.get_keys + command_access_type). This applies to the vast
majority of connections.

### 1.4 `maybe_notify_keyspace` Reads Config on Every Matching Command

When a command IS in the `@keyspace_events` map (all writes), `do_notify_keyspace`
calls `KeyspaceNotifications.notify/2`, which does:

```elixir
def notify(key, event, flags \\ nil) do
  config_flags = flags || Ferricstore.Config.get_value("notify-keyspace-events") || ""
  if config_flags != "" and should_notify?(event, config_flags) do
    ...
  end
  :ok
end
```

`Config.get_value/1` performs an ETS lookup on every write command, even when
keyspace notifications are disabled (the common case). `should_notify?` then
does 3-4 `String.contains?` calls on the flags string.

For GET commands this is not an issue (Map.get returns nil, early exit). But
for SET/DEL/etc. this adds ~100-300ns of ETS lookup + string scanning.

#### Proposed fix: Cache the notification flags in connection state

At connection init (and on CONFIG SET), read the flags once and store them in
the connection state. Pass the flags to `maybe_notify_keyspace`:

```elixir
# In dispatch_pure_command:
maybe_notify_keyspace(name, args, result, state.keyspace_flags)

# Fast path:
defp maybe_notify_keyspace(_cmd, _args, _result, ""), do: :ok
```

**Estimated savings for writes:** ~100-300ns per write command.
**Estimated savings for reads:** 0 (already fast -- Map.get returns nil).

### 1.5 `try/catch` Block Overhead

The `try/catch` wrapping `Dispatcher.dispatch` catches `:exit` signals from
dead shard processes:

```elixir
result =
  try do
    Dispatcher.dispatch(name, args, store)
  catch
    :exit, {:noproc, _} -> {:error, "ERR server not ready..."}
    :exit, {reason, _}  -> {:error, "ERR internal error: ..."}
  end
```

On the BEAM, `try/catch` has near-zero overhead on the happy path. The JIT
compiler (BeamAsm, OTP 25+) generates native code for the try block and only
branches to the catch handler if an exception is raised. The setup cost is
essentially a stack frame marker (~2-5 machine instructions, <5ns).

However, when an exit IS caught, `inspect(reason)` triggers Inspect protocol
dispatch and potentially expensive term formatting. This is acceptable since
exits are rare.

#### Verdict: No change needed

The try/catch is already optimal for the happy path. Removing it would require
upstream callers to handle exits, spreading complexity. Keep as-is.

### 1.6 `Stats.incr_commands()` Overhead

```elixir
def incr_commands do
  :counters.add(counter_ref(), @counter_commands, 1)
  :ok
end
```

`counter_ref()` reads from `:persistent_term` (~5ns, never copied) and
`:counters.add` is a lock-free atomic increment (~20-40ns). Total: ~25-45ns.

This is already well-optimized. The only possible micro-optimization would be
to batch-increment at the end of a pipeline (one add per N commands instead of
N adds), but the savings (~20ns * N vs ~20ns * 1 + overhead) are marginal and
add complexity.

#### Verdict: No change needed

The per-command cost is already at the floor of what's achievable.

### 1.7 `maybe_notify_tracking` Guard Scan for Read Commands

```elixir
defp maybe_notify_tracking(cmd, args, _result, _state) when cmd in @write_cmds do
```

`in` with a module-attribute list compiles to chained `==` comparisons at
compile time. `@write_cmds` has ~55 elements. For a GET command, the BEAM must
compare against all 55 entries before the final catch-all clause matches.
(In practice the JIT may optimize this, but worst case is ~55 comparisons.)

The catch-all clause that short-circuits on errors is correctly ordered first:
```elixir
defp maybe_notify_tracking(_cmd, _args, {:error, _}, _state), do: :ok
```

So error results skip the scan. But successful GETs still hit the 55-element
scan. The `@keyspace_events` map (used by `maybe_notify_keyspace`) avoids this
by using `Map.get/2` which is O(1).

#### Proposed fix: Convert `@write_cmds` guard to MapSet lookup

Replace the `when cmd in @write_cmds` guard with a runtime MapSet check inside
the function body:

```elixir
@write_cmds_set MapSet.new(@write_cmds)

defp maybe_notify_tracking(_cmd, _args, {:error, _}, _state), do: :ok
defp maybe_notify_tracking(cmd, args, result, state) do
  if MapSet.member?(@write_cmds_set, cmd) do
    # ... existing write notification logic
  else
    :ok
  end
end
```

`MapSet.member?` is a `Map.get` under the hood. For maps with >32 keys (we
have ~55), BEAM uses a HAMT tree with O(log32 N) ~= O(1) lookup, so this is
effectively constant time regardless of the number of write commands.

The same optimization applies to `@read_cmds` in `command_access_type`.

**Estimated savings for reads:** ~20-50ns (eliminate 55-element linear scan).

### 1.8 Dispatcher.dispatch Internal Overhead

Inside `Dispatcher.dispatch`, two operations add per-command cost on the hot
path:

```elixir
def dispatch(name, args, store) do
  cmd = if ascii_uppercase?(name), do: name, else: String.upcase(name)  # (1)
  start = System.monotonic_time(:microsecond)                           # (2)
  result = case Map.get(@cmd_dispatch_map, cmd) do ... end
  duration = System.monotonic_time(:microsecond) - start                # (3)
  Ferricstore.SlowLog.maybe_log([cmd | args], duration)                 # (4)
  result
end
```

1. **`ascii_uppercase?`** -- redundant (section 1.2).
2. **Two `System.monotonic_time` calls** -- each is a BIF reading the OS
   monotonic clock. On macOS (mach_absolute_time) this is ~15-25ns per call.
   Two calls = ~30-50ns. This is pure slowlog overhead.
3. **`SlowLog.maybe_log`** -- calls `Application.get_env` to read the
   threshold. `Application.get_env` does an ETS lookup (~100-200ns). This
   happens on EVERY command, even when slowlog is disabled (threshold = -1).

#### Proposed fix A: Skip slowlog timing when disabled

Read the slowlog threshold once at dispatcher module load (or pass it from the
connection state). If threshold is negative (disabled), skip the monotonic_time
calls entirely:

```elixir
def dispatch(name, args, store) do
  result = case Map.get(@cmd_dispatch_map, name) do ... end

  # Only time commands if slowlog is enabled
  if SlowLog.enabled?() do
    # move timing around actual dispatch
  end

  result
end
```

Better yet, have the connection layer control this. If slowlog is disabled,
call `Dispatcher.dispatch_fast/3` that skips timing. If enabled, call
`Dispatcher.dispatch/3` with timing.

#### Proposed fix B: Cache slowlog threshold in persistent_term

Replace `Application.get_env` with a `persistent_term` read (~5ns instead of
~100-200ns):

```elixir
def threshold do
  :persistent_term.get({__MODULE__, :threshold}, @default_threshold_us)
end
```

Update the persistent_term on CONFIG SET. This is a one-line change.

**Estimated savings:** ~130-270ns per command (2x monotonic_time when disabled,
or Application.get_env replaced with persistent_term).

### 1.9 `normalise_cmd` Called Twice in Pipeline Path

In `flush_pure_group_sliding_window`, each command goes through:

1. `command_shard_key(cmd)` -- calls `normalise_cmd(cmd)` to classify the shard
2. `dispatch_pure_command(cmd, store, state)` -- calls `normalise_cmd(cmd)` again

This means `String.upcase` is called twice per command just within the
connection layer, before the redundant calls in ACL and Dispatcher.

**Current flow:**
```
command_shard_key(cmd)
  └─ normalise_cmd(cmd)          # String.upcase #1
       └─ {name, args}

dispatch_pure_command(cmd, ...)
  └─ normalise_cmd(cmd)          # String.upcase #2 (same input!)
       └─ {name, args}
```

#### Proposed fix: Normalise once, pass the result through

Normalise the command once in the pipeline entry point and pass the
`{name, args}` tuple to both `command_shard_key_normalised` and
`dispatch_pure_command_normalised`:

```elixir
defp flush_pure_group_sliding_window(commands, store, state) do
  indexed_cmds =
    commands
    |> Enum.with_index()
    |> Enum.map(fn {cmd, idx} ->
      normalised = normalise_cmd(cmd)
      {normalised, idx, command_shard_key_normalised(normalised)}
    end)

  # ... in lane tasks:
  Enum.each(lane_cmds, fn {normalised, idx, _shard_key} ->
    result = dispatch_pure_command_normalised(normalised, store, state)
    send(conn_pid, {:lane_result, idx, result})
  end)
end
```

**Estimated savings:** ~30-80ns per command (one fewer String.upcase + tuple
allocation).

### 1.10 Summary of Proposed Changes

| Change | Section | Savings/cmd | Complexity | Priority |
|--------|---------|-------------|------------|----------|
| Remove redundant String.upcase x3 + downcase x1 | 1.2 | 130-340 ns | Low | **P0** |
| Normalise once in pipeline path | 1.9 | 30-80 ns | Low | **P0** |
| ACL `:full_access` fast path | 1.3 | 60-150 ns | Low | **P1** |
| Cache slowlog threshold in persistent_term | 1.8B | 100-200 ns | Low | **P1** |
| Skip slowlog timing when disabled | 1.8A | 30-50 ns | Medium | **P2** |
| Cache keyspace notification flags | 1.4 | 100-300 ns (writes) | Medium | **P2** |
| Convert `@write_cmds` guard to MapSet | 1.7 | 20-50 ns | Low | **P2** |

**Total estimated savings for a GET (P0+P1):** ~320-770ns per command, or
roughly **30-50% reduction** in per-command overhead (estimated baseline
~450-1040ns excluding actual store work).

**Total estimated savings for a SET (all changes):** ~470-1170ns per command.

### 1.11 What NOT to Change

- **try/catch around Dispatcher.dispatch** -- near-zero happy-path cost on BEAM
  JIT; removing it would spread error handling complexity.
- **Stats.incr_commands** -- already optimal (persistent_term + :counters.add).
- **Encoder.encode** -- already returns iodata, no unnecessary binary copies.
- **`maybe_notify_keyspace` for reads** -- already O(1) via Map.get returning nil.

### 1.12 Validation Plan

After implementing changes, measure with:

```elixir
# In-process micro-benchmark (run in iex):
:timer.tc(fn ->
  Enum.each(1..100_000, fn _ ->
    dispatch_pure_command(["GET", "key"], store, state)
  end)
end)
```

Also run the existing `bench/` benchmarks (Router write throughput, sync vs
async) to detect regressions, and the full test suite to verify correctness.

---

## 2. build_store Closure Overhead

### Current state

`build_raw_store/0` (connection.ex:1929) constructs a 29-key map on every invocation.
The map mixes two kinds of entries:

| Kind | Count | Examples |
|------|-------|---------|
| Function captures (`&Router.fun/N`) | 21 | `get: &Router.get/1`, `put: &Router.put/3`, `incr: &Router.incr/2` |
| Anonymous closures (`fn ... end`) | 8 | `flush`, `compound_get`, `compound_get_meta`, `compound_put`, `compound_delete`, `compound_scan`, `compound_count`, `compound_delete_prefix` |

`build_store/1` wraps `build_raw_store/0`. For sandbox connections (`ns != nil`) it
additionally replaces 5 entries (`get`, `get_meta`, `put`, `delete`, `exists?`) with new
closures that prepend the namespace prefix, bringing the closure count to 13.

### Call frequency

`build_store` is called from **10 sites** in connection.ex:

| Call site | When | Frequency |
|-----------|------|-----------|
| `flush_pure_group/2` (line 502) | Every pipeline batch (multi-command) | Hot -- every pipelined batch |
| `dispatch_normal_l2/3` (line 1697) | Every single non-pipelined command (L2 path) | Hot -- every non-L1-hit command |
| `dispatch("XREAD", ...)` (line 1727) | XREAD commands | Moderate |
| `dispatch("CLIENT", ...)` (line 792) | CLIENT subcommands | Rare |
| `dispatch(cmd, args, :queuing)` (line 1264) | Commands received during MULTI queuing | Moderate |
| `dispatch_blocking/3` (line 1304) | BLPOP/BRPOP | Moderate |
| `dispatch_blmove/2` (line 1391) | BLMOVE | Rare |
| `dispatch_blmpop/2` (line 1468) | BLMPOP | Rare |
| `dispatch_normal_l2` fallback (line 1697) | Same as above | See above |
| `execute_queued_cmd` (line 1830) | Each cmd inside EXEC | Moderate per txn |

The two hot paths (`flush_pure_group` for pipelines, `dispatch_normal_l2` for single
commands) mean `build_store` is called **at least once per command batch or per individual
command**. Under a 100K cmd/s workload this is 100K map allocations per second, each
producing a 29-key map with 8 anonymous closures.

### Cost breakdown

Each call to `build_raw_store/0` performs:

1. **Map allocation**: A 29-key flat-map (BEAM switches to hash-map at 32 keys). This
   allocates ~60 words on the process heap (key-value pairs + map header). At 8 bytes per
   word on 64-bit, that is ~480 bytes per call.

2. **Function capture creation**: The 21 `&Module.fun/arity` captures are cheap -- the
   BEAM represents these as external fun references (a small fixed-size tuple pointing to
   the module/function atom + arity). They do not close over any variables and the
   representation is compact. However, each capture still allocates a small fun object on
   the heap.

3. **Anonymous closure creation**: The 8 `fn ... end` closures (flush, compound_*)
   allocate fun objects that carry environment pointers. The compound_* closures are
   stateless (they reference only module-level functions like `Router.shard_name/1`,
   `Router.shard_for/1`, `GenServer.call/2`) so they don't capture connection state, but
   the BEAM still allocates a fun object per closure.

4. **GC pressure**: All 29 fun references + the map become garbage immediately after the
   command completes. The connection process's generational GC must trace and collect
   these. Under sustained pipelining, the minor-heap fills faster, triggering more
   frequent GC cycles.

Estimated per-call cost: **~500 ns** for the map + closure allocation (based on BEAM
microbenchmarks of map construction at this size). Under 100K cmd/s this is ~50 ms/s of
pure allocation overhead -- not dominant but unnecessary.

### Key insight: the raw store is identical across all non-sandbox connections

For `sandbox_namespace == nil` (the production case), every call to `build_store(nil)`
returns a structurally identical map. The function captures point to the same
module/function atoms every time. The anonymous closures (compound_*) capture no
per-connection or per-request state. The `flush` closure reads `Application.get_env`
which is a runtime lookup but does not depend on connection state.

This means the raw store map is a **pure constant** for non-sandbox connections.

### Optimization options

#### Option A: Cache the store map on the connection state (per-connection, one-time build)

Store `build_raw_store()` result in `state.store` during connection init. Use it for all
subsequent calls where `sandbox_namespace == nil`.

```
# In init:
state = %{..., store: build_raw_store(), ...}

# In flush_pure_group / dispatch_normal_l2 / etc:
store = if state.sandbox_namespace, do: build_store(state.sandbox_namespace), else: state.store
```

**Savings**: Eliminates 100% of map+closure allocation on the hot path for non-sandbox
connections. The single cached map lives on the process heap until connection close.

**Risk**: Minimal. The map captures only module-level function references. If the Router
module is hot-code-reloaded, the `&Router.fun/N` captures will still resolve to the
current version because external fun references use late binding (they look up the
module+function at call time, not capture time). This is safe.

**Sandbox connections**: Still need per-call `build_store(ns)` since the namespace could
change (via `FERRICSTORE.CONFIG NAMESPACE`). Could also cache on state and invalidate on
namespace change.

**Estimated savings**: ~500 ns per command batch. At 100K cmd/s = ~50 ms/s reclaimed.

#### Option B: Module attribute / persistent_term for the raw store (global singleton)

Store the raw store map in `:persistent_term` at application boot:

```
# In application.ex start/2:
:persistent_term.put(:ferricstore_raw_store, FerricstoreServer.Connection.build_raw_store())

# In connection.ex:
defp get_raw_store, do: :persistent_term.get(:ferricstore_raw_store)
```

**Savings**: Same as Option A but the map is shared across ALL connection processes
without per-process heap cost. `persistent_term.get/1` is O(1) with no locks and no
copying -- the term is stored in the literal area and referenced directly.

**Risk**: `persistent_term` update triggers a global GC scan of all processes. This is
fine because the store map is set once at boot and never updated (or updated only on
config reload, which is rare). The Erlang docs explicitly recommend this pattern for
"terms that are frequently accessed but never or infrequently updated."

**Caveat**: The `flush` closure calls `Application.get_env(:ferricstore, :shard_count, 4)`
which reads a value that could change at runtime. This is already a runtime call so
storing the closure in persistent_term doesn't change its behavior -- it will still read
the current config value when invoked.

**Estimated savings**: Same CPU savings as Option A, plus eliminates ~480 bytes of
per-connection heap usage for the cached map.

#### Option C: Eliminate the store map entirely -- Dispatcher calls Router directly

The store map exists as an indirection layer so that:
1. Command handlers are testable with mock stores
2. Sandbox namespacing can prefix keys transparently

If Dispatcher called `Router.get(key)` directly instead of `store.get.(key)`, every
command dispatch would save one indirect function call (fun invocation is ~3x slower than
a direct module call per the Erlang efficiency guide).

**However**, this would:
- Break the sandbox namespace feature (key prefixing)
- Remove the ability to inject mock stores in tests (the `build_noop_store` pattern on
  line 1864 and test mocks would stop working)
- Couple all 20+ command handler modules directly to Router

**Verdict**: Not recommended. The indirection serves real architectural purposes.
The overhead of calling through a fun (~few ns per call) is negligible compared to the
actual GenServer.call to the shard process (~1-10 us).

#### Option D: Hybrid -- persistent_term for raw store, on-the-fly wrapping for sandbox

Combine Option B with a lightweight sandbox wrapper:

```
defp build_store(nil), do: :persistent_term.get(:ferricstore_raw_store)

defp build_store(ns) when is_binary(ns) do
  raw = :persistent_term.get(:ferricstore_raw_store)
  %{raw |
    get: fn key -> raw.get.(ns <> key) end,
    get_meta: fn key -> raw.get_meta.(ns <> key) end,
    put: fn key, val, exp -> raw.put.(ns <> key, val, exp) end,
    delete: fn key -> raw.delete.(ns <> key) end,
    exists?: fn key -> raw.exists?.(ns <> key) end
  }
end
```

Non-sandbox connections (production): zero allocation, persistent_term lookup.
Sandbox connections (testing): small 5-closure allocation for namespace wrapping only.

### Recommendation

**Option D (persistent_term + sandbox wrapper)** is the best tradeoff:

- Zero allocation on the hot path for production workloads
- Maintains full testability and sandbox support
- persistent_term.get is lock-free O(1), cheaper than even reading from process state
- Compound closures are eliminated from per-call allocation entirely
- The only ongoing cost is the persistent_term hash lookup (~10-20 ns)

**Implementation steps**:
1. Extract `build_raw_store/0` as a public function
2. Call it once in `application.ex` start callback, store via `:persistent_term.put/2`
3. Replace `build_store(nil)` with `:persistent_term.get(:ferricstore_raw_store)`
4. Keep `build_store(ns)` for sandbox but source the raw map from persistent_term
5. Add a test that verifies the persistent_term store matches a freshly built one

**Estimated total savings**: ~500 ns per command (map allocation) + ~50-100 ns per store
function invocation (fun call overhead eliminated for reads from persistent_term literal
area). Under sustained 100K cmd/s pipelined load, this reclaims ~50-60 ms/s of CPU and
significantly reduces minor-GC frequency on connection processes.

## 3. Stats and Notifications Overhead

### 3.1 `Stats.incr_commands()` — per-call cost analysis

**Current code** (`apps/ferricstore/lib/ferricstore/stats.ex:132-135`):

```elixir
def incr_commands do
  :counters.add(counter_ref(), @counter_commands, 1)
  :ok
end
```

This does two operations:

1. **`counter_ref()`** — calls `:persistent_term.get({Ferricstore.Stats, :counter_ref})`. This is a
   hash-table lookup into the global persistent_term store. Because the value is a reference
   (an immediate-like term), it is returned by direct pointer dereference with no heap copy.
   Cost: **~5-10ns**.

2. **`:counters.add(ref, 2, 1)`** — performs an atomic increment on the counter array. The counter
   was created with `[:atomics]` option (not `[:write_concurrency]`), so this is a single `cmpxchg`
   on a cache line. Under low contention this is ~5-15ns, but under high concurrency (many
   connections incrementing the same slot) the cache line bounces between CPU cores.
   Cost: **~10-50ns** depending on contention.

**Total per-call cost: ~15-60ns**

This is already very cheap. The `[:atomics]` option means sequential consistency — every `add` is
a full atomic CAS. Under high load with many cores, the cache-line contention on a single counter
slot could push latency toward 50ns+. However, since this is a global counter (not per-key), there
is no way to avoid it without losing the ability to report `total_commands_processed`.

**Verdict: Leave as-is.** The cost is negligible (<0.1us). Switching to `[:write_concurrency]` would
reduce contention by using per-scheduler counter segments at the expense of read accuracy, but
`total_commands_processed` is already approximate. This is a low-priority optimization — switching to
`write_concurrency` might save 10-20ns under extreme contention but would not be measurable in
throughput benchmarks.

### 3.2 `maybe_notify_keyspace(cmd, args, result)` — disabled path

**Current code** (`connection.ex:2520-2525`):

```elixir
defp maybe_notify_keyspace(cmd, args, result) do
  case Map.get(@keyspace_events, cmd) do
    nil -> :ok
    event -> do_notify_keyspace(cmd, event, args, result)
  end
end
```

For a **GET** command, `cmd` is `"GET"`. The `@keyspace_events` map does not contain `"GET"` (it only
has write commands: SET, DEL, INCR, etc.). So this is a `Map.get` on a compile-time constant map
(~30 keys) that returns `nil`, followed by pattern match to `:ok`.

**The fast path never reaches `KeyspaceNotifications.notify/3` at all for reads.** The `Map.get` on a
compile-time constant small map is essentially free — the BEAM compiler optimizes this into a direct
hash lookup with no allocation. Cost: **~5-10ns**.

However, for **write commands** (SET, DEL, etc.), the path is:
1. `Map.get(@keyspace_events, "SET")` -> `"set"` (~5ns)
2. `do_notify_keyspace("SET", "set", [key], result)` -> `KeyspaceNotifications.notify(key, "set")`
3. Inside `notify/3`: calls `Ferricstore.Config.get_value("notify-keyspace-events")` — an ETS lookup
   on `:ferricstore_config` (~50-200ns)
4. When config is `""` (the default): the condition `config_flags != ""` fails, and we return `:ok`.

So for writes with notifications **disabled** (the default), there is an unnecessary ETS lookup per
write command. But for reads, there is zero overhead beyond a cheap Map.get nil check.

**Verdict for reads (GET): No optimization needed.** The `Map.get` on a constant map returning nil is
essentially a no-op.

**Optimization opportunity for writes:** The `Config.get_value("notify-keyspace-events")` ETS lookup
inside `KeyspaceNotifications.notify/3` could be hoisted into a `persistent_term` boolean flag. When
`CONFIG SET notify-keyspace-events ""` is called (or at startup), store `false` in
`:persistent_term.put(:ferricstore_keyspace_notify_enabled, false)`. Check the persistent_term flag
(~5ns) instead of the ETS lookup (~50-200ns). This saves ~50-195ns per write when notifications are
disabled.

### 3.3 `maybe_notify_tracking(cmd, args, result, state)` — no-clients path

**Current code** (`connection.ex:2614-2665`):

```elixir
defp maybe_notify_tracking(_cmd, _args, {:error, _}, _state), do: :ok
defp maybe_notify_tracking(cmd, args, _result, _state) when cmd in @write_cmds do
  # ... ETS lookups, Enum.each, etc.
end
defp maybe_notify_tracking(_cmd, _args, _result, _state), do: :ok
```

For a **GET** command: `cmd` is `"GET"`. The first clause doesn't match (result is not an error). The
second clause checks `cmd in @write_cmds` — since `"GET"` is not in `@write_cmds`, this guard fails.
The third clause matches with `:ok`.

**Cost for GET: ~0ns.** The `when cmd in @write_cmds` guard on a module-attribute list is compiled by
the BEAM into a literal set membership check. When `"GET"` is not in the set, the clause is skipped
immediately and the catch-all clause fires. There is no runtime list traversal.

For **write commands** (SET, DEL, etc.), even when NO clients have tracking enabled, the code
unconditionally:
1. Calls `tracking_socket_sender()` — allocates a 3-arity closure (~50ns)
2. Calls `ClientTracking.notify_key_modified(key, writer_pid, sender)` which:
   - Calls `encode_invalidation([key])` — builds RESP3 iodata (~100-200ns)
   - Calls `notify_default_mode` — does `:ets.lookup(@tracking_table, key)` (~50-200ns)
   - Calls `notify_bcast_mode` — does `:ets.select(@connections_table, match_spec)` (~100-500ns)
   - Calls `:ets.delete(@tracking_table, key)` (~50-100ns)

**Total per-write cost with zero tracking clients: ~350-1000ns.** This is significant — every
SET/DEL/INCR pays ~0.5-1us for client tracking lookups and invalidation encoding even when no client
has ever enabled tracking.

**Optimization:** Add a global `persistent_term` fast-exit flag:

```elixir
:persistent_term.put(:ferricstore_tracking_active, false)
```

Set it to `true` when the first `CLIENT TRACKING ON` is received, set it back to `false` when the
last tracking client disconnects. The `maybe_notify_tracking` for writes would check this flag first
(~5ns) and bail out immediately. This saves ~350-1000ns per write operation when no clients use
tracking. The `ClientTracking.enable/3` and `ClientTracking.cleanup/1` functions already know the
tracking state, so maintaining this flag is straightforward.

Similarly, `maybe_track_read` for reads already has efficient guards:
```elixir
defp maybe_track_read(_cmd, _args, _result, %{tracking: %{enabled: false}} = state), do: state
defp maybe_track_read(_cmd, _args, _result, %{tracking: nil} = state), do: state
```
These are zero-cost pattern matches on the connection struct — no ETS lookups. The read path for
tracking is already optimal.

### 3.4 Summary: per-GET overhead from Stats + Notifications

For a single **GET** command with default configuration (keyspace notifications disabled, no tracking
clients):

| Operation | Cost | Notes |
|---|---|---|
| `Stats.incr_commands()` | ~15-60ns | persistent_term get + atomic add |
| `maybe_notify_keyspace("GET", ...)` | ~5-10ns | Map.get on constant map -> nil |
| `maybe_track_read("GET", ...)` | ~0ns | Pattern match on `tracking: nil` |
| `maybe_notify_tracking("GET", ...)` | ~0ns | Guard `cmd in @write_cmds` fails |
| **Total** | **~20-70ns** | |

**The Stats + notifications overhead on the GET path is negligible (~20-70ns).** There is no
low-hanging fruit here for reads.

For writes (SET), the picture is different:

| Operation | Cost | Notes |
|---|---|---|
| `Stats.incr_commands()` | ~15-60ns | Same as reads |
| `maybe_notify_keyspace("SET", ...)` | ~55-210ns | Map.get hit + Config ETS lookup (returns "") |
| `maybe_notify_tracking("SET", ...)` | ~350-1000ns | Closure alloc + RESP encode + 2 ETS lookups + ETS delete |
| **Total** | **~420-1270ns** | |

### 3.5 Proposed optimizations (writes only)

1. **`persistent_term` flag for keyspace notifications** — Store a boolean
   `:ferricstore_keyspace_notify_enabled` in persistent_term. Update it in
   `Config.set("notify-keyspace-events", ...)`. Check it in `maybe_notify_keyspace` before calling
   `KeyspaceNotifications.notify`. Saves ~50-200ns per write when disabled.

2. **`persistent_term` flag for client tracking** — Store `:ferricstore_tracking_active` boolean.
   Set `true` on first `CLIENT TRACKING ON`, set `false` when last tracking client disconnects (in
   `ClientTracking.cleanup/1` when `:ets.info(@connections_table, :size) == 0`). Check it at the top
   of `maybe_notify_tracking` for write commands. Saves ~350-1000ns per write when no clients use
   tracking.

3. **Avoid closure allocation in `tracking_socket_sender/0`** — The function allocates a new
   3-arity closure on every call. Hoist it to a module attribute or persistent_term. Saves ~50ns
   per write.

4. **Avoid `encode_invalidation` before checking if anyone is listening** — Currently,
   `notify_key_modified` encodes the RESP3 invalidation message before checking ETS. Reorder: check
   ETS first, only encode if there are listeners. Saves ~100-200ns per write when no clients track.

**Combined estimated savings per SET with no tracking clients: ~550-1400ns.**

---

## 4. Enum and List Operations in Fast Path

### 4.1 Where are Enum calls in the GET path?

Counting all `Enum.*` call sites in `connection.ex`: **44 distinct call sites**. However, the vast
majority are in cold paths (SUBSCRIBE, BLPOP, ACL DELUSER, XREAD BLOCK, transaction execution,
etc.). The question is which ones execute on every single-command GET.

#### Single-command GET trace (non-pipeline, L1 miss, non-sendfile):

```
loop/1
  -> handle_data/2
    -> Parser.parse(buffer)
    -> handle_parsed/2
      -> length(commands)               -- O(n), n=1 for single cmd
      -> pipeline_dispatch([single_cmd], state)
        -> handle_command(cmd, state)
          -> String.upcase(name)
          -> requires_auth?(state)      -- Config.get_value ETS lookup
          -> check_command_cached(cache, cmd)  -- pure map/MapSet ops
          -> check_keys_cached(cache, cmd, args) -- may call Enum.all? for ACL key check
          -> Stats.incr_commands()
          -> dispatch("GET", [key], state)
            -> in_pubsub_mode?(state)   -- nil check, O(1)
            -> try_sendfile_get(key, state)  -- Router.get_file_ref
            -> dispatch_normal("GET", [key], state)
              -> l1_try_read("GET", [key], state) -- with chain, cmd in @l1_read_cmds
              -> dispatch_normal_l2("GET", [key], state)
                -> build_store(nil)     -- persistent_term get
                -> Dispatcher.dispatch("GET", [key], store)
                -> maybe_notify_keyspace("GET", [key], result) -- Map.get nil
                -> maybe_track_read("GET", [key], result, state) -- pattern match nil
                -> maybe_notify_tracking("GET", [key], result, state) -- guard fail
                -> l1_post_dispatch("GET", [key], result, state, store)
                  -> l1_maybe_warm(state, key, value, store) -- may do ETS get_meta
                -> Encoder.encode(result)
```

**Zero `Enum.*` calls execute on the single-command GET fast path.** Every step is either a direct
pattern match, a Map/MapSet operation, or a function call with no list iteration.

The one exception is `check_keys_cached` when ACL key patterns are configured (not `:all`). In that
case it calls `Catalog.get_keys(cmd, args)` and then `check_all_keys` which calls `Enum.all?` on
`types_to_check`. But `types_to_check` for a GET (`:read`) is `[:read]` — a one-element list. So
even with ACL, this `Enum.all?` iterates exactly once.

### 4.2 Pipeline GET path (multiple commands)

When a pipeline of N commands arrives, the Enum calls depend on whether the pipeline enters the
sliding window or fast-read path.

#### Fast-read path (`flush_pure_reads_fast`, all commands are reads):

```
handle_parsed/2
  -> length(commands)                           -- O(n)
  -> pipeline_dispatch(commands, state)
    -> sliding_window_dispatch(commands, state)
      -> do_sliding_window(commands, [], state)
        (for each cmd: normalise_cmd, stateful_command_normalised?, accumulate)
        -> flush_pure_group(Enum.reverse(pure_acc), state) -- Enum.reverse: O(n)
          -> all_pure_reads?(commands)
            -> Enum.all?(commands, ...)                    -- Enum.all: O(n)
          -> flush_pure_reads_fast(commands, store, state)
            -> Enum.reduce_while(commands, ...)            -- Enum.reduce_while: O(n)
```

That is **3 Enum operations** on the pipeline path for pure reads: `Enum.reverse`, `Enum.all?`,
`Enum.reduce_while`. Each is O(n) in the number of commands. For a single-command pipeline (n=1),
these reduce to trivial operations.

#### Sliding-window path (mixed commands or writes in pipeline):

```
flush_pure_group_sliding_window(commands, store, state)
  -> Enum.with_index(commands)                  -- O(n)
  -> Enum.map(indexed, ...)                     -- O(n)
  -> Enum.group_by(indexed_cmds, ...)           -- O(n)
  -> Enum.map(lanes, ...)                       -- O(lanes)
     -> Enum.each(lane_cmds, ...)               -- O(n) total across lanes
  -> sliding_window_collect(state, 0, total, %{}) -- recursive, no Enum
  -> Enum.each(lane_tasks, ...)                 -- O(lanes)
```

That is **6 Enum operations** for the sliding window: `with_index`, `map`, `group_by`, `map`
(lanes), `each` (lane cmds), `each` (task cleanup). Combined: ~3N + 2L iterations where L = number
of distinct shard lanes.

### 4.3 The "188 Enum calls per GET" claim

A single-command GET has **zero** Enum calls in the connection handler. The 188 figure likely comes
from one of:

1. **Profiling a pipeline** — if the benchmark sent commands in pipeline batches, the sliding
   window path's Enum calls would dominate. A 10-command pipeline would have ~30 Enum iterations
   in the sliding window path alone.

2. **Profiling deeper in the stack** — the `Dispatcher.dispatch`, `Router.get`, `Shard` GenServer,
   or `Parser.parse` modules may contain their own Enum calls. The connection handler itself is
   clean on the single-GET path.

3. **L1 eviction** — `l1_evict_and_put` uses `Enum.min_by(state.l1_cache, ...)` to find the LRU
   entry, which iterates over all L1 entries (up to 64). This only fires when L1 is full and a
   new entry needs to be cached.

4. **L1 invalidation from tracking pushes** — `l1_invalidate_keys` uses
   `Enum.reduce(keys, state, ...)`. This runs when a `{:tracking_invalidation, iodata, keys}`
   message is received, not during the GET dispatch itself.

### 4.4 Enum operations that DO run on every pipelined GET

For a pipeline of N pure-read commands (the common fast path):

| Enum call | Location | Iterations | Can eliminate? |
|---|---|---|---|
| `Enum.reverse(pure_acc)` | `do_sliding_window/3` L477 | O(n) | Yes — accumulate in order instead of prepending+reversing |
| `Enum.all?(commands, ...)` | `all_pure_reads?/1` L572 | O(n) | Yes — check during accumulation in do_sliding_window |
| `Enum.reduce_while(commands, ...)` | `flush_pure_reads_fast/3` L581 | O(n) | No — this IS the dispatch loop, but could be direct recursion |

For a pipeline of N mixed commands hitting the sliding window:

| Enum call | Location | Iterations | Can eliminate? |
|---|---|---|---|
| `Enum.reverse(pure_acc)` | `do_sliding_window/3` L477,494,517 | O(n) per flush | Merge with next step |
| `Enum.with_index` | `flush_pure_group_sliding_window/3` L609 | O(n) | Fuse with next map |
| `Enum.map` (index+shard_key) | L610 | O(n) | Fuse with with_index |
| `Enum.group_by` | L613 | O(n) | Single-pass fold |
| `Enum.map` (spawn lanes) | L621 | O(lanes) | Keep (spawns tasks) |
| `Enum.each` (lane cmds) | L623 | O(n) total | Keep (inside tasks) |
| `Enum.each` (await tasks) | L637 | O(lanes) | Keep (cleanup) |

### 4.5 Proposed optimizations

1. **Fuse `Enum.reverse` + `all_pure_reads?` into `do_sliding_window` accumulation.** During command
   accumulation, track a boolean flag `all_reads?` alongside `pure_acc`. When flushing, the reversed
   list and the all-reads check are already computed. Eliminates one O(n) pass.

2. **Fuse `Enum.with_index` + `Enum.map` into a single pass.** Replace:
   ```elixir
   commands
   |> Enum.with_index()
   |> Enum.map(fn {cmd, idx} -> {cmd, idx, command_shard_key(cmd)} end)
   ```
   with a single recursive function or `Enum.map` with an accumulator. Saves one O(n) list
   traversal + one intermediate list allocation.

3. **Replace `Enum.reduce_while` in `flush_pure_reads_fast` with direct recursion.** The
   `Enum.reduce_while` has overhead from the higher-order function call and the `{:cont, ...}` /
   `{:halt, ...}` tuple wrapping on each iteration. Direct recursion eliminates this. Estimated
   savings: ~20-50ns per command in pipeline.

4. **Pre-compute `all_pure_reads?` during normalization.** The `do_sliding_window` already calls
   `normalise_cmd` for each command. It could simultaneously check if `name in @read_cmds` and set
   a flag. When the pure group is flushed, the flag determines whether to use the fast-read path
   without a second pass.

**Estimated combined savings for a 10-command pure-read pipeline: ~200-500ns** (eliminating 2-3
redundant O(n) passes over the command list).

For single-command GET: **zero savings** — there are no Enum calls to eliminate.
