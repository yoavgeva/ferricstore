# Elixir Performance Guide Audit

Cross-reference of the FerricStore Elixir codebase against the Elixir Performance Guide (`elixir_performance_guide.md`). Every source file was read in full and checked against every guideline.

---

## 1. Guide Summary

| # | Rule | Priority |
|---|------|----------|
| 1 | Structs over plain maps when keys are known | Critical |
| 2 | Maps over keyword lists for lookups | Critical |
| 3 | Structs when shape is fixed, maps when it isn't | Best Practice |
| 4 | MapSet for membership checks | Important |
| 5 | Match collection type to access pattern | Best Practice |
| 6 | Avoid repeated list ops in loops | Important |
| 7 | Stream for large/infinite data | Critical |
| 8 | Reduce over multiple Enum passes | Important |
| 9 | Cons + reverse for list building | Best Practice |
| 10 | Task.async_stream for parallel work | Advanced |
| 11 | Don't use processes as caches | Critical |
| 12 | Keep GenServer handlers fast | Critical |
| 13 | Avoid chained synchronous calls | Important |
| 14 | Bound process spawning | Important |
| 15 | Guards over if/case for function clauses | Critical |
| 16 | Know when guards can't replace if/case | Important |
| 17 | Follow branching hierarchy | Best Practice |
| 18 | Avoid recomputing in branches | Important |
| 19 | ETS for shared read-heavy data | Critical |
| 20 | Set read/write_concurrency on ETS | Important |
| 21 | :persistent_term for static data | Advanced |
| 22 | TTL via scheduled messages | Best Practice |
| 23 | Binary pattern matching for parsing | Critical |
| 24 | iodata for string building | Important |
| 25 | Avoid large inter-process messages | Important |
| 26 | Process.hibernate for idle servers | Advanced |
| 27 | Binary pattern matching over regex | Critical |
| 28 | Hand-written functions over regex on hot paths | Important |
| 29 | Compile regex at compile time if used | Important |
| 30 | Module attributes for constants | Best Practice |
| 31 | Profile before optimizing | Advanced |
| 32 | Enable JIT / native compilation | Advanced |
| 33 | Return consistent shapes from functions | Critical |
| 34 | Narrow types at the function boundary | Critical |
| 35 | Validate and narrow external data at the boundary | Important |
| 36 | Prefer struct field access over Map.get | Important |
| 37 | Keep functions small and single-purpose | Best Practice |
| 38 | Use :maps.filter/2 and :maps.map/2 over Enum for maps | Important |
| 39 | Match scheduler count to CPU topology | Critical |
| 40 | Tune async thread pool for I/O-bound workloads | Important |
| 41 | Disable scheduler compaction for latency-sensitive workloads | Advanced |
| 42 | Set process priority for critical processes | Advanced |
| 43 | Increase maximum process count for high-concurrency systems | Important |
| 44 | Use NimblePool for non-process resources | Important |
| 45 | Purge logger calls at compile time in production | Important |

---

## 2. CRITICAL Issues

### C1. `FerricStore.glob_to_regex/1` compiles regex at runtime on every call

**Rule violated:** Rule 27 (binary pattern match over regex), Rule 28 (hand-written functions over regex on hot paths), Rule 29 (compile regex at compile time)

**Location:** `apps/ferricstore/lib/ferricstore.ex:3983-3994`

**Current code:**
```elixir
defp glob_to_regex(pattern) do
  regex_str =
    pattern
    |> String.graphemes()
    |> Enum.map_join(&escape_glob_char/1)

  Regex.compile!("^#{regex_str}$")
end
```

**Why it matters:** This function is called by `FerricStore.keys/1` (the Elixir embedded API). `String.graphemes/1` creates a list of UTF-8 grapheme clusters, `Enum.map_join/2` creates an intermediate list, and `Regex.compile!/1` spins up the PCRE engine. While the standalone TCP path (server.ex, generic.ex, hash.ex, set.ex) was fixed to use `GlobMatcher`, this embedded API copy was missed.

**Recommended fix:** Replace with `Ferricstore.GlobMatcher.match?/2` which already exists:
```elixir
# In keys/1, replace:
#   regex = glob_to_regex(pattern)
#   Enum.filter(filtered, &Regex.match?(regex, &1))
# With:
Enum.filter(filtered, &Ferricstore.GlobMatcher.match?(&1, pattern))
```

**Status:** Partially fixed. The 5 command handler copies were replaced with `GlobMatcher`, but the `FerricStore` module's own `glob_to_regex/1` remains at line 3983.

---

### C2. `FerricStore.keys/1` multiple traversals on every call

**Rule violated:** Rule 8 (reduce over multiple passes), Rule 6 (avoid repeated list ops)

**Location:** `apps/ferricstore/lib/ferricstore.ex:1260-1287`

**Current code:**
```elixir
def keys(pattern \\ "*") do
  namespace = Process.get(:ferricstore_sandbox)
  all_keys = Router.keys()        # pass 1: collect all keys

  filtered =
    if namespace do
      prefix_len = byte_size(namespace)
      all_keys
      |> Enum.filter(...)          # pass 2: filter by namespace
      |> Enum.map(...)             # pass 3: strip prefix
    else
      all_keys
    end

  filtered = Enum.reject(filtered, &CompoundKey.internal_key?/1)  # pass 4

  matched =
    if pattern == "*" do
      filtered
    else
      regex = glob_to_regex(pattern)
      Enum.filter(filtered, &Regex.match?(regex, &1))  # pass 5
    end
  ...
end
```

**Why it matters:** In sandbox mode with a pattern, this traverses the full key list 5 times. `FerricStore.dbsize/0` calls `keys/0` then `length/1` for a 6th traversal. These are all cold-path (admin) operations, but for large keystores (100k+ keys) the overhead compounds.

**Recommended fix:** Combine passes into a single `Enum.reduce`:
```elixir
Enum.reduce(all_keys, [], fn key, acc ->
  key = maybe_strip_ns(key, namespace, prefix_len)
  if key && not CompoundKey.internal_key?(key) && matches_pattern?(key, pattern) do
    [key | acc]
  else
    acc
  end
end)
```

---

### C3. Store map is a plain map with ~25 function closures, rebuilt per sandbox connection

**Rule violated:** Rule 1 (structs over plain maps when keys are known), Rule 36 (prefer struct field access over Map.get)

**Location:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex:1966-2037` (build_raw_store, build_store)

**Current code:**
```elixir
defp build_raw_store do
  %{
    get: &Router.get/1,
    get_meta: &Router.get_meta/1,
    put: &Router.put/3,
    ...
  }
end

defp build_store(ns) when is_binary(ns) do
  raw = raw_store()
  %{raw |
    get: fn key -> raw.get.(ns <> key) end,
    ...
  }
end
```

The store is a plain map with ~25 known keys that is passed to every command dispatch. Every field access (`store.get.()`, `store.put.()`) goes through `Map.get/2` which returns `any()` to the compiler's type inferrer, preventing downstream optimization.

**Why it matters:** The non-sandbox path correctly caches the raw store in `persistent_term` (good). But the sandbox path rebuilds 5+ closures per dispatch. More importantly, every `store.get.(key)` call in every command handler is opaque to the OTP 26+ type inferrer (Rule 36). Since every command handler uses the store, this affects the entire hot path.

**Recommended fix:** Define a `defstruct` for the store with typed fields. This gives the compiler struct field access (transparent types) and eliminates the per-access Map.get overhead:
```elixir
defmodule Ferricstore.Store.StoreMap do
  @enforce_keys [:get, :put, :delete, :exists?]
  defstruct [...]
end
```

**Impact:** This is an architectural issue. The store map pattern is deeply embedded; changing it requires touching every command handler. Severity is CRITICAL for the type inference impact but LOW for practical urgency since the closures themselves are fast.

---

## 3. HIGH Issues

### H1. `FerricStore.encode_hash/1` calls `Enum.sort/1` + `Enum.map/2` + `IO.iodata_to_binary/1` -- three passes

**Rule violated:** Rule 8 (reduce over multiple passes)

**Location:** `apps/ferricstore/lib/ferricstore.ex:3729-3738`

**Current code:**
```elixir
def encode_hash(map) when is_map(map) do
  map
  |> Enum.sort()               # pass 1: convert to sorted list
  |> Enum.map(fn {k, v} ->     # pass 2: encode each pair
    k_bin = to_string(k)
    v_bin = to_string(v)
    <<byte_size(k_bin)::32, k_bin::binary, byte_size(v_bin)::32, v_bin::binary>>
  end)
  |> IO.iodata_to_binary()     # pass 3: flatten
end
```

**Why it matters:** `encode_hash` is called on every HSET/HDEL/HINCRBY/etc via the embedded Elixir API (`FerricStore.hset/2`). For hashes with many fields, this creates two intermediate lists. The sort is necessary for deterministic encoding, but the map + flatten can be combined.

**Recommended fix:** Use `:lists.sort/1` + a single fold that builds iodata directly:
```elixir
def encode_hash(map) when is_map(map) do
  map
  |> Enum.sort()
  |> Enum.reduce([], fn {k, v}, acc ->
    k_bin = to_string(k)
    v_bin = to_string(v)
    [acc, <<byte_size(k_bin)::32>>, k_bin, <<byte_size(v_bin)::32>>, v_bin]
  end)
  |> IO.iodata_to_binary()
end
```

---

### H2. `paginate/3` in Hash and Set calls `length(items)` then `Enum.slice`

**Rule violated:** Rule 6 (avoid repeated list ops in loops)

**Files:**
- `apps/ferricstore/lib/ferricstore/commands/hash.ex:876-892`
- `apps/ferricstore/lib/ferricstore/commands/set.ex:429-445`

**Current code:**
```elixir
defp paginate(items, cursor, count) do
  total = length(items)          # O(n)
  if cursor >= total do
    {"0", []}
  else
    batch = Enum.slice(items, cursor, count)  # O(cursor + count)
    batch_len = min(count, total - cursor)
    ...
  end
end
```

**Why it matters:** `length(items)` traverses the entire list. Then `Enum.slice` traverses again up to `cursor + count` elements. For HSCAN/SSCAN iterations on large hashes (1000+ fields), this is 2000+ node traversals per cursor step. The previous audit flagged this; the `length(batch)` call was fixed to use arithmetic (`min(count, total - cursor)`) but `length(items)` + `Enum.slice` double-traversal remains.

**Recommended fix:** Use `Enum.drop` + `Enum.take` which avoids the full-length computation:
```elixir
defp paginate(items, cursor, count) do
  rest = Enum.drop(items, cursor)
  case rest do
    [] -> {"0", []}
    _ ->
      batch = Enum.take(rest, count)
      if length(batch) < count do
        {"0", batch}
      else
        {"0", batch}  # or compute next cursor from cursor + length(batch)
      end
  end
end
```

---

### H3. `COPY` handler uses `length(args)` in guard

**Rule violated:** Rule 6 (avoid repeated list ops in loops)

**Location:** `apps/ferricstore/lib/ferricstore/commands/generic.ex:130`

**Current code:**
```elixir
def handle("COPY", args, store) when length(args) >= 2 do
  [source, destination | opts] = args
  ...
end
```

**Why it matters:** `length(args)` in a guard traverses the full argument list on every COPY call. This is a cold-path command, but the fix is trivial.

**Recommended fix:** Use pattern matching:
```elixir
def handle("COPY", [source, destination | opts], store) do
  ...
end
```

---

### H4. `AUTH` handler uses `length(args)` in guard

**Rule violated:** Rule 6 (avoid repeated list ops in loops)

**Location:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex:840`

**Current code:**
```elixir
defp dispatch("AUTH", args, state) when length(args) > 2 do
  {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}), state}
end
```

**Why it matters:** `length(args)` is O(n). For AUTH this is always 1-2 args so the cost is negligible, but it is an anti-pattern.

**Recommended fix:** Pattern match:
```elixir
defp dispatch("AUTH", [_, _, _ | _], state) do
  ...
end
```

---

### H5. `DBSIZE` handler calls `store.keys.()` then pipes through `Enum.reject` then `length/1`

**Rule violated:** Rule 6 (avoid repeated list ops), Rule 8 (reduce over multiple passes)

**Location:** `apps/ferricstore/lib/ferricstore/commands/server.ex:76-82`

**Current code:**
```elixir
def handle("DBSIZE", [], store) do
  alias Ferricstore.Store.CompoundKey

  store.keys.()
  |> Enum.reject(&CompoundKey.internal_key?/1)
  |> length()
end
```

**Why it matters:** `store.keys.()` collects all keys into a list (pass 1). `Enum.reject` creates a second filtered list (pass 2). `length/1` traverses the filtered list (pass 3). For 100k keys, this creates two 100k-element lists and traverses ~300k nodes.

**Recommended fix:** Use `Enum.count` to avoid the intermediate list:
```elixir
def handle("DBSIZE", [], store) do
  Enum.count(store.keys.(), fn key -> not CompoundKey.internal_key?(key) end)
end
```

Or better: use `Router.dbsize()` which already reads `:ets.info(:size)` directly without materializing lists (it exists and is in the raw store map as `store.dbsize.()`).

---

### H6. `SCAN` handler calls `length(all_keys)` in cursor fallback

**Rule violated:** Rule 6 (avoid repeated list ops in loops)

**Location:** `apps/ferricstore/lib/ferricstore/commands/generic.ex:401`

**Current code:**
```elixir
{start_index, _} =
  if cursor_str == "0" do
    {0, nil}
  else
    idx = Enum.find_index(all_keys, fn k -> k > cursor_str end)
    {idx || length(all_keys), nil}
  end
```

**Why it matters:** When `Enum.find_index` returns `nil` (cursor past the end), `length(all_keys)` traverses the entire list. This is the "end of iteration" case and happens once per SCAN cycle, so impact is low.

**Recommended fix:** Return the length from the find_index loop, or use a sentinel:
```elixir
{idx || Enum.count(all_keys), nil}
```
Or since we already have `all_keys` as a sorted list, just return 0 (end of iteration):
```elixir
{idx || 0, nil}
```
Wait -- that would restart the scan. The correct fix is to track length alongside the list, or accept the one-time cost.

---

## 4. MEDIUM Issues

### M1. `SPOP` with count calls `length(selected)` after `Enum.take_random`

**Rule violated:** Rule 6 (avoid repeated list ops)

**Location:** `apps/ferricstore/lib/ferricstore/commands/set.ex:293`

**Current code:**
```elixir
selected = Enum.take_random(members, count)

Enum.each(selected, fn member -> ... end)

if selected != [] do
  maybe_cleanup_empty_set(key, length(selected), store)
end
```

**Why it matters:** `length(selected)` traverses the list after `Enum.take_random` already knows the count. Since `count` is the requested maximum, `length(selected)` equals `min(count, length(members))`.

**Recommended fix:** Track count during the each loop or use `min(count, length(members))` computed once.

---

### M2. `HRANDFIELD` negative count uses `for _ <- 1..abs_count, do: Enum.random(pairs)` -- O(n) per random pick

**Rule violated:** Rule 6 (avoid repeated list ops)

**Location:** `apps/ferricstore/lib/ferricstore/commands/hash.ex:914`

**Current code:**
```elixir
selected = for _ <- 1..abs_count, do: Enum.random(pairs)
```

**Why it matters:** `Enum.random/1` on a list is O(n) -- it calls `length/1` then `Enum.at/2`. For `HRANDFIELD key -100` on a hash with 1000 fields, this does 100 * 1000 = 100k node traversals. Same pattern in `set.ex:389`.

**Recommended fix:** Convert to a tuple for O(1) random access:
```elixir
tuple = List.to_tuple(pairs)
size = tuple_size(tuple)
for _ <- 1..abs_count, do: elem(tuple, :rand.uniform(size) - 1)
```

---

### M3. `Config.rewrite/0` builds output with repeated `lines_acc ++ [line]` -- O(n^2)

**Rule violated:** Rule 9 (cons + reverse for list building)

**Location:** `apps/ferricstore/lib/ferricstore/config.ex:346-372`

**Current code:**
```elixir
{output_lines, written_keys} =
  Enum.reduce(existing_lines, {[], MapSet.new()}, fn line, {lines_acc, written_acc} ->
    ...
    {lines_acc ++ [line], written_acc}
    ...
  end)
```

**Why it matters:** `lines_acc ++ [line]` is O(n) on every iteration, making the total O(n^2) for n lines. CONFIG REWRITE is a cold-path admin command, so practical impact is low.

**Recommended fix:** Prepend + reverse:
```elixir
{[line | lines_acc], written_acc}
# then at the end: Enum.reverse(output_lines)
```

---

### M4. Multiple `Application.get_env/3` calls in INFO section builders

**Rule violated:** Rule 21 (:persistent_term for static data)

**Location:** `apps/ferricstore/lib/ferricstore/commands/server.ex:388, 530, 609, 657, 698`

`Application.get_env(:ferricstore, :shard_count, 4)` is called in 5 different INFO section builders. Each call is ~200-250ns.

**Why it matters:** INFO is called periodically by monitoring tools. 5 * 250ns = 1.25us of overhead per INFO call. Negligible individually, but the shard_count is a compile-time constant that could be a module attribute.

**Recommended fix:** Use the existing `@shard_count` from Router, or add a module attribute.

---

### M5. `do_scan` calls `Enum.sort/1` on potentially large key lists

**Rule violated:** Rule 8 (reduce over multiple passes)

**Location:** `apps/ferricstore/lib/ferricstore/commands/generic.ex:377-392`

**Current code:**
```elixir
all_keys =
  store.keys.()
  |> Enum.reject(&CompoundKey.internal_key?/1)
  |> filter_by_type(type_filter, store)
  |> filter_by_match(match_pattern)
  |> Enum.sort()
```

**Why it matters:** For a 100k key store, this creates 4 intermediate lists before sorting. SCAN is an iterative command -- Redis returns randomized cursor-based results without sorting. The sort is needed here for deterministic cursor semantics but could be avoided by using ETS ordered iteration directly.

---

## 5. LOW Issues

### L1. `GlobMatcher.do_match/2` star clause uses `binary_part/3` instead of binary pattern match

**Location:** `apps/ferricstore/lib/ferricstore/glob_matcher.ex:58`

**Current code:**
```elixir
defp do_match(subject, <<"*", rest::binary>>) do
  do_match(subject, rest) or do_match(binary_part(subject, 1, byte_size(subject) - 1), <<"*", rest::binary>>)
end
```

`binary_part/3` creates a sub-binary reference, which is fine (zero-copy). However, the pattern `<<_, subject_rest::binary>>` would be more idiomatic and equally efficient:
```elixir
defp do_match(<<_, subject_rest::binary>> = subject, <<"*", rest::binary>>) do
  do_match(subject, rest) or do_match(subject_rest, <<"*", rest::binary>>)
end
```

---

### L2. `check_hash_encoding/1` uses try/rescue for control flow

**Location:** `apps/ferricstore/lib/ferricstore.ex:1560-1566`

```elixir
defp is_encoded_hash?(bin) do
  try do
    check_hash_encoding(bin)
  rescue
    _ -> false
  end
end
```

Using exceptions for normal control flow is an anti-pattern on the BEAM (exceptions are expensive). The recursive `check_hash_encoding/1` below it is properly defined with pattern match clauses, so it should never raise. The rescue is defensive but unnecessary.

---

### L3. `format_peer/1` uses string interpolation with `:inet.ntoa/1`

**Location:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex:2573`

```elixir
defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
```

This allocates a new binary on every call via interpolation. Called once per connection open/close and per AUTH, so very low frequency. Not worth changing.

---

## 6. Good Patterns Found

### G1. Pre-computed shard names (Rule 30)
`router.ex:24-26`: `@shard_names` tuple computed at compile time. `elem/2` is ~5ns vs ~300ns for runtime string interpolation.

### G2. ETS with proper concurrency flags (Rules 19, 20)
All ETS tables are created with `read_concurrency: true` and `write_concurrency: true`. Examples: keydir tables (shard.ex:139), prefix_keys (prefix_index), tracking tables (client_tracking.ex:99-100), config table (config.ex:264-268), ns_config.

### G3. :persistent_term for static/slow-changing data (Rule 21)
- LFU config cached in persistent_term (lfu.ex)
- MemoryGuard flags in persistent_term: `keydir_full`, `reject_writes` (memory_guard.ex:276-278)
- HLC atomics ref in persistent_term (hlc.ex)
- `hot_cache_max_value_size` in persistent_term (config.ex:659)
- Raw store map cached in persistent_term (connection.ex:1956-1964) -- eliminates ~25 closure allocations per non-sandbox command

### G4. iodata in RESP Encoder (Rule 24)
`encoder.ex` returns `iodata()` throughout -- no unnecessary binary concatenation. Lists like `["+", str, @crlf]` are passed directly to `transport.send`.

### G5. Binary pattern matching in RESP Parser (Rule 23)
`parser.ex` uses binary pattern matching for all RESP3 type dispatch (lines 124-139) and for bulk data reading. Zero-copy sub-binary extraction. The inline parser was fixed to use `:binary.split/3` instead of `String.split/1`.

### G6. Single-pass encode_list_counted (Rule 8)
`encoder.ex:167-171`: Counts and encodes list elements in a single traversal, fixing the previous double-traversal from `length/1` + `encode_list_elements`.

### G7. Function head pattern matching throughout (Rule 15)
Dispatcher uses compile-time map for O(1) dispatch. Command handlers use function head matching extensively. Connection's `barrier_command_normalised?` was fixed to use `match?([_, _ | _], args)` instead of `length(args) > 1`.

### G8. Compile-time dispatch map in Dispatcher (Rule 30)
`@cmd_dispatch_map` is a compile-time Map built from module attributes, giving O(log n) command routing.

### G9. ETS bypass for hot reads (Rule 19)
`Router.get/1` and `Router.get_meta/1` read ETS directly without GenServer roundtrip for cached keys. Cold keys fall through to GenServer.call. `Router.exists_fast?/1` provides direct ETS check for the write-path keydir_full guard.

### G10. Lock-free HLC using :atomics (Rule 11)
`HLC.now/0` and `HLC.now_ms/0` use :atomics stored in :persistent_term, completely avoiding GenServer serialization on the hot path.

### G11. Cons + reverse in Batcher (Rule 9)
Batcher accumulates commands with prepend `[command | slot.cmds]` and reverses once at flush time. MULTI transaction queue uses same pattern (connection.ex:1288).

### G12. Connection buffer optimization
`Connection.handle_data` avoids binary concatenation when buffer is empty: `if state.buffer == "", do: data, else: state.buffer <> data` (connection.ex:319).

### G13. Direct recursive MSET/MSETNX helpers (Rule 8)
`strings.ex:605-633`: `mset_exec/2`, `mset_validate/1`, `msetnx_any_exists?/2`, and `even_length?/1` walk [k, v, k, v, ...] directly without `Enum.chunk_every`.

### G14. Direct recursive HSET helper (Rule 8)
`hash.ex:739-747`: `hset_pairs/4` walks the flat field-value list directly without `Enum.chunk_every` intermediate allocation. Fixed from the previous audit's H3.

### G15. Config.get_value/1 reads ETS directly (Rule 11)
`config.ex:205-213`: Reads from ETS (~100ns) instead of GenServer.call (~1-5us). The ETS table is synced on every CONFIG SET. This eliminates the Config GenServer as a contention point for `requires_auth?` checks on every command. Fixed from the previous audit's C3.

### G16. GlobMatcher replaces runtime regex compilation (Rule 27)
`glob_matcher.ex`: Hand-written binary glob matcher used by server.ex, generic.ex, hash.ex, set.ex, and config.ex. Eliminates the `glob_to_regex` + `Regex.compile!` anti-pattern that was in 5 files. Fixed from the previous audit's C1 and L2.

### G17. `parse_float_value` uses `:binary.match` instead of regex (Rule 27)
`parser.ex:465-479`: Fixed from the previous audit's C4. Uses `:binary.match` for e/E search instead of `String.split(str, ~r/[eE]/, parts: 2)`.

### G18. `parse_inline` uses `:binary.split` instead of `String.split` (Rule 23)
`parser.ex:388`: Fixed from the previous audit's C2. Uses `:binary.split(line, [<<" ">>, <<"\t">>], [:global, :trim_all])` instead of `String.split(line)`.

### G19. MapSet for ACL and eviction policy validation (Rule 4)
Config uses `@read_only_params MapSet.new(...)`, `@read_write_params MapSet.new(...)`, and `@valid_eviction_policies MapSet.new(...)` for O(1) membership checks.

### G20. Single-pass discover_active_file (Rule 8)
`shard.ex:232-255`: Uses a single `Enum.reduce` pass instead of filter + map + max to find the highest file ID.

### G21. Shard and Connection use defstruct (Rule 1)
Both `Shard` (shard.ex:65-92) and `Connection` (connection.ex:78-101) use `defstruct` for state, giving compile-time field validation and memory-efficient key sharing.

### G22. MemoryGuard eviction uses `{count, list}` accumulator (Rule 6)
`memory_guard.ex:296-306`: Fixed from the previous audit's H1. Uses `{count, found}` accumulator to avoid O(n) `length/1` on every foldl iteration.

### G23. `validate_field_count/2` uses recursive short-circuit (Rule 6)
`hash.ex:837-839`: Fixed from the previous audit's M3. Uses direct recursion `validate_field_count(n - 1, rest)` instead of `length(fields)`.

---

## 7. Anti-Patterns Catalog

### A1. Remaining `glob_to_regex` in FerricStore embedded API (C1)
The standalone TCP path was fixed to use `GlobMatcher`, but the `FerricStore` module retains its own `glob_to_regex/1` at line 3983. This is the last remaining instance.

### A2. Store map as plain map with opaque function closures (C3)
The store map pattern (`%{get: fn ..., put: fn ..., ...}`) is used throughout and accessed via `store.get.()`. Every access is opaque to the type inferrer. This is an architectural pattern that would require significant refactoring to change. Documented for awareness.

### A3. `length/1` in guards and cold paths (H3, H4, H5, H6, M1)
Several handlers still use `length(args)` in guards or bodies where pattern matching would suffice. Most are cold-path (COPY, AUTH, DBSIZE) with negligible performance impact, but represent a recurring pattern to watch for.

### A4. Multiple-pass list processing (C2, H1, H2, H5, M5)
Several functions apply 3-5 Enum operations sequentially, creating intermediate lists. Most are in admin/cold paths (keys, dbsize, encode_hash, scan). The hot-path handlers (MSET, HSET, etc.) have been correctly optimized to use single-pass recursion.

### A5. `Enum.random/1` on lists is O(n)
`Enum.random` on a linked list calls `length/1` + `Enum.at/2`, making it O(n). Used in RANDOMKEY, SRANDMEMBER, HRANDFIELD, SPOP. For large collections with repeated random picks (negative count), this compounds. Converting to a tuple first would give O(1) random access.
