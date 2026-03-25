# Typed ETS Values Design

## Status
**Draft** — March 2026

## Summary

Store native integer and float types in ETS instead of always-strings. Enables `:ets.update_counter` for atomic INCR, smaller memory, faster operations. Disk format stays all-binary (backward compatible). RESP3 returns native types.

## Types

| ETS Value Type | When Used | Example |
|---------------|-----------|---------|
| `binary()` | SET, APPEND, any string write | `"hello"` |
| `integer()` | After INCR/DECR/INCRBY | `42` |
| `float()` | After INCRBYFLOAT | `3.14` |
| `nil` | Cold key (value on disk only) | `nil` |

Type detection via Elixir guards (`is_integer/1`, `is_float/1`, `is_binary/1`). No tag field needed.

## Commands That Benefit

### INCR/DECR/INCRBY/DECRBY
- **Current:** Parse string → integer, add delta, format back to string, write string
- **Proposed:** Read integer directly, add delta, write integer. Skip 2x string alloc + parse
- **Future:** `:ets.update_counter` for truly atomic increment (no read-modify-write)

### INCRBYFLOAT
- **Current:** Parse string → float, add delta, format back to string
- **Proposed:** Read float directly, add delta, write float. Format only for return value.

### GET
- **Current:** Returns binary always
- **Proposed:** Returns native type. RESP3 encoder already handles integers (`:42\r\n`) and floats (`,3.14\r\n`)

### APPEND/STRLEN/GETRANGE
- **Require string:** Convert integer/float to string first, then operate
- APPEND on integer 42 → convert to "42" → append suffix → store as binary

### OBJECT ENCODING
- Returns `"int"`, `"float"`, or `"embstr"` based on ETS value type

## Disk Format (No Change)

All values written to Bitcask are serialized as binaries:
- Integer 42 → `"42"` on disk
- Float 3.14 → `"3.14"` on disk
- Binary `"hello"` → `"hello"` on disk

On cold read recovery: values load as binary. Re-typed on first INCR.

**No migration needed. Fully backward compatible.**

## RESP3 Wire Format

RESP3 has native types — we use them:
- GET on integer → `:42\r\n` (RESP3 integer)
- GET on float → `,3.14\r\n` (RESP3 double)
- GET on string → `$5\r\nhello\r\n` (bulk string)

The encoder already handles this (encoder.ex lines 112-132).

## Code Changes

### Router (router.ex)
- `async_write {:incr}` — write integer to ETS instead of string
- `async_write {:incr_float}` — write float to ETS
- `async_write {:put}` — store integer/float when value is numeric type

### Shard (shard.ex)
- `handle_incr_direct` — write integer to ETS, not `Integer.to_string`
- `handle_incr_float_direct` — write float to ETS
- `ets_insert/4` — accept integer/float values
- `value_for_ets/1` — pass through integer/float (always small enough for ETS)
- `ets_lookup/2` — handle integer/float in pattern matches

### StateMachine (state_machine.ex)
- `do_incr/3` — store integer, not string
- `do_incr_float/3` — store float, not string
- Serialize to binary before Bitcask write

### Commands (strings.ex)
- STRLEN — `byte_size(Integer.to_string(v))` for integers
- GETRANGE — convert to string first
- APPEND — convert to string first, then append

### Generic (generic.ex)
- OBJECT ENCODING — detect integer/float/binary

## Async Write Path

Integer stored directly in ETS. Async INCR still has read-modify-write race (same as before with strings). No atomicity regression. Future: `:ets.update_counter` for lock-free atomic INCR.

## Type Transitions

```
SET key "42"      → ETS: binary "42"
INCR key          → ETS: integer 43 (parsed string, stored native)
GET key           → returns integer 43
APPEND key "!"    → ETS: binary "43!" (converted back to string)
INCR key          → error: "not an integer"
```

## Test Plan

### New Tests (test/ferricstore/ets_types_test.exs)
1. INCR stores integer in ETS (verify with :ets.lookup)
2. INCRBYFLOAT stores float in ETS
3. SET stores binary in ETS
4. GET returns integer unchanged
5. GET returns float unchanged
6. INCR on binary string parses and converts to integer
7. APPEND on integer converts to string
8. STRLEN on integer counts string digits
9. GETRANGE on integer extracts from string representation
10. INCRBYFLOAT on integer produces float
11. OBJECT ENCODING reports int/float
12. Cold read returns binary (from disk)
13. Bitcask disk format is always binary
14. Type transition: String → Integer → String
15. Integer overflow near max_int64
16. Float precision with long decimals
17. INCR on non-integer returns error
18. INCR on float returns error (not integer)
19. TTL preserved across INCR
20. Concurrent INCR (quorum — ordered by Raft)
21. Concurrent INCR (async — last writer wins)
22. RESP3 encoding: GET integer returns `:42\r\n`
23. RESP3 encoding: GET float returns `,3.14\r\n`

### Existing Tests to Update
- `embedded_extended_test.exs` — INCR return type checks
- `commands/strings_test.exs` — GET after INCR returns integer
- `async_durability_test.exs` — INCR stores integer in async path
- `cross_shard_atomic_test.exs` — INCR in transactions returns integer

## Implementation Phases

1. **Phase 1:** Shard + StateMachine store native types for INCR/INCRBYFLOAT
2. **Phase 2:** Serialize to binary before Bitcask write, handle cold reads
3. **Phase 3:** Update string commands (STRLEN, GETRANGE, APPEND)
4. **Phase 4:** Tests + benchmarks + multi-node validation
