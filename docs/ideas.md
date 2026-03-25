# Ideas — Future Explorations

Ideas that are interesting but not planned for implementation. May revisit if the need arises.

## Typed ETS Values (Integer/Float Native Storage)

Store integers and floats natively in ETS instead of strings. Would enable:
- `:ets.update_counter` for atomic INCR without read-parse-write race
- Skip `Integer.parse`/`Integer.to_string` on every INCR (~200ns saving)
- Smaller memory footprint (8-byte int vs 50-byte binary)

**Why deferred:** The ~200ns saving is invisible vs Raft latency (~750us). Every code path touching ETS values needs type guards. The NIF boundary (Bitcask) only accepts binaries — every write must serialize. First attempt crashed BitcaskWriter because the cross-shard transaction store passed integers to the NIF. Strings work fine.

**If revisited:** Convert at the NIF boundary (`to_disk_binary`), not at `value_for_ets`. ETS keeps native types, disk gets strings. Design doc at `docs/typed-ets-values-design.md`.

## RESP2 Fallback

Support RESP2 protocol for old Redis clients that don't speak RESP3. Would require a protocol negotiation layer in the connection handler.

**Why deferred:** All major Redis client libraries support RESP3 since 2021. Migration guide can document the client upgrade path.

## Lua Scripting (EVAL)

Redis EVAL/EVALSHA for server-side Lua scripts. Would require embedding a Lua VM (Luerl) and ensuring deterministic execution for Raft replay.

**Why deferred:** Complex. The spec suggests a restricted deterministic subset for v2 if customer demand justifies it.
