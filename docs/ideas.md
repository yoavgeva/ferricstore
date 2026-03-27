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

## Per-Connection Output Buffer Limit (CVE-2025-21605)

Track bytes buffered for each connection. If a client stops reading responses, the BEAM connection process heap grows unbounded. Redis solved this with hard/soft output buffer limits (256MB hard for normal clients, 64MB for pubsub).

**Current mitigation:** Auth is required for production deployments — unauthenticated clients can only trigger small error responses. Ranch detects dead sockets and crashes the connection process. PubSub subscribers that disconnect are cleaned up.

**Why deferred:** The attack requires either no-auth deployment (user config mistake) or an authenticated client deliberately slow-reading. Not a realistic threat for authenticated deployments. Implement if a user reports it or if we target untrusted-network deployments without auth.

**If revisited:** Add `bytes_pending` counter to connection state, increment on `transport.send()`, check against `@max_output_buffer` (256MB normal, 64MB pubsub). Close connection on exceed.

## SCAN Cursor Optimization

Current SCAN does a full keyspace scan + sort on every call. With 10M keys, each `SCAN 0 COUNT 10` is O(10M). Redis SCAN is also O(N) worst case but amortizes over cursor iterations.

**Why deferred:** SCAN is rarely used in hot paths. The `KEYS` command is already flagged as `@dangerous` in ACL. For typical key counts (<1M), the current approach is fast enough.

**If revisited:** Maintain a per-shard sorted key snapshot with TTL. Return cursor as the last key's binary position. Subsequent SCAN calls binary-search into the snapshot instead of re-scanning.

## ACL Replication via Raft

Currently ACL is per-node — `acl.conf` must be manually deployed to all cluster nodes. Users must copy the file and run `ACL LOAD` on each node after any change.

**Phase 1 (Raft replication):** `ACL SETUSER` becomes a Raft command replicated to all nodes automatically. No manual file distribution. Same consistency as data writes.

**Phase 2 (Control Plane + Dashboard):** A management API and web dashboard for user/ACL administration. Part of the existing health dashboard at `/dashboard`:
- Create/edit/delete users via web UI
- View active sessions and permissions
- Credential rotation with zero downtime
- Audit log of ACL changes
- OAuth/OIDC integration for enterprise SSO
- API endpoint for infrastructure automation (Terraform, Pulumi)

**Why deferred:** Phase 1 requires ACL operations in the Raft state machine. Phase 2 requires a web framework (Phoenix LiveView) and auth integration. Both are significant but would make FerricStore competitive with managed Redis services.

**Current approach:** Deploy the same `acl.conf` to all nodes via config management (Ansible, k8s Secrets, Terraform). Same as self-hosted Redis Cluster.
