# Bugs To Fix

Found by codebase audit. Sorted by severity.

## Fixed

| Bug | Severity | Status |
|-----|----------|--------|
| BLMOVE/BLMPOP/XREAD BLOCK socket deadlock — active:once never restored | CRITICAL | FIXED (c7e4652) |

## To Investigate

### 1. LINSERT Negative Index — Wrong Position Calculation

**Severity: HIGH**
**File:** `apps/ferricstore/lib/ferricstore/store/list_ops.ex:316`

LINSERT BEFORE on the first element (idx=0) does `Enum.at(sorted, idx - 1)`
which is `Enum.at(sorted, -1)` — Elixir negative indexing returns the LAST
element. For a 3-element list [A, B, C], LINSERT BEFORE A calculates the
new position using C's position instead of erroring, corrupting the list
order metadata.

**Trigger:** `RPUSH mylist a b c` then `LINSERT mylist BEFORE a newelem`

---

### 2. UNSUBSCRIBE With No Args Doesn't Clear Pattern Subscriptions

**Severity: HIGH**
**File:** `apps/ferricstore_server/lib/ferricstore_server/connection/pubsub.ex:50-56`

`UNSUBSCRIBE` with no arguments only unsubscribes from `pubsub_channels`,
not `pubsub_patterns`. Connection stays in pubsub mode permanently.
Same bug in reverse: `PUNSUBSCRIBE` with no args doesn't clear channels.

**Trigger:** `SUBSCRIBE foo` → `PSUBSCRIBE bar*` → `UNSUBSCRIBE` → `PING`
returns error (still in pubsub mode).

---

### 3. READMODE and SANDBOX Bypass ACL Checks

**Severity: HIGH**
**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex:382`

READMODE and SANDBOX are not in `@acl_bypass_cmds` but they're also not
handled by the ACL check branch. They fall through to the `true` catch-all
which executes without permission checks. A restricted user can switch to
stale reads or create sandboxes.

---

### 4. Pipeline Tasks Not Awaited on QUIT

**Severity: MEDIUM**
**File:** `apps/ferricstore_server/lib/ferricstore_server/connection/pipeline.ex:216`

If a pipeline contains QUIT, `sliding_window_collect` returns `{:quit, state}`
and the function returns without awaiting lane tasks. Orphaned Task processes
keep running and sending messages to a dead connection process.

**Trigger:** Pipeline with `[GET foo, QUIT, GET bar, ... 1000 more GETs]`

---

### 5. Position Encoding Float Precision Loss

**Severity: MEDIUM**
**File:** `apps/ferricstore/lib/ferricstore/store/compound_key.ex:189`

`encode_position` multiplies float by 10^17 and truncates. For positions near
machine epsilon, different positions can collapse to the same encoded value
after serialize/deserialize, causing list elements to have duplicate positions.

**Trigger:** Many rapid LINSERT operations that generate fractional positions
near float precision boundary.

---

### 6. parse_id! Crashes on Corrupted Stream Data

**Severity: LOW (only on storage corruption)**
**File:** `apps/ferricstore/lib/ferricstore/commands/stream.ex:964`

`parse_id!` uses `String.to_integer` which raises on invalid input. Used in
disk recovery paths (XRANGE, XREVRANGE). If a stream entry ID is corrupted
on disk, the entire shard crashes on read. Not a bug under normal operation
— only triggers on storage corruption.
