# Bugs To Fix

Found by codebase audit. Verified with failing tests.

## Fixed

| Bug | Severity | Commit |
|-----|----------|--------|
| BLMOVE/BLMPOP/XREAD BLOCK socket deadlock — active:once never restored | CRITICAL | c7e4652 |

## Verified Bugs (failing tests exist)

### 1. LINSERT Position Collisions After Many Insertions

**Severity: HIGH**
**File:** `apps/ferricstore/lib/ferricstore/store/list_ops.ex:316`
**Test:** `apps/ferricstore/test/ferricstore/bugs/linsert_negative_index_test.exs`

The midpoint position calculation breaks down after ~53 LINSERT BEFORE
operations at the same spot. Float precision runs out, positions collide,
and elements are lost. Test: 55 LINSERT operations produce 54 elements
instead of 57.

Note: The original `Enum.at(sorted, -1)` negative index bug is prevented
by the `if idx == 0` guard. The real bug is precision exhaustion.

---

### 2. UNSUBSCRIBE With No Args Doesn't Clear Pattern Subscriptions

**Severity: HIGH**
**File:** `apps/ferricstore_server/lib/ferricstore_server/connection/pubsub.ex:50-56`
**Test:** `apps/ferricstore_server/test/ferricstore_server/bugs/pubsub_unsubscribe_leak_test.exs`

`UNSUBSCRIBE` with no arguments only clears `pubsub_channels`, not
`pubsub_patterns`. Connection stays in pubsub mode permanently. Same
in reverse: `PUNSUBSCRIBE` with no args doesn't clear channels.

Test: SUBSCRIBE + PSUBSCRIBE + UNSUBSCRIBE (no args) → GET returns
"ERR Can't execute" instead of working.

---

### 3. READMODE/SANDBOX Execute in PubSub Mode

**Severity: HIGH**
**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex:440-441`
**Test:** `apps/ferricstore_server/test/ferricstore_server/bugs/acl_bypass_test.exs`

READMODE and SANDBOX have specific `dispatch/3` clauses that match BEFORE
the pubsub mode check, allowing them to execute while in pubsub mode when
they should be blocked. (The original ACL bypass hypothesis was wrong —
ACL checks work correctly.)

---

### 4. Position Encoding Float Precision Loss

**Severity: MEDIUM**
**File:** `apps/ferricstore/lib/ferricstore/store/compound_key.ex:189`
**Test:** `apps/ferricstore/test/ferricstore/bugs/position_encoding_precision_test.exs`

`encode_position` multiplies float by 10^17 and truncates. After 55
successive midpoint subdivisions, distinct float positions collapse to
the same encoded string. Elements get duplicate positions → data loss.

---

## Not A Bug

### Pipeline Tasks Not Awaited on QUIT

QUIT is a stateful command — pure-group lane tasks are always flushed
and awaited before QUIT executes. The `:quit` branch in
`sliding_window_collect` is unreachable. Tests confirm correct behavior.
