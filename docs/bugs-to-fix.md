# Bugs To Fix

## Fixed

| Bug | Severity | Commit |
|-----|----------|--------|
| BLMOVE/BLMPOP/XREAD BLOCK socket deadlock | CRITICAL | c7e4652 |
| LINSERT position collisions after many insertions | HIGH | 6edb35f |

## Remaining

### 1. READMODE/SANDBOX Execute in PubSub Mode

**Severity: MEDIUM**
**File:** `apps/ferricstore_server/lib/ferricstore_server/connection.ex:440-441`
**Test:** `apps/ferricstore_server/test/ferricstore_server/bugs/acl_bypass_test.exs`

READMODE and SANDBOX have specific `dispatch/3` clauses that match BEFORE
the pubsub mode check. They execute while the connection is in pubsub mode
when they should be blocked. Only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/
PUNSUBSCRIBE/PING/QUIT/RESET should be allowed in pubsub mode.

## Not A Bug

| Issue | Reason |
|-------|--------|
| UNSUBSCRIBE doesn't clear patterns | Redis behavior — UNSUBSCRIBE only clears channels, PUNSUBSCRIBE only clears patterns |
| Pipeline tasks not awaited on QUIT | QUIT is stateful — tasks always awaited before it executes |
| Position encoding float precision | Fixed by integer positions (no floats used anymore) |
| ACL bypass on READMODE/SANDBOX | ACL checks work correctly — the dispatch branch is the bug, not ACL |
