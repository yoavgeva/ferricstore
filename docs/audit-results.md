# Code Audit Results

10-agent sweep comparing all commands against Redis spec + checking
internal correctness.

## Real Bugs Found

### 1. INCR/DECR Integer Overflow Not Detected

**Severity: MEDIUM**
**File:** `state_machine.ex` (do_incr)

`new_val = int_val + delta` has no bounds check. Elixir integers are
arbitrary precision, so INCR past int64 max (9223372036854775807) returns
9223372036854775808 instead of Redis's "ERR increment or decrement would
overflow". Affects INCR, DECR, INCRBY, DECRBY.

### 2. XTRIM MINID Crashes on Invalid ID

**Severity: MEDIUM**
**File:** `commands/stream.ex:599`

`{:ok, min_id} = parse_full_id(min_id_str)` — pattern match fails with
MatchError when user provides invalid MINID. Should handle `{:error, msg}`
and return proper error response.

### 3. ZRANDMEMBER WITHSCORES Returns Unformatted Scores

**Severity: LOW**
**File:** `commands/sorted_set.ex:881,895`

Returns raw score strings instead of using `format_score_str()` like all
other WITHSCORES commands (ZRANGE, ZPOPMIN, ZSCAN, etc.). Score format
inconsistency across commands.

### 4. Missing WRONGTYPE Checks on 7 List Commands

**Severity: MEDIUM**
**File:** `commands/list.ex`

LSET, LREM, LTRIM, LPOS, LINSERT, LPUSHX, RPUSHX don't check key type
before operating. Using these on a string key should return WRONGTYPE
error but doesn't. Other list commands (LPUSH, RPUSH, LPOP, LRANGE, etc.)
correctly check type.

### 5. SUBSCRIBE/BLPOP Bypass MULTI Transaction Guard

**Severity: MEDIUM**
**File:** `connection.ex:446-459`

SUBSCRIBE, PSUBSCRIBE, BLPOP, BRPOP, BLMOVE, BLMPOP have dispatch
clauses that match BEFORE the MULTI queuing guard. They execute
immediately instead of being queued/rejected during a transaction. Redis
rejects blocking commands inside MULTI.

### 6. EXPIRE NX/XX/GT/LT Flags Not Implemented

**Severity: LOW (feature gap)**
**File:** `commands/expiry.ex`

EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT don't support Redis 7.0+ NX/XX/GT/LT
modifiers. Not a bug per se, but a compatibility gap if advertising
Redis 7.4 compatibility.

### 7. Verbatim String Encoding Not Validated

**Severity: LOW**
**File:** `resp/encoder.ex:143`

Encoder accepts verbatim string encoding of any length, but RESP3 spec
requires exactly 3 bytes. Parser enforces this, so encoder can produce
data the parser rejects. Minor — verbatim strings are rarely used.

## No Bugs Found

| Area | Status |
|------|--------|
| Hash commands (HSET, HGET, HDEL, HMGET, etc.) | Clean |
| Set commands (SADD, SREM, SMEMBERS, SINTER, etc.) | Clean |
| Expiry/TTL (EXPIRE, TTL, PTTL, PERSIST, etc.) | Clean |
| Raft replication determinism | Clean (now_ms captured in command, not at apply time) |
| RESP3 parser | Clean |

## Not Bugs (Agent False Positives)

| Issue | Why Not A Bug |
|-------|---------------|
| Ratelimit non-determinism | now_ms is captured at command submission and passed in the Raft command tuple — all nodes use the same value |
| UNSUBSCRIBE not clearing patterns | Redis spec — UNSUBSCRIBE only clears channels |
| Missing ZRANGEBYLEX/ZLEXCOUNT/etc. | Feature gaps, not bugs |
| Missing XCLAIM/XAUTOCLAIM/XPENDING | Feature gaps, not bugs |
