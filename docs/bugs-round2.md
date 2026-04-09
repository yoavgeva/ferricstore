# Bug Report: Round 2 Audit

## Critical Bugs Found

### Bug #1: MSETNX Race Condition (Data Corruption)
**File:** `/Users/yoavgea/repos/ferricstore/lib/ferricstore.ex:862-873`
**Severity:** CRITICAL - Can violate MSETNX atomicity

**Issue:** The `msetnx` function checks if any keys exist (line 865), then writes all keys (line 870). Between these two operations, another concurrent request could create one of the keys, causing the MSETNX to proceed when it should have aborted.

**Current Code:**
```elixir
def msetnx(pairs) when is_map(pairs) do
  ctx = default_ctx()

  any_exists = Enum.any?(pairs, fn {k, _v} -> Router.exists?(ctx, k) end)

  if any_exists do
    {:ok, false}
  else
    Enum.each(pairs, fn {k, v} -> Router.put(ctx, k, v, 0) end)
    {:ok, true}
  end
end
```

**Problem:** No atomicity guarantee. The check and write are separate operations.

**Redis Semantics:** MSETNX must be atomic: either ALL keys are set (if none existed) or NONE are set (if any existed).

**Fix:** Wrap the entire operation in a `CrossShardOp.execute` call with appropriate locking, similar to how RENAME and COPY are implemented.

---

### Bug #2: JSON Bracket String Parsing Off-by-One (Wrong Results)
**File:** `/Users/yoavgea/repos/ferricstore/apps/ferricstore/lib/ferricstore/commands/json.ex:682-688`
**Severity:** CRITICAL - Returns wrong results for JSON paths with string keys

**Issue:** The code uses `String.slice(inner, 1..-2//1)` to remove quotes from bracket expressions. This removes TWO characters from the end instead of ONE.

**Current Code:**
```elixir
defp parse_bracket_content(<<"\"", _::binary>> = inner, remainder) do
  {:ok, String.slice(inner, 1..-2//1), remainder}
end

defp parse_bracket_content(<<"'", _::binary>> = inner, remainder) do
  {:ok, String.slice(inner, 1..-2//1), remainder}
end
```

**Example:** For bracket content `"foo"`:
- Current code returns: `fo` (removes quote AND next char)
- Expected result: `foo` (removes only the quotes)

**Why:** `String.slice(str, 1..-2//1)` means:
- Start at index 1
- End at index -2 (second-to-last character)
- This includes indices 1, 2, ..., -2 (length-2)

For `"foo"` (length 5):
- Indices: 0=`"`, 1=`f`, 2=`o`, 3=`o`, 4=`"`
- Slice 1..-2 gives indices 1,2,3 = `foo`

Wait - let me recalculate. In Elixir's slice with negative indices: -2 means length-2 = 5-2 = 3. So we get indices 1 through 3 inclusive, which is `foo`. This may actually be CORRECT.

**Verification Needed:** Test with actual string to confirm behavior.

Actually, reviewing the logic more carefully: for `"foo"`:
- Length = 5
- Slice from 1 to -2: That's index 1 to index (5-2=3) inclusive
- Indices 1,2,3 = "foo" 

This is correct! Disregard this bug.

---

### Bug #3: None Found - All Other Areas Clean

After reviewing:
- GeoSearch/GeoAdd: Properly implemented with geohash encoding
- Bitmap commands: Correct bit position calculations
- HyperLogLog: Proper validation and merging
- SCAN cursor: Correct cursor-based iteration logic
- COPY: Protected by CrossShardOp locks
- RENAME/RENAMENX: Protected by CrossShardOp locks

---

## Summary

**1 Critical Bug Found:**
1. MSETNX race condition causing potential non-atomic execution

**To Fix:** Add CrossShardOp wrapper to msetnx() in ferricstore.ex similar to how rename/copy are protected.
