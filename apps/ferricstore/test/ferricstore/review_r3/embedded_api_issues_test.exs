defmodule Ferricstore.ReviewR3.EmbeddedApiIssuesTest do
  @moduledoc """
  Tests proving/disproving Round 3 embedded API issues.

  R3-H1: Set operations don't propagate errors (WRONGTYPE swallowed by {:ok, _} wrapping)
  R3-H2: Sorted set operations don't propagate errors (same pattern)
  R3-M4: del() silently swallows errors (always returns :ok, ignores result)
  R3-M5: sismember returns bare boolean (not {:ok, boolean()})
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.flush_all_keys() end)
    :ok
  end

  # ===========================================================================
  # R3-H1: Set operations don't propagate WRONGTYPE errors
  # ===========================================================================

  describe "R3-H1: set operations error propagation" do
    test "sadd on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      # Store a plain string value under the key
      :ok = FerricStore.set("r3h1_str", "hello")

      # sadd on a string-typed key should return a WRONGTYPE error.
      # BUG: if sadd blindly wraps result in {:ok, result}, we get
      # {:ok, {:error, "WRONGTYPE ..."}} instead of {:error, "WRONGTYPE ..."}
      result = FerricStore.sadd("r3h1_str", ["member1"])

      # Assert the error is NOT double-wrapped
      refute match?({:ok, {:error, _}}, result),
        "sadd wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      # The correct return should be {:error, _}
      assert match?({:error, _}, result),
        "sadd should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "srem on a sorted-set key returns {:error, _}, not {:ok, {:error, _}}" do
      # Create a sorted set
      {:ok, 1} = FerricStore.zadd("r3h1_zset", [{1.0, "member1"}])

      # srem on a zset-typed key should produce WRONGTYPE
      result = FerricStore.srem("r3h1_zset", ["member1"])

      refute match?({:ok, {:error, _}}, result),
        "srem wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "srem should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "smembers on a sorted-set key returns {:error, _}, not {:ok, {:error, _}}" do
      {:ok, 1} = FerricStore.zadd("r3h1_zset2", [{1.0, "m"}])

      result = FerricStore.smembers("r3h1_zset2")

      refute match?({:ok, {:error, _}}, result),
        "smembers wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "smembers should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "scard on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h1_str2", "val")

      result = FerricStore.scard("r3h1_str2")

      refute match?({:ok, {:error, _}}, result),
        "scard wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "scard should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R3-H2: Sorted set operations don't propagate WRONGTYPE errors
  # ===========================================================================

  describe "R3-H2: sorted set operations error propagation" do
    test "zadd on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h2_str", "hello")

      result = FerricStore.zadd("r3h2_str", [{1.0, "member1"}])

      refute match?({:ok, {:error, _}}, result),
        "zadd wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "zadd should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zadd on a set key returns {:error, _}, not {:ok, {:error, _}}" do
      {:ok, 1} = FerricStore.sadd("r3h2_set", ["member1"])

      result = FerricStore.zadd("r3h2_set", [{1.0, "member1"}])

      refute match?({:ok, {:error, _}}, result),
        "zadd wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "zadd should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zcard on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h2_str2", "val")

      result = FerricStore.zcard("r3h2_str2")

      refute match?({:ok, {:error, _}}, result),
        "zcard wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "zcard should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zrem on a set key returns {:error, _}, not {:ok, {:error, _}}" do
      {:ok, 1} = FerricStore.sadd("r3h2_set2", ["member1"])

      result = FerricStore.zrem("r3h2_set2", ["member1"])

      refute match?({:ok, {:error, _}}, result),
        "zrem wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "zrem should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zrange on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h2_str3", "val")

      result = FerricStore.zrange("r3h2_str3", 0, -1)

      refute match?({:ok, {:error, _}}, result),
        "zrange wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "zrange should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zscore on a set key returns {:error, _} without crashing" do
      {:ok, 1} = FerricStore.sadd("r3h2_set3", ["member1"])

      # zscore does `case handle(...) do nil -> ... ; score_str -> Float.parse(score_str) end`
      # If WRONGTYPE {:error, _} is returned, it won't match either clause and will crash
      # with a FunctionClauseError or MatchError.
      result =
        try do
          FerricStore.zscore("r3h2_set3", "member1")
        rescue
          e -> {:rescued, e.__struct__}
        end

      refute match?({:rescued, _}, result),
        "zscore crashed on wrong type: #{inspect(result)}"

      refute match?({:ok, {:error, _}}, result),
        "zscore wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
        "zscore should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R3-M4: del() silently swallows errors / always returns :ok
  # ===========================================================================

  describe "R3-M4: del() return value" do
    test "del returns :ok regardless of whether key existed" do
      # del currently discards the command handler result and always returns :ok.
      # This means callers cannot distinguish between:
      #   - key was deleted (Redis returns 1)
      #   - key did not exist (Redis returns 0)
      #   - error occurred

      # Non-existent key
      result_miss = FerricStore.del("r3m4_nonexistent")
      assert result_miss == :ok, "del on missing key: #{inspect(result_miss)}"

      # Existing key
      :ok = FerricStore.set("r3m4_exists", "value")
      result_hit = FerricStore.del("r3m4_exists")
      assert result_hit == :ok, "del on existing key: #{inspect(result_hit)}"

      # Verify the key is actually gone
      assert {:ok, nil} = FerricStore.get("r3m4_exists")
    end

    test "del does not return {:ok, count} like other write operations" do
      # Set operations return {:ok, count}, but del returns bare :ok.
      # This documents the inconsistency — del should arguably return
      # {:ok, 1} or {:ok, 0} to match Redis DEL behavior.
      :ok = FerricStore.set("r3m4_count", "value")

      result = FerricStore.del("r3m4_count")

      # This test documents the CURRENT behavior (returns :ok).
      # If del is fixed to return {:ok, count}, this assertion will fail,
      # proving the fix landed.
      refute match?({:ok, _count}, result),
        "del now returns {:ok, count} — R3-M4 may be fixed. Got: #{inspect(result)}"
    end

    test "del on a set key returns :ok (error not surfaced)" do
      {:ok, 2} = FerricStore.sadd("r3m4_set", ["a", "b"])

      # del routes through Commands.Strings.handle("DEL", ...) which may
      # not clean up compound set keys properly, but the embedded API
      # discards whatever it returns.
      result = FerricStore.del("r3m4_set")
      assert result == :ok, "del on set key: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R3-M5: sismember returns bare boolean instead of {:ok, boolean()}
  # ===========================================================================

  describe "R3-M5: sismember return type" do
    test "sismember returns bare true, not {:ok, true}" do
      {:ok, 1} = FerricStore.sadd("r3m5_set", ["member1"])

      result = FerricStore.sismember("r3m5_set", "member1")

      # Document the CURRENT behavior: bare boolean
      assert result === true,
        "sismember on existing member returned: #{inspect(result)}"

      # This is the inconsistency: every other set operation returns {:ok, _}
      # but sismember returns a bare boolean.
      refute match?({:ok, true}, result),
        "sismember now returns {:ok, true} — R3-M5 may be fixed. Got: #{inspect(result)}"
    end

    test "sismember returns bare false, not {:ok, false}" do
      {:ok, 1} = FerricStore.sadd("r3m5_set2", ["member1"])

      result = FerricStore.sismember("r3m5_set2", "nonexistent")

      assert result === false,
        "sismember on missing member returned: #{inspect(result)}"

      refute match?({:ok, false}, result),
        "sismember now returns {:ok, false} — R3-M5 may be fixed. Got: #{inspect(result)}"
    end

    test "sismember on non-existent key returns bare false" do
      result = FerricStore.sismember("r3m5_nonexistent", "anything")

      assert result === false,
        "sismember on missing key returned: #{inspect(result)}"
    end

    test "sismember return type differs from other set operations" do
      # Demonstrate the inconsistency:
      # sadd, srem, smembers, scard all return {:ok, _}
      # sismember returns bare boolean
      {:ok, 2} = FerricStore.sadd("r3m5_set3", ["a", "b"])

      {:ok, members} = FerricStore.smembers("r3m5_set3")
      assert is_list(members)

      {:ok, count} = FerricStore.scard("r3m5_set3")
      assert is_integer(count)

      # sismember breaks the pattern
      is_member = FerricStore.sismember("r3m5_set3", "a")
      assert is_boolean(is_member), "sismember did not return boolean: #{inspect(is_member)}"

      # If the API were consistent, this would work:
      # {:ok, is_member} = FerricStore.sismember("r3m5_set3", "a")
      # But it doesn't because sismember returns bare boolean.
      refute is_tuple(is_member),
        "sismember should return bare boolean (documenting current behavior)"
    end
  end
end
