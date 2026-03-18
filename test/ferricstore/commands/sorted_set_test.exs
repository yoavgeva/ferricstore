defmodule Ferricstore.Commands.SortedSetTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # ZADD
  # ---------------------------------------------------------------------------

  describe "ZADD" do
    test "ZADD adds new members and returns count" do
      store = MockStore.make()
      assert 3 == SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
    end

    test "ZADD updating existing member returns 0" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
    end

    test "ZADD with NX only adds new members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 1 == SortedSet.handle("ZADD", ["zs", "NX", "5.0", "a", "2.0", "b"], store)
      # a should still have score 1.0
      assert "1.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD with XX only updates existing members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "XX", "5.0", "a", "2.0", "b"], store)
      # a should be updated to 5.0
      assert "5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      # b should not exist
      assert nil == SortedSet.handle("ZSCORE", ["zs", "b"], store)
    end

    test "ZADD with CH returns count of changed (added + updated)" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      # Adding b (new) + updating a (changed) = 2
      assert 2 == SortedSet.handle("ZADD", ["zs", "CH", "5.0", "a", "2.0", "b"], store)
    end

    test "ZADD with GT only updates if new score > current" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      SortedSet.handle("ZADD", ["zs", "GT", "3.0", "a"], store)
      # Should stay at 5.0 since 3.0 < 5.0
      assert "5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      SortedSet.handle("ZADD", ["zs", "GT", "10.0", "a"], store)
      assert "10.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD with integer scores" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "5", "a"], store)
    end

    test "ZADD with invalid score returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZADD", ["zs", "abc", "a"], store)
    end

    test "ZADD with odd score/member count returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZADD", ["zs", "1.0"], store)
    end

    test "ZADD with no args returns error" do
      assert {:error, _} = SortedSet.handle("ZADD", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZSCORE
  # ---------------------------------------------------------------------------

  describe "ZSCORE" do
    test "ZSCORE returns score string" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.14", "pi"], store)
      assert "3.14" == SortedSet.handle("ZSCORE", ["zs", "pi"], store)
    end

    test "ZSCORE returns nil for missing member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "missing"], store)
    end

    test "ZSCORE returns nil for nonexistent key" do
      assert nil == SortedSet.handle("ZSCORE", ["nonexistent", "a"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANK
  # ---------------------------------------------------------------------------

  describe "ZRANK" do
    test "ZRANK returns rank (0-indexed) by ascending score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      assert 0 == SortedSet.handle("ZRANK", ["zs", "a"], store)
      assert 1 == SortedSet.handle("ZRANK", ["zs", "b"], store)
      assert 2 == SortedSet.handle("ZRANK", ["zs", "c"], store)
    end

    test "ZRANK returns nil for missing member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert nil == SortedSet.handle("ZRANK", ["zs", "missing"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGE
  # ---------------------------------------------------------------------------

  describe "ZRANGE" do
    test "ZRANGE returns members by rank" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      assert ["a", "b", "c"] == SortedSet.handle("ZRANGE", ["zs", "0", "-1"], store)
    end

    test "ZRANGE with sub-range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert ["b", "c"] == SortedSet.handle("ZRANGE", ["zs", "1", "2"], store)
    end

    test "ZRANGE WITHSCORES returns members and scores" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.5", "a", "2.5", "b"], store)
      result = SortedSet.handle("ZRANGE", ["zs", "0", "-1", "WITHSCORES"], store)
      assert length(result) == 4
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZRANGE on nonexistent key returns empty list" do
      assert [] == SortedSet.handle("ZRANGE", ["nonexistent", "0", "-1"], MockStore.make())
    end

    test "ZRANGE with start > stop returns empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert [] == SortedSet.handle("ZRANGE", ["zs", "5", "1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGE
  # ---------------------------------------------------------------------------

  describe "ZREVRANGE" do
    test "ZREVRANGE returns members in descending score order" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert ["c", "b", "a"] == SortedSet.handle("ZREVRANGE", ["zs", "0", "-1"], store)
    end

    test "ZREVRANGE with sub-range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert ["c", "b"] == SortedSet.handle("ZREVRANGE", ["zs", "0", "1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZCARD
  # ---------------------------------------------------------------------------

  describe "ZCARD" do
    test "ZCARD returns sorted set cardinality" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert 3 == SortedSet.handle("ZCARD", ["zs"], store)
    end

    test "ZCARD returns 0 for nonexistent key" do
      assert 0 == SortedSet.handle("ZCARD", ["nonexistent"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZREM
  # ---------------------------------------------------------------------------

  describe "ZREM" do
    test "ZREM removes member and returns 1" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 1 == SortedSet.handle("ZREM", ["zs", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZREM on missing member returns 0" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZREM", ["zs", "missing"], store)
    end

    test "ZREM cleans up type metadata when sorted set becomes empty" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      SortedSet.handle("ZREM", ["zs", "a"], store)
      assert nil == store.compound_get.("zs", "T:zs")
    end
  end

  # ---------------------------------------------------------------------------
  # ZINCRBY
  # ---------------------------------------------------------------------------

  describe "ZINCRBY" do
    test "ZINCRBY increments score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      result = SortedSet.handle("ZINCRBY", ["zs", "3.0", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 8.0, val, 0.001
    end

    test "ZINCRBY creates member if missing" do
      store = MockStore.make()
      result = SortedSet.handle("ZINCRBY", ["zs", "5.0", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 5.0, val, 0.001
    end

    test "ZINCRBY with integer increment" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      result = SortedSet.handle("ZINCRBY", ["zs", "3", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 8.0, val, 0.001
    end

    test "ZINCRBY with invalid increment returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZINCRBY", ["zs", "abc", "a"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZCOUNT
  # ---------------------------------------------------------------------------

  describe "ZCOUNT" do
    test "ZCOUNT with inclusive range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert 3 == SortedSet.handle("ZCOUNT", ["zs", "1.0", "3.0"], store)
    end

    test "ZCOUNT with -inf to +inf returns all" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 2 == SortedSet.handle("ZCOUNT", ["zs", "-inf", "+inf"], store)
    end

    test "ZCOUNT with exclusive bounds" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      # (1.0 to (3.0 = only score 2.0
      assert 1 == SortedSet.handle("ZCOUNT", ["zs", "(1.0", "(3.0"], store)
    end

    test "ZCOUNT on nonexistent key returns 0" do
      assert 0 == SortedSet.handle("ZCOUNT", ["nonexistent", "-inf", "+inf"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZPOPMIN and ZPOPMAX
  # ---------------------------------------------------------------------------

  describe "ZPOPMIN" do
    test "ZPOPMIN returns member with lowest score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      result = SortedSet.handle("ZPOPMIN", ["zs"], store)
      assert Enum.at(result, 0) == "a"
    end

    test "ZPOPMIN removes the member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      SortedSet.handle("ZPOPMIN", ["zs"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZPOPMIN with count returns multiple members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      result = SortedSet.handle("ZPOPMIN", ["zs", "2"], store)
      # Should return [member1, score1, member2, score2]
      assert length(result) == 4
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZPOPMIN on empty key returns empty list" do
      store = MockStore.make()
      assert [] == SortedSet.handle("ZPOPMIN", ["nonexistent"], store)
    end
  end

  describe "ZPOPMAX" do
    test "ZPOPMAX returns member with highest score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      result = SortedSet.handle("ZPOPMAX", ["zs"], store)
      assert Enum.at(result, 0) == "c"
    end

    test "ZPOPMAX removes the member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "3.0", "c"], store)
      SortedSet.handle("ZPOPMAX", ["zs"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "c"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Type enforcement
  # ---------------------------------------------------------------------------

  describe "type enforcement" do
    test "ZADD on a key used as hash returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZADD", ["mykey", "1.0", "a"], store)
    end

    test "ZRANGE on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "set", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZRANGE", ["mykey", "0", "-1"], store)
    end
  end
end
