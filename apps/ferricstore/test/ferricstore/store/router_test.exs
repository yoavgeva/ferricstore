defmodule Ferricstore.Store.RouterTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Router

  describe "shard_for/2" do
    test "returns integer in valid range" do
      assert Router.shard_for("key", 4) in 0..3
    end

    test "same key always maps to same shard" do
      assert Router.shard_for("hello", 4) == Router.shard_for("hello", 4)
    end

    test "different shard counts may map key differently" do
      # with different counts, at least some keys will differ
      results_4 = Enum.map(1..20, fn i -> Router.shard_for("key#{i}", 4) end)
      results_8 = Enum.map(1..20, fn i -> Router.shard_for("key#{i}", 8) end)
      assert results_4 != results_8
    end

    test "empty binary key works" do
      assert Router.shard_for("", 4) in 0..3
    end

    test "large key works" do
      big_key = String.duplicate("x", 10_000)
      assert Router.shard_for(big_key, 4) in 0..3
    end

    test "shard_for with default count returns valid index" do
      assert Router.shard_for("test_key") in 0..3
    end
  end

  describe "shard_name/1" do
    test "returns unique atoms per index" do
      names = Enum.map(0..3, &Router.shard_name/1)
      assert length(Enum.uniq(names)) == 4
    end

    test "returns atoms" do
      assert is_atom(Router.shard_name(0))
    end
  end
end
