defmodule Ferricstore.Store.RouterTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Router

  describe "shard_for/1" do
    test "returns integer in valid range" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert Router.shard_for("key") in 0..(shard_count - 1)
    end

    test "same key always maps to same shard" do
      assert Router.shard_for("hello") == Router.shard_for("hello")
    end

    test "empty binary key works" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert Router.shard_for("") in 0..(shard_count - 1)
    end

    test "large key works" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      big_key = String.duplicate("x", 10_000)
      assert Router.shard_for(big_key) in 0..(shard_count - 1)
    end

    test "hash tags co-locate keys on the same shard" do
      assert Router.shard_for("{user:42}:session") == Router.shard_for("{user:42}:profile")
    end
  end

  describe "slot_for/1" do
    test "returns integer in 0..1023" do
      assert Router.slot_for("key") in 0..1023
    end

    test "same key always maps to same slot" do
      assert Router.slot_for("hello") == Router.slot_for("hello")
    end

    test "hash tags co-locate keys on the same slot" do
      assert Router.slot_for("{tag}:a") == Router.slot_for("{tag}:b")
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

    test "returns expected format" do
      assert Router.shard_name(0) == :"Ferricstore.Store.Shard.0"
      assert Router.shard_name(7) == :"Ferricstore.Store.Shard.7"
    end
  end
end
