defmodule Ferricstore.Review.M8SmoveTypeRaceTest do
  @moduledoc """
  Proves the SMOVE type enforcement gap in `Commands.Set`.

  The bug (set.ex lines 335-354): SMOVE checks the destination type with
  `check_type` (read-only, does not set), then deletes the member from source,
  then calls `check_or_set` on destination — whose return value is IGNORED.

  Two consequences:

  1. **TOCTOU gap** — Between the `check_type` pass (destination doesn't exist)
     and the `check_or_set` call (line 348), another client can create the
     destination as a different type. The unchecked `check_or_set` error means
     the member is already gone from source but the destination write on line 350
     bypasses type safety.

  2. **Ignored error** — Even if `check_or_set` returns `{:error, WRONGTYPE}`,
     the code falls through to `compound_put` on line 350 and writes anyway.

  These tests verify the observable type-safety behavior: SMOVE to a wrong-type
  destination must return WRONGTYPE and must NOT delete the member from source.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Set
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Test.MockStore

  describe "SMOVE type enforcement" do
    test "SMOVE to a hash destination returns WRONGTYPE and preserves source" do
      store = MockStore.make()

      # Source: a set with member "x"
      Set.handle("SADD", ["src", "x", "y"], store)

      # Destination: a hash (different type)
      Hash.handle("HSET", ["dst", "field1", "value1"], store)

      # SMOVE should fail with WRONGTYPE
      result = Set.handle("SMOVE", ["src", "dst", "x"], store)
      assert {:error, "WRONGTYPE" <> _} = result

      # Source member "x" must still be present — not deleted
      assert 1 == Set.handle("SISMEMBER", ["src", "x"], store),
             "Source member 'x' was deleted despite WRONGTYPE error on destination"

      # Source cardinality unchanged
      assert 2 == Set.handle("SCARD", ["src"], store)
    end

    test "SMOVE to a non-existent destination succeeds and creates the set" do
      store = MockStore.make()

      Set.handle("SADD", ["src", "a", "b"], store)

      # Destination does not exist — SMOVE should create it
      assert 1 == Set.handle("SMOVE", ["src", "newdst", "a"], store)

      # "a" moved out of source
      assert 0 == Set.handle("SISMEMBER", ["src", "a"], store)
      assert 1 == Set.handle("SCARD", ["src"], store)

      # "a" now in destination
      assert 1 == Set.handle("SISMEMBER", ["newdst", "a"], store)
      assert 1 == Set.handle("SCARD", ["newdst"], store)
    end

    test "SMOVE when source member does not exist returns 0" do
      store = MockStore.make()

      Set.handle("SADD", ["src", "a"], store)
      Set.handle("SADD", ["dst", "z"], store)

      assert 0 == Set.handle("SMOVE", ["src", "dst", "missing"], store)

      # Neither set is modified
      assert 1 == Set.handle("SCARD", ["src"], store)
      assert 1 == Set.handle("SCARD", ["dst"], store)
    end

    test "SMOVE from a non-set source returns WRONGTYPE" do
      store = MockStore.make()

      Hash.handle("HSET", ["src", "f", "v"], store)
      Set.handle("SADD", ["dst", "z"], store)

      result = Set.handle("SMOVE", ["src", "dst", "f"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end
  end
end
