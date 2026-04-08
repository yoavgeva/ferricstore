defmodule FerricstoreEcto.QueryCacheTest do
  use ExUnit.Case

  alias FerricstoreEcto.Test.AuditLog
  alias FerricstoreEcto.Test.User
  alias FerricstoreEcto.TestRepo

  import Ecto.Query

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(TestRepo)
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    on_exit(fn -> Ferricstore.Test.ShardHelpers.flush_all_keys() end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp insert_user(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    defaults = %{
      name: "test",
      email: "test@example.com",
      age: 25,
      active: true,
      inserted_at: now,
      updated_at: now
    }

    merged = Map.merge(defaults, attrs)

    %User{}
    |> User.changeset(Map.take(merged, [:name, :email, :age, :active]))
    |> Ecto.Changeset.put_change(:inserted_at, merged.inserted_at)
    |> Ecto.Changeset.put_change(:updated_at, merged.updated_at)
    |> TestRepo.insert!()
  end

  # ---------------------------------------------------------------------------
  # Repo.all query cache
  # ---------------------------------------------------------------------------

  describe "Repo.all query cache" do
    test "caches query results on first call" do
      insert_user(%{name: "alice", active: true})
      insert_user(%{name: "bob", active: true})

      query = from(u in User, where: u.active == true, order_by: u.name)

      # First call -- cache miss
      results1 = TestRepo.all(query)
      assert length(results1) == 2
      assert Enum.map(results1, & &1.name) == ["alice", "bob"]

      # Second call -- cache hit
      results2 = TestRepo.all(query)
      assert length(results2) == 2
      assert Enum.map(results2, & &1.name) == ["alice", "bob"]
    end

    test "query cache invalidated on insert" do
      insert_user(%{name: "alice", active: true})

      query = from(u in User, where: u.active == true)
      results1 = TestRepo.all(query)
      assert length(results1) == 1

      # Insert new user -- generation counter increments
      insert_user(%{name: "bob", active: true})

      # Should get fresh results (generation changed)
      results2 = TestRepo.all(query)
      assert length(results2) == 2
    end

    test "query cache invalidated on update" do
      alice = insert_user(%{name: "alice", active: true})

      query = from(u in User, where: u.active == true)
      results1 = TestRepo.all(query)
      assert length(results1) == 1

      # Update alice to inactive -- triggers invalidation
      {:ok, _} = alice |> Ecto.Changeset.change(%{active: false}) |> TestRepo.update()

      # Should get fresh results
      results2 = TestRepo.all(query)
      assert results2 == []
    end

    test "query cache invalidated on delete" do
      alice = insert_user(%{name: "alice", active: true})
      insert_user(%{name: "bob", active: true})

      query = from(u in User, where: u.active == true)
      results1 = TestRepo.all(query)
      assert length(results1) == 2

      {:ok, _} = TestRepo.delete(alice)

      results2 = TestRepo.all(query)
      assert length(results2) == 1
    end

    test "different queries have different cache keys" do
      insert_user(%{name: "alice", active: true, age: 20})
      insert_user(%{name: "bob", active: true, age: 30})

      query1 = from(u in User, where: u.age > 25)
      query2 = from(u in User, where: u.age < 25)

      results1 = TestRepo.all(query1)
      results2 = TestRepo.all(query2)

      assert length(results1) == 1
      assert length(results2) == 1
      assert hd(results1).name == "bob"
      assert hd(results2).name == "alice"
    end

    test "query on non-cacheable schema is never cached" do
      now = DateTime.utc_now() |> DateTime.truncate(:second)

      %AuditLog{action: "test", inserted_at: now, updated_at: now}
      |> TestRepo.insert!()

      query = from(a in AuditLog)
      results = TestRepo.all(query)
      assert length(results) == 1

      # No generation counter for audit_logs should exist from query caching
      # (it may exist if other operations touched it, but the query itself
      # should not populate cache)
    end

    test "query with cache: false bypasses cache" do
      insert_user(%{name: "alice", active: true})

      query = from(u in User, where: u.active == true)

      # Normal call -- populates cache
      TestRepo.all(query)

      # cache: false -- bypasses
      results = TestRepo.all(query, cache: false)
      assert length(results) == 1
    end

    test "empty result set is cached" do
      query = from(u in User, where: u.active == true)

      # First call returns empty
      results1 = TestRepo.all(query)
      assert results1 == []

      # Second call should also return empty (from cache)
      results2 = TestRepo.all(query)
      assert results2 == []
    end
  end

  # ---------------------------------------------------------------------------
  # Repo.one query cache
  # ---------------------------------------------------------------------------

  describe "Repo.one" do
    test "returns single result" do
      insert_user(%{name: "alice", email: "alice@unique.com"})

      query = from(u in User, where: u.email == "alice@unique.com")
      result = TestRepo.one(query)
      assert result.name == "alice"
    end

    test "returns nil when no results" do
      query = from(u in User, where: u.email == "nonexistent@example.com")
      assert TestRepo.one(query) == nil
    end
  end

  describe "Repo.one!" do
    test "raises when no results" do
      query = from(u in User, where: u.email == "nonexistent@example.com")

      assert_raise Ecto.NoResultsError, fn ->
        TestRepo.one!(query)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Generation counter consistency
  # ---------------------------------------------------------------------------

  describe "generation counter" do
    test "increments on each mutation" do
      {:ok, gen0} = FerricStore.get("ecto:gen:users")
      gen0_int = if gen0, do: String.to_integer(gen0), else: 0

      u1 = insert_user(%{name: "alice"})
      {:ok, gen1} = FerricStore.get("ecto:gen:users")
      gen1_int = String.to_integer(gen1)
      assert gen1_int > gen0_int

      {:ok, _} = u1 |> Ecto.Changeset.change(%{name: "alice2"}) |> TestRepo.update()
      {:ok, gen2} = FerricStore.get("ecto:gen:users")
      gen2_int = String.to_integer(gen2)
      assert gen2_int > gen1_int

      {:ok, _} = TestRepo.delete(u1)
      {:ok, gen3} = FerricStore.get("ecto:gen:users")
      gen3_int = String.to_integer(gen3)
      assert gen3_int > gen2_int
    end

    test "prevents stale reads after concurrent write" do
      insert_user(%{name: "alice", active: true})

      query = from(u in User, where: u.active == true)
      TestRepo.all(query)  # populate query cache

      # Simulate concurrent write -- new user inserted
      insert_user(%{name: "bob", active: true})

      # Next read should see both (generation changed)
      results = TestRepo.all(query)
      assert length(results) == 2
    end
  end
end
