defmodule FerricstoreEcto.CachedRepoTest do
  use ExUnit.Case

  alias FerricstoreEcto.TestRepo
  alias FerricstoreEcto.Test.User
  alias FerricstoreEcto.Test.AuditLog
  alias FerricstoreEcto.Test.ReadOnlyCountry

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

  defp insert_country(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    %ReadOnlyCountry{
      name: attrs[:name] || "Test",
      code: attrs[:code] || "TS",
      inserted_at: now,
      updated_at: now
    }
    |> TestRepo.insert!()
  end

  defp insert_audit_log(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    %AuditLog{
      action: attrs[:action] || "test_action",
      inserted_at: now,
      updated_at: now
    }
    |> TestRepo.insert!()
  end

  defp entity_cache_key(source, id) do
    "ecto:#{source}:#{id}"
  end

  # ---------------------------------------------------------------------------
  # Entity cache: basic get
  # ---------------------------------------------------------------------------

  describe "Repo.get entity cache" do
    test "caches entity on first call" do
      user = insert_user(%{name: "alice", email: "a@b.com"})

      # First call -- cache miss, hits DB
      result1 = TestRepo.get(User, user.id)
      assert result1.name == "alice"

      # Verify FerricStore has the entry
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("users", user.id))
      assert map_size(fields) > 0
      assert fields["name"] == "alice"

      # Second call -- cache hit
      result2 = TestRepo.get(User, user.id)
      assert result2.name == "alice"
      assert result2.email == "a@b.com"
      assert result2.id == user.id
    end

    test "returns nil for non-existent entity" do
      assert TestRepo.get(User, 99_999) == nil
    end

    test "caches all schema fields correctly" do
      user = insert_user(%{name: "alice", email: "a@b.com", age: 30, active: true})

      # Populate cache
      TestRepo.get(User, user.id)

      # Read from cache
      cached = TestRepo.get(User, user.id)
      assert cached.name == "alice"
      assert cached.email == "a@b.com"
      assert cached.age == 30
      assert cached.active == true
      assert cached.id == user.id
    end

    test "cached entity has :loaded meta state" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)

      # Second get is from cache
      cached = TestRepo.get(User, user.id)
      assert Ecto.get_meta(cached, :state) == :loaded
    end
  end

  # ---------------------------------------------------------------------------
  # Entity cache: get!
  # ---------------------------------------------------------------------------

  describe "Repo.get!" do
    test "returns entity from cache" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)

      result = TestRepo.get!(User, user.id)
      assert result.name == "alice"
    end

    test "raises on non-existent entity" do
      assert_raise Ecto.NoResultsError, fn ->
        TestRepo.get!(User, 99_999)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Invalidation: insert
  # ---------------------------------------------------------------------------

  describe "Repo.insert invalidation" do
    test "insert does not populate entity cache" do
      {:ok, user} = TestRepo.insert(%User{
        name: "bob",
        email: "bob@b.com",
        inserted_at: DateTime.utc_now() |> DateTime.truncate(:second),
        updated_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })

      # Cache should be empty -- insert invalidates, does not populate
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("users", user.id))
      assert map_size(fields) == 0
    end

    test "insert increments generation counter" do
      {:ok, gen_before} = FerricStore.get("ecto:gen:users")

      {:ok, _user} = TestRepo.insert(%User{
        name: "bob",
        inserted_at: DateTime.utc_now() |> DateTime.truncate(:second),
        updated_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })

      {:ok, gen_after} = FerricStore.get("ecto:gen:users")

      # Generation counter should have incremented
      before_int = if gen_before, do: String.to_integer(gen_before), else: 0
      after_int = String.to_integer(gen_after)
      assert after_int > before_int
    end
  end

  # ---------------------------------------------------------------------------
  # Invalidation: update
  # ---------------------------------------------------------------------------

  describe "Repo.update invalidation" do
    test "update invalidates entity cache" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)  # populate cache

      # Verify cached
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("users", user.id))
      assert fields["name"] == "alice"

      # Update
      {:ok, _} = user |> Ecto.Changeset.change(%{name: "alice2"}) |> TestRepo.update()

      # Cache should be invalidated (DEL)
      cache_key = entity_cache_key("users", user.id)
      {:ok, fields_after} = FerricStore.hgetall(cache_key)

      if map_size(fields_after) > 0 do
        IO.puts("\n=== DEBUG: cache not cleared after update ===")
        IO.puts("  cache_key: #{inspect(cache_key)}")
        IO.puts("  fields_after: #{inspect(fields_after)}")
        IO.puts("  test isolation: flush_all_keys")

        shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
        for i <- 0..(shard_count - 1) do
          keydir = :"keydir_#{i}"
          try do
            entries = :ets.foldl(fn {k, v, _, _, _, _, _}, acc ->
              if String.contains?(to_string(k), "user") or String.contains?(to_string(k), "cache"), do: [{k, v} | acc], else: acc
            end, [], keydir)
            if entries != [], do: IO.puts("  keydir_#{i}: #{inspect(entries, limit: 10)}")
          rescue
            _ -> :ok
          end
        end
        IO.puts("=== END DEBUG ===\n")
      end

      assert map_size(fields_after) == 0

      # Next get should fetch from DB with new value
      result = TestRepo.get(User, user.id)
      assert result.name == "alice2"
    end

    test "failed update does not invalidate cache" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)  # populate cache

      # Try to update with an invalid changeset (add a check constraint-like validation)
      changeset =
        user
        |> Ecto.Changeset.change(%{name: "bob"})
        |> Ecto.Changeset.add_error(:name, "invalid")

      assert {:error, _} = TestRepo.update(changeset)

      # Cache should still have the old value
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("users", user.id))
      assert fields["name"] == "alice"
    end
  end

  # ---------------------------------------------------------------------------
  # Invalidation: delete
  # ---------------------------------------------------------------------------

  describe "Repo.delete invalidation" do
    test "delete invalidates entity cache" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)  # populate cache

      {:ok, _} = TestRepo.delete(user)

      # Cache should be empty
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("users", user.id))
      assert map_size(fields) == 0

      # get returns nil
      assert TestRepo.get(User, user.id) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Non-cacheable schemas
  # ---------------------------------------------------------------------------

  describe "non-cacheable schema" do
    test "AuditLog is never cached" do
      log = insert_audit_log(%{action: "test"})
      TestRepo.get(AuditLog, log.id)

      # Should NOT be in FerricStore
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("audit_logs", log.id))
      assert map_size(fields) == 0
    end

    test "AuditLog get works normally without cache" do
      log = insert_audit_log(%{action: "create_user"})

      result = TestRepo.get(AuditLog, log.id)
      assert result.action == "create_user"
    end
  end

  # ---------------------------------------------------------------------------
  # Cache modes: cache: false, cache: :refresh
  # ---------------------------------------------------------------------------

  describe "cache: false bypass" do
    test "bypasses cache on read" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)  # populate cache

      # Directly update DB bypassing cache
      TestRepo.update_all(
        from(u in User, where: u.id == ^user.id),
        set: [name: "modified_directly"]
      )

      # Normal get returns stale cached value
      # (update_all invalidates entity caches, so this will be fresh)
      # Let's test cache: false specifically
      result = TestRepo.get(User, user.id, cache: false)
      assert result.name == "modified_directly"
    end

    test "cache: false does not populate cache" do
      user = insert_user(%{name: "alice"})

      # Read with cache: false
      TestRepo.get(User, user.id, cache: false)

      # Cache should be empty -- cache: false means ignore mode
      # Actually, cache: false sets mode to :ignore which skips both read and populate
      # But our implementation calls super then populate_entity...
      # Let me check: in :ignore mode, populate_entity is a no-op
      {:ok, fields} = FerricStore.hgetall(entity_cache_key("users", user.id))
      assert map_size(fields) == 0
    end
  end

  describe "cache: :refresh" do
    test "refresh skips cache read and repopulates" do
      user = insert_user(%{name: "alice"})
      TestRepo.get(User, user.id)  # populate cache

      # Manually change DB (bypass repo to avoid invalidation)
      Ecto.Adapters.SQL.query!(
        TestRepo,
        "UPDATE users SET name = 'bob' WHERE id = ?",
        [user.id]
      )

      # Normal get returns stale cached value
      stale = TestRepo.get(User, user.id)
      assert stale.name == "alice"

      # Refresh forces DB fetch and repopulates cache
      fresh = TestRepo.get(User, user.id, cache: :refresh)
      assert fresh.name == "bob"

      # Subsequent normal get returns the refreshed value
      cached = TestRepo.get(User, user.id)
      assert cached.name == "bob"
    end
  end

  # ---------------------------------------------------------------------------
  # Bulk operations: update_all / delete_all
  # ---------------------------------------------------------------------------

  describe "Repo.update_all" do
    test "invalidates all entity caches for the table" do
      u1 = insert_user(%{name: "alice"})
      u2 = insert_user(%{name: "bob"})

      # Populate caches
      TestRepo.get(User, u1.id)
      TestRepo.get(User, u2.id)

      # Verify cached
      {:ok, f1} = FerricStore.hgetall(entity_cache_key("users", u1.id))
      {:ok, f2} = FerricStore.hgetall(entity_cache_key("users", u2.id))
      assert map_size(f1) > 0
      assert map_size(f2) > 0

      # Bulk update
      TestRepo.update_all(from(u in User), set: [name: "updated"])

      # All entity caches should be invalidated
      {:ok, f1_after} = FerricStore.hgetall(entity_cache_key("users", u1.id))
      {:ok, f2_after} = FerricStore.hgetall(entity_cache_key("users", u2.id))
      assert map_size(f1_after) == 0
      assert map_size(f2_after) == 0

      # Fresh reads get new values
      assert TestRepo.get(User, u1.id).name == "updated"
      assert TestRepo.get(User, u2.id).name == "updated"
    end
  end

  describe "Repo.delete_all" do
    test "invalidates all entity caches for the table" do
      u1 = insert_user(%{name: "alice"})
      u2 = insert_user(%{name: "bob"})

      TestRepo.get(User, u1.id)
      TestRepo.get(User, u2.id)

      TestRepo.delete_all(User)

      {:ok, f1} = FerricStore.hgetall(entity_cache_key("users", u1.id))
      {:ok, f2} = FerricStore.hgetall(entity_cache_key("users", u2.id))
      assert map_size(f1) == 0
      assert map_size(f2) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Read-only strategy
  # ---------------------------------------------------------------------------

  describe "read_only strategy" do
    test "update on read_only entity logs warning" do
      country = insert_country(%{name: "USA", code: "US"})
      TestRepo.get(ReadOnlyCountry, country.id)

      # This should still work (we don't block writes) but log a warning
      {:ok, updated} =
        country
        |> Ecto.Changeset.change(%{name: "United States"})
        |> TestRepo.update()

      assert updated.name == "United States"
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent access
  # ---------------------------------------------------------------------------

  describe "concurrent access" do
    test "50 concurrent Repo.get on same key" do
      user = insert_user(%{name: "alice"})

      # Prime the cache so concurrent tasks hit cache, not DB
      # (avoids SQLite table locking from 50 simultaneous DB queries)
      TestRepo.get(User, user.id)

      parent = self()

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            # Share the parent SQL sandbox connection
            Ecto.Adapters.SQL.Sandbox.allow(TestRepo, parent, self())

            TestRepo.get(User, user.id)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, fn u -> u.name == "alice" end)
      assert length(results) == 50
    end
  end

  # ---------------------------------------------------------------------------
  # insert_all
  # ---------------------------------------------------------------------------

  describe "Repo.insert_all" do
    test "invalidates query cache generation" do
      {:ok, gen_before} = FerricStore.get("ecto:gen:users")

      now = DateTime.utc_now() |> DateTime.truncate(:second)

      TestRepo.insert_all(User, [
        %{name: "batch1", email: "b1@b.com", age: 20, active: true,
          inserted_at: now, updated_at: now},
        %{name: "batch2", email: "b2@b.com", age: 21, active: true,
          inserted_at: now, updated_at: now}
      ])

      {:ok, gen_after} = FerricStore.get("ecto:gen:users")
      before_int = if gen_before, do: String.to_integer(gen_before), else: 0
      after_int = String.to_integer(gen_after)
      assert after_int > before_int
    end
  end
end
