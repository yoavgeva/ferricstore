defmodule FerricstoreNebulex.AdapterTest do
  use ExUnit.Case, async: false

  alias FerricstoreNebulex.TestCache

  setup do
    # Flush all keys to prevent cross-test pollution.
    # Nebulex Cache runs in its own GenServer (no sandbox isolation).
    # Double flush: first clears ETS, wait for Raft replays, then clear again.
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    Process.sleep(200)
    Ferricstore.Test.ShardHelpers.flush_all_keys()

    {:ok, pid} = TestCache.start_link()

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: Supervisor.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Basic KV: put / fetch / get
  # ---------------------------------------------------------------------------

  describe "put and fetch" do
    test "stores and retrieves a string value" do
      assert :ok = TestCache.put("key", "value")
      assert {:ok, "value"} = TestCache.fetch("key")
    end

    test "fetch returns KeyError for missing key" do
      assert {:error, %Nebulex.KeyError{key: "missing"}} = TestCache.fetch("missing")
    end

    test "get returns nil for missing key" do
      assert {:ok, nil} = TestCache.get("missing")
    end

    test "get returns default for missing key" do
      assert {:ok, "fallback"} = TestCache.get("missing", "fallback")
    end

    test "put with TTL stores the entry" do
      assert :ok = TestCache.put("ttl_key", "value", ttl: :timer.seconds(60))
      assert {:ok, "value"} = TestCache.fetch("ttl_key")
    end

    test "overwrite existing key" do
      TestCache.put("over", "first")
      TestCache.put("over", "second")
      assert {:ok, "second"} = TestCache.fetch("over")
    end
  end

  # ---------------------------------------------------------------------------
  # delete
  # ---------------------------------------------------------------------------

  describe "delete" do
    test "removes an existing key" do
      TestCache.put("del_key", "value")
      assert :ok = TestCache.delete("del_key")
      assert {:error, %Nebulex.KeyError{}} = TestCache.fetch("del_key")
    end

    test "delete on missing key returns :ok" do
      assert :ok = TestCache.delete("nonexistent")
    end
  end

  # ---------------------------------------------------------------------------
  # has_key?
  # ---------------------------------------------------------------------------

  describe "has_key?" do
    test "returns true for existing key" do
      TestCache.put("exists", "yes")
      assert {:ok, true} = TestCache.has_key?("exists")
    end

    test "returns false for missing key" do
      assert {:ok, false} = TestCache.has_key?("nope")
    end
  end

  # ---------------------------------------------------------------------------
  # put_new / replace
  # ---------------------------------------------------------------------------

  describe "put_new" do
    test "stores value only if key does not exist" do
      assert {:ok, true} = TestCache.put_new("pn", "first")
      assert {:ok, false} = TestCache.put_new("pn", "second")
      assert {:ok, "first"} = TestCache.fetch("pn")
    end
  end

  describe "replace" do
    test "replaces value only if key exists" do
      assert {:ok, false} = TestCache.replace("rp", "value")
      TestCache.put("rp", "original")
      assert {:ok, true} = TestCache.replace("rp", "replaced")
      assert {:ok, "replaced"} = TestCache.fetch("rp")
    end
  end

  # ---------------------------------------------------------------------------
  # put_all / put_new_all
  # ---------------------------------------------------------------------------

  describe "put_all" do
    test "stores multiple entries as list of tuples" do
      assert :ok = TestCache.put_all([{"k1", "v1"}, {"k2", "v2"}])
      assert {:ok, "v1"} = TestCache.fetch("k1")
      assert {:ok, "v2"} = TestCache.fetch("k2")
    end

    test "stores multiple entries as map" do
      assert :ok = TestCache.put_all(%{"mk1" => "mv1", "mk2" => "mv2"})
      assert {:ok, "mv1"} = TestCache.fetch("mk1")
      assert {:ok, "mv2"} = TestCache.fetch("mk2")
    end
  end

  describe "put_new_all" do
    test "stores all entries only if none exist" do
      assert {:ok, true} = TestCache.put_new_all([{"pna1", "v1"}, {"pna2", "v2"}])
      assert {:ok, "v1"} = TestCache.fetch("pna1")
    end

    test "fails if any key already exists" do
      TestCache.put("pna_x1", "existing")
      assert {:ok, false} = TestCache.put_new_all([{"pna_x1", "new"}, {"pna_x2", "v2"}])
      # pna_x2 should NOT have been set
      assert {:error, %Nebulex.KeyError{}} = TestCache.fetch("pna_x2")
    end
  end

  # ---------------------------------------------------------------------------
  # take
  # ---------------------------------------------------------------------------

  describe "take" do
    test "returns value and removes the key" do
      TestCache.put("take_key", "value")
      assert {:ok, "value"} = TestCache.take("take_key")
      assert {:error, %Nebulex.KeyError{}} = TestCache.fetch("take_key")
    end

    test "returns error for missing key" do
      assert {:error, %Nebulex.KeyError{key: "missing"}} = TestCache.take("missing")
    end
  end

  # ---------------------------------------------------------------------------
  # incr / decr (update_counter)
  # ---------------------------------------------------------------------------

  describe "incr/decr" do
    test "increments a counter from zero" do
      assert {:ok, 1} = TestCache.incr("counter")
      assert {:ok, 2} = TestCache.incr("counter")
    end

    test "increments by a custom amount" do
      assert {:ok, 10} = TestCache.incr("counter2", 10)
      assert {:ok, 20} = TestCache.incr("counter2", 10)
    end

    test "decrements a counter" do
      TestCache.incr("counter3", 10)
      assert {:ok, 7} = TestCache.decr("counter3", 3)
    end

    test "decrement from zero goes negative" do
      assert {:ok, -5} = TestCache.decr("counter4", 5)
    end
  end

  # ---------------------------------------------------------------------------
  # TTL / expire / touch
  # ---------------------------------------------------------------------------

  describe "ttl" do
    test "returns remaining TTL for key with expiry" do
      TestCache.put("ttl_check", "v", ttl: :timer.minutes(5))
      assert {:ok, ttl} = TestCache.ttl("ttl_check")
      assert is_integer(ttl) and ttl > 0
      assert ttl <= :timer.minutes(5)
    end

    test "returns :infinity for key without expiry" do
      TestCache.put("no_ttl", "v")
      assert {:ok, :infinity} = TestCache.ttl("no_ttl")
    end

    test "returns error for missing key" do
      assert {:error, %Nebulex.KeyError{}} = TestCache.ttl("missing_ttl")
    end
  end

  describe "expire" do
    test "sets TTL on existing key" do
      TestCache.put("exp_key", "v")
      assert {:ok, true} = TestCache.expire("exp_key", :timer.minutes(10))
      assert {:ok, ttl} = TestCache.ttl("exp_key")
      assert is_integer(ttl) and ttl > 0
    end

    test "returns false for missing key" do
      assert {:ok, false} = TestCache.expire("no_such_key", :timer.minutes(10))
    end
  end

  describe "touch" do
    test "returns true for existing key" do
      TestCache.put("touch_key", "v", ttl: :timer.seconds(60))
      assert {:ok, true} = TestCache.touch("touch_key")
    end

    test "returns false for missing key" do
      assert {:ok, false} = TestCache.touch("touch_missing")
    end
  end

  # ---------------------------------------------------------------------------
  # Queryable: get_all
  # ---------------------------------------------------------------------------

  describe "get_all with :in query" do
    test "returns entries for given keys as list of tuples" do
      TestCache.put("ga1", "v1")
      TestCache.put("ga2", "v2")

      assert {:ok, entries} = TestCache.get_all(in: ["ga1", "ga2", "missing"])
      map = Map.new(entries)
      assert map == %{"ga1" => "v1", "ga2" => "v2"}
    end

    test "returns empty list when no keys exist" do
      assert {:ok, []} = TestCache.get_all(in: ["no1", "no2"])
    end

    test "get_all with select: :key returns only keys" do
      TestCache.put("gk1", "v1")
      TestCache.put("gk2", "v2")

      assert {:ok, keys} = TestCache.get_all(in: ["gk1", "gk2", "missing"], select: :key)
      assert Enum.sort(keys) == ["gk1", "gk2"]
    end

    test "get_all with select: :value returns only values" do
      TestCache.put("gv1", "v1")
      TestCache.put("gv2", "v2")

      assert {:ok, values} = TestCache.get_all(in: ["gv1", "gv2", "missing"], select: :value)
      assert Enum.sort(values) == ["v1", "v2"]
    end
  end

  describe "get_all with query: nil (all entries)" do
    test "returns all entries in the cache" do
      TestCache.put("all1", "v1")
      TestCache.put("all2", "v2")

      assert {:ok, entries} = TestCache.get_all()
      map = Map.new(entries)
      assert map["all1"] == "v1"
      assert map["all2"] == "v2"
    end
  end

  # ---------------------------------------------------------------------------
  # Queryable: count_all
  # ---------------------------------------------------------------------------

  describe "count_all" do
    test "returns count of all entries" do
      TestCache.put("c1", "v")
      TestCache.put("c2", "v")
      assert {:ok, count} = TestCache.count_all()
      assert count >= 2
    end

    test "counts only specified keys with :in" do
      TestCache.put("cin1", "v1")
      TestCache.put("cin2", "v2")

      assert {:ok, 2} = TestCache.count_all(in: ["cin1", "cin2", "cin3"])
    end
  end

  # ---------------------------------------------------------------------------
  # Queryable: delete_all
  # ---------------------------------------------------------------------------

  describe "delete_all" do
    test "deletes all entries and returns count" do
      TestCache.put("da1", "v")
      TestCache.put("da2", "v")
      assert {:ok, deleted} = TestCache.delete_all()
      assert deleted >= 2
      assert {:error, %Nebulex.KeyError{}} = TestCache.fetch("da1")
    end

    test "deletes only specified keys with :in" do
      TestCache.put("din1", "v1")
      TestCache.put("din2", "v2")
      TestCache.put("din3", "v3")

      assert {:ok, 2} = TestCache.delete_all(in: ["din1", "din2"])
      assert {:error, %Nebulex.KeyError{}} = TestCache.fetch("din1")
      assert {:error, %Nebulex.KeyError{}} = TestCache.fetch("din2")
      assert {:ok, "v3"} = TestCache.fetch("din3")
    end
  end

  # ---------------------------------------------------------------------------
  # Queryable: stream
  # ---------------------------------------------------------------------------

  describe "stream" do
    test "returns a lazy stream of key-value entries" do
      TestCache.put("st1", "v1")
      TestCache.put("st2", "v2")

      assert {:ok, stream} = TestCache.stream()
      entries = Enum.to_list(stream)

      keys = Enum.map(entries, fn {k, _v} -> k end)
      assert "st1" in keys
      assert "st2" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # Value serialization (term_to_binary round-trip)
  # ---------------------------------------------------------------------------

  describe "value serialization" do
    test "complex Elixir terms" do
      value = %{name: "alice", scores: [1, 2, 3], nested: %{deep: true}}
      TestCache.put("complex", value)
      assert {:ok, ^value} = TestCache.fetch("complex")
    end

    test "raw binary values" do
      value = <<0, 1, 2, 255, 128>>
      TestCache.put("binary", value)
      assert {:ok, ^value} = TestCache.fetch("binary")
    end

    test "integer values" do
      TestCache.put("int_val", 42)
      assert {:ok, 42} = TestCache.fetch("int_val")
    end

    test "atom values" do
      TestCache.put("atom_val", :hello)
      assert {:ok, :hello} = TestCache.fetch("atom_val")
    end

    test "tuple values" do
      value = {:ok, "data", 123}
      TestCache.put("tuple_val", value)
      assert {:ok, ^value} = TestCache.fetch("tuple_val")
    end

    test "nil value" do
      TestCache.put("nil_key", nil)
      assert {:ok, nil} = TestCache.fetch("nil_key")
    end

    test "large value (100KB)" do
      value = String.duplicate("x", 100_000)
      TestCache.put("large", value)
      assert {:ok, ^value} = TestCache.fetch("large")
    end
  end

  # ---------------------------------------------------------------------------
  # Non-binary keys
  # ---------------------------------------------------------------------------

  describe "non-binary keys" do
    test "atom keys" do
      TestCache.put(:atom_key, "atom_value")
      assert {:ok, "atom_value"} = TestCache.fetch(:atom_key)
    end

    test "integer keys" do
      TestCache.put(42, "int_value")
      assert {:ok, "int_value"} = TestCache.fetch(42)
    end

    test "tuple keys" do
      TestCache.put({:user, 42}, "user_data")
      assert {:ok, "user_data"} = TestCache.fetch({:user, 42})
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent access
  # ---------------------------------------------------------------------------

  describe "concurrent access" do
    test "handles concurrent put/get correctly" do
      namespace = Process.get(:ferricstore_sandbox)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            if namespace, do: Process.put(:ferricstore_sandbox, namespace)
            key = "conc:#{i}"
            TestCache.put(key, "val:#{i}")
            {:ok, result} = TestCache.fetch(key)
            result
          end)
        end

      results = Task.await_many(tasks, 10_000)

      for {result, i} <- Enum.with_index(results, 1) do
        assert result == "val:#{i}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Stats (via info/1)
  # ---------------------------------------------------------------------------

  describe "stats via info" do
    test "returns hit/miss/write counts" do
      TestCache.put("stats_key", "value")
      TestCache.get("stats_key")
      TestCache.get("nonexistent")

      {:ok, stats} = TestCache.info(:stats)
      assert stats.hits >= 1
      assert stats.misses >= 1
      assert stats.writes >= 1
    end

    test "stats increments on successive writes" do
      {:ok, s1} = TestCache.info(:stats)
      TestCache.put("s_key", "v")
      {:ok, s2} = TestCache.info(:stats)
      assert s2.writes > s1.writes
    end

    test "stats tracks deletions" do
      TestCache.put("del_stat", "v")
      {:ok, s1} = TestCache.info(:stats)
      TestCache.delete("del_stat")
      {:ok, s2} = TestCache.info(:stats)
      assert s2.deletions > s1.deletions
    end

    test "stats tracks updates via replace" do
      TestCache.put("upd_stat", "v1")
      {:ok, s1} = TestCache.info(:stats)
      TestCache.replace("upd_stat", "v2")
      {:ok, s2} = TestCache.info(:stats)
      assert s2.updates > s1.updates
    end

    test "info :all returns server and stats" do
      {:ok, all} = TestCache.info(:all)
      assert is_map(all.server)
      assert is_map(all.stats)
      assert Map.has_key?(all.stats, :hits)
      assert Map.has_key?(all.stats, :misses)
      assert Map.has_key?(all.stats, :writes)
    end
  end

  # ---------------------------------------------------------------------------
  # Transaction
  # ---------------------------------------------------------------------------

  describe "transaction" do
    test "executes function and returns result" do
      result =
        TestCache.transaction(fn ->
          TestCache.put("tx_key", "value")
          {:ok, val} = TestCache.get("tx_key")
          val
        end)

      assert {:ok, "value"} = result
    end

    test "persists writes made inside transaction" do
      TestCache.transaction(fn ->
        TestCache.put("tx_persist", "data")
      end)

      assert {:ok, "data"} = TestCache.fetch("tx_persist")
    end

    test "propagates exceptions from transaction function" do
      assert_raise RuntimeError, "boom", fn ->
        TestCache.transaction(fn ->
          raise "boom"
        end)
      end
    end

    test "in_transaction? returns false outside transaction" do
      assert {:ok, false} = TestCache.in_transaction?()
    end

    test "in_transaction? returns true inside transaction" do
      TestCache.transaction(fn ->
        assert {:ok, true} = TestCache.in_transaction?()
      end)
    end

    test "in_transaction? resets after transaction completes" do
      TestCache.transaction(fn -> :ok end)
      assert {:ok, false} = TestCache.in_transaction?()
    end

    test "in_transaction? resets after transaction raises" do
      catch_error(
        TestCache.transaction(fn ->
          raise "fail"
        end)
      )

      assert {:ok, false} = TestCache.in_transaction?()
    end

    test "nested transactions work" do
      result =
        TestCache.transaction(fn ->
          TestCache.put("outer", "1")

          {:ok, inner_result} =
            TestCache.transaction(fn ->
              TestCache.put("inner", "2")
              {:ok, val} = TestCache.get("inner")
              val
            end)

          inner_result
        end)

      assert {:ok, "2"} = result
      assert {:ok, "1"} = TestCache.fetch("outer")
      assert {:ok, "2"} = TestCache.fetch("inner")
    end
  end
end
