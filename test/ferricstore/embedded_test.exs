defmodule Ferricstore.EmbeddedTest do
  @moduledoc """
  Comprehensive tests for the FerricStore embedded mode direct Elixir API.

  Tests cover strings (set/get/del), INCR/INCRBY, hash operations (hset/hget/hgetall),
  TTL management (expire/ttl), and pipeline batching. All tests use the sandbox
  for isolation and run with async: true.
  """
  use ExUnit.Case, async: true
  use FerricStore.Sandbox.Case

  # ===========================================================================
  # SET / GET — basic string operations
  # ===========================================================================

  describe "set/3 and get/1" do
    test "sets and gets a simple string value" do
      assert :ok = FerricStore.set("user:42:name", "alice")
      assert {:ok, "alice"} = FerricStore.get("user:42:name")
    end

    test "get returns {:ok, nil} for nonexistent key" do
      assert {:ok, nil} = FerricStore.get("nonexistent")
    end

    test "set overwrites existing value" do
      assert :ok = FerricStore.set("key", "first")
      assert :ok = FerricStore.set("key", "second")
      assert {:ok, "second"} = FerricStore.get("key")
    end

    test "set with empty string key and value" do
      assert :ok = FerricStore.set("", "")
      assert {:ok, ""} = FerricStore.get("")
    end

    test "set preserves binary data with null bytes" do
      value = <<0, 1, 2, 255, 0, 128>>
      assert :ok = FerricStore.set("binary_key", value)
      assert {:ok, ^value} = FerricStore.get("binary_key")
    end

    test "set with large value (100KB)" do
      large = String.duplicate("x", 100_000)
      assert :ok = FerricStore.set("large", large)
      assert {:ok, ^large} = FerricStore.get("large")
    end

    test "set with TTL option" do
      assert :ok = FerricStore.set("ttl_key", "value", ttl: :timer.hours(1))
      assert {:ok, "value"} = FerricStore.get("ttl_key")
    end

    test "set with TTL 0 means no expiry" do
      assert :ok = FerricStore.set("no_expire", "value", ttl: 0)
      assert {:ok, "value"} = FerricStore.get("no_expire")
    end

    test "set returns :ok" do
      result = FerricStore.set("k", "v")
      assert result == :ok
    end

    test "multiple keys can be set and retrieved independently" do
      assert :ok = FerricStore.set("a", "1")
      assert :ok = FerricStore.set("b", "2")
      assert :ok = FerricStore.set("c", "3")

      assert {:ok, "1"} = FerricStore.get("a")
      assert {:ok, "2"} = FerricStore.get("b")
      assert {:ok, "3"} = FerricStore.get("c")
    end
  end

  # ===========================================================================
  # DEL — key deletion
  # ===========================================================================

  describe "del/1" do
    test "deletes an existing key" do
      FerricStore.set("to_delete", "value")
      assert :ok = FerricStore.del("to_delete")
      assert {:ok, nil} = FerricStore.get("to_delete")
    end

    test "del on nonexistent key returns :ok" do
      assert :ok = FerricStore.del("never_existed")
    end

    test "del then set creates fresh key" do
      FerricStore.set("cycle", "first")
      FerricStore.del("cycle")
      FerricStore.set("cycle", "second")
      assert {:ok, "second"} = FerricStore.get("cycle")
    end
  end

  # ===========================================================================
  # INCR / INCRBY — integer increment
  # ===========================================================================

  describe "incr/1" do
    test "increments nonexistent key from 0 to 1" do
      assert {:ok, 1} = FerricStore.incr("counter:new")
    end

    test "increments existing integer value" do
      FerricStore.set("counter:existing", "5")
      assert {:ok, 6} = FerricStore.incr("counter:existing")
    end

    test "multiple increments accumulate" do
      assert {:ok, 1} = FerricStore.incr("counter:multi")
      assert {:ok, 2} = FerricStore.incr("counter:multi")
      assert {:ok, 3} = FerricStore.incr("counter:multi")
    end

    test "incr on non-integer value returns error" do
      FerricStore.set("not_a_number", "abc")
      assert {:error, msg} = FerricStore.incr("not_a_number")
      assert msg =~ "not an integer"
    end

    test "incr on negative integer works" do
      FerricStore.set("negative", "-3")
      assert {:ok, -2} = FerricStore.incr("negative")
    end

    test "incr on zero works" do
      FerricStore.set("zero", "0")
      assert {:ok, 1} = FerricStore.incr("zero")
    end

    test "incr preserves string representation after increment" do
      FerricStore.incr("str_counter")
      assert {:ok, "1"} = FerricStore.get("str_counter")
    end
  end

  describe "incr_by/2" do
    test "increments by specified amount" do
      assert {:ok, 10} = FerricStore.incr_by("counter:by", 10)
    end

    test "increments by negative amount (decrement)" do
      FerricStore.set("counter:dec", "100")
      assert {:ok, 95} = FerricStore.incr_by("counter:dec", -5)
    end

    test "increments by zero returns current value" do
      FerricStore.set("counter:zero", "42")
      assert {:ok, 42} = FerricStore.incr_by("counter:zero", 0)
    end

    test "incr_by on nonexistent key initializes from 0" do
      assert {:ok, 50} = FerricStore.incr_by("counter:fresh", 50)
    end

    test "incr_by on non-integer value returns error" do
      FerricStore.set("bad_val", "hello")
      assert {:error, _msg} = FerricStore.incr_by("bad_val", 5)
    end

    test "incr_by with large amount works" do
      assert {:ok, 1_000_000} = FerricStore.incr_by("big_counter", 1_000_000)
    end
  end

  # ===========================================================================
  # HSET / HGET / HGETALL — hash operations
  # ===========================================================================

  describe "hset/2 and hget/2" do
    test "sets and gets a hash field" do
      assert :ok = FerricStore.hset("user:1", %{"name" => "alice"})
      assert {:ok, "alice"} = FerricStore.hget("user:1", "name")
    end

    test "sets multiple fields at once" do
      assert :ok = FerricStore.hset("user:2", %{"name" => "bob", "age" => "25"})
      assert {:ok, "bob"} = FerricStore.hget("user:2", "name")
      assert {:ok, "25"} = FerricStore.hget("user:2", "age")
    end

    test "hget on nonexistent hash returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.hget("nonexistent_hash", "field")
    end

    test "hget on nonexistent field returns {:ok, nil}" do
      FerricStore.hset("partial_hash", %{"exists" => "yes"})
      assert {:ok, nil} = FerricStore.hget("partial_hash", "missing")
    end

    test "hset merges with existing fields" do
      FerricStore.hset("merge_hash", %{"a" => "1"})
      FerricStore.hset("merge_hash", %{"b" => "2"})
      assert {:ok, "1"} = FerricStore.hget("merge_hash", "a")
      assert {:ok, "2"} = FerricStore.hget("merge_hash", "b")
    end

    test "hset overwrites existing field value" do
      FerricStore.hset("overwrite_hash", %{"field" => "old"})
      FerricStore.hset("overwrite_hash", %{"field" => "new"})
      assert {:ok, "new"} = FerricStore.hget("overwrite_hash", "field")
    end

    test "hset with atom keys converts to strings" do
      FerricStore.hset("atom_hash", %{name: "alice", age: 30})
      assert {:ok, "alice"} = FerricStore.hget("atom_hash", "name")
      assert {:ok, "30"} = FerricStore.hget("atom_hash", "age")
    end
  end

  describe "hgetall/1" do
    test "returns all fields and values" do
      FerricStore.hset("full_hash", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, map} = FerricStore.hgetall("full_hash")
      assert map == %{"a" => "1", "b" => "2", "c" => "3"}
    end

    test "returns empty map for nonexistent hash" do
      assert {:ok, %{}} = FerricStore.hgetall("nope")
    end

    test "returns merged result after multiple hset calls" do
      FerricStore.hset("merged", %{"x" => "1"})
      FerricStore.hset("merged", %{"y" => "2"})
      assert {:ok, %{"x" => "1", "y" => "2"}} = FerricStore.hgetall("merged")
    end
  end

  # ===========================================================================
  # Hash encoding/decoding internals
  # ===========================================================================

  describe "hash encoding/decoding" do
    test "round-trips an empty map" do
      encoded = FerricStore.encode_hash(%{})
      assert FerricStore.decode_hash(encoded) == %{}
    end

    test "round-trips a single field" do
      original = %{"key" => "value"}
      encoded = FerricStore.encode_hash(original)
      assert FerricStore.decode_hash(encoded) == original
    end

    test "round-trips multiple fields" do
      original = %{"a" => "1", "b" => "2", "c" => "3"}
      encoded = FerricStore.encode_hash(original)
      assert FerricStore.decode_hash(encoded) == original
    end

    test "round-trips fields with binary data" do
      original = %{"bin" => <<0, 1, 255>>}
      encoded = FerricStore.encode_hash(original)
      assert FerricStore.decode_hash(encoded) == original
    end

    test "round-trips empty string field and value" do
      original = %{"" => ""}
      encoded = FerricStore.encode_hash(original)
      assert FerricStore.decode_hash(encoded) == original
    end

    test "decode_hash of empty binary returns empty map" do
      assert FerricStore.decode_hash(<<>>) == %{}
    end
  end

  # ===========================================================================
  # TTL — expire and ttl
  # ===========================================================================

  describe "expire/2 and ttl/1" do
    test "sets TTL on existing key" do
      FerricStore.set("ttl_test", "value")
      assert {:ok, true} = FerricStore.expire("ttl_test", :timer.minutes(30))
      assert {:ok, ms} = FerricStore.ttl("ttl_test")
      assert is_integer(ms)
      assert ms > 0
      assert ms <= :timer.minutes(30)
    end

    test "expire on nonexistent key returns {:ok, false}" do
      assert {:ok, false} = FerricStore.expire("no_such_key", :timer.seconds(10))
    end

    test "ttl on key without expiry returns {:ok, nil}" do
      FerricStore.set("no_ttl", "value")
      assert {:ok, nil} = FerricStore.ttl("no_ttl")
    end

    test "ttl on nonexistent key returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.ttl("never_set")
    end

    test "set with ttl then check ttl" do
      FerricStore.set("ttl_via_set", "value", ttl: :timer.seconds(60))
      assert {:ok, ms} = FerricStore.ttl("ttl_via_set")
      assert is_integer(ms)
      assert ms > 0
      assert ms <= 60_000
    end
  end

  # ===========================================================================
  # Pipeline — batching
  # ===========================================================================

  describe "pipeline/1" do
    test "executes multiple SET commands" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("p_key1", "val1")
                 |> FerricStore.Pipe.set("p_key2", "val2")
               end)

      assert results == [:ok, :ok]
      assert {:ok, "val1"} = FerricStore.get("p_key1")
      assert {:ok, "val2"} = FerricStore.get("p_key2")
    end

    test "executes mixed SET and GET commands" do
      FerricStore.set("existing", "hello")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("new_key", "world")
                 |> FerricStore.Pipe.get("existing")
                 |> FerricStore.Pipe.get("new_key")
               end)

      assert results == [:ok, {:ok, "hello"}, {:ok, "world"}]
    end

    test "pipeline with INCR" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr("pipe_counter")
                 |> FerricStore.Pipe.incr("pipe_counter")
                 |> FerricStore.Pipe.incr("pipe_counter")
               end)

      assert results == [{:ok, 1}, {:ok, 2}, {:ok, 3}]
    end

    test "pipeline with DEL" do
      FerricStore.set("del_target", "value")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.del("del_target")
                 |> FerricStore.Pipe.get("del_target")
               end)

      assert results == [:ok, {:ok, nil}]
    end

    test "pipeline with HSET and HGET" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.hset("pipe_hash", %{"field" => "value"})
                 |> FerricStore.Pipe.hget("pipe_hash", "field")
               end)

      assert results == [:ok, {:ok, "value"}]
    end

    test "empty pipeline returns empty results" do
      assert {:ok, []} =
               FerricStore.pipeline(fn pipe ->
                 pipe
               end)
    end

    test "pipeline with INCR_BY" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr_by("pipe_incrby", 10)
                 |> FerricStore.Pipe.incr_by("pipe_incrby", 5)
               end)

      assert results == [{:ok, 10}, {:ok, 15}]
    end

    test "pipeline commands execute in order" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("order_test", "first")
                 |> FerricStore.Pipe.get("order_test")
                 |> FerricStore.Pipe.set("order_test", "second")
                 |> FerricStore.Pipe.get("order_test")
               end)

      assert results == [:ok, {:ok, "first"}, :ok, {:ok, "second"}]
    end

    test "pipeline with SET with TTL option" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("ttl_pipe", "value", ttl: :timer.hours(1))
               end)

      assert results == [:ok]
      assert {:ok, "value"} = FerricStore.get("ttl_pipe")
    end
  end

  # ===========================================================================
  # Named caches (option passthrough)
  # ===========================================================================

  describe "named cache option" do
    test "set accepts cache option without error" do
      # The :cache option is accepted but currently routes to the default cache.
      # This test verifies the option does not cause an error.
      assert :ok = FerricStore.set("cached_key", "value", cache: :sessions)
    end

    test "get accepts cache option without error" do
      FerricStore.set("cached_key2", "value")
      assert {:ok, _} = FerricStore.get("cached_key2", cache: :sessions)
    end
  end

  # ===========================================================================
  # Sandbox key prefixing
  # ===========================================================================

  describe "sandbox_key/1" do
    test "returns the key unchanged when no sandbox is active" do
      # Save and clear any existing sandbox
      saved = Process.get(:ferricstore_sandbox)
      Process.delete(:ferricstore_sandbox)

      assert FerricStore.sandbox_key("mykey") == "mykey"

      # Restore
      if saved, do: Process.put(:ferricstore_sandbox, saved)
    end

    test "prepends namespace when sandbox is active" do
      # The sandbox is active because we use FerricStore.Sandbox.Case
      namespace = Process.get(:ferricstore_sandbox)
      assert is_binary(namespace)
      assert FerricStore.sandbox_key("mykey") == namespace <> "mykey"
    end
  end
end
