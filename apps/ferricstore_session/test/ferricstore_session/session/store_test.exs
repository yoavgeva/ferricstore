defmodule FerricStore.Session.StoreTest do
  use ExUnit.Case, async: false
  @moduletag timeout: 60_000

  alias FerricStore.Session.Store

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    on_exit(fn -> Ferricstore.Test.ShardHelpers.flush_all_keys() end)
    :ok
  end

  # =========================================================================
  # init/1
  # =========================================================================

  describe "init/1" do
    test "returns config map with defaults" do
      opts = Store.init([])
      assert opts == %{prefix: "session", ttl: 86_400}
    end

    test "accepts custom prefix and ttl" do
      opts = Store.init(prefix: "sess", ttl: 3600)
      assert opts == %{prefix: "sess", ttl: 3600}
    end
  end

  # =========================================================================
  # put/4 + get/3 round-trip
  # =========================================================================

  describe "put/4 and get/3 round-trip" do
    test "stores and retrieves session data" do
      opts = Store.init([])
      sid = Store.put(nil, nil, %{"user_id" => 42}, opts)

      assert is_binary(sid)
      assert {^sid, %{"user_id" => 42}} = Store.get(nil, sid, opts)
    end

    test "put with nil sid generates a new sid" do
      opts = Store.init([])
      sid = Store.put(nil, nil, %{"foo" => "bar"}, opts)

      assert is_binary(sid)
      assert byte_size(sid) > 0
    end

    test "put with existing sid updates the session and returns same sid" do
      opts = Store.init([])
      sid = Store.put(nil, nil, %{"v" => 1}, opts)
      returned_sid = Store.put(nil, sid, %{"v" => 2}, opts)

      assert returned_sid == sid
      assert {^sid, %{"v" => 2}} = Store.get(nil, sid, opts)
    end

    test "generated sids are unique" do
      opts = Store.init([])
      sid1 = Store.put(nil, nil, %{}, opts)
      sid2 = Store.put(nil, nil, %{}, opts)

      assert sid1 != sid2
    end
  end

  # =========================================================================
  # get/3 — missing and invalid
  # =========================================================================

  describe "get/3 edge cases" do
    test "returns {nil, %{}} for missing sid" do
      opts = Store.init([])
      assert {nil, %{}} = Store.get(nil, "nonexistent_sid", opts)
    end

    test "returns {nil, %{}} for empty string sid" do
      opts = Store.init([])
      assert {nil, %{}} = Store.get(nil, "", opts)
    end

    test "returns {nil, %{}} for nil sid" do
      opts = Store.init([])
      assert {nil, %{}} = Store.get(nil, nil, opts)
    end
  end

  # =========================================================================
  # delete/3
  # =========================================================================

  describe "delete/3" do
    test "removes session, subsequent get returns {nil, %{}}" do
      opts = Store.init([])
      sid = Store.put(nil, nil, %{"user_id" => 42}, opts)
      assert {^sid, %{"user_id" => 42}} = Store.get(nil, sid, opts)

      assert :ok = Store.delete(nil, sid, opts)
      assert {nil, %{}} = Store.get(nil, sid, opts)
    end

    test "deleting nonexistent session returns :ok" do
      opts = Store.init([])
      assert :ok = Store.delete(nil, "nonexistent", opts)
    end
  end

  # =========================================================================
  # TTL
  # =========================================================================

  describe "TTL expiry" do
    test "session expires after ttl" do
      opts = Store.init(ttl: 1)
      sid = Store.put(nil, nil, %{"ephemeral" => true}, opts)

      assert {^sid, %{"ephemeral" => true}} = Store.get(nil, sid, opts)

      Process.sleep(1_100)

      assert {nil, %{}} = Store.get(nil, sid, opts)
    end
  end

  # =========================================================================
  # Serialization
  # =========================================================================

  describe "serialization" do
    test "complex data types round-trip correctly" do
      opts = Store.init([])
      data = %{
        "string" => "hello",
        "integer" => 42,
        "float" => 3.14,
        "list" => [1, 2, 3],
        "nested" => %{"a" => %{"b" => "c"}},
        "tuple" => {1, :two, "three"},
        "atom" => :ok,
        "boolean" => true,
        "nil" => nil
      }

      sid = Store.put(nil, nil, data, opts)
      assert {^sid, ^data} = Store.get(nil, sid, opts)
    end

    test "empty session data round-trips" do
      opts = Store.init([])
      sid = Store.put(nil, nil, %{}, opts)
      assert {^sid, %{}} = Store.get(nil, sid, opts)
    end
  end

  # =========================================================================
  # Custom prefix
  # =========================================================================

  describe "custom prefix" do
    test "sessions with different prefixes are isolated" do
      opts_a = Store.init(prefix: "app_a")
      opts_b = Store.init(prefix: "app_b")

      sid = "shared_sid_123"
      Store.put(nil, sid, %{"app" => "a"}, opts_a)
      Store.put(nil, sid, %{"app" => "b"}, opts_b)

      assert {^sid, %{"app" => "a"}} = Store.get(nil, sid, opts_a)
      assert {^sid, %{"app" => "b"}} = Store.get(nil, sid, opts_b)
    end
  end

  # =========================================================================
  # Concurrent sessions
  # =========================================================================

  describe "concurrent sessions" do
    test "multiple sessions don't interfere" do
      opts = Store.init([])
      sid1 = Store.put(nil, nil, %{"user" => "alice"}, opts)
      sid2 = Store.put(nil, nil, %{"user" => "bob"}, opts)

      assert {^sid1, %{"user" => "alice"}} = Store.get(nil, sid1, opts)
      assert {^sid2, %{"user" => "bob"}} = Store.get(nil, sid2, opts)

      Store.delete(nil, sid1, opts)
      assert {nil, %{}} = Store.get(nil, sid1, opts)
      assert {^sid2, %{"user" => "bob"}} = Store.get(nil, sid2, opts)
    end
  end

  # =========================================================================
  # Corrupted data
  # =========================================================================

  describe "corrupted data" do
    test "get with corrupted binary returns {nil, %{}}" do
      opts = Store.init([])
      key = "session:corrupted_test_sid"
      FerricStore.set(key, "not_valid_erlang_term_binary")

      assert {nil, %{}} = Store.get(nil, "corrupted_test_sid", opts)
    end
  end

  # =========================================================================
  # Large session
  # =========================================================================

  describe "large session" do
    test "100KB session data works" do
      opts = Store.init([])
      large_value = :crypto.strong_rand_bytes(100_000) |> Base.encode64()
      data = %{"payload" => large_value}

      sid = Store.put(nil, nil, data, opts)
      assert {^sid, ^data} = Store.get(nil, sid, opts)
    end
  end
end
