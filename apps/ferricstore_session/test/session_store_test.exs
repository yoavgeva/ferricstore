defmodule FerricStore.Session.StoreTest do
  use ExUnit.Case, async: false

  alias FerricStore.Session.Store

  setup do
    opts = Store.init(prefix: "test_sess", ttl: 60)
    %{opts: opts}
  end

  test "init returns default options" do
    opts = Store.init([])
    assert opts.prefix == "session"
    assert opts.ttl == 86_400
  end

  test "init accepts custom options" do
    opts = Store.init(prefix: "my_app", ttl: 3600)
    assert opts.prefix == "my_app"
    assert opts.ttl == 3600
  end

  test "put creates a new session and get retrieves it", %{opts: opts} do
    data = %{"user_id" => 42, "role" => "admin"}

    sid = Store.put(nil, nil, data, opts)
    assert is_binary(sid)
    assert byte_size(sid) > 0

    {^sid, retrieved} = Store.get(nil, sid, opts)
    assert retrieved == data
  end

  test "put with existing sid updates session", %{opts: opts} do
    data1 = %{"count" => 1}
    sid = Store.put(nil, nil, data1, opts)

    data2 = %{"count" => 2}
    ^sid = Store.put(nil, sid, data2, opts)

    {^sid, retrieved} = Store.get(nil, sid, opts)
    assert retrieved == data2
  end

  test "get returns {nil, %{}} for unknown session", %{opts: opts} do
    assert {nil, %{}} = Store.get(nil, "nonexistent_sid", opts)
  end

  test "get returns {nil, %{}} for nil sid", %{opts: opts} do
    assert {nil, %{}} = Store.get(nil, nil, opts)
  end

  test "get returns {nil, %{}} for empty string sid", %{opts: opts} do
    assert {nil, %{}} = Store.get(nil, "", opts)
  end

  test "delete removes session", %{opts: opts} do
    data = %{"token" => "abc123"}
    sid = Store.put(nil, nil, data, opts)

    {^sid, _} = Store.get(nil, sid, opts)

    :ok = Store.delete(nil, sid, opts)

    assert {nil, %{}} = Store.get(nil, sid, opts)
  end

  test "sessions are isolated by prefix" do
    opts_a = Store.init(prefix: "app_a", ttl: 60)
    opts_b = Store.init(prefix: "app_b", ttl: 60)

    data = %{"user" => "alice"}
    sid = Store.put(nil, nil, data, opts_a)

    {^sid, ^data} = Store.get(nil, sid, opts_a)
    {nil, %{}} = Store.get(nil, sid, opts_b)
  end

  test "session stores complex data types", %{opts: opts} do
    data = %{
      "user_id" => 42,
      "permissions" => ["read", "write"],
      "metadata" => %{"ip" => "127.0.0.1", "ua" => "Mozilla"},
      "logged_in_at" => ~U[2026-01-01 00:00:00Z]
    }

    sid = Store.put(nil, nil, data, opts)
    {^sid, retrieved} = Store.get(nil, sid, opts)
    assert retrieved == data
  end
end
