defmodule Ferricstore.Commands.DispatcherTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Routing to Strings handler
  # ---------------------------------------------------------------------------

  test "dispatches SET to Strings handler" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("SET", ["k", "v"], store)
    assert "v" == store.get.("k")
  end

  test "dispatches GET to Strings handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert "v" == Dispatcher.dispatch("GET", ["k"], store)
  end

  test "dispatches DEL to Strings handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert 1 == Dispatcher.dispatch("DEL", ["k"], store)
  end

  test "dispatches EXISTS to Strings handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert 1 == Dispatcher.dispatch("EXISTS", ["k"], store)
  end

  test "dispatches MGET to Strings handler" do
    store = MockStore.make(%{"a" => {"1", 0}})
    assert ["1", nil] == Dispatcher.dispatch("MGET", ["a", "b"], store)
  end

  test "dispatches MSET to Strings handler" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("MSET", ["a", "1", "b", "2"], store)
    assert "1" == store.get.("a")
  end

  # ---------------------------------------------------------------------------
  # Routing to Expiry handler
  # ---------------------------------------------------------------------------

  test "dispatches EXPIRE to Expiry handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert 1 == Dispatcher.dispatch("EXPIRE", ["k", "10"], store)
  end

  test "dispatches TTL to Expiry handler" do
    future = System.os_time(:millisecond) + 10_000
    store = MockStore.make(%{"k" => {"v", future}})
    ttl = Dispatcher.dispatch("TTL", ["k"], store)
    assert ttl > 0
  end

  test "dispatches PTTL to Expiry handler" do
    future = System.os_time(:millisecond) + 10_000
    store = MockStore.make(%{"k" => {"v", future}})
    pttl = Dispatcher.dispatch("PTTL", ["k"], store)
    assert pttl > 0
  end

  test "dispatches PERSIST to Expiry handler" do
    future = System.os_time(:millisecond) + 60_000
    store = MockStore.make(%{"k" => {"v", future}})
    assert 1 == Dispatcher.dispatch("PERSIST", ["k"], store)
  end

  # ---------------------------------------------------------------------------
  # Routing to Server handler
  # ---------------------------------------------------------------------------

  test "dispatches PING to Server handler" do
    assert {:simple, "PONG"} == Dispatcher.dispatch("PING", [], MockStore.make())
  end

  test "dispatches ECHO to Server handler" do
    assert "hi" == Dispatcher.dispatch("ECHO", ["hi"], MockStore.make())
  end

  test "dispatches KEYS to Server handler" do
    store = MockStore.make(%{"a" => {"1", 0}})
    assert ["a"] == Dispatcher.dispatch("KEYS", ["*"], store)
  end

  test "dispatches DBSIZE to Server handler" do
    store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
    assert 2 == Dispatcher.dispatch("DBSIZE", [], store)
  end

  test "dispatches FLUSHDB to Server handler" do
    store = MockStore.make(%{"a" => {"1", 0}})
    assert :ok = Dispatcher.dispatch("FLUSHDB", [], store)
  end

  # ---------------------------------------------------------------------------
  # Unknown command
  # ---------------------------------------------------------------------------

  test "unknown command returns error tuple" do
    assert {:error, msg} = Dispatcher.dispatch("UNKNOWNCMD", [], MockStore.make())
    assert msg =~ "unknown command"
    assert msg =~ "unknowncmd"
  end

  # ---------------------------------------------------------------------------
  # Case insensitivity
  # ---------------------------------------------------------------------------

  test "dispatch is case-insensitive (lowercase)" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("set", ["k", "v"], store)
  end

  test "dispatch is case-insensitive (mixed case)" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("Set", ["k", "v"], store)
  end
end
