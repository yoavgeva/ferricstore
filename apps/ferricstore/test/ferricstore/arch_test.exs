defmodule Ferricstore.ArchTest do
  use ExUnit.Case, async: true
  use ArchTest, app: :ferricstore

  # ---------------------------------------------------------------------------
  # Dependency rules
  #
  # Layer order (outermost -> innermost):
  #   Commands -> Store -> Bitcask/NIF
  #
  # Server-specific modules (Connection, Listener, Resp, ACL, ClientTracking)
  # have been moved to the separate :ferricstore_server umbrella app.
  # The library has zero references to server modules.
  # ---------------------------------------------------------------------------

  @tag timeout: 60_000
  test "store layer does not depend on commands (except Shard for tx)" do
    # Shard depends on Commands.Dispatcher for tx_execute (MULTI/EXEC).
    # This is an intentional coupling: tx_execute must dispatch queued
    # commands within the shard's handle_call for atomicity.
    modules_matching("Ferricstore.Store.**")
    |> excluding("Ferricstore.Store.Shard")
    |> excluding("Ferricstore.Store.Shard.Transaction")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "bitcask NIF wrapper does not depend on any Ferricstore layer" do
    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "raft state machine may depend on Commands.Dispatcher for cross-shard tx" do
    modules_matching("Ferricstore.Raft.**")
    |> excluding("Ferricstore.Raft.StateMachine")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "no circular dependencies in Ferricstore" do
    modules_matching("Ferricstore.**")
    |> should_be_free_of_cycles()
  end
end
