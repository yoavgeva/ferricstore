defmodule Ferricstore.ArchTest do
  use ExUnit.Case, async: true
  use ArchTest, app: :ferricstore

  # ---------------------------------------------------------------------------
  # Dependency rules
  #
  # Layer order (outermost -> innermost):
  #   FerricstoreServer -> Commands -> Store -> Bitcask/NIF
  #   FerricstoreServer -> Resp (protocol codec)
  #
  # Note: Server modules (Connection, Listener, TlsListener) have been moved
  # to the separate :ferricstore_server umbrella app. These arch rules verify
  # the remaining core modules.
  # ---------------------------------------------------------------------------

  test "protocol layer does not depend on store or commands" do
    modules_matching("Ferricstore.Resp.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    modules_matching("Ferricstore.Resp.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "store layer does not depend on protocol" do
    modules_matching("Ferricstore.Store.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Resp.**"))

    # Shard depends on Commands.Dispatcher for tx_execute (MULTI/EXEC).
    # This is an intentional coupling: tx_execute must dispatch queued
    # commands within the shard's handle_call for atomicity.
    modules_matching("Ferricstore.Store.**")
    |> excluding("Ferricstore.Store.Shard")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "bitcask NIF wrapper does not depend on any Ferricstore layer" do
    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Resp.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "command handlers do not depend on protocol internals" do
    modules_matching("Ferricstore.Commands.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Resp.**"))
  end

  test "raft state machine may depend on Commands.Dispatcher for cross-shard tx" do
    # StateMachine depends on Commands.Dispatcher for cross-shard atomic
    # MULTI/EXEC transactions. This is intentional: the StateMachine's apply/3
    # must dispatch queued commands through Dispatcher to build store results.
    modules_matching("Ferricstore.Raft.**")
    |> excluding("Ferricstore.Raft.StateMachine")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "no circular dependencies in Ferricstore" do
    modules_matching("Ferricstore.**")
    |> should_be_free_of_cycles()
  end
end
