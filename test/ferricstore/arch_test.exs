defmodule Ferricstore.ArchTest do
  use ExUnit.Case, async: true
  use ArchTest, app: :ferricstore

  # ---------------------------------------------------------------------------
  # Dependency rules
  #
  # Layer order (outermost → innermost):
  #   Server → Commands → Store → Bitcask/NIF
  #   Server → Resp (protocol codec)
  # ---------------------------------------------------------------------------

  test "protocol layer does not depend on server, store, or commands" do
    modules_matching("Ferricstore.Resp.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Server.**"))

    modules_matching("Ferricstore.Resp.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    modules_matching("Ferricstore.Resp.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "store layer does not depend on protocol, server, or commands" do
    modules_matching("Ferricstore.Store.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Resp.**"))

    modules_matching("Ferricstore.Store.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Server.**"))

    modules_matching("Ferricstore.Store.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "bitcask NIF wrapper does not depend on any Ferricstore layer" do
    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Resp.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Server.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "command handlers do not depend on server or protocol internals" do
    modules_matching("Ferricstore.Commands.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Server.**"))

    modules_matching("Ferricstore.Commands.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Resp.**"))
  end

  test "server layer does not depend on bitcask directly" do
    modules_matching("Ferricstore.Server.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Bitcask.**"))
  end

  test "no circular dependencies in Ferricstore" do
    modules_matching("Ferricstore.**")
    |> should_be_free_of_cycles()
  end
end
