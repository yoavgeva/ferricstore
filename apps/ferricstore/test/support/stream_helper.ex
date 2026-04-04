defmodule Ferricstore.Jepsen.StreamHelper do
  @moduledoc """
  Helper module for executing stream commands on remote peer nodes.

  This module is loaded onto peer nodes at test setup time via
  `:code.load_binary/3`. It provides `exec/2` to run stream commands
  and `ensure_tables/0` to create persistent ETS tables for stream
  metadata.

  ## ETS Table Ownership

  Stream commands use `Ferricstore.Stream.Meta` and related ETS tables.
  These tables are normally created lazily by the first stream command.
  In RPC contexts, the table would be created by a temporary process
  that exits after the RPC call, destroying the table.

  `ensure_tables/0` spawns a long-lived process that creates and owns
  the stream ETS tables, keeping them alive across multiple RPC calls.
  Call this once during test setup after loading the module onto the
  peer node.
  """

  @doc """
  Ensures the stream metadata ETS tables exist and are owned by a
  persistent process that will keep them alive across RPC calls.

  Must be called once per peer node before any `exec/2` calls.
  Returns `:ok`.
  """
  @spec ensure_tables() :: :ok
  def ensure_tables do
    # If tables already exist (owned by another process), no-op.
    case :ets.whereis(Ferricstore.Stream.Meta) do
      :undefined ->
        # Spawn a persistent process that creates and owns the tables.
        parent = self()

        pid =
          spawn(fn ->
            Ferricstore.Commands.Stream.ensure_meta_table()
            send(parent, :tables_ready)

            # Keep the process alive to retain ETS table ownership.
            receive do
              :stop -> :ok
            end
          end)

        # Register so we can find and stop it later.
        try do
          Process.register(pid, :ferricstore_stream_table_owner)
        rescue
          ArgumentError -> :ok
        end

        receive do
          :tables_ready -> :ok
        after
          5_000 -> raise "timeout waiting for stream table creation"
        end

      _ref ->
        :ok
    end
  end

  @doc """
  Executes a stream command with a locally-built store map.

  ## Parameters

    * `cmd` -- uppercased stream command name (e.g. `"XADD"`, `"XRANGE"`)
    * `args` -- list of string arguments

  ## Returns

  The result from `Ferricstore.Commands.Stream.handle/3`.
  """
  @spec exec(binary(), [binary()]) :: term()
  def exec(cmd, args) do
    ctx = FerricStore.Instance.get(:default)
    store = %{
      get: fn k -> Ferricstore.Store.Router.get(ctx, k) end,
      get_meta: fn k -> Ferricstore.Store.Router.get_meta(ctx, k) end,
      put: fn k, v, e -> Ferricstore.Store.Router.put(ctx, k, v, e) end,
      delete: fn k -> Ferricstore.Store.Router.delete(ctx, k) end,
      exists?: fn k -> Ferricstore.Store.Router.exists?(ctx, k) end,
      keys: fn -> Ferricstore.Store.Router.keys(ctx) end
    }

    Ferricstore.Commands.Stream.handle(cmd, args, store)
  end
end
