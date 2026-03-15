defmodule Ferricstore.Server.Listener do
  @moduledoc """
  Ranch TCP listener for the FerricStore Redis-compatible server.

  In production the listener is started automatically by `Ferricstore.Application`
  via `:ranch.child_spec/5`.  It can also be started manually in tests using
  `start/1` / `stop/0`.

  ## Port 0 (ephemeral) support

  When the configured port is `0`, the OS assigns a free port.  Call
  `port/0` after the application has started to discover the actual port.
  This is the recommended pattern for tests so they never collide with a
  running development server.

  ## Usage in tests

      port = Ferricstore.Server.Listener.port()
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
  """

  @listener_ref __MODULE__

  @doc "Ranch listener reference atom."
  @spec ref() :: atom()
  def ref, do: @listener_ref

  @doc """
  Returns the actual TCP port the listener is bound to.

  Works for both fixed ports and ephemeral (port 0) bindings.  Raises if the
  listener is not running.
  """
  @spec port() :: :inet.port_number()
  def port do
    :ranch.get_port(@listener_ref)
  end

  @doc """
  Starts a Ranch TCP listener on `port` outside of the supervisor tree.

  Primarily used in isolated tests that do not start the full application.
  Returns `{:ok, pid}` or `{:error, reason}`.
  """
  @spec start(port :: :inet.port_number()) :: {:ok, pid()} | {:error, term()}
  def start(port) do
    transport_opts = %{socket_opts: [port: port]}
    protocol_opts = %{}

    :ranch.start_listener(
      @listener_ref,
      :ranch_tcp,
      transport_opts,
      Ferricstore.Server.Connection,
      protocol_opts
    )
  end

  @doc "Stops the Ranch TCP listener started with `start/1`."
  @spec stop() :: :ok
  def stop do
    :ranch.stop_listener(@listener_ref)
  end
end
