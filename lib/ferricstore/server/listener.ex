defmodule Ferricstore.Server.Listener do
  @moduledoc """
  Ranch TCP listener for the FerricStore Redis-compatible server.

  Starts a `:ranch` listener that accepts TCP connections on the configured
  port (default 6379) and dispatches each accepted socket to a new
  `Ferricstore.Server.Connection` process.

  ## Usage

      # Start on default port 6379
      Ferricstore.Server.Listener.start(6379)

      # Stop the listener
      Ferricstore.Server.Listener.stop()

  The listener is intended to be supervised. Call `child_spec/1` to embed it
  in a supervisor tree, or call `start/1` directly in tests.
  """

  @listener_ref __MODULE__

  @doc """
  Returns the Ranch listener reference atom used to identify this listener.
  """
  @spec ref() :: atom()
  def ref, do: @listener_ref

  @doc """
  Starts the Ranch TCP listener on the given port.

  Returns `{:ok, pid}` on success or `{:error, reason}` on failure.

  ## Parameters

    - `port` - TCP port to listen on (1–65535).
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

  @doc """
  Stops the Ranch TCP listener.
  """
  @spec stop() :: :ok
  def stop do
    :ranch.stop_listener(@listener_ref)
  end
end
