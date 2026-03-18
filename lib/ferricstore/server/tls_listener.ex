defmodule Ferricstore.Server.TlsListener do
  @moduledoc """
  Ranch TLS (SSL) listener for the FerricStore Redis-compatible server.

  Runs alongside the plaintext TCP listener on a separate port (default 6380).
  The listener is only started when TLS is configured via application env:

    * `:tls_port`         - port to bind (required; 0 = ephemeral)
    * `:tls_cert_file`    - path to PEM certificate file (required)
    * `:tls_key_file`     - path to PEM private key file (required)
    * `:tls_ca_cert_file` - path to CA certificate bundle (optional, for client verification)

  ## Protocol versions

  Only TLS 1.2 and TLS 1.3 are permitted. Older versions are explicitly
  disabled for security.

  ## Usage in tests

      port = Ferricstore.Server.TlsListener.port()
      {:ok, sock} = :ssl.connect(~c"127.0.0.1", port, [...])
  """

  @listener_ref __MODULE__

  @doc "Ranch listener reference atom."
  @spec ref() :: atom()
  def ref, do: @listener_ref

  @doc """
  Returns the actual TLS port the listener is bound to.

  Works for both fixed ports and ephemeral (port 0) bindings. Raises if the
  listener is not running.
  """
  @spec port() :: :inet.port_number()
  def port do
    :ranch.get_port(@listener_ref)
  end

  @doc """
  Returns `true` if the TLS listener is currently running.
  """
  @spec running?() :: boolean()
  def running? do
    try do
      _ = :ranch.get_port(@listener_ref)
      true
    rescue
      _ -> false
    catch
      :exit, _ -> false
    end
  end

  @doc """
  Starts a Ranch TLS listener on `port` outside of the supervisor tree.

  Primarily used in isolated tests that do not start the full application.
  Returns `{:ok, pid}` or `{:error, reason}`.

  ## Parameters

    - `opts` - keyword list with:
      - `:port`         - port number (0 = ephemeral)
      - `:certfile`     - path to PEM certificate file
      - `:keyfile`      - path to PEM private key file
      - `:cacertfile`   - path to CA cert file (optional)
  """
  @spec start(keyword()) :: {:ok, pid()} | {:error, term()}
  def start(opts) do
    port = Keyword.fetch!(opts, :port)
    certfile = Keyword.fetch!(opts, :certfile)
    keyfile = Keyword.fetch!(opts, :keyfile)

    socket_opts =
      [
        port: port,
        certfile: certfile,
        keyfile: keyfile,
        versions: [:"tlsv1.3", :"tlsv1.2"]
      ] ++ ca_opt(Keyword.get(opts, :cacertfile))

    transport_opts = %{socket_opts: socket_opts, num_acceptors: 10}
    protocol_opts = %{}

    :ranch.start_listener(
      @listener_ref,
      :ranch_ssl,
      transport_opts,
      Ferricstore.Server.Connection,
      protocol_opts
    )
  end

  @doc "Stops the Ranch TLS listener started with `start/1`."
  @spec stop() :: :ok
  def stop do
    :ranch.stop_listener(@listener_ref)
  end

  @doc """
  Builds a Ranch TLS listener child spec for embedding in a supervisor.

  Returns `nil` when TLS is not configured (missing port, certfile, or keyfile).

  ## Parameters

    - `opts` - keyword list with:
      - `:port`         - port number
      - `:certfile`     - path to PEM certificate file
      - `:keyfile`      - path to PEM private key file
      - `:cacertfile`   - path to CA cert file (optional)
  """
  @spec child_spec_if_configured(keyword()) :: map() | nil
  def child_spec_if_configured(opts) do
    port = Keyword.get(opts, :port)
    certfile = Keyword.get(opts, :certfile)
    keyfile = Keyword.get(opts, :keyfile)

    if port && certfile && keyfile do
      socket_opts =
        [
          port: port,
          certfile: certfile,
          keyfile: keyfile,
          versions: [:"tlsv1.3", :"tlsv1.2"]
        ] ++ ca_opt(Keyword.get(opts, :cacertfile))

      transport_opts = %{socket_opts: socket_opts, num_acceptors: 10}
      protocol_opts = %{}

      :ranch.child_spec(
        @listener_ref,
        :ranch_ssl,
        transport_opts,
        Ferricstore.Server.Connection,
        protocol_opts
      )
    else
      nil
    end
  end

  # Builds the cacertfile option list if a CA cert path is provided.
  defp ca_opt(nil), do: []
  defp ca_opt(path), do: [cacertfile: path]
end
