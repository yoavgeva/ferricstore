defmodule FerricstoreServer.Application do
  @moduledoc """
  OTP Application for the FerricStore standalone server.

  Starts the network-facing children that expose the core engine over TCP:

    * Ranch TCP listener (`FerricstoreServer.Listener`)
    * Ranch TLS listener (`FerricstoreServer.TlsListener`) -- optional
    * HTTP health/metrics endpoint (`FerricstoreServer.Health.Endpoint`)

  This application depends on `:ferricstore` (the core engine). In embedded
  mode (`config :ferricstore, :mode, :embedded`), this application is not
  started -- the host application uses the core engine API directly.

  ## Supervision tree (`:one_for_one`)

  ```
  FerricstoreServer.Supervisor
  ├── :pg scope (FerricstoreServer.PG) — ACL invalidation process groups
  ├── Ranch listener (FerricstoreServer.Listener)
  ├── Ranch TLS listener (FerricstoreServer.TlsListener) [optional]
  └── Health HTTP endpoint (FerricstoreServer.Health.Endpoint)
  ```
  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    mode = Ferricstore.Mode.current()

    if mode == :embedded do
      Logger.info("FerricstoreServer skipping startup (embedded mode)")
      Supervisor.start_link([], strategy: :one_for_one, name: FerricstoreServer.Supervisor)
    else
      port = Application.get_env(:ferricstore, :port, 6379)
      health_port = Application.get_env(:ferricstore, :health_port, 4000)

      children =
        [
          # Start :pg scope for ACL invalidation broadcasts before the Ranch
          # listener so it's ready when the first connection process starts.
          pg_child_spec()
        ] ++
        [ranch_listener_spec(port)] ++
          tls_listener_children() ++
          [FerricstoreServer.Health.Endpoint.child_spec(health_port)]

      opts = [strategy: :one_for_one, name: FerricstoreServer.Supervisor, max_restarts: 20, max_seconds: 10]
      Supervisor.start_link(children, opts)
    end
  end

  # ---------------------------------------------------------------------------
  # :pg scope for ACL cache invalidation
  # ---------------------------------------------------------------------------

  # Returns a child spec for the :pg scope used by connection processes to
  # receive ACL invalidation broadcasts. The scope name matches the constant
  # in FerricstoreServer.Connection.
  defp pg_child_spec do
    %{
      id: FerricstoreServer.PG,
      start: {:pg, :start_link, [FerricstoreServer.Connection.acl_pg_group()]}
    }
  end

  # ---------------------------------------------------------------------------
  # TLS listener children
  # ---------------------------------------------------------------------------

  # Returns a list with the TLS Ranch listener child spec if TLS is configured,
  # or an empty list otherwise.
  defp tls_listener_children do
    tls_opts = [
      port: Application.get_env(:ferricstore, :tls_port),
      certfile: Application.get_env(:ferricstore, :tls_cert_file),
      keyfile: Application.get_env(:ferricstore, :tls_key_file),
      cacertfile: Application.get_env(:ferricstore, :tls_ca_cert_file)
    ]

    case FerricstoreServer.TlsListener.child_spec_if_configured(tls_opts) do
      nil -> []
      spec -> [spec]
    end
  end

  # ---------------------------------------------------------------------------
  # Ranch child spec
  # ---------------------------------------------------------------------------

  # Builds a Ranch TCP listener child spec.
  #
  # Ranch 2.x exposes `:ranch.child_spec/5` which returns a plain Supervisor
  # child spec suitable for embedding in any supervisor.  The listener will
  # bind to `port` (0 = ephemeral, useful in tests).
  defp ranch_listener_spec(port) do
    transport_opts = %{socket_opts: [
      port: port,
      nodelay: true,
      recbuf: 65_536,
      sndbuf: 65_536,
      backlog: 1024,
      keepalive: true
    ]}
    protocol_opts = %{}

    :ranch.child_spec(
      FerricstoreServer.Listener,
      :ranch_tcp,
      transport_opts,
      FerricstoreServer.Connection,
      protocol_opts
    )
  end
end
