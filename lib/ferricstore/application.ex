defmodule Ferricstore.Application do
  @moduledoc """
  OTP Application for FerricStore.

  Supervision tree (`:one_for_one`):

  ```
  Ferricstore.Supervisor
  ├── Ferricstore.Store.ShardSupervisor   (one_for_one over N Shard GenServers)
  └── Ranch listener (Ferricstore.Server.Listener)
  ```

  The `ShardSupervisor` must start **before** the Ranch listener so that the
  key-value store is ready before any client connection can arrive.

  ## Configuration (application env)

    * `:port`     - TCP port to bind (default: `6379`; test env uses `0` for ephemeral)
    * `:data_dir` - Bitcask data directory (default: `"data"`)
  """

  use Application

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:ferricstore, :port, 6379)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")

    children = [
      {Ferricstore.Store.ShardSupervisor, data_dir: data_dir},
      ranch_listener_spec(port)
    ]

    opts = [strategy: :one_for_one, name: Ferricstore.Supervisor]
    Supervisor.start_link(children, opts)
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
    transport_opts = %{socket_opts: [port: port]}
    protocol_opts = %{}

    :ranch.child_spec(
      Ferricstore.Server.Listener,
      :ranch_tcp,
      transport_opts,
      Ferricstore.Server.Connection,
      protocol_opts
    )
  end
end
