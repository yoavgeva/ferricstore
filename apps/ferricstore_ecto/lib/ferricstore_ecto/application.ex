defmodule FerricstoreEcto.Application do
  @moduledoc "OTP application callback for the FerricstoreEcto adapter."

  use Application

  @impl true
  def start(_type, _args) do
    children = []
    opts = [strategy: :one_for_one, name: FerricstoreEcto.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
