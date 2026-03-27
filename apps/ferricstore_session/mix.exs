defmodule FerricstoreSession.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ferricstore_session,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Plug.Session.Store adapter backed by FerricStore"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ferricstore, in_umbrella: true},
      {:plug, "~> 1.14"}
    ]
  end
end
