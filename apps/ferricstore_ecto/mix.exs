defmodule FerricstoreEcto.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ferricstore_ecto,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {FerricstoreEcto.Application, []}
    ]
  end

  defp deps do
    [
      {:ferricstore, in_umbrella: true},
      {:ecto, "~> 3.11"},
      {:ecto_sql, "~> 3.11"},
      {:ecto_sqlite3, "~> 0.17", only: :test},
      {:jason, "~> 1.4"}
    ]
  end
end
