defmodule Ferricstore.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers(),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key],
      mod: {Ferricstore.Application, []}
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.37"},
      {:ra, "~> 2.14"},
      {:telemetry, "~> 1.4"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:arch_test, "~> 0.1.2", only: [:dev, :test], runtime: false}
    ]
  end
end
