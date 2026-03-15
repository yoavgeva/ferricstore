defmodule Ferricstore.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Ferricstore.Application, []}
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.37"},
      {:ranch, "~> 2.2"},
      {:telemetry, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:arch_test, "~> 0.2", only: [:dev, :test], runtime: false}
    ]
  end
end
