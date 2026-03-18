defmodule Ferricstore.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers(),
      deps: deps(),
      aliases: aliases()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

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
      {:ra, "~> 2.14"},
      {:telemetry, "~> 1.4"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:arch_test, "~> 0.1.2", only: [:dev, :test], runtime: false},
      {:benchee, "~> 1.3", only: :bench, runtime: false},
      {:benchee_html, "~> 1.0", only: :bench, runtime: false}
    ]
  end

  defp aliases do
    [
      "bench.resp": "run bench/resp_bench.exs",
      "bench.store": "run bench/store_bench.exs",
      "bench.commands": "run bench/commands_bench.exs",
      "bench.tcp": "run bench/tcp_bench.exs"
    ]
  end
end
