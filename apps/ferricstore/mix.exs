defmodule Ferricstore.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ferricstore,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers(),
      deps: deps(),
      package: package()
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

  defp package do
    [
      files: [
        "lib",
        "native/ferricstore_bitcask/.cargo",
        "native/ferricstore_bitcask/src",
        "native/ferricstore_bitcask/Cargo*",
        "checksum-*.exs",
        "mix.exs"
      ],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/YoavGivati/ferricstore"
      }
    ]
  end

  defp deps do
    [
      {:rustler_precompiled, "~> 0.8"},
      {:rustler, "~> 0.37", optional: true},
      {:ra, "~> 2.14"},
      {:libcluster, "~> 3.3"},
      {:telemetry, "~> 1.4"},
      {:jason, "~> 1.4"},
      {:plug, "~> 1.16", optional: true},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:arch_test, "~> 0.1.2", only: [:dev, :test], runtime: false}
    ]
  end
end
