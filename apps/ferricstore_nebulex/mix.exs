defmodule FerricstoreNebulex.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ferricstore_nebulex,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      files: ["lib", "mix.exs"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/YoavGivati/ferricstore"
      }
    ]
  end

  defp deps do
    [
      {:ferricstore, in_umbrella: true},
      {:nebulex, "~> 3.0"}
    ]
  end
end
