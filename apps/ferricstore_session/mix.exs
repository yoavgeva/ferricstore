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
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      description: "Plug.Session.Store adapter backed by FerricStore.",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/yoavgeva/ferricstore"},
      files: ["lib", "mix.exs"]
    ]
  end

  defp deps do
    ferricstore_dep =
      if System.get_env("HEX_PUBLISH") do
        {:ferricstore, "~> 0.1"}
      else
        {:ferricstore, in_umbrella: true}
      end

    [
      ferricstore_dep,
      {:plug, "~> 1.14"},
      {:ex_doc, "~> 0.35", only: :dev, runtime: false}
    ]
  end
end
