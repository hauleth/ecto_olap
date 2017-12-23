defmodule Ecto.OLAP.Mixfile do
  use Mix.Project

  def project do
    [app: :ecto_olap,
     version: "0.2.1",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),

     # Docs
     name: "Ecto.OLAP",
     description: description(),
     source_url: "https://github.com/hauleth/ecto_olap",

     package: package()]
  end

  def application, do: []

  defp description do
    """
    Data analytics helpers for Ecto and PostgreSQL
    """
  end

  defp deps do
    [{:ecto, ">= 2.0.0 and < 3.0.0", only: [:dev, :test]},
     {:postgrex, ">= 0.0.0", only: [:dev, :test]},
     {:ex_doc, "~> 0.14", only: :dev, runtime: false},
     {:ex_dash, ">= 0.0.0", only: :dev, runtime: false},
     {:dialyxir, ">= 0.0.0", only: :dev, runtime: false},
     {:credo, ">= 0.0.0", only: :dev, runtime: false}]
  end

  defp package do
    [maintainers: ["Łukasz Jan Niemier"],
     files: ["lib", "mix.exs", "README*", "LICENSE*"],
     licenses: ["MIT"],
     links: %{
       "GitHub" => "https://github.com/hauleth/ecto_olap"
     }]
  end
end
