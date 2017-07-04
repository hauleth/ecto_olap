defmodule EctoOLAP.Mixfile do
  use Mix.Project

  def project do
    [app: :ecto_olap,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),

     # Docs
     name: "Ecto.OLAP",
     source_url: "https://github.com/hauleth/ecto_olap",
    ]
  end

  def application, do: [applications: [:ecto]]

  defp deps do
    [{:ecto, ">= 2.0.0 and < 3.0.0"},
     {:postgrex, ">= 0.0.0", only: [:dev, :test]},
     {:ex_doc, "~> 0.14", only: :dev, runtime: false},
     {:dialyxir, ">= 0.0.0", only: :dev, runtime: false},
     {:credo, ">= 0.0.0", only: :dev, runtime: false}]
  end
end
