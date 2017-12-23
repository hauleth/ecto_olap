defmodule Ecto.OLAP.WindowTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo, as: Repo

  setup_all do
    Repo.insert_all("window", [
      %{depname: "sales"    , empno: 1 , salary: 5000, profit: 42_000} ,
      %{depname: "personnel", empno: 2 , salary: 3900, profit: -10_000},
      %{depname: "sales"    , empno: 3 , salary: 4800, profit: 60_000} ,
      %{depname: "sales"    , empno: 4 , salary: 4800, profit: 70_000} ,
      %{depname: "personnel", empno: 5 , salary: 3500, profit: -4000}  ,
      %{depname: "develop"  , empno: 7 , salary: 4200, profit: -1000}  ,
      %{depname: "develop"  , empno: 8 , salary: 6000, profit: -1000}  ,
      %{depname: "develop"  , empno: 9 , salary: 4500, profit: -1000}  ,
      %{depname: "develop"  , empno: 10, salary: 5200, profit: -1000}  ,
      %{depname: "develop"  , empno: 11, salary: 5200, profit: -1000}  ,
    ])

    on_exit fn ->
      Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    end

    :ok
  end

  doctest Ecto.OLAP.Window
end
