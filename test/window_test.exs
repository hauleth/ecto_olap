defmodule Ecto.OLAP.WindowTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo, as: Repo

  setup_all do
    Repo.insert_all("window", [
      %{depname: "develop", empno: 11, salary: 5200},
      %{depname: "develop", empno: 7, salary: 4200},
      %{depname: "develop", empno: 9, salary: 4500},
      %{depname: "develop", empno: 8, salary: 6000},
      %{depname: "develop", empno: 10, salary: 5200},
      %{depname: "personnel", empno: 5, salary: 3500},
      %{depname: "personnel", empno: 2, salary: 3900},
      %{depname: "sales", empno: 3, salary: 4800},
      %{depname: "sales", empno: 1, salary: 5000},
      %{depname: "sales", empno: 4, salary: 4800},
    ])

    on_exit fn ->
      Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    end

    :ok
  end

  doctest Ecto.OLAP.Window
end
