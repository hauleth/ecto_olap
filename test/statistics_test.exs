defmodule Ecto.OLAP.StatisticsTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo, as: Repo

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    entries = [
      %{year: 2000, divorce_rate: 5.0, marg_cons: 8.2},
      %{year: 2001, divorce_rate: 4.7, marg_cons: 7.0},
      %{year: 2002, divorce_rate: 4.6, marg_cons: 6.5},
      %{year: 2003, divorce_rate: 4.4, marg_cons: 5.3},
      %{year: 2004, divorce_rate: 4.3, marg_cons: 5.2},
      %{year: 2005, divorce_rate: 4.1, marg_cons: 4.0},
      %{year: 2006, divorce_rate: 4.2, marg_cons: 4.6},
      %{year: 2007, divorce_rate: 4.2, marg_cons: 4.5},
      %{year: 2008, divorce_rate: 4.2, marg_cons: 4.2},
      %{year: 2009, divorce_rate: 4.2, marg_cons: 3.7},
    ]

    Repo.insert_all("stats_agg", entries)

    :ok
  end

  doctest Ecto.OLAP.Statistics
end
