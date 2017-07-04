defmodule Ecto.OLAP.GroupingSetsTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo, as: Repo

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    entries = [
      %{foo: "a", bar: 1, baz: "c"},
      %{foo: "a", bar: 1, baz: "d"},
      %{foo: "a", bar: 2, baz: "c"},
      %{foo: "b", bar: 2, baz: "d"},
      %{foo: "b", bar: 3, baz: "c"},
    ]

    Repo.insert_all("example", entries)

    :ok
  end

  doctest Ecto.OLAP.GroupingSets
end
