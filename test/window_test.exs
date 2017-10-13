defmodule Ecto.OLAP.WindowTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo, as: Repo

  setup_all do
    Repo.insert_all("grouping", [
      %{foo: "a", bar: 1, baz: "c"},
      %{foo: "a", bar: 1, baz: "d"},
      %{foo: "a", bar: 2, baz: "c"},
      %{foo: "b", bar: 2, baz: "d"},
      %{foo: "b", bar: 3, baz: "c"},
    ])

    on_exit fn ->
      Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    end

    :ok
  end

  doctest Ecto.OLAP.Window
end
