defmodule Ecto.OLAP.GroupingSetsTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import Ecto.OLAP.GroupingSets

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

  doctest Ecto.OLAP.GroupingSets

  test "compare `rollup/1` and `grouping_sets/1`" do
    gs = Repo.all from e in "grouping",
      group_by: grouping_sets([{e.foo, e.bar}, {e.foo}, {}]),
      select: [e.foo, e.bar, count(e.id)]
    ro = Repo.all from e in "grouping",
      group_by: rollup([e.foo, e.bar]),
      select: [e.foo, e.bar, count(e.id)]

    assert gs == ro
  end

  test "compare `cube/1` and `grouping_sets/1`" do
    gs = Repo.all from e in "grouping",
      group_by: grouping_sets([{e.foo, e.bar}, {e.foo}, {e.bar}, {}]),
      select: [e.foo, e.bar, count(e.id)]
    cb = Repo.all from e in "grouping",
      group_by: cube([e.foo, e.bar]),
      select: [e.foo, e.bar, count(e.id)]

    assert gs == cb
  end

  test "allow dynamic columns via `field/2`" do
    column = :foo

    assert Repo.all from e in "grouping",
      group_by: rollup([field(e, ^column)]),
      select: [field(e, ^column), count(e.id)]
  end
end
