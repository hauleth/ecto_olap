defmodule Ecto.OLAP.GroupingSets do
  @moduledoc """
  Helpers for advenced grouping functions in SQL.

  **WARNING**: Currently only PostgreSQL is supported

  # Example data

  All examples assumes we have table `grouping` with content:

  | foo | bar | baz |
  | --- | --- | --- |
  | a   | 1   | c   |
  | a   | 1   | d   |
  | a   | 2   | c   |
  | b   | 2   | d   |
  | b   | 3   | c   |
  """

  @type column :: any()
  @type columns :: tuple() | list(column())
  @opaque query :: tuple()

  @doc """
  Group by each set of columns in `groups`.

  ## Params

    - groups list of tuples or lists of columns to group by, empty tuple/list
      means that we should aggregate all columns

  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.GroupingSets
      iex>
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "grouping",
      ...>   group_by: grouping_sets([{entry.foo, entry.bar}, {entry.foo}]),
      ...>   select: %{foo: entry.foo, bar: entry.bar, count: count(entry.foo)}
      [%{foo: "a", bar: 1,   count: 2},
       %{foo: "a", bar: 2,   count: 1},
       %{foo: "a", bar: nil, count: 3},
       %{foo: "b", bar: 2,   count: 1},
       %{foo: "b", bar: 3,   count: 1},
       %{foo: "b", bar: nil, count: 2}]
  """
  @spec grouping_sets([columns]) :: query
  defmacro grouping_sets(groups) when is_list(groups) do
    groups
    |> Enum.map(&to_sql/1)
    |> query("GROUPING SETS")
  end

  @doc """
  Create prefix list of given columns.

  This is shorthand notation for all prefixes of given column list.

      from e in "grouping",
        group_by: rollup([e.foo, e.bar]),
        # …

  Will be equivalent to:

      from e in "grouping",
        group_by: grouping_sets([{e.foo, e.bar}, {e.foo}, {}]),
        # …

  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.GroupingSets
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "grouping",
      ...>   group_by: rollup([entry.foo, entry.bar]),
      ...>   select: %{foo: entry.foo, bar: entry.bar, count: count(entry.foo)}
      [%{foo: "a", bar: 1,   count: 2},
       %{foo: "a", bar: 2,   count: 1},
       %{foo: "a", bar: nil, count: 3},
       %{foo: "b", bar: 2,   count: 1},
       %{foo: "b", bar: 3,   count: 1},
       %{foo: "b", bar: nil, count: 2},
       %{foo: nil, bar: nil, count: 5}]
  """
  @spec rollup([column]) :: query
  defmacro rollup(columns), do: query(columns, "ROLLUP")

  @doc """
  Create cube of given columns.

  This is shorthand notation for all combinations of given columns.

      from e in "grouping",
        group_by: cube([e.foo, e.bar, e.baz]),
        # …

  Will be equivalent to:

      from e in "grouping",
        group_by: grouping_sets([{e.foo, e.bar, e.baz},
                                 {e.foo, e.bar       },
                                 {e.foo,        e.baz},
                                 {e.foo,             },
                                 {       e.bar, e.baz},
                                 {       e.bar       },
                                 {              e.baz},
                                 {                   }]),
        # …

  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.GroupingSets
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "grouping",
      ...>   group_by: cube([entry.foo, entry.bar, entry.baz]),
      ...>   select: %{foo: entry.foo, bar: entry.bar, baz: entry.baz, count: count(entry.foo)}
      [%{foo: "a", bar: 1,   baz: "c", count: 1},
       %{foo: "a", bar: 1,   baz: "d", count: 1},
       %{foo: "a", bar: 1,   baz: nil, count: 2},
       %{foo: "a", bar: 2,   baz: "c", count: 1},
       %{foo: "a", bar: 2,   baz: nil, count: 1},
       %{foo: "a", bar: nil, baz: nil, count: 3},
       %{foo: "b", bar: 2,   baz: "d", count: 1},
       %{foo: "b", bar: 2,   baz: nil, count: 1},
       %{foo: "b", bar: 3,   baz: "c", count: 1},
       %{foo: "b", bar: 3,   baz: nil, count: 1},
       %{foo: "b", bar: nil, baz: nil, count: 2},
       %{foo: nil, bar: nil, baz: nil, count: 5},
       %{foo: "a", bar: nil, baz: "c", count: 2},
       %{foo: "b", bar: nil, baz: "c", count: 1},
       %{foo: nil, bar: nil, baz: "c", count: 3},
       %{foo: "a", bar: nil, baz: "d", count: 1},
       %{foo: "b", bar: nil, baz: "d", count: 1},
       %{foo: nil, bar: nil, baz: "d", count: 2},
       %{foo: nil, bar: 1,   baz: "c", count: 1},
       %{foo: nil, bar: 1,   baz: "d", count: 1},
       %{foo: nil, bar: 1,   baz: nil, count: 2},
       %{foo: nil, bar: 2,   baz: "c", count: 1},
       %{foo: nil, bar: 2,   baz: "d", count: 1},
       %{foo: nil, bar: 2,   baz: nil, count: 2},
       %{foo: nil, bar: 3,   baz: "c", count: 1},
       %{foo: nil, bar: 3,   baz: nil, count: 1}]
  """
  @spec cube([column]) :: query
  defmacro cube(columns), do: query(columns, "CUBE")

  @doc """
  Select operator that provide bitmask for given grouping set.

  Params for this needs to be exactly the same as the list given to any grouping
  set command. Bits are assigned with the rightmost argument being
  the least-signifant bit. Each bit is `0` if the corresponding expression is
  in the grouping criteria, and 1 if it is not.

  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.GroupingSets
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "grouping",
      ...>   group_by: cube([entry.foo, entry.bar]),
      ...>   select: %{cols: grouping([entry.foo, entry.bar]), count: count(entry.foo)}
      [%{cols: 0b00, count: 2},
       %{cols: 0b00, count: 1},
       %{cols: 0b01, count: 3},
       %{cols: 0b00, count: 1},
       %{cols: 0b00, count: 1},
       %{cols: 0b01, count: 2},
       %{cols: 0b11, count: 5},
       %{cols: 0b10, count: 2},
       %{cols: 0b10, count: 2},
       %{cols: 0b10, count: 1}]
  """
  @spec grouping([column]) :: query
  defmacro grouping(columns), do: query(columns, "GROUPING")

  defp query(data, name) do
    quote do: fragment(unquote(name <> " ?"), unquote(fragment_list data))
  end

  defp fragment_list(list) when is_list(list) do
    query = "?" |> List.duplicate(Enum.count(list)) |> Enum.join(",")

    quote do: fragment(unquote("(" <> query <> ")"), unquote_splicing(list))
  end

  defp to_sql({:{}, _, data}), do: to_sql(data)
  defp to_sql(tuple) when is_tuple(tuple), do: to_sql Tuple.to_list tuple
  defp to_sql(list) when is_list(list), do: fragment_list list
end
