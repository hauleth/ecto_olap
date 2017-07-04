defmodule Ecto.OLAP.GroupingSets do
  @moduledoc """
  Helpers for advenced grouping functions in SQL.

  **WARNING**: Currently only PostgreSQL is supported
  """

  @type columns :: tuple() | list()
  @type group :: tuple() | [columns()]
  @opaque query :: tuple()

  @doc """
  Group by each set of columns in `groups`.

  ## Params

    - groups list of tuples or lists of columns to group by, empty tuple/list
      means that we should aggregate all columns

  ## Results

  Assume we have table `example` with content:

  | foo | bar |
  | --- | --- |
  | a   | 1   |
  | a   | 2   |
  | a   | 1   |
  | a   | 2   |
  | b   | 3   |

  Then query:

      Repo.all from e in "example",
        group_by: grouping_sets([{e.foo, e.bar}, {e.foo}]),
        select: %{foo: entry.foo, bar: entry.bar, count: count(entry.foo)}

  Will return data like:

      [
        %{foo: "a", bar: 1,   count: 2},
        %{foo: "a", bar: 2,   count: 2},
        %{foo: "a", bar: nil, count: 4},
        %{foo: "b", bar: 3,   count: 1},
      ]

  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.GroupingSets
      iex>
      iex> from entry in "example",
      ...>   group_by: grouping_sets([{entry.foo, entry.bar}, {entry.foo}]),
      ...>   select: %{foo: entry.foo, bar: entry.bar, count: count(entry.foo)}
      #Ecto.Query<from e in "example", group_by: [fragment("GROUPING SETS ?", fragment("(?,?)", fragment("(?,?)", e.foo, e.bar), fragment("(?)", e.foo)))], select: %{foo: e.foo, bar: e.bar, count: count(e.foo)}>
  """
  @spec grouping_sets([group]) :: query
  defmacro grouping_sets(groups) when is_list(groups) do
    groups
    |> Enum.map(&to_sql/1)
    |> query("GROUPING SETS")
  end

  @doc """
  This is shorthand notation for all prefixes of given column list.

  ## Example

      from e in "example",
        group_by: rollup([e.foo, e.bar]),
        # …

  Will be equivalent to:

      from e in "example",
        group_by: grouping_sets([{e.foo, e.bar}, {e.foo}, {}]),
        # …
  """
  @spec rollup([columns]) :: query
  defmacro rollup(columns), do: query(columns, "ROLLUP")

  @doc """
  This is shorthand notation for all combinations of given columns.

  ## Example

      from e in "example",
        group_by: cube([e.foo, e.bar, e.baz]),
        # …

  Will be equivalent to:

      from e in "example",
        group_by: grouping_sets([{e.foo, e.bar, e.baz},
                                 {e.foo, e.bar       },
                                 {e.foo,        e.baz},
                                 {e.foo,             },
                                 {       e.bar, e.baz},
                                 {       e.bar       },
                                 {              e.baz},
                                 {                   }]),
        # …
  """
  @spec cube([columns]) :: query
  defmacro cube(columns), do: query(columns, "CUBE")

  defp query(data, name) do
    list = to_sql data
    quote do: fragment(unquote(name <> " ?"), unquote(list))
  end

  defp to_sql({:{}, _, data}), do: to_sql(data)
  defp to_sql(tuple) when is_tuple(tuple) do
    to_sql Tuple.to_list tuple
  end
  defp to_sql(list) when is_list(list) do
    query = "?" |> List.duplicate(Enum.count(list)) |> Enum.join(",")

    quote do: fragment(unquote("(" <> query <> ")"), unquote_splicing(list))
  end
end
