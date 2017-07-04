defmodule Ecto.OLAP.GroupingSets do
  @moduledoc """
  Helpers for grouping in Ecto queries
  """

  @type columns :: tuple() | list()
  @opaque query :: tuple()

  @doc """

  """
  @spec grouping_sets(list(columns())) :: query()
  defmacro grouping_sets(groups) when is_list(groups) do
    queries = groups
              |> Enum.map(&columns_to_sql/1)

    query("GROUPING SETS", queries)
  end

  @doc """

  """
  @spec rollup(list(columns())) :: query()
  defmacro rollup(columns), do: query("ROLLUP", columns)

  @doc """

  """
  @spec cube(list(columns())) :: query()
  defmacro cube(columns), do: query("CUBE", columns)

  defp query(name, data) do
    quote do: fragment(unquote(name <> " ?"), unquote(columns_to_sql data))
  end

  defp columns_to_sql(tuple) when is_tuple(tuple) do
    columns_to_sql Tuple.to_list tuple
  end
  defp columns_to_sql(columns) when is_list(columns) do
    query = List.duplicate("?", Enum.count(columns)) |> Enum.join(",")

    quote do: fragment(unquote("(" <> query <> ")"), unquote_splicing(columns))
  end
end
