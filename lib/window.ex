defmodule Ecto.OLAP.Window do
  defmacro window(aggr, [over: over]) do
    quote do: fragment("? OVER (?)", unquote(aggr), unquote(Ecto.OLAP.Window.Over.from_ast(over)))
  end

  @doc """
  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.Window
      iex>
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from i in "grouping",
      ...>   group_by: i.bar,
      ...>   select: %{count: count(i.bar), rank: window(rank(), over: [order: count(i.bar)])}
      [%{count: 2, rank: 1},
       %{count: 4, rank: 2},
       %{count: 4, rank: 2}]
  """
  defmacro rank, do: quote do: fragment("rank()")

  defmacro dense_rank, do: quote do: fragment("dense_rank()")

  defmacro percent_rank, do: quote do: fragment("percent_rank()")

  defmacro cume_dist, do: quote do: fragment("cume_dist()")
end

defmodule Ecto.OLAP.Window.Over do
  def from_ast([order: order]) do
    quote do: fragment("ORDER BY ?", unquote(order))
  end
  def from_ast([partition: partition]) do
    quote do: fragment("PARTITION BY ?", unquote(partition))
  end
end
