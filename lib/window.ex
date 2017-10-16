defmodule Ecto.OLAP.Window do
  @moduledoc """
  Allows calling window functions

  ## Example

      iex> import Ecto.Query
      iex> import Ecto.OLAP.Window
      iex>
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "window",
      ...>   select: %{
      ...>     depname: entry.depname,
      ...>     salary: entry.salary,
      ...>     empno: entry.empno,
      ...>     max: window(max(entry.salary),
      ...>                 over: [partition_by: entry.depname])}
      [%{depname: "develop"  , empno: 11, max: 6000, salary: 5200},
       %{depname: "develop"  , empno: 7 , max: 6000, salary: 4200},
       %{depname: "develop"  , empno: 9 , max: 6000, salary: 4500},
       %{depname: "develop"  , empno: 8 , max: 6000, salary: 6000},
       %{depname: "develop"  , empno: 10, max: 6000, salary: 5200},
       %{depname: "personnel", empno: 5 , max: 3900, salary: 3500},
       %{depname: "personnel", empno: 2 , max: 3900, salary: 3900},
       %{depname: "sales"    , empno: 3 , max: 5000, salary: 4800},
       %{depname: "sales"    , empno: 1 , max: 5000, salary: 5000},
       %{depname: "sales"    , empno: 4 , max: 5000, salary: 4800}]
  """

  alias Ecto.OLAP.Window.Over

  @doc "See `Ecto.OLAP.Window` module documentation"
  defmacro window(func, options) do
    quote do
      fragment(unquote("? OVER (?)"),
               unquote(func),
               unquote(opts(options)))
    end
  end

  defp opts([over: over]) do
    over
    |> Over.parse
    |> Over.to_query
  end
end

defmodule Ecto.OLAP.Window.Over do
  @moduledoc false

  defstruct [
    partition_by: nil,
    order_by: nil,
    range: nil
  ]

  def parse(window) do
    %__MODULE__{
      partition_by: Keyword.get(window, :partition_by),
      order_by: Keyword.get(window, :order_by),
      range: Keyword.get(window, :range)
    }
  end

  def to_query(%__MODULE__{} = query) do
    {q, params} =
      []
      |> order_by(query)
      |> partition_by(query)
      |> build_query(" ")

    quote do: fragment(unquote(q), unquote_splicing(params))
  end

  defp partition_by(params, %__MODULE__{partition_by: nil}), do: params
  defp partition_by(params, %__MODULE__{partition_by: []}), do: params
  defp partition_by(params, %__MODULE__{partition_by: partition}) do
    {q, partition} = build_query(partition)
    fragment =
      quote do
        fragment(unquote("PARTITION BY " <> q), unquote_splicing(partition))
      end

    [fragment | params]
  end

  defp order_by(params, %__MODULE__{order_by: nil}), do: params
  defp order_by(params, %__MODULE__{order_by: []}), do: params
  defp order_by(params, %__MODULE__{order_by: order}) when is_list(order) do
    {q, order} = build_query(order)
    parsed =
      order
      |> Enum.map(fn
        {:asc, field} -> quote do: fragment("? ASC", unquote(field))
        {:desc, field} -> quote do: fragment("? DESC", unquote(field))
        field -> quote do: fragment("?", unquote(field))
      end)

    fragment =
      quote do
        fragment(unquote("ORDER BY " <> q), unquote_splicing(parsed))
      end

    [fragment | params]
  end

  defp build_query(items, joiner \\ ",")
  defp build_query(list, joiner) when is_list(list) do
    q = "?" |> List.duplicate(Enum.count(list)) |> Enum.join(joiner)

    {q, list}
  end
  defp build_query(item, _), do: {"?", [item]}
end
