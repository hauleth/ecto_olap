defmodule Ecto.OLAP.Window.Over do
  @moduledoc false

  defstruct [
    partition_by: nil,
    order_by: nil,
    range: nil,
    rows: nil,
  ]

  def parse(window) do
    %__MODULE__{
      partition_by: Keyword.get(window, :partition_by),
      order_by: Keyword.get(window, :order_by),
      range: Keyword.get(window, :range),
      rows: Keyword.get(window, :rows)
    }
    |> validate()
  end

  def to_query(%__MODULE__{} = query) do
    {q, params} =
      []
      |> rows(query)
      |> range(query)
      |> order_by(query)
      |> partition_by(query)
      |> build_query(" ")

    fragment(q, params)
  end

  defp partition_by(params, %__MODULE__{partition_by: nil}), do: params
  defp partition_by(params, %__MODULE__{partition_by: []}), do: params
  defp partition_by(params, %__MODULE__{partition_by: partition}) do
    {q, partition} = build_query(partition)

    [fragment("PARTITION BY " <> q, partition) | params]
  end

  defp order_by(params, %__MODULE__{order_by: nil}), do: params
  defp order_by(params, %__MODULE__{order_by: []}), do: params
  defp order_by(params, %__MODULE__{order_by: order}) when is_list(order) do
    {q, order} = build_query(order)
    parsed =
      order
      |> Enum.map(fn
        {:asc, field} -> fragment("? ASC", [field])
        {:desc, field} -> fragment("? DESC", [field])
        field -> fragment("?", [field])
      end)

    [fragment("ORDER BY " <> q, parsed) | params]
  end
  defp order_by(params, %__MODULE__{order_by: column}) do
    order_by(params, %__MODULE__{order_by: [column]})
  end

  defp rows(params, %__MODULE__{rows: nil}), do: params
  defp rows(params, %__MODULE__{rows: [start, last]}) do
    start_row = row_name(start, "PRECEDING")
    last_row = row_name(last, "FOLLOWING")

    [fragment("RANGE BETWEEN ? AND ?", [start_row, last_row]) | params]
  end
  defp rows(params, %__MODULE__{rows: start}) do
    start_row = row_name(start, "PRECEDING")

    [fragment("RANGE ?", [start_row]) | params]
  end

  defp range(params, %__MODULE__{range: nil}), do: params
  defp range(params, %__MODULE__{range: [start, last]}) do
    start_row = row_name(start, "PRECEDING")
    last_row = row_name(last, "FOLLOWING")

    [fragment("RANGE BETWEEN ? AND ?", [start_row, last_row]) | params]
  end
  defp range(params, %__MODULE__{range: start}) do
    start_row = row_name(start, "PRECEDING")

    [fragment("RANGE ?", [start_row]) | params]
  end

  defp row_name(:unbounded, suffix), do: fragment("UNBOUNDED #{suffix}")
  defp row_name(:current, _), do: fragment("CURRENT ROW")
  defp row_name(value, suffix) when is_integer(value) and value > 0 do
    fragment("? #{suffix}", [value])
  end

  defp build_query(items, joiner \\ ",")
  defp build_query(list, joiner) when is_list(list) do
    q = "?" |> List.duplicate(Enum.count(list)) |> Enum.join(joiner)

    {q, list}
  end
  defp build_query(item, _), do: {"?", [item]}

  defp fragment(string, params \\ []) do
    quote do: fragment(unquote(string), unquote_splicing(params))
  end

  defp validate(%__MODULE__{rows: rows, range: range})
  when not is_nil(rows) and not is_nil(range) do
    raise ArgumentError, message: "rows and range cannot be used in the same query"
  end
  defp validate(query), do: query
end
