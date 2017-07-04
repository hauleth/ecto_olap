defmodule Ecto.OLAP.Statistics do
  @moduledoc """
  Set of macros providing additional aggregates for statistics counting.
  """

  @functions_yx [
    corr: """
    Compute correlation coefficient for given `Y` and `X` expressions
    """,
    regr_avgx: "Average of the independent variable `X`",
    regr_avgy: "Average of the dependent variable `Y`",
    regr_count: "Count rows where both expressions are nonnull",
    regr_intercept: "",
    regr_r2: "",
    regr_slope: "",
    regr_sxx: "",
    regr_sxy: "",
    regr_syy: "",
  ]

  for {name, doc} <- @functions_yx do
      @doc doc
      def unquote(name)(y, x), do: call_yx(unquote(name), y, x)
    end

  defp call_yx(name, y, x) do
    f = "#{name}(?, ?)"
    quote do: fragment(unquote(f), unquote(y), unquote(x))
  end
end
