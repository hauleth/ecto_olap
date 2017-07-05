defmodule Ecto.OLAP.Statistics do
  @moduledoc """
  Set of macros providing additional aggregates for statistics counting.

  # Example data

  All examples assumes we have table `stats_agg` with content:

  | year | divorce_rate | marg_cons |
  | ---- | ------------ | --------- |
  | 2000 | 5.0          | 8.2       |
  | 2001 | 4.7          | 7.0       |
  | 2002 | 4.6          | 6.5       |
  | 2003 | 4.4          | 5.3       |
  | 2004 | 4.3          | 5.2       |
  | 2005 | 4.1          | 4.0       |
  | 2006 | 4.2          | 4.6       |
  | 2007 | 4.2          | 4.5       |
  | 2008 | 4.2          | 4.2       |
  | 2009 | 4.2          | 3.7       |


  Example data thanks to Tyler Vigen's [Spurious Correlations][corr]

    [corr]: http://tylervigen.com/view_correlation?id=1703 "Divorce rate in Maine correlates with Per capita consumption of margarine (US)"
  """

  @functions_yx [
    corr: """
    Compute correlation coefficient for given `y` and `x` expressions.

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: corr(e.divorce_rate, e.marg_cons)
        [0.9806576205544681]
    """,
    regr_avgx: """
    Average of the independent variable `x`.

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_avgx(e.divorce_rate, e.marg_cons)
        [5.320000000000001]
    """,
    regr_avgy: """
    Average of the dependent variable `y`.

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_avgy(e.divorce_rate, e.marg_cons)
        [4.390000000000001]
    """,
    regr_count: "Count rows where both expressions are nonnull.",
    regr_r2: """
    Square of the correlation coefficient.

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_r2(e.divorce_rate, e.marg_cons)
        [0.9616893687515513]
    """,
    regr_intercept: """
    y-intercept of the least-squares-fit linear equation determined
    by the `(x, y)` pairs.

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_intercept(e.divorce_rate, e.marg_cons)
        [3.363198179561455]
    """,
    regr_slope: """
    Slope of the least-squares-fit linear equation determined
    by the `(x, y)` pairs.

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_slope(e.divorce_rate, e.marg_cons)
        [0.1930078609846896]
    """,
    regr_sxx: """
    "Sum of squares" of independent variable.

        sum(x^2) - sum(x)^2
        -------------------
                 N

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_sxx(e.divorce_rate, e.marg_cons)
        [19.33599999999983]
    """,
    regr_sxy: """
    "Sum of products" of independent times dependent variable.

        sum(x * y) - sum(x) * sum(y)
        ----------------------------
                     N

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_sxy(e.divorce_rate, e.marg_cons)
        [3.7319999999999256]
    """,
    regr_syy: """
    "Sum of squares" of dependent variable.

        sum(y^2) - sum(y)^2
        -------------------
                 N

    ## Example

        iex> import Ecto.Query
        iex> import Ecto.OLAP.Statistics
        iex>
        iex> alias Ecto.Integration.TestRepo
        iex>
        iex> TestRepo.all from e in "stats_agg",
        ...>   select: regr_syy(e.divorce_rate, e.marg_cons)
        [0.7489999999999327]
    """,
  ]

  for {name, doc} <- @functions_yx do
    @doc doc
    defmacro unquote(name)(y, x), do: call_yx(unquote(name), y, x)
  end

  defp call_yx(name, y, x) do
    quote do: fragment(unquote("#{name}(?, ?)"), unquote(y), unquote(x))
  end
end
