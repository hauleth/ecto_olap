defmodule Ecto.OLAP.Window do
  @moduledoc """
  Allows calling window functions

  ## Table

  Table used in examples for this module:

  |  depname  | empno | salary | profit  |
  | --------- | ----- | ------ | ------- |
  | sales     |     1 |   5000 |  42 000 |
  | personnel |     2 |   3900 | -10 000 |
  | sales     |     3 |   4800 |  60 000 |
  | sales     |     4 |   4800 |  70 000 |
  | personnel |     5 |   3500 |   -4000 |
  | develop   |     7 |   4200 |   -1000 |
  | develop   |     8 |   6000 |   -1000 |
  | develop   |     9 |   4500 |   -1000 |
  | develop   |    10 |   5200 |   -1000 |
  | develop   |    11 |   5200 |   -1000 |

  ## Example

  Maximum over partition by department:

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
      [
        %{depname: "develop"  , empno: 11, max: 6000, salary: 5200},
        %{depname: "develop"  , empno:  7, max: 6000, salary: 4200},
        %{depname: "develop"  , empno:  9, max: 6000, salary: 4500},
        %{depname: "develop"  , empno:  8, max: 6000, salary: 6000},
        %{depname: "develop"  , empno: 10, max: 6000, salary: 5200},
        %{depname: "personnel", empno:  5, max: 3900, salary: 3500},
        %{depname: "personnel", empno:  2, max: 3900, salary: 3900},
        %{depname: "sales"    , empno:  3, max: 5000, salary: 4800},
        %{depname: "sales"    , empno:  1, max: 5000, salary: 5000},
        %{depname: "sales"    , empno:  4, max: 5000, salary: 4800},
      ]

  Rank ordered by salary:

      iex> import Ecto.Query
      iex> import Ecto.OLAP.Window
      iex>
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "window",
      ...>   order_by: [fragment("rank"), entry.empno],
      ...>   select: %{
      ...>     salary: entry.salary,
      ...>     empno: entry.empno,
      ...>     rank: window(fragment("RANK()"),
      ...>                  over: [order_by: [desc: entry.salary]])}
      [
        %{empno:  8, salary: 6000, rank:  1},
        %{empno: 10, salary: 5200, rank:  2},
        %{empno: 11, salary: 5200, rank:  2},
        %{empno:  1, salary: 5000, rank:  4},
        %{empno:  3, salary: 4800, rank:  5},
        %{empno:  4, salary: 4800, rank:  5},
        %{empno:  9, salary: 4500, rank:  7},
        %{empno:  7, salary: 4200, rank:  8},
        %{empno:  2, salary: 3900, rank:  9},
        %{empno:  5, salary: 3500, rank: 10},
      ]

  Cumulative sum of salaries:

      iex> import Ecto.Query
      iex> import Ecto.OLAP.Window
      iex>
      iex> alias Ecto.Integration.TestRepo
      iex>
      iex> TestRepo.all from entry in "window",
      ...>   order_by: entry.salary,
      ...>   select: %{
      ...>     salary: entry.salary,
      ...>     empno: entry.empno,
      ...>     sum: window(sum(entry.salary),
      ...>                 over: [order_by: entry.salary,
      ...>                        range: [:current, :unbounded]])}
      [
        %{empno:  5, salary: 3500, sum:  3500},
        %{empno:  2, salary: 3900, sum:  7400},
        %{empno:  7, salary: 4200, sum: 11600},
        %{empno:  9, salary: 4500, sum: 16100},
        %{empno:  4, salary: 4800, sum: 25700},
        %{empno:  3, salary: 4800, sum: 25700},
        %{empno:  1, salary: 5000, sum: 30700},
        %{empno: 11, salary: 5200, sum: 41100},
        %{empno: 10, salary: 5200, sum: 41100},
        %{empno:  8, salary: 6000, sum: 47100},
      ]
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
