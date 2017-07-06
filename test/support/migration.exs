defmodule Ecto.Integration.Migration do
  use Ecto.Migration

  def change do
    create table(:grouping) do
      add :foo, :string
      add :bar, :integer
      add :baz, :string
    end

    create table(:stats_agg) do
      add :year, :integer
      add :divorce_rate, :float
      add :marg_cons, :float
    end
  end
end
