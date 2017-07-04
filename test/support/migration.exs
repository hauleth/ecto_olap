defmodule Ecto.Integration.Migration do
  use Ecto.Migration

  def change do
    create table(:example) do
      add :foo, :string
      add :bar, :integer
      add :baz, :string
    end
  end
end
