defmodule FerricstoreEcto.Test.Migrations do
  @moduledoc false
  use Ecto.Migration

  def change do
    create table(:users) do
      add :name, :string
      add :email, :string
      add :age, :integer
      add :active, :boolean, default: true
      timestamps(type: :utc_datetime)
    end

    create table(:posts) do
      add :title, :string
      add :body, :text
      add :user_id, references(:users)
      timestamps(type: :utc_datetime)
    end

    create table(:audit_logs) do
      add :action, :string
      timestamps(type: :utc_datetime)
    end

    create table(:countries) do
      add :name, :string
      add :code, :string
      timestamps(type: :utc_datetime)
    end
  end
end
