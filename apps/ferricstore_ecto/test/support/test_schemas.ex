defmodule FerricstoreEcto.Test.User do
  @moduledoc false
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable,
    ttl: :timer.minutes(5),
    strategy: :read_write

  schema "users" do
    field :name, :string
    field :email, :string
    field :age, :integer
    field :active, :boolean, default: true
    timestamps(type: :utc_datetime)
  end

  def changeset(user, attrs) do
    user
    |> Ecto.Changeset.cast(attrs, [:name, :email, :age, :active])
  end
end

defmodule FerricstoreEcto.Test.Post do
  @moduledoc false
  use Ecto.Schema

  # NOT cacheable -- no FerricStore.Ecto.Cacheable

  schema "posts" do
    field :title, :string
    field :body, :string
    belongs_to :user, FerricstoreEcto.Test.User
    timestamps(type: :utc_datetime)
  end

  def changeset(post, attrs) do
    post
    |> Ecto.Changeset.cast(attrs, [:title, :body, :user_id])
  end
end

defmodule FerricstoreEcto.Test.AuditLog do
  @moduledoc false
  use Ecto.Schema

  # NOT cacheable

  schema "audit_logs" do
    field :action, :string
    timestamps(type: :utc_datetime)
  end
end

defmodule FerricstoreEcto.Test.ReadOnlyCountry do
  @moduledoc false
  use Ecto.Schema
  use FerricStore.Ecto.Cacheable,
    ttl: :timer.hours(24),
    strategy: :read_only

  schema "countries" do
    field :name, :string
    field :code, :string
    timestamps(type: :utc_datetime)
  end
end
