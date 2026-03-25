defmodule FerricstoreEcto.TestRepo do
  @moduledoc false
  use FerricStore.Ecto.CachedRepo,
    otp_app: :ferricstore_ecto,
    adapter: Ecto.Adapters.SQLite3
end
