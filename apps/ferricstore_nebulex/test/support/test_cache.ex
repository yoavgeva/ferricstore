defmodule FerricstoreNebulex.TestCache do
  @moduledoc false

  use Nebulex.Cache,
    otp_app: :ferricstore_nebulex,
    adapter: FerricstoreNebulex.Adapter
end
