defmodule Ferricstore.Bitcask.NIF do
  @moduledoc false

  use Rustler, otp_app: :ferricstore, crate: "ferricstore_bitcask"

  # Each function body is the fallback if the NIF fails to load.
  def new(_path), do: :erlang.nif_error(:nif_not_loaded)
  def get(_store, _key), do: :erlang.nif_error(:nif_not_loaded)
  def put(_store, _key, _value, _expire_at_ms), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_store, _key), do: :erlang.nif_error(:nif_not_loaded)
  def keys(_store), do: :erlang.nif_error(:nif_not_loaded)
  def write_hint(_store), do: :erlang.nif_error(:nif_not_loaded)
end
