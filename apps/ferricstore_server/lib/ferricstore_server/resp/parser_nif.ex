defmodule FerricstoreServer.Resp.ParserNif do
  @moduledoc """
  Rust NIF binding for RESP3 protocol parsing.

  Parses raw RESP3 wire bytes into Elixir terms. Falls back to the pure-Elixir
  `FerricstoreServer.Resp.Parser` when the NIF is not loaded.
  """
  use Rustler, otp_app: :ferricstore_server, crate: "resp_parser_nif", skip_compilation?: true

  @spec parse(binary(), non_neg_integer()) :: {:ok, list(), binary()} | {:error, term()} | :fallback
  def parse(_data, _max_value_size), do: :erlang.nif_error(:nif_not_loaded)
end
