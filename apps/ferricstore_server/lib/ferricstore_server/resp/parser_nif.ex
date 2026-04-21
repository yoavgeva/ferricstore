defmodule FerricstoreServer.Resp.ParserNif do
  use Rustler, otp_app: :ferricstore_server, crate: "resp_parser_nif", skip_compilation?: true

  @spec parse(binary(), non_neg_integer()) :: {:ok, list(), binary()} | {:error, term()} | :fallback
  def parse(_data, _max_value_size), do: :erlang.nif_error(:nif_not_loaded)
end
