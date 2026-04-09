defmodule FerricstoreServer.Bugs.VerbatimEncodingTest do
  @moduledoc """
  Bug #7: Verbatim String Encoding Not Validated

  Encoder accepts verbatim string encoding of any length, but RESP3 spec
  requires exactly 3 bytes. Parser enforces this, so encoder can produce
  data the parser rejects.

  File: resp/encoder.ex:143
  """

  use ExUnit.Case, async: true
  @moduletag timeout: 30_000

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser

  describe "verbatim string encoding validation" do
    test "encoder should reject 4-byte encoding type" do
      # RESP3 spec: verbatim string encoding must be exactly 3 bytes (e.g. "txt", "mkd")
      # BUG: Encoder accepts any length — "abcd" is 4 bytes and should be rejected
      assert_raise ArgumentError, fn ->
        Encoder.encode({:verbatim, "abcd", "hello"})
      end
    end

    test "encoder should reject 2-byte encoding type" do
      # "tx" is only 2 bytes — should be rejected
      assert_raise ArgumentError, fn ->
        Encoder.encode({:verbatim, "tx", "hello"})
      end
    end

    test "encoder should reject 1-byte encoding type" do
      assert_raise ArgumentError, fn ->
        Encoder.encode({:verbatim, "t", "hello"})
      end
    end

    test "encoder should reject empty encoding type" do
      assert_raise ArgumentError, fn ->
        Encoder.encode({:verbatim, "", "hello"})
      end
    end

    test "encoder should accept exactly 3-byte encoding type" do
      # "txt" is exactly 3 bytes — should work fine
      result = Encoder.encode({:verbatim, "txt", "hello"}) |> IO.iodata_to_binary()
      assert is_binary(result)
    end

    test "encoder output with 3-byte encoding should be parseable" do
      # Verify the round-trip works for valid 3-byte encoding
      encoded = Encoder.encode({:verbatim, "txt", "hello"}) |> IO.iodata_to_binary()
      assert {:ok, [_parsed], ""} = Parser.parse(encoded)
    end

    test "encoder output with invalid encoding produces unparseable data" do
      # Demonstrate the actual bug: encoder produces data that the parser rejects.
      # With 4-byte encoding, the encoded form has wrong structure for RESP3.
      encoded = Encoder.encode({:verbatim, "abcd", "hello"}) |> IO.iodata_to_binary()

      # The parser should either fail to parse this or parse it incorrectly,
      # because RESP3 expects exactly 3 bytes before the ":" separator.
      case Parser.parse(encoded) do
        {:ok, [parsed_value], ""} ->
          # If the parser accepts it, the parsed encoding should differ from "abcd"
          # because RESP3 always reads exactly 3 bytes for the encoding type.
          # This would indicate encoder/parser disagreement.
          flunk(
            "Encoder produced data with 4-byte encoding that parser accepted: #{inspect(parsed_value)}. " <>
              "Encoder should validate encoding length to prevent this inconsistency."
          )

        {:error, _} ->
          # Parser rejected it — proves the encoder produced invalid data
          flunk(
            "Encoder produced verbatim string with 4-byte encoding that parser rejects. " <>
              "Encoder should validate and reject non-3-byte encodings upfront."
          )

        other ->
          flunk("Unexpected parser result for invalid verbatim encoding: #{inspect(other)}")
      end
    end
  end
end
