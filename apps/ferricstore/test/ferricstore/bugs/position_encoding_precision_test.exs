defmodule Ferricstore.Bugs.PositionEncodingPrecisionTest do
  @moduledoc """
  Regression test for Bug #5: Position encoding float precision loss.

  `encode_position` multiplies float by 10^17 and truncates. For positions near
  machine epsilon, different positions can collapse to the same encoded value
  after serialize/deserialize, causing list elements to have duplicate positions.

  File: apps/ferricstore/lib/ferricstore/store/compound_key.ex:189

  The bug: `trunc(frac_part * 100_000_000_000_000_000)` loses precision for
  values where the fractional part differs only in the last few bits of the
  IEEE 754 mantissa. Two distinct floats can produce the same encoded string.
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 30_000

  alias Ferricstore.Store.CompoundKey

  describe "encode_position precision" do
    test "very close positions produce different encoded strings" do
      # IEEE 754 double precision has ~15-17 significant decimal digits.
      # The encoding multiplies by 10^17 and truncates, which means
      # positions that differ only in the last bits of the mantissa
      # will collapse to the same encoded value.

      # These two values are distinct floats but their difference is
      # near the limit of float64 precision:
      a = 0.5
      b = 0.5 + :math.pow(2, -53)  # smallest increment at this magnitude

      # Verify they are actually different floats
      assert a != b, "Test setup error: a and b should be different floats"

      enc_a = CompoundKey.encode_position(a)
      enc_b = CompoundKey.encode_position(b)

      # If the bug exists, these will be identical despite a != b
      assert enc_a != enc_b,
        "encode_position collapsed two different floats to the same string: " <>
        "#{inspect(a)} and #{inspect(b)} both encode to #{inspect(enc_a)}"
    end

    test "positions from successive midpoint calculations remain distinguishable" do
      # Simulate what LINSERT does: repeatedly compute midpoints between
      # adjacent positions. After enough subdivisions, the positions will
      # be so close together that encode_position can't distinguish them.
      #
      # Start with positions 0.0 and 1.0, repeatedly take the midpoint.
      # After ~52 iterations (mantissa bits), midpoints collapse.

      left = 0.0
      right = 1.0

      # Do 55 midpoint subdivisions — well past float64 precision
      positions =
        Enum.reduce(1..55, [{left, right}], fn _i, [{l, _r} | _] = acc ->
          mid = (l + right) / 2.0
          [{mid, right} | acc]
        end)
        |> Enum.map(fn {pos, _} -> pos end)
        |> Enum.uniq()

      # Encode all unique positions
      encoded = Enum.map(positions, &CompoundKey.encode_position/1)
      unique_encoded = Enum.uniq(encoded)

      # If encoding preserves precision, all unique positions should
      # produce unique encoded strings
      assert length(unique_encoded) == length(encoded),
        "encode_position collapsed #{length(encoded) - length(unique_encoded)} " <>
        "distinct positions to duplicate encoded strings. " <>
        "#{length(encoded)} unique floats -> #{length(unique_encoded)} unique encodings"
    end

    test "round-trip encode/decode preserves value" do
      # Test that encode -> decode returns the same value.
      # Due to the trunc() in encoding, this will lose precision.
      positions = [
        0.1,
        0.3333333333333333,
        0.142857142857142857,
        1.0e-15,
        0.999999999999999
      ]

      for pos <- positions do
        encoded = CompoundKey.encode_position(pos)
        decoded = CompoundKey.decode_position(encoded)

        # The difference should be within machine epsilon
        diff = abs(pos - decoded)
        assert diff < 1.0e-15,
          "Round-trip precision loss for #{pos}: encoded=#{inspect(encoded)}, " <>
          "decoded=#{decoded}, diff=#{diff}"
      end
    end

    test "adjacent LINSERT midpoints stay ordered after encode/decode" do
      # Simulate the position generation that LINSERT does:
      # Start with two positions, compute midpoint, encode, decode,
      # and verify the decoded midpoint is still between the originals.

      left = 1000.0
      right = 1000.0 + 1.0e-10  # Very close positions

      mid = (left + right) / 2.0

      enc_left = CompoundKey.encode_position(left)
      enc_mid = CompoundKey.encode_position(mid)
      enc_right = CompoundKey.encode_position(right)

      # After encoding, the sort order must be preserved
      assert enc_left < enc_mid,
        "Encoded midpoint #{inspect(enc_mid)} should sort after left #{inspect(enc_left)}"

      assert enc_mid < enc_right,
        "Encoded midpoint #{inspect(enc_mid)} should sort before right #{inspect(enc_right)}"
    end
  end
end
