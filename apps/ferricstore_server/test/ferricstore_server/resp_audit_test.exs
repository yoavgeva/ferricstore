defmodule FerricstoreServer.RespAuditTest do
  @moduledoc """
  Tests for RESP encoder/parser audit fixes that were originally in ferricstore.
  Moved here because they depend on FerricstoreServer.Resp.{Parser, Encoder}.
  """

  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.{Encoder, Parser}

  # H2: Encoder single-pass counting
  describe "H2: encoder single-pass list counting" do
    test "array encoding correct" do
      result = Encoder.encode([1, 2, 3]) |> IO.iodata_to_binary()
      assert result == "*3\r\n:1\r\n:2\r\n:3\r\n"
    end

    test "empty array" do
      result = Encoder.encode([]) |> IO.iodata_to_binary()
      assert result == "*0\r\n"
    end

    test "push encoding correct" do
      result = Encoder.encode({:push, ["a", "b"]}) |> IO.iodata_to_binary()
      assert result == ">2\r\n$1\r\na\r\n$1\r\nb\r\n"
    end

    test "nested array encoding" do
      result = Encoder.encode([[1, 2], [3]]) |> IO.iodata_to_binary()
      assert result == "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"
    end

    test "MapSet encoding" do
      set = MapSet.new(["a", "b"])
      result = Encoder.encode(set) |> IO.iodata_to_binary()
      assert String.starts_with?(result, "~2\r\n")
    end
  end

  # C2: RESP3 inline parser uses :binary.split
  describe "C2: inline parser uses :binary.split" do
    test "splits on spaces" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING\r\n")
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET key\r\n")
    end

    test "handles multiple spaces" do
      assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
               Parser.parse("SET  key  value\r\n")
    end

    test "handles tabs as whitespace" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET\tkey\r\n")
    end

    test "handles mixed whitespace" do
      assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
               Parser.parse("SET \t key \t value\r\n")
    end

    test "trailing whitespace trimmed" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING \t \r\n")
    end
  end

  # C4: parse_float_value uses binary matching
  describe "C4: parse_float_value uses binary matching" do
    test "regular float" do
      assert {:ok, [1.5], ""} = Parser.parse(",1.5\r\n")
    end

    test "integer-form double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "scientific notation without dot" do
      assert {:ok, [float], ""} = Parser.parse(",1e5\r\n")
      assert_in_delta float, 1.0e5, 0.001
    end

    test "scientific notation with dot" do
      assert {:ok, [float], ""} = Parser.parse(",1.5e10\r\n")
      assert_in_delta float, 1.5e10, 1.0
    end

    test "negative scientific notation" do
      assert {:ok, [float], ""} = Parser.parse(",1.0E-3\r\n")
      assert_in_delta float, 0.001, 0.0001
    end

    test "special values" do
      assert {:ok, [:infinity], ""} = Parser.parse(",inf\r\n")
      assert {:ok, [:neg_infinity], ""} = Parser.parse(",-inf\r\n")
      assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
    end
  end

  # RESP parser value size limits
  describe "RESP parser value size limits" do
    test "bulk string within default limit parses successfully" do
      data = "$5\r\nhello\r\n"
      assert {:ok, ["hello"], ""} = Parser.parse(data)
    end

    test "bulk string exceeding custom limit returns error" do
      data = "$100\r\n" <> String.duplicate("x", 100) <> "\r\n"
      assert {:error, {:value_too_large, 100, 50}} = Parser.parse(data, 50)
    end

    test "hard cap of 64MB enforced regardless of config" do
      assert Parser.hard_cap_bytes() == 67_108_864

      huge_len = 67_108_865
      data = "$#{huge_len}\r\n"
      assert {:error, {:value_too_large, ^huge_len, _}} = Parser.parse(data, 100_000_000)
    end

    test "bulk string at exact limit parses successfully" do
      limit = 10
      data = "$10\r\n" <> String.duplicate("x", 10) <> "\r\n"
      assert {:ok, [val], ""} = Parser.parse(data, limit)
      assert byte_size(val) == 10
    end

    test "null bulk string always parses" do
      assert {:ok, [nil], ""} = Parser.parse("$-1\r\n")
    end
  end

  # M1: ClientTracking BCAST uses ets.select
  describe "M1: BCAST tracking uses ets.select" do
    test "notify_key_modified does not crash with empty tracking tables" do
      result =
        FerricstoreServer.ClientTracking.notify_key_modified(
          "test_key",
          self(),
          fn _pid, _msg, _keys -> :ok end
        )

      assert result == :ok
    end
  end
end
