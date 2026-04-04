defmodule FerricstoreServer.Resp.ParserBugHuntTest do
  @moduledoc """
  Adversarial / edge-case tests for the RESP3 parser.

  Each `describe` block targets one specific scenario that could reveal bugs
  in the parser's state machine: incomplete data, protocol violations,
  extreme sizes, concurrency, and boundary conditions.
  """
  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.Parser

  # ---------------------------------------------------------------------------
  # 1. Incomplete bulk string (truncated mid-value)
  # ---------------------------------------------------------------------------

  describe "incomplete bulk string (truncated mid-value)" do
    test "header complete but body has fewer bytes than declared length" do
      # $10 says 10 bytes follow, but only 5 are present — parser should
      # treat this as incomplete (waiting for more TCP data).
      assert {:ok, [], "$10\r\nhello"} = Parser.parse("$10\r\nhello")
    end

    test "body has exact byte count but trailing CRLF is missing" do
      # All 5 payload bytes are present, but the mandatory \r\n terminator
      # has not arrived yet.
      assert {:ok, [], "$5\r\nhello"} = Parser.parse("$5\r\nhello")
    end

    test "body has exact byte count but only \\r is present (no \\n)" do
      assert {:ok, [], "$5\r\nhello\r"} = Parser.parse("$5\r\nhello\r")
    end

    test "zero-byte body with missing terminator" do
      # $0 expects an empty body followed by \r\n — just the header is incomplete.
      assert {:ok, [], "$0\r\n"} = Parser.parse("$0\r\n")
    end

    test "header length line itself is truncated (no CRLF after length)" do
      assert {:ok, [], "$5"} = Parser.parse("$5")
    end

    test "only the $ prefix is present" do
      assert {:ok, [], "$"} = Parser.parse("$")
    end

    test "incomplete bulk string in a pipeline — first value parses, second does not" do
      # First value: complete integer. Second value: truncated bulk string.
      input = ":42\r\n$10\r\nhello"
      assert {:ok, [42], "$10\r\nhello"} = Parser.parse(input)
    end

    test "bulk string body truncated at a multi-byte UTF-8 boundary" do
      # The Japanese char "あ" is 3 bytes (\xe3\x81\x82). Declare 3 bytes,
      # supply only 2 — should be incomplete.
      <<b1, b2, _b3>> = "あ"
      partial_body = <<b1, b2>>
      input = "$3\r\n" <> partial_body
      assert {:ok, [], ^input} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Negative bulk string length
  # ---------------------------------------------------------------------------

  describe "negative bulk string length" do
    test "$-1 is the RESP nil bulk string" do
      assert {:ok, [nil], ""} = Parser.parse("$-1\r\n")
    end

    test "$-2 is an invalid length" do
      assert {:error, {:invalid_bulk_length, "-2"}} = Parser.parse("$-2\r\n")
    end

    test "$-100 is an invalid length" do
      assert {:error, {:invalid_bulk_length, "-100"}} = Parser.parse("$-100\r\n")
    end

    test "$-0 is treated as zero-length (valid)" do
      # Integer.parse("-0") => {0, ""}, and 0 >= 0 so it is accepted.
      # The body should be the empty string followed by CRLF.
      assert {:ok, [""], ""} = Parser.parse("$-0\r\n\r\n")
    end

    test "negative length with trailing garbage is an invalid length" do
      assert {:error, {:invalid_bulk_length, "-1abc"}} = Parser.parse("$-1abc\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Array with wrong element count
  # ---------------------------------------------------------------------------

  describe "array with wrong element count" do
    test "header says 3 elements but only 2 are present — incomplete" do
      input = "*3\r\n:1\r\n:2\r\n"
      assert {:ok, [], ^input} = Parser.parse(input)
    end

    test "header says 1 element but 3 are present — parses 1, rest parsed as pipeline" do
      # parse_all consumes the array (1 element), then continues the pipeline
      # loop and parses the two remaining integers as separate top-level values.
      input = "*1\r\n:1\r\n:2\r\n:3\r\n"
      assert {:ok, [[1], 2, 3], ""} = Parser.parse(input)
    end

    test "header says 0 elements but data follows — parses empty array, rest is remainder" do
      input = "*0\r\n:1\r\n"
      assert {:ok, [[], 1], ""} = Parser.parse(input)
    end

    test "header says 2 but only 1.5 elements present (bulk string truncated)" do
      input = "*2\r\n:1\r\n$10\r\nhello"
      assert {:ok, [], ^input} = Parser.parse(input)
    end

    test "negative array count other than -1 is invalid" do
      assert {:error, {:invalid_array_count, "-2"}} = Parser.parse("*-2\r\n")
    end

    test "non-numeric array count is invalid" do
      assert {:error, {:invalid_array_count, "abc"}} = Parser.parse("*abc\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Nested arrays 10 levels deep
  # ---------------------------------------------------------------------------

  describe "nested arrays 10 levels deep" do
    test "10-level nested array with a single integer leaf" do
      # Build *1\r\n*1\r\n*1\r\n ... :42\r\n
      depth = 10
      prefix = String.duplicate("*1\r\n", depth)
      input = prefix <> ":42\r\n"

      {:ok, [result], ""} = Parser.parse(input)

      # Unwrap 10 levels: [[[[[[[[[[42]]]]]]]]]]
      unwrapped =
        Enum.reduce(1..depth, result, fn _, acc ->
          assert is_list(acc), "Expected list at nesting level, got: #{inspect(acc)}"
          assert length(acc) == 1
          hd(acc)
        end)

      assert unwrapped == 42
    end

    test "10-level nested array where each level has 2 elements" do
      # Each level: *2\r\n<left><right>
      # Leaf level (depth 0) is *2\r\n:1\r\n:2\r\n (2 integers).
      # At depth N, each node doubles its children, so total leaves = 2^(N+1).
      # depth=10 => 2^11 = 2048 leaf integers.
      input = build_nested_binary_tree(10)
      {:ok, [result], ""} = Parser.parse(input)

      # Count all leaf integers
      leaves = count_leaves(result)
      assert leaves == round(:math.pow(2, 11))
    end

    test "deeply nested array that is incomplete at the leaf" do
      depth = 10
      prefix = String.duplicate("*1\r\n", depth)
      # The leaf integer is truncated — no CRLF.
      input = prefix <> ":42"
      assert {:ok, [], ^input} = Parser.parse(input)
    end

    test "10-level nested array with mixed types at deepest level" do
      depth = 9
      prefix = String.duplicate("*1\r\n", depth)
      # Innermost array has 3 elements of different types
      inner = "*3\r\n:1\r\n$3\r\nfoo\r\n+OK\r\n"
      input = prefix <> inner

      {:ok, [result], ""} = Parser.parse(input)

      # Unwrap 9 levels to get the inner list
      unwrapped =
        Enum.reduce(1..depth, result, fn _, acc ->
          hd(acc)
        end)

      assert unwrapped == [1, "foo", {:simple, "OK"}]
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Map with odd number of elements
  # ---------------------------------------------------------------------------

  describe "map with odd number of elements" do
    test "map header says 1 pair but only key is present — incomplete" do
      # %1 means 1 key-value pair; we provide only the key.
      input = "%1\r\n$3\r\nfoo\r\n"
      assert {:ok, [], ^input} = Parser.parse(input)
    end

    test "map header says 2 pairs but 3 elements present (1.5 pairs) — incomplete for second value" do
      # 2 pairs = 4 elements needed. We supply 3 elements (key, val, key).
      input = "%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n"
      assert {:ok, [], ^input} = Parser.parse(input)
    end

    test "map header says 0 pairs — empty map, remaining data is separate value" do
      input = "%0\r\n+leftover\r\n"
      assert {:ok, [%{}, {:simple, "leftover"}], ""} = Parser.parse(input)
    end

    test "map with negative pair count is invalid" do
      assert {:error, {:invalid_map_count, "-1"}} = Parser.parse("%-1\r\n")
    end

    test "map key parses but value is malformed — propagates error" do
      # Key is valid simple string, value is invalid integer
      input = "%1\r\n+key\r\n:notanumber\r\n"
      assert {:error, {:invalid_integer, "notanumber"}} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Very large integer (> i64 max)
  # ---------------------------------------------------------------------------

  describe "very large integer (> i64)" do
    test "integer type (:) parses values beyond i64 max (9223372036854775807)" do
      huge = 9_223_372_036_854_775_808
      input = ":#{huge}\r\n"
      assert {:ok, [^huge], ""} = Parser.parse(input)
    end

    test "integer type (:) parses values below i64 min (-9223372036854775808)" do
      tiny = -9_223_372_036_854_775_809
      input = ":#{tiny}\r\n"
      assert {:ok, [^tiny], ""} = Parser.parse(input)
    end

    test "big number type (() parses a 100-digit positive number" do
      big = String.duplicate("9", 100) |> String.to_integer()
      input = "(#{big}\r\n"
      assert {:ok, [^big], ""} = Parser.parse(input)
    end

    test "big number type (() parses a 100-digit negative number" do
      big = -(String.duplicate("9", 100) |> String.to_integer())
      input = "(#{big}\r\n"
      assert {:ok, [^big], ""} = Parser.parse(input)
    end

    test "integer type beyond i128 range still parses (Elixir bignum)" do
      # 2^128 = 340282366920938463463374607431768211456
      enormous = Integer.pow(2, 128)
      input = ":#{enormous}\r\n"
      assert {:ok, [^enormous], ""} = Parser.parse(input)
    end

    test "integer zero edge case" do
      assert {:ok, [0], ""} = Parser.parse(":0\r\n")
    end

    test "integer with leading zeros is still valid via Integer.parse" do
      # ":007\r\n" — Integer.parse("007") => {7, ""}, which is valid
      assert {:ok, [7], ""} = Parser.parse(":007\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Double with NaN / Inf
  # ---------------------------------------------------------------------------

  describe "double with NaN/Inf" do
    test "inf returns :infinity atom" do
      assert {:ok, [:infinity], ""} = Parser.parse(",inf\r\n")
    end

    test "-inf returns :neg_infinity atom" do
      assert {:ok, [:neg_infinity], ""} = Parser.parse(",-inf\r\n")
    end

    test "nan (lowercase) returns :nan atom" do
      assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
    end

    test "NaN (mixed case) returns :nan atom" do
      assert {:ok, [:nan], ""} = Parser.parse(",NaN\r\n")
    end

    test "NAN (uppercase) returns :nan atom" do
      assert {:ok, [:nan], ""} = Parser.parse(",NAN\r\n")
    end

    test "Inf (capitalized) is NOT recognized as infinity — treated as invalid double" do
      # The parser only matches lowercase "inf", so "Inf" hits parse_float_value
      # which calls Float.parse("Inf") — Elixir's Float.parse does NOT parse "Inf".
      assert {:error, {:invalid_double, "Inf"}} = Parser.parse(",Inf\r\n")
    end

    test "-Inf (capitalized) is NOT recognized — treated as invalid double" do
      assert {:error, {:invalid_double, "-Inf"}} = Parser.parse(",-Inf\r\n")
    end

    test "INF (uppercase) is NOT recognized — treated as invalid double" do
      assert {:error, {:invalid_double, "INF"}} = Parser.parse(",INF\r\n")
    end

    test "nan inside an array" do
      input = "*3\r\n,inf\r\n,-inf\r\n,nan\r\n"
      assert {:ok, [[:infinity, :neg_infinity, :nan]], ""} = Parser.parse(input)
    end

    test "double with empty string is invalid" do
      assert {:error, {:invalid_double, ""}} = Parser.parse(",\r\n")
    end

    test "double with only a sign is invalid" do
      assert {:error, {:invalid_double, "-"}} = Parser.parse(",-\r\n")
    end

    test "double with very small exponent" do
      assert {:ok, [1.0e-300], ""} = Parser.parse(",1.0e-300\r\n")
    end

    test "double negative zero" do
      {:ok, [result], ""} = Parser.parse(",-0.0\r\n")
      assert result == -0.0
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Empty simple string
  # ---------------------------------------------------------------------------

  describe "empty simple string" do
    test "empty simple string +\\r\\n" do
      assert {:ok, [{:simple, ""}], ""} = Parser.parse("+\r\n")
    end

    test "empty simple string followed by another value" do
      input = "+\r\n:42\r\n"
      assert {:ok, [{:simple, ""}, 42], ""} = Parser.parse(input)
    end

    test "multiple empty simple strings in a pipeline" do
      input = "+\r\n+\r\n+\r\n"
      assert {:ok, [{:simple, ""}, {:simple, ""}, {:simple, ""}], ""} = Parser.parse(input)
    end

    test "empty simple string inside an array" do
      input = "*2\r\n+\r\n+OK\r\n"
      assert {:ok, [[{:simple, ""}, {:simple, "OK"}]], ""} = Parser.parse(input)
    end

    test "empty simple string as a map key" do
      input = "%1\r\n+\r\n:1\r\n"
      assert {:ok, [%{{:simple, ""} => 1}], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # 9. Inline command with trailing whitespace
  # ---------------------------------------------------------------------------

  describe "inline command with trailing whitespace" do
    test "trailing spaces are stripped by String.split" do
      # String.split/1 splits on all whitespace and ignores leading/trailing.
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING   \r\n")
    end

    test "leading spaces are stripped by String.split" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("   PING\r\n")
    end

    test "tabs are treated as whitespace by String.split" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET\tkey\r\n")
    end

    test "multiple spaces between tokens are collapsed" do
      assert {:ok, [{:inline, ["SET", "foo", "bar"]}], ""} =
               Parser.parse("SET   foo   bar\r\n")
    end

    test "inline command that is only whitespace results in empty token list" do
      # String.split("   ") => []
      assert {:ok, [{:inline, []}], ""} = Parser.parse("   \r\n")
    end

    test "trailing tab and space mix" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING \t \r\n")
    end

    test "inline command with mixed whitespace between args" do
      assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
               Parser.parse("SET \t key \t value\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # 10. Binary data with \r\n inside bulk string
  # ---------------------------------------------------------------------------

  describe "binary data with \\r\\n inside bulk string" do
    test "single \\r\\n embedded in the middle of bulk payload" do
      # Payload: "he\r\nlo" = 6 bytes
      assert {:ok, ["he\r\nlo"], ""} = Parser.parse("$6\r\nhe\r\nlo\r\n")
    end

    test "multiple \\r\\n sequences inside bulk payload" do
      payload = "a\r\nb\r\nc"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk payload that is entirely \\r\\n characters" do
      payload = "\r\n\r\n\r\n"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk payload starting with \\r\\n" do
      payload = "\r\nhello"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk payload ending with \\r\\n (before the terminator \\r\\n)" do
      # Payload: "hello\r\n" = 7 bytes, then the real terminator \r\n
      payload = "hello\r\n"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk string with null bytes and \\r\\n" do
      payload = <<0, 1, 13, 10, 255, 13, 10, 0>>
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk payload of just \\r (no \\n)" do
      payload = "\r\r\r"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "array of bulk strings each containing \\r\\n" do
      b1 = "a\r\nb"
      b2 = "\r\n"
      input = "*2\r\n$#{byte_size(b1)}\r\n#{b1}\r\n$#{byte_size(b2)}\r\n#{b2}\r\n"
      assert {:ok, [[^b1, ^b2]], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # 11. Concurrent parsing from multiple processes
  # ---------------------------------------------------------------------------

  describe "concurrent parsing from multiple processes" do
    test "50 processes parsing different inputs concurrently all produce correct results" do
      parent = self()

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            # Each process parses a unique integer
            input = ":#{i}\r\n"
            result = Parser.parse(input)
            send(parent, {:done, i, result})
            result
          end)
        end

      results = Task.await_many(tasks, 5_000)

      for {result, i} <- Enum.zip(results, 1..50) do
        assert {:ok, [^i], ""} = result
      end
    end

    test "concurrent parsing of bulk strings with binary data" do
      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            payload = :crypto.strong_rand_bytes(100)
            len = byte_size(payload)
            input = "$#{len}\r\n#{payload}\r\n"
            {:ok, [parsed], ""} = Parser.parse(input)
            assert parsed == payload
            {i, :ok}
          end)
        end

      results = Task.await_many(tasks, 5_000)
      assert length(results) == 20
      assert Enum.all?(results, fn {_, status} -> status == :ok end)
    end

    test "concurrent parsing of complex nested structures" do
      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            # Each process parses a map containing an array
            input = "%1\r\n$4\r\ndata\r\n*3\r\n:1\r\n:2\r\n:3\r\n"
            {:ok, [result], ""} = Parser.parse(input)
            assert result == %{"data" => [1, 2, 3]}
            :ok
          end)
        end

      results = Task.await_many(tasks, 5_000)
      assert Enum.all?(results, &(&1 == :ok))
    end

    test "concurrent parsing of incomplete data does not corrupt state" do
      tasks =
        for _i <- 1..30 do
          Task.async(fn ->
            # Incomplete data — should safely return :ok with empty list
            input = "$100\r\nhello"
            {:ok, [], ^input} = Parser.parse(input)
            :ok
          end)
        end

      results = Task.await_many(tasks, 5_000)
      assert Enum.all?(results, &(&1 == :ok))
    end
  end

  # ---------------------------------------------------------------------------
  # 12. Parse 10MB bulk string
  # ---------------------------------------------------------------------------

  describe "10MB bulk string" do
    @tag :large_alloc
    test "parses a 10MB bulk string successfully" do
      size = 10 * 1024 * 1024
      payload = :binary.copy(<<0>>, size)
      input = "$#{size}\r\n#{payload}\r\n"

      assert {:ok, [result], ""} = Parser.parse(input)
      assert byte_size(result) == size
      assert result == payload
    end

    @tag :large_alloc
    test "10MB bulk string that is incomplete (missing 1 byte)" do
      size = 10 * 1024 * 1024
      # Provide size-1 bytes — should be incomplete
      partial_payload = :binary.copy(<<0>>, size - 1)
      input = "$#{size}\r\n#{partial_payload}"

      assert {:ok, [], ^input} = Parser.parse(input)
    end

    @tag :large_alloc
    test "10MB bulk string with realistic text data" do
      # 10 MB of repeated "abcdefghij" (10 chars)
      size = 10 * 1024 * 1024
      payload = :binary.copy("abcdefghij", div(size, 10))
      actual_size = byte_size(payload)
      input = "$#{actual_size}\r\n#{payload}\r\n"

      assert {:ok, [result], ""} = Parser.parse(input)
      assert byte_size(result) == actual_size
    end

    @tag :large_alloc
    test "10MB bulk string followed by another value in a pipeline" do
      size = 10 * 1024 * 1024
      payload = :binary.copy(<<65>>, size)
      input = "$#{size}\r\n#{payload}\r\n+OK\r\n"

      assert {:ok, [bulk, {:simple, "OK"}], ""} = Parser.parse(input)
      assert byte_size(bulk) == size
    end
  end

  # ---------------------------------------------------------------------------
  # Helper functions
  # ---------------------------------------------------------------------------

  # Builds a binary tree encoded as nested RESP3 arrays.
  # At depth 0, returns *2\r\n:1\r\n:2\r\n (leaf array with 2 integers).
  # At depth > 0, returns *2\r\n<left><right> (each child is the subtree).
  defp build_nested_binary_tree(0) do
    "*2\r\n:1\r\n:2\r\n"
  end

  defp build_nested_binary_tree(depth) do
    child = build_nested_binary_tree(depth - 1)
    "*2\r\n" <> child <> child
  end

  # Counts leaf integers in a nested list structure.
  defp count_leaves(list) when is_list(list) do
    Enum.reduce(list, 0, fn el, acc -> acc + count_leaves(el) end)
  end

  defp count_leaves(_leaf), do: 1
end
