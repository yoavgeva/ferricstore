defmodule Ferricstore.Resp.ParserTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Resp.Parser

  # ---------------------------------------------------------------------------
  # Simple string (+)
  # ---------------------------------------------------------------------------

  describe "simple string" do
    test "parses a simple string" do
      assert {:ok, [{:simple, "OK"}], ""} = Parser.parse("+OK\r\n")
    end

    test "parses an empty simple string" do
      assert {:ok, [{:simple, ""}], ""} = Parser.parse("+\r\n")
    end

    test "parses a simple string with spaces" do
      assert {:ok, [{:simple, "hello world"}], ""} = Parser.parse("+hello world\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Simple error (-)
  # ---------------------------------------------------------------------------

  describe "simple error" do
    test "parses a simple error" do
      assert {:ok, [{:error, "ERR unknown command"}], ""} =
               Parser.parse("-ERR unknown command\r\n")
    end

    test "parses WRONGTYPE error" do
      msg = "WRONGTYPE Operation against a key holding the wrong kind of value"

      assert {:ok, [{:error, ^msg}], ""} =
               Parser.parse("-#{msg}\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Integer (:)
  # ---------------------------------------------------------------------------

  describe "integer" do
    test "parses a positive integer" do
      assert {:ok, [42], ""} = Parser.parse(":42\r\n")
    end

    test "parses zero" do
      assert {:ok, [0], ""} = Parser.parse(":0\r\n")
    end

    test "parses a negative integer" do
      assert {:ok, [-100], ""} = Parser.parse(":-100\r\n")
    end

    test "parses a large integer" do
      assert {:ok, [999_999_999_999], ""} = Parser.parse(":999999999999\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Bulk string ($)
  # ---------------------------------------------------------------------------

  describe "bulk string" do
    test "parses a bulk string" do
      assert {:ok, ["hello"], ""} = Parser.parse("$5\r\nhello\r\n")
    end

    test "parses an empty bulk string" do
      assert {:ok, [""], ""} = Parser.parse("$0\r\n\r\n")
    end

    test "parses nil bulk string ($-1)" do
      assert {:ok, [nil], ""} = Parser.parse("$-1\r\n")
    end

    test "parses bulk string containing CRLF" do
      assert {:ok, ["he\r\nlo"], ""} = Parser.parse("$6\r\nhe\r\nlo\r\n")
    end

    test "parses binary data in bulk string" do
      data = <<0, 1, 2, 255, 254>>
      input = "$5\r\n" <> data <> "\r\n"
      assert {:ok, [^data], ""} = Parser.parse(input)
    end

    test "returns error when bulk crlf terminator is missing" do
      # Header says 5 bytes but payload has no \r\n at position 5
      assert {:error, :bulk_crlf_missing} = Parser.parse("$5\r\nhelloXXXXX")
    end
  end

  # ---------------------------------------------------------------------------
  # Array (*)
  # ---------------------------------------------------------------------------

  describe "array" do
    test "parses an array of integers" do
      input = "*3\r\n:1\r\n:2\r\n:3\r\n"
      assert {:ok, [[1, 2, 3]], ""} = Parser.parse(input)
    end

    test "parses an empty array" do
      assert {:ok, [[]], ""} = Parser.parse("*0\r\n")
    end

    test "parses nil array (*-1)" do
      assert {:ok, [nil], ""} = Parser.parse("*-1\r\n")
    end

    test "parses a mixed-type array" do
      input = "*3\r\n:1\r\n$5\r\nhello\r\n+OK\r\n"
      assert {:ok, [[1, "hello", {:simple, "OK"}]], ""} = Parser.parse(input)
    end

    test "parses nested arrays" do
      input = "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n"
      assert {:ok, [[[1, 2], [3, 4]]], ""} = Parser.parse(input)
    end

    test "parses array with nil elements" do
      input = "*3\r\n$3\r\nfoo\r\n_\r\n$3\r\nbar\r\n"
      assert {:ok, [["foo", nil, "bar"]], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # Null (_)
  # ---------------------------------------------------------------------------

  describe "null" do
    test "parses null" do
      assert {:ok, [nil], ""} = Parser.parse("_\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Boolean (#)
  # ---------------------------------------------------------------------------

  describe "boolean" do
    test "parses true" do
      assert {:ok, [true], ""} = Parser.parse("#t\r\n")
    end

    test "parses false" do
      assert {:ok, [false], ""} = Parser.parse("#f\r\n")
    end

    test "returns error for invalid boolean" do
      assert {:error, {:invalid_boolean, "x"}} = Parser.parse("#x\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Double (,)
  # ---------------------------------------------------------------------------

  describe "double" do
    test "parses a positive double" do
      assert {:ok, [3.14], ""} = Parser.parse(",3.14\r\n")
    end

    test "parses a negative double" do
      assert {:ok, [-1.5], ""} = Parser.parse(",-1.5\r\n")
    end

    test "parses zero as double" do
      assert {:ok, [0.0], ""} = Parser.parse(",0\r\n")
    end

    test "parses an integer-like double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "parses inf double" do
      assert {:ok, [:infinity], ""} = Parser.parse(",inf\r\n")
    end

    test "parses -inf double" do
      assert {:ok, [:neg_infinity], ""} = Parser.parse(",-inf\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Big number (()
  # ---------------------------------------------------------------------------

  describe "big number" do
    test "parses a big positive number" do
      assert {:ok, [3_492_890_328_409_238_509_324_850_943_850_943_825_024_385], ""} =
               Parser.parse("(3492890328409238509324850943850943825024385\r\n")
    end

    test "parses a big negative number" do
      assert {:ok, [-99_999_999_999_999_999_999], ""} =
               Parser.parse("(-99999999999999999999\r\n")
    end

    test "parses big number zero" do
      assert {:ok, [0], ""} = Parser.parse("(0\r\n")
    end

    test "parses large negative big number" do
      assert {:ok, [-12_345_678_901_234_567_890], ""} =
               Parser.parse("(-12345678901234567890\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Blob error (!)
  # ---------------------------------------------------------------------------

  describe "blob error" do
    test "parses a blob error" do
      assert {:ok, [{:error, "SYNTAX invalid syntax"}], ""} =
               Parser.parse("!21\r\nSYNTAX invalid syntax\r\n")
    end

    test "parses an empty blob error" do
      assert {:ok, [{:error, ""}], ""} = Parser.parse("!0\r\n\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Verbatim string (=)
  # ---------------------------------------------------------------------------

  describe "verbatim string" do
    test "parses a verbatim text string" do
      assert {:ok, [{:verbatim, "txt", "Some string"}], ""} =
               Parser.parse("=15\r\ntxt:Some string\r\n")
    end

    test "parses a verbatim markdown string" do
      assert {:ok, [{:verbatim, "mkd", "# Hello"}], ""} =
               Parser.parse("=11\r\nmkd:# Hello\r\n")
    end

    test "parses verbatim string with empty data" do
      assert {:ok, [{:verbatim, "txt", ""}], ""} = Parser.parse("=4\r\ntxt:\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Map (%)
  # ---------------------------------------------------------------------------

  describe "map" do
    test "parses a simple map" do
      input = "%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n"
      assert {:ok, [%{"foo" => 1, "bar" => 2}], ""} = Parser.parse(input)
    end

    test "parses an empty map" do
      assert {:ok, [%{}], ""} = Parser.parse("%0\r\n")
    end

    test "parses a map with mixed value types" do
      input = "%2\r\n$4\r\nname\r\n$5\r\nAlice\r\n$3\r\nage\r\n:30\r\n"
      assert {:ok, [%{"name" => "Alice", "age" => 30}], ""} = Parser.parse(input)
    end

    test "map with duplicate keys — last value wins" do
      input = "%2\r\n+k\r\n:1\r\n+k\r\n:2\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == %{{:simple, "k"} => 2}
    end
  end

  # ---------------------------------------------------------------------------
  # Set (~)
  # ---------------------------------------------------------------------------

  describe "set" do
    test "parses a set" do
      input = "~3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == MapSet.new(["foo", "bar", "baz"])
    end

    test "parses an empty set" do
      assert {:ok, [result], ""} = Parser.parse("~0\r\n")
      assert result == MapSet.new()
    end

    test "set with duplicate elements deduplicates" do
      input = "~3\r\n+a\r\n+a\r\n+b\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert MapSet.size(result) == 2
      assert MapSet.member?(result, {:simple, "a"})
      assert MapSet.member?(result, {:simple, "b"})
    end
  end

  # ---------------------------------------------------------------------------
  # Push (>)
  # ---------------------------------------------------------------------------

  describe "push" do
    test "parses a push message" do
      input = ">3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
      assert {:ok, [{:push, ["message", "hello", "world"]}], ""} = Parser.parse(input)
    end

    test "parses an empty push" do
      assert {:ok, [{:push, []}], ""} = Parser.parse(">0\r\n")
    end

    test "push type with single element" do
      assert {:ok, [{:push, [{:simple, "msg"}]}], ""} = Parser.parse(">1\r\n+msg\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Inline commands
  # ---------------------------------------------------------------------------

  describe "inline commands" do
    test "parses PING inline command" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING\r\n")
    end

    test "parses SET inline command with args" do
      assert {:ok, [{:inline, ["SET", "foo", "bar"]}], ""} = Parser.parse("SET foo bar\r\n")
    end

    test "parses CLIENT HELLO 3 inline command" do
      assert {:ok, [{:inline, ["CLIENT", "HELLO", "3"]}], ""} =
               Parser.parse("CLIENT HELLO 3\r\n")
    end

    test "parses inline command with extra whitespace" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET  key\r\n")
    end

    test "parses empty inline command (just CRLF)" do
      assert {:ok, [{:inline, []}], ""} = Parser.parse("\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Partial reads (incomplete data)
  # ---------------------------------------------------------------------------

  describe "partial reads" do
    test "returns empty list and full buffer for partial simple string" do
      assert {:ok, [], "+OK\r"} = Parser.parse("+OK\r")
    end

    test "returns empty list for partial bulk string header" do
      assert {:ok, [], "$5\r"} = Parser.parse("$5\r")
    end

    test "returns empty list for partial bulk string data" do
      assert {:ok, [], "$5\r\nhel"} = Parser.parse("$5\r\nhel")
    end

    test "returns empty list for partial bulk string missing trailing CRLF" do
      assert {:ok, [], "$5\r\nhello"} = Parser.parse("$5\r\nhello")
    end

    test "returns empty list for partial array" do
      assert {:ok, [], "*3\r\n:1\r\n:2\r\n"} = Parser.parse("*3\r\n:1\r\n:2\r\n")
    end

    test "returns empty list for partial integer" do
      assert {:ok, [], ":42"} = Parser.parse(":42")
    end

    test "returns empty list for empty input" do
      assert {:ok, [], ""} = Parser.parse("")
    end

    test "returns empty list for partial inline" do
      assert {:ok, [], "PING"} = Parser.parse("PING")
    end

    test "returns empty list for partial map" do
      # Map header says 2 entries but only 1 key provided
      assert {:ok, [], "%2\r\n$3\r\nfoo\r\n"} = Parser.parse("%2\r\n$3\r\nfoo\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Pipelining
  # ---------------------------------------------------------------------------

  describe "pipelining" do
    test "parses multiple complete commands" do
      input = "+OK\r\n:42\r\n$5\r\nhello\r\n"
      assert {:ok, [{:simple, "OK"}, 42, "hello"], ""} = Parser.parse(input)
    end

    test "parses complete commands with trailing partial" do
      input = ":1\r\n:2\r\n:3\r"
      assert {:ok, [1, 2], ":3\r"} = Parser.parse(input)
    end

    test "parses multiple inline commands" do
      input = "PING\r\nSET foo bar\r\n"

      assert {:ok, [{:inline, ["PING"]}, {:inline, ["SET", "foo", "bar"]}], ""} =
               Parser.parse(input)
    end

    test "parses mixed typed and inline commands" do
      input = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+OK\r\n"
      assert {:ok, [["SET", "foo", "bar"], {:simple, "OK"}], ""} = Parser.parse(input)
    end

    test "returns all complete from a large pipeline" do
      input = Enum.map_join(1..100, "", fn i -> ":#{i}\r\n" end)
      assert {:ok, values, ""} = Parser.parse(input)
      assert values == Enum.to_list(1..100)
    end

    test "handles pipeline where first message is complete and second is partial" do
      assert {:ok, [{:simple, "OK"}], "+HE"} = Parser.parse("+OK\r\n+HE")
    end
  end

  # ---------------------------------------------------------------------------
  # Nested types
  # ---------------------------------------------------------------------------

  describe "nested types" do
    test "array of maps" do
      input = "*2\r\n%1\r\n$1\r\na\r\n:1\r\n%1\r\n$1\r\nb\r\n:2\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == [%{"a" => 1}, %{"b" => 2}]
    end

    test "map of arrays" do
      input = "%1\r\n$4\r\nkeys\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
      assert {:ok, [%{"keys" => ["a", "b"]}], ""} = Parser.parse(input)
    end

    test "array containing sets" do
      input = "*1\r\n~2\r\n:1\r\n:2\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == [MapSet.new([1, 2])]
    end

    test "deeply nested structure" do
      input = "*1\r\n*1\r\n*1\r\n:42\r\n"
      assert {:ok, [[[[42]]]], ""} = Parser.parse(input)
    end

    test "map with nested map values" do
      input = "%1\r\n$5\r\nouter\r\n%1\r\n$5\r\ninner\r\n:1\r\n"
      assert {:ok, [%{"outer" => %{"inner" => 1}}], ""} = Parser.parse(input)
    end

    test "push containing a map" do
      input = ">2\r\n$7\r\nmessage\r\n%1\r\n$3\r\nfoo\r\n:1\r\n"
      assert {:ok, [{:push, ["message", %{"foo" => 1}]}], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "bulk string with length zero" do
      assert {:ok, [""], ""} = Parser.parse("$0\r\n\r\n")
    end

    test "array with single element" do
      assert {:ok, [[{:simple, "OK"}]], ""} = Parser.parse("*1\r\n+OK\r\n")
    end

    test "multiple nil values" do
      input = "_\r\n$-1\r\n*-1\r\n"
      assert {:ok, [nil, nil, nil], ""} = Parser.parse(input)
    end

    test "simple string that looks like a number" do
      assert {:ok, [{:simple, "42"}], ""} = Parser.parse("+42\r\n")
    end

    test "bulk string with unicode" do
      # "hello" in Japanese: 3 bytes each = 15 bytes
      str = "helloworld"
      len = byte_size(str)
      input = "$#{len}\r\n#{str}\r\n"
      assert {:ok, [^str], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # Attribute type (|)
  # ---------------------------------------------------------------------------

  describe "attribute type" do
    test "parses an attribute type" do
      # |1 = one key-value pair: key=simple "key", value=integer 42
      input = "|1\r\n+key\r\n:42\r\n"
      assert {:ok, [{:attribute, %{{:simple, "key"} => 42}}], ""} = Parser.parse(input)
    end

    test "parses attribute followed by a value" do
      input = "|1\r\n+key\r\n:42\r\n+OK\r\n"

      assert {:ok, [{:attribute, %{{:simple, "key"} => 42}}, {:simple, "OK"}], ""} =
               Parser.parse(input)
    end

    test "parses empty attribute" do
      assert {:ok, [{:attribute, %{}}], ""} = Parser.parse("|0\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Blob error — additional edge cases
  # ---------------------------------------------------------------------------

  describe "blob error edge cases" do
    test "blob error with negative length returns error" do
      assert {:error, {:invalid_blob_error_length, -1}} = Parser.parse("!-1\r\n\r\n")
    end

    test "blob error with length mismatch is incomplete (not enough data)" do
      # Header says 5 bytes but payload has 11 — the parser reads only 5 bytes
      # then expects CRLF at position 5. "hello world" has " " at position 5, not CRLF.
      assert {:error, :bulk_crlf_missing} = Parser.parse("!5\r\nhello world\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Verbatim string — additional edge cases
  # ---------------------------------------------------------------------------

  describe "verbatim string edge cases" do
    test "verbatim string with length < 4 returns error" do
      assert {:error, {:invalid_verbatim_length, 3}} = Parser.parse("=3\r\nABC\r\n")
    end

    test "verbatim string with missing colon separator returns error" do
      # Length 4, payload "ABCD" — no colon after 3-byte encoding
      assert {:error, :invalid_verbatim_payload} = Parser.parse("=4\r\nABCD\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Null — additional edge cases
  # ---------------------------------------------------------------------------

  describe "null edge cases" do
    test "null with non-empty content returns error" do
      assert {:error, {:invalid_null, "garbage"}} = Parser.parse("_garbage\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Double — additional edge cases
  # ---------------------------------------------------------------------------

  describe "double edge cases" do
    test "parses NaN double (lowercase)" do
      assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
    end

    test "parses NaN double (mixed case)" do
      assert {:ok, [:nan], ""} = Parser.parse(",NaN\r\n")
    end

    test "parses NaN double (uppercase)" do
      assert {:ok, [:nan], ""} = Parser.parse(",NAN\r\n")
    end

    test "parses scientific notation double" do
      assert {:ok, [1.5e10], ""} = Parser.parse(",1.5e10\r\n")
    end

    test "parses scientific notation without decimal point" do
      assert {:ok, [1.0e5], ""} = Parser.parse(",1e5\r\n")
    end

    test "parses integer-form double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "invalid double returns error" do
      assert {:error, {:invalid_double, "notafloat"}} = Parser.parse(",notafloat\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Integer — additional edge cases
  # ---------------------------------------------------------------------------

  describe "integer edge cases" do
    test "invalid integer returns error" do
      assert {:error, {:invalid_integer, "abc"}} = Parser.parse(":abc\r\n")
    end

    test "float-like value in integer position returns error" do
      assert {:error, {:invalid_integer, "3.14"}} = Parser.parse(":3.14\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Boolean — additional edge cases (already one test, adding invalid)
  # ---------------------------------------------------------------------------

  describe "boolean edge cases" do
    test "invalid boolean value returns error" do
      assert {:error, {:invalid_boolean, "1"}} = Parser.parse("#1\r\n")
    end

    test "empty boolean returns error" do
      assert {:error, {:invalid_boolean, ""}} = Parser.parse("#\r\n")
    end
  end
end
