defmodule Ferricstore.GlobMatcher do
  @moduledoc """
  Hand-written binary glob matcher that avoids runtime regex compilation.

  Replaces the `glob_to_regex/1` + `Regex.match?/2` pattern that was
  duplicated across 5 modules (server.ex, generic.ex, hash.ex, set.ex,
  config.ex). Each call to `Regex.compile!/1` costs ~1-5us of PCRE engine
  time; this binary matcher is O(n*m) worst-case but avoids all
  intermediate allocations (no grapheme list, no regex string, no compiled
  NFA).

  Supports:
    * `*` -- matches zero or more bytes
    * `?` -- matches exactly one byte
    * `[abc]` -- character class (matches any single listed byte)
    * `[^abc]` / `[!abc]` -- negated character class
    * `\\` -- escape next character (treat literally)
    * All other bytes match literally

  ## Examples

      iex> Ferricstore.GlobMatcher.match?("hello", "h*o")
      true

      iex> Ferricstore.GlobMatcher.match?("hello", "h?llo")
      true

      iex> Ferricstore.GlobMatcher.match?("hello", "world")
      false
  """

  @doc """
  Returns `true` if `subject` matches the glob `pattern`.

  ## Parameters

    - `subject` -- the binary to test
    - `pattern` -- glob pattern with `*`, `?`, `[...]` support

  ## Returns

  `true` if the entire subject matches the pattern, `false` otherwise.
  """
  @spec match?(binary(), binary()) :: boolean()
  def match?(subject, pattern) when is_binary(subject) and is_binary(pattern) do
    do_match(subject, pattern)
  end

  # Both exhausted: match
  defp do_match(<<>>, <<>>), do: true

  # Trailing stars match empty subject
  defp do_match(<<>>, <<"*", rest::binary>>), do: do_match(<<>>, rest)
  defp do_match(<<>>, _pattern), do: false

  # Star: try consuming zero chars (advance pattern) or one char (advance subject)
  defp do_match(<<_, subject_rest::binary>> = subject, <<"*", rest::binary>>) do
    do_match(subject, rest) or do_match(subject_rest, <<"*", rest::binary>>)
  end

  # Question mark: match exactly one byte
  defp do_match(<<_, subject_rest::binary>>, <<"?", pattern_rest::binary>>) do
    do_match(subject_rest, pattern_rest)
  end

  # Escaped character: match the next pattern byte literally
  defp do_match(<<c, subject_rest::binary>>, <<"\\", c, pattern_rest::binary>>) do
    do_match(subject_rest, pattern_rest)
  end

  defp do_match(_subject, <<"\\", _c, _pattern_rest::binary>>), do: false

  # Character class: [...]
  defp do_match(<<c, subject_rest::binary>>, <<"[", rest::binary>>) do
    case parse_char_class(rest) do
      {:ok, chars, negated, pattern_rest} ->
        in_class = c in chars

        if (negated and not in_class) or (not negated and in_class) do
          do_match(subject_rest, pattern_rest)
        else
          false
        end

      :error ->
        # Malformed class -- treat '[' as literal
        if c == ?[ do
          do_match(subject_rest, rest)
        else
          false
        end
    end
  end

  # Literal byte match
  defp do_match(<<c, subject_rest::binary>>, <<c, pattern_rest::binary>>) do
    do_match(subject_rest, pattern_rest)
  end

  # No match
  defp do_match(_subject, _pattern), do: false

  # Parse character class content up to closing ']'
  defp parse_char_class(<<"^", rest::binary>>), do: collect_class_chars(rest, [], true)
  defp parse_char_class(<<"!", rest::binary>>), do: collect_class_chars(rest, [], true)
  defp parse_char_class(rest), do: collect_class_chars(rest, [], false)

  defp collect_class_chars(<<"]", rest::binary>>, acc, negated), do: {:ok, acc, negated, rest}
  defp collect_class_chars(<<c, rest::binary>>, acc, negated), do: collect_class_chars(rest, [c | acc], negated)
  defp collect_class_chars(<<>>, _acc, _negated), do: :error
end
