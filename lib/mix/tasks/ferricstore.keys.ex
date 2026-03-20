defmodule Mix.Tasks.Ferricstore.Keys do
  @moduledoc """
  Lists keys stored in FerricStore, with optional glob pattern filtering.

  ## Usage

      mix ferricstore.keys [pattern]

  ## Arguments

    * `pattern` (optional) -- a glob pattern to filter keys. Supports `*`
      (match any sequence of characters) and `?` (match exactly one character).
      When omitted, all keys are listed.

  ## Examples

      # List all keys
      mix ferricstore.keys

      # List keys matching a prefix
      mix ferricstore.keys "user:*"

      # List keys with a single-char wildcard
      mix ferricstore.keys "k?"

  ## Output

  Prints each matching key on its own line, followed by a summary count.
  """

  use Mix.Task

  @shortdoc "List keys with optional glob pattern filtering"

  @doc """
  Runs the keys task, listing keys that match the given pattern.

  ## Parameters

    * `args` -- list of command-line arguments. The first element, if present,
      is treated as a glob pattern.

  """
  @spec run(list()) :: :ok
  @impl Mix.Task
  def run(args) do
    ensure_started()

    pattern = List.first(args)
    all_keys = Ferricstore.Store.Router.keys()

    matched =
      if pattern do
        regex = glob_to_regex(pattern)
        Enum.filter(all_keys, &Regex.match?(regex, &1))
      else
        all_keys
      end

    sorted = Enum.sort(matched)

    Enum.each(sorted, fn key ->
      Mix.shell().info(key)
    end)

    if pattern do
      Mix.shell().info("\n#{length(sorted)} key(s) matching \"#{pattern}\"")
    else
      Mix.shell().info("\n#{length(sorted)} key(s) total")
    end

    :ok
  end

  defp ensure_started do
    Mix.Task.run("app.start")
  end

  @doc false
  @spec glob_to_regex(binary()) :: Regex.t()
  def glob_to_regex(pattern) do
    regex_str =
      pattern
      |> String.graphemes()
      |> Enum.map_join(&escape_glob_char/1)

    Regex.compile!("^#{regex_str}$")
  end

  defp escape_glob_char("*"), do: ".*"
  defp escape_glob_char("?"), do: "."
  defp escape_glob_char(char), do: Regex.escape(char)
end
