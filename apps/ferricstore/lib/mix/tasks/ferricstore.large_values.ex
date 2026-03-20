defmodule Mix.Tasks.Ferricstore.LargeValues do
  @moduledoc """
  Scans the FerricStore keydir (ETS hot cache) for values exceeding a size
  threshold and reports them with their key, size, and owning shard.

  ## Usage

      mix ferricstore.large_values [--threshold <bytes>]

  ## Options

    * `--threshold` -- minimum value size in bytes to flag (default: 1048576,
      i.e. 1 MB). Values strictly larger than this threshold are reported.

  ## Output

  For each large value found, prints a line with the key name, value size
  in bytes, and the shard index that owns it. If no values exceed the
  threshold, prints a summary message.

  ## Examples

      # Scan with default 1 MB threshold
      mix ferricstore.large_values

      # Scan with a custom 10 KB threshold
      mix ferricstore.large_values --threshold 10240
  """

  use Mix.Task

  @shortdoc "List keys with values exceeding a size threshold"

  @default_threshold 1_048_576

  @doc """
  Runs the large_values scan task.

  ## Parameters

    * `args` -- command-line arguments. Accepts `--threshold <bytes>` to
      override the default 1 MB threshold.

  """
  @spec run(list()) :: :ok
  @impl Mix.Task
  def run(args) do
    ensure_started()

    threshold = parse_threshold(args)
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    large_entries = scan_large_entries(shard_count, threshold)

    if large_entries == [] do
      Mix.shell().info("No values exceed the threshold of #{threshold} bytes.")
    else
      Mix.shell().info(
        "Found #{length(large_entries)} value(s) exceeding #{threshold} bytes:\n"
      )

      Enum.each(large_entries, fn {key, size, shard_index} ->
        Mix.shell().info(
          "  key: #{key}  size: #{size} bytes  shard: #{shard_index}"
        )
      end)
    end

    :ok
  end

  defp ensure_started do
    Mix.Task.run("app.start")
  end

  @doc false
  @spec parse_threshold(list()) :: non_neg_integer()
  def parse_threshold(args) do
    case args do
      ["--threshold", value | _] ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> n
          _ -> @default_threshold
        end

      _ ->
        @default_threshold
    end
  end

  # Scans all shard ETS hot_cache tables for values exceeding the threshold.
  # Returns a list of `{key, byte_size, shard_index}` tuples sorted by size
  # descending.
  @spec scan_large_entries(non_neg_integer(), non_neg_integer()) ::
          [{binary(), non_neg_integer(), non_neg_integer()}]
  defp scan_large_entries(shard_count, threshold) do
    Enum.flat_map(0..(shard_count - 1), fn i ->
      hot_cache = :"hot_cache_#{i}"

      try do
        :ets.foldl(
          fn {key, value}, acc ->
            size = byte_size(value)

            if size > threshold do
              [{key, size, i} | acc]
            else
              acc
            end
          end,
          [],
          hot_cache
        )
      rescue
        ArgumentError -> []
      end
    end)
    |> Enum.sort_by(fn {_key, size, _shard} -> size end, :desc)
  end
end
