defmodule Ferricstore.Cluster.SnapshotStore.Manifest do
  @moduledoc """
  Builds and parses snapshot manifest JSON documents.

  A manifest describes a point-in-time snapshot of all shards. It records the
  Raft commit index for each shard at the moment the hardlink snapshot was
  taken, plus file-level checksums for integrity verification.

  ## Format

      %{
        "timestamp" => "2026-04-06T12:00:00Z",
        "shard_count" => 4,
        "shards" => [
          %{
            "index" => 0,
            "raft_index" => 45892,
            "tar_key" => "snapshots/2026-04-06T12:00:00Z/shard_0.tar.gz",
            "files" => [
              %{"name" => "00000.log", "size" => 67108864, "sha256" => "abc..."}
            ]
          }
        ]
      }
  """

  @doc """
  Builds a manifest map from shard snapshot results.

  ## Parameters

    * `timestamp` -- ISO 8601 timestamp of the snapshot
    * `shard_count` -- total number of shards
    * `shard_entries` -- list of per-shard maps with `:index`, `:raft_index`,
      `:tar_key`, and `:files` keys
  """
  @spec build(binary(), non_neg_integer(), [map()]) :: map()
  def build(timestamp, shard_count, shard_entries) do
    shards =
      Enum.map(shard_entries, fn entry ->
        %{
          "index" => entry.index,
          "raft_index" => entry.raft_index,
          "tar_key" => entry.tar_key,
          "files" =>
            Enum.map(entry.files, fn f ->
              %{"name" => f.name, "size" => f.size, "sha256" => f.sha256}
            end)
        }
      end)

    %{
      "timestamp" => timestamp,
      "shard_count" => shard_count,
      "shards" => shards
    }
  end

  @doc """
  Parses a manifest from a JSON string.

  Returns `{:ok, manifest}` or `{:error, reason}`.
  """
  @spec parse(binary()) :: {:ok, map()} | {:error, term()}
  def parse(json) do
    case Jason.decode(json) do
      {:ok, %{"timestamp" => _, "shard_count" => _, "shards" => _} = manifest} ->
        {:ok, manifest}

      {:ok, _other} ->
        {:error, :invalid_manifest_format}

      {:error, reason} ->
        {:error, {:json_decode_error, reason}}
    end
  end
end
