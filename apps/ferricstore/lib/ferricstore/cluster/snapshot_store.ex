defmodule Ferricstore.Cluster.SnapshotStore do
  @moduledoc """
  Behaviour for pluggable object storage backends for cluster snapshots.

  Snapshot files are identified by string keys (e.g. `"snapshots/2026-04-06T12:00:00Z/shard_0.tar"`).
  Each adapter maps keys to its own storage scheme -- a local filesystem path, an S3
  object key, a GCS blob name, etc.

  ## Implementing an adapter

      defmodule MyApp.SnapshotStore.S3 do
        @behaviour Ferricstore.Cluster.SnapshotStore

        @impl true
        def upload(local_path, key, opts) do
          bucket = Keyword.fetch!(opts, :bucket)
          # ... upload to S3
        end

        # ... download, list, delete
      end

  ## Configuration

  Set the adapter and its options in application config:

      config :ferricstore, :snapshot_store,
        adapter: MyApp.SnapshotStore.S3,
        bucket: "my-snapshots"
  """

  @type key :: binary()
  @type opts :: keyword()

  @doc "Uploads a local file to the store under the given key."
  @callback upload(local_path :: binary(), key :: key(), opts :: opts()) ::
              :ok | {:error, term()}

  @doc "Downloads the object at `key` to `dest_path` on the local filesystem."
  @callback download(key :: key(), dest_path :: binary(), opts :: opts()) ::
              :ok | {:error, term()}

  @doc "Lists all keys matching the given prefix."
  @callback list(prefix :: binary(), opts :: opts()) ::
              {:ok, [key()]} | {:error, term()}

  @doc "Deletes the object at `key`."
  @callback delete(key :: key(), opts :: opts()) ::
              :ok | {:error, term()}
end
