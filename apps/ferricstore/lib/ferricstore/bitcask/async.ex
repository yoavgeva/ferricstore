defmodule Ferricstore.Bitcask.Async do
  @moduledoc """
  Elixir wrappers around the Tokio-backed async IO NIFs.

  Each function submits IO work to a Tokio runtime thread pool, then
  suspends the calling Elixir process on `receive` until the result
  arrives. While suspended, the BEAM scheduler is FREE to run other
  processes — this is the key benefit over synchronous NIFs.

  ## Message protocol

  The NIF returns `{:pending, :ok}` immediately. The Tokio worker thread
  sends `{:tokio_complete, :ok | :error, result}` back to the calling process
  when the IO completes.

  ## Usage

      # Instead of blocking a BEAM scheduler:
      {:ok, value} = NIF.get(store, key)

      # Use async (BEAM scheduler stays free):
      {:ok, value} = Ferricstore.Bitcask.Async.get(store, key)

  ## Timeout

  All functions accept an optional timeout (default 5000ms). If the Tokio
  worker does not respond within the timeout, `{:error, :timeout}` is
  returned. This protects against Tokio thread pool exhaustion or extremely
  slow IO.
  """

  alias Ferricstore.Bitcask.NIF

  @default_timeout 5_000

  @doc """
  Async GET — reads a value by key from the Bitcask store.

  Returns `{:ok, value}`, `{:ok, nil}`, or `{:error, reason}`.

  The calling process suspends on `receive` while the BEAM scheduler is
  free to run other processes.
  """
  @spec get(reference(), binary(), timeout()) :: {:ok, binary() | nil} | {:error, term()}
  def get(store, key, timeout \\ @default_timeout) do
    case NIF.get_async(store, key) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, value} -> {:ok, value}
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Async DELETE — writes a tombstone for the given key.

  Returns `{:ok, true}` (key existed), `{:ok, false}` (not found), or
  `{:error, reason}`.
  """
  @spec delete(reference(), binary(), timeout()) :: {:ok, boolean()} | {:error, term()}
  def delete(store, key, timeout \\ @default_timeout) do
    case NIF.delete_async(store, key) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, result} -> {:ok, result}
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Async PUT_BATCH — writes multiple key-value pairs with a single fsync.

  `batch` is a list of `{key, value, expire_at_ms}` tuples.
  Returns `:ok` or `{:error, reason}`.
  """
  @spec put_batch(reference(), [{binary(), binary(), non_neg_integer()}], timeout()) ::
          :ok | {:error, term()}
  def put_batch(store, batch, timeout \\ @default_timeout) do
    case NIF.put_batch_tokio_async(store, batch) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, :ok} -> :ok
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Async WRITE_HINT — writes the hint file for fast keydir rebuild.

  Returns `:ok` or `{:error, reason}`.
  """
  @spec write_hint(reference(), timeout()) :: :ok | {:error, term()}
  def write_hint(store, timeout \\ @default_timeout) do
    case NIF.write_hint_async(store) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, :ok} -> :ok
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Async PURGE_EXPIRED — purges all logically expired keys.

  Returns `{:ok, count}` or `{:error, reason}`.
  """
  @spec purge_expired(reference(), timeout()) :: {:ok, non_neg_integer()} | {:error, term()}
  def purge_expired(store, timeout \\ @default_timeout) do
    case NIF.purge_expired_async(store) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, count} -> {:ok, count}
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Async RUN_COMPACTION — runs compaction on the given file IDs.

  Returns `{:ok, {written, dropped, reclaimed}}` or `{:error, reason}`.
  """
  @spec run_compaction(reference(), [non_neg_integer()], timeout()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def run_compaction(store, file_ids, timeout \\ @default_timeout) do
    case NIF.run_compaction_async(store, file_ids) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, result} -> {:ok, result}
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      {:error, _} = err ->
        err
    end
  end
end
