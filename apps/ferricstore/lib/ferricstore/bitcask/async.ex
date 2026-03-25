defmodule Ferricstore.Bitcask.Async do
  @moduledoc """
  Elixir wrappers around the Tokio-backed async IO NIFs (v2 path-based).

  Each function submits IO work to a Tokio runtime thread pool, then
  suspends the calling Elixir process on `receive` until the result
  arrives. While suspended, the BEAM scheduler is FREE to run other
  processes — this is the key benefit over synchronous NIFs.

  ## Message protocol (v2)

  The v2 async NIFs use correlation IDs in the message protocol:
  `{:tokio_complete, correlation_id, :ok | :error, result}`.
  This prevents LIFO ordering bugs when multiple async operations are
  submitted concurrently.

  ## Timeout

  All functions accept an optional timeout (default 5000ms). If the Tokio
  worker does not respond within the timeout, `{:error, :timeout}` is
  returned. This protects against Tokio thread pool exhaustion or extremely
  slow IO.
  """

  alias Ferricstore.Bitcask.NIF

  @default_timeout 5_000

  # -------------------------------------------------------------------
  # v1 async IO functions (Store resource based)
  #
  # These use the older message protocol:
  # {:tokio_complete, :ok, result} or {:tokio_complete, :error, reason}
  # -------------------------------------------------------------------

  @doc """
  Async get from a v1 Store resource.
  Returns `{:ok, value | nil}` or `{:error, reason}`.
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

      other ->
        {:error, {:nif_error, other}}
    end
  end

  @doc """
  Async delete from a v1 Store resource.
  Returns `{:ok, boolean}` or `{:error, reason}`.
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

      other ->
        {:error, {:nif_error, other}}
    end
  end

  @doc """
  Async put_batch on a v1 Store resource.
  Returns `:ok` or `{:error, reason}`.
  """
  @spec put_batch(reference(), list(), timeout()) :: :ok | {:error, term()}
  def put_batch(store, batch, timeout \\ @default_timeout) do
    case NIF.put_batch_tokio_async(store, batch) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, _} -> :ok
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      other ->
        {:error, {:nif_error, other}}
    end
  end

  @doc """
  Async write_hint on a v1 Store resource.
  Returns `:ok` or `{:error, reason}`.
  """
  @spec write_hint(reference(), timeout()) :: :ok | {:error, term()}
  def write_hint(store, timeout \\ @default_timeout) do
    case NIF.write_hint_async(store) do
      {:pending, :ok} ->
        receive do
          {:tokio_complete, :ok, _} -> :ok
          {:tokio_complete, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      other ->
        {:error, {:nif_error, other}}
    end
  end

  @doc """
  Async purge_expired on a v1 Store resource.
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

      other ->
        {:error, {:nif_error, other}}
    end
  end

  # -------------------------------------------------------------------
  # v2 async IO functions (path-based, no Store resource)
  #
  # These use correlation IDs in the message protocol:
  # {:tokio_complete, correlation_id, :ok | :error, result}
  # -------------------------------------------------------------------

  @doc """
  Async pread at a specific offset in a data file.

  Returns `{:ok, value}`, `{:ok, nil}` (tombstone), or `{:error, reason}`.
  Uses a unique correlation_id to match the response.
  """
  @spec v2_pread_at(String.t(), non_neg_integer(), timeout()) ::
          {:ok, binary() | nil} | {:error, term()}
  def v2_pread_at(path, offset, timeout \\ @default_timeout) do
    corr_id = System.unique_integer([:positive, :monotonic])

    case NIF.v2_pread_at_async(self(), corr_id, path, offset) do
      :ok ->
        receive do
          {:tokio_complete, ^corr_id, :ok, value} -> {:ok, value}
          {:tokio_complete, ^corr_id, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      other ->
        {:error, {:nif_error, other}}
    end
  end

  @doc """
  Async batch pread from multiple file locations concurrently.

  `locations` is a list of `{path, offset}` tuples. Returns
  `{:ok, [value | nil, ...]}` or `{:error, reason}`.
  All reads run concurrently on Tokio worker threads.
  """
  @spec v2_pread_batch([{String.t(), non_neg_integer()}], timeout()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def v2_pread_batch(locations, timeout \\ @default_timeout) do
    corr_id = System.unique_integer([:positive, :monotonic])

    case NIF.v2_pread_batch_async(self(), corr_id, locations) do
      :ok ->
        receive do
          {:tokio_complete, ^corr_id, :ok, values} -> {:ok, values}
          {:tokio_complete, ^corr_id, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      other ->
        {:error, {:nif_error, other}}
    end
  end

  @doc """
  Async fsync a data file.

  Returns `:ok` or `{:error, reason}`. Offloads the potentially blocking
  fsync syscall to a Tokio worker thread.
  """
  @spec v2_fsync(String.t(), timeout()) :: :ok | {:error, term()}
  def v2_fsync(path, timeout \\ @default_timeout) do
    corr_id = System.unique_integer([:positive, :monotonic])

    case NIF.v2_fsync_async(self(), corr_id, path) do
      :ok ->
        receive do
          {:tokio_complete, ^corr_id, :ok, :ok} -> :ok
          {:tokio_complete, ^corr_id, :error, reason} -> {:error, reason}
        after
          timeout -> {:error, :timeout}
        end

      other ->
        {:error, {:nif_error, other}}
    end
  end
end
