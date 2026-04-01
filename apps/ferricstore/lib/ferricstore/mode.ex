defmodule Ferricstore.Mode do
  @moduledoc """
  Runtime mode detection for FerricStore.

  FerricStore supports two operational modes:

    * `:standalone` -- full Redis-compatible server with TCP/TLS
      listener, RESP3 protocol handling, HTTP health endpoint, and Prometheus
      metrics endpoint. This is the mode used when FerricStore runs as its own
      OTP application (i.e., as a standalone database server).

    * `:embedded` -- core key-value engine only. No TCP listener, no RESP3
      parsing, no Ranch processes, no HTTP health endpoint. The host application
      interacts with FerricStore exclusively through the `FerricStore` Elixir
      module (the embedded API). This mode is intended for applications that
      want to use FerricStore as an in-process, embedded data store without
      exposing any network ports.

  ## Configuration

      # config/config.exs (or runtime.exs)
      config :ferricstore, :mode, :embedded

  When the `:mode` key is not set, FerricStore defaults to `:embedded`.
  The `ferricstore_server` release sets `mode: :standalone` in `runtime.exs`.

  ## What changes between modes

  | Component              | `:standalone` | `:embedded` |
  |------------------------|:------------:|:-----------:|
  | Shards / Raft / ETS    |      yes     |     yes     |
  | MemoryGuard            |      yes     |     yes     |
  | Merge scheduler        |      yes     |     yes     |
  | HLC / Stats            |      yes     |     yes     |
  | PubSub                 |      yes     |     yes     |
  | Ranch TCP listener     |      yes     |      no     |
  | Ranch TLS listener     |      yes     |      no     |
  | Health HTTP endpoint   |      yes     |      no     |
  | RESP3 parser (loaded)  |      yes     |  yes (*)    |
  | Connection handler     |      yes     |  yes (*)    |

  (*) Module code is still compiled and available but no processes are started.

  ## Runtime checks

  Other modules can query the current mode to adjust their behaviour:

      if Ferricstore.Mode.standalone?() do
        # ... only in standalone mode
      end

  For example, `Ferricstore.Metrics.connected_clients/0` returns 0 in embedded
  mode since there is no Ranch listener to query.
  """

  @typedoc "FerricStore operational mode."
  @type t :: :standalone | :embedded

  @doc """
  Returns the current operational mode.

  Reads from application env `{:ferricstore, :mode}`. Defaults to `:embedded`
  when the key is not set. The `ferricstore_server` release sets `:standalone`
  in `runtime.exs`.

  ## Examples

      iex> Ferricstore.Mode.current()
      :embedded

  """
  @spec current() :: t()
  def current do
    Application.get_env(:ferricstore, :mode, :embedded)
  end

  @doc """
  Returns `true` when running in standalone mode (TCP server + health endpoint).

  ## Examples

      iex> Ferricstore.Mode.standalone?()
      true

  """
  @spec standalone?() :: boolean()
  def standalone? do
    current() == :standalone
  end

  @doc """
  Returns `true` when running in embedded mode (no network listeners).

  ## Examples

      iex> Application.put_env(:ferricstore, :mode, :embedded)
      iex> Ferricstore.Mode.embedded?()
      true

  """
  @spec embedded?() :: boolean()
  def embedded? do
    current() == :embedded
  end
end
