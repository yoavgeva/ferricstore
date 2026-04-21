defmodule FerricstoreServer.Connection.TcpOpts do
  @moduledoc """
  Shared TCP socket option helpers for connection modules.

  Provides `set_cork/2` for TCP_CORK (Linux) / TCP_NOPUSH (macOS/BSD)
  to coalesce multiple small sends into fewer TCP segments.
  """

  @doc """
  Enables or disables TCP_CORK (Linux) / TCP_NOPUSH (macOS/BSD) on the socket.

  When corked, the kernel buffers small writes and sends them as a single
  TCP segment when uncorked or when the buffer fills. This reduces the
  number of syscalls and TCP segments for pipeline batches.
  """
  @spec set_cork(:inet.socket(), boolean()) :: :ok | {:error, term()}
  def set_cork(socket, enabled) do
    value = if enabled, do: 1, else: 0

    case :os.type() do
      {:unix, :linux} ->
        :inet.setopts(socket, [{:raw, 6, 3, <<value::native-32>>}])

      _ ->
        # TCP_NOPUSH on macOS/BSD doesn't reliably flush on clear — skip.
        # Production target is Linux; macOS is dev-only.
        _ = {socket, value}
        :ok
    end
  end
end
