defmodule FerricstoreServer.Connection.Sendfile do
  @moduledoc false

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  @sendfile_threshold_bytes Application.compile_env(
                              :ferricstore_server,
                              :sendfile_threshold,
                              65_536
                            )

  @doc """
  Handles the GET command with sendfile optimization for `:ranch_tcp` transport.
  Falls back to normal dispatch for non-sendfile cases.

  The `dispatch_normal_fn` parameter is a function `(cmd, args, state) -> result`
  used as fallback when sendfile is not applicable.
  """
  def dispatch_get([key], state, dispatch_normal_fn) do
    if in_pubsub_mode?(state) do
      {:continue,
       Encoder.encode({:error, "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"}),
       state}
    else
      fast_get(key, state, dispatch_normal_fn)
    end
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp fast_get(key, state, dispatch_normal_fn) do
    ns = state.sandbox_namespace
    lookup_key = if ns, do: ns <> key, else: key

    case Router.get_with_file_ref(state.instance_ctx, lookup_key) do
      {:hot, value} ->
        result = value
        new_state = ConnTracking.maybe_track_read("GET", [lookup_key], result, state)
        ConnTracking.maybe_notify_keyspace("GET", [lookup_key], result)
        ConnTracking.maybe_notify_tracking("GET", [lookup_key], result, state)
        {:continue, Encoder.encode(result), new_state}

      {:cold_ref, path, offset, size} when size >= @sendfile_threshold_bytes ->
        case do_sendfile_get(key, path, offset, size, state) do
          {:sent, new_state} -> {:continue, "", new_state}
          {:error_after_header, _reason} -> {:quit, "", state}
          :fallback -> dispatch_normal_fn.("GET", [key], state)
        end

      {:cold_ref, _path, _offset, _size} ->
        # Cold but below sendfile threshold -- read from Bitcask via GenServer
        dispatch_normal_fn.("GET", [key], state)

      {:cold_value, value} ->
        result = value
        new_state = ConnTracking.maybe_track_read("GET", [lookup_key], result, state)
        ConnTracking.maybe_notify_keyspace("GET", [lookup_key], result)
        ConnTracking.maybe_notify_tracking("GET", [lookup_key], result, state)
        {:continue, Encoder.encode(result), new_state}

      :miss ->
        # Fall back to normal dispatch for type checking (WRONGTYPE on non-string keys)
        dispatch_normal_fn.("GET", [key], state)
    end
  end

  defp do_sendfile_get(key, path, offset, size, state) do
    socket = state.socket

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          # RESP3 bulk string: $<len>\r\n<data>\r\n
          header = [?$, Integer.to_string(size), "\r\n"]
          trailer = "\r\n"

          set_cork(socket, true)

          case :gen_tcp.send(socket, header) do
            :ok ->
              case :file.sendfile(fd, socket, offset, size, []) do
                {:ok, _sent} ->
                  case :gen_tcp.send(socket, trailer) do
                    :ok ->
                      set_cork(socket, false)
                      new_state = ConnTracking.maybe_track_read("GET", [key], :sendfile_ok, state)
                      {:sent, new_state}

                    {:error, reason} ->
                      set_cork(socket, false)
                      {:error_after_header, reason}
                  end

                {:error, reason} ->
                  set_cork(socket, false)
                  {:error_after_header, reason}
              end

            {:error, _} ->
              set_cork(socket, false)
              :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _} ->
        :fallback
    end
  end

  defp set_cork(socket, enabled) do
    value = if enabled, do: 1, else: 0

    case :os.type() do
      {:unix, :linux} ->
        # TCP_CORK = 3, IPPROTO_TCP = 6
        :inet.setopts(socket, [{:raw, 6, 3, <<value::native-32>>}])

      {:unix, _bsd_or_darwin} ->
        # TCP_NOPUSH = 4 on macOS/BSD, IPPROTO_TCP = 6
        :inet.setopts(socket, [{:raw, 6, 4, <<value::native-32>>}])

      _ ->
        :ok
    end
  end

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false
  defp in_pubsub_mode?(state), do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0
end
