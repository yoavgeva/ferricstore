defmodule Ferricstore.Server.Connection do
  @moduledoc """
  Ranch protocol handler for a single FerricStore client connection.

  Each accepted TCP connection spawns one `Connection` process. The process:

  1. Performs the `CLIENT HELLO 3` handshake (RESP3-only; rejects RESP2).
  2. Enters a receive loop, accumulating TCP chunks into a binary buffer.
  3. Parses all complete RESP3 frames from the buffer via `Ferricstore.Resp.Parser`.
  4. Dispatches each parsed command, accumulates responses (iodata), and sends
     them in one `:gen_tcp.send/2` call per receive (pipelining).
  5. Handles `QUIT` (send `+OK`, close) and `RESET` (send `+RESET`, reset state).
  6. Closes cleanly on TCP EOF or any transport error.

  ## Ranch protocol contract

  Ranch requires the protocol module to export `start_link/3` and the started
  process to call `:ranch.handshake/1` before reading from the socket.

  ## BEAM scheduler notes

  All I/O uses `active: false` (passive mode) so the receive loop blocks on
  `:gen_tcp.recv/3`. This keeps the process off the scheduler while waiting
  for data and avoids mailbox flooding on bursty clients.
  """

  @behaviour :ranch_protocol

  alias Ferricstore.Resp.{Encoder, Parser}

  # Connection state
  defstruct [:socket, :transport, buffer: ""]

  @type t :: %__MODULE__{
          socket: :inet.socket(),
          transport: module(),
          buffer: binary()
        }

  # ---------------------------------------------------------------------------
  # Ranch protocol entry point
  # ---------------------------------------------------------------------------

  @doc """
  Called by Ranch to start a new connection process.

  ## Parameters

    - `ref`       - Ranch listener ref (used for handshake).
    - `transport` - Transport module (`:ranch_tcp`).
    - `opts`      - Protocol options (unused).
  """
  @spec start_link(ref :: atom(), transport :: module(), opts :: map()) :: {:ok, pid()}
  def start_link(ref, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @doc false
  @spec init(ref :: atom(), transport :: module(), opts :: map()) :: :ok
  def init(ref, transport, _opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = transport.setopts(socket, active: false)
    state = %__MODULE__{socket: socket, transport: transport}
    loop(state)
  end

  # ---------------------------------------------------------------------------
  # Receive loop
  # ---------------------------------------------------------------------------

  defp loop(%__MODULE__{socket: socket, transport: transport} = state) do
    case transport.recv(socket, 0, :infinity) do
      {:ok, <<>>} ->
        # Zero-length read: half-close or OS TCP edge case — treat as disconnect.
        transport.close(socket)

      {:ok, data} ->
        handle_data(state, data)

      {:error, _reason} ->
        # Client disconnected or transport error — clean up silently
        transport.close(socket)
    end
  end

  defp handle_data(%__MODULE__{socket: socket, transport: transport} = state, data) do
    buffer = state.buffer <> data

    case Parser.parse(buffer) do
      {:ok, [], rest} ->
        loop(%{state | buffer: rest})

      {:ok, commands, rest} ->
        handle_parsed(state, commands, rest)

      {:error, _reason} ->
        send_response(socket, transport, Encoder.encode({:error, "ERR protocol error"}))
        transport.close(socket)
    end
  end

  defp handle_parsed(%__MODULE__{socket: socket, transport: transport} = state, commands, rest) do
    case dispatch_commands(commands, socket, transport) do
      :quit -> transport.close(socket)
      :continue -> loop(%{state | buffer: rest})
    end
  end

  # ---------------------------------------------------------------------------
  # Command dispatch
  # ---------------------------------------------------------------------------

  # Returns `:quit` to signal the loop should stop, or `:continue`.
  defp dispatch_commands(commands, socket, transport) do
    responses =
      Enum.reduce_while(commands, [], fn cmd, acc ->
        case handle_command(cmd) do
          {:quit, response} ->
            {:halt, {:quit, [response | acc]}}

          {:continue, response} ->
            {:cont, [response | acc]}
        end
      end)

    case responses do
      {:quit, acc} ->
        send_responses(socket, transport, Enum.reverse(acc))
        :quit

      acc ->
        send_responses(socket, transport, Enum.reverse(acc))
        :continue
    end
  end

  # ---------------------------------------------------------------------------
  # Individual command handlers
  # ---------------------------------------------------------------------------

  # Normalise any command form to {name, args} where name is uppercase binary.
  defp handle_command({:inline, tokens}) do
    handle_command(tokens)
  end

  defp handle_command([name | args]) when is_binary(name) do
    dispatch(String.upcase(name), args)
  end

  defp handle_command(_unknown) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"})}
  end

  # ---------------------------------------------------------------------------
  # Dispatch table
  # ---------------------------------------------------------------------------

  defp dispatch("HELLO", args), do: handle_hello(args)
  # CLIENT HELLO [version] is the two-token form sent by some Redis clients.
  defp dispatch("CLIENT", ["HELLO" | args]), do: handle_hello(args)
  defp dispatch("PING", args), do: handle_ping(args)
  defp dispatch("QUIT", _args), do: {:quit, Encoder.encode(:ok)}
  defp dispatch("RESET", _args), do: {:continue, Encoder.encode({:simple, "RESET"})}

  defp dispatch(cmd, _args) do
    {:continue,
     Encoder.encode({:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "})}
  end

  # ---------------------------------------------------------------------------
  # HELLO handler
  # ---------------------------------------------------------------------------

  defp handle_hello(["3" | _rest]) do
    {:continue, Encoder.encode(greeting_map())}
  end

  defp handle_hello([version | _rest]) when is_binary(version) do
    {:continue,
     Encoder.encode({:error, "NOPROTO this server does not support the requested protocol version"})}
  end

  defp handle_hello([]) do
    # HELLO with no version returns current server info (RESP3)
    {:continue, Encoder.encode(greeting_map())}
  end

  # ---------------------------------------------------------------------------
  # PING handler
  # ---------------------------------------------------------------------------

  defp handle_ping([]) do
    {:continue, Encoder.encode({:simple, "PONG"})}
  end

  defp handle_ping([message | _rest]) do
    {:continue, Encoder.encode(message)}
  end

  # ---------------------------------------------------------------------------
  # Greeting map
  # ---------------------------------------------------------------------------

  defp greeting_map do
    %{
      "server" => "ferricstore",
      "version" => "0.1.0",
      "proto" => 3,
      "id" => generate_client_id(),
      "mode" => "standalone",
      "role" => "master",
      "modules" => []
    }
  end

  defp generate_client_id do
    :erlang.unique_integer([:positive])
  end

  # ---------------------------------------------------------------------------
  # Response sending
  # ---------------------------------------------------------------------------

  defp send_responses(socket, transport, responses) do
    iodata = Enum.map(responses, & &1)
    :ok = transport.send(socket, iodata)
  end

  defp send_response(socket, transport, iodata) do
    :ok = transport.send(socket, iodata)
  end
end
