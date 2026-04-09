defmodule FerricstoreServer.Connection do
  @moduledoc """
  Ranch protocol handler for a single FerricStore client connection.

  Each accepted TCP connection spawns one `Connection` process. The process:

  1. Performs the `CLIENT HELLO 3` handshake (RESP3-only; rejects RESP2).
  2. Enters a receive loop, accumulating TCP chunks into a binary buffer.
  3. Parses all complete RESP3 frames from the buffer via `FerricstoreServer.Resp.Parser`.
  4. Dispatches commands using a **sliding window pipeline** (spec section 2C.2):
     - All "pure" commands (those that don't mutate connection state) in a
       pipeline batch are dispatched concurrently as `Task`s.
     - Responses are sent over the socket in-order: response N is sent as
       soon as responses 0..N are all complete. This means fast commands
       before a slow command get their responses delivered immediately,
       without waiting for the slow command to finish.
     - Stateful commands (MULTI, AUTH, SUBSCRIBE, blocking ops, etc.) act
       as barriers: all prior concurrent tasks are awaited and flushed
       before the stateful command executes synchronously.
  5. Handles `QUIT` (send `+OK`, close) and `RESET` (send `+RESET`, reset state).
  6. Closes cleanly on TCP EOF or any transport error.

  ## Transaction support (MULTI/EXEC/DISCARD/WATCH)

  Transactions are connection-level state. When `MULTI` is issued, the connection
  enters `:queuing` mode. Subsequent commands (except EXEC, DISCARD, MULTI, WATCH,
  UNWATCH) are queued instead of executed, returning `+QUEUED` to the client.

  `EXEC` executes all queued commands sequentially and returns an array of results.
  If `WATCH` was used and any watched key's shard write-version changed, `EXEC`
  returns nil (transaction aborted).

  `DISCARD` clears the queue and watched keys, returning to normal mode.

  ## Ranch protocol contract

  Ranch requires the protocol module to export `start_link/3` and the started
  process to call `:ranch.handshake/1` before reading from the socket.

  ## BEAM scheduler notes (active: N mode)

  The socket operates in `active: N` mode (default N=100): the kernel
  delivers exactly one `{:tcp, socket, data}` message to the process, then
  automatically switches the socket to passive. The process re-arms via
  delivers N messages then sends `{:tcp_passive, socket}`, at which point
  we re-arm with `transport.setopts(socket, active: N)`.

  This is superior to both `active: false` (blocking recv) and `active: true`
  (unbounded mailbox flooding):
  - The process can handle OTHER messages (waiter notifications, pub/sub pushes,
    client tracking invalidations) between TCP reads.
  - The BEAM scheduler can schedule other processes while waiting for TCP data.
  - Sliding window responses can be sent incrementally.
  - No risk of mailbox flooding (unlike `active: true`) since at most N messages
    is delivered at a time.
  """

  @behaviour :ranch_protocol

  alias Ferricstore.AuditLog
  alias FerricstoreServer.ClientTracking
  alias Ferricstore.Commands.Dispatcher
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Blocking, as: ConnBlocking
  alias FerricstoreServer.Connection.Pipeline, as: ConnPipeline
  alias FerricstoreServer.Connection.PubSub, as: ConnPubSub
  alias FerricstoreServer.Connection.Sendfile, as: ConnSendfile
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking
  alias FerricstoreServer.Connection.Transaction, as: ConnTransaction

  alias Ferricstore.PubSub, as: PS

  # Connection safety limits -- prevent unbounded memory growth per connection.
  # Maximum receive buffer size before the connection is closed (128 MB).
  @max_buffer_size 134_217_728

  # Connection state
  defstruct [
    :socket,
    :transport,
    :client_id,
    :client_name,
    :created_at,
    :peer,
    :instance_ctx,
    buffer: "",
    multi_state: :none,
    multi_queue: [],
    multi_queue_count: 0,
    watched_keys: %{},
    authenticated: false,
    username: "default",
    sandbox_namespace: nil,
    pubsub_channels: nil,
    pubsub_patterns: nil,
    tracking: nil,
    acl_cache: nil,
    active_mode: 100
  ]

  @type multi_state :: :none | :queuing

  @typedoc """
  Cached ACL permissions for the current user. Populated on AUTH and connection
  init, used for O(1) command permission checks without ETS lookups.
  """
  @type acl_cache :: %{
          commands: :all | MapSet.t(binary()),
          denied_commands: MapSet.t(binary()),
          keys: :all | [FerricstoreServer.Acl.key_pattern()],
          enabled: boolean()
        }
        | nil

  @type t :: %__MODULE__{
          socket: :inet.socket(),
          transport: module(),
          buffer: binary(),
          client_id: pos_integer(),
          client_name: binary() | nil,
          created_at: integer(),
          peer: {:inet.ip_address(), :inet.port_number()} | nil,
          instance_ctx: FerricStore.Instance.t(),
          multi_state: multi_state(),
          multi_queue: [{binary(), [binary()]}],
          multi_queue_count: non_neg_integer(),
          watched_keys: %{binary() => non_neg_integer()},
          tracking: ClientTracking.tracking_config() | nil,
          acl_cache: acl_cache()
        }

  # Commands that are NOT queued during MULTI — they are always executed immediately.
  @multi_passthrough_cmds ~w(EXEC DISCARD MULTI WATCH UNWATCH)

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

    # Enforce require-tls: reject plaintext connections when TLS is required.
    if transport == :ranch_tcp and require_tls?() do
      error_msg = Encoder.encode({:error, "ERR TLS required: plaintext connections are not permitted"})
      # transport.send accepts iodata directly; no need to flatten to binary.
      transport.send(socket, error_msg)
      transport.close(socket)
    else
      # active: N delivers N TCP messages before the socket goes passive,
      # then sends {:tcp_passive, socket}. We re-arm in the receive loop.
      # N=100 balances throughput (batch of 100 messages without setopts
      # overhead) with back-pressure (mailbox can't grow beyond ~100 messages).
      # active: true has no back-pressure — mailbox can flood under load.
      active_mode = Application.get_env(:ferricstore, :socket_active_mode, 100)
      :ok = transport.setopts(socket, active: active_mode)

      Stats.incr_connections()

      peer =
        case transport.peername(socket) do
          {:ok, addr} -> addr
          _ -> nil
        end

      # Fix 3: Protected mode -- reject non-localhost connections when no ACL
      # users are configured and protected mode is active.
      case FerricstoreServer.Acl.check_protected_mode(peer) do
        {:error, reason} ->
          error_msg = Encoder.encode({:error, reason})
          # transport.send accepts iodata directly; no need to flatten to binary.
          transport.send(socket, error_msg)
          Stats.decr_connections()
          transport.close(socket)

        :ok ->
          # Populate ACL cache for the default user at connection init.
          # This avoids ETS lookups on every command for the common case.
          default_cache = ConnAuth.build_acl_cache("default")

          # Join the ACL invalidation process group so we receive
          # {:acl_invalidate, username} messages when ACL rules change.
          join_acl_invalidation_group()

          # Transitional: build instance ctx from global persistent_term state.
          # Will be replaced once listeners pass ctx explicitly.
          ctx = default_instance_ctx()

          state = %__MODULE__{
            socket: socket,
            transport: transport,
            client_id: generate_client_id(),
            client_name: nil,
            created_at: System.monotonic_time(:millisecond),
            peer: peer,
            instance_ctx: ctx,
            tracking: ClientTracking.new_config(),
            acl_cache: default_cache,
            active_mode: active_mode
          }

          AuditLog.log(:connection_open, %{
            client_id: state.client_id,
            client_ip: format_peer(peer)
          })

          loop(state)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Receive loop (active: N, event-driven)
  # ---------------------------------------------------------------------------

  # Normal receive loop. In active:N mode, the kernel delivers N messages
  # then sends {:tcp_passive, socket}. We re-arm on {:tcp_passive}.
  # In active: :once mode, we re-arm after each data message.
  # In active: true mode, no re-arming needed.
  #
  # Pubsub mode uses a separate loop (pubsub_loop) to avoid checking
  # a mode flag on every iteration of the hot path.
  defp loop(%__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state) do
    # Re-arm socket for :once mode. For true/N modes, kernel delivers
    # continuously — no re-arm needed (N mode re-arms on {:tcp_passive}).
    if active_mode == :once do
      transport.setopts(socket, active: :once)
    end

    receive do
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      {:ssl, ^socket, data} ->
        handle_data(state, data)

      # Active N mode: socket went passive after N messages, re-arm
      {:tcp_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        loop(state)

      {:ssl_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        loop(state)

      {:tcp_closed, ^socket} ->
        cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:ssl_closed, ^socket} ->
        cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:tracking_invalidation, iodata, _keys} ->
        transport.send(socket, iodata)
        loop(state)

      {:acl_invalidate, username} ->
        loop(ConnAuth.maybe_refresh_acl_cache(state, username))
    end
  end

  defp handle_data(%__MODULE__{socket: socket, transport: transport} = state, data) do
    # Avoid binary concatenation when buffer is empty (common case for
    # non-pipelined workloads). Saves one binary allocation + copy per TCP frame.
    buffer = if state.buffer == "", do: data, else: state.buffer <> data

    # Connection buffer limit: reject connections that accumulate too much
    # unparsed data (e.g. sending huge incomplete frames to exhaust memory).
    if byte_size(buffer) > @max_buffer_size do
      send_response(
        socket,
        transport,
        Encoder.encode(
          {:error,
           "ERR connection buffer overflow (max #{@max_buffer_size} bytes)"}
        )
      )

      cleanup_connection(state)
      transport.close(socket)
    else
      case Parser.parse(buffer) do
        {:ok, [], rest} ->
          loop(%{state | buffer: rest})

        {:ok, commands, rest} ->
          handle_parsed(%{state | buffer: rest}, commands)

        {:error, {:value_too_large, len, max}} ->
          send_response(
            socket,
            transport,
            Encoder.encode(
              {:error,
               "ERR value too large (#{len} bytes, max #{max} bytes)"}
            )
          )

          cleanup_connection(state)
          transport.close(socket)

        {:error, _reason} ->
          send_response(socket, transport, Encoder.encode({:error, "ERR protocol error"}))
          cleanup_connection(state)
          transport.close(socket)
      end
    end
  end

  defp handle_parsed(%__MODULE__{socket: socket, transport: transport} = state, commands) do
    # Pipeline batch limit: reject batches with too many commands to prevent
    # unbounded memory from accumulated Task results and response buffers.
    max_size = ConnPipeline.max_pipeline_size()

    if length(commands) > max_size do
      send_response(
        socket,
        transport,
        Encoder.encode(
          {:error,
           "ERR pipeline batch too large (#{length(commands)} commands, max #{max_size})"}
        )
      )

      loop(state)
    else
      case ConnPipeline.pipeline_dispatch(commands, state, &handle_command/2, &send_response/3) do
        {:quit, quit_state} ->
          cleanup_connection(quit_state)
          transport.close(socket)

        {:continue, new_state} ->
          # If SUBSCRIBE was dispatched, switch to the pubsub loop.
          # in_pubsub_mode? is a nil check (O(1)) for non-pubsub connections.
          if in_pubsub_mode?(new_state) do
            pubsub_loop(new_state)
          else
            loop(new_state)
          end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Individual command handlers
  # ---------------------------------------------------------------------------

  @pre_auth_cmds ~w(AUTH HELLO QUIT RESET)

  # Commands that bypass ACL command-level checks. These are protocol-level
  # commands needed for connection setup, teardown, and user switching.
  @acl_bypass_cmds ~w(AUTH HELLO QUIT RESET)

  # Normalise any command form to {name, args} where name is uppercase binary.
  defp handle_command({:inline, tokens}, state) do
    handle_command(tokens, state)
  end

  defp handle_command([name | args], state) when is_binary(name) do
    cmd = String.upcase(name)

    cond do
      requires_auth?(state) and cmd not in @pre_auth_cmds ->
        {:continue, Encoder.encode({:error, "NOAUTH Authentication required."}), state}

      cmd not in @acl_bypass_cmds ->
        with :ok <- ConnAuth.check_command_cached(state.acl_cache, cmd),
             :ok <- ConnAuth.check_keys_cached(state.acl_cache, cmd, args) do
          Stats.incr_commands()
          dispatch(cmd, args, state)
        else
          {:error, _reason} = err ->
            # Fix 5: Log command denials to the audit log.
            FerricstoreServer.Acl.log_command_denied(
              state.username,
              cmd,
              format_peer(state.peer),
              state.client_id
            )

            {:continue, Encoder.encode(err), state}
        end

      true ->
        Stats.incr_commands()
        dispatch(cmd, args, state)
    end
  end

  defp handle_command(_unknown, state) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"}), state}
  end

  defp requires_auth?(state) do
    not state.authenticated and Ferricstore.Config.get_value("requirepass") != ""
  end

  # ---------------------------------------------------------------------------
  # Dispatch table (all clauses contiguous)
  # ---------------------------------------------------------------------------

  defp dispatch("HELLO", args, state), do: handle_hello(args, state)
  defp dispatch("CLIENT", ["HELLO" | args], state), do: handle_hello(args, state)
  defp dispatch("CLIENT", args, state), do: dispatch_client(args, state)
  defp dispatch("QUIT", _args, state), do: {:quit, Encoder.encode(:ok), state}
  defp dispatch("AUTH", args, state), do: ConnAuth.dispatch_auth(args, state)
  defp dispatch("ACL", [subcmd | rest], state), do: ConnAuth.dispatch_acl(String.upcase(subcmd), rest, state)
  defp dispatch("ACL", [], state), do: {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl' command"}), state}
  defp dispatch("RESET", _args, state), do: dispatch_reset(state)
  defp dispatch("READMODE", _args, state) do
    {:continue, Encoder.encode({:error, "ERR unknown command 'READMODE'"}), state}
  end
  defp dispatch("SANDBOX", args, state), do: dispatch_sandbox(args, state)
  defp dispatch("MULTI", args, state), do: ConnTransaction.dispatch_multi(args, state)
  defp dispatch("EXEC", args, state), do: ConnTransaction.dispatch_exec(args, state)
  defp dispatch("DISCARD", args, state), do: ConnTransaction.dispatch_discard(args, state)
  defp dispatch("WATCH", args, state), do: ConnTransaction.dispatch_watch(args, state)
  defp dispatch("UNWATCH", args, state), do: ConnTransaction.dispatch_unwatch(args, state)
  # MULTI queuing guard — must be BEFORE pubsub and blocking commands.
  # EXEC, DISCARD, MULTI, WATCH, UNWATCH are passthrough (handled above).
  # Everything else gets queued.
  defp dispatch(cmd, args, %{multi_state: :queuing} = state)
       when cmd not in @multi_passthrough_cmds do
    ConnTransaction.dispatch_queue(cmd, args, state)
  end

  defp dispatch("SUBSCRIBE", args, state), do: ConnPubSub.dispatch_subscribe(args, state)
  defp dispatch("UNSUBSCRIBE", args, state), do: ConnPubSub.dispatch_unsubscribe(args, state)
  defp dispatch("PSUBSCRIBE", args, state), do: ConnPubSub.dispatch_psubscribe(args, state)
  defp dispatch("PUNSUBSCRIBE", args, state), do: ConnPubSub.dispatch_punsubscribe(args, state)

  defp dispatch("BLPOP", args, state), do: ConnBlocking.dispatch_blpop(args, state)
  defp dispatch("BRPOP", args, state), do: ConnBlocking.dispatch_brpop(args, state)
  defp dispatch("BLMOVE", args, state), do: ConnBlocking.dispatch_blmove(args, state)
  defp dispatch("BLMPOP", args, state), do: ConnBlocking.dispatch_blmpop(args, state)

  defp dispatch("GET", [key], %{transport: :ranch_tcp} = state)
       when byte_size(key) > 0 and byte_size(key) <= 65_535 do
    ConnSendfile.dispatch_get([key], state, &dispatch_normal/3)
  end

  defp dispatch("XREAD", args, state), do: ConnBlocking.dispatch_xread(args, state)

  defp dispatch(cmd, args, state) do
    if in_pubsub_mode?(state) and cmd not in ~w(PING) do
      {:continue,
       Encoder.encode({:error, "ERR Can't execute '#{String.downcase(cmd)}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"}),
       state}
    else
      dispatch_normal(cmd, args, state)
    end
  end

  # ---------------------------------------------------------------------------
  # Dispatch helpers (called from dispatch table above)
  # ---------------------------------------------------------------------------

  defp dispatch_client(args, state) do
    store = if state.sandbox_namespace, do: ConnStore.build_store(state.instance_ctx, state.sandbox_namespace), else: state.instance_ctx

    conn_state = %{
      client_id: state.client_id,
      client_name: state.client_name,
      created_at: state.created_at,
      peer: state.peer,
      conn_pid: self(),
      tracking: state.tracking
    }

    {result, updated_conn_state} =
      try do
        case args do
          [subcmd | rest] ->
            FerricstoreServer.Commands.Client.handle(String.upcase(subcmd), rest, conn_state, store)

          [] ->
            {{:error, "ERR wrong number of arguments for 'client' command"}, conn_state}
        end
      catch
        :exit, {:noproc, _} ->
          {{:error, "ERR server not ready, shard process unavailable"}, conn_state}

        :exit, {reason, _} ->
          {{:error, "ERR internal error: #{inspect(reason)}"}, conn_state}
      end

    updated_state = %{
      state
      | client_name: updated_conn_state[:client_name] || state.client_name,
        tracking: updated_conn_state[:tracking] || state.tracking
    }

    {:continue, Encoder.encode(result), updated_state}
  end

  defp dispatch_reset(state) do
    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    new_state = %{state |
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      watched_keys: %{},
      sandbox_namespace: nil,
      tracking: ClientTracking.new_config(),
      authenticated: false,
      username: "default",
      pubsub_channels: nil,
      pubsub_patterns: nil,
      acl_cache: ConnAuth.build_acl_cache("default")
    }
    {:continue, Encoder.encode({:simple, "RESET"}), new_state}
  end


  defp dispatch_sandbox([subcmd | rest], state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")
    sandbox_enabled? = sandbox_mode in ["local", "enabled"]

    case {String.upcase(subcmd), sandbox_enabled?} do
      {"START", true} ->
        sandbox_start(state)

      {"JOIN", true} ->
        sandbox_join(rest, state)

      {"END", true} ->
        sandbox_end(state)

      {"TOKEN", true} ->
        {:continue, Encoder.encode(state.sandbox_namespace), state}

      {cmd, false} when cmd in ~w(START JOIN END TOKEN) ->
        {:continue, Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}), state}

      _ ->
        {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    end
  end

  defp dispatch_sandbox(_args, state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")

    if sandbox_mode in ["local", "enabled"] do
      {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    else
      {:continue, Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}), state}
    end
  end

  defp sandbox_start(state) do
    ns = "test_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    {:continue, Encoder.encode(ns), %{state | sandbox_namespace: ns}}
  end

  defp sandbox_join([token | _], state) do
    {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: token}}
  end

  defp sandbox_join([], state) do
    {:continue, Encoder.encode({:error, "ERR SANDBOX JOIN requires a namespace token"}), state}
  end

  defp sandbox_end(%{sandbox_namespace: nil} = state) do
    {:continue, Encoder.encode({:error, "ERR no active sandbox session"}), state}
  end

  defp sandbox_end(state) do
    ns = state.sandbox_namespace

    try do
      ctx = state.instance_ctx
      keys = Router.keys(ctx)
      Enum.each(keys, fn k -> if String.starts_with?(k, ns), do: Router.delete(ctx, k) end)
    catch
      :exit, _ -> :ok
    end

    {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: nil}}
  end

  defp dispatch_normal(cmd, args, state) do
    # Hot path: pass ctx directly (no closure allocation).
    # Ops and Router handle Instance structs natively.
    # Namespace path: closure map for key prefixing.
    store =
      if state.sandbox_namespace do
        ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)
      else
        state.instance_ctx
      end

    result =
      try do
        Dispatcher.dispatch(cmd, args, store)
      catch
        :exit, {:noproc, _} ->
          {:error, "ERR server not ready, shard process unavailable"}

        :exit, {reason, _} ->
          {:error, "ERR internal error: #{inspect(reason)}"}
      end

    ConnTracking.maybe_notify_keyspace(cmd, args, result)
    new_state = ConnTracking.maybe_track_read(cmd, args, result, state)
    ConnTracking.maybe_notify_tracking(cmd, args, result, state)

    {:continue, Encoder.encode(result), new_state}
  end


  # ---------------------------------------------------------------------------
  # HELLO handler
  # ---------------------------------------------------------------------------

  defp handle_hello(["3" | _rest], state) do
    {:continue, Encoder.encode(greeting_map(state)), state}
  end

  defp handle_hello([version | _rest], state) when is_binary(version) do
    {:continue,
     Encoder.encode({:error, "NOPROTO this server does not support the requested protocol version"}),
     state}
  end

  defp handle_hello([], state) do
    # HELLO with no version returns current server info (RESP3)
    {:continue, Encoder.encode(greeting_map(state)), state}
  end

  # ---------------------------------------------------------------------------
  # Greeting map
  # ---------------------------------------------------------------------------

  defp greeting_map(state) do
    %{
      "server" => "ferricstore",
      "version" => "0.1.0",
      "proto" => 3,
      "id" => state.client_id,
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

  defp send_response(socket, transport, iodata) do
    case transport.send(socket, iodata) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Pub/Sub mode loop
  # ---------------------------------------------------------------------------

  defp pubsub_loop(%__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state) do
    # No setopts needed — active mode (true/N/:once) is maintained from
    # the main loop. TCP data keeps arriving and is handled below.
    if active_mode == :once do
      transport.setopts(socket, active: :once)
    end

    receive do
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      {:ssl, ^socket, data} ->
        handle_data(state, data)

      {:tcp_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        pubsub_loop(state)

      {:ssl_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        pubsub_loop(state)

      {:tcp_closed, ^socket} ->
        cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:ssl_closed, ^socket} ->
        cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:pubsub_message, channel, message} ->
        push = {:push, ["message", channel, message]}
        transport.send(socket, Encoder.encode(push))
        pubsub_loop(state)

      {:pubsub_pmessage, pattern, channel, message} ->
        push = {:push, ["pmessage", pattern, channel, message]}
        transport.send(socket, Encoder.encode(push))
        pubsub_loop(state)

      {:tracking_invalidation, iodata, _keys} ->
        transport.send(socket, iodata)
        pubsub_loop(state)

      {:acl_invalidate, username} ->
        pubsub_loop(ConnAuth.maybe_refresh_acl_cache(state, username))
    end
  end

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false
  defp in_pubsub_mode?(state), do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  defp cleanup_connection(state) do
    duration_ms = System.monotonic_time(:millisecond) - state.created_at

    AuditLog.log(:connection_close, %{
      client_id: state.client_id,
      client_ip: format_peer(state.peer),
      duration_ms: duration_ms
    })

    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
    Stats.decr_connections()
  end

  defp cleanup_pubsub(state) do
    if state.pubsub_channels, do: Enum.each(state.pubsub_channels, &PS.unsubscribe(&1, self()))
    if state.pubsub_patterns, do: Enum.each(state.pubsub_patterns, &PS.punsubscribe(&1, self()))
  end

  # ---------------------------------------------------------------------------
  # Instance context helpers
  # ---------------------------------------------------------------------------

  # Transitional: build instance ctx from persistent_term global state.
  # Will be removed once listeners pass ctx explicitly at connection init.
  @spec default_instance_ctx() :: FerricStore.Instance.t()
  defp default_instance_ctx do
    FerricStore.Instance.get(:default)
  end

  # Formats a peer tuple `{ip, port}` into a human-readable string.
  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"

  # Returns true when the require-tls configuration flag is set.
  defp require_tls? do
    Application.get_env(:ferricstore, :require_tls, false) == true
  end

  # ---------------------------------------------------------------------------
  # ACL cache — delegated to Connection.Auth
  # ---------------------------------------------------------------------------

  # The process group name for ACL invalidation broadcasts.
  @acl_pg_group :ferricstore_acl_connections

  @doc false
  @spec acl_pg_group() :: atom()
  def acl_pg_group, do: @acl_pg_group

  # Joins the OTP :pg process group for ACL invalidation broadcasts.
  # Called once during connection init. The process is automatically removed
  # from the group when it terminates (no explicit leave needed).
  # The :pg scope is started by FerricstoreServer.Application.
  @spec join_acl_invalidation_group() :: :ok
  defp join_acl_invalidation_group do
    :pg.join(@acl_pg_group, @acl_pg_group, self())
    :ok
  end
end
