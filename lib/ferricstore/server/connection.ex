# Suppress function clause grouping warnings (clauses added by different agents)
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

  ## BEAM scheduler notes

  All I/O uses `active: false` (passive mode) so the receive loop blocks on
  `:gen_tcp.recv/3`. This keeps the process off the scheduler while waiting
  for data and avoids mailbox flooding on bursty clients.
  """

  @behaviour :ranch_protocol

  alias Ferricstore.AuditLog
  alias Ferricstore.ClientTracking
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.KeyspaceNotifications
  alias Ferricstore.Resp.{Encoder, Parser}
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  alias Ferricstore.PubSub, as: PS

  # Connection state
  defstruct [
    :socket,
    :transport,
    :client_id,
    :client_name,
    :created_at,
    :peer,
    buffer: "",
    multi_state: :none,
    multi_queue: [],
    watched_keys: %{},
    authenticated: false,
    sandbox_namespace: nil,
    pubsub_channels: MapSet.new(),
    pubsub_patterns: MapSet.new(),
    tracking: nil
  ]

  @type multi_state :: :none | :queuing

  @type t :: %__MODULE__{
          socket: :inet.socket(),
          transport: module(),
          buffer: binary(),
          client_id: pos_integer(),
          client_name: binary() | nil,
          created_at: integer(),
          peer: {:inet.ip_address(), :inet.port_number()} | nil,
          multi_state: multi_state(),
          multi_queue: [{binary(), [binary()]}],
          watched_keys: %{binary() => non_neg_integer()},
          tracking: ClientTracking.tracking_config() | nil
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
    :ok = transport.setopts(socket, active: false)

    Stats.incr_connections()

    peer =
      case transport.peername(socket) do
        {:ok, addr} -> addr
        _ -> nil
      end

    state = %__MODULE__{
      socket: socket,
      transport: transport,
      client_id: generate_client_id(),
      client_name: nil,
      created_at: System.monotonic_time(:millisecond),
      peer: peer,
      tracking: ClientTracking.new_config()
    }

    AuditLog.log(:connection_open, %{
      client_id: state.client_id,
      client_ip: format_peer(peer)
    })

    loop(state)
  end

  # ---------------------------------------------------------------------------
  # Receive loop
  # ---------------------------------------------------------------------------

  defp loop(%__MODULE__{socket: socket, transport: transport} = state) do
    if in_pubsub_mode?(state) do
      pubsub_loop(state)
    else
      case transport.recv(socket, 0, :infinity) do
        {:ok, <<>>} ->
          cleanup_connection(state)
          transport.close(socket)

        {:ok, data} ->
          handle_data(state, data)

        {:error, _reason} ->
          cleanup_connection(state)
          transport.close(socket)
      end
    end
  end

  defp handle_data(%__MODULE__{socket: socket, transport: transport} = state, data) do
    buffer = state.buffer <> data

    case Parser.parse(buffer) do
      {:ok, [], rest} ->
        loop(%{state | buffer: rest})

      {:ok, commands, rest} ->
        handle_parsed(%{state | buffer: rest}, commands)

      {:error, _reason} ->
        send_response(socket, transport, Encoder.encode({:error, "ERR protocol error"}))
        cleanup_connection(state)
        transport.close(socket)
    end
  end

  defp handle_parsed(%__MODULE__{socket: socket, transport: transport} = state, commands) do
    case dispatch_commands(commands, state) do
      {:quit, responses, quit_state} ->
        send_responses(socket, transport, responses)
        cleanup_connection(quit_state)
        transport.close(socket)

      {:continue, responses, new_state} ->
        send_responses(socket, transport, responses)
        loop(new_state)
    end
  end

  # ---------------------------------------------------------------------------
  # Command dispatch
  # ---------------------------------------------------------------------------

  # Processes a list of parsed commands, accumulating encoded responses and
  # threading connection state (for transaction support). Returns
  # `{:quit | :continue, [iodata()], state}`.
  defp dispatch_commands(commands, state) do
    {action, responses, final_state} =
      Enum.reduce_while(commands, {:continue, [], state}, fn cmd, {_action, acc, st} ->
        case handle_command(cmd, st) do
          {:quit, response, new_st} ->
            {:halt, {:quit, [response | acc], new_st}}

          {:continue, response, new_st} ->
            {:cont, {:continue, [response | acc], new_st}}
        end
      end)

    {action, Enum.reverse(responses), final_state}
  end

  # ---------------------------------------------------------------------------
  # Individual command handlers
  # ---------------------------------------------------------------------------

  # Normalise any command form to {name, args} where name is uppercase binary.
  defp handle_command({:inline, tokens}, state) do
    handle_command(tokens, state)
  end

  @pre_auth_cmds ~w(AUTH HELLO QUIT RESET)

  defp handle_command([name | args], state) when is_binary(name) do
    cmd = String.upcase(name)

    if requires_auth?(state) and cmd not in @pre_auth_cmds do
      {:continue, Encoder.encode({:error, "NOAUTH Authentication required."}), state}
    else
      Stats.incr_commands()
      dispatch(cmd, args, state)
    end
  end

  defp requires_auth?(state) do
    not state.authenticated and Ferricstore.Config.get_value("requirepass") != ""
  end

  defp handle_command(_unknown, state) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"}), state}
  end

  # ---------------------------------------------------------------------------
  # Dispatch table
  # ---------------------------------------------------------------------------

  # Protocol-level commands stay in the connection layer.
  defp dispatch("HELLO", args, state), do: handle_hello(args, state)
  # CLIENT HELLO [version] is the two-token form sent by some Redis clients.
  defp dispatch("CLIENT", ["HELLO" | args], state), do: handle_hello(args, state)

  # CLIENT subcommands that need connection state.
  defp dispatch("CLIENT", args, state) do
    store = build_store(state.sandbox_namespace)

    conn_state = %{
      client_id: state.client_id,
      client_name: state.client_name,
      created_at: state.created_at,
      peer: state.peer,
      conn_pid: self(),
      tracking: state.tracking
    }

    {result, updated_conn_state} = Dispatcher.dispatch_client(args, conn_state, store)

    updated_state = %{
      state
      | client_name: updated_conn_state[:client_name] || state.client_name,
        tracking: updated_conn_state[:tracking] || state.tracking
    }

    {:continue, Encoder.encode(result), updated_state}
  end

  defp dispatch("QUIT", _args, state), do: {:quit, Encoder.encode(:ok), state}

  # AUTH command
  defp dispatch("AUTH", args, state) do
    requirepass = Ferricstore.Config.get_value("requirepass")
    client_ip = format_peer(state.peer)

    case {requirepass, args} do
      {"", _} ->
        {:continue, Encoder.encode({:error, "ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?"}), state}

      {pass, [pass]} ->
        AuditLog.log(:auth_success, %{username: "default", client_ip: client_ip})
        {:continue, Encoder.encode(:ok), %{state | authenticated: true}}

      {pass, [_user, pass]} ->
        username = List.first(args) || "default"
        AuditLog.log(:auth_success, %{username: username, client_ip: client_ip})
        {:continue, Encoder.encode(:ok), %{state | authenticated: true}}

      {_, [_wrong]} ->
        AuditLog.log(:auth_failure, %{username: "default", client_ip: client_ip})
        {:continue, Encoder.encode({:error, "WRONGPASS invalid username-password pair or user is disabled."}), state}

      {_, [_user, _wrong]} ->
        username = List.first(args) || "default"
        AuditLog.log(:auth_failure, %{username: username, client_ip: client_ip})
        {:continue, Encoder.encode({:error, "WRONGPASS invalid username-password pair or user is disabled."}), state}

      {_, []} ->
        {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}), state}

      _ ->
        {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}), state}
    end
  end

  # ACL subcommands — upcase subcommand for case-insensitive matching
  defp dispatch("ACL", [subcmd | rest], state) do
    dispatch_acl(String.upcase(subcmd), rest, state)
  end

  defp dispatch("ACL", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl' command"}), state}
  end

  defp dispatch_acl("WHOAMI", _, state) do
    {:continue, Encoder.encode("default"), state}
  end

  defp dispatch_acl("LIST", _, state) do
    {:continue, Encoder.encode(["user default on ~* &* +@all"]), state}
  end

  defp dispatch_acl("CAT", _, state) do
    cats = ~w(keyspace read write set sortedset list hash string bitmap hyperloglog geo stream pubsub admin fast slow blocking dangerous connection transaction server generic)
    {:continue, Encoder.encode(cats), state}
  end

  defp dispatch_acl("LOG", ["RESET" | _], state) do
    AuditLog.reset()
    {:continue, Encoder.encode(:ok), state}
  end

  defp dispatch_acl("LOG", ["COUNT", count_str | _], state) do
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 ->
        entries = AuditLog.get(count) |> AuditLog.format_entries()
        {:continue, Encoder.encode(entries), state}

      _ ->
        {:continue, Encoder.encode({:error, "ERR value is not an integer or out of range"}), state}
    end
  end

  defp dispatch_acl("LOG", [], state) do
    entries = AuditLog.get() |> AuditLog.format_entries()
    {:continue, Encoder.encode(entries), state}
  end

  defp dispatch_acl("LOG", _, state) do
    entries = AuditLog.get() |> AuditLog.format_entries()
    {:continue, Encoder.encode(entries), state}
  end

  defp dispatch_acl("GETUSER", ["default" | _], state) do
    info = ["flags", ["on"], "passwords", [], "commands", "+@all", "keys", "~*", "channels", "&*"]
    {:continue, Encoder.encode(info), state}
  end

  defp dispatch_acl("GETUSER", [_user | _], state) do
    {:continue, Encoder.encode(nil), state}
  end

  defp dispatch_acl(_, _, state) do
    {:continue, Encoder.encode({:error, "ERR unknown subcommand or wrong number of arguments for 'acl' command"}), state}
  end

  defp dispatch("RESET", _args, state) do
    # RESET clears transaction state, sandbox namespace, and tracking state.
    ClientTracking.cleanup(self())
    new_state = %{state | multi_state: :none, multi_queue: [], watched_keys: %{}, sandbox_namespace: nil, tracking: ClientTracking.new_config()}
    {:continue, Encoder.encode({:simple, "RESET"}), new_state}
  end

  # -- SANDBOX commands -------------------------------------------------------

  defp dispatch("SANDBOX", [subcmd | rest], state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")
    sandbox_enabled? = sandbox_mode in ["local", "enabled"]

    case String.upcase(subcmd) do
      "START" when sandbox_enabled? ->
        ns = "test_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
        {:continue, Encoder.encode(ns), %{state | sandbox_namespace: ns}}

      "JOIN" when sandbox_enabled? ->
        case rest do
          [token | _] ->
            {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: token}}

          [] ->
            {:continue, Encoder.encode({:error, "ERR SANDBOX JOIN requires a namespace token"}), state}
        end

      "END" when sandbox_enabled? ->
        if state.sandbox_namespace do
          # Flush keys with sandbox prefix
          ns = state.sandbox_namespace
          keys = Router.keys()
          Enum.each(keys, fn k -> if String.starts_with?(k, ns), do: Router.delete(k) end)
          {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: nil}}
        else
          {:continue, Encoder.encode({:error, "ERR no active sandbox session"}), state}
        end

      "TOKEN" when sandbox_enabled? ->
        {:continue, Encoder.encode(state.sandbox_namespace), state}

      cmd when cmd in ~w(START JOIN END TOKEN) ->
        {:continue, Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}), state}

      _ ->
        {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    end
  end

  defp dispatch("SANDBOX", _args, state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")

    if sandbox_mode in ["local", "enabled"] do
      {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    else
      {:continue, Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}), state}
    end
  end

  # -- Transaction commands --------------------------------------------------

  defp dispatch("MULTI", _args, %{multi_state: :queuing} = state) do
    {:continue, Encoder.encode({:error, "ERR MULTI calls can not be nested"}), state}
  end

  defp dispatch("MULTI", _args, state) do
    new_state = %{state | multi_state: :queuing, multi_queue: []}
    {:continue, Encoder.encode(:ok), new_state}
  end

  defp dispatch("EXEC", _args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR EXEC without MULTI"}), state}
  end

  defp dispatch("EXEC", _args, state) do
    result = execute_transaction(state)
    new_state = %{state | multi_state: :none, multi_queue: [], watched_keys: %{}}
    {:continue, Encoder.encode(result), new_state}
  end

  defp dispatch("DISCARD", _args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR DISCARD without MULTI"}), state}
  end

  defp dispatch("DISCARD", _args, state) do
    new_state = %{state | multi_state: :none, multi_queue: [], watched_keys: %{}}
    {:continue, Encoder.encode(:ok), new_state}
  end

  defp dispatch("WATCH", _args, %{multi_state: :queuing} = state) do
    {:continue,
     Encoder.encode({:error, "ERR WATCH inside MULTI is not allowed"}), state}
  end

  defp dispatch("WATCH", [], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'watch' command"}), state}
  end

  defp dispatch("WATCH", keys, state) do
    new_watched =
      Enum.reduce(keys, state.watched_keys, fn key, acc ->
        version = Router.get_version(key)
        Map.put(acc, key, version)
      end)

    {:continue, Encoder.encode(:ok), %{state | watched_keys: new_watched}}
  end

  defp dispatch("UNWATCH", _args, state) do
    {:continue, Encoder.encode(:ok), %{state | watched_keys: %{}}}
  end

  # -- Pub/Sub commands handled in connection layer -------------------------

  defp dispatch("SUBSCRIBE", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'subscribe' command"}), state}
  end

  defp dispatch("SUBSCRIBE", channels, state) do
    {responses, new_state} =
      Enum.reduce(channels, {[], state}, fn ch, {acc, st} ->
        PS.subscribe(ch, self())
        new_channels = MapSet.put(st.pubsub_channels, ch)
        new_st = %{st | pubsub_channels: new_channels}
        count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
        push = {:push, ["subscribe", ch, count]}
        {[Encoder.encode(push) | acc], new_st}
      end)

    # Switch to active:once for receiving pub/sub messages
    new_state.transport.setopts(new_state.socket, active: :once)
    {:continue, Enum.reverse(responses), new_state}
  end

  defp dispatch("UNSUBSCRIBE", [], state) do
    dispatch("UNSUBSCRIBE", MapSet.to_list(state.pubsub_channels), state)
  end

  defp dispatch("UNSUBSCRIBE", channels, state) do
    {responses, new_state} =
      Enum.reduce(channels, {[], state}, fn ch, {acc, st} ->
        PS.unsubscribe(ch, self())
        new_channels = MapSet.delete(st.pubsub_channels, ch)
        new_st = %{st | pubsub_channels: new_channels}
        count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
        push = {:push, ["unsubscribe", ch, count]}
        {[Encoder.encode(push) | acc], new_st}
      end)

    # Exit pub/sub mode if no subscriptions remain
    if not in_pubsub_mode?(new_state) do
      new_state.transport.setopts(new_state.socket, active: false)
    end

    {:continue, Enum.reverse(responses), new_state}
  end

  defp dispatch("PSUBSCRIBE", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'psubscribe' command"}), state}
  end

  defp dispatch("PSUBSCRIBE", patterns, state) do
    {responses, new_state} =
      Enum.reduce(patterns, {[], state}, fn pat, {acc, st} ->
        PS.psubscribe(pat, self())
        new_patterns = MapSet.put(st.pubsub_patterns, pat)
        new_st = %{st | pubsub_patterns: new_patterns}
        count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
        push = {:push, ["psubscribe", pat, count]}
        {[Encoder.encode(push) | acc], new_st}
      end)

    new_state.transport.setopts(new_state.socket, active: :once)
    {:continue, Enum.reverse(responses), new_state}
  end

  defp dispatch("PUNSUBSCRIBE", [], state) do
    dispatch("PUNSUBSCRIBE", MapSet.to_list(state.pubsub_patterns), state)
  end

  defp dispatch("PUNSUBSCRIBE", patterns, state) do
    {responses, new_state} =
      Enum.reduce(patterns, {[], state}, fn pat, {acc, st} ->
        PS.punsubscribe(pat, self())
        new_patterns = MapSet.delete(st.pubsub_patterns, pat)
        new_st = %{st | pubsub_patterns: new_patterns}
        count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
        push = {:push, ["punsubscribe", pat, count]}
        {[Encoder.encode(push) | acc], new_st}
      end)

    if not in_pubsub_mode?(new_state) do
      new_state.transport.setopts(new_state.socket, active: false)
    end

    {:continue, Enum.reverse(responses), new_state}
  end

  # -- Queuing mode: intercept all non-passthrough commands ------------------

  defp dispatch(cmd, args, %{multi_state: :queuing} = state)
       when cmd not in @multi_passthrough_cmds do
    # Validate command syntax at queue time. If the dispatcher returns an
    # error, send it immediately but stay in MULTI mode.
    store = build_store(state.sandbox_namespace)

    case validate_command(cmd, args, store) do
      :ok ->
        new_queue = state.multi_queue ++ [{cmd, args}]
        {:continue, Encoder.encode({:simple, "QUEUED"}), %{state | multi_queue: new_queue}}

      {:error, _msg} = err ->
        {:continue, Encoder.encode(err), state}
    end
  end

  # -- Blocking list commands (handled in connection layer) ------------------
  # These commands try an immediate pop; if the list is empty and timeout > 0,
  # the connection process registers as a waiter and enters a `receive` block.
  # Passive socket mode ensures no TCP data arrives during the block.

  defp dispatch("BLPOP", args, state) do
    dispatch_blocking(:blpop, args, state)
  end

  defp dispatch("BRPOP", args, state) do
    dispatch_blocking(:brpop, args, state)
  end

  defp dispatch("BLMOVE", args, state) do
    dispatch_blmove(args, state)
  end

  defp dispatch("BLMPOP", args, state) do
    dispatch_blmpop(args, state)
  end

  defp dispatch_blocking(pop_dir, args, state) do
    alias Ferricstore.Commands.{Blocking, List}
    alias Ferricstore.Waiters

    case Blocking.parse_blpop_args(args) do
      {:ok, keys, timeout_ms} ->
        store = build_store(state.sandbox_namespace)
        pop_cmd = if pop_dir == :blpop, do: "LPOP", else: "RPOP"

        # Try immediate pop on each key (first non-empty wins)
        immediate =
          Enum.find_value(keys, fn key ->
            case List.handle(pop_cmd, [key], store) do
              nil -> nil
              {:error, _} -> nil
              value -> [key, value]
            end
          end)

        if immediate do
          {:continue, Encoder.encode(immediate), state}
        else
          if timeout_ms == 0 do
            # timeout=0 means block forever (Redis semantics), but we cap at 5 min
            do_block_wait(keys, 300_000, pop_cmd, store, state)
          else
            do_block_wait(keys, timeout_ms, pop_cmd, store, state)
          end
        end

      {:error, _} = err ->
        {:continue, Encoder.encode(err), state}
    end
  end

  defp do_block_wait(keys, timeout_ms, pop_cmd, store, state) do
    alias Ferricstore.Commands.List
    alias Ferricstore.Waiters

    # Register as waiter for all watched keys
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    Enum.each(keys, fn key -> Waiters.register(key, self(), deadline) end)

    # Enter receive block — passive socket means no TCP data arrives
    result =
      receive do
        {:waiter_notify, notified_key} ->
          # A push happened on one of our keys — try to pop
          case List.handle(pop_cmd, [notified_key], store) do
            nil -> nil
            {:error, _} -> nil
            value -> [notified_key, value]
          end
      after
        timeout_ms ->
          nil
      end

    # Cleanup: unregister from all keys
    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    {:continue, Encoder.encode(result), state}
  end

  # -- BLMOVE blocking dispatch ------------------------------------------------

  defp dispatch_blmove(args, state) do
    alias Ferricstore.Commands.{Blocking, List}

    case Blocking.parse_blmove_args(args) do
      {:ok, source, destination, from_dir, to_dir, timeout_ms} ->
        store = build_store(state.sandbox_namespace)

        # Try immediate LMOVE
        case List.handle("LMOVE", [source, destination, to_string(from_dir), to_string(to_dir)], store) do
          nil ->
            # Source is empty — block if timeout allows
            if timeout_ms == 0 do
              do_blmove_wait([source], 300_000, source, destination, from_dir, to_dir, store, state)
            else
              do_blmove_wait([source], timeout_ms, source, destination, from_dir, to_dir, store, state)
            end

          {:error, _} = err ->
            {:continue, Encoder.encode(err), state}

          value ->
            {:continue, Encoder.encode(value), state}
        end

      {:error, _} = err ->
        {:continue, Encoder.encode(err), state}
    end
  end

  defp do_blmove_wait(keys, timeout_ms, source, destination, from_dir, to_dir, store, state) do
    alias Ferricstore.Commands.List
    alias Ferricstore.Waiters

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    Enum.each(keys, fn key -> Waiters.register(key, self(), deadline) end)

    result =
      receive do
        {:waiter_notify, _notified_key} ->
          # Source got a push — try LMOVE
          case List.handle("LMOVE", [source, destination, to_string(from_dir), to_string(to_dir)], store) do
            nil -> nil
            {:error, _} -> nil
            value -> value
          end
      after
        timeout_ms ->
          nil
      end

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    {:continue, Encoder.encode(result), state}
  end

  # -- BLMPOP blocking dispatch ------------------------------------------------

  defp dispatch_blmpop(args, state) do
    alias Ferricstore.Commands.{Blocking, List}

    case Blocking.parse_blmpop_args(args) do
      {:ok, keys, direction, count, timeout_ms} ->
        store = build_store(state.sandbox_namespace)
        pop_cmd = if direction == :left, do: "LPOP", else: "RPOP"

        # Build the count arg list: omit count arg when count == 1
        # to get a single-element return (not wrapped in a list)
        pop_args_fn = fn key ->
          if count == 1, do: [key], else: [key, to_string(count)]
        end

        # Try immediate pop on each key (first non-empty wins)
        immediate =
          Enum.find_value(keys, fn key ->
            case List.handle(pop_cmd, pop_args_fn.(key), store) do
              nil -> nil
              {:error, _} -> nil
              value -> {key, value}
            end
          end)

        case immediate do
          {key, value} ->
            # Wrap single value into a list for consistent BLMPOP format
            elements = if is_list(value), do: value, else: [value]
            {:continue, Encoder.encode([key, elements]), state}

          nil ->
            if timeout_ms == 0 do
              do_blmpop_wait(keys, 300_000, pop_cmd, pop_args_fn, store, state)
            else
              do_blmpop_wait(keys, timeout_ms, pop_cmd, pop_args_fn, store, state)
            end
        end

      {:error, _} = err ->
        {:continue, Encoder.encode(err), state}
    end
  end

  defp do_blmpop_wait(keys, timeout_ms, pop_cmd, pop_args_fn, store, state) do
    alias Ferricstore.Commands.List
    alias Ferricstore.Waiters

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    Enum.each(keys, fn key -> Waiters.register(key, self(), deadline) end)

    result =
      receive do
        {:waiter_notify, notified_key} ->
          case List.handle(pop_cmd, pop_args_fn.(notified_key), store) do
            nil -> nil
            {:error, _} -> nil
            value ->
              elements = if is_list(value), do: value, else: [value]
              [notified_key, elements]
          end
      after
        timeout_ms ->
          nil
      end

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    {:continue, Encoder.encode(result), state}
  end

  # All other commands go through the Dispatcher with an injected store.
  # But first check pub/sub mode restriction (PING is allowed in pub/sub mode).
  defp dispatch(cmd, args, state) do
    if in_pubsub_mode?(state) and cmd not in ~w(PING) do
      {:continue,
       Encoder.encode({:error, "ERR Can't execute '#{String.downcase(cmd)}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"}),
       state}
    else
      store = build_store(state.sandbox_namespace)
      result = Dispatcher.dispatch(cmd, args, store)
      maybe_notify_keyspace(cmd, args, result)
      {:continue, Encoder.encode(result), state}
    end
  end

  # ---------------------------------------------------------------------------
  # Transaction execution
  # ---------------------------------------------------------------------------

  # Executes the queued commands if WATCH keys are unmodified, or returns nil
  # (abort) if any watched key's version has changed.
  defp execute_transaction(%{watched_keys: watched, multi_queue: queue, sandbox_namespace: ns}) do
    if watches_clean?(watched) do
      results =
        Enum.map(queue, fn {cmd, args} ->
          store = build_store(ns)
          Dispatcher.dispatch(cmd, args, store)
        end)

      results
    else
      # WATCH detected a conflict — abort and return nil (like Redis).
      nil
    end
  end

  # Returns true if all watched keys still have their original versions.
  defp watches_clean?(watched) when map_size(watched) == 0, do: true

  defp watches_clean?(watched) do
    Enum.all?(watched, fn {key, saved_version} ->
      Router.get_version(key) == saved_version
    end)
  end

  # ---------------------------------------------------------------------------
  # Command validation (for queue-time syntax checking)
  # ---------------------------------------------------------------------------

  # Validates that a command is known and has the correct arity without
  # executing it. Uses a no-op store so no side effects occur during validation.
  defp validate_command(cmd, args, _store) do
    noop_store = build_noop_store()

    case Dispatcher.dispatch(cmd, args, noop_store) do
      {:error, "ERR unknown command" <> _} = err -> err
      {:error, "ERR wrong number of arguments" <> _} = err -> err
      {:error, "ERR syntax error" <> _} = err -> err
      _ -> :ok
    end
  end

  # A store that returns safe no-op values for all operations. Used only for
  # command validation during MULTI queuing — no data is read or written.
  defp build_noop_store do
    %{
      get: fn _key -> nil end,
      get_meta: fn _key -> nil end,
      put: fn _key, _value, _expire_at_ms -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn _key -> false end,
      keys: fn -> [] end,
      flush: fn -> :ok end,
      dbsize: fn -> 0 end,
      incr: fn _key, _delta -> {:ok, 0} end,
      incr_float: fn _key, _delta -> {:ok, "0"} end,
      append: fn _key, _suffix -> {:ok, 0} end,
      getset: fn _key, _value -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _expire -> nil end,
      setrange: fn _key, _offset, _value -> {:ok, 0} end,
      cas: fn _key, _exp, _new, _ttl -> nil end,
      lock: fn _key, _owner, _ttl -> :ok end,
      unlock: fn _key, _owner -> 1 end,
      extend: fn _key, _owner, _ttl -> 1 end,
      ratelimit_add: fn _key, _window, _max, _count -> ["allowed", 0, 0, 0] end,
      list_op: fn _key, _op -> nil end
    }
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
  # Store builder — wraps Router functions into the store map contract
  # ---------------------------------------------------------------------------

  defp build_store(nil) do
    build_raw_store()
  end

  defp build_store(ns) when is_binary(ns) do
    raw = build_raw_store()
    %{raw |
      get: fn key -> raw.get.(ns <> key) end,
      get_meta: fn key -> raw.get_meta.(ns <> key) end,
      put: fn key, val, exp -> raw.put.(ns <> key, val, exp) end,
      delete: fn key -> raw.delete.(ns <> key) end,
      exists?: fn key -> raw.exists?.(ns <> key) end
    }
  end

  defp build_raw_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      flush: fn ->
        Enum.each(Router.keys(), &Router.delete/1)
        :ok
      end,
      dbsize: &Router.dbsize/0,
      incr: &Router.incr/2,
      incr_float: &Router.incr_float/2,
      append: &Router.append/2,
      getset: &Router.getset/2,
      getdel: &Router.getdel/1,
      getex: &Router.getex/2,
      setrange: &Router.setrange/3,
      cas: &Router.cas/4,
      lock: &Router.lock/3,
      unlock: &Router.unlock/2,
      extend: &Router.extend/3,
      ratelimit_add: &Router.ratelimit_add/4,
      list_op: &Router.list_op/2,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:get, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:get_meta, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:put, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:delete, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:scan_prefix, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:count_prefix, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:delete_prefix, prefix})
      end
    }
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

  defp send_responses(socket, transport, responses) do
    iodata = List.flatten(responses)
    :ok = transport.send(socket, iodata)
  end

  defp send_response(socket, transport, iodata) do
    :ok = transport.send(socket, iodata)
  end

  # ---------------------------------------------------------------------------
  # Pub/Sub mode loop
  # ---------------------------------------------------------------------------

  defp pubsub_loop(%__MODULE__{socket: socket, transport: transport} = state) do
    receive do
      # TCP active-mode data
      {:tcp, ^socket, data} ->
        transport.setopts(socket, active: :once)
        handle_data(state, data)

      # TLS active-mode data
      {:ssl, ^socket, data} ->
        transport.setopts(socket, active: :once)
        handle_data(state, data)

      # TCP close / error
      {:tcp_closed, ^socket} ->
        cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      # TLS close / error
      {:ssl_closed, ^socket} ->
        cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      # Pub/Sub messages from the internal PubSub engine
      {:pubsub_message, channel, message} ->
        push = {:push, ["message", channel, message]}
        transport.send(socket, Encoder.encode(push))
        transport.setopts(socket, active: :once)
        pubsub_loop(state)

      {:pubsub_pmessage, pattern, channel, message} ->
        push = {:push, ["pmessage", pattern, channel, message]}
        transport.send(socket, Encoder.encode(push))
        transport.setopts(socket, active: :once)
        pubsub_loop(state)
    end
  end

  defp in_pubsub_mode?(state) do
    MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0
  end

  # ---------------------------------------------------------------------------
  # Keyspace notification helpers
  # ---------------------------------------------------------------------------

  # Maps command names to their keyspace notification event names.
  # Only fires on successful results (not errors).
  @keyspace_events %{
    "SET" => "set", "SETNX" => "set", "SETEX" => "set", "PSETEX" => "set",
    "MSET" => "mset", "MSETNX" => "mset",
    "APPEND" => "append", "GETSET" => "getset", "GETDEL" => "getdel",
    "SETRANGE" => "setrange",
    "INCR" => "incr", "DECR" => "decr", "INCRBY" => "incrby",
    "DECRBY" => "decrby", "INCRBYFLOAT" => "incrbyfloat",
    "DEL" => "del", "UNLINK" => "del",
    "EXPIRE" => "expire", "PEXPIRE" => "pexpire",
    "EXPIREAT" => "expireat", "PEXPIREAT" => "pexpireat",
    "PERSIST" => "persist",
    "RENAME" => "rename",
    "LPUSH" => "lpush", "RPUSH" => "rpush",
    "LPOP" => "lpop", "RPOP" => "rpop",
    "LSET" => "lset", "LINSERT" => "linsert", "LTRIM" => "ltrim",
    "LREM" => "lrem", "LMOVE" => "lmove",
    "SADD" => "sadd", "SREM" => "srem", "SPOP" => "spop",
    "HSET" => "hset", "HDEL" => "hdel", "HINCRBY" => "hincrby",
    "HINCRBYFLOAT" => "hincrbyfloat",
    "ZADD" => "zadd", "ZREM" => "zrem", "ZINCRBY" => "zincrby",
    "COPY" => "copy"
  }

  defp maybe_notify_keyspace(cmd, args, result) do
    case Map.get(@keyspace_events, cmd) do
      nil -> :ok
      event -> do_notify_keyspace(cmd, event, args, result)
    end
  end

  # For DEL/UNLINK with multiple keys, notify per key
  defp do_notify_keyspace(cmd, event, keys, count)
       when cmd in ~w(DEL UNLINK) and is_integer(count) and count > 0 do
    Enum.each(keys, fn key -> KeyspaceNotifications.notify(key, event) end)
  end

  # For MSET, notify per key
  defp do_notify_keyspace("MSET", event, args, :ok) do
    args
    |> Enum.chunk_every(2)
    |> Enum.each(fn [key, _val] -> KeyspaceNotifications.notify(key, event) end)
  end

  # Single-key commands: first arg is the key. Skip errors.
  defp do_notify_keyspace(_cmd, _event, _args, {:error, _}), do: :ok
  defp do_notify_keyspace(_cmd, _event, [], _result), do: :ok

  defp do_notify_keyspace(_cmd, event, [key | _], _result) do
    KeyspaceNotifications.notify(key, event)
  end

  defp cleanup_connection(state) do
    duration_ms = System.monotonic_time(:millisecond) - state.created_at

    AuditLog.log(:connection_close, %{
      client_id: state.client_id,
      client_ip: format_peer(state.peer),
      duration_ms: duration_ms
    })

    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    Stats.decr_connections()
  end

  defp cleanup_pubsub(state) do
    Enum.each(state.pubsub_channels, &PS.unsubscribe(&1, self()))
    Enum.each(state.pubsub_patterns, &PS.punsubscribe(&1, self()))
  end

  # Formats a peer tuple `{ip, port}` into a human-readable string.
  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
