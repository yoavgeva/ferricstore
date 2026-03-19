# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Server.Connection do
  @moduledoc """
  Ranch protocol handler for a single FerricStore client connection.

  Each accepted TCP connection spawns one `Connection` process. The process:

  1. Performs the `CLIENT HELLO 3` handshake (RESP3-only; rejects RESP2).
  2. Enters a receive loop, accumulating TCP chunks into a binary buffer.
  3. Parses all complete RESP3 frames from the buffer via `Ferricstore.Resp.Parser`.
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

  ## BEAM scheduler notes (active: :once mode)

  The socket operates in `active: :once` mode (spec section 2C.2): the kernel
  delivers exactly one `{:tcp, socket, data}` message to the process, then
  automatically switches the socket to passive. The process re-arms via
  `transport.setopts(socket, active: :once)` after handling each data message.

  This is superior to the older `active: false` / blocking `recv` approach:
  - The process can handle OTHER messages (waiter notifications, pub/sub pushes,
    client tracking invalidations) between TCP reads.
  - The BEAM scheduler can schedule other processes while waiting for TCP data.
  - Sliding window responses can be sent incrementally.
  - No risk of mailbox flooding (unlike `active: true`) since only one message
    is delivered at a time.
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
    username: "default",
    sandbox_namespace: nil,
    pubsub_channels: MapSet.new(),
    pubsub_patterns: MapSet.new(),
    tracking: nil,
    read_mode: :consistent
  ]

  @type multi_state :: :none | :queuing
  @type read_mode :: :consistent | :stale

  # Commands that read keys and should trigger client tracking registration.
  @read_cmds ~w(GET MGET GETRANGE STRLEN GETEX GETDEL GETSET
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN
    LRANGE LLEN LINDEX LPOS
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER
    ZSCORE ZRANK ZREVRANK ZRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE
    TYPE EXISTS TTL PTTL EXPIRETIME PEXPIRETIME
    GETBIT BITCOUNT BITPOS PFCOUNT
    OBJECT SUBSTR
    GEOHASH GEOPOS GEODIST GEOSEARCH
    XLEN XRANGE XREVRANGE XREAD XINFO
    JSON.GET JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRLEN JSON.MGET)

  # Commands that write keys and should trigger client tracking invalidation.
  @write_cmds ~w(SET SETNX SETEX PSETEX MSET MSETNX APPEND SETRANGE
    INCR DECR INCRBY DECRBY INCRBYFLOAT
    DEL UNLINK
    EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
    RENAME RENAMENX COPY
    HSET HDEL HINCRBY HINCRBYFLOAT HSETNX
    LPUSH RPUSH LPOP RPOP LSET LINSERT LTRIM LREM LMOVE LPUSHX RPUSHX
    SADD SREM SPOP SMOVE SDIFFSTORE SINTERSTORE SUNIONSTORE
    ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX
    SETBIT BITOP PFADD PFMERGE
    GEOADD GEOSEARCHSTORE
    XADD XTRIM XDEL
    JSON.SET JSON.DEL JSON.NUMINCRBY JSON.TOGGLE JSON.CLEAR JSON.ARRAPPEND
    GETSET GETDEL
    CAS LOCK UNLOCK EXTEND)

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
          tracking: ClientTracking.tracking_config() | nil,
          read_mode: read_mode()
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
      transport.send(socket, IO.iodata_to_binary(error_msg))
      transport.close(socket)
    else
      :ok = transport.setopts(socket, active: :once)

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
  end

  # ---------------------------------------------------------------------------
  # Receive loop (active: :once, event-driven)
  # ---------------------------------------------------------------------------

  defp loop(%__MODULE__{socket: socket, transport: transport} = state) do
    if in_pubsub_mode?(state) do
      pubsub_loop(state)
    else
      # Re-arm active: :once so the kernel delivers exactly one more data message.
      # This is idempotent — safe to call even if already in :once mode (e.g. on
      # first entry from init/3).
      transport.setopts(socket, active: :once)

      receive do
        # TCP active-mode data — the kernel delivered exactly one chunk
        {:tcp, ^socket, data} ->
          state
          |> handle_data(data)

        # TLS active-mode data
        {:ssl, ^socket, data} ->
          state
          |> handle_data(data)

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

        # Client tracking invalidation push — sent by notify_key_modified when
        # another connection writes a key this connection is tracking.
        {:tracking_invalidation, iodata} ->
          transport.send(socket, iodata)
          loop(state)
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
    case pipeline_dispatch(commands, state) do
      {:quit, quit_state} ->
        cleanup_connection(quit_state)
        transport.close(socket)

      {:continue, new_state} ->
        loop(new_state)
    end
  end

  # ---------------------------------------------------------------------------
  # Sliding window pipeline dispatch (spec section 2C.2)
  # ---------------------------------------------------------------------------

  # Commands that must be executed synchronously because they read or mutate
  # connection-level state (transaction mode, pub/sub subscriptions, auth,
  # sandbox, blocking ops, etc.).
  @stateful_cmds ~w(
    HELLO CLIENT QUIT AUTH ACL RESET READMODE SANDBOX
    MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    BLPOP BRPOP BLMOVE BLMPOP
  )

  # Normalises a parsed command into `{uppercase_name, args}` for classification.
  # Returns `:unknown` for un-parsable forms.
  defp normalise_cmd({:inline, [name | args]}) when is_binary(name),
    do: {String.upcase(name), args}

  defp normalise_cmd({:inline, []}), do: :unknown

  defp normalise_cmd([name | args]) when is_binary(name),
    do: {String.upcase(name), args}

  defp normalise_cmd(_other), do: :unknown

  # Returns true if the command must be handled sequentially (it reads or
  # mutates connection state, or we are in a mode where all commands are
  # stateful -- e.g. MULTI queuing, pub/sub, pre-auth).
  defp stateful_command?(cmd, state) do
    case normalise_cmd(cmd) do
      :unknown ->
        true

      {name, _args} ->
        # In MULTI queuing mode, every command is stateful (queued or passthrough).
        state.multi_state == :queuing or
          in_pubsub_mode?(state) or
          requires_auth?(state) or
          name in @stateful_cmds or
          # CLIENT subcommand form: "CLIENT" is already in @stateful_cmds,
          # but two-word forms like ["CLIENT", "HELLO", ...] need to match
          # on the first token.
          String.starts_with?(name, "CLIENT")
    end
  end

  # Dispatches a pipeline of commands using a sliding window.
  #
  # For a single command, falls through to sequential dispatch (no benefit from
  # async). For multiple commands, groups consecutive "pure" commands and
  # dispatches them concurrently as Tasks. Responses are sent over the socket
  # in-order as soon as the leading contiguous completed responses are available.
  #
  # Stateful commands (MULTI, AUTH, SUBSCRIBE, blocking ops, etc.) act as
  # barriers: all prior pure-command Tasks are awaited and flushed before the
  # stateful command executes synchronously.
  defp pipeline_dispatch([single_cmd], state) do
    # Single command -- no pipeline, no sliding window needed.
    case handle_command(single_cmd, state) do
      {:quit, response, quit_state} ->
        send_response(state.socket, state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response(state.socket, state.transport, response)
        {:continue, new_state}
    end
  end

  defp pipeline_dispatch(commands, state) do
    sliding_window_dispatch(commands, state)
  end

  # Walks through the command list, building groups of consecutive pure commands
  # that can be dispatched concurrently. When a stateful command is encountered
  # (or the list ends), the current pure group is flushed via the sliding window
  # and the stateful command is executed synchronously.
  defp sliding_window_dispatch(commands, state) do
    # Accumulate consecutive pure commands into a buffer
    do_sliding_window(commands, [], state)
  end

  # Base case: no more commands, flush any remaining pure group.
  defp do_sliding_window([], pure_acc, state) do
    case flush_pure_group(Enum.reverse(pure_acc), state) do
      {:quit, _quit_state} = quit -> quit
      {:continue, new_state} -> {:continue, new_state}
    end
  end

  # Classify current command and either accumulate it (pure) or flush + execute
  # (stateful or barrier).
  defp do_sliding_window([cmd | rest], pure_acc, state) do
    cond do
      stateful_command?(cmd, state) ->
        # Stateful: flush pure group, execute synchronously, may change state.
        case flush_pure_group(Enum.reverse(pure_acc), state) do
          {:quit, _quit_state} = quit ->
            quit

          {:continue, flushed_state} ->
            case handle_command(cmd, flushed_state) do
              {:quit, response, quit_state} ->
                send_response(quit_state.socket, quit_state.transport, response)
                {:quit, quit_state}

              {:continue, response, new_state} ->
                send_response(new_state.socket, new_state.transport, response)
                # After a stateful command, re-classify the remaining commands
                # because state may have changed (e.g. entered MULTI mode).
                do_sliding_window(rest, [], new_state)
            end
        end

      barrier_command?(cmd) ->
        # Barrier: flush pure group (so all prior commands complete first),
        # then include this barrier command as the start of a new pure group.
        # The barrier command itself is safe to dispatch concurrently with
        # SUBSEQUENT commands on different shards.
        case flush_pure_group(Enum.reverse(pure_acc), state) do
          {:quit, _quit_state} = quit ->
            quit

          {:continue, flushed_state} ->
            # Start a new pure group with the barrier command.
            do_sliding_window(rest, [cmd], flushed_state)
        end

      true ->
        # Pure command -- accumulate for concurrent dispatch.
        do_sliding_window(rest, [cmd | pure_acc], state)
    end
  end

  # Flushes a group of pure commands by dispatching them concurrently as Tasks,
  # then sending responses in order via the sliding window.
  #
  # **Shard-aware ordering**: commands that target the same shard are executed
  # sequentially (preserving causal order), while commands targeting different
  # shards execute concurrently. Each Task waits for its predecessor on the
  # same shard to complete before executing, using a lightweight ref-based
  # signalling mechanism.
  #
  # An empty group is a no-op.
  # A single-command group skips Task overhead and dispatches inline.
  defp flush_pure_group([], state), do: {:continue, state}

  defp flush_pure_group([single_cmd], state) do
    case handle_command(single_cmd, state) do
      {:quit, response, quit_state} ->
        send_response(quit_state.socket, quit_state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response(new_state.socket, new_state.transport, response)
        {:continue, new_state}
    end
  end

  defp flush_pure_group(commands, state) do
    store = build_store(state.sandbox_namespace)

    # Shard-aware concurrent dispatch with sliding-window response delivery.
    #
    # Commands are grouped by shard lane. Each lane gets its own Task that
    # executes that lane's commands sequentially (preserving per-key causal
    # order). Lanes targeting different shards run concurrently, reducing
    # total wall-clock time when a pipeline spans multiple shards.
    #
    # Each lane Task sends `{:lane_result, original_index, result}` messages
    # to the connection process as each command completes, enabling the
    # sliding window to send response N as soon as responses 0..N are ready.

    # Step 1: Assign each command an index and shard lane.
    indexed_cmds =
      commands
      |> Enum.with_index()
      |> Enum.map(fn {cmd, idx} -> {cmd, idx, command_shard_key(cmd)} end)

    # Step 2: Group by shard lane (preserving original order within each lane).
    lanes = Enum.group_by(indexed_cmds, fn {_cmd, _idx, shard_key} -> shard_key end)

    total = length(commands)
    conn_pid = self()

    # Step 3: Spawn one Task per shard lane. Each task executes its commands
    # sequentially and sends results back to the connection process.
    lane_tasks =
      Enum.map(lanes, fn {_shard_key, lane_cmds} ->
        Task.async(fn ->
          Enum.each(lane_cmds, fn {cmd, idx, _shard_key} ->
            result = dispatch_pure_command(cmd, store, state)
            send(conn_pid, {:lane_result, idx, result})
          end)
        end)
      end)

    # Step 4: Sliding window -- receive results and send responses in order.
    # We maintain a cursor (next index to send) and a buffer for out-of-order
    # arrivals. Response N is sent as soon as responses 0..N are all available.
    result = sliding_window_collect(state, 0, total, %{})

    # Step 5: Ensure all lane tasks have completed (they should be done by
    # now since we've collected all results, but await to clean up refs).
    Enum.each(lane_tasks, fn task ->
      Task.await(task, :infinity)
    end)

    result
  end

  # Collects results from lane tasks and sends responses in sliding-window order.
  # `cursor` is the next index to send. `buffer` holds results that arrived
  # out of order (index > cursor).
  defp sliding_window_collect(state, cursor, total, _buffer) when cursor >= total do
    {:continue, state}
  end

  defp sliding_window_collect(state, cursor, total, buffer) do
    # Check if the next response is already buffered.
    case Map.pop(buffer, cursor) do
      {{action, response}, new_buffer} ->
        case action do
          :quit ->
            send_response(state.socket, state.transport, response)
            {:quit, state}

          :continue ->
            send_response(state.socket, state.transport, response)
            sliding_window_collect(state, cursor + 1, total, new_buffer)
        end

      {nil, _buffer} ->
        # Not buffered yet -- wait for any lane result message.
        receive do
          {:lane_result, ^cursor, {action, response}} ->
            # It's the one we need -- send immediately.
            case action do
              :quit ->
                send_response(state.socket, state.transport, response)
                {:quit, state}

              :continue ->
                send_response(state.socket, state.transport, response)
                sliding_window_collect(state, cursor + 1, total, buffer)
            end

          {:lane_result, idx, result} when idx > cursor ->
            # Arrived out of order -- buffer it and keep waiting.
            new_buffer = Map.put(buffer, idx, result)
            sliding_window_collect(state, cursor, total, new_buffer)
        end
    end
  end

  # Commands that ALWAYS span multiple shards regardless of arg count. They
  # act as pipeline barriers: the sliding window flushes all preceding
  # commands before allowing a barrier command to execute, ensuring that
  # prior writes are visible.
  @always_multi_cmds ~w(MGET MSET MSETNX BITOP PFCOUNT PFMERGE
    SDIFF SINTER SUNION SDIFFSTORE SINTERSTORE SUNIONSTORE SINTERCARD)

  # Commands that take a variable number of keys. With a single key they can
  # be routed to that key's shard. With multiple keys, they become a barrier.
  @variadic_key_cmds ~w(DEL UNLINK EXISTS)

  # Server-level commands that span all shards and must act as barriers.
  @barrier_server_cmds ~w(DBSIZE FLUSHDB FLUSHALL KEYS SCAN RANDOMKEY)

  # Returns true if the command is a cross-shard barrier that must wait
  # for all preceding pipeline commands to complete before executing.
  defp barrier_command?(cmd) do
    case normalise_cmd(cmd) do
      :unknown -> false
      {name, args} ->
        name in @always_multi_cmds or
          name in @barrier_server_cmds or
          (name in @variadic_key_cmds and length(args) > 1)
    end
  end

  # Server-level commands that don't target a specific key.
  @server_cmds_no_key ~w(PING ECHO DBSIZE FLUSHDB FLUSHALL KEYS INFO COMMAND
    SELECT LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE CONFIG MODULE WAITAOF
    MEMORY RANDOMKEY SCAN OBJECT WAIT
    CLUSTER.HEALTH CLUSTER.STATS FERRICSTORE.HOTNESS FERRICSTORE.METRICS)

  # Determines the shard lane for a command. Returns:
  #   - `{:shard, index}` for single-key commands
  #   - `:barrier` for multi-key/multi-shard commands (global ordering barrier)
  #   - `:server` for server-level commands with no key
  defp command_shard_key(cmd) do
    case normalise_cmd(cmd) do
      :unknown ->
        :server

      {name, args} ->
        cond do
          name in @always_multi_cmds ->
            :barrier

          name in @variadic_key_cmds ->
            case args do
              # Single key: route to that key's shard lane.
              [single_key] -> {:shard, Router.shard_for(single_key)}
              # Multiple keys: global barrier.
              _ -> :barrier
            end

          name in @server_cmds_no_key ->
            :server

          # Single-key commands: first arg is the key
          args != [] ->
            {:shard, Router.shard_for(hd(args))}

          # No args
          true ->
            :server
        end
    end
  end

  # Dispatches a single pure command inside a Task. Returns {action, encoded_response}.
  # Pure commands don't modify connection state, so we don't thread state through.
  defp dispatch_pure_command(cmd, store, state) do
    case normalise_cmd(cmd) do
      :unknown ->
        {:continue, Encoder.encode({:error, "ERR unknown command format"})}

      {name, args} ->
        Stats.incr_commands()

        result =
          try do
            Dispatcher.dispatch(name, args, store)
          catch
            :exit, {:noproc, _} ->
              {:error, "ERR server not ready, shard process unavailable"}

            :exit, {reason, _} ->
              {:error, "ERR internal error: #{inspect(reason)}"}
          end

        maybe_notify_keyspace(name, args, result)
        # Client tracking: notify writes from pipelined commands
        maybe_notify_tracking(name, args, result, state)
        {:continue, Encoder.encode(result)}
    end
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

    {result, updated_conn_state} =
      try do
        Dispatcher.dispatch_client(args, conn_state, store)
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

  defp dispatch("QUIT", _args, state), do: {:quit, Encoder.encode(:ok), state}

  # AUTH command
  #
  # Authentication flow:
  # 1. If no requirepass AND no ACL user has a password set, AUTH is rejected
  #    ("no password is set").
  # 2. Otherwise, resolve the username and password from args and delegate
  #    to `Ferricstore.Acl.authenticate/2`.
  # 3. Backwards compat: when requirepass is set and the ACL default user
  #    has no password, authenticate against requirepass for the default user.
  defp dispatch("AUTH", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}), state}
  end

  defp dispatch("AUTH", args, state) when length(args) > 2 do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}), state}
  end

  defp dispatch("AUTH", args, state) do
    {username, password} =
      case args do
        [pass] -> {"default", pass}
        [user, pass] -> {user, pass}
      end

    requirepass = Ferricstore.Config.get_value("requirepass")
    acl_user = Ferricstore.Acl.get_user(username)
    client_ip = format_peer(state.peer)

    # Determine whether any auth source is configured for this user.
    has_acl_password = acl_user != nil and acl_user.password != nil
    has_requirepass = requirepass != nil and requirepass != ""

    cond do
      # No authentication source configured at all.
      not has_acl_password and not has_requirepass ->
        {:continue, Encoder.encode({:error, "ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?"}), state}

      # ACL user has a password -- always use ACL auth.
      has_acl_password ->
        do_acl_auth(username, password, client_ip, state)

      # Backwards compat: requirepass is set, default user has no ACL password.
      has_requirepass and username == "default" ->
        if password == requirepass do
          AuditLog.log(:auth_success, %{username: username, client_ip: client_ip})
          {:continue, Encoder.encode(:ok), %{state | authenticated: true, username: username}}
        else
          AuditLog.log(:auth_failure, %{username: username, client_ip: client_ip})
          {:continue, Encoder.encode({:error, "WRONGPASS invalid username-password pair or user is disabled."}), state}
        end

      # Non-default user with no ACL password, requirepass is set.
      # Fall through to ACL auth (which accepts any password for nopass users).
      has_requirepass ->
        do_acl_auth(username, password, client_ip, state)

      # Catch-all (should not happen with the above conditions).
      true ->
        {:continue, Encoder.encode({:error, "ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?"}), state}
    end
  end

  defp do_acl_auth(username, password, client_ip, state) do
    case Ferricstore.Acl.authenticate(username, password) do
      {:ok, ^username} ->
        AuditLog.log(:auth_success, %{username: username, client_ip: client_ip})
        {:continue, Encoder.encode(:ok), %{state | authenticated: true, username: username}}

      {:error, reason} ->
        AuditLog.log(:auth_failure, %{username: username, client_ip: client_ip})
        {:continue, Encoder.encode({:error, reason}), state}
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
    {:continue, Encoder.encode(state.username), state}
  end

  defp dispatch_acl("LIST", _, state) do
    {:continue, Encoder.encode(Ferricstore.Acl.list_users()), state}
  end

  defp dispatch_acl("SETUSER", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl|setuser' command"}), state}
  end

  defp dispatch_acl("SETUSER", [username | rules], state) do
    case Ferricstore.Acl.set_user(username, rules) do
      :ok ->
        {:continue, Encoder.encode(:ok), state}

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  defp dispatch_acl("DELUSER", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl|deluser' command"}), state}
  end

  defp dispatch_acl("DELUSER", usernames, state) do
    results =
      Enum.map(usernames, fn username ->
        Ferricstore.Acl.del_user(username)
      end)

    case Enum.find(results, fn r -> match?({:error, _}, r) end) do
      nil ->
        {:continue, Encoder.encode(:ok), state}

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
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

  defp dispatch_acl("GETUSER", [username | _], state) do
    case Ferricstore.Acl.get_user_info(username) do
      nil ->
        {:continue, Encoder.encode(nil), state}

      info ->
        {:continue, Encoder.encode(info), state}
    end
  end

  defp dispatch_acl("GETUSER", [], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl|getuser' command"}), state}
  end

  defp dispatch_acl(_, _, state) do
    {:continue, Encoder.encode({:error, "ERR unknown subcommand or wrong number of arguments for 'acl' command"}), state}
  end

  defp dispatch("RESET", _args, state) do
    # RESET clears transaction state, sandbox namespace, read mode, tracking state,
    # auth state, and pub/sub subscriptions.
    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    new_state = %{state |
      multi_state: :none,
      multi_queue: [],
      watched_keys: %{},
      sandbox_namespace: nil,
      tracking: ClientTracking.new_config(),
      read_mode: :consistent,
      authenticated: false,
      username: "default",
      pubsub_channels: MapSet.new(),
      pubsub_patterns: MapSet.new()
    }
    {:continue, Encoder.encode({:simple, "RESET"}), new_state}
  end

  # -- READMODE command (spec section 5.4) ------------------------------------

  defp dispatch("READMODE", [mode], state) do
    case String.upcase(mode) do
      "STALE" ->
        {:continue, Encoder.encode(:ok), %{state | read_mode: :stale}}

      "CONSISTENT" ->
        {:continue, Encoder.encode(:ok), %{state | read_mode: :consistent}}

      other ->
        {:continue,
         Encoder.encode({:error, "ERR unknown read mode '#{other}', use STALE or CONSISTENT"}),
         state}
    end
  end

  defp dispatch("READMODE", _args, state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'readmode' command"}),
     state}
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

          try do
            keys = Router.keys()
            Enum.each(keys, fn k -> if String.starts_with?(k, ns), do: Router.delete(k) end)
          catch
            :exit, _ -> :ok
          end

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
    try do
      new_watched =
        Enum.reduce(keys, state.watched_keys, fn key, acc ->
          version = Router.get_version(key)
          Map.put(acc, key, version)
        end)

      {:continue, Encoder.encode(:ok), %{state | watched_keys: new_watched}}
    catch
      :exit, {reason, _} ->
        {:continue,
         Encoder.encode({:error, "ERR server not ready: #{inspect(reason)}"}), state}
    end
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

    # No need to switch socket mode — already in active: :once from the main loop.
    # The pubsub_loop will re-arm after handling each message.
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

    # No socket mode switch needed — the main loop handles re-arming active: :once.
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

    # No socket mode switch needed — already in active: :once.
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

    # No socket mode switch needed — the main loop handles re-arming active: :once.
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
  # During the block, `active: :once` is re-armed so we can detect client
  # disconnect ({:tcp_closed, ...}) while waiting for waiter notifications.

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

    # Re-arm active: :once so we can detect client disconnect during the block.
    # The client should not send commands while blocked, but if the connection
    # is closed we need to know.
    state.transport.setopts(state.socket, active: :once)

    result =
      receive do
        {:waiter_notify, notified_key} ->
          # A push happened on one of our keys — try to pop
          case List.handle(pop_cmd, [notified_key], store) do
            nil -> nil
            {:error, _} -> nil
            value -> {:ok, [notified_key, value]}
          end

        # Client disconnected while blocked — clean up and stop.
        {:tcp_closed, _socket} ->
          :client_closed

        {:tcp_error, _socket, _reason} ->
          :client_closed
      after
        timeout_ms ->
          nil
      end

    # Cleanup: unregister from all keys
    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
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

    # Re-arm active: :once so we can detect client disconnect during the block.
    state.transport.setopts(state.socket, active: :once)

    result =
      receive do
        {:waiter_notify, _notified_key} ->
          # Source got a push — try LMOVE
          case List.handle("LMOVE", [source, destination, to_string(from_dir), to_string(to_dir)], store) do
            nil -> nil
            {:error, _} -> nil
            value -> {:ok, value}
          end

        {:tcp_closed, _socket} ->
          :client_closed

        {:tcp_error, _socket, _reason} ->
          :client_closed
      after
        timeout_ms ->
          nil
      end

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
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

    # Re-arm active: :once so we can detect client disconnect during the block.
    state.transport.setopts(state.socket, active: :once)

    result =
      receive do
        {:waiter_notify, notified_key} ->
          case List.handle(pop_cmd, pop_args_fn.(notified_key), store) do
            nil -> nil
            {:error, _} -> nil
            value ->
              elements = if is_list(value), do: value, else: [value]
              {:ok, [notified_key, elements]}
          end

        {:tcp_closed, _socket} ->
          :client_closed

        {:tcp_error, _socket, _reason} ->
          :client_closed
      after
        timeout_ms ->
          nil
      end

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
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

      result =
        try do
          Dispatcher.dispatch(cmd, args, store)
        catch
          :exit, {:noproc, _} ->
            {:error, "ERR server not ready, shard process unavailable"}

          :exit, {reason, _} ->
            {:error, "ERR internal error: #{inspect(reason)}"}
        end

      maybe_notify_keyspace(cmd, args, result)

      # Client tracking: track reads and notify writes
      new_state = maybe_track_read(cmd, args, result, state)
      maybe_notify_tracking(cmd, args, result, state)

      {:continue, Encoder.encode(result), new_state}
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

          try do
            Dispatcher.dispatch(cmd, args, store)
          catch
            :exit, {:noproc, _} ->
              {:error, "ERR server not ready, shard process unavailable"}

            :exit, {reason, _} ->
              {:error, "ERR internal error: #{inspect(reason)}"}
          end
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
      try do
        Router.get_version(key) == saved_version
      catch
        :exit, _ -> false
      end
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
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
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

  defp send_response(socket, transport, iodata) do
    case transport.send(socket, iodata) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Pub/Sub mode loop
  # ---------------------------------------------------------------------------

  defp pubsub_loop(%__MODULE__{socket: socket, transport: transport} = state) do
    # Re-arm active: :once — needed both for the next TCP chunk and so that
    # the kernel can deliver :tcp_closed while we wait for pub/sub pushes.
    transport.setopts(socket, active: :once)

    receive do
      # TCP active-mode data — client sent a command while in pub/sub mode
      # (only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT/RESET are allowed).
      # handle_data flows back through loop -> pubsub_loop, which re-arms.
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      # TLS active-mode data
      {:ssl, ^socket, data} ->
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
        pubsub_loop(state)

      {:pubsub_pmessage, pattern, channel, message} ->
        push = {:push, ["pmessage", pattern, channel, message]}
        transport.send(socket, Encoder.encode(push))
        pubsub_loop(state)

      # Client tracking invalidation push — can arrive while in pub/sub mode
      {:tracking_invalidation, iodata} ->
        transport.send(socket, iodata)
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

  # ---------------------------------------------------------------------------
  # Client tracking helpers
  # ---------------------------------------------------------------------------

  # The socket_sender callback passed to ClientTracking.notify_key_modified/3.
  # Sends a message to the target connection process, which will write the
  # invalidation push to its socket in its main loop.
  @spec tracking_socket_sender() :: (pid(), iodata() -> :ok)
  defp tracking_socket_sender do
    fn target_pid, iodata ->
      send(target_pid, {:tracking_invalidation, iodata})
      :ok
    end
  end

  # After a successful read command, register the read key(s) for tracking.
  # Only called when tracking is enabled on the connection.
  # Returns the (potentially updated) connection state.
  @spec maybe_track_read(binary(), [binary()], term(), t()) :: t()
  defp maybe_track_read(_cmd, _args, _result, %{tracking: %{enabled: false}} = state), do: state
  defp maybe_track_read(_cmd, _args, _result, %{tracking: nil} = state), do: state
  defp maybe_track_read(_cmd, _args, {:error, _}, state), do: state

  defp maybe_track_read(cmd, args, _result, state) when cmd in @read_cmds do
    conn_pid = self()

    case cmd do
      "MGET" ->
        new_tracking = ClientTracking.track_keys(conn_pid, args, state.tracking)
        %{state | tracking: new_tracking}

      "HMGET" ->
        # HMGET key field [field ...] — track the top-level key
        case args do
          [key | _] ->
            new_tracking = ClientTracking.track_key(conn_pid, key, state.tracking)
            %{state | tracking: new_tracking}

          _ ->
            state
        end

      "JSON.MGET" ->
        # JSON.MGET key [key ...] path — track all keys (last arg is the path)
        keys = Enum.drop(args, -1)
        new_tracking = ClientTracking.track_keys(conn_pid, keys, state.tracking)
        %{state | tracking: new_tracking}

      _ ->
        # Single-key commands: first arg is the key
        case args do
          [key | _] ->
            new_tracking = ClientTracking.track_key(conn_pid, key, state.tracking)
            %{state | tracking: new_tracking}

          _ ->
            state
        end
    end
  end

  defp maybe_track_read(_cmd, _args, _result, state), do: state

  # After a successful write command, notify all tracking connections.
  # This can be called from any process (connection process or Task).
  @spec maybe_notify_tracking(binary(), [binary()], term(), t()) :: :ok
  defp maybe_notify_tracking(_cmd, _args, {:error, _}, _state), do: :ok

  defp maybe_notify_tracking(cmd, args, _result, _state) when cmd in @write_cmds do
    writer_pid = self()
    sender = tracking_socket_sender()

    case cmd do
      c when c in ~w(MSET MSETNX) ->
        keys =
          args
          |> Enum.chunk_every(2)
          |> Enum.map(fn [key | _] -> key end)

        ClientTracking.notify_keys_modified(keys, writer_pid, sender)

      c when c in ~w(DEL UNLINK) ->
        ClientTracking.notify_keys_modified(args, writer_pid, sender)

      "RENAME" ->
        # RENAME source destination — both keys are affected
        case args do
          [src, dst | _] ->
            ClientTracking.notify_keys_modified([src, dst], writer_pid, sender)

          _ ->
            :ok
        end

      "COPY" ->
        # COPY source destination — destination is modified
        case args do
          [_src, dst | _] ->
            ClientTracking.notify_key_modified(dst, writer_pid, sender)

          _ ->
            :ok
        end

      _ ->
        # Single-key commands: first arg is the key
        case args do
          [key | _] ->
            ClientTracking.notify_key_modified(key, writer_pid, sender)

          _ ->
            :ok
        end
    end
  end

  defp maybe_notify_tracking(_cmd, _args, _result, _state), do: :ok

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

  # Returns true when the require-tls configuration flag is set.
  defp require_tls? do
    Application.get_env(:ferricstore, :require_tls, false) == true
  end
end
