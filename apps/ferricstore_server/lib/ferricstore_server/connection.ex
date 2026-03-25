# Suppress function clause grouping warnings (clauses added by different agents)
defmodule FerricstoreServer.Connection do
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

  # L1 per-connection cache limits.
  # Maximum number of entries in the L1 cache map.
  @l1_max_entries Application.compile_env(:ferricstore, :l1_cache_max_entries, 64)
  # Maximum total value bytes stored in L1.
  @l1_max_bytes Application.compile_env(:ferricstore, :l1_cache_max_bytes, 1_048_576)
  # Values larger than this threshold are never cached in L1.
  @l1_large_value_skip 262_144

  # Connection safety limits -- prevent unbounded memory growth per connection.
  # Maximum receive buffer size before the connection is closed (128 MB).
  @max_buffer_size 134_217_728
  # Maximum commands queued inside a MULTI transaction (100K).
  @max_multi_queue_size 100_000
  # Maximum commands in a single pipeline batch (100K).
  @max_pipeline_size 100_000

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
    multi_queue_count: 0,
    watched_keys: %{},
    authenticated: false,
    username: "default",
    sandbox_namespace: nil,
    pubsub_channels: nil,
    pubsub_patterns: nil,
    tracking: nil,
    read_mode: :consistent,
    acl_cache: nil,
    # L1 per-connection cache fields
    l1_cache: %{},
    l1_size_bytes: 0,
    l1_enabled: true,
    active_mode: true
  ]

  @type multi_state :: :none | :queuing
  @type read_mode :: :consistent | :stale

  @typedoc """
  Cached ACL permissions for the current user. Populated on AUTH and connection
  init, used for O(1) command permission checks without ETS lookups.
  """
  @type acl_cache :: %{
          commands: :all | MapSet.t(binary()),
          denied_commands: MapSet.t(binary()),
          keys: :all | [Ferricstore.Acl.key_pattern()],
          enabled: boolean()
        }
        | nil

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

  # O(1) MapSet lookups for hot-path classification (replaces linear `in` scans).
  @read_cmds_set MapSet.new(@read_cmds)
  @write_cmds_set MapSet.new(@write_cmds)

  @typedoc """
  A single L1 cache entry: the cached value, its absolute expiry timestamp
  (0 = no expiry), and a saturating hit counter for eviction priority.
  """
  @type l1_entry :: %{value: binary(), expire_at_ms: non_neg_integer(), hits: non_neg_integer()}

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
          multi_queue_count: non_neg_integer(),
          watched_keys: %{binary() => non_neg_integer()},
          tracking: ClientTracking.tracking_config() | nil,
          read_mode: read_mode(),
          acl_cache: acl_cache(),
          l1_cache: %{binary() => l1_entry()},
          l1_size_bytes: non_neg_integer(),
          l1_enabled: boolean()
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
      active_mode = Application.get_env(:ferricstore, :socket_active_mode, true)
      :ok = transport.setopts(socket, active: active_mode)

      Stats.incr_connections()

      peer =
        case transport.peername(socket) do
          {:ok, addr} -> addr
          _ -> nil
        end

      # Fix 3: Protected mode -- reject non-localhost connections when no ACL
      # users are configured and protected mode is active.
      case Ferricstore.Acl.check_protected_mode(peer) do
        {:error, reason} ->
          error_msg = Encoder.encode({:error, reason})
          # transport.send accepts iodata directly; no need to flatten to binary.
          transport.send(socket, error_msg)
          Stats.decr_connections()
          transport.close(socket)

        :ok ->
          # Populate ACL cache for the default user at connection init.
          # This avoids ETS lookups on every command for the common case.
          default_cache = build_acl_cache("default")

          # Join the ACL invalidation process group so we receive
          # {:acl_invalidate, username} messages when ACL rules change.
          join_acl_invalidation_group()

          state = %__MODULE__{
            socket: socket,
            transport: transport,
            client_id: generate_client_id(),
            client_name: nil,
            created_at: System.monotonic_time(:millisecond),
            peer: peer,
            tracking: ClientTracking.new_config(),
            acl_cache: default_cache,
            l1_enabled: Application.get_env(:ferricstore, :l1_cache_enabled, true),
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
  # Receive loop (active: :once, event-driven)
  # ---------------------------------------------------------------------------

  # Normal receive loop — active: :once is rearmed AFTER processing data,
  # not before entering receive. This saves one setopts syscall when the
  # process wakes up from non-TCP messages (tracking, ACL invalidation).
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

      {:tracking_invalidation, iodata, keys} ->
        new_state = l1_invalidate_keys(state, keys)
        transport.send(socket, iodata)
        loop(new_state)

      {:acl_invalidate, username} ->
        loop(maybe_refresh_acl_cache(state, username))
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
    if length(commands) > @max_pipeline_size do
      send_response(
        socket,
        transport,
        Encoder.encode(
          {:error,
           "ERR pipeline batch too large (#{length(commands)} commands, max #{@max_pipeline_size})"}
        )
      )

      loop(state)
    else
      case pipeline_dispatch(commands, state) do
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
  # Variant that accepts a pre-normalised command to avoid redundant String.upcase.
  defp stateful_command_normalised?(:unknown, _state), do: true

  defp stateful_command_normalised?({name, _args}, state) do
    state.multi_state == :queuing or
      in_pubsub_mode?(state) or
      requires_auth?(state) or
      name in @stateful_cmds or
      String.starts_with?(name, "CLIENT")
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
    # Accumulate consecutive pure commands into a buffer.
    # Track normalised forms and all_reads? flag during accumulation to
    # avoid redundant normalise_cmd calls and a separate all_pure_reads? pass.
    do_sliding_window(commands, [], true, state)
  end

  # Base case: no more commands, flush any remaining pure group.
  defp do_sliding_window([], pure_acc, all_reads?, state) do
    case flush_pure_group_pre(Enum.reverse(pure_acc), all_reads?, state) do
      {:quit, _quit_state} = quit -> quit
      {:continue, new_state} -> {:continue, new_state}
    end
  end

  # Classify current command and either accumulate it (pure) or flush + execute
  # (stateful or barrier).
  #
  # Normalise once and pass the result through to avoid redundant String.upcase
  # calls in stateful_command?, barrier_command?, and command_shard_key.
  # Track all_reads? flag during accumulation (optimization #8).
  defp do_sliding_window([cmd | rest], pure_acc, all_reads?, state) do
    normalised = normalise_cmd(cmd)

    cond do
      stateful_command_normalised?(normalised, state) ->
        # Stateful: flush pure group, execute synchronously, may change state.
        case flush_pure_group_pre(Enum.reverse(pure_acc), all_reads?, state) do
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
                do_sliding_window(rest, [], true, new_state)
            end
        end

      barrier_command_normalised?(normalised) ->
        # Barrier: flush pure group (so all prior commands complete first),
        # then include this barrier command as the start of a new pure group.
        case flush_pure_group_pre(Enum.reverse(pure_acc), all_reads?, state) do
          {:quit, _quit_state} = quit ->
            quit

          {:continue, flushed_state} ->
            # Start a new pure group with the barrier command.
            # Barrier commands are not reads, so all_reads? = false.
            is_read = is_read_cmd?(normalised)
            do_sliding_window(rest, [{cmd, normalised}], is_read, flushed_state)
        end

      true ->
        # Pure command -- accumulate for concurrent dispatch.
        # Track whether this command is a read for the all_reads? flag.
        is_read = is_read_cmd?(normalised)
        do_sliding_window(rest, [{cmd, normalised} | pure_acc], all_reads? and is_read, state)
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
  # flush_pure_group_pre accepts pre-normalised {cmd, normalised} pairs and
  # a pre-computed all_reads? flag from do_sliding_window accumulation.
  # This eliminates the separate Enum.reverse + all_pure_reads? pass.
  defp flush_pure_group_pre([], _all_reads?, state), do: {:continue, state}

  defp flush_pure_group_pre([{single_cmd, _norm}], _all_reads?, state) do
    case handle_command(single_cmd, state) do
      {:quit, response, quit_state} ->
        send_response(quit_state.socket, quit_state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response(new_state.socket, new_state.transport, response)
        {:continue, new_state}
    end
  end

  defp flush_pure_group_pre(normalised, all_reads?, state) do
    store = build_store(state.sandbox_namespace)

    # Fast path: if ALL commands are pure reads (tracked during accumulation),
    # skip the sliding window entirely. No Task spawning, no shard grouping,
    # no Map buffer. Just dispatch sequentially and send each response immediately.
    if all_reads? do
      flush_pure_reads_fast_normalised(normalised, store, state)
    else
      flush_pure_group_sliding_window_normalised(normalised, store, state)
    end
  end

  defp flush_pure_reads_fast_normalised(normalised, store, state) do
    Enum.reduce_while(normalised, {:continue, state}, fn {_cmd, norm}, {:continue, acc_state} ->
      case dispatch_pure_command_normalised(norm, store, acc_state) do
        {:quit, response} ->
          send_response(acc_state.socket, acc_state.transport, response)
          {:halt, {:quit, acc_state}}

        {:continue, response} ->
          send_response(acc_state.socket, acc_state.transport, response)
          {:cont, {:continue, acc_state}}
      end
    end)
  end

  defp flush_pure_group_sliding_window_normalised(normalised, store, state) do
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
    # Uses pre-normalised commands to avoid redundant normalise_cmd calls
    # in command_shard_key.
    indexed_cmds =
      normalised
      |> Enum.with_index()
      |> Enum.map(fn {{_cmd, norm}, idx} -> {norm, idx, command_shard_key_normalised(norm)} end)

    # Step 2: Group by shard lane (preserving original order within each lane).
    lanes = Enum.group_by(indexed_cmds, fn {_norm, _idx, shard_key} -> shard_key end)

    total = length(normalised)
    conn_pid = self()

    # Step 3: Spawn one Task per shard lane. Each task executes its commands
    # sequentially and sends results back to the connection process.
    lane_tasks =
      Enum.map(lanes, fn {_shard_key, lane_cmds} ->
        Task.async(fn ->
          Enum.each(lane_cmds, fn {norm, idx, _shard_key} ->
            result = dispatch_pure_command_normalised(norm, store, state)
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

  # Returns true if a normalised command is a read command (O(1) MapSet lookup).
  defp is_read_cmd?({name, _args}), do: MapSet.member?(@read_cmds_set, name)
  defp is_read_cmd?(:unknown), do: false

  # Returns true if the command is a cross-shard barrier that must wait
  # for all preceding pipeline commands to complete before executing.
  # Variant that accepts a pre-normalised command to avoid redundant String.upcase.
  defp barrier_command_normalised?(:unknown), do: false

  defp barrier_command_normalised?({name, args}) do
    name in @always_multi_cmds or
      name in @barrier_server_cmds or
      (name in @variadic_key_cmds and match?([_, _ | _], args))
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
    command_shard_key_normalised(normalise_cmd(cmd))
  end

  # Variant that accepts pre-normalised commands to avoid redundant normalise_cmd.
  defp command_shard_key_normalised(:unknown), do: :server

  defp command_shard_key_normalised({name, args}) do
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

  # Dispatches a single pure command inside a Task. Returns {action, encoded_response}.
  # Pure commands don't modify connection state, so we don't thread state through.
  # ACL command-level checks are applied here for pipelined commands.
  # Accepts a pre-normalised {name, args} tuple to avoid redundant String.upcase.
  defp dispatch_pure_command_normalised(:unknown, _store, _state) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"})}
  end

  defp dispatch_pure_command_normalised({name, args}, store, state) do
    # ACL command-level + key pattern check for pipelined commands (cached, no ETS lookup)
    with :ok <- check_command_cached(state.acl_cache, name),
         :ok <- check_keys_cached(state.acl_cache, name, args) do
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
    else
      {:error, _reason} = err ->
        # Fix 5: Log command denials to the audit log.
        Ferricstore.Acl.log_command_denied(
          state.username,
          name,
          format_peer(state.peer),
          state.client_id
        )

        {:continue, Encoder.encode(err)}
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

  # Commands that bypass ACL command-level checks. These are protocol-level
  # commands needed for connection setup, teardown, and user switching.
  @acl_bypass_cmds ~w(AUTH HELLO QUIT RESET)

  defp handle_command([name | args], state) when is_binary(name) do
    cmd = String.upcase(name)

    cond do
      requires_auth?(state) and cmd not in @pre_auth_cmds ->
        {:continue, Encoder.encode({:error, "NOAUTH Authentication required."}), state}

      cmd not in @acl_bypass_cmds ->
        with :ok <- check_command_cached(state.acl_cache, cmd),
             :ok <- check_keys_cached(state.acl_cache, cmd, args) do
          Stats.incr_commands()
          dispatch(cmd, args, state)
        else
          {:error, _reason} = err ->
            # Fix 5: Log command denials to the audit log.
            Ferricstore.Acl.log_command_denied(
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

  # CLIENT L1CACHE ON|OFF — per-connection L1 cache toggle.
  # Handled directly in the connection layer because it modifies connection struct
  # fields (l1_cache, l1_size_bytes, l1_enabled) that are not part of conn_state.
  defp dispatch("CLIENT", ["L1CACHE", toggle | _], state) do
    case String.upcase(toggle) do
      "ON" ->
        {:continue, Encoder.encode(:ok), %{state | l1_enabled: true}}

      "OFF" ->
        {:continue, Encoder.encode(:ok),
         %{state | l1_enabled: false, l1_cache: %{}, l1_size_bytes: 0}}

      _ ->
        {:continue,
         Encoder.encode({:error, "ERR syntax error, expected CLIENT L1CACHE ON|OFF"}), state}
    end
  end

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

  defp dispatch("AUTH", [_, _, _ | _], state) do
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
          new_cache = build_acl_cache(username)
          {:continue, Encoder.encode(:ok), %{state | authenticated: true, username: username, acl_cache: new_cache}}
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
        new_cache = build_acl_cache(username)
        {:continue, Encoder.encode(:ok), %{state | authenticated: true, username: username, acl_cache: new_cache}}

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
        # Broadcast ACL invalidation to all connections so they refresh
        # their cached permissions for this user.
        broadcast_acl_invalidation(username)

        # If the admin changed their own user, refresh the local cache too.
        new_state =
          if username == state.username do
            %{state | acl_cache: build_acl_cache(username)}
          else
            state
          end

        {:continue, Encoder.encode(:ok), new_state}

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
        # Broadcast ACL invalidation for all deleted users.
        Enum.each(usernames, &broadcast_acl_invalidation/1)
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
    # auth state, pub/sub subscriptions, L1 cache, and rebuilds ACL cache for the
    # default user.
    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    new_state = %{state |
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      watched_keys: %{},
      sandbox_namespace: nil,
      tracking: ClientTracking.new_config(),
      read_mode: :consistent,
      authenticated: false,
      username: "default",
      pubsub_channels: nil,
      pubsub_patterns: nil,
      acl_cache: build_acl_cache("default"),
      l1_cache: %{},
      l1_size_bytes: 0
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
    new_state = %{state | multi_state: :queuing, multi_queue: [], multi_queue_count: 0}
    {:continue, Encoder.encode(:ok), new_state}
  end

  defp dispatch("EXEC", _args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR EXEC without MULTI"}), state}
  end

  defp dispatch("EXEC", _args, state) do
    result = execute_transaction(state)
    new_state = %{state | multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}}
    {:continue, Encoder.encode(result), new_state}
  end

  defp dispatch("DISCARD", _args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR DISCARD without MULTI"}), state}
  end

  defp dispatch("DISCARD", _args, state) do
    new_state = %{state | multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}}
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
    # Lazily initialize MapSets on first subscribe (memory audit L3).
    state = ensure_pubsub_sets(state)

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
    if state.pubsub_channels == nil do
      {:continue, [], state}
    else
      dispatch("UNSUBSCRIBE", MapSet.to_list(state.pubsub_channels), state)
    end
  end

  defp dispatch("UNSUBSCRIBE", channels, state) do
    state = ensure_pubsub_sets(state)

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
    state = ensure_pubsub_sets(state)

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
    if state.pubsub_patterns == nil do
      {:continue, [], state}
    else
      dispatch("PUNSUBSCRIBE", MapSet.to_list(state.pubsub_patterns), state)
    end
  end

  defp dispatch("PUNSUBSCRIBE", patterns, state) do
    state = ensure_pubsub_sets(state)

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
    # MULTI queue limit: auto-DISCARD if queue grows beyond the limit.
    # Uses tracked count (O(1)) instead of length/1 (O(N)) on every command.
    if state.multi_queue_count >= @max_multi_queue_size do
      new_state = %{state | multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}}

      {:continue,
       Encoder.encode(
         {:error,
          "ERR MULTI queue overflow (max #{@max_multi_queue_size} commands), transaction discarded"}
       ), new_state}
    else
      # Validate command syntax at queue time. If the dispatcher returns an
      # error, send it immediately but stay in MULTI mode.
      store = build_store(state.sandbox_namespace)

      case validate_command(cmd, args, store) do
        :ok ->
          # Prepend + reverse at EXEC time: O(1) per queue vs O(N) for ++ append.
          new_queue = [{cmd, args} | state.multi_queue]
          new_count = state.multi_queue_count + 1
          {:continue, Encoder.encode({:simple, "QUEUED"}), %{state | multi_queue: new_queue, multi_queue_count: new_count}}

        {:error, _msg} = err ->
          {:continue, Encoder.encode(err), state}
      end
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

  # -- GET sendfile optimisation (standalone TCP only) --------------------------
  #
  # For large values (>= @sendfile_threshold_bytes), skip the normal path
  # (pread into Rust Vec -> copy to OwnedBinary -> BEAM writev) and instead:
  #   1. writev RESP3 bulk-string header ("$<len>\r\n")
  #   2. sendfile the value bytes from the Bitcask data file (zero userspace copy)
  #   3. writev RESP3 trailer ("\r\n")
  #
  # This only applies when ALL of:
  #   - Transport is :ranch_tcp (not TLS, not embedded)
  #   - Key is cold (on disk, not hot in ETS)
  #   - Value size >= threshold
  #
  # If any condition fails, we fall through to the normal dispatch path.

  @sendfile_threshold_bytes Application.compile_env(
                              :ferricstore_server,
                              :sendfile_threshold,
                              65_536
                            )

  defp dispatch("GET", [key], %{transport: :ranch_tcp} = state)
       when byte_size(key) > 0 and byte_size(key) <= 65_535 do
    if in_pubsub_mode?(state) do
      {:continue,
       Encoder.encode({:error, "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"}),
       state}
    else
      case try_sendfile_get(key, state) do
        {:sent, new_state} ->
          # Response was sent directly via sendfile — return empty iodata.
          {:continue, "", new_state}

        :fallback ->
          # Sendfile not applicable — use normal dispatch.
          dispatch_normal("GET", [key], state)

        {:error_after_header, _reason} ->
          # The RESP header ($<len>\r\n) was already written to the socket but
          # the sendfile or trailer write failed. The protocol stream is now
          # corrupted — we cannot fall back to dispatch_normal (that would send
          # a second RESP header). The only safe option is to close the connection.
          {:quit, "", state}
      end
    end
  end

  defp try_sendfile_get(key, state) do
    alias Ferricstore.Store.Router

    ns = state.sandbox_namespace
    lookup_key = if ns, do: ns <> key, else: key

    case Router.get_file_ref(lookup_key) do
      {path, offset, size} when size >= @sendfile_threshold_bytes ->
        do_sendfile_get(key, path, offset, size, state)

      _ ->
        :fallback
    end
  end

  defp do_sendfile_get(key, path, offset, size, state) do
    socket = state.socket

    # Open the data file (keeps the fd alive even if compaction removes the file).
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          # RESP3 bulk string: $<len>\r\n<data>\r\n
          header = [?$, Integer.to_string(size), "\r\n"]
          trailer = "\r\n"

          # TCP_NOPUSH/TCP_CORK: coalesce the header + sendfile + trailer into
          # fewer TCP segments. :ranch_tcp wraps :gen_tcp which uses :inet.
          set_cork(socket, true)

          case :gen_tcp.send(socket, header) do
            :ok ->
              # IMPORTANT: once the RESP header has been sent, we CANNOT fall
              # back to dispatch_normal — that would send a second $<len>\r\n
              # and corrupt the protocol stream. If sendfile or the trailer
              # fails at this point, the connection is unrecoverable and must
              # be closed.
              case :file.sendfile(fd, socket, offset, size, []) do
                {:ok, _sent} ->
                  case :gen_tcp.send(socket, trailer) do
                    :ok ->
                      set_cork(socket, false)
                      new_state = maybe_track_read_sendfile("GET", [key], state)
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

  # Helper to set/clear TCP_CORK (Linux) or TCP_NOPUSH (macOS/BSD).
  # Silently ignores errors (option may not be available on all platforms).
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

  # Track reads for sendfile path (client tracking support).
  # We pass a non-nil result so that maybe_track_read registers the key.
  defp maybe_track_read_sendfile(cmd, args, state) do
    maybe_track_read(cmd, args, :sendfile_ok, state)
  end

  # Normal dispatch path (extracted for reuse by sendfile fallback).
  # Includes L1 cache integration: reads check L1 first, misses warm L1,
  # and writes update L1 (write-through) or remove stale entries.
  defp dispatch_normal(cmd, args, state) do
    # L1 fast path: for single-key GET, check L1 before hitting L2.
    case l1_try_read(cmd, args, state) do
      {:l1_hit, result, new_state} ->
        # L1 hit — return immediately without touching L2, but still track reads.
        new_state2 = maybe_track_read(cmd, args, result, new_state)
        {:continue, Encoder.encode(result), new_state2}

      :l1_skip ->
        # L1 miss or not applicable — fall through to normal L2 dispatch.
        dispatch_normal_l2(cmd, args, state)
    end
  end

  # L2 dispatch: the original dispatch path, with L1 warming on read results
  # and L1 removal on write results.
  defp dispatch_normal_l2(cmd, args, state) do
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
    new_state = maybe_track_read(cmd, args, result, state)
    maybe_notify_tracking(cmd, args, result, state)

    # L1 post-dispatch: warm L1 on read miss (with expiry from L2 metadata),
    # or remove key from L1 on write.
    new_state2 = l1_post_dispatch(cmd, args, result, new_state, store)

    {:continue, Encoder.encode(result), new_state2}
  end

  # -- XREAD BLOCK dispatch -----------------------------------------------------
  # XREAD with BLOCK option needs connection-level blocking similar to BLPOP.
  # The Stream handler returns {:block, timeout_ms, stream_ids, count} when
  # no data is immediately available and BLOCK was specified.

  defp dispatch("XREAD", args, state) do
    store = build_store(state.sandbox_namespace)

    result =
      try do
        Dispatcher.dispatch("XREAD", args, store)
      catch
        :exit, {:noproc, _} ->
          {:error, "ERR server not ready, shard process unavailable"}

        :exit, {reason, _} ->
          {:error, "ERR internal error: #{inspect(reason)}"}
      end

    case result do
      {:block, timeout_ms, stream_ids, count} ->
        dispatch_xread_block(timeout_ms, stream_ids, count, store, state)

      other ->
        maybe_notify_keyspace("XREAD", args, other)
        new_state = maybe_track_read("XREAD", args, other, state)
        maybe_notify_tracking("XREAD", args, other, state)
        {:continue, Encoder.encode(other), new_state}
    end
  end

  defp dispatch_xread_block(timeout_ms, stream_ids, count, store, state) do
    alias Ferricstore.Commands.Stream, as: StreamCmd

    keys = Enum.map(stream_ids, fn {key, _id} -> key end)

    # Register as waiter for all watched stream keys.
    Enum.each(stream_ids, fn {key, id_str} ->
      StreamCmd.register_stream_waiter(key, self(), id_str)
    end)

    # Cap timeout=0 (block forever) at 5 minutes.
    effective_timeout = if timeout_ms == 0, do: 300_000, else: timeout_ms

    # Re-arm active: :once so we can detect client disconnect during the block.
    state.transport.setopts(state.socket, active: :once)

    result =
      receive do
        {:stream_waiter_notify, _notified_key} ->
          # A new entry was added to one of our watched streams -- re-read.
          read_result =
            try do
              StreamCmd.handle("XREAD", build_xread_args(stream_ids, count), store)
            catch
              _, _ -> []
            end

          case read_result do
            {:block, _, _, _} -> nil
            other when is_list(other) and other != [] -> {:ok, other}
            _ -> nil
          end

        {:tcp_closed, _socket} ->
          :client_closed

        {:tcp_error, _socket, _reason} ->
          :client_closed
      after
        effective_timeout ->
          nil
      end

    # Cleanup: unregister from all stream keys.
    Enum.each(keys, fn key -> StreamCmd.unregister_stream_waiter(key, self()) end)

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

  # Builds XREAD args from stream_ids and count for re-read after notification.
  defp build_xread_args(stream_ids, count) do
    keys = Enum.map(stream_ids, fn {key, _id} -> key end)
    ids = Enum.map(stream_ids, fn {_key, id} -> id end)

    count_args = if count == :infinity, do: [], else: ["COUNT", Integer.to_string(count)]
    count_args ++ ["STREAMS"] ++ keys ++ ids
  end

  # All other commands go through the Dispatcher with an injected store.
  # But first check pub/sub mode restriction (PING is allowed in pub/sub mode).
  # Delegates to dispatch_normal which handles L1 cache integration.
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
  # Transaction execution
  # ---------------------------------------------------------------------------

  # Delegates to the 2PC Coordinator which handles both single-shard (fast path)
  # and cross-shard (2PC) transactions. WATCH conflict detection is also handled
  # by the Coordinator.
  defp execute_transaction(%{watched_keys: watched, multi_queue: queue, sandbox_namespace: ns}) do
    # Queue is stored in reverse order (prepend during MULTI) for O(1)
    # queuing. Reverse here at EXEC time to restore command ordering.
    Ferricstore.Transaction.Coordinator.execute(Enum.reverse(queue), watched, ns)
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
      keys_with_prefix: fn _prefix -> [] end,
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

  # The raw store map is identical for every non-sandbox command dispatch.
  # Built once lazily via persistent_term to eliminate ~25 closure allocations
  # per command. Subsequent calls are a single pointer deref (~5ns).
  #
  # Audit C3 note: this is a plain map rather than a struct. A struct would
  # give OTP 26+ type inference transparent field types, but converting would
  # require touching every command handler (~25 modules) since they all
  # access `store.get.()` etc. The practical impact is negligible:
  #   - The non-sandbox path reads from persistent_term (one pointer deref).
  #   - The sandbox path rebuilds only 5 namespace-wrapping closures; the
  #     remaining ~20 fields are shared from the cached raw store.
  #   - Each closure call is a direct function dispatch (~10ns); the type
  #     opacity only prevents the JIT from inlining across the store boundary,
  #     which would save <5ns per call — well below the GenServer/ETS cost
  #     that dominates every operation.
  # The struct conversion is tracked as a future cleanup but is not worth the
  # risk of a cross-cutting refactor for the marginal type inference benefit.
  defp raw_store do
    case :persistent_term.get(:ferricstore_raw_store, nil) do
      nil ->
        store = build_raw_store()
        :persistent_term.put(:ferricstore_raw_store, store)
        store
      store ->
        store
    end
  end

  defp build_raw_store do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      keys_with_prefix: &Router.keys_with_prefix/1,
      flush: fn ->
        shard_count = Application.get_env(:ferricstore, :shard_count, 4)
        Enum.each(0..(shard_count - 1), &Ferricstore.Raft.AsyncApplyWorker.drain/1)
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

  defp build_store(nil), do: raw_store()

  defp build_store(ns) when is_binary(ns) do
    raw = raw_store()
    %{raw |
      get: fn key -> raw.get.(ns <> key) end,
      get_meta: fn key -> raw.get_meta.(ns <> key) end,
      put: fn key, val, exp -> raw.put.(ns <> key, val, exp) end,
      delete: fn key -> raw.delete.(ns <> key) end,
      exists?: fn key -> raw.exists?.(ns <> key) end
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

      # Client tracking invalidation push — can arrive while in pub/sub mode.
      # Keys are passed directly to avoid RESP re-parsing.
      {:tracking_invalidation, iodata, keys} ->
        new_state = l1_invalidate_keys(state, keys)
        transport.send(socket, iodata)
        pubsub_loop(new_state)

      # ACL invalidation — refresh cache even while in pub/sub mode
      {:acl_invalidate, username} ->
        pubsub_loop(maybe_refresh_acl_cache(state, username))
    end
  end

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false
  defp in_pubsub_mode?(state), do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  # Lazily initializes pubsub_channels and pubsub_patterns MapSets.
  # Called on first SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE to avoid allocating
  # ~80 bytes of empty MapSets on every connection (memory audit L3).
  defp ensure_pubsub_sets(%{pubsub_channels: nil} = state) do
    %{state | pubsub_channels: MapSet.new(), pubsub_patterns: MapSet.new()}
  end
  defp ensure_pubsub_sets(state), do: state

  # ---------------------------------------------------------------------------
  # L1 per-connection cache helpers
  # ---------------------------------------------------------------------------
  #
  # L1 is a Map in each connection process state that caches GET results to
  # avoid repeated ETS (L2) lookups for hot keys. Each entry stores the binary
  # value, its absolute expiry timestamp, and a saturating hit counter used for
  # eviction priority (lowest-hit-count entry is evicted first).
  #
  # Invariants:
  #   - l1_cache is a Map of key => %{value: binary, expire_at_ms: int, hits: int}
  #   - l1_size_bytes == sum of byte_size(entry.value) for all entries in l1_cache
  #   - map_size(l1_cache) <= @l1_max_entries (enforced by eviction)
  #   - l1_size_bytes <= @l1_max_bytes (enforced by eviction)

  # Commands eligible for L1 read cache. Currently only single-key GET.
  # MGET, HGET, etc. could be added later but GET covers the vast majority
  # of hot-path reads.
  @l1_read_cmds ~w(GET)

  # Commands whose results should trigger L1 removal of the affected key.
  # All writes invalidate the L1 entry to prevent staleness. Clients re-warm
  # L1 on the next GET. Write-through (caching the new value directly) is
  # intentionally NOT done because without CLIENT TRACKING enabled, other
  # connections' writes would never invalidate the L1 entry, leading to stale
  # reads.
  @l1_write_cmds ~w(SET SETNX SETEX PSETEX DEL UNLINK EXPIRE PEXPIRE EXPIREAT
    PEXPIREAT PERSIST INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND GETSET
    GETDEL SETRANGE RENAME RENAMENX COPY MSET MSETNX)

  # Attempts an L1 read for the given command. Returns {:l1_hit, value, state}
  # if the value is cached and valid, or :l1_skip if L1 doesn't apply.
  @spec l1_try_read(binary(), [binary()], t()) :: {:l1_hit, term(), t()} | :l1_skip
  defp l1_try_read(cmd, args, state) do
    with true <- state.l1_enabled,
         true <- cmd in @l1_read_cmds,
         [key] <- args do
      l1_get(state, key)
    else
      _ -> :l1_skip
    end
  end

  # Looks up a key in the L1 cache. Checks expiry and increments the hit counter.
  # Returns {:l1_hit, value, updated_state} on cache hit, or :l1_skip on miss.
  # Note: expired entries cause a miss but the stale entry is lazily evicted
  # (it will be replaced by l1_post_dispatch warming from the L2 result).
  @spec l1_get(t(), binary()) :: {:l1_hit, binary() | nil, t()} | :l1_skip
  defp l1_get(state, key) do
    case Map.get(state.l1_cache, key) do
      nil ->
        :l1_skip

      %{value: value, expire_at_ms: 0, hits: h} ->
        {:l1_hit, value, l1_touch(state, key, h)}

      %{value: value, expire_at_ms: exp, hits: h} ->
        if exp > System.os_time(:millisecond) do
          {:l1_hit, value, l1_touch(state, key, h)}
        else
          # Expired — miss. The stale entry will be replaced when
          # l1_post_dispatch warms L1 with the fresh L2 result.
          :l1_skip
        end
    end
  end

  # Increments the hit counter for a cached entry (saturates at 255).
  defp l1_touch(state, key, hits) do
    new_hits = min(hits + 1, 255)
    new_cache = Map.update!(state.l1_cache, key, fn e -> %{e | hits: new_hits} end)
    %{state | l1_cache: new_cache}
  end

  # Post-dispatch L1 integration: warms L1 on read misses, removes L1 on writes.
  # The store is passed so that read warming can fetch expiry metadata from L2.
  @spec l1_post_dispatch(binary(), [binary()], term(), t(), map()) :: t()
  defp l1_post_dispatch(_cmd, _args, {:error, _}, state, _store), do: state

  defp l1_post_dispatch(cmd, [key], result, state, store) when cmd in @l1_read_cmds do
    # Read command — warm L1 with the result value and its expiry from L2.
    l1_maybe_warm(state, key, result, store)
  end

  defp l1_post_dispatch(cmd, args, _result, state, _store) when cmd in @l1_write_cmds do
    # Write command — remove the affected key(s) from L1 to prevent staleness.
    # We do NOT write-through (cache the new value) because without CLIENT
    # TRACKING, other connections' writes would never clear the L1 entry.
    # The next GET will re-warm L1 from L2.
    case cmd do
      c when c in ~w(MSET MSETNX) ->
        # MSET key1 val1 key2 val2 ... — remove all keys
        args
        |> Enum.chunk_every(2)
        |> Enum.reduce(state, fn
          [key | _], s -> l1_remove(s, key)
          _, s -> s
        end)

      "RENAME" ->
        # RENAME source dest — both keys are affected
        case args do
          [src, dst | _] -> state |> l1_remove(src) |> l1_remove(dst)
          [key] -> l1_remove(state, key)
          _ -> state
        end

      _ ->
        # Single-key commands: first arg is the key
        case args do
          [key | _] -> l1_remove(state, key)
          _ -> state
        end
    end
  end

  defp l1_post_dispatch(_cmd, _args, _result, state, _store), do: state

  # Conditionally warms L1 with a value. Fetches expiry from L2 metadata via
  # the store's get_meta function so that expired keys are properly handled.
  @spec l1_maybe_warm(t(), binary(), term(), map()) :: t()
  defp l1_maybe_warm(state, _key, nil, _store), do: state

  defp l1_maybe_warm(state, key, value, store) when is_binary(value) do
    if not state.l1_enabled do
      state
    else
      val_size = byte_size(value)

      # Fetch expiry from L2 metadata. The get_meta call is cheap (ETS hot path)
      # and only done on L1 miss (not on every read).
      expire_at_ms =
        try do
          case store.get_meta.(key) do
            {_value, exp} when is_integer(exp) -> exp
            _ -> 0
          end
        catch
          _, _ -> 0
        end

      cond do
        val_size > @l1_large_value_skip ->
          # Skip caching values larger than the threshold.
          state

        map_size(state.l1_cache) >= @l1_max_entries ->
          l1_evict_and_put(state, key, value, expire_at_ms)

        state.l1_size_bytes + val_size > @l1_max_bytes ->
          l1_evict_and_put(state, key, value, expire_at_ms)

        true ->
          l1_put(state, key, value, expire_at_ms)
      end
    end
  end

  defp l1_maybe_warm(state, _key, _non_binary_result, _store), do: state

  # Inserts a key-value pair into L1, optionally with an expiry timestamp.
  # If the key already exists, replaces it (adjusting size tracking).
  @spec l1_put(t(), binary(), binary(), non_neg_integer()) :: t()
  defp l1_put(state, key, value, expire_at_ms) do
    if not state.l1_enabled do
      state
    else
      val_size = byte_size(value)

      if val_size > @l1_large_value_skip do
        state
      else
        entry = %{value: value, expire_at_ms: expire_at_ms, hits: 1}

        # Remove old entry size if replacing.
        old_size =
          case Map.get(state.l1_cache, key) do
            nil -> 0
            old -> byte_size(old.value)
          end

        new_bytes = state.l1_size_bytes - old_size + val_size
        %{state | l1_cache: Map.put(state.l1_cache, key, entry), l1_size_bytes: new_bytes}
      end
    end
  end

  # Evicts the lowest-hit entry and inserts a new key-value pair.
  defp l1_evict_and_put(state, key, value, expire_at_ms) do
    if map_size(state.l1_cache) == 0 do
      l1_put(state, key, value, expire_at_ms)
    else
      # Find the entry with the lowest hit count.
      {evict_key, evict_entry} = Enum.min_by(state.l1_cache, fn {_, e} -> e.hits end)
      evicted_size = byte_size(evict_entry.value)
      new_cache = Map.delete(state.l1_cache, evict_key)
      new_bytes = state.l1_size_bytes - evicted_size

      new_state = %{state | l1_cache: new_cache, l1_size_bytes: new_bytes}

      # Check if we still need more evictions (for byte limit).
      val_size = byte_size(value)

      if map_size(new_cache) >= @l1_max_entries or new_bytes + val_size > @l1_max_bytes do
        l1_evict_and_put(new_state, key, value, expire_at_ms)
      else
        l1_put(new_state, key, value, expire_at_ms)
      end
    end
  end

  # Removes a key from L1 and adjusts byte tracking.
  @spec l1_remove(t(), binary()) :: t()
  defp l1_remove(state, key) do
    case Map.pop(state.l1_cache, key) do
      {nil, _} ->
        state

      {entry, new_cache} ->
        %{state | l1_cache: new_cache, l1_size_bytes: state.l1_size_bytes - byte_size(entry.value)}
    end
  end

  # Removes the given keys from L1 cache directly, without re-parsing RESP.
  # Used when the invalidation message sender passes keys alongside iodata.
  @spec l1_invalidate_keys(t(), [binary()]) :: t()
  defp l1_invalidate_keys(state, keys) do
    if map_size(state.l1_cache) == 0 do
      state
    else
      Enum.reduce(keys, state, fn key, s -> l1_remove(s, key) end)
    end
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
  # invalidation push to its socket in its main loop. Includes the pre-extracted
  # key list alongside the iodata to avoid RESP re-parsing for L1 invalidation.
  @spec tracking_socket_sender() :: (pid(), iodata(), [binary()] -> :ok)
  defp tracking_socket_sender do
    fn target_pid, iodata, keys ->
      send(target_pid, {:tracking_invalidation, iodata, keys})
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

  defp maybe_notify_tracking(cmd, args, _result, _state) do
    # O(1) MapSet check replaces linear `when cmd in @write_cmds` guard (~55 chained ==).
    if MapSet.member?(@write_cmds_set, cmd) do
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
    else
      :ok
    end
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
    Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
    Stats.decr_connections()
  end

  defp cleanup_pubsub(state) do
    if state.pubsub_channels, do: Enum.each(state.pubsub_channels, &PS.unsubscribe(&1, self()))
    if state.pubsub_patterns, do: Enum.each(state.pubsub_patterns, &PS.punsubscribe(&1, self()))
  end

  # Formats a peer tuple `{ip, port}` into a human-readable string.
  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"

  # Returns true when the require-tls configuration flag is set.
  defp require_tls? do
    Application.get_env(:ferricstore, :require_tls, false) == true
  end

  # ---------------------------------------------------------------------------
  # ACL cache — eliminates ETS lookups on every command
  # ---------------------------------------------------------------------------

  # The process group name for ACL invalidation broadcasts.
  @acl_pg_group :ferricstore_acl_connections

  @doc false
  @spec acl_pg_group() :: atom()
  def acl_pg_group, do: @acl_pg_group

  # Builds a cached ACL permission snapshot for the given username.
  # Does a single ETS lookup and extracts the fields needed for command checks.
  # Returns nil if the user does not exist, `:full_access` for unrestricted
  # users (commands: :all, no denied commands, keys: :all, enabled: true),
  # or a map with the ACL fields for restricted users.
  #
  # The `:full_access` atom enables O(1) fast-path checks in
  # `check_command_cached/2` and `check_keys_cached/3`, skipping all MapSet
  # and Catalog operations for the common default-user case.
  @spec build_acl_cache(binary()) :: acl_cache() | :full_access
  defp build_acl_cache(username) do
    case Ferricstore.Acl.get_user(username) do
      nil ->
        nil

      user ->
        denied = Map.get(user, :denied_commands, MapSet.new())

        if user.enabled and user.commands == :all and
             MapSet.size(denied) == 0 and user.keys == :all do
          :full_access
        else
          %{
            commands: user.commands,
            denied_commands: denied,
            keys: user.keys,
            enabled: user.enabled
          }
        end
    end
  end

  # Pure function: checks if a command is permitted using the cached ACL data.
  # No ETS lookup, no process call — just pattern matching on local state.
  # The `cmd` argument is expected to be already uppercase (from normalise_cmd).
  # Returns `:ok` if permitted, `{:error, reason}` if denied.
  @spec check_command_cached(acl_cache() | :full_access, binary()) :: :ok | {:error, binary()}
  defp check_command_cached(nil, _cmd), do: :ok

  # Fast path: unrestricted user — single atom comparison, zero MapSet/map ops.
  # Covers the common default-user case (commands: :all, no denied, keys: :all).
  defp check_command_cached(:full_access, _cmd), do: :ok

  # Fast path: full-access user with no denied commands — skip all MapSet ops.
  # This covers the case where build_acl_cache returned a map (e.g. user has
  # keys restrictions but commands: :all with no denied commands).
  defp check_command_cached(%{commands: :all, denied_commands: %MapSet{map: denied_map}, enabled: true}, _cmd)
       when map_size(denied_map) == 0 do
    :ok
  end

  defp check_command_cached(cache, cmd) do
    cond do
      not cache.enabled ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd) ->
        :ok

      cache.commands == :all ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      MapSet.member?(cache.commands, cmd) and
          not MapSet.member?(cache.denied_commands, cmd) ->
        :ok

      true ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
    end
  end

  # Pure function: checks if the cached ACL key patterns allow access to
  # all keys touched by a command. Uses Catalog.get_keys/2 to extract keys
  # from the command arguments, then checks each key against the user's
  # compiled patterns.
  #
  # Returns `:ok` if all keys pass, `{:error, reason}` if any key is denied.
  # Commands with no keys (PING, INFO, etc.) always pass.
  @spec check_keys_cached(acl_cache() | :full_access, binary(), [binary()]) :: :ok | {:error, binary()}
  defp check_keys_cached(nil, _cmd, _args), do: :ok
  defp check_keys_cached(:full_access, _cmd, _args), do: :ok
  defp check_keys_cached(%{keys: :all}, _cmd, _args), do: :ok

  defp check_keys_cached(%{keys: patterns}, cmd, args) do
    alias Ferricstore.Commands.Catalog

    # cmd is already uppercase from normalise_cmd — use get_keys_upper to
    # skip the String.downcase inside Catalog.lookup.
    case Catalog.get_keys_upper(cmd, args) do
      {:ok, []} ->
        :ok

      {:ok, keys} ->
        access_type = command_access_type(cmd)
        check_all_keys(keys, access_type, patterns)

      {:error, _} ->
        # Unknown command — let the dispatcher handle the error later.
        :ok
    end
  end

  # Check every key against the patterns. Short-circuits on first denial.
  @spec check_all_keys([binary()], :read | :write | :rw, [Ferricstore.Acl.key_pattern()]) ::
          :ok | {:error, binary()}
  defp check_all_keys([], _access_type, _patterns), do: :ok

  defp check_all_keys([key | rest], access_type, patterns) do
    # For :rw access (both read and write), the key must pass BOTH read and write checks.
    types_to_check =
      case access_type do
        :rw -> [:read, :write]
        other -> [other]
      end

    all_pass =
      Enum.all?(types_to_check, fn t ->
        Ferricstore.Acl.key_matches_any?(key, t, patterns)
      end)

    if all_pass do
      check_all_keys(rest, access_type, patterns)
    else
      {:error,
       "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
    end
  end

  # Determines the access type for a command: :read, :write, or :rw (both).
  # Commands that both read and write (GETSET, GETDEL, GETEX, CAS) require
  # both read and write key permissions.
  # The `cmd` argument is expected to be already uppercase (from normalise_cmd).
  @read_write_cmds_set MapSet.new(~w(GETSET GETDEL GETEX CAS))
  @spec command_access_type(binary()) :: :read | :write | :rw
  defp command_access_type(cmd) do
    cond do
      MapSet.member?(@read_write_cmds_set, cmd) -> :rw
      MapSet.member?(@read_cmds_set, cmd) -> :read
      MapSet.member?(@write_cmds_set, cmd) -> :write
      # Default to :rw for unknown commands — most conservative.
      true -> :rw
    end
  end

  # Joins the OTP :pg process group for ACL invalidation broadcasts.
  # Called once during connection init. The process is automatically removed
  # from the group when it terminates (no explicit leave needed).
  # The :pg scope is started by FerricstoreServer.Application.
  @spec join_acl_invalidation_group() :: :ok
  defp join_acl_invalidation_group do
    :pg.join(@acl_pg_group, @acl_pg_group, self())
    :ok
  end

  # Broadcasts an ACL invalidation message to all connection processes.
  # Called by SETUSER/DELUSER after successfully modifying a user.
  @spec broadcast_acl_invalidation(binary()) :: :ok
  defp broadcast_acl_invalidation(username) do
    members =
      try do
        :pg.get_members(@acl_pg_group, @acl_pg_group)
      catch
        :error, _ -> []
      end

    for pid <- members, pid != self() do
      send(pid, {:acl_invalidate, username})
    end

    :ok
  end

  # Refreshes the local ACL cache if the invalidated username matches
  # the current connection's username. If the user was deleted, the cache
  # becomes nil (subsequent commands will be denied).
  @spec maybe_refresh_acl_cache(t(), binary()) :: t()
  defp maybe_refresh_acl_cache(state, invalidated_username) do
    if invalidated_username == state.username do
      %{state | acl_cache: build_acl_cache(state.username)}
    else
      state
    end
  end
end
