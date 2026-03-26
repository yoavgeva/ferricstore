defmodule Ferricstore.Commands.Dispatcher do
  @moduledoc """
  Routes Redis command names to the appropriate handler module.

  The dispatcher normalises the command name to uppercase and delegates to one of:

    * `Ferricstore.Commands.Strings` — GET, SET, DEL, EXISTS, MGET, MSET,
      INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETSET, GETDEL,
      GETEX, SETNX, SETEX, PSETEX, GETRANGE, SETRANGE, MSETNX
    * `Ferricstore.Commands.Expiry` — EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST
    * `Ferricstore.Commands.Generic` — TYPE, UNLINK, RENAME, RENAMENX, COPY,
      RANDOMKEY, SCAN, EXPIRETIME, PEXPIRETIME, OBJECT, WAIT
    * `Ferricstore.Commands.Server` — PING, ECHO, DBSIZE, KEYS, FLUSHDB, FLUSHALL,
      INFO, COMMAND, SELECT, LOLWUT, DEBUG

  Multi-word commands (`CLIENT`, `COMMAND`) are routed based on the first word, then
  the subcommand is extracted from args. `CLIENT` subcommands require connection
  state and are dispatched via `dispatch_client/3`.

  Unknown commands return `{:error, "ERR unknown command ..."}`.
  """

  alias Ferricstore.Commands.{Bitmap, Bloom, Client, Cluster, Cuckoo, Expiry, Generic, Geo, Hash, HyperLogLog, Json, List, Memory, Namespace, Native, PubSub, Server, Set, SortedSet, Stream, Strings, Vector}
  alias Ferricstore.Commands.CMS
  alias Ferricstore.Commands.TDigest
  alias Ferricstore.Commands.TopK

  # Build a compile-time Map from command name -> handler tag for O(1) dispatch.
  # Each tag corresponds to a handler module and any special pre-processing.
  @string_cmds ~w(GET SET DEL EXISTS MGET MSET INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETSET GETDEL GETEX SETNX SETEX PSETEX GETRANGE SETRANGE MSETNX)
  @expiry_cmds ~w(EXPIRE EXPIREAT PEXPIRE PEXPIREAT TTL PTTL PERSIST)
  @generic_cmds ~w(TYPE UNLINK RENAME RENAMENX COPY RANDOMKEY SCAN EXPIRETIME PEXPIRETIME OBJECT WAIT)
  @bitmap_cmds ~w(SETBIT GETBIT BITCOUNT BITPOS BITOP)
  @hll_cmds ~w(PFADD PFCOUNT PFMERGE)
  @hash_cmds ~w(HSET HGET HDEL HMGET HGETALL HEXISTS HKEYS HVALS HLEN HINCRBY HINCRBYFLOAT HSETNX HSTRLEN HRANDFIELD HSCAN HEXPIRE HTTL HPERSIST HPEXPIRE HPTTL HEXPIRETIME HGETDEL HGETEX HSETEX)
  @list_cmds ~w(LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX LSET LREM LTRIM LPOS LINSERT LMOVE LPUSHX RPUSHX RPOPLPUSH)
  @set_cmds ~w(SADD SREM SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SPOP SDIFF SINTER SUNION SDIFFSTORE SINTERSTORE SUNIONSTORE SINTERCARD SMOVE SSCAN)
  @zset_cmds ~w(ZADD ZREM ZSCORE ZRANK ZREVRANK ZRANGE ZREVRANGE ZCARD ZINCRBY ZCOUNT ZPOPMIN ZPOPMAX ZRANDMEMBER ZSCAN ZMSCORE ZRANGEBYSCORE ZREVRANGEBYSCORE)
  @geo_cmds ~w(GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE)
  @stream_cmds ~w(XADD XLEN XRANGE XREVRANGE XREAD XTRIM XDEL XINFO XGROUP XREADGROUP XACK)
  @json_cmds ~w(JSON.SET JSON.GET JSON.DEL JSON.NUMINCRBY JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRAPPEND JSON.ARRLEN JSON.TOGGLE JSON.CLEAR JSON.MGET)
  @native_cmds ~w(CAS LOCK UNLOCK EXTEND FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR)
  @bloom_cmds ~w(BF.RESERVE BF.ADD BF.MADD BF.EXISTS BF.MEXISTS BF.CARD BF.INFO)
  @cuckoo_cmds ~w(CF.RESERVE CF.ADD CF.ADDNX CF.DEL CF.EXISTS CF.MEXISTS CF.COUNT CF.INFO)
  @cms_cmds ~w(CMS.INITBYDIM CMS.INITBYPROB CMS.INCRBY CMS.QUERY CMS.MERGE CMS.INFO)
  @topk_cmds ~w(TOPK.RESERVE TOPK.ADD TOPK.INCRBY TOPK.QUERY TOPK.LIST TOPK.INFO)
  @tdigest_cmds ~w(TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.QUANTILE TDIGEST.CDF TDIGEST.TRIMMED_MEAN TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK TDIGEST.BYREVRANK TDIGEST.MERGE)
  @vector_cmds ~w(VCREATE VADD VGET VDEL VSEARCH VINFO VLIST VEVICT)
  @pubsub_cmds ~w(PUBLISH PUBSUB)
  @server_cmds ~w(PING ECHO DBSIZE KEYS FLUSHDB FLUSHALL INFO COMMAND SELECT LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE CONFIG MODULE WAITAOF)

  # Compile-time dispatch map: command name -> handler tag (O(1) lookup)
  @cmd_dispatch_map (
    pairs =
      Enum.map(@string_cmds, &{&1, :strings}) ++
      Enum.map(@expiry_cmds, &{&1, :expiry}) ++
      Enum.map(@generic_cmds, &{&1, :generic}) ++
      Enum.map(@bitmap_cmds, &{&1, :bitmap}) ++
      Enum.map(@hll_cmds, &{&1, :hll}) ++
      Enum.map(@hash_cmds, &{&1, :hash}) ++
      Enum.map(@list_cmds, &{&1, :list}) ++
      Enum.map(@set_cmds, &{&1, :set}) ++
      Enum.map(@zset_cmds, &{&1, :zset}) ++
      Enum.map(@geo_cmds, &{&1, :geo}) ++
      Enum.map(@stream_cmds, &{&1, :stream}) ++
      Enum.map(@json_cmds, &{&1, :json}) ++
      Enum.map(@native_cmds, &{&1, :native}) ++
      Enum.map(@bloom_cmds, &{&1, :bloom}) ++
      Enum.map(@cuckoo_cmds, &{&1, :cuckoo}) ++
      Enum.map(@cms_cmds, &{&1, :cms}) ++
      Enum.map(@topk_cmds, &{&1, :topk}) ++
      Enum.map(@tdigest_cmds, &{&1, :tdigest}) ++
      Enum.map(@vector_cmds, &{&1, :vector}) ++
      Enum.map(@pubsub_cmds, &{&1, :pubsub}) ++
      Enum.map(@server_cmds, &{&1, :server}) ++
      [
        {"RATELIMIT.ADD", :ratelimit},
        {"CLUSTER.HEALTH", :cluster},
        {"CLUSTER.STATS", :cluster},
        {"CLUSTER.KEYSLOT", :cluster},
        {"CLUSTER.SLOTS", :cluster},
        {"FERRICSTORE.HOTNESS", :cluster},
        {"FERRICSTORE.CONFIG", :ferricstore_config},
        {"FERRICSTORE.METRICS", :ferricstore_metrics},
        {"FERRICSTORE.KEY_INFO", :ferricstore_key_info},
        {"MEMORY", :memory}
      ]
    Map.new(pairs)
  )

  @doc """
  Dispatches a Redis command to the appropriate handler module.

  The command name is normalised to uppercase before routing. Each handler
  receives `(cmd, args, store)` and returns plain Elixir terms.

  ## Parameters

    - `name` - Command name (any case)
    - `args` - List of string arguments
    - `store` - Injected store map

  ## Returns

  The return value from the matched handler, or `{:error, message}` for unknown commands.

  ## Examples

      iex> store = %{get: fn "k" -> "v" end}
      iex> Ferricstore.Commands.Dispatcher.dispatch("GET", ["k"], store)
      "v"
  """
  @spec dispatch(binary(), [binary()], map()) :: term()

  # Fast path for the most common read commands: skip monotonic_time + SlowLog
  # overhead (~100-200ns). These commands are always sub-microsecond for hot
  # keys and would never appear in the slow log. The fast path avoids two
  # System.monotonic_time calls + the SlowLog.maybe_log threshold check.
  def dispatch("GET", args, store), do: Strings.handle("GET", args, store)
  def dispatch("MGET", args, store), do: Strings.handle("MGET", args, store)
  def dispatch("EXISTS", args, store), do: Strings.handle("EXISTS", args, store)

  def dispatch(name, args, store) do
    # The command name is expected to be already uppercase from normalise_cmd
    # in the connection layer. For defensive compatibility with embedded/test
    # callers that may pass lowercase, fall back to String.upcase only when
    # the name is not found in the dispatch map (rare cold path).
    cmd =
      if Map.has_key?(@cmd_dispatch_map, name),
        do: name,
        else: String.upcase(name)

    start = System.monotonic_time(:microsecond)

    result =
      case Map.get(@cmd_dispatch_map, cmd) do
        :strings -> Strings.handle(cmd, args, store)
        :expiry -> Expiry.handle(cmd, args, store)
        :generic -> Generic.handle(cmd, upcase_subcommand(cmd, args), store)
        :bitmap -> Bitmap.handle(cmd, args, store)
        :hll -> HyperLogLog.handle(cmd, args, store)
        :hash -> Hash.handle(cmd, args, store)
        :list -> List.handle(cmd, args, store)
        :set -> Set.handle(cmd, args, store)
        :zset -> SortedSet.handle(cmd, args, store)
        :geo -> Geo.handle(cmd, args, store)
        :stream -> Stream.handle(cmd, args, store)
        :json -> Json.handle(cmd, args, store)
        :native -> Native.handle(cmd, args, store)
        :bloom -> Bloom.handle(cmd, args, store)
        :cuckoo -> Cuckoo.handle(cmd, args, store)
        :cms -> CMS.handle(cmd, args, store)
        :topk -> TopK.handle(cmd, args, store)
        :tdigest -> TDigest.handle(cmd, args, store)
        :vector -> Vector.handle(cmd, args, store)
        :pubsub -> PubSub.handle(cmd, args)
        :server -> Server.handle(cmd, upcase_subcommand(cmd, args), store)
        :ratelimit -> Native.handle("RATELIMIT.ADD", args, store)
        :cluster -> Cluster.handle(cmd, args, store)
        :ferricstore_config -> Namespace.handle(cmd, upcase_subcommand_ferricstore(args), store)
        :ferricstore_metrics -> Ferricstore.Metrics.handle(cmd, args)
        :ferricstore_key_info -> Native.handle("KEY_INFO", args, store)
        :memory ->
          case args do
            [subcmd | rest] -> Memory.handle(String.upcase(subcmd), rest, store)
            [] -> {:error, "ERR wrong number of arguments for 'memory' command"}
          end
        nil -> {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}
      end

    duration = System.monotonic_time(:microsecond) - start
    Ferricstore.SlowLog.maybe_log([cmd | args], duration)

    result
  end

  @doc """
  Dispatches a CLIENT subcommand with connection state.

  CLIENT commands may mutate per-connection state (e.g. SETNAME), so they
  return `{result, updated_conn_state}`.

  ## Parameters

    - `args` - List of string arguments (first element is the subcommand)
    - `conn_state` - Per-connection state map
    - `store` - Injected store map

  ## Returns

  `{result, updated_conn_state}` tuple.
  """
  @spec dispatch_client([binary()], map(), map()) :: {term(), map()}
  def dispatch_client(args, conn_state, store) do
    case args do
      [subcmd | rest] ->
        Client.handle(String.upcase(subcmd), rest, conn_state, store)

      [] ->
        {{:error, "ERR wrong number of arguments for 'client' command"}, conn_state}
    end
  end

  # Multi-word commands need uppercasing of the subcommand portion.
  defp upcase_subcommand(cmd, args) when cmd in ~w(COMMAND DEBUG OBJECT) do
    case args do
      [subcmd | rest] -> [String.upcase(subcmd) | rest]
      [] -> []
    end
  end

  defp upcase_subcommand(_cmd, args), do: args

  # FERRICSTORE.* commands: upcase the first arg (subcommand) only.
  defp upcase_subcommand_ferricstore([subcmd | rest]), do: [String.upcase(subcmd) | rest]
  defp upcase_subcommand_ferricstore([]), do: []

end
