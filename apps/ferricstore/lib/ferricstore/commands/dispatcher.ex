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

  alias Ferricstore.Commands.{Bitmap, Bloom, Cluster, Cuckoo, Expiry, Generic, Geo, Hash, HyperLogLog, Json, List, Memory, Namespace, Native, PubSub, Server, Set, SortedSet, Stream, Strings}
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
  @topk_cmds ~w(TOPK.RESERVE TOPK.ADD TOPK.INCRBY TOPK.QUERY TOPK.LIST TOPK.COUNT TOPK.INFO)
  @tdigest_cmds ~w(TDIGEST.CREATE TDIGEST.ADD TDIGEST.RESET TDIGEST.QUANTILE TDIGEST.CDF TDIGEST.TRIMMED_MEAN TDIGEST.MIN TDIGEST.MAX TDIGEST.INFO TDIGEST.RANK TDIGEST.REVRANK TDIGEST.BYRANK TDIGEST.BYREVRANK TDIGEST.MERGE)
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
      Enum.map(@pubsub_cmds, &{&1, :pubsub}) ++
      Enum.map(@server_cmds, &{&1, :server}) ++
      [
        {"RATELIMIT.ADD", :ratelimit},
        {"CLUSTER.HEALTH", :cluster},
        {"CLUSTER.STATS", :cluster},
        {"CLUSTER.KEYSLOT", :cluster},
        {"CLUSTER.SLOTS", :cluster},
        {"CLUSTER.STATUS", :cluster},
        {"CLUSTER.JOIN", :cluster},
        {"CLUSTER.LEAVE", :cluster},
        {"CLUSTER.FAILOVER", :cluster},
        {"CLUSTER.PROMOTE", :cluster},
        {"CLUSTER.DEMOTE", :cluster},
        {"CLUSTER.ROLE", :cluster},
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

  # Fast path: skip monotonic_time + SlowLog overhead (~250ns saved per call).
  # Read-only commands are sub-microsecond ETS lookups — never slow-log worthy.
  def dispatch("GET", args, store), do: Strings.handle("GET", args, store)
  def dispatch("MGET", args, store), do: Strings.handle("MGET", args, store)
  def dispatch("EXISTS", args, store), do: Strings.handle("EXISTS", args, store)
  def dispatch("HGET", args, store), do: Hash.handle("HGET", args, store)
  def dispatch("HMGET", args, store), do: Hash.handle("HMGET", args, store)
  def dispatch("HGETALL", args, store), do: Hash.handle("HGETALL", args, store)
  def dispatch("HEXISTS", args, store), do: Hash.handle("HEXISTS", args, store)
  def dispatch("HLEN", args, store), do: Hash.handle("HLEN", args, store)
  def dispatch("HKEYS", args, store), do: Hash.handle("HKEYS", args, store)
  def dispatch("HVALS", args, store), do: Hash.handle("HVALS", args, store)
  def dispatch("LRANGE", args, store), do: List.handle("LRANGE", args, store)
  def dispatch("LLEN", args, store), do: List.handle("LLEN", args, store)
  def dispatch("LINDEX", args, store), do: List.handle("LINDEX", args, store)
  def dispatch("SISMEMBER", args, store), do: Set.handle("SISMEMBER", args, store)
  def dispatch("SMISMEMBER", args, store), do: Set.handle("SMISMEMBER", args, store)
  def dispatch("SMEMBERS", args, store), do: Set.handle("SMEMBERS", args, store)
  def dispatch("SCARD", args, store), do: Set.handle("SCARD", args, store)
  def dispatch("ZSCORE", args, store), do: SortedSet.handle("ZSCORE", args, store)
  def dispatch("ZRANK", args, store), do: SortedSet.handle("ZRANK", args, store)
  def dispatch("ZRANGE", args, store), do: SortedSet.handle("ZRANGE", args, store)
  def dispatch("ZCARD", args, store), do: SortedSet.handle("ZCARD", args, store)
  def dispatch("STRLEN", args, store), do: Strings.handle("STRLEN", args, store)
  def dispatch("GETRANGE", args, store), do: Strings.handle("GETRANGE", args, store)
  def dispatch("TTL", args, store), do: Expiry.handle("TTL", args, store)
  def dispatch("PTTL", args, store), do: Expiry.handle("PTTL", args, store)
  def dispatch("TYPE", args, store), do: Generic.handle("TYPE", args, store)
  def dispatch("GETBIT", args, store), do: Bitmap.handle("GETBIT", args, store)
  def dispatch("BITCOUNT", args, store), do: Bitmap.handle("BITCOUNT", args, store)
  def dispatch("PFCOUNT", args, store), do: HyperLogLog.handle("PFCOUNT", args, store)
  def dispatch("JSON.GET", args, store), do: Json.handle("JSON.GET", args, store)
  def dispatch("JSON.TYPE", args, store), do: Json.handle("JSON.TYPE", args, store)
  def dispatch("JSON.STRLEN", args, store), do: Json.handle("JSON.STRLEN", args, store)
  def dispatch("XLEN", args, store), do: Stream.handle("XLEN", args, store)
  def dispatch("XRANGE", args, store), do: Stream.handle("XRANGE", args, store)
  def dispatch("GEODIST", args, store), do: Geo.handle("GEODIST", args, store)
  def dispatch("GEOPOS", args, store), do: Geo.handle("GEOPOS", args, store)
  def dispatch("GEOHASH", args, store), do: Geo.handle("GEOHASH", args, store)

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

    result = dispatch_to_handler(Map.get(@cmd_dispatch_map, cmd), cmd, args, store)

    duration = System.monotonic_time(:microsecond) - start
    Ferricstore.SlowLog.maybe_log([cmd | args], duration)

    result
  end

  # CLIENT commands handled by FerricstoreServer.Commands.Client

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

  defp dispatch_to_handler(:strings, cmd, args, store), do: Strings.handle(cmd, args, store)
  defp dispatch_to_handler(:expiry, cmd, args, store), do: Expiry.handle(cmd, args, store)
  defp dispatch_to_handler(:generic, cmd, args, store), do: Generic.handle(cmd, upcase_subcommand(cmd, args), store)
  defp dispatch_to_handler(:bitmap, cmd, args, store), do: Bitmap.handle(cmd, args, store)
  defp dispatch_to_handler(:hll, cmd, args, store), do: HyperLogLog.handle(cmd, args, store)
  defp dispatch_to_handler(:hash, cmd, args, store), do: Hash.handle(cmd, args, store)
  defp dispatch_to_handler(:list, cmd, args, store), do: List.handle(cmd, args, store)
  defp dispatch_to_handler(:set, cmd, args, store), do: Set.handle(cmd, args, store)
  defp dispatch_to_handler(:zset, cmd, args, store), do: SortedSet.handle(cmd, args, store)
  defp dispatch_to_handler(:geo, cmd, args, store), do: Geo.handle(cmd, args, store)
  defp dispatch_to_handler(:stream, cmd, args, store), do: Stream.handle(cmd, args, store)
  defp dispatch_to_handler(:json, cmd, args, store), do: Json.handle(cmd, args, store)
  defp dispatch_to_handler(:native, cmd, args, store), do: Native.handle(cmd, args, store)
  defp dispatch_to_handler(:bloom, cmd, args, store), do: Bloom.handle(cmd, args, store)
  defp dispatch_to_handler(:cuckoo, cmd, args, store), do: Cuckoo.handle(cmd, args, store)
  defp dispatch_to_handler(:cms, cmd, args, store), do: CMS.handle(cmd, args, store)
  defp dispatch_to_handler(:topk, cmd, args, store), do: TopK.handle(cmd, args, store)
  defp dispatch_to_handler(:tdigest, cmd, args, store), do: TDigest.handle(cmd, args, store)
  defp dispatch_to_handler(:pubsub, cmd, args, _store), do: PubSub.handle(cmd, args)
  defp dispatch_to_handler(:server, cmd, args, store), do: Server.handle(cmd, upcase_subcommand(cmd, args), store)
  defp dispatch_to_handler(:ratelimit, _cmd, args, store), do: Native.handle("RATELIMIT.ADD", args, store)
  defp dispatch_to_handler(:cluster, cmd, args, store), do: Cluster.handle(cmd, args, store)
  defp dispatch_to_handler(:ferricstore_config, cmd, args, store), do: Namespace.handle(cmd, upcase_subcommand_ferricstore(args), store)
  defp dispatch_to_handler(:ferricstore_metrics, cmd, args, _store), do: Ferricstore.Metrics.handle(cmd, args)
  defp dispatch_to_handler(:ferricstore_key_info, _cmd, args, store), do: Native.handle("KEY_INFO", args, store)
  defp dispatch_to_handler(:memory, _cmd, [subcmd | rest], store), do: Memory.handle(String.upcase(subcmd), rest, store)
  defp dispatch_to_handler(:memory, _cmd, [], _store), do: {:error, "ERR wrong number of arguments for 'memory' command"}
  defp dispatch_to_handler(nil, cmd, _args, _store), do: {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}
end
