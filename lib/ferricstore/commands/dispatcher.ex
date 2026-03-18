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

  alias Ferricstore.Commands.{Bitmap, Bloom, Client, Cluster, Cuckoo, Expiry, Generic, Geo, Hash, HyperLogLog, Json, List, Memory, Native, PubSub, Server, Set, SortedSet, Stream, Strings}
  alias Ferricstore.Commands.CMS
  alias Ferricstore.Commands.TopK

  @string_cmds ~w(GET SET DEL EXISTS MGET MSET INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETSET GETDEL GETEX SETNX SETEX PSETEX GETRANGE SETRANGE MSETNX)
  @expiry_cmds ~w(EXPIRE EXPIREAT PEXPIRE PEXPIREAT TTL PTTL PERSIST)
  @generic_cmds ~w(TYPE UNLINK RENAME RENAMENX COPY RANDOMKEY SCAN EXPIRETIME PEXPIRETIME OBJECT WAIT)
  @bitmap_cmds ~w(SETBIT GETBIT BITCOUNT BITPOS BITOP)
  @hll_cmds ~w(PFADD PFCOUNT PFMERGE)
  @hash_cmds ~w(HSET HGET HDEL HMGET HGETALL HEXISTS HKEYS HVALS HLEN HINCRBY HINCRBYFLOAT HSETNX HSTRLEN HRANDFIELD HSCAN)
  @list_cmds ~w(LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX LSET LREM LTRIM LPOS LINSERT LMOVE LPUSHX RPUSHX)
  @set_cmds ~w(SADD SREM SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SPOP SDIFF SINTER SUNION SDIFFSTORE SINTERSTORE SUNIONSTORE SINTERCARD SMOVE SSCAN)
  @zset_cmds ~w(ZADD ZREM ZSCORE ZRANK ZREVRANK ZRANGE ZCARD ZINCRBY ZCOUNT ZPOPMIN ZPOPMAX ZRANDMEMBER ZSCAN ZMSCORE)
  @geo_cmds ~w(GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE)
  @stream_cmds ~w(XADD XLEN XRANGE XREVRANGE XREAD XTRIM XDEL XINFO XGROUP XREADGROUP XACK)
  @json_cmds ~w(JSON.SET JSON.GET JSON.DEL JSON.NUMINCRBY JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRAPPEND JSON.ARRLEN JSON.TOGGLE JSON.CLEAR JSON.MGET)
  @native_cmds ~w(CAS LOCK UNLOCK EXTEND FETCH_OR_COMPUTE FETCH_OR_COMPUTE_RESULT FETCH_OR_COMPUTE_ERROR)
  @bloom_cmds ~w(BF.RESERVE BF.ADD BF.MADD BF.EXISTS BF.MEXISTS BF.CARD BF.INFO)
  @cuckoo_cmds ~w(CF.RESERVE CF.ADD CF.ADDNX CF.DEL CF.EXISTS CF.MEXISTS CF.COUNT CF.INFO)
  @cms_cmds ~w(CMS.INITBYDIM CMS.INITBYPROB CMS.INCRBY CMS.QUERY CMS.MERGE CMS.INFO)
  @topk_cmds ~w(TOPK.RESERVE TOPK.ADD TOPK.INCRBY TOPK.QUERY TOPK.LIST TOPK.INFO)
  @pubsub_cmds ~w(PUBLISH PUBSUB)
  @server_cmds ~w(PING ECHO DBSIZE KEYS FLUSHDB FLUSHALL INFO COMMAND SELECT LOLWUT DEBUG SLOWLOG SAVE BGSAVE LASTSAVE CONFIG MODULE WAITAOF)

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
  def dispatch(name, args, store) do
    cmd = String.upcase(name)
    start = System.monotonic_time(:microsecond)

    result =
      cond do
        cmd in @string_cmds -> Strings.handle(cmd, args, store)
        cmd in @expiry_cmds -> Expiry.handle(cmd, args, store)
        cmd in @generic_cmds -> Generic.handle(cmd, upcase_subcommand(cmd, args), store)
        cmd in @bitmap_cmds -> Bitmap.handle(cmd, args, store)
        cmd in @hll_cmds -> HyperLogLog.handle(cmd, args, store)
        cmd in @hash_cmds -> Hash.handle(cmd, args, store)
        cmd in @list_cmds -> List.handle(cmd, args, store)
        cmd in @set_cmds -> Set.handle(cmd, args, store)
        cmd in @zset_cmds -> SortedSet.handle(cmd, args, store)
        cmd in @geo_cmds -> Geo.handle(cmd, args, store)
        cmd in @stream_cmds -> Stream.handle(cmd, args, store)
        cmd in @json_cmds -> Json.handle(cmd, args, store)
        cmd in @bloom_cmds -> Bloom.handle(cmd, args, store)
        cmd in @cuckoo_cmds -> Cuckoo.handle(cmd, args, store)
        cmd in @cms_cmds -> CMS.handle(cmd, args, store)
        cmd in @topk_cmds -> TopK.handle(cmd, args, store)
        cmd in @native_cmds -> Native.handle(cmd, args, store)
        cmd == "RATELIMIT.ADD" -> Native.handle("RATELIMIT.ADD", args, store)
        cmd == "CLUSTER.HEALTH" -> Cluster.handle(cmd, args, store)
        cmd == "CLUSTER.STATS" -> Cluster.handle(cmd, args, store)
        cmd == "FERRICSTORE.HOTNESS" -> Cluster.handle(cmd, args, store)
        cmd in @pubsub_cmds -> PubSub.handle(cmd, args)
        cmd == "MEMORY" ->
          case args do
            [subcmd | rest] -> Memory.handle(String.upcase(subcmd), rest, store)
            [] -> {:error, "ERR wrong number of arguments for 'memory' command"}
          end
        cmd in @server_cmds -> Server.handle(cmd, upcase_subcommand(cmd, args), store)
        true -> {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}
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
end
