defmodule FerricStore do
  @moduledoc """
  Direct Elixir API for FerricStore in embedded mode.

  When FerricStore runs embedded inside a host Elixir application, this module
  provides direct function calls that bypass TCP, RESP3 parsing, and ACL checks.
  These are thin wrappers around the same shard routing and group-commit pipeline
  that standalone mode uses, at ETS lookup latency (~1-5us for hot reads).

  ## Usage

      FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
      {:ok, "alice"} = FerricStore.get("user:42:name")
      :ok = FerricStore.del("user:42:name")

  ## Sandbox support

  All API functions are sandbox-aware. When `FerricStore.Sandbox.checkout/1` has
  been called in the current process, a namespace prefix is transparently prepended
  to all keys. Production code runs with `Process.get(:ferricstore_sandbox) == nil`
  -- zero overhead, zero behaviour change. See `FerricStore.Sandbox` for details.

  ## Named caches

  Pass the `:cache` option to direct operations to a named cache instance:

      FerricStore.set("session:abc", data, cache: :sessions)
      FerricStore.get("session:abc", cache: :sessions)

  When `:cache` is not specified, the default cache is used.
  """

  alias Ferricstore.Store.Router

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @type key :: binary()
  @type value :: binary()
  @type set_opts :: [ttl: non_neg_integer(), cache: atom()]
  @type get_opts :: [cache: atom()]
  @type cas_opts :: [ttl: non_neg_integer()]
  @type fetch_or_compute_opts :: [ttl: pos_integer(), hint: binary()]
  @type zrange_opts :: [withscores: boolean()]

  # ---------------------------------------------------------------------------
  # Strings
  # ---------------------------------------------------------------------------

  @doc """
  Sets `key` to `value` with optional TTL.

  ## Options

    * `:ttl` - Time-to-live in milliseconds. When omitted or `0`, the key
      never expires.
    * `:cache` - Named cache instance (default: the default cache).

  ## Returns

    * `:ok` on success.

  ## Examples

      :ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
      :ok = FerricStore.set("counter", "0")

  """
  @spec set(key(), value(), set_opts()) :: :ok
  def set(key, value, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, 0)

    expire_at_ms =
      if ttl > 0 do
        System.os_time(:millisecond) + ttl
      else
        0
      end

    key
    |> sandbox_key()
    |> Router.put(value, expire_at_ms)
  end

  @doc """
  Gets the value for `key`.

  Returns `{:ok, value}` if the key exists and is not expired, or `{:ok, nil}`
  if the key does not exist or has expired.

  ## Options

    * `:cache` - Named cache instance (default: the default cache).

  ## Examples

      {:ok, "alice"} = FerricStore.get("user:42:name")
      {:ok, nil} = FerricStore.get("nonexistent")

  """
  @spec get(key(), get_opts()) :: {:ok, value() | nil}
  def get(key, opts \\ []) do
    _cache = Keyword.get(opts, :cache)

    result =
      key
      |> sandbox_key()
      |> Router.get()

    {:ok, result}
  end

  @doc """
  Deletes `key`.

  Returns `:ok` whether or not the key existed.

  ## Examples

      :ok = FerricStore.del("user:42:name")

  """
  @spec del(key()) :: :ok
  def del(key) do
    key
    |> sandbox_key()
    |> Router.delete()
  end

  @doc """
  Increments the integer value stored at `key` by 1.

  If the key does not exist, it is initialized to `0` before incrementing,
  resulting in a value of `1`. If the key holds a value that cannot be parsed
  as an integer, returns `{:error, reason}`.

  ## Returns

    * `{:ok, new_value}` where `new_value` is the integer value after increment.
    * `{:error, reason}` if the stored value is not a valid integer.

  ## Examples

      {:ok, 1} = FerricStore.incr("counter")
      {:ok, 2} = FerricStore.incr("counter")

  """
  @spec incr(key()) :: {:ok, integer()} | {:error, binary()}
  def incr(key) do
    incr_by(key, 1)
  end

  @doc """
  Increments the integer value stored at `key` by `amount`.

  If the key does not exist, it is initialized to `0` before incrementing.
  If the key holds a value that cannot be parsed as an integer, returns
  `{:error, reason}`.

  ## Returns

    * `{:ok, new_value}` where `new_value` is the integer value after increment.
    * `{:error, reason}` if the stored value is not a valid integer.

  ## Examples

      {:ok, 10} = FerricStore.incr_by("counter", 10)
      {:ok, 15} = FerricStore.incr_by("counter", 5)

  """
  @spec incr_by(key(), integer()) :: {:ok, integer()} | {:error, binary()}
  def incr_by(key, amount) when is_integer(amount) do
    resolved_key = sandbox_key(key)

    current = Router.get(resolved_key)

    case parse_int_value(current) do
      {:ok, int_val} ->
        new_val = int_val + amount
        Router.put(resolved_key, Integer.to_string(new_val), 0)
        {:ok, new_val}

      {:error, _} = err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Hash
  # ---------------------------------------------------------------------------

  @doc """
  Sets one or more fields in the hash stored at `key`.

  `fields` is a map of `%{field_name => value}`. Field names and values are
  stored as binaries. If the hash does not exist, a new one is created.

  Hashes are stored as a single serialized value at the key. This is a
  convenience layer -- under the hood the entire hash is read, merged, and
  written back atomically per shard.

  ## Examples

      :ok = FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})

  """
  @spec hset(key(), %{binary() => binary()}) :: :ok
  def hset(key, fields) when is_map(fields) do
    resolved_key = sandbox_key(key)

    current_hash =
      case Router.get(resolved_key) do
        nil -> %{}
        bin -> decode_hash(bin)
      end

    string_fields = Map.new(fields, fn {k, v} -> {to_string(k), to_string(v)} end)
    merged = Map.merge(current_hash, string_fields)
    Router.put(resolved_key, encode_hash(merged), 0)
  end

  @doc """
  Gets the value of `field` from the hash stored at `key`.

  Returns `{:ok, value}` if the field exists, or `{:ok, nil}` if the field
  or the hash does not exist.

  ## Examples

      {:ok, "alice"} = FerricStore.hget("user:42", "name")
      {:ok, nil} = FerricStore.hget("user:42", "nonexistent_field")

  """
  @spec hget(key(), binary()) :: {:ok, binary() | nil}
  def hget(key, field) do
    resolved_key = sandbox_key(key)

    case Router.get(resolved_key) do
      nil ->
        {:ok, nil}

      bin ->
        hash = decode_hash(bin)
        {:ok, Map.get(hash, to_string(field))}
    end
  end

  @doc """
  Gets all fields and values from the hash stored at `key`.

  Returns `{:ok, map}` where `map` is a `%{field => value}` map. If the key
  does not exist, returns `{:ok, %{}}`.

  ## Examples

      {:ok, %{"name" => "alice", "age" => "30"}} = FerricStore.hgetall("user:42")

  """
  @spec hgetall(key()) :: {:ok, %{binary() => binary()}}
  def hgetall(key) do
    resolved_key = sandbox_key(key)

    case Router.get(resolved_key) do
      nil -> {:ok, %{}}
      bin -> {:ok, decode_hash(bin)}
    end
  end

  # ---------------------------------------------------------------------------
  # Lists
  # ---------------------------------------------------------------------------

  @doc """
  Pushes one or more elements to the left (head) of the list stored at `key`.

  If the key does not exist, a new list is created. Elements are inserted
  left-to-right, so the last element in the list ends up as the leftmost
  element (matching Redis LPUSH semantics).

  ## Parameters

    * `key` - the list key
    * `elements` - a list of binary elements to push

  ## Returns

    * `{:ok, length}` where `length` is the list length after the push.

  ## Examples

      {:ok, 3} = FerricStore.lpush("mylist", ["a", "b", "c"])

  """
  @spec lpush(key(), [binary()]) :: {:ok, non_neg_integer()}
  def lpush(key, elements) when is_list(elements) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:lpush, elements})

    {:ok, result}
  end

  @doc """
  Pushes one or more elements to the right (tail) of the list stored at `key`.

  If the key does not exist, a new list is created.

  ## Parameters

    * `key` - the list key
    * `elements` - a list of binary elements to push

  ## Returns

    * `{:ok, length}` where `length` is the list length after the push.

  ## Examples

      {:ok, 3} = FerricStore.rpush("mylist", ["a", "b", "c"])

  """
  @spec rpush(key(), [binary()]) :: {:ok, non_neg_integer()}
  def rpush(key, elements) when is_list(elements) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:rpush, elements})

    {:ok, result}
  end

  @doc """
  Pops one or more elements from the left (head) of the list stored at `key`.

  When `count` is 1 (the default), returns a single element. When `count` is
  greater than 1, returns a list of elements. Returns `{:ok, nil}` if the key
  does not exist or the list is empty.

  ## Parameters

    * `key` - the list key
    * `count` - number of elements to pop (default: 1)

  ## Returns

    * `{:ok, element}` when count is 1
    * `{:ok, [element, ...]}` when count > 1
    * `{:ok, nil}` if the key does not exist

  ## Examples

      {:ok, "a"} = FerricStore.lpop("mylist")
      {:ok, ["a", "b"]} = FerricStore.lpop("mylist", 2)

  """
  @spec lpop(key(), pos_integer()) :: {:ok, binary() | [binary()] | nil}
  def lpop(key, count \\ 1) when is_integer(count) and count >= 1 do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:lpop, count})

    {:ok, result}
  end

  @doc """
  Pops one or more elements from the right (tail) of the list stored at `key`.

  When `count` is 1 (the default), returns a single element. When `count` is
  greater than 1, returns a list of elements. Returns `{:ok, nil}` if the key
  does not exist or the list is empty.

  ## Parameters

    * `key` - the list key
    * `count` - number of elements to pop (default: 1)

  ## Returns

    * `{:ok, element}` when count is 1
    * `{:ok, [element, ...]}` when count > 1
    * `{:ok, nil}` if the key does not exist

  ## Examples

      {:ok, "c"} = FerricStore.rpop("mylist")
      {:ok, ["c", "b"]} = FerricStore.rpop("mylist", 2)

  """
  @spec rpop(key(), pos_integer()) :: {:ok, binary() | [binary()] | nil}
  def rpop(key, count \\ 1) when is_integer(count) and count >= 1 do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:rpop, count})

    {:ok, result}
  end

  @doc """
  Returns elements from the list stored at `key` within the range `start..stop`.

  Both `start` and `stop` are zero-based, inclusive indices. Negative indices
  count from the end of the list (-1 is the last element).

  ## Parameters

    * `key` - the list key
    * `start` - start index (inclusive)
    * `stop` - stop index (inclusive)

  ## Returns

    * `{:ok, [elements]}` - the elements in the range, or an empty list

  ## Examples

      {:ok, ["a", "b", "c"]} = FerricStore.lrange("mylist", 0, -1)
      {:ok, ["b"]} = FerricStore.lrange("mylist", 1, 1)

  """
  @spec lrange(key(), integer(), integer()) :: {:ok, [binary()]}
  def lrange(key, start, stop) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:lrange, start, stop})

    {:ok, result}
  end

  @doc """
  Returns the length of the list stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      {:ok, 3} = FerricStore.llen("mylist")
      {:ok, 0} = FerricStore.llen("nonexistent")

  """
  @spec llen(key()) :: {:ok, non_neg_integer()}
  def llen(key) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op(:llen)

    {:ok, result}
  end

  # ---------------------------------------------------------------------------
  # Sets
  # ---------------------------------------------------------------------------

  @doc """
  Adds one or more members to the set stored at `key`.

  If the key does not exist, a new set is created. Members that already exist
  in the set are ignored.

  ## Parameters

    * `key` - the set key
    * `members` - a list of binary members to add

  ## Returns

    * `{:ok, added_count}` - the number of members that were actually added
      (not counting duplicates).

  ## Examples

      {:ok, 3} = FerricStore.sadd("myset", ["a", "b", "c"])
      {:ok, 1} = FerricStore.sadd("myset", ["c", "d"])

  """
  @spec sadd(key(), [binary()]) :: {:ok, non_neg_integer()}
  def sadd(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Set.handle("SADD", [resolved_key | members], store)
    {:ok, result}
  end

  @doc """
  Removes one or more members from the set stored at `key`.

  Members that do not exist in the set are ignored.

  ## Parameters

    * `key` - the set key
    * `members` - a list of binary members to remove

  ## Returns

    * `{:ok, removed_count}` - the number of members that were actually removed.

  ## Examples

      {:ok, 1} = FerricStore.srem("myset", ["a"])

  """
  @spec srem(key(), [binary()]) :: {:ok, non_neg_integer()}
  def srem(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Set.handle("SREM", [resolved_key | members], store)
    {:ok, result}
  end

  @doc """
  Returns all members of the set stored at `key`.

  Returns `{:ok, []}` if the key does not exist.

  ## Examples

      {:ok, members} = FerricStore.smembers("myset")

  """
  @spec smembers(key()) :: {:ok, [binary()]}
  def smembers(key) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Set.handle("SMEMBERS", [resolved_key], store)
    {:ok, result}
  end

  @doc """
  Checks whether `member` is a member of the set stored at `key`.

  ## Returns

    * `true` if the member exists in the set
    * `false` otherwise

  ## Examples

      true = FerricStore.sismember("myset", "a")
      false = FerricStore.sismember("myset", "z")

  """
  @spec sismember(key(), binary()) :: boolean()
  def sismember(key, member) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Set.handle("SISMEMBER", [resolved_key, member], store)
    result == 1
  end

  @doc """
  Returns the number of members in the set stored at `key` (the set cardinality).

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      {:ok, 3} = FerricStore.scard("myset")
      {:ok, 0} = FerricStore.scard("nonexistent")

  """
  @spec scard(key()) :: {:ok, non_neg_integer()}
  def scard(key) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Set.handle("SCARD", [resolved_key], store)
    {:ok, result}
  end

  # ---------------------------------------------------------------------------
  # Sorted Sets
  # ---------------------------------------------------------------------------

  @doc """
  Adds members with scores to the sorted set stored at `key`.

  `score_member_pairs` is a list of `{score, member}` tuples where `score` is
  a number and `member` is a binary string. If a member already exists, its
  score is updated.

  ## Parameters

    * `key` - the sorted set key
    * `score_member_pairs` - list of `{score, member}` tuples

  ## Returns

    * `{:ok, added_count}` - the number of new members added (not counting
      score updates on existing members).

  ## Examples

      {:ok, 2} = FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])

  """
  @spec zadd(key(), [{number(), binary()}]) :: {:ok, non_neg_integer()}
  def zadd(key, score_member_pairs) when is_list(score_member_pairs) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    # Build the ZADD args: key score1 member1 score2 member2 ...
    args =
      Enum.flat_map(score_member_pairs, fn {score, member} ->
        [to_string(score * 1.0), member]
      end)

    result = Ferricstore.Commands.SortedSet.handle("ZADD", [resolved_key | args], store)
    {:ok, result}
  end

  @doc """
  Returns members in the sorted set stored at `key` within the rank range
  `start..stop` (zero-based, inclusive). Negative indices count from the end.

  ## Options

    * `:withscores` - When `true`, returns `{member, score}` tuples instead
      of bare member strings. Defaults to `false`.

  ## Returns

    * `{:ok, members}` - list of member strings (or `{member, score}` tuples
      when `:withscores` is `true`). Empty list if key does not exist.

  ## Examples

      {:ok, ["alice", "bob"]} = FerricStore.zrange("leaderboard", 0, -1)
      {:ok, [{"alice", 100.0}, {"bob", 200.0}]} = FerricStore.zrange("leaderboard", 0, -1, withscores: true)

  """
  @spec zrange(key(), integer(), integer(), zrange_opts()) :: {:ok, [binary() | {binary(), float()}]}
  def zrange(key, start, stop, opts \\ []) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    with_scores = Keyword.get(opts, :withscores, false)

    args = [resolved_key, to_string(start), to_string(stop)]
    args = if with_scores, do: args ++ ["WITHSCORES"], else: args

    result = Ferricstore.Commands.SortedSet.handle("ZRANGE", args, store)

    if with_scores and is_list(result) and result != [] do
      # ZRANGE WITHSCORES returns flat list: [member, score_str, member, score_str, ...]
      pairs =
        result
        |> Enum.chunk_every(2)
        |> Enum.map(fn [member, score_str] ->
          {score, ""} = Float.parse(score_str)
          {member, score}
        end)

      {:ok, pairs}
    else
      {:ok, result}
    end
  end

  @doc """
  Returns the score of `member` in the sorted set stored at `key`.

  Returns `{:ok, score}` if the member exists, or `{:ok, nil}` if the member
  or key does not exist.

  ## Examples

      {:ok, 100.0} = FerricStore.zscore("leaderboard", "alice")
      {:ok, nil} = FerricStore.zscore("leaderboard", "unknown")

  """
  @spec zscore(key(), binary()) :: {:ok, float() | nil}
  def zscore(key, member) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.SortedSet.handle("ZSCORE", [resolved_key, member], store) do
      nil ->
        {:ok, nil}

      score_str when is_binary(score_str) ->
        {score, ""} = Float.parse(score_str)
        {:ok, score}
    end
  end

  @doc """
  Returns the number of members in the sorted set stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      {:ok, 2} = FerricStore.zcard("leaderboard")
      {:ok, 0} = FerricStore.zcard("nonexistent")

  """
  @spec zcard(key()) :: {:ok, non_neg_integer()}
  def zcard(key) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZCARD", [resolved_key], store)
    {:ok, result}
  end

  @doc """
  Removes one or more members from the sorted set stored at `key`.

  Members that do not exist are ignored.

  ## Parameters

    * `key` - the sorted set key
    * `members` - a list of binary members to remove

  ## Returns

    * `{:ok, removed_count}` - the number of members actually removed.

  ## Examples

      {:ok, 1} = FerricStore.zrem("leaderboard", ["alice"])

  """
  @spec zrem(key(), [binary()]) :: {:ok, non_neg_integer()}
  def zrem(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZREM", [resolved_key | members], store)
    {:ok, result}
  end

  # ---------------------------------------------------------------------------
  # Native Commands
  # ---------------------------------------------------------------------------

  @doc """
  Performs an atomic compare-and-swap on `key`.

  If the current value of `key` equals `expected`, it is replaced with
  `new_value`. Optionally sets a TTL on the key.

  ## Options

    * `:ttl` - Time-to-live in milliseconds for the new value. When omitted,
      the existing TTL is preserved.

  ## Returns

    * `{:ok, true}` if the swap was performed
    * `{:ok, false}` if the current value did not match `expected`
    * `{:ok, nil}` if the key does not exist

  ## Examples

      FerricStore.set("counter", "1")
      {:ok, true} = FerricStore.cas("counter", "1", "2")
      {:ok, false} = FerricStore.cas("counter", "1", "3")

  """
  @spec cas(key(), binary(), binary(), cas_opts()) :: {:ok, true | false | nil}
  def cas(key, expected, new_value, opts \\ []) do
    resolved_key = sandbox_key(key)
    ttl_ms = Keyword.get(opts, :ttl)

    case Router.cas(resolved_key, expected, new_value, ttl_ms) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      nil -> {:ok, nil}
    end
  end

  @doc """
  Cache-aside with stampede protection.

  Checks whether `key` has a cached value. If it does, returns
  `{:ok, {:hit, value}}`. If not, returns `{:ok, {:compute, hint}}` to
  indicate that the caller should compute the value and store it via
  `fetch_or_compute_result/3`.

  Only one caller at a time receives `{:compute, hint}` for a given key --
  other concurrent callers block until the value is available (stampede
  protection).

  ## Options

    * `:ttl` (required) - TTL in milliseconds for the cached value
    * `:hint` - An opaque string passed back in `{:compute, hint}`. Defaults
      to `""`.

  ## Returns

    * `{:ok, {:hit, value}}` if the value is cached
    * `{:ok, {:compute, hint}}` if the caller should compute the value
    * `{:error, reason}` on failure

  ## Examples

      case FerricStore.fetch_or_compute("expensive:key", ttl: 60_000) do
        {:ok, {:hit, value}} -> value
        {:ok, {:compute, _hint}} ->
          value = expensive_computation()
          FerricStore.fetch_or_compute_result("expensive:key", value, ttl: 60_000)
          value
      end

  """
  @spec fetch_or_compute(key(), fetch_or_compute_opts()) ::
          {:ok, {:hit, binary()} | {:compute, binary()}} | {:error, binary()}
  def fetch_or_compute(key, opts) do
    resolved_key = sandbox_key(key)
    ttl_ms = Keyword.fetch!(opts, :ttl)
    hint = Keyword.get(opts, :hint, "")

    case Ferricstore.FetchOrCompute.fetch_or_compute(resolved_key, ttl_ms, hint) do
      {:hit, value} -> {:ok, {:hit, value}}
      {:ok, value} -> {:ok, {:hit, value}}
      {:compute, compute_hint} -> {:ok, {:compute, compute_hint}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stores the computed value for a `fetch_or_compute/2` cache miss.

  This function should be called after receiving `{:ok, {:compute, hint}}`
  from `fetch_or_compute/2`. It stores the value and unblocks any waiters.

  ## Options

    * `:ttl` (required) - TTL in milliseconds for the cached value.

  ## Returns

    * `:ok` on success.

  ## Examples

      FerricStore.fetch_or_compute_result("expensive:key", computed_value, ttl: 60_000)

  """
  @spec fetch_or_compute_result(key(), binary(), keyword()) :: :ok | {:error, binary()}
  def fetch_or_compute_result(key, value, opts) do
    resolved_key = sandbox_key(key)
    ttl_ms = Keyword.fetch!(opts, :ttl)
    Ferricstore.FetchOrCompute.fetch_or_compute_result(resolved_key, value, ttl_ms)
  end

  # ---------------------------------------------------------------------------
  # Generic Key Operations
  # ---------------------------------------------------------------------------

  @doc """
  Checks whether `key` exists and is not expired.

  ## Returns

    * `true` if the key exists
    * `false` otherwise

  ## Examples

      FerricStore.set("mykey", "value")
      true = FerricStore.exists("mykey")
      false = FerricStore.exists("nonexistent")

  """
  @spec exists(key()) :: boolean()
  def exists(key) do
    key
    |> sandbox_key()
    |> Router.exists?()
  end

  @doc """
  Returns all keys matching `pattern`.

  The pattern supports glob-style wildcards:

    * `*` matches any sequence of characters
    * `?` matches any single character

  ## Returns

    * `{:ok, [keys]}` - list of matching keys. Note that in sandbox mode,
      the sandbox prefix is stripped from returned keys.

  ## Examples

      {:ok, keys} = FerricStore.keys("user:*")
      {:ok, all_keys} = FerricStore.keys()

  """
  @spec keys(binary()) :: {:ok, [binary()]}
  def keys(pattern \\ "*") do
    namespace = Process.get(:ferricstore_sandbox)
    all_keys = Router.keys()

    filtered =
      if namespace do
        prefix_len = byte_size(namespace)

        all_keys
        |> Enum.filter(&String.starts_with?(&1, namespace))
        |> Enum.map(&binary_part(&1, prefix_len, byte_size(&1) - prefix_len))
      else
        all_keys
      end

    # Filter internal compound keys (S:, Z:, T:, H:, L: prefixes)
    filtered = Enum.reject(filtered, &Ferricstore.Store.CompoundKey.internal_key?/1)

    matched =
      if pattern == "*" do
        filtered
      else
        regex = glob_to_regex(pattern)
        Enum.filter(filtered, &Regex.match?(regex, &1))
      end

    {:ok, matched}
  end

  @doc """
  Returns the total number of keys in the store.

  In sandbox mode, only keys belonging to the current sandbox namespace
  are counted, and internal compound keys are excluded.

  ## Examples

      {:ok, count} = FerricStore.dbsize()

  """
  @spec dbsize() :: {:ok, non_neg_integer()}
  def dbsize do
    {:ok, matched_keys} = keys()
    {:ok, length(matched_keys)}
  end

  @doc """
  Deletes all keys from the store.

  In sandbox mode, only keys belonging to the current sandbox namespace
  are deleted.

  ## Returns

    * `:ok`

  ## Examples

      :ok = FerricStore.flushdb()

  """
  @spec flushdb() :: :ok
  def flushdb do
    namespace = Process.get(:ferricstore_sandbox)

    if namespace do
      # Only flush keys in this sandbox namespace
      FerricStore.Sandbox.checkin(namespace)
      # Re-set the namespace since checkin clears it
      Process.put(:ferricstore_sandbox, namespace)
    else
      all_keys = Router.keys()
      Enum.each(all_keys, &Router.delete/1)
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # TTL
  # ---------------------------------------------------------------------------

  @doc """
  Sets a TTL (in milliseconds) on an existing key.

  Returns `{:ok, true}` if the timeout was set, `{:ok, false}` if the key
  does not exist.

  ## Examples

      {:ok, true} = FerricStore.expire("user:42", :timer.minutes(30))

  """
  @spec expire(key(), non_neg_integer()) :: {:ok, boolean()}
  def expire(key, ttl_ms) when is_integer(ttl_ms) and ttl_ms > 0 do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil ->
        {:ok, false}

      {value, _old_exp} ->
        expire_at_ms = System.os_time(:millisecond) + ttl_ms
        Router.put(resolved_key, value, expire_at_ms)
        {:ok, true}
    end
  end

  @doc """
  Returns the remaining TTL in milliseconds for `key`.

  Returns `{:ok, milliseconds_remaining}` if a TTL is set, `{:ok, nil}` if the
  key has no expiry, or `{:ok, nil}` if the key does not exist.

  ## Examples

      {:ok, ms} = FerricStore.ttl("user:42")

  """
  @spec ttl(key()) :: {:ok, non_neg_integer() | nil}
  def ttl(key) do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil -> {:ok, nil}
      {_value, 0} -> {:ok, nil}
      {_value, exp} -> {:ok, max(0, exp - System.os_time(:millisecond))}
    end
  end

  # ---------------------------------------------------------------------------
  # Pipeline
  # ---------------------------------------------------------------------------

  @doc """
  Batches multiple commands into a single group-commit entry.

  The provided function receives a `FerricStore.Pipe` accumulator and should
  pipe commands into it. All commands are executed atomically on completion.

  ## Examples

      results = FerricStore.pipeline(fn pipe ->
        pipe
        |> FerricStore.Pipe.set("key1", "val1")
        |> FerricStore.Pipe.set("key2", "val2")
        |> FerricStore.Pipe.incr("counter")
      end)

  ## Returns

    * `{:ok, results}` - a list of results for each piped command, in order.

  """
  @spec pipeline((FerricStore.Pipe.t() -> FerricStore.Pipe.t())) :: {:ok, [term()]}
  def pipeline(fun) when is_function(fun, 1) do
    pipe = fun.(FerricStore.Pipe.new())
    results = FerricStore.Pipe.execute(pipe)
    {:ok, results}
  end

  # ---------------------------------------------------------------------------
  # Private — sandbox key prefixing
  # ---------------------------------------------------------------------------

  @doc false
  @spec sandbox_key(binary()) :: binary()
  def sandbox_key(key) do
    case Process.get(:ferricstore_sandbox) do
      nil -> key
      namespace -> namespace <> key
    end
  end

  # ---------------------------------------------------------------------------
  # Private — integer parsing for INCR
  # ---------------------------------------------------------------------------

  defp parse_int_value(nil), do: {:ok, 0}

  defp parse_int_value(bin) when is_binary(bin) do
    case Integer.parse(bin) do
      {int_val, ""} -> {:ok, int_val}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — hash encoding/decoding
  # ---------------------------------------------------------------------------

  # Encodes a map as a simple binary format:
  # Each field-value pair is stored as: <field_len:32><field><value_len:32><value>
  # This avoids a dependency on an external serialization library.

  @doc false
  @spec encode_hash(%{binary() => binary()}) :: binary()
  def encode_hash(map) when is_map(map) do
    map
    |> Enum.sort()
    |> Enum.map(fn {k, v} ->
      k_bin = to_string(k)
      v_bin = to_string(v)
      <<byte_size(k_bin)::32, k_bin::binary, byte_size(v_bin)::32, v_bin::binary>>
    end)
    |> IO.iodata_to_binary()
  end

  @doc false
  @spec decode_hash(binary()) :: %{binary() => binary()}
  def decode_hash(<<>>), do: %{}

  def decode_hash(bin) when is_binary(bin) do
    decode_hash_pairs(bin, %{})
  end

  defp decode_hash_pairs(<<>>, acc), do: acc

  defp decode_hash_pairs(
         <<k_len::32, k::binary-size(k_len), v_len::32, v::binary-size(v_len), rest::binary>>,
         acc
       ) do
    decode_hash_pairs(rest, Map.put(acc, k, v))
  end

  # ---------------------------------------------------------------------------
  # Private — compound key store builder for set/sorted-set operations
  # ---------------------------------------------------------------------------

  # Builds the store map expected by Commands.Set and Commands.SortedSet.
  # The store maps compound key operations to the correct shard GenServer
  # using the Redis key for routing (all sub-keys for one Redis key live
  # on the same shard).
  defp build_compound_store(resolved_key) do
    %{
      get: fn k -> Router.get(k) end,
      get_meta: fn k -> Router.get_meta(k) end,
      put: fn k, v, exp -> Router.put(k, v, exp) end,
      delete: fn k -> Router.delete(k) end,
      exists?: fn k -> Router.exists?(k) end,
      keys: &Router.keys/0,
      compound_get: fn _redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(resolved_key))
        GenServer.call(shard, {:get, compound_key})
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(Router.shard_for(resolved_key))
        GenServer.call(shard, {:put, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn _redis_key, compound_key ->
        shard = Router.shard_name(Router.shard_for(resolved_key))
        GenServer.call(shard, {:delete, compound_key})
      end,
      compound_scan: fn _redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(resolved_key))
        GenServer.call(shard, {:scan_prefix, prefix})
      end,
      compound_count: fn _redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(resolved_key))
        GenServer.call(shard, {:count_prefix, prefix})
      end,
      compound_delete_prefix: fn _redis_key, prefix ->
        shard = Router.shard_name(Router.shard_for(resolved_key))
        GenServer.call(shard, {:delete_prefix, prefix})
      end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — glob pattern matching for keys/1
  # ---------------------------------------------------------------------------

  defp glob_to_regex(pattern) do
    regex_str =
      pattern
      |> String.graphemes()
      |> Enum.map_join(&escape_glob_char/1)

    Regex.compile!("^#{regex_str}$")
  end

  defp escape_glob_char("*"), do: ".*"
  defp escape_glob_char("?"), do: "."
  defp escape_glob_char(char), do: Regex.escape(char)
end

defmodule FerricStore.Pipe do
  @moduledoc """
  Pipeline accumulator for batching multiple FerricStore commands.

  Used with `FerricStore.pipeline/1` to batch multiple operations into a single
  group-commit entry. Commands are accumulated in reverse order and executed
  sequentially when the pipeline completes.

  ## Usage

      FerricStore.pipeline(fn pipe ->
        pipe
        |> FerricStore.Pipe.set("key1", "val1")
        |> FerricStore.Pipe.set("key2", "val2")
        |> FerricStore.Pipe.incr("counter")
      end)

  """

  @type command ::
          {:set, binary(), binary(), keyword()}
          | {:get, binary()}
          | {:del, binary()}
          | {:incr, binary()}
          | {:incr_by, binary(), integer()}
          | {:hset, binary(), map()}
          | {:hget, binary(), binary()}
          | {:lpush, binary(), [binary()]}
          | {:rpush, binary(), [binary()]}
          | {:sadd, binary(), [binary()]}
          | {:zadd, binary(), [{number(), binary()}]}
          | {:expire, binary(), non_neg_integer()}

  @type t :: %__MODULE__{commands: [command()]}

  defstruct commands: []

  @doc "Creates a new empty pipeline."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds a SET command to the pipeline."
  @spec set(t(), binary(), binary(), keyword()) :: t()
  def set(%__MODULE__{} = pipe, key, value, opts \\ []) do
    %{pipe | commands: [{:set, key, value, opts} | pipe.commands]}
  end

  @doc "Adds a GET command to the pipeline."
  @spec get(t(), binary()) :: t()
  def get(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:get, key} | pipe.commands]}
  end

  @doc "Adds a DEL command to the pipeline."
  @spec del(t(), binary()) :: t()
  def del(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:del, key} | pipe.commands]}
  end

  @doc "Adds an INCR command to the pipeline."
  @spec incr(t(), binary()) :: t()
  def incr(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:incr, key} | pipe.commands]}
  end

  @doc "Adds an INCRBY command to the pipeline."
  @spec incr_by(t(), binary(), integer()) :: t()
  def incr_by(%__MODULE__{} = pipe, key, amount) do
    %{pipe | commands: [{:incr_by, key, amount} | pipe.commands]}
  end

  @doc "Adds an HSET command to the pipeline."
  @spec hset(t(), binary(), map()) :: t()
  def hset(%__MODULE__{} = pipe, key, fields) do
    %{pipe | commands: [{:hset, key, fields} | pipe.commands]}
  end

  @doc "Adds an HGET command to the pipeline."
  @spec hget(t(), binary(), binary()) :: t()
  def hget(%__MODULE__{} = pipe, key, field) do
    %{pipe | commands: [{:hget, key, field} | pipe.commands]}
  end

  @doc "Adds an LPUSH command to the pipeline."
  @spec lpush(t(), binary(), [binary()]) :: t()
  def lpush(%__MODULE__{} = pipe, key, elements) do
    %{pipe | commands: [{:lpush, key, elements} | pipe.commands]}
  end

  @doc "Adds an RPUSH command to the pipeline."
  @spec rpush(t(), binary(), [binary()]) :: t()
  def rpush(%__MODULE__{} = pipe, key, elements) do
    %{pipe | commands: [{:rpush, key, elements} | pipe.commands]}
  end

  @doc "Adds a SADD command to the pipeline."
  @spec sadd(t(), binary(), [binary()]) :: t()
  def sadd(%__MODULE__{} = pipe, key, members) do
    %{pipe | commands: [{:sadd, key, members} | pipe.commands]}
  end

  @doc "Adds a ZADD command to the pipeline."
  @spec zadd(t(), binary(), [{number(), binary()}]) :: t()
  def zadd(%__MODULE__{} = pipe, key, score_member_pairs) do
    %{pipe | commands: [{:zadd, key, score_member_pairs} | pipe.commands]}
  end

  @doc "Adds an EXPIRE command to the pipeline."
  @spec expire(t(), binary(), non_neg_integer()) :: t()
  def expire(%__MODULE__{} = pipe, key, ttl_ms) do
    %{pipe | commands: [{:expire, key, ttl_ms} | pipe.commands]}
  end

  @doc """
  Executes all accumulated pipeline commands in order and returns results.

  This is called internally by `FerricStore.pipeline/1`. Each command returns
  its result in the same format as the standalone API function.
  """
  @spec execute(t()) :: [term()]
  def execute(%__MODULE__{commands: commands}) do
    commands
    |> Enum.reverse()
    |> Enum.map(&execute_command/1)
  end

  defp execute_command({:set, key, value, opts}), do: FerricStore.set(key, value, opts)
  defp execute_command({:get, key}), do: FerricStore.get(key)
  defp execute_command({:del, key}), do: FerricStore.del(key)
  defp execute_command({:incr, key}), do: FerricStore.incr(key)
  defp execute_command({:incr_by, key, amount}), do: FerricStore.incr_by(key, amount)
  defp execute_command({:hset, key, fields}), do: FerricStore.hset(key, fields)
  defp execute_command({:hget, key, field}), do: FerricStore.hget(key, field)
  defp execute_command({:lpush, key, elements}), do: FerricStore.lpush(key, elements)
  defp execute_command({:rpush, key, elements}), do: FerricStore.rpush(key, elements)
  defp execute_command({:sadd, key, members}), do: FerricStore.sadd(key, members)
  defp execute_command({:zadd, key, pairs}), do: FerricStore.zadd(key, pairs)
  defp execute_command({:expire, key, ttl_ms}), do: FerricStore.expire(key, ttl_ms)
end
