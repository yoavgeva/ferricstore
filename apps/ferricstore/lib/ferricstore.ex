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
  @type set_opts :: [
          ttl: non_neg_integer(),
          exat: pos_integer(),
          pxat: pos_integer(),
          nx: boolean(),
          xx: boolean(),
          get: boolean(),
          keepttl: boolean(),
          cache: atom()
        ]
  @type get_opts :: [cache: atom()]
  @type cas_opts :: [ttl: non_neg_integer()]
  @type fetch_or_compute_opts :: [ttl: pos_integer(), hint: binary()]
  @type zrange_opts :: [withscores: boolean()]

  # ---------------------------------------------------------------------------
  # Strings
  # ---------------------------------------------------------------------------

  @doc """
  Sets `key` to `value` with optional TTL and condition flags.

  ## Options

    * `:ttl` - Time-to-live in milliseconds (relative). When omitted or `0`,
      the key never expires. Mutually exclusive with `:exat`, `:pxat`, `:keepttl`.
    * `:exat` - Absolute Unix timestamp in seconds at which the key expires.
      Mutually exclusive with `:ttl`, `:pxat`, `:keepttl`.
    * `:pxat` - Absolute Unix timestamp in milliseconds at which the key expires.
      Mutually exclusive with `:ttl`, `:exat`, `:keepttl`.
    * `:nx` - Only set the key if it does not already exist.
    * `:xx` - Only set the key if it already exists.
    * `:get` - Return the old value stored at the key before overwriting.
      When set, the return value changes to `{:ok, old_value}` (or `{:ok, nil}`
      if the key did not exist).
    * `:keepttl` - Retain the existing TTL associated with the key instead of
      clearing it. Mutually exclusive with `:ttl`, `:exat`, `:pxat`.
    * `:cache` - Named cache instance (default: the default cache).

  ## Returns

    * `:ok` on success (default).
    * `{:ok, old_value}` when `:get` is `true`.
    * `nil` when `:nx` or `:xx` condition prevented the write (and `:get` is
      not set).

  ## Examples

      :ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
      :ok = FerricStore.set("counter", "0")
      :ok = FerricStore.set("event:ts", "data", exat: 1711234567)
      :ok = FerricStore.set("event:ts", "data", pxat: 1711234567000)
      {:ok, "old"} = FerricStore.set("key", "new", get: true)
      :ok = FerricStore.set("key", "new_val", keepttl: true)

  """
  @spec set(key(), value(), set_opts()) :: :ok | {:ok, value() | nil} | nil | {:error, binary()}
  def set(key, value, opts \\ []) do
    max_value_size =
      Application.get_env(:ferricstore, :max_value_size, Ferricstore.Resp.Parser.default_max_value_size())

    if is_binary(value) and byte_size(value) > max_value_size do
      {:error, "ERR value too large (#{byte_size(value)} bytes, max #{max_value_size} bytes)"}
    else
      set_inner(key, value, opts)
    end
  end

  defp set_inner(key, value, opts) do
    resolved_key = sandbox_key(key)
    ttl = Keyword.get(opts, :ttl, 0)
    exat = Keyword.get(opts, :exat)
    pxat = Keyword.get(opts, :pxat)
    nx? = Keyword.get(opts, :nx, false)
    xx? = Keyword.get(opts, :xx, false)
    get? = Keyword.get(opts, :get, false)
    keepttl? = Keyword.get(opts, :keepttl, false)

    # Determine expire_at_ms from the expiry options (mutually exclusive)
    {expire_at_ms, from_keepttl?} =
      cond do
        keepttl? -> {0, true}
        exat != nil -> {exat * 1000, false}
        pxat != nil -> {pxat, false}
        ttl > 0 -> {System.os_time(:millisecond) + ttl, false}
        true -> {0, false}
      end

    # Read old metadata when GET or KEEPTTL is needed
    {old_value, effective_expire} =
      if get? or from_keepttl? do
        case Router.get_meta(resolved_key) do
          nil ->
            {nil, expire_at_ms}

          {old_val, old_exp} ->
            eff_exp = if from_keepttl?, do: old_exp, else: expire_at_ms
            {old_val, eff_exp}
        end
      else
        {nil, expire_at_ms}
      end

    # Condition check
    skip? =
      cond do
        nx? and Router.exists?(resolved_key) -> true
        xx? and not Router.exists?(resolved_key) -> true
        true -> false
      end

    if skip? do
      if get?, do: {:ok, old_value}, else: nil
    else
      Router.put(resolved_key, value, effective_expire)
      if get?, do: {:ok, old_value}, else: :ok
    end
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
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    Ferricstore.Commands.Strings.handle("DEL", [resolved_key], store)
    :ok
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
  Decrements the integer value stored at `key` by 1.

  If the key does not exist, it is initialized to `0` before decrementing,
  resulting in a value of `-1`. If the key holds a value that cannot be parsed
  as an integer, returns `{:error, reason}`.

  ## Returns

    * `{:ok, new_value}` where `new_value` is the integer value after decrement.
    * `{:error, reason}` if the stored value is not a valid integer.

  ## Examples

      {:ok, -1} = FerricStore.decr("counter")

  """
  @spec decr(key()) :: {:ok, integer()} | {:error, binary()}
  def decr(key) do
    incr_by(key, -1)
  end

  @doc """
  Decrements the integer value stored at `key` by `amount`.

  If the key does not exist, it is initialized to `0` before decrementing.

  ## Returns

    * `{:ok, new_value}` where `new_value` is the integer value after decrement.
    * `{:error, reason}` if the stored value is not a valid integer.

  ## Examples

      {:ok, 90} = FerricStore.decr_by("counter", 10)

  """
  @spec decr_by(key(), integer()) :: {:ok, integer()} | {:error, binary()}
  def decr_by(key, amount) when is_integer(amount) do
    incr_by(key, -amount)
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

  @doc """
  Increments the float value stored at `key` by `amount`.

  If the key does not exist, it is initialized to `0` before incrementing.
  Returns the new value as a string.

  ## Returns

    * `{:ok, new_value_string}` on success.
    * `{:error, reason}` if the stored value is not a valid number.

  ## Examples

      {:ok, "3.14"} = FerricStore.incr_by_float("counter", 3.14)

  """
  @spec incr_by_float(key(), float()) :: {:ok, binary()} | {:error, binary()}
  def incr_by_float(key, amount) when is_number(amount) do
    resolved_key = sandbox_key(key)

    case Router.incr_float(resolved_key, amount * 1.0) do
      {:ok, result} -> {:ok, result}
      {:error, _} = err -> err
    end
  end

  @doc """
  Gets values for multiple keys in a single call.

  Returns `{:ok, [value | nil, ...]}` with values in the same order as the keys.

  ## Examples

      {:ok, ["val1", nil, "val3"]} = FerricStore.mget(["key1", "key2", "key3"])

  """
  @spec mget([key()]) :: {:ok, [value() | nil]}
  def mget(keys) when is_list(keys) do
    values = Enum.map(keys, fn key ->
      key |> sandbox_key() |> Router.get()
    end)
    {:ok, values}
  end

  @doc """
  Sets multiple key-value pairs atomically.

  ## Returns

    * `:ok`

  ## Examples

      :ok = FerricStore.mset(%{"key1" => "val1", "key2" => "val2"})

  """
  @spec mset(%{key() => value()}) :: :ok
  def mset(pairs) when is_map(pairs) do
    Enum.each(pairs, fn {key, value} ->
      key |> sandbox_key() |> Router.put(value, 0)
    end)
    :ok
  end

  @doc """
  Appends `suffix` to the value of `key`. Creates the key if it does not exist.

  ## Returns

    * `{:ok, new_byte_length}` on success.

  ## Examples

      {:ok, 11} = FerricStore.append("key", " World")

  """
  @spec append(key(), binary()) :: {:ok, non_neg_integer()}
  def append(key, suffix) do
    resolved_key = sandbox_key(key)
    case Router.append(resolved_key, suffix) do
      {:ok, len} -> {:ok, len}
      len when is_integer(len) -> {:ok, len}
    end
  end

  @doc """
  Returns the byte length of the string value stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      {:ok, 5} = FerricStore.strlen("key")

  """
  @spec strlen(key()) :: {:ok, non_neg_integer()}
  def strlen(key) do
    resolved_key = sandbox_key(key)
    case Router.get(resolved_key) do
      nil -> {:ok, 0}
      value -> {:ok, byte_size(value)}
    end
  end

  @doc """
  Atomically sets `key` to `value` and returns the old value.

  Returns `{:ok, old_value}` or `{:ok, nil}` if the key did not exist.

  ## Examples

      {:ok, "old"} = FerricStore.getset("key", "new")

  """
  @spec getset(key(), value()) :: {:ok, value() | nil}
  def getset(key, value) do
    resolved_key = sandbox_key(key)
    result = Router.getset(resolved_key, value)
    {:ok, result}
  end

  @doc """
  Atomically gets and deletes `key`.

  Returns `{:ok, value}` or `{:ok, nil}` if the key did not exist.

  ## Examples

      {:ok, "value"} = FerricStore.getdel("key")

  """
  @spec getdel(key()) :: {:ok, value() | nil}
  def getdel(key) do
    resolved_key = sandbox_key(key)
    result = Router.getdel(resolved_key)
    {:ok, result}
  end

  @doc """
  Gets the value of `key` and optionally updates its expiry.

  ## Options

    * `:ttl` - New TTL in milliseconds.
    * `:persist` - When `true`, removes the TTL.

  ## Returns

    * `{:ok, value}` or `{:ok, nil}` if the key does not exist.

  ## Examples

      {:ok, "value"} = FerricStore.getex("key", ttl: 60_000)

  """
  @spec getex(key(), keyword()) :: {:ok, value() | nil}
  def getex(key, opts \\ []) do
    resolved_key = sandbox_key(key)

    expire_at_ms =
      cond do
        Keyword.get(opts, :persist, false) ->
          0

        ttl = Keyword.get(opts, :ttl) ->
          System.os_time(:millisecond) + ttl

        true ->
          nil
      end

    case expire_at_ms do
      nil ->
        {:ok, Router.get(resolved_key)}

      ms ->
        result = Router.getex(resolved_key, ms)
        {:ok, result}
    end
  end

  @doc """
  Sets `key` to `value` only if the key does not already exist.

  ## Returns

    * `{:ok, true}` if the key was set.
    * `{:ok, false}` if the key already existed.

  ## Examples

      {:ok, true} = FerricStore.setnx("new_key", "value")

  """
  @spec setnx(key(), value()) :: {:ok, boolean()}
  def setnx(key, value) do
    resolved_key = sandbox_key(key)
    if Router.exists?(resolved_key) do
      {:ok, false}
    else
      Router.put(resolved_key, value, 0)
      {:ok, true}
    end
  end

  @doc """
  Sets `key` to `value` with a TTL in seconds.

  ## Returns

    * `:ok`

  ## Examples

      :ok = FerricStore.setex("key", 60, "value")

  """
  @spec setex(key(), pos_integer(), value()) :: :ok
  def setex(key, seconds, value) do
    resolved_key = sandbox_key(key)
    expire_at_ms = System.os_time(:millisecond) + seconds * 1_000
    Router.put(resolved_key, value, expire_at_ms)
  end

  @doc """
  Sets `key` to `value` with a TTL in milliseconds.

  ## Returns

    * `:ok`

  ## Examples

      :ok = FerricStore.psetex("key", 60_000, "value")

  """
  @spec psetex(key(), pos_integer(), value()) :: :ok
  def psetex(key, milliseconds, value) do
    resolved_key = sandbox_key(key)
    expire_at_ms = System.os_time(:millisecond) + milliseconds
    Router.put(resolved_key, value, expire_at_ms)
  end

  @doc """
  Returns a substring of the string stored at `key` between byte offsets `start` and `stop`.

  Negative offsets count from the end. Returns `{:ok, ""}` for nonexistent keys.

  ## Examples

      {:ok, "World"} = FerricStore.getrange("key", 6, 10)

  """
  @spec getrange(key(), integer(), integer()) :: {:ok, binary()}
  def getrange(key, start, stop) do
    resolved_key = sandbox_key(key)
    value = Router.get(resolved_key) || ""
    len = byte_size(value)

    if len == 0 do
      {:ok, ""}
    else
      s = if start < 0, do: max(0, len + start), else: min(start, len - 1)
      e = if stop < 0, do: max(0, len + stop), else: min(stop, len - 1)

      if s > e do
        {:ok, ""}
      else
        {:ok, binary_part(value, s, e - s + 1)}
      end
    end
  end

  @doc """
  Overwrites part of the string at `key` starting at `offset`.

  Zero-pads if the key doesn't exist or the string is shorter than offset.
  Returns `{:ok, new_byte_length}`.

  ## Examples

      {:ok, 11} = FerricStore.setrange("key", 6, "Redis")

  """
  @spec setrange(key(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
  def setrange(key, offset, value) do
    resolved_key = sandbox_key(key)
    case Router.setrange(resolved_key, offset, value) do
      {:ok, len} -> {:ok, len}
      len when is_integer(len) -> {:ok, len}
    end
  end

  @doc """
  Sets multiple key-value pairs only if none of the keys already exist.

  ## Returns

    * `{:ok, true}` if all keys were set.
    * `{:ok, false}` if any key already existed (no keys are set).

  ## Examples

      {:ok, true} = FerricStore.msetnx(%{"a" => "1", "b" => "2"})

  """
  @spec msetnx(%{key() => value()}) :: {:ok, boolean()}
  def msetnx(pairs) when is_map(pairs) do
    resolved_pairs = Enum.map(pairs, fn {k, v} -> {sandbox_key(k), v} end)

    any_exists = Enum.any?(resolved_pairs, fn {k, _v} -> Router.exists?(k) end)

    if any_exists do
      {:ok, false}
    else
      Enum.each(resolved_pairs, fn {k, v} -> Router.put(k, v, 0) end)
      {:ok, true}
    end
  end

  # ---------------------------------------------------------------------------
  # Hash
  # ---------------------------------------------------------------------------

  @doc """
  Sets one or more fields in the hash stored at `key`.

  `fields` is a map of `%{field_name => value}`. Field names and values are
  stored as binaries. If the hash does not exist, a new one is created.

  Each field is stored as a separate compound key (`H:key\\0field`) matching
  the RESP/Dispatcher storage format.

  ## Examples

      :ok = FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})

  """
  @spec hset(key(), %{binary() => binary()}) :: :ok
  def hset(key, fields) when is_map(fields) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    args =
      Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)

    case Ferricstore.Commands.Hash.handle("HSET", [resolved_key | args], store) do
      {:error, _} = err -> err
      _count -> :ok
    end
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
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HGET", [resolved_key, to_string(field)], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
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
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HGETALL", [resolved_key], store) do
      {:error, _} = err -> err
      flat_list ->
        map =
          flat_list
          |> Enum.chunk_every(2)
          |> Map.new(fn [f, v] -> {f, v} end)

        {:ok, map}
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
    alias Ferricstore.Store.CompoundKey

    namespace = Process.get(:ferricstore_sandbox)
    all_keys = Router.keys()
    match_all? = pattern == "*"

    # Compound keys store the sandbox namespace INSIDE the type prefix:
    #   "T:ns_prefix_user:42", "H:ns_prefix_user:42\0name"
    # So we must first convert raw keys to user-visible logical keys
    # (extracting from T: entries, rejecting H:/L:/S:/Z: etc.), then
    # strip the sandbox namespace from the resulting logical keys.
    visible = CompoundKey.user_visible_keys(all_keys)

    results =
      if namespace do
        prefix_len = byte_size(namespace)

        Enum.reduce(visible, [], fn key, acc ->
          if String.starts_with?(key, namespace) do
            stripped = binary_part(key, prefix_len, byte_size(key) - prefix_len)

            if match_all? or Ferricstore.GlobMatcher.match?(stripped, pattern) do
              [stripped | acc]
            else
              acc
            end
          else
            acc
          end
        end)
      else
        if match_all? do
          visible
        else
          Enum.filter(visible, &Ferricstore.GlobMatcher.match?(&1, pattern))
        end
      end

    {:ok, results}
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
      # Delete ALL keys including compound sub-keys through the Shard
      # GenServer which handles ETS + Bitcask tombstones properly.
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

      for i <- 0..(shard_count - 1) do
        shard = Router.shard_name(i)

        # Get ALL raw keys from ETS (including H:, S:, Z:, T: compound keys)
        raw_keys =
          try do
            :ets.foldl(fn {key, _, _, _, _, _, _}, acc -> [key | acc] end, [], :"keydir_#{i}")
          rescue
            ArgumentError -> []
          end

        # Delete each through the shard to write Bitcask tombstones
        Enum.each(raw_keys, fn key ->
          try do
            GenServer.call(shard, {:delete, key}, 10_000)
          catch
            :exit, _ -> :ok
          end
        end)

        # Clear prefix index
        try do :ets.delete_all_objects(:"prefix_keys_#{i}") catch :error, :badarg -> :ok end
      end
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
  # Key Operations (copy, rename, renamenx, type, randomkey)
  # ---------------------------------------------------------------------------

  @doc """
  Copies the value from `source` to `destination`.

  ## Options

    * `:replace` - When `true`, overwrites `destination` if it exists.

  ## Returns

    * `{:ok, true}` on success.
    * `{:error, reason}` if the source does not exist or destination exists without replace.

  ## Examples

      {:ok, true} = FerricStore.copy("src", "dst")

  """
  @spec copy(key(), key(), keyword()) :: {:ok, true} | {:error, binary()}
  def copy(source, destination, opts \\ []) do
    src_key = sandbox_key(source)
    dst_key = sandbox_key(destination)
    replace = Keyword.get(opts, :replace, false)

    case Router.get_meta(src_key) do
      nil ->
        {:error, "ERR no such key"}

      {value, expire_at_ms} ->
        if Router.exists?(dst_key) and not replace do
          {:error, "ERR target key already exists"}
        else
          Router.put(dst_key, value, expire_at_ms)
          {:ok, true}
        end
    end
  end

  @doc """
  Renames `source` to `destination`. Deletes `destination` if it exists.

  ## Returns

    * `:ok` on success.
    * `{:error, reason}` if the source does not exist.

  ## Examples

      :ok = FerricStore.rename("old", "new")

  """
  @spec rename(key(), key()) :: :ok | {:error, binary()}
  def rename(source, destination) do
    src_key = sandbox_key(source)
    dst_key = sandbox_key(destination)

    case Router.get_meta(src_key) do
      nil ->
        {:error, "ERR no such key"}

      {value, expire_at_ms} ->
        Router.put(dst_key, value, expire_at_ms)
        Router.delete(src_key)
        :ok
    end
  end

  @doc """
  Renames `source` to `destination` only if `destination` does not exist.

  ## Returns

    * `{:ok, true}` if renamed.
    * `{:ok, false}` if destination already exists.
    * `{:error, reason}` if source does not exist.

  ## Examples

      {:ok, true} = FerricStore.renamenx("old", "new")

  """
  @spec renamenx(key(), key()) :: {:ok, boolean()} | {:error, binary()}
  def renamenx(source, destination) do
    src_key = sandbox_key(source)
    dst_key = sandbox_key(destination)

    case Router.get_meta(src_key) do
      nil ->
        {:error, "ERR no such key"}

      {value, expire_at_ms} ->
        if Router.exists?(dst_key) do
          {:ok, false}
        else
          Router.put(dst_key, value, expire_at_ms)
          Router.delete(src_key)
          {:ok, true}
        end
    end
  end

  @doc """
  Returns the type of the value stored at `key`.

  ## Returns

    * `{:ok, type_string}` where type_string is one of: "string", "hash", "list",
      "set", "zset", "stream", or "none".

  ## Examples

      {:ok, "string"} = FerricStore.type("key")
      {:ok, "none"} = FerricStore.type("missing")

  """
  @spec type(key()) :: {:ok, binary()}
  def type(key) do
    resolved_key = sandbox_key(key)

    # Check compound key type registry first (for hash/set/zset stored via compound keys)
    store = build_compound_store(resolved_key)
    type_key = Ferricstore.Store.CompoundKey.type_key(resolved_key)
    compound_type = store.compound_get.(resolved_key, type_key)

    cond do
      compound_type != nil ->
        {:ok, compound_type}

      # Check for list metadata key (lists use compound keys, no type marker)
      store.compound_get.(resolved_key, Ferricstore.Store.CompoundKey.list_meta_key(resolved_key)) != nil ->
        {:ok, "list"}

      true ->
        case Router.get(resolved_key) do
          nil ->
            {:ok, "none"}

          value when is_binary(value) ->
            detected = try do
              case :erlang.binary_to_term(value) do
                {:list, _} -> "list"
                _ -> nil
              end
            rescue
              ArgumentError -> nil
            end

            if detected do
              {:ok, detected}
            else
              {:ok, "string"}
            end

          _ ->
            {:ok, "string"}
        end
    end
  end

  @doc """
  Returns a random key from the store, or `{:ok, nil}` if the store is empty.

  ## Examples

      {:ok, "some_key"} = FerricStore.randomkey()

  """
  @spec randomkey() :: {:ok, key() | nil}
  def randomkey do
    {:ok, all_keys} = keys()
    case all_keys do
      [] -> {:ok, nil}
      _ -> {:ok, Enum.random(all_keys)}
    end
  end

  # ---------------------------------------------------------------------------
  # TTL extended: persist, pexpire, pexpireat, expireat, expiretime, pexpiretime, pttl
  # ---------------------------------------------------------------------------

  @doc """
  Removes the TTL from `key`, making it persistent.

  ## Returns

    * `{:ok, true}` if the TTL was removed.
    * `{:ok, false}` if the key does not exist or has no TTL.

  ## Examples

      {:ok, true} = FerricStore.persist("key")

  """
  @spec persist(key()) :: {:ok, boolean()}
  def persist(key) do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil ->
        {:ok, false}

      {_value, 0} ->
        {:ok, false}

      {value, _exp} ->
        Router.put(resolved_key, value, 0)
        {:ok, true}
    end
  end

  @doc """
  Sets a TTL in milliseconds on `key`. Alias for `expire/2`.

  ## Returns

    * `{:ok, true}` if the TTL was set.
    * `{:ok, false}` if the key does not exist.

  ## Examples

      {:ok, true} = FerricStore.pexpire("key", 30_000)

  """
  @spec pexpire(key(), non_neg_integer()) :: {:ok, boolean()}
  def pexpire(key, ttl_ms), do: expire(key, ttl_ms)

  @doc """
  Sets the key to expire at the given Unix timestamp in seconds.

  ## Returns

    * `{:ok, true}` if the expiry was set.
    * `{:ok, false}` if the key does not exist.

  ## Examples

      {:ok, true} = FerricStore.expireat("key", 1700000000)

  """
  @spec expireat(key(), non_neg_integer()) :: {:ok, boolean()}
  def expireat(key, unix_ts_seconds) do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil ->
        {:ok, false}

      {value, _old_exp} ->
        expire_at_ms = unix_ts_seconds * 1_000
        Router.put(resolved_key, value, expire_at_ms)
        {:ok, true}
    end
  end

  @doc """
  Sets the key to expire at the given Unix timestamp in milliseconds.

  ## Returns

    * `{:ok, true}` if the expiry was set.
    * `{:ok, false}` if the key does not exist.

  ## Examples

      {:ok, true} = FerricStore.pexpireat("key", 1700000000000)

  """
  @spec pexpireat(key(), non_neg_integer()) :: {:ok, boolean()}
  def pexpireat(key, unix_ts_ms) do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil ->
        {:ok, false}

      {value, _old_exp} ->
        Router.put(resolved_key, value, unix_ts_ms)
        {:ok, true}
    end
  end

  @doc """
  Returns the absolute Unix timestamp (in seconds) at which `key` will expire.

  ## Returns

    * `{:ok, timestamp}` if the key has an expiry.
    * `{:ok, -1}` if the key exists but has no expiry.
    * `{:ok, -2}` if the key does not exist.

  ## Examples

      {:ok, 1700000000} = FerricStore.expiretime("key")

  """
  @spec expiretime(key()) :: {:ok, integer()}
  def expiretime(key) do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil -> {:ok, -2}
      {_value, 0} -> {:ok, -1}
      {_value, exp} -> {:ok, div(exp, 1_000)}
    end
  end

  @doc """
  Returns the absolute Unix timestamp (in milliseconds) at which `key` will expire.

  ## Returns

    * `{:ok, timestamp_ms}` if the key has an expiry.
    * `{:ok, -1}` if the key exists but has no expiry.
    * `{:ok, -2}` if the key does not exist.

  ## Examples

      {:ok, 1700000000000} = FerricStore.pexpiretime("key")

  """
  @spec pexpiretime(key()) :: {:ok, integer()}
  def pexpiretime(key) do
    resolved_key = sandbox_key(key)

    case Router.get_meta(resolved_key) do
      nil -> {:ok, -2}
      {_value, 0} -> {:ok, -1}
      {_value, exp} -> {:ok, exp}
    end
  end

  @doc """
  Returns the remaining TTL in milliseconds. Alias for `ttl/1`.

  ## Returns

    * `{:ok, ms}` if a TTL is set.
    * `{:ok, nil}` if the key has no expiry or does not exist.

  ## Examples

      {:ok, ms} = FerricStore.pttl("key")

  """
  @spec pttl(key()) :: {:ok, non_neg_integer() | nil}
  def pttl(key), do: ttl(key)

  # ---------------------------------------------------------------------------
  # Bitmap operations
  # ---------------------------------------------------------------------------

  @doc """
  Sets or clears the bit at `offset` in the string value stored at `key`.

  Returns the original bit value at that position.

  ## Examples

      {:ok, 0} = FerricStore.setbit("key", 7, 1)

  """
  @spec setbit(key(), non_neg_integer(), 0 | 1) :: {:ok, 0 | 1}
  def setbit(key, offset, bit_value) when bit_value in [0, 1] do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Bitmap.handle("SETBIT", [resolved_key, to_string(offset), to_string(bit_value)], store)
    wrap_result(result)
  end

  @doc """
  Returns the bit value at `offset` in the string value stored at `key`.

  Returns `{:ok, 0}` for nonexistent keys or out-of-range offsets.

  ## Examples

      {:ok, 1} = FerricStore.getbit("key", 7)

  """
  @spec getbit(key(), non_neg_integer()) :: {:ok, 0 | 1}
  def getbit(key, offset) do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Bitmap.handle("GETBIT", [resolved_key, to_string(offset)], store)
    wrap_result(result)
  end

  @doc """
  Counts the number of set bits (1s) in the string value stored at `key`.

  ## Options

    * `:start` - Start byte offset (default: 0).
    * `:stop` - Stop byte offset (default: -1, meaning end of string).

  ## Returns

    * `{:ok, count}` on success.

  ## Examples

      {:ok, count} = FerricStore.bitcount("key")

  """
  @spec bitcount(key(), keyword()) :: {:ok, non_neg_integer()}
  def bitcount(key, opts \\ []) do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    start = Keyword.get(opts, :start)
    stop = Keyword.get(opts, :stop)

    args = if start != nil and stop != nil do
      [resolved_key, to_string(start), to_string(stop)]
    else
      [resolved_key]
    end

    result = Ferricstore.Commands.Bitmap.handle("BITCOUNT", args, store)
    wrap_result(result)
  end

  @doc """
  Performs a bitwise operation between strings stored at `source_keys` and
  stores the result in `dest_key`.

  ## Parameters

    * `op` - `:and`, `:or`, `:xor`, or `:not`
    * `dest_key` - Destination key.
    * `source_keys` - List of source keys.

  ## Returns

    * `{:ok, byte_length}` - Length of the result string.

  ## Examples

      {:ok, 3} = FerricStore.bitop(:and, "dest", ["key1", "key2"])

  """
  @spec bitop(atom(), key(), [key()]) :: {:ok, non_neg_integer()}
  def bitop(op, dest_key, source_keys) when is_atom(op) and is_list(source_keys) do
    resolved_dest = sandbox_key(dest_key)
    resolved_sources = Enum.map(source_keys, &sandbox_key/1)
    store = build_string_store(resolved_dest)
    op_str = op |> Atom.to_string() |> String.upcase()
    result = Ferricstore.Commands.Bitmap.handle("BITOP", [op_str, resolved_dest | resolved_sources], store)
    wrap_result(result)
  end

  @doc """
  Finds the first bit set to `bit_value` (0 or 1) in the string at `key`.

  ## Options

    * `:start` - Start byte offset.
    * `:stop` - Stop byte offset.

  ## Returns

    * `{:ok, position}` - Bit position, or -1 if not found within a bounded range.

  ## Examples

      {:ok, 8} = FerricStore.bitpos("key", 1)

  """
  @spec bitpos(key(), 0 | 1, keyword()) :: {:ok, integer()}
  def bitpos(key, bit_value, opts \\ []) when bit_value in [0, 1] do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    start = Keyword.get(opts, :start)
    stop = Keyword.get(opts, :stop)

    args = cond do
      start != nil and stop != nil ->
        [resolved_key, to_string(bit_value), to_string(start), to_string(stop)]
      start != nil ->
        [resolved_key, to_string(bit_value), to_string(start)]
      true ->
        [resolved_key, to_string(bit_value)]
    end

    result = Ferricstore.Commands.Bitmap.handle("BITPOS", args, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Hash extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Deletes one or more fields from the hash stored at `key`.

  ## Returns

    * `{:ok, count}` - Number of fields that were removed.

  ## Examples

      {:ok, 2} = FerricStore.hdel("hash", ["field1", "field2"])

  """
  @spec hdel(key(), [binary()]) :: {:ok, non_neg_integer()}
  def hdel(key, fields) when is_list(fields) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    str_fields = Enum.map(fields, &to_string/1)

    case Ferricstore.Commands.Hash.handle("HDEL", [resolved_key | str_fields], store) do
      {:error, _} = err -> err
      count -> {:ok, count}
    end
  end

  @doc """
  Returns whether `field` exists in the hash stored at `key`.

  ## Examples

      true = FerricStore.hexists("hash", "name")

  """
  @spec hexists(key(), binary()) :: boolean()
  def hexists(key, field) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HEXISTS", [resolved_key, to_string(field)], store) do
      {:error, _} = err -> err
      1 -> true
      0 -> false
    end
  end

  @doc """
  Returns the number of fields in the hash stored at `key`.

  ## Examples

      {:ok, 3} = FerricStore.hlen("hash")

  """
  @spec hlen(key()) :: {:ok, non_neg_integer()}
  def hlen(key) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HLEN", [resolved_key], store) do
      {:error, _} = err -> err
      count -> {:ok, count}
    end
  end

  @doc """
  Returns all field names from the hash stored at `key`.

  ## Examples

      {:ok, ["name", "age"]} = FerricStore.hkeys("hash")

  """
  @spec hkeys(key()) :: {:ok, [binary()]}
  def hkeys(key) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HKEYS", [resolved_key], store) do
      {:error, _} = err -> err
      keys_list -> {:ok, keys_list}
    end
  end

  @doc """
  Returns all field values from the hash stored at `key`.

  ## Examples

      {:ok, ["alice", "30"]} = FerricStore.hvals("hash")

  """
  @spec hvals(key()) :: {:ok, [binary()]}
  def hvals(key) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HVALS", [resolved_key], store) do
      {:error, _} = err -> err
      vals_list -> {:ok, vals_list}
    end
  end

  @doc """
  Returns values for the specified `fields` from the hash at `key`.

  Returns `nil` for fields that do not exist.

  ## Examples

      {:ok, ["alice", nil]} = FerricStore.hmget("hash", ["name", "missing"])

  """
  @spec hmget(key(), [binary()]) :: {:ok, [binary() | nil]}
  def hmget(key, fields) when is_list(fields) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    str_fields = Enum.map(fields, &to_string/1)

    case Ferricstore.Commands.Hash.handle("HMGET", [resolved_key | str_fields], store) do
      {:error, _} = err -> err
      values -> {:ok, values}
    end
  end

  @doc """
  Increments the integer value of `field` in the hash at `key` by `amount`.

  ## Returns

    * `{:ok, new_value}` on success.

  ## Examples

      {:ok, 15} = FerricStore.hincrby("hash", "count", 5)

  """
  @spec hincrby(key(), binary(), integer()) :: {:ok, integer()} | {:error, binary()}
  def hincrby(key, field, amount) when is_integer(amount) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle(
           "HINCRBY",
           [resolved_key, to_string(field), Integer.to_string(amount)],
           store
         ) do
      {:error, _} = err -> err
      new_val -> {:ok, new_val}
    end
  end

  @doc """
  Increments the float value of `field` in the hash at `key` by `amount`.

  ## Returns

    * `{:ok, new_value_string}` on success.

  ## Examples

      {:ok, "12.5"} = FerricStore.hincrbyfloat("hash", "val", 2.5)

  """
  @spec hincrbyfloat(key(), binary(), float()) :: {:ok, binary()} | {:error, binary()}
  def hincrbyfloat(key, field, amount) when is_number(amount) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    amount_str = :erlang.float_to_binary(amount * 1.0, [:compact, decimals: 17])

    case Ferricstore.Commands.Hash.handle(
           "HINCRBYFLOAT",
           [resolved_key, to_string(field), amount_str],
           store
         ) do
      {:error, _} = err -> err
      result_str -> {:ok, result_str}
    end
  end

  @doc """
  Sets `field` in the hash at `key` only if the field does not already exist.

  ## Returns

    * `{:ok, true}` if the field was set.
    * `{:ok, false}` if the field already existed.

  ## Examples

      {:ok, true} = FerricStore.hsetnx("hash", "field", "value")

  """
  @spec hsetnx(key(), binary(), binary()) :: {:ok, boolean()}
  def hsetnx(key, field, value) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle(
           "HSETNX",
           [resolved_key, to_string(field), to_string(value)],
           store
         ) do
      {:error, _} = err -> err
      1 -> {:ok, true}
      0 -> {:ok, false}
    end
  end

  @doc """
  Returns one or more random field names from the hash at `key`.

  Without `count`, returns a single field name or `nil`.
  With `count`, returns a list of field names.

  ## Examples

      {:ok, "name"} = FerricStore.hrandfield("hash")
      {:ok, ["name", "age"]} = FerricStore.hrandfield("hash", 2)

  """
  @spec hrandfield(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def hrandfield(key, count \\ nil) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case count do
      nil ->
        case Ferricstore.Commands.Hash.handle("HRANDFIELD", [resolved_key], store) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end

      n when is_integer(n) ->
        case Ferricstore.Commands.Hash.handle(
               "HRANDFIELD",
               [resolved_key, Integer.to_string(n)],
               store
             ) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end
    end
  end

  @doc """
  Returns the string length of the value for `field` in the hash at `key`.

  ## Examples

      {:ok, 5} = FerricStore.hstrlen("hash", "name")

  """
  @spec hstrlen(key(), binary()) :: {:ok, non_neg_integer()}
  def hstrlen(key, field) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case Ferricstore.Commands.Hash.handle("HSTRLEN", [resolved_key, to_string(field)], store) do
      {:error, _} = err -> err
      len -> {:ok, len}
    end
  end

  # ---------------------------------------------------------------------------
  # List extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Returns the element at `index` in the list stored at `key`.

  Negative indices count from the end. Returns `{:ok, nil}` for out-of-range
  indices or nonexistent keys.

  ## Examples

      {:ok, "a"} = FerricStore.lindex("list", 0)

  """
  @spec lindex(key(), integer()) :: {:ok, binary() | nil}
  def lindex(key, index) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:lindex, index})

    {:ok, result}
  end

  @doc """
  Sets the element at `index` in the list stored at `key`.

  ## Returns

    * `:ok` on success.
    * `{:error, reason}` if the index is out of range.

  ## Examples

      :ok = FerricStore.lset("list", 1, "X")

  """
  @spec lset(key(), integer(), binary()) :: :ok | {:error, binary()}
  def lset(key, index, element) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:lset, index, element})

    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Removes occurrences of `element` from the list at `key`.

  * `count > 0` - Remove `count` occurrences from head.
  * `count < 0` - Remove `abs(count)` occurrences from tail.
  * `count == 0` - Remove all occurrences.

  ## Returns

    * `{:ok, removed_count}` on success.

  ## Examples

      {:ok, 3} = FerricStore.lrem("list", 0, "a")

  """
  @spec lrem(key(), integer(), binary()) :: {:ok, non_neg_integer()}
  def lrem(key, count, element) do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:lrem, count, element})

    {:ok, result}
  end

  @doc """
  Inserts `element` before or after `pivot` in the list at `key`.

  ## Parameters

    * `direction` - `:before` or `:after`
    * `pivot` - The reference element.
    * `element` - The element to insert.

  ## Returns

    * `{:ok, new_length}` if the pivot was found.
    * `{:ok, -1}` if the pivot was not found.

  ## Examples

      {:ok, 4} = FerricStore.linsert("list", :before, "b", "X")

  """
  @spec linsert(key(), :before | :after, binary(), binary()) :: {:ok, integer()}
  def linsert(key, direction, pivot, element) when direction in [:before, :after] do
    result =
      key
      |> sandbox_key()
      |> Router.list_op({:linsert, direction, pivot, element})

    {:ok, result}
  end

  @doc """
  Atomically moves an element from one list to another.

  ## Parameters

    * `source` - Source list key.
    * `destination` - Destination list key.
    * `from_dir` - `:left` or `:right`, which end to pop from.
    * `to_dir` - `:left` or `:right`, which end to push to.

  ## Returns

    * `{:ok, element}` on success.
    * `{:ok, nil}` if the source list is empty.

  ## Examples

      {:ok, "a"} = FerricStore.lmove("src", "dst", :left, :right)

  """
  @spec lmove(key(), key(), :left | :right, :left | :right) :: {:ok, binary() | nil}
  def lmove(source, destination, from_dir, to_dir)
      when from_dir in [:left, :right] and to_dir in [:left, :right] do
    src_key = sandbox_key(source)
    dst_key = sandbox_key(destination)
    result = Router.list_op(src_key, {:lmove, dst_key, from_dir, to_dir})
    {:ok, result}
  end

  @doc """
  Finds the position of `element` in the list at `key`.

  ## Options

    * `:rank` - Start searching from the Nth match (default: 1).
    * `:count` - Return up to N positions. 0 means all. When given, returns a list.
    * `:maxlen` - Limit scan to first N elements.

  ## Returns

    * `{:ok, index}` for single result.
    * `{:ok, [indices]}` when `:count` is specified.
    * `{:ok, nil}` when element not found.

  ## Examples

      {:ok, 1} = FerricStore.lpos("list", "b")
      {:ok, [0, 2, 4]} = FerricStore.lpos("list", "a", count: 0)

  """
  @spec lpos(key(), binary(), keyword()) :: {:ok, integer() | [integer()] | nil}
  def lpos(key, element, opts \\ []) do
    resolved_key = sandbox_key(key)
    rank = Keyword.get(opts, :rank, 1)
    count = Keyword.get(opts, :count)
    maxlen = Keyword.get(opts, :maxlen, 0)

    result = Router.list_op(resolved_key, {:lpos, element, rank, count, maxlen})

    {:ok, result}
  end

  # ---------------------------------------------------------------------------
  # Set extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Returns the membership status of multiple members in the set at `key`.

  Returns a list of 1s and 0s corresponding to each member.

  ## Examples

      {:ok, [1, 0, 1]} = FerricStore.smismember("set", ["a", "z", "c"])

  """
  @spec smismember(key(), [binary()]) :: {:ok, [0 | 1]}
  def smismember(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    results = Enum.map(members, fn member ->
      compound_key = Ferricstore.Store.CompoundKey.set_member(resolved_key, member)
      if store.compound_get.(resolved_key, compound_key) != nil, do: 1, else: 0
    end)

    {:ok, results}
  end

  @doc """
  Returns one or more random members from the set at `key`.

  Without `count`, returns a single member or `nil`.
  With `count`, returns a list of members.

  ## Examples

      {:ok, "a"} = FerricStore.srandmember("set")
      {:ok, ["a", "b"]} = FerricStore.srandmember("set", 2)

  """
  @spec srandmember(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def srandmember(key, count \\ nil) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case count do
      nil ->
        result = Ferricstore.Commands.Set.handle("SRANDMEMBER", [resolved_key], store)
        {:ok, result}

      n ->
        result = Ferricstore.Commands.Set.handle("SRANDMEMBER", [resolved_key, to_string(n)], store)
        {:ok, result}
    end
  end

  @doc """
  Removes and returns one or more random members from the set at `key`.

  Without `count`, returns a single member or `nil`.
  With `count`, returns a list of members.

  ## Examples

      {:ok, "a"} = FerricStore.spop("set")
      {:ok, ["a", "b"]} = FerricStore.spop("set", 2)

  """
  @spec spop(key(), non_neg_integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def spop(key, count \\ nil) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case count do
      nil ->
        result = Ferricstore.Commands.Set.handle("SPOP", [resolved_key], store)
        {:ok, result}

      n ->
        result = Ferricstore.Commands.Set.handle("SPOP", [resolved_key, to_string(n)], store)
        {:ok, result}
    end
  end

  @doc """
  Returns the set difference: members in the first set that are not in the others.

  Handles cross-shard keys by gathering members from each key's shard.

  ## Examples

      {:ok, ["a"]} = FerricStore.sdiff(["set1", "set2"])

  """
  @spec sdiff([key()]) :: {:ok, [binary()]}
  def sdiff(keys) when is_list(keys) do
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    sets = Enum.map(resolved_keys, &gather_set_members/1)

    result =
      case sets do
        [first | rest] ->
          Enum.reduce(rest, first, fn s, acc -> MapSet.difference(acc, s) end)
        [] ->
          MapSet.new()
      end

    {:ok, MapSet.to_list(result)}
  end

  @doc """
  Returns the set intersection: members common to all given sets.

  Handles cross-shard keys by gathering members from each key's shard.

  ## Examples

      {:ok, ["b", "c"]} = FerricStore.sinter(["set1", "set2"])

  """
  @spec sinter([key()]) :: {:ok, [binary()]}
  def sinter(keys) when is_list(keys) do
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    sets = Enum.map(resolved_keys, &gather_set_members/1)

    result =
      case sets do
        [first | rest] ->
          Enum.reduce(rest, first, fn s, acc -> MapSet.intersection(acc, s) end)
        [] ->
          MapSet.new()
      end

    {:ok, MapSet.to_list(result)}
  end

  @doc """
  Returns the set union: all unique members across all given sets.

  Handles cross-shard keys by gathering members from each key's shard.

  ## Examples

      {:ok, ["a", "b", "c"]} = FerricStore.sunion(["set1", "set2"])

  """
  @spec sunion([key()]) :: {:ok, [binary()]}
  def sunion(keys) when is_list(keys) do
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    sets = Enum.map(resolved_keys, &gather_set_members/1)
    result = Enum.reduce(sets, MapSet.new(), fn s, acc -> MapSet.union(acc, s) end)
    {:ok, MapSet.to_list(result)}
  end

  @doc """
  Computes the set difference of the given keys and stores the result in `destination`.

  Returns the number of elements in the resulting set.

  ## Examples

      {:ok, 2} = FerricStore.sdiffstore("dst", ["set1", "set2"])

  """
  @spec sdiffstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sdiffstore(destination, keys) when is_list(keys) do
    resolved_dst = sandbox_key(destination)
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    store = build_compound_store(resolved_dst)
    result = Ferricstore.Commands.Set.handle("SDIFFSTORE", [resolved_dst | resolved_keys], store)
    wrap_result(result)
  end

  @doc """
  Computes the set intersection of the given keys and stores the result in `destination`.

  Returns the number of elements in the resulting set.

  ## Examples

      {:ok, 2} = FerricStore.sinterstore("dst", ["set1", "set2"])

  """
  @spec sinterstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sinterstore(destination, keys) when is_list(keys) do
    resolved_dst = sandbox_key(destination)
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    store = build_compound_store(resolved_dst)
    result = Ferricstore.Commands.Set.handle("SINTERSTORE", [resolved_dst | resolved_keys], store)
    wrap_result(result)
  end

  @doc """
  Computes the set union of the given keys and stores the result in `destination`.

  Returns the number of elements in the resulting set.

  ## Examples

      {:ok, 4} = FerricStore.sunionstore("dst", ["set1", "set2"])

  """
  @spec sunionstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sunionstore(destination, keys) when is_list(keys) do
    resolved_dst = sandbox_key(destination)
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    store = build_compound_store(resolved_dst)
    result = Ferricstore.Commands.Set.handle("SUNIONSTORE", [resolved_dst | resolved_keys], store)
    wrap_result(result)
  end

  @doc """
  Returns the cardinality of the intersection of all given sets.

  When `limit` is provided and > 0, stops counting after reaching the limit.

  ## Options

    * `:limit` - Maximum count to return (0 means no limit, default 0).

  ## Examples

      {:ok, 3} = FerricStore.sintercard(["set1", "set2"])
      {:ok, 2} = FerricStore.sintercard(["set1", "set2"], limit: 2)

  """
  @spec sintercard([key()], keyword()) :: {:ok, non_neg_integer()}
  def sintercard(keys, opts \\ []) when is_list(keys) do
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    numkeys = Integer.to_string(length(resolved_keys))
    limit = Keyword.get(opts, :limit, 0)

    args =
      [numkeys | resolved_keys] ++
        if limit > 0, do: ["LIMIT", Integer.to_string(limit)], else: []

    store =
      case resolved_keys do
        [first | _] -> build_compound_store(first)
        [] -> build_compound_store("")
      end

    result = Ferricstore.Commands.Set.handle("SINTERCARD", args, store)
    wrap_result(result)
  end

  # Gathers all set members for a resolved key, routing to the correct shard.
  defp gather_set_members(resolved_key) do
    shard = Router.shard_name(Router.shard_for(resolved_key))
    prefix = Ferricstore.Store.CompoundKey.set_prefix(resolved_key)
    pairs = GenServer.call(shard, {:scan_prefix, prefix})
    MapSet.new(pairs, fn {member, _} -> member end)
  end

  # ---------------------------------------------------------------------------
  # Sorted Set extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Returns the rank of `member` in the sorted set at `key` (ascending order).

  ## Returns

    * `{:ok, rank}` (0-based) if the member exists.
    * `{:ok, nil}` if the member does not exist.

  ## Examples

      {:ok, 0} = FerricStore.zrank("zset", "alice")

  """
  @spec zrank(key(), binary()) :: {:ok, non_neg_integer() | nil}
  def zrank(key, member) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZRANK", [resolved_key, member], store)
    wrap_result(result)
  end

  @doc """
  Returns the reverse rank of `member` in the sorted set at `key` (descending order).

  ## Returns

    * `{:ok, rank}` (0-based) if the member exists.
    * `{:ok, nil}` if the member does not exist.

  ## Examples

      {:ok, 2} = FerricStore.zrevrank("zset", "alice")

  """
  @spec zrevrank(key(), binary()) :: {:ok, non_neg_integer() | nil}
  def zrevrank(key, member) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZREVRANK", [resolved_key, member], store)
    wrap_result(result)
  end

  @doc """
  Returns members with scores between `min` and `max` (inclusive by default).

  Supports "-inf" and "+inf" as score bounds.

  ## Options

    * `:withscores` - When `true`, returns `{member, score}` tuples.

  ## Returns

    * `{:ok, members}` - list of member strings.

  ## Examples

      {:ok, ["b", "c"]} = FerricStore.zrangebyscore("zset", "2", "3")

  """
  @spec zrangebyscore(key(), binary(), binary(), keyword()) :: {:ok, [binary()]}
  def zrangebyscore(key, min, max, opts \\ []) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZRANGEBYSCORE", [resolved_key, min, max], store)
    wrap_result(result)
  end

  @doc """
  Counts members in the sorted set at `key` with scores between `min` and `max`.

  Supports "-inf" and "+inf".

  ## Returns

    * `{:ok, count}` on success.

  ## Examples

      {:ok, 2} = FerricStore.zcount("zset", "2", "3")

  """
  @spec zcount(key(), binary(), binary()) :: {:ok, non_neg_integer()}
  def zcount(key, min, max) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZCOUNT", [resolved_key, min, max], store)
    wrap_result(result)
  end

  @doc """
  Increments the score of `member` in the sorted set at `key` by `increment`.

  Creates the member with the given score if it does not exist.

  ## Returns

    * `{:ok, new_score_string}` on success.

  ## Examples

      {:ok, "15.0"} = FerricStore.zincrby("zset", 5.0, "alice")

  """
  @spec zincrby(key(), number(), binary()) :: {:ok, binary()}
  def zincrby(key, increment, member) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZINCRBY", [resolved_key, to_string(increment * 1.0), member], store)
    wrap_result(result)
  end

  @doc """
  Returns one or more random members from the sorted set at `key`.

  Without `count`, returns a single member or `nil`.
  With `count`, returns a list of members.

  ## Examples

      {:ok, "alice"} = FerricStore.zrandmember("zset")

  """
  @spec zrandmember(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def zrandmember(key, count \\ nil) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)

    case count do
      nil ->
        result = Ferricstore.Commands.SortedSet.handle("ZRANDMEMBER", [resolved_key], store)
        wrap_result(result)

      n ->
        result = Ferricstore.Commands.SortedSet.handle("ZRANDMEMBER", [resolved_key, to_string(n)], store)
        wrap_result(result)
    end
  end

  @doc """
  Removes and returns up to `count` members with the lowest scores.

  ## Returns

    * `{:ok, [{member, score}, ...]}` on success.

  ## Examples

      {:ok, [{"alice", 1.0}]} = FerricStore.zpopmin("zset", 1)

  """
  @spec zpopmin(key(), pos_integer()) :: {:ok, [{binary(), float()}]}
  def zpopmin(key, count \\ 1) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZPOPMIN", [resolved_key, to_string(count)], store)

    case result do
      {:error, _} = err -> err
      flat when is_list(flat) ->
        pairs =
          flat
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)
        {:ok, pairs}
    end
  end

  @doc """
  Removes and returns up to `count` members with the highest scores.

  ## Returns

    * `{:ok, [{member, score}, ...]}` on success.

  ## Examples

      {:ok, [{"bob", 200.0}]} = FerricStore.zpopmax("zset", 1)

  """
  @spec zpopmax(key(), pos_integer()) :: {:ok, [{binary(), float()}]}
  def zpopmax(key, count \\ 1) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZPOPMAX", [resolved_key, to_string(count)], store)

    case result do
      {:error, _} = err -> err
      flat when is_list(flat) ->
        pairs =
          flat
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)
        {:ok, pairs}
    end
  end

  @doc """
  Returns scores for multiple members in the sorted set at `key`.

  Returns `nil` for members that do not exist.

  ## Returns

    * `{:ok, [score | nil, ...]}` on success.

  ## Examples

      {:ok, [1.0, nil]} = FerricStore.zmscore("zset", ["alice", "unknown"])

  """
  @spec zmscore(key(), [binary()]) :: {:ok, [float() | nil]}
  def zmscore(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.SortedSet.handle("ZMSCORE", [resolved_key | members], store)

    case result do
      {:error, _} = err -> err
      scores when is_list(scores) ->
        parsed = Enum.map(scores, fn
          nil -> nil
          score_str when is_binary(score_str) ->
            {score, _} = Float.parse(score_str)
            score
        end)
        {:ok, parsed}
    end
  end

  # ---------------------------------------------------------------------------
  # Streams
  # ---------------------------------------------------------------------------

  @doc """
  Adds an entry to the stream at `key` with auto-generated ID.

  `fields` is a flat list of field-value pairs: `["key1", "val1", "key2", "val2"]`.

  ## Returns

    * `{:ok, entry_id}` on success.

  ## Examples

      {:ok, "1234567-0"} = FerricStore.xadd("stream", ["name", "alice"])

  """
  @spec xadd(key(), [binary()]) :: {:ok, binary()} | {:error, binary()}
  def xadd(key, fields) when is_list(fields) do
    resolved_key = sandbox_key(key)
    store = build_stream_store(resolved_key)
    result = Ferricstore.Commands.Stream.handle("XADD", [resolved_key, "*" | fields], store)
    wrap_result(result)
  end

  @doc """
  Returns the number of entries in the stream at `key`.

  ## Returns

    * `{:ok, length}` on success.

  ## Examples

      {:ok, 5} = FerricStore.xlen("stream")

  """
  @spec xlen(key()) :: {:ok, non_neg_integer()}
  def xlen(key) do
    resolved_key = sandbox_key(key)
    store = build_stream_store(resolved_key)
    result = Ferricstore.Commands.Stream.handle("XLEN", [resolved_key], store)
    wrap_result(result)
  end

  @doc """
  Returns entries from the stream at `key` in forward order between `start` and `stop`.

  Use "-" for the minimum and "+" for the maximum stream IDs.

  ## Options

    * `:count` - Maximum number of entries to return.

  ## Returns

    * `{:ok, entries}` where entries is a list of `{id, [field, value, ...]}` tuples.

  ## Examples

      {:ok, entries} = FerricStore.xrange("stream", "-", "+", count: 10)

  """
  @spec xrange(key(), binary(), binary(), keyword()) :: {:ok, [tuple()]}
  def xrange(key, start, stop, opts \\ []) do
    resolved_key = sandbox_key(key)
    store = build_stream_store(resolved_key)
    count = Keyword.get(opts, :count)
    args = [resolved_key, start, stop]
    args = if count, do: args ++ ["COUNT", to_string(count)], else: args
    result = Ferricstore.Commands.Stream.handle("XRANGE", args, store)
    wrap_result(result)
  end

  @doc """
  Returns entries from the stream at `key` in reverse order between `stop` and `start`.

  ## Options

    * `:count` - Maximum number of entries to return.

  ## Returns

    * `{:ok, entries}` where entries is a list of `{id, [field, value, ...]}` tuples.

  ## Examples

      {:ok, entries} = FerricStore.xrevrange("stream", "+", "-")

  """
  @spec xrevrange(key(), binary(), binary(), keyword()) :: {:ok, [tuple()]}
  def xrevrange(key, stop, start, opts \\ []) do
    resolved_key = sandbox_key(key)
    store = build_stream_store(resolved_key)
    count = Keyword.get(opts, :count)
    args = [resolved_key, stop, start]
    args = if count, do: args ++ ["COUNT", to_string(count)], else: args
    result = Ferricstore.Commands.Stream.handle("XREVRANGE", args, store)
    wrap_result(result)
  end

  @doc """
  Trims the stream at `key` to a specified length.

  ## Options

    * `:maxlen` - Maximum number of entries to keep.

  ## Returns

    * `{:ok, trimmed_count}` on success.

  ## Examples

      {:ok, 5} = FerricStore.xtrim("stream", maxlen: 10)

  """
  @spec xtrim(key(), keyword()) :: {:ok, non_neg_integer()}
  def xtrim(key, opts) do
    resolved_key = sandbox_key(key)
    store = build_stream_store(resolved_key)
    maxlen = Keyword.fetch!(opts, :maxlen)
    result = Ferricstore.Commands.Stream.handle("XTRIM", [resolved_key, "MAXLEN", to_string(maxlen)], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Bloom Filter operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Bloom filter with specific error rate and capacity.

  ## Examples

      :ok = FerricStore.bf_reserve("filter", 0.01, 1000)

  """
  @spec bf_reserve(key(), float(), pos_integer()) :: :ok | {:error, binary()}
  def bf_reserve(key, error_rate, capacity) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    Ferricstore.Commands.Bloom.handle("BF.RESERVE", [resolved_key, to_string(error_rate), to_string(capacity)], store)
  end

  @doc """
  Adds an element to the Bloom filter at `key`, auto-creating if needed.

  ## Returns

    * `{:ok, 1}` if the element was added.
    * `{:ok, 0}` if the element was already present.

  ## Examples

      {:ok, 1} = FerricStore.bf_add("filter", "hello")

  """
  @spec bf_add(key(), binary()) :: {:ok, 0 | 1}
  def bf_add(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Bloom.handle("BF.ADD", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc """
  Adds multiple elements to the Bloom filter at `key`.

  ## Returns

    * `{:ok, [0 | 1, ...]}` for each element.

  """
  @spec bf_madd(key(), [binary()]) :: {:ok, [0 | 1]}
  def bf_madd(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Bloom.handle("BF.MADD", [resolved_key | elements], store)
    wrap_result(result)
  end

  @doc """
  Checks if an element may exist in the Bloom filter at `key`.

  ## Returns

    * `{:ok, 1}` if the element may exist.
    * `{:ok, 0}` if the element definitely does not exist.

  """
  @spec bf_exists(key(), binary()) :: {:ok, 0 | 1}
  def bf_exists(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Bloom.handle("BF.EXISTS", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc """
  Checks if multiple elements may exist in the Bloom filter at `key`.

  ## Returns

    * `{:ok, [0 | 1, ...]}` for each element.

  """
  @spec bf_mexists(key(), [binary()]) :: {:ok, [0 | 1]}
  def bf_mexists(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Bloom.handle("BF.MEXISTS", [resolved_key | elements], store)
    wrap_result(result)
  end

  @doc """
  Returns the cardinality (number of elements added) to the Bloom filter.

  ## Returns

    * `{:ok, count}` on success.

  """
  @spec bf_card(key()) :: {:ok, non_neg_integer()}
  def bf_card(key) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Bloom.handle("BF.CARD", [resolved_key], store)
    wrap_result(result)
  end

  @doc """
  Returns information about the Bloom filter at `key`.

  ## Returns

    * `{:ok, info_list}` on success.

  """
  @spec bf_info(key()) :: {:ok, list()} | {:error, binary()}
  def bf_info(key) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Bloom.handle("BF.INFO", [resolved_key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Cuckoo Filter operations
  # ---------------------------------------------------------------------------

  @doc "Creates a Cuckoo filter with specified capacity."
  @spec cf_reserve(key(), pos_integer()) :: :ok | {:error, binary()}
  def cf_reserve(key, capacity) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    Ferricstore.Commands.Cuckoo.handle("CF.RESERVE", [resolved_key, to_string(capacity)], store)
  end

  @doc "Adds an element to the Cuckoo filter, auto-creating if needed."
  @spec cf_add(key(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def cf_add(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.ADD", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc "Adds element only if not already present."
  @spec cf_addnx(key(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def cf_addnx(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.ADDNX", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc "Deletes one occurrence of element from the Cuckoo filter."
  @spec cf_del(key(), binary()) :: {:ok, 0 | 1}
  def cf_del(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.DEL", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc "Checks if element may exist in the Cuckoo filter."
  @spec cf_exists(key(), binary()) :: {:ok, 0 | 1}
  def cf_exists(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.EXISTS", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc "Checks multiple elements against the Cuckoo filter."
  @spec cf_mexists(key(), [binary()]) :: {:ok, [0 | 1]}
  def cf_mexists(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.MEXISTS", [resolved_key | elements], store)
    wrap_result(result)
  end

  @doc "Returns approximate count of element in the Cuckoo filter."
  @spec cf_count(key(), binary()) :: {:ok, non_neg_integer()}
  def cf_count(key, element) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.COUNT", [resolved_key, element], store)
    wrap_result(result)
  end

  @doc "Returns information about the Cuckoo filter."
  @spec cf_info(key()) :: {:ok, list()} | {:error, binary()}
  def cf_info(key) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.INFO", [resolved_key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Count-Min Sketch operations
  # ---------------------------------------------------------------------------

  @doc "Creates a Count-Min Sketch with given width and depth."
  @spec cms_initbydim(key(), pos_integer(), pos_integer()) :: :ok | {:error, binary()}
  def cms_initbydim(key, width, depth) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    Ferricstore.Commands.CMS.handle("CMS.INITBYDIM", [resolved_key, to_string(width), to_string(depth)], store)
  end

  @doc "Creates a Count-Min Sketch with target error rate and probability."
  @spec cms_initbyprob(key(), float(), float()) :: :ok | {:error, binary()}
  def cms_initbyprob(key, error, probability) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    Ferricstore.Commands.CMS.handle("CMS.INITBYPROB", [resolved_key, to_string(error), to_string(probability)], store)
  end

  @doc "Increments counts for element-count pairs in the CMS."
  @spec cms_incrby(key(), [{binary(), pos_integer()}]) :: {:ok, [non_neg_integer()]} | {:error, binary()}
  def cms_incrby(key, pairs) when is_list(pairs) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    args = Enum.flat_map(pairs, fn {element, count} -> [element, to_string(count)] end)
    result = Ferricstore.Commands.CMS.handle("CMS.INCRBY", [resolved_key | args], store)
    wrap_result(result)
  end

  @doc "Queries estimated counts for elements in the CMS."
  @spec cms_query(key(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, binary()}
  def cms_query(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.CMS.handle("CMS.QUERY", [resolved_key | elements], store)
    wrap_result(result)
  end

  @doc "Returns information about the CMS."
  @spec cms_info(key()) :: {:ok, list()} | {:error, binary()}
  def cms_info(key) do
    resolved_key = sandbox_key(key)
    store = build_prob_store(resolved_key)
    result = Ferricstore.Commands.CMS.handle("CMS.INFO", [resolved_key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # TopK operations
  # ---------------------------------------------------------------------------

  @doc "Creates a Top-K tracker with specified k."
  @spec topk_reserve(key(), pos_integer()) :: :ok | {:error, binary()}
  def topk_reserve(key, k) do
    resolved_key = sandbox_key(key)
    store = build_topk_store(resolved_key)
    Ferricstore.Commands.TopK.handle("TOPK.RESERVE", [resolved_key, to_string(k)], store)
  end

  @doc "Adds elements to the Top-K tracker."
  @spec topk_add(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def topk_add(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_topk_store(resolved_key)
    result = Ferricstore.Commands.TopK.handle("TOPK.ADD", [resolved_key | elements], store)
    wrap_result(result)
  end

  @doc "Queries whether elements are in the Top-K."
  @spec topk_query(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def topk_query(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_topk_store(resolved_key)
    result = Ferricstore.Commands.TopK.handle("TOPK.QUERY", [resolved_key | elements], store)
    wrap_result(result)
  end

  @doc "Lists the current Top-K elements."
  @spec topk_list(key()) :: {:ok, [binary()]} | {:error, binary()}
  def topk_list(key) do
    resolved_key = sandbox_key(key)
    store = build_topk_store(resolved_key)
    result = Ferricstore.Commands.TopK.handle("TOPK.LIST", [resolved_key], store)
    wrap_result(result)
  end

  @doc "Returns information about the Top-K tracker."
  @spec topk_info(key()) :: {:ok, list()} | {:error, binary()}
  def topk_info(key) do
    resolved_key = sandbox_key(key)
    store = build_topk_store(resolved_key)
    result = Ferricstore.Commands.TopK.handle("TOPK.INFO", [resolved_key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # T-Digest operations
  # ---------------------------------------------------------------------------

  @doc "Creates a T-Digest structure at `key`."
  @spec tdigest_create(key()) :: :ok | {:error, binary()}
  def tdigest_create(key) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    Ferricstore.Commands.TDigest.handle("TDIGEST.CREATE", [resolved_key], store)
  end

  @doc "Adds values to the T-Digest at `key`."
  @spec tdigest_add(key(), [number()]) :: :ok | {:error, binary()}
  def tdigest_add(key, values) when is_list(values) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    value_strs = Enum.map(values, &to_string(&1 * 1.0))
    Ferricstore.Commands.TDigest.handle("TDIGEST.ADD", [resolved_key | value_strs], store)
  end

  @doc "Estimates values at given quantiles."
  @spec tdigest_quantile(key(), [float()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_quantile(key, quantiles) when is_list(quantiles) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    qs = Enum.map(quantiles, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.QUANTILE", [resolved_key | qs], store)
    wrap_result(result)
  end

  @doc "Estimates CDF at given values."
  @spec tdigest_cdf(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_cdf(key, values) when is_list(values) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    vs = Enum.map(values, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.CDF", [resolved_key | vs], store)
    wrap_result(result)
  end

  @doc "Returns the minimum observed value."
  @spec tdigest_min(key()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_min(key) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.MIN", [resolved_key], store)
    wrap_result(result)
  end

  @doc "Returns the maximum observed value."
  @spec tdigest_max(key()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_max(key) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.MAX", [resolved_key], store)
    wrap_result(result)
  end

  @doc "Returns metadata about the T-Digest."
  @spec tdigest_info(key()) :: {:ok, list()} | {:error, binary()}
  def tdigest_info(key) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.INFO", [resolved_key], store)
    wrap_result(result)
  end

  @doc "Resets the T-Digest to empty."
  @spec tdigest_reset(key()) :: :ok | {:error, binary()}
  def tdigest_reset(key) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    Ferricstore.Commands.TDigest.handle("TDIGEST.RESET", [resolved_key], store)
  end

  @doc "Computes the trimmed mean between the given quantile bounds."
  @spec tdigest_trimmed_mean(key(), float(), float()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_trimmed_mean(key, lo, hi) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.TRIMMED_MEAN", [resolved_key, to_string(lo * 1.0), to_string(hi * 1.0)], store)
    wrap_result(result)
  end

  @doc "Estimates rank of values in the T-Digest."
  @spec tdigest_rank(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_rank(key, values) when is_list(values) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    vs = Enum.map(values, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.RANK", [resolved_key | vs], store)
    wrap_result(result)
  end

  @doc "Estimates reverse rank of values."
  @spec tdigest_revrank(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_revrank(key, values) when is_list(values) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    vs = Enum.map(values, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.REVRANK", [resolved_key | vs], store)
    wrap_result(result)
  end

  @doc "Estimates value at given rank."
  @spec tdigest_byrank(key(), [integer()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_byrank(key, ranks) when is_list(ranks) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    rs = Enum.map(ranks, &to_string/1)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.BYRANK", [resolved_key | rs], store)
    wrap_result(result)
  end

  @doc "Estimates value at given reverse rank."
  @spec tdigest_byrevrank(key(), [integer()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_byrevrank(key, ranks) when is_list(ranks) do
    resolved_key = sandbox_key(key)
    store = build_tdigest_store(resolved_key)
    rs = Enum.map(ranks, &to_string/1)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.BYREVRANK", [resolved_key | rs], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Vector operations
  # ---------------------------------------------------------------------------

  @doc "Creates a vector collection."
  @spec vcreate(binary(), pos_integer(), atom()) :: :ok | {:error, binary()}
  def vcreate(collection, dims, metric) do
    resolved = sandbox_key(collection)
    store = build_compound_store(resolved)
    metric_str = Atom.to_string(metric)
    result = Ferricstore.Commands.Vector.handle("VCREATE", [resolved, to_string(dims), metric_str], store)
    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc "Adds a vector to the collection."
  @spec vadd(binary(), binary(), [float()]) :: :ok | {:error, binary()}
  def vadd(collection, key, components) when is_list(components) do
    resolved = sandbox_key(collection)
    store = build_compound_store(resolved)
    comp_strs = Enum.map(components, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.Vector.handle("VADD", [resolved, key | comp_strs], store)
    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc "Deletes a vector from the collection."
  @spec vdel(binary(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def vdel(collection, key) do
    resolved = sandbox_key(collection)
    store = build_compound_store(resolved)
    result = Ferricstore.Commands.Vector.handle("VDEL", [resolved, key], store)
    wrap_result(result)
  end

  @doc "Searches for nearest neighbors in the collection."
  @spec vsearch(binary(), [float()], pos_integer()) :: {:ok, list()} | {:error, binary()}
  def vsearch(collection, query_vector, k) when is_list(query_vector) do
    resolved = sandbox_key(collection)
    store = build_compound_store(resolved)
    comp_strs = Enum.map(query_vector, &to_string(&1 * 1.0))
    # VSEARCH format: collection f32... TOP k
    args = [resolved | comp_strs] ++ ["TOP", to_string(k)]
    result = Ferricstore.Commands.Vector.handle("VSEARCH", args, store)
    wrap_result(result)
  end

  @doc "Returns metadata about the vector collection."
  @spec vinfo(binary()) :: {:ok, list()} | {:error, binary()}
  def vinfo(collection) do
    resolved = sandbox_key(collection)
    store = build_compound_store(resolved)
    result = Ferricstore.Commands.Vector.handle("VINFO", [resolved], store)
    wrap_result(result)
  end

  @doc "Returns the number of vectors in the collection."
  @spec vcard(binary()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def vcard(collection) do
    resolved = sandbox_key(collection)
    store = build_compound_store(resolved)
    result = Ferricstore.Commands.Vector.handle("VINFO", [resolved], store)
    case result do
      {:error, _} = err -> err
      info when is_list(info) ->
        count_idx = Enum.find_index(info, &(&1 == "vector_count"))
        if count_idx, do: {:ok, Enum.at(info, count_idx + 1)}, else: {:ok, 0}
    end
  end

  # ---------------------------------------------------------------------------
  # Geo operations
  # ---------------------------------------------------------------------------

  @doc "Adds geospatial members to the sorted set at `key`."
  @spec geoadd(key(), [{number(), number(), binary()}]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def geoadd(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    args = Enum.flat_map(members, fn {lng, lat, member} ->
      [to_string(lng * 1.0), to_string(lat * 1.0), member]
    end)
    result = Ferricstore.Commands.Geo.handle("GEOADD", [resolved_key | args], store)
    wrap_result(result)
  end

  @doc "Returns the distance between two members."
  @spec geodist(key(), binary(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def geodist(key, member1, member2, unit \\ "m") do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Geo.handle("GEODIST", [resolved_key, member1, member2, unit], store)
    wrap_result(result)
  end

  @doc "Returns geohash strings for members."
  @spec geohash(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def geohash(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Geo.handle("GEOHASH", [resolved_key | members], store)
    wrap_result(result)
  end

  @doc "Returns positions (longitude, latitude) for members."
  @spec geopos(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def geopos(key, members) when is_list(members) do
    resolved_key = sandbox_key(key)
    store = build_compound_store(resolved_key)
    result = Ferricstore.Commands.Geo.handle("GEOPOS", [resolved_key | members], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # JSON operations
  # ---------------------------------------------------------------------------

  @doc "Sets a JSON value at path in the JSON document at `key`."
  @spec json_set(key(), binary(), binary()) :: :ok | {:error, binary()}
  def json_set(key, path, value) do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.SET", [resolved_key, path, value], store)
    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc "Gets the JSON value at path from the document at `key`."
  @spec json_get(key(), binary()) :: {:ok, binary()} | {:error, binary()}
  def json_get(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.GET", [resolved_key, path], store)
    wrap_result(result)
  end

  @doc "Deletes a JSON path from the document at `key`."
  @spec json_del(key(), binary()) :: {:ok, term()} | {:error, binary()}
  def json_del(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.DEL", [resolved_key, path], store)
    wrap_result(result)
  end

  @doc "Returns the JSON type at path."
  @spec json_type(key(), binary()) :: {:ok, binary()} | {:error, binary()}
  def json_type(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.TYPE", [resolved_key, path], store)
    wrap_result(result)
  end

  @doc "Increments a number value at a JSON path."
  @spec json_numincrby(key(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def json_numincrby(key, path, increment) do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.NUMINCRBY", [resolved_key, path, increment], store)
    wrap_result(result)
  end

  @doc "Appends values to a JSON array at path."
  @spec json_arrappend(key(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def json_arrappend(key, path, values) when is_list(values) do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.ARRAPPEND", [resolved_key, path | values], store)
    wrap_result(result)
  end

  @doc "Returns the length of a JSON array at path."
  @spec json_arrlen(key(), binary()) :: {:ok, integer()} | {:error, binary()}
  def json_arrlen(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.ARRLEN", [resolved_key, path], store)
    wrap_result(result)
  end

  @doc "Returns the length of a JSON string at path."
  @spec json_strlen(key(), binary()) :: {:ok, integer()} | {:error, binary()}
  def json_strlen(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.STRLEN", [resolved_key, path], store)
    wrap_result(result)
  end

  @doc "Returns the keys of a JSON object at path."
  @spec json_objkeys(key(), binary()) :: {:ok, list()} | {:error, binary()}
  def json_objkeys(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.OBJKEYS", [resolved_key, path], store)
    wrap_result(result)
  end

  @doc "Returns the number of keys in a JSON object at path."
  @spec json_objlen(key(), binary()) :: {:ok, integer()} | {:error, binary()}
  def json_objlen(key, path \\ "$") do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.Json.handle("JSON.OBJLEN", [resolved_key, path], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Native: lock, unlock, extend, ratelimit_add
  # ---------------------------------------------------------------------------

  @doc "Acquires a distributed lock on `key` with the given owner and TTL."
  @spec lock(key(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(key, owner, ttl_ms) do
    resolved_key = sandbox_key(key)
    case Router.lock(resolved_key, owner, ttl_ms) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc "Releases a lock on `key` if held by `owner`."
  @spec unlock(key(), binary()) :: {:ok, 1} | {:error, binary()}
  def unlock(key, owner) do
    resolved_key = sandbox_key(key)
    case Router.unlock(resolved_key, owner) do
      1 -> {:ok, 1}
      {:error, _} = err -> err
    end
  end

  @doc "Extends the TTL of a lock on `key` if held by `owner`."
  @spec extend(key(), binary(), pos_integer()) :: {:ok, 1} | {:error, binary()}
  def extend(key, owner, ttl_ms) do
    resolved_key = sandbox_key(key)
    case Router.extend(resolved_key, owner, ttl_ms) do
      1 -> {:ok, 1}
      {:error, _} = err -> err
    end
  end

  @doc "Adds a count to the sliding window rate limiter at `key`."
  @spec ratelimit_add(key(), pos_integer(), pos_integer(), pos_integer()) :: {:ok, list()}
  def ratelimit_add(key, window_ms, max, count \\ 1) do
    resolved_key = sandbox_key(key)
    result = Router.ratelimit_add(resolved_key, window_ms, max, count)
    {:ok, result}
  end

  # ---------------------------------------------------------------------------
  # HyperLogLog operations
  # ---------------------------------------------------------------------------

  @doc "Adds elements to the HyperLogLog at `key`."
  @spec pfadd(key(), [binary()]) :: {:ok, boolean()} | {:error, binary()}
  def pfadd(key, elements) when is_list(elements) do
    resolved_key = sandbox_key(key)
    store = build_string_store(resolved_key)
    result = Ferricstore.Commands.HyperLogLog.handle("PFADD", [resolved_key | elements], store)
    case result do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc "Returns the approximate cardinality of the union of HyperLogLogs."
  @spec pfcount([key()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def pfcount(keys) when is_list(keys) do
    resolved_keys = Enum.map(keys, &sandbox_key/1)
    store = build_string_store(hd(resolved_keys))
    result = Ferricstore.Commands.HyperLogLog.handle("PFCOUNT", resolved_keys, store)
    wrap_result(result)
  end

  @doc "Merges multiple HyperLogLogs into `dest_key`."
  @spec pfmerge(key(), [key()]) :: :ok | {:error, binary()}
  def pfmerge(dest_key, source_keys) when is_list(source_keys) do
    resolved_dest = sandbox_key(dest_key)
    resolved_sources = Enum.map(source_keys, &sandbox_key/1)
    store = build_string_store(resolved_dest)
    result = Ferricstore.Commands.HyperLogLog.handle("PFMERGE", [resolved_dest | resolved_sources], store)
    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  # ---------------------------------------------------------------------------
  # Multi/Tx
  # ---------------------------------------------------------------------------

  @doc """
  Executes a sequence of commands atomically as a transaction.

  The provided function receives a `FerricStore.Tx` accumulator and should
  pipe commands into it. All commands execute in order and results are returned.

  ## Examples

      {:ok, [:ok, {:ok, "v1"}]} = FerricStore.multi(fn tx ->
        tx
        |> FerricStore.Tx.set("k1", "v1")
        |> FerricStore.Tx.get("k1")
      end)

  """
  @spec multi((FerricStore.Tx.t() -> FerricStore.Tx.t())) :: {:ok, [term()]} | {:error, binary()}
  def multi(fun) when is_function(fun, 1) do
    tx = fun.(FerricStore.Tx.new())

    case FerricStore.Tx.execute(tx) do
      {:error, _} = err -> err
      results when is_list(results) -> {:ok, results}
    end
  end

  # ---------------------------------------------------------------------------
  # Server: ping, echo, flushall
  # ---------------------------------------------------------------------------

  @doc "Returns `{:ok, \"PONG\"}`."
  @spec ping() :: {:ok, binary()}
  def ping, do: {:ok, "PONG"}

  @doc "Echoes back the given message."
  @spec echo(binary()) :: {:ok, binary()}
  def echo(message) when is_binary(message), do: {:ok, message}

  @doc "Deletes all keys. In sandbox mode, only sandbox keys are deleted."
  @spec flushall() :: :ok
  def flushall, do: flushdb()

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

  @sandbox_enabled Application.compile_env(:ferricstore, :sandbox_enabled, false)

  @doc false
  @spec sandbox_key(binary()) :: binary()

  if @sandbox_enabled do
    def sandbox_key(key) do
      case Process.get(:ferricstore_sandbox) do
        nil -> key
        namespace -> namespace <> key
      end
    end
  else
    def sandbox_key(key), do: key
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
  # Private — result wrapping helper
  # ---------------------------------------------------------------------------

  defp wrap_result({:error, _} = err), do: err
  defp wrap_result(result), do: {:ok, result}

  # ---------------------------------------------------------------------------
  # Private — string store builder for bitmap/json/hyperloglog operations
  # ---------------------------------------------------------------------------

  defp build_string_store(resolved_key) do
    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      incr: &Router.incr/2,
      incr_float: &Router.incr_float/2,
      append: &Router.append/2,
      getset: &Router.getset/2,
      getdel: &Router.getdel/1,
      getex: &Router.getex/2,
      setrange: &Router.setrange/3,
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
  # Private — stream store builder
  # ---------------------------------------------------------------------------

  defp build_stream_store(resolved_key) do
    build_string_store(resolved_key)
  end

  # ---------------------------------------------------------------------------
  # Private — probabilistic structure store builder
  # ---------------------------------------------------------------------------

  defp build_prob_store(resolved_key) do
    # Probabilistic structures (Bloom, Cuckoo, CMS) use per-shard ETS registry
    # tables. The command handlers resolve the shard index from the key via
    # Router.shard_for/1, so we just need the basic store callbacks.
    # Ensure that the per-shard registry ETS tables exist.
    index = Router.shard_for(resolved_key)
    ensure_prob_registry_tables(index)

    %{
      get: &Router.get/1,
      get_meta: &Router.get_meta/1,
      put: &Router.put/3,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0
    }
  end

  defp ensure_prob_registry_tables(index) do
    # Ensure Cuckoo registry table exists
    cuckoo_name = :"cuckoo_reg_#{index}"
    case :ets.whereis(cuckoo_name) do
      :undefined ->
        try do
          :ets.new(cuckoo_name, [:set, :public, :named_table, {:read_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end
      _ -> :ok
    end

    # Ensure CMS registry table exists
    cms_name = :"cms_reg_#{index}"
    case :ets.whereis(cms_name) do
      :undefined ->
        try do
          :ets.new(cms_name, [:set, :public, :named_table, {:read_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end
      _ -> :ok
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private — TopK store builder
  # ---------------------------------------------------------------------------

  defp build_topk_store(resolved_key) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    index = Router.shard_for(resolved_key)
    prob_dir = Path.join([data_dir, "prob", "shard_#{index}"])

    %{
      get: fn key ->
        case Router.get(key) do
          nil -> nil
          bin when is_binary(bin) ->
            try do
              :erlang.binary_to_term(bin)
            rescue
              ArgumentError -> bin
            end
        end
      end,
      put: fn key, val, exp ->
        encoded = if is_tuple(val), do: :erlang.term_to_binary(val), else: val
        Router.put(key, encoded, exp)
      end,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      keys: &Router.keys/0,
      prob_dir: fn -> File.mkdir_p!(prob_dir); prob_dir end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — TDigest store builder
  # ---------------------------------------------------------------------------

  defp build_tdigest_store(_resolved_key) do
    %{
      get: fn key ->
        case Router.get(key) do
          nil -> nil
          bin when is_binary(bin) ->
            try do
              case :erlang.binary_to_term(bin) do
                {:tdigest, _, _} = tuple -> tuple
                _ -> bin
              end
            rescue
              ArgumentError -> bin
            end
        end
      end,
      put: fn key, val, exp ->
        encoded = if is_tuple(val) and tuple_size(val) >= 1 and elem(val, 0) == :tdigest do
          :erlang.term_to_binary(val)
        else
          val
        end
        Router.put(key, encoded, exp)
      end,
      delete: &Router.delete/1,
      exists?: fn key ->
        Router.get(key) != nil
      end,
      keys: &Router.keys/0
    }
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

end

defmodule FerricStore.Pipe do
  @moduledoc """
  Pipeline accumulator for batching multiple FerricStore commands.

  Used with `FerricStore.pipeline/1` to batch multiple operations into a single
  Raft entry per shard. Commands are accumulated in reverse order and on execute,
  converted to RESP tuples and dispatched through the Coordinator. Single-shard
  pipelines commit in one Raft round-trip; cross-shard pipelines use the
  anchor-shard mechanism.

  Results are normalized to match the FerricStore public API format (e.g.
  `{:ok, value}` for GET, `:ok` for DEL) rather than raw Dispatcher values.

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
  Executes all accumulated pipeline commands as a single batch Raft entry
  per shard via the Coordinator.

  This is called internally by `FerricStore.pipeline/1`. Commands are converted
  to RESP-style tuples and dispatched through `Ferricstore.Transaction.Coordinator`,
  which groups them by shard and submits each group as a single `{:batch}` or
  `{:tx_execute}` Raft entry. Single-shard pipelines commit in one Raft round-trip;
  cross-shard pipelines use the anchor-shard mechanism.

  Results are returned in the original command order.
  """
  @spec execute(t()) :: [term()]
  def execute(%__MODULE__{commands: []}), do: []

  def execute(%__MODULE__{commands: commands}) do
    ordered = Enum.reverse(commands)

    queue = Enum.map(ordered, &to_resp_command/1)

    sandbox_namespace =
      case Process.get(:ferricstore_sandbox) do
        nil -> nil
        ns -> ns
      end

    raw_results = Ferricstore.Transaction.Coordinator.execute(queue, %{}, sandbox_namespace)

    ordered
    |> Enum.zip(raw_results)
    |> Enum.map(fn {cmd, raw} -> normalize_result(cmd, raw) end)
  end

  # The Coordinator returns raw Dispatcher results (RESP-level values).
  # Pipeline callers expect the same format as FerricStore public API calls.
  # This maps Dispatcher results back to the public API format.
  defp normalize_result({:get, _}, {:error, _} = err), do: err
  defp normalize_result({:get, _}, value), do: {:ok, value}

  defp normalize_result({:hget, _, _}, {:error, _} = err), do: err
  defp normalize_result({:hget, _, _}, value), do: {:ok, value}

  defp normalize_result({:del, _}, {:error, _} = err), do: err
  defp normalize_result({:del, _}, _count), do: :ok

  defp normalize_result({:hset, _, _}, {:error, _} = err), do: err
  defp normalize_result({:hset, _, _}, _count), do: :ok

  defp normalize_result({:lpush, _, _}, {:error, _} = err), do: err
  defp normalize_result({:lpush, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:rpush, _, _}, {:error, _} = err), do: err
  defp normalize_result({:rpush, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:sadd, _, _}, {:error, _} = err), do: err
  defp normalize_result({:sadd, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:zadd, _, _}, {:error, _} = err), do: err
  defp normalize_result({:zadd, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:expire, _, _}, {:error, _} = err), do: err
  defp normalize_result({:expire, _, _}, 1), do: {:ok, true}
  defp normalize_result({:expire, _, _}, 0), do: {:ok, false}

  # SET, INCR, INCR_BY already return the correct format from Dispatcher
  defp normalize_result(_, result), do: result

  defp to_resp_command({:set, key, value, opts}) do
    args = [key, value]

    args =
      case Keyword.get(opts, :ttl) do
        nil -> args
        0 -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      case Keyword.get(opts, :ex) do
        nil -> args
        seconds -> args ++ ["EX", Integer.to_string(seconds)]
      end

    args =
      case Keyword.get(opts, :px) do
        nil -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      if Keyword.get(opts, :nx, false), do: args ++ ["NX"], else: args

    args =
      if Keyword.get(opts, :xx, false), do: args ++ ["XX"], else: args

    {"SET", args}
  end

  defp to_resp_command({:get, key}), do: {"GET", [key]}
  defp to_resp_command({:del, key}), do: {"DEL", [key]}
  defp to_resp_command({:incr, key}), do: {"INCR", [key]}
  defp to_resp_command({:incr_by, key, amount}), do: {"INCRBY", [key, Integer.to_string(amount)]}

  defp to_resp_command({:hset, key, fields}) do
    flat = Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)
    {"HSET", [key | flat]}
  end

  defp to_resp_command({:hget, key, field}), do: {"HGET", [key, field]}
  defp to_resp_command({:lpush, key, elements}), do: {"LPUSH", [key | elements]}
  defp to_resp_command({:rpush, key, elements}), do: {"RPUSH", [key | elements]}
  defp to_resp_command({:sadd, key, members}), do: {"SADD", [key | members]}

  defp to_resp_command({:zadd, key, pairs}) do
    flat = Enum.flat_map(pairs, fn {score, member} ->
      [to_string(score), member]
    end)
    {"ZADD", [key | flat]}
  end

  defp to_resp_command({:expire, key, ttl_ms}) do
    {"PEXPIRE", [key, Integer.to_string(ttl_ms)]}
  end
end

defmodule FerricStore.Tx do
  @moduledoc """
  Transaction accumulator for executing multiple FerricStore commands atomically.

  Used with `FerricStore.multi/1` to batch multiple operations. Commands are
  accumulated in reverse order and executed sequentially when the transaction
  completes.

  ## Usage

      FerricStore.multi(fn tx ->
        tx
        |> FerricStore.Tx.set("key1", "val1")
        |> FerricStore.Tx.get("key1")
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

  @doc "Creates a new empty transaction."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds a SET command to the transaction."
  @spec set(t(), binary(), binary(), keyword()) :: t()
  def set(%__MODULE__{} = tx, key, value, opts \\ []) do
    %{tx | commands: [{:set, key, value, opts} | tx.commands]}
  end

  @doc "Adds a GET command to the transaction."
  @spec get(t(), binary()) :: t()
  def get(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:get, key} | tx.commands]}
  end

  @doc "Adds a DEL command to the transaction."
  @spec del(t(), binary()) :: t()
  def del(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:del, key} | tx.commands]}
  end

  @doc "Adds an INCR command to the transaction."
  @spec incr(t(), binary()) :: t()
  def incr(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:incr, key} | tx.commands]}
  end

  @doc "Adds an INCRBY command to the transaction."
  @spec incr_by(t(), binary(), integer()) :: t()
  def incr_by(%__MODULE__{} = tx, key, amount) do
    %{tx | commands: [{:incr_by, key, amount} | tx.commands]}
  end

  @doc "Adds an HSET command to the transaction."
  @spec hset(t(), binary(), map()) :: t()
  def hset(%__MODULE__{} = tx, key, fields) do
    %{tx | commands: [{:hset, key, fields} | tx.commands]}
  end

  @doc "Adds an HGET command to the transaction."
  @spec hget(t(), binary(), binary()) :: t()
  def hget(%__MODULE__{} = tx, key, field) do
    %{tx | commands: [{:hget, key, field} | tx.commands]}
  end

  @doc "Adds an LPUSH command to the transaction."
  @spec lpush(t(), binary(), [binary()]) :: t()
  def lpush(%__MODULE__{} = tx, key, elements) do
    %{tx | commands: [{:lpush, key, elements} | tx.commands]}
  end

  @doc "Adds an RPUSH command to the transaction."
  @spec rpush(t(), binary(), [binary()]) :: t()
  def rpush(%__MODULE__{} = tx, key, elements) do
    %{tx | commands: [{:rpush, key, elements} | tx.commands]}
  end

  @doc "Adds a SADD command to the transaction."
  @spec sadd(t(), binary(), [binary()]) :: t()
  def sadd(%__MODULE__{} = tx, key, members) do
    %{tx | commands: [{:sadd, key, members} | tx.commands]}
  end

  @doc "Adds a ZADD command to the transaction."
  @spec zadd(t(), binary(), [{number(), binary()}]) :: t()
  def zadd(%__MODULE__{} = tx, key, score_member_pairs) do
    %{tx | commands: [{:zadd, key, score_member_pairs} | tx.commands]}
  end

  @doc "Adds an EXPIRE command to the transaction."
  @spec expire(t(), binary(), non_neg_integer()) :: t()
  def expire(%__MODULE__{} = tx, key, ttl_ms) do
    %{tx | commands: [{:expire, key, ttl_ms} | tx.commands]}
  end

  @doc """
  Executes all accumulated transaction commands atomically.

  Groups commands by shard. If all target a single shard, dispatches them
  as a batch to the shard GenServer (atomic, no interleaving). If commands
  span multiple shards, returns a CROSSSLOT error.
  """
  @spec execute(t()) :: [term()] | {:error, binary()}
  def execute(%__MODULE__{commands: []}), do: []

  def execute(%__MODULE__{commands: commands}) do
    queue =
      commands
      |> Enum.reverse()
      |> Enum.map(&to_resp_command/1)

    sandbox_namespace =
      case Process.get(:ferricstore_sandbox) do
        nil -> nil
        ns -> ns
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, sandbox_namespace)
  end

  defp to_resp_command({:set, key, value, opts}) do
    args = [key, value]

    args =
      case Keyword.get(opts, :ex) do
        nil -> args
        seconds -> args ++ ["EX", Integer.to_string(seconds)]
      end

    args =
      case Keyword.get(opts, :px) do
        nil -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      if Keyword.get(opts, :nx, false), do: args ++ ["NX"], else: args

    args =
      if Keyword.get(opts, :xx, false), do: args ++ ["XX"], else: args

    {"SET", args}
  end

  defp to_resp_command({:get, key}), do: {"GET", [key]}
  defp to_resp_command({:del, key}), do: {"DEL", [key]}
  defp to_resp_command({:incr, key}), do: {"INCR", [key]}
  defp to_resp_command({:incr_by, key, amount}), do: {"INCRBY", [key, Integer.to_string(amount)]}

  defp to_resp_command({:hset, key, fields}) do
    flat = Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)
    {"HSET", [key | flat]}
  end

  defp to_resp_command({:hget, key, field}), do: {"HGET", [key, field]}
  defp to_resp_command({:lpush, key, elements}), do: {"LPUSH", [key | elements]}
  defp to_resp_command({:rpush, key, elements}), do: {"RPUSH", [key | elements]}
  defp to_resp_command({:sadd, key, members}), do: {"SADD", [key | members]}

  defp to_resp_command({:zadd, key, pairs}) do
    flat = Enum.flat_map(pairs, fn {score, member} ->
      [to_string(score), member]
    end)
    {"ZADD", [key | flat]}
  end

  defp to_resp_command({:expire, key, ttl_ms}) do
    {"PEXPIRE", [key, Integer.to_string(ttl_ms)]}
  end
end
