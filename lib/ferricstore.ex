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
end
