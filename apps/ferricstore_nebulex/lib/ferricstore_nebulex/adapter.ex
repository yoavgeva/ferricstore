defmodule FerricstoreNebulex.Adapter do
  @moduledoc """
  Nebulex 3.x adapter backed by FerricStore.

  Provides persistent, crash-recoverable caching with LFU eviction and optional
  Raft-based replication. Drop-in replacement for `Nebulex.Adapters.Local`.

  ## Setup

      defmodule MyApp.Cache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: FerricstoreNebulex.Adapter
      end

      # config/config.exs
      config :my_app, MyApp.Cache,
        prefix: "nbx",            # key prefix in FerricStore (default: "nbx")
        serializer: :erlang_term   # :erlang_term (default) or :jason

  ## Configuration Options

  * `:prefix` - Key prefix prepended to all keys stored in FerricStore.
    Defaults to `"nbx"`. Keys are stored as `"{prefix}:{key}"`. Use different
    prefixes to namespace multiple caches in the same FerricStore instance.

  * `:serializer` - Value serialization format. Defaults to `:erlang_term`.
    - `:erlang_term` -- uses `:erlang.term_to_binary/2` with `:compressed`.
      Supports any Elixir term (structs, tuples, atoms, pids, etc.).
    - `:jason` -- uses `Jason.encode!/1`. Limited to JSON-compatible types
      (maps, lists, strings, numbers, booleans, nil). Useful when non-Elixir
      clients need to read cached values.

  ## Behaviours Implemented

  This adapter implements the following Nebulex behaviours:

  * `Nebulex.Adapter` -- lifecycle (`init/1`)
  * `Nebulex.Adapter.KV` -- `fetch`, `put`, `put_all`, `delete`, `take`,
    `has_key?`, `ttl`, `expire`, `touch`, `update_counter`
  * `Nebulex.Adapter.Queryable` -- `execute` (`get_all`, `count_all`,
    `delete_all`), `stream`
  * `Nebulex.Adapter.Transaction` -- `transaction`, `in_transaction?`

  ## Key Encoding

  Binary keys (strings) are stored as-is. Non-binary keys (atoms, integers,
  tuples) are serialized with `:erlang.term_to_binary/1` and Base64-encoded
  with a `0xFF` marker byte prefix so that `decode_key` can reliably
  distinguish them from plain binary keys. This adds roughly 1 microsecond
  overhead per operation for non-binary keys.

      # All key types work transparently
      MyApp.Cache.put("string_key", 1)    # stored as "nbx:string_key"
      MyApp.Cache.put(:atom_key, 2)       # stored as "nbx:\\xFF<base64>"
      MyApp.Cache.put({:user, 42}, 3)     # stored as "nbx:\\xFF<base64>"

  ## Value Serialization

  All values are serialized before storing and deserialized on read. The
  serializer is selected via the `:serializer` config option.

  With the default `:erlang_term` serializer, any Elixir term can be cached:

      MyApp.Cache.put("user", %User{id: 42, name: "Alice"})
      MyApp.Cache.put("config", %{retries: 3, timeout: 5000})
      MyApp.Cache.put("tags", [:elixir, :otp])

  ## TTL

  FerricStore supports millisecond-precision TTL natively. The adapter maps
  Nebulex TTL semantics directly to FerricStore's expiry system:

  * `:infinity` or `nil` -- no expiration
  * Positive integer -- TTL in milliseconds
  * `:keep_ttl` flag -- preserves existing TTL on update

  ## Write Modes

  The `put/7` callback supports three write modes via the `on_write` argument:

  * `:put` -- unconditional write (default). Always stores the value.
  * `:put_new` -- write only if the key does not already exist.
    Uses `FerricStore.setnx/2` for atomicity.
  * `:replace` -- write only if the key already exists.

  ## Queryable Operations

  The adapter supports the following query patterns:

  * `get_all` with `{:in, keys}` -- bulk fetch specific keys
  * `get_all` with `{:q, nil}` -- fetch all entries (prefix scan)
  * `count_all` with `{:in, keys}` or `{:q, nil}` -- count entries
  * `delete_all` with `{:in, keys}` or `{:q, nil}` -- delete entries
  * `stream` with `{:q, nil}` -- lazy enumeration of all entries

  ## Transactions

  FerricStore operations are individually atomic (each command is serialized
  through its shard's GenServer), but there is no multi-key transaction
  support. The `transaction/3` implementation provides a process-level
  marker so `in_transaction?/2` works correctly, and nested transactions
  are flattened (inner transaction runs the function directly).
  """

  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable
  @behaviour Nebulex.Adapter.Transaction

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  alias Nebulex.Adapters.Common.Info.Stats

  # ---------------------------------------------------------------------------
  # Nebulex.Adapter callbacks
  # ---------------------------------------------------------------------------

  @impl Nebulex.Adapter
  defmacro __before_compile__(_env), do: :ok

  @doc """
  Initializes the adapter.

  Returns adapter metadata containing the key prefix, serializer, and stats
  counter. No child processes are started because FerricStore runs its own
  supervision tree.
  """
  @impl Nebulex.Adapter
  @spec init(keyword()) :: {:ok, :supervisor.child_spec(), map()}
  def init(opts) do
    prefix = Keyword.get(opts, :prefix, "nbx")
    serializer = Keyword.get(opts, :serializer, :erlang_term)
    telemetry_prefix = Keyword.fetch!(opts, :telemetry_prefix)

    # Nebulex requires a child_spec even if the adapter has no children.
    # Use a no-op Agent as a placeholder (same pattern as Nebulex.Adapters.Nil).
    child_spec = Supervisor.child_spec({Agent, fn -> :ok end}, id: {__MODULE__, prefix})

    adapter_meta = %{
      prefix: prefix,
      serializer: serializer,
      stats_counter: Stats.init(telemetry_prefix)
    }

    {:ok, child_spec, adapter_meta}
  end

  # ---------------------------------------------------------------------------
  # Nebulex.Adapter.KV callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Fetches the value for `key` from FerricStore.

  Returns `{:ok, value}` on hit or `{:error, %Nebulex.KeyError{}}` on miss.
  """
  @impl Nebulex.Adapter.KV
  @spec fetch(map(), any(), keyword()) :: {:ok, any()} | {:error, Nebulex.KeyError.t()}
  def fetch(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    case FerricStore.get(fkey) do
      {:ok, nil} -> {:error, %Nebulex.KeyError{key: key}}
      {:ok, bin} -> {:ok, deserialize(adapter_meta, bin)}
    end
  end

  @doc """
  Stores `value` under `key` in FerricStore.

  Supports `:put`, `:put_new`, and `:replace` write modes via the `on_write`
  argument. Returns `{:ok, true}` on success, `{:ok, false}` when the write
  condition was not met.
  """
  @impl Nebulex.Adapter.KV
  @spec put(map(), any(), any(), :put | :put_new | :replace, timeout(), boolean(), keyword()) ::
          {:ok, boolean()}
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl, _opts) do
    fkey = full_key(adapter_meta, key)
    bin = serialize(adapter_meta, value)
    ttl_ms = normalize_ttl(ttl)

    case on_write do
      :put ->
        if keep_ttl do
          FerricStore.set(fkey, bin, keepttl: true)
        else
          FerricStore.set(fkey, bin, ttl: ttl_ms)
        end

        {:ok, true}

      :put_new ->
        case FerricStore.setnx(fkey, bin) do
          {:ok, true} ->
            if ttl_ms > 0, do: FerricStore.expire(fkey, ttl_ms)
            {:ok, true}

          {:ok, false} ->
            {:ok, false}
        end

      :replace ->
        if FerricStore.exists(fkey) do
          FerricStore.set(fkey, bin, ttl: ttl_ms)
          {:ok, true}
        else
          {:ok, false}
        end
    end
  end

  @doc """
  Stores multiple key-value entries in FerricStore.

  When `on_write` is `:put_new`, all entries are stored only if none of the
  keys already exist.
  """
  @impl Nebulex.Adapter.KV
  @spec put_all(map(), [{any(), any()}], :put | :put_new, timeout(), keyword()) ::
          {:ok, boolean()}
  def put_all(adapter_meta, entries, on_write, ttl, _opts) do
    ttl_ms = normalize_ttl(ttl)

    case on_write do
      :put ->
        Enum.each(entries, fn {key, value} ->
          fkey = full_key(adapter_meta, key)
          bin = serialize(adapter_meta, value)
          FerricStore.set(fkey, bin, ttl: ttl_ms)
        end)

        {:ok, true}

      :put_new ->
        fentries = Enum.map(entries, fn {k, v} -> {full_key(adapter_meta, k), v} end)
        any_exists = Enum.any?(fentries, fn {fkey, _} -> FerricStore.exists(fkey) end)

        if any_exists do
          {:ok, false}
        else
          Enum.each(fentries, fn {fkey, value} ->
            bin = serialize(adapter_meta, value)
            FerricStore.set(fkey, bin, ttl: ttl_ms)
          end)

          {:ok, true}
        end
    end
  end

  @doc """
  Deletes `key` from FerricStore.
  """
  @impl Nebulex.Adapter.KV
  @spec delete(map(), any(), keyword()) :: :ok
  def delete(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)
    FerricStore.del(fkey)
    :ok
  end

  @doc """
  Atomically gets and deletes `key` from FerricStore.

  Returns `{:ok, value}` on hit or `{:error, %Nebulex.KeyError{}}` on miss.
  """
  @impl Nebulex.Adapter.KV
  @spec take(map(), any(), keyword()) :: {:ok, any()} | {:error, Nebulex.KeyError.t()}
  def take(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    case FerricStore.getdel(fkey) do
      {:ok, nil} -> {:error, %Nebulex.KeyError{key: key}}
      {:ok, bin} -> {:ok, deserialize(adapter_meta, bin)}
    end
  end

  @doc """
  Checks whether `key` exists in FerricStore.
  """
  @impl Nebulex.Adapter.KV
  @spec has_key?(map(), any(), keyword()) :: {:ok, boolean()}
  def has_key?(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)
    {:ok, FerricStore.exists(fkey)}
  end

  @doc """
  Returns the remaining TTL in milliseconds for `key`.

  Returns `{:ok, :infinity}` for keys without expiry, or
  `{:error, %Nebulex.KeyError{}}` for missing keys.
  """
  @impl Nebulex.Adapter.KV
  @spec ttl(map(), any(), keyword()) :: {:ok, integer() | :infinity} | {:error, Nebulex.KeyError.t()}
  def ttl(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    case FerricStore.ttl(fkey) do
      {:ok, nil} ->
        if FerricStore.exists(fkey) do
          {:ok, :infinity}
        else
          {:error, %Nebulex.KeyError{key: key}}
        end

      {:ok, ms} ->
        {:ok, ms}
    end
  end

  @doc """
  Sets or updates the TTL for an existing `key`.

  Returns `{:ok, true}` if the TTL was set, `{:ok, false}` if the key does
  not exist.
  """
  @impl Nebulex.Adapter.KV
  @spec expire(map(), any(), timeout(), keyword()) :: {:ok, boolean()}
  def expire(adapter_meta, key, ttl, _opts) do
    fkey = full_key(adapter_meta, key)
    ttl_ms = normalize_ttl(ttl)

    if FerricStore.exists(fkey) do
      FerricStore.expire(fkey, ttl_ms)
      {:ok, true}
    else
      {:ok, false}
    end
  end

  @doc """
  Touches `key`, refreshing its last access time.

  FerricStore's LFU counter is automatically bumped on read, so touching
  simply verifies the key exists.
  """
  @impl Nebulex.Adapter.KV
  @spec touch(map(), any(), keyword()) :: {:ok, boolean()}
  def touch(adapter_meta, key, _opts) do
    fkey = full_key(adapter_meta, key)

    if FerricStore.exists(fkey) do
      {:ok, true}
    else
      {:ok, false}
    end
  end

  @doc """
  Atomically increments or decrements a counter stored at `key`.

  If the key does not exist, it is initialized to `default` before applying
  the `amount`. Counter values are stored as serialized integers (via
  `term_to_binary`) for consistency with all other adapter values.
  """
  @impl Nebulex.Adapter.KV
  @spec update_counter(map(), any(), integer(), integer(), timeout(), keyword()) ::
          {:ok, integer()}
  def update_counter(adapter_meta, key, amount, default, ttl, _opts) do
    fkey = full_key(adapter_meta, key)
    ttl_ms = normalize_ttl(ttl)

    current =
      case FerricStore.get(fkey) do
        {:ok, nil} -> default
        {:ok, bin} -> deserialize(adapter_meta, bin)
      end

    new_val = current + amount
    bin = serialize(adapter_meta, new_val)

    # Set TTL only when creating (key didn't exist), otherwise preserve
    if FerricStore.exists(fkey) do
      FerricStore.set(fkey, bin, keepttl: true)
    else
      FerricStore.set(fkey, bin, ttl: ttl_ms)
    end

    {:ok, new_val}
  end

  # ---------------------------------------------------------------------------
  # Nebulex.Adapter.Queryable callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Executes a query against FerricStore.

  Supports the following operations and query types:

  - `:get_all` with `{:in, keys}` -- bulk fetch specific keys
  - `:get_all` with `{:q, nil}` -- fetch all entries (full scan)
  - `:count_all` with `{:in, keys}` or `{:q, nil}` -- count entries
  - `:delete_all` with `{:in, keys}` or `{:q, nil}` -- delete entries
  """
  @impl Nebulex.Adapter.Queryable
  @spec execute(map(), map(), keyword()) :: {:ok, [any()] | non_neg_integer()}
  def execute(adapter_meta, query_meta, _opts)

  # -- get_all {:in, keys} --

  def execute(adapter_meta, %{op: :get_all, query: {:in, keys}, select: select}, _opts) do
    results =
      Enum.reduce(keys, [], fn key, acc ->
        fkey = full_key(adapter_meta, key)

        case FerricStore.get(fkey) do
          {:ok, nil} -> acc
          {:ok, bin} -> [select_fields(select, key, deserialize(adapter_meta, bin)) | acc]
        end
      end)

    {:ok, Enum.reverse(results)}
  end

  # -- get_all {:q, nil} (all entries) --

  def execute(adapter_meta, %{op: :get_all, query: {:q, nil}, select: select}, _opts) do
    results =
      all_prefixed_entries(adapter_meta)
      |> Enum.map(fn {user_key, value} -> select_fields(select, user_key, value) end)

    {:ok, results}
  end

  # -- count_all {:q, nil} --

  def execute(adapter_meta, %{op: :count_all, query: {:q, nil}}, _opts) do
    {:ok, length(all_prefixed_keys(adapter_meta))}
  end

  # -- count_all {:in, keys} --

  def execute(adapter_meta, %{op: :count_all, query: {:in, keys}}, _opts) do
    count =
      Enum.count(keys, fn key ->
        fkey = full_key(adapter_meta, key)
        FerricStore.exists(fkey)
      end)

    {:ok, count}
  end

  # -- delete_all {:q, nil} --

  def execute(adapter_meta, %{op: :delete_all, query: {:q, nil}}, _opts) do
    fkeys = all_prefixed_keys(adapter_meta)
    Enum.each(fkeys, &FerricStore.del/1)
    {:ok, length(fkeys)}
  end

  # -- delete_all {:in, keys} --

  def execute(adapter_meta, %{op: :delete_all, query: {:in, keys}}, _opts) do
    count =
      Enum.count(keys, fn key ->
        fkey = full_key(adapter_meta, key)

        if FerricStore.exists(fkey) do
          FerricStore.del(fkey)
          true
        else
          false
        end
      end)

    {:ok, count}
  end

  @doc """
  Returns a lazy stream of entries matching the query.

  Currently only supports `{:q, nil}` (all entries).
  """
  @impl Nebulex.Adapter.Queryable
  @spec stream(map(), map(), keyword()) :: {:ok, Enumerable.t()}
  def stream(adapter_meta, %{query: {:q, nil}, select: select}, _opts) do
    stream =
      Stream.resource(
        fn -> all_prefixed_keys(adapter_meta) end,
        fn
          [] ->
            {:halt, []}

          [fkey | rest] ->
            user_key = strip_prefix(adapter_meta, fkey)

            case FerricStore.get(fkey) do
              {:ok, nil} ->
                {[], rest}

              {:ok, bin} ->
                value = deserialize(adapter_meta, bin)
                {[select_fields(select, user_key, value)], rest}
            end
        end,
        fn _ -> :ok end
      )

    {:ok, stream}
  end

  # ---------------------------------------------------------------------------
  # Nebulex.Adapter.Transaction callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Runs the given function inside a transaction.

  FerricStore operations are individually atomic (each command is serialized
  through its shard's GenServer), but there is no built-in multi-key
  transaction support. This implementation provides basic isolation: the
  function is executed, and if it succeeds the result is returned wrapped in
  `{:ok, result}`. Exceptions are allowed to propagate per the Nebulex
  contract.

  A process dictionary marker is set so that `in_transaction?/2` can detect
  whether the current process is inside a transaction block.
  """
  @impl Nebulex.Adapter.Transaction
  @spec transaction(map(), fun(), keyword()) :: {:ok, any()}
  def transaction(_adapter_meta, fun, _opts) do
    if Process.get({__MODULE__, :in_transaction?}, false) do
      # Nested transaction: just run the function directly
      {:ok, fun.()}
    else
      Process.put({__MODULE__, :in_transaction?}, true)

      try do
        {:ok, fun.()}
      after
        Process.delete({__MODULE__, :in_transaction?})
      end
    end
  end

  @doc """
  Returns `{:ok, true}` if the current process is inside a transaction block;
  `{:ok, false}` otherwise.
  """
  @impl Nebulex.Adapter.Transaction
  @spec in_transaction?(map(), keyword()) :: {:ok, boolean()}
  def in_transaction?(_adapter_meta, _opts) do
    {:ok, Process.get({__MODULE__, :in_transaction?}, false)}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp full_key(%{prefix: prefix}, key) do
    "#{prefix}:#{encode_key(key)}"
  end

  defp strip_prefix(%{prefix: prefix}, fkey) do
    prefix_len = byte_size(prefix) + 1
    binary = binary_part(fkey, prefix_len, byte_size(fkey) - prefix_len)
    decode_key(binary)
  end

  # Marker byte prepended to Base64-encoded non-binary keys so that
  # decode_key can reliably distinguish encoded keys from plain binary keys.
  @encoded_key_marker <<0xFF>>

  defp encode_key(key) when is_binary(key), do: key
  defp encode_key(key), do: @encoded_key_marker <> Base.encode64(:erlang.term_to_binary(key))

  defp decode_key(<<0xFF, rest::binary>>) do
    rest |> Base.decode64!() |> :erlang.binary_to_term()
  end

  defp decode_key(bin), do: bin

  defp serialize(%{serializer: :erlang_term}, value) do
    :erlang.term_to_binary(value, [:compressed])
  end

  defp serialize(%{serializer: :jason}, value) do
    Jason.encode!(value)
  end

  defp deserialize(%{serializer: :erlang_term}, bin) do
    :erlang.binary_to_term(bin)
  end

  defp deserialize(%{serializer: :jason}, bin) do
    Jason.decode!(bin)
  end

  defp normalize_ttl(:infinity), do: 0
  defp normalize_ttl(nil), do: 0
  defp normalize_ttl(ms) when is_integer(ms) and ms > 0, do: ms
  defp normalize_ttl(_), do: 0

  defp select_fields({:key, :value}, key, value), do: {key, value}
  defp select_fields(:key, key, _value), do: key
  defp select_fields(:value, _key, value), do: value
  defp select_fields(:entry, key, value), do: {key, value}

  # Returns all FerricStore keys with the adapter's prefix (raw keys).
  defp all_prefixed_keys(%{prefix: prefix}) do
    prefix_pattern = "#{prefix}:*"

    case FerricStore.keys(prefix_pattern) do
      {:ok, keys} -> keys
    end
  end

  # Returns all entries as `[{user_key, deserialized_value}]`.
  defp all_prefixed_entries(adapter_meta) do
    all_prefixed_keys(adapter_meta)
    |> Enum.reduce([], fn fkey, acc ->
      user_key = strip_prefix(adapter_meta, fkey)

      case FerricStore.get(fkey) do
        {:ok, nil} -> acc
        {:ok, bin} -> [{user_key, deserialize(adapter_meta, bin)} | acc]
      end
    end)
    |> Enum.reverse()
  end
end
