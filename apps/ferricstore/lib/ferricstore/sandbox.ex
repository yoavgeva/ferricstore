defmodule FerricStore.Sandbox do
  @moduledoc """
  Async-safe test isolation for FerricStore.

  `FerricStore.Sandbox` provides per-test key isolation with zero changes to
  application code. Each test process gets a unique namespace prefix. All
  FerricStore API calls made from that process automatically use the prefix --
  transparently, without the test knowing. Tests can run with `async: true`
  because different test processes have different namespaces and can never collide.

  The mental model is identical to `Ecto.Adapters.SQL.Sandbox`: the sandbox is
  invisible infrastructure. Test code writes `FerricStore.set("user:42", data)`
  and reads `FerricStore.get("user:42")`. Under the hood, the sandbox rewrites
  these as `SET test_abc123_user:42` and `GET test_abc123_user:42`. Cleanup calls
  a scoped flush that deletes only that test's keys.

  ## How namespace injection works

  The sandbox stores the test namespace in the calling process's Process dictionary
  under `:ferricstore_sandbox`. Every FerricStore API function checks this key
  before resolving the final key name. Production code runs with
  `Process.get(:ferricstore_sandbox) == nil` -- zero overhead.

  ## Usage

      setup do
        namespace = FerricStore.Sandbox.checkout()
        on_exit(fn -> FerricStore.Sandbox.checkin(namespace) end)
        %{namespace: namespace}
      end

  Or, for a one-line setup, use `FerricStore.Sandbox.Case`:

      use FerricStore.Sandbox.Case

  ## TTL freeze

  Pass `freeze_ttl: true` to `checkout/1` to suspend active expiry sweep for the
  test's namespace, preventing flaky tests on slow CI machines:

      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)

  Use `FerricStore.Sandbox.expire_now/1` to manually force expiry in tests.
  """

  alias Ferricstore.Store.Router

  @type namespace :: binary()

  @doc """
  Generates a unique namespace, sets it in the calling process's Process
  dictionary, and returns it.

  ## Options

    * `:freeze_ttl` - When `true`, records that this namespace should skip
      active expiry sweep. Defaults to `false`.

  ## Returns

    * The namespace string (e.g., `"test_a1b2c3d4e5f6g7h8_"`).

  ## Examples

      namespace = FerricStore.Sandbox.checkout()
      # Process.get(:ferricstore_sandbox) == "test_<hex>_"

      namespace = FerricStore.Sandbox.checkout(freeze_ttl: true)

  """
  @spec checkout(keyword()) :: namespace()
  def checkout(opts \\ []) do
    namespace = generate_namespace()
    Process.put(:ferricstore_sandbox, namespace)

    if Keyword.get(opts, :freeze_ttl, false) do
      Process.put(:ferricstore_sandbox_freeze_ttl, true)
    end

    namespace
  end

  @doc """
  Cleans up all keys in the given `namespace` and clears the Process dictionary
  entry.

  This function deletes only the keys that belong to the given namespace by
  scanning all shard keys and removing those with the matching prefix. It runs
  even if the test crashes (when used with `on_exit`), ensuring reliable cleanup.

  ## Examples

      on_exit(fn -> FerricStore.Sandbox.checkin(namespace) end)

  """
  @spec checkin(namespace()) :: :ok
  def checkin(namespace) when is_binary(namespace) do
    flush_namespace(namespace)
    Process.delete(:ferricstore_sandbox)
    Process.delete(:ferricstore_sandbox_freeze_ttl)
    :ok
  end

  @doc """
  Propagates the current sandbox namespace to another process.

  Sends a message to `pid` instructing it to copy the current sandbox namespace
  into its own Process dictionary. The target process must be a GenServer or any
  process that handles the `{:ferricstore_sandbox_allow, namespace}` message.

  For simpler use cases (Tasks, spawned processes), this function also directly
  sets the namespace via `Process.put/2` on the calling side -- the target pid
  receives a message it can handle if it is a GenServer, and the caller can pass
  the namespace to Task functions directly.

  ## Examples

      FerricStore.Sandbox.allow(worker_pid)

  """
  @spec allow(pid()) :: :ok
  def allow(pid) when is_pid(pid) do
    namespace = Process.get(:ferricstore_sandbox)

    if namespace do
      send(pid, {:ferricstore_sandbox_allow, namespace})
    end

    :ok
  end

  @doc """
  Forces immediate expiry of `key` within the current sandbox namespace.

  This is used in tests with `freeze_ttl: true` to manually test expiry logic
  without waiting for real time to pass. The key is deleted from the store
  immediately.

  ## Examples

      FerricStore.Sandbox.expire_now("key")

  """
  @spec expire_now(binary()) :: :ok
  def expire_now(key) do
    resolved_key = FerricStore.sandbox_key(key)
    Router.delete(resolved_key)
  end

  @doc """
  Returns whether TTL is frozen for the current process's sandbox namespace.

  Used internally by active expiry to skip frozen namespaces.
  """
  @spec ttl_frozen?() :: boolean()
  def ttl_frozen? do
    Process.get(:ferricstore_sandbox_freeze_ttl, false) == true
  end

  @doc """
  Returns the current sandbox namespace for the calling process, or `nil`.
  """
  @spec current_namespace() :: namespace() | nil
  def current_namespace do
    Process.get(:ferricstore_sandbox)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp generate_namespace do
    hex =
      :crypto.strong_rand_bytes(8)
      |> Base.encode16(case: :lower)

    "test_#{hex}_"
  end

  defp flush_namespace(namespace) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.each(0..(shard_count - 1), fn i ->
      keydir = :"keydir_#{i}"

      try do
        # Scan the single keydir table for keys with the namespace prefix
        # and delete them. Format: {key, value, expire_at_ms, lfu_counter}
        keys =
          :ets.tab2list(keydir)
          |> Enum.filter(fn {key, _value, _exp, _lfu} -> String.starts_with?(key, namespace) end)
          |> Enum.map(fn {key, _value, _exp, _lfu} -> key end)

        Enum.each(keys, fn key ->
          :ets.delete(keydir, key)
          # Also delete from the underlying Bitcask store via Router.delete
          Router.delete(key)
        end)
      rescue
        ArgumentError ->
          # ETS table does not exist (shard not started) -- skip
          :ok
      end
    end)
  end
end
