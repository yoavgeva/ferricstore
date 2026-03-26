defmodule FerricStore.Sandbox do
  @moduledoc """
  True process-isolated sandbox for tests.

  Each `checkout/1` creates a fully isolated FerricStore instance:

    * 2 private Shard GenServers (no global name registration)
    * 2 private ETS keydir tables (anonymous, not named)
    * 2 private ETS prefix_keys tables (anonymous, not named)
    * Its own ra system (independent WAL + Raft groups)
    * Its own tmpdir (cleaned up on checkin)

  Full production stack: ETS hot cache + Raft WAL + Bitcask on-disk files.
  Complete isolation. Supports `async: true` tests.

  Production code has ZERO overhead -- all sandbox resolution is behind
  `@sandbox_enabled Application.compile_env(:ferricstore, :sandbox_enabled, false)`
  which compiles away in production builds.

  ## How it works

  `checkout/1` returns a `%FerricStore.Sandbox{}` struct and stores it in both
  the calling process's Process dictionary and a shared ETS registry. The Router
  checks `Process.get(:ferricstore_sandbox)` on every operation (~5ns). When a
  struct sandbox is active, it routes to the private shards instead of the
  application-supervised ones.

  `allow/2` copies the sandbox from an owner process to a child process via the
  ETS registry, enabling Tasks and spawned workers to share the same sandbox.

  ## Backward compatibility

  The old prefix-based sandbox (`checkout/0` returning a string namespace) is
  still supported. When `Process.get(:ferricstore_sandbox)` is a binary string,
  the old key-prefixing behavior is used. When it is a `%FerricStore.Sandbox{}`
  struct, the new shard-isolation behavior is used.

  ## Usage

      setup do
        sandbox = FerricStore.Sandbox.checkout()
        on_exit(fn -> FerricStore.Sandbox.checkin(sandbox) end)
        %{sandbox: sandbox}
      end

  Or use the Case template:

      use FerricStore.Sandbox.Case

  """

  alias Ferricstore.Store.Router

  defstruct [:ref, :tmpdir, :shards, :keydirs, :prefix_tables, :ra_system, :shard_count]

  @type t :: %__MODULE__{
          ref: reference(),
          tmpdir: binary(),
          shards: tuple(),
          keydirs: tuple(),
          prefix_tables: tuple(),
          ra_system: atom(),
          shard_count: non_neg_integer()
        }

  @type namespace :: binary()

  # ETS registry for allow() -- maps pid -> sandbox struct
  @registry :ferricstore_sandbox_registry

  @doc """
  Initializes the shared ETS registry used by `allow/2`.

  Called once during application startup when `sandbox_enabled: true`.
  """
  @spec init_registry() :: :ok
  def init_registry do
    if :ets.whereis(@registry) == :undefined do
      :ets.new(@registry, [:set, :public, :named_table, {:read_concurrency, true}])
    end

    :ok
  end

  @doc """
  Creates a fully isolated sandbox with private shards, ETS, ra, and tmpdir.

  ## Options

    * `:shard_count` - Number of private shards (default: 2)
    * `:freeze_ttl` - When `true`, records that expiry sweep should be skipped (default: false)

  ## Returns

    A `%FerricStore.Sandbox{}` struct.
  """
  @spec checkout(keyword()) :: t()
  def checkout(opts \\ []) do
    shard_count = Keyword.get(opts, :shard_count, 2)
    ref = make_ref()
    unique = :erlang.unique_integer([:positive])
    tmpdir = Path.join(System.tmp_dir!(), "ferricstore_sandbox_#{unique}")
    File.mkdir_p!(tmpdir)

    # Start a private ra system for this sandbox. Each sandbox gets its
    # own WAL directory and ra system so Raft groups are fully isolated.
    ra_system_name = :"ferricstore_sandbox_ra_#{unique}"
    ra_data_dir = Path.join(tmpdir, "ra")
    File.mkdir_p!(ra_data_dir)
    ra_names = :ra_system.derive_names(ra_system_name)

    ra_config = %{
      name: ra_system_name,
      names: ra_names,
      data_dir: to_charlist(ra_data_dir),
      wal_data_dir: to_charlist(ra_data_dir),
      segment_max_entries: 32_768
    }

    {:ok, _} = :ra_system.start(ra_config)

    # Start shard GenServers with private ETS and private ra system.
    # Each shard starts its own ra server in the sandbox ra system and
    # submits writes directly (no Batcher). This exercises the full
    # production Raft stack while maintaining complete isolation.
    shards_info =
      for i <- 0..(shard_count - 1) do
        shard_dir = Path.join(tmpdir, "shard_#{i}")
        File.mkdir_p!(shard_dir)

        # Create anonymous ETS tables
        keydir =
          :ets.new(:sandbox_keydir, [
            :set,
            :public,
            {:read_concurrency, true},
            {:write_concurrency, :auto},
            {:decentralized_counters, true}
          ])

        prefix_keys =
          :ets.new(:sandbox_prefix_keys, [
            :duplicate_bag,
            :public,
            {:read_concurrency, true},
            {:write_concurrency, :auto},
            {:decentralized_counters, true}
          ])

        {:ok, pid} =
          GenServer.start_link(Ferricstore.Store.Shard, [
            index: i,
            data_dir: shard_dir,
            sandbox: true,
            sandbox_keydir: keydir,
            sandbox_prefix_keys: prefix_keys,
            sandbox_ra_system: ra_system_name,
            sandbox_uid: unique
          ])

        {i, %{pid: pid, keydir: keydir, prefix_keys: prefix_keys}}
      end
      |> Map.new()

    shards_tuple =
      0..(shard_count - 1)
      |> Enum.map(fn i -> shards_info[i].pid end)
      |> List.to_tuple()

    keydirs_tuple =
      0..(shard_count - 1)
      |> Enum.map(fn i -> shards_info[i].keydir end)
      |> List.to_tuple()

    prefix_tables_tuple =
      0..(shard_count - 1)
      |> Enum.map(fn i -> shards_info[i].prefix_keys end)
      |> List.to_tuple()

    sandbox = %__MODULE__{
      ref: ref,
      tmpdir: tmpdir,
      shards: shards_tuple,
      keydirs: keydirs_tuple,
      prefix_tables: prefix_tables_tuple,
      ra_system: ra_system_name,
      shard_count: shard_count
    }

    if Keyword.get(opts, :freeze_ttl, false) do
      Process.put(:ferricstore_sandbox_freeze_ttl, true)
    end

    Process.put(:ferricstore_sandbox, sandbox)

    # Also store in the ETS registry for allow()
    if :ets.whereis(@registry) != :undefined do
      :ets.insert(@registry, {self(), sandbox})
    end

    sandbox
  end

  @doc """
  Cleans up a sandbox, stopping all private shards and removing temp files.

  Accepts both `%FerricStore.Sandbox{}` structs (new) and binary namespaces (old).
  """
  @spec checkin(t() | namespace()) :: :ok
  def checkin(%__MODULE__{} = sandbox) do
    # Stop shard GenServers (this also stops the shard's handle to ra)
    for i <- 0..(sandbox.shard_count - 1) do
      pid = elem(sandbox.shards, i)

      if Process.alive?(pid) do
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
      end
    end

    # Stop the private ra system (stops all ra servers in this system).
    # Must happen after shard GenServers are stopped to avoid writes
    # to a stopped ra system.
    if sandbox.ra_system do
      try do
        :ra_system.stop(sandbox.ra_system)
      catch
        _, _ -> :ok
      end
    end

    # Delete ETS tables
    for i <- 0..(sandbox.shard_count - 1) do
      try do
        :ets.delete(elem(sandbox.keydirs, i))
      catch
        _, _ -> :ok
      end

      try do
        :ets.delete(elem(sandbox.prefix_tables, i))
      catch
        _, _ -> :ok
      end
    end

    # Cleanup process state
    Process.delete(:ferricstore_sandbox)
    Process.delete(:ferricstore_sandbox_freeze_ttl)

    # Remove from registry
    if :ets.whereis(@registry) != :undefined do
      :ets.delete(@registry, self())
    end

    # Remove tmpdir
    File.rm_rf(sandbox.tmpdir)
    :ok
  end

  def checkin(namespace) when is_binary(namespace) do
    # Old prefix-based sandbox -- flush namespace keys from shared shards
    flush_namespace(namespace)
    Process.delete(:ferricstore_sandbox)
    Process.delete(:ferricstore_sandbox_freeze_ttl)
    :ok
  end

  @doc """
  Propagates the sandbox from `owner_pid` to `child_pid`.

  The child process will use the same private shards, ETS tables, and ra
  system as the owner. For the new struct sandbox, this uses the ETS registry.
  For the old prefix sandbox, sends a message to the child.
  """
  @spec allow(pid(), pid()) :: :ok
  def allow(owner_pid, child_pid) when is_pid(owner_pid) and is_pid(child_pid) do
    if :ets.whereis(@registry) != :undefined do
      case :ets.lookup(@registry, owner_pid) do
        [{^owner_pid, sandbox}] ->
          :ets.insert(@registry, {child_pid, sandbox})
          :ok

        [] ->
          :ok
      end
    else
      :ok
    end
  end

  @doc """
  Propagates the current sandbox namespace to another process.

  For backward compatibility with the old prefix-based sandbox.
  """
  @spec allow(pid()) :: :ok
  def allow(pid) when is_pid(pid) do
    case Process.get(:ferricstore_sandbox) do
      %__MODULE__{} = _sandbox ->
        # New struct sandbox: use registry-based allow
        allow(self(), pid)

      namespace when is_binary(namespace) ->
        send(pid, {:ferricstore_sandbox_allow, namespace})
        :ok

      nil ->
        :ok
    end
  end

  @doc """
  Resolves the current sandbox for the calling process.

  Fast path: checks Process dictionary first (~5ns). Falls back to ETS
  registry lookup (~50ns) for allowed child processes.

  Returns a `%FerricStore.Sandbox{}` struct, or `nil` if no sandbox is active
  or if the sandbox is the old string namespace type.
  """
  @spec resolve() :: t() | nil
  def resolve do
    case Process.get(:ferricstore_sandbox) do
      %__MODULE__{} = sandbox ->
        sandbox

      _other ->
        # Check registry for allowed child processes
        if :ets.whereis(@registry) != :undefined do
          case :ets.lookup(@registry, self()) do
            [{_, %__MODULE__{} = sandbox}] ->
              # Cache in process dict for future lookups
              Process.put(:ferricstore_sandbox, sandbox)
              sandbox

            _ ->
              nil
          end
        else
          nil
        end
    end
  end

  @doc """
  Forces immediate expiry of `key` within the current sandbox namespace.
  """
  @spec expire_now(binary()) :: :ok
  def expire_now(key) do
    resolved_key = FerricStore.sandbox_key(key)
    Router.delete(resolved_key)
  end

  @doc """
  Returns whether TTL is frozen for the current process's sandbox namespace.
  """
  @spec ttl_frozen?() :: boolean()
  def ttl_frozen? do
    Process.get(:ferricstore_sandbox_freeze_ttl, false) == true
  end

  @doc """
  Returns the current sandbox namespace for the calling process, or `nil`.

  For backward compat, returns the string namespace for old-style sandboxes,
  or the struct for new-style sandboxes.
  """
  @spec current_namespace() :: t() | namespace() | nil
  def current_namespace do
    Process.get(:ferricstore_sandbox)
  end

  # ---------------------------------------------------------------------------
  # Private -- old prefix-based flush
  # ---------------------------------------------------------------------------

  defp flush_namespace(namespace) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.each(0..(shard_count - 1), fn i ->
      keydir = :"keydir_#{i}"

      try do
        keys =
          :ets.tab2list(keydir)
          |> Enum.filter(fn {key, _value, _exp, _lfu, _fid, _off, _vsize} ->
            owns_namespace?(key, namespace)
          end)
          |> Enum.map(fn {key, _value, _exp, _lfu, _fid, _off, _vsize} -> key end)

        Enum.each(keys, fn key ->
          :ets.delete(keydir, key)
          Router.delete(key)
        end)
      rescue
        ArgumentError ->
          :ok
      end
    end)
  end

  defp owns_namespace?(key, namespace) do
    String.starts_with?(key, namespace) or
      compound_has_namespace?(key, namespace)
  end

  defp compound_has_namespace?(<<"H:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"L:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"S:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"Z:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"T:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"V:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"VM:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"PM:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(<<"LM:", rest::binary>>, ns), do: String.starts_with?(rest, ns)
  defp compound_has_namespace?(_, _), do: false
end
