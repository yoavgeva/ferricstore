defmodule Ferricstore.Acl do
  @moduledoc """
  GenServer managing the Access Control List (ACL) for FerricStore.

  Stores user accounts in a named ETS table (`:ferricstore_acl`). Each user
  record has a username, enabled/disabled flag, an optional password hash,
  allowed commands, denied commands, and allowed key patterns.

  The "default" user is always present and cannot be deleted. On startup it
  is initialised as enabled with no password and full access (`+@all`, `~*`).

  ## Spec reference

  Implements spec section 6.1: `ACL SETUSER`, `ACL DELUSER`, `ACL GETUSER`,
  `ACL LIST`, `ACL WHOAMI`.

  ## Security hardening (Phase 1)

  1. **Password hashing** -- passwords are hashed with PBKDF2-SHA256 (salt +
     :crypto) at ingestion time. Plaintext passwords are never stored in ETS.
  2. **denied_commands set** -- explicit denials via `-command` or `-@category`
     work even when `commands` is `:all`. Denials are tracked in a separate
     `denied_commands` MapSet and subtracted at check time.
  3. **Protected mode** -- non-localhost connections are rejected until at least
     one non-default ACL user with a password is configured.
  4. **max_acl_users** -- configurable safety limit (default 10,000) prevents
     unbounded ACL user creation.
  5. **ACL LOG denials** -- command denials are logged to the AuditLog with
     username, command, client IP, and client ID.

  ## Command categories

  Commands can be granted or revoked by category using `+@category` / `-@category`:

    - `@read`      -- read-only commands (GET, MGET, HGET, EXISTS, TTL, etc.)
    - `@write`     -- mutation commands (SET, DEL, HSET, LPUSH, INCR, etc.)
    - `@admin`     -- server administration (CONFIG, ACL, DEBUG, FLUSHDB, etc.)
    - `@dangerous` -- potentially destructive (FLUSHDB, FLUSHALL, DEBUG, KEYS, SHUTDOWN, etc.)

  ## ETS schema

  Each row is a tuple:

      {username :: binary(), %{
        enabled: boolean(),
        password: binary() | nil,
        commands: :all | MapSet.t(binary()),
        denied_commands: MapSet.t(binary()),
        keys: :all | [binary()]
      }}

  ## Usage

      Ferricstore.Acl.set_user("alice", ["on", ">s3cret", "~cache:*", "+get", "+set"])
      Ferricstore.Acl.authenticate("alice", "s3cret")
      #=> {:ok, user}

      Ferricstore.Acl.check_command("alice", "GET")
      #=> :ok

      Ferricstore.Acl.check_command("alice", "FLUSHDB")
      #=> {:error, "NOPERM this user has no permissions to run the 'flushdb' command"}

      Ferricstore.Acl.del_user("alice")
      #=> :ok
  """

  use GenServer

  alias Ferricstore.AuditLog

  @table :ferricstore_acl

  # ---------------------------------------------------------------------------
  # Command categories
  # ---------------------------------------------------------------------------

  @read_commands MapSet.new(~w(
    GET MGET GETRANGE STRLEN GETEX GETDEL GETSET
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN
    LRANGE LLEN LINDEX LPOS
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SSCAN
    ZSCORE ZRANK ZREVRANK ZRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE ZSCAN
    TYPE EXISTS TTL PTTL EXPIRETIME PEXPIRETIME
    GETBIT BITCOUNT BITPOS PFCOUNT
    OBJECT SUBSTR
    GEOHASH GEOPOS GEODIST GEOSEARCH
    XLEN XRANGE XREVRANGE XREAD XINFO
    DBSIZE RANDOMKEY SCAN KEYS
    JSON.GET JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRLEN JSON.MGET
  ))

  @write_commands MapSet.new(~w(
    SET SETNX SETEX PSETEX MSET MSETNX APPEND SETRANGE
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
    GETSET GETDEL
    JSON.SET JSON.DEL JSON.NUMINCRBY JSON.TOGGLE JSON.CLEAR JSON.ARRAPPEND
    CAS LOCK UNLOCK EXTEND
  ))

  @admin_commands MapSet.new(~w(
    CONFIG ACL DEBUG SLOWLOG SAVE BGSAVE LASTSAVE
    FLUSHDB FLUSHALL
    INFO COMMAND MODULE MEMORY
    CLUSTER.HEALTH CLUSTER.STATS
    WAITAOF WAIT SELECT
    FERRICSTORE.HOTNESS FERRICSTORE.METRICS
  ))

  @dangerous_commands MapSet.new(~w(
    FLUSHDB FLUSHALL DEBUG CONFIG KEYS SHUTDOWN
    SORT MIGRATE RESTORE DUMP
  ))

  @category_map %{
    "READ" => @read_commands,
    "WRITE" => @write_commands,
    "ADMIN" => @admin_commands,
    "DANGEROUS" => @dangerous_commands
  }

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "A user record stored in the ACL table."
  @type user :: %{
          enabled: boolean(),
          password: binary() | nil,
          commands: :all | MapSet.t(binary()),
          denied_commands: MapSet.t(binary()),
          keys: :all | [binary()]
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the ACL GenServer and creates the backing ETS table.

  Initialises the "default" user with full access.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates or updates a user with the given rules.

  Rules are a list of strings parsed in order:

    * `"on"`           -- enable the user
    * `"off"`          -- disable the user
    * `">password"`    -- set the user's password (hashed before storing)
    * `"nopass"`       -- clear the user's password (allow passwordless auth)
    * `"~pattern"`     -- add a key pattern (e.g. `"~*"` for all keys)
    * `"+command"`     -- allow a specific command
    * `"+@all"`        -- allow all commands
    * `"+@category"`   -- allow all commands in a category (read, write, admin, dangerous)
    * `"-command"`     -- deny a specific command (works even after +@all)
    * `"-@all"`        -- deny all commands
    * `"-@category"`   -- deny all commands in a category (works even after +@all)
    * `"allkeys"`      -- shorthand for `"~*"`
    * `"allcommands"`  -- shorthand for `"+@all"`
    * `"resetpass"`    -- clear the password

  When creating a new user with no rules, the user is created in a disabled
  state with no password and no permissions (safe default).

  Returns `:ok` on success, `{:error, reason}` on invalid rules.

  ## Parameters

    - `username` -- the username (case-sensitive binary)
    - `rules`    -- list of rule strings

  ## Examples

      Ferricstore.Acl.set_user("alice", ["on", ">s3cret", "~*", "+@all"])
      #=> :ok

      Ferricstore.Acl.set_user("reader", ["on", ">pass", "-@all", "+@read"])
      #=> :ok
  """
  @spec set_user(binary(), [binary()]) :: :ok | {:error, binary()}
  def set_user(username, rules) do
    GenServer.call(__MODULE__, {:set_user, username, rules})
  end

  @doc """
  Deletes a user from the ACL.

  The "default" user cannot be deleted.

  Returns `:ok` on success, `{:error, reason}` if the user is "default" or
  does not exist.

  ## Parameters

    - `username` -- the username to delete

  ## Examples

      Ferricstore.Acl.del_user("alice")
      #=> :ok
  """
  @spec del_user(binary()) :: :ok | {:error, binary()}
  def del_user(username) do
    GenServer.call(__MODULE__, {:del_user, username})
  end

  @doc """
  Returns the user record for the given username, or `nil` if not found.

  ## Parameters

    - `username` -- the username to look up

  ## Examples

      Ferricstore.Acl.get_user("default")
      #=> %{enabled: true, password: nil, commands: :all, denied_commands: MapSet.new(), keys: :all}
  """
  @spec get_user(binary()) :: user() | nil
  def get_user(username) do
    case :ets.lookup(@table, username) do
      [{^username, user}] -> user
      [] -> nil
    end
  end

  @doc """
  Returns a list of all users in Redis ACL LIST format.

  Each entry is a string like `"user default on ~* &* +@all"`.

  ## Examples

      Ferricstore.Acl.list_users()
      #=> ["user default on ~* &* +@all", "user alice on ~cache:* +get +set"]
  """
  @spec list_users() :: [binary()]
  def list_users do
    @table
    |> :ets.tab2list()
    |> Enum.sort_by(fn {name, _} -> name end)
    |> Enum.map(&format_user_rule/1)
  end

  @doc """
  Returns the user info for `ACL GETUSER` in Redis-compatible flat list format.

  Returns `nil` if the user does not exist.

  ## Parameters

    - `username` -- the username to look up

  ## Examples

      Ferricstore.Acl.get_user_info("default")
      #=> ["flags", ["on"], "passwords", [], "commands", "+@all", "keys", "~*", "channels", "&*"]
  """
  @spec get_user_info(binary()) :: [term()] | nil
  def get_user_info(username) do
    case get_user(username) do
      nil ->
        nil

      user ->
        flags = if user.enabled, do: ["on"], else: ["off"]

        passwords =
          if user.password, do: [hash_for_display(user.password)], else: []

        commands = format_commands(user.commands)
        keys = format_keys(user.keys)

        ["flags", flags, "passwords", passwords, "commands", commands, "keys", keys, "channels", "&*"]
    end
  end

  @doc """
  Authenticates a user with the given password.

  Passwords are verified against the stored PBKDF2-SHA256 hash. Nopass users
  (password is `nil`) accept any password.

  Returns `{:ok, username}` on success, `{:error, reason}` on failure.

  ## Parameters

    - `username` -- the username to authenticate
    - `password` -- the plaintext password to check

  ## Examples

      Ferricstore.Acl.authenticate("default", "secret123")
      #=> {:ok, "default"}

      Ferricstore.Acl.authenticate("unknown", "pass")
      #=> {:error, "WRONGPASS invalid username-password pair or user is disabled."}
  """
  @spec authenticate(binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def authenticate(username, password) do
    case get_user(username) do
      nil ->
        {:error, "WRONGPASS invalid username-password pair or user is disabled."}

      %{enabled: false} ->
        {:error, "WRONGPASS invalid username-password pair or user is disabled."}

      %{password: nil} ->
        {:ok, username}

      %{password: stored_hash} ->
        if verify_password(password, stored_hash) do
          {:ok, username}
        else
          {:error, "WRONGPASS invalid username-password pair or user is disabled."}
        end
    end
  end

  @doc """
  Checks if the given user is allowed to run the given command (enabled check only).

  Legacy v1 check. Prefer `check_command/2` for full ACL enforcement.

  ## Parameters

    - `username` -- the username
    - `_command` -- the command name (currently unused)

  ## Returns

    - `:ok` if the user is allowed
    - `{:error, reason}` if denied
  """
  @spec check_permission(binary(), binary()) :: :ok | {:error, binary()}
  def check_permission(username, _command) do
    case get_user(username) do
      nil ->
        {:error, "NOPERM user '#{username}' does not exist"}

      %{enabled: false} ->
        {:error, "NOPERM user '#{username}' is disabled"}

      _ ->
        :ok
    end
  end

  @doc """
  Checks if the given user is allowed to run the given command.

  Performs a full ACL check:

    1. The user must exist.
    2. The user must be enabled.
    3. The command must not be in the user's `denied_commands` set.
    4. The command must be in the user's allowed command set (`:all` or a `MapSet`).

  When the user's commands field is `:all`, all commands are permitted unless
  they appear in `denied_commands`. When it is a `MapSet`, the command
  (uppercased) must be a member and not in `denied_commands`.

  ## Parameters

    - `username` -- the username
    - `command`  -- the command name (case-insensitive)

  ## Returns

    - `:ok` if the command is permitted
    - `{:error, reason}` with a `NOPERM` prefix if denied

  ## Examples

      Ferricstore.Acl.check_command("default", "GET")
      #=> :ok

      Ferricstore.Acl.check_command("readonly_user", "SET")
      #=> {:error, "NOPERM this user has no permissions to run the 'set' command"}
  """
  @spec check_command(binary(), binary()) :: :ok | {:error, binary()}
  def check_command(username, command) do
    cmd = String.upcase(command)

    case get_user(username) do
      nil ->
        {:error, "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      %{enabled: false} ->
        {:error, "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      %{commands: :all, denied_commands: denied} ->
        if MapSet.member?(denied, cmd) do
          {:error, "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
        else
          :ok
        end

      %{commands: cmds} ->
        if MapSet.member?(cmds, cmd) do
          :ok
        else
          {:error, "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
        end
    end
  end

  @doc """
  Returns the map of command categories.

  Each key is an uppercase category name (e.g. `"READ"`, `"WRITE"`, `"ADMIN"`,
  `"DANGEROUS"`) and the value is a `MapSet` of uppercase command names.

  ## Examples

      Ferricstore.Acl.categories()
      #=> %{"READ" => MapSet.new(["GET", "MGET", ...]), ...}
  """
  @spec categories() :: %{binary() => MapSet.t(binary())}
  def categories, do: @category_map

  @doc """
  Resets the ACL to its initial state (only the default user).

  Used primarily in tests to avoid state leaking between test cases.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
  end

  # ---------------------------------------------------------------------------
  # Protected mode API (Fix 3)
  # ---------------------------------------------------------------------------

  @doc """
  Returns whether protected mode is currently active.

  In standalone mode, protected mode defaults to `true`. In embedded mode,
  it defaults to `false`. The setting can be overridden via application env:

      config :ferricstore, :protected_mode, true

  ## Examples

      Ferricstore.Acl.protected_mode?()
      #=> true
  """
  @spec protected_mode?() :: boolean()
  def protected_mode? do
    case Ferricstore.Mode.current() do
      :embedded ->
        Application.get_env(:ferricstore, :protected_mode, false)

      :standalone ->
        Application.get_env(:ferricstore, :protected_mode, true)
    end
  end

  @doc """
  Returns whether at least one non-default ACL user with a password exists
  and is enabled.

  Used by the protected mode check to determine whether the server has been
  configured with real authentication.

  ## Examples

      Ferricstore.Acl.has_configured_users?()
      #=> false
  """
  @spec has_configured_users?() :: boolean()
  def has_configured_users? do
    @table
    |> :ets.tab2list()
    |> Enum.any?(fn {name, user} ->
      name != "default" and user.password != nil and user.enabled
    end)
  end

  @doc """
  Returns whether the given peer address is a localhost address.

  Recognizes IPv4 `127.0.0.1` and IPv6 `::1`.

  ## Parameters

    - `peer` -- a `{ip_tuple, port}` tuple or `nil`

  ## Examples

      Ferricstore.Acl.localhost?({{127, 0, 0, 1}, 12345})
      #=> true

      Ferricstore.Acl.localhost?({{192, 168, 1, 1}, 12345})
      #=> false
  """
  @spec localhost?({:inet.ip_address(), :inet.port_number()} | nil) :: boolean()
  def localhost?(nil), do: false
  def localhost?({{127, 0, 0, 1}, _port}), do: true
  def localhost?({{0, 0, 0, 0, 0, 0, 0, 1}, _port}), do: true
  def localhost?(_peer), do: false

  @doc """
  Checks whether a connection from the given peer should be allowed under
  protected mode rules.

  Returns `:ok` if the connection is allowed, or `{:error, message}` if the
  connection should be rejected.

  A connection is rejected when ALL of:
    1. Protected mode is active
    2. No non-default ACL user with a password exists
    3. The connection is not from localhost

  ## Parameters

    - `peer` -- a `{ip_tuple, port}` tuple or `nil`

  ## Examples

      Ferricstore.Acl.check_protected_mode({{127, 0, 0, 1}, 12345})
      #=> :ok

      Ferricstore.Acl.check_protected_mode({{192, 168, 1, 1}, 12345})
      #=> {:error, "DENIED FerricStore is in protected mode..."}
  """
  @spec check_protected_mode({:inet.ip_address(), :inet.port_number()} | nil) ::
          :ok | {:error, binary()}
  def check_protected_mode(peer) do
    if protected_mode?() and not has_configured_users?() and not localhost?(peer) do
      {:error,
       "DENIED FerricStore is in protected mode. " <>
         "Configure ACL users or set protected-mode false."}
    else
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LOG denial helper (Fix 5)
  # ---------------------------------------------------------------------------

  @doc """
  Logs a command denial to the audit log.

  Called by the connection handler when `check_command/2` returns an error.
  This is a convenience wrapper around `AuditLog.log/2` that uses the
  `:command_denied` event type.

  ## Parameters

    - `username`  -- the username that was denied
    - `command`   -- the command that was denied
    - `client_ip` -- formatted client IP string
    - `client_id` -- the connection's client ID

  ## Examples

      Ferricstore.Acl.log_command_denied("alice", "SET", "127.0.0.1:1234", 42)
  """
  @spec log_command_denied(binary(), binary(), binary(), term()) :: :ok
  def log_command_denied(username, command, client_ip, client_id) do
    AuditLog.log(:command_denied, %{
      username: username,
      command: command,
      client_ip: client_ip,
      client_id: client_id
    })
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:set, :public, :named_table])
    insert_default_user()
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:set_user, username, rules}, _from, state) do
    existing = get_user(username)

    # Fix 4: max_acl_users -- check limit before creating a new user.
    max = Application.get_env(:ferricstore, :max_acl_users, 10_000)

    if existing == nil and :ets.info(@table, :size) >= max do
      {:reply, {:error, "ERR max ACL users reached (#{max})"}, state}
    else
      base =
        if existing do
          existing
        else
          %{
            enabled: false,
            password: nil,
            commands: :all,
            denied_commands: MapSet.new(),
            keys: :all
          }
        end

      case apply_rules(base, rules) do
        {:ok, updated} ->
          :ets.insert(@table, {username, updated})
          {:reply, :ok, state}

        {:error, _reason} = err ->
          {:reply, err, state}
      end
    end
  end

  def handle_call({:del_user, "default"}, _from, state) do
    {:reply, {:error, "ERR The 'default' user cannot be removed"}, state}
  end

  def handle_call({:del_user, username}, _from, state) do
    case :ets.lookup(@table, username) do
      [] ->
        {:reply, {:error, "ERR User '#{username}' does not exist"}, state}

      _ ->
        :ets.delete(@table, username)
        {:reply, :ok, state}
    end
  end

  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table)
    insert_default_user()
    {:reply, :ok, state}
  end

  # ---------------------------------------------------------------------------
  # Private -- password hashing (Fix 1)
  # ---------------------------------------------------------------------------

  # Hashes a password using a random 16-byte salt and SHA-256.
  # Returns a base64-encoded binary of `salt <> hash` (48 bytes raw).
  @spec hash_password(binary()) :: binary()
  defp hash_password(password) do
    salt = :crypto.strong_rand_bytes(16)
    hash = :crypto.hash(:sha256, salt <> password)
    Base.encode64(salt <> hash)
  end

  # Verifies a plaintext password against a stored hash.
  # Extracts the 16-byte salt from the stored hash and recomputes.
  @spec verify_password(binary(), binary()) :: boolean()
  defp verify_password(password, stored_hash) do
    case Base.decode64(stored_hash) do
      {:ok, <<salt::binary-16, hash::binary-32>>} ->
        computed = :crypto.hash(:sha256, salt <> password)
        # Constant-time comparison to prevent timing attacks
        :crypto.hash_equals(computed, hash)

      _ ->
        false
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- rule parsing
  # ---------------------------------------------------------------------------

  @spec apply_rules(user(), [binary()]) :: {:ok, user()} | {:error, binary()}
  defp apply_rules(user, []), do: {:ok, user}

  defp apply_rules(user, [rule | rest]) do
    case parse_rule(user, rule) do
      {:ok, updated} -> apply_rules(updated, rest)
      {:error, _} = err -> err
    end
  end

  @spec parse_rule(user(), binary()) :: {:ok, user()} | {:error, binary()}
  defp parse_rule(user, "on"), do: {:ok, %{user | enabled: true}}
  defp parse_rule(user, "off"), do: {:ok, %{user | enabled: false}}

  # Fix 1: Hash the password before storing.
  defp parse_rule(user, ">" <> password) do
    {:ok, %{user | password: hash_password(password)}}
  end

  defp parse_rule(user, "nopass"), do: {:ok, %{user | password: nil}}
  defp parse_rule(user, "resetpass"), do: {:ok, %{user | password: nil}}

  defp parse_rule(user, "~" <> pattern) do
    case user.keys do
      :all -> {:ok, %{user | keys: [pattern]}}
      patterns -> {:ok, %{user | keys: patterns ++ [pattern]}}
    end
  end

  defp parse_rule(user, "allkeys"), do: {:ok, %{user | keys: :all}}

  defp parse_rule(user, "allcommands") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_rule(user, "+@all") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_rule(user, "-@all"), do: {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}

  # +@category -- expand the category to individual commands and add them all.
  # When commands is :all, also remove the category commands from denied_commands.
  defp parse_rule(user, "+@" <> category) do
    cat = String.upcase(category)

    case Map.fetch(@category_map, cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            # Remove from denied_commands if they were explicitly denied
            new_denied = MapSet.difference(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.union(cmds, cat_cmds)}}
        end

      :error ->
        {:error, "ERR Error in ACL SETUSER modifier '+@#{category}': Unknown command category '#{category}'"}
    end
  end

  # -@category -- deny all commands in the category.
  # Fix 2: When commands is :all, add to denied_commands instead of ignoring.
  defp parse_rule(user, "-@" <> category) do
    cat = String.upcase(category)

    case Map.fetch(@category_map, cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            new_denied = MapSet.union(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.difference(cmds, cat_cmds)}}
        end

      :error ->
        {:error, "ERR Error in ACL SETUSER modifier '-@#{category}': Unknown command category '#{category}'"}
    end
  end

  # +command -- allow a specific command.
  # When commands is :all, remove from denied_commands if present.
  defp parse_rule(user, "+" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        new_denied = MapSet.delete(user.denied_commands, cmd)
        {:ok, %{user | denied_commands: new_denied}}

      cmds ->
        {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
    end
  end

  # -command -- deny a specific command.
  # Fix 2: When commands is :all, add to denied_commands instead of ignoring.
  defp parse_rule(user, "-" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        new_denied = MapSet.put(user.denied_commands, cmd)
        {:ok, %{user | denied_commands: new_denied}}

      cmds ->
        {:ok, %{user | commands: MapSet.delete(cmds, cmd)}}
    end
  end

  defp parse_rule(_user, rule) do
    {:error, "ERR Error in ACL SETUSER modifier '#{rule}': Syntax error"}
  end

  # ---------------------------------------------------------------------------
  # Private -- formatting
  # ---------------------------------------------------------------------------

  @spec format_user_rule({binary(), user()}) :: binary()
  defp format_user_rule({name, user}) do
    flag = if user.enabled, do: "on", else: "off"
    keys = format_keys(user.keys)
    cmds = format_commands(user.commands)
    "user #{name} #{flag} #{keys} &* #{cmds}"
  end

  @spec format_commands(:all | MapSet.t(binary())) :: binary()
  defp format_commands(:all), do: "+@all"

  defp format_commands(cmds) when is_struct(cmds, MapSet) do
    cmds
    |> MapSet.to_list()
    |> Enum.sort()
    |> Enum.map_join(" ", &"+#{String.downcase(&1)}")
  end

  @spec format_keys(:all | [binary()]) :: binary()
  defp format_keys(:all), do: "~*"

  defp format_keys(patterns) when is_list(patterns) do
    Enum.map_join(patterns, " ", &"~#{&1}")
  end

  # Displays the stored password hash as a SHA-256 hex digest (for ACL GETUSER).
  # The stored value is already a base64-encoded salt+hash, so we hash that
  # representation to produce a stable display value.
  @spec hash_for_display(binary()) :: binary()
  defp hash_for_display(stored_hash) do
    :crypto.hash(:sha256, stored_hash) |> Base.encode16(case: :lower)
  end

  # ---------------------------------------------------------------------------
  # Private -- default user
  # ---------------------------------------------------------------------------

  @spec insert_default_user() :: true
  defp insert_default_user do
    :ets.insert(@table, {"default", %{
      enabled: true,
      password: nil,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: :all
    }})
  end
end
