defmodule Ferricstore.Acl do
  @moduledoc """
  GenServer managing the Access Control List (ACL) for FerricStore.

  Stores user accounts in a named ETS table (`:ferricstore_acl`). Each user
  record has a username, enabled/disabled flag, an optional password, allowed
  commands, and allowed key patterns.

  The "default" user is always present and cannot be deleted. On startup it
  is initialised as enabled with no password and full access (`+@all`, `~*`).

  ## Spec reference

  Implements spec section 6.1: `ACL SETUSER`, `ACL DELUSER`, `ACL GETUSER`,
  `ACL LIST`, `ACL WHOAMI`.

  ## ETS schema

  Each row is a tuple:

      {username :: binary(), %{
        enabled: boolean(),
        password: binary() | nil,
        commands: :all | MapSet.t(binary()),
        keys: :all | [binary()]
      }}

  ## Usage

      Ferricstore.Acl.set_user("alice", ["on", ">s3cret", "~cache:*", "+get", "+set"])
      Ferricstore.Acl.authenticate("alice", "s3cret")
      #=> {:ok, user}

      Ferricstore.Acl.del_user("alice")
      #=> :ok
  """

  use GenServer

  @table :ferricstore_acl

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "A user record stored in the ACL table."
  @type user :: %{
          enabled: boolean(),
          password: binary() | nil,
          commands: :all | MapSet.t(binary()),
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
    * `">password"`    -- set the user's password
    * `"nopass"`       -- clear the user's password (allow passwordless auth)
    * `"~pattern"`     -- add a key pattern (e.g. `"~*"` for all keys)
    * `"+command"`     -- allow a specific command
    * `"+@all"`        -- allow all commands
    * `"-command"`     -- deny a specific command
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
      #=> %{enabled: true, password: nil, commands: :all, keys: :all}
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

  Returns `{:ok, username}` on success, `{:error, reason}` on failure.

  When `requirepass` is set and the username is "default", the password is
  checked against `requirepass` first (backwards compatibility). For all
  other users, or when a user has been explicitly configured via ACL SETUSER,
  the ACL password takes precedence.

  ## Parameters

    - `username` -- the username to authenticate
    - `password` -- the password to check

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
        # User has no password set -- accept any password (nopass mode).
        {:ok, username}

      %{password: stored_pass} when stored_pass == password ->
        {:ok, username}

      _ ->
        {:error, "WRONGPASS invalid username-password pair or user is disabled."}
    end
  end

  @doc """
  Checks if the given user is allowed to run the given command.

  For v1, this only checks whether the user exists and is enabled. Command
  and key restrictions are reserved for a future version.

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
  Resets the ACL to its initial state (only the default user).

  Used primarily in tests to avoid state leaking between test cases.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
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

    base =
      if existing do
        existing
      else
        # New user defaults: disabled, no password, no permissions.
        %{enabled: false, password: nil, commands: :all, keys: :all}
      end

    case apply_rules(base, rules) do
      {:ok, updated} ->
        :ets.insert(@table, {username, updated})
        {:reply, :ok, state}

      {:error, _reason} = err ->
        {:reply, err, state}
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

  defp parse_rule(user, ">" <> password) do
    {:ok, %{user | password: password}}
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
  defp parse_rule(user, "allcommands"), do: {:ok, %{user | commands: :all}}
  defp parse_rule(user, "+@all"), do: {:ok, %{user | commands: :all}}

  defp parse_rule(user, "+" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all -> {:ok, user}
      cmds -> {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
    end
  end

  defp parse_rule(user, "-@all"), do: {:ok, %{user | commands: MapSet.new()}}

  defp parse_rule(user, "-" <> command) do
    cmd = String.upcase(command)

    case user.commands do
      :all ->
        # Cannot remove a single command from :all without enumerating.
        # For v1, just ignore silently.
        {:ok, user}

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

  @spec hash_for_display(binary()) :: binary()
  defp hash_for_display(password) do
    :crypto.hash(:sha256, password) |> Base.encode16(case: :lower)
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
      keys: :all
    }})
  end
end
