# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Client do
  @moduledoc """
  Handles Redis CLIENT subcommands: ID, SETNAME, GETNAME, INFO, LIST.

  Unlike most command modules, CLIENT commands require access to per-connection
  state (client ID, client name, address, creation time). The connection layer
  passes this state via a `conn_state` map with the following shape:

      %{
        client_id: pos_integer(),
        client_name: binary() | nil,
        created_at: integer(),         # monotonic ms
        peer: {ip_tuple, port}
      }

  ## Supported subcommands

    * `CLIENT ID` — returns the unique client connection ID
    * `CLIENT SETNAME name` — sets the connection name
    * `CLIENT GETNAME` — returns the connection name or nil
    * `CLIENT INFO` — returns info about the current connection
    * `CLIENT LIST [TYPE normal|master|replica|pubsub]` — returns info about all connections
  """

  @doc """
  Handles a CLIENT subcommand.

  ## Parameters

    - `subcmd` - Uppercased subcommand name (e.g. `"ID"`, `"SETNAME"`)
    - `args` - List of string arguments after the subcommand
    - `conn_state` - Per-connection state map
    - `store` - Injected store map (unused by most CLIENT commands)

  ## Returns

  A tuple `{result, updated_conn_state}` when the command mutates connection state
  (e.g. SETNAME), or `{result, conn_state}` for read-only commands.
  """
  @spec handle(binary(), [binary()], map(), map()) :: {term(), map()}
  def handle(subcmd, args, conn_state, store)

  # ---------------------------------------------------------------------------
  # CLIENT ID
  # ---------------------------------------------------------------------------

  def handle("ID", [], conn_state, _store) do
    {conn_state.client_id, conn_state}
  end

  def handle("ID", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|id' command"}, conn_state}
  end

  # ---------------------------------------------------------------------------
  # CLIENT SETNAME
  # ---------------------------------------------------------------------------

  def handle("SETNAME", [name], conn_state, _store) do
    if String.contains?(name, " ") do
      {{:error, "ERR Client names cannot contain spaces, newlines or special characters."},
       conn_state}
    else
      {:ok, %{conn_state | client_name: name}}
    end
  end

  def handle("SETNAME", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|setname' command"}, conn_state}
  end

  # ---------------------------------------------------------------------------
  # CLIENT GETNAME
  # ---------------------------------------------------------------------------

  def handle("GETNAME", [], conn_state, _store) do
    {conn_state.client_name, conn_state}
  end

  def handle("GETNAME", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|getname' command"}, conn_state}
  end

  # ---------------------------------------------------------------------------
  # CLIENT INFO
  # ---------------------------------------------------------------------------

  def handle("INFO", [], conn_state, _store) do
    {format_client_info(conn_state), conn_state}
  end

  def handle("INFO", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|info' command"}, conn_state}
  end

  # ---------------------------------------------------------------------------
  # CLIENT LIST [TYPE type]
  # ---------------------------------------------------------------------------

  def handle("LIST", args, conn_state, _store) when args in [[], ["TYPE", "normal"]] do
    # For now, list only the current connection. A full implementation would
    # iterate all Ranch connections, but that requires a registry or ETS table.
    # Returning the current connection is the simplest correct behaviour.
    info = format_client_info(conn_state)
    {info, conn_state}
  end

  def handle("LIST", ["TYPE", type], conn_state, _store)
      when type in ~w(master replica pubsub) do
    # FerricStore has no replication or pub/sub connections — return empty.
    {"", conn_state}
  end

  def handle("LIST", ["TYPE", _bad], conn_state, _store) do
    {{:error, "ERR Unknown client type 'unknown'"}, conn_state}
  end

  def handle("LIST", _args, conn_state, _store) do
    {{:error, "ERR syntax error"}, conn_state}
  end

  # ---------------------------------------------------------------------------
  # Unknown subcommand
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Private — formatting
  # ---------------------------------------------------------------------------

  defp format_client_info(conn_state) do
    id = conn_state.client_id
    name = conn_state.client_name || ""

    {addr, fd} =
      case conn_state do
        %{peer: {ip, port}} ->
          ip_str = :inet.ntoa(ip) |> to_string()
          {"#{ip_str}:#{port}", 0}

        _ ->
          {"unknown:0", 0}
      end

    age =
      div(System.monotonic_time(:millisecond) - conn_state.created_at, 1000)

    "id=#{id} addr=#{addr} fd=#{fd} name=#{name} age=#{age}\n"
  end

  # --- Stubs for CLIENT subcommands that FerricStore doesn't fully implement ---

  def handle("KILL", _args, conn_state, _store), do: {:ok, conn_state}
  def handle("PAUSE", _args, conn_state, _store), do: {:ok, conn_state}
  def handle("UNPAUSE", _args, conn_state, _store), do: {:ok, conn_state}

  def handle("NO-EVICT", [flag], conn_state, _store) when flag in ~w(ON OFF on off) do
    {:ok, conn_state}
  end

  def handle("NO-EVICT", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|no-evict' command"}, conn_state}
  end

  def handle("NO-TOUCH", [flag], conn_state, _store) when flag in ~w(ON OFF on off) do
    {:ok, conn_state}
  end

  def handle("NO-TOUCH", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|no-touch' command"}, conn_state}
  end

  # Catch-all for unknown subcommands — must be last
  def handle(subcmd, _args, conn_state, _store) do
    {{:error, "ERR unknown subcommand '#{subcmd}'. Try CLIENT HELP."}, conn_state}
  end
end
