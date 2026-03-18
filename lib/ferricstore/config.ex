defmodule Ferricstore.Config do
  @moduledoc """
  GenServer managing runtime configuration for FerricStore.

  Provides a Redis-compatible `CONFIG GET`/`CONFIG SET` interface for reading
  and writing server parameters at runtime. All values are stored as strings
  to match Redis behaviour.

  ## Default values

  The following parameters are initialised with sensible defaults matching
  Redis conventions:

    * `"maxmemory"` — `"0"` (unlimited)
    * `"maxmemory-policy"` — `"noeviction"`
    * `"hz"` — `"10"`
    * `"timeout"` — `"0"` (no idle timeout)
    * `"tcp-keepalive"` — `"300"`
    * `"databases"` — `"1"`
    * `"bind"` — `"127.0.0.1"`
    * `"port"` — `"6379"`
    * `"requirepass"` — `""` (no password)
    * `"maxclients"` — `"10000"`

  ## Usage

      Ferricstore.Config.get("max*")
      #=> [{"maxmemory", "0"}, {"maxmemory-policy", "noeviction"}, {"maxclients", "10000"}]

      Ferricstore.Config.set("hz", "20")
      #=> :ok
  """

  use GenServer

  @defaults %{
    "maxmemory" => "0",
    "maxmemory-policy" => "noeviction",
    "hz" => "10",
    "slowlog-log-slower-than" => "10000",
    "slowlog-max-len" => "128",
    "timeout" => "0",
    "tcp-keepalive" => "300",
    "databases" => "1",
    "bind" => "127.0.0.1",
    "port" => "6379",
    "save" => "",
    "appendonly" => "no",
    "notify-keyspace-events" => "",
    "loglevel" => "notice",
    "requirepass" => "",
    "maxclients" => "10000",
    "sandbox_mode" => "disabled"
  }

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts the Config GenServer and registers it under `Ferricstore.Config`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, @defaults, name: __MODULE__)
  end

  @doc """
  Returns all config key-value pairs whose keys match the given glob `pattern`.

  The pattern supports `*` (match any sequence) and `?` (match single char).

  ## Parameters

    - `pattern` — glob pattern string (e.g. `"*"`, `"max*"`, `"hz"`)

  ## Returns

  A list of `{key, value}` tuples for every matching parameter, sorted by key.

  ## Examples

      iex> Ferricstore.Config.get("hz")
      [{"hz", "10"}]

      iex> Ferricstore.Config.get("nonexistent")
      []
  """
  @spec get(binary()) :: [{binary(), binary()}]
  def get(pattern) do
    GenServer.call(__MODULE__, {:get, pattern})
  end

  @doc """
  Sets a runtime configuration parameter.

  ## Parameters

    - `key` — parameter name (e.g. `"hz"`, `"maxmemory"`)
    - `value` — new value as a string

  ## Returns

  `:ok`
  """
  @spec set(binary(), binary()) :: :ok
  def set(key, value) do
    GenServer.call(__MODULE__, {:set, key, value})
  end

  @doc """
  Returns the current value for a single config key, or `nil` if not set.

  This is a convenience for internal use (e.g. checking `requirepass`).
  """
  @spec get_value(binary()) :: binary() | nil
  def get_value(key) do
    GenServer.call(__MODULE__, {:get_value, key})
  end

  @doc """
  Returns the full map of default configuration values.
  """
  @spec defaults() :: %{binary() => binary()}
  def defaults, do: @defaults

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(defaults) do
    {:ok, defaults}
  end

  @impl true
  def handle_call({:get, pattern}, _from, state) do
    regex = glob_to_regex(pattern)

    result =
      state
      |> Enum.filter(fn {key, _val} -> Regex.match?(regex, key) end)
      |> Enum.sort_by(fn {key, _val} -> key end)

    {:reply, result, state}
  end

  def handle_call({:set, key, value}, _from, state) do
    {:reply, :ok, Map.put(state, key, value)}
  end

  def handle_call({:get_value, key}, _from, state) do
    {:reply, Map.get(state, key), state}
  end

  # -------------------------------------------------------------------
  # Private — glob-to-regex conversion
  # -------------------------------------------------------------------

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
