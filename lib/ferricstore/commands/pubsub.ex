defmodule Ferricstore.Commands.PubSub do
  @moduledoc """
  Handles Redis pub/sub commands that go through the normal dispatcher:
  PUBLISH, and PUBSUB (with subcommands CHANNELS, NUMSUB, NUMPAT).

  The SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, and PUNSUBSCRIBE commands are
  handled directly in `Ferricstore.Server.Connection` because they require
  per-connection state management (pub/sub mode tracking, subscription counts).
  """

  alias Ferricstore.PubSub

  @doc """
  Handles a PUBLISH or PUBSUB command.

  ## PUBLISH channel message

  Publishes `message` to the given `channel`. Returns the number of subscribers
  that received the message (integer).

  ## PUBSUB CHANNELS [pattern]

  Lists channels with active subscribers. When `pattern` is given, only
  channels matching the glob pattern are returned.

  ## PUBSUB NUMSUB [channel ...]

  Returns a flat list of `[channel, count, channel, count, ...]` with
  subscriber counts for each specified channel.

  ## PUBSUB NUMPAT

  Returns the total number of active pattern subscriptions (from PSUBSCRIBE).

  ## Parameters

    - `cmd`  - Uppercased command name (`"PUBLISH"` or `"PUBSUB"`)
    - `args` - List of string arguments

  ## Returns

  Plain Elixir terms suitable for RESP encoding.
  """
  @spec handle(binary(), [binary()]) :: term()
  def handle(cmd, args)

  # PUBLISH channel message
  def handle("PUBLISH", [channel, message]) do
    PubSub.publish(channel, message)
  end

  def handle("PUBLISH", _args) do
    {:error, "ERR wrong number of arguments for 'publish' command"}
  end

  # PUBSUB subcommand dispatch
  def handle("PUBSUB", [subcommand | args]) do
    case String.upcase(subcommand) do
      "CHANNELS" -> handle_channels(args)
      "NUMSUB" -> handle_numsub(args)
      "NUMPAT" -> handle_numpat(args)
      other -> {:error, "ERR unknown subcommand '#{String.downcase(other)}'. Try PUBSUB HELP."}
    end
  end

  def handle("PUBSUB", []) do
    {:error, "ERR wrong number of arguments for 'pubsub' command"}
  end

  # ---------------------------------------------------------------------------
  # PUBSUB subcommand handlers
  # ---------------------------------------------------------------------------

  defp handle_channels([]), do: PubSub.channels()
  defp handle_channels([pattern]), do: PubSub.channels(pattern)

  defp handle_channels(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|channels' command"}
  end

  defp handle_numsub(channels), do: PubSub.numsub(channels)

  defp handle_numpat([]), do: PubSub.numpat()

  defp handle_numpat(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|numpat' command"}
  end
end
