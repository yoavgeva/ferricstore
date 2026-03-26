defmodule FerricStore.Sandbox.Case do
  @moduledoc """
  ExUnit case template that automatically checks out and checks in a
  FerricStore sandbox for each test.

  Using this module in a test module is equivalent to writing:

      setup do
        sandbox = FerricStore.Sandbox.checkout()
        on_exit(fn -> FerricStore.Sandbox.checkin(sandbox) end)
        %{sandbox: sandbox}
      end

  ## Usage

      defmodule MyApp.CacheTest do
        use ExUnit.Case, async: true
        use FerricStore.Sandbox.Case

        test "transparent isolation" do
          FerricStore.set("key", "value")
          assert {:ok, "value"} = FerricStore.get("key")
        end
      end

  ## Options

  Options passed to `use FerricStore.Sandbox.Case` are forwarded to
  `FerricStore.Sandbox.checkout/1`:

      use FerricStore.Sandbox.Case, freeze_ttl: true

  """

  @doc false
  defmacro __using__(opts) do
    quote do
      setup do
        sandbox = FerricStore.Sandbox.checkout(unquote(opts))
        on_exit(fn -> FerricStore.Sandbox.checkin(sandbox) end)
        %{sandbox: sandbox}
      end
    end
  end
end
