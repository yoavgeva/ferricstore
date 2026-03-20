defmodule Ferricstore.Test.ClusterCase do
  @moduledoc """
  Base ExUnit case template for cluster tests.

  Starts a multi-node FerricStore cluster in `setup_all` and tears it down
  via `on_exit`. Individual test modules can override the cluster size
  by setting `@moduletag cluster_size: N`.

  All tests using this case template should be tagged with `@tag :cluster`
  so they can be excluded from fast CI runs. Run them with:

      mix test test/ferricstore/cluster/ --include cluster

  ## `:peer` Availability

  If the `:peer` module is not available (OTP < 25), the setup raises
  and the test module is marked as invalid with a clear message.
  """

  use ExUnit.CaseTemplate

  alias Ferricstore.Test.ClusterHelper

  using do
    quote do
      alias Ferricstore.Test.ClusterHelper
    end
  end

  setup_all context do
    unless ClusterHelper.peer_available?() do
      raise "Skipping cluster tests: :peer not available (requires OTP 25+)"
    end

    n = Map.get(context, :cluster_size, 3)
    opts = Map.get(context, :cluster_opts, [])

    nodes = ClusterHelper.start_cluster(n, opts)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

    %{nodes: nodes}
  end
end
