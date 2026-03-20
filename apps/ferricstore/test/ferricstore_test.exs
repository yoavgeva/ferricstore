defmodule FerricstoreTest do
  use ExUnit.Case

  test "FerricStore module is available" do
    assert Code.ensure_loaded?(FerricStore)
  end
end
