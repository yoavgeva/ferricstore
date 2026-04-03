defmodule Ferricstore.ModeTest do
  @moduledoc """
  Tests for `Ferricstore.Mode` — runtime mode detection.

  Verifies that the mode API correctly reads from application env and that
  the boolean helpers (`standalone?/0`, `embedded?/0`) agree with `current/0`.
  Tests run with `async: false` because they mutate shared application env.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Mode

  # Capture the original mode and restore it after each test to avoid
  # polluting other test modules.
  setup do
    original = Application.get_env(:ferricstore, :mode)
    on_exit(fn -> restore_mode(original) end)
    %{original: original}
  end

  defp restore_mode(nil), do: Application.delete_env(:ferricstore, :mode)
  defp restore_mode(val), do: Application.put_env(:ferricstore, :mode, val)

  # ---------------------------------------------------------------------------
  # current/0
  # ---------------------------------------------------------------------------

  describe "current/0" do
    test "returns :embedded when :mode is not configured (default)" do
      Application.delete_env(:ferricstore, :mode)
      assert Mode.current() == :embedded
    end

    test "returns :standalone when explicitly set" do
      Application.put_env(:ferricstore, :mode, :standalone)
      assert Mode.current() == :standalone
    end

    test "returns :embedded when set" do
      Application.put_env(:ferricstore, :mode, :embedded)
      assert Mode.current() == :embedded
    end
  end

  # ---------------------------------------------------------------------------
  # standalone?/0
  # ---------------------------------------------------------------------------

  describe "standalone?/0" do
    test "returns true when mode is :standalone" do
      Application.put_env(:ferricstore, :mode, :standalone)
      assert Mode.standalone?() == true
    end

    test "returns false when mode is not configured (default is :embedded)" do
      Application.delete_env(:ferricstore, :mode)
      assert Mode.standalone?() == false
    end

    test "returns false when mode is :embedded" do
      Application.put_env(:ferricstore, :mode, :embedded)
      assert Mode.standalone?() == false
    end
  end

  # ---------------------------------------------------------------------------
  # embedded?/0
  # ---------------------------------------------------------------------------

  describe "embedded?/0" do
    test "returns true when mode is :embedded" do
      Application.put_env(:ferricstore, :mode, :embedded)
      assert Mode.embedded?() == true
    end

    test "returns false when mode is :standalone" do
      Application.put_env(:ferricstore, :mode, :standalone)
      assert Mode.embedded?() == false
    end

    test "returns true when mode is not configured (default is :embedded)" do
      Application.delete_env(:ferricstore, :mode)
      assert Mode.embedded?() == true
    end
  end

  # ---------------------------------------------------------------------------
  # Consistency: standalone? and embedded? are mutually exclusive
  # ---------------------------------------------------------------------------

  describe "mutual exclusivity" do
    test "standalone? and embedded? cannot both be true" do
      for mode <- [:standalone, :embedded] do
        Application.put_env(:ferricstore, :mode, mode)
        refute Mode.standalone?() and Mode.embedded?()
      end
    end

    test "exactly one of standalone? or embedded? is true for known modes" do
      for mode <- [:standalone, :embedded] do
        Application.put_env(:ferricstore, :mode, mode)
        assert Mode.standalone?() != Mode.embedded?()
      end
    end
  end
end
