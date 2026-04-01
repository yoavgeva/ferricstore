defmodule Ferricstore.Review.H5ExpireatPastTest do
  @moduledoc """
  Verifies that EXPIREAT and PEXPIREAT with past timestamps immediately
  delete the key, matching Redis behavior.

  Previously the timestamp was passed to apply_expiry without validation,
  causing the key to linger with a past expiry (or become immortal for
  timestamp=0). Now past/zero timestamps call delete_if_exists.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Expiry
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # EXPIREAT with past timestamp
  # ---------------------------------------------------------------------------

  describe "EXPIREAT past timestamp deletes key" do
    test "past non-zero timestamp deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})

      # Timestamp 1000 = 1970 — firmly in the past.
      assert 1 == Expiry.handle("EXPIREAT", ["k", "1000"], store)

      # Key is deleted, not just lazily expired.
      assert store.get.("k") == nil
      assert Expiry.handle("TTL", ["k"], store) == -2
    end

    test "EXPIREAT 0 deletes the key (not immortal)" do
      store = MockStore.make(%{"k" => {"v", 0}})

      assert 1 == Expiry.handle("EXPIREAT", ["k", "0"], store)

      # Key is gone — 0 no longer treated as "no expiry" sentinel.
      assert store.get.("k") == nil
      assert Expiry.handle("TTL", ["k"], store) == -2
    end

    test "EXPIREAT on non-existent key returns 0" do
      store = MockStore.make(%{})

      assert 0 == Expiry.handle("EXPIREAT", ["missing", "1000"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # PEXPIREAT with past timestamp
  # ---------------------------------------------------------------------------

  describe "PEXPIREAT past timestamp deletes key" do
    test "past non-zero ms timestamp deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})

      # 1_000_000 ms = 1000 seconds after epoch — still 1970.
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "1000000"], store)

      assert store.get.("k") == nil
      assert Expiry.handle("PTTL", ["k"], store) == -2
    end

    test "PEXPIREAT 0 deletes the key (not immortal)" do
      store = MockStore.make(%{"k" => {"v", 0}})

      assert 1 == Expiry.handle("PEXPIREAT", ["k", "0"], store)

      assert store.get.("k") == nil
      assert Expiry.handle("PTTL", ["k"], store) == -2
    end

    test "PEXPIREAT on non-existent key returns 0" do
      store = MockStore.make(%{})

      assert 0 == Expiry.handle("PEXPIREAT", ["missing", "1000000"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # EXPIRE/PEXPIRE still work correctly (contrast / regression)
  # ---------------------------------------------------------------------------

  describe "EXPIRE/PEXPIRE handle non-positive values correctly" do
    test "EXPIRE with negative seconds returns error (Redis 7+)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, msg} = Expiry.handle("EXPIRE", ["k", "-1"], store)
      assert msg =~ "invalid expire time"
      # Key not modified
      assert store.get.("k") == "v"
    end

    test "EXPIRE 0 deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "0"], store)
      assert store.get.("k") == nil
      assert Expiry.handle("TTL", ["k"], store) == -2
    end

    test "PEXPIRE with negative ms returns error (Redis 7+)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, msg} = Expiry.handle("PEXPIRE", ["k", "-1"], store)
      assert msg =~ "invalid expire time"
      assert store.get.("k") == "v"
    end
  end

  # ---------------------------------------------------------------------------
  # Future timestamps still work
  # ---------------------------------------------------------------------------

  describe "future timestamps set expiry correctly" do
    test "EXPIREAT with future timestamp sets TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      future = div(System.os_time(:millisecond), 1000) + 3600

      assert 1 == Expiry.handle("EXPIREAT", ["k", Integer.to_string(future)], store)

      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 3600
    end

    test "PEXPIREAT with future timestamp sets PTTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      future = System.os_time(:millisecond) + 3_600_000

      assert 1 == Expiry.handle("PEXPIREAT", ["k", Integer.to_string(future)], store)

      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0 and pttl <= 3_600_000
    end
  end
end
