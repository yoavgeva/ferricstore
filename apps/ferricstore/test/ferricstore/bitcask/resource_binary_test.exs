defmodule Ferricstore.Bitcask.ResourceBinaryTest do
  @moduledoc """
  Tests for the zero-copy `get_zero_copy/2` NIF that returns a BEAM binary
  backed by a Rust `ValueBuffer` resource (enif_make_resource_binary).
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "resource_binary_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    dir
  end

  defp open_store(dir) do
    {:ok, store} = NIF.new(dir)
    store
  end

  # ------------------------------------------------------------------
  # get_zero_copy/2 — basic correctness
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 basic" do
    test "returns same value as regular get" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "hello", "world", 0)

      assert {:ok, "world"} = NIF.get(store, "hello")
      assert {:ok, "world"} = NIF.get_zero_copy(store, "hello")
    end

    test "returns {:ok, nil} for missing key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      assert {:ok, nil} = NIF.get_zero_copy(store, "nonexistent")
    end

    test "empty value is treated as tombstone (nil)" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      # In Bitcask, value_size == 0 is a tombstone — both get and
      # get_zero_copy return nil for empty values.
      :ok = NIF.put(store, "empty", "", 0)

      assert {:ok, nil} = NIF.get(store, "empty")
      assert {:ok, nil} = NIF.get_zero_copy(store, "empty")
    end

    test "works with large value (1 MB)" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      large_value = :crypto.strong_rand_bytes(1_048_576)
      :ok = NIF.put(store, "big", large_value, 0)

      assert {:ok, ^large_value} = NIF.get_zero_copy(store, "big")
    end

    test "works with binary key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      key = <<0, 1, 2, 255>>
      value = <<128, 129, 130>>
      :ok = NIF.put(store, key, value, 0)

      assert {:ok, ^value} = NIF.get_zero_copy(store, key)
    end
  end

  # ------------------------------------------------------------------
  # Returned binary is a valid Elixir binary
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 binary validity" do
    test "returned binary supports byte_size/1" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "value123", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "k")
      assert byte_size(bin) == 8
    end

    test "returned binary supports pattern matching" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "hello world", 0)

      {:ok, <<"hello", rest::binary>>} = NIF.get_zero_copy(store, "k")
      assert rest == " world"
    end

    test "returned binary supports Enum/String operations" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "ABCDEF", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "k")
      assert String.downcase(bin) == "abcdef"
      assert String.length(bin) == 6
      assert :binary.bin_to_list(bin) == ~c"ABCDEF"
    end

    test "returned binary can be used as map key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "mapkey", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "k")
      map = %{bin => :found}
      assert map["mapkey"] == :found
    end

    test "returned binary is identical to regular get binary" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      payload = :crypto.strong_rand_bytes(4096)
      :ok = NIF.put(store, "k", payload, 0)

      {:ok, regular} = NIF.get(store, "k")
      {:ok, zero_copy} = NIF.get_zero_copy(store, "k")

      assert regular == zero_copy
      assert byte_size(regular) == byte_size(zero_copy)
    end
  end

  # ------------------------------------------------------------------
  # Expiry behaviour matches regular get
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 expiry" do
    test "returns nil for expired key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      # Expire in the past
      past_ms = System.os_time(:millisecond) - 1000
      :ok = NIF.put(store, "ephemeral", "gone", past_ms)

      assert {:ok, nil} = NIF.get_zero_copy(store, "ephemeral")
    end

    test "returns value for non-expired key" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      future_ms = System.os_time(:millisecond) + 60_000
      :ok = NIF.put(store, "alive", "present", future_ms)

      assert {:ok, "present"} = NIF.get_zero_copy(store, "alive")
    end
  end

  # ------------------------------------------------------------------
  # Concurrent access
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 concurrency" do
    test "concurrent gets return correct values" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)

      # Write 100 keys
      for i <- 1..100 do
        :ok = NIF.put(store, "key:#{i}", "value:#{i}", 0)
      end

      # Read them concurrently from 10 tasks
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for i <- 1..100 do
              {:ok, val} = NIF.get_zero_copy(store, "key:#{i}")
              assert val == "value:#{i}"
            end
          end)
        end

      Enum.each(tasks, &Task.await(&1, 10_000))
    end
  end

  # ------------------------------------------------------------------
  # GC / resource cleanup
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 resource cleanup" do
    test "multiple gets do not leak memory (GC cleans up resources)" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", :crypto.strong_rand_bytes(8192), 0)

      # Perform many reads, discarding results. If resources leaked,
      # memory would grow unboundedly. We just verify no crash occurs
      # and the process memory stays bounded.
      initial_mem = :erlang.process_info(self(), :memory) |> elem(1)

      for _ <- 1..10_000 do
        {:ok, _} = NIF.get_zero_copy(store, "k")
      end

      :erlang.garbage_collect()
      final_mem = :erlang.process_info(self(), :memory) |> elem(1)

      # Allow generous headroom — the point is no unbounded growth.
      # 10,000 x 8KB = 80MB if leaked; we expect << 1MB growth.
      assert final_mem - initial_mem < 2_000_000,
             "Possible resource leak: memory grew by #{final_mem - initial_mem} bytes"
    end

    test "returned binary survives garbage collection of intermediaries" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "survive_gc", 0)

      {:ok, bin} = NIF.get_zero_copy(store, "k")
      # Force GC to collect any temporary resources
      :erlang.garbage_collect()

      # Binary should still be valid and readable
      assert bin == "survive_gc"
      assert byte_size(bin) == 10
    end
  end

  # ------------------------------------------------------------------
  # Delete + re-read
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 after delete" do
    test "returns nil after key is deleted" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "del_me", "value", 0)
      assert {:ok, "value"} = NIF.get_zero_copy(store, "del_me")

      {:ok, true} = NIF.delete(store, "del_me")
      assert {:ok, nil} = NIF.get_zero_copy(store, "del_me")
    end
  end

  # ------------------------------------------------------------------
  # Overwrite semantics
  # ------------------------------------------------------------------

  describe "get_zero_copy/2 after overwrite" do
    test "returns latest value after overwrite" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "v1", 0)
      assert {:ok, "v1"} = NIF.get_zero_copy(store, "k")

      :ok = NIF.put(store, "k", "v2_longer", 0)
      assert {:ok, "v2_longer"} = NIF.get_zero_copy(store, "k")
    end

    test "old binary remains valid after key overwrite" do
      dir = tmp_dir()
      on_exit(fn -> File.rm_rf!(dir) end)

      store = open_store(dir)
      :ok = NIF.put(store, "k", "original", 0)
      {:ok, old_bin} = NIF.get_zero_copy(store, "k")

      :ok = NIF.put(store, "k", "replaced", 0)
      {:ok, new_bin} = NIF.get_zero_copy(store, "k")

      # The old binary is backed by a separate ValueBuffer resource
      # and must remain valid even after the key is overwritten.
      assert old_bin == "original"
      assert new_bin == "replaced"
    end
  end
end
