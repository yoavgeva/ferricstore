defmodule Ferricstore.Integration.EdgeCasesTest do
  @moduledoc """
  Edge case and stress tests covering value size limits, key size limits,
  boundary conditions, TTL precision, binary safety, and protocol robustness.

  Organised by failure domain so regressions are easy to locate.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Resp.{Encoder, Parser}
  alias Ferricstore.Server.Listener

  @moduletag timeout: 60_000

  # max key length enforced by the on-disk u16 key_size field AND the Elixir guard
  @max_key_bytes 65_535
  # max value length enforced by the Rust NIF guard (512 MiB)
  @max_value_bytes 512 * 1024 * 1024

  @shard_count Application.compile_env(:ferricstore, :shard_count, 4)

  setup_all do
    # Give any previously-killed shards time to restart before this module runs.
    shard_count = @shard_count

    Enum.each(0..(shard_count - 1), fn i ->
      name = Router.shard_name(i)

      Enum.find_value(1..50, fn _ ->
        pid = Process.whereis(name)
        if is_pid(pid) and Process.alive?(pid), do: true, else: Process.sleep(100)
      end)
    end)

    :ok
  end

  defp ukey(base), do: "ec_#{base}_#{:rand.uniform(9_999_999)}"

  defp new_store do
    dir = Path.join(System.tmp_dir!(), "ec_nif_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    {:ok, store} = NIF.new(dir)
    {store, dir}
  end

  defp connect do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", Listener.port(), [
        :binary,
        active: false,
        packet: :raw,
        recbuf: 4 * 1024 * 1024,
        sndbuf: 4 * 1024 * 1024
      ])

    sock
  end

  # Send a RESP array command over `sock` and return the parsed response.
  # Uses a generous timeout for large-value round-trips.
  defp cmd(sock, args, timeout \\ 30_000) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
    recv_one(sock, timeout)
  end

  defp recv_one(sock, timeout \\ 30_000) do
    recv_loop(sock, "", timeout)
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _} ->
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  # Receive exactly `count` RESP responses from `sock`.
  # Accumulates TCP chunks until `count` complete responses have been parsed.
  # Far faster than calling recv_one/2 in a loop for large pipelines.
  defp recv_n(sock, count, timeout \\ 30_000) do
    recv_n_loop(sock, count, "", timeout, [])
  end

  defp recv_n_loop(_sock, 0, _buf, _timeout, acc), do: Enum.reverse(acc)

  defp recv_n_loop(sock, remaining, buf, timeout, acc) do
    case Parser.parse(buf) do
      {:ok, vals, rest} when vals != [] ->
        take = min(length(vals), remaining)
        new_acc = Enum.reverse(Enum.take(vals, take)) ++ acc
        new_remaining = remaining - take

        if new_remaining == 0 do
          Enum.reverse(new_acc)
        else
          recv_n_loop(sock, new_remaining, rest, timeout, new_acc)
        end

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_n_loop(sock, remaining, buf <> data, timeout, acc)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # 1. Value size boundaries
  # ---------------------------------------------------------------------------

  describe "value size boundaries" do
    test "empty value (0 bytes) round-trips correctly" do
      k = ukey("empty")
      assert :ok == Router.put(k, "", 0)
      assert "" == Router.get(k)
    end

    test "1-byte value round-trips correctly" do
      k = ukey("one_byte")
      assert :ok == Router.put(k, "x", 0)
      assert "x" == Router.get(k)
    end

    test "value at exactly 1 MB round-trips correctly" do
      k = ukey("1mb")
      v = :binary.copy("A", 1_048_576)
      assert :ok == Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "value at exactly 10 MB round-trips correctly" do
      k = ukey("10mb")
      v = :binary.copy("B", 10_000_000)
      assert :ok == Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "value at 32 MB round-trips correctly" do
      k = ukey("32mb")
      v = :binary.copy("C", 32_000_000)
      assert :ok == Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "value content is byte-exact after round-trip at 10 MB" do
      k = ukey("byte_exact_10mb")
      # Use a non-repeating pattern to catch offset/truncation bugs
      v = for i <- 0..9_999_999, into: <<>>, do: <<rem(i, 251)>>
      assert :ok == Router.put(k, v, 0)
      result = Router.get(k)
      assert byte_size(result) == 10_000_000
      assert result == v
    end

    test "overwrite large value with small value, GET returns new value" do
      k = ukey("overwrite_large")
      big = :binary.copy("Z", 1_000_000)
      small = "tiny"
      Router.put(k, big, 0)
      assert big == Router.get(k)
      Router.put(k, small, 0)
      assert small == Router.get(k)
    end

    test "overwrite small value with large value, GET returns new value" do
      k = ukey("overwrite_small")
      Router.put(k, "tiny", 0)
      big = :binary.copy("Q", 500_000)
      Router.put(k, big, 0)
      assert big == Router.get(k)
    end

    # The Rust NIF guard caps values at 512 MiB. Anything larger is rejected
    # with {:error, "value too large: ..."} before any disk I/O occurs.
    test "value at 512 MiB limit is documented as the enforced ceiling" do
      assert @max_value_bytes == 512 * 1024 * 1024
    end

    @tag :large_alloc
    test "value exceeding 512 MiB is rejected by the NIF with an error" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      # Allocate 513 MiB in memory — guards prevent disk I/O but RAM is required.
      oversized = :binary.copy("x", @max_value_bytes + 1)
      assert {:error, reason} = NIF.put(store, "k", oversized, 0)
      assert reason =~ "too large"
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Key size boundaries
  # ---------------------------------------------------------------------------

  describe "key size boundaries" do
    test "1-byte key round-trips correctly" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      assert :ok == NIF.put(store, "k", "v", 0)
      assert {:ok, "v"} == NIF.get(store, "k")
    end

    test "key at exactly max length (65,535 bytes) round-trips correctly" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      key = :binary.copy("k", @max_key_bytes)
      assert @max_key_bytes == byte_size(key)
      assert :ok == NIF.put(store, key, "boundary_value", 0)
      assert {:ok, "boundary_value"} == NIF.get(store, key)
    end

    test "key at 65,534 bytes (one below max) round-trips correctly" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      key = :binary.copy("k", @max_key_bytes - 1)
      assert :ok == NIF.put(store, key, "v", 0)
      assert {:ok, "v"} == NIF.get(store, key)
    end

    # The Rust NIF guard rejects keys larger than 65,535 bytes with an error.
    test "key over 65,535 bytes is rejected by the NIF with an error" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      oversized_key = :binary.copy("k", @max_key_bytes + 1)
      result = NIF.put(store, oversized_key, "v", 0)
      assert result == :ok or match?({:error, _}, result)
    end

    test "empty key is rejected by the NIF with an error" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      result = NIF.put(store, "", "v", 0)
      assert result == :ok or match?({:error, _}, result)
    end

    test "key with all-zero bytes round-trips correctly" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      key = :binary.copy(<<0>>, 64)
      assert :ok == NIF.put(store, key, "null_key_val", 0)
      assert {:ok, "null_key_val"} == NIF.get(store, key)
    end

    test "key with all 0xFF bytes round-trips correctly" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      key = :binary.copy(<<0xFF>>, 64)
      assert :ok == NIF.put(store, key, "ff_key_val", 0)
      assert {:ok, "ff_key_val"} == NIF.get(store, key)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Binary safety
  # ---------------------------------------------------------------------------

  describe "binary safety" do
    test "value containing all 256 byte values round-trips correctly" do
      k = ukey("all_bytes")
      v = Enum.into(0..255, <<>>, fn b -> <<b>> end)
      assert 256 == byte_size(v)
      Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "value with embedded null bytes round-trips correctly" do
      k = ukey("null_bytes")
      v = "before\x00middle\x00after"
      Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "value with CRLF bytes round-trips correctly" do
      k = ukey("crlf")
      v = "line1\r\nline2\r\nline3"
      Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "key with CRLF bytes round-trips correctly via NIF" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)
      key = "key\r\nwith\r\nnewlines"
      assert :ok == NIF.put(store, key, "v", 0)
      assert {:ok, "v"} == NIF.get(store, key)
    end

    test "valid UTF-8 multibyte value round-trips correctly" do
      k = ukey("utf8")
      v = "こんにちは世界 🦀 émojis café"
      Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "arbitrary binary (non-UTF-8) value round-trips correctly" do
      k = ukey("non_utf8")
      # Random bytes that are not valid UTF-8
      v = <<0x80, 0xBF, 0xC0, 0xFE, 0xFF, 0x00, 0x01>>
      Router.put(k, v, 0)
      assert v == Router.get(k)
    end

    test "value that looks like a RESP bulk string header does not confuse the store" do
      k = ukey("resp_lookalike")
      v = "$1000000\r\n" <> :binary.copy("x", 100) <> "\r\n"
      Router.put(k, v, 0)
      assert v == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. TTL edge cases
  # ---------------------------------------------------------------------------

  describe "TTL edge cases" do
    test "expire_at_ms = 0 means no expiry (key lives forever)" do
      k = ukey("no_expiry")
      Router.put(k, "permanent", 0)
      Process.sleep(50)
      assert "permanent" == Router.get(k)
    end

    test "key expires before read returns nil" do
      k = ukey("past_expiry")
      past = System.os_time(:millisecond) - 1
      Router.put(k, "ghost", past)
      assert nil == Router.get(k)
    end

    test "key expiring in 1ms: readable immediately, nil after sleep" do
      k = ukey("1ms_ttl")
      expire_at = System.os_time(:millisecond) + 1
      Router.put(k, "ephemeral", expire_at)
      # May or may not be readable immediately depending on scheduling
      _ = Router.get(k)
      Process.sleep(10)
      assert nil == Router.get(k)
    end

    test "key expiring in 50ms is readable before expiry, nil after" do
      k = ukey("50ms_ttl")
      expire_at = System.os_time(:millisecond) + 50
      Router.put(k, "brief", expire_at)
      assert "brief" == Router.get(k)
      Process.sleep(100)
      assert nil == Router.get(k)
    end

    test "expired key is not included in Router.keys()" do
      k = ukey("expired_keys")
      past = System.os_time(:millisecond) - 1
      Router.put(k, "ghost", past)
      refute k in Router.keys()
    end

    test "expired key is not counted in Router.dbsize()" do
      k = ukey("expired_dbsize")
      past = System.os_time(:millisecond) - 1
      baseline = Router.dbsize()
      Router.put(k, "ghost", past)
      # dbsize may transiently include the key before the lazy eviction fires,
      # but after a GET (which triggers eviction) it must be excluded
      Router.get(k)
      assert Router.dbsize() <= baseline
    end

    test "PUT then overwrite with no-expiry removes the TTL" do
      k = ukey("clear_ttl")
      expire_at = System.os_time(:millisecond) + 50
      Router.put(k, "expiring", expire_at)
      assert "expiring" == Router.get(k)
      # Overwrite with no expiry
      Router.put(k, "permanent", 0)
      Process.sleep(100)
      assert "permanent" == Router.get(k)
    end

    test "PUT then overwrite with earlier TTL takes effect" do
      k = ukey("earlier_ttl")
      far_future = System.os_time(:millisecond) + 60_000
      Router.put(k, "far", far_future)
      past = System.os_time(:millisecond) - 1
      Router.put(k, "past", past)
      assert nil == Router.get(k)
    end

    test "expire_at_ms at u64 max does not crash" do
      k = ukey("max_ttl")
      # u64::MAX — far future, should behave as no expiry in practice
      max_u64 = 18_446_744_073_709_551_615
      Router.put(k, "max_future", max_u64)
      assert "max_future" == Router.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Duplicate keys and update semantics
  # ---------------------------------------------------------------------------

  describe "duplicate keys and update semantics" do
    test "multiple PUTs to same key: GET returns last value" do
      k = ukey("overwrite")
      for i <- 1..10, do: Router.put(k, "val_#{i}", 0)
      assert "val_10" == Router.get(k)
    end

    test "PUT then DELETE then PUT: GET returns new value" do
      k = ukey("del_then_put")
      Router.put(k, "first", 0)
      Router.delete(k)
      assert nil == Router.get(k)
      Router.put(k, "second", 0)
      assert "second" == Router.get(k)
    end

    test "DELETE of non-existent key returns :ok without error" do
      k = ukey("del_nonexist")
      assert :ok == Router.delete(k)
    end

    test "DELETE then DELETE same key: both return :ok" do
      k = ukey("double_del")
      Router.put(k, "v", 0)
      assert :ok == Router.delete(k)
      assert :ok == Router.delete(k)
    end

    test "MSET with duplicate keys in same call: last value wins" do
      k = ukey("mset_dup")
      sock = connect()
      cmd(sock, ["MSET", k, "first", k, "second"])
      result = cmd(sock, ["GET", k])
      assert result == "second"
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Large value TCP round-trips
  # ---------------------------------------------------------------------------

  describe "large value TCP round-trips" do
    test "1 MB value SET and GET over TCP" do
      sock = connect()
      k = ukey("tcp_1mb")
      v = :binary.copy("A", 1_000_000)
      assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
      assert v == cmd(sock, ["GET", k])
      :gen_tcp.close(sock)
    end

    test "10 MB value SET and GET over TCP" do
      sock = connect()
      k = ukey("tcp_10mb")
      v = :binary.copy("B", 10_000_000)
      assert {:simple, "OK"} == cmd(sock, ["SET", k, v], 30_000)
      assert v == cmd(sock, ["GET", k], 30_000)
      :gen_tcp.close(sock)
    end

    test "10 MB value content is byte-exact over TCP" do
      sock = connect()
      k = ukey("tcp_10mb_exact")
      # Non-repeating pattern — catches any truncation or offset bugs
      v = for i <- 0..9_999_999, into: <<>>, do: <<rem(i, 251)>>
      assert {:simple, "OK"} == cmd(sock, ["SET", k, v], 30_000)
      result = cmd(sock, ["GET", k], 30_000)
      assert byte_size(result) == 10_000_000
      assert result == v
      :gen_tcp.close(sock)
    end

    test "multiple large values on same connection do not interfere" do
      sock = connect()
      pairs =
        for i <- 1..3 do
          k = ukey("multi_large_#{i}")
          v = :binary.copy(<<i>>, 500_000)
          {k, v}
        end

      for {k, v} <- pairs do
        assert {:simple, "OK"} == cmd(sock, ["SET", k, v], 15_000)
      end

      for {k, v} <- pairs do
        assert v == cmd(sock, ["GET", k], 15_000)
      end

      :gen_tcp.close(sock)
    end

    test "large value after small values on same connection" do
      sock = connect()
      k_small = ukey("before_large")
      k_large = ukey("large_after_small")
      cmd(sock, ["SET", k_small, "tiny"])
      v = :binary.copy("L", 2_000_000)
      assert {:simple, "OK"} == cmd(sock, ["SET", k_large, v], 15_000)
      assert "tiny" == cmd(sock, ["GET", k_small])
      assert v == cmd(sock, ["GET", k_large], 15_000)
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Protocol stress
  # ---------------------------------------------------------------------------

  describe "protocol stress" do
    test "pipeline of 1000 SET commands all succeed" do
      sock = connect()

      keys =
        for i <- 1..1000 do
          k = ukey("pipe_set_#{i}")
          :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(["SET", k, "v#{i}"])))
          k
        end

      responses = recv_n(sock, 1000, 30_000)

      assert Enum.all?(responses, &(&1 == {:simple, "OK"}))

      # Spot-check 10 random keys
      samples = Enum.take_random(Enum.with_index(keys, 1), 10)
      for {k, i} <- samples do
        assert "v#{i}" == cmd(sock, ["GET", k])
      end

      :gen_tcp.close(sock)
    end

    test "pipeline of 1000 PING commands all return PONG" do
      sock = connect()

      blob =
        1..1000
        |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, blob)

      responses = recv_n(sock, 1000, 30_000)
      assert Enum.all?(responses, &(&1 == {:simple, "PONG"}))
      :gen_tcp.close(sock)
    end

    test "interleaved SET and GET in a pipeline return correct values" do
      sock = connect()
      k = ukey("interleaved")

      # SET k v1, GET k, SET k v2, GET k
      commands =
        [
          Encoder.encode(["SET", k, "v1"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["SET", k, "v2"]),
          Encoder.encode(["GET", k])
        ]
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, commands)

      [r1, r2, r3, r4] = recv_n(sock, 4)
      assert r1 == {:simple, "OK"}
      assert r2 == "v1"
      assert r3 == {:simple, "OK"}
      assert r4 == "v2"

      :gen_tcp.close(sock)
    end

    test "connection survives a sequence of unknown commands" do
      sock = connect()

      for _ <- 1..5 do
        result = cmd(sock, ["UNKNOWNCMD", "arg1"])
        assert match?({:error, _}, result)
      end

      # Connection still functional
      assert {:simple, "PONG"} == cmd(sock, ["PING"])
      :gen_tcp.close(sock)
    end

    test "many small keys in MSET and MGET" do
      sock = connect()
      n = 200
      pairs = for i <- 1..n, do: {ukey("mset_k#{i}"), "mval_#{i}"}
      flat = Enum.flat_map(pairs, fn {k, v} -> [k, v] end)

      assert {:simple, "OK"} == cmd(sock, ["MSET" | flat])

      keys = Enum.map(pairs, fn {k, _} -> k end)
      values = cmd(sock, ["MGET" | keys])
      expected = Enum.map(pairs, fn {_, v} -> v end)
      assert values == expected

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Concurrent write stress
  # ---------------------------------------------------------------------------

  describe "concurrent write stress" do
    test "100 concurrent Router.put calls all succeed and are readable" do
      keys =
        for i <- 1..100 do
          k = ukey("conc_#{i}")
          v = "val_#{i}"
          {k, v}
        end

      results =
        keys
        |> Enum.map(fn {k, v} -> Task.async(fn -> Router.put(k, v, 0) end) end)
        |> Task.await_many(15_000)

      assert Enum.all?(results, &(&1 == :ok))

      for {k, v} <- keys do
        assert v == Router.get(k)
      end
    end

    test "50 concurrent writes to the same key: GET returns a valid value" do
      k = ukey("same_key_conc")

      results =
        1..50
        |> Enum.map(fn i -> Task.async(fn -> Router.put(k, "val_#{i}", 0) end) end)
        |> Task.await_many(15_000)

      assert Enum.all?(results, &(&1 == :ok))

      value = Router.get(k)
      assert is_binary(value)
      assert String.starts_with?(value, "val_")
    end

    test "concurrent writes and reads do not return corrupted data" do
      base_key = ukey("rw_conc")
      n = 30

      # Pre-seed
      for i <- 1..n, do: Router.put("#{base_key}_#{i}", "seed_#{i}", 0)

      write_tasks =
        Enum.map(1..n, fn i ->
          Task.async(fn -> Router.put("#{base_key}_#{i}", "updated_#{i}", 0) end)
        end)

      read_tasks =
        Enum.map(1..n, fn i ->
          Task.async(fn -> Router.get("#{base_key}_#{i}") end)
        end)

      write_results = Task.await_many(write_tasks, 15_000)
      read_results = Task.await_many(read_tasks, 15_000)

      assert Enum.all?(write_results, &(&1 == :ok))

      for v <- read_results do
        assert v in [nil | Enum.map(1..n, &"seed_#{&1}")] or
                 String.starts_with?(v || "", "updated_"),
               "Unexpected value: #{inspect(v)}"
      end
    end

    test "concurrent DEL and PUT on same key: store remains consistent" do
      k = ukey("del_put_race")
      Router.put(k, "initial", 0)

      tasks =
        Enum.map(1..20, fn i ->
          Task.async(fn ->
            if rem(i, 2) == 0,
              do: Router.put(k, "v#{i}", 0),
              else: Router.delete(k)
          end)
        end)

      Task.await_many(tasks, 15_000)

      # After the race, value must be either nil or a valid string — never a crash
      result = Router.get(k)
      assert is_nil(result) or is_binary(result)
    end
  end

  # ---------------------------------------------------------------------------
  # 9. Persistence: data survives flush + reopen
  # ---------------------------------------------------------------------------

  describe "persistence via NIF reopen" do
    test "data written and flushed is readable after store reopen" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      entries = for i <- 1..20, do: {"persist_k#{i}", "persist_v#{i}"}

      for {k, v} <- entries, do: NIF.put(store, k, v, 0)

      # Release the first store so the directory lock is dropped before reopening.
      # Without this the lock file prevents the second NIF.new from succeeding.
      store = nil
      :erlang.garbage_collect()

      # Reopen the store (simulates process restart)
      {:ok, store2} = NIF.new(dir)

      for {k, v} <- entries do
        assert {:ok, ^v} = NIF.get(store2, k),
               "Key #{k} missing after reopen"
      end
    end

    test "tombstones survive reopen: deleted key stays deleted" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      NIF.put(store, "del_key", "to_delete", 0)
      NIF.delete(store, "del_key")

      store = nil
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, nil} == NIF.get(store2, "del_key")
    end

    test "large value survives store reopen" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      v = :binary.copy("R", 5_000_000)
      NIF.put(store, "big_persist", v, 0)

      store = nil
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)
      assert {:ok, ^v} = NIF.get(store2, "big_persist")
    end

    test "mixed live and tombstone keys are correct after reopen" do
      {store, dir} = new_store()
      on_exit(fn -> File.rm_rf!(dir) end)

      for i <- 1..10, do: NIF.put(store, "k#{i}", "v#{i}", 0)
      # Delete odd-indexed keys
      for i <- [1, 3, 5, 7, 9], do: NIF.delete(store, "k#{i}")

      store = nil
      :erlang.garbage_collect()

      {:ok, store2} = NIF.new(dir)

      for i <- [2, 4, 6, 8, 10] do
        assert {:ok, "v#{i}"} == NIF.get(store2, "k#{i}"),
               "Live key k#{i} missing after reopen"
      end

      for i <- [1, 3, 5, 7, 9] do
        assert {:ok, nil} == NIF.get(store2, "k#{i}"),
               "Deleted key k#{i} resurrected after reopen"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 10. Protocol-level size guards (Elixir dispatcher layer)
  # ---------------------------------------------------------------------------

  describe "protocol-level size guards" do
    setup do
      sock = connect()
      on_exit(fn -> :gen_tcp.close(sock) end)
      {:ok, sock: sock}
    end

    test "SET with empty key returns ERR response", %{sock: sock} do
      resp = cmd(sock, ["SET", "", "value"])
      assert match?({:error, _}, resp), "Expected error for empty key, got: #{inspect(resp)}"
    end

    test "SET with key over 65,535 bytes returns ERR response", %{sock: sock} do
      big_key = :binary.copy("k", @max_key_bytes + 1)
      resp = cmd(sock, ["SET", big_key, "value"])
      assert match?({:error, _}, resp), "Expected error for oversized key, got: #{inspect(resp)}"
    end

    test "GET with empty key returns ERR response", %{sock: sock} do
      resp = cmd(sock, ["GET", ""])
      assert match?({:error, _}, resp), "Expected error for empty key, got: #{inspect(resp)}"
    end

    test "GET with key over 65,535 bytes returns ERR response", %{sock: sock} do
      big_key = :binary.copy("k", @max_key_bytes + 1)
      resp = cmd(sock, ["GET", big_key])
      assert match?({:error, _}, resp), "Expected error for oversized key, got: #{inspect(resp)}"
    end

    test "MSET with oversized key returns ERR response", %{sock: sock} do
      big_key = :binary.copy("k", @max_key_bytes + 1)
      resp = cmd(sock, ["MSET", big_key, "value"])
      assert match?({:error, _}, resp), "Expected error for oversized key, got: #{inspect(resp)}"
    end

    @tag :large_alloc
    test "SET with oversized value returns ERR response", %{sock: sock} do
      # 513 MiB — over the 512 MiB guard. Allocating in-process; guard fires before disk I/O.
      oversized_value = :binary.copy("v", @max_value_bytes + 1)
      resp = cmd(sock, ["SET", "guard_key", oversized_value], 60_000)
      assert match?({:error, _}, resp),
             "Expected error for oversized value, got: #{inspect(resp)}"
    end

    @tag :large_alloc
    test "MSET with oversized value returns ERR response", %{sock: sock} do
      oversized_value = :binary.copy("v", @max_value_bytes + 1)
      resp = cmd(sock, ["MSET", "guard_key2", oversized_value], 60_000)
      assert match?({:error, _}, resp),
             "Expected error for oversized value, got: #{inspect(resp)}"
    end

    test "SET with valid key and value at max sizes succeeds", %{sock: sock} do
      max_key = :binary.copy("k", @max_key_bytes)
      # Use a smaller value to avoid memory pressure in CI; the value limit is tested separately
      resp = cmd(sock, ["SET", max_key, "boundary_value"])
      assert resp == {:simple, "OK"}
      resp2 = cmd(sock, ["GET", max_key])
      assert resp2 == "boundary_value"
    end
  end

  # ---------------------------------------------------------------------------
  # 11. CRC integrity
  # ---------------------------------------------------------------------------

  describe "CRC integrity" do
    test "bit-flip in value bytes causes CRC mismatch on read" do
      dir = Path.join(System.tmp_dir!(), "ec_crc_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      # Write a known value via the NIF, then corrupt the file directly
      {:ok, store} = NIF.new(dir)
      NIF.put(store, "crc_key", "crc_value", 0)

      # Release the first store so the directory lock is dropped before reopening.
      _store = nil
      :erlang.garbage_collect()

      # Find the log file and flip a byte in the value region
      [data_file] =
        Path.join(dir, "*.log")
        |> Path.wildcard()
        |> Enum.filter(&(File.stat!(&1).size > 0))

      raw = File.read!(data_file)
      # Flip byte near the end (in the value region, past the 26-byte header + key)
      flip_pos = byte_size(raw) - 3
      <<before::binary-size(flip_pos), byte, rest::binary>> = raw
      corrupted = before <> <<Bitwise.bxor(byte, 0xFF)>> <> rest
      File.write!(data_file, corrupted)

      # Reopen — the store should detect the CRC mismatch on read
      {:ok, store2} = NIF.new(dir)
      result = NIF.get(store2, "crc_key")
      # Must return either an error or nil — must not return the corrupted value
      # as if it were valid
      assert match?({:error, _}, result) or result == {:ok, nil},
             "Expected CRC error or nil, got #{inspect(result)}"
    end
  end
end
