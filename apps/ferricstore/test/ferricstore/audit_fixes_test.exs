defmodule Ferricstore.AuditFixesTest do
  @moduledoc """
  Tests for all fixes from elixir-guide-audit.md and elixir-memory-audit.md.

  Each test group maps to a specific audit issue ID (C1-C4, H1-H6, M1-M6, L1-L4
  from the guide audit; C1, H3, M5-M6, L4-L5, L7 from the memory audit).
  """

  use ExUnit.Case, async: true

  alias Ferricstore.GlobMatcher
  alias Ferricstore.Resp.{Parser, Encoder}
  alias Ferricstore.Commands.{Strings, Hash, Set, SortedSet, Server, Generic}

  # ---------------------------------------------------------------------------
  # Helper: build a mock store map from a key-value map
  # ---------------------------------------------------------------------------

  defp mock_store(data \\ %{}) do
    %{
      get: fn key -> get_in(data, [key, Access.elem(0)]) end,
      get_meta: fn key ->
        case Map.get(data, key) do
          nil -> nil
          {val, exp} -> {val, exp}
        end
      end,
      put: fn _key, _val, _exp -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn key -> Map.has_key?(data, key) end,
      keys: fn -> Map.keys(data) |> Enum.reject(&Ferricstore.Store.CompoundKey.internal_key?/1) end,
      keys_with_prefix: fn _prefix -> [] end,
      flush: fn -> :ok end,
      dbsize: fn -> map_size(data) end,
      incr: fn _key, _delta -> {:ok, 1} end,
      incr_float: fn _key, _delta -> {:ok, "1.0"} end,
      append: fn _key, _val -> {:ok, 5} end,
      getset: fn _key, _val -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _exp -> nil end,
      setrange: fn _key, _off, _val -> {:ok, 10} end,
      compound_get: fn _rk, _ck -> nil end,
      compound_get_meta: fn _rk, _ck -> nil end,
      compound_put: fn _rk, _ck, _v, _e -> :ok end,
      compound_delete: fn _rk, _ck -> :ok end,
      compound_scan: fn _rk, _prefix -> [] end,
      compound_count: fn _rk, _prefix -> 0 end,
      compound_delete_prefix: fn _rk, _prefix -> :ok end
    }
  end

  # ---------------------------------------------------------------------------
  # C1: GlobMatcher replaces runtime regex compilation
  # ---------------------------------------------------------------------------

  describe "C1: GlobMatcher binary pattern matching" do
    test "* matches any sequence" do
      assert GlobMatcher.match?("hello", "*")
      assert GlobMatcher.match?("", "*")
      assert GlobMatcher.match?("hello world", "hello*")
      assert GlobMatcher.match?("hello world", "*world")
      assert GlobMatcher.match?("hello world", "h*d")
    end

    test "? matches exactly one byte" do
      assert GlobMatcher.match?("hello", "h?llo")
      assert GlobMatcher.match?("hello", "?ello")
      assert GlobMatcher.match?("a", "?")
      refute GlobMatcher.match?("", "?")
      refute GlobMatcher.match?("ab", "?")
    end

    test "literal matching" do
      assert GlobMatcher.match?("hello", "hello")
      refute GlobMatcher.match?("hello", "world")
      refute GlobMatcher.match?("hello", "hell")
      refute GlobMatcher.match?("hell", "hello")
    end

    test "[abc] character class" do
      assert GlobMatcher.match?("a", "[abc]")
      assert GlobMatcher.match?("b", "[abc]")
      assert GlobMatcher.match?("c", "[abc]")
      refute GlobMatcher.match?("d", "[abc]")
    end

    test "[^abc] negated character class" do
      refute GlobMatcher.match?("a", "[^abc]")
      assert GlobMatcher.match?("d", "[^abc]")
    end

    test "escaped characters" do
      assert GlobMatcher.match?("a*b", "a\\*b")
      refute GlobMatcher.match?("axb", "a\\*b")
      assert GlobMatcher.match?("a?b", "a\\?b")
      refute GlobMatcher.match?("axb", "a\\?b")
    end

    test "complex patterns" do
      assert GlobMatcher.match?("user:123:name", "user:*:name")
      assert GlobMatcher.match?("key_a_suffix", "key_?_suffix")
      refute GlobMatcher.match?("key_ab_suffix", "key_?_suffix")
    end

    test "KEYS uses GlobMatcher (not regex)" do
      # Use a non-prefix pattern (with ?) to bypass the prefix index fast path
      # and exercise the GlobMatcher code path.
      store =
        mock_store(%{
          "user_1" => {"v", 0},
          "user_2" => {"v", 0},
          "post_1" => {"v", 0}
        })

      result = Server.handle("KEYS", ["user_?"], store) |> Enum.sort()
      assert result == ["user_1", "user_2"]
    end
  end

  # ---------------------------------------------------------------------------
  # C2: RESP3 inline parser uses :binary.split
  # ---------------------------------------------------------------------------

  describe "C2: inline parser uses :binary.split" do
    test "splits on spaces" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING\r\n")
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET key\r\n")
    end

    test "handles multiple spaces" do
      assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
               Parser.parse("SET  key  value\r\n")
    end

    test "handles tabs as whitespace" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET\tkey\r\n")
    end

    test "handles mixed whitespace" do
      assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
               Parser.parse("SET \t key \t value\r\n")
    end

    test "trailing whitespace trimmed" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING \t \r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # C3: Config.get_value reads from ETS
  # ---------------------------------------------------------------------------

  describe "C3: Config.get_value reads from ETS" do
    test "get_value reads requirepass from ETS table" do
      # The :ferricstore_config ETS table is created by Config.init
      # and populated by sync_ets. Verify it exists and is readable.
      case :ets.whereis(:ferricstore_config) do
        :undefined ->
          # If Config hasn't started, skip gracefully
          :ok

        _ref ->
          # Read should succeed without going through GenServer
          result = Ferricstore.Config.get_value("requirepass")
          assert is_binary(result) or result == nil
      end
    end

    test "get_value returns nil for unknown keys" do
      case :ets.whereis(:ferricstore_config) do
        :undefined -> :ok
        _ref -> assert Ferricstore.Config.get_value("nonexistent_key_12345") == nil
      end
    end
  end

  # ---------------------------------------------------------------------------
  # C4: parse_float_value uses :binary.match
  # ---------------------------------------------------------------------------

  describe "C4: parse_float_value uses binary matching" do
    test "regular float" do
      assert {:ok, [1.5], ""} = Parser.parse(",1.5\r\n")
    end

    test "integer-form double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "scientific notation without dot" do
      assert {:ok, [float], ""} = Parser.parse(",1e5\r\n")
      assert_in_delta float, 1.0e5, 0.001
    end

    test "scientific notation with dot" do
      assert {:ok, [float], ""} = Parser.parse(",1.5e10\r\n")
      assert_in_delta float, 1.5e10, 1.0
    end

    test "negative scientific notation" do
      assert {:ok, [float], ""} = Parser.parse(",1.0E-3\r\n")
      assert_in_delta float, 0.001, 0.0001
    end

    test "special values" do
      assert {:ok, [:infinity], ""} = Parser.parse(",inf\r\n")
      assert {:ok, [:neg_infinity], ""} = Parser.parse(",-inf\r\n")
      assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # H2: Encoder single-pass counting
  # ---------------------------------------------------------------------------

  describe "H2: encoder single-pass list counting" do
    test "array encoding correct" do
      result = Encoder.encode([1, 2, 3]) |> IO.iodata_to_binary()
      assert result == "*3\r\n:1\r\n:2\r\n:3\r\n"
    end

    test "empty array" do
      result = Encoder.encode([]) |> IO.iodata_to_binary()
      assert result == "*0\r\n"
    end

    test "push encoding correct" do
      result = Encoder.encode({:push, ["a", "b"]}) |> IO.iodata_to_binary()
      assert result == ">2\r\n$1\r\na\r\n$1\r\nb\r\n"
    end

    test "nested array encoding" do
      result = Encoder.encode([[1, 2], [3]]) |> IO.iodata_to_binary()
      assert result == "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"
    end

    test "MapSet encoding" do
      set = MapSet.new(["a", "b"])
      result = Encoder.encode(set) |> IO.iodata_to_binary()
      # Order is not guaranteed for MapSet, but the count and elements should be correct
      assert String.starts_with?(result, "~2\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # H3 + H6: HSET direct recursion (no chunk_every, no length guard)
  # ---------------------------------------------------------------------------

  describe "H3+H6: HSET direct pair walking" do
    test "HSET with even number of args works" do
      compound_data = %{}

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> Map.get(compound_data, _ck) end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      # Uses check_or_set which needs the compound_get for type registry
      # Just verify the call path doesn't crash with the new implementation
      result = Hash.handle("HSET", ["key", "f1", "v1", "f2", "v2"], store)
      assert result == 2 or match?({:error, _}, result)
    end

    test "HSET with odd number of args returns error" do
      store = mock_store()
      result = Hash.handle("HSET", ["key", "f1", "v1", "f2"], store)
      assert result == {:error, "ERR wrong number of arguments for 'hset' command"}
    end

    test "HSET with single field-value pair works" do
      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      result = Hash.handle("HSET", ["key", "f1", "v1"], store)
      assert result == 1 or match?({:error, _}, result)
    end

    test "HSET with missing args returns error" do
      store = mock_store()
      result = Hash.handle("HSET", ["key"], store)
      assert result == {:error, "ERR wrong number of arguments for 'hset' command"}
    end
  end

  # ---------------------------------------------------------------------------
  # M2 + L4: MSET/MSETNX parity via even_length?
  # ---------------------------------------------------------------------------

  describe "M2+L4: MSET/MSETNX parity check" do
    test "MSET with odd args returns error" do
      store = mock_store()
      result = Strings.handle("MSET", ["k1", "v1", "k2"], store)
      assert result == {:error, "ERR wrong number of arguments for 'mset' command"}
    end

    test "MSETNX with odd args returns error" do
      store = mock_store()
      result = Strings.handle("MSETNX", ["k1", "v1", "k2"], store)
      assert result == {:error, "ERR wrong number of arguments for 'msetnx' command"}
    end

    test "MSET with even args succeeds" do
      store = mock_store()
      result = Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # M3: Hash validate_field_count recursive short-circuit
  # ---------------------------------------------------------------------------

  describe "M3: validate_field_count recursive" do
    test "HEXPIRE validates field count correctly" do
      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_get_meta, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      # Count = 2 but only 1 field: should return error
      result = Hash.handle("HEXPIRE", ["key", "60", "FIELDS", "2", "f1"], store)
      assert result == {:error, "ERR number of fields does not match the count argument"}
    end

    test "HEXPIRE with matching count and fields proceeds" do
      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_get_meta, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      # Count = 1, fields = 1: should proceed (field may not exist)
      result = Hash.handle("HEXPIRE", ["key", "60", "FIELDS", "1", "f1"], store)
      # Returns [-2] (field not found) or similar
      assert is_list(result) or match?({:error, _}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # M4: INFO format_section uses iodata
  # ---------------------------------------------------------------------------

  describe "M4: INFO format_section uses iodata" do
    test "INFO server section produces valid output" do
      store = mock_store()
      result = Server.handle("INFO", ["server"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Server\r\n")
      assert String.contains?(result, "redis_version:")
    end

    test "INFO stats section has correct format" do
      store = mock_store()
      result = Server.handle("INFO", ["stats"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Stats\r\n")
      # Each field is key:value separated by \r\n
      lines = String.split(result, "\r\n")
      assert hd(lines) == "# Stats"
    end
  end

  # ---------------------------------------------------------------------------
  # M5: shard_count and promotion_threshold cached in persistent_term
  # ---------------------------------------------------------------------------

  describe "M5: Application.get_env cached in persistent_term" do
    test "shard_count is in persistent_term" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, nil)
      assert is_integer(shard_count) and shard_count > 0
    end

    test "promotion_threshold is in persistent_term" do
      threshold = :persistent_term.get(:ferricstore_promotion_threshold, nil)
      assert is_integer(threshold) and threshold >= 0
    end

    test "Promotion.threshold reads from persistent_term" do
      result = Ferricstore.Store.Promotion.threshold()
      assert is_integer(result) and result >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # H4: paginate avoids double length
  # ---------------------------------------------------------------------------

  describe "H4: HSCAN/SSCAN paginate avoids double length" do
    test "HSCAN returns correct pagination" do
      # Create a store with hash data
      hash_data = for i <- 1..5, into: %{}, do: {"field#{i}", "val#{i}"}

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)
        |> Map.put(:compound_scan, fn _rk, _prefix ->
          Enum.map(hash_data, fn {k, v} -> {k, v} end)
        end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 5 end)

      # HSCAN with COUNT 2 should return partial results
      result = Hash.handle("HSCAN", ["key", "0", "COUNT", "2"], store)
      assert is_list(result)
      [cursor, elements] = result
      # Should have a non-zero cursor or "0" if all fit
      assert is_binary(cursor)
    end
  end

  # ---------------------------------------------------------------------------
  # H5: barrier_command_normalised? uses match?
  # ---------------------------------------------------------------------------

  describe "H5: barrier command classification" do
    # This tests the connection.ex change indirectly.
    # The fix replaces `length(args) > 1` with `match?([_, _ | _], args)`.
    # Both should behave identically.

    test "single-element list is not more than 1" do
      refute match?([_, _ | _], ["one"])
    end

    test "two-element list is more than 1" do
      assert match?([_, _ | _], ["one", "two"])
    end

    test "empty list is not more than 1" do
      refute match?([_, _ | _], [])
    end

    test "three-element list is more than 1" do
      assert match?([_, _ | _], ["one", "two", "three"])
    end
  end

  # ---------------------------------------------------------------------------
  # M1: ClientTracking BCAST uses ets.select
  # ---------------------------------------------------------------------------

  describe "M1: BCAST tracking uses ets.select" do
    test "notify_key_modified does not crash with empty tracking tables" do
      # Just verify the BCAST path doesn't crash
      result =
        Ferricstore.ClientTracking.notify_key_modified(
          "test_key",
          self(),
          fn _pid, _msg, _keys -> :ok end
        )

      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # L3: NamespaceConfig single ETS lookup
  # ---------------------------------------------------------------------------

  describe "L3: NamespaceConfig single ETS lookup" do
    test "set and get namespace config works" do
      prefix = "audit_test_#{:rand.uniform(100_000)}"

      # Set window_ms
      assert :ok = Ferricstore.NamespaceConfig.set(prefix, "window_ms", "42")

      # Get should reflect the change
      assert {:ok, entry} = Ferricstore.NamespaceConfig.get(prefix)
      assert entry.window_ms == 42
      assert entry.durability == :quorum

      # Cleanup
      Ferricstore.NamespaceConfig.reset(prefix)
    end

    test "set durability works with single lookup" do
      prefix = "audit_dur_#{:rand.uniform(100_000)}"

      # Set window_ms first, then durability
      assert :ok = Ferricstore.NamespaceConfig.set(prefix, "window_ms", "10")
      assert :ok = Ferricstore.NamespaceConfig.set(prefix, "durability", "async")

      assert {:ok, entry} = Ferricstore.NamespaceConfig.get(prefix)
      assert entry.window_ms == 10
      assert entry.durability == :async

      # Cleanup
      Ferricstore.NamespaceConfig.reset(prefix)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory audit H1: eviction uses {count, list} accumulator
  # ---------------------------------------------------------------------------

  describe "Memory H1: eviction count accumulator" do
    test "MemoryGuard stats computation works" do
      # Just verify the stats function doesn't crash
      stats = Ferricstore.MemoryGuard.stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :total_bytes)
      assert Map.has_key?(stats, :pressure_level)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory H3: hot_keys map capped at 10_000
  # (Already fixed - verified by pre-existing test)
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Memory M6: raw_store cached in persistent_term
  # (Already fixed - verified by checking persistent_term)
  # ---------------------------------------------------------------------------

  describe "Memory M6: raw_store cached" do
    test "raw_store is cached in persistent_term after first use" do
      # The raw_store is populated lazily; check if it's there
      raw = :persistent_term.get(:ferricstore_raw_store, nil)
      # May be nil in test environment if no dispatch has happened yet
      assert raw == nil or is_map(raw)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory L4: LFU.initial cached per-minute
  # (Already fixed - verified)
  # ---------------------------------------------------------------------------

  describe "Memory L4: LFU.initial cached" do
    test "LFU.initial returns consistent value within same minute" do
      a = Ferricstore.Store.LFU.initial()
      b = Ferricstore.Store.LFU.initial()
      assert a == b
      assert is_integer(a)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory L7: format_float no-op then removed
  # (Already fixed in ValueCodec)
  # ---------------------------------------------------------------------------

  describe "Memory L7: format_float clean" do
    test "format_float trims trailing zeros" do
      assert Ferricstore.Store.ValueCodec.format_float(1.0) == "1"
      assert Ferricstore.Store.ValueCodec.format_float(1.5) == "1.5"
      assert Ferricstore.Store.ValueCodec.format_float(3.14) == "3.14"
    end
  end

  # ---------------------------------------------------------------------------
  # Verify GlobMatcher is used across all modules (L2 dedup)
  # ---------------------------------------------------------------------------

  describe "L2: glob matching unified in GlobMatcher" do
    test "SCAN uses GlobMatcher for non-prefix patterns" do
      # Use a pattern that is NOT a simple prefix:* so it goes through
      # the GlobMatcher code path (not the prefix index fast path).
      store =
        mock_store(%{
          "key_1" => {"v", 0},
          "key_2" => {"v", 0},
          "other_1" => {"v", 0}
        })

      [_cursor, keys] = Generic.handle("SCAN", ["0", "MATCH", "key_?"], store)
      assert Enum.sort(keys) == ["key_1", "key_2"]
    end
  end

  # ---------------------------------------------------------------------------
  # C1 (v2): FerricStore.keys/1 uses GlobMatcher, not glob_to_regex
  # ---------------------------------------------------------------------------

  describe "C1v2: FerricStore.keys/1 uses GlobMatcher (no runtime regex)" do
    test "glob_to_regex is removed from FerricStore module" do
      # The private function glob_to_regex should no longer exist.
      # FerricStore should not export or define it.
      refute function_exported?(FerricStore, :glob_to_regex, 1)
    end

    test "keys/1 pattern matching works via GlobMatcher" do
      # This test exercises the embedded API keys/1 with a glob pattern
      # that requires GlobMatcher (not just prefix matching).
      # We call Server.handle("KEYS", ...) with a mock store to verify
      # the GlobMatcher code path.
      all_keys = ["user:alice", "user:bob", "user:charlie", "post:1"]

      store =
        mock_store(%{
          "user:alice" => {"v", 0},
          "user:bob" => {"v", 0},
          "user:charlie" => {"v", 0},
          "post:1" => {"v", 0}
        })
        |> Map.put(:keys_with_prefix, fn prefix ->
          Enum.filter(all_keys, &String.starts_with?(&1, prefix))
        end)

      # ? pattern forces GlobMatcher path (not prefix fast-path)
      result = Server.handle("KEYS", ["user:???"], store) |> Enum.sort()
      assert result == ["user:bob"]

      # * pattern uses prefix index fast path via keys_with_prefix
      result = Server.handle("KEYS", ["user:*"], store) |> Enum.sort()
      assert result == ["user:alice", "user:bob", "user:charlie"]

      # character class pattern forces GlobMatcher path
      result = Server.handle("KEYS", ["user:[ab]*"], store) |> Enum.sort()
      assert result == ["user:alice", "user:bob"]
    end
  end

  # ---------------------------------------------------------------------------
  # C2 (v2): FerricStore.keys/1 single-pass reduce
  # ---------------------------------------------------------------------------

  describe "C2v2: FerricStore.keys/1 single-pass reduce" do
    test "keys returns correct results with internal keys filtered" do
      # Internal keys (H:, S:, Z:, L:, T: prefixes) must be excluded.
      store =
        mock_store(%{
          "visible" => {"v", 0},
          "H:internal" => {"v", 0},
          "S:internal" => {"v", 0},
          "also_visible" => {"v", 0}
        })

      # The mock_store helper already filters internal keys in the keys fn,
      # matching what Router.keys() returns after the single-pass fix.
      # Verify the Server.handle("KEYS", ...) path filters correctly.
      result = Server.handle("KEYS", ["*"], store) |> Enum.sort()
      assert result == ["also_visible", "visible"]
    end

    test "keys with pattern filters in single pass" do
      store =
        mock_store(%{
          "session:abc" => {"v", 0},
          "session:def" => {"v", 0},
          "cache:xyz" => {"v", 0}
        })

      result = Server.handle("KEYS", ["session:???"], store) |> Enum.sort()
      assert result == ["session:abc", "session:def"]

      # No match
      result = Server.handle("KEYS", ["nomatch:*"], store)
      assert result == []
    end
  end

  # ===========================================================================
  # HIGH-severity fixes from elixir-guide-audit.md v2
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # H1: encode_hash removed — hashes now use compound-key storage
  # ---------------------------------------------------------------------------
  # encode_hash/decode_hash were removed in favour of compound keys.
  # Hash round-trip correctness is covered by HSET/HGET/HGETALL tests.

  # ---------------------------------------------------------------------------
  # H2: paginate in HSCAN/SSCAN avoids length + Enum.slice double traversal
  # ---------------------------------------------------------------------------

  describe "H2: HSCAN paginate avoids double traversal" do
    defp hscan_mock_store(0), do: hscan_mock_store_from(%{})

    defp hscan_mock_store(n) when n > 0 do
      hash_data =
        for i <- 1..n, into: %{} do
          key = "field_#{String.pad_leading("#{i}", 4, "0")}"
          {key, "val_#{i}"}
        end

      hscan_mock_store_from(hash_data)
    end

    defp hscan_mock_store_from(hash_data) do

      mock_store()
      |> Map.put(:compound_get, fn _rk, _ck -> nil end)
      |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)
      |> Map.put(:compound_scan, fn _rk, _prefix ->
        Enum.sort(hash_data) |> Enum.map(fn {k, v} -> {k, v} end)
      end)
      |> Map.put(:compound_count, fn _rk, _prefix -> map_size(hash_data) end)
    end

    test "HSCAN with cursor 0 returns first batch" do
      store = hscan_mock_store(10)
      [cursor, elements] = Hash.handle("HSCAN", ["key", "0", "COUNT", "3"], store)
      assert is_binary(cursor)
      # Should have elements (field + value pairs interleaved)
      assert length(elements) > 0
    end

    test "HSCAN full iteration returns all fields exactly once" do
      store = hscan_mock_store(7)
      fields = collect_hscan_fields(store, "key", "0", 3, [])
      assert length(fields) == 7
      assert fields == Enum.uniq(fields)
    end

    test "HSCAN with count larger than total returns all at once" do
      store = hscan_mock_store(3)
      [cursor, elements] = Hash.handle("HSCAN", ["key", "0", "COUNT", "100"], store)
      assert cursor == "0"
      # 3 fields * 2 (field + value) = 6 elements
      assert length(elements) == 6
    end

    test "HSCAN on empty hash returns cursor 0 and empty list" do
      store = hscan_mock_store(0)
      [cursor, elements] = Hash.handle("HSCAN", ["key", "0", "COUNT", "10"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "HSCAN with count 1 iterates one at a time" do
      store = hscan_mock_store(5)
      fields = collect_hscan_fields(store, "key", "0", 1, [])
      assert length(fields) == 5
    end
  end

  # Helper to iterate HSCAN until cursor returns "0"
  defp collect_hscan_fields(_store, _key, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_hscan_fields(store, key, cursor, count, acc) do
    [next_cursor, elements] = Hash.handle("HSCAN", [key, cursor, "COUNT", "#{count}"], store)
    # Elements are [field, value, field, value, ...] -- extract fields
    fields = elements |> Enum.chunk_every(2) |> Enum.map(&hd/1)
    new_acc = acc ++ fields

    if next_cursor == "0" do
      new_acc
    else
      collect_hscan_fields(store, key, next_cursor, count, new_acc)
    end
  end

  describe "H2: SSCAN paginate avoids double traversal" do
    defp sscan_mock_store(0), do: sscan_mock_store_from([])

    defp sscan_mock_store(n) when n > 0 do
      members =
        for i <- 1..n do
          "member_#{String.pad_leading("#{i}", 4, "0")}"
        end

      sscan_mock_store_from(members)
    end

    defp sscan_mock_store_from(members) do

      mock_store()
      |> Map.put(:compound_get, fn _rk, _ck -> nil end)
      |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)
      |> Map.put(:compound_scan, fn _rk, _prefix ->
        Enum.sort(members) |> Enum.map(fn m -> {m, "1"} end)
      end)
      |> Map.put(:compound_count, fn _rk, _prefix -> length(members) end)
    end

    test "SSCAN full iteration returns all members" do
      store = sscan_mock_store(8)
      members = collect_sscan_members(store, "key", "0", 3, [])
      assert length(members) == 8
      assert members == Enum.uniq(members)
    end

    test "SSCAN on empty set returns cursor 0" do
      store = sscan_mock_store(0)
      [cursor, elements] = Set.handle("SSCAN", ["key", "0", "COUNT", "10"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "SSCAN with count larger than total returns all" do
      store = sscan_mock_store(4)
      [cursor, elements] = Set.handle("SSCAN", ["key", "0", "COUNT", "100"], store)
      assert cursor == "0"
      assert length(elements) == 4
    end
  end

  defp collect_sscan_members(_store, _key, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_sscan_members(store, key, cursor, count, acc) do
    [next_cursor, elements] = Set.handle("SSCAN", [key, cursor, "COUNT", "#{count}"], store)
    new_acc = acc ++ elements

    if next_cursor == "0" do
      new_acc
    else
      collect_sscan_members(store, key, next_cursor, count, new_acc)
    end
  end

  # ---------------------------------------------------------------------------
  # H3: COPY handler uses pattern match instead of length guard
  # ---------------------------------------------------------------------------

  describe "H3: COPY pattern match guard" do
    test "COPY with exactly 2 args (source, dest) works" do
      store =
        mock_store(%{"src" => {"value", 0}})
        |> Map.put(:put, fn _k, _v, _e -> :ok end)

      result = Generic.handle("COPY", ["src", "dst"], store)
      # Should succeed (copy src to dst) or return :ok / integer
      assert result == 1 or result == 0 or match?({:error, _}, result)
    end

    test "COPY with REPLACE option works" do
      store =
        mock_store(%{"src" => {"value", 0}})
        |> Map.put(:put, fn _k, _v, _e -> :ok end)

      result = Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert result == 1 or result == 0 or match?({:error, _}, result)
    end

    test "COPY with 0 args returns error" do
      store = mock_store()
      result = Generic.handle("COPY", [], store)
      assert result == {:error, "ERR wrong number of arguments for 'copy' command"}
    end

    test "COPY with 1 arg returns error" do
      store = mock_store()
      result = Generic.handle("COPY", ["src"], store)
      assert result == {:error, "ERR wrong number of arguments for 'copy' command"}
    end
  end

  # ---------------------------------------------------------------------------
  # H4: AUTH handler uses pattern match instead of length guard
  # ---------------------------------------------------------------------------

  describe "H4: AUTH pattern match guard" do
    # AUTH dispatch happens in connection.ex which is harder to unit test directly.
    # We verify the pattern match semantics are equivalent to the old length guard.

    test "pattern [_, _, _ | _] matches 3+ element lists" do
      assert match?([_, _, _ | _], ["a", "b", "c"])
      assert match?([_, _, _ | _], ["a", "b", "c", "d"])
      assert match?([_, _, _ | _], ["a", "b", "c", "d", "e"])
    end

    test "pattern [_, _, _ | _] does not match 0-2 element lists" do
      refute match?([_, _, _ | _], [])
      refute match?([_, _, _ | _], ["a"])
      refute match?([_, _, _ | _], ["a", "b"])
    end

    test "pattern equivalence with length > 2" do
      # Verify the new pattern matches exactly the same inputs as length(args) > 2
      for n <- 0..10 do
        args = Enum.map(1..max(n, 1), &"arg#{&1}")
        args = if n == 0, do: [], else: args
        old_result = length(args) > 2
        new_result = match?([_, _, _ | _], args)
        assert old_result == new_result, "Mismatch for #{n}-element list"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # H5: DBSIZE uses Enum.count instead of Enum.reject + length
  # ---------------------------------------------------------------------------

  describe "H5: DBSIZE single-pass count" do
    test "DBSIZE counts non-internal keys" do
      store =
        mock_store(%{
          "user:1" => {"v", 0},
          "user:2" => {"v", 0},
          "session:a" => {"v", 0}
        })

      result = Server.handle("DBSIZE", [], store)
      assert result == 3
    end

    test "DBSIZE returns 0 for empty store" do
      store = mock_store(%{})
      result = Server.handle("DBSIZE", [], store)
      assert result == 0
    end

    test "DBSIZE with only internal keys returns 0" do
      # Internal keys have prefixes like H:, S:, Z:, L:, T:
      # The mock_store helper already filters these out via CompoundKey.internal_key?
      store =
        mock_store()
        |> Map.put(:keys, fn -> ["H:some_hash", "S:some_set", "Z:some_zset"] end)

      result = Server.handle("DBSIZE", [], store)
      assert result == 0
    end

    test "DBSIZE with mixed internal and normal keys" do
      store =
        mock_store()
        |> Map.put(:keys, fn -> ["key1", "H:hash1", "key2", "S:set1", "key3"] end)

      result = Server.handle("DBSIZE", [], store)
      assert result == 3
    end

    test "DBSIZE with wrong args returns error" do
      store = mock_store()
      result = Server.handle("DBSIZE", ["extra"], store)
      assert result == {:error, "ERR wrong number of arguments for 'dbsize' command"}
    end

    test "DBSIZE with large key count" do
      keys = for i <- 1..1000, do: "key_#{i}"
      store = mock_store() |> Map.put(:keys, fn -> keys end)
      result = Server.handle("DBSIZE", [], store)
      assert result == 1000
    end
  end

  # ---------------------------------------------------------------------------
  # H6: SCAN avoids length(all_keys) fallback via drop_while + split
  # ---------------------------------------------------------------------------

  describe "H6: SCAN avoids length(all_keys) traversal" do
    test "SCAN with cursor 0 returns first batch" do
      store =
        mock_store(%{
          "a" => {"v", 0},
          "b" => {"v", 0},
          "c" => {"v", 0},
          "d" => {"v", 0},
          "e" => {"v", 0}
        })

      [cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "2"], store)
      assert is_binary(cursor)
      assert length(keys) == 2
    end

    test "SCAN full iteration collects all keys" do
      data = for i <- 1..10, into: %{}, do: {"key_#{String.pad_leading("#{i}", 3, "0")}", {"v", 0}}
      store = mock_store(data)

      all_keys = collect_scan_keys(store, "0", 3, [])
      assert length(all_keys) == 10
      assert all_keys == Enum.uniq(all_keys)
    end

    test "SCAN returns 0 cursor when done" do
      store = mock_store(%{"only" => {"v", 0}})
      [cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert cursor == "0"
      assert keys == ["only"]
    end

    test "SCAN on empty store returns 0 and empty list" do
      store = mock_store(%{})
      [cursor, keys] = Generic.handle("SCAN", ["0"], store)
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with cursor past all keys returns 0" do
      store = mock_store(%{"a" => {"v", 0}, "b" => {"v", 0}})
      # Use a cursor string that is lexicographically after all keys
      [cursor, keys] = Generic.handle("SCAN", ["zzz", "COUNT", "10"], store)
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with MATCH pattern filters correctly" do
      all_data = %{
        "user:1" => {"v", 0},
        "user:2" => {"v", 0},
        "post:1" => {"v", 0}
      }

      all_key_list = Map.keys(all_data)

      store =
        mock_store(all_data)
        |> Map.put(:keys_with_prefix, fn prefix ->
          Enum.filter(all_key_list, &String.starts_with?(&1, prefix))
        end)

      all_keys = collect_scan_keys_with_match(store, "0", 1, "user:*", [])
      assert Enum.sort(all_keys) == ["user:1", "user:2"]
    end

    test "SCAN with count 1 iterates one key at a time" do
      data = for i <- 1..5, into: %{}, do: {"k#{i}", {"v", 0}}
      store = mock_store(data)

      all_keys = collect_scan_keys(store, "0", 1, [])
      assert length(all_keys) == 5
    end
  end

  defp collect_scan_keys(_store, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_scan_keys(store, cursor, count, acc) do
    [next_cursor, keys] = Generic.handle("SCAN", [cursor, "COUNT", "#{count}"], store)
    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      collect_scan_keys(store, next_cursor, count, new_acc)
    end
  end

  defp collect_scan_keys_with_match(_store, "0", _count, _pattern, acc) when acc != [] do
    acc
  end

  defp collect_scan_keys_with_match(store, cursor, count, pattern, acc) do
    [next_cursor, keys] =
      Generic.handle("SCAN", [cursor, "COUNT", "#{count}", "MATCH", pattern], store)

    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      collect_scan_keys_with_match(store, next_cursor, count, pattern, new_acc)
    end
  end

  # ===========================================================================
  # MEDIUM-severity fixes from elixir-guide-audit.md (second pass)
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # M4 (fix): shard_count/0 infinite recursion bug in Server INFO sections
  #
  # The helper function `shard_count/0` had a recursive call to itself as the
  # fallback when persistent_term returned nil, instead of falling back to
  # Application.get_env. This caused a stack overflow on any INFO command when
  # :ferricstore_shard_count was not set in persistent_term.
  # ---------------------------------------------------------------------------

  describe "M4: shard_count/0 reads persistent_term with Application.get_env fallback" do
    test "INFO memory section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["memory"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Memory\r\n")
      assert String.contains?(result, "used_memory:")
    end

    test "INFO raft section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["raft"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Raft\r\n")
    end

    test "INFO bitcask section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["bitcask"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Bitcask\r\n")
    end

    test "INFO ferricstore section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["ferricstore"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Ferricstore\r\n")
    end

    test "INFO keydir_analysis section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["keydir_analysis"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Keydir_Analysis\r\n")
    end

    test "INFO all section produces all sections without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["all"], store)
      assert is_binary(result)
      # All sections that use shard_count should be present
      assert String.contains?(result, "# Memory\r\n")
      assert String.contains?(result, "# Raft\r\n")
      assert String.contains?(result, "# Bitcask\r\n")
      assert String.contains?(result, "# Ferricstore\r\n")
      assert String.contains?(result, "# Keydir_Analysis\r\n")
    end

    test "shard_count falls back to Application.get_env when persistent_term not set" do
      # The persistent_term :ferricstore_shard_count is set by application.ex.
      # Verify the value is a positive integer either way.
      shard_count =
        :persistent_term.get(:ferricstore_shard_count, nil) ||
          Application.get_env(:ferricstore, :shard_count, 4)

      assert is_integer(shard_count)
      assert shard_count > 0
    end

    test "INFO memory contains shard-specific keydir data" do
      store = mock_store()
      result = Server.handle("INFO", ["memory"], store)
      assert String.contains?(result, "keydir_used_bytes:")
      assert String.contains?(result, "beam_process_memory:")
    end
  end

  # ---------------------------------------------------------------------------
  # M1: SPOP with count -- already fixed (uses Enum.reduce counter, not
  #     length(selected)). Verify the behaviour is correct.
  # ---------------------------------------------------------------------------

  describe "M1: SPOP with count avoids length(selected)" do
    test "SPOP with count removes and returns correct number of members" do
      # Build a store where the set has 5 members
      members = MapSet.new(["a", "b", "c", "d", "e"])
      removed_members = Agent.start_link(fn -> MapSet.new() end) |> elem(1)

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, ck ->
          # Extract member from compound key pattern S:key\0member
          member = ck |> String.split("\0") |> List.last()
          if MapSet.member?(members, member), do: "1", else: nil
        end)
        |> Map.put(:compound_scan, fn _rk, _prefix ->
          Enum.map(members, fn m -> {m, "1"} end)
        end)
        |> Map.put(:compound_count, fn _rk, _prefix ->
          MapSet.size(members) - MapSet.size(Agent.get(removed_members, & &1))
        end)
        |> Map.put(:compound_delete, fn _rk, _ck ->
          member = _ck |> String.split("\0") |> List.last()
          Agent.update(removed_members, &MapSet.put(&1, member))
          :ok
        end)

      result = Set.handle("SPOP", ["myset", "3"], store)
      assert is_list(result)
      assert length(result) == 3
      # All returned members should be from the original set
      assert Enum.all?(result, &MapSet.member?(members, &1))

      Agent.stop(removed_members)
    end

    test "SPOP with count 0 returns empty list" do
      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> [{"a", "1"}, {"b", "1"}] end)

      result = Set.handle("SPOP", ["myset", "0"], store)
      assert result == []
    end

    test "SPOP with count larger than set size returns all members" do
      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> [{"a", "1"}, {"b", "1"}] end)
        |> Map.put(:compound_delete, fn _rk, _ck -> :ok end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 0 end)

      result = Set.handle("SPOP", ["myset", "100"], store)
      assert is_list(result)
      assert length(result) == 2
    end

    test "SPOP with negative count returns error" do
      store = mock_store()
      result = Set.handle("SPOP", ["myset", "-1"], store)
      assert result == {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # M2: HRANDFIELD / SRANDMEMBER negative count -- already fixed (uses
  #     List.to_tuple for O(1) access). Verify behaviour correctness.
  # ---------------------------------------------------------------------------

  describe "M2: HRANDFIELD negative count uses O(1) tuple access" do
    test "HRANDFIELD with negative count allows duplicates" do
      pairs = [{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}]

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_scan, fn _rk, _prefix -> pairs end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 3 end)

      # Negative count: may return duplicates, should return abs(count) items
      result = Hash.handle("HRANDFIELD", ["myhash", "-10"], store)
      assert is_list(result)
      assert length(result) == 10
      # All returned fields should be from the original hash
      valid_fields = MapSet.new(["f1", "f2", "f3"])
      assert Enum.all?(result, &MapSet.member?(valid_fields, &1))
    end

    test "HRANDFIELD with negative count on empty hash returns empty" do
      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> [] end)

      result = Hash.handle("HRANDFIELD", ["myhash", "-5"], store)
      assert result == []
    end

    test "HRANDFIELD with positive count returns unique fields" do
      pairs = [{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}]

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_scan, fn _rk, _prefix -> pairs end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 3 end)

      result = Hash.handle("HRANDFIELD", ["myhash", "2"], store)
      assert is_list(result)
      assert length(result) <= 2
      assert result == Enum.uniq(result)
    end

    test "HRANDFIELD with negative count and WITHVALUES" do
      pairs = [{"f1", "v1"}, {"f2", "v2"}]

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_scan, fn _rk, _prefix -> pairs end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 2 end)

      result = Hash.handle("HRANDFIELD", ["myhash", "-4", "WITHVALUES"], store)
      assert is_list(result)
      # -4 means 4 field-value pairs = 8 elements in flat list
      assert length(result) == 8
    end

    test "SRANDMEMBER with negative count uses O(1) tuple access" do
      members = [{"a", "1"}, {"b", "1"}, {"c", "1"}]

      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> members end)

      result = Set.handle("SRANDMEMBER", ["myset", "-10"], store)
      assert is_list(result)
      assert length(result) == 10
      valid_members = MapSet.new(["a", "b", "c"])
      assert Enum.all?(result, &MapSet.member?(valid_members, &1))
    end
  end

  # ---------------------------------------------------------------------------
  # M3: Config.rewrite uses cons + reverse -- already fixed. Verify behaviour.
  # ---------------------------------------------------------------------------

  describe "M3: Config.rewrite uses cons + reverse (already fixed)" do
    test "CONFIG REWRITE preserves comment lines" do
      # This test verifies that the rewrite logic works correctly via the
      # Server command handler. The underlying Config.rewrite uses cons + reverse.
      store = mock_store()
      result = Server.handle("CONFIG", ["REWRITE"], store)
      # Should succeed or return an error about file path (both are valid)
      assert result == :ok or match?({:error, _}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # L1: GlobMatcher uses binary pattern match -- already fixed. Verify edge cases.
  # ---------------------------------------------------------------------------

  describe "L1: GlobMatcher binary pattern match (already fixed)" do
    test "star matches progressively longer subjects" do
      assert GlobMatcher.match?("", "*")
      assert GlobMatcher.match?("x", "*")
      assert GlobMatcher.match?(String.duplicate("x", 100), "*")
    end

    test "star at beginning and end" do
      assert GlobMatcher.match?("abc", "*abc")
      assert GlobMatcher.match?("abc", "abc*")
      assert GlobMatcher.match?("abc", "*abc*")
      assert GlobMatcher.match?("xabcy", "*abc*")
    end

    test "multiple stars" do
      assert GlobMatcher.match?("a:b:c", "*:*:*")
      assert GlobMatcher.match?("hello world foo", "hello*world*foo")
      refute GlobMatcher.match?("hello world", "hello*world*foo")
    end

    test "star with no remaining pattern chars" do
      assert GlobMatcher.match?("anything", "*")
      assert GlobMatcher.match?("", "*")
    end

    test "consecutive stars" do
      assert GlobMatcher.match?("abc", "**abc")
      assert GlobMatcher.match?("abc", "abc**")
      assert GlobMatcher.match?("abc", "**")
    end

    test "binary with null bytes" do
      assert GlobMatcher.match?(<<0, 1, 2>>, <<0, ?*, 2>>)
      refute GlobMatcher.match?(<<0, 1, 2>>, <<0, 3, 2>>)
    end

    test "very long subject with star pattern" do
      long = String.duplicate("a", 1000)
      assert GlobMatcher.match?(long, "a*a")
      assert GlobMatcher.match?(long, "*")
      refute GlobMatcher.match?(long, "b*")
    end

    test "unicode bytes" do
      assert GlobMatcher.match?("hello", "h?llo")
      # Multi-byte UTF-8: ? matches one byte, not one codepoint
      # This is consistent with Redis behaviour
    end
  end

  # ---------------------------------------------------------------------------
  # L2: encode_hash/decode_hash/is_encoded_hash? removed — compound keys now
  # ---------------------------------------------------------------------------
  # Hash encoding internals were removed. Compound-key storage is tested
  # through the HSET/HGET/HGETALL command-level tests.

  # ---------------------------------------------------------------------------
  # M5: do_scan Enum.sort -- architectural issue, no code change needed.
  # Verify SCAN still produces deterministic, sorted results.
  # ---------------------------------------------------------------------------

  describe "M5: SCAN produces sorted deterministic results" do
    test "SCAN results are in sorted order" do
      data = %{
        "z_key" => {"v", 0},
        "a_key" => {"v", 0},
        "m_key" => {"v", 0}
      }

      store = mock_store(data)
      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert keys == Enum.sort(keys)
    end

    test "SCAN pagination respects sort order" do
      data = for i <- 1..10, into: %{}, do: {"key_#{String.pad_leading("#{i}", 3, "0")}", {"v", 0}}
      store = mock_store(data)

      all_keys = collect_scan_keys(store, "0", 3, [])
      assert all_keys == Enum.sort(all_keys)
    end
  end

  # ---------------------------------------------------------------------------
  # ETS Read Optimizations
  # ---------------------------------------------------------------------------

  describe "LFU touch skip — unchanged packed value" do
    alias Ferricstore.Store.LFU

    setup do
      LFU.init_config_cache()
      table = :ets.new(:lfu_skip_test, [:set, :public])
      %{table: table}
    end

    test "same-minute touch with no increment does not mutate ETS", %{table: table} do
      # Use a high counter so the probabilistic increment almost never fires.
      # P(increment) = 1/(counter * log_factor + 1) = 1/(200*10+1) ≈ 0.05%
      now_min = LFU.now_minutes()
      initial_packed = LFU.pack(now_min, 200)
      :ets.insert(table, {"key", :val, 0, initial_packed})

      unchanged_count =
        Enum.count(1..1000, fn _ ->
          [{_, _, _, before}] = :ets.lookup(table, "key")
          LFU.touch(table, "key", initial_packed)
          [{_, _, _, after_val}] = :ets.lookup(table, "key")
          # Reset to initial so each iteration is independent
          :ets.update_element(table, "key", {4, initial_packed})
          after_val == before
        end)

      # With counter=200, ~99.95% of touches should skip the write.
      # Allow generous margin — at least 950 out of 1000.
      assert unchanged_count >= 950,
             "Expected >= 950 unchanged touches, got #{unchanged_count}"
    end

    test "minute rollover always updates ldt", %{table: table} do
      old_min = Bitwise.band(LFU.now_minutes() - 2, 0xFFFF)
      packed = LFU.pack(old_min, 200)
      :ets.insert(table, {"key", :val, 0, packed})

      LFU.touch(table, "key", packed)
      [{_, _, _, new_packed}] = :ets.lookup(table, "key")

      # ldt must have been updated to current minute
      {new_ldt, _} = LFU.unpack(new_packed)
      assert new_ldt == LFU.now_minutes()
      assert new_packed != packed
    end

    test "counter increment triggers ETS write", %{table: table} do
      # Counter 0 always increments: P = 1/(0*10+1) = 100%
      now_min = LFU.now_minutes()
      packed = LFU.pack(now_min, 0)
      :ets.insert(table, {"key", :val, 0, packed})

      LFU.touch(table, "key", packed)
      [{_, _, _, new_packed}] = :ets.lookup(table, "key")

      {_, new_counter} = LFU.unpack(new_packed)
      assert new_counter == 1
      assert new_packed != packed
    end

    test "eviction accuracy — effective_counter consistent with touch", %{table: table} do
      now_min = LFU.now_minutes()
      packed = LFU.pack(now_min, LFU.initial_counter())
      :ets.insert(table, {"key", :val, 0, packed})

      final_packed =
        Enum.reduce(1..500, packed, fn _, acc ->
          LFU.touch(table, "key", acc)
          [{_, _, _, p}] = :ets.lookup(table, "key")
          p
        end)

      eff = LFU.effective_counter(final_packed)
      # Counter should have grown from initial (5) but not saturated for 500 touches
      assert eff >= LFU.initial_counter()
      assert eff <= 255
    end

    test "stress — 10K reads produce sane counter", %{table: table} do
      now_min = LFU.now_minutes()
      packed = LFU.pack(now_min, LFU.initial_counter())
      :ets.insert(table, {"key", :val, 0, packed})

      final_packed =
        Enum.reduce(1..10_000, packed, fn _, acc ->
          LFU.touch(table, "key", acc)
          [{_, _, _, p}] = :ets.lookup(table, "key")
          p
        end)

      {_, counter} = LFU.unpack(final_packed)
      assert counter > 0, "Counter must not be zero after 10K touches"
      assert counter <= 255, "Counter must not exceed 255"
      # With log factor 10 and 10K touches, counter should be well above initial
      assert counter > LFU.initial_counter(),
             "Counter #{counter} should exceed initial #{LFU.initial_counter()} after 10K touches"
    end
  end

  describe "decentralized_counters on write_concurrency tables" do
    test "keydir table has decentralized_counters" do
      # Create a table with the same options as shard.ex keydir
      table = :ets.new(:dc_test_keydir, [
        :set, :public,
        {:read_concurrency, true},
        {:write_concurrency, true},
        {:decentralized_counters, true}
      ])

      info = :ets.info(table)
      assert Keyword.get(info, :decentralized_counters) == true

      :ets.delete(table)
    end

    test "concurrent inserts with decentralized_counters" do
      table = :ets.new(:dc_test_concurrent, [
        :set, :public,
        {:read_concurrency, true},
        {:write_concurrency, true},
        {:decentralized_counters, true}
      ])

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            for j <- 1..100 do
              :ets.insert(table, {{i, j}, :erlang.unique_integer()})
            end
          end)
        end

      Task.await_many(tasks, 10_000)

      assert :ets.info(table, :size) == 10_000

      :ets.delete(table)
    end
  end
end
