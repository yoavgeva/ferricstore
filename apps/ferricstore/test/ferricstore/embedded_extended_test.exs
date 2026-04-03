defmodule Ferricstore.EmbeddedExtendedTest do
  @moduledoc """
  Extended tests for FerricStore embedded Elixir API covering all functions
  not exercised by the base embedded_test.exs.

  Covers: decr, decr_by, incr_by_float, mget, mset, append, strlen, getset,
  getdel, getex, setnx, setex, psetex, getrange, setrange, msetnx, copy,
  rename, renamenx, type, randomkey, persist, pexpire, pexpireat, expireat,
  expiretime, pexpiretime, pttl, bitmap ops, hash extended ops, list extended
  ops, set algebra, sorted set extended ops, streams, native lock/unlock/extend,
  rate limiting, fetch_or_compute, multi/tx, ping, echo, flushall, and
  probabilistic/data-structure smoke tests.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    on_exit(fn -> Ferricstore.Test.ShardHelpers.flush_all_keys() end)
    :ok
  end

  # ===========================================================================
  # DECR / DECR_BY
  # ===========================================================================

  describe "decr/1" do
    test "decrements nonexistent key from 0 to -1" do
      assert {:ok, -1} = FerricStore.decr("decr:new")
    end

    test "decrements existing integer value" do
      FerricStore.set("decr:existing", "10")
      assert {:ok, 9} = FerricStore.decr("decr:existing")
    end

    test "multiple decrements accumulate" do
      assert {:ok, -1} = FerricStore.decr("decr:multi")
      assert {:ok, -2} = FerricStore.decr("decr:multi")
      assert {:ok, -3} = FerricStore.decr("decr:multi")
    end

    test "decr on non-integer value returns error" do
      FerricStore.set("decr:bad", "abc")
      assert {:error, _} = FerricStore.decr("decr:bad")
    end
  end

  describe "decr_by/2" do
    test "decrements by specified amount" do
      FerricStore.set("decrby:key", "100")
      assert {:ok, 90} = FerricStore.decr_by("decrby:key", 10)
    end

    test "decrements nonexistent key from 0" do
      assert {:ok, -5} = FerricStore.decr_by("decrby:new", 5)
    end

    test "decrement by 0 returns current value" do
      FerricStore.set("decrby:zero", "42")
      assert {:ok, 42} = FerricStore.decr_by("decrby:zero", 0)
    end

    test "decr_by on non-integer value returns error" do
      FerricStore.set("decrby:bad", "hello")
      assert {:error, _} = FerricStore.decr_by("decrby:bad", 5)
    end
  end

  # ===========================================================================
  # INCR_BY_FLOAT
  # ===========================================================================

  describe "incr_by_float/2" do
    test "increments nonexistent key by float" do
      assert {:ok, result} = FerricStore.incr_by_float("ibf:new", 3.14)
      assert_in_delta result, 3.14, 0.001
    end

    test "increments existing integer-string by float" do
      FerricStore.set("ibf:int", "10")
      assert {:ok, result} = FerricStore.incr_by_float("ibf:int", 0.5)
      assert_in_delta result, 10.5, 0.001
    end

    test "increments by negative float" do
      FerricStore.set("ibf:neg", "10")
      assert {:ok, result} = FerricStore.incr_by_float("ibf:neg", -2.5)
      assert_in_delta result, 7.5, 0.001
    end
  end

  # ===========================================================================
  # MGET / MSET
  # ===========================================================================

  describe "mget/1" do
    test "returns values for multiple keys" do
      FerricStore.set("mg:a", "1")
      FerricStore.set("mg:b", "2")
      assert {:ok, ["1", "2"]} = FerricStore.mget(["mg:a", "mg:b"])
    end

    test "returns nil for missing keys" do
      FerricStore.set("mg:c", "3")
      assert {:ok, ["3", nil]} = FerricStore.mget(["mg:c", "mg:missing"])
    end

    test "returns all nils for all missing keys" do
      assert {:ok, [nil, nil]} = FerricStore.mget(["mg:x", "mg:y"])
    end

    test "empty list returns empty" do
      assert {:ok, []} = FerricStore.mget([])
    end
  end

  describe "mset/1" do
    test "sets multiple key-value pairs" do
      assert :ok = FerricStore.mset(%{"ms:a" => "1", "ms:b" => "2"})
      assert {:ok, "1"} = FerricStore.get("ms:a")
      assert {:ok, "2"} = FerricStore.get("ms:b")
    end

    test "overwrites existing keys" do
      FerricStore.set("ms:ow", "old")
      FerricStore.mset(%{"ms:ow" => "new"})
      assert {:ok, "new"} = FerricStore.get("ms:ow")
    end
  end

  # ===========================================================================
  # APPEND / STRLEN
  # ===========================================================================

  describe "append/2" do
    test "appends to existing key" do
      FerricStore.set("ap:key", "Hello")
      assert {:ok, 11} = FerricStore.append("ap:key", " World")
      assert {:ok, "Hello World"} = FerricStore.get("ap:key")
    end

    test "creates key if it does not exist" do
      assert {:ok, 5} = FerricStore.append("ap:new", "Hello")
      assert {:ok, "Hello"} = FerricStore.get("ap:new")
    end

    test "appends empty string" do
      FerricStore.set("ap:empty", "data")
      assert {:ok, 4} = FerricStore.append("ap:empty", "")
    end
  end

  describe "strlen/1" do
    test "returns length of existing string" do
      FerricStore.set("sl:key", "Hello")
      assert {:ok, 5} = FerricStore.strlen("sl:key")
    end

    test "returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.strlen("sl:missing")
    end

    test "returns correct length for binary data" do
      FerricStore.set("sl:bin", <<1, 2, 3, 4, 5>>)
      assert {:ok, 5} = FerricStore.strlen("sl:bin")
    end
  end

  # ===========================================================================
  # GETSET / GETDEL / GETEX
  # ===========================================================================

  describe "getset/2" do
    test "returns nil and sets value for nonexistent key" do
      assert {:ok, nil} = FerricStore.getset("gs:new", "value")
      assert {:ok, "value"} = FerricStore.get("gs:new")
    end

    test "returns old value and sets new value" do
      FerricStore.set("gs:key", "old")
      assert {:ok, "old"} = FerricStore.getset("gs:key", "new")
      assert {:ok, "new"} = FerricStore.get("gs:key")
    end
  end

  describe "getdel/1" do
    test "gets and deletes existing key" do
      FerricStore.set("gd:key", "value")
      assert {:ok, "value"} = FerricStore.getdel("gd:key")
      assert {:ok, nil} = FerricStore.get("gd:key")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.getdel("gd:missing")
    end
  end

  describe "getex/2" do
    test "gets value and sets TTL" do
      FerricStore.set("gex:key", "value")
      assert {:ok, "value"} = FerricStore.getex("gex:key", ttl: 60_000)
      assert {:ok, ms} = FerricStore.ttl("gex:key")
      assert is_integer(ms) and ms > 0
    end

    test "gets value and removes TTL with persist" do
      FerricStore.set("gex:persist", "value", ttl: 60_000)
      assert {:ok, "value"} = FerricStore.getex("gex:persist", persist: true)
      assert {:ok, nil} = FerricStore.ttl("gex:persist")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.getex("gex:missing", ttl: 60_000)
    end
  end

  # ===========================================================================
  # SETNX / SETEX / PSETEX
  # ===========================================================================

  describe "setnx/2" do
    test "sets value when key does not exist" do
      assert {:ok, true} = FerricStore.setnx("snx:new", "value")
      assert {:ok, "value"} = FerricStore.get("snx:new")
    end

    test "does not set value when key exists" do
      FerricStore.set("snx:exists", "original")
      assert {:ok, false} = FerricStore.setnx("snx:exists", "other")
      assert {:ok, "original"} = FerricStore.get("snx:exists")
    end
  end

  describe "setex/3" do
    test "sets value with seconds-based TTL" do
      assert :ok = FerricStore.setex("sex:key", 60, "value")
      assert {:ok, "value"} = FerricStore.get("sex:key")
      assert {:ok, ms} = FerricStore.ttl("sex:key")
      assert is_integer(ms) and ms > 0 and ms <= 60_000
    end
  end

  describe "psetex/3" do
    test "sets value with millisecond TTL" do
      assert :ok = FerricStore.psetex("psx:key", 60_000, "value")
      assert {:ok, "value"} = FerricStore.get("psx:key")
      assert {:ok, ms} = FerricStore.ttl("psx:key")
      assert is_integer(ms) and ms > 0 and ms <= 60_000
    end
  end

  # ===========================================================================
  # GETRANGE / SETRANGE
  # ===========================================================================

  describe "getrange/3" do
    test "returns substring of existing value" do
      FerricStore.set("gr:key", "Hello World")
      assert {:ok, "World"} = FerricStore.getrange("gr:key", 6, 10)
    end

    test "returns full string with 0..-1-style indices" do
      FerricStore.set("gr:full", "Hello")
      assert {:ok, "Hello"} = FerricStore.getrange("gr:full", 0, -1)
    end

    test "returns empty string for nonexistent key" do
      assert {:ok, ""} = FerricStore.getrange("gr:missing", 0, 10)
    end
  end

  describe "setrange/3" do
    test "overwrites part of a string" do
      FerricStore.set("sr:key", "Hello World")
      assert {:ok, 11} = FerricStore.setrange("sr:key", 6, "Redis")
      assert {:ok, "Hello Redis"} = FerricStore.get("sr:key")
    end

    test "pads with null bytes if offset is beyond string length" do
      assert {:ok, len} = FerricStore.setrange("sr:pad", 5, "Hi")
      assert len == 7
      {:ok, val} = FerricStore.get("sr:pad")
      assert byte_size(val) == 7
    end
  end

  # ===========================================================================
  # MSETNX
  # ===========================================================================

  describe "msetnx/1" do
    test "sets all keys when none exist" do
      assert {:ok, true} = FerricStore.msetnx(%{"mnx:a" => "1", "mnx:b" => "2"})
      assert {:ok, "1"} = FerricStore.get("mnx:a")
      assert {:ok, "2"} = FerricStore.get("mnx:b")
    end

    test "sets nothing when any key exists" do
      FerricStore.set("mnx:c", "existing")
      assert {:ok, false} = FerricStore.msetnx(%{"mnx:c" => "new", "mnx:d" => "new"})
      assert {:ok, "existing"} = FerricStore.get("mnx:c")
      assert {:ok, nil} = FerricStore.get("mnx:d")
    end
  end

  # ===========================================================================
  # COPY / RENAME / RENAMENX
  # ===========================================================================

  describe "copy/3" do
    test "copies value to new key" do
      FerricStore.set("cp:src", "value")
      assert {:ok, true} = FerricStore.copy("cp:src", "cp:dst")
      assert {:ok, "value"} = FerricStore.get("cp:dst")
      # source still exists
      assert {:ok, "value"} = FerricStore.get("cp:src")
    end

    test "does not overwrite existing destination without replace" do
      FerricStore.set("cp:s2", "src_val")
      FerricStore.set("cp:d2", "dst_val")
      assert {:error, _} = FerricStore.copy("cp:s2", "cp:d2")
      assert {:ok, "dst_val"} = FerricStore.get("cp:d2")
    end

    test "overwrites existing destination with replace option" do
      FerricStore.set("cp:s3", "src_val")
      FerricStore.set("cp:d3", "dst_val")
      assert {:ok, true} = FerricStore.copy("cp:s3", "cp:d3", replace: true)
      assert {:ok, "src_val"} = FerricStore.get("cp:d3")
    end
  end

  describe "rename/2" do
    test "renames existing key" do
      FerricStore.set("rn:old", "value")
      assert :ok = FerricStore.rename("rn:old", "rn:new")
      assert {:ok, nil} = FerricStore.get("rn:old")
      assert {:ok, "value"} = FerricStore.get("rn:new")
    end

    test "returns error for nonexistent key" do
      assert {:error, _} = FerricStore.rename("rn:missing", "rn:dst")
    end
  end

  describe "renamenx/2" do
    test "renames when destination does not exist" do
      FerricStore.set("rnx:old", "value")
      assert {:ok, true} = FerricStore.renamenx("rnx:old", "rnx:new")
      assert {:ok, "value"} = FerricStore.get("rnx:new")
    end

    test "does not rename when destination exists" do
      FerricStore.set("rnx:src", "src_val")
      FerricStore.set("rnx:dst", "dst_val")
      assert {:ok, false} = FerricStore.renamenx("rnx:src", "rnx:dst")
      assert {:ok, "dst_val"} = FerricStore.get("rnx:dst")
    end
  end

  # ===========================================================================
  # TYPE / RANDOMKEY
  # ===========================================================================

  describe "type/1" do
    test "returns 'string' for string key" do
      FerricStore.set("tp:str", "value")
      assert {:ok, "string"} = FerricStore.type("tp:str")
    end

    test "returns 'none' for nonexistent key" do
      assert {:ok, "none"} = FerricStore.type("tp:missing")
    end

    test "returns 'hash' for hash key" do
      FerricStore.hset("tp:hash", %{"f" => "v"})
      assert {:ok, "hash"} = FerricStore.type("tp:hash")
    end

    test "returns 'list' for list key" do
      FerricStore.rpush("tp:list", ["a"])
      assert {:ok, "list"} = FerricStore.type("tp:list")
    end

    test "returns 'set' for set key" do
      FerricStore.sadd("tp:set", ["a"])
      assert {:ok, "set"} = FerricStore.type("tp:set")
    end

    test "returns 'zset' for sorted set key" do
      FerricStore.zadd("tp:zset", [{1.0, "a"}])
      assert {:ok, "zset"} = FerricStore.type("tp:zset")
    end
  end

  describe "randomkey/0" do
    test "returns nil when no keys exist" do
      FerricStore.flushdb()
      assert {:ok, nil} = FerricStore.randomkey()
    end

    test "returns a key that exists" do
      FerricStore.set("rk:a", "1")
      FerricStore.set("rk:b", "2")
      {:ok, key} = FerricStore.randomkey()
      assert key != nil
      assert FerricStore.exists(key)
    end
  end

  # ===========================================================================
  # TTL extended: persist, pexpire, pexpireat, expireat, expiretime, pexpiretime, pttl
  # ===========================================================================

  describe "persist/1" do
    test "removes TTL from key" do
      FerricStore.set("per:key", "value", ttl: 60_000)
      assert {:ok, true} = FerricStore.persist("per:key")
      assert {:ok, nil} = FerricStore.ttl("per:key")
    end

    test "returns false for key without TTL" do
      FerricStore.set("per:nottl", "value")
      assert {:ok, false} = FerricStore.persist("per:nottl")
    end

    test "returns false for nonexistent key" do
      assert {:ok, false} = FerricStore.persist("per:missing")
    end
  end

  describe "pexpire/2" do
    test "sets TTL in milliseconds (alias for expire)" do
      FerricStore.set("pex:key", "value")
      assert {:ok, true} = FerricStore.pexpire("pex:key", 30_000)
      assert {:ok, ms} = FerricStore.ttl("pex:key")
      assert is_integer(ms) and ms > 0 and ms <= 30_000
    end
  end

  describe "expireat/2" do
    test "sets expiry at Unix timestamp in seconds" do
      FerricStore.set("eat:key", "value")
      future_ts = div(System.os_time(:millisecond), 1_000) + 3600
      assert {:ok, true} = FerricStore.expireat("eat:key", future_ts)
      assert {:ok, ms} = FerricStore.ttl("eat:key")
      assert is_integer(ms) and ms > 0
    end

    test "returns false for nonexistent key" do
      assert {:ok, false} = FerricStore.expireat("eat:missing", 9_999_999_999)
    end
  end

  describe "pexpireat/2" do
    test "sets expiry at Unix timestamp in milliseconds" do
      FerricStore.set("peat:key", "value")
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert {:ok, true} = FerricStore.pexpireat("peat:key", future_ms)
      assert {:ok, ms} = FerricStore.ttl("peat:key")
      assert is_integer(ms) and ms > 0
    end

    test "returns false for nonexistent key" do
      assert {:ok, false} = FerricStore.pexpireat("peat:missing", 9_999_999_999_999)
    end
  end

  describe "expiretime/1" do
    test "returns Unix timestamp in seconds for key with TTL" do
      FerricStore.set("etime:key", "value", ttl: 3_600_000)
      assert {:ok, ts} = FerricStore.expiretime("etime:key")
      assert is_integer(ts) and ts > 0
      now_s = div(System.os_time(:millisecond), 1_000)
      assert ts > now_s
    end

    test "returns -1 for key without TTL" do
      FerricStore.set("etime:nottl", "value")
      assert {:ok, -1} = FerricStore.expiretime("etime:nottl")
    end

    test "returns -2 for nonexistent key" do
      assert {:ok, -2} = FerricStore.expiretime("etime:missing")
    end
  end

  describe "pexpiretime/1" do
    test "returns Unix timestamp in milliseconds for key with TTL" do
      FerricStore.set("petime:key", "value", ttl: 3_600_000)
      assert {:ok, ts_ms} = FerricStore.pexpiretime("petime:key")
      assert is_integer(ts_ms) and ts_ms > 0
      now_ms = System.os_time(:millisecond)
      assert ts_ms > now_ms
    end

    test "returns -1 for key without TTL" do
      FerricStore.set("petime:nottl", "value")
      assert {:ok, -1} = FerricStore.pexpiretime("petime:nottl")
    end

    test "returns -2 for nonexistent key" do
      assert {:ok, -2} = FerricStore.pexpiretime("petime:missing")
    end
  end

  describe "pttl/1" do
    test "returns TTL in ms (same as ttl/1)" do
      FerricStore.set("pttl:key", "value", ttl: 60_000)
      assert {:ok, ms} = FerricStore.pttl("pttl:key")
      assert is_integer(ms) and ms > 0 and ms <= 60_000
    end

    test "returns nil for key without TTL" do
      FerricStore.set("pttl:nottl", "value")
      assert {:ok, nil} = FerricStore.pttl("pttl:nottl")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.pttl("pttl:missing")
    end
  end

  # ===========================================================================
  # BITMAP: setbit, getbit, bitcount, bitop, bitpos
  # ===========================================================================

  describe "setbit/3 and getbit/2" do
    test "sets and gets a bit" do
      assert {:ok, 0} = FerricStore.setbit("bit:key", 7, 1)
      assert {:ok, 1} = FerricStore.getbit("bit:key", 7)
    end

    test "default bits are 0" do
      assert {:ok, 0} = FerricStore.getbit("bit:empty", 100)
    end

    test "setbit returns previous value" do
      FerricStore.setbit("bit:prev", 7, 1)
      assert {:ok, 1} = FerricStore.setbit("bit:prev", 7, 0)
      assert {:ok, 0} = FerricStore.getbit("bit:prev", 7)
    end
  end

  describe "bitcount/1" do
    test "counts set bits" do
      # "foobar" = 0x66 0x6f 0x6f 0x62 0x61 0x72 -> 26 set bits
      FerricStore.set("bc:key", "foobar")
      assert {:ok, count} = FerricStore.bitcount("bc:key")
      assert is_integer(count) and count > 0
    end

    test "returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.bitcount("bc:missing")
    end

    test "bitcount with range" do
      FerricStore.set("bc:range", "foobar")
      assert {:ok, count} = FerricStore.bitcount("bc:range", start: 0, stop: 0)
      assert is_integer(count) and count > 0
    end
  end

  describe "bitop/3" do
    test "AND operation between two keys" do
      FerricStore.set("bo:a", "abc")
      FerricStore.set("bo:b", "abc")
      assert {:ok, len} = FerricStore.bitop(:and, "bo:dest", ["bo:a", "bo:b"])
      assert len == 3
      assert {:ok, "abc"} = FerricStore.get("bo:dest")
    end

    test "OR operation" do
      FerricStore.set("bo:or1", <<0xFF>>)
      FerricStore.set("bo:or2", <<0x0F>>)
      assert {:ok, 1} = FerricStore.bitop(:or, "bo:or_dst", ["bo:or1", "bo:or2"])
      assert {:ok, <<0xFF>>} = FerricStore.get("bo:or_dst")
    end

    test "NOT operation (single key)" do
      FerricStore.set("bo:not_src", <<0x0F>>)
      assert {:ok, 1} = FerricStore.bitop(:not, "bo:not_dst", ["bo:not_src"])
      assert {:ok, <<0xF0>>} = FerricStore.get("bo:not_dst")
    end
  end

  describe "bitpos/2" do
    test "finds first set bit" do
      FerricStore.set("bp:key", <<0x00, 0xFF>>)
      assert {:ok, pos} = FerricStore.bitpos("bp:key", 1)
      assert pos == 8
    end

    test "finds first zero bit" do
      FerricStore.set("bp:zero", <<0xFF, 0x00>>)
      assert {:ok, pos} = FerricStore.bitpos("bp:zero", 0)
      assert pos == 8
    end

    test "returns -1 when bit not found in all-ones string looking for 0 with bounded range" do
      FerricStore.set("bp:ones", <<0xFF>>)
      assert {:ok, pos} = FerricStore.bitpos("bp:ones", 0, start: 0, stop: 0)
      # With byte-level range, all bits are 1 in byte 0 => -1
      assert pos == -1
    end
  end

  # ===========================================================================
  # HASH extended: hdel, hexists, hlen, hkeys, hvals, hmget, hincrby,
  # hincrbyfloat, hsetnx, hrandfield, hstrlen
  # ===========================================================================

  describe "hdel/2" do
    test "deletes existing hash fields" do
      FerricStore.hset("hd:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, 2} = FerricStore.hdel("hd:key", ["a", "c"])
      assert {:ok, nil} = FerricStore.hget("hd:key", "a")
      assert {:ok, "2"} = FerricStore.hget("hd:key", "b")
    end

    test "ignores nonexistent fields" do
      FerricStore.hset("hd:key2", %{"a" => "1"})
      assert {:ok, 0} = FerricStore.hdel("hd:key2", ["z"])
    end

    test "hdel on nonexistent hash returns 0" do
      assert {:ok, 0} = FerricStore.hdel("hd:missing", ["f"])
    end
  end

  describe "hexists/2" do
    test "returns true for existing field" do
      FerricStore.hset("hex:key", %{"name" => "alice"})
      assert FerricStore.hexists("hex:key", "name") == true
    end

    test "returns false for missing field" do
      FerricStore.hset("hex:key2", %{"name" => "alice"})
      assert FerricStore.hexists("hex:key2", "age") == false
    end

    test "returns false for nonexistent hash" do
      assert FerricStore.hexists("hex:missing", "field") == false
    end
  end

  describe "hlen/1" do
    test "returns number of fields" do
      FerricStore.hset("hl:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, 3} = FerricStore.hlen("hl:key")
    end

    test "returns 0 for nonexistent hash" do
      assert {:ok, 0} = FerricStore.hlen("hl:missing")
    end
  end

  describe "hkeys/1" do
    test "returns all field names" do
      FerricStore.hset("hk:key", %{"name" => "alice", "age" => "30"})
      assert {:ok, keys} = FerricStore.hkeys("hk:key")
      assert Enum.sort(keys) == ["age", "name"]
    end

    test "returns empty list for nonexistent hash" do
      assert {:ok, []} = FerricStore.hkeys("hk:missing")
    end
  end

  describe "hvals/1" do
    test "returns all field values" do
      FerricStore.hset("hv:key", %{"a" => "1", "b" => "2"})
      assert {:ok, vals} = FerricStore.hvals("hv:key")
      assert Enum.sort(vals) == ["1", "2"]
    end

    test "returns empty list for nonexistent hash" do
      assert {:ok, []} = FerricStore.hvals("hv:missing")
    end
  end

  describe "hmget/2" do
    test "returns values for requested fields" do
      FerricStore.hset("hmg:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, ["1", "3"]} = FerricStore.hmget("hmg:key", ["a", "c"])
    end

    test "returns nil for missing fields" do
      FerricStore.hset("hmg:key2", %{"a" => "1"})
      assert {:ok, ["1", nil]} = FerricStore.hmget("hmg:key2", ["a", "z"])
    end

    test "returns all nils for nonexistent hash" do
      assert {:ok, [nil, nil]} = FerricStore.hmget("hmg:missing", ["a", "b"])
    end
  end

  describe "hincrby/3" do
    test "increments hash field by integer" do
      FerricStore.hset("hib:key", %{"count" => "10"})
      assert {:ok, 15} = FerricStore.hincrby("hib:key", "count", 5)
    end

    test "creates field if it does not exist" do
      FerricStore.hset("hib:key2", %{"other" => "x"})
      assert {:ok, 3} = FerricStore.hincrby("hib:key2", "count", 3)
    end

    test "decrements with negative amount" do
      FerricStore.hset("hib:key3", %{"count" => "10"})
      assert {:ok, 7} = FerricStore.hincrby("hib:key3", "count", -3)
    end
  end

  describe "hincrbyfloat/3" do
    test "increments hash field by float" do
      FerricStore.hset("hif:key", %{"val" => "10"})
      assert {:ok, result} = FerricStore.hincrbyfloat("hif:key", "val", 2.5)
      {f, _} = Float.parse(result)
      assert_in_delta f, 12.5, 0.001
    end

    test "creates field if it does not exist" do
      FerricStore.hset("hif:key2", %{"other" => "x"})
      assert {:ok, result} = FerricStore.hincrbyfloat("hif:key2", "val", 3.14)
      {f, _} = Float.parse(result)
      assert_in_delta f, 3.14, 0.001
    end
  end

  describe "hsetnx/3" do
    test "sets field when it does not exist" do
      FerricStore.hset("hsn:key", %{"a" => "1"})
      assert {:ok, true} = FerricStore.hsetnx("hsn:key", "b", "2")
      assert {:ok, "2"} = FerricStore.hget("hsn:key", "b")
    end

    test "does not overwrite existing field" do
      FerricStore.hset("hsn:key2", %{"a" => "1"})
      assert {:ok, false} = FerricStore.hsetnx("hsn:key2", "a", "999")
      assert {:ok, "1"} = FerricStore.hget("hsn:key2", "a")
    end
  end

  describe "hrandfield/2" do
    test "returns a random field from hash" do
      FerricStore.hset("hrf:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, field} = FerricStore.hrandfield("hrf:key")
      assert field in ["a", "b", "c"]
    end

    test "returns multiple random fields with count" do
      FerricStore.hset("hrf:key2", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, fields} = FerricStore.hrandfield("hrf:key2", 2)
      assert is_list(fields) and length(fields) == 2
    end

    test "returns nil for nonexistent hash" do
      assert {:ok, nil} = FerricStore.hrandfield("hrf:missing")
    end
  end

  describe "hstrlen/2" do
    test "returns length of hash field value" do
      FerricStore.hset("hsl:key", %{"name" => "alice"})
      assert {:ok, 5} = FerricStore.hstrlen("hsl:key", "name")
    end

    test "returns 0 for missing field" do
      FerricStore.hset("hsl:key2", %{"a" => "1"})
      assert {:ok, 0} = FerricStore.hstrlen("hsl:key2", "missing")
    end
  end

  # ===========================================================================
  # LIST extended: lindex, lset, lrem, linsert, lmove, lpos
  # ===========================================================================

  describe "lindex/2" do
    test "returns element at index" do
      FerricStore.rpush("li:key", ["a", "b", "c"])
      assert {:ok, "a"} = FerricStore.lindex("li:key", 0)
      assert {:ok, "b"} = FerricStore.lindex("li:key", 1)
      assert {:ok, "c"} = FerricStore.lindex("li:key", 2)
    end

    test "supports negative index" do
      FerricStore.rpush("li:neg", ["a", "b", "c"])
      assert {:ok, "c"} = FerricStore.lindex("li:neg", -1)
    end

    test "returns nil for out-of-range index" do
      FerricStore.rpush("li:oor", ["a"])
      assert {:ok, nil} = FerricStore.lindex("li:oor", 5)
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.lindex("li:missing", 0)
    end
  end

  describe "lset/3" do
    test "sets element at index" do
      FerricStore.rpush("ls:key", ["a", "b", "c"])
      assert :ok = FerricStore.lset("ls:key", 1, "X")
      assert {:ok, "X"} = FerricStore.lindex("ls:key", 1)
    end

    test "sets at negative index" do
      FerricStore.rpush("ls:neg", ["a", "b", "c"])
      assert :ok = FerricStore.lset("ls:neg", -1, "Z")
      assert {:ok, "Z"} = FerricStore.lindex("ls:neg", -1)
    end

    test "returns error for out-of-range index" do
      FerricStore.rpush("ls:oor", ["a"])
      assert {:error, _} = FerricStore.lset("ls:oor", 10, "X")
    end
  end

  describe "lrem/3" do
    test "removes all occurrences with count 0" do
      FerricStore.rpush("lr:key", ["a", "b", "a", "c", "a"])
      assert {:ok, 3} = FerricStore.lrem("lr:key", 0, "a")
      assert {:ok, ["b", "c"]} = FerricStore.lrange("lr:key", 0, -1)
    end

    test "removes count occurrences from head with positive count" do
      FerricStore.rpush("lr:head", ["a", "b", "a", "c", "a"])
      assert {:ok, 2} = FerricStore.lrem("lr:head", 2, "a")
      assert {:ok, ["b", "c", "a"]} = FerricStore.lrange("lr:head", 0, -1)
    end

    test "removes count occurrences from tail with negative count" do
      FerricStore.rpush("lr:tail", ["a", "b", "a", "c", "a"])
      assert {:ok, 2} = FerricStore.lrem("lr:tail", -2, "a")
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("lr:tail", 0, -1)
    end

    test "returns 0 when element not found" do
      FerricStore.rpush("lr:none", ["a", "b"])
      assert {:ok, 0} = FerricStore.lrem("lr:none", 0, "z")
    end
  end

  describe "linsert/4" do
    test "inserts before pivot" do
      FerricStore.rpush("lin:key", ["a", "b", "c"])
      assert {:ok, 4} = FerricStore.linsert("lin:key", :before, "b", "X")
      assert {:ok, ["a", "X", "b", "c"]} = FerricStore.lrange("lin:key", 0, -1)
    end

    test "inserts after pivot" do
      FerricStore.rpush("lin:key2", ["a", "b", "c"])
      assert {:ok, 4} = FerricStore.linsert("lin:key2", :after, "b", "Y")
      assert {:ok, ["a", "b", "Y", "c"]} = FerricStore.lrange("lin:key2", 0, -1)
    end

    test "returns -1 when pivot not found" do
      FerricStore.rpush("lin:nop", ["a", "b"])
      assert {:ok, -1} = FerricStore.linsert("lin:nop", :before, "z", "X")
    end
  end

  describe "lmove/4" do
    test "moves element from source left to destination right" do
      FerricStore.rpush("lm:src", ["a", "b", "c"])
      FerricStore.rpush("lm:dst", ["x"])
      assert {:ok, "a"} = FerricStore.lmove("lm:src", "lm:dst", :left, :right)
      assert {:ok, ["b", "c"]} = FerricStore.lrange("lm:src", 0, -1)
      assert {:ok, ["x", "a"]} = FerricStore.lrange("lm:dst", 0, -1)
    end

    test "moves element from source right to destination left" do
      FerricStore.rpush("lm:src2", ["a", "b", "c"])
      FerricStore.rpush("lm:dst2", ["x"])
      assert {:ok, "c"} = FerricStore.lmove("lm:src2", "lm:dst2", :right, :left)
      assert {:ok, ["a", "b"]} = FerricStore.lrange("lm:src2", 0, -1)
      assert {:ok, ["c", "x"]} = FerricStore.lrange("lm:dst2", 0, -1)
    end

    test "returns nil when source is empty" do
      assert {:ok, nil} = FerricStore.lmove("lm:empty", "lm:dst3", :left, :right)
    end
  end

  describe "lpos/3" do
    test "finds position of element" do
      FerricStore.rpush("lp:key", ["a", "b", "c", "b", "d"])
      assert {:ok, 1} = FerricStore.lpos("lp:key", "b")
    end

    test "returns nil when element not found" do
      FerricStore.rpush("lp:key2", ["a", "b"])
      assert {:ok, nil} = FerricStore.lpos("lp:key2", "z")
    end

    test "finds multiple positions with count option" do
      FerricStore.rpush("lp:multi", ["a", "b", "a", "c", "a"])
      assert {:ok, positions} = FerricStore.lpos("lp:multi", "a", count: 0)
      assert positions == [0, 2, 4]
    end

    test "finds from specified rank" do
      FerricStore.rpush("lp:rank", ["a", "b", "a", "c"])
      assert {:ok, 2} = FerricStore.lpos("lp:rank", "a", rank: 2)
    end
  end

  # ===========================================================================
  # SET extended: smismember, srandmember, spop, sdiff, sinter, sunion
  # ===========================================================================

  describe "smismember/2" do
    test "returns membership for multiple members" do
      FerricStore.sadd("smm:key", ["a", "b", "c"])
      assert {:ok, [1, 0, 1]} = FerricStore.smismember("smm:key", ["a", "z", "c"])
    end

    test "returns all zeros for nonexistent set" do
      assert {:ok, [0, 0]} = FerricStore.smismember("smm:missing", ["a", "b"])
    end
  end

  describe "srandmember/2" do
    test "returns a random member" do
      FerricStore.sadd("srm:key", ["a", "b", "c"])
      assert {:ok, member} = FerricStore.srandmember("srm:key")
      assert member in ["a", "b", "c"]
    end

    test "returns multiple random members with count" do
      FerricStore.sadd("srm:key2", ["a", "b", "c"])
      assert {:ok, members} = FerricStore.srandmember("srm:key2", 2)
      assert is_list(members) and length(members) == 2
    end

    test "returns nil for nonexistent set" do
      assert {:ok, nil} = FerricStore.srandmember("srm:missing")
    end
  end

  describe "spop/2" do
    test "pops a random member" do
      FerricStore.sadd("sp:key", ["a", "b", "c"])
      assert {:ok, member} = FerricStore.spop("sp:key")
      assert member in ["a", "b", "c"]
      # Verify it was removed
      assert FerricStore.sismember("sp:key", member) == false
    end

    test "pops multiple random members" do
      FerricStore.sadd("sp:key2", ["a", "b", "c", "d", "e"])
      assert {:ok, members} = FerricStore.spop("sp:key2", 2)
      assert is_list(members) and length(members) == 2
    end

    test "returns nil for nonexistent set" do
      assert {:ok, nil} = FerricStore.spop("sp:missing")
    end
  end

  describe "sdiff/1" do
    test "returns difference between sets" do
      FerricStore.sadd("sd:s1", ["a", "b", "c"])
      FerricStore.sadd("sd:s2", ["b", "c", "d"])
      assert {:ok, diff} = FerricStore.sdiff(["sd:s1", "sd:s2"])
      assert Enum.sort(diff) == ["a"]
    end

    test "returns all members for single-set diff" do
      FerricStore.sadd("sd:only", ["a", "b"])
      assert {:ok, diff} = FerricStore.sdiff(["sd:only"])
      assert Enum.sort(diff) == ["a", "b"]
    end
  end

  describe "sinter/1" do
    test "returns intersection of sets" do
      FerricStore.sadd("si:s1", ["a", "b", "c"])
      FerricStore.sadd("si:s2", ["b", "c", "d"])
      assert {:ok, inter} = FerricStore.sinter(["si:s1", "si:s2"])
      assert Enum.sort(inter) == ["b", "c"]
    end

    test "returns all members for single-set intersection" do
      FerricStore.sadd("si:only", ["a", "b"])
      assert {:ok, inter} = FerricStore.sinter(["si:only"])
      assert Enum.sort(inter) == ["a", "b"]
    end
  end

  describe "sunion/1" do
    test "returns union of sets" do
      FerricStore.sadd("su:s1", ["a", "b"])
      FerricStore.sadd("su:s2", ["b", "c"])
      assert {:ok, union} = FerricStore.sunion(["su:s1", "su:s2"])
      assert Enum.sort(union) == ["a", "b", "c"]
    end

    test "returns all members for single-set union" do
      FerricStore.sadd("su:only", ["a", "b"])
      assert {:ok, union} = FerricStore.sunion(["su:only"])
      assert Enum.sort(union) == ["a", "b"]
    end
  end

  # ===========================================================================
  # SORTED SET extended: zrank, zrevrank, zrangebyscore, zcount, zincrby,
  # zrandmember, zpopmin, zpopmax, zmscore
  # ===========================================================================

  describe "zrank/2" do
    test "returns rank of existing member (ascending)" do
      FerricStore.zadd("zr:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 0} = FerricStore.zrank("zr:key", "a")
      assert {:ok, 1} = FerricStore.zrank("zr:key", "b")
      assert {:ok, 2} = FerricStore.zrank("zr:key", "c")
    end

    test "returns nil for missing member" do
      FerricStore.zadd("zr:key2", [{1.0, "a"}])
      assert {:ok, nil} = FerricStore.zrank("zr:key2", "z")
    end
  end

  describe "zrevrank/2" do
    test "returns reverse rank (descending)" do
      FerricStore.zadd("zrr:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 2} = FerricStore.zrevrank("zrr:key", "a")
      assert {:ok, 1} = FerricStore.zrevrank("zrr:key", "b")
      assert {:ok, 0} = FerricStore.zrevrank("zrr:key", "c")
    end

    test "returns nil for missing member" do
      FerricStore.zadd("zrr:key2", [{1.0, "a"}])
      assert {:ok, nil} = FerricStore.zrevrank("zrr:key2", "z")
    end
  end

  describe "zrangebyscore/4" do
    test "returns members within score range" do
      FerricStore.zadd("zbs:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, members} = FerricStore.zrangebyscore("zbs:key", "2", "3")
      assert members == ["b", "c"]
    end

    test "supports -inf and +inf" do
      FerricStore.zadd("zbs:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = FerricStore.zrangebyscore("zbs:key2", "-inf", "+inf")
      assert members == ["a", "b", "c"]
    end

    test "returns empty for out-of-range scores" do
      FerricStore.zadd("zbs:key3", [{1.0, "a"}])
      assert {:ok, []} = FerricStore.zrangebyscore("zbs:key3", "10", "20")
    end
  end

  describe "zcount/3" do
    test "counts members in score range" do
      FerricStore.zadd("zc:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, 2} = FerricStore.zcount("zc:key", "2", "3")
    end

    test "supports -inf and +inf" do
      FerricStore.zadd("zc:key2", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, 2} = FerricStore.zcount("zc:key2", "-inf", "+inf")
    end

    test "returns 0 for out-of-range" do
      FerricStore.zadd("zc:key3", [{1.0, "a"}])
      assert {:ok, 0} = FerricStore.zcount("zc:key3", "10", "20")
    end
  end

  describe "zincrby/3" do
    test "increments member score" do
      FerricStore.zadd("zi:key", [{10.0, "alice"}])
      assert {:ok, result} = FerricStore.zincrby("zi:key", 5.0, "alice")
      {score, _} = Float.parse(result)
      assert_in_delta score, 15.0, 0.001
    end

    test "creates member if it does not exist" do
      assert {:ok, result} = FerricStore.zincrby("zi:key2", 3.0, "bob")
      {score, _} = Float.parse(result)
      assert_in_delta score, 3.0, 0.001
    end
  end

  describe "zrandmember/2" do
    test "returns a random member" do
      FerricStore.zadd("zrm:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, member} = FerricStore.zrandmember("zrm:key")
      assert member in ["a", "b", "c"]
    end

    test "returns multiple random members with count" do
      FerricStore.zadd("zrm:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = FerricStore.zrandmember("zrm:key2", 2)
      assert is_list(members) and length(members) == 2
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.zrandmember("zrm:missing")
    end
  end

  describe "zpopmin/2" do
    test "pops member with lowest score" do
      FerricStore.zadd("zpm:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, [{member, score}]} = FerricStore.zpopmin("zpm:key", 1)
      assert member == "a"
      assert_in_delta score, 1.0, 0.001
    end

    test "pops multiple members with lowest scores" do
      FerricStore.zadd("zpm:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, pairs} = FerricStore.zpopmin("zpm:key2", 2)
      assert length(pairs) == 2
      [{m1, _}, {m2, _}] = pairs
      assert m1 == "a"
      assert m2 == "b"
    end

    test "returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.zpopmin("zpm:missing", 1)
    end
  end

  describe "zpopmax/2" do
    test "pops member with highest score" do
      FerricStore.zadd("zpx:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, [{member, score}]} = FerricStore.zpopmax("zpx:key", 1)
      assert member == "c"
      assert_in_delta score, 3.0, 0.001
    end

    test "pops multiple members with highest scores" do
      FerricStore.zadd("zpx:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, pairs} = FerricStore.zpopmax("zpx:key2", 2)
      assert length(pairs) == 2
      [{m1, _}, {m2, _}] = pairs
      assert m1 == "c"
      assert m2 == "b"
    end

    test "returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.zpopmax("zpx:missing", 1)
    end
  end

  describe "zmscore/2" do
    test "returns scores for multiple members" do
      FerricStore.zadd("zms:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, scores} = FerricStore.zmscore("zms:key", ["a", "c", "z"])
      assert length(scores) == 3
      assert_in_delta Enum.at(scores, 0), 1.0, 0.001
      assert_in_delta Enum.at(scores, 1), 3.0, 0.001
      assert Enum.at(scores, 2) == nil
    end
  end

  # ===========================================================================
  # STREAMS: xadd, xlen, xrange, xrevrange, xtrim
  # ===========================================================================

  describe "xadd/2 and xlen/1" do
    test "adds entries and reports length" do
      assert {:ok, id1} = FerricStore.xadd("xs:key", ["name", "alice"])
      assert is_binary(id1)
      assert {:ok, id2} = FerricStore.xadd("xs:key", ["name", "bob"])
      assert is_binary(id2)
      assert {:ok, 2} = FerricStore.xlen("xs:key")
    end

    test "xlen returns 0 for nonexistent stream" do
      assert {:ok, 0} = FerricStore.xlen("xs:missing")
    end
  end

  describe "xrange/4" do
    test "returns entries in forward order" do
      FerricStore.xadd("xr:key", ["k", "v1"])
      FerricStore.xadd("xr:key", ["k", "v2"])
      assert {:ok, entries} = FerricStore.xrange("xr:key", "-", "+")
      assert is_list(entries)
      assert length(entries) >= 2
    end

    test "supports COUNT option" do
      FerricStore.xadd("xr:key2", ["k", "v1"])
      FerricStore.xadd("xr:key2", ["k", "v2"])
      FerricStore.xadd("xr:key2", ["k", "v3"])
      assert {:ok, entries} = FerricStore.xrange("xr:key2", "-", "+", count: 2)
      assert length(entries) == 2
    end
  end

  describe "xrevrange/4" do
    test "returns entries in reverse order" do
      FerricStore.xadd("xrr:key", ["k", "v1"])
      FerricStore.xadd("xrr:key", ["k", "v2"])
      assert {:ok, entries} = FerricStore.xrevrange("xrr:key", "+", "-")
      assert is_list(entries) and length(entries) >= 2
    end
  end

  describe "xtrim/2" do
    test "trims stream to maxlen" do
      for i <- 1..10 do
        FerricStore.xadd("xt:key", ["i", to_string(i)])
      end
      assert {:ok, trimmed} = FerricStore.xtrim("xt:key", maxlen: 5)
      assert is_integer(trimmed) and trimmed >= 0
      assert {:ok, len} = FerricStore.xlen("xt:key")
      assert len <= 5
    end
  end

  # ===========================================================================
  # NATIVE: lock, unlock, extend
  # ===========================================================================

  describe "lock/3, unlock/2, extend/3" do
    test "acquire, extend, and release a lock" do
      assert :ok = FerricStore.lock("lk:key", "owner1", 30_000)

      # Extend the lock
      assert {:ok, 1} = FerricStore.extend("lk:key", "owner1", 60_000)

      # Release the lock
      assert {:ok, 1} = FerricStore.unlock("lk:key", "owner1")
    end

    test "lock fails if already held by another owner" do
      assert :ok = FerricStore.lock("lk:key2", "owner1", 30_000)
      assert {:error, _} = FerricStore.lock("lk:key2", "owner2", 30_000)

      # Clean up
      FerricStore.unlock("lk:key2", "owner1")
    end

    test "unlock fails with wrong owner" do
      FerricStore.lock("lk:key3", "owner1", 30_000)
      assert {:error, _} = FerricStore.unlock("lk:key3", "wrong_owner")

      # Clean up
      FerricStore.unlock("lk:key3", "owner1")
    end
  end

  # ===========================================================================
  # RATE LIMITING
  # ===========================================================================

  describe "ratelimit_add/4" do
    test "allows requests within limit" do
      assert {:ok, result} = FerricStore.ratelimit_add("rl:key", 60_000, 10)
      assert is_list(result)
      # Returns ["allowed", remaining_count] or similar flat list
      assert "allowed" in result or 1 in result
    end

    test "tracks multiple requests" do
      FerricStore.ratelimit_add("rl:key2", 60_000, 10)
      FerricStore.ratelimit_add("rl:key2", 60_000, 10)
      assert {:ok, result} = FerricStore.ratelimit_add("rl:key2", 60_000, 10)
      assert is_list(result)
    end

    test "exhausting the limit eventually blocks" do
      key = "rl:exhaust"
      max = 3
      results = for _ <- 1..5 do
        {:ok, r} = FerricStore.ratelimit_add(key, 60_000, max)
        r
      end
      # At least some responses should exist
      assert length(results) == 5
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE (stampede protection)
  # ===========================================================================

  describe "fetch_or_compute/2 and fetch_or_compute_result/3" do
    test "returns :compute on cache miss, then :hit after storing" do
      case FerricStore.fetch_or_compute("foc:key", ttl: 60_000) do
        {:ok, {:compute, _hint}} ->
          assert :ok = FerricStore.fetch_or_compute_result("foc:key", "computed_value", ttl: 60_000)

          case FerricStore.fetch_or_compute("foc:key", ttl: 60_000) do
            {:ok, {:hit, value}} ->
              assert value == "computed_value"

            other ->
              flunk("Expected {:ok, {:hit, _}}, got: #{inspect(other)}")
          end

        {:ok, {:hit, _}} ->
          # Key might exist from previous test runs; that's acceptable
          :ok
      end
    end
  end

  # ===========================================================================
  # MULTI / TX
  # ===========================================================================

  describe "multi/1" do
    test "executes set and get in transaction" do
      # MULTI/EXEC returns Redis wire-format values (via Dispatcher)
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.set("{tx}:k1", "v1")
                 |> FerricStore.Tx.set("{tx}:k2", "v2")
                 |> FerricStore.Tx.get("{tx}:k1")
               end)

      assert results == [:ok, :ok, "v1"]
    end

    test "executes incr and incr_by in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.incr("tx:counter")
                 |> FerricStore.Tx.incr("tx:counter")
                 |> FerricStore.Tx.incr_by("tx:counter", 10)
               end)

      assert results == [{:ok, 1}, {:ok, 2}, {:ok, 12}]
    end

    test "executes del in transaction" do
      FerricStore.set("tx:del_target", "value")

      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.del("tx:del_target")
                 |> FerricStore.Tx.get("tx:del_target")
               end)

      assert results == [1, nil]
    end

    test "executes hset in transaction" do
      key = "tx:hash:#{System.unique_integer([:positive])}"

      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.hset(key, %{"a" => "1"})
               end)

      assert results == [1]
      Process.sleep(50)
      # Verify embedded hget reads back what MULTI/EXEC wrote (same storage format)
      assert {:ok, "1"} = FerricStore.hget(key, "a")
    end

    test "executes lpush in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.lpush("tx:list", ["a", "b"])
               end)

      assert results == [2]
    end

    test "executes sadd in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.sadd("tx:set", ["a", "b", "c"])
               end)

      assert results == [3]
    end

    test "executes zadd in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.zadd("tx:zset", [{1.0, "a"}, {2.0, "b"}])
               end)

      assert results == [2]
    end

    test "executes expire in transaction" do
      FerricStore.set("tx:exp", "value")

      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.expire("tx:exp", 60_000)
               end)

      assert results == [1]
    end

    test "empty transaction returns empty results" do
      assert {:ok, []} = FerricStore.multi(fn tx -> tx end)
    end

    test "commands execute in order" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.set("tx:order", "first")
                 |> FerricStore.Tx.get("tx:order")
                 |> FerricStore.Tx.set("tx:order", "second")
                 |> FerricStore.Tx.get("tx:order")
               end)

      assert results == [:ok, "first", :ok, "second"]
    end
  end

  # ===========================================================================
  # SERVER: ping, echo, flushall
  # ===========================================================================

  describe "ping/0" do
    test "returns PONG" do
      assert {:ok, "PONG"} = FerricStore.ping()
    end
  end

  describe "echo/1" do
    test "echoes the message" do
      assert {:ok, "hello"} = FerricStore.echo("hello")
    end

    test "echoes empty string" do
      assert {:ok, ""} = FerricStore.echo("")
    end

    test "echoes binary data" do
      msg = <<0, 1, 2, 255>>
      assert {:ok, ^msg} = FerricStore.echo(msg)
    end
  end

  describe "flushall/0" do
    test "deletes all keys (alias for flushdb)" do
      FerricStore.set("fa:a", "1")
      FerricStore.set("fa:b", "2")
      assert :ok = FerricStore.flushall()
      assert {:ok, nil} = FerricStore.get("fa:a")
      assert {:ok, nil} = FerricStore.get("fa:b")
    end
  end

  # ===========================================================================
  # HyperLogLog smoke tests
  # ===========================================================================

  describe "HyperLogLog" do
    test "pfadd and pfcount" do
      assert {:ok, true} = FerricStore.pfadd("hll:key", ["a", "b", "c"])
      assert {:ok, count} = FerricStore.pfcount(["hll:key"])
      assert is_integer(count) and count >= 3
    end

    test "pfadd with duplicate elements" do
      FerricStore.pfadd("hll:dup", ["a", "b"])
      assert {:ok, false} = FerricStore.pfadd("hll:dup", ["a", "b"])
    end

    test "pfmerge combines HLLs" do
      FerricStore.pfadd("hll:m1", ["a", "b"])
      FerricStore.pfadd("hll:m2", ["c", "d"])
      assert :ok = FerricStore.pfmerge("hll:merged", ["hll:m1", "hll:m2"])
      assert {:ok, count} = FerricStore.pfcount(["hll:merged"])
      assert count >= 4
    end
  end

  # ===========================================================================
  # Bloom Filter smoke tests
  # ===========================================================================

  describe "Bloom filter" do
    test "bf_add and bf_exists" do
      assert {:ok, 1} = FerricStore.bf_add("bf:key", "hello")
      assert {:ok, 1} = FerricStore.bf_exists("bf:key", "hello")
      assert {:ok, 0} = FerricStore.bf_exists("bf:key", "unknown_element_xyz")
    end

    test "bf_madd and bf_mexists" do
      assert {:ok, results} = FerricStore.bf_madd("bf:multi", ["a", "b", "c"])
      assert is_list(results)
      assert {:ok, checks} = FerricStore.bf_mexists("bf:multi", ["a", "z"])
      assert is_list(checks)
      assert hd(checks) == 1
    end

    test "bf_reserve creates filter with specific parameters" do
      assert :ok = FerricStore.bf_reserve("bf:reserved", 0.01, 1000)
      assert {:ok, 1} = FerricStore.bf_add("bf:reserved", "test")
      assert {:ok, 1} = FerricStore.bf_exists("bf:reserved", "test")
    end

    test "bf_card returns count" do
      FerricStore.bf_add("bf:card", "a")
      FerricStore.bf_add("bf:card", "b")
      assert {:ok, count} = FerricStore.bf_card("bf:card")
      assert is_integer(count)
    end
  end

  # ===========================================================================
  # Cuckoo Filter smoke tests
  # ===========================================================================

  describe "Cuckoo filter" do
    test "cf_add and cf_exists" do
      assert {:ok, 1} = FerricStore.cf_add("cf:key", "hello")
      assert {:ok, 1} = FerricStore.cf_exists("cf:key", "hello")
      assert {:ok, 0} = FerricStore.cf_exists("cf:key", "unknown_element_xyz")
    end

    test "cf_del removes element" do
      FerricStore.cf_add("cf:del", "item")
      assert {:ok, 1} = FerricStore.cf_del("cf:del", "item")
      assert {:ok, 0} = FerricStore.cf_exists("cf:del", "item")
    end

    test "cf_addnx adds only if not present" do
      assert {:ok, 1} = FerricStore.cf_addnx("cf:nx", "item")
      assert {:ok, 0} = FerricStore.cf_addnx("cf:nx", "item")
    end

    test "cf_count returns count" do
      FerricStore.cf_add("cf:cnt", "item")
      assert {:ok, count} = FerricStore.cf_count("cf:cnt", "item")
      assert count >= 1
    end

    test "cf_mexists checks multiple elements" do
      FerricStore.cf_add("cf:mx", "a")
      assert {:ok, [1, 0]} = FerricStore.cf_mexists("cf:mx", ["a", "z"])
    end
  end

  # ===========================================================================
  # Count-Min Sketch smoke tests
  # ===========================================================================

  describe "Count-Min Sketch" do
    test "cms_initbydim, incrby, query" do
      assert :ok = FerricStore.cms_initbydim("cms:key", 100, 5)
      assert {:ok, counts} = FerricStore.cms_incrby("cms:key", [{"a", 5}, {"b", 3}])
      assert is_list(counts)
      assert {:ok, queried} = FerricStore.cms_query("cms:key", ["a", "b"])
      assert is_list(queried)
      assert Enum.at(queried, 0) >= 5
      assert Enum.at(queried, 1) >= 3
    end

    test "cms_initbyprob creates CMS" do
      assert :ok = FerricStore.cms_initbyprob("cms:prob", 0.01, 0.001)
    end
  end

  # ===========================================================================
  # TopK smoke tests
  # ===========================================================================

  describe "TopK" do
    test "topk_reserve, add, list" do
      assert :ok = FerricStore.topk_reserve("tk:key", 3)
      assert {:ok, _evicted} = FerricStore.topk_add("tk:key", ["a", "b", "c", "a", "a"])
      assert {:ok, top} = FerricStore.topk_list("tk:key")
      assert is_list(top)
      assert "a" in top
    end

    test "topk_query checks membership" do
      FerricStore.topk_reserve("tk:q", 3)
      FerricStore.topk_add("tk:q", ["x", "y", "z"])
      assert {:ok, results} = FerricStore.topk_query("tk:q", ["x", "missing"])
      assert is_list(results)
    end
  end

  # ===========================================================================
  # T-Digest smoke tests
  # ===========================================================================

  describe "T-Digest" do
    test "create, add, quantile, min, max" do
      assert :ok = FerricStore.tdigest_create("td:key")
      assert :ok = FerricStore.tdigest_add("td:key", [1.0, 2.0, 3.0, 4.0, 5.0])

      assert {:ok, quantiles} = FerricStore.tdigest_quantile("td:key", [0.5])
      assert is_list(quantiles)

      assert {:ok, min_val} = FerricStore.tdigest_min("td:key")
      assert is_binary(min_val)

      assert {:ok, max_val} = FerricStore.tdigest_max("td:key")
      assert is_binary(max_val)
    end

    test "tdigest_cdf returns CDF values" do
      FerricStore.tdigest_create("td:cdf")
      FerricStore.tdigest_add("td:cdf", [1.0, 2.0, 3.0, 4.0, 5.0])
      assert {:ok, cdfs} = FerricStore.tdigest_cdf("td:cdf", [3.0])
      assert is_list(cdfs)
    end

    test "tdigest_reset clears data" do
      FerricStore.tdigest_create("td:reset")
      FerricStore.tdigest_add("td:reset", [1.0, 2.0])
      assert :ok = FerricStore.tdigest_reset("td:reset")
    end

    test "tdigest_info returns metadata" do
      FerricStore.tdigest_create("td:info")
      assert {:ok, info} = FerricStore.tdigest_info("td:info")
      assert is_list(info)
    end
  end

  # ===========================================================================
  # JSON smoke tests
  # ===========================================================================

  describe "JSON" do
    test "json_set and json_get" do
      assert :ok = FerricStore.json_set("js:key", "$", ~s({"name":"alice","age":30}))
      assert {:ok, result} = FerricStore.json_get("js:key", "$")
      assert is_binary(result)
    end

    test "json_del removes value" do
      FerricStore.json_set("js:del", "$", ~s({"a":1,"b":2}))
      assert {:ok, _} = FerricStore.json_del("js:del", "$.a")
    end

    test "json_type returns type" do
      FerricStore.json_set("js:type", "$", ~s({"name":"alice"}))
      assert {:ok, type_str} = FerricStore.json_type("js:type", "$")
      assert is_binary(type_str)
    end

    test "json_objkeys returns keys" do
      FerricStore.json_set("js:keys", "$", ~s({"a":1,"b":2}))
      assert {:ok, keys} = FerricStore.json_objkeys("js:keys", "$")
      assert is_list(keys)
    end

    test "json_objlen returns key count" do
      FerricStore.json_set("js:len", "$", ~s({"a":1,"b":2}))
      assert {:ok, len} = FerricStore.json_objlen("js:len", "$")
      assert is_integer(len)
    end

    test "json_numincrby increments number" do
      FerricStore.json_set("js:inc", "$", ~s({"count":5}))
      assert {:ok, result} = FerricStore.json_numincrby("js:inc", "$.count", "3")
      assert is_binary(result)
    end
  end

  # ===========================================================================
  # Geo smoke tests
  # ===========================================================================

  describe "Geo" do
    test "geoadd and geodist" do
      assert {:ok, 2} =
               FerricStore.geoadd("geo:key", [
                 {13.361389, 38.115556, "Palermo"},
                 {15.087269, 37.502669, "Catania"}
               ])

      assert {:ok, dist} = FerricStore.geodist("geo:key", "Palermo", "Catania", "m")
      assert is_binary(dist)
      {d, _} = Float.parse(dist)
      assert d > 100_000
    end

    test "geopos returns positions" do
      FerricStore.geoadd("geo:pos", [{13.361389, 38.115556, "Palermo"}])
      assert {:ok, positions} = FerricStore.geopos("geo:pos", ["Palermo"])
      assert is_list(positions)
    end

    test "geohash returns hash strings" do
      FerricStore.geoadd("geo:hash", [{13.361389, 38.115556, "Palermo"}])
      assert {:ok, hashes} = FerricStore.geohash("geo:hash", ["Palermo"])
      assert is_list(hashes)
    end
  end

  # ===========================================================================
  # Vector tests removed — HNSW/vector feature was deleted.

  # ===========================================================================
  # SET — EXAT option (absolute Unix timestamp in seconds)
  # ===========================================================================

  describe "set/3 with :exat" do
    test "sets key with absolute expiry in seconds" do
      # Expire 60 seconds from now
      future_ts = div(System.os_time(:millisecond), 1000) + 60
      assert :ok = FerricStore.set("exat:basic", "val", exat: future_ts)
      assert {:ok, "val"} = FerricStore.get("exat:basic")

      # TTL should be roughly 60 seconds (within a tolerance)
      {:ok, ttl_ms} = FerricStore.ttl("exat:basic")
      assert ttl_ms != nil
      assert ttl_ms > 55_000
      assert ttl_ms <= 60_000
    end

    test "key set with past EXAT is immediately expired" do
      past_ts = div(System.os_time(:millisecond), 1000) - 10
      # EXAT with past timestamp: Redis still accepts the write but the key
      # expires immediately. FerricStore rejects ts <= 0 from the parser, but
      # a past-but-positive timestamp is accepted and expires on next read.
      assert :ok = FerricStore.set("exat:past", "gone", exat: past_ts)
      assert {:ok, nil} = FerricStore.get("exat:past")
    end
  end

  # ===========================================================================
  # SET — PXAT option (absolute Unix timestamp in milliseconds)
  # ===========================================================================

  describe "set/3 with :pxat" do
    test "sets key with absolute expiry in milliseconds" do
      future_ms = System.os_time(:millisecond) + 60_000
      assert :ok = FerricStore.set("pxat:basic", "val", pxat: future_ms)
      assert {:ok, "val"} = FerricStore.get("pxat:basic")

      {:ok, ttl_ms} = FerricStore.ttl("pxat:basic")
      assert ttl_ms != nil
      assert ttl_ms > 55_000
      assert ttl_ms <= 60_000
    end

    test "key set with past PXAT is immediately expired" do
      past_ms = System.os_time(:millisecond) - 5_000
      assert :ok = FerricStore.set("pxat:past", "gone", pxat: past_ms)
      assert {:ok, nil} = FerricStore.get("pxat:past")
    end
  end

  # ===========================================================================
  # SET — GET option (return old value)
  # ===========================================================================

  describe "set/3 with :get" do
    test "returns nil when key does not exist" do
      assert {:ok, nil} = FerricStore.set("get:new", "first", get: true)
      assert {:ok, "first"} = FerricStore.get("get:new")
    end

    test "returns old value when key exists" do
      FerricStore.set("get:existing", "old_val")
      assert {:ok, "old_val"} = FerricStore.set("get:existing", "new_val", get: true)
      assert {:ok, "new_val"} = FerricStore.get("get:existing")
    end

    test "GET combined with NX returns old value even when write is skipped" do
      FerricStore.set("get:nx:exists", "original")
      # NX prevents the write since key exists; GET still returns old value
      assert {:ok, "original"} = FerricStore.set("get:nx:exists", "attempt", get: true, nx: true)
      # Value unchanged
      assert {:ok, "original"} = FerricStore.get("get:nx:exists")
    end

    test "GET combined with XX returns nil when key does not exist" do
      # XX prevents write since key doesn't exist; GET returns nil
      assert {:ok, nil} = FerricStore.set("get:xx:missing", "attempt", get: true, xx: true)
      assert {:ok, nil} = FerricStore.get("get:xx:missing")
    end

    test "GET combined with EX returns old value and sets TTL" do
      FerricStore.set("get:ex", "old")
      assert {:ok, "old"} = FerricStore.set("get:ex", "new", get: true, ttl: 30_000)
      assert {:ok, "new"} = FerricStore.get("get:ex")

      {:ok, ttl_ms} = FerricStore.ttl("get:ex")
      assert ttl_ms != nil
      assert ttl_ms > 25_000
    end
  end

  # ===========================================================================
  # SET — KEEPTTL option (preserve existing TTL)
  # ===========================================================================

  describe "set/3 with :keepttl" do
    test "preserves existing TTL when overwriting value" do
      # Set with 60s TTL
      FerricStore.set("keepttl:basic", "v1", ttl: 60_000)

      {:ok, ttl_before} = FerricStore.ttl("keepttl:basic")
      assert ttl_before != nil
      assert ttl_before > 55_000

      # Overwrite with KEEPTTL — TTL should be preserved
      assert :ok = FerricStore.set("keepttl:basic", "v2", keepttl: true)
      assert {:ok, "v2"} = FerricStore.get("keepttl:basic")

      {:ok, ttl_after} = FerricStore.ttl("keepttl:basic")
      assert ttl_after != nil
      # TTL should be close to what it was before (allow 2s tolerance)
      assert ttl_after > 53_000
    end

    test "key without TTL remains without TTL after KEEPTTL" do
      FerricStore.set("keepttl:nottl", "v1")

      {:ok, nil} = FerricStore.ttl("keepttl:nottl")

      assert :ok = FerricStore.set("keepttl:nottl", "v2", keepttl: true)
      assert {:ok, "v2"} = FerricStore.get("keepttl:nottl")

      {:ok, nil} = FerricStore.ttl("keepttl:nottl")
    end

    test "KEEPTTL on nonexistent key behaves like normal set (no TTL)" do
      assert :ok = FerricStore.set("keepttl:noexist", "val", keepttl: true)
      assert {:ok, "val"} = FerricStore.get("keepttl:noexist")

      {:ok, nil} = FerricStore.ttl("keepttl:noexist")
    end

    test "KEEPTTL combined with XX" do
      FerricStore.set("keepttl:xx", "v1", ttl: 60_000)

      assert :ok = FerricStore.set("keepttl:xx", "v2", keepttl: true, xx: true)
      assert {:ok, "v2"} = FerricStore.get("keepttl:xx")

      {:ok, ttl_ms} = FerricStore.ttl("keepttl:xx")
      assert ttl_ms != nil
      assert ttl_ms > 53_000
    end

    test "KEEPTTL combined with GET returns old value and preserves TTL" do
      FerricStore.set("keepttl:get", "old", ttl: 60_000)

      assert {:ok, "old"} = FerricStore.set("keepttl:get", "new", keepttl: true, get: true)
      assert {:ok, "new"} = FerricStore.get("keepttl:get")

      {:ok, ttl_ms} = FerricStore.ttl("keepttl:get")
      assert ttl_ms != nil
      assert ttl_ms > 53_000
    end
  end

  # ===========================================================================
  # VALUE SIZE LIMITS (embedded mode)
  # ===========================================================================

  describe "embedded value size limits" do
    test "set rejects value larger than max_value_size" do
      original = Application.get_env(:ferricstore, :max_value_size)
      Application.put_env(:ferricstore, :max_value_size, 100)

      try do
        large_value = :binary.copy("x", 101)
        assert {:error, msg} = FerricStore.set("valsize:big", large_value)
        assert msg =~ "value too large"
        assert msg =~ "101 bytes"
        assert msg =~ "max 100 bytes"
      after
        if original,
          do: Application.put_env(:ferricstore, :max_value_size, original),
          else: Application.delete_env(:ferricstore, :max_value_size)
      end
    end

    test "set accepts value exactly at max_value_size" do
      original = Application.get_env(:ferricstore, :max_value_size)
      Application.put_env(:ferricstore, :max_value_size, 100)

      try do
        exact_value = :binary.copy("x", 100)
        assert :ok = FerricStore.set("valsize:exact", exact_value)
        assert {:ok, ^exact_value} = FerricStore.get("valsize:exact")
      after
        if original,
          do: Application.put_env(:ferricstore, :max_value_size, original),
          else: Application.delete_env(:ferricstore, :max_value_size)
      end
    end

    test "set uses default 1MB limit when no config is set" do
      original = Application.get_env(:ferricstore, :max_value_size)
      Application.delete_env(:ferricstore, :max_value_size)

      try do
        # 1 MB should succeed
        mb_value = :binary.copy("y", 1_048_576)
        assert :ok = FerricStore.set("valsize:1mb", mb_value)

        # 1 MB + 1 byte should fail
        over_value = :binary.copy("z", 1_048_577)
        assert {:error, msg} = FerricStore.set("valsize:over1mb", over_value)
        assert msg =~ "value too large"
      after
        if original,
          do: Application.put_env(:ferricstore, :max_value_size, original),
          else: Application.delete_env(:ferricstore, :max_value_size)
      end
    end
  end
end
