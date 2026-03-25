defmodule Ferricstore.Store.TypedValuesTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  describe "INCR stores native integer" do
    test "INCR on nonexistent key creates integer in ETS" do
      assert {:ok, 1} = Router.incr("typed:incr_new", 1)
      value = Router.get("typed:incr_new")
      assert is_integer(value)
      assert value == 1
    end

    test "INCR on string parses and stores integer" do
      Router.put("typed:incr_str", "10", 0)
      assert {:ok, 11} = Router.incr("typed:incr_str", 1)
      value = Router.get("typed:incr_str")
      assert is_integer(value)
      assert value == 11
    end

    test "multiple INCRs accumulate correctly" do
      assert {:ok, 1} = Router.incr("typed:incr_multi", 1)
      assert {:ok, 3} = Router.incr("typed:incr_multi", 2)
      assert {:ok, 13} = Router.incr("typed:incr_multi", 10)
      assert Router.get("typed:incr_multi") == 13
    end

    test "INCR on non-integer returns error, value unchanged" do
      Router.put("typed:incr_bad", "hello", 0)
      assert {:error, _} = Router.incr("typed:incr_bad", 1)
      assert Router.get("typed:incr_bad") == "hello"
    end

    test "INCR on float returns error" do
      Router.put("typed:incr_on_float", "3.14", 0)
      # INCR on "3.14" should fail since it's not an integer
      assert {:error, _} = Router.incr("typed:incr_on_float", 1)
    end
  end

  describe "INCRBYFLOAT stores native float" do
    test "INCRBYFLOAT on nonexistent key creates float in ETS" do
      assert {:ok, result} = Router.incr_float("typed:float_new", 3.14)
      assert is_float(result)
      assert_in_delta result, 3.14, 0.001

      value = Router.get("typed:float_new")
      assert is_float(value)
      assert_in_delta value, 3.14, 0.001
    end

    test "INCRBYFLOAT on integer produces float" do
      Router.incr("typed:float_from_int", 10)
      assert {:ok, result} = Router.incr_float("typed:float_from_int", 0.5)
      assert is_float(result)
      assert_in_delta result, 10.5, 0.001

      value = Router.get("typed:float_from_int")
      assert is_float(value)
    end

    test "INCRBYFLOAT on string parses and stores float" do
      Router.put("typed:float_str", "10.5", 0)
      assert {:ok, result} = Router.incr_float("typed:float_str", 2.5)
      assert is_float(result)
      assert_in_delta result, 13.0, 0.001
    end
  end

  describe "SET stores binary" do
    test "SET always stores binary" do
      Router.put("typed:set_str", "hello", 0)
      value = Router.get("typed:set_str")
      assert is_binary(value)
      assert value == "hello"
    end

    test "SET overwrites integer with string" do
      Router.incr("typed:set_overwrite", 42)
      assert Router.get("typed:set_overwrite") == 42
      Router.put("typed:set_overwrite", "hello", 0)
      assert Router.get("typed:set_overwrite") == "hello"
    end
  end

  describe "GET returns native types" do
    test "GET returns integer unchanged" do
      Router.incr("typed:get_int", 42)
      value = Router.get("typed:get_int")
      assert is_integer(value)
      assert value == 42
    end

    test "GET returns float unchanged" do
      Router.incr_float("typed:get_float", 3.14)
      value = Router.get("typed:get_float")
      assert is_float(value)
      assert_in_delta value, 3.14, 0.001
    end

    test "GET returns binary unchanged" do
      Router.put("typed:get_bin", "hello", 0)
      assert Router.get("typed:get_bin") == "hello"
    end
  end

  describe "string commands handle typed values" do
    test "APPEND on integer converts to string" do
      Router.incr("typed:append_int", 42)
      assert {:ok, 3} = Router.append("typed:append_int", "!")
      value = Router.get("typed:append_int")
      assert is_binary(value)
      assert value == "42!"
    end

    test "STRLEN on integer counts string representation length" do
      Router.incr("typed:strlen_int", 42)
      # STRLEN dispatches to the commands layer which handles conversion
      store = build_store()
      result = Ferricstore.Commands.Strings.handle("STRLEN", ["typed:strlen_int"], store)
      assert result == 2
    end

    test "GETRANGE on integer extracts from string representation" do
      Router.incr("typed:getrange_int", 12345)
      store = build_store()
      result = Ferricstore.Commands.Strings.handle("GETRANGE", ["typed:getrange_int", "0", "2"], store)
      assert result == "123"
    end
  end

  describe "type transitions" do
    test "String -> Integer -> String (via APPEND)" do
      # Start as string
      Router.put("typed:transition", "42", 0)
      assert is_binary(Router.get("typed:transition"))

      # INCR converts to integer
      assert {:ok, 43} = Router.incr("typed:transition", 1)
      assert is_integer(Router.get("typed:transition"))

      # APPEND converts back to string
      assert {:ok, 3} = Router.append("typed:transition", "!")
      value = Router.get("typed:transition")
      assert is_binary(value)
      assert value == "43!"

      # INCR on "43!" should fail
      assert {:error, _} = Router.incr("typed:transition", 1)
    end
  end

  describe "disk format is binary" do
    test "Bitcask disk format is binary after flush" do
      Router.incr("typed:disk_int", 42)
      Router.incr_float("typed:disk_float", 3.14)

      # Flush to disk
      Ferricstore.Store.BitcaskWriter.flush_all()
      ShardHelpers.flush_all_shards()

      # Verify values are still readable
      assert Router.get("typed:disk_int") == 42
      assert_in_delta Router.get("typed:disk_float"), 3.14, 0.001
    end
  end

  # Build a store map for testing commands layer directly
  defp build_store do
    %{
      get: &Router.get/1,
      put: fn key, value, exp -> Router.put(key, value, exp) end,
      delete: &Router.delete/1,
      exists?: &Router.exists?/1,
      incr: &Router.incr/2,
      incr_float: &Router.incr_float/2,
      append: &Router.append/2,
      getset: &Router.getset/2,
      getdel: &Router.getdel/1,
      getex: &Router.getex/2,
      setrange: &Router.setrange/3,
      get_meta: &Router.get_meta/1
    }
  end
end
