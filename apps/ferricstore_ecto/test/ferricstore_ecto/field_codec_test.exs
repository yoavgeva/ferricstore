defmodule FerricstoreEcto.FieldCodecTest do
  use ExUnit.Case, async: true

  alias FerricstoreEcto.FieldCodec

  describe "encode/decode round-trips" do
    test "string" do
      assert FieldCodec.decode(:string, FieldCodec.encode(:string, "hello")) == "hello"
    end

    test "empty string" do
      assert FieldCodec.decode(:string, FieldCodec.encode(:string, "")) == ""
    end

    test "integer" do
      assert FieldCodec.decode(:integer, FieldCodec.encode(:integer, 42)) == 42
    end

    test "negative integer" do
      assert FieldCodec.decode(:integer, FieldCodec.encode(:integer, -100)) == -100
    end

    test "zero" do
      assert FieldCodec.decode(:integer, FieldCodec.encode(:integer, 0)) == 0
    end

    test "float" do
      encoded = FieldCodec.encode(:float, 3.14)
      assert is_binary(encoded)
      assert_in_delta FieldCodec.decode(:float, encoded), 3.14, 0.001
    end

    test "boolean true" do
      assert FieldCodec.decode(:boolean, FieldCodec.encode(:boolean, true)) == true
    end

    test "boolean false" do
      assert FieldCodec.decode(:boolean, FieldCodec.encode(:boolean, false)) == false
    end

    test "utc_datetime" do
      now = DateTime.utc_now() |> DateTime.truncate(:second)
      encoded = FieldCodec.encode(:utc_datetime, now)
      decoded = FieldCodec.decode(:utc_datetime, encoded)
      assert DateTime.compare(decoded, now) == :eq
    end

    test "utc_datetime_usec" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      encoded = FieldCodec.encode(:utc_datetime_usec, now)
      decoded = FieldCodec.decode(:utc_datetime_usec, encoded)
      assert DateTime.compare(decoded, now) == :eq
    end

    test "naive_datetime" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      encoded = FieldCodec.encode(:naive_datetime, now)
      decoded = FieldCodec.decode(:naive_datetime, encoded)
      assert NaiveDateTime.compare(decoded, now) == :eq
    end

    test "naive_datetime_usec" do
      now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:microsecond)
      encoded = FieldCodec.encode(:naive_datetime_usec, now)
      decoded = FieldCodec.decode(:naive_datetime_usec, encoded)
      assert NaiveDateTime.compare(decoded, now) == :eq
    end

    test "date" do
      today = Date.utc_today()
      assert FieldCodec.decode(:date, FieldCodec.encode(:date, today)) == today
    end

    test "id type" do
      assert FieldCodec.decode(:id, FieldCodec.encode(:id, 42)) == 42
    end

    test "binary_id type" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert FieldCodec.decode(:binary_id, FieldCodec.encode(:binary_id, uuid)) == uuid
    end

    test "map" do
      m = %{"key" => "value", "nested" => %{"a" => 1}}
      assert FieldCodec.decode(:map, FieldCodec.encode(:map, m)) == m
    end

    test "array" do
      arr = [1, 2, 3]
      assert FieldCodec.decode({:array, :integer}, FieldCodec.encode({:array, :integer}, arr)) == arr
    end
  end

  describe "nil handling" do
    test "encode nil returns nil sentinel" do
      sentinel = FieldCodec.encode(:string, nil)
      assert sentinel == "\0"
    end

    test "decode nil returns nil" do
      assert FieldCodec.decode(:string, nil) == nil
    end

    test "decode nil sentinel returns nil" do
      assert FieldCodec.decode(:string, "\0") == nil
    end

    test "nil round-trips for all types" do
      for type <- [:string, :integer, :float, :boolean, :utc_datetime, :date, :map] do
        encoded = FieldCodec.encode(type, nil)
        assert FieldCodec.decode(type, encoded) == nil, "nil round-trip failed for #{inspect(type)}"
      end
    end
  end

  describe "serialize_fields/1" do
    test "serializes schema struct fields" do
      now = DateTime.utc_now() |> DateTime.truncate(:second)

      user = %FerricstoreEcto.Test.User{
        id: 1,
        name: "alice",
        email: "alice@example.com",
        age: 30,
        active: true,
        inserted_at: now,
        updated_at: now
      }

      fields = FieldCodec.serialize_fields(user)

      # PK (id) should NOT be in the serialized fields
      refute Map.has_key?(fields, "id")

      assert fields["name"] == "alice"
      assert fields["email"] == "alice@example.com"
      assert fields["age"] == "30"
      assert fields["active"] == "true"
      assert is_binary(fields["inserted_at"])
      assert is_binary(fields["updated_at"])
    end

    test "serializes nil fields with sentinel" do
      user = %FerricstoreEcto.Test.User{
        id: 1,
        name: nil,
        email: nil,
        age: nil,
        active: true,
        inserted_at: nil,
        updated_at: nil
      }

      fields = FieldCodec.serialize_fields(user)
      assert fields["name"] == "\0"
      assert fields["email"] == "\0"
      assert fields["age"] == "\0"
    end
  end

  describe "deserialize_fields/2" do
    test "deserializes hash map to field values" do
      now = DateTime.utc_now() |> DateTime.truncate(:second)
      now_str = DateTime.to_iso8601(now)

      hash_map = %{
        "name" => "bob",
        "email" => "bob@example.com",
        "age" => "25",
        "active" => "false",
        "inserted_at" => now_str,
        "updated_at" => now_str
      }

      result = FieldCodec.deserialize_fields(FerricstoreEcto.Test.User, hash_map)

      assert result.name == "bob"
      assert result.email == "bob@example.com"
      assert result.age == 25
      assert result.active == false
      assert DateTime.compare(result.inserted_at, now) == :eq
    end

    test "missing fields get nil" do
      result = FieldCodec.deserialize_fields(FerricstoreEcto.Test.User, %{})

      assert result.name == nil
      assert result.email == nil
      assert result.age == nil
    end

    test "nil sentinel fields decode to nil" do
      hash_map = %{
        "name" => "\0",
        "email" => "\0",
        "age" => "\0",
        "active" => "true",
        "inserted_at" => "\0",
        "updated_at" => "\0"
      }

      result = FieldCodec.deserialize_fields(FerricstoreEcto.Test.User, hash_map)
      assert result.name == nil
      assert result.email == nil
      assert result.age == nil
    end
  end
end
