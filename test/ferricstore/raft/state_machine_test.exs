defmodule Ferricstore.Raft.StateMachineTest do
  @moduledoc """
  Unit tests for `Ferricstore.Raft.StateMachine`.

  These tests exercise the state machine callbacks directly without running
  a full ra server. The state machine is deterministic and its callbacks can
  be tested in isolation by constructing state manually.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.StateMachine

  # ---------------------------------------------------------------------------
  # Setup: create a temporary Bitcask store and ETS table for each test
  # ---------------------------------------------------------------------------

  setup do
    dir = Path.join(System.tmp_dir!(), "sm_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)

    {:ok, store} = NIF.new(dir)
    ets_name = :"sm_test_ets_#{:rand.uniform(9_999_999)}"
    :ets.new(ets_name, [:set, :public, :named_table])

    state = StateMachine.init(%{shard_index: 0, store: store, ets: ets_name})

    on_exit(fn ->
      try do
        :ets.delete(ets_name)
      rescue
        ArgumentError -> :ok
      end

      File.rm_rf!(dir)
    end)

    %{state: state, ets: ets_name, store: store, dir: dir}
  end

  # ---------------------------------------------------------------------------
  # init/1
  # ---------------------------------------------------------------------------

  describe "init/1" do
    test "creates initial state with expected fields", %{state: state} do
      assert state.shard_index == 0
      assert is_reference(state.store)
      assert is_atom(state.ets)
      assert state.applied_count == 0
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :put
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:put, key, value, expire_at_ms}" do
    test "writes value to Bitcask and ETS", %{state: state, ets: ets, store: store} do
      {new_state, result} =
        StateMachine.apply(%{}, {:put, "key1", "value1", 0}, state)

      assert result == :ok
      assert new_state.applied_count == 1

      # Verify ETS
      assert [{_, "value1", 0}] = :ets.lookup(ets, "key1")

      # Verify Bitcask
      assert {:ok, "value1"} = NIF.get(store, "key1")
    end

    test "put with expiry stores expire_at_ms", %{state: state, ets: ets} do
      future = System.os_time(:millisecond) + 60_000

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "expiring", "val", future}, state)

      assert result == :ok
      assert [{_, "val", ^future}] = :ets.lookup(ets, "expiring")
    end

    test "put overwrites previous value", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "k", "v1", 0}, state)
      {state3, :ok} = StateMachine.apply(%{}, {:put, "k", "v2", 0}, state2)

      assert state3.applied_count == 2
      assert [{_, "v2", 0}] = :ets.lookup(ets, "k")
    end

    test "increments applied_count on each put", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert s3.applied_count == 3
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :delete
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:delete, key}" do
    test "removes key from ETS", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "del_me", "val", 0}, state)
      {state3, :ok} = StateMachine.apply(%{}, {:delete, "del_me"}, state2)

      assert state3.applied_count == 2
      assert [] == :ets.lookup(ets, "del_me")
    end

    test "delete nonexistent key returns :ok", %{state: state} do
      {_new_state, result} = StateMachine.apply(%{}, {:delete, "nonexistent"}, state)
      assert result == :ok
    end

    test "delete after delete is idempotent", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "k", "v", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:delete, "k"}, s1)
      {_s3, :ok} = StateMachine.apply(%{}, {:delete, "k"}, s2)
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :batch
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:batch, commands}" do
    test "processes all commands and returns results list", %{state: state, ets: ets} do
      commands = [
        {:put, "batch_a", "val_a", 0},
        {:put, "batch_b", "val_b", 0},
        {:put, "batch_c", "val_c", 0}
      ]

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, commands}, state)

      assert results == [:ok, :ok, :ok]
      assert new_state.applied_count == 3

      # All keys in ETS
      assert [{_, "val_a", 0}] = :ets.lookup(ets, "batch_a")
      assert [{_, "val_b", 0}] = :ets.lookup(ets, "batch_b")
      assert [{_, "val_c", 0}] = :ets.lookup(ets, "batch_c")
    end

    test "mixed put and delete batch", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "mix_a", "va", 0}, state)

      commands = [
        {:put, "mix_b", "vb", 0},
        {:delete, "mix_a"},
        {:put, "mix_c", "vc", 0}
      ]

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, commands}, state2)

      assert results == [:ok, :ok, :ok]
      assert new_state.applied_count == 4

      assert [] == :ets.lookup(ets, "mix_a")
      assert [{_, "vb", 0}] = :ets.lookup(ets, "mix_b")
      assert [{_, "vc", 0}] = :ets.lookup(ets, "mix_c")
    end

    test "empty batch returns empty results", %{state: state} do
      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, []}, state)

      assert results == []
      assert new_state.applied_count == 0
    end

    test "large batch (100 commands)", %{state: state} do
      commands = for i <- 1..100, do: {:put, "batch_k#{i}", "batch_v#{i}", 0}

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, commands}, state)

      assert length(results) == 100
      assert Enum.all?(results, &(&1 == :ok))
      assert new_state.applied_count == 100
    end
  end

  # ---------------------------------------------------------------------------
  # state_enter/2
  # ---------------------------------------------------------------------------

  describe "state_enter/2" do
    test "returns empty effects for all roles", %{state: state} do
      assert StateMachine.state_enter(:leader, state) == []
      assert StateMachine.state_enter(:follower, state) == []
      assert StateMachine.state_enter(:candidate, state) == []
      assert StateMachine.state_enter(:await_condition, state) == []
      assert StateMachine.state_enter(:delete_and_terminate, state) == []
      assert StateMachine.state_enter(:receive_snapshot, state) == []
    end
  end

  # ---------------------------------------------------------------------------
  # tick/2
  # ---------------------------------------------------------------------------

  describe "tick/2" do
    test "returns empty effects", %{state: state} do
      assert StateMachine.tick(System.os_time(:millisecond), state) == []
    end
  end

  # ---------------------------------------------------------------------------
  # init_aux/1
  # ---------------------------------------------------------------------------

  describe "init_aux/1" do
    test "returns initial aux state with empty hot_keys" do
      aux = StateMachine.init_aux(:test_name)
      assert aux == %{hot_keys: %{}}
    end
  end

  # ---------------------------------------------------------------------------
  # handle_aux/5
  # ---------------------------------------------------------------------------

  describe "handle_aux/5" do
    test "key_written increments hot key counter" do
      aux = %{hot_keys: %{}}
      int_state = %{some: :internal_state}

      {:no_reply, new_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 1
      assert returned_state == int_state
    end

    test "key_written accumulates counts" do
      aux = %{hot_keys: %{"hot_key" => 5}}
      int_state = %{}

      {:no_reply, new_aux, _} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 6
    end

    test "unknown command returns aux unchanged" do
      aux = %{hot_keys: %{}}
      int_state = %{}

      {:no_reply, returned_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, :unknown, aux, int_state)

      assert returned_aux == aux
      assert returned_state == int_state
    end
  end

  # ---------------------------------------------------------------------------
  # overview/1
  # ---------------------------------------------------------------------------

  describe "overview/1" do
    test "returns shard_index, keydir_size, and applied_count", %{state: state} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "ov_k", "ov_v", 0}, state)

      overview = StateMachine.overview(state2)
      assert overview.shard_index == 0
      assert overview.keydir_size == 1
      assert overview.applied_count == 1
    end

    test "keydir_size reflects ETS size", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert StateMachine.overview(s3).keydir_size == 3
    end
  end
end
