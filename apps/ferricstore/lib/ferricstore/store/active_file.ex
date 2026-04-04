defmodule Ferricstore.Store.ActiveFile do
  @moduledoc """
  Tracks the active log file for each shard.

  Uses atomics generation counter + process dictionary cache — ~15ns reads
  on the hot path. On file rotation, only an ETS re-read (~100ns) happens
  per caller process. No global GC from persistent_term.put, which matters
  when the host app has 50K+ LiveView/Channel processes.

  ## Usage

      # In Shard init and rotation:
      ActiveFile.publish(shard_index, file_id, file_path, shard_data_path)

      # In Router's async write path (hot path):
      {file_id, file_path, shard_data_path} = ActiveFile.get(shard_index)
  """

  @table :ferricstore_active_files
  @atomics_key :ferricstore_active_file_gen

  @doc """
  Initializes the registry. Called once from Application.start.
  """
  @spec init(non_neg_integer()) :: :ok
  def init(_shard_count) do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    end

    unless :persistent_term.get(@atomics_key, nil) do
      ref = :atomics.new(1, signed: false)
      :persistent_term.put(@atomics_key, ref)
    end

    :ok
  end

  @doc """
  Publishes the active file metadata for a shard.

  Called from `Shard.init/1` and `Shard.maybe_rotate_file/1`.
  """
  @spec publish(non_neg_integer(), non_neg_integer(), binary(), binary()) :: :ok
  def publish(shard_index, file_id, file_path, shard_data_path) do
    :ets.insert(@table, {shard_index, file_id, file_path, shard_data_path})
    ref = :persistent_term.get(@atomics_key)
    :atomics.add(ref, 1, 1)
    :ok
  end

  @doc """
  Returns `{file_id, file_path, shard_data_path}` for the given shard.

  ~15ns hot path (atomics check + process dictionary cache hit).
  ~100ns cold (ETS lookup on generation mismatch).
  """
  @spec get(non_neg_integer()) :: {non_neg_integer(), binary(), binary()}
  def get(shard_index) do
    ref = :persistent_term.get(@atomics_key)
    current_gen = :atomics.get(ref, 1)

    case Process.get({:active_file_cache, shard_index}) do
      {^current_gen, file_id, file_path, shard_data_path} ->
        {file_id, file_path, shard_data_path}

      _ ->
        [{^shard_index, file_id, file_path, shard_data_path}] =
          :ets.lookup(@table, shard_index)

        Process.put({:active_file_cache, shard_index}, {current_gen, file_id, file_path, shard_data_path})
        {file_id, file_path, shard_data_path}
    end
  end
end
