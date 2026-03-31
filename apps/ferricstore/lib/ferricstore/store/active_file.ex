defmodule Ferricstore.Store.ActiveFile do
  @moduledoc """
  Tracks the active log file for each shard without `persistent_term.put`.

  ## Problem

  The previous approach stored `{file_id, file_path, shard_data_path}` in
  `persistent_term`. This gives ~5ns reads but every `persistent_term.put`
  (on file rotation) triggers a **global GC across all BEAM processes**.
  In embedded mode, a host app with 50K LiveView/Channel processes would
  experience GC latency spikes on every file rotation.

  ## Solution: single atomics counter + process dictionary cache

  1. A single `:atomics` counter (1 integer) is bumped on any shard's file
     rotation. Reading it costs ~5ns.
  2. An ETS table holds `{shard_index, file_id, file_path, shard_data_path}`.
  3. Callers cache the ETS value in their **process dictionary** keyed by
     `{:active_file_cache, shard_index}` along with the generation at read time.
  4. On read: compare cached generation to current atomic — if equal, use
     cache (~15ns total). If stale, re-read ETS (~100ns), update cache.
  5. On rotation: Shard updates ETS + bumps the global atomic. No global GC.

  Using a global counter instead of per-shard means a rotation on shard 0
  invalidates caches for all shards. This causes at most `shard_count` extra
  ETS reads (one per shard, one per caller process). Since rotation happens
  ~once per 25 seconds, this false invalidation is negligible.

  ## Usage

      # In Shard init and rotation:
      ActiveFile.publish(shard_index, file_id, file_path, shard_data_path)

      # In Router's async write path (hot path):
      {file_id, file_path, shard_data_path} = ActiveFile.get(shard_index)
  """

  @table :ferricstore_active_files
  @atomics_key :ferricstore_active_file_gen

  @doc """
  Initializes the ETS table and atomics counter. Called once from Application.start.
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

  Called from `Shard.init/1` and `Shard.maybe_rotate_file/1`. Updates the
  ETS table and bumps the global generation counter so callers invalidate
  their process dictionary cache on next read.
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

  Uses a two-level cache:
  1. Process dictionary (L1, ~10ns) — checked first via generation match
  2. ETS table (L2, ~100ns) — on cache miss or generation mismatch

  The global atomics counter (~5ns read) detects staleness. After the first
  ETS read, subsequent calls for the same shard hit the process dictionary
  until any shard rotates its file.
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
