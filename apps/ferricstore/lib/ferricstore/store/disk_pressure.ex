defmodule Ferricstore.Store.DiskPressure do
  @moduledoc """
  Per-shard atomic disk pressure flags.

  When a Bitcask flush fails (e.g., ENOSPC), the shard sets its disk pressure
  flag. The async write path in Router checks this flag before accepting writes,
  returning an error instead of silently queuing data that can't be persisted.

  The flag is cleared when a flush succeeds, allowing writes to resume
  automatically once disk space is available (e.g., after compaction reclaims
  space or an operator adds storage).

  Uses `:atomics` for lock-free ~5ns reads from any process.
  """

  @pt_key :ferricstore_disk_pressure

  @spec init(pos_integer()) :: :ok
  def init(shard_count) do
    ref = :atomics.new(shard_count, signed: false)
    :persistent_term.put(@pt_key, ref)
    :ok
  end

  @spec set(non_neg_integer()) :: :ok
  def set(shard_index) do
    ref = :persistent_term.get(@pt_key)
    :atomics.put(ref, shard_index + 1, 1)
    :ok
  end

  @spec clear(non_neg_integer()) :: :ok
  def clear(shard_index) do
    ref = :persistent_term.get(@pt_key)
    :atomics.put(ref, shard_index + 1, 0)
    :ok
  end

  @spec under_pressure?(non_neg_integer()) :: boolean()
  def under_pressure?(shard_index) do
    ref = :persistent_term.get(@pt_key)
    :atomics.get(ref, shard_index + 1) == 1
  end
end
