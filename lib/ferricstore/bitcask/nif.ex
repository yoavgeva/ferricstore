defmodule Ferricstore.Bitcask.NIF do
  @moduledoc false

  use Rustler, otp_app: :ferricstore, crate: "ferricstore_bitcask"

  # Each function body is the fallback if the NIF fails to load.
  def new(_path), do: :erlang.nif_error(:nif_not_loaded)
  def get(_store, _key), do: :erlang.nif_error(:nif_not_loaded)
  def put(_store, _key, _value, _expire_at_ms), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_store, _key), do: :erlang.nif_error(:nif_not_loaded)
  def put_batch(_store, _batch), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Async variant of `put_batch/2`.

  On Linux with io_uring: submits writes + fsync to the io_uring ring and
  returns `{:pending, op_id}` immediately. The calling process will receive
  `{:io_complete, op_id, :ok | {:error, reason}}` when the fsync completes.

  On macOS or when io_uring is unavailable: falls back to synchronous
  `put_batch`, returning `:ok` directly (no pending token, no message).
  """
  def put_batch_async(_store, _batch), do: :erlang.nif_error(:nif_not_loaded)

  def keys(_store), do: :erlang.nif_error(:nif_not_loaded)
  def write_hint(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Purge all expired keys, writing tombstones. Returns {:ok, count}."
  def purge_expired(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Returns shard statistics for the merge scheduler.

  Returns `{:ok, {total_disk_bytes, live_bytes, dead_bytes, file_count, key_count, fragmentation_ratio}}`.
  """
  def shard_stats(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Returns `{:ok, [{file_id, size_bytes}, ...]}` for all log files, sorted by file_id.
  """
  def file_sizes(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Runs compaction on the specified file_ids, merging them into a single new file.

  Returns `{:ok, {records_written, records_dropped, bytes_reclaimed}}` or `{:error, reason}`.
  """
  def run_compaction(_store, _file_ids), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Returns `{:ok, available_bytes}` for the filesystem containing the store's data directory.
  """
  def available_disk_space(_store), do: :erlang.nif_error(:nif_not_loaded)
end
