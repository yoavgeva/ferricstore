defmodule Ferricstore.Bitcask.NIF do
  @moduledoc false

  use Rustler, otp_app: :ferricstore, crate: "ferricstore_bitcask", skip_compilation?: true

  # Each function body is the fallback if the NIF fails to load.
  def new(_path), do: :erlang.nif_error(:nif_not_loaded)
  def get(_store, _key), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Zero-copy GET: returns a BEAM binary backed by a Rust resource,
  avoiding the memcpy that `get/2` performs.

  Same return shape as `get/2`: `{:ok, binary}`, `{:ok, nil}`, or
  `{:error, reason}`.
  """
  def get_zero_copy(_store, _key), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Look up a key's on-disk file location without reading the value.

  Returns `{:ok, {file_path, value_offset, value_size}}` if the key exists,
  `{:ok, nil}` if not found/expired, or `{:error, reason}`.

  Used by the sendfile optimisation: the caller can use `:file.sendfile/5`
  to zero-copy the value bytes from the Bitcask data file to a TCP socket.
  """
  def get_file_ref(_store, _key), do: :erlang.nif_error(:nif_not_loaded)

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

  # Extended NIF functions

  @doc "Return all live key-value pairs. Returns {:ok, [{key, value}, ...]}."
  def get_all(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Lookup multiple keys at once. Returns {:ok, [value | nil, ...]}."
  def get_batch(_store, _keys), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Range scan: returns sorted key-value pairs in [min, max], up to count."
  def get_range(_store, _min_key, _max_key, _max_count), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Atomic read-modify-write. Returns {:ok, new_value} or {:error, reason}."
  def read_modify_write(_store, _key, _operation), do: :erlang.nif_error(:nif_not_loaded)

  # Zero-copy bulk NIF functions

  @doc "Zero-copy GET ALL. Same return shape as get_all/1."
  def get_all_zero_copy(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Zero-copy GET BATCH. Same return shape as get_batch/2."
  def get_batch_zero_copy(_store, _keys), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Zero-copy GET RANGE. Same return shape as get_range/4."
  def get_range_zero_copy(_store, _min_key, _max_key, _max_count),
    do: :erlang.nif_error(:nif_not_loaded)

  # Merge NIF functions

  @doc "Returns shard stats: {:ok, {total, live, dead, file_count, key_count, frag_ratio}}."
  def shard_stats(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Returns file sizes: {:ok, [{file_id, size}, ...]}."
  def file_sizes(_store), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Run compaction on file_ids. Returns {:ok, {written, dropped, reclaimed}}."
  def run_compaction(_store, _file_ids), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Returns available disk space in bytes: {:ok, bytes}."
  def available_disk_space(_store), do: :erlang.nif_error(:nif_not_loaded)
end
