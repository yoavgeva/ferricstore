defmodule :ferricstore_wal_nif do
  @moduledoc """
  Rust NIF WAL I/O module for ra_log_wal.

  Replaces file:write and file:datasync with a Rust background thread
  that handles O_DIRECT, commit_delay batching, and fdatasync.

  All NIF functions run on normal BEAM schedulers (<1μs each).
  The blocking I/O runs on a dedicated Rust OS thread.

  This module is registered as an Erlang atom `:ferricstore_wal_nif`
  so ra_log_wal can call it as IoMod:write(Handle, Data).
  """

  use Rustler, otp_app: :ferricstore, crate: "ferricstore_wal_nif", skip_compilation?: true

  @doc "Open a WAL file. Spawns background I/O thread."
  def open(_path, _commit_delay_us, _pre_allocate_bytes, _max_buffer_bytes),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc "Write pre-formatted iodata to the WAL buffer. Does NOT write to disk."
  def write(_handle, _iodata), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Request async fdatasync. Sends {wal_sync_complete, Ref} on completion."
  def sync(_handle, _caller_pid, _ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Close the WAL file. Blocks until drain + sync + close."
  def close(_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Returns current logical file size in bytes."
  def position(_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Read bytes from WAL at offset. Used during recovery."
  def pread(_handle, _offset, _len), do: :erlang.nif_error(:nif_not_loaded)
end
