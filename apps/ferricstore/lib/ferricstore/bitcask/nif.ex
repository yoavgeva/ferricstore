defmodule Ferricstore.Bitcask.NIF do
  @moduledoc false

  use Rustler, otp_app: :ferricstore, crate: "ferricstore_bitcask", skip_compilation?: true

  # -- Tracking allocator --
  def rust_allocated_bytes, do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Pure stateless NIFs (no Store resource, no Mutex) --
  def v2_append_record(_path, _key, _value, _expire_at_ms), do: :erlang.nif_error(:nif_not_loaded)
  def v2_append_tombstone(_path, _key), do: :erlang.nif_error(:nif_not_loaded)
  def v2_append_batch(_path, _records), do: :erlang.nif_error(:nif_not_loaded)
  def v2_pread_at(_path, _offset), do: :erlang.nif_error(:nif_not_loaded)
  def v2_scan_file(_path), do: :erlang.nif_error(:nif_not_loaded)
  def v2_pread_batch(_path, _locations), do: :erlang.nif_error(:nif_not_loaded)
  def v2_fsync(_path), do: :erlang.nif_error(:nif_not_loaded)
  def v2_write_hint_file(_path, _entries), do: :erlang.nif_error(:nif_not_loaded)
  def v2_read_hint_file(_path), do: :erlang.nif_error(:nif_not_loaded)
  def v2_copy_records(_source_path, _dest_path, _offsets), do: :erlang.nif_error(:nif_not_loaded)
  def v2_append_batch_nosync(_path, _records), do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Tokio async IO NIFs --
  def v2_pread_at_async(_caller_pid, _correlation_id, _path, _offset), do: :erlang.nif_error(:nif_not_loaded)
  def v2_pread_batch_async(_caller_pid, _correlation_id, _locations), do: :erlang.nif_error(:nif_not_loaded)
  def v2_fsync_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)
  def v2_append_batch_async(_caller_pid, _correlation_id, _path, _records), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite Bloom NIFs --
  def bloom_file_create(_path, _num_bits, _num_hashes), do: :erlang.nif_error(:nif_not_loaded)
  def bloom_file_add(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def bloom_file_madd(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)
  def bloom_file_exists(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def bloom_file_mexists(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)
  def bloom_file_card(_path), do: :erlang.nif_error(:nif_not_loaded)
  def bloom_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite CMS NIFs --
  def cms_file_create(_path, _width, _depth), do: :erlang.nif_error(:nif_not_loaded)
  def cms_file_incrby(_path, _items), do: :erlang.nif_error(:nif_not_loaded)
  def cms_file_query(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)
  def cms_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)
  def cms_file_merge(_dst_path, _src_paths, _weights), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite Cuckoo NIFs --
  def cuckoo_file_create(_path, _capacity, _bucket_size), do: :erlang.nif_error(:nif_not_loaded)
  def cuckoo_file_add(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def cuckoo_file_addnx(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def cuckoo_file_del(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def cuckoo_file_exists(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def cuckoo_file_count(_path, _element), do: :erlang.nif_error(:nif_not_loaded)
  def cuckoo_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite TopK v2 NIFs --
  def topk_file_create_v2(_path, _k, _width, _depth, _decay), do: :erlang.nif_error(:nif_not_loaded)
  def topk_file_add_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)
  def topk_file_incrby_v2(_path, _pairs), do: :erlang.nif_error(:nif_not_loaded)
  def topk_file_query_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)
  def topk_file_list_v2(_path), do: :erlang.nif_error(:nif_not_loaded)
  def topk_file_count_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)
  def topk_file_info_v2(_path), do: :erlang.nif_error(:nif_not_loaded)
end
