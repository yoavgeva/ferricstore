defmodule Ferricstore.Bitcask.NIF do
  @moduledoc "Rustler NIF bindings for Bitcask record I/O, hint files, and mmap-backed probabilistic data structure file operations."

  use Rustler, otp_app: :ferricstore, crate: "ferricstore_bitcask", skip_compilation?: true

  # -- Tracking allocator --
  @spec rust_allocated_bytes() :: {:ok, non_neg_integer()} | {:error, term()}
  def rust_allocated_bytes, do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Pure stateless NIFs (no Store resource, no Mutex) --
  @spec v2_append_record(binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def v2_append_record(_path, _key, _value, _expire_at_ms), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_tombstone(binary(), binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def v2_append_tombstone(_path, _key), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_batch(binary(), [{binary(), binary(), non_neg_integer()}]) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def v2_append_batch(_path, _records), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_at(binary(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def v2_pread_at(_path, _offset), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_file(binary()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}]}
          | {:error, term()}
  def v2_scan_file(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch(binary(), [{non_neg_integer(), non_neg_integer()}]) :: {:ok, [binary()]} | {:error, term()}
  def v2_pread_batch(_path, _locations), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_fsync(binary()) :: :ok | {:error, term()}
  def v2_fsync(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_write_hint_file(
          binary(),
          [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}]
        ) :: :ok | {:error, term()}
  def v2_write_hint_file(_path, _entries), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_read_hint_file(binary()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}]}
          | {:error, term()}
  def v2_read_hint_file(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_copy_records(binary(), binary(), [non_neg_integer()]) :: :ok | {:error, term()}
  def v2_copy_records(_source_path, _dest_path, _offsets), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_batch_nosync(binary(), [{binary(), binary(), non_neg_integer()}]) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def v2_append_batch_nosync(_path, _records), do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Tokio async IO NIFs --
  @spec v2_pread_at_async(pid(), term(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def v2_pread_at_async(_caller_pid, _correlation_id, _path, _offset), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch_async(pid(), term(), [{binary(), non_neg_integer()}]) :: :ok | {:error, term()}
  def v2_pread_batch_async(_caller_pid, _correlation_id, _locations), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_fsync_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def v2_fsync_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_batch_async(pid(), term(), binary(), [{binary(), binary(), non_neg_integer()}]) ::
          :ok | {:error, term()}
  def v2_append_batch_async(_caller_pid, _correlation_id, _path, _records), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite Bloom NIFs --
  @spec bloom_file_create(binary(), non_neg_integer(), non_neg_integer()) :: :ok | {:error, term()}
  def bloom_file_create(_path, _num_bits, _num_hashes), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_add(binary(), binary()) :: :ok | {:error, term()}
  def bloom_file_add(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_madd(binary(), [binary()]) :: :ok | {:error, term()}
  def bloom_file_madd(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_exists(binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def bloom_file_exists(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_mexists(binary(), [binary()]) :: {:ok, [boolean()]} | {:error, term()}
  def bloom_file_mexists(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_card(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def bloom_file_card(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_info(binary()) :: {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def bloom_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async Bloom read NIFs (Tokio spawn_blocking) --
  @spec bloom_file_exists_async(pid(), term(), binary(), binary()) :: :ok | {:error, term()}
  def bloom_file_exists_async(_caller_pid, _correlation_id, _path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_mexists_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def bloom_file_mexists_async(_caller_pid, _correlation_id, _path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_card_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def bloom_file_card_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_info_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def bloom_file_info_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite CMS NIFs --
  @spec cms_file_create(binary(), non_neg_integer(), non_neg_integer()) :: :ok | {:error, term()}
  def cms_file_create(_path, _width, _depth), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_incrby(binary(), [{binary(), non_neg_integer()}]) :: :ok | {:error, term()}
  def cms_file_incrby(_path, _items), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_query(binary(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def cms_file_query(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_info(binary()) :: {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def cms_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_merge(binary(), [binary()], [number()]) :: :ok | {:error, term()}
  def cms_file_merge(_dst_path, _src_paths, _weights), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async CMS read NIFs (Tokio spawn_blocking) --
  @spec cms_file_query_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def cms_file_query_async(_caller_pid, _correlation_id, _path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_info_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def cms_file_info_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite Cuckoo NIFs --
  @spec cuckoo_file_create(binary(), non_neg_integer(), non_neg_integer()) :: :ok | {:error, term()}
  def cuckoo_file_create(_path, _capacity, _bucket_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_add(binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_add(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_addnx(binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def cuckoo_file_addnx(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_del(binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_del(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_exists(binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def cuckoo_file_exists(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_count(binary(), binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def cuckoo_file_count(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_info(binary()) :: {:ok, tuple()} | {:error, term()}
  def cuckoo_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async Cuckoo read NIFs (Tokio spawn_blocking) --
  @spec cuckoo_file_exists_async(pid(), term(), binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_exists_async(_caller_pid, _correlation_id, _path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_count_async(pid(), term(), binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_count_async(_caller_pid, _correlation_id, _path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_info_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_info_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite TopK v2 NIFs --
  @spec topk_file_create_v2(binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), float()) ::
          :ok | {:error, term()}
  def topk_file_create_v2(_path, _k, _width, _depth, _decay), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_add_v2(binary(), [binary()]) :: {:ok, [binary() | nil]} | {:error, term()}
  def topk_file_add_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_incrby_v2(binary(), [{binary(), non_neg_integer()}]) :: {:ok, [binary() | nil]} | {:error, term()}
  def topk_file_incrby_v2(_path, _pairs), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_query_v2(binary(), [binary()]) :: {:ok, [boolean()]} | {:error, term()}
  def topk_file_query_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_list_v2(binary()) :: {:ok, [{binary(), non_neg_integer()}]} | {:error, term()}
  def topk_file_list_v2(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_count_v2(binary(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def topk_file_count_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_info_v2(binary()) :: {:ok, tuple()} | {:error, term()}
  def topk_file_info_v2(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async TopK v2 read NIFs (Tokio spawn_blocking) --
  @spec topk_file_query_v2_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def topk_file_query_v2_async(_caller_pid, _correlation_id, _path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_list_v2_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def topk_file_list_v2_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_count_v2_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def topk_file_count_v2_async(_caller_pid, _correlation_id, _path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_info_v2_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def topk_file_info_v2_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)
end
