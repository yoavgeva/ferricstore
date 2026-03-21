# FerricStore NIF Catalog

Complete catalog of every NIF function exposed to Elixir, with classification
of type, current scheduler optimization, recommended optimization, and whether
a benchmark is needed.

## Classification Key

- **Type**: `tiny` (<1us, single item, no IO), `cpu_bulk` (processes list/batch in CPU), `io_bound` (disk read/write), `cpu_heavy` (graph traversal, sort, merge), `io_maint` (maintenance IO -- infrequent)
- **Current**: `none` (plain NIF on Normal scheduler), `yield` (consume_timeslice + Reschedule), `tokio` (async via Tokio runtime), `uring` (io_uring async submit)
- **Should Have**: recommended optimization based on workload type

## Bitcask Core (lib.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| new | lib.rs | io_maint | none | none | yes |
| get | lib.rs | io_bound | none | none | yes |
| get_zero_copy | lib.rs | io_bound | none | none | yes |
| get_file_ref | lib.rs | tiny | none | none | yes |
| put | lib.rs | io_bound | none | none | yes |
| delete | lib.rs | io_bound | none | none | yes |
| put_batch | lib.rs | io_bound | none | none | yes |
| put_batch_async | lib.rs | io_bound | uring | uring | yes |
| keys | lib.rs | cpu_bulk | yield | yield | yes |
| get_all | lib.rs | cpu_bulk+io | yield | yield | yes |
| get_batch | lib.rs | cpu_bulk+io | yield | yield | yes |
| get_range | lib.rs | cpu_bulk+io | yield | yield | yes |
| get_all_zero_copy | lib.rs | cpu_bulk+io | none | yield | yes |
| get_batch_zero_copy | lib.rs | cpu_bulk+io | none | yield | yes |
| get_range_zero_copy | lib.rs | cpu_bulk+io | none | yield | yes |
| read_modify_write | lib.rs | io_bound | none | none | yes |
| write_hint | lib.rs | io_maint | none | none | yes |
| purge_expired | lib.rs | io_maint | none | none | yes |
| shard_stats | lib.rs | tiny | none | none | yes |
| file_sizes | lib.rs | tiny | none | none | yes |
| run_compaction | lib.rs | io_maint | none | none | yes |
| available_disk_space | lib.rs | tiny | none | none | yes |

## Tokio Async IO NIFs (lib.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| get_async | lib.rs | io_bound | tokio | tokio | yes |
| delete_async | lib.rs | io_bound | tokio | tokio | yes |
| put_batch_tokio_async | lib.rs | io_bound | tokio | tokio | yes |
| write_hint_async | lib.rs | io_maint | tokio | tokio | yes |
| purge_expired_async | lib.rs | io_maint | tokio | tokio | yes |
| run_compaction_async | lib.rs | io_maint | tokio | tokio | yes |

## Bloom Filter (bloom.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| bloom_create | bloom.rs | io_bound | none | none | yes |
| bloom_open | bloom.rs | io_bound | none | none | yes |
| bloom_add | bloom.rs | tiny | none | none | yes |
| bloom_madd | bloom.rs | cpu_bulk | none | yield | yes |
| bloom_exists | bloom.rs | tiny | none | none | yes |
| bloom_mexists | bloom.rs | cpu_bulk | none | yield | yes |
| bloom_card | bloom.rs | tiny | none | none | yes |
| bloom_info | bloom.rs | tiny | none | none | yes |
| bloom_delete | bloom.rs | tiny | none | none | yes |

## Cuckoo Filter (cuckoo.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| cuckoo_create | cuckoo.rs | tiny | none | none | yes |
| cuckoo_add | cuckoo.rs | tiny | none | none | yes |
| cuckoo_addnx | cuckoo.rs | tiny | none | none | yes |
| cuckoo_del | cuckoo.rs | tiny | none | none | yes |
| cuckoo_exists | cuckoo.rs | tiny | none | none | yes |
| cuckoo_mexists | cuckoo.rs | cpu_bulk | none | yield | yes |
| cuckoo_count | cuckoo.rs | tiny | none | none | yes |
| cuckoo_info | cuckoo.rs | tiny | none | none | yes |
| cuckoo_serialize | cuckoo.rs | cpu_bulk | none | none | yes |
| cuckoo_deserialize | cuckoo.rs | cpu_bulk | none | none | yes |

## Count-Min Sketch (cms.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| cms_create | cms.rs | tiny | none | none | yes |
| cms_incrby | cms.rs | cpu_bulk | none | yield | yes |
| cms_query | cms.rs | cpu_bulk | none | yield | yes |
| cms_merge | cms.rs | cpu_heavy | none | yield | yes |
| cms_info | cms.rs | tiny | none | none | yes |
| cms_to_bytes | cms.rs | cpu_bulk | none | none | yes |
| cms_from_bytes | cms.rs | cpu_bulk | none | none | yes |

## Top-K (topk.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| topk_create | topk.rs | tiny | none | none | yes |
| topk_add | topk.rs | cpu_bulk | none | yield | yes |
| topk_incrby | topk.rs | cpu_bulk | none | yield | yes |
| topk_query | topk.rs | cpu_bulk | none | yield | yes |
| topk_list | topk.rs | tiny | none | none | yes |
| topk_list_with_count | topk.rs | tiny | none | none | yes |
| topk_info | topk.rs | tiny | none | none | yes |
| topk_to_bytes | topk.rs | cpu_bulk | none | none | yes |
| topk_from_bytes | topk.rs | cpu_bulk | none | none | yes |

## T-Digest (tdigest.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| tdigest_create | tdigest.rs | tiny | none | none | yes |
| tdigest_add | tdigest.rs | cpu_bulk | none | yield | yes |
| tdigest_quantile | tdigest.rs | tiny | none | none | yes |
| tdigest_cdf | tdigest.rs | tiny | none | none | yes |
| tdigest_trimmed_mean | tdigest.rs | tiny | none | none | yes |
| tdigest_min | tdigest.rs | tiny | none | none | yes |
| tdigest_max | tdigest.rs | tiny | none | none | yes |
| tdigest_info | tdigest.rs | tiny | none | none | yes |
| tdigest_rank | tdigest.rs | cpu_bulk | none | yield | yes |
| tdigest_revrank | tdigest.rs | cpu_bulk | none | yield | yes |
| tdigest_byrank | tdigest.rs | cpu_bulk | none | yield | yes |
| tdigest_byrevrank | tdigest.rs | cpu_bulk | none | yield | yes |
| tdigest_merge | tdigest.rs | cpu_heavy | none | yield | yes |
| tdigest_reset | tdigest.rs | tiny | none | none | yes |
| tdigest_serialize | tdigest.rs | cpu_bulk | none | none | yes |
| tdigest_deserialize | tdigest.rs | cpu_bulk | none | none | yes |

## HNSW Vector Search (hnsw.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| hnsw_new | hnsw.rs | tiny | none | none | yes |
| hnsw_add | hnsw.rs | cpu_heavy | none | yield | yes |
| hnsw_delete | hnsw.rs | tiny | none | none | yes |
| hnsw_search | hnsw.rs | cpu_heavy | none | yield | yes |
| hnsw_count | hnsw.rs | tiny | none | none | yes |
| vsearch_nif | hnsw.rs | cpu_heavy | yield | yield | yes |

## Tracking Allocator (tracking_alloc.rs)

| NIF | Module | Type | Current | Should Have | Benchmark? |
|-----|--------|------|---------|-------------|------------|
| rust_allocated_bytes | tracking_alloc.rs | tiny | none | none | yes |

## Summary

**Total NIFs: 76**

| Category | Count | Examples |
|----------|-------|---------|
| tiny (<1us) | 26 | rust_allocated_bytes, bloom_card, cuckoo_exists, shard_stats |
| cpu_bulk (list/batch) | 25 | bloom_madd, cms_incrby, keys, get_batch, topk_add |
| io_bound (disk IO) | 12 | get, put, delete, put_batch, get_async |
| cpu_heavy (graph/sort) | 5 | hnsw_search, vsearch_nif, cms_merge, tdigest_merge, hnsw_add |
| io_maint (infrequent) | 8 | new, write_hint, purge_expired, run_compaction |

| Optimization | Count |
|-------------|-------|
| none (plain Normal scheduler) | 63 |
| yield (consume_timeslice + Reschedule) | 5 (keys, get_all, get_batch, get_range, vsearch_nif) |
| tokio (async via Tokio runtime) | 6 (get_async, delete_async, put_batch_tokio_async, write_hint_async, purge_expired_async, run_compaction_async) |
| uring (io_uring async submit) | 1 (put_batch_async) |
| **should add yield** | ~18 (bloom_madd, bloom_mexists, cuckoo_mexists, cms_incrby, cms_query, cms_merge, topk_add, topk_incrby, topk_query, tdigest_add, tdigest_rank/revrank/byrank/byrevrank, tdigest_merge, hnsw_add, hnsw_search, get_all_zero_copy, get_batch_zero_copy, get_range_zero_copy) |
