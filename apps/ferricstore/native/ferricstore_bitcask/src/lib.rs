#![deny(clippy::all, clippy::pedantic)]
// Yielding NIFs require `unsafe` for raw extern "C" continuation functions
// and direct use of `NifReturned::Reschedule` / `enif_schedule_nif`.
#![allow(unsafe_code)]
// These pedantic lints are noisy without adding safety value for this codebase:
// - possible_truncation: we target 64-bit Linux only; u64→usize is always safe.
// - cast_sign_loss / cast_lossless: io_uring result codes require these casts.
// - items_after_statements: common in test helpers and is clear.
// - doc_markdown: minor style preference, not a correctness issue.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
// Yielding NIF continuations use raw pointers and similar variable names:
#![allow(clippy::ptr_as_ptr)]
#![allow(clippy::similar_names)]
#![allow(clippy::ref_option)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::doc_link_with_quotes)]

pub mod async_io;
pub mod bloom;
pub mod cms;
pub mod compaction;
pub mod cuckoo;
pub mod hint;
pub mod hnsw;
pub mod io_backend;
pub mod keydir;
pub mod log;
pub mod store;
pub mod tdigest;
pub mod topk;
pub mod tracking_alloc;

use rustler::schedule::consume_timeslice;
use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term};
use std::sync::Mutex;

use std::sync::atomic::AtomicU64;

#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;

use store::Store;

/// A resource that owns a value buffer read from the Bitcask log.
///
/// When used with `ResourceArc::make_binary`, the BEAM creates a binary term
/// that points directly into this buffer — zero copy from Rust to BEAM.
/// The BEAM's GC tracks the reference: once the Erlang binary term becomes
/// unreachable, the `ResourceArc` ref-count drops to zero and this `Vec` is
/// freed.
///
/// ## Safety invariant
///
/// The `data` field MUST NOT be mutated after the `ResourceArc<ValueBuffer>`
/// is passed to `make_binary`. The returned BEAM binary shares the same
/// backing memory; any mutation would violate the immutability guarantee of
/// Erlang binaries and cause undefined behaviour.
struct ValueBuffer {
    data: Vec<u8>,
}

/// A resource that owns a collection of key-value pairs from a bulk read.
///
/// When used with `ResourceArc::make_binary`, the BEAM creates binary terms
/// that point directly into this buffer's `Vec<u8>` allocations - zero copy.
/// The BEAM's GC tracks the reference: as long as any binary term created from
/// this buffer is reachable, the `ResourceArc` ref-count stays above zero and
/// the backing `Vec`s are not freed.
///
/// ## Safety invariant
///
/// The `pairs` field MUST NOT be mutated after any `ResourceArc::make_binary`
/// call. The returned BEAM binaries share the same backing memory; any mutation
/// would violate the immutability guarantee of Erlang binaries and cause
/// undefined behaviour.
struct BulkKvBuffer {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A resource that owns a collection of optional value buffers from a batch
/// lookup. Similar to `BulkKvBuffer` but for `get_batch` which returns
/// `Option<Vec<u8>>` per key (missing keys map to `None`/`:nil`).
///
/// ## Safety invariant
///
/// Same as `BulkKvBuffer` - the `results` field MUST NOT be mutated after any
/// `ResourceArc::make_binary` call.
struct BulkOptValBuffer {
    results: Vec<Option<Vec<u8>>>,
}

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        true_ = "true",
        false_ = "false",
        pending,
        io_complete,
        tokio_complete,
        set_range,
        set_bit,
        append,
        incr_by,
        incr_by_float,
    }
}

/// Wraps `Store` in a `Mutex` so it satisfies `Send + Sync` for `ResourceArc`.
///
/// ## Ownership model
///
/// Each Elixir shard `GenServer` owns **one** `StoreResource` for its keyspace
/// partition. There is no sharing across shards — the Mutex is uncontested in
/// the steady state. Concurrent access to the same shard resource would indicate
/// a bug in the Elixir routing layer.
///
/// The Mutex exists only to satisfy Rust's `Send + Sync` bounds required by
/// `ResourceArc`; in practice it is always acquired by exactly one BEAM process
/// (the shard `GenServer`) at a time.
///
/// The `async_uring` field holds an optional `AsyncUringBackend` (Linux only)
/// that is initialised alongside the store when `io_uring` is available. It is
/// behind its own Mutex because it may be accessed from both the NIF submit
/// path and the background completion thread.
struct StoreResource {
    store: Mutex<Store>,
    /// Monotonically increasing counter for async operation IDs.
    /// Only used on Linux with `io_uring`; always present for structural consistency.
    #[allow(dead_code)]
    next_op_id: AtomicU64,
    /// Async `io_uring` backend (Linux only, `None` on other platforms or when
    /// `io_uring` is unavailable).
    #[cfg(target_os = "linux")]
    async_uring: Option<io_backend::async_uring::AsyncUringBackend>,
}

// ---------------------------------------------------------------------------
// Yielding NIF continuation state resources
// ---------------------------------------------------------------------------

/// Safety limit: maximum items to encode before unconditionally checking
/// `consume_timeslice`. In practice, the BEAM-guided check every
/// `YIELD_CHECK_INTERVAL` items will trigger yielding much sooner than
/// this for large datasets.
const YIELD_CHUNK_SIZE: usize = 500;

/// How often (in items) to call `consume_timeslice` and let the BEAM
/// decide whether we should yield.  64 is a good trade-off: low enough
/// to keep scheduler latency under ~1ms for typical item sizes, high
/// enough that the per-item overhead of the timeslice check is negligible.
const YIELD_CHECK_INTERVAL: usize = 64;

/// Continuation state for `keys()` — holds pre-collected keys from RAM and
/// tracks encoding progress across yield points.
struct KeysIterState {
    keys: Vec<Vec<u8>>,
    index: Mutex<usize>,
}

/// Continuation state for `get_all()` — holds pre-collected key-value pairs
/// (all disk I/O done upfront under the store Mutex) and tracks encoding
/// progress across yield points.
struct GetAllIterState {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
    index: Mutex<usize>,
}

/// Continuation state for `get_batch()` — holds pre-fetched results
/// (one Option<Vec<u8>> per requested key) and tracks encoding progress.
struct GetBatchIterState {
    results: Vec<Option<Vec<u8>>>,
    index: Mutex<usize>,
}

/// Continuation state for `get_range()` — holds pre-fetched sorted key-value
/// pairs and tracks encoding progress across yield points.
struct GetRangeIterState {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
    index: Mutex<usize>,
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(StoreResource, env);
    let _ = rustler::resource!(ValueBuffer, env);
    let _ = rustler::resource!(KeysIterState, env);
    let _ = rustler::resource!(GetAllIterState, env);
    let _ = rustler::resource!(GetBatchIterState, env);
    let _ = rustler::resource!(GetRangeIterState, env);
    let _ = rustler::resource!(BulkKvBuffer, env);
    let _ = rustler::resource!(BulkOptValBuffer, env);
    let _ = rustler::resource!(hnsw::HnswResource, env);
    let _ = rustler::resource!(cuckoo::CuckooResource, env);
    let _ = rustler::resource!(topk::TopKResource, env);
    let _ = rustler::resource!(cms::CmsResource, env);
    let _ = rustler::resource!(bloom::BloomResource, env);
    tdigest::register_resource(env);
    true
}

/// Open (or create) a Bitcask store at the given path.
/// Returns `{:ok, resource}` or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Although this does filesystem I/O (reading
/// hint files, scanning data files), it is called once per shard lifecycle —
/// the brief scheduler block on startup is acceptable and avoids occupying a
/// slot in the limited DirtyIo thread pool (default 10 threads shared by the
/// entire BEAM).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn new(env: Env, path: String) -> NifResult<Term> {
    let p = std::path::Path::new(&path);
    match Store::open(p) {
        Ok(store) => {
            // On Linux, try to open an AsyncUringBackend for the same data
            // file. If io_uring is unavailable, `async_uring` stays `None`
            // and `put_batch_async` will fall back to sync.
            #[cfg(target_os = "linux")]
            let async_uring = if io_backend::detect_io_uring() {
                let log_path = store.active_log_path();
                io_backend::async_uring::AsyncUringBackend::open(&log_path).ok()
            } else {
                None
            };

            let resource = ResourceArc::new(StoreResource {
                store: Mutex::new(store),
                next_op_id: AtomicU64::new(1),
                #[cfg(target_os = "linux")]
                async_uring,
            });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Get a value by key. Returns `{:ok, value}`, `{:ok, :nil}`, or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. A single pread (<1ms on NVMe) is fast
/// enough that the brief scheduler block is preferable to consuming a DirtyIo
/// thread. The shard GenServer serializes access, so the Mutex is uncontested.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.get(key.as_slice()) {
        Ok(Some(value)) => {
            let mut bin = OwnedBinary::new(value.len()).ok_or(rustler::Error::BadArg)?;
            bin.as_mut_slice().copy_from_slice(&value);
            Ok((atoms::ok(), Binary::from_owned(bin, env)).encode(env))
        }
        Ok(None) => Ok((atoms::ok(), atoms::nil()).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Zero-copy GET: returns a BEAM binary that points directly into a
/// Rust-owned `ValueBuffer` resource, avoiding the `Vec → OwnedBinary`
/// memcpy that the regular `get/3` performs.
///
/// The BEAM binary created by `ResourceArc::make_binary` shares the
/// backing memory of the `ValueBuffer`. The resource's ref-count is
/// incremented by the binary term; when the Erlang binary is GC'd the
/// ref-count drops and the `Vec` is freed. No copy occurs.
///
/// Returns `{:ok, binary}`, `{:ok, :nil}`, or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Same as `get/3` — runs on a Normal BEAM scheduler. A single pread
/// (<1ms on NVMe) is fast enough; the shard GenServer serializes access.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_zero_copy<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.get(key.as_slice()) {
        Ok(Some(value)) => {
            // Wrap the value Vec in a resource. `make_binary` calls
            // `enif_make_resource_binary` under the hood, which creates
            // a BEAM binary term pointing directly into `buffer.data`.
            let buffer = ResourceArc::new(ValueBuffer { data: value });
            let bin = buffer.make_binary(env, |buf| &buf.data);
            Ok((atoms::ok(), bin).encode(env))
        }
        Ok(None) => Ok((atoms::ok(), atoms::nil()).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Look up a key's on-disk file location without reading the value.
///
/// Returns `{:ok, {file_path, value_offset, value_size}}` if the key exists,
/// `{:ok, :nil}` if not found/expired, or `{:error, reason}`.
///
/// Used by the sendfile optimisation in standalone TCP mode: the Elixir
/// connection layer can use `:file.sendfile/5` to zero-copy the value bytes
/// directly from the Bitcask data file to the TCP socket.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Only a keydir lookup — no disk I/O.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_file_ref<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.get_file_ref(key.as_slice()) {
        Ok(Some((path, offset, size))) => {
            let path_str = path.to_string_lossy().to_string();
            Ok((atoms::ok(), (path_str, offset, u64::from(size))).encode(env))
        }
        Ok(None) => Ok((atoms::ok(), atoms::nil()).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Put a key-value pair. `expire_at_ms` = 0 means no expiry.
/// Returns `:ok` or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. A single pwrite + fsync (<1ms on NVMe)
/// is fast enough that the brief scheduler block is preferable to consuming a
/// DirtyIo thread. The shard GenServer serializes access.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
    value: Binary<'a>,
    expire_at_ms: u64,
) -> NifResult<Term<'a>> {
    if let Err(msg) = crate::log::validate_kv_sizes(key.as_slice(), value.as_slice()) {
        return Ok((atoms::error(), msg).encode(env));
    }
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.put(key.as_slice(), value.as_slice(), expire_at_ms) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Write multiple key-value pairs in a single fsync (group commit).
///
/// `batch` is a list of `{key, value, expire_at_ms}` tuples.
/// Returns `:ok` or `{:error, reason}`.
///
/// ## BEAM scheduler contract
///
/// Runs on a Normal BEAM scheduler. The shard GenServer serializes all access,
/// so the Mutex is uncontested and the batch completes quickly (<10ms for 10K
/// entries on NVMe). Blocking a Normal scheduler briefly is preferable to
/// occupying a DirtyIo thread from the limited pool (default 10 threads shared
/// by the entire BEAM). The GenServer collects writes during a batch window,
/// then calls `put_batch` once per window — O(1) scheduler occupancy per batch.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put_batch<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    batch: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    // Validate key/value sizes before acquiring the lock.
    for (k, v, _) in &batch {
        if let Err(msg) = crate::log::validate_kv_sizes(k.as_slice(), v.as_slice()) {
            return Ok((atoms::error(), msg).encode(env));
        }
    }
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let entries: Vec<(&[u8], &[u8], u64)> = batch
        .iter()
        .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
        .collect();
    match store.put_batch(&entries) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Async variant of `put_batch` that runs on a **normal BEAM scheduler thread**.
///
/// ## Why `schedule = "Normal"`
///
/// This NIF does no blocking I/O itself on the async path:
///   - Record serialisation is pure CPU work (microseconds).
///   - `submit_batch` pushes SQEs to the `io_uring` ring buffer and calls
///     `ring.submit()` — a single non-blocking syscall.
///   - The actual `fsync` runs in a dedicated background thread; no scheduler
///     thread is ever blocked waiting for disk.
///
/// All NIFs in this module use Normal scheduling to avoid occupying the limited
/// DirtyIo thread pool (default 10 threads shared by the entire BEAM). The
/// shard GenServer serializes access to each store, so Mutex contention is
/// negligible and individual NIF calls complete quickly.
///
/// On the **fallback path** (macOS / no `io_uring`) this NIF calls
/// `store.put_batch` synchronously, which blocks on fsync. This is acceptable
/// because the shard GenServer serializes access and the block duration is
/// brief (<10ms for typical batches on NVMe).
///
/// ## Linux with `io_uring`
///
/// 1. Encodes records and updates the keydir optimistically.
/// 2. Submits write SQEs + one fsync SQE to an `AsyncUringBackend` (non-blocking).
/// 3. Returns `{:pending, op_id}` immediately.
/// 4. A background Rust thread drains the fsync CQE and sends
///    `{:io_complete, op_id, :ok | {:error, reason}}` to the calling process.
///
/// ## Fallback (macOS / no `io_uring`)
///
/// Falls back to synchronous `put_batch`, returning `:ok` directly.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put_batch_async<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    batch: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    let entries: Vec<(&[u8], &[u8], u64)> = batch
        .iter()
        .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
        .collect();

    // Try the async path on Linux.
    #[cfg(target_os = "linux")]
    if let Some(ref async_uring) = resource.async_uring {
        let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;

        // Step 1: Encode records and compute file offsets (pure, no side effects).
        let (encoded, file_offsets) = store.encode_for_async(&entries);

        // Build buffer refs from the owned encoded Vecs.
        let buf_refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();

        // Allocate a unique op_id.
        let op_id = resource.next_op_id.fetch_add(1, Ordering::Relaxed);

        // Get the caller's PID so the background thread can send the result.
        let caller_pid: LocalPid = env.pid();

        // Step 2: Submit to the async ring (non-blocking).
        // Only update the keydir and advance offsets if submission succeeds.
        match async_uring.submit_batch(&buf_refs, &file_offsets, caller_pid, op_id) {
            Ok(()) if buf_refs.is_empty() => {
                // Empty batch — nothing was submitted to the ring, so no CQE
                // will arrive. Return :ok immediately instead of {:pending, _}.
                return Ok(atoms::ok().encode(env));
            }
            Ok(()) => {
                // Step 3: Ring submission succeeded — commit side effects.
                // The keydir and writer offset are only updated now, after we
                // know the SQEs were accepted by the kernel. This ensures that
                // if submit_batch failed (e.g. ring capacity exceeded), NIF.get
                // will not try to read from file offsets that were never written.
                store.commit_async_batch(&entries, &file_offsets, &encoded);
                return Ok((atoms::pending(), op_id).encode(env));
            }
            Err(e) => {
                // Ring submission failed — return error WITHOUT updating the
                // keydir or advancing offsets. No data was written.
                return Ok((atoms::error(), e.to_string()).encode(env));
            }
        }
    }

    // Fallback: synchronous put_batch.
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.put_batch(&entries) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Delete a key. Returns `{:ok, true}`, `{:ok, false}` (not found), or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. A single tombstone write + fsync (<1ms on
/// NVMe) is fast enough that the brief scheduler block is preferable to
/// consuming a DirtyIo thread. The shard GenServer serializes access.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn delete<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.delete(key.as_slice()) {
        Ok(true) => Ok((atoms::ok(), atoms::true_()).encode(env)),
        Ok(false) => Ok((atoms::ok(), atoms::false_()).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

// ===========================================================================
// Yielding NIFs: keys, get_all, get_batch, get_range
// ===========================================================================
//
// ## How yielding works (the `enif_schedule_nif` pattern)
//
// These NIFs use Rustler's `NifReturned::Reschedule` (which calls
// `enif_schedule_nif`) to cooperatively yield back to the BEAM scheduler
// between chunks of work. This is the standard Erlang/OTP pattern for
// long-running NIFs.
//
// 1. **Entry NIF**: Locks the store Mutex, collects all data (keys or
//    key-value pairs) into owned Rust Vecs, releases the Mutex, wraps
//    the data in a `ResourceArc<*IterState>`, then reschedules to the
//    continuation function with two args: [state_resource, partial_list].
//
// 2. **Continuation**: Decodes the state resource and the partial BEAM
//    list from argv. Processes items one at a time, calling
//    `consume_timeslice(env, 1)` every `YIELD_CHECK_INTERVAL` items.
//    When `consume_timeslice` returns `true` (timeslice exhausted) and
//    more work remains, reschedules itself. This is BEAM-guided: the
//    scheduler decides when to yield based on actual CPU time consumed,
//    not a fixed item count. When done, reverses the list and returns.
//
// 3. **Result**: From the caller's perspective: one NIF call, one result.
//    From the BEAM's perspective: N short slices with other processes
//    scheduled between them.
//
// Terms created via `enif_make_*` in a NIF env survive across reschedules
// because `enif_schedule_nif` copies the argv terms into the process heap
// before destroying the old env. The partial list is a proper BEAM list
// that lives on the process heap.
//
// For small result sets (<= YIELD_CHUNK_SIZE items), the entry NIF
// encodes everything directly without rescheduling, avoiding the overhead
// of resource allocation and env switching.
//
// The continuation functions use BEAM-guided adaptive yielding: every
// YIELD_CHECK_INTERVAL items (64), we call `consume_timeslice(env, 1)`.
// If it returns `true`, the BEAM wants the thread back and we yield.
// This adapts naturally to item size: encoding large values consumes the
// timeslice faster, so we yield after fewer items. For small items, we
// may process thousands before the BEAM asks us to yield.

/// Encode one key `Vec<u8>` into a BEAM binary term.
fn encode_key_term<'a>(env: Env<'a>, k: &[u8]) -> Option<Term<'a>> {
    OwnedBinary::new(k.len()).map(|mut bin| {
        bin.as_mut_slice().copy_from_slice(k);
        Binary::from_owned(bin, env).encode(env)
    })
}

/// Encode one key-value pair into a BEAM `{key, value}` tuple term.
fn encode_kv_term<'a>(env: Env<'a>, k: &[u8], v: &[u8]) -> Option<Term<'a>> {
    let kbin = OwnedBinary::new(k.len()).map(|mut b| {
        b.as_mut_slice().copy_from_slice(k);
        b
    })?;
    let vbin = OwnedBinary::new(v.len()).map(|mut b| {
        b.as_mut_slice().copy_from_slice(v);
        b
    })?;
    Some((Binary::from_owned(kbin, env), Binary::from_owned(vbin, env)).encode(env))
}

/// Encode one `Option<Vec<u8>>` into either a BEAM binary or `:nil` atom.
fn encode_opt_val_term<'a>(env: Env<'a>, opt: &Option<Vec<u8>>) -> Term<'a> {
    match opt {
        Some(v) => {
            if let Some(mut bin) = OwnedBinary::new(v.len()) {
                bin.as_mut_slice().copy_from_slice(v);
                Binary::from_owned(bin, env).encode(env)
            } else {
                atoms::nil().encode(env)
            }
        }
        None => atoms::nil().encode(env),
    }
}

/// Reverse a BEAM list. Uses `enif_make_reverse_list` under the hood via
/// Rustler's `Term::list_reverse`.
fn reverse_list<'a>(env: Env<'a>, list: Term<'a>) -> Term<'a> {
    list.list_reverse()
        .unwrap_or_else(|_| Vec::<Term>::new().encode(env))
}

// ---------------------------------------------------------------------------
// Yielding NIF: keys()
// ---------------------------------------------------------------------------

/// Continuation for `keys()`. Args: [state_resource, partial_list].
///
/// # Safety
///
/// Raw NIF callback invoked by BEAM via `enif_schedule_nif`.
unsafe extern "C" fn keys_continue(
    nif_env: rustler::codegen_runtime::NIF_ENV,
    argc: rustler::codegen_runtime::c_int,
    argv: *const rustler::codegen_runtime::NIF_TERM,
) -> rustler::codegen_runtime::NIF_TERM {
    let lifetime = ();
    let env = Env::new(&lifetime, nif_env);
    let args = std::slice::from_raw_parts(argv, argc as usize);
    let state_term = Term::new(env, args[0]);
    let partial_list = Term::new(env, args[1]);

    let state: ResourceArc<KeysIterState> = match state_term.decode() {
        Ok(s) => s,
        Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
    };

    let mut idx = state.index.lock().unwrap();
    let total = state.keys.len();

    // BEAM-guided adaptive yielding: process items one at a time, checking
    // consume_timeslice every YIELD_CHECK_INTERVAL items. The BEAM tells
    // us when our timeslice is exhausted (returns true), at which point we
    // yield. This adapts naturally to item size and system load.
    let mut acc = partial_list;
    let mut items_this_slice: usize = 0;

    while *idx < total {
        if let Some(term) = encode_key_term(env, &state.keys[*idx]) {
            acc = acc.list_prepend(term);
        } else {
            return rustler::codegen_runtime::NifReturned::BadArg.apply(env);
        }
        *idx += 1;
        items_this_slice += 1;

        // Ask the BEAM every 64 items if our timeslice is exhausted
        if items_this_slice % YIELD_CHECK_INTERVAL == 0 && *idx < total {
            if consume_timeslice(env, 1) {
                // BEAM says yield — timeslice exhausted
                drop(idx);
                let new_state_term = state_term.as_c_arg();
                let new_list = acc.as_c_arg();
                return rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("keys").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: keys_continue,
                    args: vec![new_state_term, new_list],
                }
                .apply(env);
            }
        }
    }

    // Done — consume full timeslice, reverse, and return.
    let _ = consume_timeslice(env, 100);
    drop(idx);
    let result = reverse_list(env, acc);
    rustler::codegen_runtime::NifReturned::Term(result.as_c_arg()).apply(env)
}

rustler::codegen_runtime::inventory::submit! {
    rustler::Nif {
        name: "keys\0".as_ptr() as *const rustler::codegen_runtime::c_char,
        arity: 1,
        flags: rustler::SchedulerFlags::Normal as rustler::codegen_runtime::c_uint,
        raw_func: {
            /// Entry NIF for `keys/1`.
            ///
            /// # Safety
            ///
            /// Raw NIF callback. BEAM guarantees valid env/argc/argv.
            unsafe extern "C" fn keys_entry(
                nif_env: rustler::codegen_runtime::NIF_ENV,
                argc: rustler::codegen_runtime::c_int,
                argv: *const rustler::codegen_runtime::NIF_TERM,
            ) -> rustler::codegen_runtime::NIF_TERM {
                let lifetime = ();
                let env = Env::new(&lifetime, nif_env);
                let args = std::slice::from_raw_parts(argv, argc as usize);
                let store_term = Term::new(env, args[0]);

                let resource: ResourceArc<StoreResource> = match store_term.decode() {
                    Ok(r) => r,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let store = match resource.store.lock() {
                    Ok(s) => s,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };
                let raw_keys = store.keys();
                drop(store);

                // Small result: encode directly, no yielding overhead.
                if raw_keys.len() <= YIELD_CHUNK_SIZE {
                    let all_keys: Vec<Term> = raw_keys
                        .iter()
                        .filter_map(|k| encode_key_term(env, k))
                        .collect();
                    return rustler::codegen_runtime::NifReturned::Term(
                        all_keys.encode(env).as_c_arg(),
                    )
                    .apply(env);
                }

                let state = ResourceArc::new(KeysIterState {
                    keys: raw_keys,
                    index: Mutex::new(0),
                });
                let state_c = state.encode(env).as_c_arg();
                let empty_list = Vec::<Term>::new().encode(env).as_c_arg();

                rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("keys").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: keys_continue,
                    args: vec![state_c, empty_list],
                }
                .apply(env)
            }
            keys_entry
        },
    }
}

// ---------------------------------------------------------------------------
// Yielding NIF: get_all()
// ---------------------------------------------------------------------------

/// Continuation for `get_all()`. Args: [state_resource, partial_list].
///
/// # Safety
///
/// Raw NIF callback invoked by BEAM via `enif_schedule_nif`.
unsafe extern "C" fn get_all_continue(
    nif_env: rustler::codegen_runtime::NIF_ENV,
    argc: rustler::codegen_runtime::c_int,
    argv: *const rustler::codegen_runtime::NIF_TERM,
) -> rustler::codegen_runtime::NIF_TERM {
    let lifetime = ();
    let env = Env::new(&lifetime, nif_env);
    let args = std::slice::from_raw_parts(argv, argc as usize);
    let state_term = Term::new(env, args[0]);
    let partial_list = Term::new(env, args[1]);

    let state: ResourceArc<GetAllIterState> = match state_term.decode() {
        Ok(s) => s,
        Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
    };

    let mut idx = state.index.lock().unwrap();
    let total = state.pairs.len();

    let mut acc = partial_list;
    let mut items_this_slice: usize = 0;

    while *idx < total {
        let (k, v) = &state.pairs[*idx];
        if let Some(term) = encode_kv_term(env, k, v) {
            acc = acc.list_prepend(term);
        } else {
            return rustler::codegen_runtime::NifReturned::BadArg.apply(env);
        }
        *idx += 1;
        items_this_slice += 1;

        if items_this_slice % YIELD_CHECK_INTERVAL == 0 && *idx < total {
            if consume_timeslice(env, 1) {
                drop(idx);
                let new_state = state_term.as_c_arg();
                let new_list = acc.as_c_arg();
                return rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("get_all").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: get_all_continue,
                    args: vec![new_state, new_list],
                }
                .apply(env);
            }
        }
    }

    let _ = consume_timeslice(env, 100);
    drop(idx);
    let result = reverse_list(env, acc);
    rustler::codegen_runtime::NifReturned::Term((atoms::ok(), result).encode(env).as_c_arg())
        .apply(env)
}

rustler::codegen_runtime::inventory::submit! {
    rustler::Nif {
        name: "get_all\0".as_ptr() as *const rustler::codegen_runtime::c_char,
        arity: 1,
        flags: rustler::SchedulerFlags::Normal as rustler::codegen_runtime::c_uint,
        raw_func: {
            /// Entry NIF for `get_all/1`.
            ///
            /// # Safety
            ///
            /// Raw NIF callback.
            unsafe extern "C" fn get_all_entry(
                nif_env: rustler::codegen_runtime::NIF_ENV,
                argc: rustler::codegen_runtime::c_int,
                argv: *const rustler::codegen_runtime::NIF_TERM,
            ) -> rustler::codegen_runtime::NIF_TERM {
                let lifetime = ();
                let env = Env::new(&lifetime, nif_env);
                let args = std::slice::from_raw_parts(argv, argc as usize);
                let store_term = Term::new(env, args[0]);

                let resource: ResourceArc<StoreResource> = match store_term.decode() {
                    Ok(r) => r,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let mut store = match resource.store.lock() {
                    Ok(s) => s,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let pairs = match store.get_all() {
                    Ok(p) => p,
                    Err(e) => {
                        drop(store);
                        return rustler::codegen_runtime::NifReturned::Term(
                            (atoms::error(), e.to_string()).encode(env).as_c_arg(),
                        )
                        .apply(env);
                    }
                };
                drop(store);

                if pairs.len() <= YIELD_CHUNK_SIZE {
                    let terms: Vec<Term> = pairs
                        .iter()
                        .filter_map(|(k, v)| encode_kv_term(env, k, v))
                        .collect();
                    return rustler::codegen_runtime::NifReturned::Term(
                        (atoms::ok(), terms).encode(env).as_c_arg(),
                    )
                    .apply(env);
                }

                let state = ResourceArc::new(GetAllIterState {
                    pairs,
                    index: Mutex::new(0),
                });
                let state_c = state.encode(env).as_c_arg();
                let empty_list = Vec::<Term>::new().encode(env).as_c_arg();

                rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("get_all").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: get_all_continue,
                    args: vec![state_c, empty_list],
                }
                .apply(env)
            }
            get_all_entry
        },
    }
}

// ---------------------------------------------------------------------------
// Yielding NIF: get_batch()
// ---------------------------------------------------------------------------

/// Continuation for `get_batch()`. Args: [state_resource, partial_list].
///
/// # Safety
///
/// Raw NIF callback invoked by BEAM via `enif_schedule_nif`.
unsafe extern "C" fn get_batch_continue(
    nif_env: rustler::codegen_runtime::NIF_ENV,
    argc: rustler::codegen_runtime::c_int,
    argv: *const rustler::codegen_runtime::NIF_TERM,
) -> rustler::codegen_runtime::NIF_TERM {
    let lifetime = ();
    let env = Env::new(&lifetime, nif_env);
    let args = std::slice::from_raw_parts(argv, argc as usize);
    let state_term = Term::new(env, args[0]);
    let partial_list = Term::new(env, args[1]);

    let state: ResourceArc<GetBatchIterState> = match state_term.decode() {
        Ok(s) => s,
        Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
    };

    let mut idx = state.index.lock().unwrap();
    let total = state.results.len();

    let mut acc = partial_list;
    let mut items_this_slice: usize = 0;

    while *idx < total {
        let term = encode_opt_val_term(env, &state.results[*idx]);
        acc = acc.list_prepend(term);
        *idx += 1;
        items_this_slice += 1;

        if items_this_slice % YIELD_CHECK_INTERVAL == 0 && *idx < total {
            if consume_timeslice(env, 1) {
                drop(idx);
                let new_state = state_term.as_c_arg();
                let new_list = acc.as_c_arg();
                return rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("get_batch").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: get_batch_continue,
                    args: vec![new_state, new_list],
                }
                .apply(env);
            }
        }
    }

    let _ = consume_timeslice(env, 100);
    drop(idx);
    let result = reverse_list(env, acc);
    rustler::codegen_runtime::NifReturned::Term((atoms::ok(), result).encode(env).as_c_arg())
        .apply(env)
}

rustler::codegen_runtime::inventory::submit! {
    rustler::Nif {
        name: "get_batch\0".as_ptr() as *const rustler::codegen_runtime::c_char,
        arity: 2,
        flags: rustler::SchedulerFlags::Normal as rustler::codegen_runtime::c_uint,
        raw_func: {
            /// Entry NIF for `get_batch/2`.
            ///
            /// # Safety
            ///
            /// Raw NIF callback.
            unsafe extern "C" fn get_batch_entry(
                nif_env: rustler::codegen_runtime::NIF_ENV,
                argc: rustler::codegen_runtime::c_int,
                argv: *const rustler::codegen_runtime::NIF_TERM,
            ) -> rustler::codegen_runtime::NIF_TERM {
                let lifetime = ();
                let env = Env::new(&lifetime, nif_env);
                let args = std::slice::from_raw_parts(argv, argc as usize);
                let store_term = Term::new(env, args[0]);
                let keys_term = Term::new(env, args[1]);

                let resource: ResourceArc<StoreResource> = match store_term.decode() {
                    Ok(r) => r,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let key_binaries: Vec<Binary> = match keys_term.decode() {
                    Ok(k) => k,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };
                let key_slices: Vec<&[u8]> = key_binaries.iter().map(Binary::as_slice).collect();

                let mut store = match resource.store.lock() {
                    Ok(s) => s,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let results = match store.get_batch(&key_slices) {
                    Ok(r) => r,
                    Err(e) => {
                        drop(store);
                        return rustler::codegen_runtime::NifReturned::Term(
                            (atoms::error(), e.to_string()).encode(env).as_c_arg(),
                        )
                        .apply(env);
                    }
                };
                drop(store);

                if results.len() <= YIELD_CHUNK_SIZE {
                    let terms: Vec<Term> = results
                        .iter()
                        .map(|opt| encode_opt_val_term(env, opt))
                        .collect();
                    return rustler::codegen_runtime::NifReturned::Term(
                        (atoms::ok(), terms).encode(env).as_c_arg(),
                    )
                    .apply(env);
                }

                let state = ResourceArc::new(GetBatchIterState {
                    results,
                    index: Mutex::new(0),
                });
                let state_c = state.encode(env).as_c_arg();
                let empty_list = Vec::<Term>::new().encode(env).as_c_arg();

                rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("get_batch").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: get_batch_continue,
                    args: vec![state_c, empty_list],
                }
                .apply(env)
            }
            get_batch_entry
        },
    }
}

// ---------------------------------------------------------------------------
// Yielding NIF: get_range()
// ---------------------------------------------------------------------------

/// Continuation for `get_range()`. Args: [state_resource, partial_list].
///
/// # Safety
///
/// Raw NIF callback invoked by BEAM via `enif_schedule_nif`.
unsafe extern "C" fn get_range_continue(
    nif_env: rustler::codegen_runtime::NIF_ENV,
    argc: rustler::codegen_runtime::c_int,
    argv: *const rustler::codegen_runtime::NIF_TERM,
) -> rustler::codegen_runtime::NIF_TERM {
    let lifetime = ();
    let env = Env::new(&lifetime, nif_env);
    let args = std::slice::from_raw_parts(argv, argc as usize);
    let state_term = Term::new(env, args[0]);
    let partial_list = Term::new(env, args[1]);

    let state: ResourceArc<GetRangeIterState> = match state_term.decode() {
        Ok(s) => s,
        Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
    };

    let mut idx = state.index.lock().unwrap();
    let total = state.pairs.len();

    let mut acc = partial_list;
    let mut items_this_slice: usize = 0;

    while *idx < total {
        let (k, v) = &state.pairs[*idx];
        if let Some(term) = encode_kv_term(env, k, v) {
            acc = acc.list_prepend(term);
        } else {
            return rustler::codegen_runtime::NifReturned::BadArg.apply(env);
        }
        *idx += 1;
        items_this_slice += 1;

        if items_this_slice % YIELD_CHECK_INTERVAL == 0 && *idx < total {
            if consume_timeslice(env, 1) {
                drop(idx);
                let new_state = state_term.as_c_arg();
                let new_list = acc.as_c_arg();
                return rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("get_range").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: get_range_continue,
                    args: vec![new_state, new_list],
                }
                .apply(env);
            }
        }
    }

    let _ = consume_timeslice(env, 100);
    drop(idx);
    let result = reverse_list(env, acc);
    rustler::codegen_runtime::NifReturned::Term((atoms::ok(), result).encode(env).as_c_arg())
        .apply(env)
}

rustler::codegen_runtime::inventory::submit! {
    rustler::Nif {
        name: "get_range\0".as_ptr() as *const rustler::codegen_runtime::c_char,
        arity: 4,
        flags: rustler::SchedulerFlags::Normal as rustler::codegen_runtime::c_uint,
        raw_func: {
            /// Entry NIF for `get_range/4`.
            ///
            /// # Safety
            ///
            /// Raw NIF callback.
            unsafe extern "C" fn get_range_entry(
                nif_env: rustler::codegen_runtime::NIF_ENV,
                argc: rustler::codegen_runtime::c_int,
                argv: *const rustler::codegen_runtime::NIF_TERM,
            ) -> rustler::codegen_runtime::NIF_TERM {
                let lifetime = ();
                let env = Env::new(&lifetime, nif_env);
                let args = std::slice::from_raw_parts(argv, argc as usize);
                let store_term = Term::new(env, args[0]);
                let min_key_term = Term::new(env, args[1]);
                let max_key_term = Term::new(env, args[2]);
                let max_count_term = Term::new(env, args[3]);

                let resource: ResourceArc<StoreResource> = match store_term.decode() {
                    Ok(r) => r,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };
                let min_key: Binary = match min_key_term.decode() {
                    Ok(b) => b,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };
                let max_key: Binary = match max_key_term.decode() {
                    Ok(b) => b,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };
                let max_count: u64 = match max_count_term.decode() {
                    Ok(c) => c,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let mut store = match resource.store.lock() {
                    Ok(s) => s,
                    Err(_) => return rustler::codegen_runtime::NifReturned::BadArg.apply(env),
                };

                let pairs = match store.get_range(
                    min_key.as_slice(),
                    max_key.as_slice(),
                    max_count as usize,
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        drop(store);
                        return rustler::codegen_runtime::NifReturned::Term(
                            (atoms::error(), e.to_string()).encode(env).as_c_arg(),
                        )
                        .apply(env);
                    }
                };
                drop(store);

                if pairs.len() <= YIELD_CHUNK_SIZE {
                    let terms: Vec<Term> = pairs
                        .iter()
                        .filter_map(|(k, v)| encode_kv_term(env, k, v))
                        .collect();
                    return rustler::codegen_runtime::NifReturned::Term(
                        (atoms::ok(), terms).encode(env).as_c_arg(),
                    )
                    .apply(env);
                }

                let state = ResourceArc::new(GetRangeIterState {
                    pairs,
                    index: Mutex::new(0),
                });
                let state_c = state.encode(env).as_c_arg();
                let empty_list = Vec::<Term>::new().encode(env).as_c_arg();

                rustler::codegen_runtime::NifReturned::Reschedule {
                    fun_name: std::ffi::CString::new("get_range").unwrap(),
                    flags: rustler::SchedulerFlags::Normal,
                    fun: get_range_continue,
                    args: vec![state_c, empty_list],
                }
                .apply(env)
            }
            get_range_entry
        },
    }
}

/// Write a hint file for the active data file.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Hint file writes are infrequent maintenance
/// operations. The brief scheduler block is preferable to consuming a DirtyIo
/// thread.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn write_hint(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.write_hint_file() {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Purge all logically expired keys from the keydir and write tombstones.
/// Returns `{:ok, count}` where count is the number of keys purged, or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Purging is an infrequent maintenance
/// operation triggered by the shard GenServer. The brief scheduler block is
/// acceptable and avoids consuming a DirtyIo thread.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn purge_expired(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.purge_expired() {
        Ok(count) => Ok((atoms::ok(), count as u64).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Single-key read-modify-write is a fast
/// operation (one pread + one pwrite + fsync, <1ms on NVMe). The shard
/// GenServer serializes access.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn read_modify_write<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
    operation: Term<'a>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let op = decode_rmw_op(operation)?;
    match store.read_modify_write(key.as_slice(), &op) {
        Ok(value) => {
            let mut bin = OwnedBinary::new(value.len()).ok_or(rustler::Error::BadArg)?;
            bin.as_mut_slice().copy_from_slice(&value);
            Ok((atoms::ok(), Binary::from_owned(bin, env)).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

fn decode_rmw_op(term: Term) -> NifResult<store::RmwOp> {
    use rustler::types::tuple::get_tuple;
    let tuple = get_tuple(term).map_err(|_| rustler::Error::BadArg)?;
    if tuple.is_empty() {
        return Err(rustler::Error::BadArg);
    }
    let tag: rustler::types::atom::Atom = tuple[0].decode().map_err(|_| rustler::Error::BadArg)?;
    if tag == atoms::set_range() {
        if tuple.len() != 3 {
            return Err(rustler::Error::BadArg);
        }
        let offset: u64 = tuple[1].decode().map_err(|_| rustler::Error::BadArg)?;
        let bytes: Binary = tuple[2].decode().map_err(|_| rustler::Error::BadArg)?;
        Ok(store::RmwOp::SetRange(offset, bytes.as_slice().to_vec()))
    } else if tag == atoms::set_bit() {
        if tuple.len() != 3 {
            return Err(rustler::Error::BadArg);
        }
        let bit_offset: u64 = tuple[1].decode().map_err(|_| rustler::Error::BadArg)?;
        let bit_value: u64 = tuple[2].decode().map_err(|_| rustler::Error::BadArg)?;
        Ok(store::RmwOp::SetBit(bit_offset, bit_value as u8))
    } else if tag == atoms::append() {
        if tuple.len() != 2 {
            return Err(rustler::Error::BadArg);
        }
        let data: Binary = tuple[1].decode().map_err(|_| rustler::Error::BadArg)?;
        Ok(store::RmwOp::Append(data.as_slice().to_vec()))
    } else if tag == atoms::incr_by() {
        if tuple.len() != 2 {
            return Err(rustler::Error::BadArg);
        }
        let delta: i64 = tuple[1].decode().map_err(|_| rustler::Error::BadArg)?;
        Ok(store::RmwOp::IncrBy(delta))
    } else if tag == atoms::incr_by_float() {
        if tuple.len() != 2 {
            return Err(rustler::Error::BadArg);
        }
        let delta: f64 = tuple[1].decode().map_err(|_| rustler::Error::BadArg)?;
        Ok(store::RmwOp::IncrByFloat(delta))
    } else {
        Err(rustler::Error::BadArg)
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Stats computation is a fast in-memory
/// operation. Infrequent maintenance call.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn shard_stats(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.shard_stats() {
        Ok((total, live, dead, file_count, key_count, frag)) => Ok((
            atoms::ok(),
            (total, live, dead, file_count, key_count, frag),
        )
            .encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. File size lookup is a fast stat syscall.
/// Infrequent maintenance call.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn file_sizes(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.file_sizes() {
        Ok(sizes) => Ok((atoms::ok(), sizes).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Compaction is an infrequent maintenance
/// operation triggered by the shard GenServer. Although it may take longer than
/// data-plane operations, it runs at most once per compaction cycle and the
/// shard GenServer serializes access. The brief Normal scheduler block is
/// preferable to consuming a DirtyIo thread.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn run_compaction(
    env: Env,
    resource: ResourceArc<StoreResource>,
    file_ids: Vec<u64>,
) -> NifResult<Term> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.run_compaction(&file_ids) {
        Ok((written, dropped, reclaimed)) => {
            Ok((atoms::ok(), (written, dropped, reclaimed)).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. A single statfs syscall. Infrequent
/// maintenance call.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn available_disk_space(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.available_disk_space() {
        Ok(space) => Ok((atoms::ok(), space).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

// ===========================================================================
// Zero-copy bulk NIFs
// ===========================================================================

/// Zero-copy get_all: returns `{:ok, [{key, value}, ...]}` where every key and
/// value binary points directly into a Rust-owned `BulkKvBuffer` resource.
#[rustler::nif(schedule = "Normal")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::elidable_lifetime_names
)]
fn get_all_zero_copy<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let pairs = match store.get_all() {
        Ok(p) => p,
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };
    drop(store);

    if pairs.is_empty() {
        let empty: Vec<Term> = Vec::new();
        return Ok((atoms::ok(), empty).encode(env));
    }

    let buffer = ResourceArc::new(BulkKvBuffer { pairs });
    let len = buffer.pairs.len();
    let mut terms = Vec::with_capacity(len);
    for i in 0..len {
        let kbin = buffer.make_binary(env, |buf| &buf.pairs[i].0);
        let vbin = buffer.make_binary(env, |buf| &buf.pairs[i].1);
        terms.push((kbin, vbin).encode(env));
    }
    Ok((atoms::ok(), terms).encode(env))
}

/// Zero-copy get_batch: returns `{:ok, [value | nil, ...]}` where every non-nil
/// value binary points directly into a Rust-owned `BulkOptValBuffer` resource.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_batch_zero_copy<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    keys: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let key_slices: Vec<&[u8]> = keys.iter().map(Binary::as_slice).collect();

    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let results = match store.get_batch(&key_slices) {
        Ok(r) => r,
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };
    drop(store);

    let buffer = ResourceArc::new(BulkOptValBuffer { results });
    let len = buffer.results.len();
    let mut terms = Vec::with_capacity(len);
    for i in 0..len {
        let term = match buffer.results[i] {
            Some(ref _v) => buffer
                .make_binary(env, |buf| buf.results[i].as_deref().unwrap())
                .encode(env),
            None => atoms::nil().encode(env),
        };
        terms.push(term);
    }
    Ok((atoms::ok(), terms).encode(env))
}

/// Zero-copy get_range: returns `{:ok, [{key, value}, ...]}` where every key
/// and value binary points directly into a Rust-owned `BulkKvBuffer` resource.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_range_zero_copy<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    min_key: Binary<'a>,
    max_key: Binary<'a>,
    max_count: u64,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let pairs = match store.get_range(min_key.as_slice(), max_key.as_slice(), max_count as usize) {
        Ok(p) => p,
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };
    drop(store);

    if pairs.is_empty() {
        let empty: Vec<Term> = Vec::new();
        return Ok((atoms::ok(), empty).encode(env));
    }

    let buffer = ResourceArc::new(BulkKvBuffer { pairs });
    let len = buffer.pairs.len();
    let mut terms = Vec::with_capacity(len);
    for i in 0..len {
        let kbin = buffer.make_binary(env, |buf| &buf.pairs[i].0);
        let vbin = buffer.make_binary(env, |buf| &buf.pairs[i].1);
        terms.push((kbin, vbin).encode(env));
    }
    Ok((atoms::ok(), terms).encode(env))
}

#[cfg(test)]
mod bulk_buffer_tests {
    use super::*;

    #[test]
    fn bulk_kv_buffer_holds_pairs() {
        let buf = BulkKvBuffer {
            pairs: vec![
                (b"key1".to_vec(), b"val1".to_vec()),
                (b"key2".to_vec(), b"val2".to_vec()),
                (b"key3".to_vec(), b"val3".to_vec()),
            ],
        };
        assert_eq!(buf.pairs.len(), 3);
        assert_eq!(&buf.pairs[0].0, b"key1");
        assert_eq!(&buf.pairs[0].1, b"val1");
        assert_eq!(&buf.pairs[2].0, b"key3");
        assert_eq!(&buf.pairs[2].1, b"val3");
    }

    #[test]
    fn bulk_kv_buffer_empty() {
        let buf = BulkKvBuffer { pairs: Vec::new() };
        assert!(buf.pairs.is_empty());
    }

    #[test]
    fn bulk_kv_buffer_large_values() {
        let large_val = vec![0xABu8; 100_000];
        let buf = BulkKvBuffer {
            pairs: vec![
                (b"k1".to_vec(), large_val.clone()),
                (b"k2".to_vec(), large_val.clone()),
            ],
        };
        assert_eq!(buf.pairs[0].1.len(), 100_000);
        assert_eq!(buf.pairs[1].1.len(), 100_000);
        assert_eq!(buf.pairs[0].1[0], 0xAB);
    }

    #[test]
    fn bulk_kv_buffer_pointer_stability() {
        let buf = BulkKvBuffer {
            pairs: vec![
                (b"key1".to_vec(), b"val1".to_vec()),
                (b"key2".to_vec(), b"val2".to_vec()),
            ],
        };
        let ptr0_k = buf.pairs[0].0.as_ptr();
        let ptr0_v = buf.pairs[0].1.as_ptr();
        let ptr1_k = buf.pairs[1].0.as_ptr();
        let ptr1_v = buf.pairs[1].1.as_ptr();
        assert_eq!(buf.pairs[0].0.as_ptr(), ptr0_k);
        assert_eq!(buf.pairs[0].1.as_ptr(), ptr0_v);
        assert_eq!(buf.pairs[1].0.as_ptr(), ptr1_k);
        assert_eq!(buf.pairs[1].1.as_ptr(), ptr1_v);
    }

    #[test]
    fn bulk_kv_buffer_binary_data() {
        let buf = BulkKvBuffer {
            pairs: vec![(vec![0, 1, 2, 3], vec![0xFF, 0xFE, 0x00, 0x01])],
        };
        assert_eq!(&buf.pairs[0].0, &[0, 1, 2, 3]);
        assert_eq!(&buf.pairs[0].1, &[0xFF, 0xFE, 0x00, 0x01]);
    }

    #[test]
    fn bulk_opt_val_buffer_holds_results() {
        let buf = BulkOptValBuffer {
            results: vec![Some(b"val1".to_vec()), None, Some(b"val3".to_vec()), None],
        };
        assert_eq!(buf.results.len(), 4);
        assert_eq!(buf.results[0].as_ref().unwrap(), b"val1");
        assert!(buf.results[1].is_none());
        assert_eq!(buf.results[2].as_ref().unwrap(), b"val3");
        assert!(buf.results[3].is_none());
    }

    #[test]
    fn bulk_opt_val_buffer_empty() {
        let buf = BulkOptValBuffer {
            results: Vec::new(),
        };
        assert!(buf.results.is_empty());
    }

    #[test]
    fn bulk_opt_val_buffer_all_none() {
        let buf = BulkOptValBuffer {
            results: vec![None, None, None],
        };
        assert!(buf.results.iter().all(Option::is_none));
    }

    #[test]
    fn bulk_opt_val_buffer_all_some() {
        let buf = BulkOptValBuffer {
            results: vec![
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"c".to_vec()),
            ],
        };
        assert!(buf.results.iter().all(Option::is_some));
    }

    #[test]
    fn bulk_opt_val_buffer_large_values() {
        let large = vec![0x42u8; 100_000];
        let buf = BulkOptValBuffer {
            results: vec![Some(large.clone()), None, Some(large)],
        };
        assert_eq!(buf.results[0].as_ref().unwrap().len(), 100_000);
        assert!(buf.results[1].is_none());
        assert_eq!(buf.results[2].as_ref().unwrap().len(), 100_000);
    }

    #[test]
    fn bulk_opt_val_buffer_pointer_stability() {
        let buf = BulkOptValBuffer {
            results: vec![Some(b"val1".to_vec()), Some(b"val2".to_vec())],
        };
        let ptr0 = buf.results[0].as_ref().unwrap().as_ptr();
        let ptr1 = buf.results[1].as_ref().unwrap().as_ptr();
        assert_eq!(buf.results[0].as_ref().unwrap().as_ptr(), ptr0);
        assert_eq!(buf.results[1].as_ref().unwrap().as_ptr(), ptr1);
    }

    #[test]
    fn bulk_kv_buffer_many_pairs() {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000)
            .map(|i| (format!("key_{i:06}").into_bytes(), vec![0xAAu8; 1024]))
            .collect();
        let buf = BulkKvBuffer { pairs };
        assert_eq!(buf.pairs.len(), 10_000);
        assert_eq!(&buf.pairs[0].0, b"key_000000");
        assert_eq!(&buf.pairs[9999].0, b"key_009999");
        assert_eq!(buf.pairs[5000].1.len(), 1024);
    }
}

// ===========================================================================
// Tokio async IO NIFs
// ===========================================================================

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_async<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let pid: LocalPid = env.pid();
    let key_bytes = key.as_slice().to_vec();
    let store_clone = resource.clone();
    async_io::runtime().spawn(async move {
        let result = {
            let mut store = store_clone.store.lock().unwrap();
            store.get(&key_bytes)
        };
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&pid, |env| match result {
            Ok(Some(value)) => match OwnedBinary::new(value.len()) {
                Some(mut bin) => {
                    bin.as_mut_slice().copy_from_slice(&value);
                    (
                        atoms::tokio_complete(),
                        atoms::ok(),
                        Binary::from_owned(bin, env),
                    )
                        .encode(env)
                }
                None => (atoms::tokio_complete(), atoms::error(), "alloc_failed").encode(env),
            },
            Ok(None) => (atoms::tokio_complete(), atoms::ok(), atoms::nil()).encode(env),
            Err(e) => (atoms::tokio_complete(), atoms::error(), e.to_string()).encode(env),
        });
    });
    Ok((atoms::pending(), atoms::ok()).encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn delete_async<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let pid: LocalPid = env.pid();
    let key_bytes = key.as_slice().to_vec();
    let store_clone = resource.clone();
    async_io::runtime().spawn(async move {
        let result = {
            let mut store = store_clone.store.lock().unwrap();
            store.delete(&key_bytes)
        };
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&pid, |env| match result {
            Ok(true) => (atoms::tokio_complete(), atoms::ok(), atoms::true_()).encode(env),
            Ok(false) => (atoms::tokio_complete(), atoms::ok(), atoms::false_()).encode(env),
            Err(e) => (atoms::tokio_complete(), atoms::error(), e.to_string()).encode(env),
        });
    });
    Ok((atoms::pending(), atoms::ok()).encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put_batch_tokio_async<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    batch: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    for (k, v, _) in &batch {
        if let Err(msg) = crate::log::validate_kv_sizes(k.as_slice(), v.as_slice()) {
            return Ok((atoms::error(), msg).encode(env));
        }
    }
    let pid: LocalPid = env.pid();
    let owned_batch: Vec<(Vec<u8>, Vec<u8>, u64)> = batch
        .iter()
        .map(|(k, v, exp)| (k.as_slice().to_vec(), v.as_slice().to_vec(), *exp))
        .collect();
    let store_clone = resource.clone();
    async_io::runtime().spawn(async move {
        let result = {
            let mut store = store_clone.store.lock().unwrap();
            let entries: Vec<(&[u8], &[u8], u64)> = owned_batch
                .iter()
                .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
                .collect();
            store.put_batch(&entries)
        };
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&pid, |env| match result {
            Ok(()) => (atoms::tokio_complete(), atoms::ok(), atoms::ok()).encode(env),
            Err(e) => (atoms::tokio_complete(), atoms::error(), e.to_string()).encode(env),
        });
    });
    Ok((atoms::pending(), atoms::ok()).encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn write_hint_async<'a>(env: Env<'a>, resource: ResourceArc<StoreResource>) -> NifResult<Term<'a>> {
    let pid: LocalPid = env.pid();
    let store_clone = resource.clone();
    async_io::runtime().spawn(async move {
        let result = {
            let mut store = store_clone.store.lock().unwrap();
            store.write_hint_file()
        };
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&pid, |env| match result {
            Ok(()) => (atoms::tokio_complete(), atoms::ok(), atoms::ok()).encode(env),
            Err(e) => (atoms::tokio_complete(), atoms::error(), e.to_string()).encode(env),
        });
    });
    Ok((atoms::pending(), atoms::ok()).encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn purge_expired_async<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
) -> NifResult<Term<'a>> {
    let pid: LocalPid = env.pid();
    let store_clone = resource.clone();
    async_io::runtime().spawn(async move {
        let result = {
            let mut store = store_clone.store.lock().unwrap();
            store.purge_expired()
        };
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&pid, |env| match result {
            Ok(count) => (atoms::tokio_complete(), atoms::ok(), count as u64).encode(env),
            Err(e) => (atoms::tokio_complete(), atoms::error(), e.to_string()).encode(env),
        });
    });
    Ok((atoms::pending(), atoms::ok()).encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn run_compaction_async<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    file_ids: Vec<u64>,
) -> NifResult<Term<'a>> {
    let pid: LocalPid = env.pid();
    let store_clone = resource.clone();
    async_io::runtime().spawn(async move {
        let result = {
            let mut store = store_clone.store.lock().unwrap();
            store.run_compaction(&file_ids)
        };
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&pid, |env| match result {
            Ok((written, dropped, reclaimed)) => (
                atoms::tokio_complete(),
                atoms::ok(),
                (written, dropped, reclaimed),
            )
                .encode(env),
            Err(e) => (atoms::tokio_complete(), atoms::error(), e.to_string()).encode(env),
        });
    });
    Ok((atoms::pending(), atoms::ok()).encode(env))
}

rustler::init!("Elixir.Ferricstore.Bitcask.NIF", load = load);
