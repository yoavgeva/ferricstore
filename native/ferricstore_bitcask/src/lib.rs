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

pub mod compaction;
pub mod hint;
pub mod io_backend;
pub mod keydir;
pub mod log;
pub mod store;

use rustler::schedule::consume_timeslice;
use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, ResourceArc, Term};
use std::sync::Mutex;

#[cfg(target_os = "linux")]
use rustler::LocalPid;
use std::sync::atomic::AtomicU64;
#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;

use store::Store;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        true_ = "true",
        false_ = "false",
        pending,
        io_complete,
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

/// Number of items to encode per yielding NIF timeslice before checking
/// whether the BEAM scheduler wants us to yield.
const YIELD_CHUNK_SIZE: usize = 500;

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
    let _ = rustler::resource!(KeysIterState, env);
    let _ = rustler::resource!(GetAllIterState, env);
    let _ = rustler::resource!(GetBatchIterState, env);
    let _ = rustler::resource!(GetRangeIterState, env);
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
//    list from argv. Encodes up to YIELD_CHUNK_SIZE items into BEAM
//    terms and prepends them to the partial list. Calls
//    `consume_timeslice`. If timeslice exhausted and more work remains,
//    reschedules itself. When done, reverses the list and returns.
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
    let chunk_end = (*idx + YIELD_CHUNK_SIZE).min(total);

    // Build up the list by consing new elements onto the front.
    // We iterate in reverse so the final reversed list is in the original order.
    let mut acc = partial_list;
    for k in &state.keys[*idx..chunk_end] {
        if let Some(term) = encode_key_term(env, k) {
            acc = acc.list_prepend(term);
        } else {
            return rustler::codegen_runtime::NifReturned::BadArg.apply(env);
        }
    }
    *idx = chunk_end;

    if *idx < total {
        let pct = ((*idx as f64 / total as f64) * 100.0) as i32;
        let _ = consume_timeslice(env, pct.clamp(1, 100));
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
    let chunk_end = (*idx + YIELD_CHUNK_SIZE).min(total);

    let mut acc = partial_list;
    for (k, v) in &state.pairs[*idx..chunk_end] {
        if let Some(term) = encode_kv_term(env, k, v) {
            acc = acc.list_prepend(term);
        } else {
            return rustler::codegen_runtime::NifReturned::BadArg.apply(env);
        }
    }
    *idx = chunk_end;

    if *idx < total {
        let pct = ((*idx as f64 / total as f64) * 100.0) as i32;
        let _ = consume_timeslice(env, pct.clamp(1, 100));
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
    let chunk_end = (*idx + YIELD_CHUNK_SIZE).min(total);

    let mut acc = partial_list;
    for opt in &state.results[*idx..chunk_end] {
        let term = encode_opt_val_term(env, opt);
        acc = acc.list_prepend(term);
    }
    *idx = chunk_end;

    if *idx < total {
        let pct = ((*idx as f64 / total as f64) * 100.0) as i32;
        let _ = consume_timeslice(env, pct.clamp(1, 100));
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
    let chunk_end = (*idx + YIELD_CHUNK_SIZE).min(total);

    let mut acc = partial_list;
    for (k, v) in &state.pairs[*idx..chunk_end] {
        if let Some(term) = encode_kv_term(env, k, v) {
            acc = acc.list_prepend(term);
        } else {
            return rustler::codegen_runtime::NifReturned::BadArg.apply(env);
        }
    }
    *idx = chunk_end;

    if *idx < total {
        let pct = ((*idx as f64 / total as f64) * 100.0) as i32;
        let _ = consume_timeslice(env, pct.clamp(1, 100));
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

rustler::init!("Elixir.Ferricstore.Bitcask.NIF", load = load);
