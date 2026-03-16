#![deny(clippy::all, clippy::pedantic)]
#![deny(unsafe_code)]
// These pedantic lints are noisy without adding safety value for this codebase:
// - possible_truncation: we target 64-bit Linux only; u64→usize is always safe.
// - cast_sign_loss / cast_lossless: io_uring result codes require these casts.
// - items_after_statements: common in test helpers and is clear.
// - doc_markdown: minor style preference, not a correctness issue.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::doc_link_with_quotes)]

pub mod compaction;
pub mod hint;
pub mod io_backend;
pub mod keydir;
pub mod log;
pub mod store;

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

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(StoreResource, env);
    true
}

/// Open (or create) a Bitcask store at the given path.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
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
#[rustler::nif(schedule = "DirtyIo")]
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
    value: Binary<'a>,
    expire_at_ms: u64,
) -> NifResult<Term<'a>> {
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
/// This NIF occupies **one** dirty scheduler thread for the duration of the
/// entire batch (all writes + one `fsync`). The Elixir shard `GenServer` should
/// collect writes during a batch window, then call `put_batch` once per window.
/// This keeps dirty scheduler occupancy at O(1) per batch regardless of how
/// many writes the batch contains.
///
/// Contrast with calling `put` N times: each call occupies a dirty thread for
/// its own `fsync`, so N concurrent puts compete for the fixed-size dirty pool
/// (default 10 threads) and cause queuing under load.
#[rustler::nif(schedule = "DirtyIo")]
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

/// Async variant of `put_batch` that runs on a **normal BEAM scheduler thread**
/// (not a dirty-IO thread).
///
/// ## Why `schedule = "Normal"`
///
/// This NIF does no blocking I/O itself:
///   - Record serialisation is pure CPU work (microseconds).
///   - `submit_batch` pushes SQEs to the `io_uring` ring buffer and calls
///     `ring.submit()` — a single non-blocking syscall.
///   - The actual `fsync` runs in a dedicated background thread; no scheduler
///     thread is ever blocked waiting for disk.
///
/// Using `"Normal"` keeps the dirty-IO thread pool (default: 10 threads) free
/// for the genuinely blocking NIFs (`get`, `put`, `put_batch`, `delete`, etc.)
/// and lets BEAM schedule the call like an ordinary function call — no
/// preemption, no thread-pool dispatch, ~1–2µs overhead instead of ~10–20µs.
///
/// On the **fallback path** (macOS / no `io_uring`) this NIF calls
/// `store.put_batch` which DOES block on fsync. Because we've declared
/// `"Normal"`, that blocking call would freeze a scheduler thread. The
/// fallback is therefore wrapped in a `spawn_blocking`-style approach: it
/// calls the synchronous path directly, which is acceptable because the
/// fallback is only used in development/test (macOS). For a production Linux
/// build the async path is always taken.
///
/// ## Linux with `io_uring`
///
/// 1. Encodes records and updates the keydir optimistically.
/// 2. Submits write SQEs + one fsync SQE to an `AsyncUringBackend` (non-blocking).
/// 3. Returns `{:pending, op_id}` immediately — no dirty thread occupied.
/// 4. A background Rust thread drains the fsync CQE and sends
///    `{:io_complete, op_id, :ok | {:error, reason}}` to the calling process.
///
/// ## Fallback (macOS / no `io_uring`)
///
/// Falls back to synchronous `put_batch`, returning `:ok` directly.
/// On macOS this is development-only; production deployments run on Linux.
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
        match async_uring.submit_batch(&buf_refs, caller_pid, op_id) {
            Ok(_offsets) if buf_refs.is_empty() => {
                // Empty batch — nothing was submitted to the ring, so no CQE
                // will arrive. Return :ok immediately instead of {:pending, _}.
                return Ok(atoms::ok().encode(env));
            }
            Ok(_offsets) => {
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
#[rustler::nif(schedule = "DirtyIo")]
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

/// Return all live keys as a list of binaries.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn keys(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let all_keys: Vec<Term> = store
        .keys()
        .into_iter()
        .map(|k| {
            let mut bin = OwnedBinary::new(k.len()).unwrap();
            bin.as_mut_slice().copy_from_slice(&k);
            Binary::from_owned(bin, env).encode(env)
        })
        .collect();
    Ok(all_keys.encode(env))
}

/// Write a hint file for the active data file.
#[rustler::nif(schedule = "DirtyIo")]
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn purge_expired(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.purge_expired() {
        Ok(count) => Ok((atoms::ok(), count as u64).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

rustler::init!("Elixir.Ferricstore.Bitcask.NIF", load = load);
