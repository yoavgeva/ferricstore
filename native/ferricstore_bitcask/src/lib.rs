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
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
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

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(StoreResource, env);
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

/// Return all live keys as a list of binaries.
///
/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Iterates the in-memory keydir (no disk
/// I/O). Uses `enif_consume_timeslice` to report progress to the BEAM
/// scheduler every 100 keys, allowing the scheduler to remain aware of how
/// much work is being done. Full cooperative yielding via `enif_schedule_nif`
/// is not used because Rustler does not expose it and saving/restoring the
/// iteration state across NIF re-entries would require complex continuation
/// state management (the keydir iterator, partial result list, and Mutex
/// guard would all need to be preserved across yield points).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn keys(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let raw_keys = store.keys();
    let total = raw_keys.len();
    let mut all_keys = Vec::with_capacity(total);
    for (i, k) in raw_keys.into_iter().enumerate() {
        let mut bin = OwnedBinary::new(k.len()).ok_or(rustler::Error::BadArg)?;
        bin.as_mut_slice().copy_from_slice(&k);
        all_keys.push(Binary::from_owned(bin, env).encode(env));
        // Report progress to BEAM scheduler every 100 keys
        if i % 100 == 99 && total > 0 {
            let pct = ((i + 1) as f64 / total as f64 * 100.0) as i32;
            let _ = consume_timeslice(env, pct.clamp(1, 100));
        }
    }
    Ok(all_keys.encode(env))
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

// ---------------------------------------------------------------------------
// Extended NIF functions
// ---------------------------------------------------------------------------

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Iterates all keys doing one pread per
/// entry. Uses `enif_consume_timeslice` to report progress to the BEAM
/// scheduler every 100 entries. Full cooperative yielding via
/// `enif_schedule_nif` is not used because the iteration holds a Mutex guard
/// and opens file readers per entry -- saving/restoring this state across
/// yield points is not feasible without restructuring the Store API.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_all(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.get_all() {
        Ok(pairs) => {
            let total = pairs.len();
            let mut terms = Vec::with_capacity(total);
            for (i, (k, v)) in pairs.into_iter().enumerate() {
                let mut kbin = OwnedBinary::new(k.len()).ok_or(rustler::Error::BadArg)?;
                kbin.as_mut_slice().copy_from_slice(&k);
                let mut vbin = OwnedBinary::new(v.len()).ok_or(rustler::Error::BadArg)?;
                vbin.as_mut_slice().copy_from_slice(&v);
                terms.push(
                    (Binary::from_owned(kbin, env), Binary::from_owned(vbin, env)).encode(env),
                );
                if i % 100 == 99 && total > 0 {
                    let pct = ((i + 1) as f64 / total as f64 * 100.0) as i32;
                    let _ = consume_timeslice(env, pct.clamp(1, 100));
                }
            }
            Ok((atoms::ok(), terms).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Batch reads are serialized by the shard
/// GenServer. Uses `enif_consume_timeslice` to report progress every 100
/// entries. Full yielding via `enif_schedule_nif` is not used because the
/// batch holds a Mutex guard and the per-key file reads make
/// save/restore across yield points impractical.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_batch<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    keys: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    let key_slices: Vec<&[u8]> = keys.iter().map(Binary::as_slice).collect();
    match store.get_batch(&key_slices) {
        Ok(results) => {
            let total = results.len();
            let mut terms = Vec::with_capacity(total);
            for (i, opt) in results.into_iter().enumerate() {
                match opt {
                    Some(v) => {
                        let mut bin = OwnedBinary::new(v.len()).ok_or(rustler::Error::BadArg)?;
                        bin.as_mut_slice().copy_from_slice(&v);
                        terms.push(Binary::from_owned(bin, env).encode(env));
                    }
                    None => terms.push(atoms::nil().encode(env)),
                }
                if i % 100 == 99 && total > 0 {
                    let pct = ((i + 1) as f64 / total as f64 * 100.0) as i32;
                    let _ = consume_timeslice(env, pct.clamp(1, 100));
                }
            }
            Ok((atoms::ok(), terms).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// ## Scheduler contract
///
/// Runs on a Normal BEAM scheduler. Range scans iterate the sorted keydir and
/// do one pread per matching key. Uses `enif_consume_timeslice` to report
/// progress every 100 entries. Full yielding via `enif_schedule_nif` is not
/// used because the range iteration holds a Mutex guard and opens file readers
/// per entry -- save/restore across yield points is not feasible.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get_range<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    min_key: Binary<'a>,
    max_key: Binary<'a>,
    max_count: u64,
) -> NifResult<Term<'a>> {
    let mut store = resource.store.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.get_range(min_key.as_slice(), max_key.as_slice(), max_count as usize) {
        Ok(pairs) => {
            let total = pairs.len();
            let mut terms = Vec::with_capacity(total);
            for (i, (k, v)) in pairs.into_iter().enumerate() {
                let mut kbin = OwnedBinary::new(k.len()).ok_or(rustler::Error::BadArg)?;
                kbin.as_mut_slice().copy_from_slice(&k);
                let mut vbin = OwnedBinary::new(v.len()).ok_or(rustler::Error::BadArg)?;
                vbin.as_mut_slice().copy_from_slice(&v);
                terms.push(
                    (Binary::from_owned(kbin, env), Binary::from_owned(vbin, env)).encode(env),
                );
                if i % 100 == 99 && total > 0 {
                    let pct = ((i + 1) as f64 / total as f64 * 100.0) as i32;
                    let _ = consume_timeslice(env, pct.clamp(1, 100));
                }
            }
            Ok((atoms::ok(), terms).encode(env))
        }
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
