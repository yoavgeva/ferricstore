// ferricstore_wal_nif — Rust NIF WAL I/O layer for ra_log_wal
//
// All NIF functions run on normal BEAM schedulers (<1μs each).
// Blocking I/O (write + fdatasync) runs on a dedicated background thread.
//
// Architecture:
//   NIF calls → Mutex<AlignedBuffer> (shared) → FlushRequest channel → Background thread
//   Background thread: commit_delay → write() → fdatasync() → notify caller

#![allow(clippy::needless_pass_by_value)] // Rustler NIF convention

mod aligned_buffer;
mod background_thread;
mod wal_handle;

#[cfg(test)]
mod tests;

use rustler::{Atom, Binary, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term, Encoder, Resource};
use std::ops::Deref;
use wal_handle::WalHandle;

// Implement Resource trait for WalHandle
impl Resource for WalHandle {}

mod atoms {
    rustler::atoms! {
        ok,
        error,
        wal_sync_complete,
        wal_sync_error,
        wal_thread_dead,
        backpressure,
        closed,
        timeout,
    }
}

// ---------------------------------------------------------------------------
// NIF Functions
// ---------------------------------------------------------------------------

/// Open a WAL file. Spawns background I/O thread.
/// commit_delay_us: microseconds to wait before fdatasync (default 200)
/// pre_allocate_bytes: fallocate size (default 256MB)
/// max_buffer_bytes: backpressure limit (default 64MB)
#[rustler::nif]
fn open(
    path: String,
    commit_delay_us: u64,
    pre_allocate_bytes: u64,
    max_buffer_bytes: u64,
) -> NifResult<(Atom, ResourceArc<WalHandle>)> {
    match WalHandle::open(path, commit_delay_us, pre_allocate_bytes, max_buffer_bytes) {
        Ok(handle) => Ok((atoms::ok(), ResourceArc::new(handle))),
        Err(e) => Err(rustler::Error::Term(Box::new(format!("{e}")))),
    }
}

/// Write pre-formatted iodata to the WAL buffer.
/// Copies bytes into the shared aligned buffer. Does NOT write to disk.
/// Returns :ok | {:error, :wal_thread_dead} | {:error, :backpressure}
#[rustler::nif]
fn write(handle: ResourceArc<WalHandle>, iodata: Term) -> NifResult<Atom> {
    handle.check_alive()?;

    // Collect iodata into bytes
    let bytes = iodata_to_bytes(iodata)?;

    handle.buffer_write(&bytes)?;
    Ok(atoms::ok())
}

/// Request async fdatasync. Background thread will flush buffer to disk,
/// fdatasync, and send {wal_sync_complete, Ref} to CallerPid.
/// Returns :ok immediately.
#[rustler::nif]
#[allow(unused_variables)]
fn sync(env: Env, handle: ResourceArc<WalHandle>, caller_pid: LocalPid, ref_term: Term<'_>) -> NifResult<Atom> {
    handle.check_alive()?;

    // Save the ref in an OwnedEnv so it survives past this NIF call
    let owned_env = rustler::OwnedEnv::new();
    let saved_ref = owned_env.save(ref_term);

    handle.request_sync(caller_pid, owned_env, saved_ref)?;
    Ok(atoms::ok())
}

/// Close the WAL file. Blocks until background thread drains, syncs, and exits.
/// Timeout: 30 seconds.
#[rustler::nif]
fn close(handle: ResourceArc<WalHandle>) -> NifResult<Atom> {
    match handle.close() {
        Ok(()) => Ok(atoms::ok()),
        Err(e) => Err(rustler::Error::Term(Box::new(format!("{e}")))),
    }
}

/// Returns current logical file size in bytes. No syscall — reads atomic.
#[rustler::nif]
fn position(handle: ResourceArc<WalHandle>) -> NifResult<(Atom, u64)> {
    Ok((atoms::ok(), handle.file_size()))
}

/// Returns profiling counters as a list of {key, value} tuples.
/// Keys: flush_count, callers_per_flush_avg, bytes_written, sync_latency_us_avg,
///       sync_latency_us_max, write_calls, sync_calls
#[rustler::nif]
fn stats(handle: ResourceArc<WalHandle>) -> Vec<(String, u64)> {
    use std::sync::atomic::Ordering::Relaxed;
    let s = &handle.stats;
    let flush_count = s.flush_count.load(Relaxed);
    let callers = s.callers_notified.load(Relaxed);
    let bytes = s.bytes_written.load(Relaxed);
    let sync_total = s.sync_latency_us_total.load(Relaxed);
    let sync_max = s.sync_latency_us_max.load(Relaxed);
    let writes = s.write_calls.load(Relaxed);
    let syncs = s.sync_calls.load(Relaxed);

    let avg_callers = if flush_count > 0 { callers / flush_count } else { 0 };
    let avg_sync_us = if flush_count > 0 { sync_total / flush_count } else { 0 };

    vec![
        ("flush_count".to_string(), flush_count),
        ("callers_per_flush_avg".to_string(), avg_callers),
        ("bytes_written".to_string(), bytes),
        ("sync_latency_us_avg".to_string(), avg_sync_us),
        ("sync_latency_us_max".to_string(), sync_max),
        ("write_calls".to_string(), writes),
        ("sync_calls".to_string(), syncs),
    ]
}

/// Read bytes from WAL at offset. Used during recovery.
#[rustler::nif]
fn pread<'a>(env: Env<'a>, handle: ResourceArc<WalHandle>, offset: u64, len: u64) -> NifResult<(Atom, Binary<'a>)> {
    let data = handle.pread(offset, len)?;
    let mut binary = OwnedBinary::new(data.len()).ok_or(rustler::Error::Term(Box::new("alloc_failed")))?;
    binary.as_mut_slice().copy_from_slice(&data);
    Ok((atoms::ok(), binary.release(env)))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert Erlang iodata (binary or iolist) to bytes.
fn iodata_to_bytes(term: Term) -> NifResult<Vec<u8>> {
    // Try as binary first (fast path)
    if let Ok(bin) = term.decode::<Binary>() {
        return Ok(bin.as_slice().to_vec());
    }

    // iolist: flatten recursively
    let mut result = Vec::new();
    flatten_iolist(term, &mut result)?;
    Ok(result)
}

fn flatten_iolist(term: Term, out: &mut Vec<u8>) -> NifResult<()> {
    if let Ok(bin) = term.decode::<Binary>() {
        out.extend_from_slice(bin.as_slice());
    } else if let Ok(items) = term.decode::<Vec<Term>>() {
        for item in items {
            flatten_iolist(item, out)?;
        }
    } else if let Ok(byte) = term.decode::<u8>() {
        out.push(byte);
    } else {
        return Err(rustler::Error::BadArg);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// NIF Registration
// ---------------------------------------------------------------------------

rustler::init!("ferricstore_wal_nif");
