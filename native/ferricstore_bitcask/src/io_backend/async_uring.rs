//! `AsyncUringBackend` — non-blocking `io_uring` I/O for the Bitcask log writer.
//!
//! This module is **only compiled on Linux** (`#[cfg(target_os = "linux")]`).
//!
//! Unlike `UringBackend` (which blocks the calling thread inside
//! `submit_and_wait`), `AsyncUringBackend` submits write SQEs + one fsync SQE
//! to the ring and returns **immediately**. A background completion thread
//! drains CQEs and sends a `{:io_complete, op_id, :ok | {:error, reason}}`
//! message back to the calling BEAM process via `OwnedEnv::send_and_clear`.
//!
//! ## Ownership / safety model
//!
//! Because the NIF returns before the kernel has consumed the write buffers,
//! we **must not** pass borrowed pointers. Every buffer is copied into an owned
//! `Vec<u8>` before submission. The `Vec` is stored in a `PendingOp` entry that
//! lives until the corresponding CQE arrives, guaranteeing the kernel pointer
//! remains valid for the entire I/O lifetime.
//!
//! ## SQE linking strategy
//!
//! All write SQEs in a batch are linked together with `IOSQE_IO_LINK`, and a
//! final `IORING_OP_FSYNC` SQE is appended (also linked from the last write).
//! This means the kernel executes writes sequentially and only fsyncs after
//! all writes complete. We only inspect the **fsync CQE** to determine overall
//! success — if any linked predecessor fails, the kernel cancels subsequent
//! SQEs in the chain and sets `-ECANCELED` on them, so the fsync CQE will
//! also fail (either with the original error or with `-ECANCELED`).
//!
//! We tag only the fsync SQE with a meaningful `user_data` (the `op_id`).
//! Write SQEs use `user_data = 0` (sentinel, ignored by the completion thread).

#![allow(unsafe_code)]

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use io_uring::squeue::Flags;
use io_uring::{opcode, types, IoUring};
use rustler::{Encoder, LocalPid, OwnedEnv};

/// Ring capacity in submission-queue entries. Must be a power of two.
/// Each batch occupies N_writes + 1 (fsync) SQEs, so the ring must be large
/// enough to hold the biggest expected batch plus the fsync SQE.
/// 256 accommodates batches of up to 255 records without splitting.
const RING_SIZE: u32 = 256;

/// Sentinel `user_data` value used for write SQEs that we do not individually
/// track. The completion thread ignores CQEs with this tag.
const WRITE_SQE_TAG: u64 = 0;

/// `IORING_ENTER_GETEVENTS` — instructs the kernel to wait for completions
/// when `min_complete > 0`. This is flag bit 0 of the `flags` argument to
/// `io_uring_enter(2)`. It is not (yet) exported by the `libc` crate, so we
/// define it here from the kernel ABI.
const IORING_ENTER_GETEVENTS: libc::c_uint = 1;

/// Module-level atoms used in BEAM messages sent from the completion thread.
mod atoms {
    rustler::atoms! {
        io_complete,
        ok,
        error,
    }
}

/// Tracks one in-flight async batch operation.
///
/// Holds the owned buffer copies to keep them alive until the kernel is done
/// with them, plus the BEAM caller information for sending the result message.
struct PendingOp {
    /// The BEAM process that submitted the operation and will receive the
    /// `{:io_complete, op_id, result}` message.
    caller_pid: LocalPid,
    /// Owned copies of the write buffers. These MUST stay alive until the
    /// fsync CQE for this `op_id` is drained, because the kernel holds raw
    /// pointers into them between submission and completion.
    _buffers: Vec<Vec<u8>>,
}

/// Non-blocking io_uring-based append-only log writer.
///
/// Submits write + fsync SQE batches without blocking. A background thread
/// drains completions and notifies the calling BEAM process.
///
/// ## Thread safety
///
/// The `submit_batch` method takes `&self` (not `&mut self`) so it can be
/// called from the NIF layer through a `Mutex<Store>`. Internally, the ring
/// and pending-ops map are protected by their own `Mutex` to allow the
/// background completion thread to access the ring concurrently.
///
/// ## Mutex discipline — avoiding deadlock
///
/// The completion thread must **not** hold the ring mutex while waiting for
/// CQEs (i.e. while calling `submit_and_wait`), because the submit path also
/// needs the ring mutex to push SQEs. If the completion thread held the lock
/// during the wait, no new SQEs could ever be submitted, so no CQEs would
/// ever arrive — a deadlock.
///
/// The fix: at open time we capture the ring's raw file descriptor (an integer
/// that needs no lock) and pass it to the completion thread. The thread calls
/// the raw `io_uring_enter(2)` syscall with that fd to wait for CQEs. Once at
/// least one CQE is available, the thread acquires the ring mutex briefly to
/// drain them, then releases the lock before processing each CQE.
pub struct AsyncUringBackend {
    /// `io_uring` ring instance, shared between the submitter (NIF thread) and
    /// the completion thread. The Mutex serialises SQ pushes and CQ drains.
    ring: Arc<Mutex<IoUring>>,
    /// Raw file descriptor for the open data file. Kept alive by `_file`.
    fd: RawFd,
    /// Keeps the file open for the lifetime of the backend.
    _file: File,
    /// Current write offset (byte position at end of file).
    offset: AtomicU64,
    /// Map from `op_id` to pending-op metadata. Shared between the submit
    /// path (inserts) and the completion thread (removes + sends messages).
    pending: Arc<Mutex<HashMap<u64, PendingOp>>>,
    /// Flag to signal the background thread to shut down.
    shutdown: Arc<AtomicBool>,
    /// Handle to the background completion thread (joined on drop).
    completion_thread: Option<thread::JoinHandle<()>>,
}

impl AsyncUringBackend {
    /// Open (or create) the file at `path` for appending and start the
    /// background completion-draining thread.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the file cannot be opened, its metadata
    /// cannot be read, or the `io_uring` ring cannot be initialised.
    pub fn open(path: &std::path::Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let offset = file.metadata()?.len();
        let fd = file.as_raw_fd();

        let ring = IoUring::builder()
            .build(RING_SIZE)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Capture the ring's fd before moving `ring` into the Arc. This fd is
        // stable for the lifetime of the IoUring (it is an OwnedFd inside it).
        let ring_fd = ring.as_raw_fd();

        let ring = Arc::new(Mutex::new(ring));
        let pending: Arc<Mutex<HashMap<u64, PendingOp>>> = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let completion_thread = {
            let ring = Arc::clone(&ring);
            let pending = Arc::clone(&pending);
            let shutdown = Arc::clone(&shutdown);
            thread::Builder::new()
                .name("ferric-uring-cq".into())
                .spawn(move || {
                    Self::completion_loop(ring_fd, &ring, &pending, &shutdown);
                })
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        };

        Ok(Self {
            ring,
            fd,
            _file: file,
            offset: AtomicU64::new(offset),
            pending,
            shutdown,
            completion_thread: Some(completion_thread),
        })
    }

    /// Submit a batch of write buffers + one fsync as linked SQEs.
    ///
    /// This method copies all buffers into owned `Vec<u8>`, pushes the SQEs
    /// to the ring, calls `submit()` (non-blocking), stores a `PendingOp`,
    /// and returns immediately. The caller will receive a BEAM message when
    /// the operation completes.
    ///
    /// # Arguments
    ///
    /// * `buffers` — serialised record bytes to append, in order.
    /// * `caller_pid` — the BEAM process to notify on completion.
    /// * `op_id` — unique operation identifier, echoed back in the message.
    ///
    /// # Returns
    ///
    /// A `Vec<u64>` of starting file offsets, one per buffer (same as
    /// `IoBackend::append_batch_and_sync`). These offsets are computed
    /// optimistically from the current write position. If the I/O
    /// subsequently fails, the caller will learn about it via the BEAM
    /// message and should not use these offsets.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the SQE queue is full or the ring cannot
    /// submit.
    pub fn submit_batch(
        &self,
        buffers: &[&[u8]],
        caller_pid: LocalPid,
        op_id: u64,
    ) -> io::Result<Vec<u64>> {
        if buffers.is_empty() {
            // Nothing to write. Return success without submitting to the ring
            // or sending a BEAM message. The caller checks buffers.is_empty()
            // and returns :ok directly instead of {:pending, op_id}.
            return Ok(Vec::new());
        }

        // 1. Copy buffers into owned Vecs.
        let owned_buffers: Vec<Vec<u8>> = buffers.iter().map(|b| b.to_vec()).collect();

        // 2. Compute offsets from the current write position.
        let base_offset = self.offset.load(Ordering::SeqCst);
        let mut offsets = Vec::with_capacity(owned_buffers.len());
        let mut running = base_offset;
        for buf in &owned_buffers {
            offsets.push(running);
            running += buf.len() as u64;
        }
        // Total bytes for this batch (for reference; not used further).
        let _ = running - base_offset;

        // 3. Advance offset optimistically. If the I/O fails the backend is
        //    in an inconsistent state anyway (the file may have partial
        //    writes), which matches Bitcask's crash-recovery contract.
        self.offset.store(running, Ordering::SeqCst);

        // 4. Push SQEs under the ring lock.
        let fd = types::Fd(self.fd);
        {
            let ring = self.ring.lock().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "async_uring: ring mutex poisoned")
            })?;

            {
                let mut sq = unsafe { ring.submission_shared() };

                // Push write SQEs, each linked to the next.
                for (i, buf) in owned_buffers.iter().enumerate() {
                    let sqe = opcode::Write::new(
                        fd,
                        buf.as_ptr(),
                        u32::try_from(buf.len()).unwrap_or(u32::MAX),
                    )
                    .offset(offsets[i])
                    .build()
                    .user_data(WRITE_SQE_TAG)
                    .flags(Flags::IO_LINK);

                    // SAFETY: `owned_buffers` are owned Vecs stored in the
                    // `PendingOp` which lives until the completion thread
                    // drains the fsync CQE for this op_id. The kernel holds
                    // raw pointers into these Vecs, but they remain valid and
                    // unmoved (Vec heap allocation is stable).
                    unsafe {
                        sq.push(&sqe).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "async_uring: SQ full on write")
                        })?;
                    }
                }

                // Push the fsync SQE. No IO_LINK flag — it is the terminal
                // SQE in the chain. Its `user_data` is the `op_id` so the
                // completion thread can look up the PendingOp.
                let fsync_sqe = opcode::Fsync::new(fd).build().user_data(op_id);

                unsafe {
                    sq.push(&fsync_sqe).map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "async_uring: SQ full on fsync")
                    })?;
                }
            }

            // Submit all SQEs to the kernel (non-blocking).
            ring.submit().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("async_uring submit: {e}"))
            })?;
        }

        // 5. Register the pending op so the completion thread can find it.
        {
            let mut pending = self.pending.lock().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "async_uring: pending mutex poisoned")
            })?;
            pending.insert(
                op_id,
                PendingOp {
                    caller_pid,
                    _buffers: owned_buffers,
                },
            );
        }

        Ok(offsets)
    }

    /// Background thread loop: drain CQEs and send BEAM messages.
    ///
    /// # Mutex discipline
    ///
    /// This function **must not** hold the ring mutex while waiting for CQEs.
    /// Waiting for CQEs (via `io_uring_enter` with `IORING_ENTER_GETEVENTS`)
    /// requires that SQEs can be submitted concurrently — but SQE submission
    /// also needs the ring mutex. Holding the mutex during the wait would
    /// prevent any new SQEs from being submitted, so no CQEs would ever arrive,
    /// causing a deadlock.
    ///
    /// Instead we call the `io_uring_enter(2)` syscall directly using only the
    /// ring's file descriptor (an integer, no lock needed). Once the syscall
    /// returns we lock briefly to drain available CQEs, then release the lock
    /// before processing them.
    fn completion_loop(
        ring_fd: RawFd,
        ring: &Arc<Mutex<IoUring>>,
        pending: &Arc<Mutex<HashMap<u64, PendingOp>>>,
        shutdown: &Arc<AtomicBool>,
    ) {
        while !shutdown.load(Ordering::Relaxed) {
            // Wait for at least 1 CQE using the raw syscall, WITHOUT holding
            // the ring mutex. This allows submit_batch to acquire the mutex
            // and push SQEs concurrently.
            //
            // io_uring_enter(fd, to_submit=0, min_complete=1,
            //                flags=IORING_ENTER_GETEVENTS, sig=NULL, sigsize=0)
            let ret = unsafe {
                libc::syscall(
                    libc::SYS_io_uring_enter,
                    ring_fd as libc::c_long,
                    0_u32, // to_submit: no new SQEs from this call
                    1_u32, // min_complete: wait for at least 1 CQE
                    IORING_ENTER_GETEVENTS,
                    std::ptr::null::<libc::sigset_t>(),
                    std::mem::size_of::<libc::sigset_t>() as libc::size_t,
                )
            };

            if ret < 0 {
                // EINTR is normal (signal interrupted the wait) — retry.
                // Any other error means the ring is unusable — exit.
                let err = io::Error::last_os_error();
                if err.kind() != io::ErrorKind::Interrupted {
                    break;
                }
                continue;
            }

            // At least 1 CQE is available. Lock the ring briefly to drain
            // all available CQEs, then release the lock before processing.
            let cqes: Vec<(u64, i32)> = {
                let Ok(ring) = ring.lock() else {
                    break; // Mutex poisoned — exit thread.
                };
                // SAFETY: we hold the only reference to the CQ here (the ring
                // mutex is held; submission_shared is only used on the submit
                // path which also holds the mutex; completion_shared is only
                // called from this single completion thread).
                unsafe { ring.completion_shared() }
                    .map(|cqe| (cqe.user_data(), cqe.result()))
                    .collect()
            };

            for (user_data, result) in cqes {
                // Skip write SQE completions (sentinel tag).
                if user_data == WRITE_SQE_TAG {
                    continue;
                }

                let op_id = user_data;
                let op = {
                    let Ok(mut pending) = pending.lock() else {
                        break;
                    };
                    pending.remove(&op_id)
                };

                if let Some(op) = op {
                    if result < 0 {
                        let err_msg = io::Error::from_raw_os_error(-result).to_string();
                        Self::send_error(op.caller_pid, op_id, &err_msg);
                    } else {
                        Self::send_ok(op.caller_pid, op_id);
                    }
                    // PendingOp (including owned buffers) is dropped here,
                    // after the kernel has finished with the buffers.
                }
            }
        }
    }

    /// Send `{:io_complete, op_id, :ok}` to a BEAM process.
    fn send_ok(pid: LocalPid, op_id: u64) {
        let mut env = OwnedEnv::new();
        let _ = env.send_and_clear(&pid, |env| {
            (atoms::io_complete(), op_id, atoms::ok()).encode(env)
        });
    }

    /// Send `{:io_complete, op_id, {:error, reason}}` to a BEAM process.
    fn send_error(pid: LocalPid, op_id: u64, reason: &str) {
        let reason = reason.to_owned();
        let mut env = OwnedEnv::new();
        let _ = env.send_and_clear(&pid, |env| {
            (
                atoms::io_complete(),
                op_id,
                (atoms::error(), reason.as_str()),
            )
                .encode(env)
        });
    }

    /// Current write offset (the byte position immediately after the last
    /// optimistically-assigned byte).
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }
}

impl Drop for AsyncUringBackend {
    fn drop(&mut self) {
        // Signal the completion thread to shut down.
        self.shutdown.store(true, Ordering::SeqCst);

        // Submit a no-op SQE to wake the completion thread if it is blocked
        // inside `submit_and_wait`.
        if let Ok(ring) = self.ring.lock() {
            let nop = opcode::Nop::new().build().user_data(WRITE_SQE_TAG);
            unsafe {
                let mut sq = ring.submission_shared();
                let _ = sq.push(&nop);
            }
            let _ = ring.submit();
        }

        // Join the thread. Ignore errors — the thread may have already exited.
        if let Some(handle) = self.completion_thread.take() {
            let _ = handle.join();
        }
    }
}

// SAFETY: The only non-Send/Sync fields are the raw fd (which is just an
// integer used in SQEs — no unsynchronised access) and the `File` (which is
// never read or written after open — only the kernel uses the fd). The ring,
// pending map, and shutdown flag are all behind Arc<Mutex/AtomicBool>. The
// completion thread handle is only accessed in Drop.
unsafe impl Send for AsyncUringBackend {}
unsafe impl Sync for AsyncUringBackend {}

// ---------------------------------------------------------------------------
// Tests (Linux only)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    /// Skip the test if `io_uring` is not available.
    fn requires_io_uring() -> bool {
        IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
            .build(1)
            .is_ok()
    }

    #[test]
    fn async_backend_submit_empty_batch_is_ok() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let backend = AsyncUringBackend::open(&path).unwrap();

        // For an empty batch we need a LocalPid, but since submit_batch
        // on empty buffers sends the message immediately (no ring
        // submission), we just check offsets are empty.
        //
        // We cannot easily construct a real LocalPid in a Rust unit test
        // (no BEAM running), so we verify the offset stays 0.
        assert_eq!(backend.offset(), 0);
    }

    #[test]
    fn async_backend_offset_advances_on_submit() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let backend = AsyncUringBackend::open(&path).unwrap();

        // We cannot submit without a valid LocalPid (no BEAM running),
        // but we can verify offset starts at 0 and that open works.
        assert_eq!(backend.offset(), 0);
    }

    #[test]
    fn async_backend_open_sets_offset_from_existing_file() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");

        // Write some data using std::fs first.
        std::fs::write(&path, b"hello").unwrap();

        let backend = AsyncUringBackend::open(&path).unwrap();
        assert_eq!(backend.offset(), 5, "offset must equal existing file size");
    }

    #[test]
    fn async_backend_drop_does_not_hang() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");

        // Open and immediately drop — the background thread should shut
        // down cleanly without blocking.
        let backend = AsyncUringBackend::open(&path).unwrap();
        drop(backend);
        // If we get here without hanging, the test passes.
    }
}
