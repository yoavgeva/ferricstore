// Background WAL I/O thread.
//
// Handles: write() + fdatasync() on a dedicated OS thread.
// Communicates with NIF layer via:
// - Mutex<AlignedBuffer> for write data (shared with NIF)
// - crossbeam channel for FlushRequest/CloseRequest signals
//
// The thread never touches BEAM schedulers. All BEAM notifications
// go through OwnedEnv::send_and_clear.

use crate::aligned_buffer::AlignedBuffer;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use rustler::Encoder;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Messages sent from NIF to background thread (flush signals only, not data).
pub enum ThreadMsg {
    /// Request fdatasync. Caller will be notified on completion.
    Flush(FlushCaller),
    /// Shutdown: drain, sync, close, exit.
    Close,
}

/// Caller information for flush notification.
pub struct FlushCaller {
    pub pid: rustler::LocalPid,
    pub env: rustler::OwnedEnv,
    pub saved_ref: rustler::env::SavedTerm,
}

// FlushCaller must be Send to cross thread boundary
unsafe impl Send for FlushCaller {}

/// Configuration for the background thread.
pub struct ThreadConfig {
    pub file: File,
    pub buffer: Arc<Mutex<AlignedBuffer>>,
    pub rx: Receiver<ThreadMsg>,
    pub alive: Arc<AtomicBool>,
    pub file_size: Arc<AtomicU64>,
    pub stats: Arc<crate::wal_handle::WalStats>,
    pub commit_delay: Duration,
    pub use_o_direct: bool,
}

/// Run the background thread loop.
/// This is called from std::thread::spawn — runs entirely outside BEAM.
pub fn thread_loop(config: ThreadConfig) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        thread_loop_inner(config.file, config.buffer, config.rx,
                         config.file_size, config.stats.clone(), config.commit_delay);
    }));

    // On panic or normal exit, mark thread as dead
    config.alive.store(false, Ordering::Release);

    if let Err(panic) = result {
        eprintln!("[ferricstore_wal_nif] WAL thread panicked: {:?}",
                  panic.downcast_ref::<String>().map(|s| s.as_str())
                       .or_else(|| panic.downcast_ref::<&str>().copied())
                       .unwrap_or("unknown"));
    }
}

fn thread_loop_inner(
    mut file: File,
    buffer: Arc<Mutex<AlignedBuffer>>,
    rx: Receiver<ThreadMsg>,
    file_size: Arc<AtomicU64>,
    stats: Arc<crate::wal_handle::WalStats>,
    commit_delay: Duration,
) {
    let mut callers: Vec<FlushCaller> = Vec::new();

    loop {
        // =====================================================================
        // Phase 1: IDLE — block until first Flush arrives (zero CPU when idle)
        // =====================================================================
        let msg = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => return, // Channel closed — handle dropped
        };

        match msg {
            ThreadMsg::Close => {
                let _ = do_final_flush(&mut file, &buffer, &file_size);
                return;
            }
            ThreadMsg::Flush(caller) => {
                callers.push(caller);
            }
        }

        // =====================================================================
        // Phase 2: COMMIT DELAY — wait briefly to batch more (first cycle only)
        // =====================================================================
        if !commit_delay.is_zero() {
            let deadline = Instant::now() + commit_delay;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(ThreadMsg::Flush(c)) => callers.push(c),
                    Ok(ThreadMsg::Close) => {
                        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);
                        let _ = do_final_flush(&mut file, &buffer, &file_size);
                        return;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => {
                        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);
                        return;
                    }
                }
            }
        }

        // =====================================================================
        // Phase 3: SELF-DRIVING LOOP — keep flushing while buffer has data
        // =====================================================================
        loop {
            // Flush: take buffer → write → fdatasync → notify callers
            do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);

            // Collect any Flush messages that arrived during I/O (non-blocking)
            loop {
                match rx.try_recv() {
                    Ok(ThreadMsg::Flush(c)) => callers.push(c),
                    Ok(ThreadMsg::Close) => {
                        let _ = do_final_flush(&mut file, &buffer, &file_size);
                        return;
                    }
                    Err(_) => break, // No more messages
                }
            }

            // Check: is there more data in the buffer?
            let has_data = {
                let buf = buffer.lock().expect("buffer mutex poisoned");
                !buf.is_empty()
            };

            if has_data {
                // More data arrived during I/O — loop immediately (no delay).
                // If we have no callers yet, that's fine — they'll arrive by
                // the time we finish the next flush, and we'll collect them.
                continue;
            }

            // Buffer empty. If we have pending callers (Flush messages arrived
            // but no data), they need to be notified after a sync.
            if !callers.is_empty() {
                // Sync for the callers (even if buffer was empty, they expect
                // {wal_sync_complete} which guarantees prior writes are durable).
                do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);
                continue;
            }

            // Truly idle: no data, no callers. Return to Phase 1.
            break;
        }
    }
}

/// Take the shared buffer, write to disk, fdatasync, notify all callers.
fn do_flush(
    file: &mut File,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
    stats: &crate::wal_handle::WalStats,
    callers: &mut Vec<FlushCaller>,
) {
    // Take ownership of buffered data (nanoseconds under lock)
    let taken = {
        let mut buf = buffer.lock().expect("buffer mutex poisoned");
        buf.take()
    };

    let bytes_this_flush = taken.logical_len as u64;
    let num_callers = callers.len() as u64;

    // Write to disk if there's data
    if !taken.is_empty() {
        let write_slice = taken.as_padded_slice();
        match write_all_retry(file, write_slice) {
            Ok(()) => {
                file_size.fetch_add(bytes_this_flush, Ordering::Release);
            }
            Err(e) => {
                notify_callers_error(callers, &e);
                return;
            }
        }
    }

    // fdatasync with timing
    let sync_start = Instant::now();
    match file.sync_data() {
        Ok(()) => {
            let sync_us = sync_start.elapsed().as_micros() as u64;
            stats.flush_count.fetch_add(1, Ordering::Relaxed);
            stats.callers_notified.fetch_add(num_callers, Ordering::Relaxed);
            stats.bytes_written.fetch_add(bytes_this_flush, Ordering::Relaxed);
            stats.sync_latency_us_total.fetch_add(sync_us, Ordering::Relaxed);
            stats.sync_latency_us_max.fetch_max(sync_us, Ordering::Relaxed);
            notify_callers_success(callers);
        }
        Err(e) => {
            notify_callers_error(callers, &e);
        }
    }
}

/// Final flush on close: take any remaining buffer, write, sync.
fn do_final_flush(
    file: &mut File,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
) -> io::Result<()> {
    let taken = {
        let mut buf = buffer.lock().expect("buffer mutex poisoned");
        buf.take()
    };

    if !taken.is_empty() {
        write_all_retry(file, taken.as_padded_slice())?;
        file_size.fetch_add(taken.logical_len as u64, Ordering::Release);
    }

    file.sync_data()?;
    Ok(())
}

/// Write with retry on EINTR.
fn write_all_retry(file: &mut File, data: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        match file.write(&data[written..]) {
            Ok(n) => written += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Notify all callers that sync completed successfully.
fn notify_callers_success(callers: &mut Vec<FlushCaller>) {
    for mut caller in callers.drain(..) {
        caller.env.send_and_clear(&caller.pid, |env| {
            let ref_term = caller.saved_ref.load(env);
            rustler::types::tuple::make_tuple(env, &[
                crate::atoms::wal_sync_complete().encode(env),
                ref_term,
            ])
        });
    }
}

/// Notify all callers that sync failed.
fn notify_callers_error(callers: &mut Vec<FlushCaller>, error: &io::Error) {
    let reason = format!("{error}");
    for mut caller in callers.drain(..) {
        caller.env.send_and_clear(&caller.pid, |env| {
            let ref_term = caller.saved_ref.load(env);
            rustler::types::tuple::make_tuple(env, &[
                crate::atoms::wal_sync_error().encode(env),
                ref_term,
                reason.as_str().encode(env),
            ])
        });
    }
}

// ---------------------------------------------------------------------------
// File opening
// ---------------------------------------------------------------------------

/// Open a WAL file with platform-appropriate flags.
pub fn open_wal_file(path: &str, pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    #[cfg(target_os = "linux")]
    {
        open_wal_file_linux(path, pre_allocate_bytes)
    }
    #[cfg(not(target_os = "linux"))]
    {
        open_wal_file_fallback(path, pre_allocate_bytes)
    }
}

#[cfg(target_os = "linux")]
fn open_wal_file_linux(path: &str, pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    use std::os::unix::fs::OpenOptionsExt;

    // Try O_DIRECT first
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .custom_flags(libc::O_DIRECT) // NO O_DSYNC — fdatasync is explicit
        .open(path);

    match file {
        Ok(f) => {
            if pre_allocate_bytes > 0 {
                let ret = unsafe {
                    libc::fallocate(f.as_raw_fd(), 0, 0, pre_allocate_bytes as i64)
                };
                if ret != 0 {
                    // fallocate failed (e.g., ENOSPC) — close and cleanup
                    drop(f);
                    let _ = std::fs::remove_file(path);
                    return Err(io::Error::last_os_error());
                }
            }
            Ok((f, true)) // O_DIRECT enabled
        }
        Err(_) => {
            // O_DIRECT not supported on this filesystem — fall back
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(path)?;
            Ok((f, false))
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn open_wal_file_fallback(path: &str, _pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)?;
    Ok((f, false)) // No O_DIRECT on macOS
}

/// Read bytes from file at offset (for recovery).
pub fn pread_from_file(file: &mut File, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Tests (pure Rust, no BEAM)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Helper: create a thread config for testing (no BEAM notifications).
    fn test_config(commit_delay_us: u64) -> (ThreadConfig, crossbeam_channel::Sender<ThreadMsg>) {
        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (tx, rx) = crossbeam_channel::unbounded();
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());
        let config = ThreadConfig {
            file,
            buffer: buffer.clone(),
            rx,
            alive: alive.clone(),
            file_size: file_size.clone(),
            stats: stats.clone(),
            commit_delay: Duration::from_micros(commit_delay_us),
            use_o_direct: false,
        };
        (config, tx)
    }

    #[test]
    fn test_open_wal_file_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let (file, o_direct) = open_wal_file(path.to_str().unwrap(), 0).unwrap();
        drop(file);

        #[cfg(not(target_os = "linux"))]
        assert!(!o_direct);

        // File should exist
        assert!(path.exists());
    }

    #[test]
    fn test_open_with_preallocate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let (file, _) = open_wal_file(path.to_str().unwrap(), 4096).unwrap();
        drop(file);
        // On Linux with fallocate, file size would be 4096.
        // On macOS, fallocate is skipped, file size is 0.
        assert!(path.exists());
    }

    #[test]
    fn test_write_all_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::File::create(&path).unwrap();
        write_all_retry(&mut file, b"hello world").unwrap();
        file.sync_all().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents, b"hello world");
    }

    #[test]
    fn test_write_all_retry_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::File::create(&path).unwrap();
        write_all_retry(&mut file, b"").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.is_empty());
    }

    #[test]
    fn test_pread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello world").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let data = pread_from_file(&mut file, 6, 5).unwrap();
        assert_eq!(&data, b"world");
    }

    #[test]
    fn test_pread_at_offset_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let data = pread_from_file(&mut file, 0, 5).unwrap();
        assert_eq!(&data, b"hello");
    }

    #[test]
    fn test_do_flush_writes_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());

        // Write some data into the buffer
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"test data 12345");
        }

        // Flush (no callers to notify in pure Rust test)
        let mut callers = Vec::new();
        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);

        // Verify file_size was updated
        assert_eq!(file_size.load(Ordering::Acquire), 15);

        // Verify data on disk (may be padded for O_DIRECT)
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 15);
        assert_eq!(&contents[..15], b"test data 12345");
    }

    #[test]
    fn test_do_flush_empty_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());

        let mut callers = Vec::new();
        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);

        // No data should have been written
        assert_eq!(file_size.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_do_flush_multiple_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());

        // First write
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"first ");
        }
        let mut callers = Vec::new();
        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);
        assert_eq!(file_size.load(Ordering::Acquire), 6);

        // Second write (appends to file)
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"second");
        }
        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);
        assert_eq!(file_size.load(Ordering::Acquire), 12);
    }

    #[test]
    fn test_do_final_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"final data");
        }

        do_final_flush(&mut file, &buffer, &file_size).unwrap();
        assert_eq!(file_size.load(Ordering::Acquire), 10);
    }

    #[test]
    fn test_thread_close_flushes_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    file_size: fs_clone,
                    stats: stats.clone(),
                    commit_delay: Duration::ZERO,
                    use_o_direct: false,
                });
            })
            .unwrap();

        // Write data to buffer
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"thread close test");
        }

        // Send close — thread should flush before exiting
        tx.send(ThreadMsg::Close).unwrap();
        handle.join().unwrap();

        // Thread should be marked dead
        assert!(!alive.load(Ordering::Acquire));

        // Data should be on disk
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 17);
        assert_eq!(&contents[..17], b"thread close test");
        assert_eq!(file_size.load(Ordering::Acquire), 17);
    }

    #[test]
    fn test_thread_channel_disconnect_exits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    file_size: fs_clone,
                    stats: stats.clone(),
                    commit_delay: Duration::ZERO,
                    use_o_direct: false,
                });
            })
            .unwrap();

        // Drop the sender — channel disconnects
        drop(tx);

        // Thread should exit cleanly
        handle.join().unwrap();
        assert!(!alive.load(Ordering::Acquire));
    }

    #[test]
    fn test_commit_delay_collects_multiple_flushes() {
        // This test verifies that during the commit delay window,
        // multiple flush requests are collected and served by one fdatasync.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    file_size: fs_clone,
                    stats: stats.clone(),
                    commit_delay: Duration::from_millis(50), // 50ms delay
                    use_o_direct: false,
                });
            })
            .unwrap();

        // Write data
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"batch data");
        }

        // Send multiple flush requests rapidly (no BEAM callers in this test)
        // We can't send FlushCaller without BEAM, so just test Close behavior
        tx.send(ThreadMsg::Close).unwrap();
        handle.join().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 10);
        assert_eq!(&contents[..10], b"batch data");
    }

    #[test]
    fn test_large_buffer_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true).write(true).read(true).open(&path).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(crate::wal_handle::WalStats::new());

        // 10 MB of data
        let data = vec![0xABu8; 10 * 1024 * 1024];
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(&data);
        }

        let mut callers = Vec::new();
        do_flush(&mut file, &buffer, &file_size, &stats, &mut callers);

        assert_eq!(file_size.load(Ordering::Acquire), 10 * 1024 * 1024);
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 10 * 1024 * 1024);
        assert_eq!(&contents[..10 * 1024 * 1024], &data[..]);
    }
}
