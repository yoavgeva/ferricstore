# Ra Fork: Rust NIF WAL Design (v2)

Updated after review. Addresses all critical/high issues from `ra-fork-wal-nif-review.md`.

## Principle

**Keep all ra business logic in Erlang. Replace only the file I/O syscalls with Rust NIFs.**

Ra's `ra_log_wal.erl` stays as the `gen_batch_server`. Record formatting, checksums, header serialization, writer tracking, mem table notifications, rollover logic, recovery parsing — all unchanged. The NIF is a thin I/O layer: open, write, sync, close, position, pread.

## What changes in the ra fork

### 1. `ra_log.erl` — `{ttb}` pass-through (3 lines × 3 locations)

Skip double serialization when command is already pre-serialized:

```erlang
%% Before:
Cmd = {ttb, term_to_iovec(Cmd0)},

%% After:
Cmd = case Cmd0 of
    {ttb, _} -> Cmd0;
    _ -> {ttb, term_to_iovec(Cmd0)}
end,
```

### 2. `ra_log_wal.erl` — Replace 6 syscalls with NIF calls

Everything else in `ra_log_wal.erl` stays exactly as-is.

| Erlang syscall | Rust NIF | Where in ra_log_wal.erl |
|---|---|---|
| `file:open/2` | `ferricstore_wal_nif:open/1` | `open_wal/1`, `roll_over/1` |
| `file:write/2` | `ferricstore_wal_nif:write/2` | `flush_pending/1` |
| `file:datasync/1` or `ra_file:sync/2` | `ferricstore_wal_nif:sync/3` | `complete_batch/1` |
| `file:close/1` | `ferricstore_wal_nif:close/1` | `roll_over/1`, `cleanup/1` |
| `file:position/2` | `ferricstore_wal_nif:position/1` | recovery, fill_ratio |
| `file:read/2` | `ferricstore_wal_nif:pread/3` | recovery |

### 3. `ra_log_wal.erl` — Async sync completion handler

Add a handler for the async sync notification:

```erlang
handle_op({info, {wal_sync_complete, Ref}},
          {#state{pending_sync = {Ref, Waiting, WalRanges}} = State0, Actions}) ->
    %% Sync completed — notify all writers from this batch
    State1 = complete_batch_writers(Waiting, WalRanges, State0),
    State2 = State1#state{pending_sync = undefined},
    {State2, Actions};
handle_op({info, {wal_sync_complete, _StaleRef}}, {State, Actions}) ->
    %% Stale ref from a previous rollover — ignore
    {State, Actions};
handle_op({info, {wal_sync_error, _Ref, Reason}}, _State) ->
    %% Disk error during sync — crash the WAL (ra will restart it)
    throw({stop, {wal_sync_failed, Reason}});
```

### 4. `ra_log_wal.erl` — Modified `complete_batch/1`

```erlang
complete_batch(#state{batch = #batch{pending = Pend,
                                     waiting = Waiting,
                                     wal_ranges = WalRanges},
                      wal = #wal{fd = Handle}} = State0)
  when Pend =/= [] ->
    %% Write formatted records to NIF buffer (instant, no syscall)
    case ferricstore_wal_nif:write(Handle, Pend) of
        ok ->
            %% Request async sync — background thread will fdatasync
            %% and send {wal_sync_complete, Ref} when done
            Ref = make_ref(),
            case ferricstore_wal_nif:sync(Handle, self(), Ref) of
                ok ->
                    State0#state{batch = new_batch(),
                                 pending_sync = {Ref, Waiting, WalRanges}};
                {error, Reason} ->
                    throw({stop, {wal_sync_failed, Reason}})
            end;
        {error, Reason} ->
            throw({stop, {wal_write_failed, Reason}})
    end;
complete_batch(#state{batch = #batch{pending = []}} = State) ->
    State#state{batch = new_batch()}.
```

### 5. `ra_log_wal.erl` — Modified `complete_batch_and_roll/1`

```erlang
complete_batch_and_roll(#state{} = State0) ->
    State1 = complete_batch(State0),
    %% Before rollover, wait for pending sync to complete
    State2 = drain_pending_sync(State1),
    roll_over(start_batch(State2)).

drain_pending_sync(#state{pending_sync = undefined} = State) ->
    State;
drain_pending_sync(#state{pending_sync = {Ref, Waiting, WalRanges}} = State) ->
    receive
        {wal_sync_complete, Ref} ->
            State1 = complete_batch_writers(Waiting, WalRanges, State),
            State1#state{pending_sync = undefined}
    after 30000 ->
        throw({stop, wal_sync_timeout_during_rollover})
    end.
```

### 6. Constants (already done)

- `FLUSH_COMMANDS_SIZE` 16→128
- `AER_CHUNK_SIZE` 128→512
- `WAL_DEFAULT_MAX_BATCH_SIZE` 8192→32768

## Rust NIF Module: `ferricstore_wal_nif`

Lives in `apps/ferricstore/native/ferricstore_wal_nif/`. All NIFs run on **normal BEAM schedulers** — every call returns in <1μs.

### Architecture

```
ra_log_wal.erl (gen_batch_server)
    │  record formatting, checksums, headers — unchanged
    │  builds iolist of formatted records
    │
    │  NIF calls (instant, <1μs each)
    ▼
ferricstore_wal_nif (Rust)
    │  write: copy iolist into aligned buffer, return immediately
    │  sync: send flush message to background thread, return immediately
    ▼
WAL background thread (Rust, 1 per handle)
    │  commit_delay: recv_timeout collecting more writes
    │  single write() to disk
    │  fdatasync() (or io_uring on Linux)
    │  send {wal_sync_complete, Ref} to caller
    ▼
NVMe disk
```

### NIF Functions

All return `ok | {error, Reason}` (or `{ok, Value} | {error, Reason}`).

```rust
/// Open a WAL file. Spawns the background WAL thread.
/// Linux: O_DIRECT + fallocate (NOT O_DSYNC — that defeats batching).
/// macOS: regular open (no O_DIRECT, no fallocate).
///
/// Opts is a map: #{commit_delay_us => 200, pre_allocate_bytes => 268435456}
#[rustler::nif]
fn open(path: String, opts: Term) -> NifResult<(Atom, ResourceArc<WalHandle>)>
// Returns {:ok, handle} | {:error, reason}
```

```rust
/// Write pre-formatted iolist to the WAL buffer.
/// Copies iolist bytes into an aligned buffer in shared state.
/// Does NOT write to disk — just buffers.
/// Returns immediately (<1μs).
#[rustler::nif]
fn write(handle: ResourceArc<WalHandle>, iodata: Term) -> NifResult<Atom>
// Returns :ok | {:error, :wal_thread_dead}
```

```rust
/// Request async fdatasync. Background thread will:
/// 1. Take ownership of the buffered writes
/// 2. Wait commit_delay_us via recv_timeout (collecting more writes)
/// 3. write() all collected data to disk (aligned for O_DIRECT)
/// 4. fdatasync() (or io_uring)
/// 5. Send {wal_sync_complete, Ref} to CallerPid
///    OR {wal_sync_error, Ref, Reason} on failure
///
/// Returns immediately (<1μs).
#[rustler::nif]
fn sync(handle: ResourceArc<WalHandle>, caller_pid: LocalPid,
        ref_term: Term) -> NifResult<Atom>
// Returns :ok | {:error, :wal_thread_dead}
```

```rust
/// Close the WAL file. Signals the background thread to:
/// 1. Write any remaining buffered data
/// 2. fdatasync
/// 3. Close the fd
/// 4. Exit the thread
///
/// Blocks until complete (called during rollover, which is rare).
/// Timeout: 30 seconds.
#[rustler::nif]
fn close(handle: ResourceArc<WalHandle>) -> NifResult<Atom>
// Returns :ok | {:error, reason}
```

```rust
/// Returns current file size in bytes.
/// Reads from AtomicU64 with Acquire ordering — no syscall.
#[rustler::nif]
fn position(handle: ResourceArc<WalHandle>) -> NifResult<(Atom, u64)>
// Returns {:ok, size}
```

```rust
/// Read bytes from the WAL file at a given offset.
/// Used during recovery. Runs on normal scheduler (small reads)
/// or yields for large reads.
#[rustler::nif]
fn pread(handle: ResourceArc<WalHandle>, offset: u64, len: u64)
    -> NifResult<(Atom, Binary)>
// Returns {:ok, binary} | {:error, reason}
```

### WalHandle and Background Thread

```rust
struct WalHandle {
    /// Mutex-protected write buffer. NIF writes append here.
    /// Background thread swaps it out during flush.
    buffer: Mutex<AlignedBuffer>,

    /// Signal channel: NIF sends FlushRequest, background thread receives.
    flush_tx: Sender<FlushRequest>,

    /// Thread alive flag — set to false on panic or exit.
    alive: AtomicBool,

    /// File size — updated by background thread after each write.
    file_size: AtomicU64,

    /// Join handle for cleanup.
    thread: Mutex<Option<JoinHandle<()>>>,
}

/// 4096-byte aligned buffer for O_DIRECT compatibility.
struct AlignedBuffer {
    ptr: *mut u8,       // allocated via posix_memalign or aligned_alloc
    len: usize,
    capacity: usize,
}

struct FlushRequest {
    caller: LocalPid,
    ref_env: OwnedEnv,
    ref_saved: SavedTerm,
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        // Non-blocking: signal thread to exit, don't wait.
        // Thread will close fd and exit on its own.
        let _ = self.flush_tx.send_timeout(/* poison pill */, Duration::from_millis(100));
        self.alive.store(false, Ordering::Release);
    }
}
```

### Background Thread Loop

```rust
fn wal_thread_loop(
    rx: Receiver<FlushRequest>,
    buffer: Arc<Mutex<AlignedBuffer>>,
    file: File,
    file_size: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
    commit_delay: Duration,
) {
    // Wrap everything in catch_unwind to prevent silent death
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        loop {
            // Block until a flush is requested
            let flush_req = match rx.recv() {
                Ok(FlushRequest::Flush(req)) => req,
                Ok(FlushRequest::Close) => {
                    // Final flush and close
                    flush_and_close(&buffer, &file, &file_size);
                    return;
                }
                Err(_) => return, // channel closed (handle dropped)
            };

            // Collect the first flush caller
            let mut callers = vec![flush_req];

            // Commit delay: wait for more flushes to arrive
            // Uses recv_timeout — yields CPU, no spin-wait
            let deadline = Instant::now() + commit_delay;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() { break; }

                match rx.recv_timeout(remaining) {
                    Ok(FlushRequest::Flush(req)) => callers.push(req),
                    Ok(FlushRequest::Close) => {
                        // Close requested during commit delay — flush everything and exit
                        do_flush(&buffer, &file, &file_size, &mut callers);
                        flush_and_close(&buffer, &file, &file_size);
                        return;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => {
                        // Best-effort flush before exit
                        do_flush(&buffer, &file, &file_size, &mut callers);
                        return;
                    }
                }
            }

            // Perform the actual I/O
            do_flush(&buffer, &file, &file_size, &mut callers);
        }
    }));

    // If we get here via panic, mark thread as dead
    if result.is_err() {
        alive.store(false, Ordering::Release);
        // Log the panic somehow (eprintln at minimum)
    }
}

fn do_flush(
    buffer: &Mutex<AlignedBuffer>,
    file: &File,
    file_size: &AtomicU64,
    callers: &mut Vec<FlushCaller>,
) {
    // Swap out the buffer under lock (nanoseconds)
    let data = {
        let mut buf = buffer.lock().unwrap();
        buf.take() // returns owned data, replaces with empty
    };

    if data.len() > 0 {
        // Pad to 4096-byte alignment for O_DIRECT
        let padded = pad_to_alignment(&data, 4096);

        // Single write syscall
        match file.write_all(&padded) {
            Ok(()) => {
                file_size.fetch_add(data.len() as u64, Ordering::Release);
            }
            Err(e) => {
                // Notify all callers of error
                for caller in callers.drain(..) {
                    send_sync_error(&caller, &e);
                }
                return;
            }
        }
    }

    // fdatasync
    match file.sync_data() {
        Ok(()) => {
            // Notify ALL callers of success
            for caller in callers.drain(..) {
                send_sync_complete(&caller);
            }
        }
        Err(e) => {
            for caller in callers.drain(..) {
                send_sync_error(&caller, &e);
            }
        }
    }
}
```

### io_uring (Linux, optional feature)

Replaces `file.write_all()` + `file.sync_data()` in `do_flush`:

```rust
#[cfg(feature = "io_uring")]
fn do_flush_uring(
    ring: &mut IoUring,
    fd: RawFd,
    data: &[u8],
    // ... same callers notification
) {
    // Chain write → fsync with IO_LINK for guaranteed ordering
    let write_e = opcode::Write::new(Fd(fd), data.as_ptr(), data.len() as u32)
        .build()
        .flags(squeue::Flags::IO_LINK)
        .user_data(1);
    let sync_e = opcode::Fsync::new(Fd(fd))
        .build()
        .user_data(2);

    unsafe {
        ring.submission().push(&write_e).unwrap();
        ring.submission().push(&sync_e).unwrap();
    }
    ring.submit_and_wait(2).unwrap();

    // Check both CQE results for errors
    for cqe in ring.completion() {
        if cqe.result() < 0 {
            // handle error, notify callers
        }
    }
}
```

Auto-detected at startup:
```rust
fn detect_io_uring() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check kernel >= 5.10
        let uname = nix::sys::utsname::uname().ok()?;
        let release = uname.release().to_str()?;
        // parse major.minor >= 5.10
        // Also try to create a probe ring
        IoUring::new(2).is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    false
}
```

### O_DIRECT Details

**Open (Linux only, no O_DSYNC):**
```rust
#[cfg(target_os = "linux")]
fn open_wal_file(path: &str, pre_allocate: u64) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true) // needed for pread during recovery
        .custom_flags(libc::O_DIRECT) // NO O_DSYNC
        .open(path)?;

    if pre_allocate > 0 {
        let ret = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, pre_allocate as i64) };
        if ret != 0 {
            let _ = std::fs::remove_file(path); // cleanup on failure
            return Err(io::Error::last_os_error());
        }
    }
    Ok(file)
}
```

**Aligned buffer:**
```rust
impl AlignedBuffer {
    fn new(alignment: usize) -> Self {
        // Start with 256KB capacity, aligned to 4096
        let capacity = 256 * 1024;
        let ptr = unsafe {
            let mut p: *mut libc::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut p, alignment, capacity);
            if ret != 0 { panic!("posix_memalign failed"); }
            p as *mut u8
        };
        AlignedBuffer { ptr, len: 0, capacity }
    }

    fn extend(&mut self, data: &[u8]) {
        // Grow if needed (always aligned allocation)
        if self.len + data.len() > self.capacity {
            self.grow(self.len + data.len());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len += data.len();
    }

    fn take(&mut self) -> AlignedSlice {
        let slice = AlignedSlice { ptr: self.ptr, len: self.len };
        // Replace with new empty buffer (reuse requires careful sizing)
        *self = AlignedBuffer::new(4096);
        slice
    }
}

fn pad_to_alignment(data: &AlignedSlice, align: usize) -> &[u8] {
    // O_DIRECT requires write size to be multiple of 512 (or 4096)
    // The actual data length is tracked by ra's file_size counter
    // Padding bytes are zeros (safe: ra recovery stops at valid record boundary)
    let padded_len = (data.len + align - 1) & !(align - 1);
    // Zero-fill the padding region
    unsafe {
        std::ptr::write_bytes(data.ptr.add(data.len), 0, padded_len - data.len);
        std::slice::from_raw_parts(data.ptr, padded_len)
    }
}
```

**macOS fallback:**
```rust
#[cfg(not(target_os = "linux"))]
fn open_wal_file(path: &str, _pre_allocate: u64) -> io::Result<File> {
    // No O_DIRECT, no fallocate on macOS
    // AlignedBuffer still used but alignment not required
    OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)
}
```

## On-Disk Format

**Not changed.** Ra formats records in Erlang (`write_data` in `ra_log_wal.erl`):

```
[HeaderData][Checksum:32][EntryDataLen:32][Index:64][Term:64][EntryData]
```

The NIF receives this as an iolist and writes it byte-for-byte. Recovery uses ra's existing `recover_wal` function which reads and parses this format. O_DIRECT padding zeros at the end of each write are safe because ra's parser stops at the first invalid record header (zero bytes = invalid).

## Error Handling

### NIF → Erlang

Every NIF checks `alive` flag before operating:

```rust
fn check_alive(handle: &WalHandle) -> NifResult<()> {
    if !handle.alive.load(Ordering::Acquire) {
        Err(rustler::Error::Term(Box::new("wal_thread_dead")))
    } else {
        Ok(())
    }
}
```

### Background thread → Erlang

On I/O error, sends `{wal_sync_error, Ref, Reason}` to caller.
On panic, `catch_unwind` sets `alive = false`. Next NIF call returns `{error, wal_thread_dead}`.

### ra_log_wal.erl → ra

On `{wal_sync_error, ...}` or NIF error return, `throw({stop, Reason})` crashes the WAL process. Ra's supervision tree restarts it, triggering WAL recovery.

## Shared Buffer (Mutex) vs Channel

The review noted channel overhead (100K allocations/sec). Using a mutex-protected shared buffer instead:

- **write NIF**: locks mutex, extends buffer, unlocks (~nanoseconds)
- **sync NIF**: sends FlushRequest to channel (only flush signals, not data)
- **Background thread**: locks mutex, swaps buffer, unlocks, then writes to disk

The channel carries only `FlushRequest` messages (small), not data. Data goes through the shared buffer. This gives ~3,300 lock/unlocks per second instead of 100K channel sends.

## Backpressure

The `AlignedBuffer` tracks total buffered bytes. If it exceeds a configurable limit (default 64MB), the `write` NIF returns `{error, backpressure}`. The gen_batch_server propagates this to callers.

```rust
fn write(handle: ResourceArc<WalHandle>, iodata: Term) -> NifResult<Atom> {
    check_alive(&handle)?;
    let mut buf = handle.buffer.lock().unwrap();
    if buf.len() > handle.max_buffer_bytes {
        return Err(rustler::Error::Term(Box::new("backpressure")));
    }
    // ... copy iodata into buf
    Ok(atoms::ok())
}
```

## ResourceArc Drop

Non-blocking. Signals thread to exit but does NOT wait:

```rust
impl Drop for WalHandle {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        // Best-effort: try to send close signal, don't block if channel full
        let _ = self.flush_tx.try_send(FlushRequest::Close);
        // Thread will notice channel disconnect or Close signal and exit
        // fd closed by OS when thread exits
    }
}
```

## Crate Dependencies

```toml
[dependencies]
rustler = "0.37"
crossbeam-channel = "0.5"  # only for FlushRequest signals

[target.'cfg(target_os = "linux")'.dependencies]
libc = "0.2"

[target.'cfg(all(target_os = "linux", feature = "io_uring"))'.dependencies]
io-uring = "0.7"
```

No tokio. No async runtime. One background `std::thread`.

## Expected Performance

With 200μs commit_delay on NVMe (~100μs fdatasync):
- Each flush cycle: 200μs delay + 100μs fsync = 300μs
- ~3,300 flush cycles/sec
- Entries per flush scales with load:
  - Low (10 clients): ~10 entries/flush → 33K/sec
  - Medium (50 clients, pipeline=10): ~30-50 entries/flush → 100-165K/sec
  - High (200 clients, pipeline=50): ~100+ entries/flush → 330K+/sec (CPU-limited)

vs current ~7,500 entries/sec. **Target: 10-20x improvement.**

## Summary of Changes

| File | Lines changed | What |
|---|---|---|
| `ra/src/ra_log.erl` | ~9 | `{ttb}` pass-through (3 locations) |
| `ra/src/ra_log_wal.erl` | ~50 | Replace 6 syscalls with NIF, add async sync handler |
| `ra/src/ra_server.hrl` | 2 | Constants |
| `ra/src/ra.hrl` | 1 | Constants |
| `ferricstore_wal_nif/src/lib.rs` | ~400 | 6 NIF functions + background thread |
| `ferricstore_wal_nif/Cargo.toml` | ~15 | Dependencies |
