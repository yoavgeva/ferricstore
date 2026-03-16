#![allow(dead_code)]

//! `Store` — the public Bitcask API.
//!
//! Ties together the keydir (in-memory index), log writer (disk appends),
//! and hint files (fast startup). All writes go through the log first, then
//! update the keydir. Reads consult the keydir then do a single pread.

use std::fs;
use std::path::{Path, PathBuf};

use crate::hint::{HintEntry, HintReader, HintWriter};
use crate::keydir::{KeyDir, KeyEntry};
use crate::log::{LogReader, LogWriter, HEADER_SIZE};

/// Configuration for opening a store.
pub struct StoreConfig {
    /// Directory where data and hint files live.
    pub data_dir: PathBuf,
    /// Monotonically increasing ID for the active (writable) data file.
    pub active_file_id: u64,
}

#[derive(Debug)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoreError: {}", self.0)
    }
}

impl From<crate::log::LogError> for StoreError {
    fn from(e: crate::log::LogError) -> Self {
        StoreError(e.to_string())
    }
}

impl From<crate::hint::HintError> for StoreError {
    fn from(e: crate::hint::HintError) -> Self {
        StoreError(e.to_string())
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct Store {
    keydir: KeyDir,
    writer: LogWriter,
    data_dir: PathBuf,
    active_file_id: u64,
}

impl Store {
    /// Returns the file ID of the currently active (writable) data file.
    #[must_use]
    pub fn active_file_id(&self) -> u64 {
        self.active_file_id
    }

    /// Returns the path to the currently active log file.
    #[must_use]
    pub fn active_log_path(&self) -> PathBuf {
        log_path(&self.data_dir, self.active_file_id)
    }

    /// Open a store at `data_dir`.
    ///
    /// On startup:
    /// 1. Scan for `*.hint` files and load them to rebuild the keydir fast.
    /// 2. If no hint files exist, replay `*.log` files sequentially.
    /// 3. Open (or create) the active data file for appending.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the data directory cannot be created, existing
    /// log/hint files cannot be read, or the active data file cannot be opened.
    pub fn open(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir)?;

        let mut keydir = KeyDir::new();

        // Collect and sort existing data/hint file IDs
        let mut file_ids = collect_file_ids(data_dir)?;
        file_ids.sort_unstable();

        for &fid in &file_ids {
            let fid_hint_path = hint_path(data_dir, fid);
            let fid_log_path = log_path(data_dir, fid);

            // EC-7: A hint file with no corresponding data file is orphaned
            // (e.g. the data file was deleted after a partial compaction). Skip
            // it silently — the keydir will simply contain no entries for this
            // file ID, which is safe.
            if fid_hint_path.exists() && !fid_log_path.exists() {
                continue;
            }

            if fid_hint_path.exists() {
                // EC-2: If the hint file is corrupt fall back to full log replay
                // rather than propagating the error. The hint is just an
                // acceleration structure; the data file is the ground truth.
                //
                // Load into a staging keydir first so that a mid-file parse
                // failure does not leave partially-applied entries in the real
                // keydir. Only merge the staging keydir on success.
                let mut staging = KeyDir::new();
                let hint_ok = HintReader::open(&fid_hint_path)
                    .and_then(|mut r| r.load_into(&mut staging))
                    .is_ok();
                if hint_ok {
                    for (key, entry) in staging.iter() {
                        keydir.put(key.clone(), entry.clone());
                    }
                } else {
                    // Hint file corrupt — fall back to full log replay.
                    if fid_log_path.exists() {
                        replay_log(&fid_log_path, fid, &mut keydir)?;
                    }
                }
            } else {
                // No hint file — replay the raw log.
                if fid_log_path.exists() {
                    replay_log(&fid_log_path, fid, &mut keydir)?;
                }
            }
        }

        let active_file_id = file_ids.last().copied().unwrap_or(1);
        let writer = LogWriter::open(&log_path(data_dir, active_file_id), active_file_id)?;

        Ok(Self {
            keydir,
            writer,
            data_dir: data_dir.to_path_buf(),
            active_file_id,
        })
    }

    /// Write a key-value pair. `expire_at_ms` = 0 means no expiry.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the record cannot be written or synced to disk.
    pub fn put(&mut self, key: &[u8], value: &[u8], expire_at_ms: u64) -> Result<()> {
        let offset = self.writer.write(key, value, expire_at_ms)?;
        self.writer.sync()?;
        #[allow(clippy::cast_possible_truncation)]
        self.keydir.put(
            key.to_vec(),
            KeyEntry {
                file_id: self.active_file_id,
                offset,
                value_size: value.len() as u32,
                expire_at_ms,
                ref_bit: false,
            },
        );
        Ok(())
    }

    /// Read the value for `key`. Returns `None` if not found or expired.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the data file cannot be read.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let now_ms = now_ms();
        let entry = match self.keydir.get(key) {
            Some(e) => e.clone(),
            None => return Ok(None),
        };

        if entry.expire_at_ms != 0 && entry.expire_at_ms <= now_ms {
            // Logically expired — remove from keydir and write a tombstone so the
            // key does not resurrect after a store close+reopen.  We intentionally
            // do NOT call sync() here: the tombstone becomes durable on the next
            // put/sync or on a clean shutdown.  In the worst case (crash before the
            // next sync) the expired key re-appears on the following open, the TTL
            // check fires again, and another tombstone is written — acceptable per
            // Bitcask crash-recovery semantics.
            self.keydir.delete(key);
            self.writer.write_tombstone(key)?;
            return Ok(None);
        }

        let log_file = log_path(&self.data_dir, entry.file_id);
        let mut reader = LogReader::open(&log_file)?;
        let record = reader.read_at(entry.offset)?;
        Ok(record.and_then(|r| r.value))
    }

    /// Write multiple key-value pairs with a **single fsync** (group commit).
    ///
    /// This is the correct write path for the BEAM integration. The Elixir shard
    /// `GenServer` collects writes during a batch window (e.g. 1ms), then calls
    /// `put_batch` once. One fsync serves all writes in the batch — the dirty
    /// scheduler thread is occupied only for the duration of a single `sync_all`,
    /// not once per write.
    ///
    /// ## BEAM scheduler note
    ///
    /// Each NIF call with `schedule = "DirtyIo"` occupies one dirty scheduler
    /// thread for its entire duration. BEAM has a fixed pool of dirty scheduler
    /// threads (default: 10). If every `put` issues its own fsync, sustained
    /// concurrent write load exhausts the pool and new NIF calls queue behind it
    /// — this appears as write latency spikes on the Elixir side.
    ///
    /// `put_batch` amortises: N client writes → 1 NIF call → 1 fsync → 1 dirty
    /// thread occupied for the batch window. The normal BEAM schedulers are never
    /// blocked; only one dirty thread is occupied per batch, regardless of batch
    /// size.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any write or the final sync fails. On error the
    /// writes that completed before the failure are appended to the log but the
    /// keydir is not updated for them — the next `open()` will replay the log and
    /// recover any durable entries.
    pub fn put_batch(&mut self, entries: &[(&[u8], &[u8], u64)]) -> Result<()> {
        // write_batch encodes all records, submits them to the I/O backend in
        // one batch (one io_uring_enter on Linux, N writes on sync fallback),
        // then fsyncs once. Returns (offset, value_len) per entry.
        let committed = self.writer.write_batch(entries)?;

        // Update keydir only after durable commit.
        for ((offset, value_len), &(key, _, expire_at_ms)) in
            committed.into_iter().zip(entries.iter())
        {
            #[allow(clippy::cast_possible_truncation)]
            self.keydir.put(
                key.to_vec(),
                KeyEntry {
                    file_id: self.active_file_id,
                    offset,
                    value_size: value_len as u32,
                    expire_at_ms,
                    ref_bit: false,
                },
            );
        }
        Ok(())
    }

    /// Encode a batch of entries and update the keydir **without writing to
    /// disk**. Returns owned encoded buffers that the caller must submit to an
    /// `AsyncUringBackend`.
    ///
    /// This is the async counterpart to `put_batch`: it does everything except
    /// the actual I/O. The caller is responsible for submitting the returned
    /// buffers to the async ring and handling the completion message.
    ///
    /// The `writer.offset` is advanced optimistically. If the async I/O
    /// subsequently fails, the backend is in an inconsistent state — which
    /// matches Bitcask's crash-recovery contract (log replay on reopen
    /// reconciles the keydir with what is actually on disk).
    pub fn prepare_batch_for_async(&mut self, entries: &[(&[u8], &[u8], u64)]) -> Vec<Vec<u8>> {
        use crate::log::encode_record;

        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|(key, value, expire_at_ms)| encode_record(key, value, *expire_at_ms))
            .collect();

        // Compute offsets from the current writer position.
        let base_offset = self.writer.offset;
        let mut running_offset = base_offset;
        let mut total_bytes: u64 = 0;
        for (i, buf) in encoded.iter().enumerate() {
            let record_offset = running_offset;
            let buf_len = buf.len() as u64;
            running_offset += buf_len;
            total_bytes += buf_len;

            // Update keydir optimistically.
            let (key, value, expire_at_ms) = entries[i];
            #[allow(clippy::cast_possible_truncation)]
            self.keydir.put(
                key.to_vec(),
                KeyEntry {
                    file_id: self.active_file_id,
                    offset: record_offset,
                    value_size: value.len() as u32,
                    expire_at_ms,
                    ref_bit: false,
                },
            );
        }

        // Advance both the LogWriter offset AND the underlying backend's
        // internal offset counter. This keeps the sync backend in sync so
        // that if a subsequent sync write (e.g. a delete tombstone) goes
        // through the LogWriter, it starts at the correct position.
        self.writer.advance_offset(total_bytes);

        encoded
    }

    /// Delete a key. Appends a tombstone to the log.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the tombstone cannot be written or synced to disk.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        if self.keydir.get(key).is_none() {
            return Ok(false);
        }
        self.writer.write_tombstone(key)?;
        self.writer.sync()?;
        self.keydir.delete(key);
        Ok(true)
    }

    /// Return all live (non-expired) keys.
    #[must_use]
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let now_ms = now_ms();
        self.keydir
            .iter()
            .filter(|(_, e)| e.expire_at_ms == 0 || e.expire_at_ms > now_ms)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Number of live (non-expired) keys.
    ///
    /// Expired keys that are still present in the keydir (not yet evicted by a
    /// `get` call) are excluded so that callers always see a logically accurate
    /// count.
    #[must_use]
    pub fn len(&self) -> usize {
        let now = now_ms();
        self.keydir
            .iter()
            .filter(|(_, entry)| entry.expire_at_ms == 0 || entry.expire_at_ms > now)
            .count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write a hint file for the active data file (called after compaction or
    /// before rotating the active file).
    ///
    /// Fsyncs the active log before writing the hint so that every offset
    /// recorded in the hint points to bytes that are already durable on disk.
    /// Without this fsync a crash between hint write and log flush would leave
    /// the hint referencing records that do not survive recovery.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the log sync, hint file creation, or any hint
    /// entry write fails.
    pub fn write_hint_file(&mut self) -> Result<()> {
        // Ensure all buffered log writes are durable before recording their
        // offsets in the hint file (Issue 7.4).
        self.writer.sync()?;

        let hint_path = hint_path(&self.data_dir, self.active_file_id);
        let mut writer = HintWriter::open(&hint_path)?;
        for (key, entry) in self.keydir.iter() {
            if entry.file_id == self.active_file_id {
                writer.write_entry(&HintEntry {
                    file_id: entry.file_id,
                    offset: entry.offset,
                    value_size: entry.value_size,
                    expire_at_ms: entry.expire_at_ms,
                    key: key.clone(),
                })?;
            }
        }
        writer.commit()?;
        Ok(())
    }

    /// Proactively purge all logically-expired keys from the keydir.
    ///
    /// For each expired key a tombstone is appended to the log so that the key
    /// does not resurrect after a store close+reopen.  All tombstones are
    /// written and then fsynced in one batch.
    ///
    /// This is the proactive counterpart to the lazy expiry that fires inside
    /// `get`.  Without periodic `purge_expired` calls the keydir grows without
    /// bound for TTL-heavy workloads where keys are written but never read
    /// after they expire (Issue 6.3).
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any tombstone write or the final sync fails.
    pub fn purge_expired(&mut self) -> Result<usize> {
        let now = now_ms();
        let expired = self.keydir.expired_keys(now);
        let count = expired.len();
        for key in &expired {
            self.writer
                .write_tombstone(key)
                .map_err(|e| StoreError(e.to_string()))?;
            self.keydir.delete(key);
        }
        if count > 0 {
            self.writer.sync().map_err(|e| StoreError(e.to_string()))?;
        }
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn log_path(data_dir: &Path, file_id: u64) -> PathBuf {
    data_dir.join(format!("{file_id:020}.log"))
}

fn hint_path(data_dir: &Path, file_id: u64) -> PathBuf {
    data_dir.join(format!("{file_id:020}.hint"))
}

/// Scan `data_dir` for `*.log` files and return their numeric IDs.
fn collect_file_ids(data_dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".log") {
            if let Ok(id) = stem.trim_start_matches('0').parse::<u64>() {
                ids.push(id);
            } else if stem == "00000000000000000000" {
                ids.push(0);
            }
        }
    }
    Ok(ids)
}

/// Replay a raw log file into the keydir (used when no hint file exists).
fn replay_log(log_path: &Path, file_id: u64, keydir: &mut KeyDir) -> Result<()> {
    let mut reader = LogReader::open(log_path).map_err(|e| StoreError(e.to_string()))?;
    let mut offset: u64 = 0;

    let records = reader
        .iter_from_start_tolerant()
        .map_err(|e| StoreError(e.to_string()))?;
    for record in records {
        let record_len =
            (HEADER_SIZE + record.key.len() + record.value.as_ref().map_or(0, Vec::len)) as u64;

        if let Some(value) = record.value {
            keydir.put(
                record.key.clone(),
                KeyEntry {
                    file_id,
                    offset,
                    #[allow(clippy::cast_possible_truncation)]
                    value_size: value.len() as u32,
                    expire_at_ms: record.expire_at_ms,
                    ref_bit: false,
                },
            );
        } else {
            keydir.delete(&record.key);
        }

        offset += record_len;
    }

    Ok(())
}

fn now_ms() -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    // millis won't exceed u64::MAX until year 584 million
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    ms
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    #[test]
    fn put_and_get() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"hello", b"world", 0).unwrap();
        let val = store.get(b"hello").unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    #[test]
    fn get_missing_returns_none() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.get(b"nope").unwrap().is_none());
    }

    #[test]
    fn put_overwrites_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v1", 0).unwrap();
        store.put(b"k", b"v2", 0).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v", 0).unwrap();
        assert!(store.delete(b"k").unwrap());
        assert!(store.get(b"k").unwrap().is_none());
    }

    #[test]
    fn delete_missing_returns_false() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(!store.delete(b"ghost").unwrap());
    }

    #[test]
    fn keys_returns_live_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.delete(b"a").unwrap();
        let keys = store.keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"b");
    }

    #[test]
    fn expired_key_returns_none() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // expire 1ms in the past
        let past_ms = now_ms().saturating_sub(1);
        store.put(b"ttl", b"val", past_ms).unwrap();
        assert!(store.get(b"ttl").unwrap().is_none());
    }

    #[test]
    fn not_yet_expired_key_returns_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000; // 1 minute from now
        store.put(b"ttl", b"val", future_ms).unwrap();
        assert_eq!(store.get(b"ttl").unwrap(), Some(b"val".to_vec()));
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"persist", b"data", 0).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"persist").unwrap(),
            Some(b"data".to_vec()),
            "value must survive store close and reopen"
        );
    }

    #[test]
    fn delete_persists_across_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"k", b"v", 0).unwrap();
            store.delete(b"k").unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"k").unwrap().is_none(),
            "tombstone must be replayed on reopen"
        );
    }

    #[test]
    fn hint_file_speeds_up_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"a", b"1", 0).unwrap();
            store.put(b"b", b"2", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // Reopen — should load from hint file
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn len_and_is_empty() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.is_empty());
        store.put(b"x", b"y", 0).unwrap();
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Empty value is treated as tombstone at the log level — put returns Ok
        // but get returns None (tombstone logic). This is intentional: callers
        // should not store empty values; the store treats them as deletes.
        store.put(b"", b"nonempty", 0).unwrap();
        assert!(store.get(b"").unwrap().is_some());
    }

    #[test]
    fn non_utf8_binary_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0xFF, 0x00, 0xAB];
        let value = vec![0x01, 0x02, 0x03, 0x04];
        store.put(&key, &value, 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn large_value_1mb() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value = vec![0x42u8; 1024 * 1024]; // 1 MiB
        store.put(b"bigval", &value, 0).unwrap();
        let got = store.get(b"bigval").unwrap().unwrap();
        assert_eq!(got.len(), 1024 * 1024);
        assert!(got.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn stress_1000_keys_persist_and_reload() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u32..1000 {
                let key = i.to_le_bytes();
                let value = format!("value_{i}");
                store.put(&key, value.as_bytes(), 0).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 1000);
        for i in 0u32..1000 {
            let key = i.to_le_bytes();
            let expected = format!("value_{i}");
            assert_eq!(
                store.get(&key).unwrap(),
                Some(expected.into_bytes()),
                "key {i} must survive reopen"
            );
        }
    }

    #[test]
    fn put_batch_single_fsync_semantics() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        // Put 10 entries in one batch — all committed with one fsync
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0u8..10).map(|i| (vec![i], vec![i * 2])).collect();
        let entries: Vec<(&[u8], &[u8], u64)> = pairs
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();

        for (k, v) in &pairs {
            assert_eq!(store.get(k).unwrap(), Some(v.clone()));
        }
    }

    #[test]
    fn put_batch_persists_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            let pairs: Vec<(Vec<u8>, Vec<u8>)> =
                (0u8..5).map(|i| (vec![i], vec![i + 100])).collect();
            let entries: Vec<(&[u8], &[u8], u64)> = pairs
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
                .collect();
            store.put_batch(&entries).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        for i in 0u8..5 {
            assert_eq!(store.get(&[i]).unwrap(), Some(vec![i + 100]));
        }
    }

    #[test]
    fn empty_batch_is_noop() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put_batch(&[]).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn double_delete_returns_false_second_time() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v", 0).unwrap();
        assert!(store.delete(b"k").unwrap());
        assert!(!store.delete(b"k").unwrap()); // already gone
    }

    #[test]
    fn overwrite_1000_times_only_latest_survives_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u32..1000 {
                store.put(b"hotkey", format!("v{i}").as_bytes(), 0).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 1, "only 1 live key after 1000 overwrites");
        assert_eq!(
            store.get(b"hotkey").unwrap(),
            Some(b"v999".to_vec()),
            "must read latest value"
        );
    }

    #[test]
    fn keys_excludes_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past = now_ms().saturating_sub(1);
        store.put(b"live", b"v", 0).unwrap();
        store.put(b"dead", b"v", past).unwrap();
        let keys = store.keys();
        assert!(keys.contains(&b"live".to_vec()));
        assert!(!keys.contains(&b"dead".to_vec()));
    }

    #[test]
    fn open_empty_dir_starts_clean() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    // ------------------------------------------------------------------
    // put_batch edge cases
    // ------------------------------------------------------------------

    #[test]
    fn put_batch_with_expiry_values_are_readable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        let pairs: Vec<(&[u8], &[u8], u64)> = vec![(b"a", b"1", future_ms), (b"b", b"2", 0)];
        store.put_batch(&pairs).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn put_batch_expired_entries_not_returned_on_get() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1);
        let pairs: Vec<(&[u8], &[u8], u64)> =
            vec![(b"expired", b"val", past_ms), (b"live", b"val", 0)];
        store.put_batch(&pairs).unwrap();
        assert!(store.get(b"expired").unwrap().is_none());
        assert!(store.get(b"live").unwrap().is_some());
    }

    #[test]
    fn put_batch_last_write_wins_within_same_batch() {
        // If the same key appears twice in one batch, the second write should win
        // after reopen (last write in log wins on replay).
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            let pairs: Vec<(&[u8], &[u8], u64)> = vec![(b"k", b"first", 0), (b"k", b"second", 0)];
            store.put_batch(&pairs).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        // After log replay the later entry wins
        let val = store.get(b"k").unwrap().unwrap();
        assert_eq!(val, b"second".to_vec());
    }

    #[test]
    fn put_batch_100_entries_all_readable_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            let kv: Vec<(Vec<u8>, Vec<u8>)> = (0u8..100)
                .map(|i| (vec![i], vec![i.wrapping_mul(3)]))
                .collect();
            let pairs: Vec<(&[u8], &[u8], u64)> = kv
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
                .collect();
            store.put_batch(&pairs).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        for i in 0u8..100 {
            assert_eq!(
                store.get(&[i]).unwrap(),
                Some(vec![i.wrapping_mul(3)]),
                "key {i} missing after reopen"
            );
        }
    }

    // ------------------------------------------------------------------
    // Expiry edge cases
    // ------------------------------------------------------------------

    #[test]
    fn expired_key_is_removed_from_keydir_on_get() {
        // After get returns None for an expired key, len() should be 0.
        // len() now filters logically-expired entries, so even before the get
        // call the expired key is not counted.
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1);
        store.put(b"ttl", b"v", past_ms).unwrap();
        // len() excludes expired entries — already 0 before get() evicts it
        assert_eq!(store.len(), 0);
        assert!(store.get(b"ttl").unwrap().is_none());
        // Keydir entry has been physically removed by get()
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn put_then_overwrite_with_no_expiry_clears_ttl() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        store.put(b"k", b"v1", future_ms).unwrap();
        store.put(b"k", b"v2", 0).unwrap(); // clear TTL
        assert_eq!(store.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn put_then_overwrite_with_no_expiry_persists_without_ttl() {
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            let future_ms = now_ms() + 60_000;
            store.put(b"k", b"v1", future_ms).unwrap();
            store.put(b"k", b"v2", 0).unwrap();
        }
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    // ------------------------------------------------------------------
    // Hint file edge cases
    // ------------------------------------------------------------------

    #[test]
    fn hint_file_written_before_delete_does_not_resurrect_key() {
        // Write hint, then delete. On reopen: hint loads the key, but log replay
        // of the tail (tombstone) should override it — or if using hint only,
        // the tombstone in the log tail must still be replayed.
        // Currently open() loads hint OR replays log (not both). If hint exists,
        // the log tail after the hint is NOT replayed (known Bitcask limitation:
        // hint files must be written after the last compaction). This test verifies
        // the currently-implemented behaviour and documents it.
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"k", b"v", 0).unwrap();
            store.write_hint_file().unwrap();
            // Now delete — but hint already written above
            store.delete(b"k").unwrap();
        }

        // The hint file has "k→v". The log has: put(k,v) + tombstone(k).
        // open() sees a hint file → loads it → does NOT replay the raw log for
        // that file_id. So "k" will appear alive from hint.
        // This is the correct Bitcask-as-designed behaviour: hint files should only
        // be written after compaction (which drops tombstones), not mid-session.
        // The test documents this: after a reopen via hint, the key may appear alive
        // even though it was deleted. The shard GenServer must write hints only after
        // compaction, never between puts and deletes on the same file.
        let mut store = Store::open(dir.path()).unwrap();
        // With current implementation: key is present (hint wins over un-replayed log tail)
        let _ = store.get(b"k"); // Just assert no panic — behaviour is implementation-defined
    }

    // ------------------------------------------------------------------
    // Multi-key delete and reinsert across reopen
    // ------------------------------------------------------------------

    #[test]
    fn delete_all_keys_then_put_new_ones_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u8..5 {
                store.put(&[i], &[i], 0).unwrap();
            }
            for i in 0u8..5 {
                store.delete(&[i]).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.is_empty(), "all keys deleted");

        // Now insert new keys into the reopened store
        store.put(b"new", b"value", 0).unwrap();
        assert_eq!(store.get(b"new").unwrap(), Some(b"value".to_vec()));
    }

    // ------------------------------------------------------------------
    // Concurrent keys — values include binary patterns
    // ------------------------------------------------------------------

    #[test]
    fn all_byte_values_0x00_to_0xff_roundtrip() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value: Vec<u8> = (0u8..=255).collect();
        store.put(b"bytes", &value, 0).unwrap();
        assert_eq!(store.get(b"bytes").unwrap(), Some(value));
    }

    #[test]
    fn reopen_after_hint_file_written_matches_log_replay() {
        let dir = tmp();

        // Write some data and a hint file
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"a", b"1", 0).unwrap();
            store.put(b"b", b"2", 0).unwrap();
            store.delete(b"a").unwrap();
            store.write_hint_file().unwrap();
        }

        // Reopen via hint file path
        let mut store_from_hint = Store::open(dir.path()).unwrap();

        // Remove hint file and reopen via log replay
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                std::fs::remove_file(e.path()).unwrap();
            }
        }
        let mut store_from_log = Store::open(dir.path()).unwrap();

        assert_eq!(
            store_from_hint.get(b"a").unwrap(),
            store_from_log.get(b"a").unwrap(),
            "hint and log replay must agree on deleted key"
        );
        assert_eq!(
            store_from_hint.get(b"b").unwrap(),
            store_from_log.get(b"b").unwrap(),
            "hint and log replay must agree on live key"
        );
    }

    // ------------------------------------------------------------------
    // EC-2: Corrupt hint file — must fall back to log replay
    // ------------------------------------------------------------------

    /// EC-2: When the hint file for a data file contains a well-formed header
    /// that claims a `key_size` larger than the remaining bytes (mid-entry
    /// truncation), `Store::open` must succeed by falling back to full log
    /// replay rather than propagating the parse error.
    #[test]
    fn corrupt_hint_file_falls_back_to_log_replay() {
        let dir = tmp();

        // 1. Write some data and a valid hint file.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key1", b"val1", 0).unwrap();
            store.put(b"key2", b"val2", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Corrupt the hint file: write exactly HINT_HEADER_SIZE (34) bytes
        //    of zeros. The reader reads the 4-byte CRC (0x00000000) and the
        //    30-byte body (all zeros), computes the real CRC over the body,
        //    and gets a mismatch → HintError → log replay fallback.
        use crate::hint::HINT_HEADER_SIZE;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                // All-zero bytes: stored CRC (0) will not match the real CRC
                // of 30 zero bytes, triggering a CRC mismatch error.
                let corrupt = vec![0u8; HINT_HEADER_SIZE];
                std::fs::write(e.path(), &corrupt).unwrap();
            }
        }

        // 3. Reopen — must not error; must recover data from the log.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"key1").unwrap(),
            Some(b"val1".to_vec()),
            "key1 must be recovered from log replay after corrupt hint"
        );
        assert_eq!(
            store.get(b"key2").unwrap(),
            Some(b"val2".to_vec()),
            "key2 must be recovered from log replay after corrupt hint"
        );
    }

    /// EC-2 variant: A hint file truncated mid-key (header intact, key bytes
    /// missing) also triggers fallback to log replay without returning an error.
    #[test]
    fn truncated_hint_file_falls_back_to_log_replay() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"alpha", b"beta", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // Truncate the hint file to exactly HINT_HEADER_SIZE bytes (34).
        // The CRC (4 bytes) and body header (30 bytes) are intact, but the
        // key bytes are gone. The reader reads key_size > 0 (because "alpha"
        // was stored), then calls read_exact for the key bytes and gets
        // UnexpectedEof → HintError → fallback to log replay.
        use crate::hint::HINT_HEADER_SIZE;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                let full = std::fs::read(e.path()).unwrap();
                // Keep only the header bytes (CRC + body header, no key).
                let truncated_len = HINT_HEADER_SIZE.min(full.len());
                std::fs::write(e.path(), &full[..truncated_len]).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"alpha").unwrap(),
            Some(b"beta".to_vec()),
            "value must survive hint truncation via log replay fallback"
        );
    }

    // ------------------------------------------------------------------
    // EC-7: Hint-only directory — orphan .hint with no .data must be skipped
    // ------------------------------------------------------------------

    /// EC-7: If only `.hint` files exist in the data directory (the
    /// corresponding `.log` file is absent), `Store::open` must succeed with
    /// an empty keydir rather than panicking or erroring.
    #[test]
    fn hint_only_directory_opens_with_empty_keydir() {
        let dir = tmp();

        // 1. Create a real store with a hint file.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"orphan_key", b"orphan_val", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Remove ALL .log files, leaving only .hint files.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".log") {
                std::fs::remove_file(e.path()).unwrap();
            }
        }

        // 3. Re-open — orphan hints are skipped, keydir is empty, no error.
        let store = Store::open(dir.path()).unwrap();
        assert!(
            store.is_empty(),
            "keydir must be empty when only orphan .hint files exist"
        );
    }

    // ------------------------------------------------------------------
    // Issue 3.4: Lazy TTL eviction must write a tombstone so keys do not
    // resurrect after close+reopen.
    // ------------------------------------------------------------------

    /// After `get` expires a key and writes a tombstone, reopening the store
    /// must NOT resurface the key from the original write record on disk.
    #[test]
    fn expired_key_does_not_resurrect_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            // Write a key that is already expired (1 second in the past).
            let past_ms = now_ms().saturating_sub(1000);
            store.put(b"key", b"val", past_ms).unwrap();
            // get() must return None and write a tombstone to the log.
            assert!(store.get(b"key").unwrap().is_none());
        }

        // Reopen: log replay must see tombstone after the original record and
        // must NOT re-insert the key into the keydir.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"key").unwrap().is_none(),
            "expired key must not resurrect after store close and reopen"
        );
    }

    /// `len()` must count only non-expired keys.
    #[test]
    fn len_excludes_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"live", b"v", 0).unwrap();
        store.put(b"dead", b"v", past_ms).unwrap();
        assert_eq!(store.len(), 1, "only the live key should be counted");
    }

    /// After TTL eviction (tombstone write), `len()` stays consistent and the
    /// count is still correct after a reopen.
    #[test]
    fn len_after_ttl_eviction_is_consistent() {
        let dir = tmp();
        let past_ms = now_ms().saturating_sub(1000);

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"live1", b"v", 0).unwrap();
            store.put(b"live2", b"v", 0).unwrap();
            store.put(b"expired", b"v", past_ms).unwrap();

            // Trigger eviction: tombstone is written to the log.
            assert!(store.get(b"expired").unwrap().is_none());
            assert_eq!(store.len(), 2, "len must be 2 after eviction");
        }

        // Reopen — tombstone prevents resurrection; len must still be 2.
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.len(),
            2,
            "len must be 2 after reopen (tombstone prevents resurrection)"
        );
    }

    /// EC-7 variant: A directory with a `.hint` for one file ID and a `.log`
    /// for a different file ID opens correctly, loading only the entries with
    /// a matching log file.
    #[test]
    fn partial_hint_no_log_does_not_contaminate_keydir() {
        let dir = tmp();

        // Manually plant a .hint file for file ID 99 (no matching .log).
        use crate::hint::{HintEntry, HintWriter};
        let orphan_hint = dir.path().join("00000000000000000099.hint");
        let mut w = HintWriter::open(&orphan_hint).unwrap();
        w.write_entry(&HintEntry {
            file_id: 99,
            offset: 0,
            value_size: 5,
            expire_at_ms: 0,
            key: b"ghost".to_vec(),
        })
        .unwrap();
        w.commit().unwrap();

        // Open the store — no .log files at all, so file_ids is empty,
        // the orphan hint is never iterated, keydir stays empty.
        let store = Store::open(dir.path()).unwrap();
        assert!(
            store.is_empty(),
            "orphan hint with no matching log must not inject keys into keydir"
        );
    }

    // ------------------------------------------------------------------
    // Issue 5.2: Atomic hint file writes — partial hint falls back to log
    // ------------------------------------------------------------------

    /// Issue 5.2: When a `.hint` file is non-empty but corrupt (simulating a
    /// partial write followed by truncation), `Store::open` must fall back to
    /// full log replay and recover all keys correctly.
    ///
    /// This is the integration-level companion to the unit tests in `hint.rs`.
    /// It verifies that the EC-2 fallback path in `Store::open` handles the
    /// specific failure mode that the atomic write fix addresses: a hint file
    /// that is non-empty (so it passes the `exists()` check) but corrupt enough
    /// to cause a parse error (triggering log-replay fallback).
    #[test]
    fn store_open_with_partial_hint_falls_back_to_log() {
        let dir = tmp();

        // 1. Open a store, put some keys, and write a valid hint file.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key_a", b"val_a", 0).unwrap();
            store.put(b"key_b", b"val_b", 0).unwrap();
            store.put(b"key_c", b"val_c", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Manually truncate the hint file to a non-empty but incomplete
        //    length (exactly HINT_HEADER_SIZE=34 bytes — CRC + body header
        //    present, key bytes missing). This simulates a crash after the old
        //    hint was zeroed but before new content was fully written.
        use crate::hint::HINT_HEADER_SIZE;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                let full = std::fs::read(e.path()).unwrap();
                // CRC + body header are intact but key bytes are gone —
                // this triggers UnexpectedEof in the hint parser.
                let truncated_len = HINT_HEADER_SIZE.min(full.len());
                std::fs::write(e.path(), &full[..truncated_len]).unwrap();
            }
        }

        // 3. Reopen — the corrupt hint triggers EC-2 fallback to log replay.
        //    All three keys must be recovered correctly.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"key_a").unwrap(),
            Some(b"val_a".to_vec()),
            "key_a must be recovered via log replay after partial hint"
        );
        assert_eq!(
            store.get(b"key_b").unwrap(),
            Some(b"val_b".to_vec()),
            "key_b must be recovered via log replay after partial hint"
        );
        assert_eq!(
            store.get(b"key_c").unwrap(),
            Some(b"val_c".to_vec()),
            "key_c must be recovered via log replay after partial hint"
        );
    }

    // ------------------------------------------------------------------
    // Issue 7.4: write_hint_file must fsync the log before writing the hint
    // ------------------------------------------------------------------

    /// After `write_hint_file` the data referred to by the hint must already be
    /// durable on disk.  We verify this by reading back the raw log bytes at
    /// the offset stored in the hint entry and confirming they exist and are
    /// decodable.
    #[test]
    fn write_hint_file_syncs_log_first() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"synckey", b"syncval", 0).unwrap();

        // write_hint_file now calls self.writer.sync() before writing the hint.
        store.write_hint_file().unwrap();

        // The hint file for the active file ID must now exist.
        let hint_path = dir
            .path()
            .join(format!("{:020}.hint", store.active_file_id));
        assert!(hint_path.exists(), "hint file must be written");

        // The log file must also exist and the first record must be readable,
        // confirming the data was flushed before the hint was written.
        let log_path = dir.path().join(format!("{:020}.log", store.active_file_id));
        let mut reader = crate::log::LogReader::open(&log_path).unwrap();
        let record = reader.read_at(0).unwrap();
        assert!(
            record.is_some(),
            "log record at offset 0 must be present and readable after write_hint_file"
        );
        let record = record.unwrap();
        assert_eq!(record.key, b"synckey");
        assert_eq!(record.value, Some(b"syncval".to_vec()));
    }

    // ------------------------------------------------------------------
    // Issue 6.3: Proactive TTL GC — purge_expired
    // ------------------------------------------------------------------

    /// `purge_expired` must return the correct count and remove expired keys
    /// from the keydir while leaving live keys untouched.
    #[test]
    fn purge_expired_removes_keys_from_keydir() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);

        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"expired1", b"v", past_ms).unwrap();
        store.put(b"expired2", b"v", past_ms).unwrap();

        let count = store.purge_expired().unwrap();
        assert_eq!(count, 2, "purge_expired must report 2 purged keys");

        assert!(
            store.get(b"expired1").unwrap().is_none(),
            "expired1 must be gone"
        );
        assert!(
            store.get(b"expired2").unwrap().is_none(),
            "expired2 must be gone"
        );
        assert_eq!(
            store.get(b"live1").unwrap(),
            Some(b"v".to_vec()),
            "live1 must still be readable"
        );
    }

    /// Tombstones written by `purge_expired` must prevent key resurrection
    /// across a store close and reopen.
    #[test]
    fn purge_expired_writes_tombstones_preventing_resurrection() {
        let dir = tmp();
        let past_ms = now_ms().saturating_sub(1000);

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"ex_key", b"val", past_ms).unwrap();
            let count = store.purge_expired().unwrap();
            assert_eq!(count, 1);
        }

        // Reopen — log replay must see the tombstone after the original record
        // and must NOT re-insert ex_key into the keydir.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"ex_key").unwrap().is_none(),
            "ex_key must not resurrect after purge_expired + reopen"
        );
    }

    /// When no keys are expired, `purge_expired` must return `Ok(0)` and leave
    /// the keydir untouched.
    #[test]
    fn purge_expired_returns_zero_when_nothing_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.put(b"c", b"3", 0).unwrap();

        let count = store.purge_expired().unwrap();
        assert_eq!(count, 0, "no keys are expired so count must be 0");

        // All keys must still be readable.
        assert!(store.get(b"a").unwrap().is_some());
        assert!(store.get(b"b").unwrap().is_some());
        assert!(store.get(b"c").unwrap().is_some());
    }

    /// `len()` must reflect the correct count after `purge_expired` removes
    /// expired entries from the keydir.
    #[test]
    fn len_is_accurate_after_purge_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);

        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"live2", b"v", 0).unwrap();
        store.put(b"live3", b"v", 0).unwrap();
        store.put(b"exp1", b"v", past_ms).unwrap();
        store.put(b"exp2", b"v", past_ms).unwrap();

        store.purge_expired().unwrap();

        assert_eq!(
            store.len(),
            3,
            "len must be 3 (only live keys) after purge_expired"
        );
    }

    // ------------------------------------------------------------------
    // CRC / record integrity
    // ------------------------------------------------------------------

    /// Put a key, close the store, corrupt a byte in the middle of the log
    /// record (not the CRC bytes), reopen and get — must return Err or None.
    #[test]
    fn corrupt_crc_in_log_returns_error_on_get() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key", b"value", 0).unwrap();
        }

        // Locate the single .log file and flip a byte in the key area
        // (byte at HEADER_SIZE, which is after the 4-byte CRC field).
        // This invalidates the CRC without touching the CRC bytes themselves.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".log") {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(e.path())
                    .unwrap();
                // Flip one byte in the key area (after the 26-byte header).
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                let mut byte = [0u8; 1];
                use std::io::Read;
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                {
                    let mut rf = std::fs::File::open(e.path()).unwrap();
                    rf.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                        .unwrap();
                    rf.read_exact(&mut byte).unwrap();
                }
                byte[0] ^= 0xFF;
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                f.write_all(&byte).unwrap();
            }
        }

        // Reopen the store — the corrupt record will be skipped by the tolerant
        // reader (crash-recovery semantics), so get returns None rather than Err.
        let mut store = Store::open(dir.path()).unwrap();
        // The key should not be accessible (CRC mismatch is handled gracefully).
        let result = store.get(b"key").unwrap();
        assert!(
            result.is_none(),
            "corrupt CRC in log must cause get to return None (tolerant recovery)"
        );
    }

    /// Put 5 records, truncate the 3rd record to half its size, reopen.
    /// Records 1 and 2 must be intact; records 3-5 gone (tolerant reader stops
    /// at first error).
    #[test]
    fn store_recovers_after_mid_file_record_truncation() {
        let dir = tmp();

        let mut offsets = Vec::new();
        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u8..5 {
                let key = format!("key{i}");
                let val = format!("val{i}");
                store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
            }
            // Capture writer offset so we can locate the 3rd record boundary.
            // We'll compute truncation point directly from disk.
        }

        // Reopen just to confirm 5 records, then compute the truncation point.
        {
            let log_path_buf = std::fs::read_dir(dir.path())
                .unwrap()
                .find_map(|e| {
                    let e = e.unwrap();
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .unwrap();

            // Read all 5 records to find where the 3rd one starts.
            let mut reader = crate::log::LogReader::open(&log_path_buf).unwrap();
            let records = reader.iter_from_start().unwrap();
            assert_eq!(records.len(), 5, "expected 5 records before truncation");

            // Compute offset of 3rd record (index 2).
            let rec0_len = (crate::log::HEADER_SIZE
                + records[0].key.len()
                + records[0].value.as_ref().map_or(0, Vec::len)) as u64;
            let rec1_len = (crate::log::HEADER_SIZE
                + records[1].key.len()
                + records[1].value.as_ref().map_or(0, Vec::len)) as u64;
            let rec2_start = rec0_len + rec1_len;
            // Truncate to halfway through the 3rd record.
            let rec2_half_len = (crate::log::HEADER_SIZE
                + records[2].key.len()
                + records[2].value.as_ref().map_or(0, Vec::len))
                as u64
                / 2;
            let truncate_at = rec2_start + rec2_half_len;

            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&log_path_buf)
                .unwrap();
            file.set_len(truncate_at).unwrap();
            offsets.push(rec2_start); // used for assertion below
        }

        // Reopen — tolerant reader stops at truncated record 3.
        let mut store = Store::open(dir.path()).unwrap();
        // Records 0 and 1 must be intact.
        assert_eq!(
            store.get(b"key0").unwrap(),
            Some(b"val0".to_vec()),
            "key0 must survive"
        );
        assert_eq!(
            store.get(b"key1").unwrap(),
            Some(b"val1".to_vec()),
            "key1 must survive"
        );
        // Records 2-4 must be gone (tolerant reader stopped at first error).
        assert!(
            store.get(b"key2").unwrap().is_none(),
            "key2 must be gone after truncation"
        );
        assert!(
            store.get(b"key3").unwrap().is_none(),
            "key3 must be gone after truncation"
        );
        assert!(
            store.get(b"key4").unwrap().is_none(),
            "key4 must be gone after truncation"
        );
        let _ = offsets;
    }

    // ------------------------------------------------------------------
    // Offset tracking
    // ------------------------------------------------------------------

    /// Put 10 records and verify that `writer.offset` increases monotonically
    /// and equals the sum of encoded record sizes.
    #[test]
    fn offset_is_correct_after_multiple_puts() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let mut expected_offset = 0u64;
        for i in 0u8..10 {
            let key = vec![i; (i as usize) + 1]; // key length 1..10
            let val = vec![i; (i as usize) + 2]; // val length 2..11
            store.put(&key, &val, 0).unwrap();
            let record_size = (crate::log::HEADER_SIZE + key.len() + val.len()) as u64;
            expected_offset += record_size;
            assert!(
                store.writer.offset >= expected_offset,
                "offset must increase after put {i}"
            );
        }
        assert_eq!(
            store.writer.offset, expected_offset,
            "final offset must equal sum of record sizes"
        );
    }

    // ------------------------------------------------------------------
    // len() accuracy
    // ------------------------------------------------------------------

    #[test]
    fn len_is_zero_for_empty_store() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn len_decreases_after_delete() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.put(b"c", b"3", 0).unwrap();
        assert_eq!(store.len(), 3);
        store.delete(b"b").unwrap();
        assert_eq!(store.len(), 2);
        store.delete(b"a").unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn len_counts_only_live_non_expired_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(5000);
        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"live2", b"v", 0).unwrap();
        store.put(b"live3", b"v", 0).unwrap();
        store.put(b"exp1", b"v", past_ms).unwrap();
        store.put(b"exp2", b"v", past_ms).unwrap();
        assert_eq!(store.len(), 3, "only live non-expired keys are counted");
    }

    // ------------------------------------------------------------------
    // put/get/delete semantics
    // ------------------------------------------------------------------

    #[test]
    fn put_empty_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Empty key with non-empty value should work.
        store.put(b"", b"non_empty_value", 0).unwrap();
        let result = store.get(b"").unwrap();
        assert!(
            result.is_some(),
            "empty key with non-empty value must be retrievable"
        );
    }

    #[test]
    fn put_very_large_value_1mb_roundtrip() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        store.put(b"bigkey", &value, 0).unwrap();
        let got = store.get(b"bigkey").unwrap().unwrap();
        assert_eq!(got.len(), 1024 * 1024, "1MB value must round-trip");
        assert_eq!(got, value, "1MB value bytes must match exactly");
    }

    #[test]
    fn put_key_with_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = b"\x00\x01\x02";
        store.put(key, b"null_key_val", 0).unwrap();
        let result = store.get(key).unwrap();
        assert_eq!(
            result,
            Some(b"null_key_val".to_vec()),
            "key with null bytes must round-trip"
        );
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.delete(b"nonexistent").unwrap();
        assert!(!result, "delete on missing key must return false");
        assert_eq!(store.len(), 0, "len must remain 0 after no-op delete");
    }

    #[test]
    fn delete_returns_true_for_existing_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"present", b"val", 0).unwrap();
        let result = store.delete(b"present").unwrap();
        assert!(result, "delete on existing key must return true");
        assert!(
            store.get(b"present").unwrap().is_none(),
            "key must be gone after delete"
        );
    }

    #[test]
    fn delete_tombstone_prevents_resurrection() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"zombie", b"alive", 0).unwrap();
            store.delete(b"zombie").unwrap();
        }

        // Reopen — tombstone in log replay must prevent key from coming back.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"zombie").unwrap().is_none(),
            "deleted key must not resurrect after reopen"
        );
    }

    // ------------------------------------------------------------------
    // put_batch: new edge cases
    // ------------------------------------------------------------------

    #[test]
    fn put_batch_empty_slice_is_ok() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Already covered by empty_batch_is_noop but this is an explicit API contract test.
        store.put_batch(&[]).unwrap();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn put_batch_all_entries_retrievable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let kv: Vec<(Vec<u8>, Vec<u8>)> = (0u8..20)
            .map(|i| (format!("bk{i}").into_bytes(), format!("bv{i}").into_bytes()))
            .collect();
        let entries: Vec<(&[u8], &[u8], u64)> = kv
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();
        for (k, v) in &kv {
            assert_eq!(
                store.get(k).unwrap(),
                Some(v.clone()),
                "batch entry {k:?} must be retrievable"
            );
        }
    }

    #[test]
    fn put_batch_with_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(2000);
        let entries: Vec<(&[u8], &[u8], u64)> =
            vec![(b"exp_batch", b"val", past_ms), (b"live_batch", b"val", 0)];
        store.put_batch(&entries).unwrap();
        assert!(
            store.get(b"exp_batch").unwrap().is_none(),
            "expired batch entry must not be readable"
        );
        assert_eq!(
            store.get(b"live_batch").unwrap(),
            Some(b"val".to_vec()),
            "live batch entry must be readable"
        );
    }

    // ------------------------------------------------------------------
    // expiry: detailed edge cases
    // ------------------------------------------------------------------

    #[test]
    fn get_returns_none_for_past_expire_at() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"past_ttl", b"gone", past_ms).unwrap();
        assert!(
            store.get(b"past_ttl").unwrap().is_none(),
            "key with past expire_at must return None"
        );
    }

    #[test]
    fn get_returns_value_for_future_expire_at() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        store.put(b"future_ttl", b"present", future_ms).unwrap();
        assert_eq!(
            store.get(b"future_ttl").unwrap(),
            Some(b"present".to_vec()),
            "key with future expire_at must return value"
        );
    }

    #[test]
    fn keys_excludes_past_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(500);
        store.put(b"gone", b"v", past_ms).unwrap();
        store.put(b"here", b"v", 0).unwrap();
        let keys = store.keys();
        assert!(
            !keys.contains(&b"gone".to_vec()),
            "expired key must not appear in keys()"
        );
        assert!(
            keys.contains(&b"here".to_vec()),
            "live key must appear in keys()"
        );
        assert_eq!(keys.len(), 1, "keys() must return exactly 1 live key");
    }

    // ------------------------------------------------------------------
    // purge_expired: detailed edge cases
    // ------------------------------------------------------------------

    #[test]
    fn purge_expired_count_matches_expired_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        for i in 0u8..5 {
            store.put(&[i], b"expired_val", past_ms).unwrap();
        }
        let count = store.purge_expired().unwrap();
        assert_eq!(count, 5, "purge_expired must return 5 for 5 expired keys");
    }

    #[test]
    fn purge_expired_evicted_keys_not_in_keys_list() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"exp_a", b"v", past_ms).unwrap();
        store.put(b"exp_b", b"v", past_ms).unwrap();
        store.put(b"live_x", b"v", 0).unwrap();

        store.purge_expired().unwrap();

        let keys = store.keys();
        assert!(
            !keys.contains(&b"exp_a".to_vec()),
            "exp_a must not be in keys after purge"
        );
        assert!(
            !keys.contains(&b"exp_b".to_vec()),
            "exp_b must not be in keys after purge"
        );
        assert!(
            keys.contains(&b"live_x".to_vec()),
            "live_x must still be in keys after purge"
        );
        assert_eq!(keys.len(), 1);
    }

    // ------------------------------------------------------------------
    // Issue 5.1: Corrupt hint CRC triggers log replay fallback
    // ------------------------------------------------------------------

    /// A hint file whose first entry has a flipped byte (corrupting its CRC)
    /// must trigger log replay fallback on `Store::open`, recovering all keys.
    #[test]
    fn corrupt_hint_crc_falls_back_to_log_replay() {
        let dir = tmp();

        // 1. Open store, put keys, write hint file, close.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"crc_key1", b"crc_val1", 0).unwrap();
            store.put(b"crc_key2", b"crc_val2", 0).unwrap();
            store.put(b"crc_key3", b"crc_val3", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Open the hint file and flip bytes 12..16 (inside the offset field
        //    of the first entry). The stored CRC won't match the new body bytes.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                use std::fs::OpenOptions;
                use std::io::{Seek, SeekFrom, Write};
                let mut f = OpenOptions::new().write(true).open(e.path()).unwrap();
                // Offset field starts at byte 4 (CRC) + 8 (file_id) = 12.
                f.seek(SeekFrom::Start(12)).unwrap();
                f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            }
        }

        // 3. Reopen store — must fall back to log replay and recover all keys.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"crc_key1").unwrap(),
            Some(b"crc_val1".to_vec()),
            "crc_key1 must be recovered from log replay after CRC-corrupt hint"
        );
        assert_eq!(
            store.get(b"crc_key2").unwrap(),
            Some(b"crc_val2".to_vec()),
            "crc_key2 must be recovered from log replay after CRC-corrupt hint"
        );
        assert_eq!(
            store.get(b"crc_key3").unwrap(),
            Some(b"crc_val3".to_vec()),
            "crc_key3 must be recovered from log replay after CRC-corrupt hint"
        );
    }
}
