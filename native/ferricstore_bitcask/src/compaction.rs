#![allow(dead_code)]

//! Compaction — merges old log files into a single file, removes stale and
//! deleted entries, and writes a hint file for fast startup.
//!
//! After compaction the caller should:
//!   1. Replace the keydir entries for the compacted file IDs with the new ones.
//!   2. Delete the old data + hint files.
//!   3. Point the `Store` at the new compacted file.

use std::fs;
use std::path::{Path, PathBuf};

use crate::hint::{HintEntry, HintWriter};
use crate::keydir::KeyDir;
use crate::log::{LogReader, LogWriter, HEADER_SIZE};

#[derive(Debug)]
pub struct CompactionError(pub String);

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionError: {}", self.0)
    }
}

impl From<crate::log::LogError> for CompactionError {
    fn from(e: crate::log::LogError) -> Self {
        CompactionError(e.to_string())
    }
}

impl From<crate::hint::HintError> for CompactionError {
    fn from(e: crate::hint::HintError) -> Self {
        CompactionError(e.to_string())
    }
}

impl From<std::io::Error> for CompactionError {
    fn from(e: std::io::Error) -> Self {
        CompactionError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CompactionError>;

/// Result of a compaction run.
pub struct CompactionOutput {
    /// ID of the newly written merged data file.
    pub new_file_id: u64,
    /// Absolute path of the new data file.
    pub new_log_path: PathBuf,
    /// Absolute path of the new hint file.
    pub new_hint_path: PathBuf,
    /// Number of live records written to the new file.
    pub records_written: usize,
    /// Number of stale / deleted records dropped.
    pub records_dropped: usize,
}

/// Merge all log files listed in `file_ids` into a single new file.
///
/// Only records whose key is still live in `keydir` (same `file_id` + offset)
/// are copied. Expired and deleted keys are dropped.
///
/// The caller is responsible for updating the `Store`'s keydir and deleting
/// the old files after this returns successfully.
///
/// # Errors
///
/// Returns a `CompactionError` if any log file cannot be read, the new merged
/// data file or hint file cannot be written, or an I/O error occurs during
/// sync/flush.
pub fn compact(
    data_dir: &Path,
    file_ids: &[u64],
    keydir: &KeyDir,
    new_file_id: u64,
    now_ms: u64,
) -> Result<CompactionOutput> {
    let new_log_path = log_path(data_dir, new_file_id);
    let new_hint_path = hint_path(data_dir, new_file_id);

    let mut writer = LogWriter::open(&new_log_path, new_file_id)?;
    let mut hint_writer = HintWriter::open(&new_hint_path)?;

    let mut records_written = 0usize;
    let mut records_dropped = 0usize;

    for &fid in file_ids {
        let source_log = log_path(data_dir, fid);
        if !source_log.exists() {
            continue;
        }

        let mut reader = LogReader::open(&source_log)?;
        let mut offset: u64 = 0;

        let records = reader.iter_from_start()?;

        for record in records {
            let record_len = (HEADER_SIZE
                + record.key.len()
                + record.value.as_ref().map_or(0, Vec::len))
                as u64;

            // Only copy if this offset is still the live entry in the keydir
            let is_live = keydir
                .get(&record.key)
                .is_some_and(|e| e.file_id == fid && e.offset == offset);

            let is_expired =
                record.expire_at_ms != 0 && record.expire_at_ms <= now_ms;

            if is_live && !is_expired {
                if let Some(ref value) = record.value {
                    let new_offset =
                        writer.write(&record.key, value, record.expire_at_ms)?;
                    #[allow(clippy::cast_possible_truncation)]
                    hint_writer.write_entry(&HintEntry {
                        file_id: new_file_id,
                        offset: new_offset,
                        value_size: value.len() as u32,
                        expire_at_ms: record.expire_at_ms,
                        key: record.key,
                    })?;
                    records_written += 1;
                } else {
                    records_dropped += 1; // tombstone for a live key — skip
                }
            } else {
                records_dropped += 1;
            }

            offset += record_len;
        }
    }

    writer.sync()?;
    hint_writer.flush()?;

    Ok(CompactionOutput {
        new_file_id,
        new_log_path,
        new_hint_path,
        records_written,
        records_dropped,
    })
}

/// Delete the old data and hint files after a successful compaction.
///
/// # Errors
///
/// Returns a `CompactionError` if any file cannot be removed due to an I/O error.
pub fn remove_old_files(data_dir: &Path, file_ids: &[u64]) -> Result<()> {
    for &fid in file_ids {
        let lp = log_path(data_dir, fid);
        let hp = hint_path(data_dir, fid);
        if lp.exists() {
            fs::remove_file(&lp)?;
        }
        if hp.exists() {
            fs::remove_file(&hp)?;
        }
    }
    Ok(())
}

fn log_path(data_dir: &Path, file_id: u64) -> PathBuf {
    data_dir.join(format!("{file_id:020}.log"))
}

fn hint_path(data_dir: &Path, file_id: u64) -> PathBuf {
    data_dir.join(format!("{file_id:020}.hint"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hint::HintReader;
    use crate::keydir::KeyEntry;
    use crate::log::LogWriter;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn make_log(dir: &Path, file_id: u64, writes: &[(&[u8], &[u8])]) -> u64 {
        let path = log_path(dir, file_id);
        let mut w = LogWriter::open(&path, file_id).unwrap();
        let mut last_offset = 0u64;
        for (k, v) in writes {
            last_offset = w.write(k, v, 0).unwrap();
        }
        w.sync().unwrap();
        last_offset
    }

    #[test]
    fn compact_single_file_no_stale() {
        let dir = tmp();
        let last_offset = make_log(dir.path(), 1, &[(b"a", b"1"), (b"b", b"2")]);

        // Build a keydir pointing to the latest records
        use crate::log::HEADER_SIZE;
        let off_a = 0u64;
        let off_b = (HEADER_SIZE + 1 + 1) as u64; // after first record

        let mut kd = KeyDir::new();
        kd.put(
            b"a".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_a,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        kd.put(
            b"b".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_b,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 2);
        assert_eq!(out.records_dropped, 0);
        assert!(out.new_log_path.exists());
        assert!(out.new_hint_path.exists());

        // Verify hint file has 2 entries
        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        let hints = hr.read_all().unwrap();
        assert_eq!(hints.len(), 2);
        let _ = last_offset; // suppress unused warning
    }

    #[test]
    fn compact_drops_overwritten_entries() {
        let dir = tmp();
        // Write "k" twice — only the second write is live
        use crate::log::HEADER_SIZE;
        let off_v1 = 0u64;
        let off_v2 = (HEADER_SIZE + 1 + 2) as u64; // after "k"/"v1" record

        make_log(dir.path(), 1, &[(b"k", b"v1"), (b"k", b"v2")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_v2,
                value_size: 2,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = off_v1;

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 1, "only latest write survives");
        assert_eq!(out.records_dropped, 1, "stale write is dropped");
    }

    #[test]
    fn compact_drops_deleted_entries() {
        let dir = tmp();
        // Write "k", then delete it — keydir has no entry for "k"
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.write_tombstone(b"k").unwrap();
        w.sync().unwrap();

        let kd = KeyDir::new(); // empty — key is deleted

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 2); // both the write and the tombstone
    }

    #[test]
    fn compact_drops_expired_entries() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"ttl", b"val", 1000).unwrap(); // expires at ms=1000
        w.sync().unwrap();

        use crate::log::HEADER_SIZE;
        let mut kd = KeyDir::new();
        kd.put(
            b"ttl".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 3,
                expire_at_ms: 1000,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 5000).unwrap(); // now_ms=5000 > 1000
        let _ = HEADER_SIZE;

        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 1);
    }

    #[test]
    fn compact_merges_multiple_files() {
        let dir = tmp();
        use crate::log::HEADER_SIZE;

        // file 1: "a"="1"
        make_log(dir.path(), 1, &[(b"a", b"1")]);
        // file 2: "b"="2"
        make_log(dir.path(), 2, &[(b"b", b"2")]);

        let off_a = 0u64;
        let off_b = 0u64;

        let mut kd = KeyDir::new();
        kd.put(
            b"a".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_a,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        kd.put(
            b"b".to_vec(),
            KeyEntry {
                file_id: 2,
                offset: off_b,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = HEADER_SIZE;

        let out = compact(dir.path(), &[1, 2], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 2);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn remove_old_files_cleans_up() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"k", b"v")]);
        let hint = hint_path(dir.path(), 1);
        // create a dummy hint file
        std::fs::write(&hint, b"dummy").unwrap();

        assert!(log_path(dir.path(), 1).exists());
        assert!(hint.exists());

        remove_old_files(dir.path(), &[1]).unwrap();

        assert!(!log_path(dir.path(), 1).exists());
        assert!(!hint.exists());
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn compact_empty_file_list_produces_empty_output() {
        let dir = tmp();
        let kd = KeyDir::new();
        let out = compact(dir.path(), &[], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 0);
        // Output files are still created (empty)
        assert!(out.new_log_path.exists());
        assert!(out.new_hint_path.exists());
    }

    #[test]
    fn compact_missing_file_id_is_skipped_gracefully() {
        let dir = tmp();
        // file_id 42 does not exist on disk
        let kd = KeyDir::new();
        // Should not error — just skip the missing file
        let out = compact(dir.path(), &[42], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn compact_all_entries_expired_produces_empty_output() {
        let dir = tmp();
        // Write 5 entries all expiring at ms=100
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut offsets = Vec::new();
        for i in 0u8..5 {
            offsets.push(w.write(&[i], &[i], 100).unwrap());
        }
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        for (i, &offset) in offsets.iter().enumerate() {
            kd.put(
                vec![i as u8],
                KeyEntry {
                    file_id: 1,
                    offset,
                    value_size: 1,
                    expire_at_ms: 100,
                    ref_bit: false,
                },
            );
        }

        // now_ms=9999 — all expired
        let out = compact(dir.path(), &[1], &kd, 99, 9999).unwrap();
        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 5);
    }

    #[test]
    fn compact_not_yet_expired_entries_are_kept() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"k", b"v", 9_999_999_999).unwrap(); // far future
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset,
                value_size: 1,
                expire_at_ms: 9_999_999_999,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap(); // now_ms=0
        assert_eq!(out.records_written, 1);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn compact_output_file_id_must_not_collide_with_inputs() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"k", b"v")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        // new_file_id=100 must be different from input file_id=1
        let out = compact(dir.path(), &[1], &kd, 100, 0).unwrap();
        assert_eq!(out.new_file_id, 100);
        assert!(out.new_log_path.exists());
        // Input file must still exist (caller is responsible for deletion)
        assert!(log_path(dir.path(), 1).exists());
    }

    #[test]
    fn remove_old_files_missing_files_is_ok() {
        let dir = tmp();
        // file_ids 5 and 6 don't exist — should not error
        remove_old_files(dir.path(), &[5, 6]).unwrap();
    }

    #[test]
    fn compact_compacted_output_roundtrips_through_hint() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"x", b"y")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"x".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        // Reload keydir from the hint file produced by compaction
        let mut new_kd = KeyDir::new();
        let mut reader = crate::hint::HintReader::open(&out.new_hint_path).unwrap();
        reader.load_into(&mut new_kd).unwrap();

        let entry = new_kd.get(b"x").unwrap();
        assert_eq!(entry.file_id, 99);

        // Read the value from the new log file
        let mut log_reader = crate::log::LogReader::open(&out.new_log_path).unwrap();
        let record = log_reader.read_at(entry.offset).unwrap().unwrap();
        assert_eq!(record.value, Some(b"y".to_vec()));
    }

    #[test]
    fn compact_keeps_entries_with_future_expiry() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"k", b"v", u64::MAX).unwrap(); // far future
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset,
                value_size: 1,
                expire_at_ms: u64::MAX,
                ref_bit: false,
            },
        );

        // now_ms=0, far below u64::MAX — entry must survive
        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 1);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn compact_with_100_live_entries_writes_all() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut offsets = Vec::with_capacity(100);
        for i in 0u8..100 {
            offsets.push(w.write(&[i], &[i], 0).unwrap());
        }
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        for (i, &offset) in offsets.iter().enumerate() {
            kd.put(
                vec![i as u8],
                KeyEntry {
                    file_id: 1,
                    offset,
                    value_size: 1,
                    expire_at_ms: 0,
                    ref_bit: false,
                },
            );
        }

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 100);

        // Verify hint file loads back to 100 entries
        let mut new_kd = KeyDir::new();
        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        hr.load_into(&mut new_kd).unwrap();
        assert_eq!(new_kd.len(), 100);
    }

    #[test]
    fn compact_preserves_expire_at_ms_in_output_hint() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"ttl_key", b"val", 99_999).unwrap();
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        kd.put(
            b"ttl_key".to_vec(),
            KeyEntry {
                file_id: 1,
                offset,
                value_size: 3,
                expire_at_ms: 99_999,
                ref_bit: false,
            },
        );

        // now_ms=0 so the entry is not expired
        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 1);

        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        let hints = hr.read_all().unwrap();
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].expire_at_ms, 99_999);
    }

    #[test]
    fn compact_drops_stale_entry_when_keydir_points_to_newer_offset() {
        let dir = tmp();
        // Write key "k" twice — two different offsets in the same file
        use crate::log::HEADER_SIZE;
        let off_v1 = 0u64;
        let off_v2 = (HEADER_SIZE + 1 + 2) as u64; // after "k"/"v1" record (key=1, value=2)

        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v1", 0).unwrap();
        w.write(b"k", b"v2", 0).unwrap();
        w.sync().unwrap();

        // Keydir points to the second (newer) write
        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_v2,
                value_size: 2,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = off_v1;

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 1, "only the live (newer) record survives");
        assert_eq!(out.records_dropped, 1, "stale first record is dropped");
    }
}
