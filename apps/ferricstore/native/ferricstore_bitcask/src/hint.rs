#![allow(dead_code)]

//! Hint files accelerate startup by storing the keydir index without values.
//!
//! A hint file mirrors the corresponding data file but omits value bytes.
//! Each entry is prefixed with a CRC32 checksum that covers all remaining
//! fields, preventing a corrupt hint file from silently inserting bad disk
//! pointers into the keydir.
//!
//! ```text
//! [ crc32: u32 | file_id: u64 | offset: u64 | value_size: u32 | expire_at_ms: u64 | key_size: u16 | key: [u8] ]
//! ```
//!
//! The CRC covers: `file_id || offset || value_size || expire_at_ms || key_size || key`
//! (everything after the `crc32` field).
//!
//! Reading hint files rebuilds the full keydir in milliseconds on startup —
//! no need to scan the entire value log.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::keydir::{KeyDir, KeyEntry};

/// CRC32 field size prepended to every hint entry.
const CRC_SIZE: usize = 4;

/// Fixed-size body header per hint record (after the CRC, before the key bytes).
/// `file_id`(8) + `offset`(8) + `value_size`(4) + `expire_at_ms`(8) + `key_size`(2) = 30
const HINT_BODY_HEADER_SIZE: usize = 30;

/// Total header bytes per hint record (before the key bytes):
/// `crc32`(4) + `file_id`(8) + `offset`(8) + `value_size`(4) + `expire_at_ms`(8) + `key_size`(2) = 34
pub const HINT_HEADER_SIZE: usize = CRC_SIZE + HINT_BODY_HEADER_SIZE;

#[derive(Debug)]
pub struct HintError(pub String);

impl std::fmt::Display for HintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HintError: {}", self.0)
    }
}

impl From<io::Error> for HintError {
    fn from(e: io::Error) -> Self {
        HintError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HintError>;

/// A single entry read from a hint file.
#[derive(Debug, PartialEq, Eq)]
pub struct HintEntry {
    pub file_id: u64,
    pub offset: u64,
    pub value_size: u32,
    pub expire_at_ms: u64,
    pub key: Vec<u8>,
}

/// Writes a hint file alongside a data file using an atomic write-then-rename
/// strategy.
///
/// All content is written to a `.hint.tmp` temporary file. Only when
/// [`HintWriter::commit`] is called are the contents flushed, synced to disk,
/// and the temporary file atomically renamed to the final `.hint` path.
///
/// If `commit` is never called (e.g. the process panics or an error occurs),
/// the [`Drop`] implementation removes the `.hint.tmp` file so no partial
/// content is left on disk.
pub struct HintWriter {
    writer: BufWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
}

impl HintWriter {
    /// Open a new hint writer targeting `path`.
    ///
    /// Content is written to `<path>.tmp` and only moved to `path` on
    /// [`commit`](HintWriter::commit).
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if the temporary file cannot be created or opened.
    pub fn open(path: &Path) -> Result<Self> {
        let tmp_path = path.with_extension("hint.tmp");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            tmp_path,
            final_path: path.to_path_buf(),
        })
    }

    /// # Errors
    ///
    /// Returns a `HintError` if the entry cannot be written to disk.
    pub fn write_entry(&mut self, entry: &HintEntry) -> Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        let key_size = entry.key.len() as u16;

        // Build the body first so we can compute the CRC over it.
        let mut body = Vec::with_capacity(HINT_BODY_HEADER_SIZE + entry.key.len());
        body.extend_from_slice(&entry.file_id.to_le_bytes());
        body.extend_from_slice(&entry.offset.to_le_bytes());
        body.extend_from_slice(&entry.value_size.to_le_bytes());
        body.extend_from_slice(&entry.expire_at_ms.to_le_bytes());
        body.extend_from_slice(&key_size.to_le_bytes());
        body.extend_from_slice(&entry.key);

        let crc = crc32(&body);
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&body)?;
        Ok(())
    }

    /// Flush all buffered data, sync to disk, and atomically rename the
    /// temporary file to the final hint path.
    ///
    /// After a successful `commit` the `.hint.tmp` file no longer exists (it
    /// has been renamed to `.hint`). The subsequent `drop` will attempt to
    /// remove the `.hint.tmp` path and fail silently, which is correct.
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if flushing, syncing, or renaming fails. On
    /// error the temporary file is left in place; the [`Drop`] impl will
    /// remove it when the writer is dropped.
    pub fn commit(mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        std::fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(())
    }
}

impl Drop for HintWriter {
    /// Best-effort cleanup of the temporary file if [`commit`](HintWriter::commit)
    /// was never called (crash / error path). The `remove_file` call is allowed
    /// to fail silently: after a successful `commit` the file has already been
    /// renamed away, so `remove_file` will return `NotFound`, which is harmless.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp_path);
    }
}

/// Reads hint entries and reconstructs the keydir.
pub struct HintReader {
    file: File,
}

impl HintReader {
    /// # Errors
    ///
    /// Returns a `HintError` if the file cannot be opened.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Load all entries from the hint file into `keydir`.
    /// Tombstones (`value_size` == 0) are skipped -- they represent deleted keys.
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if the hint file cannot be read or is malformed.
    pub fn load_into(&mut self, keydir: &mut KeyDir) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        while let Some(entry) = read_hint_entry(&mut self.file)? {
            if entry.value_size == 0 {
                keydir.delete(&entry.key);
            } else {
                keydir.put(
                    entry.key.clone(),
                    KeyEntry {
                        file_id: entry.file_id,
                        offset: entry.offset,
                        value_size: entry.value_size,
                        expire_at_ms: entry.expire_at_ms,
                        ref_bit: false,
                    },
                );
            }
        }
        Ok(())
    }

    /// Collect all hint entries (used in tests and compaction).
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if the hint file cannot be read or is malformed.
    pub fn read_all(&mut self) -> Result<Vec<HintEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();
        while let Some(entry) = read_hint_entry(&mut self.file)? {
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn read_hint_entry(reader: &mut impl Read) -> Result<Option<HintEntry>> {
    // Read the CRC32 (4 bytes). A clean EOF here means the file ended normally.
    let mut crc_buf = [0u8; CRC_SIZE];
    match reader.read_exact(&mut crc_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let stored_crc = u32::from_le_bytes(crc_buf);

    // Read the fixed-size body header (30 bytes).
    let mut header = [0u8; HINT_BODY_HEADER_SIZE];
    reader.read_exact(&mut header).map_err(HintError::from)?;

    let file_id = u64::from_le_bytes(
        header[0..8]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let offset = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let value_size = u32::from_le_bytes(
        header[16..20]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let expire_at_ms = u64::from_le_bytes(
        header[20..28]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let key_size = u16::from_le_bytes(
        header[28..30]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    ) as usize;

    let mut key = vec![0u8; key_size];
    reader.read_exact(&mut key).map_err(HintError::from)?;

    // Validate CRC over body (header + key).
    let mut body = Vec::with_capacity(HINT_BODY_HEADER_SIZE + key.len());
    body.extend_from_slice(&header);
    body.extend_from_slice(&key);
    let computed_crc = crc32(&body);

    if computed_crc != stored_crc {
        return Err(HintError(format!(
            "hint entry CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
        )));
    }

    Ok(Some(HintEntry {
        file_id,
        offset,
        value_size,
        expire_at_ms,
        key,
    }))
}

/// CRC-32/ISO-HDLC (same polynomial used by `log.rs`).
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn sample_entry(key: &[u8], file_id: u64, offset: u64) -> HintEntry {
        HintEntry {
            file_id,
            offset,
            value_size: 42,
            expire_at_ms: 0,
            key: key.to_vec(),
        }
    }

    #[test]
    fn write_and_read_single_entry() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let original = sample_entry(b"hello", 3, 128);

        let mut writer = HintWriter::open(&path).unwrap();
        writer.write_entry(&original).unwrap();
        writer.commit().unwrap();

        let mut reader = HintReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], original);
    }

    #[test]
    fn write_and_read_multiple_entries() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries_in = vec![
            sample_entry(b"alpha", 1, 0),
            sample_entry(b"beta", 1, 100),
            sample_entry(b"gamma", 2, 0),
        ];

        let mut writer = HintWriter::open(&path).unwrap();
        for e in &entries_in {
            writer.write_entry(e).unwrap();
        }
        writer.commit().unwrap();

        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();
        assert_eq!(entries_out, entries_in);
    }

    #[test]
    fn load_into_keydir_populates_entries() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let mut writer = HintWriter::open(&path).unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 5,
                offset: 256,
                value_size: 20,
                expire_at_ms: 99_000,
                key: b"mykey".to_vec(),
            })
            .unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        let entry = kd.get(b"mykey").unwrap();
        assert_eq!(entry.file_id, 5);
        assert_eq!(entry.offset, 256);
        assert_eq!(entry.value_size, 20);
        assert_eq!(entry.expire_at_ms, 99_000);
    }

    #[test]
    fn load_into_keydir_skips_tombstones() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let mut writer = HintWriter::open(&path).unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 1,
                offset: 0,
                value_size: 10,
                expire_at_ms: 0,
                key: b"live".to_vec(),
            })
            .unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 2,
                offset: 0,
                value_size: 0, // tombstone
                expire_at_ms: 0,
                key: b"dead".to_vec(),
            })
            .unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.get(b"live").is_some());
        assert!(kd.get(b"dead").is_none());
    }

    #[test]
    fn load_into_keydir_later_entry_overwrites_earlier() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let mut writer = HintWriter::open(&path).unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 1,
                offset: 0,
                value_size: 5,
                expire_at_ms: 0,
                key: b"k".to_vec(),
            })
            .unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 2,
                offset: 500,
                value_size: 8,
                expire_at_ms: 0,
                key: b"k".to_vec(),
            })
            .unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        let entry = kd.get(b"k").unwrap();
        assert_eq!(entry.file_id, 2);
        assert_eq!(entry.offset, 500);
    }

    #[test]
    fn empty_hint_file_loads_empty_keydir() {
        let dir = tmp();
        let path = dir.path().join("empty.hint");

        let writer = HintWriter::open(&path).unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.is_empty());
    }

    // ------------------------------------------------------------------
    // Atomicity tests (Issue 5.2)
    // ------------------------------------------------------------------

    /// Dropping a `HintWriter` without calling `commit` must not leave any
    /// file at the final `.hint` path, and must also clean up the `.hint.tmp`
    /// file (best-effort via `Drop`).
    #[test]
    fn hint_write_is_atomic_on_crash() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        // Open a writer and write some entries, but never call commit().
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&sample_entry(b"key1", 1, 0)).unwrap();
            writer.write_entry(&sample_entry(b"key2", 1, 64)).unwrap();
            // writer drops here without commit — simulates a crash
        }

        // The final .hint file must NOT exist (was never renamed into place).
        assert!(
            !path.exists(),
            "final .hint must not exist when commit was never called"
        );

        // The .hint.tmp file must also be gone (cleaned up by Drop).
        assert!(
            !tmp_path.exists(),
            ".hint.tmp must be cleaned up by Drop when commit was never called"
        );
    }

    /// Writing a second hint file over an existing one must atomically replace
    /// the content: only the new keys are visible after commit, and no `.tmp`
    /// file remains.
    #[test]
    fn hint_commit_replaces_existing_hint_atomically() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        // Write the first hint file with "old_key".
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&sample_entry(b"old_key", 1, 0)).unwrap();
            writer.commit().unwrap();
        }

        // Verify "old_key" is readable.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].key, b"old_key");
        }

        // Write a second hint file with "new_key" over the same path.
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"new_key", 2, 128))
                .unwrap();
            writer.commit().unwrap();
        }

        // Only "new_key" must be present.
        let mut reader = HintReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(
            entries.len(),
            1,
            "only new_key should be present after second commit"
        );
        assert_eq!(
            entries[0].key, b"new_key",
            "old_key must be replaced by new_key"
        );

        // No .tmp file should remain after a successful commit.
        assert!(
            !tmp_path.exists(),
            ".hint.tmp must not exist after a successful commit"
        );
    }

    // ------------------------------------------------------------------
    // CRC integrity tests (Issue 5.1)
    // ------------------------------------------------------------------

    /// A valid hint entry round-trips correctly; flipping a byte in the offset
    /// field must cause a CRC mismatch error on re-read.
    #[test]
    fn hint_entry_crc_validated_on_read() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let original = sample_entry(b"hello", 3, 128);
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&original).unwrap();
            writer.commit().unwrap();
        }

        // Clean read — must succeed.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0], original);
        }

        // Corrupt the offset field. The entry layout on disk is:
        //   [crc32: 4][file_id: 8][offset: 8][value_size: 4][expire_at_ms: 8][key_size: 2][key]
        // The offset field starts at byte 4 + 8 = 12.
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(12)).unwrap();
            f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        }

        // Re-read — must return a CRC error.
        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "corrupted offset field must trigger a CRC mismatch error"
        );
    }

    /// Corrupting the `file_id` field (not the key) must also be caught by the CRC.
    #[test]
    fn hint_entry_crc_covers_all_fields() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entry = HintEntry {
            file_id: 42,
            offset: 1024,
            value_size: 7,
            expire_at_ms: 999_000,
            key: b"integrity".to_vec(),
        };
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        // Verify clean read.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0], entry);
        }

        // Corrupt the file_id field. It starts at byte 4 (right after the 4-byte CRC).
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(4)).unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "corrupted file_id field must trigger a CRC mismatch error"
        );
    }

    /// Writing 10 entries and reading them all back must succeed and produce
    /// identical entries in the same order.
    #[test]
    fn hint_multiple_entries_round_trip() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries_in: Vec<HintEntry> = (0..10)
            .map(|i| HintEntry {
                file_id: i,
                offset: i * 64,
                value_size: (i as u32) + 1,
                expire_at_ms: i * 1_000,
                key: format!("key-{i}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries_in {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();

        assert_eq!(entries_out.len(), entries_in.len());
        for (a, b) in entries_in.iter().zip(entries_out.iter()) {
            assert_eq!(a, b);
        }
    }

    // ------------------------------------------------------------------
    // CRC integrity (additional)
    // ------------------------------------------------------------------

    /// Write a valid entry, flip a byte in the key area, re-read returns error.
    #[test]
    fn crc32_in_hint_entry_is_validated() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entry = sample_entry(b"validate_me", 7, 512);
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        // Compute where the key starts:
        // Layout: [crc32: 4][file_id: 8][offset: 8][value_size: 4][expire_at_ms: 8][key_size: 2][key]
        // Key starts at byte 4 + 8 + 8 + 4 + 8 + 2 = 34.
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(34)).unwrap();
            f.write_all(&[0xAB]).unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "flipped byte in key area must trigger CRC mismatch"
        );
    }

    /// Empty key round-trips through `write_entry` / `read_all`.
    #[test]
    fn hint_entry_empty_key_round_trips() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entry = HintEntry {
            file_id: 1,
            offset: 0,
            value_size: 5,
            expire_at_ms: 0,
            key: Vec::new(), // empty key
        };
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].key,
            Vec::<u8>::new(),
            "empty key must round-trip"
        );
        assert_eq!(entries[0].file_id, 1);
        assert_eq!(entries[0].value_size, 5);
    }

    /// Write 20 entries and verify all read back without CRC error.
    #[test]
    fn hint_multiple_entries_all_have_valid_crcs() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries_in: Vec<HintEntry> = (0u64..20)
            .map(|i| HintEntry {
                file_id: i % 3,
                offset: i * 128,
                value_size: (i as u32) * 10 + 1,
                expire_at_ms: if i % 2 == 0 { 0 } else { i * 1000 },
                key: format!("crc_check_key_{i}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries_in {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        // All 20 entries must be readable without CRC error.
        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();
        assert_eq!(entries_out.len(), 20, "all 20 entries must be present");
        for (a, b) in entries_in.iter().zip(entries_out.iter()) {
            assert_eq!(a, b, "entry must survive round-trip with correct CRC");
        }
    }

    // ------------------------------------------------------------------
    // commit() atomicity (additional)
    // ------------------------------------------------------------------

    /// After `commit()`, no `.hint.tmp` file exists and `.hint` file does.
    #[test]
    fn commit_renames_tmp_to_final() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"committed", 1, 0))
                .unwrap();
            writer.commit().unwrap();
        }

        assert!(path.exists(), "final .hint file must exist after commit");
        assert!(
            !tmp_path.exists(),
            ".hint.tmp file must not exist after commit"
        );
    }

    /// Dropping the writer without calling commit must remove the .tmp file
    /// and not create the final .hint file.
    #[test]
    fn drop_without_commit_cleans_up_tmp() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"uncommitted", 1, 0))
                .unwrap();
            // Drop without commit.
        }

        assert!(
            !path.exists(),
            "final .hint must not exist when commit was never called"
        );
        assert!(!tmp_path.exists(), ".hint.tmp must be cleaned up by Drop");
    }

    // ------------------------------------------------------------------
    // load_into: detailed edge cases
    // ------------------------------------------------------------------

    /// Loading an empty hint file results in an empty keydir.
    #[test]
    fn load_into_empty_file_returns_empty_keydir() {
        let dir = tmp();
        let path = dir.path().join("empty2.hint");

        let writer = HintWriter::open(&path).unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.is_empty(), "empty hint file must produce empty keydir");
        assert_eq!(kd.len(), 0);
    }

    /// Writing 15 entries and loading them populates the keydir with 15 entries.
    #[test]
    fn load_into_all_entries_added_to_keydir() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries: Vec<HintEntry> = (0u64..15)
            .map(|i| HintEntry {
                file_id: 1,
                offset: i * 50,
                value_size: 10,
                expire_at_ms: 0,
                key: format!("load_key_{i}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert_eq!(kd.len(), 15, "all 15 entries must be loaded into keydir");
        for e in &entries {
            assert!(kd.get(&e.key).is_some(), "keydir must contain {:?}", e.key);
        }
    }

    /// A hint file with a corrupt CRC in the first entry causes `load_into` to return Err.
    #[test]
    fn load_into_corrupt_crc_returns_error() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"crc_load_test", 1, 0))
                .unwrap();
            writer.commit().unwrap();
        }

        // Flip a byte in the key area (byte 34) to invalidate the CRC.
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(34)).unwrap();
            f.write_all(&[0xCC]).unwrap();
        }

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.load_into(&mut kd).is_err(),
            "load_into on corrupt hint must return Err"
        );
    }
}
