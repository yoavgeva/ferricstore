#![allow(dead_code)]

//! Hint files accelerate startup by storing the keydir index without values.
//!
//! A hint file mirrors the corresponding data file but omits value bytes:
//!
//! ```text
//! [ file_id: u64 | offset: u64 | value_size: u32 | expire_at_ms: u64 | key_size: u16 | key: [u8] ]
//! ```
//!
//! Reading hint files rebuilds the full keydir in milliseconds on startup —
//! no need to scan the entire value log.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::keydir::{KeyDir, KeyEntry};

/// Header bytes per hint record (before the key bytes).
/// `file_id`(8) + `offset`(8) + `value_size`(4) + `expire_at_ms`(8) + `key_size`(2) = 30
pub const HINT_HEADER_SIZE: usize = 30;

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

/// Writes a hint file alongside a data file.
pub struct HintWriter {
    writer: BufWriter<File>,
}

impl HintWriter {
    /// # Errors
    ///
    /// Returns a `HintError` if the file cannot be created or opened.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// # Errors
    ///
    /// Returns a `HintError` if the entry cannot be written to disk.
    pub fn write_entry(&mut self, entry: &HintEntry) -> Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        let key_size = entry.key.len() as u16;

        self.writer.write_all(&entry.file_id.to_le_bytes())?;
        self.writer.write_all(&entry.offset.to_le_bytes())?;
        self.writer.write_all(&entry.value_size.to_le_bytes())?;
        self.writer.write_all(&entry.expire_at_ms.to_le_bytes())?;
        self.writer.write_all(&key_size.to_le_bytes())?;
        self.writer.write_all(&entry.key)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns a `HintError` if the underlying writer cannot be flushed.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
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
    let mut header = [0u8; HINT_HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let file_id = u64::from_le_bytes(header[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let value_size = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let expire_at_ms = u64::from_le_bytes(header[20..28].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[28..30].try_into().unwrap()) as usize;

    let mut key = vec![0u8; key_size];
    reader.read_exact(&mut key)?;

    Ok(Some(HintEntry {
        file_id,
        offset,
        value_size,
        expire_at_ms,
        key,
    }))
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
        writer.flush().unwrap();

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
        writer.flush().unwrap();

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
        writer.flush().unwrap();

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
        writer.flush().unwrap();

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
        writer.flush().unwrap();

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

        let mut writer = HintWriter::open(&path).unwrap();
        writer.flush().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.is_empty());
    }
}
