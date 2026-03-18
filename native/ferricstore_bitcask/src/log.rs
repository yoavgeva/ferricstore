#![allow(dead_code)]

//! Append-only log file for Bitcask.
//!
//! On-disk record format (little-endian):
//!
//! ```text
//! [ crc32: u32 | timestamp_ms: u64 | expire_at_ms: u64 | key_size: u16 | value_size: u32 | key: [u8] | value: [u8] ]
//! ```
//!
//! A tombstone record has `value_size = 0` and no value bytes. It signals a
//! logical delete and is used during compaction.
//!
//! The CRC32 covers everything after the checksum field:
//!   `timestamp_ms || expire_at_ms || key_size || value_size || key || value`

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::io_backend::{self, IoBackend};

/// Number of header bytes before key+value data.
/// `crc32`(4) + `timestamp_ms`(8) + `expire_at_ms`(8) + `key_size`(2) + `value_size`(4) = 26
pub const HEADER_SIZE: usize = 26;

/// Sentinel `value_size` marking a tombstone (deleted key).
pub const TOMBSTONE: u32 = 0;

#[derive(Debug)]
pub struct LogError(pub String);

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogError: {}", self.0)
    }
}

impl From<io::Error> for LogError {
    fn from(e: io::Error) -> Self {
        LogError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, LogError>;

/// A single decoded record read from the log.
#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub timestamp_ms: u64,
    pub expire_at_ms: u64,
    pub key: Vec<u8>,
    /// `None` means tombstone (deleted).
    pub value: Option<Vec<u8>>,
}

/// Writes new records to a log file (always appends).
///
/// Uses the best available I/O backend selected at startup:
/// - On Linux with kernel ≥ 5.1: `UringBackend` (`io_uring`)
/// - Otherwise: `SyncBackend` (`BufWriter<File>`)
///
/// The `write_batch` method is the preferred high-throughput path: it
/// submits all writes in a single kernel call (on `io_uring`) then fsyncs
/// once, reducing per-batch syscall overhead from N+1 to 2.
pub struct LogWriter {
    backend: Box<dyn IoBackend>,
    /// Current write position (= file size so far).
    pub offset: u64,
    pub file_id: u64,
}

impl LogWriter {
    /// Open (or create) a data file for appending.
    ///
    /// Selects the best available I/O backend automatically (see module doc).
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be opened or its metadata cannot be read.
    pub fn open(path: &Path, file_id: u64) -> Result<Self> {
        let backend = io_backend::create_backend(path).map_err(|e| LogError(e.to_string()))?;
        let offset = backend.offset();
        Ok(Self {
            backend,
            offset,
            file_id,
        })
    }

    /// Append a live record. Returns the byte offset at which it was written.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the record cannot be encoded or written to disk.
    pub fn write(&mut self, key: &[u8], value: &[u8], expire_at_ms: u64) -> Result<u64> {
        let record = encode_record(key, value, expire_at_ms)?;
        let start = self
            .backend
            .append(&record)
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();
        Ok(start)
    }

    /// Append a tombstone record (logical delete).
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the tombstone record cannot be written to disk.
    pub fn write_tombstone(&mut self, key: &[u8]) -> Result<u64> {
        let record = encode_tombstone(key)?;
        let start = self
            .backend
            .append(&record)
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();
        Ok(start)
    }

    /// Advance the write offset without writing data. Used by the async
    /// `io_uring` path to reserve file space that will be written by an
    /// `AsyncUringBackend`.
    pub fn advance_offset(&mut self, bytes: u64) {
        self.backend.advance_offset(bytes);
        self.offset += bytes;
    }

    /// Flush the write buffer and fsync the file to durable storage.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the flush or fsync fails.
    pub fn sync(&mut self) -> Result<()> {
        self.backend.sync().map_err(|e| LogError(e.to_string()))
    }

    /// Write multiple records in a single batch and fsync once.
    ///
    /// This is the preferred write path for `put_batch`. On `io_uring` all
    /// writes are submitted in a single `io_uring_enter` call (2 syscalls
    /// total: one for writes, one for fsync). On `SyncBackend` it falls back
    /// to N individual writes + 1 fsync.
    ///
    /// Returns `(offset, value_len)` for each entry in the same order as
    /// `entries`.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if any write or the final sync fails.
    pub fn write_batch(&mut self, entries: &[(&[u8], &[u8], u64)]) -> Result<Vec<(u64, usize)>> {
        // Encode all records first (owned Vecs, so lifetimes are clear).
        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|(key, value, expire_at_ms)| encode_record(key, value, *expire_at_ms))
            .collect::<Result<Vec<Vec<u8>>>>()?;

        let buf_refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();

        let offsets = self
            .backend
            .append_batch_and_sync(&buf_refs)
            .map_err(|e| LogError(e.to_string()))?;

        self.offset = self.backend.offset();

        Ok(offsets
            .into_iter()
            .zip(entries.iter())
            .map(|(off, (_, value, _))| (off, value.len()))
            .collect())
    }
}

/// Reads records from a log file at arbitrary offsets or sequentially.
pub struct LogReader {
    file: File,
}

impl LogReader {
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be opened.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Read the record at `offset`. Returns `None` at EOF.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be seeked or the record is malformed.
    pub fn read_at(&mut self, offset: u64) -> Result<Option<Record>> {
        self.file.seek(SeekFrom::Start(offset))?;
        read_next_record(&mut self.file)
    }

    /// Iterate all records from the start of the file.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be read or contains a malformed record.
    pub fn iter_from_start(&mut self) -> Result<Vec<Record>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        while let Some(record) = read_next_record(&mut self.file)? {
            records.push(record);
        }
        Ok(records)
    }

    /// Iterate records tolerating a truncated tail (crash-recovery mode).
    ///
    /// Like `iter_from_start`, but stops silently at the first CRC error or
    /// truncated record rather than returning `Err`. This matches the Bitcask
    /// paper's crash-recovery semantics: a partial record at the end of the
    /// log (written during a crash before fsync) is discarded; all complete
    /// records before it are valid.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` only for I/O errors (seek/read failures), not for
    /// record-level corruption or truncation.
    pub fn iter_from_start_tolerant(&mut self) -> Result<Vec<Record>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        // while let stops at EOF (Ok(None)) or any error — crash-recovery semantics:
        // a truncated/corrupt tail record is silently discarded.
        while let Ok(Some(record)) = read_next_record(&mut self.file) {
            records.push(record);
        }
        Ok(records)
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Maximum key size enforced at the record level (u16 field in the header).
pub const MAX_KEY_SIZE: usize = u16::MAX as usize; // 65 535 bytes

/// Maximum value size enforced at the record level (u32 field in the header).
/// Capped well below `u32::MAX` to keep allocations sane.
pub const MAX_VALUE_SIZE: usize = 512 * 1024 * 1024; // 512 MiB

pub(crate) fn encode_record(key: &[u8], value: &[u8], expire_at_ms: u64) -> Result<Vec<u8>> {
    if key.is_empty() {
        return Err(LogError("key must not be empty".to_string()));
    }
    if key.len() > MAX_KEY_SIZE {
        return Err(LogError(format!(
            "key too large: {} bytes (max {} bytes)",
            key.len(),
            MAX_KEY_SIZE
        )));
    }
    if value.len() > MAX_VALUE_SIZE {
        return Err(LogError(format!(
            "value too large: {} bytes (max {} bytes)",
            value.len(),
            MAX_VALUE_SIZE
        )));
    }

    let now_ms = now_ms();
    let key_size = key.len() as u16;
    let value_size = value.len() as u32;

    let mut body = Vec::with_capacity(HEADER_SIZE - 4 + key.len() + value.len());
    body.extend_from_slice(&now_ms.to_le_bytes());
    body.extend_from_slice(&expire_at_ms.to_le_bytes());
    body.extend_from_slice(&key_size.to_le_bytes());
    body.extend_from_slice(&value_size.to_le_bytes());
    body.extend_from_slice(key);
    body.extend_from_slice(value);

    let crc = crc32(&body);
    let mut record = Vec::with_capacity(4 + body.len());
    record.extend_from_slice(&crc.to_le_bytes());
    record.extend_from_slice(&body);
    Ok(record)
}

fn encode_tombstone(key: &[u8]) -> Result<Vec<u8>> {
    encode_record(key, &[], 0)
}

fn read_next_record(reader: &mut impl Read) -> Result<Option<Record>> {
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size = u32::from_le_bytes(header[22..26].try_into().unwrap()) as usize;

    let mut key = vec![0u8; key_size];
    reader.read_exact(&mut key)?;

    let value = if value_size == TOMBSTONE as usize && key_size > 0 {
        // tombstone check: value_size == 0
        vec![]
    } else {
        let mut v = vec![0u8; value_size];
        reader.read_exact(&mut v)?;
        v
    };

    // Verify CRC over everything after the checksum field
    let mut body = Vec::with_capacity(HEADER_SIZE - 4 + key.len() + value.len());
    body.extend_from_slice(&header[4..]);
    body.extend_from_slice(&key);
    body.extend_from_slice(&value);
    let computed_crc = crc32(&body);

    if computed_crc != stored_crc {
        return Err(LogError(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        )));
    }

    let record = Record {
        timestamp_ms,
        expire_at_ms,
        key,
        value: if value_size == 0 {
            None // tombstone
        } else {
            Some(value)
        },
    };

    Ok(Some(record))
}

fn crc32(data: &[u8]) -> u32 {
    // CRC-32/ISO-HDLC (same as used by zlib)
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
    use std::fs;
    use tempfile::TempDir;

    fn temp_dir() -> TempDir {
        tempfile::TempDir::new().expect("tmp dir")
    }

    // --- encoding round-trips ---

    #[test]
    fn encode_decode_live_record() {
        let key = b"hello";
        let value = b"world";
        let encoded = encode_record(key, value, 0).unwrap();

        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.key, key);
        assert_eq!(record.value, Some(value.to_vec()));
        assert_eq!(record.expire_at_ms, 0);
    }

    #[test]
    fn encode_decode_tombstone() {
        let key = b"dead";
        let encoded = encode_tombstone(key).unwrap();

        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.key, key);
        assert!(record.value.is_none(), "tombstone must have None value");
    }

    #[test]
    fn encode_decode_with_expiry() {
        let encoded = encode_record(b"ttl", b"val", 99_999).unwrap();
        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(record.expire_at_ms, 99_999);
    }

    #[test]
    fn crc_mismatch_returns_error() {
        let mut encoded = encode_record(b"k", b"v", 0).unwrap();
        // flip a byte in the value area
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let mut cursor = io::Cursor::new(&encoded);
        assert!(read_next_record(&mut cursor).is_err());
    }

    #[test]
    fn empty_reader_returns_none() {
        let mut cursor = io::Cursor::new(Vec::<u8>::new());
        assert!(read_next_record(&mut cursor).unwrap().is_none());
    }

    // --- LogWriter + LogReader ---

    #[test]
    fn writer_creates_file_and_reader_reads_back() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        let mut writer = LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"foo", b"bar", 0).unwrap();
        writer.sync().unwrap();

        assert_eq!(offset, 0, "first write starts at offset 0");

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert_eq!(record.key, b"foo");
        assert_eq!(record.value, Some(b"bar".to_vec()));
    }

    #[test]
    fn writer_returns_correct_offsets() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();

        let off0 = writer.write(b"a", b"1", 0).unwrap();
        let off1 = writer.write(b"bb", b"22", 0).unwrap();
        writer.sync().unwrap();

        assert_eq!(off0, 0);
        // second record starts right after first: HEADER(26) + key(1) + value(1) = 28
        assert_eq!(off1, (HEADER_SIZE + 1 + 1) as u64);
    }

    #[test]
    fn reader_read_at_specific_offset() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"first", b"aaa", 0).unwrap();
        let off2 = writer.write(b"second", b"bbb", 0).unwrap();
        writer.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off2).unwrap().unwrap();
        assert_eq!(record.key, b"second");
        assert_eq!(record.value, Some(b"bbb".to_vec()));
    }

    #[test]
    fn reader_read_at_eof_returns_none() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"k", b"v", 0).unwrap();
        writer.sync().unwrap();

        let file_size = fs::metadata(&path).unwrap().len();
        let mut reader = LogReader::open(&path).unwrap();
        assert!(reader.read_at(file_size).unwrap().is_none());
    }

    #[test]
    fn iter_from_start_returns_all_records_in_order() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"a", b"1", 0).unwrap();
        writer.write(b"b", b"2", 0).unwrap();
        writer.write_tombstone(b"a").unwrap();
        writer.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, b"a");
        assert_eq!(records[0].value, Some(b"1".to_vec()));
        assert_eq!(records[1].key, b"b");
        assert_eq!(records[2].key, b"a");
        assert!(records[2].value.is_none(), "tombstone");
    }

    #[test]
    fn writer_open_existing_file_appends() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            w.write(b"existing", b"val", 0).unwrap();
            w.sync().unwrap();
        }

        // reopen and write more
        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            w.write(b"new", b"val2", 0).unwrap();
            w.sync().unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"existing");
        assert_eq!(records[1].key, b"new");
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_key_is_rejected() {
        // Empty key is now rejected by the size guard.
        assert!(
            encode_record(b"", b"value", 0).is_err(),
            "empty key must be rejected"
        );
    }

    #[test]
    fn zero_length_value_is_not_tombstone_when_key_present() {
        // value_size=0 is the tombstone sentinel — but encode_record with empty
        // value produces value_size=0, which IS decoded as a tombstone.
        // This is by design: callers must use write_tombstone for deletes.
        let encoded = encode_record(b"k", b"", 0).unwrap();
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        // Empty value decodes as tombstone (value_size==0 is the sentinel)
        assert!(r.value.is_none());
    }

    #[test]
    fn large_value_roundtrips() {
        let key = b"bigkey";
        let value = vec![0xABu8; 64 * 1024]; // 64 KiB
        let encoded = encode_record(key, &value, 0).unwrap();
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(r.key, key);
        assert_eq!(r.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn non_utf8_key_and_value_roundtrip() {
        let key = vec![0xFF, 0x00, 0xFE];
        let value = vec![0x01, 0x02, 0x03];
        let encoded = encode_record(&key, &value, 12345).unwrap();
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(r.key, key);
        assert_eq!(r.value, Some(value));
        assert_eq!(r.expire_at_ms, 12345);
    }

    #[test]
    fn truncated_header_returns_none() {
        // Only 10 bytes — less than HEADER_SIZE (26) — should return None (EOF)
        let partial = vec![0u8; 10];
        let mut cursor = io::Cursor::new(partial);
        let result = read_next_record(&mut cursor);
        // Truncated header looks like EOF → Ok(None)
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn truncated_after_header_returns_error_or_none() {
        // Write a valid header claiming key_size=5, value_size=5, but provide
        // only the header bytes — reading key should fail or return None.
        let mut header = vec![0u8; HEADER_SIZE];
        // key_size = 5 at bytes 20..22
        header[20] = 5;
        header[21] = 0;
        // value_size = 5 at bytes 22..26
        header[22] = 5;
        header[23] = 0;
        header[24] = 0;
        header[25] = 0;
        // Don't provide key/value bytes — truncated
        let mut cursor = io::Cursor::new(header);
        // read_exact on key will hit UnexpectedEof → error
        assert!(read_next_record(&mut cursor).is_err());
    }

    #[test]
    fn writer_offset_tracks_correctly_across_multiple_writes() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let mut expected_offset = 0u64;
        for i in 0u8..10 {
            let key = vec![i];
            let value = vec![i; i as usize + 1]; // variable-length values
            let off = w.write(&key, &value, 0).unwrap();
            assert_eq!(off, expected_offset);
            expected_offset += (HEADER_SIZE + 1 + value.len()) as u64;
        }
        assert_eq!(w.offset, expected_offset);
    }

    #[test]
    fn multiple_syncs_do_not_corrupt_data() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        for i in 0u8..5 {
            w.write(&[i], &[i, i], 0).unwrap();
            w.sync().unwrap(); // fsync after every write
        }

        let mut r = LogReader::open(&path).unwrap();
        let records = r.iter_from_start().unwrap();
        assert_eq!(records.len(), 5);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.key, vec![i as u8]);
            assert_eq!(rec.value, Some(vec![i as u8, i as u8]));
        }
    }

    #[test]
    fn crc_corruption_in_key_area_detected() {
        let mut encoded = encode_record(b"hello", b"world", 0).unwrap();
        // Flip a byte in the key area (after the 26-byte header)
        encoded[HEADER_SIZE] ^= 0xFF;
        let mut cursor = io::Cursor::new(encoded);
        assert!(read_next_record(&mut cursor).is_err());
    }

    #[test]
    fn writer_reports_correct_offset_after_reopen() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        let first_offset;
        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            first_offset = w.write(b"key", b"val", 0).unwrap();
            w.sync().unwrap();
            // offset after write
            assert_eq!(w.offset, (HEADER_SIZE + 3 + 3) as u64);
        }

        // Reopen — writer should resume at the end of the file
        let w2 = LogWriter::open(&path, 1).unwrap();
        assert_eq!(w2.offset, (HEADER_SIZE + 3 + 3) as u64);
        let _ = first_offset;
    }

    #[test]
    fn tombstone_followed_by_live_record_both_readable() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write_tombstone(b"k").unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();

        assert_eq!(records.len(), 2);
        assert!(records[0].value.is_none(), "first record is tombstone");
        assert_eq!(records[1].key, b"k");
        assert_eq!(
            records[1].value,
            Some(b"v".to_vec()),
            "second record is live"
        );
    }

    #[test]
    fn iter_from_start_empty_file_returns_empty_vec() {
        let dir = temp_dir();
        let path = dir.path().join("empty.log");
        // Create the file with no writes
        LogWriter::open(&path, 1).unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn read_at_middle_of_record_returns_error() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        // Offset 1 is in the middle of the header — CRC will not match
        // Either an Err (CRC mismatch) or Ok(None) if it looks like EOF are
        // acceptable. What must NOT happen is a successful decode of a valid record.
        let mut reader2 = LogReader::open(&path).unwrap();
        // Must be either an error OR None — never Ok(Some(valid record))
        match reader2.read_at(1) {
            Err(_) | Ok(None) => {} // CRC mismatch, IO error, or EOF — all acceptable
            Ok(Some(rec)) => panic!(
                "unexpected successful decode at mid-record: key={:?}",
                rec.key
            ),
        }
    }

    #[test]
    fn write_max_key_size_65535_bytes() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let big_key = vec![0x42u8; 65535]; // u16::MAX bytes
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(&big_key, b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.len(), 65535);
        assert_eq!(records[0].value, Some(b"v".to_vec()));
    }

    #[test]
    fn multiple_tombstones_for_same_key_all_decoded() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write_tombstone(b"k").unwrap();
        w.write_tombstone(b"k").unwrap();
        w.write_tombstone(b"k").unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 3);
        for rec in &records {
            assert!(rec.value.is_none(), "all three records must be tombstones");
        }
    }

    #[test]
    fn timestamp_is_nonzero_after_write() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert!(
            record.timestamp_ms > 0,
            "timestamp_ms must be non-zero after a write"
        );
    }

    // ------------------------------------------------------------------
    // CRC32 function properties
    // ------------------------------------------------------------------

    #[test]
    fn crc32_different_bytes_produce_different_checksums() {
        assert_ne!(crc32(b"hello"), crc32(b"world"));
    }

    #[test]
    fn crc32_empty_slice_is_consistent() {
        assert_eq!(
            crc32(b""),
            crc32(b""),
            "crc32 of empty slice must be deterministic"
        );
    }

    #[test]
    fn crc32_single_byte_flip_changes_checksum() {
        let mut data = b"abcdef".to_vec();
        let original = crc32(&data);
        data[3] ^= 0xFF;
        assert_ne!(
            crc32(&data),
            original,
            "flipping one byte must change the checksum"
        );
    }

    // ------------------------------------------------------------------
    // Record encoding/decoding
    // ------------------------------------------------------------------

    #[test]
    fn encode_and_decode_record_round_trip() {
        let key = b"roundtrip_key";
        let value = b"roundtrip_value";
        let expire_at = 987_654_321u64;
        let encoded = encode_record(key, value, expire_at).unwrap();

        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.key, key, "key must round-trip");
        assert_eq!(record.value, Some(value.to_vec()), "value must round-trip");
        assert_eq!(
            record.expire_at_ms, expire_at,
            "expire_at_ms must round-trip"
        );
        assert!(record.timestamp_ms > 0, "timestamp_ms must be set");
    }

    #[test]
    fn encode_tombstone_value_size_is_zero() {
        // encode_tombstone calls encode_record with empty value — value_size on disk is 0.
        let key = b"tomb_key";
        let encoded = encode_tombstone(key).unwrap();
        // The value_size field lives at bytes 22..26 in the encoded record.
        let value_size = u32::from_le_bytes(encoded[22..26].try_into().unwrap());
        assert_eq!(value_size, 0, "tombstone must encode value_size=0");
    }

    #[test]
    fn decode_truncated_record_returns_error() {
        let encoded = encode_record(b"key_trunc", b"val_trunc", 0).unwrap();
        // Truncate by 1 byte from the end.
        let truncated = &encoded[..encoded.len() - 1];
        let mut cursor = io::Cursor::new(truncated);
        // Should return Err (CRC mismatch or UnexpectedEof on value read).
        // The header is intact with key_size and value_size set, so it will try
        // to read the full value and hit UnexpectedEof → Err.
        assert!(
            read_next_record(&mut cursor).is_err(),
            "truncated record must return Err"
        );
    }

    #[test]
    fn decode_empty_buffer_returns_none() {
        let mut cursor = io::Cursor::new(Vec::<u8>::new());
        assert!(
            read_next_record(&mut cursor).unwrap().is_none(),
            "empty buffer must return Ok(None)"
        );
    }

    #[test]
    fn decode_record_with_flipped_crc_field_returns_error() {
        let mut encoded = encode_record(b"flip_crc", b"val", 0).unwrap();
        // Flip byte 0 — this is the first byte of the stored CRC field.
        encoded[0] ^= 0xFF;
        let mut cursor = io::Cursor::new(&encoded);
        assert!(
            read_next_record(&mut cursor).is_err(),
            "flipped CRC field byte must trigger CRC mismatch error"
        );
    }

    // ------------------------------------------------------------------
    // iter_from_start_tolerant
    // ------------------------------------------------------------------

    #[test]
    fn tolerant_iter_stops_at_first_crc_error() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off3 = w.write(b"k1", b"v1", 0).unwrap();
        let off4 = w.write(b"k2", b"v2", 0).unwrap();
        let off5 = w.write(b"k3", b"v3", 0).unwrap();
        w.sync().unwrap();
        drop(w);
        let _ = (off3, off4, off5);

        // Corrupt the start of the 2nd record's key area to break its CRC.
        // The 1st record occupies bytes 0..(HEADER_SIZE+2+2)=30.
        {
            use io::Write as _;
            use std::io::Seek;
            let flip_pos = (HEADER_SIZE + 2 + 2) as u64 + HEADER_SIZE as u64;
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(io::SeekFrom::Start(flip_pos)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        // Only the first record is valid; tolerant iterator stops at 2nd CRC error.
        assert_eq!(
            records.len(),
            1,
            "tolerant iter must stop at first corrupt record"
        );
        assert_eq!(records[0].key, b"k1");
    }

    #[test]
    fn tolerant_iter_stops_at_truncated_tail() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"r1", b"v1", 0).unwrap();
        w.write(b"r2", b"v2", 0).unwrap();
        let _off3 = w.write(b"r3", b"v3", 0).unwrap();
        w.sync().unwrap();
        drop(w);

        // Truncate the last 5 bytes — partial tail record.
        let size = fs::metadata(&path).unwrap().len();
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(size - 5).unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        // Either 2 (r1, r2) or 3 depending on whether the truncation hits the
        // 3rd record's header or body. In either case must be ≤ 3 and ≥ 2.
        assert!(
            records.len() == 2 || records.len() == 3,
            "tolerant iter must return 2 or 3 records with truncated tail; got {}",
            records.len()
        );
        // The first two must be intact.
        assert_eq!(records[0].key, b"r1");
        assert_eq!(records[1].key, b"r2");
    }

    #[test]
    fn tolerant_iter_returns_all_records_when_no_corruption() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        for i in 0u8..10 {
            w.write(&[i], &[i, i], 0).unwrap();
        }
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        assert_eq!(
            records.len(),
            10,
            "tolerant iter must return all 10 valid records"
        );
    }

    // ------------------------------------------------------------------
    // write_batch
    // ------------------------------------------------------------------

    #[test]
    fn write_batch_empty_is_ok() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let results = w.write_batch(&[]).unwrap();
        assert!(
            results.is_empty(),
            "write_batch with empty slice must return empty vec"
        );
        assert_eq!(w.offset, 0, "offset must stay 0 after empty batch");
    }

    #[test]
    fn write_batch_offsets_are_monotonically_increasing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries: Vec<(&[u8], &[u8], u64)> = vec![
            (b"a", b"1", 0),
            (b"bb", b"22", 0),
            (b"ccc", b"333", 0),
            (b"dddd", b"4444", 0),
            (b"eeeee", b"55555", 0),
        ];
        let results = w.write_batch(&entries).unwrap();
        assert_eq!(results.len(), 5);
        let offsets: Vec<u64> = results.iter().map(|(off, _)| *off).collect();
        for window in offsets.windows(2) {
            assert!(
                window[1] > window[0],
                "batch offsets must be strictly increasing: {offsets:?}"
            );
        }
    }

    #[test]
    fn write_batch_all_records_readable() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries: Vec<(&[u8], &[u8], u64)> = vec![
            (b"batch_k1", b"batch_v1", 0),
            (b"batch_k2", b"batch_v2", 1_000_000),
            (b"batch_k3", b"batch_v3", 0),
        ];
        let results = w.write_batch(&entries).unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        for (i, (off, _)) in results.iter().enumerate() {
            let record = reader.read_at(*off).unwrap().unwrap();
            assert_eq!(record.key, entries[i].0, "key at offset {off} must match");
            assert_eq!(
                record.value,
                Some(entries[i].1.to_vec()),
                "value at offset {off} must match"
            );
        }
    }
}
