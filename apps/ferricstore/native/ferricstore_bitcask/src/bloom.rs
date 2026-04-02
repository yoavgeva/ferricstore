//! Bloom filter exposed as Rustler NIFs.
//!
//! Each Bloom filter is stored as a file on disk. The file layout is:
//!
//! ```text
//! [header: 32 bytes][bit array: ceil(num_bits / 8) bytes]
//! ```
//!
//! Header (32 bytes, little-endian):
//!   - bytes  0..7:  magic number (0x424C4F4F4D465F31 = "BLOOMF_1")
//!   - bytes  8..15: num_bits (u64)
//!   - bytes 16..19: num_hashes (u32)
//!   - bytes 20..23: reserved (u32, zero)
//!   - bytes 24..31: count (u64) — number of elements inserted
//!
//! ## Hash functions
//!
//! Uses the Kirsch-Mitzenmacker (2006) enhanced double-hashing technique:
//!   `h_i(x) = (h1(x) + i * h2(x)) mod m`
//!
//! where h1 and h2 are derived from xxh3 with two different seeds.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustler::schedule::consume_timeslice;
use rustler::{Binary, Encoder, Env, NifResult, Term};

/// How often (in items) to call `consume_timeslice` and let the BEAM
/// decide whether we should yield. 64 matches the interval used in lib.rs.
const YIELD_CHECK_INTERVAL: usize = 64;

const MAGIC: u64 = 0x424C_4F4F_4D46_5F31; // "BLOOMF_1"
const HEADER_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// NIF atoms
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        error,
        enoent,
    }
}

// ---------------------------------------------------------------------------
// Stateless file-based bloom filter NIFs (pread/pwrite, no mmap, no ResourceArc)
// ---------------------------------------------------------------------------

/// Compute hash positions for an element using Kirsch-Mitzenmacker double hashing
/// with xxh3. Standalone version of `BloomFilter::hash_positions` for stateless NIFs.
fn file_hash_positions(element: &[u8], num_bits: u64, num_hashes: u32) -> Vec<u64> {
    let h1 = xxhash_rust::xxh3::xxh3_64_with_seed(element, 0);
    let h2 = xxhash_rust::xxh3::xxh3_64_with_seed(element, 0x9E37_79B9_7F4A_7C15);
    (0..num_hashes as u64)
        .map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % num_bits)
        .collect()
}

/// Read the bloom file header via pread. Returns `(num_bits, num_hashes, count)`.
fn file_read_header(file: &File) -> Result<(u64, u32, u64), String> {
    let mut header = [0u8; HEADER_SIZE];
    file.read_at(&mut header, 0)
        .map_err(|e| format!("pread header: {e}"))?;

    let magic = u64::from_le_bytes(header[0..8].try_into().unwrap());
    if magic != MAGIC {
        return Err("invalid bloom file magic".into());
    }

    let num_bits = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let num_hashes = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let count = u64::from_le_bytes(header[24..32].try_into().unwrap());

    Ok((num_bits, num_hashes, count))
}

/// Map an IO error to either `:enoent` atom or a string reason.
fn map_io_error(e: &std::io::Error) -> FileError {
    if e.kind() == std::io::ErrorKind::NotFound {
        FileError::Enoent
    } else {
        FileError::Other(e.to_string())
    }
}

enum FileError {
    Enoent,
    Other(String),
}

fn encode_file_error(env: Env, fe: FileError) -> Term {
    match fe {
        FileError::Enoent => (atoms::error(), atoms::enoent()).encode(env),
        FileError::Other(s) => (atoms::error(), s).encode(env),
    }
}

/// Create a new bloom filter file at the given path.
/// Returns `{:ok, :ok}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_create(env: Env, path: String, num_bits: u64, num_hashes: u32) -> NifResult<Term> {
    if num_bits == 0 {
        return Ok((atoms::error(), "num_bits must be > 0").encode(env));
    }
    if num_hashes == 0 {
        return Ok((atoms::error(), "num_hashes must be > 0").encode(env));
    }

    let p = Path::new(&path);

    // Ensure parent directory exists.
    if let Some(parent) = p.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            return Ok((atoms::error(), format!("mkdir: {e}")).encode(env));
        }
    }

    let byte_count = num_bits.div_ceil(8) as usize;

    // Write the file with header + zeroed bit array.
    let mut file = match File::create(p) {
        Ok(f) => f,
        Err(e) => return Ok((atoms::error(), format!("create: {e}")).encode(env)),
    };

    let mut header = [0u8; HEADER_SIZE];
    header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&num_bits.to_le_bytes());
    header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
    // bytes 20..24 reserved (zero)
    // bytes 24..32 count = 0

    if let Err(e) = file.write_all(&header) {
        return Ok((atoms::error(), format!("write header: {e}")).encode(env));
    }

    let zeros = vec![0u8; byte_count];
    if let Err(e) = file.write_all(&zeros) {
        return Ok((atoms::error(), format!("write bits: {e}")).encode(env));
    }

    if let Err(e) = file.sync_all() {
        return Ok((atoms::error(), format!("fsync: {e}")).encode(env));
    }

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Add an element to a bloom filter file via pread/pwrite.
/// Returns `{:ok, 1}` if any bit was newly set, `{:ok, 0}` if all bits were already set.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_add<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
    let mut any_new = false;

    for pos in positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;

        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset)
            .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

        let mask = 1u8 << bit_offset;
        if (buf[0] & mask) == 0 {
            buf[0] |= mask;
            file.write_at(&buf, file_offset)
                .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite bit: {e}"))))?;
            any_new = true;
        }
    }

    if any_new {
        let new_count = count + 1;
        file.write_at(&new_count.to_le_bytes(), 24)
            .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite count: {e}"))))?;
    }

    Ok((atoms::ok(), u32::from(any_new)).encode(env))
}

/// Add multiple elements to a bloom filter file via pread/pwrite.
/// Returns `{:ok, [0|1, ...]}` with one result per element.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_madd<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, mut count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut results: Vec<u32> = Vec::with_capacity(elements.len());

    for (i, element) in elements.iter().enumerate() {
        let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
        let mut any_new = false;

        for pos in positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;

            let mut buf = [0u8; 1];
            file.read_at(&mut buf, file_offset)
                .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

            let mask = 1u8 << bit_offset;
            if (buf[0] & mask) == 0 {
                buf[0] |= mask;
                file.write_at(&buf, file_offset)
                    .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite bit: {e}"))))?;
                any_new = true;
            }
        }

        if any_new {
            count += 1;
        }
        results.push(u32::from(any_new));

        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    // Write final count once after all additions.
    file.write_at(&count.to_le_bytes(), 24)
        .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite count: {e}"))))?;

    Ok((atoms::ok(), results).encode(env))
}

/// Check if an element may exist in a bloom filter file via pread.
/// Returns `{:ok, 1}` if possibly present, `{:ok, 0}` if definitely not.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_exists<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, _count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);

    for pos in positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;

        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset)
            .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

        if (buf[0] & (1u8 << bit_offset)) == 0 {
            return Ok((atoms::ok(), 0u32).encode(env));
        }
    }

    Ok((atoms::ok(), 1u32).encode(env))
}

/// Check if multiple elements may exist in a bloom filter file via pread.
/// Returns `{:ok, [0|1, ...]}` with one result per element.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_mexists<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, _count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut results: Vec<u32> = Vec::with_capacity(elements.len());

    for (i, element) in elements.iter().enumerate() {
        let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
        let mut found = true;

        for pos in positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;

            let mut buf = [0u8; 1];
            file.read_at(&mut buf, file_offset)
                .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

            if (buf[0] & (1u8 << bit_offset)) == 0 {
                found = false;
                break;
            }
        }

        results.push(u32::from(found));

        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    Ok((atoms::ok(), results).encode(env))
}

/// Return the insertion count from a bloom filter file header.
/// Returns `{:ok, count}` or `{:error, :enoent}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_card(env: Env, path: String) -> NifResult<Term> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (_num_bits, _num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    Ok((atoms::ok(), count).encode(env))
}

/// Return bloom filter info from a file header.
/// Returns `{:ok, {num_bits, count, num_hashes}}` or `{:error, :enoent}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_info(env: Env, path: String) -> NifResult<Term> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    Ok((atoms::ok(), (num_bits, count, num_hashes as u64)).encode(env))
}

// ---------------------------------------------------------------------------
// Rust unit tests (stateless file-based functions only)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn file_create_and_read_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bloom");

        // Create a bloom file manually
        let num_bits = 1000u64;
        let num_hashes = 7u32;
        let byte_count = num_bits.div_ceil(8) as usize;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&num_bits.to_le_bytes());
        header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&vec![0u8; byte_count]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let file = File::open(&path).unwrap();
        let (bits, hashes, count) = file_read_header(&file).unwrap();
        assert_eq!(bits, 1000);
        assert_eq!(hashes, 7);
        assert_eq!(count, 0);
    }

    #[test]
    fn file_hash_positions_deterministic() {
        let pos1 = file_hash_positions(b"hello", 1000, 7);
        let pos2 = file_hash_positions(b"hello", 1000, 7);
        assert_eq!(pos1, pos2);
        assert_eq!(pos1.len(), 7);
        for &p in &pos1 {
            assert!(p < 1000);
        }
    }

    #[test]
    fn file_hash_positions_different_elements_differ() {
        let pos1 = file_hash_positions(b"hello", 100_000, 7);
        let pos2 = file_hash_positions(b"world", 100_000, 7);
        assert_ne!(pos1, pos2);
    }

    #[test]
    fn file_read_header_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.bloom");
        std::fs::write(&path, [0xFF; 64]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(file_read_header(&file).is_err());
    }

    #[test]
    fn map_io_error_enoent() {
        let e = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        match map_io_error(&e) {
            FileError::Enoent => {}
            FileError::Other(_) => panic!("expected Enoent"),
        }
    }

    #[test]
    fn map_io_error_other() {
        let e = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        match map_io_error(&e) {
            FileError::Other(s) => assert!(s.contains("denied")),
            FileError::Enoent => panic!("expected Other"),
        }
    }
}
