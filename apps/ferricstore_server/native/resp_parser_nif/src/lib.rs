use rustler::{Binary, Encoder, Env, NewBinary, NifResult, Term};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        inline,
        simple,
        nil,
        fallback,
        value_too_large,
        invalid_bulk_length,
        protocol_error,
    }
}

const MAX_ARRAY_COUNT: i64 = 1_048_576;

// =========================================================================
// Pure parsing layer — no Env/Term, fully testable.
// Only compiled in test builds; the NIF layer above is the production path.
// =========================================================================

#[cfg(test)]
mod resp {
    use super::*;

    /// A parsed RESP value with byte-slice references into the input buffer.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum RespValue<'a> {
        /// Bulk string data — a slice of the input buffer.
        BulkString(&'a [u8]),
        /// Simple string (+OK\r\n) — the string content.
        SimpleString(&'a [u8]),
        /// Simple error (-ERR ...\r\n) — the error content.
        SimpleError(&'a [u8]),
        /// RESP integer.
        Integer(i64),
        /// Nil ($-1 or *-1 or _).
        Nil,
        /// Array of values.
        Array(Vec<RespValue<'a>>),
        /// Inline command — tokens split on whitespace.
        Inline(Vec<&'a [u8]>),
    }

    /// Result of parsing one RESP element from the buffer.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum RespParseResult<'a> {
        /// Successfully parsed value + new position.
        Ok(RespValue<'a>, usize),
        /// Not enough data yet.
        Incomplete,
        /// Unsupported RESP3 type — caller should fall back.
        Fallback,
        /// Protocol error.
        Error(RespError),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum RespError {
        InvalidArrayCount(String),
        ArrayTooLarge,
        InvalidBulkLength(String),
        ValueTooLarge { len: usize, max: usize },
        BulkCrlfMissing,
        InvalidInteger(String),
        InvalidNull(String),
        InlineTooLong,
        ProtocolError(String),
    }

    /// Parse all complete RESP messages from `buf`.
    /// Returns (parsed_values, consumed_bytes) on success.
    pub(crate) fn parse_resp(buf: &[u8], max_value_size: usize) -> Result<(Vec<RespValue<'_>>, usize), RespError> {
        let mut pos = 0;
        let mut commands = Vec::new();

        loop {
            if pos >= buf.len() {
                break;
            }

            match parse_one_resp(buf, pos, max_value_size) {
                RespParseResult::Ok(val, new_pos) => {
                    commands.push(val);
                    pos = new_pos;
                }
                RespParseResult::Incomplete => break,
                RespParseResult::Fallback => return Err(RespError::ProtocolError("fallback".into())),
                RespParseResult::Error(e) => return Err(e),
            }
        }

        Ok((commands, pos))
    }

    pub(crate) fn parse_one_resp(buf: &[u8], pos: usize, max_value_size: usize) -> RespParseResult<'_> {
        if pos >= buf.len() {
            return RespParseResult::Incomplete;
        }

        match buf[pos] {
            b'*' => parse_array_resp(buf, pos + 1, max_value_size),
            b'$' => parse_bulk_string_resp(buf, pos + 1, max_value_size),
            b'+' => parse_simple_string_resp(buf, pos + 1),
            b'-' => parse_simple_error_resp(buf, pos + 1),
            b':' => parse_integer_resp(buf, pos + 1),
            b'_' => parse_null_resp(buf, pos + 1),
            b'#' | b',' | b'(' | b'!' | b'=' | b'%' | b'~' | b'>' | b'|' => RespParseResult::Fallback,
            _ => parse_inline_resp(buf, pos),
        }
    }

    fn parse_array_resp(buf: &[u8], pos: usize, max_value_size: usize) -> RespParseResult<'_> {
        let (line, after_crlf) = match find_crlf(buf, pos) {
            Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
            None => return RespParseResult::Incomplete,
        };

        if line == b"-1" {
            return RespParseResult::Ok(RespValue::Nil, after_crlf);
        }

        let count = match parse_int_bytes(line) {
            Some(n) => n,
            None => {
                return RespParseResult::Error(RespError::InvalidArrayCount(lossy_str(line)));
            }
        };

        if count > MAX_ARRAY_COUNT {
            return RespParseResult::Error(RespError::ArrayTooLarge);
        }

        if count < 0 {
            return RespParseResult::Error(RespError::InvalidArrayCount(lossy_str(line)));
        }

        let mut elements = Vec::with_capacity(count as usize);
        let mut cur = after_crlf;

        for _ in 0..count {
            match parse_one_resp(buf, cur, max_value_size) {
                RespParseResult::Ok(val, new_pos) => {
                    elements.push(val);
                    cur = new_pos;
                }
                RespParseResult::Incomplete => return RespParseResult::Incomplete,
                RespParseResult::Fallback => return RespParseResult::Fallback,
                RespParseResult::Error(e) => return RespParseResult::Error(e),
            }
        }

        RespParseResult::Ok(RespValue::Array(elements), cur)
    }

    fn parse_bulk_string_resp(buf: &[u8], pos: usize, max_value_size: usize) -> RespParseResult<'_> {
        let (line, after_crlf) = match find_crlf(buf, pos) {
            Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
            None => return RespParseResult::Incomplete,
        };

        if line == b"-1" {
            return RespParseResult::Ok(RespValue::Nil, after_crlf);
        }

        let len = match parse_int_bytes(line) {
            Some(n) if n >= 0 => n as usize,
            Some(_) => {
                return RespParseResult::Error(RespError::InvalidBulkLength(lossy_str(line)));
            }
            None => {
                return RespParseResult::Error(RespError::InvalidBulkLength(lossy_str(line)));
            }
        };

        if len > max_value_size {
            return RespParseResult::Error(RespError::ValueTooLarge {
                len,
                max: max_value_size,
            });
        }

        let needed = after_crlf + len + 2;
        if needed > buf.len() {
            return RespParseResult::Incomplete;
        }

        if buf[after_crlf + len] != b'\r' || buf[after_crlf + len + 1] != b'\n' {
            return RespParseResult::Error(RespError::BulkCrlfMissing);
        }

        RespParseResult::Ok(
            RespValue::BulkString(&buf[after_crlf..after_crlf + len]),
            after_crlf + len + 2,
        )
    }

    fn parse_simple_string_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (line_end, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };
        RespParseResult::Ok(RespValue::SimpleString(&buf[pos..line_end]), after_crlf)
    }

    fn parse_simple_error_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (line_end, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };
        RespParseResult::Ok(RespValue::SimpleError(&buf[pos..line_end]), after_crlf)
    }

    fn parse_integer_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };

        let line = &buf[pos..cr_pos];
        match parse_int_bytes(line) {
            Some(n) => RespParseResult::Ok(RespValue::Integer(n), after_crlf),
            None => RespParseResult::Error(RespError::InvalidInteger(lossy_str(line))),
        }
    }

    fn parse_null_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };

        if cr_pos != pos {
            return RespParseResult::Error(RespError::InvalidNull(lossy_str(&buf[pos..cr_pos])));
        }

        RespParseResult::Ok(RespValue::Nil, after_crlf)
    }

    fn parse_inline_resp(buf: &[u8], pos: usize) -> RespParseResult<'_> {
        let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
            Some(v) => v,
            None => return RespParseResult::Incomplete,
        };

        let line = &buf[pos..cr_pos];
        if line.len() > 1_048_576 {
            return RespParseResult::Error(RespError::InlineTooLong);
        }

        let tokens: Vec<&[u8]> = line
            .split(|&b| b == b' ' || b == b'\t')
            .filter(|s| !s.is_empty())
            .collect();

        RespParseResult::Ok(RespValue::Inline(tokens), after_crlf)
    }
}

// =========================================================================
// NIF layer — thin wrapper converting RespValue -> Term
// =========================================================================

#[rustler::nif]
fn parse<'a>(env: Env<'a>, data: Binary<'a>, max_value_size: usize) -> NifResult<Term<'a>> {
    let buf = data.as_slice();
    let mut pos = 0;
    let mut commands: Vec<Term<'a>> = Vec::new();

    loop {
        if pos >= buf.len() {
            break;
        }

        match parse_one(env, &data, buf, pos, max_value_size) {
            ParseResult::Ok(term, new_pos) => {
                commands.push(term);
                pos = new_pos;
            }
            ParseResult::Incomplete => break,
            ParseResult::Fallback => return Ok(atoms::fallback().encode(env)),
            ParseResult::Error(reason) => {
                return Ok((atoms::error(), reason).encode(env));
            }
        }
    }

    let rest_len = buf.len() - pos;
    let rest = if rest_len == 0 {
        "".encode(env)
    } else {
        unsafe { data.make_subbinary_unchecked(pos, rest_len) }.encode(env)
    };
    let list = commands.encode(env);
    Ok((atoms::ok(), list, rest).encode(env))
}

enum ParseResult<'a> {
    Ok(Term<'a>, usize),
    Incomplete,
    Fallback,
    Error(Term<'a>),
}

fn parse_one<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    if pos >= buf.len() {
        return ParseResult::Incomplete;
    }

    match buf[pos] {
        b'*' => parse_array(env, data, buf, pos + 1, max_value_size),
        b'$' => parse_bulk_string(env, data, buf, pos + 1, max_value_size),
        b'+' => parse_simple_string(env, data, buf, pos + 1),
        b'-' => parse_simple_error(env, data, buf, pos + 1),
        b':' => parse_integer(env, buf, pos + 1),
        b'_' => parse_null(env, buf, pos + 1),
        b'#' | b',' | b'(' | b'!' | b'=' | b'%' | b'~' | b'>' | b'|' => ParseResult::Fallback,
        _ => parse_inline(env, buf, pos),
    }
}

fn parse_array<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseResult::Incomplete,
    };

    if line == b"-1" {
        return ParseResult::Ok(atoms::nil().encode(env), after_crlf);
    }

    let count = match parse_int_bytes(line) {
        Some(n) => n,
        None => {
            let line_str = lossy_str(line);
            return ParseResult::Error(
                (
                    rustler::types::atom::Atom::from_str(env, "invalid_array_count").unwrap(),
                    line_str,
                )
                    .encode(env),
            );
        }
    };

    if count > MAX_ARRAY_COUNT {
        return ParseResult::Error(make_binary_term(
            env,
            b"ERR protocol error: array too large",
        ));
    }

    if count < 0 {
        let line_str = lossy_str(line);
        return ParseResult::Error(
            (
                rustler::types::atom::Atom::from_str(env, "invalid_array_count").unwrap(),
                line_str,
            )
                .encode(env),
        );
    }

    let mut elements: Vec<Term<'a>> = Vec::with_capacity(count as usize);
    let mut cur = after_crlf;

    for _ in 0..count {
        match parse_one(env, data, buf, cur, max_value_size) {
            ParseResult::Ok(term, new_pos) => {
                elements.push(term);
                cur = new_pos;
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Fallback => return ParseResult::Fallback,
            ParseResult::Error(e) => return ParseResult::Error(e),
        }
    }

    ParseResult::Ok(elements.encode(env), cur)
}

fn parse_bulk_string<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
    max_value_size: usize,
) -> ParseResult<'a> {
    let (line, after_crlf) = match find_crlf(buf, pos) {
        Some((cr_pos, after)) => (&buf[pos..cr_pos], after),
        None => return ParseResult::Incomplete,
    };

    if line == b"-1" {
        return ParseResult::Ok(atoms::nil().encode(env), after_crlf);
    }

    let len = match parse_int_bytes(line) {
        Some(n) if n >= 0 => n as usize,
        Some(_) => {
            let line_str = lossy_str(line);
            return ParseResult::Error((atoms::invalid_bulk_length(), line_str).encode(env));
        }
        None => {
            let line_str = lossy_str(line);
            return ParseResult::Error((atoms::invalid_bulk_length(), line_str).encode(env));
        }
    };

    if len > max_value_size {
        return ParseResult::Error((atoms::value_too_large(), len, max_value_size).encode(env));
    }

    let needed = after_crlf + len + 2;
    if needed > buf.len() {
        return ParseResult::Incomplete;
    }

    if buf[after_crlf + len] != b'\r' || buf[after_crlf + len + 1] != b'\n' {
        return ParseResult::Error(make_binary_term(env, b"bulk_crlf_missing"));
    }

    let term = unsafe { data.make_subbinary_unchecked(after_crlf, len) }.encode(env);
    ParseResult::Ok(term, after_crlf + len + 2)
}

fn parse_simple_string<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
) -> ParseResult<'a> {
    let (line_end, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };
    let line = unsafe { data.make_subbinary_unchecked(pos, line_end - pos) }.encode(env);
    ParseResult::Ok((atoms::simple(), line).encode(env), after_crlf)
}

fn parse_simple_error<'a>(
    env: Env<'a>,
    data: &Binary<'a>,
    buf: &[u8],
    pos: usize,
) -> ParseResult<'a> {
    let (line_end, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };
    let line = unsafe { data.make_subbinary_unchecked(pos, line_end - pos) }.encode(env);
    ParseResult::Ok((atoms::error(), line).encode(env), after_crlf)
}

fn parse_integer<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..cr_pos];
    match parse_int_bytes(line) {
        Some(n) => ParseResult::Ok(n.encode(env), after_crlf),
        None => {
            let line_str = lossy_str(line);
            ParseResult::Error(
                (
                    rustler::types::atom::Atom::from_str(env, "invalid_integer").unwrap(),
                    line_str,
                )
                    .encode(env),
            )
        }
    }
}

fn parse_null<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    if cr_pos != pos {
        let line = lossy_str(&buf[pos..cr_pos]);
        return ParseResult::Error(
            (
                rustler::types::atom::Atom::from_str(env, "invalid_null").unwrap(),
                line,
            )
                .encode(env),
        );
    }

    ParseResult::Ok(atoms::nil().encode(env), after_crlf)
}

fn parse_inline<'a>(env: Env<'a>, buf: &[u8], pos: usize) -> ParseResult<'a> {
    let (cr_pos, after_crlf) = match find_crlf(buf, pos) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let line = &buf[pos..cr_pos];
    if line.len() > 1_048_576 {
        return ParseResult::Error(make_binary_term(
            env,
            b"ERR protocol error: inline command too long",
        ));
    }

    // Inline tokens must be copied — they're substrings split on whitespace,
    // not contiguous ranges of the input binary.
    let tokens: Vec<Term<'a>> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|s| !s.is_empty())
        .map(|token| {
            let mut bin = NewBinary::new(env, token.len());
            bin.as_mut_slice().copy_from_slice(token);
            bin.into()
        })
        .collect();

    ParseResult::Ok((atoms::inline(), tokens).encode(env), after_crlf)
}

// -- Helpers --

fn find_crlf(buf: &[u8], start: usize) -> Option<(usize, usize)> {
    if buf.len() < start + 2 {
        return None;
    }
    let end = buf.len() - 1;
    let mut i = start;
    while i < end {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((i, i + 2));
        }
        i += 1;
    }
    None
}

fn parse_int_bytes(data: &[u8]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    let (negative, start) = if data[0] == b'-' {
        (true, 1)
    } else if data[0] == b'+' {
        (false, 1)
    } else {
        (false, 0)
    };

    if start >= data.len() {
        return None;
    }

    let mut result: i64 = 0;
    for &b in &data[start..] {
        if !b.is_ascii_digit() {
            return None;
        }
        result = result.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }

    if negative {
        Some(-result)
    } else {
        Some(result)
    }
}

fn make_binary_term<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let mut bin = NewBinary::new(env, data.len());
    bin.as_mut_slice().copy_from_slice(data);
    bin.into()
}

fn lossy_str(data: &[u8]) -> String {
    String::from_utf8_lossy(data).into_owned()
}

rustler::init!("Elixir.FerricstoreServer.Resp.ParserNif");

#[cfg(test)]
mod tests {
    use super::*;
    use super::resp::*;

    const MAX_SIZE: usize = 512 * 1024 * 1024; // 512 MB — generous limit for tests

    // Helper: parse single value, assert Ok, return (value, consumed_bytes)
    fn parse_ok(input: &[u8]) -> (RespValue<'_>, usize) {
        match parse_one_resp(input, 0, MAX_SIZE) {
            RespParseResult::Ok(v, pos) => (v, pos),
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    // =========================================================================
    // parse_int_bytes
    // =========================================================================

    #[test]
    fn parse_int_positive() {
        assert_eq!(parse_int_bytes(b"123"), Some(123));
        assert_eq!(parse_int_bytes(b"0"), Some(0));
        assert_eq!(parse_int_bytes(b"1"), Some(1));
        assert_eq!(parse_int_bytes(b"9999999999"), Some(9_999_999_999));
    }

    #[test]
    fn parse_int_negative() {
        assert_eq!(parse_int_bytes(b"-1"), Some(-1));
        assert_eq!(parse_int_bytes(b"-123"), Some(-123));
        assert_eq!(parse_int_bytes(b"-0"), Some(0));
    }

    #[test]
    fn parse_int_explicit_positive_sign() {
        assert_eq!(parse_int_bytes(b"+42"), Some(42));
        assert_eq!(parse_int_bytes(b"+0"), Some(0));
    }

    #[test]
    fn parse_int_empty() {
        assert_eq!(parse_int_bytes(b""), None);
    }

    #[test]
    fn parse_int_sign_only() {
        assert_eq!(parse_int_bytes(b"-"), None);
        assert_eq!(parse_int_bytes(b"+"), None);
    }

    #[test]
    fn parse_int_non_digit() {
        assert_eq!(parse_int_bytes(b"12a3"), None);
        assert_eq!(parse_int_bytes(b"abc"), None);
        assert_eq!(parse_int_bytes(b"1.5"), None);
        assert_eq!(parse_int_bytes(b" 1"), None);
        assert_eq!(parse_int_bytes(b"1 "), None);
    }

    #[test]
    fn parse_int_i64_max() {
        // i64::MAX = 9223372036854775807
        assert_eq!(parse_int_bytes(b"9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn parse_int_overflow() {
        // i64::MAX + 1 overflows
        assert_eq!(parse_int_bytes(b"9223372036854775808"), None);
        // Way beyond i64
        assert_eq!(parse_int_bytes(b"99999999999999999999"), None);
    }

    #[test]
    fn parse_int_negative_overflow() {
        // -9223372036854775808 = i64::MIN, but our impl does -result which can't represent MIN
        // parse_int_bytes computes positive then negates, so i64::MAX+1 as positive overflows
        assert_eq!(parse_int_bytes(b"-9223372036854775808"), None);
    }

    #[test]
    fn parse_int_leading_zeros() {
        assert_eq!(parse_int_bytes(b"007"), Some(7));
        assert_eq!(parse_int_bytes(b"00"), Some(0));
    }

    // =========================================================================
    // find_crlf
    // =========================================================================

    #[test]
    fn find_crlf_basic() {
        assert_eq!(find_crlf(b"hello\r\n", 0), Some((5, 7)));
        assert_eq!(find_crlf(b"OK\r\n", 0), Some((2, 4)));
    }

    #[test]
    fn find_crlf_at_start() {
        assert_eq!(find_crlf(b"\r\n", 0), Some((0, 2)));
        assert_eq!(find_crlf(b"\r\nrest", 0), Some((0, 2)));
    }

    #[test]
    fn find_crlf_with_offset() {
        assert_eq!(find_crlf(b"skip\r\n", 2), Some((4, 6)));
        assert_eq!(find_crlf(b"AB\r\nCD\r\n", 4), Some((6, 8)));
    }

    #[test]
    fn find_crlf_not_found() {
        assert_eq!(find_crlf(b"no crlf here", 0), None);
        assert_eq!(find_crlf(b"only\n", 0), None);
        assert_eq!(find_crlf(b"only\r", 0), None);
    }

    #[test]
    fn find_crlf_buffer_too_short() {
        assert_eq!(find_crlf(b"", 0), None);
        assert_eq!(find_crlf(b"x", 0), None);
        assert_eq!(find_crlf(b"ab", 1), None);
    }

    #[test]
    fn find_crlf_cr_without_lf() {
        assert_eq!(find_crlf(b"a\rb", 0), None);
        assert_eq!(find_crlf(b"\r\r\r", 0), None);
    }

    #[test]
    fn find_crlf_lf_without_cr() {
        assert_eq!(find_crlf(b"\n\n", 0), None);
        assert_eq!(find_crlf(b"a\nb\n", 0), None);
    }

    #[test]
    fn find_crlf_multiple_takes_first() {
        assert_eq!(find_crlf(b"a\r\nb\r\n", 0), Some((1, 3)));
    }

    #[test]
    fn find_crlf_start_beyond_buffer() {
        assert_eq!(find_crlf(b"ab\r\n", 10), None);
    }

    // =========================================================================
    // lossy_str
    // =========================================================================

    #[test]
    fn lossy_str_valid_utf8() {
        assert_eq!(lossy_str(b"hello"), "hello");
        assert_eq!(lossy_str(b""), "");
        assert_eq!(lossy_str(b"123"), "123");
    }

    #[test]
    fn lossy_str_invalid_utf8() {
        let result = lossy_str(&[0xFF, 0xFE, b'a']);
        assert!(result.contains('\u{FFFD}'));
        assert!(result.contains('a'));
    }

    #[test]
    fn lossy_str_mixed_valid_invalid() {
        let input = b"hello\xFFworld";
        let result = lossy_str(input);
        assert!(result.starts_with("hello"));
        assert!(result.ends_with("world"));
        assert!(result.contains('\u{FFFD}'));
    }

    // =========================================================================
    // MAX_ARRAY_COUNT constant
    // =========================================================================

    #[test]
    fn max_array_count_value() {
        assert_eq!(MAX_ARRAY_COUNT, 1_048_576);
    }

    // =========================================================================
    // Basic RESP types — parse_one_resp
    // =========================================================================

    #[test]
    fn bulk_string_basic() {
        let (val, pos) = parse_ok(b"$3\r\nfoo\r\n");
        assert_eq!(val, RespValue::BulkString(b"foo"));
        assert_eq!(pos, 9);
    }

    #[test]
    fn bulk_string_empty() {
        let (val, pos) = parse_ok(b"$0\r\n\r\n");
        assert_eq!(val, RespValue::BulkString(b""));
        assert_eq!(pos, 6);
    }

    #[test]
    fn bulk_string_nil() {
        let (val, _) = parse_ok(b"$-1\r\n");
        assert_eq!(val, RespValue::Nil);
    }

    #[test]
    fn bulk_string_binary_data() {
        // Bulk string containing bytes that are NOT valid utf-8
        let mut input = b"$4\r\n".to_vec();
        input.extend_from_slice(&[0x00, 0xFF, 0xFE, 0x01]);
        input.extend_from_slice(b"\r\n");
        let (val, pos) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&[0x00, 0xFF, 0xFE, 0x01]));
        assert_eq!(pos, input.len());
    }

    #[test]
    fn bulk_string_with_crlf_inside() {
        // Bulk string containing \r\n within its payload: "he\r\nlo" = 6 bytes
        let (val, pos) = parse_ok(b"$6\r\nhe\r\nlo\r\n");
        assert_eq!(val, RespValue::BulkString(b"he\r\nlo"));
        assert_eq!(pos, 12); // $6\r\n(4) + he\r\nlo(6) + \r\n(2) = 12
    }

    #[test]
    fn simple_string() {
        let (val, pos) = parse_ok(b"+OK\r\n");
        assert_eq!(val, RespValue::SimpleString(b"OK"));
        assert_eq!(pos, 5);
    }

    #[test]
    fn simple_string_empty() {
        let (val, _) = parse_ok(b"+\r\n");
        assert_eq!(val, RespValue::SimpleString(b""));
    }

    #[test]
    fn simple_string_with_spaces() {
        let (val, _) = parse_ok(b"+hello world\r\n");
        assert_eq!(val, RespValue::SimpleString(b"hello world"));
    }

    #[test]
    fn simple_error() {
        let (val, pos) = parse_ok(b"-ERR unknown command\r\n");
        assert_eq!(val, RespValue::SimpleError(b"ERR unknown command"));
        assert_eq!(pos, 22);
    }

    #[test]
    fn simple_error_empty() {
        let (val, _) = parse_ok(b"-\r\n");
        assert_eq!(val, RespValue::SimpleError(b""));
    }

    #[test]
    fn integer_positive() {
        let (val, pos) = parse_ok(b":42\r\n");
        assert_eq!(val, RespValue::Integer(42));
        assert_eq!(pos, 5);
    }

    #[test]
    fn integer_zero() {
        let (val, _) = parse_ok(b":0\r\n");
        assert_eq!(val, RespValue::Integer(0));
    }

    #[test]
    fn integer_negative() {
        let (val, _) = parse_ok(b":-1\r\n");
        assert_eq!(val, RespValue::Integer(-1));
    }

    #[test]
    fn integer_large() {
        let (val, _) = parse_ok(b":9223372036854775807\r\n");
        assert_eq!(val, RespValue::Integer(i64::MAX));
    }

    #[test]
    fn null_resp3() {
        let (val, pos) = parse_ok(b"_\r\n");
        assert_eq!(val, RespValue::Nil);
        assert_eq!(pos, 3);
    }

    #[test]
    fn array_empty() {
        let (val, pos) = parse_ok(b"*0\r\n");
        assert_eq!(val, RespValue::Array(vec![]));
        assert_eq!(pos, 4);
    }

    #[test]
    fn array_nil() {
        let (val, _) = parse_ok(b"*-1\r\n");
        assert_eq!(val, RespValue::Nil);
    }

    #[test]
    fn array_single_bulk_string() {
        let (val, _) = parse_ok(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(val, RespValue::Array(vec![RespValue::BulkString(b"PING")]));
    }

    #[test]
    fn array_mixed_types() {
        // Array with bulk string, integer, simple string
        let input = b"*3\r\n$3\r\nfoo\r\n:42\r\n+OK\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo"),
                RespValue::Integer(42),
                RespValue::SimpleString(b"OK"),
            ])
        );
    }

    #[test]
    fn array_nested() {
        // *2\r\n *1\r\n$1\r\na\r\n *1\r\n$1\r\nb\r\n
        let input = b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::BulkString(b"a")]),
                RespValue::Array(vec![RespValue::BulkString(b"b")]),
            ])
        );
    }

    // =========================================================================
    // Full commands via parse_resp
    // =========================================================================

    #[test]
    fn full_command_get() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(b"foo"),
            ])
        );
    }

    #[test]
    fn full_command_set() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![
                RespValue::BulkString(b"SET"),
                RespValue::BulkString(b"foo"),
                RespValue::BulkString(b"bar"),
            ])
        );
    }

    #[test]
    fn multiple_commands_in_buffer() {
        // Two pipelined commands
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 2);
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![RespValue::BulkString(b"PING")])
        );
        assert_eq!(
            cmds[1],
            RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(b"k"),
            ])
        );
    }

    #[test]
    fn command_with_trailing_partial() {
        // One complete command + partial second command
        // *1\r\n$4\r\nPING\r\n = 4+4+4+2 = 14 bytes
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 1); // only the PING command
        assert_eq!(consumed, 14); // consumed the first command
        assert_eq!(
            cmds[0],
            RespValue::Array(vec![RespValue::BulkString(b"PING")])
        );
    }

    // =========================================================================
    // Inline commands
    // =========================================================================

    #[test]
    fn inline_ping() {
        let (val, pos) = parse_ok(b"PING\r\n");
        assert_eq!(val, RespValue::Inline(vec![b"PING".as_slice()]));
        assert_eq!(pos, 6);
    }

    #[test]
    fn inline_set_with_args() {
        let (val, _) = parse_ok(b"SET foo bar\r\n");
        assert_eq!(
            val,
            RespValue::Inline(vec![
                b"SET".as_slice(),
                b"foo".as_slice(),
                b"bar".as_slice(),
            ])
        );
    }

    #[test]
    fn inline_extra_whitespace() {
        let (val, _) = parse_ok(b"SET  foo \t bar\r\n");
        assert_eq!(
            val,
            RespValue::Inline(vec![
                b"SET".as_slice(),
                b"foo".as_slice(),
                b"bar".as_slice(),
            ])
        );
    }

    #[test]
    fn inline_via_parse_resp() {
        let input = b"PING\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0], RespValue::Inline(vec![b"PING".as_slice()]));
    }

    #[test]
    fn inline_multiple_commands() {
        let input = b"PING\r\nINFO\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], RespValue::Inline(vec![b"PING".as_slice()]));
        assert_eq!(cmds[1], RespValue::Inline(vec![b"INFO".as_slice()]));
    }

    // =========================================================================
    // Incomplete messages
    // =========================================================================

    #[test]
    fn incomplete_empty_input() {
        let (cmds, consumed) = parse_resp(b"", MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 0);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn incomplete_partial_bulk_header() {
        // "$3\r\n" without the data
        let result = parse_one_resp(b"$3\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_bulk_data() {
        // "$3\r\nfo" — data truncated
        let result = parse_one_resp(b"$3\r\nfo", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_bulk_no_trailing_crlf() {
        // "$3\r\nfoo" — data present but missing trailing \r\n
        let result = parse_one_resp(b"$3\r\nfoo", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_array_header() {
        // "*2\r\n" without any elements
        let result = parse_one_resp(b"*2\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_partial_array_one_of_two() {
        // "*2\r\n$3\r\nfoo\r\n" — first element present, second missing
        let result = parse_one_resp(b"*2\r\n$3\r\nfoo\r\n", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_truncated_header_no_crlf() {
        let result = parse_one_resp(b"$3", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_just_type_byte() {
        assert_eq!(parse_one_resp(b"*", 0, MAX_SIZE), RespParseResult::Incomplete);
        assert_eq!(parse_one_resp(b"$", 0, MAX_SIZE), RespParseResult::Incomplete);
        assert_eq!(parse_one_resp(b"+", 0, MAX_SIZE), RespParseResult::Incomplete);
        assert_eq!(parse_one_resp(b"-", 0, MAX_SIZE), RespParseResult::Incomplete);
        assert_eq!(parse_one_resp(b":", 0, MAX_SIZE), RespParseResult::Incomplete);
        assert_eq!(parse_one_resp(b"_", 0, MAX_SIZE), RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_simple_string_no_crlf() {
        let result = parse_one_resp(b"+OK", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_integer_no_crlf() {
        let result = parse_one_resp(b":42", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_inline_no_crlf() {
        let result = parse_one_resp(b"PING", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);
    }

    #[test]
    fn incomplete_preserves_rest_in_parse_resp() {
        // One complete command + incomplete fragment
        let input = b"+OK\r\n$3\r\nfo";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(consumed, 5); // only "+OK\r\n"
        assert_eq!(cmds[0], RespValue::SimpleString(b"OK"));
    }

    // =========================================================================
    // Malformed input
    // =========================================================================

    #[test]
    fn malformed_invalid_array_count() {
        let result = parse_one_resp(b"*abc\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidArrayCount("abc".into()))
        );
    }

    #[test]
    fn malformed_negative_array_count() {
        // *-2 is invalid (only *-1 for nil array)
        let result = parse_one_resp(b"*-2\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidArrayCount("-2".into()))
        );
    }

    #[test]
    fn malformed_array_too_large() {
        // MAX_ARRAY_COUNT + 1
        let input = format!("*{}\r\n", MAX_ARRAY_COUNT + 1);
        let result = parse_one_resp(input.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::ArrayTooLarge));
    }

    #[test]
    fn malformed_bulk_string_exceeds_max_value_size() {
        // max_value_size = 10, but bulk string claims length 100
        let result = parse_one_resp(b"$100\r\n", 0, 10);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::ValueTooLarge { len: 100, max: 10 })
        );
    }

    #[test]
    fn malformed_invalid_bulk_length() {
        let result = parse_one_resp(b"$xyz\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidBulkLength("xyz".into()))
        );
    }

    #[test]
    fn malformed_negative_bulk_length() {
        // $-2 is invalid (only $-1 for nil)
        let result = parse_one_resp(b"$-2\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidBulkLength("-2".into()))
        );
    }

    #[test]
    fn malformed_bulk_string_missing_crlf_terminator() {
        // Data present but \r\n replaced with something else
        let result = parse_one_resp(b"$3\r\nfooXY", 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Error(RespError::BulkCrlfMissing));
    }

    #[test]
    fn malformed_invalid_integer() {
        let result = parse_one_resp(b":abc\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidInteger("abc".into()))
        );
    }

    #[test]
    fn malformed_invalid_null() {
        // "_" should be followed immediately by \r\n, not extra data
        let result = parse_one_resp(b"_extra\r\n", 0, MAX_SIZE);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::InvalidNull("extra".into()))
        );
    }

    #[test]
    fn malformed_error_propagates_through_parse_resp() {
        let result = parse_resp(b"$xyz\r\n", MAX_SIZE);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            RespError::InvalidBulkLength("xyz".into())
        );
    }

    // =========================================================================
    // RESP3 fallback types
    // =========================================================================

    #[test]
    fn fallback_resp3_types() {
        for &prefix in b"#,(!=%~>|" {
            let mut input = vec![prefix];
            input.extend_from_slice(b"data\r\n");
            let result = parse_one_resp(&input, 0, MAX_SIZE);
            assert_eq!(result, RespParseResult::Fallback, "prefix={}", prefix as char);
        }
    }

    #[test]
    fn fallback_propagates_through_parse_resp() {
        let result = parse_resp(b"#t\r\n", MAX_SIZE);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            RespError::ProtocolError("fallback".into())
        );
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn edge_bulk_string_large_valid() {
        let data = vec![b'x'; 1000];
        let mut input = format!("${}\r\n", data.len()).into_bytes();
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let (val, pos) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&data));
        assert_eq!(pos, input.len());
    }

    #[test]
    fn edge_array_with_nil_elements() {
        // Array containing nil bulk strings
        let input = b"*2\r\n$-1\r\n$-1\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![RespValue::Nil, RespValue::Nil])
        );
    }

    #[test]
    fn edge_position_tracking_across_multiple() {
        // Verify parse_resp consumes exactly the right number of bytes
        let input = b":1\r\n:2\r\n:3\r\n";
        let (cmds, consumed) = parse_resp(input, MAX_SIZE).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmds.len(), 3);
        assert_eq!(cmds[0], RespValue::Integer(1));
        assert_eq!(cmds[1], RespValue::Integer(2));
        assert_eq!(cmds[2], RespValue::Integer(3));
    }

    #[test]
    fn edge_bulk_string_with_zero_bytes() {
        // Bulk string containing null bytes
        let mut input = b"$3\r\n".to_vec();
        input.extend_from_slice(&[0x00, 0x00, 0x00]);
        input.extend_from_slice(b"\r\n");
        let (val, _) = parse_ok(&input);
        assert_eq!(val, RespValue::BulkString(&[0x00, 0x00, 0x00]));
    }

    #[test]
    fn edge_max_value_size_boundary() {
        // Exactly at the limit should succeed
        let data = vec![b'a'; 100];
        let mut input = format!("${}\r\n", data.len()).into_bytes();
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let (val, _) = match parse_one_resp(&input, 0, 100) {
            RespParseResult::Ok(v, p) => (v, p),
            other => panic!("expected Ok, got {:?}", other),
        };
        assert_eq!(val, RespValue::BulkString(data.as_slice()));

        // One over the limit should fail
        let data2 = vec![b'a'; 101];
        let mut input2 = format!("${}\r\n", data2.len()).into_bytes();
        input2.extend_from_slice(&data2);
        input2.extend_from_slice(b"\r\n");
        let result = parse_one_resp(&input2, 0, 100);
        assert_eq!(
            result,
            RespParseResult::Error(RespError::ValueTooLarge { len: 101, max: 100 })
        );
    }

    #[test]
    fn edge_array_at_max_count_boundary() {
        // Array count exactly at MAX_ARRAY_COUNT should be accepted (structurally)
        // but will be Incomplete since we don't provide the elements
        let input = format!("*{}\r\n", MAX_ARRAY_COUNT);
        let result = parse_one_resp(input.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result, RespParseResult::Incomplete);

        // One over should be rejected immediately
        let input2 = format!("*{}\r\n", MAX_ARRAY_COUNT + 1);
        let result2 = parse_one_resp(input2.as_bytes(), 0, MAX_SIZE);
        assert_eq!(result2, RespParseResult::Error(RespError::ArrayTooLarge));
    }

    #[test]
    fn edge_deeply_nested_array() {
        // *1\r\n *1\r\n *1\r\n $1\r\na\r\n
        let input = b"*1\r\n*1\r\n*1\r\n$1\r\na\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![RespValue::Array(vec![RespValue::Array(vec![
                RespValue::BulkString(b"a")
            ])])])
        );
    }

    #[test]
    fn edge_array_containing_error_element() {
        let input = b"*2\r\n$3\r\nfoo\r\n-ERR bad\r\n";
        let (val, _) = parse_ok(input);
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo"),
                RespValue::SimpleError(b"ERR bad"),
            ])
        );
    }

    #[test]
    fn edge_inline_single_char() {
        let (val, _) = parse_ok(b"Q\r\n");
        assert_eq!(val, RespValue::Inline(vec![b"Q".as_slice()]));
    }

    #[test]
    fn edge_parse_resp_empty_yields_no_commands() {
        let (cmds, consumed) = parse_resp(b"", MAX_SIZE).unwrap();
        assert_eq!(cmds.len(), 0);
        assert_eq!(consumed, 0);
    }
}
