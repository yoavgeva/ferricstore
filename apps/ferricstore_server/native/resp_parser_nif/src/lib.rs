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
}
