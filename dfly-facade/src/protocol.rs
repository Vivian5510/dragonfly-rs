//! Protocol-level command parsing and encoding primitives.

use std::str;

use dfly_common::error::{DflyError, DflyResult};

/// Supported client wire protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientProtocol {
    /// Redis RESP protocol family.
    Resp,
    /// Memcached text protocol.
    Memcache,
}

/// Protocol-decoded command representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommand {
    /// Command name in canonical uppercase form.
    pub name: String,
    /// Raw argument payload.
    pub args: Vec<Vec<u8>>,
}

/// Parsing result for a streaming connection buffer.
///
/// Dragonfly's network stack parses incrementally because one socket read may contain
/// less than one command or multiple commands. This enum models that exact boundary:
/// either we need more bytes, or one complete command was decoded and we report how many
/// bytes were consumed from the front of the buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseStatus {
    /// Current buffer does not yet hold one full command.
    Incomplete,
    /// One command was decoded successfully.
    Complete {
        /// Parsed command in canonical form.
        command: ParsedCommand,
        /// Number of bytes consumed from the read buffer.
        consumed: usize,
    },
}

/// Attempts to parse one command from the provided input bytes.
///
/// The function never mutates input and is safe for repeated calls on a growing buffer.
/// Callers should remove `consumed` bytes only when `ParseStatus::Complete` is returned.
///
/// # Errors
///
/// Returns `DflyError::Protocol` when the input contains a protocol violation
/// (for example malformed RESP framing or invalid UTF-8 command name).
pub fn parse_next_command(protocol: ClientProtocol, input: &[u8]) -> DflyResult<ParseStatus> {
    match protocol {
        ClientProtocol::Resp => parse_next_resp_command(input),
        ClientProtocol::Memcache => parse_next_memcache_command(input),
    }
}

/// Parses a single RESP array command (`*<n> ...`) from the input buffer.
fn parse_next_resp_command(input: &[u8]) -> DflyResult<ParseStatus> {
    if input.is_empty() {
        return Ok(ParseStatus::Incomplete);
    }
    if input[0] != b'*' {
        return Err(DflyError::Protocol(
            "RESP command must start with '*'".to_owned(),
        ));
    }

    let mut cursor = 1;
    let Some((array_len_line, next)) = read_crlf_line(input, cursor)? else {
        return Ok(ParseStatus::Incomplete);
    };
    cursor = next;

    let array_len = parse_decimal_i64(array_len_line, "RESP array length")?;
    if array_len <= 0 {
        return Err(DflyError::Protocol(
            "RESP command array length must be positive".to_owned(),
        ));
    }
    let array_len = usize::try_from(array_len).map_err(|_| {
        DflyError::Protocol("RESP array length exceeds supported platform usize".to_owned())
    })?;

    let mut items = Vec::with_capacity(array_len);
    for _ in 0..array_len {
        if cursor >= input.len() {
            return Ok(ParseStatus::Incomplete);
        }
        if input[cursor] != b'$' {
            return Err(DflyError::Protocol(
                "RESP command items must be bulk strings".to_owned(),
            ));
        }
        cursor += 1;

        let Some((len_line, next_after_len)) = read_crlf_line(input, cursor)? else {
            return Ok(ParseStatus::Incomplete);
        };
        cursor = next_after_len;

        let bulk_len = parse_decimal_i64(len_line, "RESP bulk length")?;
        if bulk_len < 0 {
            return Err(DflyError::Protocol(
                "RESP command bulk length cannot be negative".to_owned(),
            ));
        }
        let bulk_len = usize::try_from(bulk_len).map_err(|_| {
            DflyError::Protocol("RESP bulk length exceeds supported platform usize".to_owned())
        })?;

        // Need `bulk_len` data bytes plus trailing CRLF.
        if input.len().saturating_sub(cursor) < bulk_len + 2 {
            return Ok(ParseStatus::Incomplete);
        }

        let payload = &input[cursor..cursor + bulk_len];
        cursor += bulk_len;
        if input[cursor] != b'\r' || input[cursor + 1] != b'\n' {
            return Err(DflyError::Protocol(
                "RESP bulk payload must end with CRLF".to_owned(),
            ));
        }
        cursor += 2;
        items.push(payload.to_vec());
    }

    let Some(command_name_raw) = items.first() else {
        return Err(DflyError::Protocol(
            "RESP command requires command name".to_owned(),
        ));
    };
    if command_name_raw.is_empty() {
        return Err(DflyError::Protocol(
            "RESP command name cannot be empty".to_owned(),
        ));
    }
    let command_name = parse_uppercase_token(command_name_raw, "RESP command name")?;

    Ok(ParseStatus::Complete {
        command: ParsedCommand {
            name: command_name,
            args: items[1..].to_vec(),
        },
        consumed: cursor,
    })
}

/// Parses one Memcached text-protocol command line from the input buffer.
///
/// For Unit 1 we focus on the command-line form where one request maps to one CRLF line.
/// Value-body commands (for example `set`) are modeled later together with storage semantics.
fn parse_next_memcache_command(input: &[u8]) -> DflyResult<ParseStatus> {
    let Some((line, line_consumed)) = read_crlf_line(input, 0)? else {
        return Ok(ParseStatus::Incomplete);
    };

    if line.is_empty() {
        return Err(DflyError::Protocol(
            "memcache command line cannot be empty".to_owned(),
        ));
    }

    let parts = line
        .split(|byte| *byte == b' ')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();

    let Some(name_raw) = parts.first() else {
        return Err(DflyError::Protocol(
            "memcache command requires command name".to_owned(),
        ));
    };
    let name = parse_uppercase_token(name_raw, "memcache command name")?;

    // Memcache storage commands carry value payload in a second logical segment after the
    // command line. We normalize it into the same `SET key value` frame used by RESP path.
    if name == "SET" {
        return parse_memcache_set_command(input, &parts, line_consumed);
    }

    let args = parts.iter().skip(1).map(|part| (*part).to_vec()).collect();

    Ok(ParseStatus::Complete {
        command: ParsedCommand { name, args },
        consumed: line_consumed,
    })
}

/// Reads one CRLF-terminated logical line starting at `start`.
///
/// Returned `usize` points to the first byte after the `\r\n` delimiter.
fn read_crlf_line(input: &[u8], start: usize) -> DflyResult<Option<(&[u8], usize)>> {
    if start >= input.len() {
        return Ok(None);
    }

    let mut cursor = start;
    while cursor + 1 < input.len() {
        if input[cursor] == b'\r' {
            if input[cursor + 1] == b'\n' {
                return Ok(Some((&input[start..cursor], cursor + 2)));
            }
            return Err(DflyError::Protocol(
                "line terminator must be CRLF".to_owned(),
            ));
        }
        cursor += 1;
    }

    Ok(None)
}

/// Parses a decimal integer used by RESP lengths.
fn parse_decimal_i64(value: &[u8], field_name: &str) -> DflyResult<i64> {
    let text = str::from_utf8(value)
        .map_err(|_| DflyError::Protocol(format!("{field_name} must be valid UTF-8")))?;
    text.parse::<i64>()
        .map_err(|_| DflyError::Protocol(format!("{field_name} must be a valid integer")))
}

/// Parses one storage `set` command with payload block from memcache protocol.
fn parse_memcache_set_command(
    input: &[u8],
    parts: &[&[u8]],
    line_consumed: usize,
) -> DflyResult<ParseStatus> {
    if !(parts.len() == 5 || parts.len() == 6) {
        return Err(DflyError::Protocol(
            "memcache set requires: set <key> <flags> <exptime> <bytes> [noreply]".to_owned(),
        ));
    }
    if parts.len() == 6 && !parts[5].eq_ignore_ascii_case(b"noreply") {
        return Err(DflyError::Protocol(
            "memcache set optional trailing token must be 'noreply'".to_owned(),
        ));
    }

    let key = parts[1].to_vec();
    let value_len = parse_decimal_usize(parts[4], "memcache set <bytes>")?;
    if input.len().saturating_sub(line_consumed) < value_len + 2 {
        return Ok(ParseStatus::Incomplete);
    }

    let value_start = line_consumed;
    let value_end = value_start + value_len;
    if input[value_end] != b'\r' || input[value_end + 1] != b'\n' {
        return Err(DflyError::Protocol(
            "memcache set value payload must end with CRLF".to_owned(),
        ));
    }

    Ok(ParseStatus::Complete {
        command: ParsedCommand {
            name: "SET".to_owned(),
            args: vec![key, input[value_start..value_end].to_vec()],
        },
        consumed: value_end + 2,
    })
}

/// Parses a UTF-8 token and normalizes it to uppercase ASCII.
fn parse_uppercase_token(value: &[u8], field_name: &str) -> DflyResult<String> {
    str::from_utf8(value)
        .map(str::to_owned)
        .map(|token| token.to_ascii_uppercase())
        .map_err(|_| DflyError::Protocol(format!("{field_name} must be valid UTF-8")))
}

/// Parses a decimal `usize` value used by payload length fields.
fn parse_decimal_usize(value: &[u8], field_name: &str) -> DflyResult<usize> {
    let value = parse_decimal_i64(value, field_name)?;
    if value < 0 {
        return Err(DflyError::Protocol(format!(
            "{field_name} must be non-negative"
        )));
    }
    usize::try_from(value)
        .map_err(|_| DflyError::Protocol(format!("{field_name} exceeds supported usize")))
}

#[cfg(test)]
mod tests {
    use super::{ClientProtocol, ParseStatus, parse_next_command};
    use googletest::prelude::*;
    use rstest::rstest;

    #[rstest]
    fn parse_resp_set_command() {
        let wire = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let status = parse_next_command(ClientProtocol::Resp, wire).expect("valid RESP command");
        let ParseStatus::Complete { command, consumed } = status else {
            panic!("expected complete parse");
        };
        let expected_args = vec![b"foo".to_vec(), b"bar".to_vec()];

        assert_that!(consumed, eq(wire.len()));
        assert_that!(command.name, eq("SET"));
        assert_that!(&command.args, eq(&expected_args));
    }

    #[rstest]
    fn parse_resp_reports_incomplete_frame() {
        let partial = b"*2\r\n$4\r\nECHO\r\n$5\r\nhel";
        let status =
            parse_next_command(ClientProtocol::Resp, partial).expect("incomplete is not failure");
        assert_that!(&status, eq(&ParseStatus::Incomplete));
    }

    #[rstest]
    fn parse_memcache_get_command() {
        let wire = b"get user:42\r\n";
        let status =
            parse_next_command(ClientProtocol::Memcache, wire).expect("valid memcache command");
        let ParseStatus::Complete { command, consumed } = status else {
            panic!("expected complete parse");
        };
        let expected_args = vec![b"user:42".to_vec()];

        assert_that!(consumed, eq(wire.len()));
        assert_that!(command.name, eq("GET"));
        assert_that!(&command.args, eq(&expected_args));
    }

    #[rstest]
    fn parse_memcache_set_command_with_value_payload() {
        let wire = b"set user:42 0 0 5\r\nalice\r\n";
        let status =
            parse_next_command(ClientProtocol::Memcache, wire).expect("valid memcache set");
        let ParseStatus::Complete { command, consumed } = status else {
            panic!("expected complete parse");
        };
        let expected_args = vec![b"user:42".to_vec(), b"alice".to_vec()];

        assert_that!(consumed, eq(wire.len()));
        assert_that!(command.name, eq("SET"));
        assert_that!(&command.args, eq(&expected_args));
    }

    #[rstest]
    fn parse_memcache_set_reports_incomplete_value_payload() {
        let partial = b"set user:42 0 0 5\r\nali";
        let status = parse_next_command(ClientProtocol::Memcache, partial)
            .expect("incomplete payload should not fail");
        assert_that!(&status, eq(&ParseStatus::Incomplete));
    }

    #[rstest]
    #[case(ClientProtocol::Resp, b"+PING\r\n".as_slice())]
    #[case(ClientProtocol::Memcache, b"\r\n".as_slice())]
    fn parse_rejects_malformed_payloads(#[case] protocol: ClientProtocol, #[case] wire: &[u8]) {
        let result = parse_next_command(protocol, wire);
        assert_that!(result.is_err(), eq(true));
    }
}
