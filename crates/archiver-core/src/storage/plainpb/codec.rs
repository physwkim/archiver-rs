//! LineEscaper — byte-compatible port of the Java archiver's LineEscaper.java.
//!
//! PB files store one protobuf message per line. Since protobuf binary can
//! contain newline bytes (0x0A), they must be escaped before writing and
//! unescaped when reading.
//!
//! Escape rules:
//!   0x1B (ESC) → [0x1B, 0x01]
//!   0x0A (LF)  → [0x1B, 0x02]
//!   0x0D (CR)  → [0x1B, 0x03]

const ESCAPE_CHAR: u8 = 0x1B;
const ESCAPE_ESCAPE_CHAR: u8 = 0x01;
const NEWLINE_CHAR: u8 = 0x0A;
const NEWLINE_ESCAPE_CHAR: u8 = 0x02;
const CARRIAGERETURN_CHAR: u8 = 0x0D;
const CARRIAGERETURN_ESCAPE_CHAR: u8 = 0x03;

/// Escape newlines, carriage returns, and escape chars in a byte slice.
pub fn escape(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    for &b in input {
        match b {
            ESCAPE_CHAR => {
                out.push(ESCAPE_CHAR);
                out.push(ESCAPE_ESCAPE_CHAR);
            }
            NEWLINE_CHAR => {
                out.push(ESCAPE_CHAR);
                out.push(NEWLINE_ESCAPE_CHAR);
            }
            CARRIAGERETURN_CHAR => {
                out.push(ESCAPE_CHAR);
                out.push(CARRIAGERETURN_ESCAPE_CHAR);
            }
            _ => out.push(b),
        }
    }
    out
}

/// Unescape a previously escaped byte slice, restoring original bytes.
pub fn unescape(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == ESCAPE_CHAR {
            i += 1;
            if i >= input.len() {
                break;
            }
            match input[i] {
                ESCAPE_ESCAPE_CHAR => out.push(ESCAPE_CHAR),
                NEWLINE_ESCAPE_CHAR => out.push(NEWLINE_CHAR),
                CARRIAGERETURN_ESCAPE_CHAR => out.push(CARRIAGERETURN_CHAR),
                other => out.push(other),
            }
        } else {
            out.push(input[i]);
        }
        i += 1;
    }
    out
}

pub const NEWLINE: u8 = NEWLINE_CHAR;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let original = vec![0x00, 0x1B, 0x0A, 0x0D, 0xFF, 0x42];
        let escaped = escape(&original);
        let unescaped = unescape(&escaped);
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_escape_values() {
        assert_eq!(escape(&[0x1B]), vec![0x1B, 0x01]);
        assert_eq!(escape(&[0x0A]), vec![0x1B, 0x02]);
        assert_eq!(escape(&[0x0D]), vec![0x1B, 0x03]);
        assert_eq!(escape(&[0x42]), vec![0x42]);
    }

    #[test]
    fn test_no_special_bytes() {
        let data = b"hello world";
        assert_eq!(escape(data), data.to_vec());
    }

    #[test]
    fn test_empty() {
        assert_eq!(escape(&[]), Vec::<u8>::new());
        assert_eq!(unescape(&[]), Vec::<u8>::new());
    }
}
