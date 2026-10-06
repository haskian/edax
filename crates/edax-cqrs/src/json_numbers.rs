//! The integer width a JSON reader keeps exact, checked on the input text before `serde_json` parses
//! it: the text is the one place the width is still known. The canonical form keeps a double's safe
//! integers ([`Width::Double`]); the filter, the sort and a stored document keep 64 bits
//! ([`Width::Bits64`]).

use edax_core::Error;

/// The largest integer magnitude the canonical form keeps exact: `2^53 - 1`, I-JSON's safe range.
pub(crate) const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// The integers a reader keeps exactly.
#[derive(Clone, Copy)]
pub(crate) enum Width {
    /// Magnitude at most [`MAX_SAFE_INTEGER`]: what an IEEE 754 double, and so RFC 8785, holds
    /// without ambiguity.
    Double,
    /// `i64::MIN ..= u64::MAX`: what the filter and the sort compare exactly. A query spec states a
    /// comparand exactly, and `serde_json` reads `2^70` as the nearest float, so `eq(x, 2**70)` would
    /// also match `2**70 + 1`, and a document holding `2^70` would be read as that float.
    Bits64,
}

impl Width {
    fn holds(self, n: i128) -> bool {
        match self {
            Width::Double => n.unsigned_abs() <= u128::from(MAX_SAFE_INTEGER),
            Width::Bits64 => (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(&n),
        }
    }

    fn range(self) -> &'static str {
        match self {
            Width::Double => "-(2^53-1)..=2^53-1, the I-JSON safe integers RFC 8785 keeps exact",
            Width::Bits64 => "the 64-bit range",
        }
    }
}

/// Refuse, as `codec`, every number token in `json` outside `width` (integers) or outside a double's
/// range (others). A wider number written as a float (`1e21`) is a float and passes.
///
/// A lexer, not a parser: it only tells strings from number tokens, and leaves the grammar to
/// `serde_json`. A digit inside a string is text, so strings are skipped whole, escapes included.
pub(crate) fn check_integers(json: &[u8], width: Width) -> Result<(), Error> {
    let mut at = 0;
    while let Some(&byte) = json.get(at) {
        match byte {
            b'"' => at = past_string(json, at + 1),
            b'-' | b'0'..=b'9' => {
                let start = at;
                while matches!(json.get(at), Some(b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')) {
                    at += 1;
                }
                check_number(json.get(start..at).unwrap_or_default(), start, width)?;
            }
            _ => at += 1,
        }
    }
    Ok(())
}

/// The offset just past the string whose body starts at `at` (or the end of an unterminated one).
fn past_string(json: &[u8], mut at: usize) -> usize {
    while let Some(&byte) = json.get(at) {
        at += 1;
        match byte {
            b'\\' => at += 1,
            b'"' => break,
            _ => {}
        }
    }
    at
}

fn check_number(token: &[u8], at: usize, width: Width) -> Result<(), Error> {
    // The token holds only ASCII digits and signs, so it is always UTF-8.
    let text = std::str::from_utf8(token).unwrap_or_default();
    if token.iter().any(|b| matches!(b, b'.' | b'e' | b'E')) {
        if text.parse::<f64>().is_ok_and(f64::is_infinite) {
            return Err(Error::codec(format_args!("number at byte {at} is out of range")));
        }
        return Ok(());
    }
    let holds = match text.parse::<i128>() {
        Ok(n) => width.holds(n),
        Err(e) => match e.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => false,
            // Not a number at all (`-`, `1-2`): the grammar check that follows names it.
            _ => true,
        },
    };
    if !holds {
        return Err(Error::codec(format_args!(
            "integer at byte {at} is outside {}",
            width.range()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The query side keeps every 64-bit integer exact and refuses only what is wider.
    #[test]
    fn bits64_branches() {
        let cases: &[(&str, &str, Result<(), &str>)] = &[
            (
                "i64_min_and_u64_max",
                "[-9223372036854775808,18446744073709551615]",
                Ok(()),
            ),
            ("past_2_pow_53_is_exact_here", "9007199254740993", Ok(())),
            (
                "past_u64",
                "18446744073709551616",
                Err("codec: integer at byte 0 is outside the 64-bit range"),
            ),
            (
                "below_i64_min",
                r#"{"v":-9223372036854775809}"#,
                Err("codec: integer at byte 5 is outside the 64-bit range"),
            ),
            ("wide_float_is_a_float", "1e21", Ok(())),
            ("in_a_string", r#""18446744073709551616""#, Ok(())),
        ];
        for (name, input, want) in cases {
            let got = check_integers(input.as_bytes(), Width::Bits64).map_err(|e| e.to_string());
            assert_eq!(got, want.map_err(ToString::to_string), "{name}");
        }
    }
}
