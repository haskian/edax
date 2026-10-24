//! The text a projection document is stored as, and number text Postgres reads at the value the kernel
//! compares.
//!
//! `jsonb` keeps a number as the decimal value of its text, while the kernel reads a non-integer number as
//! the nearest `f64`. Two rules make the two agree:
//!
//! - The stored text is always the kernel's own re-serialization, never the host's text: a host writing
//!   `0.30000000000000000001` stores `0.3`, the double the kernel compares, not a value no double has.
//! - Below 2^53 the shortest text of a double sits closer to it than to any other double or integer, so
//!   ordering by that text orders by the double. At or past 2^53 it can sit on the far side of an integer
//!   (`5.808820077685227e16` is `58088200776852270`, below the integer `58088200776852271` that the
//!   double `58088200776852272.0` exceeds), so such a double is written as its exact integer digits with
//!   a `.0`: `58088200776852272.0`. `jsonb` keeps that value exactly, and every reader reads it back as
//!   the same double.
//!
//! No value is refused for its size. A document written to the table by anything but this function is
//! outside the guarantee.

use std::io;

use edax_core::Error;
use serde::Serialize;
use serde_json::ser::{CompactFormatter, Formatter, Serializer};
use serde_json::{Number, Value};

/// The magnitude from which a double's shortest text may misorder against an integer.
const EXACT_FROM: f64 = 9_007_199_254_740_992.0; // 2^53

/// The text projection `id`'s `document` is stored as: the kernel's keyed document (the row key as its
/// `"id"`, refused as `codec` when [`edax_cqrs::document_json`] refuses it), serialized by [`exact_json`].
pub fn stored_document(id: &str, document: &[u8]) -> Result<String, Error> {
    Ok(exact_json(&Value::Object(edax_cqrs::keyed_document(
        id,
        edax_cqrs::document_json(id, document)?,
    ))))
}

/// `value` as JSON text, with every non-integer number of magnitude 2^53 or more written as its exact
/// integer digits and a `.0`, at every depth. Every other number keeps `serde_json`'s shortest text.
pub fn exact_json(value: &Value) -> String {
    let mut out = Vec::new();
    // Writing a `Value` into memory fails on no input, and the writer emits only UTF-8.
    let written = value.serialize(&mut Serializer::with_formatter(&mut out, ExactFormatter));
    match written {
        Ok(()) => String::from_utf8(out).unwrap_or_default(),
        Err(_) => String::new(),
    }
}

/// A number's text, as [`exact_json`] writes it.
pub(crate) fn exact_number(n: &Number) -> String {
    exact_json(&Value::Number(n.clone()))
}

/// `serde_json`'s compact text, but a double of magnitude 2^53 or more, always a whole number, is
/// written as its exact digits with a `.0`, so a reader keeps reading it as a double. Rust's fixed
/// precision formatting prints a double's exact decimal value.
struct ExactFormatter;

impl Formatter for ExactFormatter {
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        if value.abs() >= EXACT_FROM {
            write!(writer, "{value:.0}.0")
        } else {
            CompactFormatter.write_f64(writer, value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_number_branches() {
        let cases: [(&str, Value, &str); 8] = [
            ("small_integer", json!(42), "42"),
            ("u64_max", json!(u64::MAX), "18446744073709551615"),
            ("small_double", json!(0.1), "0.1"),
            (
                "double_below_2_53",
                json!(9_007_199_254_740_991.0_f64),
                "9007199254740991.0",
            ),
            (
                "double_at_2_53",
                json!(9_007_199_254_740_992.0_f64),
                "9007199254740992.0",
            ),
            (
                "double_past_2_53",
                json!(5.808_820_077_685_227e16_f64),
                "58088200776852272.0",
            ),
            (
                "negative_past_2_64",
                json!(-1.8446744073709552e19_f64),
                "-18446744073709551616.0",
            ),
            ("double_1e23", json!(1e23_f64), "99999999999999991611392.0"),
        ];
        for (name, value, want) in cases {
            let Value::Number(n) = &value else { unreachable!() };
            assert_eq!(exact_number(n), want, "{name}");
        }
        assert_eq!(
            exact_json(&json!(f64::MAX)),
            "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878\
             171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075\
             868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026\
             184124858368.0",
            "the largest double, exact"
        );
    }

    /// Every double of magnitude 2^53 or more prints as whole digits and `.0` that read back as the same
    /// double; below 2^128 the digits are the integer's own, which `u128` prints exactly.
    #[test]
    fn exact_number_is_the_double_exactly() {
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let doubles = (0..50_000).map(|i: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            // A biased exponent from 1076 (2^53) to 2046 (the largest), either sign.
            let exponent = 1076 + seed % 971;
            f64::from_bits(((i & 1) << 63) | (exponent << 52) | (seed >> 12))
        });
        for double in doubles.chain([f64::MAX, -f64::MAX, EXACT_FROM, -EXACT_FROM]) {
            let text = exact_json(&json!(double));
            let digits = text.strip_suffix(".0").unwrap();
            let unsigned = digits.strip_prefix('-').unwrap_or(digits);
            assert!(unsigned.bytes().all(|b| b.is_ascii_digit()), "{text}");
            assert_eq!(digits.starts_with('-'), double < 0.0, "{text}");
            assert_eq!(text.parse::<f64>().unwrap(), double, "{text} reads back");
            if double.abs() < 2f64.powi(128) {
                assert_eq!(unsigned, (double.abs() as u128).to_string(), "{text}");
            }
        }
    }

    /// Below 2^53, and for everything but numbers, the text is `serde_json`'s own, byte for byte.
    #[test]
    fn exact_json_is_serde_json_text_below_2_53() {
        let value = json!({
            "b": [1, -1, 0.1, -0.0, 1e-7, 9_007_199_254_740_991.0_f64, u64::MAX, i64::MIN],
            "a": {"s": "é \u{1f980} \"q\" \\ \n \u{7f}", "t": true, "n": null, "e": [], "o": {}},
        });
        assert_eq!(exact_json(&value), value.to_string());
    }

    #[test]
    fn stored_document_branches() {
        assert_eq!(
            stored_document(
                "a-1",
                br#"{"n":5.808820077685227e16,"id":"x","m":[0.30000000000000000001]}"#
            ),
            Ok(r#"{"id":"a-1","m":[0.3],"n":58088200776852272.0}"#.to_owned())
        );
        assert!(stored_document("a-1", b"[1]").is_err(), "not an object");
    }
}
