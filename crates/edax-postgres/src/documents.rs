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

use edax_core::Error;
use serde_json::{Map, Number, Value};

/// The magnitude from which a double's shortest text may misorder against an integer.
const EXACT_FROM: f64 = 9_007_199_254_740_992.0; // 2^53

/// The text projection `id`'s `document` is stored as: the kernel's keyed document (the row key as its
/// `"id"`, refused as `codec` when it is not a JSON object or holds an integer wider than 64 bits),
/// re-serialized by [`exact_json`]. A string or object key holding NUL is a `codec` error: `jsonb`
/// cannot store it.
pub fn stored_document(id: &str, document: &[u8]) -> Result<String, Error> {
    let keyed = edax_cqrs::keyed_document_json(id, document)?;
    let value: Value = serde_json::from_str(&keyed).map_err(Error::codec)?;
    check_nul(&value)?;
    Ok(exact_json(&value))
}

/// `value` as JSON text, with every non-integer number of magnitude 2^53 or more written as its exact
/// integer digits and a `.0`, at every depth. Every other number keeps `serde_json`'s shortest text.
pub fn exact_json(value: &Value) -> String {
    let mut out = String::new();
    write_exact(value, &mut out);
    out
}

fn write_exact(value: &Value, out: &mut String) {
    match value {
        Value::Number(n) => out.push_str(&exact_number(n)),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_exact(item, out);
            }
            out.push(']');
        }
        Value::Object(fields) => write_object(fields, out),
        // Strings, bools and null have one text, `serde_json`'s.
        other => out.push_str(&other.to_string()),
    }
}

fn write_object(fields: &Map<String, Value>, out: &mut String) {
    out.push('{');
    for (i, (key, item)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&Value::String(key.clone()).to_string());
        out.push(':');
        write_exact(item, out);
    }
    out.push('}');
}

/// A number's text: exact digits for a double of magnitude 2^53 or more, else `serde_json`'s.
pub(crate) fn exact_number(n: &Number) -> String {
    match n.as_f64() {
        Some(double) if !n.is_i64() && !n.is_u64() && double.abs() >= EXACT_FROM => exact_digits(double),
        _ => n.to_string(),
    }
}

/// The exact decimal digits of a whole-number double (every double of magnitude 2^53 or more is one),
/// with a `.0` so a reader keeps reading it as a double.
fn exact_digits(double: f64) -> String {
    // A double is mantissa * 2^exponent with a 53-bit mantissa; a whole one has a non-negative exponent
    // once the mantissa's trailing bits are folded in. Long multiplication in base 10^9 keeps it exact
    // up to the largest double (about 1.8e308, 309 digits).
    let bits = double.to_bits();
    let negative = bits >> 63 == 1;
    let exponent_bits = (bits >> 52) & 0x7ff;
    let fraction = bits & ((1 << 52) - 1);
    let (mantissa, exponent) = if exponent_bits == 0 {
        (fraction, -1074_i64)
    } else {
        (fraction | (1 << 52), i64::from(exponent_bits as u16) - 1075)
    };
    let mut limbs: Vec<u64> = vec![mantissa % 1_000_000_000, mantissa / 1_000_000_000];
    for _ in 0..exponent.max(0) {
        let mut carry = 0u64;
        for limb in limbs.iter_mut() {
            let product = *limb * 2 + carry;
            *limb = product % 1_000_000_000;
            carry = product / 1_000_000_000;
        }
        if carry > 0 {
            limbs.push(carry);
        }
    }
    while limbs.len() > 1 && limbs.last() == Some(&0) {
        limbs.pop();
    }
    let mut digits = String::new();
    if negative {
        digits.push('-');
    }
    let mut rest = limbs.iter().rev();
    if let Some(top) = rest.next() {
        digits.push_str(&top.to_string());
    }
    for limb in rest {
        digits.push_str(&format!("{limb:09}"));
    }
    digits.push_str(".0");
    digits
}

/// Refuse a string or object key holding NUL, which `jsonb` cannot store, as `codec`.
fn check_nul(value: &Value) -> Result<(), Error> {
    let refuse = || Error::codec("a projection document holds a NUL character, which Postgres cannot store");
    match value {
        Value::String(s) if s.contains('\0') => Err(refuse()),
        Value::Array(items) => items.iter().try_for_each(check_nul),
        Value::Object(fields) => fields.iter().try_for_each(|(key, item)| {
            if key.contains('\0') {
                return Err(refuse());
            }
            check_nul(item)
        }),
        _ => Ok(()),
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
        assert_eq!(exact_json(&json!(f64::MAX)).len(), 311, "309 digits, `.0`");
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
        assert_eq!(
            stored_document("a-1", br#"{"s":"a\u0000b"}"#).map_err(|e| e.to_string()),
            Err("codec: a projection document holds a NUL character, which Postgres cannot store".to_owned())
        );
        assert!(stored_document("a-1", br#"{"a\u0000":1}"#).is_err(), "NUL in a key");
    }
}
