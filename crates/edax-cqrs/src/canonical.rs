//! The canonical JSON form: the bytes an idempotency hash and a keyset cursor are taken over.
//!
//! The form is the JSON Canonicalization Scheme, RFC 8785 (JCS), written by
//! `serde_json_canonicalizer`. Any SDK can reproduce it with an off-the-shelf JCS library:
//!
//! - **Objects** sort their keys by the UTF-16 code units of the key. A repeated key keeps its last
//!   value. No whitespace anywhere.
//! - **Numbers** are IEEE 754 doubles written in the ECMAScript form, so `2`, `2.0`, `2e0` and
//!   `20e-1` are one number (`2`), `-0` is `0`, `1e20` is `100000000000000000000` and `1e21` is
//!   `1e+21`.
//! - **Strings** escape `"`, `\` and the control characters below U+0020 (`\b \f \n \r \t` by name,
//!   the rest as lowercase `\u00xx`), and write everything else as raw UTF-8. An input escape is
//!   decoded first, so `"\u00e9"` and `"é"` are one string. A lone surrogate is a `codec` error.
//!
//! One rule is the kernel's own. A double rounds an integer past 2^53, so JCS would write
//! `9007199254740993` as `9007199254740992`: two ids one apart would hash alike. And 2^53 itself is
//! ambiguous, since `2^53 + 1` reads back as it. So an integer literal outside the I-JSON safe range
//! (RFC 7493), `-(2^53 - 1)..=2^53 - 1`, is a `codec` error, never a silent rounding. That is
//! JavaScript's `Number.MAX_SAFE_INTEGER` and the bound off-the-shelf JCS libraries enforce, so
//! every number the kernel accepts, they accept too. The check reads the input text before
//! `serde_json` parses it, so no feature of any dependency (`arbitrary_precision` keeps number text,
//! `preserve_order` keeps key order) can hide a literal or move the bytes. A number too large for a
//! double (`1e400`) is a `codec` error too. `testdata/golden/canonical_json.json` pins the form.

use edax_core::Error;
use serde_json::Value;

use crate::json_numbers::{check_integers, Width};

/// Re-emit `json` in the canonical form (RFC 8785). Every failure is a `codec` error.
pub fn canonical_json(json: &[u8]) -> Result<Vec<u8>, Error> {
    check_integers(json, Width::Double)?;
    let value: Value = serde_json::from_slice(json).map_err(Error::codec)?;
    serde_json_canonicalizer::to_vec(&value).map_err(Error::codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical(input: &str) -> Result<String, String> {
        canonical_json(input.as_bytes())
            .map(|b| String::from_utf8(b).unwrap())
            .map_err(|e| e.to_string())
    }

    /// One row per branch of the number check and per rule of the form. The golden vector repeats the
    /// rows a second SDK must agree on; this table also covers every refusal.
    #[test]
    fn canonical_branches() {
        let cases: &[(&str, &str, Result<&str, &str>)] = &[
            // --- objects ---
            ("keys_sorted_whitespace_dropped", r#"{ "b": 2, "a": 1 }"#, Ok(r#"{"a":1,"b":2}"#)),
            ("nested_keys_sorted", r#"{"o":{"z":1,"a":{"y":2,"b":3}}}"#, Ok(r#"{"o":{"a":{"b":3,"y":2},"z":1}}"#)),
            // UTF-16 order: U+1F980 (a surrogate pair, 0xD83E...) sorts before U+FF71; UTF-8 bytes would not.
            ("keys_sort_by_utf16_code_units", r#"{"ｱ":1,"🦀":2,"é":3,"Z":4,"a":5}"#, Ok(r#"{"Z":4,"a":5,"é":3,"🦀":2,"ｱ":1}"#)),
            ("repeated_key_keeps_the_last_value", r#"{"a":1,"a":2}"#, Ok(r#"{"a":2}"#)),
            ("empty_object", " { } ", Ok("{}")),
            ("missing_colon", r#"{"a" 1}"#, Err("codec: expected `:` at line 1 column 6")),
            // --- arrays ---
            ("array_order_is_data", "[3, 1, 2]", Ok("[3,1,2]")),
            // --- numbers: the form ---
            ("int_and_whole_float_are_one_number", r#"[2,2.0,2e0,20e-1,2E+0]"#, Ok("[2,2,2,2,2]")),
            ("negative_zero_is_zero", "[-0,-0.0,0e5]", Ok("[0,0,0]")),
            ("fraction_is_shortest_round_trip", "[1.10,0.1,1.5e-7,0.000001,123456.789]", Ok("[1.1,0.1,1.5e-7,0.000001,123456.789]")),
            ("whole_float_below_1e21_is_written_out", "[1e20,-1e20]", Ok("[100000000000000000000,-100000000000000000000]")),
            ("float_from_1e21_takes_an_exponent", "[1e21,1.5e300]", Ok("[1e+21,1.5e+300]")),
            // A fraction or an exponent makes a float, and a float past 2^53 rounds as a float does.
            ("float_past_2_pow_53_rounds", "9007199254740993.0", Ok("9007199254740992")),
            // --- numbers: the safe-integer boundary, 2^53 - 1 ---
            ("safe_integers_stay_exact", "[9007199254740991,-9007199254740991]", Ok("[9007199254740991,-9007199254740991]")),
            // 2^53 + 1 reads back as 2^53, so 2^53 itself is ambiguous: I-JSON calls it unsafe.
            ("integer_at_2_pow_53", "9007199254740992", Err("codec: integer at byte 0 is outside -(2^53-1)..=2^53-1, the I-JSON safe integers RFC 8785 keeps exact")),
            ("negative_integer_at_2_pow_53", "[-9007199254740992]", Err("codec: integer at byte 1 is outside -(2^53-1)..=2^53-1, the I-JSON safe integers RFC 8785 keeps exact")),
            ("integer_past_i128", "1000000000000000000000000000000000000000000", Err("codec: integer at byte 0 is outside -(2^53-1)..=2^53-1, the I-JSON safe integers RFC 8785 keeps exact")),
            ("nested_wide_integer", r#"{"a":[{"b":18446744073709551615}]}"#, Err("codec: integer at byte 11 is outside -(2^53-1)..=2^53-1, the I-JSON safe integers RFC 8785 keeps exact")),
            ("out_of_range", "[1e400]", Err("codec: number at byte 1 is out of range")),
            ("negative_out_of_range", "[-1e400]", Err("codec: number at byte 1 is out of range")),
            // A token the lexer does not read as a number is left to the grammar check.
            ("bare_minus", "-", Err("codec: EOF while parsing a value at line 1 column 1")),
            ("sign_inside_a_token", "[1-2]", Err("codec: expected `,` or `]` at line 1 column 3")),
            ("dot_without_digits", "1.", Err("codec: EOF while parsing a value at line 1 column 2")),
            // --- strings ---
            ("digits_in_a_string_are_text", r#"{"id":"18446744073709551616","q":"\"9007199254740993"}"#, Ok(r#"{"id":"18446744073709551616","q":"\"9007199254740993"}"#)),
            ("escapes_decoded_then_minimal", r#""\u00e9\/\"\\""#, Ok(r#""é/\"\\""#)),
            ("surrogate_pair_is_one_code_point", r#""\ud83e\udd80""#, Ok("\"🦀\"")),
            ("control_characters_escaped", "\"\\b\\f\\n\\r\\t\\u0001\\u001f\"", Ok("\"\\b\\f\\n\\r\\t\\u0001\\u001f\"")),
            ("del_and_non_ascii_raw", "\"\u{7f}café\u{2028}\"", Ok("\"\u{7f}café\u{2028}\"")),
            ("lone_high_surrogate", r#""\ud800""#, Err("codec: unexpected end of hex escape at line 1 column 8")),
            ("unterminated_string", r#""abc"#, Err("codec: EOF while parsing a string at line 1 column 4")),
            // --- literals and the document ---
            ("literals", "[true,false,null]", Ok("[true,false,null]")),
            ("empty_input", "", Err("codec: EOF while parsing a value at line 1 column 0")),
            ("trailing_characters", "{} 1", Err("codec: trailing characters at line 1 column 4")),
        ];
        for (name, input, want) in cases {
            assert_eq!(
                canonical(input).as_deref(),
                want.map_err(ToString::to_string).as_deref(),
                "{name}"
            );
        }
    }

    #[test]
    fn invalid_utf8_is_a_codec_error() {
        let err = canonical_json(&[b'"', 0xff, b'"']).expect_err("not UTF-8");
        assert_eq!(err.code, edax_core::ErrorCode::Codec);
    }

    #[test]
    fn nesting_is_bounded() {
        let ok = format!("{}{}", "[".repeat(127), "]".repeat(127));
        assert_eq!(canonical(&ok), Ok(ok.clone()));
        let deep = format!("{}{}", "[".repeat(129), "]".repeat(129));
        assert!(
            canonical(&deep).is_err_and(|e| e.starts_with("codec: recursion limit exceeded")),
            "{deep}"
        );
    }

    #[test]
    fn the_form_is_a_fixed_point_inside_the_safe_integers() {
        // Canonicalizing canonical bytes changes nothing, so a store may re-canonicalize safely...
        for input in [
            r#"{"b":[1.5,2.0,{"y":null,"x":"\u00e9"}],"a":-0}"#,
            "1e21",
            "9007199254740991",
            r#""\ud83e\udd80""#,
        ] {
            let once = canonical(input).unwrap();
            assert_eq!(canonical(&once), Ok(once.clone()), "{input}");
        }
        // ...except a whole float past 2^53 below 1e21: ECMAScript writes it out as an integer, and
        // an integer literal that wide is refused on the way back in.
        let once = canonical("1e20").unwrap();
        assert_eq!(once, "100000000000000000000");
        assert!(canonical(&once).is_err_and(|e| e.contains("outside -(2^53-1)..=2^53-1")));
    }
}
