//! Decimal text: an exact decimal number carried as a JSON string.
//!
//! JSON numbers are doubles to most readers, so an amount with more than 15 significant digits, or
//! one like `0.1`, does not survive them exactly. A host therefore sends a decimal as a string, and
//! the kernel owns the two decisions every SDK must make the same way:
//!
//! - **The canonical text** ([`canonical_decimal`]), which a host writes into a document, a filter
//!   or a command, so `9.50` and `9.5` are one value to equality and to the command hash. It is plain
//!   notation: an optional `-`, the integer digits without leading zeros, and a fraction only when
//!   it is not zero, without trailing zeros. `9.50` is `9.5`, `1E+2` is `100`, `-0.0` is `0`, `.5` is
//!   `0.5` and `1.5E-7` is `0.00000015`.
//! - **The order** of two decimal texts by value, which a filter and a sort apply under the
//!   [`Collation::Decimal`](crate::filter::Collation) collation. Code point order would put `"10"`
//!   before `"9.5"`.
//!
//! Decimal text is an optional sign, digits with at most one `.` and at least one digit, and an
//! optional exponent: `e` or `E`, an optional sign, and digits within the 32-bit range. No
//! whitespace, no `_`, no `NaN` and no `Infinity`. It is at most [`MAX_DECIMAL_TEXT`] bytes long, its
//! digits without leading or trailing zeros number at most [`MAX_DECIMAL_DIGITS`], and so does its
//! plain notation, so a short text such as `1E+999999` cannot expand into a megabyte.
//! `testdata/golden/host_values.json` pins the form.
//!
//! `bigdecimal` does the arithmetic: the parse, the normal form and the exact order. This module adds
//! what it leaves open. Its parser is looser than this grammar (it reads `1_000`) and panics on an
//! exponent past `i64`, so the grammar is checked first. Its parse and normal form cost more than
//! linear time in the digits, and a filter parses its comparand once per document, so the grammar
//! also bounds the text and its digits before the parse. The plain notation's length is bounded on the
//! normal form, before anything is written out.

use bigdecimal::BigDecimal;
use edax_core::Error;
use std::cmp::Ordering;
use std::str::FromStr;

/// The most digits a decimal's plain notation may hold, counting both sides of the point and the
/// `0` before the point of a value below one.
pub const MAX_DECIMAL_DIGITS: usize = 1000;

/// The longest decimal text the kernel reads, in bytes: the longest plain notation with its sign and
/// point, and about as much again for padding zeros and an exponent.
pub const MAX_DECIMAL_TEXT: usize = 2 * MAX_DECIMAL_DIGITS;

/// Read decimal text into its normal form, or say why it is not decimal text.
fn parse(text: &str) -> Result<BigDecimal, Error> {
    check_grammar(text)?;
    let decimal = BigDecimal::from_str(text).map_err(|e| Error::codec(format_args!("{text:?} is not decimal text: {e}")))?;
    let decimal = decimal.normalized();
    // The digits before the point (one, the `0`, for a value below one) and after it. The grammar holds
    // the exponent to 32 bits and the digits to a few thousand, so neither sum overflows.
    let plain = decimal.order_of_magnitude().max(0) + 1 + decimal.fractional_digit_count().max(0);
    if !usize::try_from(plain).is_ok_and(|plain| plain <= MAX_DECIMAL_DIGITS) {
        return Err(Error::codec(format_args!(
            "{text:?} has more than {MAX_DECIMAL_DIGITS} digits in plain notation"
        )));
    }
    Ok(decimal)
}

/// Refuse what `BigDecimal::from_str` would read but the grammar does not admit, an exponent it could
/// not hold, and text too long to parse in bounded time.
fn check_grammar(text: &str) -> Result<(), Error> {
    if text.len() > MAX_DECIMAL_TEXT {
        return Err(Error::codec(format_args!(
            "decimal text of {} bytes is longer than {MAX_DECIMAL_TEXT}",
            text.len()
        )));
    }
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(text);
    let not_decimal = || Error::codec(format_args!("{text:?} is not decimal text"));
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if integer.len() + fraction.len() == 0 || !digits(integer) || !digits(fraction) {
        return Err(not_decimal());
    }
    if let Some(exponent) = exponent {
        let magnitude = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
        if magnitude.is_empty() || !digits(magnitude) {
            return Err(not_decimal());
        }
        if exponent.parse::<i32>().is_err() {
            return Err(Error::codec(format_args!("{text:?} has an exponent outside the 32-bit range")));
        }
    }
    // The coefficient the parse builds, but for the zeros that do not change the value.
    let significant = integer.trim_start_matches('0').len() + fraction.trim_end_matches('0').len();
    if significant > MAX_DECIMAL_DIGITS {
        return Err(Error::codec(format_args!("{text:?} has more than {MAX_DECIMAL_DIGITS} significant digits")));
    }
    Ok(())
}

/// The canonical text of decimal text (see the module docs), or the `codec` error that says why it is
/// not decimal text: not finite, not the grammar, or more than [`MAX_DECIMAL_DIGITS`] digits.
pub fn canonical_decimal(text: &str) -> Result<String, Error> {
    parse(text).map(|decimal| decimal.to_plain_string())
}

/// Whether `text` is decimal text the kernel reads.
pub(crate) fn is_decimal(text: &str) -> bool {
    parse(text).is_ok()
}

/// Two decimal texts by value, or `None` when either one is not decimal text.
pub(crate) fn decimal_cmp(a: &str, b: &str) -> Option<Ordering> {
    Some(parse(a).ok()?.cmp(&parse(b).ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One case per branch of the parse and the render: sign, special value, exponent, point,
    /// leading and trailing zeros, zero, the text, significant digit and plain digit bounds, and every
    /// grammar refusal.
    #[test]
    fn canonical_decimal_branches() {
        let at_bound = format!("1E+{}", MAX_DECIMAL_DIGITS - 1);
        let at_bound_plain = format!("1{}", "0".repeat(MAX_DECIMAL_DIGITS - 1));
        let past_bound = format!("1E+{MAX_DECIMAL_DIGITS}");
        let tiny_at_bound = format!("1E-{}", MAX_DECIMAL_DIGITS - 1);
        let tiny_at_bound_plain = format!("0.{}1", "0".repeat(MAX_DECIMAL_DIGITS - 2));
        let tiny_past_bound = format!("1E-{MAX_DECIMAL_DIGITS}");
        let padded_at_text_bound = format!("{}1.{}", "0".repeat(999), "0".repeat(MAX_DECIMAL_TEXT - 1001));
        let padded_past_text_bound = format!("{padded_at_text_bound}0");
        let significant_at_bound = format!("1{}E-{}", "0".repeat(MAX_DECIMAL_DIGITS - 1), MAX_DECIMAL_DIGITS - 1);
        let integer_zeros_past_bound = format!("1{}E-{MAX_DECIMAL_DIGITS}", "0".repeat(MAX_DECIMAL_DIGITS));
        let fraction_zeros_past_bound = format!("0.{}1E+{}", "0".repeat(MAX_DECIMAL_DIGITS), MAX_DECIMAL_DIGITS + 1);
        let cases: Vec<(&str, &str, Result<&str, &str>)> = vec![
            ("an_integer", "42", Ok("42")),
            ("a_fraction", "9.5", Ok("9.5")),
            ("trailing_fraction_zeros_go", "9.50", Ok("9.5")),
            ("a_zero_fraction_goes", "100.000", Ok("100")),
            ("leading_zeros_go", "007.25", Ok("7.25")),
            ("a_value_below_one_keeps_its_zero", "0.10", Ok("0.1")),
            ("a_bare_point_fraction", ".5", Ok("0.5")),
            ("a_bare_point_integer", "5.", Ok("5")),
            ("a_plus_sign_goes", "+3", Ok("3")),
            ("a_negative", "-12.30", Ok("-12.3")),
            ("negative_zero_is_zero", "-0.0", Ok("0")),
            ("zero_with_an_exponent", "0E-10", Ok("0")),
            ("zero_with_a_huge_exponent", "0E+999999999", Ok("0")),
            ("a_positive_exponent", "1E+2", Ok("100")),
            ("a_lowercase_exponent", "1.5e3", Ok("1500")),
            ("a_negative_exponent", "1.5E-7", Ok("0.00000015")),
            ("an_exponent_inside_the_digits", "12345E-2", Ok("123.45")),
            ("past_17_digits_stays_exact", "12345678901234567890.123456789", Ok("12345678901234567890.123456789")),
            ("at_the_digit_bound", &at_bound, Ok(&at_bound_plain)),
            ("a_fraction_at_the_digit_bound", &tiny_at_bound, Ok(&tiny_at_bound_plain)),
            ("past_the_digit_bound", &past_bound, Err("more than 1000 digits")),
            ("a_fraction_past_the_digit_bound", &tiny_past_bound, Err("more than 1000 digits")),
            ("zero_padding_at_the_text_bound", &padded_at_text_bound, Ok("1")),
            // Trailing zeros make the coefficient the parse builds long, and its normal form costs more
            // than linear time: the length is refused before the parse.
            ("zero_padding_past_the_text_bound", &padded_past_text_bound, Err("decimal text of 2001 bytes is longer than 2000")),
            ("significant_digits_at_the_bound", &significant_at_bound, Ok("1")),
            ("integer_zeros_past_the_significant_bound", &integer_zeros_past_bound, Err("more than 1000 significant digits")),
            ("fraction_zeros_past_the_significant_bound", &fraction_zeros_past_bound, Err("more than 1000 significant digits")),
            ("zero_at_the_widest_exponent", "0E+2147483647", Ok("0")),
            ("the_widest_exponent_meets_the_digit_bound", "1E-2147483648", Err("more than 1000 digits")),
            ("an_exponent_past_32_bits", "1E+2147483648", Err("outside the 32-bit range")),
            // `BigDecimal::from_str` panics on this one: the grammar must refuse it first.
            ("an_exponent_past_i64", "1E+9223372036854775808", Err("outside the 32-bit range")),
            ("a_signed_exponent_sign_alone", "1E-", Err("not decimal text")),
            ("nan", "NaN", Err("not decimal text")),
            ("signalling_nan", "sNaN", Err("not decimal text")),
            ("infinity", "-Infinity", Err("not decimal text")),
            ("inf", "inf", Err("not decimal text")),
            ("empty", "", Err("not decimal text")),
            ("a_sign_alone", "-", Err("not decimal text")),
            ("a_point_alone", ".", Err("not decimal text")),
            ("two_points", "1.2.3", Err("not decimal text")),
            ("whitespace", " 1", Err("not decimal text")),
            ("an_underscore", "1_000", Err("not decimal text")),
            ("an_exponent_without_digits", "1E", Err("not decimal text")),
            ("an_exponent_with_a_sign_alone", "1E+", Err("not decimal text")),
            ("an_exponent_without_a_mantissa", "E5", Err("not decimal text")),
            ("a_fraction_in_the_exponent", "1E1.5", Err("not decimal text")),
            ("a_word", "ten", Err("not decimal text")),
            ("non_ascii_digits", "١٢", Err("not decimal text")),
        ];
        for (name, text, want) in cases {
            match (canonical_decimal(text), want) {
                (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
                (Err(error), Err(needle)) => {
                    assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
                    assert!(error.message.contains(needle), "{name}: {error}");
                }
                (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[test]
    fn decimal_cmp_orders_by_value() {
        let cases: [(&str, &str, &str, Option<Ordering>); 13] = [
            ("more_digits_is_not_larger", "9.5", "10", Some(Ordering::Less)),
            ("trailing_zeros_are_equal", "9.50", "9.5", Some(Ordering::Equal)),
            ("spellings_of_one_value", "1E+2", "100.0", Some(Ordering::Equal)),
            ("an_extended_coefficient_is_larger", "1.5", "1.51", Some(Ordering::Less)),
            ("same_digits_other_magnitude", "0.15", "1.5", Some(Ordering::Less)),
            ("negatives_reverse", "-10", "-9.5", Some(Ordering::Less)),
            ("negative_below_positive", "-1", "0.001", Some(Ordering::Less)),
            ("positive_above_negative", "1", "-1000", Some(Ordering::Greater)),
            ("zero_against_a_positive", "0", "0.001", Some(Ordering::Less)),
            ("zero_against_a_negative", "-0", "-0.001", Some(Ordering::Greater)),
            ("past_17_digits_stays_exact", "12345678901234567890.1", "12345678901234567890.01", Some(Ordering::Greater)),
            ("a_left_side_that_is_not_decimal", "abc", "1", None),
            ("a_right_side_that_is_not_decimal", "1", "NaN", None),
        ];
        for (name, a, b, want) in cases {
            assert_eq!(decimal_cmp(a, b), want, "{name}");
            assert_eq!(decimal_cmp(b, a), want.map(Ordering::reverse), "{name} reversed");
        }
    }
}
