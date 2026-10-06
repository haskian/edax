//! Timestamp text: the one form an instant travels as in a document, a filter or a command.
//!
//! The filter and the sort compare strings by code point, which is chronological for one form only:
//! UTC, a `Z` suffix and a fixed fraction width. [`canonical_timestamp`] writes every instant in it,
//! `YYYY-MM-DDTHH:MM:SS.ffffffZ`, so string order is instant order and one instant is one string to
//! equality and to the command hash. `2024-01-01T01:30:00+01:00` becomes
//! `2024-01-01T00:30:00.000000Z`.
//!
//! The input is RFC 3339 text with an offset: `Z` or `±HH:MM`. `jiff` parses it and converts it to
//! UTC; it also reads the other ISO 8601 spellings of an instant (a space for the `T`, a lowercase
//! `z`, `+HH:MM:SS`). This module adds the refusals, each a `codec` error:
//!
//! - text with no offset, whose instant is unknown: guessing UTC would misplace it silently;
//! - a fraction finer than a microsecond, which the form cannot hold: truncating it would make two
//!   instants one string;
//! - a UTC year below `0000`, which four digits cannot hold (`jiff` refuses one past 9999 itself).
//!
//! `jiff` then prints the instant at six fraction digits: four-digit year, `T`, and `Z`.
//! `testdata/golden/host_values.json` pins the form.

use edax_core::Error;
use jiff::tz::Offset;
use jiff::Timestamp;

/// The fixed-width UTC text of an RFC 3339 timestamp (see the module docs), or the `codec` error that
/// says why it has none.
pub fn canonical_timestamp(text: &str) -> Result<String, Error> {
    let instant: Timestamp = text.parse().map_err(|e| {
        Error::codec(format_args!(
            "{text:?} is not an RFC 3339 timestamp with an offset: {e}"
        ))
    })?;
    if instant.subsec_nanosecond() % 1_000 != 0 {
        return Err(Error::codec(format_args!("{text:?} is finer than a microsecond")));
    }
    // jiff prints a year below 0000 with a `-00` prefix, which breaks the fixed width.
    if Offset::UTC.to_datetime(instant).year() < 0 {
        return Err(Error::codec(format_args!("{text:?} falls before the UTC year 0000")));
    }
    Ok(format!("{instant:.6}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One case per branch: each offset form, the fraction widths, the year bounds, and every refusal.
    #[test]
    fn canonical_timestamp_branches() {
        let cases: [(&str, &str, Result<&str, &str>); 16] = [
            (
                "utc_z_gets_a_zero_fraction",
                "2024-01-01T00:00:00Z",
                Ok("2024-01-01T00:00:00.000000Z"),
            ),
            (
                "a_zero_offset",
                "2024-01-01T00:00:00+00:00",
                Ok("2024-01-01T00:00:00.000000Z"),
            ),
            (
                "a_positive_offset_converts",
                "2024-01-01T01:30:00+01:00",
                Ok("2024-01-01T00:30:00.000000Z"),
            ),
            (
                "a_negative_offset_crosses_midnight",
                "2023-12-31T22:00:00-05:00",
                Ok("2024-01-01T03:00:00.000000Z"),
            ),
            (
                "an_offset_with_seconds",
                "2024-01-01T00:00:00+01:00:30",
                Ok("2023-12-31T22:59:30.000000Z"),
            ),
            (
                "milliseconds_widen",
                "2024-01-01T00:00:00.123Z",
                Ok("2024-01-01T00:00:00.123000Z"),
            ),
            (
                "microseconds_stay",
                "2024-01-01T00:00:00.000001Z",
                Ok("2024-01-01T00:00:00.000001Z"),
            ),
            (
                "nanoseconds_in_whole_microseconds",
                "2024-01-01T00:00:00.000001000Z",
                Ok("2024-01-01T00:00:00.000001Z"),
            ),
            (
                "a_space_for_the_t",
                "2024-01-01 00:00:00z",
                Ok("2024-01-01T00:00:00.000000Z"),
            ),
            (
                "a_year_below_1000_keeps_four_digits",
                "0999-12-31T23:59:59.999999Z",
                Ok("0999-12-31T23:59:59.999999Z"),
            ),
            (
                "year_zero_is_four_digits",
                "0001-01-01T00:30:00+01:00",
                Ok("0000-12-31T23:30:00.000000Z"),
            ),
            (
                "no_offset",
                "2024-01-01T00:00:00",
                Err("not an RFC 3339 timestamp with an offset"),
            ),
            (
                "not_a_timestamp",
                "yesterday",
                Err("not an RFC 3339 timestamp with an offset"),
            ),
            (
                "finer_than_a_microsecond",
                "2024-01-01T00:00:00.0000001Z",
                Err("finer than a microsecond"),
            ),
            (
                "a_negative_year",
                "0000-01-01T00:00:00+01:00",
                Err("before the UTC year 0000"),
            ),
            // Past jiff's own range, which ends in the last days of 9999.
            (
                "past_year_9999",
                "9999-12-31T23:30:00-01:00",
                Err("not an RFC 3339 timestamp with an offset"),
            ),
        ];
        for (name, text, want) in cases {
            match (canonical_timestamp(text), want) {
                (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
                (Err(error), Err(needle)) => {
                    assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
                    assert!(error.message.contains(needle), "{name}: {error}");
                }
                (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }
}
