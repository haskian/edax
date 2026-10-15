//! The host-value types: a decimal and an instant that hold the kernel's canonical text.
//!
//! A host declares a field as [`Decimal`] or [`Instant`] instead of a `String`, a float or a third-party
//! type, and the field then serializes as the one text every SDK writes for it: in a command's hash, a
//! document, a filter and an event payload. `9.50` and `9.5` are one [`Decimal`], and
//! `2024-01-01T01:30:00+01:00` and `2024-01-01T00:30:00Z` are one [`Instant`], so a Rust service and a
//! Python service (whose encoder normalizes `Decimal` and `datetime` the same way) hash one command to one
//! number. Both types canonicalize when they are built, by [`FromStr`] or by `Deserialize`, so a value
//! that exists is always canonical. Each failure is the kernel's `codec` error. Pinned by
//! `testdata/golden/host_values.json` and the host-value rows of `testdata/golden/command_hash.json`.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use edax_core::Error;
use serde::{Deserialize, Serialize, Serializer};

use crate::decimal_text::parse;
use crate::timestamp::{canonical_timestamp, timestamp_from_unix_nanos};

/// An exact decimal number in its canonical text (see [`canonical_decimal`](crate::canonical_decimal)),
/// written as a JSON string. Two decimals are equal when their values are, and they order by value, not
/// by text.
// The value is kept beside the text, read once when the decimal is built, so an order (a sort, a
// `BTreeMap` key) parses nothing. Equal values have one canonical text, so deriving `Eq` and `Hash` over
// both fields agrees with comparing either one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct Decimal {
    text: String,
    value: BigDecimal,
}

/// An instant in the kernel's fixed-width UTC text (see [`canonical_timestamp`]), written as a JSON
/// string. The text has one width, so text order is instant order.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct Instant(String);

impl Decimal {
    /// The canonical text, as it travels.
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

impl Instant {
    /// The fixed-width UTC text, as it travels.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The instant `nanos` after the Unix epoch, floored to the microsecond: a stored event's
    /// `recorded_at` or `occurred_at`, or a clock reading. See [`timestamp_from_unix_nanos`].
    pub fn from_unix_nanos(nanos: i64) -> Result<Instant, Error> {
        timestamp_from_unix_nanos(nanos).map(Instant)
    }
}

impl FromStr for Decimal {
    type Err = Error;

    /// Decimal text in any spelling the kernel reads (`9.50`, `1E+2`, `.5`). Not decimal text, `NaN`, an
    /// infinity, or more than [`MAX_DECIMAL_DIGITS`](crate::MAX_DECIMAL_DIGITS) digits is a `codec` error.
    fn from_str(text: &str) -> Result<Self, Error> {
        let value = parse(text)?;
        Ok(Decimal {
            text: value.to_plain_string(),
            value,
        })
    }
}

impl FromStr for Instant {
    type Err = Error;

    /// RFC 3339 text with an offset. Text with no offset, a fraction finer than a microsecond, or an
    /// instant outside the four-digit UTC years is a `codec` error.
    fn from_str(text: &str) -> Result<Self, Error> {
        canonical_timestamp(text).map(Instant)
    }
}

/// What `Deserialize` reads a decimal from: any decimal text, kept in its canonical form, as [`FromStr`]
/// does.
impl TryFrom<String> for Decimal {
    type Error = Error;

    fn try_from(text: String) -> Result<Self, Error> {
        text.parse()
    }
}

/// What `Deserialize` reads an instant from: any RFC 3339 text with an offset, kept in its fixed-width UTC
/// form, as [`FromStr`] does.
impl TryFrom<String> for Instant {
    type Error = Error;

    fn try_from(text: String) -> Result<Self, Error> {
        text.parse()
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl fmt::Display for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The canonical text alone, as a JSON string: the value beside it is a reading of the text.
impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_core::ErrorCode;

    /// One case per branch of each constructor: canonical text kept, another spelling canonicalized, and
    /// each refusal the kernel's text functions make.
    #[test]
    fn parse_branches() {
        let decimals: [(&str, &str, Result<&str, &str>); 5] = [
            ("canonical_text_is_kept", "9.5", Ok("9.5")),
            ("trailing_zeros_go", "9.50", Ok("9.5")),
            ("an_exponent_is_written_plain", "0.95E+1", Ok("9.5")),
            ("nan_is_refused", "NaN", Err("is not decimal text")),
            (
                "past_the_digit_bound_is_refused",
                "1E+1000",
                Err("more than 1000 digits"),
            ),
        ];
        for (name, text, want) in decimals {
            let got = text.parse::<Decimal>().map(|d| d.to_string());
            check(name, got, want);
        }
        let instants: [(&str, &str, Result<&str, &str>); 4] = [
            (
                "canonical_text_is_kept",
                "2024-01-01T00:30:00.000000Z",
                Ok("2024-01-01T00:30:00.000000Z"),
            ),
            (
                "an_offset_converts_to_utc",
                "2024-01-01T01:30:00+01:00",
                Ok("2024-01-01T00:30:00.000000Z"),
            ),
            (
                "no_offset_is_refused",
                "2024-01-01T00:30:00",
                Err("not an RFC 3339 timestamp with an offset"),
            ),
            (
                "finer_than_a_microsecond_is_refused",
                "2024-01-01T00:30:00.0000001Z",
                Err("finer than a microsecond"),
            ),
        ];
        for (name, text, want) in instants {
            let got = text.parse::<Instant>().map(|i| i.to_string());
            check(name, got, want);
        }
    }

    fn check(name: &str, got: Result<String, Error>, want: Result<&str, &str>) {
        match (got, want) {
            (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(error), Err(needle)) => {
                assert_eq!(error.code, ErrorCode::Codec, "{name}");
                assert!(error.message.contains(needle), "{name}: {error}");
            }
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }

    /// The serde form is the canonical text in both directions: a field written in another spelling reads
    /// canonical, and refused text is the format's error carrying the kernel's message.
    #[test]
    fn serde_reads_any_spelling_and_writes_the_canonical_text() {
        #[derive(Debug, Serialize, Deserialize)]
        struct Withdraw {
            amount: Decimal,
            at: Instant,
        }
        let read: Withdraw = serde_json::from_str(r#"{"amount":"9.50","at":"2024-01-01T01:30:00+01:00"}"#).unwrap();
        assert_eq!(
            serde_json::to_string(&read).unwrap(),
            r#"{"amount":"9.5","at":"2024-01-01T00:30:00.000000Z"}"#
        );
        let naive = serde_json::from_str::<Withdraw>(r#"{"amount":"9.5","at":"2024-01-01T00:30:00"}"#).unwrap_err();
        assert!(
            naive
                .to_string()
                .starts_with(r#"codec: "2024-01-01T00:30:00" is not an RFC 3339 timestamp"#),
            "{naive}"
        );
        let number = serde_json::from_str::<Withdraw>(r#"{"amount":9.5,"at":"2024-01-01T00:30:00Z"}"#).unwrap_err();
        assert!(
            number.to_string().starts_with("invalid type: floating point"),
            "a JSON number is no decimal text: {number}"
        );
    }

    /// Decimals order by value, where text order would put `"10"` before `"9.5"`; instants order by their
    /// fixed-width text, which is instant order.
    #[test]
    fn order_is_value_order() {
        let decimal = |text: &str| text.parse::<Decimal>().unwrap();
        let mut decimals = [decimal("10"), decimal("-1"), decimal("9.5"), decimal("0.25")];
        decimals.sort();
        assert_eq!(decimals.map(|d| d.to_string()), ["-1", "0.25", "9.5", "10"]);
        assert_eq!(decimal("9.50"), decimal("9.5"), "one value, one canonical text");
        let instant = |text: &str| text.parse::<Instant>().unwrap();
        assert!(
            instant("2024-01-01T00:30:00+01:00") < instant("2024-01-01T00:00:00Z"),
            "23:30 the day before"
        );
    }
}
