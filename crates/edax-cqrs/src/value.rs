//! The document value model the filter and the query share: how a path resolves, how two values
//! compare, which spellings a spec or a document may use. Crate-private: the filter and the query are
//! the contracts, and this is the one place both read values through.

use edax_core::Error;
use serde_json::{Number, Value};
use std::cmp::Ordering;

use crate::json_numbers::{check_integers, Width};

/// Resolve a dot-delimited path into a document. Intermediate segments must be objects; anything
/// else (or a missing key) resolves to `None`.
pub(crate) fn resolve<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => current = map.get(segment)?,
            _ => return None,
        }
    }
    Some(current)
}

/// Every JSON integer as `i128`, which holds both the `i64` and the `u64` range exactly.
fn as_int(n: &Number) -> Option<i128> {
    n.as_i64().map(i128::from).or_else(|| n.as_u64().map(i128::from))
}

/// Compare an integer against a float exactly, without promoting the integer to `f64`.
///
/// Splitting the float at its floor keeps every comparison in integer space: equal integer parts are
/// then decided by whether the float carries a fraction. JSON has no NaN or infinity, so a finite
/// float is the only case.
fn int_vs_float(int: i128, float: f64) -> Ordering {
    let floor = float.floor();
    // A float past the i128 range saturates on cast, which would tie against the bound instead of
    // ordering. The bounds are asymmetric because `i128::MIN` is exactly representable as an `f64`
    // and `i128::MAX` is not: `i128::MAX as f64` rounds up to 2^127, already past every `i128`,
    // whereas a floor of exactly `i128::MIN` is a legitimate comparand and must fall through.
    if floor >= i128::MAX as f64 {
        return Ordering::Less;
    }
    if floor < i128::MIN as f64 {
        return Ordering::Greater;
    }
    match int.cmp(&(floor as i128)) {
        // Same integer part: a fraction makes the float the larger of the two.
        Ordering::Equal if float > floor => Ordering::Less,
        other => other,
    }
}

/// Compare two JSON numbers exactly.
///
/// `f64` loses precision above 2^53, so promoting both sides would make two distinct ids (snowflakes,
/// sequences) compare equal — a filter matching the wrong row, keyset paging dropping rows. Worse, a
/// mixed integer/float promotion is not even *transitive* (`2^53+1 == 2^53.0` and `2^53.0 == 2^53`,
/// yet `2^53+1 > 2^53`), and an intransitive comparator is allowed to panic `slice::sort_by`. So
/// integers compare as integers and a mixed pair compares without promoting the integer.
fn number_cmp(a: &Number, b: &Number) -> Option<Ordering> {
    match (as_int(a), as_int(b)) {
        (Some(x), Some(y)) => Some(x.cmp(&y)),
        (Some(x), None) => Some(int_vs_float(x, b.as_f64()?)),
        (None, Some(y)) => Some(int_vs_float(y, a.as_f64()?).reverse()),
        (None, None) => a.as_f64()?.partial_cmp(&b.as_f64()?),
    }
}

/// Equality with numbers compared by value at every depth, so a number means the same inside an
/// array or an object as it does at the top level.
pub(crate) fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_cmp(x, y) == Some(Ordering::Equal),
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| value_eq(x, y)),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(key, x)| y.get(key).is_some_and(|y| value_eq(x, y)))
        }
        _ => a == b,
    }
}

/// Order two values within one type: numbers numerically, strings by Unicode code point, bools
/// `false < true`. Any other pairing is unordered (`None`). The sort's total order
/// ([`crate::collation::total_cmp`]) ranks the types by [`type_rank`] and each within its rank by
/// this, so a sort and a range filter rank every orderable pair the same way.
pub(crate) fn value_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_cmp(x, y),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// The JSON types in the total order a sort gives them, each by the name JSON gives it: number < string <
/// bool < array < object < null. A store that sorts in its own engine ranks types by this list.
pub const TYPE_ORDER: [&str; 6] = ["number", "string", "boolean", "array", "object", "null"];

/// A value's place in [`TYPE_ORDER`].
pub fn type_rank(v: &Value) -> usize {
    let name = match v {
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Bool(_) => "boolean",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Null => "null",
    };
    TYPE_ORDER.iter().position(|t| *t == name).unwrap_or(TYPE_ORDER.len())
}

/// A path is one or more dot-delimited object keys, none of them empty.
pub(crate) fn check_path(path: &str) -> Result<(), Error> {
    if path.split('.').any(str::is_empty) {
        return Err(Error::codec(format_args!("path {path:?} has an empty segment")));
    }
    Ok(())
}

/// Parse `json` as `T` under the strict reading every filter and query spec, and every document a
/// boundary reads, gets: integer literals within 64 bits ([`check_document`]), unknown fields refused by
/// `T`'s own attributes. [`read_json`] is the same reading with the failure naming what was read.
pub(crate) fn parse_strict<T: serde::de::DeserializeOwned>(json: &[u8]) -> Result<T, Error> {
    check_document(json)?;
    serde_json::from_slice(json).map_err(Error::codec)
}

/// Read `json` as `T` under the strict reading every stored document gets: an integer literal wider than
/// 64 bits is refused by the rule every stored document gets, and text that is not a `T` as `"{what} is not JSON:
/// …"`. Both are `codec` errors. For a boundary that holds a document as text, such as a saga's state.
pub fn read_json<T: serde::de::DeserializeOwned>(what: &str, json: &[u8]) -> Result<T, Error> {
    check_document(json)?;
    serde_json::from_slice(json).map_err(|e| Error::codec(format_args!("{what} is not JSON: {e}")))
}

/// Refuse a document the filter and the sort could not compare exactly: one holding an integer
/// literal wider than 64 bits, which `serde_json` reads as the nearest float, so it would come back
/// rounded and match or sort as its neighbour. The query spec refuses the same literals. A `codec`
/// error. Every boundary that reads a document from text runs it, a projection store's save among
/// them, so such a document is refused where it is written rather than read back changed.
pub(crate) fn check_document(json: &[u8]) -> Result<(), Error> {
    check_integers(json, Width::Bits64)
}

/// Whether a string or an object key anywhere in `value` holds NUL. Postgres `jsonb` cannot store one,
/// so every store refuses such a value alike, as `codec`, rather than only the store that fails.
pub fn holds_nul(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains('\0'),
        Value::Array(items) => items.iter().any(holds_nul),
        Value::Object(fields) => fields.iter().any(|(key, item)| key.contains('\0') || holds_nul(item)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn read_json_branches() {
        for (name, json, want) in [
            (
                "a_document",
                r#"{"status":"shipped","total":18446744073709551615}"#,
                Ok(json!({"status":"shipped","total":u64::MAX})),
            ),
            (
                "past_64_bits",
                r#"{"status":"shipped","total":18446744073709551616}"#,
                Err("codec: integer at byte 28 is outside the 64-bit range"),
            ),
            (
                "not_json",
                r#"{"status":"#,
                Err("codec: saga state is not JSON: EOF while parsing a value at line 1 column 10"),
            ),
        ] {
            let got = read_json::<Value>("saga state", json.as_bytes()).map_err(|e| e.to_string());
            assert_eq!(got, want.map_err(str::to_owned), "{name}");
        }
    }

    #[test]
    fn number_cmp_is_exact_and_transitive() {
        // 2^53 is the last integer f64 represents exactly, so it is where a promoting comparator
        // starts lying. The three 2^53 rows are also the transitivity triangle `slice::sort_by` may
        // panic over: promoting the integer made big == as_float and as_float == at_53 while
        // big > at_53. Each row is (label, left, right, expected).
        let big = json!(9_007_199_254_740_993u64); // 2^53 + 1
        let at_53 = json!(9_007_199_254_740_992u64); // 2^53
        let as_float = json!(9_007_199_254_740_992.0f64); // 2^53, but stored as f64
        let cases: Vec<(&str, Value, Value, Ordering)> = vec![
            ("int_equals_its_own_float_form", json!(1), json!(1.0), Ordering::Equal),
            (
                "int_above_2_53_beats_its_float_neighbour",
                big.clone(),
                as_float.clone(),
                Ordering::Greater,
            ),
            ("float_at_2_53_equals_the_int", as_float, at_53.clone(), Ordering::Equal),
            ("ints_above_2_53_stay_distinct", big, at_53, Ordering::Greater),
            (
                "fraction_breaks_an_equal_integer_part",
                json!(1),
                json!(1.5),
                Ordering::Less,
            ),
            (
                "negative_fraction_is_below_its_floor",
                json!(-1),
                json!(-1.5),
                Ordering::Greater,
            ),
            ("float_beyond_i128_is_greater", json!(0), json!(1e300), Ordering::Less),
            ("float_below_i128_is_less", json!(0), json!(-1e300), Ordering::Greater),
            ("u64_above_i64_max", json!(u64::MAX), json!(i64::MAX), Ordering::Greater),
            ("two_floats_order_by_value", json!(1.5), json!(2.5), Ordering::Less),
            ("signed_zeroes_are_one_value", json!(-0.0), json!(0.0), Ordering::Equal),
        ];
        for (label, left, right, want) in &cases {
            assert_eq!(value_cmp(left, right), Some(*want), "{label}");
            assert_eq!(value_cmp(right, left), Some(want.reverse()), "{label} reversed");
        }
    }

    #[test]
    fn int_vs_float_stays_exact_at_the_i128_bounds() {
        // Unreachable through a JSON number (which is at most u64-wide) but the helper is declared
        // over all of i128: `i128::MIN as f64` is exact, so that floor must compare, not saturate.
        let min_as_float = i128::MIN as f64;
        assert_eq!(int_vs_float(i128::MIN, min_as_float), Ordering::Equal);
        assert_eq!(int_vs_float(i128::MIN + 1, min_as_float), Ordering::Greater);
        assert_eq!(int_vs_float(i128::MIN, min_as_float * 2.0), Ordering::Greater);
        // `i128::MAX as f64` rounds up to 2^127, past every i128, so the upper bound is exclusive.
        assert_eq!(int_vs_float(i128::MAX, i128::MAX as f64), Ordering::Less);
    }
}
