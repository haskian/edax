//! The collation a filter and a sort read values under, and the comparisons it gives both: one owner
//! for the decimal order, so a range filter and a sort can never rank two decimals apart.

use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

use crate::decimal_text::{decimal_cmp, is_decimal, parse};
use crate::value::{type_rank, value_cmp, value_eq};

/// How a filter or a sort reads the values it compares, when plain JSON order is not what they
/// mean. Unset, numbers compare as numbers and strings by code point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Collation {
    /// Strings holding decimal text, compared by exact value (see [`crate::canonical_decimal`]).
    Decimal,
}

/// The order a range filter reads: [`value_cmp`] unset; under [`Collation::Decimal`] two decimal texts
/// order by value, and any other pair is unordered (`None`), as a mismatched type is.
pub(crate) fn partial_cmp(collation: Option<Collation>, a: &Value, b: &Value) -> Option<Ordering> {
    match collation {
        None => value_cmp(a, b),
        Some(Collation::Decimal) => match (a, b) {
            (Value::String(x), Value::String(y)) => decimal_cmp(x, y),
            _ => None,
        },
    }
}

/// The order a sort reads, never "incomparable", so a sort is always deterministic. Types rank by
/// [`type_rank`]. Under [`Collation::Decimal`] two strings order as [`partial_cmp`] orders them, and a
/// string that is not decimal text sorts after every decimal text, and among its own kind by code
/// point. Any other pair within a rank orders as [`value_cmp`] orders it, so a decimal key still
/// sorts its numbers by value, and the pairs it leaves unordered (two arrays, two objects, two nulls)
/// tie.
pub(crate) fn total_cmp(collation: Option<Collation>, a: &Value, b: &Value) -> Ordering {
    total_cmp_read(
        collation,
        (a, read_decimal(collation, a).as_ref()),
        (b, read_decimal(collation, b).as_ref()),
    )
}

/// `value` as the decimal a sort under `collation` orders it by: a string of decimal text under
/// [`Collation::Decimal`], else `None`. A sort reads each row's value once, not once per comparison.
pub(crate) fn read_decimal(collation: Option<Collation>, value: &Value) -> Option<BigDecimal> {
    match (collation, value) {
        (Some(Collation::Decimal), Value::String(text)) => parse(text).ok(),
        _ => None,
    }
}

/// [`total_cmp`] over two values each paired with its [`read_decimal`] reading.
pub(crate) fn total_cmp_read(
    collation: Option<Collation>,
    (a, a_decimal): (&Value, Option<&BigDecimal>),
    (b, b_decimal): (&Value, Option<&BigDecimal>),
) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (collation, a, b) {
        (Some(Collation::Decimal), Value::String(x), Value::String(y)) => match (a_decimal, b_decimal) {
            (Some(a_decimal), Some(b_decimal)) => a_decimal.cmp(b_decimal),
            // Text that is not decimal sorts after decimal text, and among its own kind by code point.
            _ => a_decimal.is_none().cmp(&b_decimal.is_none()).then_with(|| x.cmp(y)),
        },
        _ => value_cmp(a, b).unwrap_or(Ordering::Equal),
    }
}

/// Equality under a collation: [`value_eq`] unset, equal decimal values under [`Collation::Decimal`].
pub(crate) fn eq(collation: Option<Collation>, a: &Value, b: &Value) -> bool {
    match collation {
        None => value_eq(a, b),
        Some(_) => partial_cmp(collation, a, b) == Some(Ordering::Equal),
    }
}

/// Whether a filter's comparand is one the collation can read: any value unset, decimal text under
/// [`Collation::Decimal`]. No document value could equal or order against any other comparand.
pub(crate) fn admits(collation: Option<Collation>, value: &Value) -> bool {
    match (collation, value) {
        (None, _) => true,
        (Some(Collation::Decimal), Value::String(text)) => is_decimal(text),
        (Some(Collation::Decimal), _) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn total_order_across_types() {
        // number < string < bool < array < object < null
        let vals = [
            json!(1),
            json!("a"),
            json!(true),
            json!([1]),
            json!({"k": 1}),
            json!(null),
        ];
        for (i, a) in vals.iter().enumerate() {
            for (j, b) in vals.iter().enumerate() {
                assert_eq!(total_cmp(None, a, b), i.cmp(&j), "{i} vs {j}");
            }
        }
        let decimal = Some(Collation::Decimal);
        let within: [(&str, Option<Collation>, Value, Value, Ordering); 12] = [
            (
                "int_and_float_compare_numerically",
                None,
                json!(1),
                json!(1.0),
                Ordering::Equal,
            ),
            // Above 2^53 an f64 cast would round both to the same value and report Equal.
            (
                "integers_order_exactly",
                None,
                json!(9_007_199_254_740_993u64),
                json!(9_007_199_254_740_992u64),
                Ordering::Greater,
            ),
            (
                "bools_order_false_first",
                None,
                json!(false),
                json!(true),
                Ordering::Less,
            ),
            (
                "arrays_order_by_rank_only",
                None,
                json!([1]),
                json!([2]),
                Ordering::Equal,
            ),
            ("nulls_tie", None, json!(null), json!(null), Ordering::Equal),
            (
                "plain_strings_order_by_code_point",
                None,
                json!("9.5"),
                json!("10"),
                Ordering::Greater,
            ),
            (
                "decimals_order_by_value",
                decimal,
                json!("9.5"),
                json!("10"),
                Ordering::Less,
            ),
            (
                "decimal_spellings_of_one_value_tie",
                decimal,
                json!("9.50"),
                json!("9.5"),
                Ordering::Equal,
            ),
            (
                "decimal_text_before_other_text",
                decimal,
                json!("100"),
                json!("abc"),
                Ordering::Less,
            ),
            (
                "other_text_by_code_point",
                decimal,
                json!("b"),
                json!("a"),
                Ordering::Greater,
            ),
            (
                "the_type_order_stays_under_the_collation",
                decimal,
                json!(1),
                json!("0"),
                Ordering::Less,
            ),
            (
                "numbers_stay_numeric_under_the_collation",
                decimal,
                json!(2),
                json!(10),
                Ordering::Less,
            ),
        ];
        for (name, collation, a, b, want) in within {
            assert_eq!(total_cmp(collation, &a, &b), want, "{name}");
            assert_eq!(total_cmp(collation, &b, &a), want.reverse(), "{name} reversed");
        }
    }
}
