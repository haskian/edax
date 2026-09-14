//! Backend-neutral query filter: a sealed AST plus a reference evaluator with precise,
//! everywhere-identical semantics. The bindings build filters and call [`eval_json`]; the match
//! decision is made only here, so the tricky null/missing-path rules can't drift between languages.
//! Translating a filter into a storage backend's native query dialect is left to that backend.
//!
//! Two-valued semantics (no third "unknown"):
//! - A path predicate (`Compare`, `In`) is satisfied only when the path is **present, non-null,
//!   and the comparison holds**. A missing or null path makes that leaf `false`.
//! - `Exists(path, true)` is true iff the path is present and non-null; `Exists(path, false)` is
//!   its complement.
//! - `And` / `Or` / `Not` combine leaves as ordinary booleans (`And([])` = true, `Or([])` = false).
//!
//! A consequence worth stating: `Ne(p, x)` does NOT match a document where `p` is absent/null
//! (the leaf is false), whereas `Not(Eq(p, x))` DOES (the inner leaf is false, negated to true).
//!
//! Value comparison: numbers compare numerically (so `1` and `1.0` are equal), two integers compare
//! exactly (no `f64` rounding above 2^53), strings compare by Unicode order. Ordering (`Gt`/`Gte`/
//! `Lt`/`Lte`) is defined only for two numbers or two strings; any other pairing (including
//! mismatched types, bool, array, object) is not ordered and yields `false`. Equality is numeric for
//! numbers and structural otherwise. Dates/timestamps need no special operator: as RFC3339 strings
//! they sort correctly under the string ordering above.
//!
//! `StartsWith` is a string-prefix test (both operands must be strings, else `false`). `Contains`
//! is the mirror of `In`: `In` tests a scalar path against a given set, `Contains` tests whether
//! the **array** at a path includes a given value.

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::cmp::Ordering;

/// Comparison operator for [`Filter::Compare`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Operator {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// String prefix test: matches iff both the document and filter values are strings and the
    /// document value starts with the filter value.
    StartsWith,
}

/// A sealed filter AST. Each variant serializes to an externally-tagged JSON object, e.g.
/// `{"compare": {"path": "...", "op": "eq", "value": ...}}`, `{"and": [...]}`, `{"not": {...}}`.
/// The bindings produce this exact shape, so a filter built in any language is identical.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    /// Compare the value at `path` with `value` under `op`.
    Compare {
        /// Dot-delimited path into the document.
        path: String,
        /// The comparison to apply.
        op: Operator,
        /// The value to compare against.
        value: Value,
    },
    /// Set membership: matches iff the value at `path` equals one of `values`.
    In {
        /// Dot-delimited path into the document.
        path: String,
        /// The set the document value must belong to.
        values: Vec<Value>,
    },
    /// Array membership: matches iff the path resolves to an array that contains `value`. The
    /// mirror of `In` (which tests a scalar path against a given set).
    Contains {
        /// Dot-delimited path to the array.
        path: String,
        /// The element the array must contain.
        value: Value,
    },
    /// Presence test: matches iff the path resolving to a value equals `exists`.
    Exists {
        /// Dot-delimited path into the document.
        path: String,
        /// Whether the path must be present (true) or absent (false).
        exists: bool,
    },
    /// Every sub-filter matches. An empty list matches.
    And(Vec<Filter>),
    /// At least one sub-filter matches. An empty list does not match.
    Or(Vec<Filter>),
    /// The sub-filter does not match.
    Not(Box<Filter>),
}

// --- constructors (ergonomic; the bindings mirror these) ---

/// The value at `path` equals `value`.
pub fn eq(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::Eq,
        value,
    }
}
/// The value at `path` differs from `value`.
pub fn ne(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::Ne,
        value,
    }
}
/// The value at `path` is greater than `value`.
pub fn gt(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::Gt,
        value,
    }
}
/// The value at `path` is greater than or equal to `value`.
pub fn gte(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::Gte,
        value,
    }
}
/// The value at `path` is less than `value`.
pub fn lt(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::Lt,
        value,
    }
}
/// The value at `path` is less than or equal to `value`.
pub fn lte(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::Lte,
        value,
    }
}
/// The value at `path` is one of `values`.
pub fn is_in(path: impl Into<String>, values: Vec<Value>) -> Filter {
    Filter::In {
        path: path.into(),
        values,
    }
}
/// The string at `path` starts with the string `value`.
pub fn starts_with(path: impl Into<String>, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op: Operator::StartsWith,
        value,
    }
}
/// The array at `path` contains `value`.
pub fn contains(path: impl Into<String>, value: Value) -> Filter {
    Filter::Contains {
        path: path.into(),
        value,
    }
}
/// The path is present when `exists` is true, absent when it is false.
pub fn exists(path: impl Into<String>, exists: bool) -> Filter {
    Filter::Exists {
        path: path.into(),
        exists,
    }
}
/// Every filter matches.
pub fn and(filters: Vec<Filter>) -> Filter {
    Filter::And(filters)
}
/// At least one filter matches.
pub fn or(filters: Vec<Filter>) -> Filter {
    Filter::Or(filters)
}
/// The filter does not match.
pub fn not(filter: Filter) -> Filter {
    Filter::Not(Box::new(filter))
}

// --- evaluation ---

/// Resolve a dot-delimited path into a document. Intermediate segments must be objects; anything
/// else (or a missing key) resolves to `None`. Shared with the read-side query so path semantics
/// match.
pub fn resolve<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
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

fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_cmp(x, y) == Some(Ordering::Equal),
        _ => a == b,
    }
}

/// Order two values within one type: numbers numerically, strings by Unicode. Any other pairing is
/// unordered (`None`). The read-side query layers its cross-type total order on top of this, so both
/// sides order a number or a string the same way.
pub fn value_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_cmp(x, y),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Apply an operator to a present, non-null document value and the filter's value.
fn apply(op: Operator, doc_value: &Value, filter_value: &Value) -> bool {
    match op {
        Operator::Eq => value_eq(doc_value, filter_value),
        Operator::Ne => !value_eq(doc_value, filter_value),
        Operator::Gt => value_cmp(doc_value, filter_value) == Some(Ordering::Greater),
        Operator::Gte => matches!(
            value_cmp(doc_value, filter_value),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        Operator::Lt => value_cmp(doc_value, filter_value) == Some(Ordering::Less),
        Operator::Lte => matches!(
            value_cmp(doc_value, filter_value),
            Some(Ordering::Less | Ordering::Equal)
        ),
        Operator::StartsWith => match (doc_value.as_str(), filter_value.as_str()) {
            (Some(s), Some(prefix)) => s.starts_with(prefix),
            _ => false,
        },
    }
}

/// A path is "present" iff it resolves to a non-null value.
fn present<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    match resolve(doc, path) {
        Some(v) if !v.is_null() => Some(v),
        _ => None,
    }
}

/// Evaluate a filter against a document.
pub fn eval(filter: &Filter, doc: &Value) -> bool {
    match filter {
        Filter::Compare { path, op, value } => present(doc, path).is_some_and(|v| apply(*op, v, value)),
        Filter::In { path, values } => present(doc, path).is_some_and(|v| values.iter().any(|x| value_eq(v, x))),
        Filter::Contains { path, value } => match present(doc, path) {
            Some(Value::Array(items)) => items.iter().any(|x| value_eq(x, value)),
            _ => false,
        },
        Filter::Exists { path, exists } => present(doc, path).is_some() == *exists,
        Filter::And(fs) => fs.iter().all(|f| eval(f, doc)),
        Filter::Or(fs) => fs.iter().any(|f| eval(f, doc)),
        Filter::Not(f) => !eval(f, doc),
    }
}

/// JSON boundary for the bindings: parse a filter and a document, return the match decision.
pub fn eval_json(filter_json: &[u8], doc_json: &[u8]) -> Result<bool, String> {
    let filter: Filter = serde_json::from_slice(filter_json).map_err(|e| e.to_string())?;
    let doc: Value = serde_json::from_slice(doc_json).map_err(|e| e.to_string())?;
    Ok(eval(&filter, &doc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// One document rich enough to hit every leaf: present/absent/null paths, both orderable types,
    /// an array, a nested object, a non-object path segment, and an integer beyond f64 precision.
    fn doc() -> Value {
        json!({
            "status": "open",
            "total": 5000,
            "name": "m",
            "sku": "INV-2024-01",
            "tags": ["paid", "eu"],
            "address": {"city": "NYC"},
            "owner": 5,
            "maybe": null,
            "big": 9_007_199_254_740_993u64,
        })
    }

    #[test]
    fn eval_decides_every_leaf() {
        let cases: Vec<(&str, Filter, bool)> = vec![
            ("eq_matches", eq("status", json!("open")), true),
            ("eq_differs", eq("status", json!("closed")), false),
            ("eq_is_numeric_across_int_and_float", eq("total", json!(5000.0)), true),
            // An f64 cast would round both sides to 2^53 and report a match.
            (
                "eq_integers_compare_exactly",
                eq("big", json!(9_007_199_254_740_992u64)),
                false,
            ),
            ("ne_differs", ne("status", json!("closed")), true),
            ("ne_on_a_missing_path_is_false", ne("missing", json!("x")), false),
            (
                "not_of_eq_on_a_missing_path_is_true",
                not(eq("missing", json!("x"))),
                true,
            ),
            ("gt_greater", gt("total", json!(4200)), true),
            ("gt_equal", gt("total", json!(5000)), false),
            ("gte_at_the_boundary", gte("total", json!(5000)), true),
            ("lt_less", lt("total", json!(9999)), true),
            ("lte_at_the_boundary", lte("total", json!(5000)), true),
            ("strings_order_by_unicode", gt("name", json!("a")), true),
            ("mismatched_types_are_unordered", gt("sku", json!(4200)), false),
            ("compare_on_a_null_path_is_false", eq("maybe", json!(null)), false),
            ("starts_with_prefix", starts_with("sku", json!("INV-")), true),
            ("starts_with_other_prefix", starts_with("sku", json!("ORD-")), false),
            ("starts_with_needs_two_strings", starts_with("owner", json!("5")), false),
            (
                "in_hits_the_set",
                is_in("status", vec![json!("open"), json!("closed")]),
                true,
            ),
            ("in_misses_the_set", is_in("status", vec![json!("a")]), false),
            (
                "in_on_a_missing_path_is_false",
                is_in("missing", vec![json!("a")]),
                false,
            ),
            ("contains_array_member", contains("tags", json!("eu")), true),
            ("contains_missing_array_member", contains("tags", json!("usd")), false),
            ("contains_needs_an_array", contains("sku", json!("INV-2024-01")), false),
            (
                "contains_on_a_missing_path_is_false",
                contains("missing", json!("x")),
                false,
            ),
            ("exists_true_on_a_present_path", exists("status", true), true),
            ("exists_true_on_a_null_path", exists("maybe", true), false),
            ("exists_false_on_a_missing_path", exists("missing", false), true),
            ("nested_path_resolves", eq("address.city", json!("NYC")), true),
            ("missing_nested_key", eq("address.zip", json!("x")), false),
            ("path_through_a_non_object_segment", eq("owner.name", json!("x")), false),
            (
                "and_all_true",
                and(vec![eq("status", json!("open")), gt("total", json!(4200))]),
                true,
            ),
            (
                "and_one_false",
                and(vec![eq("status", json!("open")), gt("total", json!(9999))]),
                false,
            ),
            ("and_empty_is_vacuously_true", and(vec![]), true),
            (
                "or_one_true",
                or(vec![eq("status", json!("closed")), gt("total", json!(4200))]),
                true,
            ),
            (
                "or_none_true",
                or(vec![eq("status", json!("closed")), gt("total", json!(9999))]),
                false,
            ),
            ("or_empty_is_vacuously_false", or(vec![]), false),
        ];
        for (name, filter, want) in cases {
            assert_eq!(eval(&filter, &doc()), want, "{name}");
        }
    }

    #[test]
    fn json_boundary_round_trips_and_rejects_bad_input() {
        let filter = and(vec![eq("status", json!("open")), not(exists("deleted_at", true))]);
        let wire = serde_json::to_vec(&filter).unwrap();
        assert_eq!(serde_json::from_slice::<Filter>(&wire).unwrap(), filter);

        let document = serde_json::to_vec(&doc()).unwrap();
        assert!(eval_json(&wire, &document).unwrap());
        assert!(eval_json(b"not json", &document).is_err());
        assert!(eval_json(&wire, b"not json").is_err());
        assert!(eval_json(br#"{"unknown_variant": {}}"#, &document).is_err());
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

    #[test]
    fn external_tag_shape_is_stable() {
        // The bindings depend on this exact JSON shape.
        assert_eq!(
            serde_json::to_value(eq("p", json!(1))).unwrap(),
            json!({"compare": {"path": "p", "op": "eq", "value": 1}})
        );
        assert_eq!(
            serde_json::to_value(not(exists("p", true))).unwrap(),
            json!({"not": {"exists": {"path": "p", "exists": true}}})
        );
    }
}
