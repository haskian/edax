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
//! So `Ne(p, x)` does NOT match a document where `p` is absent/null
//! (the leaf is false), whereas `Not(Eq(p, x))` DOES (the inner leaf is false, negated to true).
//!
//! Value comparison:
//! - **Equality** is numeric for numbers at every depth (`1 == 1.0`, `[1] == [1.0]`,
//!   `{"a": 1} == {"a": 1.0}`), exact for two integers (no `f64` rounding above 2^53), and structural
//!   otherwise. `Eq`, `Ne`, `In` and `Contains` all use it.
//! - **Ordering** (`Gt`/`Gte`/`Lt`/`Lte`) is defined for two numbers, two strings (by Unicode code
//!   point, which is UTF-8 byte order) and two bools (`false < true`), the same ranking the read
//!   side's sort gives each pair. Any other pairing (mismatched types, arrays, objects) is not
//!   ordered and yields `false`.
//!
//! - **The decimal collation** (`"collation": "decimal"` on `Compare` or `In`) reads both sides as
//!   decimal text ([`crate::canonical_decimal`]) and compares them by exact value: `"9.5" < "10"`
//!   and `"9.50" == "9.5"`, at any precision. Under it, a document value that is not decimal text
//!   (a number, a bool, a string such as `"n/a"`) compares like a mismatched type: `Eq`, ordering
//!   and `In` are `false`, and `Ne` is `true`.
//!
//! Dates and timestamps need no operator of their own, but string order is chronological only for
//! one fixed-width form in one zone: UTC with a `Z` suffix and six fraction digits
//! (`2024-01-01T00:00:00.000000Z`), the form every SDK writes a host timestamp in, or epoch integers.
//! A form that omits a zero fraction (`...00Z` sorts after `...00.500000Z`) or keeps an offset
//! (`+01:00` sorts by wall time, not by instant) orders wrongly, silently.
//!
//! `StartsWith` is a string-prefix test (both operands must be strings, else `false`). `Contains`
//! is the mirror of `In`: `In` tests a scalar path against a given set, `Contains` tests whether
//! the **array** at a path includes a given value.
//!
//! What the kernel refuses, as `codec` errors, rather than reading silently wrong ([`Filter::validate`]):
//! - an unknown field in any node (`{"compare": {..., "vlaue": 1}}`), so a typo is not a no-op;
//! - a `null` comparand for `Compare` or inside `In`, which no present value could equal — ask
//!   `Exists` for a missing or null path instead;
//! - a path with an empty segment (`""`, `"a."`, `"a..b"`). A path is dot-delimited object keys:
//!   a key containing `.` cannot be addressed, and a numeric segment is a key, never an array index;
//! - an integer literal wider than 64 bits, which would otherwise be compared as the nearest float;
//! - under the decimal collation, a comparand that is not decimal text, and `StartsWith`, which tests
//!   text and not a value.

use edax_core::Error;
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

/// How a filter or a sort reads the values it compares, when plain JSON order is not what they
/// mean. Unset, numbers compare as numbers and strings by code point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Collation {
    /// Strings holding decimal text, compared by exact value (see [`crate::canonical_decimal`]).
    Decimal,
}

/// A sealed filter AST. Each variant serializes to an externally-tagged JSON object, e.g.
/// `{"compare": {"path": "...", "op": "eq", "value": ...}}`, `{"and": [...]}`, `{"not": {...}}`.
/// The bindings produce this exact shape, so a filter built in any language is identical.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Filter {
    /// Compare the value at `path` with `value` under `op`.
    Compare {
        /// Dot-delimited path into the document.
        path: String,
        /// The comparison to apply.
        op: Operator,
        /// The value to compare against.
        value: Value,
        /// How both sides are read; unset compares plain JSON values.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collation: Option<Collation>,
    },
    /// Set membership: matches iff the value at `path` equals one of `values`.
    In {
        /// Dot-delimited path into the document.
        path: String,
        /// The set the document value must belong to.
        values: Vec<Value>,
        /// How the document value and each member are read; unset compares plain JSON values.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collation: Option<Collation>,
    },
    /// Array membership: matches iff the path resolves to an array that contains `value`. The
    /// mirror of `In` (which tests a scalar path against a given set).
    Contains {
        /// Dot-delimited path to the array.
        path: String,
        /// The element the array must contain.
        value: Value,
    },
    /// Presence test: matches iff the path being present (resolving to a non-null value) equals
    /// `exists`.
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

fn compare(path: impl Into<String>, op: Operator, value: Value) -> Filter {
    Filter::Compare {
        path: path.into(),
        op,
        value,
        collation: None,
    }
}

/// The value at `path` equals `value`.
pub fn eq(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::Eq, value)
}
/// The value at `path` differs from `value`.
pub fn ne(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::Ne, value)
}
/// The value at `path` is greater than `value`.
pub fn gt(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::Gt, value)
}
/// The value at `path` is greater than or equal to `value`.
pub fn gte(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::Gte, value)
}
/// The value at `path` is less than `value`.
pub fn lt(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::Lt, value)
}
/// The value at `path` is less than or equal to `value`.
pub fn lte(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::Lte, value)
}
/// The value at `path` is one of `values`.
pub fn is_in(path: impl Into<String>, values: Vec<Value>) -> Filter {
    Filter::In {
        path: path.into(),
        values,
        collation: None,
    }
}
/// The string at `path` starts with the string `value`.
pub fn starts_with(path: impl Into<String>, value: Value) -> Filter {
    compare(path, Operator::StartsWith, value)
}
/// Constructors under [`Collation::Decimal`]: the value at `path` and `value` are decimal text, and
/// compare by exact value (`decimal::lt("price", "10")` matches `"9.5"`).
pub mod decimal {
    use super::{Collation, Filter, Operator};
    use serde_json::Value;

    fn compare(path: impl Into<String>, op: Operator, value: impl Into<String>) -> Filter {
        Filter::Compare {
            path: path.into(),
            op,
            value: Value::String(value.into()),
            collation: Some(Collation::Decimal),
        }
    }

    /// The decimal at `path` equals `value`.
    pub fn eq(path: impl Into<String>, value: impl Into<String>) -> Filter {
        compare(path, Operator::Eq, value)
    }
    /// The decimal at `path` differs from `value`.
    pub fn ne(path: impl Into<String>, value: impl Into<String>) -> Filter {
        compare(path, Operator::Ne, value)
    }
    /// The decimal at `path` is greater than `value`.
    pub fn gt(path: impl Into<String>, value: impl Into<String>) -> Filter {
        compare(path, Operator::Gt, value)
    }
    /// The decimal at `path` is greater than or equal to `value`.
    pub fn gte(path: impl Into<String>, value: impl Into<String>) -> Filter {
        compare(path, Operator::Gte, value)
    }
    /// The decimal at `path` is less than `value`.
    pub fn lt(path: impl Into<String>, value: impl Into<String>) -> Filter {
        compare(path, Operator::Lt, value)
    }
    /// The decimal at `path` is less than or equal to `value`.
    pub fn lte(path: impl Into<String>, value: impl Into<String>) -> Filter {
        compare(path, Operator::Lte, value)
    }
    /// The decimal at `path` equals one of `values`.
    pub fn is_in<V: Into<String>>(path: impl Into<String>, values: impl IntoIterator<Item = V>) -> Filter {
        Filter::In {
            path: path.into(),
            values: values.into_iter().map(|value| Value::String(value.into())).collect(),
            collation: Some(Collation::Decimal),
        }
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

/// Equality with numbers compared by value at every depth, so a number means the same inside an
/// array or an object as it does at the top level.
fn value_eq(a: &Value, b: &Value) -> bool {
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
/// `false < true`. Any other pairing is unordered (`None`). The read-side query layers its
/// cross-type total order on top of this, so a sort and a range filter rank every orderable pair the
/// same way.
pub fn value_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_cmp(x, y),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// [`value_cmp`] under a collation. Under [`Collation::Decimal`] two decimal texts order by value,
/// and any other pair is unordered (`None`), as a mismatched type is.
pub fn collated_cmp(collation: Option<Collation>, a: &Value, b: &Value) -> Option<Ordering> {
    match collation {
        None => value_cmp(a, b),
        Some(Collation::Decimal) => match (a, b) {
            (Value::String(x), Value::String(y)) => crate::decimal::decimal_cmp(x, y),
            _ => None,
        },
    }
}

/// Equality under a collation: [`value_eq`] unset, equal decimal values under [`Collation::Decimal`].
fn collated_eq(collation: Option<Collation>, a: &Value, b: &Value) -> bool {
    match collation {
        None => value_eq(a, b),
        Some(_) => collated_cmp(collation, a, b) == Some(Ordering::Equal),
    }
}

/// Apply an operator to a present, non-null document value and the filter's value.
fn apply(op: Operator, collation: Option<Collation>, doc_value: &Value, filter_value: &Value) -> bool {
    let order = || collated_cmp(collation, doc_value, filter_value);
    match op {
        Operator::Eq => collated_eq(collation, doc_value, filter_value),
        Operator::Ne => !collated_eq(collation, doc_value, filter_value),
        Operator::Gt => order() == Some(Ordering::Greater),
        Operator::Gte => matches!(order(), Some(Ordering::Greater | Ordering::Equal)),
        Operator::Lt => order() == Some(Ordering::Less),
        Operator::Lte => matches!(order(), Some(Ordering::Less | Ordering::Equal)),
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
        Filter::Compare { path, op, value, collation } => present(doc, path).is_some_and(|v| apply(*op, *collation, v, value)),
        Filter::In { path, values, collation } => {
            present(doc, path).is_some_and(|v| values.iter().any(|x| collated_eq(*collation, v, x)))
        }
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

impl Filter {
    /// Refuse what the grammar admits but no document could mean: a `null` comparand and a path
    /// with an empty segment (see the module docs). Recursive, so a bad leaf under `And`, `Or` or
    /// `Not` is found too.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Filter::Compare { path, op, value, collation } => {
                check_path(path)?;
                check_comparand(path, value)?;
                if collation.is_some() && *op == Operator::StartsWith {
                    return Err(Error::codec(format_args!(
                        "starts_with at {path:?} tests text, so it takes no collation"
                    )));
                }
                check_collated(path, *collation, value)
            }
            Filter::In { path, values, collation } => {
                check_path(path)?;
                values.iter().try_for_each(|value| {
                    check_comparand(path, value)?;
                    check_collated(path, *collation, value)
                })
            }
            Filter::Contains { path, .. } | Filter::Exists { path, .. } => check_path(path),
            Filter::And(filters) | Filter::Or(filters) => filters.iter().try_for_each(Filter::validate),
            Filter::Not(filter) => filter.validate(),
        }
    }
}

/// A path is one or more dot-delimited object keys, none of them empty.
pub(crate) fn check_path(path: &str) -> Result<(), Error> {
    if path.split('.').any(str::is_empty) {
        return Err(Error::codec(format_args!("path {path:?} has an empty segment")));
    }
    Ok(())
}

fn check_comparand(path: &str, value: &Value) -> Result<(), Error> {
    if value.is_null() {
        return Err(Error::codec(format_args!(
            "a null comparand at {path:?} matches nothing; use exists({path:?}, false) for a missing or null path"
        )));
    }
    Ok(())
}

/// Refuse a comparand the collation cannot read, which no document value could equal or order against.
fn check_collated(path: &str, collation: Option<Collation>, value: &Value) -> Result<(), Error> {
    match (collation, value) {
        (None, _) => Ok(()),
        (Some(Collation::Decimal), Value::String(text)) if crate::decimal::is_decimal(text) => Ok(()),
        (Some(Collation::Decimal), _) => Err(Error::codec(format_args!(
            "the decimal collation at {path:?} needs decimal text as the comparand, not {value}"
        ))),
    }
}

/// Parse `json` as `T` under the strict reading every query spec gets: integer literals within 64
/// bits, unknown fields refused by `T`'s own attributes. Shared with the read side's query parse.
pub(crate) fn parse_spec<T: serde::de::DeserializeOwned>(json: &[u8]) -> Result<T, Error> {
    crate::canonical::reject_wide_integers(json)?;
    serde_json::from_slice(json).map_err(Error::codec)
}

/// Refuse a document the filter and the sort could not compare exactly: one holding an integer
/// literal wider than 64 bits, which `serde_json` reads as the nearest float, so it would come back
/// rounded and match or sort as its neighbour. The query spec refuses the same literals. A `codec`
/// error. Every boundary that reads a document from text runs it, a projection store's save among
/// them, so such a document is refused where it is written rather than read back changed.
pub fn check_document(json: &[u8]) -> Result<(), Error> {
    crate::canonical::reject_wide_integers(json)
}

/// JSON boundary for the bindings: parse a filter and a document, return the match decision. A
/// filter the kernel refuses (see [`Filter::validate`]), a document [`check_document`] refuses, or
/// JSON it cannot read is a `codec` error.
pub fn eval_json(filter_json: &[u8], doc_json: &[u8]) -> Result<bool, Error> {
    let filter: Filter = parse_spec(filter_json)?;
    filter.validate()?;
    check_document(doc_json)?;
    let doc: Value = serde_json::from_slice(doc_json).map_err(Error::codec)?;
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
            "paid": true,
            "amounts": [1, 2.5],
            "totals": {"net": 100, "tax": 20},
            "price": "9.50",
            "debt": "-12.5",
            "exact": "12345678901234567890.12",
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
            ("lte_below", lte("total", json!(9999)), true),
            ("lte_above", lte("total", json!(4999)), false),
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
            // Numbers mean the same at every depth as at the top level.
            ("eq_array_is_numeric_per_element", eq("amounts", json!([1.0, 2.5])), true),
            ("eq_array_needs_the_same_length", eq("amounts", json!([1.0])), false),
            ("eq_array_needs_equal_elements", eq("amounts", json!([1, 2])), false),
            ("eq_object_is_numeric_per_member", eq("totals", json!({"net": 100.0, "tax": 20})), true),
            ("eq_object_needs_the_same_keys", eq("totals", json!({"net": 100, "fee": 20})), false),
            ("eq_object_needs_the_same_size", eq("totals", json!({"net": 100})), false),
            ("in_is_numeric_inside_arrays", is_in("amounts", vec![json!([1.0, 2.5])]), true),
            ("contains_is_numeric", contains("amounts", json!(2.50)), true),
            // Bools order `false < true`, as the sort ranks them.
            ("gt_true_over_false", gt("paid", json!(false)), true),
            ("lt_true_under_false", lt("paid", json!(false)), false),
            ("gte_bool_at_the_boundary", gte("paid", json!(true)), true),
            ("bool_against_a_number_is_unordered", gt("paid", json!(0)), false),
            ("arrays_are_unordered", gt("amounts", json!([0])), false),
            // The decimal collation: exact value order, where code point order puts "10" first.
            ("decimal_lt_by_value", decimal::lt("price", "10"), true),
            ("decimal_gt_by_value", decimal::gt("price", "10"), false),
            ("text_order_is_not_value_order", lt("price", json!("10")), false),
            ("decimal_eq_ignores_trailing_zeros", decimal::eq("price", "9.5"), true),
            ("decimal_ne_on_an_equal_value", decimal::ne("price", "9.5"), false),
            ("decimal_gte_at_the_boundary", decimal::gte("price", "9.5000"), true),
            ("decimal_lte_below", decimal::lte("price", "9.49"), false),
            ("decimal_negatives_order_by_value", decimal::lt("debt", "-9.5"), true),
            ("decimal_past_17_digits_is_exact", decimal::gt("exact", "12345678901234567890.119"), true),
            ("decimal_in_by_value", decimal::is_in("price", ["1", "9.5"]), true),
            ("decimal_in_misses", decimal::is_in("price", ["9.51"]), false),
            ("decimal_against_text_is_unordered", decimal::lt("sku", "10"), false),
            ("decimal_eq_against_text_is_false", decimal::eq("sku", "10"), false),
            ("decimal_ne_against_text_is_true", decimal::ne("sku", "10"), true),
            ("decimal_against_a_number_is_unordered", decimal::gt("total", "1"), false),
            ("decimal_on_a_missing_path_is_false", decimal::ne("missing", "1"), false),
        ];
        for (name, filter, want) in cases {
            assert_eq!(eval(&filter, &doc()), want, "{name}");
        }
    }

    /// One case per arm of [`Filter::validate`], at the top level and nested.
    #[test]
    fn validate_branches() {
        let ok: Result<(), &str> = Ok(());
        let cases: Vec<(&str, Filter, Result<(), &str>)> = vec![
            ("compare_with_a_value", eq("status", json!("open")), ok),
            ("compare_with_null", eq("status", json!(null)), Err("null comparand")),
            ("ne_with_null", ne("status", json!(null)), Err("null comparand")),
            ("in_with_values", is_in("status", vec![json!("a"), json!("b")]), ok),
            ("in_empty_matches_nothing_but_is_valid", is_in("status", vec![]), ok),
            ("in_with_a_null_member", is_in("status", vec![json!("a"), json!(null)]), Err("null comparand")),
            ("contains_null_asks_for_a_null_element", contains("tags", json!(null)), ok),
            ("exists", exists("status", false), ok),
            ("an_empty_path", eq("", json!(1)), Err("empty segment")),
            ("a_trailing_dot", exists("a.", true), Err("empty segment")),
            ("a_leading_dot", contains(".a", json!(1)), Err("empty segment")),
            ("a_double_dot", is_in("a..b", vec![json!(1)]), Err("empty segment")),
            ("a_compare_path_is_checked_before_its_value", eq("a..b", json!(null)), Err("empty segment")),
            ("and_finds_a_bad_leaf", and(vec![exists("a", true), eq("b", json!(null))]), Err("null comparand")),
            ("or_finds_a_bad_leaf", or(vec![exists("a.", true)]), Err("empty segment")),
            ("not_finds_a_bad_leaf", not(eq("b", json!(null))), Err("null comparand")),
            ("empty_and_or", and(vec![or(vec![])]), ok),
            ("decimal_compare_with_decimal_text", decimal::gt("price", "1E+2"), ok),
            ("decimal_compare_with_other_text", decimal::gt("price", "ten"), Err("needs decimal text")),
            ("decimal_compare_with_nan", decimal::eq("price", "NaN"), Err("needs decimal text")),
            (
                "decimal_compare_with_a_number",
                Filter::Compare { path: "price".into(), op: Operator::Lt, value: json!(10), collation: Some(Collation::Decimal) },
                Err("needs decimal text"),
            ),
            (
                "decimal_starts_with",
                Filter::Compare { path: "price".into(), op: Operator::StartsWith, value: json!("9"), collation: Some(Collation::Decimal) },
                Err("takes no collation"),
            ),
            ("decimal_in_with_decimal_text", decimal::is_in("price", ["1", "2.5"]), ok),
            ("decimal_in_with_other_text", decimal::is_in("price", ["1", "x"]), Err("needs decimal text")),
            ("decimal_in_with_a_null", Filter::In { path: "p".into(), values: vec![json!(null)], collation: Some(Collation::Decimal) }, Err("null comparand")),
        ];
        for (name, filter, want) in cases {
            match (filter.validate(), want) {
                (Ok(()), Ok(())) => {}
                (Err(error), Err(needle)) => {
                    assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
                    assert!(error.message.contains(needle), "{name}: {error}");
                }
                (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }

    /// The JSON boundary reads a filter strictly: an unknown field anywhere, a wide integer, or a
    /// filter `validate` refuses is an error, never a filter that silently means something else.
    #[test]
    fn eval_json_refusals() {
        let document = serde_json::to_vec(&doc()).unwrap();
        let wide_document = br#"{"big": 18446744073709551617}"#;
        let cases: [(&str, &str, &[u8]); 8] = [
            ("a_typo_in_a_compare_field", r#"{"compare": {"path": "total", "op": "gt", "vlaue": 1}}"#, &document),
            ("an_extra_field_in_exists", r#"{"exists": {"path": "total", "exists": true, "deep": true}}"#, &document),
            ("an_extra_field_in_in", r#"{"in": {"path": "total", "values": [1], "all": true}}"#, &document),
            ("an_unknown_operator", r#"{"compare": {"path": "total", "op": "like", "value": 1}}"#, &document),
            ("an_integer_wider_than_64_bits", r#"{"compare": {"path": "big", "op": "eq", "value": 1180591620717411303424}}"#, &document),
            ("a_negative_integer_wider_than_64_bits", r#"{"compare": {"path": "big", "op": "eq", "value": -9223372036854775809}}"#, &document),
            ("a_null_comparand", r#"{"compare": {"path": "maybe", "op": "eq", "value": null}}"#, &document),
            // Read as the nearest float, the document would match `eq(big, 2^64)`.
            ("a_document_integer_wider_than_64_bits", r#"{"compare": {"path": "big", "op": "eq", "value": 1}}"#, wide_document),
        ];
        for (name, filter, document) in cases {
            let error = eval_json(filter.as_bytes(), document).expect_err(name);
            assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
        }
        // The widest integers that fit are exact, and a wide number written as a float is a float.
        let fits = r#"{"compare": {"path": "big", "op": "lt", "value": 18446744073709551615}}"#;
        assert_eq!(eval_json(fits.as_bytes(), &document), Ok(true));
        let float = r#"{"compare": {"path": "big", "op": "lt", "value": 1.2e21}}"#;
        assert_eq!(eval_json(float.as_bytes(), &document), Ok(true));
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
        assert_eq!(
            serde_json::to_value(decimal::is_in("p", ["1"])).unwrap(),
            json!({"in": {"path": "p", "values": ["1"], "collation": "decimal"}})
        );
    }
}
