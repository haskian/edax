//! Read-side query semantics: how a filtered set of documents is ordered and paginated.
//!
//! Backends translate this into their native `ORDER BY` / `LIMIT` / keyset clauses, but the
//! ordering and pagination rules must be identical everywhere or two services paging the same
//! read model would disagree on row order and cursor boundaries. So the kernel fixes:
//!
//! - a **total order** across JSON values (the comparator never returns "incomparable"), so a sort
//!   is always deterministic regardless of the value types present;
//! - the **effective sort**: the declared keys plus a unique [`TIEBREAK_PATH`] key, so no two rows
//!   tie and keyset paging never drops or repeats one;
//! - **keyset cursors** bound to the sort they were minted under, so a cursor minted by one binding
//!   is read byte-for-byte by every other binding, and a cursor replayed under another sort is
//!   refused instead of paging garbage.
//!
//! [`query`] is the reference applier (filter -> keyset -> sort -> offset -> limit) that the golden
//! vectors pin; backends that push the query down to storage assert against it. It returns the page
//! and the cursor for the next one, so a caller never restates the sort to mint it.
//!
//! Total type order (smallest to largest): number < string < bool < array < object < null. Within
//! a type: numbers numerically, strings by Unicode code point, bools `false < true` — the order
//! [`crate::filter::value_cmp`] gives a range filter. A **missing** path and an explicit `null`
//! both sort as null (last in ascending). Arrays and objects order by type rank only — two of them
//! compare equal.
//!
//! A key under the decimal collation (`{"path": "price", "collation": "decimal"}`) orders decimal
//! text by exact value, as the collated range filter does (`"9.5"` before `"10"`, `"9.50"` tied with
//! `"9.5"`). The type order stays, and within the strings, decimal text sorts before every other
//! string, which keeps code point order among itself. A cursor carries the row's string unchanged,
//! so it resumes at the exact decimal.
//!
//! The cursor is `base64url(canonical_json({"s": <sort fingerprint>, "v": [<sort-key values>]}))`,
//! unpadded so it survives a URL query string. The canonical form is RFC 8785, whose numbers are
//! doubles, so a sort value that is an integer outside `-(2^53-1)..=2^53-1` cannot be minted into a
//! cursor. RFC 8785 writes a whole-number double past that range as integer digits
//! (`5.808820077685227e16` as `58088200776852270`), so the decoder reads any integer outside the
//! range back as the double it came from. The fingerprint is the [`crate::payload_hash`] of the
//! canonical effective sort, as 16 lowercase hex digits.

use crate::canonical::MAX_SAFE_INTEGER;
use crate::filter::{self, Collation, Filter};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use edax_core::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

/// One ordering key: a dot-delimited path, a direction (`desc` defaults to ascending) and an
/// optional collation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortKey {
    /// Dot-delimited path into the document; a missing path sorts as null.
    pub path: String,
    /// Descending when true, ascending otherwise.
    #[serde(default)]
    pub desc: bool,
    /// How the values compare; unset sorts plain JSON values. Left out of the wire when unset, so a
    /// plain key keeps its cursor fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<Collation>,
}

impl SortKey {
    /// A key over plain JSON values.
    pub fn new(path: impl Into<String>, desc: bool) -> SortKey {
        SortKey { path: path.into(), desc, collation: None }
    }

    /// A key over decimal text, ordered by exact value ([`Collation::Decimal`]).
    pub fn decimal(path: impl Into<String>, desc: bool) -> SortKey {
        SortKey { path: path.into(), desc, collation: Some(Collation::Decimal) }
    }
}

/// A read query over a set of documents: filter -> keyset(`after`) -> sort -> offset -> limit. All
/// fields are optional; the boundary JSON is
/// `{"filter": <Filter|null>, "sort": [SortKey], "offset": u, "limit": u|null, "after": cursor|null}`.
///
/// The one authoritative shape: an SDK builds and serializes it, the kernel deserializes it, so both
/// attribute sets sit on the same fields. `default` governs the way in — a spec that omits a field is
/// legal. `skip_serializing_if` governs the way out — an unset field is left out, so a spec carries
/// only what the caller set and a backend reading it as its own dialect sees no phantom clauses.
/// `offset: 0` is skipped for the same reason an empty `sort` is: it asks for nothing, and a
/// binding that emitted it while another omitted it would hand one backend a document the other
/// never sends. An unknown field is refused, so a typo (`"limt"`, `"dsc"`) is an error rather than
/// a query that silently ignores it.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Match condition; every document passes when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    /// Ordering keys, applied in order. Empty keeps the input order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<SortKey>,
    /// Rows to skip after sorting.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub offset: usize,
    /// Maximum rows in the page; unset returns the rest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Keyset cursor from a prior page's [`Page::next`]. Requires the same non-empty `sort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl Query {
    /// Refuse a spec the kernel would read silently wrong: a filter [`Filter::validate`] refuses, a
    /// sort path with an empty segment, or a cursor on an unsorted query (it has no order to resume
    /// from).
    pub fn validate(&self) -> Result<(), Error> {
        if let Some(filter) = &self.filter {
            filter.validate()?;
        }
        self.sort.iter().try_for_each(|key| filter::check_path(&key.path))?;
        if self.after.is_some() && self.sort.is_empty() {
            return Err(unsorted_cursor());
        }
        Ok(())
    }
}

/// One page of a query: the rows, and the cursor that resumes after them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    /// The documents on this page, in the query's order.
    pub rows: Vec<Value>,
    /// The cursor for the next page, minted from the last row under the query's own sort: pass it
    /// back as [`Query::after`] with the same sort. `None` when no row follows this page, and always
    /// for an unsorted query, which has no keyset to resume and pages by `offset` alone.
    pub next: Option<String>,
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Number(_) => 0,
        Value::String(_) => 1,
        Value::Bool(_) => 2,
        Value::Array(_) => 3,
        Value::Object(_) => 4,
        Value::Null => 5,
    }
}

/// Total order over JSON values — never "incomparable", so sorting is always deterministic.
fn compare_values(collation: Option<Collation>, a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (collation, a, b) {
        // Under the decimal collation two decimal texts order by value, as the collated range filter
        // orders them. A string that is not decimal text sorts after every decimal text, and among
        // its own kind by code point, so the order stays total.
        (Some(Collation::Decimal), Value::String(x), Value::String(y)) => {
            filter::collated_cmp(collation, a, b).unwrap_or_else(|| {
                let other_text = |text: &str| !crate::decimal::is_decimal(text);
                other_text(x).cmp(&other_text(y)).then_with(|| x.cmp(y))
            })
        }
        // Within a rank the filter kernel's order, so a sort and a range filter rank a number, a
        // string or a bool pair identically. Same-rank arrays, objects and nulls have no order beyond
        // the rank.
        _ => filter::value_cmp(a, b).unwrap_or(Ordering::Equal),
    }
}

static NULL: Value = Value::Null;

/// The value at `path`, with a missing path reading as `null` so it sorts last.
fn at<'a>(doc: &'a Value, path: &str) -> &'a Value {
    filter::resolve(doc, path).unwrap_or(&NULL)
}

/// Apply a key's direction to an ascending comparison.
fn directed(key: &SortKey, order: Ordering) -> Ordering {
    if key.desc {
        order.reverse()
    } else {
        order
    }
}

/// The unique document path appended as the final sort key, so no two rows share a full sort tuple.
/// A row a cursor is minted from must carry a string or a number here; a backend that keys its rows
/// by something else injects it under this name (the in-memory store injects its row key).
pub const TIEBREAK_PATH: &str = "id";

/// The keys a page is actually ordered by: `sort` with an ascending [`TIEBREAK_PATH`] key appended
/// unless it is already the last key. Empty for an empty sort: an unsorted query keeps input order
/// and takes no cursor.
///
/// A backend translating a [`Query`] into its own dialect orders by exactly these keys, tiebreak
/// included, or its pages disagree with the kernel at every tie.
pub fn effective_sort(sort: &[SortKey]) -> Vec<SortKey> {
    let mut keys = sort.to_vec();
    if !keys.is_empty() && keys.last().map(|k| k.path.as_str()) != Some(TIEBREAK_PATH) {
        keys.push(SortKey::new(TIEBREAK_PATH, false));
    }
    keys
}

/// Compare two documents under the given sort keys (missing/null sort last in ascending order).
fn compare(keys: &[SortKey], a: &Value, b: &Value) -> Ordering {
    keys.iter().fold(Ordering::Equal, |order, key| {
        order.then_with(|| directed(key, compare_values(key.collation, at(a, &key.path), at(b, &key.path))))
    })
}

fn unsorted_cursor() -> Error {
    Error::codec("an unsorted query has no order to resume from, so it takes no cursor")
}

/// What a cursor carries: the fingerprint of the effective sort it was minted under, and the row's
/// value for each of its keys.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorWire {
    s: String,
    v: Vec<Value>,
}

/// The fingerprint of an effective sort: its canonical JSON, hashed, as 16 hex digits.
fn fingerprint(keys: &[SortKey]) -> Result<String, Error> {
    let json = serde_json::to_vec(keys).map_err(Error::codec)?;
    Ok(format!("{:016x}", crate::payload_hash(&crate::canonical_json(&json)?)))
}

/// Mint the keyset cursor that resumes after `doc` under `sort` (the query's declared sort; the
/// tiebreak is applied here). Every binding produces the same string, so cursors are portable across
/// services. [`query`] mints [`Page::next`] with it; a backend that pages natively mints its own
/// with it too.
///
/// Refused as `codec`: an empty sort (nothing to resume by); a row whose [`TIEBREAK_PATH`] is not a
/// string or a number — a missing or null one, an array or an object each compare equal to their own
/// kind, so rows tied on every other key would tie on the tiebreak too, and paging past them would
/// drop the rest of the tie; and a sort value that is an integer outside `-(2^53-1)..=2^53-1`, which
/// the canonical form would round onto its neighbour and so resume at the wrong row.
pub fn encode_cursor(doc: &Value, sort: &[SortKey]) -> Result<String, Error> {
    let keys = effective_sort(sort);
    if keys.is_empty() {
        return Err(unsorted_cursor());
    }
    let values: Vec<Value> = keys.iter().map(|k| at(doc, &k.path).clone()).collect();
    if !matches!(at(doc, TIEBREAK_PATH), Value::String(_) | Value::Number(_)) {
        return Err(Error::codec(format_args!(
            "a keyset cursor needs a string or number `{TIEBREAK_PATH}` on the row it resumes after"
        )));
    }
    let json = serde_json::to_vec(&CursorWire { s: fingerprint(&keys)?, v: values }).map_err(Error::codec)?;
    Ok(B64.encode(crate::canonical_json(&json)?))
}

/// Decode a keyset cursor into the value tuple it carries, one value per key of the effective sort.
/// The inverse of [`encode_cursor`], and the piece a backend needs to turn [`Query::after`] into the
/// bound values of its own `WHERE` clause without re-deriving the wire format from prose.
///
/// Pass the query's declared sort. A cursor minted under any other sort — other keys, another
/// direction, another order — is a `codec` error, so it can never page garbage; so are an unsorted
/// query and text that is not a cursor.
pub fn decode_cursor(cursor: &str, sort: &[SortKey]) -> Result<Vec<Value>, Error> {
    let keys = effective_sort(sort);
    if keys.is_empty() {
        return Err(unsorted_cursor());
    }
    let bytes = B64.decode(cursor).map_err(|e| Error::codec(format_args!("cursor is not base64url: {e}")))?;
    let wire: CursorWire =
        serde_json::from_slice(&bytes).map_err(|e| Error::codec(format_args!("cursor is not a keyset cursor: {e}")))?;
    if wire.s != fingerprint(&keys)? {
        return Err(Error::codec("cursor was minted under another sort"));
    }
    if wire.v.len() != keys.len() {
        return Err(Error::codec(format_args!(
            "cursor carries {} values for {} sort keys",
            wire.v.len(),
            keys.len()
        )));
    }
    Ok(wire.v.into_iter().map(as_minted).collect())
}

/// A cursor value as it was minted. [`encode_cursor`] refuses an integer outside the safe range, so
/// an integer outside it is a whole-number double that RFC 8785 wrote out. Read as an exact integer
/// it would sit beside the row's own value, and the walk would repeat that row or skip its ties.
fn as_minted(value: Value) -> Value {
    match value {
        Value::Number(n) => {
            let magnitude = n.as_i64().map(i64::unsigned_abs).or_else(|| n.as_u64());
            match (magnitude, n.as_f64()) {
                (Some(magnitude), Some(double)) if magnitude > MAX_SAFE_INTEGER => Value::from(double),
                _ => Value::Number(n),
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(as_minted).collect()),
        Value::Object(fields) => Value::Object(fields.into_iter().map(|(k, v)| (k, as_minted(v))).collect()),
        other => other,
    }
}

/// Whether a document sorts strictly after the cursor tuple under the given keys. An equal tuple is
/// not "after": that row was the last of the previous page.
fn after_cursor(keys: &[SortKey], doc: &Value, tuple: &[Value]) -> bool {
    let order = keys.iter().zip(tuple).fold(Ordering::Equal, |order, (key, bound)| {
        order.then_with(|| directed(key, compare_values(key.collation, at(doc, &key.path), bound)))
    });
    order == Ordering::Greater
}

/// Reference applier: filter -> keyset(after) -> stable sort -> offset -> limit, and the cursor for
/// the next page. A spec [`Query::validate`] refuses, or a cursor [`decode_cursor`] refuses, is a
/// `codec` error.
pub fn query(query: &Query, docs: Vec<Value>) -> Result<Page, Error> {
    query.validate()?;
    let mut rows: Vec<Value> = match &query.filter {
        Some(f) => docs.into_iter().filter(|d| filter::eval(f, d)).collect(),
        None => docs,
    };
    let sort = effective_sort(&query.sort);
    if let Some(cursor) = &query.after {
        let tuple = decode_cursor(cursor, &query.sort)?;
        rows.retain(|d| after_cursor(&sort, d, &tuple));
    }
    rows.sort_by(|a, b| compare(&sort, a, b)); // stable

    let mut rest = rows.into_iter().skip(query.offset);
    let page: Vec<Value> = match query.limit {
        Some(limit) => rest.by_ref().take(limit).collect(),
        None => rest.by_ref().collect(),
    };
    let next = match (page.last(), rest.next(), sort.is_empty()) {
        (Some(last), Some(_), false) => Some(encode_cursor(last, &query.sort)?),
        _ => None,
    };
    Ok(Page { rows: page, next })
}

/// A query spec from JSON text, read strictly as [`query_json`] reads it: an integer literal wider than
/// 64 bits or an unknown field is the terminal `codec` error. For a store that holds its documents
/// itself and must refuse exactly the specs the kernel refuses.
pub fn query_spec(spec: &[u8]) -> Result<Query, Error> {
    filter::parse_spec(spec)
}

/// JSON boundary for [`query`]: the spec and a JSON array of documents in, the [`Page`] as
/// `{"rows": [...], "next": cursor|null}` out. The spec is read strictly (see [`query_spec`]), and so
/// are the documents (see [`filter::check_document`]).
pub fn query_json(spec: &[u8], docs: &[u8]) -> Result<String, Error> {
    let spec = query_spec(spec)?;
    filter::check_document(docs)?;
    let docs: Vec<Value> = serde_json::from_slice(docs).map_err(Error::codec)?;
    serde_json::to_string(&query(&spec, docs)?).map_err(Error::codec)
}

/// Boundary helper: decode a cursor into its value tuple under `sort` (JSON in, JSON array out).
pub fn decode_cursor_json(cursor: &[u8], sort: &[u8]) -> Result<String, Error> {
    let cursor = std::str::from_utf8(cursor).map_err(Error::codec)?;
    let keys: Vec<SortKey> = filter::parse_spec(sort)?;
    let tuple = decode_cursor(cursor, &keys)?;
    serde_json::to_string(&tuple).map_err(Error::codec)
}

/// Boundary helper: the sort keys a page is actually ordered by, tiebreak included (JSON in, out).
pub fn effective_sort_json(sort: &[u8]) -> Result<String, Error> {
    let keys: Vec<SortKey> = filter::parse_spec(sort)?;
    serde_json::to_string(&effective_sort(&keys)).map_err(Error::codec)
}

/// Boundary helper: mint the cursor that resumes after a document under the sort keys (both JSON).
pub fn encode_cursor_json(doc: &[u8], sort: &[u8]) -> Result<String, Error> {
    filter::check_document(doc)?;
    let doc: Value = serde_json::from_slice(doc).map_err(Error::codec)?;
    let keys: Vec<SortKey> = filter::parse_spec(sort)?;
    encode_cursor(&doc, &keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn keys(spec: &[(&str, bool)]) -> Vec<SortKey> {
        spec.iter().map(|(p, d)| SortKey::new(*p, *d)).collect()
    }

    /// Four rows: distinct `n` plus one row missing `n`, two statuses, unique ids out of sort order.
    fn docs() -> Vec<Value> {
        vec![
            json!({"id": "c", "n": 3, "s": "open"}),
            json!({"id": "a", "n": 1, "s": "closed"}),
            json!({"id": "b", "n": 2, "s": "open"}),
            json!({"id": "d", "s": "open"}),
        ]
    }

    fn run(spec: Value, docs: Vec<Value>) -> Result<Page, Error> {
        query(&serde_json::from_value(spec).unwrap(), docs)
    }

    fn ids(rows: &[Value]) -> Vec<&str> {
        rows.iter().map(|d| d["id"].as_str().unwrap_or("?")).collect()
    }

    #[test]
    fn total_order_across_types() {
        // number < string < bool < array < object < null
        let vals = [json!(1), json!("a"), json!(true), json!([1]), json!({"k": 1}), json!(null)];
        for (i, a) in vals.iter().enumerate() {
            for (j, b) in vals.iter().enumerate() {
                assert_eq!(compare_values(None, a, b), i.cmp(&j), "{i} vs {j}");
            }
        }
        let decimal = Some(Collation::Decimal);
        let within: [(&str, Option<Collation>, Value, Value, Ordering); 12] = [
            ("int_and_float_compare_numerically", None, json!(1), json!(1.0), Ordering::Equal),
            // Above 2^53 an f64 cast would round both to the same value and report Equal.
            ("integers_order_exactly", None, json!(9_007_199_254_740_993u64), json!(9_007_199_254_740_992u64), Ordering::Greater),
            ("bools_order_false_first", None, json!(false), json!(true), Ordering::Less),
            ("arrays_order_by_rank_only", None, json!([1]), json!([2]), Ordering::Equal),
            ("nulls_tie", None, json!(null), json!(null), Ordering::Equal),
            ("plain_strings_order_by_code_point", None, json!("9.5"), json!("10"), Ordering::Greater),
            ("decimals_order_by_value", decimal, json!("9.5"), json!("10"), Ordering::Less),
            ("decimal_spellings_of_one_value_tie", decimal, json!("9.50"), json!("9.5"), Ordering::Equal),
            ("decimal_text_before_other_text", decimal, json!("100"), json!("abc"), Ordering::Less),
            ("other_text_by_code_point", decimal, json!("b"), json!("a"), Ordering::Greater),
            ("the_type_order_stays_under_the_collation", decimal, json!(1), json!("0"), Ordering::Less),
            ("numbers_stay_numeric_under_the_collation", decimal, json!(2), json!(10), Ordering::Less),
        ];
        for (name, collation, a, b, want) in within {
            assert_eq!(compare_values(collation, &a, &b), want, "{name}");
            assert_eq!(compare_values(collation, &b, &a), want.reverse(), "{name} reversed");
        }
    }

    /// One case per branch of [`query`]: the selection, the keyset, the paging and the `next` rule.
    #[test]
    fn query_selects_orders_pages_and_mints_next() {
        let after_a = encode_cursor(&json!({"id": "a", "n": 1}), &keys(&[("n", false)])).unwrap();
        let after_b = encode_cursor(&json!({"id": "b", "n": 2}), &keys(&[("n", false)])).unwrap();
        // (name, spec, rows, the row `next` resumes after)
        let cases: Vec<(&str, Value, Vec<&str>, Option<&str>)> = vec![
            ("no_sort_keeps_input_order", json!({}), vec!["c", "a", "b", "d"], None),
            // Rows follow, but there is no keyset to resume: an unsorted query pages by offset.
            ("an_unsorted_page_mints_no_cursor", json!({"limit": 2}), vec!["c", "a"], None),
            ("sort_ascending_missing_path_last", json!({"sort": [{"path": "n"}]}), vec!["a", "b", "c", "d"], None),
            ("sort_descending_reverses", json!({"sort": [{"path": "n", "desc": true}]}), vec!["d", "c", "b", "a"], None),
            (
                "filter_then_sort",
                json!({"filter": {"compare": {"path": "s", "op": "eq", "value": "open"}}, "sort": [{"path": "n"}]}),
                vec!["b", "c", "d"],
                None,
            ),
            ("a_full_page_with_rows_after_it_mints_next", json!({"sort": [{"path": "n"}], "limit": 2}), vec!["a", "b"], Some("b")),
            ("offset_and_limit", json!({"sort": [{"path": "n"}], "offset": 1, "limit": 2}), vec!["b", "c"], Some("c")),
            // Exactly the last rows: nothing follows, so no empty page is promised.
            ("a_page_ending_at_the_last_row_mints_none", json!({"sort": [{"path": "n"}], "offset": 2, "limit": 2}), vec!["c", "d"], None),
            ("offset_past_the_end_is_empty", json!({"sort": [{"path": "n"}], "offset": 9}), vec![], None),
            // Rows follow an empty page, but there is no last row to resume after.
            ("zero_limit_is_empty", json!({"sort": [{"path": "n"}], "limit": 0}), vec![], None),
            ("after_resumes_strictly_after", json!({"sort": [{"path": "n"}], "after": after_a}), vec!["b", "c", "d"], None),
            ("after_and_limit_mint_the_next_cursor", json!({"sort": [{"path": "n"}], "after": after_a, "limit": 1}), vec!["b"], Some("b")),
            // A cursor names a position, not a row: resuming after a deleted row still works.
            ("after_a_row_no_longer_present", json!({"sort": [{"path": "n"}], "after": after_b}), vec!["c", "d"], None),
        ];
        for (name, spec, want, next_after) in cases {
            let sort: Vec<SortKey> = serde_json::from_value(spec.get("sort").cloned().unwrap_or(json!([]))).unwrap();
            let page = run(spec, docs()).unwrap();
            assert_eq!(ids(&page.rows), want, "{name}");
            let want_next = next_after.map(|id| {
                let row = docs().into_iter().find(|d| d["id"] == id).unwrap();
                encode_cursor(&row, &sort).unwrap()
            });
            assert_eq!(page.next, want_next, "{name}: next");
        }
    }

    /// Every way the applier refuses, each as the kernel's `codec` error.
    #[test]
    fn query_refusals() {
        let sorted = keys(&[("n", false)]);
        let cursor = encode_cursor(&json!({"id": "a", "n": 1}), &sorted).unwrap();
        let tied_without_ids = vec![json!({"k": 1}), json!({"k": 1}), json!({"k": 1})];
        // Object ids compare by rank only, so these three tie on the tiebreak as null ids would.
        let tied_object_ids: Vec<Value> = (0..3).map(|n| json!({"id": {"tenant": "t1", "n": n}, "k": 1})).collect();
        let wide = vec![json!({"id": "a", "n": 9_007_199_254_740_992u64}), json!({"id": "b", "n": u64::MAX})];
        let wide_document = br#"[{"id": "a", "n": 18446744073709551617}]"#;
        let cases: Vec<(&str, Value, Vec<Value>, &str)> = vec![
            ("a_cursor_without_a_sort", json!({"after": cursor}), docs(), "takes no cursor"),
            ("a_cursor_under_another_sort", json!({"sort": [{"path": "s"}], "after": cursor}), docs(), "another sort"),
            ("a_cursor_under_another_direction", json!({"sort": [{"path": "n", "desc": true}], "after": cursor}), docs(), "another sort"),
            ("a_sort_path_with_an_empty_segment", json!({"sort": [{"path": "a..b"}]}), docs(), "empty segment"),
            ("a_filter_the_kernel_refuses", json!({"filter": {"compare": {"path": "n", "op": "eq", "value": null}}}), docs(), "null comparand"),
            // Tied rows with no id: minting `next` would drop the rest of the tie, so it is refused.
            ("next_from_a_row_without_an_id", json!({"sort": [{"path": "k"}], "limit": 1}), tied_without_ids, "string or number `id`"),
            ("next_from_a_row_with_an_object_id", json!({"sort": [{"path": "k"}], "limit": 1}), tied_object_ids, "string or number `id`"),
            // A page ending on a sort value at 2^53: the RFC 8785 cursor could not tell it from
            // 2^53 + 1, so it would resume at the wrong row. One below the boundary pages (`cursor_branches`).
            ("next_from_a_sort_value_at_2_pow_53", json!({"sort": [{"path": "n"}], "limit": 1}), wide, "outside -(2^53-1)..=2^53-1"),
        ];
        for (name, spec, docs, needle) in cases {
            let error = run(spec, docs).expect_err(name);
            assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
            assert!(error.message.contains(needle), "{name}: {error}");
        }
        // A document integer wider than 64 bits would be read as the nearest float and come back
        // rounded, so the JSON boundary refuses it as it refuses one in the spec.
        let error = query_json(b"{}", wide_document).expect_err("a wide document integer");
        assert_eq!(error.to_string(), "codec: integer at byte 18 is outside the 64-bit range");
    }

    #[test]
    fn keyset_paging_walks_every_row_exactly_once() {
        // One row per page, resuming from the previous page's `next`: no row may be dropped or repeated.
        let tied = vec![json!({"id": "a", "bal": 100}), json!({"id": "b", "bal": 100}), json!({"id": "c", "bal": 100})];
        // Whole-number doubles past 2^53, which RFC 8785 writes as integer digits: tied, then one
        // whose digits sit above the double, then a row with no value at all.
        let wide_doubles = vec![
            json!({"id": "a", "n": 5.808820077685227e16}),
            json!({"id": "b", "n": 5.808820077685227e16}),
            json!({"id": "c", "n": 1.1108287563226614e18}),
            json!({"id": "d"}),
        ];
        type Case = (&'static str, Vec<Value>, Vec<SortKey>, Vec<&'static str>);
        let cases: Vec<Case> = vec![
            ("unique_sort_key", docs(), keys(&[("n", false)]), vec!["a", "b", "c", "d"]),
            ("descending_sort_key", docs(), keys(&[("n", true)]), vec!["d", "c", "b", "a"]),
            ("tied_sort_key_broken_by_id", tied, keys(&[("bal", false)]), vec!["a", "b", "c"]),
            ("sort_already_ends_in_id", docs(), keys(&[("n", false), ("id", false)]), vec!["a", "b", "c", "d"]),
            ("sort_ends_in_id_descending", docs(), keys(&[("id", true)]), vec!["d", "c", "b", "a"]),
            ("whole_number_doubles_past_2_pow_53", wide_doubles, keys(&[("n", false)]), vec!["a", "b", "c", "d"]),
        ];
        for (name, documents, sort, want) in cases {
            let mut seen: Vec<String> = Vec::new();
            let mut after: Option<String> = None;
            // One row per page: a walk longer than the rows plus one repeated a row.
            for _ in 0..=want.len() {
                let spec = Query { sort: sort.clone(), limit: Some(1), after: after.clone(), ..Default::default() };
                let page = query(&spec, documents.clone()).unwrap();
                seen.extend(page.rows.iter().map(|r| r["id"].as_str().unwrap().to_string()));
                match page.next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            assert_eq!(seen, want, "{name}");
        }
    }

    /// The fingerprint binds the effective sort: the same order spelled two ways is one sort, and
    /// every other change is another.
    #[test]
    fn a_cursor_replays_only_under_its_own_sort() {
        let row = json!({"id": "a", "n": 1, "s": "x"});
        let minted = encode_cursor(&row, &keys(&[("n", false)])).unwrap();
        let cases: [(&str, Vec<SortKey>, bool); 6] = [
            ("the_same_sort", keys(&[("n", false)]), true),
            ("the_tiebreak_spelled_out", keys(&[("n", false), ("id", false)]), true),
            ("another_key", keys(&[("s", false)]), false),
            ("another_direction", keys(&[("n", true)]), false),
            ("one_key_more", keys(&[("n", false), ("s", false)]), false),
            ("the_tiebreak_descending", keys(&[("n", false), ("id", true)]), false),
        ];
        for (name, sort, accepted) in cases {
            assert_eq!(decode_cursor(&minted, &sort).is_ok(), accepted, "{name}");
        }
    }

    #[test]
    fn cursor_branches() {
        let sort = keys(&[("n", false)]);
        let wire = |value: Value| B64.encode(serde_json::to_vec(&value).unwrap());
        let print = fingerprint(&effective_sort(&sort)).unwrap();
        // encode
        assert_eq!(
            decode_cursor(&encode_cursor(&json!({"id": "a", "n": 1}), &sort).unwrap(), &sort),
            Ok(vec![json!(1), json!("a")]),
            "the tuple is one value per effective key, tiebreak last"
        );
        assert!(encode_cursor(&json!({"id": "a"}), &[]).is_err(), "an unsorted query mints no cursor");
        assert!(encode_cursor(&json!({"n": 1}), &sort).is_err(), "a missing id");
        assert!(encode_cursor(&json!({"n": 1, "id": null}), &sort).is_err(), "a null id");
        assert!(encode_cursor(&json!({"n": 1, "id": ["a"]}), &sort).is_err(), "an array id");
        assert!(encode_cursor(&json!({"n": 1, "id": {"k": "a"}}), &sort).is_err(), "an object id");
        assert!(encode_cursor(&json!({"n": 1, "id": 7}), &sort).is_ok(), "a number id");
        // A sort value nested to the canonical form's depth bound, which the cursor's own two levels
        // then exceed: refused as `codec`, never a cursor no reader could parse.
        let deep = (0..127).fold(json!(1), |inner, _| json!([inner]));
        let error = encode_cursor(&json!({"id": "a", "n": deep}), &sort).expect_err("too deep");
        assert!(error.message.contains("recursion limit exceeded"), "{error}");
        // The widest integers the cursor text keeps exact round-trip; one past them is refused.
        let edge = json!({"id": "a", "n": -9_007_199_254_740_991i64});
        assert_eq!(decode_cursor(&encode_cursor(&edge, &sort).unwrap(), &sort), Ok(vec![json!(-9_007_199_254_740_991i64), json!("a")]));
        let error = encode_cursor(&json!({"id": "a", "n": -9_007_199_254_740_992i64}), &sort).expect_err("at 2^53");
        assert!(error.message.contains("outside -(2^53-1)..=2^53-1"), "{error}");
        // A whole-number double past the safe range is written as integer digits that are not its
        // exact value (`58088200776852270`); it decodes as the double, nested values included.
        let double = json!({"id": "a", "n": 5.808820077685227e16});
        assert_eq!(decode_cursor(&encode_cursor(&double, &sort).unwrap(), &sort), Ok(vec![json!(5.808820077685227e16), json!("a")]));
        let nested = json!({"id": "a", "n": {"k": [-1.1108287563226614e18]}});
        assert_eq!(decode_cursor(&encode_cursor(&nested, &sort).unwrap(), &sort), Ok(vec![nested["n"].clone(), json!("a")]));
        // decode: every refusal arm
        let cases: [(&str, String, Vec<SortKey>, &str); 6] = [
            ("unsorted", wire(json!({"s": print, "v": [1, "a"]})), vec![], "takes no cursor"),
            ("not_base64url", "!!".to_owned(), sort.clone(), "not base64url"),
            ("standard_base64_padding_is_refused", "e30=".to_owned(), sort.clone(), "not base64url"),
            ("not_a_cursor", wire(json!([1, "a"])), sort.clone(), "not a keyset cursor"),
            ("an_unknown_field", wire(json!({"s": print, "v": [1, "a"], "x": 0})), sort.clone(), "not a keyset cursor"),
            ("forged_arity", wire(json!({"s": print, "v": [1]})), sort.clone(), "carries 1 values for 2 sort keys"),
        ];
        for (name, cursor, sort, needle) in cases {
            let error = decode_cursor(&cursor, &sort).expect_err(name);
            assert!(error.to_string().starts_with("codec: ") && error.message.contains(needle), "{name}: {error}");
        }
    }

    /// Both directions of the one shape, per field: `skip_serializing_if` decides what a spec emits,
    /// `default` decides what a missing field means on the way back in. Every skip branch is covered
    /// both ways, and each case round-trips, so the two attribute sets cannot drift apart.
    #[test]
    fn spec_omits_unset_fields_and_round_trips() {
        let full = Query {
            filter: Some(filter::eq("status", json!("open"))),
            sort: vec![SortKey::new("name", true)],
            offset: 2,
            limit: Some(5),
            after: Some("e30".to_string()),
        };
        let cases: Vec<(&str, Query, Value)> = vec![
            ("every_field_unset_emits_an_empty_spec", Query::default(), json!({})),
            (
                "an_empty_sort_is_omitted_but_a_set_one_is_not",
                Query { sort: full.sort.clone(), ..Default::default() },
                json!({"sort": [{"path": "name", "desc": true}]}),
            ),
            ("a_nonzero_offset_is_emitted", Query { offset: 3, ..Default::default() }, json!({"offset": 3})),
            (
                "every_field_set_emits_every_field",
                full,
                json!({
                    "filter": {"compare": {"path": "status", "op": "eq", "value": "open"}},
                    "sort": [{"path": "name", "desc": true}],
                    "offset": 2,
                    "limit": 5,
                    "after": "e30",
                }),
            ),
        ];
        for (name, query, wire) in cases {
            assert_eq!(serde_json::to_value(&query).unwrap(), wire, "{name}");
            assert_eq!(serde_json::from_value::<Query>(wire).unwrap(), query, "{name} round-trip");
        }
    }

    #[test]
    fn tiebreak_is_appended_once() {
        assert_eq!(effective_sort(&[]), vec![], "an unsorted query takes no tiebreak");
        assert_eq!(effective_sort(&keys(&[("n", false)])), keys(&[("n", false), ("id", false)]));
        assert_eq!(effective_sort(&keys(&[("id", true)])), keys(&[("id", true)]), "a terminal id key is kept as-is");
    }

    /// The JSON boundary reads the spec strictly: a typo or a wide integer is an error, not a query
    /// that silently means something else.
    #[test]
    fn malformed_input_is_rejected() {
        let documents = serde_json::to_vec(&docs()).unwrap();
        let cases: Vec<(&str, &[u8], &[u8])> = vec![
            ("query_is_not_json", b"{", &documents),
            ("documents_are_not_json", br#"{"sort": [{"path": "n"}]}"#, b"{"),
            ("documents_are_not_an_array", br#"{"sort": [{"path": "n"}]}"#, b"{}"),
            ("a_typo_in_the_spec", br#"{"limt": 1}"#, &documents),
            ("a_typo_in_a_sort_key", br#"{"sort": [{"path": "n", "dsc": true}]}"#, &documents),
            ("an_integer_wider_than_64_bits", br#"{"filter": {"compare": {"path": "n", "op": "eq", "value": 1180591620717411303424}}}"#, &documents),
        ];
        for (name, spec, documents) in cases {
            assert!(query_json(spec, documents).is_err(), "{name}");
        }
        let page = query_json(br#"{"sort": [{"path": "n"}], "limit": 1}"#, &documents).unwrap();
        let next = encode_cursor(&json!({"id": "a", "n": 1}), &keys(&[("n", false)])).unwrap();
        assert_eq!(page, format!(r#"{{"rows":[{{"id":"a","n":1,"s":"closed"}}],"next":"{next}"}}"#));
    }

    #[test]
    fn cursor_boundary_round_trips() {
        let sort = br#"[{"path":"n"}]"#;
        let doc = br#"{"id":"a","n":1}"#;
        let cursor = encode_cursor_json(doc, sort).unwrap();
        assert!(!cursor.contains(['+', '/', '=']), "URL-safe and unpadded: {cursor}");
        assert_eq!(decode_cursor_json(cursor.as_bytes(), sort).unwrap(), r#"[1,"a"]"#);
        assert_eq!(effective_sort_json(sort).unwrap(), r#"[{"path":"n","desc":false},{"path":"id","desc":false}]"#);
        assert!(encode_cursor_json(b"not json", sort).is_err());
        assert!(encode_cursor_json(br#"{"id":"a","n":18446744073709551617}"#, sort).is_err(), "a wide document integer");
        assert!(encode_cursor_json(doc, b"not json").is_err());
        assert!(decode_cursor_json(b"not base64!", sort).is_err());
        assert!(decode_cursor_json(&[0xff], sort).is_err(), "a cursor is text");
        assert!(decode_cursor_json(cursor.as_bytes(), b"not json").is_err());
        assert!(effective_sort_json(b"not json").is_err());
    }

    /// One change between two pages of a paging walk.
    #[derive(Debug, Clone)]
    enum Change {
        Insert(Option<Value>),
        Delete(usize),
    }

    /// A sort value: a small integer, so rows tie often, or a whole-number double past 2^53, which
    /// the cursor carries as integer digits.
    fn sort_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            3 => (0u8..4).prop_map(Value::from),
            1 => prop::sample::select(vec![5.808820077685227e16, 1.1108287563226614e18]).prop_map(Value::from),
        ]
    }

    fn change() -> impl Strategy<Value = Change> {
        prop_oneof![
            (prop::option::weighted(0.8, sort_value())).prop_map(Change::Insert),
            (0usize..64).prop_map(Change::Delete),
        ]
    }

    fn sorts() -> impl Strategy<Value = Vec<SortKey>> {
        prop_oneof![
            Just(keys(&[("k", false)])),
            Just(keys(&[("k", true)])),
            Just(keys(&[("k", false), ("id", true)])),
            Just(keys(&[("id", false)])),
        ]
    }

    proptest! {
        /// Every finite double a row sorts by comes back from its cursor as that same double, so the
        /// walk resumes exactly at its row.
        #[test]
        fn a_double_sort_value_round_trips_through_the_cursor(
            n in prop::num::f64::NORMAL | prop::num::f64::SUBNORMAL | prop::num::f64::ZERO,
        ) {
            let sort = keys(&[("n", false)]);
            let tuple = decode_cursor(&encode_cursor(&json!({"id": "a", "n": n}), &sort).unwrap(), &sort).unwrap();
            prop_assert_eq!(tuple.first().and_then(Value::as_f64), Some(n));
        }

        /// Keyset paging while other writers insert and delete rows between pages (rows never change
        /// their sort key, the stable-sort premise). No row is returned twice, every row present for
        /// the whole walk is returned exactly once, and rows arrive in the sort's order.
        #[test]
        fn paging_under_concurrent_inserts_and_deletes_never_skips_or_repeats(
            initial in prop::collection::vec(prop::option::weighted(0.8, sort_value()), 0..12),
            sort in sorts(),
            limit in 1usize..4,
            changes in prop::collection::vec(prop::collection::vec(change(), 0..3), 0..40),
        ) {
            let row = |id: usize, k: Option<Value>| match k {
                Some(k) => json!({"id": format!("r{id:03}"), "k": k}),
                None => json!({"id": format!("r{id:03}")}),
            };
            let mut table: Vec<Value> = initial.into_iter().enumerate().map(|(i, k)| row(i, k)).collect();
            let mut next_id = table.len();
            let mut survivors: std::collections::BTreeSet<String> =
                table.iter().map(|r| r["id"].as_str().unwrap().to_owned()).collect();
            let mut seen: Vec<Value> = Vec::new();
            let mut after: Option<String> = None;
            // Every page holds at least one row no earlier page held, so the walk ends within this
            // many pages; a cursor that resumes at or before its own row would loop forever.
            let mut budget = table.len() + changes.iter().map(Vec::len).sum::<usize>() + 1;
            let mut rounds = changes.into_iter();
            loop {
                prop_assert!(budget > 0, "paging never reached the last page: {} rows seen", seen.len());
                budget -= 1;
                let spec = Query { sort: sort.clone(), limit: Some(limit), after: after.clone(), ..Default::default() };
                let page = query(&spec, table.clone()).unwrap();
                seen.extend(page.rows);
                let Some(next) = page.next else { break };
                after = Some(next);
                for change in rounds.next().unwrap_or_default() {
                    match change {
                        Change::Insert(k) => {
                            table.push(row(next_id, k));
                            next_id += 1;
                        }
                        Change::Delete(i) if !table.is_empty() => {
                            let gone = table.remove(i % table.len());
                            survivors.remove(gone["id"].as_str().unwrap());
                        }
                        Change::Delete(_) => {}
                    }
                }
            }
            let ids: Vec<&str> = seen.iter().map(|r| r["id"].as_str().unwrap()).collect();
            let unique: std::collections::BTreeSet<&str> = ids.iter().copied().collect();
            prop_assert_eq!(unique.len(), ids.len(), "a row was returned twice: {:?}", ids);
            for id in &survivors {
                prop_assert!(unique.contains(id.as_str()), "{} was present throughout and skipped: {:?}", id, ids);
            }
            let effective = effective_sort(&sort);
            for pair in seen.windows(2) {
                if let [a, b] = pair {
                    prop_assert_eq!(compare(&effective, a, b), Ordering::Less, "out of order: {} then {}", a, b);
                }
            }
        }
    }
}
