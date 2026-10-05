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
//! a type: numbers numerically, strings by Unicode code point, bools `false < true` — the order a
//! range filter gives each pair. A **missing** path and an explicit `null`
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

use crate::collation::{self, Collation};
use crate::filter::{self, Filter};
use crate::json_numbers::MAX_SAFE_INTEGER;
use crate::value::{check_path, parse_strict, read_json, resolve};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use bigdecimal::BigDecimal;
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
        SortKey {
            path: path.into(),
            desc,
            collation: None,
        }
    }

    /// A key over decimal text, ordered by exact value ([`Collation::Decimal`]).
    pub fn decimal(path: impl Into<String>, desc: bool) -> SortKey {
        SortKey {
            path: path.into(),
            desc,
            collation: Some(Collation::Decimal),
        }
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
        self.sort.iter().try_for_each(|key| check_path(&key.path))?;
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

impl Page {
    /// The page as the text every JSON boundary returns: `{"rows": [...], "next": cursor|null}`.
    pub fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(Error::codec)
    }
}

static NULL: Value = Value::Null;

/// The value at `path`, with a missing path reading as `null` so it sorts last.
fn at<'a>(doc: &'a Value, path: &str) -> &'a Value {
    resolve(doc, path).unwrap_or(&NULL)
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
/// A row a cursor is minted from must carry a string or a number here; a store reads every row with its
/// key under this name ([`keyed_document`]).
pub const TIEBREAK_PATH: &str = "id";

/// The keys a page is actually ordered by: `sort` with an ascending [`TIEBREAK_PATH`] key appended
/// unless it is already the last key, with no collation. A collated id key is no tiebreak: under the
/// decimal collation `"7"` and `"07"` tie. Empty for an empty sort: an unsorted query keeps input
/// order and takes no cursor.
///
/// A backend translating a [`Query`] into its own dialect orders by exactly these keys, tiebreak
/// included, or its pages disagree with the kernel at every tie.
pub fn effective_sort(sort: &[SortKey]) -> Vec<SortKey> {
    let mut keys = sort.to_vec();
    let ends_on_tiebreak = keys
        .last()
        .is_some_and(|k| k.path == TIEBREAK_PATH && k.collation.is_none());
    if !keys.is_empty() && !ends_on_tiebreak {
        keys.push(SortKey::new(TIEBREAK_PATH, false));
    }
    keys
}

/// The values `doc` is ordered by under `keys`, one per key, a missing path reading as `null`: what a
/// sort compares and what a cursor carries.
fn sort_tuple<'a>(keys: &'a [SortKey], doc: &'a Value) -> impl Iterator<Item = &'a Value> + 'a {
    keys.iter().map(move |key| at(doc, &key.path))
}

/// Compare two sort tuples under `keys`, each key under its collation and direction (missing/null sort
/// last in ascending order).
fn compare_tuples<'a>(
    keys: &[SortKey],
    a: impl IntoIterator<Item = &'a Value>,
    b: impl IntoIterator<Item = &'a Value>,
) -> Ordering {
    keys.iter()
        .zip(a.into_iter().zip(b))
        .fold(Ordering::Equal, |order, (key, (a, b))| {
            order.then_with(|| directed(key, collation::total_cmp(key.collation, a, b)))
        })
}

/// Compare two documents under the given sort keys: the order [`sort_rows`] sorts by, compared afresh.
#[cfg(test)]
fn compare(keys: &[SortKey], a: &Value, b: &Value) -> Ordering {
    compare_tuples(keys, sort_tuple(keys, a), sort_tuple(keys, b))
}

/// Sort `rows` under `keys`, stable. Each row's decimal readings are taken once before the sort, so a
/// decimal key parses each value once rather than twice per comparison.
fn sort_rows(keys: &[SortKey], rows: &mut Vec<Value>) {
    let mut read: Vec<(Vec<Option<BigDecimal>>, Value)> = std::mem::take(rows)
        .into_iter()
        .map(|row| {
            (
                sort_tuple(keys, &row)
                    .zip(keys)
                    .map(|(value, key)| collation::read_decimal(key.collation, value))
                    .collect(),
                row,
            )
        })
        .collect();
    read.sort_by(|(a_read, a), (b_read, b)| {
        keys.iter()
            .zip(a_read.iter().zip(b_read))
            .fold(Ordering::Equal, |order, (key, (a_decimal, b_decimal))| {
                order.then_with(|| {
                    let (a, b) = (
                        (at(a, &key.path), a_decimal.as_ref()),
                        (at(b, &key.path), b_decimal.as_ref()),
                    );
                    directed(key, collation::total_cmp_read(key.collation, a, b))
                })
            })
    });
    rows.extend(read.into_iter().map(|(_, row)| row));
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
/// string or a number (a missing or null one, an array or an object each compare equal to their own
/// kind, so rows tied on every other key would tie on the tiebreak too, and paging past them would
/// drop the rest of the tie); and a sort value that is an integer outside `-(2^53-1)..=2^53-1`, which
/// the canonical form would round onto its neighbour and so resume at the wrong row.
pub fn encode_cursor(doc: &Value, sort: &[SortKey]) -> Result<String, Error> {
    let keys = effective_sort(sort);
    if keys.is_empty() {
        return Err(unsorted_cursor());
    }
    let values: Vec<Value> = sort_tuple(&keys, doc).cloned().collect();
    if !matches!(at(doc, TIEBREAK_PATH), Value::String(_) | Value::Number(_)) {
        return Err(Error::codec(format_args!(
            "a keyset cursor needs a string or number `{TIEBREAK_PATH}` on the row it resumes after"
        )));
    }
    let json = serde_json::to_vec(&CursorWire {
        s: fingerprint(&keys)?,
        v: values,
    })
    .map_err(Error::codec)?;
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
    let bytes = B64
        .decode(cursor)
        .map_err(|e| Error::codec(format_args!("cursor is not base64url: {e}")))?;
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
    compare_tuples(keys, sort_tuple(keys, doc), tuple) == Ordering::Greater
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
    sort_rows(&sort, &mut rows);

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
/// 64 bits, an unknown field, or a spec [`Query::validate`] refuses is the terminal `codec` error. For a
/// store that holds its documents itself and must refuse exactly the specs the kernel refuses.
pub fn query_spec(spec: &[u8]) -> Result<Query, Error> {
    let query: Query = parse_strict(spec)?;
    query.validate()?;
    Ok(query)
}

/// JSON boundary for [`query`]: the spec and a JSON array of documents in, the [`Page`] as
/// `{"rows": [...], "next": cursor|null}` out. The spec is read strictly (see [`query_spec`]), and so
/// are the documents: an integer literal wider than 64 bits is refused.
pub fn query_json(spec: &[u8], docs: &[u8]) -> Result<String, Error> {
    // Not `query_spec`: `query` validates the spec itself.
    let spec: Query = parse_strict(spec)?;
    let docs: Vec<Value> = parse_strict(docs)?;
    query(&spec, docs)?.to_json()
}

/// A read-model document as a store keeps it: a JSON object, because the query reads the row key from
/// its [`TIEBREAK_PATH`] field and a store that keys rows itself injects the key there. A scalar, an
/// array or `null` could be saved and never queried, so it is refused as `codec`, naming the row `id`.
/// Every store's save runs this, or [`document_json`] for a document held as text.
pub fn document(id: &str, value: Value) -> Result<serde_json::Map<String, Value>, Error> {
    match value {
        Value::Object(fields) => Ok(fields),
        _ => Err(Error::codec(format_args!(
            "projection {id} is not a JSON object: the query reads each row's key from its `{TIEBREAK_PATH}` field"
        ))),
    }
}

/// A stored read-model `document` as a projection store reads it back, by `get` and `get_many` alike: its
/// row key `id` under [`TIEBREAK_PATH`]. The key is authoritative, so it overwrites an `"id"` the document
/// carries. The query sorts its keyset tiebreak on that field and mints every cursor from it, so a store
/// that read rows without it would page only while each writer copied the key in, and a filter on `id`
/// would match a stale copy. Pinned by `testdata/golden/projection_document.json`.
pub fn keyed_document(id: &str, mut document: serde_json::Map<String, Value>) -> serde_json::Map<String, Value> {
    document.insert(TIEBREAK_PATH.to_owned(), Value::String(id.to_owned()));
    document
}

/// [`keyed_document`] for a document held as JSON text: read by [`document_json`], so a document a
/// store's save refuses is refused here too, and written back as JSON text.
pub fn keyed_document_json(id: &str, json: &[u8]) -> Result<String, Error> {
    serde_json::to_string(&keyed_document(id, document_json(id, json)?)).map_err(Error::codec)
}

/// [`document`] for a document held as JSON text, read strictly: an integer literal wider than 64 bits
/// or text that is not JSON is refused as `codec` too.
pub fn document_json(id: &str, json: &[u8]) -> Result<serde_json::Map<String, Value>, Error> {
    document(id, read_json(&format!("projection {id}"), json)?)
}

/// Boundary helper: decode a cursor into its value tuple under `sort` (JSON in, JSON array out).
pub fn decode_cursor_json(cursor: &[u8], sort: &[u8]) -> Result<String, Error> {
    let cursor = std::str::from_utf8(cursor).map_err(Error::codec)?;
    let keys: Vec<SortKey> = parse_strict(sort)?;
    let tuple = decode_cursor(cursor, &keys)?;
    serde_json::to_string(&tuple).map_err(Error::codec)
}

/// Boundary helper: the sort keys a page is actually ordered by, tiebreak included (JSON in, out).
pub fn effective_sort_json(sort: &[u8]) -> Result<String, Error> {
    let keys: Vec<SortKey> = parse_strict(sort)?;
    serde_json::to_string(&effective_sort(&keys)).map_err(Error::codec)
}

/// Boundary helper: mint the cursor that resumes after a document under the sort keys (both JSON).
pub fn encode_cursor_json(doc: &[u8], sort: &[u8]) -> Result<String, Error> {
    let doc: Value = parse_strict(doc)?;
    let keys: Vec<SortKey> = parse_strict(sort)?;
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

    /// One case per branch of [`query`]: the selection, the keyset, the paging and the `next` rule.
    #[test]
    fn query_selects_orders_pages_and_mints_next() {
        let after_a = encode_cursor(&json!({"id": "a", "n": 1}), &keys(&[("n", false)])).unwrap();
        let after_b = encode_cursor(&json!({"id": "b", "n": 2}), &keys(&[("n", false)])).unwrap();
        // (name, spec, rows, the row `next` resumes after)
        let cases: Vec<(&str, Value, Vec<&str>, Option<&str>)> = vec![
            ("no_sort_keeps_input_order", json!({}), vec!["c", "a", "b", "d"], None),
            // Rows follow, but there is no keyset to resume: an unsorted query pages by offset.
            (
                "an_unsorted_page_mints_no_cursor",
                json!({"limit": 2}),
                vec!["c", "a"],
                None,
            ),
            (
                "sort_ascending_missing_path_last",
                json!({"sort": [{"path": "n"}]}),
                vec!["a", "b", "c", "d"],
                None,
            ),
            (
                "sort_descending_reverses",
                json!({"sort": [{"path": "n", "desc": true}]}),
                vec!["d", "c", "b", "a"],
                None,
            ),
            (
                "filter_then_sort",
                json!({"filter": {"compare": {"path": "s", "op": "eq", "value": "open"}}, "sort": [{"path": "n"}]}),
                vec!["b", "c", "d"],
                None,
            ),
            (
                "a_full_page_with_rows_after_it_mints_next",
                json!({"sort": [{"path": "n"}], "limit": 2}),
                vec!["a", "b"],
                Some("b"),
            ),
            (
                "offset_and_limit",
                json!({"sort": [{"path": "n"}], "offset": 1, "limit": 2}),
                vec!["b", "c"],
                Some("c"),
            ),
            // Exactly the last rows: nothing follows, so no empty page is promised.
            (
                "a_page_ending_at_the_last_row_mints_none",
                json!({"sort": [{"path": "n"}], "offset": 2, "limit": 2}),
                vec!["c", "d"],
                None,
            ),
            (
                "offset_past_the_end_is_empty",
                json!({"sort": [{"path": "n"}], "offset": 9}),
                vec![],
                None,
            ),
            // Rows follow an empty page, but there is no last row to resume after.
            (
                "zero_limit_is_empty",
                json!({"sort": [{"path": "n"}], "limit": 0}),
                vec![],
                None,
            ),
            (
                "after_resumes_strictly_after",
                json!({"sort": [{"path": "n"}], "after": after_a}),
                vec!["b", "c", "d"],
                None,
            ),
            (
                "after_and_limit_mint_the_next_cursor",
                json!({"sort": [{"path": "n"}], "after": after_a, "limit": 1}),
                vec!["b"],
                Some("b"),
            ),
            // A cursor names a position, not a row: resuming after a deleted row still works.
            (
                "after_a_row_no_longer_present",
                json!({"sort": [{"path": "n"}], "after": after_b}),
                vec!["c", "d"],
                None,
            ),
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
        let tied_object_ids: Vec<Value> = (0..3)
            .map(|n| json!({"id": {"tenant": "t1", "n": n}, "k": 1}))
            .collect();
        let wide = vec![
            json!({"id": "a", "n": 9_007_199_254_740_992u64}),
            json!({"id": "b", "n": u64::MAX}),
        ];
        let wide_document = br#"[{"id": "a", "n": 18446744073709551617}]"#;
        let cases: Vec<(&str, Value, Vec<Value>, &str)> = vec![
            (
                "a_cursor_without_a_sort",
                json!({"after": cursor}),
                docs(),
                "takes no cursor",
            ),
            (
                "a_cursor_under_another_sort",
                json!({"sort": [{"path": "s"}], "after": cursor}),
                docs(),
                "another sort",
            ),
            (
                "a_cursor_under_another_direction",
                json!({"sort": [{"path": "n", "desc": true}], "after": cursor}),
                docs(),
                "another sort",
            ),
            (
                "a_sort_path_with_an_empty_segment",
                json!({"sort": [{"path": "a..b"}]}),
                docs(),
                "empty segment",
            ),
            (
                "a_filter_the_kernel_refuses",
                json!({"filter": {"compare": {"path": "n", "op": "eq", "value": null}}}),
                docs(),
                "null comparand",
            ),
            // Tied rows with no id: minting `next` would drop the rest of the tie, so it is refused.
            (
                "next_from_a_row_without_an_id",
                json!({"sort": [{"path": "k"}], "limit": 1}),
                tied_without_ids,
                "string or number `id`",
            ),
            (
                "next_from_a_row_with_an_object_id",
                json!({"sort": [{"path": "k"}], "limit": 1}),
                tied_object_ids,
                "string or number `id`",
            ),
            // A page ending on a sort value at 2^53: the RFC 8785 cursor could not tell it from
            // 2^53 + 1, so it would resume at the wrong row. One below the boundary pages (`cursor_branches`).
            (
                "next_from_a_sort_value_at_2_pow_53",
                json!({"sort": [{"path": "n"}], "limit": 1}),
                wide,
                "outside -(2^53-1)..=2^53-1",
            ),
        ];
        for (name, spec, docs, needle) in cases {
            let error = run(spec, docs).expect_err(name);
            assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
            assert!(error.message.contains(needle), "{name}: {error}");
        }
        // A document integer wider than 64 bits would be read as the nearest float and come back
        // rounded, so the JSON boundary refuses it as it refuses one in the spec.
        let error = query_json(b"{}", wide_document).expect_err("a wide document integer");
        assert_eq!(
            error.to_string(),
            "codec: integer at byte 18 is outside the 64-bit range"
        );
    }

    /// A store that reads its own spec refuses what the applier refuses, before it reads a document.
    #[test]
    fn query_spec_branches() {
        let cases: [(&str, &str, Option<&str>); 6] = [
            (
                "a_valid_spec",
                r#"{"filter": {"exists": {"path": "n", "exists": true}}, "sort": [{"path": "n"}]}"#,
                None,
            ),
            ("not_json", "{not json}", Some("key must be a string")),
            (
                "a_wide_integer",
                r#"{"limit": 18446744073709551617}"#,
                Some("outside the 64-bit range"),
            ),
            (
                "a_null_comparand",
                r#"{"filter": {"compare": {"path": "n", "op": "eq", "value": null}}}"#,
                Some("null comparand"),
            ),
            (
                "a_sort_path_with_an_empty_segment",
                r#"{"sort": [{"path": "a..b"}]}"#,
                Some("empty segment"),
            ),
            (
                "a_cursor_without_a_sort",
                r#"{"after": "W10"}"#,
                Some("takes no cursor"),
            ),
        ];
        for (name, spec, want) in cases {
            match (query_spec(spec.as_bytes()), want) {
                (Ok(_), None) => {}
                (Err(error), Some(needle)) => {
                    assert_eq!(error.code, edax_core::ErrorCode::Codec, "{name}");
                    assert!(error.message.contains(needle), "{name}: {error}");
                }
                (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[test]
    fn keyset_paging_walks_every_row_exactly_once() {
        // One row per page, resuming from the previous page's `next`: no row may be dropped or repeated.
        let tied = vec![
            json!({"id": "a", "bal": 100}),
            json!({"id": "b", "bal": 100}),
            json!({"id": "c", "bal": 100}),
        ];
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
            (
                "unique_sort_key",
                docs(),
                keys(&[("n", false)]),
                vec!["a", "b", "c", "d"],
            ),
            (
                "descending_sort_key",
                docs(),
                keys(&[("n", true)]),
                vec!["d", "c", "b", "a"],
            ),
            (
                "tied_sort_key_broken_by_id",
                tied,
                keys(&[("bal", false)]),
                vec!["a", "b", "c"],
            ),
            (
                "sort_already_ends_in_id",
                docs(),
                keys(&[("n", false), ("id", false)]),
                vec!["a", "b", "c", "d"],
            ),
            (
                "sort_ends_in_id_descending",
                docs(),
                keys(&[("id", true)]),
                vec!["d", "c", "b", "a"],
            ),
            (
                "whole_number_doubles_past_2_pow_53",
                wide_doubles,
                keys(&[("n", false)]),
                vec!["a", "b", "c", "d"],
            ),
        ];
        for (name, documents, sort, want) in cases {
            let mut seen: Vec<String> = Vec::new();
            let mut after: Option<String> = None;
            // One row per page: a walk longer than the rows plus one repeated a row.
            for _ in 0..=want.len() {
                let spec = Query {
                    sort: sort.clone(),
                    limit: Some(1),
                    after: after.clone(),
                    ..Default::default()
                };
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
        assert!(
            encode_cursor(&json!({"id": "a"}), &[]).is_err(),
            "an unsorted query mints no cursor"
        );
        assert!(encode_cursor(&json!({"n": 1}), &sort).is_err(), "a missing id");
        assert!(encode_cursor(&json!({"n": 1, "id": null}), &sort).is_err(), "a null id");
        assert!(
            encode_cursor(&json!({"n": 1, "id": ["a"]}), &sort).is_err(),
            "an array id"
        );
        assert!(
            encode_cursor(&json!({"n": 1, "id": {"k": "a"}}), &sort).is_err(),
            "an object id"
        );
        assert!(encode_cursor(&json!({"n": 1, "id": 7}), &sort).is_ok(), "a number id");
        // A sort value nested to the canonical form's depth bound, which the cursor's own two levels
        // then exceed: refused as `codec`, never a cursor no reader could parse.
        let deep = (0..127).fold(json!(1), |inner, _| json!([inner]));
        let error = encode_cursor(&json!({"id": "a", "n": deep}), &sort).expect_err("too deep");
        assert!(error.message.contains("recursion limit exceeded"), "{error}");
        // The widest integers the cursor text keeps exact round-trip; one past them is refused.
        let edge = json!({"id": "a", "n": -9_007_199_254_740_991i64});
        assert_eq!(
            decode_cursor(&encode_cursor(&edge, &sort).unwrap(), &sort),
            Ok(vec![json!(-9_007_199_254_740_991i64), json!("a")])
        );
        let error = encode_cursor(&json!({"id": "a", "n": -9_007_199_254_740_992i64}), &sort).expect_err("at 2^53");
        assert!(error.message.contains("outside -(2^53-1)..=2^53-1"), "{error}");
        // A whole-number double past the safe range is written as integer digits that are not its
        // exact value (`58088200776852270`); it decodes as the double, nested values included.
        let double = json!({"id": "a", "n": 5.808820077685227e16});
        assert_eq!(
            decode_cursor(&encode_cursor(&double, &sort).unwrap(), &sort),
            Ok(vec![json!(5.808820077685227e16), json!("a")])
        );
        let nested = json!({"id": "a", "n": {"k": [-1.1108287563226614e18]}});
        assert_eq!(
            decode_cursor(&encode_cursor(&nested, &sort).unwrap(), &sort),
            Ok(vec![nested["n"].clone(), json!("a")])
        );
        // decode: every refusal arm
        let cases: [(&str, String, Vec<SortKey>, &str); 6] = [
            (
                "unsorted",
                wire(json!({"s": print, "v": [1, "a"]})),
                vec![],
                "takes no cursor",
            ),
            ("not_base64url", "!!".to_owned(), sort.clone(), "not base64url"),
            (
                "standard_base64_padding_is_refused",
                "e30=".to_owned(),
                sort.clone(),
                "not base64url",
            ),
            (
                "not_a_cursor",
                wire(json!([1, "a"])),
                sort.clone(),
                "not a keyset cursor",
            ),
            (
                "an_unknown_field",
                wire(json!({"s": print, "v": [1, "a"], "x": 0})),
                sort.clone(),
                "not a keyset cursor",
            ),
            (
                "forged_arity",
                wire(json!({"s": print, "v": [1]})),
                sort.clone(),
                "carries 1 values for 2 sort keys",
            ),
        ];
        for (name, cursor, sort, needle) in cases {
            let error = decode_cursor(&cursor, &sort).expect_err(name);
            assert!(
                error.to_string().starts_with("codec: ") && error.message.contains(needle),
                "{name}: {error}"
            );
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
                Query {
                    sort: full.sort.clone(),
                    ..Default::default()
                },
                json!({"sort": [{"path": "name", "desc": true}]}),
            ),
            (
                "a_nonzero_offset_is_emitted",
                Query {
                    offset: 3,
                    ..Default::default()
                },
                json!({"offset": 3}),
            ),
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
            assert_eq!(
                serde_json::from_value::<Query>(wire).unwrap(),
                query,
                "{name} round-trip"
            );
        }
    }

    #[test]
    fn tiebreak_is_appended_once() {
        assert_eq!(effective_sort(&[]), vec![], "an unsorted query takes no tiebreak");
        assert_eq!(
            effective_sort(&keys(&[("n", false)])),
            keys(&[("n", false), ("id", false)])
        );
        assert_eq!(
            effective_sort(&keys(&[("id", true)])),
            keys(&[("id", true)]),
            "a terminal id key is kept as-is"
        );
        assert_eq!(
            effective_sort(&[SortKey::decimal("id", false)]),
            vec![SortKey::decimal("id", false), SortKey::new("id", false)],
            "a collated id key ties \"7\" and \"07\": the plain id follows it"
        );
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
            (
                "a_typo_in_a_sort_key",
                br#"{"sort": [{"path": "n", "dsc": true}]}"#,
                &documents,
            ),
            (
                "an_integer_wider_than_64_bits",
                br#"{"filter": {"compare": {"path": "n", "op": "eq", "value": 1180591620717411303424}}}"#,
                &documents,
            ),
        ];
        for (name, spec, documents) in cases {
            assert!(query_json(spec, documents).is_err(), "{name}");
        }
        let page = query_json(br#"{"sort": [{"path": "n"}], "limit": 1}"#, &documents).unwrap();
        let next = encode_cursor(&json!({"id": "a", "n": 1}), &keys(&[("n", false)])).unwrap();
        assert_eq!(
            page,
            format!(r#"{{"rows":[{{"id":"a","n":1,"s":"closed"}}],"next":"{next}"}}"#)
        );
    }

    #[test]
    fn cursor_boundary_round_trips() {
        let sort = br#"[{"path":"n"}]"#;
        let doc = br#"{"id":"a","n":1}"#;
        let cursor = encode_cursor_json(doc, sort).unwrap();
        assert!(!cursor.contains(['+', '/', '=']), "URL-safe and unpadded: {cursor}");
        assert_eq!(decode_cursor_json(cursor.as_bytes(), sort).unwrap(), r#"[1,"a"]"#);
        assert_eq!(
            effective_sort_json(sort).unwrap(),
            r#"[{"path":"n","desc":false},{"path":"id","desc":false}]"#
        );
        assert!(encode_cursor_json(b"not json", sort).is_err());
        assert!(
            encode_cursor_json(br#"{"id":"a","n":18446744073709551617}"#, sort).is_err(),
            "a wide document integer"
        );
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
