//! Read-side query semantics: how a filtered set of documents is ordered and paginated.
//!
//! Backends translate this into their native `ORDER BY` / `LIMIT` / keyset clauses, but the
//! ordering and pagination rules must be identical everywhere or two services paging the same
//! read model would disagree on row order and cursor boundaries. So the kernel fixes:
//!
//! - a **total order** across JSON values (the comparator never returns "incomparable"), so a sort
//!   is always deterministic regardless of the value types present;
//! - **keyset cursors** as `base64(json(sort-key tuple))`, so a cursor minted by one binding is
//!   understood byte-for-byte by every other binding.
//!
//! [`query_json`] is the reference applier (filter -> keyset -> sort -> offset -> limit) that the
//! golden vectors pin; backends that push the query down to storage assert against it.
//!
//! Total type order (smallest to largest): number < string < bool < array < object < null. Within
//! a type: numbers numerically, strings by Unicode, bools `false < true`. A **missing** path and an
//! explicit `null` both sort as null (last in ascending). Arrays and objects order by type rank
//! only — two of them compare equal.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use crate::filter::{self, Filter};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

/// One ordering key: a dot-delimited path and a direction (`desc` defaults to ascending).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    /// Dot-delimited path into the document; a missing path sorts as null.
    pub path: String,
    /// Descending when true, ascending otherwise.
    #[serde(default)]
    pub desc: bool,
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
/// never sends.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Keyset cursor from a prior page (see [`encode_cursor`]). Requires a non-empty `sort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
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
fn compare_values(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // Numbers and strings order through the filter kernel, so a sort and a range filter rank
        // the same pair identically. Same-rank arrays/objects/nulls have no order beyond the rank
        // (`None`); upgrade to element-wise if anyone needs to sort by a composite path.
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

/// Conventional unique document path appended as the final sort key so keyset pagination is stable.
const ID_PATH: &str = "id";

/// Append a unique `id` tiebreak to `sort` unless it is already the terminal key, so no two rows share
/// a full sort tuple and keyset paging never drops or duplicates a tied row. A no-op on an empty sort
/// (an unsorted query keeps input order and takes no cursor) and on docs lacking `id` (the tiebreak
/// resolves to null for every row — callers wanting tie-free paging must give docs a unique `id`).
/// Applied identically by [`query_json`] and [`encode_cursor`] so a minted cursor matches the order.
///
/// Public as [`effective_sort`]: a backend translating a `Query` into its own dialect has to order by
/// the same keys the reference applier does, tiebreak included, or its pages disagree with the kernel
/// at every tie.
pub fn effective_sort(sort: &[SortKey]) -> Vec<SortKey> {
    tiebroken(sort)
}

fn tiebroken(sort: &[SortKey]) -> Vec<SortKey> {
    let mut keys = sort.to_vec();
    if !keys.is_empty() && keys.last().map(|k| k.path.as_str()) != Some(ID_PATH) {
        keys.push(SortKey {
            path: ID_PATH.to_string(),
            desc: false,
        });
    }
    keys
}

/// Compare two documents under the given sort keys (missing/null sort last in ascending order).
fn compare(keys: &[SortKey], a: &Value, b: &Value) -> Ordering {
    keys.iter().fold(Ordering::Equal, |order, key| {
        order.then_with(|| directed(key, compare_values(at(a, &key.path), at(b, &key.path))))
    })
}

/// Encode a keyset cursor from the last document of a page: `base64(json([sort-key values...]))`.
/// Every binding produces the same string, so cursors are portable across services. The `id` tiebreak
/// [`query_json`] applies is appended here too, so the cursor tuple lines up with the paged order.
pub fn encode_cursor(doc: &Value, keys: &[SortKey]) -> String {
    let keys = tiebroken(keys);
    let tuple: Vec<&Value> = keys.iter().map(|k| at(doc, &k.path)).collect();
    // serde_json::Map is BTreeMap here (no preserve_order), so nested object keys are sorted.
    // Infallible: a `Vec<Value>` holds only values serde_json itself produced.
    let bytes = serde_json::to_vec(&tuple).expect("a JSON value array is always serializable");
    B64.encode(bytes)
}

fn decode_tuple(s: &str) -> Result<Vec<Value>, String> {
    let bytes = B64.decode(s).map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

/// Decode a keyset cursor into the value tuple it carries, aligned to `keys`. The inverse of
/// [`encode_cursor`], and the piece a backend needs to turn `Query::after` into the bound values of
/// its own `WHERE` clause without re-deriving the wire format from prose.
///
/// `keys` is tiebroken first, so pass the query's declared sort and get back one value per key the
/// page is actually ordered by. A short tuple pads with `null` and a long one is truncated, which is
/// exactly what [`after_cursor`] does when it compares — a cursor minted under a different sort is
/// therefore read the same way the reference applier reads it, rather than rejected here and accepted
/// there.
pub fn decode_cursor(cursor: &str, keys: &[SortKey]) -> Result<Vec<Value>, String> {
    let keys = tiebroken(keys);
    let mut tuple = decode_tuple(cursor)?;
    tuple.resize(keys.len(), NULL.clone());
    Ok(tuple)
}

/// Whether a document sorts strictly after the cursor tuple under the given keys. An equal tuple is
/// not "after": that row was the last of the previous page. A short tuple pads with `null`.
fn after_cursor(keys: &[SortKey], doc: &Value, tuple: &[Value]) -> bool {
    let order = keys.iter().enumerate().fold(Ordering::Equal, |order, (i, key)| {
        order.then_with(|| directed(key, compare_values(at(doc, &key.path), tuple.get(i).unwrap_or(&NULL))))
    });
    order == Ordering::Greater
}

/// Reference applier: filter -> keyset(after) -> stable sort -> offset -> limit. Returns the page
/// as a JSON array of documents. `docs` is a JSON array.
pub fn query_json(query: &[u8], docs: &[u8]) -> Result<Vec<u8>, String> {
    let q: Query = serde_json::from_slice(query).map_err(|e| e.to_string())?;
    let docs: Vec<Value> = serde_json::from_slice(docs).map_err(|e| e.to_string())?;

    let mut rows: Vec<Value> = match &q.filter {
        Some(f) => docs.into_iter().filter(|d| filter::eval(f, d)).collect(),
        None => docs,
    };

    let sort = tiebroken(&q.sort);
    // An unsorted query has no order to resume from, so its cursor is ignored rather than rejected.
    if !sort.is_empty() {
        if let Some(cursor) = &q.after {
            let tuple = decode_tuple(cursor)?;
            rows.retain(|d| after_cursor(&sort, d, &tuple));
        }
    }
    rows.sort_by(|a, b| compare(&sort, a, b)); // stable

    let mut page: Vec<Value> = rows.into_iter().skip(q.offset).collect();
    if let Some(l) = q.limit {
        page.truncate(l);
    }
    serde_json::to_vec(&page).map_err(|e| e.to_string())
}

/// Boundary helper: decode a cursor into its value tuple under `sort` (JSON in, JSON array out).
pub fn decode_cursor_json(cursor: &[u8], sort: &[u8]) -> Result<Vec<u8>, String> {
    let cursor = std::str::from_utf8(cursor).map_err(|e| e.to_string())?;
    let keys: Vec<SortKey> = serde_json::from_slice(sort).map_err(|e| e.to_string())?;
    let tuple = decode_cursor(cursor, &keys)?;
    serde_json::to_vec(&tuple).map_err(|e| e.to_string())
}

/// Boundary helper: the sort keys a page is actually ordered by, tiebreak included (JSON in, out).
pub fn effective_sort_json(sort: &[u8]) -> Result<Vec<u8>, String> {
    let keys: Vec<SortKey> = serde_json::from_slice(sort).map_err(|e| e.to_string())?;
    serde_json::to_vec(&effective_sort(&keys)).map_err(|e| e.to_string())
}

/// Boundary helper: encode the next-page cursor from a document and the sort keys (both JSON).
pub fn encode_cursor_json(doc: &[u8], sort: &[u8]) -> Result<Vec<u8>, String> {
    let doc: Value = serde_json::from_slice(doc).map_err(|e| e.to_string())?;
    let keys: Vec<SortKey> = serde_json::from_slice(sort).map_err(|e| e.to_string())?;
    Ok(encode_cursor(&doc, &keys).into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn keys(spec: &[(&str, bool)]) -> Vec<SortKey> {
        spec.iter()
            .map(|(p, d)| SortKey {
                path: p.to_string(),
                desc: *d,
            })
            .collect()
    }

    /// Four rows: distinct `n` plus one row missing `n`, two statuses, unique ids out of sort order.
    fn docs() -> Value {
        json!([
            {"id": "c", "n": 3, "s": "open"},
            {"id": "a", "n": 1, "s": "closed"},
            {"id": "b", "n": 2, "s": "open"},
            {"id": "d", "s": "open"},
        ])
    }

    fn page(query: &Value, docs: &Value) -> Vec<Value> {
        let bytes = query_json(&serde_json::to_vec(query).unwrap(), &serde_json::to_vec(docs).unwrap()).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn ids(page: &[Value]) -> Vec<String> {
        page.iter()
            .map(|d| d["id"].as_str().unwrap_or("?").to_string())
            .collect()
    }

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
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let want = i.cmp(&j);
                assert_eq!(compare_values(&vals[i], &vals[j]), want, "{i} vs {j}");
            }
        }
        assert_eq!(
            compare_values(&json!(1), &json!(1.0)),
            Ordering::Equal,
            "int and float compare numerically"
        );
        // Above 2^53 an f64 cast would round both to the same value and report Equal.
        assert_eq!(
            compare_values(&json!(9_007_199_254_740_993u64), &json!(9_007_199_254_740_992u64)),
            Ordering::Greater,
            "integers order exactly"
        );
        assert_eq!(
            compare_values(&json!([1]), &json!([2])),
            Ordering::Equal,
            "arrays order by rank only"
        );
    }

    /// One case per branch of [`query_json`]; the ordering cases mirror `testdata/golden/query.json`,
    /// the cross-language contract.
    #[test]
    fn query_selects_orders_and_pages() {
        // Cursor after row "a" (n=1) under the `n` sort, minted the way a caller would.
        let after_a = encode_cursor(&json!({"id": "a", "n": 1}), &keys(&[("n", false)]));
        let cases: Vec<(&str, Value, Vec<&str>)> = vec![
            ("no_sort_keeps_input_order", json!({}), vec!["c", "a", "b", "d"]),
            (
                "sort_ascending_missing_path_last",
                json!({"sort": [{"path": "n"}]}),
                vec!["a", "b", "c", "d"],
            ),
            (
                "sort_descending_reverses",
                json!({"sort": [{"path": "n", "desc": true}]}),
                vec!["d", "c", "b", "a"],
            ),
            (
                "filter_then_sort",
                json!({"filter": {"compare": {"path": "s", "op": "eq", "value": "open"}}, "sort": [{"path": "n"}]}),
                vec!["b", "c", "d"],
            ),
            (
                "offset_and_limit",
                json!({"sort": [{"path": "n"}], "offset": 1, "limit": 2}),
                vec!["b", "c"],
            ),
            (
                "offset_past_the_end_is_empty",
                json!({"sort": [{"path": "n"}], "offset": 9}),
                vec![],
            ),
            (
                "zero_limit_is_empty",
                json!({"sort": [{"path": "n"}], "limit": 0}),
                vec![],
            ),
            (
                "after_cursor_resumes_strictly_after",
                json!({"sort": [{"path": "n"}], "after": after_a}),
                vec!["b", "c", "d"],
            ),
            // A tuple shorter than the tiebroken sort pads with null, so "a" (id "a" > null) is excluded.
            (
                "short_cursor_tuple_pads_with_null",
                json!({"sort": [{"path": "n"}], "after": "WzFd"}),
                vec!["b", "c", "d"],
            ),
            // An unsorted query has no order to resume from, so the cursor is ignored, not an error.
            (
                "cursor_without_sort_is_ignored",
                json!({"after": "WzFd"}),
                vec!["c", "a", "b", "d"],
            ),
        ];
        for (name, query, want) in cases {
            assert_eq!(ids(&page(&query, &docs())), want, "{name}");
        }
    }

    #[test]
    fn keyset_paging_walks_every_row_exactly_once() {
        // One row per page, resuming from the previous page's cursor: no row may be dropped or repeated.
        let tied = json!([
            {"id": "a", "bal": 100},
            {"id": "b", "bal": 100},
            {"id": "c", "bal": 100},
        ]);
        struct Case {
            name: &'static str,
            documents: Value,
            sort: Vec<SortKey>,
            want: Vec<&'static str>,
        }
        let cases = vec![
            Case {
                name: "unique_sort_key",
                documents: docs(),
                sort: keys(&[("n", false)]),
                want: vec!["a", "b", "c", "d"],
            },
            Case {
                name: "descending_sort_key",
                documents: docs(),
                sort: keys(&[("n", true)]),
                want: vec!["d", "c", "b", "a"],
            },
            Case {
                name: "tied_sort_key_broken_by_id",
                documents: tied,
                sort: keys(&[("bal", false)]),
                want: vec!["a", "b", "c"],
            },
            Case {
                name: "sort_already_ends_in_id",
                documents: docs(),
                sort: keys(&[("n", false), ("id", false)]),
                want: vec!["a", "b", "c", "d"],
            },
        ];
        for Case {
            name,
            documents,
            sort,
            want,
        } in cases
        {
            let mut seen: Vec<String> = Vec::new();
            let mut cursor: Option<String> = None;
            for _ in 0..want.len() + 1 {
                let mut query = json!({"sort": sort, "limit": 1});
                if let Some(c) = &cursor {
                    query["after"] = json!(c);
                }
                let rows = page(&query, &documents);
                let Some(row) = rows.first() else { break };
                seen.push(row["id"].as_str().unwrap().to_string());
                cursor = Some(encode_cursor(row, &sort));
            }
            assert_eq!(seen, want, "{name}");
        }
    }

    /// Both directions of the one shape, per field: `skip_serializing_if` decides what a spec emits,
    /// `default` decides what a missing field means on the way back in. Every skip branch is covered
    /// both ways, and each case round-trips, so the two attribute sets cannot drift apart.
    #[test]
    fn spec_omits_unset_fields_and_round_trips() {
        let full = Query {
            filter: Some(filter::eq("status", json!("open"))),
            sort: vec![SortKey {
                path: "name".to_string(),
                desc: true,
            }],
            offset: 2,
            limit: Some(5),
            after: Some("WzFd".to_string()),
        };
        let cases: Vec<(&str, Query, Value)> = vec![
            // An unset spec emits nothing at all — the same document every binding sends for "no
            // constraints". A skipped `offset: 0` still reads back as 0, which the round-trip pins.
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
                    "after": "WzFd",
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
        assert_eq!(tiebroken(&[]), vec![], "an unsorted query takes no tiebreak");
        assert_eq!(tiebroken(&keys(&[("n", false)])), keys(&[("n", false), ("id", false)]));
        assert_eq!(
            tiebroken(&keys(&[("id", true)])),
            keys(&[("id", true)]),
            "a terminal id key is kept as-is"
        );
    }

    #[test]
    fn malformed_input_is_rejected() {
        let sorted = serde_json::to_vec(&json!({"sort": [{"path": "n"}]})).unwrap();
        let documents = serde_json::to_vec(&docs()).unwrap();
        let cursor_of = |c: &str| serde_json::to_vec(&json!({"sort": [{"path": "n"}], "after": c})).unwrap();
        let cases: Vec<(&str, Vec<u8>, Vec<u8>)> = vec![
            ("query_is_not_json", b"{".to_vec(), documents.clone()),
            ("documents_are_not_json", sorted.clone(), b"{".to_vec()),
            ("documents_are_not_an_array", sorted.clone(), b"{}".to_vec()),
            ("cursor_is_not_base64", cursor_of("!!not-base64!!"), documents.clone()),
            ("cursor_is_not_json", cursor_of(&B64.encode("nope")), documents.clone()),
        ];
        for (name, query, documents) in cases {
            assert!(query_json(&query, &documents).is_err(), "{name}");
        }
    }

    #[test]
    fn cursor_boundary_round_trips() {
        let sort = serde_json::to_vec(&json!([{"path": "n"}])).unwrap();
        let doc = serde_json::to_vec(&json!({"id": "a", "n": 1})).unwrap();
        let cursor = String::from_utf8(encode_cursor_json(&doc, &sort).unwrap()).unwrap();
        assert_eq!(
            decode_tuple(&cursor).unwrap(),
            vec![json!(1), json!("a")],
            "id tiebreak is in the tuple"
        );
        // The boundary the bindings call: the same tuple, aligned to the tiebroken sort.
        let tuple: Vec<Value> =
            serde_json::from_slice(&decode_cursor_json(cursor.as_bytes(), &sort).unwrap()).unwrap();
        assert_eq!(tuple, vec![json!(1), json!("a")]);
        assert_eq!(
            String::from_utf8(effective_sort_json(&sort).unwrap()).unwrap(),
            r#"[{"path":"n","desc":false},{"path":"id","desc":false}]"#,
            "the sort a page is really ordered by"
        );
        assert!(encode_cursor_json(b"not json", &sort).is_err());
        assert!(encode_cursor_json(&doc, b"not json").is_err());
        assert!(decode_cursor_json(b"not base64!", &sort).is_err());
        assert!(decode_cursor_json(cursor.as_bytes(), b"not json").is_err());
    }
}
