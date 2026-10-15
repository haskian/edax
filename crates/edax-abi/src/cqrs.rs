//! The `edax-cqrs` surface: filter and query, canonical hashing, the command guard's key, the
//! idempotency record and its claim.

use edax_core::ErrorCode;

use crate::{error_code, names, tenancy_mode, to_wire};

/// Evaluate a filter (JSON) against a document (JSON). The match decision — including the two-valued
/// null/missing-path rules — is the kernel's. (The boundary is JSON text: alef has no object-passing
/// for arbitrary values, so structured crossing isn't available here.)
pub fn eval_filter(filter: &str, doc: &str) -> Result<bool, String> {
    edax_cqrs::eval_json(filter.as_bytes(), doc.as_bytes()).map_err(to_wire)
}

/// Apply a read query (JSON string) to a JSON array of documents; returns the page as JSON text
/// `{"rows": [...], "next": cursor|null}`, `next` minted under the query's own sort. See
/// [`edax_cqrs::query()`].
pub fn query(query: &str, docs: &str) -> Result<String, String> {
    edax_cqrs::query_json(query.as_bytes(), docs.as_bytes()).map_err(to_wire)
}

/// Refuse a read query spec (JSON string) as [`query`] refuses it: an unknown field, an integer literal
/// wider than 64 bits, a `null` comparand, a path with an empty segment, or a cursor on an unsorted query
/// is a `codec:` error. The cursor itself is read by [`decode_cursor`]. For a store that answers the
/// query itself and must refuse the specs the kernel refuses. See [`edax_cqrs::query_spec`].
pub fn check_query(query: &str) -> Result<(), String> {
    edax_cqrs::query_spec(query.as_bytes()).map(|_| ()).map_err(to_wire)
}

/// Mint the keyset cursor that resumes after a document (JSON string) under sort keys (JSON string),
/// for a backend that pages natively. A row whose `id` is not a string or a number is a `codec:` error.
pub fn encode_cursor(doc: &str, sort: &str) -> Result<String, String> {
    edax_cqrs::encode_cursor_json(doc.as_bytes(), sort.as_bytes()).map_err(to_wire)
}

/// Decode a keyset cursor into the value tuple (JSON array string) a backend binds into its own
/// keyset `WHERE` clause, one value per key of the effective sort (sort keys as a JSON string). A
/// cursor minted under another sort is a `codec:` error.
pub fn decode_cursor(cursor: &str, sort: &str) -> Result<String, String> {
    edax_cqrs::decode_cursor_json(cursor.as_bytes(), sort.as_bytes()).map_err(to_wire)
}

/// The sort keys a page is actually ordered by, with the `id` tiebreak appended (JSON in, JSON out).
///
/// A backend ordering by the declared sort alone disagrees with the kernel at every tie.
pub fn effective_sort(sort: &str) -> Result<String, String> {
    edax_cqrs::effective_sort_json(sort.as_bytes()).map_err(to_wire)
}

/// The key a guard claims for a received command: its `idempotency_key`, or its own `id` when it
/// carries none, so a redelivery of the same message still deduplicates. See
/// [`edax_cqrs::received_key`].
// `idempotency_key` is optional, so it trails `id`: alef moves optional parameters to the end.
pub fn received_key(id: &str, idempotency_key: Option<String>) -> String {
    edax_cqrs::received_key(idempotency_key.as_deref(), id).to_owned()
}

/// Canonicalize a UTF-8 JSON payload (RFC 8785) and hash it. Canonicalization lives in the kernel,
/// so every binding gets the same hash; don't canonicalize on the host side. A command's guard uses
/// [`command_hash_json`], which also binds the command name. An integer literal outside
/// `-(2^53-1)..=2^53-1` is a `codec` error.
pub fn payload_hash_json(payload: &[u8]) -> Result<u64, String> {
    let canonical = edax_cqrs::canonical_json(payload).map_err(to_wire)?;
    Ok(edax_core::payload_hash(&canonical))
}

/// The kernel's canonical form of a UTF-8 JSON payload: the exact text [`payload_hash_json`] hashes.
/// For a host that must check bytes, not only hashes. See [`edax_cqrs::canonical_json`].
pub fn canonical_json(payload: &[u8]) -> Result<String, String> {
    let canonical = edax_cqrs::canonical_json(payload).map_err(to_wire)?;
    // The canonical writer emits UTF-8 only, so the lossy path never replaces anything.
    Ok(String::from_utf8_lossy(&canonical).into_owned())
}

/// The canonical text of decimal text (`"9.50"` → `"9.5"`, `"1E+2"` → `"100"`): the one string a host
/// writes an exact decimal as, so equality and the command hash see one value. `NaN`, an infinity,
/// text that is not a decimal, or one past the digit bound is a `codec:` error. See
/// [`edax_cqrs::canonical_decimal`].
pub fn canonical_decimal(text: &str) -> Result<String, String> {
    edax_cqrs::canonical_decimal(text).map_err(to_wire)
}

/// The fixed-width UTC text of an RFC 3339 timestamp (`"2024-01-01T01:30:00+01:00"` →
/// `"2024-01-01T00:30:00.000000Z"`): the one string a host writes an instant as, so string order is
/// instant order. Text without an offset, a fraction finer than a microsecond, or a year outside
/// 0000 to 9999 is a `codec:` error. See [`edax_cqrs::canonical_timestamp`].
pub fn canonical_timestamp(text: &str) -> Result<String, String> {
    edax_cqrs::canonical_timestamp(text).map_err(to_wire)
}

/// The hash a command's idempotency guard binds to its key: FNV-1a over the RFC 8785 form of
/// `{"name": name, "payload": payload}`. `payload` is the command value as UTF-8 JSON, in the shape
/// the handler receives (after upcast). The kernel canonicalizes, so every binding agrees. An integer
/// literal outside `-(2^53-1)..=2^53-1` is a `codec` error. See [`edax_cqrs::command_hash_json`].
pub fn command_hash_json(name: &str, payload: &[u8]) -> Result<u64, String> {
    edax_cqrs::command_hash_json(name, payload).map_err(to_wire)
}

/// The ledger key a command's idempotency guard claims for `idempotency_key` under the guard's
/// `scope`, in a deployment of tenancy `mode` (`"silo"`, `"pool"` or `"bridge"`):
/// `"edax:cmd:{len(scope)}:{scope}:{idempotency_key}"`, composed with `tenant` under a pool. Two
/// guards with different scopes never share a key. A mode that requires a tenant and a call that
/// names none is a `codec:` error. Pinned by `testdata/golden/claim_key.json`. See
/// [`edax_cqrs::claim_key`].
pub fn claim_key(mode: &str, scope: &str, idempotency_key: &str, tenant: Option<String>) -> Result<String, String> {
    let mode = tenancy_mode(mode)?;
    edax_cqrs::claim_key(mode, tenant.as_deref(), scope, idempotency_key).map_err(to_wire)
}

/// Every name [`IdempotencyRecord::state`] takes, in the kernel's order. A host checks the states it
/// branches on against this list. See [`edax_cqrs::StateKind`].
pub fn idempotency_states() -> Vec<String> {
    names(edax_cqrs::StateKind::NAMES)
}

/// Every name [`IdempotencyReplay::outcome`] takes, in the kernel's order. A host checks the verdicts it
/// branches on against this list. See [`edax_cqrs::OutcomeKind`].
pub fn idempotency_outcomes() -> Vec<String> {
    names(edax_cqrs::OutcomeKind::NAMES)
}

/// A stored idempotency record, decoded by the kernel. `state` is `"pending"`, `"failed"` or
/// `"done"`; `error` is set only for `"failed"`, and `error_code` beside it when the failure was
/// stored with a code the kernel knows; `result_json` (the cached result as JSON text) is set only for
/// `"done"`.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IdempotencyRecord {
    pub payload_hash: u64,
    pub state: String,
    pub error: Option<String>,
    pub error_code: Option<String>,
    pub result_json: Option<String>,
}

/// The canonical JSON record for a claim, a cached success, or a cached failure. The kernel applies
/// its reading order to the flat inputs (`pending` wins, then `error`, then `result_json`), so a
/// host that models the record as separate fields cannot write an ambiguous one. `result_json` is
/// the host's result as JSON text; it is read only when the record is neither pending nor failed.
/// `error_code` is the failure's taxonomy code, read only beside `error`; an unknown one is a
/// `codec:` error. `error` is `None` for a record that did not fail; any `Some`, the empty message
/// included, is a failure. See [`edax_cqrs::Record`].
pub fn encode_idempotency_record(
    payload_hash: u64,
    pending: bool,
    result_json: &str,
    error: Option<String>,
    error_code: Option<String>,
) -> Result<String, String> {
    let record = record_of_flat(payload_hash, pending, result_json, error, error_code.as_deref()).map_err(to_wire)?;
    let bytes = edax_cqrs::encode_record(&record).map_err(to_wire)?;
    // The kernel's encoder writes UTF-8 only, so the lossy path never replaces anything.
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Decode a stored idempotency record with the kernel's reading order. A record that is not a JSON
/// object, lacks `payload_hash`, or carries a non-integer hash is a `codec:` error. A store reads the
/// record holding a key with [`decode_prior_idempotency_record`] instead.
pub fn decode_idempotency_record(record_json: &str) -> Result<IdempotencyRecord, String> {
    let record = edax_cqrs::decode_record::<serde_json::Value>(record_json.as_bytes()).map_err(to_wire)?;
    Ok(flat_record(record))
}

/// The record a losing claimant reads from the JSON holding its key. Never fails: a record no reader
/// can decode is a terminal `codec` failure, carrying the stored hash when it still reads, else
/// `claimant_hash`. See [`edax_cqrs::decode_prior`].
pub fn decode_prior_idempotency_record(record_json: &str, claimant_hash: u64) -> IdempotencyRecord {
    flat_record(edax_cqrs::decode_prior(record_json.as_bytes(), claimant_hash))
}

/// The record that stands in for a cached result the host cannot read back as its result type: a
/// terminal `codec` failure under `payload_hash` whose message names `cause`. See
/// [`edax_cqrs::Record::unreadable`].
pub fn unreadable_idempotency_record(payload_hash: u64, cause: &str) -> IdempotencyRecord {
    flat_record(edax_cqrs::Record::unreadable(
        payload_hash,
        &edax_core::Error::codec(cause),
    ))
}

// The flat record is a binding shape, so both of its directions live here, side by side: the record
// a binding's flat fields describe, and the flat fields of a decoded record.

/// The record the flat fields describe, under the kernel's reading order ([`edax_cqrs::State::from_wire`]):
/// `result_json` is read only for a success, and `error_code` only beside an `error`. A writer must name
/// a code the taxonomy knows: an unknown one, or `result_json` that is not JSON or holds an integer
/// wider than 64 bits ([`edax_cqrs::read_json`]), is a `codec` error.
fn record_of_flat(
    payload_hash: u64,
    pending: bool,
    result_json: &str,
    error: Option<String>,
    error_code: Option<&str>,
) -> Result<edax_cqrs::Record<serde_json::Value>, edax_core::Error> {
    Ok(match edax_cqrs::State::from_wire(pending, error, None, ()) {
        edax_cqrs::State::Pending => edax_cqrs::Record::pending(payload_hash),
        edax_cqrs::State::Failed { message, .. } => {
            let code = error_code.map(str::parse::<ErrorCode>).transpose()?;
            edax_cqrs::Record {
                payload_hash,
                state: edax_cqrs::State::Failed { code, message },
            }
        }
        edax_cqrs::State::Done(()) => edax_cqrs::Record::done(
            payload_hash,
            edax_cqrs::read_json("cached result", result_json.as_bytes())?,
        ),
    })
}

/// A decoded record as the flat fields a binding holds.
fn flat_record(record: edax_cqrs::Record<serde_json::Value>) -> IdempotencyRecord {
    let state = record.state.kind().as_str().to_owned();
    let (error, error_code, result_json) = match record.state {
        edax_cqrs::State::Pending => (None, None, None),
        edax_cqrs::State::Failed { code, message } => (Some(message), code.map(|c| c.as_str().to_owned()), None),
        edax_cqrs::State::Done(result) => (None, None, Some(result.to_string())),
    };
    IdempotencyRecord {
        payload_hash: record.payload_hash,
        state,
        error,
        error_code,
        result_json,
    }
}

/// The failure half of the guard's settle table, as the write itself: the failed record a won claim
/// caches for a failure with `message` and the taxonomy `code` under `payload_hash`, or `None` to release
/// the claim instead. `code` is `None` for a failure no code classifies; an unknown code is a `codec:`
/// error. See [`edax_cqrs::settle_failure`].
// `code` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn idempotency_settle_failure(
    payload_hash: u64,
    message: &str,
    code: Option<String>,
) -> Result<Option<IdempotencyRecord>, String> {
    let code = error_code(code)?;
    Ok(match edax_cqrs::settle_failure(payload_hash, code, message) {
        edax_cqrs::Settle::Cache(record) => Some(flat_record(record)),
        edax_cqrs::Settle::Release => None,
    })
}

/// What a claimant that lost the claim on `key` reads from the winner's record: `outcome` is one of
/// [`idempotency_outcomes`], and `error` the wire form of the error the claimant fails with, `None` for
/// `"succeeded"` (replay the cached result).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IdempotencyReplay {
    pub outcome: String,
    pub error: Option<String>,
}

/// The answer of a claimant whose command hashes to `current`, having lost the claim on `key` to the
/// record with these classifying fields: a reused key is `payload_mismatch:` and a claim in flight
/// `in_progress:`, each naming `key`, and a cached failure `cached_failure:` carrying the original
/// failure's wire form. Takes the record's fields rather than its JSON so a host that already decoded
/// the record does not re-encode it; the result plays no part in the answer. `error` is `None` for a
/// record that did not fail. See [`edax_cqrs::replay`].
// `error` and `error_code` are optional, so they trail: alef moves optional parameters to the end.
pub fn idempotency_replay(
    current: u64,
    key: &str,
    payload_hash: u64,
    pending: bool,
    error: Option<String>,
    error_code: Option<String>,
) -> IdempotencyReplay {
    let prior = edax_cqrs::Record {
        payload_hash,
        state: edax_cqrs::State::from_wire(pending, error, error_code.as_deref(), ()),
    };
    let outcome = edax_cqrs::outcome(&prior, current).kind().as_str().to_owned();
    let error = edax_cqrs::replay(&prior, current, key).err().map(to_wire);
    IdempotencyReplay { outcome, error }
}

/// What an idempotency claim found, on either ledger ([`MemoryIdempotencyStore::claim`](crate::MemoryIdempotencyStore::claim),
/// [`redis_claim_outcome`](crate::redis_claim_outcome)), flattened from [`edax_cqrs::Claimed`]: `won` with the `handle` that settles
/// the claim, or else the `prior` record JSON holding the key. One struct rather than a union, which the
/// boundary cannot carry.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ClaimResult {
    pub won: bool,
    pub handle: Option<Vec<u8>>,
    pub prior: Option<String>,
}

/// The kernel's claim result as the boundary carries it. Both ledgers hold only text the kernel or a
/// `complete` wrote from a `&str`, so the lossy path never replaces anything.
pub(crate) fn claim_result(claimed: edax_cqrs::Claimed) -> ClaimResult {
    match claimed {
        edax_cqrs::Claimed::Won(handle) => ClaimResult {
            won: true,
            handle: Some(handle),
            prior: None,
        },
        edax_cqrs::Claimed::Prior(bytes) => ClaimResult {
            won: false,
            handle: None,
            prior: Some(String::from_utf8_lossy(&bytes).into_owned()),
        },
    }
}

/// Refuse `document_json` as the stored document of projection `id` exactly as the kernel's stores do,
/// as a `codec:` error: one that is not a JSON object, holds an integer wider than 64 bits, or is not
/// JSON. For a host's own projection store. See [`edax_cqrs::document_json`].
pub fn check_projection_document(id: &str, document_json: &str) -> Result<(), String> {
    edax_cqrs::document_json(id, document_json.as_bytes())
        .map(|_| ())
        .map_err(to_wire)
}

/// `document_json`, the stored document of projection `id`, as a store's `get` and `get_many` read it back:
/// with the row key as its `"id"` field, overwriting any the document carries. A document
/// [`check_projection_document`] refuses is refused here with the same `codec:` error. For a host's own
/// projection store. See [`edax_cqrs::keyed_document`].
pub fn keyed_projection_document(id: &str, document_json: &str) -> Result<String, String> {
    edax_cqrs::keyed_document_json(id, document_json.as_bytes()).map_err(to_wire)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use crate::*;
    use serde_json::{json, Value};

    #[test]
    fn filter_and_query_defer_to_the_kernel() {
        let doc = r#"{"id":"user-1","email":"ada@example.com","age":36,"tags":["eu","paid"]}"#;
        assert!(eval_filter(r#"{"compare":{"path":"age","op":"gte","value":36}}"#, doc).unwrap());
        assert!(!eval_filter(r#"{"compare":{"path":"age","op":"gt","value":36}}"#, doc).unwrap());
        assert!(
            !eval_filter(r#"{"compare":{"path":"deleted_at","op":"ne","value":"x"}}"#, doc).unwrap(),
            "a missing path makes the leaf false — the kernel's two-valued rule"
        );
        assert!(
            eval_filter(r#"{"compare":{"path":"deleted_at","op":"ne","value":null}}"#, doc)
                .unwrap_err()
                .starts_with("codec: a null comparand"),
            "a null comparand is refused, with the kernel's pointer to `exists`"
        );

        let docs = r#"[{"id":"user-1","age":36},{"id":"user-2","age":29},{"id":"user-3","age":41}]"#;
        let page: Value =
            serde_json::from_str(&query(r#"{"sort":[{"path":"age"}],"limit":2}"#, docs).unwrap()).unwrap();
        let rows = |page: &Value| {
            page["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| d["id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(rows(&page), ["user-2", "user-1"]);

        // The page's own `next` continues it with no gap or repeat, and is the cursor a backend mints.
        let cursor = page["next"].as_str().unwrap();
        assert_eq!(
            encode_cursor(r#"{"id":"user-1","age":36}"#, r#"[{"path":"age"}]"#).unwrap(),
            cursor
        );
        assert_eq!(
            decode_cursor(cursor, r#"[{"path":"age"}]"#).unwrap(),
            r#"[36,"user-1"]"#
        );
        let spec = json!({"sort": [{"path": "age"}], "limit": 2, "after": cursor}).to_string();
        let next: Value = serde_json::from_str(&query(&spec, docs).unwrap()).unwrap();
        assert_eq!(rows(&next), ["user-3"]);
        assert!(next["next"].is_null(), "nothing follows the last page");
    }

    /// One case per branch of the flat record's encoding that the round-trip table does not reach.
    #[test]
    fn encode_idempotency_record_branches() {
        for (name, pending, result, error, code, want) in [
            (
                "failed_without_code",
                false,
                "",
                Some("boom"),
                None,
                Ok(r#"{"payload_hash":7,"result":null,"error":"boom","error_code":null,"pending":false}"#),
            ),
            (
                "done_reads_the_result",
                false,
                r#"{"b":1,"a":2}"#,
                None,
                Some("codec"),
                Ok(r#"{"payload_hash":7,"result":{"a":2,"b":1},"error":null,"error_code":null,"pending":false}"#),
            ),
            (
                "unknown_code_is_refused",
                false,
                "",
                Some("boom"),
                Some("timeout"),
                Err("codec: unknown error code: timeout"),
            ),
            (
                "result_not_json",
                false,
                "{",
                None,
                None,
                Err("codec: cached result is not JSON: EOF while parsing an object at line 1 column 1"),
            ),
            (
                "result_past_64_bits",
                false,
                r#"{"total":18446744073709551616}"#,
                None,
                None,
                Err("codec: integer at byte 9 is outside the 64-bit range"),
            ),
        ] {
            let got = encode_idempotency_record(7, pending, result, error.map(str::to_owned), code.map(str::to_owned));
            assert_eq!(got.as_deref(), want.map_err(ToString::to_string).as_deref(), "{name}");
        }
    }

    #[test]
    fn check_query_refuses_what_query_refuses() {
        assert_eq!(check_query(r#"{"sort":[{"path":"age"}],"limit":2}"#), Ok(()));
        let refusals = [
            ("unknown_field", r#"{"limt":2}"#, "unknown field `limt`"),
            (
                "null_comparand",
                r#"{"filter":{"compare":{"path":"age","op":"eq","value":null}}}"#,
                "null comparand",
            ),
            ("empty_sort_segment", r#"{"sort":[{"path":"a..b"}]}"#, "empty segment"),
            ("cursor_without_a_sort", r#"{"after":"x"}"#, "unsorted query"),
        ];
        for (name, spec, needle) in refusals {
            let error = check_query(spec).expect_err(name);
            assert!(
                error.starts_with("codec: ") && error.contains(needle),
                "{name}: {error}"
            );
            assert_eq!(query(spec, "[]").err(), Some(error), "{name}: query refuses alike");
        }
    }

    #[test]
    fn idempotency_settle_failure_crosses_the_record_to_cache() {
        let cached = |code: &str| idempotency_settle_failure(HASH, "bad payload", Some(code.to_owned()));
        assert!(
            matches!(idempotency_settle_failure(HASH, "boom", None), Ok(None)),
            "unclassified releases"
        );
        assert!(matches!(cached("store"), Ok(None)), "a transient code releases");
        let record = cached("codec").ok().flatten().expect("a terminal code caches");
        assert_eq!((record.payload_hash, record.state.as_str()), (HASH, "failed"));
        assert_eq!(
            (
                record.error.as_deref(),
                record.error_code.as_deref(),
                record.result_json
            ),
            (Some(""), Some("codec"), None)
        );
        assert_eq!(
            cached("timeout").err().as_deref(),
            Some("codec: unknown error code: timeout")
        );
    }

    #[test]
    fn idempotency_inputs_cross_in_argument_order() {
        // The event guard's key and hash cross as two fields: the scoped event id, and the hash of the
        // body. The mode crosses by name, and an unknown one is the kernel's `codec` refusal.
        let rule = event_idempotency(event_message("evt-1", Some(1)), "silo", "balances").unwrap();
        assert_eq!(rule.key, "edax:evt:8:balances:evt-1");
        assert_eq!(rule.payload_hash, payload_hash(&event_message("evt-1", Some(1)).data));
        assert!(event_idempotency(event_message("evt-1", Some(1)), "shard", "balances")
            .is_err_and(|e| e.starts_with("codec:")));

        // (mode, scope, key, tenant): the tenant reaches the key only under a pool.
        assert_eq!(
            claim_key("pool", "api", "req-1", Some("acme".to_owned())),
            Ok("4:acme:edax:cmd:3:api:req-1".to_owned())
        );
        assert!(claim_key("pool", "api", "req-1", None).is_err_and(|e| e.starts_with("codec:")));

        // (name, payload): the name reaches the hash, so another name over one payload is another hash.
        let payload = br#"{"invoice_id":"invoice-7","total":5000}"#;
        assert_ne!(
            command_hash_json("IssueInvoice", payload),
            command_hash_json("VoidInvoice", payload)
        );
    }

    #[test]
    fn idempotency_record_branches() {
        struct Case {
            name: &'static str,
            pending: bool,
            result_json: &'static str,
            error: Option<&'static str>,
            error_code: Option<&'static str>,
            /// The canonical record bytes — the cross-language contract, pinned by
            /// `testdata/golden/idempotency_record.json`.
            encoded: &'static str,
            state: &'static str,
            /// What `decode_idempotency_record` reads back into the flat `(error, error_code, result_json)`.
            decoded: (Option<&'static str>, Option<&'static str>, Option<&'static str>),
            /// The verdict a losing claimant reaches from the record.
            outcome: &'static str,
        }
        let cases = [
            Case {
                name: "pending_claim",
                pending: true,
                result_json: RESULT,
                error: None,
                error_code: None,
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":true}"#,
                state: "pending",
                decoded: (None, None, None),
                outcome: "in_progress",
            },
            Case {
                name: "cached_success",
                pending: false,
                result_json: RESULT,
                error: None,
                error_code: None,
                encoded: concat!(
                    r#"{"payload_hash":11234062831410934249,"result":{"invoice_id":"invoice-7","#,
                    r#""total":5000},"error":null,"error_code":null,"pending":false}"#
                ),
                state: "done",
                decoded: (None, None, Some(RESULT)),
                outcome: "succeeded",
            },
            Case {
                // A completed `null` is a success whose value is null, not "no result" and not an
                // in-flight claim.
                name: "cached_success_with_a_null_result",
                pending: false,
                result_json: "null",
                error: None,
                error_code: None,
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":false}"#,
                state: "done",
                decoded: (None, None, Some("null")),
                outcome: "succeeded",
            },
            Case {
                name: "cached_failure",
                pending: false,
                result_json: RESULT,
                error: Some("bad payload"),
                error_code: Some("codec"),
                encoded: concat!(
                    r#"{"payload_hash":11234062831410934249,"result":null,"#,
                    r#""error":"bad payload","error_code":"codec","pending":false}"#
                ),
                state: "failed",
                decoded: (Some("bad payload"), Some("codec"), None),
                outcome: "failed",
            },
            Case {
                // Reading order, first rule: pending wins, so a stale error cannot mask an
                // in-flight claim into a terminal one.
                name: "pending_wins_over_an_error",
                pending: true,
                result_json: RESULT,
                error: Some("boom"),
                error_code: Some("rejected"),
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":true}"#,
                state: "pending",
                decoded: (None, None, None),
                outcome: "in_progress",
            },
            Case {
                // Second rule: error wins over a result, so a half-written record never replays as
                // a success.
                name: "error_wins_over_a_result",
                pending: false,
                result_json: RESULT,
                error: Some("boom"),
                error_code: Some("rejected"),
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","error_code":"rejected","pending":false}"#,
                state: "failed",
                decoded: (Some("boom"), Some("rejected"), None),
                outcome: "failed",
            },
        ];

        for case in cases {
            let owned_error = || case.error.map(str::to_owned);
            let encoded = encode_idempotency_record(
                HASH,
                case.pending,
                case.result_json,
                owned_error(),
                case.error_code.map(str::to_owned),
            );
            assert_eq!(encoded.as_deref(), Ok(case.encoded), "{}", case.name);

            let record = decode_idempotency_record(case.encoded).expect(case.name);
            assert_eq!(record.payload_hash, HASH, "{} hash", case.name);
            assert_eq!(record.state, case.state, "{} state", case.name);
            assert_eq!(
                (
                    record.error.as_deref(),
                    record.error_code.as_deref(),
                    record.result_json.as_deref()
                ),
                case.decoded,
                "{} fields",
                case.name
            );

            // The verdict reads only the classifying fields, and the hash is checked before them:
            // a key reused with another command mismatches whatever state the winner is in.
            let code = || case.error_code.map(str::to_owned);
            let verdict = idempotency_replay(HASH, "req-1", record.payload_hash, case.pending, owned_error(), code());
            assert_eq!(verdict.outcome, case.outcome, "{} outcome", case.name);
            assert_eq!(
                verdict.error.is_none(),
                case.outcome == "succeeded",
                "{} error only when the claimant fails",
                case.name
            );
            let reused = idempotency_replay(
                HASH ^ 1,
                "req-1",
                record.payload_hash,
                case.pending,
                owned_error(),
                code(),
            );
            assert_eq!(
                (reused.outcome.as_str(), reused.error.as_deref()),
                ("payload_mismatch", Some("payload_mismatch: req-1")),
                "{} reused key",
                case.name
            );
        }
    }

    #[test]
    fn a_cached_failure_replays_with_its_original_code() {
        for (name, error, code, want) in [
            (
                "known_code_is_kept_in_the_wire_form",
                "bad payload",
                Some("codec"),
                "codec: bad payload",
            ),
            ("no_code_replays_the_message", "boom", None, "boom"),
            ("unknown_code_reads_as_no_code", "boom", Some("quota_exceeded"), "boom"),
        ] {
            let replay = idempotency_replay(
                HASH,
                "req-1",
                HASH,
                false,
                Some(error.to_owned()),
                code.map(str::to_owned),
            );
            assert_eq!(replay.error, Some(format!("cached_failure: {want}")), "{name}");
        }
    }

    #[test]
    fn a_prior_record_never_fails_to_decode() {
        let flat = |r: IdempotencyRecord| (r.payload_hash, r.state, r.error, r.error_code, r.result_json);
        let readable = r#"{"payload_hash":5,"result":{"n":1},"error":null,"pending":false}"#;
        assert_eq!(
            flat(decode_prior_idempotency_record(readable, 9)),
            flat(decode_idempotency_record(readable).unwrap())
        );
        let unreadable = |hash: u64, error: &str| {
            (
                hash,
                "failed".to_owned(),
                Some(error.to_owned()),
                Some("codec".to_owned()),
                None,
            )
        };
        assert_eq!(
            flat(decode_prior_idempotency_record("{", 9)),
            unreadable(
                9,
                "unreadable idempotency record: EOF while parsing an object at line 1 column 1"
            ),
            "garbage carries the claimant's hash"
        );
        assert_eq!(
            flat(decode_prior_idempotency_record(r#"{"payload_hash":5,"error":7}"#, 9)),
            unreadable(
                5,
                "unreadable idempotency record: invalid type: integer `7`, expected a string"
            ),
            "a readable hash is kept"
        );
        assert_eq!(
            flat(unreadable_idempotency_record(5, "does not convert to Invoice")),
            unreadable(5, "unreadable idempotency record: does not convert to Invoice"),
        );
    }

    #[test]
    fn a_record_from_an_older_writer_reads_by_the_kernels_defaults() {
        // Postel's law on the way in: a writer from before `pending` existed, and one that omitted
        // an absent result, both read as a completed success rather than an endless in-flight claim.
        for (name, stored, state) in [
            (
                "missing_pending_reads_as_completed",
                r#"{"payload_hash":11234062831410934249,"result":42}"#,
                "done",
            ),
            (
                "missing_result_reads_as_null",
                r#"{"payload_hash":11234062831410934249,"pending":false}"#,
                "done",
            ),
            (
                "missing_pending_with_an_error_reads_as_failed",
                r#"{"payload_hash":11234062831410934249,"error":"boom"}"#,
                "failed",
            ),
        ] {
            let record = decode_idempotency_record(stored).expect(name);
            assert_eq!((record.payload_hash, record.state.as_str()), (HASH, state), "{name}");
        }
    }
}

fn protected_record(record: &str) -> Result<edax_cqrs::protected::ProtectedRecord, String> {
    edax_cqrs::protected::decode_record(record).map_err(crate::to_wire)
}
fn protected_fingerprint_record(record: &str) -> Result<edax_cqrs::protected::Fingerprint, String> {
    serde_json::from_str(record).map_err(|_| crate::to_wire(edax_core::Error::codec("invalid protected fingerprint")))
}
/// Full HMAC-SHA-256 command fingerprint; no plaintext hash is retained.
pub fn protected_fingerprint(
    name: &str,
    version: u32,
    data: &[u8],
    key_id: &str,
    key: &[u8],
) -> Result<String, String> {
    let fp = edax_cqrs::protected::command_fingerprint(name, version, data, key_id, key).map_err(crate::to_wire)?;
    serde_json::to_string(&fp).map_err(|e| crate::to_wire(edax_core::Error::codec(e)))
}
/// Preserve the ordinary claim identity across protected-ledger migration.
pub fn protected_claim_key(scope: crate::KeyScope, key: &str) -> Result<String, String> {
    edax_cqrs::protected::claim_key(&scope.try_into().map_err(crate::to_wire)?, key).map_err(crate::to_wire)
}
/// Construct a protected pending record bound to its claim owner.
pub fn protected_pending(
    scope: crate::KeyScope,
    claim_id: &str,
    owner: &str,
    fingerprint: &str,
) -> Result<String, String> {
    let record = edax_cqrs::protected::ProtectedRecord::pending(
        scope.try_into().map_err(crate::to_wire)?,
        claim_id.to_owned(),
        owner.to_owned(),
        protected_fingerprint_record(fingerprint)?,
    )
    .map_err(crate::to_wire)?;
    edax_cqrs::protected::encode_record(&record).map_err(crate::to_wire)
}
/// Replay decision, with ciphertext retained until the provider authorizes opening.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProtectedReplay {
    pub action: String,
    pub encryption: Option<crate::EncryptionInfo>,
    pub code: Option<String>,
}
/// Stable protected-ledger replay decisions, defined by the owning kernel.
pub fn protected_replay_actions() -> Vec<String> {
    crate::names(edax_cqrs::protected::ProtectedReplayKind::NAMES)
}
/// Compare the complete protected fingerprint and security scope.
pub fn protected_replay(
    record: &str,
    scope: crate::KeyScope,
    claim_id: &str,
    fingerprint: &str,
) -> Result<ProtectedReplay, String> {
    use edax_cqrs::protected::ProtectedReplay as Decision;
    let record = protected_record(record)?;
    let fp = protected_fingerprint_record(fingerprint)?;
    let decision = record
        .replay(&scope.try_into().map_err(crate::to_wire)?, claim_id, &fp)
        .map_err(crate::to_wire)?;
    Ok(match decision {
        Decision::Pending => ProtectedReplay {
            action: decision.kind().as_str().into(),
            encryption: None,
            code: None,
        },
        Decision::Done(value) => ProtectedReplay {
            action: decision.kind().as_str().into(),
            encryption: Some(value.encryption.clone().into()),
            code: None,
        },
        Decision::Failed(code) => ProtectedReplay {
            action: decision.kind().as_str().into(),
            encryption: None,
            code: Some(code.as_str().into()),
        },
    })
}
/// Encrypt a successful command result and settle its fenced record.
pub fn protected_finish(
    record: &str,
    owner: &str,
    info: crate::EncryptionInfo,
    key: &[u8],
    plaintext: &[u8],
) -> Result<String, String> {
    let record = protected_record(record)?;
    let record = record
        .seal_result(owner, info.try_into().map_err(crate::to_wire)?, key, plaintext)
        .map_err(crate::to_wire)?;
    edax_cqrs::protected::encode_record(&record).map_err(crate::to_wire)
}
/// Persist only a stable failure code, with no arbitrary exception text.
pub fn protected_failure(record: &str, owner: &str, code: &str) -> Result<String, String> {
    let record = protected_record(record)?;
    let record = record
        .fail(owner, code.parse().map_err(crate::to_wire)?)
        .map_err(crate::to_wire)?;
    edax_cqrs::protected::encode_record(&record).map_err(crate::to_wire)
}
/// Authenticate a cached result after scoped provider resolution.
pub fn protected_open(record: &str, key: &[u8]) -> Result<Vec<u8>, String> {
    protected_record(record)?.open_result(key).map_err(crate::to_wire)
}
