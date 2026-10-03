//! edax-cqrs — the command-and-query consistency kernel.
//!
//! Both sides of the separation the name describes. The command bus (handler registration, typed
//! dispatch, the middleware chain) is host-local ergonomics with no cross-language requirement, so
//! it lives in the SDKs. The kernel owns only the decisions that must agree across languages
//! and processes.
//!
//! Command side — what makes a command's identity:
//!
//! - [`canonical_json`] — re-emit JSON in the RFC 8785 canonical form so the same logical command
//!   serializes identically regardless of how the host produced it, or which `serde_json` features a
//!   build turned on. An integer literal past 2^53, which that form would round, is refused.
//! - [`payload_hash`] — a stable hash of bytes, for idempotency-key payload binding.
//! - [`command_hash`] — *which* bytes a command's idempotency hash covers: the whole command value,
//!   serialized, canonicalized, hashed. The input rule is the kernel's, not the host's.
//! - [`Record`] / [`outcome`] — the stored idempotency record's wire shape, its reading order, and
//!   the verdict a losing claimant reaches from it (in flight, cached failure, cached success).
//!
//! Query side — what a read model answers with:
//!
//! - [`filter`] — the backend-neutral filter AST and its evaluator, whose match decision must be
//!   identical in every language.
//! - [`mod@query`] — sort and pagination semantics over the documents a filter selects, including
//!   the keyset cursor a page boundary is minted as, bound to the sort it was minted under.
//!
//! The two travel together because the query evaluates the filter: a read model that pages results
//! needs both, and neither is an event-sourcing concern. Shared primitives (hashing, backoff, the
//! error taxonomy) live in `edax-core`; the event-driven kernel is `edax-event`.

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

mod canonical;
pub mod filter;
pub mod query;
pub mod record;

pub use canonical::canonical_json;
use edax_core::Error;

pub use filter::{eval, eval_json, Filter, Operator};
// The whole query surface, so no binding reaches into `query::` and makes a module path an
// external contract.
pub use query::{
    decode_cursor, decode_cursor_json, effective_sort, effective_sort_json, encode_cursor, encode_cursor_json, query,
    query_json, query_spec, Page, Query, SortKey, TIEBREAK_PATH,
};
pub use record::{cached_failure, cached_failure_of, decode_prior, decode_record, encode_record, encode_record_fields, outcome, settle, Outcome, Record, Settle, State};

/// Stable 64-bit hash of bytes (FNV-1a). Identical across every binding. For commands, hash the
/// output of [`canonical_json`] so two services agree on whether a reused idempotency key carries
/// the same command.
pub fn payload_hash(bytes: &[u8]) -> u64 {
    edax_core::fnv1a64(bytes)
}

/// The idempotency hash of a command: the **whole command value** serialized as one JSON object,
/// then [`canonical_json`], then [`payload_hash`].
///
/// This is the input rule, and it is the kernel's so that no SDK (and no user) picks the bytes. A
/// host that cannot hand the kernel a Rust value serializes the command with its own standard
/// encoder (Python: `msgspec.json.encode`) and hands the kernel the JSON — that is what the ABI's
/// `payload_hash_json` is. Both paths land on the same bytes because canonicalization removes
/// everything an encoder is free to choose: key order, whitespace, string escaping, and how a number
/// is spelled (`2` and `2.0` are one number). A command holding an integer past 2^53 is refused, since
/// the canonical form's doubles would round it onto its neighbour.
///
/// What the value *contains* is its wire shape — the field names and which fields exist — and
/// that belongs to the host, as it does for events: two services that share an idempotency store
/// declare the command with the same fields. Two consequences follow from "the whole value":
///
/// - The field the idempotency key was read from is hashed too. Reusing a key with a different
///   command is a mismatch even when only that field moved.
/// - Routing metadata is not a field. A command's type name is `command_type()` in Rust and a
///   class attribute in Python, so it stays out of the hash on both sides alike.
///
/// `testdata/golden/command_hash.json` maps commands to hashes; both SDK suites dispatch each one
/// through their own `idempotent` middleware and compare the stored hash.
///
/// Both failures are `codec`: the same command fails the same way every time.
pub fn command_hash<C: serde::Serialize + ?Sized>(command: &C) -> Result<u64, Error> {
    // Serialization then canonicalization report separately: a value serde cannot encode is a
    // different defect from JSON the kernel cannot read, and the message says which.
    let json =
        serde_json::to_vec(command).map_err(|e| Error::codec(format_args!("command payload is not JSON-encodable: {e}")))?;
    let canonical = canonical_json(&json)
        .map_err(|e| Error::codec(format_args!("command payload canonicalization failed: {}", e.message)))?;
    Ok(payload_hash(&canonical))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A realistic command payload.
    const COMMAND: &str = r#"{"age":36,"email":"ada@example.com","tags":["eu","paid"]}"#;

    #[test]
    fn canonical_json_is_the_hash_input() {
        // The point of canonicalizing: two spellings of one command bind to the same idempotency key.
        let terse = canonical_json(COMMAND.as_bytes()).unwrap();
        let roomy = canonical_json(br#"{ "age": 36, "tags": ["eu", "paid"], "email": "ada@example.com" }"#).unwrap();
        assert_eq!(payload_hash(&terse), payload_hash(&roomy));

        // ...and two different commands do not.
        let other = canonical_json(br#"{"email":"grace@example.com","age":36,"tags":["eu","paid"]}"#).unwrap();
        assert_ne!(payload_hash(&terse), payload_hash(&other));
    }

    #[test]
    fn payload_hash_is_fnv1a64() {
        // Known FNV-1a vectors: every binding must reproduce these bit for bit.
        assert_eq!(payload_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(payload_hash(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(
            payload_hash("café ☕".as_bytes()),
            edax_core::fnv1a64("café ☕".as_bytes())
        );
    }

    #[test]
    fn command_hash_is_the_whole_value_canonicalized() {
        #[derive(serde::Serialize)]
        struct PlaceOrder {
            // Declared out of alphabetical order: canonicalization, not declaration order, decides.
            total_cents: i64,
            order_id: &'static str,
            request_id: &'static str,
        }
        let cmd = PlaceOrder { total_cents: 4200, order_id: "ord-7f3c", request_id: "req-1" };
        let expected = payload_hash(br#"{"order_id":"ord-7f3c","request_id":"req-1","total_cents":4200}"#);
        assert_eq!(command_hash(&cmd), Ok(expected));

        // The idempotency-key field is part of the value, so it is part of the hash.
        let body_only = payload_hash(br#"{"order_id":"ord-7f3c","total_cents":4200}"#);
        assert_ne!(command_hash(&cmd), Ok(body_only));

        // A serde_json::Value with the same fields is the same command: the golden generator builds
        // its cases this way, the SDKs from typed structs, and both must agree.
        let value = serde_json::json!({ "request_id": "req-1", "total_cents": 4200, "order_id": "ord-7f3c" });
        assert_eq!(command_hash(&value), Ok(expected));
    }

    #[test]
    fn command_hash_reports_a_value_serde_cannot_encode() {
        struct Unencodable;
        impl serde::Serialize for Unencodable {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("secret field must not leave the process"))
            }
        }
        let error = command_hash(&Unencodable).expect_err("serialization fails");
        assert_eq!(error, Error::codec("command payload is not JSON-encodable: secret field must not leave the process"));
    }
}
