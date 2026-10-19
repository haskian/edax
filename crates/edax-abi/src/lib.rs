//! Flat, plain-Rust API surface over the edax kernels and adapters.
//!
//! This crate holds no logic of its own: every function forwards to a kernel (`edax-core` /
//! `edax-event` / `edax-cqrs` / `edax-kafka` / `edax-redis` / `edax-postgres`) or to a backend (`edax-memory`),
//! reshaping only at the edges so the surface is trivial to bind. alef extracts these signatures and
//! emits the native binding from them, so the shapes here are chosen to map cleanly:
//!
//! - A fallible call returns `Result<_, String>`, and the generator raises on `Err`. The text is
//!   always a kernel [`edax_core::Error`]'s wire form `"<code>: <message>"`, so the code comes from
//!   the kernel and never from a choice made here.
//! - Bytes are `Vec<u8>` / `&[u8]`.
//! - Dictionaries are `HashMap<String, String>`, converted to the kernel's ordered `BTreeMap` where
//!   deterministic encoding requires it.
//! - An aggregate result is a plain struct rather than a tuple.
//!
//! One module per kernel crate holds its surface, and every module is re-exported here, so the binding
//! sees one flat namespace. This file keeps only the helpers the modules share.

use std::collections::{BTreeMap, HashMap};

use edax_core::ErrorCode;

mod cqrs;
mod event;
mod kafka;
mod memory;
mod postgres;
mod primitives;
mod redis;
mod retry;
mod saga;
mod seal;
#[cfg(test)]
mod testkit;

pub use cqrs::*;
pub use event::*;
pub use kafka::*;
pub use memory::*;
pub use postgres::*;
pub use primitives::*;
pub use redis::*;
pub use retry::*;
pub use saga::*;
pub use seal::*;

/// A kernel failure as it crosses: the kernel's own wire form `"<code>: <message>"`, nothing added.
/// Every kernel function fails with [`edax_core::Error`], so the code is always the kernel's and a
/// call site here cannot pick a wrong one, or none.
fn to_wire(error: edax_core::Error) -> String {
    error.to_string()
}

fn read_json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, String> {
    serde_json::from_str(value).map_err(|e| to_wire(edax_core::Error::codec(e)))
}

fn write_json<T: serde::Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| to_wire(edax_core::Error::codec(e)))
}

/// The taxonomy code a host names, `None` for a failure no code classifies. A code the kernel does not
/// know is a `codec:` error.
fn error_code(code: Option<String>) -> Result<Option<ErrorCode>, String> {
    code.map(|code| code.parse::<ErrorCode>()).transpose().map_err(to_wire)
}

/// The kind of a failure that carries the taxonomy code a host names, `None` for one no code classifies.
/// A code the kernel does not know is a `codec:` error.
fn failure_of(code: Option<String>) -> Result<edax_event::Failure, String> {
    Ok(edax_event::Failure::of(error_code(code)?))
}

/// The tenancy mode a host names (`"silo"`, `"pool"`, `"bridge"`); any other name is a `codec:` error.
fn tenancy_mode(mode: &str) -> Result<edax_core::Tenancy, String> {
    edax_core::Tenancy::from_name(mode).map_err(to_wire)
}

/// A kernel's closed set of names (a `closed_names!` enum's `NAMES`) as the host list it checks its own
/// names against.
fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|&name| name.to_owned()).collect()
}

/// A kernel count as the boundary's fixed-width `u64`, saturating on a target wider than 64 bits.
fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// A boundary count as the host width, saturating rather than truncating on narrower targets.
fn host_count(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

/// A host dictionary as the kernel's key-ordered map, so encoded bytes are deterministic.
fn ordered(map: HashMap<String, String>) -> BTreeMap<String, String> {
    map.into_iter().collect()
}

/// The kernel's key-ordered map as a host dictionary.
fn unordered(map: BTreeMap<String, String>) -> HashMap<String, String> {
    map.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;

    #[test]
    fn closed_sets_are_the_kernels_names() {
        // A host checks its own names against these lists at import, so each must be the kernel's own.
        let names = |names: Vec<String>| names.join(",");
        assert_eq!(names(tenancy_modes()), "silo,pool,bridge");
        assert_eq!(names(idempotency_states()), "pending,failed,done");
        assert_eq!(
            names(idempotency_outcomes()),
            "payload_mismatch,in_progress,failed,succeeded"
        );
        assert_eq!(names(saga_write_actions()), "nothing,save,complete");
        assert_eq!(names(saga_admit_actions()), "completed,repeat,below_mark,fold");
        assert_eq!(names(drain_actions()), "hand_back,dead_letter,stop");
        assert_eq!(names(message_kinds()), "event,command");
        assert_eq!(names(wire_formats()), "json,msgpack");
    }

    #[test]
    fn kernel_failures_carry_the_codec_code() {
        // Every deterministic marshaling failure must name its cause *and* carry the taxonomy code, or
        // the host falls back to a transient store error and retries a failure that always repeats.
        let valid_doc = r#"{"id":"user-1","age":36}"#;
        let cases: [(&str, Result<(), String>, &str); 22] = [
            (
                "filter_is_not_json",
                eval_filter("{not json}", valid_doc).map(|_| ()),
                "key must be a string at line 1 column 2",
            ),
            (
                "filter_is_not_a_filter",
                eval_filter(r#"{"nope":1}"#, valid_doc).map(|_| ()),
                "unknown variant",
            ),
            (
                "document_is_not_json",
                eval_filter(r#"{"exists":{"path":"id","exists":true}}"#, "nope").map(|_| ()),
                "expected ident",
            ),
            (
                "query_spec_is_not_json",
                query("{not json}", "[]").map(|_| ()),
                "key must be a string at line 1 column 2",
            ),
            (
                "cursor_is_not_base64",
                query(r#"{"sort":[{"path":"age"}],"after":"!!!"}"#, "[]").map(|_| ()),
                "not base64url",
            ),
            (
                "cursor_document_is_not_json",
                encode_cursor("nope", "[]").map(|_| ()),
                "expected ident",
            ),
            (
                "command_payload_is_not_json",
                payload_hash_json(b"{not json}").map(|_| ()),
                "key must be a string at line 1 column 2",
            ),
            (
                "wire_bytes_are_not_the_format",
                decode_event("json", b"not an event").map(|_| ()),
                "decode",
            ),
            (
                "command_bytes_are_not_the_format",
                decode_command("msgpack", b"not a command").map(|_| ()),
                "decode",
            ),
            (
                "command_name_payload_is_not_json",
                command_hash_json("IssueInvoice", b"{not json}").map(|_| ()),
                "not one JSON value",
            ),
            (
                // A producer never mints a message every consumer would refuse.
                "event_extension_key_is_refused",
                encode_event(
                    "json",
                    EventMessage {
                        extensions: HashMap::from([("X-Trace".to_owned(), String::new())]),
                        ..event_message("evt-1", Some(1))
                    },
                )
                .map(|_| ()),
                "extension key \"X-Trace\"",
            ),
            (
                "command_extension_key_is_refused",
                encode_command(
                    "msgpack",
                    CommandMessage {
                        extensions: HashMap::from([(String::new(), String::new())]),
                        ..command_message(None)
                    },
                )
                .map(|_| ()),
                "extension key \"\"",
            ),
            (
                "cached_result_is_not_json",
                encode_idempotency_record(HASH, false, "{not json}", None, None, None).map(|_| ()),
                "key must be a string",
            ),
            (
                "cached_error_code_is_unknown",
                encode_idempotency_record(
                    HASH,
                    false,
                    "null",
                    Some("boom".to_owned()),
                    Some("timeout".to_owned()),
                    None,
                )
                .map(|_| ()),
                "unknown error code: timeout",
            ),
            // A batch refusal and an unknown code carry the code too, or a host reads them as transient.
            (
                "sequence_batch_has_a_gap",
                check_sequence_batch(1, vec![Some(2), Some(4)]).map(drop),
                "event 1 has sequence 4, expected 3",
            ),
            (
                "error_code_is_unknown",
                error_verdict(Some("timeout".to_owned())).map(|_| ()),
                "unknown error code: timeout",
            ),
            (
                "record_is_not_a_json_object",
                decode_idempotency_record("[11234062831410934249]").map(|_| ()),
                "idempotency record is not a JSON object",
            ),
            (
                "record_has_no_payload_hash",
                decode_idempotency_record(r#"{"pending":true}"#).map(|_| ()),
                "missing field `payload_hash`",
            ),
            (
                // A JSON reader that rounds the hash through a float writes it back as a string or a
                // decimal; either is terminal, not a store hiccup to retry.
                "record_hash_is_not_an_integer",
                decode_idempotency_record(r#"{"payload_hash":"11234062831410934249"}"#).map(|_| ()),
                "invalid type: string",
            ),
            (
                // A format name no kernel knows repeats identically on every retry, so it carries
                // the terminal code rather than a transient one the host would retry.
                "media_type_format_is_not_a_format",
                media_type("protobuf", "event").map(|_| ()),
                "unknown format: protobuf",
            ),
            (
                "media_type_kind_is_not_a_kind",
                media_type("json", "query").map(|_| ()),
                "unknown message kind: query",
            ),
            (
                // A Redis kernel failure reaches the host under the same terminal code.
                "redis_claim_handle_is_not_a_claim",
                redis_settled_value(b"1deadbeef", "{}").map(|_| ()),
                "claim handle is not a claim this kernel minted",
            ),
        ];
        for (name, got, cause) in cases {
            let message = got.expect_err(name);
            assert!(message.starts_with("codec: "), "{name}: {message}");
            assert!(message.contains(cause), "{name}: {message}");
        }

        // An unknown format is rejected on both directions of the codec, and names the format.
        for message in [
            encode_event("protobuf", event_message("evt-1", Some(1))).unwrap_err(),
            decode_event("protobuf", b"{}").map(|_| ()).unwrap_err(),
            encode_command("protobuf", command_message(None)).unwrap_err(),
            decode_command("protobuf", b"{}").map(|_| ()).unwrap_err(),
        ] {
            assert_eq!(message, "codec: unknown format: protobuf");
        }
    }
}
