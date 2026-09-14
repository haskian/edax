//! The idempotency record: the stored outcome of a dispatch under an idempotency key, and the
//! verdict a losing claimant reaches from it. Every SDK and every JSON-backed store shares this one
//! wire shape and one reading order, so a record written in one language replays identically in the
//! other. The golden vector `testdata/golden/idempotency_record.json` pins both.
//!
//! Wire shape — flat JSON, fixed key order:
//! `{"payload_hash": u64, "result": <json>, "error": string|null, "pending": bool}`
//!
//! Reading order: `pending` wins, then `error`, then `result`. A missing `pending` reads as `false`
//! (records written before the field existed). A missing `result` reads as `null`.
//!
//! A `null` result on a completed, non-failed record is a **success whose value is null** — Python
//! `None`, Rust `()` — not "no result". A typed reader decodes the stored value as its result type,
//! so `null` decodes to `()` for a unit-result command and fails, typed, for a command whose result
//! type has no null form.
//!
//! Number bound: a `result` integer is exact within the `i64`/`u64` range and is read back as an
//! `f64` outside it (`serde_json` without `arbitrary_precision`, which `edax-core`'s `number_cmp`
//! relies on). Pinned by [`tests::result_integers_are_exact_only_within_64_bits`].

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{decide, Decision};

/// The stored outcome for an idempotency key. `R` is the handler's result type: the host's own
/// type in an in-process store, [`serde_json::Value`] at a JSON boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<R> {
    /// Hash of the payload the key was claimed with, so a reused key with different bytes is caught.
    pub payload_hash: u64,
    /// Where the claim stands: pending, failed, or done with a result.
    pub state: State<R>,
}

/// Where a claimed key stands. An enum, not three optional fields, so a record cannot be both
/// pending and failed: the wire's precedence is applied once, in [`State::from_wire`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State<R> {
    /// The claim is taken and the handler has not completed.
    Pending,
    /// The handler failed deterministically; a redelivery replays this message.
    Failed(String),
    /// The handler completed; a redelivery replays this value (which may be JSON `null`).
    Done(R),
}

impl<R> Record<R> {
    /// A claim that has been taken but whose handler has not completed.
    pub fn pending(payload_hash: u64) -> Self {
        Self {
            payload_hash,
            state: State::Pending,
        }
    }

    /// A completed success carrying the handler's result.
    pub fn done(payload_hash: u64, result: R) -> Self {
        Self {
            payload_hash,
            state: State::Done(result),
        }
    }

    /// A completed terminal failure carrying the error message.
    pub fn failed(payload_hash: u64, error: impl Into<String>) -> Self {
        Self {
            payload_hash,
            state: State::Failed(error.into()),
        }
    }
}

impl<R> State<R> {
    /// Read the wire's three fields in the kernel's order: `pending` wins, then `error`, then the
    /// result. This is the single place the precedence is written down.
    pub fn from_wire(pending: bool, error: Option<String>, result: R) -> Self {
        match (pending, error) {
            (true, _) => State::Pending,
            (false, Some(error)) => State::Failed(error),
            (false, None) => State::Done(result),
        }
    }

    /// Stable lowercase name of the variant, shared with every binding.
    pub fn as_str(&self) -> &'static str {
        match self {
            State::Pending => "pending",
            State::Failed(_) => "failed",
            State::Done(_) => "done",
        }
    }
}

/// The wire shape, serialized. Borrowed so encoding never clones the result.
#[derive(Serialize)]
struct WireRef<'a, R> {
    payload_hash: u64,
    result: Option<&'a R>,
    error: Option<&'a str>,
    pending: bool,
}

/// The wire shape, parsed. Every field but the hash is optional on the way in (Postel's law): an
/// older or foreign writer may omit `pending` or `result`.
#[derive(Deserialize)]
struct WireOwned {
    payload_hash: u64,
    #[serde(default)]
    result: Value,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    pending: bool,
}

impl<R: Serialize> Serialize for Record<R> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (result, error, pending) = match &self.state {
            State::Pending => (None, None, true),
            State::Failed(error) => (None, Some(error.as_str()), false),
            State::Done(result) => (Some(result), None, false),
        };
        WireRef {
            payload_hash: self.payload_hash,
            result,
            error,
            pending,
        }
        .serialize(serializer)
    }
}

impl<'de, R: DeserializeOwned> Deserialize<'de> for Record<R> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Through a `Value` first: the derive would also read a JSON array positionally, and a
        // record is an object by contract.
        let value = Value::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom("idempotency record is not a JSON object"));
        }
        let wire: WireOwned = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        // The result is decoded as `R` only on the `Done` branch: a pending or failed record carries
        // `null` there, which must not be forced through a result type that has no null form.
        let state = match State::from_wire(wire.pending, wire.error, wire.result) {
            State::Pending => State::Pending,
            State::Failed(error) => State::Failed(error),
            State::Done(value) => State::Done(
                serde_json::from_value(value)
                    .map_err(|e| serde::de::Error::custom(format!("cached result is not a valid replay value: {e}")))?,
            ),
        };
        Ok(Record {
            payload_hash: wire.payload_hash,
            state,
        })
    }
}

/// The canonical bytes of `record`: the only encoding a store may persist.
pub fn encode_record<R: Serialize>(record: &Record<R>) -> Result<Vec<u8>, String> {
    serde_json::to_vec(record).map_err(|e| e.to_string())
}

/// Parse stored bytes into a record whose result is decoded as `R`.
pub fn decode_record<R: DeserializeOwned>(json: &[u8]) -> Result<Record<R>, String> {
    serde_json::from_slice(json).map_err(|e| e.to_string())
}

/// What a claimant that lost the claim does with the winner's record. The four variants are the
/// whole classification (a hash check, then the three [`State`]s), so a new one is a wire-format
/// change and should break every SDK at compile time rather than be a catch-all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome<'a, R> {
    /// The key was reused with a different command. Terminal.
    PayloadMismatch,
    /// The winner has not completed. Transient: retry or redeliver.
    InProgress,
    /// The winner failed deterministically. Terminal: replay the message.
    Failed(&'a str),
    /// The winner succeeded. Replay the value.
    Succeeded(&'a R),
}

impl<R> Outcome<'_, R> {
    /// Stable lowercase name — the single source of truth for the string form across bindings.
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::PayloadMismatch => "payload_mismatch",
            Outcome::InProgress => "in_progress",
            Outcome::Failed(_) => "failed",
            Outcome::Succeeded(_) => "succeeded",
        }
    }
}

/// Classify the winner's record against this command's payload hash. The hash is checked first
/// (via [`decide`]) so a mismatched payload is rejected even while the winner is still in flight;
/// then the record's state decides.
pub fn outcome<R>(prior: &Record<R>, current: u64) -> Outcome<'_, R> {
    match decide(Some(prior.payload_hash), current) {
        Decision::PayloadMismatch => Outcome::PayloadMismatch,
        // `Proceed` needs an absent prior, which `Some(..)` above rules out; only `Replay` remains.
        Decision::Proceed | Decision::Replay => match &prior.state {
            State::Pending => Outcome::InProgress,
            State::Failed(error) => Outcome::Failed(error),
            State::Done(result) => Outcome::Succeeded(result),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    // A real 64-bit payload hash, past 2^53, so a lossy (float) reader would be caught.
    const HASH: u64 = 11_234_062_831_410_934_249;

    #[test]
    fn from_wire_applies_the_precedence() {
        for (name, pending, error, want, want_str) in [
            (
                "pending_wins_over_error_and_result",
                true,
                Some("boom".to_owned()),
                State::Pending,
                "pending",
            ),
            (
                "error_wins_over_result",
                false,
                Some("boom".to_owned()),
                State::Failed("boom".to_owned()),
                "failed",
            ),
            ("neither_is_done", false, None, State::Done(42), "done"),
        ] {
            let got = State::from_wire(pending, error, 42);
            assert_eq!(got, want, "{name}");
            assert_eq!(got.as_str(), want_str, "{name}");
        }
    }

    #[test]
    fn decode_branches() {
        struct Case {
            name: &'static str,
            json: &'static str,
            want: Result<Record<Value>, &'static str>,
        }
        let cases = [
            Case {
                name: "legacy_record_without_pending_reads_as_done",
                json: r#"{"payload_hash":11234062831410934249,"result":3,"error":null}"#,
                want: Ok(Record::done(HASH, json!(3))),
            },
            Case {
                name: "absent_result_reads_as_null",
                json: r#"{"payload_hash":11234062831410934249,"error":null,"pending":false}"#,
                want: Ok(Record::done(HASH, Value::Null)),
            },
            Case {
                name: "unknown_fields_are_ignored",
                json: r#"{"payload_hash":11234062831410934249,"result":3,"error":null,"pending":false,"ttl":9}"#,
                want: Ok(Record::done(HASH, json!(3))),
            },
            Case {
                name: "malformed_json",
                json: r#"{"payload_hash":"#,
                want: Err("EOF while parsing"),
            },
            Case {
                name: "missing_payload_hash",
                json: r#"{"result":3}"#,
                want: Err("missing field `payload_hash`"),
            },
            Case {
                name: "float_payload_hash_is_rejected_not_rounded",
                json: r#"{"payload_hash":1.5,"result":3}"#,
                want: Err("invalid type: floating point"),
            },
            Case {
                name: "not_an_object",
                json: "[1,2]",
                want: Err("not a JSON object"),
            },
        ];
        for case in cases {
            let got = decode_record::<Value>(case.json.as_bytes());
            match case.want {
                Ok(want) => assert_eq!(got.as_ref(), Ok(&want), "{}", case.name),
                Err(cause) => {
                    let message = got.expect_err(case.name);
                    assert!(message.contains(cause), "{}: {message}", case.name);
                }
            }
        }
    }

    #[test]
    fn typed_decode_reads_null_as_the_result_type() {
        // The divergence this module settles: a stored `null` result is a success. A unit-result
        // command decodes it as `()`; a command whose result type has no null form gets a typed error
        // instead of a fabricated value. A pending or failed record never touches the result type.
        let null_success = br#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":false}"#;
        assert_eq!(decode_record::<()>(null_success), Ok(Record::done(HASH, ())));
        assert_eq!(decode_record::<Option<i64>>(null_success), Ok(Record::done(HASH, None)));
        let err = decode_record::<i64>(null_success).expect_err("null is not an i64");
        assert!(err.contains("cached result is not a valid replay value"), "{err}");

        let pending = br#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":true}"#;
        assert_eq!(decode_record::<i64>(pending), Ok(Record::pending(HASH)));
        let failed = br#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","pending":false}"#;
        assert_eq!(decode_record::<i64>(failed), Ok(Record::failed(HASH, "boom")));
    }

    #[test]
    fn encode_is_the_fixed_wire_shape() {
        for (name, record, want) in [
            (
                "pending",
                Record::<Value>::pending(HASH),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":true}"#,
            ),
            (
                "done",
                Record::done(HASH, json!({"z": 1, "a": [1, 2]})),
                r#"{"payload_hash":11234062831410934249,"result":{"a":[1,2],"z":1},"error":null,"pending":false}"#,
            ),
            (
                "done_with_null",
                Record::done(HASH, Value::Null),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":false}"#,
            ),
            (
                "failed",
                Record::failed(HASH, "boom"),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","pending":false}"#,
            ),
        ] {
            assert_eq!(encode_record(&record).unwrap(), want.as_bytes(), "{name}");
            // Round trip is lossless: the wire is the record.
            assert_eq!(decode_record::<Value>(want.as_bytes()).unwrap(), record, "{name}");
        }
        // A unit result serializes as `null`, the same bytes as `done_with_null` above.
        let unit = encode_record(&Record::done(HASH, ())).unwrap();
        assert_eq!(unit, encode_record(&Record::done(HASH, Value::Null)).unwrap());
    }

    #[test]
    fn encode_names_a_result_json_cannot_express() {
        // A handler result JSON has no form for (here, a map keyed by a tenant/sku pair) must fail
        // loudly: the claim is already taken, so a silently dropped record would strand the key.
        let by_tenant_sku = BTreeMap::from([(("acme".to_owned(), "sku-1".to_owned()), 3u32)]);
        let error = encode_record(&Record::done(HASH, by_tenant_sku)).expect_err("tuple keys are not JSON");
        assert!(error.contains("key must be a string"), "{error}");
    }

    #[test]
    fn result_integers_are_exact_only_within_64_bits() {
        // The bound every JSON-backed store inherits: a result integer past the u64 range (or below
        // i64::MIN) comes back as a double, equal in value where a double can hold it and lossy
        // beyond. Known and pinned rather than discovered in production.
        struct Case {
            name: &'static str,
            literal: &'static str,
            want: Value,
            exact: bool,
        }
        let cases = [
            Case {
                name: "past 2^53 is still an exact integer",
                literal: "9007199254740993",
                want: json!(9007199254740993u64),
                exact: true,
            },
            Case {
                name: "u64::MAX is exact",
                literal: "18446744073709551615",
                want: json!(u64::MAX),
                exact: true,
            },
            Case {
                name: "2^64 becomes a double, equal in value",
                literal: "18446744073709551616",
                want: json!(1.8446744073709552e19),
                exact: false,
            },
            Case {
                name: "below i64::MIN becomes a double, lossy",
                literal: "-9223372036854775809",
                want: json!(-9.223372036854776e18),
                exact: false,
            },
        ];
        for c in cases {
            let stored = format!(
                r#"{{"payload_hash":11234062831410934249,"result":{},"error":null,"pending":false}}"#,
                c.literal
            );
            let record = decode_record::<Value>(stored.as_bytes()).unwrap();
            let State::Done(got) = &record.state else {
                panic!("{}: not a success", c.name)
            };
            assert_eq!(*got, c.want, "{}", c.name);
            assert_eq!(got.is_i64() || got.is_u64(), c.exact, "{}: integer-ness", c.name);
            // Re-encoding writes the double back, so the loss is visible in the stored text too.
            let text = String::from_utf8(encode_record(&record).unwrap()).unwrap();
            assert_eq!(text.contains(c.literal), c.exact, "{}: re-encoded {text}", c.name);
        }
    }

    #[test]
    fn outcome_branches() {
        let other = HASH ^ 1;
        for (name, prior, current, want) in [
            (
                "pending_is_in_progress",
                Record::pending(HASH),
                HASH,
                Outcome::InProgress,
            ),
            (
                "failed_replays_the_error",
                Record::failed(HASH, "boom"),
                HASH,
                Outcome::Failed("boom"),
            ),
            (
                "done_replays_the_value",
                Record::done(HASH, 42),
                HASH,
                Outcome::Succeeded(&42),
            ),
            // The hash is checked before the state: a mismatch is rejected whatever the winner did.
            (
                "mismatch_beats_pending",
                Record::pending(HASH),
                other,
                Outcome::PayloadMismatch,
            ),
            (
                "mismatch_beats_failed",
                Record::failed(HASH, "boom"),
                other,
                Outcome::PayloadMismatch,
            ),
            (
                "mismatch_beats_done",
                Record::done(HASH, 42),
                other,
                Outcome::PayloadMismatch,
            ),
        ] {
            let got = outcome(&prior, current);
            assert_eq!(got, want, "{name}");
        }
    }
}
