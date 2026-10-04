//! The idempotency record: the stored outcome of a dispatch under an idempotency key, and the
//! verdict a losing claimant reaches from it. Every SDK and every JSON-backed store shares this one
//! wire shape and one reading order, so a record written in one language replays identically in the
//! other. The golden vector `testdata/golden/idempotency_record.json` pins both.
//!
//! Wire shape — flat JSON, fixed key order:
//! `{"payload_hash": u64, "result": <json>, "error": string|null, "error_code": string|null, "pending": bool}`
//!
//! Reading order: `pending` wins, then `error`, then `result`. A missing `pending` reads as `false`
//! (records written before the field existed). A missing `result` reads as `null`.
//!
//! A failed record carries the failure's message in `error` and its taxonomy code in `error_code`,
//! side by side, so a replay in any SDK keeps the original code. A sibling field rather than an
//! object in `error`: a reader that predates it ignores the unknown field and still reads the
//! failure, which keeps a shared ledger readable through a rolling deploy. A missing, `null` or
//! unknown `error_code` reads as no code; the failure is still a failure. An empty `error` is still
//! a failure: only `null` means "not failed".
//!
//! A `null` result on a completed, non-failed record is a **success whose value is null** — Python
//! `None`, Rust `()` — not "no result". A typed reader decodes the stored value as its result type,
//! so `null` decodes to `()` for a unit-result command and fails, typed, for a command whose result
//! type has no null form.
//!
//! A stored record no reader can decode is still a cached outcome: [`decode_prior`] reads it as a
//! terminal `codec` failure, so every SDK replays it as a cached failure instead of running the
//! handler again or raising on every redelivery.
//!
//! Number bound: a `result` integer is exact within the `i64`/`u64` range and is read back as an
//! `f64` outside it (`serde_json` without `arbitrary_precision`, which the filter's `number_cmp`
//! relies on). Pinned by the test `result_integers_are_exact_only_within_64_bits`.

use edax_core::{Error, ErrorCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;


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
    /// The handler failed deterministically; a redelivery replays this failure.
    Failed {
        /// The failure's taxonomy code, or `None` for a record whose writer stored none.
        code: Option<ErrorCode>,
        /// The failure's message, without the code.
        message: String,
    },
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

    /// A completed terminal failure: its code and its message.
    pub fn failed(payload_hash: u64, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            payload_hash,
            state: State::Failed {
                code: Some(code),
                message: message.into(),
            },
        }
    }

    /// The record that stands in for a stored one no reader can decode: a terminal `codec` failure whose
    /// message names `cause`, the decode error. See [`decode_prior`] for which hash it carries.
    pub fn unreadable(payload_hash: u64, cause: &Error) -> Self {
        Self::failed(payload_hash, ErrorCode::Codec, format!("unreadable idempotency record: {}", cause.message))
    }
}

impl<R> State<R> {
    /// Read the wire's fields in the kernel's order: `pending` wins, then `error`, then the
    /// result. This is the single place the precedence is written down. `error_code` is read only
    /// beside an `error`, and a name the taxonomy does not know reads as no code.
    pub fn from_wire(pending: bool, error: Option<String>, error_code: Option<&str>, result: R) -> Self {
        match (pending, error) {
            (true, _) => State::Pending,
            (false, Some(message)) => State::Failed { code: stored_code(error_code), message },
            (false, None) => State::Done(result),
        }
    }

    /// Stable lowercase name of the variant, shared with every binding.
    pub fn as_str(&self) -> &'static str {
        match self {
            State::Pending => "pending",
            State::Failed { .. } => "failed",
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
    error_code: Option<&'static str>,
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
    error_code: Option<String>,
    #[serde(default)]
    pending: bool,
}

impl<R: Serialize> Serialize for Record<R> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (result, error, error_code, pending) = match &self.state {
            State::Pending => (None, None, None, true),
            State::Failed { code, message } => (None, Some(message.as_str()), code.map(ErrorCode::as_str), false),
            State::Done(result) => (Some(result), None, None, false),
        };
        WireRef {
            payload_hash: self.payload_hash,
            result,
            error,
            error_code,
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
        let state = match State::from_wire(wire.pending, wire.error, wire.error_code.as_deref(), wire.result) {
            State::Pending => State::Pending,
            State::Failed { code, message } => State::Failed { code, message },
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

/// The canonical bytes of `record`: the only encoding a store may persist. A result JSON cannot
/// express is a `codec` error.
pub fn encode_record<R: Serialize>(record: &Record<R>) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(record).map_err(Error::codec)
}

/// Parse stored bytes into a record whose result is decoded as `R`. Unreadable bytes are a `codec`
/// error. A store reads the record that holds a key with [`decode_prior`] instead.
pub fn decode_record<R: DeserializeOwned>(json: &[u8]) -> Result<Record<R>, Error> {
    serde_json::from_slice(json).map_err(Error::codec)
}

/// The record a losing claimant reads from the bytes that hold its key. Never fails: bytes
/// [`decode_record`] refuses (garbage, a torn write, another layout, a field of the wrong type, a
/// result that is not an `R`) are [`Record::unreadable`], a terminal `codec` failure, so every SDK
/// replays them as a cached failure. The handler never runs again for the key, and a dispatcher
/// dead-letters the message instead of redelivering it until the record's retention ends.
///
/// The failure keeps the stored `payload_hash` when that field alone still reads as a `u64` in a JSON
/// object. The kernel checks the hash first, so a claimant carrying a *different* command still learns
/// that the key was reused, not that its own command failed. When the hash itself is unreadable, the
/// record carries `claimant_hash`: nothing tells the two commands apart, so the claimant gets the
/// failure, the side that never runs a handler twice.
pub fn decode_prior<R: DeserializeOwned>(bytes: &[u8], claimant_hash: u64) -> Record<R> {
    decode_record(bytes).unwrap_or_else(|cause| Record::unreadable(stored_hash(bytes).unwrap_or(claimant_hash), &cause))
}

/// Whether the record stored under `key` may be forgotten: deleted so a re-driven message runs its
/// handler again.
///
/// A pending claim is refused with [`unsettled_claim`]: its run may still complete, and its record
/// would land after the delete. A completed or failed record may go, and so may bytes no reader
/// decodes, which [`decode_prior`] replays as a cached failure: forgetting them is the way out of a
/// poisoned key. Every store decides with this one function, pinned by the ledger goldens.
pub fn forget(key: &str, record: &[u8]) -> Result<(), Error> {
    // Read as `()`: only the state matters, and `pending` is read before the result.
    if matches!(decode_prior::<()>(record, 0).state, State::Pending) {
        return Err(unsettled_claim(key));
    }
    Ok(())
}

/// The `in_progress` refusal of a forget of `key` while a claim on it has not settled: its run may
/// still complete, and its record would land after the delete. One wording for every store, so a
/// store that sees the claim before it reads the record refuses as [`forget`] does.
pub fn unsettled_claim(key: &str) -> Error {
    Error::new(
        edax_core::ErrorCode::InProgress,
        format_args!(
            "idempotency key {key} is claimed by a run that has not completed: forget it once the claim settles or lapses"
        ),
    )
}

/// The `payload_hash` of bytes that do not decode as a record, if that one field still reads. Through
/// a `Value`, as the record itself is read, so an array is never read positionally.
fn stored_hash(bytes: &[u8]) -> Option<u64> {
    match serde_json::from_slice::<Value>(bytes).ok()? {
        Value::Object(mut fields) => fields.remove("payload_hash")?.as_u64(),
        _ => None,
    }
}

/// The canonical record text from the flat fields a binding holds. The kernel applies its reading
/// order (`pending` wins, then `error`, then `result_json`), so a host that models the record as
/// separate fields cannot write an ambiguous one. `result_json` is read only for a success, and
/// `error_code` only beside an `error`. A writer must name a code the taxonomy knows: an unknown
/// one, or `result_json` that is not JSON, is a `codec` error.
pub fn encode_record_fields(
    payload_hash: u64,
    pending: bool,
    result_json: &str,
    error: Option<String>,
    error_code: Option<&str>,
) -> Result<String, Error> {
    let record = match State::from_wire(pending, error, None, ()) {
        State::Pending => Record::<Value>::pending(payload_hash),
        State::Failed { message, .. } => {
            let code = error_code.map(str::parse::<ErrorCode>).transpose()?;
            Record { payload_hash, state: State::Failed { code, message } }
        }
        State::Done(()) => Record::done(payload_hash, serde_json::from_str(result_json).map_err(Error::codec)?),
    };
    serde_json::to_string(&record).map_err(Error::codec)
}

/// How a record reads its `error_code`: a name the taxonomy does not know, or none, is no code. The
/// failure is still a failure; a writer that knows a newer code must not make an older reader refuse it.
fn stored_code(name: Option<&str>) -> Option<ErrorCode> {
    name.and_then(|name| name.parse().ok())
}

/// [`cached_failure`] from a record's flat `error` and `error_code` fields, read the way the record
/// reads them. For a binding that holds the fields, not a [`Record`].
pub fn cached_failure_of(message: &str, error_code: Option<&str>) -> Error {
    cached_failure(stored_code(error_code), message)
}

/// The error a replay of a cached failure raises: `cached_failure`, terminal, whatever the original
/// code was. Its message is the original failure's wire form (`"codec: bad payload"`), so the code
/// survives the replay; a failure stored without a code replays its message alone.
pub fn cached_failure(code: Option<ErrorCode>, message: &str) -> Error {
    match code {
        Some(code) => Error::new(ErrorCode::CachedFailure, code.tag(message)),
        None => Error::new(ErrorCode::CachedFailure, message),
    }
}

/// How the guard that won a claim settles it after the handler failed. The success path always
/// completes with [`Record::done`]; this is the failure half of the settle table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Settle {
    /// Cache the failure under this code ([`Record::failed`]), so a redelivery replays it instead
    /// of running the handler again. The failure is deterministic.
    Cache(ErrorCode),
    /// Release the claim, so a retry or a redelivery runs the handler again. The failure may not
    /// repeat: a transient code, no code at all, a panic or a cancellation.
    Release,
}

/// The failure half of the guard's settle table: cache a failure whose code is terminal, release
/// anything else. `code` is the failure's taxonomy code, `None` for a failure no code classifies (a
/// host's own error, a panic, a cancellation), which [`edax_core::verdict`] reads as transient. Two
/// SDKs guarding one ledger must cache the same failures, or one replays as terminal what the other
/// re-runs; pinned by `testdata/golden/idempotency_settle.json`.
pub fn settle(code: Option<ErrorCode>) -> Settle {
    match code {
        Some(code) if edax_core::verdict(Some(code)) => Settle::Cache(code),
        _ => Settle::Release,
    }
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
    /// The winner failed deterministically. Terminal: replay the failure ([`cached_failure`]).
    Failed {
        /// The cached failure's code, if its writer stored one.
        code: Option<ErrorCode>,
        /// The cached failure's message.
        message: &'a str,
    },
    /// The winner succeeded. Replay the value.
    Succeeded(&'a R),
}

impl<R> Outcome<'_, R> {
    /// Stable lowercase name — the single source of truth for the string form across bindings.
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::PayloadMismatch => "payload_mismatch",
            Outcome::InProgress => "in_progress",
            Outcome::Failed { .. } => "failed",
            Outcome::Succeeded(_) => "succeeded",
        }
    }
}

/// Classify the winner's record against this command's payload hash. The hash is checked first,
/// so a mismatched payload is rejected even while the winner is still in flight; then the record's
/// state decides. A stored hash of `0` is a real hash like any other.
pub fn outcome<R>(prior: &Record<R>, current: u64) -> Outcome<'_, R> {
    if prior.payload_hash != current {
        return Outcome::PayloadMismatch;
    }
    match &prior.state {
        State::Pending => Outcome::InProgress,
        State::Failed { code, message } => Outcome::Failed { code: *code, message },
        State::Done(result) => Outcome::Succeeded(result),
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
    fn settle_caches_a_terminal_code_and_releases_everything_else() {
        assert_eq!(settle(None), Settle::Release, "no code is the transient default");
        for code in ErrorCode::ALL {
            let want = if code.is_terminal() { Settle::Cache(*code) } else { Settle::Release };
            assert_eq!(settle(Some(*code)), want, "{code}");
        }
    }

    #[test]
    fn from_wire_applies_the_precedence() {
        let failed = |code| State::Failed { code, message: "boom".to_owned() };
        for (name, pending, error, error_code, want, want_str) in [
            ("pending_wins_over_error_and_result", true, Some("boom"), Some("codec"), State::Pending, "pending"),
            ("error_wins_over_result", false, Some("boom"), Some("codec"), failed(Some(ErrorCode::Codec)), "failed"),
            ("absent_code_is_no_code", false, Some("boom"), None, failed(None), "failed"),
            ("unknown_code_is_no_code", false, Some("boom"), Some("timeout"), failed(None), "failed"),
            ("a_code_without_an_error_is_ignored", false, None, Some("codec"), State::Done(42), "done"),
            ("neither_is_done", false, None, None, State::Done(42), "done"),
        ] {
            let got = State::from_wire(pending, error.map(str::to_owned), error_code, 42);
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
                    let error = got.expect_err(case.name);
                    assert_eq!(error.code, ErrorCode::Codec, "{}", case.name);
                    assert!(error.message.contains(cause), "{}: {error}", case.name);
                }
            }
        }
    }

    #[test]
    fn decode_prior_branches() {
        // Every way stored bytes can fail to be a record, and the hash each failure keeps. A readable
        // record reads exactly as `decode_record` reads it.
        const CLAIMANT: u64 = 7;
        let unreadable = |hash: u64, cause: &str| Record::<u64>::unreadable(hash, &Error::codec(cause));
        for (name, bytes, want) in [
            (
                "a_readable_record_is_itself",
                br#"{"payload_hash":11234062831410934249,"result":5,"error":null,"pending":false}"#.as_slice(),
                Record::done(HASH, 5),
            ),
            ("garbage_carries_the_claimants_hash", b"{not json".as_slice(), unreadable(CLAIMANT, "key must be a string at line 1 column 2")),
            ("not_utf8_carries_the_claimants_hash", b"\xff\xfe".as_slice(), unreadable(CLAIMANT, "expected value at line 1 column 1")),
            (
                "a_torn_write_carries_the_claimants_hash",
                br#"{"payload_hash":11234062831410934249,"res"#.as_slice(),
                unreadable(CLAIMANT, "EOF while parsing a string at line 1 column 41"),
            ),
            // An array is never read positionally, so its first element is not taken for a hash.
            ("an_array_carries_the_claimants_hash", b"[11234062831410934249,null]".as_slice(), unreadable(CLAIMANT, "idempotency record is not a JSON object")),
            ("no_hash_carries_the_claimants_hash", br#"{"result":5}"#.as_slice(), unreadable(CLAIMANT, "missing field `payload_hash`")),
            (
                "a_string_hash_carries_the_claimants_hash",
                br#"{"payload_hash":"11234062831410934249"}"#.as_slice(),
                unreadable(CLAIMANT, r#"invalid type: string "11234062831410934249", expected u64"#),
            ),
            (
                "a_wrong_type_beside_a_readable_hash_keeps_the_stored_hash",
                br#"{"payload_hash":11234062831410934249,"pending":"yes"}"#.as_slice(),
                unreadable(HASH, r#"invalid type: string "yes", expected a boolean"#),
            ),
            (
                "a_result_that_is_not_an_r_keeps_the_stored_hash",
                br#"{"payload_hash":11234062831410934249,"result":"x","error":null,"pending":false}"#.as_slice(),
                unreadable(HASH, r#"cached result is not a valid replay value: invalid type: string "x", expected u64"#),
            ),
        ] {
            assert_eq!(decode_prior::<u64>(bytes, CLAIMANT), want, "{name}");
        }
    }

    #[test]
    fn an_unreadable_record_is_a_terminal_codec_failure_naming_the_cause() {
        let record = Record::<Value>::unreadable(HASH, &Error::codec("EOF while parsing"));
        assert_eq!(record, Record::failed(HASH, ErrorCode::Codec, "unreadable idempotency record: EOF while parsing"));
        assert_eq!(
            outcome(&record, HASH),
            Outcome::Failed { code: Some(ErrorCode::Codec), message: "unreadable idempotency record: EOF while parsing" }
        );
        assert_eq!(outcome(&record, HASH ^ 1), Outcome::PayloadMismatch, "the hash is still checked first");
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
        assert!(err.message.contains("cached result is not a valid replay value"), "{err}");

        let pending = br#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":true}"#;
        assert_eq!(decode_record::<i64>(pending), Ok(Record::pending(HASH)));
        let failed = br#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","error_code":"codec","pending":false}"#;
        assert_eq!(decode_record::<i64>(failed), Ok(Record::failed(HASH, ErrorCode::Codec, "boom")));
    }

    #[test]
    fn encode_is_the_fixed_wire_shape() {
        for (name, record, want) in [
            (
                "pending",
                Record::<Value>::pending(HASH),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":true}"#,
            ),
            (
                "done",
                Record::done(HASH, json!({"z": 1, "a": [1, 2]})),
                r#"{"payload_hash":11234062831410934249,"result":{"a":[1,2],"z":1},"error":null,"error_code":null,"pending":false}"#,
            ),
            (
                "done_with_null",
                Record::done(HASH, Value::Null),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":false}"#,
            ),
            (
                "failed",
                Record::failed(HASH, ErrorCode::Rejected, "boom"),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","error_code":"rejected","pending":false}"#,
            ),
            (
                // A failure with an empty message is still a failure: only `null` means "not failed".
                "failed_with_an_empty_message",
                Record::failed(HASH, ErrorCode::Codec, ""),
                r#"{"payload_hash":11234062831410934249,"result":null,"error":"","error_code":"codec","pending":false}"#,
            ),
            (
                "failed_without_a_code_reencodes_null",
                Record { payload_hash: HASH, state: State::Failed { code: None, message: "boom".to_owned() } },
                r#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","error_code":null,"pending":false}"#,
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
        assert_eq!(error.code, ErrorCode::Codec);
        assert!(error.message.contains("key must be a string"), "{error}");
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
                Record::failed(HASH, ErrorCode::Codec, "boom"),
                HASH,
                Outcome::Failed { code: Some(ErrorCode::Codec), message: "boom" },
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
                Record::failed(HASH, ErrorCode::Codec, "boom"),
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

    #[test]
    fn a_replayed_failure_is_cached_failure_carrying_the_original_wire_form() {
        let coded = cached_failure(Some(ErrorCode::Codec), "bad payload");
        assert_eq!(coded, Error::new(ErrorCode::CachedFailure, "codec: bad payload"));
        assert_eq!(coded.to_string(), "cached_failure: codec: bad payload");
        assert_eq!(ErrorCode::split(&coded.message), Some((ErrorCode::Codec, "bad payload")), "the code survives");
        let uncoded = cached_failure(None, "boom");
        assert_eq!(uncoded, Error::new(ErrorCode::CachedFailure, "boom"));
        assert!(coded.is_terminal() && uncoded.is_terminal());
        // From flat fields: an unknown code reads as none, as the record itself reads it.
        assert_eq!(cached_failure_of("bad payload", Some("codec")), coded);
        assert_eq!(cached_failure_of("boom", Some("quota_exceeded")), uncoded);
        assert_eq!(cached_failure_of("boom", None), uncoded);
    }

    #[test]
    fn encode_record_fields_branches() {
        for (name, pending, result, error, code, want) in [
            ("pending_wins", true, "42", Some("boom"), Some("codec"), Ok(r#"{"payload_hash":7,"result":null,"error":null,"error_code":null,"pending":true}"#)),
            ("failed_with_code", false, "42", Some("boom"), Some("rejected"), Ok(r#"{"payload_hash":7,"result":null,"error":"boom","error_code":"rejected","pending":false}"#)),
            ("failed_without_code", false, "", Some("boom"), None, Ok(r#"{"payload_hash":7,"result":null,"error":"boom","error_code":null,"pending":false}"#)),
            ("done_reads_the_result", false, r#"{"b":1,"a":2}"#, None, Some("codec"), Ok(r#"{"payload_hash":7,"result":{"a":2,"b":1},"error":null,"error_code":null,"pending":false}"#)),
            ("unknown_code_is_refused", false, "", Some("boom"), Some("timeout"), Err("codec: unknown error code: timeout")),
            ("result_not_json", false, "{", None, None, Err("codec: EOF while parsing an object at line 1 column 1")),
        ] {
            let got = encode_record_fields(7, pending, result, error.map(str::to_owned), code).map_err(|e| e.to_string());
            assert_eq!(got.as_deref(), want.map_err(ToString::to_string).as_deref(), "{name}");
        }
    }
}
