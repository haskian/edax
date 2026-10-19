//! Decisions for append-only events and complete, sequence-ordered state ingestion.
//!
//! The caller holds deterministic locks for all target and rejection keys and commits the
//! resulting writes and rejections together. `source` identifies a record in an immutable
//! source incarnation; `key` already includes the target generation and tenant.

use std::collections::BTreeMap;

use edax_core::{Error, ErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::namespace::{key, prefix};
use crate::{canonical_json, holds_nul, payload_hash};

edax_core::closed_names! {
    /// The interpretation of rows in one ingestion target.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Mode {
        /// Retain each event identity once.
        Append = "append",
        /// Replace the complete state at a sequence-ordered row identity.
        Upsert = "upsert",
    }
}

edax_core::closed_names! {
    /// The durable decision for an original input.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Outcome {
        /// Applied, or already represented by committed state.
        Accepted = "accepted",
        /// Refused; its durable rejection must complete publication.
        Rejected = "rejected",
    }
}

/// Exact source coordinates. Kafka offsets are decimal strings in canonical JSON keys,
/// so their entire signed 64-bit range survives the JSON safe-integer boundary.
pub fn source_key(target: &str, incarnation: &str, topic: &str, partition: i32, offset: i64) -> Result<String, Error> {
    if incarnation.is_empty() || topic.is_empty() || partition < 0 || offset < 0 {
        return Err(Error::bad_handle("invalid ingestion source coordinate"));
    }
    let parts = serde_json::json!(["source", incarnation, topic, partition, offset.to_string()]);
    key(target, &parts.to_string())
}

/// Scope an append event identity or upsert row identity to its generation and tenant.
pub fn row_key(target: &str, mode: &str, tenant: Option<&str>, row_id: &str) -> Result<String, Error> {
    let mode = Mode::by_name(mode).ok_or_else(|| Error::bad_handle("invalid ingestion row identity"))?;
    if row_id.is_empty() {
        return Err(Error::bad_handle("invalid ingestion row identity"));
    }
    key(target, &serde_json::json!([mode.as_str(), tenant, row_id]).to_string())
}

/// Immutable interpretation of one target generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    /// Append events or replace complete state.
    pub mode: String,
    /// Unique physical target namespace.
    pub generation: String,
    /// Verified source identity, changed when the source is replaced.
    pub source_incarnation: String,
    /// Immutable mapping and keying implementation identity.
    pub mapper_id: String,
}

/// A restored/reused target cannot silently change how records are interpreted.
pub fn bind_contract(previous: Option<&Contract>, expected: &Contract) -> Result<(), Error> {
    prefix(&expected.generation)?;
    if Mode::by_name(&expected.mode).is_none()
        || expected.source_incarnation.is_empty()
        || expected.mapper_id.is_empty()
        || expected.source_incarnation.contains('\0')
        || expected.mapper_id.contains('\0')
        || previous.is_some_and(|prior| prior != expected)
    {
        return Err(Error::bad_handle("invalid or changed ingestion target contract"));
    }
    Ok(())
}

/// Prevent a caller from escaping the target manifest through hand-built input keys.
pub fn validate_inputs(contract: &Contract, inputs: &[Input]) -> Result<(), Error> {
    bind_contract(None, contract)?;
    let namespace = prefix(&contract.generation)?;
    for input in inputs {
        let parse = |value: &str| -> Result<Vec<Value>, Error> {
            let suffix = value
                .strip_prefix(&namespace)
                .ok_or_else(|| Error::bad_handle("input belongs to another ingestion target"))?;
            let parts: Vec<Value> = serde_json::from_str(suffix).map_err(Error::codec)?;
            if key(&contract.generation, suffix)? != value {
                return Err(Error::bad_handle("ingestion identity is not canonical"));
            }
            Ok(parts)
        };
        let source = parse(&input.source)?;
        let row = parse(&input.key)?;
        if source.len() != 5
            || source.first().and_then(Value::as_str) != Some("source")
            || source.get(1).and_then(Value::as_str) != Some(&contract.source_incarnation)
            || source.get(2).and_then(Value::as_str).is_none_or(str::is_empty)
            || source
                .get(3)
                .and_then(Value::as_u64)
                .is_none_or(|v| v > i32::MAX as u64)
            || source
                .get(4)
                .and_then(Value::as_str)
                .is_none_or(|v| !v.parse::<i64>().is_ok_and(|n| n >= 0 && n.to_string() == v))
            || row.len() != 3
            || row.first().and_then(Value::as_str) != Some(&contract.mode)
            || row.get(1).is_none_or(|v| !v.is_null() && !v.is_string())
            || row.get(2).and_then(Value::as_str).is_none_or(str::is_empty)
            || (contract.mode == Mode::Append.as_str() && row.get(2).and_then(Value::as_str) != Some(&input.event_id))
            || input.stream.is_empty()
            || input.event_id.is_empty()
        {
            return Err(Error::bad_handle("invalid ingestion source or row identity"));
        }
        if !is_fingerprint(&input.raw_fingerprint) {
            return Err(Error::bad_handle(
                "ingestion raw fingerprint must be a SHA-256 digest in lowercase hex",
            ));
        }
    }
    Ok(())
}

/// One original source record, before compaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Input<T = Value> {
    /// Immutable source coordinate, including source incarnation.
    pub source: String,
    /// Fully scoped target key.
    pub key: String,
    /// Aggregate stream that permanently owns an upsert key.
    pub stream: String,
    /// Stable event identity; the append key must include it.
    pub event_id: String,
    /// Aggregate sequence. Upserts require a PostgreSQL bigint-compatible value.
    pub sequence: Option<u64>,
    /// Canonical event content used to detect conflicting replays.
    pub content: T,
    /// Complete mapped state; absence is a retained deletion marker.
    pub value: Option<T>,
    /// Lossless source representation retained by a rejection.
    pub raw: String,
    /// SHA-256 of the original source record, 64 lowercase hex digits, independent of a retained
    /// full record or stub. Kafka's raw encoding supplies it; [`raw_fingerprint()`] computes it for any
    /// other source.
    pub raw_fingerprint: String,
    /// An already classified decode or mapping rejection in kernel error wire form.
    pub failure: Option<String>,
}

/// Parse original payload tokens before JSON can round an integer wider than 64 bits.
/// Invalid payloads become per-record rejections; source/sequence fields keep their full width.
pub fn inputs_json(text: &str) -> Result<Vec<Input>, Error> {
    use crate::json_numbers::{check_integers, Width};
    use serde_json::value::RawValue;

    let inputs: Vec<Input<&RawValue>> = serde_json::from_str(text).map_err(Error::codec)?;
    inputs
        .into_iter()
        .map(|input| {
            let parse = |raw: &RawValue| -> Result<Value, Error> {
                // Preserve integers the Value model represents exactly. The planner applies
                // canonical-number validation after sequence and durable rejection checks.
                check_integers(raw.get().as_bytes(), Width::Bits64)?;
                serde_json::from_str(raw.get()).map_err(Error::codec)
            };
            let payload = parse(input.content)
                .and_then(|content| input.value.map(parse).transpose().map(|value| (content, value)));
            let (content, value, failure) = match payload {
                Ok((content, value)) => (content, value, input.failure),
                Err(error) => (Value::Null, None, input.failure.or_else(|| Some(error.to_string()))),
            };
            Ok(Input {
                source: input.source,
                key: input.key,
                stream: input.stream,
                event_id: input.event_id,
                sequence: input.sequence,
                content,
                value,
                raw: input.raw,
                raw_fingerprint: input.raw_fingerprint,
                failure,
            })
        })
        .collect()
}

/// The committed value and its permanent owner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stored {
    /// Permanent aggregate stream owner, including for deletion markers.
    pub stream: String,
    /// Last accepted aggregate sequence; append mode does not require one.
    pub sequence: Option<u64>,
    /// Hash of canonical event content and mapped value.
    pub fingerprint: String,
    /// Complete state or a retained deletion marker.
    pub value: Option<Value>,
}

/// Durable refusal, independent of later target state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Rejection {
    /// Hash of the original source representation.
    pub fingerprint: String,
    /// Original error in kernel wire form.
    pub error: String,
    /// Original source representation for the dead-letter publisher.
    pub raw: String,
    /// Whether publication has been confirmed; delivery itself remains at least once.
    pub published: bool,
}

/// Atomic changes plus an outcome for every original input, in source order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// `accepted` includes exact and stale replays; `rejected` requires dead-letter completion.
    pub outcomes: Vec<String>,
    /// Changed target rows only. Deletion markers are rows, not removals.
    pub writes: BTreeMap<String, Stored>,
    /// New rejections and persisted rejections touched by this call. Persist only absent keys:
    /// a fingerprint-verified replay may carry its current bounded raw representation.
    pub rejections: BTreeMap<String, Rejection>,
}

fn fingerprint(input: &Input) -> Result<String, Error> {
    let bytes = serde_json::to_vec(&(&input.content, &input.value)).map_err(Error::codec)?;
    Ok(payload_hash(&canonical_json(&bytes)?).to_string())
}

/// The raw fingerprint of a source record whose `raw` representation is the whole record.
pub fn raw_fingerprint(raw: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::new()
            .chain_update(b"edax.ingestion.raw/v1\0")
            .chain_update(raw)
            .finalize()
    )
}

fn is_fingerprint(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn reject(plan: &mut Plan, input: &Input, error: String) {
    plan.rejections.insert(
        input.source.clone(),
        Rejection {
            fingerprint: input.raw_fingerprint.clone(),
            error,
            raw: input.raw.clone(),
            published: false,
        },
    );
    plan.outcomes.push(Outcome::Rejected.as_str().into());
}

/// Plan one append or full-state upsert transaction against locked committed state.
///
/// Persisted rejects and invalid inputs cannot propose ownership or participate in compaction.
/// Upserts compare stale inputs against committed state, retain the first value at each newer
/// sequence, and write only the greatest accepted sequence. No historical fingerprints survive
/// beyond the current state. Unknown modes and reused source coordinates fail the whole call.
pub fn plan(
    mode: &str,
    inputs: &[Input],
    stored: &BTreeMap<String, Stored>,
    rejected: &BTreeMap<String, Rejection>,
) -> Result<Plan, Error> {
    let mode = Mode::by_name(mode).ok_or_else(|| Error::bad_handle("ingestion mode must be append or upsert"))?;
    let mut sources = BTreeMap::<&str, &Input>::new();
    for input in inputs {
        if sources
            .insert(&input.source, input)
            .is_some_and(|prior| prior.raw_fingerprint != input.raw_fingerprint)
            || rejected
                .get(&input.source)
                .is_some_and(|prior| prior.fingerprint != input.raw_fingerprint)
        {
            return Err(Error::bad_handle(
                "ingestion source coordinate reused with different raw content",
            ));
        }
    }
    let mut result = Plan {
        outcomes: Vec::with_capacity(inputs.len()),
        writes: BTreeMap::new(),
        rejections: BTreeMap::new(),
    };
    let mut owners = BTreeMap::<String, String>::new();
    let mut versions = BTreeMap::<(String, u64), String>::new();
    for input in inputs {
        if let Some(prior) = rejected
            .get(&input.source)
            .or_else(|| result.rejections.get(&input.source))
        {
            let prior = Rejection {
                fingerprint: prior.fingerprint.clone(),
                error: prior.error.clone(),
                // A store need not read back a durable rejection's raw: the verified replay carries it.
                raw: if rejected.contains_key(&input.source) {
                    input.raw.clone()
                } else {
                    prior.raw.clone()
                },
                published: prior.published,
            };
            result.rejections.insert(input.source.clone(), prior);
            result.outcomes.push(Outcome::Rejected.as_str().into());
            continue;
        }
        if let Some(failure) = &input.failure {
            reject(&mut result, input, failure.clone());
            continue;
        }
        // Postgres cannot store NUL in jsonb: refuse the record here, as every store would.
        if holds_nul(&input.content)
            || input.value.as_ref().is_some_and(holds_nul)
            || input.stream.contains('\0')
            || input.event_id.contains('\0')
        {
            reject(
                &mut result,
                input,
                Error::codec("an ingested record holds a NUL character, which Postgres cannot store").to_string(),
            );
            continue;
        }
        if mode == Mode::Upsert && input.sequence.is_none_or(|sequence| sequence > i64::MAX as u64) {
            reject(
                &mut result,
                input,
                Error::codec("upsert requires a sequence in 0..=i64::MAX").to_string(),
            );
            continue;
        }
        let fingerprint = match fingerprint(input) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                reject(&mut result, input, error.to_string());
                continue;
            }
        };
        if mode == Mode::Append {
            if let Some(prior) = result.writes.get(&input.key).or_else(|| stored.get(&input.key)) {
                if prior.fingerprint != fingerprint {
                    reject(
                        &mut result,
                        input,
                        Error::new(ErrorCode::Rejected, "append event identity has different content").to_string(),
                    );
                    continue;
                }
            } else {
                result.writes.insert(
                    input.key.clone(),
                    Stored {
                        stream: input.stream.clone(),
                        sequence: input.sequence,
                        fingerprint,
                        value: input.value.clone(),
                    },
                );
            }
            result.outcomes.push(Outcome::Accepted.as_str().into());
            continue;
        }
        let owner = stored
            .get(&input.key)
            .map(|row| &row.stream)
            .or_else(|| owners.get(&input.key));
        if owner.is_some_and(|owner| owner != &input.stream) {
            reject(
                &mut result,
                input,
                Error::new(ErrorCode::Rejected, "upsert key belongs to another aggregate stream").to_string(),
            );
            continue;
        }
        // Presence was checked above; keep the fallible boundary explicit rather than panic.
        let sequence = input
            .sequence
            .ok_or_else(|| Error::bad_handle("validated sequence missing"))?;
        if let Some(current) = stored.get(&input.key) {
            let current_sequence = current
                .sequence
                .ok_or_else(|| Error::bad_handle("stored upsert sequence missing"))?;
            if sequence < current_sequence {
                result.outcomes.push(Outcome::Accepted.as_str().into());
                continue;
            }
            if sequence == current_sequence {
                if fingerprint != current.fingerprint {
                    reject(
                        &mut result,
                        input,
                        Error::new(
                            ErrorCode::Rejected,
                            "committed aggregate sequence has different content",
                        )
                        .to_string(),
                    );
                } else {
                    result.outcomes.push(Outcome::Accepted.as_str().into());
                }
                continue;
            }
        }
        let version_key = (input.key.clone(), sequence);
        if versions.get(&version_key).is_some_and(|prior| prior != &fingerprint) {
            reject(
                &mut result,
                input,
                Error::new(
                    ErrorCode::Rejected,
                    "aggregate sequence has different content in this batch",
                )
                .to_string(),
            );
            continue;
        }
        owners.entry(input.key.clone()).or_insert_with(|| input.stream.clone());
        versions.insert(version_key, fingerprint.clone());
        if result
            .writes
            .get(&input.key)
            .is_none_or(|prior| prior.sequence < Some(sequence))
        {
            result.writes.insert(
                input.key.clone(),
                Stored {
                    stream: input.stream.clone(),
                    sequence: Some(sequence),
                    fingerprint,
                    value: input.value.clone(),
                },
            );
        }
        result.outcomes.push(Outcome::Accepted.as_str().into());
    }
    Ok(result)
}
